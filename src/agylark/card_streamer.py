"""Feishu CardKit 2.0 流式交互卡片模板构建、回调载荷生成与时间窗口节流更新器。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import json
import logging
import math
import threading
import time
from typing import Any, Protocol
import uuid as uuid_lib

from .acp_client import AcpPromptResult, AcpSessionUpdate
from .config import DEFAULT_STREAM_THROTTLE

logger = logging.getLogger(__name__)

CARD_SCHEMA_VERSION = "2.0"
STATUS_ELEMENT_ID = "status_bar"
CONTENT_ELEMENT_ID = "content_md"
STOP_BUTTON_ELEMENT_ID = "stop_btn"
STOP_ACTION_NAME = "stop"
MODEL_SELECT_ACTION_NAME = "select_model"
MODEL_STATUS_ELEMENT_ID = "model_status"
MODEL_HINT_ELEMENT_ID = "model_hint"
EFFORT_SELECT_ACTION_NAME = "select_effort"
EFFORT_STATUS_ELEMENT_ID = "effort_status"
EFFORT_HINT_ELEMENT_ID = "effort_hint"
ACP_MODEL_CONFIG_ID = "model"
ACP_EFFORT_CONFIG_ID = "thought_level"
DEFAULT_MAX_CONTENT_CHARS = 28_000


class CardRunState(str, Enum):
    """卡片当前的执行生命周期状态。"""

    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"


_STATE_HEADER_TEMPLATES: dict[CardRunState, str] = {
    CardRunState.RUNNING: "blue",
    CardRunState.COMPLETED: "green",
    CardRunState.CANCELLED: "orange",
    CardRunState.ERROR: "red",
}

_STATE_DEFAULT_SUMMARIES: dict[CardRunState, str] = {
    CardRunState.RUNNING: "正在生成回复...",
    CardRunState.COMPLETED: "回复已完成",
    CardRunState.CANCELLED: "已停止生成",
    CardRunState.ERROR: "执行失败",
}


class CardTransport(Protocol):
    """CardKit v1 卡片实体创建与增量/全量更新传输协议接口。"""

    def create_card(self, card_json: dict[str, Any]) -> str:
        """创建 CardKit 2.0 卡片实体并返回 card_id。"""

    def stream_element_content(
        self,
        card_id: str,
        element_id: str,
        content: str,
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        """向启用流式模式的文本/Markdown 组件推送最新全量内容。"""

    def update_card(
        self,
        card_id: str,
        card_json: dict[str, Any],
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        """全量更新卡片实体结构（用于终态收口与移除交互按钮）。"""

    def update_settings(
        self,
        card_id: str,
        settings: dict[str, Any],
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        """更新卡片实体配置（如关闭 streaming_mode）。"""


@dataclass(frozen=True)
class StopCallbackPayload:
    """解析后的卡片停止按钮回调载荷。"""

    session_id: str
    turn_id: str | None = None


def build_stop_callback_value(
    session_id: str,
    *,
    turn_id: str | None = None,
    action: str = STOP_ACTION_NAME,
) -> dict[str, str]:
    """生成 CardKit 2.0 停止按钮 behaviors[].value 对象（飞书要求必须是键值对字典）。"""
    normalized_session = session_id.strip()
    if not normalized_session:
        raise ValueError("session_id 不能为空")
    normalized_action = action.strip()
    if not normalized_action:
        raise ValueError("action 不能为空")

    payload: dict[str, str] = {
        "action": normalized_action,
        "session_id": normalized_session,
    }
    if turn_id is not None and turn_id.strip():
        payload["turn_id"] = turn_id.strip()
    return payload


def parse_stop_callback_value(value: Any) -> StopCallbackPayload | None:
    """从 card.action.trigger 的 action.value 解析停止指令；不匹配时返回 None。"""
    if not isinstance(value, dict):
        return None
    if value.get("action") != STOP_ACTION_NAME:
        return None
    session_id = value.get("session_id")
    if not isinstance(session_id, str) or not session_id.strip():
        return None
    raw_turn_id = value.get("turn_id")
    turn_id = raw_turn_id.strip() if isinstance(raw_turn_id, str) and raw_turn_id.strip() else None
    return StopCallbackPayload(session_id=session_id.strip(), turn_id=turn_id)


@dataclass(frozen=True)
class AcpModelOption:
    """ACP 返回的单个可选模型元数据。"""

    model_id: str
    name: str


@dataclass(frozen=True)
class AcpEffortOption:
    """ACP 返回的单个可选思考强度元数据。"""

    effort_id: str
    name: str


@dataclass(frozen=True)
class ModelSelectCallbackPayload:
    """解析后的 `/model` 卡片模型切换回调载荷。"""

    model_id: str
    conv_key: str | None = None


@dataclass(frozen=True)
class EffortSelectCallbackPayload:
    """解析后的 `/effort` 卡片思考强度切换回调载荷。"""

    effort_id: str
    conv_key: str | None = None


def _find_config_option(payload: Any, *, option_id: str) -> Mapping[str, Any] | None:
    """从 ACP `session/new` 或 `session/set_config_option` 响应中按 id/category 查找配置项。"""
    if not isinstance(payload, Mapping):
        return None
    raw_options = payload.get("configOptions")
    if not isinstance(raw_options, list):
        return None
    for item in raw_options:
        if not isinstance(item, Mapping):
            continue
        item_id = item.get("id")
        category = item.get("category")
        if item_id == option_id or category == option_id:
            return item
    return None


def has_config_option(payload: Any, *, option_id: str) -> bool:
    """判断 ACP 返回元数据是否包含指定 `configOptions` 配置项。"""
    return _find_config_option(payload, option_id=option_id) is not None


def _parse_config_option_choices(payload: Any, *, option_id: str) -> tuple[tuple[str, str], ...]:
    option_block = _find_config_option(payload, option_id=option_id)
    if option_block is None:
        return ()
    raw_choices = option_block.get("options")
    if not isinstance(raw_choices, list):
        return ()

    seen_ids: set[str] = set()
    parsed: list[tuple[str, str]] = []
    for item in raw_choices:
        if not isinstance(item, Mapping):
            continue
        raw_val = item.get("value")
        if not isinstance(raw_val, str) or not raw_val.strip():
            continue
        val_id = raw_val.strip()
        if val_id in seen_ids:
            continue
        seen_ids.add(val_id)
        raw_name = item.get("name")
        name = raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else val_id
        parsed.append((val_id, name))
    return tuple(parsed)


def _parse_config_option_current(payload: Any, *, option_id: str) -> str | None:
    option_block = _find_config_option(payload, option_id=option_id)
    if option_block is None:
        return None
    raw_current = option_block.get("currentValue")
    if not isinstance(raw_current, str) or not raw_current.strip():
        return None
    return raw_current.strip()


def parse_available_models(session_info: Any) -> tuple[AcpModelOption, ...]:
    """从 ACP 元数据提取并去重可用模型列表；优先读取 `configOptions.model`（仅模型本身）。"""
    config_choices = _parse_config_option_choices(session_info, option_id=ACP_MODEL_CONFIG_ID)
    if config_choices:
        return tuple(AcpModelOption(model_id=mid, name=name) for mid, name in config_choices)

    if not isinstance(session_info, Mapping):
        return ()
    models_block = session_info.get("models")
    if not isinstance(models_block, Mapping):
        return ()
    raw_models = models_block.get("availableModels")
    if not isinstance(raw_models, list):
        return ()

    seen_ids: set[str] = set()
    parsed: list[AcpModelOption] = []
    for item in raw_models:
        if not isinstance(item, Mapping):
            continue
        raw_id = item.get("modelId")
        if not isinstance(raw_id, str) or not raw_id.strip():
            continue
        model_id = raw_id.strip()
        if model_id in seen_ids:
            continue
        seen_ids.add(model_id)
        raw_name = item.get("name")
        name = raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else model_id
        parsed.append(AcpModelOption(model_id=model_id, name=name))
    return tuple(parsed)


def parse_current_model_id(session_info: Any) -> str | None:
    """从 ACP 元数据提取当前生效模型 ID；优先读取 `configOptions.model.currentValue`。"""
    config_current = _parse_config_option_current(session_info, option_id=ACP_MODEL_CONFIG_ID)
    if config_current is not None:
        return config_current

    if not isinstance(session_info, Mapping):
        return None
    models_block = session_info.get("models")
    if not isinstance(models_block, Mapping):
        return None
    raw_current = models_block.get("currentModelId")
    if not isinstance(raw_current, str) or not raw_current.strip():
        return None
    return raw_current.strip()


def parse_available_efforts(session_info: Any) -> tuple[AcpEffortOption, ...]:
    """从 ACP `configOptions.thought_level` 提取当前模型支持的思考强度选项列表。"""
    choices = _parse_config_option_choices(session_info, option_id=ACP_EFFORT_CONFIG_ID)
    return tuple(AcpEffortOption(effort_id=eid, name=name) for eid, name in choices)


def parse_current_effort_id(session_info: Any) -> str | None:
    """从 ACP `configOptions.thought_level` 提取当前生效的思考强度标识。"""
    return _parse_config_option_current(session_info, option_id=ACP_EFFORT_CONFIG_ID)


def _build_select_callback_value(
    *,
    action: str,
    field_name: str,
    value: str,
    conv_key: str | None = None,
) -> dict[str, str]:
    clean_value = value.strip() if isinstance(value, str) else ""
    if not clean_value:
        raise ValueError(f"{field_name} 不能为空")
    clean_action = action.strip() if isinstance(action, str) else ""
    if not clean_action:
        raise ValueError("action 不能为空")

    payload: dict[str, str] = {
        "action": clean_action,
        field_name: clean_value,
    }
    if isinstance(conv_key, str) and conv_key.strip():
        payload["conv_key"] = conv_key.strip()
    return payload


def _parse_select_callback_value(
    raw_payload: Any,
    *,
    action: str,
    field_name: str,
) -> tuple[str, str | None] | None:
    if not isinstance(raw_payload, Mapping):
        return None
    if raw_payload.get("action") != action:
        return None
    raw_value = raw_payload.get(field_name)
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    raw_conv_key = raw_payload.get("conv_key")
    conv_key = (
        raw_conv_key.strip()
        if isinstance(raw_conv_key, str) and raw_conv_key.strip()
        else None
    )
    return raw_value.strip(), conv_key


def build_model_select_callback_value(
    model_id: str,
    *,
    conv_key: str | None = None,
    action: str = MODEL_SELECT_ACTION_NAME,
) -> dict[str, str]:
    """构造飞书卡片按钮 `behaviors[].value` 模型切换回传载荷。"""
    return _build_select_callback_value(
        action=action,
        field_name="model_id",
        value=model_id,
        conv_key=conv_key,
    )


def parse_model_select_callback_value(value: Any) -> ModelSelectCallbackPayload | None:
    """从 `card.action.trigger` 的 `action.value` 解析模型切换指令；不匹配时返回 None。"""
    parsed = _parse_select_callback_value(
        value,
        action=MODEL_SELECT_ACTION_NAME,
        field_name="model_id",
    )
    if parsed is None:
        return None
    return ModelSelectCallbackPayload(model_id=parsed[0], conv_key=parsed[1])


def build_effort_select_callback_value(
    effort_id: str,
    *,
    conv_key: str | None = None,
    action: str = EFFORT_SELECT_ACTION_NAME,
) -> dict[str, str]:
    """构造飞书卡片按钮 `behaviors[].value` 思考强度切换回传载荷。"""
    return _build_select_callback_value(
        action=action,
        field_name="effort_id",
        value=effort_id,
        conv_key=conv_key,
    )


def parse_effort_select_callback_value(value: Any) -> EffortSelectCallbackPayload | None:
    """从 `card.action.trigger` 的 `action.value` 解析思考强度切换指令；不匹配时返回 None。"""
    parsed = _parse_select_callback_value(
        value,
        action=EFFORT_SELECT_ACTION_NAME,
        field_name="effort_id",
    )
    if parsed is None:
        return None
    return EffortSelectCallbackPayload(effort_id=parsed[0], conv_key=parsed[1])


@dataclass(frozen=True)
class _SelectorButtonSpec:
    element_id: str
    label: str
    is_current: bool
    callback_value: dict[str, str]


def _build_selector_button(spec: _SelectorButtonSpec) -> dict[str, Any]:
    return {
        "tag": "button",
        "element_id": spec.element_id,
        "text": {
            "tag": "plain_text",
            "content": f"✓ {spec.label}" if spec.is_current else spec.label,
        },
        "type": "primary" if spec.is_current else "default",
        "size": "small",
        "width": "fill",
        "behaviors": [
            {
                "type": "callback",
                "value": spec.callback_value,
            }
        ],
    }


def _build_selector_card_json(
    *,
    title: str,
    subtitle: str | None,
    summary_text: str,
    status_element_id: str,
    status_markdown: str,
    hint_element_id: str,
    hint_markdown: str,
    empty_markdown: str,
    buttons: Sequence[_SelectorButtonSpec],
) -> dict[str, Any]:
    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "element_id": status_element_id,
            "content": status_markdown,
        },
        {"tag": "hr"},
    ]

    if buttons:
        for spec in buttons:
            elements.append(_build_selector_button(spec))
        elements.append(
            {
                "tag": "markdown",
                "element_id": hint_element_id,
                "content": hint_markdown,
            }
        )
    else:
        elements.append(
            {
                "tag": "markdown",
                "element_id": hint_element_id,
                "content": empty_markdown,
            }
        )

    header: dict[str, Any] = {
        "title": {"tag": "plain_text", "content": title},
        "template": "blue",
    }
    if isinstance(subtitle, str) and subtitle.strip():
        header["subtitle"] = {"tag": "plain_text", "content": subtitle.strip()}

    return {
        "schema": CARD_SCHEMA_VERSION,
        "config": {
            "update_multi": True,
            "summary": {"content": summary_text},
        },
        "header": header,
        "body": {
            "elements": elements,
        },
    }


def build_model_selector_card_json(
    *,
    models: Sequence[AcpModelOption],
    current_model_id: str | None = None,
    conv_key: str | None = None,
    title: str = "Antigravity 模型选择",
    subtitle: str | None = None,
) -> dict[str, Any]:
    """构造 CardKit 2.0 交互式 `/model` 模型选择卡片 JSON（仅包含模型本身）。"""
    clean_current = (
        current_model_id.strip()
        if isinstance(current_model_id, str) and current_model_id.strip()
        else None
    )
    matched_option = next(
        (opt for opt in models if opt.model_id == clean_current),
        None,
    )
    if matched_option is not None:
        current_desc = f"{matched_option.name} (`{matched_option.model_id}`)"
        summary_model = matched_option.name
    elif clean_current is not None:
        current_desc = f"`{clean_current}`"
        summary_model = clean_current
    else:
        current_desc = "未指定（将使用 Antigravity 默认模型）"
        summary_model = "默认模型"

    buttons = [
        _SelectorButtonSpec(
            element_id=f"model_btn_{idx}",
            label=opt.name,
            is_current=opt.model_id == clean_current,
            callback_value=build_model_select_callback_value(opt.model_id, conv_key=conv_key),
        )
        for idx, opt in enumerate(models)
    ]
    return _build_selector_card_json(
        title=title,
        subtitle=subtitle,
        summary_text=f"模型切换：{summary_model}",
        status_element_id=MODEL_STATUS_ELEMENT_ID,
        status_markdown=f"**当前模型**：{current_desc}",
        hint_element_id=MODEL_HINT_ELEMENT_ID,
        hint_markdown="💡`/effort` 用于切换思考强度",
        empty_markdown="⚠️ Antigravity 暂未返回可用模型列表，请稍后重试。",
        buttons=buttons,
    )


def build_effort_selector_card_json(
    *,
    efforts: Sequence[AcpEffortOption],
    current_effort_id: str | None = None,
    current_model_name: str | None = None,
    conv_key: str | None = None,
    title: str = "Antigravity 思考强度",
    subtitle: str | None = None,
) -> dict[str, Any]:
    """构造 CardKit 2.0 交互式 `/effort` 思考强度选择卡片 JSON。"""
    clean_current = (
        current_effort_id.strip()
        if isinstance(current_effort_id, str) and current_effort_id.strip()
        else None
    )
    matched_option = next(
        (opt for opt in efforts if opt.effort_id == clean_current),
        None,
    )
    if not efforts:
        current_desc = "当前模型不支持调节"
        summary_effort = "不支持调节"
    elif matched_option is not None:
        current_desc = f"{matched_option.name} (`{matched_option.effort_id}`)"
        summary_effort = matched_option.name
    elif clean_current is not None:
        current_desc = f"`{clean_current}`"
        summary_effort = clean_current
    else:
        current_desc = "未指定（将使用模型默认思考强度）"
        summary_effort = "默认强度"

    status_lines: list[str] = []
    if isinstance(current_model_name, str) and current_model_name.strip():
        status_lines.append(f"**当前模型**：{current_model_name.strip()}")
    status_lines.append(f"**当前思考强度**：{current_desc}")

    buttons = [
        _SelectorButtonSpec(
            element_id=f"effort_btn_{idx}",
            label=opt.name,
            is_current=opt.effort_id == clean_current,
            callback_value=build_effort_select_callback_value(opt.effort_id, conv_key=conv_key),
        )
        for idx, opt in enumerate(efforts)
    ]
    return _build_selector_card_json(
        title=title,
        subtitle=subtitle,
        summary_text=f"思考强度：{summary_effort}",
        status_element_id=EFFORT_STATUS_ELEMENT_ID,
        status_markdown="\n".join(status_lines),
        hint_element_id=EFFORT_HINT_ELEMENT_ID,
        hint_markdown="💡 点击按钮立即调整当前会话思考强度；配置 `.env` 的 `AGY_EFFORT` 可设默认强度（不同模型支持档位可能不同）。",
        empty_markdown="ℹ️ 当前模型不支持单独调节思考强度；可使用 `/model` 切换到支持思考强度调节的模型后再试。",
        buttons=buttons,
    )


def build_streaming_card_json(
    *,
    session_id: str,
    status_text: str,
    content_text: str,
    state: CardRunState = CardRunState.RUNNING,
    title: str = "Antigravity",
    subtitle: str | None = None,
    turn_id: str | None = None,
    summary_text: str | None = None,
    stop_button_text: str = "停止生成",
    show_stop_button: bool | None = None,
) -> dict[str, Any]:
    """组装符合飞书 Card JSON 2.0 规范的流式交互卡片结构。"""
    is_running = state == CardRunState.RUNNING
    include_stop = is_running if show_stop_button is None else show_stop_button

    header: dict[str, Any] = {
        "title": {"tag": "plain_text", "content": title},
        "template": _STATE_HEADER_TEMPLATES[state],
    }
    if subtitle:
        header["subtitle"] = {"tag": "plain_text", "content": subtitle}

    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "element_id": STATUS_ELEMENT_ID,
            "content": status_text or _STATE_DEFAULT_SUMMARIES[state],
        },
        {"tag": "hr"},
        {
            "tag": "markdown",
            "element_id": CONTENT_ELEMENT_ID,
            "content": content_text or " ",
        },
    ]

    if include_stop:
        elements.append(
            {
                "tag": "button",
                "element_id": STOP_BUTTON_ELEMENT_ID,
                "type": "danger",
                "size": "medium",
                "text": {"tag": "plain_text", "content": stop_button_text},
                "behaviors": [
                    {
                        "type": "callback",
                        "value": build_stop_callback_value(session_id, turn_id=turn_id),
                    }
                ],
            }
        )

    return {
        "schema": CARD_SCHEMA_VERSION,
        "config": {
            "streaming_mode": is_running,
            "update_multi": True,
            "summary": {"content": summary_text or _STATE_DEFAULT_SUMMARIES[state]},
        },
        "header": header,
        "body": {"elements": elements},
    }


def build_create_card_payload(card_json: dict[str, Any]) -> dict[str, str]:
    """构造 POST /open-apis/cardkit/v1/cards 请求体。"""
    return {
        "type": "card_json",
        "data": json.dumps(card_json, ensure_ascii=False),
    }


def build_stream_content_payload(
    content: str,
    *,
    sequence: int,
    uuid: str | None = None,
) -> dict[str, Any]:
    """构造 PUT /open-apis/cardkit/v1/cards/:card_id/elements/:element_id/content 请求体。"""
    if sequence <= 0:
        raise ValueError("sequence 必须为正整数")
    payload: dict[str, Any] = {
        "content": content,
        "sequence": sequence,
    }
    if uuid:
        payload["uuid"] = uuid
    return payload


def build_update_card_payload(
    card_json: dict[str, Any],
    *,
    sequence: int,
    uuid: str | None = None,
) -> dict[str, Any]:
    """构造 PUT /open-apis/cardkit/v1/cards/:card_id 全量更新请求体。"""
    if sequence <= 0:
        raise ValueError("sequence 必须为正整数")
    payload: dict[str, Any] = {
        "card": {
            "type": "card_json",
            "data": json.dumps(card_json, ensure_ascii=False),
        },
        "sequence": sequence,
    }
    if uuid:
        payload["uuid"] = uuid
    return payload


def build_update_settings_payload(
    settings: dict[str, Any],
    *,
    sequence: int,
    uuid: str | None = None,
) -> dict[str, Any]:
    """构造 PATCH /open-apis/cardkit/v1/cards/:card_id/settings 请求体。"""
    if sequence <= 0:
        raise ValueError("sequence 必须为正整数")
    payload: dict[str, Any] = {
        "settings": json.dumps(settings, ensure_ascii=False),
        "sequence": sequence,
    }
    if uuid:
        payload["uuid"] = uuid
    return payload


def _truncate_markdown_preserving_prefix(text: str, max_chars: int) -> str:
    """超长时保留前缀稳定性，避免流式打字机更新反复全量闪烁。"""
    if len(text) <= max_chars:
        return text
    suffix = "\n\n_（输出内容较长，后续部分已截断）_"
    keep = max(0, max_chars - len(suffix))
    return text[:keep] + suffix


def _format_usage_suffix(usage: dict[str, Any] | None) -> str:
    """将 ACP usage 统计格式化为简洁状态后缀。"""
    if not isinstance(usage, dict):
        return ""
    total = usage.get("totalTokens")
    if isinstance(total, int) and total > 0:
        return f" · {total} tokens"
    return ""


class StreamingCardUpdater:
    """管理单轮对话 CardKit 2.0 卡片的时间窗口节流刷新与终态收口。"""

    def __init__(
        self,
        transport: CardTransport,
        session_id: str,
        *,
        turn_id: str | None = None,
        title: str = "Antigravity",
        subtitle: str | None = None,
        throttle_seconds: float = DEFAULT_STREAM_THROTTLE,
        max_content_chars: int = DEFAULT_MAX_CONTENT_CHARS,
        clock: Callable[[], float] = time.monotonic,
        timer_factory: Callable[[float, Callable[[], None]], Any] = threading.Timer,
        uuid_factory: Callable[[], str] | None = None,
    ) -> None:
        if not session_id.strip():
            raise ValueError("session_id 不能为空")
        if not math.isfinite(throttle_seconds) or throttle_seconds < 0:
            raise ValueError(f"throttle_seconds 必须为非负有限数字: {throttle_seconds}")
        if max_content_chars <= 100:
            raise ValueError("max_content_chars 必须大于 100")

        self.transport = transport
        self.session_id = session_id.strip()
        self.turn_id = turn_id.strip() if isinstance(turn_id, str) and turn_id.strip() else None
        self.title = title
        self.subtitle = subtitle
        self.throttle_seconds = float(throttle_seconds)
        self.max_content_chars = max_content_chars
        self._clock = clock
        self._timer_factory = timer_factory
        self._uuid_factory = uuid_factory or (lambda: str(uuid_lib.uuid4()))

        self._state_lock = threading.RLock()
        self._send_lock = threading.Lock()

        self._card_id: str | None = None
        self._sequence = 0
        self._terminal_state: CardRunState | None = None

        self._status_text = "⏳ 正在思考..."
        self._initial_placeholder = "_等待模型响应中..._"
        self._text_parts: list[str] = []
        self._thought_parts: list[str] = []
        self._usage: dict[str, Any] | None = None

        self._status_dirty = False
        self._content_dirty = False
        self._last_sent_status: str | None = None
        self._last_sent_content: str | None = None
        self._last_flush_at: float | None = None
        self._flush_timer: Any = None
        self._last_card_json: dict[str, Any] | None = None

    @property
    def card_id(self) -> str | None:
        """返回已创建的飞书卡片实体 ID。"""
        with self._state_lock:
            return self._card_id

    @property
    def is_finished(self) -> bool:
        """返回当前卡片是否已完成终态收口。"""
        with self._state_lock:
            return self._terminal_state is not None

    @property
    def terminal_state(self) -> CardRunState | None:
        """返回卡片终态枚举；未收口时为 None。"""
        with self._state_lock:
            return self._terminal_state

    @property
    def accumulated_text(self) -> str:
        """返回当前累积的助手正文输出。"""
        with self._state_lock:
            return "".join(self._text_parts)

    def start(
        self,
        *,
        initial_status: str = "⏳ 正在思考...",
        initial_content: str = "_等待模型响应中..._",
    ) -> str:
        """创建初始流式 CardKit 2.0 卡片实体并记录限流起点时间。"""
        with self._send_lock:
            with self._state_lock:
                if self._card_id is not None:
                    raise RuntimeError("卡片已启动，不能重复调用 start()")
                self._status_text = initial_status
                self._initial_placeholder = initial_content
                card_json = build_streaming_card_json(
                    session_id=self.session_id,
                    status_text=initial_status,
                    content_text=initial_content,
                    state=CardRunState.RUNNING,
                    title=self.title,
                    subtitle=self.subtitle,
                    turn_id=self.turn_id,
                )

            card_id = self.transport.create_card(card_json)

            with self._state_lock:
                self._card_id = card_id
                self._last_sent_status = initial_status
                self._last_sent_content = initial_content
                self._last_flush_at = self._clock()
                self._last_card_json = card_json
            return card_id

    def handle_update(self, update: AcpSessionUpdate) -> None:
        """消费 ACP session/update 增量事件，按时间窗口节流刷新卡片。"""
        update_type = update.update_type
        if update_type == "agent_message_chunk":
            chunk = update.text
            if chunk:
                self.append_text(chunk)
            return

        if update_type == "agent_thought_chunk":
            chunk = update.text
            if chunk:
                self.append_thought(chunk)
            return

        if update_type in {"tool_call", "tool_call_update"}:
            tool_name = update.tool_name or "tool"
            tool_status = (update.tool_status or "").lower()
            if tool_status == "completed":
                self.set_status(f"✅ 工具完成：`{tool_name}`")
            elif tool_status in {"failed", "error"}:
                self.set_status(f"⚠️ 工具异常：`{tool_name}`")
            else:
                self.set_status(f"🔧 正在执行：`{tool_name}`")
            return

        if update_type == "user_message_chunk":
            with self._state_lock:
                already_started = bool(self._text_parts or self._thought_parts)
            if already_started:
                self.set_status("🧭 收到插话指令，正在调整处理...")

    def append_text(self, chunk: str) -> None:
        """追加模型回复正文增量并触发节流调度。"""
        if not chunk:
            return
        with self._state_lock:
            if self._terminal_state is not None:
                return
            self._text_parts.append(chunk)
            self._content_dirty = True
            if self._status_text != "✍️ 正在生成回复...":
                self._status_text = "✍️ 正在生成回复..."
                self._status_dirty = True
            should_flush_now = self._schedule_or_check_immediate_locked()
        if should_flush_now:
            self._flush_to_transport()

    def append_thought(self, chunk: str) -> None:
        """记录思考增量并在状态栏反映思考进度。"""
        if not chunk:
            return
        with self._state_lock:
            if self._terminal_state is not None:
                return
            self._thought_parts.append(chunk)
            if not self._text_parts and self._status_text != "💭 正在深度思考...":
                self._status_text = "💭 正在深度思考..."
                self._status_dirty = True
                should_flush_now = self._schedule_or_check_immediate_locked()
            else:
                should_flush_now = False
        if should_flush_now:
            self._flush_to_transport()

    def set_status(self, status_text: str) -> None:
        """更新卡片顶部状态栏文案并触发节流调度。"""
        with self._state_lock:
            if self._terminal_state is not None:
                return
            if status_text == self._status_text:
                return
            self._status_text = status_text
            self._status_dirty = True
            should_flush_now = self._schedule_or_check_immediate_locked()
        if should_flush_now:
            self._flush_to_transport()

    def mark_stopping(self) -> None:
        """收到用户停止操作后立即将状态栏切换为停止中提示。"""
        self.set_status("🛑 正在停止当前任务...")

    def flush(self) -> None:
        """忽略节流窗口，立即将当前缓冲状态与正文推送到卡片。"""
        with self._state_lock:
            if self._card_id is None or self._terminal_state is not None:
                return
            self._cancel_timer_locked()
        self._flush_to_transport()

    def finish_completed(
        self,
        *,
        final_text: str | None = None,
        usage: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """将卡片收口为已完成终态（绿色顶栏、关闭流式并移除停止按钮）。"""
        usage_suffix = _format_usage_suffix(usage if usage is not None else self._usage)
        return self._finalize(
            CardRunState.COMPLETED,
            status_text=f"✅ 已完成{usage_suffix}",
            override_text=final_text,
            empty_fallback="_（已完成，暂无文本输出）_",
            usage=usage,
        )

    def finish_cancelled(
        self,
        *,
        final_text: str | None = None,
        usage: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """将卡片收口为已取消终态（橙色顶栏、保留已输出正文并移除停止按钮）。"""
        usage_suffix = _format_usage_suffix(usage if usage is not None else self._usage)
        return self._finalize(
            CardRunState.CANCELLED,
            status_text=f"⏹️ 已停止生成{usage_suffix}",
            override_text=final_text,
            empty_fallback="_（已停止生成，暂无已输出内容）_",
            usage=usage,
        )

    def finish_error(self, error_message: str) -> dict[str, Any]:
        """将卡片收口为异常失败终态（红色顶栏、展示错误信息并移除停止按钮）。"""
        reason = error_message.strip() or "未知错误"
        return self._finalize(
            CardRunState.ERROR,
            status_text=f"❌ 执行失败：{reason}",
            empty_fallback="_（任务执行异常中断，未生成正文内容）_",
        )

    def finalize_from_result(self, result: AcpPromptResult) -> dict[str, Any]:
        """根据 ACP 回合结果自动选择完成或取消终态收口。"""
        if result.is_cancelled:
            return self.finish_cancelled(final_text=result.text or None, usage=result.usage)
        return self.finish_completed(final_text=result.text or None, usage=result.usage)

    def _schedule_or_check_immediate_locked(self) -> bool:
        if self._card_id is None or self._terminal_state is not None:
            return False
        if not self._status_dirty and not self._content_dirty:
            return False

        now = self._clock()
        if self._last_flush_at is None or self.throttle_seconds == 0.0:
            self._cancel_timer_locked()
            return True

        elapsed = now - self._last_flush_at
        if elapsed >= self.throttle_seconds:
            self._cancel_timer_locked()
            return True

        if self._flush_timer is None:
            delay = max(0.0, self.throttle_seconds - elapsed)
            timer = self._timer_factory(delay, self._on_flush_timer)
            if hasattr(timer, "daemon"):
                timer.daemon = True
            self._flush_timer = timer
            timer.start()
        return False

    def _cancel_timer_locked(self) -> None:
        if self._flush_timer is not None:
            self._flush_timer.cancel()
            self._flush_timer = None

    def _on_flush_timer(self) -> None:
        with self._state_lock:
            self._flush_timer = None
            if self._card_id is None or self._terminal_state is not None:
                return
        try:
            self._flush_to_transport()
        except Exception:
            logger.exception("卡片流式定时刷新失败 (card_id=%s)", self._card_id)

    def _next_sequence_locked(self) -> int:
        self._sequence += 1
        return self._sequence

    def _render_content_text_locked(self) -> str:
        raw = "".join(self._text_parts)
        if not raw:
            return self._initial_placeholder
        return _truncate_markdown_preserving_prefix(raw, self.max_content_chars)

    def _flush_to_transport(self) -> None:
        with self._send_lock:
            with self._state_lock:
                if self._card_id is None or self._terminal_state is not None:
                    return
                card_id = self._card_id
                status_candidate = self._status_text if self._status_dirty else None
                content_candidate = (
                    self._render_content_text_locked() if self._content_dirty else None
                )

                if status_candidate == self._last_sent_status:
                    status_candidate = None
                if content_candidate == self._last_sent_content:
                    content_candidate = None

                self._status_dirty = False
                self._content_dirty = False

                if status_candidate is None and content_candidate is None:
                    return

                self._last_flush_at = self._clock()
                status_seq = (
                    self._next_sequence_locked() if status_candidate is not None else None
                )
                content_seq = (
                    self._next_sequence_locked() if content_candidate is not None else None
                )

            if status_candidate is not None and status_seq is not None:
                try:
                    self.transport.stream_element_content(
                        card_id,
                        STATUS_ELEMENT_ID,
                        status_candidate,
                        sequence=status_seq,
                        uuid=self._uuid_factory(),
                    )
                except Exception:
                    with self._state_lock:
                        if self._terminal_state is None:
                            self._status_dirty = True
                    raise
                with self._state_lock:
                    self._last_sent_status = status_candidate

            if content_candidate is not None and content_seq is not None:
                try:
                    self.transport.stream_element_content(
                        card_id,
                        CONTENT_ELEMENT_ID,
                        content_candidate,
                        sequence=content_seq,
                        uuid=self._uuid_factory(),
                    )
                except Exception:
                    with self._state_lock:
                        if self._terminal_state is None:
                            self._content_dirty = True
                    raise
                with self._state_lock:
                    self._last_sent_content = content_candidate

    def _finalize(
        self,
        state: CardRunState,
        *,
        status_text: str,
        empty_fallback: str,
        override_text: str | None = None,
        usage: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._send_lock:
            with self._state_lock:
                if self._card_id is None:
                    raise RuntimeError("卡片尚未创建，请先调用 start()")
                if self._terminal_state is not None and self._last_card_json is not None:
                    return self._last_card_json

                self._cancel_timer_locked()
                self._terminal_state = state
                if usage is not None:
                    self._usage = usage

                if override_text is not None:
                    raw_body = override_text
                else:
                    raw_body = "".join(self._text_parts)

                body_text = (
                    _truncate_markdown_preserving_prefix(raw_body, self.max_content_chars)
                    if raw_body
                    else empty_fallback
                )
                self._status_text = status_text
                self._status_dirty = False
                self._content_dirty = False

                final_card = build_streaming_card_json(
                    session_id=self.session_id,
                    status_text=status_text,
                    content_text=body_text,
                    state=state,
                    title=self.title,
                    subtitle=self.subtitle,
                    turn_id=self.turn_id,
                    summary_text=status_text,
                    show_stop_button=False,
                )
                card_id = self._card_id
                settings_seq = self._next_sequence_locked()
                card_seq = self._next_sequence_locked()
                self._last_sent_status = status_text
                self._last_sent_content = body_text
                self._last_card_json = final_card

            self.transport.update_settings(
                card_id,
                {"config": {"streaming_mode": False}},
                sequence=settings_seq,
                uuid=self._uuid_factory(),
            )
            self.transport.update_card(
                card_id,
                final_card,
                sequence=card_seq,
                uuid=self._uuid_factory(),
            )
            return final_card
