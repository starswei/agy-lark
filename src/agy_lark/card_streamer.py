"""Feishu CardKit 2.0 流式交互卡片模板构建、回调载荷生成与时间窗口节流更新器。"""

from __future__ import annotations

from collections.abc import Callable
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
