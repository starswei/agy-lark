"""基于 lark-oapi WebSocket 长连接的飞书 ↔ Antigravity 单进程守护服务。"""

from __future__ import annotations

import argparse
from collections import OrderedDict
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
import importlib
import json
import logging
import os
from pathlib import Path
import signal
import sys
import threading
import time
from typing import Any, Protocol
import uuid as uuid_lib
import warnings

from .acp_client import AcpPromptResult, AcpSessionUpdate, AgyAcpClient
from .card_streamer import (
    ACP_EFFORT_CONFIG_ID,
    ACP_MODEL_CONFIG_ID,
    AcpEffortOption,
    AcpModelOption,
    CardTransport,
    StreamingCardUpdater,
    build_create_card_payload,
    build_effort_selector_card_json,
    build_model_selector_card_json,
    build_stream_content_payload,
    build_update_card_payload,
    build_update_settings_payload,
    has_config_option,
    parse_available_efforts,
    parse_available_models,
    parse_current_effort_id,
    parse_current_model_id,
    parse_effort_select_callback_value,
    parse_model_select_callback_value,
    parse_stop_callback_value,
)
from .config import AgyLarkConfig, load_config
from .session_store import (
    DEFAULT_MAPPING_CAPACITY,
    DEFAULT_SESSIONS_FILENAME,
    JsonSessionStore,
    PersistedConversation,
    PersistedSessionState,
    SessionStore,
)

logger = logging.getLogger(__name__)

_DEFAULT_DEDUPE_CAPACITY = DEFAULT_MAPPING_CAPACITY
_DEFAULT_MAX_WORKERS = 8
_TURN_READY_WAIT_SECONDS = 15.0


class LarkApiError(RuntimeError):
    """飞书开放平台 OpenAPI 调用异常。"""

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        log_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.log_id = log_id

    def __str__(self) -> str:
        parts: list[str] = []
        if self.code is not None:
            parts.append(f"[{self.code}]")
        parts.append(self.message)
        if self.log_id:
            parts.append(f"(log_id={self.log_id})")
        return " ".join(parts)


class MessageSender(Protocol):
    """飞书 IM 消息发送与回复适配器协议。"""

    def send_card_message(
        self,
        *,
        chat_id: str,
        card_id: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> str | None:
        """发送或回复关联 CardKit 2.0 实体的交互卡片消息，返回新消息 ID。"""

    def send_text_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> str | None:
        """发送或回复纯文本提示消息，返回新消息 ID。"""


class AcpClientProtocol(Protocol):
    """守护服务依赖的 ACP 客户端最小协议（便于解耦与测试替身注入）。"""

    @property
    def is_running(self) -> bool:
        """返回底层 ACP 运行时是否存活。"""

    def start(self, *, timeout: float | None = None) -> dict[str, Any]:
        """启动 ACP 运行时并完成初始化握手。"""

    def close(self) -> None:
        """优雅关闭 ACP 运行时与子进程管道。"""

    def new_session(
        self,
        cwd: str | Path | None = None,
        mcp_servers: Sequence[dict[str, Any]] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """创建新的 ACP 会话并返回包含 sessionId 的元数据字典。"""

    def resume_session(
        self,
        session_id: str,
        cwd: str | Path | None = None,
        mcp_servers: Sequence[dict[str, Any]] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """恢复已有 ACP 会话（`session/resume`）并返回包含 sessionId 的元数据字典。"""

    def prompt_async(
        self,
        session_id: str,
        prompt: str | Sequence[dict[str, Any]],
        *,
        on_update: Callable[[AcpSessionUpdate], None] | None = None,
        message_id: str | None = None,
    ) -> Future[AcpPromptResult]:
        """异步发送 prompt 或运行中插话请求，返回承载回合结果的 Future。"""

    def set_model(
        self,
        session_id: str,
        model_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """为指定 ACP 会话切换当前使用的模型标识。"""

    def set_config_option(
        self,
        session_id: str,
        config_id: str,
        value: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """更新指定 ACP 会话的配置项（如 `model` 或 `thought_level`）。"""

    def close_session(
        self,
        session_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """关闭指定 ACP 会话并释放服务端资源。"""

    def cancel(self, session_id: str) -> None:
        """向指定会话下发协议级 session/cancel 中止通知。"""


@dataclass(frozen=True)
class IncomingMention:
    """规范化后的飞书消息 @mention 描述。"""

    key: str = ""
    open_id: str | None = None
    name: str | None = None
    mentioned_type: int | None = None


@dataclass(frozen=True)
class IncomingMessage:
    """从 im.message.receive_v1 事件解析出的有效文本消息。"""

    message_id: str
    chat_id: str
    chat_type: str
    sender_open_id: str
    text: str
    raw_text: str
    root_id: str | None = None
    parent_id: str | None = None
    thread_id: str | None = None
    mentions: tuple[IncomingMention, ...] = ()
    is_bot_mentioned: bool = False


def _read_field(obj: Any, name: str) -> Any:
    """兼容从 SDK 对象属性或字典键读取字段。"""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _read_str(obj: Any, name: str) -> str:
    """读取字符串字段并去除首尾空白；缺失或非字符串时返回空串。"""
    value = _read_field(obj, name)
    return value.strip() if isinstance(value, str) else ""


def _ensure_lark_response_ok(response: Any, action: str) -> None:
    """校验飞书 SDK 响应是否成功；失败时抛出含错误码与 log_id 的 LarkApiError。"""
    success_fn = getattr(response, "success", None)
    is_ok = bool(success_fn()) if callable(success_fn) else bool(response)
    if is_ok:
        return

    raw_code = getattr(response, "code", None)
    code = raw_code if isinstance(raw_code, int) else None
    raw_msg = getattr(response, "msg", None)
    msg = raw_msg.strip() if isinstance(raw_msg, str) and raw_msg.strip() else "未知飞书接口错误"
    get_log_id = getattr(response, "get_log_id", None)
    log_id = get_log_id() if callable(get_log_id) else None
    raise LarkApiError(
        f"{action}失败: {msg}",
        code=code,
        log_id=log_id if isinstance(log_id, str) and log_id else None,
    )


def _import_lark_oapi() -> Any:
    """延迟导入 lark_oapi，并仅隔离该第三方 SDK 在导入阶段触发的已知 DeprecationWarning。"""
    if "lark_oapi" in sys.modules:
        return sys.modules["lark_oapi"]
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            module=r"^lark_oapi(\.|$)",
        )
        # lark_oapi.ws.pb.google 导入时会调用 pkg_resources.declare_namespace，
        # 该弃用警告归属 module="pkg_resources"，需同时在此短暂导入窗口内隔离。
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            module=r"^pkg_resources(\.|$)",
        )
        import lark_oapi as lark
    return lark


def _resolve_lark_log_level(level_name: str, log_level_enum: Any) -> Any:
    """将配置中的日志级别字符串映射为 lark_oapi.LogLevel 枚举。"""
    normalized = (level_name or "INFO").strip().upper()
    mapping = {
        "DEBUG": log_level_enum.DEBUG,
        "INFO": log_level_enum.INFO,
        "WARNING": log_level_enum.WARNING,
        "WARN": log_level_enum.WARNING,
        "ERROR": log_level_enum.ERROR,
        "CRITICAL": log_level_enum.CRITICAL,
    }
    return mapping.get(normalized, log_level_enum.INFO)


class LarkCardTransport:
    """基于 lark-oapi cardkit.v1 的 CardTransport 协议实现。"""

    def __init__(
        self,
        client: Any,
        *,
        uuid_factory: Callable[[], str] | None = None,
    ) -> None:
        self._client = client
        self._uuid_factory = uuid_factory or (lambda: str(uuid_lib.uuid4()))

    def create_card(self, card_json: dict[str, Any]) -> str:
        """调用 cardkit.v1.card.create 创建 CardKit 2.0 卡片实体并返回 card_id。"""
        _import_lark_oapi()
        from lark_oapi.api.cardkit.v1 import CreateCardRequest, CreateCardRequestBody

        payload = build_create_card_payload(card_json)
        body = (
            CreateCardRequestBody.builder()
            .type(payload["type"])
            .data(payload["data"])
            .build()
        )
        request = CreateCardRequest.builder().request_body(body).build()
        response = self._client.cardkit.v1.card.create(request)
        _ensure_lark_response_ok(response, "创建卡片实体")

        data = getattr(response, "data", None)
        card_id = getattr(data, "card_id", None)
        if not isinstance(card_id, str) or not card_id.strip():
            get_log_id = getattr(response, "get_log_id", None)
            log_id = get_log_id() if callable(get_log_id) else None
            raise LarkApiError("创建卡片成功但响应缺少 card_id", log_id=log_id)
        return card_id.strip()

    def stream_element_content(
        self,
        card_id: str,
        element_id: str,
        content: str,
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        """调用 cardkit.v1.card_element.content 推送流式 Markdown 组件全量文本。"""
        _import_lark_oapi()
        from lark_oapi.api.cardkit.v1 import (
            ContentCardElementRequest,
            ContentCardElementRequestBody,
        )

        payload = build_stream_content_payload(
            content,
            sequence=sequence,
            uuid=uuid or self._uuid_factory(),
        )
        body_builder = (
            ContentCardElementRequestBody.builder()
            .content(payload["content"])
            .sequence(payload["sequence"])
        )
        if "uuid" in payload:
            body_builder = body_builder.uuid(payload["uuid"])

        request = (
            ContentCardElementRequest.builder()
            .card_id(card_id)
            .element_id(element_id)
            .request_body(body_builder.build())
            .build()
        )
        response = self._client.cardkit.v1.card_element.content(request)
        _ensure_lark_response_ok(response, f"流式更新卡片组件 {element_id}")

    def update_card(
        self,
        card_id: str,
        card_json: dict[str, Any],
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        """调用 cardkit.v1.card.update 全量更新卡片结构（通过 Card 对象构造）。"""
        _import_lark_oapi()
        from lark_oapi.api.cardkit.v1 import Card, UpdateCardRequest, UpdateCardRequestBody

        payload = build_update_card_payload(
            card_json,
            sequence=sequence,
            uuid=uuid or self._uuid_factory(),
        )
        card_spec = payload["card"]
        card_obj = (
            Card.builder()
            .type(card_spec["type"])
            .data(card_spec["data"])
            .build()
        )
        body_builder = (
            UpdateCardRequestBody.builder()
            .card(card_obj)
            .sequence(payload["sequence"])
        )
        if "uuid" in payload:
            body_builder = body_builder.uuid(payload["uuid"])

        request = (
            UpdateCardRequest.builder()
            .card_id(card_id)
            .request_body(body_builder.build())
            .build()
        )
        response = self._client.cardkit.v1.card.update(request)
        _ensure_lark_response_ok(response, "全量更新卡片")

    def update_settings(
        self,
        card_id: str,
        settings: dict[str, Any],
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        """调用 cardkit.v1.card.settings 更新卡片配置（如关闭 streaming_mode）。"""
        _import_lark_oapi()
        from lark_oapi.api.cardkit.v1 import SettingsCardRequest, SettingsCardRequestBody

        payload = build_update_settings_payload(
            settings,
            sequence=sequence,
            uuid=uuid or self._uuid_factory(),
        )
        body_builder = (
            SettingsCardRequestBody.builder()
            .settings(payload["settings"])
            .sequence(payload["sequence"])
        )
        if "uuid" in payload:
            body_builder = body_builder.uuid(payload["uuid"])

        request = (
            SettingsCardRequest.builder()
            .card_id(card_id)
            .request_body(body_builder.build())
            .build()
        )
        response = self._client.cardkit.v1.card.settings(request)
        _ensure_lark_response_ok(response, "更新卡片配置")


class LarkMessageSender:
    """基于 lark-oapi im.v1 的飞书消息发送与回复适配器。"""

    def __init__(
        self,
        client: Any,
        *,
        uuid_factory: Callable[[], str] | None = None,
    ) -> None:
        self._client = client
        self._uuid_factory = uuid_factory or (lambda: str(uuid_lib.uuid4()))

    def send_card_message(
        self,
        *,
        chat_id: str,
        card_id: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> str | None:
        """发送或回复关联 card_id 的 CardKit 2.0 交互卡片消息。"""
        content = json.dumps(
            {"type": "card", "data": {"card_id": card_id}},
            ensure_ascii=False,
        )
        return self._send(
            chat_id=chat_id,
            msg_type="interactive",
            content=content,
            reply_to_message_id=reply_to_message_id,
            reply_in_thread=reply_in_thread,
            action="发送交互卡片消息",
        )

    def send_text_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> str | None:
        """发送或回复纯文本消息。"""
        content = json.dumps({"text": text}, ensure_ascii=False)
        return self._send(
            chat_id=chat_id,
            msg_type="text",
            content=content,
            reply_to_message_id=reply_to_message_id,
            reply_in_thread=reply_in_thread,
            action="发送文本消息",
        )

    def _send(
        self,
        *,
        chat_id: str,
        msg_type: str,
        content: str,
        reply_to_message_id: str | None,
        reply_in_thread: bool,
        action: str,
    ) -> str | None:
        _import_lark_oapi()
        from lark_oapi.api.im.v1 import (
            CreateMessageRequest,
            CreateMessageRequestBody,
            ReplyMessageRequest,
            ReplyMessageRequestBody,
        )

        req_uuid = self._uuid_factory()
        if reply_to_message_id:
            body_builder = (
                ReplyMessageRequestBody.builder()
                .msg_type(msg_type)
                .content(content)
                .uuid(req_uuid)
            )
            if reply_in_thread:
                body_builder = body_builder.reply_in_thread(True)
            request = (
                ReplyMessageRequest.builder()
                .message_id(reply_to_message_id)
                .request_body(body_builder.build())
                .build()
            )
            response = self._client.im.v1.message.reply(request)
        else:
            body = (
                CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type(msg_type)
                .content(content)
                .uuid(req_uuid)
                .build()
            )
            request = (
                CreateMessageRequest.builder()
                .receive_id_type("chat_id")
                .request_body(body)
                .build()
            )
            response = self._client.im.v1.message.create(request)

        _ensure_lark_response_ok(response, action)
        data = getattr(response, "data", None)
        msg_id = getattr(data, "message_id", None)
        return msg_id.strip() if isinstance(msg_id, str) and msg_id.strip() else None


def build_feishu_client(config: AgyLarkConfig) -> Any:
    """根据运行时配置创建同步 lark_oapi.Client（延迟导入 SDK）。"""
    lark = _import_lark_oapi()

    log_level = _resolve_lark_log_level(config.log_level, lark.LogLevel)
    return (
        lark.Client.builder()
        .app_id(config.feishu_app_id)
        .app_secret(config.feishu_app_secret)
        .domain(config.feishu_domain)
        .log_level(log_level)
        .build()
    )


def build_event_dispatcher(
    daemon: AgyLarkDaemon,
    *,
    encrypt_key: str = "",
    verification_token: str = "",
) -> Any:
    """构造注册了消息事件与卡片回调的 lark_oapi.EventDispatcherHandler。"""
    lark = _import_lark_oapi()
    from lark_oapi.event.callback.model.p2_card_action_trigger import (
        P2CardActionTriggerResponse,
    )

    def _on_message_receive(data: Any) -> None:
        daemon.handle_message_event(data)

    def _on_card_action(data: Any) -> P2CardActionTriggerResponse:
        payload = daemon.handle_card_action(data)
        if payload:
            return P2CardActionTriggerResponse(payload)
        return P2CardActionTriggerResponse()

    return (
        lark.EventDispatcherHandler.builder(encrypt_key, verification_token)
        .register_p2_im_message_receive_v1(_on_message_receive)
        .register_p2_card_action_trigger(_on_card_action)
        .build()
    )


def build_ws_client(config: AgyLarkConfig, event_handler: Any) -> Any:
    """延迟导入 lark_oapi.ws 并构建长连接客户端，避免模块导入期触发事件循环警告。"""
    lark = _import_lark_oapi()
    lark_ws = importlib.import_module("lark_oapi.ws")

    log_level = _resolve_lark_log_level(config.log_level, lark.LogLevel)
    return lark_ws.Client(
        config.feishu_app_id,
        config.feishu_app_secret,
        log_level=log_level,
        event_handler=event_handler,
        domain=config.feishu_domain,
        auto_reconnect=True,
    )


def _parse_mentions(raw_mentions: Any) -> tuple[IncomingMention, ...]:
    """将飞书事件中的 mentions 列表规范化为不可变元组。"""
    if not isinstance(raw_mentions, Sequence) or isinstance(raw_mentions, (str, bytes)):
        return ()
    parsed: list[IncomingMention] = []
    for item in raw_mentions:
        key = _read_str(item, "key")
        user_id = _read_field(item, "id")
        open_id = _read_str(user_id, "open_id") or None
        name = _read_str(item, "name") or None
        raw_type = _read_field(item, "mentioned_type")
        mentioned_type = raw_type if isinstance(raw_type, int) else None
        if key or open_id or name:
            parsed.append(
                IncomingMention(
                    key=key,
                    open_id=open_id,
                    name=name,
                    mentioned_type=mentioned_type,
                )
            )
    return tuple(parsed)


def _strip_mentions_from_text(
    raw_text: str,
    mentions: tuple[IncomingMention, ...],
    *,
    bot_open_id: str | None = None,
) -> str:
    """清理文本中的飞书 @占位符，仅保留用户真实指令正文。"""
    cleaned = raw_text
    if not mentions:
        return cleaned.strip()

    normalized_bot_id = bot_open_id.strip() if isinstance(bot_open_id, str) and bot_open_id.strip() else None
    for idx, mention in enumerate(mentions):
        if not mention.key or mention.key == "@_all":
            continue
        if normalized_bot_id is not None:
            if mention.open_id == normalized_bot_id:
                cleaned = cleaned.replace(mention.key, "")
            elif mention.name:
                cleaned = cleaned.replace(mention.key, f"@{mention.name}")
        else:
            # 未显式指定 bot_open_id 时，默认首个 mention 为被唤醒的机器人占位符，其余恢复可读昵称
            if idx == 0 or not mention.name:
                cleaned = cleaned.replace(mention.key, "")
            else:
                cleaned = cleaned.replace(mention.key, f"@{mention.name}")
    return " ".join(cleaned.split()) if "\n" not in cleaned else "\n".join(
        line.strip() for line in cleaned.splitlines() if line.strip()
    )


def extract_incoming_message(
    raw_event: Any,
    *,
    bot_open_id: str | None = None,
) -> IncomingMessage | None:
    """从 P2ImMessageReceiveV1 事件提取并过滤有效的用户纯文本消息。"""
    event = _read_field(raw_event, "event")
    if event is None and (
        _read_field(raw_event, "message") is not None
        or _read_field(raw_event, "sender") is not None
    ):
        event = raw_event
    if event is None:
        return None

    sender = _read_field(event, "sender")
    sender_type = _read_str(sender, "sender_type").lower()
    if sender_type and sender_type != "user":
        return None

    sender_id = _read_field(sender, "sender_id")
    sender_open_id = _read_str(sender_id, "open_id")

    message = _read_field(event, "message")
    if message is None:
        return None

    message_id = _read_str(message, "message_id")
    chat_id = _read_str(message, "chat_id")
    chat_type = (_read_str(message, "chat_type") or "p2p").lower()
    message_type = _read_str(message, "message_type").lower()
    if not message_id or not chat_id or message_type != "text":
        return None

    raw_content = _read_field(message, "content")
    content_dict: dict[str, Any] | None = None
    if isinstance(raw_content, str):
        try:
            loaded = json.loads(raw_content)
        except json.JSONDecodeError:
            return None
        if isinstance(loaded, dict):
            content_dict = loaded
    elif isinstance(raw_content, dict):
        content_dict = raw_content

    if content_dict is None:
        return None

    raw_text = content_dict.get("text")
    if not isinstance(raw_text, str):
        return None

    mentions = _parse_mentions(_read_field(message, "mentions"))
    normalized_bot_id = bot_open_id.strip() if isinstance(bot_open_id, str) and bot_open_id.strip() else None
    if normalized_bot_id is not None:
        is_bot_mentioned = any(m.open_id == normalized_bot_id for m in mentions)
    else:
        is_bot_mentioned = any(m.key != "@_all" for m in mentions)

    clean_text = _strip_mentions_from_text(
        raw_text,
        mentions,
        bot_open_id=normalized_bot_id,
    )
    if not clean_text:
        return None

    return IncomingMessage(
        message_id=message_id,
        chat_id=chat_id,
        chat_type=chat_type,
        sender_open_id=sender_open_id,
        text=clean_text,
        raw_text=raw_text,
        root_id=_read_str(message, "root_id") or None,
        parent_id=_read_str(message, "parent_id") or None,
        thread_id=_read_str(message, "thread_id") or None,
        mentions=mentions,
        is_bot_mentioned=is_bot_mentioned,
    )


class _BoundedOrderedDict:
    """线程外部加锁使用的固定上限有序字典，防止长期运行内存无限增长。"""

    def __init__(self, max_size: int = _DEFAULT_DEDUPE_CAPACITY) -> None:
        self._max_size = max(1, max_size)
        self._items: OrderedDict[str, Any] = OrderedDict()

    def get(self, key: str, default: Any = None) -> Any:
        return self._items.get(key, default)

    def put(self, key: str, value: Any = True) -> None:
        if key in self._items:
            self._items.move_to_end(key)
        self._items[key] = value
        while len(self._items) > self._max_size:
            self._items.popitem(last=False)

    def add_if_absent(self, key: str) -> bool:
        """若键不存在则插入并返回 True；已存在则返回 False。"""
        if key in self._items:
            return False
        self.put(key, True)
        return True

    def items_tuple(self) -> tuple[tuple[str, Any], ...]:
        """按从旧到新顺序返回当前有序键值元组快照。"""
        return tuple(self._items.items())

    def load_items(self, pairs: Iterable[tuple[str, Any]]) -> None:
        """清空并用给定有序键值序列重新填充有界字典。"""
        self._items.clear()
        for key, value in pairs:
            self.put(key, value)

    def clear(self) -> None:
        self._items.clear()


@dataclass
class _ActiveTurn:
    """跟踪单个在途回合的卡片更新器、关联 Future 与插话接收状态。"""

    turn_id: str
    session_id: str
    updater: StreamingCardUpdater
    lock: threading.Lock = field(default_factory=threading.Lock)
    ready_event: threading.Event = field(default_factory=threading.Event)
    done_event: threading.Event = field(default_factory=threading.Event)
    futures: list[Future[AcpPromptResult]] = field(default_factory=list)
    pending_steer_count: int = 0
    accepting_steers: bool = True
    cancelled_requested: bool = False


@dataclass
class _ConversationEntry:
    """维护单个飞书会话（私聊或群聊话题）到稳定 ACP 会话的绑定。"""

    conv_key: str
    dispatch_lock: threading.RLock = field(default_factory=threading.RLock)
    session_id: str | None = None
    persisted_cwd: str | None = None
    session_attached: bool = False
    active_turn: _ActiveTurn | None = None
    uses_config_options: bool = False
    model_id: str | None = None
    available_models: tuple[AcpModelOption, ...] = ()
    effort_id: str | None = None
    available_efforts: tuple[AcpEffortOption, ...] = ()


@dataclass(frozen=True)
class _EnsureSessionResult:
    """按需建立或恢复 ACP 会话的返回结果（含降级/回退诊断通知）。"""

    session_id: str | None
    error: str | None = None
    notice: str | None = None


def _format_workspace_path(path: str | Path) -> str:
    """将工作区路径规范化为可持久化存储的绝对路径字符串。"""
    return str(Path(os.path.expanduser(str(path))).resolve())


def _normalize_workspace_compare_key(path: str | Path | None) -> str | None:
    """将工作区路径规范化为跨平台可比对键。"""
    if path is None:
        return None
    raw = str(path).strip()
    if not raw:
        return None
    return os.path.normcase(_format_workspace_path(raw))


def _workspaces_match(left: str | Path | None, right: str | Path | None) -> bool:
    """校验持久化历史 cwd 与当前工作区路径是否一致。"""
    left_key = _normalize_workspace_compare_key(left)
    right_key = _normalize_workspace_compare_key(right)
    return left_key is not None and right_key is not None and left_key == right_key


@dataclass
class _SelectorCardRecord:
    """记录 `/model` 或 `/effort` 交互卡片对应的 CardKit 实体与递增序号状态。"""

    kind: str
    card_id: str
    conv_key: str
    models: tuple[AcpModelOption, ...] = ()
    efforts: tuple[AcpEffortOption, ...] = ()
    current_model_name: str | None = None
    sequence: int = 0
    update_lock: threading.RLock = field(default_factory=threading.RLock)


class AgyLarkDaemon:
    """单进程飞书 ↔ Antigravity 守护服务，统筹鉴权、会话路由、流式卡片与取消控制。"""

    def __init__(
        self,
        config: AgyLarkConfig,
        *,
        acp_client: AcpClientProtocol | None = None,
        transport: CardTransport | None = None,
        message_sender: MessageSender | None = None,
        session_store: SessionStore | None = None,
        lark_client: Any = None,
        ws_client: Any = None,
        bot_open_id: str | None = None,
        max_workers: int = _DEFAULT_MAX_WORKERS,
        updater_factory: Callable[..., StreamingCardUpdater] | None = None,
        uuid_factory: Callable[[], str] | None = None,
    ) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers 必须为正整数")

        self.config = config
        self.bot_open_id = bot_open_id.strip() if isinstance(bot_open_id, str) and bot_open_id.strip() else None
        self._uuid_factory = uuid_factory or (lambda: str(uuid_lib.uuid4()))
        self._updater_factory = updater_factory or StreamingCardUpdater

        self._lark_client = lark_client
        self._ws_client = ws_client
        self._transport = transport
        self._message_sender = message_sender
        self.session_store: SessionStore = (
            session_store
            if session_store is not None
            else JsonSessionStore(
                config.data_dir / DEFAULT_SESSIONS_FILENAME,
                max_mapping_entries=_DEFAULT_DEDUPE_CAPACITY,
            )
        )

        if acp_client is not None:
            self.acp_client: AcpClientProtocol = acp_client
        else:
            self.acp_client = AgyAcpClient(
                executable=config.agy_exe or None,
                cwd=config.default_workspace,
                default_timeout=float(config.turn_timeout_seconds),
            )

        self._lock = threading.RLock()
        self._acp_start_lock = threading.Lock()
        self._persist_lock = threading.Lock()
        self._state_version = 0
        self._persisted_version = 0
        self._seen_messages = _BoundedOrderedDict(_DEFAULT_DEDUPE_CAPACITY)
        self._thread_aliases = _BoundedOrderedDict(_DEFAULT_DEDUPE_CAPACITY)
        self._reply_message_keys = _BoundedOrderedDict(_DEFAULT_DEDUPE_CAPACITY)
        self._selector_cards = _BoundedOrderedDict(_DEFAULT_DEDUPE_CAPACITY)
        self._conversations: dict[str, _ConversationEntry] = {}
        self._persisted_conversations: dict[str, PersistedConversation] = {}
        self._session_entries: dict[str, _ConversationEntry] = {}
        self._active_turns: dict[str, _ActiveTurn] = {}
        self._default_available_models: tuple[AcpModelOption, ...] = ()

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="agy-lark-worker",
        )
        self._closing = False
        self._closed = False
        self._load_persisted_state()

    def _load_persisted_state(self) -> None:
        """启动时仅将持久化会话与群聊路由载入内存，保留未附着态以待首轮消息按需恢复。"""
        state = self.session_store.load()
        with self._lock:
            self._thread_aliases.load_items(state.thread_aliases)
            self._reply_message_keys.load_items(state.reply_message_keys)
            for conv_key, item in state.conversations.items():
                self._persisted_conversations[conv_key] = item
                self._conversations[conv_key] = _ConversationEntry(
                    conv_key=conv_key,
                    session_id=item.session_id,
                    persisted_cwd=item.cwd,
                    session_attached=False,
                    model_id=item.model_id,
                    effort_id=item.effort_id,
                )

    def _build_persist_snapshot_locked(self) -> tuple[int, PersistedSessionState]:
        return (
            self._state_version,
            PersistedSessionState(
                conversations=dict(self._persisted_conversations),
                thread_aliases=self._thread_aliases.items_tuple(),
                reply_message_keys=self._reply_message_keys.items_tuple(),
            ),
        )

    def _persist_state(self) -> None:
        with self._persist_lock:
            with self._lock:
                version, snapshot = self._build_persist_snapshot_locked()
            if version <= self._persisted_version:
                return
            try:
                self.session_store.save(snapshot)
            except Exception:
                logger.exception("写入会话持久化状态失败")
                return
            self._persisted_version = version

    def _commit_entry_persistence_locked(self, entry: _ConversationEntry) -> None:
        """在持有 `entry.dispatch_lock` 时更新会话持久化镜像并原子落盘。"""
        with self._lock:
            if entry.session_id is None and entry.model_id is None and entry.effort_id is None:
                self._persisted_conversations.pop(entry.conv_key, None)
            else:
                self._persisted_conversations[entry.conv_key] = PersistedConversation(
                    session_id=entry.session_id,
                    cwd=entry.persisted_cwd,
                    model_id=entry.model_id,
                    effort_id=entry.effort_id,
                    updated_at=datetime.now(timezone.utc).isoformat(),
                )
            self._state_version += 1
        self._persist_state()

    def _record_reply_message_key(self, message_id: str, conv_key: str) -> None:
        """记录机器人回复消息 ID 到 conv_key 的映射并及时落盘。"""
        with self._lock:
            self._reply_message_keys.put(message_id, conv_key)
            self._state_version += 1
        self._persist_state()

    @property
    def transport(self) -> CardTransport:
        """延迟获取 CardKit v1 卡片传输适配器。"""
        if self._transport is None:
            if self._lark_client is None:
                self._lark_client = build_feishu_client(self.config)
            self._transport = LarkCardTransport(
                self._lark_client,
                uuid_factory=self._uuid_factory,
            )
        return self._transport

    @property
    def message_sender(self) -> MessageSender:
        """延迟获取 IM 消息发送与回复适配器。"""
        if self._message_sender is None:
            if self._lark_client is None:
                self._lark_client = build_feishu_client(self.config)
            self._message_sender = LarkMessageSender(
                self._lark_client,
                uuid_factory=self._uuid_factory,
            )
        return self._message_sender

    def start(self) -> None:
        """初始化底层 ACP 客户端；供长连接启动前或独立集成验证调用。"""
        with self._lock:
            if self._closing:
                raise RuntimeError("守护服务正在关闭，无法重新启动")
        workspace = self.config.default_workspace
        if not workspace.is_dir():
            raise FileNotFoundError(
                f"工作区目录不存在，请先创建或检查 AGY_DEFAULT_WORKSPACE 配置: {workspace}"
            )
        self._ensure_acp_started()

    def run_forever(self) -> None:
        """启动 ACP 客户端与飞书 WebSocket 长连接并阻塞运行，退出时优雅回收全部资源。"""
        try:
            self.start()
            ws_client = self._ws_client
            if ws_client is None:
                dispatcher = build_event_dispatcher(self)
                ws_client = build_ws_client(self.config, dispatcher)
                self._ws_client = ws_client
            logger.info(
                "agy-lark 守护服务已就绪，开始建立飞书 WebSocket 长连接 (domain=%s, workspace=%s)",
                self.config.feishu_domain,
                self.config.default_workspace,
            )
            ws_client.start()
        finally:
            self.close()

    def close(self) -> None:
        """停止接收新任务，尝试中止全部在途 ACP 回合并清理客户端与线程池资源。"""
        with self._lock:
            if self._closed:
                return
            self._closing = True
            active_turns = list(self._active_turns.values())

        for turn in active_turns:
            turn.cancelled_requested = True
            try:
                turn.updater.mark_stopping()
            except Exception:
                logger.debug("关闭阶段更新卡片停止态失败 (session_id=%s)", turn.session_id, exc_info=True)
            try:
                self.acp_client.cancel(turn.session_id)
            except Exception:
                logger.debug("关闭阶段发送 session/cancel 失败 (session_id=%s)", turn.session_id, exc_info=True)

        ws_stop = getattr(self._ws_client, "stop", None)
        if callable(ws_stop):
            try:
                ws_stop()
            except Exception:
                logger.debug("停止 WebSocket 客户端异常", exc_info=True)

        try:
            self.acp_client.close()
        except Exception:
            logger.exception("关闭 ACP 客户端时发生异常")

        self._executor.shutdown(wait=True, cancel_futures=False)
        with self._lock:
            self._closed = True

    def handle_message_event(self, raw_event: Any) -> None:
        """WebSocket im.message.receive_v1 事件入口：快速校验后异步派发到工作线程池。"""
        self.dispatch_message_event(raw_event)

    def dispatch_message_event(self, raw_event: Any) -> Future[None] | None:
        """解析、鉴权并去重入站消息，返回提交到线程池的 Future（被忽略或拒绝时返回 None）。"""
        msg = extract_incoming_message(raw_event, bot_open_id=self.bot_open_id)
        if msg is None:
            return None

        if not self._is_message_authorized(msg):
            logger.warning(
                "拒绝未授权消息 (chat_id=%s, chat_type=%s, open_id=%s)",
                msg.chat_id,
                msg.chat_type,
                msg.sender_open_id or "<empty>",
            )
            return None

        with self._lock:
            if self._closing:
                return None
            if not self._seen_messages.add_if_absent(msg.message_id):
                logger.debug("跳过重复投递消息 (message_id=%s)", msg.message_id)
                return None

        return self._executor.submit(self._process_incoming_message, msg)

    def handle_card_action(self, raw_event: Any) -> dict[str, Any]:
        """处理 card.action.trigger 卡片回调，验证点击者白名单并执行会话取消。"""
        event = _read_field(raw_event, "event")
        if event is None and (
            _read_field(raw_event, "operator") is not None
            or _read_field(raw_event, "action") is not None
        ):
            event = raw_event
        if event is None:
            return {}

        operator = _read_field(event, "operator")
        operator_open_id = _read_str(operator, "open_id")
        if not self.config.is_open_id_allowed(operator_open_id):
            logger.warning(
                "拒绝未授权用户点击卡片按钮 (open_id=%s)",
                operator_open_id or "<empty>",
            )
            return {"toast": {"type": "error", "content": "无权限操作该会话"}}

        action = _read_field(event, "action")
        action_value = _read_field(action, "value")
        stop_payload = parse_stop_callback_value(action_value)
        if stop_payload is None:
            model_payload = parse_model_select_callback_value(action_value)
            if model_payload is not None:
                return self._handle_selector_callback(
                    event=event,
                    kind="model",
                    target_value=model_payload.model_id,
                    conv_key_hint=model_payload.conv_key,
                )
            effort_payload = parse_effort_select_callback_value(action_value)
            if effort_payload is not None:
                return self._handle_selector_callback(
                    event=event,
                    kind="effort",
                    target_value=effort_payload.effort_id,
                    conv_key_hint=effort_payload.conv_key,
                )
            return {}

        cancelled = self._cancel_session_turn(
            stop_payload.session_id,
            expected_turn_id=stop_payload.turn_id,
        )
        if cancelled:
            return {"toast": {"type": "info", "content": "正在停止当前任务..."}}
        return {"toast": {"type": "info", "content": "当前任务已结束"}}

    def resolve_conversation_key(self, msg: IncomingMessage) -> str:
        """将飞书私聊窗口或群聊话题根映射为稳定的会话 Key。"""
        if msg.chat_type == "p2p":
            return f"p2p:{msg.chat_id}"

        need_persist = False
        with self._lock:
            if msg.thread_id:
                thread_alias = f"thread:{msg.chat_id}:{msg.thread_id}"
                existing = self._thread_aliases.get(thread_alias)
                if isinstance(existing, str) and existing:
                    return existing

            if msg.root_id:
                root_key = f"group:{msg.chat_id}:root:{msg.root_id}"
                if msg.thread_id:
                    self._thread_aliases.put(f"thread:{msg.chat_id}:{msg.thread_id}", root_key)
                    self._state_version += 1
                    need_persist = True
                resolved = root_key
            elif msg.parent_id:
                parent_key = self._reply_message_keys.get(msg.parent_id)
                if isinstance(parent_key, str) and parent_key:
                    if msg.thread_id:
                        self._thread_aliases.put(f"thread:{msg.chat_id}:{msg.thread_id}", parent_key)
                        self._state_version += 1
                        need_persist = True
                    resolved = parent_key
                elif msg.thread_id:
                    thread_key = f"group:{msg.chat_id}:thread:{msg.thread_id}"
                    self._thread_aliases.put(f"thread:{msg.chat_id}:{msg.thread_id}", thread_key)
                    self._state_version += 1
                    need_persist = True
                    resolved = thread_key
                else:
                    resolved = f"group:{msg.chat_id}:root:{msg.message_id}"
            elif msg.thread_id:
                thread_key = f"group:{msg.chat_id}:thread:{msg.thread_id}"
                self._thread_aliases.put(f"thread:{msg.chat_id}:{msg.thread_id}", thread_key)
                self._state_version += 1
                need_persist = True
                resolved = thread_key
            else:
                resolved = f"group:{msg.chat_id}:root:{msg.message_id}"

        if need_persist:
            self._persist_state()
        return resolved

    def get_session_id_for_key(self, conv_key: str) -> str | None:
        """查询给定会话 Key 当前绑定的 ACP session_id（未创建时返回 None）。"""
        with self._lock:
            entry = self._conversations.get(conv_key)
            return entry.session_id if entry is not None else None

    def _is_message_authorized(self, msg: IncomingMessage) -> bool:
        if not self.config.is_open_id_allowed(msg.sender_open_id):
            return False
        if msg.chat_type == "p2p":
            return True
        if not self.config.allow_group_mention:
            return False
        return msg.is_bot_mentioned

    def _get_or_create_entry(self, conv_key: str) -> _ConversationEntry:
        with self._lock:
            entry = self._conversations.get(conv_key)
            if entry is None:
                entry = _ConversationEntry(conv_key=conv_key)
                self._conversations[conv_key] = entry
            return entry

    def _ensure_acp_started(self) -> None:
        with self._acp_start_lock:
            with self._lock:
                if self.acp_client.is_running:
                    return
                # 底层子进程失效或重启后，保留 session_id 以便按需 session/resume，仅重置附着态
                for entry in self._conversations.values():
                    entry.session_attached = False
                self._session_entries.clear()
            self.acp_client.start()

    def _process_incoming_message(self, msg: IncomingMessage) -> None:
        conv_key = self.resolve_conversation_key(msg)
        normalized_cmd = msg.text.strip().lower()
        if normalized_cmd == "/stop":
            self._handle_stop_command(msg, conv_key)
            return
        if normalized_cmd == "/model":
            self._handle_selector_command(msg, conv_key, kind="model")
            return
        if normalized_cmd == "/effort":
            self._handle_selector_command(msg, conv_key, kind="effort")
            return

        self._handle_prompt_or_steer(msg, conv_key)

    def _sync_entry_config_locked(self, entry: _ConversationEntry, payload: Any) -> None:
        """在持有 `entry.dispatch_lock` 时同步 ACP 返回的可用模型与思考强度元数据。"""
        has_model_cfg = has_config_option(payload, option_id=ACP_MODEL_CONFIG_ID)
        has_effort_cfg = has_config_option(payload, option_id=ACP_EFFORT_CONFIG_ID)
        if has_model_cfg or has_effort_cfg:
            entry.uses_config_options = True

        models = parse_available_models(payload)
        if models:
            entry.available_models = models
            with self._lock:
                self._default_available_models = models

        reported_model = parse_current_model_id(payload)
        if reported_model:
            entry.model_id = reported_model

        if entry.uses_config_options:
            entry.available_efforts = parse_available_efforts(payload)
            entry.effort_id = parse_current_effort_id(payload)

    def _reset_entry_server_capabilities_locked(
        self,
        entry: _ConversationEntry,
        payload: Any,
    ) -> None:
        """在新建或恢复会话刚收到服务端元数据时，重置会话能力与服务端默认模型/思考强度。"""
        has_model_cfg = has_config_option(payload, option_id=ACP_MODEL_CONFIG_ID)
        has_effort_cfg = has_config_option(payload, option_id=ACP_EFFORT_CONFIG_ID)
        entry.uses_config_options = has_model_cfg or has_effort_cfg

        models = parse_available_models(payload)
        if models:
            entry.available_models = models
            with self._lock:
                self._default_available_models = models

        entry.model_id = parse_current_model_id(payload)
        if entry.uses_config_options:
            entry.available_efforts = parse_available_efforts(payload)
            entry.effort_id = parse_current_effort_id(payload)
        else:
            entry.available_efforts = ()
            entry.effort_id = None

    def _clear_entry_session_binding_locked(self, entry: _ConversationEntry) -> None:
        """在持有 `entry.dispatch_lock` 时清理失效的旧 ACP session_id 绑定。"""
        old_sid = entry.session_id
        entry.session_id = None
        entry.persisted_cwd = None
        entry.session_attached = False
        if old_sid is not None:
            with self._lock:
                if self._session_entries.get(old_sid) is entry:
                    self._session_entries.pop(old_sid, None)

    def _apply_session_config_locked(
        self,
        entry: _ConversationEntry,
        session_id: str,
        *,
        kind: str,
        target_value: str,
    ) -> None:
        """在持有 `entry.dispatch_lock` 时对已存在会话调用 ACP 设置模型或思考强度。"""
        if kind == "model":
            if entry.uses_config_options:
                resp = self.acp_client.set_config_option(
                    session_id,
                    ACP_MODEL_CONFIG_ID,
                    target_value,
                )
                if has_config_option(resp, option_id=ACP_MODEL_CONFIG_ID) or has_config_option(
                    resp,
                    option_id=ACP_EFFORT_CONFIG_ID,
                ):
                    self._sync_entry_config_locked(entry, resp)
                else:
                    entry.model_id = target_value
            else:
                self.acp_client.set_model(session_id, target_value)
                entry.model_id = target_value
            return

        resp = self.acp_client.set_config_option(
            session_id,
            ACP_EFFORT_CONFIG_ID,
            target_value,
        )
        if has_config_option(resp, option_id=ACP_MODEL_CONFIG_ID) or has_config_option(
            resp,
            option_id=ACP_EFFORT_CONFIG_ID,
        ):
            self._sync_entry_config_locked(entry, resp)
        else:
            entry.effort_id = target_value

    def _replay_or_apply_session_configs_locked(
        self,
        entry: _ConversationEntry,
        session_id: str,
        *,
        desired_model: str,
        desired_effort: str,
        explicit_model: str,
        explicit_effort: str,
        fallback_on_failure: bool,
        had_persisted_effort: bool,
        notices: list[str],
    ) -> str | None:
        """在持有 `entry.dispatch_lock` 时重放或设置模型与思考强度；回退失败项时附带告警。"""
        if desired_model and desired_model != (entry.model_id or ""):
            try:
                self._apply_session_config_locked(
                    entry,
                    session_id,
                    kind="model",
                    target_value=desired_model,
                )
            except Exception as exc:
                is_explicit_model = bool(explicit_model and desired_model == explicit_model)
                if is_explicit_model or not fallback_on_failure:
                    logger.exception(
                        "为 ACP 会话设置模型失败 (conv_key=%s, session_id=%s, model_id=%s)",
                        entry.conv_key,
                        session_id,
                        desired_model,
                    )
                    return f"设置模型 {desired_model} 失败：{exc}"
                logger.warning(
                    "重放历史模型 %s 失败，保留会话上下文并回退为默认模型 (conv_key=%s, session_id=%s): %s",
                    desired_model,
                    entry.conv_key,
                    session_id,
                    exc,
                )
                notices.append(
                    f"⚠️ 原选定模型 `{desired_model}` 当前不可用（{exc}），已保留会话上下文并回退为默认模型。"
                )

        if desired_effort:
            is_explicit_effort = bool(explicit_effort and desired_effort == explicit_effort)
            if entry.uses_config_options and not entry.available_efforts:
                if is_explicit_effort:
                    return "当前模型不支持调节思考强度。"
                if fallback_on_failure and had_persisted_effort:
                    logger.warning(
                        "当前模型不支持原思考强度 %s，保留会话上下文并回退为默认设置 (conv_key=%s, session_id=%s)",
                        desired_effort,
                        entry.conv_key,
                        session_id,
                    )
                    notices.append(
                        f"⚠️ 当前模型不支持原思考强度 `{desired_effort}`，已保留会话上下文并回退为默认设置。"
                    )
                else:
                    logger.debug(
                        "当前模型不支持 thought_level，跳过默认思考强度 (conv_key=%s, effort=%s)",
                        entry.conv_key,
                        desired_effort,
                    )
            elif desired_effort != (entry.effort_id or ""):
                try:
                    self._apply_session_config_locked(
                        entry,
                        session_id,
                        kind="effort",
                        target_value=desired_effort,
                    )
                except Exception as exc:
                    if is_explicit_effort or not fallback_on_failure:
                        logger.exception(
                            "为 ACP 会话设置思考强度失败 (conv_key=%s, session_id=%s, effort=%s)",
                            entry.conv_key,
                            session_id,
                            desired_effort,
                        )
                        return f"设置思考强度 {desired_effort} 失败：{exc}"
                    logger.warning(
                        "重放历史思考强度 %s 失败，保留会话上下文并回退为默认设置 (conv_key=%s, session_id=%s): %s",
                        desired_effort,
                        entry.conv_key,
                        session_id,
                        exc,
                    )
                    notices.append(
                        f"⚠️ 原选定思考强度 `{desired_effort}` 当前不可用（{exc}），已保留会话上下文并回退为默认设置。"
                    )

        return None

    def _ensure_entry_session_locked(
        self,
        entry: _ConversationEntry,
        *,
        initial_model_id: str | None = None,
        initial_effort_id: str | None = None,
    ) -> _EnsureSessionResult:
        """在持有 `entry.dispatch_lock` 的前提下按需恢复或新建 ACP 会话并重放模型与思考强度。"""
        if entry.session_id is not None and entry.session_attached:
            return _EnsureSessionResult(session_id=entry.session_id)

        explicit_model = initial_model_id.strip() if isinstance(initial_model_id, str) else ""
        explicit_effort = initial_effort_id.strip() if isinstance(initial_effort_id, str) else ""
        had_persisted_effort = bool(isinstance(entry.effort_id, str) and entry.effort_id.strip())
        desired_model = (explicit_model or entry.model_id or self.config.agy_model or "").strip()
        desired_effort = (explicit_effort or entry.effort_id or self.config.agy_effort or "").strip()

        current_workspace = self.config.default_workspace
        current_cwd = _format_workspace_path(current_workspace)
        notices: list[str] = []
        degraded = False

        if entry.session_id is not None:
            candidate_sid = entry.session_id
            if not _workspaces_match(entry.persisted_cwd, current_workspace):
                degraded = True
                logger.warning(
                    "工作区路径已变更，放弃恢复旧会话并将开启新会话 "
                    "(conv_key=%s, old_session_id=%s, old_cwd=%s, new_cwd=%s)",
                    entry.conv_key,
                    candidate_sid,
                    entry.persisted_cwd,
                    current_cwd,
                )
                if entry.persisted_cwd:
                    notices.append(
                        f"⚠️ 工作区路径已变更（{entry.persisted_cwd} → {current_cwd}），已为新工作区开启新会话。"
                    )
                else:
                    notices.append("⚠️ 历史会话工作区信息缺失，已为当前工作区开启新会话。")
                self._clear_entry_session_binding_locked(entry)
            else:
                resume_fn = getattr(self.acp_client, "resume_session", None)
                try:
                    if not callable(resume_fn):
                        raise NotImplementedError("当前 ACP 客户端不支持 session/resume")
                    session_info = resume_fn(candidate_sid, cwd=current_workspace)
                except Exception as exc:
                    degraded = True
                    logger.warning(
                        "恢复历史 ACP 会话失败，将降级开启新会话 (conv_key=%s, session_id=%s): %s",
                        entry.conv_key,
                        candidate_sid,
                        exc,
                    )
                    notices.append(f"⚠️ 原会话已失效或无法恢复（{exc}），已为您开启新会话。")
                    self._clear_entry_session_binding_locked(entry)
                else:
                    raw_resumed_sid = (
                        session_info.get("sessionId") if isinstance(session_info, dict) else None
                    )
                    effective_sid = (
                        raw_resumed_sid.strip()
                        if isinstance(raw_resumed_sid, str) and raw_resumed_sid.strip()
                        else candidate_sid
                    )
                    self._reset_entry_server_capabilities_locked(entry, session_info)
                    config_error = self._replay_or_apply_session_configs_locked(
                        entry,
                        effective_sid,
                        desired_model=desired_model,
                        desired_effort=desired_effort,
                        explicit_model=explicit_model,
                        explicit_effort=explicit_effort,
                        fallback_on_failure=True,
                        had_persisted_effort=had_persisted_effort,
                        notices=notices,
                    )
                    entry.session_id = effective_sid
                    entry.persisted_cwd = current_cwd
                    entry.session_attached = True
                    with self._lock:
                        if candidate_sid != effective_sid and self._session_entries.get(candidate_sid) is entry:
                            self._session_entries.pop(candidate_sid, None)
                        self._session_entries[effective_sid] = entry
                    self._commit_entry_persistence_locked(entry)
                    combined_notice = "\n".join(notices) if notices else None
                    if config_error is not None:
                        return _EnsureSessionResult(
                            session_id=None,
                            error=config_error,
                            notice=combined_notice,
                        )
                    return _EnsureSessionResult(
                        session_id=effective_sid,
                        notice=combined_notice,
                    )

        try:
            session_info = self.acp_client.new_session(cwd=current_workspace)
        except Exception as exc:
            logger.exception("创建 ACP 会话失败 (conv_key=%s)", entry.conv_key)
            if degraded:
                self._commit_entry_persistence_locked(entry)
            return _EnsureSessionResult(
                session_id=None,
                error=f"启动 Antigravity 会话失败：{exc}",
                notice="\n".join(notices) if notices else None,
            )

        raw_sid = session_info.get("sessionId") if isinstance(session_info, dict) else None
        if not isinstance(raw_sid, str) or not raw_sid.strip():
            if degraded:
                self._commit_entry_persistence_locked(entry)
            return _EnsureSessionResult(
                session_id=None,
                error="启动 Antigravity 会话失败：未返回有效 sessionId。",
                notice="\n".join(notices) if notices else None,
            )

        session_id = raw_sid.strip()
        self._reset_entry_server_capabilities_locked(entry, session_info)
        config_error = self._replay_or_apply_session_configs_locked(
            entry,
            session_id,
            desired_model=desired_model,
            desired_effort=desired_effort,
            explicit_model=explicit_model,
            explicit_effort=explicit_effort,
            fallback_on_failure=degraded,
            had_persisted_effort=had_persisted_effort,
            notices=notices,
        )
        if config_error is not None:
            self._cleanup_failed_session(session_id)
            if degraded:
                self._commit_entry_persistence_locked(entry)
            return _EnsureSessionResult(
                session_id=None,
                error=config_error,
                notice="\n".join(notices) if notices else None,
            )

        entry.session_id = session_id
        entry.persisted_cwd = current_cwd
        entry.session_attached = True
        with self._lock:
            self._session_entries[session_id] = entry
        self._commit_entry_persistence_locked(entry)
        return _EnsureSessionResult(
            session_id=session_id,
            notice="\n".join(notices) if notices else None,
        )

    def _cleanup_failed_session(self, session_id: str) -> None:
        try:
            self.acp_client.close_session(session_id)
        except Exception:
            logger.debug("清理失败会话时忽略异常 (session_id=%s)", session_id)

    def _format_current_model_name_locked(self, entry: _ConversationEntry) -> str | None:
        with self._lock:
            default_models = self._default_available_models
        models = entry.available_models or default_models
        current_model = entry.model_id or (self.config.agy_model or None)
        if not current_model:
            return None
        matched = next((opt for opt in models if opt.model_id == current_model), None)
        if matched is not None:
            return f"{matched.name} (`{matched.model_id}`)"
        return f"`{current_model}`"

    def _build_selector_card_payload(
        self,
        *,
        kind: str,
        conv_key: str,
        models: tuple[AcpModelOption, ...],
        efforts: tuple[AcpEffortOption, ...],
        current_model_id: str | None,
        current_effort_id: str | None,
        current_model_name: str | None,
    ) -> dict[str, Any]:
        if kind == "model":
            return build_model_selector_card_json(
                models=models,
                current_model_id=current_model_id,
                conv_key=conv_key,
            )
        return build_effort_selector_card_json(
            efforts=efforts,
            current_effort_id=current_effort_id,
            current_model_name=current_model_name,
            conv_key=conv_key,
        )

    def _handle_selector_command(
        self,
        msg: IncomingMessage,
        conv_key: str,
        *,
        kind: str,
    ) -> None:
        cmd_label = "/model" if kind == "model" else "/effort"
        card_noun = "模型选择卡片" if kind == "model" else "思考强度选择卡片"
        try:
            self._ensure_acp_started()
        except Exception as exc:
            logger.exception("处理 %s 时启动 ACP 失败 (conv_key=%s)", cmd_label, conv_key)
            self._safe_send_text(
                chat_id=msg.chat_id,
                text=f"启动 Antigravity 会话失败：{exc}",
                reply_to_message_id=msg.message_id,
                reply_in_thread=bool(msg.thread_id),
                conv_key=conv_key,
            )
            return

        entry = self._get_or_create_entry(conv_key)
        session_notice: str | None = None
        with entry.dispatch_lock:
            session_res = self._ensure_entry_session_locked(entry)
            if session_res.session_id is None:
                self._safe_send_text(
                    chat_id=msg.chat_id,
                    text=session_res.error or "启动 Antigravity 会话失败。",
                    reply_to_message_id=msg.message_id,
                    reply_in_thread=bool(msg.thread_id),
                    conv_key=conv_key,
                )
                return
            session_notice = session_res.notice
            with self._lock:
                default_models = self._default_available_models
            models = entry.available_models or default_models
            efforts = entry.available_efforts
            current_model_id = entry.model_id or (self.config.agy_model or None)
            current_effort_id = entry.effort_id or (self.config.agy_effort or None)
            current_model_name = self._format_current_model_name_locked(entry)

        if session_notice:
            self._safe_send_text(
                chat_id=msg.chat_id,
                text=session_notice,
                reply_to_message_id=msg.message_id,
                reply_in_thread=bool(msg.thread_id),
                conv_key=conv_key,
            )

        card_json = self._build_selector_card_payload(
            kind=kind,
            conv_key=conv_key,
            models=models,
            efforts=efforts,
            current_model_id=current_model_id,
            current_effort_id=current_effort_id,
            current_model_name=current_model_name,
        )
        try:
            card_id = self.transport.create_card(card_json)
            sent_msg_id = self.message_sender.send_card_message(
                chat_id=msg.chat_id,
                card_id=card_id,
                reply_to_message_id=msg.message_id,
                reply_in_thread=bool(msg.thread_id),
            )
        except Exception as exc:
            logger.exception("发送 %s %s失败 (conv_key=%s)", cmd_label, card_noun, conv_key)
            self._safe_send_text(
                chat_id=msg.chat_id,
                text=f"发送{card_noun}失败：{exc}",
                reply_to_message_id=msg.message_id,
                reply_in_thread=bool(msg.thread_id),
                conv_key=conv_key,
            )
            return

        if sent_msg_id:
            with self._lock:
                self._reply_message_keys.put(sent_msg_id, conv_key)
                self._state_version += 1
                self._selector_cards.put(
                    sent_msg_id,
                    _SelectorCardRecord(
                        kind=kind,
                        card_id=card_id,
                        conv_key=conv_key,
                        models=models,
                        efforts=efforts,
                        current_model_name=current_model_name,
                    ),
                )
            self._persist_state()

    def _handle_selector_callback(
        self,
        *,
        event: Any,
        kind: str,
        target_value: str,
        conv_key_hint: str | None,
    ) -> dict[str, Any]:
        cmd_label = "/model" if kind == "model" else "/effort"
        action_noun = "模型" if kind == "model" else "思考强度"
        context = _read_field(event, "context")
        open_message_id = _read_str(context, "open_message_id")
        with self._lock:
            record = (
                self._selector_cards.get(open_message_id)
                if open_message_id
                else None
            )
            default_models = self._default_available_models

        conv_key = record.conv_key if record is not None else conv_key_hint
        if not conv_key:
            return {
                "toast": {
                    "type": "error",
                    "content": f"未能识别所属会话，请重新发送 {cmd_label}",
                }
            }

        try:
            self._ensure_acp_started()
        except Exception as exc:
            logger.exception("卡片切换%s时启动 ACP 失败 (conv_key=%s)", action_noun, conv_key)
            return {"toast": {"type": "error", "content": f"切换{action_noun}失败：{exc}"}}

        entry = self._get_or_create_entry(conv_key)
        session_notice: str | None = None
        with entry.dispatch_lock:
            active_turn = entry.active_turn
            if active_turn is not None and not active_turn.done_event.is_set():
                return {
                    "toast": {
                        "type": "info",
                        "content": f"当前有任务正在执行，请等待完成或先停止后再切换{action_noun}",
                    }
                }

            if entry.session_id is None or not entry.session_attached:
                init_kw = (
                    {"initial_model_id": target_value}
                    if kind == "model"
                    else {"initial_effort_id": target_value}
                )
                session_res = self._ensure_entry_session_locked(
                    entry,
                    **init_kw,
                )
                if session_res.session_id is None:
                    return {
                        "toast": {
                            "type": "error",
                            "content": session_res.error or f"切换{action_noun}失败",
                        }
                    }
                session_notice = session_res.notice
            else:
                try:
                    self._apply_session_config_locked(
                        entry,
                        entry.session_id,
                        kind=kind,
                        target_value=target_value,
                    )
                except Exception as exc:
                    logger.exception(
                        "卡片切换%s调用 ACP 配置更新失败 (conv_key=%s, session_id=%s, target=%s)",
                        action_noun,
                        conv_key,
                        entry.session_id,
                        target_value,
                    )
                    return {"toast": {"type": "error", "content": f"切换{action_noun}失败：{exc}"}}
                self._commit_entry_persistence_locked(entry)

            models_for_card = (
                entry.available_models
                or (record.models if record is not None and record.models else default_models)
            )
            efforts_for_card = (
                entry.available_efforts
                if (kind == "effort" and entry.available_efforts)
                else (
                    record.efforts
                    if record is not None and record.efforts
                    else entry.available_efforts
                )
            )
            current_model_id = entry.model_id or target_value if kind == "model" else entry.model_id
            current_effort_id = (
                entry.effort_id or target_value if kind == "effort" else entry.effort_id
            )
            current_model_name = (
                self._format_current_model_name_locked(entry)
                or (record.current_model_name if record is not None else None)
            )

        if kind == "model":
            matched = next(
                (opt for opt in models_for_card if opt.model_id == (current_model_id or target_value)),
                None,
            )
            display_name = matched.name if matched is not None else (current_model_id or target_value)
        else:
            matched = next(
                (
                    opt
                    for opt in efforts_for_card
                    if opt.effort_id == (current_effort_id or target_value)
                ),
                None,
            )
            display_name = matched.name if matched is not None else (current_effort_id or target_value)

        if record is not None:
            record_kind = record.kind
            updated_card = self._build_selector_card_payload(
                kind=record_kind,
                conv_key=conv_key,
                models=models_for_card if models_for_card else record.models,
                efforts=efforts_for_card,
                current_model_id=current_model_id,
                current_effort_id=current_effort_id,
                current_model_name=current_model_name,
            )
            try:
                with record.update_lock:
                    record.models = models_for_card if models_for_card else record.models
                    if efforts_for_card or kind == "effort":
                        record.efforts = efforts_for_card
                    record.current_model_name = current_model_name
                    record.sequence += 1
                    seq = record.sequence
                    self.transport.update_card(
                        record.card_id,
                        updated_card,
                        sequence=seq,
                        uuid=self._uuid_factory(),
                    )
            except Exception:
                logger.exception(
                    "更新 %s 交互卡片失败 (card_id=%s, target=%s)",
                    cmd_label,
                    record.card_id,
                    target_value,
                )

        if session_notice:
            return {
                "toast": {
                    "type": "warning",
                    "content": f"已切换{action_noun}为 {display_name}；{session_notice}",
                }
            }
        return {"toast": {"type": "success", "content": f"已切换{action_noun}为 {display_name}"}}

    def _handle_stop_command(self, msg: IncomingMessage, conv_key: str) -> None:
        with self._lock:
            entry = self._conversations.get(conv_key)
            session_id = entry.session_id if entry is not None else None

        if session_id and self._cancel_session_turn(session_id):
            return

        self._safe_send_text(
            chat_id=msg.chat_id,
            text="当前会话暂无正在运行的任务。",
            reply_to_message_id=msg.message_id,
            reply_in_thread=bool(msg.thread_id),
            conv_key=conv_key,
        )

    def _cancel_session_turn(
        self,
        session_id: str,
        *,
        expected_turn_id: str | None = None,
    ) -> bool:
        with self._lock:
            turn = self._active_turns.get(session_id)
            if turn is None or turn.done_event.is_set():
                return False
            if expected_turn_id is not None and turn.turn_id != expected_turn_id:
                return False
            turn.cancelled_requested = True

        try:
            turn.updater.mark_stopping()
        except Exception:
            logger.exception("更新卡片停止中状态失败 (session_id=%s)", session_id)

        try:
            self.acp_client.cancel(session_id)
        except Exception:
            logger.exception("发送 session/cancel 失败 (session_id=%s)", session_id)
            return False
        return True

    def _handle_prompt_or_steer(self, msg: IncomingMessage, conv_key: str) -> None:
        while True:
            self._ensure_acp_started()
            entry = self._get_or_create_entry(conv_key)

            run_as_new_turn = False
            steer_turn: _ActiveTurn | None = None
            new_turn: _ActiveTurn | None = None
            session_notice: str | None = None

            with entry.dispatch_lock:
                session_res = self._ensure_entry_session_locked(entry)
                session_id = session_res.session_id
                if session_id is None:
                    self._safe_send_text(
                        chat_id=msg.chat_id,
                        text=session_res.error or "启动 Antigravity 会话失败。",
                        reply_to_message_id=msg.message_id,
                        reply_in_thread=bool(msg.thread_id),
                        conv_key=conv_key,
                    )
                    return
                session_notice = session_res.notice

                existing_turn = entry.active_turn
                if existing_turn is not None and not existing_turn.done_event.is_set():
                    steer_turn = existing_turn
                else:
                    turn_id = self._uuid_factory()
                    updater = self._updater_factory(
                        self.transport,
                        session_id,
                        turn_id=turn_id,
                        throttle_seconds=self.config.stream_throttle,
                        uuid_factory=self._uuid_factory,
                    )
                    new_turn = _ActiveTurn(
                        turn_id=turn_id,
                        session_id=session_id,
                        updater=updater,
                    )
                    entry.active_turn = new_turn
                    with self._lock:
                        self._active_turns[session_id] = new_turn
                    run_as_new_turn = True

            if run_as_new_turn and new_turn is not None:
                if session_notice:
                    self._safe_send_text(
                        chat_id=msg.chat_id,
                        text=session_notice,
                        reply_to_message_id=msg.message_id,
                        reply_in_thread=bool(msg.thread_id),
                        conv_key=conv_key,
                    )
                self._execute_new_turn(entry, new_turn, msg)
                return

            if steer_turn is not None:
                dispatched = self._execute_mid_turn_steer(steer_turn, msg)
                if dispatched:
                    return
                # 原回合在接入前已结束收口，重新循环按新回合执行本条消息
                continue

    def _execute_new_turn(
        self,
        entry: _ConversationEntry,
        turn: _ActiveTurn,
        msg: IncomingMessage,
    ) -> None:
        updater = turn.updater
        session_id = turn.session_id
        try:
            card_id = updater.start()
            sent_msg_id = self.message_sender.send_card_message(
                chat_id=msg.chat_id,
                card_id=card_id,
                reply_to_message_id=msg.message_id,
                reply_in_thread=bool(msg.thread_id),
            )
            if sent_msg_id:
                self._record_reply_message_key(sent_msg_id, entry.conv_key)

            first_future = self.acp_client.prompt_async(
                session_id,
                msg.text,
                on_update=updater.handle_update,
                message_id=msg.message_id,
            )
            with turn.lock:
                turn.futures.append(first_future)
            turn.ready_event.set()

            last_result = self._wait_for_turn_futures(
                turn,
                timeout_seconds=float(self.config.turn_timeout_seconds),
            )
            updater.finalize_from_result(last_result)
        except TimeoutError:
            logger.warning("ACP 回合执行超时 (session_id=%s)", session_id)
            try:
                self.acp_client.cancel(session_id)
            except Exception:
                logger.debug("超时后发送取消通知失败", exc_info=True)
            self._finalize_error_safely(
                updater,
                f"任务执行超时（超过 {self.config.turn_timeout_seconds} 秒）",
                msg=msg,
                conv_key=entry.conv_key,
            )
        except Exception as exc:
            if self._closing and turn.cancelled_requested:
                logger.info("守护服务关闭期间已中止在途回合 (session_id=%s)", session_id)
                if updater.card_id is not None:
                    try:
                        updater.finish_cancelled()
                    except Exception:
                        logger.debug("关闭阶段收口取消卡片失败", exc_info=True)
            else:
                logger.exception("ACP 回合执行异常 (session_id=%s)", session_id)
                if not self.acp_client.is_running:
                    with entry.dispatch_lock:
                        if entry.session_id == session_id:
                            entry.session_attached = False
                    with self._lock:
                        if self._session_entries.get(session_id) is entry:
                            self._session_entries.pop(session_id, None)
                self._finalize_error_safely(
                    updater,
                    str(exc) or type(exc).__name__,
                    msg=msg,
                    conv_key=entry.conv_key,
                )
        finally:
            with turn.lock:
                turn.accepting_steers = False
            turn.ready_event.set()
            turn.done_event.set()
            with entry.dispatch_lock:
                if entry.active_turn is turn:
                    entry.active_turn = None
            with self._lock:
                if self._active_turns.get(session_id) is turn:
                    self._active_turns.pop(session_id, None)

    def _execute_mid_turn_steer(self, turn: _ActiveTurn, msg: IncomingMessage) -> bool:
        """将新消息作为运行中插话送入当前在途回合；若原回合已停止接收则返回 False。"""
        turn.ready_event.wait(timeout=_TURN_READY_WAIT_SECONDS)
        with turn.lock:
            if not turn.accepting_steers or turn.done_event.is_set() or turn.cancelled_requested:
                return False
            turn.pending_steer_count += 1

        try:
            try:
                turn.updater.set_status("🧭 收到插话指令，正在调整处理...")
            except Exception:
                logger.debug("更新插话状态栏失败", exc_info=True)

            steer_future = self.acp_client.prompt_async(
                turn.session_id,
                msg.text,
                on_update=turn.updater.handle_update,
                message_id=msg.message_id,
            )
            with turn.lock:
                turn.futures.append(steer_future)
            return True
        finally:
            with turn.lock:
                turn.pending_steer_count = max(0, turn.pending_steer_count - 1)

    def _wait_for_turn_futures(
        self,
        turn: _ActiveTurn,
        *,
        timeout_seconds: float,
    ) -> AcpPromptResult:
        """等待主回合及所有关联运行中插话 Future 完成，并返回最后一个回合结果。"""
        waited_ids: set[int] = set()
        last_result: AcpPromptResult | None = None

        while True:
            next_future: Future[AcpPromptResult] | None = None
            with turn.lock:
                for candidate in turn.futures:
                    if id(candidate) not in waited_ids:
                        next_future = candidate
                        waited_ids.add(id(candidate))
                        break
                if next_future is None and turn.pending_steer_count == 0:
                    turn.accepting_steers = False
                    break

            if next_future is None:
                time.sleep(0.01)
                continue

            last_result = next_future.result(timeout=timeout_seconds)

        if last_result is None:
            raise RuntimeError("回合结束但未获得任何 ACP 结果")
        return last_result

    def _finalize_error_safely(
        self,
        updater: StreamingCardUpdater,
        reason: str,
        *,
        msg: IncomingMessage,
        conv_key: str,
    ) -> None:
        if updater.card_id is not None:
            try:
                updater.finish_error(reason)
                return
            except Exception:
                logger.exception("卡片异常终态收口失败 (card_id=%s)", updater.card_id)

        self._safe_send_text(
            chat_id=msg.chat_id,
            text=f"执行失败：{reason}",
            reply_to_message_id=msg.message_id,
            reply_in_thread=bool(msg.thread_id),
            conv_key=conv_key,
        )

    def _safe_send_text(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None,
        reply_in_thread: bool,
        conv_key: str,
    ) -> None:
        try:
            sent_id = self.message_sender.send_text_message(
                chat_id=chat_id,
                text=text,
                reply_to_message_id=reply_to_message_id,
                reply_in_thread=reply_in_thread,
            )
            if sent_id:
                self._record_reply_message_key(sent_id, conv_key)
        except Exception:
            logger.exception("发送文本提示消息失败 (chat_id=%s)", chat_id)


def _configure_logging(level_name: str) -> None:
    """配置基础标准输出日志格式（仅在未初始化根 logger 时生效）。"""
    level = getattr(logging, (level_name or "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
    else:
        root.setLevel(level)


def build_arg_parser() -> argparse.ArgumentParser:
    """构建 agy-lark 守护服务 CLI 参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="agy-lark",
        description="启动飞书 ↔ Antigravity (agy acp --stdio) 单进程桥接守护服务。",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="指定环境变量配置文件路径（默认读取当前目录下的 .env；传空字符串跳过读取）。",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="启用 DEBUG 级别详细日志输出。",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    daemon_factory: Callable[[AgyLarkConfig], AgyLarkDaemon] = AgyLarkDaemon,
    environ: dict[str, str] | None = None,
) -> int:
    """agy-lark 命令行入口：加载配置、启动 WebSocket 守护服务并在退出时干净清理。"""
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    dotenv_path: str | None = args.env_file.strip() if isinstance(args.env_file, str) else ".env"
    if not dotenv_path:
        dotenv_path = None

    try:
        config = load_config(
            dotenv_path=dotenv_path,
            environ=environ,
            resolve_exe=True,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"配置加载失败：{exc}", file=sys.stderr)
        return 1

    if not config.feishu_app_id or not config.feishu_app_secret:
        print(
            "配置缺少 FEISHU_APP_ID 或 FEISHU_APP_SECRET，请检查 .env 或环境变量。",
            file=sys.stderr,
        )
        return 1

    if not config.default_workspace.is_dir():
        print(
            f"配置加载失败：工作区目录不存在，请先创建或检查 AGY_DEFAULT_WORKSPACE 配置: {config.default_workspace}",
            file=sys.stderr,
        )
        return 1

    log_level = "DEBUG" if args.verbose else config.log_level
    _configure_logging(log_level)

    if not config.allowed_open_ids:
        logger.warning(
            "AGY_LARK_ALLOWED_OPEN_IDS 为空：依据 Fail-Closed 安全策略，所有入站消息均将被拒绝。"
        )

    daemon = daemon_factory(config)

    def _on_sigterm(signum: int, _frame: Any) -> None:
        logger.info("收到终止信号 (%s)，正在准备退出 agy-lark...", signum)
        raise KeyboardInterrupt

    previous_sigterm = (
        signal.getsignal(signal.SIGTERM) if hasattr(signal, "SIGTERM") else None
    )
    if threading.current_thread() is threading.main_thread() and hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        daemon.run_forever()
    except KeyboardInterrupt:
        logger.info("收到中断请求，正在退出 agy-lark...")
    finally:
        daemon.close()
        if (
            threading.current_thread() is threading.main_thread()
            and previous_sigterm is not None
            and hasattr(signal, "SIGTERM")
        ):
            signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
