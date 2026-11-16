"""本地会话映射与群聊路由状态的原子 JSON 持久化存储。"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
from typing import Any, Protocol

logger = logging.getLogger(__name__)

SESSION_STORE_SCHEMA_VERSION = 1
DEFAULT_SESSIONS_FILENAME = "sessions.json"
DEFAULT_MAPPING_CAPACITY = 2048


@dataclass(frozen=True)
class PersistedConversation:
    """单个飞书会话（私聊或群聊话题）在磁盘上的持久化状态快照。"""

    session_id: str | None = None
    cwd: str | None = None
    model_id: str | None = None
    effort_id: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class PersistedSessionState:
    """`sessions.json` 对应的完整反序列化/序列化状态模型。"""

    schema_version: int = SESSION_STORE_SCHEMA_VERSION
    conversations: dict[str, PersistedConversation] = field(default_factory=dict)
    thread_aliases: tuple[tuple[str, str], ...] = ()
    reply_message_keys: tuple[tuple[str, str], ...] = ()


class SessionStore(Protocol):
    """会话映射与群聊路由持久化存储协议（便于测试替身注入）。"""

    def load(self) -> PersistedSessionState:
        """读取并返回当前持久化的会话状态快照。"""

    def save(self, state: PersistedSessionState) -> None:
        """将给定会话状态快照原子写入持久化介质。"""


def _clean_optional_str(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return None


def _clean_updated_at(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _normalize_mapping_pairs(
    raw_pairs: Iterable[Any],
    *,
    max_entries: int,
    section_name: str,
) -> tuple[tuple[str, str], ...]:
    ordered: OrderedDict[str, str] = OrderedDict()
    for item in raw_pairs:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            logger.warning("忽略 %s 中的非法路由条目: %r", section_name, item)
            continue
        key = _clean_optional_str(item[0])
        val = _clean_optional_str(item[1])
        if key is None or val is None:
            logger.warning("忽略 %s 中的空键值路由条目: %r", section_name, item)
            continue
        if key in ordered:
            ordered.move_to_end(key)
        ordered[key] = val
        while len(ordered) > max_entries:
            ordered.popitem(last=False)
    return tuple(ordered.items())


def _parse_conversations(raw_conversations: Any) -> dict[str, PersistedConversation]:
    if raw_conversations is None:
        return {}
    if not isinstance(raw_conversations, dict):
        logger.error("sessions.json 中 conversations 字段格式无效，已忽略该字段")
        return {}

    parsed: dict[str, PersistedConversation] = {}
    for raw_key, raw_entry in raw_conversations.items():
        conv_key = _clean_optional_str(raw_key)
        if conv_key is None or not isinstance(raw_entry, dict):
            logger.warning("忽略 sessions.json 中无效的会话条目: %r", raw_key)
            continue
        parsed[conv_key] = PersistedConversation(
            session_id=_clean_optional_str(raw_entry.get("session_id")),
            cwd=_clean_optional_str(raw_entry.get("cwd")),
            model_id=_clean_optional_str(raw_entry.get("model_id")),
            effort_id=_clean_optional_str(raw_entry.get("effort_id")),
            updated_at=_clean_updated_at(raw_entry.get("updated_at")),
        )
    return parsed


def _parse_mapping_section(
    raw_section: Any,
    *,
    max_entries: int,
    section_name: str,
) -> tuple[tuple[str, str], ...]:
    if raw_section is None:
        return ()
    if not isinstance(raw_section, list):
        logger.error("sessions.json 中 %s 字段格式无效，已忽略该字段", section_name)
        return ()
    return _normalize_mapping_pairs(
        raw_section,
        max_entries=max_entries,
        section_name=section_name,
    )


class JsonSessionStore:
    """基于本地 JSON 文件 + 内部锁 + 同目录临时文件原子替换的会话存储。"""

    def __init__(
        self,
        path: str | Path,
        *,
        max_mapping_entries: int = DEFAULT_MAPPING_CAPACITY,
    ) -> None:
        if max_mapping_entries <= 0:
            raise ValueError("max_mapping_entries 必须为正整数")
        self.path = Path(path)
        self.max_mapping_entries = max_mapping_entries
        self._lock = threading.RLock()

    def load(self) -> PersistedSessionState:
        """从磁盘加载 `sessions.json`；若文件不存在或损坏则返回空状态而不阻塞启动。"""
        with self._lock:
            if not self.path.exists():
                return PersistedSessionState()
            if not self.path.is_file():
                logger.error("会话持久化路径不是常规文件，已降级为空状态 (path=%s)", self.path)
                return PersistedSessionState()

            try:
                raw_text = self.path.read_text(encoding="utf-8")
                payload = json.loads(raw_text)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                logger.error(
                    "读取或解析会话持久化文件失败，已降级为空状态 (path=%s): %s",
                    self.path,
                    exc,
                )
                return PersistedSessionState()

            if not isinstance(payload, dict):
                logger.error(
                    "会话持久化文件根节点必须为 JSON 对象，已降级为空状态 (path=%s)",
                    self.path,
                )
                return PersistedSessionState()

            version = payload.get("schema_version")
            if (
                isinstance(version, bool)
                or not isinstance(version, int)
                or version != SESSION_STORE_SCHEMA_VERSION
            ):
                logger.error(
                    "会话持久化文件 schema_version=%r 不受支持（期望 %s），已降级为空状态 (path=%s)",
                    version,
                    SESSION_STORE_SCHEMA_VERSION,
                    self.path,
                )
                return PersistedSessionState()

            conversations = _parse_conversations(payload.get("conversations"))
            thread_aliases = _parse_mapping_section(
                payload.get("thread_aliases"),
                max_entries=self.max_mapping_entries,
                section_name="thread_aliases",
            )
            reply_message_keys = _parse_mapping_section(
                payload.get("reply_message_keys"),
                max_entries=self.max_mapping_entries,
                section_name="reply_message_keys",
            )
            return PersistedSessionState(
                schema_version=SESSION_STORE_SCHEMA_VERSION,
                conversations=conversations,
                thread_aliases=thread_aliases,
                reply_message_keys=reply_message_keys,
            )

    def save(self, state: PersistedSessionState) -> None:
        """将状态原子写入 `sessions.json`（同目录临时文件 + fsync + os.replace）。"""
        normalized_aliases = _normalize_mapping_pairs(
            state.thread_aliases,
            max_entries=self.max_mapping_entries,
            section_name="thread_aliases",
        )
        normalized_replies = _normalize_mapping_pairs(
            state.reply_message_keys,
            max_entries=self.max_mapping_entries,
            section_name="reply_message_keys",
        )
        payload = {
            "schema_version": SESSION_STORE_SCHEMA_VERSION,
            "conversations": {
                conv_key: {
                    "session_id": conv.session_id,
                    "cwd": conv.cwd,
                    "model_id": conv.model_id,
                    "effort_id": conv.effort_id,
                    "updated_at": conv.updated_at,
                }
                for conv_key, conv in state.conversations.items()
            },
            "thread_aliases": [[alias, canonical] for alias, canonical in normalized_aliases],
            "reply_message_keys": [[msg_id, conv_key] for msg_id, conv_key in normalized_replies],
        }
        serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=self.path.parent,
                    prefix=f".{self.path.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as tmp_file:
                    tmp_path = Path(tmp_file.name)
                    tmp_file.write(serialized)
                    tmp_file.flush()
                    os.fsync(tmp_file.fileno())
                os.replace(tmp_path, self.path)
            except Exception:
                if tmp_path is not None:
                    tmp_path.unlink(missing_ok=True)
                raise
