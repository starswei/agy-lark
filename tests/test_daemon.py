"""agy_lark.daemon 离线集成测试（兼容 unittest 与 pytest）。"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from dataclasses import dataclass
import gc
from io import StringIO
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import tomllib
from types import SimpleNamespace
from typing import Any
import unittest
from unittest import mock

import agy_lark
from agy_lark import (
    CARD_SCHEMA_VERSION,
    CONTENT_ELEMENT_ID,
    EFFORT_STATUS_ELEMENT_ID,
    MODEL_STATUS_ELEMENT_ID,
    STATUS_ELEMENT_ID,
    STOP_BUTTON_ELEMENT_ID,
    AcpError,
    AcpPromptResult,
    AcpSessionUpdate,
    AgyAcpClient,
    AgyLarkConfig,
    AgyLarkDaemon,
    CardRunState,
    LarkApiError,
    LarkCardTransport,
    LarkMessageSender,
    build_effort_select_callback_value,
    build_event_dispatcher,
    build_feishu_client,
    build_model_select_callback_value,
    build_stop_callback_value,
    build_ws_client,
    extract_incoming_message,
    main,
    parse_effort_select_callback_value,
    parse_model_select_callback_value,
)
from agy_lark.daemon import _import_lark_oapi

_MOCK_DAEMON_ACP_SCRIPT = r"""
import json
import sys
import threading

if hasattr(sys.stdin, "reconfigure"):
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

write_lock = threading.Lock()
state_lock = threading.Lock()
sessions = {}
next_session_idx = 0


def send(payload):
    with write_lock:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def run_turn(session_id, req_id, prompt_text):
    st = sessions[session_id]
    if prompt_text.startswith("stream:"):
        for update in (
            {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "分析需求"},
            },
            {
                "sessionUpdate": "tool_call",
                "toolCallId": "tc-1",
                "name": "read_file",
                "status": "in_progress",
            },
            {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "第一段，"},
            },
            {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "第二段。"},
            },
        ):
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {"sessionId": session_id, "update": update},
                }
            )
        send(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "stopReason": "end_turn",
                    "usage": {"totalTokens": 88},
                },
            }
        )
        return

    if prompt_text.startswith("steer_hold:"):
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "初始阶段;"},
                    },
                },
            }
        )
        st["steer_event"].wait(timeout=5.0)
        if st["cancelled"]:
            send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "cancelled"}})
            return
        steer_text = st["steer_text"]
        steer_req_id = st["steer_req_id"]
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": f"已纠偏:{steer_text}"},
                    },
                },
            }
        )
        if steer_req_id is not None:
            send({"jsonrpc": "2.0", "id": steer_req_id, "result": {"stopReason": "end_turn"}})
        send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "end_turn"}})
        return

    if prompt_text.startswith("cancel_wait:"):
        send(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "部分输出"},
                    },
                },
            }
        )
        st["cancel_event"].wait(timeout=5.0)
        send(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"stopReason": "cancelled" if st["cancelled"] else "end_turn"},
            }
        )
        return

    if prompt_text.startswith("error:"):
        send(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32001, "message": "simulated acp failure"},
            }
        )
        return

    send(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": f"echo:{prompt_text}"},
                },
            },
        }
    )
    send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "end_turn"}})


for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    params = msg.get("params") or {}

    if method == "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": params.get("protocolVersion"),
                    "agentInfo": {"name": "Mock ACP Daemon Server", "version": "1.0.0"},
                },
            }
        )
    elif method == "session/new":
        with state_lock:
            next_session_idx += 1
            sid = f"sess-acp-{next_session_idx}"
            sessions[sid] = {
                "busy": False,
                "cancelled": False,
                "cancel_event": threading.Event(),
                "steer_event": threading.Event(),
                "steer_req_id": None,
                "steer_text": "",
            }
        send({"jsonrpc": "2.0", "id": req_id, "result": {"sessionId": sid}})
    elif method == "session/prompt":
        sid = params.get("sessionId", "")
        blocks = params.get("prompt") or []
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        st = sessions[sid]
        with state_lock:
            if st["busy"]:
                st["steer_req_id"] = req_id
                st["steer_text"] = text
                st["steer_event"].set()
                continue
            st["busy"] = True
            st["cancelled"] = False
            st["cancel_event"].clear()
            st["steer_event"].clear()
            st["steer_req_id"] = None
            st["steer_text"] = ""

        def _worker(s_id=sid, r_id=req_id, p_text=text, state=st):
            try:
                run_turn(s_id, r_id, p_text)
            finally:
                with state_lock:
                    state["busy"] = False

        threading.Thread(target=_worker, daemon=True).start()
    elif method == "session/cancel":
        sid = params.get("sessionId", "")
        st = sessions.get(sid)
        if st is not None:
            st["cancelled"] = True
            st["cancel_event"].set()
            st["steer_event"].set()
"""


class _FakeTransport:
    """记录 CardKit v1 操作的纯内存测试传输器。"""

    def __init__(self) -> None:
        self._counter = 0
        self._lock = threading.Lock()
        self.created_cards: list[dict[str, Any]] = []
        self.stream_calls: list[dict[str, Any]] = []
        self.update_settings_calls: list[dict[str, Any]] = []
        self.update_card_calls: list[dict[str, Any]] = []

    def create_card(self, card_json: dict[str, Any]) -> str:
        with self._lock:
            self._counter += 1
            card_id = f"card_mock_{self._counter}"
            self.created_cards.append({"card_id": card_id, "card_json": card_json})
            return card_id

    def stream_element_content(
        self,
        card_id: str,
        element_id: str,
        content: str,
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        with self._lock:
            self.stream_calls.append(
                {
                    "card_id": card_id,
                    "element_id": element_id,
                    "content": content,
                    "sequence": sequence,
                    "uuid": uuid,
                }
            )

    def update_card(
        self,
        card_id: str,
        card_json: dict[str, Any],
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        with self._lock:
            self.update_card_calls.append(
                {
                    "card_id": card_id,
                    "card_json": card_json,
                    "sequence": sequence,
                    "uuid": uuid,
                }
            )

    def update_settings(
        self,
        card_id: str,
        settings: dict[str, Any],
        *,
        sequence: int,
        uuid: str,
    ) -> None:
        with self._lock:
            self.update_settings_calls.append(
                {
                    "card_id": card_id,
                    "settings": settings,
                    "sequence": sequence,
                    "uuid": uuid,
                }
            )


class _FakeMessageSender:
    """记录 IM 卡片及文本发送调用的内存桩。"""

    def __init__(self) -> None:
        self._counter = 0
        self._lock = threading.Lock()
        self.card_messages: list[dict[str, Any]] = []
        self.text_messages: list[dict[str, Any]] = []

    def send_card_message(
        self,
        *,
        chat_id: str,
        card_id: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> str | None:
        with self._lock:
            self._counter += 1
            msg_id = f"om_reply_card_{self._counter}"
            self.card_messages.append(
                {
                    "message_id": msg_id,
                    "chat_id": chat_id,
                    "card_id": card_id,
                    "reply_to_message_id": reply_to_message_id,
                    "reply_in_thread": reply_in_thread,
                }
            )
            return msg_id

    def send_text_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
        reply_in_thread: bool = False,
    ) -> str | None:
        with self._lock:
            self._counter += 1
            msg_id = f"om_reply_text_{self._counter}"
            self.text_messages.append(
                {
                    "message_id": msg_id,
                    "chat_id": chat_id,
                    "text": text,
                    "reply_to_message_id": reply_to_message_id,
                    "reply_in_thread": reply_in_thread,
                }
            )
            return msg_id


class _FakeAcpClient:
    """轻量可控内存 ACP 替身，用于精准编排并发、超时与取消测试。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running = False
        self._session_seq = 0
        self.new_session_cwds: list[Any] = []
        self.prompt_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[str] = []
        self.set_model_calls: list[tuple[str, str]] = []
        self.set_config_option_calls: list[tuple[str, str, str]] = []
        self.closed_sessions: list[str] = []
        self.models_payload: dict[str, Any] | None = None
        self.use_config_options: bool = False
        self.config_model: str = "gemini-4-argon"
        self.config_effort: str = "medium"
        self.set_model_error: Exception | None = None
        self.set_config_option_error: Exception | None = None
        self.closed = False
        self.prompt_handler: Any = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self, *, timeout: float | None = None) -> dict[str, Any]:
        self._running = True
        return {"protocolVersion": 1}

    def close(self) -> None:
        self._running = False
        self.closed = True

    def _build_config_options(self, model_id: str, effort_id: str) -> list[dict[str, Any]]:
        opts: list[dict[str, Any]] = [
            {
                "id": "model",
                "name": "Model",
                "category": "model",
                "type": "select",
                "currentValue": model_id,
                "options": [
                    {"value": "gemini-4-argon", "name": "Gemini 4 Argon"},
                    {"value": "gemini-4-hydrogen", "name": "Gemini 4 Hydrogen"},
                    {"value": "gemini-3.8-flash", "name": "Gemini 3.8 Flash"},
                    {"value": "claude-sonnet-5-5", "name": "Claude Sonnet 5.5"},
                    {"value": "gpt-oss-120b-medium", "name": "GPT-OSS 120B (Medium)"},
                ],
            }
        ]
        if model_id != "gpt-oss-120b-medium":
            levels = [
                {"value": "low", "name": "Low"},
                {"value": "medium", "name": "Medium"},
                {"value": "high", "name": "High"},
            ]
            if model_id == "gemini-4-hydrogen":
                levels.append({"value": "xhigh", "name": "Extra"})
            opts.append(
                {
                    "id": "thought_level",
                    "name": "Thinking Level",
                    "category": "thought_level",
                    "type": "select",
                    "currentValue": effort_id,
                    "options": levels,
                }
            )
        return opts

    def new_session(
        self,
        cwd: str | Path | None = None,
        mcp_servers: Any = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._session_seq += 1
            self.new_session_cwds.append(cwd)
            info: dict[str, Any] = {"sessionId": f"sess_fake_{self._session_seq}"}
            if self.models_payload is not None:
                info["models"] = self.models_payload
            if self.use_config_options:
                info["configOptions"] = self._build_config_options(
                    self.config_model,
                    self.config_effort,
                )
            return info

    def set_model(
        self,
        session_id: str,
        model_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            if self.set_model_error is not None:
                raise self.set_model_error
            self.set_model_calls.append((session_id, model_id))
            return {}

    def set_config_option(
        self,
        session_id: str,
        config_id: str,
        value: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            if self.set_config_option_error is not None:
                raise self.set_config_option_error
            self.set_config_option_calls.append((session_id, config_id, value))
            if config_id == "model":
                if value == "nonexistent-model":
                    raise RuntimeError("unknown model")
                self._session_active_model = value
                if not hasattr(self, "_session_active_effort"):
                    self._session_active_effort = self.config_effort
                if value != "gemini-4-hydrogen" and self._session_active_effort == "xhigh":
                    self._session_active_effort = "high"
            elif config_id == "thought_level":
                active_model = getattr(self, "_session_active_model", self.config_model)
                allowed = (
                    {"low", "medium", "high", "xhigh"}
                    if active_model == "gemini-4-hydrogen"
                    else (set() if active_model == "gpt-oss-120b-medium" else {"low", "medium", "high"})
                )
                if value not in allowed:
                    raise RuntimeError(f"Thinking level {value!r} not offered for {active_model}")
                self._session_active_effort = value
            active_model = getattr(self, "_session_active_model", self.config_model)
            active_effort = getattr(self, "_session_active_effort", self.config_effort)
            return {"configOptions": self._build_config_options(active_model, active_effort)}

    def close_session(self, session_id: str) -> None:
        with self._lock:
            self.closed_sessions.append(session_id)

    def prompt_async(
        self,
        session_id: str,
        prompt: Any,
        *,
        on_update: Any = None,
        message_id: str | None = None,
    ) -> Future[AcpPromptResult]:
        with self._lock:
            self.prompt_calls.append(
                {
                    "session_id": session_id,
                    "prompt": prompt,
                    "message_id": message_id,
                    "on_update": on_update,
                }
            )
        if self.prompt_handler is not None:
            return self.prompt_handler(session_id, prompt, on_update, message_id)

        future: Future[AcpPromptResult] = Future()
        if on_update is not None:
            on_update(
                AcpSessionUpdate(
                    session_id=session_id,
                    update_type="agent_message_chunk",
                    raw={"content": {"type": "text", "text": f"reply:{prompt}"}},
                )
            )
        future.set_result(
            AcpPromptResult(
                session_id=session_id,
                stop_reason="end_turn",
                text=f"reply:{prompt}",
                thoughts="",
                updates=[],
            )
        )
        return future

    def cancel(self, session_id: str) -> None:
        with self._lock:
            self.cancel_calls.append(session_id)


@dataclass
class _FakeSdkResponse:
    """模拟 lark_oapi 响应对象。"""

    ok: bool = True
    code: int = 0
    msg: str = "success"
    data: Any = None
    log_id: str = "log-mock-1"

    def success(self) -> bool:
        return self.ok

    def get_log_id(self) -> str:
        return self.log_id


class _FakeSdkClient:
    """模拟 lark_oapi.Client 资源层级，用于验证真实 SDK 请求对象构造。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.next_response: _FakeSdkResponse | None = None
        self.card_counter = 0
        self.msg_counter = 0

        outer = self

        class _CardApi:
            def create(self, request: Any) -> _FakeSdkResponse:
                outer.calls.append(("card.create", request))
                if outer.next_response is not None:
                    resp = outer.next_response
                    outer.next_response = None
                    return resp
                outer.card_counter += 1
                return _FakeSdkResponse(data=SimpleNamespace(card_id=f"card_sdk_{outer.card_counter}"))

            def settings(self, request: Any) -> _FakeSdkResponse:
                outer.calls.append(("card.settings", request))
                return outer._consume_or_ok()

            def update(self, request: Any) -> _FakeSdkResponse:
                outer.calls.append(("card.update", request))
                return outer._consume_or_ok()

        class _CardElementApi:
            def content(self, request: Any) -> _FakeSdkResponse:
                outer.calls.append(("card_element.content", request))
                return outer._consume_or_ok()

        class _MessageApi:
            def reply(self, request: Any) -> _FakeSdkResponse:
                outer.calls.append(("message.reply", request))
                if outer.next_response is not None:
                    resp = outer.next_response
                    outer.next_response = None
                    return resp
                outer.msg_counter += 1
                return _FakeSdkResponse(data=SimpleNamespace(message_id=f"om_sdk_{outer.msg_counter}"))

            def create(self, request: Any) -> _FakeSdkResponse:
                outer.calls.append(("message.create", request))
                if outer.next_response is not None:
                    resp = outer.next_response
                    outer.next_response = None
                    return resp
                outer.msg_counter += 1
                return _FakeSdkResponse(data=SimpleNamespace(message_id=f"om_sdk_{outer.msg_counter}"))

        self.cardkit = SimpleNamespace(
            v1=SimpleNamespace(card=_CardApi(), card_element=_CardElementApi())
        )
        self.im = SimpleNamespace(v1=SimpleNamespace(message=_MessageApi()))

    def _consume_or_ok(self) -> _FakeSdkResponse:
        if self.next_response is not None:
            resp = self.next_response
            self.next_response = None
            return resp
        return _FakeSdkResponse()


def _make_config(
    *,
    allowed_open_ids: frozenset[str] = frozenset({"ou_allowed_1"}),
    allow_group_mention: bool = False,
    agy_model: str = "",
    agy_effort: str = "",
    stream_throttle: float = 0.0,
    turn_timeout_seconds: int = 10,
) -> AgyLarkConfig:
    return AgyLarkConfig(
        feishu_app_id="cli_test_app",
        feishu_app_secret="sec_test_secret",
        feishu_domain="https://open.feishu.cn",
        allowed_open_ids=allowed_open_ids,
        allow_group_mention=allow_group_mention,
        agy_exe="",
        agy_model=agy_model,
        agy_effort=agy_effort,
        default_workspace=Path(tempfile.gettempdir()),
        turn_timeout_seconds=turn_timeout_seconds,
        stream_throttle=stream_throttle,
        data_dir=Path(".agy-lark-test"),
        log_level="INFO",
    )


def _make_p2_message_event(
    *,
    message_id: str = "om_msg_1",
    chat_id: str = "oc_chat_1",
    chat_type: str = "p2p",
    message_type: str = "text",
    content: str | None = None,
    text: str = "你好",
    sender_open_id: str = "ou_allowed_1",
    sender_type: str = "user",
    root_id: str | None = None,
    parent_id: str | None = None,
    thread_id: str | None = None,
    mentions: list[dict[str, Any]] | None = None,
) -> Any:
    """使用真实 lark_oapi P2ImMessageReceiveV1 模型构造离线入站消息事件。"""
    _import_lark_oapi()
    from lark_oapi.api.im.v1 import P2ImMessageReceiveV1

    payload_content = (
        content
        if content is not None
        else json.dumps({"text": text}, ensure_ascii=False)
    )
    msg_dict: dict[str, Any] = {
        "message_id": message_id,
        "chat_id": chat_id,
        "chat_type": chat_type,
        "message_type": message_type,
        "content": payload_content,
    }
    if root_id is not None:
        msg_dict["root_id"] = root_id
    if parent_id is not None:
        msg_dict["parent_id"] = parent_id
    if thread_id is not None:
        msg_dict["thread_id"] = thread_id
    if mentions is not None:
        msg_dict["mentions"] = mentions

    return P2ImMessageReceiveV1(
        {
            "schema": "2.0",
            "header": {
                "event_id": f"evt_{message_id}",
                "event_type": "im.message.receive_v1",
            },
            "event": {
                "sender": {
                    "sender_id": {"open_id": sender_open_id},
                    "sender_type": sender_type,
                },
                "message": msg_dict,
            },
        }
    )


def _make_p2_card_action_event(
    *,
    operator_open_id: str = "ou_allowed_1",
    action_value: dict[str, Any] | None = None,
    open_message_id: str = "om_card_msg_1",
    open_chat_id: str = "oc_chat_1",
) -> Any:
    """使用真实 lark_oapi P2CardActionTrigger 模型构造卡片回调事件。"""
    _import_lark_oapi()
    from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTrigger

    return P2CardActionTrigger(
        {
            "schema": "2.0",
            "header": {
                "event_id": "evt_card_action_1",
                "event_type": "card.action.trigger",
            },
            "event": {
                "operator": {"open_id": operator_open_id},
                "action": {
                    "tag": "button",
                    "value": action_value if action_value is not None else {},
                },
                "context": {
                    "open_message_id": open_message_id,
                    "open_chat_id": open_chat_id,
                },
            },
        }
    )


def _find_card_element(card_json: dict[str, Any], element_id: str) -> dict[str, Any] | None:
    for elem in card_json["body"]["elements"]:
        if elem.get("element_id") == element_id:
            return elem
    return None


class TestSdkIntegrationAndLazyImport(unittest.TestCase):
    """验证真实 lark_oapi 请求构造、响应校验与延迟导入无警告特性。"""

    def test_importing_daemon_does_not_eagerly_import_lark_ws_without_warnings(self) -> None:
        repo_src = str(Path(__file__).resolve().parents[1] / "src")
        env = dict(os.environ)
        env["PYTHONPATH"] = repo_src
        proc = subprocess.run(
            [
                sys.executable,
                "-W",
                "error",
                "-c",
                (
                    "import sys, agy_lark, agy_lark.daemon; "
                    "assert 'lark_oapi.ws' not in sys.modules"
                ),
            ],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)

    def test_import_lark_oapi_isolates_pkg_resources_deprecation_warnings(self) -> None:
        repo_src = str(Path(__file__).resolve().parents[1] / "src")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (tmp_path / "pkg_resources.py").write_text(
                "\n".join(
                    [
                        "import warnings",
                        "",
                        "def _warn(name: str) -> None:",
                        "    warnings.warn(",
                        '        f"Deprecated call to `pkg_resources.declare_namespace({name!r})`.",',
                        "        DeprecationWarning,",
                        "        stacklevel=2,",
                        "    )",
                        "",
                        "def declare_namespace(name: str) -> None:",
                        "    _warn(name)",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (tmp_path / "lark_oapi.py").write_text(
                "\n".join(
                    [
                        "import warnings",
                        'warnings.warn("direct lark_oapi deprecation", DeprecationWarning, stacklevel=1)',
                        '__import__("pkg_resources").declare_namespace("lark_oapi.ws.pb")',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            env = dict(os.environ)
            env["PYTHONPATH"] = f"{tmp_dir}{os.pathsep}{repo_src}"
            script = (
                "import warnings; "
                "from agy_lark.daemon import _import_lark_oapi; "
                "mod = _import_lark_oapi(); "
                "assert mod.__name__ == 'lark_oapi'; "
                "raised = False\n"
                "try:\n"
                "    warnings.warn('outside warning', DeprecationWarning, stacklevel=1)\n"
                "except DeprecationWarning:\n"
                "    raised = True\n"
                "assert raised, 'expected DeprecationWarning to still fail under -W error outside import'"
            )
            proc = subprocess.run(
                [sys.executable, "-W", "error", "-c", script],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)

    def test_lark_card_transport_builds_valid_cardkit_v1_requests_and_errors(self) -> None:
        _import_lark_oapi()
        from lark_oapi.api.cardkit.v1 import Card

        sdk_client = _FakeSdkClient()
        transport = LarkCardTransport(sdk_client, uuid_factory=lambda: "uuid-sdk-1")

        card_json = {
            "schema": CARD_SCHEMA_VERSION,
            "config": {"streaming_mode": True},
            "body": {"elements": []},
        }
        card_id = transport.create_card(card_json)
        self.assertEqual(card_id, "card_sdk_1")

        create_name, create_req = sdk_client.calls[0]
        self.assertEqual(create_name, "card.create")
        self.assertEqual(create_req.request_body.type, "card_json")
        self.assertEqual(json.loads(create_req.request_body.data), card_json)

        transport.stream_element_content(
            card_id,
            CONTENT_ELEMENT_ID,
            "stream body",
            sequence=1,
            uuid="uuid-s-1",
        )
        stream_name, stream_req = sdk_client.calls[1]
        self.assertEqual(stream_name, "card_element.content")
        self.assertEqual(stream_req.card_id, card_id)
        self.assertEqual(stream_req.element_id, CONTENT_ELEMENT_ID)
        self.assertEqual(stream_req.request_body.content, "stream body")
        self.assertEqual(stream_req.request_body.sequence, 1)
        self.assertEqual(stream_req.request_body.uuid, "uuid-s-1")

        transport.update_settings(
            card_id,
            {"config": {"streaming_mode": False}},
            sequence=2,
            uuid="uuid-s-2",
        )
        settings_name, settings_req = sdk_client.calls[2]
        self.assertEqual(settings_name, "card.settings")
        self.assertEqual(settings_req.card_id, card_id)
        self.assertEqual(
            json.loads(settings_req.request_body.settings),
            {"config": {"streaming_mode": False}},
        )
        self.assertEqual(settings_req.request_body.sequence, 2)

        transport.update_card(card_id, card_json, sequence=3, uuid="uuid-s-3")
        update_name, update_req = sdk_client.calls[3]
        self.assertEqual(update_name, "card.update")
        self.assertEqual(update_req.card_id, card_id)
        self.assertIsInstance(update_req.request_body.card, Card)
        self.assertEqual(update_req.request_body.card.type, "card_json")
        self.assertEqual(json.loads(update_req.request_body.card.data), card_json)
        self.assertEqual(update_req.request_body.sequence, 3)

        # 接口返回失败时应抛出含错误码与 log_id 的 LarkApiError
        sdk_client.next_response = _FakeSdkResponse(
            ok=False,
            code=230099,
            msg="invalid sequence",
            log_id="log-err-99",
        )
        with self.assertRaises(LarkApiError) as ctx:
            transport.stream_element_content(
                card_id,
                STATUS_ELEMENT_ID,
                "status",
                sequence=4,
                uuid="uuid-s-4",
            )
        self.assertEqual(ctx.exception.code, 230099)
        self.assertEqual(ctx.exception.log_id, "log-err-99")
        self.assertIn("230099", str(ctx.exception))

        # 创建成功但缺少 card_id 时显式报错
        sdk_client.next_response = _FakeSdkResponse(ok=True, data=SimpleNamespace(card_id=""))
        with self.assertRaises(LarkApiError):
            transport.create_card(card_json)

    def test_lark_message_sender_builds_reply_and_create_requests(self) -> None:
        sdk_client = _FakeSdkClient()
        sender = LarkMessageSender(sdk_client, uuid_factory=lambda: "uuid-msg-1")

        reply_id = sender.send_card_message(
            chat_id="oc_chat_1",
            card_id="card_sdk_1",
            reply_to_message_id="om_origin_1",
            reply_in_thread=True,
        )
        self.assertEqual(reply_id, "om_sdk_1")
        call_name, req = sdk_client.calls[0]
        self.assertEqual(call_name, "message.reply")
        self.assertEqual(req.message_id, "om_origin_1")
        self.assertEqual(req.request_body.msg_type, "interactive")
        self.assertTrue(req.request_body.reply_in_thread)
        self.assertEqual(
            json.loads(req.request_body.content),
            {"type": "card", "data": {"card_id": "card_sdk_1"}},
        )

        create_id = sender.send_text_message(chat_id="oc_chat_1", text="你好")
        self.assertEqual(create_id, "om_sdk_2")
        create_name, create_req = sdk_client.calls[1]
        self.assertEqual(create_name, "message.create")
        self.assertEqual(create_req.receive_id_type, "chat_id")
        self.assertEqual(create_req.request_body.receive_id, "oc_chat_1")
        self.assertEqual(create_req.request_body.msg_type, "text")
        self.assertEqual(json.loads(create_req.request_body.content), {"text": "你好"})

        sdk_client.next_response = _FakeSdkResponse(ok=False, code=999, msg="permission denied")
        with self.assertRaises(LarkApiError):
            sender.send_text_message(chat_id="oc_chat_1", text="fail")

    def test_sdk_builders_for_client_dispatcher_and_ws(self) -> None:
        lark = _import_lark_oapi()
        from lark_oapi.event.callback.model.p2_card_action_trigger import (
            P2CardActionTriggerResponse,
        )

        cfg = _make_config()
        client = build_feishu_client(cfg)
        self.assertIsInstance(client, lark.Client)

        fake_daemon = AgyLarkDaemon(
            cfg,
            acp_client=_FakeAcpClient(),
            transport=_FakeTransport(),
            message_sender=_FakeMessageSender(),
        )
        try:
            dispatcher = build_event_dispatcher(fake_daemon)
            self.assertIsInstance(dispatcher, lark.EventDispatcherHandler)

            # 直接验证注册后的卡片回调处理器返回 P2CardActionTriggerResponse
            card_evt = _make_p2_card_action_event(
                operator_open_id="ou_stranger",
                action_value=build_stop_callback_value("sess_fake_1"),
            )
            card_resp = P2CardActionTriggerResponse(fake_daemon.handle_card_action(card_evt))
            self.assertIsNotNone(card_resp.toast)
            self.assertEqual(card_resp.toast.type, "error")

            recorded_ws_args: dict[str, Any] = {}

            class _FakeWsModule:
                @staticmethod
                def Client(*args: Any, **kwargs: Any) -> SimpleNamespace:
                    recorded_ws_args["args"] = args
                    recorded_ws_args["kwargs"] = kwargs
                    return SimpleNamespace(start=lambda: None)

            with (
                mock.patch.dict(sys.modules, {"lark_oapi.ws": _FakeWsModule}),
                mock.patch.object(lark, "ws", _FakeWsModule, create=True),
            ):
                ws_client = build_ws_client(cfg, dispatcher)
                self.assertTrue(callable(ws_client.start))

            self.assertEqual(recorded_ws_args["args"], ("cli_test_app", "sec_test_secret"))
            self.assertEqual(recorded_ws_args["kwargs"]["domain"], "https://open.feishu.cn")
            self.assertEqual(recorded_ws_args["kwargs"]["log_level"], lark.LogLevel.INFO)
            self.assertTrue(recorded_ws_args["kwargs"]["auto_reconnect"])
            self.assertIs(recorded_ws_args["kwargs"]["event_handler"], dispatcher)
        finally:
            fake_daemon.close()


class TestMessageParsingAndAuthorization(unittest.TestCase):
    """验证入站消息解析、Fail-Closed 鉴权、群聊 @mention 控制与去重。"""

    def test_extract_incoming_message_filters_bots_non_text_and_empty_text(self) -> None:
        # 机器人自身发出（sender_type=app）应被忽略
        self.assertIsNone(extract_incoming_message(_make_p2_message_event(sender_type="app")))
        # 非文本消息应安全忽略
        self.assertIsNone(
            extract_incoming_message(
                _make_p2_message_event(message_type="image", content='{"image_key":"img_1"}')
            )
        )
        # 非法 JSON content 不崩溃且返回 None
        self.assertIsNone(
            extract_incoming_message(_make_p2_message_event(content="{not-json"))
        )
        # 剥离 @机器人 后为空白文本应被忽略
        only_mention = _make_p2_message_event(
            chat_type="group",
            text=" @_user_1   ",
            mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}, "name": "agy"}],
        )
        self.assertIsNone(extract_incoming_message(only_mention, bot_open_id="ou_bot_1"))

    def test_fail_closed_and_group_mention_policies(self) -> None:
        # 1) 允许列表为空：严格默认拒绝一切消息
        empty_cfg = _make_config(allowed_open_ids=frozenset())
        acp = _FakeAcpClient()
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        daemon = AgyLarkDaemon(
            empty_cfg,
            acp_client=acp,
            transport=transport,
            message_sender=sender,
        )
        try:
            self.assertIsNone(
                daemon.dispatch_message_event(_make_p2_message_event(sender_open_id="ou_allowed_1"))
            )
            self.assertEqual(acp.prompt_calls, [])
            self.assertEqual(transport.created_cards, [])
        finally:
            daemon.close()

        # 2) 私聊鉴权 + 群聊开关关闭时拒绝 @mention
        dm_only_cfg = _make_config(
            allowed_open_ids=frozenset({"ou_allowed_1"}),
            allow_group_mention=False,
        )
        acp2 = _FakeAcpClient()
        daemon2 = AgyLarkDaemon(
            dm_only_cfg,
            acp_client=acp2,
            transport=_FakeTransport(),
            message_sender=_FakeMessageSender(),
            bot_open_id="ou_bot_1",
        )
        try:
            # 非白名单私聊拒绝
            self.assertIsNone(
                daemon2.dispatch_message_event(_make_p2_message_event(sender_open_id="ou_stranger"))
            )
            # 群聊开关关闭：白名单用户 @机器人 仍被拒绝
            self.assertIsNone(
                daemon2.dispatch_message_event(
                    _make_p2_message_event(
                        message_id="om_grp_1",
                        chat_type="group",
                        text="@_user_1 帮我分析",
                        mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}, "name": "agy"}],
                    )
                )
            )
            self.assertEqual(acp2.prompt_calls, [])
        finally:
            daemon2.close()

        # 3) 群聊开启后仅在白名单用户确实 @机器人 时响应，并清洗文本且对同 message_id 去重
        grp_cfg = _make_config(
            allowed_open_ids=frozenset({"ou_allowed_1"}),
            allow_group_mention=True,
        )
        acp3 = _FakeAcpClient()
        daemon3 = AgyLarkDaemon(
            grp_cfg,
            acp_client=acp3,
            transport=_FakeTransport(),
            message_sender=_FakeMessageSender(),
            bot_open_id="ou_bot_1",
        )
        try:
            # 群聊未 @ 不触发
            self.assertIsNone(
                daemon3.dispatch_message_event(
                    _make_p2_message_event(message_id="om_no_at", chat_type="group", text="普通聊天")
                )
            )
            # 群聊仅 @其他人 不触发
            self.assertIsNone(
                daemon3.dispatch_message_event(
                    _make_p2_message_event(
                        message_id="om_other_at",
                        chat_type="group",
                        text="@_user_1 看看这个",
                        mentions=[{"key": "@_user_1", "id": {"open_id": "ou_colleague"}, "name": "Bob"}],
                    )
                )
            )
            # 群聊 @机器人：触发，同时还原其余提及对象昵称
            grp_hit = _make_p2_message_event(
                message_id="om_bot_at",
                chat_type="group",
                text="@_user_1 请帮 @_user_2 审查",
                mentions=[
                    {"key": "@_user_1", "id": {"open_id": "ou_bot_1"}, "name": "agy"},
                    {"key": "@_user_2", "id": {"open_id": "ou_colleague"}, "name": "Alice"},
                ],
            )
            fut = daemon3.dispatch_message_event(grp_hit)
            assert fut is not None
            fut.result(timeout=5.0)

            # 相同 message_id 重复推送应被幂等去重忽略
            self.assertIsNone(daemon3.dispatch_message_event(grp_hit))

            self.assertEqual(len(acp3.prompt_calls), 1)
            self.assertEqual(acp3.prompt_calls[0]["prompt"], "请帮 @Alice 审查")
        finally:
            daemon3.close()


class TestConversationMappingAndMultiTurn(unittest.TestCase):
    """验证私聊与群聊话题根映射到稳定 ACP session_id。"""

    def test_dm_and_group_thread_root_session_stability(self) -> None:
        cfg = _make_config(allow_group_mention=True)
        acp = _FakeAcpClient()
        sender = _FakeMessageSender()
        daemon = AgyLarkDaemon(
            cfg,
            acp_client=acp,
            transport=_FakeTransport(),
            message_sender=sender,
            bot_open_id="ou_bot_1",
        )
        try:
            # 私聊两轮复用同一个 session_id
            f1 = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_dm_1", chat_id="oc_dm_1", text="第一问")
            )
            assert f1 is not None
            f1.result(timeout=5.0)
            f2 = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_dm_2", chat_id="oc_dm_1", text="第二问")
            )
            assert f2 is not None
            f2.result(timeout=5.0)

            self.assertEqual(acp.prompt_calls[0]["session_id"], acp.prompt_calls[1]["session_id"])
            dm_sid = acp.prompt_calls[0]["session_id"]

            # 群聊话题首条消息
            f3 = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_g_root_1",
                    chat_id="oc_group_1",
                    chat_type="group",
                    text="@_user_1 话题一",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f3 is not None
            f3.result(timeout=5.0)
            grp_sid_1 = acp.prompt_calls[2]["session_id"]
            self.assertNotEqual(grp_sid_1, dm_sid)

            # 同话题后续跟进（携带 root_id 与 thread_id）应复用 grp_sid_1
            f4 = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_g_follow_1",
                    chat_id="oc_group_1",
                    chat_type="group",
                    root_id="om_g_root_1",
                    thread_id="omt_thread_1",
                    text="@_user_1 继续追问",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f4 is not None
            f4.result(timeout=5.0)
            self.assertEqual(acp.prompt_calls[3]["session_id"], grp_sid_1)

            # 同 thread_id 的下一条消息即使缺省 root_id 也通过 thread 别名复用 grp_sid_1
            f5 = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_g_follow_2",
                    chat_id="oc_group_1",
                    chat_type="group",
                    thread_id="omt_thread_1",
                    text="@_user_1 第三问",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f5 is not None
            f5.result(timeout=5.0)
            self.assertEqual(acp.prompt_calls[4]["session_id"], grp_sid_1)

            # 直接回复机器人发出的卡片消息（仅含 parent_id，无 root_id/thread_id）也复用 grp_sid_1
            card_reply_msg_id = sender.card_messages[2]["message_id"]
            f5_parent = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_g_follow_parent",
                    chat_id="oc_group_1",
                    chat_type="group",
                    parent_id=card_reply_msg_id,
                    text="@_user_1 引用卡片追问",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f5_parent is not None
            f5_parent.result(timeout=5.0)
            self.assertEqual(acp.prompt_calls[5]["session_id"], grp_sid_1)

            # 群内新开一条非回复消息应开启独立的 grp_sid_2
            f6 = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_g_root_2",
                    chat_id="oc_group_1",
                    chat_type="group",
                    text="@_user_1 新话题",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f6 is not None
            f6.result(timeout=5.0)
            grp_sid_2 = acp.prompt_calls[6]["session_id"]
            self.assertNotEqual(grp_sid_2, grp_sid_1)
        finally:
            daemon.close()


class TestStreamingEndToEndAndErrors(unittest.TestCase):
    """通过真实 AgyAcpClient（模拟 stdio 子进程）验证端到端流式卡片闭环与异常/超时收口。"""

    def _make_stdio_acp_client(self, *, timeout: float = 5.0) -> AgyAcpClient:
        return AgyAcpClient(
            executable=sys.executable,
            args=["-u", "-c", _MOCK_DAEMON_ACP_SCRIPT],
            cwd=tempfile.gettempdir(),
            default_timeout=timeout,
        )

    def test_end_to_end_streaming_card_completion_and_error_finalization(self) -> None:
        cfg = _make_config(stream_throttle=0.0, turn_timeout_seconds=5)
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        acp_client = self._make_stdio_acp_client()
        daemon = AgyLarkDaemon(
            cfg,
            acp_client=acp_client,
            transport=transport,
            message_sender=sender,
        )
        try:
            daemon.start()
            self.assertTrue(acp_client.is_running)

            # 1) 正常流式回合闭环
            f1 = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_stream_1", text="stream:请分析代码")
            )
            assert f1 is not None
            f1.result(timeout=5.0)

            self.assertEqual(len(transport.created_cards), 1)
            card_id_1 = transport.created_cards[0]["card_id"]
            self.assertEqual(sender.card_messages[0]["card_id"], card_id_1)
            self.assertEqual(sender.card_messages[0]["reply_to_message_id"], "om_stream_1")

            self.assertEqual(len(transport.update_settings_calls), 1)
            self.assertEqual(
                transport.update_settings_calls[0]["settings"],
                {"config": {"streaming_mode": False}},
            )
            final_card = transport.update_card_calls[0]["card_json"]
            self.assertEqual(final_card["header"]["template"], "green")
            self.assertIsNone(_find_card_element(final_card, STOP_BUTTON_ELEMENT_ID))
            status_el = _find_card_element(final_card, STATUS_ELEMENT_ID)
            content_el = _find_card_element(final_card, CONTENT_ELEMENT_ID)
            assert status_el is not None and content_el is not None
            self.assertIn("88 tokens", status_el["content"])
            self.assertEqual(content_el["content"], "第一段，第二段。")

            # 2) ACP 返回异常时优雅收口为红色错误卡片
            with self.assertLogs("agy_lark.daemon", level="ERROR") as err_logs:
                f2 = daemon.dispatch_message_event(
                    _make_p2_message_event(message_id="om_err_1", text="error:trigger")
                )
                assert f2 is not None
                f2.result(timeout=5.0)
            self.assertTrue(any("simulated acp failure" in msg for msg in err_logs.output))

            err_card = transport.update_card_calls[1]["card_json"]
            self.assertEqual(err_card["header"]["template"], "red")
            err_status = _find_card_element(err_card, STATUS_ELEMENT_ID)
            assert err_status is not None
            self.assertIn("simulated acp failure", err_status["content"])
        finally:
            daemon.close()
        self.assertFalse(acp_client.is_running)

    def test_turn_timeout_cancels_acp_and_marks_card_error(self) -> None:
        cfg = _make_config(stream_throttle=0.0, turn_timeout_seconds=1)
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        fake_acp = _FakeAcpClient()
        hanging_future: Future[AcpPromptResult] = Future()
        fake_acp.prompt_handler = lambda *_args: hanging_future

        daemon = AgyLarkDaemon(
            cfg,
            acp_client=fake_acp,
            transport=transport,
            message_sender=sender,
        )
        try:
            # 使用短超时快速验证超时分支
            with self.assertLogs("agy_lark.daemon", level="WARNING") as warn_logs:
                fut = daemon.dispatch_message_event(
                    _make_p2_message_event(message_id="om_timeout_1", text="hang")
                )
                assert fut is not None
                fut.result(timeout=5.0)
            self.assertTrue(any("超时" in msg for msg in warn_logs.output))

            self.assertEqual(fake_acp.cancel_calls, ["sess_fake_1"])
            self.assertEqual(len(transport.update_card_calls), 1)
            timeout_card = transport.update_card_calls[0]["card_json"]
            self.assertEqual(timeout_card["header"]["template"], "red")
            status_el = _find_card_element(timeout_card, STATUS_ELEMENT_ID)
            assert status_el is not None
            self.assertIn("超时", status_el["content"])
        finally:
            daemon.close()

    def test_daemon_close_cancels_inflight_and_closes_acp_client(self) -> None:
        cfg = _make_config(stream_throttle=0.0, turn_timeout_seconds=10)
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        acp_client = self._make_stdio_acp_client(timeout=10.0)
        daemon = AgyLarkDaemon(
            cfg,
            acp_client=acp_client,
            transport=transport,
            message_sender=sender,
        )

        partial_seen = threading.Event()
        orig_stream = transport.stream_element_content

        def _watch_stream(card_id, element_id, content, *, sequence, uuid):
            orig_stream(card_id, element_id, content, sequence=sequence, uuid=uuid)
            if element_id == CONTENT_ELEMENT_ID and "部分输出" in content:
                partial_seen.set()

        transport.stream_element_content = _watch_stream  # type: ignore[method-assign]

        fut = daemon.dispatch_message_event(
            _make_p2_message_event(message_id="om_close_inflight_1", text="cancel_wait:退出测试")
        )
        assert fut is not None
        self.assertTrue(partial_seen.wait(timeout=4.0))

        daemon.close()
        fut.result(timeout=5.0)
        self.assertFalse(acp_client.is_running)
        self.assertEqual(len(transport.update_card_calls), 1)
        self.assertEqual(transport.update_card_calls[0]["card_json"]["header"]["template"], "orange")


class TestSteeringAndCancellation(unittest.TestCase):
    """验证运行中插话、/stop 命令与 card.action.trigger 停止按钮回调。"""

    def test_mid_turn_steering_reuses_active_card_and_session(self) -> None:
        cfg = _make_config(stream_throttle=0.0, turn_timeout_seconds=5)
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        acp_client = AgyAcpClient(
            executable=sys.executable,
            args=["-u", "-c", _MOCK_DAEMON_ACP_SCRIPT],
            cwd=tempfile.gettempdir(),
            default_timeout=5.0,
        )
        daemon = AgyLarkDaemon(
            cfg,
            acp_client=acp_client,
            transport=transport,
            message_sender=sender,
        )

        # 通过包装 transport 捕捉首个流式内容到达信号，确保在途后发送插话
        first_chunk_seen = threading.Event()
        orig_stream = transport.stream_element_content

        def _watch_stream(card_id, element_id, content, *, sequence, uuid):
            orig_stream(card_id, element_id, content, sequence=sequence, uuid=uuid)
            if element_id == CONTENT_ELEMENT_ID and "初始阶段;" in content:
                first_chunk_seen.set()

        transport.stream_element_content = _watch_stream  # type: ignore[method-assign]

        try:
            f_main = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_steer_1", text="steer_hold:主任务")
            )
            assert f_main is not None
            self.assertTrue(first_chunk_seen.wait(timeout=4.0), "首个流式块未在预期内到达")

            f_steer = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_steer_2", text="改成只输出摘要")
            )
            assert f_steer is not None

            f_steer.result(timeout=5.0)
            f_main.result(timeout=5.0)

            # 全程只创建一张卡片，未另起第二张卡片
            self.assertEqual(len(transport.created_cards), 1)
            self.assertEqual(len(sender.card_messages), 1)

            status_history = [
                call["content"]
                for call in transport.stream_calls
                if call["element_id"] == STATUS_ELEMENT_ID
            ]
            self.assertIn("🧭 收到插话指令，正在调整处理...", status_history)

            final_card = transport.update_card_calls[0]["card_json"]
            self.assertEqual(final_card["header"]["template"], "green")
            content_el = _find_card_element(final_card, CONTENT_ELEMENT_ID)
            assert content_el is not None
            self.assertEqual(content_el["content"], "初始阶段;已纠偏:改成只输出摘要")
        finally:
            daemon.close()

    def test_stop_button_callback_checks_authorization_turn_id_and_cancels(self) -> None:
        cfg = _make_config(stream_throttle=0.0, turn_timeout_seconds=5)
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        acp_client = AgyAcpClient(
            executable=sys.executable,
            args=["-u", "-c", _MOCK_DAEMON_ACP_SCRIPT],
            cwd=tempfile.gettempdir(),
            default_timeout=5.0,
        )
        daemon = AgyLarkDaemon(
            cfg,
            acp_client=acp_client,
            transport=transport,
            message_sender=sender,
        )

        partial_seen = threading.Event()
        orig_stream = transport.stream_element_content

        def _watch_stream(card_id, element_id, content, *, sequence, uuid):
            orig_stream(card_id, element_id, content, sequence=sequence, uuid=uuid)
            if element_id == CONTENT_ELEMENT_ID and "部分输出" in content:
                partial_seen.set()

        transport.stream_element_content = _watch_stream  # type: ignore[method-assign]

        try:
            f_run = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_cancel_btn_1", text="cancel_wait:长任务")
            )
            assert f_run is not None
            self.assertTrue(partial_seen.wait(timeout=4.0), "在途回合未发出首个输出块")

            # 从初始卡片提取真实停止按钮回调 payload（包含 session_id 与 turn_id）
            stop_btn = _find_card_element(
                transport.created_cards[0]["card_json"],
                STOP_BUTTON_ELEMENT_ID,
            )
            assert stop_btn is not None
            stop_val = stop_btn["behaviors"][0]["value"]

            # 1) 非白名单用户点击停止按钮：拒绝并返回错误 toast，不中止任务
            unauth_resp = daemon.handle_card_action(
                _make_p2_card_action_event(
                    operator_open_id="ou_stranger",
                    action_value=stop_val,
                )
            )
            self.assertEqual(unauth_resp["toast"]["type"], "error")
            self.assertFalse(f_run.done())

            # 2) 过期的旧 turn_id 不应误杀当前运行回合
            stale_val = build_stop_callback_value(stop_val["session_id"], turn_id="stale-turn")
            stale_resp = daemon.handle_card_action(
                _make_p2_card_action_event(
                    operator_open_id="ou_allowed_1",
                    action_value=stale_val,
                )
            )
            self.assertEqual(stale_resp["toast"]["content"], "当前任务已结束")
            self.assertFalse(f_run.done())

            # 3) 授权用户点击匹配的停止按钮：触发 mark_stopping 与 session/cancel
            ok_resp = daemon.handle_card_action(
                _make_p2_card_action_event(
                    operator_open_id="ou_allowed_1",
                    action_value=stop_val,
                )
            )
            self.assertEqual(ok_resp["toast"]["type"], "info")
            self.assertIn("正在停止", ok_resp["toast"]["content"])

            f_run.result(timeout=5.0)
            final_card = transport.update_card_calls[0]["card_json"]
            self.assertEqual(final_card["header"]["template"], "orange")
            content_el = _find_card_element(final_card, CONTENT_ELEMENT_ID)
            assert content_el is not None
            self.assertEqual(content_el["content"], "部分输出")

            status_history = [
                call["content"]
                for call in transport.stream_calls
                if call["element_id"] == STATUS_ELEMENT_ID
            ]
            self.assertIn("🛑 正在停止当前任务...", status_history)
        finally:
            daemon.close()

    def test_slash_stop_command_cancels_active_turn_and_handles_idle_gracefully(self) -> None:
        cfg = _make_config(allow_group_mention=True, stream_throttle=0.0, turn_timeout_seconds=5)
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        acp_client = AgyAcpClient(
            executable=sys.executable,
            args=["-u", "-c", _MOCK_DAEMON_ACP_SCRIPT],
            cwd=tempfile.gettempdir(),
            default_timeout=5.0,
        )
        daemon = AgyLarkDaemon(
            cfg,
            acp_client=acp_client,
            transport=transport,
            message_sender=sender,
            bot_open_id="ou_bot_1",
        )

        partial_seen = threading.Event()
        orig_stream = transport.stream_element_content

        def _watch_stream(card_id, element_id, content, *, sequence, uuid):
            orig_stream(card_id, element_id, content, sequence=sequence, uuid=uuid)
            if element_id == CONTENT_ELEMENT_ID and "部分输出" in content:
                partial_seen.set()

        transport.stream_element_content = _watch_stream  # type: ignore[method-assign]

        try:
            # 空闲会话发 /stop：友好提示，不崩溃
            f_idle = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_idle_stop", text="/stop")
            )
            assert f_idle is not None
            f_idle.result(timeout=5.0)
            self.assertEqual(len(sender.text_messages), 1)
            self.assertIn("暂无正在运行的任务", sender.text_messages[0]["text"])

            # 群聊开启在途任务后通过 `@机器人 /stop` 停止
            f_grp = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_grp_run",
                    chat_id="oc_grp_stop",
                    chat_type="group",
                    text="@_user_1 cancel_wait:群任务",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f_grp is not None
            self.assertTrue(partial_seen.wait(timeout=4.0))

            f_stop = daemon.dispatch_message_event(
                _make_p2_message_event(
                    message_id="om_grp_stop_cmd",
                    chat_id="oc_grp_stop",
                    chat_type="group",
                    root_id="om_grp_run",
                    text="@_user_1 /stop",
                    mentions=[{"key": "@_user_1", "id": {"open_id": "ou_bot_1"}}],
                )
            )
            assert f_stop is not None
            f_stop.result(timeout=5.0)
            f_grp.result(timeout=5.0)

            self.assertEqual(len(transport.update_card_calls), 1)
            self.assertEqual(transport.update_card_calls[0]["card_json"]["header"]["template"], "orange")
            self.assertEqual(CardRunState.CANCELLED.value, "cancelled")
        finally:
            daemon.close()


class TestPackagingAndCli(unittest.TestCase):
    """验证 pyproject.toml 脚本注册、公共导出与 main() CLI 解析。"""

    def test_pyproject_scripts_and_package_exports(self) -> None:
        pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        self.assertEqual(data.get("project", {}).get("version"), agy_lark.__version__)
        scripts = data.get("project", {}).get("scripts", {})
        self.assertEqual(scripts.get("agy-lark"), "agy_lark.daemon:main")

        for symbol in (
            "AgyLarkDaemon",
            "LarkApiError",
            "LarkCardTransport",
            "LarkMessageSender",
            "MessageSender",
            "build_event_dispatcher",
            "build_feishu_client",
            "build_ws_client",
            "extract_incoming_message",
            "main",
        ):
            self.assertTrue(hasattr(agy_lark, symbol), f"缺少导出符号: {symbol}")

    def test_main_cli_validation_and_runner_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            exe_name = "agy.exe" if os.name == "nt" else "agy"
            fake_agy = tmp_path / exe_name
            fake_agy.write_text("mock agy", encoding="utf-8")
            if os.name != "nt":
                fake_agy.chmod(0o755)

            # 1) 缺失飞书密钥时返回退出码 1
            missing_env = tmp_path / "missing.env"
            missing_env.write_text(f"AGY_EXE={fake_agy}\n", encoding="utf-8")
            stderr_buf = StringIO()
            with mock.patch("sys.stderr", stderr_buf):
                rc_missing = main(
                    ["--env-file", str(missing_env)],
                    environ={"PATH": ""},
                )
            self.assertEqual(rc_missing, 1)
            self.assertIn("FEISHU_APP_ID", stderr_buf.getvalue())

            # 2) 完整配置 + --verbose 正常启动并清理退出
            valid_env = tmp_path / "valid.env"
            valid_env.write_text(
                "\n".join(
                    [
                        "FEISHU_APP_ID=cli_cli_test",
                        "FEISHU_APP_SECRET=sec_cli_test",
                        "AGY_LARK_ALLOWED_OPEN_IDS=ou_cli_user",
                        f"AGY_EXE={fake_agy}",
                    ]
                ),
                encoding="utf-8",
            )
            lifecycle: list[str] = []
            captured_cfg: list[AgyLarkConfig] = []

            class _FakeRunnerDaemon:
                def __init__(self, cfg: AgyLarkConfig) -> None:
                    captured_cfg.append(cfg)

                def run_forever(self) -> None:
                    lifecycle.append("run_forever")

                def close(self) -> None:
                    lifecycle.append("close")

            with mock.patch("agy_lark.daemon._configure_logging") as mock_cfg_log:
                rc_ok = main(
                    ["--env-file", str(valid_env), "--verbose"],
                    daemon_factory=_FakeRunnerDaemon,  # type: ignore[arg-type]
                    environ={"PATH": ""},
                )
            self.assertEqual(rc_ok, 0)
            mock_cfg_log.assert_called_once_with("DEBUG")
            self.assertEqual(lifecycle, ["run_forever", "close"])
            self.assertEqual(captured_cfg[0].feishu_app_id, "cli_cli_test")
            self.assertEqual(captured_cfg[0].allowed_open_ids, frozenset({"ou_cli_user"}))

    def test_main_cli_resolves_portable_workspace_and_rejects_missing_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            runtime_dir = tmp_path / "portable_runtime"
            workspace_dir = runtime_dir / "workspace"
            workspace_dir.mkdir(parents=True)

            exe_name = "agy.exe" if os.name == "nt" else "agy"
            fake_agy = tmp_path / exe_name
            fake_agy.write_text("mock agy", encoding="utf-8")
            if os.name != "nt":
                fake_agy.chmod(0o755)

            # 1) 相对路径 ./workspace 在有效目录存在时正确映射到 .env 所在目录
            portable_env = runtime_dir / ".env"
            portable_env.write_text(
                "\n".join(
                    [
                        "FEISHU_APP_ID=cli_cli_test",
                        "FEISHU_APP_SECRET=sec_cli_test",
                        "AGY_LARK_ALLOWED_OPEN_IDS=ou_cli_user",
                        f"AGY_EXE={fake_agy}",
                        "AGY_DEFAULT_WORKSPACE=./workspace",
                    ]
                ),
                encoding="utf-8",
            )
            captured_cfg: list[AgyLarkConfig] = []

            class _FakeRunner:
                def __init__(self, cfg: AgyLarkConfig) -> None:
                    captured_cfg.append(cfg)

                def run_forever(self) -> None:
                    return None

                def close(self) -> None:
                    return None

            with mock.patch("agy_lark.daemon._configure_logging"):
                rc_ok = main(
                    ["--env-file", str(portable_env)],
                    daemon_factory=_FakeRunner,  # type: ignore[arg-type]
                    environ={"PATH": ""},
                )
            self.assertEqual(rc_ok, 0)
            self.assertEqual(captured_cfg[0].default_workspace, workspace_dir.resolve())

            # 2) 配置了不存在的工作区时直接报错退出，不自动创建目录也不启动守护进程
            missing_ws = runtime_dir / "nonexistent_ws"
            bad_env = runtime_dir / ".bad_ws.env"
            bad_env.write_text(
                "\n".join(
                    [
                        "FEISHU_APP_ID=cli_cli_test",
                        "FEISHU_APP_SECRET=sec_cli_test",
                        "AGY_LARK_ALLOWED_OPEN_IDS=ou_cli_user",
                        f"AGY_EXE={fake_agy}",
                        "AGY_DEFAULT_WORKSPACE=./nonexistent_ws",
                    ]
                ),
                encoding="utf-8",
            )
            called_factory = False

            def _unexpected_factory(cfg: AgyLarkConfig) -> Any:
                nonlocal called_factory
                called_factory = True
                return _FakeRunner(cfg)

            stderr_buf = StringIO()
            with mock.patch("sys.stderr", stderr_buf):
                rc_bad = main(
                    ["--env-file", str(bad_env)],
                    daemon_factory=_unexpected_factory,
                    environ={"PATH": ""},
                )
            self.assertEqual(rc_bad, 1)
            self.assertFalse(called_factory)
            self.assertFalse(missing_ws.exists())
            self.assertIn("工作区目录不存在", stderr_buf.getvalue())

            # 3) 编程式直接调用 AgyLarkDaemon.start() 同样校验工作区存在性，run_forever 日志打印 workspace
            missing_cfg = AgyLarkConfig(
                feishu_app_id="cli_cli_test",
                feishu_app_secret="sec_cli_test",
                allowed_open_ids=frozenset({"ou_cli_user"}),
                default_workspace=missing_ws,
            )
            bad_daemon = AgyLarkDaemon(
                missing_cfg,
                acp_client=_FakeAcpClient(),
                transport=_FakeTransport(),
                message_sender=_FakeMessageSender(),
            )
            try:
                with self.assertRaises(FileNotFoundError):
                    bad_daemon.start()
            finally:
                bad_daemon.close()

            good_cfg = AgyLarkConfig(
                feishu_app_id="cli_cli_test",
                feishu_app_secret="sec_cli_test",
                allowed_open_ids=frozenset({"ou_cli_user"}),
                default_workspace=workspace_dir.resolve(),
            )
            good_daemon = AgyLarkDaemon(
                good_cfg,
                acp_client=_FakeAcpClient(),
                transport=_FakeTransport(),
                message_sender=_FakeMessageSender(),
                ws_client=SimpleNamespace(start=lambda: None, stop=lambda: None),
            )
            with self.assertLogs("agy_lark.daemon", level="INFO") as info_logs:
                good_daemon.run_forever()
            self.assertTrue(
                any(f"workspace={workspace_dir.resolve()}" in line for line in info_logs.output)
            )


def _extract_selector_buttons(card_json: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        elem
        for elem in card_json["body"]["elements"]
        if elem.get("tag") == "button"
    ]


class TestModelAndEffortConfigAndCommands(unittest.TestCase):
    """验证 AGY_MODEL / AGY_EFFORT 默认配置、/model 与 /effort 交互卡片及解耦切换闭环。"""

    @staticmethod
    def _sample_legacy_models(current: str = "gemini-4-argon-medium") -> dict[str, Any]:
        return {
            "currentModelId": current,
            "availableModels": [
                {"modelId": "gemini-4-argon-medium", "name": "Gemini 4 Argon (Medium)"},
                {"modelId": "gemini-3.8-flash-medium", "name": "Gemini 3.8 Flash (Medium)"},
                {"modelId": "claude-sonnet-5-5-medium", "name": "Claude Sonnet 5.5 (Medium)"},
            ],
        }

    def test_agy_model_and_effort_auto_apply_on_new_session_and_cleanup_errors(self) -> None:
        # 1) 使用 configOptions 时，AGY_MODEL 与 AGY_EFFORT 分别通过 set_config_option 应用一次
        acp = _FakeAcpClient()
        acp.use_config_options = True
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        daemon = AgyLarkDaemon(
            _make_config(agy_model="gemini-3.8-flash", agy_effort="high"),
            acp_client=acp,
            transport=transport,
            message_sender=sender,
        )
        try:
            f1 = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_cfg_1", text="你好")
            )
            assert f1 is not None
            f1.result(timeout=5.0)
            self.assertEqual(
                acp.set_config_option_calls,
                [
                    ("sess_fake_1", "model", "gemini-3.8-flash"),
                    ("sess_fake_1", "thought_level", "high"),
                ],
            )
            self.assertEqual(acp.set_model_calls, [])

            # 同一会话下一轮消息不重复下发配置
            f2 = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_cfg_2", text="继续")
            )
            assert f2 is not None
            f2.result(timeout=5.0)
            self.assertEqual(len(acp.set_config_option_calls), 2)
        finally:
            daemon.close()

        # 2) 模型与思考强度均已匹配默认上报时跳过冗余 RPC；无 thought_level 的模型优雅跳过 AGY_EFFORT
        acp_skip = _FakeAcpClient()
        acp_skip.use_config_options = True
        daemon_skip = AgyLarkDaemon(
            _make_config(agy_model="gpt-oss-120b-medium", agy_effort="high"),
            acp_client=acp_skip,
            transport=_FakeTransport(),
            message_sender=_FakeMessageSender(),
        )
        try:
            fs = daemon_skip.dispatch_message_event(
                _make_p2_message_event(message_id="om_skip_1", text="你好")
            )
            assert fs is not None
            fs.result(timeout=5.0)
            self.assertEqual(
                acp_skip.set_config_option_calls,
                [("sess_fake_1", "model", "gpt-oss-120b-medium")],
            )
        finally:
            daemon_skip.close()

        # 3) 建会话阶段设置配置失败时关闭僵尸会话、回复错误且不触发 prompt_async
        acp_err = _FakeAcpClient()
        acp_err.use_config_options = True
        sender_err = _FakeMessageSender()
        daemon_err = AgyLarkDaemon(
            _make_config(agy_model="nonexistent-model"),
            acp_client=acp_err,
            transport=_FakeTransport(),
            message_sender=sender_err,
        )
        try:
            fe = daemon_err.dispatch_message_event(
                _make_p2_message_event(message_id="om_err_1", text="你好")
            )
            assert fe is not None
            fe.result(timeout=5.0)
            self.assertEqual(acp_err.closed_sessions, ["sess_fake_1"])
            self.assertEqual(acp_err.prompt_calls, [])
            self.assertIn("设置模型 nonexistent-model 失败", sender_err.text_messages[0]["text"])
            self.assertIsNone(daemon_err.get_session_id_for_key("p2p:oc_chat_1"))
        finally:
            daemon_err.close()

        # 4) 仅返回 legacy models（无 configOptions）时，回退调用 set_model
        acp_legacy = _FakeAcpClient()
        acp_legacy.models_payload = self._sample_legacy_models("gemini-4-argon-medium")
        daemon_legacy = AgyLarkDaemon(
            _make_config(agy_model="gemini-3.8-flash-medium"),
            acp_client=acp_legacy,
            transport=_FakeTransport(),
            message_sender=_FakeMessageSender(),
        )
        try:
            fl = daemon_legacy.dispatch_message_event(
                _make_p2_message_event(message_id="om_legacy_1", text="你好")
            )
            assert fl is not None
            fl.result(timeout=5.0)
            self.assertEqual(acp_legacy.set_model_calls, [("sess_fake_1", "gemini-3.8-flash-medium")])
            self.assertEqual(acp_legacy.set_config_option_calls, [])
        finally:
            daemon_legacy.close()

    def test_slash_model_and_effort_are_decoupled_and_persist_across_restart(self) -> None:
        acp = _FakeAcpClient()
        acp.use_config_options = True
        acp.models_payload = self._sample_legacy_models("gemini-4-argon-medium")
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        daemon = AgyLarkDaemon(
            _make_config(),
            acp_client=acp,
            transport=transport,
            message_sender=sender,
        )
        try:
            # 1) 发送 /model：只展示纯粹的 5 个基础模型（优先于 legacy 笛卡尔积列表）
            f_model = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_model_cmd_1", text="  /Model  ")
            )
            assert f_model is not None
            f_model.result(timeout=5.0)

            self.assertEqual(acp.prompt_calls, [])
            model_msg_id = sender.card_messages[0]["message_id"]
            model_card = transport.created_cards[0]["card_json"]
            m_buttons = _extract_selector_buttons(model_card)
            self.assertEqual(len(m_buttons), 5)
            self.assertEqual(m_buttons[0]["type"], "primary")
            self.assertEqual(m_buttons[0]["text"]["content"], "✓ Gemini 4 Argon")

            # 点击选择 Gemini 4 Hydrogen（不改变思考强度，但会解锁 Extra 档位）
            hydrogen_val = m_buttons[1]["behaviors"][0]["value"]
            self.assertEqual(
                parse_model_select_callback_value(hydrogen_val).model_id,  # type: ignore[union-attr]
                "gemini-4-hydrogen",
            )

            # 非白名单用户拒绝
            unauth_evt = _make_p2_card_action_event(
                operator_open_id="ou_stranger",
                action_value=hydrogen_val,
                open_message_id=model_msg_id,
            )
            unauth_resp = daemon.handle_card_action(unauth_evt)
            self.assertEqual(unauth_resp["toast"]["type"], "error")
            self.assertEqual(acp.set_config_option_calls, [])

            # 白名单点击：仅下发 configOptions.model 并原地刷新 /model 卡片
            ok_model_evt = _make_p2_card_action_event(
                operator_open_id="ou_allowed_1",
                action_value=hydrogen_val,
                open_message_id=model_msg_id,
            )
            ok_model_resp = daemon.handle_card_action(ok_model_evt)
            self.assertEqual(ok_model_resp["toast"]["type"], "success")
            self.assertIn("Gemini 4 Hydrogen", ok_model_resp["toast"]["content"])
            self.assertEqual(
                acp.set_config_option_calls,
                [("sess_fake_1", "model", "gemini-4-hydrogen")],
            )
            self.assertEqual(transport.update_card_calls[0]["sequence"], 1)
            m_updated_btns = _extract_selector_buttons(transport.update_card_calls[0]["card_json"])
            self.assertEqual(m_updated_btns[1]["type"], "primary")
            self.assertEqual(m_updated_btns[1]["text"]["content"], "✓ Gemini 4 Hydrogen")

            # 2) 发送 /effort：展示 Gemini 4 Hydrogen 专属的 4 个思考强度（含 Extra）
            f_effort = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_effort_cmd_1", text="  /Effort ")
            )
            assert f_effort is not None
            f_effort.result(timeout=5.0)
            effort_msg_id = sender.card_messages[1]["message_id"]
            effort_card = transport.created_cards[1]["card_json"]
            e_buttons = _extract_selector_buttons(effort_card)
            self.assertEqual(len(e_buttons), 4)
            self.assertEqual(e_buttons[1]["type"], "primary")
            self.assertEqual(e_buttons[1]["text"]["content"], "✓ Medium")

            xhigh_val = e_buttons[3]["behaviors"][0]["value"]
            self.assertEqual(
                parse_effort_select_callback_value(xhigh_val).effort_id,  # type: ignore[union-attr]
                "xhigh",
            )
            ok_effort_evt = _make_p2_card_action_event(
                operator_open_id="ou_allowed_1",
                action_value=xhigh_val,
                open_message_id=effort_msg_id,
            )
            ok_effort_resp = daemon.handle_card_action(ok_effort_evt)
            self.assertEqual(ok_effort_resp["toast"]["type"], "success")
            self.assertIn("Extra", ok_effort_resp["toast"]["content"])
            self.assertEqual(
                acp.set_config_option_calls,
                [
                    ("sess_fake_1", "model", "gemini-4-hydrogen"),
                    ("sess_fake_1", "thought_level", "xhigh"),
                ],
            )
            self.assertEqual(transport.update_card_calls[1]["sequence"], 1)
            e_updated_status = _find_card_element(
                transport.update_card_calls[1]["card_json"],
                EFFORT_STATUS_ELEMENT_ID,
            )
            assert e_updated_status is not None
            self.assertIn("Extra (`xhigh`)", e_updated_status["content"])

            # 3) 模拟 ACP 重启：同时恢复本会话选定的模型与思考强度
            acp._running = False
            acp._session_active_model = "gemini-4-argon"
            acp._session_active_effort = "medium"
            f_next = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_after_restart_1", text="继续对话")
            )
            assert f_next is not None
            f_next.result(timeout=5.0)
            self.assertEqual(
                acp.set_config_option_calls[2:],
                [
                    ("sess_fake_2", "model", "gemini-4-hydrogen"),
                    ("sess_fake_2", "thought_level", "xhigh"),
                ],
            )
            self.assertEqual(acp.prompt_calls[0]["session_id"], "sess_fake_2")

            # 4) 在模型本身无思考强度可调时，/effort 弹出说明卡片而非报错
            gpt_oss_val = m_buttons[4]["behaviors"][0]["value"]
            gpt_evt = _make_p2_card_action_event(
                operator_open_id="ou_allowed_1",
                action_value=gpt_oss_val,
                open_message_id=model_msg_id,
            )
            daemon.handle_card_action(gpt_evt)
            f_effort_unsup = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_effort_unsup", text="/effort")
            )
            assert f_effort_unsup is not None
            f_effort_unsup.result(timeout=5.0)
            unsup_card = transport.created_cards[-1]["card_json"]
            self.assertEqual(_extract_selector_buttons(unsup_card), [])
        finally:
            daemon.close()

    def test_slash_commands_with_arguments_pass_through_as_normal_prompts(self) -> None:
        acp = _FakeAcpClient()
        acp.use_config_options = True
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        daemon = AgyLarkDaemon(
            _make_config(),
            acp_client=acp,
            transport=transport,
            message_sender=sender,
        )
        try:
            for idx, text in enumerate(("/model gemini-3.8-flash", "/effort high"), start=1):
                f = daemon.dispatch_message_event(
                    _make_p2_message_event(message_id=f"om_arg_{idx}", text=text)
                )
                assert f is not None
                f.result(timeout=5.0)

            self.assertEqual(acp.set_config_option_calls, [])
            self.assertEqual(
                [call["prompt"] for call in acp.prompt_calls],
                ["/model gemini-3.8-flash", "/effort high"],
            )
        finally:
            daemon.close()

    def test_model_and_effort_callbacks_block_active_turn_and_surface_rpc_errors(self) -> None:
        acp = _FakeAcpClient()
        acp.use_config_options = True
        turn_started = threading.Event()
        release_turn: list[Future[AcpPromptResult]] = []

        def _hold_turn(
            session_id: str,
            prompt: Any,
            on_update: Any,
            message_id: str | None,
        ) -> Future[AcpPromptResult]:
            fut: Future[AcpPromptResult] = Future()
            release_turn.append(fut)
            turn_started.set()
            return fut

        acp.prompt_handler = _hold_turn
        transport = _FakeTransport()
        sender = _FakeMessageSender()
        daemon = AgyLarkDaemon(
            _make_config(),
            acp_client=acp,
            transport=transport,
            message_sender=sender,
        )
        try:
            f_turn = daemon.dispatch_message_event(
                _make_p2_message_event(message_id="om_active_1", text="长任务进行中")
            )
            assert f_turn is not None
            self.assertTrue(turn_started.wait(timeout=3.0))

            # 在途任务执行期间仍可弹出 /model 与 /effort 卡片
            for cmd_id, cmd_text in (("om_busy_m", "/model"), ("om_busy_e", "/effort")):
                fc = daemon.dispatch_message_event(
                    _make_p2_message_event(message_id=cmd_id, text=cmd_text)
                )
                assert fc is not None
                fc.result(timeout=5.0)
            model_card_msg_id = sender.card_messages[-2]["message_id"]
            effort_card_msg_id = sender.card_messages[-1]["message_id"]

            # 点击模型或思考强度按钮均被拦截并提示 info toast
            m_click = build_model_select_callback_value("gemini-3.8-flash", conv_key="p2p:oc_chat_1")
            e_click = build_effort_select_callback_value("high", conv_key="p2p:oc_chat_1")
            for msg_id, val in ((model_card_msg_id, m_click), (effort_card_msg_id, e_click)):
                evt = _make_p2_card_action_event(
                    operator_open_id="ou_allowed_1",
                    action_value=val,
                    open_message_id=msg_id,
                )
                resp = daemon.handle_card_action(evt)
                self.assertEqual(resp["toast"]["type"], "info")
                self.assertIn("当前有任务正在执行", resp["toast"]["content"])
            self.assertEqual(acp.set_config_option_calls, [])

            # 放行并结束在途回合后，模拟底层 set_config_option 抛错
            release_turn[0].set_result(
                AcpPromptResult(
                    session_id="sess_fake_1",
                    stop_reason="end_turn",
                    text="done",
                    thoughts="",
                    updates=[],
                )
            )
            f_turn.result(timeout=5.0)

            acp.set_config_option_error = RuntimeError("invalid effort level")
            err_evt = _make_p2_card_action_event(
                operator_open_id="ou_allowed_1",
                action_value=e_click,
                open_message_id=effort_card_msg_id,
            )
            err_resp = daemon.handle_card_action(err_evt)
            self.assertEqual(err_resp["toast"]["type"], "error")
            self.assertIn("invalid effort level", err_resp["toast"]["content"])
            effort_card_id = sender.card_messages[-1]["card_id"]
            self.assertFalse(
                any(call["card_id"] == effort_card_id for call in transport.update_card_calls)
            )
        finally:
            if release_turn and not release_turn[0].done():
                release_turn[0].cancel()
            daemon.close()


def tearDownModule() -> None:
    """清理第三方 lark_oapi.ws 模块级未关闭事件循环与后台定时任务，防止退出时产生警告。"""
    gc.collect()
    ws_client_mod = sys.modules.get("lark_oapi.ws.client")
    loop = getattr(ws_client_mod, "loop", None)
    if loop is not None and not loop.is_running() and not loop.is_closed():
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
    # 确保即使在模块测试期间调整过 root logger，也维持干净默认状态
    logging.getLogger().handlers.clear()


if __name__ == "__main__":
    unittest.main()
