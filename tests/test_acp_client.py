"""agylark.acp_client 离线单元测试（通过模拟 stdio 子进程验证协议与并发控制）。"""

from __future__ import annotations

import sys
import tempfile
import threading
import unittest

from agylark import (
    AcpError,
    AcpSessionUpdate,
    AgyAcpClient,
)

_MOCK_ACP_SERVER_SCRIPT = r"""
import json
import sys
import threading

if hasattr(sys.stdin, "reconfigure"):
    sys.stdin.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

write_lock = threading.Lock()
state_lock = threading.Lock()
active_sessions = {}
perm_waiters = {}


def send(payload):
    with write_lock:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def run_turn(session_id, req_id, prompt_text):
    state = active_sessions[session_id]
    if prompt_text.startswith("stream:"):
        for update in (
            {
                "sessionUpdate": "agent_thought_chunk",
                "content": {"type": "text", "text": "先分析上下文"},
            },
            {
                "sessionUpdate": "tool_call",
                "toolCallId": "tc-1",
                "name": "read_file",
                "status": "in_progress",
            },
            {
                "sessionUpdate": "tool_call_update",
                "toolCallId": "tc-1",
                "name": "read_file",
                "status": "completed",
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
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {"sessionId": session_id, "update": update},
            })
        send({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "stopReason": "end_turn",
                "usage": {"inputTokens": 12, "outputTokens": 8, "totalTokens": 20},
            },
        })
        return

    if prompt_text.startswith("slow_steer:"):
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "阶段1;"},
                },
            },
        })
        state["steer_event"].wait(timeout=5.0)
        if state["cancelled"]:
            send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "cancelled"}})
            return
        steer_text = state["steer_text"]
        steer_req_id = state["steer_req_id"]
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": f"已纠偏:{steer_text}"},
                },
            },
        })
        if steer_req_id is not None:
            send({"jsonrpc": "2.0", "id": steer_req_id, "result": {"stopReason": "end_turn"}})
        send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "end_turn"}})
        return

    if prompt_text.startswith("block_until_cancel:"):
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "等待取消"},
                },
            },
        })
        state["cancel_event"].wait(timeout=5.0)
        send({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"stopReason": "cancelled" if state["cancelled"] else "end_turn"},
        })
        return

    if prompt_text.startswith("need_perm:"):
        perm_id = "perm-1"
        ev = threading.Event()
        holder = {"event": ev, "result": None}
        with state_lock:
            perm_waiters[perm_id] = holder
        send({
            "jsonrpc": "2.0",
            "id": perm_id,
            "method": "session/request_permission",
            "params": {
                "sessionId": session_id,
                "toolCall": {"toolCallId": "tc-perm", "name": "run_command", "status": "pending"},
                "options": [
                    {"optionId": "allow_once", "name": "Allow once", "kind": "allow_once"},
                    {"optionId": "reject_once", "name": "Reject", "kind": "reject_once"},
                ],
            },
        })
        ev.wait(timeout=5.0)
        outcome = ((holder["result"] or {}).get("outcome") or {}).get("optionId", "missing")
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": f"perm={outcome}"},
                },
            },
        })
        send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "end_turn"}})
        return

    send({"jsonrpc": "2.0", "id": req_id, "result": {"stopReason": "end_turn"}})


for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    req_id = msg.get("id")
    params = msg.get("params") or {}

    if "id" in msg and method is None:
        with state_lock:
            holder = perm_waiters.pop(req_id, None)
        if holder is not None:
            holder["result"] = msg.get("result")
            holder["event"].set()
        continue

    if method == "initialize":
        send({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": params.get("protocolVersion"),
                "agentInfo": {"name": "Mock ACP Server", "version": "1.0.0"},
                "agentCapabilities": {"loadSession": True},
                "authMethods": [],
            },
        })
    elif method == "session/new":
        sid = "sess-mock-1"
        with state_lock:
            active_sessions[sid] = {
                "busy": False,
                "cancelled": False,
                "cancel_event": threading.Event(),
                "steer_event": threading.Event(),
                "steer_req_id": None,
                "steer_text": "",
            }
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": sid,
                "update": {"sessionUpdate": "available_commands_update", "availableCommands": []},
            },
        })
        send({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "sessionId": sid,
                "models": {"currentModelId": "mock-model-default", "availableModels": []},
                "modes": {"currentModeId": "default", "availableModes": []},
                "configOptions": [],
            },
        })
    elif method == "session/resume":
        sid = params.get("sessionId", "")
        with state_lock:
            exists = sid in active_sessions
        if not exists:
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32603,
                    "message": f"Failed to read session {sid}; retry, or load it with session/load",
                },
            })
        else:
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "sessionId": sid,
                    "models": {"currentModelId": "mock-model-default", "availableModels": []},
                    "modes": {"currentModeId": "default", "availableModes": []},
                    "configOptions": [],
                },
            })
    elif method == "session/set_mode":
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": params["sessionId"],
                "update": {"sessionUpdate": "current_mode_update", "currentModeId": params["modeId"]},
            },
        })
        send({"jsonrpc": "2.0", "id": req_id, "result": {}})
    elif method == "session/set_model":
        send({"jsonrpc": "2.0", "id": req_id, "result": {}})
    elif method == "session/set_config_option":
        send({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"configOptions": [{"id": params["configId"], "currentValue": params["value"]}]},
        })
    elif method == "session/close":
        active_sessions.pop(params.get("sessionId"), None)
        send({"jsonrpc": "2.0", "id": req_id, "result": {}})
    elif method == "session/prompt":
        sid = params.get("sessionId", "")
        if sid not in active_sessions:
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32602, "message": f"Session not found: {sid}"},
            })
            continue
        blocks = params.get("prompt") or []
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        state = active_sessions[sid]
        with state_lock:
            if state["busy"]:
                state["steer_req_id"] = req_id
                state["steer_text"] = text
                state["steer_event"].set()
                continue
            state["busy"] = True
            state["cancelled"] = False
            state["cancel_event"].clear()
            state["steer_event"].clear()
            state["steer_req_id"] = None
            state["steer_text"] = ""

        def _worker(s_id=sid, r_id=req_id, p_text=text, st=state):
            try:
                run_turn(s_id, r_id, p_text)
            finally:
                with state_lock:
                    st["busy"] = False

        threading.Thread(target=_worker, daemon=True).start()
    elif method == "session/cancel":
        # 严格校验取消协议为无 id 单向通知
        assert "id" not in msg
        sid = params.get("sessionId", "")
        state = active_sessions.get(sid)
        if state is not None:
            state["cancelled"] = True
            state["cancel_event"].set()
            state["steer_event"].set()
    else:
        if req_id is not None:
            send({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            })
"""


class TestAgyAcpClient(unittest.TestCase):
    """通过模拟 stdio 子进程验证 AgyAcpClient 完整协议与控制逻辑。"""

    def _build_client(self, *, on_update=None, on_permission_request=None) -> AgyAcpClient:
        return AgyAcpClient(
            executable=sys.executable,
            args=["-u", "-c", _MOCK_ACP_SERVER_SCRIPT],
            cwd=tempfile.gettempdir(),
            default_timeout=5.0,
            on_update=on_update,
            on_permission_request=on_permission_request,
        )

    def test_handshake_new_session_and_auxiliary_methods(self) -> None:
        global_updates: list[AcpSessionUpdate] = []
        with self._build_client(on_update=global_updates.append) as client:
            self.assertTrue(client.is_running)
            self.assertIsNotNone(client.initialize_result)
            self.assertEqual(client.initialize_result["protocolVersion"], 1)
            self.assertEqual(client.initialize_result["agentInfo"]["name"], "Mock ACP Server")

            session = client.new_session()
            sid = session["sessionId"]
            self.assertEqual(sid, "sess-mock-1")

            self.assertEqual(client.set_mode(sid, "plan"), {})
            self.assertEqual(client.set_model(sid, "mock-model-alt"), {})
            cfg_res = client.set_config_option(sid, "thought_level", "high")
            self.assertEqual(cfg_res["configOptions"][0]["currentValue"], "high")
            self.assertEqual(client.close_session(sid), {})

        self.assertFalse(client.is_running)
        update_types = {item.update_type for item in global_updates}
        self.assertIn("available_commands_update", update_types)
        self.assertIn("current_mode_update", update_types)

    def test_prompt_streaming_chunks_and_tool_calls(self) -> None:
        seen_updates: list[AcpSessionUpdate] = []
        with self._build_client() as client:
            sid = client.new_session()["sessionId"]
            result = client.prompt(sid, "stream:请分析", on_update=seen_updates.append)

        self.assertEqual(result.stop_reason, "end_turn")
        self.assertFalse(result.is_cancelled)
        self.assertEqual(result.thoughts, "先分析上下文")
        self.assertEqual(result.text, "第一段，第二段。")
        self.assertEqual(result.usage, {"inputTokens": 12, "outputTokens": 8, "totalTokens": 20})
        self.assertEqual(
            [item.update_type for item in seen_updates],
            [
                "agent_thought_chunk",
                "tool_call",
                "tool_call_update",
                "agent_message_chunk",
                "agent_message_chunk",
            ],
        )
        self.assertEqual(seen_updates[1].tool_call_id, "tc-1")
        self.assertEqual(seen_updates[1].tool_name, "read_file")
        self.assertEqual(seen_updates[2].tool_status, "completed")

    def test_mid_turn_steering(self) -> None:
        chunk_seen = threading.Event()
        streamed_chunks: list[str] = []

        def on_turn_update(update: AcpSessionUpdate) -> None:
            if update.update_type == "agent_message_chunk":
                streamed_chunks.append(update.text)
                if "阶段1;" in update.text:
                    chunk_seen.set()

        with self._build_client() as client:
            sid = client.new_session()["sessionId"]
            first_future = client.prompt_async(sid, "slow_steer:长任务", on_update=on_turn_update)
            self.assertTrue(chunk_seen.wait(timeout=3.0), "首个在途流式块未按预期到达")

            steer_result = client.steer(sid, "改写为中文")
            first_result = first_future.result(timeout=5.0)

        self.assertEqual(steer_result.stop_reason, "end_turn")
        self.assertEqual(first_result.stop_reason, "end_turn")
        self.assertEqual("".join(streamed_chunks), "阶段1;已纠偏:改写为中文")
        self.assertEqual(first_result.text, "阶段1;已纠偏:改写为中文")

    def test_session_cancel_notification_aborts_active_turn(self) -> None:
        started = threading.Event()

        def on_turn_update(update: AcpSessionUpdate) -> None:
            if update.update_type == "agent_message_chunk" and update.text == "等待取消":
                started.set()

        with self._build_client() as client:
            sid = client.new_session()["sessionId"]
            future = client.prompt_async(sid, "block_until_cancel:测试中止", on_update=on_turn_update)
            self.assertTrue(started.wait(timeout=3.0), "在途回合未进入等待取消阶段")

            client.cancel(sid)
            result = future.result(timeout=5.0)

        self.assertEqual(result.stop_reason, "cancelled")
        self.assertTrue(result.is_cancelled)
        self.assertEqual(result.text, "等待取消")

    def test_permission_request_auto_response_and_rpc_error(self) -> None:
        captured_perm: list[dict] = []

        def handle_perm(params: dict) -> str:
            captured_perm.append(params)
            return "allow_once"

        with self._build_client(on_permission_request=handle_perm) as client:
            sid = client.new_session()["sessionId"]
            perm_res = client.prompt(sid, "need_perm:执行命令")
            self.assertEqual(perm_res.text, "perm=allow_once")
            self.assertEqual(len(captured_perm), 1)

            with self.assertRaises(AcpError) as ctx:
                client.prompt("missing", "hello")
            self.assertEqual(ctx.exception.code, -32602)
            self.assertIn("Session not found", str(ctx.exception))

    def test_resume_session_success_error_and_validation(self) -> None:
        updates_during_resume: list[AcpSessionUpdate] = []
        with self._build_client(on_update=updates_during_resume.append) as client:
            created = client.new_session()
            sid = created["sessionId"]
            updates_during_resume.clear()

            resumed = client.resume_session(sid)
            self.assertEqual(resumed["sessionId"], sid)
            self.assertEqual(resumed["models"]["currentModelId"], "mock-model-default")
            # session/resume 不应触发历史 session/update 回放通知
            self.assertEqual(updates_during_resume, [])

            with self.assertRaises(AcpError) as ctx:
                client.resume_session("missing-session-id")
            self.assertEqual(ctx.exception.code, -32603)
            self.assertIn("Failed to read session", str(ctx.exception))

            with self.assertRaises(ValueError):
                client.resume_session("   ")


if __name__ == "__main__":
    unittest.main()
