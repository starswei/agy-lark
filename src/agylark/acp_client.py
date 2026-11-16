"""原生 agy acp --stdio 双向 JSON-RPC 2.0 客户端与流式会话控制。"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
import json
import logging
from pathlib import Path
import subprocess
import threading
from typing import Any

from .config import DEFAULT_TURN_TIMEOUT_SECONDS, resolve_agy_executable

logger = logging.getLogger(__name__)

ACP_PROTOCOL_VERSION = 1
_TEXT_CHUNK_UPDATE_TYPES = frozenset(
    {"agent_message_chunk", "agent_thought_chunk", "user_message_chunk"}
)


class AcpError(RuntimeError):
    """ACP 协议或 JSON-RPC 通信异常。"""

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        data: Any = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.data = data

    def __str__(self) -> str:
        if self.code is None:
            return self.message
        return f"[{self.code}] {self.message}"


@dataclass(frozen=True)
class AcpSessionUpdate:
    """封装服务端下发的单条 session/update 流式通知。"""

    session_id: str
    update_type: str
    raw: dict[str, Any]

    @property
    def text(self) -> str:
        """提取文本增量块的内容；非文本块返回空字符串。"""
        if self.update_type not in _TEXT_CHUNK_UPDATE_TYPES:
            return ""
        content = self.raw.get("content")
        if not isinstance(content, dict):
            return ""
        if content.get("type") not in (None, "text"):
            return ""
        text = content.get("text")
        return text if isinstance(text, str) else ""

    @property
    def tool_call_id(self) -> str | None:
        """返回工具调用唯一标识。"""
        value = self.raw.get("toolCallId")
        return value if isinstance(value, str) else None

    @property
    def tool_name(self) -> str | None:
        """返回工具名称。"""
        value = self.raw.get("name")
        return value if isinstance(value, str) else None

    @property
    def tool_status(self) -> str | None:
        """返回工具执行状态。"""
        value = self.raw.get("status")
        return value if isinstance(value, str) else None


@dataclass(frozen=True)
class AcpPromptResult:
    """单次 session/prompt 或运行中插话请求结束时的聚合结果。"""

    session_id: str
    stop_reason: str
    text: str
    thoughts: str
    updates: list[AcpSessionUpdate]
    usage: dict[str, Any] | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_cancelled(self) -> bool:
        """判断当前回合是否因 session/cancel 中止。"""
        return self.stop_reason == "cancelled"


@dataclass
class _TurnAccumulator:
    """跟踪同一会话内并发（含运行中插话）回合的流式增量与在途请求集合。"""

    session_id: str
    updates: list[AcpSessionUpdate] = field(default_factory=list)
    text_parts: list[str] = field(default_factory=list)
    thought_parts: list[str] = field(default_factory=list)
    callbacks: list[Callable[[AcpSessionUpdate], None]] = field(default_factory=list)
    inflight_ids: set[int] = field(default_factory=set)

    def add_callback(self, callback: Callable[[AcpSessionUpdate], None] | None) -> None:
        if callback is not None and callback not in self.callbacks:
            self.callbacks.append(callback)

    def record(self, update: AcpSessionUpdate) -> tuple[Callable[[AcpSessionUpdate], None], ...]:
        self.updates.append(update)
        chunk_text = update.text
        if chunk_text:
            if update.update_type == "agent_message_chunk":
                self.text_parts.append(chunk_text)
            elif update.update_type == "agent_thought_chunk":
                self.thought_parts.append(chunk_text)
        return tuple(self.callbacks)

    def snapshot_result(self, payload: dict[str, Any]) -> AcpPromptResult:
        usage = payload.get("usage")
        return AcpPromptResult(
            session_id=self.session_id,
            stop_reason=str(payload.get("stopReason") or ""),
            text="".join(self.text_parts),
            thoughts="".join(self.thought_parts),
            updates=list(self.updates),
            usage=usage if isinstance(usage, dict) else None,
            raw=payload,
        )


def _normalize_prompt_blocks(prompt: str | Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """将纯文本或内容块序列统一转换为 ACP prompt 数组格式。"""
    if isinstance(prompt, str):
        return [{"type": "text", "text": prompt}]
    blocks = [dict(item) for item in prompt]
    if not blocks:
        raise ValueError("prompt 内容块不能为空")
    return blocks


def _select_permission_option_id(
    params: dict[str, Any],
    preferred_option_id: str,
) -> str:
    """从权限确认选项中选择预设的 optionId，找不到时回退到首项或默认值。"""
    options = params.get("options")
    if isinstance(options, list):
        option_ids = [
            item.get("optionId")
            for item in options
            if isinstance(item, dict) and isinstance(item.get("optionId"), str)
        ]
        if preferred_option_id in option_ids:
            return preferred_option_id
        if option_ids:
            return option_ids[0]
    return preferred_option_id


class AgyAcpClient:
    """维护长驻 `agy acp --stdio` 子进程并提供异步 JSON-RPC 2.0 会话控制。"""

    def __init__(
        self,
        *,
        executable: str | None = None,
        args: Sequence[str] | None = None,
        cwd: str | Path | None = None,
        skip_permissions: bool = True,
        permission_option_id: str = "allow_once",
        on_update: Callable[[AcpSessionUpdate], None] | None = None,
        on_permission_request: Callable[[dict[str, Any]], str] | None = None,
        default_timeout: float = float(DEFAULT_TURN_TIMEOUT_SECONDS),
        client_name: str = "agy-lark",
        client_version: str = "0.1.0",
    ) -> None:
        self._explicit_executable = executable
        self.executable: str | None = executable
        if args is None:
            default_args = ["acp", "--stdio", "--disable-telemetry"]
            if skip_permissions:
                default_args.append("--dangerously-skip-permissions")
            self.args = default_args
        else:
            self.args = list(args)

        self.cwd = Path.cwd() if cwd is None else Path(cwd)
        self.skip_permissions = skip_permissions
        self.permission_option_id = permission_option_id
        self.on_update = on_update
        self.on_permission_request = on_permission_request
        self.default_timeout = default_timeout
        self.client_name = client_name
        self.client_version = client_version

        self._lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._next_id = 1
        self._pending_requests: dict[int, Future[dict[str, Any]]] = {}
        self._active_turns: dict[str, _TurnAccumulator] = {}
        self._stderr_lines: deque[str] = deque(maxlen=200)
        self._closing = False
        self.initialize_result: dict[str, Any] | None = None

    @property
    def is_running(self) -> bool:
        """返回底层 ACP 子进程是否仍在存活运行。"""
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    @property
    def recent_stderr(self) -> list[str]:
        """返回近期捕获的子进程标准错误输出（用于排障）。"""
        with self._lock:
            return list(self._stderr_lines)

    def __enter__(self) -> AgyAcpClient:
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def start(self, *, timeout: float | None = None) -> dict[str, Any]:
        """启动 `agy acp --stdio` 长驻子进程并完成 initialize 协议握手。"""
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                raise AcpError("ACP 子进程已在运行中")

            exe = (
                self._explicit_executable
                if self._explicit_executable is not None
                else resolve_agy_executable()
            )
            self.executable = exe
            self._closing = False
            self._next_id = 1
            self._pending_requests.clear()
            self._active_turns.clear()
            self._stderr_lines.clear()

            proc = subprocess.Popen(
                [exe, *self.args],
                cwd=str(self.cwd),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            self._proc = proc
            self._stdout_thread = threading.Thread(
                target=self._stdout_reader_loop,
                args=(proc,),
                name="agy-acp-stdout",
                daemon=True,
            )
            self._stderr_thread = threading.Thread(
                target=self._stderr_reader_loop,
                args=(proc,),
                name="agy-acp-stderr",
                daemon=True,
            )
            self._stdout_thread.start()
            self._stderr_thread.start()

        try:
            result = self.send_request(
                "initialize",
                {
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": {
                        "name": self.client_name,
                        "version": self.client_version,
                    },
                    "clientCapabilities": {},
                },
                timeout=timeout,
            )
        except Exception:
            self.close()
            raise

        self.initialize_result = result
        return result

    def close(self) -> None:
        """优雅中止在途会话回合、关闭标准输入输出管道并回收子进程资源。"""
        with self._lock:
            self._closing = True
            proc = self._proc
            stdout_thread = self._stdout_thread
            stderr_thread = self._stderr_thread
            active_session_ids = list(self._active_turns.keys())

        if proc is not None and proc.poll() is None:
            for sid in active_session_ids:
                try:
                    self.cancel(sid)
                except Exception:
                    break

            if proc.stdin is not None:
                try:
                    with self._write_lock:
                        proc.stdin.close()
                except (OSError, ValueError):
                    pass

            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2.0)

        current = threading.current_thread()
        for thread in (stdout_thread, stderr_thread):
            if thread is not None and thread is not current:
                thread.join(timeout=1.0)

        if proc is not None:
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                if pipe is not None:
                    try:
                        pipe.close()
                    except (OSError, ValueError):
                        pass

        self._fail_all_pending(AcpError("ACP 客户端已关闭"))
        with self._lock:
            self._proc = None
            self._stdout_thread = None
            self._stderr_thread = None

    def send_request_async(
        self,
        method: str,
        params: dict[str, Any] | None = None,
    ) -> Future[dict[str, Any]]:
        """发送 JSON-RPC 2.0 请求并立即返回承载结果的 Future。"""
        req_id, future = self._register_request()
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
        }
        if params is not None:
            payload["params"] = params

        try:
            self._write_json_line(payload)
        except Exception:
            with self._lock:
                self._pending_requests.pop(req_id, None)
            raise
        return future

    def send_request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """同步发送 JSON-RPC 2.0 请求并在指定超时内等待结果。"""
        future = self.send_request_async(method, params)
        wait_timeout = self.default_timeout if timeout is None else timeout
        return future.result(timeout=wait_timeout)

    def send_notification(
        self,
        method: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        """发送单向 JSON-RPC 2.0 通知（不带 id，不等待回包）。"""
        payload: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
        }
        if params is not None:
            payload["params"] = params
        self._write_json_line(payload)

    def new_session(
        self,
        cwd: str | Path | None = None,
        mcp_servers: Sequence[dict[str, Any]] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """创建新的 ACP 会话并返回 sessionId 及可用模型、模式元数据。"""
        target_cwd = self.cwd if cwd is None else Path(cwd)
        return self.send_request(
            "session/new",
            {
                "cwd": str(target_cwd),
                "mcpServers": [dict(server) for server in (mcp_servers or ())],
            },
            timeout=timeout,
        )

    def resume_session(
        self,
        session_id: str,
        cwd: str | Path | None = None,
        mcp_servers: Sequence[dict[str, Any]] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """恢复已有 ACP 会话（`session/resume`），返回元数据而不回放历史消息流。"""
        normalized_session_id = session_id.strip() if isinstance(session_id, str) else ""
        if not normalized_session_id:
            raise ValueError("session_id 不能为空")
        target_cwd = self.cwd if cwd is None else Path(cwd)
        return self.send_request(
            "session/resume",
            {
                "sessionId": normalized_session_id,
                "cwd": str(target_cwd),
                "mcpServers": [dict(server) for server in (mcp_servers or ())],
            },
            timeout=timeout,
        )

    def set_mode(
        self,
        session_id: str,
        mode_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """切换指定会话的工作模式（如 default、accept-edits、plan）。"""
        return self.send_request(
            "session/set_mode",
            {"sessionId": session_id, "modeId": mode_id},
            timeout=timeout,
        )

    def set_model(
        self,
        session_id: str,
        model_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """切换指定会话使用的模型标识。"""
        return self.send_request(
            "session/set_model",
            {"sessionId": session_id, "modelId": model_id},
            timeout=timeout,
        )

    def set_config_option(
        self,
        session_id: str,
        config_id: str,
        value: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """更新会话配置项（如 model、thought_level、mode）。"""
        return self.send_request(
            "session/set_config_option",
            {"sessionId": session_id, "configId": config_id, "value": value},
            timeout=timeout,
        )

    def close_session(
        self,
        session_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """关闭指定会话并释放服务端资源。"""
        return self.send_request(
            "session/close",
            {"sessionId": session_id},
            timeout=timeout,
        )

    def prompt_async(
        self,
        session_id: str,
        prompt: str | Sequence[dict[str, Any]],
        *,
        on_update: Callable[[AcpSessionUpdate], None] | None = None,
        message_id: str | None = None,
    ) -> Future[AcpPromptResult]:
        """发起异步 session/prompt 请求；若同会话已有在途回合，则自动作为运行中插话接入流式收集器。"""
        blocks = _normalize_prompt_blocks(prompt)
        req_id, rpc_future = self._register_request()
        result_future: Future[AcpPromptResult] = Future()

        with self._lock:
            acc = self._active_turns.get(session_id)
            if acc is None:
                acc = _TurnAccumulator(session_id=session_id)
                self._active_turns[session_id] = acc
            acc.inflight_ids.add(req_id)
            acc.add_callback(on_update)

        def _on_rpc_done(done_future: Future[dict[str, Any]]) -> None:
            with self._lock:
                current_acc = self._active_turns.get(session_id)
                if current_acc is acc:
                    acc.inflight_ids.discard(req_id)
                    if not acc.inflight_ids:
                        self._active_turns.pop(session_id, None)

            exc = done_future.exception()
            if exc is not None:
                if not result_future.done():
                    result_future.set_exception(exc)
                return

            payload = done_future.result()
            if not result_future.done():
                result_future.set_result(acc.snapshot_result(payload))

        rpc_future.add_done_callback(_on_rpc_done)

        params: dict[str, Any] = {"sessionId": session_id, "prompt": blocks}
        if message_id is not None:
            params["messageId"] = message_id

        try:
            self._write_json_line(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "session/prompt",
                    "params": params,
                }
            )
        except Exception:
            with self._lock:
                self._pending_requests.pop(req_id, None)
                current_acc = self._active_turns.get(session_id)
                if current_acc is acc:
                    acc.inflight_ids.discard(req_id)
                    if not acc.inflight_ids:
                        self._active_turns.pop(session_id, None)
            raise

        return result_future

    def prompt(
        self,
        session_id: str,
        prompt: str | Sequence[dict[str, Any]],
        *,
        on_update: Callable[[AcpSessionUpdate], None] | None = None,
        message_id: str | None = None,
        timeout: float | None = None,
    ) -> AcpPromptResult:
        """同步执行 session/prompt 并等待结束；若超时则自动下发 session/cancel 止损。"""
        future = self.prompt_async(
            session_id,
            prompt,
            on_update=on_update,
            message_id=message_id,
        )
        wait_timeout = self.default_timeout if timeout is None else timeout
        try:
            return future.result(timeout=wait_timeout)
        except TimeoutError:
            try:
                self.cancel(session_id)
            except Exception:
                pass
            raise

    def steer(
        self,
        session_id: str,
        prompt: str | Sequence[dict[str, Any]],
        *,
        on_update: Callable[[AcpSessionUpdate], None] | None = None,
        message_id: str | None = None,
        timeout: float | None = None,
    ) -> AcpPromptResult:
        """在回合运行中向同一会话发送补充指示（Steering），不断开当前上下文。"""
        return self.prompt(
            session_id,
            prompt,
            on_update=on_update,
            message_id=message_id,
            timeout=timeout,
        )

    def cancel(self, session_id: str) -> None:
        """向指定会话下发协议级 session/cancel 单向通知。"""
        self.send_notification("session/cancel", {"sessionId": session_id})

    def _register_request(self) -> tuple[int, Future[dict[str, Any]]]:
        with self._lock:
            if self._closing or self._proc is None or self._proc.poll() is not None:
                raise AcpError("ACP 子进程未运行或正在关闭")
            req_id = self._next_id
            self._next_id += 1
            future: Future[dict[str, Any]] = Future()
            self._pending_requests[req_id] = future
            return req_id, future

    def _write_json_line(self, payload: dict[str, Any]) -> None:
        serialized = json.dumps(payload, ensure_ascii=False)
        with self._lock:
            proc = self._proc
        if proc is None or proc.stdin is None or proc.poll() is not None:
            raise AcpError("ACP 子进程标准输入不可用")

        with self._write_lock:
            try:
                proc.stdin.write(serialized + "\n")
                proc.stdin.flush()
            except OSError as exc:
                raise AcpError(f"写入 ACP 标准输入失败: {exc}") from exc

    def _stdout_reader_loop(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        try:
            for raw_line in iter(proc.stdout.readline, ""):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    logger.debug("忽略非 JSON 标准输出行: %s", line[:200])
                    continue
                if isinstance(msg, dict):
                    self._handle_incoming_message(msg)
        except (OSError, ValueError):
            pass
        finally:
            reason = self._build_eof_error_message(proc)
            self._fail_all_pending(AcpError(reason))

    def _stderr_reader_loop(self, proc: subprocess.Popen[str]) -> None:
        assert proc.stderr is not None
        try:
            for raw_line in iter(proc.stderr.readline, ""):
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                with self._lock:
                    self._stderr_lines.append(line)
                logger.debug("[agy acp stderr] %s", line)
        except (OSError, ValueError):
            pass

    def _build_eof_error_message(self, proc: subprocess.Popen[str]) -> str:
        code = proc.poll()
        with self._lock:
            tail = list(self._stderr_lines)[-3:]
        if code is None:
            return "ACP 子进程标准输出已关闭"
        if tail:
            return f"ACP 子进程已退出 (exit={code}): {' | '.join(tail)}"
        return f"ACP 子进程已退出 (exit={code})"

    def _handle_incoming_message(self, msg: dict[str, Any]) -> None:
        method = msg.get("method")
        if "id" in msg and method is None:
            self._resolve_pending_response(msg)
            return

        if method == "session/update":
            self._dispatch_session_update(msg.get("params"))
            return

        if method == "session/request_permission" and "id" in msg:
            self._handle_permission_request(msg["id"], msg.get("params"))

    def _resolve_pending_response(self, msg: dict[str, Any]) -> None:
        req_id = msg.get("id")
        if not isinstance(req_id, int):
            return
        with self._lock:
            future = self._pending_requests.pop(req_id, None)
        if future is None or future.done():
            return

        error = msg.get("error")
        if isinstance(error, dict):
            future.set_exception(
                AcpError(
                    str(error.get("message") or "ACP RPC 错误"),
                    code=error.get("code") if isinstance(error.get("code"), int) else None,
                    data=error.get("data"),
                )
            )
            return

        result = msg.get("result")
        future.set_result(result if isinstance(result, dict) else {"value": result})

    def _dispatch_session_update(self, raw_params: Any) -> None:
        if not isinstance(raw_params, dict):
            return
        session_id = raw_params.get("sessionId")
        raw_update = raw_params.get("update")
        if not isinstance(session_id, str) or not isinstance(raw_update, dict):
            return

        update_type = str(raw_update.get("sessionUpdate") or "")
        update = AcpSessionUpdate(
            session_id=session_id,
            update_type=update_type,
            raw=raw_update,
        )

        callbacks: tuple[Callable[[AcpSessionUpdate], None], ...] = ()
        with self._lock:
            acc = self._active_turns.get(session_id)
            if acc is not None:
                callbacks = acc.record(update)

        for callback in callbacks:
            try:
                callback(update)
            except Exception:
                logger.exception("session/update 回调执行失败: %s", update_type)

        if self.on_update is not None:
            try:
                self.on_update(update)
            except Exception:
                logger.exception("全局 on_update 回调执行失败: %s", update_type)

    def _handle_permission_request(self, req_id: Any, raw_params: Any) -> None:
        params = raw_params if isinstance(raw_params, dict) else {}
        if self.on_permission_request is None:
            option_id = _select_permission_option_id(params, self.permission_option_id)
            self._send_permission_outcome(req_id, option_id)
            return

        # 在独立线程中执行自定义权限回调，避免阻塞 stdout 读循环接收后续取消通知
        threading.Thread(
            target=self._run_custom_permission_handler,
            args=(req_id, params),
            name="agy-acp-perm",
            daemon=True,
        ).start()

    def _run_custom_permission_handler(
        self,
        req_id: Any,
        params: dict[str, Any],
    ) -> None:
        assert self.on_permission_request is not None
        try:
            option_id = self.on_permission_request(params)
        except Exception:
            logger.exception("自定义权限确认回调异常，回退为拒绝本次操作")
            option_id = _select_permission_option_id(params, "reject_once")
        self._send_permission_outcome(req_id, option_id)

    def _send_permission_outcome(self, req_id: Any, option_id: str) -> None:
        try:
            self._write_json_line(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {"outcome": {"optionId": option_id}},
                }
            )
        except AcpError:
            logger.debug("子进程已退出，忽略权限确认回包 (id=%s)", req_id)

    def _fail_all_pending(self, error: AcpError) -> None:
        with self._lock:
            pending = list(self._pending_requests.values())
            self._pending_requests.clear()
        for future in pending:
            if not future.done():
                future.set_exception(error)
