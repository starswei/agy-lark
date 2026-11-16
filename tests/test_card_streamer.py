"""agylark.card_streamer 离线单元测试（兼容 unittest 与 pytest）。"""

from __future__ import annotations

from collections.abc import Callable
import json
import re
import threading
from typing import Any
import unittest

from agylark import (
    CARD_SCHEMA_VERSION,
    CONTENT_ELEMENT_ID,
    EFFORT_HINT_ELEMENT_ID,
    EFFORT_SELECT_ACTION_NAME,
    EFFORT_STATUS_ELEMENT_ID,
    MODEL_HINT_ELEMENT_ID,
    MODEL_SELECT_ACTION_NAME,
    MODEL_STATUS_ELEMENT_ID,
    STATUS_ELEMENT_ID,
    STOP_ACTION_NAME,
    STOP_BUTTON_ELEMENT_ID,
    AcpEffortOption,
    AcpModelOption,
    AcpPromptResult,
    AcpSessionUpdate,
    CardRunState,
    CardTransport,
    EffortSelectCallbackPayload,
    ModelSelectCallbackPayload,
    StopCallbackPayload,
    StreamingCardUpdater,
    build_create_card_payload,
    build_effort_select_callback_value,
    build_effort_selector_card_json,
    build_model_select_callback_value,
    build_model_selector_card_json,
    build_stop_callback_value,
    build_stream_content_payload,
    build_streaming_card_json,
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
from agylark.card_streamer import DEFAULT_MAX_CONTENT_CHARS

_VALID_ELEMENT_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,19}$")


class _RecordingTransport:
    """记录 CardKit v1 传输调用的纯内存桩对象。"""

    def __init__(self, card_id: str = "card-mock-1") -> None:
        self.card_id_to_return = card_id
        self.created_cards: list[dict[str, Any]] = []
        self.stream_calls: list[dict[str, Any]] = []
        self.update_settings_calls: list[dict[str, Any]] = []
        self.update_card_calls: list[dict[str, Any]] = []
        self.events: list[tuple[str, int | None]] = []
        self.fail_on_element_id: str | None = None
        self.remaining_failures: int = 0
        self._lock = threading.Lock()

    def create_card(self, card_json: dict[str, Any]) -> str:
        with self._lock:
            self.created_cards.append(card_json)
            self.events.append(("create_card", None))
            return self.card_id_to_return

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
            if (
                self.remaining_failures > 0
                and (self.fail_on_element_id is None or self.fail_on_element_id == element_id)
            ):
                self.remaining_failures -= 1
                raise RuntimeError("mock transient stream error")
            self.stream_calls.append(
                {
                    "card_id": card_id,
                    "element_id": element_id,
                    "content": content,
                    "sequence": sequence,
                    "uuid": uuid,
                }
            )
            self.events.append((f"stream:{element_id}", sequence))

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
            self.events.append(("update_card", sequence))

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
            self.events.append(("update_settings", sequence))

    def all_sequences(self) -> list[int]:
        """返回按记录顺序收集到的全部正整数 sequence。"""
        with self._lock:
            return [seq for _, seq in self.events if seq is not None]


class _FakeClock:
    """可控虚拟单调时钟。"""

    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


class _FakeTimer:
    """可检查触发时机与取消状态的虚拟定时器。"""

    def __init__(self, delay: float, callback: Callable[[], None]) -> None:
        self.delay = delay
        self.callback = callback
        self.daemon = False
        self.started = False
        self.cancelled = False
        self.fired = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        if self.started and not self.cancelled and not self.fired:
            self.fired = True
            self.callback()


class _FakeTimerScheduler:
    """收集并驱动所有虚拟定时器的测试调度器。"""

    def __init__(self) -> None:
        self.timers: list[_FakeTimer] = []

    def __call__(self, delay: float, callback: Callable[[], None]) -> _FakeTimer:
        timer = _FakeTimer(delay, callback)
        self.timers.append(timer)
        return timer


class _SeqUuidFactory:
    """生成可预测顺序 UUID 的测试工厂。"""

    def __init__(self) -> None:
        self._counter = 0

    def __call__(self) -> str:
        self._counter += 1
        return f"uuid-mock-{self._counter}"


def _extract_elements(card_json: dict[str, Any]) -> list[dict[str, Any]]:
    body = card_json["body"]
    elements = body["elements"]
    assert isinstance(elements, list)
    return elements


def _find_element(card_json: dict[str, Any], element_id: str) -> dict[str, Any] | None:
    for elem in _extract_elements(card_json):
        if elem.get("element_id") == element_id:
            return elem
    return None


class TestCardSchemaAndStopPayload(unittest.TestCase):
    """验证 CardKit 2.0 卡片结构规范与停止按钮回调载荷编解码。"""

    def test_stop_callback_value_round_trip_and_validation(self) -> None:
        value = build_stop_callback_value(" sess-mock-1 ", turn_id=" turn-mock-1 ")
        self.assertEqual(
            value,
            {
                "action": STOP_ACTION_NAME,
                "session_id": "sess-mock-1",
                "turn_id": "turn-mock-1",
            },
        )
        parsed = parse_stop_callback_value(value)
        self.assertEqual(parsed, StopCallbackPayload(session_id="sess-mock-1", turn_id="turn-mock-1"))

        without_turn = build_stop_callback_value("sess-mock-2", turn_id="   ")
        self.assertEqual(without_turn, {"action": STOP_ACTION_NAME, "session_id": "sess-mock-2"})
        self.assertEqual(
            parse_stop_callback_value(without_turn),
            StopCallbackPayload(session_id="sess-mock-2", turn_id=None),
        )

        with self.assertRaises(ValueError):
            build_stop_callback_value("   ")
        with self.assertRaises(ValueError):
            build_stop_callback_value("sess-mock-1", action=" ")

    def test_parse_stop_callback_value_rejects_invalid_inputs(self) -> None:
        self.assertIsNone(parse_stop_callback_value(None))
        self.assertIsNone(parse_stop_callback_value("stop"))
        self.assertIsNone(parse_stop_callback_value([]))
        self.assertIsNone(parse_stop_callback_value({"action": "other", "session_id": "sess-mock-1"}))
        self.assertIsNone(parse_stop_callback_value({"action": STOP_ACTION_NAME}))
        self.assertIsNone(parse_stop_callback_value({"action": STOP_ACTION_NAME, "session_id": "  "}))
        self.assertIsNone(parse_stop_callback_value({"action": STOP_ACTION_NAME, "session_id": 123}))

    def test_build_streaming_card_json_matches_feishu_v2_schema(self) -> None:
        card = build_streaming_card_json(
            session_id="sess-mock-1",
            status_text="✍️ 正在生成回复...",
            content_text="Hello world",
            state=CardRunState.RUNNING,
            title="Demo Title",
            subtitle="Sub",
            turn_id="turn-mock-1",
        )
        self.assertEqual(card["schema"], CARD_SCHEMA_VERSION)
        self.assertNotIn("elements", card)
        self.assertTrue(card["config"]["streaming_mode"])
        self.assertTrue(card["config"]["update_multi"])
        self.assertEqual(card["header"]["template"], "blue")
        self.assertEqual(card["header"]["title"]["content"], "Demo Title")
        self.assertEqual(card["header"]["subtitle"]["content"], "Sub")

        elements = _extract_elements(card)
        tags = [elem["tag"] for elem in elements]
        self.assertEqual(tags, ["markdown", "hr", "markdown", "button"])
        self.assertNotIn("action", tags)

        for elem in elements:
            elem_id = elem.get("element_id")
            if elem_id is not None:
                self.assertRegex(elem_id, _VALID_ELEMENT_ID_RE)

        stop_btn = _find_element(card, STOP_BUTTON_ELEMENT_ID)
        assert stop_btn is not None
        self.assertEqual(stop_btn["tag"], "button")
        self.assertEqual(stop_btn["type"], "danger")
        behavior = stop_btn["behaviors"][0]
        self.assertEqual(behavior["type"], "callback")
        self.assertEqual(
            parse_stop_callback_value(behavior["value"]),
            StopCallbackPayload(session_id="sess-mock-1", turn_id="turn-mock-1"),
        )

    def test_terminal_states_colors_and_stop_button_visibility(self) -> None:
        expected_templates = {
            CardRunState.RUNNING: ("blue", True),
            CardRunState.COMPLETED: ("green", False),
            CardRunState.CANCELLED: ("orange", False),
            CardRunState.ERROR: ("red", False),
        }
        for state, (template, streaming_mode) in expected_templates.items():
            card = build_streaming_card_json(
                session_id="sess-mock-1",
                status_text="status",
                content_text="body",
                state=state,
            )
            self.assertEqual(card["header"]["template"], template)
            self.assertEqual(card["config"]["streaming_mode"], streaming_mode)
            has_stop = _find_element(card, STOP_BUTTON_ELEMENT_ID) is not None
            self.assertEqual(has_stop, streaming_mode)

        forced_no_stop = build_streaming_card_json(
            session_id="sess-mock-1",
            status_text="status",
            content_text="body",
            state=CardRunState.RUNNING,
            show_stop_button=False,
        )
        self.assertIsNone(_find_element(forced_no_stop, STOP_BUTTON_ELEMENT_ID))


class TestCardKitPayloadBuilders(unittest.TestCase):
    """验证 CardKit v1 REST 接口载荷构造函数。"""

    def test_cardkit_v1_payload_builders(self) -> None:
        card_json = build_streaming_card_json(
            session_id="sess-mock-1",
            status_text="⏳ 正在思考...",
            content_text="Init",
        )
        create_payload = build_create_card_payload(card_json)
        self.assertEqual(create_payload["type"], "card_json")
        self.assertEqual(json.loads(create_payload["data"]), card_json)

        stream_payload = build_stream_content_payload("chunk", sequence=1, uuid="uuid-mock-1")
        self.assertEqual(stream_payload, {"content": "chunk", "sequence": 1, "uuid": "uuid-mock-1"})
        self.assertEqual(build_stream_content_payload("chunk", sequence=2), {"content": "chunk", "sequence": 2})

        update_payload = build_update_card_payload(card_json, sequence=3, uuid="uuid-mock-2")
        self.assertEqual(update_payload["sequence"], 3)
        self.assertEqual(update_payload["uuid"], "uuid-mock-2")
        self.assertEqual(update_payload["card"]["type"], "card_json")
        self.assertEqual(json.loads(update_payload["card"]["data"]), card_json)

        settings_payload = build_update_settings_payload(
            {"config": {"streaming_mode": False}},
            sequence=4,
            uuid="uuid-mock-3",
        )
        self.assertEqual(settings_payload["sequence"], 4)
        self.assertEqual(settings_payload["uuid"], "uuid-mock-3")
        self.assertEqual(json.loads(settings_payload["settings"]), {"config": {"streaming_mode": False}})

        for bad_seq in (0, -1):
            with self.assertRaises(ValueError):
                build_stream_content_payload("x", sequence=bad_seq)
            with self.assertRaises(ValueError):
                build_update_card_payload(card_json, sequence=bad_seq)
            with self.assertRaises(ValueError):
                build_update_settings_payload({"config": {}}, sequence=bad_seq)


class TestStreamingCardUpdaterThrottling(unittest.TestCase):
    """验证时间窗口节流、去重、顺序号严格递增与异常恢复。"""

    def test_constructor_validation_and_lifecycle_guards(self) -> None:
        transport = _RecordingTransport()
        for method in ("create_card", "stream_element_content", "update_card", "update_settings"):
            self.assertTrue(callable(getattr(transport, method)))
        self.assertTrue(hasattr(CardTransport, "__protocol_attrs__") or callable(CardTransport))

        with self.assertRaises(ValueError):
            StreamingCardUpdater(transport, "   ")
        with self.assertRaises(ValueError):
            StreamingCardUpdater(transport, "sess-mock-1", throttle_seconds=-0.1)
        with self.assertRaises(ValueError):
            StreamingCardUpdater(transport, "sess-mock-1", throttle_seconds=float("inf"))
        with self.assertRaises(ValueError):
            StreamingCardUpdater(transport, "sess-mock-1", max_content_chars=100)

        updater = StreamingCardUpdater(transport, "sess-mock-1")
        with self.assertRaises(RuntimeError):
            updater.finish_completed()

        card_id = updater.start()
        self.assertEqual(card_id, "card-mock-1")
        with self.assertRaises(RuntimeError):
            updater.start()

    def test_throttling_coalesces_within_window_and_flushes_on_timer(self) -> None:
        transport = _RecordingTransport()
        clock = _FakeClock(100.0)
        timers = _FakeTimerScheduler()
        updater = StreamingCardUpdater(
            transport,
            "sess-mock-1",
            turn_id="turn-mock-1",
            throttle_seconds=0.25,
            clock=clock,
            timer_factory=timers,
            uuid_factory=_SeqUuidFactory(),
        )

        self.assertEqual(updater.start(), "card-mock-1")
        self.assertEqual(len(transport.created_cards), 1)
        self.assertEqual(len(transport.stream_calls), 0)

        # 窗口内连续追加多个 chunk：应当聚合且只启动一个尾随定时器
        clock.advance(0.05)
        updater.append_text("Hel")
        clock.advance(0.05)
        updater.append_text("lo, ")
        clock.advance(0.05)
        updater.append_text("world!")

        self.assertEqual(len(transport.stream_calls), 0)
        self.assertEqual(len(timers.timers), 1)
        self.assertAlmostEqual(timers.timers[0].delay, 0.20, places=6)
        self.assertTrue(timers.timers[0].daemon)
        self.assertEqual(updater.accumulated_text, "Hello, world!")

        # 触发定时器：应推送状态栏 + 完整聚合后的正文
        clock.advance(0.20)
        timers.timers[0].fire()

        self.assertEqual(len(transport.stream_calls), 2)
        self.assertEqual(transport.stream_calls[0]["element_id"], STATUS_ELEMENT_ID)
        self.assertEqual(transport.stream_calls[0]["content"], "✍️ 正在生成回复...")
        self.assertEqual(transport.stream_calls[0]["sequence"], 1)
        self.assertEqual(transport.stream_calls[1]["element_id"], CONTENT_ELEMENT_ID)
        self.assertEqual(transport.stream_calls[1]["content"], "Hello, world!")
        self.assertEqual(transport.stream_calls[1]["sequence"], 2)

        # 窗口再次到期后的下一增量应立即发送，不创建多余定时器
        clock.advance(0.30)
        updater.append_text(" Next")
        self.assertEqual(len(timers.timers), 1)
        self.assertEqual(len(transport.stream_calls), 3)
        self.assertEqual(transport.stream_calls[2]["content"], "Hello, world! Next")
        self.assertEqual(transport.stream_calls[2]["sequence"], 3)

    def test_explicit_flush_cancels_timer_and_deduplicates_unchanged_state(self) -> None:
        transport = _RecordingTransport()
        clock = _FakeClock(50.0)
        timers = _FakeTimerScheduler()
        updater = StreamingCardUpdater(
            transport,
            "sess-mock-1",
            throttle_seconds=0.5,
            clock=clock,
            timer_factory=timers,
            uuid_factory=_SeqUuidFactory(),
        )
        updater.start()

        clock.advance(0.1)
        updater.append_text("Part 1")
        self.assertEqual(len(timers.timers), 1)
        self.assertFalse(timers.timers[0].cancelled)

        # 显式 flush 应取消挂起定时器并立即发送
        updater.flush()
        self.assertTrue(timers.timers[0].cancelled)
        self.assertEqual(len(transport.stream_calls), 2)

        # 内容未变化时再次 flush 不产生重复请求
        updater.flush()
        self.assertEqual(len(transport.stream_calls), 2)

    def test_zero_throttle_streams_immediately_and_recovers_after_transport_error(self) -> None:
        transport = _RecordingTransport()
        clock = _FakeClock(0.0)
        timers = _FakeTimerScheduler()
        updater = StreamingCardUpdater(
            transport,
            "sess-mock-1",
            throttle_seconds=0.0,
            clock=clock,
            timer_factory=timers,
            uuid_factory=_SeqUuidFactory(),
        )
        updater.start()

        # 模拟正文要素推送发生一次瞬态异常：status 先成功，content 抛出并保留脏标记
        transport.fail_on_element_id = CONTENT_ELEMENT_ID
        transport.remaining_failures = 1
        with self.assertRaises(RuntimeError):
            updater.append_text("First ")

        self.assertEqual(len(timers.timers), 0)
        self.assertEqual(len(transport.stream_calls), 1)
        self.assertEqual(transport.stream_calls[0]["element_id"], STATUS_ELEMENT_ID)
        self.assertEqual(transport.stream_calls[0]["sequence"], 1)

        # 恢复后显式 flush 应重发尚未成功推送的正文（序列号继续严格递增）
        updater.flush()
        self.assertEqual(len(transport.stream_calls), 2)
        self.assertEqual(transport.stream_calls[1]["element_id"], CONTENT_ELEMENT_ID)
        self.assertEqual(transport.stream_calls[1]["content"], "First ")
        self.assertEqual(transport.stream_calls[1]["sequence"], 3)


class TestStreamingCardUpdaterLifecycle(unittest.TestCase):
    """验证 ACP 事件消费、超长前缀截断、终态收口与真实多线程并发节流。"""

    def test_handle_acp_updates_and_mark_stopping(self) -> None:
        transport = _RecordingTransport()
        updater = StreamingCardUpdater(
            transport,
            "sess-mock-1",
            throttle_seconds=0.0,
            clock=_FakeClock(10.0),
            uuid_factory=_SeqUuidFactory(),
        )
        updater.start()

        # 未开始任何思考或输出时的 user_message_chunk 不切插话状态
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="user_message_chunk",
                raw={"content": {"type": "text", "text": "hi"}},
            )
        )
        self.assertEqual(len(transport.stream_calls), 0)

        # 思考阶段切换为思考状态
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="agent_thought_chunk",
                raw={"content": {"type": "text", "text": "analyzing..."}},
            )
        )
        # 工具调用与完成
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="tool_call",
                raw={"toolCallId": "tc-1", "name": "search_docs", "status": "in_progress"},
            )
        )
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="tool_call_update",
                raw={"toolCallId": "tc-1", "name": "search_docs", "status": "completed"},
            )
        )
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="tool_call_update",
                raw={"toolCallId": "tc-2", "name": "run_check", "status": "failed"},
            )
        )
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="tool_call",
                raw={"toolCallId": "tc-3", "status": "in_progress"},
            )
        )
        # 正文生成后，后续思考片段不应覆盖生成状态
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="agent_message_chunk",
                raw={"content": {"type": "text", "text": "answer"}},
            )
        )
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="agent_thought_chunk",
                raw={"content": {"type": "text", "text": "more thoughts"}},
            )
        )
        # 已开始生成后收到插话指令
        updater.handle_update(
            AcpSessionUpdate(
                session_id="sess-mock-1",
                update_type="user_message_chunk",
                raw={"content": {"type": "text", "text": "focus on tests"}},
            )
        )
        updater.mark_stopping()

        status_history = [
            call["content"]
            for call in transport.stream_calls
            if call["element_id"] == STATUS_ELEMENT_ID
        ]
        self.assertEqual(
            status_history,
            [
                "💭 正在深度思考...",
                "🔧 正在执行：`search_docs`",
                "✅ 工具完成：`search_docs`",
                "⚠️ 工具异常：`run_check`",
                "🔧 正在执行：`tool`",
                "✍️ 正在生成回复...",
                "🧭 收到插话指令，正在调整处理...",
                "🛑 正在停止当前任务...",
            ],
        )

    def test_prefix_preserving_truncation_when_exceeding_max_chars(self) -> None:
        transport = _RecordingTransport()
        self.assertGreater(DEFAULT_MAX_CONTENT_CHARS, 1000)
        updater = StreamingCardUpdater(
            transport,
            "sess-mock-1",
            throttle_seconds=0.0,
            max_content_chars=150,
            uuid_factory=_SeqUuidFactory(),
        )
        updater.start()

        chunk1 = "A" * 100
        chunk2 = "B" * 120
        updater.append_text(chunk1)
        updater.append_text(chunk2)
        updater.finish_completed()

        content_pushes = [
            call["content"]
            for call in transport.stream_calls
            if call["element_id"] == CONTENT_ELEMENT_ID
        ]
        self.assertEqual(len(content_pushes), 2)
        self.assertEqual(content_pushes[0], chunk1)
        self.assertLessEqual(len(content_pushes[1]), 150)
        self.assertTrue(content_pushes[1].startswith(chunk1))
        self.assertIn("后续部分已截断", content_pushes[1])

        final_card = transport.update_card_calls[0]["card_json"]
        final_md = _find_element(final_card, CONTENT_ELEMENT_ID)
        assert final_md is not None
        self.assertEqual(final_md["content"], content_pushes[1])

    def test_finalization_variants_settings_order_and_idempotency(self) -> None:
        # 1) COMPLETED via finalize_from_result with usage tokens
        t1 = _RecordingTransport("card-mock-completed")
        clock1 = _FakeClock(0.0)
        timers1 = _FakeTimerScheduler()
        u1 = StreamingCardUpdater(
            t1,
            "sess-mock-1",
            turn_id="turn-mock-1",
            throttle_seconds=0.5,
            clock=clock1,
            timer_factory=timers1,
            uuid_factory=_SeqUuidFactory(),
        )
        u1.start()
        clock1.advance(0.1)
        u1.append_text("partial ")
        pending_timer = timers1.timers[0]

        result = AcpPromptResult(
            session_id="sess-mock-1",
            stop_reason="end_turn",
            text="final result body",
            thoughts="",
            updates=[],
            usage={"totalTokens": 420},
        )
        final_card = u1.finalize_from_result(result)

        self.assertTrue(pending_timer.cancelled)
        self.assertTrue(u1.is_finished)
        self.assertEqual(u1.terminal_state, CardRunState.COMPLETED)
        self.assertFalse(final_card["config"]["streaming_mode"])
        self.assertEqual(final_card["header"]["template"], "green")
        self.assertIsNone(_find_element(final_card, STOP_BUTTON_ELEMENT_ID))

        status_el = _find_element(final_card, STATUS_ELEMENT_ID)
        content_el = _find_element(final_card, CONTENT_ELEMENT_ID)
        assert status_el is not None and content_el is not None
        self.assertEqual(status_el["content"], "✅ 已完成 · 420 tokens")
        self.assertEqual(content_el["content"], "final result body")

        # 确认先 PATCH settings 关闭流式，再 PUT card 全量结构更新
        self.assertEqual(
            [name for name, _ in t1.events[-2:]],
            ["update_settings", "update_card"],
        )
        self.assertEqual(
            t1.update_settings_calls[0]["settings"],
            {"config": {"streaming_mode": False}},
        )
        seqs = t1.all_sequences()
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), len(seqs))

        # 终态幂等性与后置操作免疫
        events_before = len(t1.events)
        same_card = u1.finish_cancelled()
        self.assertEqual(same_card, final_card)
        u1.append_text("ignored")
        u1.set_status("ignored")
        u1.flush()
        self.assertEqual(len(t1.events), events_before)

        # 2) CANCELLED via finalize_from_result (empty fallback preserved)
        t2 = _RecordingTransport("card-mock-cancelled")
        u2 = StreamingCardUpdater(t2, "sess-mock-2", throttle_seconds=0.0)
        u2.start()
        cancelled_card = u2.finalize_from_result(
            AcpPromptResult(
                session_id="sess-mock-2",
                stop_reason="cancelled",
                text="",
                thoughts="",
                updates=[],
            )
        )
        self.assertEqual(u2.terminal_state, CardRunState.CANCELLED)
        self.assertEqual(cancelled_card["header"]["template"], "orange")
        cancelled_content = _find_element(cancelled_card, CONTENT_ELEMENT_ID)
        assert cancelled_content is not None
        self.assertIn("已停止生成", cancelled_content["content"])

        # 3) ERROR via finish_error
        t3 = _RecordingTransport("card-mock-error")
        u3 = StreamingCardUpdater(t3, "sess-mock-3", throttle_seconds=0.0)
        u3.start()
        u3.append_text("partial output before crash")
        err_card = u3.finish_error("network timeout")
        self.assertEqual(u3.terminal_state, CardRunState.ERROR)
        self.assertEqual(err_card["header"]["template"], "red")
        err_status = _find_element(err_card, STATUS_ELEMENT_ID)
        err_content = _find_element(err_card, CONTENT_ELEMENT_ID)
        assert err_status is not None and err_content is not None
        self.assertIn("network timeout", err_status["content"])
        self.assertEqual(err_content["content"], "partial output before crash")

    def test_real_timer_multithreaded_burst_produces_strict_monotonic_sequences(self) -> None:
        transport = _RecordingTransport("card-mock-mt")
        updater = StreamingCardUpdater(
            transport,
            "sess-mock-mt",
            throttle_seconds=0.02,
            uuid_factory=_SeqUuidFactory(),
        )
        updater.start()

        errors: list[BaseException] = []

        def worker(worker_idx: int) -> None:
            try:
                for step in range(15):
                    updater.handle_update(
                        AcpSessionUpdate(
                            session_id="sess-mock-mt",
                            update_type="agent_message_chunk",
                            raw={"content": {"type": "text", "text": f"[{worker_idx}:{step}]"}},
                        )
                    )
            except BaseException as exc:  # pragma: no cover - 仅失败诊断用
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)
            self.assertFalse(thread.is_alive())

        self.assertEqual(errors, [])
        full_text = updater.accumulated_text
        self.assertEqual(len(full_text.split("]")) - 1, 60)

        final_card = updater.finalize_from_result(
            AcpPromptResult(
                session_id="sess-mock-mt",
                stop_reason="end_turn",
                text=full_text,
                thoughts="",
                updates=[],
            )
        )
        self.assertTrue(updater.is_finished)
        self.assertEqual(final_card["header"]["template"], "green")

        seqs = transport.all_sequences()
        self.assertGreaterEqual(len(seqs), 2)
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))


class TestModelAndEffortSelectorCards(unittest.TestCase):
    """验证 ACP 模型与思考强度元数据解析、/model 与 /effort CardKit 2.0 卡片结构及回调载荷编解码。"""

    def test_parse_config_options_prefers_base_models_and_efforts_with_legacy_fallback(self) -> None:
        session_info = {
            "sessionId": "sess-mock-1",
            "models": {
                "currentModelId": "gemini-4-argon-medium",
                "availableModels": [
                    {"modelId": "gemini-4-argon-medium", "name": "Gemini 4 Argon (Medium)"},
                    {"modelId": "gemini-4-argon-high", "name": "Gemini 4 Argon (High)"},
                ],
            },
            "configOptions": [
                {
                    "id": "model",
                    "category": "model",
                    "currentValue": " gemini-3.8-flash ",
                    "options": [
                        {"value": "gemini-4-argon", "name": "Gemini 4 Argon"},
                        {"value": "gemini-3.8-flash", "name": "Gemini 3.8 Flash"},
                        {"value": "gemini-3.8-flash", "name": "Duplicate Ignored"},
                        {"value": "  ", "name": "Invalid Blank"},
                    ],
                },
                {
                    "id": "thought_level",
                    "category": "thought_level",
                    "currentValue": " xhigh ",
                    "options": [
                        {"value": "low", "name": "Low"},
                        {"value": "medium", "name": "Medium"},
                        {"value": "high", "name": "High"},
                        {"value": "xhigh", "name": "Extra"},
                    ],
                },
            ],
        }
        self.assertTrue(has_config_option(session_info, option_id="model"))
        self.assertTrue(has_config_option(session_info, option_id="thought_level"))
        self.assertEqual(
            parse_available_models(session_info),
            (
                AcpModelOption("gemini-4-argon", "Gemini 4 Argon"),
                AcpModelOption("gemini-3.8-flash", "Gemini 3.8 Flash"),
            ),
        )
        self.assertEqual(parse_current_model_id(session_info), "gemini-3.8-flash")
        self.assertEqual(
            parse_available_efforts(session_info),
            (
                AcpEffortOption("low", "Low"),
                AcpEffortOption("medium", "Medium"),
                AcpEffortOption("high", "High"),
                AcpEffortOption("xhigh", "Extra"),
            ),
        )
        self.assertEqual(parse_current_effort_id(session_info), "xhigh")

        # 若无 configOptions（兼容旧格式），则回退至 models.availableModels；effort 返回空
        legacy_only = {
            "models": {
                "currentModelId": " legacy-model ",
                "availableModels": [{"modelId": "legacy-model"}],
            }
        }
        self.assertFalse(has_config_option(legacy_only, option_id="model"))
        self.assertEqual(
            parse_available_models(legacy_only),
            (AcpModelOption("legacy-model", "legacy-model"),),
        )
        self.assertEqual(parse_current_model_id(legacy_only), "legacy-model")
        self.assertEqual(parse_available_efforts(legacy_only), ())
        self.assertIsNone(parse_current_effort_id(legacy_only))
        self.assertEqual(parse_available_models({}), ())
        self.assertIsNone(parse_current_model_id({}))

    def test_callback_value_round_trip_and_validation_for_model_and_effort(self) -> None:
        m_val = build_model_select_callback_value(" gemini-3.8-flash ", conv_key=" p2p:oc_mock_1 ")
        self.assertEqual(
            m_val,
            {"action": MODEL_SELECT_ACTION_NAME, "model_id": "gemini-3.8-flash", "conv_key": "p2p:oc_mock_1"},
        )
        self.assertEqual(
            parse_model_select_callback_value(m_val),
            ModelSelectCallbackPayload(model_id="gemini-3.8-flash", conv_key="p2p:oc_mock_1"),
        )

        e_val = build_effort_select_callback_value(" high ", conv_key=" p2p:oc_mock_1 ")
        self.assertEqual(
            e_val,
            {"action": EFFORT_SELECT_ACTION_NAME, "effort_id": "high", "conv_key": "p2p:oc_mock_1"},
        )
        self.assertEqual(
            parse_effort_select_callback_value(e_val),
            EffortSelectCallbackPayload(effort_id="high", conv_key="p2p:oc_mock_1"),
        )
        self.assertEqual(
            parse_effort_select_callback_value(build_effort_select_callback_value("low")),
            EffortSelectCallbackPayload(effort_id="low", conv_key=None),
        )

        with self.assertRaises(ValueError):
            build_model_select_callback_value("  ")
        with self.assertRaises(ValueError):
            build_effort_select_callback_value("  ")
        with self.assertRaises(ValueError):
            build_effort_select_callback_value("low", action=" ")

        self.assertIsNone(parse_model_select_callback_value(e_val))
        self.assertIsNone(parse_effort_select_callback_value(m_val))
        self.assertIsNone(parse_effort_select_callback_value(None))
        self.assertIsNone(
            parse_effort_select_callback_value({"action": EFFORT_SELECT_ACTION_NAME, "effort_id": " "})
        )

    def test_build_model_and_effort_selector_cards_layout_and_empty_fallbacks(self) -> None:
        models = (
            AcpModelOption("model-a", "Model Alpha"),
            AcpModelOption("model-b", "Model Beta"),
            AcpModelOption("model-c", "Model Gamma"),
        )
        model_card = build_model_selector_card_json(
            models=models,
            current_model_id="model-b",
            conv_key="p2p:oc_mock_1",
            subtitle="会话演示",
        )
        self.assertEqual(model_card["schema"], CARD_SCHEMA_VERSION)
        self.assertTrue(model_card["config"]["update_multi"])
        self.assertEqual(model_card["header"]["subtitle"]["content"], "会话演示")
        m_status = _find_element(model_card, MODEL_STATUS_ELEMENT_ID)
        m_hint = _find_element(model_card, MODEL_HINT_ELEMENT_ID)
        assert m_status is not None and m_hint is not None
        self.assertIn("Model Beta", m_status["content"])
        self.assertIn("/effort", m_hint["content"])

        self.assertEqual(
            [e for e in _extract_elements(model_card) if e.get("tag") == "column_set"],
            [],
        )
        m_buttons = [e for e in _extract_elements(model_card) if e.get("tag") == "button"]
        self.assertEqual(len(m_buttons), 3)
        for idx, btn in enumerate(m_buttons):
            self.assertEqual(btn["width"], "fill")
            self.assertEqual(btn["element_id"], f"model_btn_{idx}")
            self.assertRegex(btn["element_id"], _VALID_ELEMENT_ID_RE)
        self.assertEqual(m_buttons[1]["type"], "primary")
        self.assertEqual(m_buttons[1]["text"]["content"], "✓ Model Beta")

        efforts = (
            AcpEffortOption("low", "Low"),
            AcpEffortOption("medium", "Medium"),
            AcpEffortOption("high", "High"),
            AcpEffortOption("xhigh", "Extra"),
        )
        effort_card = build_effort_selector_card_json(
            efforts=efforts,
            current_effort_id="xhigh",
            current_model_name="Gemini 4 Hydrogen (`gemini-4-hydrogen`)",
            conv_key="p2p:oc_mock_1",
        )
        e_status = _find_element(effort_card, EFFORT_STATUS_ELEMENT_ID)
        assert e_status is not None
        self.assertIn("Gemini 4 Hydrogen", e_status["content"])
        self.assertIn("Extra (`xhigh`)", e_status["content"])
        self.assertEqual(
            [e for e in _extract_elements(effort_card) if e.get("tag") == "column_set"],
            [],
        )
        e_buttons = [e for e in _extract_elements(effort_card) if e.get("tag") == "button"]
        self.assertEqual(len(e_buttons), 4)
        for idx, btn in enumerate(e_buttons):
            self.assertEqual(btn["width"], "fill")
            self.assertEqual(btn["element_id"], f"effort_btn_{idx}")
            self.assertRegex(btn["element_id"], _VALID_ELEMENT_ID_RE)
        self.assertEqual(e_buttons[3]["type"], "primary")
        self.assertEqual(e_buttons[3]["text"]["content"], "✓ Extra")
        self.assertEqual(
            parse_effort_select_callback_value(e_buttons[2]["behaviors"][0]["value"]),
            EffortSelectCallbackPayload(effort_id="high", conv_key="p2p:oc_mock_1"),
        )

        # 不支持思考强度调节时的提示卡片
        unsupported_card = build_effort_selector_card_json(
            efforts=(),
            current_effort_id=None,
            current_model_name="GPT-OSS 120B (Medium)",
        )
        u_status = _find_element(unsupported_card, EFFORT_STATUS_ELEMENT_ID)
        u_hint = _find_element(unsupported_card, EFFORT_HINT_ELEMENT_ID)
        assert u_status is not None and u_hint is not None
        self.assertIn("不支持调节", u_status["content"])
        self.assertIn("不支持单独调节思考强度", u_hint["content"])
        self.assertEqual(
            [
                e
                for e in _extract_elements(unsupported_card)
                if e.get("tag") in ("column_set", "button")
            ],
            [],
        )


if __name__ == "__main__":
    unittest.main()
