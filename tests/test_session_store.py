"""agylark.session_store 离线单元测试（原子写入、损坏容错与有界裁剪）。"""

from __future__ import annotations

import json
import logging
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

from agylark import (
    DEFAULT_MAPPING_CAPACITY,
    SESSION_STORE_SCHEMA_VERSION,
    JsonSessionStore,
    PersistedConversation,
    PersistedSessionState,
)


class TestJsonSessionStore(unittest.TestCase):
    """验证 sessions.json 读写、原子替换、损坏容错与有界裁剪。"""

    def test_missing_file_loads_empty_and_roundtrip_preserves_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "nested" / "sessions.json"
            store = JsonSessionStore(store_path)

            initial = store.load()
            self.assertEqual(initial.schema_version, SESSION_STORE_SCHEMA_VERSION)
            self.assertEqual(initial.conversations, {})
            self.assertEqual(initial.thread_aliases, ())
            self.assertEqual(initial.reply_message_keys, ())
            self.assertFalse(store_path.exists())

            state = PersistedSessionState(
                conversations={
                    "p2p:oc_dm_1": PersistedConversation(
                        session_id="sess-dm-1",
                        cwd="/workspace/alpha",
                        model_id="gemini-4-hydrogen",
                        effort_id="xhigh",
                        updated_at="2026-11-15T12:00:00+00:00",
                    ),
                    "group:oc_g_1:root:om_root_1": PersistedConversation(
                        session_id="sess-g-1",
                        cwd="/workspace/alpha",
                        model_id=None,
                        effort_id=None,
                        updated_at="2026-11-15T12:01:00+00:00",
                    ),
                },
                thread_aliases=(
                    ("thread:oc_g_1:omt_1", "group:oc_g_1:root:om_root_1"),
                ),
                reply_message_keys=(
                    ("om_bot_card_1", "group:oc_g_1:root:om_root_1"),
                    ("om_bot_text_2", "p2p:oc_dm_1"),
                ),
            )
            store.save(state)
            self.assertTrue(store_path.is_file())

            raw = json.loads(store_path.read_text(encoding="utf-8"))
            self.assertEqual(raw["schema_version"], 1)
            self.assertEqual(raw["conversations"]["p2p:oc_dm_1"]["session_id"], "sess-dm-1")
            self.assertEqual(raw["conversations"]["p2p:oc_dm_1"]["cwd"], "/workspace/alpha")
            self.assertEqual(raw["conversations"]["p2p:oc_dm_1"]["model_id"], "gemini-4-hydrogen")
            self.assertEqual(raw["conversations"]["p2p:oc_dm_1"]["effort_id"], "xhigh")
            self.assertEqual(
                raw["thread_aliases"],
                [["thread:oc_g_1:omt_1", "group:oc_g_1:root:om_root_1"]],
            )
            self.assertEqual(
                raw["reply_message_keys"],
                [
                    ["om_bot_card_1", "group:oc_g_1:root:om_root_1"],
                    ["om_bot_text_2", "p2p:oc_dm_1"],
                ],
            )

            reloaded = store.load()
            self.assertEqual(reloaded, state)

    def test_atomic_replace_preserves_old_file_on_failure_and_supports_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_dir = Path(tmp)
            store_path = store_dir / "sessions.json"
            store = JsonSessionStore(store_path)

            original_state = PersistedSessionState(
                conversations={
                    "p2p:oc_1": PersistedConversation(session_id="sess-orig", cwd="/ws"),
                }
            )
            store.save(original_state)

            new_state = PersistedSessionState(
                conversations={
                    "p2p:oc_1": PersistedConversation(session_id="sess-new", cwd="/ws"),
                }
            )
            with mock.patch("agylark.session_store.os.replace", side_effect=OSError("disk error")):
                with self.assertRaises(OSError):
                    store.save(new_state)

            # 旧文件完好且同目录无残留临时文件
            self.assertEqual(store.load(), original_state)
            self.assertEqual([p.name for p in store_dir.iterdir()], ["sessions.json"])

            # 多线程并发保存与读取不损坏 JSON
            errors: list[Exception] = []

            def _writer(idx: int) -> None:
                try:
                    for step in range(10):
                        store.save(
                            PersistedSessionState(
                                conversations={
                                    f"p2p:oc_{idx}": PersistedConversation(
                                        session_id=f"sess-{idx}-{step}",
                                        cwd="/ws",
                                    )
                                },
                                reply_message_keys=((f"msg_{idx}_{step}", f"p2p:oc_{idx}"),),
                            )
                        )
                        loaded = store.load()
                        self.assertEqual(loaded.schema_version, 1)
                except Exception as exc:  # pragma: no cover - 仅失败时捕获断言
                    errors.append(exc)

            threads = [threading.Thread(target=_writer, args=(i,)) for i in range(4)]
            for th in threads:
                th.start()
            for th in threads:
                th.join(timeout=5.0)
            self.assertEqual(errors, [])
            self.assertEqual([p.name for p in store_dir.iterdir()], ["sessions.json"])

    def test_corrupted_json_and_bad_schema_degrade_to_empty_with_error_logs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "sessions.json"
            store = JsonSessionStore(store_path)

            # 1) 损坏 JSON 文本
            store_path.write_text("{broken json", encoding="utf-8")
            with self.assertLogs("agylark.session_store", level=logging.ERROR) as err_logs:
                state = store.load()
            self.assertEqual(state, PersistedSessionState())
            self.assertTrue(any("读取或解析会话持久化文件失败" in line for line in err_logs.output))

            # 2) 非字典根节点
            store_path.write_text('["not", "an", "object"]', encoding="utf-8")
            with self.assertLogs("agylark.session_store", level=logging.ERROR):
                self.assertEqual(store.load(), PersistedSessionState())

            # 3) 不受支持的 schema_version（含布尔值 True 等畸形值）
            for bad_ver in (999, "1", True, None):
                store_path.write_text(
                    json.dumps({"schema_version": bad_ver, "conversations": {}}),
                    encoding="utf-8",
                )
                with self.assertLogs("agylark.session_store", level=logging.ERROR):
                    self.assertEqual(store.load(), PersistedSessionState())

            # 4) 合法 schema_version 下局部脏条目容错跳过，保留合法条目
            store_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "conversations": {
                            "valid_key": {"session_id": "sess-ok", "cwd": "/ws"},
                            "": {"session_id": "bad"},
                            "bad_entry": "not-a-dict",
                        },
                        "thread_aliases": [
                            ["thread:c:1", "group:c:root:1"],
                            ["bad_only_one"],
                            ["", "group:c:root:2"],
                        ],
                        "reply_message_keys": [
                            ["om_1", "valid_key"],
                            [123, "valid_key"],
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertLogs("agylark.session_store", level=logging.WARNING):
                partial = store.load()
            self.assertEqual(set(partial.conversations.keys()), {"valid_key"})
            self.assertEqual(partial.conversations["valid_key"].session_id, "sess-ok")
            self.assertEqual(partial.thread_aliases, (("thread:c:1", "group:c:root:1"),))
            self.assertEqual(partial.reply_message_keys, (("om_1", "valid_key"),))

    def test_bounded_trimming_enforces_default_capacity_and_lru_order(self) -> None:
        self.assertEqual(DEFAULT_MAPPING_CAPACITY, 2048)
        with tempfile.TemporaryDirectory() as tmp:
            store_path = Path(tmp) / "sessions.json"
            store = JsonSessionStore(store_path)

            total = DEFAULT_MAPPING_CAPACITY + 250
            aliases = [(f"thread:oc:{i}", f"group:oc:root:{i}") for i in range(total)]
            # 重复命中早期键 thread:oc:0 应将其提升至末尾避免被裁剪
            aliases.append(("thread:oc:0", "group:oc:root:updated"))
            replies = [(f"om_bot_{i}", f"conv_{i}") for i in range(total)]

            store.save(
                PersistedSessionState(
                    thread_aliases=tuple(aliases),
                    reply_message_keys=tuple(replies),
                )
            )

            loaded = store.load()
            self.assertEqual(len(loaded.thread_aliases), DEFAULT_MAPPING_CAPACITY)
            self.assertEqual(len(loaded.reply_message_keys), DEFAULT_MAPPING_CAPACITY)

            alias_dict = dict(loaded.thread_aliases)
            self.assertEqual(alias_dict["thread:oc:0"], "group:oc:root:updated")
            self.assertNotIn("thread:oc:1", alias_dict)
            self.assertEqual(loaded.thread_aliases[-1], ("thread:oc:0", "group:oc:root:updated"))

            self.assertEqual(loaded.reply_message_keys[0], ("om_bot_250", "conv_250"))
            self.assertEqual(loaded.reply_message_keys[-1], (f"om_bot_{total - 1}", f"conv_{total - 1}"))


if __name__ == "__main__":
    unittest.main()
