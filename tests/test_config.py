"""agy_lark.config 离线单元测试（兼容 unittest 与 pytest）。"""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

from agy_lark import (
    AgyLarkConfig,
    is_authorized_open_id,
    load_config,
    load_dotenv_file,
    parse_allowed_open_ids,
    resolve_agy_executable,
)


def _create_mock_agy(directory: Path) -> Path:
    """在临时目录中创建模拟的跨平台 agy 可执行文件。"""
    filename = "agy.exe" if os.name == "nt" else "agy"
    exe_path = directory / filename
    exe_path.write_text("mock agy binary", encoding="utf-8")
    if os.name != "nt":
        exe_path.chmod(0o755)
    return exe_path


class TestOpenIdAuthorization(unittest.TestCase):
    """验证 Fail-Closed（默认拒绝）白名单鉴权边界。"""

    def test_fail_closed_when_allowed_ids_empty_none_or_whitespace(self) -> None:
        self.assertEqual(parse_allowed_open_ids(None), frozenset())
        self.assertEqual(parse_allowed_open_ids(""), frozenset())
        self.assertEqual(parse_allowed_open_ids(" , , \t "), frozenset())

        self.assertFalse(is_authorized_open_id("ou_abc123", None))
        self.assertFalse(is_authorized_open_id("ou_abc123", frozenset()))
        self.assertFalse(is_authorized_open_id("ou_abc123", ["", "   "]))
        self.assertFalse(is_authorized_open_id("", ["ou_abc123"]))
        self.assertFalse(is_authorized_open_id("   ", ["ou_abc123"]))
        self.assertFalse(is_authorized_open_id(None, ["ou_abc123"]))

    def test_parse_and_authorize_multiple_open_ids(self) -> None:
        allowed = parse_allowed_open_ids(" ou_alice , ou_bob ,, ")
        self.assertEqual(allowed, frozenset({"ou_alice", "ou_bob"}))

        self.assertTrue(is_authorized_open_id("ou_alice", allowed))
        self.assertTrue(is_authorized_open_id(" ou_bob ", allowed))
        self.assertFalse(is_authorized_open_id("ou_mallory", allowed))

        cfg = AgyLarkConfig(allowed_open_ids=allowed)
        self.assertTrue(cfg.is_open_id_allowed("ou_alice"))
        self.assertFalse(cfg.is_open_id_allowed("ou_eve"))


class TestDotenvLoader(unittest.TestCase):
    """验证 .env 文件解析与环境变量覆盖行为。"""

    def test_load_dotenv_handles_comments_quotes_export_and_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_file = Path(tmp) / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "# 注释行应忽略",
                        "",
                        "FEISHU_APP_ID=cli_demo_123",
                        'FEISHU_APP_SECRET="secret with spaces"',
                        "export FEISHU_DOMAIN='https://open.feishu.cn'",
                        "AGY_LARK_LOG_LEVEL=DEBUG # 行内注释",
                        "AGY_LARK_EMPTY=",
                        "INVALID_LINE_WITHOUT_EQUALS",
                    ]
                ),
                encoding="utf-8",
            )

            target_env = {"FEISHU_APP_ID": "already_set_in_os"}
            parsed = load_dotenv_file(env_file, environ=target_env, override=False)

            self.assertEqual(parsed["FEISHU_APP_ID"], "cli_demo_123")
            self.assertEqual(target_env["FEISHU_APP_ID"], "already_set_in_os")
            self.assertEqual(target_env["FEISHU_APP_SECRET"], "secret with spaces")
            self.assertEqual(target_env["FEISHU_DOMAIN"], "https://open.feishu.cn")
            self.assertEqual(target_env["AGY_LARK_LOG_LEVEL"], "DEBUG")
            self.assertEqual(target_env["AGY_LARK_EMPTY"], "")

            load_dotenv_file(env_file, environ=target_env, override=True)
            self.assertEqual(target_env["FEISHU_APP_ID"], "cli_demo_123")

    def test_missing_dotenv_returns_empty_dict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "nonexistent.env"
            env: dict[str, str] = {}
            self.assertEqual(load_dotenv_file(missing, environ=env), {})
            self.assertEqual(env, {})


class TestResolveAgyExecutable(unittest.TestCase):
    """验证跨平台 AGY_EXE / PATH 自动发现逻辑。"""

    def test_explicit_agy_exe_hit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            exe = _create_mock_agy(Path(tmp))
            resolved = resolve_agy_executable(str(exe), path_env="")
            self.assertEqual(Path(resolved), exe.resolve())

    def test_explicit_agy_exe_miss_raises_file_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_name = "missing_agy.exe" if os.name == "nt" else "missing_agy"
            missing = Path(tmp) / missing_name
            with self.assertRaises(FileNotFoundError):
                resolve_agy_executable(str(missing), path_env="")

    def test_path_autodiscovery_hit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            exe = _create_mock_agy(Path(tmp))
            resolved = resolve_agy_executable(None, path_env=tmp)
            self.assertEqual(Path(resolved), exe.resolve())

    def test_empty_or_missing_path_raises_file_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as empty_dir:
            with self.assertRaises(FileNotFoundError):
                resolve_agy_executable("", path_env="")
            with self.assertRaises(FileNotFoundError):
                resolve_agy_executable(None, path_env=empty_dir)


class TestLoadConfig(unittest.TestCase):
    """验证完整配置对象组装与非法数值参数拦截。"""

    def test_load_config_end_to_end_from_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            exe = _create_mock_agy(tmp_path)
            ws_dir = tmp_path / "workspace"
            ws_dir.mkdir()

            env_file = tmp_path / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "FEISHU_APP_ID=cli_test_app",
                        "FEISHU_APP_SECRET=sec_test",
                        "FEISHU_DOMAIN=https://open.larksuite.com",
                        "AGY_LARK_ALLOWED_OPEN_IDS=ou_1, ou_2",
                        "AGY_LARK_ALLOW_GROUP_MENTION=true",
                        f"AGY_EXE={exe}",
                        f"AGY_DEFAULT_WORKSPACE={ws_dir}",
                        "AGY_TURN_TIMEOUT_SECONDS=120",
                        "AGY_LARK_STREAM_THROTTLE=0.5",
                        "AGY_LARK_DATA_DIR=.custom-agy",
                        "AGY_LARK_LOG_LEVEL=debug",
                    ]
                ),
                encoding="utf-8",
            )

            isolated_env: dict[str, str] = {"PATH": ""}
            cfg = load_config(dotenv_path=env_file, environ=isolated_env, resolve_exe=True)

            self.assertEqual(cfg.feishu_app_id, "cli_test_app")
            self.assertEqual(cfg.feishu_app_secret, "sec_test")
            self.assertEqual(cfg.feishu_domain, "https://open.larksuite.com")
            self.assertEqual(cfg.allowed_open_ids, frozenset({"ou_1", "ou_2"}))
            self.assertTrue(cfg.allow_group_mention)
            self.assertEqual(Path(cfg.agy_exe), exe.resolve())
            self.assertEqual(cfg.default_workspace, ws_dir)
            self.assertEqual(cfg.turn_timeout_seconds, 120)
            self.assertEqual(cfg.stream_throttle, 0.5)
            self.assertEqual(cfg.data_dir, Path(".custom-agy"))
            self.assertEqual(cfg.log_level, "DEBUG")
            self.assertTrue(cfg.is_open_id_allowed("ou_1"))
            self.assertFalse(cfg.is_open_id_allowed("ou_unknown"))

    def test_load_config_validation_errors(self) -> None:
        base_env = {"AGY_TURN_TIMEOUT_SECONDS": "0"}
        with self.assertRaises(ValueError):
            load_config(dotenv_path=None, environ=base_env, resolve_exe=False)

        neg_throttle_env = {"AGY_LARK_STREAM_THROTTLE": "-0.1"}
        with self.assertRaises(ValueError):
            load_config(dotenv_path=None, environ=neg_throttle_env, resolve_exe=False)

        bad_bool_env = {"AGY_LARK_ALLOW_GROUP_MENTION": "maybe"}
        with self.assertRaises(ValueError):
            load_config(dotenv_path=None, environ=bad_bool_env, resolve_exe=False)


if __name__ == "__main__":
    unittest.main()
