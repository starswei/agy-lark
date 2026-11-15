"""配置加载、open_id 默认拒绝鉴权与跨平台 agy 可执行文件发现。"""

from __future__ import annotations

from collections.abc import Iterable, MutableMapping
from dataclasses import dataclass, field
import math
import os
from pathlib import Path
import shutil

DEFAULT_FEISHU_DOMAIN = "https://open.feishu.cn"
DEFAULT_TURN_TIMEOUT_SECONDS = 600
DEFAULT_STREAM_THROTTLE = 0.25
DEFAULT_DATA_DIR = ".agy-lark"
DEFAULT_LOG_LEVEL = "INFO"


def _parse_env_value(raw_value: str) -> str:
    """解析单条 .env 赋值语句右侧的字符串值，支持首尾引号与行内注释。"""
    value = raw_value.strip()
    if not value:
        return ""

    if value[0] in ("'", '"'):
        quote = value[0]
        chars: list[str] = []
        escaped = False
        for ch in value[1:]:
            if quote == '"' and not escaped and ch == "\\":
                escaped = True
                continue
            if not escaped and ch == quote:
                return "".join(chars)
            if escaped:
                chars.append(ch)
                escaped = False
            else:
                chars.append(ch)
        # 未匹配到闭合引号时，仅剥离开头引号并返回清理后的内容
        return "".join(chars).strip()

    if value.startswith("#"):
        return ""

    # 未加引号时，仅剥离空白之后出现的 `#` 行内注释，避免误切 URL 中的 `#`
    for idx, ch in enumerate(value):
        if ch == "#" and idx > 0 and value[idx - 1].isspace():
            return value[:idx].strip()

    return value


def load_dotenv_file(
    dotenv_path: str | Path = ".env",
    *,
    environ: MutableMapping[str, str] | None = None,
    override: bool = False,
) -> dict[str, str]:
    """从 .env 文件读取键值对并写入目标环境变量映射，返回解析到的键值对字典。"""
    path = Path(dotenv_path)
    if not path.is_file():
        return {}

    target_env: MutableMapping[str, str] = os.environ if environ is None else environ
    parsed: dict[str, str] = {}

    content = path.read_text(encoding="utf-8-sig")
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export ") or line.startswith("export\t"):
            line = line[len("export") :].lstrip()

        raw_key, sep, raw_val = line.partition("=")
        if sep != "=":
            continue

        key = raw_key.strip()
        if not key:
            continue

        value = _parse_env_value(raw_val)
        parsed[key] = value
        if override or key not in target_env:
            target_env[key] = value

    return parsed


def parse_allowed_open_ids(raw: str | None) -> frozenset[str]:
    """将逗号分隔的 open_id 白名单字符串解析为不可变集合。"""
    if not raw:
        return frozenset()
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def is_authorized_open_id(
    open_id: str | None,
    allowed_open_ids: Iterable[str] | None,
) -> bool:
    """校验发送者 open_id 是否在白名单中；白名单为空时严格默认拒绝（Fail-Closed）。"""
    candidate = (open_id or "").strip()
    if not candidate or not allowed_open_ids:
        return False

    normalized = {
        item.strip()
        for item in allowed_open_ids
        if isinstance(item, str) and item.strip()
    }
    if not normalized:
        return False

    return candidate in normalized


def resolve_agy_executable(
    agy_exe: str | None = None,
    *,
    path_env: str | None = None,
) -> str:
    """跨平台定位 agy 可执行文件：优先使用显式路径，否则从 PATH 自动发现。"""
    candidate = (agy_exe or "").strip()
    if candidate:
        expanded = os.path.expanduser(candidate)
        explicit_path = Path(expanded)
        if explicit_path.is_file():
            return str(explicit_path.resolve())

        if path_env is None or path_env.strip():
            found = (
                shutil.which(expanded)
                if path_env is None
                else shutil.which(expanded, path=path_env)
            )
            if found:
                return str(Path(found).resolve())

        raise FileNotFoundError(f"未找到指定的 agy 可执行文件: {candidate}")

    if path_env is not None and not path_env.strip():
        raise FileNotFoundError("PATH 为空，无法自动发现 agy 可执行文件")

    found = shutil.which("agy") if path_env is None else shutil.which("agy", path=path_env)
    if not found:
        raise FileNotFoundError(
            "未在系统 PATH 中找到 agy 可执行文件，请安装 Antigravity CLI 或设置 AGY_EXE"
        )

    return str(Path(found).resolve())


def _parse_bool_env(raw: str | None, *, default: bool, name: str) -> bool:
    """解析布尔型环境变量。"""
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"环境变量 {name} 的布尔值无效: {raw!r}")


def _parse_positive_int_env(raw: str | None, *, default: int, name: str) -> int:
    """解析正整数环境变量。"""
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 必须为正整数: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"环境变量 {name} 必须大于 0: {value}")
    return value


def _parse_non_negative_float_env(raw: str | None, *, default: float, name: str) -> float:
    """解析非负有限浮点数环境变量。"""
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 必须为数字: {raw!r}") from exc
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"环境变量 {name} 必须为非负有限数字: {value}")
    return value


@dataclass(frozen=True)
class AgyLarkConfig:
    """agy-lark 桥接器不可变运行时配置对象。"""

    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_domain: str = DEFAULT_FEISHU_DOMAIN
    allowed_open_ids: frozenset[str] = field(default_factory=frozenset)
    allow_group_mention: bool = False
    agy_exe: str = ""
    default_workspace: Path = field(default_factory=Path.cwd)
    turn_timeout_seconds: int = DEFAULT_TURN_TIMEOUT_SECONDS
    stream_throttle: float = DEFAULT_STREAM_THROTTLE
    data_dir: Path = Path(DEFAULT_DATA_DIR)
    log_level: str = DEFAULT_LOG_LEVEL

    def is_open_id_allowed(self, open_id: str | None) -> bool:
        """判断给定发送者 open_id 是否有权触发桥接器。"""
        return is_authorized_open_id(open_id, self.allowed_open_ids)


def _looks_like_explicit_path(raw_value: str) -> bool:
    """判断配置值是否为显式路径（区别于依赖系统 PATH 查找的裸命令名）。"""
    return (
        "/" in raw_value
        or "\\" in raw_value
        or raw_value.startswith((".", "~"))
        or (len(raw_value) >= 2 and raw_value[1] == ":")
    )


def _resolve_config_path(raw_value: str, *, base_dir: Path | None) -> Path:
    """展开用户主目录 ~；若仍为相对路径且提供了基准目录，则相对基准目录解析为规范路径。"""
    expanded = Path(os.path.expanduser(raw_value))
    if expanded.is_absolute():
        return expanded
    if base_dir is not None:
        return (base_dir / expanded).resolve()
    return expanded


def load_config(
    *,
    dotenv_path: str | Path | None = ".env",
    environ: MutableMapping[str, str] | None = None,
    resolve_exe: bool = True,
) -> AgyLarkConfig:
    """加载 .env 与环境变量，返回校验后的 AgyLarkConfig 实例。"""
    env: MutableMapping[str, str] = os.environ if environ is None else environ
    preexisting_keys = set(env.keys())
    parsed_dotenv: dict[str, str] = {}
    dotenv_base_dir: Path | None = None

    if dotenv_path is not None:
        dotenv_file = Path(dotenv_path)
        if dotenv_file.is_file():
            dotenv_base_dir = dotenv_file.resolve().parent
            parsed_dotenv = load_dotenv_file(dotenv_file, environ=env, override=False)

    def _source_base_dir(key: str) -> Path | None:
        """仅当生效配置来自 .env 文件（未被外部环境变量提前占坑）时返回 .env 所在目录。"""
        if dotenv_base_dir is not None and key not in preexisting_keys and key in parsed_dotenv:
            return dotenv_base_dir
        return None

    feishu_app_id = env.get("FEISHU_APP_ID", "").strip()
    feishu_app_secret = env.get("FEISHU_APP_SECRET", "").strip()
    feishu_domain = env.get("FEISHU_DOMAIN", "").strip() or DEFAULT_FEISHU_DOMAIN

    allowed_open_ids = parse_allowed_open_ids(env.get("AGY_LARK_ALLOWED_OPEN_IDS"))
    allow_group_mention = _parse_bool_env(
        env.get("AGY_LARK_ALLOW_GROUP_MENTION"),
        default=False,
        name="AGY_LARK_ALLOW_GROUP_MENTION",
    )

    raw_agy_exe = env.get("AGY_EXE", "").strip()
    if raw_agy_exe and _looks_like_explicit_path(raw_agy_exe):
        exe_base_dir = _source_base_dir("AGY_EXE")
        if exe_base_dir is not None:
            raw_agy_exe = str(_resolve_config_path(raw_agy_exe, base_dir=exe_base_dir))
    agy_exe = (
        resolve_agy_executable(raw_agy_exe, path_env=env.get("PATH"))
        if resolve_exe
        else raw_agy_exe
    )

    raw_workspace = env.get("AGY_DEFAULT_WORKSPACE", "").strip()
    default_workspace = (
        _resolve_config_path(raw_workspace, base_dir=_source_base_dir("AGY_DEFAULT_WORKSPACE"))
        if raw_workspace
        else Path.cwd()
    )

    turn_timeout_seconds = _parse_positive_int_env(
        env.get("AGY_TURN_TIMEOUT_SECONDS"),
        default=DEFAULT_TURN_TIMEOUT_SECONDS,
        name="AGY_TURN_TIMEOUT_SECONDS",
    )
    stream_throttle = _parse_non_negative_float_env(
        env.get("AGY_LARK_STREAM_THROTTLE"),
        default=DEFAULT_STREAM_THROTTLE,
        name="AGY_LARK_STREAM_THROTTLE",
    )

    raw_data_dir = env.get("AGY_LARK_DATA_DIR", "").strip() or DEFAULT_DATA_DIR
    data_dir = _resolve_config_path(raw_data_dir, base_dir=_source_base_dir("AGY_LARK_DATA_DIR"))

    log_level = (env.get("AGY_LARK_LOG_LEVEL", "").strip() or DEFAULT_LOG_LEVEL).upper()

    return AgyLarkConfig(
        feishu_app_id=feishu_app_id,
        feishu_app_secret=feishu_app_secret,
        feishu_domain=feishu_domain,
        allowed_open_ids=allowed_open_ids,
        allow_group_mention=allow_group_mention,
        agy_exe=agy_exe,
        default_workspace=default_workspace,
        turn_timeout_seconds=turn_timeout_seconds,
        stream_throttle=stream_throttle,
        data_dir=data_dir,
        log_level=log_level,
    )
