"""交互式及非交互式 `agylark init` 初始化向导与环境自检工具。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import getpass
import os
from pathlib import Path
import sys
from typing import TextIO

from .acp_client import AgyAcpClient
from .config import (
    DEFAULT_DATA_DIR,
    DEFAULT_FEISHU_DOMAIN,
    DEFAULT_LOG_LEVEL,
    DEFAULT_STREAM_THROTTLE,
    DEFAULT_TURN_TIMEOUT_SECONDS,
    KNOWN_CONFIG_ENV_KEYS,
    parse_allowed_open_ids,
    resolve_agy_executable,
)

LARK_GLOBAL_DOMAIN = "https://open.larksuite.com"


@dataclass(frozen=True)
class InitValues:
    """待写入 `.env` 的结构化初始化配置值。"""

    feishu_app_id: str = ""
    feishu_app_secret: str = ""
    feishu_domain: str = DEFAULT_FEISHU_DOMAIN
    allowed_open_ids: str = ""
    allow_group_mention: bool = False
    agy_exe: str = ""
    agy_model: str = ""
    agy_effort: str = ""
    default_workspace: str = "./workspace"
    turn_timeout_seconds: int = DEFAULT_TURN_TIMEOUT_SECONDS
    stream_throttle: float = DEFAULT_STREAM_THROTTLE
    data_dir: str = DEFAULT_DATA_DIR
    log_level: str = DEFAULT_LOG_LEVEL


def detect_residual_env_vars(environ: Mapping[str, str] | None = None) -> list[str]:
    """检测当前进程环境中已存在、可能覆盖 `.env` 的配置变量名列表。"""
    env = os.environ if environ is None else environ
    return [key for key in KNOWN_CONFIG_ENV_KEYS if key in env]


def detect_agy_executable(environ: Mapping[str, str] | None = None) -> tuple[str | None, str | None]:
    """探测本地 `agy` 可执行文件路径，返回 `(resolved_path, error_message)`。"""
    env = os.environ if environ is None else environ
    explicit = env.get("AGY_EXE")
    path_env = env.get("PATH")
    try:
        resolved = resolve_agy_executable(explicit, path_env=path_env)
    except FileNotFoundError as exc:
        return None, str(exc)
    return resolved, None


def _format_env_value(value: str) -> str:
    """将配置值安全格式化为 `.env` 右侧字符串，保证 `load_dotenv_file` 无损回读。"""
    if not value:
        return ""
    needs_quotes = (
        any(ch.isspace() for ch in value)
        or "#" in value
        or '"' in value
        or "'" in value
    )
    if not needs_quotes:
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def format_env_file_content(values: InitValues) -> str:
    """生成带分区说明注释的标准 `.env` 配置文件内容。"""
    allowed_normalized = ",".join(sorted(parse_allowed_open_ids(values.allowed_open_ids)))
    allow_group_str = "true" if values.allow_group_mention else "false"
    lines = [
        "# ==============================================================================",
        "# agy-lark 运行配置文件（由 `agylark init` 生成）",
        "# 注意：此文件包含敏感应用凭据，严禁提交到任何 Git 仓库！",
        "# ==============================================================================",
        "",
        "# --- 飞书 / Lark 应用凭据（官方 lark-oapi WebSocket 长连接） ---",
        "# 必填：在飞书开放平台应用「凭证与基础信息」中获取（例如 cli_xxxxxxxxxxxxxxxx）",
        f"FEISHU_APP_ID={_format_env_value(values.feishu_app_id)}",
        f"FEISHU_APP_SECRET={_format_env_value(values.feishu_app_secret)}",
        "# 飞书国内版使用 https://open.feishu.cn ；Lark 国际版使用 https://open.larksuite.com",
        f"FEISHU_DOMAIN={_format_env_value(values.feishu_domain or DEFAULT_FEISHU_DOMAIN)}",
        "",
        "# --- 安全与访问控制（默认 Fail-Closed 严格拒绝） ---",
        "# 逗号分隔的已授权用户 open_id（ou_xxx）；留空将拒绝所有飞书消息！",
        f"AGY_LARK_ALLOWED_OPEN_IDS={_format_env_value(allowed_normalized)}",
        "# 是否允许已授权用户在群聊中通过 @机器人 触发对话",
        f"AGY_LARK_ALLOW_GROUP_MENTION={allow_group_str}",
        "",
        "# --- Antigravity (agy) ACP 运行时 ---",
        "# 留空则自动从系统 PATH 查找 agy，或填写显式可执行文件路径",
        f"AGY_EXE={_format_env_value(values.agy_exe)}",
        "# 新建会话自动下发的默认模型与思考强度（留空使用 Antigravity 默认设置）",
        f"AGY_MODEL={_format_env_value(values.agy_model)}",
        f"AGY_EFFORT={_format_env_value(values.agy_effort)}",
        "# 相对路径（如 ./workspace）将严格相对于本 .env 所在目录解析",
        f"AGY_DEFAULT_WORKSPACE={_format_env_value(values.default_workspace or './workspace')}",
        f"AGY_TURN_TIMEOUT_SECONDS={values.turn_timeout_seconds}",
        "",
        "# --- 流式卡片与守护服务日志 ---",
        f"AGY_LARK_STREAM_THROTTLE={values.stream_throttle}",
        f"AGY_LARK_DATA_DIR={_format_env_value(values.data_dir or DEFAULT_DATA_DIR)}",
        f"AGY_LARK_LOG_LEVEL={_format_env_value((values.log_level or DEFAULT_LOG_LEVEL).upper())}",
        "",
    ]
    return "\n".join(lines)


def verify_feishu_credentials(
    app_id: str,
    app_secret: str,
    domain: str = DEFAULT_FEISHU_DOMAIN,
) -> tuple[bool, str]:
    """调用飞书开放平台 `tenant_access_token/internal` 接口校验 App ID 与 App Secret。"""
    from .config import AgyLarkConfig
    from .daemon import _ensure_lark_response_ok, _import_lark_oapi, build_feishu_client

    try:
        _import_lark_oapi()
        from lark_oapi.api.auth.v3 import (
            InternalTenantAccessTokenRequest,
            InternalTenantAccessTokenRequestBody,
        )

        cfg = AgyLarkConfig(
            feishu_app_id=app_id,
            feishu_app_secret=app_secret,
            feishu_domain=domain or DEFAULT_FEISHU_DOMAIN,
            log_level="ERROR",
        )
        client = build_feishu_client(cfg)
        req_body = (
            InternalTenantAccessTokenRequestBody.builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .build()
        )
        request = InternalTenantAccessTokenRequest.builder().request_body(req_body).build()
        response = client.auth.v3.tenant_access_token.internal(request)
        _ensure_lark_response_ok(response, "校验飞书应用凭据")
    except Exception as exc:
        return False, f"飞书凭据校验失败：{exc}"
    return True, "飞书凭据校验通过（已成功获取 tenant_access_token）"


def verify_acp_handshake(
    agy_exe: str | None,
    workspace: Path,
    *,
    timeout: float = 15.0,
) -> tuple[bool, str]:
    """启动本地 `agy acp --stdio` 并执行一次 `initialize` 协议握手自检。"""
    try:
        client = AgyAcpClient(
            executable=agy_exe.strip() if isinstance(agy_exe, str) and agy_exe.strip() else None,
            cwd=workspace,
            default_timeout=timeout,
        )
        try:
            init_res = client.start(timeout=timeout)
        finally:
            client.close()
    except Exception as exc:
        return False, f"本地 agy acp 握手失败：{exc}"

    agent_info = init_res.get("agentInfo") if isinstance(init_res, dict) else None
    if isinstance(agent_info, dict):
        name = agent_info.get("name") or "agy"
        version = agent_info.get("version") or ""
        desc = f"{name} {version}".strip()
        return True, f"本地 agy acp 握手通过（{desc}）"
    return True, "本地 agy acp 握手通过"


def _read_prompt(
    prompt: str,
    *,
    input_fn: Callable[[str], str],
    default: str = "",
) -> str:
    try:
        raw = input_fn(prompt)
    except EOFError:
        return default
    cleaned = raw.strip()
    return cleaned if cleaned else default


def _prompt_domain(
    *,
    input_fn: Callable[[str], str],
    out: TextIO,
) -> str:
    out.write(
        "选择飞书平台站点：\n"
        f"  1) 飞书国内版 ({DEFAULT_FEISHU_DOMAIN}) [默认]\n"
        f"  2) Lark 国际版 ({LARK_GLOBAL_DOMAIN})\n"
        "  3) 直接输入自定义 HTTPS 域名\n"
    )
    while True:
        choice = _read_prompt("平台编号或域名 [1]: ", input_fn=input_fn, default="1")
        lowered = choice.lower()
        if lowered in {"1", "feishu", "cn"}:
            return DEFAULT_FEISHU_DOMAIN
        if lowered in {"2", "lark", "global", "intl"}:
            return LARK_GLOBAL_DOMAIN
        if choice.startswith(("https://", "http://")):
            return choice.rstrip("/")
        out.write("输入无效，请输入 1、2 或以 https:// 开头的完整域名。\n")


def _prompt_yes_no(
    question: str,
    *,
    default: bool,
    input_fn: Callable[[str], str],
    out: TextIO,
) -> bool:
    hint = "Y/n" if default else "y/N"
    while True:
        raw = _read_prompt(f"{question} [{hint}]: ", input_fn=input_fn, default="")
        if not raw:
            return default
        lowered = raw.lower()
        if lowered in {"y", "yes", "true", "1"}:
            return True
        if lowered in {"n", "no", "false", "0"}:
            return False
        out.write("请输入 y 或 n。\n")


def _read_secret_prompt(
    prompt: str,
    *,
    secret_fn: Callable[[str], str],
) -> str:
    try:
        raw = secret_fn(prompt)
    except EOFError:
        return ""
    return raw.strip()


def _resolve_relative_dir(raw_value: str, *, base_dir: Path) -> Path:
    expanded = Path(os.path.expanduser(raw_value.strip()))
    if expanded.is_absolute():
        return expanded
    return (base_dir / expanded).resolve()


def _gather_interactive_values(
    *,
    resolved_dir: Path,
    detected_agy: str | None,
    has_env_agy_exe: bool,
    input_fn: Callable[[str], str],
    secret_fn: Callable[[str], str],
    out: TextIO,
) -> InitValues:
    domain = _prompt_domain(input_fn=input_fn, out=out)
    app_id = _read_prompt(
        "FEISHU_APP_ID（如 cli_xxx，回车留空稍后填写）: ",
        input_fn=input_fn,
        default="",
    )
    app_secret = _read_secret_prompt(
        "FEISHU_APP_SECRET（已隐藏回显，回车留空稍后填写）: ",
        secret_fn=secret_fn,
    )
    if app_secret:
        out.write(f"  [已录入 FEISHU_APP_SECRET（已隐藏，长度 {len(app_secret)}）]\n")
    else:
        out.write("  [暂未录入 FEISHU_APP_SECRET，可在生成后的 .env 中补充]\n")

    out.write("安全提示：AGY_LARK_ALLOWED_OPEN_IDS 留空将依据 Fail-Closed 策略拒绝所有飞书消息。\n")
    allowed_open_ids = _read_prompt(
        "授权用户 open_id 列表（多个用英文逗号分隔，回车留空）: ",
        input_fn=input_fn,
        default="",
    )
    allow_group = _prompt_yes_no(
        "是否允许已授权用户在群聊中通过 @机器人 触发对话？",
        default=False,
        input_fn=input_fn,
        out=out,
    )

    workspace_raw = _read_prompt(
        f"默认工作区目录（相对 {resolved_dir} 解析）[./workspace]: ",
        input_fn=input_fn,
        default="./workspace",
    )
    data_dir_raw = _read_prompt(
        f"持久化数据目录（相对 {resolved_dir} 解析）[{DEFAULT_DATA_DIR}]: ",
        input_fn=input_fn,
        default=DEFAULT_DATA_DIR,
    )

    if detected_agy and not has_env_agy_exe:
        agy_prompt = "AGY_EXE 显式路径（已从 PATH 探测到，直接回车留空沿用自动发现）: "
    else:
        agy_prompt = "AGY_EXE 显式路径（未在 PATH 发现时可填完整路径，回车留空）: "
    agy_exe = _read_prompt(agy_prompt, input_fn=input_fn, default="")

    agy_model = _read_prompt(
        "默认模型 AGY_MODEL（如 gemini-3.8-flash，回车留空使用默认）: ",
        input_fn=input_fn,
        default="",
    )
    agy_effort = _read_prompt(
        "默认思考强度 AGY_EFFORT（low/medium/high/xhigh，回车留空使用默认）: ",
        input_fn=input_fn,
        default="",
    ).lower()

    return InitValues(
        feishu_app_id=app_id,
        feishu_app_secret=app_secret,
        feishu_domain=domain,
        allowed_open_ids=allowed_open_ids,
        allow_group_mention=allow_group,
        agy_exe=agy_exe,
        agy_model=agy_model,
        agy_effort=agy_effort,
        default_workspace=workspace_raw,
        data_dir=data_dir_raw,
    )


def _format_startup_command(
    *,
    env_path: Path,
    need_env_override: bool,
) -> str:
    cwd_env = (Path.cwd() / ".env").resolve()
    env_arg = ".env" if env_path.resolve() == cwd_env else f'"{env_path}"'
    parts = ["agylark", "--env-file", env_arg]
    if need_env_override:
        parts.append("--env-override")
    parts.append("-v")
    return " ".join(parts)


def run_init_wizard(
    *,
    target_dir: str | Path | None = None,
    non_interactive: bool = False,
    force: bool = False,
    verify: bool | None = None,
    environ: Mapping[str, str] | None = None,
    input_fn: Callable[[str], str] = input,
    secret_fn: Callable[[str], str] = getpass.getpass,
    out: TextIO | None = None,
    err: TextIO | None = None,
    verify_feishu_fn: Callable[[str, str, str], tuple[bool, str]] | None = None,
    verify_acp_fn: Callable[[str | None, Path], tuple[bool, str]] | None = None,
) -> int:
    """运行 `agylark init` 向导，创建规范目录、`.env` 并执行可选自检。"""
    stdout = sys.stdout if out is None else out
    stderr = sys.stderr if err is None else err
    env = os.environ if environ is None else environ
    feishu_checker = verify_feishu_credentials if verify_feishu_fn is None else verify_feishu_fn
    acp_checker = verify_acp_handshake if verify_acp_fn is None else verify_acp_fn

    if target_dir is None:
        raw_dir = "."
        if not non_interactive:
            raw_dir = _read_prompt(
                f"运行目录（将创建 .env、workspace/ 与 {DEFAULT_DATA_DIR}/）[.]: ",
                input_fn=input_fn,
                default=".",
            )
    else:
        raw_dir = str(target_dir)

    resolved_dir = Path(os.path.expanduser(raw_dir)).resolve()
    env_path = resolved_dir / ".env"

    if env_path.exists() and not force:
        stderr.write(
            f"错误：目标目录已存在 {env_path}，为保护现有配置已中止初始化（如需覆盖请显式传入 --force）。\n"
        )
        return 1

    stdout.write(f"=== agylark 初始化向导（目标目录：{resolved_dir}）===\n")

    detected_agy, agy_warning = detect_agy_executable(env)
    if detected_agy:
        stdout.write(f"[OK] 已探测到 agy 可执行文件：{detected_agy}\n")
    else:
        stdout.write(f"[WARN] 暂未探测到 agy 可执行文件：{agy_warning}\n")

    residual_keys = detect_residual_env_vars(env)
    if residual_keys:
        stdout.write(
            f"[WARN] 检测到当前终端/宿主环境已存在冲突变量：{', '.join(residual_keys)}\n"
            "       默认情况下外部环境变量优先于 .env；建议清理上述变量，或启动时加入 --env-override。\n"
        )

    if non_interactive:
        values = InitValues()
    else:
        values = _gather_interactive_values(
            resolved_dir=resolved_dir,
            detected_agy=detected_agy,
            has_env_agy_exe=bool(env.get("AGY_EXE", "").strip()),
            input_fn=input_fn,
            secret_fn=secret_fn,
            out=stdout,
        )

    workspace_dir = _resolve_relative_dir(values.default_workspace, base_dir=resolved_dir)
    data_dir = _resolve_relative_dir(values.data_dir, base_dir=resolved_dir)
    resolved_dir.mkdir(parents=True, exist_ok=True)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    env_content = format_env_file_content(values)
    env_path.write_text(env_content, encoding="utf-8")
    if os.name != "nt":
        try:
            os.chmod(env_path, 0o600)
        except OSError:
            pass

    stdout.write(f"\n[OK] 已写入配置文件：{env_path}\n")
    stdout.write(f"[OK] 已准备目录：workspace={workspace_dir}，data_dir={data_dir}\n")

    if verify is True:
        should_verify = True
    elif verify is False or non_interactive:
        should_verify = False
    else:
        should_verify = _prompt_yes_no(
            "是否立即执行可选自检（飞书凭据连通性 + 本地 agy acp 握手）？",
            default=True,
            input_fn=input_fn,
            out=stdout,
        )

    if should_verify:
        stdout.write("\n--- 执行可选自检 ---\n")
        if values.feishu_app_id and values.feishu_app_secret:
            ok_feishu, msg_feishu = feishu_checker(
                values.feishu_app_id,
                values.feishu_app_secret,
                values.feishu_domain,
            )
            tag = "[OK]" if ok_feishu else "[WARN]"
            stdout.write(f"{tag} {msg_feishu}\n")
        else:
            stdout.write("[SKIP] 未填写 FEISHU_APP_ID 或 FEISHU_APP_SECRET，跳过飞书凭据校验。\n")

        exe_for_check = values.agy_exe.strip() or detected_agy
        if exe_for_check:
            ok_acp, msg_acp = acp_checker(exe_for_check, workspace_dir)
            tag = "[OK]" if ok_acp else "[WARN]"
            stdout.write(f"{tag} {msg_acp}\n")
        else:
            stdout.write("[SKIP] 尚未定位到可用 agy 可执行文件，跳过本地 agy acp 握手自检。\n")

    startup_cmd = _format_startup_command(
        env_path=env_path,
        need_env_override=bool(residual_keys),
    )
    masked_secret = (
        f"<已录入，已隐藏 (长度 {len(values.feishu_app_secret)})>"
        if values.feishu_app_secret
        else "<待填写>"
    )
    stdout.write(
        "\n--- 初始化摘要 ---\n"
        f"- 站点域名：{values.feishu_domain}\n"
        f"- FEISHU_APP_ID：{values.feishu_app_id or '<待填写>'}\n"
        f"- FEISHU_APP_SECRET：{masked_secret}\n"
        f"- 允许 open_id：{values.allowed_open_ids or '<留空：默认拒绝所有消息>'}\n"
        "\n--- 飞书开放平台后台检查清单 ---\n"
        "1. 在应用「添加应用能力」中启用【机器人】能力\n"
        "2. 在「事件与回调 -> 事件配置」订阅方式中选择【使用长连接接收事件】\n"
        "3. 添加事件订阅 `im.message.receive_v1`，并在回调配置添加 `card.action.trigger`\n"
        "4. 在「权限管理」开通获取/发送消息及 CardKit 交互卡片权限，并发布应用新版本\n"
        "5. 确认 `.env` 中已填入 FEISHU_APP_ID、FEISHU_APP_SECRET 与授权用户 `open_id`（勿提交 `.env` 到 Git）\n"
        "\n--- 启动命令（可直接复制） ---\n"
        f"{startup_cmd}\n"
    )
    return 0
