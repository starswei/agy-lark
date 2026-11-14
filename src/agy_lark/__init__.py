"""agy-lark 轻量级飞书 ↔ Antigravity (agy acp --stdio) 桥接器包。"""

from .acp_client import (
    AcpError,
    AcpPromptResult,
    AcpSessionUpdate,
    AgyAcpClient,
)
from .config import (
    AgyLarkConfig,
    is_authorized_open_id,
    load_config,
    load_dotenv_file,
    parse_allowed_open_ids,
    resolve_agy_executable,
)

__version__ = "0.1.0a1"

__all__ = [
    "AcpError",
    "AcpPromptResult",
    "AcpSessionUpdate",
    "AgyAcpClient",
    "AgyLarkConfig",
    "__version__",
    "is_authorized_open_id",
    "load_config",
    "load_dotenv_file",
    "parse_allowed_open_ids",
    "resolve_agy_executable",
]
