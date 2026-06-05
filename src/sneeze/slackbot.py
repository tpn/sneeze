from __future__ import annotations

import hashlib
import json
import os
import plistlib
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .console import (
    DEFAULT_CONSOLE_ROOT_PATH,
    DEFAULT_TMUX_SOCKET,
    normalize_root_path,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None


class SlackbotError(RuntimeError):
    pass


class SlackApiError(SlackbotError):
    def __init__(self, method: str, result: dict[str, Any]):
        self.method = method
        self.result = result
        self.error = str(result.get("error") or result)
        super().__init__(f"Slack API {method} failed: {self.error}")


SLACK_BOT_TOKEN_ENV = "SLACK_BOT_TOKEN"
SLACK_APP_TOKEN_ENV = "SLACK_APP_TOKEN"
SLACK_TEAM_DOMAIN_ENV = "SLACK_TEAM_DOMAIN"
SLACK_APP_ID_ENV = "SLACK_APP_ID"
SLACK_CLIENT_ID_ENV = "SLACK_CLIENT_ID"

CODEX_MODE_VALUES = (
    "danger-full-access",
    "workspace-write",
    "read-only",
)
EXECUTION_MODE_VALUES = ("raw", "tmux")
NOTIFY_KIND_VALUES = ("none", "slack_message", "codex_prompt")
MAX_SLACK_TEXT_CHARS = 3500
RECENT_EVENT_TTL_SECONDS = 300.0
MAX_CONVERSATION_LOCKS = 1024
INGRESS_PROCESSING_STALE_SECONDS = 3600.0
AGENT_TMUX_LOCK = threading.Lock()
CHILD_ENV_ALIASES = ("MCP_SERVER_URL", "TEAM_CONFIG_PATH")
CHILD_ENV_PASSTHROUGH = {
    "CODEX_HOME",
    "GIT_SSH_COMMAND",
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LOGNAME",
    "PATH",
    "SHELL",
    "SSH_AUTH_SOCK",
    "TERM",
    "TMPDIR",
    "USER",
}
CODEX_SESSION_EVENT_TYPES = ("session_meta", "session_created")
WORK_QUEUE_DEPTH_PER_WORKER = 4
USER_CONFIG_DISPATCH_LIMIT = 4
USER_CONFIG_MODAL_CALLBACK_ID = "sneeze_user_config"
USER_CONFIG_OPEN_ACTION_ID = "sneeze_open_user_config"
USER_CONFIG_CODEX_BLOCK_ID = "codex_enabled"
USER_CONFIG_CODEX_ACTION_ID = "codex_enabled"
USER_CONFIG_CODEX_OPTION_VALUE = "enabled"
USER_CONFIG_TRIGGER_WORDS = {
    "bootstrap",
    "configure",
    "config",
    "onboard",
    "setup",
}
SLACKBOT_CODEX_TRANSPORT_INSTRUCTIONS = """Slack transport:
- You are running inside a Slackbot transport. The Slackbot wrapper will post
  your final answer back to the Slack route shown below.
- Do not call Slack send, draft, schedule, canvas, or message-write tools for
  the current route. Return the exact reply text as your final answer.
- Use Slack read/search tools only when the user's request explicitly needs
  Slack context that was not included in this prompt.
"""
SECRET_ENV_NAME_PARTS = {
    "APIKEY",
    "AUTHORIZATION",
    "CREDENTIAL",
    "CREDENTIALS",
    "KEY",
    "KEYS",
    "OAUTH",
    "PASS",
    "PASSWORD",
    "PRIVATE",
    "SECRET",
    "SIGNATURE",
    "SIGNING",
    "TOKEN",
    "TOKENS",
}
SECRET_ENV_AUTH_PARTS = {
    "AUTH",
    "AUTHENTICATE",
    "BEARER",
    "OAUTH",
}
SECRET_ENV_AUTH_CONTEXT_PARTS = {
    "API",
    "BASIC",
    "CONFIG",
    "CREDENTIAL",
    "CREDENTIALS",
    "DIGEST",
    "FILE",
    "HEADER",
    "JSON",
    "KEY",
    "KEYS",
    "PRIVATE",
    "PROXY",
    "SECRET",
    "TOKEN",
    "TOKENS",
    "WWW",
}
SECRET_ENV_NAME_EXCEPTIONS = {
    "SSH_AUTH_SOCK",
}
SECRET_ENV_NAME_SUBSTRINGS = {
    "PASSWORD",
    "SECRET",
    "TOKEN",
}
USER_CONFIG_FIELDS = {
    "display_name": ("display_name", "display_name"),
    "default_project": ("default_project", "default_project"),
    "notes": ("notes", "notes"),
}


@dataclass(frozen=True)
class SlackbotUserConfigSecretField:
    service_name: str
    label: str
    env_names: tuple[str, ...]
    block_id: str | None = None
    action_id: str | None = None
    placeholder_name: str | None = None
    scrub_env_names: tuple[str, ...] = ()
    required: bool = False


@dataclass(frozen=True)
class SlackbotStaticResponse:
    names: tuple[str, ...]
    text: str


CORE_USER_CONFIG_SECRET_FIELDS: tuple[SlackbotUserConfigSecretField, ...] = ()


@dataclass(frozen=True)
class SlackbotProfile:
    app_slug: str
    env_prefix: str
    default_bot_name: str
    default_command_name: str
    default_runtime_root: str
    default_codex_workdir: str
    default_system_prompt: str
    default_env_path: str | None = None
    default_unit_name: str | None = None
    default_worker_count: int = 2
    default_codex_bin: str = "codex"
    default_codex_mode: str = "workspace-write"
    default_codex_model: str | None = None
    default_codex_profile: str | None = None
    default_codex_extra_args: tuple[str, ...] = ()
    default_mcp_server_url: str | None = None
    default_team_config_path: str | None = None
    default_state_dir: str | None = None
    default_system_prompt_path: str | None = None
    user_config_secret_fields: tuple[SlackbotUserConfigSecretField, ...] = ()
    child_env_scrub_allowlist: tuple[str, ...] = ()
    static_responses: tuple[SlackbotStaticResponse, ...] = ()
    default_allowed_dm_user_ids: tuple[str, ...] = ()
    default_allowed_user_ids: tuple[str, ...] = ()
    default_allowed_channel_ids: tuple[str, ...] = ()

    @property
    def normalized_env_prefix(self) -> str:
        return self.env_prefix.strip().upper().replace("-", "_")

    def env_name(self, suffix: str) -> str:
        return f"{self.normalized_env_prefix}_{suffix}"

    @property
    def unit_name(self) -> str:
        return self.default_unit_name or f"{self.app_slug}-slackbot.service"


@dataclass(frozen=True)
class SlackbotPaths:
    runtime_root: str
    env_path: str
    state_dir: str
    ingress_dir: str
    ingress_processing_dir: str
    ingress_done_dir: str
    ingress_error_dir: str
    schedule_dir: str
    schedule_reports_dir: str
    agents_dir: str
    user_config_dir: str
    systemd_dir: str
    launchd_dir: str
    system_prompt_path: str
    sessions_path: str
    agent_tmux_bindings_path: str
    agent_tmux_jobs_path: str
    log_path: str
    service_manager: str
    unit_name: str
    unit_label: str
    unit_path: str


@dataclass(frozen=True)
class SlackbotConfig:
    profile: SlackbotProfile
    paths: SlackbotPaths
    slack_domain: str | None
    app_id: str | None
    client_id: str | None
    bot_token: str | None = field(repr=False)
    app_token: str | None = field(repr=False)
    env: dict[str, str] = field(default_factory=dict, repr=False)
    bot_name: str = ""
    command_name: str = ""
    codex_bin: str = "codex"
    codex_mode: str = "danger-full-access"
    codex_model: str | None = None
    codex_profile: str | None = None
    codex_workdir: str = ""
    codex_extra_args: tuple[str, ...] = ()
    allowed_dm_user_ids: tuple[str, ...] = ()
    allowed_user_ids: tuple[str, ...] = ()
    allowed_channel_ids: tuple[str, ...] = ()
    worker_count: int = 2
    mcp_server_url: str | None = None
    team_config_path: str | None = None


def emit(out: Callable[[str], None] | None, message: str) -> None:
    if out is not None:
        out(message)


@dataclass(frozen=True)
class SlackbotRoute:
    channel_id: str | None = None
    dm_user_id: str | None = None
    mention_user_ids: tuple[str, ...] = ()
    thread_ts: str | None = None
    response_url: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class SlackbotIngressPayload:
    kind: str
    text: str
    route: SlackbotRoute
    execution_mode: str = "raw"
    project: str | None = None
    session: str | None = None
    slack_user_id: str | None = None
    system_scoped: bool | None = None
    created_at: str = ""


@dataclass(frozen=True)
class ScheduleDefinition:
    name: str
    on_calendar: str
    command: tuple[str, ...]
    workdir: str
    notify_kind: str
    route: SlackbotRoute
    execution_mode: str = "raw"
    project: str | None = None
    session: str | None = None
    persistent: bool = True


@dataclass
class ConversationLockEntry:
    lock: threading.Lock = field(default_factory=threading.Lock)
    refs: int = 0


def utcnow_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def service_manager_kind() -> str:
    return "launchd" if sys.platform == "darwin" else "systemd"


def systemd_user_unit_dir() -> Path:
    return Path.home() / ".config" / "systemd" / "user"


def launchd_agent_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def expand_path(path: str | None) -> Path | None:
    if not path:
        return None
    return Path(path).expanduser().resolve()


def require_expanded_path(path: str | None, description: str) -> str:
    expanded = expand_path(path)
    if expanded is None:
        raise SlackbotError(f"{description} is required")
    return str(expanded)


def normalize_command_name(value: str | None, default: str) -> str:
    text = (value or default).strip()
    if not text:
        text = default
    if not text.startswith("/"):
        text = f"/{text}"
    return text


def normalize_codex_mode(value: str | None) -> str:
    mode = (value or "danger-full-access").strip().lower()
    if mode not in CODEX_MODE_VALUES:
        raise ValueError(
            "Unsupported Codex mode. Expected one of: "
            f"{', '.join(CODEX_MODE_VALUES)}"
        )
    return mode


def normalize_execution_mode(value: str | None) -> str:
    mode = (value or "raw").strip().lower()
    if mode not in EXECUTION_MODE_VALUES:
        raise ValueError(
            "Unsupported execution mode. Expected one of: "
            f"{', '.join(EXECUTION_MODE_VALUES)}"
        )
    return mode


def normalize_notify_kind(value: str | None) -> str:
    kind = (value or "none").strip().lower()
    if kind not in NOTIFY_KIND_VALUES:
        raise ValueError(
            "Unsupported notify kind. Expected one of: "
            f"{', '.join(NOTIFY_KIND_VALUES)}"
        )
    return kind


def normalize_positive_int(value: str | int | None, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Expected a positive integer, got: {value}"
        ) from exc
    if number <= 0:
        raise ValueError(f"Expected a positive integer, got: {value}")
    return number


def parse_csv(value: str | Iterable[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(
            item.strip() for item in value.split(",") if item.strip()
        )
    return tuple(str(item).strip() for item in value if str(item).strip())


def render_csv(value: Iterable[str] | None) -> str:
    return ",".join(parse_csv(value))


def parse_extra_args(value: str | Iterable[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(shlex.split(value)) if value.strip() else ()
    return tuple(str(item) for item in value)


def quote_env_value(value: str) -> str:
    if not value:
        return ""
    safe = all(ch.isalnum() or ch in "_./:@+-" for ch in value)
    if safe:
        return value
    return "'" + value.replace("'", "'\\''") + "'"


def parse_env_value(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text[1:-1]
        return parsed if isinstance(parsed, str) else text[1:-1]
    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("'\\''", "'")
    return text


def quote_systemd_value(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("$", "$$")
        .replace("\n", "\\n")
    )
    return f'"{escaped}"'


def systemd_path_value(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\x20")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
    )


def read_env_file(path: str | Path) -> OrderedDict[str, str]:
    env: OrderedDict[str, str] = OrderedDict()
    env_path = Path(path).expanduser()
    if not env_path.exists():
        return env
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = raw_line.partition("=")
        if sep and key.strip():
            env[key.strip()] = parse_env_value(value)
    return env


def render_env_file(values: OrderedDict[str, str], manager: str) -> str:
    lines = [f"# Managed by {manager}", ""]
    for key, value in values.items():
        lines.append(f"{key}={quote_env_value(value)}")
    return "\n".join(lines) + "\n"


def write_text(path: str | Path, text: str, mode: int | None = None) -> None:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    if mode is None:
        target.write_text(text, encoding="utf-8", newline="\n")
        return
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
    target.chmod(mode)


def write_json(
    path: str | Path,
    value: Any,
    mode: int | None = 0o600,
) -> None:
    write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n", mode)


def write_json_atomic(
    path: str | Path,
    value: Any,
    mode: int = 0o600,
) -> None:
    target = Path(path).expanduser()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp_path = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    write_json(tmp_path, value, mode=mode)
    os.replace(tmp_path, target)
    target.chmod(mode)


def child_process_env(config: SlackbotConfig) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key in CHILD_ENV_PASSTHROUGH or key.startswith("LC_")
    }
    for key in CHILD_ENV_ALIASES:
        prefixed = config.profile.env_name(key)
        resolved = (
            config.mcp_server_url
            if key == "MCP_SERVER_URL"
            else config.team_config_path
        )
        configured = (
            resolved
            or os.environ.get(prefixed)
            or os.environ.get(key)
            or config.env.get(prefixed)
            or config.env.get(key)
        )
        if configured:
            env[key] = configured
            env[prefixed] = configured
    return env


def is_secret_env_name(env_name: str) -> bool:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", env_name).upper()
    if normalized in SECRET_ENV_NAME_EXCEPTIONS:
        return False
    parts = {part for part in re.split(r"[^A-Za-z0-9]+", normalized) if part}
    if normalized == "AUTH":
        return True
    if len(parts) == 1 and any(
        normalized.endswith(marker) for marker in SECRET_ENV_NAME_SUBSTRINGS
    ):
        return True
    if parts & SECRET_ENV_NAME_PARTS:
        return True
    auth_parts = parts & SECRET_ENV_AUTH_PARTS
    return bool(
        len(auth_parts) > 1
        or (auth_parts and (parts & SECRET_ENV_AUTH_CONTEXT_PARTS))
    )


def child_env_scrub_allowlist(profile: SlackbotProfile) -> tuple[str, ...]:
    env_names = tuple(
        OrderedDict.fromkeys(
            env_name.strip()
            for env_name in profile.child_env_scrub_allowlist
            if env_name.strip()
        )
    )
    for env_name in env_names:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
            raise SlackbotError(
                "Unsupported child env scrub allowlist variable: "
                f"{env_name!r}"
            )
    return env_names


def merge_child_process_env(
    config: SlackbotConfig,
    extra_env: dict[str, str] | None = None,
    *,
    scrub_secret_env: bool = False,
    use_scrub_allowlist: bool = False,
) -> dict[str, str]:
    env = child_process_env(config)
    if scrub_secret_env:
        scrub_allowlist = (
            set(child_env_scrub_allowlist(config.profile))
            if use_scrub_allowlist
            else set()
        )
        for env_name in tuple(env):
            if env_name not in scrub_allowlist and is_secret_env_name(
                env_name
            ):
                env.pop(env_name, None)
        for secret_field in user_config_secret_fields(config.profile):
            for env_name in (
                *secret_field.env_names,
                *secret_field.scrub_env_names,
            ):
                env.pop(env_name, None)
    for key, value in (extra_env or {}).items():
        if key and value:
            env[key] = value
    return env


def read_json(path: str | Path, default: Any) -> Any:
    target = Path(path).expanduser()
    if not target.exists():
        return default
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


@contextmanager
def locked_json_path(path: str | Path):
    target = Path(path).expanduser()
    lock_path = target.with_suffix(target.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as handle:
        if fcntl is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def default_executable_path(name: str) -> str:
    resolved = shutil.which(name)
    if resolved:
        return resolved
    candidate = Path(sys.executable).resolve().with_name(name)
    return str(candidate) if candidate.exists() else name


def launchd_label_from_unit_name(unit_name: str) -> str:
    label = unit_name
    if label.endswith(".service"):
        label = label[: -len(".service")]
    return label.replace("/", ".")


def schedule_unit_name(profile: SlackbotProfile, name: str) -> str:
    return f"{profile.app_slug}-schedule-{safe_schedule_name(name)}.service"


def safe_schedule_name(name: str) -> str:
    if not name or not all(ch.isalnum() or ch in "-_" for ch in name):
        raise SlackbotError(
            "Schedule names may only contain letters, numbers, '-' and '_'"
        )
    return name


def resolve_paths(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> SlackbotPaths:
    root = expand_path(runtime_root or profile.default_runtime_root)
    if root is None:
        raise SlackbotError("runtime root is required")
    state = (
        expand_path(state_dir or profile.default_state_dir) or root / "var"
    )
    service_manager = service_manager_kind()
    resolved_unit_name = unit_name or profile.unit_name
    systemd_dir = root / "systemd"
    launchd_dir = root / "launchd"
    if service_manager == "launchd":
        label = launchd_label_from_unit_name(resolved_unit_name)
        unit_path = launchd_dir / f"{label}.plist"
    else:
        unit_path = systemd_dir / resolved_unit_name
    prompt_path = (
        expand_path(system_prompt_path or profile.default_system_prompt_path)
        or root / "agents" / f"{profile.app_slug}.md"
    )
    env = expand_path(env_path or profile.default_env_path) or root / ".env"
    return SlackbotPaths(
        runtime_root=str(root),
        env_path=str(env),
        state_dir=str(state),
        ingress_dir=str(state / "ingress"),
        ingress_processing_dir=str(state / "ingress-processing"),
        ingress_done_dir=str(state / "ingress-done"),
        ingress_error_dir=str(state / "ingress-error"),
        schedule_dir=str(state / "schedules"),
        schedule_reports_dir=str(state / "schedule-runs"),
        agents_dir=str(prompt_path.parent),
        user_config_dir=str(state / "users"),
        systemd_dir=str(systemd_dir),
        launchd_dir=str(launchd_dir),
        system_prompt_path=str(prompt_path),
        sessions_path=str(state / "sessions.json"),
        agent_tmux_bindings_path=str(state / "agent-tmux-bindings.json"),
        agent_tmux_jobs_path=str(state / "agent-tmux-jobs.json"),
        log_path=str(state / "slackbot.log"),
        service_manager=service_manager,
        unit_name=resolved_unit_name,
        unit_label=launchd_label_from_unit_name(resolved_unit_name),
        unit_path=str(unit_path),
    )


def prefixed_env_keys(profile: SlackbotProfile) -> OrderedDict[str, str]:
    keys: OrderedDict[str, str] = OrderedDict()
    for key in (
        SLACK_TEAM_DOMAIN_ENV,
        SLACK_APP_ID_ENV,
        SLACK_CLIENT_ID_ENV,
        SLACK_BOT_TOKEN_ENV,
        SLACK_APP_TOKEN_ENV,
        "SLACK_SIGNING_SECRET",
        "SLACK_USER_TOKEN",
        "SLACKBOT_BOT_NAME",
        "SLACKBOT_COMMAND_NAME",
        "SLACKBOT_CODEX_BIN",
        "SLACKBOT_CODEX_MODE",
        "SLACKBOT_CODEX_WORKDIR",
        "SLACKBOT_CODEX_MODEL",
        "SLACKBOT_CODEX_PROFILE",
        "SLACKBOT_CODEX_EXTRA_ARGS",
        "SLACKBOT_EXECUTION_MODE",
        "SLACKBOT_ALLOWED_DM_USER_IDS",
        "SLACKBOT_ALLOWED_USER_IDS",
        "SLACKBOT_ALLOWED_CHANNEL_IDS",
        "SLACKBOT_SYSTEM_PROMPT_PATH",
        "SLACKBOT_WORKER_COUNT",
        "MCP_SERVER_URL",
        "TEAM_CONFIG_PATH",
        "CONSOLE_HOST",
        "CONSOLE_PORT",
        "CONSOLE_ROOT_PATH",
        "CONSOLE_PUBLIC_URL",
        "CONSOLE_PUBLIC_BASE_URL",
        "CONSOLE_TMUX_BIN",
        "CONSOLE_TMUX_SOCKET",
        "CONSOLE_AUTH_EMAIL_HEADER",
        "CONSOLE_AUTH_EMAIL_DOMAIN",
        "CONSOLE_ADMIN_EMAILS",
    ):
        keys[key] = profile.env_name(key)
    return keys


def env_lookup(
    profile: SlackbotProfile,
    env_values: dict[str, str],
    key: str,
    default: str | None = None,
) -> str | None:
    prefixed = profile.env_name(key)
    for candidate in (prefixed, key):
        value = os.environ.get(candidate)
        if value:
            return value
        value = env_values.get(candidate)
        if value:
            return value
    return default


def env_lookup_allow_empty(
    profile: SlackbotProfile,
    env_values: dict[str, str],
    key: str,
    default: str | None = None,
) -> str | None:
    prefixed = profile.env_name(key)
    for candidate in (prefixed, key):
        if candidate in os.environ:
            return os.environ[candidate]
        if candidate in env_values:
            return env_values[candidate]
    return default


def managed_env_values(
    profile: SlackbotProfile,
    current: OrderedDict[str, str],
    *,
    paths: SlackbotPaths,
    slack_domain: str | None = None,
    app_id: str | None = None,
    client_id: str | None = None,
    bot_token: str | None = None,
    app_token: str | None = None,
    bot_name: str | None = None,
    command_name: str | None = None,
    codex_bin: str | None = None,
    codex_mode: str | None = None,
    codex_model: str | None = None,
    codex_profile: str | None = None,
    codex_workdir: str | None = None,
    codex_extra_args: str | None = None,
    worker_count: int | None = None,
    mcp_server_url: str | None = None,
    team_config_path: str | None = None,
) -> OrderedDict[str, str]:
    keys = prefixed_env_keys(profile)

    def current_value(
        key: str,
        default: str | None = None,
        *,
        allow_empty: bool = False,
    ) -> str:
        for candidate in (keys[key], key):
            if candidate in current:
                value = current[candidate]
                if allow_empty or value:
                    return value
        return default or ""

    values: OrderedDict[str, str] = OrderedDict()
    values[keys[SLACK_TEAM_DOMAIN_ENV]] = (
        slack_domain or current_value(SLACK_TEAM_DOMAIN_ENV)
    ).strip()
    values[keys[SLACK_APP_ID_ENV]] = (
        app_id or current_value(SLACK_APP_ID_ENV)
    ).strip()
    values[keys[SLACK_CLIENT_ID_ENV]] = (
        client_id or current_value(SLACK_CLIENT_ID_ENV)
    ).strip()
    values[keys[SLACK_BOT_TOKEN_ENV]] = (
        bot_token or current_value(SLACK_BOT_TOKEN_ENV)
    ).strip()
    values[keys[SLACK_APP_TOKEN_ENV]] = (
        app_token or current_value(SLACK_APP_TOKEN_ENV)
    ).strip()
    values[keys["SLACKBOT_BOT_NAME"]] = (
        bot_name
        or current_value("SLACKBOT_BOT_NAME", profile.default_bot_name)
    ).strip()
    values[keys["SLACKBOT_COMMAND_NAME"]] = normalize_command_name(
        command_name
        or current_value(
            "SLACKBOT_COMMAND_NAME",
            profile.default_command_name,
        ),
        profile.default_command_name,
    )
    values[keys["SLACKBOT_CODEX_BIN"]] = (
        codex_bin
        or current_value(
            "SLACKBOT_CODEX_BIN",
            default_executable_path(profile.default_codex_bin),
        )
    ).strip()
    values[keys["SLACKBOT_CODEX_MODE"]] = normalize_codex_mode(
        codex_mode
        or current_value("SLACKBOT_CODEX_MODE", profile.default_codex_mode)
    )
    values[keys["SLACKBOT_CODEX_WORKDIR"]] = require_expanded_path(
        codex_workdir
        or current_value(
            "SLACKBOT_CODEX_WORKDIR",
            profile.default_codex_workdir,
        ),
        "Codex workdir",
    )
    values[keys["SLACKBOT_CODEX_MODEL"]] = (
        codex_model
        or current_value("SLACKBOT_CODEX_MODEL", profile.default_codex_model)
    ).strip()
    values[keys["SLACKBOT_CODEX_PROFILE"]] = (
        codex_profile
        or current_value(
            "SLACKBOT_CODEX_PROFILE",
            profile.default_codex_profile,
        )
    ).strip()
    values[keys["SLACKBOT_CODEX_EXTRA_ARGS"]] = (
        codex_extra_args
        or current_value(
            "SLACKBOT_CODEX_EXTRA_ARGS",
            " ".join(profile.default_codex_extra_args),
        )
    ).strip()
    values[keys["SLACKBOT_EXECUTION_MODE"]] = normalize_execution_mode(
        current_value("SLACKBOT_EXECUTION_MODE", "raw")
    )
    values[keys["SLACKBOT_ALLOWED_DM_USER_IDS"]] = current_value(
        "SLACKBOT_ALLOWED_DM_USER_IDS",
        render_csv(profile.default_allowed_dm_user_ids),
        allow_empty=True,
    ).strip()
    values[keys["SLACKBOT_ALLOWED_USER_IDS"]] = current_value(
        "SLACKBOT_ALLOWED_USER_IDS",
        render_csv(profile.default_allowed_user_ids),
        allow_empty=True,
    ).strip()
    values[keys["SLACKBOT_ALLOWED_CHANNEL_IDS"]] = current_value(
        "SLACKBOT_ALLOWED_CHANNEL_IDS",
        render_csv(profile.default_allowed_channel_ids),
        allow_empty=True,
    ).strip()
    values[keys["SLACKBOT_SYSTEM_PROMPT_PATH"]] = paths.system_prompt_path
    values[keys["SLACKBOT_WORKER_COUNT"]] = str(
        normalize_positive_int(
            worker_count or current_value("SLACKBOT_WORKER_COUNT"),
            profile.default_worker_count,
        )
    )
    values[keys["MCP_SERVER_URL"]] = (
        mcp_server_url
        or current_value("MCP_SERVER_URL", profile.default_mcp_server_url)
    ).strip()
    values[keys["TEAM_CONFIG_PATH"]] = (
        team_config_path
        or current_value("TEAM_CONFIG_PATH", profile.default_team_config_path)
    ).strip()
    for key in (
        "CONSOLE_HOST",
        "CONSOLE_PORT",
        "CONSOLE_ROOT_PATH",
        "CONSOLE_PUBLIC_URL",
        "CONSOLE_PUBLIC_BASE_URL",
        "CONSOLE_TMUX_BIN",
        "CONSOLE_TMUX_SOCKET",
        "CONSOLE_AUTH_EMAIL_HEADER",
        "CONSOLE_AUTH_EMAIL_DOMAIN",
        "CONSOLE_ADMIN_EMAILS",
    ):
        values[keys[key]] = current_value(key, allow_empty=True).strip()
    for key, value in current.items():
        values.setdefault(key, value)
    return values


def scaffold_runtime(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
    slack_domain: str | None = None,
    app_id: str | None = None,
    client_id: str | None = None,
    bot_token: str | None = None,
    app_token: str | None = None,
    bot_name: str | None = None,
    command_name: str | None = None,
    codex_bin: str | None = None,
    codex_mode: str | None = None,
    codex_model: str | None = None,
    codex_profile: str | None = None,
    codex_workdir: str | None = None,
    codex_extra_args: str | None = None,
    worker_count: int | None = None,
    mcp_server_url: str | None = None,
    team_config_path: str | None = None,
    out: Callable[[str], None] | None = None,
) -> dict[str, str]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    current = read_env_file(paths.env_path)
    if system_prompt_path is None:
        existing_prompt_path = env_lookup(
            profile, current, "SLACKBOT_SYSTEM_PROMPT_PATH"
        )
        if existing_prompt_path:
            paths = resolve_paths(
                profile,
                runtime_root=runtime_root,
                env_path=env_path,
                state_dir=state_dir,
                system_prompt_path=existing_prompt_path,
                unit_name=unit_name,
            )
    values = managed_env_values(
        profile,
        current,
        paths=paths,
        slack_domain=slack_domain,
        app_id=app_id,
        client_id=client_id,
        bot_token=bot_token,
        app_token=app_token,
        bot_name=bot_name,
        command_name=command_name,
        codex_bin=codex_bin,
        codex_mode=codex_mode,
        codex_model=codex_model,
        codex_profile=codex_profile,
        codex_workdir=codex_workdir,
        codex_extra_args=codex_extra_args,
        worker_count=worker_count,
        mcp_server_url=mcp_server_url,
        team_config_path=team_config_path,
    )
    env_parent = Path(paths.env_path).parent
    env_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    write_text(
        paths.env_path,
        render_env_file(values, f"{profile.app_slug} slackbot-init"),
        mode=0o600,
    )
    if out:
        out(f"Wrote Slackbot env file at {paths.env_path}")

    prompt_path = Path(paths.system_prompt_path)
    if not prompt_path.exists():
        write_text(prompt_path, profile.default_system_prompt)
        if out:
            out(f"Created system prompt at {paths.system_prompt_path}")

    for dirname in (
        paths.state_dir,
        paths.ingress_dir,
        paths.ingress_processing_dir,
        paths.ingress_done_dir,
        paths.ingress_error_dir,
        paths.schedule_dir,
        paths.schedule_reports_dir,
        paths.agents_dir,
        paths.user_config_dir,
        paths.systemd_dir,
        paths.launchd_dir,
    ):
        path = Path(dirname)
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.chmod(0o700)
    return {
        "runtime_root": paths.runtime_root,
        "env_path": paths.env_path,
        "state_dir": paths.state_dir,
        "ingress_dir": paths.ingress_dir,
        "schedule_dir": paths.schedule_dir,
        "agents_dir": paths.agents_dir,
        "user_config_dir": paths.user_config_dir,
        "system_prompt_path": paths.system_prompt_path,
        "unit_path": paths.unit_path,
    }


def load_config(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
    allow_missing_tokens: bool = False,
) -> SlackbotConfig:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    env = read_env_file(paths.env_path)
    if system_prompt_path is None:
        env_system_prompt_path = env_lookup(
            profile, env, "SLACKBOT_SYSTEM_PROMPT_PATH"
        )
        if env_system_prompt_path:
            paths = resolve_paths(
                profile,
                runtime_root=runtime_root,
                env_path=env_path,
                state_dir=state_dir,
                system_prompt_path=env_system_prompt_path,
                unit_name=unit_name,
            )
    bot_token = env_lookup(profile, env, SLACK_BOT_TOKEN_ENV)
    app_token = env_lookup(profile, env, SLACK_APP_TOKEN_ENV)
    if not allow_missing_tokens:
        missing = []
        if not bot_token:
            missing.append(profile.env_name(SLACK_BOT_TOKEN_ENV))
        if not app_token:
            missing.append(profile.env_name(SLACK_APP_TOKEN_ENV))
        if missing:
            raise SlackbotError(
                "Missing required Slackbot configuration: "
                f"{', '.join(missing)}. Populate {paths.env_path} first."
            )
    codex_workdir = require_expanded_path(
        env_lookup(
            profile,
            env,
            "SLACKBOT_CODEX_WORKDIR",
            profile.default_codex_workdir,
        ),
        "Codex workdir",
    )
    return SlackbotConfig(
        profile=profile,
        paths=paths,
        slack_domain=env_lookup(profile, env, SLACK_TEAM_DOMAIN_ENV),
        app_id=env_lookup(profile, env, SLACK_APP_ID_ENV),
        client_id=env_lookup(profile, env, SLACK_CLIENT_ID_ENV),
        bot_token=bot_token,
        app_token=app_token,
        env=dict(env),
        bot_name=env_lookup(
            profile,
            env,
            "SLACKBOT_BOT_NAME",
            profile.default_bot_name,
        )
        or profile.default_bot_name,
        command_name=normalize_command_name(
            env_lookup(
                profile,
                env,
                "SLACKBOT_COMMAND_NAME",
                profile.default_command_name,
            ),
            profile.default_command_name,
        ),
        codex_bin=env_lookup(
            profile,
            env,
            "SLACKBOT_CODEX_BIN",
            default_executable_path(profile.default_codex_bin),
        )
        or profile.default_codex_bin,
        codex_mode=normalize_codex_mode(
            env_lookup(
                profile,
                env,
                "SLACKBOT_CODEX_MODE",
                profile.default_codex_mode,
            )
        ),
        codex_model=env_lookup(
            profile,
            env,
            "SLACKBOT_CODEX_MODEL",
            profile.default_codex_model,
        ),
        codex_profile=env_lookup(
            profile,
            env,
            "SLACKBOT_CODEX_PROFILE",
            profile.default_codex_profile,
        ),
        codex_workdir=codex_workdir,
        codex_extra_args=parse_extra_args(
            env_lookup(
                profile,
                env,
                "SLACKBOT_CODEX_EXTRA_ARGS",
                " ".join(profile.default_codex_extra_args),
            )
        ),
        allowed_dm_user_ids=parse_csv(
            env_lookup_allow_empty(
                profile,
                env,
                "SLACKBOT_ALLOWED_DM_USER_IDS",
                render_csv(profile.default_allowed_dm_user_ids),
            )
        ),
        allowed_user_ids=parse_csv(
            env_lookup_allow_empty(
                profile,
                env,
                "SLACKBOT_ALLOWED_USER_IDS",
                render_csv(profile.default_allowed_user_ids),
            )
        ),
        allowed_channel_ids=parse_csv(
            env_lookup_allow_empty(
                profile,
                env,
                "SLACKBOT_ALLOWED_CHANNEL_IDS",
                render_csv(profile.default_allowed_channel_ids),
            )
        ),
        worker_count=normalize_positive_int(
            env_lookup(profile, env, "SLACKBOT_WORKER_COUNT"),
            profile.default_worker_count,
        ),
        mcp_server_url=env_lookup(
            profile,
            env,
            "MCP_SERVER_URL",
            profile.default_mcp_server_url,
        ),
        team_config_path=env_lookup(
            profile,
            env,
            "TEAM_CONFIG_PATH",
            profile.default_team_config_path,
        ),
    )


def mask_secret(value: str | None) -> str:
    if not value:
        return "missing"
    return f"present:{len(value)}"


def query_status(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    config = load_config(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
        allow_missing_tokens=True,
    )
    status = {
        "app_slug": profile.app_slug,
        "runtime_root": config.paths.runtime_root,
        "env_path": config.paths.env_path,
        "state_dir": config.paths.state_dir,
        "service_manager": config.paths.service_manager,
        "unit_name": config.paths.unit_name,
        "bot_name": config.bot_name,
        "command_name": config.command_name,
        "codex_bin": config.codex_bin,
        "codex_mode": config.codex_mode,
        "codex_workdir": config.codex_workdir,
        "worker_count": config.worker_count,
        "mcp_server_url": config.mcp_server_url or "",
        "team_config_path": config.team_config_path or "",
        "user_config_dir": config.paths.user_config_dir,
        "bot_token": mask_secret(config.bot_token),
        "app_token": mask_secret(config.app_token),
    }
    if config.bot_token:
        try:
            auth = slack_api_post(config.bot_token, "auth.test", {})
        except SlackbotError as exc:
            status["slack_auth"] = str(exc)
        else:
            status["slack_auth"] = "ok"
            status["slack_team"] = auth.get("team") or ""
            status["slack_user_id"] = auth.get("user_id") or ""
            auth_url = str(auth.get("url") or "")
            status["slack_team_domain"] = (
                urllib.parse.urlparse(auth_url).netloc or ""
            )
    else:
        status["slack_auth"] = "missing-token"
    if config.app_token:
        status["slack_socket_mode"] = "configured"
    else:
        status["slack_socket_mode"] = "missing-token"
    return status


def route_to_dict(
    route: SlackbotRoute,
    *,
    include_response_url: bool = False,
) -> dict[str, Any]:
    data = {
        "channel_id": route.channel_id,
        "dm_user_id": route.dm_user_id,
        "mention_user_ids": list(route.mention_user_ids),
        "thread_ts": route.thread_ts,
    }
    if include_response_url:
        data["response_url"] = route.response_url
    return data


def route_from_dict(data: dict[str, Any]) -> SlackbotRoute:
    return SlackbotRoute(
        channel_id=data.get("channel_id") or None,
        dm_user_id=data.get("dm_user_id") or None,
        mention_user_ids=tuple(data.get("mention_user_ids") or ()),
        thread_ts=data.get("thread_ts") or None,
        response_url=data.get("response_url") or None,
    )


def payload_to_dict(payload: SlackbotIngressPayload) -> dict[str, Any]:
    data = {
        "kind": payload.kind,
        "text": payload.text,
        "route": route_to_dict(payload.route, include_response_url=True),
        "execution_mode": payload.execution_mode,
        "project": payload.project,
        "session": payload.session,
        "slack_user_id": payload.slack_user_id,
        "created_at": payload.created_at or utcnow_iso(),
    }
    if payload.system_scoped is not None:
        data["system_scoped"] = payload.system_scoped
    return data


def parse_ingress_system_scoped(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    raise SlackbotError("Codex ingress system_scoped must be a boolean")


def enqueue_ingress(
    profile: SlackbotProfile,
    *,
    kind: str,
    text: str,
    route: SlackbotRoute,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
    execution_mode: str = "raw",
    project: str | None = None,
    session: str | None = None,
    slack_user_id: str | None = None,
    system_scoped: bool | None = None,
) -> str:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    Path(paths.ingress_dir).mkdir(parents=True, exist_ok=True)
    if kind == "codex_prompt":
        if system_scoped is None:
            if slack_user_id or route.dm_user_id:
                system_scoped = False
            else:
                raise SlackbotError(
                    "Codex ingress requires slack_user_id or "
                    "system_scoped=True"
                )
        system_scoped = parse_ingress_system_scoped(system_scoped)
        if system_scoped and slack_user_id:
            raise SlackbotError(
                "Codex ingress cannot set both slack_user_id "
                "and system_scoped"
            )
        resolved_slack_user_id = None
        if not system_scoped:
            resolved_slack_user_id = slack_user_id or route.dm_user_id
            if not resolved_slack_user_id:
                raise SlackbotError(
                    "Codex ingress requires slack_user_id when "
                    "system_scoped=False"
                )
    else:
        if system_scoped is not None:
            parse_ingress_system_scoped(system_scoped)
        system_scoped = None
        resolved_slack_user_id = slack_user_id
    payload = SlackbotIngressPayload(
        kind=kind,
        text=text,
        route=route,
        execution_mode=normalize_execution_mode(execution_mode),
        project=project,
        session=session,
        slack_user_id=resolved_slack_user_id,
        system_scoped=system_scoped,
        created_at=utcnow_iso(),
    )
    digest = hashlib.sha256(
        json.dumps(payload_to_dict(payload), sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    unique = uuid.uuid4().hex[:8]
    filename = (
        f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{os.getpid()}-{unique}-{digest}.json"
    )
    path = Path(paths.ingress_dir) / filename
    write_json_atomic(path, payload_to_dict(payload))
    return str(path)


def slack_api_post(
    token: str,
    method: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    url = f"https://slack.com/api/{method}"
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    body = ""
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
            break
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            retry_after = exc.headers.get("Retry-After")
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if retryable and attempt < 2:
                try:
                    delay = float(retry_after or "")
                except ValueError:
                    delay = 0.5 * (2**attempt)
                if delay <= 0:
                    delay = 0.5 * (2**attempt)
                delay = min(delay, 30.0)
                time.sleep(delay)
                continue
            raise SlackbotError(f"Slack API HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            raise SlackbotError(
                f"Slack API {method} connection failed: {exc.reason}"
            ) from exc
        except OSError as exc:
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            raise SlackbotError(
                f"Slack API {method} connection failed: {exc}"
            ) from exc
    try:
        result = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SlackbotError(
            f"Slack API {method} returned invalid JSON: {body}"
        ) from exc
    if not result.get("ok"):
        raise SlackApiError(method, result)
    return result


def slack_response_url_post(
    response_url: str,
    text: str,
    *,
    blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        response_url,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError) as exc:
        raise SlackbotError(f"Slack response_url post failed: {exc}") from exc
    return {"ok": True, "body": body}


def post_slack_message(
    config: SlackbotConfig,
    route: SlackbotRoute,
    text: str,
) -> dict[str, Any] | None:
    if not config.bot_token:
        raise SlackbotError("Slack bot token is missing")
    if not text:
        return None
    rendered_text = render_route_text(route, text)
    chunks = chunk_text(rendered_text)
    first_response = None
    post_route = route
    force_response_url = False
    for chunk in chunks:
        if force_response_url:
            response = slack_response_url_post(post_route.response_url, chunk)
        else:
            try:
                response = _post_single_slack_message(
                    config, post_route, chunk
                )
            except SlackbotError:
                if not post_route.response_url:
                    raise
                response = slack_response_url_post(
                    post_route.response_url, chunk
                )
                force_response_url = True
        if first_response is None:
            first_response = response
            if response and response.get("ts") and not route.thread_ts:
                post_route = SlackbotRoute(
                    channel_id=route.channel_id or response.get("channel"),
                    dm_user_id=route.dm_user_id,
                    mention_user_ids=(),
                    thread_ts=response.get("ts"),
                    response_url=route.response_url,
                )
    return first_response


def render_route_text(route: SlackbotRoute, text: str) -> str:
    prefix = " ".join(f"<@{user}>" for user in route.mention_user_ids)
    slack_text = markdown_to_slack_mrkdwn(text)
    return f"{prefix} {slack_text}".strip() if prefix else slack_text


def markdown_to_slack_mrkdwn(text: str) -> str:
    """Convert common Codex Markdown emphasis to Slack mrkdwn."""
    if "**" not in text and "__" not in text:
        return text
    parts = re.split(r"(```.*?```|`[^`\n]*`)", text, flags=re.DOTALL)
    for index in range(0, len(parts), 2):
        parts[index] = re.sub(
            r"(?<!\*)\*\*(?!\s)(.+?)(?<!\s)\*\*(?!\*)",
            r"*\1*",
            parts[index],
        )
        parts[index] = re.sub(
            r"(?<!_)__(?!\s)(.+?)(?<!\s)__(?!_)",
            r"*\1*",
            parts[index],
        )
    return "".join(parts)


def _post_single_slack_message(
    config: SlackbotConfig,
    route: SlackbotRoute,
    text: str,
    *,
    blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    channel = route.channel_id
    if not channel and route.dm_user_id:
        opened = slack_api_post(
            config.bot_token,
            "conversations.open",
            {"users": route.dm_user_id},
        )
        channel = opened["channel"]["id"]
    if not channel:
        if route.response_url:
            return slack_response_url_post(
                route.response_url,
                text,
                blocks=blocks,
            )
        return None
    payload: dict[str, Any] = {"channel": channel, "text": text}
    if blocks:
        payload["blocks"] = blocks
    if route.thread_ts:
        payload["thread_ts"] = route.thread_ts
    try:
        return slack_api_post(config.bot_token, "chat.postMessage", payload)
    except SlackApiError as exc:
        if exc.error != "not_in_channel":
            raise
        slack_api_post(
            config.bot_token,
            "conversations.join",
            {"channel": channel},
        )
        return slack_api_post(config.bot_token, "chat.postMessage", payload)


def post_slack_blocks(
    config: SlackbotConfig,
    route: SlackbotRoute,
    text: str,
    blocks: list[dict[str, Any]],
) -> dict[str, Any] | None:
    rendered_text = render_route_text(route, text)
    if not config.bot_token and route.response_url:
        return slack_response_url_post(
            route.response_url,
            rendered_text,
            blocks=blocks,
        )
    if not config.bot_token:
        raise SlackbotError("Slack bot token is missing")
    try:
        return _post_single_slack_message(
            config,
            route,
            rendered_text,
            blocks=blocks,
        )
    except SlackbotError:
        if not route.response_url:
            raise
        return slack_response_url_post(
            route.response_url,
            rendered_text,
            blocks=blocks,
        )


def update_slack_message(
    config: SlackbotConfig,
    *,
    channel: str,
    ts: str,
    text: str,
) -> dict[str, Any] | None:
    if not config.bot_token:
        raise SlackbotError("Slack bot token is missing")
    return slack_api_post(
        config.bot_token,
        "chat.update",
        {"channel": channel, "ts": ts, "text": text},
    )


def parse_slack_thread_permalink(permalink: str) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(permalink)
    parts = [part for part in parsed.path.split("/") if part]
    for index, part in enumerate(parts):
        if part == "archives" and index + 2 < len(parts):
            channel_id = parts[index + 1]
            message_key = parts[index + 2]
            break
    else:
        raise SlackbotError(f"Unsupported Slack permalink: {permalink}")
    if not message_key.startswith("p"):
        raise SlackbotError(f"Unsupported Slack permalink: {permalink}")
    query_thread_ts = (
        urllib.parse.parse_qs(parsed.query).get("thread_ts") or [None]
    )[0]
    if query_thread_ts:
        raw_query_ts = query_thread_ts.replace(".", "")
        if raw_query_ts.isdigit() and "." in query_thread_ts:
            return channel_id, query_thread_ts
    raw = message_key[1:]
    if not raw.isdigit() or len(raw) < 7:
        raise SlackbotError(f"Unsupported Slack permalink: {permalink}")
    thread_ts = f"{raw[:-6]}.{raw[-6:]}"
    return channel_id, thread_ts


def runtime_cli_args(paths: SlackbotPaths) -> tuple[str, ...]:
    return (
        f"--runtime-root={paths.runtime_root}",
        f"--env-path={paths.env_path}",
        f"--state-dir={paths.state_dir}",
        f"--system-prompt-path={paths.system_prompt_path}",
        f"--unit-name={paths.unit_name}",
    )


def read_slack_thread(
    config: SlackbotConfig,
    permalink: str,
) -> list[dict[str, Any]]:
    if not config.bot_token:
        raise SlackbotError("Slack bot token is missing")
    channel_id, thread_ts = parse_slack_thread_permalink(permalink)
    messages: list[dict[str, Any]] = []
    cursor = None
    while True:
        payload = {"channel": channel_id, "ts": thread_ts, "limit": 200}
        if cursor:
            payload["cursor"] = cursor
        response = slack_api_post(
            config.bot_token,
            "conversations.replies",
            payload,
        )
        messages.extend(response.get("messages") or [])
        cursor = (response.get("response_metadata") or {}).get(
            "next_cursor"
        ) or None
        if not cursor:
            return messages


def render_thread_transcript(messages: list[dict[str, Any]]) -> str:
    lines = []
    for message in messages:
        user = message.get("user") or message.get("username") or "unknown"
        ts = message.get("ts") or ""
        text = message.get("text") or ""
        lines.append(f"[{ts}] {user}: {text}")
    return "\n".join(lines)


def slackbot_console_url(config: SlackbotConfig) -> str | None:
    direct = env_lookup(config.profile, config.env, "CONSOLE_PUBLIC_URL")
    if direct:
        return direct.rstrip("/") + "/"
    base = env_lookup(config.profile, config.env, "CONSOLE_PUBLIC_BASE_URL")
    if not base:
        return None
    root_path = normalize_root_path(
        env_lookup(
            config.profile,
            config.env,
            "CONSOLE_ROOT_PATH",
            DEFAULT_CONSOLE_ROOT_PATH,
        )
    )
    return base.rstrip("/") + root_path + "/"


def slackbot_working_text(config: SlackbotConfig) -> str:
    url = slackbot_console_url(config)
    if not url:
        return "Working..."
    return f"Working...\n\nKickle console: {url}"


def _block_plain_text(
    text: str,
    *,
    max_length: int = 3000,
) -> dict[str, Any]:
    return {"type": "plain_text", "text": text[:max_length], "emoji": True}


def _block_mrkdwn(text: str, *, max_length: int = 3000) -> dict[str, Any]:
    return {"type": "mrkdwn", "text": text[:max_length]}


def _plain_text_input(
    *,
    action_id: str,
    placeholder: str,
    initial_value: str | None = None,
    multiline: bool = False,
) -> dict[str, Any]:
    element: dict[str, Any] = {
        "type": "plain_text_input",
        "action_id": action_id,
        "placeholder": _block_plain_text(placeholder, max_length=150),
        "multiline": multiline,
    }
    if initial_value:
        element["initial_value"] = initial_value[:3000]
    return element


def _input_block(
    *,
    block_id: str,
    label: str,
    element: dict[str, Any],
    optional: bool = True,
    hint: str | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "type": "input",
        "block_id": block_id,
        "label": _block_plain_text(label, max_length=2000),
        "element": element,
        "optional": optional,
    }
    if hint:
        block["hint"] = _block_plain_text(hint, max_length=2000)
    return block


def _checkbox_option(text: str, value: str) -> dict[str, Any]:
    return {
        "text": _block_plain_text(text, max_length=75),
        "value": value[:75],
    }


def safe_slack_storage_id(value: str, description: str = "Slack ID") -> str:
    text = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{2,100}", text) or set(text) == {"."}:
        raise SlackbotError(f"Unsupported {description}: {value!r}")
    return text


def _normalize_user_config_secret_field(
    field: SlackbotUserConfigSecretField,
) -> SlackbotUserConfigSecretField:
    service_name = safe_slack_storage_id(
        field.service_name,
        "secret service name",
    ).lower()
    block_id = safe_slack_storage_id(
        field.block_id or f"{service_name}_token",
        "secret block ID",
    )
    action_id = safe_slack_storage_id(
        field.action_id or block_id,
        "secret action ID",
    )
    label = field.label.strip()
    if not label:
        raise SlackbotError(
            f"Secret field {service_name!r} is missing a label"
        )
    env_names = tuple(
        OrderedDict.fromkeys(
            env_name.strip()
            for env_name in field.env_names
            if env_name.strip()
        )
    )
    scrub_env_names = tuple(
        OrderedDict.fromkeys(
            env_name.strip()
            for env_name in field.scrub_env_names
            if env_name.strip()
        )
    )
    if not env_names:
        raise SlackbotError(
            f"Secret field {service_name!r} has no environment names"
        )
    for env_name in (*env_names, *scrub_env_names):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
            raise SlackbotError(
                f"Unsupported secret environment variable: {env_name!r}"
            )
    return replace(
        field,
        service_name=service_name,
        block_id=block_id,
        action_id=action_id,
        label=label,
        env_names=env_names,
        scrub_env_names=scrub_env_names,
        required=bool(field.required),
    )


def user_config_secret_fields(
    profile: SlackbotProfile | None = None,
) -> tuple[SlackbotUserConfigSecretField, ...]:
    fields = list(CORE_USER_CONFIG_SECRET_FIELDS)
    if profile is not None:
        fields.extend(profile.user_config_secret_fields)
    merged: OrderedDict[str, SlackbotUserConfigSecretField] = OrderedDict()
    for secret_field in fields:
        normalized = _normalize_user_config_secret_field(secret_field)
        existing = merged.get(normalized.service_name)
        if existing is not None:
            env_names = tuple(
                OrderedDict.fromkeys(
                    (*existing.env_names, *normalized.env_names)
                )
            )
            scrub_env_names = tuple(
                OrderedDict.fromkeys(
                    (
                        *existing.scrub_env_names,
                        *normalized.scrub_env_names,
                    )
                )
            )
            merged[normalized.service_name] = replace(
                existing,
                env_names=env_names,
                scrub_env_names=scrub_env_names,
                required=existing.required or normalized.required,
            )
            continue
        merged[normalized.service_name] = normalized
    return tuple(merged.values())


def user_config_secret_field(
    profile: SlackbotProfile | None,
    service_name: str,
) -> SlackbotUserConfigSecretField:
    key = safe_slack_storage_id(
        service_name.strip().lower(),
        "secret service name",
    )
    for secret_field in user_config_secret_fields(profile):
        if secret_field.service_name == key:
            return secret_field
    raise SlackbotError(f"Unsupported Slack user secret: {service_name}")


def user_config_directory(paths: SlackbotPaths, user_id: str) -> Path:
    safe_user = safe_slack_storage_id(user_id, "Slack user ID")
    return Path(paths.user_config_dir) / safe_user


def _read_user_secret_token(token_path: Path) -> str | None:
    try:
        return token_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise SlackbotError(
            f"Could not read Slack user secret file: {token_path}"
        ) from exc


def _user_secret_status(token_path: Path) -> str:
    try:
        token = _read_user_secret_token(token_path)
    except SlackbotError:
        return "invalid"
    if token is None:
        return "missing"
    if not token:
        return "empty"
    return "configured"


def read_user_config(
    paths: SlackbotPaths,
    user_id: str,
    profile: SlackbotProfile | None = None,
) -> dict[str, Any]:
    directory = user_config_directory(paths, user_id)
    profile_data = read_json(directory / "profile.json", {})
    secrets = {}
    for secret_field in user_config_secret_fields(profile):
        secrets[secret_field.service_name] = _user_secret_status(
            directory / "secrets" / f"{secret_field.service_name}.token"
        )
    return {"profile": profile_data, "secrets": secrets}


def user_codex_sessions_enabled(paths: SlackbotPaths, user_id: str) -> bool:
    directory = user_config_directory(paths, user_id)
    profile_path = directory / "profile.json"
    if profile_path.exists():
        try:
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SlackbotError(
                f"Malformed Slack user profile: {profile_path}"
            ) from exc
    else:
        profile = {}
    if not isinstance(profile, dict):
        raise SlackbotError(
            f"Slack user profile is not an object: {profile_path}"
        )
    preferences = profile.get("preferences") or {}
    return bool(preferences.get("codex_enabled", True))


def read_user_secret_env(
    paths: SlackbotPaths,
    user_id: str,
    profile: SlackbotProfile | None = None,
) -> dict[str, str]:
    directory = user_config_directory(paths, user_id)
    secrets_dir = directory / "secrets"
    env: dict[str, str] = {}
    for secret_field in user_config_secret_fields(profile):
        token_path = secrets_dir / f"{secret_field.service_name}.token"
        try:
            token = _read_user_secret_token(token_path)
        except SlackbotError:
            if secret_field.required:
                raise
            continue
        if token is None:
            if secret_field.required:
                raise SlackbotError(
                    "Missing required Slack user secret: "
                    f"{secret_field.service_name}"
                )
            continue
        if not token:
            if secret_field.required:
                raise SlackbotError(
                    "Required Slack user secret is empty: "
                    f"{secret_field.service_name}"
                )
            continue
        for env_name in secret_field.env_names:
            env[env_name] = token
    return env


def store_user_secret_token(
    paths: SlackbotPaths,
    user_id: str,
    profile: SlackbotProfile,
    service_name: str,
    token: str,
    *,
    source: str = "cli",
) -> dict[str, str]:
    secret_field = user_config_secret_field(profile, service_name)
    clean_token = token.strip()
    if not clean_token:
        raise SlackbotError(
            f"Refusing to store empty {secret_field.service_name} token"
        )

    now = utcnow_iso()
    directory = user_config_directory(paths, user_id)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    secrets_dir = directory / "secrets"
    secrets_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    secrets_dir.chmod(0o700)
    token_path = secrets_dir / f"{secret_field.service_name}.token"
    write_text(token_path, clean_token + "\n", mode=0o600)

    profile_path = directory / "profile.json"
    with locked_json_path(profile_path):
        existing = read_json(profile_path, {})
        preferences = dict(existing.get("preferences") or {})
        secrets = dict(existing.get("secrets") or {})
        previous = secrets.get(secret_field.service_name)
        created_at = None
        if isinstance(previous, dict):
            created_at = previous.get("created_at")
        secrets[secret_field.service_name] = {
            "created_at": created_at or now,
            "source": str(source or "cli"),
            "updated_at": now,
        }
        data = {
            "app_slug": profile.app_slug,
            "created_at": existing.get("created_at") or now,
            "preferences": preferences,
            "secrets": secrets,
            "slack_team_id": existing.get("slack_team_id") or "",
            "slack_user_id": safe_slack_storage_id(user_id, "Slack user ID"),
            "updated_at": now,
        }
        if existing.get("last_mode"):
            data["last_mode"] = existing["last_mode"]
        write_json_atomic(profile_path, data)

    return {
        "service_name": secret_field.service_name,
        "token_path": str(token_path),
        "profile_path": str(profile_path),
        "user_id": safe_slack_storage_id(user_id, "Slack user ID"),
    }


def user_config_mode_from_text(text: str) -> str | None:
    tokens = text.strip().split()
    if len(tokens) != 1:
        return None
    command = tokens[0].lower()
    if command not in USER_CONFIG_TRIGGER_WORDS:
        return None
    if command in {"bootstrap", "onboard", "setup"}:
        return "bootstrap"
    return "config"


def normalize_user_config_mode(value: Any) -> str:
    return (
        "bootstrap" if str(value or "").strip() == "bootstrap" else "config"
    )


def normalize_static_response_name(text: Any) -> str:
    return " ".join(str(text or "").casefold().strip().split())


def static_response_text_from_profile(
    profile: SlackbotProfile,
    text: Any,
) -> str | None:
    key = normalize_static_response_name(text)
    if not key:
        return None
    for response in profile.static_responses:
        names = {
            normalize_static_response_name(name) for name in response.names
        }
        if key in names:
            return response.text
    return None


def _secret_status_block(
    secret_field: SlackbotUserConfigSecretField,
    *,
    status: str,
) -> dict[str, Any]:
    secret_field = _normalize_user_config_secret_field(secret_field)
    labels = {
        "configured": "configured",
        "empty": "empty",
        "invalid": "invalid",
        "missing": "not configured",
    }
    label = labels.get(status, "not configured")
    if secret_field.required and status != "configured":
        label = f"required, {label}"
    return {
        "type": "section",
        "block_id": secret_field.block_id,
        "text": _block_mrkdwn(
            f"*{secret_field.label}*: {label}. "
            "Managed outside Slack; this modal never accepts token values."
        ),
    }


def build_user_config_view(
    config: SlackbotConfig,
    *,
    user_id: str,
    channel_id: str | None = None,
    mode: str = "config",
) -> dict[str, Any]:
    mode = normalize_user_config_mode(mode)
    existing = read_user_config(config.paths, user_id, config.profile)
    profile = existing.get("profile") or {}
    preferences = profile.get("preferences") or {}
    secrets = existing.get("secrets") or {}
    codex_enabled = bool(preferences.get("codex_enabled", True))
    private_metadata = json.dumps(
        {
            "channel_id": channel_id,
            "mode": mode,
            "user_id": user_id,
        },
        separators=(",", ":"),
    )
    title = (
        f"{config.profile.app_slug} bootstrap"
        if mode == "bootstrap"
        else f"{config.profile.app_slug} config"
    )
    codex_option = _checkbox_option(
        "Allow Codex-backed work sessions",
        USER_CONFIG_CODEX_OPTION_VALUE,
    )
    codex_element: dict[str, Any] = {
        "type": "checkboxes",
        "action_id": USER_CONFIG_CODEX_ACTION_ID,
        "options": [codex_option],
    }
    if codex_enabled:
        codex_element["initial_options"] = [codex_option]
    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": _block_mrkdwn(
                "User-scoped settings for this bot. Secret fields are "
                "managed outside Slack and are shown here only as status."
            ),
        },
        _input_block(
            block_id="display_name",
            label="Display name",
            element=_plain_text_input(
                action_id="display_name",
                placeholder="Your preferred name",
                initial_value=preferences.get("display_name") or "",
            ),
        ),
        _input_block(
            block_id="default_project",
            label="Default project",
            element=_plain_text_input(
                action_id="default_project",
                placeholder="docs, tooling, operations, ...",
                initial_value=preferences.get("default_project") or "",
            ),
        ),
        *[
            _secret_status_block(
                secret_field,
                status=str(secrets.get(secret_field.service_name) or ""),
            )
            for secret_field in user_config_secret_fields(config.profile)
        ],
        {
            "type": "input",
            "block_id": USER_CONFIG_CODEX_BLOCK_ID,
            "label": _block_plain_text("Work sessions"),
            "optional": True,
            "element": codex_element,
        },
        _input_block(
            block_id="notes",
            label="Notes",
            element=_plain_text_input(
                action_id="notes",
                placeholder=(
                    "Anything this bot should remember for your setup"
                ),
                initial_value=preferences.get("notes") or "",
                multiline=True,
            ),
        ),
    ]
    return {
        "type": "modal",
        "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
        "title": _block_plain_text(title, max_length=24),
        "submit": _block_plain_text("Save"),
        "close": _block_plain_text("Cancel"),
        "private_metadata": private_metadata,
        "blocks": blocks,
    }


def _view_private_metadata(view: dict[str, Any]) -> dict[str, Any]:
    raw = view.get("private_metadata") or ""
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _view_state_value(
    values: dict[str, Any],
    block_id: str,
    action_id: str,
) -> str:
    block = values.get(block_id) or {}
    element = block.get(action_id) or {}
    return str(element.get("value") or "").strip()


def _view_state_checked(
    values: dict[str, Any],
    block_id: str,
    action_id: str,
    value: str,
) -> bool:
    block = values.get(block_id) or {}
    element = block.get(action_id) or {}
    selected = element.get("selected_options") or []
    return any(option.get("value") == value for option in selected)


def save_user_config_submission(
    config: SlackbotConfig,
    payload: dict[str, Any],
) -> dict[str, Any]:
    user_id = str((payload.get("user") or {}).get("id") or "").strip()
    if not user_id:
        raise SlackbotError("Slack config submission is missing user ID")
    view = payload.get("view") or {}
    metadata = _view_private_metadata(view)
    metadata_user = str(metadata.get("user_id") or "").strip()
    if metadata_user and metadata_user != user_id:
        raise SlackbotError("Slack config submission user mismatch")
    callback_id = view.get("callback_id")
    if callback_id != USER_CONFIG_MODAL_CALLBACK_ID:
        raise SlackbotError(
            f"Unsupported config modal callback: {callback_id}"
        )
    values = (view.get("state") or {}).get("values") or {}
    now = utcnow_iso()
    directory = user_config_directory(config.paths, user_id)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    profile_path = directory / "profile.json"
    updated_fields = []
    team_id = str((payload.get("team") or {}).get("id") or "").strip()
    with locked_json_path(profile_path):
        existing = read_json(profile_path, {})
        preferences = dict(existing.get("preferences") or {})
        for name, (block_id, action_id) in USER_CONFIG_FIELDS.items():
            value = _view_state_value(values, block_id, action_id)
            previous = preferences.get(name, "")
            if value:
                preferences[name] = value
            else:
                preferences.pop(name, None)
            if value != previous:
                updated_fields.append(name)
        codex_enabled = _view_state_checked(
            values,
            USER_CONFIG_CODEX_BLOCK_ID,
            USER_CONFIG_CODEX_ACTION_ID,
            USER_CONFIG_CODEX_OPTION_VALUE,
        )
        if preferences.get("codex_enabled") != codex_enabled:
            updated_fields.append("codex_enabled")
        preferences["codex_enabled"] = codex_enabled
        data = {
            "app_slug": config.profile.app_slug,
            "created_at": existing.get("created_at") or now,
            "last_mode": normalize_user_config_mode(metadata.get("mode")),
            "preferences": preferences,
            "secrets": dict(existing.get("secrets") or {}),
            "slack_team_id": team_id or existing.get("slack_team_id") or "",
            "slack_user_id": user_id,
            "updated_at": now,
        }
        write_json_atomic(profile_path, data)
    return {
        "profile_path": str(profile_path),
        "secret_updates": [],
        "updated_fields": updated_fields,
        "user_id": user_id,
    }


def user_config_saved_message(summary: dict[str, Any]) -> str:
    fields = sorted(set(summary.get("updated_fields") or []))
    secrets = sorted(set(summary.get("secret_updates") or []))
    parts = ["Config saved."]
    if fields:
        parts.append("Updated fields: " + ", ".join(fields) + ".")
    if secrets:
        parts.append(
            "Secret files changed outside Slack: " + ", ".join(secrets) + "."
        )
    if not fields and not secrets:
        parts.append("No fields changed. Extremely tidy.")
    parts.append(
        "Use the slash command again whenever you need to adjust it."
    )
    return " ".join(parts)


def user_config_launcher_blocks(
    mode: str = "config",
    *,
    command_name: str = "/sneeze",
) -> list[dict[str, Any]]:
    mode = normalize_user_config_mode(mode)
    label = "Open bootstrap" if mode == "bootstrap" else "Open config"
    text = (
        "Use the button to open the config modal. Slash command shortcut: "
        f"`{command_name} config`."
    )
    return [
        {"type": "section", "text": _block_mrkdwn(text)},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "action_id": USER_CONFIG_OPEN_ACTION_ID,
                    "text": _block_plain_text(label),
                    "style": "primary",
                    "value": mode,
                }
            ],
        },
    ]


def post_user_config_launcher(
    config: SlackbotConfig,
    route: SlackbotRoute,
    *,
    mode: str = "config",
) -> dict[str, Any] | None:
    mode = normalize_user_config_mode(mode)
    return post_slack_blocks(
        config,
        route,
        f"Open the {config.profile.app_slug} config modal.",
        user_config_launcher_blocks(mode, command_name=config.command_name),
    )


def render_systemd_service(
    config: SlackbotConfig,
    *,
    cli_bin: str,
    run_subcommand: str,
    restart: str = "on-failure",
    restart_sec: int = 10,
    start_limit_interval_sec: int = 300,
    start_limit_burst: int = 5,
    syslog_identifier: str | None = None,
) -> str:
    command = " ".join(
        shlex.quote(part)
        for part in (cli_bin, run_subcommand, *runtime_cli_args(config.paths))
    )
    syslog_name = syslog_identifier or f"{config.profile.app_slug}-slackbot"
    working_dir = systemd_path_value(config.codex_workdir)
    log_file = quote_systemd_value(f"SNEEZE_LOG_FILE={config.paths.log_path}")
    env_file = systemd_path_value(config.paths.env_path)
    return "\n".join(
        [
            "[Unit]",
            f"Description={config.profile.app_slug} Slack bot",
            "Wants=network-online.target",
            "After=network-online.target",
            f"StartLimitIntervalSec={start_limit_interval_sec}",
            f"StartLimitBurst={start_limit_burst}",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={working_dir}",
            'Environment="PYTHONUNBUFFERED=1"',
            f"Environment={log_file}",
            f"EnvironmentFile={env_file}",
            f"ExecStart={command}",
            f"Restart={restart}",
            f"RestartSec={restart_sec}",
            "TimeoutStopSec=30",
            f"SyslogIdentifier={syslog_name}",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def render_launchd_service(
    config: SlackbotConfig,
    *,
    cli_bin: str,
    run_subcommand: str,
) -> str:
    env_vars = {
        "PYTHONUNBUFFERED": "1",
        "SNEEZE_LOG_FILE": config.paths.log_path,
    }
    for key in CHILD_ENV_ALIASES:
        value = env_lookup(config.profile, config.env, key)
        if value:
            env_vars.setdefault(key, value)
            env_vars.setdefault(config.profile.env_name(key), value)
    obj = {
        "Label": config.paths.unit_label,
        "ProgramArguments": [
            cli_bin,
            run_subcommand,
            *runtime_cli_args(config.paths),
        ],
        "WorkingDirectory": config.codex_workdir,
        "EnvironmentVariables": env_vars,
        "KeepAlive": True,
        "RunAtLoad": True,
        "StandardOutPath": config.paths.log_path,
        "StandardErrorPath": config.paths.log_path,
    }
    return plistlib.dumps(obj).decode("utf-8")


def install_service(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
    cli_bin: str,
    run_subcommand: str,
    restart: str = "on-failure",
    restart_sec: int = 10,
    start_limit_interval_sec: int = 300,
    start_limit_burst: int = 5,
    enable: bool = True,
) -> dict[str, Any]:
    config = load_config(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
        allow_missing_tokens=True,
    )
    if config.paths.service_manager == "launchd":
        text = render_launchd_service(
            config,
            cli_bin=cli_bin,
            run_subcommand=run_subcommand,
        )
        installed_path = (
            launchd_agent_dir() / f"{config.paths.unit_label}.plist"
        )
    else:
        text = render_systemd_service(
            config,
            cli_bin=cli_bin,
            run_subcommand=run_subcommand,
            restart=restart,
            restart_sec=restart_sec,
            start_limit_interval_sec=start_limit_interval_sec,
            start_limit_burst=start_limit_burst,
        )
        installed_path = systemd_user_unit_dir() / config.paths.unit_name
    write_text(config.paths.unit_path, text)
    write_text(installed_path, text)
    enabled = False
    systemctl_results = []
    launchctl_results = []
    if enable and config.paths.service_manager == "systemd":
        reload_proc = subprocess.run(
            ["systemctl", "--user", "daemon-reload"],
            check=False,
            capture_output=True,
            text=True,
        )
        systemctl_results.append(
            {
                "command": "systemctl --user daemon-reload",
                "returncode": reload_proc.returncode,
                "stdout": reload_proc.stdout,
                "stderr": reload_proc.stderr,
            }
        )
        enable_proc = subprocess.run(
            ["systemctl", "--user", "enable", config.paths.unit_name],
            check=False,
            capture_output=True,
            text=True,
        )
        systemctl_results.append(
            {
                "command": (
                    f"systemctl --user enable {config.paths.unit_name}"
                ),
                "returncode": enable_proc.returncode,
                "stdout": enable_proc.stdout,
                "stderr": enable_proc.stderr,
            }
        )
        enabled = enable_proc.returncode == 0
    elif enable and config.paths.service_manager == "launchd":
        unload_proc = subprocess.run(
            ["launchctl", "unload", str(installed_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        launchctl_results.append(
            {
                "command": f"launchctl unload {installed_path}",
                "returncode": unload_proc.returncode,
                "stdout": unload_proc.stdout,
                "stderr": unload_proc.stderr,
            }
        )
        load_proc = subprocess.run(
            ["launchctl", "load", "-w", str(installed_path)],
            check=False,
            capture_output=True,
            text=True,
        )
        launchctl_results.append(
            {
                "command": f"launchctl load -w {installed_path}",
                "returncode": load_proc.returncode,
                "stdout": load_proc.stdout,
                "stderr": load_proc.stderr,
            }
        )
        enabled = load_proc.returncode == 0
    return {
        "unit_path": config.paths.unit_path,
        "installed_path": str(installed_path),
        "enabled": enabled,
        "systemctl": systemctl_results,
        "launchctl": launchctl_results,
    }


def remove_service(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    removed: list[str] = []
    if paths.service_manager == "systemd":
        subprocess.run(
            ["systemctl", "--user", "disable", paths.unit_name],
            check=False,
        )
        subprocess.run(
            ["systemctl", "--user", "stop", paths.unit_name],
            check=False,
        )
        installed = systemd_user_unit_dir() / paths.unit_name
        candidates = [installed, Path(paths.unit_path)]
        for path in candidates:
            if path.exists():
                path.unlink()
                removed.append(str(path))
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    else:
        installed = launchd_agent_dir() / f"{paths.unit_label}.plist"
        if installed.exists():
            subprocess.run(
                ["launchctl", "unload", "-w", str(installed)],
                check=False,
            )
        for path in (installed, Path(paths.unit_path)):
            if path.exists():
                path.unlink()
                removed.append(str(path))
    return {"removed": removed}


def service_status(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    result: dict[str, Any] = {
        "service_manager": paths.service_manager,
        "unit_name": paths.unit_name,
        "unit_path": paths.unit_path,
        "installed": False,
        "active": "unknown",
    }
    if paths.service_manager != "systemd":
        installed = launchd_agent_dir() / f"{paths.unit_label}.plist"
        result["installed"] = installed.exists()
        return result
    installed = systemd_user_unit_dir() / paths.unit_name
    result["installed"] = installed.exists()
    proc = subprocess.run(
        ["systemctl", "--user", "is-active", paths.unit_name],
        check=False,
        text=True,
        capture_output=True,
    )
    result["active"] = proc.stdout.strip() or proc.stderr.strip()
    return result


def schedule_to_dict(schedule: ScheduleDefinition) -> dict[str, Any]:
    return {
        "name": schedule.name,
        "on_calendar": schedule.on_calendar,
        "command": list(schedule.command),
        "workdir": schedule.workdir,
        "notify_kind": schedule.notify_kind,
        "route": route_to_dict(schedule.route),
        "execution_mode": schedule.execution_mode,
        "project": schedule.project,
        "session": schedule.session,
        "persistent": schedule.persistent,
    }


def schedule_from_dict(data: dict[str, Any]) -> ScheduleDefinition:
    return ScheduleDefinition(
        name=data["name"],
        on_calendar=data["on_calendar"],
        command=tuple(data["command"]),
        workdir=data["workdir"],
        notify_kind=normalize_notify_kind(data.get("notify_kind")),
        route=route_from_dict(data.get("route") or {}),
        execution_mode=normalize_execution_mode(data.get("execution_mode")),
        project=data.get("project") or None,
        session=data.get("session") or None,
        persistent=bool(data.get("persistent", True)),
    )


def schedule_path(paths: SlackbotPaths, name: str) -> Path:
    return Path(paths.schedule_dir) / f"{safe_schedule_name(name)}.json"


def render_schedule_service(
    profile: SlackbotProfile,
    *,
    schedule: ScheduleDefinition,
    paths: SlackbotPaths,
    cli_bin: str,
    run_subcommand: str,
) -> str:
    command = " ".join(
        shlex.quote(part)
        for part in (
            cli_bin,
            run_subcommand,
            *runtime_cli_args(paths),
            f"--name={schedule.name}",
        )
    )
    return "\n".join(
        [
            "[Unit]",
            f"Description={profile.app_slug} scheduled task: {schedule.name}",
            "Wants=network-online.target",
            "After=network-online.target",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={systemd_path_value(schedule.workdir)}",
            'Environment="PYTHONUNBUFFERED=1"',
            f"ExecStart={command}",
            "",
        ]
    )


def render_schedule_timer(
    profile: SlackbotProfile,
    schedule: ScheduleDefinition,
) -> str:
    unit = schedule_unit_name(profile, schedule.name)
    persistent = "true" if schedule.persistent else "false"
    return "\n".join(
        [
            "[Unit]",
            f"Description={profile.app_slug} schedule timer: {schedule.name}",
            "",
            "[Timer]",
            f"OnCalendar={schedule.on_calendar}",
            f"Persistent={persistent}",
            f"Unit={unit}",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def upsert_schedule(
    profile: SlackbotProfile,
    *,
    name: str,
    on_calendar: str,
    command: Iterable[str],
    workdir: str,
    notify_kind: str = "none",
    route: SlackbotRoute | None = None,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
    cli_bin: str,
    run_subcommand: str,
    execution_mode: str = "raw",
    project: str | None = None,
    session: str | None = None,
    install_timer: bool = True,
) -> dict[str, Any]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    route = route or SlackbotRoute()
    notify_kind = normalize_notify_kind(notify_kind)
    if notify_kind != "none" and not (route.channel_id or route.dm_user_id):
        raise SlackbotError(
            "Scheduled notifications require --channel-id or --dm-user-id"
        )
    command = tuple(command)
    if not command:
        raise SlackbotError("Scheduled command must not be empty")
    schedule = ScheduleDefinition(
        name=name,
        on_calendar=on_calendar,
        command=command,
        workdir=str(expand_path(workdir)),
        notify_kind=notify_kind,
        route=route,
        execution_mode=normalize_execution_mode(execution_mode),
        project=project,
        session=session,
    )
    Path(paths.schedule_dir).mkdir(parents=True, exist_ok=True)
    write_json(schedule_path(paths, name), schedule_to_dict(schedule))
    service_text = render_schedule_service(
        profile,
        schedule=schedule,
        paths=paths,
        cli_bin=cli_bin,
        run_subcommand=run_subcommand,
    )
    timer_text = render_schedule_timer(profile, schedule)
    unit = schedule_unit_name(profile, name)
    timer = unit.replace(".service", ".timer")
    installed: list[str] = []
    systemctl_results = []
    install_skipped = ""
    enabled = False
    if paths.service_manager == "systemd":
        local_dir = Path(paths.systemd_dir) / "schedules"
        write_text(local_dir / unit, service_text)
        write_text(local_dir / timer, timer_text)
    if install_timer and paths.service_manager == "systemd":
        unit_dir = systemd_user_unit_dir()
        write_text(unit_dir / unit, service_text)
        write_text(unit_dir / timer, timer_text)
        reload_proc = subprocess.run(
            ["systemctl", "--user", "daemon-reload"],
            check=False,
            capture_output=True,
            text=True,
        )
        enable_proc = subprocess.run(
            ["systemctl", "--user", "enable", "--now", timer],
            check=False,
            capture_output=True,
            text=True,
        )
        systemctl_results = [
            {
                "command": "systemctl --user daemon-reload",
                "returncode": reload_proc.returncode,
                "stdout": reload_proc.stdout,
                "stderr": reload_proc.stderr,
            },
            {
                "command": f"systemctl --user enable --now {timer}",
                "returncode": enable_proc.returncode,
                "stdout": enable_proc.stdout,
                "stderr": enable_proc.stderr,
            },
        ]
        installed = [str(unit_dir / unit), str(unit_dir / timer)]
        enabled = enable_proc.returncode == 0
    elif install_timer:
        install_skipped = (
            f"Scheduled timers are not implemented for "
            f"{paths.service_manager}"
        )
    return {
        "schedule_path": str(schedule_path(paths, name)),
        "unit": unit,
        "timer": timer,
        "installed": installed,
        "enabled": enabled,
        "install_skipped": install_skipped,
        "systemctl": systemctl_results,
    }


def remove_schedule(
    profile: SlackbotProfile,
    *,
    name: str,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    unit = schedule_unit_name(profile, name)
    timer = unit.replace(".service", ".timer")
    removed: list[str] = []
    if paths.service_manager == "systemd":
        subprocess.run(["systemctl", "--user", "stop", timer], check=False)
        subprocess.run(["systemctl", "--user", "disable", timer], check=False)
    for path in (
        schedule_path(paths, name),
        Path(paths.systemd_dir) / "schedules" / unit,
        Path(paths.systemd_dir) / "schedules" / timer,
        systemd_user_unit_dir() / unit,
        systemd_user_unit_dir() / timer,
    ):
        if path.exists():
            path.unlink()
            removed.append(str(path))
    if paths.service_manager == "systemd":
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    return {"removed": removed}


def list_schedules(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> list[ScheduleDefinition]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    directory = Path(paths.schedule_dir)
    if not directory.exists():
        return []
    schedules = []
    for path in sorted(directory.glob("*.json")):
        schedules.append(schedule_from_dict(read_json(path, {})))
    return schedules


def run_schedule(
    profile: SlackbotProfile,
    *,
    name: str,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    config = load_config(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
        allow_missing_tokens=True,
    )
    path = schedule_path(config.paths, name)
    schedule_data = read_json(path, None)
    if not isinstance(schedule_data, dict):
        raise SlackbotError(f"Schedule not found or invalid: {name}")
    try:
        schedule = schedule_from_dict(schedule_data)
    except KeyError as exc:
        raise SlackbotError(
            f"Schedule is missing field {exc}: {name}"
        ) from exc
    started = utcnow_iso()
    command = list(schedule.command)
    stdout = ""
    stderr = ""
    returncode = 127
    error = ""
    if not command:
        error = "Scheduled command must not be empty"
        stderr = error
    else:
        try:
            proc = subprocess.run(
                command,
                cwd=schedule.workdir,
                env=merge_child_process_env(
                    config,
                    scrub_secret_env=True,
                    use_scrub_allowlist=True,
                ),
                text=True,
                check=False,
                capture_output=True,
            )
        except OSError as exc:
            error = str(exc)
            stderr = error
        else:
            stdout = proc.stdout
            stderr = proc.stderr
            returncode = proc.returncode
    report = {
        "name": name,
        "started_at": started,
        "finished_at": utcnow_iso(),
        "command": command,
        "workdir": schedule.workdir,
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    if error:
        report["error"] = error
    report_dir = Path(config.paths.schedule_reports_dir) / safe_schedule_name(
        name
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    report_name = (
        f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{os.getpid()}-{uuid.uuid4().hex[:8]}.json"
    )
    report_path = report_dir / report_name
    write_json_atomic(report_path, report)
    if schedule.notify_kind == "slack_message" and stdout.strip():
        post_slack_message(config, schedule.route, stdout.strip())
    elif schedule.notify_kind == "codex_prompt":
        exit_text = f"Scheduled task `{name}` finished with exit "
        text = (
            f"{exit_text}{returncode}.\n\n"
            f"stdout:\n{stdout}\n\nstderr:\n{stderr}"
        )
        enqueue_ingress(
            profile,
            kind="codex_prompt",
            text=text,
            route=schedule.route,
            runtime_root=config.paths.runtime_root,
            env_path=config.paths.env_path,
            state_dir=config.paths.state_dir,
            system_prompt_path=config.paths.system_prompt_path,
            unit_name=config.paths.unit_name,
            execution_mode=schedule.execution_mode,
            project=schedule.project,
            session=schedule.session,
            system_scoped=True,
        )
    report["report_path"] = str(report_path)
    return report


class ConversationStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.cache: dict[str, Any] | None = None

    def _load(self) -> dict[str, Any]:
        try:
            data = read_json(self.path, {})
        except json.JSONDecodeError:
            data = {}
        self.cache = data
        return dict(data)

    def _save(self, data: dict[str, Any]) -> None:
        write_json_atomic(self.path, data)
        self.cache = dict(data)

    def get(self, key: str) -> dict[str, Any] | None:
        with self.lock:
            with locked_json_path(self.path):
                return self._load().get(key)

    def set(self, key: str, value: dict[str, Any]) -> None:
        with self.lock:
            with locked_json_path(self.path):
                data = self._load()
                data[key] = value
                self._save(data)


class CodexRunner:
    def __init__(self, config: SlackbotConfig):
        self.config = config

    def _common_args(self) -> list[str]:
        args = ["--json"]
        if self.config.codex_mode == "danger-full-access":
            args.append("--dangerously-bypass-approvals-and-sandbox")
        else:
            args.extend(["--sandbox", self.config.codex_mode])
        if self.config.codex_model:
            args.extend(["--model", self.config.codex_model])
        if self.config.codex_profile:
            args.extend(["--profile", self.config.codex_profile])
        args.extend(self.config.codex_extra_args)
        args.extend(
            ["--skip-git-repo-check", "-C", self.config.codex_workdir]
        )
        return args

    def _exec_args(self) -> list[str]:
        return [self.config.codex_bin, "exec", *self._common_args()]

    def _resume_args(self) -> list[str]:
        return [
            self.config.codex_bin,
            "exec",
            "resume",
            *self._common_args(),
        ]

    def _run_args(
        self,
        output_path: str,
        *,
        session_id: str | None = None,
    ) -> list[str]:
        args = self._resume_args() if session_id else self._exec_args()
        if session_id:
            args.extend(["-o", output_path, session_id, "-"])
        else:
            args.extend(["-o", output_path, "-"])
        return args

    def run(
        self,
        prompt: str,
        session_id: str | None = None,
        *,
        extra_env: dict[str, str] | None = None,
        scrub_secret_env: bool = False,
        use_scrub_allowlist: bool = False,
    ) -> dict[str, Any]:
        output = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
        output.close()
        try:
            args = self._run_args(output.name, session_id=session_id)
            proc = subprocess.run(
                args,
                input=prompt,
                env=merge_child_process_env(
                    self.config,
                    extra_env,
                    scrub_secret_env=scrub_secret_env,
                    use_scrub_allowlist=use_scrub_allowlist,
                ),
                text=True,
                check=False,
                capture_output=True,
            )
            last_message = Path(output.name).read_text(
                encoding="utf-8",
                errors="replace",
            )
            return {
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "last_message": last_message,
                "session_id": extract_codex_session_id(proc.stdout),
            }
        finally:
            try:
                os.unlink(output.name)
            except OSError:
                pass

    def run_tmux(
        self,
        prompt: str,
        session_id: str | None = None,
        *,
        extra_env: dict[str, str] | None = None,
        scrub_secret_env: bool = False,
        use_scrub_allowlist: bool = False,
        route: SlackbotRoute | None = None,
        project: str | None = None,
    ) -> dict[str, Any]:
        run_id = uuid.uuid4().hex[:10]
        created = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        tmux_session = safe_tmux_session_name(
            f"{self.config.profile.app_slug}-codex-{created}-{run_id}"
        )
        run_dir = (
            Path(self.config.paths.state_dir)
            / "tmux-codex-runs"
            / f"{created}-{run_id}"
        )
        run_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        prompt_path = run_dir / "prompt.txt"
        output_path = run_dir / "output.txt"
        stdout_path = run_dir / "stdout.jsonl"
        stderr_path = run_dir / "stderr.txt"
        status_path = run_dir / "status.txt"
        script_path = run_dir / "run.sh"
        write_text(prompt_path, prompt, mode=0o600)
        child_env = merge_child_process_env(
            self.config,
            extra_env,
            scrub_secret_env=scrub_secret_env,
            use_scrub_allowlist=use_scrub_allowlist,
        )
        args = self._run_args(str(output_path), session_id=session_id)
        script = render_tmux_codex_script(
            command=args,
            env=child_env,
            prompt_path=prompt_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            status_path=status_path,
            workdir=Path(self.config.codex_workdir),
        )
        write_text(script_path, script, mode=0o700)
        tmux_bin = codex_tmux_bin(self.config)
        tmux_socket = codex_tmux_socket(self.config)
        _record_agent_tmux_job(
            self.config,
            run_id,
            {
                "id": run_id,
                "status": "running",
                "tmux_session": tmux_session,
                "tmux_socket": tmux_socket,
                "run_dir": str(run_dir),
                "route": route_to_dict(route) if route else None,
                "project": project,
                "created_at": utcnow_iso(),
            },
        )
        start = subprocess.run(
            [
                tmux_bin,
                "-L",
                tmux_socket,
                "new-session",
                "-d",
                "-s",
                tmux_session,
                str(script_path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if start.returncode:
            detail = (start.stderr or start.stdout or "").strip()
            _record_agent_tmux_job(
                self.config,
                run_id,
                {"status": "start-failed", "error": detail},
            )
            raise SlackbotError(
                f"tmux Codex session failed to start: {detail}"
            )
        try:
            while not status_path.exists():
                time.sleep(0.5)
            status_text = status_path.read_text(
                encoding="utf-8",
                errors="replace",
            ).strip()
            returncode = int(status_text or "1")
            last_message = _read_optional_text(output_path)
            stdout = _read_optional_text(stdout_path)
            stderr = _read_optional_text(stderr_path)
            result = {
                "returncode": returncode,
                "stdout": stdout,
                "stderr": stderr,
                "last_message": last_message,
                "session_id": extract_codex_session_id(stdout),
                "tmux_session": tmux_session,
                "tmux_socket": tmux_socket,
                "run_dir": str(run_dir),
            }
            _record_agent_tmux_job(
                self.config,
                run_id,
                {
                    "status": "finished",
                    "returncode": returncode,
                    "codex_session_id": result["session_id"],
                    "finished_at": utcnow_iso(),
                },
            )
            return result
        finally:
            try:
                script_path.unlink()
            except OSError:
                pass


def safe_tmux_session_name(value: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9_.:-]+", "-", value).strip("-")
    if not candidate:
        candidate = f"codex-{uuid.uuid4().hex[:8]}"
    return candidate[:120]


def codex_tmux_bin(config: SlackbotConfig) -> str:
    value = (
        env_lookup(config.profile, config.env, "CODEX_TMUX_BIN")
        or env_lookup(config.profile, config.env, "CONSOLE_TMUX_BIN")
        or "tmux"
    )
    resolved = shutil.which(value)
    return resolved or value


def codex_tmux_socket(config: SlackbotConfig) -> str:
    return (
        env_lookup(config.profile, config.env, "CODEX_TMUX_SOCKET")
        or env_lookup(config.profile, config.env, "CONSOLE_TMUX_SOCKET")
        or DEFAULT_TMUX_SOCKET
    )


def render_tmux_codex_script(
    *,
    command: list[str],
    env: dict[str, str],
    prompt_path: Path,
    stdout_path: Path,
    stderr_path: Path,
    status_path: Path,
    workdir: Path,
) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "set +e",
        f"cd {shlex.quote(str(workdir))} || exit 111",
    ]
    for name, value in sorted(env.items()):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            continue
        lines.append(f"export {name}={quote_env_value(str(value))}")
    lines.extend(
        [
            "",
            (
                f"{shlex.join(command)} "
                f"< {shlex.quote(str(prompt_path))} "
                f"> {shlex.quote(str(stdout_path))} "
                f"2> {shlex.quote(str(stderr_path))}"
            ),
            "status=$?",
            f"printf '%s\\n' \"$status\" > {shlex.quote(str(status_path))}",
            "printf '\\nKickle Codex job finished with status %s.\\n' "
            '"$status"',
            "printf 'This tmux session is left open for inspection.\\n'",
            "exec bash -l",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_optional_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def _record_agent_tmux_job(
    config: SlackbotConfig,
    job_id: str,
    values: dict[str, Any],
) -> None:
    with AGENT_TMUX_LOCK, locked_json_path(config.paths.agent_tmux_jobs_path):
        data = read_json(config.paths.agent_tmux_jobs_path, {})
        current = data.get(job_id)
        if not isinstance(current, dict):
            current = {}
        current.update(values)
        current.setdefault("id", job_id)
        current["updated_at"] = utcnow_iso()
        data[job_id] = current
        write_json_atomic(config.paths.agent_tmux_jobs_path, data)


def extract_codex_session_id(jsonl: str) -> str | None:
    for line in jsonl.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("type") or "")
        direct_session_id = event.get("session_id")
        if direct_session_id:
            return str(direct_session_id)
        payload = event.get("payload")
        if isinstance(payload, dict):
            if event_type in CODEX_SESSION_EVENT_TYPES:
                payload_session_id = payload.get("session_id")
                if payload_session_id:
                    return str(payload_session_id)
                payload_id = payload.get("id")
                if payload_id:
                    return str(payload_id)
    return None


def chunk_text(text: str, max_chars: int = MAX_SLACK_TEXT_CHARS) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    chunks = []
    remaining = text
    while remaining:
        remaining = remaining.lstrip()
        if not remaining:
            break
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break
        split_at = remaining.rfind(" ", 0, max_chars + 1)
        split_at = max(split_at, remaining.rfind("\n", 0, max_chars + 1))
        if split_at <= 0:
            split_at = max_chars
        chunk = remaining[:split_at].rstrip() or remaining[:max_chars]
        chunks.append(chunk)
        remaining = remaining[len(chunk) :].lstrip()
    return chunks


def slack_mention_user_id(token: str) -> str | None:
    match = re.match(r"^<@([^>|]+)(?:\|[^>]+)?>[,.!?:;]*$", token)
    if not match:
        return None
    return match.group(1)


def strip_bot_mentions(text: str, bot_user_id: str | None = None) -> str:
    removed = False

    def replace(match: re.Match[str]) -> str:
        nonlocal removed
        mention_user_id = match.group(1)
        if bot_user_id is None or mention_user_id == bot_user_id:
            removed = True
            return ""
        return match.group(0)

    pattern = r"<@([^>|]+)(?:\|[^>]+)?>[,.!?:;]*[ \t]*"
    result = re.sub(pattern, replace, text)
    if not removed:
        return text.strip()
    result = result.strip(" \t")
    if result.startswith("\r\n"):
        result = result[2:]
    elif result.startswith("\n"):
        result = result[1:]
    return result.rstrip()


class SlackSocketBot:
    def __init__(
        self,
        config: SlackbotConfig,
        out: Callable[[str], None] | None = None,
    ):
        self.config = config
        self.out = out
        self.executor = ThreadPoolExecutor(max_workers=config.worker_count)
        self.work_semaphore = threading.Semaphore(
            config.worker_count * WORK_QUEUE_DEPTH_PER_WORKER
        )
        self.user_config_semaphore = threading.Semaphore(
            min(USER_CONFIG_DISPATCH_LIMIT, max(1, config.worker_count))
        )
        self.recent_events: dict[str, float] = {}
        self.recent_events_lock = threading.Lock()
        self.conversation_locks: OrderedDict[str, ConversationLockEntry] = (
            OrderedDict()
        )
        self.conversation_locks_lock = threading.Lock()
        self.active_ingress: set[str] = set()
        self.active_ingress_lock = threading.Lock()
        self.conversations = ConversationStore(config.paths.sessions_path)
        self.runner = CodexRunner(config)
        self.bot_user_id: str | None = None
        self.team_name: str | None = None
        self.team_domain: str | None = config.slack_domain
        if (
            config.allowed_channel_ids
            and not config.allowed_dm_user_ids
            and not config.allowed_user_ids
        ):
            emit(
                self.out,
                "warning: SLACKBOT_ALLOWED_CHANNEL_IDS does not authorize "
                "DMs; set SLACKBOT_ALLOWED_DM_USER_IDS or "
                "SLACKBOT_ALLOWED_USER_IDS for DM access.",
            )

    def _import_slack_sdk(self):
        try:
            from slack_sdk import WebClient
            from slack_sdk.socket_mode import SocketModeClient
            from slack_sdk.socket_mode.response import SocketModeResponse
        except ImportError as exc:
            raise SlackbotError(
                "slack-sdk is required for slackbot-run. Install "
                "`sneeze[slackbot]` or the plugin extra that includes it."
            ) from exc
        return WebClient, SocketModeClient, SocketModeResponse

    def run(self, max_runtime_seconds: int | None = None) -> None:
        if not self.config.bot_token or not self.config.app_token:
            raise SlackbotError("Slack bot and app tokens are required")
        WebClient, SocketModeClient, _ = self._import_slack_sdk()
        auth_payload = slack_api_post(
            self.config.bot_token,
            "auth.test",
            {},
        )
        slack_api_post(
            self.config.app_token,
            "apps.connections.open",
            {},
        )
        self.bot_user_id = str(auth_payload.get("user_id") or "")
        self.team_name = auth_payload.get("team") or None
        auth_url = str(auth_payload.get("url") or "")
        auth_domain = urllib.parse.urlparse(auth_url).netloc
        if auth_domain:
            self.team_domain = auth_domain
        web_client = WebClient(token=self.config.bot_token)
        client = SocketModeClient(
            app_token=self.config.app_token,
            web_client=web_client,
        )
        client.socket_mode_request_listeners.append(self._handle_request)
        client.connect()
        stop_event = threading.Event()
        previous_handlers = {}

        def request_stop(signum, frame):
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                previous_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, request_stop)
            except ValueError:
                pass
        emit(
            self.out,
            "Slack Socket Mode connected: "
            f"{self.team_name or 'unknown team'} "
            f"({self.team_domain or 'unknown domain'})",
        )
        if not (
            self.config.allowed_dm_user_ids
            or self.config.allowed_user_ids
            or self.config.allowed_channel_ids
        ):
            emit(
                self.out,
                "No Slackbot allowed users or channels configured; "
                "direct messages will still be accepted, but app mentions, "
                "slash commands, and group DMs will be rejected.",
            )
        deadline = (
            time.monotonic() + max_runtime_seconds
            if max_runtime_seconds
            else None
        )
        try:
            while not stop_event.is_set() and (
                deadline is None or time.monotonic() < deadline
            ):
                self.drain_ingress()
                stop_event.wait(1.0)
        finally:
            for sig, previous in previous_handlers.items():
                try:
                    signal.signal(sig, previous)
                except ValueError:
                    pass
            try:
                client.close()
            except Exception:
                pass
            self.executor.shutdown(wait=True, cancel_futures=False)
            emit(self.out, "Slack bot stopped")

    def _handle_request(self, client: Any, request: Any) -> None:
        _, _, SocketModeResponse = self._import_slack_sdk()
        request_type = getattr(request, "type", None)
        payload = getattr(request, "payload", {}) or {}
        user_config_submission = self._user_config_view_submission_payload(
            request_type,
            payload,
        )
        is_view_submission = (
            self._view_submission_payload(
                request_type,
                payload,
            )
            is not None
        )
        work_acquired = False
        ack_payload = self._socket_ack_payload(
            request_type,
            payload,
            user_config_submission=user_config_submission,
        )

        def send_response(response_payload: dict[str, Any] | None) -> None:
            try:
                response_kwargs = {"envelope_id": request.envelope_id}
                if response_payload is not None:
                    response_kwargs["payload"] = response_payload
                client.send_socket_mode_response(
                    SocketModeResponse(**response_kwargs)
                )
            except Exception:
                pass

        if user_config_submission is not None and ack_payload is not None:
            send_response(ack_payload)
            return
        if self._immediate_user_config_request(request_type, payload):
            send_response(None)
            self._start_immediate_user_config_dispatch(request_type, payload)
            return
        if user_config_submission is not None:
            work_acquired = self.work_semaphore.acquire(blocking=False)
            if not work_acquired:
                ack_payload = self._view_submission_error_payload(
                    "The bot is busy. Please submit the modal again.",
                    block_id=self._view_submission_error_block_id(
                        request_type,
                        payload,
                        preferred_block_id=USER_CONFIG_CODEX_BLOCK_ID,
                    ),
                )
                emit(
                    self.out,
                    "Dropping Slack view submission: worker queue is full",
                )
                send_response(ack_payload)
                return
            send_response(ack_payload)
        else:
            work_acquired = self.work_semaphore.acquire(blocking=False)
            if not work_acquired:
                if is_view_submission:
                    ack_payload = self._view_submission_error_payload(
                        "The bot is busy. Please submit the modal again.",
                        block_id=self._view_submission_error_block_id(
                            request_type,
                            payload,
                        ),
                    )
                send_response(ack_payload)
                emit(self.out, "Dropping Slack request: worker queue is full")
                return
            send_response(ack_payload)
        try:
            self.executor.submit(
                self._dispatch_payload_with_semaphore_release,
                request_type,
                payload,
            )
        except Exception:
            self.work_semaphore.release()
            raise

    def _immediate_user_config_request(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> bool:
        payload_type = request_type or payload.get("type")
        if payload_type == "slash_commands":
            command_name = normalize_command_name(
                str(payload.get("command") or "").strip(),
                self.config.command_name,
            )
            return (
                command_name == self.config.command_name
                and user_config_mode_from_text(payload.get("text") or "")
                is not None
            )
        if payload_type not in {"interactive", "block_actions"}:
            return False
        interactive_payload = self._normalize_interactive_payload(payload)
        if interactive_payload.get("type") != "block_actions":
            return False
        return any(
            action.get("action_id") == USER_CONFIG_OPEN_ACTION_ID
            for action in interactive_payload.get("actions") or []
        )

    def _start_immediate_user_config_dispatch(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> None:
        if not self.user_config_semaphore.acquire(blocking=False):
            emit(
                self.out,
                "Dropping Slack user-config open: dispatch queue is full",
            )
            return
        thread = threading.Thread(
            target=self._dispatch_user_config_with_semaphore_release,
            args=(request_type, payload),
            name=f"{self.config.profile.app_slug}-user-config-open",
            daemon=True,
        )
        try:
            thread.start()
        except Exception as exc:
            self.user_config_semaphore.release()
            emit(
                self.out,
                f"Slack user-config dispatch thread failed to start: {exc}",
            )

    def _dispatch_user_config_with_semaphore_release(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> None:
        try:
            self._dispatch_payload_safely(request_type, payload)
        finally:
            self.user_config_semaphore.release()

    def _dispatch_payload_safely(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> None:
        try:
            self._dispatch_payload(request_type, payload)
        except Exception as exc:
            emit(self.out, f"Slack request dispatch failed: {exc}")

    def _socket_ack_payload(
        self,
        request_type: str | None,
        payload: dict[str, Any],
        *,
        user_config_submission: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        interactive_payload = user_config_submission
        if interactive_payload is None:
            interactive_payload = self._user_config_view_submission_payload(
                request_type,
                payload,
            )
        if interactive_payload is None:
            return None
        view = interactive_payload.get("view") or {}
        metadata = _view_private_metadata(view)
        user_id = str((interactive_payload.get("user") or {}).get("id") or "")
        channel_id = metadata.get("channel_id") or None
        channel_type = "im" if str(channel_id or "").startswith("D") else None
        if self._is_user_config_authorized(
            user_id,
            channel_id,
            channel_type=channel_type,
        ):
            return None
        return self._view_submission_error_payload(
            "This Slackbot is not configured to accept that request."
        )

    def _user_config_view_submission_payload(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        interactive_payload = self._view_submission_payload(
            request_type,
            payload,
        )
        if interactive_payload is None:
            return None
        view = interactive_payload.get("view") or {}
        if view.get("callback_id") != USER_CONFIG_MODAL_CALLBACK_ID:
            return None
        return interactive_payload

    def _view_submission_payload(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        payload_type = request_type or payload.get("type")
        if payload_type not in {"interactive", "view_submission"}:
            return None
        interactive_payload = self._normalize_interactive_payload(payload)
        if interactive_payload.get("type") != "view_submission":
            return None
        return interactive_payload

    def _view_submission_error_block_id(
        self,
        request_type: str | None,
        payload: dict[str, Any],
        *,
        preferred_block_id: str | None = None,
    ) -> str:
        interactive_payload = self._view_submission_payload(
            request_type,
            payload,
        )
        view = (interactive_payload or {}).get("view") or {}
        block_ids = []
        for block in view.get("blocks") or []:
            block_id = block.get("block_id")
            if block_id:
                block_ids.append(str(block_id))
        if preferred_block_id and preferred_block_id in block_ids:
            return preferred_block_id
        if block_ids:
            return block_ids[0]
        return preferred_block_id or USER_CONFIG_CODEX_BLOCK_ID

    def _view_submission_error_payload(
        self,
        message: str,
        *,
        block_id: str = USER_CONFIG_CODEX_BLOCK_ID,
    ) -> dict[str, Any]:
        return {
            "response_action": "errors",
            "errors": {
                block_id: message,
            },
        }

    def _dispatch_payload_with_semaphore_release(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> None:
        # _handle_request acquires this slot before submitting the worker.
        try:
            self._dispatch_payload_safely(request_type, payload)
        finally:
            self.work_semaphore.release()

    def _dispatch_payload(
        self,
        request_type: str | None,
        payload: dict[str, Any],
    ) -> None:
        payload_type = request_type or payload.get("type")
        if payload_type == "events_api":
            event = payload.get("event") or {}
            event_id = payload.get("event_id") or self._event_dedupe_key(
                event
            )
            if self._seen_recently(event_id):
                return
            self._handle_event(event)
        elif payload_type == "slash_commands":
            dedupe_key = payload.get("trigger_id") or self._slash_dedupe_key(
                payload
            )
            if self._seen_recently(f"slash:{dedupe_key}"):
                return
            self._handle_slash(payload)
        elif payload_type in (
            "interactive",
            "block_actions",
            "view_submission",
        ):
            interactive_payload = self._normalize_interactive_payload(payload)
            dedupe_key = self._interactive_dedupe_key(interactive_payload)
            if self._seen_recently(f"interactive:{dedupe_key}"):
                return
            self._handle_interactive(interactive_payload)

    def _event_dedupe_key(self, event: dict[str, Any]) -> str:
        digest = hashlib.sha256(
            json.dumps(event, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        return f"event:{digest}"

    def _slash_dedupe_key(self, payload: dict[str, Any]) -> str:
        parts = {
            "channel_id": payload.get("channel_id"),
            "command": payload.get("command"),
            "text": payload.get("text"),
            "user_id": payload.get("user_id"),
        }
        digest = hashlib.sha256(
            json.dumps(parts, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        return f"fallback:{digest}"

    def _interactive_dedupe_key(self, payload: dict[str, Any]) -> str:
        view = payload.get("view") or {}
        view_state = (view.get("state") or {}).get("values")
        parts = {
            "actions": [
                {
                    "action_id": action.get("action_id"),
                    "action_ts": action.get("action_ts"),
                    "value": action.get("value"),
                }
                for action in payload.get("actions") or []
            ],
            "callback_id": view.get("callback_id"),
            "trigger_id": payload.get("trigger_id"),
            "user_id": (payload.get("user") or {}).get("id"),
            "view_hash": view.get("hash"),
            "view_id": view.get("id"),
            "view_state": view_state,
        }
        digest = hashlib.sha256(
            json.dumps(parts, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]
        return f"fallback:{digest}"

    def _normalize_interactive_payload(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if payload.get("type") != "interactive":
            return payload
        nested = payload.get("payload")
        if isinstance(nested, dict):
            return nested
        if isinstance(nested, str):
            try:
                parsed = json.loads(nested)
            except json.JSONDecodeError:
                return payload
            if isinstance(parsed, dict):
                return parsed
        return payload

    def _seen_recently(self, key: str | None) -> bool:
        if not key:
            return False
        with self.recent_events_lock:
            now = time.monotonic()
            self.recent_events = {
                item: ts
                for item, ts in self.recent_events.items()
                if now - ts < RECENT_EVENT_TTL_SECONDS
            }
            if key in self.recent_events:
                return True
            self.recent_events[key] = now
            return False

    def _handle_event(self, event: dict[str, Any]) -> None:
        event_type = event.get("type")
        channel_type = event.get("channel_type")
        if event_type not in ("app_mention", "message"):
            return
        if event_type == "message" and channel_type not in {"im", "mpim"}:
            return
        if event.get("subtype") is not None:
            return
        if event.get("bot_id"):
            return
        slack_user_id = str(event.get("user") or "").strip() or None
        if not slack_user_id:
            return
        if self.bot_user_id and slack_user_id == self.bot_user_id:
            return
        text = strip_bot_mentions(event.get("text") or "", self.bot_user_id)
        if not text:
            return
        channel = event.get("channel")
        if channel_type == "im":
            thread_ts = event.get("thread_ts") or None
        else:
            thread_ts = event.get("thread_ts") or event.get("ts")
        route = SlackbotRoute(
            channel_id=channel,
            dm_user_id=slack_user_id if channel_type == "im" else None,
            thread_ts=thread_ts,
        )
        if channel_type == "im" and not self._allows_early_dm_response(
            slack_user_id
        ):
            self._post_unauthorized(route)
            return
        if channel_type == "im" and self._maybe_handle_user_config(
            text,
            route,
        ):
            return
        if channel_type == "im" and self._maybe_handle_static_response(
            text,
            route,
        ):
            return
        if not self._is_authorized(
            slack_user_id,
            channel,
            channel_type=channel_type,
        ):
            self._post_unauthorized(route)
            return
        if channel_type != "im" and self._maybe_handle_static_response(
            text,
            route,
        ):
            return
        if self._maybe_handle_agent_tmux(text, route):
            return
        self._run_codex_for_route(
            text,
            route,
            execution_mode=self._default_execution_mode(),
            slack_user_id=slack_user_id,
        )

    def _handle_slash(self, payload: dict[str, Any]) -> None:
        raw_command = str(payload.get("command") or "").strip()
        if not raw_command:
            return
        command_name = normalize_command_name(
            raw_command,
            self.config.command_name,
        )
        if command_name != self.config.command_name:
            return
        text = (payload.get("text") or "").strip()
        route = SlackbotRoute(
            channel_id=payload.get("channel_id"),
            thread_ts=payload.get("thread_ts") or None,
            response_url=payload.get("response_url") or None,
        )
        channel_type = (
            "im"
            if str(route.channel_id or "").startswith("D")
            or payload.get("channel_name") == "directmessage"
            else None
        )
        mode = user_config_mode_from_text(text)
        if mode:
            if not self._is_user_config_authorized(
                payload.get("user_id"),
                route.channel_id,
                channel_type=channel_type,
            ):
                self._post_unauthorized(route)
                return
            self._open_user_config_modal(payload, mode=mode)
            return
        if not self._is_authorized(
            payload.get("user_id"),
            route.channel_id,
            channel_type=channel_type,
        ):
            self._post_unauthorized(route)
            return
        if self._maybe_handle_static_response(text, route):
            return
        if not text:
            return
        self._run_codex_for_route(
            text,
            route,
            execution_mode=self._default_execution_mode(),
            slack_user_id=str(payload.get("user_id") or "").strip() or None,
        )

    def _handle_interactive(self, payload: dict[str, Any]) -> None:
        payload_type = payload.get("type")
        if payload_type == "block_actions":
            for action in payload.get("actions") or []:
                if action.get("action_id") != USER_CONFIG_OPEN_ACTION_ID:
                    continue
                user_id = str((payload.get("user") or {}).get("id") or "")
                channel_id = (payload.get("channel") or {}).get("id") or None
                channel_type = (
                    "im" if str(channel_id or "").startswith("D") else None
                )
                if not self._is_user_config_authorized(
                    user_id,
                    channel_id,
                    channel_type=channel_type,
                ):
                    self._post_unauthorized(
                        SlackbotRoute(
                            channel_id=channel_id,
                            dm_user_id=user_id,
                        )
                    )
                    return
                mode = normalize_user_config_mode(action.get("value"))
                self._open_user_config_modal(payload, mode=mode)
                return
        if payload_type == "view_submission":
            view = payload.get("view") or {}
            if view.get("callback_id") != USER_CONFIG_MODAL_CALLBACK_ID:
                return
            metadata = _view_private_metadata(view)
            user_id = str((payload.get("user") or {}).get("id") or "")
            channel_id = metadata.get("channel_id") or None
            channel_type = self._trusted_user_config_channel_type(
                user_id,
                channel_id,
            )
            if not self._is_user_config_authorized(
                user_id,
                channel_id,
                channel_type=channel_type,
            ):
                self._post_unauthorized(SlackbotRoute(dm_user_id=user_id))
                return
            try:
                summary = save_user_config_submission(self.config, payload)
            except Exception as exc:
                emit(self.out, f"Failed to save Slack config modal: {exc}")
                self._post_user_config_feedback(
                    user_id,
                    channel_id,
                    private_text=f"I could not save your config: {exc}",
                    fallback_text=(
                        "I could not save your config. Please try again."
                    ),
                )
                return
            self._post_user_config_feedback(
                user_id,
                channel_id,
                private_text=user_config_saved_message(summary),
                fallback_text="Config saved.",
            )

    def _post_user_config_feedback(
        self,
        user_id: str,
        channel_id: str | None,
        *,
        private_text: str,
        fallback_text: str,
    ) -> None:
        try:
            post_slack_message(
                self.config,
                SlackbotRoute(dm_user_id=user_id),
                private_text,
            )
            return
        except Exception as exc:
            emit(self.out, f"Failed to post Slack config DM feedback: {exc}")
        if not channel_id:
            return
        try:
            post_slack_message(
                self.config,
                SlackbotRoute(channel_id=channel_id),
                fallback_text,
            )
        except Exception as exc:
            emit(
                self.out,
                f"Failed to post Slack config channel feedback: {exc}",
            )

    def _is_user_config_authorized(
        self,
        user_id: str | None,
        channel_id: str | None,
        *,
        channel_type: str | None = None,
    ) -> bool:
        return self._is_authorized(
            user_id,
            channel_id,
            channel_type=channel_type,
        )

    def _allows_early_dm_response(self, user_id: str | None) -> bool:
        return self._is_authorized(user_id, None, channel_type="im")

    def _trusted_user_config_channel_type(
        self,
        user_id: str | None,
        channel_id: str | None,
    ) -> str | None:
        if not str(channel_id or "").startswith("D"):
            return None
        if not (
            self.config.allowed_dm_user_ids
            or self.config.allowed_user_ids
            or self.config.allowed_channel_ids
        ):
            return "im"
        if user_id and (
            user_id in self.config.allowed_dm_user_ids
            or user_id in self.config.allowed_user_ids
        ):
            return "im"
        return None

    def _maybe_handle_static_response(
        self,
        text: str,
        route: SlackbotRoute,
    ) -> bool:
        response = static_response_text_from_profile(
            self.config.profile,
            text,
        )
        if response is None:
            return False
        post_slack_message(self.config, route, response)
        return True

    def _default_execution_mode(self) -> str:
        return normalize_execution_mode(
            env_lookup(
                self.config.profile,
                self.config.env,
                "SLACKBOT_EXECUTION_MODE",
                "raw",
            )
        )

    def _open_user_config_modal(
        self,
        payload: dict[str, Any],
        *,
        mode: str = "config",
    ) -> None:
        trigger_id = str(payload.get("trigger_id") or "").strip()
        user_id = str((payload.get("user") or {}).get("id") or "").strip()
        if not user_id:
            user_id = str(payload.get("user_id") or "").strip()
        channel_id = (
            payload.get("channel_id")
            or (payload.get("channel") or {}).get("id")
            or (payload.get("container") or {}).get("channel_id")
            or None
        )
        response_url = payload.get("response_url") or None
        route = SlackbotRoute(
            channel_id=channel_id,
            dm_user_id=user_id,
            response_url=response_url,
        )
        if not trigger_id or not self.config.bot_token:
            post_user_config_launcher(self.config, route, mode=mode)
            return
        try:
            slack_api_post(
                self.config.bot_token,
                "views.open",
                {
                    "trigger_id": trigger_id,
                    "view": build_user_config_view(
                        self.config,
                        user_id=user_id,
                        channel_id=channel_id,
                        mode=mode,
                    ),
                },
            )
        except Exception as exc:
            post_slack_message(
                self.config,
                route,
                f"I could not open the config modal: {exc}",
            )

    def _is_authorized(
        self,
        user_id: str | None,
        channel_id: str | None,
        *,
        channel_type: str | None = None,
    ) -> bool:
        allowed_dm_users = self.config.allowed_dm_user_ids
        allowed_users = self.config.allowed_user_ids
        allowed_channels = self.config.allowed_channel_ids
        if channel_type == "im":
            if allowed_dm_users or allowed_users:
                return bool(
                    (allowed_dm_users and user_id in allowed_dm_users)
                    or (allowed_users and user_id in allowed_users)
                )
            return not allowed_channels
        if not allowed_users and not allowed_channels:
            return False
        return bool(
            (allowed_users and user_id in allowed_users)
            or (allowed_channels and channel_id in allowed_channels)
        )

    def _post_unauthorized(self, route: SlackbotRoute) -> None:
        try:
            post_slack_message(
                self.config,
                route,
                (
                    "This Slackbot is not configured to accept that request. "
                    "Set SLACKBOT_ALLOWED_DM_USER_IDS, "
                    "SLACKBOT_ALLOWED_USER_IDS, or "
                    "SLACKBOT_ALLOWED_CHANNEL_IDS in its env file."
                ),
            )
        except Exception as exc:
            emit(self.out, f"Failed to post authorization failure: {exc}")

    @contextmanager
    def _conversation_lock(self, key: str):
        with self.conversation_locks_lock:
            entry = self.conversation_locks.get(key)
            if entry is None:
                entry = ConversationLockEntry()
                self.conversation_locks[key] = entry
            else:
                self.conversation_locks.move_to_end(key)
            entry.refs += 1
            lock = entry.lock
            self._prune_conversation_locks(current_key=key)
        try:
            with lock:
                yield
        finally:
            with self.conversation_locks_lock:
                current = self.conversation_locks.get(key)
                if current is entry:
                    entry.refs = max(0, entry.refs - 1)
                self._prune_conversation_locks()

    def _prune_conversation_locks(
        self,
        *,
        current_key: str | None = None,
    ) -> None:
        while len(self.conversation_locks) > MAX_CONVERSATION_LOCKS:
            evicted = False
            for stale_key, stale_entry in list(
                self.conversation_locks.items()
            ):
                if stale_key == current_key:
                    continue
                if stale_entry.refs == 0:
                    self.conversation_locks.pop(stale_key, None)
                    evicted = True
                    break
            if not evicted:
                break

    def _conversation_key(
        self,
        route: SlackbotRoute,
        *,
        project: str | None = None,
        session: str | None = None,
        slack_user_id: str | None = None,
    ) -> str | None:
        prefix = ""
        if project:
            prefix += f"project:{project}:"
        if session:
            if slack_user_id:
                prefix += f"user:{slack_user_id}:"
            return f"{prefix}session:{session}"
        if slack_user_id and not route.dm_user_id:
            prefix += f"user:{slack_user_id}:"
        if route.dm_user_id and route.channel_id:
            return f"{prefix}dm:{route.channel_id}:{route.dm_user_id}"
        if route.dm_user_id and not route.thread_ts:
            return f"{prefix}dm:{route.dm_user_id}"
        if route.channel_id and route.thread_ts:
            return f"{prefix}slack:{route.channel_id}:{route.thread_ts}"
        return None

    def _run_codex_for_route(
        self,
        prompt: str,
        route: SlackbotRoute,
        *,
        execution_mode: str = "raw",
        project: str | None = None,
        session: str | None = None,
        slack_user_id: str | None = None,
        scrub_secret_env: bool = False,
        conversation_route: SlackbotRoute | None = None,
        user_config_error_callback: Callable[[Exception], None] | None = None,
        raise_user_config_errors: bool = False,
    ) -> None:
        execution_mode = normalize_execution_mode(execution_mode)
        user_config_error: Exception | None = None
        reply_body = None
        user_secret_env: dict[str, str] = {}
        route_for_conversation = conversation_route or route

        def record_user_config_error(exc: Exception) -> None:
            nonlocal user_config_error
            if user_config_error is None:
                user_config_error = exc
            if user_config_error_callback:
                user_config_error_callback(exc)

        if slack_user_id:
            try:
                slack_user_id = safe_slack_storage_id(
                    slack_user_id,
                    "Slack user ID",
                )
            except Exception as exc:
                emit(
                    self.out,
                    f"Failed to normalize user-scoped Slack ID: {exc}",
                )
                reply_body = (
                    "I could not verify your Codex-backed work session "
                    "settings, so I did not start a session."
                )
                record_user_config_error(exc)
                slack_user_id = None
            if reply_body is None:
                try:
                    codex_enabled = user_codex_sessions_enabled(
                        self.config.paths, slack_user_id
                    )
                except Exception as exc:
                    emit(
                        self.out,
                        "Failed to verify user-scoped Slack config: "
                        f"{exc}",
                    )
                    reply_body = (
                        "I could not verify your Codex-backed work "
                        "session settings, so I did not start a session."
                    )
                    record_user_config_error(exc)
                else:
                    if not codex_enabled:
                        reply_body = (
                            "Codex-backed work sessions are disabled for "
                            "your profile. Use the config modal to "
                            "re-enable them."
                        )
            if reply_body is None:
                try:
                    user_secret_env = read_user_secret_env(
                        self.config.paths,
                        slack_user_id,
                        self.config.profile,
                    )
                except Exception as exc:
                    emit(
                        self.out,
                        f"Failed to load user-scoped Slack secrets: {exc}",
                    )
                    reply_body = (
                        "I could not load your user-scoped Slackbot "
                        "secrets, so I did not start a Codex session."
                    )
                    record_user_config_error(exc)
        if route.dm_user_id and reply_body is None:
            try:
                dm_user_id = safe_slack_storage_id(
                    route.dm_user_id,
                    "Slack user ID",
                )
            except Exception as exc:
                emit(
                    self.out,
                    f"Failed to normalize Slack DM user ID: {exc}",
                )
                reply_body = (
                    "I could not verify your Codex-backed work session "
                    "settings, so I did not start a session."
                )
                record_user_config_error(exc)
                dm_user_id = None
            if dm_user_id != route.dm_user_id:
                route = replace(route, dm_user_id=dm_user_id)
        should_run_codex = reply_body is None
        placeholder = None
        placeholder_ts = None
        placeholder_channel = None
        if should_run_codex:
            try:
                placeholder = post_slack_message(
                    self.config, route, slackbot_working_text(self.config)
                )
            except Exception as exc:
                emit(self.out, f"Failed to post Slack placeholder: {exc}")
            else:
                placeholder_ts = (
                    placeholder.get("ts") if placeholder else None
                )
                placeholder_channel = (
                    placeholder.get("channel") if placeholder else None
                )
        if placeholder_ts and not route.thread_ts:
            route = SlackbotRoute(
                channel_id=route.channel_id or placeholder_channel,
                dm_user_id=route.dm_user_id,
                mention_user_ids=route.mention_user_ids,
                thread_ts=placeholder_ts,
                response_url=route.response_url,
            )
            if conversation_route is None:
                route_for_conversation = route
            elif not conversation_route.thread_ts:
                route_for_conversation = replace(
                    conversation_route,
                    channel_id=(
                        conversation_route.channel_id
                        or route.channel_id
                        or placeholder_channel
                    ),
                    thread_ts=placeholder_ts,
                )
        conversation_key = (
            self._conversation_key(
                route_for_conversation,
                project=project,
                session=session,
                slack_user_id=slack_user_id,
            )
            if should_run_codex
            else None
        )
        try:
            lock_context = (
                self._conversation_lock(conversation_key)
                if conversation_key
                else nullcontext()
            )
            with lock_context:
                record = (
                    self.conversations.get(conversation_key)
                    if should_run_codex and conversation_key
                    else None
                )
                if should_run_codex:
                    run_kwargs = {}
                    if slack_user_id or scrub_secret_env:
                        run_kwargs["scrub_secret_env"] = True
                    if scrub_secret_env or slack_user_id:
                        run_kwargs["use_scrub_allowlist"] = True
                    if user_secret_env:
                        run_kwargs["extra_env"] = user_secret_env
                    built_prompt = self._build_prompt(
                        prompt,
                        route,
                        project=project,
                    )
                    if execution_mode == "tmux":
                        result = self.runner.run_tmux(
                            built_prompt,
                            session_id=(record or {}).get("session_id"),
                            route=route,
                            project=project,
                            **run_kwargs,
                        )
                    else:
                        result = self.runner.run(
                            built_prompt,
                            session_id=(record or {}).get("session_id"),
                            **run_kwargs,
                        )
                    if conversation_key and result.get("session_id"):
                        self.conversations.set(
                            conversation_key,
                            {
                                "session_id": result["session_id"],
                                "project": project,
                                "route": route_to_dict(route),
                                "updated_at": utcnow_iso(),
                            },
                        )
                    reply_body = (
                        result["last_message"]
                        or result["stderr"]
                        or result["stdout"]
                    )
                    if result["returncode"]:
                        reply_body = (
                            f"Codex exited with {result['returncode']}.\n\n"
                            f"{reply_body}"
                        )
        except Exception as exc:
            if reply_body is None:
                reply_body = (
                    f"I hit an internal error while talking to Codex: {exc}"
                )
        reply_text = render_route_text(route, reply_body or "(no response)")
        chunks = chunk_text(reply_text)
        followup_route = SlackbotRoute(
            channel_id=route.channel_id,
            dm_user_id=route.dm_user_id,
            thread_ts=route.thread_ts,
            response_url=route.response_url,
        )
        # chunks already contains the rendered mention prefix on the first
        # message; follow-up posts avoid re-prefixing every chunk.
        update_channel = placeholder_channel or route.channel_id
        if placeholder_ts and update_channel:
            try:
                update_slack_message(
                    self.config,
                    channel=update_channel,
                    ts=placeholder_ts,
                    text=chunks[0],
                )
            except Exception as exc:
                response = post_slack_message(
                    self.config, followup_route, chunks[0]
                )
                if response and not followup_route.thread_ts:
                    followup_route = SlackbotRoute(
                        channel_id=followup_route.channel_id
                        or response.get("channel"),
                        dm_user_id=followup_route.dm_user_id,
                        thread_ts=response.get("ts"),
                        response_url=followup_route.response_url,
                    )
                emit(
                    self.out,
                    "Failed to update Slack placeholder "
                    f"{update_channel}:{placeholder_ts}; posted follow-up: "
                    f"{exc}",
                )
        else:
            response = post_slack_message(
                self.config, followup_route, chunks[0]
            )
            if response and not followup_route.thread_ts:
                followup_route = SlackbotRoute(
                    channel_id=followup_route.channel_id
                    or response.get("channel"),
                    dm_user_id=followup_route.dm_user_id,
                    thread_ts=response.get("ts"),
                    response_url=followup_route.response_url,
                )
        for chunk in chunks[1:]:
            try:
                post_slack_message(self.config, followup_route, chunk)
            except Exception as exc:
                emit(self.out, f"Failed to post Slack response chunk: {exc}")
        if raise_user_config_errors and user_config_error is not None:
            raise user_config_error

    def _maybe_handle_user_config(
        self,
        text: str,
        route: SlackbotRoute,
    ) -> bool:
        mode = user_config_mode_from_text(text)
        if not mode:
            return False
        post_user_config_launcher(self.config, route, mode=mode)
        return True

    def _maybe_handle_agent_tmux(
        self,
        text: str,
        route: SlackbotRoute,
    ) -> bool:
        if not route.channel_id or not route.thread_ts:
            return False
        tokens = text.strip().split()
        if not tokens or tokens[0].lower() != "tmux":
            return False
        if len(tokens) < 2:
            return False
        command = tokens[1].lower()
        if command == "use" and len(tokens) == 4:
            binding = bind_agent_tmux_thread(
                self.config.profile,
                runtime_root=self.config.paths.runtime_root,
                env_path=self.config.paths.env_path,
                state_dir=self.config.paths.state_dir,
                system_prompt_path=self.config.paths.system_prompt_path,
                unit_name=self.config.paths.unit_name,
                channel_id=route.channel_id,
                thread_ts=route.thread_ts,
                host=tokens[2],
                tmux_session=tokens[3],
            )
            post_slack_message(
                self.config,
                route,
                (
                    "Bound this thread to "
                    f"{binding['host']}:{binding['tmux_session']}."
                ),
            )
            return True
        if command in ("st", "status") and len(tokens) == 2:
            binding = query_agent_tmux_thread(
                self.config.profile,
                runtime_root=self.config.paths.runtime_root,
                env_path=self.config.paths.env_path,
                state_dir=self.config.paths.state_dir,
                system_prompt_path=self.config.paths.system_prompt_path,
                unit_name=self.config.paths.unit_name,
                channel_id=route.channel_id,
                thread_ts=route.thread_ts,
            )
            body = json.dumps(binding or {}, indent=2, sort_keys=True)
            post_slack_message(self.config, route, body)
            return True
        if command == "jobs" and len(tokens) == 2:
            jobs = list_agent_tmux_jobs(
                self.config.profile,
                runtime_root=self.config.paths.runtime_root,
                env_path=self.config.paths.env_path,
                state_dir=self.config.paths.state_dir,
                system_prompt_path=self.config.paths.system_prompt_path,
                unit_name=self.config.paths.unit_name,
            )
            body = json.dumps(jobs, indent=2, sort_keys=True)
            post_slack_message(self.config, route, body)
            return True
        return False

    def _build_prompt(
        self,
        prompt: str,
        route: SlackbotRoute,
        *,
        project: str | None = None,
    ) -> str:
        try:
            primer = Path(self.config.paths.system_prompt_path).read_text(
                encoding="utf-8"
            )
        except FileNotFoundError:
            primer = self.config.profile.default_system_prompt
        route_info = json.dumps(route_to_dict(route), sort_keys=True)
        project_info = f"\nProject:\n{project}\n" if project else ""
        return (
            f"{primer}\n\n{SLACKBOT_CODEX_TRANSPORT_INSTRUCTIONS}\n"
            f"Slack route:\n{route_info}\n{project_info}\n"
            f"Request:\n{prompt}\n"
        )

    def drain_ingress(self) -> None:
        ingress = Path(self.config.paths.ingress_dir)
        if not ingress.exists():
            return
        processing = Path(self.config.paths.ingress_processing_dir)
        processing.mkdir(parents=True, exist_ok=True)
        with self.active_ingress_lock:
            active_ingress = set(self.active_ingress)
        for stale in sorted(processing.glob("*.json")):
            if str(stale) in active_ingress:
                continue
            try:
                age = time.time() - stale.stat().st_mtime
            except OSError as exc:
                emit(
                    self.out,
                    f"Failed to stat ingress file {stale}: {exc}",
                )
                continue
            if age < INGRESS_PROCESSING_STALE_SECONDS:
                continue
            try:
                stale.replace(ingress / stale.name)
            except OSError as exc:
                emit(
                    self.out,
                    f"Failed to reclaim ingress file {stale}: {exc}",
                )
        for path in sorted(ingress.glob("*.json")):
            claimed = processing / path.name
            try:
                path.replace(claimed)
            except FileNotFoundError:
                continue
            try:
                payload = read_json(claimed, {})
                route = route_from_dict(payload.get("route") or {})
                kind = payload.get("kind")
                text = payload.get("text") or ""
                if kind == "slack_message":
                    if text.strip():
                        if not self.work_semaphore.acquire(blocking=False):
                            claimed.replace(ingress / claimed.name)
                            emit(
                                self.out,
                                "Deferring ingress Slack message: "
                                "worker queue is full",
                            )
                            continue
                        with self.active_ingress_lock:
                            self.active_ingress.add(str(claimed))
                        try:
                            self.executor.submit(
                                self._run_ingress_slack_message_safe,
                                claimed,
                                route,
                                text,
                            )
                        except Exception:
                            with self.active_ingress_lock:
                                self.active_ingress.discard(str(claimed))
                            self.work_semaphore.release()
                            claimed.replace(ingress / claimed.name)
                            raise
                        continue
                elif kind == "codex_prompt":
                    if "system_scoped" not in payload:
                        system_scoped = payload.get("slack_user_id") is None
                    else:
                        system_scoped = parse_ingress_system_scoped(
                            payload.get("system_scoped")
                        )
                    slack_user_id = None
                    if not system_scoped:
                        slack_user_id = payload.get("slack_user_id") or None
                        if not slack_user_id and route.dm_user_id:
                            # Older DM ingress files implied user scope from
                            # the delivery route. Treat them as system-scoped
                            # instead of loading the recipient's secrets.
                            system_scoped = True
                    if not slack_user_id and not system_scoped:
                        raise SlackbotError(
                            "Codex ingress is missing Slack user ID; "
                            "set system_scoped for non-user jobs"
                        )
                    conversation_route = None
                    if system_scoped and route.dm_user_id:
                        conversation_route = replace(route, dm_user_id=None)
                    if not self.work_semaphore.acquire(blocking=False):
                        claimed.replace(ingress / claimed.name)
                        emit(
                            self.out,
                            "Deferring ingress Codex prompt: "
                            "worker queue is full",
                        )
                        continue
                    with self.active_ingress_lock:
                        self.active_ingress.add(str(claimed))
                    try:
                        self.executor.submit(
                            self._run_ingress_codex_safe,
                            claimed,
                            text,
                            route,
                            payload.get("execution_mode") or "raw",
                            payload.get("project") or None,
                            payload.get("session") or None,
                            slack_user_id,
                            conversation_route,
                        )
                    except Exception:
                        with self.active_ingress_lock:
                            self.active_ingress.discard(str(claimed))
                        self.work_semaphore.release()
                        claimed.replace(ingress / claimed.name)
                        raise
                    continue
                else:
                    raise SlackbotError(f"unknown ingress kind: {kind}")
            except Exception as exc:
                if not claimed.exists():
                    continue
                target = (
                    Path(self.config.paths.ingress_error_dir) / claimed.name
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                claimed.replace(target)
                write_text(str(target) + ".error", str(exc) + "\n")
            else:
                target = (
                    Path(self.config.paths.ingress_done_dir) / claimed.name
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                claimed.replace(target)

    def _run_ingress_slack_message_safe(
        self,
        claimed: Path,
        route: SlackbotRoute,
        text: str,
    ) -> None:
        try:
            post_slack_message(self.config, route, text)
        except Exception as exc:
            target = Path(self.config.paths.ingress_error_dir) / claimed.name
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                claimed.replace(target)
            except FileNotFoundError:
                pass
            write_text(str(target) + ".error", str(exc) + "\n")
        else:
            target = Path(self.config.paths.ingress_done_dir) / claimed.name
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                claimed.replace(target)
            except FileNotFoundError:
                pass
        finally:
            with self.active_ingress_lock:
                self.active_ingress.discard(str(claimed))
            self.work_semaphore.release()

    def _run_ingress_codex_safe(
        self,
        claimed: Path,
        text: str,
        route: SlackbotRoute,
        execution_mode: str,
        project: str | None,
        session: str | None,
        slack_user_id: str | None,
        conversation_route: SlackbotRoute | None,
    ) -> None:
        user_config_errors: list[str] = []

        def write_user_config_error_sidecar(target: Path) -> None:
            if user_config_errors:
                write_text(
                    str(target) + ".user-config-error",
                    "\n".join(user_config_errors) + "\n",
                )

        try:
            self._run_codex_for_route(
                text,
                route,
                execution_mode=execution_mode,
                project=project,
                session=session,
                slack_user_id=slack_user_id,
                scrub_secret_env=True,
                conversation_route=conversation_route,
                user_config_error_callback=(
                    lambda exc: user_config_errors.append(str(exc))
                ),
                raise_user_config_errors=True,
            )
        except Exception as exc:
            target = Path(self.config.paths.ingress_error_dir) / claimed.name
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                claimed.replace(target)
            except FileNotFoundError:
                pass
            write_text(str(target) + ".error", str(exc) + "\n")
            write_user_config_error_sidecar(target)
        else:
            target = Path(self.config.paths.ingress_done_dir) / claimed.name
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                claimed.replace(target)
            except FileNotFoundError:
                pass
            write_user_config_error_sidecar(target)
        finally:
            with self.active_ingress_lock:
                self.active_ingress.discard(str(claimed))
            self.work_semaphore.release()


def run_slackbot(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
    slack_domain: str | None = None,
    app_id: str | None = None,
    client_id: str | None = None,
    bot_name: str | None = None,
    command_name: str | None = None,
    codex_bin: str | None = None,
    codex_mode: str | None = None,
    codex_model: str | None = None,
    codex_profile: str | None = None,
    codex_workdir: str | None = None,
    codex_extra_args: str | None = None,
    worker_count: int | None = None,
    mcp_server_url: str | None = None,
    team_config_path: str | None = None,
    max_runtime_seconds: int | None = None,
    out: Callable[[str], None] | None = None,
) -> None:
    config = load_config(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    overrides: dict[str, Any] = {}
    for key, value in (
        ("slack_domain", slack_domain),
        ("app_id", app_id),
        ("client_id", client_id),
        ("bot_name", bot_name),
        ("codex_bin", codex_bin),
        ("codex_model", codex_model),
        ("codex_profile", codex_profile),
        ("mcp_server_url", mcp_server_url),
        ("team_config_path", team_config_path),
    ):
        if value is not None:
            overrides[key] = value.strip() or None
    if command_name is not None:
        overrides["command_name"] = normalize_command_name(
            command_name,
            profile.default_command_name,
        )
    if codex_mode is not None:
        overrides["codex_mode"] = normalize_codex_mode(codex_mode)
    if codex_workdir is not None:
        overrides["codex_workdir"] = require_expanded_path(
            codex_workdir,
            "Codex workdir",
        )
    if codex_extra_args is not None:
        overrides["codex_extra_args"] = parse_extra_args(codex_extra_args)
    if worker_count:
        overrides["worker_count"] = worker_count
    if overrides:
        config = replace(config, **overrides)
    SlackSocketBot(config, out=out).run(
        max_runtime_seconds=max_runtime_seconds
    )


def bind_agent_tmux_thread(
    profile: SlackbotProfile,
    *,
    channel_id: str,
    thread_ts: str,
    host: str,
    tmux_session: str,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    key = f"{channel_id}:{thread_ts}"
    with AGENT_TMUX_LOCK, locked_json_path(paths.agent_tmux_bindings_path):
        data = read_json(paths.agent_tmux_bindings_path, {})
        data[key] = {
            "channel_id": channel_id,
            "thread_ts": thread_ts,
            "host": host,
            "tmux_session": tmux_session,
            "updated_at": utcnow_iso(),
        }
        write_json_atomic(paths.agent_tmux_bindings_path, data)
        return data[key]


def query_agent_tmux_thread(
    profile: SlackbotProfile,
    *,
    channel_id: str,
    thread_ts: str,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any] | None:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    with AGENT_TMUX_LOCK, locked_json_path(paths.agent_tmux_bindings_path):
        data = read_json(paths.agent_tmux_bindings_path, {})
        return data.get(f"{channel_id}:{thread_ts}")


def list_agent_tmux_jobs(
    profile: SlackbotProfile,
    *,
    runtime_root: str | None = None,
    env_path: str | None = None,
    state_dir: str | None = None,
    system_prompt_path: str | None = None,
    unit_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_paths(
        profile,
        runtime_root=runtime_root,
        env_path=env_path,
        state_dir=state_dir,
        system_prompt_path=system_prompt_path,
        unit_name=unit_name,
    )
    with AGENT_TMUX_LOCK, locked_json_path(paths.agent_tmux_jobs_path):
        return read_json(paths.agent_tmux_jobs_path, {})
