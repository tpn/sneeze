from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from .command import CommandError
from .invariant import PositiveIntegerInvariant, StringInvariant

TMUX_ACTIONS = ("start", "stop", "restart", "tmux-status", "attach", "logs")
TAIL_BLOCK_SIZE = 8192


def resolve_executable(value: str | None, *names: str) -> str | None:
    if value:
        return value
    for name in names:
        resolved = shutil.which(name)
        if resolved:
            return resolved
        for prefix in _candidate_prefixes():
            candidate = prefix / "bin" / name
            if candidate.exists():
                return str(candidate)
    return None


def _candidate_prefixes() -> tuple[Path, ...]:
    prefix = Path(sys.prefix).resolve()
    prefixes = [prefix]
    if prefix.parent.name == "envs":
        prefixes.append(prefix.parent.parent)
    return tuple(dict.fromkeys(prefixes))


def resolve_required_executable(value: str | None, *names: str) -> str:
    resolved = resolve_executable(value, *names)
    if resolved:
        return resolved
    choices = ", ".join(names)
    raise CommandError(f"required executable not found: {choices}")


def shell_join(parts) -> str:
    return " ".join(shlex.quote(str(part)) for part in parts)


def tail_text(path: Path, *, lines: int) -> str:
    if lines <= 0:
        return ""
    chunks = []
    newline_count = 0
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        position = handle.tell()
        while position > 0 and newline_count <= lines:
            read_size = min(TAIL_BLOCK_SIZE, position)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
    data = b"".join(reversed(chunks))
    tail = data.split(b"\n")
    if tail and tail[-1] == b"":
        tail = tail[:-1]
    return b"\n".join(tail[-lines:]).decode(
        "utf-8",
        errors="replace",
    )


class TmuxDevController:
    def __init__(
        self,
        *,
        tmux_bin: str,
        socket: str,
        session: str,
        root: str,
        log_path: str,
    ):
        self.tmux_bin = tmux_bin
        self.socket = socket
        self.session = session
        self.root = root
        self.log_path = log_path

    def _tmux(self, *args: str, check: bool = False):
        proc = subprocess.run(
            [self.tmux_bin, "-L", self.socket, *args],
            check=False,
            capture_output=True,
            text=True,
        )
        if check and proc.returncode:
            detail = (proc.stderr or proc.stdout or "").strip()
            suffix = f": {detail}" if detail else ""
            raise CommandError(f"tmux {' '.join(args)} failed{suffix}")
        return proc

    def exists(self) -> bool:
        return self._tmux("has-session", "-t", self.session).returncode == 0

    def start(self, command, *, out) -> None:
        log_path = Path(self.log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.close(fd)
        log_path.chmod(0o600)
        if self.exists():
            out(
                "already running: "
                f"tmux -L {self.socket} attach -t {self.session}"
            )
            return
        shell_script = (
            f"cd {shlex.quote(self.root)} || exit; "
            f"{shell_join(command)} 2>&1 | "
            f"tee -a {shlex.quote(self.log_path)}; "
            "status=${PIPESTATUS[0]}; exit ${status}"
        )
        tmux_command = f"exec bash -c {shlex.quote(shell_script)}"
        self._tmux(
            "new-session",
            "-d",
            "-s",
            self.session,
            tmux_command,
            check=True,
        )
        out(f"started: tmux -L {self.socket} attach -t {self.session}")

    def stop(self, *, out) -> None:
        if not self.exists():
            out(f"not running: {self.session}")
            return
        self._tmux("kill-session", "-t", self.session, check=True)
        out(f"stopped: {self.session}")

    def restart(self, command, *, out) -> None:
        self.stop(out=out)
        self.start(command, out=out)

    def status(self, *, out) -> None:
        if not self.exists():
            raise CommandError(f"not running: {self.session}")
        proc = self._tmux("list-sessions", check=True)
        for line in proc.stdout.splitlines():
            if line.startswith(f"{self.session}:"):
                out(line)
                return
        out(f"running: {self.session}")

    def attach(self) -> None:
        os.execvp(
            self.tmux_bin,
            [self.tmux_bin, "-L", self.socket, "attach", "-t", self.session],
        )

    def logs(self, *, lines: int, out) -> None:
        path = Path(self.log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch(mode=0o600)
        tail = tail_text(path, lines=lines)
        if tail:
            out(tail.rstrip("\n"))


class TmuxDevCommandMixin:
    _vargc_ = True

    default_env_name = None
    default_tmux_socket = None
    default_tmux_session = None
    default_log_path = None

    env_name = None
    _env_name = None

    class EnvNameArg(StringInvariant):
        _arg = "--env-name"
        _help = "Conda/mamba environment name for tmux-managed runs."
        _mandatory = False

    mamba_bin = None
    _mamba_bin = None

    class MambaBinArg(StringInvariant):
        _arg = "--mamba-bin"
        _help = "mamba or conda executable for tmux-managed runs."
        _mandatory = False

    tmux_bin = None
    _tmux_bin = None

    class TmuxBinArg(StringInvariant):
        _arg = "--tmux-bin"
        _help = "tmux executable."
        _mandatory = False

    tmux_socket = None
    _tmux_socket = None

    class TmuxSocketArg(StringInvariant):
        _arg = "--tmux-socket"
        _help = "tmux socket name."
        _mandatory = False

    tmux_session = None
    _tmux_session = None

    class TmuxSessionArg(StringInvariant):
        _arg = "--tmux-session"
        _help = "tmux session name."
        _mandatory = False

    log_path = None
    _log_path = None

    class LogPathArg(StringInvariant):
        _arg = "--log-path"
        _help = "tmux-managed log file."
        _mandatory = False

    cli_bin = None
    _cli_bin = None

    class CliBinArg(StringInvariant):
        _arg = "--cli-bin"
        _help = "CLI executable used inside tmux."
        _mandatory = False

    log_lines = None
    _log_lines = None

    class LogLinesArg(PositiveIntegerInvariant):
        _arg = "--log-lines"
        _help = "Number of log lines to show."
        _mandatory = False

    def _app_slug(self) -> str:
        raise NotImplementedError

    def _env_prefix(self) -> str:
        return self._app_slug().replace("-", "_").upper()

    def _dev_action(self) -> str:
        return (self.args[0] if self.args else "run").lower()

    def _runtime_root_value(self) -> str:
        raise NotImplementedError

    def _env_name_value(self) -> str:
        return (
            self._env_name
            or self.env_name
            or os.environ.get(f"{self._env_prefix()}_ENV_NAME")
            or self.default_env_name
            or self._app_slug()
        )

    def _mamba_bin_value(self) -> str:
        return resolve_required_executable(
            self._mamba_bin or self.mamba_bin or os.environ.get("MAMBA"),
            "mamba",
            "conda",
        )

    def _tmux_bin_value(self) -> str:
        return resolve_required_executable(
            self._tmux_bin or self.tmux_bin,
            "tmux",
        )

    def _tmux_socket_value(self) -> str:
        return (
            self._tmux_socket
            or self.tmux_socket
            or os.environ.get(f"{self._env_prefix()}_TMUX_SOCKET")
            or self.default_tmux_socket
            or self._app_slug()
        )

    def _tmux_session_env_name(self) -> str:
        return f"{self._env_prefix()}_TMUX_SESSION"

    def _tmux_session_default(self) -> str:
        return f"{self._app_slug()}-dev"

    def _tmux_session_value(self) -> str:
        return (
            self._tmux_session
            or self.tmux_session
            or os.environ.get(self._tmux_session_env_name())
            or self.default_tmux_session
            or self._tmux_session_default()
        )

    def _log_path_env_name(self) -> str:
        return f"{self._env_prefix()}_DEV_LOG"

    def _log_path_default(self) -> str:
        return str(Path(self._runtime_root_value()) / "dev.log")

    def _log_path_value(self) -> str:
        value = (
            self._log_path
            or self.log_path
            or os.environ.get(self._log_path_env_name())
            or self.default_log_path
            or self._log_path_default()
        )
        return str(Path(value).expanduser())

    def _cli_bin_value(self) -> str:
        value = self._cli_bin or self.cli_bin
        if value:
            return value
        requested = sys.argv[0]
        resolved = resolve_executable(requested)
        if resolved:
            return resolved
        return requested

    def _child_cli_args(self) -> list[str]:
        display_name = getattr(self, "cli_display_name", None) or self.name
        return str(display_name).split()

    def _log_lines_env_name(self) -> str:
        return f"{self._env_prefix()}_LOG_LINES"

    def _log_lines_value(self) -> int:
        value = self._log_lines or self.log_lines
        if value:
            return int(value)
        return int(os.environ.get(self._log_lines_env_name(), 80))

    def _tmux_root(self) -> str:
        return os.getcwd()

    def _tmux_controller(self) -> TmuxDevController:
        return TmuxDevController(
            tmux_bin=self._tmux_bin_value(),
            socket=self._tmux_socket_value(),
            session=self._tmux_session_value(),
            root=self._tmux_root(),
            log_path=self._log_path_value(),
        )

    def _child_command(self) -> list[str]:
        raise NotImplementedError

    def _run_tmux_action(self, action: str) -> bool:
        if action == "start":
            self._tmux_controller().start(
                self._child_command(), out=self._out
            )
            return True
        if action == "stop":
            self._tmux_controller().stop(out=self._out)
            return True
        if action == "restart":
            self._tmux_controller().restart(
                self._child_command(),
                out=self._out,
            )
            return True
        if action == "tmux-status":
            self._tmux_controller().status(out=self._out)
            return True
        if action == "attach":
            self._tmux_controller().attach()
            return True
        if action == "logs":
            self._tmux_controller().logs(
                lines=self._log_lines_value(),
                out=self._out,
            )
            return True
        return False

    def _run_dev_action(
        self,
        handlers,
        *,
        command_name: str,
        choices: tuple[str, ...],
    ) -> None:
        action = self._dev_action()
        handler = handlers.get(action)
        if handler is not None:
            handler()
            return
        if self._run_tmux_action(action):
            return
        expected = ", ".join(choices)
        raise CommandError(
            f"unknown {command_name} action: {action}; "
            f"expected one of: {expected}"
        )
