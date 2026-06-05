from __future__ import annotations

import asyncio
import json
import os
import pty
import shlex
import signal
import struct
import subprocess
import termios
from collections.abc import Iterable
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from .command import CommandError

TMUX_FIELD_SEP = "\x1f"
DEFAULT_CONSOLE_HOST = "127.0.0.1"
DEFAULT_CONSOLE_PORT = 8960
DEFAULT_CONSOLE_ROOT_PATH = "/kickle/console"
DEFAULT_TMUX_SOCKET = "kickle"
DEFAULT_AUTH_EMAIL_HEADER = "x-auth-request-email"
DEFAULT_AUTH_EMAIL_DOMAIN = "nvidia.com"
AUTH_EMAIL_HEADER_ALIASES = (
    "x-auth-request-email",
    "x-forwarded-email",
    "x-webauth-user",
    "mail",
    "email",
)


class ConsoleError(CommandError):
    pass


class ConsoleAuthError(ConsoleError):
    def __init__(self, message: str, status_code: int = 401):
        super().__init__(message)
        self.status_code = status_code


@dataclass(frozen=True)
class ConsoleConfig:
    host: str = DEFAULT_CONSOLE_HOST
    port: int = DEFAULT_CONSOLE_PORT
    root_path: str = DEFAULT_CONSOLE_ROOT_PATH
    tmux_bin: str = "tmux"
    tmux_socket: str = DEFAULT_TMUX_SOCKET
    auth_email_header: str = DEFAULT_AUTH_EMAIL_HEADER
    auth_email_domain: str = DEFAULT_AUTH_EMAIL_DOMAIN
    admin_emails: tuple[str, ...] = ()
    allow_unauthenticated: bool = False


@dataclass(frozen=True)
class ConsoleUser:
    email: str
    can_write: bool
    authenticated: bool


def normalize_root_path(value: str | None) -> str:
    path = (value or DEFAULT_CONSOLE_ROOT_PATH).strip() or "/"
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/") or "/"


def parse_email_list(value: str | Iterable[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = value
    return tuple(
        dict.fromkeys(
            item.strip().lower() for item in items if item and item.strip()
        )
    )


def console_url(config: ConsoleConfig, *, public_base_url: str | None = None):
    root_path = normalize_root_path(config.root_path)
    if public_base_url:
        return public_base_url.rstrip("/") + root_path + "/"
    host = "localhost" if config.host == "127.0.0.1" else config.host
    return f"http://{host}:{config.port}{root_path}/"


def _tmux_command(config: ConsoleConfig, *args: str) -> list[str]:
    return [config.tmux_bin, "-L", config.tmux_socket, *args]


def _run_tmux(
    config: ConsoleConfig,
    *args: str,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        _tmux_command(config, *args),
        check=False,
        capture_output=True,
        text=True,
    )


def _tmux_no_server(proc: subprocess.CompletedProcess) -> bool:
    message = f"{proc.stderr}\n{proc.stdout}".lower()
    return "no server running" in message or "failed to connect" in message


def _field_format(*fields: str) -> str:
    return TMUX_FIELD_SEP.join(fields)


def _split_fields(line: str, expected: int) -> list[str]:
    parts = line.rstrip("\n").split(TMUX_FIELD_SEP)
    if len(parts) < expected:
        parts.extend([""] * (expected - len(parts)))
    return parts[:expected]


def list_tmux_sessions(config: ConsoleConfig) -> list[dict[str, Any]]:
    session_proc = _run_tmux(
        config,
        "list-sessions",
        "-F",
        _field_format(
            "#{session_name}",
            "#{session_windows}",
            "#{session_created}",
            "#{session_attached}",
        ),
    )
    if session_proc.returncode:
        if _tmux_no_server(session_proc):
            return []
        detail = (session_proc.stderr or session_proc.stdout).strip()
        raise ConsoleError(f"tmux list-sessions failed: {detail}")

    sessions: dict[str, dict[str, Any]] = {}
    for line in session_proc.stdout.splitlines():
        name, window_count, created, attached = _split_fields(line, 4)
        if not name:
            continue
        sessions[name] = {
            "name": name,
            "window_count": _int_or_zero(window_count),
            "created": _int_or_zero(created),
            "attached": attached == "1",
            "windows": [],
        }

    if not sessions:
        return []

    window_proc = _run_tmux(
        config,
        "list-windows",
        "-a",
        "-F",
        _field_format(
            "#{session_name}",
            "#{window_index}",
            "#{window_name}",
            "#{window_active}",
            "#{window_panes}",
            "#{pane_current_command}",
        ),
    )
    if window_proc.returncode and not _tmux_no_server(window_proc):
        detail = (window_proc.stderr or window_proc.stdout).strip()
        raise ConsoleError(f"tmux list-windows failed: {detail}")
    if window_proc.returncode == 0:
        for line in window_proc.stdout.splitlines():
            (
                session_name,
                index,
                name,
                active,
                pane_count,
                command,
            ) = _split_fields(line, 6)
            session = sessions.get(session_name)
            if session is None:
                continue
            session["windows"].append(
                {
                    "index": _int_or_zero(index),
                    "name": name,
                    "active": active == "1",
                    "pane_count": _int_or_zero(pane_count),
                    "current_command": command,
                }
            )

    return sorted(sessions.values(), key=lambda item: item["name"])


def _int_or_zero(value: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def require_existing_session(
    config: ConsoleConfig,
    session_name: str,
) -> str:
    candidate = session_name.strip()
    if not candidate or "\0" in candidate:
        raise ConsoleError("tmux session name is required")
    names = {session["name"] for session in list_tmux_sessions(config)}
    if candidate not in names:
        raise ConsoleError(f"unknown tmux session: {candidate}")
    return candidate


def user_from_headers(
    config: ConsoleConfig,
    headers: Any,
) -> ConsoleUser:
    configured = config.auth_email_header.lower()
    aliases = tuple(dict.fromkeys((configured, *AUTH_EMAIL_HEADER_ALIASES)))
    email = ""
    for header_name in aliases:
        value = headers.get(header_name)
        if value:
            email = str(value).strip().split(",", 1)[0].strip().lower()
            break
    authenticated = bool(email)
    if not email and config.allow_unauthenticated:
        email = "anonymous"
    if not email:
        raise ConsoleAuthError("missing authenticated email header", 401)
    domain = config.auth_email_domain.strip().lower()
    if domain and email != "anonymous" and not email.endswith(f"@{domain}"):
        raise ConsoleAuthError("authenticated email is not allowed", 403)
    admin_emails = set(config.admin_emails)
    return ConsoleUser(
        email=email,
        can_write=email in admin_emails,
        authenticated=authenticated,
    )


def _static_root() -> Path:
    return Path(str(files("sneeze") / "console_static"))


def build_console_app(config: ConsoleConfig):
    try:
        from starlette.applications import Starlette
        from starlette.exceptions import HTTPException
        from starlette.responses import (
            FileResponse,
            JSONResponse,
            RedirectResponse,
        )
        from starlette.routing import Mount, Route, WebSocketRoute
        from starlette.staticfiles import StaticFiles
    except ImportError as exc:
        raise ConsoleError(
            "console support requires starlette and uvicorn; install "
            "the 'sneeze[mcp]' extra"
        ) from exc

    static_root = _static_root()

    def authenticated(request):
        try:
            return user_from_headers(config, request.headers)
        except ConsoleAuthError as exc:
            raise HTTPException(exc.status_code, str(exc)) from exc

    async def index(request):
        authenticated(request)
        return FileResponse(static_root / "index.html")

    async def api_sessions(request):
        user = authenticated(request)
        try:
            sessions = list_tmux_sessions(config)
        except ConsoleError as exc:
            raise HTTPException(500, str(exc)) from exc
        return JSONResponse(
            {
                "user": {
                    "email": user.email,
                    "can_write": user.can_write,
                    "authenticated": user.authenticated,
                },
                "tmux_socket": config.tmux_socket,
                "sessions": sessions,
            }
        )

    async def websocket_session(websocket):
        await _websocket_session(config, websocket)

    subapp = Starlette(
        routes=[
            Route("/", index),
            Route("/api/sessions", api_sessions),
            WebSocketRoute(
                "/ws/session/{session_name:str}",
                websocket_session,
            ),
            Mount(
                "/static",
                app=StaticFiles(directory=static_root),
                name="static",
            ),
        ]
    )
    root_path = normalize_root_path(config.root_path)
    if root_path == "/":
        return subapp

    async def redirect_root(request):
        return RedirectResponse(root_path + "/")

    return Starlette(
        routes=[
            Route("/", redirect_root),
            Mount(root_path, app=subapp),
        ]
    )


async def _websocket_session(config: ConsoleConfig, websocket) -> None:
    try:
        user = user_from_headers(config, websocket.headers)
    except ConsoleAuthError as exc:
        await websocket.close(code=4401 if exc.status_code == 401 else 4403)
        return
    raw_session = websocket.path_params["session_name"]
    try:
        session_name = require_existing_session(config, raw_session)
    except ConsoleError:
        await websocket.close(code=4404)
        return
    write_requested = websocket.query_params.get("write") == "1"
    writable = user.can_write and write_requested
    await websocket.accept()
    await _attach_tmux(websocket, config, session_name, writable=writable)


async def _attach_tmux(
    websocket,
    config: ConsoleConfig,
    session_name: str,
    *,
    writable: bool,
) -> None:
    master_fd, slave_fd = pty.openpty()
    args = _tmux_command(config, "attach-session", "-t", session_name)
    if not writable:
        args.insert(-2, "-r")
    env = {
        **os.environ,
        "TERM": os.environ.get("TERM") or "xterm-256color",
    }
    proc = subprocess.Popen(
        args,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
        env=env,
    )
    os.close(slave_fd)
    os.set_blocking(master_fd, False)
    reader = asyncio.create_task(_read_pty(websocket, master_fd, proc))
    writer = asyncio.create_task(
        _write_pty(websocket, master_fd, proc, writable=writable)
    )
    done, pending = await asyncio.wait(
        {reader, writer},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in done:
        task.result()
    _terminate_process(proc)
    try:
        os.close(master_fd)
    except OSError:
        pass


async def _read_pty(
    websocket,
    master_fd: int,
    proc: subprocess.Popen,
) -> None:
    while proc.poll() is None:
        try:
            data = os.read(master_fd, 8192)
        except BlockingIOError:
            await asyncio.sleep(0.03)
            continue
        except OSError:
            break
        if not data:
            await asyncio.sleep(0.03)
            continue
        await websocket.send_text(
            json.dumps(
                {
                    "type": "output",
                    "data": data.decode("utf-8", errors="replace"),
                }
            )
        )


async def _write_pty(
    websocket,
    master_fd: int,
    proc: subprocess.Popen,
    *,
    writable: bool,
) -> None:
    while proc.poll() is None:
        message = await websocket.receive_text()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        payload_type = payload.get("type")
        if payload_type == "input" and writable:
            data = str(payload.get("data") or "")
            if data:
                os.write(master_fd, data.encode("utf-8", errors="replace"))
        elif payload_type == "resize":
            cols = max(20, min(400, _int_or_zero(str(payload.get("cols")))))
            rows = max(5, min(200, _int_or_zero(str(payload.get("rows")))))
            _resize_pty(master_fd, rows=rows, cols=cols)


def _resize_pty(master_fd: int, *, rows: int, cols: int) -> None:
    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    try:
        import fcntl

        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)
    except OSError:
        return


def _terminate_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGHUP)
        proc.wait(timeout=2)
    except (OSError, subprocess.TimeoutExpired):
        proc.kill()


def run_console_server(config: ConsoleConfig) -> None:
    try:
        import uvicorn
    except ImportError as exc:
        raise ConsoleError(
            "console support requires uvicorn; install the "
            "'sneeze[mcp]' extra"
        ) from exc
    uvicorn.run(
        build_console_app(config),
        host=config.host,
        port=config.port,
        ws="auto",
    )


def describe_console_config(
    config: ConsoleConfig,
    *,
    public_base_url: str | None = None,
) -> dict[str, Any]:
    return {
        "host": config.host,
        "port": config.port,
        "root_path": normalize_root_path(config.root_path),
        "url": console_url(config, public_base_url=public_base_url),
        "tmux_bin": config.tmux_bin,
        "tmux_socket": config.tmux_socket,
        "auth_email_header": config.auth_email_header,
        "auth_email_domain": config.auth_email_domain,
        "admin_emails": list(config.admin_emails),
        "allow_unauthenticated": config.allow_unauthenticated,
        "run_command": shlex.join(
            [
                "uvicorn",
                "sneeze.console:build_console_app",
                "--host",
                config.host,
                "--port",
                str(config.port),
            ]
        ),
    }
