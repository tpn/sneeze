from __future__ import annotations

import json
import os
import queue
import stat
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


class CodexAppServerError(RuntimeError):
    pass


class CodexAppServerProtocolError(CodexAppServerError):
    pass


class CodexAppServerRequestError(CodexAppServerError):
    PERMANENT_CODES = frozenset({-32600, -32601, -32602})
    PERMANENT_MESSAGE_MARKERS = (
        "invalid params",
        "invalid request",
        "method not found",
        "not found",
        "does not exist",
        "unsupported method",
        "unknown method",
    )

    def __init__(self, method: str, error: Any | None = None):
        self.method = method
        self.error = error
        self.code = error.get("code") if isinstance(error, dict) else None
        self.data = error.get("data") if isinstance(error, dict) else None
        error_text = (
            CodexAppServerConnection._error_text(error)
            if error is not None
            else method
        )
        message = f"{method}: {error_text}" if error is not None else method
        super().__init__(message)

    @property
    def permanent(self) -> bool:
        if self.code in self.PERMANENT_CODES:
            return True
        message = str(
            self.error.get("message")
            if isinstance(self.error, dict)
            else self.error or self
        ).lower()
        return any(
            marker in message for marker in self.PERMANENT_MESSAGE_MARKERS
        )


def codex_app_server_socket_path(
    configured_path: str | None = None,
    *,
    environment: dict[str, str] | None = None,
) -> str:
    environment = os.environ if environment is None else environment
    if configured_path:
        return str(Path(configured_path).expanduser())
    codex_home = environment.get("CODEX_HOME")
    root = (
        Path(codex_home).expanduser()
        if codex_home
        else Path.home() / ".codex"
    )
    return str(root / "app-server-control" / "app-server-control.sock")


def _codex_home_for_socket(socket_path: str) -> Path:
    target = Path(socket_path).expanduser()
    parent = target.parent
    if parent.name != "app-server-control":
        raise CodexAppServerError(
            "Codex app-server socket must be inside app-server-control"
        )
    return parent.parent.resolve()


def validate_codex_app_server_socket(socket_path: str) -> Path:
    target = Path(socket_path).expanduser()
    codex_home = _codex_home_for_socket(str(target))
    resolved_parent = target.parent.resolve()
    if resolved_parent.parent != codex_home:
        raise CodexAppServerError(
            "Codex app-server socket resolves outside the configured "
            "Codex home"
        )
    try:
        parent_stat = target.parent.stat()
        socket_stat = target.lstat()
    except FileNotFoundError as exc:
        raise CodexAppServerError(
            f"Codex app-server socket does not exist: {target}"
        ) from exc
    getuid = getattr(os, "getuid", None)
    if getuid is None:  # pragma: no cover - Windows cannot host this socket
        raise CodexAppServerError("Unix socket control requires a POSIX host")
    uid = getuid()
    if parent_stat.st_uid != uid or socket_stat.st_uid != uid:
        raise CodexAppServerError(
            "Codex app-server socket and parent must be owned by this user"
        )
    if stat.S_IMODE(parent_stat.st_mode) & 0o077:
        raise CodexAppServerError(
            "Codex app-server control directory must not be group/world "
            "accessible"
        )
    if not stat.S_ISSOCK(socket_stat.st_mode):
        raise CodexAppServerError(
            f"Codex app-server path is not a Unix socket: {target}"
        )
    if stat.S_IMODE(socket_stat.st_mode) & 0o177:
        raise CodexAppServerError(
            "Codex app-server socket must not be group/world accessible"
        )
    return target


def start_codex_app_server_daemon(
    codex_bin: str,
    socket_path: str,
    *,
    timeout_seconds: float = 10.0,
) -> None:
    target = Path(socket_path).expanduser()
    codex_home = _codex_home_for_socket(str(target))
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CodexAppServerError(
            "Could not start Codex app-server daemon: startup timed out"
        )
    try:
        result = subprocess.run(
            [codex_bin, "app-server", "daemon", "start"],
            check=False,
            capture_output=True,
            text=True,
            timeout=remaining,
            env={**os.environ, "CODEX_HOME": str(codex_home)},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise CodexAppServerError(
            f"Could not start Codex app-server daemon: {exc}"
        ) from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise CodexAppServerError(
            "Could not start Codex app-server daemon"
            + (f": {detail}" if detail else "")
        )
    while time.monotonic() < deadline:
        if target.exists():
            validate_codex_app_server_socket(str(target))
            return
        time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
    raise CodexAppServerError(
        f"Codex app-server daemon did not create its socket: {target}"
    )


class CodexAppServerConnection:
    def __init__(
        self,
        socket_path: str,
        *,
        client_name: str,
        client_title: str,
        client_version: str,
        request_timeout_seconds: float = 5.0,
        notification_handler: Callable[[dict[str, Any]], None] | None = None,
        server_request_handler: (
            Callable[[dict[str, Any]], None] | None
        ) = None,
        disconnect_handler: (
            Callable[[BaseException | None], None] | None
        ) = None,
    ):
        self.socket_path = socket_path
        self.client_name = client_name
        self.client_title = client_title
        self.client_version = client_version
        self.request_timeout_seconds = request_timeout_seconds
        self.notification_handler = notification_handler
        self.server_request_handler = server_request_handler
        self.disconnect_handler = disconnect_handler
        self.codex_home: str | None = None
        self.user_agent: str | None = None
        self._websocket: Any = None
        self._reader: threading.Thread | None = None
        self._send_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: dict[int, queue.Queue[dict[str, Any]]] = {}
        self._next_id = 2
        self._closed = threading.Event()
        self._handler_queue: queue.Queue[
            tuple[Callable[[dict[str, Any]], None], dict[str, Any]] | None
        ] = queue.Queue()
        self._handler_worker: threading.Thread | None = None
        self._disconnect_lock = threading.Lock()
        self._disconnect_notified = False

    @property
    def connected(self) -> bool:
        return self._websocket is not None and not self._closed.is_set()

    def __enter__(self) -> CodexAppServerConnection:
        return self.connect()

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def connect(self) -> CodexAppServerConnection:
        if self.connected:
            return self
        target = validate_codex_app_server_socket(self.socket_path)
        try:
            from websockets.sync.client import unix_connect
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise CodexAppServerError(
                "websockets is required for Codex app-server interaction"
            ) from exc
        websocket = None
        try:
            websocket = unix_connect(
                str(target),
                uri="ws://localhost/rpc",
                compression=None,
                proxy=None,
                open_timeout=self.request_timeout_seconds,
                close_timeout=1,
            )
            websocket.send(
                json.dumps(
                    {
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "clientInfo": {
                                "name": self.client_name,
                                "title": self.client_title,
                                "version": self.client_version,
                            },
                            "capabilities": {"experimentalApi": True},
                        },
                    },
                    separators=(",", ":"),
                )
            )
            deadline = time.monotonic() + self.request_timeout_seconds
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("initialize response timed out")
                message = self._decode_message(
                    websocket.recv(timeout=remaining)
                )
                if message.get("id") != 1:
                    continue
                if message.get("error") is not None:
                    raise CodexAppServerRequestError(
                        "initialize",
                        message["error"],
                    )
                result = message.get("result")
                if not isinstance(result, dict):
                    raise CodexAppServerProtocolError(
                        "initialize returned no result object"
                    )
                self.codex_home = str(result.get("codexHome") or "") or None
                self.user_agent = str(result.get("userAgent") or "") or None
                break
            websocket.send(json.dumps({"method": "initialized"}))
        except Exception as exc:
            if websocket is not None:
                try:
                    websocket.close()
                except Exception:
                    pass
            if isinstance(exc, CodexAppServerError):
                raise
            raise CodexAppServerError(
                f"Could not connect to Codex app-server: {exc}"
            ) from exc
        self._websocket = websocket
        self._closed.clear()
        self._handler_queue = queue.Queue()
        with self._disconnect_lock:
            self._disconnect_notified = False
        self._handler_worker = threading.Thread(
            target=self._handler_loop,
            name=f"{self.client_name}-handlers",
            daemon=True,
        )
        self._handler_worker.start()
        self._reader = threading.Thread(
            target=self._reader_loop,
            name=f"{self.client_name}-reader",
            daemon=True,
        )
        self._reader.start()
        return self

    def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        if not self.connected:
            raise CodexAppServerError("Codex app-server connection is closed")
        response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
        with self._pending_lock:
            request_id = self._next_id
            self._next_id += 1
            self._pending[request_id] = response_queue
        try:
            self._send(
                {
                    "id": request_id,
                    "method": method,
                    "params": params or {},
                }
            )
            timeout = (
                self.request_timeout_seconds
                if timeout_seconds is None
                else timeout_seconds
            )
            try:
                response = response_queue.get(timeout=timeout)
            except queue.Empty as exc:
                raise CodexAppServerError(
                    f"Codex app-server request timed out: {method}"
                ) from exc
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        if response.get("error") is not None:
            raise CodexAppServerRequestError(method, response["error"])
        result = response.get("result")
        if result is None:
            return {}
        if not isinstance(result, dict):
            raise CodexAppServerProtocolError(
                f"{method} returned a non-object result"
            )
        return result

    def respond(self, request_id: Any, result: dict[str, Any]) -> None:
        if not self.connected:
            raise CodexAppServerError("Codex app-server connection is closed")
        self._send({"id": request_id, "result": result})

    def notify(
        self,
        method: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        if not self.connected:
            raise CodexAppServerError("Codex app-server connection is closed")
        self._send({"method": method, "params": params or {}})

    def drain_handlers(self, *, timeout_seconds: float | None = None) -> None:
        if threading.current_thread() is self._handler_worker:
            return
        if not self.connected:
            raise CodexAppServerError(
                "Codex app-server connection closed before callbacks drained"
            )
        drained = threading.Event()

        def mark_drained(message: dict[str, Any]) -> None:
            del message
            drained.set()

        self._handler_queue.put_nowait((mark_drained, {}))
        timeout = (
            self.request_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            remaining = deadline - time.monotonic()
            if drained.wait(timeout=max(0.0, min(0.05, remaining))):
                return
            if self._closed.is_set():
                raise CodexAppServerError(
                    "Codex app-server connection closed before callbacks "
                    "drained"
                )
            if remaining <= 0:
                raise CodexAppServerError(
                    "Codex app-server callbacks did not drain before timeout"
                )

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        websocket = self._websocket
        self._websocket = None
        if websocket is not None:
            try:
                websocket.close()
            except Exception:
                pass
        reader = self._reader
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=1)
        self._stop_handler_worker()
        self._fail_pending("Codex app-server connection closed")

    def _send(self, message: dict[str, Any]) -> None:
        websocket = self._websocket
        if websocket is None or self._closed.is_set():
            raise CodexAppServerError("Codex app-server connection is closed")
        encoded = json.dumps(message, separators=(",", ":"))
        try:
            with self._send_lock:
                websocket.send(encoded)
        except Exception as exc:
            raise CodexAppServerError(
                f"Could not write to Codex app-server: {exc}"
            ) from exc

    def _reader_loop(self) -> None:
        disconnect_error: BaseException | None = None
        try:
            while not self._closed.is_set():
                websocket = self._websocket
                if websocket is None:
                    break
                try:
                    raw_message = websocket.recv(timeout=1)
                except TimeoutError:
                    continue
                message = self._decode_message(raw_message)
                request_id = message.get("id")
                method = message.get("method")
                if request_id is not None and not method:
                    with self._pending_lock:
                        response_queue = self._pending.get(request_id)
                    if response_queue is not None:
                        response_queue.put(message)
                    continue
                if method and request_id is not None:
                    if self.server_request_handler is not None:
                        self._dispatch_handler(
                            self.server_request_handler,
                            message,
                        )
                    continue
                if method and self.notification_handler is not None:
                    self._dispatch_handler(
                        self.notification_handler,
                        message,
                    )
        except BaseException as exc:
            disconnect_error = exc
        finally:
            already_closed = self._closed.is_set()
            self._closed.set()
            self._websocket = None
            self._fail_pending("Codex app-server connection lost")
            self._stop_handler_worker()
            if not already_closed and self.disconnect_handler is not None:
                self._notify_disconnect(disconnect_error)

    def _dispatch_handler(
        self,
        handler: Callable[[dict[str, Any]], None],
        message: dict[str, Any],
    ) -> None:
        if self._closed.is_set():
            return
        self._handler_queue.put_nowait((handler, dict(message)))

    def _handler_loop(self) -> None:
        while True:
            item = self._handler_queue.get()
            if item is None or self._closed.is_set():
                return
            handler, message = item
            try:
                handler(message)
            except BaseException as exc:
                self._abort_from_handler(exc)
                return

    def _abort_from_handler(self, error: BaseException) -> None:
        already_closed = self._closed.is_set()
        self._closed.set()
        websocket = self._websocket
        self._websocket = None
        if websocket is not None:
            try:
                websocket.close()
            except Exception:
                pass
        self._fail_pending("Codex app-server callback failed")
        if not already_closed and self.disconnect_handler is not None:
            self._notify_disconnect(error)

    def _stop_handler_worker(self) -> None:
        worker = self._handler_worker
        self._handler_worker = None
        try:
            self._handler_queue.put_nowait(None)
        except queue.Full:  # pragma: no cover - the queue is unbounded
            pass
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=1)

    def _notify_disconnect(self, error: BaseException | None) -> None:
        with self._disconnect_lock:
            if self._disconnect_notified:
                return
            self._disconnect_notified = True
        if self.disconnect_handler is not None:
            self.disconnect_handler(error)

    def _fail_pending(self, message: str) -> None:
        with self._pending_lock:
            pending = list(self._pending.values())
        for response_queue in pending:
            try:
                response_queue.put_nowait({"error": {"message": message}})
            except queue.Full:
                pass

    @staticmethod
    def _decode_message(raw_message: Any) -> dict[str, Any]:
        if not isinstance(raw_message, str):
            raise CodexAppServerProtocolError(
                "Codex app-server returned a non-text WebSocket frame"
            )
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError as exc:
            raise CodexAppServerProtocolError(
                f"Codex app-server returned invalid JSON: {exc}"
            ) from exc
        if not isinstance(message, dict):
            raise CodexAppServerProtocolError(
                "Codex app-server message must be an object"
            )
        return message

    @staticmethod
    def _error_text(error: Any) -> str:
        if isinstance(error, dict):
            code = error.get("code")
            message = str(error.get("message") or error)
            return f"{code}: {message}" if code is not None else message
        return str(error)
