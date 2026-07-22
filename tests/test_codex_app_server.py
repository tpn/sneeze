import hashlib
import json
import queue
import shutil
import socket
import stat
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import sneeze.codex_app_server as codex_app_server_module
import sneeze.slackbot as slackbot_module
from sneeze.codex_app_server import (
    CodexAppServerConnection,
    CodexAppServerError,
    CodexAppServerProtocolError,
    CodexAppServerRequestError,
    start_codex_app_server_daemon,
    validate_codex_app_server_socket,
)
from sneeze.codex_sessions import CodexSessionMetadata
from sneeze.slackbot import (
    CodexAppController,
    CodexAppPreDispatchError,
    CodexAppRecoveryExhaustedError,
    CodexAppSessionValidationError,
    SlackbotError,
    SlackbotProfile,
    SlackbotRoute,
    SlackSocketBot,
    load_config,
    query_codex_app_server_status,
    scaffold_runtime,
)


def _profile(tmp_path: Path) -> SlackbotProfile:
    return SlackbotProfile(
        app_slug="sample",
        env_prefix="SAMPLE",
        default_bot_name="sample",
        default_command_name="/sample",
        default_runtime_root=str(tmp_path / "runtime"),
        default_codex_workdir=str(tmp_path),
        default_system_prompt="# Sample\n",
        default_unit_name="sample.service",
        codex_stop_notify_sources=("cli", "vscode"),
        codex_app_server_interaction_enabled=True,
        default_codex_app_server_socket=str(
            tmp_path
            / ".codex"
            / "app-server-control"
            / "app-server-control.sock"
        ),
        codex_app_server_interaction_sources=("cli", "vscode"),
        codex_app_server_auto_start=True,
    )


def _bot(tmp_path: Path, *, notification_only: bool = True) -> SlackSocketBot:
    profile = _profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    allowed_user = "U222" if notification_only else "U111"
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n"
        + f"SAMPLE_SLACKBOT_ALLOWED_DM_USER_IDS={allowed_user}\n",
        encoding="utf-8",
    )
    return SlackSocketBot(load_config(profile))


def _seed_cached_app_server_candidate(bot: SlackSocketBot) -> Path:
    state_path = Path(
        slackbot_module.agent_codex_hook_state_path(bot.config.paths)
    )
    slackbot_module.write_json_atomic(
        state_path,
        {
            "version": 1,
            "sessions": {
                "session:desktop-session": {
                    "key": "session:desktop-session",
                    "session_id": "desktop-session",
                    "channel_id": "D111",
                    "thread_ts": "1.000001",
                    "notify_user_id": "U111",
                    "last_turn_id": "turn-eligible",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "notifications": [],
                    "codex_app_server": {
                        "kind": "codex_app_server",
                        "thread_id": "desktop-session",
                        "session_id": "desktop-session",
                        "source": "vscode",
                        "cwd": "/workspace/exact",
                        "verified_at": "2026-07-21T00:00:00Z",
                    },
                }
            },
        },
    )
    return state_path


class FakeControllerConnection:
    instances = []

    def __init__(self, *args, **kwargs):
        del args
        self.notification_handler = kwargs.get("notification_handler")
        self.server_request_handler = kwargs.get("server_request_handler")
        self.disconnect_handler = kwargs.get("disconnect_handler")
        self.connected = False
        self.active_turn_id = None
        self.turn_items = []
        self.turn_history = []
        self.turn_history_pages = {}
        self.requests = []
        self.responses = []
        self.closed = False
        self.drain_calls = 0
        self.drain_timeouts = []
        self.events = []
        type(self).instances.append(self)

    def connect(self):
        self.connected = True
        return self

    def close(self):
        self.connected = False
        self.closed = True

    def drain_handlers(self, **kwargs):
        self.drain_calls += 1
        self.drain_timeouts.append(kwargs.get("timeout_seconds"))
        self.events.append("drain")

    def request(self, method, params):
        self.events.append(method)
        self.requests.append((method, params))
        if method in {"thread/read", "thread/resume"}:
            return {
                "thread": {
                    "id": "desktop-session",
                    "sessionId": "desktop-session",
                    "source": "vscode",
                    "thread_source": "user",
                    "cwd": "/workspace/exact",
                    "status": {
                        "type": "active" if self.active_turn_id else "idle"
                    },
                }
            }
        if method == "thread/turns/list":
            if self.turn_history_pages:
                cursor = params.get("cursor")
                assert cursor in self.turn_history_pages
                return dict(self.turn_history_pages[cursor])
            if self.turn_history:
                return {"data": list(self.turn_history)}
            if not self.active_turn_id:
                return {"data": []}
            return {
                "data": [
                    {
                        "id": self.active_turn_id,
                        "status": "inProgress",
                        "items": list(self.turn_items),
                    }
                ]
            }
        if method == "turn/start":
            self.active_turn_id = "turn-started"
            return {"turn": {"id": self.active_turn_id}}
        if method == "turn/steer":
            return {"turnId": self.active_turn_id}
        if method == "turn/interrupt":
            return {}
        raise AssertionError(f"unexpected method: {method}")

    def respond(self, request_id, result):
        self.responses.append((request_id, result))


class FakeJsonRpcWebSocket:
    def __init__(self):
        self.messages = queue.Queue()
        self.messages.put(
            json.dumps(
                {
                    "id": 1,
                    "result": {
                        "codexHome": "/home/test/.codex",
                        "userAgent": "codex-test",
                    },
                }
            )
        )
        self.sent = []
        self.closed = False

    def send(self, encoded):
        message = json.loads(encoded)
        self.sent.append(message)
        if message.get("id") not in {None, 1}:
            self.messages.put(
                json.dumps(
                    {
                        "id": message["id"],
                        "result": {"echo": message.get("method")},
                    }
                )
            )

    def recv(self, timeout):
        try:
            return self.messages.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError from exc

    def close(self):
        self.closed = True


def _private_socket(tmp_path: Path):
    # pytest's tmp_path plus the fixed control-socket suffix exceeds Linux's
    # AF_UNIX path limit on this host, so keep this fixture deliberately
    # short.
    del tmp_path
    codex_home = Path(tempfile.mkdtemp(prefix="sc-", dir="/var/tmp"))
    control_dir = codex_home / "app-server-control"
    control_dir.mkdir(parents=True, mode=0o700)
    control_dir.chmod(0o700)
    socket_path = control_dir / "app-server-control.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    socket_path.chmod(0o600)
    return listener, socket_path, codex_home


def test_socket_validation_requires_private_same_user_socket(tmp_path):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    try:
        assert validate_codex_app_server_socket(str(socket_path)) == (
            socket_path
        )
        socket_path.chmod(0o660)
        with pytest.raises(
            CodexAppServerError,
            match="must not be group/world accessible",
        ):
            validate_codex_app_server_socket(str(socket_path))
    finally:
        listener.close()
        shutil.rmtree(codex_home)


def test_json_rpc_connection_initializes_and_correlates_requests(
    tmp_path,
    monkeypatch,
):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = FakeJsonRpcWebSocket()
    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=1,
    )
    try:
        connection.connect()
        result = connection.request("thread/read", {"threadId": "one"})

        assert result == {"echo": "thread/read"}
        assert websocket.sent[0]["method"] == "initialize"
        assert websocket.sent[1] == {"method": "initialized"}
        assert websocket.sent[2]["method"] == "thread/read"
    finally:
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_json_rpc_request_error_is_not_a_protocol_error(
    tmp_path,
    monkeypatch,
):
    class ErrorJsonRpcWebSocket(FakeJsonRpcWebSocket):
        def send(self, encoded):
            message = json.loads(encoded)
            self.sent.append(message)
            if message.get("id") not in {None, 1}:
                self.messages.put(
                    json.dumps(
                        {
                            "id": message["id"],
                            "error": {
                                "code": -32000,
                                "message": "temporarily unavailable",
                            },
                        }
                    )
                )

    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = ErrorJsonRpcWebSocket()
    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=1,
    )
    try:
        connection.connect()
        with pytest.raises(
            CodexAppServerRequestError,
            match="temporarily unavailable",
        ) as request_error:
            connection.request("thread/read", {"threadId": "one"})

        assert not isinstance(
            request_error.value,
            CodexAppServerProtocolError,
        )
        assert request_error.value.code == -32000
        assert request_error.value.data is None
        assert request_error.value.permanent is False
    finally:
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_request_error_classifies_permanent_json_rpc_failures():
    request_error = CodexAppServerRequestError(
        "thread/read",
        {
            "code": -32602,
            "message": "Invalid params: thread not found",
            "data": {"threadId": "missing"},
        },
    )

    assert request_error.code == -32602
    assert request_error.data == {"threadId": "missing"}
    assert request_error.permanent is True


@pytest.mark.parametrize(
    ("handler_name", "message"),
    (
        ("notification_handler", {"method": "turn/started"}),
        (
            "server_request_handler",
            {"id": 42, "method": "item/commandExecution/requestApproval"},
        ),
    ),
)
def test_callbacks_do_not_block_response_correlation(
    tmp_path,
    monkeypatch,
    handler_name,
    message,
):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = FakeJsonRpcWebSocket()
    callback_started = threading.Event()
    callback_release = threading.Event()

    def blocking_handler(received):
        assert received == message
        callback_started.set()
        callback_release.wait(timeout=2)

    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=1,
        **{handler_name: blocking_handler},
    )
    try:
        connection.connect()
        websocket.messages.put(json.dumps(message))
        assert callback_started.wait(timeout=1)

        result = connection.request("thread/read", {"threadId": "one"})

        assert result == {"echo": "thread/read"}
    finally:
        callback_release.set()
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_handler_drain_waits_for_queued_async_callback(
    tmp_path,
    monkeypatch,
):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = FakeJsonRpcWebSocket()
    callback_started = threading.Event()
    callback_release = threading.Event()
    drain_started = threading.Event()
    drain_complete = threading.Event()

    def blocking_handler(message):
        assert message == {"method": "serverRequest/resolved"}
        callback_started.set()
        assert callback_release.wait(timeout=2)

    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=1,
        notification_handler=blocking_handler,
    )
    try:
        connection.connect()
        websocket.messages.put(
            json.dumps({"method": "serverRequest/resolved"})
        )
        assert callback_started.wait(timeout=1)

        def drain_handlers():
            drain_started.set()
            connection.drain_handlers()
            drain_complete.set()

        drain_thread = threading.Thread(target=drain_handlers)
        drain_thread.start()
        assert drain_started.wait(timeout=1)
        assert not drain_complete.is_set()
        callback_release.set()
        drain_thread.join(timeout=2)

        assert not drain_thread.is_alive()
        assert drain_complete.is_set()
    finally:
        callback_release.set()
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_handler_drain_success_survives_immediate_disconnect(
    tmp_path,
    monkeypatch,
):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = FakeJsonRpcWebSocket()
    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=1,
    )
    connected_checks = 0

    def connection_state(recovered):
        nonlocal connected_checks
        assert recovered is connection
        connected_checks += 1
        return connected_checks == 1

    try:
        connection.connect()
        monkeypatch.setattr(
            CodexAppServerConnection,
            "connected",
            property(connection_state),
        )

        connection.drain_handlers()

        assert connected_checks == 1
    finally:
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_handler_drain_fails_promptly_when_connection_closes(
    tmp_path,
    monkeypatch,
):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = FakeJsonRpcWebSocket()
    callback_started = threading.Event()
    callback_release = threading.Event()
    drain_started = threading.Event()
    drain_errors = []
    drain_elapsed = []

    def blocking_handler(message):
        assert message == {"method": "serverRequest/resolved"}
        callback_started.set()
        callback_release.wait(timeout=2)

    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=5,
        notification_handler=blocking_handler,
    )
    try:
        connection.connect()
        websocket.messages.put(
            json.dumps({"method": "serverRequest/resolved"})
        )
        assert callback_started.wait(timeout=1)

        def drain_handlers():
            started_at = time.monotonic()
            drain_started.set()
            try:
                connection.drain_handlers(timeout_seconds=5)
            except CodexAppServerError as exc:
                drain_errors.append(str(exc))
            drain_elapsed.append(time.monotonic() - started_at)

        drain_thread = threading.Thread(target=drain_handlers)
        drain_thread.start()
        assert drain_started.wait(timeout=1)
        deadline = time.monotonic() + 1
        while connection._handler_queue.qsize() < 1:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        websocket.messages.put(None)
        drain_thread.join(timeout=1)

        assert not drain_thread.is_alive()
        assert drain_errors == [
            "Codex app-server connection closed before callbacks drained"
        ]
        assert drain_elapsed[0] < 1
    finally:
        callback_release.set()
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_protocol_request_barrier_orders_post_response_callback(
    tmp_path,
    monkeypatch,
):
    class ManualWebSocket:
        def __init__(self):
            self.messages = queue.Queue()
            self.messages.put(
                json.dumps(
                    {
                        "id": 1,
                        "result": {
                            "codexHome": "/home/test/.codex",
                            "userAgent": "codex-test",
                        },
                    }
                )
            )
            self.sent = queue.Queue()
            self.closed = False

        def send(self, encoded):
            self.sent.put(json.loads(encoded))

        def recv(self, timeout=None):
            try:
                return self.messages.get(timeout=timeout)
            except queue.Empty as exc:
                raise TimeoutError from exc

        def close(self):
            self.closed = True

    listener, socket_path, codex_home = _private_socket(tmp_path)
    websocket = ManualWebSocket()
    callback_seen = threading.Event()
    monkeypatch.setattr(
        "websockets.sync.client.unix_connect",
        lambda *args, **kwargs: websocket,
    )
    connection = CodexAppServerConnection(
        str(socket_path),
        client_name="test",
        client_title="Test",
        client_version="1",
        request_timeout_seconds=1,
        notification_handler=lambda message: callback_seen.set(),
    )

    def next_request(method):
        while True:
            message = websocket.sent.get(timeout=1)
            if message.get("method") == method and "id" in message:
                return message

    errors = []

    def run_request(method):
        try:
            connection.request(method, {"threadId": "one"})
        except Exception as exc:
            errors.append(exc)

    try:
        connection.connect()
        resume_thread = threading.Thread(
            target=run_request,
            args=("thread/resume",),
        )
        resume_thread.start()
        resume_request = next_request("thread/resume")
        websocket.messages.put(
            json.dumps({"id": resume_request["id"], "result": {}})
        )
        websocket.messages.put(
            json.dumps(
                {
                    "method": "serverRequest/resolved",
                    "params": {"threadId": "one", "requestId": 42},
                }
            )
        )
        resume_thread.join(timeout=2)
        assert not resume_thread.is_alive()

        barrier_thread = threading.Thread(
            target=run_request,
            args=("thread/read",),
        )
        barrier_thread.start()
        barrier_request = next_request("thread/read")
        websocket.messages.put(
            json.dumps({"id": barrier_request["id"], "result": {}})
        )
        barrier_thread.join(timeout=2)
        assert not barrier_thread.is_alive()

        connection.drain_handlers()

        assert errors == []
        assert callback_seen.is_set()
    finally:
        connection.close()
        listener.close()
        shutil.rmtree(codex_home)


def test_daemon_start_and_socket_wait_share_timeout_budget(
    tmp_path,
    monkeypatch,
):
    clock = {"now": 0.0}
    run_timeouts = []

    def fake_run(*args, timeout, **kwargs):
        del args, kwargs
        run_timeouts.append(timeout)
        clock["now"] = 1.5
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_sleep(seconds):
        clock["now"] += seconds

    monkeypatch.setattr(codex_app_server_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        codex_app_server_module.time,
        "monotonic",
        lambda: clock["now"],
    )
    monkeypatch.setattr(codex_app_server_module.time, "sleep", fake_sleep)
    socket_path = (
        tmp_path / ".codex" / "app-server-control" / "app-server-control.sock"
    )

    with pytest.raises(CodexAppServerError, match="did not create"):
        start_codex_app_server_daemon(
            "codex",
            str(socket_path),
            timeout_seconds=2.0,
        )

    assert run_timeouts == [pytest.approx(2.0)]
    assert clock["now"] == pytest.approx(2.0)


def test_no_tmux_notification_thread_claims_app_server_route(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "session_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    calls = []
    monkeypatch.setattr(
        bot,
        "_is_agent_notify_tmux_thread",
        lambda *args: False,
    )
    monkeypatch.setattr(
        bot,
        "_codex_app_binding_for_route",
        lambda *args: binding,
    )
    monkeypatch.setattr(
        bot,
        "_maybe_handle_codex_app",
        lambda text, route, user, **kwargs: (
            calls.append((text, route, user, kwargs)) or True
        ),
    )
    monkeypatch.setattr(
        bot,
        "_run_codex_for_route",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("must not launch a separate Codex session")
        ),
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D111",
            "thread_ts": "1.000001",
            "ts": "2.000001",
            "text": "continue this exact session",
            "_sneeze_event_id": "event-one",
        }
    )

    assert calls[0][0] == "continue this exact session"
    assert calls[0][2] == "U111"
    assert calls[0][3] == {
        "event_id": "event-one",
        "binding": binding,
    }


def test_tmux_binding_takes_precedence_over_app_server(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    tmux_calls = []
    monkeypatch.setattr(
        bot,
        "_is_agent_notify_tmux_thread",
        lambda *args: True,
    )
    monkeypatch.setattr(
        bot,
        "_codex_app_binding_for_route",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("app binding must not be considered")
        ),
    )
    monkeypatch.setattr(
        bot,
        "_maybe_handle_agent_tmux",
        lambda text, route, **kwargs: tmux_calls.append(text) or True,
    )
    monkeypatch.setattr(
        bot,
        "_maybe_handle_codex_app",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("app controller must not be used")
        ),
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D111",
            "thread_ts": "1.000001",
            "ts": "2.000001",
            "text": "continue",
        }
    )

    assert tmux_calls == ["continue"]


def test_review_turn_revokes_cached_app_server_candidate(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path)
    state_path = _seed_cached_app_server_candidate(bot)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    with controller.lock:
        controller.active_turn_id = "turn-one"
        controller.thread_status = "active"
        controller.terminal = False
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(controller, status="active")
    review_metadata = CodexSessionMetadata(
        session_id="desktop-session",
        source="vscode",
        thread_source="user",
        cwd="/workspace/exact",
        rollout_path=None,
        parent_thread_id=None,
        review_turn=True,
        verified=True,
    )
    monkeypatch.setattr(
        slackbot_module,
        "resolve_codex_session_metadata",
        lambda *args, **kwargs: review_metadata,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("review hook must not post")
        ),
    )
    payload = {
        "session_id": "desktop-session",
        "turn_id": "turn-review",
        "hook_event_name": "Stop",
        "last_assistant_message": "Review complete.",
    }

    result = slackbot_module.notify_codex_agent_stop(
        bot.config.profile,
        json.dumps(payload),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "automated-review-session"
    entry = slackbot_module.read_json(state_path, {})["sessions"][
        "session:desktop-session"
    ]
    assert "codex_app_server" not in entry
    assert entry["codex_app_server_revoked_reason"] == (
        "automated-review-session"
    )
    revoked_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert revoked_job["status"] == "revoked"
    assert revoked_job["revoked_reason"] == "automated-review-session"

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 42,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )

    assert controller.invalidated_snapshot() is True
    assert controller.route_key not in bot.codex_app_controllers
    assert FakeControllerConnection.instances[-1].closed is True
    assert (
        controller.route_key
        not in slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"]
    )
    state = slackbot_module.read_json(state_path, {})
    state["sessions"]["session:desktop-session"].pop("session_id", None)
    slackbot_module.write_json_atomic(state_path, state)
    stale_receipt_path = (
        slackbot_module._codex_hook_delivery_route_receipt_path(
            str(state_path),
            channel_id="D111",
            thread_ts="1.000001",
            notify_user_id="U111",
        )
    )
    slackbot_module.write_json_atomic(
        stale_receipt_path,
        {
            "version": 1,
            "delivery_state": "delivered",
            "key": "session:desktop-session",
            "notify_user_id": "U111",
            "channel_id": "D111",
            "thread_ts": "1.000001",
            "turn_id": "turn-eligible",
            "message_sha256": "old-message",
            "session_id": "desktop-session",
            "notified_at": "2026-07-21T00:00:00Z",
            "codex_app_server": {
                "kind": "codex_app_server",
                "thread_id": "desktop-session",
                "session_id": "desktop-session",
                "source": "vscode",
                "cwd": "/workspace/exact",
                "verified_at": "2026-07-21T00:00:00Z",
            },
        },
    )
    eligible_metadata = replace(review_metadata, review_turn=False)
    monkeypatch.setattr(
        slackbot_module,
        "resolve_codex_session_metadata",
        lambda *args, **kwargs: eligible_metadata,
    )
    assert (
        bot._codex_app_binding_for_route(
            SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
            "U111",
        )
        is None
    )


def test_recovery_rejects_stale_job_after_session_revocation(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path)
    _seed_cached_app_server_candidate(bot)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    slackbot_module._revoke_codex_app_server_candidates(
        bot.config,
        "desktop-session",
        reason="automated-review-session",
    )
    slackbot_module._update_codex_app_job(
        bot.config,
        route_key,
        {
            "status": "active",
            "route": {
                "channel_id": route.channel_id,
                "thread_ts": route.thread_ts,
            },
            "binding": binding,
            "thread_id": "desktop-session",
            "updated_at": "2026-07-21T00:00:00Z",
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    bot._recover_codex_app_jobs()

    assert route_key not in bot.codex_app_controllers
    assert FakeControllerConnection.instances == []
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][route_key]
    assert saved_job["status"] == "revoked"
    assert saved_job["revoked_reason"] == "current-session-ineligible"


def test_degraded_metadata_revalidates_and_revokes_cached_candidate(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    state_path = _seed_cached_app_server_candidate(bot)
    degraded_metadata = CodexSessionMetadata(
        session_id="desktop-session",
        source=None,
        thread_source=None,
        cwd=None,
        rollout_path=None,
        parent_thread_id=None,
        review_turn=False,
        verified=False,
        error="unreadable-state-db",
    )
    monkeypatch.setattr(
        slackbot_module,
        "resolve_codex_session_metadata",
        lambda *args, **kwargs: degraded_metadata,
    )
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")

    assert bot._codex_app_binding_for_route(route, "U111") is None

    monkeypatch.setattr(
        slackbot_module,
        "resolve_codex_agent_tmux_binding",
        lambda *args, **kwargs: None,
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: (
            posts.append((recovered_route, text))
            or {"channel": "D111", "ts": "2.000002"}
        ),
    )
    payload = {
        "session_id": "desktop-session",
        "turn_id": "turn-degraded",
        "hook_event_name": "Stop",
        "last_assistant_message": "Completed with degraded metadata.",
    }

    result = slackbot_module.notify_codex_agent_stop(
        bot.config.profile,
        json.dumps(payload),
    )

    assert result["status"] == "notified"
    assert len(posts) == 1
    entry = slackbot_module.read_json(state_path, {})["sessions"][
        "session:desktop-session"
    ]
    assert "codex_app_server" not in entry
    assert entry["codex_app_server_revoked_reason"] == "unreadable-state-db"


def test_missing_daemon_is_started_once_before_socket_revalidation(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    validations = []
    starts = []
    probes = []

    def fake_validate(socket_path):
        validations.append(socket_path)
        if len(validations) == 1:
            raise CodexAppServerError("socket is missing")
        return Path(socket_path)

    monkeypatch.setattr(
        slackbot_module,
        "validate_codex_app_server_socket",
        fake_validate,
    )
    monkeypatch.setattr(
        slackbot_module,
        "start_codex_app_server_daemon",
        lambda codex_bin, socket_path: starts.append(
            (codex_bin, socket_path)
        ),
    )
    monkeypatch.setattr(
        bot,
        "_probe_codex_app_server",
        lambda socket_path: probes.append(socket_path),
    )

    socket_path = bot._ensure_codex_app_server()

    assert validations == [socket_path, socket_path]
    assert starts == [(bot.config.codex_bin, socket_path)]
    assert probes == [socket_path]


def test_stale_daemon_socket_triggers_restart_and_reprobe(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    validations = []
    starts = []
    probes = []

    monkeypatch.setattr(
        slackbot_module,
        "validate_codex_app_server_socket",
        lambda socket_path: validations.append(socket_path)
        or Path(socket_path),
    )

    def probe(socket_path):
        probes.append(socket_path)
        if len(probes) == 1:
            raise CodexAppServerError("stale socket")

    monkeypatch.setattr(bot, "_probe_codex_app_server", probe)
    monkeypatch.setattr(
        slackbot_module,
        "start_codex_app_server_daemon",
        lambda codex_bin, socket_path: starts.append(
            (codex_bin, socket_path)
        ),
    )

    socket_path = bot._ensure_codex_app_server()

    assert validations == [socket_path, socket_path]
    assert starts == [(bot.config.codex_bin, socket_path)]
    assert probes == [socket_path, socket_path]


def test_process_codex_home_overrides_env_file_for_socket(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    env_codex_home = tmp_path / "from-env-file"
    process_codex_home = tmp_path / "from-process"
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            default_codex_app_server_socket=None,
        ),
        env={**bot.config.env, "CODEX_HOME": str(env_codex_home)},
    )
    monkeypatch.setenv("CODEX_HOME", str(process_codex_home))
    expected = str(
        process_codex_home / "app-server-control" / "app-server-control.sock"
    )

    assert bot._codex_app_server_socket() == expected
    assert (
        query_codex_app_server_status(bot.config)["socket_path"] == expected
    )


def test_status_counts_desktop_required_as_active_saved_job(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                "recoverable": {"status": "desktop-required"},
                "terminal": {"status": "completed"},
            }
        },
    )

    status = query_codex_app_server_status(bot.config)

    assert status["saved_jobs"] == 2
    assert status["active_saved_jobs"] == 1


def test_controller_resumes_exact_thread_and_starts_then_steers(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)

    controller.connect()
    turn_id = controller.start_turn("continue", "client-one")
    steered_turn = controller.steer("one more thing", "client-two")

    connection = FakeControllerConnection.instances[-1]
    assert turn_id == "turn-started"
    assert steered_turn == turn_id
    assert (
        "thread/resume",
        {
            "threadId": "desktop-session",
            "excludeTurns": True,
        },
    ) in connection.requests
    assert connection.drain_calls == 1
    assert connection.drain_timeouts == [
        slackbot_module.CODEX_APP_HANDLER_DRAIN_TIMEOUT_SECONDS
    ]
    assert connection.drain_timeouts[0] > 150.0
    assert connection.events[:4] == [
        "thread/read",
        "thread/resume",
        "thread/read",
        "drain",
    ]
    assert any(
        method == "turn/start"
        and params["clientUserMessageId"] == "client-one"
        for method, params in connection.requests
    )
    assert any(
        method == "turn/steer" and params["expectedTurnId"] == "turn-started"
        for method, params in connection.requests
    )


def test_start_turn_recovery_refreshes_completed_turn_state(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    connection.turn_history = [
        {
            "id": "turn-completed",
            "status": "completed",
            "items": [
                {
                    "type": "userMessage",
                    "clientId": "client-one",
                }
            ],
        }
    ]
    original_request = connection.request

    def ambiguous_start(method, params):
        if method == "turn/start":
            connection.events.append(method)
            connection.requests.append((method, params))
            raise CodexAppServerError("turn/start response was lost")
        return original_request(method, params)

    connection.request = ambiguous_start

    turn_id = controller.start_turn("continue", "client-one")

    assert turn_id == "turn-completed"
    assert controller.last_action_reused is True
    assert controller.active_turn_id is None
    assert controller.thread_status == "idle"
    assert controller.terminal is True


def test_controller_connect_does_not_hold_global_controller_lock(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    map_lock_acquired = threading.Event()

    class LockProbeConnection(FakeControllerConnection):
        instances = []

        def connect(self):
            def probe_map_lock():
                with bot.codex_app_controllers_lock:
                    map_lock_acquired.set()

            probe = threading.Thread(target=probe_map_lock)
            probe.start()
            assert map_lock_acquired.wait(timeout=2)
            probe.join(timeout=2)
            assert not probe.is_alive()
            return super().connect()

    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        LockProbeConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }

    controller = bot._get_codex_app_controller(route, binding)

    assert map_lock_acquired.is_set()
    assert bot.codex_app_controllers[controller.route_key] is controller


def test_controller_revoked_during_connect_is_not_installed(
    tmp_path,
    monkeypatch,
):
    connect_started = threading.Event()
    release_connect = threading.Event()

    class BlockingConnection(FakeControllerConnection):
        instances = []

        def connect(self):
            connect_started.set()
            assert release_connect.wait(timeout=2)
            return super().connect()

    bot = _bot(tmp_path, notification_only=False)
    _seed_cached_app_server_candidate(bot)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        BlockingConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    errors = []

    def connect_controller():
        try:
            bot._get_codex_app_controller(route, binding)
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=connect_controller)
    worker.start()
    assert connect_started.wait(timeout=2)
    slackbot_module._revoke_codex_app_server_candidates(
        bot.config,
        "desktop-session",
        reason="automated-review-session",
    )
    release_connect.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(
        errors[0],
        slackbot_module.CodexAppIneligibleControllerError,
    )
    assert bot.codex_app_controllers == {}
    assert BlockingConnection.instances[-1].closed is True


def test_controller_refresh_ignores_turn_list_request_error(
    tmp_path,
    monkeypatch,
):
    class UnsupportedTurnListConnection(FakeControllerConnection):
        instances = []

        def request(self, method, params):
            if (
                method == "thread/turns/list"
                and params.get("itemsView") == "summary"
            ):
                self.events.append(method)
                self.requests.append((method, params))
                raise CodexAppServerRequestError(
                    method,
                    {"code": -32601, "message": "Method not found"},
                )
            return super().request(method, params)

    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        UnsupportedTurnListConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )

    controller.connect()

    connection = UnsupportedTurnListConnection.instances[-1]
    assert connection.connected is True
    assert controller.thread_status == "idle"
    assert controller.active_turn_id is None
    assert controller.terminal is True


@pytest.mark.parametrize(
    "recovery_method",
    (
        "recover_start_turn",
        "recover_steer",
    ),
)
def test_recovered_input_reuses_existing_client_id_without_dispatch(
    tmp_path,
    monkeypatch,
    recovery_method,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    if recovery_method == "recover_start_turn":
        connection.turn_history = [
            {
                "id": "turn-existing",
                "status": "completed",
                "items": [
                    {
                        "type": "userMessage",
                        "clientId": "client-one",
                    }
                ],
            }
        ]
    else:
        connection.active_turn_id = "turn-existing"
        connection.turn_items = [
            {
                "type": "userMessage",
                "clientId": "client-one",
            }
        ]

    turn_id = getattr(controller, recovery_method)("continue", "client-one")

    assert turn_id == "turn-existing"
    assert controller.last_action_reused is True
    dispatches = [
        method
        for method, _ in connection.requests
        if method in {"turn/start", "turn/steer"}
    ]
    assert dispatches == []


@pytest.mark.parametrize(
    ("recovery_method", "dispatch_method", "needs_active_turn"),
    (
        ("recover_start_turn", "turn/start", False),
        ("recover_steer", "turn/steer", True),
    ),
)
def test_recovered_input_missing_client_id_dispatches_once(
    tmp_path,
    monkeypatch,
    recovery_method,
    dispatch_method,
    needs_active_turn,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    if needs_active_turn:
        connection.active_turn_id = "turn-existing"
        history_status = "inProgress"
        history_turn_id = "turn-existing"
    else:
        history_status = "completed"
        history_turn_id = "turn-completed"
    connection.turn_history = [
        {
            "id": history_turn_id,
            "status": history_status,
            "items": [
                {
                    "type": "userMessage",
                    "clientId": "client-other",
                }
            ],
        }
    ]

    getattr(controller, recovery_method)("continue", "client-missing")

    dispatches = [
        method
        for method, _ in connection.requests
        if method in {"turn/start", "turn/steer"}
    ]
    assert dispatches == [dispatch_method]


def test_recovered_input_searches_all_turn_history_pages(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    connection.turn_history_pages = {
        None: {
            "data": [
                {
                    "id": "turn-newer",
                    "status": "completed",
                    "items": [],
                }
            ],
            "nextCursor": "older-turns",
        },
        "older-turns": {
            "data": [
                {
                    "id": "turn-existing",
                    "status": "completed",
                    "items": [
                        {
                            "type": "userMessage",
                            "clientId": "client-one",
                        }
                    ],
                }
            ],
            "nextCursor": None,
        },
    }

    turn_id = controller.recover_start_turn("continue", "client-one")

    assert turn_id == "turn-existing"
    assert controller.last_action_reused is True
    turn_list_requests = [
        params
        for method, params in connection.requests
        if method == "thread/turns/list" and params.get("itemsView") == "full"
    ]
    assert [params.get("cursor") for params in turn_list_requests] == [
        None,
        "older-turns",
    ]
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )


def test_recovery_history_failure_does_not_dispatch_duplicate_input(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    original_request = connection.request

    def failing_history_request(method, params):
        if (
            method == "thread/turns/list"
            and params.get("itemsView") == "full"
        ):
            raise CodexAppServerError("history unavailable")
        return original_request(method, params)

    connection.request = failing_history_request

    with pytest.raises(CodexAppServerError, match="history unavailable"):
        controller.recover_start_turn("continue", "client-one")

    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )


@pytest.mark.parametrize(
    "malformed_turns",
    (
        [None],
        [{"id": "turn-one", "status": "completed", "items": None}],
        [{"id": "turn-one", "status": "completed", "items": [None]}],
    ),
)
def test_recovery_history_malformed_page_does_not_dispatch(
    tmp_path,
    monkeypatch,
    malformed_turns,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    connection.turn_history = malformed_turns

    with pytest.raises(CodexAppServerError, match="malformed"):
        controller.recover_start_turn("continue", "client-one")

    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )


def test_recovery_history_pagination_has_a_hard_limit(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    original_request = connection.request
    pages_read = 0

    def endless_history_request(method, params):
        nonlocal pages_read
        if (
            method == "thread/turns/list"
            and params.get("itemsView") == "full"
        ):
            pages_read += 1
            return {"data": [], "nextCursor": f"cursor-{pages_read}"}
        return original_request(method, params)

    connection.request = endless_history_request

    with pytest.raises(CodexAppServerError, match="page limit"):
        controller.recover_start_turn("continue", "client-one")

    assert pages_read == slackbot_module.CODEX_APP_RECOVERY_MAX_TURN_PAGES
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )


@pytest.mark.parametrize(
    "recovery_error",
    (
        CodexAppRecoveryExhaustedError("page limit"),
        CodexAppServerProtocolError("malformed history"),
    ),
)
def test_deterministic_preparing_recovery_requires_desktop(
    tmp_path,
    monkeypatch,
    recovery_error,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        "recover_start_turn",
        lambda *args, **kwargs: (_ for _ in ()).throw(recovery_error),
    )
    records = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, **fields: records.append(fields),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    assert bot._recover_codex_app_job(
        controller.route_key,
        {
            "status": "preparing",
            "binding": binding,
            "client_message_id": "client-one",
            "text_sha256": hashlib.sha256(b"continue").hexdigest(),
            "preparing_action": "start",
            "preparing_text": "continue",
        },
        route,
        binding,
    )

    assert controller.recovery_blocked_snapshot() is False
    assert controller.desktop_required_snapshot() is True
    assert records[-1]["status"] == "desktop-required"


@pytest.mark.parametrize(
    ("request_error", "retry_exhausted", "expected_reason"),
    (
        (
            CodexAppServerRequestError(
                "thread/turns/list",
                {"code": -32602, "message": "thread not found"},
            ),
            False,
            "permanent-request-error",
        ),
        (
            CodexAppServerRequestError(
                "thread/turns/list",
                {"code": -32000, "message": "server busy"},
            ),
            True,
            "transient-request-retries-exhausted",
        ),
    ),
)
def test_request_error_preparing_recovery_requires_bounded_desktop_handoff(
    tmp_path,
    monkeypatch,
    request_error,
    retry_exhausted,
    expected_reason,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    if retry_exhausted:
        controller.restore_recovery_retry(
            time.time(),
            slackbot_module.CODEX_APP_RECOVERY_RETRY_MAX_SECONDS,
        )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        "recover_start_turn",
        lambda *args, **kwargs: (_ for _ in ()).throw(request_error),
    )
    records = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, **fields: records.append(fields),
    )
    audits = []
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append((action, fields)),
    )

    assert bot._recover_codex_app_job(
        controller.route_key,
        {
            "status": "preparing",
            "binding": binding,
            "client_message_id": "client-one",
            "text_sha256": hashlib.sha256(b"continue").hexdigest(),
            "preparing_action": "start",
            "preparing_text": "continue",
        },
        route,
        binding,
    )

    assert controller.desktop_required_snapshot() is True
    assert controller.recovery_blocked_snapshot() is False
    assert records[-1]["status"] == "desktop-required"
    assert audits[-1][0] == "preparing_recovery_rejected"
    assert audits[-1][1]["reason"] == expected_reason


@pytest.mark.parametrize(
    ("preparing_action", "active_turn_id", "expected_status"),
    (("start", "turn-other", "active"), ("steer", None, "completed")),
)
def test_stale_preparing_action_is_rejected_before_dispatch(
    tmp_path,
    monkeypatch,
    preparing_action,
    active_turn_id,
    expected_status,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    connection.active_turn_id = active_turn_id
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    records = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, **fields: records.append(fields),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, message: posts.append(message),
    )

    assert bot._recover_codex_app_job(
        controller.route_key,
        {
            "status": "preparing",
            "binding": binding,
            "client_message_id": "client-one",
            "text_sha256": hashlib.sha256(b"continue").hexdigest(),
            "preparing_action": preparing_action,
            "preparing_text": "continue",
            "preparing_turn_id": (
                "turn-original" if preparing_action == "steer" else None
            ),
        },
        route,
        binding,
    )

    assert controller.recovery_blocked_snapshot() is False
    assert controller.desktop_required_snapshot() is False
    assert records[-1]["status"] == expected_status
    assert records[-1]["clear_client_identity"] is True
    assert "was not sent" in posts[-1]
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )


def test_controller_closes_connection_when_initial_refresh_fails(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    connection = FakeControllerConnection()
    original_request = connection.request
    thread_reads = 0

    def failing_request(method, params):
        nonlocal thread_reads
        if method == "thread/read":
            thread_reads += 1
            if thread_reads == 2:
                raise CodexAppServerError("refresh failed")
        return original_request(method, params)

    connection.request = failing_request
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        lambda *args, **kwargs: connection,
    )
    controller = CodexAppController(bot, route, binding)

    with pytest.raises(CodexAppServerError, match="refresh failed"):
        controller.connect()

    assert connection.closed is True
    assert controller.connection is None


def test_recovery_waits_for_fresh_pending_request_id(tmp_path, monkeypatch):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.connection = SimpleNamespace(connected=True)
    request_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    pending = {
        "request_id": 42,
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-one",
        "expires_at": time.time() + 60,
        "request_fingerprint": bot._codex_app_server_request_fingerprint(
            "item/commandExecution/requestApproval",
            request_params,
        ),
        "slack_client_msg_id": "request-client-id",
        "slack_delivery_state": "pending",
    }
    controller.recover_pending_request(pending)
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                controller.route_key: {
                    "status": "pending-request",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "pending_request": pending,
                }
            }
        },
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda recovered_route, recovered_binding, **kwargs: controller,
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )

    bot._recover_codex_app_jobs()

    assert controller.pending_snapshot() is None
    persisted = controller.pending_persistence_snapshot()
    assert persisted == pending
    assert statuses == ["awaiting-request-replay"]

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 99,
            "method": pending["method"],
            "params": request_params,
        },
    )

    assert controller.pending_snapshot()["request_id"] == 99
    assert statuses[-1] == "pending-request"
    assert posts


def test_recovered_request_resolution_requires_matching_request_id(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    controller = CodexAppController(
        bot,
        route,
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 60,
        }
    )
    records = []
    posts = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: records.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, message: posts.append(message),
    )

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 41,
            },
        },
    )

    assert controller.pending_replay_snapshot()["request_id"] == 42
    assert records == []
    assert posts == []

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )

    assert controller.pending_replay_snapshot() is None
    assert records == ["active"]
    assert posts


def test_desktop_handoff_resolution_requires_matching_request_id(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.mark_desktop_required(42)
    records = []
    posts = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: records.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, message: posts.append(message),
    )

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 41,
            },
        },
    )

    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_id_snapshot() == 42
    assert records == []
    assert posts == []

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )

    assert controller.desktop_required_snapshot() is False
    assert controller.desktop_request_id_snapshot() is None
    assert records == ["active"]
    assert "Slack controls are available again" in posts[-1]


def test_handoff_resolution_reports_blocked_input_recovery(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.mark_desktop_required(42)
    controller.mark_recovery_blocked()
    records = []
    posts = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: records.append(status),
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, route, text: posts.append(text),
    )

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )

    assert controller.desktop_required_snapshot() is False
    assert controller.recovery_blocked_snapshot() is True
    assert records == ["preparing"]
    assert "input recovery is still pending" in posts[-1]
    assert "controls remain closed" in posts[-1]


def test_reissued_desktop_request_uses_new_connection_request_id(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    request_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    request_fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        request_params,
    )
    controller.mark_desktop_required(42, request_fingerprint)
    bot._record_codex_app_controller(
        controller,
        status="desktop-required",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": request_params,
        },
    )

    assert controller.desktop_request_id_snapshot() == 99
    assert controller.desktop_request_ids_snapshot() == [99]
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        controller.route_key
    ]
    assert job["desktop_request_id"] == 99
    assert job["desktop_request_ids"] == [99]
    assert job["desktop_request_fingerprint"] == request_fingerprint

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == [99]

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 99,
            },
        },
    )
    assert controller.desktop_required_snapshot() is False
    assert controller.desktop_request_ids_snapshot() == []


def test_different_desktop_request_makes_handoff_sticky(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    original_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    controller.mark_desktop_required(
        42,
        bot._codex_app_server_request_fingerprint(
            "item/commandExecution/requestApproval",
            original_params,
        ),
    )
    bot._record_codex_app_controller(
        controller,
        status="desktop-required",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 42,
            "method": "item/commandExecution/requestApproval",
            "params": {**original_params, "command": "git diff"},
        },
    )

    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_id_snapshot() is None
    assert controller.desktop_request_ids_snapshot() == []
    assert controller.desktop_request_fingerprint_snapshot() is None
    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )
    assert controller.desktop_required_snapshot() is True


@pytest.mark.parametrize(
    "expires_at", ("invalid", float("inf"), float("nan"))
)
def test_invalid_pending_deadline_expires_safely(
    tmp_path,
    expires_at,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": expires_at,
            "request_fingerprint": "request-fingerprint",
        }
    )

    assert controller.expired(time.time()) is True


def test_missing_pending_deadline_does_not_expire_recovery(tmp_path):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": None,
            "request_fingerprint": "request-fingerprint",
        }
    )
    controller.mark_recovery_blocked()
    now = time.time()

    assert controller.expired(now) is False
    assert controller.expire_pending_during_recovery(now) is None


def test_legacy_replay_without_request_id_fails_closed_to_desktop(
    tmp_path,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )

    status = controller.recover_pending_request(
        {
            "request_id": None,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 60,
        }
    )

    assert status == "desktop-required"
    assert controller.pending_replay_snapshot()["request_id"] is None
    assert controller.desktop_required_snapshot() is True
    assert (
        controller.set_pending_request(
            {
                "request_id": 99,
                "method": "item/commandExecution/requestApproval",
                "turn_id": "turn-one",
                "expires_at": time.time() + 60,
            }
        )
        is False
    )


def test_restart_adopts_matching_legacy_request_reissue(
    tmp_path,
    monkeypatch,
):
    params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }

    class LegacyReissueConnection(FakeControllerConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"
            self.reissued = False

        def request(self, method, request_params):
            result = super().request(method, request_params)
            if method == "thread/resume" and not self.reissued:
                self.reissued = True
                self.server_request_handler(
                    {
                        "id": 99,
                        "method": "item/commandExecution/requestApproval",
                        "params": params,
                    }
                )
            return result

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original = CodexAppController(bot, route, binding)
    original.recover_pending_request(
        {
            "request_id": None,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 60,
            "request_fingerprint": (
                bot._codex_app_server_request_fingerprint(
                    "item/commandExecution/requestApproval",
                    params,
                )
            ),
        }
    )
    bot._record_codex_app_controller(
        original,
        status="desktop-required",
    )
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        original.route_key
    ]
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        LegacyReissueConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    recovered = bot._get_codex_app_controller(
        route,
        binding,
        record_connected=False,
        pending_recovery=job["pending_request"],
    )

    assert recovered.desktop_required_snapshot() is True
    assert recovered.desktop_request_ids_snapshot() == [99]
    assert recovered.pending_replay_snapshot() is None
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][original.route_key]
    assert saved_job["desktop_request_ids"] == [99]
    bot._handle_codex_app_notification(
        recovered,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 99,
            },
        },
    )
    assert recovered.desktop_required_snapshot() is False


def test_explicit_pending_recovery_cannot_override_persisted_desktop_state(
    tmp_path,
    monkeypatch,
):
    class ActiveConnection(FakeControllerConnection):
        def connect(self):
            self.active_turn_id = "turn-one"
            return super().connect()

    ActiveConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    pending = {
        "request_id": 42,
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-one",
        "expires_at": time.time() + 60,
    }
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                route_key: {
                    "status": "desktop-required",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "pending_request": pending,
                }
            }
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ActiveConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    controller = bot._get_codex_app_controller(
        route,
        binding,
        pending_recovery=pending,
    )

    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_id_snapshot() == 42
    assert controller.pending_snapshot() is None
    assert controller.pending_replay_snapshot() is None


def test_expired_recovery_without_reissue_clears_replay_state(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.connection = SimpleNamespace(connected=True)
    pending = {
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-one",
        "expires_at": time.time() - 1,
        "request_fingerprint": "expired-request",
    }
    controller.recover_pending_request(pending)
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                controller.route_key: {
                    "status": "pending-request",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "pending_request": pending,
                }
            }
        },
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    assert controller.pending_replay_snapshot() is None
    assert controller.desktop_required_snapshot() is True
    assert statuses[-1] == "desktop-required"


def test_expired_replay_during_preparing_recovery_finalizes_desktop(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original = CodexAppController(bot, route, binding)
    original.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() - 1,
            "request_fingerprint": "expired-request",
        }
    )
    original.mark_recovery_blocked()
    text = "recover this input"
    bot._record_codex_app_controller(
        original,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(text.encode()).hexdigest(),
        preparing_action="start",
        preparing_text=text,
    )
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        original.route_key
    ]
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    assert bot._recover_codex_app_job(
        original.route_key,
        job,
        route,
        binding,
    )

    recovered = bot.codex_app_controllers[original.route_key]
    assert recovered.recovery_blocked_snapshot() is False
    assert recovered.recovery_handoff_required_snapshot() is False
    assert recovered.desktop_required_snapshot() is True
    assert recovered.desktop_request_id_snapshot() == 42
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][original.route_key]
    assert saved_job["status"] == "desktop-required"
    assert saved_job["pending_request"] is None
    assert saved_job["desktop_request_id"] == 42


def test_fresh_pending_request_wins_over_expired_persisted_replay(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.connection = SimpleNamespace(connected=True)
    fresh_pending = {
        "request_id": 99,
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-new",
        "expires_at": time.time() + 30,
        "request_fingerprint": "fresh-request",
    }
    assert controller.set_pending_request(fresh_pending) is True
    expired_persisted = {
        "request_id": 42,
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-old",
        "expires_at": time.time() - 1,
        "request_fingerprint": "expired-request",
    }
    job = {
        "status": "pending-request",
        "binding": binding,
        "pending_request": expired_persisted,
    }
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    assert bot._recover_codex_app_job(
        controller.route_key,
        job,
        route,
        binding,
    )

    assert controller.pending_snapshot() == fresh_pending
    assert controller.desktop_required_snapshot() is False
    assert statuses == ["pending-request"]


@pytest.mark.parametrize("expiry_delta", (30, -1))
def test_reconnect_seeds_replay_state_before_resume_request(
    tmp_path,
    monkeypatch,
    expiry_delta,
):
    params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }

    class ReissuingConnection(FakeControllerConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.reissued = False

        def request(self, method, request_params):
            if method == "thread/resume" and not self.reissued:
                self.reissued = True
                self.active_turn_id = "turn-one"
                self.server_request_handler(
                    {
                        "id": 99,
                        "method": "item/commandExecution/requestApproval",
                        "params": params,
                    }
                )
            return super().request(method, request_params)

    ReissuingConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original_expiry = time.time() + expiry_delta
    fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        params,
    )
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                route_key: {
                    "status": "pending-request",
                    "binding": binding,
                    "pending_request": {
                        "request_id": 42,
                        "method": "item/commandExecution/requestApproval",
                        "turn_id": "turn-one",
                        "expires_at": original_expiry,
                        "request_fingerprint": fingerprint,
                        "slack_client_msg_id": "request-client-id",
                        "slack_delivery_state": "pending",
                    },
                }
            }
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ReissuingConnection,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    controller = bot._get_codex_app_controller(route, binding)

    pending = controller.pending_snapshot()
    if expiry_delta > 0:
        assert pending["request_id"] == 99
        assert pending["expires_at"] == original_expiry
        assert statuses[-1] == "pending-request"
        assert controller.desktop_required_snapshot() is False
    else:
        assert pending is None
        assert statuses[-1] == "desktop-required"
        assert controller.desktop_required_snapshot() is True
    assert controller.pending_replay_snapshot() is None


def test_reconnect_unsupported_request_ends_replay_state(
    tmp_path,
    monkeypatch,
):
    unsupported_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "questions": [
            {
                "id": "token",
                "question": "Paste the token",
                "isSecret": True,
            }
        ],
    }

    class UnsupportedConnection(FakeControllerConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.reissued = False

        def request(self, method, request_params):
            if method == "thread/resume" and not self.reissued:
                self.reissued = True
                self.active_turn_id = "turn-one"
                self.server_request_handler(
                    {
                        "id": 100,
                        "method": "item/tool/requestUserInput",
                        "params": unsupported_params,
                    }
                )
            return super().request(method, request_params)

    UnsupportedConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                route_key: {
                    "status": "pending-request",
                    "binding": binding,
                    "pending_request": {
                        "method": "item/commandExecution/requestApproval",
                        "turn_id": "turn-one",
                        "expires_at": time.time() + 30,
                        "request_fingerprint": "old-request",
                    },
                }
            }
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        UnsupportedConnection,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    controller = bot._get_codex_app_controller(route, binding)

    assert controller.pending_snapshot() is None
    assert controller.pending_replay_snapshot() is None
    assert controller.desktop_required_snapshot() is True
    assert statuses[-1] == "desktop-required"


def test_resume_resolution_does_not_resurrect_persisted_request(
    tmp_path,
    monkeypatch,
):
    class ResolvingConnection(FakeControllerConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.resolved = False

        def request(self, method, request_params):
            if method == "thread/resume" and not self.resolved:
                self.resolved = True
                self.notification_handler(
                    {
                        "method": "serverRequest/resolved",
                        "params": {
                            "threadId": "desktop-session",
                            "requestId": 42,
                        },
                    }
                )
            return super().request(method, request_params)

    ResolvingConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    pending = {
        "request_id": 42,
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-one",
        "expires_at": time.time() + 30,
        "request_fingerprint": "persisted-request",
    }
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                route_key: {
                    "status": "pending-request",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "pending_request": pending,
                }
            }
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ResolvingConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    controller = bot.codex_app_controllers[route_key]
    assert controller.pending_snapshot() is None
    assert controller.pending_replay_snapshot() is None
    assert statuses[-1] == "completed"


@pytest.mark.parametrize(
    ("persisted_status", "include_pending"),
    (
        ("desktop-required", False),
        ("pending-request", True),
    ),
)
def test_terminal_restart_clears_stale_request_recovery(
    tmp_path,
    monkeypatch,
    persisted_status,
    include_pending,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    pending = (
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 30,
            "request_fingerprint": "persisted-request",
        }
        if include_pending
        else None
    )
    job = {
        "status": persisted_status,
        "updated_at": "2026-07-21T00:00:00Z",
        "route": {
            "channel_id": route.channel_id,
            "thread_ts": route.thread_ts,
        },
        "binding": binding,
        "pending_request": pending,
    }
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {"jobs": {route_key: job}},
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    controller = bot.codex_app_controllers[route_key]
    assert controller.terminal is True
    assert controller.pending_snapshot() is None
    assert controller.pending_replay_snapshot() is None
    assert controller.desktop_required_snapshot() is False
    assert statuses[-1] == "completed"


def test_recovery_limit_counts_only_successful_controllers(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            codex_app_server_max_controllers=1,
        ),
    )
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                "malformed": {
                    "status": "active",
                    "updated_at": "2026-07-21T00:00:02Z",
                    "route": {},
                    "binding": binding,
                },
                controller.route_key: {
                    "status": "active",
                    "updated_at": "2026-07-21T00:00:01Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                },
            }
        },
    )
    recovered = []
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda recovered_route, recovered_binding, **kwargs: (
            recovered.append(recovered_route) or controller
        ),
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    assert recovered == [route]


def test_startup_recovery_rechecks_capacity_inside_route_lock(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            codex_app_server_max_controllers=1,
        ),
    )
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    job = {
        "status": "active",
        "updated_at": "2026-07-21T00:00:00Z",
        "route": {
            "channel_id": route.channel_id,
            "thread_ts": route.thread_ts,
        },
        "binding": binding,
    }
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {"jobs": {route_key: job}},
    )
    concurrent = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D222", thread_ts="2.000002"),
        {**binding, "thread_id": "concurrent-session"},
    )
    original_conversation_lock = bot._conversation_lock

    @contextmanager
    def concurrent_arrival(key):
        bot.codex_app_controllers[concurrent.route_key] = concurrent
        with original_conversation_lock(key):
            yield

    monkeypatch.setattr(bot, "_conversation_lock", concurrent_arrival)
    recovery_calls = []
    monkeypatch.setattr(
        bot,
        "_recover_codex_app_job",
        lambda *args, **kwargs: recovery_calls.append((args, kwargs)),
    )

    bot._recover_codex_app_jobs()

    assert recovery_calls == []
    assert list(bot.codex_app_controllers) == [concurrent.route_key]


def test_startup_recovery_rereads_job_inside_route_lock(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    preparing_job = {
        "status": "preparing",
        "updated_at": "2026-07-21T00:00:00Z",
        "route": {
            "channel_id": route.channel_id,
            "thread_ts": route.thread_ts,
        },
        "binding": binding,
        "client_message_id": "client-stale",
        "preparing_action": "start",
        "preparing_text": "stale input",
    }
    active_job = {
        **preparing_job,
        "status": "active",
        "updated_at": "2026-07-21T00:00:01Z",
        "client_message_id": None,
        "preparing_action": None,
        "preparing_text": None,
    }
    reads = 0

    def read_jobs(paths):
        nonlocal reads
        del paths
        reads += 1
        return {
            "jobs": {
                route_key: preparing_job if reads == 1 else active_job,
            }
        }

    monkeypatch.setattr(slackbot_module, "_read_codex_app_jobs", read_jobs)
    recovered_jobs = []
    monkeypatch.setattr(
        bot,
        "_recover_codex_app_job",
        lambda key, recovered_job, recovered_route, recovered_binding: (
            recovered_jobs.append(dict(recovered_job)) or True
        ),
    )

    bot._recover_codex_app_jobs()

    assert reads == 2
    assert [job["status"] for job in recovered_jobs] == ["active"]
    assert recovered_jobs[0]["preparing_text"] is None


def test_recovery_limit_counts_retained_retry_controllers(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            codex_app_server_max_controllers=1,
        ),
    )
    first_route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    second_route = SlackbotRoute(channel_id="D222", thread_ts="2.000002")

    def job(route, thread_id, updated_at):
        return {
            "status": "preparing",
            "updated_at": updated_at,
            "route": {
                "channel_id": route.channel_id,
                "thread_ts": route.thread_ts,
            },
            "binding": {
                "kind": "codex_app_server",
                "thread_id": thread_id,
                "source": "vscode",
                "cwd": "/workspace/exact",
            },
            "client_message_id": f"client-{thread_id}",
            "preparing_action": "start",
            "preparing_text": "continue",
        }

    first_key = slackbot_module.agent_tmux_thread_key(
        first_route.channel_id,
        first_route.thread_ts,
    )
    second_key = slackbot_module.agent_tmux_thread_key(
        second_route.channel_id,
        second_route.thread_ts,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                first_key: job(
                    first_route,
                    "first-session",
                    "2026-07-21T00:00:02Z",
                ),
                second_key: job(
                    second_route,
                    "second-session",
                    "2026-07-21T00:00:01Z",
                ),
            }
        },
    )
    recovery_attempts = []

    def fail_recovery(route, binding, **kwargs):
        del binding, kwargs
        recovery_attempts.append(route)
        raise CodexAppServerProtocolError("temporary JSON-RPC failure")

    monkeypatch.setattr(bot, "_get_codex_app_controller", fail_recovery)

    bot._recover_codex_app_jobs()

    assert recovery_attempts == [first_route]
    assert list(bot.codex_app_controllers) == [first_key]
    retained = bot.codex_app_controllers[first_key]
    assert retained.recovery_blocked_snapshot() is True
    assert retained.recovery_retry_snapshot()[1] > 0


def test_deterministic_recovery_failure_is_rejected_without_using_capacity(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            codex_app_server_max_controllers=1,
        ),
    )
    rejected_route = SlackbotRoute(
        channel_id="D111",
        thread_ts="1.000001",
    )
    valid_route = SlackbotRoute(
        channel_id="D222",
        thread_ts="2.000002",
    )
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }

    def job(route, updated_at):
        return {
            "status": "active",
            "updated_at": updated_at,
            "route": {
                "channel_id": route.channel_id,
                "thread_ts": route.thread_ts,
            },
            "binding": binding,
        }

    rejected_key = slackbot_module.agent_tmux_thread_key(
        rejected_route.channel_id,
        rejected_route.thread_ts,
    )
    valid_key = slackbot_module.agent_tmux_thread_key(
        valid_route.channel_id,
        valid_route.thread_ts,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                rejected_key: job(
                    rejected_route,
                    "2026-07-21T00:00:02Z",
                ),
                valid_key: job(
                    valid_route,
                    "2026-07-21T00:00:01Z",
                ),
            }
        },
    )
    valid_controller = CodexAppController(bot, valid_route, binding)
    with valid_controller.lock:
        valid_controller.active_turn_id = "turn-one"
        valid_controller.terminal = False
    attempts = []

    def recover(route, recovered_binding, **kwargs):
        del recovered_binding, kwargs
        attempts.append(route)
        if route == rejected_route:
            raise CodexAppSessionValidationError("thread identity mismatch")
        return valid_controller

    monkeypatch.setattr(bot, "_get_codex_app_controller", recover)
    updates = []
    monkeypatch.setattr(
        slackbot_module,
        "_update_codex_app_job",
        lambda config, key, value: updates.append((key, value)),
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    assert attempts == [rejected_route, valid_route]
    assert updates[0][0] == rejected_key
    assert updates[0][1]["status"] == "recovery-rejected"


def test_recovery_limit_prioritizes_preparing_over_desktop_handoff(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            codex_app_server_max_controllers=1,
        ),
    )
    desktop_route = SlackbotRoute(
        channel_id="D111",
        thread_ts="1.000001",
    )
    preparing_route = SlackbotRoute(
        channel_id="D222",
        thread_ts="2.000002",
    )

    def job(route, thread_id, status, updated_at):
        return {
            "status": status,
            "updated_at": updated_at,
            "route": {
                "channel_id": route.channel_id,
                "thread_ts": route.thread_ts,
            },
            "binding": {
                "kind": "codex_app_server",
                "thread_id": thread_id,
                "source": "vscode",
                "cwd": "/workspace/exact",
            },
        }

    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                slackbot_module.agent_tmux_thread_key(
                    desktop_route.channel_id,
                    desktop_route.thread_ts,
                ): job(
                    desktop_route,
                    "desktop-session",
                    "desktop-required",
                    "2026-07-21T00:00:02Z",
                ),
                slackbot_module.agent_tmux_thread_key(
                    preparing_route.channel_id,
                    preparing_route.thread_ts,
                ): job(
                    preparing_route,
                    "preparing-session",
                    "preparing",
                    "2026-07-21T00:00:01Z",
                ),
            }
        },
    )
    recovered = []
    monkeypatch.setattr(
        bot,
        "_recover_codex_app_job",
        lambda key, recovered_job, route, binding: (
            recovered.append(recovered_job["status"]) or True
        ),
    )

    bot._recover_codex_app_jobs()

    assert recovered == ["preparing"]


def test_deferred_recovery_rescans_when_controller_capacity_opens(
    tmp_path,
    monkeypatch,
):
    class ActiveConnection(FakeControllerConnection):
        instances = []

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"

    bot = _bot(tmp_path, notification_only=False)
    bot.config = replace(
        bot.config,
        profile=replace(
            bot.config.profile,
            codex_app_server_max_controllers=1,
        ),
    )
    first_route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    second_route = SlackbotRoute(channel_id="D222", thread_ts="2.000002")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    first_key = slackbot_module.agent_tmux_thread_key(
        first_route.channel_id,
        first_route.thread_ts,
    )
    second_key = slackbot_module.agent_tmux_thread_key(
        second_route.channel_id,
        second_route.thread_ts,
    )

    def job(route, updated_at):
        return {
            "status": "active",
            "updated_at": updated_at,
            "route": {
                "channel_id": route.channel_id,
                "thread_ts": route.thread_ts,
            },
            "binding": binding,
            "pending_request": None,
        }

    jobs = {
        first_key: job(first_route, "2026-07-21T00:00:02Z"),
        second_key: job(second_route, "2026-07-21T00:00:01Z"),
    }
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {"jobs": jobs},
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ActiveConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    assert list(bot.codex_app_controllers) == [first_key]
    first = bot.codex_app_controllers[first_key]
    with first.lock:
        first.active_turn_id = None
        first.terminal = True
    jobs[first_key]["status"] = "completed"

    bot._tick_codex_app_controllers()

    assert list(bot.codex_app_controllers) == [second_key]
    assert bot.codex_app_controllers[second_key].active_turn_id == "turn-one"


def test_active_recovery_status_survives_two_restarts(
    tmp_path,
    monkeypatch,
):
    class ActiveConnection(FakeControllerConnection):
        instances = []

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"

    original_bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original = CodexAppController(original_bot, route, binding)
    with original.lock:
        original.active_turn_id = "turn-one"
        original.thread_status = "active"
        original.terminal = False
    original_bot._record_codex_app_controller(original, status="active")
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ActiveConnection,
    )

    first_restart = SlackSocketBot(original_bot.config)
    monkeypatch.setattr(
        first_restart,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(
        first_restart,
        "_audit_codex_app",
        lambda *args, **kwargs: None,
    )
    first_restart._recover_codex_app_jobs()

    saved_job = slackbot_module._read_codex_app_jobs(
        original_bot.config.paths
    )["jobs"][original.route_key]
    assert saved_job["status"] == "active"
    first_restart._close_codex_app_controllers()

    second_restart = SlackSocketBot(original_bot.config)
    monkeypatch.setattr(
        second_restart,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(
        second_restart,
        "_audit_codex_app",
        lambda *args, **kwargs: None,
    )
    second_restart._recover_codex_app_jobs()

    recovered = second_restart.codex_app_controllers[original.route_key]
    assert recovered.active_turn_id == "turn-one"
    assert recovered.terminal is False


def test_startup_recovery_serializes_with_live_slack_input(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    text = "recover this input"
    client_message_id = "client-recovery"
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                controller.route_key: {
                    "status": "preparing",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "client_message_id": client_message_id,
                    "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                    "preparing_action": "start",
                    "preparing_text": text,
                }
            }
        },
    )
    controller_lookups = []

    def get_controller(*args, **kwargs):
        controller_lookups.append((args, kwargs))
        return controller

    monkeypatch.setattr(bot, "_get_codex_app_controller", get_controller)
    recovery_started = threading.Event()
    release_recovery = threading.Event()

    def recover_start_turn(recovered_text, recovered_client_id):
        assert (recovered_text, recovered_client_id) == (
            text,
            client_message_id,
        )
        recovery_started.set()
        assert release_recovery.wait(timeout=2)
        with controller.lock:
            controller.active_turn_id = "turn-recovered"
            controller.terminal = False
        return "turn-recovered"

    monkeypatch.setattr(
        controller,
        "recover_start_turn",
        recover_start_turn,
    )
    route_lock_attempts = 0
    route_lock_guard = threading.Lock()
    second_route_lock = threading.Event()
    original_conversation_lock = bot._conversation_lock

    @contextmanager
    def observed_conversation_lock(key):
        nonlocal route_lock_attempts
        with route_lock_guard:
            route_lock_attempts += 1
            if route_lock_attempts == 2:
                second_route_lock.set()
        with original_conversation_lock(key):
            yield

    monkeypatch.setattr(
        bot,
        "_conversation_lock",
        observed_conversation_lock,
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    recovery_thread = threading.Thread(target=bot._recover_codex_app_jobs)
    slack_thread = threading.Thread(
        target=bot._maybe_handle_codex_app,
        args=("second input", route, "U111"),
        kwargs={"event_id": "event-two", "binding": binding},
    )
    recovery_thread.start()
    assert recovery_started.wait(timeout=2)
    slack_thread.start()
    try:
        assert second_route_lock.wait(timeout=2)
        assert len(controller_lookups) == 1
    finally:
        release_recovery.set()
    recovery_thread.join(timeout=2)
    slack_thread.join(timeout=2)

    assert not recovery_thread.is_alive()
    assert not slack_thread.is_alive()
    assert len(controller_lookups) == 2


@pytest.mark.parametrize(
    ("preparing_action", "recovery_method"),
    (
        ("start", "recover_start_turn"),
        ("steer", "recover_steer"),
    ),
)
def test_preparing_input_is_replayed_with_same_client_id(
    tmp_path,
    monkeypatch,
    preparing_action,
    recovery_method,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    text = "continue the exact task"
    client_message_id = "client-one"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                controller.route_key: {
                    "status": "preparing",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "client_message_id": client_message_id,
                    "text_sha256": text_hash,
                    "preparing_action": preparing_action,
                    "preparing_text": text,
                    "preparing_turn_id": (
                        "turn-original"
                        if preparing_action == "steer"
                        else None
                    ),
                }
            }
        },
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    replayed = []

    def fake_recover(recovered_text, recovered_client_id, **kwargs):
        if preparing_action == "steer":
            assert kwargs == {"expected_turn_id": "turn-original"}
        else:
            assert kwargs == {}
        replayed.append((recovered_text, recovered_client_id))
        with controller.lock:
            controller.active_turn_id = "turn-recovered"
            controller.terminal = False
        return "turn-recovered"

    monkeypatch.setattr(controller, recovery_method, fake_recover)
    monkeypatch.setattr(controller, "drain_callbacks", lambda: None)
    records = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered_controller, **fields: records.append(fields),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    assert replayed == [(text, client_message_id)]
    assert records[0]["status"] == "preparing"
    assert records[0]["preparing_text"] == text
    assert records[-1]["status"] == "active"
    assert records[-1].get("preparing_text") is None


def test_preparing_steer_recovery_refuses_a_new_active_turn(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    connection.active_turn_id = "turn-new"
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )
    text = "apply this to the original turn"
    job = {
        "status": "preparing",
        "binding": binding,
        "active_turn_id": "turn-original",
        "client_message_id": "client-one",
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "preparing_action": "steer",
        "preparing_text": text,
        "preparing_turn_id": "turn-original",
    }

    assert bot._recover_codex_app_job(
        controller.route_key,
        job,
        route,
        binding,
    )

    assert not any(
        method == "turn/steer" for method, _ in connection.requests
    )
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "active"
    assert saved_job["active_turn_id"] == "turn-new"
    assert posts == [
        "Recovered Slack input was not sent because the Codex session state "
        "changed. Resend it if it is still needed."
    ]


def test_preparing_connection_failure_schedules_durable_retry(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                route_key: {
                    "status": "preparing",
                    "updated_at": "2026-07-21T00:00:00Z",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "client_message_id": "client-one",
                    "text_sha256": hashlib.sha256(b"continue").hexdigest(),
                    "preparing_action": "start",
                    "preparing_text": "continue",
                }
            }
        },
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppServerError("app server unavailable")
        ),
    )

    bot._recover_codex_app_jobs()

    controller = bot.codex_app_controllers[route_key]
    assert controller.connection is None
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_retry_at > time.time()


def test_preparing_validation_failure_discards_invalid_controller(
    tmp_path,
    monkeypatch,
):
    class InvalidSessionConnection(FakeControllerConnection):
        instances = []

        def request(self, method, params):
            result = super().request(method, params)
            if method == "thread/read":
                result["thread"]["id"] = "different-session"
                result["thread"]["sessionId"] = "different-session"
            return result

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    job = {
        "status": "preparing",
        "updated_at": "2026-07-21T00:00:00Z",
        "route": {
            "channel_id": route.channel_id,
            "thread_ts": route.thread_ts,
        },
        "binding": binding,
        "client_message_id": "client-one",
        "text_sha256": hashlib.sha256(b"continue").hexdigest(),
        "preparing_action": "start",
        "preparing_text": "continue",
    }
    slackbot_module._update_codex_app_job(bot.config, route_key, job)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        InvalidSessionConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    assert not bot._recover_codex_app_job(
        route_key,
        job,
        route,
        binding,
    )

    failed_connection = InvalidSessionConnection.instances[-1]
    failed_controller = failed_connection.notification_handler.__self__
    assert failed_controller.invalidated_snapshot() is True
    assert route_key not in bot.codex_app_controllers
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][route_key]
    assert saved_job["status"] == "recovery-rejected"


def test_deferred_validation_failure_does_not_reinsert_invalid_controller(
    tmp_path,
    monkeypatch,
):
    class InvalidSessionConnection(FakeControllerConnection):
        instances = []

        def request(self, method, params):
            result = super().request(method, params)
            if method == "thread/read":
                result["thread"]["id"] = "different-session"
                result["thread"]["sessionId"] = "different-session"
            return result

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.defer_recovery_retry()
    bot.codex_app_controllers[controller.route_key] = controller
    job = {
        "status": "preparing",
        "updated_at": "2026-07-21T00:00:00Z",
        "route": {
            "channel_id": route.channel_id,
            "thread_ts": route.thread_ts,
        },
        "binding": binding,
        "client_message_id": "client-one",
        "text_sha256": hashlib.sha256(b"continue").hexdigest(),
        "preparing_action": "start",
        "preparing_text": "continue",
    }
    slackbot_module._update_codex_app_job(
        bot.config,
        controller.route_key,
        job,
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        InvalidSessionConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    bot._retry_codex_app_recovery(controller)

    assert controller.invalidated_snapshot() is True
    assert controller.route_key not in bot.codex_app_controllers
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "recovery-rejected"
    assert InvalidSessionConnection.instances[-1].closed is True


def test_connect_request_recovery_hands_off_after_persisted_retry_ceiling(
    tmp_path,
    monkeypatch,
):
    class BusyConnection(FakeControllerConnection):
        instances = []

        def request(self, method, params):
            del params
            if method == "thread/read":
                raise CodexAppServerRequestError(
                    method,
                    {"code": -32000, "message": "server busy"},
                )
            return super().request(method, {})

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    job = {
        "status": "preparing",
        "updated_at": "2026-07-21T00:00:00Z",
        "route": {
            "channel_id": route.channel_id,
            "thread_ts": route.thread_ts,
        },
        "binding": binding,
        "client_message_id": "client-one",
        "text_sha256": hashlib.sha256(b"continue").hexdigest(),
        "preparing_action": "start",
        "preparing_text": "continue",
        "recovery_retry_at": time.time(),
        "recovery_retry_delay": (
            slackbot_module.CODEX_APP_RECOVERY_RETRY_MAX_SECONDS
        ),
    }
    slackbot_module._update_codex_app_job(bot.config, route_key, job)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        BusyConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    assert bot._recover_codex_app_job(
        route_key,
        job,
        route,
        binding,
    )

    handoff = bot.codex_app_controllers[route_key]
    assert handoff.desktop_required_snapshot() is True
    assert handoff.recovery_blocked_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][route_key]
    assert saved_job["status"] == "desktop-required"
    assert saved_job["client_message_id"] == "client-one"
    assert saved_job["preparing_action"] == "start"
    assert saved_job["preparing_text"] == "continue"

    class ActiveConnection(FakeControllerConnection):
        instances = []

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"

    restarted = SlackSocketBot(bot.config)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ActiveConnection,
    )
    monkeypatch.setattr(
        restarted,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(
        restarted,
        "_audit_codex_app",
        lambda *args, **kwargs: None,
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )

    assert restarted._maybe_handle_codex_app(
        "do not dispatch this yet",
        route,
        "U111",
        event_id="event-blocked",
        binding=binding,
    )

    recovered = restarted.codex_app_controllers[route_key]
    connection = ActiveConnection.instances[-1]
    assert recovered.desktop_required_snapshot() is True
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )
    assert "must be resolved in Desktop" in posts[-1]
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][route_key]
    assert saved_job["status"] == "desktop-required"
    assert saved_job["preparing_text"] == "continue"

    connection.active_turn_id = None
    restarted._handle_codex_app_notification(
        recovered,
        {
            "method": "turn/completed",
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": "turn-one"},
            },
        },
    )
    assert recovered.desktop_required_snapshot() is False

    assert restarted._maybe_handle_codex_app(
        "dispatch after Desktop resolution",
        route,
        "U111",
        event_id="event-resolved",
        binding=binding,
    )
    assert (
        sum(method == "turn/start" for method, _ in connection.requests) == 1
    )


def test_permanent_connect_request_error_rejects_empty_handoff(
    tmp_path,
    monkeypatch,
):
    class UnsupportedConnection(FakeControllerConnection):
        instances = []

        def request(self, method, params):
            if method == "thread/read":
                raise CodexAppServerRequestError(
                    method,
                    {"code": -32601, "message": "Method not found"},
                )
            return super().request(method, params)

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        UnsupportedConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    with pytest.raises(CodexAppServerRequestError, match="Method not found"):
        bot._get_codex_app_controller(route, binding)

    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    assert route_key not in bot.codex_app_controllers
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][route_key]
    assert saved_job["status"] == "recovery-rejected"
    failed_connection = UnsupportedConnection.instances[-1]
    failed_controller = failed_connection.notification_handler.__self__
    assert failed_controller.invalidated_snapshot() is True
    assert failed_connection.closed is True


def test_failed_new_controller_invalidates_before_late_callback(
    tmp_path,
    monkeypatch,
):
    callback_waiting = threading.Event()
    release_callback = threading.Event()

    class LateCallbackFailureConnection:
        instances = []

        def __init__(self, *args, **kwargs):
            del args
            self.notification_handler = kwargs["notification_handler"]
            self.server_request_handler = kwargs["server_request_handler"]
            self.disconnect_handler = kwargs["disconnect_handler"]
            self.connected = False
            self.callback_thread = None
            type(self).instances.append(self)

        def connect(self):
            self.connected = True
            return self

        def request(self, method, params):
            del params
            assert method == "thread/read"

            def late_callback():
                callback_waiting.set()
                assert release_callback.wait(timeout=2)
                self.server_request_handler(
                    {
                        "id": 42,
                        "method": ("item/commandExecution/requestApproval"),
                        "params": {
                            "threadId": "desktop-session",
                            "turnId": "turn-one",
                            "command": "git status",
                            "cwd": "/workspace/exact",
                        },
                    }
                )

            self.callback_thread = threading.Thread(target=late_callback)
            self.callback_thread.start()
            assert callback_waiting.wait(timeout=2)
            raise CodexAppServerError("setup failed")

        def close(self):
            self.connected = False

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        LateCallbackFailureConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: posts.append((args, kwargs)),
    )

    with pytest.raises(CodexAppServerError, match="setup failed"):
        bot._get_codex_app_controller(route, binding)

    failed_connection = LateCallbackFailureConnection.instances[-1]
    failed_controller = failed_connection.notification_handler.__self__
    assert failed_controller.invalidated_snapshot() is True
    assert failed_controller.route_key not in bot.codex_app_controllers
    replacement = CodexAppController(bot, route, binding)
    bot.codex_app_controllers[replacement.route_key] = replacement
    release_callback.set()
    failed_connection.callback_thread.join(timeout=2)

    assert not failed_connection.callback_thread.is_alive()
    assert bot.codex_app_controllers[replacement.route_key] is replacement
    assert posts == []
    assert (
        slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"] == {}
    )


def test_lazy_controller_fails_closed_when_recovery_state_is_unreadable(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: (_ for _ in ()).throw(OSError("state unavailable")),
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: (_ for _ in ()).throw(
            AssertionError("must not connect without recovery state")
        ),
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, message: posts.append(message),
    )

    assert bot._maybe_handle_codex_app(
        "new input",
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    assert bot.codex_app_controllers == {}
    assert len(posts) == 1
    assert "recovery state" in posts[0]


def test_lazy_preparing_controller_schedules_recovery_retry(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    route_key = slackbot_module.agent_tmux_thread_key(
        route.channel_id,
        route.thread_ts,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                route_key: {
                    "status": "preparing",
                    "route": {
                        "channel_id": route.channel_id,
                        "thread_ts": route.thread_ts,
                    },
                    "binding": binding,
                    "client_message_id": "client-one",
                    "text_sha256": hashlib.sha256(b"continue").hexdigest(),
                    "preparing_action": "start",
                    "preparing_text": "continue",
                }
            }
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    controller = bot._get_codex_app_controller(route, binding)

    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_retry_at > time.time()


def test_deferred_preparing_recovery_blocks_and_preserves_input(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    text = "ambiguous input"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-ambiguous",
        text_hash=text_hash,
        preparing_action="start",
        preparing_text=text,
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        "recover_start_turn",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppServerRequestError("history unavailable")
        ),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, message: posts.append(message),
    )

    bot._recover_codex_app_jobs()

    assert controller.recovery_blocked_snapshot() is True
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["preparing_action"] == "start"
    assert saved_job["preparing_text"] == text
    assert saved_job["text_sha256"] == text_hash

    assert bot._maybe_handle_codex_app(
        "different input",
        route,
        "U111",
        event_id="event-two",
        binding=binding,
    )
    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 101,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )
    bot._handle_codex_app_notification(
        controller,
        {
            "method": "turn/completed",
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": "turn-one"},
            },
        },
    )
    assert controller.recovery_blocked_snapshot() is True
    bot._handle_codex_app_disconnect(controller, RuntimeError("lost"))
    bot.codex_app_controllers[controller.route_key] = controller
    bot._tick_codex_app_controllers()

    connection = FakeControllerConnection.instances[-1]
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )
    assert controller.route_key in bot.codex_app_controllers
    preserved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert preserved_job["status"] == "preparing"
    assert preserved_job["preparing_text"] == text
    assert any("ambiguous Slack input" in message for message in posts)
    assert any(
        "preserved the input recovery record" in message for message in posts
    )


def test_deferred_preparing_recovery_retries_from_durable_input(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    text = "transiently ambiguous input"
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-transient",
        text_hash=hashlib.sha256(text.encode()).hexdigest(),
        preparing_action="start",
        preparing_text=text,
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    attempts = 0

    def recover_start_turn(recovered_text, recovered_client_id):
        nonlocal attempts
        attempts += 1
        assert (recovered_text, recovered_client_id) == (
            text,
            "client-transient",
        )
        if attempts == 1:
            raise CodexAppServerError("history temporarily unavailable")
        with controller.lock:
            controller.active_turn_id = "turn-recovered"
            controller.terminal = False
        return "turn-recovered"

    monkeypatch.setattr(controller, "recover_start_turn", recover_start_turn)
    monkeypatch.setattr(controller, "drain_callbacks", lambda: None)
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._recover_codex_app_jobs()

    assert attempts == 1
    assert controller.recovery_blocked_snapshot() is True
    with controller.lock:
        controller.recovery_retry_at = time.time() - 1
    bot.codex_app_controllers[controller.route_key] = controller

    bot._tick_codex_app_controllers()

    assert attempts == 2
    assert controller.recovery_blocked_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "active"
    assert saved_job["preparing_text"] is None


@pytest.mark.parametrize("handoff_first", (True, False))
def test_recovery_handoff_transition_is_atomic(
    tmp_path,
    handoff_first,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.mark_recovery_blocked()
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )

    if handoff_first:
        controller.recover_pending_request(
            {
                "request_id": 42,
                "method": "item/commandExecution/requestApproval",
                "turn_id": "turn-one",
                "expires_at": time.time() + 60,
            }
        )
        assert bot._persist_codex_app_recovery_handoff(controller, 42) is True
        assert controller.finalize_recovery() is True
        assert controller.desktop_required_snapshot() is True
        assert controller.pending_snapshot() is None
        assert controller.pending_replay_snapshot() is None
        job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
            controller.route_key
        ]
        assert job["recovery_handoff_required"] is True
        assert job["client_message_id"] == "client-one"
        assert job["text_sha256"] == hashlib.sha256(b"continue").hexdigest()
        assert job["preparing_text"] == "continue"
    else:
        assert controller.finalize_recovery() is False
        assert (
            bot._persist_codex_app_recovery_handoff(controller, 42) is False
        )
        assert controller.desktop_required_snapshot() is False

    assert controller.recovery_blocked_snapshot() is False
    assert controller.recovery_handoff_required_snapshot() is False


def test_distinct_recovery_handoffs_finalize_sticky_desktop_state(
    tmp_path,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.mark_recovery_blocked()
    controller.mark_recovery_handoff_required(41, "first-request")
    controller.mark_recovery_handoff_required(42, "second-request")

    assert controller.recovery_handoff_required_snapshot() is True
    assert controller.recovery_handoff_request_id_snapshot() is None
    assert controller.recovery_handoff_request_fingerprint_snapshot() is None
    assert controller.finalize_recovery() is True
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == []
    assert controller.clear_desktop_required_for_request(41) is False
    assert controller.clear_desktop_required_for_request(42) is False


def test_legacy_recovery_handoff_accepts_same_request_fingerprint(
    tmp_path,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.mark_recovery_handoff_required(42)
    controller.mark_recovery_handoff_required(42, "request-fingerprint")

    assert controller.recovery_handoff_required_snapshot() is True
    assert controller.recovery_handoff_request_id_snapshot() == 42
    assert (
        controller.recovery_handoff_request_fingerprint_snapshot()
        == "request-fingerprint"
    )


def test_restart_restores_durable_recovery_handoff(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    request_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    request_fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        request_params,
    )
    original = CodexAppController(bot, route, binding)
    original.mark_recovery_blocked()
    bot._record_codex_app_controller(
        original,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    assert (
        bot._persist_codex_app_recovery_handoff(
            original,
            42,
            request_fingerprint,
        )
        is True
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    bot.codex_app_controllers.clear()

    recovered = bot._get_codex_app_controller(
        route,
        binding,
        record_connected=False,
    )

    assert recovered is not original
    assert recovered.recovery_blocked_snapshot() is True
    assert recovered.recovery_handoff_required_snapshot() is True
    assert (
        recovered.recovery_handoff_request_fingerprint_snapshot()
        == request_fingerprint
    )
    assert recovered.finalize_recovery() is True
    assert recovered.desktop_required_snapshot() is True
    assert recovered.desktop_request_id_snapshot() == 42
    assert (
        recovered.desktop_request_fingerprint_snapshot()
        == request_fingerprint
    )
    bot._handle_codex_app_server_request(
        recovered,
        {
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": request_params,
        },
    )
    assert recovered.desktop_request_ids_snapshot() == [99]
    bot._handle_codex_app_notification(
        recovered,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 99,
            },
        },
    )
    assert recovered.desktop_required_snapshot() is False


def test_reconnect_preserves_reissued_desktop_request_id(
    tmp_path,
    monkeypatch,
):
    request_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }

    class ReissuingDesktopConnection(FakeControllerConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"
            self.reissued = False

        def request(self, method, params):
            result = super().request(method, params)
            if method == "thread/resume" and not self.reissued:
                self.reissued = True
                self.server_request_handler(
                    {
                        "id": 99,
                        "method": "item/commandExecution/requestApproval",
                        "params": request_params,
                    }
                )
            return result

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original = CodexAppController(bot, route, binding)
    request_fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        request_params,
    )
    original.mark_desktop_required(42, request_fingerprint)
    bot._record_codex_app_controller(
        original,
        status="desktop-required",
    )
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        original.route_key
    ]
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ReissuingDesktopConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._recover_codex_app_job(
        original.route_key,
        job,
        route,
        binding,
    )

    recovered = bot.codex_app_controllers[original.route_key]
    assert recovered is not original
    assert recovered.desktop_required_snapshot() is True
    assert recovered.desktop_request_id_snapshot() == 99
    assert recovered.desktop_request_ids_snapshot() == [99]
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][original.route_key]
    assert saved_job["status"] == "desktop-required"
    assert saved_job["desktop_request_id"] == 99
    assert saved_job["desktop_request_ids"] == [99]
    assert saved_job["desktop_request_fingerprint"] == request_fingerprint


def test_reconnect_does_not_restore_resolved_recovery_handoff(
    tmp_path,
    monkeypatch,
):
    class ResolvingHandoffConnection(FakeControllerConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"
            self.turn_history = [
                {
                    "id": "turn-one",
                    "status": "inProgress",
                    "items": [
                        {
                            "type": "userMessage",
                            "clientId": "client-one",
                        }
                    ],
                }
            ]
            self.resolved = False

        def request(self, method, params):
            result = super().request(method, params)
            if method == "thread/resume" and not self.resolved:
                self.resolved = True
                self.notification_handler(
                    {
                        "method": "serverRequest/resolved",
                        "params": {
                            "threadId": "desktop-session",
                            "requestId": 42,
                        },
                    }
                )
            return result

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original = CodexAppController(bot, route, binding)
    original.mark_recovery_blocked()
    bot._record_codex_app_controller(
        original,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    assert bot._persist_codex_app_recovery_handoff(original, 42) is True
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        original.route_key
    ]
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ResolvingHandoffConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._recover_codex_app_job(
        original.route_key,
        job,
        route,
        binding,
    )

    recovered = bot.codex_app_controllers[original.route_key]
    assert recovered is not original
    assert recovered.recovery_blocked_snapshot() is False
    assert recovered.recovery_handoff_required_snapshot() is False
    assert recovered.desktop_required_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][original.route_key]
    assert saved_job["status"] == "active"
    assert saved_job["recovery_handoff_required"] is False
    assert all(
        method != "turn/start" for method, _ in recovered.connection.requests
    )


def test_resolved_recovery_handoff_cannot_reassert_desktop(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.mark_recovery_blocked()
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    assert bot._persist_codex_app_recovery_handoff(controller, 42) is True
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )

    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_handoff_required_snapshot() is False
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        controller.route_key
    ]
    assert job["status"] == "preparing"
    assert job["recovery_handoff_required"] is False
    assert job["preparing_text"] == "continue"
    assert controller.finalize_recovery() is False
    assert controller.desktop_required_snapshot() is False


def test_resolved_replay_preserves_blocked_preparing_input(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 60,
            "request_fingerprint": "persisted-request",
        }
    )
    controller.mark_recovery_blocked()
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 42,
            },
        },
    )

    assert controller.pending_replay_snapshot() is None
    assert controller.recovery_blocked_snapshot() is True
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        controller.route_key
    ]
    assert job["status"] == "preparing"
    assert job["preparing_action"] == "start"
    assert job["preparing_text"] == "continue"


def test_deferred_reconnect_preserves_exponential_backoff(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.mark_recovery_blocked()
    controller.defer_recovery_retry(now=100)
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    monkeypatch.setattr(
        bot,
        "_recover_codex_app_job",
        lambda *args, **kwargs: False,
    )

    bot._retry_codex_app_recovery(controller)
    assert controller.recovery_retry_delay == 10
    assert bot.codex_app_controllers[controller.route_key] is controller

    bot._retry_codex_app_recovery(controller)
    assert controller.recovery_retry_delay == 20
    assert bot.codex_app_controllers[controller.route_key] is controller


def test_stale_recovery_retry_cannot_overwrite_rebound_job(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    stale = CodexAppController(
        bot,
        route,
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/old",
        },
    )
    stale.defer_recovery_retry()
    bot.codex_app_controllers[stale.route_key] = stale
    rebound = CodexAppController(
        bot,
        route,
        {
            "kind": "codex_app_server",
            "thread_id": "new-session",
            "source": "vscode",
            "cwd": "/workspace/new",
        },
    )
    bot._record_codex_app_controller(rebound, status="active")
    audits = []
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append((action, fields)),
    )

    bot._retry_codex_app_recovery(stale)

    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][stale.route_key]
    assert saved_job["status"] == "active"
    assert saved_job["binding"] == rebound.binding
    assert stale.route_key not in bot.codex_app_controllers
    assert audits == [
        (
            "preparing_recovery_discarded",
            {"reason": "durable-retry-state-mismatch"},
        )
    ]


def test_failed_recovery_reconnect_preserves_exponential_backoff(
    tmp_path,
    monkeypatch,
):
    class FailingConnection(FakeControllerConnection):
        def connect(self):
            raise CodexAppServerError("app-server unavailable")

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.defer_recovery_retry()
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FailingConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    bot._retry_codex_app_recovery(controller)

    replacement = bot.codex_app_controllers[controller.route_key]
    assert replacement is not controller
    assert replacement.recovery_retry_snapshot()[1] == 10

    bot._retry_codex_app_recovery(replacement)

    second_replacement = bot.codex_app_controllers[controller.route_key]
    assert second_replacement is not replacement
    assert second_replacement.recovery_retry_snapshot()[1] == 20


def test_failed_daemon_probe_preserves_exponential_backoff(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.defer_recovery_retry()
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: (_ for _ in ()).throw(
            CodexAppServerError("daemon unavailable")
        ),
    )

    bot._retry_codex_app_recovery(controller)

    replacement = bot.codex_app_controllers[controller.route_key]
    assert replacement is not controller
    assert replacement.recovery_retry_snapshot()[1] == 10

    bot._retry_codex_app_recovery(replacement)

    second_replacement = bot.codex_app_controllers[controller.route_key]
    assert second_replacement is not replacement
    assert second_replacement.recovery_retry_snapshot()[1] == 20


def test_replacement_controller_inherits_recovery_backoff(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    original = CodexAppController(bot, route, binding)
    original.mark_recovery_blocked()
    original.defer_recovery_retry()
    original.defer_recovery_retry()
    original.defer_recovery_retry()
    retry_at, retry_delay = original.recovery_retry_snapshot()
    bot.codex_app_controllers[original.route_key] = original
    bot._record_codex_app_controller(
        original,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    replacement = bot._get_codex_app_controller(
        route,
        binding,
        record_connected=False,
    )

    assert replacement is not original
    assert replacement.recovery_blocked_snapshot() is True
    assert replacement.recovery_retry_snapshot() == (retry_at, retry_delay)


def test_controller_state_is_not_copied_across_different_binding(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    existing = CodexAppController(bot, route, dict(binding))
    existing.connect()
    existing.binding["cwd"] = "/workspace/old"
    existing.mark_desktop_required(42)
    bot.codex_app_controllers[existing.route_key] = existing
    bot._record_codex_app_controller(
        existing,
        status="desktop-required",
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )

    replacement = bot._get_codex_app_controller(
        route,
        binding,
        record_connected=False,
    )

    assert replacement is not existing
    assert existing.connection is None
    assert replacement.desktop_required_snapshot() is False
    assert replacement.recovery_blocked_snapshot() is False


def test_request_during_preparing_recovery_requires_desktop_after_success(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    controller.mark_recovery_blocked()
    text = "recover this input"
    client_message_id = "client-one"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    job = {
        "status": "preparing",
        "binding": binding,
        "client_message_id": client_message_id,
        "text_sha256": text_hash,
        "preparing_action": "start",
        "preparing_text": text,
    }
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )

    def recover_start_turn(recovered_text, recovered_client_id):
        assert (recovered_text, recovered_client_id) == (
            text,
            client_message_id,
        )
        bot._handle_codex_app_server_request(
            controller,
            {
                "id": 102,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "desktop-session",
                    "turnId": "turn-one",
                    "command": "git status",
                    "cwd": "/workspace/exact",
                },
            },
        )
        with controller.lock:
            controller.active_turn_id = "turn-one"
            controller.terminal = False
        return "turn-one"

    monkeypatch.setattr(
        controller,
        "recover_start_turn",
        recover_start_turn,
    )
    records = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, **fields: records.append(fields),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._recover_codex_app_job(
        controller.route_key,
        job,
        route,
        binding,
    )

    assert controller.recovery_blocked_snapshot() is False
    assert controller.recovery_handoff_required_snapshot() is False
    assert controller.desktop_required_snapshot() is True
    assert records[0]["status"] == "preparing"
    assert records[-1]["status"] == "desktop-required"
    assert not any(record["status"] == "active" for record in records)


@pytest.mark.parametrize(
    (
        "message_text",
        "dispatch_method",
        "preparing_text",
        "expected_status",
    ),
    (
        (
            "continue the task",
            "start_turn",
            "continue the task",
            "connected",
        ),
        ("steer add this", "steer", "add this", "active"),
    ),
)
def test_pre_dispatch_failure_clears_replayable_input(
    tmp_path,
    monkeypatch,
    message_text,
    dispatch_method,
    preparing_text,
    expected_status,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    if dispatch_method == "steer":
        controller.active_turn_id = "turn-one"
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        dispatch_method,
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppPreDispatchError("not sent")
        ),
    )
    monkeypatch.setattr(controller, "drain_callbacks", lambda: None)
    records = []
    original_record = bot._record_codex_app_controller

    def capture_record(recovered, **fields):
        records.append(fields)
        original_record(recovered, **fields)

    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        capture_record,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )

    assert bot._maybe_handle_codex_app(
        message_text,
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    assert records[0]["status"] == "preparing"
    assert records[0]["preparing_text"] == preparing_text
    assert records[-1]["status"] == expected_status
    assert records[-1].get("preparing_text") is None
    assert records[-1]["clear_client_identity"] is True
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["client_message_id"] is None
    assert saved_job["text_sha256"] is None
    assert saved_job["preparing_text"] is None
    assert "not sent" in posts[-1]


def test_failed_pre_dispatch_clear_defers_preparing_recovery(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        "start_turn",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppPreDispatchError("not sent")
        ),
    )
    monkeypatch.setattr(controller, "drain_callbacks", lambda: None)
    monkeypatch.setattr(
        bot,
        "_clear_codex_app_preparing",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("state unavailable")
        ),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._maybe_handle_codex_app(
        "recover this input",
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    assert controller.dispatch_preparing_snapshot() is False
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_retry_snapshot()[1] > 0
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["preparing_action"] == "start"
    assert saved_job["preparing_text"] == "recover this input"


def test_live_pre_dispatch_callback_drain_failure_defers_recovery(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        "start_turn",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppPreDispatchError("not sent")
        ),
    )
    monkeypatch.setattr(
        controller,
        "drain_callbacks",
        lambda: (_ for _ in ()).throw(
            CodexAppServerError("callbacks still queued")
        ),
    )
    audits = []
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append((action, fields)),
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._maybe_handle_codex_app(
        "recover this input",
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    assert controller.dispatch_preparing_snapshot() is False
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_retry_snapshot()[1] > 0
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["client_message_id"]
    assert saved_job["preparing_text"] == "recover this input"
    assert audits[-1] == (
        "dispatch_recovery_deferred",
        {
            "error_type": "CodexAppServerError",
            "reason": "callback-drain-failed",
        },
    )


def test_live_pre_dispatch_failure_finalizes_callback_handoff(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )

    def fail_after_callback(*args, **kwargs):
        del args, kwargs
        bot._handle_codex_app_server_request(
            controller,
            {
                "id": 42,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "desktop-session",
                    "turnId": "turn-one",
                    "command": "git status",
                    "cwd": "/workspace/exact",
                },
            },
        )
        raise CodexAppPreDispatchError("not sent")

    monkeypatch.setattr(controller, "start_turn", fail_after_callback)
    monkeypatch.setattr(controller, "drain_callbacks", lambda: None)
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._maybe_handle_codex_app(
        "recover this input",
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    assert controller.dispatch_preparing_snapshot() is False
    assert controller.recovery_blocked_snapshot() is False
    assert controller.recovery_handoff_required_snapshot() is False
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == [42]
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "desktop-required"
    assert saved_job["client_message_id"] is None
    assert saved_job["text_sha256"] is None


def test_pre_dispatch_callback_drain_failure_defers_recovery(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.mark_recovery_blocked()
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(
        controller,
        "recover_start_turn",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppPreDispatchError("not sent")
        ),
    )
    monkeypatch.setattr(
        controller,
        "drain_callbacks",
        lambda: (_ for _ in ()).throw(
            CodexAppServerError("callbacks still queued")
        ),
    )
    posts = []
    audits = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: posts.append(args[-1]),
    )
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append((action, fields)),
    )
    text = "recover this input"
    job = {
        "status": "preparing",
        "binding": binding,
        "client_message_id": "client-one",
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "preparing_action": "start",
        "preparing_text": text,
    }

    assert bot._recover_codex_app_job(
        controller.route_key,
        job,
        route,
        binding,
    )

    assert controller.recovery_blocked_snapshot() is True
    _, retry_delay = controller.recovery_retry_snapshot()
    assert retry_delay > 0
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["preparing_text"] == text
    assert posts == []
    assert audits == [
        (
            "preparing_recovery_deferred",
            {
                "error_type": "CodexAppServerError",
                "reason": "callback-drain-failed",
            },
        )
    ]


def test_disconnect_during_dispatch_preserves_preparing_recovery(
    tmp_path,
    monkeypatch,
):
    class DisconnectingStartConnection(FakeControllerConnection):
        def request(self, method, params):
            if method == "turn/start":
                self.server_request_handler(
                    {
                        "id": 102,
                        "method": "item/commandExecution/requestApproval",
                        "params": {
                            "threadId": "desktop-session",
                            "turnId": "turn-one",
                            "command": "git status",
                            "cwd": "/workspace/exact",
                        },
                    }
                )
                self.connected = False
                error = CodexAppServerError("connection lost during start")
                self.disconnect_handler(error)
                raise error
            return super().request(method, params)

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        DisconnectingStartConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )

    assert bot._maybe_handle_codex_app(
        "recover this input",
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    controller = bot.codex_app_controllers[
        slackbot_module.agent_tmux_thread_key(
            route.channel_id,
            route.thread_ts,
        )
    ]
    assert controller.disconnected is True
    assert controller.dispatch_preparing_snapshot() is False
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_handoff_required_snapshot() is True
    assert controller.recovery_handoff_request_id_snapshot() == 102
    assert controller.recovery_retry_snapshot()[1] > 0
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["preparing_action"] == "start"
    assert saved_job["preparing_text"] == "recover this input"
    assert saved_job["recovery_handoff_required"] is True
    assert "unavailable" in posts[-1]


def test_active_disconnect_reconnects_with_backoff(
    tmp_path,
    monkeypatch,
):
    class ActiveConnection(FakeControllerConnection):
        instances = []

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ActiveConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(controller, status="active")
    controller.connection.connected = False

    controller._handle_disconnect(RuntimeError("connection lost"))

    assert controller.disconnected is True
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_retry_snapshot()[1] > 0
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "disconnected"
    with controller.lock:
        controller.recovery_retry_at = time.time() - 1

    bot._tick_codex_app_controllers()

    recovered = bot.codex_app_controllers[controller.route_key]
    assert recovered is not controller
    assert recovered.disconnected is False
    assert recovered.active_turn_id == "turn-one"
    assert recovered.recovery_blocked_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "active"


def test_active_reconnect_failure_remains_disconnected_before_retry(
    tmp_path,
    monkeypatch,
):
    class FlakyActiveConnection(FakeControllerConnection):
        instances = []
        connect_attempts = 0

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"

        def connect(self):
            type(self).connect_attempts += 1
            if type(self).connect_attempts == 2:
                raise CodexAppServerError("app-server unavailable")
            return super().connect()

    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FlakyActiveConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(controller, status="active")
    controller.connection.connected = False
    controller._handle_disconnect(RuntimeError("connection lost"))
    with controller.lock:
        controller.recovery_retry_at = time.time() - 1

    bot._tick_codex_app_controllers()

    retry = bot.codex_app_controllers[controller.route_key]
    assert retry is not controller
    assert retry.recovery_input_blocked_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "disconnected"
    assert saved_job["preparing_action"] is None
    assert saved_job["preparing_text"] is None
    with retry.lock:
        retry.recovery_retry_at = time.time() - 1

    bot._tick_codex_app_controllers()

    recovered = bot.codex_app_controllers[controller.route_key]
    assert recovered is not retry
    assert recovered.active_turn_id == "turn-one"
    assert recovered.desktop_required_snapshot() is False
    assert recovered.recovery_blocked_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "active"


def test_disconnect_during_recovery_finalization_preserves_retry(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    connection = SimpleNamespace(connected=True)
    controller.connection = connection
    controller.thread_status = "active"
    controller.active_turn_id = "turn-one"
    controller.terminal = False
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    audits = []
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append((action, fields)),
    )

    class DisconnectingJob(dict):
        disconnected = False

        def get(self, key, default=None):
            value = super().get(key, default)
            if key == "status" and not self.disconnected:
                self.disconnected = True
                connection.connected = False
                controller._handle_disconnect(
                    RuntimeError("connection lost after reconnect")
                )
            return value

    job = DisconnectingJob(status="active", binding=binding)

    assert bot._recover_codex_app_job(
        controller.route_key,
        job,
        route,
        binding,
    )

    assert controller.disconnected is True
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_retry_snapshot()[1] > 0
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "disconnected"
    assert [action for action, _ in audits] == [
        "disconnect",
        "recovery_deferred",
    ]


def test_codex_app_slack_dispatches_are_serialized(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    connection = FakeControllerConnection.instances[-1]
    first_dispatch = threading.Event()
    release_first = threading.Event()
    original_request = connection.request

    def blocking_request(method, params):
        if method == "turn/start" and not first_dispatch.is_set():
            first_dispatch.set()
            assert release_first.wait(timeout=2)
        return original_request(method, params)

    connection.request = blocking_request
    controller_lookups = []

    def get_controller(*args, **kwargs):
        controller_lookups.append((args, kwargs))
        return controller

    monkeypatch.setattr(bot, "_get_codex_app_controller", get_controller)
    route_lock_attempts = 0
    route_lock_guard = threading.Lock()
    second_route_lock = threading.Event()
    original_conversation_lock = bot._conversation_lock

    @contextmanager
    def observed_conversation_lock(key):
        nonlocal route_lock_attempts
        with route_lock_guard:
            route_lock_attempts += 1
            if route_lock_attempts == 2:
                second_route_lock.set()
        with original_conversation_lock(key):
            yield

    monkeypatch.setattr(
        bot,
        "_conversation_lock",
        observed_conversation_lock,
    )
    second_preparing = threading.Event()
    original_record = bot._record_codex_app_controller

    def capture_record(recovered, **fields):
        if fields.get("preparing_text") == "second input":
            second_preparing.set()
        original_record(recovered, **fields)

    monkeypatch.setattr(bot, "_record_codex_app_controller", capture_record)
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    first = threading.Thread(
        target=bot._maybe_handle_codex_app,
        args=("first input", route, "U111"),
        kwargs={"event_id": "event-one", "binding": binding},
    )
    second = threading.Thread(
        target=bot._maybe_handle_codex_app,
        args=("second input", route, "U111"),
        kwargs={"event_id": "event-two", "binding": binding},
    )
    first.start()
    assert first_dispatch.wait(timeout=2)
    second.start()
    try:
        assert second_route_lock.wait(timeout=2)
        assert not second_preparing.is_set()
        assert len(controller_lookups) == 1
    finally:
        release_first.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert len(controller_lookups) == 2
    assert second_preparing.is_set()
    dispatches = [
        method
        for method, _ in connection.requests
        if method in {"turn/start", "turn/steer"}
    ]
    assert dispatches == ["turn/start"]


def test_cleanup_tick_does_not_evict_preparing_dispatch(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    controller.thread_status = "idle"
    controller.terminal = True
    bot.codex_app_controllers[controller.route_key] = controller
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    preparation_started = threading.Event()
    release_dispatch = threading.Event()

    def blocking_start(text, client_message_id):
        del text, client_message_id
        preparation_started.set()
        assert release_dispatch.wait(timeout=2)
        with controller.lock:
            controller.active_turn_id = "turn-one"
            controller.thread_status = "active"
            controller.terminal = False
        return "turn-one"

    monkeypatch.setattr(controller, "start_turn", blocking_start)
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    results = []
    dispatch = threading.Thread(
        target=lambda: results.append(
            bot._maybe_handle_codex_app(
                "continue the task",
                route,
                "U111",
                event_id="event-one",
                binding=binding,
            )
        )
    )
    dispatch.start()
    assert preparation_started.wait(timeout=2)
    try:
        assert controller.dispatch_preparing_snapshot() is True
        bot._tick_codex_app_controllers()
        assert bot.codex_app_controllers[controller.route_key] is controller
        assert controller.invalidated_snapshot() is False
    finally:
        release_dispatch.set()
    dispatch.join(timeout=2)

    assert not dispatch.is_alive()
    assert results == [True]
    assert controller.dispatch_preparing_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "active"


def test_cleanup_tick_prunes_completed_durable_job(tmp_path):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    with controller.lock:
        controller.thread_status = "idle"
        controller.terminal = True
    bot.codex_app_controllers[controller.route_key] = controller
    bot._record_codex_app_controller(controller, status="completed")

    bot._tick_codex_app_controllers()

    assert controller.route_key not in bot.codex_app_controllers
    jobs = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"]
    assert controller.route_key not in jobs


@pytest.mark.parametrize("message_text", ("steering this way", "steerage"))
def test_steer_prefix_words_start_normal_turns(
    tmp_path,
    monkeypatch,
    message_text,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    started = []

    def start_turn(text, client_message_id):
        started.append((text, client_message_id))
        with controller.lock:
            controller.active_turn_id = "turn-one"
            controller.terminal = False
        return "turn-one"

    monkeypatch.setattr(controller, "start_turn", start_turn)
    monkeypatch.setattr(
        controller,
        "steer",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("ordinary steer-prefixed text must not steer")
        ),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    assert bot._maybe_handle_codex_app(
        message_text,
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    assert started[0][0] == message_text


def test_dispatch_preparation_serializes_callback_and_persistence(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    record_started = threading.Event()
    release_record = threading.Event()
    callback_started = threading.Event()
    recorded_statuses = []
    original_record = bot._record_codex_app_controller

    def blocking_record(recovered, **fields):
        original_record(recovered, **fields)
        recorded_statuses.append(fields["status"])
        if len(recorded_statuses) == 1:
            record_started.set()
            assert release_record.wait(timeout=2)

    monkeypatch.setattr(bot, "_record_codex_app_controller", blocking_record)
    original_persist_handoff = bot._persist_codex_app_recovery_handoff

    def observed_persist_handoff(*args, **kwargs):
        callback_started.set()
        return original_persist_handoff(*args, **kwargs)

    monkeypatch.setattr(
        bot,
        "_persist_codex_app_recovery_handoff",
        observed_persist_handoff,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    text = "continue this task"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    preparation = threading.Thread(
        target=bot._begin_codex_app_dispatch_preparation,
        args=(controller,),
        kwargs={
            "client_message_id": "client-one",
            "text_hash": text_hash,
            "preparing_action": "start",
            "preparing_text": text,
        },
    )
    callback = threading.Thread(
        target=bot._handle_codex_app_server_request,
        args=(
            controller,
            {
                "id": 42,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "desktop-session",
                    "turnId": "turn-one",
                    "command": "git status",
                    "cwd": "/workspace/exact",
                },
            },
        ),
    )

    preparation.start()
    assert record_started.wait(timeout=2)
    callback.start()
    try:
        assert callback_started.wait(timeout=2)
        assert callback.is_alive()
        assert recorded_statuses == ["preparing"]
    finally:
        release_record.set()
    preparation.join(timeout=2)
    callback.join(timeout=2)

    assert not preparation.is_alive()
    assert not callback.is_alive()
    assert controller.dispatch_preparing_snapshot() is True
    assert controller.recovery_handoff_required_snapshot() is True
    assert controller.recovery_handoff_request_id_snapshot() == 42
    assert recorded_statuses == ["preparing", "preparing"]
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["client_message_id"] == "client-one"
    assert saved_job["text_sha256"] == text_hash
    assert saved_job["preparing_text"] == text
    assert saved_job["recovery_handoff_required"] is True


def test_dispatch_preparation_rolls_back_after_state_write_failure(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("state unavailable")
        ),
    )

    with pytest.raises(OSError, match="state unavailable"):
        bot._begin_codex_app_dispatch_preparation(
            controller,
            client_message_id="client-one",
            text_hash="text-hash",
            preparing_action="start",
            preparing_text="continue",
        )

    assert controller.dispatch_preparing_snapshot() is False


@pytest.mark.parametrize(
    "notification_method", ("turn/started", "turn/completed")
)
def test_dispatch_notification_preserves_durable_preparing_input(
    tmp_path,
    monkeypatch,
    notification_method,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    if notification_method == "turn/completed":
        with controller.lock:
            controller.active_turn_id = "turn-one"
            controller.thread_status = "active"
            controller.terminal = False
        preparing_action = "steer"
        preparing_text = "add this context"
    else:
        preparing_action = "start"
        preparing_text = "continue the task"
    text_hash = hashlib.sha256(preparing_text.encode()).hexdigest()
    bot._begin_codex_app_dispatch_preparation(
        controller,
        client_message_id="client-one",
        text_hash=text_hash,
        preparing_action=preparing_action,
        preparing_text=preparing_text,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_notification(
        controller,
        {
            "method": notification_method,
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": "turn-one"},
            },
        },
    )

    assert controller.dispatch_preparing_snapshot() is True
    if notification_method == "turn/started":
        assert controller.active_turn_id == "turn-one"
        assert controller.terminal is False
    else:
        assert controller.active_turn_id is None
        assert controller.terminal is True
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["client_message_id"] == "client-one"
    assert saved_job["text_sha256"] == text_hash
    assert saved_job["preparing_action"] == preparing_action
    assert saved_job["preparing_text"] == preparing_text


def test_dispatch_completion_rechecks_turn_after_completion_callback(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    with controller.lock:
        controller.active_turn_id = "turn-one"
        controller.thread_status = "active"
        controller.terminal = False
    bot._begin_codex_app_dispatch_preparation(
        controller,
        client_message_id="client-one",
        text_hash="text-hash",
        preparing_action="steer",
        preparing_text="additional context",
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "turn/completed",
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": "turn-one"},
            },
        },
    )
    status = bot._record_codex_app_dispatch_completion(
        controller,
        default_status="active",
        client_message_id="client-one",
        text_hash="text-hash",
    )

    assert status == "completed"
    assert controller.dispatch_preparing_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "completed"
    assert saved_job["active_turn_id"] is None


def test_dispatch_completion_serializes_recovery_handoff(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    with controller.lock:
        controller.active_turn_id = "turn-one"
        controller.thread_status = "active"
        controller.terminal = False
    controller.begin_dispatch_preparation()
    record_started = threading.Event()
    release_record = threading.Event()
    callback_started = threading.Event()
    recorded_statuses = []

    def blocking_record(recovered, **fields):
        recorded_statuses.append(fields["status"])
        if fields["status"] == "active":
            record_started.set()
            assert release_record.wait(timeout=2)

    monkeypatch.setattr(bot, "_record_codex_app_controller", blocking_record)
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    original_persist_handoff = bot._persist_codex_app_recovery_handoff

    def observed_persist_handoff(*args, **kwargs):
        callback_started.set()
        return original_persist_handoff(*args, **kwargs)

    monkeypatch.setattr(
        bot,
        "_persist_codex_app_recovery_handoff",
        observed_persist_handoff,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    completion = threading.Thread(
        target=bot._record_codex_app_dispatch_completion,
        args=(controller,),
        kwargs={
            "default_status": "active",
            "client_message_id": "client-one",
            "text_hash": "text-hash",
        },
    )
    callback = threading.Thread(
        target=bot._handle_codex_app_server_request,
        args=(
            controller,
            {
                "id": 42,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "desktop-session",
                    "turnId": "turn-one",
                    "command": "git status",
                    "cwd": "/workspace/exact",
                },
            },
        ),
    )

    completion.start()
    assert record_started.wait(timeout=2)
    callback.start()
    try:
        assert callback_started.wait(timeout=2)
        assert callback.is_alive()
        assert recorded_statuses == ["active"]
    finally:
        release_record.set()
    completion.join(timeout=2)
    callback.join(timeout=2)

    assert not completion.is_alive()
    assert not callback.is_alive()
    assert controller.dispatch_preparing_snapshot() is False
    assert controller.recovery_handoff_required_snapshot() is False
    assert controller.pending_snapshot()["request_id"] == 42
    assert recorded_statuses == [
        "active",
        "pending-request",
        "pending-request",
    ]


def test_replaced_controller_rejects_late_callback_side_effects(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    stale = CodexAppController(bot, route, binding)
    bot.codex_app_controllers[stale.route_key] = stale
    callback_paused = threading.Event()
    release_callback = threading.Event()
    original_set_pending = stale.set_pending_request

    def pause_before_pending(*args, **kwargs):
        callback_paused.set()
        assert release_callback.wait(timeout=3)
        return original_set_pending(*args, **kwargs)

    monkeypatch.setattr(stale, "set_pending_request", pause_before_pending)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        bot,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    callback = threading.Thread(
        target=bot._handle_codex_app_server_request,
        args=(
            stale,
            {
                "id": 42,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "desktop-session",
                    "turnId": "turn-one",
                    "command": "git status",
                    "cwd": "/workspace/exact",
                },
            },
        ),
    )
    callback.start()
    assert callback_paused.wait(timeout=2)

    replacement = bot._get_codex_app_controller(
        route,
        binding,
        record_connected=False,
    )
    assert replacement is not stale
    assert stale.invalidated_snapshot() is True
    with replacement.lock:
        replacement.active_turn_id = "turn-new"
        replacement.thread_status = "active"
        replacement.terminal = False
    bot._record_codex_app_controller(replacement, status="active")
    release_callback.set()
    callback.join(timeout=2)

    assert not callback.is_alive()
    assert posts == []
    assert bot.codex_app_controllers[stale.route_key] is replacement
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][stale.route_key]
    assert saved_job["status"] == "active"
    assert saved_job["active_turn_id"] == "turn-new"
    assert saved_job["pending_request"] is None


def test_ineligible_stale_controller_cannot_delete_replacement_job(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    stale_binding = {
        "kind": "codex_app_server",
        "thread_id": "stale-session",
        "source": "vscode",
        "cwd": "/workspace/stale",
    }
    replacement_binding = {
        "kind": "codex_app_server",
        "thread_id": "replacement-session",
        "source": "vscode",
        "cwd": "/workspace/replacement",
    }
    state_path = slackbot_module.agent_codex_hook_state_path(bot.config.paths)
    slackbot_module.write_json_atomic(
        state_path,
        {
            "version": 1,
            "sessions": {
                "session:stale-session": {
                    "key": "session:stale-session",
                    "session_id": "stale-session",
                    "channel_id": route.channel_id,
                    "thread_ts": route.thread_ts,
                    "notify_user_id": "U111",
                    "codex_app_server_revoked_at": ("2026-07-21T00:00:00Z"),
                    "codex_app_server_revoked_reason": (
                        "automated-review-session"
                    ),
                }
            },
        },
    )
    stale = CodexAppController(bot, route, stale_binding)
    replacement = CodexAppController(bot, route, replacement_binding)
    bot.codex_app_controllers[replacement.route_key] = replacement
    with replacement.lock:
        replacement.active_turn_id = "turn-new"
        replacement.thread_status = "active"
        replacement.terminal = False
    bot._record_codex_app_controller(replacement, status="active")
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_notification(
        stale,
        {
            "method": "turn/started",
            "params": {
                "threadId": "stale-session",
                "turn": {"id": "turn-stale"},
            },
        },
    )

    assert stale.invalidated_snapshot() is True
    assert bot.codex_app_controllers[replacement.route_key] is replacement
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][replacement.route_key]
    assert saved_job["binding"] == replacement_binding
    assert saved_job["status"] == "active"
    assert saved_job["active_turn_id"] == "turn-new"


@pytest.mark.parametrize("message_text", ("continue", "approve"))
def test_pending_replay_blocks_slack_input_until_async_reissue(
    tmp_path,
    monkeypatch,
    message_text,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 30,
            "request_fingerprint": "replay-fingerprint",
        }
    )
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )

    assert bot._maybe_handle_codex_app(
        message_text,
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    connection = FakeControllerConnection.instances[-1]
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )
    assert connection.responses == []
    assert controller.pending_replay_snapshot() is not None
    assert "restoring a pending approval or answer" in posts[-1]


def test_nonmatching_reissued_request_fails_closed_to_desktop(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    original_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 30,
            "request_fingerprint": (
                bot._codex_app_server_request_fingerprint(
                    "item/commandExecution/requestApproval",
                    original_params,
                )
            ),
        }
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": {**original_params, "command": "git diff"},
        },
    )

    assert controller.pending_snapshot() is None
    assert controller.pending_replay_snapshot() is None
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_id_snapshot() is None
    connection = FakeControllerConnection.instances[-1]
    assert connection.responses == []
    assert "different request" in posts[-1]
    job = slackbot_module._read_codex_app_jobs(bot.config.paths)["jobs"][
        controller.route_key
    ]
    assert job["status"] == "desktop-required"
    bot._handle_codex_app_notification(
        controller,
        {
            "method": "serverRequest/resolved",
            "params": {
                "threadId": "desktop-session",
                "requestId": 99,
            },
        },
    )
    assert controller.desktop_required_snapshot() is True
    bot._handle_codex_app_notification(
        controller,
        {
            "method": "turn/completed",
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": ""},
            },
        },
    )
    assert controller.desktop_required_snapshot() is False


def test_desktop_required_blocks_input_until_resolution(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(bot, route, binding)
    controller.connect()
    controller.mark_desktop_required()
    monkeypatch.setattr(
        bot,
        "_get_codex_app_controller",
        lambda *args, **kwargs: controller,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, message: posts.append(message),
    )

    assert bot._maybe_handle_codex_app(
        "blocked input",
        route,
        "U111",
        event_id="event-one",
        binding=binding,
    )

    connection = FakeControllerConnection.instances[-1]
    assert not any(
        method in {"turn/start", "turn/steer"}
        for method, _ in connection.requests
    )
    assert "must be resolved in Desktop" in posts[-1]

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "turn/completed",
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": "turn-desktop"},
            },
        },
    )
    assert controller.desktop_required_snapshot() is False

    assert bot._maybe_handle_codex_app(
        "unblocked input",
        route,
        "U111",
        event_id="event-two",
        binding=binding,
    )
    assert (
        sum(method == "turn/start" for method, _ in connection.requests) == 1
    )


def test_start_turn_marks_refresh_failure_as_pre_dispatch(tmp_path):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.refresh = lambda: (_ for _ in ()).throw(
        CodexAppServerError("refresh failed")
    )

    with pytest.raises(CodexAppPreDispatchError, match="refresh failed"):
        controller.start_turn("continue", "client-one")


def test_recovery_does_not_overwrite_freshly_reissued_request(tmp_path):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    fresh = {
        "request_id": 99,
        "method": "item/commandExecution/requestApproval",
        "turn_id": "turn-one",
        "expires_at": time.time() + 60,
    }
    persisted = {**fresh, "request_id": 42}
    assert controller.set_pending_request(fresh) is True

    status = controller.recover_pending_request(persisted)

    assert status == "pending-request"
    assert controller.pending_snapshot() == fresh
    assert controller.pending_request_replay is None


def test_stale_turn_completion_does_not_clear_newer_turn(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    with controller.lock:
        controller.active_turn_id = "turn-new"
        controller.thread_status = "active"
        controller.terminal = False
    controller.set_pending_request(
        {
            "request_id": 100,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-new",
            "expires_at": time.time() + 60,
        }
    )
    records = []
    audits = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: records.append(kwargs),
    )
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append((action, fields)),
    )

    bot._handle_codex_app_notification(
        controller,
        {
            "method": "turn/completed",
            "params": {
                "threadId": "desktop-session",
                "turn": {"id": "turn-old"},
            },
        },
    )

    assert controller.active_turn_id == "turn-new"
    assert controller.pending_snapshot()["request_id"] == 100
    assert controller.terminal is False
    assert records == []
    assert audits == [
        (
            "stale_turn_completion_ignored",
            {"turn_id": "turn-old", "active_turn_id": "turn-new"},
        )
    ]


def test_malformed_job_state_is_not_overwritten(tmp_path):
    bot = _bot(tmp_path, notification_only=False)
    state_path = Path(
        slackbot_module.agent_codex_app_jobs_path(bot.config.paths)
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{malformed", encoding="utf-8")

    with pytest.raises(SlackbotError, match="Cannot parse"):
        slackbot_module._update_codex_app_job(
            bot.config,
            "D111:1.000001",
            {"status": "connected"},
        )

    assert state_path.read_text(encoding="utf-8") == "{malformed"


def test_newer_job_state_version_is_not_overwritten(tmp_path):
    bot = _bot(tmp_path, notification_only=False)
    state_path = Path(
        slackbot_module.agent_codex_app_jobs_path(bot.config.paths)
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    original = '{"version": 2, "jobs": {}}\n'
    state_path.write_text(original, encoding="utf-8")

    with pytest.raises(SlackbotError, match="Unsupported.*version"):
        slackbot_module._update_codex_app_job(
            bot.config,
            "D111:1.000001",
            {"status": "connected"},
        )

    assert state_path.read_text(encoding="utf-8") == original


def test_only_safe_app_server_requests_are_relayed(tmp_path, monkeypatch):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, route, text: posts.append(text),
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 41,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )
    pending = controller.pending_snapshot()
    assert pending["request_id"] == 41
    assert "Codex requests command approval" in posts[-1]

    bot._respond_codex_app_approval(controller, "approve", pending)
    connection = FakeControllerConnection.instances[-1]
    assert connection.responses == [(41, {"decision": "accept"})]

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 42,
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "questions": [
                    {
                        "id": "token",
                        "header": "Token",
                        "question": "Paste the token",
                        "isSecret": True,
                    }
                ],
            },
        },
    )

    assert controller.pending_snapshot() is None
    assert len(connection.responses) == 1
    assert "Handle it in Desktop" in posts[-1]


def test_app_request_delivery_receipt_is_durable_and_stable(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    client_msg_ids = []

    def deliver(config, route, text):
        del config, route, text
        client_msg_ids.append(slackbot_module.SLACK_CLIENT_MSG_ID.get())
        slackbot_module._mark_slack_delivery_stage("posting")
        return {"ts": "9.000009"}

    monkeypatch.setattr(slackbot_module, "post_slack_message", deliver)
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 41,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )

    assert len(client_msg_ids) == 1
    assert str(uuid.UUID(client_msg_ids[0])) == client_msg_ids[0]
    pending = controller.pending_snapshot()
    assert pending["slack_client_msg_id"] == client_msg_ids[0]
    assert pending["slack_delivery_state"] == "delivered"
    assert pending["slack_message_ts"] == "9.000009"
    saved_pending = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]["pending_request"]
    assert saved_pending["slack_client_msg_id"] == client_msg_ids[0]
    assert saved_pending["slack_delivery_state"] == "delivered"
    assert saved_pending["slack_message_ts"] == "9.000009"


def test_delivered_reissued_request_does_not_duplicate_slack_prompt(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        params,
    )
    controller.recover_pending_request(
        {
            "request_id": 41,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 30,
            "request_fingerprint": fingerprint,
            "slack_client_msg_id": "request-client-id",
            "slack_delivery_state": "delivered",
            "slack_message_ts": "9.000009",
        }
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: posts.append(args[-1]),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": params,
        },
    )

    assert posts == []
    pending = controller.pending_snapshot()
    assert pending["request_id"] == 99
    assert pending["slack_client_msg_id"] == "request-client-id"
    assert pending["slack_delivery_state"] == "delivered"
    saved_pending = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]["pending_request"]
    assert saved_pending["request_id"] == 99
    assert saved_pending["slack_delivery_state"] == "delivered"


def test_ambiguous_reissued_request_fails_closed_without_duplicate_prompt(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        params,
    )
    controller.recover_pending_request(
        {
            "request_id": 41,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() + 30,
            "request_fingerprint": fingerprint,
            "slack_client_msg_id": "request-client-id",
            "slack_delivery_state": "posting",
        }
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, route, text: posts.append(text),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": params,
        },
    )

    assert controller.pending_snapshot() is None
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == [99]
    assert len(posts) == 1
    assert "delivery was ambiguous" in posts[0]
    assert "Codex requests command approval" not in posts[0]
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "desktop-required"


def test_second_pending_request_keeps_desktop_handoff_sticky(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, route, text: posts.append(text),
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    def request(request_id, command):
        bot._handle_codex_app_server_request(
            controller,
            {
                "id": request_id,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": "desktop-session",
                    "turnId": "turn-one",
                    "command": command,
                    "cwd": "/workspace/exact",
                },
            },
        )

    request(41, "git status")
    assert controller.pending_snapshot()["request_id"] == 41

    request(42, "git diff")

    assert controller.pending_snapshot() is None
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == []
    assert "another request" in posts[-1]
    for request_id in (41, 42):
        bot._handle_codex_app_notification(
            controller,
            {
                "method": "serverRequest/resolved",
                "params": {
                    "threadId": "desktop-session",
                    "requestId": request_id,
                },
            },
        )
    assert controller.desktop_required_snapshot() is True


def test_unsupported_request_behind_pending_keeps_handoff_sticky(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 41,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )
    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 42,
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "questions": [
                    {
                        "id": "token",
                        "header": "Token",
                        "question": "Paste the token",
                        "isSecret": True,
                    }
                ],
            },
        },
    )

    assert controller.pending_snapshot() is None
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == []
    for request_id in (41, 42):
        bot._handle_codex_app_notification(
            controller,
            {
                "method": "serverRequest/resolved",
                "params": {
                    "threadId": "desktop-session",
                    "requestId": request_id,
                },
            },
        )
    assert controller.desktop_required_snapshot() is True


def test_unsupported_request_handoff_survives_restart_and_slack_failure(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    binding = {
        "kind": "codex_app_server",
        "thread_id": "desktop-session",
        "source": "vscode",
        "cwd": "/workspace/exact",
    }
    controller = CodexAppController(bot, route, binding)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("Slack unavailable")
        ),
    )

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 42,
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "questions": [
                    {
                        "id": "token",
                        "header": "Token",
                        "question": "Paste the token",
                        "isSecret": True,
                    }
                ],
            },
        },
    )

    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "desktop-required"
    assert saved_job["desktop_request_ids"] == [42]

    class ActiveConnection(FakeControllerConnection):
        instances = []

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active_turn_id = "turn-one"

    restarted = SlackSocketBot(bot.config)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        ActiveConnection,
    )
    monkeypatch.setattr(
        restarted,
        "_ensure_codex_app_server",
        lambda: "/tmp/fake-app-server.sock",
    )
    monkeypatch.setattr(
        restarted,
        "_audit_codex_app",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )

    restarted._recover_codex_app_jobs()

    recovered = restarted.codex_app_controllers[controller.route_key]
    assert recovered.desktop_required_snapshot() is True
    assert recovered.desktop_request_ids_snapshot() == [42]
    assert recovered.pending_snapshot() is None


def test_connected_request_ignores_stale_persisted_recovery_deadline(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, route, text: posts.append(text),
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    stale_params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    current_params = dict(stale_params)
    stale_fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        stale_params,
    )
    monkeypatch.setattr(
        slackbot_module,
        "_read_codex_app_jobs",
        lambda paths: {
            "jobs": {
                controller.route_key: {
                    "status": "awaiting-request-replay",
                    "pending_request": {
                        "method": "item/commandExecution/requestApproval",
                        "turn_id": "turn-one",
                        "expires_at": time.time() - 1,
                        "request_fingerprint": stale_fingerprint,
                    },
                }
            }
        },
    )

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 44,
            "method": "item/commandExecution/requestApproval",
            "params": current_params,
        },
    )

    pending = controller.pending_snapshot()
    assert pending["request_id"] == 44
    assert pending["expires_at"] > time.time()
    assert "Codex requests command approval" in posts[-1]


def test_replayed_request_preserves_original_deadline(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    params = {
        "threadId": "desktop-session",
        "turnId": "turn-one",
        "command": "git status",
        "cwd": "/workspace/exact",
    }
    original_expiry = time.time() + 30
    fingerprint = bot._codex_app_server_request_fingerprint(
        "item/commandExecution/requestApproval",
        params,
    )
    controller.recover_pending_request(
        {
            "request_id": 42,
            "method": "item/commandExecution/requestApproval",
            "turn_id": "turn-one",
            "expires_at": original_expiry,
            "request_fingerprint": fingerprint,
            "slack_client_msg_id": "request-client-id",
            "slack_delivery_state": "pending",
        },
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(bot, "_audit_codex_app", lambda *args, **kwargs: None)

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 45,
            "method": "item/commandExecution/requestApproval",
            "params": params,
        },
    )

    assert controller.pending_snapshot()["expires_at"] == original_expiry


def test_failed_slack_request_delivery_clears_pending_state(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    output = []
    bot.out = output.append
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("Slack unavailable")
        ),
    )
    statuses = []
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda recovered, *, status: statuses.append(status),
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 43,
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )

    assert controller.pending_snapshot() is None
    assert statuses == ["pending-request", "desktop-required"]
    assert "request relay failed" in output[-1]


def test_expired_prompt_hands_off_without_responding_or_interrupting(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, route, text: posts.append(text),
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    with controller.lock:
        controller.active_turn_id = "turn-one"
        controller.terminal = False
    controller.set_pending_request(
        {
            "request_id": 51,
            "method": "item/fileChange/requestApproval",
            "expires_at": time.time() - 1,
        }
    )
    bot.codex_app_controllers[controller.route_key] = controller

    bot._tick_codex_app_controllers()

    connection = FakeControllerConnection.instances[-1]
    assert connection.responses == []
    assert not any(
        method == "turn/interrupt" for method, _ in connection.requests
    )
    assert not connection.closed
    assert bot.codex_app_controllers[controller.route_key] is controller
    assert controller.desktop_required_snapshot() is True
    assert "No response or interrupt was sent" in posts[-1]


@pytest.mark.parametrize(
    "expires_at",
    (0, "invalid", float("inf"), float("nan")),
)
def test_expired_prompt_preserves_preparing_recovery(
    tmp_path,
    monkeypatch,
    expires_at,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    controller = CodexAppController(
        bot,
        route,
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.recover_pending_request(
        {
            "request_id": 51,
            "method": "item/fileChange/requestApproval",
            "turn_id": "turn-one",
            "expires_at": expires_at,
            "request_fingerprint": "expired-request",
        }
    )
    controller.defer_recovery_retry()
    bot._record_codex_app_controller(
        controller,
        status="preparing",
        client_message_id="client-one",
        text_hash=hashlib.sha256(b"continue").hexdigest(),
        preparing_action="start",
        preparing_text="continue",
    )
    bot.codex_app_controllers[controller.route_key] = controller
    posts = []
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda config, recovered_route, text: posts.append(text),
    )

    bot._tick_codex_app_controllers()

    assert bot.codex_app_controllers[controller.route_key] is controller
    assert controller.pending_snapshot() is None
    assert controller.pending_replay_snapshot() is None
    assert controller.recovery_blocked_snapshot() is True
    assert controller.recovery_handoff_required_snapshot() is True
    assert controller.recovery_handoff_request_id_snapshot() == 51
    assert controller.desktop_required_snapshot() is False
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "preparing"
    assert saved_job["preparing_text"] == "continue"
    assert saved_job["pending_request"] is None
    assert saved_job["recovery_handoff_required"] is True
    assert "preserved the pending input recovery record" in posts[-1]


def test_recovery_expiry_preserves_distinct_existing_handoff(
    tmp_path,
):
    bot = _bot(tmp_path, notification_only=False)
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.recover_pending_request(
        {
            "request_id": 51,
            "method": "item/fileChange/requestApproval",
            "turn_id": "turn-one",
            "expires_at": time.time() - 1,
            "request_fingerprint": "expired-request",
        }
    )
    controller.mark_recovery_blocked()
    controller.mark_recovery_handoff_required(41, "first-request")

    expired = controller.expire_pending_during_recovery(time.time())

    assert expired["request_id"] == 51
    assert controller.recovery_handoff_required_snapshot() is True
    assert controller.recovery_handoff_request_id_snapshot() is None
    assert controller.recovery_handoff_request_fingerprint_snapshot() is None
    assert controller.finalize_recovery() is True
    assert controller.desktop_required_snapshot() is True
    assert controller.desktop_request_ids_snapshot() == []
    assert controller.clear_desktop_required_for_request(41) is False
    assert controller.clear_desktop_required_for_request(51) is False


def test_expiry_isolates_notification_and_state_failures(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    output = []
    bot.out = output.append
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    controller.set_pending_request(
        {
            "request_id": 52,
            "method": "item/fileChange/requestApproval",
            "expires_at": time.time() - 1,
        }
    )
    bot.codex_app_controllers[controller.route_key] = controller
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("Slack unavailable")
        ),
    )
    monkeypatch.setattr(
        bot,
        "_record_codex_app_controller",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("state unavailable")
        ),
    )
    audits = []
    monkeypatch.setattr(
        bot,
        "_audit_codex_app",
        lambda recovered, action, **fields: audits.append(action),
    )

    bot._tick_codex_app_controllers()

    assert bot.codex_app_controllers[controller.route_key] is controller
    assert audits == ["request_expired"]
    assert any("expiry notification failed" in item for item in output)
    assert any("expiry state update failed" in item for item in output)


def test_expiry_resolution_race_retains_active_controller(
    tmp_path,
    monkeypatch,
):
    FakeControllerConnection.instances.clear()
    bot = _bot(tmp_path, notification_only=False)
    monkeypatch.setattr(
        slackbot_module,
        "CodexAppServerConnection",
        FakeControllerConnection,
    )
    controller = CodexAppController(
        bot,
        SlackbotRoute(channel_id="D111", thread_ts="1.000001"),
        {
            "kind": "codex_app_server",
            "thread_id": "desktop-session",
            "source": "vscode",
            "cwd": "/workspace/exact",
        },
    )
    controller.connect()
    with controller.lock:
        controller.active_turn_id = "turn-one"
        controller.terminal = False
    controller.set_pending_request(
        {
            "request_id": 51,
            "method": "item/fileChange/requestApproval",
            "expires_at": time.time() - 1,
        }
    )
    bot.codex_app_controllers[controller.route_key] = controller
    expiry_post_started = threading.Event()
    release_expiry_post = threading.Event()
    resolution_started = threading.Event()
    resolution_finished = threading.Event()
    posts = []

    def blocking_post(config, route, text):
        posts.append(text)
        if "Codex Slack prompt expired" in text:
            expiry_post_started.set()
            assert release_expiry_post.wait(timeout=2)

    def resolve_request():
        resolution_started.set()
        bot._handle_codex_app_notification(
            controller,
            {
                "method": "serverRequest/resolved",
                "params": {
                    "threadId": "desktop-session",
                    "requestId": 51,
                },
            },
        )
        resolution_finished.set()

    monkeypatch.setattr(slackbot_module, "post_slack_message", blocking_post)
    worker = threading.Thread(target=bot._tick_codex_app_controllers)
    worker.start()
    assert expiry_post_started.wait(timeout=2)
    resolution_worker = threading.Thread(target=resolve_request)
    resolution_worker.start()
    assert resolution_started.wait(timeout=2)
    try:
        assert not resolution_finished.wait(timeout=0.1)
    finally:
        release_expiry_post.set()
    worker.join(timeout=2)
    resolution_worker.join(timeout=2)

    assert not worker.is_alive()
    assert not resolution_worker.is_alive()
    assert resolution_finished.is_set()
    assert "Codex Slack prompt expired" in posts[0]
    assert "Slack controls are available again" in posts[-1]
    assert bot.codex_app_controllers[controller.route_key] is controller
    assert controller.desktop_required_snapshot() is False
    assert not FakeControllerConnection.instances[-1].closed
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["status"] == "active"


def test_expiry_cannot_overwrite_rebound_controller_state(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path, notification_only=False)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    expired = CodexAppController(
        bot,
        route,
        {
            "kind": "codex_app_server",
            "thread_id": "old-session",
            "source": "vscode",
            "cwd": "/workspace/old",
        },
    )
    expired.set_pending_request(
        {
            "request_id": 51,
            "method": "item/fileChange/requestApproval",
            "expires_at": time.time() - 1,
        }
    )
    bot.codex_app_controllers[expired.route_key] = expired
    expiry_observed = threading.Event()
    original_expired = expired.expired

    def observe_expiry(now):
        expiry_observed.set()
        return original_expired(now)

    monkeypatch.setattr(expired, "expired", observe_expiry)
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: None,
    )
    worker = threading.Thread(target=bot._tick_codex_app_controllers)
    route_lock = f"codex-app:{expired.route_key}"
    with bot._conversation_lock(route_lock):
        worker.start()
        assert expiry_observed.wait(timeout=2)
        rebound = CodexAppController(
            bot,
            route,
            {
                "kind": "codex_app_server",
                "thread_id": "new-session",
                "source": "vscode",
                "cwd": "/workspace/new",
            },
        )
        with bot.codex_app_controllers_lock:
            bot.codex_app_controllers[expired.route_key] = rebound
        bot._record_codex_app_controller(rebound, status="active")
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert bot.codex_app_controllers[expired.route_key] is rebound
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][expired.route_key]
    assert saved_job["status"] == "active"
    assert saved_job["binding"] == rebound.binding


def test_private_socket_mode_check_uses_posix_permissions(tmp_path):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    try:
        mode = stat.S_IMODE(socket_path.stat().st_mode)
        assert mode == 0o600
    finally:
        listener.close()
        shutil.rmtree(codex_home)
