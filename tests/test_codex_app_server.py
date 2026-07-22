import hashlib
import json
import queue
import shutil
import socket
import stat
import tempfile
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import sneeze.codex_app_server as codex_app_server_module
import sneeze.slackbot as slackbot_module
from sneeze.codex_app_server import (
    CodexAppServerConnection,
    CodexAppServerError,
    start_codex_app_server_daemon,
    validate_codex_app_server_socket,
)
from sneeze.slackbot import (
    CodexAppController,
    CodexAppPreDispatchError,
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
        self.requests = []
        self.responses = []
        self.closed = False
        type(self).instances.append(self)

    def connect(self):
        self.connected = True
        return self

    def close(self):
        self.connected = False
        self.closed = True

    def request(self, method, params):
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


def test_missing_daemon_is_started_once_before_socket_revalidation(
    tmp_path,
    monkeypatch,
):
    bot = _bot(tmp_path)
    validations = []
    starts = []

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

    socket_path = bot._ensure_codex_app_server()

    assert validations == [socket_path, socket_path]
    assert starts == [(bot.config.codex_bin, socket_path)]


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
    assert any(
        method == "turn/start"
        and params["clientUserMessageId"] == "client-one"
        for method, params in connection.requests
    )
    assert any(
        method == "turn/steer" and params["expectedTurnId"] == "turn-started"
        for method, params in connection.requests
    )


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
    assert persisted == {
        "request_id": None,
        "method": pending["method"],
        "turn_id": pending["turn_id"],
        "expires_at": pending["expires_at"],
        "request_fingerprint": None,
    }
    assert statuses == ["awaiting-request-replay"]

    bot._handle_codex_app_server_request(
        controller,
        {
            "id": 99,
            "method": pending["method"],
            "params": {
                "threadId": "desktop-session",
                "turnId": "turn-one",
                "command": "git status",
                "cwd": "/workspace/exact",
            },
        },
    )

    assert controller.pending_snapshot()["request_id"] == 99
    assert statuses[-1] == "pending-request"
    assert posts


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

    def fake_recover(recovered_text, recovered_client_id):
        replayed.append((recovered_text, recovered_client_id))
        with controller.lock:
            controller.active_turn_id = "turn-recovered"
            controller.terminal = False
        return "turn-recovered"

    monkeypatch.setattr(controller, recovery_method, fake_recover)
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


@pytest.mark.parametrize(
    ("message_text", "dispatch_method", "preparing_text"),
    (
        ("continue the task", "start_turn", "continue the task"),
        ("steer add this", "steer", "add this"),
    ),
)
def test_pre_dispatch_failure_clears_replayable_input(
    tmp_path,
    monkeypatch,
    message_text,
    dispatch_method,
    preparing_text,
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
        dispatch_method,
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CodexAppPreDispatchError("not sent")
        ),
    )
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
    assert records[-1]["status"] == "connected"
    assert records[-1].get("preparing_text") is None
    assert records[-1]["clear_client_identity"] is True
    saved_job = slackbot_module._read_codex_app_jobs(bot.config.paths)[
        "jobs"
    ][controller.route_key]
    assert saved_job["client_message_id"] is None
    assert saved_job["text_sha256"] is None
    assert saved_job["preparing_text"] is None
    assert "not sent" in posts[-1]


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


def test_stale_request_context_does_not_reuse_recovery_deadline(
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
    current_params = {**stale_params, "command": "git diff"}
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
    assert statuses == ["desktop-required"]
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
    assert connection.closed
    assert "No response or interrupt was sent" in posts[-1]


def test_expiry_cleanup_isolates_notification_state_and_close_failures(
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
    monkeypatch.setattr(
        controller,
        "close",
        lambda: (_ for _ in ()).throw(RuntimeError("close failed")),
    )

    bot._tick_codex_app_controllers()

    assert controller.route_key not in bot.codex_app_controllers
    assert audits == ["request_expired"]
    assert any("expiry notification failed" in item for item in output)
    assert any("expiry state update failed" in item for item in output)
    assert any("controller cleanup failed" in item for item in output)


def test_private_socket_mode_check_uses_posix_permissions(tmp_path):
    listener, socket_path, codex_home = _private_socket(tmp_path)
    try:
        mode = stat.S_IMODE(socket_path.stat().st_mode)
        assert mode == 0o600
    finally:
        listener.close()
        shutil.rmtree(codex_home)
