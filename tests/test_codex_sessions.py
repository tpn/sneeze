import json
import sqlite3
from pathlib import Path

import sneeze.slackbot as slackbot_module
from sneeze.codex_sessions import (
    resolve_codex_hook_message,
    resolve_codex_session_metadata,
)
from sneeze.slackbot import (
    SlackbotProfile,
    decide_codex_stop_notification,
    notify_codex_agent_stop,
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
    )


def _write_session(
    codex_home: Path,
    *,
    session_id: str,
    source: object,
    thread_source: str = "user",
    parent_thread_id: str | None = None,
    first_user_message: str = "Please continue.",
) -> Path:
    rollout_path = codex_home / "sessions" / f"{session_id}.jsonl"
    rollout_path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        {
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "source": source,
                "thread_source": thread_source,
                "cwd": "/workspace/exact",
                "parent_thread_id": parent_thread_id,
            },
        },
        {
            "type": "turn_context",
            "payload": {"turn_id": "turn-target"},
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "message": first_user_message,
            },
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "last_agent_message": "Exact target turn result.",
            },
        },
        {
            "type": "turn_context",
            "payload": {"turn_id": "turn-newer"},
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "last_agent_message": "Different newer turn result.",
            },
        },
    ]
    rollout_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    state_db = codex_home / "state_5.sqlite"
    connection = sqlite3.connect(state_db)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS threads ("
            "id TEXT PRIMARY KEY, source TEXT, thread_source TEXT, "
            "cwd TEXT, rollout_path TEXT, first_user_message TEXT)"
        )
        stored_source = (
            json.dumps(source, separators=(",", ":"))
            if isinstance(source, dict)
            else str(source)
        )
        connection.execute(
            "INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?)",
            (
                session_id,
                stored_source,
                thread_source,
                "/workspace/exact",
                str(rollout_path),
                first_user_message,
            ),
        )
        connection.commit()
    finally:
        connection.close()
    return rollout_path


def test_exact_session_and_turn_resolve_from_codex_root(tmp_path):
    codex_home = tmp_path / ".codex"
    rollout_path = _write_session(
        codex_home,
        session_id="session-target",
        source="vscode",
    )
    _write_session(
        codex_home,
        session_id="session-newer",
        source="vscode",
    )
    payload = {
        "session_id": "session-target",
        "turn_id": "turn-target",
    }

    metadata = resolve_codex_session_metadata(
        payload,
        environment={"CODEX_HOME": str(codex_home)},
    )

    assert metadata.verified
    assert metadata.source == "vscode"
    assert metadata.cwd == "/workspace/exact"
    assert metadata.rollout_path == str(rollout_path)
    assert resolve_codex_hook_message(payload, metadata) == (
        "Exact target turn result."
    )


def test_rollout_only_metadata_requires_matching_session_id(tmp_path):
    codex_home = tmp_path / ".codex"
    rollout_path = _write_session(
        codex_home,
        session_id="session-target",
        source="vscode",
    )
    events = [
        json.loads(line)
        for line in rollout_path.read_text(encoding="utf-8").splitlines()
    ]
    events[0]["payload"].pop("id")
    rollout_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    (codex_home / "state_5.sqlite").unlink()

    metadata = resolve_codex_session_metadata(
        {
            "session_id": "session-target",
            "transcript_path": str(rollout_path),
        },
        environment={"CODEX_HOME": str(codex_home)},
    )

    assert metadata.verified is False
    assert metadata.source is None
    assert metadata.cwd is None
    assert metadata.error == "transcript-session-mismatch"


def test_roborev_exec_session_is_detected_from_codex_state(tmp_path):
    codex_home = tmp_path / ".codex"
    _write_session(
        codex_home,
        session_id="review-session",
        source="exec",
        first_user_message=(
            "IMPORTANT: You are being invoked by roborev to perform this "
            "review directly."
        ),
    )
    payload = {
        "session_id": "review-session",
        "turn_id": "turn-target",
    }
    environment = {"CODEX_HOME": str(codex_home)}
    metadata = resolve_codex_session_metadata(
        payload,
        environment=environment,
    )

    decision = decide_codex_stop_notification(
        _profile(tmp_path),
        payload,
        environment=environment,
        metadata=metadata,
    )

    assert metadata.verified
    assert metadata.source == "exec"
    assert metadata.automated_review
    assert not decision.notify
    assert decision.reason == "automated-review-session"

    forced_decision = decide_codex_stop_notification(
        _profile(tmp_path),
        payload,
        environment={
            **environment,
            "SNEEZE_CODEX_STOP_NOTIFY": "1",
        },
        metadata=metadata,
    )
    assert not forced_decision.notify
    assert forced_decision.reason == "automated-review-session"

    (codex_home / "state_5.sqlite").unlink()
    rollout_only_payload = {
        **payload,
        "transcript_path": str(
            codex_home / "sessions" / "review-session.jsonl"
        ),
    }
    rollout_only_metadata = resolve_codex_session_metadata(
        rollout_only_payload,
        environment=environment,
    )
    assert rollout_only_metadata.verified
    assert rollout_only_metadata.automated_review


def test_noninteractive_exec_is_excluded_by_source_allowlist(tmp_path):
    codex_home = tmp_path / ".codex"
    _write_session(
        codex_home,
        session_id="exec-session",
        source="exec",
        first_user_message="Run this one-off noninteractive task.",
    )
    payload = {
        "session_id": "exec-session",
        "turn_id": "turn-target",
    }
    environment = {"CODEX_HOME": str(codex_home)}
    metadata = resolve_codex_session_metadata(
        payload,
        environment=environment,
    )

    decision = decide_codex_stop_notification(
        _profile(tmp_path),
        payload,
        environment=environment,
        metadata=metadata,
    )

    assert not metadata.automated_review
    assert not decision.notify
    assert decision.reason == "session-source-not-allowed"


def test_malformed_json_session_source_is_unverified(tmp_path):
    codex_home = tmp_path / ".codex"
    _write_session(
        codex_home,
        session_id="malformed-source",
        source='{"subagent":',
    )

    metadata = resolve_codex_session_metadata(
        {
            "session_id": "malformed-source",
            "turn_id": "turn-target",
        },
        environment={"CODEX_HOME": str(codex_home)},
    )

    assert metadata.verified is False
    assert metadata.source is None
    assert metadata.error == "unknown-session-source"


def test_desktop_session_is_eligible_for_notification(tmp_path):
    codex_home = tmp_path / ".codex"
    _write_session(
        codex_home,
        session_id="desktop-session",
        source="vscode",
    )
    payload = {
        "session_id": "desktop-session",
        "turn_id": "turn-target",
    }
    environment = {"CODEX_HOME": str(codex_home)}
    metadata = resolve_codex_session_metadata(
        payload,
        environment=environment,
    )

    decision = decide_codex_stop_notification(
        _profile(tmp_path),
        payload,
        environment=environment,
        metadata=metadata,
    )

    assert decision.notify
    assert decision.reason == "interactive-session"


def test_subagent_session_is_not_eligible(tmp_path):
    codex_home = tmp_path / ".codex"
    source = {
        "subagent": {"thread_spawn": {"parent_thread_id": "parent-session"}}
    }
    _write_session(
        codex_home,
        session_id="subagent-session",
        source=source,
        thread_source="subagent",
        parent_thread_id="parent-session",
    )
    payload = {
        "session_id": "subagent-session",
        "turn_id": "turn-target",
    }
    environment = {"CODEX_HOME": str(codex_home)}
    metadata = resolve_codex_session_metadata(
        payload,
        environment=environment,
    )

    decision = decide_codex_stop_notification(
        _profile(tmp_path),
        payload,
        environment=environment,
        metadata=metadata,
    )

    assert not decision.notify
    assert decision.reason == "subagent-session"


def test_unavailable_codex_metadata_fails_open(tmp_path):
    codex_home = tmp_path / ".codex"
    payload = {
        "session_id": "missing-session",
        "turn_id": "turn-target",
        "last_assistant_message": "Still deliver this result.",
    }
    environment = {"CODEX_HOME": str(codex_home)}
    metadata = resolve_codex_session_metadata(
        payload,
        environment=environment,
    )

    decision = decide_codex_stop_notification(
        _profile(tmp_path),
        payload,
        environment=environment,
        metadata=metadata,
    )

    assert not metadata.verified
    assert decision.notify
    assert decision.reason == "metadata-unavailable-fail-open"


def test_stop_hook_skips_roborev_before_slack_delivery(
    tmp_path,
    monkeypatch,
):
    codex_home = tmp_path / ".codex"
    rollout_path = _write_session(
        codex_home,
        session_id="review-session",
        source="exec",
        first_user_message=(
            "IMPORTANT: You are being invoked by roborev to perform this "
            "review directly."
        ),
    )
    profile = _profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(
        slackbot_module,
        "post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Roborev Stop hook reached Slack delivery")
        ),
    )

    result = notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "hook_event_name": "Stop",
                "session_id": "review-session",
                "turn_id": "turn-target",
                "transcript_path": str(rollout_path),
                "last_assistant_message": "No issues found.",
            }
        ),
    )

    assert result == {
        "status": "skipped",
        "reason": "automated-review-session",
        "source": "exec",
        "origin": None,
    }
