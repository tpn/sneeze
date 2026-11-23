from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CodexSessionMetadata:
    session_id: str
    source: str | None
    thread_source: str | None
    cwd: str | None
    rollout_path: str | None
    parent_thread_id: str | None
    review_turn: bool
    verified: bool
    error: str | None = None
    automated_review: bool = False


ROBOREV_PROMPT_MARKERS = (
    "you are being invoked by roborev",
    "invoked by roborev to perform this review directly",
)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_source(value: Any) -> str | None:
    if isinstance(value, dict):
        return "subagent" if "subagent" in value else None
    text = _text(value)
    if not text:
        return None
    if text.startswith("{"):
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            return None
        else:
            if isinstance(decoded, dict) and "subagent" in decoded:
                return "subagent"
            return None
    return text.lower()


def codex_home_path(
    *,
    environment: dict[str, str] | None = None,
) -> Path:
    environment = os.environ if environment is None else environment
    configured = environment.get("CODEX_HOME")
    return (
        Path(configured).expanduser()
        if configured
        else Path.home() / ".codex"
    )


def codex_thread_name(
    session_id: str | None,
    *,
    environment: dict[str, str] | None = None,
) -> str | None:
    """Return the latest saved name for this chat, including renames."""
    if not session_id:
        return None
    index = codex_home_path(environment=environment) / "session_index.jsonl"
    latest_name = None
    try:
        with index.open("rb") as records:
            for line in records:
                try:
                    record = json.loads(line)
                except (ValueError, RecursionError):
                    continue
                if not isinstance(record, dict):
                    continue
                name = record.get("thread_name")
                if record.get("id") == session_id and isinstance(name, str):
                    latest_name = name.strip() or latest_name
    except OSError:
        pass
    return latest_name


def _safe_rollout_path(value: Any, codex_home: Path) -> Path | None:
    text = _text(value)
    if not text:
        return None
    target = Path(text).expanduser().resolve()
    try:
        target.relative_to(codex_home.resolve())
    except ValueError:
        return None
    return target


def _state_db_thread(
    codex_home: Path,
    session_id: str,
) -> dict[str, Any] | None:
    state_db = codex_home / "state_5.sqlite"
    if not state_db.is_file():
        return None
    connection = sqlite3.connect(
        f"{state_db.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=0.1,
    )
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        row = connection.execute(
            "SELECT id, source, thread_source, cwd, rollout_path, "
            "first_user_message "
            "FROM threads WHERE id = ?",
            (session_id,),
        ).fetchone()
    finally:
        connection.close()
    return dict(row) if row is not None else None


def _session_meta(rollout_path: Path) -> dict[str, Any] | None:
    try:
        with rollout_path.open(encoding="utf-8") as handle:
            first_line = handle.readline()
        event = json.loads(first_line)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(event, dict) or event.get("type") != "session_meta":
        return None
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else None


def _event_turn_id(
    payload: dict[str, Any],
    current_turn_id: str | None,
) -> str | None:
    direct = _text(payload.get("turn_id"))
    if direct:
        return direct
    passthrough = payload.get("internal_chat_message_metadata_passthrough")
    if isinstance(passthrough, dict):
        direct = _text(passthrough.get("turn_id"))
        if direct:
            return direct
    return current_turn_id


def _rollout_turn_details(
    rollout_path: Path,
    turn_id: str | None,
) -> tuple[str | None, bool, bool]:
    if not turn_id:
        return None, False, False
    current_turn_id = None
    final_message = None
    task_message = None
    review_turn = False
    roborev_marker = False
    try:
        with rollout_path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict):
                    continue
                payload = event.get("payload")
                if not isinstance(payload, dict):
                    continue
                if event.get("type") == "turn_context":
                    current_turn_id = _text(payload.get("turn_id"))
                    continue
                event_turn_id = _event_turn_id(payload, current_turn_id)
                if event_turn_id != turn_id:
                    continue
                payload_type = _text(payload.get("type"))
                if payload_type in {
                    "enteredReviewMode",
                    "exitedReviewMode",
                    "entered_review_mode",
                    "exited_review_mode",
                }:
                    review_turn = True
                if event.get("type") == "event_msg":
                    if payload_type == "task_complete":
                        task_message = _text(
                            payload.get("last_agent_message")
                        )
                    elif (
                        payload_type == "agent_message"
                        and payload.get("phase") == "final_answer"
                    ):
                        final_message = _text(payload.get("message"))
                    elif payload_type == "user_message":
                        user_message = _text(payload.get("message")) or ""
                        roborev_marker = roborev_marker or any(
                            marker in user_message.lower()
                            for marker in ROBOREV_PROMPT_MARKERS
                        )
                if (
                    event.get("type") == "response_item"
                    and payload_type == "message"
                    and payload.get("role") == "assistant"
                    and payload.get("phase") == "final_answer"
                ):
                    content = payload.get("content")
                    if isinstance(content, list):
                        texts = [
                            str(item.get("text") or "")
                            for item in content
                            if isinstance(item, dict)
                            and item.get("type") == "output_text"
                        ]
                        candidate = "".join(texts).strip()
                        if candidate:
                            final_message = candidate
    except (OSError, UnicodeDecodeError):
        return None, review_turn, roborev_marker
    return task_message or final_message, review_turn, roborev_marker


def resolve_codex_session_metadata(
    payload: dict[str, Any],
    *,
    environment: dict[str, str] | None = None,
) -> CodexSessionMetadata:
    session_id = _text(payload.get("session_id"))
    if not session_id:
        return CodexSessionMetadata(
            session_id="",
            source=None,
            thread_source=None,
            cwd=None,
            rollout_path=None,
            parent_thread_id=None,
            review_turn=False,
            verified=False,
            error="missing-session-id",
        )
    codex_home = codex_home_path(environment=environment)
    try:
        db_row = _state_db_thread(codex_home, session_id)
    except sqlite3.Error:
        db_row = None
        db_error = "unreadable-state-db"
    else:
        db_error = None
    payload_rollout = _safe_rollout_path(
        payload.get("transcript_path"), codex_home
    )
    db_rollout = _safe_rollout_path(
        db_row.get("rollout_path") if db_row else None,
        codex_home,
    )
    if payload.get("transcript_path") and payload_rollout is None:
        return CodexSessionMetadata(
            session_id=session_id,
            source=None,
            thread_source=None,
            cwd=None,
            rollout_path=None,
            parent_thread_id=None,
            review_turn=False,
            verified=False,
            error="unsafe-transcript-path",
        )
    if payload_rollout and db_rollout and payload_rollout != db_rollout:
        return CodexSessionMetadata(
            session_id=session_id,
            source=None,
            thread_source=None,
            cwd=None,
            rollout_path=None,
            parent_thread_id=None,
            review_turn=False,
            verified=False,
            error="transcript-path-mismatch",
        )
    rollout_path = payload_rollout or db_rollout
    metadata = _session_meta(rollout_path) if rollout_path else None
    metadata_id = _text(metadata.get("id")) if metadata else None
    if (
        metadata is not None
        and metadata_id != session_id
        and (metadata_id is not None or db_row is None)
    ):
        return CodexSessionMetadata(
            session_id=session_id,
            source=None,
            thread_source=None,
            cwd=None,
            rollout_path=str(rollout_path) if rollout_path else None,
            parent_thread_id=None,
            review_turn=False,
            verified=False,
            error="transcript-session-mismatch",
        )
    db_source = _normalize_source(db_row.get("source")) if db_row else None
    transcript_source = (
        _normalize_source(metadata.get("source")) if metadata else None
    )
    if db_source and transcript_source and db_source != transcript_source:
        return CodexSessionMetadata(
            session_id=session_id,
            source=None,
            thread_source=None,
            cwd=None,
            rollout_path=str(rollout_path) if rollout_path else None,
            parent_thread_id=None,
            review_turn=False,
            verified=False,
            error="session-source-mismatch",
        )
    source = transcript_source or db_source
    thread_source = _text(
        metadata.get("thread_source") if metadata else None
    ) or _text(db_row.get("thread_source") if db_row else None)
    parent_thread_id = _text(
        metadata.get("parent_thread_id") if metadata else None
    )
    cwd = _text(metadata.get("cwd") if metadata else None) or _text(
        db_row.get("cwd") if db_row else None
    )
    _, review_turn, rollout_review_marker = (
        _rollout_turn_details(
            rollout_path,
            _text(payload.get("turn_id")),
        )
        if rollout_path
        else (None, False, False)
    )
    first_user_message = _text(
        db_row.get("first_user_message") if db_row else None
    )
    normalized_first_message = (first_user_message or "").lower()
    automated_review = source == "exec" and (
        rollout_review_marker
        or any(
            marker in normalized_first_message
            for marker in ROBOREV_PROMPT_MARKERS
        )
    )
    error = db_error if metadata is None else None
    if source is None:
        error = error or "unknown-session-source"
    if metadata is None and db_row is None:
        error = error or "session-not-found"
    return CodexSessionMetadata(
        session_id=session_id,
        source=source,
        thread_source=thread_source.lower() if thread_source else None,
        cwd=cwd,
        rollout_path=str(rollout_path) if rollout_path else None,
        parent_thread_id=parent_thread_id,
        review_turn=review_turn,
        verified=error is None,
        error=error,
        automated_review=automated_review,
    )


def resolve_codex_hook_message(
    payload: dict[str, Any],
    metadata: CodexSessionMetadata,
) -> str | None:
    message = _text(payload.get("last_assistant_message"))
    if message:
        return message
    if not metadata.rollout_path:
        return None
    resolved, _, _ = _rollout_turn_details(
        Path(metadata.rollout_path),
        _text(payload.get("turn_id")),
    )
    return resolved
