from __future__ import annotations

from types import SimpleNamespace

import pytest

from sneeze.console import (
    ConsoleAuthError,
    ConsoleConfig,
    list_tmux_sessions,
    normalize_root_path,
    parse_email_list,
    user_from_headers,
)


def test_normalize_root_path():
    assert normalize_root_path("sample/console/") == "/sample/console"
    assert normalize_root_path("/") == "/"
    assert normalize_root_path("") == "/console"


def test_parse_email_list_normalizes_and_deduplicates():
    assert parse_email_list("Admin@Example.com, admin@example.com,") == (
        "admin@example.com",
    )


def test_console_user_requires_authenticated_email_header():
    config = ConsoleConfig()

    with pytest.raises(ConsoleAuthError) as exc_info:
        user_from_headers(config, {})

    assert exc_info.value.status_code == 401


def test_console_user_allows_matching_domain_viewer_and_admin():
    config = ConsoleConfig(
        auth_email_domain="example.com",
        admin_emails=("admin@example.com",),
    )

    viewer = user_from_headers(
        config,
        {"x-auth-request-email": "viewer@example.com"},
    )
    admin = user_from_headers(
        config,
        {"x-forwarded-email": "admin@example.com"},
    )

    assert viewer.email == "viewer@example.com"
    assert viewer.can_write is False
    assert admin.email == "admin@example.com"
    assert admin.can_write is True


def test_console_user_rejects_non_matching_domain():
    config = ConsoleConfig(auth_email_domain="example.com")

    with pytest.raises(ConsoleAuthError) as exc_info:
        user_from_headers(config, {"x-auth-request-email": "a@other.test"})

    assert exc_info.value.status_code == 403


def test_list_tmux_sessions_parses_session_and_window_rows(monkeypatch):
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if args[3] == "list-sessions":
            return SimpleNamespace(
                returncode=0,
                stdout="alpha\x1f2\x1f123\x1f1\nbeta\x1f1\x1f456\x1f0\n",
                stderr="",
            )
        if args[3] == "list-windows":
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "alpha\x1f0\x1fshell\x1f1\x1f1\x1fzsh\n"
                    "alpha\x1f1\x1flogs\x1f0\x1f1\x1ftail\n"
                    "beta\x1f0\x1fmain\x1f1\x1f1\x1fbash\n"
                ),
                stderr="",
            )
        raise AssertionError(args)

    monkeypatch.setattr("sneeze.console.subprocess.run", fake_run)

    sessions = list_tmux_sessions(ConsoleConfig(tmux_bin="tmux"))

    assert calls[0][:3] == ["tmux", "-L", "sneeze"]
    assert sessions == [
        {
            "name": "alpha",
            "window_count": 2,
            "created": 123,
            "attached": True,
            "windows": [
                {
                    "index": 0,
                    "name": "shell",
                    "active": True,
                    "pane_count": 1,
                    "current_command": "zsh",
                },
                {
                    "index": 1,
                    "name": "logs",
                    "active": False,
                    "pane_count": 1,
                    "current_command": "tail",
                },
            ],
        },
        {
            "name": "beta",
            "window_count": 1,
            "created": 456,
            "attached": False,
            "windows": [
                {
                    "index": 0,
                    "name": "main",
                    "active": True,
                    "pane_count": 1,
                    "current_command": "bash",
                }
            ],
        },
    ]


def test_list_tmux_sessions_returns_empty_without_tmux_server(monkeypatch):
    def fake_run(args, check, capture_output, text):
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="no server running on /tmp/tmux-1000/sneeze\n",
        )

    monkeypatch.setattr("sneeze.console.subprocess.run", fake_run)

    assert list_tmux_sessions(ConsoleConfig(tmux_bin="tmux")) == []
