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
    assert normalize_root_path("kickle/console/") == "/kickle/console"
    assert normalize_root_path("/") == "/"
    assert normalize_root_path("") == "/kickle/console"


def test_parse_email_list_normalizes_and_deduplicates():
    assert parse_email_list("TrentN@nvidia.com, trentn@nvidia.com,") == (
        "trentn@nvidia.com",
    )


def test_console_user_requires_authenticated_email_header():
    config = ConsoleConfig()

    with pytest.raises(ConsoleAuthError) as exc_info:
        user_from_headers(config, {})

    assert exc_info.value.status_code == 401


def test_console_user_allows_nvidia_viewer_and_admin():
    config = ConsoleConfig(admin_emails=("trentn@nvidia.com",))

    viewer = user_from_headers(
        config,
        {"x-auth-request-email": "a@nvidia.com"},
    )
    admin = user_from_headers(
        config,
        {"x-forwarded-email": "trentn@nvidia.com"},
    )

    assert viewer.email == "a@nvidia.com"
    assert viewer.can_write is False
    assert admin.email == "trentn@nvidia.com"
    assert admin.can_write is True


def test_console_user_rejects_non_matching_domain():
    config = ConsoleConfig()

    with pytest.raises(ConsoleAuthError) as exc_info:
        user_from_headers(config, {"x-auth-request-email": "a@example.com"})

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

    assert calls[0][:3] == ["tmux", "-L", "kickle"]
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
            stderr="no server running on /tmp/tmux-1000/kickle\n",
        )

    monkeypatch.setattr("sneeze.console.subprocess.run", fake_run)

    assert list_tmux_sessions(ConsoleConfig(tmux_bin="tmux")) == []
