import json
import os
import shlex
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import sneeze.slackbot as slackbot_module
from sneeze.slackbot import (
    CHILD_ENV_PASSTHROUGH,
    USER_CONFIG_DISPATCH_LIMIT,
    USER_CONFIG_MODAL_CALLBACK_ID,
    USER_CONFIG_OPEN_ACTION_ID,
    WORK_QUEUE_DEPTH_PER_WORKER,
    AgentTmuxState,
    CodexRunner,
    SlackApiError,
    SlackbotDiagnosticResult,
    SlackbotError,
    SlackbotProfile,
    SlackbotRoute,
    SlackbotStaticResponse,
    SlackbotUserConfigSecretField,
    SlackSocketBot,
    append_slackbot_audit_event,
    bind_agent_tmux_thread,
    build_user_config_view,
    capture_agent_tmux_thread,
    chunk_text,
    effective_codex_policy,
    enqueue_ingress,
    extract_codex_session_id,
    install_codex_agent_hooks,
    list_schedules,
    load_config,
    merge_child_process_env,
    notify_codex_agent_stop,
    parse_slack_thread_permalink,
    paste_agent_tmux_text,
    post_slack_blocks,
    post_slack_message,
    query_agent_tmux_thread,
    query_codex_agent_threads,
    query_status,
    read_env_file,
    read_json,
    read_slack_route_context,
    read_slack_thread,
    read_slackbot_trust,
    read_user_secret_env,
    render_effective_codex_policy,
    render_launchd_service,
    render_slack_blocks,
    render_systemd_service,
    resolve_codex_agent_tmux_binding,
    resolve_slack_mentions,
    route_from_dict,
    route_to_dict,
    run_schedule,
    run_slackbot,
    safe_slack_storage_id,
    sample_agent_tmux_thread,
    save_user_config_submission,
    scaffold_runtime,
    slack_api_post,
    slackbot_working_text,
    static_response_text_from_profile,
    store_agent_tmux_job,
    store_user_secret_token,
    strip_bot_mentions,
    tmux_command_for_binding,
    trust_slack_channel,
    untrust_slack_channel,
    update_slack_message,
    upsert_schedule,
    user_config_mode_from_text,
    user_config_secret_field,
)
from sneeze.slackbot_commands import SlackbotEnqueueCodexBase
from sneeze.tmux_dev import resolve_executable

GITHUB_SECRET_FIELD = SlackbotUserConfigSecretField(
    service_name="github",
    label="GitHub token",
    env_names=("GH_TOKEN",),
    block_id="github_token",
    action_id="github_token",
    placeholder_name="GitHub",
    scrub_env_names=("GITHUB_TOKEN",),
)
GITLAB_SECRET_FIELD = SlackbotUserConfigSecretField(
    service_name="gitlab",
    label="GitLab token",
    env_names=("GITLAB_TOKEN",),
    block_id="gitlab_token",
    action_id="gitlab_token",
    placeholder_name="GitLab",
)
JIRA_SECRET_FIELD = SlackbotUserConfigSecretField(
    service_name="jira",
    label="Jira or Atlassian token",
    env_names=("ATLASSIAN_API_TOKEN", "JIRA_API_TOKEN", "JIRA_TOKEN"),
    block_id="jira_token",
    action_id="jira_token",
    placeholder_name="Atlassian",
)


def make_profile(tmp_path, *, user_config_secret_fields=()):
    return SlackbotProfile(
        app_slug="sample",
        env_prefix="SAMPLE",
        default_bot_name="sample",
        default_command_name="/sample",
        default_runtime_root=str(tmp_path / "runtime"),
        default_codex_workdir=str(tmp_path),
        default_system_prompt="# Sample Bot\n",
        default_unit_name="sample-slackbot.service",
        default_mcp_server_url="http://localhost:8945/mcp",
        user_config_secret_fields=tuple(user_config_secret_fields),
    )


class InlineExecutor:
    def submit(self, fn, *args):
        fn(*args)
        return SimpleNamespace()


def count_available_worker_slots(bot):
    acquired = 0
    while bot.work_semaphore.acquire(blocking=False):
        acquired += 1
    for _ in range(acquired):
        bot.work_semaphore.release()
    return acquired


def test_slackbot_working_text_includes_console_url(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(
        profile,
        bot_token="xoxb-test",
        app_token="xapp-test",
    )
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_CONSOLE_PUBLIC_BASE_URL=https://console.example.com\n",
        encoding="utf-8",
    )
    config = load_config(profile)

    assert slackbot_working_text(config) == (
        "Working...\n\n"
        "Sneeze console: https://console.example.com/sneeze/console/"
    )


def test_app_mention_uses_configured_default_execution_mode(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    config = replace(
        config,
        allowed_user_ids=("U111",),
        env={
            **config.env,
            "SAMPLE_SLACKBOT_EXECUTION_MODE": "tmux",
        },
    )
    bot = SlackSocketBot(config)
    calls = []
    monkeypatch.setattr(
        bot,
        "_run_codex_for_route",
        lambda text, route, **kwargs: calls.append(kwargs),
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "user": "U111",
            "channel": "C123",
            "text": "summarize",
            "ts": "1.000001",
        }
    )

    assert calls[0]["execution_mode"] == "tmux"


def test_profile_execution_and_tmux_defaults_flow_into_config(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        default_execution_mode="tmux",
        default_codex_tmux_socket="codex",
        default_codex_tmux_conf=str(tmp_path / "codex.tmux.conf"),
        default_codex_tmux_lifecycle="persistent",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    config = load_config(profile)
    status = query_status(profile)

    assert config.execution_mode == "tmux"
    assert config.env["SAMPLE_CODEX_TMUX_SOCKET"] == "codex"
    assert config.env["SAMPLE_CODEX_TMUX_CONF"] == str(
        tmp_path / "codex.tmux.conf"
    )
    assert config.env["SAMPLE_CODEX_TMUX_LIFECYCLE"] == "persistent"
    assert config.codex_tmux_lifecycle == "persistent"
    assert status["execution_mode"] == "tmux"
    assert status["codex_tmux_socket"] == "codex"
    assert status["codex_tmux_conf"] == str(tmp_path / "codex.tmux.conf")
    assert status["codex_tmux_lifecycle"] == "persistent"


def test_codex_runner_tmux_mode_records_job(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    runner = CodexRunner(config)
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        assert Path(args[0]).name == "tmux"
        if "kill-session" in args:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        assert args[1:4] == ["-L", "sneeze", "new-session"]
        run_dir = Path(args[-1]).parent
        (run_dir / "output.txt").write_text("done\n", encoding="utf-8")
        (run_dir / "stdout.jsonl").write_text(
            '{"type":"session_created","session_id":"codex-1"}\n',
            encoding="utf-8",
        )
        (run_dir / "stderr.txt").write_text("", encoding="utf-8")
        (run_dir / "status.txt").write_text("0\n", encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    result = runner.run_tmux(
        "prompt",
        route=SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
        project="docs",
    )
    jobs = read_json(config.paths.agent_tmux_jobs_path, {})
    job = next(iter(jobs.values()))

    assert result["returncode"] == 0
    assert result["last_message"] == "done\n"
    assert result["session_id"] == "codex-1"
    assert result["tmux_session"].startswith("sample-codex-")
    assert result["tmux_lifecycle"] == "ephemeral"
    assert job["status"] == "finished"
    assert job["tmux_socket"] == "sneeze"
    assert job["tmux_lifecycle"] == "ephemeral"
    assert job["tmux_session_closed"] is True
    assert job["route"]["channel_id"] == "C123"
    assert job["project"] == "docs"
    assert not (Path(result["run_dir"]) / "run.sh").exists()
    assert any("kill-session" in call for call in calls)


def test_codex_runner_tmux_mode_uses_configured_tmux_conf(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
        default_codex_tmux_conf=str(tmp_path / "codex.tmux.conf"),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    runner = CodexRunner(config)
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if args[-2:] == ["source-file", str(tmp_path / "codex.tmux.conf")]:
            assert Path(args[0]).name == "tmux"
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if "kill-session" in args:
            assert args[1:5] == [
                "-L",
                "codex",
                "-f",
                str(tmp_path / "codex.tmux.conf"),
            ]
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        assert Path(args[0]).name == "tmux"
        assert args[1:5] == [
            "-L",
            "codex",
            "-f",
            str(tmp_path / "codex.tmux.conf"),
        ]
        assert args[5:11] == [
            "new-session",
            "-d",
            "-s",
            args[8],
            "-c",
            str(tmp_path),
        ]
        run_dir = Path(args[-1]).parent
        (run_dir / "output.txt").write_text("done\n", encoding="utf-8")
        (run_dir / "stdout.jsonl").write_text(
            '{"type":"session_created","session_id":"codex-1"}\n',
            encoding="utf-8",
        )
        (run_dir / "stderr.txt").write_text("", encoding="utf-8")
        (run_dir / "status.txt").write_text("0\n", encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    result = runner.run_tmux("prompt")

    assert len(calls) == 3
    assert Path(calls[0][0]).name == "tmux"
    assert calls[0][1:] == [
        "-L",
        "codex",
        "-f",
        str(tmp_path / "codex.tmux.conf"),
        "source-file",
        str(tmp_path / "codex.tmux.conf"),
    ]
    assert result["tmux_socket"] == "codex"
    assert result["tmux_conf"] == str(tmp_path / "codex.tmux.conf")
    assert any("kill-session" in call for call in calls)


def test_codex_runner_persistent_tmux_lifecycle_leaves_session(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_lifecycle="persistent",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    runner = CodexRunner(config)
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        assert "kill-session" not in args
        run_dir = Path(args[-1]).parent
        (run_dir / "output.txt").write_text("done\n", encoding="utf-8")
        (run_dir / "stdout.jsonl").write_text(
            '{"type":"session_created","session_id":"codex-1"}\n',
            encoding="utf-8",
        )
        (run_dir / "stderr.txt").write_text("", encoding="utf-8")
        (run_dir / "status.txt").write_text("0\n", encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    result = runner.run_tmux("prompt")
    jobs = read_json(config.paths.agent_tmux_jobs_path, {})
    job = next(iter(jobs.values()))

    assert result["tmux_lifecycle"] == "persistent"
    assert job["tmux_lifecycle"] == "persistent"
    assert "tmux_session_closed" not in job
    assert not any("kill-session" in call for call in calls)


def wait_until(predicate, *, timeout_s=1.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_scaffold_runtime_writes_prefixed_env_and_prompt(tmp_path):
    profile = make_profile(tmp_path)
    custom_prompt = tmp_path / "custom" / "prompt.md"

    result = scaffold_runtime(
        profile,
        bot_token="xoxb-test",
        app_token="xapp-test",
        system_prompt_path=str(custom_prompt),
    )

    env_text = (tmp_path / "runtime" / ".env").read_text()
    prompt_text = custom_prompt.read_text()
    assert "SAMPLE_SLACK_BOT_TOKEN=xoxb-test" in env_text
    assert "SAMPLE_SLACK_APP_TOKEN=xapp-test" in env_text
    assert f"SAMPLE_SLACKBOT_SYSTEM_PROMPT_PATH={custom_prompt}" in env_text
    assert "# Sample Bot" in prompt_text
    assert result["runtime_root"] == str(tmp_path / "runtime")
    config = load_config(profile)
    assert config.paths.system_prompt_path == str(custom_prompt)
    assert config.codex_mode == "workspace-write"
    assert not any(
        word in env_text + prompt_text
        for word in ("legacy-internal-tool", "ticket-system")
    )
    mode = os.stat(tmp_path / "runtime" / ".env").st_mode & 0o777
    assert mode == 0o600


def test_scaffold_runtime_does_not_chmod_existing_env_parent(tmp_path):
    env_parent = tmp_path / "shared"
    env_parent.mkdir()
    env_parent.chmod(0o755)
    profile = replace(
        make_profile(tmp_path),
        default_env_path=str(env_parent / "sample.env"),
    )

    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    assert (env_parent.stat().st_mode & 0o777) == 0o755


def test_scaffold_runtime_honors_profile_state_and_prompt_defaults(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        default_runtime_root=str(tmp_path / "runtime-root"),
        default_state_dir=str(tmp_path / "state-root"),
        default_env_path=str(tmp_path / "config" / "sample.env"),
        default_system_prompt_path=str(
            tmp_path / "config" / "slackbot" / "prompt.md"
        ),
    )

    result = scaffold_runtime(profile)

    assert result["runtime_root"] == str(tmp_path / "runtime-root")
    assert result["state_dir"] == str(tmp_path / "state-root")
    assert result["env_path"] == str(tmp_path / "config" / "sample.env")
    assert result["system_prompt_path"] == str(
        tmp_path / "config" / "slackbot" / "prompt.md"
    )
    assert os.stat(tmp_path / "config").st_mode & 0o777 == 0o700
    assert os.stat(result["env_path"]).st_mode & 0o777 == 0o600
    assert os.stat(result["state_dir"]).st_mode & 0o777 == 0o700


def test_scaffold_runtime_honors_profile_default_allowlists(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        default_allowed_dm_user_ids=("U999",),
        default_allowed_user_ids=("U111",),
        default_allowed_channel_ids=("C999",),
        default_admin_user_ids=("UADMIN",),
    )

    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    assert config.allowed_dm_user_ids == ("U999",)
    assert config.allowed_user_ids == ("U111",)
    assert config.allowed_channel_ids == ("C999",)
    assert config.admin_user_ids == ("UADMIN",)
    env_path = Path(config.paths.env_path)
    env_text = env_path.read_text(encoding="utf-8")
    assert "SAMPLE_SLACKBOT_ALLOWED_DM_USER_IDS=U999" in env_text
    assert "SAMPLE_SLACKBOT_ALLOWED_USER_IDS=U111" in env_text
    assert "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=C999" in env_text
    assert "SAMPLE_SLACKBOT_ADMIN_USER_IDS=UADMIN" in env_text
    assert SlackSocketBot(config)._is_authorized("U222", "C999")

    env_text = (
        env_text.replace(
            "SAMPLE_SLACKBOT_ALLOWED_DM_USER_IDS=U999",
            "SAMPLE_SLACKBOT_ALLOWED_DM_USER_IDS=",
        )
        .replace(
            "SAMPLE_SLACKBOT_ALLOWED_USER_IDS=U111",
            "SAMPLE_SLACKBOT_ALLOWED_USER_IDS=",
        )
        .replace(
            "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=C999",
            "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=",
        )
        .replace(
            "SAMPLE_SLACKBOT_ADMIN_USER_IDS=UADMIN",
            "SAMPLE_SLACKBOT_ADMIN_USER_IDS=",
        )
    )
    env_path.write_text(env_text, encoding="utf-8")
    cleared_config = load_config(profile)

    assert cleared_config.allowed_dm_user_ids == ()
    assert cleared_config.allowed_user_ids == ()
    assert cleared_config.allowed_channel_ids == ()
    assert cleared_config.admin_user_ids == ()
    assert not SlackSocketBot(cleared_config)._is_authorized("U222", "C999")

    scaffold_runtime(profile)

    env_text = env_path.read_text(encoding="utf-8")
    assert "SAMPLE_SLACKBOT_ALLOWED_DM_USER_IDS=\n" in env_text
    assert "SAMPLE_SLACKBOT_ALLOWED_USER_IDS=\n" in env_text
    assert "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=\n" in env_text
    assert "SAMPLE_SLACKBOT_ADMIN_USER_IDS=\n" in env_text


def test_admin_falls_back_to_allowed_users(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        default_allowed_user_ids=("U111",),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    assert config.admin_user_ids == ("U111",)
    assert SlackSocketBot(config)._is_admin("U111")


def test_slackbot_profile_keeps_existing_positional_optionals_stable(
    tmp_path,
):
    profile = SlackbotProfile(
        "sample",
        "SAMPLE",
        "sample",
        "/sample",
        str(tmp_path / "runtime"),
        str(tmp_path),
        "# Sample Bot\n",
        str(tmp_path / "env"),
        "sample.service",
        3,
        "/tmp/codex",
    )

    assert profile.default_env_path == str(tmp_path / "env")
    assert profile.default_unit_name == "sample.service"
    assert profile.default_worker_count == 3
    assert profile.default_codex_bin == "/tmp/codex"
    assert profile.default_state_dir is None
    assert profile.default_system_prompt_path is None
    assert profile.user_config_secret_fields == ()
    assert profile.default_service_restart == "on-failure"
    assert profile.default_service_restart_sec == 10
    assert profile.default_systemd_wants == ()
    assert profile.default_systemd_after == ()

    full_profile = SlackbotProfile(
        "sample",
        "SAMPLE",
        "sample",
        "/sample",
        str(tmp_path / "runtime"),
        str(tmp_path),
        "# Sample Bot\n",
        str(tmp_path / "env"),
        "sample.service",
        3,
        "/tmp/codex",
        "workspace-write",
        None,
        None,
        (),
        None,
        None,
        None,
        None,
        (),
        ("ACME_API_KEY",),
    )
    assert full_profile.child_env_scrub_allowlist == ("ACME_API_KEY",)
    assert full_profile.static_responses == ()


def test_query_status_masks_tokens(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    monkeypatch.setattr(
        "sneeze.slackbot.slack_api_post",
        lambda token, method, payload: {
            "ok": True,
            "team": "Sample",
            "user_id": "U123",
            "url": "https://sample.slack.com/",
        },
    )

    status = query_status(profile)

    assert status["bot_token"] == "present:9"
    assert status["app_token"] == "present:9"
    assert status["command_name"] == "/sample"


def test_enqueue_ingress_writes_json_payload(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="hello",
        route=SlackbotRoute(
            channel_id="C123",
            thread_ts="1.23",
            response_url="https://response",
        ),
        slack_user_id="U111",
    )

    payload = read_json(path, {})
    assert payload["kind"] == "codex_prompt"
    assert payload["route"]["channel_id"] == "C123"
    assert payload["route"]["response_url"] == "https://response"
    assert payload["slack_user_id"] == "U111"
    assert payload["system_scoped"] is False
    assert route_to_dict(route_from_dict(payload["route"])) == {
        "channel_id": "C123",
        "dm_user_id": None,
        "mention_user_ids": [],
        "thread_ts": "1.23",
    }
    assert os.stat(path).st_mode & 0o777 == 0o600


def test_enqueue_ingress_channel_codex_requires_explicit_scope(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    with pytest.raises(SlackbotError, match="system_scoped=True"):
        enqueue_ingress(
            profile,
            kind="codex_prompt",
            text="hello",
            route=SlackbotRoute(channel_id="C123"),
        )


def test_enqueue_ingress_channel_codex_false_requires_user_scope(
    tmp_path,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    with pytest.raises(SlackbotError, match="requires slack_user_id"):
        enqueue_ingress(
            profile,
            kind="codex_prompt",
            text="hello",
            route=SlackbotRoute(channel_id="C123"),
            system_scoped=False,
        )


def test_enqueue_ingress_identical_payloads_do_not_collide(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    route = SlackbotRoute(channel_id="C123")
    first = enqueue_ingress(
        profile, kind="slack_message", text="same", route=route
    )
    second = enqueue_ingress(
        profile, kind="slack_message", text="same", route=route
    )

    assert first != second
    assert os.path.exists(first)
    assert os.path.exists(second)
    assert "system_scoped" not in read_json(first, {})


def test_schedule_upsert_list_and_run(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    result = upsert_schedule(
        profile,
        name="smoke",
        on_calendar="*-*-* 08:00:00",
        command=[
            sys.executable,
            "-c",
            "print('schedule smoke')",
        ],
        workdir=str(tmp_path),
        cli_bin="sample",
        run_subcommand="slackbot-schedule-run",
        install_timer=False,
    )

    schedules = list_schedules(profile)
    report = run_schedule(profile, name="smoke")
    assert result["unit"] == "sample-schedule-smoke.service"
    assert schedules[0].name == "smoke"
    assert report["returncode"] == 0
    assert report["stdout"].strip() == "schedule smoke"
    service_text = (
        tmp_path
        / "runtime"
        / "systemd"
        / "schedules"
        / "sample-schedule-smoke.service"
    ).read_text()
    assert "--runtime-root=" in service_text
    assert "--env-path=" in service_text
    assert "--state-dir=" in service_text
    assert "--system-prompt-path=" in service_text
    assert f"WorkingDirectory={tmp_path}" in service_text


def test_run_schedule_scrubs_secret_passthrough_env(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)
    captured_env = []

    monkeypatch.setenv("GITHUB_TOKEN", "ambient-secret")
    monkeypatch.setenv("ACME_MODE", "debug")
    monkeypatch.setattr(
        "sneeze.slackbot.CHILD_ENV_PASSTHROUGH",
        {
            *CHILD_ENV_PASSTHROUGH,
            "GITHUB_TOKEN",
            "ACME_MODE",
        },
    )

    def fake_run(command, cwd, env, **kwargs):
        captured_env.append(env)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    upsert_schedule(
        profile,
        name="smoke",
        on_calendar="*-*-* 08:00:00",
        command=[sys.executable, "-c", "print('schedule smoke')"],
        workdir=str(tmp_path),
        cli_bin="sample",
        run_subcommand="slackbot-schedule-run",
        install_timer=False,
    )
    report = run_schedule(profile, name="smoke")

    assert report["returncode"] == 0
    assert "GITHUB_TOKEN" not in captured_env[0]
    assert captured_env[0]["ACME_MODE"] == "debug"


def test_schedule_notifications_require_route(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    with pytest.raises(SlackbotError, match="require --channel-id"):
        upsert_schedule(
            profile,
            name="notify",
            on_calendar="*-*-* 08:00:00",
            command=[sys.executable, "-c", "print('notify')"],
            workdir=str(tmp_path),
            notify_kind="slack_message",
            cli_bin="sample",
            run_subcommand="slackbot-schedule-run",
            install_timer=False,
        )


def test_run_schedule_missing_file_reports_slackbot_error(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    with pytest.raises(SlackbotError, match="Schedule not found"):
        run_schedule(profile, name="missing")


def test_run_schedule_records_launch_failure(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)
    upsert_schedule(
        profile,
        name="missing-bin",
        on_calendar="*-*-* 08:00:00",
        command=[str(tmp_path / "does-not-exist")],
        workdir=str(tmp_path),
        cli_bin="sample",
        run_subcommand="slackbot-schedule-run",
        install_timer=False,
    )

    report = run_schedule(profile, name="missing-bin")

    assert report["returncode"] == 127
    assert report["error"]
    assert Path(report["report_path"]).exists()


def test_schedule_names_reject_path_traversal(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    with pytest.raises(SlackbotError, match="Schedule names"):
        upsert_schedule(
            profile,
            name="../escape",
            on_calendar="*-*-* 08:00:00",
            command=[sys.executable, "-c", "print('bad')"],
            workdir=str(tmp_path),
            cli_bin="sample",
            run_subcommand="slackbot-schedule-run",
            install_timer=False,
        )


def test_permalink_parser():
    channel, ts = parse_slack_thread_permalink(
        "https://example.slack.com/archives/C123/p1716249600123456"
    )
    assert channel == "C123"
    assert ts == "1716249600.123456"


def test_permalink_parser_prefers_parent_thread_query():
    channel, ts = parse_slack_thread_permalink(
        "https://example.slack.com/archives/C123/p1716249600456789"
        "?thread_ts=1716249600.123456&cid=C123"
    )
    assert channel == "C123"
    assert ts == "1716249600.123456"


def test_permalink_parser_rejects_non_slack_timestamp():
    with pytest.raises(SlackbotError, match="Unsupported Slack permalink"):
        parse_slack_thread_permalink(
            "https://example.slack.com/archives/C123/pabc"
        )


def test_read_slack_thread_resolves_reply_permalink_to_parent(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        if method == "conversations.history":
            return {"ok": True, "messages": []}
        if (
            method == "conversations.replies"
            and payload["ts"] == "1716249602.000001"
        ):
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "1716249602.000001",
                        "thread_ts": "1716249600.000001",
                        "user": "U222",
                        "text": "reply",
                    }
                ],
            }
        if method == "conversations.replies":
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "1716249600.000001",
                        "thread_ts": "1716249600.000001",
                        "user": "U111",
                        "text": "parent",
                    },
                    {
                        "ts": "1716249602.000001",
                        "thread_ts": "1716249600.000001",
                        "user": "U222",
                        "text": "reply",
                    },
                ],
            }
        raise AssertionError(method)

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    messages = read_slack_thread(
        config,
        "https://example.slack.com/archives/C123/p1716249602000001",
    )

    assert calls == [
        (
            "conversations.history",
            {
                "channel": "C123",
                "limit": 1,
                "latest": "1716249602.000001",
                "oldest": "1716249602.000001",
                "inclusive": True,
            },
        ),
        (
            "conversations.replies",
            {"channel": "C123", "ts": "1716249602.000001", "limit": 1},
        ),
        (
            "conversations.replies",
            {"channel": "C123", "ts": "1716249600.000001", "limit": 200},
        ),
    ]
    assert [message["text"] for message in messages] == ["parent", "reply"]


def test_read_slack_route_context_uses_thread_replies(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {
            "ok": True,
            "messages": [
                {"ts": "2.000001", "user": "U222", "text": "reply"},
                {"ts": "1.000001", "user": "U111", "text": "parent"},
            ],
        }

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    messages = read_slack_route_context(
        config,
        SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
        source_ts="2.000001",
        source_thread_ts="1.000001",
    )

    assert calls == [
        (
            "conversations.replies",
            {"channel": "C123", "ts": "1.000001", "limit": 200},
        )
    ]
    assert [message["text"] for message in messages] == ["parent", "reply"]


def test_read_slack_route_context_auto_joins_before_history(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []
    attempts = {"history": 0}

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        if method == "conversations.history":
            attempts["history"] += 1
            if attempts["history"] == 1:
                raise SlackApiError(
                    method,
                    {"ok": False, "error": "not_in_channel"},
                )
            return {
                "ok": True,
                "messages": [
                    {"ts": "1.000001", "user": "U111", "text": "parent"},
                ],
            }
        if method == "conversations.join":
            return {"ok": True, "channel": {"id": "C123"}}
        raise AssertionError(method)

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    messages = read_slack_route_context(
        config,
        SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
        source_ts="1.000001",
    )

    assert calls == [
        (
            "conversations.history",
            {
                "channel": "C123",
                "limit": 10,
                "latest": "1.000001",
                "inclusive": True,
            },
        ),
        ("conversations.join", {"channel": "C123"}),
        (
            "conversations.history",
            {
                "channel": "C123",
                "limit": 10,
                "latest": "1.000001",
                "inclusive": True,
            },
        ),
    ]
    assert [message["text"] for message in messages] == ["parent"]


def test_read_slack_route_context_falls_back_when_thread_replies_invalid(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        if method == "conversations.replies":
            raise SlackApiError(
                method,
                {
                    "ok": False,
                    "error": "invalid_arguments",
                    "response_metadata": {
                        "messages": ["[ERROR] invalid ts"],
                    },
                },
            )
        return {
            "ok": True,
            "messages": [
                {
                    "ts": "1.000001",
                    "user": "U222",
                    "text": "<@UBOT> compare to above",
                },
                {
                    "ts": "0.999999",
                    "user": "U111",
                    "text": "Leo bot link: https://example.test/issue/19",
                },
            ],
        }

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    messages = read_slack_route_context(
        config,
        SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
        source_ts="2.000001",
        source_thread_ts="1.000001",
    )

    assert calls == [
        (
            "conversations.replies",
            {"channel": "C123", "ts": "1.000001", "limit": 200},
        ),
        (
            "conversations.history",
            {
                "channel": "C123",
                "limit": 10,
                "latest": "1.000001",
                "inclusive": True,
            },
        ),
    ]
    assert [message["text"] for message in messages] == [
        "Leo bot link: https://example.test/issue/19",
        "<@UBOT> compare to above",
    ]


def test_read_slack_route_context_uses_recent_channel_history(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {
            "ok": True,
            "messages": [
                {
                    "ts": "2.000001",
                    "user": "U222",
                    "text": "<@UBOT> compare to above",
                },
                {
                    "ts": "1.000001",
                    "user": "U111",
                    "text": "Leo bot link: https://example.test/issue/19",
                },
            ],
        }

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    messages = read_slack_route_context(
        config,
        SlackbotRoute(channel_id="C123", thread_ts="2.000001"),
        source_ts="2.000001",
    )

    assert calls == [
        (
            "conversations.history",
            {
                "channel": "C123",
                "limit": 10,
                "latest": "2.000001",
                "inclusive": True,
            },
        )
    ]
    assert [message["text"] for message in messages] == [
        "Leo bot link: https://example.test/issue/19",
        "<@UBOT> compare to above",
    ]


def test_slack_api_error_includes_response_metadata_messages():
    error = SlackApiError(
        "conversations.replies",
        {
            "ok": False,
            "error": "invalid_arguments",
            "response_metadata": {
                "messages": ["[ERROR] invalid ts"],
            },
        },
    )

    assert error.error == "invalid_arguments"
    assert "[ERROR] invalid ts" in str(error)


def test_slack_api_post_form_encodes_web_api_payloads(monkeypatch):
    requests = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        requests.append(request)
        assert timeout == 30
        return FakeResponse()

    monkeypatch.setattr(
        "sneeze.slackbot.urllib.request.urlopen",
        fake_urlopen,
    )

    slack_api_post(
        "xoxb-test",
        "chat.postMessage",
        {
            "channel": "C123",
            "text": "hello",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*hello*"},
                }
            ],
        },
    )

    request = requests[0]
    parsed = urllib.parse.parse_qs(request.data.decode("utf-8"))
    assert parsed["channel"] == ["C123"]
    assert parsed["text"] == ["hello"]
    assert json.loads(parsed["blocks"][0]) == [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*hello*"},
        }
    ]
    assert request.get_header("Content-type").startswith(
        "application/x-www-form-urlencoded"
    )


def test_slack_api_request_policy_bounds_retries(monkeypatch):
    timeouts = []
    sleeps = []
    client_msg_ids = []

    def fail_urlopen(request, timeout):
        timeouts.append(timeout)
        payload = urllib.parse.parse_qs(request.data.decode("utf-8"))
        client_msg_ids.append(payload["client_msg_id"][0])
        raise slackbot_module.urllib.error.URLError("offline")

    monkeypatch.setattr(
        "sneeze.slackbot.urllib.request.urlopen",
        fail_urlopen,
    )
    monkeypatch.setattr(
        "sneeze.slackbot.time.sleep",
        lambda delay: sleeps.append(delay),
    )

    with slackbot_module.slack_api_request_policy(
        timeout_seconds=2.0,
        attempts=2,
        retry_delay_seconds=0.25,
        total_timeout_seconds=10.0,
    ):
        with pytest.raises(SlackbotError, match="connection failed"):
            slack_api_post(
                "xoxb-test",
                "chat.postMessage",
                {"text": "hi", "client_msg_id": "stable-message-id"},
            )

    assert timeouts == [2.0, 2.0]
    assert sleeps == [0.25]
    assert client_msg_ids == ["stable-message-id", "stable-message-id"]


def test_extract_codex_session_id_ignores_event_id():
    jsonl = "\n".join(
        [
            (
                '{"type":"session_item",'
                '"payload":{"id":"tool-call-not-session",'
                '"session_id":"wrong-session"}}'
            ),
            '{"type":"item","id":"event-not-session"}',
            ('{"type":"session_meta",' '"payload":{"id":"real-session-id"}}'),
        ]
    )

    assert extract_codex_session_id(jsonl) == "real-session-id"


def test_extract_codex_session_id_reads_thread_started_event():
    assert (
        extract_codex_session_id(
            '{"type":"thread.started","thread_id":"session-thread"}\n'
        )
        == "session-thread"
    )


def test_text_helpers_cover_mentions_and_chunk_boundary():
    assert strip_bot_mentions("<@U123> hello <@U456>") == "hello"
    assert strip_bot_mentions("<@BOT> ask <@U456>", "BOT") == "ask <@U456>"
    assert strip_bot_mentions("<@BOT>, please", "BOT") == "please"
    assert (
        strip_bot_mentions("<@BOT>\n    def f():\n        pass", "BOT")
        == "    def f():\n        pass"
    )
    assert strip_bot_mentions("hello") == "hello"
    assert chunk_text("abcd", max_chars=2) == ["ab", "cd"]
    assert chunk_text("alpha beta gamma", max_chars=10) == [
        "alpha beta",
        "gamma",
    ]
    assert chunk_text("  hello", max_chars=5) == ["hello"]


def test_agent_tmux_binding_store(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.23",
        host="dgx",
        tmux_session="codex-work",
        tmux_socket="codex",
        tmux_conf=str(tmp_path / "codex.tmux.conf"),
        tmux_pane="%7",
        tmux_pane_pid="101",
        tmux_session_created="202",
        tmux_server_pid="303",
    )
    binding = query_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.23",
    )

    assert binding["host"] == "dgx"
    assert binding["tmux_session"] == "codex-work"
    assert binding["tmux_socket"] == "codex"
    assert binding["tmux_pane"] == "%7"
    assert binding["tmux_pane_pid"] == "101"
    assert binding["tmux_session_created"] == "202"
    assert binding["tmux_server_pid"] == "303"
    assert binding["tmux_conf"] == str(tmp_path / "codex.tmux.conf")


def test_tmux_binding_uses_absolute_socket_path(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    socket_path = str(tmp_path / "tmux.sock")

    command, tmux_socket, _ = tmux_command_for_binding(
        config,
        {
            "host": "localhost",
            "tmux_session": "codex-work",
            "tmux_socket": socket_path,
        },
    )

    assert command[1:3] == ["-S", socket_path]
    assert tmux_socket == socket_path


def test_tmux_binding_explicit_session_uses_active_socket(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    socket_path = str(tmp_path / "active-tmux.sock")
    monkeypatch.setenv("WEBMUX_AGENT_SESSION", "codex-work")
    monkeypatch.setenv("TMUX", f"{socket_path},123,0")
    monkeypatch.setenv("TMUX_PANE", "%7")
    monkeypatch.delenv("CODEX_TMUX_SOCKET", raising=False)
    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        lambda tmux_bin, pane_id, **kwargs: {
            "tmux_session": "codex-work",
            "tmux_pane": pane_id,
            "tmux_pane_pid": "101",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        },
    )

    binding = resolve_codex_agent_tmux_binding(config)

    assert binding["tmux_session"] == "codex-work"
    assert binding["tmux_socket"] == socket_path
    assert binding["tmux_pane"] == "%7"
    assert binding["tmux_pane_pid"] == "101"
    assert binding["tmux_session_created"] == "202"
    assert binding["tmux_server_pid"] == "303"
    assert "tmux_conf" not in binding


def test_tmux_binding_rejects_invalid_supplied_pane(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    monkeypatch.setenv("WEBMUX_AGENT_SESSION", "codex-work")
    monkeypatch.setenv("TMUX_PANE", "%7")
    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        lambda tmux_bin, pane_id, **kwargs: {
            "tmux_session": "different-session",
            "tmux_pane": pane_id,
        },
    )

    binding = resolve_codex_agent_tmux_binding(config)

    assert binding is None


def test_tmux_binding_rejects_reused_pane_identity(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    binding = {
        "host": "localhost",
        "tmux_session": "codex-work",
        "tmux_socket": "codex",
        "tmux_pane": "%7",
        "tmux_pane_pid": "101",
        "tmux_session_created": "202",
        "tmux_server_pid": "303",
    }
    monkeypatch.setattr(
        "sneeze.slackbot._tmux_pane_identity",
        lambda *args, **kwargs: {
            "tmux_session": "codex-work",
            "tmux_pane": "%7",
            "tmux_pane_pid": "404",
            "tmux_session_created": "505",
            "tmux_server_pid": "606",
        },
    )

    with pytest.raises(SlackbotError, match="identity changed"):
        slackbot_module.agent_tmux_target(config, binding)


def test_tmux_binding_resolves_pane_from_tmux_socket_env(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    socket_path = str(tmp_path / "tmux.sock")
    monkeypatch.setenv("TMUX_PANE", "%7")
    monkeypatch.setenv("TMUX", f"{socket_path},123,0")
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if args[1:3] == ["-S", socket_path]:
            return SimpleNamespace(
                returncode=0,
                stdout="codex-work\n",
                stderr="",
            )
        if args[1:3] == ["-L", "codex"]:
            return SimpleNamespace(
                returncode=0,
                stdout="wrong-session\n",
                stderr="",
            )
        return SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    binding = resolve_codex_agent_tmux_binding(config)

    assert binding["tmux_session"] == "codex-work"
    assert binding["tmux_socket"] == socket_path
    assert binding["tmux_pane"] == "%7"
    assert calls[0][1:3] == ["-S", socket_path]


def test_codex_stop_hook_creates_reuses_and_dedupes_dm_thread(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        policy = slackbot_module.SLACK_API_REQUEST_POLICY.get()
        assert policy["timeout_seconds"] == 2.0
        assert policy["attempts"] == 2
        assert policy["retry_delay_seconds"] == 1.0
        assert policy["deadline_monotonic"] > time.monotonic()
        calls.append((route, text))
        if len(calls) == 1:
            assert route.dm_user_id == "U111"
            assert route.thread_ts is None
            return {"channel": "D111", "ts": "1.000001"}
        assert route.channel_id == "D111"
        assert route.thread_ts == "1.000001"
        return {"channel": "D111", "ts": f"{len(calls)}.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "cwd": str(tmp_path),
        "hook_event_name": "Stop",
        "last_assistant_message": "Ready for review.",
    }
    result = notify_codex_agent_stop(
        profile,
        json.dumps(payload),
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    Path(load_config(profile).paths.agent_tmux_bindings_path).unlink()
    duplicate = notify_codex_agent_stop(
        profile,
        json.dumps(payload),
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    payload["turn_id"] = "turn-2"
    payload["last_assistant_message"] = "Second stop."
    second = notify_codex_agent_stop(
        profile,
        json.dumps(payload),
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    payload.pop("session_id")
    payload["turn_id"] = "turn-3"
    payload["last_assistant_message"] = "Fallback stop."
    fallback = notify_codex_agent_stop(
        profile,
        json.dumps(payload),
        tmux_session="codex-work",
        tmux_socket="codex",
    )

    assert result["status"] == "notified"
    assert duplicate["status"] == "skipped"
    assert second["thread_ts"] == "1.000001"
    assert fallback["key"] == "session:codex-session-1"
    assert len(calls) == 3
    assert "Reply in this thread" in calls[0][1]
    assert "Second stop." in calls[1][1]
    binding = query_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
    )
    assert binding["tmux_session"] == "codex-work"
    status = query_codex_agent_threads(profile)
    entry = status["hook_state"]["sessions"]["session:codex-session-1"]
    assert entry["channel_id"] == "D111"
    assert entry["session_id"] == "codex-session-1"
    assert len(entry["notifications"]) == 3


def test_codex_stop_hook_posts_long_notification_once(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_slack_api_post(token, method, payload):
        if method == "conversations.open":
            return {"ok": True, "channel": {"id": "D111"}}
        calls.append(payload)
        if len(calls) > 1:
            raise SlackbotError("a second chunk must not be posted")
        return {"ok": True, "channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr(
        "sneeze.slackbot.slack_api_post",
        fake_slack_api_post,
    )

    result = notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "session_id": "codex-session-long",
                "turn_id": "turn-1",
                "last_assistant_message": "x" * 8000,
            }
        ),
    )

    assert result["status"] == "notified"
    assert len(calls) == 1
    assert calls[0]["mrkdwn"] is False
    assert calls[0]["client_msg_id"] == (
        slackbot_module._codex_hook_client_msg_id(
            key="session:codex-session-long",
            turn_id="turn-1",
            message_hash=slackbot_module._codex_hook_message_hash("x" * 8000),
            notify_user_id="U111",
            transcript_path=None,
        )
    )
    assert len(calls[0]["text"]) == slackbot_module.MAX_SLACK_TEXT_CHARS
    assert calls[0]["text"].endswith(
        "[Notification truncated to fit one Slack message.]"
    )


def test_codex_stop_hook_rejected_pane_clears_stale_binding(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )

    def fake_pane_identity(tmux_bin, pane_id, **kwargs):
        session = "codex-work" if pane_id == "%1" else "different-session"
        return {
            "tmux_session": session,
            "tmux_pane": pane_id,
            "tmux_pane_pid": "101",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        }

    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        fake_pane_identity,
    )
    calls = []

    def fake_post(config, route, text):
        calls.append(route)
        return {"channel": "D111", "ts": f"{len(calls)}.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "Initial stop.",
        "tmux_session": "codex-work",
        "tmux_socket": "codex",
        "tmux_pane": "%1",
    }
    notify_codex_agent_stop(profile, json.dumps(payload))
    assert (
        query_agent_tmux_thread(
            profile,
            channel_id="D111",
            thread_ts="1.000001",
        )["tmux_pane"]
        == "%1"
    )
    payload.update(
        {
            "turn_id": "turn-2",
            "last_assistant_message": "Replacement pane stop.",
            "tmux_pane": "%2",
        }
    )

    rejected = notify_codex_agent_stop(profile, json.dumps(payload))

    assert rejected["tmux_binding"] is None
    assert calls[1].channel_id == "D111"
    assert calls[1].thread_ts == "1.000001"
    assert (
        query_agent_tmux_thread(
            profile,
            channel_id="D111",
            thread_ts="1.000001",
        )
        is None
    )
    entry = query_codex_agent_threads(profile)["hook_state"]["sessions"][
        "session:codex-session-1"
    ]
    assert "tmux_session" not in entry
    assert "tmux_pane" not in entry
    assert entry.get("fallback_key") is None
    payload.pop("session_id")
    payload.update(
        {
            "turn_id": "turn-3",
            "last_assistant_message": "Old pane starts separately.",
            "tmux_pane": "%1",
        }
    )

    restarted = notify_codex_agent_stop(profile, json.dumps(payload))

    assert restarted["key"].startswith("tmux:")
    assert calls[2].dm_user_id == "U111"
    assert calls[2].thread_ts is None


def test_codex_stop_hook_transient_query_only_keeps_matching_binding(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    pane_available = True

    def fake_pane_identity(tmux_bin, pane_id, **kwargs):
        if not pane_available:
            return None
        return {
            "tmux_session": "codex-work",
            "tmux_pane": pane_id,
            "tmux_pane_pid": "101",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        }

    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        fake_pane_identity,
    )
    calls = []
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append(route)
        or {"channel": "D111", "ts": f"{len(calls)}.000001"},
    )
    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "Initial stop.",
        "tmux_session": "codex-work",
        "tmux_socket": "codex",
        "tmux_pane": "%1",
    }
    notify_codex_agent_stop(profile, json.dumps(payload))
    pane_available = False
    payload.update(
        {
            "turn_id": "turn-2",
            "last_assistant_message": "Transient query failure.",
        }
    )

    transient = notify_codex_agent_stop(profile, json.dumps(payload))

    assert transient["tmux_binding"]["tmux_pane"] == "%1"
    assert calls[1].channel_id == "D111"
    assert calls[1].thread_ts == "1.000001"
    binding = query_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
    )
    assert binding["tmux_pane"] == "%1"
    payload.update(
        {
            "turn_id": "turn-3",
            "last_assistant_message": "Different unavailable pane.",
            "tmux_pane": "%2",
        }
    )

    changed = notify_codex_agent_stop(profile, json.dumps(payload))

    assert changed["tmux_binding"] is None
    assert calls[2].channel_id == "D111"
    assert calls[2].thread_ts == "1.000001"
    assert (
        query_agent_tmux_thread(
            profile,
            channel_id="D111",
            thread_ts="1.000001",
        )
        is None
    )


def test_codex_stop_hook_transient_query_rejects_changed_active_socket(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    old_socket = str(tmp_path / "old.sock")
    new_socket = str(tmp_path / "new.sock")
    monkeypatch.setenv("TMUX", f"{old_socket},123,0")
    pane_available = True

    def fake_pane_identity(tmux_bin, pane_id, **kwargs):
        if not pane_available:
            return None
        return {
            "tmux_session": "codex-work",
            "tmux_pane": pane_id,
            "tmux_pane_pid": "101",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        }

    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        fake_pane_identity,
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {
            "channel": "D111",
            "ts": "1.000001",
        },
    )
    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "Initial stop.",
        "tmux_session": "codex-work",
        "tmux_pane": "%1",
    }
    notify_codex_agent_stop(profile, json.dumps(payload))
    pane_available = False
    monkeypatch.setenv("TMUX", f"{new_socket},456,0")
    payload.update(
        {
            "turn_id": "turn-2",
            "last_assistant_message": "Different socket stop.",
        }
    )

    changed = notify_codex_agent_stop(profile, json.dumps(payload))

    assert changed["tmux_binding"] is None
    assert (
        query_agent_tmux_thread(
            profile,
            channel_id="D111",
            thread_ts="1.000001",
        )
        is None
    )


def test_codex_stop_hook_bounds_dedupe_without_turn_id(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append(route)
        return {"channel": "D111", "ts": f"{len(calls)}.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "transcript_path": "/tmp/codex-session-1.jsonl",
            "last_assistant_message": "Waiting for input.",
        }
    )

    first = notify_codex_agent_stop(profile, payload)
    immediate = notify_codex_agent_stop(profile, payload)
    status = query_codex_agent_threads(profile)
    entry = status["hook_state"]["sessions"]["session:codex-session-1"]
    entry["notifications"][-1]["notified_at"] = "2020-01-01T00:00:00Z"
    Path(status["hook_state_path"]).write_text(
        json.dumps(status["hook_state"]),
        encoding="utf-8",
    )
    after_window = notify_codex_agent_stop(profile, payload)

    assert first["status"] == "notified"
    assert immediate["status"] == "skipped"
    assert after_window["status"] == "notified"
    assert len(calls) == 2
    assert calls[1].channel_id == "D111"
    assert calls[1].thread_ts == "1.000001"


def test_codex_stop_hook_starts_new_thread_when_recipient_changes(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        if route.dm_user_id == "U111":
            return {"channel": "D111", "ts": "1.000001"}
        if route.dm_user_id == "U222":
            return {"channel": "D222", "ts": "2.000001"}
        return {"channel": route.channel_id, "ts": "3.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "First recipient.",
    }
    notify_codex_agent_stop(
        profile,
        json.dumps(payload),
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace(
            "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111",
            "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U222",
        ),
        encoding="utf-8",
    )
    payload["turn_id"] = "turn-2"
    payload["last_assistant_message"] = "Second recipient."
    changed = notify_codex_agent_stop(profile, json.dumps(payload))
    payload["turn_id"] = "turn-3"
    payload["last_assistant_message"] = "Same second recipient."
    notify_codex_agent_stop(profile, json.dumps(payload))

    assert calls[0][0].dm_user_id == "U111"
    assert calls[1][0].dm_user_id == "U222"
    assert calls[1][0].thread_ts is None
    assert calls[2][0].channel_id == "D222"
    assert calls[2][0].thread_ts == "2.000001"
    assert changed["tmux_binding"]["tmux_session"] == "codex-work"
    state = query_codex_agent_threads(profile)["hook_state"]
    entry = state["sessions"]["session:codex-session-1"]
    assert entry["notify_user_id"] == "U222"
    assert entry["channel_id"] == "D222"
    assert len(entry["notifications"]) == 2
    binding = query_agent_tmux_thread(
        profile,
        channel_id="D222",
        thread_ts="2.000001",
    )
    assert binding["tmux_session"] == "codex-work"


def test_codex_stop_hook_keeps_distinct_sessions_on_same_tmux_separate(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append(route)
        index = len(calls)
        return {"channel": "D111", "ts": f"{index}.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    for session_id in ("codex-session-1", "codex-session-2"):
        notify_codex_agent_stop(
            profile,
            json.dumps(
                {
                    "session_id": session_id,
                    "turn_id": "turn-1",
                    "last_assistant_message": session_id,
                }
            ),
            tmux_session="codex-work",
            tmux_socket="codex",
        )

    status = query_codex_agent_threads(profile)
    sessions = status["hook_state"]["sessions"]
    sessions["session:codex-session-1"]["updated_at"] = "2026-07-01T10:00:00Z"
    sessions["session:codex-session-2"]["updated_at"] = "2026-07-01T10:01:00Z"
    Path(status["hook_state_path"]).write_text(
        json.dumps(status["hook_state"]),
        encoding="utf-8",
    )
    fallback = notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "turn_id": "turn-2",
                "last_assistant_message": "session-less stop",
            }
        ),
        tmux_session="codex-work",
        tmux_socket="codex",
    )

    assert len(calls) == 3
    assert all(route.dm_user_id == "U111" for route in calls[:2])
    assert all(route.thread_ts is None for route in calls[:2])
    assert calls[2].channel_id == "D111"
    assert calls[2].thread_ts == "2.000001"
    assert fallback["key"] == "session:codex-session-2"
    assert set(sessions) == {
        "session:codex-session-1",
        "session:codex-session-2",
    }


def test_codex_stop_hook_keeps_sessionless_tmux_panes_separate(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        lambda tmux_bin, pane_id, **kwargs: {
            "tmux_session": "codex-work",
            "tmux_pane": pane_id,
            "tmux_pane_pid": f"pid-{pane_id}",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        },
    )
    calls = []

    def fake_post(config, route, text):
        calls.append(route)
        return {"channel": f"D{len(calls)}", "ts": f"{len(calls)}.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    def hook_payload(pane, turn_id):
        return json.dumps(
            {
                "turn_id": turn_id,
                "last_assistant_message": f"Stop from {pane}",
                "tmux_session": "codex-work",
                "tmux_socket": "codex",
                "tmux_pane": pane,
            }
        )

    first = notify_codex_agent_stop(
        profile,
        hook_payload("%1", "turn-1"),
    )
    second = notify_codex_agent_stop(
        profile,
        hook_payload("%2", "turn-1"),
    )
    first_again = notify_codex_agent_stop(
        profile,
        hook_payload("%1", "turn-2"),
    )
    immediate_duplicate = notify_codex_agent_stop(
        profile,
        hook_payload("%1", "turn-2"),
    )
    status = query_codex_agent_threads(profile)
    entry = status["hook_state"]["sessions"][first["key"]]
    entry["notifications"][-1]["notified_at"] = "2020-01-01T00:00:00Z"
    Path(status["hook_state_path"]).write_text(
        json.dumps(status["hook_state"]),
        encoding="utf-8",
    )
    after_dedupe_window = notify_codex_agent_stop(
        profile,
        hook_payload("%1", "turn-2"),
    )

    assert first["key"] != second["key"]
    assert first["key"].endswith(":%1")
    assert second["key"].endswith(":%2")
    assert calls[0].dm_user_id == "U111"
    assert calls[1].dm_user_id == "U111"
    assert calls[2].channel_id == "D1"
    assert calls[2].thread_ts == "1.000001"
    assert first_again["key"] == first["key"]
    assert immediate_duplicate["status"] == "skipped"
    assert after_dedupe_window["status"] == "notified"
    assert len(calls) == 4
    sessions = query_codex_agent_threads(profile)["hook_state"]["sessions"]
    assert sessions[first["key"]]["tmux_pane"] == "%1"
    assert sessions[first["key"]]["tmux_pane_pid"] == "pid-%1"
    assert sessions[first["key"]]["tmux_session_created"] == "202"
    assert sessions[first["key"]]["tmux_server_pid"] == "303"
    assert sessions[second["key"]]["tmux_pane"] == "%2"


def test_codex_stop_hook_migrates_legacy_sessionless_pane_key(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        lambda tmux_bin, pane_id, **kwargs: {
            "tmux_session": "codex-work",
            "tmux_pane": pane_id,
            "tmux_pane_pid": "101",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        },
    )
    calls = []

    def fake_post(config, route, text):
        calls.append(route)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = {
        "turn_id": "turn-1",
        "last_assistant_message": "First pane stop.",
        "tmux_session": "codex-work",
        "tmux_socket": "codex",
        "tmux_pane": "%1",
    }
    first = notify_codex_agent_stop(profile, json.dumps(payload))
    status = query_codex_agent_threads(profile)
    entry = status["hook_state"]["sessions"].pop(first["key"])
    legacy_key = first["key"].rsplit(":", 1)[0]
    entry["key"] = legacy_key
    entry["fallback_key"] = legacy_key
    status["hook_state"]["sessions"][legacy_key] = entry
    Path(status["hook_state_path"]).write_text(
        json.dumps(status["hook_state"]),
        encoding="utf-8",
    )
    payload["turn_id"] = "turn-2"
    payload["last_assistant_message"] = "Second pane stop."

    migrated = notify_codex_agent_stop(profile, json.dumps(payload))

    assert migrated["key"] == first["key"]
    assert calls[1].channel_id == "D111"
    assert calls[1].thread_ts == "1.000001"
    sessions = query_codex_agent_threads(profile)["hook_state"]["sessions"]
    assert set(sessions) == {first["key"]}
    assert sessions[first["key"]]["fallback_key"] == first["key"]


def test_codex_stop_hook_suppresses_overlapping_duplicate(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        time.sleep(0.05)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                notify_codex_agent_stop,
                profile,
                payload,
                **kwargs,
            )
            for kwargs in (
                {
                    "tmux_session": "codex-work",
                    "tmux_socket": "codex",
                },
                {},
            )
        ]
        results = [future.result() for future in futures]

    assert len(calls) == 1
    assert sorted(result["status"] for result in results) == [
        "notified",
        "skipped",
    ]


def test_codex_stop_hook_serializes_session_and_fallback_aliases(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        time.sleep(0.05)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "Ready for review.",
    }
    session_payload = json.dumps(payload)
    payload.pop("session_id")
    fallback_payload = json.dumps(payload)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                notify_codex_agent_stop,
                profile,
                hook_input,
                tmux_session="codex-work",
                tmux_socket="codex",
            )
            for hook_input in (session_payload, fallback_payload)
        ]
        results = [future.result() for future in futures]

    assert len(calls) == 1
    assert sorted(result["status"] for result in results) == [
        "notified",
        "skipped",
    ]
    sessions = query_codex_agent_threads(profile)["hook_state"]["sessions"]
    assert set(sessions) == {"session:codex-session-1"}


def test_codex_stop_hook_serializes_persisted_aliases_without_binding(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        time.sleep(0.05)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    initial_payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "Initial stop.",
    }
    notify_codex_agent_stop(
        profile,
        json.dumps(initial_payload),
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    session_only_payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-2",
        "last_assistant_message": "Session-only stop.",
    }
    notify_codex_agent_stop(
        profile,
        json.dumps(session_only_payload),
    )
    status = query_codex_agent_threads(profile)
    entry = status["hook_state"]["sessions"]["session:codex-session-1"]
    assert entry["fallback_key"].startswith("tmux:")
    assert entry["tmux_session"] == "codex-work"

    next_payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-3",
        "last_assistant_message": "Ready for review.",
    }
    session_payload = json.dumps(next_payload)
    next_payload.pop("session_id")
    fallback_payload = json.dumps(next_payload)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                notify_codex_agent_stop,
                profile,
                hook_input,
                **kwargs,
            )
            for hook_input, kwargs in (
                (session_payload, {}),
                (
                    fallback_payload,
                    {
                        "tmux_session": "codex-work",
                        "tmux_socket": "codex",
                    },
                ),
            )
        ]
        results = [future.result() for future in futures]

    assert len(calls) == 3
    assert sorted(result["status"] for result in results) == [
        "notified",
        "skipped",
    ]
    sessions = query_codex_agent_threads(profile)["hook_state"]["sessions"]
    entry = sessions["session:codex-session-1"]
    assert len(entry["notifications"]) == 3


def test_codex_stop_hook_recovers_legacy_null_fallback_key(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "session_id": "codex-session-1",
                "turn_id": "turn-1",
                "last_assistant_message": "Initial stop.",
            }
        ),
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    status = query_codex_agent_threads(profile)
    entry = status["hook_state"]["sessions"]["session:codex-session-1"]
    entry["fallback_key"] = None
    Path(status["hook_state_path"]).write_text(
        json.dumps(status["hook_state"]),
        encoding="utf-8",
    )

    result = notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "turn_id": "turn-2",
                "last_assistant_message": "Legacy fallback stop.",
            }
        ),
        tmux_session="codex-work",
        tmux_socket="codex",
    )

    assert len(calls) == 2
    assert calls[1][0].channel_id == "D111"
    assert calls[1][0].thread_ts == "1.000001"
    assert result["key"] == "session:codex-session-1"
    sessions = query_codex_agent_threads(profile)["hook_state"]["sessions"]
    entry = sessions["session:codex-session-1"]
    assert entry["fallback_key"].startswith("tmux:")


def test_codex_stop_hook_busy_alias_does_not_block_unrelated_session(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    slow_post_started = threading.Event()
    release_slow_post = threading.Event()
    unrelated_posted = threading.Event()
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        if "Slow stop." in text:
            slow_post_started.set()
            release_slow_post.wait(timeout=2)
        if "Unrelated stop." in text:
            unrelated_posted.set()
        return {"channel": "D111", "ts": f"{len(calls)}.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    slow_payload = json.dumps(
        {
            "session_id": "codex-session-slow",
            "turn_id": "turn-1",
            "last_assistant_message": "Slow stop.",
        }
    )
    unrelated_payload = json.dumps(
        {
            "session_id": "codex-session-unrelated",
            "turn_id": "turn-1",
            "last_assistant_message": "Unrelated stop.",
        }
    )

    with ThreadPoolExecutor(max_workers=3) as executor:
        slow = executor.submit(notify_codex_agent_stop, profile, slow_payload)
        assert slow_post_started.wait(timeout=1)
        waiter = executor.submit(
            notify_codex_agent_stop, profile, slow_payload
        )
        time.sleep(0.05)
        unrelated = executor.submit(
            notify_codex_agent_stop,
            profile,
            unrelated_payload,
        )
        try:
            assert unrelated_posted.wait(timeout=1)
        finally:
            release_slow_post.set()
        results = [future.result() for future in (slow, waiter, unrelated)]

    assert len(calls) == 2
    assert sorted(result["status"] for result in results) == [
        "notified",
        "notified",
        "skipped",
    ]


def test_codex_stop_hook_times_out_waiting_for_busy_alias(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setattr(
        "sneeze.slackbot.AGENT_CODEX_HOOK_DELIVERY_LOCK_TIMEOUT_SECONDS",
        0.05,
    )
    slow_post_started = threading.Event()
    release_slow_post = threading.Event()

    def fake_post(config, route, text):
        slow_post_started.set()
        release_slow_post.wait(timeout=2)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = json.dumps(
        {
            "session_id": "codex-session-slow",
            "turn_id": "turn-1",
            "last_assistant_message": "Slow stop.",
        }
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        slow = executor.submit(notify_codex_agent_stop, profile, payload)
        assert slow_post_started.wait(timeout=1)
        try:
            with pytest.raises(SlackbotError, match="Timed out waiting"):
                notify_codex_agent_stop(profile, payload)
        finally:
            release_slow_post.set()
        assert slow.result()["status"] == "notified"


def test_codex_stop_hook_times_out_waiting_for_resolver_lock(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sneeze.slackbot.AGENT_CODEX_HOOK_DELIVERY_LOCK_TIMEOUT_SECONDS",
        0.05,
    )
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    resolver_lock_path = slackbot_module._codex_hook_delivery_lock_path(
        state_path,
        "internal:alias-resolution",
    )
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )

    with slackbot_module.locked_json_path(resolver_lock_path):
        with pytest.raises(SlackbotError, match="Timed out waiting"):
            notify_codex_agent_stop(profile, payload)


def test_codex_hook_delivery_locks_use_fixed_buckets(tmp_path):
    state_path = str(tmp_path / "agent-codex-hooks.json")
    lock_paths = {
        slackbot_module._codex_hook_delivery_lock_path(
            state_path,
            f"session:codex-session-{index}",
        )
        for index in range(2048)
    }
    resolver_path = slackbot_module._codex_hook_delivery_lock_path(
        state_path,
        "internal:alias-resolution",
    )

    assert len(lock_paths) <= (
        slackbot_module.AGENT_CODEX_HOOK_DELIVERY_LOCK_BUCKETS
    )
    assert resolver_path not in lock_paths


def test_codex_stop_hook_recovers_receipt_after_post_state_timeout(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sneeze.slackbot.AGENT_CODEX_HOOK_DELIVERY_LOCK_TIMEOUT_SECONDS",
        0.05,
    )
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    start_holder = threading.Event()
    lock_held = threading.Event()
    release_holder = threading.Event()
    calls = []

    def hold_state_lock():
        start_holder.wait(timeout=1)
        with slackbot_module.locked_agent_tmux_json_path(state_path):
            lock_held.set()
            release_holder.wait(timeout=2)

    holder = threading.Thread(target=hold_state_lock)
    holder.start()

    def fake_post(config, route, text):
        calls.append((route, text))
        start_holder.set()
        assert lock_held.wait(timeout=1)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )

    try:
        with pytest.raises(
            slackbot_module.CodexHookRetryableError,
            match="Slackbot state lock",
        ):
            notify_codex_agent_stop(profile, payload)
        receipt_paths = list(Path(state_path).parent.rglob("*.receipt.json"))
        assert receipt_paths
    finally:
        release_holder.set()
        holder.join(timeout=1)

    retry = notify_codex_agent_stop(profile, payload)

    assert retry["status"] == "skipped"
    assert retry["reason"] == "delivery_receipt"
    assert len(calls) == 1
    entry = query_codex_agent_threads(profile)["hook_state"]["sessions"][
        "session:codex-session-1"
    ]
    assert len(entry["notifications"]) == 1
    assert not list(Path(state_path).parent.rglob("*.receipt.json"))


def test_codex_stop_hook_recovers_legacy_receipt_identity(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    message = "Ready for review."
    message_hash = slackbot_module._codex_hook_message_hash(message)
    legacy_receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {"session:codex-session-1"},
        turn_id="turn-1",
        message_hash=message_hash,
        notify_user_id="U111",
        transcript_path="/tmp/transcript.jsonl",
        legacy_identity=True,
        legacy_location=True,
    )[0]
    slackbot_module.write_json_atomic(
        legacy_receipt_path,
        {
            "version": 1,
            "key": "session:codex-session-1",
            "fallback_key": None,
            "notify_user_id": "U111",
            "channel_id": "D111",
            "thread_ts": "1.000001",
            "turn_id": "turn-1",
            "message_sha256": message_hash,
            "tmux_binding": None,
            "notified_at": slackbot_module.utcnow_iso(),
        },
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("legacy receipt should prevent a second post")
        ),
    )

    result = notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "session_id": "codex-session-1",
                "turn_id": "turn-1",
                "transcript_path": "/tmp/transcript.jsonl",
                "last_assistant_message": message,
            }
        ),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "delivery_receipt"
    entry = query_codex_agent_threads(profile)["hook_state"]["sessions"][
        "session:codex-session-1"
    ]
    assert entry["channel_id"] == "D111"
    assert not Path(legacy_receipt_path).exists()


def test_codex_hook_receipt_dedupe_without_turn_id_expires(tmp_path):
    state_path = str(tmp_path / "agent-codex-hooks.json")
    message_hash = "message-hash"
    receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {"session:codex-session-1"},
        turn_id=None,
        message_hash=message_hash,
        notify_user_id="U111",
        transcript_path="/tmp/codex-session-1.jsonl",
    )[0]
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        "notify_user_id": "U111",
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "turn_id": None,
        "message_sha256": message_hash,
        "transcript_path": "/tmp/codex-session-1.jsonl",
        "notified_at": "2020-01-01T00:00:00Z",
    }
    slackbot_module.write_json_atomic(receipt_path, receipt)

    expired = slackbot_module._codex_hook_delivery_receipt(
        [receipt_path],
        desired_key="session:codex-session-1",
        turn_id=None,
        message_hash=message_hash,
        notify_user_id="U111",
        transcript_path="/tmp/codex-session-1.jsonl",
    )
    receipt["notified_at"] = slackbot_module.utcnow_iso()
    slackbot_module.write_json_atomic(receipt_path, receipt)
    fresh = slackbot_module._codex_hook_delivery_receipt(
        [receipt_path],
        desired_key="session:codex-session-1",
        turn_id=None,
        message_hash=message_hash,
        notify_user_id="U111",
        transcript_path="/tmp/codex-session-1.jsonl",
    )

    assert expired is None
    assert fresh == receipt


def test_codex_hook_migrates_legacy_receipt_among_unrelated_state_files(
    tmp_path,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    legacy_receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {"session:codex-session-1"},
        turn_id="turn-1",
        message_hash="message-hash",
        notify_user_id="U111",
        transcript_path=None,
        legacy_location=True,
    )[0]
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        "notify_user_id": "U111",
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "turn_id": "turn-1",
        "message_sha256": "message-hash",
        "tmux_binding": None,
        "notified_at": slackbot_module.utcnow_iso(),
    }
    slackbot_module.write_json_atomic(legacy_receipt_path, receipt)
    state_dir = Path(state_path).parent
    for index in range(300):
        (state_dir / f"unrelated-{index:03d}.lock").touch()

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        channel_id="D111",
        thread_ts="1.000001",
        notify_user_id="U111",
    )

    assert recovered == receipt
    route_receipt_path = (
        slackbot_module._codex_hook_delivery_route_receipt_path(
            state_path,
            channel_id="D111",
            thread_ts="1.000001",
            notify_user_id="U111",
        )
    )
    assert Path(route_receipt_path).exists()


def test_codex_hook_rechecks_legacy_receipts_after_state_dir_changes(
    tmp_path,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    route = {
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "notify_user_id": "U111",
    }
    assert (
        slackbot_module.codex_hook_delivery_receipt_for_route(
            config.paths,
            **route,
        )
        is None
    )
    legacy_receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {"session:codex-session-1"},
        turn_id="turn-1",
        message_hash="message-hash",
        notify_user_id="U111",
        transcript_path=None,
        legacy_location=True,
    )[0]
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        **route,
        "turn_id": "turn-1",
        "message_sha256": "message-hash",
        "tmux_binding": None,
        "notified_at": slackbot_module.utcnow_iso(),
    }
    slackbot_module.write_json_atomic(legacy_receipt_path, receipt)

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        **route,
    )

    assert recovered == receipt


def test_codex_hook_does_not_route_or_delete_stale_alias_receipts(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    route = {
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "notify_user_id": "U111",
    }
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        **route,
        "turn_id": "turn-1",
        "message_sha256": "message-hash",
        "tmux_binding": None,
        "notified_at": slackbot_module.utcnow_iso(),
    }
    receipt_paths = []
    for legacy_location in (False, True):
        receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
            state_path,
            {"session:codex-session-1"},
            turn_id="turn-1",
            message_hash="message-hash",
            notify_user_id="U111",
            transcript_path=None,
            legacy_location=legacy_location,
        )[0]
        slackbot_module.write_json_atomic(receipt_path, receipt)
        old_timestamp = (
            time.time()
            - slackbot_module.AGENT_CODEX_HOOK_LEGACY_ROUTE_MAX_AGE_SECONDS
            - 1
        )
        os.utime(receipt_path, (old_timestamp, old_timestamp))
        receipt_paths.append(receipt_path)

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        **route,
    )

    assert recovered is None
    assert all(Path(path).exists() for path in receipt_paths)


def test_codex_hook_indexes_every_fresh_pending_alias_receipt(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    receipts = []
    for index in range(300):
        route = {
            "channel_id": f"D{index:03d}",
            "thread_ts": f"{index}.000001",
            "notify_user_id": "U111",
        }
        receipt = {
            "version": 1,
            "key": f"session:codex-session-{index}",
            "fallback_key": None,
            **route,
            "turn_id": "turn-1",
            "message_sha256": "message-hash",
            "tmux_binding": None,
            "notified_at": slackbot_module.utcnow_iso(),
        }
        receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
            state_path,
            {receipt["key"]},
            turn_id="turn-1",
            message_hash="message-hash",
            notify_user_id="U111",
            transcript_path=None,
        )[0]
        slackbot_module.write_json_atomic(receipt_path, receipt)
        receipts.append(receipt)

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        channel_id=receipts[-1]["channel_id"],
        thread_ts=receipts[-1]["thread_ts"],
        notify_user_id="U111",
    )

    assert recovered == receipts[-1]
    first_route_path = (
        slackbot_module._codex_hook_delivery_route_receipt_path(
            state_path,
            channel_id=receipts[0]["channel_id"],
            thread_ts=receipts[0]["thread_ts"],
            notify_user_id="U111",
        )
    )
    assert Path(first_route_path).exists()
    Path(first_route_path).unlink()

    recovered_after_route_delete = (
        slackbot_module.codex_hook_delivery_receipt_for_route(
            config.paths,
            channel_id=receipts[0]["channel_id"],
            thread_ts=receipts[0]["thread_ts"],
            notify_user_id="U111",
        )
    )

    assert recovered_after_route_delete == receipts[0]
    assert Path(first_route_path).exists()


def test_codex_hook_indexes_newest_pending_receipt_for_each_route(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    route = {
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "notify_user_id": "U111",
    }
    receipts = []
    now = time.time()
    for index, modified_at in enumerate((now - 10, now), start=1):
        receipt = {
            "version": 1,
            "key": f"session:codex-session-{index}",
            "fallback_key": None,
            **route,
            "turn_id": f"turn-{index}",
            "message_sha256": f"message-hash-{index}",
            "tmux_binding": None,
            "notified_at": slackbot_module.utcnow_iso(),
        }
        receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
            state_path,
            {receipt["key"]},
            turn_id=receipt["turn_id"],
            message_hash=receipt["message_sha256"],
            notify_user_id="U111",
            transcript_path=None,
        )[0]
        slackbot_module.write_json_atomic(receipt_path, receipt)
        os.utime(receipt_path, (modified_at, modified_at))
        receipts.append(receipt)

    first_lookup = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        **route,
    )
    second_lookup = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        **route,
    )

    assert first_lookup == receipts[-1]
    assert second_lookup == receipts[-1]


def test_codex_hook_recovers_alias_when_route_index_writes_fail(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "notify_user_id": "U111",
        "turn_id": "turn-1",
        "message_sha256": "message-hash",
        "tmux_binding": None,
        "notified_at": slackbot_module.utcnow_iso(),
    }
    receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {receipt["key"]},
        turn_id="turn-1",
        message_hash="message-hash",
        notify_user_id="U111",
        transcript_path=None,
    )[0]
    slackbot_module.write_json_atomic(receipt_path, receipt)
    monkeypatch.setattr(
        "sneeze.slackbot._index_codex_hook_receipt_route",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("route directory is read-only")
        ),
    )

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        channel_id="D111",
        thread_ts="1.000001",
        notify_user_id="U111",
    )

    assert recovered == receipt


def test_codex_hook_rebuilds_preupgrade_pending_alias_marker(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "notify_user_id": "U111",
        "turn_id": "turn-1",
        "message_sha256": "message-hash",
        "tmux_binding": None,
        "notified_at": slackbot_module.utcnow_iso(),
    }
    receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {receipt["key"]},
        turn_id="turn-1",
        message_hash="message-hash",
        notify_user_id="U111",
        transcript_path=None,
    )[0]
    slackbot_module.write_json_atomic(receipt_path, receipt)
    alias_dir = slackbot_module._codex_hook_delivery_alias_receipt_dir(
        state_path
    )
    marker_path = (
        slackbot_module._codex_hook_delivery_receipt_root(state_path)
        / "pending-alias-route-index-v1.json"
    )
    slackbot_module.write_json_atomic(
        marker_path,
        {
            "version": 1,
            "alias_dir_mtime_ns": alias_dir.stat().st_mtime_ns,
            "indexed_at": slackbot_module.utcnow_iso(),
        },
    )

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        channel_id="D111",
        thread_ts="1.000001",
        notify_user_id="U111",
    )

    assert recovered == receipt
    marker = slackbot_module.read_json(marker_path, {})
    assert marker["version"] == 2
    assert marker["routes"]


def test_codex_hook_rebuilds_invalid_pending_alias_mapping(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    route = {
        "channel_id": "D111",
        "thread_ts": "1.000001",
        "notify_user_id": "U111",
    }
    receipt = {
        "version": 1,
        "key": "session:codex-session-1",
        "fallback_key": None,
        **route,
        "turn_id": "turn-1",
        "message_sha256": "message-hash",
        "tmux_binding": None,
        "notified_at": slackbot_module.utcnow_iso(),
    }
    receipt_path = slackbot_module._codex_hook_delivery_receipt_paths(
        state_path,
        {receipt["key"]},
        turn_id="turn-1",
        message_hash="message-hash",
        notify_user_id="U111",
        transcript_path=None,
    )[0]
    slackbot_module.write_json_atomic(receipt_path, receipt)
    alias_dir = slackbot_module._codex_hook_delivery_alias_receipt_dir(
        state_path
    )
    marker_path = (
        slackbot_module._codex_hook_delivery_receipt_root(state_path)
        / "pending-alias-route-index-v1.json"
    )
    route_name = Path(
        slackbot_module._codex_hook_delivery_route_receipt_path(
            state_path,
            **route,
        )
    ).name
    slackbot_module.write_json_atomic(
        marker_path,
        {
            "version": 2,
            "alias_dir_mtime_ns": alias_dir.stat().st_mtime_ns,
            "routes": {route_name: "missing.receipt.json"},
            "indexed_at": slackbot_module.utcnow_iso(),
        },
    )

    recovered = slackbot_module.codex_hook_delivery_receipt_for_route(
        config.paths,
        **route,
    )

    assert recovered == receipt
    marker = slackbot_module.read_json(marker_path, {})
    assert marker["routes"][route_name] == Path(receipt_path).name


def test_codex_stop_hook_wraps_post_receipt_state_io_failure(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        return {"channel": "D111", "ts": "1.000001"}

    original_write_json = slackbot_module.write_json_atomic

    def fail_state_write(path, data, *args, **kwargs):
        if str(path) == state_path:
            raise OSError("state disk error")
        return original_write_json(path, data, *args, **kwargs)

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    monkeypatch.setattr(
        "sneeze.slackbot.write_json_atomic",
        fail_state_write,
    )
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )

    with pytest.raises(
        slackbot_module.CodexHookRetryableError,
        match="state disk error",
    ):
        notify_codex_agent_stop(profile, payload)

    route_receipt_path = (
        slackbot_module._codex_hook_delivery_route_receipt_path(
            state_path,
            channel_id="D111",
            thread_ts="1.000001",
            notify_user_id="U111",
        )
    )
    Path(route_receipt_path).unlink()
    route_posts = []
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: route_posts.append(text),
    )
    bot = SlackSocketBot(load_config(profile))
    monkeypatch.setattr(
        bot,
        "_run_codex_for_route",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("receipt-backed reply should not start Codex")
        ),
    )
    state_dir = Path(state_path).parent
    original_iterdir = Path.iterdir

    def reject_state_dir_scan(path):
        if path == state_dir:
            raise AssertionError(
                "receipt routing must not scan the state directory"
            )
        return original_iterdir(path)

    monkeypatch.setattr(
        Path,
        "iterdir",
        reject_state_dir_scan,
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
    assert route_posts == [
        "No tmux pane is bound to this notification thread. "
        "Replies are read-only."
    ]
    assert Path(route_receipt_path).exists()
    monkeypatch.setattr(Path, "iterdir", original_iterdir)

    monkeypatch.setattr(
        "sneeze.slackbot.write_json_atomic",
        original_write_json,
    )
    retry = notify_codex_agent_stop(profile, payload)

    assert retry["status"] == "skipped"
    assert retry["reason"] == "delivery_receipt"
    assert len(calls) == 1


def test_codex_stop_hook_rejects_mismatched_receipt_binding(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    pane_available = True

    def fake_pane_identity(tmux_bin, pane_id, **kwargs):
        if not pane_available:
            return None
        return {
            "tmux_session": "codex-work",
            "tmux_pane": pane_id,
            "tmux_pane_pid": "101",
            "tmux_session_created": "202",
            "tmux_server_pid": "303",
        }

    monkeypatch.setattr(
        "sneeze.slackbot._resolve_tmux_pane_identity",
        fake_pane_identity,
    )
    calls = []
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append(route)
        or {"channel": "D111", "ts": "1.000001"},
    )
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    original_write_json = slackbot_module.write_json_atomic

    def fail_state_write(path, data, *args, **kwargs):
        if str(path) == state_path:
            raise OSError("state disk error")
        return original_write_json(path, data, *args, **kwargs)

    monkeypatch.setattr(
        "sneeze.slackbot.write_json_atomic",
        fail_state_write,
    )
    payload = {
        "session_id": "codex-session-1",
        "turn_id": "turn-1",
        "last_assistant_message": "Ready for review.",
        "tmux_session": "codex-work",
        "tmux_socket": "codex",
        "tmux_pane": "%1",
    }
    with pytest.raises(
        slackbot_module.CodexHookRetryableError,
        match="state disk error",
    ):
        notify_codex_agent_stop(profile, json.dumps(payload))

    monkeypatch.setattr(
        "sneeze.slackbot.write_json_atomic",
        original_write_json,
    )
    pane_available = False
    payload["tmux_pane"] = "%2"

    recovered = notify_codex_agent_stop(profile, json.dumps(payload))

    assert recovered["status"] == "skipped"
    assert recovered["reason"] == "delivery_receipt"
    assert recovered["tmux_binding"] is None
    assert len(calls) == 1
    assert (
        query_agent_tmux_thread(
            profile,
            channel_id="D111",
            thread_ts="1.000001",
        )
        is None
    )
    entry = query_codex_agent_threads(profile)["hook_state"]["sessions"][
        "session:codex-session-1"
    ]
    assert entry.get("fallback_key") is None
    assert "tmux_pane" not in entry


def test_codex_stop_hook_wraps_durable_receipt_cleanup_failure(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        return {"channel": "D111", "ts": "1.000001"}

    original_unlink = Path.unlink

    def fail_receipt_unlink(path, *args, **kwargs):
        if path.name.endswith(".receipt.json"):
            raise OSError("cleanup disk error")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    monkeypatch.setattr(Path, "unlink", fail_receipt_unlink)
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )

    with pytest.raises(
        slackbot_module.CodexHookRetryableError,
        match="cleanup disk error",
    ):
        notify_codex_agent_stop(profile, payload)

    monkeypatch.setattr(Path, "unlink", original_unlink)
    retry = notify_codex_agent_stop(profile, payload)

    assert retry["status"] == "skipped"
    assert retry["reason"] == "duplicate"
    assert len(calls) == 1


def test_codex_stop_hook_wraps_duplicate_receipt_cleanup_failure(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )
    notify_codex_agent_stop(profile, payload)
    original_unlink = Path.unlink

    def fail_receipt_unlink(path, *args, **kwargs):
        if path.name.endswith(".receipt.json"):
            raise OSError("duplicate cleanup disk error")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_receipt_unlink)

    with pytest.raises(
        slackbot_module.CodexHookRetryableError,
        match="duplicate cleanup disk error",
    ):
        notify_codex_agent_stop(profile, payload)

    monkeypatch.setattr(Path, "unlink", original_unlink)
    retry = notify_codex_agent_stop(profile, payload)
    assert retry["status"] == "skipped"
    assert retry["reason"] == "duplicate"
    assert len(calls) == 1


def test_codex_stop_hook_receipt_does_not_cross_shared_tmux_sessions(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setattr(
        "sneeze.slackbot.AGENT_CODEX_HOOK_DELIVERY_LOCK_TIMEOUT_SECONDS",
        0.05,
    )
    state_path = slackbot_module.agent_codex_hook_state_path(config.paths)
    start_holder = threading.Event()
    lock_held = threading.Event()
    release_holder = threading.Event()
    calls = []

    def hold_state_lock():
        start_holder.wait(timeout=1)
        with slackbot_module.locked_agent_tmux_json_path(state_path):
            lock_held.set()
            release_holder.wait(timeout=2)

    holder = threading.Thread(target=hold_state_lock)
    holder.start()

    def fake_post(config, route, text):
        calls.append((route, text))
        if len(calls) == 1:
            start_holder.set()
            assert lock_held.wait(timeout=1)
        return {
            "channel": f"D{len(calls)}",
            "ts": f"{len(calls)}.000001",
        }

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    def payload(session_id):
        return json.dumps(
            {
                "session_id": session_id,
                "turn_id": "turn-1",
                "last_assistant_message": "Identical stop.",
            }
        )

    try:
        with pytest.raises(
            slackbot_module.CodexHookRetryableError,
            match="Slackbot state lock",
        ):
            notify_codex_agent_stop(
                profile,
                payload("codex-session-1"),
                tmux_session="codex-work",
                tmux_socket="codex",
            )
    finally:
        release_holder.set()
        holder.join(timeout=1)

    monkeypatch.setattr(
        "sneeze.slackbot.AGENT_CODEX_HOOK_DELIVERY_LOCK_TIMEOUT_SECONDS",
        1.0,
    )
    original_write_receipts = (
        slackbot_module._write_codex_hook_delivery_receipts
    )
    second_receipt_written = threading.Event()
    release_second_receipt = threading.Event()

    def blocking_write_receipts(receipt_paths, receipt):
        original_write_receipts(receipt_paths, receipt)
        if receipt["key"] == "session:codex-session-2":
            second_receipt_written.set()
            release_second_receipt.wait(timeout=2)

    monkeypatch.setattr(
        "sneeze.slackbot._write_codex_hook_delivery_receipts",
        blocking_write_receipts,
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        second_future = executor.submit(
            notify_codex_agent_stop,
            profile,
            payload("codex-session-2"),
            tmux_session="codex-work",
            tmux_socket="codex",
        )
        assert second_receipt_written.wait(timeout=1)
        first_retry_future = executor.submit(
            notify_codex_agent_stop,
            profile,
            payload("codex-session-1"),
        )
        time.sleep(0.05)
        assert not first_retry_future.done()
        release_second_receipt.set()
        second = second_future.result()
        first_retry = first_retry_future.result()

    assert second["status"] == "notified"
    assert first_retry["status"] == "skipped"
    assert first_retry["reason"] == "delivery_receipt"
    assert len(calls) == 2
    sessions = query_codex_agent_threads(profile)["hook_state"]["sessions"]
    assert set(sessions) == {
        "session:codex-session-1",
        "session:codex-session-2",
    }
    assert sessions["session:codex-session-1"]["channel_id"] == "D1"
    assert sessions["session:codex-session-2"]["channel_id"] == "D2"
    assert not list(Path(state_path).parent.rglob("*.receipt.json"))


def test_codex_stop_hook_recovers_binding_without_duplicate_post(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        return {"channel": "D111", "ts": "1.000001"}

    def fail_bind(*args, **kwargs):
        raise OSError("simulated binding failure")

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    monkeypatch.setattr("sneeze.slackbot.bind_agent_tmux_thread", fail_bind)
    payload = json.dumps(
        {
            "session_id": "codex-session-1",
            "turn_id": "turn-1",
            "last_assistant_message": "Ready for review.",
        }
    )

    with pytest.raises(
        slackbot_module.CodexHookRetryableError,
        match="simulated binding failure",
    ):
        notify_codex_agent_stop(
            profile,
            payload,
            tmux_session="codex-work",
            tmux_socket="codex",
        )

    state = query_codex_agent_threads(profile)["hook_state"]
    entry = state["sessions"]["session:codex-session-1"]
    assert entry["channel_id"] == "D111"
    assert len(entry["notifications"]) == 1

    monkeypatch.setattr(
        "sneeze.slackbot.bind_agent_tmux_thread",
        bind_agent_tmux_thread,
    )
    retry = notify_codex_agent_stop(
        profile,
        payload,
        tmux_session="codex-work",
        tmux_socket="codex",
    )

    assert retry["status"] == "skipped"
    assert len(calls) == 1
    binding = query_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
    )
    assert binding["tmux_session"] == "codex-work"


def test_codex_stop_hook_can_notify_without_tmux_binding(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = Path(load_config(profile).paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    calls = []

    def fake_post(config, route, text):
        calls.append((route, text))
        return {
            "channel": "D111",
            "ts": "1.000001",
        }

    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        fake_post,
    )

    result = notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "session_id": "codex-session-1",
                "turn_id": "turn-1",
                "last_assistant_message": "Ready for review.",
            }
        ),
    )

    assert result["status"] == "notified"
    assert result["tmux_binding"] is None
    assert "read-only" in calls[0][1]
    assert "Reply in this thread" not in calls[0][1]
    assert query_codex_agent_threads(profile)["agent_tmux_bindings"] == {}


def test_codex_agent_hooks_install_preserves_unrelated_hooks(tmp_path):
    hooks_path = tmp_path / "hooks.json"
    webmux_script = tmp_path / "webmux-agent-status.js"
    webmux_script.write_text("#!/usr/bin/env node\n", encoding="utf-8")
    hooks_path.write_text(
        json.dumps(
            {
                "hooks": {
                    "Stop": [
                        {
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python keep.py",
                                },
                                {
                                    "type": "command",
                                    "command": (
                                        "trentn-bot slackbot-agent-stop-hook"
                                    ),
                                },
                            ]
                        }
                    ],
                    "UserPromptSubmit": [
                        {
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": (
                                        "node old/webmux-agent-status.js "
                                        "--status working"
                                    ),
                                }
                            ]
                        }
                    ],
                    "PostToolUse": [
                        {
                            "matcher": "Bash",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": "python audit.py",
                                }
                            ],
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    hooks_path.chmod(0o644)

    result = install_codex_agent_hooks(
        hooks_path=str(hooks_path),
        slack_stop_command="trentn-bot slackbot-agent-stop-hook",
        webmux_script_path=str(webmux_script),
    )

    updated = json.loads(hooks_path.read_text(encoding="utf-8"))
    stop_commands = [
        handler["command"]
        for group in updated["hooks"]["Stop"]
        for handler in group["hooks"]
    ]
    prompt_commands = [
        handler["command"]
        for group in updated["hooks"]["UserPromptSubmit"]
        for handler in group["hooks"]
    ]
    post_tool_commands = [
        handler["command"]
        for group in updated["hooks"]["PostToolUse"]
        for handler in group["hooks"]
    ]
    stop_handlers = [
        handler
        for group in updated["hooks"]["Stop"]
        for handler in group["hooks"]
    ]
    prompt_handlers = [
        handler
        for group in updated["hooks"]["UserPromptSubmit"]
        for handler in group["hooks"]
    ]

    backup_path = Path(result["backup_path"])
    assert backup_path.exists()
    assert backup_path.stat().st_mode & 0o777 == 0o600
    assert "python keep.py" in stop_commands
    assert stop_commands.count("trentn-bot slackbot-agent-stop-hook") == 1
    assert any(str(webmux_script) in command for command in stop_commands)
    assert any(str(webmux_script) in command for command in prompt_commands)
    assert post_tool_commands == ["python audit.py"]
    assert result["installed"]["webmux"] is True
    assert (
        next(
            handler["timeout"]
            for handler in stop_handlers
            if handler["command"] == "trentn-bot slackbot-agent-stop-hook"
        )
        == 60
    )
    assert all(handler["timeout"] == 10 for handler in prompt_handlers)


def test_codex_agent_hooks_install_can_skip_webmux(tmp_path):
    hooks_path = tmp_path / "hooks.json"

    result = install_codex_agent_hooks(
        hooks_path=str(hooks_path),
        slack_stop_command="trentn-bot slackbot-agent-stop-hook",
        include_webmux=False,
    )

    updated = json.loads(hooks_path.read_text(encoding="utf-8"))
    stop_handlers = [
        handler
        for group in updated["hooks"]["Stop"]
        for handler in group["hooks"]
    ]
    assert updated["hooks"]["UserPromptSubmit"] == []
    assert stop_handlers == [
        {
            "type": "command",
            "command": "trentn-bot slackbot-agent-stop-hook",
            "timeout": 60,
            "statusMessage": "Notifying Slack",
        }
    ]
    assert result["installed"] == {"slack_stop": True, "webmux": False}


def test_codex_agent_hooks_install_warns_without_webmux_path(tmp_path):
    result = install_codex_agent_hooks(
        hooks_path=str(tmp_path / "hooks.json"),
        slack_stop_command="trentn-bot slackbot-agent-stop-hook",
        include_webmux=True,
    )

    assert result["installed"] == {"slack_stop": True, "webmux": False}
    assert result["warnings"] == [
        "WebMux hooks requested without a status script path"
    ]


def test_agent_hooks_install_command_builds_runtime_stop_command(tmp_path):
    import io

    from sneeze.slackbot_commands import SlackbotAgentHooksInstallBase

    class SampleAgentHooksInstall(SlackbotAgentHooksInstallBase):
        profile = make_profile(tmp_path)

    cli_bin = tmp_path / "sample-bot"
    cli_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    cli_bin.chmod(0o755)
    hooks_path = tmp_path / "hooks.json"
    runtime_root = tmp_path / "runtime-custom"
    env_path = runtime_root / "bot.env"
    state_dir = runtime_root / "state-custom"
    prompt_path = runtime_root / "prompt.md"
    command = SampleAgentHooksInstall(
        io.StringIO(),
        io.StringIO(),
        io.StringIO(),
    )
    command._cli_bin = str(cli_bin)
    command._hooks_path = str(hooks_path)
    command._no_webmux = True
    command._runtime_root = str(runtime_root)
    command._env_path = str(env_path)
    command._state_dir = str(state_dir)
    command._system_prompt_path = str(prompt_path)
    command._unit_name = "sample-custom.service"

    command.run()

    installed = json.loads(hooks_path.read_text(encoding="utf-8"))
    stop_handler = installed["hooks"]["Stop"][0]["hooks"][0]
    argv = shlex.split(stop_handler["command"])
    assert argv == [
        str(cli_bin),
        "slackbot-agent-stop-hook",
        f"--runtime-root={runtime_root}",
        f"--env-path={env_path}",
        f"--state-dir={state_dir}",
        f"--system-prompt-path={prompt_path}",
        "--unit-name=sample-custom.service",
    ]
    assert stop_handler["timeout"] == 60


def test_agent_stop_hook_command_retries_before_soft_failure(
    tmp_path,
    monkeypatch,
):
    import io

    from sneeze.slackbot_commands import SlackbotAgentStopHookBase

    class SampleAgentStopHook(SlackbotAgentStopHookBase):
        profile = make_profile(tmp_path)

    calls = []

    def fake_notify(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise slackbot_module.CodexHookRetryableError(
                "state finalization timed out"
            )
        return {"status": "skipped", "reason": "delivery_receipt"}

    monkeypatch.setattr(
        "sneeze.slackbot_commands.notify_codex_agent_stop",
        fake_notify,
    )
    estream = io.StringIO()
    command = SampleAgentStopHook(
        io.StringIO('{"session_id":"codex-session-1"}'),
        io.StringIO(),
        estream,
    )

    command.run()

    assert len(calls) == 2
    assert estream.getvalue() == ""


def test_agent_stop_hook_command_does_not_retry_unsafe_failure(
    tmp_path,
    monkeypatch,
):
    import io

    from sneeze.slackbot_commands import SlackbotAgentStopHookBase

    class SampleAgentStopHook(SlackbotAgentStopHookBase):
        profile = make_profile(tmp_path)

    calls = []

    def fail_notify(*args, **kwargs):
        calls.append((args, kwargs))
        raise SlackbotError("receipt write failed")

    monkeypatch.setattr(
        "sneeze.slackbot_commands.notify_codex_agent_stop",
        fail_notify,
    )
    estream = io.StringIO()
    command = SampleAgentStopHook(
        io.StringIO('{"session_id":"codex-session-1"}'),
        io.StringIO(),
        estream,
    )

    command.run()

    assert len(calls) == 1
    assert "receipt write failed" in estream.getvalue()


def test_agent_tmux_capture_bound_thread(tmp_path, monkeypatch):
    tmux_conf = tmp_path / "codex.tmux.conf"
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
        default_codex_tmux_conf=str(tmux_conf),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.23",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
        tmux_conf=str(tmux_conf),
        tmux_pane="%7",
    )
    config = load_config(profile)
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if "display-message" in args:
            return SimpleNamespace(
                returncode=0,
                stdout="codex-work\n",
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout="alpha\nbeta\n",
            stderr="",
        )

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    result = capture_agent_tmux_thread(
        config,
        channel_id="C123",
        thread_ts="1.23",
        line_count=5,
    )

    assert result["text"] == "alpha\nbeta"
    assert result["line_count"] == 5
    capture_call = next(call for call in calls if "capture-pane" in call)
    assert Path(capture_call[0]).name == "tmux"
    assert capture_call[1:] == [
        "-L",
        "codex",
        "-f",
        str(tmux_conf),
        "capture-pane",
        "-p",
        "-t",
        "%7",
        "-S",
        "-5",
    ]


def test_agent_tmux_paste_targets_bound_pane(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        if "display-message" in args:
            return SimpleNamespace(
                returncode=0,
                stdout="codex-work\n",
                stderr="",
            )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    paste_agent_tmux_text(
        config,
        {
            "host": "localhost",
            "tmux_session": "codex-work",
            "tmux_socket": "codex",
            "tmux_pane": "%7",
        },
        "literal $HOME; -- text\nsecond line",
    )

    assert calls[1][0][3:5] == ["load-buffer", "-b"]
    assert calls[1][1]["input"] == "literal $HOME; -- text\nsecond line"
    assert "-p" in calls[2][0]
    assert calls[2][0][-2:] == ["-t", "%7"]
    assert calls[3][0][-2:] == ["%7", "Enter"]


def test_agent_tmux_paste_refuses_remote_host(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    monkeypatch.setattr(
        "sneeze.slackbot.subprocess.run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("remote binding must not invoke tmux")
        ),
    )

    with pytest.raises(SlackbotError, match="remote tmux host"):
        paste_agent_tmux_text(
            config,
            {
                "host": "remote.example.com",
                "tmux_session": "codex-work",
                "tmux_socket": "codex",
            },
            "do not paste",
        )


def test_agent_tmux_rejects_reused_pane_from_another_session(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(
            returncode=0,
            stdout="different-session\n",
            stderr="",
        )

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    with pytest.raises(SlackbotError, match="no longer belongs"):
        paste_agent_tmux_text(
            config,
            {
                "host": "localhost",
                "tmux_session": "codex-work",
                "tmux_socket": "codex",
                "tmux_pane": "%7",
            },
            "do not paste",
        )

    assert len(calls) == 1
    assert "display-message" in calls[0]


def test_agent_tmux_reports_missing_bound_pane(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    monkeypatch.setattr(
        "sneeze.slackbot.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="can't find pane: %7",
        ),
    )

    with pytest.raises(SlackbotError, match="could not be found"):
        paste_agent_tmux_text(
            config,
            {
                "host": "localhost",
                "tmux_session": "codex-work",
                "tmux_socket": "codex",
                "tmux_pane": "%7",
            },
            "do not paste",
        )


def test_agent_tmux_sample_selects_bound_pane_cwd(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
        tmux_pane="%7",
    )
    config = load_config(profile)
    calls = []
    session_cwds = []

    def fake_run(args, **kwargs):
        calls.append(args)
        if "display-message" in args:
            return SimpleNamespace(
                returncode=0,
                stdout="codex-work\n",
                stderr="",
            )
        if "capture-pane" in args:
            return SimpleNamespace(
                returncode=0, stdout="Ready\n›\n", stderr=""
            )
        if "list-panes" in args:
            return SimpleNamespace(
                returncode=0,
                stdout="%8\t1\t/wrong\n%7\t0\t/right\n",
                stderr="",
            )
        raise AssertionError(args)

    def fake_latest(cwd):
        session_cwds.append(cwd)
        return None, None, None, None

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)
    monkeypatch.setattr(
        "sneeze.slackbot._agent_tmux_latest_codex_messages",
        fake_latest,
    )

    state = sample_agent_tmux_thread(
        config,
        channel_id="D111",
        thread_ts="1.000001",
    )

    assert state.pane_cwd == "/right"
    assert session_cwds == ["/right"]
    list_panes = next(call for call in calls if "list-panes" in call)
    assert list_panes[list_panes.index("-t") + 1] == "%7"


def test_systemd_service_has_no_source_project_leaks(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)
    from sneeze.slackbot import load_config

    config = load_config(profile, allow_missing_tokens=True)
    text = render_systemd_service(
        config,
        cli_bin="sample",
        run_subcommand="slackbot-run",
    )

    assert "sample Slack bot" in text
    assert "--runtime-root=" in text
    assert "--env-path=" in text
    assert "--state-dir=" in text
    assert "--system-prompt-path=" in text
    assert "--unit-name=sample-slackbot.service" in text
    assert f"EnvironmentFile={config.paths.env_path}" in text
    assert 'EnvironmentFile="' not in text
    assert not any(
        word in text for word in ("legacy-internal-tool", "ticket-system")
    )


def test_systemd_service_includes_profile_service_defaults(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        default_service_restart="always",
        default_service_restart_sec=5,
        default_systemd_wants=(
            "sample-mcp.service",
            "network-online.target",
        ),
        default_systemd_after=("sample-mcp.service",),
    )
    scaffold_runtime(profile)
    from sneeze.slackbot import load_config

    config = load_config(profile, allow_missing_tokens=True)
    text = render_systemd_service(
        config,
        cli_bin="sample",
        run_subcommand="slackbot-run",
        restart=profile.default_service_restart,
        restart_sec=profile.default_service_restart_sec,
    )

    assert "Wants=network-online.target sample-mcp.service" in text
    assert "After=network-online.target sample-mcp.service" in text
    assert "Restart=always" in text
    assert "RestartSec=5" in text


def test_slackbot_service_install_uses_profile_restart_defaults(
    tmp_path,
    monkeypatch,
):
    import io

    from sneeze.slackbot_commands import SlackbotServiceInstallBase

    service_profile = replace(
        make_profile(tmp_path),
        default_service_restart="always",
        default_service_restart_sec=5,
    )
    calls = {}

    def fake_install_service(profile_arg, **kwds):
        calls["profile"] = profile_arg
        calls.update(kwds)
        return {"ok": True}

    monkeypatch.setattr(
        "sneeze.slackbot_commands.install_service",
        fake_install_service,
    )

    class SampleSlackbotServiceInstall(SlackbotServiceInstallBase):
        profile = service_profile

    command = SampleSlackbotServiceInstall(
        sys.stdin,
        io.StringIO(),
        io.StringIO(),
    )
    command.cli_display_name = "slackbot-service-install"
    command.cli_bin = "/bin/echo"
    command.run()

    assert calls["profile"] == service_profile
    assert calls["restart"] == "always"
    assert calls["restart_sec"] == 5


def test_build_prompt_tells_codex_not_to_send_to_slack(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))

    text = bot._build_prompt(
        "Hi, you up?",
        SlackbotRoute(
            channel_id="D123",
            dm_user_id="U123",
            thread_ts="1.234",
        ),
        slack_context="[1.111111] U123: earlier message",
    )

    assert "The Slackbot wrapper will post" in text
    assert "Do not call Slack send" in text
    assert "Return the exact reply text as your final answer" in text
    assert "Slack route:" in text
    assert "Slack context:" in text
    assert "[1.111111] U123: earlier message" in text
    assert "Hi, you up?" in text


def test_launchd_service_includes_only_non_secret_env(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    monkeypatch.setattr("sneeze.slackbot.sys.platform", "darwin")
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    env_path = tmp_path / "runtime" / ".env"
    with env_path.open("a", encoding="utf-8") as handle:
        handle.write("SAMPLE_INTERNAL_API_KEY=secret\n")
    config = load_config(profile)

    text = render_launchd_service(
        config,
        cli_bin="sample",
        run_subcommand="slackbot-run",
    )

    assert "SAMPLE_MCP_SERVER_URL" in text
    assert "SAMPLE_INTERNAL_API_KEY" not in text
    assert "secret" not in text
    assert "xoxb-test" not in text
    assert "xapp-test" not in text


def test_post_slack_message_chunks_large_text(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload["text"]))
        return {"ok": True, "ts": str(len(calls))}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    post_slack_message(config, SlackbotRoute(channel_id="C123"), "x" * 3501)

    assert [item[0] for item in calls] == ["chat.postMessage"] * 2
    assert [len(item[1]) for item in calls] == [3500, 1]


def test_post_slack_message_renders_mentions(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append(payload["text"])
        return {"ok": True, "ts": "1.000001", "channel": "C123"}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    post_slack_message(
        config,
        SlackbotRoute(channel_id="C123", mention_user_ids=("U999",)),
        "hello",
    )

    assert calls == ["<@U999> hello"]


def test_post_slack_message_converts_markdown_bold_to_slack(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append(payload["text"])
        return {"ok": True, "ts": "1.000001", "channel": "C123"}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    post_slack_message(
        config,
        SlackbotRoute(channel_id="C123"),
        "**Summary**\n- `**literal**` stays literal\n```\n**code**\n```",
    )

    assert calls == [
        "*Summary*\n- `**literal**` stays literal\n```\n**code**\n```"
    ]


def test_update_slack_message_converts_markdown_bold_to_slack(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {"ok": True}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    update_slack_message(
        config,
        channel="C123",
        ts="1.000001",
        text="**Done**\n- `**literal**` stays literal",
    )

    assert calls == [
        (
            "chat.update",
            {
                "channel": "C123",
                "ts": "1.000001",
                "text": "*Done*\n- `**literal**` stays literal",
            },
        )
    ]


def test_render_slack_blocks_converts_mrkdwn_only():
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "**Summary**"},
            "fields": [
                {"type": "mrkdwn", "text": "__Status__: good"},
                {"type": "plain_text", "text": "**literal**"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": "`**literal**`"},
                {"type": "plain_text", "text": "__literal__"},
            ],
        },
    ]

    rendered = render_slack_blocks(blocks)

    assert rendered == [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Summary*"},
            "fields": [
                {"type": "mrkdwn", "text": "*Status*: good"},
                {"type": "plain_text", "text": "**literal**"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": "`**literal**`"},
                {"type": "plain_text", "text": "__literal__"},
            ],
        },
    ]
    assert blocks[0]["text"]["text"] == "**Summary**"


def test_post_slack_message_uses_response_url_fallback(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fail_post(config, route, text):
        raise SlackbotError("not in channel")

    def fake_response_url_post(response_url, text):
        calls.append((response_url, text))
        return {"ok": True}

    monkeypatch.setattr(
        "sneeze.slackbot._post_single_slack_message",
        fail_post,
    )
    monkeypatch.setattr(
        "sneeze.slackbot.slack_response_url_post",
        fake_response_url_post,
    )

    post_slack_message(
        config,
        SlackbotRoute(channel_id="C123", response_url="https://response"),
        "hello",
    )

    assert calls == [("https://response", "hello")]


def test_post_slack_blocks_preserves_blocks_with_response_url(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), bot_token=None)
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]
    calls = []

    def fake_response_url_post(response_url, text, *, blocks=None):
        calls.append((response_url, text, blocks))
        return {"ok": True}

    monkeypatch.setattr(
        "sneeze.slackbot.slack_response_url_post",
        fake_response_url_post,
    )

    post_slack_blocks(
        config,
        SlackbotRoute(response_url="https://response"),
        "hello",
        blocks,
    )

    assert calls == [("https://response", "hello", blocks)]


def test_post_slack_blocks_converts_mrkdwn_blocks_with_response_url(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), bot_token=None)
    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "**Summary**"},
        }
    ]
    calls = []

    def fake_response_url_post(response_url, text, *, blocks=None):
        calls.append((response_url, text, blocks))
        return {"ok": True}

    monkeypatch.setattr(
        "sneeze.slackbot.slack_response_url_post",
        fake_response_url_post,
    )

    post_slack_blocks(
        config,
        SlackbotRoute(response_url="https://response"),
        "__Fallback__",
        blocks,
    )

    assert calls == [
        (
            "https://response",
            "*Fallback*",
            [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Summary*"},
                }
            ],
        )
    ]


def test_post_slack_blocks_preserves_blocks_after_dm_open(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        if method == "conversations.open":
            return {"ok": True, "channel": {"id": "D123"}}
        return {"ok": True, "channel": "D123", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    post_slack_blocks(
        config,
        SlackbotRoute(dm_user_id="U111"),
        "hello",
        blocks,
    )

    assert calls[1][0] == "chat.postMessage"
    assert calls[1][1]["blocks"] == blocks


def test_post_slack_message_keeps_response_url_for_all_chunks(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fail_post(config, route, text):
        raise SlackbotError("not in channel")

    def fake_response_url_post(response_url, text):
        calls.append((response_url, len(text)))
        return {"ok": True}

    monkeypatch.setattr(
        "sneeze.slackbot._post_single_slack_message",
        fail_post,
    )
    monkeypatch.setattr(
        "sneeze.slackbot.slack_response_url_post",
        fake_response_url_post,
    )

    post_slack_message(
        config,
        SlackbotRoute(channel_id="C123", response_url="https://response"),
        "x" * 3501,
    )

    assert calls == [("https://response", 3500), ("https://response", 1)]


def test_codex_runner_resume_argv_orders_session_before_stdin(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), codex_bin="codex-test")
    monkeypatch.setenv("SAMPLE_SLACK_BOT_TOKEN", "from-os-env")
    monkeypatch.setenv("SLACK_APP_TOKEN", "from-os-env")
    monkeypatch.setenv("MCP_SERVER_URL", "http://ambient.invalid/mcp")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp-secret")
    monkeypatch.setenv("SAMPLE_INTERNAL_API_KEY", "secret")
    captured = []
    captured_env = []

    def fake_run(args, **kwds):
        captured.append(args)
        captured_env.append(kwds["env"])
        output_path = Path(args[args.index("-o") + 1])
        output_path.write_text("last", encoding="utf-8")
        return SimpleNamespace(
            returncode=0,
            stdout='{"type":"session_meta","payload":{"id":"next"}}\n',
            stderr="",
        )

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    result = CodexRunner(config).run("continue", session_id="prior")

    assert result["session_id"] == "next"
    assert captured[0][:3] == ["codex-test", "exec", "resume"]
    assert captured[0][-2:] == ["prior", "-"]
    assert "SAMPLE_SLACK_BOT_TOKEN" not in captured_env[0]
    assert "SAMPLE_SLACK_APP_TOKEN" not in captured_env[0]
    assert "SLACK_APP_TOKEN" not in captured_env[0]
    assert "GITHUB_TOKEN" not in captured_env[0]
    assert "SAMPLE_INTERNAL_API_KEY" not in captured_env[0]
    assert captured_env[0]["MCP_SERVER_URL"] == "http://localhost:8945/mcp"


def test_codex_runner_allows_explicit_user_secret_env(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), codex_bin="codex-test")
    monkeypatch.setenv("GITHUB_TOKEN", "ambient-secret")
    captured_env = []

    def fake_run(args, **kwds):
        captured_env.append(kwds["env"])
        output_path = Path(args[args.index("-o") + 1])
        output_path.write_text("last", encoding="utf-8")
        return SimpleNamespace(
            returncode=0,
            stdout='{"type":"session_meta","payload":{"id":"next"}}\n',
            stderr="",
        )

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)

    CodexRunner(config).run(
        "continue",
        extra_env={"GH_TOKEN": "user-secret", "GITHUB_TOKEN": "user-secret"},
    )

    assert captured_env[0]["GH_TOKEN"] == "user-secret"
    assert captured_env[0]["GITHUB_TOKEN"] == "user-secret"


def test_merge_child_env_scrubs_secret_names_before_user_env(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        child_env_scrub_allowlist=("ACME_API_KEY",),
        user_config_secret_fields=(
            GITHUB_SECRET_FIELD,
            SlackbotUserConfigSecretField(
                service_name="acme",
                label="Acme token",
                env_names=("ACME_TOKEN",),
            ),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    monkeypatch.setenv("ACME_API_KEY", "ambient-api-key")
    monkeypatch.setenv("AUTH", "ambient-auth-token")
    monkeypatch.setenv("AUTH_HEADER", "ambient-auth")
    monkeypatch.setenv("AUTH_TYPE", "ambient-auth-type")
    monkeypatch.setenv("BEARER_AUTH", "ambient-bearer")
    monkeypatch.setenv("BEARER_TIMEOUT", "ambient-timeout")
    monkeypatch.setenv("HTTP_AUTHORIZATION", "ambient-http-auth")
    monkeypatch.setenv("AUTHORIZATION_HEADER", "ambient-authz-header")
    monkeypatch.setenv("DOCKER_AUTH_CONFIG", "ambient-docker-auth")
    monkeypatch.setenv("BASIC_AUTH", "ambient-basic-auth")
    monkeypatch.setenv("DIGEST_AUTH", "ambient-digest-auth")
    monkeypatch.setenv("PROXY_AUTH", "ambient-proxy-auth")
    monkeypatch.setenv("API_AUTH", "ambient-api-auth")
    monkeypatch.setenv("WWW_AUTHENTICATE", "ambient-www-authenticate")
    monkeypatch.setenv("ACME_TOKEN", "ambient-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "ambient-gh")
    monkeypatch.setenv("GITHUBTOKEN", "ambient-glued-gh")
    monkeypatch.setenv("APITOKEN", "ambient-glued-api")
    monkeypatch.setenv("GitHubToken", "ambient-camel-token")
    monkeypatch.setenv("MY_API_KEYS", "ambient-keys")
    monkeypatch.setenv("TOKENIZER_LIMIT", "ambient-tokenizer-limit")
    monkeypatch.setenv("SECRETARY", "ambient-secretary")
    monkeypatch.setenv("PASSWORDS_COUNT", "ambient-passwords-count")
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/ssh-agent.sock")
    monkeypatch.setattr(
        "sneeze.slackbot.CHILD_ENV_PASSTHROUGH",
        {
            *CHILD_ENV_PASSTHROUGH,
            "ACME_API_KEY",
            "ACME_TOKEN",
            "AUTH",
            "AUTH_HEADER",
            "AUTH_TYPE",
            "BEARER_AUTH",
            "BEARER_TIMEOUT",
            "HTTP_AUTHORIZATION",
            "AUTHORIZATION_HEADER",
            "DOCKER_AUTH_CONFIG",
            "BASIC_AUTH",
            "DIGEST_AUTH",
            "PROXY_AUTH",
            "API_AUTH",
            "WWW_AUTHENTICATE",
            "GITHUB_TOKEN",
            "GITHUBTOKEN",
            "APITOKEN",
            "GitHubToken",
            "MY_API_KEYS",
            "TOKENIZER_LIMIT",
            "SECRETARY",
            "PASSWORDS_COUNT",
        },
    )

    scrubbed = merge_child_process_env(config, scrub_secret_env=True)
    system_scrubbed = merge_child_process_env(
        config,
        scrub_secret_env=True,
        use_scrub_allowlist=True,
    )
    injected = merge_child_process_env(
        config,
        {"ACME_TOKEN": "user-secret"},
        scrub_secret_env=True,
    )

    assert "ACME_API_KEY" not in scrubbed
    assert "AUTH" not in scrubbed
    assert "AUTH_HEADER" not in scrubbed
    assert scrubbed["AUTH_TYPE"] == "ambient-auth-type"
    assert "BEARER_AUTH" not in scrubbed
    assert scrubbed["BEARER_TIMEOUT"] == "ambient-timeout"
    assert "HTTP_AUTHORIZATION" not in scrubbed
    assert "AUTHORIZATION_HEADER" not in scrubbed
    assert "DOCKER_AUTH_CONFIG" not in scrubbed
    assert "BASIC_AUTH" not in scrubbed
    assert "DIGEST_AUTH" not in scrubbed
    assert "PROXY_AUTH" not in scrubbed
    assert "API_AUTH" not in scrubbed
    assert "WWW_AUTHENTICATE" not in scrubbed
    assert "GitHubToken" not in scrubbed
    assert "GITHUBTOKEN" not in scrubbed
    assert "APITOKEN" not in scrubbed
    assert "MY_API_KEYS" not in scrubbed
    assert scrubbed["TOKENIZER_LIMIT"] == "ambient-tokenizer-limit"
    assert scrubbed["SECRETARY"] == "ambient-secretary"
    assert scrubbed["PASSWORDS_COUNT"] == "ambient-passwords-count"
    assert scrubbed["SSH_AUTH_SOCK"] == "/tmp/ssh-agent.sock"
    assert system_scrubbed["ACME_API_KEY"] == "ambient-api-key"
    assert "ACME_TOKEN" not in scrubbed
    assert "GITHUB_TOKEN" not in scrubbed
    assert injected["ACME_TOKEN"] == "user-secret"


def test_dispatch_uses_socket_mode_envelope_type(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    bot.bot_user_id = "UBOT"
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._dispatch_payload(
        "events_api",
        {
            "type": "event_callback",
            "event_id": "E123",
            "event": {
                "type": "app_mention",
                "channel_type": "channel",
                "user": "U111",
                "channel": "C123",
                "ts": "1.000001",
                "text": "<@UBOT> ask <@U222>",
            },
        },
    )

    assert "<@U222>" in prompts[0]


def test_empty_mentions_and_slash_text_do_not_dispatch(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    bot.bot_user_id = "UBOT"
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT>",
        }
    )
    bot._handle_slash(
        {
            "command": "/sample",
            "user_id": "U111",
            "channel_id": "C123",
            "text": " ",
        }
    )

    assert prompts == []


def test_mentions_and_slash_static_responses_do_not_dispatch(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        static_responses=(
            SlackbotStaticResponse(
                names=("help", "workflows"),
                text="sample help",
            ),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    bot.bot_user_id = "UBOT"
    posts = []
    routes = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("static response should not dispatch")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or routes.append(route)
        or {"ts": "2.000001", "channel": route.channel_id or "C123"},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> HELP",
        }
    )
    bot._handle_slash(
        {
            "command": "/sample",
            "user_id": "U111",
            "channel_id": "C123",
            "text": "workflows",
        }
    )

    assert posts == ["sample help", "sample help"]
    assert routes[0].thread_ts == "1.000001"


def test_slash_command_happy_path_requires_command_name(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._handle_slash({"user_id": "U111", "channel_id": "C123", "text": "x"})
    assert prompts == []

    bot._handle_slash(
        {
            "command": "/sample",
            "user_id": "U111",
            "channel_id": "C123",
            "text": "summarize",
        }
    )

    assert "summarize" in prompts[0]


def test_slash_config_opens_modal_instead_of_codex(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((token, method, payload))
        return {"ok": True}

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("config should not dispatch to Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    bot._handle_slash(
        {
            "command": "/sample",
            "trigger_id": "TRIGGER",
            "user_id": "U111",
            "channel_id": "C123",
            "text": "config",
        }
    )

    assert calls[0][1] == "views.open"
    view = calls[0][2]["view"]
    assert view["callback_id"] == USER_CONFIG_MODAL_CALLBACK_ID
    assert calls[0][2]["trigger_id"] == "TRIGGER"


def test_slash_dm_config_bypasses_codex_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_dm_user_ids=("U111",),
            allowed_user_ids=("U999",),
        )
    )
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {"ok": True}

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("config should not dispatch to Codex")
        )
    )
    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("config should not post unauthorized")
        ),
    )

    bot._handle_slash(
        {
            "command": "/sample",
            "trigger_id": "TRIGGER",
            "user_id": "U111",
            "channel_id": "D123",
            "channel_name": "directmessage",
            "text": "config",
        }
    )

    assert calls[0][0] == "views.open"
    assert calls[0][1]["view"]["callback_id"] == USER_CONFIG_MODAL_CALLBACK_ID


def test_slash_dm_config_honors_dm_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_dm_user_ids=("U999",))
    )
    posts = []

    monkeypatch.setattr(
        "sneeze.slackbot.slack_api_post",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unauthorized config should not open a modal")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append((route, text))
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_slash(
        {
            "command": "/sample",
            "trigger_id": "TRIGGER",
            "user_id": "U111",
            "channel_id": "D123",
            "channel_name": "directmessage",
            "text": "config",
        }
    )

    assert posts[0][0].channel_id == "D123"
    assert posts[0][1].startswith("This Slackbot is not configured")


def test_slash_config_fallback_preserves_response_url(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("config should not dispatch to Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_user_config_launcher",
        lambda config, route, mode: calls.append((route, mode)),
    )

    bot._handle_slash(
        {
            "command": "/sample",
            "response_url": "https://response",
            "user_id": "U111",
            "channel_id": "C123",
            "text": "config",
        }
    )

    assert calls[0][0].response_url == "https://response"
    assert calls[0][1] == "config"


def test_slash_config_trigger_without_bot_token_posts_launcher(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile, allow_missing_tokens=True),
            allowed_user_ids=("U111",),
        )
    )
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("config should not dispatch to Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_user_config_launcher",
        lambda config, route, mode: calls.append((route, mode)),
    )

    bot._handle_slash(
        {
            "command": "/sample",
            "trigger_id": "TRIGGER",
            "response_url": "https://response",
            "user_id": "U111",
            "channel_id": "C123",
            "text": "config",
        }
    )

    assert calls[0][0].response_url == "https://response"
    assert calls[0][1] == "config"


def test_dm_config_posts_button_launcher(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    def fake_post_blocks(config, route, text, blocks):
        calls.append((route, text, blocks))
        return {"channel": "D123", "ts": "1.000001"}

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("config should not dispatch to Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_blocks",
        fake_post_blocks,
    )
    monkeypatch.setattr(
        bot,
        "_maybe_handle_agent_tmux",
        lambda text, route: (_ for _ in ()).throw(
            AssertionError("config should be handled before tmux")
        ),
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "bootstrap",
        }
    )

    assert calls[0][0].dm_user_id == "U111"
    button = calls[0][2][1]["elements"][0]
    assert button["action_id"] == USER_CONFIG_OPEN_ACTION_ID
    assert button["value"] == "bootstrap"


def test_dm_config_bypasses_codex_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_dm_user_ids=("U111",),
            allowed_user_ids=("U999",),
        )
    )
    calls = []

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("config should not dispatch to Codex")
        )
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_blocks",
        lambda config, route, text, blocks: calls.append((route, blocks))
        or {"channel": "D123", "ts": "1.000001"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("config should not post unauthorized")
        ),
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "config",
        }
    )

    assert calls[0][0].dm_user_id == "U111"


def test_dm_config_honors_early_dm_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_dm_user_ids=("U999",))
    )
    posts = []

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unauthorized config should not dispatch to Codex")
        )
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_blocks",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unauthorized config should not post launcher")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append((route, text))
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "config",
        }
    )

    assert posts[0][0].dm_user_id == "U111"
    assert posts[0][1].startswith("This Slackbot is not configured")


@pytest.mark.parametrize(
    "allowlist",
    (
        {"allowed_user_ids": ("U999",)},
        {"allowed_channel_ids": ("C999",)},
    ),
)
def test_dm_config_honors_non_dm_allowlists(
    tmp_path,
    monkeypatch,
    allowlist,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(replace(load_config(profile), **allowlist))
    posts = []

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unauthorized config should not dispatch to Codex")
        )
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_blocks",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unauthorized config should not post launcher")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append((route, text))
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "config",
        }
    )

    assert posts[0][0].dm_user_id == "U111"
    assert posts[0][1].startswith("This Slackbot is not configured")


def test_dm_config_button_bypasses_codex_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_dm_user_ids=("U111",),
            allowed_user_ids=("U999",),
        )
    )
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.slack_api_post",
        lambda token, method, payload: calls.append((method, payload))
        or {"ok": True},
    )

    bot._handle_interactive(
        {
            "type": "block_actions",
            "trigger_id": "TRIGGER",
            "user": {"id": "U111"},
            "channel": {"id": "D123"},
            "actions": [
                {
                    "action_id": USER_CONFIG_OPEN_ACTION_ID,
                    "value": "config",
                }
            ],
        }
    )

    assert calls[0][0] == "views.open"


def test_dm_config_submission_bypasses_codex_allowlist(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_dm_user_ids=("U111",),
            allowed_user_ids=("U999",),
        )
    )
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.save_user_config_submission",
        lambda config, payload: {"updated_fields": ["notes"]},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append((route, text))
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_interactive(
        {
            "type": "view_submission",
            "user": {"id": "U111"},
            "view": {
                "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {"channel_id": "D123", "user_id": "U111"}
                ),
            },
        }
    )

    assert calls[0][0].dm_user_id == "U111"
    assert calls[0][1].startswith("Config saved.")


def test_dm_config_submission_honors_dm_allowlist_metadata(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_dm_user_ids=("U999",))
    )
    posts = []

    monkeypatch.setattr(
        "sneeze.slackbot.save_user_config_submission",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unauthorized submission should not save")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append((route, text))
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_interactive(
        {
            "type": "view_submission",
            "user": {"id": "U111"},
            "view": {
                "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {"channel_id": "D123", "user_id": "U111"}
                ),
            },
        }
    )

    assert posts[0][0].dm_user_id == "U111"
    assert posts[0][1].startswith("This Slackbot is not configured")


def test_dm_static_response_bypasses_codex_allowlist(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        static_responses=(
            SlackbotStaticResponse(names=("help",), text="catalog"),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_dm_user_ids=("U111",),
            allowed_user_ids=("U999",),
        )
    )
    posts = []

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("static response should not dispatch to Codex")
        )
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "help",
        }
    )

    assert posts == ["catalog"]


def test_dm_static_response_honors_early_dm_allowlist(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        static_responses=(
            SlackbotStaticResponse(names=("help",), text="catalog"),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_dm_user_ids=("U999",))
    )
    posts = []

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError(
                "unauthorized static response should not dispatch to Codex"
            )
        )
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "help",
        }
    )

    assert len(posts) == 1
    assert posts[0].startswith("This Slackbot is not configured")


@pytest.mark.parametrize(
    "allowlist",
    (
        {"allowed_user_ids": ("U999",)},
        {"allowed_channel_ids": ("C999",)},
    ),
)
def test_dm_static_response_honors_non_dm_allowlists(
    tmp_path,
    monkeypatch,
    allowlist,
):
    profile = replace(
        make_profile(tmp_path),
        static_responses=(
            SlackbotStaticResponse(names=("help",), text="catalog"),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(replace(load_config(profile), **allowlist))
    posts = []

    bot.runner = SimpleNamespace(
        run=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError(
                "unauthorized static response should not dispatch to Codex"
            )
        )
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"channel": "D123", "ts": "1.000001"},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "text": "help",
        }
    )

    assert len(posts) == 1
    assert posts[0].startswith("This Slackbot is not configured")


def test_block_action_opens_user_config_modal(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {"ok": True}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    bot._handle_interactive(
        {
            "type": "block_actions",
            "trigger_id": "TRIGGER",
            "user": {"id": "U111"},
            "channel": {"id": "D123"},
            "actions": [
                {
                    "action_id": USER_CONFIG_OPEN_ACTION_ID,
                    "value": "config",
                }
            ],
        }
    )

    assert calls[0][0] == "views.open"
    assert calls[0][1]["view"]["callback_id"] == USER_CONFIG_MODAL_CALLBACK_ID


def test_block_action_normalizes_user_config_mode(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {"ok": True}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    bot._handle_interactive(
        {
            "type": "block_actions",
            "trigger_id": "TRIGGER",
            "user": {"id": "U111"},
            "channel": {"id": "D123"},
            "actions": [
                {
                    "action_id": USER_CONFIG_OPEN_ACTION_ID,
                    "value": "unexpected",
                }
            ],
        }
    )

    metadata = json.loads(calls[0][1]["view"]["private_metadata"])
    assert metadata["mode"] == "config"


def test_interactive_envelope_unwraps_nested_payload(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {"ok": True}

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    bot._dispatch_payload(
        None,
        {
            "type": "interactive",
            "payload": {
                "type": "block_actions",
                "trigger_id": "TRIGGER",
                "user": {"id": "U111"},
                "channel": {"id": "D123"},
                "actions": [
                    {
                        "action_id": USER_CONFIG_OPEN_ACTION_ID,
                        "value": "config",
                    }
                ],
            },
        },
    )

    assert calls[0][0] == "views.open"


def test_config_open_bypasses_full_worker_queue(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    responses = []
    calls = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    class FailingExecutor:
        def submit(self, *args, **kwargs):
            raise AssertionError("config open should not queue work")

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {"ok": True}

    bot.executor = FailingExecutor()
    while bot.work_semaphore.acquire(blocking=False):
        pass
    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )
    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    bot._handle_request(
        Client(),
        SimpleNamespace(
            envelope_id="ENV123",
            type="slash_commands",
            payload={
                "type": "slash_commands",
                "command": "/sample",
                "trigger_id": "TRIGGER",
                "user_id": "U111",
                "channel_id": "C123",
                "text": "config",
            },
        ),
    )

    assert responses[0].envelope_id == "ENV123"
    assert responses[0].payload is None
    assert wait_until(lambda: len(calls) >= 1)
    assert calls[0][0] == "views.open"
    assert calls[0][1]["trigger_id"] == "TRIGGER"

    bot._handle_request(
        Client(),
        SimpleNamespace(
            envelope_id="ENV124",
            type="interactive",
            payload={
                "type": "interactive",
                "payload": {
                    "type": "block_actions",
                    "trigger_id": "TRIGGER2",
                    "user": {"id": "U111"},
                    "channel": {"id": "C123"},
                    "actions": [
                        {
                            "action_id": USER_CONFIG_OPEN_ACTION_ID,
                            "value": "config",
                        }
                    ],
                },
            },
        ),
    )

    assert responses[1].envelope_id == "ENV124"
    assert responses[1].payload is None
    assert wait_until(lambda: len(calls) >= 2)
    assert calls[1][0] == "views.open"
    assert calls[1][1]["trigger_id"] == "TRIGGER2"


def test_config_open_dispatch_has_dedicated_backpressure(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))

    acquired = 0
    while bot.user_config_semaphore.acquire(blocking=False):
        acquired += 1

    def fail_dispatch(*args, **kwargs):
        raise AssertionError("full user-config queue should not dispatch")

    bot._dispatch_payload_safely = fail_dispatch

    bot._start_immediate_user_config_dispatch(
        "slash_commands",
        {"command": "/sample", "text": "config"},
    )
    for _ in range(acquired):
        bot.user_config_semaphore.release()


def test_config_open_dispatch_releases_semaphore_when_thread_fails(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))

    class FailingThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            raise RuntimeError("no threads")

    monkeypatch.setattr("sneeze.slackbot.threading.Thread", FailingThread)

    bot._start_immediate_user_config_dispatch(
        "slash_commands",
        {"command": "/sample", "text": "config"},
    )

    acquired = 0
    while bot.user_config_semaphore.acquire(blocking=False):
        acquired += 1
    for _ in range(acquired):
        bot.user_config_semaphore.release()

    assert acquired == min(
        USER_CONFIG_DISPATCH_LIMIT, bot.config.worker_count
    )


def test_unauthorized_view_submission_acknowledges_with_errors(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(
        load_config(profile),
        allowed_dm_user_ids=("U111",),
        allowed_user_ids=("U999",),
    )
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    responses = []
    posts = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    class FailingExecutor:
        def submit(self, *args, **kwargs):
            raise AssertionError("unauthorized submit should not queue work")

    request = SimpleNamespace(
        envelope_id="ENV123",
        type="interactive",
        payload={
            "type": "interactive",
            "payload": {
                "type": "view_submission",
                "user": {"id": "U111"},
                "view": {
                    "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                    "private_metadata": json.dumps(
                        {"channel_id": "C123", "user_id": "U111"}
                    ),
                },
            },
        },
    )
    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )
    bot.executor = FailingExecutor()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "D123"},
    )

    bot._handle_request(Client(), request)

    assert responses[0].envelope_id == "ENV123"
    assert responses[0].payload["response_action"] == "errors"
    assert "codex_enabled" in responses[0].payload["errors"]
    assert posts == []


def test_dm_config_submission_socket_ack_bypasses_codex_allowlist(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(
        load_config(profile),
        allowed_dm_user_ids=("U111",),
        allowed_user_ids=("U999",),
    )
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    responses = []
    posts = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.save_user_config_submission",
        lambda config, payload: {"updated_fields": ["notes"]},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append((route, text))
        or {"channel": "D123", "ts": "1.000001"},
    )

    request = SimpleNamespace(
        envelope_id="ENV123",
        type="interactive",
        payload={
            "type": "interactive",
            "payload": {
                "type": "view_submission",
                "user": {"id": "U111"},
                "view": {
                    "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                    "private_metadata": json.dumps(
                        {"channel_id": "D123", "user_id": "U111"}
                    ),
                    "blocks": [
                        {"type": "input", "block_id": "display_name"},
                        {"type": "input", "block_id": "codex_enabled"},
                    ],
                },
            },
        },
    )

    bot._handle_request(Client(), request)

    assert responses[0].envelope_id == "ENV123"
    assert responses[0].payload is None
    assert posts[0][0].dm_user_id == "U111"
    assert posts[0][1].startswith("Config saved.")


def test_view_submission_queue_full_returns_modal_error(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    responses = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    class FailingExecutor:
        def submit(self, *args, **kwargs):
            raise AssertionError("full queue should not submit work")

    request = SimpleNamespace(
        envelope_id="ENV123",
        type="interactive",
        payload={
            "type": "interactive",
            "payload": {
                "type": "view_submission",
                "user": {"id": "U111"},
                "view": {
                    "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                    "private_metadata": json.dumps(
                        {"channel_id": "D123", "user_id": "U111"}
                    ),
                    "blocks": [
                        {"type": "input", "block_id": "display_name"},
                        {"type": "input", "block_id": "codex_enabled"},
                    ],
                },
            },
        },
    )
    bot.executor = FailingExecutor()
    while bot.work_semaphore.acquire(blocking=False):
        pass
    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )

    bot._handle_request(Client(), request)

    assert responses[0].envelope_id == "ENV123"
    assert responses[0].payload["response_action"] == "errors"
    assert responses[0].payload["errors"]["codex_enabled"] == (
        "The bot is busy. Please submit the modal again."
    )


def test_non_config_view_submission_queue_full_returns_modal_error(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    responses = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    request = SimpleNamespace(
        envelope_id="ENV123",
        type="interactive",
        payload={
            "type": "interactive",
            "payload": {
                "type": "view_submission",
                "user": {"id": "U111"},
                "view": {
                    "callback_id": "other_modal",
                    "blocks": [{"type": "input", "block_id": "other_input"}],
                },
            },
        },
    )
    while bot.work_semaphore.acquire(blocking=False):
        pass
    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )

    bot._handle_request(Client(), request)

    assert responses[0].envelope_id == "ENV123"
    assert responses[0].payload["response_action"] == "errors"
    assert responses[0].payload["errors"]["other_input"] == (
        "The bot is busy. Please submit the modal again."
    )


def test_deduped_socket_request_releases_worker_slot(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    bot.executor = InlineExecutor()
    responses = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    request = SimpleNamespace(
        envelope_id="ENV123",
        type="events_api",
        payload={
            "type": "events_api",
            "event_id": "EV123",
            "event": {"type": "message", "bot_id": "B123"},
        },
    )
    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )

    bot._handle_request(Client(), request)
    bot._handle_request(Client(), request)

    assert count_available_worker_slots(bot) == (
        bot.config.worker_count * WORK_QUEUE_DEPTH_PER_WORKER
    )


def test_socket_request_releases_worker_slot_after_dispatch(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    bot.executor = InlineExecutor()
    responses = []
    handled = []

    class Response:
        def __init__(self, envelope_id, payload=None):
            self.envelope_id = envelope_id
            self.payload = payload

    class Client:
        def send_socket_mode_response(self, response):
            responses.append(response)

    request = SimpleNamespace(
        envelope_id="ENV123",
        type="events_api",
        payload={
            "type": "events_api",
            "event_id": "EV123",
            "event": {"type": "message", "text": "hello"},
        },
    )
    monkeypatch.setattr(
        bot,
        "_import_slack_sdk",
        lambda: (None, None, Response),
    )
    monkeypatch.setattr(
        bot, "_handle_event", lambda event: handled.append(event)
    )

    bot._handle_request(Client(), request)

    assert responses[0].envelope_id == "ENV123"
    assert handled == [{"type": "message", "text": "hello"}]
    assert count_available_worker_slots(bot) == (
        bot.config.worker_count * WORK_QUEUE_DEPTH_PER_WORKER
    )


def test_user_config_submission_feedback_falls_back_to_channel(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.save_user_config_submission",
        lambda config, payload: {"updated_fields": ["notes"]},
    )

    def fake_post(config, route, text):
        if route.dm_user_id:
            raise SlackbotError("dm unavailable")
        calls.append((route, text))
        return {"channel": route.channel_id, "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    bot._handle_interactive(
        {
            "type": "view_submission",
            "user": {"id": "U111"},
            "view": {
                "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {"channel_id": "C123", "user_id": "U111"}
                ),
            },
        }
    )

    assert calls == [(SlackbotRoute(channel_id="C123"), "Config saved.")]


def test_user_config_submission_failure_feedback_falls_back_to_channel(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.save_user_config_submission",
        lambda config, payload: (_ for _ in ()).throw(
            SlackbotError("bad private field")
        ),
    )

    def fake_post(config, route, text):
        if route.dm_user_id:
            raise SlackbotError("dm unavailable")
        calls.append((route, text))
        return {"channel": route.channel_id, "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    bot._handle_interactive(
        {
            "type": "view_submission",
            "user": {"id": "U111"},
            "view": {
                "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {"channel_id": "C123", "user_id": "U111"}
                ),
            },
        }
    )

    assert calls == [
        (
            SlackbotRoute(channel_id="C123"),
            "I could not save your config. Please try again.",
        )
    ]


def test_interactive_dedupe_key_includes_view_state(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    first = {
        "type": "view_submission",
        "user": {"id": "U111"},
        "view": {
            "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
            "id": "V123",
            "state": {
                "values": {
                    "display_name": {
                        "display_name": {"value": "Ada"},
                    },
                },
            },
        },
    }
    second = json.loads(json.dumps(first))
    second["view"]["state"]["values"]["display_name"]["display_name"][
        "value"
    ] = "Grace"

    assert bot._interactive_dedupe_key(first) == bot._interactive_dedupe_key(
        json.loads(json.dumps(first))
    )
    assert bot._interactive_dedupe_key(first) != bot._interactive_dedupe_key(
        second
    )


def test_safe_slack_storage_id_rejects_dot_segments():
    with pytest.raises(SlackbotError):
        safe_slack_storage_id("..")
    with pytest.raises(SlackbotError):
        safe_slack_storage_id("...")


def test_user_config_submission_ignores_modal_token_values(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        user_config_secret_fields=(
            JIRA_SECRET_FIELD,
            GITLAB_SECRET_FIELD,
            GITHUB_SECRET_FIELD,
            SlackbotUserConfigSecretField(
                service_name="acme",
                label="Acme token",
                env_names=("ACME_TOKEN", "ACME_SESSION_TOKEN"),
                placeholder_name="Acme",
            ),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    acme_secret = (
        Path(config.paths.user_config_dir) / "U111" / "secrets" / "acme.token"
    )
    acme_secret.parent.mkdir(parents=True)
    acme_secret.write_text("old-acme\n", encoding="utf-8")
    acme_secret.chmod(0o600)
    view = build_user_config_view(
        config,
        user_id="U111",
        channel_id="D123",
        mode="bootstrap",
    )
    block_ids = {block.get("block_id") for block in view["blocks"]}
    assert "acme_token" in block_ids
    acme_block = next(
        block
        for block in view["blocks"]
        if block.get("block_id") == "acme_token"
    )
    assert acme_block["type"] == "section"
    assert "configured" in acme_block["text"]["text"]
    view["state"] = {
        "values": {
            "display_name": {"display_name": {"value": "Ada"}},
            "default_project": {"default_project": {"value": "docs"}},
            # Secret-looking modal values are ignored. Tokens must be
            # provisioned outside Slack in the per-user secrets directory.
            "jira_token": {"jira_token": {"value": ""}},
            "gitlab_token": {"gitlab_token": {"value": "gl-secret"}},
            "github_token": {"github_token": {"value": "gh-secret"}},
            "acme_token": {"acme_token": {"value": "  acme-secret  "}},
            "codex_enabled": {
                "codex_enabled": {
                    "selected_options": [
                        {"value": "enabled", "text": {"text": "Enabled"}}
                    ]
                }
            },
            "notes": {"notes": {"value": "uses CUDA"}},
        }
    }
    payload = {
        "type": "view_submission",
        "team": {"id": "T123"},
        "user": {"id": "U111"},
        "view": view,
    }

    summary = save_user_config_submission(config, payload)

    profile_path = Path(summary["profile_path"])
    profile_text = profile_path.read_text(encoding="utf-8")
    profile_data = read_json(profile_path, {})
    user_dir = Path(config.paths.user_config_dir) / "U111"
    assert profile_data["preferences"]["display_name"] == "Ada"
    assert profile_data["preferences"]["default_project"] == "docs"
    assert profile_data["preferences"]["codex_enabled"] is True
    assert "codex_enabled" in summary["updated_fields"]
    assert not (user_dir / "secrets" / "gitlab.token").exists()
    assert not (user_dir / "secrets" / "github.token").exists()
    assert (user_dir / "secrets" / "acme.token").read_text() == ("old-acme\n")
    assert "gl-secret" not in profile_text
    assert "gh-secret" not in profile_text
    assert "acme-secret" not in profile_text
    assert summary["secret_updates"] == []
    core_secret_env = read_user_secret_env(config.paths, "U111", profile)
    assert "GH_TOKEN" not in core_secret_env
    assert "GITHUB_TOKEN" not in core_secret_env
    user_secret_env = read_user_secret_env(config.paths, "U111", profile)
    assert user_secret_env["ACME_TOKEN"] == "old-acme"
    assert user_secret_env["ACME_SESSION_TOKEN"] == "old-acme"
    assert os.stat(user_dir).st_mode & 0o777 == 0o700
    assert os.stat(user_dir / "secrets" / "acme.token").st_mode & 0o777 == (
        0o600
    )


def test_user_config_submission_leaves_secret_files_unchanged(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(
            JIRA_SECRET_FIELD,
            GITLAB_SECRET_FIELD,
            GITHUB_SECRET_FIELD,
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    user_dir = Path(config.paths.user_config_dir) / "U111"
    secrets_dir = user_dir / "secrets"
    secrets_dir.mkdir(parents=True)
    (secrets_dir / "github.token").write_text("old-gh\n")
    (secrets_dir / "gitlab.token").write_text("old-gl\n")
    view = build_user_config_view(
        config,
        user_id="U111",
        channel_id="D123",
        mode="config",
    )
    view["state"] = {
        "values": {
            "display_name": {"display_name": {"value": ""}},
            "default_project": {"default_project": {"value": ""}},
            "jira_token": {"jira_token": {"value": ""}},
            "gitlab_token": {"gitlab_token": {"value": ""}},
            "github_token": {"github_token": {"value": " CLEAR "}},
            "codex_enabled": {
                "codex_enabled": {
                    "selected_options": [
                        {"value": "enabled", "text": {"text": "Enabled"}}
                    ]
                }
            },
            "notes": {"notes": {"value": ""}},
        }
    }
    payload = {
        "type": "view_submission",
        "team": {"id": "T123"},
        "user": {"id": "U111"},
        "view": view,
    }

    summary = save_user_config_submission(config, payload)

    profile_data = read_json(summary["profile_path"], {})
    assert (secrets_dir / "github.token").read_text() == "old-gh\n"
    assert (secrets_dir / "gitlab.token").read_text() == "old-gl\n"
    assert profile_data.get("secrets", {}) == {}
    assert summary["secret_updates"] == []


def test_user_config_submission_checks_owner_before_state(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    class View(dict):
        def get(self, key, default=None):
            if key == "state":
                raise AssertionError("state should not be parsed")
            return super().get(key, default)

    payload = {
        "type": "view_submission",
        "user": {"id": "U111"},
        "view": View(
            {
                "callback_id": USER_CONFIG_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps({"user_id": "U222"}),
                "state": {
                    "values": {
                        "github_token": {
                            "github_token": {"value": "gh-secret"}
                        }
                    }
                },
            }
        ),
    }

    with pytest.raises(SlackbotError, match="user mismatch"):
        save_user_config_submission(config, payload)


def test_read_user_secret_env_skips_unreadable_optional_secret(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    secrets_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    secrets_dir.mkdir(parents=True)
    token_path = secrets_dir / "github.token"
    token_path.mkdir()

    assert read_user_secret_env(config.paths, "U111", profile) == {}


def test_read_user_secret_env_raises_on_unreadable_required_secret(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(
            replace(GITHUB_SECRET_FIELD, required=True),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    secrets_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    secrets_dir.mkdir(parents=True)
    token_path = secrets_dir / "github.token"
    token_path.mkdir()

    with pytest.raises(SlackbotError, match="Could not read"):
        read_user_secret_env(config.paths, "U111", profile)


def test_read_user_secret_env_allows_missing_optional_secret(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    monkeypatch.setenv("GH_TOKEN", "ambient-gh")

    assert read_user_secret_env(config.paths, "U111", profile) == {}


def test_store_user_secret_token_writes_0600_secret_and_metadata(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    summary = store_user_secret_token(
        config.paths,
        "U111",
        profile,
        "github",
        "  gh-secret  ",
        source="unit-test",
    )

    token_path = Path(summary["token_path"])
    profile_path = Path(summary["profile_path"])
    assert token_path.read_text(encoding="utf-8") == "gh-secret\n"
    assert token_path.stat().st_mode & 0o777 == 0o600
    assert token_path.parent.stat().st_mode & 0o777 == 0o700
    assert token_path.parent.parent.stat().st_mode & 0o777 == 0o700
    data = json.loads(profile_path.read_text(encoding="utf-8"))
    assert data["app_slug"] == "sample"
    assert data["slack_user_id"] == "U111"
    assert data["secrets"]["github"]["source"] == "unit-test"
    assert data["secrets"]["github"]["created_at"]
    assert data["secrets"]["github"]["updated_at"]

    env = read_user_secret_env(config.paths, "U111", profile)
    assert env == {"GH_TOKEN": "gh-secret"}


def test_user_secret_service_names_normalize_to_lowercase(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(
            SlackbotUserConfigSecretField(
                service_name="GitHub",
                label="GitHub token",
                env_names=("GH_TOKEN",),
            ),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    secret_field = user_config_secret_field(profile, "github")
    summary = store_user_secret_token(
        config.paths,
        "U111",
        profile,
        "GitHub",
        "gh-secret",
    )

    assert secret_field.service_name == "github"
    assert secret_field.block_id == "github_token"
    assert Path(summary["token_path"]).name == "github.token"
    assert read_user_secret_env(config.paths, "U111", profile) == {
        "GH_TOKEN": "gh-secret"
    }


def test_store_user_secret_token_rejects_unknown_or_empty_secret(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    with pytest.raises(SlackbotError, match="Unsupported"):
        user_config_secret_field(profile, "missing")
    with pytest.raises(SlackbotError, match="Unsupported"):
        store_user_secret_token(
            config.paths,
            "U111",
            profile,
            "missing",
            "secret",
        )
    with pytest.raises(SlackbotError, match="empty"):
        store_user_secret_token(
            config.paths,
            "U111",
            profile,
            "github",
            " ",
        )


def test_read_user_secret_env_requires_marked_secret(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(
            replace(GITHUB_SECRET_FIELD, required=True),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    with pytest.raises(SlackbotError, match="Missing required"):
        read_user_secret_env(config.paths, "U111", profile)


def test_required_user_secret_field_status_reflects_presence(tmp_path):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(
            replace(GITHUB_SECRET_FIELD, required=True),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    missing_view = build_user_config_view(config, user_id="U111")
    missing_github_block = next(
        block
        for block in missing_view["blocks"]
        if block.get("block_id") == "github_token"
    )
    assert missing_github_block["type"] == "section"
    assert "required, not configured" in missing_github_block["text"]["text"]

    secrets_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    secrets_dir.mkdir(parents=True)
    token_path = secrets_dir / "github.token"
    token_path.write_text("\n")
    empty_view = build_user_config_view(config, user_id="U111")
    empty_github_block = next(
        block
        for block in empty_view["blocks"]
        if block.get("block_id") == "github_token"
    )
    assert "required, empty" in empty_github_block["text"]["text"]

    token_path.unlink()
    token_path.mkdir()
    invalid_view = build_user_config_view(config, user_id="U111")
    invalid_github_block = next(
        block
        for block in invalid_view["blocks"]
        if block.get("block_id") == "github_token"
    )
    assert "required, invalid" in invalid_github_block["text"]["text"]

    token_path.rmdir()
    token_path.write_text("gh-secret\n")

    view = build_user_config_view(config, user_id="U111")
    github_block = next(
        block
        for block in view["blocks"]
        if block.get("block_id") == "github_token"
    )

    assert github_block["type"] == "section"
    assert "configured" in github_block["text"]["text"]


def test_slack_dispatch_injects_user_github_token(
    tmp_path,
    monkeypatch,
):
    profile = replace(
        make_profile(tmp_path),
        child_env_scrub_allowlist=("ACME_API_KEY",),
        user_config_secret_fields=(
            GITHUB_SECRET_FIELD,
            SlackbotUserConfigSecretField(
                service_name="acme",
                label="Acme token",
                env_names=("ACME_TOKEN",),
            ),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    user_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    user_dir.mkdir(parents=True)
    (user_dir / "github.token").write_text("gh-user-secret\n")
    (user_dir / "acme.token").write_text("acme-user-secret\n")
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            calls.append((prompt, kwargs))
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> triage PR #8948",
        }
    )

    assert "gh-user-secret" not in calls[0][0]
    assert calls[0][1]["scrub_secret_env"] is True
    assert calls[0][1]["use_scrub_allowlist"] is True
    assert calls[0][1]["extra_env"]["GH_TOKEN"] == "gh-user-secret"
    assert calls[0][1]["extra_env"]["ACME_TOKEN"] == "acme-user-secret"


def test_codex_final_answer_update_uses_slack_mrkdwn(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    updates = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": (
                    "Slack send was cancelled. Drafted reply:\n\n"
                    "**Yep**, I'm up."
                ),
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> are you up?",
        }
    )

    assert updates == [
        "Slack send was cancelled. Drafted reply:\n\n*Yep*, I'm up."
    ]


def test_noisy_public_codex_result_is_dm_detail_and_sanitized_update(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    posts = []
    updates = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": (
                    "I drafted the post at /tmp/post/index.qmd.\n\n"
                    "I could not complete the actual publish flow:\n"
                    "mkdir: cannot create directory: Read-only file system\n"
                    "```bash\ncd ~/src/website\n./render-all.sh\n```"
                ),
                "session_id": "codex-session-1",
            }

    def fake_post(config, route, text):
        posts.append((route, text))
        channel = "D111" if route.dm_user_id else "C123"
        return {"ts": f"{len(posts)}.000001", "channel": channel}

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> publish the site",
        }
    )

    detail_post = posts[1]
    assert detail_post[0].dm_user_id == "U111"
    assert "/tmp/post/index.qmd" in detail_post[1]
    assert "Read-only file system" in detail_post[1]
    assert updates == [
        (
            "I could not finish that from this bot run because the execution "
            "environment blocked part of the requested work. I sent the "
            "details to the requester in DM."
        )
    ]
    assert "/tmp/" not in updates[0]
    assert "```" not in updates[0]


def test_top_level_app_mention_includes_recent_slack_context(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "I can compare them now.",
                "session_id": "codex-session-1",
            }

    def fake_slack_api_post(token, method, payload):
        if method == "conversations.history":
            assert payload == {
                "channel": "C123",
                "limit": 10,
                "latest": "2.000001",
                "inclusive": True,
            }
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "2.000001",
                        "user": "U111",
                        "text": "<@UBOT> compare to the above link",
                    },
                    {
                        "ts": "1.000001",
                        "user": "U222",
                        "text": (
                            "Leo bot issue: "
                            "https://gitlab.example/leof/bot/-/issues/19"
                        ),
                    },
                ],
            }
        return {"ok": True}

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "3.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "2.000001",
            "text": "<@UBOT> compare to the above link",
        }
    )

    assert "Slack context:" in prompts[0]
    assert "https://gitlab.example/leof/bot/-/issues/19" in prompts[0]
    assert "you compare to the above link" in prompts[0]


def test_codex_disabled_user_profile_blocks_work_session(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    user_dir = Path(config.paths.user_config_dir) / "U111"
    user_dir.mkdir(parents=True)
    (user_dir / "profile.json").write_text(
        '{"preferences":{"codex_enabled":false}}\n',
        encoding="utf-8",
    )
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    posts = []
    updates = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("disabled user should not launch Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> summarize",
        }
    )

    assert posts == [
        "Codex-backed work sessions are disabled for your profile. Use the "
        "config modal to re-enable them."
    ]
    assert updates == []


def test_user_config_guidance_survives_conversation_store_failure(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    user_dir = Path(config.paths.user_config_dir) / "U111"
    user_dir.mkdir(parents=True)
    (user_dir / "profile.json").write_text(
        '{"preferences":{"codex_enabled":false}}\n',
        encoding="utf-8",
    )
    bot = SlackSocketBot(config)
    posts = []

    monkeypatch.setattr(
        bot.conversations,
        "get",
        lambda key: (_ for _ in ()).throw(SlackbotError("store failed")),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )

    bot._run_codex_for_route(
        "summarize",
        SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
        slack_user_id="U111",
    )

    assert posts == [
        "Codex-backed work sessions are disabled for your profile. Use the "
        "config modal to re-enable them."
    ]


def test_malformed_codex_profile_blocks_work_session(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    user_dir = Path(config.paths.user_config_dir) / "U111"
    user_dir.mkdir(parents=True)
    (user_dir / "profile.json").write_text("{bad json\n", encoding="utf-8")
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    posts = []
    updates = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("malformed profile should not launch Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> summarize",
        }
    )

    assert posts == [
        "I could not verify your Codex-backed work session settings, so I "
        "did not start a session."
    ]
    assert updates == []


def test_codex_profile_load_failure_blocks_work_session(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    posts = []
    updates = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("unverified user should not launch Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.user_codex_sessions_enabled",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            SlackbotError("bad profile")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> summarize",
        }
    )

    assert posts == [
        "I could not verify your Codex-backed work session settings, so I "
        "did not start a session."
    ]
    assert updates == []


def test_codex_secret_load_failure_blocks_work_session(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U111",))
    bot = SlackSocketBot(config)
    bot.bot_user_id = "UBOT"
    posts = []
    updates = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("missing secrets should not launch Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.read_user_secret_env",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            SlackbotError("bad secrets")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    bot._handle_event(
        {
            "type": "app_mention",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "ts": "1.000001",
            "text": "<@UBOT> summarize",
        }
    )

    assert posts == [
        "I could not load your user-scoped Slackbot secrets, so I did not "
        "start a Codex session."
    ]
    assert updates == []


def test_mpim_message_preserves_thread_context(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "G123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "mpim",
            "user": "U111",
            "channel": "G123",
            "thread_ts": "1.000001",
            "text": "continue",
        }
    )

    assert '"thread_ts": "1.000001"' in prompts[0]


def test_dm_conversation_key_survives_placeholder_thread(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    session_ids = []
    placeholder_counter = {"value": 0}

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            session_ids.append(session_id)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    def fake_post(config, route, text):
        placeholder_counter["value"] += 1
        return {
            "channel": "D123",
            "ts": f"2.00000{placeholder_counter['value']}",
        }

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    event = {
        "type": "message",
        "channel_type": "im",
        "user": "U111",
        "channel": "D123",
        "text": "hello",
    }
    bot._handle_event(event)
    bot._handle_event({**event, "thread_ts": "2.000001"})

    assert session_ids == [None, "codex-session-1"]
    assert bot.conversations.get("dm:D123:U111")["session_id"] == (
        "codex-session-1"
    )
    assert bot.conversations.get("user:U111:dm:D123:U111") is None


def test_dm_named_sessions_are_isolated_by_slack_user(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111", "U222"))
    )
    session_ids = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            session_ids.append(session_id)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": f"codex-session-{len(session_ids)}",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {
            "ts": f"2.00000{len(session_ids)}",
            "channel": route.channel_id,
        },
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._run_codex_for_route(
        "summarize",
        SlackbotRoute(channel_id="D111", dm_user_id="U111"),
        session="shared",
        slack_user_id="U111",
    )
    bot._run_codex_for_route(
        "summarize",
        SlackbotRoute(channel_id="D222", dm_user_id="U222"),
        session="shared",
        slack_user_id="U222",
    )
    bot._run_codex_for_route(
        "continue",
        SlackbotRoute(channel_id="D111", dm_user_id="U111"),
        session="shared",
        slack_user_id="U111",
    )

    assert session_ids == [None, None, "codex-session-1"]
    assert (
        bot.conversations.get("user:U111:session:shared")["session_id"]
        == "codex-session-3"
    )
    assert (
        bot.conversations.get("user:U222:session:shared")["session_id"]
        == "codex-session-2"
    )


def test_stale_session_resume_retries_as_fresh_session(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    bot.conversations.set(
        "dm:D111:U111",
        {"session_id": "stale-session", "updated_at": "now"},
    )
    session_ids = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            session_ids.append(session_id)
            if session_id == "stale-session":
                return {
                    "returncode": 1,
                    "stdout": "",
                    "stderr": "session not found",
                    "last_message": "",
                    "session_id": None,
                }
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "fresh reply",
                "session_id": "fresh-session",
            }

    bot.runner = FakeRunner()
    posts = []
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "D111"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: posts.append(kwds["text"]) or {"ok": True},
    )

    bot._run_codex_for_route(
        "continue",
        SlackbotRoute(channel_id="D111", dm_user_id="U111"),
    )

    assert session_ids == ["stale-session", None]
    assert bot.conversations.get("dm:D111:U111")["session_id"] == (
        "fresh-session"
    )
    assert posts[-1] == "fresh reply"


def test_channel_thread_sessions_are_isolated_by_slack_user(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111", "U222"))
    )
    bot.bot_user_id = "UBOT"
    session_ids = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            session_ids.append(session_id)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": f"codex-session-{len(session_ids)}",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )
    base_event = {
        "type": "app_mention",
        "channel_type": "channel",
        "channel": "C123",
        "thread_ts": "1.000001",
        "ts": "1.000002",
        "text": "<@UBOT> summarize",
    }

    bot._handle_event({**base_event, "user": "U111"})
    bot._handle_event({**base_event, "user": "U222"})

    assert session_ids == [None, None]


def test_dm_thread_reply_preserves_thread_context(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "D123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "channel": "D123",
            "thread_ts": "1.000001",
            "text": "continue",
        }
    )

    assert '"thread_ts": "1.000001"' in prompts[0]


def test_dm_message_dispatches_without_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            prompts.append(prompt)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "D123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U222",
            "channel": "D123",
            "text": "hello from a DM",
        }
    )

    assert "hello from a DM" in prompts[0]


def test_event_dispatch_uses_normalized_slack_user_id(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    def fake_run_codex(text, route, *, slack_user_id=None, **kwargs):
        calls.append((text, route, slack_user_id))

    bot._run_codex_for_route = fake_run_codex

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": " U222 ",
            "channel": "D123",
            "text": "hello from a DM",
        }
    )

    assert calls == [
        (
            "hello from a DM",
            SlackbotRoute(channel_id="D123", dm_user_id="U222"),
            "U222",
        )
    ]


def test_run_codex_rejects_unsafe_slack_user_id_before_session_key(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    posts = []

    class FailingConversationStore:
        def get(self, key):
            raise AssertionError(
                "unsafe Slack user ID should not read session"
            )

        def set(self, key, value):
            raise AssertionError(
                "unsafe Slack user ID should not write session"
            )

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("unsafe Slack user ID should not run Codex")

    bot.runner = FakeRunner()
    bot.conversations = FailingConversationStore()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )

    bot._run_codex_for_route(
        "hello",
        SlackbotRoute(channel_id="C123"),
        slack_user_id="../bad",
    )

    assert posts == [
        "I could not verify your Codex-backed work session settings, so I "
        "did not start a session."
    ]


def test_run_codex_can_raise_user_config_errors(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    posts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("unsafe Slack user ID should not run Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )

    with pytest.raises(SlackbotError, match="Unsupported Slack user ID"):
        bot._run_codex_for_route(
            "hello",
            SlackbotRoute(channel_id="C123"),
            slack_user_id="../bad",
            raise_user_config_errors=True,
        )

    assert posts == [
        "I could not verify your Codex-backed work session settings, so I "
        "did not start a session."
    ]


def test_run_codex_rejects_unsafe_dm_user_id_before_session_key(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    posts = []

    class FailingConversationStore:
        def get(self, key):
            raise AssertionError("unsafe DM user ID should not read session")

        def set(self, key, value):
            raise AssertionError("unsafe DM user ID should not write session")

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("unsafe DM user ID should not run Codex")

    bot.runner = FakeRunner()
    bot.conversations = FailingConversationStore()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "D123"},
    )

    bot._run_codex_for_route(
        "hello",
        SlackbotRoute(channel_id="D123", dm_user_id="../bad"),
        slack_user_id="U111",
    )

    assert posts == [
        "I could not verify your Codex-backed work session settings, so I "
        "did not start a session."
    ]


def test_unsafe_dm_user_id_preserves_existing_user_config_guidance(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    user_dir = Path(config.paths.user_config_dir) / "U111"
    user_dir.mkdir(parents=True)
    (user_dir / "profile.json").write_text(
        '{"preferences":{"codex_enabled":false}}\n',
        encoding="utf-8",
    )
    bot = SlackSocketBot(config)
    posts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("disabled user should not run Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "D123"},
    )

    bot._run_codex_for_route(
        "hello",
        SlackbotRoute(channel_id="D123", dm_user_id="../bad"),
        slack_user_id="U111",
    )

    assert posts == [
        "Codex-backed work sessions are disabled for your profile. Use the "
        "config modal to re-enable them."
    ]


def test_placeholder_failure_replies_in_original_thread(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    def fake_post(config, route, text):
        if text == "Working...":
            raise SlackbotError("placeholder failed")
        calls.append((route, text))
        return {"ts": "3.000001", "channel": "C123"}

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    bot._run_codex_for_route(
        "continue",
        SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
    )

    assert calls[0][0].thread_ts == "1.000001"
    assert calls[0][1] == "done"


def test_placeholder_failure_threads_followup_chunks(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "x" * 3501,
                "session_id": "codex-session-1",
            }

    def fake_post(config, route, text):
        if text == "Working...":
            raise SlackbotError("placeholder failed")
        calls.append(route)
        return {"ts": "3.000001", "channel": "C123"}

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)

    bot._run_codex_for_route("continue", SlackbotRoute(channel_id="C123"))

    assert calls[0].thread_ts is None
    assert calls[1].thread_ts == "3.000001"


def test_placeholder_thread_preserves_response_url_on_update_failure(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    def fake_post(config, route, text):
        calls.append((route, text))
        return {"ts": "3.000001", "channel": "C123"}

    bot.runner = FakeRunner()
    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: (_ for _ in ()).throw(
            SlackbotError("update failed")
        ),
    )

    bot._run_codex_for_route(
        "continue",
        SlackbotRoute(channel_id="C123", response_url="https://response"),
    )

    assert calls[1][0].response_url == "https://response"


def test_drain_ingress_resumes_named_conversation(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            calls.append((prompt, session_id, kwargs))
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="C123"),
        project="docs",
        session="weekly",
        system_scoped=True,
    )
    bot.drain_ingress()
    enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="continue",
        route=SlackbotRoute(channel_id="C123"),
        project="docs",
        session="weekly",
        system_scoped=True,
    )
    bot.drain_ingress()

    assert calls[0][1] is None
    assert calls[1][1] == "codex-session-1"
    assert calls[0][2]["scrub_secret_env"] is True
    assert "Project:\ndocs" in calls[0][0]


def test_drain_ingress_uses_payload_slack_user_secrets(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    user_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    user_dir.mkdir(parents=True)
    (user_dir / "github.token").write_text("gh-user-secret\n")
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            calls.append(kwargs)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="C123"),
        slack_user_id="U111",
    )
    bot.drain_ingress()

    assert calls[0]["scrub_secret_env"] is True
    assert calls[0]["extra_env"]["GH_TOKEN"] == "gh-user-secret"


def test_drain_ingress_codex_disabled_user_moves_to_done(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    user_dir = Path(config.paths.user_config_dir) / "U111"
    user_dir.mkdir(parents=True)
    (user_dir / "profile.json").write_text(
        '{"preferences":{"codex_enabled":false}}\n',
        encoding="utf-8",
    )
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    posts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("disabled user should not run Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )
    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="C123"),
        slack_user_id="U111",
    )

    bot.drain_ingress()

    ingress_path = Path(path)
    done_path = Path(config.paths.ingress_done_dir) / ingress_path.name
    error_path = Path(config.paths.ingress_error_dir) / ingress_path.name
    assert done_path.exists()
    assert not error_path.exists()
    assert not Path(str(error_path) + ".error").exists()
    assert posts == [
        "Codex-backed work sessions are disabled for your profile. Use the "
        "config modal to re-enable them."
    ]


def test_drain_ingress_user_secret_failure_notifies_and_errors(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    posts = []
    updates = []

    class FakeRunner:
        def run(self, *args, **kwargs):
            raise AssertionError("unreadable secrets should not launch Codex")

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.read_user_secret_env",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            SlackbotError("bad secrets")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text)
        or {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: updates.append(kwds["text"]) or {"ok": True},
    )

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="C123"),
        slack_user_id="U111",
    )
    bot.drain_ingress()

    ingress_path = Path(path)
    error_path = Path(config.paths.ingress_error_dir) / ingress_path.name
    done_path = Path(config.paths.ingress_done_dir) / ingress_path.name
    assert error_path.exists()
    assert not done_path.exists()
    assert "bad secrets" in Path(str(error_path) + ".error").read_text(
        encoding="utf-8"
    )
    assert "bad secrets" in Path(
        str(error_path) + ".user-config-error"
    ).read_text(encoding="utf-8")
    assert posts == [
        "I could not load your user-scoped Slackbot secrets, so I did not "
        "start a Codex session."
    ]
    assert updates == []


def test_drain_ingress_scrubs_ambient_secrets_without_slack_user_id(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    captured_env = []

    monkeypatch.setenv("GITHUB_TOKEN", "ambient-github")
    monkeypatch.setenv("GITLAB_TOKEN", "ambient-gitlab")
    monkeypatch.setenv("ACME_API_KEY", "ambient-api-key")
    monkeypatch.setenv("ACME_MODE", "debug")
    monkeypatch.setattr(
        "sneeze.slackbot.CHILD_ENV_PASSTHROUGH",
        {
            *CHILD_ENV_PASSTHROUGH,
            "ACME_API_KEY",
            "ACME_MODE",
            "GITHUB_TOKEN",
            "GITLAB_TOKEN",
        },
    )

    def fake_run(args, input, env, **kwargs):
        captured_env.append(env)
        Path(args[args.index("-o") + 1]).write_text("done")
        return SimpleNamespace(
            returncode=0,
            stdout=(
                '{"type":"session_meta",'
                '"payload":{"id":"codex-session-1"}}\n'
            ),
            stderr="",
        )

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="C123"),
        system_scoped=True,
    )
    bot.drain_ingress()

    assert captured_env
    assert "GITHUB_TOKEN" not in captured_env[0]
    assert "GITLAB_TOKEN" not in captured_env[0]
    assert "ACME_API_KEY" not in captured_env[0]
    assert captured_env[0]["ACME_MODE"] == "debug"


def test_drain_ingress_treats_legacy_channel_codex_as_system_scoped(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            calls.append(kwargs)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "C123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )
    ingress = Path(config.paths.ingress_dir)
    ingress.mkdir(parents=True, exist_ok=True)
    path = ingress / "legacy-codex.json"
    path.write_text(
        json.dumps(
            {
                "kind": "codex_prompt",
                "text": "summarize",
                "route": route_to_dict(SlackbotRoute(channel_id="C123")),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    bot.drain_ingress()

    error_path = Path(config.paths.ingress_error_dir) / path.name
    done_path = Path(config.paths.ingress_done_dir) / path.name
    assert done_path.exists()
    assert not error_path.exists()
    assert calls[0]["scrub_secret_env"] is True
    assert calls[0]["use_scrub_allowlist"] is True


def test_drain_ingress_migrates_legacy_dm_codex_to_system_scoped(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    secrets_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    secrets_dir.mkdir(parents=True)
    (secrets_dir / "github.token").write_text("gh-user-secret\n")
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            calls.append(kwargs)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: {"ts": "2.000001", "channel": "D123"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )
    ingress = Path(config.paths.ingress_dir)
    ingress.mkdir(parents=True, exist_ok=True)
    path = ingress / "legacy-dm-codex.json"
    path.write_text(
        json.dumps(
            {
                "kind": "codex_prompt",
                "system_scoped": False,
                "text": "summarize",
                "route": route_to_dict(
                    SlackbotRoute(channel_id="D123", dm_user_id="U111")
                ),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    bot.drain_ingress()

    error_path = Path(config.paths.ingress_error_dir) / path.name
    done_path = Path(config.paths.ingress_done_dir) / path.name
    assert done_path.exists()
    assert not error_path.exists()
    assert calls[0]["scrub_secret_env"] is True
    assert calls[0]["use_scrub_allowlist"] is True
    assert "extra_env" not in calls[0]


def test_drain_ingress_rejects_string_system_scoped(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()

    class FakeRunner:
        def run(self, *args, **kwargs):
            raise AssertionError("string system_scoped should not run Codex")

    bot.runner = FakeRunner()
    ingress = Path(config.paths.ingress_dir)
    ingress.mkdir(parents=True, exist_ok=True)
    path = ingress / "string-system-scoped.json"
    path.write_text(
        json.dumps(
            {
                "kind": "codex_prompt",
                "system_scoped": "false",
                "text": "summarize",
                "route": route_to_dict(SlackbotRoute(channel_id="C123")),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    bot.drain_ingress()

    error_path = Path(config.paths.ingress_error_dir) / path.name
    assert error_path.exists()
    assert "system_scoped must be a boolean" in Path(
        str(error_path) + ".error"
    ).read_text(encoding="utf-8")


def test_enqueue_codex_dm_defaults_user_scoped(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="D123", dm_user_id="U111"),
    )

    payload = read_json(path, {})
    assert payload["slack_user_id"] == "U111"
    assert payload["system_scoped"] is False


def test_enqueue_codex_uses_explicit_slack_user_id(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="D123", dm_user_id="U111"),
        slack_user_id="U111",
    )

    payload = read_json(path, {})
    assert payload["slack_user_id"] == "U111"
    assert payload["system_scoped"] is False


def test_enqueue_codex_command_defaults_to_system_scope(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    calls = []

    def fake_enqueue(profile_arg, **kwargs):
        calls.append((profile_arg, kwargs))
        return str(tmp_path / "ingress.json")

    monkeypatch.setattr(
        "sneeze.slackbot_commands.enqueue_ingress",
        fake_enqueue,
    )

    command = SlackbotEnqueueCodexBase()
    command.profile = profile
    command.options = SimpleNamespace(quiet=False)
    command.args = ["summarize"]
    command._channel_id = "D123"
    command._dm_user_id = "U111"

    command.run()

    assert calls[0][0] is profile
    assert calls[0][1]["kind"] == "codex_prompt"
    assert calls[0][1]["route"].dm_user_id == "U111"
    assert calls[0][1]["slack_user_id"] is None
    assert calls[0][1]["system_scoped"] is True


def test_enqueue_codex_command_accepts_explicit_slack_user_id(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    calls = []

    def fake_enqueue(profile_arg, **kwargs):
        calls.append((profile_arg, kwargs))
        return str(tmp_path / "ingress.json")

    monkeypatch.setattr(
        "sneeze.slackbot_commands.enqueue_ingress",
        fake_enqueue,
    )

    command = SlackbotEnqueueCodexBase()
    command.profile = profile
    command.options = SimpleNamespace(quiet=False)
    command.args = ["summarize"]
    command._channel_id = "D123"
    command._dm_user_id = "U111"
    command._slack_user_id = "U111"

    command.run()

    assert calls[0][1]["slack_user_id"] == "U111"
    assert calls[0][1]["system_scoped"] is None


def test_system_scoped_codex_dm_does_not_use_user_secrets(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(
        tmp_path,
        user_config_secret_fields=(GITHUB_SECRET_FIELD,),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    secrets_dir = Path(config.paths.user_config_dir) / "U111" / "secrets"
    secrets_dir.mkdir(parents=True)
    (secrets_dir / "github.token").write_text("gh-user-secret\n")
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    calls = []
    session_ids = []
    posts = []
    bot.conversations.set(
        "dm:D123:U111",
        {"session_id": "user-session", "updated_at": "now"},
    )

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            calls.append(kwargs)
            session_ids.append(session_id)
            return {
                "returncode": 0,
                "stdout": "",
                "stderr": "",
                "last_message": "done",
                "session_id": "codex-session-1",
            }

    bot.runner = FakeRunner()
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(route) or {"ts": "2.000001"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.update_slack_message",
        lambda *args, **kwds: {"ok": True},
    )

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="D123", dm_user_id="U111"),
        system_scoped=True,
    )
    assert read_json(path, {})["slack_user_id"] is None
    bot.drain_ingress()

    assert calls[0]["scrub_secret_env"] is True
    assert "extra_env" not in calls[0]
    assert session_ids == [None]
    assert posts[0].dm_user_id == "U111"
    assert bot.conversations.get("slack:D123:2.000001")["session_id"] == (
        "codex-session-1"
    )

    enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="continue",
        route=SlackbotRoute(
            channel_id="D123",
            dm_user_id="U111",
            thread_ts="2.000001",
        ),
        system_scoped=True,
    )
    bot.drain_ingress()

    assert session_ids == [None, "codex-session-1"]


def test_authorization_defaults_fail_closed(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))

    assert not bot._is_authorized("U111", "C123")
    assert not bot._is_authorized("U111", "G123", channel_type="mpim")


def test_authorization_allows_dm_without_user_allowlist(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))

    assert bot._is_authorized("U111", "D123", channel_type="im")


def test_channel_only_allowlist_emits_dm_migration_warning(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    messages = []

    SlackSocketBot(
        replace(load_config(profile), allowed_channel_ids=("C999",)),
        out=messages.append,
    )

    assert messages == [
        "warning: SLACKBOT_ALLOWED_CHANNEL_IDS does not authorize DMs; "
        "set SLACKBOT_ALLOWED_DM_USER_IDS or SLACKBOT_ALLOWED_USER_IDS "
        "for DM access."
    ]


def test_authorization_channel_allowlist_does_not_allow_any_dm(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_channel_ids=("C999",))
    )

    assert not bot._is_authorized("U111", "D123", channel_type="im")


def test_authorization_restricts_dm_with_user_allowlist(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )

    assert bot._is_authorized("U111", "D123", channel_type="im")
    assert not bot._is_authorized("U222", "D123", channel_type="im")


def test_authorization_restricts_dm_with_dm_user_allowlist(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_dm_user_ids=("U111",))
    )

    assert bot._is_authorized("U111", "D123", channel_type="im")
    assert not bot._is_authorized("U222", "D123", channel_type="im")
    assert not bot._is_authorized("U111", "C123")


def test_authorization_allows_global_user_with_dm_user_allowlist(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_dm_user_ids=("U999",),
            allowed_user_ids=("U111",),
        )
    )

    assert bot._is_authorized("U111", "D123", channel_type="im")


def test_authorization_allows_user_or_channel_match(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(
            load_config(profile),
            allowed_user_ids=("U111",),
            allowed_channel_ids=("C999",),
        )
    )

    assert bot._is_authorized("U111", "D123")
    assert bot._is_authorized("U222", "C999")
    assert not bot._is_authorized("U222", "C123")


def test_runtime_channel_trust_allows_channel(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    assert trust_slack_channel(config.paths, "C777")
    assert not trust_slack_channel(config.paths, "C777")
    trust = read_slackbot_trust(config.paths)
    assert trust["trusted_channel_ids"] == ["C777"]
    assert SlackSocketBot(config)._is_authorized("U222", "C777")
    assert untrust_slack_channel(config.paths, "C777")
    assert not SlackSocketBot(config)._is_authorized("U222", "C777")


def test_control_policy_command_posts_without_codex(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        default_allowed_user_ids=("U111",),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append(text),
    )
    monkeypatch.setattr(
        bot,
        "_run_codex_for_route",
        lambda *args, **kwds: pytest.fail("control command used Codex"),
    )

    handled = bot._maybe_handle_control_command(
        "!config",
        SlackbotRoute(channel_id="C123"),
        "U111",
    )

    assert handled
    assert "effective Codex policy" in calls[0]
    assert "workspace-write" in calls[0]


def test_diag_command_dms_public_details(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        default_allowed_user_ids=("U111",),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.run_slackbot_diagnostics",
        lambda config: [
            SlackbotDiagnosticResult(
                name="sample",
                label="Sample check",
                status="PASS",
                detail="ok",
            )
        ],
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append((route, text)),
    )

    assert bot._maybe_handle_control_command(
        "!diag",
        SlackbotRoute(channel_id="C123", thread_ts="1.000001"),
        "U111",
    )

    assert calls[0][0].dm_user_id == "U111"
    assert "Sample check" in calls[0][1]
    assert calls[1][0].channel_id == "C123"
    assert "DM" in calls[1][1]


def test_effective_codex_policy_and_rendering(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(
        profile,
        bot_token="xoxb-test",
        app_token="xapp-test",
        codex_mode="danger-full-access",
    )
    config = load_config(profile)

    policy = effective_codex_policy(config)
    rendered = render_effective_codex_policy(config)

    assert policy["danger_full_access"] is True
    assert "yolo-style" in rendered


def test_append_slackbot_audit_event_writes_private_jsonl(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    append_slackbot_audit_event(
        config,
        {
            "kind": "test",
            "value": 1,
            "SLACK_BOT_TOKEN": "xoxb-secret-token",
            "message": "Bearer abcdef ghp_secret sk-secret",
        },
    )

    path = Path(config.paths.audit_path)
    record = json.loads(path.read_text().splitlines()[0])
    assert record["kind"] == "test"
    assert record["app_slug"] == "sample"
    assert record["SLACK_BOT_TOKEN"] == "***"
    assert record["message"] == "Bearer *** ghp_*** sk-***"
    assert path.stat().st_mode & 0o777 == 0o600


def test_resolve_slack_mentions_uses_users_info(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    calls = []

    def fake_slack_api_post(token, method, payload):
        calls.append((method, payload))
        return {
            "ok": True,
            "user": {
                "profile": {"display_name": "Alice"},
                "real_name": "Alice Example",
            },
        }

    monkeypatch.setattr("sneeze.slackbot.slack_api_post", fake_slack_api_post)

    assert (
        resolve_slack_mentions(
            config,
            "hi <@U111> and <@UBOT>",
            bot_user_id="UBOT",
            cache={},
        )
        == "hi @Alice and you"
    )
    assert calls == [("users.info", {"user": "U111"})]


def test_agent_tmux_commands_accept_exact_aliases(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    route = SlackbotRoute(channel_id="C123", thread_ts="1.000001")
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.query_agent_tmux_thread",
        lambda *args, **kwds: {"host": "dgx"},
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append(text),
    )

    assert not bot._maybe_handle_agent_tmux("status of the PR", route)
    assert bot._maybe_handle_agent_tmux("status", route)
    assert bot._maybe_handle_agent_tmux("tmux status", route)
    assert bot._maybe_handle_agent_tmux("u", route)
    assert len(calls) == 3


def test_agent_tmux_help_alias_posts_command_summary(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    route = SlackbotRoute(channel_id="C123", thread_ts="1.000001")
    calls = []

    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: calls.append(text),
    )

    assert bot._maybe_handle_agent_tmux("?", route)
    assert "Tmux thread controls" in calls[0]


def test_agent_tmux_tail_command_posts_pane_capture(tmp_path, monkeypatch):
    tmux_conf = tmp_path / "codex.tmux.conf"
    profile = replace(
        make_profile(tmp_path),
        default_codex_tmux_socket="codex",
        default_codex_tmux_conf=str(tmux_conf),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
        tmux_conf=str(tmux_conf),
    )
    bot = SlackSocketBot(load_config(profile))
    route = SlackbotRoute(channel_id="C123", thread_ts="1.000001")
    posts = []
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        return SimpleNamespace(
            returncode=0,
            stdout="worker line one\nworker line two\n",
            stderr="",
        )

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
    )

    assert bot._maybe_handle_agent_tmux("tail 3", route)

    assert calls[0][-6:] == [
        "capture-pane",
        "-p",
        "-t",
        "codex-work",
        "-S",
        "-3",
    ]
    assert "Last 3 lines" in posts[0]
    assert "worker line two" in posts[0]


def test_bound_dm_reply_is_pasted_into_idle_tmux(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n"
        + "SAMPLE_SLACKBOT_ALLOWED_DM_USER_IDS=U222\n",
        encoding="utf-8",
    )
    config = load_config(profile)
    bind_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    bot = SlackSocketBot(config)
    pasted = []
    posts = []
    idle_state = AgentTmuxState(
        binding={
            "host": "localhost",
            "tmux_session": "codex-work",
            "tmux_socket": "codex",
        },
        pane_cwd=str(tmp_path),
        pane_text="Ready\n›",
        pane_excerpt="Ready\n›",
        prompt_line="›",
        status_line="›",
        context_remaining_percent=None,
        working=False,
    )

    monkeypatch.setattr(
        bot,
        "_sample_agent_tmux_route",
        lambda route: idle_state,
    )
    monkeypatch.setattr(
        "sneeze.slackbot.paste_agent_tmux_text",
        lambda config, binding, text: pasted.append((binding, text)),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
    )
    monkeypatch.setattr(
        bot,
        "_run_codex_for_route",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not start Codex")
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
            "text": "please continue exactly",
        }
    )

    assert len(pasted) == 1
    binding, text = pasted[0]
    assert binding["channel_id"] == "D111"
    assert binding["thread_ts"] == "1.000001"
    assert binding["tmux_session"] == "codex-work"
    assert binding["tmux_socket"] == "codex"
    assert text == "please continue exactly"
    assert posts == []


@pytest.mark.parametrize(
    ("failure_stage", "expected_text"),
    (
        ("inspect", "could not inspect"),
        ("paste", "could not paste"),
    ),
)
def test_bound_dm_reply_reports_tmux_oserror(
    tmp_path,
    monkeypatch,
    failure_stage,
    expected_text,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    config = load_config(profile)
    bind_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    bot = SlackSocketBot(config)
    route = SlackbotRoute(channel_id="D111", thread_ts="1.000001")
    idle_state = AgentTmuxState(
        binding={},
        pane_cwd=str(tmp_path),
        pane_text="Ready\n›",
        pane_excerpt="Ready\n›",
        prompt_line="›",
        status_line="›",
        context_remaining_percent=None,
        working=False,
    )
    posts = []

    if failure_stage == "inspect":
        monkeypatch.setattr(
            bot,
            "_sample_agent_tmux_route",
            lambda route: (_ for _ in ()).throw(
                FileNotFoundError("tmux is missing")
            ),
        )
    else:
        monkeypatch.setattr(
            bot,
            "_sample_agent_tmux_route",
            lambda route: idle_state,
        )
        monkeypatch.setattr(
            "sneeze.slackbot.paste_agent_tmux_text",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                FileNotFoundError("tmux is missing")
            ),
        )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
    )

    assert bot._maybe_mirror_agent_tmux_reply("continue", route, "U111")
    assert len(posts) == 1
    assert expected_text in posts[0]
    assert "tmux is missing" in posts[0]


def test_unbound_notification_thread_replies_are_read_only(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    for env_name in (
        "WEBMUX_AGENT_SESSION",
        "CODEX_TMUX_SESSION_NAME",
        "TMUX_PANE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    posts = []

    def fake_post(config, route, text):
        posts.append(text)
        return {"channel": "D111", "ts": "1.000001"}

    monkeypatch.setattr("sneeze.slackbot.post_slack_message", fake_post)
    notify_codex_agent_stop(
        profile,
        json.dumps(
            {
                "session_id": "codex-session-1",
                "turn_id": "turn-1",
                "last_assistant_message": "Read-only stop.",
            }
        ),
    )
    posts.clear()
    bot = SlackSocketBot(load_config(profile))
    monkeypatch.setattr(
        bot,
        "_run_codex_for_route",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("read-only reply should not start Codex")
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
            "text": "please continue",
        }
    )

    assert posts == [
        "No tmux pane is bound to this notification thread. "
        "Replies are read-only."
    ]


def test_bound_dm_reply_refuses_busy_tmux(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    config = load_config(profile)
    bind_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    bot = SlackSocketBot(config)
    posts = []
    busy_state = AgentTmuxState(
        binding={},
        pane_cwd=str(tmp_path),
        pane_text="Working (1s)",
        pane_excerpt="Working (1s)",
        prompt_line=None,
        status_line="Working (1s)",
        context_remaining_percent=None,
        working=True,
    )

    monkeypatch.setattr(
        bot,
        "_sample_agent_tmux_route",
        lambda route: busy_state,
    )
    monkeypatch.setattr(
        "sneeze.slackbot.paste_agent_tmux_text",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("busy pane should not receive text")
        ),
    )
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
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

    assert posts == [
        "The bound tmux pane appears busy. I did not send that reply."
    ]


def test_public_thread_and_bot_messages_do_not_mirror_to_tmux(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    env_path = Path(config.paths.env_path)
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_AGENT_NOTIFY_SLACK_USER_ID=U111\n",
        encoding="utf-8",
    )
    config = load_config(profile)
    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    bot = SlackSocketBot(config)
    monkeypatch.setattr(
        "sneeze.slackbot.paste_agent_tmux_text",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("message should not be mirrored")
        ),
    )

    bot._handle_event(
        {
            "type": "message",
            "channel_type": "channel",
            "user": "U111",
            "channel": "C123",
            "thread_ts": "1.000001",
            "ts": "2.000001",
            "text": "public reply",
        }
    )
    bot._handle_event(
        {
            "type": "message",
            "channel_type": "im",
            "user": "U111",
            "bot_id": "B111",
            "channel": "D111",
            "thread_ts": "1.000001",
            "ts": "2.000001",
            "text": "bot reply",
        }
    )


def test_bound_dm_status_uses_tmux_status_before_bot_policy(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="D111",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    bot = SlackSocketBot(load_config(profile))
    calls = []

    monkeypatch.setattr(
        bot,
        "_handle_agent_tmux_status",
        lambda route, job: calls.append((route, job)),
    )
    monkeypatch.setattr(
        bot,
        "_maybe_handle_control_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("tmux status should run first")
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
            "text": "status",
        }
    )

    assert calls and calls[0][0].channel_id == "D111"


def test_agent_tmux_proceed_sends_explicit_next_point(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    config = load_config(profile)
    bot = SlackSocketBot(config)
    route = SlackbotRoute(channel_id="C123", thread_ts="1.000001")
    posts = []
    calls = []

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if "list-panes" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=f"%1\t1\t{tmp_path}\n",
                stderr="",
            )
        if "capture-pane" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "Ready\n"
                    "Next continuation point: inspect README\n"
                    "›\n"
                ),
                stderr="",
            )
        if "send-keys" in args:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(args)

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
    )

    assert bot._maybe_handle_agent_tmux("p", route)

    send_calls = [call for call in calls if "send-keys" in call]
    assert send_calls[0][-1] == "Inspect README."
    assert send_calls[1][-1] == "Enter"
    assert "Nudge sent" in posts[0]
    job = read_json(config.paths.agent_tmux_jobs_path, {})["C123:1.000001"]
    assert job["mode"] == "proceed"
    assert job["state"] == "awaiting_start"
    assert job["last_nudge_text"] == "Inspect README."


def test_agent_tmux_proceed_text_is_not_hijacked_without_binding(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    route = SlackbotRoute(channel_id="C123", thread_ts="1.000001")

    assert not bot._maybe_handle_agent_tmux("proceed with the PR", route)


def test_agent_tmux_awaiting_start_pauses_on_stale_prompt(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    config = load_config(profile)
    bot = SlackSocketBot(config)
    posts = []
    store_agent_tmux_job(
        config,
        channel_id="C123",
        thread_ts="1.000001",
        values={
            "host": "localhost",
            "tmux_session": "codex-work",
            "tmux_socket": "codex",
            "mode": "proceed",
            "state": "awaiting_start",
            "last_nudge_text": "Inspect README.",
            "next_poll_at": 0,
        },
    )

    def fake_run(args, check, capture_output, text):
        if "list-panes" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=f"%1\t1\t{tmp_path}\n",
                stderr="",
            )
        if "capture-pane" in args:
            return SimpleNamespace(
                returncode=0,
                stdout="› Inspect README.\n",
                stderr="",
            )
        raise AssertionError(args)

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
    )

    bot._process_agent_tmux_job("C123:1.000001")

    job = read_json(config.paths.agent_tmux_jobs_path, {})["C123:1.000001"]
    assert job["state"] == "paused"
    assert "still be sitting" in job["paused_reason"]
    assert "Nudge paused" in posts[0]


def test_agent_tmux_tick_processes_due_watch_jobs(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    bot = SlackSocketBot(config)
    bot.executor = InlineExecutor()
    processed = []
    store_agent_tmux_job(
        config,
        channel_id="C123",
        thread_ts="1.000001",
        values={
            "host": "localhost",
            "tmux_session": "codex-work",
            "tmux_socket": "codex",
            "mode": "nurse",
            "state": "watching",
            "goal_text": "finish",
            "next_poll_at": 0,
        },
    )
    monkeypatch.setattr(
        bot,
        "_process_agent_tmux_job_safe",
        lambda key: processed.append(key),
    )

    bot._tick_agent_tmux_jobs()

    assert processed == ["C123:1.000001"]


def test_agent_tmux_nurse_sends_next_nudge_after_idle(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bind_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.000001",
        host="localhost",
        tmux_session="codex-work",
        tmux_socket="codex",
    )
    config = load_config(profile)
    bot = SlackSocketBot(config)
    posts = []
    calls = []
    store_agent_tmux_job(
        config,
        channel_id="C123",
        thread_ts="1.000001",
        values={
            "host": "localhost",
            "tmux_session": "codex-work",
            "tmux_socket": "codex",
            "mode": "nurse",
            "state": "watching",
            "goal_text": "finish",
            "next_poll_at": 0,
        },
    )

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if "list-panes" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=f"%1\t1\t{tmp_path}\n",
                stderr="",
            )
        if "capture-pane" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "Done with previous chunk.\n"
                    "Next continuation point: run tests\n"
                    "›\n"
                ),
                stderr="",
            )
        if "send-keys" in args:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(args)

    monkeypatch.setattr("sneeze.slackbot.subprocess.run", fake_run)
    monkeypatch.setattr(
        "sneeze.slackbot.post_slack_message",
        lambda config, route, text: posts.append(text),
    )

    bot._process_agent_tmux_job("C123:1.000001")

    send_calls = [call for call in calls if "send-keys" in call]
    assert send_calls[0][-1] == "Run tests."
    assert "Completion snapshot" in posts[0]
    assert "Next nurse nudge" in posts[1]
    job = read_json(config.paths.agent_tmux_jobs_path, {})["C123:1.000001"]
    assert job["last_nudge_text"] == "Run tests."
    assert job["state"] == "awaiting_start"


def test_user_config_trigger_words_require_exact_message():
    assert user_config_mode_from_text("setup") == "bootstrap"
    assert user_config_mode_from_text("config") == "config"
    assert (
        user_config_mode_from_text("setup the local dev environment") is None
    )
    assert user_config_mode_from_text("configure the database") is None


def test_static_response_names_require_exact_message(tmp_path):
    profile = replace(
        make_profile(tmp_path),
        static_responses=(
            SlackbotStaticResponse(
                names=("help", "sample-workflow list"),
                text="catalog",
            ),
        ),
    )

    assert static_response_text_from_profile(profile, " HELP ") == "catalog"
    assert (
        static_response_text_from_profile(profile, "sample-workflow   list")
        == "catalog"
    )
    assert static_response_text_from_profile(profile, "help me") is None
    assert (
        static_response_text_from_profile(
            profile,
            "sample-workflow triage-pr 8948",
        )
        is None
    )
    assert static_response_text_from_profile(profile, None) is None


def test_drain_ingress_does_not_reclaim_fresh_processing_file(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)
    path = Path(
        enqueue_ingress(
            profile,
            kind="slack_message",
            text="hello",
            route=SlackbotRoute(channel_id="C123"),
        )
    )
    processing = Path(config.paths.ingress_processing_dir)
    processing.mkdir(parents=True, exist_ok=True)
    claimed = processing / path.name
    path.replace(claimed)

    SlackSocketBot(config).drain_ingress()

    assert claimed.exists()


def test_enqueue_accepts_tmux_execution_mode(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="C123"),
        execution_mode="tmux",
        system_scoped=True,
    )

    assert read_json(path, {})["execution_mode"] == "tmux"


def test_launchd_schedule_upsert_skips_systemd_unit_files(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    monkeypatch.setattr("sneeze.slackbot.sys.platform", "darwin")

    result = upsert_schedule(
        profile,
        name="daily",
        on_calendar="daily",
        command=["echo", "ok"],
        workdir=str(tmp_path),
        cli_bin="sample",
        run_subcommand="slackbot-schedule-run",
        install_timer=True,
    )

    assert "launchd" in result["install_skipped"]
    assert not (tmp_path / "runtime" / "systemd" / "schedules").exists()


def test_slackbot_command_common_args_and_siblings(tmp_path):
    from sneeze.slackbot_commands import SlackbotScheduleUpsertBase

    class SampleScheduleUpsert(SlackbotScheduleUpsertBase):
        profile = make_profile(tmp_path)

    command = SampleScheduleUpsert(None, None, None)
    command.cli_display_name = "sample-slackbot-schedule-upsert"
    command._unit_name = "custom.service"
    cli = tmp_path / "sample"
    cli.write_text("#!/bin/sh\n", encoding="utf-8")

    assert command._common()["unit_name"] == "custom.service"
    assert (
        command._sibling_subcommand("slackbot-schedule-run")
        == "sample-slackbot-schedule-run"
    )
    assert command._resolved_cli_bin(str(cli)) == str(cli)


def test_tmux_dev_prefers_managed_env_codex_bin(tmp_path, monkeypatch):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)
    scaffold_runtime(sample_profile, codex_bin="/tmp/sample-codex")

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    command = SampleDevSlackbot(None, None, None)
    command.cli_display_name = "sample dev-slackbot"
    command._cli_bin = "/tmp/sample"
    command._mamba_bin = "/tmp/mamba"
    monkeypatch.setattr(
        "sneeze.slackbot_commands.resolve_required_executable",
        lambda requested, *names: f"/tmp/{names[0]}",
    )

    assert command._codex_bin_value() == "/tmp/sample-codex"
    child = command._child_command()
    cli_index = child.index("/tmp/sample")
    assert child[cli_index + 1 : cli_index + 3] == ["sample", "dev-slackbot"]
    assert child[child.index("--codex-bin") + 1] == "/tmp/sample-codex"


def test_tmux_dev_child_defers_unconfigured_codex_bin(
    tmp_path,
    monkeypatch,
):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    def fake_required(requested, *names):
        if "codex" in names:
            raise AssertionError("child should resolve codex inside env")
        return f"/tmp/{names[0]}"

    command = SampleDevSlackbot(None, None, None)
    command._cli_bin = "/tmp/sample"
    command._mamba_bin = "/tmp/mamba"
    monkeypatch.setattr(
        "sneeze.slackbot_commands.resolve_required_executable",
        fake_required,
    )

    child = command._child_command()

    assert "--codex-bin" not in child


def test_tmux_dev_child_command_forwards_slackbot_overrides(tmp_path):
    from sneeze.slackbot_commands import SlackbotDevBase

    workdir = tmp_path / "work"
    workdir.mkdir()
    prompt_path = tmp_path / "prompt.md"
    state_dir = tmp_path / "state"
    team_config_path = tmp_path / "team.json"

    class SampleDevSlackbot(SlackbotDevBase):
        profile = make_profile(tmp_path)

    command = SampleDevSlackbot(None, None, None)
    command.cli_display_name = "sample dev-slackbot"
    command._mamba_bin = "/tmp/mamba"
    command._cli_bin = "/tmp/sample"
    command._runtime_root = str(tmp_path / "runtime-custom")
    command._env_path = str(tmp_path / "runtime-custom" / ".env")
    command._state_dir = str(state_dir)
    command._system_prompt_path = str(prompt_path)
    command._unit_name = "sample-custom.service"
    command._slack_domain = "sample.slack.com"
    command._app_id = "A123"
    command._client_id = "C123"
    command._bot_name = "samplebot"
    command._command_name = "/sample"
    command._codex_bin = "/tmp/codex-custom"
    command._codex_mode = "read-only"
    command._codex_model = "gpt-test"
    command._codex_profile = "dev"
    command._codex_workdir = str(workdir)
    command._codex_extra_args = "--approval-mode never"
    command._worker_count = 7
    command._mcp_server_url = "http://127.0.0.1:8765/mcp"
    command._team_config_path = str(team_config_path)
    command._max_runtime_seconds = 30

    child = command._child_command()
    values = {
        child[index]: child[index + 1]
        for index in range(len(child) - 1)
        if child[index].startswith("--")
    }

    assert values["--runtime-root"] == str(tmp_path / "runtime-custom")
    assert values["--env-path"] == str(tmp_path / "runtime-custom" / ".env")
    assert values["--state-dir"] == str(state_dir)
    assert values["--system-prompt-path"] == str(prompt_path)
    assert values["--unit-name"] == "sample-custom.service"
    assert values["--slack-domain"] == "sample.slack.com"
    assert values["--app-id"] == "A123"
    assert values["--client-id"] == "C123"
    assert values["--bot-name"] == "samplebot"
    assert values["--command-name"] == "/sample"
    assert values["--codex-bin"] == "/tmp/codex-custom"
    assert values["--codex-mode"] == "read-only"
    assert values["--codex-model"] == "gpt-test"
    assert values["--codex-profile"] == "dev"
    assert values["--codex-workdir"] == str(workdir.resolve())
    assert values["--codex-extra-args"] == "--approval-mode never"
    assert values["--worker-count"] == "7"
    assert values["--mcp-server-url"] == "http://127.0.0.1:8765/mcp"
    assert values["--team-config-path"] == str(team_config_path)
    assert values["--max-runtime-seconds"] == "30"


def test_slackbot_dev_smoke_defaults_to_bounded_runtime(
    tmp_path,
    monkeypatch,
    capsys,
):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)
    scaffold_runtime(sample_profile)

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    captured = []
    command = SampleDevSlackbot(None, None, None)
    monkeypatch.setattr(
        "sneeze.slackbot_commands.query_status", lambda *a, **k: {}
    )
    monkeypatch.setattr(
        command,
        "_run_slackbot",
        lambda *, max_runtime_seconds=None: captured.append(
            max_runtime_seconds
        ),
    )

    command._smoke()
    capsys.readouterr()

    assert captured == [3]


def test_slackbot_dev_smoke_zero_runtime_is_unbounded(
    tmp_path,
    monkeypatch,
    capsys,
):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)
    scaffold_runtime(sample_profile)

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    captured = []
    command = SampleDevSlackbot(None, None, None)
    command._max_runtime_seconds = 0
    monkeypatch.setattr(
        "sneeze.slackbot_commands.query_status", lambda *a, **k: {}
    )
    monkeypatch.setattr(
        command,
        "_run_slackbot",
        lambda *, max_runtime_seconds=None: captured.append(
            max_runtime_seconds
        ),
    )

    command._smoke()
    capsys.readouterr()

    assert captured == [None]


def test_tmux_dev_honors_live_slackbot_codex_bin_env(tmp_path, monkeypatch):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)
    scaffold_runtime(sample_profile)
    monkeypatch.setenv("SAMPLE_SLACKBOT_CODEX_BIN", "/tmp/live-codex")
    monkeypatch.setenv("SAMPLE_CODEX_BIN", "/tmp/dev-codex")

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    command = SampleDevSlackbot(None, None, None)

    assert command._codex_bin_value() == "/tmp/live-codex"


def test_tmux_dev_expands_tilde_codex_workdir(tmp_path, monkeypatch):
    from sneeze.slackbot_commands import SlackbotDevBase

    home = tmp_path / "home"
    home.mkdir()
    sample_profile = replace(
        make_profile(tmp_path),
        default_codex_workdir="~/src/sample",
    )
    monkeypatch.setenv("HOME", str(home))
    scaffold_runtime(sample_profile)

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    command = SampleDevSlackbot(None, None, None)

    assert command._codex_workdir_value() == str(home / "src" / "sample")
    assert command._tmux_root() == str(home / "src" / "sample")


def test_tmux_dev_defaults_missing_codex_workdir_to_cwd(
    tmp_path,
    monkeypatch,
):
    from sneeze.slackbot_commands import SlackbotDevBase

    cwd = tmp_path / "cwd"
    cwd.mkdir()
    sample_profile = replace(make_profile(tmp_path), default_codex_workdir="")
    monkeypatch.chdir(cwd)

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    command = SampleDevSlackbot(None, None, None)

    assert command._codex_workdir_value() == str(cwd)


def test_slackbot_dev_run_preserves_env_tokens(tmp_path, monkeypatch):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)
    scaffold_runtime(
        sample_profile,
        bot_token="xoxb-existing",
        app_token="xapp-existing",
    )
    env_path = tmp_path / "runtime" / ".env"
    env_path.write_text(
        env_path.read_text(encoding="utf-8")
        + "SAMPLE_PLUGIN_SETTING=keep-me\n",
        encoding="utf-8",
    )

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    calls = []
    command = SampleDevSlackbot(None, None, None)
    command.args = ("run",)
    monkeypatch.setattr(
        "sneeze.slackbot_commands.run_slackbot",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    command.run()

    env = read_env_file(env_path)
    assert env["SAMPLE_SLACK_BOT_TOKEN"] == "xoxb-existing"
    assert env["SAMPLE_SLACK_APP_TOKEN"] == "xapp-existing"
    assert env["SAMPLE_PLUGIN_SETTING"] == "keep-me"
    assert calls


def test_slackbot_dev_run_forwards_slackbot_overrides(tmp_path, monkeypatch):
    from sneeze.slackbot_commands import SlackbotDevBase

    workdir = tmp_path / "work"
    workdir.mkdir()
    team_config_path = tmp_path / "team.json"

    class SampleDevSlackbot(SlackbotDevBase):
        profile = make_profile(tmp_path)

    calls = []
    command = SampleDevSlackbot(None, None, None)
    command.args = ("run",)
    command._slack_domain = "sample.slack.com"
    command._app_id = "A123"
    command._client_id = "C123"
    command._bot_name = "samplebot"
    command._command_name = "/sample"
    command._codex_bin = "/tmp/codex-custom"
    command._codex_mode = "read-only"
    command._codex_model = "gpt-test"
    command._codex_profile = "dev"
    command._codex_workdir = str(workdir)
    command._codex_extra_args = "--approval-mode never"
    command._worker_count = 7
    command._mcp_server_url = "http://127.0.0.1:8765/mcp"
    command._team_config_path = str(team_config_path)
    monkeypatch.setattr(
        "sneeze.slackbot_commands.run_slackbot",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    command.run()

    kwargs = calls[0][1]
    assert kwargs["slack_domain"] == "sample.slack.com"
    assert kwargs["app_id"] == "A123"
    assert kwargs["client_id"] == "C123"
    assert kwargs["bot_name"] == "samplebot"
    assert kwargs["command_name"] == "/sample"
    assert kwargs["codex_bin"] == "/tmp/codex-custom"
    assert kwargs["codex_mode"] == "read-only"
    assert kwargs["codex_model"] == "gpt-test"
    assert kwargs["codex_profile"] == "dev"
    assert kwargs["codex_workdir"] == str(workdir)
    assert kwargs["codex_extra_args"] == "--approval-mode never"
    assert kwargs["worker_count"] == 7
    assert kwargs["mcp_server_url"] == "http://127.0.0.1:8765/mcp"
    assert kwargs["team_config_path"] == str(team_config_path)


def test_run_slackbot_applies_runtime_overrides(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    workdir = tmp_path / "work"
    workdir.mkdir()
    team_config_path = tmp_path / "team.json"
    captured = {}

    class FakeBot:
        def __init__(self, config, *, out=None):
            captured["config"] = config
            captured["out"] = out

        def run(self, *, max_runtime_seconds=None):
            captured["max_runtime_seconds"] = max_runtime_seconds

    monkeypatch.setattr("sneeze.slackbot.SlackSocketBot", FakeBot)

    run_slackbot(
        profile,
        slack_domain="sample.slack.com",
        app_id="A123",
        client_id="C123",
        bot_name="samplebot",
        command_name="sample",
        codex_bin="/tmp/codex-custom",
        codex_mode="read-only",
        codex_model="gpt-test",
        codex_profile="dev",
        codex_workdir=str(workdir),
        codex_extra_args="--approval-mode never",
        execution_mode="tmux",
        worker_count=7,
        mcp_server_url="http://127.0.0.1:8765/mcp",
        team_config_path=str(team_config_path),
        max_runtime_seconds=30,
    )

    config = captured["config"]
    assert config.slack_domain == "sample.slack.com"
    assert config.app_id == "A123"
    assert config.client_id == "C123"
    assert config.bot_name == "samplebot"
    assert config.command_name == "/sample"
    assert config.codex_bin == "/tmp/codex-custom"
    assert config.codex_mode == "read-only"
    assert config.codex_model == "gpt-test"
    assert config.codex_profile == "dev"
    assert config.codex_workdir == str(workdir.resolve())
    assert config.codex_extra_args == ("--approval-mode", "never")
    assert config.execution_mode == "tmux"
    assert config.worker_count == 7
    assert config.mcp_server_url == "http://127.0.0.1:8765/mcp"
    assert config.team_config_path == str(team_config_path)
    assert captured["max_runtime_seconds"] == 30


def test_slackbot_dev_run_does_not_persist_process_env_tokens(
    tmp_path,
    monkeypatch,
):
    from sneeze.slackbot_commands import SlackbotDevBase

    sample_profile = make_profile(tmp_path)
    scaffold_runtime(sample_profile)

    class SampleDevSlackbot(SlackbotDevBase):
        profile = sample_profile

    command = SampleDevSlackbot(None, None, None)
    command.args = ("run",)
    monkeypatch.setenv("SAMPLE_SLACK_BOT_TOKEN", "xoxb-temporary")
    monkeypatch.setenv("SAMPLE_SLACK_APP_TOKEN", "xapp-temporary")
    monkeypatch.setattr(
        "sneeze.slackbot_commands.run_slackbot",
        lambda *args, **kwargs: None,
    )

    command.run()

    env = read_env_file(tmp_path / "runtime" / ".env")
    assert env["SAMPLE_SLACK_BOT_TOKEN"] == ""
    assert env["SAMPLE_SLACK_APP_TOKEN"] == ""


def test_slackbot_dev_init_does_not_require_codex(tmp_path, monkeypatch):
    from sneeze.slackbot_commands import SlackbotDevBase

    class SampleDevSlackbot(SlackbotDevBase):
        profile = make_profile(tmp_path)

    def fake_required(requested, *names):
        if "codex" in names:
            raise AssertionError("init should not require codex")
        return f"/tmp/{names[0]}"

    command = SampleDevSlackbot(None, None, None)
    command.args = ("init",)
    command._slack_domain = "sample.slack.com"
    command._app_id = "A123"
    command._client_id = "C123"
    monkeypatch.setattr(
        "sneeze.slackbot_commands.resolve_required_executable",
        fake_required,
    )

    command.run()

    env = read_env_file(tmp_path / "runtime" / ".env")
    assert "SAMPLE_SLACKBOT_CODEX_BIN" in env
    assert env["SAMPLE_SLACK_TEAM_DOMAIN"] == "sample.slack.com"
    assert env["SAMPLE_SLACK_APP_ID"] == "A123"
    assert env["SAMPLE_SLACK_CLIENT_ID"] == "C123"


def test_slackbot_dev_status_does_not_require_tmux(tmp_path, monkeypatch):
    from io import StringIO

    from sneeze.slackbot_commands import SlackbotDevBase

    class SampleDevSlackbot(SlackbotDevBase):
        profile = make_profile(tmp_path)

    def fake_required(requested, *names):
        if "tmux" in names:
            raise AssertionError("status should not resolve tmux")
        return f"/tmp/{names[0]}"

    out = StringIO()
    command = SampleDevSlackbot(None, out, None)
    command.args = ("status",)
    monkeypatch.setattr(
        "sneeze.slackbot_commands.resolve_required_executable",
        fake_required,
    )

    command.run()

    assert '"app_slug": "sample"' in out.getvalue()


def test_tmux_dev_resolves_conda_manager_from_env_prefix(
    tmp_path, monkeypatch
):
    env_prefix = tmp_path / "miniforge3" / "envs" / "sample"
    mamba = tmp_path / "miniforge3" / "bin" / "mamba"
    mamba.parent.mkdir(parents=True)
    mamba.write_text("#!/bin/sh\n", encoding="utf-8")
    monkeypatch.setattr("sneeze.tmux_dev.sys.prefix", str(env_prefix))
    monkeypatch.setattr("sneeze.tmux_dev.shutil.which", lambda name: None)

    assert resolve_executable(None, "mamba") == str(mamba)
