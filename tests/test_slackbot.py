import json
import os
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from sneeze.slackbot import (
    CHILD_ENV_PASSTHROUGH,
    USER_CONFIG_MODAL_CALLBACK_ID,
    USER_CONFIG_OPEN_ACTION_ID,
    CodexRunner,
    SlackbotError,
    SlackbotProfile,
    SlackbotRoute,
    SlackbotStaticResponse,
    SlackbotUserConfigSecretField,
    SlackSocketBot,
    bind_agent_tmux_thread,
    build_user_config_view,
    chunk_text,
    enqueue_ingress,
    extract_codex_session_id,
    list_schedules,
    load_config,
    merge_child_process_env,
    parse_slack_thread_permalink,
    post_slack_blocks,
    post_slack_message,
    query_agent_tmux_thread,
    query_status,
    read_env_file,
    read_json,
    read_user_secret_env,
    render_launchd_service,
    render_systemd_service,
    route_from_dict,
    route_to_dict,
    run_schedule,
    run_slackbot,
    safe_slack_storage_id,
    save_user_config_submission,
    scaffold_runtime,
    static_response_text_from_profile,
    store_user_secret_token,
    strip_bot_mentions,
    upsert_schedule,
    user_config_mode_from_text,
    user_config_secret_field,
)
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

    scaffold_runtime(profile)

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
        default_allowed_channel_ids=("C999",),
    )

    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = load_config(profile)

    assert config.allowed_channel_ids == ("C999",)
    env_path = Path(config.paths.env_path)
    assert "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=C999" in env_path.read_text(
        encoding="utf-8"
    )
    assert SlackSocketBot(config)._is_authorized("U222", "C999")

    env_path.write_text(
        env_path.read_text(encoding="utf-8").replace(
            "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=C999",
            "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=",
        ),
        encoding="utf-8",
    )
    cleared_config = load_config(profile)

    assert cleared_config.allowed_channel_ids == ()
    assert not SlackSocketBot(cleared_config)._is_authorized("U222", "C999")

    scaffold_runtime(profile)

    assert "SAMPLE_SLACKBOT_ALLOWED_CHANNEL_IDS=\n" in env_path.read_text(
        encoding="utf-8"
    )


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


def test_enqueue_ingress_channel_codex_defaults_system_scoped(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile)

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="hello",
        route=SlackbotRoute(channel_id="C123"),
    )

    payload = read_json(path, {})
    assert payload["kind"] == "codex_prompt"
    assert payload["slack_user_id"] is None
    assert payload["system_scoped"] is True


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
    assert read_json(first, {})["system_scoped"] is False


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
    assert f'WorkingDirectory="{tmp_path}"' in service_text


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
    )
    binding = query_agent_tmux_thread(
        profile,
        channel_id="C123",
        thread_ts="1.23",
    )

    assert binding["host"] == "dgx"
    assert binding["tmux_session"] == "codex-work"


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
    assert 'EnvironmentFile="' in text
    assert not any(
        word in text for word in ("legacy-internal-tool", "ticket-system")
    )


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
    monkeypatch.setenv("AUTH_HEADER", "ambient-auth")
    monkeypatch.setenv("BEARER_AUTH", "ambient-bearer")
    monkeypatch.setenv("ACME_TOKEN", "ambient-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "ambient-gh")
    monkeypatch.setenv("MY_API_KEYS", "ambient-keys")
    monkeypatch.setenv("SSH_AUTH_SOCK", "/tmp/ssh-agent.sock")
    monkeypatch.setattr(
        "sneeze.slackbot.CHILD_ENV_PASSTHROUGH",
        {
            *CHILD_ENV_PASSTHROUGH,
            "ACME_API_KEY",
            "ACME_TOKEN",
            "AUTH_HEADER",
            "BEARER_AUTH",
            "GITHUB_TOKEN",
            "MY_API_KEYS",
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
    assert scrubbed["AUTH_HEADER"] == "ambient-auth"
    assert scrubbed["BEARER_AUTH"] == "ambient-bearer"
    assert "MY_API_KEYS" not in scrubbed
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
        replace(load_config(profile), allowed_user_ids=("U999",))
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


def test_dm_config_button_bypasses_codex_allowlist(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U999",))
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
        replace(load_config(profile), allowed_user_ids=("U999",))
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


def test_dm_static_response_bypasses_codex_allowlist(tmp_path, monkeypatch):
    profile = replace(
        make_profile(tmp_path),
        static_responses=(
            SlackbotStaticResponse(names=("help",), text="catalog"),
        ),
    )
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U999",))
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


def test_unauthorized_view_submission_acknowledges_with_errors(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    config = replace(load_config(profile), allowed_user_ids=("U999",))
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
    config = replace(load_config(profile), allowed_user_ids=("U999",))
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
    assert "use_scrub_allowlist" not in calls[0][1]
    assert calls[0][1]["extra_env"]["GH_TOKEN"] == "gh-user-secret"
    assert calls[0][1]["extra_env"]["ACME_TOKEN"] == "acme-user-secret"


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

    def fake_run_codex(text, route, *, slack_user_id=None):
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

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("unsafe Slack user ID should not run Codex")

    bot.runner = FakeRunner()
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
    assert bot.conversations.get("user:../bad:slack:C123:2.000001") is None


def test_run_codex_rejects_unsafe_dm_user_id_before_session_key(
    tmp_path,
    monkeypatch,
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    posts = []

    class FakeRunner:
        def run(self, prompt, session_id=None, **kwargs):
            raise AssertionError("unsafe DM user ID should not run Codex")

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
        "I could not verify your Codex-backed work session settings, so I "
        "did not start a session."
    ]
    assert bot.conversations.get("dm:D123:../bad") is None


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


def test_drain_ingress_moves_user_secret_failures_to_error(
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


def test_enqueue_codex_derives_slack_user_id_from_dm_route(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    path = enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="summarize",
        route=SlackbotRoute(channel_id="D123", dm_user_id="U111"),
    )

    assert read_json(path, {})["slack_user_id"] == "U111"


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
        lambda config, route, text: {"ts": "2.000001"},
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


def test_agent_tmux_commands_require_tmux_prefix(tmp_path, monkeypatch):
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
    assert bot._maybe_handle_agent_tmux("tmux status", route)
    assert calls


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


def test_enqueue_rejects_unimplemented_tmux_mode(tmp_path):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")

    with pytest.raises(ValueError, match="Expected one of: raw"):
        enqueue_ingress(
            profile,
            kind="codex_prompt",
            text="summarize",
            route=SlackbotRoute(channel_id="C123"),
            execution_mode="tmux",
            system_scoped=True,
        )


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
