import os
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from sneeze.slackbot import (
    CodexRunner,
    SlackbotError,
    SlackbotProfile,
    SlackbotRoute,
    SlackSocketBot,
    bind_agent_tmux_thread,
    chunk_text,
    enqueue_ingress,
    extract_codex_session_id,
    list_schedules,
    load_config,
    parse_slack_thread_permalink,
    post_slack_message,
    query_agent_tmux_thread,
    query_status,
    read_json,
    render_launchd_service,
    render_systemd_service,
    route_from_dict,
    route_to_dict,
    run_schedule,
    scaffold_runtime,
    strip_bot_mentions,
    upsert_schedule,
)


def make_profile(tmp_path):
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
    )

    payload = read_json(path, {})
    assert payload["kind"] == "codex_prompt"
    assert payload["route"]["channel_id"] == "C123"
    assert payload["route"]["response_url"] == "https://response"
    assert route_to_dict(route_from_dict(payload["route"])) == {
        "channel_id": "C123",
        "dm_user_id": None,
        "mention_user_ids": [],
        "thread_ts": "1.23",
    }
    assert os.stat(path).st_mode & 0o777 == 0o600


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


def test_dispatch_uses_socket_mode_envelope_type(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    bot.bot_user_id = "UBOT"
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
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


def test_mpim_message_preserves_thread_context(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
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


def test_dm_thread_reply_preserves_thread_context(tmp_path, monkeypatch):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(
        replace(load_config(profile), allowed_user_ids=("U111",))
    )
    prompts = []

    class FakeRunner:
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
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


def test_placeholder_failure_replies_in_original_thread(
    tmp_path, monkeypatch
):
    profile = make_profile(tmp_path)
    scaffold_runtime(profile, bot_token="xoxb-test", app_token="xapp-test")
    bot = SlackSocketBot(load_config(profile))
    calls = []

    class FakeRunner:
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
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
        def run(self, prompt, session_id=None):
            calls.append((prompt, session_id))
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
        project="user",
        session="weekly",
    )
    bot.drain_ingress()
    enqueue_ingress(
        profile,
        kind="codex_prompt",
        text="continue",
        route=SlackbotRoute(channel_id="C123"),
        project="user",
        session="weekly",
    )
    bot.drain_ingress()

    assert calls[0][1] is None
    assert calls[1][1] == "codex-session-1"
    assert "Project:\nuser" in calls[0][0]


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
