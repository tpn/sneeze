from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

from .command import CommandError
from .commandinvariant import InvariantAwareCommand
from .invariant import (
    BoolInvariant,
    NonNegativeIntegerInvariant,
    PositiveIntegerInvariant,
    StringInvariant,
)
from .slackbot import (
    SLACK_APP_TOKEN_ENV,
    SLACK_BOT_TOKEN_ENV,
    SlackbotProfile,
    SlackbotRoute,
    bind_agent_tmux_thread,
    enqueue_ingress,
    env_lookup,
    install_service,
    list_agent_tmux_jobs,
    list_schedules,
    query_agent_tmux_thread,
    query_status,
    read_env_file,
    read_slack_thread,
    remove_schedule,
    remove_service,
    render_thread_transcript,
    resolve_paths,
    run_schedule,
    run_slackbot,
    scaffold_runtime,
    service_status,
    upsert_schedule,
)
from .tmux_dev import (
    TMUX_ACTIONS,
    TmuxDevCommandMixin,
    resolve_required_executable,
)


class ProfiledSlackbotCommand(InvariantAwareCommand):
    profile: SlackbotProfile | None = None
    run_subcommand = "slackbot-run"
    schedule_run_subcommand = "slackbot-schedule-run"

    runtime_root = None
    _runtime_root = None

    class RuntimeRootArg(StringInvariant):
        _arg = "--runtime-root"
        _help = "Slackbot runtime root."
        _mandatory = False

    env_path = None
    _env_path = None

    class EnvPathArg(StringInvariant):
        _arg = "--env-path"
        _help = "Slackbot env file."
        _mandatory = False

    state_dir = None
    _state_dir = None

    class StateDirArg(StringInvariant):
        _arg = "--state-dir"
        _help = "Slackbot state directory."
        _mandatory = False

    system_prompt_path = None
    _system_prompt_path = None

    class SystemPromptPathArg(StringInvariant):
        _arg = "--system-prompt-path"
        _help = "Slackbot system prompt path."
        _mandatory = False

    unit_name = None
    _unit_name = None

    class UnitNameArg(StringInvariant):
        _arg = "--unit-name"
        _help = "Slackbot service unit name."
        _mandatory = False

    def _profile(self) -> SlackbotProfile:
        if self.profile is None:
            raise CommandError("Slackbot command profile is not configured")
        return self.profile

    def _common(self):
        return {
            "runtime_root": self._runtime_root or self.runtime_root,
            "env_path": self._env_path or self.env_path,
            "state_dir": self._state_dir or self.state_dir,
            "system_prompt_path": (
                self._system_prompt_path or self.system_prompt_path
            ),
            "unit_name": self._unit_name or self.unit_name,
        }

    def _json(self, value):
        self._out(json.dumps(value, indent=2, sort_keys=True))

    def _resolved_cli_bin(self, value=None):
        requested = value or sys.argv[0]
        resolved = shutil.which(requested)
        if resolved:
            return os.path.abspath(resolved)
        if os.path.isabs(requested) and os.path.exists(requested):
            return requested
        raise CommandError(f"Cannot resolve CLI executable: {requested}")

    def _sibling_subcommand(self, canonical_name):
        display_name = getattr(self, "cli_display_name", None) or self.name
        for suffix in (
            "slackbot-service-install",
            "slackbot-schedule-upsert",
        ):
            if display_name.endswith(suffix):
                return f"{display_name[: -len(suffix)]}{canonical_name}"
        raise CommandError(
            f"Cannot derive sibling Slackbot command from {display_name}"
        )


class SlackbotInitBase(ProfiledSlackbotCommand):
    slack_domain = None
    _slack_domain = None

    class SlackDomainArg(StringInvariant):
        _arg = "--slack-domain"
        _help = "Slack team domain."
        _mandatory = False

    app_id = None
    _app_id = None

    class AppIdArg(StringInvariant):
        _arg = "--app-id"
        _help = "Slack app ID."
        _mandatory = False

    client_id = None
    _client_id = None

    class ClientIdArg(StringInvariant):
        _arg = "--client-id"
        _help = "Slack client ID."
        _mandatory = False

    bot_name = None
    _bot_name = None

    class BotNameArg(StringInvariant):
        _arg = "--bot-name"
        _help = "Bot display name."
        _mandatory = False

    command_name = None
    _command_name = None

    class CommandNameArg(StringInvariant):
        _arg = "--command-name"
        _help = "Slash command name."
        _mandatory = False

    codex_bin = None
    _codex_bin = None

    class CodexBinArg(StringInvariant):
        _arg = "--codex-bin"
        _help = "Codex executable."
        _mandatory = False

    codex_mode = None
    _codex_mode = None

    class CodexModeArg(StringInvariant):
        _arg = "--codex-mode"
        _help = "Codex sandbox mode."
        _mandatory = False

    codex_model = None
    _codex_model = None

    class CodexModelArg(StringInvariant):
        _arg = "--codex-model"
        _help = "Codex model."
        _mandatory = False

    codex_profile = None
    _codex_profile = None

    class CodexProfileArg(StringInvariant):
        _arg = "--codex-profile"
        _help = "Codex config profile."
        _mandatory = False

    codex_workdir = None
    _codex_workdir = None

    class CodexWorkdirArg(StringInvariant):
        _arg = "--codex-workdir"
        _help = "Codex working directory."
        _mandatory = False

    codex_extra_args = None
    _codex_extra_args = None

    class CodexExtraArgsArg(StringInvariant):
        _arg = "--codex-extra-args"
        _help = "Extra Codex CLI arguments."
        _mandatory = False

    worker_count = None
    _worker_count = None

    class WorkerCountArg(PositiveIntegerInvariant):
        _arg = "--worker-count"
        _help = "Slackbot worker count."
        _mandatory = False

    mcp_server_url = None
    _mcp_server_url = None

    class McpServerUrlArg(StringInvariant):
        _arg = "--mcp-server-url"
        _help = "Default MCP server URL."
        _mandatory = False

    team_config_path = None
    _team_config_path = None

    class TeamConfigPathArg(StringInvariant):
        _arg = "--team-config-path"
        _help = "Team registry/config path."
        _mandatory = False

    def run(self):
        result = scaffold_runtime(
            self._profile(),
            **self._common(),
            slack_domain=self._slack_domain or self.slack_domain,
            app_id=self._app_id or self.app_id,
            client_id=self._client_id or self.client_id,
            bot_name=self._bot_name or self.bot_name,
            command_name=self._command_name or self.command_name,
            codex_bin=self._codex_bin or self.codex_bin,
            codex_mode=self._codex_mode or self.codex_mode,
            codex_model=self._codex_model or self.codex_model,
            codex_profile=self._codex_profile or self.codex_profile,
            codex_workdir=self._codex_workdir or self.codex_workdir,
            codex_extra_args=(
                self._codex_extra_args or self.codex_extra_args
            ),
            worker_count=self._worker_count or self.worker_count,
            mcp_server_url=self._mcp_server_url or self.mcp_server_url,
            team_config_path=(
                self._team_config_path or self.team_config_path
            ),
            out=self._err,
        )
        self._err(
            "Slack tokens are file/env-only; populate the generated env "
            "file instead of passing tokens on the command line."
        )
        self._json(result)


class SlackbotStatusBase(ProfiledSlackbotCommand):
    def run(self):
        self._json(query_status(self._profile(), **self._common()))


class SlackbotRunBase(ProfiledSlackbotCommand):
    worker_count = None
    _worker_count = None

    class WorkerCountArg(PositiveIntegerInvariant):
        _arg = "--worker-count"
        _help = "Override worker count."
        _mandatory = False

    max_runtime_seconds = None
    _max_runtime_seconds = None

    class MaxRuntimeSecondsArg(NonNegativeIntegerInvariant):
        _arg = "--max-runtime-seconds"
        _help = "Stop after this many seconds; 0 means no limit."
        _mandatory = False

    def run(self):
        max_seconds = (
            self._max_runtime_seconds
            if self._max_runtime_seconds is not None
            else self.max_runtime_seconds
        )
        if max_seconds == 0:
            max_seconds = None
        run_slackbot(
            self._profile(),
            **self._common(),
            worker_count=self._worker_count or self.worker_count,
            max_runtime_seconds=max_seconds,
            out=self._out,
        )


class SlackbotDevBase(TmuxDevCommandMixin, SlackbotInitBase):
    """Manage a profiled Slackbot development runtime."""

    max_runtime_seconds = None
    _max_runtime_seconds = None

    class MaxRuntimeSecondsArg(NonNegativeIntegerInvariant):
        _arg = "--max-runtime-seconds"
        _help = "Stop run/smoke after this many seconds; 0 means no limit."
        _mandatory = False

    def _app_slug(self) -> str:
        return self._profile().app_slug

    def _env_prefix(self) -> str:
        return self._profile().env_prefix

    def _runtime_root_value(self) -> str:
        return resolve_paths(self._profile(), **self._common()).runtime_root

    def _env_path_value(self) -> str:
        return resolve_paths(self._profile(), **self._common()).env_path

    def _env_file_value(self, key: str) -> str | None:
        return env_lookup(
            self._profile(), read_env_file(self._env_path_value()), key
        )

    def _managed_env_file_value(self, key: str) -> str | None:
        env = read_env_file(self._env_path_value())
        prefixed = self._profile().env_name(key)
        return env.get(prefixed) or env.get(key) or None

    def _codex_workdir_value(self) -> str:
        value = (
            self._codex_workdir
            or self.codex_workdir
            or self._env_file_value("SLACKBOT_CODEX_WORKDIR")
            or self._profile().default_codex_workdir
        )
        if not value:
            return os.getcwd()
        return str(Path(value).expanduser().resolve())

    def _codex_bin_value(self) -> str:
        value = self._codex_bin or self.codex_bin
        if value:
            return value
        env_value = env_lookup(
            self._profile(),
            os.environ,
            "SLACKBOT_CODEX_BIN",
        )
        if env_value:
            return env_value
        env_value = os.environ.get(f"{self._env_prefix()}_CODEX_BIN")
        if env_value:
            return env_value
        env_file_value = self._env_file_value("SLACKBOT_CODEX_BIN")
        if env_file_value:
            return env_file_value
        candidate = Path(sys.prefix) / "bin" / "codex"
        if candidate.exists():
            return str(candidate)
        return resolve_required_executable(None, "codex")

    def _scaffold_codex_bin_value(self) -> str | None:
        value = self._codex_bin or self.codex_bin
        if value:
            return value
        env_value = env_lookup(
            self._profile(),
            os.environ,
            "SLACKBOT_CODEX_BIN",
        )
        if env_value:
            return env_value
        env_value = os.environ.get(f"{self._env_prefix()}_CODEX_BIN")
        if env_value:
            return env_value
        return self._env_file_value("SLACKBOT_CODEX_BIN")

    def _tmux_session_env_name(self) -> str:
        return f"{self._env_prefix()}_SLACKBOT_TMUX_SESSION"

    def _tmux_session_default(self) -> str:
        return f"{self._app_slug()}-slackbot-dev"

    def _log_path_env_name(self) -> str:
        return f"{self._env_prefix()}_SLACKBOT_DEV_LOG"

    def _log_path_default(self) -> str:
        return str(
            Path(self._runtime_root_value()) / "var" / "dev-slackbot.log"
        )

    def _log_lines_env_name(self) -> str:
        return f"{self._env_prefix()}_SLACKBOT_LOG_LINES"

    def _tmux_root(self) -> str:
        return self._codex_workdir_value()

    def _raw_max_seconds_value(self) -> int | None:
        return (
            self._max_runtime_seconds
            if self._max_runtime_seconds is not None
            else self.max_runtime_seconds
        )

    def _max_seconds_value(self) -> int | None:
        value = self._raw_max_seconds_value()
        if value == 0:
            return None
        return value

    @staticmethod
    def _append_child_arg(args: list[str], flag: str, value) -> None:
        if value is None or value == "":
            return
        args.extend([flag, str(value)])

    def _scaffold(self) -> None:
        scaffold_runtime(
            self._profile(),
            **self._common(),
            slack_domain=self._slack_domain or self.slack_domain,
            app_id=self._app_id or self.app_id,
            client_id=self._client_id or self.client_id,
            bot_token=self._managed_env_file_value(SLACK_BOT_TOKEN_ENV),
            app_token=self._managed_env_file_value(SLACK_APP_TOKEN_ENV),
            bot_name=self._bot_name or self.bot_name,
            command_name=self._command_name or self.command_name,
            codex_bin=self._scaffold_codex_bin_value(),
            codex_mode=self._codex_mode or self.codex_mode,
            codex_model=self._codex_model or self.codex_model,
            codex_profile=self._codex_profile or self.codex_profile,
            codex_workdir=self._codex_workdir_value(),
            codex_extra_args=(
                self._codex_extra_args or self.codex_extra_args
            ),
            worker_count=self._worker_count or self.worker_count,
            mcp_server_url=self._mcp_server_url or self.mcp_server_url,
            team_config_path=(
                self._team_config_path or self.team_config_path
            ),
            out=self._err,
        )
        self._err(
            "Slack tokens are file/env-only; populate the generated env "
            "file instead of passing tokens on the command line."
        )

    def _child_command(self) -> list[str]:
        args = [
            self._mamba_bin_value(),
            "run",
            "-n",
            self._env_name_value(),
            self._cli_bin_value(),
            *self._child_cli_args(),
            "run",
        ]
        for flag, value in (
            ("--runtime-root", self._runtime_root_value()),
            ("--env-path", self._env_path_value()),
            ("--state-dir", self._state_dir or self.state_dir),
            (
                "--system-prompt-path",
                self._system_prompt_path or self.system_prompt_path,
            ),
            ("--unit-name", self._unit_name or self.unit_name),
            ("--slack-domain", self._slack_domain or self.slack_domain),
            ("--app-id", self._app_id or self.app_id),
            ("--client-id", self._client_id or self.client_id),
            ("--bot-name", self._bot_name or self.bot_name),
            ("--command-name", self._command_name or self.command_name),
            ("--codex-bin", self._scaffold_codex_bin_value()),
            ("--codex-mode", self._codex_mode or self.codex_mode),
            ("--codex-model", self._codex_model or self.codex_model),
            ("--codex-profile", self._codex_profile or self.codex_profile),
            ("--codex-workdir", self._codex_workdir_value()),
            (
                "--codex-extra-args",
                self._codex_extra_args or self.codex_extra_args,
            ),
            ("--worker-count", self._worker_count or self.worker_count),
            ("--mcp-server-url", self._mcp_server_url or self.mcp_server_url),
            (
                "--team-config-path",
                self._team_config_path or self.team_config_path,
            ),
            (
                "--max-runtime-seconds",
                (
                    self._max_runtime_seconds
                    if self._max_runtime_seconds is not None
                    else self.max_runtime_seconds
                ),
            ),
        ):
            self._append_child_arg(args, flag, value)
        return args

    def _run_slackbot(
        self, *, max_runtime_seconds: int | None = None
    ) -> None:
        self._ensure_scaffolded()
        run_slackbot(
            self._profile(),
            **self._common(),
            slack_domain=self._slack_domain or self.slack_domain,
            app_id=self._app_id or self.app_id,
            client_id=self._client_id or self.client_id,
            bot_name=self._bot_name or self.bot_name,
            command_name=self._command_name or self.command_name,
            codex_bin=self._codex_bin or self.codex_bin,
            codex_mode=self._codex_mode or self.codex_mode,
            codex_model=self._codex_model or self.codex_model,
            codex_profile=self._codex_profile or self.codex_profile,
            codex_workdir=self._codex_workdir or self.codex_workdir,
            codex_extra_args=(
                self._codex_extra_args or self.codex_extra_args
            ),
            worker_count=self._worker_count or self.worker_count,
            mcp_server_url=self._mcp_server_url or self.mcp_server_url,
            team_config_path=(
                self._team_config_path or self.team_config_path
            ),
            max_runtime_seconds=max_runtime_seconds,
            out=self._out,
        )

    def _ensure_scaffolded(self) -> None:
        if Path(self._env_path_value()).exists():
            return
        self._scaffold()

    def _smoke(self) -> None:
        self._ensure_scaffolded()
        self._json(query_status(self._profile(), **self._common()))
        max_seconds = self._max_seconds_value()
        if max_seconds is None and self._raw_max_seconds_value() is None:
            max_seconds = 3
        self._run_slackbot(max_runtime_seconds=max_seconds)

    def run(self):
        self._run_dev_action(
            {
                "init": self._scaffold,
                "status": lambda: self._json(
                    query_status(self._profile(), **self._common())
                ),
                "run": lambda: self._run_slackbot(
                    max_runtime_seconds=self._max_seconds_value()
                ),
                "smoke": self._smoke,
            },
            command_name="dev-slackbot",
            choices=("init", "status", "run", "smoke", *TMUX_ACTIONS),
        )


class SlackbotServiceInstallBase(ProfiledSlackbotCommand):
    cli_bin = None
    _cli_bin = None

    class CliBinArg(StringInvariant):
        _arg = "--cli-bin"
        _help = "CLI executable for the service."
        _mandatory = False

    enable = None

    class EnableArg(BoolInvariant):
        _arg = "--enable"
        _help = "Enable the user service."
        _mandatory = False
        _default = False

    restart = None
    _restart = None

    class RestartArg(StringInvariant):
        _arg = "--restart"
        _help = "systemd Restart policy."
        _mandatory = False

    restart_sec = None
    _restart_sec = None

    class RestartSecArg(PositiveIntegerInvariant):
        _arg = "--restart-sec"
        _help = "systemd RestartSec value."
        _mandatory = False

    def run(self):
        result = install_service(
            self._profile(),
            **self._common(),
            cli_bin=self._resolved_cli_bin(
                self._cli_bin or self.cli_bin or sys.argv[0]
            ),
            run_subcommand=self._sibling_subcommand(self.run_subcommand),
            restart=self._restart or self.restart or "on-failure",
            restart_sec=self._restart_sec or self.restart_sec or 10,
            enable=bool(self.enable),
        )
        self._json(result)


class SlackbotServiceStatusBase(ProfiledSlackbotCommand):
    def run(self):
        self._json(service_status(self._profile(), **self._common()))


class SlackbotServiceRemoveBase(ProfiledSlackbotCommand):
    def run(self):
        self._json(remove_service(self._profile(), **self._common()))


class SlackbotReadThreadBase(ProfiledSlackbotCommand):
    _argc_ = 1

    def run(self):
        from .slackbot import load_config

        config = load_config(self._profile(), **self._common())
        messages = read_slack_thread(config, self.args[0])
        self._out(render_thread_transcript(messages))


class SlackbotRouteCommand(ProfiledSlackbotCommand):
    channel_id = None
    _channel_id = None

    class ChannelIdArg(StringInvariant):
        _arg = "--channel-id"
        _help = "Slack channel ID."
        _mandatory = False

    dm_user_id = None
    _dm_user_id = None

    class DmUserIdArg(StringInvariant):
        _arg = "--dm-user-id"
        _help = "Slack DM user ID."
        _mandatory = False

    mention_user_ids = None
    _mention_user_ids = None

    class MentionUserIdsArg(StringInvariant):
        _arg = "--mention-user-ids"
        _help = "Comma-separated Slack users to mention."
        _mandatory = False

    thread_ts = None
    _thread_ts = None

    class ThreadTsArg(StringInvariant):
        _arg = "--thread-ts"
        _help = "Slack thread timestamp."
        _mandatory = False

    execution_mode = None
    _execution_mode = None

    class ExecutionModeArg(StringInvariant):
        _arg = "--execution-mode"
        _help = "Execution mode: raw or tmux."
        _mandatory = False

    project = None
    _project = None

    class ProjectArg(StringInvariant):
        _arg = "--project"
        _help = "Project/session namespace."
        _mandatory = False

    session = None
    _session = None

    class SessionArg(StringInvariant):
        _arg = "--session"
        _help = "Conversation session key."
        _mandatory = False

    def _route(self):
        mention_value = self._mention_user_ids or self.mention_user_ids
        mentions = ()
        if mention_value:
            mentions = tuple(
                item.strip()
                for item in mention_value.split(",")
                if item.strip()
            )
        return SlackbotRoute(
            channel_id=self._channel_id or self.channel_id,
            dm_user_id=self._dm_user_id or self.dm_user_id,
            mention_user_ids=mentions,
            thread_ts=self._thread_ts or self.thread_ts,
        )

    def _delivery_route(self):
        route = self._route()
        if not route.channel_id and not route.dm_user_id:
            raise CommandError("--channel-id or --dm-user-id is required")
        return route


class SlackbotEnqueueMessageBase(SlackbotRouteCommand):
    _vargc_ = True
    _disable_interspersed_args_ = True

    def run(self):
        text = " ".join(self.args).strip()
        if not text:
            raise CommandError("message text is required")
        path = enqueue_ingress(
            self._profile(),
            **self._common(),
            kind="slack_message",
            text=text,
            route=self._delivery_route(),
        )
        self._out(path)


class SlackbotEnqueueCodexBase(SlackbotRouteCommand):
    _vargc_ = True
    _disable_interspersed_args_ = True

    slack_user_id = None
    _slack_user_id = None

    class SlackUserIdArg(StringInvariant):
        _arg = "--slack-user-id"
        _help = "Slack user ID whose user-scoped config/secrets apply."
        _mandatory = False

    system_scoped = None

    class SystemScopedArg(BoolInvariant):
        _arg = "--system-scoped"
        _help = "Force a system-scoped Codex run without user secrets."
        _mandatory = False
        _default = False

    def run(self):
        text = " ".join(self.args).strip()
        if not text:
            raise CommandError("Codex prompt is required")
        slack_user_id = self._slack_user_id or self.slack_user_id
        path = enqueue_ingress(
            self._profile(),
            **self._common(),
            kind="codex_prompt",
            text=text,
            route=self._delivery_route(),
            execution_mode=(
                self._execution_mode or self.execution_mode or "raw"
            ),
            project=self._project or self.project,
            session=self._session or self.session,
            slack_user_id=slack_user_id,
            system_scoped=(
                True if self.system_scoped or not slack_user_id else None
            ),
        )
        self._out(path)


class SlackbotScheduleUpsertBase(SlackbotRouteCommand):
    _vargc_ = True
    _disable_interspersed_args_ = True

    name = None
    _name = None

    class NameArg(StringInvariant):
        _arg = "--name"
        _help = "Schedule name."
        _mandatory = True

    on_calendar = None
    _on_calendar = None

    class OnCalendarArg(StringInvariant):
        _arg = "--on-calendar"
        _help = "systemd OnCalendar expression."
        _mandatory = True

    workdir = None
    _workdir = None

    class WorkdirArg(StringInvariant):
        _arg = "--workdir"
        _help = "Command working directory."
        _mandatory = True

    notify_kind = None
    _notify_kind = None

    class NotifyKindArg(StringInvariant):
        _arg = "--notify-kind"
        _help = "none, slack_message, or codex_prompt."
        _mandatory = False

    no_install_timer = None

    class NoInstallTimerArg(BoolInvariant):
        _arg = "--no-install-timer"
        _help = "Do not install the user systemd timer."
        _mandatory = False
        _default = False

    def run(self):
        if not self.args:
            raise CommandError("schedule command is required")
        result = upsert_schedule(
            self._profile(),
            **self._common(),
            name=self._name or self.name,
            on_calendar=self._on_calendar or self.on_calendar,
            command=self.args,
            workdir=self._workdir or self.workdir,
            notify_kind=self._notify_kind or self.notify_kind or "none",
            route=self._route(),
            cli_bin=self._resolved_cli_bin(sys.argv[0]),
            run_subcommand=self._sibling_subcommand(
                self.schedule_run_subcommand
            ),
            execution_mode=(
                self._execution_mode or self.execution_mode or "raw"
            ),
            project=self._project or self.project,
            session=self._session or self.session,
            install_timer=not self.no_install_timer,
        )
        self._json(result)


class SlackbotScheduleRemoveBase(ProfiledSlackbotCommand):
    name = None
    _name = None

    class NameArg(StringInvariant):
        _arg = "--name"
        _help = "Schedule name."
        _mandatory = True

    def run(self):
        self._json(
            remove_schedule(
                self._profile(),
                **self._common(),
                name=self._name or self.name,
            )
        )


class SlackbotScheduleListBase(ProfiledSlackbotCommand):
    def run(self):
        self._json(
            [
                {
                    "name": item.name,
                    "on_calendar": item.on_calendar,
                    "command": list(item.command),
                    "workdir": item.workdir,
                    "notify_kind": item.notify_kind,
                }
                for item in list_schedules(self._profile(), **self._common())
            ]
        )


class SlackbotScheduleRunBase(ProfiledSlackbotCommand):
    name = None
    _name = None

    class NameArg(StringInvariant):
        _arg = "--name"
        _help = "Schedule name."
        _mandatory = True

    def run(self):
        self._json(
            run_schedule(
                self._profile(),
                **self._common(),
                name=self._name or self.name,
            )
        )


class SlackbotBindSessionBase(ProfiledSlackbotCommand):
    channel_id = None
    _channel_id = None

    class ChannelIdArg(StringInvariant):
        _arg = "--channel-id"
        _help = "Slack channel ID."
        _mandatory = True

    thread_ts = None
    _thread_ts = None

    class ThreadTsArg(StringInvariant):
        _arg = "--thread-ts"
        _help = "Slack thread timestamp."
        _mandatory = True

    host = None
    _host = None

    class HostArg(StringInvariant):
        _arg = "--host"
        _help = "Host containing the tmux session."
        _mandatory = True

    tmux_session = None
    _tmux_session = None

    class TmuxSessionArg(StringInvariant):
        _arg = "--tmux-session"
        _help = "tmux session name."
        _mandatory = True

    def run(self):
        self._json(
            bind_agent_tmux_thread(
                self._profile(),
                **self._common(),
                channel_id=self._channel_id or self.channel_id,
                thread_ts=self._thread_ts or self.thread_ts,
                host=self._host or self.host,
                tmux_session=self._tmux_session or self.tmux_session,
            )
        )


class SlackbotThreadStatusBase(ProfiledSlackbotCommand):
    channel_id = None
    _channel_id = None

    class ChannelIdArg(StringInvariant):
        _arg = "--channel-id"
        _help = "Slack channel ID."
        _mandatory = True

    thread_ts = None
    _thread_ts = None

    class ThreadTsArg(StringInvariant):
        _arg = "--thread-ts"
        _help = "Slack thread timestamp."
        _mandatory = True

    def run(self):
        self._json(
            query_agent_tmux_thread(
                self._profile(),
                **self._common(),
                channel_id=self._channel_id or self.channel_id,
                thread_ts=self._thread_ts or self.thread_ts,
            )
            or {}
        )


class SlackbotNurseJobsBase(ProfiledSlackbotCommand):
    def run(self):
        self._json(list_agent_tmux_jobs(self._profile(), **self._common()))
