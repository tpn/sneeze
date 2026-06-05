from __future__ import annotations

import json
import os

from .command import CommandError
from .console import (
    DEFAULT_AUTH_EMAIL_DOMAIN,
    DEFAULT_AUTH_EMAIL_HEADER,
    DEFAULT_CONSOLE_HOST,
    DEFAULT_CONSOLE_PORT,
    DEFAULT_CONSOLE_ROOT_PATH,
    DEFAULT_TMUX_SOCKET,
    ConsoleConfig,
    describe_console_config,
    list_tmux_sessions,
    normalize_root_path,
    parse_email_list,
    run_console_server,
)
from .invariant import (
    BoolInvariant,
    PositiveIntegerInvariant,
    StringInvariant,
)
from .slackbot import env_lookup, read_env_file, resolve_paths
from .slackbot_commands import ProfiledSlackbotCommand
from .tmux_dev import resolve_required_executable

CONSOLE_ACTIONS = ("run", "status", "sessions")


class SlackbotConsoleBase(ProfiledSlackbotCommand):
    """Run and inspect the profiled tmux web console."""

    _vargc_ = True

    host = None
    _host = None

    class HostArg(StringInvariant):
        _arg = "--host"
        _help = "Console bind host."
        _mandatory = False

    port = None
    _port = None

    class PortArg(PositiveIntegerInvariant):
        _arg = "--port"
        _help = "Console bind port."
        _mandatory = False

    root_path = None
    _root_path = None

    class RootPathArg(StringInvariant):
        _arg = "--root-path"
        _help = "Console HTTP mount path."
        _mandatory = False

    tmux_bin = None
    _tmux_bin = None

    class TmuxBinArg(StringInvariant):
        _arg = "--tmux-bin"
        _help = "tmux executable."
        _mandatory = False

    tmux_socket = None
    _tmux_socket = None

    class TmuxSocketArg(StringInvariant):
        _arg = "--tmux-socket"
        _help = "tmux socket name."
        _mandatory = False

    auth_email_header = None
    _auth_email_header = None

    class AuthEmailHeaderArg(StringInvariant):
        _arg = "--auth-email-header"
        _help = "Trusted SSO email header."
        _mandatory = False

    auth_email_domain = None
    _auth_email_domain = None

    class AuthEmailDomainArg(StringInvariant):
        _arg = "--auth-email-domain"
        _help = "Required authenticated email domain; empty disables check."
        _mandatory = False

    admin_emails = None
    _admin_emails = None

    class AdminEmailsArg(StringInvariant):
        _arg = "--admin-emails"
        _help = "Comma-separated emails allowed to type into tmux."
        _mandatory = False

    public_base_url = None
    _public_base_url = None

    class PublicBaseUrlArg(StringInvariant):
        _arg = "--public-base-url"
        _help = "Public scheme/host used when rendering status URLs."
        _mandatory = False

    allow_unauthenticated = None

    class AllowUnauthenticatedArg(BoolInvariant):
        _arg = "--allow-unauthenticated"
        _help = "Allow local unauthenticated console access."
        _mandatory = False
        _default = False

    def _action(self) -> str:
        return (self.args[0] if self.args else "status").strip().lower()

    def _env(self):
        paths = resolve_paths(self._profile(), **self._common())
        return read_env_file(paths.env_path)

    def _env_value(self, suffix: str, default: str | None = None):
        return env_lookup(self._profile(), self._env(), suffix, default)

    def _config(self) -> ConsoleConfig:
        host = (
            self._host
            or self.host
            or self._env_value("CONSOLE_HOST", DEFAULT_CONSOLE_HOST)
            or DEFAULT_CONSOLE_HOST
        )
        port_value = (
            self._port
            or self.port
            or self._env_value("CONSOLE_PORT", str(DEFAULT_CONSOLE_PORT))
            or DEFAULT_CONSOLE_PORT
        )
        root_path = normalize_root_path(
            self._root_path
            or self.root_path
            or self._env_value("CONSOLE_ROOT_PATH", DEFAULT_CONSOLE_ROOT_PATH)
        )
        tmux_socket = (
            self._tmux_socket
            or self.tmux_socket
            or self._env_value("CONSOLE_TMUX_SOCKET", DEFAULT_TMUX_SOCKET)
            or DEFAULT_TMUX_SOCKET
        )
        auth_email_header = (
            self._auth_email_header
            or self.auth_email_header
            or self._env_value(
                "CONSOLE_AUTH_EMAIL_HEADER",
                DEFAULT_AUTH_EMAIL_HEADER,
            )
            or DEFAULT_AUTH_EMAIL_HEADER
        )
        auth_email_domain = (
            self._auth_email_domain
            if self._auth_email_domain is not None
            else self.auth_email_domain
        )
        if auth_email_domain is None:
            auth_email_domain = self._env_value(
                "CONSOLE_AUTH_EMAIL_DOMAIN",
                DEFAULT_AUTH_EMAIL_DOMAIN,
            )
        admin_emails = (
            self._admin_emails
            or self.admin_emails
            or self._env_value("CONSOLE_ADMIN_EMAILS", "")
            or ""
        )
        tmux_bin = (
            self._tmux_bin
            or self.tmux_bin
            or self._env_value("CONSOLE_TMUX_BIN")
        )
        return ConsoleConfig(
            host=str(host),
            port=int(port_value),
            root_path=root_path,
            tmux_bin=resolve_required_executable(tmux_bin, "tmux"),
            tmux_socket=str(tmux_socket),
            auth_email_header=str(auth_email_header).lower(),
            auth_email_domain=str(auth_email_domain or "").lower(),
            admin_emails=parse_email_list(admin_emails),
            allow_unauthenticated=bool(self.allow_unauthenticated),
        )

    def _public_base_url_value(self) -> str | None:
        return (
            self._public_base_url
            or self.public_base_url
            or self._env_value("CONSOLE_PUBLIC_BASE_URL")
            or os.environ.get(
                self._profile().env_name("CONSOLE_PUBLIC_BASE_URL")
            )
            or None
        )

    def _json(self, value):
        self._out(json.dumps(value, indent=2, sort_keys=True))

    def run(self):
        action = self._action()
        config = self._config()
        if action == "run":
            run_console_server(config)
            return
        if action == "status":
            status = describe_console_config(
                config,
                public_base_url=self._public_base_url_value(),
            )
            status["sessions"] = [
                item["name"] for item in list_tmux_sessions(config)
            ]
            self._json(status)
            return
        if action == "sessions":
            self._json(list_tmux_sessions(config))
            return
        expected = ", ".join(CONSOLE_ACTIONS)
        raise CommandError(
            f"unknown console action: {action}; expected one of: {expected}"
        )
