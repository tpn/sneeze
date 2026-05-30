from __future__ import annotations

import json
import os
from pathlib import Path

from .command import CommandError
from .commandinvariant import InvariantAwareCommand
from .invariant import PositiveIntegerInvariant, StringInvariant
from .mcp_server import (
    McpServerProfile,
    call_tool_sync,
    canonical_mcp_url,
    describe_tools,
    run_mcp_http_server,
)
from .tmux_dev import (
    TMUX_ACTIONS,
    TmuxDevCommandMixin,
)
from .xdg import xdg_state_dir


class ProfiledMcpCommand(InvariantAwareCommand):
    mcp_profile: McpServerProfile | None = None

    host = None
    _host = None

    class HostArg(StringInvariant):
        _arg = "--host"
        _help = "MCP server bind host."
        _mandatory = False

    port = None
    _port = None

    class PortArg(PositiveIntegerInvariant):
        _arg = "--port"
        _help = "MCP server bind port."
        _mandatory = False

    path = None
    _path = None

    class PathArg(StringInvariant):
        _arg = "--path"
        _help = "MCP HTTP mount path."
        _mandatory = False

    def _profile(self) -> McpServerProfile:
        if self.mcp_profile is None:
            raise CommandError("MCP command profile is not configured")
        return self.mcp_profile

    def _resolved_host(self) -> str:
        profile = self._profile()
        return self._host or self.host or profile.default_host

    def _resolved_port(self) -> int:
        profile = self._profile()
        return int(self._port or self.port or profile.default_port)

    def _resolved_path(self) -> str:
        profile = self._profile()
        return self._path or self.path or profile.default_path

    def _json(self, value):
        self._out(json.dumps(value, indent=2, sort_keys=True))


class McpRunBase(ProfiledMcpCommand):
    """Run a profiled streamable HTTP MCP server."""

    def run(self):
        run_mcp_http_server(
            self._profile(),
            host=self._resolved_host(),
            port=self._resolved_port(),
            path=self._resolved_path(),
        )


class McpStatusBase(ProfiledMcpCommand):
    """Show the profiled MCP server configuration."""

    def run(self):
        profile = self._profile()
        self._json(
            {
                "name": profile.name,
                "host": self._resolved_host(),
                "port": self._resolved_port(),
                "path": self._resolved_path(),
                "url": canonical_mcp_url(
                    self._resolved_host(),
                    self._resolved_port(),
                    self._resolved_path(),
                ),
                "tools": [tool["name"] for tool in describe_tools(profile)],
            }
        )


class McpToolsBase(ProfiledMcpCommand):
    """List profiled MCP tools."""

    def run(self):
        self._json(describe_tools(self._profile()))


class McpCallBase(ProfiledMcpCommand):
    """Call a profiled MCP tool directly without starting HTTP transport."""

    tool = None
    _tool = None

    class ToolArg(StringInvariant):
        _arg = "--tool"
        _help = "MCP tool name."

    arguments = None
    _arguments = None

    class ArgumentsArg(StringInvariant):
        _arg = "--arguments"
        _help = "Tool arguments as a JSON object."
        _mandatory = False

    def run(self):
        raw_arguments = self._arguments or self.arguments or "{}"
        try:
            arguments = json.loads(raw_arguments)
        except json.JSONDecodeError as exc:
            raise CommandError(f"invalid JSON arguments: {exc}") from exc
        if not isinstance(arguments, dict):
            raise CommandError("MCP tool arguments must be a JSON object")
        self._out(
            call_tool_sync(
                self._profile(),
                self._tool or self.tool,
                arguments,
            )
        )


class McpDevBase(TmuxDevCommandMixin, McpRunBase):
    """Manage a profiled MCP development runtime."""

    default_runtime_root = None
    default_app_slug = None

    runtime_root = None
    _runtime_root = None

    class RuntimeRootArg(StringInvariant):
        _arg = "--runtime-root"
        _help = "MCP runtime root."
        _mandatory = False

    def _app_slug(self) -> str:
        if self.default_app_slug:
            return self.default_app_slug
        return self._profile().name.removesuffix("-mcp").replace("_", "-")

    def _runtime_root_value(self) -> str:
        value = (
            self._runtime_root
            or self.runtime_root
            or os.environ.get(f"{self._env_prefix()}_MCP_RUNTIME_ROOT")
            or self.default_runtime_root
        )
        if value:
            return str(Path(value).expanduser())
        return str(xdg_state_dir(self._app_slug()) / "mcp")

    def _tmux_session_env_name(self) -> str:
        return f"{self._env_prefix()}_MCP_TMUX_SESSION"

    def _tmux_session_default(self) -> str:
        return f"{self._app_slug()}-mcp-dev"

    def _log_path_env_name(self) -> str:
        return f"{self._env_prefix()}_MCP_LOG_FILE"

    def _log_path_default(self) -> str:
        return str(
            Path(self._runtime_root_value()) / f"{self._profile().name}.log"
        )

    def _log_lines_env_name(self) -> str:
        return f"{self._env_prefix()}_MCP_LOG_LINES"

    def _runtime_env(self) -> dict[str, str]:
        return {}

    def _child_command(self) -> list[str]:
        args = [
            self._mamba_bin_value(),
            "run",
            "-n",
            self._env_name_value(),
        ]
        env_items = sorted(self._runtime_env().items())
        if env_items:
            args.append("env")
            args.extend(f"{name}={value}" for name, value in env_items)
        args.append(self._cli_bin_value())
        args.extend(self._child_cli_args())
        args.extend(
            [
                "run",
                "--host",
                self._resolved_host(),
                "--port",
                str(self._resolved_port()),
                "--path",
                self._resolved_path(),
                "--runtime-root",
                self._runtime_root_value(),
                "--log-path",
                self._log_path_value(),
            ]
        )
        return args

    def _run_server(self) -> None:
        Path(self._runtime_root_value()).mkdir(parents=True, exist_ok=True)
        for name, value in self._runtime_env().items():
            os.environ[name] = value
        run_mcp_http_server(
            self._profile(),
            host=self._resolved_host(),
            port=self._resolved_port(),
            path=self._resolved_path(),
        )

    def _status(self) -> None:
        profile = self._profile()
        self._json(
            {
                "name": profile.name,
                "host": self._resolved_host(),
                "port": self._resolved_port(),
                "path": self._resolved_path(),
                "url": canonical_mcp_url(
                    self._resolved_host(),
                    self._resolved_port(),
                    self._resolved_path(),
                ),
                "tools": [tool["name"] for tool in describe_tools(profile)],
            }
        )

    def _tmux_status(self) -> None:
        self._tmux_controller().status(out=self._out)

    def run(self):
        self._run_dev_action(
            {
                "run": self._run_server,
                "metadata": self._status,
                "status": self._tmux_status,
            },
            command_name="dev-mcp",
            choices=("run", "metadata", "status", *TMUX_ACTIONS),
        )
