from __future__ import annotations

import json

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
