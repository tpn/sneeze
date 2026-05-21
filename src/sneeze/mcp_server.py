from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from .command import CommandError


class McpServerError(CommandError):
    pass


@dataclass(frozen=True)
class McpToolSpec:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    handler: Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class McpServerProfile:
    name: str
    tools_factory: Callable[[], Iterable[McpToolSpec]]
    default_host: str = "127.0.0.1"
    default_port: int = 8945
    default_path: str = "/mcp"


def normalized_mcp_path(path: str | None) -> str:
    value = (path or "/mcp").strip() or "/mcp"
    if not value.startswith("/"):
        value = "/" + value
    return value.rstrip("/") or "/"


def canonical_mcp_url(host: str, port: int, path: str | None = None) -> str:
    mount_path = normalized_mcp_path(path)
    display_host = "localhost" if host in {"0.0.0.0", "127.0.0.1"} else host
    return f"http://{display_host}:{port}{mount_path.rstrip('/')}/"


def load_tool_specs(profile: McpServerProfile) -> tuple[McpToolSpec, ...]:
    tools = tuple(profile.tools_factory())
    seen = set()
    for tool in tools:
        if tool.name in seen:
            raise McpServerError(f"duplicate MCP tool name: {tool.name}")
        seen.add(tool.name)
    return tools


def describe_tools(profile: McpServerProfile) -> list[dict[str, Any]]:
    return [
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": dict(tool.input_schema),
        }
        for tool in load_tool_specs(profile)
    ]


def _content_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=2, sort_keys=True, default=str)


async def call_tool_spec(tool: McpToolSpec, arguments: dict[str, Any]) -> str:
    result = tool.handler(arguments)
    if inspect.isawaitable(result):
        result = await result
    return _content_text(result)


async def call_tool_by_name(
    profile: McpServerProfile,
    name: str,
    arguments: dict[str, Any] | None = None,
) -> str:
    tools = {tool.name: tool for tool in load_tool_specs(profile)}
    tool = tools.get(name)
    if not tool:
        choices = ", ".join(sorted(tools))
        raise McpServerError(
            f"unknown MCP tool {name!r}; expected one of: {choices}"
        )
    return await call_tool_spec(tool, arguments or {})


def call_tool_sync(
    profile: McpServerProfile,
    name: str,
    arguments: dict[str, Any] | None = None,
) -> str:
    return asyncio.run(call_tool_by_name(profile, name, arguments))


def build_mcp_app(profile: McpServerProfile):
    try:
        from mcp.server import Server
        from mcp.types import TextContent, Tool
    except ImportError as exc:
        raise McpServerError(
            "MCP support is not installed; install the 'sneeze[mcp]' extra"
        ) from exc

    app = Server(profile.name)
    tools = load_tool_specs(profile)
    tools_by_name = {tool.name: tool for tool in tools}

    @app.list_tools()
    async def list_tools():
        return [
            Tool(
                name=tool.name,
                description=tool.description,
                inputSchema=dict(tool.input_schema),
            )
            for tool in tools
        ]

    @app.call_tool()
    async def call_tool(name: str, arguments: Any):
        tool = tools_by_name.get(name)
        if not tool:
            raise McpServerError(f"Unknown tool: {name}")
        text = await call_tool_spec(tool, arguments or {})
        return [TextContent(type="text", text=text)]

    return app


def run_mcp_http_server(
    profile: McpServerProfile,
    *,
    host: str | None = None,
    port: int | None = None,
    path: str | None = None,
) -> None:
    try:
        import uvicorn
        from mcp.server.streamable_http_manager import (
            StreamableHTTPSessionManager,
        )
        from starlette.applications import Starlette
        from starlette.routing import Mount
    except ImportError as exc:
        raise McpServerError(
            "HTTP MCP support is not installed; install the "
            "'sneeze[mcp]' extra"
        ) from exc

    bind_host = host or profile.default_host
    bind_port = int(port or profile.default_port)
    mount_path = normalized_mcp_path(path or profile.default_path)
    app = build_mcp_app(profile)
    manager = StreamableHTTPSessionManager(
        app=app,
        json_response=True,
        stateless=True,
    )

    @asynccontextmanager
    async def lifespan(_app):
        async with manager.run():
            yield

    starlette_app = Starlette(
        routes=[Mount(mount_path, app=manager.handle_request)],
        lifespan=lifespan,
    )
    uvicorn.run(starlette_app, host=bind_host, port=bind_port)
