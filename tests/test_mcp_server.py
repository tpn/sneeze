import asyncio
import json
from io import StringIO

import pytest

from sneeze.mcp_commands import (
    McpCallBase,
    McpDevBase,
    McpRunBase,
    McpStatusBase,
)
from sneeze.mcp_server import (
    McpServerError,
    McpServerProfile,
    McpToolSpec,
    build_mcp_app,
    call_tool_sync,
    canonical_mcp_url,
    describe_tools,
    load_tool_specs,
)


def make_profile(*tools):
    return McpServerProfile(
        name="sample",
        tools_factory=lambda: tools,
        default_host="127.0.0.1",
        default_port=8945,
    )


def test_mcp_profile_describes_tools_without_importing_mcp():
    profile = make_profile(
        McpToolSpec(
            name="hello",
            description="Say hello.",
            input_schema={
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
            handler=lambda args: f"hello {args['name']}",
        )
    )

    assert describe_tools(profile) == [
        {
            "name": "hello",
            "description": "Say hello.",
            "input_schema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        }
    ]
    assert call_tool_sync(profile, "hello", {"name": "user"}) == "hello user"


def test_mcp_profile_supports_async_tool_handlers():
    async def handler(args):
        return {"greeting": f"hello {args['name']}"}

    profile = make_profile(
        McpToolSpec(
            name="hello",
            description="Say hello.",
            input_schema={"type": "object"},
            handler=handler,
        )
    )

    assert (
        call_tool_sync(profile, "hello", {"name": "user"})
        == '{\n  "greeting": "hello user"\n}'
    )


def test_mcp_profile_rejects_duplicate_tool_names():
    profile = make_profile(
        McpToolSpec("same", "first", {"type": "object"}, lambda args: "one"),
        McpToolSpec("same", "second", {"type": "object"}, lambda args: "two"),
    )

    with pytest.raises(McpServerError, match="duplicate MCP tool name"):
        load_tool_specs(profile)


def test_mcp_profile_rejects_unknown_tool_names():
    profile = make_profile(
        McpToolSpec(
            "hello",
            "Say hello.",
            {"type": "object"},
            lambda args: "",
        )
    )

    with pytest.raises(McpServerError, match="unknown MCP tool 'missing'"):
        call_tool_sync(profile, "missing", {})


def test_canonical_mcp_url_normalizes_loopback_and_path():
    assert (
        canonical_mcp_url("127.0.0.1", 8945, "mcp")
        == "http://localhost:8945/mcp/"
    )
    assert (
        canonical_mcp_url("0.0.0.0", 8945, "/mcp/")
        == "http://localhost:8945/mcp/"
    )
    assert (
        canonical_mcp_url("127.0.0.1", 8945, "/") == "http://localhost:8945/"
    )


def test_build_mcp_app_registers_tools_and_errors_unknown_tool():
    pytest.importorskip("mcp")
    from mcp.types import (
        CallToolRequest,
        CallToolRequestParams,
        ListToolsRequest,
    )

    profile = make_profile(
        McpToolSpec(
            "hello",
            "Say hello.",
            {"type": "object"},
            lambda args: "hello",
        )
    )
    app = build_mcp_app(profile)

    async def run_handlers():
        tools = await app.request_handlers[ListToolsRequest](
            ListToolsRequest()
        )
        ok = await app.request_handlers[CallToolRequest](
            CallToolRequest(
                params=CallToolRequestParams(name="hello", arguments={})
            )
        )
        missing = await app.request_handlers[CallToolRequest](
            CallToolRequest(
                params=CallToolRequestParams(name="missing", arguments={})
            )
        )
        return tools.root, ok.root, missing.root

    tools, ok, missing = asyncio.run(run_handlers())
    assert [tool.name for tool in tools.tools] == ["hello"]
    assert ok.content[0].text == "hello"
    assert ok.isError is False
    assert missing.isError is True
    assert "Unknown tool: missing" in missing.content[0].text


def test_mcp_command_bases_report_status_and_call_tools(monkeypatch):
    profile = make_profile(
        McpToolSpec(
            "hello",
            "Say hello.",
            {"type": "object"},
            lambda args: f"hello {args['name']}",
        )
    )

    class StatusCommand(McpStatusBase):
        mcp_profile = profile

    class CallCommand(McpCallBase):
        mcp_profile = profile

    class RunCommand(McpRunBase):
        mcp_profile = profile

    out = StringIO()
    status = StatusCommand(None, out, None)
    status._path = "/"
    status.run()
    payload = json.loads(out.getvalue())
    assert payload["url"] == "http://localhost:8945/"
    assert payload["tools"] == ["hello"]

    out = StringIO()
    call = CallCommand(None, out, None)
    call._tool = "hello"
    call._arguments = '{"name": "user"}'
    call.run()
    assert out.getvalue() == "hello user\n"

    calls = []
    monkeypatch.setattr(
        "sneeze.mcp_commands.run_mcp_http_server",
        lambda *args, **kwds: calls.append((args, kwds)),
    )
    run = RunCommand(None, StringIO(), None)
    run._host = "0.0.0.0"
    run._port = 9999
    run._path = "/mcp/"
    run.run()
    assert calls == [
        (
            (profile,),
            {"host": "0.0.0.0", "port": 9999, "path": "/mcp/"},
        )
    ]


def test_mcp_dev_metadata_does_not_require_tmux(monkeypatch):
    profile = make_profile(
        McpToolSpec(
            "hello", "Say hello.", {"type": "object"}, lambda args: ""
        )
    )

    class DevCommand(McpDevBase):
        mcp_profile = profile

    def fake_required(requested, *names):
        if "tmux" in names:
            raise AssertionError("status should not resolve tmux")
        return f"/tmp/{names[0]}"

    out = StringIO()
    command = DevCommand(None, out, None)
    command.args = ("metadata",)
    monkeypatch.setattr(
        "sneeze.tmux_dev.resolve_required_executable",
        fake_required,
    )

    command.run()
    payload = json.loads(out.getvalue())

    assert payload["url"] == "http://localhost:8945/mcp/"
    assert payload["tools"] == ["hello"]


def test_mcp_dev_status_uses_tmux(monkeypatch):
    profile = make_profile(
        McpToolSpec(
            "hello", "Say hello.", {"type": "object"}, lambda args: ""
        )
    )

    class DevCommand(McpDevBase):
        mcp_profile = profile

    class FakeController:
        def status(self, *, out):
            out("running: sample")

    out = StringIO()
    command = DevCommand(None, out, None)
    command.args = ("status",)
    monkeypatch.setattr(
        "sneeze.tmux_dev.TmuxDevCommandMixin._tmux_controller",
        lambda self: FakeController(),
    )

    command.run()

    assert out.getvalue() == "running: sample\n"


def test_mcp_dev_child_command_splits_display_name():
    profile = make_profile(
        McpToolSpec(
            "hello", "Say hello.", {"type": "object"}, lambda args: ""
        )
    )

    class DevCommand(McpDevBase):
        mcp_profile = profile

    command = DevCommand(None, None, None)
    command.cli_display_name = "sample dev-mcp"
    command._cli_bin = "/tmp/sne"
    command._mamba_bin = "/tmp/mamba"

    child = command._child_command()
    cli_index = child.index("/tmp/sne")

    assert child[cli_index + 1 : cli_index + 3] == ["sample", "dev-mcp"]


def test_mcp_dev_app_slug_only_strips_mcp_suffix():
    profile = McpServerProfile(
        name="my-mcp_server-mcp",
        tools_factory=tuple,
        default_host="127.0.0.1",
        default_port=8945,
    )

    class DevCommand(McpDevBase):
        mcp_profile = profile

    command = DevCommand(None, None, None)

    assert command._app_slug() == "my-mcp-server"


def test_mcp_dev_expands_runtime_root_and_log_path(monkeypatch, tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    profile = make_profile(
        McpToolSpec(
            "hello", "Say hello.", {"type": "object"}, lambda args: ""
        )
    )

    class DevCommand(McpDevBase):
        mcp_profile = profile

    command = DevCommand(None, None, None)
    command._runtime_root = "~/mcp-root"
    command._log_path = "~/logs/mcp.log"
    monkeypatch.setenv("HOME", str(home))

    assert command._runtime_root_value() == str(home / "mcp-root")
    assert command._log_path_value() == str(home / "logs" / "mcp.log")
