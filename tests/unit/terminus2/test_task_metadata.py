from pathlib import PurePosixPath
from types import SimpleNamespace

import pytest

from loom.agent.terminus2.runtime import _harbor_task_options
from loom.agent.terminus2.task_metadata import (
    harbor_mcp_documents,
    terminus_task_metadata_rejections,
)
from loom.errors import AgentError
from loom.models.mcp import MCPConnection


def test_native_mcp_transport_projection():
    assert harbor_mcp_documents([
        MCPConnection(name="stdio", transport="stdio", command=["tool", "--arg"]),
        MCPConnection(name="http", transport="http", url="http://service/mcp"),
        MCPConnection(name="sse", transport="sse", url="http://service/sse"),
    ]) == [
        {"name": "stdio", "transport": "stdio", "command": "tool", "args": ["--arg"]},
        {"name": "http", "transport": "streamable-http", "url": "http://service/mcp"},
        {"name": "sse", "transport": "sse", "url": "http://service/sse"},
    ]


@pytest.mark.parametrize("change,reason", [
    ({"transport": "websocket"}, "transport"),
    ({"env": {"TOKEN": "env:MCP_TOKEN"}}, "environment"),
    ({"headers": {"Authorization": "Bearer ${MCP_TOKEN}"}}, "headers"),
    ({"url": "http://user:private-password@server/sse"}, "url_credentials"),
    ({"url": "http://[invalid"}, "url_invalid"),
    ({"transport": "stdio", "command": [""], "url": None}, "command"),
    ({"transport": "stdio", "command": [" \t"], "url": None}, "command"),
])
def test_unsupported_mcp_fields_reject_without_leaking_values(change, reason):
    server = MCPConnection.model_validate({"name": "tool", "transport": "sse",
                                          "url": "http://server/sse", **change})
    reasons = terminus_task_metadata_rejections([server])
    assert any(reason in item for item in reasons)
    with pytest.raises(ValueError) as error:
        harbor_mcp_documents([server])
    assert "MCP_TOKEN" not in str(error.value)
    assert "private-password" not in str(error.value)


@pytest.mark.parametrize("metadata", ["mcp", "skills"])
def test_old_harbor_cannot_silently_ignore_native_metadata(metadata):
    old_terminus = type("LegacyTerminus", (), {})
    with pytest.raises(AgentError, match="does not support task MCP/skills"):
        _harbor_task_options(old_terminus,
            [MCPConnection(name="tool", transport="sse", url="http://tool/sse")] if metadata == "mcp" else [],
            PurePosixPath("/skills") if metadata == "skills" else None)
    assert _harbor_task_options(old_terminus, [], None) == {}


def test_native_skills_path_is_forwarded_without_host_filesystem_access():
    native = type("NativeTerminus", (), {"capabilities": SimpleNamespace(skills=True)})
    assert _harbor_task_options(native, [], PurePosixPath("/sandbox/skills")) == {
        "skills_dir": "/sandbox/skills",
    }
