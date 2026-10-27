"""OpencodeAdapter contract: build_invocation + stream_stdout_jsonl capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("opencode")
    assert adapter is not None
    env = {"OPENAI_API_KEY": "step-token", "OPENAI_BASE_URL": "http://gateway.local"}
    argv = adapter.build_invocation(
        instruction="ping",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv[:4] == ["sh", "-c", argv[2], "loom-opencode"]
    assert argv[4] == "ping"
    assert "opencode run --model loom-openai-compatible/gpt-5" in argv[2]
    assert "--dangerously-skip-permissions" in argv[2]
    assert "</dev/null" in argv[2]
    assert '"$schema":"https://opencode.ai/config.json"' in argv[2]
    assert '"model":"loom-openai-compatible/gpt-5"' in argv[2]
    assert '"small_model":"loom-openai-compatible/gpt-5"' in argv[2]
    assert '"npm":"@ai-sdk/openai-compatible"' in argv[2]
    assert '"name":"Loom OpenAI-compatible Gateway"' in argv[2]
    assert '"baseURL":"http://gateway.local"' in argv[2]
    assert '"apiKey":"{env:OPENAI_API_KEY}"' in argv[2]
    assert "step-token" not in str(argv)
    assert env["OPENAI_API_KEY"] == "step-token"
    assert '"models":{"gpt-5"' in argv[2]
    assert env["HOME"] == "/tmp/loom-opencode-home"


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("opencode")
    assert adapter is not None
    handle = make_handle(
        stdout_chunks=[
            b'{"kind": "thought", "text": "exploring"}\n',
            b'{"kind": "tool_use", "name": "Read"}\n',
        ]
    )
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle,
            step_id="main",
            trial_id=uuid4(),
        )
    ]
    assert events == [
        {"kind": "thought", "text": "exploring"},
        {"kind": "tool_use", "name": "Read"},
    ]
