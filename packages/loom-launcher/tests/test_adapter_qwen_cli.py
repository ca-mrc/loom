"""QwenCliAdapter contract: best-effort argv + tail_pty capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("qwen-cli")
    assert adapter is not None
    env: dict[str, str] = {
        "OPENAI_API_KEY": "step-token",
        "OPENAI_BASE_URL": "https://gateway.example",
    }
    argv = adapter.build_invocation(
        instruction="ping",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="qwen-2.5"),
        env=env,
    )
    assert argv == [
        "qwen",
        "--model",
        "qwen-2.5",
        "--prompt",
        "ping",
        "--output-format",
        "stream-json",
        "--auth-type",
        "openai",
        "--openai-base-url",
        "https://gateway.example",
    ]
    assert env["OPENAI_API_KEY"] == "step-token"
    assert "step-token" not in str(argv)


async def test_capture_via_pty(make_handle) -> None:
    adapter = get_adapter("qwen-cli")
    assert adapter is not None
    handle = make_handle(
        stdout_chunks=[
            b"\x1b[32mqwen> thinking\x1b[0m\n",
            b"qwen> response ready\n",
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
        {"line": "qwen> thinking", "kind": "tty_thought"},
        {"line": "qwen> response ready", "kind": "tty_thought"},
    ]
