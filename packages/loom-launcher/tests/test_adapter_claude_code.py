"""ClaudeCodeAdapter contract: sh -c wrapper + stream-json capture.

Verifies Amendment A12.1: --print, no --workdir flag (wrapped in
sh -c "cd ... && claude ..."), non-interactive permissions,
telemetry+auto-update disabled via env.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv_uses_sh_c_cd_and_print() -> None:
    adapter = get_adapter("claude-code")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="anthropic", name="claude-sonnet-4"),
        env=env,
    )
    assert argv[:2] == ["sh", "-c"]
    cmd = argv[2]
    assert cmd.startswith("cd /workspace && ")
    assert "claude --verbose --output-format stream-json" in cmd
    assert "--model claude-sonnet-4" in cmd
    assert "--print 'solve fizzbuzz'" in cmd
    assert "--permission-mode acceptEdits" in cmd
    assert "--tools Bash,Write,Edit,Read,Glob,Grep" in cmd
    assert "--allowedTools Bash Write Edit Read Glob Grep" in cmd
    assert "--no-session-persistence" in cmd
    # Telemetry / auto-update disabled via env, NOT flags.
    assert env["DISABLE_TELEMETRY"] == "1"
    assert env["CLAUDE_CODE_AUTO_UPDATE"] == "false"
    # Verify no --workdir / --instruction CLI flags slipped in.
    assert "--workdir" not in cmd
    assert "--instruction" not in cmd


def test_build_invocation_shell_escapes_instruction() -> None:
    """Instructions with quotes/specials must shell-escape cleanly."""
    adapter = get_adapter("claude-code")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="say 'hi'; rm -rf /",
        workdir=PurePosixPath("/work"),
        model=ModelSpec(provider="anthropic", name="claude-sonnet-4"),
        env=env,
    )
    # shlex.quote wraps in single-quotes, escaping embedded ones.
    assert "'\"'\"'" in argv[2] or "rm -rf /" in argv[2]
    # Crucially, the dangerous payload is inside the quoted region.
    assert "; rm" not in argv[2].split("'\"'\"'")[0].replace("--print '", "")


async def test_capture_via_stream_json(make_handle) -> None:
    adapter = get_adapter("claude-code")
    assert adapter is not None
    # Real claude-code stream-json shape: one JSON object per line, each with
    # a discriminating `type` field.
    handle = make_handle(
        stdout_chunks=[
            b'{"type": "assistant", "text": "Let me look at the code."}\n',
            b'{"type": "tool_use", "name": "Read", "input": {"path": "main.py"}}\n',
            b'{"type": "assistant", "text": "Done."}\n',
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
        {"type": "assistant", "text": "Let me look at the code."},
        {"type": "tool_use", "name": "Read", "input": {"path": "main.py"}},
        {"type": "assistant", "text": "Done."},
    ]


def test_controlled_invocation_isolates_settings_and_pins_all_model_slots() -> None:
    from loom_launcher.adapters.claude_code import ClaudeCodeAdapter

    env = {"CLAUDE_CONFIG_DIR": "/tmp/loom-harness/claude-code-home", "CLAUDE_CODE_SIMPLE": "1"}
    argv = ClaudeCodeAdapter().build_invocation(instruction="work", workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="vendor", name="selected"), env=env, runtime_mode="chat")
    assert "--setting-sources ''" in argv[2] and "--strict-mcp-config" in argv[2]
    assert '--mcp-config' in argv[2] and '{"mcpServers":{}}' in argv[2]
    assert "--bare" not in argv[2] and "CLAUDE_CODE_SIMPLE" not in env
    assert env["CLAUDE_CONFIG_DIR"] == "/tmp/loom-harness/claude-code-home"
    for key in ("ANTHROPIC_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL", "ANTHROPIC_DEFAULT_OPUS_MODEL",
                "ANTHROPIC_DEFAULT_HAIKU_MODEL", "CLAUDE_CODE_SUBAGENT_MODEL"):
        assert env[key] == "selected"
    assert env["MAX_THINKING_TOKENS"] == "0"
    assert env["CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING"] == "1"
    assert env["DISABLE_PROMPT_CACHING"] == "1"


def test_native_profile_does_not_disable_native_thinking_or_cache() -> None:
    from loom_launcher.adapters.claude_code import ClaudeCodeAdapter

    env = {}
    ClaudeCodeAdapter().build_invocation(instruction="work", workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="anthropic", name="selected"), env=env, runtime_mode="native")
    assert not {"MAX_THINKING_TOKENS", "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS", "DISABLE_PROMPT_CACHING"} & env.keys()
    assert env["DISABLE_AUTOUPDATER"] == "1"
