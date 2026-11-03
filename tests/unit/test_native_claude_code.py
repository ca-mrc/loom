"""Claude's installed harness uses the same sandbox authority and cache as Codex."""

from __future__ import annotations

from dataclasses import replace

import pytest

import loom.hosted_harness as hosted
import loom.service_execution_sandbox_task as controller
from loom.models.exec import ExecResult
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
)
from loom.service_execution_task import ServiceExecutionTaskError
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def _trial(mode="chat"):
    return TrialConfig(agent_name="claude-code", agent_model=ModelSpec(provider="vendor", name="code-model"), claude_code_mode=mode)


@pytest.mark.parametrize("mode", ["native", "chat"])
def test_claude_plan_freezes_verified_install_and_required_native_evidence(mode):
    task, _, profile = _inputs()
    plan = compile_service_execution_plan(task=task, trial=_trial(mode), profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert plan.main.argv[4] == "claude-code" and [p.argv[4] for p in plan.setup] == ["setup"]
    assert plan.setup_egress is not None and [d.host for d in plan.setup_egress.destinations] == ["registry.npmjs.org"]
    assert plan.task_egress is None and plan.setup_cache is not None
    outputs = {o.relative_path: o for o in plan.output_declarations}
    assert {"artifacts/claude-code/events.jsonl", "artifacts/claude-code/instruction.txt", "trajectory/events.jsonl"} <= outputs.keys()
    assert outputs["artifacts/claude-code/events.jsonl"].required


def test_unresolved_claude_mode_is_rejected_before_a_sandbox_is_planned():
    task, _, _ = _inputs()
    trial = TrialConfig(agent_name="claude-code", agent_model=ModelSpec(provider="vendor", name="m"))
    assert "claude_code_mode_required" in automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert "claude_code_mode" not in trial.model_dump()


@pytest.mark.parametrize("guest,host", [(False, "127.0.0.1"), (True, "10.0.2.2")])
def test_claude_gets_messages_broker_without_any_provider_key(monkeypatch, guest, host):
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:43111")
    env = controller.installed_agent_model_environment(hosted.CLAUDE_CODE,
        base_url_env="ANTHROPIC_BASE_URL", api_key_env="ANTHROPIC_AUTH_TOKEN", guest=guest)
    assert env == {"ANTHROPIC_BASE_URL": f"http://{host}:43111/anthropic", "ANTHROPIC_AUTH_TOKEN": "loom_workload_proxy"}


class _LibcDriver:
    def __init__(self, output, code=0):
        self.output, self.code = output, code

    async def exec(self, command, **kwargs):
        return ExecResult(return_code=self.code, stdout=self.output, stderr=b"", duration_sec=0)


@pytest.mark.parametrize("output,variant", [
    (b"x86_64\nldd (Debian GLIBC 2.36) 2.36\n", "archive"),
    (b"x86_64\nmusl libc (x86_64)\nVersion 1.2.5\n", "archive_musl"),
])
async def test_libc_selects_only_its_frozen_verified_archive(output, variant):
    setup = hosted.CLAUDE_CODE.setup
    selected = await controller._select_pinned_archive(_LibcDriver(output), setup, None)
    assert selected == getattr(setup, variant)
    assert selected.integrity.startswith("sha512-")
    assert selected.strip_components == 1


@pytest.mark.parametrize("output", [b"aarch64\nmusl libc", b"x86_64\nunknown libc", b"", b"x86_64\nglibc\nmusl libc"])
async def test_unknown_or_conflicting_platform_fails_closed(output):
    with pytest.raises(ServiceExecutionTaskError, match="platform"):
        await controller._select_pinned_archive(_LibcDriver(output), hosted.CLAUDE_CODE.setup, None)


async def test_cache_hit_requires_the_pinned_executable_version():
    setup = hosted.CLAUDE_CODE.setup
    assert await controller._check_install(_LibcDriver(b"2.1.183 (Claude Code)\n"), setup, 30)
    assert not await controller._check_install(_LibcDriver(b"2.0.0 (Claude Code)\n"), setup, 30)
    assert not await controller._check_install(_LibcDriver(b"2.1.183 (Claude Code)\n", 1), setup, 30)


def test_both_archive_variants_and_version_check_participate_in_cache_identity():
    setup = hosted.CLAUDE_CODE.setup
    changed = replace(setup, archive_musl=replace(setup.archive_musl, url="https://registry.npmjs.org/another.tgz"))
    assert setup.cache_identity("claude-code") != changed.cache_identity("claude-code")
    assert setup.cache_identity("claude-code") != replace(setup, check_output="different").cache_identity("claude-code")
    with pytest.raises(ValueError, match="declared https"):
        replace(setup, archive_musl=replace(setup.archive_musl, url="https://other.example/release.tgz"))


class _ClaudeProcess:
    def __init__(self, lines, code=0):
        import json
        self.lines, self.code, self.killed = lines, code, False
        self.stdout = self._chunks(b"".join(json.dumps(line).encode() + b"\n" for line in lines))
        self.stderr = self._chunks(b"")

    async def _chunks(self, data):
        yield data

    async def wait(self):
        return self.code

    async def kill(self):
        self.killed = True


class _ClaudeSandbox(_LibcDriver):
    def __init__(self, lines):
        super().__init__(b"")
        self.process = _ClaudeProcess(lines)
        self.environment = None

    async def exec_streaming(self, argv, *, env_vars, cwd, timeout_sec):
        self.environment = env_vars
        self.argv = argv
        return self.process


async def _run_claude(sandbox, directory, *, rows=None, row_count=1):
    from uuid import uuid4

    from loom.service_execution_claude_code import run_claude_code

    trial_id = uuid4()
    task, _, _ = _inputs()

    async def ledger(requested):
        assert requested == trial_id
        return rows if rows is not None else [{"id": str(uuid4()), "trial_id": str(trial_id), "step_id": "agent",
            "model": "code-model", "dialect": "anthropic_facade", "input_tokens": 20, "output_tokens": 5,
            "cost_usd": 0.01, "rate_card_hash": "test", "provider_extras": {
                "_loom_model_identity": {"provider": "vendor", "name": "code-model"},
                "_loom_gateway_route": "anthropic_to_chat", "_loom_cost_confidence": "configured"}} for _ in range(row_count)]

    await run_claude_code(driver=sandbox, workspace=directory, task_config=task, trial_config=_trial(),
        trial_id=trial_id, instruction="Fix the task.", ledger=ledger,
        model_environment={"ANTHROPIC_BASE_URL": "http://127.0.0.1:43111/anthropic", "ANTHROPIC_AUTH_TOKEN": "placeholder"})


async def test_controller_captures_native_and_authoritative_usage_outside_graded_workspace(tmp_path):
    import json

    from loom.service_execution_claude_code import parse_claude_events
    from tests.unit.test_claude_code_capture import _lines

    sandbox = _ClaudeSandbox(_lines())
    await _run_claude(sandbox, tmp_path)
    usage = json.loads((tmp_path / "usage.json").read_text())
    assert usage["call_count"] == 1
    assert usage["totals"]["cost_usd"] == 0.01  # CLI claimed $999; ledger owns cost.
    assert usage["totals"]["input_tokens"] == 20
    events = parse_claude_events((tmp_path / "trajectory.jsonl").read_bytes(), trial=_trial())
    assert len(events) == 5 and events[-1].model.provider == "vendor"
    assert (tmp_path / "claude-code/instruction.txt").read_text() == "Fix the task."
    assert len((tmp_path / "claude-code/events.jsonl").read_text().splitlines()) == 5
    assert sandbox.environment["CLAUDE_CONFIG_DIR"] == hosted.CLAUDE_CODE_HOME
    assert sandbox.environment["TMPDIR"] == hosted.CLAUDE_CODE_TMPDIR
    assert sandbox.environment["PATH"].startswith(hosted.CLAUDE_CODE_INSTALL_ROOT)
    assert sandbox.environment["MAX_THINKING_TOKENS"] == "0"


async def test_terminal_error_keeps_partial_evidence_and_billable_usage(tmp_path):
    import json

    from loom.errors import AgentError
    from tests.unit.test_claude_code_capture import _lines

    lines = _lines()
    lines[-1].update(subtype="error_during_execution", is_error=True)
    with pytest.raises(AgentError, match="terminal"):
        await _run_claude(_ClaudeSandbox(lines), tmp_path)
    assert json.loads((tmp_path / "usage.json").read_text())["call_count"] == 1
    assert (tmp_path / "trajectory.jsonl").stat().st_size > 0


async def test_success_requires_an_observed_model_call(tmp_path):
    from loom.errors import AgentError
    from tests.unit.test_claude_code_capture import _lines

    with pytest.raises(AgentError, match="model call"):
        await _run_claude(_ClaudeSandbox(_lines()), tmp_path, rows=[])


async def test_multiple_gateway_calls_keep_contiguous_trace_sequence(tmp_path):
    from loom.service_execution_claude_code import parse_claude_events
    from tests.unit.test_claude_code_capture import _lines
    await _run_claude(_ClaudeSandbox(_lines()), tmp_path, row_count=2)
    events = parse_claude_events((tmp_path / "trajectory.jsonl").read_bytes(), trial=_trial())
    assert [event.seq for event in events] == list(range(6))


async def test_cancellation_kills_the_owned_process_and_finalizes_evidence(tmp_path):
    import asyncio
    import json
    from tests.unit.test_claude_code_capture import _lines

    waiting = asyncio.Event()
    class WaitingProcess(_ClaudeProcess):
        async def wait(self):
            waiting.set()
            await asyncio.Event().wait()

    sandbox = _ClaudeSandbox(_lines())
    sandbox.process = WaitingProcess(_lines())
    running = asyncio.create_task(_run_claude(sandbox, tmp_path))
    await asyncio.wait_for(waiting.wait(), 3)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert sandbox.process.killed
    assert json.loads((tmp_path / "usage.json").read_text())["call_count"] == 1
