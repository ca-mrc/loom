"""Hosted workspace admission must match the controller's existing healthcheck path."""
from __future__ import annotations

import asyncio
import shlex
from pathlib import PurePosixPath
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.driver.healthcheck import run_command_healthcheck
from loom.errors import DriverError
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import automatic_service_execution_rejections
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_service_execution_sandbox_task import Sandbox
from tests.unit.test_service_execution_terminus_plan import _inputs


def _health():
    return HealthcheckSpec(command="test -f /tmp/baseline-done", start_period_sec=120,
                           start_interval_sec=2, interval_sec=2, timeout_sec=10, retries=30)


@pytest.mark.parametrize("agent_name", ["oracle", "terminus-2", "codex"])
def test_workspace_controller_admits_authored_healthcheck(agent_name):
    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"healthcheck": _health()})})
    trial = TrialConfig(agent_name="oracle", agent_model=None) if agent_name == "oracle" else trial.model_copy(update={"agent_name": agent_name})
    assert "extended_environment_unsupported" not in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance())


@pytest.mark.parametrize("agent_name", ["direct-completion", "litellm", "unknown-harness"])
def test_response_only_or_unknown_harness_does_not_claim_healthcheck_support(agent_name):
    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"healthcheck": _health()})})
    trial = trial.model_copy(update={"agent_name": agent_name})
    assert "extended_environment_unsupported" in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance())


@pytest.mark.parametrize("field,value", [
    ("dns", ["192.0.2.53"]), ("tmpfs", ["/cache:size=1m"]),
    ("extra_hosts", {"fixture": "192.0.2.1"}),
])
def test_healthcheck_support_does_not_admit_other_container_options(field, value):
    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"healthcheck": _health(), field: value})})
    assert "extended_environment_unsupported" in automatic_service_execution_rejections(
        task, trial, source_provenance=_provenance())


@pytest.mark.parametrize("agent_name", ["oracle", "terminus-2"])
@pytest.mark.parametrize("healthy", [False, True])
async def test_controller_waits_for_selected_main_health_before_agent(tmp_path, monkeypatch, agent_name, healthy):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    trial = TrialConfig(agent_name="oracle", agent_model=None) if agent_name == "oracle" else trial
    events = []
    baseline = tmp_path / "baseline-done"
    health = HealthcheckSpec(command="test -f " + shlex.quote(str(baseline)), retries=1,
                            interval_sec=.001, start_interval_sec=.001, timeout_sec=1)
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"healthcheck": health})})
    (tmp_path / "instruction.md").write_text("Work only after the baseline completes")
    if agent_name == "oracle":
        (tmp_path / "solution").mkdir()
        (tmp_path / "solution/solve.sh").write_text("true")

    class Main(Sandbox):
        async def run_healthcheck(self, spec):
            assert spec == health
            assert events == ["main_started", "loadgen_started"]
            events.append("baseline_probe")
            await run_command_healthcheck(self, spec)
            events.append("baseline_ready")

        async def exec(self, command, **kwargs):
            if command == health.command:
                process = await asyncio.create_subprocess_exec("/bin/sh", "-c", command,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0)
            return await super().exec(command, **kwargs)

    root, main = Sandbox(), Main()

    async def compose(*_):
        # The controller must use the main service selected by the Compose lifecycle,
        # not the outer guest/control driver. Each service's own readiness precedes this.
        await main.start()
        events.extend(["main_started", "loadgen_started"])
        if healthy:
            baseline.write_text("public loadgen baseline")
        return main, SimpleNamespace(stop=main.stop, containers={"main": "main", "loadgen": "loadgen"},
                                     driver=lambda _: main)

    async def identity(_):
        return uuid4(), uuid4()

    async def agent(**kwargs):
        assert kwargs["driver"] is main
        assert events[-1] == "baseline_ready"
        events.append("agent_started")

    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.delenv("LOOM_EXECUTION_PHASE_DEADLINE", raising=False)
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: root)
    monkeypatch.setattr(module, "_compose_main_driver", compose)
    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_oracle" if agent_name == "oracle" else "run_terminus2", agent)
    if healthy:
        await module.run_agent(tmp_path, task, trial)
        assert events[-1] == "agent_started"
    else:
        with pytest.raises(DriverError, match="Healthcheck failed"):
            await module.run_agent(tmp_path, task, trial)
        assert "agent_started" not in events
    assert root.state == main.state == "stopped"
    assert not root.filesystem  # Task inputs and healthcheck belong only to main.
    assert PurePosixPath("/app/instruction.md") in main.filesystem
