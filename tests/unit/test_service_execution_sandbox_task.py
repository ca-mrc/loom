from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.driver.fake import FakeDriver
from loom.errors import DriverError
from loom.models.exec import ExecResult
from loom.service_execution_sandbox_task import run_agent, run_verifier
from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.parametrize("agent_error", [False, True])
async def test_collects_many_immutable_input_artifacts_after_agent_exit(
    tmp_path, monkeypatch, agent_error,
):
    task, trial, _ = _inputs()
    paths = [f"out/part-{index:04}.json" for index in range(515)]
    task = task.model_copy(update={"steps": [task.steps[0].model_copy(update={
        "artifacts": paths, "required_artifacts": [paths[-1], "required.txt"],
    })]})
    (tmp_path / "instruction.md").write_text("Produce declared output")
    agent = Sandbox()
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.delenv("LOOM_TASK_ARTIFACTS_JSON", raising=False)
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_FROM_INPUT", "1")
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver", lambda role, task, trial: agent)

    async def identity(gateway):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        # The agent can replace its public task.toml, but declarations belong
        # to the controller's frozen input, never the returned workspace.
        agent.filesystem[PurePosixPath("/app/task.toml")] = b'[[steps]]\nartifacts = ["forged.txt"]\n'
        agent.filesystem[PurePosixPath("/app/forged.txt")] = b"untrusted"
        for path in (*paths, "required.txt"):
            agent.filesystem[PurePosixPath("/app") / path] = path.encode()
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        if agent_error:
            raise RuntimeError("controlled agent failure")

    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    if agent_error:
        with pytest.raises(RuntimeError, match="controlled agent failure"):
            await run_agent(tmp_path, task, trial)
    else:
        await run_agent(tmp_path, task, trial)
    for path in (*paths, "required.txt"):
        assert (tmp_path / ".loom/collected" / path).read_bytes() == path.encode()
    assert not (tmp_path / ".loom/collected/forged.txt").exists()
    assert agent.state == "stopped"


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot_fails", [False, True])
async def test_deadline_timeout_handoff_requires_completed_snapshot(
    tmp_path, monkeypatch, snapshot_fails,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("work")
    driver = Sandbox()
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    epoch = time.time() + 0.05
    monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(epoch))
    monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "1")
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert kwargs["deadline"].wall_deadline_epoch_sec == epoch
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        driver.filesystem[PurePosixPath("/app/answer.txt")] = b"partial answer"
        await asyncio.sleep(10)

    async def failed_snapshot(*args, **kwargs):
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    if snapshot_fails:
        monkeypatch.setattr(module, "_export_workspace_archive", failed_snapshot)
    expected = RuntimeError if snapshot_fails else module.AgentTimeoutFinalizedError
    with pytest.raises(expected):
        await module.run_agent(tmp_path, task, trial)
    assert driver.quiesced and driver.state == "stopped"
    assert (tmp_path / ".loom/agent/usage.json").is_file()
    assert (tmp_path / ".loom/workspace.tar").is_file() is not snapshot_fails


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["deadline_signal", "early_cancel", "second_signal", "stuck_snapshot", "late_unwind"])
async def test_signal_and_finalization_budget_do_not_grant_unsafe_handoff(
    tmp_path, monkeypatch, mode,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("work")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    # An explicit early signal must precede the deadline even on a busy runner.
    monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(time.time() + (5 if mode == "early_cancel" else 0.05)))
    # Successful handoff and early-cancel cleanup both perform real archive IO
    # on worker threads. Only expiry cases require a short grace budget.
    monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "2" if mode in {"deadline_signal", "early_cancel"} else "0.1")
    callbacks = []
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda _, callback: callbacks.append(callback))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)

    class SignalSandbox(Sandbox):
        async def export_workspace_archive(self, src, dst):
            if mode == "early_cancel":
                # Model archive-thread scheduling delay seen in the full CI
                # suite; early cancellation must not become a deadline test.
                await asyncio.sleep(0.2)
            if mode in {"deadline_signal", "second_signal"}:
                callbacks[0]()  # Same deadline signal must not interrupt cleanup.
            if mode == "second_signal":
                callbacks[0]()  # A second stop cancels; never authorize verifier.
                await asyncio.sleep(0)
            if mode == "stuck_snapshot":
                await asyncio.sleep(10)
            if mode == "late_unwind":
                await asyncio.sleep(0.01)
            await super().export_workspace_archive(src, dst)

    driver = SignalSandbox()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        if mode == "early_cancel":
            callbacks[0]()
        try:
            await asyncio.sleep(10)
        finally:
            if mode == "late_unwind":
                # Slow unwinding cannot restart the grace period.
                time.sleep(0.2)

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    expected = {
        "deadline_signal": module.AgentTimeoutFinalizedError,
        "early_cancel": asyncio.CancelledError,
        "second_signal": asyncio.CancelledError,
        "stuck_snapshot": TimeoutError,
        "late_unwind": TimeoutError,
    }[mode]
    with pytest.raises(expected) as caught:
        await module.run_agent(tmp_path, task, trial)
    if mode != "deadline_signal":
        assert not isinstance(caught.value, module.AgentTimeoutFinalizedError)
    assert driver.quiesced and driver.state == "stopped"


class Sandbox(FakeDriver):
    quiesced = False

    async def stop_processes(self):
        self.quiesced = True

    async def export_workspace_archive(self, src, dst):
        assert self.quiesced
        await super().export_workspace_archive(src, dst)


@pytest.mark.parametrize("verifier_drift", [False, True])
async def test_declared_workspace_reference_manifest_is_used_by_both_phases(tmp_path, monkeypatch, verifier_drift):
    from loom.models.task import TaskConfig
    from loom.trial.workspace_snapshot import WorkspaceSnapshotError

    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["workspace_reference_files"] = ["/usr/local/bin/python3", "/usr/local/bin/python3.11"]
    raw["environment"]["reference_file_symlinks"] = {"/usr/local/bin/python3": "python3.11"}
    task = TaskConfig.model_validate(raw)
    (tmp_path / "instruction.md").write_text("Use an ordinary virtual environment")

    class ReferenceSandbox(Sandbox):
        drift = False
        imported = False

        async def inspect_reference_symlink(self, path):
            return "python3.11"

        async def inspect_reference_file(self, path, *, max_bytes):
            return {"path": str(path), "size_bytes": 6, "mode": 0o755, "uid": 0, "gid": 0,
                    "sha256": ("1" if self.drift else "0") * 64}

        async def import_workspace_archive(self, src, dst, *, policy, preserve_acls, external_reference_files):
            assert external_reference_files == frozenset(task.environment.workspace_reference_files)
            self.imported = True
            await super().import_workspace_archive(src, dst, policy=policy)

    agent, verifier = ReferenceSandbox(), ReferenceSandbox()
    verifier.drift = verifier_drift
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver",
                        lambda role, task, trial: agent if role == "task-sandbox" else verifier)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        agent.filesystem[PurePosixPath("/app/answer")] = b"output"

    def check(cmd, user, cwd, env):
        if env and "LOOM_VERIFIER_OUTPUT" in env:
            assert verifier.filesystem[PurePosixPath("/app/answer")] == b"output"
            verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    verifier.exec_handler = check
    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    await run_agent(tmp_path, task, trial)
    assert (tmp_path / ".loom/workspace-references.json").is_file()
    if verifier_drift:
        with pytest.raises(WorkspaceSnapshotError, match="differs"):
            await run_verifier(tmp_path, task, trial)
        assert not verifier.imported
    else:
        await run_verifier(tmp_path, task, trial)
        assert verifier.imported
    assert verifier.quiesced and verifier.state == "stopped"


@pytest.mark.asyncio
async def test_phase_handoff_preserves_declared_state_at_original_absolute_path(tmp_path, monkeypatch):
    from loom.models.task import TaskConfig

    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["mutable_paths"] = ["/data", "/home/agent"]
    task = TaskConfig.model_validate(raw)
    (tmp_path / "instruction.md").write_text("Produce outputs outside workdir")
    agent, verifier = Sandbox(), Sandbox()
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver",
                        lambda role, task, trial: agent if role == "task-sandbox" else verifier)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        agent.filesystem[PurePosixPath("/data/answer")] = b"unique output"
        agent.filesystem[PurePosixPath("/home/agent/kernelspec")] = b"installed kernel"
        agent.filesystem[PurePosixPath("/undeclared/secret")] = b"not exported"

    def check(cmd, user, cwd, env):
        if cmd == "id -u; id -g":
            return ExecResult(return_code=0, stdout=b"0\n0\n", stderr=b"", duration_sec=0)
        if env and "LOOM_VERIFIER_OUTPUT" in env:
            assert verifier.filesystem[PurePosixPath("/data/answer")] == b"unique output"
            assert verifier.filesystem[PurePosixPath("/home/agent/kernelspec")] == b"installed kernel"
            assert PurePosixPath("/undeclared/secret") not in verifier.filesystem
            verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    verifier.exec_handler = check
    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    await run_agent(tmp_path, task, trial)
    await run_verifier(tmp_path, task, trial)
    manifest = json.loads((tmp_path / ".loom/mutable-paths/manifest.json").read_text())
    assert [item["path"] for item in manifest["paths"]] == ["/data", "/home/agent"]
    # #2589: the fresh verifier reports omitted links beside its result.
    assert json.loads((tmp_path / ".loom/verifier/workspace-handoff.json").read_text()) == {
        "schema_version": "loom.workspace-handoff.v1", "omitted_links": [], "omitted_count": 0,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_error", [False, True])
@pytest.mark.parametrize("wrapper_kind", ["native", "harbor", "harbor_native", "modified_harbor", "same_size_modified_harbor"])
async def test_phase_handoff_keeps_tests_private_and_quiesces_before_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, agent_error: bool, wrapper_kind: str,
):
    from loom.harbor_verifier_script import (
        native_verifier_run_sh_bytes,
        offline_verifier_run_sh_bytes,
    )

    task, trial, _ = _inputs()
    separate_private_inputs = wrapper_kind in {"harbor", "harbor_native"}
    if wrapper_kind != "native":
        task.verifier.args["script_path"] = "verifier/run.sh"
    (tmp_path / "instruction.md").write_text("Produce answer.txt")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/check.py").write_text("trusted assertions")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier/check.sh").write_text("trusted verifier")
    if wrapper_kind != "native":
        wrapper = (native_verifier_run_sh_bytes() if wrapper_kind == "harbor_native"
                   else offline_verifier_run_sh_bytes())
        if wrapper_kind == "modified_harbor":
            wrapper += b"\n"
        elif wrapper_kind == "same_size_modified_harbor":
            wrapper = wrapper.replace(b"# Loom", b"# loom", 1)
            assert len(wrapper) == len(offline_verifier_run_sh_bytes())
        (tmp_path / "verifier/run.sh").write_bytes(wrapper)
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution/solve.sh").write_text("private oracle")
    agent, verifier = Sandbox(), Sandbox()
    agent.filesystem[PurePosixPath("/app/fixture.txt")] = b"baked fixture"
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", '["answer.txt", "optional_script.py"]')
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver",
                        lambda role, task, trial: agent if role == "task-sandbox" else verifier)

    async def identity(gateway):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert kwargs["instruction"] == "Produce answer.txt"
        if separate_private_inputs:
            assert PurePosixPath("/app/instruction.md") not in agent.filesystem
        else:
            assert agent.filesystem[PurePosixPath("/app/instruction.md")] == b"Produce answer.txt"
        assert not any(path.parts[2] in {"tests", "verifier", "solution", ".loom"}
                       for path in agent.filesystem)
        agent.filesystem[PurePosixPath("/app/answer.txt")] = b"42"
        # A task can create fake private paths, but these never replace evaluator inputs.
        agent.filesystem[PurePosixPath("/app/tests/check.py")] = b"forged assertions"
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        if agent_error:
            raise RuntimeError("agent stopped")

    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    if agent_error:
        with pytest.raises(RuntimeError, match="agent stopped"):
            await run_agent(tmp_path, task, trial)
        assert agent.quiesced and agent.state == "stopped"
        assert (tmp_path / ".loom/workspace.tar").exists()
        info = json.loads((tmp_path / ".loom/agent/exception.json").read_text())
        assert info["exception_type"] == "RuntimeError"
        assert info["exception_message"] == "agent stopped"
        return
    await run_agent(tmp_path, task, trial)
    assert (tmp_path / ".loom/collected/answer.txt").read_bytes() == b"42"

    def check(cmd, user, cwd, env):
        assert cwd == PurePosixPath("/app") and user is None
        private_root = PurePosixPath("/loom/verifier/task" if separate_private_inputs else "/app")
        assert verifier.filesystem[private_root / "tests/check.py"] == b"trusted assertions"
        assert env["LOOM_TASK_DIR"] == str(private_root)
        if separate_private_inputs:
            expected = ("/bin/sh /loom/verifier/run.sh" if wrapper_kind == "harbor_native"
                        else "/bin/sh /loom/verifier/task/verifier/run.sh")
            assert cmd == expected
            assert env["LOOM_VERIFIER_OUTPUT"] == "/loom/verifier/output.json"
            assert not any(path.is_relative_to("/app/tests") or path.is_relative_to("/app/verifier")
                           or path.is_relative_to("/app/solution") for path in verifier.filesystem)
        else:
            assert cmd == "/bin/sh " + task.verifier.args["script_path"]
        assert verifier.filesystem[PurePosixPath("/app/answer.txt")] == b"42"
        assert verifier.filesystem[PurePosixPath("/app/fixture.txt")] == b"baked fixture"
        workspace_reports = {path for path in verifier.filesystem if str(path).startswith("/app/.loom/")}
        assert workspace_reports == (set() if separate_private_inputs else {PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])})
        assert verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] == b""
        verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        verifier.filesystem[PurePosixPath("/logs/verifier/ctrf.json")] = b'{}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    verifier.exec_handler = check
    await run_verifier(tmp_path, task, trial)
    assert verifier.quiesced and verifier.state == "stopped"
    assert json.loads((tmp_path / ".loom/verifier/output.json").read_bytes())["rewards"] == {"passed": 0}


@pytest.mark.parametrize("phase", ["terminus-2", "verify-sandbox"])
def test_phase_entrypoint_retains_sanitized_typed_failure(tmp_path, monkeypatch, phase):
    import tomli_w

    from loom import service_execution_sandbox_task as module
    from loom.errors import AgentError

    class ContextLengthExceededError(Exception):
        pass

    task, trial, _ = _inputs()
    (tmp_path / "task.toml").write_text(tomli_w.dumps(task.model_dump(mode="json", exclude_none=True)))
    monkeypatch.setenv("LOOM_TASK_TRIAL_JSON", trial.model_dump_json())
    monkeypatch.setattr("sys.argv", ["sandbox-task", phase, "--workspace", str(tmp_path)])

    async def fail(*args):
        try:
            raise ContextLengthExceededError("Bearer private-step-token")
        except ContextLengthExceededError as exc:
            raise AgentError("") from exc

    monkeypatch.setattr(module, "run_agent" if phase == "terminus-2" else "run_verifier", fail)
    with pytest.raises(AgentError):
        module.main()
    directory = "agent" if phase == "terminus-2" else "verifier"
    body = (tmp_path / ".loom" / directory / "exception.json").read_text()
    assert "private-step-token" not in body
    assert json.loads(body)["exception_type"] == "ContextLengthExceededError"


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_during_snapshot", [False, True])
async def test_successful_agent_snapshot_crossing_deadline_still_hands_off(
    tmp_path, monkeypatch, signal_during_snapshot,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("work")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(time.time() + 0.05))
    monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "1")
    callbacks = []
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda _, callback: callbacks.append(callback))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda _: True)

    class SlowSnapshotSandbox(Sandbox):
        async def export_workspace_archive(self, src, dst):
            await asyncio.sleep(0.08)
            if signal_during_snapshot:
                callbacks[0]()
            await super().export_workspace_archive(src, dst)

    driver = SlowSnapshotSandbox()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        assert not kwargs["deadline"].reached
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
        # Agent finishes before its cutoff; only the snapshot crosses it.

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    with pytest.raises(module.AgentTimeoutFinalizedError):
        await module.run_agent(tmp_path, task, trial)
    assert driver.quiesced and driver.state == "stopped"
    assert (tmp_path / ".loom/workspace.tar").is_file()


@pytest.mark.asyncio
@pytest.mark.parametrize("report", [b'{"rewards":{"passed":0}}', b'{"rewards":'])
async def test_verifier_cleanup_failure_retains_reports_without_success(
    tmp_path, monkeypatch, capsys, report,
):
    from pydantic import ValidationError

    from loom import service_execution_sandbox_task as module
    from loom.errors import DriverError

    task, trial, _ = _inputs()
    cleanup_error = DriverError("cleanup-secret-must-not-be-printed")

    class FailingCleanup(Sandbox):
        stops = 0

        async def stop_processes(self):
            self.stops += 1
            raise cleanup_error

    driver = FailingCleanup()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(module, "materialize_workspace", noop)
    monkeypatch.setattr(module, "_import_workspace_archive", noop)
    (tmp_path / ".loom").mkdir()
    (tmp_path / ".loom/workspace.tar").write_bytes(b"workspace")

    def verify(cmd, user, cwd, env):
        driver.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = report
        driver.filesystem[PurePosixPath("/logs/verifier/ctrf.json")] = b'{"results":{}}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    driver.exec_handler = verify
    valid = report.endswith(b"}}")
    with pytest.raises(DriverError if valid else ValidationError) as caught:
        await run_verifier(tmp_path, task, trial)
    if valid:
        assert caught.value is cleanup_error
    assert (tmp_path / ".loom/verifier/output.json").read_bytes() == report
    assert (tmp_path / ".loom/verifier/ctrf.json").read_bytes() == b'{"results":{}}'
    assert driver.stops == 1 and driver.state == "stopped"
    stderr = capsys.readouterr().err
    assert "cleanup-secret" not in stderr
    if not valid:
        assert "secondary verifier stop_processes failure (DriverError)" in stderr


@pytest.mark.parametrize("mode", ["shared", "separate"])
async def test_verifier_cannot_reuse_stale_report_when_script_writes_nothing(tmp_path, monkeypatch, mode):
    from pydantic import ValidationError

    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    trial = trial.model_copy(update={"verifier_env_mode": mode})
    driver = Sandbox()
    driver.filesystem[PurePosixPath("/app/.loom/verifier/output.json")] = b'{"rewards":{"passed":1}}'
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(module, "materialize_workspace", noop)
    monkeypatch.setattr(module, "_import_workspace_archive", noop)
    (tmp_path / ".loom").mkdir()
    (tmp_path / ".loom/workspace.tar").write_bytes(b"workspace")
    with pytest.raises(ValidationError):
        await run_verifier(tmp_path, task, trial)
    assert not (tmp_path / ".loom/verifier/output.json").read_bytes()
    assert driver.quiesced and driver.state == "stopped"


async def test_verifier_report_preparation_failure_still_cleans_up(tmp_path, monkeypatch):
    from loom import service_execution_sandbox_task as module
    from loom.errors import DriverError

    task, trial, _ = _inputs()
    trial = trial.model_copy(update={"verifier_env_mode": "shared"})
    preparation_error = DriverError("report path is not writable")

    class UnwritableReport(Sandbox):
        async def upload(self, src, dst):
            raise preparation_error

    driver = UnwritableReport()
    script_calls = []

    def execute(cmd, user, cwd, env):
        if env and "LOOM_VERIFIER_OUTPUT" in env:
            script_calls.append(cmd)
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    driver.exec_handler = execute
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    with pytest.raises(DriverError) as caught:
        await run_verifier(tmp_path, task, trial)
    assert caught.value is preparation_error
    assert not script_calls
    assert driver.quiesced and driver.state == "stopped"


@pytest.mark.asyncio
@pytest.mark.parametrize("exec_raises", [False, True])
async def test_verifier_primary_failure_survives_cleanup_and_driver_stop_failure(
    tmp_path, monkeypatch, capsys, exec_raises,
):
    from loom import service_execution_sandbox_task as module
    from loom.errors import DriverError

    task, trial, _ = _inputs()
    exec_error = DriverError("private-command-in-error")

    class FailedDriver(Sandbox):
        stops = 0
        closed = False

        async def stop_processes(self):
            self.stops += 1
            raise DriverError("private-cleanup-detail")

        async def stop(self, **kwargs):
            self.closed = True
            raise DriverError("private-close-detail")

    driver = FailedDriver()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(module, "materialize_workspace", noop)
    monkeypatch.setattr(module, "_import_workspace_archive", noop)
    (tmp_path / ".loom").mkdir()
    (tmp_path / ".loom/workspace.tar").write_bytes(b"workspace")

    def verify(cmd, user, cwd, env):
        if exec_raises:
            raise exec_error
        driver.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        return ExecResult(return_code=1, stdout=b"", stderr=b"", duration_sec=0)

    driver.exec_handler = verify
    expected = DriverError if exec_raises else module.ServiceExecutionTaskError
    with pytest.raises(expected) as caught:
        await run_verifier(tmp_path, task, trial)
    if exec_raises:
        assert caught.value is exec_error
    else:
        assert str(caught.value) == "isolated verifier process failed"
        assert json.loads((tmp_path / ".loom/verifier/output.json").read_bytes())["rewards"] == {"passed": 0}
    assert driver.stops == 1 and driver.closed
    stderr = capsys.readouterr().err
    assert "private-" not in stderr
    assert "secondary verifier stop_processes failure (DriverError)" in stderr
    assert "secondary verifier stop failure (DriverError)" in stderr


class _VenvSandbox(Sandbox):
    """The agent ran `python3 -m venv /app/venv`, linking the image interpreter."""

    async def export_workspace_archive(self, src, dst):
        import io
        import tarfile

        # Shared mode keeps agent processes for grading, so it is not quiesced.
        with tarfile.open(dst, "w") as archive:
            for name in (".", "venv", "venv/bin"):
                directory = tarfile.TarInfo(name)
                directory.type, directory.mode = tarfile.DIRTYPE, 0o755
                archive.addfile(directory)
            link = tarfile.TarInfo("venv/bin/python3")
            link.type, link.linkname = tarfile.SYMTYPE, "/usr/bin/python3"
            archive.addfile(link)
            report = tarfile.TarInfo("report.txt")
            report.size = 2
            archive.addfile(report, io.BytesIO(b"ok"))

    # The trusted native RPCs fingerprint the image interpreter (Debian layout).
    async def inspect_reference_symlink(self, path):
        if str(path) != "/usr/bin/python3":
            raise DriverError("sandbox symlink inspection failed")
        return "python3.11"

    async def inspect_reference_file(self, path, *, max_bytes):
        if str(path) != "/usr/bin/python3.11":
            raise DriverError("sandbox reference inspection failed")
        return {"path": str(path), "size_bytes": 6, "mode": 0o755, "uid": 0, "gid": 0, "sha256": "a" * 64}


def _agent_run(tmp_path, monkeypatch, driver, *, shared: bool):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    if shared:
        trial = trial.model_copy(update={"verifier_env_mode": "shared"})
    (tmp_path / "instruction.md").write_text("Create a virtual environment")
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    return module.run_agent(tmp_path, task, trial)


@pytest.mark.asyncio
async def test_in_place_verifier_keeps_an_agent_venv_without_restore_validation(tmp_path, monkeypatch):
    # #2589: an absolute link into the image fails restore validation, but an
    # in-place verifier grades the live workdir and never restores the archive.
    await _agent_run(tmp_path, monkeypatch, _VenvSandbox(), shared=True)

    import tarfile

    with tarfile.open(tmp_path / ".loom/workspace.tar") as archive:
        assert archive.getmember("venv/bin/python3").linkname == "/usr/bin/python3"
    assert not (tmp_path / ".loom/workspace-archive.json").exists()


@pytest.mark.asyncio
async def test_fresh_verifier_handoff_keeps_a_venv_link_to_the_image_interpreter(tmp_path, monkeypatch):
    # #2589: separate/guest receives the full workdir. The link stays a link,
    # bound to the image interpreter it resolves to in both sandboxes.
    import tarfile

    await _agent_run(tmp_path, monkeypatch, _VenvSandbox(), shared=False)

    with tarfile.open(tmp_path / ".loom/workspace.tar") as archive:
        assert archive.getmember("venv/bin/python3").linkname == "/usr/bin/python3"
    manifest = json.loads((tmp_path / ".loom/workspace-references.json").read_text())
    assert [item["target"] for item in manifest["discovered"]] == ["/usr/bin/python3"]
    assert manifest["discovered"][0]["references"][0] == {"path": "/usr/bin/python3", "target": "python3.11"}
    assert manifest["omitted"] == [] and manifest["omitted_count"] == 0


@pytest.mark.asyncio
async def test_fresh_verifier_handoff_omits_a_link_it_cannot_verify(tmp_path, monkeypatch):
    import tarfile

    class Unverifiable(_VenvSandbox):
        async def inspect_reference_file(self, path, *, max_bytes):
            raise DriverError("sandbox reference inspection failed")

    await _agent_run(tmp_path, monkeypatch, Unverifiable(), shared=False)

    with tarfile.open(tmp_path / ".loom/workspace.tar") as archive:
        assert "venv/bin/python3" not in archive.getnames() and "report.txt" in archive.getnames()
    manifest = json.loads((tmp_path / ".loom/workspace-references.json").read_text())
    assert manifest["omitted"] == [
        {"path": "venv/bin/python3", "target": "/usr/bin/python3", "reason": "target_unverifiable"},
    ]


@pytest.mark.asyncio
async def test_in_place_archive_timeout_is_recorded_and_does_not_fail_the_agent(tmp_path, monkeypatch):
    from loom import service_execution_sandbox_task as module

    class SlowSandbox(Sandbox):
        async def export_workspace_archive(self, src, dst):
            dst.write_bytes(b"partial")
            await asyncio.sleep(10)

    monkeypatch.setattr(module, "DEBUG_ARCHIVE_TIMEOUT_SECONDS", 0.05)
    await _agent_run(tmp_path, monkeypatch, SlowSandbox(), shared=True)

    assert not (tmp_path / ".loom/workspace.tar").exists()
    outcome = json.loads((tmp_path / ".loom/workspace-archive.json").read_text())
    assert (outcome["state"], outcome["error_type"], outcome["detail"]) == ("failed", "TimeoutError", "")


@pytest.mark.asyncio
async def test_in_place_archive_failure_is_recorded_and_does_not_fail_the_agent(tmp_path, monkeypatch):
    from loom.trial.workspace_snapshot import WorkspaceSnapshotError

    class FailingSandbox(Sandbox):
        async def export_workspace_archive(self, src, dst):
            dst.write_bytes(b"partial")
            raise WorkspaceSnapshotError("agent workspace contains unsupported device, FIFO, or socket: /app/s")

    await _agent_run(tmp_path, monkeypatch, FailingSandbox(), shared=True)

    assert not (tmp_path / ".loom/workspace.tar").exists()
    outcome = json.loads((tmp_path / ".loom/workspace-archive.json").read_text())
    assert outcome == {
        "schema_version": "loom.workspace-archive-outcome.v1",
        "state": "failed",
        "error_type": "WorkspaceSnapshotError",
        "detail": "agent workspace contains unsupported device, FIFO, or socket: /app/s",
    }


@pytest.mark.parametrize("kind", ["native", "native-v1", "offline"])
@pytest.mark.parametrize("modified", [False, True])
def test_harbor_layout_recognizes_only_complete_known_templates(tmp_path, kind, modified):
    from loom import service_execution_sandbox_task as module
    from loom.harbor_verifier_script import (
        native_verifier_run_sh_templates,
        offline_verifier_run_sh_bytes,
    )

    task, _, _ = _inputs()
    task.verifier.args["script_path"] = "verifier/run.sh"
    current, legacy = native_verifier_run_sh_templates()
    wrapper = {"native": current, "native-v1": legacy, "offline": offline_verifier_run_sh_bytes()}[kind]
    (tmp_path / "verifier").mkdir()
    if modified:
        wrapper = wrapper.replace(b"# ", b"##", 1)
    (tmp_path / "verifier/run.sh").write_bytes(wrapper)
    assert module._uses_harbor_private_inputs(tmp_path, task) is not modified


class _FilesystemSandbox:
    """Real shells and files in disposable directories; no Docker or root access."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.executions = []
        self.stopped_processes = False
        self.stopped = False
        self.healthchecks = 0

    async def start(self):
        pass

    async def stop(self):
        self.stopped = True

    async def stop_processes(self):
        self.stopped_processes = True

    async def run_healthcheck(self, _):
        self.healthchecks += 1

    async def upload(self, src, dst):
        import shutil

        target = Path(dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, target)
        target.chmod(src.stat().st_mode & 0o777)

    async def download(self, src, dst):
        import shutil

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)

    async def export_workspace_archive(self, src, dst):
        import tarfile

        dst.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(dst, "w") as archive:
            archive.add(str(src), arcname=".")

    async def exec(self, cmd, *, user=None, cwd=None, env=None, **kwargs):
        import os
        import sys

        assert user is None  # Native staging must not require privilege escalation.
        self.executions.append((cmd, cwd, env))
        effective_env = {
            **os.environ, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"],
            "LOOM_HARBOR_LOG_DIR": str(self.root / "logs"),
            "LOOM_HARBOR_TEST_DIR": str(self.root / "tests"),
            **(env or {}),
        }
        process = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", cmd, cwd=str(cwd) if cwd else str(self.root), env=effective_env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return ExecResult(return_code=process.returncode, stdout=stdout, stderr=stderr, duration_sec=0)


@pytest.mark.parametrize("requirement", ["shell-whitelist", "git-clean"])
@pytest.mark.parametrize("extra_business_file", [False, True])
@pytest.mark.parametrize("wrapper_version", [0, 1], ids=["current", "published-v1"])
async def test_shared_native_harbor_executes_with_clean_business_tree(
    tmp_path, monkeypatch, requirement, extra_business_file, wrapper_version,
):
    import shlex

    from loom import service_execution_sandbox_task as module
    from loom.harbor_verifier_script import native_verifier_run_sh_templates
    from loom.models.trial import TrialConfig
    from loom.service_execution_oracle import parse_oracle_events

    controller, sandbox = tmp_path / "controller", tmp_path / "sandbox"
    controller.mkdir()
    business = sandbox / "app"
    business.mkdir(parents=True)
    private = sandbox / "loom/verifier/task"
    task, _, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "workdir": PurePosixPath(business),
    })})
    task.verifier.args["script_path"] = "verifier/run.sh"
    trial = TrialConfig(agent_name="oracle", agent_model=None, verifier_env_mode="shared")
    (controller / "instruction.md").write_text("Do not commit this harness input")
    (controller / "verifier").mkdir()
    original_wrapper = native_verifier_run_sh_templates()[wrapper_version]
    (controller / "verifier/run.sh").write_bytes(original_wrapper)
    (controller / "tests").mkdir()
    (controller / "solution").mkdir()
    business_q = shlex.quote(str(business))
    if requirement == "shell-whitelist":
        # Preserve the exact four-file whitelist that failed in RST 9c53e03e.
        solve = "for name in daftarnama2 result.txt empty_result.txt README.md; do echo output > \"$name\"; done\n"
        assertion = (
            'allowed = {"daftarnama2", "result.txt", "empty_result.txt", "README.md"}\n'
            'unexpected = set(os.listdir(root)) - allowed\n'
            'assert not unexpected, f"Unexpected files/dirs: {sorted(unexpected)}"\n'
            'assert all(os.path.isfile(os.path.join(root, name)) for name in allowed)\n'
        )
    else:
        # Preserve the whole-repository clean-worktree requirement from RST 841b7e07.
        solve = (
            'git init -q\ngit config user.name "Oracle Test"\n'
            'git config user.email oracle@example.test\necho output > README.md\n'
            'git add README.md\ngit commit -qm "Deliver artifact"\n'
        )
        assertion = (
            'assert not subprocess.check_output(["git", "-C", root, "status", "--porcelain"])\n'
        )
    (controller / "solution/solve.sh").write_text(
        "#!/bin/sh\nset -eu\n" + f"cd {business_q}\n" + solve,
    )
    # The authored test.sh runs against business state, even though its trusted
    # file is outside that tree. No chmod/ignore/cleanup may hide extra outputs.
    (controller / "tests/test.sh").write_text(
        "#!/bin/sh\nset -eu\n"
        "trap 'test -f \"$LOOM_HARBOR_LOG_DIR/reward.txt\" || echo 0 > \"$LOOM_HARBOR_LOG_DIR/reward.txt\"' EXIT\n"
        f"test \"$PWD\" = {business_q}\n"
        "python - <<'CHECK'\nimport os, subprocess\n"
        f"root = {str(business)!r}\n" + assertion + "CHECK\n"
        'echo .7 > "$LOOM_HARBOR_LOG_DIR/reward.txt"\n'
        'echo \'{"reward": 1}\' > "$LOOM_HARBOR_LOG_DIR/reward.json"\n',
    )
    driver = _FilesystemSandbox(sandbox)
    monkeypatch.setattr(module, "_PRIVATE_VERIFIER_INPUT_ROOT", PurePosixPath(private))
    monkeypatch.setattr(module, "_NATIVE_ORACLE_SOLUTION_DIR", PurePosixPath(private.parent / "oracle/solution"))
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")

    async def identity(_):
        return uuid4(), uuid4()

    monkeypatch.setattr(module, "_execution_identity", identity)
    await run_agent(controller, task, trial)
    assert not driver.stopped_processes  # Shared service/process lifetime is preserved.
    assert not (private / "tests/test.sh").exists()  # Withheld until grading.
    assert not list((private.parent / "oracle/solution").iterdir())  # Empty before snapshot.
    if extra_business_file:
        (business / "unexpected.txt").write_text("must remain a failure")
    await run_verifier(controller, task, trial)
    result = json.loads((controller / ".loom/verifier/output.json").read_text())
    assert result["rewards"] == {"reward": 0 if extra_business_file else 1}
    assert (controller / "verifier/run.sh").read_bytes() == original_wrapper
    trace = parse_oracle_events((controller / ".loom/agent/trajectory.jsonl").read_bytes())
    assert trace[0].cwd == str(business)
    assert not (business / "instruction.md").exists()
    assert not (business / "solution").exists()
    assert driver.stopped_processes and driver.stopped
    assert not (business / "tests").exists()
    assert not (business / "verifier").exists()
    assert not (business / ".loom").exists()


@pytest.mark.parametrize("workdir", ["/", "/tmp", "/loom/verifier/task"])
async def test_harbor_external_layout_does_not_broaden_supported_workdirs(tmp_path, workdir):
    from loom.harbor_verifier_script import native_verifier_run_sh_bytes

    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={
        "workdir": PurePosixPath(workdir),
    })})
    task.verifier.args["script_path"] = "verifier/run.sh"
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier/run.sh").write_bytes(native_verifier_run_sh_bytes())
    with pytest.raises(ValueError, match="task workdir requires"):
        await run_agent(tmp_path, task, trial)


@pytest.mark.parametrize("kind", ["native", "native-v1", "offline"])
@pytest.mark.parametrize("reward", [0, 1])
async def test_shared_harbor_private_inputs_and_report_leave_business_tree_untouched(
    tmp_path, monkeypatch, kind, reward,
):
    from loom import service_execution_sandbox_task as module
    from loom.harbor_verifier_script import (
        native_verifier_run_sh_bytes,
        native_verifier_workdir_run_sh_bytes,
        offline_verifier_run_sh_bytes,
    )

    task, trial, _ = _inputs()
    task.verifier.args["script_path"] = "verifier/run.sh"
    trial = trial.model_copy(update={"verifier_env_mode": "shared"})
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/check.py").write_text("private evaluator")
    (tmp_path / "verifier").mkdir()
    wrapper = native_verifier_run_sh_bytes() if kind == "native" else offline_verifier_run_sh_bytes()
    (tmp_path / "verifier/run.sh").write_bytes(wrapper)
    driver = Sandbox()
    business = {PurePosixPath("/app/README.md"): b"unchanged business output"}
    driver.filesystem.update(business)

    def execute(cmd, user, cwd, env):
        if env is not None and "LOOM_VERIFIER_OUTPUT" in env:
            private = module._PRIVATE_VERIFIER_INPUT_ROOT
            assert cwd == task.environment.workdir
            assert driver.filesystem[private / "tests/check.py"] == b"private evaluator"
            assert driver.filesystem[private / "verifier/run.sh"] == wrapper
            assert env["LOOM_TASK_DIR"] == str(private)
            assert env["LOOM_HARBOR_WORKDIR"] == "/app"
            if kind == "native":
                assert cmd == "/bin/sh /loom/verifier/run.sh"
                assert driver.filesystem[private.parent / "run.sh"] == native_verifier_workdir_run_sh_bytes()
            else:
                assert cmd == "/bin/sh /loom/verifier/task/verifier/run.sh"
            assert {p: body for p, body in driver.filesystem.items() if p.is_relative_to("/app")} == business
            driver.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = json.dumps({
                "rewards": {"resolved": reward},
            }).encode()
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    driver.exec_handler = execute
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    await run_verifier(tmp_path, task, trial)
    result = json.loads((tmp_path / ".loom/verifier/output.json").read_text())
    assert result["rewards"] == {"resolved": reward}
    assert (tmp_path / "verifier/run.sh").read_bytes() == wrapper  # Input remains immutable.


@pytest.mark.parametrize("raw", ["", "not-a-budget", "0", "-1", str(10 * 1024**3 + 1)])
def test_workspace_archive_budget_rejects_invalid_controller_limits(tmp_path, monkeypatch, raw):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    monkeypatch.setattr(module, "_frozen_network_policy", lambda: task.environment.baseline_network_policy)
    monkeypatch.setenv("LOOM_WORKSPACE_MAX_ARCHIVE_BYTES", raw)
    with pytest.raises(module.ServiceExecutionTaskError, match="workspace_archive_limit_invalid"):
        module.sandbox_driver("task-sandbox", task, trial)


@pytest.mark.parametrize("raw, expected_mib", [(None, 256), (str(400 * 1024**2), 400), (str(1024**3), 512)])
@pytest.mark.parametrize("guest", [False, True])
def test_workspace_archive_budget_is_independent_and_bounded_by_storage(
    monkeypatch, raw, expected_mib, guest,
):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = _inputs()
    task = task.model_copy(update={"environment": task.environment.model_copy(update={"storage_mb": 512})})
    monkeypatch.setattr(module, "_frozen_network_policy", lambda: task.environment.baseline_network_policy)
    monkeypatch.setattr(module, "in_guest", lambda *_: guest)
    monkeypatch.setenv("LOOM_SANDBOX_MAX_TRANSFER_BYTES", str(300 * 1024**2))
    if raw is None:
        monkeypatch.delenv("LOOM_WORKSPACE_MAX_ARCHIVE_BYTES", raising=False)
        expected_mib = 300 if guest else 256
    else:
        monkeypatch.setenv("LOOM_WORKSPACE_MAX_ARCHIVE_BYTES", raw)
    driver = module.sandbox_driver("task-sandbox", task, trial)
    assert driver._max_transfer == (300 if guest else 256) * 1024**2
    assert driver._max_workspace_archive == expected_mib * 1024**2


@pytest.mark.parametrize("agent_name", ["oracle", "terminus-2"])
async def test_harbor_delivers_only_oracle_reference_tree_at_absolute_solution(tmp_path, monkeypatch, agent_name):
    from loom import service_execution_sandbox_task as module
    from loom.harbor_verifier_script import native_verifier_run_sh_bytes
    from loom.models.trial import TrialConfig

    task, model_trial, _ = _inputs()
    task.verifier.args["script_path"] = "verifier/run.sh"
    trial = (TrialConfig(agent_name="oracle", agent_model=None)
             if agent_name == "oracle" else model_trial)
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier/run.sh").write_bytes(native_verifier_run_sh_bytes())
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/private.py").write_text("private grader")
    (tmp_path / "solution/helpers").mkdir(parents=True)
    (tmp_path / "solution/solve.sh").write_text("#!/bin/sh\npython3 /solution/helpers/solve.py\n")
    (tmp_path / "solution/helpers/solve.py").write_text("print('solved')\n")
    (tmp_path / "instruction.md").write_text("Do the task")
    (tmp_path / "README.md").write_text("Documentation must not overwrite read-only business files")

    class ReadOnlyBusiness(Sandbox):
        async def upload(self, src, dst):
            if dst.is_relative_to("/app"):
                raise PermissionError("business tree is read-only for image USER nobody")
            await super().upload(src, dst)

    driver = ReadOnlyBusiness()
    driver.filesystem[PurePosixPath("/app/authored.txt")] = b"image content"
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    async def identity(_):
        return uuid4(), uuid4()
    async def execute(**kwargs):
        assert driver.filesystem[PurePosixPath("/app/authored.txt")] == b"image content"
        assert not any(p.is_relative_to("/tests") or p.is_relative_to("/loom/verifier") for p in driver.filesystem)
        if agent_name == "oracle":
            assert kwargs["solution_dir"] == PurePosixPath("/solution")
            assert kwargs["execution_cwd"] == PurePosixPath("/app")
            assert driver.filesystem[PurePosixPath("/solution/helpers/solve.py")] == b"print('solved')\n"
        else:
            assert not any(p.is_relative_to("/solution") for p in driver.filesystem)
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")
    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_oracle" if agent_name == "oracle" else "run_terminus2", execute)
    await module.run_agent(tmp_path, task, trial)
