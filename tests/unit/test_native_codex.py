"""#2311: Codex on native execution: spec, install, event mapping, ledger
accounting and materialization."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx
import pytest

import loom.hosted_harness as hosted
import loom.service_execution_sandbox_task as controller
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.hosted_harness import CODEX, CODEX_INSTALL_ROOT, InstallSource, PinnedArchive
from loom.models.exec import ExecResult
from loom.models.trajectory import AgentThoughtEvent, LLMCallEvent, ToolUseEvent
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_codex import (
    codex_usage,
    ledger_calls,
    map_codex_event,
    parse_codex_events,
)
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
)
from loom.service_execution_task import ServiceExecutionTaskError
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    build_canonical_events,
    validate_usage_accounting,
)
from tests.unit.test_service_execution_materialization import _REVISION, _RUNTIME_IMAGE, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_NOW = datetime(2026, 10, 6, tzinfo=UTC)
# Lines captured from the real Codex 0.146.0 binary against a scripted endpoint.
_REAL_LINES = [
    {"type": "thread.started", "thread_id": "01a111ca-96ae-77a0-8695-d70f815fe7d7"},
    {"type": "turn.started"},
    {"type": "item.started", "item": {"id": "item_1", "type": "command_execution",
     "command": "/bin/bash -lc 'echo from-codex > proof.txt'", "aggregated_output": "", "exit_code": None,
     "status": "in_progress"}},
    {"type": "item.completed", "item": {"id": "item_1", "type": "command_execution",
     "command": "/bin/bash -lc 'echo from-codex > proof.txt'", "aggregated_output": "from-codex\n",
     "exit_code": 0, "status": "completed"}},
    {"type": "item.completed", "item": {"id": "item_2", "type": "agent_message", "text": "wrote proof.txt"}},
    {"type": "turn.completed", "usage": {"input_tokens": 20, "cached_input_tokens": 0, "output_tokens": 4}},
]


def _trial(name: str = "gpt-4.1-mini") -> TrialConfig:
    return TrialConfig(agent_name="codex", agent_model=ModelSpec(provider="openai", name=name))


def _row(trial_id, *, model: str = "gpt-4.1-mini", tokens: tuple[int, int] = (10, 2)) -> dict:
    return {"id": str(uuid4()), "trial_id": str(trial_id), "step_id": "agent", "dialect": "openai_facade",
            "model": model, "input_tokens": tokens[0], "output_tokens": tokens[1], "cost_usd": 0.001,
            "rate_card_hash": "r", "provider_extras": {}, "captured_at": _NOW.isoformat(),
            "finish_reason": "stop", "attempt": 1}


# --- spec -------------------------------------------------------------------------


def test_codex_is_selectable_on_native_execution() -> None:
    task, _, _ = _inputs()
    assert CODEX.readiness == "ready" and CODEX.natively_runnable
    assert "codex" in hosted.NATIVE_EXECUTION_AGENT_NAMES
    assert "direct_completion_required" not in automatic_service_execution_rejections(
        task, _trial(), source_provenance=_provenance(),
    )


def test_codex_pins_a_verified_release_into_a_cacheable_root() -> None:
    setup = CODEX.setup
    assert setup is not None and setup.archive is not None and not setup.install
    assert setup.archive.url == "https://registry.npmjs.org/@openai/codex/-/codex-0.146.0-linux-x64.tgz"
    assert setup.archive.integrity.startswith("sha512-")
    assert setup.cacheable and setup.install_root == CODEX_INSTALL_ROOT
    assert CODEX.gateway_protocol == "openai-responses" and "exec_streaming" in CODEX.required_driver_capabilities


@pytest.mark.parametrize(("kwargs", "message"), [
    ({"url": "http://registry.npmjs.org/x.tgz"}, "plain https"),
    ({"url": "https://registry.npmjs.org/x.tgz?y=1"}, "plain https"),
    ({"integrity": "sha256-abc"}, "sha512"),
    ({"integrity": "sha512-not*base64"}, "sha512"),
    ({"strip_components": 9}, "strip_components"),
])
def test_pinned_archive_is_strict(kwargs: dict, message: str) -> None:
    base = {"url": "https://registry.npmjs.org/x.tgz",
            "integrity": "sha512-" + base64.b64encode(b"\0" * 64).decode()}
    with pytest.raises(ValueError, match=message):
        PinnedArchive(**{**base, **kwargs})


def test_archive_host_must_be_a_declared_source() -> None:
    setup = CODEX.setup
    assert setup is not None
    with pytest.raises(ValueError, match="declared https install source"):
        replace(setup, sources=(InstallSource("pypi.org"),))
    with pytest.raises(ValueError, match="exactly one"):
        replace(setup, install=("true",))


def test_codex_plan_freezes_setup_egress_cache_and_native_evidence() -> None:
    task, _, profile = _inputs()
    plan = compile_service_execution_plan(
        task=task, trial=_trial(), profile=profile, source_provenance=_provenance(), task_revision_sha256=_REVISION,
    )

    assert plan.main.argv[4] == "codex" and [p.argv[4] for p in plan.setup] == ["setup"]
    assert plan.setup_egress is not None and [d.host for d in plan.setup_egress.destinations] == ["registry.npmjs.org"]
    assert plan.task_egress is None  # the task stays gateway-only
    assert plan.setup_cache is not None and plan.setup_cache.install_root == CODEX_INSTALL_ROOT
    paths = {o.relative_path for o in plan.output_declarations}
    assert {"artifacts/codex/events.jsonl", "diagnostics/harness-cache.json", "trajectory/events.jsonl"} <= paths
    sandbox = next(s for s in plan.sidecars if s.role_name == "task-sandbox")
    assert int(sandbox.argv[sandbox.argv.index("--exec-timeout-seconds") + 1]) >= 900


def test_codex_is_rejected_on_guest_execution() -> None:
    from tests.unit.test_guest_execution_materialization import _guest_inputs

    task, _, profile = _guest_inputs("nested_docker")
    reasons = automatic_service_execution_rejections(
        task, _trial(), source_provenance=_provenance(), supported_capabilities=profile.supported_guest_capabilities,
    )
    assert "guest_driver_capabilities_unsupported" in reasons


# --- install ------------------------------------------------------------------------


class _Sandbox:
    def __init__(self) -> None:
        self.uploads: list[PurePosixPath] = []
        self.commands: list[str] = []

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self.uploads.append(dst)

    async def exec(self, cmd: str, **_: object) -> ExecResult:
        self.commands.append(cmd)
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0.0)


def _serve(monkeypatch, payload: bytes, status: int = 200) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, content=payload)

    real = httpx.AsyncClient
    monkeypatch.setattr(controller.httpx, "AsyncClient", lambda **kw: real(
        transport=httpx.MockTransport(handler), **{k: v for k, v in kw.items() if k != "proxy"}))
    return seen


def _setup_for(payload: bytes):
    integrity = "sha512-" + base64.b64encode(hashlib.sha512(payload).digest()).decode()
    return replace(CODEX.setup, archive=replace(CODEX.setup.archive, integrity=integrity))  # type: ignore[arg-type]


async def test_archive_is_verified_before_any_byte_reaches_the_sandbox(monkeypatch, tmp_path) -> None:
    sandbox, payload = _Sandbox(), b"release-bytes"
    _serve(monkeypatch, b"tampered-bytes")

    with pytest.raises(ServiceExecutionTaskError, match="integrity mismatch"):
        await controller._install_pinned_archive(sandbox, tmp_path, _setup_for(payload), {"https_proxy": "http://127.0.0.1:1"}, None)
    assert sandbox.uploads == [] and sandbox.commands == []
    assert not list((tmp_path / ".loom").rglob("*.download"))


async def test_verified_archive_is_uploaded_and_extracted(monkeypatch, tmp_path) -> None:
    sandbox, payload = _Sandbox(), b"release-bytes"
    seen = _serve(monkeypatch, payload)

    await controller._install_pinned_archive(sandbox, tmp_path, _setup_for(payload), {"https_proxy": "http://127.0.0.1:1"}, None)

    assert [str(r.url) for r in seen] == [CODEX.setup.archive.url]  # type: ignore[union-attr]
    assert sandbox.uploads == [PurePosixPath("/tmp/loom-harness-release.tar.gz")]
    assert "--strip-components=3" in sandbox.commands[0] and CODEX_INSTALL_ROOT in sandbox.commands[0]
    assert "command -v tar" in sandbox.commands[0]  # a clear error when the image lacks tar/gzip


async def test_failed_download_is_a_setup_failure(monkeypatch, tmp_path) -> None:
    _serve(monkeypatch, b"", status=404)
    with pytest.raises(ServiceExecutionTaskError, match="HTTP 404"):
        await controller._install_pinned_archive(_Sandbox(), tmp_path, _setup_for(b"x"), {"https_proxy": "http://127.0.0.1:1"}, None)


# --- events and accounting --------------------------------------------------------------


def test_real_codex_lines_map_to_typed_events() -> None:
    trial_id = uuid4()
    events = [e for line in _REAL_LINES if (e := map_codex_event(line, trial_id=trial_id, emitted_at=_NOW))]

    assert [type(e) for e in events] == [ToolUseEvent, AgentThoughtEvent]
    shell, message = events
    assert isinstance(shell, ToolUseEvent) and shell.tool_name == "shell"
    assert shell.result == {"exit_code": 0, "status": "completed", "output": "from-codex\n"}
    assert isinstance(message, AgentThoughtEvent) and message.content == "wrote proof.txt"
    failure = map_codex_event({"type": "turn.failed", "error": {"message": "boom"}}, trial_id=trial_id, emitted_at=_NOW)
    assert isinstance(failure, AgentThoughtEvent) and failure.sdk_event_type == "codex.turn.failed"
    long = map_codex_event({"type": "item.completed", "item": {"type": "agent_message", "text": "x" * 100_000}},
                           trial_id=trial_id, emitted_at=_NOW)
    assert isinstance(long, AgentThoughtEvent) and len(long.content) < 70_000


def test_ledger_is_the_source_of_model_calls_and_rejects_other_models() -> None:
    trial_id = uuid4()
    calls = ledger_calls([_row(trial_id), _row(trial_id, tokens=(5, 1))], trial=_trial(), trial_id=trial_id)
    assert [c.input_tokens for c in calls] == [10, 5]
    with pytest.raises(ValueError, match="another model identity"):
        ledger_calls([_row(trial_id, model="gpt-5")], trial=_trial(), trial_id=trial_id)
    with pytest.raises(ValueError, match="invalid or duplicate"):
        ledger_calls([_row(uuid4())], trial=_trial(), trial_id=trial_id)


def _trace(trial_id, *, model: str = "gpt-4.1-mini"):
    events = [e for line in _REAL_LINES if (e := map_codex_event(line, trial_id=trial_id, emitted_at=_NOW))]
    rows = [_row(trial_id, model=model), _row(trial_id, tokens=(7, 3))]
    events.extend(ledger_calls(rows, trial=_trial(), trial_id=trial_id) if model == "gpt-4.1-mini" else [])
    events = [e.model_copy(update={"seq": i}) for i, e in enumerate(events)]
    body = b"".join(e.model_dump_json().encode() + b"\n" for e in events)
    return events, body, rows


def test_trace_and_usage_validation() -> None:
    trial_id = uuid4()
    events, body, _ = _trace(trial_id)
    usage = codex_usage(events, _trial())
    assert usage["call_count"] == 2 and usage["totals"]["input_tokens"] == 17

    validate_usage_accounting(trace_body=body, usage_body=json.dumps(usage).encode(), trial_config=_trial())
    drift = {**usage, "call_count": 1}
    with pytest.raises(MaterializationIntegrityError, match="usage_output_identity_drift"):
        validate_usage_accounting(trace_body=body, usage_body=json.dumps(drift).encode(), trial_config=_trial())
    with pytest.raises(ValueError, match="another model identity"):
        parse_codex_events(body, trial=_trial("gpt-5"))
    foreign = LLMCallEvent.model_validate({**json.loads(body.splitlines()[-1]), "trial_id": str(uuid4()),
                                           "seq": len(events)})
    with pytest.raises(ValueError, match="another Trial"):
        parse_codex_events(body + foreign.model_dump_json().encode() + b"\n", trial=_trial())


def _result() -> ExecutionRuntimeResultV1:
    task, _, _ = _inputs()
    return ExecutionRuntimeResultV1.model_validate({
        "schema_version": "loom.execution-runtime-result.v1", "runtime_contract_sha256": "sha256:" + "1" * 64,
        "candidate_sha": "1" * 40, "task_revision_sha256": _REVISION, "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt", "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": task.environment.docker_image, "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "3" * 64, "execution_class_id": "linux-amd64-cpu-pod-v1",
        "status": "succeeded", "started_at": _NOW, "finished_at": _NOW, "phases": [], "outputs": [],
        "verifier_rewards": {"passed": 1}, "partial_evidence": False,
    })


def test_canonical_events_take_model_calls_from_the_db_ledger() -> None:
    trial_id = uuid4()
    _, body, rows = _trace(trial_id)
    task, _, _ = _inputs()

    canonical = build_canonical_events(
        trial_id=trial_id, task_id="task-1", task_config=task, trial_config=_trial(), runtime_result=_result(),
        trace_body=body, verifier_body=b'{"rewards":{"passed":1}}', gateway_calls=rows,
    )

    kinds = [e.kind for e in canonical]
    assert kinds.count("llm_call") == 2 and "tool_use" in kinds and "agent_thought" in kinds
    assert canonical[-1].final_state == "succeeded"
    assert [e.seq for e in canonical] == list(range(len(canonical)))

    # A runtime trace that disagrees with the DB ledger is refused.
    with pytest.raises(MaterializationIntegrityError, match="trajectory_invalid"):
        build_canonical_events(
            trial_id=trial_id, task_id="task-1", task_config=task, trial_config=_trial(), runtime_result=_result(),
            trace_body=body, verifier_body=b'{"rewards":{"passed":1}}', gateway_calls=rows[:1],
        )


# --- runner ---------------------------------------------------------------------------


class _CodexSandbox:
    def __init__(self, *, exit_code: int = 0) -> None:
        self.exit_code = exit_code
        self.calls: list[dict] = []

    async def exec_streaming(self, argv, *, env_vars, cwd, timeout_sec=None):
        from loom.driver.base import ExecHandle

        self.calls.append({"argv": argv, "env": dict(env_vars), "cwd": cwd})

        async def stdout():
            for line in _REAL_LINES:
                yield json.dumps(line).encode() + b"\n"

        async def stderr():
            if False:
                yield b""

        async def wait() -> int:
            return self.exit_code

        async def kill() -> None: ...

        return ExecHandle(pid=1, stdout=stdout(), stderr=stderr(), _wait=wait, _kill=kill)


async def _run(sandbox: _CodexSandbox, tmp_path: Path, trial_id, rows: list[dict]) -> None:
    from loom.service_execution_codex import run_codex

    task, _, _ = _inputs()

    async def ledger(requested):
        assert requested == trial_id
        return rows

    await run_codex(
        driver=sandbox, workspace=tmp_path / "agent", task_config=task, trial_config=_trial(), trial_id=trial_id,
        instruction="Do it.", ledger=ledger,
        model_environment={"OPENAI_BASE_URL": "http://127.0.0.1:40000/v1", "OPENAI_API_KEY": "loom_workload_proxy"},
    )


async def test_runner_environment_keeps_codex_out_of_the_workdir(tmp_path) -> None:
    from loom.hosted_harness import CODEX_HOME, CODEX_TMPDIR

    sandbox, trial_id = _CodexSandbox(), uuid4()
    await _run(sandbox, tmp_path, trial_id, [_row(trial_id)])

    (call,) = sandbox.calls
    task, _, _ = _inputs()
    assert call["cwd"] == task.environment.workdir
    assert call["env"]["CODEX_HOME"] == CODEX_HOME and call["env"]["TMPDIR"] == CODEX_TMPDIR
    assert not call["env"]["CODEX_HOME"].startswith(str(task.environment.workdir))
    assert call["env"]["OPENAI_API_KEY"] == "loom_workload_proxy"
    assert call["env"]["PATH"].startswith(f"{CODEX_INSTALL_ROOT}/bin:")
    assert "-c 'web_search=\"disabled\"'" in call["argv"][2]
    events = parse_codex_events((tmp_path / "agent" / "trajectory.jsonl").read_bytes(), trial=_trial(), trial_id=trial_id)
    assert [e.kind for e in events] == ["tool_use", "agent_thought", "llm_call"]
    assert json.loads((tmp_path / "agent" / "codex" / "events.jsonl").read_text().splitlines()[0])["type"] == "thread.started"


async def test_failed_codex_still_records_its_model_calls(tmp_path) -> None:
    from loom.errors import AgentError

    trial_id = uuid4()
    with pytest.raises(AgentError, match="codex exited rc=3"):
        await _run(_CodexSandbox(exit_code=3), tmp_path, trial_id, [_row(trial_id)])
    usage = json.loads((tmp_path / "agent" / "usage.json").read_text())
    assert usage["call_count"] == 1


async def test_off_model_ledger_call_fails_the_run(tmp_path) -> None:
    trial_id = uuid4()
    with pytest.raises(ValueError, match="another model identity"):
        await _run(_CodexSandbox(), tmp_path, trial_id, [_row(trial_id, model="gpt-5")])
