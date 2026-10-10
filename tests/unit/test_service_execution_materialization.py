from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    ServiceExecutionRuntimeProfileV1,
    automatic_service_execution_rejections,
    build_service_execution_input_manifest,
    compile_service_execution_plan,
    load_service_execution_runtime_profile,
    prepare_service_execution_input_manifest,
    service_execution_input_binding,
)
from loom.trajectory.storage import FakeObjectStore, ObjectReadback
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    ServiceExecutionMaterializer,
    build_canonical_events,
    run_service_execution_materializer_loop,
)
from tests.support.execution_image_admission import signed_image_admission_bundle

_TASK_IMAGE = "registry.example/task@sha256:" + "a" * 64
_RUNTIME_IMAGE = "registry.example/runtime@sha256:" + "b" * 64
_REVISION = "sha256:" + "c" * 64


def _task(**updates: object) -> TaskConfig:
    raw: dict[str, object] = {
        "schema_version": "1",
        "task": {"id": "task-1", "name": "Automatic Nebius task"},
        "environment": {
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "docker_image": _TASK_IMAGE,
            "cpus": 1,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "network_policies_supported": ["gateway-only"],
            "baseline_network_policy": {"kind": "gateway-only"},
        },
        "agent": {"name": "direct-completion"},
        "verifier": {
            "name": "script",
            "args": {"script_path": "verifier/check.sh"},
        },
        "steps": [
            {
                "name": "main",
                "instruction_file": "instruction.md",
                "artifacts": ["answer.txt"],
            }
        ],
    }
    raw.update(updates)
    return TaskConfig.model_validate(raw)


def _trial() -> TrialConfig:
    return TrialConfig(
        agent_name="direct-completion",
        agent_model=ModelSpec(provider="openai", name="gpt-5"),
        request_params={"temperature": 0.2},
    )


@pytest.mark.parametrize("agent_name", ["terminus-2", "direct-completion", "litellm", "oracle"])
def test_continuation_admission_uses_resolved_agent_not_source_default(agent_name):
    task = _task(agent={"name": "oracle", "continue_until_timeout": True})
    trial = _trial().model_copy(update={"agent_name": agent_name})
    reasons = automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert ("agent_continuation_unsupported" in reasons) is (agent_name != "terminus-2")


def test_default_completion_does_not_restrict_direct_completion():
    reasons = automatic_service_execution_rejections(_task(), _trial(), source_provenance=_provenance())
    assert "agent_continuation_unsupported" not in reasons


@pytest.mark.parametrize("workdir", ["/media/project", "/opt/project", "/root/project"])
@pytest.mark.parametrize("agent", ["terminus-2", "direct-completion"])
def test_custom_workdir_admission_is_scoped_to_terminus(workdir: str, agent: str) -> None:
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(
        update={"workdir": PurePosixPath(workdir)},
    )})
    trial = _trial().model_copy(update={"agent_name": agent})
    reasons = automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert ("standard_workspace_identity_required" in reasons) is (agent != "terminus-2")


@pytest.mark.parametrize("workdir", ["/", "/loom", "/opt", "/tests", "/var", "/tmp", "/app/../tests"])
def test_terminus_rejects_protected_workdir(workdir: str) -> None:
    task = _task()
    task = task.model_copy(update={"environment": task.environment.model_copy(
        update={"workdir": PurePosixPath(workdir)},
    )})
    reasons = automatic_service_execution_rejections(
        task, _trial().model_copy(update={"agent_name": "terminus-2"}),
        source_provenance=_provenance(),
    )
    assert "standard_workspace_identity_required" in reasons


def _provenance() -> dict[str, object]:
    return {
        "service_execution_input": {
            "schema_version": "loom.service-execution-input.v1",
            "manifest_uri": "s3://artifacts/task-inputs/task-1.json",
            "manifest_sha256": "sha256:" + "d" * 64,
            "file_count": 3,
            "total_bytes": 4096,
        }
    }


def _profile() -> ServiceExecutionRuntimeProfileV1:
    return ServiceExecutionRuntimeProfileV1(
        candidate_sha="1" * 40,
        execution_class_id="linux-amd64-cpu-pod-v1",
        task_image_ref=_TASK_IMAGE,
        runtime_image_ref=_RUNTIME_IMAGE,
        runtime_binary_sha256="sha256:" + "e" * 64,
        image_admission=signed_image_admission_bundle((_TASK_IMAGE, _RUNTIME_IMAGE)),
    )


def test_artifact_input_capability_preserves_frozen_profile_serialization() -> None:
    original = _profile().model_dump(mode="json")
    assert "supports_task_artifact_inputs" not in original
    loaded = load_service_execution_runtime_profile(json.dumps(original))
    assert loaded is not None and not loaded.supports_task_artifact_inputs
    assert loaded.model_dump(mode="json") == original
    enabled = loaded.model_copy(update={"supports_task_artifact_inputs": True})
    assert enabled.model_dump(mode="json") == {**original, "supports_task_artifact_inputs": True}


@pytest.mark.parametrize(("class_id", "web_egress"), [
    ("linux-amd64-cpu-pod-v1", True),
    ("linux-amd64-cpu-web-pod-v1", False),
    ("unqualified-class", False),
])
def test_runtime_profile_load_rejects_conflicting_class_capability(
    class_id: str, web_egress: bool,
) -> None:
    payload = _profile().model_dump(mode="json")
    payload.update(execution_class_id=class_id, supports_task_web_egress=web_egress)
    with pytest.raises(ValueError, match="execution class"):
        load_service_execution_runtime_profile(json.dumps(payload))


@pytest.mark.parametrize(("class_id", "web_egress"), [
    ("linux-amd64-cpu-pod-v1", None),
    ("linux-amd64-cpu-pod-v1", False),
    ("linux-amd64-cpu-web-pod-v1", True),
])
def test_runtime_profile_load_preserves_matching_class_capability(
    class_id: str, web_egress: bool | None,
) -> None:
    payload = _profile().model_dump(mode="json")
    payload["execution_class_id"] = class_id
    if web_egress is not None:
        payload["supports_task_web_egress"] = web_egress
    profile = load_service_execution_runtime_profile(json.dumps(payload))
    assert profile is not None
    assert profile.execution_class_id == class_id
    assert profile.supports_task_web_egress is (web_egress is True)


@pytest.mark.parametrize("architecture", ["x86_64", "arm64", "any"])
async def test_x86_policy_applies_to_shared_validation_and_image_planning(architecture: str) -> None:
    from unittest.mock import AsyncMock, MagicMock

    from loom.task_image_materialization import required_task_image_architectures
    from loom_service.task_config_validation import split_valid_task_configs

    raw = _task().model_dump(mode="json")
    raw["environment"].update(cpu_arch=architecture, docker_image=None, dockerfile="Dockerfile")
    task = TaskConfig.model_validate(raw)
    result = MagicMock()
    result.all.return_value = [(task.task.id, task.model_dump(mode="json"))]
    session = AsyncMock()
    session.execute.return_value = result

    # All new execution uses x86; generic architecture selects that target.
    valid, invalid = await split_valid_task_configs(session, [task.task.id])
    if architecture == "arm64":
        assert valid == []
        assert [item.task_id for item in invalid] == [task.task.id]
        assert "x86_64 only" in invalid[0].detail
        with pytest.raises(ValueError, match="x86_64 only"):
            required_task_image_architectures(task)
    else:
        assert valid == [task.task.id]
        assert invalid == []
        assert required_task_image_architectures(task) == ("x86_64",)
    reasons = automatic_service_execution_rejections(
        task, _trial(), source_provenance=_provenance(), allow_task_image_preparation=True,
    )
    assert ("linux_x86_64_required" in reasons) == (architecture == "arm64")


def test_input_manifest_is_canonical_and_preserves_executable_mode(tmp_path: Path) -> None:
    (tmp_path / "instruction.md").write_text("hello\n", encoding="utf-8")
    script = tmp_path / "verifier" / "check.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)

    manifest = build_service_execution_input_manifest(
        tmp_path,
        task_checksum=_REVISION,
    )

    assert [item.relative_path for item in manifest.files] == [
        "instruction.md",
        "verifier/check.sh",
    ]
    assert [item.mode for item in manifest.files] == ["0644", "0755"]
    assert json.loads(manifest.canonical_bytes()) == manifest.model_dump(mode="json")


def test_input_manifest_sorts_by_utf8_relative_path_not_pathlib(
    tmp_path: Path,
) -> None:
    # Pathlib component order puts tests/test/ before tests/test.sh; UTF-8
    # byte order of the relative POSIX path does the opposite ('.' < '/').
    nested = tmp_path / "tests" / "test"
    nested.mkdir(parents=True)
    (nested / "01-factorial.scm").write_text("1\n", encoding="utf-8")
    (tmp_path / "tests" / "test.sh").write_text("#!/bin/sh\n", encoding="utf-8")

    manifest = build_service_execution_input_manifest(
        tmp_path,
        task_checksum=_REVISION,
    )

    assert [item.relative_path for item in manifest.files] == [
        "tests/test.sh",
        "tests/test/01-factorial.scm",
    ]


def test_input_manifest_omits_only_top_level_transport_metadata(tmp_path: Path) -> None:
    (tmp_path / ".loom-bundle-files.v1.json").write_text("stale transport metadata")
    (tmp_path / ".authored").write_text("authored dotfile")
    nested = tmp_path / "inputs" / ".loom-bundle-files.v1.json"
    nested.parent.mkdir()
    nested.write_text("authored nested file")
    nested.chmod(0o755)
    (tmp_path / "task.toml").write_text("task")

    manifest = build_service_execution_input_manifest(tmp_path, task_checksum=_REVISION)

    assert [(item.relative_path, item.mode) for item in manifest.files] == [
        (".authored", "0644"),
        ("inputs/.loom-bundle-files.v1.json", "0755"),
        ("task.toml", "0644"),
    ]
    for item in manifest.files:
        body = (tmp_path / item.relative_path).read_bytes()
        assert item.size_bytes == len(body)
        assert item.sha256 == "sha256:" + hashlib.sha256(body).hexdigest()


@pytest.mark.parametrize("entry", ["symlink", "fifo"])
@pytest.mark.parametrize("name", [".loom-bundle-files.v1.json", "authored-input"])
def test_input_manifest_rejects_nonregular_files_even_at_transport_path(
    tmp_path: Path, entry: str, name: str,
) -> None:
    import os

    (tmp_path / "task.toml").write_text("task")
    path = tmp_path / name
    if entry == "symlink":
        path.symlink_to(tmp_path / "task.toml")
    else:
        os.mkfifo(path)
    with pytest.raises(ValueError, match="non-regular file"):
        build_service_execution_input_manifest(tmp_path, task_checksum=_REVISION)


def test_prepare_service_execution_input_manifest_binding_matches_body(
    tmp_path: Path,
) -> None:
    (tmp_path / "instruction.md").write_text("hello\n", encoding="utf-8")
    body, provenance = prepare_service_execution_input_manifest(
        tmp_path,
        task_checksum=_REVISION,
        bucket="artifacts",
        manifest_key="bench/task/service-execution-input.json",
    )
    binding = service_execution_input_binding(provenance)
    assert binding is not None
    assert binding.manifest_uri == (
        "s3://artifacts/bench/task/service-execution-input.json"
    )
    assert binding.manifest_sha256 == "sha256:" + hashlib.sha256(body).hexdigest()
    assert binding.file_count == 1
    assert binding.total_bytes == len(b"hello\n")
    assert "immutable_task_input_unavailable" not in automatic_service_execution_rejections(
        _task(),
        _trial(),
        source_provenance=provenance,
        allow_task_image_preparation=True,
    )


def test_ordinary_task_compiles_to_profile_owned_nebius_plan() -> None:
    task = _task()
    trial = _trial()

    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )

    assert plan.execution_class_id == "linux-amd64-cpu-pod-v1"
    assert plan.task_input is not None
    assert plan.task_input.file_count == 3
    assert plan.main.argv == (
        "python",
        "-m",
        "loom.service_execution_task",
        "direct-completion",
    )
    assert plan.main.environment["LOOM_TASK_MODEL"] == "openai/gpt-5"
    assert plan.verifier is not None
    assert plan.verifier.argv == ("/bin/sh", "verifier/check.sh")
    assert plan.task_resources.cpu_millis == 1000
    assert plan.workspace_mib == 2048
    assert [item.model_dump(mode="json") for item in plan.output_declarations] == [
        {
            "source_path": "answer.txt",
            "relative_path": "artifacts/answer.txt",
            "kind": "task_artifact",
            "required": True,
        },
        {
            "source_path": ".loom/agent/trajectory.jsonl",
            "relative_path": "trajectory/events.jsonl",
            "kind": "trajectory",
            "required": True,
        },
        {
            "source_path": ".loom/agent/usage.json",
            "relative_path": "accounting/usage.json",
            "kind": "usage",
            "required": True,
        },
        {
            "source_path": ".loom/verifier/output.json",
            "relative_path": "verifier/output.json",
            "kind": "verifier",
            "required": True,
        },
    ]


def test_ordinary_task_preserves_complete_multi_artifact_and_trace_contract() -> None:
    raw = _task().model_dump(mode="json")
    raw["steps"][0]["artifacts"] = ["answer.txt"]
    raw["steps"][0]["required_artifacts"] = ["answer.txt", "reasoning.md"]
    plan = compile_service_execution_plan(
        task=TaskConfig.model_validate(raw),
        trial=_trial(),
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )

    assert plan.main.environment["LOOM_TASK_ARTIFACTS_JSON"] == ('["answer.txt","reasoning.md"]')
    assert [item.relative_path for item in plan.output_declarations] == [
        "artifacts/answer.txt",
        "artifacts/reasoning.md",
        "trajectory/events.jsonl",
        "accounting/usage.json",
        "verifier/output.json",
    ]
    assert all(item.required for item in plan.output_declarations)
    assert plan.verifier_execution == "in_attempt"


def test_automatic_compiler_rejects_unimplemented_task_semantics() -> None:
    task = _task(
        environment={
            **_task().environment.model_dump(mode="json"),
            "environment": {"UNHANDLED": "value"},
        }
    )

    assert automatic_service_execution_rejections(
        task,
        _trial(),
        source_provenance={},
    ) == (
        "immutable_task_input_unavailable",
        "extended_environment_unsupported",
    )


def test_manifest_digest_matches_persisted_binding_bytes(tmp_path: Path) -> None:
    (tmp_path / "task.toml").write_text("schema_version = '1'\n", encoding="utf-8")
    manifest = build_service_execution_input_manifest(tmp_path, task_checksum=_REVISION)
    body = manifest.canonical_bytes()
    assert "sha256:" + hashlib.sha256(body).hexdigest() == (
        "sha256:" + hashlib.sha256(manifest.canonical_bytes()).hexdigest()
    )


def test_complete_source_trace_projects_to_training_grade_loom_events() -> None:
    now = datetime.now(UTC)
    plan = compile_service_execution_plan(
        task=_task(),
        trial=_trial(),
        task_revision_sha256=_REVISION,
        source_provenance=_provenance(),
        profile=_profile(),
    )
    payloads = {
        "artifacts/answer.txt": b"42\n",
        "trajectory/events.jsonl": b"trace\n",
        "accounting/usage.json": b"{}\n",
        "verifier/output.json": b'{"rewards":{"passed":1.0}}',
    }
    runtime_result = ExecutionRuntimeResultV1.model_validate(
        {
            "schema_version": "loom.execution-runtime-result.v1",
            "runtime_contract_sha256": "sha256:" + "f" * 64,
            "candidate_sha": plan.candidate_sha,
            "task_revision_sha256": plan.task_revision_sha256,
            "command_identity_sha256": plan.command_identity_sha256,
            "execution_role": "attempt",
            "container_roles": ["execution", "agent", "verifier"],
            "task_image_ref": plan.task_image_ref,
            "runtime_image_ref": plan.runtime_image_ref,
            "runtime_binary_sha256": plan.runtime_binary_sha256,
            "execution_class_id": plan.execution_class_id,
            "status": "succeeded",
            "started_at": now,
            "finished_at": now + timedelta(seconds=2),
            "phases": [],
            "outputs": [
                {
                    **declaration.model_dump(mode="json"),
                    "state": "captured",
                    "size_bytes": len(payloads[declaration.relative_path]),
                    "sha256": "sha256:"
                    + hashlib.sha256(payloads[declaration.relative_path]).hexdigest(),
                }
                for declaration in plan.output_declarations
            ],
            "verifier_rewards": {"passed": 1.0},
            "partial_evidence": False,
        }
    )
    trace = (
        json.dumps(
            {
                "schema_version": "loom.service-execution-llm-call.v1",
                "turn": 0,
                "started_at": now.isoformat(),
                "finished_at": (now + timedelta(seconds=1)).isoformat(),
                "model": "openai/gpt-5",
                "request": {
                    "messages": [{"role": "user", "content": "Solve it"}],
                    "request_params": {"temperature": 0.2},
                },
                "response": {"role": "assistant", "content": "42"},
                "usage": {
                    "rate_card_hash": "rate-card-v1",
                    "gateway_request_id": "request-1",
                    "finish_reason": "stop",
                    "input_tokens": 2,
                    "cached_input_tokens": 0,
                    "cache_write_tokens": 0,
                    "output_tokens": 1,
                    "thinking_tokens": 0,
                    "provider_extras": {"accepted_prediction_tokens": 0},
                    "cost_usd": 0.01,
                    "duration_sec": 1.0,
                    "streamed": False,
                    "time_to_first_token_sec": None,
                    "attempt": 1,
                },
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode()

    events = build_canonical_events(
        trial_id=uuid4(),
        task_id="task-1",
        task_config=_task(),
        trial_config=_trial(),
        runtime_result=runtime_result,
        trace_body=trace,
        verifier_body=payloads["verifier/output.json"],
    )

    assert [event.seq for event in events] == list(range(len(events)))
    assert [event.kind.value for event in events] == [
        "trial_start",
        "step_start",
        "llm_call",
        "step_end",
        "verifier_start",
        "verifier_end",
        "trial_end",
    ]
    llm_call = events[2]
    assert llm_call.kind.value == "llm_call"
    assert llm_call.model_dump()["messages"] == [
        {
            "role": "user",
            "content": "Solve it",
            "name": None,
            "tool_calls": None,
            "tool_call_id": None,
        }
    ]
    assert llm_call.model_dump()["response"]["content"] == "42"
    assert llm_call.model_dump()["input_tokens"] == 2
    assert events[-1].model_dump()["reward"] == {"passed": 1.0}


async def test_materializer_rejects_source_digest_mismatch() -> None:
    store = FakeObjectStore(objects={("source", "bundle/file"): b"tampered"})
    materializer = ServiceExecutionMaterializer(
        session_factory=None,  # type: ignore[arg-type]
        source_store=store,
        source_bucket="source",
        canonical_store=store,
        artifacts_bucket="canonical",
        trajectories_bucket="trajectories",
    )

    with pytest.raises(MaterializationIntegrityError, match="source_object_digest_mismatch"):
        await materializer._read_exact(
            key="bundle/file",
            expected="sha256:" + hashlib.sha256(b"expected").hexdigest(),
            size=len(b"expected"),
        )


@pytest.mark.parametrize("readback", [b"payload", b"PAYLOAD", b"short", b"payload-extra"])
async def test_materializer_hashes_destination_when_store_has_no_full_checksum(readback) -> None:
    class NoChecksumStore(FakeObjectStore):
        async def stat_object(self, *, bucket, key):
            # A multipart HEAD may give length but no full-object checksum.
            return ObjectReadback(content_length=7, checksum_sha256=None)

        async def stream_object(self, **kwargs):
            for offset in range(0, len(readback), 3):
                yield readback[offset:offset + 3]

    source = FakeObjectStore(objects={("source", "input"): b"payload"})
    materializer = ServiceExecutionMaterializer(
        session_factory=None,  # type: ignore[arg-type]
        source_store=source, source_bucket="source", canonical_store=NoChecksumStore(),
        artifacts_bucket="canonical", trajectories_bucket="trajectories",
    )
    operation = materializer._copy_exact(
        source_key="input", destination_key="output",
        expected="sha256:" + hashlib.sha256(b"payload").hexdigest(), size=7,
    )
    if readback == b"payload":
        await operation
    else:
        with pytest.raises(MaterializationIntegrityError, match="canonical_object_readback_mismatch"):
            await operation


async def test_materializer_loop_recovers_after_control_database_outage() -> None:
    recovered = asyncio.Event()

    class RecoveringMaterializer:
        run_calls = 0
        cleanup_calls = 0
        metric_calls = 0

        async def run_once(self) -> bool:
            self.run_calls += 1
            if self.run_calls == 1:
                raise OSError("database temporarily unavailable")
            return False

        async def cleanup_source_once(self) -> bool:
            self.cleanup_calls += 1
            return False

        async def reconcile_accounting_once(self) -> bool:
            return False

        async def refresh_metrics(self) -> None:
            self.metric_calls += 1
            recovered.set()
            await asyncio.Event().wait()

    materializer = RecoveringMaterializer()
    loop_task = asyncio.create_task(
        run_service_execution_materializer_loop(
            materializer=materializer,  # type: ignore[arg-type]
            interval_seconds=0.01,
        )
    )
    await recovered.wait()
    loop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop_task
    assert materializer.run_calls == 2
    assert materializer.cleanup_calls == 1
    assert materializer.metric_calls == 1


async def test_materializer_loop_does_not_retry_an_error_after_shutdown() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    stop = asyncio.Event()

    class ShutdownMaterializer:
        run_calls = 0

        async def run_once(self) -> bool:
            self.run_calls += 1
            started.set()
            await release.wait()
            raise OSError("database cancellation translated to a transport error")

        async def cleanup_source_once(self) -> bool:
            raise AssertionError("shutdown must stop before source cleanup")

        async def reconcile_accounting_once(self) -> bool:
            return False

        async def refresh_metrics(self) -> None:
            raise AssertionError("shutdown must stop before metrics refresh")

    materializer = ShutdownMaterializer()
    loop_task = asyncio.create_task(
        run_service_execution_materializer_loop(
            materializer=materializer,  # type: ignore[arg-type]
            interval_seconds=60.0,
            stop_event=stop,
        )
    )
    await started.wait()
    stop.set()
    release.set()

    await asyncio.wait_for(loop_task, timeout=0.5)
    assert materializer.run_calls == 1


async def test_materializer_loop_preserves_translated_worker_cancellation() -> None:
    started = asyncio.Event()
    retry_wait_started = asyncio.Event()

    class ObservableStopEvent(asyncio.Event):
        async def wait(self) -> bool:
            retry_wait_started.set()
            return await super().wait()

    stop = ObservableStopEvent()

    class CancellationTranslatingMaterializer:
        async def run_once(self) -> bool:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError as exc:
                raise OSError("database driver translated cancellation") from exc

        async def cleanup_source_once(self) -> bool:
            raise AssertionError("cancelled worker must not start source cleanup")

        async def reconcile_accounting_once(self) -> bool:
            return False

        async def refresh_metrics(self) -> None:
            raise AssertionError("cancelled worker must not refresh metrics")

    loop_task = asyncio.create_task(
        run_service_execution_materializer_loop(
            materializer=CancellationTranslatingMaterializer(),  # type: ignore[arg-type]
            interval_seconds=60.0,
            stop_event=stop,
        )
    )
    await started.wait()
    loop_task.cancel()
    retry_observer = asyncio.create_task(retry_wait_started.wait())
    done, _ = await asyncio.wait(
        {loop_task, retry_observer},
        return_when=asyncio.FIRST_COMPLETED,
    )
    cancellation_was_preserved = loop_task in done

    stop.set()
    with pytest.raises(asyncio.CancelledError):
        await loop_task
    if retry_observer.done():
        await retry_observer
    else:
        retry_observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await retry_observer

    assert cancellation_was_preserved


async def test_materializer_loop_does_not_finish_before_workers_are_drained() -> None:
    all_workers_started = asyncio.Event()
    release_cancellation = asyncio.Event()
    delayed_worker_stopped = asyncio.Event()
    prompt_worker_stopped = asyncio.Event()
    stop = asyncio.Event()

    class DelayedCancellationMaterializer:
        run_calls = 0

        async def run_once(self) -> bool:
            self.run_calls += 1
            worker_index = self.run_calls
            if self.run_calls == 2:
                all_workers_started.set()
            if worker_index != 1:
                try:
                    await asyncio.Event().wait()
                finally:
                    prompt_worker_stopped.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release_cancellation.wait()
                raise
            finally:
                delayed_worker_stopped.set()

        async def cleanup_source_once(self) -> bool:
            raise AssertionError("cancelled worker must not start source cleanup")

        async def reconcile_accounting_once(self) -> bool:
            return False

        async def refresh_metrics(self) -> None:
            raise AssertionError("cancelled worker must not refresh metrics")

    loop_task = asyncio.create_task(
        run_service_execution_materializer_loop(
            materializer=DelayedCancellationMaterializer(),  # type: ignore[arg-type]
            interval_seconds=60.0,
            concurrency=2,
            stop_event=stop,
        )
    )
    await all_workers_started.wait()
    stop.set()
    loop_task.cancel()
    await prompt_worker_stopped.wait()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    parent_waited_for_worker = not loop_task.done()

    release_cancellation.set()
    with pytest.raises(asyncio.CancelledError):
        await loop_task
    await asyncio.wait_for(delayed_worker_stopped.wait(), timeout=0.5)

    assert parent_waited_for_worker


async def test_materializer_loop_wakes_idle_workers_when_stopped() -> None:
    idle_cycle_finished = asyncio.Event()
    stop = asyncio.Event()

    class IdleMaterializer:
        run_calls = 0

        async def run_once(self) -> bool:
            self.run_calls += 1
            return False

        async def cleanup_source_once(self) -> bool:
            return False

        async def reconcile_accounting_once(self) -> bool:
            return False

        async def refresh_metrics(self) -> None:
            idle_cycle_finished.set()

    materializer = IdleMaterializer()
    loop_task = asyncio.create_task(
        run_service_execution_materializer_loop(
            materializer=materializer,  # type: ignore[arg-type]
            interval_seconds=60.0,
            stop_event=stop,
        )
    )
    await idle_cycle_finished.wait()
    stop.set()

    await asyncio.wait_for(loop_task, timeout=0.5)
    assert materializer.run_calls == 1


async def test_busy_materializer_still_reconciles_late_accounting() -> None:
    stop = asyncio.Event()
    operations: list[str] = []

    class BusyMaterializer:
        async def run_once(self) -> bool:
            operations.append("materialize")
            return True

        async def cleanup_source_once(self) -> bool:
            operations.append("cleanup")
            return False

        async def refresh_metrics(self) -> None:
            operations.append("metrics")

        async def reconcile_accounting_once(self) -> bool:
            operations.append("accounting")
            stop.set()
            return True

    await asyncio.wait_for(run_service_execution_materializer_loop(
        materializer=BusyMaterializer(),  # type: ignore[arg-type]
        interval_seconds=60,
        stop_event=stop,
    ), timeout=0.5)
    assert operations == ["materialize", "cleanup", "metrics", "accounting"]


async def test_materializer_run_does_not_retry_a_translated_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeObjectStore(objects={})
    materializer = ServiceExecutionMaterializer(
        session_factory=None,  # type: ignore[arg-type]
        source_store=store,
        source_bucket="source",
        canonical_store=store,
        artifacts_bucket="canonical",
        trajectories_bucket="trajectories",
    )
    operation_started = asyncio.Event()
    retry_called = False

    async def claim_one() -> object:
        return object()

    async def load_and_materialize(_claim: object) -> None:
        operation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            raise OSError("database driver translated cancellation") from exc

    async def retry(_claim: object, _exc: Exception) -> None:
        nonlocal retry_called
        retry_called = True

    monkeypatch.setattr(materializer, "claim_one", claim_one)
    monkeypatch.setattr(materializer, "_load_and_materialize", load_and_materialize)
    monkeypatch.setattr(materializer, "_retry", retry)

    operation = asyncio.create_task(materializer.run_once())
    await operation_started.wait()
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert not retry_called


async def test_source_cleanup_does_not_retry_a_translated_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeObjectStore(objects={})
    materializer = ServiceExecutionMaterializer(
        session_factory=None,  # type: ignore[arg-type]
        source_store=store,
        source_bucket="source",
        canonical_store=store,
        artifacts_bucket="canonical",
        trajectories_bucket="trajectories",
    )
    operation_started = asyncio.Event()
    retry_called = False

    async def claim_source_cleanup() -> object:
        return object()

    async def source_cleanup_keys(_claim: object) -> tuple[str, ...]:
        operation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            raise OSError("object-store driver translated cancellation") from exc

    async def retry_source_cleanup(_claim: object, _exc: Exception) -> None:
        nonlocal retry_called
        retry_called = True

    monkeypatch.setattr(materializer, "claim_source_cleanup", claim_source_cleanup)
    monkeypatch.setattr(materializer, "_source_cleanup_keys", source_cleanup_keys)
    monkeypatch.setattr(materializer, "_retry_source_cleanup", retry_source_cleanup)

    operation = asyncio.create_task(materializer.cleanup_source_once())
    await operation_started.wait()
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert not retry_called


async def test_accounting_refresh_defers_bad_archive_and_preserves_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane import service_execution_accounting_repair as repair

    lease_id, team_id = uuid4(), uuid4()

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def execute(self, _statement):
            return self

        def one_or_none(self):
            return lease_id, team_id

    store = FakeObjectStore(objects={})
    materializer = ServiceExecutionMaterializer(
        session_factory=Session,  # type: ignore[arg-type]
        source_store=store, source_bucket="source", canonical_store=store,
        artifacts_bucket="canonical", trajectories_bucket="trajectories",
        retry_max_seconds=60,
    )
    now = datetime.now(UTC)

    async def unavailable(**_):
        raise OSError("unavailable object store")

    monkeypatch.setattr(repair, "repair_accounting", unavailable)
    assert not await materializer.reconcile_accounting_once(now=now)
    assert materializer._accounting_retry_after == {lease_id: now + timedelta(seconds=60)}
    assert store.objects == {}

    async def cancelled(**_):
        raise asyncio.CancelledError

    monkeypatch.setattr(repair, "repair_accounting", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await materializer.reconcile_accounting_once(now=now + timedelta(seconds=61))
    assert materializer._accounting_retry_after == {}


@pytest.mark.parametrize("agent", ["direct-completion", "terminus-2"])
def test_compiler_opts_only_isolated_terminus_into_timeout_verification(agent: str) -> None:
    task = _task(
        agent={"name": agent, "timeout_sec": 900},
        verifier={
            "name": "script",
            "args": {"script_path": "verifier/check.sh"},
            "timeout_sec": 1200,
        },
    )
    controller = "registry.example/controller@sha256:" + "9" * 64
    profile = _profile().model_copy(
        update={
            "agent_image_ref": controller,
            "image_admission": signed_image_admission_bundle(
                (_TASK_IMAGE, _RUNTIME_IMAGE, controller)
            ),
        }
    )
    trial = _trial().model_copy(update={"agent_name": agent})
    if agent == "terminus-2":
        task = task.model_copy(update={"environment": task.environment.model_copy(
            update={"workdir": PurePosixPath("/media/project")},
        )})
    plan = compile_service_execution_plan(
        task=task,
        trial=trial,
        profile=profile,
        source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    assert plan.main.timeout_seconds == 900
    if agent == "terminus-2":
        assert plan.verifier is None
        assert plan.verifier_execution == "separate_execution"
        assert plan.main.working_directory == "/app"
        assert plan.main.argv[:3] == ("python", "-I", "-m")
        assert "verifier_after_agent_timeout" not in plan.canonical_payload()
        assert plan.agent_image_ref == controller
        assert {sidecar.role_name for sidecar in plan.sidecars if sidecar.private_sandbox} == {
            "task-sandbox",
        }
    else:
        assert plan.verifier is not None and plan.verifier.timeout_seconds == 1200
        assert not plan.verifier_after_agent_timeout
        assert "verifier_after_agent_timeout" not in plan.canonical_payload()
