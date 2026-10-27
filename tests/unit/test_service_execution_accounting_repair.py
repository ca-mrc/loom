from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.db.schema import ServiceExecutionLease, Task, Trial
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.trajectory import LLMCallEvent, Terminus2UserPromptEvent, TrialStartEvent
from loom.models.trial import TrialConfig
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane import service_execution_accounting_repair as repair
from tests.unit.test_service_execution_materialization import _task, _trial

_REAL_BUILD_EVENTS = repair.build_canonical_events
_REAL_BUILD_ATIF = repair.build_canonical_atif
_REAL_USAGE = repair.terminus_usage
_REAL_RUNTIME_VALIDATE = ExecutionRuntimeResultV1.model_validate_json


@pytest.fixture
def case(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    team_id, trial_id, lease_id, artifact_id, authority = (uuid4() for _ in range(5))
    now = datetime.now(UTC)
    task_config = _task(agent={"name": "terminus-2", "version": "2.0.0"})
    trial_config = _trial().model_copy(update={"agent_name": "terminus-2"})
    trial = SimpleNamespace(id=trial_id, team_id=team_id, batch_id=None, task_id="task-1", state="succeeded",
                            attempt_count=1, config=trial_config.model_dump(mode="json"),
                            trajectory_index={"trajectory_uri": "s3://trajectories/old/events.jsonl",
                                              "atif_uri": "s3://trajectories/old/atif.json"},
                            finished_at=now, result={"reward": 1.0})
    lease = SimpleNamespace(id=lease_id, trial_id=trial_id, team_id=team_id, attempt=1,
                            runtime_contract_json=None,
                            generation=1, output_generation=1, materialization_state="committed", output_commit_state="committed",
                            materialization_committed_at=now, source_retain_until=now,
                            canonical_trajectory_sha256="old", canonical_atif_sha256="old")
    store = FakeObjectStore()
    records = []
    for path in (*repair._PATHS, "result.json", "verifier/output.json"):
        body = json.dumps({"source": path}).encode()
        store.objects[("artifacts", "original/" + path)] = body
        records.append({"relative_path": path, "bucket": "artifacts", "key": "original/" + path,
                        "size_bytes": len(body), "sha256": "sha256:" + repair._digest(body),
                        "media_type": "application/json"})
    artifact = SimpleNamespace(id=artifact_id, trial_id=trial_id, team_id=team_id,
                               control_producer_kind="service_execution", control_producer_id=lease_id,
                               lifecycle_authority_id=authority, created_at=now, artifact_metadata={},
                               storage={"schema_version": "loom.canonical-trial-bundle-storage.v1",
                                        "attempt": 1, "files": records, "source_evidence": []})
    original_event = SimpleNamespace(seq=0, kind="trial_start", payload={"old": True},
                                     source="service-execution-materializer", lifecycle_authority_id=authority)
    data = SimpleNamespace(lease=lease, trial=trial, artifact=artifact, team_id=team_id, store=store,
                           events=[original_event], commits=0, enters=0, change_on_commit=None)
    event = TrialStartEvent(trial_id=trial_id, step_id="trial", seq=0, emitted_at=now,
                           task_id="task-1", agent_name="terminus-2", agent_mode="out-of-box")
    projector = AsyncMock()  # Projection itself has separate full ledger regression tests.
    monkeypatch.setattr(repair.ExecutionRuntimeResultV1, "model_validate_json", lambda body: SimpleNamespace(status="succeeded"))
    monkeypatch.setattr(repair, "build_canonical_events", lambda **kwargs: (event,))
    monkeypatch.setattr(repair, "build_canonical_atif", lambda *args, **kwargs: b'{"schema_version":"harbor-tb2-v2-projection"}')
    monkeypatch.setattr(repair, "terminus_usage", lambda *args: {"call_count": 6, "totals": {"input_tokens": 13493, "output_tokens": 11888}})
    monkeypatch.setattr(repair, "read_service_execution_llm_calls", AsyncMock(return_value=[{"id": "length-call"}]))
    monkeypatch.setattr(repair, "register_lifecycle_object", projector)

    class Session:
        async def __aenter__(self):
            data.enters += 1
            if data.enters == 2 and data.change_on_commit:
                data.change_on_commit()
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, model, identity, **kwargs):
            return {ServiceExecutionLease: lease, Trial: trial,
                    Task: SimpleNamespace(config=task_config.model_dump(mode="json"), source=None,
                                          source_provenance={})}[model]

        async def scalar(self, query):
            return artifact

        async def scalars(self, query):
            return SimpleNamespace(all=lambda: list(data.events))

        async def execute(self, query):
            data.events.clear()

        def add(self, row):
            data.events.append(row)

        async def flush(self):
            pass

        async def commit(self):
            data.commits += 1

    # The commit lookup also requests Artifact, while scalar selects it initially.
    from loom.db.schema import Artifact
    original_get = Session.get

    async def get(self, model, identity, **kwargs):
        return artifact if model is Artifact else await original_get(self, model, identity, **kwargs)

    Session.get = get
    data.kwargs = {"session_factory": Session, "store": store, "artifacts_bucket": "artifacts",
                   "trajectories_bucket": "trajectories", "lease_id": lease_id, "team_id": team_id}
    data.register = projector
    return data


async def test_repair_publishes_separate_revision_and_is_idempotent(case: SimpleNamespace) -> None:
    original_objects = copy.deepcopy(case.store.objects)
    preserved = (case.trial.finished_at, case.trial.result, case.lease.materialization_committed_at,
                 case.lease.source_retain_until, case.lease.output_commit_state)
    assert (await repair.repair_accounting(**case.kwargs))["status"] == "prepared"
    assert case.store.objects == original_objects and case.commits == 0
    result = await repair.repair_accounting(**case.kwargs, apply=True)
    assert result["status"] == "corrected" and result["usage"]["call_count"] == 6
    assert case.commits == 1 and case.register.await_count == 5
    assert all(case.store.objects[key] == body for key, body in original_objects.items())
    assert len(case.store.objects) == len(original_objects) + 5
    assert "/accounting-v2/" in case.trial.trajectory_index["atif_uri"]
    assert {item["relative_path"] for item in case.artifact.storage["source_evidence"]} == {
        "source/trajectory/events.jsonl", "source/accounting/usage.json",
    }
    assert preserved == (case.trial.finished_at, case.trial.result, case.lease.materialization_committed_at,
                         case.lease.source_retain_until, case.lease.output_commit_state)
    assert case.trial.state == "succeeded" and case.trial.attempt_count == 1
    assert case.events[0].payload != {"old": True}
    count = len(case.store.objects)
    assert (await repair.repair_accounting(**case.kwargs, apply=True))["status"] == "already_corrected"
    assert len(case.store.objects) == count and case.commits == 1


@pytest.mark.parametrize("selected,expected", [(None, "2.0.0"), ("harbor-frozen", "harbor-frozen")])
async def test_repair_preserves_trial_agent_version(case, monkeypatch, selected, expected):
    case.trial.config["agent_version"] = selected
    seen = []

    def project(*args, **kwargs):
        seen.append(kwargs["agent_version"])
        return b'{}'

    monkeypatch.setattr(repair, "build_canonical_atif", project)
    await repair.repair_accounting(**case.kwargs)
    assert seen == [expected]


async def test_repair_fences_concurrent_pointer_change(case: SimpleNamespace) -> None:
    old_storage = copy.deepcopy(case.artifact.storage)
    case.change_on_commit = lambda: case.trial.trajectory_index.update({"concurrent": True})
    with pytest.raises(ValueError, match="repair input changed"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.commits == 0 and case.artifact.storage == old_storage
    assert case.events[0].payload == {"old": True}


@pytest.mark.parametrize("violation", ["team", "foreign_events", "running", "retry"])
async def test_repair_rejects_unowned_or_ineligible_trial(case: SimpleNamespace, violation: str) -> None:
    if violation == "team":
        case.kwargs["team_id"] = uuid4()
    elif violation == "foreign_events":
        case.events[0].source = "other-producer"
    elif violation == "running":
        case.trial.state = "running"
    else:
        case.trial.attempt_count = 2
    original_objects = copy.deepcopy(case.store.objects)
    with pytest.raises(ValueError):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.store.objects == original_objects and case.commits == 0


async def test_repair_source_corruption_never_updates_pointers(case: SimpleNamespace) -> None:
    case.store.objects[("artifacts", "original/result.json")] = b"corrupt"
    old_index = copy.deepcopy(case.trial.trajectory_index)
    with pytest.raises(ValueError, match="canonical file"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.trial.trajectory_index == old_index and case.commits == 0


async def test_corrected_download_assembles_with_original_source(case: SimpleNamespace) -> None:
    import io
    import tarfile

    from loom_service.delivery_export import build_canonical_trial_bundle_archive
    from loom_service.trial_bundles import canonical_bundle_from_artifact

    case.artifact.manifest_sha256 = "sha256:" + "a" * 64
    case.artifact.content_hash = "sha256:" + "b" * 64
    await repair.repair_accounting(**case.kwargs, apply=True)
    bundle = canonical_bundle_from_artifact(case.artifact, trial=case.trial)
    assert bundle is not None

    def get_object(**kwargs):
        body = case.store.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {"Body": io.BytesIO(body), "ContentLength": len(body)}

    archive = build_canonical_trial_bundle_archive(client=SimpleNamespace(get_object=get_object), bundle=bundle)
    try:
        with tarfile.open(fileobj=archive.body, mode="r:gz") as tar:
            usage = json.load(tar.extractfile("files/accounting/usage.json"))
            assert usage["call_count"] == 6
            assert usage["totals"] == {"input_tokens": 13493, "output_tokens": 11888}
            assert json.load(tar.extractfile("source/accounting/usage.json")) == {"source": "accounting/usage.json"}
            assert json.load(tar.extractfile("files/accounting/gateway-calls.json"))["calls"] == [{"id": "length-call"}]
            assert json.load(tar.extractfile("files/trajectory/events.jsonl"))["trial_id"] == str(case.trial.id)
    finally:
        archive.body.close()


async def test_partial_object_write_failure_cleans_only_new_revision(case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    original_objects = copy.deepcopy(case.store.objects)
    write = case.store.put_object_with_metadata
    calls = 0

    async def fail_third(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise RuntimeError("injected object outage")
        return await write(**kwargs)

    monkeypatch.setattr(case.store, "put_object_with_metadata", fail_third)
    with pytest.raises(RuntimeError, match="injected object outage"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.commits == 0 and case.store.objects == original_objects


async def test_accounting_marker_does_not_hide_late_calls(case, monkeypatch):
    await repair.repair_accounting(**case.kwargs, apply=True)
    case.enters = 0
    monkeypatch.setattr(repair, "read_service_execution_llm_calls", AsyncMock(return_value=[
        {"id": "length-call"}, {"id": "late-call"},
    ]))
    monkeypatch.setattr(repair, "terminus_usage", lambda *args: {"call_count": 7})
    result = await repair.repair_accounting(**case.kwargs, apply=True)
    assert result["status"] == "corrected"
    assert result["gateway_call_count"] == 2
    assert case.artifact.artifact_metadata["accounting_call_count"] == 2
    assert len(case.artifact.storage["source_evidence"]) == 2


async def test_failed_trial_without_verifier_can_reconcile(case, monkeypatch):
    case.trial.state = "failed"
    case.artifact.storage["files"] = [r for r in case.artifact.storage["files"]
                                       if r["relative_path"] != "verifier/output.json"]
    monkeypatch.setattr(repair.ExecutionRuntimeResultV1, "model_validate_json",
                        lambda body: SimpleNamespace(status="timed_out"))
    observed = []
    projector = repair.build_canonical_events

    def project(**kwargs):
        observed.append(kwargs["verifier_body"])
        return projector(**kwargs)

    monkeypatch.setattr(repair, "build_canonical_events", project)
    assert (await repair.repair_accounting(**case.kwargs, apply=True))["status"] == "corrected"
    assert observed == [None]
    assert case.trial.state == "failed"


async def test_repeated_repair_projects_immutable_native_source(case, monkeypatch):
    await repair.repair_accounting(**case.kwargs, apply=True)
    case.enters = 0
    observed = []
    projector = repair.build_canonical_events

    def project(**kwargs):
        observed.append(kwargs["trace_body"])
        return projector(**kwargs)

    monkeypatch.setattr(repair, "build_canonical_events", project)
    await repair.repair_accounting(**case.kwargs, apply=True)
    assert observed == [case.store.objects[("artifacts", "original/trajectory/events.jsonl")]]


async def test_timed_out_archive_converges_four_failed_calls_then_late_9191_tokens(case, monkeypatch):
    from tests.unit.test_service_execution_materialization import (
        _REVISION,
        _RUNTIME_IMAGE,
        _TASK_IMAGE,
    )

    monkeypatch.setattr(repair, "build_canonical_events", _REAL_BUILD_EVENTS)
    monkeypatch.setattr(repair, "build_canonical_atif", _REAL_BUILD_ATIF)
    monkeypatch.setattr(repair, "terminus_usage", _REAL_USAGE)
    monkeypatch.setattr(repair.ExecutionRuntimeResultV1, "model_validate_json", _REAL_RUNTIME_VALIDATE)
    case.trial.state = "failed"
    case.trial.result = {"error_type": "timed_out"}
    trial_config = TrialConfig(agent_name="terminus-2", agent_model={"provider": "openai", "name": "glm-5.2"})
    case.trial.config = trial_config.model_dump(mode="json")
    now = case.trial.finished_at
    native = Terminus2UserPromptEvent(trial_id=case.trial.id, step_id="agent", seq=0,
                                     emitted_at=now, prompt_id="p", harbor_step_id=1, message="Solve")
    runtime = {
        "schema_version": "loom.execution-runtime-result.v1",
        "runtime_contract_sha256": "sha256:" + "1" * 64,
        "candidate_sha": "1" * 40, "task_revision_sha256": _REVISION,
        "command_identity_sha256": "sha256:" + "2" * 64,
        "execution_role": "attempt", "container_roles": ["execution", "agent", "verifier"],
        "task_image_ref": _TASK_IMAGE, "runtime_image_ref": _RUNTIME_IMAGE,
        "runtime_binary_sha256": "sha256:" + "3" * 64,
        "execution_class_id": "linux-amd64-cpu-pod-v1", "status": "timed_out",
        "started_at": now.isoformat(), "finished_at": now.isoformat(),
        "phases": [], "outputs": [], "verifier_rewards": None, "partial_evidence": True,
    }
    bodies = {"result.json": repair._body(runtime), "trajectory/events.jsonl": native.model_dump_json().encode() + b"\n",
              "accounting/usage.json": repair._body({"call_count": 0})}
    case.artifact.storage["files"] = []
    for path, body in bodies.items():
        key = "original/" + path
        case.store.objects[("artifacts", key)] = body
        case.artifact.storage["files"].append({"relative_path": path, "bucket": "artifacts", "key": key,
            "size_bytes": len(body), "sha256": "sha256:" + repair._digest(body), "media_type": "application/json"})
    rows = [{"id": str(uuid4()), "trial_id": str(case.trial.id), "step_id": "agent",
             "dialect": "openai_facade", "model": "glm-5.2", "input_tokens": 0, "output_tokens": 0,
             "cost_usd": 0, "rate_card_hash": "test",
             "provider_extras": {"_loom_call_status": "failed", "_loom_usage_status": "missing",
                                 "_loom_failure_category": "upstream_timeout"},
             "call_status": "failed", "captured_at": now.isoformat(), "attempt": 1} for _ in range(4)]
    monkeypatch.setattr(repair, "read_service_execution_llm_calls", AsyncMock(side_effect=lambda *a, **k: copy.deepcopy(rows)))
    first = await repair.repair_accounting(**case.kwargs, apply=True)
    assert first["usage"]["call_count"] == 4
    assert first["usage"]["totals"]["input_tokens"] == 0
    assert first["usage"]["missing_usage_call_count"] == first["usage"]["failed_call_count"] == 4
    source_before = copy.deepcopy(case.artifact.storage["source_evidence"])
    rows.append({**rows[0], "id": str(uuid4()), "input_tokens": 999, "output_tokens": 8192,
                 "provider_extras": {}, "call_status": "completed", "finish_reason": "length"})
    second = await repair.repair_accounting(**case.kwargs, apply=True)
    assert second["status"] == "corrected" and second["gateway_call_count"] == 5
    assert second["usage"]["call_count"] == 5
    assert second["usage"]["missing_usage_call_count"] == 4
    assert second["usage"]["failed_call_count"] == 4
    assert second["usage"]["partial_usage_call_count"] == 0
    assert second["usage"]["totals"]["input_tokens"] + second["usage"]["totals"]["output_tokens"] == 9191
    assert case.artifact.storage["source_evidence"] == source_before
    atif_bucket, atif_key = case.trial.trajectory_index["atif_uri"][5:].split("/", 1)
    atif = json.loads(case.store.objects[(atif_bucket, atif_key)])
    assert atif["accounting"] == second["usage"]
    assert len(atif["steps"]) == 1  # Only the original prompt, no invented agent turn.
    assert case.events[-1].payload["final_state"] == "failed"
    calls = [event for event in case.events if event.kind == LLMCallEvent.model_fields["kind"].default]
    assert len(calls) == 5
    failed = [event.payload for event in calls if event.payload["call_status"] == "failed"]
    assert len(failed) == 4
    assert all(event["usage_status"] == "missing" and event["failure_category"] == "upstream_timeout"
               for event in failed)
    known = [event.payload for event in calls if event.payload["call_status"] == "completed"]
    assert len(known) == 1 and known[0]["usage_status"] is None
    ledger_record = next(item for item in case.artifact.storage["files"]
                         if item["relative_path"] == "accounting/gateway-calls.json")
    exported_calls = json.loads(case.store.objects[(ledger_record["bucket"], ledger_record["key"])])["calls"]
    assert sum(call["provider_extras"].get("_loom_usage_status") == "missing" for call in exported_calls) == 4
    assert sum(call["provider_extras"].get("_loom_failure_category") == "upstream_timeout" for call in exported_calls) == 4
    original_count, commits = len(case.store.objects), case.commits
    third = await repair.repair_accounting(**case.kwargs, apply=True)
    assert third["status"] == "already_corrected"
    assert len(case.store.objects) == original_count and case.commits == commits
    assert case.trial.result == {"error_type": "timed_out"}


@pytest.mark.parametrize("changed", ["ledger", "outcome", "generation"])
async def test_repair_fences_changed_inputs_and_removes_unpublished_objects(case, monkeypatch, changed):
    original_objects = copy.deepcopy(case.store.objects)
    original_storage = copy.deepcopy(case.artifact.storage)

    def race():
        if changed == "ledger":
            monkeypatch.setattr(repair, "read_service_execution_llm_calls", AsyncMock(return_value=[{"id": "new-call"}]))
        elif changed == "outcome":
            case.trial.result = {"reward": 0}
        else:
            case.lease.output_generation = 2

    case.change_on_commit = race
    with pytest.raises(ValueError, match="repair input changed"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.store.objects == original_objects
    assert case.artifact.storage == original_storage and case.commits == 0


async def test_current_content_only_backfills_missing_count_without_new_objects(case):
    await repair.repair_accounting(**case.kwargs, apply=True)
    case.artifact.artifact_metadata.pop("accounting_call_count")
    original_objects, original_storage = copy.deepcopy(case.store.objects), copy.deepcopy(case.artifact.storage)
    assert (await repair.repair_accounting(**case.kwargs))["status"] == "already_corrected"
    assert "accounting_call_count" not in case.artifact.artifact_metadata
    result = await repair.repair_accounting(**case.kwargs, apply=True)
    assert result["status"] == "already_corrected"
    assert case.artifact.artifact_metadata["accounting_call_count"] == 1
    assert case.store.objects == original_objects and case.artifact.storage == original_storage
    assert case.commits == 2


async def test_successful_trial_still_requires_verifier(case):
    case.artifact.storage["files"] = [r for r in case.artifact.storage["files"]
                                       if r["relative_path"] != "verifier/output.json"]
    with pytest.raises(ValueError, match="requires verifier"):
        await repair.repair_accounting(**case.kwargs, apply=True)
    assert case.commits == 0


async def test_matching_materializer_json_format_does_not_publish_redundant_revision(case):
    await repair.repair_accounting(**case.kwargs, apply=True)
    record = next(r for r in case.artifact.storage["files"] if r["relative_path"] == "trajectory/events.jsonl")
    # The initial materializer uses sorted-key JSON; repair's historical
    # serializer used model field order. Formatting is not a ledger change.
    body = b"".join(json.dumps(row.payload, sort_keys=True).encode() + b"\n" for row in case.events)
    case.store.objects[(record["bucket"], record["key"])] = body
    record.update(size_bytes=len(body), sha256="sha256:" + repair._digest(body))
    case.trial.trajectory_index["trajectory_sha256"] = repair._digest(body)
    objects = copy.deepcopy(case.store.objects)
    assert (await repair.repair_accounting(**case.kwargs, apply=True))["status"] == "already_corrected"
    assert case.store.objects == objects and case.commits == 1
