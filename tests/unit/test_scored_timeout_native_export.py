"""A scored timeout can export its committed Harbor file without a late event."""
import hashlib
import json
from copy import deepcopy

import pytest

from loom_service.delivery_export_tb2_v2 import Tb2V2ExportError, build_per_trial_v2_bundle
from tests.unit.terminus2.test_export_v2 import _messages_from_raw_log, _typed_turn_chain
from tests.unit.test_native_delivery_acceptance import native_trial


def fixture(*, identity=None, team=None):
    trial, client = native_trial(timed_out=True, identity=identity, team=team)
    events = _typed_turn_chain(trial_id=trial.id)[:-1]
    native = b'{"steps": []}'
    path = "artifacts/harbor/trajectory.json"
    digest = "sha256:" + hashlib.sha256(native).hexdigest()
    rows = trial.trajectory_index["artifacts"]
    result_row = next(x for x in rows if x["relative_path"] == "result.json")
    prefix = result_row["key"].removesuffix("result.json")
    runtime = trial.result["runtime_result"]
    runtime["outputs"].append({"source_path": ".loom/agent/harbor/trajectory.json", "relative_path": path,
        "kind": "agent_native", "required": True, "state": "captured", "size_bytes": len(native), "sha256": digest})
    body = json.dumps(runtime).encode()
    result_row.update(size_bytes=len(body), sha256="sha256:"+hashlib.sha256(body).hexdigest())
    client._objects[("artifacts", result_row["key"])] = body
    rows.append({"relative_path": path, "key": prefix+path, "bucket": "artifacts",
                 "size_bytes": len(native), "sha256": digest})
    client._objects[("artifacts", prefix+path)] = native
    return trial, events, client, native


def build(trial, events, client):
    return build_per_trial_v2_bundle(trial=trial, events=events, calls=[], client=client,
        artifacts_bucket="artifacts", messages_from_raw_log=_messages_from_raw_log)


def test_scored_timeout_exports_committed_native_file_without_changing_audit_or_outcome():
    trial, events, client, native = fixture()
    before = deepcopy(trial.result), [e.model_dump() for e in events]
    bundle = build(trial, events, client)
    assert bundle.native_artifacts["native/harbor_trajectory.json"] == native
    assert bundle.export_provenance["native_artifact_authority"] == "committed_runtime_output"
    assert (trial.result, [e.model_dump() for e in events]) == before
    assert trial.state == "failed" and trial.result["runtime_result"]["status"] == "timed_out"


@pytest.mark.parametrize("damage", ["missing", "corrupt", "other_trial", "other_attempt", "other_bundle", "blocked", "runtime_digest", "bad_join", "not_timeout"])
def test_timeout_export_rejects_invalid_native_evidence(damage):
    trial, events, client, _ = fixture()
    row = trial.trajectory_index["artifacts"][-1]
    if damage == "missing":
        del client._objects[("artifacts", row["key"])]
    elif damage == "corrupt":
        client._objects[("artifacts", row["key"])] = b'{"steps": 11}'
    elif damage == "other_trial":
        row["key"] = row["key"].replace(str(trial.id), "00000000-0000-0000-0000-000000000000")
    elif damage == "other_attempt":
        trial.trajectory_index["attempt"] = 2
    elif damage == "other_bundle":
        row["key"] = row["key"].replace("/bundles/", "/bundles/00000000-0000-0000-0000-000000000000/")
    elif damage == "blocked":
        row["share_status"] = "blocked"
    elif damage == "runtime_digest":
        row["sha256"] = "sha256:" + "0" * 64
    elif damage == "bad_join":
        events[2] = events[2].model_copy(update={"gateway_request_id": "absent"})
    elif damage == "not_timeout":
        trial.state = "succeeded"
    with pytest.raises(Tb2V2ExportError):
        build(trial, events, client)


def test_existing_recording_reference_is_still_validated_for_timeout():
    from loom.models.trajectory import Terminus2ArtifactRefEvent

    trial, events, client, _ = fixture()
    events.append(Terminus2ArtifactRefEvent(
        emitted_at=events[-1].emitted_at, trial_id=trial.id, step_id="main", seq=20,
        artifact_kind="recording.cast", sandbox_path="/app/recording.cast",
        content_hash="0" * 64, size_bytes=1, share_policy="restricted",
    ))
    with pytest.raises(Tb2V2ExportError, match="missing_native_artifact"):
        build(trial, events, client)
