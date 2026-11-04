"""Ungraded terminus-2 trials export a committed trajectory and no score."""
import hashlib

import pytest

from loom_service.delivery_export_tb2_v2 import (
    Tb2V2ExportError,
    build_per_trial_v2_bundle,
    resolve_verifier_artifacts,
)
from tests.unit.terminus2.test_export_v2 import _messages_from_raw_log, _typed_turn_chain
from tests.unit.test_native_delivery_acceptance import native_trial


def fixture(kind: str):
    timed_out = kind == "timed_out"
    trial, client = native_trial(timed_out=timed_out)
    runtime = trial.result["runtime_result"]
    agent = next(phase for phase in runtime["phases"] if phase["role"] == "agent")
    runtime["phases"] = [agent]
    runtime["verifier_rewards"] = None
    runtime["failure_reason"] = None
    trial.result["reward"] = None
    if timed_out:
        runtime["status"] = "timed_out"
    else:
        trial.state = "failed"
        trial.failure_reason = "verifier_error"
        runtime["status"] = "succeeded"
        agent.update(exit_code=0, timed_out=False)
    native = b'{"steps": []}'
    path = "artifacts/harbor/trajectory.json"
    digest = "sha256:" + hashlib.sha256(native).hexdigest()
    runtime["outputs"] = [{
        "source_path": ".loom/agent/harbor/trajectory.json",
        "relative_path": path,
        "kind": "agent_native",
        "required": True,
        "state": "captured",
        "size_bytes": len(native),
        "sha256": digest,
    }]
    rows = trial.trajectory_index["artifacts"]
    prefix = next(row["key"] for row in rows if row["relative_path"] == "result.json").removesuffix("result.json")
    rows.append({
        "relative_path": path, "key": prefix + path, "bucket": "artifacts",
        "size_bytes": len(native), "sha256": digest,
    })
    client._objects[("artifacts", prefix + path)] = native
    chain = _typed_turn_chain(trial_id=trial.id)
    events = chain[:-2] if timed_out else chain[:-1]
    return trial, events, client, native


def build(trial, events, client):
    return build_per_trial_v2_bundle(
        trial=trial, events=events, calls=[], client=client,
        artifacts_bucket="artifacts", messages_from_raw_log=_messages_from_raw_log,
    )


def test_agent_budget_timeout_exports_trajectory_and_keeps_the_timeout():
    trial, events, client, native = fixture("timed_out")
    bundle = build(trial, events, client)
    assert bundle.native_artifacts["native/harbor_trajectory.json"] == native
    assert bundle.export_provenance["outcome"] == "agent_budget_timeout"
    assert bundle.export_provenance["native_artifact_authority"] == "committed_runtime_output"
    assert trial.state == "failed"
    assert trial.failure_reason == "timed_out"
    assert trial.result["reward"] is None
    assert resolve_verifier_artifacts(
        trial, client=client, artifacts_bucket="artifacts",
    ) == []


def test_verifier_error_exports_the_agent_trajectory_without_a_score():
    trial, events, client, native = fixture("verifier_error")
    bundle = build(trial, events, client)
    assert bundle.native_artifacts["native/harbor_trajectory.json"] == native
    assert bundle.export_provenance["outcome"] == "verification_indeterminate"
    assert trial.failure_reason == "verifier_error"
    assert trial.result["reward"] is None


def test_missing_committed_trajectory_rejects_the_export():
    trial, events, client, _ = fixture("timed_out")
    row = trial.trajectory_index["artifacts"][-1]
    del client._objects[("artifacts", row["key"])]
    with pytest.raises(Tb2V2ExportError):
        build(trial, events, client)


def test_an_earlier_turn_still_needs_its_observation():
    trial, events, client, _ = fixture("timed_out")
    earlier = _typed_turn_chain(trial_id=trial.id, gw_id="gw-0", turn_id="turn-0", seq_start=1)
    later = _typed_turn_chain(trial_id=trial.id, gw_id="gw-1", turn_id="turn-1", seq_start=10)
    events = earlier[:4] + later[1:4]
    with pytest.raises(Tb2V2ExportError) as caught:
        build(trial, events, client)
    assert any("turn-0" in item for item in caught.value.detail["errors"])
