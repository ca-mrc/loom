"""Regressions from the single-attempt September 25 acceptance round."""
import hashlib
import json
from copy import deepcopy
from uuid import uuid4

import pytest

from loom.db.schema import Trial
from loom.service_execution_terminus_trace import terminus_usage
from loom_control_plane.service_execution_materializer import (
    MaterializationIntegrityError,
    validate_usage_accounting,
)
from loom_service.delivery_export_tb2_v2 import Tb2V2ExportError, resolve_verifier_artifacts
from tests.unit.test_service_execution_terminus_plan import _events
from tests.unit.test_verifier_archival_recovery import legacy_result
from tests.unit.test_verifier_audit_artifacts import _FakeS3


def _usage():
    trial, _, events = _events()
    events[1] = events[1].model_copy(update={"cost_usd_snapshot": 0.248161})
    trace = b"\n".join(e.model_dump_json().encode() for e in events)
    return trial, trace, terminus_usage(events, trial)


def test_retained_usage_accepts_only_accumulation_roundoff():
    trial, trace, usage = _usage()
    usage["totals"]["cost_usd"] = 0.24816100000000002
    before = deepcopy(usage)
    validate_usage_accounting(trace_body=trace, usage_body=json.dumps(usage).encode(), trial_config=trial)
    assert usage == before


@pytest.mark.parametrize("field,value", [
    ("cost_usd", 0.248162), ("cost_usd", float("nan")),
    ("cost_usd", float("inf")), ("cost_usd", True),
    ("duration_sec", 1.01), ("input_tokens", 6),
])
def test_usage_rejects_real_or_invalid_differences(field, value):
    trial, trace, usage = _usage()
    usage["totals"][field] = value
    with pytest.raises(MaterializationIntegrityError, match="usage_output_identity_drift"):
        validate_usage_accounting(trace_body=trace, usage_body=json.dumps(usage).encode(), trial_config=trial)


def native_trial(*, timed_out=False, identity=None, team=None, reward=0):
    identity, team, bundle = identity or uuid4(), team or uuid4(), uuid4()
    prefix = f"trials/{team}/{identity}/attempts/1/bundles/{bundle}/files/"
    output = json.dumps({"rewards": {"passed": reward}}).encode()
    bodies = {"02-verifier.stdout": b"one failed test\n", "02-verifier.stderr": b"", "verifier/output.json": output}
    raw = legacy_result().model_dump(mode="json")
    phase = raw["phases"][0]
    phase.update(ordinal=2, exit_code=0)
    for name in ("stdout", "stderr"):
        body = bodies[f"02-verifier.{name}"]
        phase[name].update(sha256="sha256:" + hashlib.sha256(body).hexdigest(), bytes_seen=len(body), bytes_saved=len(body))
    agent = deepcopy(phase)
    agent.update(role="agent", ordinal=1, exit_code=124 if timed_out else 0, timed_out=timed_out)
    for name in ("stdout", "stderr"):
        agent[name]["path"] = f"01-agent.{name}"
    raw.update(status="timed_out" if timed_out else "succeeded", partial_evidence=timed_out,
               verifier_rewards={"passed": reward}, phases=[agent, phase], outputs=[raw["outputs"][1]])
    raw["outputs"][0].update(size_bytes=len(output), sha256="sha256:"+hashlib.sha256(output).hexdigest())
    bodies["result.json"] = json.dumps(raw).encode()
    index = [{"relative_path": path, "key": prefix+path, "bucket": "artifacts",
              "size_bytes": len(body), "sha256": "sha256:"+hashlib.sha256(body).hexdigest()}
             for path, body in bodies.items()]
    trial = Trial(id=identity, team_id=team, task_id="task", batch_id=uuid4(),
                  state="failed" if timed_out else "succeeded", attempt_count=1,
                  failure_reason="timed_out" if timed_out else None,
                  config={"agent_name": "terminus-2"},
                  result={"runtime_result": raw, "reward": {"passed": reward}},
                  trajectory_index={"attempt": 1, "artifacts": index})
    return trial, _FakeS3({("artifacts", prefix+path): body for path, body in bodies.items()})


def separate_native_trial(*, identity=None, team=None, reward=0):
    """Parent attempt is agent-only; child files live under verifier-execution/."""
    identity, team, bundle = identity or uuid4(), team or uuid4(), uuid4()
    prefix = f"trials/{team}/{identity}/attempts/1/bundles/{bundle}/files/"
    output = json.dumps({"rewards": {"passed": reward}}).encode()
    child_logs = {"01-verifier.stdout": b"one failed test\n", "01-verifier.stderr": b""}
    child = legacy_result().model_dump(mode="json")
    phase = child["phases"][0]
    phase.update(ordinal=1, exit_code=0)
    for name, body in child_logs.items():
        stream = name.split(".", 1)[1]
        phase[stream].update(
            path=name, sha256="sha256:" + hashlib.sha256(body).hexdigest(),
            bytes_seen=len(body), bytes_saved=len(body),
        )
    child.update(
        status="succeeded", partial_evidence=False, execution_role="verifier",
        container_roles=["execution", "verifier"], verifier_rewards={"passed": reward},
        phases=[phase], outputs=[child["outputs"][1]],
    )
    child["outputs"][0].update(size_bytes=len(output), sha256="sha256:"+hashlib.sha256(output).hexdigest())
    child_bodies = {**child_logs, "verifier/output.json": output, "result.json": json.dumps(child).encode()}

    agent = deepcopy(phase)
    agent.update(role="agent", ordinal=1, exit_code=0, timed_out=False)
    for name in ("stdout", "stderr"):
        agent[name]["path"] = f"01-agent.{name}"
    attempt = legacy_result().model_dump(mode="json")
    attempt.update(
        status="succeeded", partial_evidence=False, execution_role="attempt",
        container_roles=["execution", "agent"], verifier_rewards=None,
        phases=[agent], outputs=[],
    )
    attempt_bodies = {"result.json": json.dumps(attempt).encode()}
    index, objects = [], {}
    for path, body in attempt_bodies.items():
        index.append({"relative_path": path, "key": prefix+path, "bucket": "artifacts",
                      "size_bytes": len(body), "sha256": "sha256:"+hashlib.sha256(body).hexdigest()})
        objects[("artifacts", prefix+path)] = body
    for path, body in child_bodies.items():
        namespaced = "verifier-execution/" + path
        index.append({"relative_path": namespaced, "key": prefix+namespaced, "bucket": "artifacts",
                      "size_bytes": len(body), "sha256": "sha256:"+hashlib.sha256(body).hexdigest()})
        objects[("artifacts", prefix+namespaced)] = body
    trial = Trial(
        id=identity, team_id=team, task_id="task", batch_id=uuid4(),
        state="succeeded", attempt_count=1,
        config={"agent_name": "terminus-2", "verifier_env_mode": "separate"},
        result={
            "runtime_result": attempt, "reward": {"passed": reward},
            "verifier_execution": {
                "state": "committed", "lease_id": str(uuid4()), "runtime_result": child,
            },
        },
        trajectory_index={"attempt": 1, "artifacts": index},
    )
    return trial, _FakeS3(objects)


@pytest.mark.parametrize("timed_out", [False, True])
def test_native_verifier_exports_original_logs_result_and_zero_reward(timed_out):
    trial, s3 = native_trial(timed_out=timed_out)
    before = deepcopy(trial.result)
    files = resolve_verifier_artifacts(trial, client=s3, artifacts_bucket="artifacts")
    contents = {f.archive_path: f.data for f in files}
    assert contents["verifier/02-verifier.stdout"] == b"one failed test\n"
    assert json.loads(contents["verifier/output.json"])["rewards"] == {"passed": 0}
    assert json.loads(contents["verifier/runtime-result.json"])["status"] == ("timed_out" if timed_out else "succeeded")
    assert trial.result == before
    assert trial.state == ("failed" if timed_out else "succeeded")


@pytest.mark.parametrize("damage", ["missing_log", "missing_result", "wrong_team", "wrong_attempt", "hash", "blocked", "score", "phase_path", "secret"])
def test_native_verifier_rejects_missing_corrupt_or_unrelated_evidence(damage):
    trial, s3 = native_trial()
    index = trial.trajectory_index["artifacts"]
    if damage == "missing_log":
        index.pop(0)
    elif damage == "missing_result":
        index.pop()
    elif damage == "wrong_team":
        index[0]["key"] = index[0]["key"].replace(str(trial.team_id), str(uuid4()))
    elif damage == "wrong_attempt":
        trial.attempt_count = 2
    elif damage == "hash":
        index[0]["sha256"] = "sha256:" + "0" * 64
    elif damage == "blocked":
        index[0]["share_status"] = "blocked"
    else:
        result = trial.result["runtime_result"]
        if damage == "score":
            result["verifier_rewards"] = {"passed": 1}
        elif damage == "phase_path":
            result["phases"][1]["stdout"]["path"] = "01-agent.stdout"
        else:
            row = index[0]
            body = b"Bearer abcdefghijklmnop"
            s3._objects[("artifacts", row["key"])] = body
            row.update(size_bytes=len(body), sha256="sha256:"+hashlib.sha256(body).hexdigest())
            result["phases"][1]["stdout"].update(bytes_seen=len(body), bytes_saved=len(body), sha256=row["sha256"])
        row = index[-1]
        body = json.dumps(result).encode()
        s3._objects[("artifacts", row["key"])] = body
        row.update(size_bytes=len(body), sha256="sha256:"+hashlib.sha256(body).hexdigest())
    with pytest.raises(Tb2V2ExportError):
        resolve_verifier_artifacts(trial, client=s3, artifacts_bucket="artifacts")


def test_native_verifier_packs_separate_child_namespace():
    trial, s3 = separate_native_trial()
    before = deepcopy(trial.result)
    files = resolve_verifier_artifacts(trial, client=s3, artifacts_bucket="artifacts")
    contents = {f.archive_path: f.data for f in files}
    assert contents["verifier/01-verifier.stdout"] == b"one failed test\n"
    assert json.loads(contents["verifier/output.json"])["rewards"] == {"passed": 0}
    packed = json.loads(contents["verifier/runtime-result.json"])
    assert packed["execution_role"] == "verifier"
    assert packed["status"] == "succeeded"
    assert [phase["role"] for phase in packed["phases"]] == ["verifier"]
    assert trial.result == before


def test_native_verifier_sts_shared_still_uses_in_attempt_result():
    trial, s3 = native_trial()
    trial.config = {
        **trial.config,
        "multi_model": {
            "policy": "student_teacher_student",
            "switch_episode": 2,
            "teacher_episodes": 1,
        },
    }
    files = resolve_verifier_artifacts(trial, client=s3, artifacts_bucket="artifacts")
    packed = json.loads(next(f.data for f in files if f.archive_path == "verifier/runtime-result.json"))
    assert packed["execution_role"] == "attempt"
    assert {phase["role"] for phase in packed["phases"]} == {"agent", "verifier"}


def test_skip_verifier_does_not_require_native_child_or_in_attempt_phase():
    trial, s3 = native_trial()
    trial.config = {**trial.config, "skip_verifier": True}
    assert resolve_verifier_artifacts(trial, client=s3, artifacts_bucket="artifacts") == []


@pytest.mark.parametrize("damage", ["missing_child_result", "pending_handoff", "wrong_child_role", "child_vs_attempt"])
def test_native_verifier_rejects_incomplete_or_wrong_separate_evidence(damage):
    trial, s3 = separate_native_trial()
    if damage == "missing_child_result":
        trial.trajectory_index["artifacts"] = [
            row for row in trial.trajectory_index["artifacts"]
            if row["relative_path"] != "verifier-execution/result.json"
        ]
    elif damage == "pending_handoff":
        trial.result["verifier_execution"]["state"] = "pending"
    elif damage == "wrong_child_role":
        child = trial.result["verifier_execution"]["runtime_result"]
        child["execution_role"] = "attempt"
        row = next(
            item for item in trial.trajectory_index["artifacts"]
            if item["relative_path"] == "verifier-execution/result.json"
        )
        body = json.dumps(child).encode()
        s3._objects[("artifacts", row["key"])] = body
        row.update(size_bytes=len(body), sha256="sha256:"+hashlib.sha256(body).hexdigest())
    else:
        trial.result["verifier_execution"]["runtime_result"] = trial.result["runtime_result"]
    with pytest.raises(Tb2V2ExportError):
        resolve_verifier_artifacts(trial, client=s3, artifacts_bucket="artifacts")
