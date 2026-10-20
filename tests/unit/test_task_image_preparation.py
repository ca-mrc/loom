"""Public build diagnostics retain useful exit details without builder secrets."""

from types import SimpleNamespace
from uuid import uuid4

from loom_service.task_image_preparation import preparation_response


def materialization(**changes):
    return SimpleNamespace(
        **{
            "id": uuid4(),
            "cpu_arch": "x86_64",
            "state": "failed",
            "attempt_count": 1,
            "lease_epoch": 3,
            "failure_reason": "build_build_failed",
            "failure_message": "secret=do-not-return registry.private/team/image",
            "next_attempt_at": None,
            **changes,
        }
    )


def test_failed_build_reports_exit_code_without_arbitrary_builder_text():
    row = materialization()
    attempt = SimpleNamespace(
        materialization_id=row.id,
        lease_epoch=3,
        native_build={
            "builder_log": "unknown-credential-value private.registry/team",
            "configmap": {"data": {"claim.json": "secret"}},
            "phases": [
                {
                    "name": "build",
                    "state": {
                        "terminated": {
                            "exitCode": 37,
                            "message": "secret",
                            "reason": "secret",
                            "finishedAt": "2026-09-13T00:06:00Z",
                        }
                    },
                }
            ],
            "capacity_released_at": "2026-09-13T00:07:00Z",
        },
    )
    result = preparation_response(row, attempt)
    assert result["failure_reason"] == "build_build_failed"
    assert (
        result["message"]
        == "Task image build failed (exit code 37). Check the task Dockerfile and its build inputs."
    )
    assert result["phases"][0]["exit_code"] == 37
    assert result["resources_released"] is True
    assert "secret" not in str(result)
    assert "registry" not in str(result)


def test_cancelled_build_can_be_queued_without_active_demand():
    result = preparation_response(
        materialization(state="queued", failure_reason="build_cancelled"), None
    )
    assert result["state"] == "queued"
    assert (
        result["message"]
        == "Task image preparation stopped because no active trial requires this build."
    )
    assert result["resources_released"] is None


def test_mismatched_attempt_is_not_projected_and_unknown_reason_is_bounded():
    row = materialization(failure_reason="password=secret")
    attempt = SimpleNamespace(
        materialization_id=row.id,
        lease_epoch=2,
        native_build={
            "phases": [{"name": "build", "state": {"terminated": {"exitCode": 99}}}],
        },
    )
    result = preparation_response(row, attempt)
    assert result["failure_reason"] == "build_failed"
    assert result["phases"] == []
    assert "secret" not in str(result)


def test_only_known_phase_shape_and_timestamps_are_returned():
    row = materialization(state="running", failure_reason=None)
    attempt = SimpleNamespace(
        materialization_id=row.id,
        lease_epoch=3,
        native_build={
            "phases": [
                {"name": "password=secret", "state": {"running": {}}},
                {"name": "prepare", "state": {"waiting": {"message": "secret"}}},
                {"name": "build", "state": {"running": {"startedAt": "secret"}}},
            ],
        },
    )
    result = preparation_response(row, attempt)
    assert result["message"] is None
    assert [phase["name"] for phase in result["phases"]] == ["prepare", "build"]
    assert "secret" not in str(result)


def test_build_budget_and_job_deadline_use_structured_evidence_only():
    row = materialization(failure_reason="build_deadline_exceeded")
    native = {"phases": [{"name": "build", "state": {"terminated": {"exitCode": 124}}}],
              "builder_log": "private; build complete; scratch cleanup started"}
    attempt = SimpleNamespace(materialization_id=row.id, lease_epoch=3, native_build=native)
    result = preparation_response(row, attempt)
    assert "task's build-time budget" in result["message"]
    native["job_conditions"] = [{"status": "True", "reason": "DeadlineExceeded"}]
    result = preparation_response(row, attempt)
    assert "platform Job lifecycle deadline" in result["message"]
    assert "private" not in str(result)
    native.clear()
    assert preparation_response(row, attempt)["message"] == "Task image preparation exceeded its deadline."


def test_platform_preparation_incompatibility_keeps_typed_reason_without_raw_logs():
    result = preparation_response(materialization(failure_reason="image_preparation_base_unsupported"), None)
    assert result["failure_reason"] == "image_preparation_base_unsupported"
    assert result["attribution"] == "platform"
    assert result["retryable"] is False
    assert "secret" not in str(result) and "registry.private" not in str(result)
