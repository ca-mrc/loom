from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from loom.db.schema import Trial
from loom.models.trajectory import (
    OpenHandsSdkArtifactRefEvent,
    OpenHandsSdkRuntimeProvenanceEvent,
)
from loom_service.delivery_export_openhands import (
    OpenHandsExportError,
    resolve_native_artifacts,
    validate_openhands_eligibility,
)


def _trial(*, agent_name: str = "openhands-sdk") -> Trial:
    trial_id = uuid4()
    return Trial(
        id=trial_id,
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": agent_name},
        trajectory_index={"artifacts": []},
    )


def _common_fields(*, trial_id: UUID) -> dict[str, object]:
    return {
        "emitted_at": datetime.now(UTC),
        "trial_id": trial_id,
        "step_id": "main",
        "seq": 1,
    }


def test_eligibility_rejects_non_openhands_agent() -> None:
    trial = _trial(agent_name="terminus-2")
    with pytest.raises(OpenHandsExportError) as exc:
        validate_openhands_eligibility([], trial)
    assert exc.value.code == "incompatible_agent"


def test_eligibility_accepts_legacy_openhands_alias_but_still_needs_evidence() -> None:
    """#2054: `openhands` resolves to `openhands-sdk`; evidence is still required."""
    trial = _trial(agent_name="openhands")
    with pytest.raises(OpenHandsExportError) as exc:
        validate_openhands_eligibility([], trial)
    assert exc.value.code == "missing_provenance"


def test_eligibility_requires_provenance_and_artifact_ref() -> None:
    trial = _trial()
    with pytest.raises(OpenHandsExportError) as exc:
        validate_openhands_eligibility([], trial)
    assert exc.value.code == "missing_provenance"

    provenance = OpenHandsSdkRuntimeProvenanceEvent(
        **_common_fields(trial_id=trial.id),
        sdk_version="1.34.0",
        openhands_tools_version="1.34.0",
        loom_bridge_revision="1.0",
    )
    with pytest.raises(OpenHandsExportError) as exc:
        validate_openhands_eligibility([provenance], trial)
    assert exc.value.code == "missing_native_artifact"


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}


def test_resolve_native_artifacts_hash_mismatch_fail_closed() -> None:
    trial = _trial()
    native = json.dumps([{"event_type": "ActionEvent"}]).encode()
    actual_hash = hashlib.sha256(native).hexdigest()
    artifact_key = f"{trial.team_id}/{trial.id}/main/.loom/agent/openhands_sdk_events.json"
    trial.trajectory_index = {
        "artifacts": [
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": artifact_key,
                "size": len(native),
                "content_hash": f"sha256:{actual_hash}",
            }
        ]
    }
    ref = OpenHandsSdkArtifactRefEvent(
        **_common_fields(trial_id=trial.id),
        artifact_kind="openhands_sdk.events",
        sandbox_path=".loom/agent/openhands_sdk_events.json",
        content_hash="deadbeef",
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", artifact_key): native})
    with pytest.raises(OpenHandsExportError) as exc:
        resolve_native_artifacts(
            trial,
            [ref],
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_native_artifact"
    assert exc.value.detail["expected_hash"] == "deadbeef"
    assert exc.value.detail["actual_hash"] == actual_hash


def test_resolve_native_artifacts_success() -> None:
    trial = _trial()
    native = json.dumps([{"event_type": "MessageEvent"}]).encode()
    actual_hash = hashlib.sha256(native).hexdigest()
    artifact_key = f"{trial.team_id}/{trial.id}/main/.loom/agent/openhands_sdk_events.json"
    trial.trajectory_index = {
        "artifacts": [
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": artifact_key,
                "size": len(native),
                "content_hash": f"sha256:{actual_hash}",
            }
        ]
    }
    ref = OpenHandsSdkArtifactRefEvent(
        **_common_fields(trial_id=trial.id),
        artifact_kind="openhands_sdk.events",
        sandbox_path=".loom/agent/openhands_sdk_events.json",
        content_hash=actual_hash,
        size_bytes=len(native),
        share_policy="restricted",
    )
    client = _FakeS3({("artifacts", artifact_key): native})
    resolved = resolve_native_artifacts(
        trial,
        [ref],
        client=client,
        artifacts_bucket="artifacts",
    )
    assert resolved["native/openhands_sdk_events.json"] == native
