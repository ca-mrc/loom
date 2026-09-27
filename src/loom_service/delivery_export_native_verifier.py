"""Read native verifier evidence from the committed, attempt-bound file index."""
from __future__ import annotations

import hashlib
import re
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from loom.db.schema import Trial
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.models.verifier import VerifierResult
from loom_service.delivery_export_tb2_v2 import (
    MAX_VERIFIER_LOG_BYTES,
    MAX_VERIFIER_OUTPUT_BYTES,
    Tb2V2ExportError,
    VerifierDeliveryArtifact,
    _fetch_bounded_verifier_artifact,
    scan_members_for_secrets,
)


def resolve_native_verifier_artifacts(
    trial: Trial, *, indexed: list[dict[str, Any]], client: Any, artifacts_bucket: str,
) -> list[VerifierDeliveryArtifact]:
    """Keep native names and phase metadata; never fabricate legacy log pairs."""
    def fail(code: str, message: str) -> None:
        raise Tb2V2ExportError(code, {"message": message, "trial_id": str(trial.id)})

    index = trial.trajectory_index or {}
    if not trial.attempt_count or index.get("attempt") != trial.attempt_count:
        fail("invalid_verifier_artifact_index", "native verifier index has another attempt")
    prefix = f"trials/{trial.team_id}/{trial.id}/attempts/{trial.attempt_count}/bundles/"
    bundle_prefix: str | None = None
    resolved: list[VerifierDeliveryArtifact] = []

    def read(path: str, archive_path: str, limit: int, *, truncated: bool | None = None) -> bytes:
        nonlocal bundle_prefix
        matches = [row for row in indexed if row.get("relative_path") == path]
        if len(matches) != 1:
            fail("missing_verifier_artifact", f"native verifier requires one indexed {path}")
        row = matches[0]
        key = row.get("key")
        suffix = "/files/" + path
        if not isinstance(key, str) or not key.startswith(prefix) or not key.endswith(suffix):
            fail("invalid_verifier_artifact_index", "native verifier key is outside this Trial attempt")
        assert isinstance(key, str)
        bundle = key[len(prefix):-len(suffix)]
        try:
            if str(UUID(bundle)) != bundle:
                raise ValueError("noncanonical bundle")
        except ValueError:
            fail("invalid_verifier_artifact_index", "native verifier bundle identity is invalid")
        current_prefix = prefix + bundle
        if bundle_prefix is not None and current_prefix != bundle_prefix:
            fail("invalid_verifier_artifact_index", "native verifier files span different bundles")
        bundle_prefix = current_prefix
        # Canonical rows are published by the materializer, unlike legacy
        # workspace globs. Respect explicit sharing blocks when present and scan
        # the exact bytes before approving them for the delivery archive.
        if row.get("share_status") not in (None, "shared") or row.get("blocked_reason"):
            fail("verifier_artifact_blocked", "native verifier artifact is share-blocked")
        if row.get("bucket") != artifacts_bucket:
            fail("invalid_verifier_artifact_index", "native verifier artifact has another bucket")
        size, digest = row.get("size_bytes"), row.get("sha256")
        if (isinstance(size, bool) or not isinstance(size, int) or size < 0
                or not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)):
            fail("invalid_verifier_artifact_index", "native verifier size or digest is invalid")
        assert isinstance(size, int) and isinstance(digest, str)
        data = _fetch_bounded_verifier_artifact(
            client, bucket=artifacts_bucket, key=key, indexed_size=size, max_bytes=limit,
        )
        if "sha256:" + hashlib.sha256(data).hexdigest() != digest:
            fail("verifier_artifact_hash_mismatch", "native verifier bytes do not match the index")
        resolved.append(VerifierDeliveryArtifact(
            archive_path=archive_path, data=data, content_hash=digest, size_bytes=size,
            truncated=truncated, share_status="shared", blocked_reason=None,
            step_name="verifier", source_key=key,
        ))
        return data

    body = read("result.json", "verifier/runtime-result.json", 16 * 1024 * 1024)
    try:
        runtime = ExecutionRuntimeResultV1.model_validate_json(body)
    except ValidationError as exc:
        raise Tb2V2ExportError("invalid_verifier_artifact_metadata", {
            "message": "native verifier runtime result is invalid", "trial_id": str(trial.id),
        }) from exc
    if (trial.result or {}).get("runtime_result") != runtime.model_dump(mode="json"):
        fail("invalid_verifier_artifact_metadata", "native result differs from the persisted runtime outcome")
    phases = [phase for phase in runtime.phases if phase.role == "verifier"]
    if len(phases) != 1 or runtime.execution_role != "attempt":
        fail("missing_verifier_artifact", "native attempt requires one independent verifier phase")
    phase = phases[0]
    for name in ("stdout", "stderr"):
        stream = getattr(phase, name)
        path = f"{phase.ordinal:02d}-verifier.{name}"
        if stream.path != path:
            fail("invalid_verifier_artifact_metadata", "verifier stream refers to another phase")
        data = read(path, "verifier/" + path, MAX_VERIFIER_LOG_BYTES, truncated=stream.truncated)
        if len(data) != stream.bytes_saved or resolved[-1].content_hash != stream.sha256:
            fail("invalid_verifier_artifact_metadata", "verifier stream differs from runtime evidence")
    output = read("verifier/output.json", "verifier/output.json", MAX_VERIFIER_OUTPUT_BYTES)
    evidence = [item for item in runtime.outputs if item.relative_path == "verifier/output.json"]
    if (len(evidence) != 1 or evidence[0].kind != "verifier" or evidence[0].state != "captured"
            or evidence[0].size_bytes != len(output)
            or evidence[0].sha256 != resolved[-1].content_hash):
        fail("invalid_verifier_artifact_metadata", "verifier output differs from runtime evidence")
    try:
        verifier = VerifierResult.model_validate_json(output)
    except ValidationError as exc:
        raise Tb2V2ExportError("invalid_verifier_artifact_metadata", {
            "message": "native verifier output is invalid", "trial_id": str(trial.id),
        }) from exc
    if verifier.rewards != runtime.verifier_rewards:
        fail("invalid_verifier_artifact_metadata", "verifier reward differs from the runtime outcome")
    # Optional structured test reports are still original, declared evidence.
    for item in runtime.outputs:
        if item.relative_path == "artifacts/verifier/ctrf.json" and item.state == "captured":
            data = read(item.relative_path, "verifier/ctrf.json", MAX_VERIFIER_OUTPUT_BYTES)
            if len(data) != item.size_bytes or resolved[-1].content_hash != item.sha256:
                fail("invalid_verifier_artifact_metadata", "verifier test report differs from runtime evidence")
    scan_members_for_secrets({item.archive_path: item.data for item in resolved})
    return resolved
