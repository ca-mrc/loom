"""Resolve deadline-retained native files without modifying the frozen event log."""
from __future__ import annotations

import hashlib
from typing import Any, NoReturn

from loom.db.schema import Trial
from loom_service.delivery_export_native_verifier import resolve_native_verifier_artifacts
from loom_service.delivery_export_tb2_v2 import (
    MAX_JSONL_BYTES,
    NATIVE_HARBOR_TRAJECTORY,
    NATIVE_RECORDING_CAST,
    Tb2V2ExportError,
    _fetch_bounded_verifier_artifact,
)


def resolve_timeout_native_artifacts(
    trial: Trial, *, client: Any, artifacts_bucket: str,
) -> dict[str, bytes]:
    """Bind native bytes to the same validated attempt bundle as its verifier.

    At the agent deadline the event writer is fenced, but the runtime still
    commits its local Harbor files during bounded finalization. That committed
    result/index is the authority for these files, not a fabricated late event.
    """
    indexed = (trial.trajectory_index or {}).get("artifacts", [])
    verifier = resolve_native_verifier_artifacts(
        trial, indexed=indexed, client=client, artifacts_bucket=artifacts_bucket,
    )
    runtime_file = next(x for x in verifier if x.archive_path == "verifier/runtime-result.json")
    prefix = runtime_file.source_key.removesuffix("result.json")
    assert trial.result is not None  # The verifier resolver validated this outcome.
    runtime = trial.result["runtime_result"]
    resolved: dict[str, bytes] = {}

    def fail(message: str) -> None:
        raise Tb2V2ExportError("invalid_native_artifact", {
            "message": message, "trial_id": str(trial.id),
        })

    for path, archive_path, required in (
        ("artifacts/harbor/trajectory.json", NATIVE_HARBOR_TRAJECTORY, True),
        ("artifacts/harbor/recording.cast", NATIVE_RECORDING_CAST, False),
    ):
        outputs = [x for x in runtime["outputs"] if x.get("relative_path") == path]
        if not required and (not outputs or outputs[0].get("state") == "missing"):
            continue
        if len(outputs) != 1 or outputs[0].get("state") != "captured" or outputs[0].get("kind") != "agent_native":
            fail("timeout requires captured native runtime evidence")
        matches = [x for x in indexed if x.get("relative_path") == path]
        if len(matches) != 1:
            fail("timeout requires one indexed native file")
        row, output = matches[0], outputs[0]
        if row.get("key") != prefix + path or row.get("bucket") != artifacts_bucket:
            fail("native file is outside the verified Trial attempt bundle")
        if row.get("share_status") not in (None, "shared") or row.get("blocked_reason"):
            fail("native file is share-blocked")
        size, digest = row.get("size_bytes"), row.get("sha256")
        if (isinstance(size, bool) or not isinstance(size, int) or size < 0
                or size != output.get("size_bytes") or digest != output.get("sha256")):
            fail("native index differs from committed runtime output")
        data = _fetch_bounded_verifier_artifact(
            client, bucket=artifacts_bucket, key=row["key"], indexed_size=size,
            max_bytes=MAX_JSONL_BYTES,
        )
        if "sha256:" + hashlib.sha256(data).hexdigest() != digest:
            fail("native file bytes differ from committed runtime output")
        resolved[archive_path] = data
    return resolved


def resolve_committed_agent_artifacts(
    trial: Trial, *, client: Any, artifacts_bucket: str,
) -> dict[str, bytes]:
    """Bind the agent Harbor file without requiring a verifier result.

    Ungraded timeouts and verifier errors keep the trajectory the attempt
    committed. A missing or mismatched trajectory rejects the export. A
    recording is copied only when that attempt captured one.
    """
    indexed = (trial.trajectory_index or {}).get("artifacts", [])
    if not isinstance(indexed, list):
        indexed = []
    result_rows = [
        row for row in indexed
        if isinstance(row, dict) and row.get("relative_path") == "result.json"
    ]
    if len(result_rows) != 1 or not isinstance(result_rows[0].get("key"), str):
        raise Tb2V2ExportError("invalid_native_artifact", {
            "message": "ungraded export requires the attempt result",
            "trial_id": str(trial.id),
        })
    prefix = result_rows[0]["key"].removesuffix("result.json")
    if "verifier-execution/" in prefix or not prefix.endswith("/files/"):
        raise Tb2V2ExportError("invalid_native_artifact", {
            "message": "ungraded export requires the attempt result",
            "trial_id": str(trial.id),
        })
    assert trial.result is not None
    runtime = trial.result["runtime_result"]
    resolved: dict[str, bytes] = {}

    def fail(message: str) -> NoReturn:
        raise Tb2V2ExportError("invalid_native_artifact", {
            "message": message, "trial_id": str(trial.id),
        })

    for path, archive_path, required in (
        ("artifacts/harbor/trajectory.json", NATIVE_HARBOR_TRAJECTORY, True),
        ("artifacts/harbor/recording.cast", NATIVE_RECORDING_CAST, False),
    ):
        outputs = [item for item in runtime["outputs"] if item.get("relative_path") == path]
        if not required and (not outputs or outputs[0].get("state") == "missing"):
            continue
        if (
            len(outputs) != 1
            or outputs[0].get("state") != "captured"
            or outputs[0].get("kind") != "agent_native"
        ):
            fail("ungraded export requires a captured agent trajectory")
        matches = [item for item in indexed if isinstance(item, dict) and item.get("relative_path") == path]
        if len(matches) != 1:
            fail("ungraded export requires one indexed native file")
        row, output = matches[0], outputs[0]
        if row.get("key") != prefix + path or row.get("bucket") != artifacts_bucket:
            fail("native file is outside the verified Trial attempt bundle")
        if row.get("share_status") not in (None, "shared") or row.get("blocked_reason"):
            fail("native file is share-blocked")
        size, digest = row.get("size_bytes"), row.get("sha256")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            fail("native index differs from committed runtime output")
        if size != output.get("size_bytes") or digest != output.get("sha256"):
            fail("native index differs from committed runtime output")
        data = _fetch_bounded_verifier_artifact(
            client, bucket=artifacts_bucket, key=row["key"], indexed_size=size,
            max_bytes=MAX_JSONL_BYTES,
        )
        if "sha256:" + hashlib.sha256(data).hexdigest() != digest:
            fail("native file bytes differ from committed runtime output")
        resolved[archive_path] = data
    if NATIVE_HARBOR_TRAJECTORY not in resolved:
        fail("ungraded export requires a captured agent trajectory")
    return resolved
