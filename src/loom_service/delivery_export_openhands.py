"""openhands-export delivery export framework (#1590)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from loom.agent.openhands_sdk.mapper import OpenHandsSdkTrajectoryMapper
from loom.agent.openhands_sdk.provenance import LOOM_BRIDGE_REVISION
from loom.db.schema import LlmCall, Trial
from loom.models.trajectory import (
    EventKind,
    OpenHandsSdkArtifactRefEvent,
    OpenHandsSdkRuntimeProvenanceEvent,
    TrajectoryEvent,
)
from loom_service.agent_catalog import get_agent
from loom_service.delivery_export_tb2_v2 import (
    _fetch_artifact_bytes,
    _index_artifacts,
    _normalize_hash,
    _resolve_indexed_artifact_bucket,
    build_model_input_trajectory,
    parse_trajectory_events,
    scan_members_for_secrets,
)

NATIVE_OPENHANDS_SDK_EVENTS = "native/openhands_sdk_events.json"
SANDBOX_OPENHANDS_SDK_EVENTS = ".loom/agent/openhands_sdk_events.json"


class OpenHandsExportError(Exception):
    """Fail-closed export errors for openhands-export."""

    status_code = 409

    def __init__(self, code: str, detail: dict[str, Any]) -> None:
        self.code = code
        super().__init__(code)
        self.detail = {"code": code, **detail}


@dataclass
class OpenHandsTrialBundle:
    execution_trajectory: dict[str, Any]
    model_input_trajectory: dict[str, Any]
    export_provenance: dict[str, Any]
    native_artifacts: dict[str, bytes] = field(default_factory=dict)
    artifact_manifest_entries: list[dict[str, str]] = field(default_factory=list)


def _agent_name_for_trial(trial: Trial) -> str:
    config = trial.config if isinstance(trial.config, dict) else {}
    raw = config.get("agent_name")
    return str(raw) if raw else "unknown"


def validate_openhands_eligibility(
    events: list[TrajectoryEvent],
    trial: Trial,
) -> None:
    agent_name = _agent_name_for_trial(trial)
    # `openhands` is the legacy alias of `openhands-sdk` (#2054); both run the
    # same SDK runner, and the checks below still require its typed evidence.
    entry = get_agent(agent_name)
    if entry is None or entry.name != "openhands-sdk":
        raise OpenHandsExportError(
            "incompatible_agent",
            {
                "message": "openhands-export requires openhands-sdk trials",
                "trial_id": str(trial.id),
                "agent_name": agent_name,
            },
        )

    provenance_events = [
        event
        for event in events
        if event.kind == EventKind.OPENHANDS_SDK_RUNTIME_PROVENANCE
    ]
    if not provenance_events:
        raise OpenHandsExportError(
            "missing_provenance",
            {
                "message": "trajectory is missing openhands_sdk_runtime_provenance",
                "trial_id": str(trial.id),
            },
        )

    artifact_refs = [
        event
        for event in events
        if isinstance(event, OpenHandsSdkArtifactRefEvent)
    ]
    if not artifact_refs:
        raise OpenHandsExportError(
            "missing_native_artifact",
            {
                "message": "trajectory is missing openhands_sdk_artifact_ref",
                "trial_id": str(trial.id),
            },
        )


def resolve_native_artifacts(
    trial: Trial,
    events: list[TrajectoryEvent],
    *,
    client: Any,
    artifacts_bucket: str,
) -> dict[str, bytes]:
    refs = [
        event
        for event in events
        if isinstance(event, OpenHandsSdkArtifactRefEvent)
    ]
    indexed = _index_artifacts(trial)
    resolved: dict[str, bytes] = {}

    for ref in refs:
        if ref.artifact_kind != "openhands_sdk.events":
            continue
        candidates = [
            item
            for item in indexed
            if isinstance(item.get("key"), str)
            and str(item["key"]).endswith(SANDBOX_OPENHANDS_SDK_EVENTS)
        ]
        if not candidates:
            raise OpenHandsExportError(
                "missing_native_artifact",
                {
                    "message": (
                        "openhands_sdk_artifact_ref is present but no matching "
                        "trajectory_index artifact was found"
                    ),
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "sandbox_path": ref.sandbox_path,
                },
            )
        indexed_item = candidates[0]
        data = _fetch_artifact_bytes(
            client,
            bucket=_resolve_indexed_artifact_bucket(
                indexed_item,
                default_bucket=artifacts_bucket,
            ),
            key=str(indexed_item["key"]),
            missing_message="native OpenHands SDK artifact is missing from object storage",
        )
        actual_hash = hashlib.sha256(data).hexdigest()
        expected_hash = _normalize_hash(ref.content_hash)
        if actual_hash != expected_hash:
            raise OpenHandsExportError(
                "missing_native_artifact",
                {
                    "message": "native OpenHands SDK artifact hash does not match artifact ref",
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                },
            )
        resolved[NATIVE_OPENHANDS_SDK_EVENTS] = data

    if NATIVE_OPENHANDS_SDK_EVENTS not in resolved:
        raise OpenHandsExportError(
            "missing_native_artifact",
            {
                "message": "no openhands_sdk.events artifact ref was resolved",
                "trial_id": str(trial.id),
            },
        )
    return resolved


def build_export_provenance(
    events: list[TrajectoryEvent],
    *,
    trial_id: str,
    native_hash: str,
) -> dict[str, Any]:
    provenance = next(
        event
        for event in events
        if isinstance(event, OpenHandsSdkRuntimeProvenanceEvent)
    )
    return {
        "schema_version": "1",
        "trial_id": trial_id,
        "bridge_revision": LOOM_BRIDGE_REVISION,
        "sdk_version": provenance.sdk_version,
        "openhands_tools_version": provenance.openhands_tools_version,
        "loom_bridge_revision": provenance.loom_bridge_revision,
        "native_events_hash": native_hash,
    }


def build_per_trial_openhands_bundle(
    *,
    trial: Trial,
    events: list[TrajectoryEvent],
    calls: list[LlmCall],
    client: Any,
    artifacts_bucket: str,
    messages_from_raw_log: Any,
) -> OpenHandsTrialBundle:
    validate_openhands_eligibility(events, trial)
    native_artifacts = resolve_native_artifacts(
        trial,
        events,
        client=client,
        artifacts_bucket=artifacts_bucket,
    )
    native_bytes = native_artifacts[NATIVE_OPENHANDS_SDK_EVENTS]
    native_hash = hashlib.sha256(native_bytes).hexdigest()
    execution_trajectory = OpenHandsSdkTrajectoryMapper.project_trajectory(native_bytes)
    model_input_trajectory = build_model_input_trajectory(
        trial_id=str(trial.id),
        task_id=trial.task_id,
        calls=calls,
        messages_from_raw_log=messages_from_raw_log,
    )
    export_provenance = build_export_provenance(
        events,
        trial_id=str(trial.id),
        native_hash=native_hash,
    )
    artifact_manifest_entries = [
        {"kind": "execution_trajectory", "path": "trajectory.json"},
        {"kind": "model_input_trajectory", "path": "model_input_trajectory.json"},
        {"kind": "audit_spine", "path": "loom_trajectory.jsonl"},
        {"kind": "export_provenance", "path": "export_provenance.json"},
        {"kind": "native_openhands_sdk_events", "path": NATIVE_OPENHANDS_SDK_EVENTS},
    ]
    scan_payload = {
        "trajectory.json": json.dumps(execution_trajectory, ensure_ascii=False).encode(),
        "model_input_trajectory.json": json.dumps(
            model_input_trajectory,
            ensure_ascii=False,
        ).encode(),
        "export_provenance.json": json.dumps(export_provenance, ensure_ascii=False).encode(),
        **native_artifacts,
    }
    scan_members_for_secrets(scan_payload)
    return OpenHandsTrialBundle(
        execution_trajectory=execution_trajectory,
        model_input_trajectory=model_input_trajectory,
        export_provenance=export_provenance,
        native_artifacts=native_artifacts,
        artifact_manifest_entries=artifact_manifest_entries,
    )


__all__ = [
    "NATIVE_OPENHANDS_SDK_EVENTS",
    "OpenHandsExportError",
    "OpenHandsTrialBundle",
    "build_per_trial_openhands_bundle",
    "parse_trajectory_events",
    "resolve_native_artifacts",
    "validate_openhands_eligibility",
]
