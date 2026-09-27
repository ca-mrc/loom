"""raw-harbor-tb2-v2 delivery export framework (#745)."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, BinaryIO

from botocore.exceptions import ClientError
from pydantic import TypeAdapter, ValidationError

from loom.agent.terminus2.mapper import Terminus2TrajectoryMapper
from loom.agent.terminus2.provenance import HARBOR_COMPAT_SHA, LOOM_BRIDGE_REVISION
from loom.db.schema import LlmCall, Trial
from loom.models.trajectory import (
    AgentRetryEvent,
    EventKind,
    Terminus2ArtifactRefEvent,
    Terminus2RuntimeProvenanceEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
    TrajectoryEvent,
)

MAX_JSONL_BYTES = 50 * 1024 * 1024
MAX_JSONL_LINES = 100_000
LEGACY_BENCHMARK_SHA = "91e10457"
NATIVE_HARBOR_TRAJECTORY = "native/harbor_trajectory.json"
NATIVE_RECORDING_CAST = "native/recording.cast"
SECRET_PATTERNS = (
    re.compile(r"sk-[A-Za-z0-9]{8,}"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}"),
)

# #1263: agent tool_calls that touch Harbor private paths (solution/tests/
# verifier) indicate workspace contamination — fail closed on tb2-v2 pack.
# Strict path forms only (avoid English/dict FPs like "All tests passed",
# "'tests': {", "boundary solution"):
#   - absolute: /app/solution|tests|verifier[/…]
#   - relative path component: solution/|tests/|verifier/
# Shell forms from trial 9b6194d5-… step_id 5–6 (`ls /app/solution/`,
# `cat /app/solution/solve.sh`, `pytest tests/test_outputs.py`).
PRIVATE_PATH_KEYSTROKE_RE = re.compile(
    r"(?i)"
    r"(?:"
    r"/app/(?P<abs_path>solution|tests|verifier)(?:/|\s|$|[;'\"`)|&])"
    r"|"
    r"(?:^|[\s;|&`'\"(])(?P<rel_path>solution|tests|verifier)/"
    r")"
)
_PRIVATE_PATH_MATCH_CAP = 20
_KEYSTROKES_EXCERPT_LEN = 160

_event_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


class Tb2V2ExportError(Exception):
    """Fail-closed export errors for raw-harbor-tb2-v2."""

    status_code = 409

    def __init__(self, code: str, detail: dict[str, Any]) -> None:
        self.code = code
        super().__init__(code)
        self.detail = {"code": code, **detail}


@dataclass
class Tb2V2TrialBundle:
    execution_trajectory: dict[str, Any]
    model_input_trajectory: dict[str, Any]
    terminal_transcript: bytes
    export_provenance: dict[str, Any]
    native_artifacts: dict[str, bytes] = field(default_factory=dict)
    artifact_manifest_entries: list[dict[str, str]] = field(default_factory=list)


def _iter_jsonl_raw_lines(stream: BinaryIO) -> Iterator[bytes]:
    """Yield JSONL records as raw bytes.

    boto3 ``StreamingBody.__iter__`` yields fixed-size *chunks* (typically
    1 KiB), not newline-delimited records. Prefer ``iter_lines()`` when
    present so MinIO GetObject streams parse as true JSONL.
    """
    iter_lines = getattr(stream, "iter_lines", None)
    if callable(iter_lines):
        yield from iter_lines()
        return
    yield from stream


def parse_trajectory_events(stream: BinaryIO) -> list[TrajectoryEvent]:
    """Parse trajectory JSONL with bounded size/line caps."""
    events: list[TrajectoryEvent] = []
    total_bytes = 0
    for line_no, raw in enumerate(_iter_jsonl_raw_lines(stream), start=1):
        if line_no > MAX_JSONL_LINES:
            raise Tb2V2ExportError(
                "trajectory_parse_limit_exceeded",
                {
                    "message": "trajectory JSONL exceeds maximum line count",
                    "max_lines": MAX_JSONL_LINES,
                },
            )
        total_bytes += len(raw)
        if total_bytes > MAX_JSONL_BYTES:
            raise Tb2V2ExportError(
                "trajectory_parse_limit_exceeded",
                {
                    "message": "trajectory JSONL exceeds maximum byte size",
                    "max_bytes": MAX_JSONL_BYTES,
                },
            )
        line = raw.strip()
        if not line:
            continue
        try:
            events.append(_event_adapter.validate_json(line))
        except ValidationError as exc:
            raise Tb2V2ExportError(
                "trajectory_parse_failed",
                {
                    "message": "trajectory JSONL contains an invalid event",
                    "line": line_no,
                    "error": str(exc),
                },
            ) from exc
    return events


def _agent_name_for_trial(trial: Trial) -> str:
    config = trial.config if isinstance(trial.config, dict) else {}
    raw = config.get("agent_name")
    return str(raw) if raw else "unknown"


def _trial_skipped_verifier(trial: Trial) -> bool:
    config = trial.config if isinstance(trial.config, dict) else {}
    return config.get("skip_verifier") is True


def _has_legacy_runtime_markers(events: list[TrajectoryEvent]) -> bool:
    for event in events:
        if event.kind == EventKind.AGENT_THOUGHT:
            return True
        if event.kind == EventKind.ENV_EXEC:
            return True
    return False


def _normalize_hash(value: str) -> str:
    normalized = value.strip().lower()
    if normalized.startswith("sha256:"):
        return normalized.removeprefix("sha256:")
    return normalized


def validate_v2_eligibility(events: list[TrajectoryEvent], trial: Trial) -> None:
    agent_name = _agent_name_for_trial(trial)
    if agent_name != "terminus-2":
        raise Tb2V2ExportError(
            "incompatible_agent",
            {
                "message": "raw-harbor-tb2-v2 requires terminus-2 trials",
                "trial_id": str(trial.id),
                "agent_name": agent_name,
            },
        )

    provenance_events = [
        event
        for event in events
        if event.kind == EventKind.TERMINUS2_RUNTIME_PROVENANCE
    ]
    if not provenance_events:
        raise Tb2V2ExportError(
            "missing_provenance",
            {
                "message": "trajectory is missing terminus2_runtime_provenance",
                "trial_id": str(trial.id),
            },
        )
    provenance = provenance_events[0]
    assert isinstance(provenance, Terminus2RuntimeProvenanceEvent)
    if provenance.harbor_compat_sha != HARBOR_COMPAT_SHA:
        raise Tb2V2ExportError(
            "mixed_provenance",
            {
                "message": "harbor_compat_sha does not match the pinned bridge revision",
                "trial_id": str(trial.id),
                "expected": HARBOR_COMPAT_SHA,
                "actual": provenance.harbor_compat_sha,
            },
        )

    benchmark = provenance.benchmark_provenance
    if isinstance(benchmark, dict):
        serialized = json.dumps(benchmark, sort_keys=True)
        if LEGACY_BENCHMARK_SHA in serialized:
            raise Tb2V2ExportError(
                "legacy_benchmark_provenance",
                {
                    "message": "trajectory contains legacy benchmark provenance",
                    "trial_id": str(trial.id),
                },
            )

    turn_events = [event for event in events if event.kind == EventKind.TERMINUS2_TURN]
    if not turn_events:
        if _has_legacy_runtime_markers(events):
            raise Tb2V2ExportError(
                "legacy_runtime_stream",
                {
                    "message": (
                        "trajectory contains legacy subprocess runtime markers "
                        "without typed terminus2 turns"
                    ),
                    "trial_id": str(trial.id),
                },
            )
        raise Tb2V2ExportError(
            "legacy_runtime_stream",
            {
                "message": "trajectory lacks typed terminus2 turn events",
                "trial_id": str(trial.id),
            },
        )


def validate_v2_joins(events: list[TrajectoryEvent]) -> list[str]:
    errors = Terminus2TrajectoryMapper.validate_turn_joins(events)
    turn_events = [
        event
        for event in events
        if isinstance(event, Terminus2TurnEvent)
    ]
    if not turn_events:
        return errors

    artifact_refs = [
        event
        for event in events
        if isinstance(event, Terminus2ArtifactRefEvent)
    ]
    has_trajectory_ref = any(
        ref.artifact_kind == "terminus_2.pane" for ref in artifact_refs
    )
    if not has_trajectory_ref:
        errors.append(
            "missing terminus2_artifact_ref for native harbor trajectory",
        )
    return errors


def build_model_input_trajectory(
    *,
    trial_id: str,
    task_id: str,
    calls: list[LlmCall],
    messages_from_raw_log: Any,
) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for index, call in enumerate(calls, start=1):
        extras = call.provider_extras if isinstance(call.provider_extras, dict) else {}
        raw_log = extras.get("_loom_raw_provider_log")
        if not isinstance(raw_log, dict):
            continue
        messages = messages_from_raw_log(raw_log, normalize_tb2=False)
        if not messages:
            continue
        entries.append(
            {
                "index": index,
                "llm_call_id": str(call.id),
                "step_id": call.step_id,
                "model": call.model,
                "requested_model": call.requested_model or call.model,
                "response_model": call.response_model,
                "role": call.role,
                "dialect": call.dialect,
                "messages": messages,
            }
        )
    return {
        "schema_version": "harbor-tb2-v2-model-input",
        "trial_id": trial_id,
        "task_id": task_id,
        "source_of_truth": "provider_logs",
        "calls": entries,
    }



def events_after_last_agent_retry(
    events: list[TrajectoryEvent],
) -> list[TrajectoryEvent]:
    """Keep only events after the last gateway agent retry.

    Loom appends every attempt into ``loom_trajectory.jsonl``; native Harbor
    overwrite keeps the final attempt only. Delivery projections should match.
    """
    last_retry_idx: int | None = None
    for i, event in enumerate(events):
        if isinstance(event, AgentRetryEvent):
            last_retry_idx = i
    if last_retry_idx is None:
        return events
    return events[last_retry_idx + 1 :]


def build_terminal_transcript(events: list[TrajectoryEvent]) -> bytes:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, Terminus2TerminalObservationEvent):
            continue
        turn_index = next(
            (
                turn.turn_index
                for turn in events
                if isinstance(turn, Terminus2TurnEvent)
                and turn.turn_id == event.turn_id
            ),
            None,
        )
        rows.append(
            {
                "turn_id": event.turn_id,
                "turn_index": turn_index,
                "text": event.text,
                "content_hash": event.content_hash,
                "is_aggregate": event.is_aggregate,
                "completeness": event.completeness,
                "capture_source": event.capture_source,
            }
        )
    return b"".join(
        (json.dumps(row, ensure_ascii=False) + "\n").encode() for row in rows
    )


def build_execution_trajectory(
    events: list[TrajectoryEvent],
    *,
    task_id: str,
    agent_name: str,
    agent_version: str,
) -> dict[str, Any]:
    return Terminus2TrajectoryMapper.project_to_atif(
        events,
        task_id=task_id,
        agent_name=agent_name,
        agent_version=agent_version,
    )


def reasoning_by_gateway_from_calls(
    calls: list[LlmCall],
    *,
    messages_from_raw_log: Any,
) -> dict[str, str]:
    """Map gateway/llm call id → reasoning_content from provider logs."""
    out: dict[str, str] = {}
    for call in calls:
        extras = call.provider_extras if isinstance(call.provider_extras, dict) else {}
        raw_log = extras.get("_loom_raw_provider_log")
        if not isinstance(raw_log, dict):
            continue
        messages = messages_from_raw_log(raw_log, normalize_tb2=False)
        for message in reversed(messages or []):
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            reasoning = message.get("reasoning_content")
            if isinstance(reasoning, str) and reasoning:
                out[str(call.id)] = reasoning
            break
    return out


def enrich_execution_trajectory(
    trajectory: dict[str, Any],
    *,
    native_bytes: bytes | None,
    reasoning_by_gateway: dict[str, str] | None = None,
) -> dict[str, Any]:
    native: dict[str, Any] | None = None
    if native_bytes:
        try:
            parsed = json.loads(native_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            native = parsed
    return Terminus2TrajectoryMapper.enrich_from_native(
        trajectory,
        native,
        reasoning_by_gateway=reasoning_by_gateway,
    )


def build_export_provenance(
    events: list[TrajectoryEvent],
    *,
    trial_id: str,
    join_errors: list[str],
) -> dict[str, Any]:
    provenance = next(
        event
        for event in events
        if isinstance(event, Terminus2RuntimeProvenanceEvent)
    )
    return {
        "schema_version": "1",
        "trial_id": trial_id,
        "bridge_revision": LOOM_BRIDGE_REVISION,
        "harbor_compat_sha": provenance.harbor_compat_sha,
        "loom_runtime_revision": provenance.loom_runtime_revision,
        "parser_name": provenance.parser_name,
        "benchmark_provenance": provenance.benchmark_provenance,
        "join_validation": {
            "ok": not join_errors,
            "errors": join_errors,
        },
    }


def _index_artifacts(trial: Trial) -> list[dict[str, Any]]:
    trajectory_index = (
        trial.trajectory_index if isinstance(trial.trajectory_index, dict) else {}
    )
    raw = trajectory_index.get("artifacts")
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _resolve_indexed_artifact_bucket(
    item: dict[str, Any],
    *,
    default_bucket: str,
) -> str:
    """Prefer the bucket recorded at upload time over the service setting.

    Workers historically wrote to the CP default ``artifacts`` /
    ``trajectories`` buckets while loom-service was configured with
    env-specific names (e.g. ``loom-staging-artifacts``). Download routes
    already follow the indexed bucket; delivery export must do the same so
    historical trials remain exportable after config alignment.
    """
    bucket = item.get("bucket")
    if isinstance(bucket, str) and bucket.strip():
        return bucket.strip()
    return default_bucket


def _artifact_key_matches(key: str, *, suffix: str) -> bool:
    return key.endswith(suffix) or suffix in key


def _fetch_artifact_bytes(
    client: Any,
    *,
    bucket: str,
    key: str,
    missing_code: str = "missing_native_artifact",
    missing_message: str = "native Harbor artifact is missing from object storage",
) -> bytes:
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        raise Tb2V2ExportError(
            missing_code,
            {
                "message": missing_message,
                "bucket": bucket,
                "key": key,
                "error_code": code,
            },
        ) from exc
    body = obj["Body"]
    try:
        return bytes(body.read())
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


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
        if isinstance(event, Terminus2ArtifactRefEvent)
    ]
    indexed = _index_artifacts(trial)
    resolved: dict[str, bytes] = {}

    def _resolve_ref(
        ref: Terminus2ArtifactRefEvent,
        *,
        archive_path: str,
        key_suffix: str,
        canonical_path: str,
    ) -> None:
        candidates = [
            item
            for item in indexed
            if isinstance(item.get("key"), str)
            and (
                _artifact_key_matches(item["key"], suffix=key_suffix)
                or (
                    item.get("relative_path") == canonical_path
                    and item["key"].endswith("/files/" + canonical_path)
                )
            )
        ]
        if not candidates:
            raise Tb2V2ExportError(
                "missing_native_artifact",
                {
                    "message": (
                        "terminus2_artifact_ref is present but no matching "
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
        )
        actual_hash = hashlib.sha256(data).hexdigest()
        expected_hash = _normalize_hash(ref.content_hash)
        if actual_hash != expected_hash:
            raise Tb2V2ExportError(
                "missing_native_artifact",
                {
                    "message": "native Harbor artifact hash does not match artifact ref",
                    "trial_id": str(trial.id),
                    "artifact_kind": ref.artifact_kind,
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                },
            )
        resolved[archive_path] = data

    for ref in refs:
        if ref.artifact_kind == "terminus_2.pane":
            _resolve_ref(
                ref,
                archive_path=NATIVE_HARBOR_TRAJECTORY,
                key_suffix=".loom/agent/trajectory.json",
                canonical_path="artifacts/harbor/trajectory.json",
            )
        elif ref.artifact_kind == "recording.cast":
            _resolve_ref(
                ref,
                archive_path=NATIVE_RECORDING_CAST,
                key_suffix="recording.cast",
                canonical_path="artifacts/harbor/recording.cast",
            )

    return resolved


@dataclass(frozen=True)
class VerifierDeliveryArtifact:
    """One native or legacy verifier file packed into a raw-harbor delivery bundle."""

    archive_path: str
    data: bytes
    content_hash: str
    size_bytes: int
    truncated: bool | None
    share_status: str | None
    blocked_reason: str | None
    step_name: str | None
    source_key: str


_VERIFIER_KEY_MARKER = "/.loom/verifier/"
MAX_VERIFIER_LOG_BYTES = 1_048_576
MAX_VERIFIER_META_BYTES = 65_536
MAX_VERIFIER_OUTPUT_BYTES = 1_048_576
MAX_VERIFIER_JUNIT_BYTES = 4_194_304
MAX_VERIFIER_ARTIFACT_FILES = 16
_SHA256_RE = re.compile(r"sha256:([0-9a-f]{64})")
_VERIFIER_LOG_NAMES = frozenset(
    {"script.log", "pytest.log", "pytest-install.log"}
)
_VERIFIER_CANONICAL_LIMITS = {
    "output.json": MAX_VERIFIER_OUTPUT_BYTES,
    "junit.xml": MAX_VERIFIER_JUNIT_BYTES,
}


def _verifier_index_fields(
    trial: Trial,
    item: dict[str, Any],
    *,
    artifacts_bucket: str,
) -> tuple[str, str, str, int, str]:
    key = item.get("key")
    step_name = item.get("step_name")
    if (
        not isinstance(key, str)
        or not isinstance(step_name, str)
        or not step_name
        or step_name in {".", ".."}
        or "/" in step_name
    ):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {"message": "verifier artifact key and step_name are required"},
        )
    expected_prefix = f"{trial.team_id}/{trial.id}/{step_name}/.loom/verifier/"
    if not key.startswith(expected_prefix):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {
                "message": "verifier artifact key is not canonical for the trial",
                "key": key,
                "expected_prefix": expected_prefix,
            },
        )
    bucket = _resolve_indexed_artifact_bucket(
        item,
        default_bucket=artifacts_bucket,
    )
    if not bucket:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {
                "message": "verifier artifact bucket is required",
                "key": key,
            },
        )
    if item.get("share_status") != "shared":
        raise Tb2V2ExportError(
            "verifier_artifact_blocked",
            {
                "message": "verifier audit artifact is not approved for sharing",
                "key": key,
                "share_status": item.get("share_status"),
                "blocked_reason": item.get("blocked_reason"),
            },
        )
    size = item.get("size")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {
                "message": "verifier artifact size must be a non-negative integer",
                "key": key,
            },
        )
    content_hash = item.get("content_hash")
    if not isinstance(content_hash, str):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {"message": "verifier artifact content_hash is required", "key": key},
        )
    match = _SHA256_RE.fullmatch(content_hash.strip().lower())
    if match is None:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {
                "message": "verifier artifact content_hash must be sha256:<64 hex>",
                "key": key,
            },
        )
    rel = key.removeprefix(expected_prefix)
    if not rel or rel.startswith("/") or ".." in rel.split("/"):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_index",
            {"message": "verifier artifact relative path is unsafe", "key": key},
        )
    return key, bucket, rel, size, match.group(1)


def _fetch_bounded_verifier_artifact(
    client: Any,
    *,
    bucket: str,
    key: str,
    indexed_size: int,
    max_bytes: int,
) -> bytes:
    if indexed_size > max_bytes:
        raise Tb2V2ExportError(
            "verifier_artifact_too_large",
            {
                "message": "indexed verifier artifact exceeds its size limit",
                "key": key,
                "max_bytes": max_bytes,
            },
        )
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        raise Tb2V2ExportError(
            "missing_verifier_artifact",
            {
                "message": "verifier audit artifact is missing from object storage",
                "bucket": bucket,
                "key": key,
                "error_code": code,
            },
        ) from exc
    body = obj["Body"]
    try:
        content_length = obj.get("ContentLength")
        if content_length is not None and content_length != indexed_size:
            raise Tb2V2ExportError(
                "verifier_artifact_size_mismatch",
                {
                    "message": "object ContentLength does not match indexed size",
                    "key": key,
                    "indexed_size": indexed_size,
                    "content_length": content_length,
                },
            )
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = bytes(body.read(remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise Tb2V2ExportError(
                "verifier_artifact_too_large",
                {
                    "message": "verifier artifact exceeds its runtime size limit",
                    "key": key,
                    "max_bytes": max_bytes,
                },
            )
        if len(data) != indexed_size:
            raise Tb2V2ExportError(
                "verifier_artifact_size_mismatch",
                {
                    "message": "verifier artifact body does not match indexed size",
                    "key": key,
                    "indexed_size": indexed_size,
                    "actual_size": len(data),
                },
            )
        return data
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()


def _validate_verifier_meta(
    *,
    data: bytes,
    log_rel: str,
    log_size: int,
) -> dict[str, Any]:
    try:
        parsed = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {"message": "verifier metadata must be valid UTF-8 JSON", "path": log_rel},
        ) from exc
    if not isinstance(parsed, dict) or parsed.get("schema_version") != "1":
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {
                "message": "verifier metadata must be a schema_version 1 object",
                "path": log_rel,
            },
        )
    truncated = parsed.get("truncated")
    original_bytes = parsed.get("original_bytes")
    kept_bytes = parsed.get("kept_bytes")
    if not isinstance(truncated, bool):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {"message": "verifier metadata truncated must be boolean", "path": log_rel},
        )
    for name, value in (("original_bytes", original_bytes), ("kept_bytes", kept_bytes)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise Tb2V2ExportError(
                "invalid_verifier_artifact_metadata",
                {
                    "message": f"verifier metadata {name} must be a non-negative integer",
                    "path": log_rel,
                },
            )
    return_code = parsed.get("return_code")
    if isinstance(return_code, bool) or not isinstance(return_code, int):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {"message": "verifier metadata return_code must be an integer", "path": log_rel},
        )
    script_path = parsed.get("script_path")
    if not isinstance(script_path, str) or not script_path:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {
                "message": "verifier metadata script_path must be a non-empty string",
                "path": log_rel,
            },
        )
    assert isinstance(original_bytes, int) and isinstance(kept_bytes, int)
    if kept_bytes != log_size:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {
                "message": "metadata kept_bytes does not match the paired log",
                "path": log_rel,
            },
        )
    raw_driver_truncated = parsed.get("driver_truncated", False)
    if not isinstance(raw_driver_truncated, bool):
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {
                "message": "verifier metadata driver_truncated must be boolean",
                "path": log_rel,
            },
        )
    driver_truncated = raw_driver_truncated
    valid_lengths = (
        (not truncated and original_bytes == kept_bytes)
        or (truncated and original_bytes > kept_bytes)
        or (truncated and driver_truncated and original_bytes >= kept_bytes)
    )
    if not valid_lengths:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {
                "message": "metadata truncation flags and byte counts are inconsistent",
                "path": log_rel,
            },
        )
    expected_log_path = f".loom/verifier/{log_rel}"
    if parsed.get("log_path") != expected_log_path:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_metadata",
            {
                "message": "metadata log_path does not match the paired log",
                "path": log_rel,
                "expected_log_path": expected_log_path,
            },
        )
    return parsed


def resolve_verifier_artifacts(
    trial: Trial,
    *,
    client: Any,
    artifacts_bucket: str,
) -> list[VerifierDeliveryArtifact]:
    """Fetch native phase evidence or legacy indexed verifier files for delivery.

    Fail-closed when indexed verifier artifacts are missing, unreadable,
    hash-mismatched, share-blocked, or contain secret-like content.

    Terminus-2 trials with ``trial.config.skip_verifier`` may export without
    indexed verifier audit artifacts; agent-only trajectories still pack.
    """
    indexed = _index_artifacts(trial)
    if any("relative_path" in item for item in indexed) and not _trial_skipped_verifier(trial):
        from loom_service.delivery_export_native_verifier import resolve_native_verifier_artifacts

        return resolve_native_verifier_artifacts(
            trial, indexed=indexed, client=client, artifacts_bucket=artifacts_bucket,
        )
    candidates = [
        item
        for item in indexed
        if isinstance(item.get("key"), str) and _VERIFIER_KEY_MARKER in str(item["key"])
    ]
    if not candidates:
        if _agent_name_for_trial(trial) != "terminus-2" or _trial_skipped_verifier(trial):
            return []
        raise Tb2V2ExportError(
            "missing_verifier_artifact",
            {
                "message": "eligible trial has no indexed verifier audit artifacts",
                "trial_id": str(trial.id),
            },
        )
    if len(candidates) > MAX_VERIFIER_ARTIFACT_FILES:
        raise Tb2V2ExportError(
            "verifier_artifact_too_large",
            {"message": "too many indexed verifier audit files", "trial_id": str(trial.id)},
        )

    fetched: dict[tuple[str, str], tuple[dict[str, Any], bytes, str]] = {}
    for item in sorted(candidates, key=lambda row: str(row.get("key"))):
        key, bucket, rel, size, expected_hash = _verifier_index_fields(
            trial,
            item,
            artifacts_bucket=artifacts_bucket,
        )
        step_name = str(item["step_name"])
        identity = (step_name, rel)
        if identity in fetched:
            raise Tb2V2ExportError(
                "duplicate_verifier_artifact",
                {
                    "message": "multiple source keys map to the same verifier archive path",
                    "path": rel,
                    "step_name": step_name,
                },
            )
        if "/" in rel:
            raise Tb2V2ExportError(
                "invalid_verifier_artifact_pair",
                {"message": "verifier artifact names must be canonical leaves", "path": rel},
            )
        if rel in _VERIFIER_LOG_NAMES:
            max_bytes = MAX_VERIFIER_LOG_BYTES
        elif rel.endswith(".meta.json") and rel.removesuffix(
            ".meta.json"
        ) in _VERIFIER_LOG_NAMES:
            max_bytes = MAX_VERIFIER_META_BYTES
        elif rel in _VERIFIER_CANONICAL_LIMITS:
            max_bytes = _VERIFIER_CANONICAL_LIMITS[rel]
        else:
            raise Tb2V2ExportError(
                "invalid_verifier_artifact_pair",
                {
                    "message": "verifier artifact name is not in the canonical allowlist",
                    "path": rel,
                },
            )
        data = _fetch_bounded_verifier_artifact(
            client,
            bucket=bucket,
            key=key,
            indexed_size=size,
            max_bytes=max_bytes,
        )
        actual_hash = hashlib.sha256(data).hexdigest()
        if actual_hash != expected_hash:
            raise Tb2V2ExportError(
                "verifier_artifact_hash_mismatch",
                {
                    "message": "verifier audit artifact hash does not match index",
                    "trial_id": str(trial.id),
                    "key": key,
                    "expected_hash": expected_hash,
                    "actual_hash": actual_hash,
                },
            )
        fetched[identity] = (item, data, actual_hash)

    step_names = {step_name for step_name, _rel in fetched}
    multiple_steps = len(step_names) > 1
    log_keys = {key for key in fetched if key[1] in _VERIFIER_LOG_NAMES}
    meta_keys = {
        key
        for key in fetched
        if key[1].endswith(".meta.json")
        and key[1].removesuffix(".meta.json") in _VERIFIER_LOG_NAMES
    }
    expected_meta_keys = {
        (step_name, f"{rel}.meta.json") for step_name, rel in log_keys
    }
    if meta_keys != expected_meta_keys:
        raise Tb2V2ExportError(
            "invalid_verifier_artifact_pair",
            {
                "message": "verifier audit logs and metadata must be complete pairs",
                "logs": sorted(f"{step}/{rel}" for step, rel in log_keys),
                "metadata": sorted(f"{step}/{rel}" for step, rel in meta_keys),
            },
        )
    if (
        _agent_name_for_trial(trial) == "terminus-2"
        and not log_keys
        and not _trial_skipped_verifier(trial)
    ):
        raise Tb2V2ExportError(
            "missing_verifier_artifact",
            {
                "message": "eligible trial has no complete verifier audit log pair",
                "trial_id": str(trial.id),
            },
        )
    if _trial_skipped_verifier(trial) and not log_keys:
        return []

    resolved: list[VerifierDeliveryArtifact] = []
    bodies_for_scan: dict[str, bytes] = {}
    for step_name, log_rel in sorted(log_keys):
        log_item, log_data, log_hash = fetched[(step_name, log_rel)]
        meta_rel = f"{log_rel}.meta.json"
        meta_item, meta_data, meta_hash = fetched[(step_name, meta_rel)]
        meta = _validate_verifier_meta(
            data=meta_data,
            log_rel=log_rel,
            log_size=len(log_data),
        )
        for rel, item, data, digest in (
            (log_rel, log_item, log_data, log_hash),
            (meta_rel, meta_item, meta_data, meta_hash),
        ):
            archive_path = (
                f"verifier/{step_name}/{rel}" if multiple_steps else f"verifier/{rel}"
            )
            bodies_for_scan[archive_path] = data
            resolved.append(
                VerifierDeliveryArtifact(
                    archive_path=archive_path,
                    data=data,
                    content_hash=f"sha256:{digest}",
                    size_bytes=len(data),
                    truncated=bool(meta["truncated"]),
                    share_status="shared",
                    blocked_reason=None,
                    step_name=str(item["step_name"]),
                    source_key=str(item["key"]),
                )
            )

    canonical_keys = {
        key for key in fetched if key[1] in _VERIFIER_CANONICAL_LIMITS
    }
    for step_name, rel in sorted(canonical_keys):
        item, data, digest = fetched[(step_name, rel)]
        archive_path = (
            f"verifier/{step_name}/{rel}" if multiple_steps else f"verifier/{rel}"
        )
        bodies_for_scan[archive_path] = data
        resolved.append(
            VerifierDeliveryArtifact(
                archive_path=archive_path,
                data=data,
                content_hash=f"sha256:{digest}",
                size_bytes=len(data),
                truncated=None,
                share_status="shared",
                blocked_reason=None,
                step_name=step_name,
                source_key=str(item["key"]),
            )
        )

    scan_members_for_secrets(bodies_for_scan)
    return resolved


def scan_members_for_secrets(members: dict[str, bytes]) -> None:
    for path, data in members.items():
        text = data.decode("utf-8", errors="replace")
        for pattern in SECRET_PATTERNS:
            if pattern.search(text):
                raise Tb2V2ExportError(
                    "secret_scan_failed",
                    {
                        "message": "export payload matched a secret pattern",
                        "path": path,
                    },
                )


def scan_execution_trajectory_for_private_path_keystrokes(
    execution_trajectory: dict[str, Any],
    *,
    trial_id: str,
) -> None:
    """Fail closed when agent keystrokes reference Harbor private paths (#1263)."""
    steps = execution_trajectory.get("steps")
    if not isinstance(steps, list):
        return
    matches: list[dict[str, str]] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        source = step.get("source")
        tool_calls = step.get("tool_calls")
        if not isinstance(tool_calls, list):
            continue
        # Packed ATIF agent turns use source=agent; also scan when source is
        # absent but tool_calls exist (defensive for older projections).
        if source not in (None, "agent"):
            continue
        step_id = str(step.get("step_id") or "")
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            arguments = tool_call.get("arguments")
            if not isinstance(arguments, dict):
                continue
            keystrokes = arguments.get("keystrokes")
            if not isinstance(keystrokes, str) or not keystrokes:
                continue
            found = PRIVATE_PATH_KEYSTROKE_RE.search(keystrokes)
            if found is None:
                continue
            matched_path = (found.group("abs_path") or found.group("rel_path") or "").lower()
            if not matched_path:
                continue
            excerpt = keystrokes.strip().replace("\n", "\\n")
            if len(excerpt) > _KEYSTROKES_EXCERPT_LEN:
                excerpt = excerpt[:_KEYSTROKES_EXCERPT_LEN] + "…"
            matches.append(
                {
                    "step_id": step_id,
                    "tool_call_id": str(tool_call.get("tool_call_id") or ""),
                    "matched_path": matched_path,
                    "keystrokes_excerpt": excerpt,
                }
            )
            if len(matches) >= _PRIVATE_PATH_MATCH_CAP:
                break
        if len(matches) >= _PRIVATE_PATH_MATCH_CAP:
            break
    if not matches:
        return
    raise Tb2V2ExportError(
        "forbidden_path_keystrokes",
        {
            "message": "agent tool_calls keystrokes reference a private Harbor path",
            "trial_id": trial_id,
            "forbidden_paths": ["/app/solution", "/app/tests", "/app/verifier"],
            "matches": matches,
        },
    )


def build_per_trial_v2_bundle(
    *,
    trial: Trial,
    events: list[TrajectoryEvent],
    calls: list[LlmCall],
    client: Any,
    artifacts_bucket: str,
    messages_from_raw_log: Any,
    agent_version: str = "1.0",
) -> Tb2V2TrialBundle:
    validate_v2_eligibility(events, trial)
    join_errors = validate_v2_joins(events)
    if join_errors:
        raise Tb2V2ExportError(
            "join_validation_failed",
            {
                "message": "typed terminus2 turn joins failed validation",
                "trial_id": str(trial.id),
                "errors": join_errors,
            },
        )

    # Full event list stays the audit spine; delivery projections match native
    # Harbor's last-attempt-only overwrite after gateway retries.
    delivery_events = events_after_last_agent_retry(events)

    agent_name = _agent_name_for_trial(trial)
    execution_trajectory = build_execution_trajectory(
        delivery_events,
        task_id=trial.task_id,
        agent_name=agent_name,
        agent_version=agent_version,
    )
    model_input_trajectory = build_model_input_trajectory(
        trial_id=str(trial.id),
        task_id=trial.task_id,
        calls=calls,
        messages_from_raw_log=messages_from_raw_log,
    )
    terminal_transcript = build_terminal_transcript(delivery_events)
    export_provenance = build_export_provenance(
        events,
        trial_id=str(trial.id),
        join_errors=join_errors,
    )
    native_artifacts = resolve_native_artifacts(
        trial,
        events,
        client=client,
        artifacts_bucket=artifacts_bucket,
    )
    execution_trajectory = enrich_execution_trajectory(
        execution_trajectory,
        native_bytes=native_artifacts.get(NATIVE_HARBOR_TRAJECTORY),
        reasoning_by_gateway=reasoning_by_gateway_from_calls(
            calls,
            messages_from_raw_log=messages_from_raw_log,
        ),
    )
    # #1263: reject contaminated agent runs before secret scan / tar write.
    scan_execution_trajectory_for_private_path_keystrokes(
        execution_trajectory,
        trial_id=str(trial.id),
    )

    artifact_manifest_entries = [
        {"kind": "execution_trajectory", "path": "trajectory.json"},
        {"kind": "model_input_trajectory", "path": "model_input_trajectory.json"},
        {"kind": "terminal_transcript", "path": "terminal_transcript.jsonl"},
        {"kind": "audit_spine", "path": "loom_trajectory.jsonl"},
        {"kind": "export_provenance", "path": "export_provenance.json"},
    ]
    if NATIVE_HARBOR_TRAJECTORY in native_artifacts:
        artifact_manifest_entries.append(
            {"kind": "native_harbor_trajectory", "path": NATIVE_HARBOR_TRAJECTORY},
        )
    if NATIVE_RECORDING_CAST in native_artifacts:
        artifact_manifest_entries.append(
            {"kind": "native_recording_cast", "path": NATIVE_RECORDING_CAST},
        )

    scan_payload = {
        "trajectory.json": json.dumps(execution_trajectory, ensure_ascii=False).encode(),
        "model_input_trajectory.json": json.dumps(
            model_input_trajectory,
            ensure_ascii=False,
        ).encode(),
        "terminal_transcript.jsonl": terminal_transcript,
        "export_provenance.json": json.dumps(export_provenance, ensure_ascii=False).encode(),
        **native_artifacts,
    }
    scan_members_for_secrets(scan_payload)

    return Tb2V2TrialBundle(
        execution_trajectory=execution_trajectory,
        model_input_trajectory=model_input_trajectory,
        terminal_transcript=terminal_transcript,
        export_provenance=export_provenance,
        native_artifacts=native_artifacts,
        artifact_manifest_entries=artifact_manifest_entries,
    )
