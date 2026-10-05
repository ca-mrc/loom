"""Converge committed Terminus accounting on the bound Gateway ledger (#1962).

Run in the Control Plane environment for one explicit lease UUID. No execution,
source commit, outcome, or retention state changes; original objects remain.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import logging
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.data_lifecycle_registry import register_lifecycle_object
from loom.db.schema import Artifact, ServiceExecutionLease, Trial, TrialEvent
from loom.execution_runtime_contract import ExecutionRuntimeResultV1
from loom.llm_call_ledger import read_service_execution_llm_calls
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.service_execution_terminus_trace import terminus_usage
from loom.trajectory.storage import MinioObjectStore, ObjectStore
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.service_execution_materializer import (
    build_canonical_atif,
    build_canonical_events,
    read_exception_info,
)
from loom_control_plane.service_execution_task_snapshot import (
    resolve_service_execution_task_snapshot,
)

_LOGGER = logging.getLogger(__name__)
_SOURCE = "gateway_lease_ledger"
_PATHS = ("trajectory/events.jsonl", "accounting/usage.json")
_MAX_INPUT = 16 * 1024 * 1024


def _body(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode()


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


async def _read_file(store: ObjectStore, record: dict[str, Any], bucket: str) -> bytes:
    if record.get("bucket") != bucket or not isinstance(record.get("key"), str):
        raise ValueError("canonical file storage identity invalid")
    size = record.get("size_bytes")
    if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= _MAX_INPUT:
        raise ValueError("canonical derivation input too large")
    body = bytearray()
    async for chunk in store.stream_object(bucket=bucket, key=record["key"]):
        body.extend(chunk)
        if len(body) > size:
            raise ValueError("canonical file size mismatch")
    if len(body) != size or _digest(bytes(body)) != str(record.get("sha256", "")).removeprefix("sha256:"):
        raise ValueError("canonical file content mismatch")
    return bytes(body)


def _guard(lease: ServiceExecutionLease, trial: Trial, artifact: Artifact, team_id: UUID) -> None:
    if (
        lease.team_id != team_id or trial.team_id != team_id or artifact.team_id != team_id
        or artifact.trial_id != trial.id or lease.trial_id != trial.id
        or artifact.control_producer_kind != "service_execution"
        or artifact.control_producer_id != lease.id
        or lease.materialization_state != "committed" or lease.output_commit_state != "committed"
        or trial.state not in {"succeeded", "failed", "cancelled"}
        or trial.attempt_count != lease.attempt
        or artifact.lifecycle_authority_id is None or lease.output_generation is None
    ):
        raise ValueError("repair requires the selected team's committed current terminal attempt")


async def repair_accounting(
    *, session_factory: async_sessionmaker[AsyncSession], store: ObjectStore,
    artifacts_bucket: str, trajectories_bucket: str, lease_id: UUID, team_id: UUID,
    apply: bool = False,
) -> dict[str, Any]:
    """Publish a new derived revision, fenced against concurrent pointer/ledger changes."""
    async with session_factory() as session:
        lease = await session.get(ServiceExecutionLease, lease_id)
        if lease is None:
            raise ValueError("lease not found")
        trial = await session.get(Trial, lease.trial_id)
        artifact = await session.scalar(select(Artifact).where(
            Artifact.control_producer_kind == "service_execution", Artifact.control_producer_id == lease.id,
        ))
        if trial is None or artifact is None:
            raise ValueError("canonical trial identity missing")
        _guard(lease, trial, artifact, team_id)
        metadata = copy.deepcopy(artifact.artifact_metadata or {})
        task = await resolve_service_execution_task_snapshot(session, lease=lease, trial=trial)
        trial_config = TrialConfig.model_validate(trial.config)
        if trial_config.agent_name != "terminus-2":
            raise ValueError("accounting repair only supports Terminus-2")
        task_config = TaskConfig.model_validate(task.config)
        storage = copy.deepcopy(artifact.storage)
        old_index = copy.deepcopy(trial.trajectory_index)
        outcome = copy.deepcopy((trial.state, trial.finished_at, trial.result, trial.config))
        output_generation = lease.output_generation
        rows = await read_service_execution_llm_calls(session, lease, generation=output_generation)
        existing = list((await session.scalars(select(TrialEvent).where(
            TrialEvent.trial_id == trial.id,
        ).order_by(TrialEvent.seq))).all())
        if not existing or any(row.source != "service-execution-materializer" for row in existing):
            raise ValueError("repair refuses foreign or absent durable trajectory events")
        old_events = [(row.seq, row.kind, row.payload, row.source, row.lifecycle_authority_id) for row in existing]
        event_authority = existing[0].lifecycle_authority_id
        if event_authority is None or any(row.lifecycle_authority_id != event_authority for row in existing):
            raise ValueError("trajectory lifecycle authority mismatch")
        artifact_id, trial_id, task_id = artifact.id, trial.id, trial.task_id
        authority_id, created_at = artifact.lifecycle_authority_id, artifact.created_at
        attempt = lease.attempt

    if not isinstance(storage, dict) or storage.get("schema_version") != "loom.canonical-trial-bundle-storage.v1":
        raise ValueError("canonical bundle storage unavailable")
    if not isinstance(old_index, dict) or authority_id is None:
        raise ValueError("canonical trajectory index or lifecycle authority missing")
    records = {item["relative_path"]: item for item in storage["files"]}
    sources = {item["relative_path"]: item for item in storage.get("source_evidence", [])}
    if len(records) != len(storage["files"]) or len(sources) != len(storage.get("source_evidence", [])):
        raise ValueError("duplicate canonical bundle paths")
    inputs = {path: await _read_file(store, record, artifacts_bucket)
              for path, record in records.items()
              if path in {*_PATHS, "result.json", "verifier/output.json", "accounting/gateway-calls.json",
                          "diagnostics/agent-exception.json", "diagnostics/verifier-exception.json"}}
    runtime_result = ExecutionRuntimeResultV1.model_validate_json(inputs["result.json"])
    if "verifier/output.json" not in inputs and (
        outcome[0] == "succeeded" or runtime_result.status == "succeeded"
    ):
        raise ValueError("successful runtime requires verifier output")
    # A canonical trace contains lifecycle/accounting events and is not a native
    # Harbor input. Subsequent revisions always start from immutable evidence.
    # Failed executions may have produced no native trace at all.
    source_trace = sources.get("source/trajectory/events.jsonl")
    trace_body = (
        await _read_file(store, source_trace, artifacts_bucket) if source_trace is not None
        else None if metadata.get("accounting_source") == _SOURCE
        else inputs.get("trajectory/events.jsonl")
    )
    if runtime_result.status == "succeeded" and trace_body is None:
        raise ValueError("successful runtime requires immutable native trace")
    events = build_canonical_events(
        trial_id=trial_id, task_id=task_id, task_config=task_config, trial_config=trial_config,
        runtime_result=runtime_result,
        trace_body=trace_body, verifier_body=inputs.get("verifier/output.json"),
        gateway_calls=rows,
        exception_info=read_exception_info(runtime_result, inputs),
    )
    events_body = b"".join(event.model_dump_json().encode() + b"\n" for event in events)
    usage = terminus_usage(list(events), trial_config)
    atif_body = build_canonical_atif(
        events, task_id=task_id, agent_name=trial_config.agent_name,
        agent_version=trial_config.agent_version or task_config.agent.version or "service-execution-v1",
    )
    ledger = {"schema_version": "loom.gateway-lease-ledger.v1", "calls": rows}
    event_rows = [(event.seq, event.kind.value, event.model_dump(mode="json"),
                   "service-execution-materializer", event_authority) for event in events]
    matches = (
        [json.loads(line) for line in inputs.get("trajectory/events.jsonl", b"").splitlines()]
        == [event.model_dump(mode="json") for event in events]
        and json.loads(inputs.get("accounting/usage.json", b"null")) == usage
        and json.loads(inputs.get("accounting/gateway-calls.json", b"null")) == ledger
        and old_events == event_rows
        and old_index.get("trajectory_sha256")
        == str(records.get("trajectory/events.jsonl", {}).get("sha256", "")).removeprefix("sha256:")
        and old_index.get("atif_sha256") == _digest(atif_body)
    )
    report = {"status": "already_corrected" if matches else "prepared",
              "trial_id": str(trial_id), "lease_id": str(lease_id), "usage": usage,
              "gateway_call_count": len(rows)}
    if not apply:
        return report

    async def locked_inputs(session: AsyncSession) -> tuple[Trial, Artifact]:
        current_lease = await session.get(ServiceExecutionLease, lease_id, with_for_update=True)
        current_trial = await session.get(Trial, trial_id, with_for_update=True)
        current_artifact = await session.get(Artifact, artifact_id, with_for_update=True)
        if current_lease is None or current_trial is None or current_artifact is None:
            raise ValueError("repair identity disappeared")
        _guard(current_lease, current_trial, current_artifact, team_id)
        current_events = list((await session.scalars(select(TrialEvent).where(
            TrialEvent.trial_id == trial_id,
        ).order_by(TrialEvent.seq).with_for_update())).all())
        if (current_artifact.storage != storage or current_trial.trajectory_index != old_index
                or (current_artifact.artifact_metadata or {}) != metadata
                or [(row.seq, row.kind, row.payload, row.source, row.lifecycle_authority_id)
                    for row in current_events] != old_events
                or current_lease.output_generation != output_generation
                or (current_trial.state, current_trial.finished_at,
                    current_trial.result, current_trial.config) != outcome
                or await read_service_execution_llm_calls(
                    session, current_lease, generation=output_generation) != rows):
            raise ValueError("repair input changed; canonical pointers were not updated")
        return current_trial, current_artifact

    if matches:
        if metadata.get("accounting_call_count") != len(rows):
            async with session_factory() as session:
                _, current_artifact = await locked_inputs(session)
                current_artifact.artifact_metadata = {**metadata, "accounting_call_count": len(rows)}
                await session.commit()
        return report

    revision = str(uuid4())
    prefix = f"trials/{team_id}/{trial_id}/attempts/{attempt}/bundles/{artifact_id}/accounting-v2/{revision}/"
    trajectory_prefix = f"{team_id}/{trial_id}/attempts/{attempt}/accounting-v2/{revision}/"
    new_storage = copy.deepcopy(storage)
    objects: list[dict[str, Any]] = []

    async def write(path: str, body: bytes, bucket: str, key: str) -> dict[str, Any]:
        record: dict[str, Any] = {"relative_path": path, "media_type": "application/jsonl" if path.endswith("jsonl") else "application/json",
                  "size_bytes": len(body), "sha256": "sha256:" + _digest(body), "bucket": bucket, "key": key}
        objects.append(record)
        receipt = await store.put_object_with_metadata(bucket=bucket, key=key, body=body)
        record["version_id"] = receipt.version_id
        readback = await store.stat_object(bucket=bucket, key=key)
        if readback.content_length != len(body) or (
            readback.checksum_sha256 is not None and readback.checksum_sha256 != record["sha256"]
        ):
            raise ValueError("corrected object readback mismatch")
        return record

    commit_attempted = False
    try:
        replacements = {}
        for path, body in (
            ("trajectory/events.jsonl", events_body), ("accounting/usage.json", _body(usage)),
            ("accounting/gateway-calls.json", _body(ledger)),
        ):
            replacements[path] = await write(path, body, artifacts_bucket, prefix + "files/" + path)
        new_storage["files"] = [replacements.pop(item["relative_path"], item) for item in storage["files"]]
        new_storage["files"].extend(replacements.values())
        for path in _PATHS:
            # Preserve only the original runtime evidence once, never a prior
            # canonical revision. Missing source on failed runs remains missing.
            if "source/" + path not in sources and path in records and metadata.get("accounting_source") != _SOURCE:
                new_storage.setdefault("source_evidence", []).append({**records[path], "relative_path": "source/" + path})
        trajectory = await write("events.jsonl", events_body, trajectories_bucket, trajectory_prefix + "events.jsonl")
        atif = await write("atif.json", atif_body, trajectories_bucket, trajectory_prefix + "atif.json")
        new_index = copy.deepcopy(old_index)
        for name, record in (("trajectory", trajectory), ("atif", atif)):
            new_index.update({f"{name}_uri": f"s3://{record['bucket']}/{record['key']}",
                              f"{name}_sha256": record["sha256"].removeprefix("sha256:"),
                              f"{name}_size_bytes": record["size_bytes"], f"{name}_version_id": record["version_id"]})
        new_index["artifacts"] = new_storage["files"]
        new_index["atif_schema_version"] = json.loads(atif_body)["schema_version"]

        async with session_factory() as session:
            trial, artifact = await locked_inputs(session)
            for record in objects:
                await register_lifecycle_object(
                    session, authority_id=authority_id, bucket=record["bucket"], object_key=record["key"],
                    version_id=record["version_id"], content_sha256=record["sha256"].removeprefix("sha256:"),
                    size_bytes=record["size_bytes"], created_at=created_at,
                )
            await session.execute(delete(TrialEvent).where(TrialEvent.trial_id == trial_id))
            for event in events:
                session.add(TrialEvent(
                    trial_id=trial_id, seq=event.seq, kind=event.kind.value,
                    source="service-execution-materializer", schema_version=1,
                    payload=event.model_dump(mode="json"), lifecycle_authority_id=event_authority,
                ))
            artifact.storage = new_storage
            artifact.artifact_metadata = {**metadata, "accounting_source": _SOURCE,
                                          "accounting_repair_id": revision,
                                          "accounting_call_count": len(rows),
                                          "accounting_previous_trajectory_index": old_index}
            trial.trajectory_index = new_index
            # The lease retains its immutable original materialization ACK.
            # Corrected object identities belong to the new canonical pointers.
            await session.flush()
            commit_attempted = True
            await session.commit()
        return {**report, "status": "corrected", "repair_id": revision}
    except BaseException:
        # A connection loss during COMMIT can be ambiguous. Retain the unique
        # revision then; deleting it could destroy an already-published pointer.
        if not commit_attempted:
            cleanup = await asyncio.gather(*(
                store.delete_object(bucket=record["bucket"], key=record["key"])
                for record in objects
            ), return_exceptions=True)
            if any(isinstance(result, BaseException) for result in cleanup):
                _LOGGER.warning("accounting repair retained unpublished revision %s after cleanup failure", revision)
        raise



async def _main(args: argparse.Namespace) -> None:
    settings = ControlPlaneSettings()
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    store = MinioObjectStore(
        endpoint_url=settings.minio_endpoint, access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(), region=settings.minio_region,
    )
    try:
        print(json.dumps(await repair_accounting(
            session_factory=async_sessionmaker(engine, expire_on_commit=False), store=store,
            artifacts_bucket=settings.artifacts_bucket, trajectories_bucket=settings.trajectories_bucket,
            lease_id=args.lease_id, team_id=args.team_id, apply=args.apply,
        )))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lease-id", required=True, type=UUID)
    parser.add_argument("--team-id", required=True, type=UUID)
    parser.add_argument("--apply", action="store_true", help="publish the correction; default only prepares totals")
    asyncio.run(_main(parser.parse_args()))
