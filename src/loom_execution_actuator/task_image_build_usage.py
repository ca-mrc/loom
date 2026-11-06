"""Trusted node-observer samples of native build Pods (#2750).

The build controller calls ``BuildUsageSampler.sample`` after an attempt's
lifecycle transaction has closed. It reads the node's builder snapshot (same
producer identity, freshness and field validation as Trial observations),
projects only this attempt's Pod by namespace and UID, and updates one row in
``task_image_build_resource_usage``. It never touches ``native_build`` or any
lifecycle state, and its failures are recorded as unavailable, never zero.

Storage is complete-or-unknown per snapshot: when the trusted helper could not
produce a Pod total (entry cap, churn, mount or deadline), the snapshot counts
as storage-unknown with the helper's reason codes and leaves maxima untouched.
"""
from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import TaskImageBuildResourceUsage, TaskImageMaterializationAttempt
from loom_execution_actuator.resource_diagnostics import resource_sample_diagnostic

SOURCE = "native_node_observer"
CONTAINERS = ("prepare", "build", "publish")
_CODE = re.compile(r"[a-z_]{1,64}")
_MAX_CODES = 16
_CACHE_SECONDS = 15.0
_MAXIMA = {"cpu_nanocores": "cpu_max_nanocores", "memory_bytes": "memory_max_bytes",
           "filesystem_bytes": "filesystem_max_bytes"}


@dataclass(frozen=True)
class BuildSample:
    snapshot_at: datetime
    storage_bytes: int | None
    unknown_codes: tuple[str, ...]
    containers: dict[str, dict[str, int | None]] = field(default_factory=dict)


def _int(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value < 2**63 else None


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.utcoffset() is not None else None


def project_build_pod(summary: dict[str, Any], *, namespace: str, pod_uid: str) -> BuildSample | str:
    """This Pod's validated sample, or a fixed reason it is unavailable in this snapshot."""
    metadata = summary.get("_native_observation")
    snapshot_at = _time(metadata.get("observed_at")) if isinstance(metadata, dict) else None
    if snapshot_at is None:
        return "snapshot_time_unavailable"
    pod = next((row for row in summary.get("pods") or [] if isinstance(row, dict)
                and isinstance(row.get("podRef"), dict) and row["podRef"].get("uid") == pod_uid), None)
    if pod is None:
        return "pod_not_in_snapshot"
    if pod["podRef"].get("namespace") != namespace:
        return "pod_identity_mismatch"
    storage = pod.get("ephemeral-storage")
    total = _int(storage.get("usedBytes")) if isinstance(storage, dict) else None
    codes = pod.get("_diagnostic_codes")
    unknown = tuple(code for code in codes if isinstance(code, str) and _CODE.fullmatch(code))[:_MAX_CODES] \
        if isinstance(codes, list) else ()
    if total is None and not unknown:
        unknown = ("storage_unavailable",)
    containers: dict[str, dict[str, int | None]] = {}
    for row in pod.get("containers") or []:
        if not isinstance(row, dict) or row.get("name") not in CONTAINERS:
            continue
        rootfs = _int((row.get("rootfs") or {}).get("usedBytes"))
        logs = _int((row.get("logs") or {}).get("usedBytes"))
        containers[row["name"]] = {
            "cpu_nanocores": _int((row.get("cpu") or {}).get("usageNanoCores")),
            "memory_bytes": _int((row.get("memory") or {}).get("usageBytes")),
            # Only a same-snapshot pair is a container filesystem figure.
            "filesystem_bytes": rootfs + logs if rootfs is not None and logs is not None else None,
        }
    return BuildSample(snapshot_at=snapshot_at, storage_bytes=total,
                       unknown_codes=unknown if total is None else (), containers=containers)


def _merge_containers(current: dict[str, Any], sample: BuildSample) -> dict[str, Any]:
    merged = {name: dict(values) for name, values in current.items() if name in CONTAINERS}
    for name, values in sample.containers.items():
        row = merged.setdefault(name, {"samples": 0})
        row["samples"] = int(row.get("samples", 0)) + 1
        for key, value in values.items():
            target = _MAXIMA[key]
            if value is not None and (row.get(target) is None or value > row[target]):
                row[target] = value
    return merged


async def record_build_sample(
    session: AsyncSession, *, attempt_id: UUID, materialization_id: UUID, lease_epoch: int,
    pod_uid: str, node_name: str, observation: BuildSample | str, now: datetime,
) -> None:
    """Upsert this attempt/Pod row; idempotent per producer snapshot."""
    await session.execute(insert(TaskImageBuildResourceUsage).values(
        attempt_id=attempt_id, materialization_id=materialization_id, lease_epoch=lease_epoch,
        pod_uid=pod_uid, node_name=node_name, source=SOURCE, first_observed_at=now, last_observed_at=now,
    ).on_conflict_do_nothing(constraint="uq_task_image_build_resource_usage_pod"))
    row = await session.scalar(select(TaskImageBuildResourceUsage).where(
        TaskImageBuildResourceUsage.attempt_id == attempt_id,
        TaskImageBuildResourceUsage.pod_uid == pod_uid,
    ).with_for_update())
    assert row is not None
    if row.node_name != node_name or row.lease_epoch != lease_epoch:
        return  # never merge a different Pod placement or epoch into this row
    if isinstance(observation, str):
        row.unavailable += 1
        row.last_unavailable_code = observation
        row.last_observed_at = now
        return
    if row.last_snapshot_at is not None and observation.snapshot_at <= row.last_snapshot_at:
        return  # the same (or an older) producer snapshot is counted once
    row.snapshots += 1
    row.last_snapshot_at = observation.snapshot_at
    row.last_observed_at = now
    if observation.storage_bytes is not None:
        row.storage_complete += 1
        if row.ephemeral_storage_max_bytes is None or observation.storage_bytes > row.ephemeral_storage_max_bytes:
            row.ephemeral_storage_max_bytes = observation.storage_bytes
            row.ephemeral_storage_max_at = observation.snapshot_at
    else:
        row.storage_unknown += 1
        codes = dict(row.unknown_codes or {})
        for code in observation.unknown_codes:
            if code in codes or len(codes) < _MAX_CODES:
                codes[code] = int(codes.get(code, 0)) + 1
        row.unknown_codes = codes
    row.containers = _merge_containers(row.containers or {}, observation)


class BuildUsageSampler:
    """Bounded, cached builder-snapshot reads; one row update per call."""

    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], namespace: str,
                 reader: Callable[[str], Awaitable[dict[str, Any]]]) -> None:
        self.sessions, self.namespace, self.reader = sessions, namespace, reader
        self._cache: dict[str, tuple[float, dict[str, Any] | None, str | None]] = {}

    async def _snapshot(self, node_name: str) -> tuple[dict[str, Any] | None, str | None]:
        now = time.monotonic()
        cached = self._cache.get(node_name)
        if cached is not None and now - cached[0] < _CACHE_SECONDS:
            return cached[1], cached[2]
        try:
            summary, code = await self.reader(node_name), None
        except Exception as error:  # a fixed diagnostic only; never a raw message
            summary, code = None, resource_sample_diagnostic(error)
        self._cache = {key: value for key, value in self._cache.items() if now - value[0] < 60}
        self._cache[node_name] = (now, summary, code)
        return summary, code

    async def sample(self, attempt_id: UUID) -> None:
        async with self.sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            native = (attempt.native_build or {}) if attempt is not None else {}
        pod_uid, node_name = native.get("pod_uid"), native.get("node_name")
        if (attempt is None or native.get("capacity_released_at") or not isinstance(pod_uid, str)
                or not isinstance(node_name, str) or not pod_uid or not node_name):
            return
        summary, code = await self._snapshot(node_name)
        observation: BuildSample | str = (code or "snapshot_unavailable") if summary is None else \
            project_build_pod(summary, namespace=self.namespace, pod_uid=pod_uid)
        async with self.sessions() as session, session.begin():
            await record_build_sample(session, attempt_id=attempt.id, materialization_id=attempt.materialization_id,
                                      lease_epoch=attempt.lease_epoch, pod_uid=pod_uid, node_name=node_name,
                                      observation=observation, now=datetime.now(UTC))


async def sample_safely(sampler: BuildUsageSampler | None, attempt_id: UUID, *, timeout: float = 5.0) -> str | None:
    """Telemetry boundary: bounded, and never raises into build reconciliation."""
    if sampler is None:
        return None
    try:
        await asyncio.wait_for(sampler.sample(attempt_id), timeout)
    except TimeoutError:
        return "timeout"
    except Exception:
        return "error"
    return None
