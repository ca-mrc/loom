"""Trusted node-observer samples of native build Pods (#2750).

The build controller starts ``BuildUsageSampler.sample_round`` in the
background; a reconcile pass never awaits it. A round samples the least
recently sampled outstanding attempts with bounded concurrency, and reads each
node's builder snapshot at most once at a time (same producer identity,
freshness and field validation as Trial observations, plus the attempt's
qualified Node UID where it has one). It projects only this attempt's Pod by
namespace and UID and updates one row in ``task_image_build_resource_usage``.
It never touches ``native_build`` or any lifecycle state, and its failures are
recorded as unavailable, never zero.

A cached snapshot is checked again against its own freshness clocks when used.
Each counted snapshot keeps its own times, so coverage of each build phase and
the gaps between snapshots can be stated rather than inferred from counts.
Sampling is best effort: a build that finishes between rounds may have no row,
and the report treats that as unobserved.

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
_MAX_SAMPLES = 240  # two hours at the observer's 30 s schedule
_SAMPLES_PER_ROUND = 64
_SAMPLE_CONCURRENCY = 8
_READ_CONCURRENCY = 4
# A shared node read (three API GETs with their own 10 s budget) never holds a
# reader slot longer than this, even if the underlying call never returns.
_READ_SECONDS = 12.0
_NODE_UID_ANNOTATION = "loom.openai.com/build-node-uid"
_NODE_NAME_ANNOTATION = "loom.openai.com/build-node-name"
_MAXIMA = {"cpu_nanocores": "cpu_max_nanocores", "memory_bytes": "memory_max_bytes",
           "filesystem_bytes": "filesystem_max_bytes"}


@dataclass(frozen=True)
class BuildSample:
    snapshot_at: datetime
    storage_bytes: int | None
    unknown_codes: tuple[str, ...]
    containers: dict[str, dict[str, int | None]] = field(default_factory=dict)
    node_uid: str | None = None
    storage_at: datetime | None = None
    # When this Pod's storage walk finished: the total is measured over [storage_at, this].
    storage_finished_at: datetime | None = None
    # Each container's own CPU and memory measurement times (None when that field is unavailable).
    counter_times: dict[str, dict[str, datetime | None]] = field(default_factory=dict)


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


def _fresh(value: Any, now: datetime, max_age: float) -> datetime | None:
    at = _time(value)
    return at if at is not None and -5 <= (now - at).total_seconds() <= max_age else None


def project_build_pod(summary: dict[str, Any], *, namespace: str, pod_uid: str,
                      now: datetime | None = None) -> BuildSample | str:
    """This Pod's validated sample, or a fixed reason it is unavailable in this snapshot.

    Freshness is checked against ``now`` (the time of use), so a cached snapshot
    or field past its own maximum age is never counted as a current reading.
    """
    current = now or datetime.now(UTC)
    metadata = summary.get("_native_observation")
    if not isinstance(metadata, dict):
        return "snapshot_time_unavailable"
    declared = metadata.get("max_age_seconds")
    max_age = float(declared) if isinstance(declared, int | float) and 0 < declared <= 120 else 60.0
    snapshot_at = _time(metadata.get("observed_at"))
    if snapshot_at is None:
        return "snapshot_time_unavailable"
    if _fresh(metadata.get("observed_at"), current, max_age) is None:
        return "snapshot_stale"
    node_uid = metadata.get("node_uid") if isinstance(metadata.get("node_uid"), str) else None
    pod = next((row for row in summary.get("pods") or [] if isinstance(row, dict)
                and isinstance(row.get("podRef"), dict) and row["podRef"].get("uid") == pod_uid), None)
    if pod is None:
        return "pod_not_in_snapshot"
    if pod["podRef"].get("namespace") != namespace:
        return "pod_identity_mismatch"
    storage = pod.get("ephemeral-storage")
    total = _int(storage.get("usedBytes")) if isinstance(storage, dict) else None
    storage_at = _time(storage.get("time")) if isinstance(storage, dict) else None
    codes = pod.get("_diagnostic_codes")
    unknown = tuple(code for code in codes if isinstance(code, str) and _CODE.fullmatch(code))[:_MAX_CODES] \
        if isinstance(codes, list) else ()
    if total is not None and isinstance(storage, dict) and (
            storage_at is None or _fresh(storage.get("time"), current, max_age) is None):
        total, unknown = None, ("storage_stale",)
    if total is None and not unknown:
        unknown = ("storage_unavailable",)
    finished_at = _time(storage.get("_finished_at")) if isinstance(storage, dict) else None
    if total is None or storage_at is None or finished_at is None or finished_at < storage_at:
        finished_at = None
    containers: dict[str, dict[str, int | None]] = {}
    counter_times: dict[str, dict[str, datetime | None]] = {}
    for row in pod.get("containers") or []:
        if not isinstance(row, dict) or row.get("name") not in CONTAINERS:
            continue
        cpu, memory = row.get("cpu") or {}, row.get("memory") or {}
        rootfs, logs = row.get("rootfs") or {}, row.get("logs") or {}
        rootfs_bytes = _int(rootfs.get("usedBytes")) if _fresh(rootfs.get("time"), current, max_age) else None
        log_bytes = _int(logs.get("usedBytes")) if _fresh(logs.get("time"), current, max_age) else None
        cpu_value = _int(cpu.get("usageNanoCores")) if _fresh(cpu.get("time"), current, max_age) else None
        memory_value = _int(memory.get("usageBytes")) if _fresh(memory.get("time"), current, max_age) else None
        counter_times[row["name"]] = {
            "cpu_at": _time(cpu.get("time")) if cpu_value is not None else None,
            "memory_at": _time(memory.get("time")) if memory_value is not None else None,
        }
        containers[row["name"]] = {
            "cpu_nanocores": cpu_value,
            "memory_bytes": memory_value,
            # Only a same-snapshot pair is a container filesystem figure.
            "filesystem_bytes": rootfs_bytes + log_bytes
            if rootfs_bytes is not None and log_bytes is not None else None,
        }
    return BuildSample(snapshot_at=snapshot_at, storage_bytes=total,
                       unknown_codes=unknown if total is None else (), containers=containers,
                       node_uid=node_uid, storage_at=storage_at if total is not None else None,
                       storage_finished_at=finished_at, counter_times=counter_times)


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
    """Upsert this attempt/Pod row; idempotent per producer snapshot.

    The row is fenced by lease epoch, node name and (once known) Node UID: a
    snapshot from a different Node incarnation is never merged, even under a
    reused name.
    """
    node_uid = observation.node_uid if isinstance(observation, BuildSample) else None
    await session.execute(insert(TaskImageBuildResourceUsage).values(
        attempt_id=attempt_id, materialization_id=materialization_id, lease_epoch=lease_epoch,
        pod_uid=pod_uid, node_name=node_name, node_uid=node_uid, source=SOURCE,
        first_observed_at=now, last_observed_at=now,
    ).on_conflict_do_nothing(constraint="uq_task_image_build_resource_usage_pod"))
    row = await session.scalar(select(TaskImageBuildResourceUsage).where(
        TaskImageBuildResourceUsage.attempt_id == attempt_id,
        TaskImageBuildResourceUsage.pod_uid == pod_uid,
    ).with_for_update())
    assert row is not None
    if row.node_name != node_name or row.lease_epoch != lease_epoch:
        return  # never merge a different Pod placement or epoch into this row
    if node_uid is not None and row.node_uid is not None and row.node_uid != node_uid:
        observation = "node_identity_changed"
    if isinstance(observation, str):
        row.unavailable += 1
        row.last_unavailable_code = observation
        row.last_observed_at = now
        return
    if row.last_snapshot_at is not None and observation.snapshot_at <= row.last_snapshot_at:
        return  # the same (or an older) producer snapshot is counted once
    if row.node_uid is None and node_uid is not None:
        row.node_uid = node_uid
    row.snapshots += 1
    row.last_snapshot_at = observation.snapshot_at
    row.last_observed_at = now
    if observation.storage_bytes is not None:
        row.storage_complete += 1
        if row.ephemeral_storage_max_bytes is None or observation.storage_bytes > row.ephemeral_storage_max_bytes:
            row.ephemeral_storage_max_bytes = observation.storage_bytes
            # The storage field's own measurement time, not the snapshot's.
            row.ephemeral_storage_max_at = observation.storage_at or observation.snapshot_at
    else:
        row.storage_unknown += 1
        codes = dict(row.unknown_codes or {})
        for code in observation.unknown_codes:
            if code in codes or len(codes) < _MAX_CODES:
                codes[code] = int(codes.get(code, 0)) + 1
        row.unknown_codes = codes
    row.containers = _merge_containers(row.containers or {}, observation)
    samples = list(row.samples or [])
    def stamp(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    # Each signal keeps its own measurement time: coverage never attributes a
    # reading to a phase by the snapshot's start.
    samples.append({"at": observation.snapshot_at.isoformat(),
                    "storage_at": stamp(observation.storage_at),
                    "storage_finished_at": stamp(observation.storage_finished_at),
                    "storage_bytes": observation.storage_bytes,
                    "counters": {name: {key: stamp(value) for key, value in times.items()}
                                 for name, times in sorted(observation.counter_times.items())}})
    if len(samples) > _MAX_SAMPLES:
        # Keep the first and the most recent; the dropped count makes the gap explicit.
        row.samples_dropped += len(samples) - _MAX_SAMPLES
        samples = samples[:1] + samples[-(_MAX_SAMPLES - 1):]
    row.samples = samples


def _expected_node_uid(native: dict[str, Any], node_name: str) -> tuple[bool, str | None]:
    """(consistent, the attempt's qualified Node UID or None). Inconsistent when it names another node."""
    job = native.get("job")
    annotations = ((job.get("metadata") or {}).get("annotations") or {}) if isinstance(job, dict) else {}
    uid = annotations.get(_NODE_UID_ANNOTATION) if isinstance(annotations, dict) else None
    if not isinstance(uid, str) or not uid:
        return True, None
    return annotations.get(_NODE_NAME_ANNOTATION) == node_name, uid


class BuildUsageSampler:
    """Fair, bounded rounds of builder-snapshot reads; one row update per sampled attempt."""

    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], namespace: str,
                 reader: Callable[..., Awaitable[dict[str, Any]]], read_seconds: float = _READ_SECONDS) -> None:
        self.sessions, self.namespace, self.reader = sessions, namespace, reader
        self.read_seconds = read_seconds
        self._cache: dict[tuple[str, str | None], tuple[float, dict[str, Any] | None, str | None]] = {}
        self._reads: dict[tuple[str, str | None], asyncio.Task[tuple[dict[str, Any] | None, str | None]]] = {}
        self._read_slots = asyncio.Semaphore(_READ_CONCURRENCY)
        self._last_sampled: dict[UUID, float] = {}

    async def _read(self, key: tuple[str, str | None]) -> tuple[dict[str, Any] | None, str | None]:
        async with self._read_slots:
            node_name, expected = key
            read = asyncio.ensure_future(
                self.reader(node_name, expected_node_uid=expected) if expected else self.reader(node_name))
            try:
                done, _ = await asyncio.wait({read}, timeout=self.read_seconds)
                if not done:
                    read.cancel()
                    summary, code = None, "snapshot_read_timeout"
                else:
                    summary, code = read.result(), None
            except asyncio.CancelledError:
                read.cancel()
                raise
            except Exception as error:  # a fixed diagnostic only; never a raw message
                summary, code = None, resource_sample_diagnostic(error)
        now = time.monotonic()
        self._cache = {k: v for k, v in self._cache.items() if now - v[0] < 60}
        self._cache[key] = (now, summary, code)
        return summary, code

    def _forget(self, key: tuple[str, str | None]) -> Callable[[asyncio.Task[Any]], None]:
        def done(task: asyncio.Task[Any]) -> None:
            if self._reads.get(key) is task:
                del self._reads[key]
        return done

    async def _snapshot(self, node_name: str, expected: str | None = None) -> tuple[dict[str, Any] | None, str | None]:
        key = (node_name, expected)
        cached = self._cache.get(key)
        if cached is not None and time.monotonic() - cached[0] < _CACHE_SECONDS:
            return cached[1], cached[2]
        task = self._reads.get(key)
        if task is None or task.done():
            # One read per node at a time; waiters share it and cancelling one never cancels the read.
            task = asyncio.ensure_future(self._read(key))
            self._reads[key] = task
            task.add_done_callback(self._forget(key))
        return await asyncio.shield(task)

    async def close(self) -> None:
        """Cancel in-flight shared reads (controller shutdown)."""
        reads = list(self._reads.values())
        for task in reads:
            task.cancel()
        if reads:
            await asyncio.gather(*reads, return_exceptions=True)
        self._reads.clear()

    async def sample(self, attempt_id: UUID) -> None:
        async with self.sessions() as session:
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
            native = (attempt.native_build or {}) if attempt is not None else {}
        pod_uid, node_name = native.get("pod_uid"), native.get("node_name")
        if (attempt is None or native.get("capacity_released_at") or not isinstance(pod_uid, str)
                or not isinstance(node_name, str) or not pod_uid or not node_name):
            return
        consistent, expected = _expected_node_uid(native, node_name)
        observation: BuildSample | str
        if not consistent:
            observation = "node_identity_mismatch"
        else:
            summary, code = await self._snapshot(node_name, expected)
            observation = (code or "snapshot_unavailable") if summary is None else \
                project_build_pod(summary, namespace=self.namespace, pod_uid=pod_uid)
            if isinstance(observation, BuildSample) and expected and observation.node_uid != expected:
                observation = "node_identity_mismatch"
        async with self.sessions() as session, session.begin():
            await record_build_sample(session, attempt_id=attempt.id, materialization_id=attempt.materialization_id,
                                      lease_epoch=attempt.lease_epoch, pod_uid=pod_uid, node_name=node_name,
                                      observation=observation, now=datetime.now(UTC))

    def schedule(self, attempt_ids: list[UUID], *, limit: int = _SAMPLES_PER_ROUND) -> list[UUID]:
        """Least recently sampled first, so no outstanding attempt is starved by list order or count."""
        present = set(attempt_ids)
        self._last_sampled = {key: value for key, value in self._last_sampled.items() if key in present}
        order = sorted(range(len(attempt_ids)), key=lambda i: (self._last_sampled.get(attempt_ids[i], -1.0), i))
        return [attempt_ids[i] for i in order[:limit]]

    async def sample_round(self, attempt_ids: list[UUID], *, limit: int = _SAMPLES_PER_ROUND,
                           concurrency: int = _SAMPLE_CONCURRENCY, timeout: float = 5.0) -> dict[UUID, str | None]:
        """One bounded round: at most ``limit`` attempts, ``concurrency`` at once, each bounded by ``timeout``."""
        chosen = self.schedule(attempt_ids, limit=limit)
        slots = asyncio.Semaphore(concurrency)
        outcomes: dict[UUID, str | None] = {}

        async def one(attempt_id: UUID) -> None:
            async with slots:
                self._last_sampled[attempt_id] = time.monotonic()
                outcomes[attempt_id] = await sample_safely(self, attempt_id, timeout=timeout)

        await asyncio.gather(*(one(attempt_id) for attempt_id in chosen))
        return outcomes


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
