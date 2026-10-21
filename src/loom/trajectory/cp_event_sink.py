"""CP-side event observer that batches typed trajectory events and
POSTs them through to the `trial_events` table via the worker's
`HttpControlPlaneClient.append_events` method.

Owned by the worker for the lifetime of one trial. The worker
constructs it once per trial, wires it as `TrialContext.event_observer`,
and `TrajectoryWriter` calls `observe(event)` from `append`
alongside the existing local-JSONL + MinIO multipart writes. On
trial close, `flush_and_close()` drains the buffer.

Postgres supplies event replay and live SSE notifications. The sink remains
best-effort: failed POSTs are logged and dropped without failing the writer.
The object-store trajectory retains the durable event copy.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from loom.models.trajectory import TrajectoryEvent

logger = logging.getLogger(__name__)


# Type alias used by TrajectoryWriter — any async callback that
# takes one event observed at append time. Decouples the writer
# from the CP-sink concrete class so tests + future alternative
# sinks (e.g. a local-debug echo) plug in cleanly.
EventObserver = Callable[[TrajectoryEvent], Awaitable[None]]


# Default flush triggers. Smaller than MinIO's 5 MiB / 1000-event
# multipart cadence because the CP event table is what powers the
# live SSE path — low latency matters more than throughput.
_DEFAULT_FLUSH_EVENT_COUNT = 50
_DEFAULT_FLUSH_INTERVAL_SEC = 2.0
# Cap per HTTP batch so we stay under the CP route's 500-event limit
# even on pathological flushes that catch a long backlog.
_MAX_BATCH_SIZE = 500


# Callback shape for the actual HTTP send. Pulled out as a Protocol
# so tests can inject a fake without depending on the worker's
# concrete `HttpControlPlaneClient`.
EventBatchSender = Callable[
    [list[dict[str, Any]]],
    Awaitable[bool],
]


class CpEventSink:
    """Buffers typed events and flushes batches asynchronously.

    Flush triggers: event count >= `flush_event_count`, OR seconds
    since last flush >= `flush_interval_sec`, OR explicit
    `flush_and_close()`. Flush failures are logged and the batch is
    dropped; the object-store trajectory retains the durable copy.
    """

    def __init__(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        send_batch: EventBatchSender,
        source: str = "worker",
        schema_version: int = 1,
        flush_event_count: int = _DEFAULT_FLUSH_EVENT_COUNT,
        flush_interval_sec: float = _DEFAULT_FLUSH_INTERVAL_SEC,
    ) -> None:
        self._trial_id = trial_id
        self._worker_id = worker_id
        self._send_batch = send_batch
        self._source = source
        self._schema_version = schema_version
        self._flush_event_count = flush_event_count
        self._flush_interval_sec = flush_interval_sec
        self._buf: list[dict[str, Any]] = []
        self._last_flush_at = time.monotonic()
        self._closed = False
        self._lost_claim = False
        # Lock around buffer mutations so concurrent appends from
        # async tasks don't interleave inside flush.
        self._lock = asyncio.Lock()

    @property
    def lost_claim(self) -> bool:
        """True once the CP returned 409 to a flush — the worker no
        longer owns the trial, so the writer should stop calling us."""
        return self._lost_claim

    async def observe(self, event: TrajectoryEvent) -> None:
        """Buffer the event and flush if either threshold trips.

        Best-effort: never raises. CP outages, network errors, and
        worker-lost-claim are logged + swallowed; MinIO remains the
        durable trajectory copy."""
        if self._closed:
            return
        if self._lost_claim:
            return
        payload = event.model_dump(mode="json")
        row = {
            "seq": payload["seq"],
            "kind": payload["kind"],
            "source": self._source,
            "schema_version": self._schema_version,
            "payload": payload,
        }
        async with self._lock:
            self._buf.append(row)
            should_flush = (
                len(self._buf) >= self._flush_event_count
                or (time.monotonic() - self._last_flush_at)
                >= self._flush_interval_sec
            )
        if should_flush:
            await self._flush()

    async def observe_raw(self, payload: dict[str, Any]) -> None:
        """For TrajectoryWriter.write_raw_dict — payload already has
        seq/kind/etc on it from the upstream subprocess adapter."""
        if self._closed:
            return
        if self._lost_claim:
            return
        seq = payload.get("seq")
        kind = payload.get("kind")
        if not isinstance(seq, int) or not isinstance(kind, str):
            return  # Adapter emitted a shape we can't index against; skip.
        row = {
            "seq": seq,
            "kind": kind,
            "source": self._source,
            "schema_version": self._schema_version,
            "payload": payload,
        }
        async with self._lock:
            self._buf.append(row)
            should_flush = (
                len(self._buf) >= self._flush_event_count
                or (time.monotonic() - self._last_flush_at)
                >= self._flush_interval_sec
            )
        if should_flush:
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            if not self._buf:
                self._last_flush_at = time.monotonic()
                return
            batch = self._buf[:_MAX_BATCH_SIZE]
            remaining = self._buf[_MAX_BATCH_SIZE:]
            self._buf = remaining
            self._last_flush_at = time.monotonic()
        try:
            ok = await self._send_batch(batch)
        except Exception:
            logger.warning(
                "cp_event_sink_flush_failed trial=%s n=%d",
                self._trial_id, len(batch),
                exc_info=True,
            )
            return
        if not ok:
            # 409 — worker lost claim. Stop further flushes; MinIO
            # already has the events and the reclaim path will
            # re-attempt the trial under a new worker.
            self._lost_claim = True
            logger.warning(
                "cp_event_sink_lost_claim trial=%s — stopping CP writes",
                self._trial_id,
            )

    async def flush_and_close(self) -> None:
        """Drain remaining events + mark closed. Called by
        TrajectoryWriter._close() after the local file is flushed
        and BEFORE the MinIO multipart upload is completed.

        Drains in batches up to `_MAX_BATCH_SIZE` so an end-of-trial
        backlog of thousands of events doesn't trip the CP route's
        per-request cap."""
        if self._closed:
            return
        # Drain as many times as needed — _flush takes at most
        # _MAX_BATCH_SIZE per call.
        while True:
            async with self._lock:
                remaining = len(self._buf)
            if remaining == 0 or self._lost_claim:
                break
            await self._flush()
        self._closed = True
