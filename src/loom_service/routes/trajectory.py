"""Trajectory object pagination, authenticated downloads and event replay.

The legacy line-cursor endpoint reads the stored events.jsonl object. Downloads
use that object when present and reconstruct JSONL from PostgreSQL otherwise.
Sequence-cursor replay and SSE read trial_events first, with an object fallback
when no newer database events are available for a running or terminal trial.
This preserves retained object-only trajectories.

SSE listens on trial_events_inserted (migration 0041) and drains committed rows
after notifications. Periodic polling also detects terminal state and missed
notifications; LISTEN connection failure falls back to polling.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from itertools import pairwise
from typing import Annotated, Any, cast
from urllib.parse import quote
from uuid import UUID

import psycopg
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select

from loom.db.schema import Trial, TrialEvent
from loom.service_execution_live import read_live_preview
from loom.trajectory.object_identity import resolve_trajectory_object_key
from loom_listen.metrics import PUSH_MODE_GAUGE as _PUSH_MODE_GAUGE
from loom_listen.self_test import notify_round_trip
from loom_service import wire_responses as wire
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.metrics import ARTIFACT_DOWNLOAD_BYTES
from loom_service.routes.object_downloads import stream_object_response
from loom_service.trajectory_reconstruction import (
    read_all_events_from_postgres,
    read_llm_calls_from_postgres,
    reconstruct_postgres_trajectory_events,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
# Migration 0041's NOTIFY channel; payload is `<trial_id>:<seq>`.
_LISTEN_CHANNEL = "trial_events_inserted"
# Poll Postgres-first event pages (with the stored-trajectory fallback) and
# terminal state at this cadence when LISTEN/NOTIFY has not already woken us.
_DEFAULT_SSE_POLL_INTERVAL_SEC = 1.5
# Cap any single SSE response at this long to bound resource cost
# from forgotten browser tabs and to give the client a deterministic
# reconnect point. The client should reconnect with the last seen
# seq as `after_seq` — standard SSE Last-Event-ID semantics.
_DEFAULT_SSE_MAX_CONNECTION_SEC = 600.0


def _key(trial: Trial, *, expected_bucket: str) -> str:
    index = trial.trajectory_index if isinstance(trial.trajectory_index, dict) else {}
    try:
        return resolve_trajectory_object_key(
            uri=index.get("trajectory_uri"),
            expected_bucket=expected_bucket,
            team_id=trial.team_id,
            trial_id=trial.id,
            filename="events.jsonl",
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "trajectory_object_identity_invalid",
                "message": str(exc),
            },
        ) from exc


async def _load_trial(session: Any, trial_id: UUID, ctx: Any) -> Trial:
    trial = (await session.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    return cast(Trial, trial)


def _read_events_after_seq(
    client: Any,
    *,
    bucket: str,
    key: str,
    after_seq: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch the MinIO object and return events whose `seq` is strictly
    greater than `after_seq`, ordered by seq, capped at `limit`.

    Mirrors the line-cursor read in `list_events` but pivots on the
    event payload's own `seq` field, matching PostgreSQL's event index and
    the replay endpoint's cursor contract.

    Events without a numeric `seq` are skipped — they can't be ordered
    or resumed against. The worker's TrajectoryWriter always emits
    `seq`, so this only filters legacy/test data.
    """
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return []
        raise
    out: list[dict[str, Any]] = []
    body = obj["Body"]
    try:
        for raw in body.iter_lines():
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                ev = json.loads(text)
            except json.JSONDecodeError:
                continue
            seq = ev.get("seq")
            if not isinstance(seq, int):
                continue
            if seq <= after_seq:
                continue
            out.append(ev)
            if len(out) >= limit:
                break
    finally:
        body.close()
    out.sort(key=lambda e: e["seq"])
    return out


async def _read_events_from_postgres(
    session: Any, *, trial_id: UUID, after_seq: int, limit: int,
) -> list[dict[str, Any]]:
    """Primary replay read path. Pulls payloads from the
    `trial_events` table indexed on (trial_id, seq).

    The payload column already carries the full typed event body
    (kind, seq, emitted_at, plus per-type fields), so the response
    shape matches what `_read_events_after_seq` returns from MinIO —
    callers can swap reads transparently."""
    rows = (
        await session.execute(
            select(TrialEvent.payload)
            .where(
                TrialEvent.trial_id == trial_id,
                TrialEvent.seq > after_seq,
            )
            .order_by(TrialEvent.seq.asc())
            .limit(limit),
        )
    ).all()
    return [row[0] for row in rows]


def _postgres_events_download_response(
    events: list[dict[str, Any]], *, trial_id: UUID,
) -> Response:
    content = "".join(
        json.dumps(event, separators=(",", ":")) + "\n"
        for event in events
    ).encode("utf-8")
    headers = {
        "Content-Disposition": (
            f"attachment; filename*=UTF-8''{quote(f'{trial_id}-events.jsonl', safe='')}"
        ),
        "Content-Length": str(len(content)),
    }
    ARTIFACT_DOWNLOAD_BYTES.labels(artifact_kind="trajectory").inc(len(content))
    return Response(
        content=content,
        headers=headers,
        media_type="application/x-ndjson",
    )


@router.get("/trials/{trial_id}/trajectory", response_model=wire.TrajectoryPage, response_model_exclude_unset=True)
async def list_events(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    cursor: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
) -> dict[str, Any]:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    client = request.app.state.minio_client
    try:
        obj = client.get_object(
            Bucket=settings.trajectories_bucket,
            Key=_key(trial, expected_bucket=settings.trajectories_bucket),
        )
    except ClientError as exc:
        # A missing object means the trial hasn't written a first event
        # yet (queued/just-claimed) OR the worker crashed pre-first-event.
        # Either way we return an empty page rather than 404 — the trial
        # row exists (we already validated), so the UI's polling loop
        # should show "no events yet" not a scary 404. Other S3 errors
        # (perms, bucket missing) keep propagating.
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return {"events": [], "next_cursor": None}
        raise
    # Stream-decode the JSONL line by line instead of materializing
    # the whole object into memory — a 100k-event trial would otherwise
    # cost ~200 MB raw + ~400 MB after split. `iter_lines()` lets us
    # skip lines up to `cursor`, decode `limit` events, and exit
    # early; remaining bytes stay on the wire (the response.close()
    # triggers a connection-close).
    body = obj["Body"]
    events: list[dict[str, Any]] = []
    next_cursor: int | None = None
    line_index = 0  # 1-based count of non-blank lines seen
    try:
        for raw in body.iter_lines():
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            line_index += 1
            if line_index <= cursor:
                continue
            if len(events) >= limit:
                next_cursor = cursor + limit
                break
            try:
                events.append(json.loads(text))
            except json.JSONDecodeError:
                # Tolerate truncation tails — finalize crashes can
                # leave a partial last line.
                continue
    finally:
        body.close()
    return {"events": events, "next_cursor": next_cursor}


@router.get("/trials/{trial_id}/trajectory/download")
async def download_trajectory(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> Response:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    try:
        return stream_object_response(
            client=request.app.state.minio_client,
            bucket=settings.trajectories_bucket,
            key=_key(trial, expected_bucket=settings.trajectories_bucket),
            filename=f"{trial.id}-events.jsonl",
            artifact_kind="trajectory",
            media_type="application/x-ndjson",
        )
    except HTTPException as exc:
        if exc.status_code != 404:
            raise

    events = await read_all_events_from_postgres(s, trial_id=trial.id)
    if not events:
        raise HTTPException(
            status_code=404,
            detail="download object not found",
        )
    llm_calls = await read_llm_calls_from_postgres(s, trial_id=trial.id)
    events = reconstruct_postgres_trajectory_events(
        events,
        trial=trial,
        llm_calls=llm_calls,
    )
    return _postgres_events_download_response(events, trial_id=trial.id)



async def _read_events_with_minio_fallback(
    session: Any,
    minio_client: Any,
    *,
    trial: Trial,
    bucket: str,
    after_seq: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Read the database page first, then try stored events when it is empty.

    The fallback applies to running and terminal trials and preserves
    object-only history. Queued and claimed trials return the database page
    without an object lookup.
    """
    events = await _read_events_from_postgres(
        session, trial_id=trial.id, after_seq=after_seq, limit=limit,
    )
    if events:
        return events
    if trial.state in _TERMINAL_TRIAL_STATES or trial.state == "running":
        # The requested database suffix is empty; retained object-backed
        # history may still have newer events. Log successful fallback reads.
        legacy = await asyncio.to_thread(
            _read_events_after_seq,
            minio_client,
            bucket=bucket,
            key=_key(trial, expected_bucket=bucket),
            after_seq=after_seq, limit=limit,
        )
        if legacy:
            logger.info(
                "events_minio_fallback trial=%s state=%s n=%d "
                "after_seq=%d — pre-Slice-3b trial, served from MinIO",
                trial.id, trial.state, len(legacy), after_seq,
            )
        return legacy
    return events


@router.get("/trials/{trial_id}/events")
async def list_events_by_seq(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    after_seq: Annotated[int, Query(ge=-1)] = -1,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
    include_preview: bool = False,
    preview_source: Annotated[str | None, Query(max_length=80)] = None,
    preview_after_seq: Annotated[int, Query(ge=-1)] = -1,
) -> dict[str, Any]:
    """Return events with `seq > after_seq`, capped at `limit`.

    Reads from the Postgres `trial_events` table, with MinIO fallback for
    retained object-backed events.

    Use `after_seq=-1` (the default) to start from the beginning;
    `after_seq=N` to resume after event seq `N`. `next_after_seq` in
    the response is the seq of the last event returned, or `null`
    when no events were returned (caller should re-poll with the
    same cursor)."""
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    events = await _read_events_with_minio_fallback(
        s,
        request.app.state.minio_client,
        trial=trial,
        bucket=settings.trajectories_bucket,
        after_seq=after_seq,
        limit=limit,
    )
    next_after_seq = events[-1]["seq"] if events else None
    result: dict[str, Any] = {"events": events, "next_after_seq": next_after_seq}
    if include_preview:
        preview = await read_live_preview(
            s, trial=trial, source=preview_source, after_seq=preview_after_seq, limit=limit,
        ) if not events else {"source": None, "events": [], "next_after_seq": None}
        result["preview"] = preview if preview["source"] is not None else None
    return result


def _sse_format(event_kind: str | None, data: dict[str, Any], event_id: str | None = None) -> bytes:
    """Format one SSE message. `event_kind` is optional (defaults to
    `message`); `event_id` populates `id:` so clients reconnect with
    Last-Event-ID."""
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event_kind is not None:
        lines.append(f"event: {event_kind}")
    payload = json.dumps(data, separators=(",", ":"))
    lines.append(f"data: {payload}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


def _sqla_url_to_psycopg_dsn(url: str) -> str:
    """SQLAlchemy uses `postgresql+psycopg://`; psycopg.AsyncConnection
    wants the bare `postgresql://` scheme."""
    for prefix in ("postgresql+psycopg://", "postgresql+asyncpg://"):
        if url.startswith(prefix):
            return "postgresql://" + url[len(prefix):]
    return url


class _ListenSubscription:
    """One trial's psycopg LISTEN subscription. Owns the dedicated
    autocommit connection + the drain task that watches
    `conn.notifies()` and sets `wake` when a NOTIFY whose payload
    starts with `<trial_id>:` arrives.

    Use as `async with _ListenSubscription(...) as sub: ...`. On exit
    the drain task is cancelled and the connection is closed. The
    connection is opened lazily inside `__aenter__` so a connection
    failure can be caught + the route can degrade to pure-poll mode
    without blowing up the request. Failed or cancelled entry closes
    partially acquired resources before propagating the original error.
    """

    def __init__(self, dsn: str, trial_id: UUID) -> None:
        self._dsn = dsn
        self._target_prefix = f"{trial_id}:"
        self._conn: psycopg.AsyncConnection[Any] | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._push_mode: bool = False
        self.wake = asyncio.Event()

    async def __aenter__(self) -> _ListenSubscription:
        try:
            # Autocommit is required for LISTEN — the connection is a
            # streaming consumer, not transaction-scoped.
            self._conn = await psycopg.AsyncConnection.connect(
                self._dsn, autocommit=True,
            )
            await self._conn.execute(f"LISTEN {_LISTEN_CHANNEL}")
            push_ok = await notify_round_trip(self._conn, timeout_sec=1.0)
            if push_ok:
                _PUSH_MODE_GAUGE.labels(watcher="trajectory").set(1)
            else:
                logger.error(
                    "trajectory_listen_selftest_failed — NOTIFY round-trip timed out; "
                    "SSE stream will fall back to poll-only mode. "
                    "Check that the LISTEN connection is not routed through "
                    "pgbouncer transaction mode.",
                )
                _PUSH_MODE_GAUGE.labels(watcher="trajectory").set(0)
            self._push_mode = push_ok
            self._drain_task = asyncio.create_task(self._drain())
        except BaseException:
            # Context managers do not call __aexit__ when __aenter__ fails.
            # Include cancellation, which must clean up rather than fall back.
            await self.__aexit__()
            raise
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._drain_task is not None:
            self._drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._drain_task
        if self._conn is not None:
            with contextlib.suppress(Exception):
                await self._conn.close()

    async def _drain(self) -> None:
        """Consume the connection's NOTIFY stream forever. Set `wake`
        on every notification whose payload starts with our trial's
        prefix; ignore notifications for other trials (multiple SSE
        streams across many trials all share the same channel)."""
        assert self._conn is not None
        try:
            async for notify in self._conn.notifies():
                if notify.payload.startswith(self._target_prefix):
                    self.wake.set()
        except Exception:
            # Connection died — the main loop will fall back to its
            # fixed-interval poll. Log once at WARN so operators see
            # listen-drop noise without a per-event traceback.
            logger.warning(
                "trial_events_listen_drained_err — falling back to poll",
                exc_info=True,
            )


@router.get("/trials/{trial_id}/stream")
async def stream_events(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    after_seq: Annotated[int, Query(ge=-1)] = -1,
    preview_source: Annotated[str | None, Query(max_length=80)] = None,
    preview_after_seq: Annotated[int, Query(ge=-1)] = -1,
) -> StreamingResponse:
    """SSE live event stream for `trial_id`, starting at `after_seq + 1`.

    The connection drains Postgres-first event pages with the stored MinIO
    trajectory as a fallback, then waits for notifications or the polling
    interval. It completes only after a terminal trial has no remaining events.
    It also stops if the client disconnects or the connection has been open for longer
    than `_DEFAULT_SSE_MAX_CONNECTION_SEC` (clients reconnect with
    the last seen seq as `after_seq`).

    When an event insert commits, migration 0041's trigger
    fires NOTIFY on `trial_events_inserted` and the inner loop wakes
    immediately to read+ship the new rows. The fixed `poll_interval`
    fallback still ticks as a safety valve for missed notifications
    AND for terminal-state detection (state transitions don't
    trigger NOTIFY). On LISTEN connection error the route degrades
    to pure-poll mode without a UX regression.

    The frontend should still implement an `useAdaptivePolling`
    fallback for environments without working `EventSource` (some
    corp proxies strip `text/event-stream`).
    """
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    session_factory = request.app.state.session_factory
    minio_client = request.app.state.minio_client
    bucket = settings.trajectories_bucket
    poll_interval = _DEFAULT_SSE_POLL_INTERVAL_SEC
    max_connection_sec = _DEFAULT_SSE_MAX_CONNECTION_SEC
    listen_dsn = _sqla_url_to_psycopg_dsn(str(settings.db_url))

    async def _read_loop_chunk(
        seq_cursor: int, live_source: str | None, live_cursor: int,
    ) -> tuple[list[dict[str, Any]], str | None, dict[str, Any], bool]:
        """One poll iteration: read events from Postgres (with MinIO
        fallback) AND check trial state. Returning both from one
        helper keeps the per-iteration session lifetime clean."""
        async with session_factory() as fresh:
            fresh_trial = (await fresh.execute(
                select(Trial).where(Trial.id == trial.id),
            )).scalar_one_or_none()
            if fresh_trial is None:
                return [], None, {"source": None, "events": [], "next_after_seq": None}, False
            evts = await _read_events_with_minio_fallback(
                fresh,
                minio_client,
                trial=fresh_trial,
                bucket=bucket,
                after_seq=seq_cursor,
                limit=200,
            )
            canonical_available = bool(evts) or bool(await fresh.scalar(
                select(TrialEvent.id).where(TrialEvent.trial_id == trial.id).limit(1),
            ))
            preview = {"source": None, "events": [], "next_after_seq": None} if canonical_available else await read_live_preview(
                fresh, trial=fresh_trial, source=live_source, after_seq=live_cursor, limit=200,
            )
            return evts, fresh_trial.state, preview, canonical_available

    async def _open_subscription() -> _ListenSubscription | None:
        """Best-effort LISTEN setup. On error (Postgres unreachable
        on autocommit, channel ACL issue) return None and let the
        loop fall back to pure-poll behavior."""
        sub = _ListenSubscription(listen_dsn, trial.id)
        try:
            await sub.__aenter__()
            return sub
        except Exception:
            logger.warning(
                "trial_events_listen_open_failed — pure-poll mode",
                exc_info=True,
            )
            return None

    async def event_source() -> AsyncIterator[bytes]:
        current_seq = after_seq
        current_preview_source = preview_source
        current_preview_seq = preview_after_seq if preview_source is not None else -1
        canonical_available = False
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        completed_state: str | None = None
        # Emit a comment line up front so any proxy that buffers SSE
        # gets a flush before the first real event lands.
        yield b": stream open\n\n"
        subscription = await _open_subscription()
        try:
            while True:
                if await request.is_disconnected():
                    return
                if loop.time() - started_at >= max_connection_sec:
                    yield _sse_format(
                        event_kind="reconnect",
                        data={"reason": "max_connection_sec", "last_seq": current_seq},
                    )
                    return
                if completed_state is not None:
                    yield _sse_format(
                        event_kind="complete",
                        data={"final_state": completed_state, "last_seq": current_seq,
                              **({"canonical_available": False} if current_preview_source is not None and not canonical_available else {})},
                    )
                    return
                events, current_state, preview, canonical_available = await _read_loop_chunk(
                    current_seq, current_preview_source, current_preview_seq,
                )
                if await request.is_disconnected():
                    return
                if loop.time() - started_at >= max_connection_sec:
                    continue
                if preview["source"] != current_preview_source:
                    current_preview_source = preview["source"]
                    current_preview_seq = -1
                    yield _sse_format(event_kind="preview_reset", data={"source": current_preview_source})
                if preview["events"]:
                    seqs = [event["seq"] for event in preview["events"]]
                    if any(type(seq) is not int for seq in seqs) or seqs[0] <= current_preview_seq or any(
                        b <= a for a, b in pairwise(seqs)
                    ):
                        raise ValueError("live trajectory preview sequence did not advance")
                    yield _sse_format(event_kind="preview", data=preview)
                    current_preview_seq = seqs[-1]
                for ev in events:
                    # Sending a large backlog can itself exhaust the connection
                    # budget or outlive the client; do not wait for an empty page.
                    if await request.is_disconnected():
                        return
                    if loop.time() - started_at >= max_connection_sec:
                        yield _sse_format(
                            event_kind="reconnect",
                            data={"reason": "max_connection_sec", "last_seq": current_seq},
                        )
                        return
                    seq = ev["seq"]
                    if type(seq) is not int or seq <= current_seq:
                        raise ValueError("trajectory stream event sequence did not advance")
                    yield _sse_format(
                        event_kind=None,
                        data=ev,
                        event_id=str(seq),
                    )
                    current_seq = seq

                if events or preview["events"]:
                    # More pages may already be available, including after a
                    # terminal transition. Drain without a polling delay, while
                    # letting timers, cancellations and other requests run.
                    await asyncio.sleep(0)
                    continue

                if current_state in _TERMINAL_TRIAL_STATES:
                    # Only an empty read with terminal state proves the durable
                    # tail is drained. Recheck disconnect/deadline before sending
                    # completion in case the final read crossed either boundary.
                    completed_state = current_state
                    continue

                if loop.time() - started_at >= max_connection_sec:
                    continue

                # Wait for either: a NOTIFY matching our trial fires
                # (push), or `poll_interval` elapses (fallback for
                # missed notifies + terminal-state polling). The wake
                # event is cleared AFTER the wait so any NOTIFY that
                # lands while we were emitting events above isn't
                # lost — drain may have set wake during the yield
                # iterator's pause.
                if subscription is not None and subscription._push_mode:
                    try:
                        await asyncio.wait_for(
                            subscription.wake.wait(),
                            timeout=poll_interval,
                        )
                    except TimeoutError:
                        pass
                    subscription.wake.clear()
                else:
                    await asyncio.sleep(poll_interval)
        finally:
            if subscription is not None:
                await subscription.__aexit__(None, None, None)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            # Block proxy/CDN buffering — SSE needs every chunk
            # flushed immediately.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
