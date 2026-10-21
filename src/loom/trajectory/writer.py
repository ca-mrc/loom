"""TrajectoryWriter — async append-only JSONL writer with local-first durability
and MinIO multipart upload (spec §3.5).

Behaviour:
- Every append() first writes the JSONL line to a local file on the worker
  (durability survives MinIO outage).
- A flush trigger (size / event count / age) accumulates a chunk in memory
  and uploads it as a multipart part.
- On close (__aexit__), the final buffer flushes and the multipart upload
  completes — unless the with-block raised, in which case the upload aborts.
- Retry: up to 3 attempts per flush with exponential backoff; final failure
  raises TrajectoryFlushFailedError.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import TracebackType
from typing import Any

import aiofiles

from loom.errors import TrajectoryFlushFailedError
from loom.models.trajectory import EventKind, TrajectoryEvent
from loom.trajectory.cp_event_sink import CpEventSink
from loom.trajectory.storage import MultipartUpload, ObjectStore

S3_MIN_PART_BYTES = 5 * 1024 * 1024          # S3/MinIO multipart floor (5 MiB)
DEFAULT_FLUSH_BYTES = 8 * 1024 * 1024        # 8 MB (comfortably above floor)
DEFAULT_FLUSH_EVENT_COUNT = 1000
DEFAULT_FLUSH_SEC = 30.0
DEFAULT_FLUSH_RETRIES = 3


class TrajectoryWriter:
    """Owns one trial's trajectory stream."""

    def __init__(
        self,
        *,
        local_path: Path,
        store: ObjectStore,
        bucket: str,
        key: str,
        flush_bytes: int = DEFAULT_FLUSH_BYTES,
        flush_event_count: int = DEFAULT_FLUSH_EVENT_COUNT,
        flush_sec: float = DEFAULT_FLUSH_SEC,
        flush_retries: int = DEFAULT_FLUSH_RETRIES,
        min_part_bytes: int = S3_MIN_PART_BYTES,
        # #5 Slice 3b: optional CP-side dual-write observer. When set,
        # every appended event is mirrored to the `trial_events` table
        # via the sink, with its own (smaller) flush cadence. MinIO
        # remains authoritative in Slice 3b; Slice 3c flips the SSE
        # reader to Postgres, at which point sink failures start
        # mattering more. The sink swallows all errors internally.
        cp_event_sink: CpEventSink | None = None,
    ) -> None:
        self._local_path = local_path
        self._store = store
        self._bucket = bucket
        self._key = key
        self._flush_bytes = flush_bytes
        self._flush_event_count = flush_event_count
        self._flush_sec = flush_sec
        self._flush_retries = flush_retries
        # S3/MinIO multipart floor — every non-final part must clear this.
        # The final flush at close() ignores it (last-part has no minimum).
        self._min_part_bytes = min_part_bytes
        self._cp_event_sink = cp_event_sink
        self._append_lock = asyncio.Lock()

        self._buf: list[bytes] = []
        self._buf_bytes = 0
        self._last_flush_at = time.monotonic()
        self._upload: MultipartUpload | None = None
        self._part_number = 0
        self._local_file: Any | None = None  # aiofiles handle
        self._closed = False
        self._remote_committed = False
        self._remote_version_id: str | None = None
        self.parts_uploaded = 0
        self._next_seq = 0
        # #1186: agents that emit gateway LLMCallEvents (terminus-2) need
        # this count before Trial terminal-state selection.
        self._llm_call_event_count = 0

    @property
    def remote_uri(self) -> str:
        return f"s3://{self._bucket}/{self._key}"

    @property
    def local_path(self) -> Path:
        return self._local_path

    @property
    def remote_committed(self) -> bool:
        """Whether the exact remote object completed successfully."""
        return self._remote_committed

    @property
    def remote_version_id(self) -> str | None:
        """Exact object version returned by multipart completion, when any."""
        return self._remote_version_id

    @property
    def llm_call_event_count(self) -> int:
        return self._llm_call_event_count

    async def __aenter__(self) -> TrajectoryWriter:
        self._local_path.parent.mkdir(parents=True, exist_ok=True)
        self._local_file = await aiofiles.open(self._local_path, "wb")
        self._upload = await self._store.create_multipart_upload(
            bucket=self._bucket, key=self._key,
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self._close(error=exc is not None)

    async def append(self, event: TrajectoryEvent) -> None:
        if self._closed:
            raise RuntimeError("append after close")
        async with self._append_lock:
            event = event.model_copy(update={"seq": self._assign_seq(event.seq)})
            if event.kind == EventKind.LLM_CALL:
                self._llm_call_event_count += 1
            line = event.model_dump_json().encode("utf-8") + b"\n"
            await self._write_line(line)
            if self._cp_event_sink is not None:
                await self._cp_event_sink.observe(event)

    async def write_raw_dict(self, data: dict[str, object]) -> None:
        """Append a pre-shaped dict as one JSONL line WITHOUT pydantic
        validation. Used by `SubprocessAgent` to forward
        adapter-emitted events whose shape is the adapter's contract,
        not Loom's `TrajectoryEvent` discriminated union. Adapters are
        responsible for emitting valid shapes; typed callers use append().
        """
        if self._closed:
            raise RuntimeError("append after close")
        import json
        payload = dict(data)
        raw_seq = payload.get("seq")
        if isinstance(raw_seq, int):
            payload["seq"] = self._assign_seq(raw_seq)
        if payload.get("kind") == EventKind.LLM_CALL.value:
            self._llm_call_event_count += 1
        line = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
        await self._write_line(line)
        # #5 Slice 3b: mirror to CP sink. observe_raw skips payloads
        # without int seq + str kind (subprocess adapters that pre-date
        # the typed envelope), so a malformed adapter emit doesn't
        # break the sink.
        if self._cp_event_sink is not None:
            await self._cp_event_sink.observe_raw(payload)

    def _assign_seq(self, proposed: int) -> int:
        """Keep the persisted per-trial event sequence globally monotonic.

        Older call sites used local counters per trial section, step, or
        subprocess adapter. The CP event table de-duplicates by `(trial_id, seq)`,
        so repeated local seqs silently dropped later events. The writer is the
        single append boundary and normalizes those local counters into the
        canonical trial-wide sequence before local JSONL, MinIO, and CP mirror
        writes see the event.
        """

        assigned = proposed if proposed >= self._next_seq else self._next_seq
        self._next_seq = assigned + 1
        return assigned

    async def _write_line(self, line: bytes) -> None:
        assert self._local_file is not None
        await self._local_file.write(line)
        await self._local_file.flush()
        self._buf.append(line)
        self._buf_bytes += len(line)
        if self._should_flush():
            await self._flush()

    def _should_flush(self) -> bool:
        # Mid-trial flushes become non-final multipart parts; S3/MinIO rejects
        # any non-final part below ~5 MiB. Gate every trigger behind that
        # floor — close() drains the remainder as the final part regardless
        # of size.
        if self._buf_bytes < self._min_part_bytes:
            return False
        if self._buf_bytes >= self._flush_bytes:
            return True
        if len(self._buf) >= self._flush_event_count:
            return True
        if time.monotonic() - self._last_flush_at >= self._flush_sec:
            return True
        return False

    async def _flush(self) -> None:
        if not self._buf:
            return
        assert self._upload is not None
        chunk = b"".join(self._buf)
        self._part_number += 1
        last_exc: Exception | None = None
        delay = 0.5
        for _ in range(self._flush_retries):
            try:
                await self._store.upload_part(
                    self._upload, part_number=self._part_number, body=chunk,
                )
                self.parts_uploaded += 1
                self._buf.clear()
                self._buf_bytes = 0
                self._last_flush_at = time.monotonic()
                return
            except Exception as exc:
                last_exc = exc
                await asyncio.sleep(delay)
                delay *= 2
        raise TrajectoryFlushFailedError(
            f"flush to {self.remote_uri} failed after {self._flush_retries} retries: "
            f"{last_exc!r}",
        )

    async def _close(self, *, error: bool) -> None:
        if self._closed:
            return
        self._closed = True
        # Capture a final-flush failure so we can abort cleanly THEN re-raise.
        # Silently swallowing it would make the caller's `async with writer:`
        # return normally even though MinIO has nothing.
        final_flush_exc: TrajectoryFlushFailedError | None = None
        try:
            if self._local_file is not None:
                if self._buf and not error:
                    try:
                        await self._flush()
                    except TrajectoryFlushFailedError as exc:
                        final_flush_exc = exc
                        error = True
                await self._local_file.close()
        finally:
            self._local_file = None
            if self._upload is not None:
                if error or self.parts_uploaded == 0:
                    try:
                        await self._store.abort_multipart_upload(self._upload)
                    except Exception:
                        pass
                else:
                    try:
                        write_result = (
                            await self._store.complete_multipart_upload_with_metadata(
                                self._upload,
                            )
                        )
                        self._remote_version_id = write_result.version_id
                        self._remote_committed = True
                    except Exception as exc:
                        self._upload = None
                        raise TrajectoryFlushFailedError(
                            f"complete multipart failed: {exc!r}",
                        ) from exc
                self._upload = None
            # #5 Slice 3b: drain the CP sink after MinIO is committed
            # (or aborted). The sink swallows its own errors so a
            # failure here never overwrites `final_flush_exc`.
            if self._cp_event_sink is not None:
                try:
                    await self._cp_event_sink.flush_and_close()
                except Exception:
                    # Defensive — sink internally swallows, but if
                    # anything escapes don't blow up trial close.
                    pass
        if final_flush_exc is not None:
            raise final_flush_exc
