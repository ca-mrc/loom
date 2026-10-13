"""Measure real botocore HTTP sends against an owned local transport."""
from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections import deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import UUID

import pytest

from loom.trajectory.storage import MinioObjectStore, MultipartUpload

LEASE = UUID("10000000-0000-0000-0000-000000000001")
SESSION = UUID("20000000-0000-0000-0000-000000000002")


@contextmanager
def s3_transport(statuses=(), block=False):
    """Independent fixture intervals/bytes, without stubbing the SDK's hooks."""
    replies = deque(statuses)
    entered = threading.Event()
    release = threading.Event()
    bodies = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_PUT(self):  # noqa: N802
            body = self.rfile.read(int(self.headers["Content-Length"]))
            with lock:
                bodies.append(body)
                status = replies.popleft() if replies else 200
            entered.set()
            if block:
                assert release.wait(5)
            payload = b"<Error><Code>SlowDown</Code></Error>" if status == 503 else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("ETag", '"owned-etag"')
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    store = MinioObjectStore(
        endpoint_url=f"http://127.0.0.1:{server.server_port}",
        access_key="SECRET-ACCESS", secret_key="SECRET-KEY", operation_timeout=4,
    )
    try:
        yield store, bodies, entered, release
    finally:
        release.set()
        store.close()
        server.shutdown()
        server.server_close()
        thread.join(2)


def records(caplog, stage=None, event=None):
    values = [json.loads(r.message) for r in caplog.records if r.name == "loom.upload_observation"]
    return [v for v in values if (stage is None or v["stage"] == stage)
            and (event is None or v["event"] == event)]


async def upload(store):
    from loom.trajectory.upload_observation import observe_upload

    with observe_upload("source", lease_id=LEASE, generation=7, session_id=SESSION):
        await store.put_object(bucket="private-bucket", key="SECRET-OBJECT", body=b"PRIVATE-PAYLOAD")


@pytest.mark.parametrize("overlap", [True, False])
async def test_transport_intervals_distinguish_overlap_and_preserve_bytes(caplog, overlap):
    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    with s3_transport(block=overlap) as (store, bodies, entered, release):
        if overlap:
            tasks = [asyncio.create_task(upload(store)) for _ in range(2)]
            try:
                for _ in range(200):
                    if len(bodies) == 2:
                        break
                    await asyncio.sleep(0.01)
                assert len(bodies) == 2
                assert len(records(caplog, "s3_transport", "started")) == 2
                assert not records(caplog, "s3_transport", "finished")
            finally:
                release.set()
                await asyncio.gather(*tasks)
        else:
            await upload(store)
            await upload(store)
        assert bodies == [b"PRIVATE-PAYLOAD"] * 2
    events = records(caplog, "s3_transport")
    starts = [e for e in events if e["event"] == "started"]
    ends = [e for e in events if e["event"] == "finished"]
    assert len(starts) == len(ends) == 2
    intervals = [(s["monotonic_ns"], next(e["monotonic_ns"] for e in ends if e["span_id"] == s["span_id"])) for s in starts]
    assert (max(s for s, _ in intervals) < min(e for _, e in intervals)) == overlap
    assert all(e["outcome"] == "success" and e["status_code"] == 200 for e in ends)
    assert all(e["declared_bytes"] == len(b"PRIVATE-PAYLOAD") for e in starts)
    assert all(e["lease_id"] == str(LEASE) and e["generation"] == 7 for e in events)
    serialized = json.dumps(records(caplog))
    for secret in ("SECRET", "PRIVATE-PAYLOAD", "private-bucket", "127.0.0.1", "Authorization"):
        assert secret not in serialized


async def test_sdk_retry_has_two_transport_attempts_inside_one_worker(caplog):
    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    with s3_transport(statuses=[503, 200]) as (store, bodies, _, _):
        await upload(store)
        assert bodies == [b"PRIVATE-PAYLOAD"] * 2
    workers = records(caplog, "sdk_worker", "started")
    transports = records(caplog, "s3_transport", "finished")
    assert len(workers) == 1 and len(transports) == 2
    assert [e["status_code"] for e in transports] == [503, 200]
    assert {e["parent_span_id"] for e in transports} == {workers[0]["span_id"]}
    assert [e["attempt"] for e in transports] == [1, 2]


@pytest.mark.parametrize("cancel", [True, False], ids=["cancel", "timeout"])
async def test_abandoned_wait_does_not_finish_live_worker_or_transport(caplog, cancel):
    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    with s3_transport(block=True) as (store, _, entered, release):
        store._operation_timeout = 3 if cancel else 0.1
        store._operation_attempts = 1
        task = asyncio.create_task(upload(store))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            if cancel:
                task.cancel()
            with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
                await task
            abandoned = records(caplog, "sdk_worker", "wait_abandoned")
            assert len(abandoned) == 1 and abandoned[0]["completion_state"] == "running"
            assert not records(caplog, "sdk_worker", "finished")
            assert not records(caplog, "s3_transport", "finished")
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            for _ in range(200):
                if records(caplog, "sdk_worker", "finished"):
                    break
                await asyncio.sleep(0.01)
        assert len(records(caplog, "sdk_worker", "finished")) == 1
        assert len(records(caplog, "s3_transport", "finished")) == 1


async def test_spooling_is_separate_and_observations_are_scoped(caplog):
    from loom.trajectory.upload_observation import observe_upload

    caplog.set_level(logging.INFO, logger="loom.upload_observation")
    with s3_transport() as (store, bodies, _, _):
        async def chunks():
            yield b"first"
            await asyncio.sleep(0)
            yield b"second"

        with observe_upload("source", lease_id=LEASE, generation=1, session_id=SESSION):
            await store.upload_part_stream(
                MultipartUpload(bucket="b", key="private", upload_id="secret-grant"),
                part_number=1, body=chunks(),
            )
        assert bodies == [b"firstsecond"]
        count = len(records(caplog))
        await store.put_object(bucket="b", key="other", body=b"not observed")
        assert len(records(caplog)) == count
    spool_end, = records(caplog, "spool", "finished")
    transport_start, = records(caplog, "s3_transport", "started")
    assert spool_end["observed_bytes"] == 11
    assert spool_end["monotonic_ns"] < transport_start["monotonic_ns"]
