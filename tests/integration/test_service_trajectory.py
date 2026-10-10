"""Trajectory paginated read + download route (Plan 18 Task 4).

`traj_setup` lives in `tests/integration/conftest.py` so both
trajectory + ATIF tests share it (and the underlying MinIO container).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall, Task, Trial


async def test_trajectory_paginates(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["events"]) == 2
        assert j1["events"][0]["kind"] == "trial_start"
        assert j1["next_cursor"] == 2

        r2 = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory?limit=10&"
            f"cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j2 = r2.json()
    assert len(j2["events"]) == 3
    assert j2["next_cursor"] is None


async def test_trajectory_unknown_trial_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/trajectory",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_trajectory_object_missing_returns_empty_page(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """A trial row exists but the trajectory object was never written
    (queued/just-claimed, or crashed pre-first-event); we return an
    empty page so the SPA shows "no events yet" rather than a 404."""
    app, raw, team_id, _trial_id = traj_setup
    bare_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_row_id = s.execute(
            select(Task.id).limit(1),
        ).scalar_one()
        s.execute(insert(Trial).values(
            id=bare_trial, task_id=task_row_id, team_id=team_id,
            state="queued", config={}, requires_caps={},
            submitted_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{bare_trial}/trajectory",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json() == {"events": [], "next_cursor": None}


async def test_trajectory_download_proxies_object_through_service(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert b'"kind": "trial_start"' in r.content


async def test_trajectory_download_uses_attempt_scoped_persisted_uri(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_id = traj_setup
    key = f"{team_id}/{trial_id}/attempts/2/events.jsonl"
    attempt_body = (
        json.dumps({
            "kind": "trial_end",
            "trial_id": str(trial_id),
            "seq": 0,
            "marker": "attempt-two",
        }) + "\n"
    ).encode()
    app.state.minio_client.put_object(
        Bucket=app.state.settings.trajectories_bucket,
        Key=key,
        Body=attempt_body,
    )
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(trajectory_index={
                "trajectory_uri": (
                    f"s3://{app.state.settings.trajectories_bucket}/{key}"
                ),
            })
        )
    engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        response = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 200, response.text
    assert response.content == attempt_body


async def test_trajectory_download_rejects_cross_bucket_persisted_uri(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_id = traj_setup
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(trajectory_index={
                "trajectory_uri": (
                    f"s3://foreign/{team_id}/{trial_id}/attempts/2/events.jsonl"
                ),
            })
        )
    engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        response = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 409


async def test_trajectory_download_falls_back_to_postgres_events(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, _trial_id = traj_setup
    postgres_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_row_id = s.execute(
            select(Task.id).limit(1),
        ).scalar_one()
        s.execute(insert(Trial).values(
            id=postgres_trial,
            task_id=task_row_id,
            team_id=team_id,
            state="succeeded",
            config={},
            requires_caps={},
            result={"aggregate_reward": 1.0},
            submitted_at=datetime.now(UTC),
            started_at=datetime.now(UTC),
            finished_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    _seed_trial_events_postgres(postgres_url, postgres_trial, [
        {
            "seq": 0,
            "kind": "trial_start",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "seq": 0,
                "kind": "trial_start",
                "marker": "postgres-download",
            },
        },
        {
            "seq": 1,
            "kind": "trial_end",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "seq": 1,
                "kind": "trial_end",
                "marker": "postgres-download",
            },
        },
    ])

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{postgres_trial}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/x-ndjson")
    lines = [json.loads(line) for line in r.text.splitlines()]
    assert [line["seq"] for line in lines] == [0, 1]
    assert all(line["marker"] == "postgres-download" for line in lines)


async def test_trajectory_download_enriches_sparse_postgres_events(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, _trial_id = traj_setup
    postgres_trial = uuid4()
    now = datetime.now(UTC)
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_row_id = s.execute(
            select(Task.id).limit(1),
        ).scalar_one()
        s.execute(insert(Trial).values(
            id=postgres_trial,
            task_id=task_row_id,
            team_id=team_id,
            state="succeeded",
            config={},
            requires_caps={},
            result={
                "aggregate_reward": 0.75,
                "task_id": task_row_id,
                "agent": {
                    "name": "opencode",
                    "version": "1.0",
                },
            },
            submitted_at=now,
            started_at=now,
            finished_at=now,
        ))
        s.execute(insert(LlmCall).values(
            team_id=team_id,
            trial_id=postgres_trial,
            step_id="main",
            dialect="openai_facade",
            model="glm5.1-thinking",
            input_tokens=101,
            output_tokens=17,
            provider_extras={
                "reasoning_tokens": 9,
                "ignored_text": "not persisted into the trajectory event",
            },
            request_params={
                "status": "available",
                "parameters": {"temperature": 0},
            },
            cost_usd=Decimal("0.123456"),
            rate_card_hash="facade:test-card",
            captured_at=now,
            attempt=2,
        ))
        s.commit()
    sync_engine.dispose()
    _seed_trial_events_postgres(postgres_url, postgres_trial, [
        {
            "seq": 0,
            "kind": "trial_start",
            "source": "worker",
            "schema_version": 1,
            "payload": {
                "emitted_at": now.isoformat(),
                "trial_id": str(postgres_trial),
                "step_id": "__trial__",
                "seq": 0,
                "kind": "trial_start",
                "task_id": task_row_id,
                "agent_name": "opencode",
                "agent_mode": "out-of-box",
            },
        },
    ])

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{postgres_trial}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    lines = [json.loads(line) for line in r.text.splitlines()]
    assert [line["seq"] for line in lines] == [0, 1, 2]
    assert [line["kind"] for line in lines] == [
        "trial_start",
        "llm_call",
        "trial_end",
    ]
    llm_call = lines[1]
    assert llm_call["model"] == {
        "provider": "openai",
        "name": "glm5.1-thinking",
        "source": "api",
        "local_server": None,
        "hf_execution": "local-vllm",
        "tier": None,
        "region": None,
        "max_input_tokens": None,
        "max_output_tokens": None,
    }
    assert llm_call["input_tokens"] == 101
    assert llm_call["output_tokens"] == 17
    assert llm_call["thinking_tokens"] == 9
    assert llm_call["provider_extras"] == {"reasoning_tokens": 9}
    assert llm_call["request_params"] == {
        "status": "available",
        "parameters": {"temperature": 0},
    }
    assert llm_call["attempt"] == 2
    assert lines[2]["final_state"] == "succeeded"
    assert lines[2]["reward"] == {"aggregate_reward": 0.75}


async def test_artifact_download_proxies_object_through_service(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_id = traj_setup
    settings = app.state.settings
    artifact_key = f"{team_id}/{trial_id}/main/result.txt"
    existing = {
        bucket["Name"]
        for bucket in app.state.minio_client.list_buckets()["Buckets"]
    }
    if settings.artifacts_bucket not in existing:
        app.state.minio_client.create_bucket(Bucket=settings.artifacts_bucket)
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=artifact_key,
        Body=b"hello artifact",
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": settings.artifacts_bucket,
                            "key": artifact_key,
                            "size": 14,
                        }
                    ],
                },
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/artifacts/download",
            params={"key": artifact_key},
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.content == b"hello artifact"


# ──────────────────────────────────────────────────────────────────────
# #5 Slice 1: seq-cursor events endpoint
# ──────────────────────────────────────────────────────────────────────


async def test_events_default_returns_all(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`after_seq=-1` (the default) returns every event in seq order.
    Mirrors the legacy /trajectory list but keys on the event payload's
    `seq` field instead of a line cursor — forward-compatible naming
    for the upcoming Postgres event table (#5 Phase 2)."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    seqs = [e["seq"] for e in body["events"]]
    assert seqs == [0, 1, 2, 3, 4]
    assert body["next_after_seq"] == 4


async def test_events_after_seq_skips_already_seen(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`after_seq=N` returns events with seq strictly greater than N,
    so the client can resume a stream without re-emitting events it
    already processed."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?after_seq=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert [e["seq"] for e in body["events"]] == [3, 4]
    assert body["next_after_seq"] == 4


async def test_events_after_terminal_seq_returns_empty(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`after_seq` larger than any event's seq returns an empty list
    and `next_after_seq=null` — the standard "no new events, poll
    again" response shape."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?after_seq=999",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body == {"events": [], "next_after_seq": None}


async def test_events_limit_respected(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """`limit=N` returns at most N events; client resumes via the
    returned `next_after_seq`."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert [e["seq"] for e in body["events"]] == [0, 1]
    assert body["next_after_seq"] == 1


async def test_events_unknown_trial_returns_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/events",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# #5 Slice 1: SSE stream
# ──────────────────────────────────────────────────────────────────────


def _parse_sse(body: str) -> list[dict[str, str]]:
    """Tiny SSE parser — splits on blank lines, returns each message
    as a dict of header → value (data, event, id, comment)."""
    messages: list[dict[str, str]] = []
    for block in body.split("\n\n"):
        block = block.strip("\n")
        if not block:
            continue
        msg: dict[str, str] = {}
        for line in block.split("\n"):
            if line.startswith(":"):
                msg.setdefault("comment", line[1:].strip())
                continue
            if ":" not in line:
                continue
            field, _, value = line.partition(":")
            value = value.lstrip(" ")
            if field in msg:
                msg[field] += "\n" + value
            else:
                msg[field] = value
        messages.append(msg)
    return messages


async def test_stream_emits_replay_then_completes_on_terminal(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """Trial is already in a terminal state (succeeded — seeded by the
    fixture), so the SSE stream emits every event from MinIO and then
    a `complete` event, closing cleanly."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        timeout=10.0,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/stream",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/event-stream")
    assert r.headers["cache-control"] == "no-cache"
    assert r.headers["x-accel-buffering"] == "no"

    messages = _parse_sse(r.text)
    # First message is the "stream open" comment.
    assert messages[0].get("comment") == "stream open"
    # Then 5 data messages (one per seeded event), then complete.
    data_messages = [m for m in messages if "data" in m and m.get("event") != "complete"]
    assert len(data_messages) == 5
    parsed = [json.loads(m["data"]) for m in data_messages]
    assert [e["seq"] for e in parsed] == [0, 1, 2, 3, 4]
    # Each data message carries the SSE `id` field for client reconnect.
    assert [m["id"] for m in data_messages] == ["0", "1", "2", "3", "4"]
    # Final message is the `complete` event with last_seq.
    complete = next(m for m in messages if m.get("event") == "complete")
    payload = json.loads(complete["data"])
    assert payload["final_state"] == "succeeded"
    assert payload["last_seq"] == 4


async def test_stream_resumes_from_after_seq(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """Reconnecting clients pass `after_seq=N`; stream emits only
    events with seq > N (standard SSE Last-Event-ID semantics)."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        timeout=10.0,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/stream?after_seq=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    messages = _parse_sse(r.text)
    data_messages = [
        m for m in messages
        if "data" in m and m.get("event") != "complete"
    ]
    parsed = [json.loads(m["data"]) for m in data_messages]
    assert [e["seq"] for e in parsed] == [3, 4]


async def test_stream_unknown_trial_returns_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/stream",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


# ──────────────────────────────────────────────────────────────────────
# #5 Slice 3c: /events reader prefers Postgres, falls back to MinIO
# ──────────────────────────────────────────────────────────────────────


def _seed_trial_events_postgres(
    postgres_url: str, trial_id: UUID,
    rows: list[dict[str, object]],
) -> None:
    """Insert rows into the `trial_events` table directly. The traj_setup
    fixture only seeds MinIO; tests that want to verify the Postgres-
    primary path need a way to bypass the worker's normal write path."""
    from sqlalchemy import insert as sa_insert

    from loom.db.schema import TrialEvent
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for row in rows:
            s.execute(sa_insert(TrialEvent).values(
                trial_id=trial_id, **row,
            ))
        s.commit()
    sync_engine.dispose()


async def test_events_reads_from_postgres_when_present(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """Slice 3c primary path: when `trial_events` has rows for the
    trial, the endpoint serves from Postgres — not MinIO. Use a
    distinct payload that doesn't appear in the fixture's MinIO
    seed so the test can tell which source served the response."""
    app, raw, _team_id, trial_id = traj_setup
    _seed_trial_events_postgres(postgres_url, trial_id, [
        {
            "seq": 100, "kind": "step_start", "source": "worker",
            "schema_version": 1,
            "payload": {
                "seq": 100, "kind": "step_start",
                "marker": "from-postgres-not-minio",
            },
        },
        {
            "seq": 101, "kind": "step_end", "source": "worker",
            "schema_version": 1,
            "payload": {
                "seq": 101, "kind": "step_end",
                "marker": "from-postgres-not-minio",
            },
        },
    ])
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events?after_seq=99",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert [e["seq"] for e in body["events"]] == [100, 101]
    # Marker confirms the Postgres rows served the response, not the
    # MinIO seed (which contains seqs 0..4 with no `marker` field).
    assert all(
        e.get("marker") == "from-postgres-not-minio"
        for e in body["events"]
    )
    assert body["next_after_seq"] == 101


async def test_events_falls_back_to_minio_for_legacy_trial(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    """Trials whose worker shipped before Slice 3b have 0 rows in
    `trial_events` but a populated MinIO trajectory. The fixture's
    seed is exactly that shape — no Postgres rows, but a complete
    MinIO events.jsonl. The endpoint must fall back transparently
    so legacy trials' UX isn't broken by the cutover."""
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    # MinIO seed: 5 events with seqs 0..4.
    assert [e["seq"] for e in body["events"]] == [0, 1, 2, 3, 4]
    assert body["next_after_seq"] == 4


async def test_events_minio_fallback_uses_attempt_scoped_persisted_uri(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_id = traj_setup
    key = f"{team_id}/{trial_id}/attempts/3/events.jsonl"
    event = {
        "kind": "trial_end",
        "trial_id": str(trial_id),
        "seq": 9,
        "marker": "attempt-three",
    }
    app.state.minio_client.put_object(
        Bucket=app.state.settings.trajectories_bucket,
        Key=key,
        Body=(json.dumps(event) + "\n").encode(),
    )
    engine = create_engine(postgres_url)
    with engine.begin() as connection:
        connection.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(trajectory_index={
                "trajectory_uri": (
                    f"s3://{app.state.settings.trajectories_bucket}/{key}"
                ),
            })
        )
    engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        response = await ac.get(
            f"/api/v1/trials/{trial_id}/events",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["events"] == [event]


async def test_stream_serves_postgres_events_for_terminal_trial(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """End-to-end: terminal trial with Postgres-backed events streams
    them via SSE without touching MinIO. Verifies the Slice-3c
    flip applies to /stream too, not just /events."""
    app, raw, _team_id, trial_id = traj_setup
    _seed_trial_events_postgres(postgres_url, trial_id, [
        {
            "seq": 200, "kind": "step_start", "source": "worker",
            "schema_version": 1,
            "payload": {
                "seq": 200, "kind": "step_start",
                "marker": "stream-from-postgres",
            },
        },
    ])
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc", timeout=10.0,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/stream?after_seq=199",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    messages = _parse_sse(r.text)
    data_messages = [
        m for m in messages
        if "data" in m and m.get("event") != "complete"
    ]
    # Just the seq=200 row from Postgres (the MinIO seed has seqs
    # 0..4 which are below the after_seq=199 cutoff).
    parsed = [json.loads(m["data"]) for m in data_messages]
    assert [e["seq"] for e in parsed] == [200]
    assert parsed[0].get("marker") == "stream-from-postgres"


@pytest.mark.parametrize("source", ["postgres", "minio"])
@pytest.mark.parametrize(("count", "after_seq"), [
    (0, 499), (200, 499), (400, 499), (401, 499), (1000, 499),
    (1000, 699), (1000, 1499),
])
async def test_terminal_stream_drains_every_page_before_complete(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
    source: str,
    count: int,
    after_seq: int,
) -> None:
    """Completed archives must not lose the third page or a resumed tail."""
    app, raw, team_id, trial_id = traj_setup
    events = [
        {"seq": seq, "kind": "env_exec", "marker": source}
        for seq in range(500, 500 + count)
    ]
    if source == "postgres":
        _seed_trial_events_postgres(postgres_url, trial_id, [
            {"seq": event["seq"], "kind": "env_exec", "source": "worker",
             "schema_version": 1, "payload": event}
            for event in events
        ])
    else:
        app.state.minio_client.put_object(
            Bucket=app.state.settings.trajectories_bucket,
            Key=f"{team_id}/{trial_id}/events.jsonl",
            Body="".join(json.dumps(event) + "\n" for event in events).encode(),
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        response = await client.get(
            f"/api/v1/trials/{trial_id}/stream?after_seq={after_seq}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert response.status_code == 200, response.text
    messages = _parse_sse(response.text)
    delivered = [json.loads(message["data"]) for message in messages
                 if "data" in message and message.get("event") != "complete"]
    expected_seqs = list(range(after_seq + 1, 500 + count))
    assert [event["seq"] for event in delivered] == expected_seqs
    assert all(event["marker"] == source for event in delivered)
    completions = [json.loads(message["data"]) for message in messages
                   if message.get("event") == "complete"]
    assert completions == [{
        "final_state": "succeeded", "last_seq": expected_seqs[-1] if expected_seqs else after_seq,
    }]
    assert messages[-1]["event"] == "complete"


# ──────────────────────────────────────────────────────────────────────
# #5 Slice 3e: SSE inner loop wakes on NOTIFY (LISTEN consumer)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("tail_count", [1, 601])
async def test_stream_wakes_on_listen_notify_mid_run(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
    tail_count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end LISTEN/NOTIFY. Open the SSE stream against a
    running trial that already has its initial-replay row in
    Postgres. Mid-stream, a side task inserts a new event row +
    flips the trial to `succeeded`. The route's LISTEN consumer
    must wake on migration 0041's NOTIFY so the new row lands
    before the deterministic terminal-state path closes the
    stream — proves the push path actually fires."""
    import asyncio as asyncio_local

    from sqlalchemy import update as sa_update

    from loom.db.schema import Trial, TrialEvent
    from loom_service.routes import trajectory

    app, raw, team_id, completed_trial_id = traj_setup

    # Use a fresh running trial: the shared fixture is already terminal and
    # its evidence must not be reopened to simulate an active stream.
    trial_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_id = s.execute(
            select(Trial.task_id).where(Trial.id == completed_trial_id),
        ).scalar_one()
        s.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                state="running",
                config={},
                requires_caps={},
                result={},
            ),
        )
        s.commit()
    sync_engine.dispose()

    # Initial-replay row (seq=300) so the opening Postgres read has
    # something to emit; subsequent push has to come from NOTIFY.
    _seed_trial_events_postgres(postgres_url, trial_id, [
        {
            "seq": 300, "kind": "step_start", "source": "worker",
            "schema_version": 1,
            "payload": {
                "seq": 300, "kind": "step_start",
                "marker": "initial-replay",
            },
        },
    ])

    replay_drained = asyncio_local.Event()
    read_events = trajectory._read_events_from_postgres

    async def observe_initial_replay(*args, **kwargs):
        # Observe the actual database query without replacing its result. The
        # final commit must follow a running-state read, even on a slow runner.
        events = await read_events(*args, **kwargs)
        if kwargs["trial_id"] == trial_id and kwargs["after_seq"] == 300 and not events:
            replay_drained.set()
        return events

    monkeypatch.setattr(trajectory, "_read_events_from_postgres", observe_initial_replay)
    monkeypatch.setattr(trajectory, "_DEFAULT_SSE_POLL_INTERVAL_SEC", 30.0)

    async def insert_then_terminate() -> None:
        """Commit the final events after initial replay, waking the LISTEN reader."""
        await replay_drained.wait()
        sync = create_engine(postgres_url)
        slx = sessionmaker(sync)
        with slx() as s:
            # Commit the final event batch and terminal state together, as native
            # materialization does. LISTEN wakes only after all rows are durable.
            s.execute(insert(TrialEvent), [
                {
                    "trial_id": trial_id,
                    "seq": seq, "kind": "step_end", "source": "worker",
                    "schema_version": 1,
                    "payload": {
                        "seq": seq, "kind": "step_end",
                        "marker": "fired-by-notify",
                    },
                } for seq in range(301, 301 + tail_count)
            ])
            s.execute(
                sa_update(Trial)
                .where(Trial.id == trial_id)
                .values(state="succeeded"),
            )
            s.commit()
        sync.dispose()

    transport = httpx.ASGITransport(app=app)
    async with asyncio_local.TaskGroup() as tasks:
        tasks.create_task(insert_then_terminate())
        async with httpx.AsyncClient(
            transport=transport, base_url="http://svc", timeout=10.0,
        ) as ac:
            # ASGITransport has no network read timeout; bound the coroutine itself.
            # A fallback-only reader waits 30 seconds and cannot pass this check.
            r = await asyncio_local.wait_for(
                ac.get(
                    f"/api/v1/trials/{trial_id}/stream?after_seq=299",
                    headers={"Authorization": f"Bearer {raw}"},
                ),
                timeout=10.0,
            )

    assert r.status_code == 200
    messages = _parse_sse(r.text)
    data_messages = [
        m for m in messages
        if "data" in m and m.get("event") != "complete"
    ]
    parsed = [json.loads(m["data"]) for m in data_messages]
    seqs = [e["seq"] for e in parsed]
    # The opening event and the entire atomically committed terminal tail land.
    assert seqs == list(range(300, 301 + tail_count))
    assert parsed[1].get("marker") == "fired-by-notify"
    # Final state event present.
    complete = next(m for m in messages if m.get("event") == "complete")
    payload = json.loads(complete["data"])
    assert payload["final_state"] == "succeeded"
    assert payload["last_seq"] == 300 + tail_count
