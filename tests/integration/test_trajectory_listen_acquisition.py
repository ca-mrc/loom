"""Real-connection ownership when trajectory LISTEN setup only partly succeeds."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import psycopg
import pytest
from fastapi import FastAPI

from loom_service.routes import trajectory


@pytest.fixture
async def listen_connections(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[list[psycopg.AsyncConnection[Any]]]:
    """Observe real autocommit connections, then clean leaks even on RED runs."""
    connections: list[psycopg.AsyncConnection[Any]] = []
    connect = psycopg.AsyncConnection.connect
    close = psycopg.AsyncConnection.close

    async def observe_connect(*args: Any, **kwargs: Any) -> psycopg.AsyncConnection[Any]:
        connection = await connect(*args, **kwargs)
        if kwargs.get("autocommit") is True:
            connections.append(connection)
        return connection

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", observe_connect)
    try:
        yield connections
    finally:
        for connection in connections:
            await close(connection)


async def test_listen_registration_failure_closes_acquired_connection(
    postgres_url: str,
    listen_connections: list[psycopg.AsyncConnection[Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(trajectory, "_LISTEN_CHANNEL", "invalid-channel")
    subscription = trajectory._ListenSubscription(
        trajectory._sqla_url_to_psycopg_dsn(postgres_url), uuid4(),
    )

    with pytest.raises(psycopg.errors.SyntaxError) as error:
        async with subscription:
            pytest.fail("invalid LISTEN must fail before entering the body")

    assert error.value.sqlstate == "42601"
    assert len(listen_connections) == 1
    assert listen_connections[0].closed


@pytest.mark.parametrize("close_raises", [False, True])
async def test_probe_failure_closes_connection_and_preserves_original_error(
    postgres_url: str,
    listen_connections: list[psycopg.AsyncConnection[Any]],
    monkeypatch: pytest.MonkeyPatch,
    close_raises: bool,
) -> None:
    errors: list[psycopg.Error] = []

    async def failing_probe(
        connection: psycopg.AsyncConnection[Any], *, timeout_sec: float,
    ) -> bool:
        try:
            await connection.execute("SELECT 1 / 0")
        except psycopg.Error as error:
            errors.append(error)
            raise
        return True

    if close_raises:
        close = psycopg.AsyncConnection.close

        async def close_then_raise(connection: psycopg.AsyncConnection[Any]) -> None:
            await close(connection)
            raise RuntimeError("cleanup error must not replace the probe failure")

        monkeypatch.setattr(psycopg.AsyncConnection, "close", close_then_raise)
    monkeypatch.setattr(trajectory, "notify_round_trip", failing_probe)
    subscription = trajectory._ListenSubscription(
        trajectory._sqla_url_to_psycopg_dsn(postgres_url), uuid4(),
    )

    with pytest.raises(psycopg.errors.DivisionByZero) as error:
        await subscription.__aenter__()

    assert error.value is errors[0]
    assert len(listen_connections) == 1
    assert listen_connections[0].closed


async def test_probe_cancellation_closes_connection_and_preserves_cancellation(
    postgres_url: str,
    listen_connections: list[psycopg.AsyncConnection[Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probing = asyncio.Event()
    release_probe = asyncio.Event()

    async def pending_probe(
        connection: psycopg.AsyncConnection[Any], *, timeout_sec: float,
    ) -> bool:
        await connection.execute("SELECT 1")
        probing.set()
        await release_probe.wait()
        return True

    monkeypatch.setattr(trajectory, "notify_round_trip", pending_probe)
    subscription = trajectory._ListenSubscription(
        trajectory._sqla_url_to_psycopg_dsn(postgres_url), uuid4(),
    )
    entry = asyncio.create_task(subscription.__aenter__())
    try:
        await asyncio.wait_for(probing.wait(), timeout=5)
        entry.cancel("cancel-during-listen-probe")
        with pytest.raises(asyncio.CancelledError) as error:
            await entry
        assert error.value.args == ("cancel-during-listen-probe",)
        assert len(listen_connections) == 1
        assert listen_connections[0].closed
    finally:
        entry.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await entry


async def test_successful_listen_delivers_notify_and_closes_on_context_exit(
    postgres_url: str,
    listen_connections: list[psycopg.AsyncConnection[Any]],
) -> None:
    dsn = trajectory._sqla_url_to_psycopg_dsn(postgres_url)
    trial_id = uuid4()
    subscription = trajectory._ListenSubscription(dsn, trial_id)
    async with subscription:
        listener = listen_connections[0]
        drain = subscription._drain_task
        assert not listener.closed
        assert subscription._push_mode is True
        assert drain is not None
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as sender:
            await sender.execute(
                "SELECT pg_notify(%s, %s)",
                (trajectory._LISTEN_CHANNEL, f"{trial_id}:0"),
            )
        await asyncio.wait_for(subscription.wake.wait(), timeout=5)
        assert not drain.done()

    assert listener.closed
    assert drain.cancelled()


async def test_stream_falls_back_to_polling_without_leaking_failed_listener(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    listen_connections: list[psycopg.AsyncConnection[Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, token, _, trial_id = traj_setup
    monkeypatch.setattr(trajectory, "_LISTEN_CHANNEL", "invalid-channel")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://svc",
    ) as client:
        response = await client.get(
            f"/api/v1/trials/{trial_id}/stream",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    payloads = [
        json.loads(line.removeprefix("data: "))
        for line in response.text.splitlines() if line.startswith("data: ")
    ]
    assert [payload["seq"] for payload in payloads if "seq" in payload] == [0, 1, 2, 3, 4]
    assert payloads[-1] == {"final_state": "succeeded", "last_seq": 4}
    assert len(listen_connections) == 1
    assert listen_connections[0].closed
