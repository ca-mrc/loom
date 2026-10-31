"""Real TCP Gateway/PG/upstream pressure, including database-resource release."""

from __future__ import annotations

import asyncio
import json
import socket
from collections import Counter
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
import uvicorn
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import LlmCall, ProviderConnection, ProviderRequestPool
from loom_llm_gateway import dispatch_audit
from loom_llm_gateway.egress_client_pool import EgressClientPool, create_upstream_client
from tests.integration import test_gateway_request_pool as request_pool_tests

pool_setup = request_pool_tests.pool_setup
receipts = request_pool_tests.receipts


@asynccontextmanager
async def _tcp_gateway(app: Any):
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{listener.getsockname()[1]}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        listener.close()


async def test_replica_pool_admission_releases_database_before_upstream(
    pool_setup: dict, postgres_url: str, monkeypatch
) -> None:
    p = pool_setup
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    # Exercise the production admission deadline, not the fixture's debugging allowance.
    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 1)
    expected = 36  # > each replica's default SQLAlchemy 5 + 10 overflow slots.
    arrived = asyncio.Event()
    release = asyncio.Event()
    requests: list[tuple[str, str]] = []
    handlers: set[asyncio.Task] = set()

    async def upstream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            headers = {
                line.split(": ", 1)[0].lower(): line.split(": ", 1)[1]
                for line in head.split("\r\n")[1:]
                if ": " in line
            }
            payload = json.loads(await reader.readexactly(int(headers["content-length"])))
            requests.append((headers["authorization"], headers["x-request-id"]))
            if len(requests) == expected:
                arrived.set()
            await release.wait()
            body = json.dumps(
                {
                    "id": "completion",
                    "model": payload["model"],
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": "answer",
                            },
                        }
                    ],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120},
                }
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    provider = await asyncio.start_server(upstream, "127.0.0.1", 0, backlog=128)
    port = provider.sockets[0].getsockname()[1]
    engines = [p["sessions"].kw["bind"], create_async_engine(postgres_url)]
    p["apps"][1].state.session_factory = async_sessionmaker(engines[1], expire_on_commit=False)
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id.in_(p["ids"]))
            .values(base_url=f"http://127.0.0.1:{port}/v1")
        )
        await session.execute(
            update(ProviderRequestPool)
            .where(ProviderRequestPool.connection_id == p["root"])
            .values(attempt_timeout_sec=20)
        )
        await session.commit()
    for app in p["apps"]:
        await app.state.upstream_client.aclose()
        app.state.upstream_client = create_upstream_client(timeout_sec=30)
        app.state.egress_client_pool = EgressClientPool(
            upstream_client=app.state.upstream_client,
            proxy_url="",
            upstream_timeout_sec=30,
        )
    tasks = []
    try:
        async with _tcp_gateway(p["apps"][0]) as first, _tcp_gateway(p["apps"][1]) as second:
            async with httpx.AsyncClient(
                limits=httpx.Limits(max_connections=None), timeout=30
            ) as client:
                tasks = [
                    asyncio.create_task(
                        client.post(
                            f"{[first, second][i % 2]}/openai/v1/chat/completions",
                            headers={"Authorization": "Bearer " + p["jwt"]},
                            json={
                                "model": "student",
                                "messages": [{"role": "user", "content": "hi"}],
                            },
                        )
                    )
                    for i in range(expected)
                ]
                try:
                    await asyncio.wait_for(arrived.wait(), 8)
                    # Every inference is held upstream, yet neither replica holds a DB slot.
                    assert [engine.pool.checkedout() for engine in engines] == [0, 0]
                    rows = await receipts(p)
                    assert len(rows) == expected
                    assert len({r.request_id for r in rows}) == expected
                    assert Counter(r.provider_connection_id for r in rows) == {
                        member: expected // 3 for member in p["ids"]
                    }
                    assert len({receipt for _, receipt in requests}) == expected
                    assert Counter(key for key, _ in requests) == {
                        f"Bearer test-credential-{i}": expected // 3 for i in range(3)
                    }
                finally:
                    release.set()
                    responses = await asyncio.gather(*tasks)
                assert all(r.status_code == 200 for r in responses), [
                    r.status_code for r in responses
                ]
                recovery = await client.post(
                    first + "/openai/v1/chat/completions",
                    headers={"Authorization": "Bearer " + p["jwt"]},
                    json={"model": "student", "messages": [{"role": "user", "content": "again"}]},
                )
                assert recovery.status_code == 200, recovery.text
        assert [engine.pool.checkedout() for engine in engines] == [0, 0]
    finally:
        release.set()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        provider.close()
        await provider.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
        await engines[1].dispose()


async def test_client_disconnect_closes_upstream_and_finishes_receipt(
    pool_setup: dict, monkeypatch
) -> None:
    p = pool_setup
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    arrived, disconnected, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    handlers: set[asyncio.Task] = set()

    async def upstream(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        eof = None
        unblocked = None
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            length = next(
                int(line.split(":", 1)[1])
                for line in head.split("\r\n")
                if line.lower().startswith("content-length:")
            )
            await reader.readexactly(length)
            if arrived.is_set():
                body = json.dumps(
                    {
                        "model": "student",
                        "choices": [
                            {
                                "index": 0,
                                "finish_reason": "stop",
                                "message": {"role": "assistant", "content": "recovered"},
                            }
                        ],
                        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                    }
                ).encode()
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    b"Connection: close\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode()
                    + body
                )
                await writer.drain()
                return
            arrived.set()
            eof = asyncio.create_task(reader.read())
            unblocked = asyncio.create_task(release.wait())
            done, _ = await asyncio.wait([eof, unblocked], return_when=asyncio.FIRST_COMPLETED)
            if eof in done:
                assert eof.result() == b""
                disconnected.set()
            else:
                writer.write(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
                await writer.drain()
        finally:
            for pending in [eof, unblocked]:
                if pending is not None:
                    pending.cancel()
            await asyncio.gather(*(t for t in [eof, unblocked] if t), return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    provider = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = provider.sockets[0].getsockname()[1]
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id.in_(p["ids"]))
            .values(base_url=f"http://127.0.0.1:{port}/v1")
        )
        await session.execute(
            update(ProviderRequestPool)
            .where(ProviderRequestPool.connection_id == p["root"])
            .values(attempt_timeout_sec=20)
        )
        await session.commit()
    app = p["apps"][0]
    await app.state.upstream_client.aclose()
    app.state.upstream_client = create_upstream_client(timeout_sec=30)
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url="",
        upstream_timeout_sec=30,
    )
    try:
        async with _tcp_gateway(app) as base:
            async with httpx.AsyncClient(timeout=30) as client:
                call = asyncio.create_task(
                    client.post(
                        base + "/openai/v1/chat/completions",
                        headers={"Authorization": "Bearer " + p["jwt"]},
                        json={"model": "student", "messages": [{"role": "user", "content": "hi"}]},
                    )
                )
                try:
                    await asyncio.wait_for(arrived.wait(), 3)
                    call.cancel()
                    await asyncio.gather(call, return_exceptions=True)
                    await asyncio.wait_for(disconnected.wait(), 1)
                    async with asyncio.timeout(3):
                        while True:
                            rows = await receipts(p)
                            if rows and rows[0].gateway_outcome == "cancelled":
                                break
                            await asyncio.sleep(0.01)
                    assert len(rows) == 1
                    assert rows[0].provider_outcome == "cancelled"
                    assert app.state.drain.in_flight == 0
                    assert p["sessions"].kw["bind"].pool.checkedout() == 0
                    recovered = await client.post(
                        base + "/openai/v1/chat/completions",
                        headers={"Authorization": "Bearer " + p["jwt"]},
                        json={
                            "model": "student",
                            "messages": [{"role": "user", "content": "recover"}],
                        },
                    )
                    assert recovered.status_code == 200, recovered.text
                    async with asyncio.timeout(3):
                        while True:
                            rows = await receipts(p)
                            if len(rows) == 2 and rows[1].gateway_outcome == "completed":
                                break
                            await asyncio.sleep(0.01)
                    assert rows[1].provider_outcome == "response_received"
                finally:
                    release.set()
                    call.cancel()
                    await asyncio.gather(call, return_exceptions=True)
    finally:
        release.set()
        provider.close()
        await provider.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)


@pytest.mark.parametrize("dialect", ["facade", "platform"])
async def test_live_stream_disconnect_retains_drain_and_closes_upstream(
    pool_setup: dict, monkeypatch, dialect: str
) -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from pydantic import SecretStr
    from sqlalchemy import delete

    from loom.db.schema import RateCard, Trial

    p = pool_setup
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    disconnected, release = asyncio.Event(), asyncio.Event()
    handlers = set()

    async def upstream(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        eof = unblocked = None
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            length = next(
                int(line.split(":", 1)[1])
                for line in head.split("\r\n")
                if line.lower().startswith("content-length:")
            )
            await reader.readexactly(length)
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                b"Connection: close\r\n\r\n"
                b'event: message_start\ndata: {"message":{"usage":{"input_tokens":2}}}\n\n'
            )
            await writer.drain()
            eof = asyncio.create_task(reader.read())
            unblocked = asyncio.create_task(release.wait())
            done, _ = await asyncio.wait([eof, unblocked], return_when=asyncio.FIRST_COMPLETED)
            if eof in done:
                assert eof.result() == b""
                disconnected.set()
        finally:
            for pending in [eof, unblocked]:
                if pending is not None:
                    pending.cancel()
            await asyncio.gather(*(t for t in [eof, unblocked] if t), return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    provider = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = provider.sockets[0].getsockname()[1]
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id == p["root"])
            .values(base_url=f"http://127.0.0.1:{port}", provider_type="anthropic")
        )
        await session.execute(
            update(ProviderRequestPool)
            .where(ProviderRequestPool.connection_id == p["root"])
            .values(enabled=False)
        )
        await session.execute(
            update(Trial)
            .where(Trial.id == p["trial_id"])
            .values(
                config={
                    "agent_name": "terminus-2",
                    "agent_model": {"provider": "anthropic", "name": "student"},
                }
            )
        )
        await session.commit()
    app = p["apps"][0]
    rate_card_id = f"disconnect-{uuid4()}"
    if dialect == "platform":
        from loom_llm_gateway.routes import messages as messages_route

        monkeypatch.setattr(messages_route, "ANTHROPIC_BASE_URL", f"http://127.0.0.1:{port}")
        app.state.settings.anthropic_api_key = SecretStr("test-platform-stream-key")
        async with p["sessions"]() as session:
            session.add(
                RateCard(
                    id=rate_card_id,
                    captured_at=datetime.now(UTC),
                    table={
                        "entries": [
                            {
                                "provider": "anthropic",
                                "model": "student",
                                "input_per_mtok": 1,
                                "output_per_mtok": 1,
                                "cache_read_per_mtok": 0,
                                "cache_write_per_mtok": 0,
                            }
                        ]
                    },
                )
            )
            await session.commit()
    await app.state.upstream_client.aclose()
    app.state.upstream_client = create_upstream_client(timeout_sec=30)
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url="",
        upstream_timeout_sec=30,
    )
    try:
        async with _tcp_gateway(app) as base:
            async with httpx.AsyncClient(timeout=30) as client:
                async with client.stream(
                    "POST",
                    base + ("/anthropic/v1/messages" if dialect == "facade" else "/v1/messages"),
                    headers={"Authorization": "Bearer " + p["jwt"]},
                    json={
                        "model": "student",
                        "stream": True,
                        "max_tokens": 8,
                        "messages": [{"role": "user", "content": "hi"}],
                    },
                ) as response:
                    assert response.status_code == 200
                    chunk = await response.aiter_bytes().__anext__()
                    assert b"message_start" in chunk
                    assert app.state.drain.in_flight == 1
                    assert p["sessions"].kw["bind"].pool.checkedout() == 0
                await asyncio.wait_for(disconnected.wait(), 1)
                async with asyncio.timeout(3):
                    while True:
                        rows = await receipts(p)
                        if rows and rows[0].gateway_outcome == "cancelled":
                            break
                        await asyncio.sleep(0.01)
                assert len(rows) == 1
                assert rows[0].provider_outcome == "cancelled"
                assert app.state.drain.in_flight == 0
                assert p["sessions"].kw["bind"].pool.checkedout() == 0
                async with p["sessions"]() as session:
                    calls = list(
                        (
                            await session.scalars(
                                select(LlmCall).where(LlmCall.trial_id == p["trial_id"])
                            )
                        ).all()
                    )
                assert len(calls) == 1
                assert calls[0].input_tokens == 2 and calls[0].output_tokens == 0
    finally:
        release.set()
        provider.close()
        await provider.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
        if dialect == "platform":
            async with p["sessions"]() as session:
                await session.execute(delete(RateCard).where(RateCard.id == rate_card_id))
                await session.commit()


@pytest.mark.parametrize("phase", ["upstream", "pre_send"])
async def test_pipeline_disconnect_preserves_durable_budget_state(
    postgres_url: str, monkeypatch, phase: str
) -> None:
    import base64
    from uuid import uuid4

    from sqlalchemy import delete

    from loom.auth import mint_step_jwt
    from loom.db.schema import (
        ExecutionAttempt,
        ExecutionAttemptProviderBudget,
        PipelineBudgetLedger,
        PipelineProviderDispatch,
        Secret,
    )
    from loom.security.secret_store import LocalEncryptedSecretStore
    from loom_llm_gateway.app import create_app
    from loom_llm_gateway.config import GatewaySettings
    from loom_llm_gateway.rate_card import RateCardCache
    from tests.integration.test_pipeline_provider_dispatch_ledger import _seed_ledger

    monkeypatch.setenv("LOOM_GW_DB_URL", postgres_url)
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", base64.b64encode(bytes(range(32))).decode())
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    arrived, disconnected, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    handlers = set()

    async def upstream(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        eof = unblocked = None
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            length = next(
                int(line.split(":", 1)[1])
                for line in head.split("\r\n")
                if line.lower().startswith("content-length:")
            )
            await reader.readexactly(length)
            arrived.set()
            eof = asyncio.create_task(reader.read())
            unblocked = asyncio.create_task(release.wait())
            done, _ = await asyncio.wait([eof, unblocked], return_when=asyncio.FIRST_COMPLETED)
            if eof in done:
                assert eof.result() == b""
                disconnected.set()
        finally:
            for pending in [eof, unblocked]:
                if pending is not None:
                    pending.cancel()
            await asyncio.gather(*(t for t in [eof, unblocked] if t), return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    provider = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = provider.sockets[0].getsockname()[1]
    try:
        async with _seed_ledger(postgres_url) as seed:
            settings = GatewaySettings(_env_file=None)
            app = create_app(settings)
            app.state.settings = settings
            app.state.session_factory = seed.sessions
            app.state.rate_card_cache = RateCardCache(session_factory=seed.sessions, ttl_sec=60)
            app.state.upstream_client = create_upstream_client(timeout_sec=30)
            app.state.egress_client_pool = EgressClientPool(
                upstream_client=app.state.upstream_client,
                proxy_url="",
                upstream_timeout_sec=30,
            )
            async with seed.sessions() as session:
                ref = await LocalEncryptedSecretStore(session).put(
                    namespace=f"team:{seed.ids['team']}",
                    value="test-disconnect-key",
                )
                await session.execute(
                    update(ProviderConnection)
                    .where(ProviderConnection.id == seed.ids["connection"])
                    .values(
                        base_url=f"http://127.0.0.1:{port}/v1",
                        encrypted_api_key_ref=ref,
                        responses_api_supported=True,
                        responses_api_probed_at=seed.now,
                        pricing_source="rate-card",
                        rate_card_provider="openai",
                    )
                )
                await session.commit()
            ctx = seed.ctx
            blocked = asyncio.Event()
            blocker = seed.sessions()
            if phase == "pre_send":
                from loom_llm_gateway.routes import responses as responses_route

                original_mark = responses_route.mark_provider_dispatch_sent

                async def blocked_mark(session, **kwargs):
                    # Hold an actual PG lock only after reserve committed. The
                    # production mark call then waits in its own transaction.
                    await blocker.scalar(
                        select(ExecutionAttempt)
                        .where(
                            ExecutionAttempt.id == ctx.execution_attempt_id,
                        )
                        .with_for_update()
                    )
                    blocked.set()
                    await original_mark(session, **kwargs)

                monkeypatch.setattr(responses_route, "mark_provider_dispatch_sent", blocked_mark)
            jwt = mint_step_jwt(
                team_id=ctx.team_id,
                execution_attempt_id=ctx.execution_attempt_id,
                step_id=ctx.step_id,
                ttl_sec=300,
                signing_key=settings.step_jwt_signing_key.get_secret_value(),
                provider_connection_id=ctx.provider_connection_id,
                provider_connection_id_bound=True,
                step_jwt_id=ctx.step_jwt_id,
                execution_attempt_lease_epoch=ctx.execution_attempt_lease_epoch,
                execution_spec_digest=ctx.execution_spec_digest,
                control_binding_snapshot_digest=ctx.control_binding_snapshot_digest,
                execution_authorization_digest=ctx.execution_authorization_digest,
            )
            try:
                async with _tcp_gateway(app) as base:
                    async with httpx.AsyncClient(timeout=30) as client:
                        call = asyncio.create_task(
                            client.post(
                                base + "/openai/v1/responses",
                                headers={
                                    "Authorization": "Bearer " + jwt,
                                    "x-loom-provider-request-id": str(uuid4()),
                                },
                                json={"model": "gpt-test", "input": "accountable test"},
                            )
                        )
                        try:
                            await asyncio.wait_for(
                                (arrived if phase == "upstream" else blocked).wait(),
                                3,
                            )
                            call.cancel()
                            await asyncio.gather(call, return_exceptions=True)
                            if phase == "upstream":
                                await asyncio.wait_for(disconnected.wait(), 1)
                            else:
                                assert not arrived.is_set()
                            async with asyncio.timeout(3):
                                while app.state.drain.in_flight:
                                    await asyncio.sleep(0.01)
                            async with seed.sessions() as session:
                                row = await session.scalar(
                                    select(PipelineProviderDispatch).where(
                                        PipelineProviderDispatch.execution_attempt_id
                                        == ctx.execution_attempt_id,
                                    )
                                )
                                assert row.state == "settled"
                                if phase == "pre_send":
                                    assert row.outcome == "not_dispatched"
                                    assert row.upstream_attempt_count == 0
                                    assert row.llm_call_id is None
                                    budget = await session.get(
                                        ExecutionAttemptProviderBudget,
                                        ctx.execution_attempt_id,
                                    )
                                    ledger = await session.get(
                                        PipelineBudgetLedger, seed.ids["run"]
                                    )
                                    assert budget.requests_reserved == budget.requests_settled == 0
                                    assert (
                                        budget.cost_reserved_microusd
                                        == ledger.provider_reserved_microusd
                                        == 0
                                    )
                                    assert (
                                        budget.cost_settled_microusd
                                        == ledger.provider_settled_microusd
                                        == 0
                                    )
                                    return
                                assert row.outcome == "uncertain"
                                assert row.actual_cost_microusd == row.reserved_cost_microusd == 10
                                assert row.upstream_attempt_count == 1
                                call_row = await session.get(LlmCall, row.llm_call_id)
                                assert "gateway_cancelled" in json.dumps(call_row.provider_extras)
                                budget = await session.get(
                                    ExecutionAttemptProviderBudget, ctx.execution_attempt_id
                                )
                                ledger = await session.get(PipelineBudgetLedger, seed.ids["run"])
                                assert (
                                    budget.requests_reserved == 0 and budget.requests_settled == 1
                                )
                                assert (
                                    budget.cost_reserved_microusd
                                    == ledger.provider_reserved_microusd
                                    == 0
                                )
                                assert (
                                    budget.cost_settled_microusd
                                    == ledger.provider_settled_microusd
                                    == 10
                                )
                        finally:
                            call.cancel()
                            release.set()
                            await asyncio.gather(call, return_exceptions=True)
            finally:
                await blocker.close()
                await app.state.upstream_client.aclose()
                async with seed.sessions() as session:
                    await session.execute(
                        update(ProviderConnection)
                        .where(ProviderConnection.id == seed.ids["connection"])
                        .values(encrypted_api_key_ref="loom://test/provider")
                    )
                    await session.execute(delete(Secret).where(Secret.ref == ref))
                    await session.commit()
    finally:
        release.set()
        provider.close()
        await provider.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)


async def test_real_upstream_timeout_rotates_key_and_closes_previous_socket(
    pool_setup: dict, monkeypatch
) -> None:
    p = pool_setup
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    disconnected, release = asyncio.Event(), asyncio.Event()
    observed = []
    handlers = set()

    async def upstream(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        eof = unblocked = None
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode()
            headers = {
                line.split(": ", 1)[0].lower(): line.split(": ", 1)[1]
                for line in head.split("\r\n")[1:]
                if ": " in line
            }
            payload = json.loads(await reader.readexactly(int(headers["content-length"])))
            observed.append((headers["authorization"], payload))
            if headers["authorization"] == "Bearer test-credential-0":
                eof = asyncio.create_task(reader.read())
                unblocked = asyncio.create_task(release.wait())
                done, _ = await asyncio.wait([eof, unblocked], return_when=asyncio.FIRST_COMPLETED)
                if eof in done:
                    assert eof.result() == b""
                    disconnected.set()
                return
            body = json.dumps(
                {
                    "model": "student",
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {"role": "assistant", "content": "spare succeeded"},
                        }
                    ],
                    "usage": {"prompt_tokens": 2, "completion_tokens": 1, "total_tokens": 3},
                }
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            for pending in [eof, unblocked]:
                if pending is not None:
                    pending.cancel()
            await asyncio.gather(*(t for t in [eof, unblocked] if t), return_exceptions=True)
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    provider = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = provider.sockets[0].getsockname()[1]
    async with p["sessions"]() as session:
        await session.execute(
            update(ProviderConnection)
            .where(ProviderConnection.id.in_(p["ids"]))
            .values(base_url=f"http://127.0.0.1:{port}/v1")
        )
        await session.execute(
            update(ProviderRequestPool)
            .where(ProviderRequestPool.connection_id == p["root"])
            .values(attempt_timeout_sec=0.2)
        )
        await session.commit()
    app = p["apps"][0]
    await app.state.upstream_client.aclose()
    app.state.upstream_client = create_upstream_client(timeout_sec=30)
    app.state.egress_client_pool = EgressClientPool(
        upstream_client=app.state.upstream_client,
        proxy_url="",
        upstream_timeout_sec=30,
    )
    payload = {
        "model": "student",
        "messages": [
            {"role": "user", "content": "first observation"},
            {"role": "assistant", "content": "prior answer"},
            {"role": "user", "content": "next observation"},
        ],
    }
    try:
        async with _tcp_gateway(app) as base:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.post(
                    base + "/openai/v1/chat/completions",
                    headers={"Authorization": "Bearer " + p["jwt"]},
                    json=payload,
                )
                assert response.status_code == 200, response.text
                await asyncio.wait_for(disconnected.wait(), 1)
                async with asyncio.timeout(3):
                    while True:
                        rows = await receipts(p)
                        if len(rows) == 2 and all(r.gateway_outcome == "completed" for r in rows):
                            break
                        await asyncio.sleep(0.01)
                assert [key for key, _ in observed] == [
                    "Bearer test-credential-0",
                    "Bearer test-credential-1",
                ]
                assert [body for _, body in observed] == [payload, payload]
                assert rows[0].request_id == rows[1].request_id
                assert [r.provider_connection_id for r in rows] == p["ids"][:2]
                assert [r.provider_outcome for r in rows] == [
                    "transport_error",
                    "response_received",
                ]
                assert rows[1].failover_reason == "upstream_timeout"
                assert rows[1].provider_http_status == 200
                async with p["sessions"]() as session:
                    calls = list(
                        (
                            await session.scalars(
                                select(LlmCall).where(LlmCall.trial_id == p["trial_id"])
                            )
                        ).all()
                    )
                assert len(calls) == 1 and calls[0].attempt == 2
                assert calls[0].provider_extras["_loom_request_pool"][
                    "possible_duplicate_inference"
                ]
                assert app.state.drain.in_flight == 0
                assert p["sessions"].kw["bind"].pool.checkedout() == 0
    finally:
        release.set()
        provider.close()
        await provider.wait_closed()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
