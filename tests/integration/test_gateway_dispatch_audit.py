"""Real PostgreSQL receipt regression; no LlmCall or provider credentials."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import Request
from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.db.schema import GatewayDispatchReceipt, LlmCall, Team
from loom_llm_gateway import dispatch_audit
from loom_llm_gateway.attempt_deadline import AttemptDeadlineReachedError, GatewayAttemptDeadline
from loom_llm_gateway.dispatch_audit import (
    DispatchAudit,
    DispatchAuditMiddleware,
    DispatchAuditUnavailableError,
    audit_for_request,
    request_dispatch_audit,
)
from tests.integration.gateway_db import delete_gateway_trial, insert_gateway_trial


@pytest.fixture
async def audit_setup(postgres_url: str) -> AsyncIterator[tuple[DispatchAudit, Request]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id, trial_id = uuid4(), uuid4()
    async with engine.begin() as conn:
        await conn.execute(insert(Team).values(id=team_id, name=f"audit-{team_id}"))
        task_id = await conn.run_sync(
            lambda connection: insert_gateway_trial(connection, team_id=team_id, trial_id=trial_id)
        )
    ctx = AuthContext(
        token_hash=b"",
        type="step",
        scopes=["llm:call"],
        team_id=team_id,
        expires_at=None,
        trial_id=trial_id,
        step_id="main",
        step_jwt_id=uuid4(),
        attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=20),
    )
    request = Request(
        {
            "type": "http",
            "app": SimpleNamespace(state=SimpleNamespace(session_factory=factory)),
            "_loom_dispatch_auth": ctx,
        }
    )
    yield audit_for_request(request, ctx, "openai"), request
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda connection: delete_gateway_trial(connection, trial_id=trial_id, task_id=task_id)
        )
        await conn.execute(delete(Team).where(Team.id == team_id))
    await engine.dispose()


async def _rows(audit: DispatchAudit) -> list[GatewayDispatchReceipt]:
    assert audit.state.session_factory is not None
    async with audit.state.session_factory() as session:
        return list(
            (
                await session.scalars(
                    select(GatewayDispatchReceipt)
                    .where(GatewayDispatchReceipt.request_id == audit.state.request_id)
                    .order_by(GatewayDispatchReceipt.dispatch_ordinal)
                )
            ).all()
        )


async def test_wire_trace_id_is_committed_per_dispatch_and_context_is_reset(
    audit_setup: tuple,
) -> None:
    audit, _ = audit_setup
    headers = {"Authorization": "Bearer test-provider-only"}
    observed = []

    async def operation() -> httpx.Response:
        outgoing = dispatch_audit.dispatch_request_headers(headers)
        receipt_id = outgoing["X-Request-ID"]
        assert receipt_id in {str(row.id) for row in await _rows(audit)}
        observed.append(receipt_id)
        return httpx.Response(200)

    await asyncio.gather(audit.send(operation, deadline=None), audit.send(operation, deadline=None))
    assert len(set(observed)) == 2
    assert dispatch_audit.dispatch_request_headers(headers) == headers

    async def broken() -> httpx.Response:
        assert "X-Request-ID" in dispatch_audit.dispatch_request_headers(headers)
        raise RuntimeError("disposable transport failure")

    with pytest.raises(RuntimeError):
        await audit.send(broken, deadline=None)
    assert dispatch_audit.dispatch_request_headers(headers) == headers


def _deadline(seconds: float) -> GatewayAttemptDeadline:
    return GatewayAttemptDeadline(time.monotonic() + seconds)


async def test_durable_before_io_retry_identity_and_no_billing(audit_setup: tuple) -> None:
    audit, request = audit_setup

    async def operation() -> httpx.Response:
        rows = await _rows(audit)
        assert rows[-1].provider_outcome == "admitted"
        assert rows[-1].provider_observed_at is None
        return httpx.Response(200)

    await audit.send(operation, deadline=_deadline(3))
    second = request_dispatch_audit(request, "responses", purpose="capability_probe")
    assert second is not None
    await second.send(operation, deadline=None, attempt=2)
    rows = await _rows(audit)
    assert [row.dispatch_ordinal for row in rows] == [1, 2]
    assert [row.attempt for row in rows] == [1, 2]
    assert [row.purpose for row in rows] == ["model_call", "capability_probe"]
    assert all(row.step_jwt_id == audit.ctx.step_jwt_id for row in rows)
    assert all(row.provider_outcome == "response_received" for row in rows)
    assert all(row.gateway_outcome == "pending" for row in rows)
    assert all(row.provider_http_status == 200 for row in rows)
    async with audit.state.session_factory() as session:
        assert not (
            await session.scalars(select(LlmCall).where(LlmCall.trial_id == audit.ctx.trial_id))
        ).all()
    # Terminal compare-and-set cannot relabel a response after downstream failure.
    await audit._finish(rows[0].id, "transport_error")
    assert (await _rows(audit))[0].provider_outcome == "response_received"


async def test_platform_route_does_not_infer_actual_connection_from_jwt(audit_setup: tuple) -> None:
    audit, request = audit_setup
    ctx = replace(audit.ctx, provider_connection_id=uuid4())
    platform_audit = audit_for_request(request, ctx, "anthropic", provider_connection_id=None)
    await platform_audit.send(lambda: asyncio.sleep(0, result=httpx.Response(200)), deadline=None)
    assert (await _rows(audit))[0].provider_connection_id is None


@pytest.mark.parametrize("kind", ["deadline", "cancelled", "transport_error"])
async def test_interrupted_io_preserves_observation(audit_setup: tuple, kind: str) -> None:
    audit, _ = audit_setup
    started = asyncio.Event()
    clock = [0.0]
    deadline = GatewayAttemptDeadline(1.0, clock=lambda: clock[0])

    async def operation() -> None:
        started.set()
        if kind == "deadline":
            # Cross the deadline during provider I/O, independently of how
            # long the real database admission transaction takes.
            clock[0] = 2.0
            raise TimeoutError("synthetic provider timeout")
        if kind == "transport_error":
            raise httpx.ConnectError("synthetic-private-url-must-not-be-persisted")
        await asyncio.Event().wait()

    exception = {
        "deadline": AttemptDeadlineReachedError,
        "cancelled": asyncio.CancelledError,
        "transport_error": httpx.ConnectError,
    }[kind]
    task = asyncio.create_task(audit.send(operation, deadline=deadline))
    waiter = asyncio.create_task(started.wait())
    try:
        # Surface an admission failure instead of waiting forever for an
        # operation that was never entered.
        await asyncio.wait((task, waiter), return_when=asyncio.FIRST_COMPLETED)
        if kind == "cancelled" and started.is_set():
            task.cancel()
        with pytest.raises(exception):
            await task
    finally:
        for pending in (task, waiter):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(task, waiter, return_exceptions=True)
    rows = await _rows(audit)
    assert len(rows) == 1
    assert rows[0].provider_outcome == kind
    assert rows[0].provider_http_status is None
    assert rows[0].provider_observed_at is not None


async def test_expired_admission_commit_does_not_dispatch(
    audit_setup: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit, _ = audit_setup
    original = audit._admit
    clock = [0.0]
    deadline = GatewayAttemptDeadline(1.0, clock=lambda: clock[0])

    async def slow_admit(attempt: int):
        result = await original(attempt)
        clock[0] = 2.0
        return result

    monkeypatch.setattr(audit, "_admit", slow_admit)

    async def forbidden() -> None:
        pytest.fail("must not dispatch after committed admission consumes deadline")

    with pytest.raises(AttemptDeadlineReachedError):
        await audit.send(forbidden, deadline=deadline)
    assert (await _rows(audit))[0].provider_outcome == "not_dispatched"


async def test_audit_finalization_is_not_provider_deadline(
    audit_setup: tuple, monkeypatch: pytest.MonkeyPatch
) -> None:
    audit, _ = audit_setup
    original = audit._finish
    clock = [0.0]

    async def slow_finish(*args):
        clock[0] = 2.0
        await original(*args)

    monkeypatch.setattr(audit, "_finish", slow_finish)

    async def operation() -> dict:
        return {"provider_result": "not persisted"}

    assert await audit.send(
        operation, deadline=GatewayAttemptDeadline(1.0, clock=lambda: clock[0])
    ) == {"provider_result": "not persisted"}
    assert (await _rows(audit))[0].provider_outcome == "response_received"


async def test_returned_response_race_keeps_provider_fact(audit_setup: tuple) -> None:
    audit, request = audit_setup
    clock = [0.0]
    deadline = GatewayAttemptDeadline(1.0, clock=lambda: clock[0])
    request.scope["state"] = {"loom_gateway_attempt_deadline": deadline}

    async def app(scope, receive, send):
        async def operation():
            clock[0] = 2.0
            return httpx.Response(200)

        with pytest.raises(AttemptDeadlineReachedError):
            await audit.send(operation, deadline=deadline)
        await send({"type": "http.response.start", "status": 504, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def noop(*args):
        pass

    await DispatchAuditMiddleware(app)(request.scope, noop, noop)
    row = (await _rows(audit))[0]
    assert row.provider_outcome == "response_received"
    assert row.provider_http_status == 200
    assert row.gateway_outcome == "deadline"
    assert row.gateway_http_status == 504


async def test_repeated_cancellation_during_cleanup_propagates(
    audit_setup: tuple, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    audit, _ = audit_setup
    cleanup_started = asyncio.Event()
    provider_started = asyncio.Event()
    original_factory = audit.state.session_factory

    @asynccontextmanager
    async def stuck_cleanup():
        cleanup_started.set()
        await asyncio.Event().wait()
        yield

    async def operation():
        provider_started.set()
        audit.state.session_factory = stuck_cleanup
        await asyncio.Event().wait()

    task = asyncio.create_task(audit.send(operation, deadline=None))
    await provider_started.wait()
    task.cancel()
    await cleanup_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    audit.state.session_factory = original_factory
    assert (await _rows(audit))[0].provider_outcome == "admitted"
    assert "gateway_dispatch_observation_cancelled" in caplog.text


@pytest.mark.parametrize("kind", ["stream_completed", "deadline", "cancelled"])
async def test_stream_lifetime_not_just_headers(audit_setup: tuple, kind: str) -> None:
    audit, _ = audit_setup
    closed = False

    @asynccontextmanager
    async def operation():
        nonlocal closed
        try:
            yield httpx.Response(200)
        finally:
            closed = True

    async def run() -> None:
        async with audit.stream(operation, deadline=_deadline(1)):
            assert (await _rows(audit))[0].provider_outcome == "admitted"
            if kind == "deadline":
                raise AttemptDeadlineReachedError("test")
            if kind == "cancelled":
                raise asyncio.CancelledError

    if kind == "stream_completed":
        await run()
    else:
        with pytest.raises(
            AttemptDeadlineReachedError if kind == "deadline" else asyncio.CancelledError
        ):
            await run()
    assert closed
    row = (await _rows(audit))[0]
    assert row.provider_outcome == kind
    assert row.provider_http_status == 200


async def test_stream_header_deadline_race_closes_response(audit_setup: tuple) -> None:
    audit, _ = audit_setup
    clock = [0.0]
    closed = False

    @asynccontextmanager
    async def operation():
        nonlocal closed
        try:
            clock[0] = 2.0
            yield httpx.Response(200)
        finally:
            closed = True

    with pytest.raises(AttemptDeadlineReachedError):
        async with audit.stream(
            operation, deadline=GatewayAttemptDeadline(1.0, clock=lambda: clock[0])
        ):
            pytest.fail("deadline race must win")
    assert closed
    assert (await _rows(audit))[0].provider_outcome == "deadline"


@pytest.mark.parametrize("failure", ["usage_error", "cancelled", "deadline", "close_error"])
async def test_stream_eof_survives_later_gateway_failure(audit_setup: tuple, failure: str) -> None:
    audit, _ = audit_setup
    eof = asyncio.Event()

    @asynccontextmanager
    async def operation():
        try:
            yield httpx.Response(200)
        finally:
            if failure == "close_error":
                raise RuntimeError("synthetic close failure")

    exception = {
        "usage_error": RuntimeError,
        "cancelled": asyncio.CancelledError,
        "deadline": AttemptDeadlineReachedError,
        "close_error": RuntimeError,
    }[failure]
    with pytest.raises(exception):
        async with audit.stream(operation, deadline=_deadline(1), completed=eof.is_set):
            eof.set()
            if failure != "close_error":
                raise exception("synthetic downstream failure")
    row = (await _rows(audit))[0]
    assert row.provider_outcome == "stream_completed"
    assert row.provider_http_status == 200


async def test_explicit_stream_completion_is_not_inferred_from_cm_exit(audit_setup: tuple) -> None:
    audit, _ = audit_setup

    @asynccontextmanager
    async def operation():
        yield httpx.Response(200)

    async with audit.stream(operation, deadline=None, completed=lambda: False):
        pass  # Closing after headers is not proof that the body was exhausted.
    assert (await _rows(audit))[0].provider_outcome == "response_received"


@pytest.mark.parametrize("final_status", [200, 504])
async def test_gateway_outcome_independent_of_provider_response(
    audit_setup: tuple, final_status: int
) -> None:
    audit, request = audit_setup

    async def app(scope, receive, send):
        async def operation():
            return httpx.Response(200)

        await audit.send(operation, deadline=None)
        await send({"type": "http.response.start", "status": final_status, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def noop(*args):
        pass

    await DispatchAuditMiddleware(app)(request.scope, noop, noop)
    row = (await _rows(audit))[0]
    assert row.provider_outcome == "response_received"
    assert row.provider_http_status == 200
    assert row.gateway_outcome == "completed"
    assert row.gateway_http_status == final_status


async def test_fail_closed_and_bounded_observation_failure(
    audit_setup: tuple, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    audit, _ = audit_setup
    original_factory = audit.state.session_factory

    @asynccontextmanager
    async def unavailable():
        raise RuntimeError("secret-fixture-token")
        yield

    audit.state.session_factory = unavailable

    async def forbidden():
        pytest.fail("failed receipt admission must prevent transport")

    with pytest.raises(DispatchAuditUnavailableError, match="audit unavailable"):
        await audit.send(forbidden, deadline=None)
    audit.state.session_factory = original_factory
    assert await _rows(audit) == []
    receipt = await audit._admit(1)

    @asynccontextmanager
    async def stuck():
        await asyncio.Event().wait()
        yield

    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 0.02)
    audit.state.session_factory = stuck
    start = time.monotonic()
    await audit._finish(receipt, "deadline")
    assert time.monotonic() - start < 0.5
    audit.state.session_factory = original_factory
    assert (await _rows(audit))[0].provider_outcome == "admitted"
    assert "gateway_dispatch_observation_failed" in caplog.text
    assert "secret-fixture-token" not in caplog.text


@pytest.mark.parametrize("category", ["timeout", "database", "other"])
async def test_admission_failure_logs_only_fixed_error_category(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture, category: str,
) -> None:
    audit = DispatchAudit(
        dispatch_audit._RequestAudit(),
        AuthContext(token_hash=b"", type="step", scopes=["llm:call"],
                    team_id=uuid4(), expires_at=None, trial_id=uuid4(),
                    step_id="main", step_jwt_id=uuid4()),
        "openai", None, "model_call",
    )
    secret = "private-driver-bind-value"

    @asynccontextmanager
    async def unavailable():
        if category == "timeout":
            await asyncio.Event().wait()
        if category == "database":
            raise IntegrityError("private SQL", {"token": secret}, RuntimeError(secret))
        raise RuntimeError(secret)
        yield

    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 0.02)
    audit.state.session_factory = unavailable

    async def forbidden():
        pytest.fail("failed receipt admission must prevent transport")

    with pytest.raises(DispatchAuditUnavailableError, match="audit unavailable"):
        await audit.send(forbidden, deadline=None)
    assert audit.state.receipt_ids == []
    records = [record for record in caplog.records
               if "gateway_dispatch_admission_failed" in record.getMessage()]
    assert len(records) == 1
    assert records[0].getMessage() == (
        f"gateway_dispatch_admission_failed request_id={audit.state.request_id} "
        f"error_category={category}"
    )
    assert records[0].exc_info is None
    assert secret not in caplog.text
    assert "private SQL" not in caplog.text


async def test_legacy_and_pipeline_subjects_not_fabricated(audit_setup: tuple) -> None:
    audit, request = audit_setup
    request.scope["_loom_dispatch_auth"] = replace(audit.ctx, trial_id=None, step_id=None)
    assert request_dispatch_audit(request, "openai") is None
    request.scope["_loom_dispatch_auth"] = replace(
        audit.ctx, trial_id=None, execution_attempt_id=uuid4()
    )
    assert request_dispatch_audit(request, "responses") is None


@pytest.mark.parametrize("cancelled", [False, True])
async def test_commit_acknowledgement_unknown_never_dispatches(
    audit_setup: tuple, caplog: pytest.LogCaptureFixture, cancelled: bool
) -> None:
    audit, _ = audit_setup
    original_factory = audit.state.session_factory
    assert original_factory is not None

    @asynccontextmanager
    async def commit_unknown():
        async with original_factory() as session:
            original_commit = session.commit

            async def commit():
                await original_commit()
                if cancelled:
                    raise asyncio.CancelledError
                raise RuntimeError("private-driver-detail")

            session.commit = commit
            yield session

    audit.state.session_factory = commit_unknown

    async def forbidden():
        pytest.fail("uncertain admission acknowledgement cannot authorize send")

    with pytest.raises(asyncio.CancelledError if cancelled else DispatchAuditUnavailableError):
        await audit.send(forbidden, deadline=None)
    audit.state.session_factory = original_factory
    rows = await _rows(audit)
    assert len(rows) == 1
    assert rows[0].provider_outcome == "admitted"
    assert rows[0].gateway_outcome == "pending"
    assert "private-driver-detail" not in caplog.text
    if cancelled:
        assert str(rows[0].id) in caplog.text


async def test_database_enforces_request_dispatch_uniqueness(audit_setup: tuple) -> None:
    audit, _ = audit_setup
    await audit._admit(1)
    row = (await _rows(audit))[0]
    values = {
        column.name: getattr(row, column.name)
        for column in GatewayDispatchReceipt.__table__.columns
    }
    values["id"] = uuid4()
    async with audit.state.session_factory() as session:
        with pytest.raises(IntegrityError, match="gateway_receipt_request_uidx"):
            await session.execute(insert(GatewayDispatchReceipt).values(**values))
        await session.rollback()


@pytest.mark.parametrize("observation", ["provider", "gateway"])
@pytest.mark.parametrize("phase", ["session", "execute", "commit", "session_exit"])
@pytest.mark.parametrize("category", ["timeout", "database", "other"])
async def test_observation_failure_identifies_boundary_without_private_details(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    observation: str,
    phase: str,
    category: str,
) -> None:
    audit = DispatchAudit(
        dispatch_audit._RequestAudit(),
        AuthContext(token_hash=b"", type="step", scopes=["llm:call"],
                    team_id=uuid4(), expires_at=None, trial_id=uuid4(),
                    step_id="main", step_jwt_id=uuid4()),
        "openai", None, "model_call",
    )
    receipt_id = uuid4()
    audit.state.receipt_ids.append(receipt_id)
    secret = "private-observation-driver-bind-value"

    async def maybe_fail(current: str) -> None:
        if current != phase:
            return
        if category == "timeout":
            await asyncio.Event().wait()
        if category == "database":
            raise IntegrityError("private observation SQL", {"token": secret}, RuntimeError(secret))
        raise RuntimeError(secret)

    async def execute(statement):
        await maybe_fail("execute")

    async def commit():
        await maybe_fail("commit")

    @asynccontextmanager
    async def session():
        await maybe_fail("session")
        yield SimpleNamespace(execute=execute, commit=commit)
        await maybe_fail("session_exit")

    audit.state.session_factory = session
    monkeypatch.setattr(dispatch_audit, "_AUDIT_TIMEOUT_SECONDS", 0.02)
    if observation == "provider":
        await audit._finish(receipt_id, "deadline")
    else:
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 504, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def noop(*args):
            pass

        await DispatchAuditMiddleware(app)(
            {"type": "http", "_loom_dispatch_audit": audit.state}, noop, noop,
        )
    records = [record for record in caplog.records
               if "gateway_dispatch_observation_failed" in record.getMessage()]
    assert len(records) == 1
    message = records[0].getMessage()
    assert f"request_id={audit.state.request_id}" in message
    assert f"observation={observation}" in message
    assert f"phase={phase}" in message
    assert f"error_category={category}" in message
    assert records[0].exc_info is None
    assert secret not in caplog.text
    assert "private observation SQL" not in caplog.text


async def test_terminal_receipt_reader_does_not_starve_gateway_finalization(
    audit_setup: tuple, postgres_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.integration import test_gateway_dispatch_deadline_http as http_tests

    audit, request = audit_setup
    await audit._admit(1)
    original_factory = audit.state.session_factory
    original_receipts = http_tests._receipts
    assert original_factory is not None
    observing = asyncio.Event()
    reading = asyncio.Event()
    loop = asyncio.get_running_loop()

    @asynccontextmanager
    async def session_factory():
        async with original_factory() as session:
            execute = session.execute

            async def wait_for_reader(statement):
                # Place the real gateway update inside its existing budget
                # before the reader starts. No production deadline is changed.
                observing.set()
                await reading.wait()
                return await execute(statement)

            monkeypatch.setattr(session, "execute", wait_for_reader)
            yield session

    def slow_receipts(*args):
        loop.call_soon_threadsafe(reading.set)
        time.sleep(1.1)
        return original_receipts(*args)

    audit.state.session_factory = session_factory
    monkeypatch.setattr(http_tests, "_receipts", slow_receipts)

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async def noop(*args):
        pass

    finalizing = asyncio.create_task(DispatchAuditMiddleware(app)(request.scope, noop, noop))
    try:
        await asyncio.wait_for(observing.wait(), timeout=5)
        # Stop the old 100-read loop promptly after a starvation failure.
        rows = await asyncio.wait_for(
            http_tests._terminal_receipts(postgres_url, audit.ctx.trial_id), timeout=3,
        )
        assert len(rows) == 1
        assert rows[0]["gateway_outcome"] == "completed"
        assert rows[0]["gateway_http_status"] == 200
    finally:
        finalizing.cancel()
        await asyncio.gather(finalizing, return_exceptions=True)
        audit.state.session_factory = original_factory


@pytest.mark.parametrize("reader_fails", [False, True])
async def test_terminal_receipt_cancellation_drains_reader(
    monkeypatch: pytest.MonkeyPatch, reader_fails: bool,
) -> None:
    import threading

    from tests.integration import test_gateway_dispatch_deadline_http as http_tests

    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()
    exited = threading.Event()

    def read(*args):
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(timeout=5), "test did not release receipt reader"
            if reader_fails:
                raise RuntimeError("reader failed while cancellation was draining")
            return [{"gateway_outcome": "completed"}]
        finally:
            exited.set()

    monkeypatch.setattr(http_tests, "_receipts", read)
    polling = asyncio.create_task(http_tests._terminal_receipts("unused", uuid4()))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        polling.cancel()
        await asyncio.sleep(0)
        assert not polling.done(), "cancellation abandoned the active database reader"
        assert not exited.is_set()
        polling.cancel()
        await asyncio.sleep(0)
        assert not polling.done(), "repeated cancellation abandoned the active database reader"
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await polling
        assert exited.is_set()
    finally:
        release.set()
        await asyncio.gather(polling, return_exceptions=True)
        await asyncio.to_thread(exited.wait, 5)
