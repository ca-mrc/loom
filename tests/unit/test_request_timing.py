"""`Server-Timing` per-request split (#2771)."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from loom_service.metrics import instrument_engine
from loom_service.request_timing import (
    RequestTiming,
    record_auth,
    record_checkout,
    record_statement,
    start_request_timing,
)


def test_header_reports_milliseconds_and_statement_count() -> None:
    timing = RequestTiming(db_sec=0.70312, statements=18, auth_sec=0.021, checkout_sec=0.0012)

    assert timing.header_value(0.9124) == (
        'total;dur=912.4, db;dur=703.1;desc="18 statements", auth;dur=21.0, checkout;dur=1.2'
    )


def test_unauthenticated_request_omits_auth_and_checkout() -> None:
    assert RequestTiming().header_value(0.002) == 'total;dur=2.0, db;dur=0.0;desc="0 statements"'


def test_statements_outside_a_request_are_not_attributed() -> None:
    async def background() -> None:
        record_statement(1.0)
        record_auth(1.0)
        record_checkout(1.0)

    # A fresh task with an empty context: no request is being timed.
    asyncio.run(background())


def test_statement_time_accumulates_on_the_request_including_failures() -> None:
    engine = create_engine("sqlite://")
    instrument_engine(engine)

    async def request() -> RequestTiming:
        timing = start_request_timing()

        async def handler() -> None:
            # Route handlers run in a child task that copies the context.
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                with pytest.raises(OperationalError):
                    conn.execute(text("SELECT * FROM missing_table"))

        await asyncio.create_task(handler())
        return timing

    try:
        timing = asyncio.run(request())
    finally:
        engine.dispose()

    assert timing.statements == 2
    assert timing.db_sec > 0


def test_concurrent_requests_keep_separate_totals() -> None:
    async def request(n: int) -> RequestTiming:
        timing = start_request_timing()
        for _ in range(n):
            record_statement(0.001)
            await asyncio.sleep(0)
        return timing

    async def both() -> list[RequestTiming]:
        return await asyncio.gather(request(2), request(5))

    first, second = asyncio.run(both())
    assert (first.statements, second.statements) == (2, 5)
