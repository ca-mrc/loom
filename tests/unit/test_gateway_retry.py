"""`loom_llm_gateway.retry.send_with_retry` + `loom.errors.is_retryable`
(#298 Slice A).

`send_with_retry` is the per-request loop that wraps every upstream
LLM call in `loom_llm_gateway/routes/*`. Tests cover:

- Classification: retryable vs non-retryable statuses + transport errors.
- Success on first attempt: no extra waits, RETRY_ATTEMPTS observed at 1.
- Success after N retries: returns the final 200; waits in between.
- Exhaustion: returns the last 5xx response after max_attempts.
- Non-retryable status (401): returns immediately, no retries.
- Budget exceeded: stops before max_attempts when wall-clock budget runs out.
- Transport error: retries httpx.TimeoutException; raises after exhaustion.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from loom.errors import is_retryable
from loom_llm_gateway.attempt_deadline import AttemptDeadlineReachedError, GatewayAttemptDeadline
from loom_llm_gateway.retry import RetryOutcome, send_with_retry


@dataclass
class _StubSettings:
    """Mirror of the codegen'd GatewaySettings llm_retry_* fields."""

    llm_retry_max_attempts: int = 3
    llm_retry_base_backoff_sec: float = 0.01
    llm_retry_jitter_sec: float = 0.0
    llm_retry_max_backoff_sec: float = 1.0
    llm_retry_budget_sec: float = 30.0


def _resp(status: int, body: dict[str, Any] | None = None) -> httpx.Response:
    return httpx.Response(
        status, request=httpx.Request("POST", "http://upstream"),
        json=body or {},
    )


# ─── is_retryable classification ─────────────────────────────────────


@pytest.mark.parametrize("status", [502, 503, 504, 408, 429, 500])
def test_is_retryable_retryable_statuses(status: int) -> None:
    err = httpx.HTTPStatusError(
        "x", request=httpx.Request("POST", "http://u"),
        response=_resp(status),
    )
    assert is_retryable(err) is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_is_retryable_deterministic_4xx_not_retryable(status: int) -> None:
    err = httpx.HTTPStatusError(
        "x", request=httpx.Request("POST", "http://u"),
        response=_resp(status),
    )
    assert is_retryable(err) is False


def test_is_retryable_2xx_not_retryable() -> None:
    """is_retryable is for *exceptions*; a successful response never
    becomes an HTTPStatusError. But guard anyway."""
    err = httpx.HTTPStatusError(
        "x", request=httpx.Request("POST", "http://u"),
        response=_resp(200),
    )
    assert is_retryable(err) is False


@pytest.mark.parametrize("exc_cls", [
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.PoolTimeout,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.RemoteProtocolError,
])
def test_is_retryable_transport_errors(exc_cls: type) -> None:
    exc = exc_cls("transient transport blip")
    assert is_retryable(exc) is True


def test_is_retryable_unrelated_exception_false() -> None:
    assert is_retryable(ValueError("not a network thing")) is False


# ─── send_with_retry ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_with_retry_success_first_attempt() -> None:
    """No retries needed → returns response, no sleeps."""
    sleeps: list[float] = []
    settings = _StubSettings()
    call_count = 0

    async def send() -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return _resp(200)

    async def sleep(_s: float) -> None:
        sleeps.append(_s)

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 200
    assert call_count == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_retries_5xx_then_succeeds() -> None:
    """Three 503s then a 200 → returns 200 on 4th attempt."""
    settings = _StubSettings(llm_retry_max_attempts=4)
    statuses_to_return = [503, 503, 503, 200]
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        return _resp(statuses_to_return.pop(0))

    async def sleep(s: float) -> None:
        sleeps.append(s)

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 200
    # 3 retries → 3 sleeps
    assert len(sleeps) == 3


@pytest.mark.asyncio
async def test_send_with_retry_exhausts_returns_last_5xx() -> None:
    """Persistent 503 → returns 503 after max_attempts (caller raises)."""
    settings = _StubSettings(llm_retry_max_attempts=3)
    attempts = 0

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(503)

    async def sleep(_s: float) -> None:
        pass

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 503
    assert attempts == 3


@pytest.mark.asyncio
async def test_send_with_retry_non_retryable_4xx_immediate() -> None:
    """401 → returns immediately, no retries."""
    settings = _StubSettings()
    attempts = 0
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(401)

    async def sleep(s: float) -> None:
        sleeps.append(s)

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 401
    assert attempts == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_transport_exhaust_raises() -> None:
    """Persistent ConnectTimeout → raises after max_attempts."""
    settings = _StubSettings(llm_retry_max_attempts=2)
    attempts = 0

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectTimeout("nope")

    async def sleep(_s: float) -> None:
        pass

    with pytest.raises(httpx.ConnectTimeout):
        await send_with_retry(
            send, settings=settings, dialect="test", sleep=sleep,
        )
    assert attempts == 2


@pytest.mark.asyncio
async def test_send_with_retry_non_retryable_exception_immediate() -> None:
    """ValueError → propagates immediately, no retries."""
    settings = _StubSettings()
    attempts = 0

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.HTTPError("non-retryable subclass missing?")  # actually retryable type root

    async def sleep(_s: float) -> None:
        pass

    # httpx.HTTPError is the base — not in our retryable set, not classified
    # as retryable by is_retryable. Verify it propagates.
    with pytest.raises(httpx.HTTPError):
        await send_with_retry(
            send, settings=settings, dialect="test", sleep=sleep,
        )
    assert attempts == 1


@pytest.mark.asyncio
async def test_send_with_retry_budget_exceeded_stops_early() -> None:
    """Budget tight → stops before max_attempts."""
    # 3 attempts allowed, but budget = 0.5s. Backoff at attempt 2 = base
    # (0.5s), so we'd hit budget on the second wait.
    settings = _StubSettings(
        llm_retry_max_attempts=5,
        llm_retry_base_backoff_sec=0.5,
        llm_retry_jitter_sec=0.0,
        llm_retry_max_backoff_sec=10.0,
        llm_retry_budget_sec=0.4,
    )
    attempts = 0
    sleeps: list[float] = []
    clock = [0.0]

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(503)

    async def sleep(s: float) -> None:
        sleeps.append(s)
        clock[0] += s

    def now() -> float:
        return clock[0]

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep, now=now,
    )
    assert outcome.response.status_code == 503
    # Attempt 1 happens; first backoff (0.5s) would exceed 0.4s budget
    # → exit before sleeping. Should have made exactly 1 attempt.
    assert attempts == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_single_attempt_setting_disables_retry() -> None:
    """max_attempts=1 → never retries even on 503."""
    settings = _StubSettings(llm_retry_max_attempts=1)
    attempts = 0
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(503)

    async def sleep(s: float) -> None:
        sleeps.append(s)

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 503
    assert attempts == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_504_increments_ambiguous_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """504 retries bump the ambiguous-504 counter (per §D-idempotency)."""
    from loom_llm_gateway import metrics

    initial = metrics.RETRY_AMBIGUOUS_504_TOTAL.labels(dialect="test")._value.get()  # type: ignore[attr-defined]

    settings = _StubSettings(llm_retry_max_attempts=3)
    statuses = [504, 504, 200]

    async def send() -> httpx.Response:
        return _resp(statuses.pop(0))

    async def sleep(_s: float) -> None:
        pass

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 200
    # Two 504s were retried (the third response was 200 — not counted)
    final = metrics.RETRY_AMBIGUOUS_504_TOTAL.labels(dialect="test")._value.get()  # type: ignore[attr-defined]
    assert final - initial == 2.0


# ─── #298 Slice B: attempt counter on the return value ──────────────


@pytest.mark.asyncio
async def test_outcome_attempt_is_one_on_first_try_success() -> None:
    """First attempt succeeds → outcome.attempt == 1."""
    settings = _StubSettings()

    async def send() -> httpx.Response:
        return _resp(200)

    async def sleep(_s: float) -> None:
        pass

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.attempt == 1
    assert isinstance(outcome, RetryOutcome)


@pytest.mark.asyncio
async def test_outcome_attempt_counts_retries() -> None:
    """503, 503, 200 → outcome.attempt == 3 (the successful try)."""
    settings = _StubSettings(llm_retry_max_attempts=4)
    statuses = [503, 503, 200]

    async def send() -> httpx.Response:
        return _resp(statuses.pop(0))

    async def sleep(_s: float) -> None:
        pass

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 200
    assert outcome.attempt == 3


@pytest.mark.asyncio
async def test_outcome_attempt_on_exhaustion() -> None:
    """Persistent 503 → outcome.attempt == max_attempts (the last try)."""
    settings = _StubSettings(llm_retry_max_attempts=3)

    async def send() -> httpx.Response:
        return _resp(503)

    async def sleep(_s: float) -> None:
        pass

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 503
    assert outcome.attempt == 3


@pytest.mark.asyncio
async def test_outcome_attempt_on_budget_short_circuit() -> None:
    """Budget exits at attempt 1 → outcome.attempt == 1."""
    settings = _StubSettings(
        llm_retry_max_attempts=5,
        llm_retry_base_backoff_sec=5.0,
        llm_retry_jitter_sec=0.0,
        llm_retry_max_backoff_sec=10.0,
        llm_retry_budget_sec=0.1,
    )

    async def send() -> httpx.Response:
        return _resp(503)

    async def sleep(_s: float) -> None:
        pass

    outcome = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert outcome.response.status_code == 503
    assert outcome.attempt == 1


@pytest.mark.asyncio
async def test_retry_send_cannot_run_past_the_remaining_retry_budget() -> None:
    settings = _StubSettings(
        llm_retry_budget_sec=0.03,
        llm_retry_base_backoff_sec=0,
    )
    calls = 0
    cancelled = False

    async def send() -> httpx.Response:
        nonlocal calls, cancelled
        calls += 1
        if calls == 1:
            return _resp(503)
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True
        raise AssertionError("unreachable")

    with pytest.raises(httpx.TimeoutException, match="gateway retry budget exhausted"):
        await asyncio.wait_for(
            send_with_retry(send, settings=settings, dialect="test"),
            timeout=0.5,
        )
    assert calls == 2
    assert cancelled


@pytest.mark.asyncio
async def test_initial_attempt_is_bounded_by_the_total_retry_budget() -> None:
    settings = _StubSettings(llm_retry_budget_sec=0.01)
    cancelled = False

    async def send() -> httpx.Response:
        nonlocal cancelled
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True
        raise AssertionError("unreachable")

    with pytest.raises(httpx.TimeoutException, match="gateway retry budget exhausted"):
        await asyncio.wait_for(
            send_with_retry(send, settings=settings, dialect="test"), timeout=0.2,
        )
    assert cancelled


@pytest.mark.asyncio
async def test_signed_deadline_still_wins_over_the_retry_budget() -> None:
    settings = _StubSettings(llm_retry_budget_sec=10, llm_retry_base_backoff_sec=0)
    deadline = GatewayAttemptDeadline(time.monotonic() + 0.03)
    calls = 0

    async def send() -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return _resp(503)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    with pytest.raises(AttemptDeadlineReachedError):
        await send_with_retry(send, settings=settings, dialect="test", deadline=deadline)
    assert calls == 2


@pytest.mark.asyncio
async def test_received_success_is_not_discarded_after_retry_budget_clock_observation() -> None:
    settings = _StubSettings(llm_retry_budget_sec=60, llm_retry_base_backoff_sec=0)
    clock = [0.0]
    calls = 0

    async def send() -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            clock[0] = 59.0
            return _resp(503)
        clock[0] = 61.0
        return _resp(200)

    result = await send_with_retry(send, settings=settings, dialect="test", now=lambda: clock[0])
    assert result.response.status_code == 200
    assert result.attempt == 2


@pytest.mark.asyncio
async def test_continuous_response_body_is_bounded_by_total_budget() -> None:
    class SlowBody(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            while True:
                await asyncio.sleep(0.005)
                yield b"still producing body bytes\n"

        async def aclose(self):
            self.closed = True

    body = SlowBody()
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, request=request, stream=body)

    settings = _StubSettings(llm_retry_budget_sec=0.025)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), timeout=0.01) as client:
        with pytest.raises(httpx.TimeoutException, match="gateway retry budget exhausted"):
            await asyncio.wait_for(send_with_retry(
                lambda: client.post("http://upstream"), settings=settings, dialect="test",
            ), timeout=0.3)
    assert calls == 1
    assert body.closed


@pytest.mark.asyncio
async def test_backoff_cannot_run_past_total_budget() -> None:
    settings = _StubSettings(llm_retry_budget_sec=0.025, llm_retry_base_backoff_sec=0.005)
    calls = 0
    cancelled = False

    async def send():
        nonlocal calls
        calls += 1
        return _resp(503)

    async def blocked_sleep(_delay):
        nonlocal cancelled
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True

    with pytest.raises(httpx.TimeoutException, match="gateway retry budget exhausted"):
        await asyncio.wait_for(send_with_retry(
            send, settings=settings, dialect="test", sleep=blocked_sleep,
        ), timeout=0.3)
    assert calls == 1
    assert cancelled


@pytest.mark.asyncio
async def test_retry_after_cannot_extend_explicit_total_budget() -> None:
    settings = _StubSettings(llm_retry_budget_sec=1, llm_retry_base_backoff_sec=0)
    calls = 0
    sleeps = []

    async def send():
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "2"},
                              request=httpx.Request("POST", "http://upstream"))

    async def sleep(delay):
        sleeps.append(delay)

    result = await send_with_retry(send, settings=settings, dialect="test", sleep=sleep)
    assert result.response.status_code == 429
    assert calls == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_dispatch_admission_cannot_exceed_total_budget() -> None:
    settings = _StubSettings(llm_retry_budget_sec=0.01)
    calls = 0

    class WaitingAudit:
        async def send(self, send, **kwargs):
            await asyncio.Event().wait()
            return await send()

    async def send():
        nonlocal calls
        calls += 1
        return _resp(200)

    with pytest.raises(httpx.TimeoutException, match="gateway retry budget exhausted"):
        await asyncio.wait_for(send_with_retry(
            send, settings=settings, dialect="test", dispatch_audit=WaitingAudit(),
        ), timeout=0.2)
    assert calls == 0


@pytest.mark.asyncio
async def test_local_admission_429_is_not_an_upstream_retry() -> None:
    from fastapi import HTTPException

    settings = _StubSettings()
    calls = 0

    class LimitedAudit:
        async def send(self, send, **kwargs):
            nonlocal calls
            calls += 1
            raise HTTPException(429, "provider_request_pool_unavailable")

    async def send():
        raise AssertionError("admission must not contact the provider")

    with pytest.raises(HTTPException) as error:
        await send_with_retry(send, settings=settings, dialect="test", dispatch_audit=LimitedAudit())
    assert error.value.status_code == 429
    assert calls == 1
