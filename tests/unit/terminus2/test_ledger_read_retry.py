"""The call-ledger read stays, and a transport failure is retried."""

import pytest

from loom.agent.terminus2.gateway_ledger import LedgerReadUnavailableError, retry_ledger_read


@pytest.mark.asyncio
async def test_ledger_read_retries_a_transport_failure() -> None:
    calls = 0
    pauses: list[float] = []

    async def read() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise LedgerReadUnavailableError("Terminus ledger GET failed phase=read_timeout")
        return "rows"

    async def sleep(seconds: float) -> None:
        pauses.append(seconds)

    assert await retry_ledger_read(read, sleep=sleep) == "rows"
    assert pauses == [5.0, 5.0]


@pytest.mark.asyncio
async def test_ledger_read_raises_the_last_transport_failure() -> None:
    async def read() -> str:
        raise LedgerReadUnavailableError("Terminus ledger GET failed phase=read_timeout")

    async def sleep(_seconds: float) -> None:
        return None

    with pytest.raises(LedgerReadUnavailableError, match="phase=read_timeout"):
        await retry_ledger_read(read, attempts=2, sleep=sleep)
