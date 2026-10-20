"""A worker-test timeout must expose awaited coroutines without local values."""

import asyncio
import json
from contextlib import nullcontext

import pytest

from tests.integration import test_nebius_application_build_worker as worker_tests


async def test_stalled_worker_diagnostics_include_nested_awaits_without_locals(capsys):
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait_for_release():
        private_value = "private-worker-test-value"
        entered.set()
        await release.wait()
        return private_value

    async def await_inner():
        return await wait_for_release()

    task = asyncio.create_task(await_inner())
    try:
        await entered.wait()
        with worker_tests.observe_worker_waits(delay_seconds=0):
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        output = capsys.readouterr().err
        snapshot = json.loads(output)
        assert snapshot["event"] == "slow_application_build_worker_test"
        assert any(
            {"await_inner", "wait_for_release"}
            <= {frame["function"] for frame in item["await_chain"]}
            for item in snapshot["tasks"]
        )
        assert "private-worker-test-value" not in output
        assert not task.done()
    finally:
        release.set()
        await task


@pytest.mark.parametrize("fail", [False, True])
async def test_worker_diagnostic_timer_is_cancelled_on_context_exit(capsys, fail):
    with pytest.raises(RuntimeError) if fail else nullcontext():
        with worker_tests.observe_worker_waits(delay_seconds=0):
            if fail:
                raise RuntimeError("test body failed")
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert capsys.readouterr().err == ""
