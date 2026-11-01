"""Publish complete observations in a bounded window of an existing job."""

from __future__ import annotations

import asyncio
import json
from time import monotonic, time

from loom_execution_capacity_collector.collector import collect_capacity_observation
from loom_execution_capacity_collector.config import (
    CapacityCollectorModeSettings,
    ExecutionCapacityCollectorSettings,
    PoolCapacityCollectorSettings,
)
from loom_execution_capacity_collector.contracts import CapacityObservationReceipt
from loom_execution_capacity_collector.pool_collector import collect_pool_observation
from loom_execution_capacity_collector.pool_contracts import PoolObservationReceiptV1


async def _run() -> None:
    mode = CapacityCollectorModeSettings()
    settings: ExecutionCapacityCollectorSettings | PoolCapacityCollectorSettings
    settings = (PoolCapacityCollectorSettings() if mode.collection_mode == "pool"
                else ExecutionCapacityCollectorSettings())
    started = monotonic()
    deadline = started + mode.sample_window_seconds
    finish_bound = bool(mode.sample_window_seconds and mode.sample_stop_before_minute_seconds is not None)
    if finish_bound:
        assert mode.sample_stop_before_minute_seconds is not None
        # Pod scheduling/init consume part of the CronJob's minute. A process-
        # relative window alone can otherwise make Forbid skip the next job.
        remaining_minute = 60 - time() % 60 - mode.sample_stop_before_minute_seconds
        deadline = min(deadline, started + remaining_minute)
    next_sample = started
    receipt: CapacityObservationReceipt | PoolObservationReceiptV1 | None = None

    async def capture() -> CapacityObservationReceipt | PoolObservationReceiptV1:
        if isinstance(settings, PoolCapacityCollectorSettings):
            return await collect_pool_observation(settings)
        return await collect_capacity_observation(settings)

    while True:
        remaining = deadline - monotonic() if finish_bound else None
        if remaining is not None and remaining <= 0:
            if receipt is None:
                raise RuntimeError("collector sampling window elapsed before first capture")
            break
        budget = asyncio.timeout(remaining)
        try:
            async with budget:
                receipt = await capture()
        except TimeoutError:
            # A schedule budget expires independently of an actual source
            # error. Never suppress a provider timeout or report success when
            # no complete observation was published. Collector cancellation
            # retains its existing reader/SDK close discipline.
            if not budget.expired() or receipt is None:
                raise
            break
        if not mode.sample_window_seconds:
            break
        # Anchor starts to the job clock; a slow capture skips elapsed slots
        # instead of issuing a burst of catch-up reads. No overlapping pass,
        # hidden retry, or new collection begins beyond the sampling window.
        now = monotonic()
        next_sample += mode.sample_interval_seconds
        while next_sample < now:
            next_sample += mode.sample_interval_seconds
        if next_sample >= deadline:
            break
        if next_sample > now:
            await asyncio.sleep(next_sample - now)
        if monotonic() >= deadline:
            break
    # Retain the original single-receipt CLI output contract. Each preceding
    # observation was already independently published by the owning collector.
    assert receipt is not None
    print(json.dumps(receipt.model_dump(mode="json"), sort_keys=True, separators=(",", ":")))


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
