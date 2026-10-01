"""Run one complete read-only capacity collection and publish transaction."""

from __future__ import annotations

import asyncio
import json

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
    receipt: CapacityObservationReceipt | PoolObservationReceiptV1
    if CapacityCollectorModeSettings().collection_mode == "pool":
        receipt = await collect_pool_observation(PoolCapacityCollectorSettings())
    else:
        receipt = await collect_capacity_observation(ExecutionCapacityCollectorSettings())
    print(json.dumps(receipt.model_dump(mode="json"), sort_keys=True, separators=(",", ":")))


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
