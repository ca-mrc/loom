"""Shared command-probe scheduling for sandbox drivers."""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from loom.errors import DriverError
from loom.models.healthcheck import HealthcheckSpec

if TYPE_CHECKING:
    from loom.driver.base import Driver


async def run_command_healthcheck(driver: Driver, health: HealthcheckSpec) -> None:
    """Probe during startup grace; only later failures consume retry allowance."""
    grace_end = time.monotonic() + health.start_period_sec
    failures = 0
    while True:
        in_grace = time.monotonic() < grace_end
        try:
            result = await driver.exec(health.command, timeout_sec=health.timeout_sec)
            if result.return_code == 0:
                return
        except TimeoutError:
            pass
        if not in_grace:
            failures += 1
            if failures > health.retries:
                raise DriverError(f"Healthcheck failed after {health.retries} retries")
        grace_remaining = grace_end - time.monotonic()
        interval = health.interval_sec
        if grace_remaining > 0:
            interval = min(health.start_interval_sec or interval, grace_remaining)
        await asyncio.sleep(interval)
