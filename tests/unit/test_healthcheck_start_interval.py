"""Startup probes run during grace and switch to the normal retry cadence."""
from types import SimpleNamespace

import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverError
from loom.models.healthcheck import HealthcheckSpec


@pytest.mark.asyncio
async def test_service_probe_runs_during_start_period(monkeypatch):
    elapsed = 0.0
    attempts = []

    async def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    async def execute(command, *, timeout_sec):
        attempts.append(elapsed)
        return SimpleNamespace(return_code=0 if elapsed >= 2 else 1)

    monkeypatch.setattr('asyncio.sleep', sleep)
    monkeypatch.setattr('time.monotonic', lambda: elapsed)
    driver = SimpleNamespace(exec=execute)
    await ServiceSandboxDriver.run_healthcheck(driver, HealthcheckSpec(
        command='ready', start_period_sec=10, start_interval_sec=1,
        interval_sec=5, timeout_sec=1, retries=1,
    ))
    assert attempts == [0, 1, 2]


@pytest.mark.asyncio
async def test_timeout_counts_only_after_grace_and_fails_bounded(monkeypatch):
    elapsed = 0.0
    attempts = []

    async def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    async def execute(command, *, timeout_sec):
        attempts.append(elapsed)
        raise TimeoutError

    monkeypatch.setattr('asyncio.sleep', sleep)
    monkeypatch.setattr('time.monotonic', lambda: elapsed)
    driver = SimpleNamespace(exec=execute)
    with pytest.raises(DriverError, match='Healthcheck failed'):
        await ServiceSandboxDriver.run_healthcheck(driver, HealthcheckSpec(
            command='ready', start_period_sec=2, start_interval_sec=1,
            interval_sec=5, timeout_sec=1, retries=1,
        ))
    assert attempts == [0, 1, 2, 7]
