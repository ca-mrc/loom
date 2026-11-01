from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from loom_execution_capacity_collector import __main__ as entry
from loom_execution_capacity_collector.config import CapacityCollectorModeSettings


class Clock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class TargetSettings:
    pass


class PoolSettings:
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["target", "pool"])
@pytest.mark.parametrize("capture_seconds,expected", [(6, [0, 15, 30, 45]), (21, [0, 30])])
async def test_existing_minutely_job_refreshes_without_sixty_second_feedback_gap(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    mode: str, capture_seconds: int, expected: list[int],
) -> None:
    clock = Clock()
    calls: list[float] = []

    async def capture(_settings: object) -> SimpleNamespace:
        calls.append(clock.now())
        clock.value += capture_seconds
        return SimpleNamespace(model_dump=lambda **_kw: {"id": str(len(calls))})

    async def target(settings: object) -> SimpleNamespace:
        assert isinstance(settings, TargetSettings)
        return await capture(settings)

    async def pool(settings: object) -> SimpleNamespace:
        assert isinstance(settings, PoolSettings)
        return await capture(settings)

    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", mode)
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", "50")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_INTERVAL_SECONDS", "15")
    monkeypatch.setattr(entry, "monotonic", clock.now, raising=False)
    monkeypatch.setattr(entry.asyncio, "sleep", clock.sleep)
    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", TargetSettings)
    monkeypatch.setattr(entry, "PoolCapacityCollectorSettings", PoolSettings)
    monkeypatch.setattr(entry, "collect_capacity_observation", target)
    monkeypatch.setattr(entry, "collect_pool_observation", pool)
    await entry._run()
    assert calls == expected
    assert all(delay > 0 for delay in clock.sleeps)
    assert json.loads(capsys.readouterr().out) == {"id": str(len(calls))}


@pytest.mark.asyncio
async def test_default_single_capture_remains_compatible(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", raising=False)
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", "target")
    calls = []

    async def capture(_settings: object) -> SimpleNamespace:
        calls.append(1)
        return SimpleNamespace(model_dump=lambda **_kw: {"id": "once"})

    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", object)
    monkeypatch.setattr(entry, "collect_capacity_observation", capture)
    await entry._run()
    assert calls == [1]
    assert json.loads(capsys.readouterr().out) == {"id": "once"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, TimeoutError, asyncio.CancelledError])
async def test_later_failed_capture_is_not_hidden_or_retried(
    monkeypatch: pytest.MonkeyPatch, failure: type[BaseException],
) -> None:
    clock = Clock()
    calls = []

    async def capture(_settings: object) -> SimpleNamespace:
        calls.append(clock.now())
        if len(calls) == 2:
            raise failure("observation_unavailable")
        return SimpleNamespace(model_dump=lambda **_kw: {"id": "first"})

    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", "target")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", "50")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_STOP_BEFORE_MINUTE_SECONDS", "5")
    monkeypatch.setattr(entry, "monotonic", clock.now, raising=False)
    monkeypatch.setattr(entry, "time", lambda: clock.now())
    monkeypatch.setattr(entry.asyncio, "sleep", clock.sleep)
    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", object)
    monkeypatch.setattr(entry, "collect_capacity_observation", capture)
    with pytest.raises(failure, match="observation_unavailable"):
        await entry._run()
    assert calls == [0, 15]


@pytest.mark.asyncio
async def test_expired_capture_budget_cannot_report_success_without_any_observation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    calls = []

    async def capture(_settings: object) -> None:
        calls.append(1)
        await asyncio.Event().wait()

    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", "target")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", "50")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_STOP_BEFORE_MINUTE_SECONDS", "5")
    monkeypatch.setattr(entry, "time", lambda: 54.99)
    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", object)
    monkeypatch.setattr(entry, "collect_capacity_observation", capture)
    with pytest.raises(TimeoutError):
        await entry._run()
    assert calls == [1]
    assert capsys.readouterr().out == ""


def test_invalid_refresh_cadence_is_rejected() -> None:
    with pytest.raises(ValueError):
        CapacityCollectorModeSettings(sample_window_seconds=60, sample_interval_seconds=0)


@pytest.mark.asyncio
async def test_delayed_wakeup_does_not_start_another_capture_after_window(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    clock = Clock()
    calls = []

    async def capture(_settings: object) -> SimpleNamespace:
        calls.append(clock.now())
        return SimpleNamespace(model_dump=lambda **_kw: {"id": "first"})

    async def delayed_wakeup(seconds: float) -> None:
        clock.value += seconds + 50

    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", "target")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", "50")
    monkeypatch.setattr(entry, "monotonic", clock.now)
    monkeypatch.setattr(entry.asyncio, "sleep", delayed_wakeup)
    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", object)
    monkeypatch.setattr(entry, "collect_capacity_observation", capture)
    await entry._run()
    assert calls == [0]
    assert json.loads(capsys.readouterr().out) == {"id": "first"}


def test_installed_collector_refreshes_during_existing_minutely_job() -> None:
    path = Path(__file__).resolve().parents[2] / "deploy/k8s/nebius-capacity-collector.yaml"
    config = next(doc for doc in yaml.safe_load_all(path.read_text()) if doc["kind"] == "ConfigMap")
    assert config["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS"] == "50"
    assert config["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_INTERVAL_SECONDS"] == "15"


@pytest.mark.asyncio
async def test_late_container_start_does_not_extend_sampling_across_next_cron(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    calls = []

    async def capture(_settings: object) -> SimpleNamespace:
        calls.append(clock.now())
        clock.value += 6
        return SimpleNamespace(model_dump=lambda **_kw: {"id": str(len(calls))})

    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", "target")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", "50")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_STOP_BEFORE_MINUTE_SECONDS", "5")
    monkeypatch.setattr(entry, "monotonic", clock.now)
    monkeypatch.setattr(entry, "time", lambda: 15 + clock.now(), raising=False)
    monkeypatch.setattr(entry.asyncio, "sleep", clock.sleep)
    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", object)
    monkeypatch.setattr(entry, "collect_capacity_observation", capture)
    await entry._run()
    # Process begins at cron+15s; starts at cron+15/30/45, ends at+51.
    assert calls == [0, 15, 30]
    assert clock.now() == 36
