"""Host telemetry and its public bounded, gap-preserving history projection."""

import os
from datetime import UTC, datetime, timedelta

from loom_execution_actuator.host_resource_usage import capture_host_usage
from loom_execution_capacity_collector.contracts import (
    NodeHostUsage,
    PoolResourceUsage,
    ResourceUsageAmount,
)
from loom_execution_capacity_collector.host_usage import aggregate_host_usage
from loom_service.monitor_resource_trends import _metric


def test_linux_host_samples_use_cpu_deltas_available_ram_and_log_filesystem(tmp_path) -> None:
    cpu = tmp_path / "cpu.stat"
    memory = tmp_path / "meminfo"
    cpu.write_text("cpu 20 0 10 50 0 0 0 0 0 0\ncpu0 0\ncpu1 0\n")
    memory.write_text("MemTotal:        1000 kB\nMemAvailable:      600 kB\n")
    first, counters = capture_host_usage(cpu, memory, str(tmp_path), None)
    assert first.cpu is None
    assert first.memory is not None
    assert (first.memory.used, first.memory.total) == (400 * 1024, 1000 * 1024)
    fs = os.statvfs(tmp_path)
    assert first.disk is not None
    assert first.disk.total == fs.f_frsize * fs.f_blocks
    assert first.disk.used == first.disk.total - fs.f_frsize * fs.f_bavail

    cpu.write_text("cpu 30 0 20 80 0 0 0 0 0 0\ncpu0 0\ncpu1 0\n")
    second, counters = capture_host_usage(cpu, memory, str(tmp_path), counters)
    assert second.cpu is not None
    assert (second.cpu.used, second.cpu.total) == (0.8, 2)

    cpu.write_text("cpu 0 0 0 1 0 0 0 0 0 0\ncpu0 0\ncpu1 0\n")
    reset, _ = capture_host_usage(cpu, memory, str(tmp_path), counters)
    assert reset.cpu is None
    assert reset.memory is not None


def test_pool_usage_keeps_missing_nodes_and_disk_hotspot_visible() -> None:
    now = datetime(2026, 11, 4, 15, 0, 0, tzinfo=UTC)
    later = NodeHostUsage(
        sampled_at=now - timedelta(seconds=15),
        cpu=ResourceUsageAmount(used=6, total=8),
        disk=ResourceUsageAmount(used=90, total=100),
    )
    earlier = NodeHostUsage(
        sampled_at=now - timedelta(seconds=135),
        cpu=ResourceUsageAmount(used=1, total=2),
        disk=ResourceUsageAmount(used=120, total=200),
    )
    old = aggregate_host_usage([earlier], ready_nodes=1)
    current = aggregate_host_usage([later, earlier], ready_nodes=3)
    disk = _metric([current, old], "disk", now=now, duration=300, step=30, current_usage=current)
    assert disk["latest"] == {"used": 90, "total": 100}
    assert disk["covered_nodes"] == 2
    assert disk["ready_nodes"] == 3
    assert disk["status"] == "stale"  # The oldest participating observation is stale.

    later_only = aggregate_host_usage([later], ready_nodes=2)
    cpu = _metric([later_only, current, old], "cpu", now=now, duration=300, step=30, current_usage=later_only)
    assert cpu["latest"] == {"used": 6, "total": 8}
    assert [point["value"] for point in cpu["points"]] == [70.0, None, 75.0]

    scale_zero = PoolResourceUsage(ready_nodes=0)
    stopped = _metric([old], "disk", now=now, duration=300, step=30, current_usage=scale_zero)
    assert stopped["status"] == "stale"
    assert stopped["latest"] == {"used": 120, "total": 200}
    assert stopped["covered_nodes"] == stopped["ready_nodes"] == 0
    assert 60.0 in [point["value"] for point in stopped["points"]]

    missing = _metric([current], "memory", now=now, duration=300, step=30, current_usage=current)
    assert missing["status"] == "unavailable"
    assert missing["latest"] is None
