"""Measure host resources from the observer's narrow read-only mounts."""

import os
import re
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic

from loom_execution_capacity_collector.contracts import NodeHostUsage, ResourceUsageAmount

_CPU = re.compile(r"cpu[0-9]+")
_MEMINFO = re.compile(r"(MemTotal|MemAvailable):\s+([0-9]+) kB")
CpuCounters = tuple[float, int, tuple[int, ...]]


def _cpu_counters(path: Path) -> CpuCounters:
    with path.open(encoding="ascii") as stream:
        raw = stream.read(256 * 1024 + 1)
    if len(raw) > 256 * 1024:
        raise ValueError("host CPU counters exceeded their size bound")
    rows = raw.splitlines()
    if not rows or not rows[0].startswith("cpu "):
        raise ValueError("host CPU counters unavailable")
    totals = tuple(int(field) for field in rows[0].split()[1:9])
    cores = sum(bool(row and _CPU.fullmatch(row.split(maxsplit=1)[0])) for row in rows)
    if not cores or len(totals) != 8 or any(value < 0 for value in totals):
        raise ValueError("host CPU counters unavailable")
    return monotonic(), cores, totals


def capture_host_usage(
    cpu_stat: Path, meminfo: Path, logs_path: str, previous_cpu: CpuCounters | None
) -> tuple[NodeHostUsage, CpuCounters | None]:
    cpu = memory = disk = None
    counters = None
    try:
        counters = _cpu_counters(cpu_stat)
        read_at, cores, totals = counters
        if previous_cpu is not None and previous_cpu[1] == cores and read_at - previous_cpu[0] <= 60:
            deltas = tuple(current - previous for current, previous in zip(totals, previous_cpu[2], strict=True))
            total = sum(deltas)
            # Linux CPU fields: user, nice, system, idle, iowait, irq, softirq, steal.
            if total > 0 and min(deltas) >= 0:
                busy = total - deltas[3] - deltas[4] - deltas[7]
                cpu = ResourceUsageAmount(used=cores * busy / total, total=cores)
    except (OSError, ValueError):
        pass

    try:
        with meminfo.open(encoding="ascii") as stream:
            amounts = {
                match[1]: int(match[2]) * 1024
                for line in stream
                if (match := _MEMINFO.fullmatch(line.rstrip()))
            }
        total_bytes, available = amounts["MemTotal"], amounts["MemAvailable"]
        memory = ResourceUsageAmount(used=total_bytes - available, total=total_bytes)
    except (KeyError, OSError, ValueError):
        pass

    try:
        # The directory is already a read-only bind from the host. Its
        # backing filesystem is explicitly identified in the Monitor UI.
        fs = os.statvfs(logs_path)
        total_bytes = fs.f_frsize * fs.f_blocks
        available = fs.f_frsize * fs.f_bavail
        disk = ResourceUsageAmount(used=total_bytes - available, total=total_bytes)
    except (OSError, ValueError):
        pass

    return NodeHostUsage(sampled_at=datetime.now(UTC), cpu=cpu, memory=memory, disk=disk), counters
