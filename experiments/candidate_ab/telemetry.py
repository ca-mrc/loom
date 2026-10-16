#!/usr/bin/env python3
"""Read-only Linux VM/BuildKit telemetry for the unsigned Actions experiment.

Counters are cumulative unless named ``delta``/``percent``. Disk totals cover
leaf whole block devices (exclude partitions, loop/ram, and stacked parents).
Network totals use a fixed set of default-route interfaces discovered at startup,
excluding unrelated Docker bridge/veth churn. No addresses or names are emitted.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path, PurePosixPath

CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
PROC_ROOT = Path("/proc")
SYS_ROOT = Path("/sys")


def integers(text: str) -> dict[str, int]:
    return {key: int(value) for key, value in (line.split() for line in text.splitlines())}


def cpu_stat(text: str) -> dict[str, int]:
    line = next(line for line in text.splitlines() if line.startswith("cpu "))
    values = line.split()[1:]
    if len(values) < len(CPU_FIELDS):
        raise ValueError("aggregate CPU stat has fewer than eight counters")
    # guest/guest_nice are already included in user/nice; do not count twice.
    return dict(zip(CPU_FIELDS, map(int, values[:8]), strict=True))


def meminfo(text: str) -> dict[str, int]:
    result = {}
    for line in text.splitlines():
        key, value = line.split(":", 1)
        parts = value.split()
        if parts[1:] == ["kB"]:
            result[key] = int(parts[0]) * 1024
    required = ("MemTotal", "MemAvailable", "MemFree", "Buffers", "Cached", "SwapTotal", "SwapFree")
    return {key + "_bytes": result[key] for key in required}


def pressure(text: str) -> dict[str, dict[str, float | int]]:
    result = {}
    for line in text.splitlines():
        name, *fields = line.split()
        values = dict(field.split("=", 1) for field in fields)
        result[name] = {key: int(values[key]) if key == "total" else float(values[key])
                        for key in ("avg10", "avg60", "avg300", "total")}
    if "some" not in result:
        raise ValueError("pressure data is missing some counters")
    return result


def diskstats(text: str, devices: set[str]) -> dict[str, int]:
    result = dict.fromkeys(("read_ios", "read_bytes", "read_ms", "write_ios", "write_bytes",
                           "write_ms", "in_flight", "io_ms", "weighted_io_ms"), 0)
    found = set()
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 14 or fields[2] not in devices:
            continue
        found.add(fields[2])
        for key, index in (("read_ios", 3), ("read_bytes", 5), ("read_ms", 6),
                           ("write_ios", 7), ("write_bytes", 9), ("write_ms", 10),
                           ("in_flight", 11), ("io_ms", 12), ("weighted_io_ms", 13)):
            # Linux diskstats sectors are always 512 bytes, regardless of block size.
            result[key] += int(fields[index]) * (512 if key.endswith("bytes") else 1)
    if found != devices:
        raise ValueError("diskstats missing a discovered leaf device")
    return result | {"device_count": len(found)}


def default_route_interfaces(text: str) -> set[str]:
    interfaces = set()
    for line in text.splitlines():
        fields = line.split()
        if not fields or fields[0] == "Iface":
            continue
        if len(fields) < 11:
            raise ValueError("malformed IPv4 route counters")
        if fields[1] == "00000000" and fields[7] == "00000000" and int(fields[3], 16) & 1:
            if fields[0] != "lo":
                interfaces.add(fields[0])
    if not interfaces:
        raise ValueError("no active non-loopback IPv4 default-route interfaces")
    return interfaces


def network(text: str, interfaces: set[str]) -> dict[str, int]:
    result = dict.fromkeys(("rx_bytes", "rx_packets", "rx_errors", "rx_dropped",
                           "tx_bytes", "tx_packets", "tx_errors", "tx_dropped"), 0)
    found = set()
    for line in text.splitlines():
        if ":" not in line:
            continue
        interface, values = line.split(":", 1)
        if interface.strip() not in interfaces:
            continue
        fields = values.split()
        if len(fields) != 16:
            raise ValueError("network interface has malformed counters")
        found.add(interface.strip())
        for key, index in (("rx_bytes", 0), ("rx_packets", 1), ("rx_errors", 2), ("rx_dropped", 3),
                           ("tx_bytes", 8), ("tx_packets", 9), ("tx_errors", 10), ("tx_dropped", 11)):
            result[key] += int(fields[index])
    if not interfaces or found != interfaces:
        raise ValueError("network counters missing an initial default-route interface")
    return result | {"interface_count": len(found)}


def cgroup_io(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for line in text.splitlines():
        for entry in line.split()[1:]:
            key, value = entry.split("=", 1)
            result[key] = result.get(key, 0) + int(value)
    return result


def v2_path(text: str) -> PurePosixPath:
    paths = [line.split(":", 2)[2] for line in text.splitlines() if line.startswith("0::")]
    if len(paths) != 1:
        raise ValueError("expected one cgroup v2 membership")
    path = PurePosixPath(paths[0])
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("invalid cgroup v2 path")
    return path


def delta(current: dict[str, int], previous: dict[str, int], exclude=()) -> dict[str, int]:
    result = {key: value - previous.get(key, 0) for key, value in current.items()
              if key not in exclude}
    if any(value < 0 for value in result.values()):
        raise ValueError("cumulative counters decreased")
    return result


class Collector:
    def __init__(self, builder: str, proc_root: Path = PROC_ROOT, sys_root: Path = SYS_ROOT):
        self.proc, self.sys = proc_root, sys_root
        self.previous = None
        self.errors: list[dict[str, str]] = []
        self.cgroup = None
        self.pid = None
        self.devices: set[str] = set()
        self.network_interfaces: set[str] = set()
        try:
            result = subprocess.run(["docker", "inspect", "--format", "{{.State.Pid}}", builder],
                                    check=True, capture_output=True, text=True, timeout=10)
            self.pid = int(result.stdout.strip())
            if self.pid <= 0:
                raise ValueError("builder has no running PID")
            path = v2_path((self.proc / str(self.pid) / "cgroup").read_text())
            self.cgroup = self.sys / "fs/cgroup" / str(path).lstrip("/")
        except Exception as exc:
            self.errors.append(self.error("builder_discovery", exc))
        try:
            for path in (self.sys / "block").iterdir():
                if not path.name.startswith(("loop", "ram")) and not any((path / "slaves").iterdir()):
                    self.devices.add(path.name)
            if not self.devices:
                raise ValueError("no leaf whole block devices found")
        except Exception as exc:
            self.errors.append(self.error("disk_discovery", exc))
        try:
            # Keep this selection fixed: changing routes or Docker veth lifetimes
            # must not add/remove cumulative counters between adjacent samples.
            self.network_interfaces = default_route_interfaces((self.proc / "net/route").read_text())
        except Exception as exc:
            self.errors.append(self.error("network_discovery", exc))

    @staticmethod
    def error(source: str, exc: Exception) -> dict[str, str]:
        return {"source": source, "error": type(exc).__name__ + ": " + str(exc)[:500]}

    def sample(self) -> dict:
        record = {"type": "sample", "time": time.time(), "monotonic": time.monotonic(),
                  "vm": {}, "builder": {}, "errors": []}
        def read(target, key, path, parse):
            try:
                target[key] = parse(path.read_text())
            except Exception as exc:
                record["errors"].append(self.error(str(path), exc))
        vm, builder = record["vm"], record["builder"]
        read(vm, "cpu_ticks", self.proc / "stat", cpu_stat)
        read(vm, "memory", self.proc / "meminfo", meminfo)
        vm["pressure"] = {}
        for name in ("cpu", "io", "memory"):
            read(vm["pressure"], name, self.proc / "pressure" / name, pressure)
        read(vm, "disk", self.proc / "diskstats", lambda text: diskstats(text, self.devices))
        read(vm, "network", self.proc / "net/dev", lambda text: network(text, self.network_interfaces))
        if self.cgroup is not None:
            for key, filename, parse in (("cpu", "cpu.stat", integers),
                    ("memory_current_bytes", "memory.current", int),
                    ("memory_peak_bytes", "memory.peak", int),
                    ("memory_events", "memory.events", integers), ("io", "io.stat", cgroup_io)):
                read(builder, key, self.cgroup / filename, parse)
        if self.previous:
            elapsed = record["monotonic"] - self.previous["monotonic"]
            record["interval_seconds"] = elapsed
            for scope, fields in (("vm", ("cpu_ticks", "disk", "network")),
                                   ("builder", ("cpu", "memory_events", "io"))):
                for field in fields:
                    try:
                        old = self.previous[scope].get(field)
                        current = record[scope].get(field)
                        if old is None or current is None:
                            continue
                        changes = delta(current, old, ("device_count", "interface_count", "in_flight"))
                        record[scope][field + "_delta"] = changes
                        if scope == "vm" and field == "cpu_ticks":
                            total = sum(changes.values())
                            if total <= 0:
                                raise ValueError("CPU counters did not advance")
                            vm["cpu_percent"] = {key: 100 * changes[key] / total for key in ("idle", "iowait", "steal")}
                            vm["cpu_percent"]["usage"] = 100 * sum(changes[key] for key in ("user", "nice", "system", "irq", "softirq")) / total
                        if scope == "builder" and field == "cpu":
                            builder["usage_cores"] = changes["usage_usec"] / 1_000_000 / elapsed
                    except Exception as exc:
                        record["errors"].append(self.error(scope + "." + field + "_delta", exc))
        self.previous = record
        return record


def atomic_json(path: Path, data: dict) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, sort_keys=True) + "\n")
    temp.replace(path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--builder", required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    args = parser.parse_args(argv)
    stopped = threading.Event()
    reason = "stop_file"
    def terminate(signum, _frame):
        nonlocal reason
        reason = signal.Signals(signum).name
        stopped.set()
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    errors = 0
    returncode = 1
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", buffering=1) as output:
            collector = Collector(args.builder)
            def emit(record):
                output.write(json.dumps(record, sort_keys=True, allow_nan=False) + "\n")
            errors += len(collector.errors)
            emit({"type": "start", "time": time.time(), "monotonic": time.monotonic(),
                  "pid": os.getpid(), "builder_pid": collector.pid,
                  "builder_cgroup": str(collector.cgroup) if collector.cgroup else None,
                  "interval_seconds": 1, "disk_scope": "leaf_whole_block_devices",
                  "network_scope": "initial_default_route_interfaces", "errors": collector.errors})
            deadline = time.monotonic()
            while not stopped.is_set() and not args.stop_file.exists():
                sample = collector.sample()
                errors += len(sample["errors"])
                emit(sample)
                deadline += 1
                stopped.wait(max(0, deadline - time.monotonic()))
            returncode = int(errors > 0)
            emit({"type": "stop", "time": time.time(), "monotonic": time.monotonic(),
                  "reason": reason, "error_count": errors, "returncode": returncode})
    except Exception as exc:
        errors += 1
        returncode = 1
        print("Telemetry failed: " + type(exc).__name__ + ": " + str(exc), file=sys.stderr)
    finally:
        atomic_json(args.output.with_suffix(".exit.json"),
                    {"returncode": returncode, "error_count": errors, "finished": time.time()})
    if errors:
        print(f"Telemetry retained {errors} errors; inspect {args.output}", file=sys.stderr)
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
