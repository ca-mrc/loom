#!/usr/bin/env python3
"""Add an isolated-guest capability to the existing single-job KVM lifecycle."""

from __future__ import annotations

import argparse
import ast
import fcntl
import json
import math
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

CAPABILITY = "loom-ci-isolated-v1"
MEMORY_CAPABILITY = "loom-ci-guest12g-v1"
MARKER = "/etc/loom-ci-ephemeral-runner"
RUNTIME = Path("/usr/local/lib/loom-ci-runner-expansion")
# Reuse the service image's existing pinned kubectl build. Hosted images already
# provide this CLI; the minimal OLDLAB guest must provide the same executable
# boundary before accepting operator-test jobs.
KUBECTL_IMAGE = "registry.k8s.io/kubectl:v1.36.2@sha256:b0d792e0d8dfb9bb1b922b78b23137e2a34bb6f9667640353a9d2aadd1fd7761"



# Supported operational sampler ABI. Keep this small fixture with its adapter;
# policy.py, retirement, and the remaining host lifecycle stay externally owned.
LEGACY_CPU_FUNCTIONS = """\
def counters():
    fields = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    # guest counters are already included in user/nice and must not be counted twice.
    total = sum(fields)
    idle = fields[3] + fields[4]
    usage = 0
    for group in (SLICE, 'loom-ci-runner-pool.slice'):
        path = Path('/sys/fs/cgroup') / group / 'cpu.stat'
        if path.exists():
            stat = dict(line.split() for line in path.read_text().splitlines())
            usage += int(stat.get('usage_usec', 0))
    return total, idle, usage, time.monotonic()

def snapshot(profile, states, inventory):
    first = counters()
    time.sleep(1)
    last = counters()
    delta = last[0] - first[0]
    if delta <= 0 or last[3] <= first[3]:
        raise b.PoolOperationError('CPU sample did not advance')
    cpus = os.cpu_count()
    mem = {line.split(':')[0]: int(line.split()[1]) * 1024
           for line in Path('/proc/meminfo').read_text().splitlines()}
    anon = 0
    for group in (SLICE, 'loom-ci-runner-pool.slice'):
        path = Path('/sys/fs/cgroup') / group / 'memory.stat'
        if path.exists():
            values = dict(line.split() for line in path.read_text().splitlines())
            anon += int(values.get('anon', 0))
    fs = os.statvfs(profile.state_root)
    mp, ip = psi('memory'), psi('io')
    return dict(total_cpus=cpus, idle_cpus=(last[1]-first[1])/delta*cpus,
                pool_cpu_usage_cpus=max(0, last[2]-first[2])/1e6/(last[3]-first[3]),
                mem_total_bytes=mem['MemTotal'], mem_available_bytes=mem['MemAvailable'],
                pool_anon_bytes=anon, current_slots=len(states),
                busy_runners=sum(b._runner_busy(inventory.get(s.runner_id)) for s in states.values()),
                fs_total_bytes=fs.f_blocks*fs.f_frsize, fs_free_bytes=fs.f_bavail*fs.f_frsize,
                memory_psi_some_avg10=mp['some'], memory_psi_full_avg10=mp['full'],
                io_psi_some_avg10=ip['some'], io_psi_full_avg10=ip['full'])
"""
CPU_SAMPLER_HOOK = (
    "from ci_runner_unified_host import install_cpu_sampler\n"
    "install_cpu_sampler(globals())\n\n"
)


def cpu_sample_values(first: tuple, last: tuple, spans: tuple,
                      cpus: int, ticks_per_second: int) -> dict[str, float]:
    """Lower idle credit only inside a bounded counter-acquisition envelope."""
    invalid = {"total_cpus": cpus, "idle_cpus": -1.0, "pool_cpu_usage_cpus": 0.0}
    if (type(cpus) is not int or cpus <= 0 or type(ticks_per_second) is not int
            or ticks_per_second <= 0 or len(first) != 4 or len(last) != 4):
        return invalid
    values = (*first, *last, *spans[0], *spans[1])
    if any(isinstance(value, bool) or not isinstance(value, (int, float))
           or not math.isfinite(value) for value in values):
        return invalid
    if any(type(value) is not int or value < 0 for value in (*first[:3], *last[:3])):
        return invalid
    start, end = spans
    if not start[0] <= first[3] <= start[1] < end[0] <= last[3] <= end[1]:
        return invalid
    elapsed = last[3] - first[3]
    total, idle, usage = (last[index] - first[index] for index in range(3))
    if elapsed <= 0 or total <= 0 or not 0 <= idle <= total or usage < 0:
        return invalid
    # Every counter read falls within these brackets. Scheduler preemption
    # widens the observed interval; it is not evidence of corrupt counters.
    inner_elapsed = end[0] - start[1]
    outer_elapsed = end[1] - start[0]
    # Retain the operational accounting-consistency envelope, not a claim of
    # kernel-atomic accounting. Do not normalize grossly inconsistent input.
    tick_error = 2 * cpus + 8
    if not (cpus * ticks_per_second * inner_elapsed - tick_error <= total
            <= cpus * ticks_per_second * outer_elapsed + tick_error):
        return invalid
    ratio_idle = idle / total * cpus
    # idle and iowait are independently truncated aggregate USER_HZ fields;
    # the two disjoint cgroups independently truncate usage to microseconds.
    # Subtract their known export uncertainty before using the common outer
    # interval. This only lowers credit; genuine excess still reaches policy.
    wall_idle = max(0, idle - 2) / ticks_per_second / outer_elapsed
    pool_usage = max(0, usage - 2) / 1e6 / outer_elapsed
    return {"total_cpus": cpus, "idle_cpus": min(ratio_idle, wall_idle),
            "pool_cpu_usage_cpus": pool_usage}


def install_cpu_sampler(namespace: dict[str, Any]) -> None:
    """Own the supported snapshot adapter without changing resource policy."""
    if namespace.get("_loom_cpu_sampler_installed"):
        return
    for name in ("counters", "snapshot", "psi", "b", "SLICE", "os", "Path", "time"):
        if name not in namespace:
            raise ValueError("installed CPU sampler has an unsupported runtime namespace")

    def snapshot(profile: Any, states: Any, inventory: Any) -> dict[str, Any]:
        def read() -> tuple[tuple, tuple[float, float]]:
            start = time.monotonic()
            values = namespace["counters"]()
            return values, (start, time.monotonic())

        first, first_span = read()
        namespace["time"].sleep(1)
        last, last_span = read()
        cpus = namespace["os"].cpu_count()
        cpu = cpu_sample_values(first, last, (first_span, last_span),
                                cpus, os.sysconf("SC_CLK_TCK"))
        if cpu["idle_cpus"] == -1.0:
            raise namespace["b"].PoolOperationError(
                "CPU acquisition unavailable; reconciliation skipped")
        # Preserve the existing non-CPU snapshot fields and their policy inputs.
        path = namespace["Path"]
        mem = {line.split(":")[0]: int(line.split()[1]) * 1024
               for line in path("/proc/meminfo").read_text().splitlines()}
        anon = 0
        for group in (namespace["SLICE"], "loom-ci-runner-pool.slice"):
            memory_stat = path("/sys/fs/cgroup") / group / "memory.stat"
            if memory_stat.exists():
                values = dict(line.split() for line in memory_stat.read_text().splitlines())
                anon += int(values.get("anon", 0))
        fs = namespace["os"].statvfs(profile.state_root)
        mp, ip = namespace["psi"]("memory"), namespace["psi"]("io")
        return dict(**cpu, mem_total_bytes=mem["MemTotal"],
                    mem_available_bytes=mem["MemAvailable"], pool_anon_bytes=anon,
                    current_slots=len(states),
                    busy_runners=sum(namespace["b"]._runner_busy(inventory.get(s.runner_id))
                                     for s in states.values()),
                    fs_total_bytes=fs.f_blocks * fs.f_frsize,
                    fs_free_bytes=fs.f_bavail * fs.f_frsize,
                    memory_psi_some_avg10=mp["some"], memory_psi_full_avg10=mp["full"],
                    io_psi_some_avg10=ip["some"], io_psi_full_avg10=ip["full"])

    namespace["snapshot"] = snapshot
    namespace["_loom_cpu_sampler_installed"] = True


def validate_cpu_runtime(source: str) -> int:
    expected = {node.name: ast.dump(node, include_attributes=False)
                for node in ast.parse(LEGACY_CPU_FUNCTIONS).body
                if isinstance(node, ast.FunctionDef)}
    actual = {node.name: ast.dump(node, include_attributes=False)
              for node in ast.parse(source).body
              if isinstance(node, ast.FunctionDef) and node.name in expected}
    main_test = ast.dump(ast.parse('if __name__ == "__main__": pass').body[0].test)
    main_blocks = [node for node in ast.parse(source).body
                   if isinstance(node, ast.If) and ast.dump(node.test) == main_test]
    if actual != expected or len(main_blocks) != 1:
        raise ValueError("installed pool has an unsupported CPU sampler ABI")
    return main_blocks[0].lineno - 1


def guest_tools_script() -> str:
    return (
        "if ! PATH=/usr/local/bin:/usr/bin:/bin command -v kubectl >/dev/null; then\n"
        "  kubectl_container=$(timeout 180 docker create " + KUBECTL_IMAGE + ")\n"
        "  if ! docker cp \"$kubectl_container:/bin/kubectl\" /usr/local/bin/kubectl; then\n"
        "    docker rm \"$kubectl_container\" >/dev/null\n"
        "    exit 1\n"
        "  fi\n"
        "  docker rm \"$kubectl_container\" >/dev/null\n"
        "  chmod 0755 /usr/local/bin/kubectl\n"
        "fi\n"
    )


def guest_script(original: str) -> str:
    boundary = "systemctl is-active --quiet docker\n"
    if original.count(boundary) != 1:
        raise ValueError("installed guest lifecycle has an unsupported Docker-ready boundary")
    marker = (
        'printf \'%s\\n\' \'{"schema_version":1,"provider":"oldlab"}\' > ' + MARKER + "\n"
        "chmod 0444 " + MARKER + "\n"
    )
    return original.replace(boundary, boundary + guest_tools_script() + marker)


def install(base: Any) -> None:
    if getattr(base, "_loom_unified_installed", False):
        return
    original_script = base._slot_run_script
    original_jit = base.GitHubAPI.generate_jit_config
    original_create = base._create_slot

    def slot_script() -> str:
        return guest_script(original_script())

    def generate_jit(self: Any, *, name: str, labels: Any) -> Any:
        return original_jit(
            self,
            name=name,
            labels=tuple(dict.fromkeys((*labels, CAPABILITY, MEMORY_CAPABILITY, "loom-ci-exec"))),
        )

    def create_slot(profile: Any, **kwargs: Any) -> Any:
        if profile.memory_mib_per_slot < 12288:
            raise ValueError("common-queue guests need 12 GiB for the existing 8 GiB sandbox tests")
        return original_create(profile, **kwargs)

    base._slot_run_script = slot_script
    base.GitHubAPI.generate_jit_config = generate_jit
    base._create_slot = create_slot
    base._loom_unified_installed = True


def install_runtime(runtime: Path) -> None:
    if runtime != RUNTIME:
        raise ValueError("unified plugin installs only into the existing OLDLAB pool runtime")
    adapter = runtime / "expand_host.py"
    source = adapter.read_text()
    sampler = runtime / "adaptive_pool.py"
    sampler_source = sampler.read_text()
    sampler_boundary_line = validate_cpu_runtime(sampler_source)
    sampler_backup = sampler.with_suffix(".py.before-cpu-sampler")
    if CPU_SAMPLER_HOOK not in sampler_source and sampler_backup.exists():
        raise ValueError("an earlier CPU sampler backup exists; inspect before installation")
    boundary = 'if __name__ == "__main__":'
    addition = "from ci_runner_unified_host import install as install_unified_guest\ninstall_unified_guest(base)\n\n"
    if boundary not in source:
        raise ValueError("installed pool adapter has an unsupported entrypoint")
    target = runtime / "ci_runner_unified_host.py"
    if Path(__file__).resolve() != target:
        shutil.copyfile(__file__, target)
        target.chmod(0o600)
    if addition not in source:
        backup = adapter.with_suffix(".py.before-unified")
        if backup.exists():
            raise ValueError(
                "an earlier adapter backup exists; inspect before another installation"
            )
        shutil.copy2(adapter, backup)
        temporary = adapter.with_suffix(".py.new")
        temporary.write_text(source.replace(boundary, addition + boundary, 1))
        temporary.chmod(0o600)
        temporary.replace(adapter)
    if CPU_SAMPLER_HOOK not in sampler_source:
        shutil.copy2(sampler, sampler_backup)
        temporary = sampler.with_suffix(".py.new")
        lines = sampler_source.splitlines(keepends=True)
        temporary.write_text("".join(lines[:sampler_boundary_line]) + CPU_SAMPLER_HOOK
                             + "".join(lines[sampler_boundary_line:]))
        temporary.chmod(0o600)
        temporary.replace(sampler)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--install-runtime", type=Path, required=True)
    parser.add_argument("--retire-legacy-idle", action="store_true")
    args = parser.parse_args()
    install_runtime(args.install_runtime)
    if args.retire_legacy_idle:
        sys.path.insert(0, str(RUNTIME))
        import adaptive_pool

        base = adaptive_pool.b
        profile = base.load_profile(Path("/etc/loom-ci-runner-pool/profile.toml"))
        base._verify_host(profile)
        api = adaptive_pool.API(
            repository=profile.repository,
            token=base.read_token(Path("/etc/loom-ci-runner-pool/github-token")),
        )
        runner = base.SubprocessCommandRunner()
        retired, retained = [], []
        with (profile.state_root / "adaptive.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            inventory = base._runner_inventory(api, profile)
            for slot, state in base._existing_states(profile).items():
                record = inventory.get(state.runner_id)
                if record and MEMORY_CAPABILITY in {item["name"] for item in record.get("labels", [])}:
                    continue
                if adaptive_pool.retire(profile, state, api, runner):
                    retired.append(slot)
                else:
                    retained.append(slot)
        print(json.dumps({"legacy_idle_retired": retired, "busy_or_racing_retained": retained}))


if __name__ == "__main__":
    main()
