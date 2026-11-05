"""CPU acquisition correction for the legacy pool snapshot ABI."""

import ast
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


def plugin():
    spec = importlib.util.spec_from_file_location(
        "ci_runner_unified_host", ROOT / "scripts/ops/ci_runner_unified_host.py"
    )
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def legacy_namespace(host):
    class FakePath:
        def __init__(self, *parts):
            self.name = "/".join(str(part) for part in parts)

        def __truediv__(self, part):
            return FakePath(self.name, part)

        def exists(self):
            return False

        def read_text(self):
            assert self.name == "/proc/meminfo"
            return "MemTotal: 67108864 kB\nMemAvailable: 58720256 kB\n"

    namespace = {
        "__name__": "legacy_sampler_fixture",
        "Path": FakePath,
        "SLICE": "loomciexpansion.slice",
        "os": SimpleNamespace(
            cpu_count=lambda: 8,
            statvfs=lambda _: SimpleNamespace(f_blocks=1024**4, f_frsize=1, f_bavail=800 * 1024**3),
        ),
        "time": SimpleNamespace(sleep=lambda _: None),
        "psi": lambda _: {"some": 0.0, "full": 0.0},
        "b": SimpleNamespace(
            PoolOperationError=RuntimeError,
            _runner_busy=lambda record: bool(record and record["busy"]),
        ),
    }
    exec(host.LEGACY_CPU_FUNCTIONS, namespace)
    return namespace


def sample(namespace, first=(0, 0, 0, 100.0), last=(792, 600, 2000000, 101.0)):
    values = iter((first, last))
    namespace["counters"] = lambda: next(values)
    states = {index: SimpleNamespace(runner_id=index) for index in range(2)}
    inventory = {index: {"busy": True} for index in range(2)}
    return namespace["snapshot"](SimpleNamespace(state_root="/fixture"), states, inventory)


def test_snapshot_fixture_aligns_tick_and_wall_intervals(monkeypatch):
    host = plugin()
    namespace = legacy_namespace(host)
    before = sample(namespace)
    assert before["idle_cpus"] + before["pool_cpu_usage_cpus"] == pytest.approx(8.0606060606)
    assert before["idle_cpus"] + before["pool_cpu_usage_cpus"] > 8.05
    host.install_cpu_sampler(namespace)
    clock = iter((99.9999, 100.0001, 100.9999, 101.0001))
    monkeypatch.setattr(host.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(host.os, "sysconf", lambda _: 100)
    after = sample(namespace)
    assert after["idle_cpus"] == pytest.approx(5.98 / 1.0002)
    assert after["pool_cpu_usage_cpus"] == pytest.approx(1.999998 / 1.0002)
    assert after["idle_cpus"] + after["pool_cpu_usage_cpus"] < 8.0
    assert {
        key: value
        for key, value in after.items()
        if key not in {"idle_cpus", "pool_cpu_usage_cpus"}
    } == {
        key: value
        for key, value in before.items()
        if key not in {"idle_cpus", "pool_cpu_usage_cpus"}
    }


def test_idle_credit_never_increases_when_tick_rate_is_high():
    first, last = (0, 0, 0, 100.0), (808, 600, 2000000, 101.0)
    result = plugin().cpu_sample_values(
        first, last, ((99.9999, 100.0001), (100.9999, 101.0001)), 8, 100
    )
    assert result["idle_cpus"] == 600 / 808 * 8


@pytest.mark.parametrize(
    "first,last,spans",
    [
        ((0, 0, 0), (792, 600, 2000000, 101.0), ((99.9999, 100.0001), (100.9999, 101.0001))),
        (
            (0, 0, 0, float("nan")),
            (792, 600, 2000000, 101.0),
            ((99.9999, 100.0001), (100.9999, 101.0001)),
        ),
        (
            (800, 200, 2000000, 100.0),
            (799, 201, 3000000, 101.0),
            ((99.9999, 100.0001), (100.9999, 101.0001)),
        ),
        (
            (0, 0, 2000000, 100.0),
            (792, 600, 1999999, 101.0),
            ((99.9999, 100.0001), (100.9999, 101.0001)),
        ),
        (
            (0, 0, 0, 100.0),
            (792, 800, 2000000, 101.0),
            ((99.9999, 100.0001), (100.9999, 101.0001)),
        ),
        ((0, 0, 0, 100.0), (0, 0, 0, 101.0), ((99.9999, 100.0001), (100.9999, 101.0001))),
        ((0, 0, 0, 100.0), (792, 600, 2000000, 99.0), ((99.9999, 100.0001), (98.9999, 99.0001))),
    ],
)
def test_bad_acquisition_remains_invalid(first, last, spans):
    assert plugin().cpu_sample_values(first, last, spans, 8, 100)["idle_cpus"] < 0


def test_hardware_exceeding_pool_usage_is_not_clipped_into_health():
    result = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (800, 0, 9000000, 101.0),
        ((99.9999, 100.0001), (100.9999, 101.0001)),
        8,
        100,
    )
    assert result["pool_cpu_usage_cpus"] > 8.0


def fixture_runtime(host, root):
    root.mkdir()
    (root / "expand_host.py").write_text('base = object()\nif __name__ == "__main__":\n    pass\n')
    source = host.LEGACY_CPU_FUNCTIONS + '\nif __name__ == "__main__":\n    pass\n'
    (root / "adaptive_pool.py").write_text(source)
    return source


@pytest.mark.parametrize("quote", ["single", "double"])
def test_installer_is_idempotent_and_installed_hook_runs(tmp_path, monkeypatch, quote):
    host = plugin()
    runtime = tmp_path / "runtime"
    original = fixture_runtime(host, runtime)
    if quote == "single":
        original = original.replace('"__main__"', "'__main__'")
        (runtime / "adaptive_pool.py").write_text(original)
    monkeypatch.setattr(host, "RUNTIME", runtime)
    host.install_runtime(runtime)
    once = (runtime / "adaptive_pool.py").read_text()
    host.install_runtime(runtime)
    assert (runtime / "adaptive_pool.py").read_text() == once
    assert once.count(host.CPU_SAMPLER_HOOK) == 1
    assert (runtime / "adaptive_pool.py.before-cpu-sampler").read_text() == original
    namespace = legacy_namespace(host)
    monkeypatch.setitem(__import__("sys").modules, "ci_runner_unified_host", host)
    exec(once, namespace)
    assert namespace["_loom_cpu_sampler_installed"]
    snapshot = namespace["snapshot"]
    host.install_cpu_sampler(namespace)
    assert namespace["snapshot"] is snapshot
    old_functions = [
        ast.dump(node) for node in ast.parse(original).body if isinstance(node, ast.FunctionDef)
    ]
    new_functions = [
        ast.dump(node) for node in ast.parse(once).body if isinstance(node, ast.FunctionDef)
    ]
    assert old_functions == new_functions


def test_unknown_sampler_source_fails_before_any_installation(tmp_path, monkeypatch):
    host = plugin()
    runtime = tmp_path / "runtime"
    original = fixture_runtime(host, runtime)
    (runtime / "adaptive_pool.py").write_text(original.replace("/delta*cpus", "/delta*(cpus+1)"))
    monkeypatch.setattr(host, "RUNTIME", runtime)
    adapter_before = (runtime / "expand_host.py").read_bytes()
    with pytest.raises(ValueError, match="unsupported CPU sampler"):
        host.install_runtime(runtime)
    assert (runtime / "expand_host.py").read_bytes() == adapter_before
    assert not (runtime / "ci_runner_unified_host.py").exists()
    assert not (runtime / "adaptive_pool.py.before-cpu-sampler").exists()


def test_truthful_scheduler_preemption_lowers_credit_instead_of_invalidating():
    result = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (816, 612, 2000000, 101.0),
        ((99.98, 100.0), (101.0, 101.0001)),
        8,
        100,
    )
    assert result["idle_cpus"] == pytest.approx(6.10 / 1.0201)
    assert result["pool_cpu_usage_cpus"] == pytest.approx(1.999998 / 1.0201)
    assert 0 < result["idle_cpus"] + result["pool_cpu_usage_cpus"] < 8


def test_export_quantization_cannot_create_physical_cpu_excess():
    result = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (80, 42, 400001, 100.1),
        ((100.0, 100.0), (100.1, 100.1)),
        8,
        100,
    )
    assert 42 / 100 / 0.1 + 400001 / 1e6 / 0.1 > 8.05
    assert result["idle_cpus"] == pytest.approx(4.0)
    assert result["pool_cpu_usage_cpus"] == pytest.approx(3.99999)
    assert result["idle_cpus"] + result["pool_cpu_usage_cpus"] <= 8
    excessive = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (80, 42, 900001, 100.1),
        ((100.0, 100.0), (100.1, 100.1)),
        8,
        100,
    )
    assert excessive["pool_cpu_usage_cpus"] > 8


@pytest.mark.parametrize(
    "last,clock",
    [
        ((816, 612, 2000000, 101.0), (99.98, 100.0, 101.0, 101.0001)),
        ((800, 402, 4000001, 101.0), (100.0, 100.0, 101.0, 101.0)),
    ],
)
def test_installed_snapshot_preserves_busy_fields_under_delay_and_rounding(
    monkeypatch, last, clock
):
    host = plugin()
    namespace = legacy_namespace(host)
    host.install_cpu_sampler(namespace)
    times = iter(clock)
    monkeypatch.setattr(host.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(host.os, "sysconf", lambda _: 100)
    result = sample(namespace, last=last)
    assert result["current_slots"] == result["busy_runners"] == 2
    assert 0 < result["idle_cpus"] + result["pool_cpu_usage_cpus"] <= 8


@pytest.mark.parametrize(
    "first,last",
    [
        ((0, 0, 0), (792, 600, 2000000, 101.0)),
        ((0, 0, 0, float("nan")), (792, 600, 2000000, 101.0)),
        ((0, 0, 0, 100.0), (0, 0, 0, 101.0)),
        ((0, 0, 2000000, 100.0), (792, 600, 1999999, 101.0)),
        ((0, 0, 0, 100.0), (1000, 803, 0, 101.0)),
    ],
)
def test_unavailable_acquisition_raises_before_other_snapshot_reads(monkeypatch, first, last):
    host = plugin()
    namespace = legacy_namespace(host)
    host.install_cpu_sampler(namespace)
    clock = iter((99.9999, 100.0001, 100.9999, 101.0001))
    monkeypatch.setattr(host.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(host.os, "sysconf", lambda _: 100)
    namespace["Path"] = lambda *_: pytest.fail("unavailable acquisition read other telemetry")
    with pytest.raises(namespace["b"].PoolOperationError, match="CPU acquisition unavailable"):
        sample(namespace, first=first, last=last)


def test_valid_numeric_hardware_excess_still_reaches_policy_inputs(monkeypatch):
    host = plugin()
    namespace = legacy_namespace(host)
    host.install_cpu_sampler(namespace)
    clock = iter((99.9999, 100.0001, 100.9999, 101.0001))
    monkeypatch.setattr(host.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(host.os, "sysconf", lambda _: 100)
    result = sample(namespace, last=(800, 0, 9000000, 101.0))
    assert result["idle_cpus"] == 0
    assert result["pool_cpu_usage_cpus"] > result["total_cpus"]
    assert result["current_slots"] == result["busy_runners"] == 2


@pytest.mark.parametrize(
    "total,idle,usage", [(100, 80, 2000000), (720, 600, 2000000), (880, 600, 2000000)]
)
def test_total_accounting_does_not_require_wall_tick_identity(total, idle, usage):
    result = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (total, idle, usage, 101.0),
        ((99.9999, 100.0001), (100.9999, 101.0001)),
        8,
        100,
    )
    assert 0 <= result["idle_cpus"] <= idle / total * 8
    assert result["idle_cpus"] + result["pool_cpu_usage_cpus"] <= 8.05


def test_snapshot_accepts_independent_total_accounting(monkeypatch):
    host = plugin()
    namespace = legacy_namespace(host)
    host.install_cpu_sampler(namespace)
    clock = iter((99.9999, 100.0001, 100.9999, 101.0001))
    monkeypatch.setattr(host.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(host.os, "sysconf", lambda _: 100)
    result = sample(namespace, last=(840, 760, 250000, 101.0))
    assert result["idle_cpus"] == pytest.approx(760 / 840 * 8)
    assert result["idle_cpus"] + result["pool_cpu_usage_cpus"] < 8
    assert result["current_slots"] == result["busy_runners"] == 2


@pytest.mark.parametrize("idle", [801, 802])
def test_direct_idle_credit_accepts_physical_boundary_after_export_rounding(idle):
    result = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (1000, idle, 0, 101.0),
        ((100.0, 100.0), (101.0, 101.0)),
        8,
        100,
    )
    assert result["idle_cpus"] == pytest.approx(idle / 1000 * 8)


@pytest.mark.parametrize("total,idle", [(1000, 803), (10000, 6000)])
def test_direct_idle_excess_is_not_hidden_by_ratio(total, idle):
    assert idle / total * 8 < 8
    result = plugin().cpu_sample_values(
        (0, 0, 0, 100.0),
        (total, idle, 0, 101.0),
        ((100.0, 100.0), (101.0, 101.0)),
        8,
        100,
    )
    assert result["idle_cpus"] < 0
