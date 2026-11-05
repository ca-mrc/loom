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
            return "MemTotal: 16777216 kB\nMemAvailable: 12582912 kB\n"

    namespace = {
        "__name__": "legacy_sampler_fixture",
        "Path": FakePath,
        "SLICE": "loomciexpansion.slice",
        "os": SimpleNamespace(
            cpu_count=lambda: 8,
            statvfs=lambda _: SimpleNamespace(
                f_blocks=128 * 1024**3, f_frsize=1, f_bavail=64 * 1024**3
            ),
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


def sample(namespace):
    values = iter(((0, 0, 0, 100.0), (792, 600, 2000000, 101.0)))
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
    assert after["idle_cpus"] == pytest.approx(6.0)
    assert after["pool_cpu_usage_cpus"] == before["pool_cpu_usage_cpus"]
    assert after["idle_cpus"] + after["pool_cpu_usage_cpus"] == pytest.approx(8.0)
    assert {key: value for key, value in after.items() if key != "idle_cpus"} == {
        key: value for key, value in before.items() if key != "idle_cpus"
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
        ((0, 0, 0, 100.0), (100, 80, 2000000, 101.0), ((99.9999, 100.0001), (100.9999, 101.0001))),
        ((0, 0, 0, 100.0), (792, 600, 2000000, 99.0), ((99.9999, 100.0001), (98.9999, 99.0001))),
        ((0, 0, 0, 100.0), (792, 600, 2000000, 101.0), ((99.9, 100.1), (100.9, 101.1))),
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
    assert result["pool_cpu_usage_cpus"] == 9.0


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
