"""Local publication must work without the optional upstream adapter packages."""

import os
import subprocess
import sys
import textwrap

import pytest


def _run_without_adapters(
    code: str,
    blocked: tuple[str, ...] = ("loom_benchmarks", "loom_benchmark_terminal_bench_2", "datasets"),
) -> subprocess.CompletedProcess[str]:
    script = """
import importlib.abc
import sys

class NoAdapters(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in BLOCKED:
            raise ModuleNotFoundError(f"No module named '{fullname}'", name=fullname)

sys.meta_path.insert(0, NoAdapters())
"""
    return subprocess.run(
        [sys.executable, "-c", f"BLOCKED = {blocked!r}\n" + textwrap.dedent(script) + textwrap.dedent(code)],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=30,
    )


@pytest.mark.parametrize("command", ["publish", "publish-local"])
def test_local_cli_reaches_validation_without_adapters(command):
    result = _run_without_adapters(f"""
import os
import tempfile
from loom_cli.datasets_cmd import dispatch
from loom_cli.local_benchmark_source_publish import publish_versioned_local_benchmark
os.environ.update(LOOM_DB_URL='postgresql://test:test@localhost/test',
                  LOOM_MINIO_ACCESS_KEY='test-access', LOOM_MINIO_SECRET_KEY='test-secret')
with tempfile.TemporaryDirectory() as root:
    # Validation fails before any database or object-store access.
    assert dispatch([{command!r}, root, '--minio-endpoint', 'https://example.test']) == 2
""")
    assert result.returncode == 0, result.stderr
    assert "Traceback" not in result.stderr
    assert "benchmark" in result.stderr.lower()


@pytest.mark.parametrize("missing", ["loom_benchmarks", "datasets"])
def test_upstream_cli_explains_missing_optional_dependencies(missing):
    result = _run_without_adapters("""
import os
from loom_cli.datasets_cmd import dispatch
os.environ.update(LOOM_DB_URL='postgresql://test:test@localhost/test',
                  LOOM_MINIO_ACCESS_KEY='test-access', LOOM_MINIO_SECRET_KEY='test-secret')
assert dispatch(['publish', '--benchmark', 'humaneval', '--minio-endpoint', 'https://example.test']) == 1
""", blocked=(missing,))
    assert result.returncode == 0, result.stderr
    assert "uv sync --locked --extra rollout" in result.stderr
    assert "--benchmark" in result.stderr
    assert "Traceback" not in result.stderr


def test_unrelated_import_failure_is_not_reported_as_an_optional_dependency():
    result = _run_without_adapters("""
import asyncio
from loom_cli.benchmark_publish import publish_benchmark
class BrokenPreparation(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'loom_cli.benchmark_prepare':
            raise ModuleNotFoundError('internal import failure', name='loom.unexpected_missing_module')
sys.meta_path.insert(0, BrokenPreparation())
try:
    asyncio.run(publish_benchmark(benchmark='humaneval'))
except ModuleNotFoundError as exc:
    assert exc.name == 'loom.unexpected_missing_module'
else:
    raise AssertionError('internal import failure was hidden')
""")
    assert result.returncode == 0, result.stderr
