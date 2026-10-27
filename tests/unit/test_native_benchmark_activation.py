"""Native admission leaves the TB2.1 audit path and optional import boundary intact."""

import subprocess
import sys
from unittest.mock import AsyncMock

import pytest

from loom.models.harbor import HarborBenchmarkSpec
from loom_cli.native_benchmark_activation import activate_native_benchmark


async def test_native_activation_cannot_replace_tb21_audit():
    descriptor = HarborBenchmarkSpec(
        id="terminal-bench-2@tb2.1-r6", display_name="TB2.1", series="terminal-bench",
        license_spdx="MIT", license_url="https://example.test/license", expected_task_count=89,
        origin={"kind": "git", "locator": "https://example.test/tasks.git",
                "revision": "a" * 40, "release": "2.1"},
    )
    session = AsyncMock()
    with pytest.raises(ValueError, match=r"TB2\.1 requires datasets activate"):
        await activate_native_benchmark(session, spec=descriptor)
    session.scalar.assert_not_awaited()


def test_native_operator_import_does_not_require_optional_adapters():
    result = subprocess.run([sys.executable, "-c", """
import importlib.abc
import sys
class NoAdapters(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'loom_benchmarks', 'datasets'}:
            raise AssertionError('optional adapter import: ' + fullname)
sys.meta_path.insert(0, NoAdapters())
from loom_cli.datasets_cmd import dispatch
from loom_cli.native_benchmark_activation import activate_native_benchmark
dispatch(['activate-native', '--help'])
"""], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--harbor-source" in result.stdout
