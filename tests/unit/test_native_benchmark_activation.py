"""Native admission leaves the TB2.1 audit path and optional import boundary intact."""

import subprocess
import sys


def test_native_descriptor_cannot_replace_stored_adapter_contract():
    from loom.benchmark_admission import trusted_adapter_contract
    from loom.db.schema import Benchmark

    benchmark = Benchmark(id="terminal-bench-2@tb2.1-r6", profile_provenance={})
    assert trusted_adapter_contract(benchmark) == "tb21.v1"


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
dispatch(['activate-native', '--help'])
"""], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--harbor-source" in result.stdout
