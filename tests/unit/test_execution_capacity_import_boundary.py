"""Read-only capacity contracts must not bootstrap collection adapters."""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("module", [
    "loom_execution_capacity_collector.contracts",
    "loom_execution_capacity_collector.control_plane",
    "loom_service.pool_management.participant_refresh",
])
def test_capacity_read_import_does_not_construct_collection_runtime(module):
    # A fresh interpreter proves the dependency boundary even when the parent
    # pytest process has already imported every collector adapter.
    child = subprocess.run([sys.executable, "-c", """
import importlib
import importlib.abc
import sys

class RejectCollectionRuntime(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'loom_execution_capacity_collector.collector',
            'loom_execution_capacity_collector.config',
            'loom_execution_capacity_collector.nebius',
        }:
            raise AssertionError('capacity read bootstrapped collection runtime: ' + fullname)

sys.meta_path.insert(0, RejectCollectionRuntime())
importlib.import_module(sys.argv[1])
""", module], env=dict(os.environ), capture_output=True, text=True, timeout=15, check=False)
    assert child.returncode == 0, child.stderr


def test_package_collection_export_preserves_the_real_callable():
    import loom_execution_capacity_collector as package
    from loom_execution_capacity_collector.collector import collect_capacity_observation

    assert package.collect_capacity_observation is collect_capacity_observation
    assert package.__all__ == ["collect_capacity_observation"]
    with pytest.raises(AttributeError):
        _ = package.missing_collector_export
