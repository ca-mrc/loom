"""The minimal Harbor controller must not import ingestion/control-plane code."""

import subprocess
import sys


def test_native_controller_import_does_not_load_ingestion_or_materialization() -> None:
    script = """
import importlib.abc
import sys
class RuntimeBoundary(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {'loom.nebius_terminus_ingest', 'loom.service_execution_materialization'}:
            raise ImportError('controller crossed the runtime dependency boundary: ' + fullname)
sys.meta_path.insert(0, RuntimeBoundary())
import loom.service_execution_sandbox_task
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_native_private_reward_failure_preserves_script_return_code(tmp_path) -> None:
    import os

    from loom.harbor_verifier_script import native_verifier_workdir_run_sh_bytes

    tests, logs = tmp_path / "tests", tmp_path / "logs"
    tests.mkdir()
    logs.mkdir()
    (tests / "test.sh").write_text("exit 70\n")
    output = tmp_path / "result.json"
    script = tmp_path / "bridge.sh"
    script.write_bytes(native_verifier_workdir_run_sh_bytes())
    result = subprocess.run(["/bin/sh", str(script)], capture_output=True, text=True,
        env={**os.environ, "LOOM_TASK_DIR":str(tmp_path), "LOOM_HARBOR_LOG_DIR":str(logs),
             "LOOM_HARBOR_TEST_DIR":str(tmp_path / "mounted-tests"),
             "LOOM_VERIFIER_OUTPUT":str(output)}, timeout=10, check=False)
    assert result.returncode != 0 and not output.exists()
    assert "harbor_test_sh_returncode=70" in result.stderr
    assert "harbor_reward_error=" in result.stderr
