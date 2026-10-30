import json
import subprocess
import sys

import pytest

from loom_execution_actuator.config import ExecutionActuatorSettings


def test_execution_actuator_settings_parse_target_placement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_DB_URL", "postgresql+asyncpg://loom@db/loom")
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID", "actuator-test")
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_TARGET_ID", "nebius-eu-north1-development")
    monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_NAMESPACE", "loom-nebius-development")
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR",
        '{"loom.nebius/node-role":"execution"}',
    )
    monkeypatch.setenv(
        "LOOM_EXECUTION_ACTUATOR_TOLERATIONS",
        '[{"key":"loom.nebius/execution","operator":"Equal","value":"true","effect":"NoSchedule"}]',
    )

    settings = ExecutionActuatorSettings()

    assert settings.node_selector == {"loom.nebius/node-role": "execution"}
    assert settings.tolerations == (
        {
            "key": "loom.nebius/execution",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        },
    )


def test_optional_native_build_loop_parses_env_and_participates_in_readiness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_execution_actuator.__main__ import ActuatorRuntimeHealth

    values = {
        "DB_URL": "postgresql+asyncpg://loom@db/loom",
        "CONTROLLER_ID": "actuator",
        "TARGET_ID": "primary",
        "NAMESPACE": "loom-execution",
        "TASK_IMAGE_BUILDER": json.dumps(
            {
                "namespace": "loom-execution-build",
                "service_image": "registry.example/service@sha256:" + "a" * 64,
                "storage_endpoint": "https://storage.example",
                "storage_region": "eu-north1",
                "source_bucket": "source",
                "cache_bucket": None,
                "cache_secret_name": None,
                "registry_repository": "registry.example/task-images",
            }
        ),
    }
    for key, value in values.items():
        monkeypatch.setenv("LOOM_EXECUTION_ACTUATOR_" + key, value)
    settings = ExecutionActuatorSettings()
    assert settings.task_image_builder is not None
    assert settings.task_image_builder.max_concurrent is None
    health = ActuatorRuntimeHealth(stale_after_seconds=90, task_image_builder_enabled=True)
    health.mark_success("command")
    health.mark_success("reconcile")
    assert not health.ready()
    health.mark_success("build")
    assert health.ready()


def test_actuator_main_prints_its_own_info_logs_but_not_dependency_info() -> None:
    script = """
import logging
import loom_execution_actuator.__main__ as actuator
from loom_execution_actuator.metrics import record_build_cycle

async def run():
    record_build_cycle("native", outstanding=1, reconcile_seconds=0.1, outcome="idle", admission_seconds=0.1)
    logging.getLogger("httpx").info("dependency info line")

actuator._run = run
actuator.main()
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True, timeout=60)

    assert "Native build cycle controller=native outstanding=1" in result.stderr
    assert "dependency info line" not in result.stderr
