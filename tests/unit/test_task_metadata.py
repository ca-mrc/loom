from pathlib import PurePosixPath

import pytest
from pydantic import ValidationError

from loom.models.networking import Public
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    TaskMetadata,
    VerifierDefaults,
)


def test_task_metadata():
    m = TaskMetadata(id="skillflow-task-17", name="Implement binary search")
    assert m.id == "skillflow-task-17"
    assert m.labels == []


def test_environment_config_defaults():
    env = EnvironmentConfig(os="linux")
    assert env.gpu_vendor == "none"
    assert env.docker_image is None
    assert env.dockerfile is None
    assert env.workdir == PurePosixPath("/workspace")
    assert env.user == "agent"
    assert env.build_timeout_sec == 1200
    assert env.baseline_network_policy == Public()


def test_environment_config_with_image():
    env = EnvironmentConfig(os="linux", docker_image="python:3.11-slim")
    assert env.docker_image == "python:3.11-slim"


def test_agent_defaults():
    d = AgentDefaults(name="litellm-agent")
    assert d.timeout_sec == 1800
    assert d.setup_timeout_sec == 360
    assert d.skills == []


def test_verifier_defaults():
    d = VerifierDefaults(name="pytest")
    assert d.env_mode == "separate"
    assert d.timeout_sec == 300


def test_environment_config_immutable():
    env = EnvironmentConfig(os="linux")
    with pytest.raises(ValidationError):
        env.os = "windows"  # type: ignore[misc]
