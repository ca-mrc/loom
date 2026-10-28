from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from loom.models.capabilities import Capabilities, RequiredCapabilities
from loom.models.task import EnvironmentConfig
from loom.models.trial import TrialConfig
from loom_control_plane.scheduler.requires_caps import derive_requires_caps
from loom_service.execution_admission import (
    admit_execution_backend,
    reject_unsupported_mainland_proxy,
)
from tests.unit.test_requires_caps import _task


@pytest.mark.parametrize("agent", ["terminus-2", "oracle", "direct-completion", "litellm"])
def test_mainland_proxy_is_explicit_and_roundtrips(agent):
    ordinary = TrialConfig(agent_name=agent, agent_model=None)
    assert ordinary.mainland_proxy is False
    enabled = TrialConfig(agent_name=agent, agent_model=None, mainland_proxy=True)
    assert TrialConfig.model_validate(enabled.model_dump(mode="json")).mainland_proxy is True
    task = _task(EnvironmentConfig(os="linux"))
    default_caps = derive_requires_caps(task)
    disabled_caps = derive_requires_caps(task, ordinary)
    enabled_caps = derive_requires_caps(task, enabled)
    assert default_caps.mainland_proxy is disabled_caps.mainland_proxy is False
    assert "mainland_proxy" not in default_caps.model_dump(mode="json")
    assert "mainland_proxy" not in disabled_caps.model_dump(mode="json")
    assert enabled_caps.model_dump(mode="json")["mainland_proxy"] is True
    assert RequiredCapabilities.model_validate(enabled_caps.model_dump(mode="json")).mainland_proxy is True


@pytest.mark.parametrize("agent", ["claude-code", "codex", "opencode", "custom-inbox"])
def test_mainland_proxy_rejects_in_sandbox_model_clients(agent):
    assert not TrialConfig(agent_name=agent, agent_model=None).mainland_proxy
    with pytest.raises(ValidationError, match="mainland_proxy supports only"):
        TrialConfig(agent_name=agent, agent_model=None, mainland_proxy=True)


@pytest.mark.parametrize("value", ["true", "false", 0, 1, None])
def test_mainland_proxy_requires_an_actual_boolean(value):
    with pytest.raises(ValidationError):
        TrialConfig(agent_name="oracle", agent_model=None, mainland_proxy=value)


def test_mainland_proxy_requires_an_advertised_worker_capability():
    worker = Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies={"public"},
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes={"auto"},
    )
    ordinary = RequiredCapabilities(os="linux", gpu_vendor="none", network_policies={"public"})
    enabled = ordinary.model_copy(update={"mainland_proxy": True})
    assert ordinary.satisfied_by(worker)
    assert not enabled.satisfied_by(worker)
    assert enabled.satisfied_by(worker.model_copy(update={"mainland_proxy": True}))


@pytest.mark.parametrize("backend", ["modal", "fake"])
def test_mainland_proxy_rejects_unsupported_backends(backend):
    reject_unsupported_mainland_proxy(backend, {})
    reject_unsupported_mainland_proxy(backend, {"mainland_proxy": False})
    with pytest.raises(HTTPException) as error:
        reject_unsupported_mainland_proxy(backend, {"mainland_proxy": True})
    assert error.value.status_code == 400
    assert error.value.detail["reason"] == "mainland_proxy_unsupported_backend"
    reject_unsupported_mainland_proxy("docker", {"mainland_proxy": True})
    reject_unsupported_mainland_proxy("nebius", {"mainland_proxy": True})


async def test_unsupported_mainland_proxy_is_rejected_before_capacity_or_image_lookup(monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    session = AsyncMock()
    with pytest.raises(HTTPException) as error:
        await admit_execution_backend(
            session,
            backend="modal",
            task_ids=[],
            trial_config={"agent_name": "oracle", "agent_model": None, "mainland_proxy": True},
            combinations=[],
            runtime_profile_json="",
        )
    assert error.value.detail["reason"] == "mainland_proxy_unsupported_backend"
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("value", ["true", "false", 1, None])
async def test_batch_admission_rejects_invalid_mainland_flag_before_lookup(monkeypatch, value):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    session = AsyncMock()
    with pytest.raises(HTTPException) as error:
        await admit_execution_backend(
            session,
            backend="docker",
            task_ids=[],
            trial_config={"agent_name": "oracle", "agent_model": None, "mainland_proxy": value},
            combinations=[],
            runtime_profile_json="",
        )
    assert error.value.detail["reason"] == "invalid_mainland_proxy_config"
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("enabled", [False, True])
async def test_static_nebius_binding_cannot_ignore_mainland_proxy(monkeypatch, enabled):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from tests.integration.test_service_batches_crud import _service_execution_task_config

    monkeypatch.setenv("LOOM_ENV", "production")
    task_id = "bound-mainland-task"
    rows = Mock()
    rows.all.return_value = [
        (task_id, _service_execution_task_config(task_id), {}, "a" * 64, None, None)
    ]
    session = SimpleNamespace(execute=AsyncMock(return_value=rows))
    pools = AsyncMock(return_value=[SimpleNamespace(pool_name="nebius-cpu")])
    monkeypatch.setattr(
        "loom_service.execution_admission.get_service_execution_backend_pools", pools
    )
    args = dict(
        backend="nebius",
        task_ids=[task_id],
        trial_config={"agent_name": "oracle", "agent_model": None, "mainland_proxy": enabled},
        combinations=[],
        runtime_profile_json="{}",
    )
    if enabled:
        with pytest.raises(HTTPException) as error:
            await admit_execution_backend(session, **args)
        assert error.value.status_code == 400
        assert error.value.detail["reason"] == "mainland_proxy_bound_execution_unsupported"
        pools.assert_not_awaited()
    else:
        assert await admit_execution_backend(session, **args) is None
        pools.assert_awaited_once()


def test_static_binding_is_not_used_by_local_docker():
    reject_unsupported_mainland_proxy("docker", {"mainland_proxy": True}, has_service_binding=True)
