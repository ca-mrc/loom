"""#2841: local Docker workers advertise gateway-only only with an internal
sandbox gateway network."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import loom_worker.main_loop as ml
from loom.models.capabilities import Capabilities, RequiredCapabilities


class _Network:
    def __init__(self, internal: bool) -> None:
        self.attrs = {"Internal": internal}


def _fake_docker(monkeypatch: pytest.MonkeyPatch, networks: dict[str, bool]) -> list[str]:
    import docker

    looked_up: list[str] = []

    class _Networks:
        def get(self, name: str) -> _Network:
            looked_up.append(name)
            if name not in networks:
                raise docker.errors.NotFound(f"network {name} not found")
            return _Network(networks[name])

    class _Client:
        networks = _Networks()

        def close(self) -> None:
            pass

    monkeypatch.setattr(docker, "from_env", lambda: _Client())
    ml._internal_docker_network.cache_clear()
    return looked_up


def _settings(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "mainland_proxy_image": "",
        "mainland_proxy_profile_dir": None,
        "sandbox_gateway_network": "loom-dev-sandbox-gateway",
        "sandbox_isolation": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _policies(settings: SimpleNamespace) -> list[str]:
    return list(ml._worker_capabilities(settings)[0]["network_policies"])


def test_internal_gateway_network_adds_gateway_only(monkeypatch: pytest.MonkeyPatch) -> None:
    looked_up = _fake_docker(monkeypatch, {"loom-dev-sandbox-gateway": True})
    settings = _settings()

    assert _policies(settings) == ["public", "no-network", "allowlist", "gateway-only"]
    _policies(settings)
    assert looked_up == ["loom-dev-sandbox-gateway"]  # checked once per process

    caps = Capabilities.model_validate(ml._worker_capabilities(settings)[0])
    required = RequiredCapabilities(os="linux", gpu_vendor="none", network_policies=frozenset({"gateway-only"}))
    assert required.satisfied_by(caps)
    assert ml._DEFAULT_CAPS[0]["network_policies"] == ["public", "no-network", "allowlist"]


@pytest.mark.parametrize(
    ("networks", "overrides"),
    [
        pytest.param({"loom-dev-sandbox-gateway": False}, {}, id="network-not-internal"),
        pytest.param({}, {}, id="network-missing"),
        pytest.param({"loom-dev-sandbox-gateway": True}, {"sandbox_gateway_network": ""}, id="not-configured"),
        pytest.param({"loom-dev-sandbox-gateway": True}, {"sandbox_isolation": True}, id="sandbox-isolation"),
    ],
)
def test_gateway_only_is_not_advertised_without_an_internal_gateway_network(
    monkeypatch: pytest.MonkeyPatch,
    networks: dict[str, bool],
    overrides: dict[str, Any],
) -> None:
    _fake_docker(monkeypatch, networks)
    assert _policies(_settings(**overrides)) == ["public", "no-network", "allowlist"]


@pytest.mark.asyncio
async def test_terminus2_gets_a_layer_with_its_sandbox_deps(monkeypatch: pytest.MonkeyPatch) -> None:
    """#2841: Terminus-2's tmux/asciinema install happens at image build."""
    from loom.agent.terminus2.harbor_environment import SANDBOX_DEPS_INSTALL_SCRIPT

    calls: list[dict[str, Any]] = []

    async def _resolve(**kwargs: Any) -> str:
        calls.append(kwargs)
        return "loom-trial-cache:abc"

    monkeypatch.setattr(ml, "resolve_trial_image", _resolve)
    common: dict[str, Any] = {
        "task_image": "task:latest", "settings": object(), "cp_client": object(), "worker_id": object(),
    }

    assert await ml._resolve_layered_trial_image(agent_name="terminus-2", **common) == "loom-trial-cache:abc"
    assert calls[0]["adapter"].install_script == SANDBOX_DEPS_INSTALL_SCRIPT
    assert await ml._resolve_layered_trial_image(agent_name="oracle", **common) == "task:latest"
    assert len(calls) == 1
