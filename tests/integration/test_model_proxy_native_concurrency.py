"""Real isolated model transport concurrency; no provider/cloud traffic."""
from __future__ import annotations

import shutil
import subprocess

import pytest

from tests.support.model_proxy_concurrency import (
    CONNECTION_IDS,
    envoy_renderer_environment,
    native_envoy_case,
    native_ingress_case,
)
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs

pytestmark = pytest.mark.docker


@pytest.fixture(scope="module", autouse=True)
def docker_available() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker unavailable for native proxy check")
    if subprocess.run(["docker", "info"], capture_output=True).returncode:
        pytest.skip("Docker daemon unavailable for native proxy check")


@pytest.fixture(scope="module")
def renderer_environment(docker_available):
    with envoy_renderer_environment() as environment:
        yield environment


@pytest.mark.timeout(180)
@pytest.mark.parametrize("scheme", ["http", "https"])
def test_native_envoy_exceeds_old_cid_threshold_and_releases_resources(scheme: str, renderer_environment) -> None:
    baseline = native_envoy_case(scheme, baseline=True, renderer_env=renderer_environment)
    blocked = baseline["same_cid"]
    assert blocked["requested"] == 1200
    assert blocked["active_before_release"] == blocked["arrived_before_release"] == 1024
    assert blocked["barrier_reached"] is False
    assert blocked["responses"] == {"200": 1024, "503": 176}
    assert baseline["active_at_end"] == 0

    fixed = native_envoy_case(scheme, baseline=False, renderer_env=renderer_environment)
    single = fixed["same_cid"]
    assert single["active_before_release"] == single["arrived_before_release"] == 1200
    assert single["barrier_reached"] is True and single["pending_before_release"] == 0
    assert single["cancelled"] == 600 and single["replacement_active"] == 1200
    assert single["responses"] == {"200": 1200}
    shared = fixed["three_cid_reuse"]
    assert shared["barrier_reached"] is True and shared["pending_before_release"] == 0
    assert shared["active_before_release"] == 1200
    assert shared["per_cid_before_release"] == {cid: 400 for cid in CONNECTION_IDS}
    assert shared["responses"] == {"200": 1200}
    assert fixed["upstream_statuses"] == [429, 500] and fixed["recovery"] == 200
    assert fixed["wrong_cid"] == fixed["wrong_authority"] == 404
    assert fixed["denied_never_arrived"] is True
    assert fixed["active_at_end"] == fixed["header_leaks"] == fixed["auth_errors"] == 0


@pytest.mark.timeout(180)
@pytest.mark.parametrize("protocol", ["h1", "h2"])
def test_native_ingress_preserves_model_concurrency_across_connections(regional_inputs, renderer_environment, protocol):
    result = native_ingress_case(regional_inputs[0], protocol=protocol, renderer_env=renderer_environment)
    if protocol == "h1":
        single = result["same_cid"]
        assert single["arrived_before_release"] == single["active_before_release"] == 1200
        assert single["cancelled"] == 600 and single["replacement_active"] == 1200
        assert single["responses"] == {"200": 1200}
        shared = result["three_cid_reuse"]
        assert shared["active_before_release"] == 1200
        assert shared["per_cid_before_release"] == {cid: 400 for cid in CONNECTION_IDS}
        assert shared["responses"] == {"200": 1200}
        # Stock Caddy returns empty 200 for an unmatched host. The transport
        # security contract is no forward/header leak, not an invented status.
        assert result["wrong_authority"] == 200 and result["wrong_authority_empty"] is True
        assert result["denied_never_arrived"] is True
    else:
        multiplexed = result["h2"]
        assert multiplexed["active_before_release"] == 1200
        assert multiplexed["connections"] > 1
        assert all(0 < cap < 1200 for cap in multiplexed["peer_max_streams"])
        assert multiplexed["cancelled"] == 600 and multiplexed["replacement_active"] == 1200
        assert multiplexed["per_cid"] == {cid: 400 for cid in CONNECTION_IDS}
    assert result["upstream_statuses"] == [429, 500] and result["recovery"] == 200
    assert result["status_request_counts"] == {"429": 1, "500": 1}
    assert result["active_at_end"] == result["header_leaks"] == result["auth_errors"] == 0
