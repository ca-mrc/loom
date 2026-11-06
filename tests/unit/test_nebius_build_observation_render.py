"""Opt-in builder observation in rendering and environment validation (#2750)."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import NebiusPlatformError, build_platform, validate_environment
from loom.nebius_resource_observer_render import NAME, builder_observation_namespace
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_platform_render_regional_builder import builder

ROOT = Path(__file__).resolve().parents[2]


def _render(config, candidate, profile):
    return [doc for docs in build_platform(config, candidate, profile, {}, repo_root=ROOT).values() for doc in docs]


def _env(doc):
    return {row["name"]: row.get("value") for row in doc["spec"]["template"]["spec"]["containers"][0].get("env", [])}


def _with_builder(platform_inputs, *, flag=None):
    config, candidate, profile = deepcopy(platform_inputs)
    config["task_image_builder"] = builder()
    if flag is not None:
        config["resource_observer_builds"] = flag
    return config, candidate, profile


def test_flag_off_renders_exactly_what_it_did_before(platform_inputs) -> None:
    config, candidate, profile = _with_builder(platform_inputs)
    unset = _render(config, candidate, profile)
    explicit = _render({**config, "resource_observer_builds": False}, candidate, profile)
    # Any saved-config edit changes the configuration-revision annotation; what the flag
    # controls (RBAC, the observer DaemonSet, the actuator environment) must be identical.
    def controlled(docs):
        kept = [doc for doc in docs if doc["kind"] in {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding",
                                                        "DaemonSet", "NetworkPolicy"}]
        actuator = next(doc for doc in docs if doc["kind"] == "Deployment"
                        and doc["metadata"]["name"] == "loom-execution-actuator")
        return json.dumps([kept, _env(actuator)], sort_keys=True)

    assert controlled(unset) == controlled(explicit)
    build_ns = config["execution_namespace"] + "-build"
    assert not any(doc["kind"] in {"Role", "RoleBinding"} and doc["metadata"].get("namespace") == build_ns
                   and doc["metadata"]["name"] == NAME for doc in unset)
    observer = next(doc for doc in unset if doc["kind"] == "DaemonSet" and doc["metadata"]["name"] == NAME)
    assert "LOOM_RESOURCE_OBSERVER_BUILD_NAMESPACE" not in _env(observer)
    actuator = next(doc for doc in unset if doc["kind"] == "Deployment"
                    and doc["metadata"]["name"] == "loom-execution-actuator")
    assert "LOOM_EXECUTION_ACTUATOR_BUILD_RESOURCE_OBSERVATIONS" not in _env(actuator)


def test_flag_on_grants_only_build_pod_reads_and_enables_both_sides(platform_inputs) -> None:
    config, candidate, profile = _with_builder(platform_inputs, flag=True)
    docs = _render(config, candidate, profile)
    build_ns = config["execution_namespace"] + "-build"
    assert builder_observation_namespace(config) == build_ns
    role = next(doc for doc in docs if doc["kind"] == "Role" and doc["metadata"].get("namespace") == build_ns
                and doc["metadata"]["name"] == NAME)
    assert role["rules"] == [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}]
    binding = next(doc for doc in docs if doc["kind"] == "RoleBinding" and doc["metadata"].get("namespace") == build_ns
                   and doc["metadata"]["name"] == NAME)
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": NAME,
                                    "namespace": config["execution_namespace"] + "-observations"}]
    observer = next(doc for doc in docs if doc["kind"] == "DaemonSet" and doc["metadata"]["name"] == NAME)
    assert _env(observer)["LOOM_RESOURCE_OBSERVER_BUILD_NAMESPACE"] == build_ns
    actuator = next(doc for doc in docs if doc["kind"] == "Deployment"
                    and doc["metadata"]["name"] == "loom-execution-actuator")
    assert _env(actuator)["LOOM_EXECUTION_ACTUATOR_BUILD_RESOURCE_OBSERVATIONS"] == "true"


@pytest.mark.parametrize(("value", "with_builder"), [("true", True), (1, True), (True, False)])
def test_flag_requires_a_boolean_and_a_configured_builder(platform_inputs, value, with_builder) -> None:
    config, _, _ = deepcopy(platform_inputs)
    if with_builder:
        config["task_image_builder"] = builder()
    config["resource_observer_builds"] = value
    with pytest.raises(NebiusPlatformError):
        validate_environment(config)


def test_retained_runtime_authority_excludes_the_newer_table_and_upgrades_include_it() -> None:
    from loom.nebius_development_runtime_database import _authority
    from loom.nebius_platform_bootstrap import ACTUATOR_TASK_IMAGE_WRITES

    table = "task_image_build_resource_usage"
    assert ACTUATOR_TASK_IMAGE_WRITES[table] == ("INSERT", "UPDATE")  # never DELETE
    retained, _ = _authority()
    upgraded, _ = _authority(include_newer=True)
    assert table not in retained  # the retained 0176 foundation has no such table
    assert upgraded[table] == {"SELECT", "INSERT", "UPDATE"}
    assert {name: privileges for name, privileges in upgraded.items() if name != table} == retained
