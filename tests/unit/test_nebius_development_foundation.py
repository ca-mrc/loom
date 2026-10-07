"""Shared development has its own data foundation, not staging's namespace."""

from __future__ import annotations

import copy
import json

import pytest

from loom.nebius_application_render import render_application
from loom.nebius_platform_render import NebiusPlatformError, build_platform
from tests.unit.test_nebius_application_render import inputs, named
from tests.unit.test_nebius_platform_render import ROOT
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def development_inputs(platform_inputs):
    config, candidate, profile = copy.deepcopy(platform_inputs)
    config.update(
        namespace="loom-dev", execution_namespace="loom-nebius-dev-execution",
        target_id="nebius-eu-north1-shared-dev", public_host="shared-dev.example.com",
        public_allocation_id="development-allocation",
    )
    config["buckets"] = {purpose: "loom-dev-" + purpose for purpose in config["buckets"]}
    config["capacity_policy"]["enabled"] = False
    return config, candidate, profile


def test_shared_development_renders_own_data_and_runtime_endpoints(development_inputs):
    config, candidate, profile = development_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    assert {doc["metadata"]["name"] for doc in files["00-namespaces.yaml"]} == {
        "loom-dev", "loom-nebius-dev-execution",
    }
    postgres = next(doc for doc in files["20-database.yaml"] if doc["kind"] == "StatefulSet")
    assert postgres["metadata"]["namespace"] == "loom-dev"
    assert postgres["spec"]["persistentVolumeClaimRetentionPolicy"]["whenDeleted"] == "Retain"
    service = next(doc for doc in files["40-services.yaml"] if doc["kind"] == "Deployment"
                   and doc["metadata"]["name"] == "loom-service")
    env = {item["name"]: item.get("value") for item in service["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["LOOM_NAMESPACE"] == "loom-dev"
    assert env["LOOM_ENV"] == "development"
    assert env["LOOM_SVC_CONTROL_PLANE_URL"] == "http://loom-control-plane.loom-dev.svc:8080"
    assert env["LOOM_SVC_GATEWAY_URL"] == "http://loom-llm-gateway.loom-dev.svc:9100"
    assert env["LOOM_SVC_ARTIFACTS_BUCKET"] == "loom-dev-artifacts"
    assert env["LOOM_SVC_TRAJECTORIES_BUCKET"] == "loom-dev-trajectories"
    cm = next(doc for doc in files["10-config-network.yaml"] if doc["kind"] == "ConfigMap")
    target = json.loads(cm["data"]["catalog.json"])["topology"]["targets"][0]
    assert target["target_id"] == "nebius-eu-north1-shared-dev"
    assert target["namespace_name"] == "loom-nebius-dev-execution"
    assert json.loads(cm["data"]["environment.json"])["capacity_policy"]["enabled"] is False


def test_new_dev_render_does_not_adopt_existing_platform_objects(platform_inputs, development_inputs):
    before = copy.deepcopy(platform_inputs)

    def identities(value):
        config, candidate, profile = value
        return {
            (doc["apiVersion"], doc["kind"], doc["metadata"].get("namespace"), doc["metadata"]["name"])
            for group in build_platform(config, candidate, profile, {}, repo_root=ROOT).values()
            for doc in group
        }

    assert not identities(platform_inputs) & identities(development_inputs)
    assert platform_inputs == before


@pytest.mark.parametrize("slug", ["alice", "bob", "execution"])
def test_personal_apis_bind_to_canonical_shared_development(development_inputs, slug):
    row, release, shared, foundation = inputs(development_inputs, slug)
    result = render_application(row, release, shared, foundation)
    api = named(result, "Deployment", "loom-service")
    env = {item["name"]: item.get("value") for item in api["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert api["metadata"]["namespace"] == "loom-dev-" + slug
    assert env["LOOM_SVC_SERVICE_MODE"] == "api_only"
    assert env["LOOM_SVC_CONTROL_PLANE_URL"] == "http://loom-control-plane.loom-dev.svc:8080"
    assert env["LOOM_SVC_GATEWAY_URL"] == "http://loom-llm-gateway.loom-dev.svc:9100"
    assert env["LOOM_SVC_ARTIFACTS_BUCKET"] == "loom-dev-artifacts"
    assert shared.platform_namespace == "loom-dev"
    assert not {"StatefulSet", "PersistentVolumeClaim", "Job", "CronJob"} & {
        doc["kind"] for group in result.files.values() for doc in group
    }


@pytest.mark.parametrize("change", [
    {"namespace": "loom-dev-alice"},
    {"namespace": "loom-staging"},
    {"namespace": "loom-prod"},
    {"execution_namespace": "loom-dev"},
    {"execution_namespace": "loom-dev-execution"},
    {"execution_namespace": "loom-staging"},
    {"environment": "staging"},
    {"environment": "production"},
])
def test_shared_dev_namespace_exception_does_not_admit_other_boundaries(development_inputs, change):
    config, candidate, profile = development_inputs
    with pytest.raises(NebiusPlatformError):
        build_platform({**config, **change}, candidate, profile, {}, repo_root=ROOT)
