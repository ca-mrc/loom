"""The retained development collector becomes the single read-only pool observer."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def collector_inputs(platform_inputs, runtime_inputs):
    from loom.nebius_platform_render import build_platform
    from loom_service.pool_management.installation import PoolInstallation

    request, _, _, _ = runtime_inputs
    participant, = [row for row in request.registration.spec.participants if row.environment_class == "development"]
    guard, = [row for row in request.guards if row.participant_id == participant.participant_id]
    config, candidate, profile = copy.deepcopy(platform_inputs)
    candidate["source_ref"] = "refs/heads/dev"
    config.update(namespace=guard.namespace, execution_namespace=participant.execution_namespace.name,
        execution_node_group_id=request.registration.spec.node_group_id)
    documents = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])["60-execution.yaml"]
    cronjob, = [row for row in documents if row["kind"] == "CronJob"]
    configmap, = [row for row in documents if row["kind"] == "ConfigMap"]
    for doc in (cronjob, configmap):
        doc["metadata"].update(uid=str(uuid4()), resourceVersion="1")
    spec = request.registration.spec.model_dump(mode="json")
    spec["quota_identities"] = {
        "nodes": ["tenant-test", "eu-north1", "compute", "compute.instance.count", "count"],
        "vcpu": ["tenant-test", "eu-north1", "compute", "compute.instance.non-gpu.vcpu", "count"],
        "storage": ["tenant-test", "eu-north1", "compute", "compute.disk.size.network-ssd", "byte"],
    }
    request = replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(spec)))
    return request, cronjob, configmap


def test_single_collector_consumes_pool_mode_and_preserves_cloud_authority(collector_inputs, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_collector

    from loom_execution_capacity_collector.config import (
        CapacityCollectorModeSettings,
        PoolCapacityCollectorSettings,
    )

    request, original, configmap = collector_inputs
    before = copy.deepcopy((original, configmap))
    result = wire_collector(request=request, original=original, config_map=configmap, management_origin="https://manage.example.com")
    assert (original, configmap) == before
    configuration, = result["configuration"]
    collector, = result["workload"]
    assert collector["metadata"]["uid"] == original["metadata"]["uid"]
    assert collector["spec"]["suspend"] is True and collector["spec"]["concurrencyPolicy"] == "Forbid"
    assert configuration["immutable"] is True and "uid" not in configuration["metadata"]
    pod = collector["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    container, = pod["containers"]
    init, = pod["initContainers"]
    assert container["envFrom"] == [{"configMapRef": {"name": configuration["metadata"]["name"]}}]
    assert container["command"] == ["python", "-m", "loom_execution_capacity_collector"]
    image = request.registration.candidate["images"]["execution_actuator"]["image_ref"]
    assert container["image"] == init["image"] == image
    for name, value in configuration["data"].items():
        monkeypatch.setenv(name, value)
    for row in container["env"]:
        monkeypatch.setenv(row["name"], row["value"])
    assert CapacityCollectorModeSettings(_env_file=None).collection_mode == "pool"
    settings = PoolCapacityCollectorSettings(_env_file=None)
    assert settings.pool_id == request.registration.spec.pool_id
    assert settings.management_url == "https://manage.example.com"
    assert str(settings.management_bearer_token_file) == "/var/run/loom-owned/credentials/control-plane-token"
    assert settings.nebius_node_group_id == request.registration.spec.node_group_id
    assert settings.nebius_quota_parent_id == "tenant-test"
    assert settings.quota_memory_name is None  # Do not invent a provider quota.
    projected, = [row for row in pod["volumes"] if row["name"] == "projected-credentials"]
    old_pod = original["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    old_projected, = [row for row in old_pod["volumes"] if row["name"] == "projected-credentials"]
    assert projected["projected"]["sources"][0] == old_projected["projected"]["sources"][0]
    observer, = [row for row in request.registration.spec.machines if row.role == "observer"]
    assert projected["projected"]["sources"][1] == {"secret": {
        "name": "loom-pool-machine-" + observer.machine_id.hex, "items": [{"key": "token", "path": "control-plane-token"}]}}
    serialized = json.dumps(result)
    assert "CONTROL_PLANE_URL" not in serialized and "CONTROL_PLANE_BEARER_TOKEN_FILE" not in serialized
    assert "loom-execution-capacity-collector-control-plane" not in serialized


@pytest.mark.parametrize("field", ["NEBIUS_NODE_GROUP_ID", "NEBIUS_PROJECT_ID", "QUOTA_VCPU_NAME", "NEBIUS_REGION"])
def test_collector_cannot_borrow_missing_retained_authority_from_operator_environment(collector_inputs, monkeypatch, field):
    from scripts.ops.nebius_pool_runtime import wire_collector

    request, original, configmap = collector_inputs
    key = "LOOM_EXECUTION_CAPACITY_COLLECTOR_" + field
    monkeypatch.setenv(key, configmap["data"].pop(key))
    with pytest.raises(ValueError, match="pool_collector_runtime_unqualified"):
        wire_collector(request=request, original=original, config_map=configmap, management_origin="https://manage.example.com")


def test_collector_projection_ignores_operator_settings_with_complete_retained_inputs(collector_inputs, monkeypatch):
    from scripts.ops.nebius_pool_runtime import wire_collector

    request, original, configmap = collector_inputs
    expected = wire_collector(request=request, original=original, config_map=configmap, management_origin="https://manage.example.com")
    for key in configmap["data"]:
        monkeypatch.setenv(key, "foreign-operator-value")
    assert wire_collector(request=request, original=original, config_map=configmap,
        management_origin="https://manage.example.com") == expected


@pytest.mark.parametrize("damage", ["namespace", "cloud_group", "quota_parent", "quota_unit", "plain_http",
    "credential_url", "config_reference", "old_token", "extra_container", "already_pool"])
def test_collector_rejects_unqualified_source_or_legacy_binding(collector_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_collector

    request, cronjob, configmap = collector_inputs
    pod = cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    origin = "https://manage.example.com"
    if damage == "namespace":
        cronjob["metadata"]["namespace"] = request.guards[0].namespace
    elif damage == "cloud_group":
        configmap["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID"] = "foreign"
    elif damage == "quota_parent":
        configmap["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_QUOTA_PARENT_ID"] = "foreign"
    elif damage == "quota_unit":
        configmap["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_VCPU_UNIT"] = "vcpu"
    elif damage == "plain_http":
        origin = "http://manage.example.com"
    elif damage == "credential_url":
        origin = "https://secret@manage.example.com"
    elif damage == "config_reference":
        pod["containers"][0]["envFrom"][0]["configMapRef"]["name"] = "foreign"
    elif damage == "old_token":
        pod["volumes"][0]["projected"]["sources"][1]["secret"]["name"] = "foreign"
    elif damage == "extra_container":
        pod["containers"].append(copy.deepcopy(pod["containers"][0]))
    else:
        configmap["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE"] = "pool"
    with pytest.raises(ValueError):
        wire_collector(request=request, original=cronjob, config_map=configmap, management_origin=origin)


@pytest.mark.parametrize('damage', ['initializer_mount', 'reader_mount', 'extra_volume', 'initializer_env',
    'initializer_args', 'reader_args', 'initializer_lifecycle', 'reader_lifecycle', 'projected_mode', 'subpath'])
def test_collector_rejects_a_credential_path_that_does_not_use_the_qualified_secret(collector_inputs, damage):
    from scripts.ops.nebius_pool_runtime import wire_collector

    request, cronjob, configmap = collector_inputs
    pod = cronjob['spec']['jobTemplate']['spec']['template']['spec']
    initializer, = pod['initContainers']
    reader, = pod['containers']
    if damage == 'initializer_mount':
        initializer['volumeMounts'][0]['name'] = 'foreign'
    elif damage == 'reader_mount':
        reader['volumeMounts'][0]['name'] = 'foreign'
    elif damage == 'extra_volume':
        pod['volumes'].append({'name': 'foreign', 'emptyDir': {}})
    elif damage == 'initializer_env':
        initializer['env'] = [{'name': 'PYTHONPATH', 'value': '/foreign'}]
    elif damage in {'initializer_args', 'reader_args'}:
        (initializer if damage == 'initializer_args' else reader)['args'] = ['--source', '/foreign']
    elif damage in {'initializer_lifecycle', 'reader_lifecycle'}:
        (initializer if damage == 'initializer_lifecycle' else reader)['lifecycle'] = {
            'postStart': {'exec': {'command': ['sh', '-c', 'replace-credentials']}}}
    elif damage == 'projected_mode':
        pod['volumes'][0]['projected']['defaultMode'] = 0o666
    else:
        reader['volumeMounts'][0]['subPath'] = 'foreign'
    with pytest.raises(ValueError, match='pool_collector_runtime_unqualified'):
        wire_collector(request=request, original=cronjob, config_map=configmap, management_origin='https://manage.example.com')
