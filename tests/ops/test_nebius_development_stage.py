"""Fixed dev resource phases retain identity and never retry uncertain creates."""
from __future__ import annotations

import copy
import importlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_development_stage")


@pytest.fixture
def inputs(platform_inputs):
    from scripts.ops.nebius_development_bootstrap import DevelopmentBootstrapBinding

    config, candidate, profile = copy.deepcopy(platform_inputs)
    config.update(namespace="loom-dev", execution_namespace="loom-nebius-dev-execution")
    candidate["source_ref"] = "refs/heads/dev"
    bootstrap = DevelopmentBootstrapBinding(str(uuid4()), str(uuid4()), config["db_tls_secret_name"])
    binding = module().DevelopmentResourceBinding(bootstrap, str(uuid4()), str(uuid4()))
    selection = module().DevelopmentStageInput(config, candidate, profile, {}, {
        "access-key": "test-dev-access", "secret-key": "test-dev-secret",
        "source-access-key": "test-source-access", "source-secret-key": "test-source-secret",
    })
    return selection, binding, PhaseAPI(binding)


def run(inputs, state, phase="config"):
    selection, binding, api = inputs
    return module().stage_development_resources(selection=selection, binding=binding,
        api=api, phase=phase, state_dir=state)


def ready(inputs, state, phase):
    selection, binding, api = inputs
    return module().development_phase_ready(selection=selection, binding=binding,
        api=api, phase=phase, state_dir=state)


def test_fixed_dev_configuration_and_replay_create_once(inputs, tmp_path):
    first = run(inputs, tmp_path / "config")
    snapshot = copy.deepcopy(inputs[2].resources)
    journal = (tmp_path / "config/stage.json").read_bytes()
    assert first["status"] == "development_phase_staged"
    assert first["phase"] == "config"
    assert set(first["resource_uids"]) == {
        "ConfigMap:loom-platform-config", "ServiceAccount:loom-platform",
        "NetworkPolicy:default-deny-ingress", "NetworkPolicy:development-internal",
    }
    assert run(inputs, tmp_path / "config") == first
    assert inputs[2].resources == snapshot
    assert (tmp_path / "config/stage.json").read_bytes() == journal
    assert len(inputs[2].creates) == 4
    for row in inputs[2].resources.values():
        assert row["metadata"]["namespace"] == "loom-dev"
        assert row["metadata"]["labels"]["loom.nebius/development-installation"] == inputs[1].bootstrap.installation_id


@pytest.mark.parametrize("phase,count", [("supplied", 1), ("database", 2), ("migration", 1), ("services", 8)])
def test_only_fixed_private_resource_inventory_is_created(inputs, tmp_path, phase, count):
    receipt = run(inputs, tmp_path / phase, phase)
    assert len(receipt["resource_uids"]) == count
    assert run(inputs, tmp_path / phase, phase) == receipt
    assert len(inputs[2].creates) == count
    assert not {"Namespace", "Ingress", "Role", "RoleBinding", "CronJob"} & {
        doc["kind"] for doc in inputs[2].resources.values()}
    if phase == "database":
        db = inputs[2].resources["StatefulSet:loom-postgres"]
        assert db["spec"]["volumeClaimTemplates"][0]["metadata"]["labels"]["loom.nebius/development-installation"] == (
            inputs[1].bootstrap.installation_id)
    if phase == "supplied":
        secret = inputs[2].resources["Secret:loom-platform-storage"]
        assert secret["immutable"] is True
        assert set(secret["data"]) == {"access-key", "secret-key", "source-access-key", "source-secret-key"}


@pytest.mark.parametrize("point", ["before", "after"])
def test_uncertain_resource_create_is_read_back_without_retry(inputs, tmp_path, point):
    inputs[2].failure = point
    if point == "after":
        first = run(inputs, tmp_path / "config")
        assert run(inputs, tmp_path / "config") == first
    else:
        for _ in range(2):
            with pytest.raises(module().DevelopmentStageError, match="unresolved"):
                run(inputs, tmp_path / "config")
            inputs[2].failure = None
    assert len(inputs[2].creates) == (4 if point == "after" else 1)


@pytest.mark.parametrize("change", ["late_collision", "namespace", "resource_uid", "policy", "journal", "inputs"])
def test_drift_stops_before_any_further_resource_creation(inputs, tmp_path, change):
    selection, binding, api = inputs
    state = tmp_path / "config"
    if change == "late_collision":
        api.resources["NetworkPolicy:development-internal"] = {"foreign": True}
    else:
        run(inputs, state)
        if change == "namespace":
            api.binding = replace(binding, namespace_uid=str(uuid4()))
        elif change == "resource_uid":
            api.resources["ConfigMap:loom-platform-config"]["metadata"]["uid"] = str(uuid4())
        elif change == "policy":
            api.resources["NetworkPolicy:default-deny-ingress"]["spec"]["ingress"] = [{}]
        elif change == "journal":
            value = json.loads((state / "stage.json").read_bytes())
            value["phase"] = "services"
            (state / "stage.json").write_text(json.dumps(value))
        else:
            selection.storage["secret-key"] = "changed-dev-secret"
    count = len(api.creates)
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, state)
    assert len(api.creates) == count


def test_late_default_injection_is_rejected_before_first_persistent_write(inputs, tmp_path):
    def inject(doc):
        if doc["kind"] == "Service":
            doc["spec"]["externalIPs"] = ["203.0.113.9"]
    inputs[2].default_change = inject
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, tmp_path / "database", "database")
    assert not inputs[2].creates


@pytest.mark.parametrize("injected", [
    {"dataSource": {"apiGroup": "snapshot.storage.k8s.io", "kind": "VolumeSnapshot", "name": "foreign-data"}},
    {"dataSourceRef": {"kind": "PersistentVolumeClaim", "namespace": "loom-nebius-platform", "name": "data"}},
    {"volumeName": "foreign-pv"}, {"selector": {"matchLabels": {"imported": "true"}}},
])
def test_database_preview_cannot_adopt_clone_or_prebind_existing_data(inputs, tmp_path, injected):
    def inject(doc):
        if doc["kind"] == "StatefulSet":
            doc["spec"]["volumeClaimTemplates"][0]["spec"].update(injected)
    inputs[2].default_change = inject
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, tmp_path / "database", "database")
    assert not inputs[2].creates


@pytest.mark.parametrize("injected", [
    {"podFailurePolicy": {"rules": [{"action": "Ignore", "onExitCodes": {"operator": "In", "values": [1]}}]}},
    {"successPolicy": {"rules": [{"succeededCount": 0}]}}, {"parallelism": 2}, {"completions": 2},
    {"completionMode": "Indexed"}, {"suspend": True}, {"managedBy": "foreign.example/controller"},
])
def test_migration_preview_cannot_change_execution_or_retry_policy(inputs, tmp_path, injected):
    def inject(doc):
        if doc["kind"] == "Job":
            doc["spec"].update(injected)
    inputs[2].default_change = inject
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, tmp_path / "migration", "migration")
    assert not inputs[2].creates


@pytest.mark.parametrize("change", ["host_port", "dns", "scheduler", "host_alias", "image_policy"])
def test_service_preview_rejects_nondefault_pod_network_and_runtime_settings(inputs, tmp_path, change):
    def inject(doc):
        if doc["kind"] != "Deployment":
            return
        pod = doc["spec"]["template"]["spec"]
        if change == "host_port":
            pod["containers"][0]["ports"][0].update(hostPort=8080, hostIP="0.0.0.0")
        elif change == "dns":
            pod.update(dnsPolicy="None", dnsConfig={"nameservers": ["203.0.113.9"]})
        elif change == "scheduler":
            pod["schedulerName"] = "foreign-scheduler"
        elif change == "host_alias":
            pod["hostAliases"] = [{"ip": "203.0.113.9", "hostnames": ["loom-postgres.loom-dev.svc"]}]
        else:
            pod["containers"][0]["imagePullPolicy"] = "Never"
    inputs[2].default_change = inject
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, tmp_path / "services", "services")
    assert not inputs[2].creates


@pytest.mark.parametrize("phase", ["execution", "public", "backup", "../escape", "bootstrap"])
def test_unrelated_phases_are_not_a_manifest_apply_interface(inputs, tmp_path, phase):
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, tmp_path / "stage", phase)
    assert not inputs[2].creates


def test_source_inputs_cannot_target_staging_or_copy_extra_credentials(inputs, tmp_path):
    selection, _, api = inputs
    for field, value in [("namespace", "loom-nebius-platform"), ("environment", "staging")]:
        original = selection.config[field]
        selection.config[field] = value
        with pytest.raises(module().DevelopmentStageError):
            run(inputs, tmp_path / field)
        selection.config[field] = original
    selection.storage["backup-secret-key"] = "not-dev-runtime-material"
    with pytest.raises(module().DevelopmentStageError):
        run(inputs, tmp_path / "extra")
    assert not api.creates


def test_workload_readiness_requires_current_identity_generation_and_all_replicas(inputs, tmp_path):
    state = tmp_path / "services"
    run(inputs, state, "services")
    assert not ready(inputs, state, "services")
    for row in inputs[2].resources.values():
        if row["kind"] == "Deployment":
            row["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
                "updatedReplicas": 1, "availableReplicas": 1, "unavailableReplicas": 0}
    assert ready(inputs, state, "services")
    row = inputs[2].resources["Deployment:loom-service"]
    row["status"]["observedGeneration"] = 0
    assert not ready(inputs, state, "services")
    row["status"]["observedGeneration"] = 1
    row["status"]["replicas"] = 2
    assert not ready(inputs, state, "services")
    row["metadata"]["uid"] = str(uuid4())
    with pytest.raises(module().DevelopmentStageError):
        ready(inputs, state, "services")


def test_database_and_migration_readiness_do_not_repeat_writes(inputs, tmp_path):
    state = tmp_path / "database"
    run(inputs, state, "database")
    db = inputs[2].resources["StatefulSet:loom-postgres"]
    db["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
        "updatedReplicas": 1, "currentRevision": "r1", "updateRevision": "r2"}
    assert not ready(inputs, state, "database")
    db["status"]["updateRevision"] = "r1"
    assert ready(inputs, state, "database")
    migration = tmp_path / "migration"
    run(inputs, migration, "migration")
    job = next(row for row in inputs[2].resources.values() if row["kind"] == "Job")
    assert "ttlSecondsAfterFinished" not in job["spec"]
    assert job["spec"]["backoffLimit"] == 0
    assert not ready(inputs, migration, "migration")
    job["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
    assert ready(inputs, migration, "migration")
    job["status"]["conditions"].append({"type": "Failed", "status": "True"})
    with pytest.raises(module().DevelopmentStageError, match="migration failed"):
        ready(inputs, migration, "migration")
    assert len(inputs[2].creates) == 3


def test_readiness_does_not_create_missing_phase_state(inputs, tmp_path):
    state = tmp_path / "missing"
    with pytest.raises(module().DevelopmentStageError):
        ready(inputs, state, "database")
    assert not state.exists()
    assert not inputs[2].creates
