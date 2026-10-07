"""The dev installer resumes fixed phases and never adopts existing data."""
from __future__ import annotations

import copy
import importlib
import json
from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_bootstrap import BootstrapAPI
from tests.ops.test_nebius_development_stage import inputs as inputs
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_development_install")


class InstallationAPI:
    def __init__(self, request):
        self.bootstrap = BootstrapAPI(request.bootstrap)
        self.stage = None
        self.qualified = []
        self.dependencies = 0
        self.fail_qualification = False
        self.fail_volume_qualification = False
        self.qualified_volumes = []

    def qualify(self, request, *, fresh):
        self.qualified.append(fresh)
        if self.fail_qualification:
            raise RuntimeError("private-provisioner-error")
        if fresh:
            assert self.bootstrap.namespace is None

    @contextmanager
    def bootstrap_api(self):
        yield self.bootstrap

    @contextmanager
    def resources(self, binding, selection, phase):
        if self.stage is None:
            self.stage = PhaseAPI(binding)
        assert self.stage.binding == binding
        yield self.stage

    def verify_private_dependencies(self, request, binding, material_state):
        assert (material_state / "bootstrap.json").is_file()
        assert binding == self.stage.binding
        self.dependencies += 1

    def qualify_volume(self, request, binding, observation):
        assert observation["pv_spec"]["csi"]["driver"] == "compute.csi.nebius.com"
        assert observation["pv_spec"]["csi"]["volumeHandle"] == "dev-disk-123"
        self.qualified_volumes.append(copy.deepcopy(observation))
        if self.fail_volume_qualification:
            raise RuntimeError("private-provider-disk-error")


def setup(inputs):
    selection, binding, _ = inputs
    request = module().DevelopmentInstallRequest(binding.bootstrap, selection)
    return request, InstallationAPI(request)


def install(request, api, tmp_path):
    return module().install_private_development(request=request, api=api,
        state_dir=tmp_path / "state", anchor_dir=tmp_path / "anchor")


def storage_ready(api, selection):
    stateful = api.stage.resources["StatefulSet:loom-postgres"]
    claim = copy.deepcopy(stateful["spec"]["volumeClaimTemplates"][0])
    claim.update(apiVersion="v1", kind="PersistentVolumeClaim", status={"phase": "Bound"})
    uid = str(uuid4())
    claim["metadata"].update(name="data-loom-postgres-0", namespace="loom-dev", uid=uid)
    claim["spec"]["volumeName"] = "pvc-" + uid
    volume = {"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {
        "name": "pvc-" + uid, "uid": str(uuid4()),
        "annotations": {"pv.kubernetes.io/provisioned-by": "compute.csi.nebius.com"}},
        "spec": {"claimRef": {"namespace": "loom-dev", "name": "data-loom-postgres-0", "uid": uid},
            "storageClassName": selection.config["storage_class"], "accessModes": ["ReadWriteOnce"],
            "capacity": {"storage": str(selection.config["postgres_storage_gi"]) + "Gi"},
            "csi": {"driver": "compute.csi.nebius.com", "volumeHandle": "dev-disk-123"}},
        "status": {"phase": "Bound"}}
    api.stage.resources["PersistentVolumeClaim:data-loom-postgres-0"] = claim
    api.stage.resources["PersistentVolume:" + volume["metadata"]["name"]] = volume
    return claim, volume


def workloads_ready(api, kind):
    for row in api.stage.resources.values():
        if row["kind"] != kind:
            continue
        if kind == "Job":
            row["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
        else:
            row["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
                "updatedReplicas": 1, "currentRevision": "r1", "updateRevision": "r1",
                "availableReplicas": 1, "unavailableReplicas": 0}


def test_installer_resumes_storage_database_migration_and_service_barriers(inputs, tmp_path):
    request, api = setup(inputs)
    first = install(request, api, tmp_path)
    assert first["status"] == "pending" and first["phase"] == "storage"
    assert not any(key.startswith(("Job:", "Deployment:")) for key in api.stage.resources)
    assert install(request, api, tmp_path) == first
    storage_ready(api, request.selection)
    assert install(request, api, tmp_path)["phase"] == "database"
    workloads_ready(api, "StatefulSet")
    assert install(request, api, tmp_path)["phase"] == "migration"
    assert not any(key.startswith("Deployment:") for key in api.stage.resources)
    workloads_ready(api, "Job")
    assert install(request, api, tmp_path)["phase"] == "services"
    workloads_ready(api, "Deployment")
    final = install(request, api, tmp_path)
    assert final["status"] == "development_private_installed"
    assert final["namespace"] == "loom-dev"
    creates = copy.deepcopy((api.bootstrap.creates, api.stage.creates))
    assert install(request, api, tmp_path) == final
    assert (api.bootstrap.creates, api.stage.creates) == creates
    assert api.qualified.count(True) == 1
    assert api.dependencies == 2
    assert api.qualified_volumes


def test_replay_preserves_old_anchor_format_without_reopening_writes(inputs, tmp_path):
    request, api = setup(inputs)
    request = replace(request, qualification_digest='sha256:' + 'a' * 64)
    first = install(request, api, tmp_path)
    paths = (tmp_path / 'state/installation.json', tmp_path / 'anchor' / (request.bootstrap.installation_id + '.json'))
    for path in paths:
        value = json.loads(path.read_text())
        value.pop('qualification_digest')
        path.write_text(json.dumps(value))
    before = {path: path.read_bytes() for path in paths}
    creates = copy.deepcopy((api.bootstrap.creates, api.stage.creates))
    assert install(request, api, tmp_path) == first
    assert (api.bootstrap.creates, api.stage.creates) == creates
    assert {path: path.read_bytes() for path in paths} == before


@pytest.mark.parametrize("change", ["pvc_uid", "pv_uid", "disk", "driver", "claim", "size", "class", "clone", "missing"])
def test_bound_storage_drift_never_starts_migration(inputs, tmp_path, change):
    request, api = setup(inputs)
    install(request, api, tmp_path)
    claim, volume = storage_ready(api, request.selection)
    assert install(request, api, tmp_path)["phase"] == "database"
    workloads_ready(api, "StatefulSet")
    if change == "pvc_uid":
        claim["metadata"]["uid"] = str(uuid4())
    elif change == "pv_uid":
        volume["metadata"]["uid"] = str(uuid4())
    elif change == "disk":
        volume["spec"]["csi"]["volumeHandle"] = "other-disk"
    elif change == "driver":
        volume["spec"]["csi"]["driver"] = "foreign.csi"
    elif change == "claim":
        volume["spec"]["claimRef"]["namespace"] = "loom-nebius-platform"
    elif change == "size":
        claim["spec"]["resources"]["requests"]["storage"] = "1Gi"
    elif change == "class":
        volume["spec"]["storageClassName"] = "other-class"
    elif change == "clone":
        claim["spec"]["dataSource"] = {"kind": "PersistentVolumeClaim", "name": "staging"}
    else:
        del api.stage.resources["PersistentVolumeClaim:data-loom-postgres-0"]
    with pytest.raises(module().DevelopmentInstallError):
        install(request, api, tmp_path)
    assert not any(key.startswith("Job:") for key in api.stage.resources)


@pytest.mark.parametrize("loss", ["anchor", "installation", "local_keys", "phase", "storage"])
def test_missing_recovery_evidence_never_reopens_fresh_installation(inputs, tmp_path, loss):
    request, api = setup(inputs)
    install(request, api, tmp_path)
    storage_ready(api, request.selection)
    install(request, api, tmp_path)
    path = {"anchor": tmp_path / "anchor" / (request.bootstrap.installation_id + ".json"),
        "installation": tmp_path / "state/installation.json", "local_keys": tmp_path / "state/bootstrap/bootstrap.json",
        "phase": tmp_path / "state/config/stage.json", "storage": tmp_path / "state/storage/stage.json"}[loss]
    path.unlink()
    before = copy.deepcopy((api.bootstrap.creates, api.stage.creates))
    with pytest.raises(module().DevelopmentInstallError):
        install(request, api, tmp_path)
    assert (api.bootstrap.creates, api.stage.creates) == before


def test_failed_current_qualification_preserves_remote_resources_and_private_evidence(inputs, tmp_path):
    request, api = setup(inputs)
    install(request, api, tmp_path)
    before = copy.deepcopy((api.bootstrap.creates, api.stage.creates))
    journal = (tmp_path / "state/installation.json").read_bytes()
    api.fail_qualification = True
    with pytest.raises(module().DevelopmentInstallError) as error:
        install(request, api, tmp_path)
    assert "private-provisioner-error" not in str(error.value)
    assert (api.bootstrap.creates, api.stage.creates) == before
    assert (tmp_path / "state/installation.json").read_bytes() == journal


def test_changed_source_or_material_cannot_resume_existing_installation(inputs, tmp_path):
    request, api = setup(inputs)
    install(request, api, tmp_path)
    request.selection.storage["secret-key"] = "changed-secret"
    before = copy.deepcopy((api.bootstrap.creates, api.stage.creates))
    with pytest.raises(module().DevelopmentInstallError):
        install(request, api, tmp_path)
    assert (api.bootstrap.creates, api.stage.creates) == before


def test_unknown_namespace_create_is_not_repeated_by_top_level_resume(inputs, tmp_path):
    request, api = setup(inputs)
    api.bootstrap.failure = "namespace", "before"
    for _ in range(2):
        with pytest.raises(module().DevelopmentInstallError):
            install(request, api, tmp_path)
        api.bootstrap.failure = None
    assert api.bootstrap.creates == ["namespace"]
    assert api.stage is None


def test_competing_installer_anchor_cannot_overwrite_successful_journal(inputs, tmp_path, monkeypatch):
    request, api = setup(inputs)
    atomic = module().private_state._atomic_json
    winner = None

    def compete(path, value):
        nonlocal winner
        if path.parent == tmp_path / "anchor" and path.name == request.bootstrap.installation_id + ".json":
            module().install_private_development(request=request, api=api,
                state_dir=tmp_path / "state", anchor_dir=tmp_path / "competitor")
            winner = (tmp_path / "state/installation.json").read_bytes()
        atomic(path, value)

    monkeypatch.setattr(module().private_state, "_atomic_json", compete)
    with pytest.raises(module().DevelopmentInstallError):
        install(request, api, tmp_path)
    assert (tmp_path / "state/installation.json").read_bytes() == winner
    assert module().install_private_development(request=request, api=api,
        state_dir=tmp_path / "state", anchor_dir=tmp_path / "competitor")["phase"] == "storage"


def test_final_dependency_probe_cannot_hide_changed_earlier_phase(inputs, tmp_path):
    request, api = setup(inputs)
    install(request, api, tmp_path)
    storage_ready(api, request.selection)
    workloads_ready(api, "StatefulSet")
    install(request, api, tmp_path)
    workloads_ready(api, "Job")
    install(request, api, tmp_path)
    workloads_ready(api, "Deployment")

    def concurrent_change(request, binding, material_state):
        api.stage.resources["ConfigMap:loom-platform-config"]["data"]["environment.json"] = "{}"

    api.verify_private_dependencies = concurrent_change
    before = copy.deepcopy((api.bootstrap.creates, api.stage.creates))
    with pytest.raises(module().DevelopmentInstallError):
        install(request, api, tmp_path)
    assert (api.bootstrap.creates, api.stage.creates) == before


def test_csi_binding_does_not_substitute_for_actual_provider_disk_qualification(inputs, tmp_path):
    request, api = setup(inputs)
    install(request, api, tmp_path)
    storage_ready(api, request.selection)
    workloads_ready(api, "StatefulSet")
    api.fail_volume_qualification = True
    with pytest.raises(module().DevelopmentInstallError) as error:
        install(request, api, tmp_path)
    assert "private-provider-disk-error" not in str(error.value)
    assert not any(key.startswith("Job:") for key in api.stage.resources)
    api.fail_volume_qualification = False
    assert install(request, api, tmp_path)["phase"] == "migration"
