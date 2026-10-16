"""Connected installation cannot cross readiness or retained-state boundaries."""
from __future__ import annotations

import copy
import json
import shutil
from contextlib import contextmanager
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_bootstrap import BootstrapAPI
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_phase_api import PhaseAPI as PhaseAPI
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


class InstallationAPI:
    """External stores/probes only; renderer, bootstrap, staging and journals real."""

    def __init__(self, bootstrap):
        self.bootstrap = BootstrapAPI(bootstrap)
        self.store = None
        self.events = []
        self.block = None
        self.ready = set()

    def preflight(self, request, rendered):
        self.events.append("preflight")
        if self.block == "preflight":
            raise RuntimeError("private-preflight-diagnostic")

    @contextmanager
    def bootstrap_api(self):
        yield self.bootstrap

    @contextmanager
    def resources(self, binding, phase):
        if self.store is None:
            self.store = PhaseAPI(binding)
        yield self.store

    def qualify_authority(self, binding, state_dir):
        self.events.append("authority")
        if self.block == "authority":
            raise RuntimeError("private-authority-diagnostic")

    def verify_backup(self, binding, rendered, job_uid):
        self.events.append("backup")
        if self.block == "backup":
            raise RuntimeError("private-backup-diagnostic")
        return {"job_uid": job_uid, "sha256": "a" * 64, "bytes": 123, "key": "management/backup.dump"}

    def verify_public(self, binding, rendered, material_dir):
        self.events.append("public")
        if self.block == "public":
            raise RuntimeError("private-public-diagnostic")

    def complete(self, kind):
        for doc in list(self.store.resources.values()):
            if doc["kind"] != kind:
                continue
            if kind == "Job":
                doc["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
            else:
                doc["metadata"]["generation"] = 1
                doc["status"] = {"observedGeneration": 1, "replicas": 1, "readyReplicas": 1,
                                 "updatedReplicas": 1, "availableReplicas": 1,
                                 "currentRevision": "current", "updateRevision": "current"}
            if kind == "StatefulSet":
                claim = copy.deepcopy(doc["spec"]["volumeClaimTemplates"][0])
                claim.update(apiVersion="v1", kind="PersistentVolumeClaim", status={"phase": "Bound"})
                claim["metadata"].update(name="data-loom-postgres-0", namespace=self.store.binding.namespace, uid=str(uuid4()))
                claim["spec"]["volumeName"] = "pvc-" + claim["metadata"]["uid"]
                self.store.resources["PersistentVolumeClaim:data-loom-postgres-0"] = claim
                self.store.resources["PersistentVolume:" + claim["spec"]["volumeName"]] = {
                    "apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": claim["spec"]["volumeName"], "uid": str(uuid4())},
                    "spec": {"claimRef": {"namespace": self.store.binding.namespace, "name": "data-loom-postgres-0", "uid": claim["metadata"]["uid"]},
                             "storageClassName": claim["spec"]["storageClassName"], "capacity": {"storage": "10Gi"},
                             "csi": {"driver": "test.csi.example.com", "volumeHandle": "database-disk"}},
                    "status": {"phase": "Bound"},
                }


@pytest.fixture
def installation(management_inputs, material):
    from scripts.ops.nebius_management_bootstrap import BootstrapBinding
    from scripts.ops.nebius_management_install import ManagementInstallRequest

    from loom_service.environment_management.deployment import ManagementDeployment

    deployment, candidate, profile = copy.deepcopy(management_inputs)
    config = deployment["installation"]
    config["foundation"]["namespace_authority"] = {"installation_id": deployment["installation_id"],
                                                  "namespace": deployment["namespace"]}
    config["provider_runtime"]["kubernetes"].pop("credentials_file")
    config["provider_runtime"]["kubernetes"].update(kind="projected_service_account",
                                                  token_file="/var/run/loom-management-kubernetes/token")
    binding = BootstrapBinding(deployment["installation_id"], deployment["namespace"], str(uuid4()))
    request = ManagementInstallRequest(binding=binding, deployment=ManagementDeployment.model_validate(deployment),
                                       candidate=candidate, profile=profile, material=material)
    return request, InstallationAPI(binding)


def run(installation, tmp_path):
    from scripts.ops.nebius_management_install import install_management

    request, api = installation
    return install_management(request=request, api=api, state_dir=tmp_path / "installation",
                              anchor_dir=tmp_path / "independent")


def test_installer_waits_at_database_migration_backup_and_service_before_publication(installation, tmp_path):
    request, api = installation
    first = run(installation, tmp_path)
    assert first["status"] == "pending" and first["phase"] == "database"
    assert not any(doc["kind"] in {"Job", "Deployment", "Ingress"} for doc in api.store.resources.values())
    before = len(api.store.creates)
    assert run(installation, tmp_path) == first
    assert len(api.store.creates) == before
    api.complete("StatefulSet")
    assert run(installation, tmp_path)["phase"] == "migration"
    assert not any(doc["kind"] == "Deployment" for doc in api.store.resources.values())
    api.complete("Job")
    assert run(installation, tmp_path)["phase"] == "backup"
    assert "backup" not in api.events
    api.complete("Job")
    assert run(installation, tmp_path)["phase"] == "service"
    assert "backup" in api.events
    assert not any(doc["kind"] == "Ingress" for doc in api.store.resources.values())
    api.complete("Deployment")
    final = run(installation, tmp_path)
    assert final["status"] == "management_installed"
    assert final["installation_id"] == str(request.deployment.installation_id)
    assert api.events[-1] == "public"
    before = len(api.store.creates)
    assert run(installation, tmp_path) == final
    assert len(api.store.creates) == before
    assert "scoped-publication-test-token" not in json.dumps(final)


@pytest.mark.parametrize("missing", ["whole_state", "bootstrap", "config", "anchor"])
def test_lost_recorded_installation_state_never_reopens_writes(installation, tmp_path, missing):
    from scripts.ops.nebius_management_install import ManagementInstallError

    run(installation, tmp_path)
    api = installation[1]
    before = len(api.store.creates)
    paths = {"whole_state": "installation", "bootstrap": "installation/bootstrap",
             "config": "installation/config", "anchor": "independent"}
    shutil.rmtree(tmp_path / paths[missing])
    with pytest.raises(ManagementInstallError, match="recovery") as error:
        run(installation, tmp_path)
    assert error.value.stage == "recovery"
    assert len(api.store.creates) == before
    assert len(api.bootstrap.creates) == 1


@pytest.mark.parametrize("blocked", ["preflight", "authority"])
def test_failed_qualification_stops_before_runtime_or_supplied_credentials(installation, tmp_path, blocked):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api = installation[1]
    api.block = blocked
    with pytest.raises(ManagementInstallError) as error:
        run(installation, tmp_path)
    assert "private-" not in str(error.value)
    assert error.value.stage == (None if blocked == "preflight" else "runtime_authority")
    if blocked == "preflight":
        assert api.bootstrap.creates == [] and api.store is None
    else:
        assert not any(doc["kind"] in {"Secret", "StatefulSet", "Deployment"} for doc in api.store.resources.values())


def test_failed_migration_is_retained_without_replacement_or_publication(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    run(installation, tmp_path)
    api = installation[1]
    api.complete("StatefulSet")
    run(installation, tmp_path)
    job = next(doc for doc in api.store.resources.values() if doc["kind"] == "Job")
    job["status"] = {"conditions": [{"type": "Failed", "status": "True"}]}
    before = len(api.store.creates)
    for _ in range(2):
        with pytest.raises(ManagementInstallError):
            run(installation, tmp_path)
    assert len(api.store.creates) == before
    assert not any(doc["kind"] == "Deployment" for doc in api.store.resources.values())


def test_installation_input_change_is_rejected_before_any_new_resources(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    run(installation, tmp_path)
    before = len(installation[1].store.creates)
    installation[0].material["loom-management-publications"]["token"] = "changed"
    with pytest.raises(ManagementInstallError):
        run(installation, tmp_path)
    assert len(installation[1].store.creates) == before


def test_installer_accounts_for_retained_one_shot_backup_scratch(installation):
    from scripts.ops.nebius_management_install import render_installation

    rendered = render_installation(installation[0])
    job = rendered.files["85-backup-verify.yaml"][0]
    assert job["spec"]["backoffLimit"] == 0
    assert "ttlSecondsAfterFinished" not in job["spec"]
    assert rendered.platform_envelope.ephemeral_storage_mib == 21504


@pytest.mark.parametrize("blocked", ["backup", "public"])
def test_external_backup_or_public_auth_failure_cannot_report_installation_complete(installation, tmp_path, blocked):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api = installation[1]
    run(installation, tmp_path)
    api.complete("StatefulSet")
    run(installation, tmp_path)
    api.complete("Job")
    run(installation, tmp_path)
    api.complete("Job")
    api.block = blocked
    if blocked == "public":
        run(installation, tmp_path)
        api.complete("Deployment")
    with pytest.raises(ManagementInstallError) as error:
        run(installation, tmp_path)
    assert "private-" not in str(error.value)
    assert error.value.stage == ("backup_execution" if blocked == "backup" else "public_authentication")
    if blocked == "backup":
        assert not any(doc["kind"] in {"Deployment", "Ingress"} for doc in api.store.resources.values())


def test_database_volume_replacement_blocks_replay_before_migration_or_new_workloads(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    run(installation, tmp_path)
    api = installation[1]
    api.complete("StatefulSet")
    assert run(installation, tmp_path)["phase"] == "migration"
    api.complete("Job")
    before = len(api.store.creates)
    api.store.resources["PersistentVolumeClaim:data-loom-postgres-0"]["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ManagementInstallError):
        run(installation, tmp_path)
    assert len(api.store.creates) == before
