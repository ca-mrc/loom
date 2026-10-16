"""Database installation must not adopt or silently replace retained data."""
from __future__ import annotations

import copy
from uuid import uuid4

import pytest
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


class StorageAPI:
    def __init__(self, binding):
        self.binding, self.claim, self.volume = binding, None, None

    def verify_identity(self, binding):
        assert binding == self.binding

    def get_database_claim(self):
        return copy.deepcopy(self.claim)

    def get_database_volume(self):
        return copy.deepcopy(self.volume)


@pytest.fixture
def storage(installation):
    from scripts.ops.nebius_management_install import render_installation
    from scripts.ops.nebius_management_material import ManagementBinding

    request, _ = installation
    binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                                str(uuid4()), request.binding.kube_system_uid)
    rendered = render_installation(request)
    return rendered, binding, StorageAPI(binding)


def bind(storage):
    rendered, binding, api = storage
    stateful = next(doc for doc in rendered.files["20-database.yaml"] if doc["kind"] == "StatefulSet")
    claim = copy.deepcopy(stateful["spec"]["volumeClaimTemplates"][0])
    claim.update(apiVersion="v1", kind="PersistentVolumeClaim", status={"phase": "Bound"})
    claim["metadata"].update(name="data-loom-postgres-0", namespace=binding.namespace, uid=str(uuid4()))
    claim["metadata"].setdefault("labels", {})["loom.nebius/management-installation"] = binding.installation_id
    claim["spec"]["volumeName"] = "pvc-" + claim["metadata"]["uid"]
    api.claim = claim
    api.volume = {"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {
        "name": claim["spec"]["volumeName"], "uid": str(uuid4()),
    }, "spec": {"claimRef": {"namespace": binding.namespace, "name": "data-loom-postgres-0", "uid": claim["metadata"]["uid"]},
                "storageClassName": claim["spec"]["storageClassName"], "capacity": {"storage": "10Gi"},
                "csi": {"driver": "test.csi.example.com", "volumeHandle": "database-disk"}},
        "status": {"phase": "Bound"}}


def test_database_controller_labels_its_created_claim_for_this_installation(storage):
    rendered, binding, _ = storage
    stateful = next(doc for doc in rendered.files["20-database.yaml"] if doc["kind"] == "StatefulSet")
    claim = stateful["spec"]["volumeClaimTemplates"][0]
    assert claim["metadata"].get("labels", {}).get("loom.nebius/management-installation") == binding.installation_id


def test_preexisting_claim_is_rejected_before_controller_creation(storage, tmp_path):
    from scripts.ops.nebius_management_storage import (
        ManagementStorageError,
        prepare_management_storage,
    )

    rendered, binding, api = storage
    bind(storage)
    with pytest.raises(ManagementStorageError, match="untracked"):
        prepare_management_storage(rendered=rendered, binding=binding, api=api, state_dir=tmp_path / "database")
    assert not (tmp_path / "database" / "storage-intent.json").exists()


@pytest.mark.parametrize("change", [None, "missing_claim", "claim_uid", "volume_uid", "disk", "claim_binding"])
def test_storage_readback_pins_claim_and_physical_volume_before_migration(storage, tmp_path, change):
    from scripts.ops.nebius_management_storage import (
        ManagementStorageError,
        prepare_management_storage,
        verify_management_storage,
    )

    rendered, binding, api = storage
    args = dict(rendered=rendered, binding=binding, api=api, state_dir=tmp_path / "database")
    prepare_management_storage(**args)
    bind(storage)
    receipt = verify_management_storage(**args, evidence_dir=tmp_path / "storage")
    assert receipt["pvc_uid"] == api.claim["metadata"]["uid"]
    assert receipt["pv_uid"] == api.volume["metadata"]["uid"]
    if change is None:
        assert verify_management_storage(**args, evidence_dir=tmp_path / "storage") == receipt
        return
    if change == "missing_claim":
        api.claim = None
    elif change == "claim_uid":
        api.claim["metadata"]["uid"] = str(uuid4())
    elif change == "volume_uid":
        api.volume["metadata"]["uid"] = str(uuid4())
    elif change == "disk":
        api.volume["spec"]["csi"]["volumeHandle"] = "empty-replacement-disk"
    else:
        api.volume["spec"]["claimRef"]["namespace"] = "foreign"
    with pytest.raises(ManagementStorageError):
        verify_management_storage(**args, evidence_dir=tmp_path / "storage")


def test_storage_binding_cannot_be_recorded_without_prior_absence_intent(storage, tmp_path):
    from scripts.ops.nebius_management_storage import (
        ManagementStorageError,
        verify_management_storage,
    )

    bind(storage)
    rendered, binding, api = storage
    with pytest.raises(ManagementStorageError):
        verify_management_storage(rendered=rendered, binding=binding, api=api,
                                  state_dir=tmp_path / "database", evidence_dir=tmp_path / "storage")
    assert not (tmp_path / "storage").exists()
