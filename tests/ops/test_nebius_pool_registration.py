"""Protected staging retains the fixed registration Job and its uncertain writes."""
import copy
import json
from uuid import uuid4

import pytest

from scripts.ops.nebius_management_material import ManagementBinding
from tests.integration.test_nebius_pool_installation import installation
from tests.ops.test_nebius_management_stage import PhaseAPI


def request():
    from scripts.ops.nebius_pool_registration import PoolRegistrationRequest

    from loom_service.pool_management.installation import PoolInstallation

    config, _ = installation()
    spec = PoolInstallation.model_validate(config)
    binding = ManagementBinding(str(spec.installation_id), "loom-nebius-management", str(uuid4()), str(uuid4()))
    candidate = {"source_ref": "refs/heads/dev", "candidate_sha": "1" * 40,
        "images": {"service": {"image_ref": "registry.example/service@sha256:" + "a" * 64}}}
    return PoolRegistrationRequest(spec=spec, binding=binding, candidate=candidate)


def test_fixed_registration_staging_replays_without_new_writes(tmp_path):
    from scripts.ops.nebius_pool_registration import registration_documents, stage_pool_registration

    current = request()
    api = PhaseAPI(current.binding)
    first = stage_pool_registration(request=current, api=api, state_dir=tmp_path)
    assert stage_pool_registration(request=current, api=api, state_dir=tmp_path) == first
    assert len(api.creates) == 2
    docs = registration_documents(current)
    assert {row["kind"] for row in docs.values()} == {"ConfigMap", "Job"}
    record = json.loads((tmp_path / "stage.json").read_text())
    assert all(row["status"] == "created" and row["uid"] for row in record["resources"].values())


@pytest.mark.parametrize("failure", ["before", "after"])
def test_registration_staging_does_not_repeat_uncertain_create(tmp_path, failure):
    from scripts.ops.nebius_pool_registration import stage_pool_registration

    current = request()
    api = PhaseAPI(current.binding)
    api.failure = failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ValueError):
                stage_pool_registration(request=current, api=api, state_dir=tmp_path)
        assert len(api.creates) == 1
    else:
        stage_pool_registration(request=current, api=api, state_dir=tmp_path)
        stage_pool_registration(request=current, api=api, state_dir=tmp_path)
        assert len(api.creates) == 2


def test_stage_refuses_different_installation_or_changed_candidate(tmp_path):
    from dataclasses import replace

    from scripts.ops.nebius_pool_registration import stage_pool_registration

    current = request()
    api = PhaseAPI(current.binding)
    with pytest.raises(ValueError):
        stage_pool_registration(request=replace(current, binding=replace(current.binding, installation_id=str(uuid4()))),
            api=api, state_dir=tmp_path)
    assert not api.creates
    stage_pool_registration(request=current, api=api, state_dir=tmp_path)
    candidate = copy.deepcopy(current.candidate)
    candidate["images"]["service"]["image_ref"] = "registry.example/service@sha256:" + "b" * 64
    with pytest.raises(ValueError):
        stage_pool_registration(request=replace(current, candidate=candidate), api=api, state_dir=tmp_path)
    assert len(api.creates) == 2
