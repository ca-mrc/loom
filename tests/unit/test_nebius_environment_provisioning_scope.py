"""Provisioning project authority never replaces cluster or quota identity."""
from __future__ import annotations

from dataclasses import replace

import pytest
from pydantic import ValidationError

from loom.nebius_environment_contract import FoundationBinding
from loom.nebius_environment_render import render_environment
from loom_service.environment_management.cloud_provider import NebiusEnvironmentCloudProvider
from loom_service.environment_management.installation import ManagementInstallation
from loom_service.environment_management.steps import ProvisioningStep
from tests.support.nebius_platform import ROOT
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_cloud_provider import CloudApi, cloud_context
from tests.unit.test_nebius_environment_contract import foundation_from, registration_for
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


def test_scoped_render_keeps_cluster_and_quota_configuration(platform_inputs):
    config, candidate, profile = platform_inputs
    foundation = FoundationBinding.model_validate({
        **foundation_from(config).model_dump(), "provisioning_project_id": "project-provisioning-a",
    })
    prepared = render_environment(registration_for(foundation, "alice"), candidate, foundation,
                                  profile=profile, keyring={}, repo_root=ROOT)
    assert prepared.provisioning_project_id == "project-provisioning-a"
    assert "provisioning_project_id" not in prepared.config
    for key in ("project_id", "quota_parent_id", "cluster_id", "execution_node_group_id"):
        assert prepared.config[key] == config[key]
    assert prepared.execution_enabled is False


@pytest.mark.parametrize("scope", ["cluster", "tenant", "", "../project", "a" * 129])
def test_foundation_rejects_shared_authority_or_invalid_scope(platform_inputs, scope):
    config = platform_inputs[0]
    value = config["project_id"] if scope == "cluster" else config["quota_parent_id"] if scope == "tenant" else scope
    with pytest.raises(ValidationError):
        FoundationBinding.model_validate({**foundation_from(config).model_dump(), "provisioning_project_id": value})


def test_provider_activation_requires_explicit_provisioning_scope(management_inputs):
    data = management_inputs[0]["installation"]
    data["foundation"].pop("provisioning_project_id", None)
    with pytest.raises(ValidationError, match="provisioning"):
        ManagementInstallation.model_validate(data)
    data["foundation"]["provisioning_project_id"] = "project-provisioning-a"
    assert ManagementInstallation.model_validate(data).foundation.provisioning_project_id == "project-provisioning-a"
    # Registry-only/offline installs remain compatible without cloud authority.
    data["provider_runtime"] = None
    del data["foundation"]["provisioning_project_id"]
    assert ManagementInstallation.model_validate(data).provider_runtime is None


@pytest.mark.parametrize("scoped", [False, True])
@pytest.mark.parametrize("kind", ["service_account", "group", "membership", "access_key", "bucket"])
def test_cloud_intents_use_frozen_project_but_memberships_use_their_group(scoped, kind):
    ctx = cloud_context()
    if scoped:
        ctx = replace(ctx, provisioning_project_id="project-provisioning-a")
    ctx.identities.update({"iam:source:group": "group-source", "iam:source:service_account": "sa-source"})
    if kind == "bucket":
        step = ProvisioningStep("bucket:source", "object_bucket", {
            "purpose": "source", "name": "loom-" + ctx.registration["incarnation"].replace("-", "") + "-source",
        })
    else:
        step = ProvisioningStep("iam:source:" + kind, "credentials", {"purpose": "source", "action": kind})
    actual_kind, intent = NebiusEnvironmentCloudProvider(CloudApi()).intent(ctx, step)
    assert actual_kind == kind
    expected = "group-source" if kind == "membership" else (
        "project-provisioning-a" if scoped else "tenant-owned" if kind == "group" else "project-owned"
    )
    assert intent["metadata"]["parent_id"] == expected
    if kind == "membership":
        assert intent["spec"] == {"member_id": "sa-source"}
