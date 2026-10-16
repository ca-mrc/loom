"""Environment identity must not collapse developers into an environment class."""

from __future__ import annotations

import json
from uuid import UUID

import pytest
from pydantic import ValidationError

from tests.support.nebius_platform import platform_inputs as platform_inputs

ALICE = UUID("10000000-0000-4000-8000-000000000001")
BOB = UUID("10000000-0000-4000-8000-000000000002")
TEAM = UUID("20000000-0000-4000-8000-000000000001")


def foundation_from(config: dict):
    from loom.nebius_environment_contract import FoundationBinding

    return FoundationBinding(
        platform_config_json=json.dumps(config),
        public_dns_zone="dev.example.com",
        ingress_class_name="loom-shared",
        ingress_namespace="loom-ingress",
        ingress_controller_label="loom-ingress",
    )


def registration_for(foundation, slug: str, identity: UUID = ALICE, **kwargs):
    from loom.nebius_environment_contract import new_environment_registration

    return new_environment_registration(
        foundation, environment_id=identity, incarnation=identity,
        owner_user_id=identity, owner_team_id=TEAM, slug=slug, **kwargs,
    )


def test_personal_instances_share_pool_not_namespace_or_target(platform_inputs: tuple) -> None:
    foundation = foundation_from(platform_inputs[0])
    a = registration_for(foundation, "alice")
    b = registration_for(foundation, "bob", BOB)
    assert a.application_namespace == "loom-dev-alice"
    assert b.application_namespace == "loom-dev-bob"
    assert a.public_host == "alice.dev.example.com"
    assert b.public_host == "bob.dev.example.com"
    assert a.execution_namespace == "loom-run-10000000000040008000000000000001"
    assert a.build_namespace == "loom-run-10000000000040008000000000000001-build"
    assert a.physical_pool_id == b.physical_pool_id
    assert a.cluster_id == b.cluster_id
    assert a.target_id != b.target_id
    assert foundation.min_nodes == 0


def test_suffix_slugs_cannot_collide_with_another_owners_execution_namespace(
    platform_inputs: tuple,
) -> None:
    foundation = foundation_from(platform_inputs[0])
    rows = [registration_for(foundation, slug, UUID(int=i + 1))
            for i, slug in enumerate(("alice", "alice-exec", "alice-build"))]
    namespaces = [name for row in rows for name in row.namespaces]
    assert len(namespaces) == len(set(namespaces)) == 9


@pytest.mark.parametrize("slug", ["shared", "dev", "staging", "prod", "", "Alice", "a.b",
                                 "../alice", "-alice", "alice-", "a" * 55])
def test_personal_names_reject_reserved_or_unsafe_slugs(platform_inputs: tuple, slug: str) -> None:
    with pytest.raises(ValueError):
        registration_for(foundation_from(platform_inputs[0]), slug)


@pytest.mark.parametrize("kind,slug,namespace", [
    ("development", "dev", "loom-dev"),
    ("staging", "staging", "loom-staging"),
    ("production", "prod", "loom-prod"),
])
def test_shared_identity_uses_canonical_namespace(
    platform_inputs: tuple, kind: str, slug: str, namespace: str,
) -> None:
    row = registration_for(foundation_from(platform_inputs[0]), slug, kind=kind, scope="shared")
    assert row.application_namespace == namespace


def test_personal_environment_cannot_claim_production_kind(platform_inputs: tuple) -> None:
    with pytest.raises(ValueError):
        registration_for(foundation_from(platform_inputs[0]), "alice", kind="production")


def test_registration_rejects_generated_namespace_override(platform_inputs: tuple) -> None:
    from loom.nebius_environment_contract import EnvironmentRegistrationV1

    row = registration_for(foundation_from(platform_inputs[0]), "alice")
    for field in ("application_namespace", "execution_namespace", "build_namespace", "target_id"):
        with pytest.raises(ValidationError):
            EnvironmentRegistrationV1.model_validate({**row.model_dump(), field: "another-owner"})


def test_imported_bindings_require_explicit_mode_and_disjoint_names(platform_inputs: tuple) -> None:
    from loom.nebius_environment_contract import EnvironmentRegistrationV1

    row = registration_for(foundation_from(platform_inputs[0]), "alice")
    imported = EnvironmentRegistrationV1.model_validate({
        **row.model_dump(), "binding_mode": "imported",
        "application_namespace": "loom-nebius-integration",
        "execution_namespace": "loom-nebius-integration-execution",
        "build_namespace": "loom-nebius-integration-execution-build",
        "target_id": "nebius-integration",
    })
    assert imported.application_namespace == "loom-nebius-integration"
    with pytest.raises(ValidationError):
        EnvironmentRegistrationV1.model_validate({
            **imported.model_dump(), "execution_namespace": imported.application_namespace,
        })


def test_registration_identity_survives_candidate_update(platform_inputs: tuple) -> None:
    from loom.nebius_environment_contract import EnvironmentRegistrationV1

    row = registration_for(foundation_from(platform_inputs[0]), "alice")
    updated = EnvironmentRegistrationV1.model_validate({
        **row.model_dump(), "candidate_id": BOB, "deployment_generation": 2,
    })
    assert updated.namespaces == row.namespaces
    assert updated.environment_id == row.environment_id
    assert updated.incarnation == row.incarnation
    assert updated.public_host == row.public_host
    with pytest.raises(ValidationError):
        row.slug = "bob"


@pytest.mark.parametrize("key,value", [
    ("incarnation", UUID(int=0)), ("environment_id", UUID(int=0)),
    ("owner_team_id", UUID(int=0)), ("owner_user_id", None),
    ("deployment_generation", True), ("deployment_generation", 0),
    ("public_host", "alice.dev.example.com/escape"),
])
def test_registration_rejects_invalid_identity_fields(
    platform_inputs: tuple, key: str, value: object,
) -> None:
    from loom.nebius_environment_contract import EnvironmentRegistrationV1

    row = registration_for(foundation_from(platform_inputs[0]), "alice")
    with pytest.raises(ValidationError):
        EnvironmentRegistrationV1.model_validate({**row.model_dump(), key: value})


@pytest.mark.parametrize("warm_floor", [-1, True, 101])
def test_foundation_rejects_warm_floor_outside_pool_envelope(
    platform_inputs: tuple, warm_floor: object,
) -> None:
    from loom.nebius_environment_contract import FoundationBinding

    foundation = foundation_from(platform_inputs[0])
    with pytest.raises(ValidationError):
        FoundationBinding.model_validate({**foundation.model_dump(), "min_nodes": warm_floor})


@pytest.mark.parametrize("storage", [-1, 0, 9, True, 10.5, "10", 1025])
def test_generated_storage_requires_bounded_strict_gib(platform_inputs, storage):
    from loom.nebius_environment_contract import FoundationBinding

    with pytest.raises(ValidationError):
        FoundationBinding.model_validate({
            **foundation_from(platform_inputs[0]).model_dump(), "generated_postgres_storage_gi": storage,
        })
