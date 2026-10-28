"""Pure credential-first repository discovery; never retirement or deletion authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import rfc8785

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageRegistryCredentialGeneration,
)
from loom.task_image_build_plan import parse_task_image_build_plan
from loom.task_image_materialization import task_image_materialization_keys
from loom_task_image_authority.config import _validate_https_origin
from loom_task_image_authority.registry_public import validate_stored_registry_credential_public
from loom_task_image_authority.registry_token import publication_repository


@dataclass(frozen=True)
class AttemptRepository:
    component: str
    repository: str
    last_credential_expires_at: datetime
    credential_count: int
    credential_set_sha256: str


@dataclass(frozen=True)
class AttemptRepositoryInventory:
    materialization_id: UUID
    attempt_id: UUID
    registry_origin: str
    repositories: tuple[AttemptRepository, ...]
    canonical_bytes: bytes


def derive_attempt_repository_inventory(
    *,
    materialization: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    credentials: Sequence[TaskImageRegistryCredentialGeneration],
    registry_origin: str,
) -> AttemptRepositoryInventory:
    """Validate a supplied row snapshot without reading secrets.

    Retirement must establish ALL retained credentials under its shared attempt
    fence, either by a direct complete read or by rechecking a prepared immutable
    snapshot. This pure function cannot prove database inventory completeness,
    absence of live references, retirement or writer quiescence.
    """
    try:
        return _derive(materialization, attempt, credentials, registry_origin)
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("attempt repository inventory unavailable") from exc


def _derive(
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    credentials: Sequence[TaskImageRegistryCredentialGeneration],
    registry_origin: str,
) -> AttemptRepositoryInventory:
    _validate_https_origin(registry_origin, label="registry origin")
    if attempt.claim_plan_json is None or len(credentials) > 128 * 512:
        raise ValueError("inventory is outside bounds")
    plan = parse_task_image_build_plan(json.dumps(attempt.claim_plan_json, ensure_ascii=False, separators=(",", ":")))
    payload = plan.model_dump(mode="json", exclude_none=False)
    if (
        payload != attempt.claim_plan_json
        or hashlib.sha256(rfc8785.dumps(payload)).hexdigest() != attempt.claim_plan_sha256
        or type(attempt.id) is not UUID
        or attempt.id.int == 0
        or attempt.claim_id is None
        or attempt.materialization_id != row.id
        or plan.materialization_id != row.id
        or plan.grant_id != attempt.grant_id
        or plan.session_id != attempt.session_id
        or plan.session_generation != attempt.session_generation
        or plan.builder_id != attempt.builder_id
        or plan.task_id != row.task_id
        or plan.task_checksum != row.task_checksum
        or plan.cpu_arch != row.cpu_arch
        or plan.content_manifest_digest != row.bundle_content_manifest_sha256
        or row.materialization_key
        not in task_image_materialization_keys(
            task_config=row.task_config,
            task_id=row.task_id, task_checksum=row.task_checksum, cpu_arch=row.cpu_arch,
            bundle_content_manifest_sha256=plan.content_manifest_digest,
        )
    ):
        raise ValueError("frozen attempt identity changed")
    by_component: dict[str, list[TaskImageRegistryCredentialGeneration]] = {
        item.name: [] for item in plan.components
    }
    identities: set[UUID] = set()
    requests: set[UUID] = set()
    for credential in credentials:
        validate_stored_registry_credential_public(
            credential, registry_origin=registry_origin, plan=plan
        )
        if (
            credential.credential_id in identities
            or credential.request_id in requests
            or credential.component not in by_component
            or credential.materialization_attempt_id != attempt.id
            or credential.materialization_id != row.id
            or credential.attempt_number != attempt.attempt_number
            or credential.lease_epoch != attempt.lease_epoch
            or credential.grant_id != attempt.grant_id
            or credential.builder_id != attempt.builder_id
            or credential.repository
            != publication_repository(
                purpose="production",
                shadow_campaign_id=None,
                cpu_arch=plan.cpu_arch,
                attempt_id=attempt.id,
                component=credential.component,
            )
            or credential.session_generation < attempt.session_generation
        ):
            raise ValueError("credential attempt binding changed")
        identities.add(credential.credential_id)
        requests.add(credential.request_id)
        by_component[credential.component].append(credential)
    repositories = []
    for component, generations in by_component.items():
        if not generations:
            continue
        generations.sort(key=lambda item: item.generation)
        previous = None
        for number, credential in enumerate(generations, 1):
            if credential.generation != number or (
                previous is not None
                and (
                    credential.predecessor_credential_id != previous.credential_id
                    or credential.session_generation <= previous.session_generation
                    or credential.attestation_generation <= previous.attestation_generation
                )
            ):
                raise ValueError("credential history is incomplete or changed")
            previous = credential
        evidence = [
            {
                "credential_id": str(item.credential_id),
                "public_binding_sha256": item.response_sha256,
            }
            for item in generations
        ]
        repositories.append(
            AttemptRepository(
                component,
                generations[0].repository,
                max(item.expires_at.astimezone(UTC) for item in generations),
                len(generations),
                hashlib.sha256(rfc8785.dumps(evidence)).hexdigest(),
            )
        )
    canonical = rfc8785.dumps(
        {
            "schema": "loom.task-image-attempt-repositories/v1",
            "materialization_id": str(row.id),
            "attempt_id": str(attempt.id),
            "grant_id": str(attempt.grant_id),
            "cpu_arch": plan.cpu_arch,
            "purpose": "production",
            "registry_origin": registry_origin,
            "frozen_plan_sha256": attempt.claim_plan_sha256,
            "repositories": [
                {
                    "component": item.component,
                    "repository": item.repository,
                    "last_credential_expires_at": item.last_credential_expires_at.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "credential_count": item.credential_count,
                    "credential_set_sha256": item.credential_set_sha256,
                }
                for item in repositories
            ],
        }
    )
    if len(canonical) > 128 * 1024:
        raise ValueError("inventory exceeds bound")
    return AttemptRepositoryInventory(
        row.id, attempt.id, registry_origin, tuple(repositories), canonical
    )
