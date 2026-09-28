"""Atomic stopped receipts and app-only reservation release.

Only the internal concrete lifecycle coordinator supplies attestations. Database
locks serialize completion with new effects, transitions and platform admission;
physical admission/access fences remain the authority for external retirement.
"""
from __future__ import annotations

import hashlib
import re
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_cloud_schema import NebiusApplicationCloudEffect
from loom.db.nebius_application_effect_schema import NebiusApplicationEffect
from loom.db.nebius_application_material_schema import NebiusApplicationMaterial
from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom.db.nebius_application_schema import NebiusApplication
from loom.db.nebius_environment_schema import NebiusPlatformBudget
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_credentials import application_credential_names
from loom_service.application_management.cloud_effects import ApplicationCloudJournal
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.material import _load
from loom_service.application_management.proofs import (
    ApplicationKeyRetirement,
    ApplicationResourceObservation,
    ApplicationRetirementIdentity,
    ApplicationStopEvidence,
)
from loom_service.environment_management.platform_accounting import ENVELOPE_FIELDS
from loom_service.environment_management.registry import ManagementError


def _conflict() -> ManagementError:
    return ManagementError("application_completion_evidence_conflict")


def _workloads(evidence: ApplicationStopEvidence, operation: NebiusApplicationOperation,
               effects: list[NebiusApplicationEffect]) -> None:
    row = operation.plan_json["registration"]
    workload = evidence.workloads
    observed = [item for item in effects if item.phase == "observed"]

    def reference(proof: ApplicationResourceObservation, kind: str, name: str) -> NebiusApplicationEffect:
        effect = next((item for item in observed if item.operation_id == proof.operation_id
                       and item.effect_key == proof.key), None)
        if (effect is None or effect.observed_uid != proof.uid or proof.name != name
                or effect.intent_json["kind"] != kind or effect.intent_json["name"] != name
                or effect.intent_json["namespace"] != (None if kind == "Namespace" else row["application_namespace"])
                or effect.intent_json["action"] not in {"create", "patch"}):
            raise _conflict()
        latest = next(item for item in reversed(observed) if item.intent_json["kind"] == kind
                      and item.intent_json["name"] == name and item.observed_uid == proof.uid)
        if latest is not effect:
            raise _conflict()
        return effect

    namespace = reference(workload.namespace, "Namespace", row["application_namespace"])
    fence = reference(workload.fence, "ResourceQuota", "loom-application-retired")
    if namespace.intent_json["action"] != "create" or fence.operation_id != operation.operation_id:
        raise _conflict()
    deployments: dict[tuple[str, str], NebiusApplicationEffect] = {}
    routes: set[tuple[str, str, str]] = set()
    for item in observed:
        kind, action, name = (item.intent_json[field] for field in ("kind", "action", "name"))
        uid = item.observed_uid
        assert uid is not None
        if kind == "Deployment":
            if action == "delete":
                deployments.pop((name, uid), None)
            else:
                deployments[name, uid] = item
        elif kind in {"Service", "Ingress"}:
            if action == "delete":
                routes.discard((kind, name, uid))
            else:
                routes.add((kind, name, uid))
    actual_deployments = {(item.name, item.uid) for item in workload.deployments}
    if routes or actual_deployments != deployments.keys() or len(actual_deployments) != len(workload.deployments):
        raise _conflict()
    for proof in workload.deployments:
        effect = reference(proof, "Deployment", proof.name)
        if (effect.intent_json["action"] != "patch"
                or re.fullmatch(r"retire:scale:[0-9a-f]{32}:[0-9a-f]{64}", effect.effect_key) is None
                or proof.observed_generation < proof.generation):
            raise _conflict()


async def _cloud(session: AsyncSession, evidence: ApplicationStopEvidence,
                 operations: dict[UUID, NebiusApplicationOperation], effects: list[NebiusApplicationCloudEffect]) -> None:
    expected: list[ApplicationKeyRetirement] = []
    for effect in effects:
        intent = effect.intent_json
        if effect.phase != "observed" or intent["action"] != "create":
            continue
        deletion_intent = intent | {"action": "delete", "resource_id": effect.observed_resource_id,
            "source_operation_id": str(effect.operation_id), "source_key": effect.effect_key}
        deletions = [item for item in effects
                     if item.effect_key == f"retire:{effect.operation_id.hex}:{effect.effect_key}"]
        if (len(deletions) != 1 or deletions[0].phase != "observed"
                or deletions[0].intent_json != deletion_intent
                or deletions[0].observed_resource_id != effect.observed_resource_id):
            raise _conflict()
        if intent["kind"] != "access_key":
            continue
        operation = operations[effect.operation_id]
        material_row = await session.get(NebiusApplicationMaterial, effect.operation_id)
        if material_row is None:
            if any(item.operation_id == effect.operation_id and item.intent_json["kind"] == "membership"
                   for item in effects):
                raise _conflict()
            continue  # Permissionless, never committed or delivered.
        material = await _load(session, operation, material_row)
        registration = ApplicationRegistrationV1.model_validate(operation.plan_json["registration"])
        if (registration.data_environment_id != evidence.objects.identity.data_environment_id
                or registration.incarnation != evidence.objects.identity.incarnation):
            raise _conflict()
        bundle = material[application_credential_names(registration)["storage"]]
        key = bundle.get("access-key")
        if not key:
            raise _conflict()
        expected.append(ApplicationKeyRetirement(operation_id=effect.operation_id, key=effect.effect_key,
            access_key_sha256=hashlib.sha256(key.encode()).hexdigest()))
    # Provider history and this query have the same generation/sequence order.
    if tuple(expected) != evidence.objects.keys:
        raise _conflict()


class ApplicationCompletionJournal(ApplicationCloudJournal):
    @staticmethod
    async def _lock_budget(session: AsyncSession, cluster_id: str) -> NebiusPlatformBudget:
        budget = await session.scalar(select(NebiusPlatformBudget).where(
            NebiusPlatformBudget.cluster_id == cluster_id,
        ).with_for_update())
        if budget is None:
            raise ManagementError("platform_budget_not_configured", 503)
        return budget

    async def complete_stopped(self, lease: ApplicationLease, evidence: ApplicationStopEvidence) -> None:
        try:
            evidence = ApplicationStopEvidence.model_validate(evidence)
        except ValidationError:
            raise _conflict() from None
        async with self.session_factory.begin() as session:
            cluster_id = await session.scalar(select(NebiusApplication.cluster_id).where(
                NebiusApplication.application_id == lease.application_id))
            if cluster_id is None:
                raise ManagementError("stale_operation_lease")
            await self._lock_budget(session, cluster_id)
            operation, row, _ = await self._locked_operation(session, lease.operation_id)
            identity = ApplicationRetirementIdentity.for_lease(lease, row.data_environment_id)
            if (not self._current(operation, row) or row.application_id != lease.application_id
                    or row.incarnation != lease.incarnation or operation.deployment_generation != lease.deployment_generation
                    or operation.access_generation != lease.access_generation or operation.runner_epoch != lease.runner_epoch):
                raise ManagementError("stale_operation_lease")
            receipt = evidence.model_dump(mode="json")
            if operation.phase == "completed":
                if operation.completion_json == receipt and evidence.workloads.identity == identity:
                    return  # Exact replay is read-only, including its original timestamp.
                raise _conflict()
            await self._leased(session, lease)
            if (row.desired_state not in {"suspended", "destroyed"}
                    or operation.action not in {"suspend", "destroy_retained"}
                    or operation.plan_json["registration"]["desired_state"] != row.desired_state
                    or any(proof.identity != identity for proof in (evidence.workloads, evidence.database, evidence.objects))
                    or evidence.database.retired_through != lease.access_generation):
                raise _conflict()
            history = list(await session.scalars(select(NebiusApplicationOperation).where(
                NebiusApplicationOperation.application_id == row.application_id,
            ).order_by(NebiusApplicationOperation.deployment_generation)))
            operations = {item.operation_id: item for item in history}
            effects = list(await session.scalars(select(NebiusApplicationEffect).join(NebiusApplicationOperation).where(
                NebiusApplicationOperation.application_id == row.application_id,
            ).order_by(NebiusApplicationOperation.deployment_generation, NebiusApplicationEffect.sequence)))
            cloud = list(await session.scalars(select(NebiusApplicationCloudEffect).join(NebiusApplicationOperation).where(
                NebiusApplicationOperation.application_id == row.application_id,
            ).order_by(NebiusApplicationOperation.deployment_generation, NebiusApplicationCloudEffect.sequence)))
            all_effects: list[NebiusApplicationEffect | NebiusApplicationCloudEffect] = [*effects, *cloud]
            if any(item.phase == "dispatched" or (item.operation_id == lease.operation_id and item.phase == "prepared")
                   for item in all_effects):
                raise ManagementError("application_completion_effects_pending")
            _workloads(evidence, operation, effects)
            await _cloud(session, evidence, operations, cloud)
            reservation = await session.get(NebiusApplicationReservation, row.application_id)
            if reservation is None or reservation.cluster_id != cluster_id or reservation.storage_mib != 0:
                raise ManagementError("platform_reservation_missing")
            # Decryption/history validation can take time; expiry before COMMIT
            # must not convert an old proof into completion authority.
            _, now = await self._leased(session, lease)
            operation.completion_json, operation.completed_at = receipt, now
            operation.phase, operation.error_code = "completed", None
            operation.lease_token = operation.lease_expires_at = None
            for name in ENVELOPE_FIELDS:
                setattr(reservation, name, 0)
