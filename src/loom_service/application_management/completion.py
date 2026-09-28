"""Atomic ready/stopped receipts and app-only reservation accounting.

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
from loom.nebius_application_authority import APPLICATION_INSTALLATION_LABEL
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_credentials import application_credential_names
from loom_service.application_management.cloud_effects import ApplicationCloudJournal
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.material import _load
from loom_service.application_management.proofs import (
    ApplicationCloudRetirement,
    ApplicationKeyRetirement,
    ApplicationReadyEvidence,
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


async def _cloud(session: AsyncSession, evidence: ApplicationCloudRetirement,
                 operations: dict[UUID, NebiusApplicationOperation], effects: list[NebiusApplicationCloudEffect],
                 *, keep_operation: UUID | None = None) -> None:
    expected: list[ApplicationKeyRetirement] = []
    for effect in effects:
        intent = effect.intent_json
        if effect.phase != "observed" or intent["action"] != "create" or effect.operation_id == keep_operation:
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
        if (registration.data_environment_id != evidence.identity.data_environment_id
                or registration.incarnation != evidence.identity.incarnation):
            raise _conflict()
        bundle = material[application_credential_names(registration)["storage"]]
        key = bundle.get("access-key")
        if not key:
            raise _conflict()
        expected.append(ApplicationKeyRetirement(operation_id=effect.operation_id, key=effect.effect_key,
            access_key_sha256=hashlib.sha256(key.encode()).hexdigest()))
    # Provider history and this query have the same generation/sequence order.
    if tuple(expected) != evidence.keys:
        raise _conflict()


def _ready_resources(evidence: ApplicationReadyEvidence, operation: NebiusApplicationOperation,
                     effects: list[NebiusApplicationEffect]) -> None:
    plan, workload = operation.plan_json, evidence.workloads
    row = plan["registration"]
    observed = [item for item in effects if item.phase == "observed"]

    def reference(proof: ApplicationResourceObservation, kind: str, name: str,
                  *, current: bool = True) -> NebiusApplicationEffect:
        effect = next((item for item in observed if item.operation_id == proof.operation_id
                       and item.effect_key == proof.key), None)
        if (effect is None or effect.observed_uid != proof.uid or proof.name != name
                or effect.intent_json["kind"] != kind or effect.intent_json["name"] != name
                or effect.intent_json["namespace"] != (None if kind == "Namespace" else row["application_namespace"])
                or effect.intent_json["action"] not in {"create", "patch"}
                or (current and effect.operation_id != operation.operation_id)):
            raise _conflict()
        latest = next(item for item in reversed(observed) if item.intent_json["kind"] == kind
                      and item.intent_json["name"] == name and item.observed_uid == proof.uid)
        if latest is not effect:
            raise _conflict()
        return effect

    namespace = reference(workload.namespace, "Namespace", row["application_namespace"], current=False)
    if reference(evidence.prepared.namespace, "Namespace", row["application_namespace"], current=False) is not namespace:
        raise _conflict()
    opening = next((item for item in reversed(effects) if item.operation_id == operation.operation_id
                    and item.effect_key.startswith("activate:unfence:")), None)
    if (opening is None or opening.phase != "observed" or opening.effect_key != workload.activation_key
            or opening.observed_uid != workload.retired_quota_uid
            or opening.intent_json["kind"] != "ResourceQuota" or opening.intent_json["action"] != "delete"
            or opening.intent_json["name"] != "loom-application-retired"):
        raise _conflict()
    documents = [doc for docs in plan["files"].values() for doc in docs]
    for kind, proofs in (("Deployment", workload.deployments), ("Service", workload.services), ("Ingress", (workload.ingress,))):
        expected = {doc["metadata"]["name"]: doc for doc in documents if doc["kind"] == kind}
        if {proof.name for proof in proofs} != expected.keys() or len(proofs) != len(expected):
            raise _conflict()
        for proof in proofs:
            effect = reference(proof, kind, proof.name)
            if not effect.effect_key.startswith("start:"):
                raise _conflict()
    deployments = {doc["metadata"]["name"]: doc for doc in documents if doc["kind"] == "Deployment"}
    for proof in workload.deployments:
        if (proof.observed_generation < proof.generation
                or proof.replicas != deployments[proof.name]["spec"]["replicas"]):
            raise _conflict()
    static = {doc["metadata"]["name"]: doc["kind"] for doc in documents
              if doc["kind"] in {"ServiceAccount", "NetworkPolicy"}}
    names = application_credential_names(ApplicationRegistrationV1.model_validate(row))
    static.update({name: "Secret" for name in names.values()})
    if {proof.name for proof in evidence.prepared.resources} != static.keys() or len(evidence.prepared.resources) != len(static):
        raise _conflict()
    for proof in evidence.prepared.resources:
        effect = reference(proof, static[proof.name], proof.name, current=static[proof.name] != "ServiceAccount")
        if static[proof.name] in {"Secret", "ServiceAccount"} and effect.intent_json["action"] != "create":
            raise _conflict()
    namespace_doc = next(doc for doc in documents if doc["kind"] == "Namespace")
    installation = UUID(namespace_doc["metadata"]["labels"][APPLICATION_INSTALLATION_LABEL])
    network_names = {f"loom-applications-{installation.hex}-{purpose}" for purpose in ("postgres", "control-plane", "gateway")}
    if {proof.name for proof in evidence.network} != network_names or len(evidence.network) != len(network_names):
        raise _conflict()


async def _ready_access(session: AsyncSession, evidence: ApplicationReadyEvidence,
                        operation: NebiusApplicationOperation, cloud: list[NebiusApplicationCloudEffect]) -> None:
    row = ApplicationRegistrationV1.model_validate(operation.plan_json["registration"])
    access = evidence.access
    if (access.user_id != row.owner_user_id or access.team_id != row.owner_team_id
            or access.schema_revision != operation.plan_json["release"]["schema_revision"]
            or access.database_role != f"lap_{row.incarnation.hex}_g{row.access_generation}"):
        raise _conflict()
    current = [effect for effect in cloud if effect.operation_id == operation.operation_id and effect.intent_json["action"] == "create"]
    expected = {"account": "service_account", "key": "access_key", "data": "membership", "source": "membership"}
    if ({effect.effect_key for effect in current} != expected.keys() or len(current) != len(expected)
            or any(effect.phase != "observed" or effect.intent_json["kind"] != expected[effect.effect_key] for effect in current)
            or any(effect.intent_json.get("source_operation_id") == str(operation.operation_id) for effect in cloud)):
        raise _conflict()
    material_row = await session.get(NebiusApplicationMaterial, operation.operation_id)
    if material_row is None:
        raise _conflict()
    material = await _load(session, operation, material_row)
    storage = material[application_credential_names(row)["storage"]]
    if hashlib.sha256(storage["access-key"].encode()).hexdigest() != access.access_key_sha256:
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
            await _cloud(session, evidence.objects, operations, cloud)
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

    async def complete_ready(self, lease: ApplicationLease, evidence: ApplicationReadyEvidence) -> None:
        """Record current concrete readiness; keep actual active compute charged."""
        try:
            evidence = ApplicationReadyEvidence.model_validate(evidence)
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
                    return
                raise _conflict()
            await self._leased(session, lease)
            if (row.desired_state != "active" or operation.action not in {"create", "update", "resume"}
                    or operation.plan_json["registration"]["desired_state"] != "active"
                    or any(proof.identity != identity for proof in (evidence.workloads, evidence.database,
                        evidence.objects, evidence.prepared, evidence.access))
                    or evidence.database.retired_through != lease.access_generation - 1):
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
            _ready_resources(evidence, operation, effects)
            await _cloud(session, evidence.objects, operations, cloud, keep_operation=lease.operation_id)
            await _ready_access(session, evidence, operation, cloud)
            reservation = await session.get(NebiusApplicationReservation, row.application_id)
            target = operation.plan_json["platform_envelope"]
            if (reservation is None or reservation.cluster_id != cluster_id or reservation.storage_mib != 0
                    or target["storage_mib"] != 0 or any(getattr(reservation, name) < target[name] for name in ENVELOPE_FIELDS)):
                raise ManagementError("platform_reservation_missing")
            _, now = await self._leased(session, lease)
            operation.completion_json, operation.completed_at = receipt, now
            operation.phase, operation.error_code = "completed", None
            operation.lease_token = operation.lease_expires_at = None
            for name in ENVELOPE_FIELDS:
                setattr(reservation, name, target[name])
