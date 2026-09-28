"""Durable mutation intent; not external-effect fencing or runtime readiness.

Only trusted lifecycle providers use this journal. They must verify request
digests, ownership and actual responses; a stored digest is not that proof.
An uncertain dispatch is never automatically re-authorized after lease expiry.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Literal, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_effect_schema import NebiusApplicationEffect
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom_service.application_management.leases import ApplicationLease, ApplicationOperationJournal
from loom_service.environment_management.registry import ManagementError

_NAME = r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?"
_IDENTITY = r"[A-Za-z0-9._:-]{1,256}"
_APIS = {
    "Namespace": "v1", "Secret": "v1", "Service": "v1", "ServiceAccount": "v1",
    "Pod": "v1", "ResourceQuota": "v1", "Deployment": "apps/v1",
    "RoleBinding": "rbac.authorization.k8s.io/v1",
    "Ingress": "networking.k8s.io/v1", "NetworkPolicy": "networking.k8s.io/v1",
}
_ACTIVATION_PREFIX = "activate:unfence:"


class KubernetesEffectIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    api_version: str
    kind: str
    namespace: str | None
    name: str = Field(pattern="^" + _NAME + "$")
    action: Literal["create", "patch", "delete"]
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    uid: str | None = Field(default=None, pattern="^" + _IDENTITY + "$")
    resource_version: str | None = Field(default=None, pattern="^" + _IDENTITY + "$")

    @model_validator(mode="after")
    def exact_target(self) -> Self:
        if _APIS.get(self.kind) != self.api_version:
            raise ValueError("unsupported application resource")
        if self.action == "create":
            if self.uid is not None or self.resource_version is not None:
                raise ValueError("CREATE has no preconditions")
        elif self.uid is None or self.resource_version is None:
            raise ValueError("PATCH/DELETE require exact UID and resourceVersion")
        if self.kind in {"Namespace", "RoleBinding"} and self.action != "create":
            raise ValueError("bootstrap identities are create-only")
        if self.kind == "Pod" and self.action != "delete":
            raise ValueError("only exact Pod retirement is allowed")
        return self


@dataclass(frozen=True)
class ApplicationEffect:
    operation_id: UUID
    key: str
    sequence: int
    intent: KubernetesEffectIntent
    phase: str
    dispatch_epoch: int | None
    observed_uid: str | None
    observed_resource_version: str | None
    rejection_status: int | None


def _view(row: NebiusApplicationEffect) -> ApplicationEffect:
    return ApplicationEffect(row.operation_id, row.effect_key, row.sequence,
                             KubernetesEffectIntent.model_validate(row.intent_json), row.phase,
                             row.dispatch_epoch, row.observed_uid, row.observed_resource_version, row.rejection_status)


def _secret_targets(operation: NebiusApplicationOperation) -> set[str]:
    """Use frozen references, including predecessor material on a stop plan.

    This is not qualification of arbitrary Deployment templates. Plans are
    trusted management inputs; new active providers must qualify their material
    generation before use. Historical fixed-name plans remain readable/retirable.
    """
    names: set[str] = set()
    for docs in operation.plan_json["files"].values():
        for doc in docs:
            if doc["kind"] != "Deployment":
                continue
            pod = doc["spec"]["template"]["spec"]
            for container in pod.get("containers", []):
                for env in container.get("env", []):
                    name = env.get("valueFrom", {}).get("secretKeyRef", {}).get("name")
                    if isinstance(name, str):
                        names.add(name)
            for volume in pod.get("volumes", []):
                name = volume.get("secret", {}).get("secretName")
                if isinstance(name, str):
                    names.add(name)
    return names


async def _validate_target(session: AsyncSession, operation: NebiusApplicationOperation,
                           intent: KubernetesEffectIntent) -> None:
    binding = ApplicationRegistrationV1.model_validate(operation.plan_json["registration"])
    ns = binding.application_namespace
    if intent.kind == "Namespace":
        owned = intent.namespace is None and intent.name == ns
    else:
        owned = intent.namespace == ns
    planned = any(doc["kind"] == intent.kind and doc["apiVersion"] == intent.api_version
                  and doc["metadata"]["name"] == intent.name
                  for docs in operation.plan_json["files"].values() for doc in docs)
    additional = (intent.kind == "Secret" and intent.name in _secret_targets(operation)) or (
        intent.kind == "ResourceQuota" and intent.name == "loom-application-retired") or intent.kind == "Pod"
    if owned and not (planned or additional) and intent.kind == "Secret" and intent.action == "delete":
        # Update/resume freeze NEW references and supersede the old lease. A
        # subsequent stop may interrupt that update before older material retires.
        # Consult all earlier plans for this application, never another app's
        # history, and never authorize CREATE/PATCH of retired material.
        predecessors = await session.scalars(select(NebiusApplicationOperation).where(
            NebiusApplicationOperation.application_id == operation.application_id,
            NebiusApplicationOperation.deployment_generation < operation.deployment_generation,
        ))
        additional = any(intent.name in _secret_targets(previous) for previous in predecessors)
    if not owned or not (planned or additional):
        raise ManagementError("invalid_application_effect", 422)


def _activation_intent(operation: NebiusApplicationOperation, key: str, intent: KubernetesEffectIntent) -> bool:
    quota_delete = intent.kind == "ResourceQuota" and intent.action == "delete"
    if not key.startswith("activate:") and not quota_delete:
        return False
    expected_key = _ACTIVATION_PREFIX + hashlib.sha256(f"{intent.uid}:{intent.resource_version}".encode()).hexdigest()
    if (not quota_delete or intent.name != "loom-application-retired" or key != expected_key
            or operation.action not in {"create", "update", "resume"}
            or operation.plan_json["registration"]["desired_state"] != "active"):
        raise ManagementError("invalid_application_effect", 422)
    return True


async def _activation_effect(session: AsyncSession, operation_id: UUID) -> NebiusApplicationEffect | None:
    return cast(NebiusApplicationEffect | None, await session.scalar(select(NebiusApplicationEffect).where(
        NebiusApplicationEffect.operation_id == operation_id,
        NebiusApplicationEffect.effect_key.startswith(_ACTIVATION_PREFIX),
    ).order_by(NebiusApplicationEffect.sequence.desc()).limit(1)))


class ApplicationEffectJournal(ApplicationOperationJournal):
    async def activation_started(self, lease: ApplicationLease) -> bool:
        """Current generation crossed preparation; not Pod or API readiness.

        Even a rejected unfence retains this phase. A fresh exact precondition
        may retry opening admission, but peers cannot re-enter old retirement.
        """
        async with self.session_factory.begin() as session:
            await self._leased(session, lease)
            return await _activation_effect(session, lease.operation_id) is not None

    async def prepare_effect(self, lease: ApplicationLease, key: str, intent: dict[str, Any]) -> ApplicationEffect:
        """Persist a no-secret locator and request digest before authorizing I/O."""
        try:
            parsed = KubernetesEffectIntent.model_validate(intent)
        except ValidationError:
            raise ManagementError("invalid_application_effect", 422) from None
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", key) is None:
            raise ManagementError("invalid_application_effect", 422)
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            await _validate_target(session, operation, parsed)
            activation = _activation_intent(operation, key, parsed)
            existing = await session.get(NebiusApplicationEffect, (lease.operation_id, key))
            value = parsed.model_dump(mode="json")
            if existing is not None:
                if existing.intent_json != value:
                    raise ManagementError("application_effect_conflict")
                return _view(existing)
            opening = await _activation_effect(session, lease.operation_id)
            if key.startswith("start:"):
                if (parsed.kind not in {"Deployment", "Service", "Ingress"}
                        or parsed.action not in {"create", "patch"}
                        or operation.plan_json["registration"]["desired_state"] != "active"):
                    raise ManagementError("invalid_application_effect", 422)
                if opening is None or opening.phase != "observed":
                    raise ManagementError("application_activation_pending")
            if opening is not None and (
                key.startswith("retire:") or (parsed.kind == "ResourceQuota" and not activation)
                or (parsed.kind in {"ServiceAccount", "NetworkPolicy", "Secret"} and parsed.action in {"create", "patch"})
                or (activation and opening.phase != "rejected")
            ):
                # Atomic with preparation under the application/operation locks:
                # a stale same-lease peer must not stop the just-started runtime.
                raise ManagementError("application_activation_started")
            previous = await session.scalar(select(NebiusApplicationEffect).where(
                NebiusApplicationEffect.operation_id == lease.operation_id,
            ).order_by(NebiusApplicationEffect.sequence.desc()).limit(1))
            if previous is not None and previous.phase not in {"observed", "rejected"}:
                raise ManagementError("application_effect_unresolved")
            row = NebiusApplicationEffect(operation_id=lease.operation_id, effect_key=key,
                sequence=1 if previous is None else previous.sequence + 1, intent_json=value, phase="prepared")
            session.add(row)
            await session.flush()
            return _view(row)

    async def _effect(self, session: AsyncSession, lease: ApplicationLease, key: str, *,
                      operation_id: UUID | None = None) -> NebiusApplicationEffect:
        current, _ = await self._leased(session, lease)
        if operation_id is not None:
            await self._historical_operation(session, current, operation_id)
        # Application/operation locks serialize all journal writers, including
        # callers sharing a lease. Never take a platform budget lock after this.
        row = await session.get(NebiusApplicationEffect, (operation_id or lease.operation_id, key))
        if row is None:
            raise ManagementError("application_effect_missing")
        return row

    async def dispatch_effect(self, lease: ApplicationLease, key: str) -> bool:
        """Exactly one caller gets True; False means reconcile, NEVER resend.

        A crash after this commit but before I/O is deliberately ambiguous.
        A GET/404 alone cannot rule out an earlier request completing later.
        """
        async with self.session_factory.begin() as session:
            row = await self._effect(session, lease, key)
            if row.phase != "prepared":
                return False
            row.phase, row.dispatch_epoch = "dispatched", lease.runner_epoch
            return True

    async def observe_effect(self, lease: ApplicationLease, key: str, *, uid: str,
                             resource_version: str | None, operation_id: UUID | None = None) -> None:
        """Trusted provider confirms effect, NOT readiness or process retirement.

        For DELETE, uid identifies the retired object and resource_version is
        None. Other actions require the observed UID/RV. Preconditioned actions
        cannot be confirmed against a replacement object with a different UID.
        A current lease may record a predecessor effect for the same application;
        this grants no permission to dispatch that predecessor's request.
        """
        async with self.session_factory.begin() as session:
            row = await self._effect(session, lease, key, operation_id=operation_id)
            intent = KubernetesEffectIntent.model_validate(row.intent_json)
            if (not isinstance(uid, str) or re.fullmatch(_IDENTITY, uid) is None
                    or (intent.uid is not None and uid != intent.uid)
                    or (intent.action == "delete" and resource_version is not None)
                    or (intent.action != "delete" and (not isinstance(resource_version, str)
                        or re.fullmatch(_IDENTITY, resource_version) is None))):
                raise ManagementError("invalid_application_effect_observation", 422)
            if row.phase == "prepared":
                raise ManagementError("application_effect_not_dispatched")
            if row.phase == "rejected":
                raise ManagementError("application_effect_observation_conflict")
            if row.phase == "observed":
                if (row.observed_uid, row.observed_resource_version) != (uid, resource_version):
                    raise ManagementError("application_effect_observation_conflict")
                return
            row.phase, row.observed_uid, row.observed_resource_version = "observed", uid, resource_version

    async def reject_effect(self, lease: ApplicationLease, key: str, *, status_code: int) -> None:
        """Trusted direct API response proves no write: retain, NEVER reset.

        Only exact Kubernetes conflict/validation responses qualify. Timeout,
        throttling and server errors remain uncertain. A new key may freeze new
        preconditions; this key and its dispatch can never be reused for a write.
        """
        if type(status_code) is not int or status_code not in {409, 422}:
            raise ManagementError("invalid_application_effect_rejection", 422)
        async with self.session_factory.begin() as session:
            row = await self._effect(session, lease, key)
            if row.phase == "prepared":
                raise ManagementError("application_effect_not_dispatched")
            if row.phase == "observed" or (row.phase == "rejected" and row.rejection_status != status_code):
                raise ManagementError("application_effect_observation_conflict")
            row.phase, row.rejection_status = "rejected", status_code

    async def effect_history(self, lease: ApplicationLease) -> list[ApplicationEffect]:
        """Include superseded/uncertain predecessors for SAME-application cleanup."""
        async with self.session_factory.begin() as session:
            await self._leased(session, lease)
            rows = await session.scalars(select(NebiusApplicationEffect).join(
                NebiusApplicationOperation,
                NebiusApplicationOperation.operation_id == NebiusApplicationEffect.operation_id,
            ).where(NebiusApplicationOperation.application_id == lease.application_id).order_by(
                NebiusApplicationOperation.deployment_generation, NebiusApplicationEffect.sequence,
            ))
            return [_view(row) for row in rows]
