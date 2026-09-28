"""Fixed-purpose application IAM intent; no bucket/policy or shared-group writes.

Only protected lifecycle callers supply storage bindings. Intent observation is
not runtime readiness, S3 propagation evidence or permission to release capacity.
"""
from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_cloud_schema import NebiusApplicationCloudEffect
from loom.db.nebius_application_material_schema import NebiusApplicationMaterial
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.nebius_environment_contract import _PROVIDER_ID
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.material import (
    ApplicationMaterialJournal,
    _identity,
    _load,
)
from loom_service.environment_management.registry import ManagementError


class ApplicationStorageAccessV1(BaseModel):
    """Already-qualified shared groups in a protected provisioning project."""
    model_config = ConfigDict(extra="forbid", frozen=True)

    data_environment_id: UUID
    project_id: str = Field(pattern=_PROVIDER_ID)
    data_group_id: str = Field(pattern=_PROVIDER_ID)
    source_group_id: str = Field(pattern=_PROVIDER_ID)

    @model_validator(mode="after")
    def distinct(self) -> Self:
        if self.data_environment_id.int == 0 or self.data_group_id == self.source_group_id:
            raise ValueError("invalid shared storage binding")
        return self


@dataclass(frozen=True)
class ApplicationCloudEffect:
    operation_id: UUID
    key: str
    sequence: int
    kind: str
    action: str
    expected: dict[str, Any]
    resource_id: str | None
    phase: str
    dispatch_epoch: int | None
    observed_resource_id: str | None


def _view(row: NebiusApplicationCloudEffect) -> ApplicationCloudEffect:
    value = row.intent_json
    return ApplicationCloudEffect(row.operation_id, row.effect_key, row.sequence, value["kind"], value["action"],
        copy.deepcopy(value["expected"]), value["resource_id"], row.phase, row.dispatch_epoch, row.observed_resource_id)


class ApplicationCloudJournal(ApplicationMaterialJournal):
    @staticmethod
    async def _cloud_dependency(session: AsyncSession, operation_id: UUID, key: str) -> NebiusApplicationCloudEffect:
        row = await session.get(NebiusApplicationCloudEffect, (operation_id, key))
        if row is None or row.phase != "observed" or row.intent_json["action"] != "create":
            raise ManagementError("application_cloud_dependency_missing")
        return row

    @staticmethod
    async def _insert_cloud(session: AsyncSession, operation_id: UUID, key: str,
                            intent: dict[str, Any]) -> ApplicationCloudEffect:
        existing = await session.get(NebiusApplicationCloudEffect, (operation_id, key))
        if existing is not None:
            if existing.intent_json != intent:
                raise ManagementError("application_cloud_intent_conflict")
            return _view(existing)
        last = await session.scalar(select(NebiusApplicationCloudEffect).where(
            NebiusApplicationCloudEffect.operation_id == operation_id,
        ).order_by(NebiusApplicationCloudEffect.sequence.desc()).limit(1))
        if last is not None and last.phase != "observed":
            raise ManagementError("application_cloud_unresolved")
        row = NebiusApplicationCloudEffect(operation_id=operation_id, effect_key=key,
            sequence=1 if last is None else last.sequence + 1, intent_json=intent, phase="prepared")
        session.add(row)
        await session.flush()
        return _view(row)

    async def prepare_cloud_create(self, lease: ApplicationLease, key: str,
                                   binding: dict[str, Any]) -> ApplicationCloudEffect:
        try:
            access = ApplicationStorageAccessV1.model_validate(binding)
            if key not in {"account", "key", "data", "source"}:
                raise ValueError("unsupported cloud effect")
        except (ValueError, TypeError):
            raise ManagementError("invalid_application_cloud_binding", 422) from None
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            try:
                identity = _identity(operation)
            except (ValueError, TypeError, KeyError):
                raise ManagementError("invalid_application_cloud_operation", 422) from None
            if str(access.data_environment_id) != identity["data_environment_id"]:
                raise ManagementError("invalid_application_cloud_binding", 422)
            frozen = access.model_dump(mode="json")
            account = await session.get(NebiusApplicationCloudEffect, (lease.operation_id, "account"))
            if account is not None and account.intent_json["binding"] != frozen:
                raise ManagementError("application_cloud_binding_conflict")
            name = f"loom-app-{lease.incarnation.hex}-g{lease.access_generation}"
            labels = {"loom-application-id": str(lease.application_id), "loom-incarnation": str(lease.incarnation),
                      "loom-data-environment-id": str(access.data_environment_id),
                      "loom-operation-id": str(lease.operation_id), "loom-access-generation": str(lease.access_generation),
                      "loom-effect-key": key}
            metadata: dict[str, Any] = {"parent_id": access.project_id, "name": name, "labels": labels}
            spec: dict[str, Any] = {"description": "Loom personal application generation objects"}
            kind = "service_account"
            if key != "account":
                account = await self._cloud_dependency(session, lease.operation_id, "account")
                if key == "key":
                    kind = "access_key"
                    spec.update(account={"service_account": {"id": account.observed_resource_id}},
                                secret_delivery_mode="EXPLICIT")
                else:
                    await self._cloud_dependency(session, lease.operation_id, "key")
                    material = await session.get(NebiusApplicationMaterial, lease.operation_id)
                    if material is None:
                        raise ManagementError("application_material_missing", 503)
                    await _load(session, operation, material)
                    kind, spec = "membership", {"member_id": account.observed_resource_id}
                    metadata.pop("name")
                    metadata["parent_id"] = access.data_group_id if key == "data" else access.source_group_id
            return await self._insert_cloud(session, lease.operation_id, key, dict(
                kind=kind, action="create", expected={"metadata": metadata, "spec": spec},
                resource_id=None, binding=frozen))

    @staticmethod
    async def _cloud_target(session: AsyncSession, current: NebiusApplicationOperation,
                            operation_id: UUID, key: str) -> NebiusApplicationCloudEffect:
        target = await session.get(NebiusApplicationOperation, operation_id)
        if (target is None or target.application_id != current.application_id
                or target.deployment_generation > current.deployment_generation):
            raise ManagementError("application_cloud_forbidden", 403)
        effect = await session.get(NebiusApplicationCloudEffect, (operation_id, key))
        if effect is None:
            raise ManagementError("application_cloud_dependency_missing")
        return effect

    async def prepare_cloud_delete(self, lease: ApplicationLease, operation_id: UUID,
                                   key: str) -> ApplicationCloudEffect:
        async with self.session_factory.begin() as session:
            current, _ = await self._leased(session, lease)
            target = await self._cloud_target(session, current, operation_id, key)
            if target.phase != "observed" or target.intent_json["action"] != "create":
                raise ManagementError("application_cloud_dependency_missing")
            intent = copy.deepcopy(target.intent_json) | {"action": "delete",
                "resource_id": target.observed_resource_id, "source_operation_id": str(operation_id), "source_key": key}
            retirement_key = f"retire:{operation_id.hex}:{key}"
            # Retirement belongs to the resource, not the latest stop operation.
            # Supersession must not grant a second send after a lost DELETE reply.
            previous = (await session.scalars(select(NebiusApplicationCloudEffect).join(
                NebiusApplicationOperation,
                NebiusApplicationOperation.operation_id == NebiusApplicationCloudEffect.operation_id,
            ).where(
                NebiusApplicationOperation.application_id == current.application_id,
                NebiusApplicationOperation.deployment_generation <= current.deployment_generation,
                NebiusApplicationCloudEffect.effect_key == retirement_key,
            ))).all()
            if previous:
                if len(previous) != 1 or previous[0].intent_json != intent:
                    raise ManagementError("application_cloud_retirement_conflict")
                return _view(previous[0])
            return await self._insert_cloud(session, lease.operation_id, retirement_key, intent)

    async def dispatch_cloud_effect(self, lease: ApplicationLease, key: str,
                                    *, operation_id: UUID | None = None) -> bool:
        """Exactly one True. False means reconcile; NEVER resend an unknown write."""
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            row = await self._cloud_target(session, operation, operation_id or lease.operation_id, key)
            if row.operation_id != lease.operation_id and row.intent_json["action"] != "delete":
                raise ManagementError("application_cloud_forbidden", 403)
            if row.phase != "prepared":
                return False
            row.phase, row.dispatch_epoch = "dispatched", lease.runner_epoch
            return True

    async def observe_cloud_effect(self, lease: ApplicationLease, operation_id: UUID, key: str,
                                   *, resource_id: str) -> None:
        """Current authority may reconcile old requests, but cannot resend them."""
        if not isinstance(resource_id, str) or re.fullmatch(_PROVIDER_ID, resource_id) is None:
            raise ManagementError("invalid_application_cloud_observation", 422)
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            row = await self._cloud_target(session, operation, operation_id, key)
            if row.phase == "prepared":
                raise ManagementError("application_cloud_not_dispatched")
            if ((row.intent_json["action"] == "delete" and row.intent_json["resource_id"] != resource_id)
                    or (row.phase == "observed" and row.observed_resource_id != resource_id)):
                raise ManagementError("application_cloud_observation_conflict")
            row.phase, row.observed_resource_id = "observed", resource_id

    async def cloud_history(self, lease: ApplicationLease) -> list[ApplicationCloudEffect]:
        async with self.session_factory.begin() as session:
            current, _ = await self._leased(session, lease)
            rows = await session.scalars(select(NebiusApplicationCloudEffect).join(NebiusApplicationOperation,
                NebiusApplicationOperation.operation_id == NebiusApplicationCloudEffect.operation_id).where(
                    NebiusApplicationOperation.application_id == lease.application_id,
                    NebiusApplicationOperation.deployment_generation <= current.deployment_generation,
                ).order_by(NebiusApplicationOperation.deployment_generation, NebiusApplicationCloudEffect.sequence))
            return [_view(row) for row in rows]
