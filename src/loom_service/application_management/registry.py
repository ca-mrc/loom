"""Authenticated application intent, with no provider writes or readiness claims.

    Only trusted management callers pass qualified rendered plans. Public owners
    must never supply these plans, bindings, identities, manifests or resource costs.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.nebius_application_operation_schema import (
    NebiusApplicationOperation,
    NebiusApplicationReservation,
)
from loom.db.nebius_application_schema import NebiusApplication
from loom.db.nebius_environment_schema import NebiusPlatformBudget
from loom.nebius_application_contract import (
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
    SharedDevelopmentBindingV1,
)
from loom.nebius_application_render import RenderedApplication
from loom_service.application_management.material import ApplicationMaterialJournal
from loom_service.application_management.plans import freeze_plan
from loom_service.environment_management.platform_accounting import ENVELOPE_FIELDS, platform_usage
from loom_service.environment_management.registry import ManagementError, owner_identity


def operation_view(row: NebiusApplicationOperation) -> ApplicationOperationV1:
    return ApplicationOperationV1.model_validate({
        name: getattr(row, name) for name in ApplicationOperationV1.model_fields if name != "schema_version"
    })


def registration_view(row: NebiusApplication) -> ApplicationRegistrationV1:
    return ApplicationRegistrationV1.model_validate({
        name: getattr(row, name) for name in ApplicationRegistrationV1.model_fields if name != "schema_version"
    })


def _key(value: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value) is None:
        raise ManagementError("invalid_idempotency_key", 422)


def _fingerprint(**intent: Any) -> str:
    return hashlib.sha256(json.dumps(intent, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class ApplicationRegistry(ApplicationMaterialJournal):
    @staticmethod
    async def _replay(session: AsyncSession, owner: UUID, key: str, fingerprint: str) -> ApplicationOperationV1 | None:
        row = await session.scalar(select(NebiusApplicationOperation).where(
            NebiusApplicationOperation.owner_user_id == owner, NebiusApplicationOperation.idempotency_key == key,
        ))
        if row is None:
            return None
        if row.request_sha256 != fingerprint:
            raise ManagementError("idempotency_conflict")
        return operation_view(row)

    async def replay_create(self, *, principal: AuthContext, idempotency_key: str,
                            slug: str, release_id: UUID) -> ApplicationOperationV1 | None:
        owner, team = owner_identity(principal, mutation=True)
        _key(idempotency_key)
        fingerprint = _fingerprint(action="create", team=team, slug=slug, release_id=release_id)
        async with self.session_factory() as session:
            return await self._replay(session, owner, idempotency_key, fingerprint)

    @staticmethod
    async def _lock_budget(session: AsyncSession, cluster_id: str) -> NebiusPlatformBudget:
        budget = await session.scalar(select(NebiusPlatformBudget).where(
            NebiusPlatformBudget.cluster_id == cluster_id,
        ).with_for_update())
        if budget is None:
            raise ManagementError("platform_budget_not_configured", 503)
        return budget

    @staticmethod
    async def _capacity(session: AsyncSession, budget: NebiusPlatformBudget, needed: dict[str, int]) -> None:
        used = await platform_usage(session, budget.cluster_id)
        available = {name: max(0, getattr(budget, name) - used[name]) for name in ENVELOPE_FIELDS}
        if any(needed[name] > available[name] for name in ENVELOPE_FIELDS):
            raise ManagementError("platform_capacity_exhausted", details={"needed": needed, "available": available})

    async def create(self, *, principal: AuthContext, idempotency_key: str, prepared: RenderedApplication,
                     release: ApplicationReleaseV1, shared: SharedDevelopmentBindingV1) -> ApplicationOperationV1:
        owner, team = owner_identity(principal, mutation=True)
        replay = await self.replay_create(principal=principal, idempotency_key=idempotency_key,
                                          slug=prepared.registration.slug, release_id=release.release_id)
        if replay is not None:
            return replay
        plan = freeze_plan(prepared, release, shared)
        row = prepared.registration
        if (row.owner_user_id, row.owner_team_id) != (owner, team):
            raise ManagementError("application_owner_mismatch", 403)
        if row.deployment_generation != 1 or row.access_generation != 1:
            raise ManagementError("invalid_application_plan", 422)
        fingerprint = _fingerprint(action="create", team=team, slug=row.slug, release_id=release.release_id)
        try:
            async with self.session_factory.begin() as session:
                # Budget -> application -> operation is the global mutation order.
                budget = await self._lock_budget(session, row.cluster_id)
                replay = await self._replay(session, owner, idempotency_key, fingerprint)
                if replay is not None:
                    return replay
                await self._capacity(session, budget, plan["platform_envelope"])
                session.add(NebiusApplication(**row.model_dump(exclude={"schema_version"})))
                await session.flush()
                session.add(NebiusApplicationReservation(application_id=row.application_id, cluster_id=row.cluster_id,
                                                        **plan["platform_envelope"]))
                operation = NebiusApplicationOperation(
                    operation_id=uuid4(), application_id=row.application_id, owner_user_id=owner,
                    idempotency_key=idempotency_key, request_sha256=fingerprint,
                    deployment_generation=1, access_generation=1, action="create", phase="pending", plan_json=plan,
                )
                session.add(operation)
                await session.flush()
                return operation_view(operation)
        except IntegrityError:
            # Cross-cluster duplicate requests can race on the global owner key.
            replay = await self.replay_create(principal=principal, idempotency_key=idempotency_key,
                                              slug=row.slug, release_id=release.release_id)
            if replay is not None:
                return replay
            raise ManagementError("application_name_conflict") from None

    async def get_operation(self, operation_id: UUID, *, principal: AuthContext) -> ApplicationOperationV1:
        owner, team = owner_identity(principal)
        async with self.session_factory() as session:
            row = await session.scalar(select(NebiusApplicationOperation).join(
                NebiusApplication, NebiusApplication.application_id == NebiusApplicationOperation.application_id,
            ).where(NebiusApplicationOperation.operation_id == operation_id,
                    NebiusApplication.owner_user_id == owner, NebiusApplication.owner_team_id == team))
            if row is None:
                raise ManagementError("application_forbidden", 403)
            return operation_view(row)

    async def list_applications(self, *, principal: AuthContext) -> list[ApplicationRegistrationV1]:
        owner, team = owner_identity(principal)
        async with self.session_factory() as session:
            rows = await session.scalars(select(NebiusApplication).where(
                NebiusApplication.owner_user_id == owner, NebiusApplication.owner_team_id == team,
                NebiusApplication.purged_at.is_(None),
            ).order_by(NebiusApplication.created_at, NebiusApplication.application_id).limit(1000))
            return [registration_view(row) for row in rows]

    async def transition(
        self, application_id: UUID, *, principal: AuthContext, idempotency_key: str,
        action: str, expected_generation: int, release_id: UUID | None = None,
        prepared: RenderedApplication | None = None, release: ApplicationReleaseV1 | None = None,
        shared: SharedDevelopmentBindingV1 | None = None,
    ) -> ApplicationOperationV1:
        owner, team = owner_identity(principal, mutation=True)
        _key(idempotency_key)
        if (action not in {"update", "suspend", "resume", "destroy_retained"}
                or type(expected_generation) is not int or expected_generation < 1
                or (action == "update") != (release_id is not None)):
            raise ManagementError("invalid_application_transition", 422)
        fingerprint = _fingerprint(action=action, team=team, application_id=application_id,
                                   expected_generation=expected_generation, release_id=release_id)
        try:
            async with self.session_factory.begin() as session:
                cluster_id = await session.scalar(select(NebiusApplication.cluster_id).where(
                    NebiusApplication.application_id == application_id,
                    NebiusApplication.owner_user_id == owner, NebiusApplication.owner_team_id == team,
                ))
                if cluster_id is None:
                    raise ManagementError("application_forbidden", 403)
                budget = await self._lock_budget(session, cluster_id)
                row = (await session.scalars(select(NebiusApplication).where(
                    NebiusApplication.application_id == application_id,
                    NebiusApplication.owner_user_id == owner, NebiusApplication.owner_team_id == team,
                ).with_for_update())).one_or_none()
                if row is None:
                    raise ManagementError("application_forbidden", 403)
                replay = await self._replay(session, owner, idempotency_key, fingerprint)
                if replay is not None:
                    return replay
                if row.deployment_generation != expected_generation:
                    raise ManagementError("application_generation_conflict")
                supported = ((action == "destroy_retained" and row.desired_state in {"active", "suspended"})
                             or (action in {"update", "suspend"} and row.desired_state == "active")
                             or (action == "resume" and row.desired_state == "suspended"))
                if not supported or row.purged_at is not None:
                    raise ManagementError("application_transition_not_supported")
                source = (await session.scalars(select(NebiusApplicationOperation).where(
                    NebiusApplicationOperation.application_id == application_id,
                    NebiusApplicationOperation.deployment_generation == expected_generation,
                ).with_for_update())).one_or_none()
                if source is None or not self._current(source, row):
                    raise ManagementError("application_operation_missing")
                active = action in {"update", "resume"}
                if active and source.phase != "completed":
                    raise ManagementError("application_transition_not_ready")
                desired = "active" if active else "suspended" if action == "suspend" else "destroyed"
                next_row = registration_view(row).model_copy(update={
                    "deployment_generation": row.deployment_generation + 1,
                    "access_generation": row.access_generation + 1, "desired_state": desired,
                    "release_id": release_id if action == "update" else row.release_id,
                })
                if active:
                    if prepared is None or release is None or shared is None:
                        raise ManagementError("invalid_application_plan", 422)
                    plan = freeze_plan(prepared, release, shared)
                    if prepared.registration != next_row or any(
                        plan["shared"][key] != source.plan_json["shared"][key]
                        for key in ("data_environment_id", "cluster_id", "platform_namespace")
                    ):
                        raise ManagementError("invalid_application_plan", 422)
                    reservation = await session.get(NebiusApplicationReservation, application_id)
                    if reservation is None:
                        raise ManagementError("platform_reservation_missing")
                    increment = {key: max(0, plan["platform_envelope"][key] - getattr(reservation, key))
                                 for key in ENVELOPE_FIELDS}
                    await self._capacity(session, budget, increment)
                    for key in ENVELOPE_FIELDS:
                        setattr(reservation, key, getattr(reservation, key) + increment[key])
                else:
                    if any(value is not None for value in (prepared, release, shared)):
                        raise ManagementError("invalid_application_plan", 422)
                    plan = copy.deepcopy(source.plan_json)
                plan["registration"] = next_row.model_dump(mode="json")
                plan["source_operation_id"] = str(source.operation_id)
                plan["requires_previous_retirement"] = True
                source.phase, source.error_code = "superseded", "application_transition_requested"
                source.lease_token = source.lease_expires_at = None
                for key in ("deployment_generation", "access_generation", "desired_state", "release_id"):
                    setattr(row, key, getattr(next_row, key))
                operation = NebiusApplicationOperation(
                    operation_id=uuid4(), application_id=application_id, owner_user_id=owner,
                    idempotency_key=idempotency_key, request_sha256=fingerprint,
                    deployment_generation=row.deployment_generation, access_generation=row.access_generation,
                    action=action, phase="pending", plan_json=plan,
                )
                session.add(operation)
                await session.flush()
                return operation_view(operation)
        except IntegrityError:
            raise ManagementError("idempotency_conflict") from None

    async def retry(self, operation_id: UUID, *, principal: AuthContext) -> ApplicationOperationV1:
        owner, team = owner_identity(principal, mutation=True)
        await self.get_operation(operation_id, principal=principal)
        async with self.session_factory.begin() as session:
            operation, row, _ = await self._locked_operation(session, operation_id)
            if (row.owner_user_id, row.owner_team_id) != (owner, team):
                raise ManagementError("application_forbidden", 403)
            if not self._current(operation, row):
                raise ManagementError("stale_operation_generation")
            if operation.phase == "blocked":
                operation.phase, operation.error_code = "pending", None
            return operation_view(operation)
