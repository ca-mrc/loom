"""Owner-scoped immutable version projection; no catalog or provider readback."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.auth import AuthContext
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.nebius_application_schema import NebiusApplication
from loom.nebius_application_contract import (
    ApplicationOperationV1,
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
)
from loom.nebius_application_versions import ApplicationVersionsV1
from loom_service.environment_management.registry import ManagementError, owner_identity


def _release(record: Mapping[str, Any], current: ApplicationRegistrationV1) -> ApplicationReleaseV1:
    frozen = ApplicationRegistrationV1.model_validate(record["registration"])
    release = ApplicationReleaseV1.model_validate(record["release"])
    stable = ("application_id", "incarnation", "owner_user_id", "owner_team_id", "data_environment_id",
              "cluster_id", "slug", "application_namespace", "public_host")
    if (any(getattr(frozen, key) != getattr(current, key) for key in stable)
            or frozen.release_id != release.release_id
            or frozen.deployment_generation != record["deployment_generation"]
            or frozen.access_generation != record["access_generation"]
            or release.schema_revision != record["shared_schema_revision"]):
        raise ValueError("inconsistent frozen application version")
    return release


async def read_application_versions(factory: async_sessionmaker[AsyncSession], application_id: UUID, *,
                                    principal: AuthContext, shared_schema_revision: str) -> ApplicationVersionsV1:
    owner, team = owner_identity(principal)
    app, op = NebiusApplication, NebiusApplicationOperation
    registration_fields = tuple(name for name in ApplicationRegistrationV1.model_fields if name != "schema_version")
    operation_fields = tuple(name for name in ApplicationOperationV1.model_fields if name != "schema_version")
    # Select only the public release/registration subset, never manifests,
    # runtime profiles, credentials, lease tokens or full completion proofs.
    version_fields = (
        *(getattr(op, name) for name in operation_fields), op.completed_at,
        op.plan_json["release"].label("release"), op.plan_json["registration"].label("registration"),
        op.plan_json["shared"]["schema_revision"].astext.label("shared_schema_revision"),
    )
    async with factory.begin() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        row = (await session.execute(select(*(getattr(app, name) for name in registration_fields)).where(
            app.application_id == application_id, app.owner_user_id == owner, app.owner_team_id == team,
            app.purged_at.is_(None)))).mappings().one_or_none()
        if row is None:
            raise ManagementError("application_forbidden", 403)
        try:
            registration = ApplicationRegistrationV1.model_validate(dict(row))
            requested = (await session.execute(select(*version_fields).where(op.application_id == application_id,
                op.deployment_generation == registration.deployment_generation,
                op.access_generation == registration.access_generation))).mappings().one_or_none()
            if requested is None or requested["registration"] != registration.model_dump(mode="json"):
                raise ValueError("application version journal missing or inconsistent")
            requested_release = _release(dict(requested), registration)
            previous = (await session.execute(select(*version_fields).where(op.application_id == application_id,
                op.action.in_(("create", "update", "resume")), op.phase.in_(("completed", "superseded")),
                op.completion_json.is_not(None), op.completed_at.is_not(None),
                op.deployment_generation <= registration.deployment_generation)
                .order_by(op.deployment_generation.desc()).limit(1))).mappings().one_or_none()
            return ApplicationVersionsV1.model_validate({
                "status": {"registration": registration,
                           "operation": {name: requested[name] for name in operation_fields}},
                "requested_release": requested_release,
                "last_completed_deployment": None if previous is None else {
                    "operation_id": previous["operation_id"], "deployment_generation": previous["deployment_generation"],
                    "completed_at": previous["completed_at"], "release": _release(dict(previous), registration)},
                "shared_schema_revision": shared_schema_revision,
                "schema_compatibility": "compatible" if requested_release.schema_revision == shared_schema_revision else "schema_mismatch",
            })
        except (ValueError, TypeError, KeyError):
            raise ManagementError("application_versions_unqualified", 503) from None
