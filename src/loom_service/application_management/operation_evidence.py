"""Read-only, owner-scoped journal counts without live provider calls or material."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.auth import AuthContext
from loom.db.nebius_application_cloud_schema import NebiusApplicationCloudEffect
from loom.db.nebius_application_effect_schema import NebiusApplicationEffect
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.db.nebius_application_schema import NebiusApplication
from loom.nebius_application_contract import ApplicationOperationV1
from loom.nebius_application_evidence import ApplicationOperationEvidenceV1
from loom_service.environment_management.registry import ManagementError, owner_identity


async def _counts(session: AsyncSession, model: type[NebiusApplicationEffect] | type[NebiusApplicationCloudEffect],
                  operation_id: UUID, limit: int) -> list[dict[str, Any]]:
    kind, action = model.intent_json["kind"].astext, model.intent_json["action"].astext
    result = await session.execute(select(kind.label("kind"), action.label("action"), model.phase,
        func.count().label("count")).where(model.operation_id == operation_id)
        .group_by(kind, action, model.phase).order_by(kind, action, model.phase).limit(limit + 1))
    # Only these selected scalars enter the public contract. Never load raw intent,
    # target identities, credentials, dispatch tokens or complete frozen plans.
    return [dict(row) for row in result.mappings()]


async def read_operation_evidence(factory: async_sessionmaker[AsyncSession], operation_id: UUID, *,
                                  principal: AuthContext) -> ApplicationOperationEvidenceV1:
    owner, team = owner_identity(principal)
    operation = NebiusApplicationOperation
    fields = tuple(name for name in ApplicationOperationV1.model_fields if name != "schema_version")
    async with factory.begin() as session:
        # A single MVCC snapshot prevents counts and lease/state from describing
        # different generations of the worker's journal. SQL cannot mutate it.
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        row = (await session.execute(select(*(getattr(operation, name) for name in fields),
            operation.runner_epoch,
            and_(operation.phase == "running", operation.lease_expires_at > func.current_timestamp()).label("lease_active"),
            operation.completion_json.is_not(None).label("completion_recorded"))
            .join(NebiusApplication, NebiusApplication.application_id == operation.application_id)
            .where(operation.operation_id == operation_id, NebiusApplication.owner_user_id == owner,
                   NebiusApplication.owner_team_id == team))).mappings().one_or_none()
        if row is None:
            raise ManagementError("application_forbidden", 403)
        payload = {"operation": {name: row[name] for name in fields}, "runner_epoch": row["runner_epoch"],
            "lease_active": row["lease_active"], "completion_recorded": row["completion_recorded"],
            "kubernetes": await _counts(session, NebiusApplicationEffect, operation_id, 128),
            "cloud": await _counts(session, NebiusApplicationCloudEffect, operation_id, 32)}
        try:
            return ApplicationOperationEvidenceV1.model_validate(payload)
        except ValidationError:
            raise ManagementError("application_evidence_unqualified", 503) from None
