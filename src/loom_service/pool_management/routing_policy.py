"""Independent management-owned preference through protected administration."""
from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolBinding
from loom_service.pool_management.lifecycle_journal import replay, retain_operation


class PoolRoutingPreferenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.pool-routing-preference.v1"] = "loom.pool-routing-preference.v1"
    operation_id: UUID
    installation_id: UUID
    pool_id: UUID
    # None is an inspect-only request. A participant token cannot update this.
    prefer_incoming_warm_fit: bool | None = Field(default=None, strict=True)

    @model_validator(mode="after")
    def identity(self) -> PoolRoutingPreferenceV1:
        if not all(value.int for value in (self.operation_id, self.installation_id, self.pool_id)):
            raise ValueError("pool_routing_identity_unavailable")
        return self


async def routing_preference(session: AsyncSession, request: PoolRoutingPreferenceV1, *, apply: bool) -> dict[str, Any]:
    document = request.model_dump(mode="json")
    previous = await replay(session, request.operation_id, document) if apply else None
    pool = await session.get(NebiusPoolBinding, request.pool_id, populate_existing=True)
    if pool is None or pool.installation_id != request.installation_id:
        raise ValueError("pool_routing_scope_unavailable")
    await session.refresh(pool, attribute_names=["prefer_incoming_warm_fit"])
    if apply and request.prefer_incoming_warm_fit is None:
        raise ValueError("pool_routing_inspection_is_read_only")
    if apply and previous is None:
        assert request.prefer_incoming_warm_fit is not None
        pool.prefer_incoming_warm_fit = request.prefer_incoming_warm_fit
        await session.flush()
    current = dict(pool_id=str(pool.pool_id), installation_id=str(pool.installation_id),
        prefer_incoming_warm_fit=pool.prefer_incoming_warm_fit, policy_source="management-db",
        admission_epoch=pool.admission_epoch, policy_revision=pool.policy_revision)
    receipt = previous or dict(schema_version="loom.pool-routing-preference-receipt.v1",
        operation_id=str(request.operation_id), **current)
    if apply and previous is None:
        await retain_operation(session, document, receipt, pool_id=pool.pool_id, participant_id=None)
    return dict(receipt=receipt, current=current)
