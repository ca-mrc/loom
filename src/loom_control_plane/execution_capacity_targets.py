"""Explicit immutable aliases of one physical capacity owner.

Target health, leases, prices and operator intent remain independent. This
module resolves only the physical inventory/policy family; it grants no create
or cleanup authority and never infers sharing from overlapping observations.
"""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ServiceExecutionTarget
from loom.execution_contract import (
    ExecutionTargetV1,
    nebius_cpu_execution_class,
    nebius_guest_class_by_id,
)
from loom_execution_capacity_collector.contracts import CapacityTargetScopeV1


def validate_capacity_owner(alias: ExecutionTargetV1, owner: ExecutionTargetV1) -> None:
    """Only an exact guest sibling may consume the ordinary target's capacity."""
    guest_class = nebius_guest_class_by_id(alias.execution_class_id)
    web = guest_class.supports_task_web_egress if guest_class is not None else False
    if (alias.capacity_owner_target_id != owner.target_id or owner.capacity_owner_target_id is not None
            or guest_class is None
            or owner.execution_class_id != nebius_cpu_execution_class(supports_task_web_egress=web).class_id
            or alias.cluster_scope_id is None
            or any(getattr(alias, key) != getattr(owner, key) for key in (
                "logical_pool_id", "environment", "provider", "region", "failure_domain",
                "data_residency", "cluster_scope_id", "namespace_name",
            ))):
        raise ValueError("guest capacity owner must be an ordinary target in the exact physical and environment scope")


@dataclass(frozen=True)
class CapacityTargetGroup:
    owner: ServiceExecutionTarget
    members: tuple[ServiceExecutionTarget, ...]

    @property
    def target_ids(self) -> frozenset[str]:
        return frozenset(row.id for row in self.members)

    @property
    def scope(self) -> CapacityTargetScopeV1 | None:
        if len(self.members) == 1:
            return None
        return CapacityTargetScopeV1(
            owner_target_id=self.owner.id,
            namespace_name=ExecutionTargetV1.model_validate(self.owner.spec_json).namespace_name,
            target_ids=sorted(self.target_ids),
        )

    def matches_observation_scope(self, payload: dict[str, Any]) -> bool:
        placement = payload.get("placement") or {}
        scope = self.scope
        return bool(placement.get("target_scope") == (scope.model_dump(mode="json") if scope else None))


async def resolve_capacity_targets(session: AsyncSession, target_id: str) -> CapacityTargetGroup:
    target = await session.get(ServiceExecutionTarget, target_id)
    if target is None:
        raise ValueError("capacity target is unavailable")
    spec = ExecutionTargetV1.model_validate(target.spec_json)
    owner = target if spec.capacity_owner_target_id is None else await session.get(
        ServiceExecutionTarget, spec.capacity_owner_target_id,
    )
    if owner is None:
        raise ValueError("capacity owner is unavailable")
    owner_spec = ExecutionTargetV1.model_validate(owner.spec_json)
    if owner_spec.capacity_owner_target_id is not None:
        raise ValueError("capacity owner chains are forbidden")
    if target.id != owner.id:
        validate_capacity_owner(spec, owner_spec)
    members = tuple((await session.scalars(select(ServiceExecutionTarget).where(or_(
        ServiceExecutionTarget.id == owner.id,
        ServiceExecutionTarget.spec_json["capacity_owner_target_id"].as_string() == owner.id,
    )).order_by(ServiceExecutionTarget.id))).all())
    for member in members:
        if member.id != owner.id:
            validate_capacity_owner(ExecutionTargetV1.model_validate(member.spec_json), owner_spec)
    return CapacityTargetGroup(owner=owner, members=members)
