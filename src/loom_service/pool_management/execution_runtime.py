"""Participant-only execution identity from the retained activated plan."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import NebiusPoolEffect
from loom.nebius_pool_contract import PoolNamespaceBindingV1, PoolRequestActionV1
from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import PoolControlError, _locked_request
from loom_service.pool_management.gateway_journal import _view
from loom_service.pool_management.registry import _receipt


async def execution_runtime(session: AsyncSession, principal: PoolPrincipal,
                            action: PoolRequestActionV1) -> PoolExecutionRuntimeV1:
    async with _locked_request(session, principal, action) as (row, _):
        if (row is None or row.workload_kind not in {"trial", "verifier"} or row.plan_json is None
                or digest(row.plan_json) != row.plan_sha256):
            raise PoolControlError
        request = PoolExecutionPrepareV1.model_validate(row.request_json)
        plan = row.plan_json
        job = plan["job"]
        labels, annotations = job["metadata"]["labels"], job["metadata"]["annotations"]
        if (plan["request_sha256"] != row.request_sha256 or plan["configmap"] is not None
                or job["apiVersion"] != "batch/v1" or job["kind"] != "Job"
                or labels["loom.openai.com/lease-id"] != str(row.local_work_id)
                or labels["loom.openai.com/generation"] != str(row.generation)
                or annotations["loom.openai.com/target-id"] != row.target_id
                or annotations["loom.openai.com/execution-unit-key"] != str(request.execution.execution_unit_key)
                or annotations["loom.openai.com/execution-role"] != request.execution.runtime.execution_role):
            raise PoolControlError
        effect_id = None
        if row.job_uid is not None:
            effect = await session.scalar(select(NebiusPoolEffect).where(
                NebiusPoolEffect.request_id == row.request_id, NebiusPoolEffect.effect_key == "create:job"))
            if effect is None or effect.phase != "observed" or effect.observed_uid != row.job_uid:
                raise PoolControlError
            _view(effect, row)  # Retained effect must bind this exact plan/namespace/document.
            effect_id = effect.effect_id
        return PoolExecutionRuntimeV1(receipt=_receipt(row), target_id=row.target_id,
            namespace=PoolNamespaceBindingV1(name=job["metadata"]["namespace"], uid=row.namespace_uid),
            job_name=job["metadata"]["name"], resource_generation=row.generation,
            lease_generation=request.execution.lease_generation, execution_unit_key=request.execution.execution_unit_key,
            deadline_at=row.deadline_at, job_effect_id=effect_id)
