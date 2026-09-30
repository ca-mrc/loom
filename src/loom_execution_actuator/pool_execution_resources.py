"""Global resource authority for the existing execution controller.

Owns neither results nor usage. Local claims and fixed management effects replace
only provisioning/create/delete; Kubernetes access is qualified and read-only.
"""
from __future__ import annotations

from typing import Protocol

from loom.db.schema import ServiceExecutionLease
from loom.nebius_pool_contract import PoolNamespaceBindingV1, PoolRequestKeyV1
from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1
from loom_execution_actuator.contracts import KubernetesJobObservation
from loom_execution_actuator.pool_execution_driver import PoolExecutionDriver
from loom_execution_actuator.pool_execution_outbox import PoolExecutionHandoff
from loom_execution_actuator.pool_outbox import PoolHandoffError


class PoolExecutionReadApi(Protocol):
    async def probe_pool_namespace(self, namespace: PoolNamespaceBindingV1) -> None: ...

    async def observe_pool(self, runtime: PoolExecutionRuntimeV1) -> KubernetesJobObservation | None: ...


class PoolExecutionResources:
    def __init__(self, *, driver: PoolExecutionDriver, kubernetes: PoolExecutionReadApi, target_id: str) -> None:
        driver.outbox.participant.target(target_id, "trial")
        self.driver, self.kubernetes, self.target_id = driver, kubernetes, target_id
        self.namespace = driver.outbox.participant.execution_namespace

    def key(self, lease: ServiceExecutionLease) -> PoolRequestKeyV1:
        if (lease.target_id != self.target_id or lease.namespace_name != self.namespace.name
                or lease.execution_role != "attempt"):
            raise PoolHandoffError
        return PoolRequestKeyV1(participant_id=self.driver.outbox.participant.participant_id,
            workload_kind="trial", local_work_id=lease.id, generation=lease.resource_generation)

    async def probe(self) -> None:
        await self.kubernetes.probe_pool_namespace(self.namespace)

    async def advance(self, key: PoolRequestKeyV1) -> PoolExecutionHandoff:
        saved = await self.driver.outbox.get(key)
        if saved.request.target_id != self.target_id:
            raise PoolHandoffError
        saved = await self.driver.advance(key)
        if saved.phase == "stop_pending":
            await self.driver.outbox.revoke_stopped(key)
        return saved

    async def observe(self, lease: ServiceExecutionLease) -> KubernetesJobObservation | None:
        key = self.key(lease)
        saved = await self.driver.outbox.get(key)
        if saved.phase in {"cancelled", "released"}:
            return None
        if saved.activated is None or saved.lease_id != lease.id:
            raise PoolHandoffError
        runtime = await self.driver.management.execution_runtime(saved.action)
        if (runtime.receipt.reservation_id != saved.reservation_id
                or runtime.receipt.plan_sha256 != saved.activated.plan_sha256
                or (saved.activated.job_uid is not None and runtime.receipt.job_uid != saved.activated.job_uid)
                or runtime.target_id != self.target_id or runtime.namespace != self.namespace
                or runtime.job_name != lease.job_name or runtime.execution_unit_key != lease.execution_unit_key
                or runtime.resource_generation != lease.resource_generation
                or runtime.lease_generation != saved.request.execution.lease_generation
                or runtime.deadline_at != lease.deadline_at
                or (lease.job_uid is not None and str(runtime.receipt.job_uid) != lease.job_uid)):
            raise PoolHandoffError
        if runtime.receipt.phase == "released":
            await self.driver.outbox.confirm_release(key, runtime.receipt)
            return None
        if runtime.receipt.job_uid is None:
            return None
        return await self.kubernetes.observe_pool(runtime)

    async def create(self, lease: ServiceExecutionLease) -> KubernetesJobObservation | None:
        await self.advance(self.key(lease))
        return await self.observe(lease)

    async def stop(self, lease: ServiceExecutionLease) -> None:
        await self.driver.stop_and_drain(self.key(lease))

    async def drain(self, lease: ServiceExecutionLease) -> None:
        drain = await self.driver.outbox.begin_drain(self.key(lease))
        if drain is not None:
            await self.driver.management.drain(drain)
