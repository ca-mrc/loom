"""Trusted gateway cleanup snapshots; never expose caller-supplied absence facts.

Prepare and finalize own their transactions. Kubernetes reads happen between
them. Only the fixed provider calls finalize after a complete qualified scan.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_pool_schema import (
    NebiusPoolCleanupObservation,
    NebiusPoolEffect,
    NebiusPoolRequest,
)
from loom.nebius_pool_contract import PoolNamespaceBindingV1, PoolReceiptV1, PoolRequestActionV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import _clock
from loom_service.pool_management.gateway_journal import (
    PoolGatewayEffect,
    PoolGatewayError,
    PoolGatewayJournal,
    _view,
)
from loom_service.pool_management.lifecycle import _qualified
from loom_service.pool_management.registry import _receipt


@dataclass(frozen=True)
class PoolCleanupSnapshot:
    receipt: PoolReceiptV1
    namespace: PoolNamespaceBindingV1
    job_name: str
    has_configmap: bool
    job: PoolGatewayEffect | None
    fingerprint: str
    captured_at: datetime


class PoolCleanupJournal:
    def __init__(self, journal: PoolGatewayJournal) -> None:
        self.journal = journal

    async def _snapshot(self, session: AsyncSession, row: NebiusPoolRequest) -> PoolCleanupSnapshot:
        if row.phase != "cleanup_intent" or row.stop_json is None or row.drain_json is None or row.plan_json is None:
            raise PoolGatewayError("pool_cleanup_output_not_drained")
        stop = PoolStopV1.model_validate(row.stop_json["request"])
        drain = PoolDrainV1.model_validate(row.drain_json)
        _qualified(row, stop)
        _qualified(row, drain)
        receipt = _receipt(row)
        action = PoolRequestActionV1(pool_id=row.pool_id, request_key=receipt.request_key,
            admission_epoch=row.admission_epoch, request_sha256=row.request_sha256)
        if (stop.action != action or drain.action != action or row.stop_json["request_sha256"] != digest(stop.model_dump(mode="json"))
                or drain.stop_sha256 != row.stop_json["request_sha256"]
                or (row.workload_kind in {"task_image_build", "application_image_build"}
                    and drain.output_generation != drain.lease_generation)):
            raise PoolGatewayError
        effects = list((await session.scalars(select(NebiusPoolEffect).where(
            NebiusPoolEffect.request_id == row.request_id).order_by(NebiusPoolEffect.sequence))).all())
        job = None
        for effect in effects:
            if effect.intent_json["action"] == "create":
                if (effect.effect_key not in {"create:job", "create:configmap"}
                        or effect.phase not in {"prepared", "observed", "rejected"}):
                    raise PoolGatewayError("pool_cleanup_create_unconfirmed")
                created = _view(effect, row)
                if created.document["kind"] == "Job":
                    if created.observed_uid != row.job_uid:
                        raise PoolGatewayError
                    if created.phase == "observed":
                        job = created
        if row.job_uid is not None and job is None:
            raise PoolGatewayError
        metadata = row.plan_json["job"]["metadata"]
        if metadata["name"] != "loom-pool-" + row.request_id.hex:
            raise PoolGatewayError
        fingerprint = digest({"receipt": receipt.model_dump(mode="json"), "stop": row.stop_json,
            "drain": row.drain_json, "effects": [{
                "id": str(effect.effect_id), "sequence": effect.sequence, "key": effect.effect_key,
                "phase": effect.phase, "intent": effect.intent_json,
                "dispatch_id": str(effect.dispatch_id) if effect.dispatch_id is not None else None,
                "dispatch_machine_id": str(effect.dispatch_machine_id) if effect.dispatch_machine_id is not None else None,
                "dispatch_epoch": effect.dispatch_epoch,
                "observed_uid": str(effect.observed_uid) if effect.observed_uid is not None else None,
                "observed_resource_version": effect.observed_resource_version,
                "rejection_status": effect.rejection_status} for effect in effects]})
        return PoolCleanupSnapshot(receipt, PoolNamespaceBindingV1(name=metadata["namespace"], uid=row.namespace_uid),
            metadata["name"], row.plan_json["configmap"] is not None, job, fingerprint, await _clock(session))

    async def prepare(self, principal: PoolPrincipal, reservation_id: UUID) -> PoolCleanupSnapshot | PoolReceiptV1:
        async with self.journal._transaction(principal) as (session, pool):
            row = await self.journal._request(session, pool, reservation_id)
            if row.phase == "released":
                return _receipt(row)
            return await self._snapshot(session, row)

    async def finalize(self, principal: PoolPrincipal, snapshot: PoolCleanupSnapshot, *, pod_list_resource_version: str) -> PoolReceiptV1:
        """Internal provider-only completion, after its qualified absence scan."""
        if not isinstance(pod_list_resource_version, str) or re.fullmatch(r"[A-Za-z0-9._:-]{1,253}", pod_list_resource_version) is None:
            raise PoolGatewayError
        async with self.journal._transaction(principal) as (session, pool):
            row = await self.journal._request(session, pool, snapshot.receipt.reservation_id)
            if row.phase == "released":
                return _receipt(row)
            current = await self._snapshot(session, row)
            if (replace(current, captured_at=snapshot.captured_at) != snapshot
                    or not 0 <= (current.captured_at - snapshot.captured_at).total_seconds() <= 60):
                raise PoolGatewayError("pool_cleanup_snapshot_changed_or_expired")
            assert row.plan_sha256 is not None
            identity = uuid4()
            session.add(NebiusPoolCleanupObservation(observation_id=identity, request_id=row.request_id,
                plan_sha256=row.plan_sha256, namespace_uid=row.namespace_uid,
                writer_epoch=principal.credential_epoch, observed_at=current.captured_at, evidence_json={
                    "schema_version": "loom.pool-cleanup-evidence.v1", "gateway_machine_id": str(principal.machine_id),
                    "snapshot_sha256": current.fingerprint, "observation_window_start": snapshot.captured_at.isoformat(),
                    "namespace": current.namespace.model_dump(mode="json"), "job_name": current.job_name,
                    "job_uid": str(row.job_uid) if row.job_uid is not None else None,
                    "configmap_required": current.has_configmap, "pod_list_resource_version": pod_list_resource_version,
                    "stop_sha256": row.stop_json["request_sha256"] if row.stop_json is not None else None,
                    "drain_sha256": digest(row.drain_json)}))
            await session.flush()
            row.phase, row.cleanup_observation_id = "released", identity
            await session.flush()
            return _receipt(row)
