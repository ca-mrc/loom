from __future__ import annotations

import logging
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import ServiceExecutionLease
from loom_control_plane.execution_capacity import (
    ExecutionProvisioningBlockedError,
    reserve_execution_provisioning,
)
from loom_control_plane.execution_capacity_targets import resolve_capacity_targets
from loom_control_plane.service_execution import (
    ClaimedExecutionCommand,
    ServiceExecutionConflict,
    acknowledge_execution_command,
    claim_execution_commands,
    defer_execution_command,
    finalize_committed_service_execution,
    finalize_failed_service_execution,
    mark_execution_output_unavailable,
    record_kubernetes_observation,
    refresh_execution_target_health,
    retry_unscheduled_execution_after_deadline,
)
from loom_execution_actuator.contracts import (
    ActuatorContractError,
    KubernetesApiError,
    KubernetesJobApi,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.metrics import (
    KUBERNETES_API_ERRORS_TOTAL,
    KUBERNETES_API_SECONDS,
    KUBERNETES_CLEANUP_RETRIES_TOTAL,
    KUBERNETES_ORPHAN_COUNT,
    KUBERNETES_PENDING_TOTAL,
    KUBERNETES_RECONCILE_CONVERGED,
    KUBERNETES_WATCH_RESTARTS_TOTAL,
    RESOURCE_USAGE_ERRORS_TOTAL,
)
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from loom_execution_actuator.resource_usage import ResourceSample, persist_native_usage, pod_samples

_LOG = logging.getLogger(__name__)

_MANAGED_SELECTOR = "app.kubernetes.io/managed-by=loom-execution-actuator"
_FAILURE_REASONS = frozenset(
    {
        NormalizedJobState.UNSCHEDULABLE,
        NormalizedJobState.IMAGE_PULL_BACKOFF,
        NormalizedJobState.FAILED,
        NormalizedJobState.OOM_KILLED,
        NormalizedJobState.EVICTED,
        NormalizedJobState.NODE_LOST,
        NormalizedJobState.DEADLINE_EXCEEDED,
    }
)
_DELETE_COMMANDS = frozenset({"cancel", "timeout", "retry", "delete"})
_CLEANUP_DESIRED_STATES = frozenset({"cancel", "timeout", "retry", "delete_pending"})
_COMMITTED_RESULT_TERMINAL_STATES = frozenset(
    {
        NormalizedJobState.SUCCEEDED,
        NormalizedJobState.FAILED,
        NormalizedJobState.OOM_KILLED,
        NormalizedJobState.EVICTED,
        NormalizedJobState.NODE_LOST,
        NormalizedJobState.DEADLINE_EXCEEDED,
    }
)


class _ExecutionOutputPendingError(ActuatorContractError):
    pass


class ExecutionActuator:
    def __init__(
        self,
        *,
        sessions: async_sessionmaker[AsyncSession],
        kubernetes: KubernetesJobApi,
        target: ExecutionTargetRuntime,
        controller_id: str,
        command_limit: int = 20,
        command_lease_seconds: int = 60,
        delete_grace_seconds: int = 30,
    ) -> None:
        if not controller_id or len(controller_id) > 120:
            raise ValueError("invalid controller_id")
        self._sessions = sessions
        self._kubernetes = kubernetes
        self._target = target
        self._controller_id = controller_id
        self._command_limit = command_limit
        self._command_lease_seconds = command_lease_seconds
        self._delete_grace_seconds = delete_grace_seconds
        self._watch_resource_version: str | None = None
        self._usage_cache: dict[str, tuple[datetime, dict[str, Any] | None]] = {}

    async def run_commands_once(self, *, now: datetime | None = None) -> int:
        current_time = now or datetime.now(UTC)
        async with self._sessions() as session:
            commands = await claim_execution_commands(
                session,
                consumer_id=self._controller_id,
                target_id=self._target.target_id,
                limit=self._command_limit,
                lease_seconds=self._command_lease_seconds,
                now=current_time,
            )
            await session.commit()
        for command in commands:
            await self._process_command(command, now=current_time)
        return len(commands)

    async def _lease(self, lease_id: UUID) -> ServiceExecutionLease:
        async with self._sessions() as session:
            lease = await session.get(ServiceExecutionLease, lease_id)
            if lease is None:
                raise ActuatorContractError("execution lease no longer exists")
            session.expunge(lease)
            return lease

    def _validate_observation(
        self,
        lease: ServiceExecutionLease,
        observation: KubernetesJobObservation,
    ) -> None:
        expected = (
            lease.target_id == self._target.target_id
            and observation.namespace == self._target.namespace
            and observation.job_name == lease.job_name
            and observation.lease_id == str(lease.id)
            and observation.resource_generation == lease.resource_generation
            and observation.target_id == self._target.target_id
            and observation.execution_unit_key == str(lease.execution_unit_key)
        )
        if not expected:
            raise ActuatorContractError("Kubernetes resource scope does not match lease")
        if lease.job_uid is not None and observation.job_uid != lease.job_uid:
            raise ActuatorContractError("Kubernetes Job UID changed")
        if lease.pod_uid is not None and observation.pod_uid not in {None, lease.pod_uid}:
            raise ActuatorContractError("Kubernetes Pod UID changed")
        summary = observation.termination_summary
        if summary is not None and summary.output_committed:
            if (
                lease.output_commit_state != "committed"
                or summary.output_upload_session_id != lease.output_upload_session_id
                or summary.output_manifest_sha256 != lease.output_manifest_sha256
                or summary.output_marker_sha256 != lease.output_marker_sha256
            ):
                raise ActuatorContractError(
                    "Kubernetes termination output does not match durable commit"
                )

    async def _usage_samples(
        self, observation: KubernetesJobObservation, *, now: datetime
    ) -> tuple[dict[str, ResourceSample], str | None]:
        reader = getattr(self._kubernetes, "resource_summary", None)
        node = observation.node_name
        if reader is None or node is None or observation.pod_uid is None:
            return {}, "kubelet_sample_unavailable"
        cached = self._usage_cache.get(node)
        if cached is None or (now - cached[0]).total_seconds() >= 15:
            try:
                summary = await reader(node_name=node)
            except Exception:
                RESOURCE_USAGE_ERRORS_TOTAL.labels(operation="collect").inc()
                _LOG.warning(
                    "Native resource sample unavailable for lease %s", observation.lease_id
                )
                summary = None
            # Bound retention to active nodes; no durable raw node or foreign Pod data.
            self._usage_cache = {
                key: item
                for key, item in self._usage_cache.items()
                if (now - item[0]).total_seconds() < 60
            }
            self._usage_cache[node] = (now, summary)
        else:
            summary = cached[1]
        if summary is None:
            return {}, "kubelet_sample_unavailable"
        try:
            samples = pod_samples(
                summary, namespace=observation.namespace, pod_uid=observation.pod_uid
            )
        except (ValueError, TypeError, KeyError, AttributeError):
            RESOURCE_USAGE_ERRORS_TOTAL.labels(operation="parse").inc()
            _LOG.warning("Malformed native resource sample for lease %s", observation.lease_id)
            return {}, "kubelet_sample_malformed"
        return samples, None if samples else "pod_stats_unavailable"

    async def _persist_usage(
        self,
        session: AsyncSession,
        *,
        lease: ServiceExecutionLease,
        observation: KubernetesJobObservation,
        samples: dict[str, ResourceSample],
        now: datetime,
        terminal: bool,
        diagnostic: str | None,
    ) -> None:
        # Only telemetry is isolated. Primary observation and delete ownership
        # checks run outside this savepoint and must still fail closed.
        try:
            async with session.begin_nested():
                await persist_native_usage(
                    session,
                    lease=lease,
                    observation=observation,
                    samples=samples,
                    now=now,
                    terminal=terminal,
                    diagnostic=diagnostic,
                )
                await session.flush()
        except Exception:
            RESOURCE_USAGE_ERRORS_TOTAL.labels(operation="persist").inc()
            _LOG.warning("Native resource persistence failed for lease %s", lease.id)

    async def _persist_observation(
        self,
        lease: ServiceExecutionLease,
        observation: KubernetesJobObservation,
        *,
        now: datetime,
    ) -> None:
        self._validate_observation(lease, observation)
        if (
            observation.normalized_state == NormalizedJobState.DELETED
            and lease.desired_state in _CLEANUP_DESIRED_STATES
        ):
            await self._close_output_before_delete(
                lease,
                now=now,
                cancel_immediately=lease.desired_state == "cancel",
            )
        if observation.normalized_state in _FAILURE_REASONS:
            KUBERNETES_PENDING_TOTAL.labels(reason=observation.normalized_state.value).inc()
        samples, diagnostic = await self._usage_samples(observation, now=now)
        async with self._sessions() as session:
            await record_kubernetes_observation(
                session,
                lease_id=lease.id,
                generation=lease.generation,
                payload=observation.event_payload(),
                observed_at=now,
            )
            await self._persist_usage(
                session,
                lease=lease,
                observation=observation,
                samples=samples,
                now=now,
                terminal=observation.normalized_state in _COMMITTED_RESULT_TERMINAL_STATES
                or observation.normalized_state == NormalizedJobState.DELETED,
                diagnostic=diagnostic,
            )
            if observation.normalized_state == NormalizedJobState.UNSCHEDULABLE:
                await retry_unscheduled_execution_after_deadline(
                    session, lease_id=lease.id, generation=lease.generation, observed_at=now
                )
            if observation.normalized_state in _COMMITTED_RESULT_TERMINAL_STATES:
                committed = await finalize_committed_service_execution(
                    session,
                    lease_id=lease.id,
                    observed_at=now,
                )
                if not committed:
                    await finalize_failed_service_execution(
                        session,
                        lease_id=lease.id,
                        generation=lease.generation,
                        observed_at=now,
                    )
            await session.commit()

    async def _close_output_before_delete(
        self,
        lease: ServiceExecutionLease,
        *,
        now: datetime,
        cancel_immediately: bool = False,
    ) -> None:
        if lease.output_commit_state in {"committed", "unavailable"}:
            return
        if lease.cleanup_deadline_at is None or (
            now < lease.cleanup_deadline_at and not cancel_immediately
        ):
            raise _ExecutionOutputPendingError("durable output window remains open")
        async with self._sessions() as session:
            await mark_execution_output_unavailable(
                session,
                lease_id=lease.id,
                expected_generation=lease.generation,
                reason=("operator_cancelled" if cancel_immediately else "cleanup_deadline_elapsed"),
                now=now,
                allow_cancel_before_deadline=cancel_immediately,
            )
            await session.commit()

    async def _get(self, lease: ServiceExecutionLease) -> KubernetesJobObservation | None:
        with KUBERNETES_API_SECONDS.labels(operation="get").time():
            return await self._kubernetes.get_job(
                namespace=self._target.namespace,
                job_name=lease.job_name,
            )

    async def _create(
        self, lease: ServiceExecutionLease, *, now: datetime
    ) -> KubernetesJobObservation:
        existing = await self._get(lease)
        if existing is not None:
            self._validate_observation(lease, existing)
            return existing
        async with self._sessions() as session:
            await reserve_execution_provisioning(
                session, lease_id=lease.id, now=now, revalidate_existing=True
            )
            await session.commit()
        manifest = render_execution_job(lease, target=self._target, now=now)
        try:
            with KUBERNETES_API_SECONDS.labels(operation="create").time():
                created = await self._kubernetes.create_job(
                    namespace=self._target.namespace,
                    manifest=manifest,
                )
        except KubernetesApiError as exc:
            if not exc.ambiguous and exc.status_code != 409:
                raise
            # A timeout/409 may mean create committed before the response was
            # lost. Exact readback is the only safe resolution.
            recovered = await self._get(lease)
            if recovered is None:
                raise
            created = recovered
        self._validate_observation(lease, created)
        return created

    async def _delete(
        self,
        lease: ServiceExecutionLease,
        observation: KubernetesJobObservation,
        *,
        now: datetime,
        cancel_immediately: bool = False,
    ) -> None:
        self._validate_observation(lease, observation)
        if observation.job_uid is None:
            raise ActuatorContractError("cannot delete a Job without exact UID")
        samples, diagnostic = await self._usage_samples(observation, now=now)
        async with self._sessions() as session:
            # The delete path can be the first observer of a sandbox death.
            # Persist its UID-bound status before Kubernetes removes the Pod.
            await record_kubernetes_observation(
                session,
                lease_id=lease.id,
                generation=lease.generation,
                payload=observation.event_payload(),
                observed_at=now,
            )
            await self._persist_usage(
                session,
                lease=lease,
                observation=observation,
                samples=samples,
                now=now,
                terminal=True,
                diagnostic=diagnostic,
            )
            await session.commit()
        await self._close_output_before_delete(
            lease,
            now=now,
            cancel_immediately=cancel_immediately,
        )
        with KUBERNETES_API_SECONDS.labels(operation="delete").time():
            await self._kubernetes.delete_job(
                namespace=self._target.namespace,
                job_name=lease.job_name,
                expected_uid=observation.job_uid,
                grace_period_seconds=self._delete_grace_seconds,
            )

    async def _ack(
        self,
        command: ClaimedExecutionCommand,
        acknowledgement: dict[str, Any],
        *,
        now: datetime,
    ) -> None:
        async with self._sessions() as session:
            await acknowledge_execution_command(
                session,
                command_id=command.id,
                consumer_id=self._controller_id,
                acknowledgement=acknowledgement,
                now=now,
            )
            await session.commit()

    async def _defer(
        self,
        command: ClaimedExecutionCommand,
        exc: Exception,
        *,
        now: datetime,
    ) -> None:
        if isinstance(exc, _ExecutionOutputPendingError):
            delay = 5
            max_deliveries = 100
            code = "output_pending"
        elif isinstance(exc, ExecutionProvisioningBlockedError):
            delay = exc.retry_after_seconds
            max_deliveries = 100
            code = exc.reason
        elif isinstance(exc, KubernetesApiError):
            status_class = (
                f"{exc.status_code // 100}xx" if exc.status_code is not None else "transport"
            )
            KUBERNETES_API_ERRORS_TOTAL.labels(
                operation=command.command_type,
                status_class=status_class,
            ).inc()
            delay = exc.retry_after_seconds or min(300, 2 ** min(command.delivery_count, 8))
            max_deliveries = 20
            code = f"kubernetes_{status_class}"
        else:
            delay = 300
            max_deliveries = 1
            code = "contract_error"
        if command.command_type in _DELETE_COMMANDS:
            KUBERNETES_CLEANUP_RETRIES_TOTAL.labels(cause=code).inc()
        async with self._sessions() as session:
            await defer_execution_command(
                session,
                command_id=command.id,
                consumer_id=self._controller_id,
                error_code=code,
                error_message=str(exc),
                retry_after_seconds=delay,
                max_deliveries=max_deliveries,
                now=now,
            )
            await session.commit()

    async def _process_command(self, command: ClaimedExecutionCommand, *, now: datetime) -> None:
        try:
            lease = await self._lease(command.lease_id)
            if lease.target_id != self._target.target_id:
                raise ActuatorContractError("command targets another actuator")
            if lease.generation != command.generation:
                raise ActuatorContractError("command generation is stale")
            observation: KubernetesJobObservation | None = None
            if command.command_type == "create":
                observation = await self._create(lease, now=now)
                await self._persist_observation(lease, observation, now=now)
            elif command.command_type in _DELETE_COMMANDS:
                observation = await self._get(lease)
                if observation is not None:
                    await self._delete(
                        lease,
                        observation,
                        now=now,
                        cancel_immediately=command.command_type == "cancel",
                    )
                else:
                    observation = KubernetesJobObservation(
                        namespace=self._target.namespace,
                        job_name=lease.job_name,
                        lease_id=str(lease.id),
                        resource_generation=lease.resource_generation,
                        target_id=self._target.target_id,
                        execution_unit_key=str(lease.execution_unit_key),
                        normalized_state=NormalizedJobState.DELETED,
                        job_uid=lease.job_uid,
                    )
                    await self._persist_observation(lease, observation, now=now)
            elif command.command_type in {"start", "finalize"}:
                observation = await self._get(lease)
                if observation is None:
                    raise ActuatorContractError("expected Kubernetes Job is missing")
                if command.command_type == "start" and lease.desired_state == "finalize":
                    # Output commit may durably queue start and finalize before
                    # the actuator's next poll. The later finalize command owns
                    # terminal projection; acknowledge the superseded start
                    # intent without letting it revoke that command's generation.
                    self._validate_observation(lease, observation)
                else:
                    await self._persist_observation(lease, observation, now=now)
            else:
                raise ActuatorContractError("unsupported execution command")
            await self._ack(
                command,
                {
                    "schema_version": "loom.kubernetes-command-ack.v1",
                    "command_type": command.command_type,
                    "job_uid": observation.job_uid if observation else None,
                    "resource_version": observation.resource_version if observation else None,
                },
                now=now,
            )
        except (
            ActuatorContractError,
            ExecutionProvisioningBlockedError,
            KubernetesApiError,
            ServiceExecutionConflict,
        ) as exc:
            await self._defer(command, exc, now=now)

    async def reconcile_full_once(self, *, now: datetime | None = None) -> int:
        current_time = now or datetime.now(UTC)
        try:
            with KUBERNETES_API_SECONDS.labels(operation="list").time():
                inventory = await self._kubernetes.list_jobs(
                    namespace=self._target.namespace,
                    label_selector=_MANAGED_SELECTOR,
                )
        except KubernetesApiError as exc:
            status_class = (
                f"{exc.status_code // 100}xx" if exc.status_code is not None else "transport"
            )
            KUBERNETES_API_ERRORS_TOTAL.labels(operation="list", status_class=status_class).inc()
            raise
        async with self._sessions() as session:
            family = await resolve_capacity_targets(session, self._target.target_id)
            sibling_ids = family.target_ids - {self._target.target_id}
            leases = (
                (
                    await session.execute(
                        select(ServiceExecutionLease).where(
                            ServiceExecutionLease.target_id == self._target.target_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            for lease in leases:
                session.expunge(lease)
        by_lease: dict[str, list[KubernetesJobObservation]] = defaultdict(list)
        for observation in inventory.observations:
            # A family shares one namespace, not reconciliation authority. Each
            # sibling alone diagnoses and deletes its own Jobs. Unknown target
            # annotations remain drift; namespace equality is still required.
            if observation.target_id in sibling_ids and observation.namespace == self._target.namespace:
                continue
            by_lease[observation.lease_id].append(observation)
        leases_by_id = {str(lease.id): lease for lease in leases}
        drift = inventory.rejected_count
        for lease_id, resources in by_lease.items():
            matched_lease = leases_by_id.get(lease_id)
            if matched_lease is None or len(resources) != 1:
                drift += len(resources)
                continue
            observation = resources[0]
            try:
                self._validate_observation(matched_lease, observation)
                if matched_lease.desired_state == "deleted":
                    await self._delete(matched_lease, observation, now=current_time)
                    drift += 1
                else:
                    await self._persist_observation(matched_lease, observation, now=current_time)
            except (ActuatorContractError, KubernetesApiError, ServiceExecutionConflict):
                drift += 1
        for lease in leases:
            if str(lease.id) in by_lease:
                continue
            if lease.desired_state == "deleted":
                continue
            if lease.job_uid is None and (
                lease.desired_state not in _CLEANUP_DESIRED_STATES or inventory.rejected_count
            ):
                # An unstarted create is not missing. Cleanup can converge without
                # a recorded UID only when the namespace inventory is authoritative.
                continue
            drift += 1
            missing = KubernetesJobObservation(
                namespace=self._target.namespace,
                job_name=lease.job_name,
                lease_id=str(lease.id),
                resource_generation=lease.resource_generation,
                target_id=self._target.target_id,
                execution_unit_key=str(lease.execution_unit_key),
                normalized_state=(
                    NormalizedJobState.DELETED
                    if lease.desired_state in _CLEANUP_DESIRED_STATES
                    else NormalizedJobState.MISSING
                ),
                job_uid=lease.job_uid,
                reason="JobMissing",
                message="expected exact Kubernetes Job was not listed",
            )
            try:
                await self._persist_observation(lease, missing, now=current_time)
            except (ActuatorContractError, ServiceExecutionConflict):
                continue
        KUBERNETES_ORPHAN_COUNT.set(drift)
        KUBERNETES_RECONCILE_CONVERGED.set(1 if drift == 0 else 0)
        # A successful namespace list proves that the target remains reachable
        # even while expected create/delete convergence is still in progress.
        # Coupling health freshness to zero drift makes a slow scale-from-zero
        # batch expire its own broker eligibility before its Pods can start.
        async with self._sessions() as session:
            await refresh_execution_target_health(
                session,
                target_id=self._target.target_id,
                observed_at=current_time,
            )
            await session.commit()
        return drift

    async def watch_once(self, *, timeout_seconds: int = 15) -> int:
        try:
            observations = await self._kubernetes.watch_jobs(
                namespace=self._target.namespace,
                label_selector=_MANAGED_SELECTOR,
                resource_version=self._watch_resource_version,
                timeout_seconds=timeout_seconds,
            )
            KUBERNETES_WATCH_RESTARTS_TOTAL.labels(outcome="completed").inc()
        except KubernetesApiError as exc:
            self._watch_resource_version = None
            KUBERNETES_WATCH_RESTARTS_TOTAL.labels(outcome="error").inc()
            status_class = (
                f"{exc.status_code // 100}xx" if exc.status_code is not None else "transport"
            )
            KUBERNETES_API_ERRORS_TOTAL.labels(operation="watch", status_class=status_class).inc()
            raise
        persisted = 0
        for observation in observations:
            if observation.resource_version is not None:
                # Always advance past a malformed/foreign managed event; the
                # full list remains the repair authority for quarantined drift.
                self._watch_resource_version = observation.resource_version
            try:
                lease_id = UUID(observation.lease_id)
            except ValueError:
                continue
            try:
                lease = await self._lease(lease_id)
                await self._persist_observation(lease, observation, now=datetime.now(UTC))
            except (ActuatorContractError, ServiceExecutionConflict):
                continue
            persisted += 1
        return persisted


__all__ = ["ExecutionActuator"]
