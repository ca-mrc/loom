from __future__ import annotations

import asyncio
import json
import re
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from datetime import UTC, datetime
from typing import Any

from loom.nebius_kubernetes import (
    NebiusKubernetesConnection,
    NebiusKubernetesCredentials,
    create_api_client,
    read_kubelet_summary,
    read_sdk_before_close,
)
from loom.nebius_pool_contract import PoolNamespaceBindingV1
from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1
from loom.security.redaction import redact_text
from loom_execution_actuator.contracts import (
    ContainerDiagnostic,
    ContainerLogExcerpt,
    ContainerTerminationDiagnostic,
    ExecutionTerminationSummaryV1,
    KubernetesApiError,
    KubernetesJobInventory,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.pool_execution_observation import qualify_execution_observation
from loom_execution_actuator.resource_observations import read_resource_observation

_LEASE_LABEL = "loom.openai.com/lease-id"
_GENERATION_LABEL = "loom.openai.com/generation"
_TARGET_ANNOTATION = "loom.openai.com/target-id"
_EXECUTION_UNIT_ANNOTATION = "loom.openai.com/execution-unit-key"
_RUNTIME_CONTRACT_ANNOTATION = "loom.openai.com/runtime-contract-sha256"
_COMMAND_IDENTITY_ANNOTATION = "loom.openai.com/command-identity-sha256"
_EXECUTION_ROLE_ANNOTATION = "loom.openai.com/execution-role"


def _termination_diagnostic(value: Any) -> ContainerTerminationDiagnostic | None:
    if value is None:
        return None
    reason = getattr(value, "reason", None)
    # Reasons originate at the runtime boundary. Do not persist free-form
    # termination messages, container IDs, image refs, or arbitrary reason text.
    safe_reasons = {
        "OOMKilled",
        "Error",
        "Completed",
        "ContainerCannotRun",
        "StartError",
        "DeadlineExceeded",
        "ContainerStatusUnknown",
    }
    return ContainerTerminationDiagnostic(
        reason=reason if reason in safe_reasons else "Unknown",
        exit_code=getattr(value, "exit_code", None),
        signal=getattr(value, "signal", None),
        started_at=getattr(value, "started_at", None),
        finished_at=getattr(value, "finished_at", None),
    )


def _container_diagnostics(statuses: list[Any]) -> tuple[ContainerDiagnostic, ...]:
    return tuple(
        ContainerDiagnostic(
            name=status.name,
            restart_count=getattr(status, "restart_count", None) or 0,
            current_termination=_termination_diagnostic(
                getattr(getattr(status, "state", None), "terminated", None)
            ),
            previous_termination=_termination_diagnostic(
                getattr(getattr(status, "last_state", None), "terminated", None)
            ),
        )
        for status in statuses
        if getattr(status, "name", None) in {"execution", "task-sandbox", "verifier-sandbox"}
        or str(getattr(status, "name", "")).startswith("fixture-")
    )


def _sandbox_lost(item: ContainerDiagnostic, execution_terminated: Any) -> bool:
    if item.name == "execution":
        return False
    execution_finished = getattr(execution_terminated, "finished_at", None)
    previous = item.previous_termination
    if (
        item.restart_count == 1
        and previous is not None
        and previous.finished_at is not None
        and execution_finished is not None
        and previous.finished_at > execution_finished
    ):
        # The only restart occurred after execution ended. Multiple restarts
        # or missing timestamps cannot establish that earlier state was intact.
        return False
    current = item.current_termination
    return (
        item.restart_count > 0
        or previous is not None
        or (
            current is not None
            and (
                execution_terminated is None
                or (
                    current.finished_at is not None
                    and execution_finished is not None
                    and current.finished_at < execution_finished
                )
            )
        )
    )


def _condition(conditions: list[Any] | None, condition_type: str) -> Any | None:
    return next(
        (
            condition
            for condition in conditions or []
            if getattr(condition, "type", None) == condition_type
        ),
        None,
    )


def _newest_pod(pods: list[Any]) -> Any:
    return max(
        pods,
        key=lambda item: (
            getattr(item.metadata, "creation_timestamp", None) or datetime.min.replace(tzinfo=UTC)
        ),
        default=None,
    )


def _scrub_log(value: str) -> str:
    value = re.sub(
        r"(?i)((?:password|secret(?:[_-]?key)?|(?:api|access)[_-]?key|token)\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;]+)",
        r"\1[REDACTED]",
        value,
    )
    return redact_text(value, limit=4096).strip()


def _normalize(job: Any, pods: list[Any]) -> KubernetesJobObservation:
    metadata = job.metadata
    labels = dict(metadata.labels or {})
    annotations = dict(metadata.annotations or {})
    lease_id = labels.get(_LEASE_LABEL)
    generation = labels.get(_GENERATION_LABEL)
    target_id = annotations.get(_TARGET_ANNOTATION)
    execution_unit_key = annotations.get(_EXECUTION_UNIT_ANNOTATION)
    if not lease_id or not generation or not target_id or not execution_unit_key:
        raise KubernetesApiError("managed Job is missing Loom identity", status_code=409)
    try:
        generation_value = int(generation)
    except (TypeError, ValueError) as exc:
        raise KubernetesApiError("managed Job generation is invalid", status_code=409) from exc

    pod = _newest_pod(pods)
    state = NormalizedJobState.PENDING
    reason: str | None = None
    message: str | None = None
    termination_summary: ExecutionTerminationSummaryV1 | None = None
    scheduled_at = None
    started_at = None
    terminated_at = getattr(job.status, "completion_time", None)
    node_name = None
    pod_uid = None
    pod_ip = None
    pod_resource_version = None
    diagnostics: tuple[ContainerDiagnostic, ...] = ()

    if metadata.deletion_timestamp is not None:
        state = NormalizedJobState.TERMINATING
    elif _condition(getattr(job.status, "conditions", None), "Complete") is not None:
        state = NormalizedJobState.SUCCEEDED
    elif (failed := _condition(getattr(job.status, "conditions", None), "Failed")) is not None:
        reason = getattr(failed, "reason", None)
        message = getattr(failed, "message", None)
        state = (
            NormalizedJobState.DEADLINE_EXCEEDED
            if reason == "DeadlineExceeded"
            else NormalizedJobState.FAILED
        )

    if pod is not None:
        pod_uid = str(pod.metadata.uid) if pod.metadata.uid is not None else None
        pod_resource_version = getattr(pod.metadata, "resource_version", None)
        pod_ip = getattr(pod.status, "pod_ip", None)
        node_name = getattr(pod.spec, "node_name", None)
        scheduled = _condition(getattr(pod.status, "conditions", None), "PodScheduled")
        if getattr(scheduled, "status", None) == "True":
            scheduled_at = getattr(scheduled, "last_transition_time", None)
        statuses = list(getattr(pod.status, "container_statuses", None) or [])
        init_statuses = list(getattr(pod.status, "init_container_statuses", None) or [])
        diagnostics = _container_diagnostics(statuses + init_statuses)
        execution_status = next(
            (status for status in statuses if getattr(status, "name", None) == "execution"),
            None,
        )
        execution_state = getattr(execution_status, "state", None)
        execution_terminated = getattr(execution_state, "terminated", None)
        # Pod startTime acknowledges the kubelet, before image pulls and init
        # containers. Only the execution container starts the workload; task
        # and verifier init sidecars merely prepare its private sandboxes.
        started_at = getattr(
            getattr(execution_state, "running", None), "started_at", None
        ) or getattr(execution_terminated, "started_at", None)
        pod_reason = getattr(pod.status, "reason", None)
        disruption = _condition(getattr(pod.status, "conditions", None), "DisruptionTarget")
        pod_message = getattr(pod.status, "message", None)
        if pod_reason == "Evicted":
            state, reason, message = NormalizedJobState.EVICTED, pod_reason, pod_message
        elif (
            pod_uid is not None
            and getattr(disruption, "status", None) == "True"
            and getattr(disruption, "reason", None) == "DeletionByTaintManager"
        ):
            # This condition belongs to the UID-bound Pod observation. Keep
            # its eviction cause while deletion and generic Job backoff status
            # arrive; do not infer it from unbound Events or other disruptions.
            state, reason, message = (
                NormalizedJobState.EVICTED,
                "DeletionByTaintManager",
                getattr(disruption, "message", None),
            )
        elif pod_reason in {"NodeLost", "Shutdown"}:
            state, reason, message = NormalizedJobState.NODE_LOST, pod_reason, pod_message
        elif pod.metadata.deletion_timestamp is not None or metadata.deletion_timestamp is not None:
            state = NormalizedJobState.TERMINATING
        else:
            terminated = [
                status.state.terminated
                for status in statuses
                if getattr(getattr(status, "state", None), "terminated", None) is not None
            ]
            waiting = [
                status.state.waiting
                for status in statuses + init_statuses
                if getattr(getattr(status, "state", None), "waiting", None) is not None
            ]
            raw_summary = getattr(execution_terminated, "message", None)
            if raw_summary:
                try:
                    if len(raw_summary.encode("utf-8")) > 4096:
                        raise ValueError("termination summary exceeds 4096 bytes")
                    termination_summary = ExecutionTerminationSummaryV1.model_validate(
                        json.loads(raw_summary)
                    )
                except (TypeError, ValueError) as exc:
                    state = NormalizedJobState.FAILED
                    reason = "InvalidTerminationSummary"
                    message = str(exc)[:2000]
            if any(getattr(item, "reason", None) == "OOMKilled" for item in terminated):
                item = next(item for item in terminated if item.reason == "OOMKilled")
                state, reason, message = (
                    NormalizedJobState.OOM_KILLED,
                    "OOMKilled",
                    getattr(item, "message", None),
                )
                terminated_at = getattr(item, "finished_at", None) or terminated_at
            elif any(
                getattr(item, "reason", None) in {"ImagePullBackOff", "ErrImagePull"}
                for item in waiting
            ):
                item = next(
                    item for item in waiting if item.reason in {"ImagePullBackOff", "ErrImagePull"}
                )
                state, reason, message = (
                    NormalizedJobState.IMAGE_PULL_BACKOFF,
                    item.reason,
                    getattr(item, "message", None),
                )
            elif (
                scheduled is not None
                and getattr(scheduled, "status", None) == "False"
                and getattr(scheduled, "reason", None) == "Unschedulable"
            ):
                state, reason, message = (
                    NormalizedJobState.UNSCHEDULABLE,
                    "Unschedulable",
                    getattr(scheduled, "message", None),
                )
            elif getattr(pod.status, "phase", None) == "Running":
                state = NormalizedJobState.RUNNING
            elif getattr(pod.status, "phase", None) == "Succeeded":
                state = NormalizedJobState.SUCCEEDED
            elif getattr(pod.status, "phase", None) == "Failed" and state not in {
                NormalizedJobState.EVICTED,
                NormalizedJobState.NODE_LOST,
                NormalizedJobState.OOM_KILLED,
            }:
                state, reason, message = (
                    NormalizedJobState.FAILED,
                    pod_reason or "PodFailed",
                    pod_message,
                )

            # Kubernetes native sidecars restart independently of the Job.
            # A fresh sandbox or fixture cannot continue the same attempt's process state.
            # Normal sidecar teardown after execution exits is not a failure.
            lost = next(
                (item for item in diagnostics if _sandbox_lost(item, execution_terminated)),
                None,
            )
            if lost is not None:
                endings = (lost.previous_termination, lost.current_termination)
                state = (
                    NormalizedJobState.OOM_KILLED
                    if any(item is not None and item.reason == "OOMKilled" for item in endings)
                    else NormalizedJobState.FAILED
                )
                reason = (
                    "SandboxRestarted"
                    if lost.restart_count > 0 or lost.previous_termination
                    else "SandboxTerminated"
                )
                message = f"{lost.name} lost its attempt process state (restart count {lost.restart_count})"

    if termination_summary is not None and (
        termination_summary.runtime_contract_sha256 != annotations.get(_RUNTIME_CONTRACT_ANNOTATION)
        or termination_summary.command_identity_sha256
        != annotations.get(_COMMAND_IDENTITY_ANNOTATION)
        or termination_summary.execution_role != annotations.get(_EXECUTION_ROLE_ANNOTATION)
    ):
        state = NormalizedJobState.FAILED
        reason = "TerminationSummaryIdentityMismatch"
        message = "runtime termination summary does not match Job identity"
    elif state == NormalizedJobState.SUCCEEDED and termination_summary is None:
        state = NormalizedJobState.FAILED
        reason = "MissingTerminationSummary"
        message = "successful runtime Job has no termination summary"
    elif (
        state == NormalizedJobState.SUCCEEDED
        and termination_summary is not None
        and termination_summary.status != "succeeded"
    ):
        state = NormalizedJobState.FAILED
        reason = "RuntimeReportedFailure"
        message = f"runtime reported {termination_summary.status}"

    return KubernetesJobObservation(
        namespace=metadata.namespace,
        job_name=metadata.name,
        lease_id=lease_id,
        resource_generation=generation_value,
        target_id=target_id,
        execution_unit_key=execution_unit_key,
        normalized_state=state,
        job_uid=str(metadata.uid) if metadata.uid is not None else None,
        pod_uid=pod_uid,
        pod_ip=pod_ip,
        resource_version=(
            str(metadata.resource_version) if metadata.resource_version is not None else None
        ),
        pod_resource_version=str(pod_resource_version)
        if pod_resource_version is not None
        else None,
        container_diagnostics=diagnostics,
        node_name=node_name,
        scheduled_at=scheduled_at,
        started_at=started_at,
        terminated_at=terminated_at,
        reason=reason,
        message=message,
        termination_summary=termination_summary,
    )


class InClusterKubernetesJobApi:
    """Small async seam over namespace-scoped Batch/Core APIs."""

    def __init__(
        self,
        *,
        connection: NebiusKubernetesConnection | None = None,
        client_module: Any | None = None,
        batch_api: Any | None = None,
        core_api: Any | None = None,
        resource_observation_namespace: str | None = None,
        resource_observation_execution_namespace: str | None = None,
        resource_observation_max_age_seconds: float = 60,
    ) -> None:
        if resource_observation_namespace is not None and resource_observation_execution_namespace is None:
            raise ValueError("resource observation execution namespace is required")
        self._resource_observation_namespace = resource_observation_namespace
        self._resource_observation_execution_namespace = resource_observation_execution_namespace
        self._resource_observation_max_age_seconds = resource_observation_max_age_seconds
        self._api_client: Any | None = None
        self._credentials: NebiusKubernetesCredentials | None = None
        try:
            from kubernetes import client, config
        except ModuleNotFoundError as exc:
            raise RuntimeError("install Loom with the cluster extra") from exc
        if connection is not None and any(
            value is not None for value in (client_module, batch_api, core_api)
        ):
            raise ValueError("remote connection cannot be combined with injected clients")
        if any(value is not None for value in (client_module, batch_api, core_api)):
            if client_module is None or batch_api is None or core_api is None:
                raise ValueError("client_module, batch_api, and core_api must be provided together")
            self._client = client_module
            self._batch = batch_api
            self._core = core_api
        elif connection is not None:
            self._api_client, self._credentials = create_api_client(connection)
            self._client = client
            self._batch = client.BatchV1Api(self._api_client)
            self._core = client.CoreV1Api(self._api_client)
        else:
            config.load_incluster_config()
            self._client = client
            self._batch = client.BatchV1Api()
            self._core = client.CoreV1Api()

    async def resource_summary(self, *, node_name: str, expected_node_uid: str | None = None) -> dict[str, Any]:
        """Read the configured verified source, without proxy or trust fallback."""

        def read() -> dict[str, Any]:
            try:
                if self._resource_observation_namespace is not None:
                    assert self._resource_observation_execution_namespace is not None
                    return read_resource_observation(
                        self._core, node_name=node_name, expected_node_uid=expected_node_uid,
                        namespace=self._resource_observation_namespace,
                        execution_namespace=self._resource_observation_execution_namespace,
                        max_age_seconds=self._resource_observation_max_age_seconds,
                    )
                return read_kubelet_summary(
                    self._core, node_name=node_name, expected_node_uid=expected_node_uid,
                )
            except Exception as exc:
                raise self._translate(exc, "resource_summary") from exc

        return await read_sdk_before_close(read)

    async def close(self) -> None:
        try:
            if self._api_client is not None:
                await asyncio.to_thread(self._api_client.close)
        finally:
            if self._credentials is not None:
                await self._credentials.close()

    def _observe(self, namespace: str, job: Any, *, deleted: bool = False) -> KubernetesJobObservation:
        pods = self._pods_for_job(namespace, job.metadata.name)
        observed = _normalize(job, pods)
        if deleted and not pods:
            return observed.model_copy(update={"normalized_state": NormalizedJobState.DELETED})
        if observed.normalized_state != NormalizedJobState.FAILED:
            return observed
        pod = _newest_pod(pods)
        if pod is None:
            return observed
        return observed.model_copy(update={"container_logs": self._log_excerpts(namespace, pod)})

    def _log_excerpts(self, namespace: str, pod: Any) -> tuple[ContainerLogExcerpt, ...]:
        pod_name = getattr(getattr(pod, "metadata", None), "name", None)
        if not isinstance(pod_name, str) or not pod_name:
            return ()
        statuses = list(getattr(pod.status, "container_statuses", None) or [])
        statuses.extend(getattr(pod.status, "init_container_statuses", None) or [])
        excerpts: list[ContainerLogExcerpt] = []
        seen: set[str] = set()
        for status in statuses:
            name = getattr(status, "name", None)
            if not isinstance(name, str) or name in seen:
                continue
            if name not in {
                "execution",
                "task-sandbox",
                "verifier-sandbox",
            } and not name.startswith("fixture-"):
                continue
            seen.add(name)
            try:
                text = self._core.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=namespace,
                    container=name,
                    tail_lines=100,
                    limit_bytes=4096,
                    _request_timeout=20,
                )
            except Exception:
                continue
            scrubbed = _scrub_log(str(text))
            if scrubbed:
                excerpts.append(ContainerLogExcerpt(name=name, text=scrubbed))
            if len(excerpts) == 4:
                break
        return tuple(excerpts)

    def _pods_for_job(self, namespace: str, job_name: str) -> list[Any]:
        return list(
            self._core.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}",
            ).items
        )

    def _translate(self, exc: Exception, operation: str) -> KubernetesApiError:
        raw_status = getattr(exc, "status", None)
        status = raw_status if isinstance(raw_status, int) else None
        headers = getattr(exc, "headers", None) or {}
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        return KubernetesApiError(
            f"Kubernetes {operation} failed",
            status_code=status,
            retry_after_seconds=(int(str(retry_after)) if str(retry_after).isdigit() else None),
            ambiguous=status is None or status >= 500,
        )

    def _get_sync(self, namespace: str, job_name: str) -> KubernetesJobObservation | None:
        try:
            job = self._batch.read_namespaced_job(name=job_name, namespace=namespace)
            return self._observe(namespace, job)
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return None
            raise self._translate(exc, "get") from exc

    async def get_job(self, *, namespace: str, job_name: str) -> KubernetesJobObservation | None:
        return await asyncio.to_thread(self._get_sync, namespace, job_name)

    def _pool_namespace(self, namespace: PoolNamespaceBindingV1) -> None:
        value = self._core.read_namespace(namespace.name, _request_timeout=20)
        if (value.api_version != "v1" or value.kind != "Namespace"
                or value.metadata.name != namespace.name or value.metadata.uid != str(namespace.uid)
                or value.metadata.deletion_timestamp is not None):
            raise ValueError("pool_execution_namespace_changed")

    async def probe_pool_namespace(self, namespace: PoolNamespaceBindingV1) -> None:
        namespace = PoolNamespaceBindingV1.model_validate_json(namespace.model_dump_json())
        try:
            await read_sdk_before_close(lambda: self._pool_namespace(namespace))
        except (ValueError, TypeError, AttributeError):
            raise KubernetesApiError("pool execution namespace identity conflict", status_code=409) from None
        except Exception as exc:
            raise self._translate(exc, "probe_pool_namespace") from exc

    async def observe_pool(self, runtime: PoolExecutionRuntimeV1) -> KubernetesJobObservation | None:
        """Read the exact gateway-observed task; absence is never release proof."""
        runtime = PoolExecutionRuntimeV1.model_validate_json(runtime.model_dump_json())
        if runtime.receipt.job_uid is None:
            raise KubernetesApiError("pool execution Job is not observed", status_code=409)

        def read() -> KubernetesJobObservation | None:
            try:
                self._pool_namespace(runtime.namespace)
                try:
                    job = self._batch.read_namespaced_job(runtime.job_name, runtime.namespace.name, _request_timeout=20)
                except Exception as exc:
                    if getattr(exc, "status", None) != 404:
                        raise
                    self._pool_namespace(runtime.namespace)
                    return None
                qualify_execution_observation(job, [], runtime)
                listing = self._core.list_namespaced_pod(runtime.namespace.name,
                    label_selector="job-name=" + runtime.job_name, limit=100, _request_timeout=20)
                if (listing.api_version != "v1" or listing.kind != "PodList"
                        or not isinstance(listing.items, list) or listing.metadata._continue):
                    raise ValueError("pool_execution_partial_pod_list")
                qualify_execution_observation(job, listing.items, runtime)
                observed = _normalize(job, listing.items)
                if observed.normalized_state == NormalizedJobState.FAILED and listing.items:
                    pod = listing.items[0]

                    def current_pod() -> None:
                        current = self._core.read_namespaced_pod(pod.metadata.name, runtime.namespace.name,
                            _request_timeout=20)
                        qualify_execution_observation(job, [current], runtime)
                        if current.metadata.uid != pod.metadata.uid:
                            raise ValueError("pool_execution_log_pod_changed")

                    current_pod()
                    excerpts = self._log_excerpts(runtime.namespace.name, pod)
                    current_pod()
                    observed = observed.model_copy(update={"container_logs": excerpts})
                self._pool_namespace(runtime.namespace)
                return observed
            except KubernetesApiError:
                raise
            except (ValueError, TypeError, AttributeError):
                raise KubernetesApiError("pool execution observation identity conflict", status_code=409) from None
            except Exception as exc:
                raise self._translate(exc, "observe_pool") from exc

        return await read_sdk_before_close(read)

    async def create_job(
        self, *, namespace: str, manifest: dict[str, Any]
    ) -> KubernetesJobObservation:
        def create() -> KubernetesJobObservation:
            try:
                job = self._batch.create_namespaced_job(namespace=namespace, body=manifest)
                return self._observe(namespace, job)
            except Exception as exc:
                raise self._translate(exc, "create") from exc

        return await asyncio.to_thread(create)

    async def delete_job(
        self,
        *,
        namespace: str,
        job_name: str,
        expected_uid: str,
        grace_period_seconds: int,
    ) -> None:
        def delete() -> None:
            try:
                options = self._client.V1DeleteOptions(
                    propagation_policy="Foreground",
                    grace_period_seconds=grace_period_seconds,
                    preconditions=self._client.V1Preconditions(uid=expected_uid),
                )
                self._batch.delete_namespaced_job(
                    name=job_name,
                    namespace=namespace,
                    body=options,
                )
            except Exception as exc:
                if getattr(exc, "status", None) == 404:
                    return
                raise self._translate(exc, "delete") from exc

        await asyncio.to_thread(delete)

    def _list_sync(self, namespace: str, label_selector: str) -> KubernetesJobInventory:
        try:
            jobs = self._batch.list_namespaced_job(
                namespace=namespace,
                label_selector=label_selector,
            ).items
            observations: list[KubernetesJobObservation] = []
            rejected_count = 0
            for job in jobs:
                try:
                    observations.append(self._observe(namespace, job))
                except KubernetesApiError as exc:
                    if exc.status_code != 409:
                        raise
                    # A malformed object inside the managed label scope is
                    # quarantined as drift instead of poisoning all repair.
                    rejected_count += 1
            return KubernetesJobInventory(tuple(observations), rejected_count)
        except Exception as exc:
            if isinstance(exc, KubernetesApiError):
                raise
            raise self._translate(exc, "list") from exc

    async def list_jobs(self, *, namespace: str, label_selector: str) -> KubernetesJobInventory:
        return await asyncio.to_thread(self._list_sync, namespace, label_selector)

    async def watch_jobs(
        self,
        *,
        namespace: str,
        label_selector: str,
        resource_version: str | None,
        timeout_seconds: int,
        on_observation: Callable[[KubernetesJobObservation], Awaitable[None]] | None = None,
    ) -> tuple[KubernetesJobObservation, ...]:
        from kubernetes import watch

        loop = asyncio.get_running_loop()
        stream = watch.Watch()
        stopped = threading.Event()
        pending: Future[None] | None = None

        async def deliver(observation: KubernetesJobObservation) -> None:
            assert on_observation is not None
            await on_observation(observation)

        def watch_once() -> tuple[KubernetesJobObservation, ...]:
            nonlocal pending
            try:
                observations: list[KubernetesJobObservation] = []
                for event in stream.stream(
                    self._batch.list_namespaced_job,
                    namespace=namespace,
                    label_selector=label_selector,
                    resource_version=resource_version,
                    timeout_seconds=timeout_seconds,
                ):
                    if stopped.is_set():
                        break
                    job = event["object"]
                    # A tombstone alone must not release resources while a
                    # dependent Pod still exists. _observe lists those Pods.
                    observation = self._observe(namespace, job, deleted=event["type"] == "DELETED")
                    if on_observation is None:
                        observations.append(observation)
                    else:
                        # Persist each event immediately on the owning event loop.
                        # Waiting for the callback bounds memory and applies
                        # backpressure without buffering an entire watch window.
                        pending = asyncio.run_coroutine_threadsafe(deliver(observation), loop)
                        if stopped.is_set():
                            pending.cancel()
                        pending.result()
                        pending = None
                return tuple(observations)
            except Exception as exc:
                if stopped.is_set():
                    return ()
                if getattr(exc, "status", None) == 410:
                    raise self._translate(exc, "watch resource version expired") from exc
                raise self._translate(exc, "watch") from exc
            finally:
                stream.stop()

        try:
            return await asyncio.to_thread(watch_once)
        finally:
            stopped.set()
            stream.stop()
            if pending is not None:
                pending.cancel()


__all__ = ["InClusterKubernetesJobApi"]
