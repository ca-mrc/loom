"""Native build reconciliation inside the existing execution actuator.

The materialization lease owns retries; Kubernetes Jobs never retry a build.
One durable attempt retains the Job UID and resource/lifecycle observations.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    ServiceExecutionTarget,
    TaskImageCapacityWait,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
)
from loom.nebius_kubernetes import (
    NebiusKubernetesConnection,
    create_api_client,
    read_sdk_before_close,
)
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.security.redaction import redact_text
from loom.task_image_build_plan import derive_task_image_build_components
from loom_control_plane.execution_capacity import (
    _CAPACITY_ADMISSION_LOCK,
    ExecutionProvisioningBlockedError,
    native_build_resources,
)
from loom_control_plane.task_image_capacity import reserve_native_task_image_capacity
from loom_control_plane.task_image_capacity_wait import remember_capacity_wait
from loom_control_plane.task_image_materializations import (
    TaskImageCompletionError,
    claim_task_image_materialization,
    complete_task_image_materialization,
    fail_task_image_materialization,
    has_nebius_task_image_demand,
    heartbeat_task_image_materialization,
    same_task_compatible_cache_key,
    start_task_image_materialization,
    task_image_materialization_payload,
)
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import render_task_image_job
from loom_execution_actuator.task_image_settings import (
    NativeTaskImageSettings as NativeTaskImageSettings,
)

_MANAGER = "loom-task-image-builder"
_SELECTOR = "app.kubernetes.io/managed-by=" + _MANAGER
_LIVE = ("claimed", "running")
_LOG = logging.getLogger(__name__)


class NativeBuildApi(Protocol):
    async def inventory(self, namespace: str) -> list[dict[str, Any]]: ...
    async def ensure(self, configmap: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]: ...
    async def observe(self, namespace: str, name: str, *, capture_logs: bool = False) -> dict[str, Any] | None: ...
    async def delete(self, namespace: str, name: str, uid: str | None, *, configmap: dict[str, Any]) -> bool: ...


# Kubernetes omits these explicit false defaults when serializing a Job.
# Other booleans (notably automountServiceAccountToken) have different defaults.
_POD_SPEC_PATH = ("spec", "template", "spec")
_FALSE_DEFAULT_PATHS = {
    *((*_POD_SPEC_PATH, field) for field in ("hostIPC", "hostPID", "hostNetwork")),
    *((*_POD_SPEC_PATH, containers, "*", "volumeMounts", "*", "readOnly")
      for containers in ("initContainers", "containers")),
}
_EMPTYDIR_SIZE_PATH = (*_POD_SPEC_PATH, "volumes", "*", "emptyDir", "sizeLimit")


def _matches(
    actual: Any, expected: Any, *, quantities: bool = False, _path: tuple[str, ...] = (),
) -> bool:
    """Compare owned fields, allowing only known API defaults and quantities."""
    if actual is None and expected is False and _path in _FALSE_DEFAULT_PATHS:
        return True
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            _matches(
                actual.get(key), value,
                quantities=key in {"limits", "requests"} or quantities,
                _path=(*_path, key),
            )
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(
            _matches(a, e, quantities=quantities, _path=(*_path, "*"))
            for a, e in zip(actual, expected, strict=True)
        )
    if (quantities or _path == _EMPTYDIR_SIZE_PATH) and isinstance(actual, str) and isinstance(expected, str):
        from kubernetes.utils.quantity import parse_quantity
        try:
            return bool(parse_quantity(actual) == parse_quantity(expected))
        except ValueError:
            return False
    return bool(actual == expected)


def _owned_pods(observation: dict[str, Any], uid: str | None) -> list[dict[str, Any]]:
    return [pod for pod in observation.get("pods", []) if uid and any(
        owner.get("kind") == "Job" and owner.get("uid") == uid
        for owner in pod.get("metadata", {}).get("ownerReferences", [])
    )]


class NativeBuildKubernetesApi:
    def __init__(self, *, connection: NebiusKubernetesConnection | None = None, api_client: Any | None = None) -> None:
        """Own one SDK client, optionally transferred by an explicit credential owner."""
        from kubernetes import client, config
        self._credentials = None
        if connection is not None and api_client is not None:
            raise ValueError("ambiguous_native_build_client")
        if api_client is not None:
            self._api = api_client
        elif connection is None:
            config.load_incluster_config()
            self._api = client.ApiClient()
        else:
            self._api, self._credentials = create_api_client(connection)
        self._core, self._batch = client.CoreV1Api(self._api), client.BatchV1Api(self._api)

    async def close(self) -> None:
        await asyncio.to_thread(self._api.close)
        if self._credentials is not None:
            await self._credentials.close()

    def _json(self, item: Any) -> dict[str, Any]:
        return cast(dict[str, Any], self._api.sanitize_for_serialization(item))

    async def inventory(self, namespace: str) -> list[dict[str, Any]]:
        def run() -> list[dict[str, Any]]:
            return [self._json(job) for job in self._batch.list_namespaced_job(
                namespace, label_selector=_SELECTOR, _request_timeout=20,
            ).items]
        return await asyncio.to_thread(run)

    async def ensure(self, configmap: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            ns, name = job["metadata"]["namespace"], job["metadata"]["name"]
            try:
                self._core.create_namespaced_config_map(ns, configmap, _request_timeout=20)
            except Exception as error:
                if getattr(error, "status", None) != 409:
                    raise
                current = self._json(self._core.read_namespaced_config_map(name, ns, _request_timeout=20))
                if not _matches(current, configmap):
                    raise ValueError("existing build configuration differs from the frozen attempt") from None
            try:
                observed = self._batch.create_namespaced_job(ns, job, _request_timeout=20)
            except Exception as error:
                if getattr(error, "status", None) != 409:
                    raise
                observed = self._batch.read_namespaced_job(name, ns, _request_timeout=20)
            observed_json = self._json(observed)
            if not _matches(observed_json, job):
                raise ValueError("existing build Job differs from the frozen attempt")
            return observed_json
        return await asyncio.to_thread(run)

    async def observe(self, namespace: str, name: str, *, capture_logs: bool = False) -> dict[str, Any] | None:
        def run() -> dict[str, Any] | None:
            try:
                job = self._json(self._batch.read_namespaced_job(name, namespace, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    raise
                job = {"metadata": {"name": name, "namespace": namespace}, "job_missing": True}
            pods = self._core.list_namespaced_pod(namespace, label_selector="job-name=" + name, _request_timeout=20).items
            job["pods"] = [self._json(pod) for pod in pods]
            if job.get("job_missing") and not pods:
                return None
            if capture_logs or job.get("status", {}).get("succeeded") or job.get("status", {}).get("failed"):
                for pod in _owned_pods(job, job["metadata"].get("uid"))[:1]:
                    try:
                        log = self._core.read_namespaced_pod_log(
                            pod["metadata"]["name"], namespace, container="build", tail_lines=100,
                            limit_bytes=16384, _request_timeout=20,
                        )
                        job["builder_log"] = _safe_log(str(log))
                    except Exception:
                        pass  # An unscheduled Pod has no build log.
            return job
        return await asyncio.to_thread(run)

    async def observe_pool(self, runtime: PoolNativeRuntimeV1, *, capture_logs: bool = False) -> dict[str, Any] | None:
        """Read only a gateway-observed native identity; never adopt/create/delete."""
        from loom_execution_actuator.pool_native_observation import qualify_native_observation

        runtime = PoolNativeRuntimeV1.model_validate_json(runtime.model_dump_json())
        if runtime.receipt.job_uid is None:
            raise ValueError("pool_native_job_not_observed")

        def namespace() -> None:
            value = self._json(self._core.read_namespace(runtime.namespace.name, _request_timeout=20))
            metadata = value.get("metadata", {})
            if (value.get("kind") != "Namespace" or value.get("apiVersion") != "v1"
                    or metadata.get("uid") != str(runtime.namespace.uid)
                    or metadata.get("name") != runtime.namespace.name or metadata.get("deletionTimestamp")):
                raise ValueError("pool_native_namespace_changed")

        def run() -> dict[str, Any] | None:
            namespace()
            try:
                job = self._json(self._batch.read_namespaced_job(runtime.job_name, runtime.namespace.name, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    raise
                # Absence is not cleanup proof or permission to recreate. The
                # gateway separately inventories residual Pods before release.
                namespace()
                return None
            qualify_native_observation(job, runtime)
            listing = self._json(self._core.list_namespaced_pod(runtime.namespace.name,
                label_selector="job-name=" + runtime.job_name, limit=100, _request_timeout=20))
            if (listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                    or not isinstance(listing.get("items"), list)
                    or any(not isinstance(pod, dict) for pod in listing["items"])
                    or listing.get("metadata", {}).get("continue")):
                raise ValueError("pool_native_partial_pod_list")
            # List item type metadata is legitimately omitted by Kubernetes.
            # Inherit only from the qualified PodList, retaining explicit types.
            job["pods"] = [{"apiVersion": "v1", "kind": "Pod", **pod} for pod in listing["items"]]
            qualify_native_observation(job, runtime)
            if job["pods"] and (capture_logs or job.get("status", {}).get("succeeded") or job.get("status", {}).get("failed")):
                pod = job["pods"][0]

                def current_pod() -> None:
                    value = self._json(self._core.read_namespaced_pod(pod["metadata"]["name"], runtime.namespace.name, _request_timeout=20))
                    qualify_native_observation({**job, "pods": [value]}, runtime)
                    if value["metadata"]["uid"] != pod["metadata"]["uid"]:
                        raise ValueError("pool_native_pod_changed")

                current_pod()
                try:
                    log = self._core.read_namespaced_pod_log(pod["metadata"]["name"], runtime.namespace.name,
                        container="build", tail_lines=100, limit_bytes=16384, _request_timeout=20)
                except Exception:
                    pass  # Logs may not exist yet; never fabricate a result.
                else:
                    job["builder_log"] = _safe_log(str(log))
                current_pod()
            namespace()
            return job
        return await read_sdk_before_close(run)

    async def delete(self, namespace: str, name: str, uid: str | None, *, configmap: dict[str, Any]) -> bool:
        def run() -> bool:
            try:
                current = self._json(self._batch.read_namespaced_job(name, namespace, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) != 404:
                    raise
            else:
                if not uid or current["metadata"]["uid"] != uid:
                    raise ValueError("build cleanup Job UID changed")
                self._batch.delete_namespaced_job(name, namespace, body={
                    "propagationPolicy": "Foreground", "gracePeriodSeconds": 0,
                    "preconditions": {"uid": uid},
                }, _request_timeout=20)
                return False
            pods = [self._json(pod) for pod in self._core.list_namespaced_pod(
                namespace, label_selector="job-name=" + name, _request_timeout=20,
            ).items]
            if pods:
                owned = _owned_pods({"pods": pods}, uid)
                if len(owned) != len(pods):
                    raise ValueError("build cleanup Pod ownership differs")
                for pod in owned:
                    try:
                        self._core.delete_namespaced_pod(pod["metadata"]["name"], namespace, body={
                            "gracePeriodSeconds": 0, "preconditions": {"uid": pod["metadata"]["uid"]},
                        }, _request_timeout=20)
                    except Exception as error:
                        if getattr(error, "status", None) != 404:
                            raise
                return False
            try:
                current_cm = self._json(self._core.read_namespaced_config_map(name, namespace, _request_timeout=20))
            except Exception as error:
                if getattr(error, "status", None) == 404:
                    return True
                raise
            if not _matches(current_cm, configmap):
                raise ValueError("build cleanup ConfigMap ownership differs")
            self._core.delete_namespaced_config_map(name, namespace, body={
                "preconditions": {"uid": current_cm["metadata"]["uid"]},
            }, _request_timeout=20)
            return False
        return await asyncio.to_thread(run)


def _safe_log(value: str) -> str:
    value = re.sub(
        r'''(?i)((?:["']?(?:password|secret(?:[_-]?key)?|(?:api|access)[_-]?key|token)["']?)\s*[=:]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)''',
        r"\1[REDACTED]", value,
    )
    return redact_text(value, limit=16384)


def publication_receipt(observation: dict[str, Any], *, materialization_id: UUID, lease_epoch: int) -> dict[str, str]:
    pods = _owned_pods(observation, observation.get("metadata", {}).get("uid"))
    if len(pods) != 1:
        raise ValueError("native build must have exactly one owned Pod")
    statuses = pods[0].get("status", {}).get("containerStatuses", [])
    publisher = next((status for status in statuses if status.get("name") == "publish"), None)
    terminated = (publisher or {}).get("state", {}).get("terminated", {})
    if observation.get("status", {}).get("succeeded") and terminated.get("exitCode") != 0:
        raise ValueError("successful publication requires publisher exit zero")
    message = terminated.get("message", "")
    if not isinstance(message, str) or len(message.encode()) > 4096:
        raise ValueError("invalid publication receipt size")
    receipt = json.loads(message)
    if not isinstance(receipt, dict) or receipt.get("materialization_id") != str(materialization_id) or receipt.get("lease_epoch") != lease_epoch:
        raise ValueError("publication receipt does not belong to this build")
    images = receipt.get("registry_images")
    if not isinstance(images, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in images.items()):
        raise ValueError("publication receipt has invalid images")
    return images


def _phase_error(observation: dict[str, Any], name: str) -> str:
    for pod in _owned_pods(observation, observation.get("metadata", {}).get("uid")):
        for row in (*pod.get("status", {}).get("initContainerStatuses", []), *pod.get("status", {}).get("containerStatuses", [])):
            if row.get("name") != name:
                continue
            message = row.get("state", {}).get("terminated", {}).get("message", "")
            if not isinstance(message, str) or len(message.encode()) > 4096:
                continue
            try:
                receipt = json.loads(message)
            except ValueError:
                continue
            if isinstance(receipt, dict) and isinstance(receipt.get("error"), str):
                return _safe_log(receipt["error"][:500])
    return "check source/cache/registry access and Pod status"


def build_observation(observation: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"observed_at": datetime.now(UTC).isoformat()}
    result["job_conditions"] = [
        _status_diagnostic(condition)
        for condition in observation.get("status", {}).get("conditions", [])[:10]
        if isinstance(condition, dict)
    ]
    pods = _owned_pods(observation, observation.get("metadata", {}).get("uid"))
    if len(pods) > 1:
        raise ValueError("native build unexpectedly has multiple owned Pods")
    if pods:
        pod = pods[0]
        result.update(pod_uid=pod["metadata"]["uid"], node_name=pod.get("spec", {}).get("nodeName"))
        result["pod_status"] = _status_diagnostic(pod.get("status", {}))
        result["scheduling"] = next((
            {**_status_diagnostic(c), "transition_at": c.get("lastTransitionTime")}
            for c in pod.get("status", {}).get("conditions", [])
            if c.get("type") == "PodScheduled"
        ), None)
        phases = []
        for row in (*pod.get("status", {}).get("initContainerStatuses", []), *pod.get("status", {}).get("containerStatuses", [])):
            if row.get("name") not in {"prepare", "build", "publish"}:
                continue
            phase: dict[str, Any] = {"name": row["name"]}
            for kind, detail in row.get("state", {}).items():
                if kind not in {"waiting", "running", "terminated"}:
                    continue
                safe = {key: detail[key] for key in ("exitCode", "signal", "startedAt", "finishedAt") if key in detail}
                reason = detail.get("reason", "")
                if isinstance(reason, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", reason):
                    safe["reason"] = reason
                phase["state"] = {kind: safe}
            phases.append(phase)
        result["phases"] = phases
    if observation.get("builder_log"):
        result["builder_log"] = _safe_log(observation["builder_log"])
    return result


def _status_diagnostic(status: dict[str, Any]) -> dict[str, str]:
    """Keep bounded native diagnostics, never arbitrary status/receipt payloads."""
    result = {}
    for key in ("type", "status", "phase", "reason"):
        value = status.get(key)
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", value):
            result[key] = value
    if isinstance(status.get("message"), str):
        result["message"] = _safe_log(status["message"])[:2000]
    return result


def _native_failure(native: dict[str, Any]) -> tuple[str, str, bool] | None:
    pod = native.get("pod_status", {})
    if pod.get("reason") == "Evicted":
        message = pod.get("message", "")
        storage = any(marker in message.lower() for marker in ("ephemeral-storage", "ephemeral storage", "emptydir"))
        return ("build_storage_exceeded" if storage else "build_pod_evicted", "Pod Evicted: " + message, not storage)
    for phase in native.get("phases", []):
        if phase.get("state", {}).get("terminated", {}).get("reason") == "OOMKilled":
            return "build_oom_killed", "Native build phase " + phase["name"] + " was OOMKilled", False
    for phase in native.get("phases", []):
        if phase.get("name") == "build" and phase.get("state", {}).get("terminated", {}).get("exitCode") == 124:
            return "build_deadline_exceeded", "Native build exceeded the task's build timeout", True
    for condition in native.get("job_conditions", []):
        if condition.get("status") == "True" and condition.get("reason") == "DeadlineExceeded":
            return "build_deadline_exceeded", "Job DeadlineExceeded: " + condition.get("message", ""), True
    return None


class NativeTaskImageController:
    def __init__(self, *, sessions: async_sessionmaker[AsyncSession], kubernetes: NativeBuildApi,
                 target: ExecutionTargetRuntime, settings: NativeTaskImageSettings) -> None:
        self.sessions, self.kubernetes, self.settings = sessions, kubernetes, settings
        self.target = replace(target, namespace=settings.namespace, runtime_class_name=None)
        self.builder_id = "nebius:" + target.target_id

    async def _outstanding(self, session: AsyncSession) -> list[TaskImageMaterializationAttempt]:
        return list((await session.scalars(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.native_build["target_id"].as_string() == self.target.target_id,
            TaskImageMaterializationAttempt.native_build["capacity_released_at"].as_string().is_(None),
        ))).all())

    async def run_once(self) -> None:
        # Database scans recover missing Jobs, stale epochs and ambiguous creates.
        async with self.sessions() as session:
            attempt_ids = [attempt.id for attempt in await self._outstanding(session)]
        unexpected = False
        for attempt_id in attempt_ids:
            try:
                await self._reconcile(attempt_id)
            except ExecutionProvisioningBlockedError:
                pass  # Waiting for quota or a fresh observation is normal health.
            except Exception as error:
                unexpected = True
                _LOG.warning("Native build reconcile deferred attempt=%s error=%s", attempt_id, type(error).__name__)
        if unexpected:
            raise RuntimeError("Native build reconciliation failed; inspect bounded actuator diagnostics")
        jobs = await self.kubernetes.inventory(self.settings.namespace)
        if len(jobs) >= self.settings.max_concurrent:
            return
        try:
            claimed_attempt_id = await self._claim()
        except ExecutionProvisioningBlockedError:
            return  # The entire claim/reservation transaction was rolled back.
        if claimed_attempt_id is not None:
            try:
                await self._reconcile(claimed_attempt_id)
            except ExecutionProvisioningBlockedError:
                pass

    async def _claim(self) -> UUID | None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            waiting: tuple[UUID, int, dict[str, Any]] | None = None
            try:
                # Keep the capacity lock outside the savepoint. Rejected work
                # consumes no attempt, but a validated waiting head survives.
                async with session.begin_nested():
                    attempt = await self._prepare_claim(session)
                    if attempt is None:
                        return None
                    assert attempt.native_build is not None
                    waiting = (attempt.materialization_id, attempt.lease_epoch - 1,
                               dict(attempt.native_build))
                    attempt_id = attempt.id
                    await reserve_native_task_image_capacity(session, attempt_id=attempt_id)
                    wait = await session.get(TaskImageCapacityWait, self.target.target_id)
                    if wait is not None and wait.materialization_id == attempt.materialization_id:
                        await session.delete(wait)
                    return attempt_id
            except ExecutionProvisioningBlockedError as exc:
                if waiting is None:
                    raise
                materialization_id, lease_epoch, native = waiting
                await remember_capacity_wait(
                    session, target_id=self.target.target_id, materialization_id=materialization_id,
                    lease_epoch=lease_epoch, pool_id=self.settings.pool_id,
                    resources=native_build_resources(native), now=datetime.now(UTC), reason=exc.reason,
                )
                return None

    async def _prepare_claim(self, session: AsyncSession) -> TaskImageMaterializationAttempt | None:
        target = await session.get(ServiceExecutionTarget, self.target.target_id)
        if target is None or target.provider != "nebius" or target.desired_state != "active" or target.health_status != "healthy":
            return None
        if len(await self._outstanding(session)) >= self.settings.max_concurrent:
            return None
        row = await claim_task_image_materialization(session, builder_id=self.builder_id, cpu_arch="x86_64",
                                                    nebius_pool_id=self.settings.pool_id)
        if row is None:
            return None
        attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.materialization_id == row.id,
            TaskImageMaterializationAttempt.lease_epoch == row.lease_epoch,
        ).with_for_update())
        assert attempt is not None
        try:
            claim = {**task_image_materialization_payload(row), **self.settings.runtime_configuration()}
            if (
                self.settings.compatible_revision_cache == "same_task"
                and self.settings.cache_bucket is not None
            ):
                donor = await same_task_compatible_cache_key(
                    session,
                    task_id=row.task_id,
                    cpu_arch=row.cpu_arch,
                    exclude_materialization_key=row.materialization_key,
                )
                if donor is not None:
                    claim["cache_import_materialization_key"] = donor
            components = derive_task_image_build_components(row.task_config)
            cm, job = render_task_image_job(materialization_id=row.id, lease_epoch=row.lease_epoch,
                                           claim=claim, components=components, target=self.target,
                                           config=self.settings.job_config())
        except (ValueError, TypeError, KeyError):
            await self._fail(session, row, "build_input_unsupported", retryable=False,
                             message="Unsupported Dockerfile, context, architecture, or task build configuration")
            return None
        for metadata in (cm["metadata"], job["metadata"], job["spec"]["template"]["metadata"]):
            metadata["labels"]["app.kubernetes.io/managed-by"] = _MANAGER
            metadata.setdefault("annotations", {})["loom.openai.com/target-id"] = self.target.target_id
        now = datetime.now(UTC)
        attempt.native_build = {
            "target_id": self.target.target_id, "namespace": self.settings.namespace,
            "job_name": job["metadata"]["name"], "job_uid": None, "state": "reserved",
            "resources": {"vcpu_millis": self.settings.cpu_millis, "memory_mib": self.settings.memory_mib,
                          "storage_mib": self.settings.ephemeral_storage_mib},
            "max_processes": self.settings.max_processes, "reserved_at": now.isoformat(),
            "concurrency_limit": self.settings.max_concurrent,
            "deadline_at": (now + timedelta(seconds=job["spec"]["activeDeadlineSeconds"])).isoformat(),
            "configmap": cm, "job": job,
        }
        await session.flush()
        return attempt

    def _owned(self, row: TaskImageMaterialization | None, attempt: TaskImageMaterializationAttempt) -> bool:
        return bool(row is not None and row.lease_epoch == attempt.lease_epoch and row.claimed_by == self.builder_id
                    and row.state in _LIVE and row.lease_expires_at and row.lease_expires_at > datetime.now(UTC))

    async def _fail(self, session: AsyncSession, row: TaskImageMaterialization, reason: str, *,
                    retryable: bool, images: dict[str, str] | None = None, message: str | None = None) -> None:
        await fail_native_build(session, row, reason, builder_id=self.builder_id,
            retryable=retryable, images=images, message=message)

    async def _lock_build(
        self, session: AsyncSession, attempt_id: UUID,
    ) -> tuple[TaskImageMaterialization | None, TaskImageMaterializationAttempt | None]:
        # New catalog references hold the materialization before refunding old
        # attempts. Use the same order during reconciliation and result capture.
        row = await session.scalar(select(TaskImageMaterialization).join(
            TaskImageMaterializationAttempt,
            TaskImageMaterializationAttempt.materialization_id == TaskImageMaterialization.id,
        ).where(TaskImageMaterializationAttempt.id == attempt_id).with_for_update(of=TaskImageMaterialization))
        attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
        return row, attempt

    async def _reconcile(self, attempt_id: UUID) -> None:
        # Serialize one attempt across actuator replicas without holding the
        # regional admission lock during Kubernetes calls.
        async with self.sessions() as guard, guard.begin():
            locked = await guard.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 1550))"),
                                        {"key": "native-build:" + str(attempt_id)})
            if locked:
                await self._reconcile_locked(attempt_id)

    async def _reconcile_locked(self, attempt_id: UUID) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            row, attempt = await self._lock_build(session, attempt_id)
            if attempt is None or not attempt.native_build or attempt.native_build.get("capacity_released_at"):
                return
            native = dict(attempt.native_build)
            owned = self._owned(row, attempt)
            demand = owned and await has_nebius_task_image_demand(session, materialization_id=attempt.materialization_id,
                                                                 pool_id=self.settings.pool_id)
            deadline = native.get("deadline_at") or (datetime.fromisoformat(native["reserved_at"]) + timedelta(
                seconds=native["job"]["spec"]["activeDeadlineSeconds"])).isoformat()
            expired = datetime.fromisoformat(deadline) <= datetime.now(UTC)
            if owned and not demand:
                assert row is not None
                await self._fail(session, row, "build_cancelled", retryable=True)
                native = dict(attempt.native_build)
                owned = False
            if owned:
                await heartbeat_task_image_materialization(session, materialization_id=attempt.materialization_id,
                                                           builder_id=self.builder_id, lease_epoch=attempt.lease_epoch)
            native["state"] = native.get("state", "reserved") if owned else "cleaning"
            attempt.native_build = native
        # Expiry/cancellation can precede Kubernetes' terminal status. Capture
        # bounded output before cleanup removes the running Pod's only log.
        observed = await self.kubernetes.observe(native["namespace"], native["job_name"], capture_logs=not owned or expired)
        uid = native.get("job_uid")
        if observed is not None and not observed.get("job_missing"):
            actual_uid = observed.get("metadata", {}).get("uid")
            if not actual_uid or uid not in (None, actual_uid) or not _matches(observed, native["job"]):
                raise ValueError("observed build identity differs from its durable attempt")
            uid = actual_uid
        elif observed is not None and uid is None:
            # Recover a create whose response and Job were lost before restart.
            owners = {owner.get("uid") for pod in observed.get("pods", [])
                      if _matches(pod.get("metadata", {}).get("labels", {}), native["job"]["spec"]["template"]["metadata"]["labels"])
                      for owner in pod.get("metadata", {}).get("ownerReferences", [])
                      if owner.get("kind") == "Job" and owner.get("name") == native["job_name"]}
            if len(owners) != 1 or None in owners:
                raise ValueError("orphan build Pod has no unique owner")
            uid = next(iter(owners))
        if uid != native.get("job_uid"):
            native["job_uid"] = uid
            await self._save_native(attempt_id, native)
        if owned and expired:
            # Reconciliation can run after the Job has already published. Honor
            # its validated receipt before applying the lifecycle deadline.
            if observed is not None and observed.get("status", {}).get("succeeded"):
                await self._record_result(attempt_id, observed)
                return
            await self._finish_failure(attempt_id, "build_deadline_exceeded", retryable=True)
            native["state"] = "cleaning"
            owned = False
        if not owned:
            if observed is not None:
                # Expiry/cancellation also needs a final observation before the
                # only native evidence disappears. Never inspect a foreign Pod.
                try:
                    summary = build_observation({**observed, "metadata": {**observed.get("metadata", {}), "uid": uid}})
                    old_uid = native.get("pod_uid")
                    if old_uid and summary.get("pod_uid") not in (None, old_uid):
                        raise ValueError("build Pod identity changed during cleanup")
                except ValueError:
                    # Cleanup still validates Job ownership for every Pod. An
                    # ambiguous observation must not strand those resources.
                    summary = {"observation_error": "build_pod_identity_changed"}
                native = {**native, **summary}
                await self._save_native(attempt_id, native)
            await self._cleanup(attempt_id, native)
            return
        if observed is None or observed.get("job_missing"):
            if uid is not None:
                await self._finish_failure(attempt_id, "build_job_missing", retryable=True)
                await self._cleanup(attempt_id, native)
                return
            # Retry only an unacknowledged frozen create after admission recheck.
            async with self.sessions() as session, session.begin():
                await session.execute(_CAPACITY_ADMISSION_LOCK)
                native = await reserve_native_task_image_capacity(session, attempt_id=attempt_id, revalidate_existing=True)
                native = {**native, "state": "creating"}
                attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
                assert attempt is not None
                attempt.native_build = native
            observed = await self.kubernetes.ensure(native["configmap"], native["job"])
            native["job_uid"] = observed["metadata"]["uid"]
            await self._save_native(attempt_id, native)
        await self._record_result(attempt_id, observed)

    async def _save_native(self, attempt_id: UUID, native: dict[str, Any]) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            attempt = await session.get(TaskImageMaterializationAttempt, attempt_id, with_for_update=True)
            assert attempt is not None
            # A failure may have been recorded in a separate transaction since
            # this lifecycle snapshot was read. Cleanup must retain that result.
            attempt.native_build = {**(attempt.native_build or {}), **native}

    async def _finish_failure(self, attempt_id: UUID, reason: str, *, retryable: bool) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            row, attempt = await self._lock_build(session, attempt_id)
            assert attempt is not None
            if self._owned(row, attempt):
                assert row is not None
                await self._fail(session, row, reason, retryable=retryable)

    async def _cleanup(self, attempt_id: UUID, native: dict[str, Any]) -> None:
        if await self.kubernetes.delete(native["namespace"], native["job_name"], native.get("job_uid"), configmap=native["configmap"]):
            native.update(state="released", capacity_released_at=datetime.now(UTC).isoformat())
            await self._save_native(attempt_id, native)

    async def _record_result(self, attempt_id: UUID, observed: dict[str, Any]) -> None:
        async with self.sessions() as session, session.begin():
            await session.execute(_CAPACITY_ADMISSION_LOCK)
            row, attempt = await self._lock_build(session, attempt_id)
            assert attempt is not None and attempt.native_build is not None
            if not self._owned(row, attempt):
                return  # Next DB scan cleans the old epoch without touching its successor.
            assert row is not None
            await record_native_build_result(session, row, attempt, builder_id=self.builder_id, observed=observed,
                registry_repository=json.loads(attempt.native_build["configmap"]["data"]["claim.json"])["registry_repository"])


async def fail_native_build(session: AsyncSession, row: TaskImageMaterialization, reason: str, *, builder_id: str,
                            retryable: bool, images: dict[str, str] | None = None, message: str | None = None) -> None:
    """Existing native failure semantics; caller owns claim qualification/commit."""
    safe_message = _safe_log(message or reason.replace("_", " "))[-2000:]
    await fail_task_image_materialization(session, materialization_id=row.id, builder_id=builder_id,
        lease_epoch=row.lease_epoch, retryable=retryable, failure_reason=reason,
        failure_message=safe_message, registry_images=images or {})
    attempt = await session.scalar(select(TaskImageMaterializationAttempt).where(
        TaskImageMaterializationAttempt.materialization_id == row.id,
        TaskImageMaterializationAttempt.lease_epoch == row.lease_epoch))
    if attempt is not None and attempt.native_build:
        attempt.native_build = {**attempt.native_build, "failure_reason": reason, "failure_message": safe_message}
        if reason == "build_cancelled":
            # Refund only the retry budget, never the immutable attempt/epoch.
            row.attempt_count -= 1
            row.state, row.next_attempt_at, row.finished_at = "queued", None, None
            attempt.native_build = {**attempt.native_build, "retry_budget_refunded": True}


async def record_native_build_result(session: AsyncSession, row: TaskImageMaterialization,
                                     attempt: TaskImageMaterializationAttempt, *, builder_id: str,
                                     observed: dict[str, Any], registry_repository: str) -> None:
    """Share publication/failure handling without coupling it to any Job writer.

    Caller holds the materialization and attempt locks, qualifies the live lease
    and source, and owns commit. The repository comes from retained authority.
    """
    assert attempt.native_build is not None
    status = observed.get("status", {})
    terminal = bool(status.get("succeeded") or status.get("failed"))
    if row.state == "claimed":
        await start_task_image_materialization(session, materialization_id=row.id,
            builder_id=builder_id, lease_epoch=attempt.lease_epoch)
    try:
        summary = build_observation(observed)
        old_uid = attempt.native_build.get("pod_uid")
        if old_uid and summary.get("pod_uid") not in (None, old_uid):
            raise ValueError("build Pod identity changed")
    except ValueError:
        await fail_native_build(session, row, "build_pod_identity_changed", builder_id=builder_id, retryable=False,
            message="Native build created multiple Pods or replaced its recorded Pod")
        return
    native = {**attempt.native_build, **summary, "state": "terminal" if terminal else "pending"}
    attempt.native_build = native
    if not terminal:
        return
    images: dict[str, str] = {}
    invalid = False
    try:
        images = publication_receipt(observed, materialization_id=row.id, lease_epoch=attempt.lease_epoch)
        if any(not re.fullmatch(re.escape(registry_repository) + r"@sha256:[a-f0-9]{64}", ref) for ref in images.values()):
            raise ValueError("publication outside the frozen task repository")
        if status.get("succeeded"):
            await complete_task_image_materialization(session, materialization_id=row.id,
                builder_id=builder_id, lease_epoch=attempt.lease_epoch, registry_images=images)
            return
    except (ValueError, TypeError, KeyError):
        invalid, images = True, {}
    invalid_message = "Publisher receipt is missing, malformed, or differs from the frozen build identity/components/repository"
    if status.get("succeeded") and invalid:
        await fail_native_build(session, row, "build_publication_receipt_invalid", builder_id=builder_id,
            retryable=False, message=invalid_message)
    else:
        phase = next((phase["name"] for phase in native.get("phases", [])
            if phase.get("state", {}).get("terminated", {}).get("exitCode", 0)), "job")
        reason, message, retryable = _native_failure(native) or (
            "build_" + phase + "_failed",
            ("Native build phase " + phase + " failed. " + native.get("builder_log", ""))
            if phase == "build" else "Native build phase " + phase + " failed: " + _phase_error(observed, phase),
            phase != "build")
        try:
            await fail_native_build(session, row, reason, builder_id=builder_id, retryable=retryable, images=images, message=message)
        except TaskImageCompletionError:
            await fail_native_build(session, row, "build_publication_receipt_invalid", builder_id=builder_id,
                retryable=False, message=invalid_message)
