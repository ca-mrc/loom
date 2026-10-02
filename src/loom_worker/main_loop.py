"""Worker main loop — wires settings → register → heartbeat thread →
claim loop → runner pool → drain.

Dockerfile-backed claims carry the immutable task snapshot and registry
digests selected by the scheduler. Prebuilt-image tasks retain the historic
second round-trip to `GET /tasks/{task_id}/bundle`.

Remaining v1 limitation: the worker uses a tempfile mkdtemp() for the
task directory. The solution/ + tests/ + environment/ subtrees that live
under a real fixture directory must be fetched out-of-band — production
deploys mount a shared volume or run a git clone against
`bundle["source"]`. v1 documents this as an ops requirement and leaves
the dir empty; agents that depend on disk content (OracleAgent,
PytestVerifier with local tests) will error out until the ops
integration ships.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import platform
import re
import shutil
import socket
import tempfile
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import httpx
from loom_bundle_checksum import sha256_of_dir

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.driver.docker import DockerDriver
from loom.errors import AgentError, classify_failure_message
from loom.models.resource_usage import TrialResourceUsageReport
from loom.models.result import FailureReason
from loom.models.task import TaskConfig
from loom.models.trial import RetryPolicy, RetryReason, TrialConfig
from loom.models.types import ModelSpec
from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1
from loom.pipeline.keys import canonical_document
from loom.pipeline.work_protocol import ExecutionAttemptClaimV1, WorkClaimV1
from loom.retry import next_attempt_at
from loom.security.redaction import redact_text
from loom.startup_retry import (
    DEFAULT_STARTUP_RETRY_CONFIG,
    StartupRetryConfig,
    retry_startup_dependency,
    retry_startup_dependency_sync,
)
from loom.task_bundle_compat import validate_task_dir_compatibility
from loom.task_image_materialization import TaskImageExecutionGrantV1
from loom.trajectory.cp_event_sink import CpEventSink
from loom.trajectory.storage import (
    MinioObjectStore,
    ObjectStore,
    bundle_file_metadata_sha256,
)
from loom.trial.workspace import (
    WorkspaceStagingPolicy,
    require_tb21_workspace_staging_policy,
    resolve_trial_workspace_staging_policy,
)
from loom.verifier.base import Verifier
from loom.verifier.pytest_verifier import PytestVerifier
from loom.verifier.script_verifier import ScriptVerifier
from loom_task_image_authority.execution_delivery import SignedWorkClaim, TaskImageExecutionDelivery
from loom_task_image_authority.execution_grant import verify_execution_grant
from loom_worker.artifact_input_journal import allocatable_capacity
from loom_worker.capability_snapshot import build_worker_capability_snapshot
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import (
    HttpControlPlaneClient,
    StepTokenClient,
    validate_task_image_execution_origin,
)
from loom_worker.heartbeat import HeartbeatThread
from loom_worker.materializers import (
    build_default_materializers,
    dispatch_materialize,
)
from loom_worker.orphan_cleanup import cleanup_orphan_trajectories
from loom_worker.orphan_containers import cleanup_orphan_sandbox_containers
from loom_worker.resource_accounting import ResourceAccountingDriver, execution_key
from loom_worker.resource_usage_outbox import ResourceUsageOutbox
from loom_worker.runner_pool import RunnerPool
from loom_worker.sandbox_network import SandboxNetworkAllocator
from loom_worker.sandbox_singleton import (
    SandboxSingletonManager,
    SingletonStartupError,
)
from loom_worker.signal_handler import ShutdownState, install_signal_handlers
from loom_worker.step_gateway_client import StepTokenGatewayClient
from loom_worker.task_bundle_integrity import verified_task_image_cache_identity
from loom_worker.task_image import TaskImageBuildError, resolve_task_image
from loom_worker.task_image_execution import WorkerExecutionTrust, WorkerTaskImageExecution
from loom_worker.task_sidecars import DockerTaskSidecarRuntime
from loom_worker.trial_cache import (
    _daemon_build_slot,
    evict_stale_managed_images_from_env,
    resolve_trial_image,
)
from loom_worker.trial_cancellation_watchdog import (
    resolve_hard_deadline_sec,
    run_with_watchdog,
)
from loom_worker.trial_runner import AgentFactory, LocalTrialRunner
from loom_worker.vllm_registry import WorkerVLLMRegistry
from loom_worker.worker_health import WorkerUnhealthyError, hard_exit_unhealthy_worker

_DOCKER_HOST_GATEWAY_EXTRA_HOSTS: tuple[tuple[str, str], ...] = (
    ("host.docker.internal", "host-gateway"),
)
_SETUP_FAILURE_MESSAGE_LIMIT = 1000
_SETUP_FAILURE_HEAD_CHARS = 360
_SETUP_FAILURE_TRUNCATION_MARKER = (
    "\n...[truncated setup diagnostic; preserved trailing output]...\n"
)
_WORKER_SLICE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*\.slice$")
_CANDIDATE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

logger = logging.getLogger(__name__)

_VERIFIER_CTORS: dict[str, Callable[..., Verifier]] = {
    "pytest": PytestVerifier,
    "script": ScriptVerifier,
}


def _tb21_workspace_staging_policy_from_provenance(
    raw_policy: object,
) -> WorkspaceStagingPolicy:
    """Compat wrapper; prefer ``require_tb21_workspace_staging_policy``."""
    return require_tb21_workspace_staging_policy(raw_policy)


def _host_cpu_arch() -> str:
    # Disposable local Compose runs on Apple Silicon but publishes/runs the
    # supported execution architecture (x86_64) via Docker Desktop qemu (#1462).
    from loom.execution_architecture import execution_cpu_arch
    from loom.service_execution_backend import local_execution_enabled

    if local_execution_enabled():
        return execution_cpu_arch("any")
    machine = platform.machine().lower()
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    if machine in {"amd64", "x86_64"}:
        return "x86_64"
    raise RuntimeError("worker CPU architecture is unsupported")


def _worker_hostname(configured_hostname: str | None) -> str:
    if configured_hostname:
        return configured_hostname
    return socket.gethostname()


def _runtime_identity_labels(
    settings: WorkerSettings,
) -> tuple[tuple[str, str], ...]:
    """Return local environment identity labels for spawned containers."""
    values = (
        ("loom.sandbox", getattr(settings, "sandbox_identity", "")),
        ("loom.candidate_sha", getattr(settings, "candidate_sha", "")),
        ("loom.compose_project", getattr(settings, "compose_project", "")),
    )
    return tuple((key, value) for key, value in values if value)


def _worker_cgroup_parent(settings: WorkerSettings) -> str | None:
    """Validate an optional local Docker resource parent before registration."""
    raw_parent = str(getattr(settings, "cgroup_parent", "")).strip()
    if not raw_parent:
        return None
    if any(char in raw_parent for char in ("\x00", "\n", "\r")):
        raise RuntimeError("worker cgroup parent is malformed")
    if _WORKER_SLICE_RE.fullmatch(raw_parent):
        return raw_parent
    parent = PurePosixPath(raw_parent)
    if not parent.is_absolute() or parent == PurePosixPath("/"):
        raise RuntimeError("worker cgroup parent must be a non-root absolute path")
    if any(part in {".", ".."} for part in raw_parent.split("/")):
        raise RuntimeError("worker cgroup parent contains traversal")
    return parent.as_posix()


_DEFAULT_CAPS = [
    {
        "os": "linux",
        "backend": "docker",
        "cpu_arch": _host_cpu_arch(),
        "gpu_vendor": "none",
        "network_policies": ["public", "no-network", "allowlist"],
        "dynamic_network_policy": True,
        "mounted_fs": True,
        "resource_modes": ["auto", "limit", "guarantee"],
        "terminus2_model_switch": True,
    }
]


def _worker_capabilities(settings: WorkerSettings) -> list[dict[str, Any]]:
    return [dict(_DEFAULT_CAPS[0])]


def _host_memory_bytes() -> int:
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (OSError, ValueError):
        raise RuntimeError("worker host memory cannot be measured") from None


def _trial_execution_registration_payload(settings: WorkerSettings) -> dict[str, Any]:
    """Measure an ordinary reader without enabling Pipeline, GPU or input-cache work."""
    snapshot = build_worker_capability_snapshot(
        cpu_arch=_host_cpu_arch(),
        cpu_cores=max(1, os.cpu_count() or 1),
        memory_bytes=_host_memory_bytes(),
        scratch_bytes=shutil.disk_usage(settings.trajectory_cache_dir).total,
        network_profiles=["gateway", "none"],
        container_runtime_features=[],
        gpu_devices=(),
        input_cache_capacity_bytes=0,
        input_cache_reserved_bytes=0,
        input_cache_ready_bytes=0,
    )
    capabilities = _worker_capabilities(settings)
    capabilities[0].update(cpu_arch=snapshot.cpu_arch, gpu_vendor="none")
    return {
        "capabilities": capabilities,
        "capability_snapshot": snapshot.model_dump(mode="json"),
        "capability_snapshot_digest": snapshot.digest,
    }


def _pipeline_registration_payload(
    settings: WorkerSettings,
    *,
    cache_fields: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Measure CPU resources for an explicitly supplied local Pipeline runner."""

    cpu_arch = _host_cpu_arch()
    raw_filesystem_bytes = shutil.disk_usage(settings.trajectory_cache_dir).total
    cache = dict(cache_fields or {})
    snapshot = build_worker_capability_snapshot(
        cpu_arch=cpu_arch,
        cpu_cores=max(1, os.cpu_count() or 1),
        memory_bytes=_host_memory_bytes(),
        scratch_bytes=raw_filesystem_bytes,
        network_profiles=["gateway", "none"],
        container_runtime_features=[],
        input_cache_capacity_bytes=cache.get(
            "input_cache_capacity_bytes", allocatable_capacity(raw_filesystem_bytes)
        ),
        input_cache_reserved_bytes=cache.get("input_cache_reserved_bytes", 0),
        input_cache_ready_bytes=cache.get("input_cache_ready_bytes", 0),
        gpu_devices=(),
    )
    legacy_capabilities = _worker_capabilities(settings)
    legacy_capabilities[0].update(
        cpu_arch=snapshot.cpu_arch,
        gpu_vendor="nvidia" if snapshot.gpu_devices else "none",
    )
    return {
        "capabilities": legacy_capabilities,
        "capability_snapshot": snapshot.model_dump(mode="json"),
        "capability_snapshot_digest": snapshot.digest,
        "input_cache_capacity_bytes": snapshot.input_cache_capacity_bytes,
        "input_cache_reserved_bytes": snapshot.input_cache_reserved_bytes,
        "input_cache_ready_bytes": snapshot.input_cache_ready_bytes,
    }


@dataclass
class _IdleExitTracker:
    after_seconds: float | None
    now: Callable[[], float] = time.monotonic
    _idle_started_at: float | None = None
    _idle_for_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.after_seconds is not None and self.after_seconds < 0:
            raise ValueError("idle_exit_after_seconds must be >= 0")

    @property
    def idle_for_seconds(self) -> float:
        return self._idle_for_seconds

    def observe(self, *, claimed: int, in_flight: int) -> bool:
        if self.after_seconds is None:
            self._idle_started_at = None
            self._idle_for_seconds = 0.0
            return False
        if claimed > 0 or in_flight > 0:
            self._idle_started_at = None
            self._idle_for_seconds = 0.0
            return False

        current = self.now()
        if self._idle_started_at is None:
            self._idle_started_at = current
        self._idle_for_seconds = current - self._idle_started_at
        return self._idle_for_seconds >= self.after_seconds


@dataclass
class _PeriodicMaintenanceTracker:
    interval_seconds: float
    now: Callable[[], float] = time.monotonic
    _last_run_at: float = 0.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("maintenance interval must be positive")
        self._last_run_at = self.now()

    def due(self) -> bool:
        current = self.now()
        if current - self._last_run_at < self.interval_seconds:
            return False
        self._last_run_at = current
        return True


def _resolve_blocking_io_max_workers(settings: WorkerSettings) -> int:
    configured = settings.blocking_io_max_workers
    if configured is not None:
        if configured < 1:
            raise ValueError("blocking_io_max_workers must be >= 1")
        return configured
    concurrency = max(1, settings.max_concurrent)
    return max(32, min(concurrency * 4, 256))


def _configure_blocking_io_executor(settings: WorkerSettings) -> None:
    max_workers = _resolve_blocking_io_max_workers(settings)
    executor = ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="loom-worker-io",
    )
    asyncio.get_running_loop().set_default_executor(executor)
    logger.info("worker_blocking_io_executor_configured max_workers=%d", max_workers)


def _build_worker_object_store(settings: WorkerSettings) -> MinioObjectStore:
    return MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
        max_pool_connections=settings.minio_max_pool_connections,
        connect_timeout=settings.minio_connect_timeout_sec,
        read_timeout=settings.minio_read_timeout_sec,
        operation_timeout=settings.minio_operation_timeout_sec,
        operation_attempts=settings.minio_operation_attempts,
    )


def _docker_registry_auth_summary(
    config_path: Path | None = None,
) -> dict[str, object]:
    if config_path is None:
        docker_config = os.environ.get("DOCKER_CONFIG")
        if docker_config:
            config_path = Path(docker_config) / "config.json"
        else:
            config_path = Path.home() / ".docker" / "config.json"

    summary: dict[str, object] = {
        "config_path": str(config_path),
        "present": False,
        "auth_registries": [],
        "uses_credential_store": False,
    }
    try:
        present = config_path.is_file()
    except OSError:
        return summary
    if not present:
        return summary

    summary["present"] = True
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return summary

    auths = data.get("auths")
    if isinstance(auths, dict):
        summary["auth_registries"] = sorted(str(key) for key in auths)
    cred_helpers = data.get("credHelpers")
    summary["uses_credential_store"] = bool(
        data.get("credsStore") or (isinstance(cred_helpers, dict) and cred_helpers)
    )
    return summary


def _log_docker_registry_auth_summary() -> None:
    summary = _docker_registry_auth_summary()
    logger.info(
        "docker_registry_auth_config config_path=%s present=%s "
        "auth_registries=%s uses_credential_store=%s",
        summary["config_path"],
        summary["present"],
        summary["auth_registries"],
        summary["uses_credential_store"],
    )


async def run_worker(
    settings: WorkerSettings,
    *,
    pipeline_run: Callable[[ExecutionAttemptClaimV1], Coroutine[Any, Any, None]] | None = None,
    execution_trust: WorkerExecutionTrust | None = None,
) -> None:
    execution_config = getattr(settings, "task_image_execution_config_file", None)
    if execution_config is not None:
        from loom_task_image_authority.execution_config import load_execution_reader_settings

        if execution_trust is not None:
            raise ValueError("execution configuration conflicts with injected trust")
        release = load_execution_reader_settings(execution_config)
        execution_trust = WorkerExecutionTrust(
            root=release.root.trust_root(),
            purpose=release.purpose,
            shadow_campaign_id=release.shadow_campaign_id,
        )
    if execution_trust is not None:
        # Protect registration/claim bearer credentials as well as the later
        # online receipt. Enabling V2 on the legacy HTTP default is forbidden.
        validate_task_image_execution_origin(str(settings.control_plane_url))
    state = ShutdownState()
    install_signal_handlers(state)
    # Evaluate the controller-owned containment binding before registration,
    # cleanup, or claims. Reject a malformed local resource parent early.
    _worker_cgroup_parent(settings)
    settings.trajectory_cache_dir.mkdir(parents=True, exist_ok=True)
    _configure_blocking_io_executor(settings)
    _log_docker_registry_auth_summary()

    async with (
        httpx.AsyncClient(
            base_url=str(settings.control_plane_url),
            timeout=30.0,
        ) as cp_http,
        httpx.AsyncClient(
            base_url=str(settings.gateway_url),
            timeout=120.0,
        ) as gw_http,
    ):
        cp_client = HttpControlPlaneClient(
            base_url=str(settings.control_plane_url),
            token=settings.token.get_secret_value(),
            _client=cp_http,
        )
        gateway_client = HttpLLMGatewayClient(
            base_url=str(settings.gateway_url),
            token=settings.token.get_secret_value(),
            _client=gw_http,
        )

        info = await _register_worker_with_retry(
            cp_client=cp_client,
            settings=settings,
            pipeline_enabled=pipeline_run is not None,
            execution_trust=execution_trust,
        )
        worker_id = UUID(info["worker_id"])
        resource_usage_outbox = ResourceUsageOutbox(
            settings.trajectory_cache_dir / "resource-usage-outbox",
        )
        await resource_usage_outbox.replay(cp_client.report_resource_usage)
        capability_snapshot_digest = info.get("capability_snapshot_digest")
        logger.info("worker_registered worker_id=%s", worker_id)

        _run_orphan_cleanup(settings, worker_id)
        _run_trial_cache_eviction(settings)

        sync_http = httpx.Client(
            base_url=str(settings.control_plane_url),
            timeout=5.0,
        )

        def _hb_tick() -> None:
            sync_http.post(
                f"/workers/{worker_id}/heartbeat",
                headers=cp_client.request_headers,
            )

        hb = HeartbeatThread(
            worker_id=worker_id,
            interval_sec=settings.heartbeat_interval_sec,
            tick_fn=_hb_tick,
        )
        hb.start()

        try:
            pool = RunnerPool(
                max_concurrent=settings.max_concurrent,
                unhealthy_callback=hard_exit_unhealthy_worker,
            )
            object_store = _build_worker_object_store(settings)
            await _ensure_runtime_buckets(
                object_store,
                trajectories_bucket=settings.trajectories_bucket,
                artifacts_bucket=settings.artifacts_bucket,
            )
            idle_exit = _IdleExitTracker(
                after_seconds=settings.idle_exit_after_seconds,
            )
            image_eviction = _PeriodicMaintenanceTracker(interval_seconds=3_600)
            # PR-E: per-worker vLLM registry. Opt-in via settings; the
            # `enabled=False` path still constructs the object so the
            # trial runner gets a deterministic AgentError instead of
            # a None-dereference when a trial requests local-vllm.
            vllm_registry = WorkerVLLMRegistry(
                enabled=settings.enable_worker_vllm,
                default_gpu_memory_utilization=(settings.vllm_gpu_memory_utilization),
                default_tensor_parallel_size=(settings.vllm_tensor_parallel_size),
            )

            # #188 / Phase B: per-worker sandbox-isolation resources.
            # Allocator is always built (cheap); singleton is started
            # only when isolation is on so default-off workers don't
            # need the singleton image pulled.
            sandbox_allocator = SandboxNetworkAllocator(
                worker_index=settings.sandbox_worker_index,
            )
            sandbox_singleton: SandboxSingletonManager | None = None
            if settings.sandbox_isolation:
                sandbox_singleton = SandboxSingletonManager(
                    worker_id=worker_id,
                    image=settings.sandbox_singleton_image,
                    secrets_host_dir=settings.sandbox_singleton_secrets_dir,
                )
                try:
                    await sandbox_singleton.start()
                except SingletonStartupError as exc:
                    logger.error(
                        "sandbox_singleton_start_failed err=%s — "
                        "isolation disabled for this worker",
                        exc,
                    )
                    sandbox_singleton = None

            while not state.shutting_down:
                pool.raise_if_unhealthy()
                claimed = await _claim_available_work(
                    pool=pool,
                    settings=settings,
                    cp_client=cp_client,
                    gateway_client=gateway_client,
                    object_store=object_store,
                    worker_id=worker_id,
                    capability_snapshot_digest=(
                        str(capability_snapshot_digest)
                        if capability_snapshot_digest is not None
                        else None
                    ),
                    pipeline_run=pipeline_run,
                    vllm_registry=vllm_registry,
                    sandbox_allocator=sandbox_allocator,
                    sandbox_singleton=sandbox_singleton,
                    resource_usage_outbox=resource_usage_outbox,
                    execution_trust=execution_trust,
                )
                if image_eviction.due():
                    await asyncio.to_thread(_run_trial_cache_eviction, settings)
                should_idle_exit = idle_exit.observe(
                    claimed=claimed,
                    in_flight=pool.in_flight,
                )
                if should_idle_exit:
                    idle_exit_after = settings.idle_exit_after_seconds
                    if idle_exit_after is None:
                        raise RuntimeError("idle exit triggered while disabled")
                    logger.info(
                        "worker_idle_exit worker_id=%s idle_for_seconds=%.3f "
                        "idle_exit_after_seconds=%.3f",
                        worker_id,
                        idle_exit.idle_for_seconds,
                        idle_exit_after,
                    )
                    await _report_worker_idle_exit(cp_client, worker_id)
                    break
                if claimed == 0 or pool.in_flight >= settings.max_concurrent:
                    await asyncio.sleep(settings.claim_poll_interval_sec)
                pool.raise_if_unhealthy()

            logger.info(
                "drain_started timeout=%ss in_flight=%d",
                settings.drain_timeout_sec,
                pool.in_flight,
            )
            await pool.wait_all(timeout=float(settings.drain_timeout_sec))
            if pool.in_flight > 0:
                logger.warning(
                    "drain_timeout in_flight=%d — cancelling",
                    pool.in_flight,
                )
                pool.cancel_all()
                await pool.wait_all(timeout=60.0)
        finally:
            hb.stop()
            hb.join(timeout=10.0)
            sync_http.close()
            # PR-E: tear down any worker-spawned vLLMs on graceful
            # drain. Hard-kill via signal handlers in vllm_runner
            # covers crash paths.
            await vllm_registry.shutdown()


async def _register_worker_with_retry(
    *,
    cp_client: HttpControlPlaneClient,
    settings: WorkerSettings,
    retry_config: StartupRetryConfig = DEFAULT_STARTUP_RETRY_CONFIG,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    pipeline_enabled: bool = False,
    execution_trust: WorkerExecutionTrust | None = None,
) -> dict[str, Any]:
    if execution_trust is not None:
        execution_trust.__post_init__()
        validate_task_image_execution_origin(str(settings.control_plane_url))
        if settings.executor_worker_credential is not None:
            raise ValueError(
                "execution reader requires the authenticated legacy shared-queue assembly"
            )
    register_kwargs: dict[str, Any] = {
        "hostname": _worker_hostname(settings.hostname),
        "version": "0.0.1",
        "capabilities": _worker_capabilities(settings),
        "max_concurrent": max(1, settings.max_concurrent),
        "pool_name": settings.pool_name,
    }
    if settings.executor_worker_credential is not None:
        register_kwargs["executor_worker_credential"] = (
            settings.executor_worker_credential.get_secret_value()
        )
    if pipeline_enabled or execution_trust is not None:
        register_kwargs["supported_work_kinds"] = (
            ["trial", "execution_attempt"] if pipeline_enabled else ["trial"]
        )
        if not pipeline_enabled:
            registration = _trial_execution_registration_payload(settings)
        else:
            registration = _pipeline_registration_payload(settings)
        register_kwargs.update(registration)
        if execution_trust is not None:
            raw_snapshot = dict(registration["capability_snapshot"])
            raw_snapshot["container_runtime_features"] = sorted(
                {
                    *raw_snapshot["container_runtime_features"],
                    "task-image-execution-v2",
                }
            )
            snapshot = WorkerCapabilitySnapshotV1.model_validate_json(json.dumps(raw_snapshot))
            register_kwargs["capability_snapshot"] = snapshot.model_dump(mode="json")
            register_kwargs["capability_snapshot_digest"] = snapshot.digest
    return await retry_startup_dependency(
        lambda: cp_client.register(**register_kwargs),
        operation_name="worker control-plane registration",
        config=retry_config,
        sleep=sleep,
    )


async def _report_worker_idle_exit(
    cp_client: HttpControlPlaneClient,
    worker_id: UUID,
) -> None:
    try:
        await cp_client.heartbeat(worker_id, status="idle-exit")
    except Exception:
        logger.warning(
            "worker_idle_exit_heartbeat_failed worker_id=%s",
            worker_id,
            exc_info=True,
        )


def _run_orphan_cleanup(
    settings: WorkerSettings,
    worker_id: UUID,
    *,
    retry_config: StartupRetryConfig = DEFAULT_STARTUP_RETRY_CONFIG,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Sync HTTP lookup against /trials/{id} — invoked once at startup."""
    token_value = settings.token.get_secret_value()

    def _lookup(trial_id: UUID) -> tuple[str, UUID | None]:
        with httpx.Client(
            base_url=str(settings.control_plane_url),
            timeout=10.0,
        ) as sync_http:
            r = sync_http.get(
                f"/trials/{trial_id}",
                headers={"Authorization": f"Bearer {token_value}"},
            )
            if r.status_code == 404:
                raise LookupError(str(trial_id))
            r.raise_for_status()
            body = r.json()
            # The Control Plane's GET /trials/{id} returns `state` but
            # currently doesn't expose `worker_id`. Owner is returned
            # as None — only `state` drives the predicate now (#416).
            return body["state"], None

    retry_startup_dependency_sync(
        lambda: cleanup_orphan_trajectories(
            cache_dir=settings.trajectory_cache_dir,
            owned_worker_id=worker_id,
            state_and_owner_lookup=_lookup,
        ),
        operation_name="worker orphan trajectory startup cleanup",
        config=retry_config,
        sleep=sleep,
    )
    _run_orphan_sandbox_cleanup(
        _lookup,
        sandbox_identity=getattr(settings, "sandbox_identity", ""),
    )


def _run_orphan_sandbox_cleanup(
    lookup: Callable[[UUID], tuple[str, UUID | None]],
    *,
    sandbox_identity: str,
) -> None:
    """Best-effort sweep of Docker containers left by dead workers (#605).

    Errors are logged and swallowed — sandbox cleanup must not fail
    worker boot. The reclaim sweep will eventually re-queue any trial
    whose worker went away, and the next startup will retry the sweep.
    """
    import docker as _docker

    try:
        client = _docker.from_env()
    except Exception:
        logger.exception("orphan_sandbox_docker_client_failed")
        return
    try:
        cleanup_orphan_sandbox_containers(
            docker_client=client,
            state_lookup=lambda tid: lookup(tid)[0],
            sandbox_identity=sandbox_identity,
        )
    except Exception:
        logger.exception("orphan_sandbox_cleanup_failed")
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _run_trial_cache_eviction(settings: WorkerSettings) -> None:
    """Best-effort prune of all Loom-managed images.

    Docker errors are logged and swallowed by the shared eviction helper;
    opportunistic cleanup must not fail worker boot or claim processing.
    """
    evict_stale_managed_images_from_env(settings)


async def _ensure_runtime_buckets(
    object_store: ObjectStore,
    *,
    trajectories_bucket: str = "trajectories",
    artifacts_bucket: str = "artifacts",
) -> None:
    for bucket in (trajectories_bucket, artifacts_bucket):
        try:
            await object_store.ensure_bucket(bucket)
        except Exception:
            logger.exception("runtime_bucket_ensure_failed bucket=%s", bucket)
            raise
        logger.info("runtime_bucket_ensured bucket=%s", bucket)


async def _claim_available_trials(
    *,
    pool: RunnerPool,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    gateway_client: HttpLLMGatewayClient,
    object_store: ObjectStore,
    worker_id: UUID,
    vllm_registry: WorkerVLLMRegistry,
    sandbox_allocator: SandboxNetworkAllocator | None = None,
    sandbox_singleton: SandboxSingletonManager | None = None,
    read_setup_health: Callable[[], Any] | None = None,
    resource_usage_outbox: ResourceUsageOutbox | None = None,
) -> int:
    from loom_worker.setup_admission import (
        policy_from_settings,
        read_node_health_snapshot,
    )

    claimed = 0
    setup_health_policy = policy_from_settings(settings)
    read_health_snapshot = read_setup_health or read_node_health_snapshot
    while pool.in_flight < settings.max_concurrent:
        pool.raise_if_unhealthy()
        health = setup_health_policy.evaluate(read_health_snapshot())
        if not health.ok:
            logger.warning(
                "trial_claim_paused_node_setup_health worker_id=%s reason=%s detail=%s",
                worker_id,
                health.reason,
                health.detail,
            )
            break
        try:
            trial_payload = await cp_client.claim(
                worker_id=worker_id,
                caps=_worker_capabilities(settings),
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "trial_claim_transient_error worker_id=%s err=%s",
                worker_id,
                exc,
            )
            break
        if trial_payload is None:
            break
        # A concurrently finishing attempt can poison this process while the
        # claim request is in flight. Never start the raced claim here; the CP
        # lease reclaimer will make it available to a fresh worker.
        pool.raise_if_unhealthy()
        await _spawn_trial(
            pool=pool,
            settings=settings,
            cp_client=cp_client,
            gateway_client=gateway_client,
            object_store=object_store,
            worker_id=worker_id,
            payload=trial_payload,
            vllm_registry=vllm_registry,
            sandbox_allocator=sandbox_allocator,
            sandbox_singleton=sandbox_singleton,
            resource_usage_outbox=resource_usage_outbox,
        )
        claimed += 1
    return claimed


async def _claim_available_work(
    *,
    pool: RunnerPool,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    gateway_client: HttpLLMGatewayClient,
    object_store: ObjectStore,
    worker_id: UUID,
    capability_snapshot_digest: str | None,
    pipeline_run: Callable[[ExecutionAttemptClaimV1], Coroutine[Any, Any, None]] | None,
    vllm_registry: WorkerVLLMRegistry,
    sandbox_allocator: SandboxNetworkAllocator | None = None,
    sandbox_singleton: SandboxSingletonManager | None = None,
    read_setup_health: Callable[[], Any] | None = None,
    resource_usage_outbox: ResourceUsageOutbox | None = None,
    execution_trust: WorkerExecutionTrust | None = None,
) -> int:
    """Use measured shared claims for Pipeline or an explicitly trusted Trial reader.

    Without either assembly, preserve the old Trial endpoint. A trusted reader
    without a Pipeline runner requests only Trials and must never dispatch attempts.
    """

    if (pipeline_run is None and execution_trust is None) or capability_snapshot_digest is None:
        if execution_trust is not None:
            raise ValueError("trusted task images require authenticated shared-queue claims")
        return await _claim_available_trials(
            pool=pool,
            settings=settings,
            cp_client=cp_client,
            gateway_client=gateway_client,
            object_store=object_store,
            worker_id=worker_id,
            vllm_registry=vllm_registry,
            sandbox_allocator=sandbox_allocator,
            sandbox_singleton=sandbox_singleton,
            read_setup_health=read_setup_health,
            resource_usage_outbox=resource_usage_outbox,
        )

    from loom_worker.setup_admission import (
        policy_from_settings,
        read_node_health_snapshot,
    )

    claimed = 0
    setup_health_policy = policy_from_settings(settings)
    read_health_snapshot = read_setup_health or read_node_health_snapshot
    while pool.in_flight < settings.max_concurrent:
        pool.raise_if_unhealthy()
        health = setup_health_policy.evaluate(read_health_snapshot())
        if not health.ok:
            logger.warning(
                "work_claim_paused_node_setup_health worker_id=%s reason=%s detail=%s",
                worker_id,
                health.reason,
                health.detail,
            )
            break
        try:
            envelope = await cp_client.claim_work(
                worker_id=worker_id,
                capability_snapshot_digest=capability_snapshot_digest,
                free_slots=settings.max_concurrent - pool.in_flight,
                supported_work_kinds=["trial", "execution_attempt"]
                if pipeline_run is not None
                else ["trial"],
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "work_claim_transient_error worker_id=%s err=%s",
                worker_id,
                exc,
            )
            break
        if envelope is None:
            break
        pool.raise_if_unhealthy()
        # The HTTP boundary contains JSON UUID and timestamp strings.  Validate
        # through Pydantic's JSON path so strict in-process construction remains
        # closed without rejecting the canonical transport representation.
        raw_payload = envelope.get("payload")
        parsed: WorkClaimV1 | SignedWorkClaim
        if isinstance(raw_payload, dict) and raw_payload.get("task_image_execution") is not None:
            parsed = SignedWorkClaim.model_validate_json(canonical_document(envelope))
        else:
            parsed = WorkClaimV1.model_validate_json(canonical_document(envelope))
        payload = parsed.payload
        if parsed.work_kind == "trial":
            await _spawn_trial(
                pool=pool,
                settings=settings,
                cp_client=cp_client,
                gateway_client=gateway_client,
                object_store=object_store,
                worker_id=worker_id,
                payload=payload.model_dump(mode="json", by_alias=True),
                vllm_registry=vllm_registry,
                sandbox_allocator=sandbox_allocator,
                sandbox_singleton=sandbox_singleton,
                resource_usage_outbox=resource_usage_outbox,
                execution_trust=execution_trust,
            )
        elif parsed.work_kind == "execution_attempt":
            if pipeline_run is None or not isinstance(payload, ExecutionAttemptClaimV1):
                raise RuntimeError("Control Plane returned a mismatched Pipeline claim")
            await pool.spawn(pipeline_run(payload))
        else:
            raise RuntimeError("Control Plane returned an unknown work kind")
        claimed += 1
    return claimed


async def _prepare_family_state_mount_if_any(
    *,
    payload: dict[str, Any],
    settings: WorkerSettings,
    trial_id: UUID,
    object_store: ObjectStore,
) -> Any:
    """Download the family-run state tarball when the claim payload
    carries a ``family_state_uri`` (#672 PR-3).

    Returns None for classic trials so the runner picks up an empty
    ``family_state_volumes`` tuple. Raises are propagated so the trial
    is marked ``failed`` via the outer ``_mark_setup_failed`` path —
    a family-run trial that can't hydrate its shared state has no way
    to run correctly.
    """
    from loom.family_run.prestart import (
        FamilyStateMount,
        prepare_family_state_mount,
    )
    from loom.family_run.state_backends import S3ArtifactsStateBackend

    state_uri = payload.get("family_state_uri")
    if not state_uri:
        return None
    family_run_spec = payload.get("family_run_spec") or {}
    state_backend_ref = family_run_spec.get("state_backend") or {}
    backend_name = state_backend_ref.get("name") or "s3_artifacts"
    if backend_name != "s3_artifacts":
        # Only the built-in backend is wired in v1; when a caller ships
        # a custom entry-point the plugin will need to be resolvable
        # from the worker's python env. For now we fail loud instead
        # of silently mounting an empty dir.
        raise RuntimeError(
            f"unsupported family_run state_backend {backend_name!r}; "
            "only 's3_artifacts' is wired in the worker path",
        )
    # Use the same configured artifacts bucket ensured at worker startup
    # (see _ensure_runtime_buckets) so family-state objects land with
    # loom-service / control-plane.
    backend = S3ArtifactsStateBackend(
        store=object_store,
        bucket=settings.artifacts_bucket,
    )
    mount_path = family_run_spec.get("mount_path") or "/root/.skills"
    timeout_sec = getattr(
        settings,
        "family_state_download_timeout_sec",
        120.0,
    )
    mount: FamilyStateMount = await prepare_family_state_mount(
        trial_id=str(trial_id),
        state_uri=state_uri,
        mount_path=mount_path,
        state_backend=backend,
        backend_params=state_backend_ref.get("params") or {},
        download_timeout_sec=timeout_sec,
    )
    return mount


async def _resolve_layered_trial_image(
    *,
    task_image: str,
    agent_name: str,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    worker_id: UUID,
) -> str:
    """Look up the agent adapter and, if it declares an install_script,
    return the cached layered image (or build it). Returns `task_image`
    unchanged for agents without an install_script (oracle, direct completion,
    or adapters that haven't declared an install_script yet)."""
    # Built-in agents aren't in the launcher registry
    # and don't need an install step. Skip.
    if agent_name in {"oracle", "direct-completion", "litellm", "terminus-2"}:
        return task_image
    adapter_name = agent_name
    try:
        from loom_launcher import get_adapter
    except ImportError:
        return task_image
    adapter = get_adapter(adapter_name)
    if adapter is None:
        # Unknown name — let `_default_agent_factory` raise AgentError
        # at agent-spawn time with the existing "unknown agent.name"
        # message. Don't duplicate that here.
        return task_image
    return await resolve_trial_image(
        task_image=task_image,
        adapter=adapter,
        settings=settings,
        cp_client=cp_client,
        worker_id=worker_id,
    )


async def _spawn_trial(
    *,
    pool: RunnerPool,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    gateway_client: HttpLLMGatewayClient,
    object_store: ObjectStore,
    worker_id: UUID,
    payload: dict[str, Any],
    vllm_registry: WorkerVLLMRegistry,
    sandbox_allocator: SandboxNetworkAllocator | None = None,
    sandbox_singleton: SandboxSingletonManager | None = None,
    resource_usage_outbox: ResourceUsageOutbox | None = None,
    execution_trust: WorkerExecutionTrust | None = None,
) -> None:
    trial_id = UUID(str(payload["trial_id"]))
    team_id = UUID(str(payload["team_id"]))
    raw_attempt_count = payload.get("attempt_count")
    if type(raw_attempt_count) is not int or raw_attempt_count <= 0:
        raise ValueError("attempt_count must be a positive integer")
    attempt_count = raw_attempt_count
    usage_outbox = resource_usage_outbox or ResourceUsageOutbox(
        settings.trajectory_cache_dir / "resource-usage-outbox",
    )

    async def _setup_run_and_cleanup() -> None:
        task_dir: Path | None = None
        trial_config: TrialConfig | None = None
        trusted_execution: WorkerTaskImageExecution | None = None
        pre_start_heartbeat_task = asyncio.create_task(
            _run_pre_start_heartbeat(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                interval_sec=settings.pre_start_heartbeat_interval_sec,
            )
        )
        try:
            trial_config = TrialConfig.model_validate(payload.get("config") or {})
            raw_task_image_materialization = payload.get("task_image_materialization")
            task_image_materialization = (
                TaskImageExecutionGrantV1.model_validate(raw_task_image_materialization)
                if raw_task_image_materialization is not None
                else None
            )
            raw_execution = payload.get("task_image_execution")
            delivery: TaskImageExecutionDelivery | None = None
            if raw_execution is not None:
                if execution_trust is None or task_image_materialization is not None:
                    raise ValueError(
                        "signed execution requires release trust and forbids V1 fallback"
                    )
                delivery = TaskImageExecutionDelivery.model_validate_json(json.dumps(raw_execution))
                expected = delivery.claim
                if (
                    expected.trial_id != str(trial_id)
                    or expected.team_id != str(team_id)
                    or expected.worker_id != str(worker_id)
                    or expected.trial_attempt_count != attempt_count
                ):
                    raise ValueError(
                        "signed execution delivery differs from authenticated worker claim"
                    )
                verified = verify_execution_grant(
                    wire=delivery.grant_envelope.encode(),
                    plan_wire=delivery.frozen_plan.encode(),
                    publication_wires=tuple(item.encode() for item in delivery.publications),
                    keyset_wire=delivery.keyset.encode(),
                    trust_root=execution_trust.root,
                    expected_claim=expected,
                    expected_purpose=execution_trust.purpose,
                    expected_shadow_campaign_id=execution_trust.shadow_campaign_id,
                    now=execution_trust.clock(),
                )
                frozen_task, frozen_provenance = verified.grant.snapshots()
                if TaskConfig.model_validate(frozen_task).task.id != str(payload["task_id"]):
                    raise ValueError("signed execution task differs from claim")
                # Reuse the existing pull-only preparation adapter, retaining
                # the full signed evidence separately for the online start.
                # This object is never the V2 runtime's start authority.
                task_image_materialization = TaskImageExecutionGrantV1.model_validate(
                    dict(
                        schema_version="loom.task-image-execution-grant.v1",
                        materialization_id=verified.grant.materialization_id,
                        materialization_key=verified.grant.materialization_key,
                        cpu_arch=verified.grant.cpu_arch,
                        task_checksum=verified.grant.task_checksum,
                        task_config=frozen_task,
                        task_source=verified.grant.task_source,
                        task_source_provenance=frozen_provenance,
                        registry_images=dict(verified.registry_images),
                    )
                )
            if task_image_materialization is None:
                bundle = await cp_client.get_task_bundle(str(payload["task_id"]))
            else:
                execution_arch = _host_cpu_arch()
                if task_image_materialization.cpu_arch != execution_arch:
                    raise TaskImageBuildError(
                        "task image materialization architecture mismatch "
                        f"expected={execution_arch} "
                        f"granted={task_image_materialization.cpu_arch}"
                    )
                bundle = {
                    "id": str(payload["task_id"]),
                    "checksum": task_image_materialization.task_checksum,
                    "config": task_image_materialization.task_config,
                    "source": task_image_materialization.task_source,
                    "source_provenance": (task_image_materialization.task_source_provenance),
                }
            task_config = TaskConfig.model_validate(bundle["config"])
            task_checksum = str(bundle["checksum"])
            raw_provenance = bundle.get("source_provenance")
            provenance = raw_provenance if isinstance(raw_provenance, dict) else {}
            raw_policy = provenance.get("workspace_staging_policy")
            # #1263: trial_config.workspace_staging_policy_name (from batch
            # create CLI) wins over provenance; TB2.1-r6 still fail-closed.
            workspace_staging_policy = resolve_trial_workspace_staging_policy(
                policy_name=trial_config.workspace_staging_policy_name,
                task_id=task_config.task.id,
                raw_provenance_policy=raw_policy,
            )
            logger.info(
                "trial %s workspace_staging_policy_name=%s applied=%s",
                trial_id,
                trial_config.workspace_staging_policy_name,
                workspace_staging_policy is not None,
            )

            # Plan 13 Task 3: materialize the fixture content from
            # bundle["source"] when it's an s3:// URL (benchmark-imported
            # tasks). Hand-authored tasks with source=None or git+... still
            # get an empty tempdir; the operator runbook documents the
            # volume-mount / git-clone alternatives.
            task_dir = await _materialize_task_dir(
                bundle=bundle,
                object_store=object_store,
                trial_id=trial_id,
                fixtures_root=settings.fixtures_root,
                benchmark_cache=settings.benchmark_cache,
                timeout_sec=settings.task_materialize_timeout_sec,
            )
            image_cache_checksum = verified_task_image_cache_identity(
                task_dir,
                task_checksum=task_checksum,
                source_provenance=provenance,
            )
            # A different cache identity means strong capture already verified
            # the full files/checksum/modes. Legacy grants retain their old checks.
            if task_image_materialization is not None and image_cache_checksum == task_checksum:
                actual_checksum = sha256_of_dir(task_dir)
                if actual_checksum != task_image_materialization.task_checksum:
                    raise TaskImageBuildError(
                        "materialized task bundle checksum mismatch "
                        f"expected={task_image_materialization.task_checksum} "
                        f"actual={actual_checksum}"
                    )
                expected_metadata_checksum = task_image_materialization.task_source_provenance.get(
                    "bundle_file_metadata_sha256"
                )
                if expected_metadata_checksum is not None:
                    actual_metadata_checksum = bundle_file_metadata_sha256(task_dir)
                    if actual_metadata_checksum != expected_metadata_checksum:
                        raise TaskImageBuildError(
                            "materialized task bundle metadata mismatch "
                            f"expected={expected_metadata_checksum} "
                            f"actual={actual_metadata_checksum}"
                        )
            if task_config.task.id.startswith("terminal-bench-2@tb2.1-r6/"):
                _verify_materialized_tb21_bundle_checksum(
                    task_dir=task_dir,
                    expected_checksum=task_checksum,
                    source_provenance=provenance,
                )
            validate_task_dir_compatibility(task_dir)
            # Optional shared-cache pulls may fall back to a local build after
            # a short timeout. A materialization grant is different: its
            # immutable digest is the only executable image, so give that base
            # image the existing long pull window instead of failing closed
            # after the short cache-probe window.
            registry_pull_timeout_sec = (
                getattr(settings, "trial_cache_base_image_pull_timeout_sec", 1800.0)
                if task_image_materialization is not None
                else getattr(settings, "trial_cache_registry_pull_timeout_sec", 15.0)
            )
            # #275: serialize concurrent task-image builds so a burst of
            # trials cannot fan out unbounded apt-get / dpkg / build
            # containers on the same local Docker daemon.
            task_image = await resolve_task_image(
                task_config=task_config,
                task_dir=task_dir,
                task_checksum=image_cache_checksum,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
                build_slot_provider=lambda: _daemon_build_slot(
                    cp_client,
                    settings,
                    worker_id,
                ),
                registry_repo=(getattr(settings, "trial_cache_registry_repo", "") or None),
                registry_image=(
                    task_image_materialization.registry_images.get("task")
                    if task_image_materialization is not None
                    else None
                ),
                registry_pull_timeout_sec=registry_pull_timeout_sec,
                cpu_arch=_host_cpu_arch(),
                build_if_missing=False,
            )
            # #317 Phase 1: optional Docker-local agent layer.
            if delivery is None:
                task_image = await _resolve_layered_trial_image(
                    task_image=task_image,
                    agent_name=trial_config.agent_name,
                    settings=settings,
                    cp_client=cp_client,
                    worker_id=worker_id,
                )
            else:
                assert execution_trust is not None
                trusted_execution = WorkerTaskImageExecution(
                    wire=delivery.grant_envelope.encode(),
                    plan_wire=delivery.frozen_plan.encode(),
                    publication_wires=tuple(item.encode() for item in delivery.publications),
                    keyset_wire=delivery.keyset.encode(),
                    trust_root=execution_trust.root,
                    expected_claim=delivery.claim,
                    expected_purpose=execution_trust.purpose,
                    expected_shadow_campaign_id=execution_trust.shadow_campaign_id,
                    task_dir=task_dir,
                    task_config=task_config,
                    task_checksum=task_checksum,
                    cpu_arch=_host_cpu_arch(),
                    task_image=task_image,
                    consume=cp_client.consume_task_image_execution_start,
                    refresh=cp_client.refresh_task_image_execution,
                    clock=execution_trust.clock,
                )
                await trusted_execution.prepare()
        except Exception as exc:
            if task_dir is not None:
                shutil.rmtree(task_dir, ignore_errors=True)
            await _mark_setup_failed(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                detail=str(exc),
                diagnostic_detail=getattr(exc, "diagnostic_detail", None),
                setup_diagnostics_root=(settings.trajectory_cache_dir / "setup-diagnostics"),
                retry_policy=trial_config.retry if trial_config is not None else None,
                attempt_count=attempt_count,
            )
            return
        finally:
            pre_start_heartbeat_task.cancel()
            try:
                await pre_start_heartbeat_task
            except asyncio.CancelledError:
                pass

        async def _state_patch(
            state: str,
            fr: str | None,
            fm: str | None = None,
        ) -> bool:
            return await cp_client.patch_state(
                trial_id=trial_id,
                worker_id=worker_id,
                state=state,
                failure_reason=fr,
                failure_message=fm,
            )

        async def _output_projection(
            result_payload: dict[str, object],
            trajectory_index: dict[str, object],
        ) -> bool:
            return await cp_client.patch_output_projection(
                trial_id=trial_id,
                worker_id=worker_id,
                result=result_payload,
                trajectory_index=trajectory_index,
            )

        # #5 Slice 3b: per-trial CP event sink. The send callback is
        # a thin shim around `cp_client.append_events` that captures
        # trial_id + worker_id; the sink owns batching + flush
        # scheduling internally. MinIO is still authoritative in
        # this slice — sink errors are swallowed.
        async def _send_event_batch(
            events: list[dict[str, object]],
        ) -> bool:
            return await cp_client.append_events(
                trial_id=trial_id,
                worker_id=worker_id,
                events=events,
            )

        trial_cp_event_sink = CpEventSink(
            trial_id=trial_id,
            worker_id=worker_id,
            send_batch=_send_event_batch,
        )

        subprocess_gateway_url = getattr(settings, "subprocess_gateway_url", None)
        subprocess_gateway_url_str = (
            str(subprocess_gateway_url) if subprocess_gateway_url is not None else None
        )

        # #672 PR-3: when the CP claim payload carries a family_state_uri,
        # download the shared skills tarball into a per-trial staging
        # dir and hand the (host, container, mode) volume tuple to the
        # runner. Cleanup lives in the outer finally block below.
        family_state_mount = await _prepare_family_state_mount_if_any(
            payload=payload,
            settings=settings,
            trial_id=trial_id,
            object_store=object_store,
        )
        family_state_volumes: tuple[tuple[str, str, str], ...] = ()
        if family_state_mount is not None:
            family_state_volumes = (family_state_mount.as_volume_tuple(),)

        hard_deadline_sec = resolve_hard_deadline_sec(
            task_config=task_config,
            trial_config=trial_config,
            multiplier=settings.trial_hard_deadline_multiplier,
            grace_sec=settings.trial_hard_deadline_grace_sec,
        )

        async def _resource_usage_sink(
            report: TrialResourceUsageReport,
            final: bool,
        ) -> None:
            if final:
                await usage_outbox.stage_and_deliver(
                    report,
                    cp_client.report_resource_usage,
                )
            else:
                await usage_outbox.stage(report)

        driver_sequence = 0

        def _driver_factory() -> ResourceAccountingDriver:
            nonlocal driver_sequence
            ordinal = driver_sequence
            driver_sequence += 1
            inner = DockerDriver(
                image=task_image,
                workspace=task_config.environment.workdir,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
            )
            role = "agent" if ordinal == 0 else "verifier"
            return ResourceAccountingDriver(
                inner,
                trial_id=trial_id,
                attempt_count=attempt_count,
                worker_id=worker_id,
                execution_key=execution_key(
                    trial_id,
                    attempt_count,
                    worker_id,
                    role,
                    ordinal,
                ),
                container_role=role,
                role_name="primary" if role == "agent" else f"isolated-{ordinal}",
                architecture=_host_cpu_arch(),
                candidate_sha=(getattr(settings, "candidate_sha", "") or None),
                sink=_resource_usage_sink,
            )

        def _docker_sidecar_runtime() -> DockerTaskSidecarRuntime:
            return DockerTaskSidecarRuntime(
                task_config=task_config,
                task_dir=task_dir,
                task_checksum=image_cache_checksum,
                trial_id=trial_id,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
                container_cpus=settings.container_cpus,
                container_memory_mib=settings.container_memory_mib,
                container_pids=settings.container_pids,
                container_cgroup_parent=_worker_cgroup_parent(settings),
                runtime_identity_labels=_runtime_identity_labels(settings),
                cpu_arch=_host_cpu_arch(),
                registry_repo=(getattr(settings, "trial_cache_registry_repo", "") or None),
                registry_images=(
                    task_image_materialization.registry_images
                    if task_image_materialization is not None
                    else None
                ),
                materialized_image_pull_timeout_sec=getattr(
                    settings, "trial_cache_base_image_pull_timeout_sec", 1800.0
                ),
                pull_only=True,
                attempt_count=attempt_count,
                worker_id=worker_id,
                candidate_sha=(getattr(settings, "candidate_sha", "") or None),
                usage_sink=_resource_usage_sink,
                setup_slot_provider=lambda: _daemon_build_slot(
                    cp_client,
                    settings,
                    worker_id,
                ),
            )

        runner = LocalTrialRunner(
            trial_id=trial_id,
            team_id=team_id,
            attempt_count=attempt_count,
            task_config=task_config,
            task_checksum=task_checksum,
            task_dir=task_dir,
            trial_config=trial_config,
            driver_factory=_driver_factory,
            agent_factory=_default_agent_factory(
                team_id,
                trial_id,
                cp_client=cp_client,
                worker_gateway_url=str(settings.gateway_url),
                sandbox_gateway_url=subprocess_gateway_url_str,
                provider_connection_id=payload.get("provider_connection_id"),
            ),
            verifier_factory=_verifier_factory(task_config),
            object_store=object_store,
            gateway_client=gateway_client,
            local_trajectory_root=settings.trajectory_cache_dir,
            trajectory_bucket=settings.trajectories_bucket,
            artifacts_bucket=settings.artifacts_bucket,
            state_patch_callback=_state_patch,
            output_projection_callback=_output_projection,
            # A11.1: query CP for the trial's llm_calls rows at finalize,
            # project each into an LLMCallEvent. No-op for trials that
            # don't route through the Gateway (oracle, in-box runtimes).
            llm_calls_fetcher=cp_client.get_trial_llm_calls,
            # #5 Slice 3b: mirror typed trajectory events to the CP
            # `trial_events` table alongside the existing MinIO writer.
            # Slice 3c will flip the SSE reader from MinIO-poll to
            # Postgres-LISTEN, at which point sink failures start
            # mattering more; for now MinIO remains authoritative
            # and the sink swallows its own errors.
            cp_event_sink=trial_cp_event_sink,
            model_switch_plan=payload.get("model_switch_plan"),
            # PR-E: worker-spawned vLLM registry. Shared across trials
            # claimed by this worker process so the 1-3 min vLLM startup
            # is amortised across many same-model trials.
            vllm_registry=vllm_registry,
            # #188 / Phase B: per-trial sandbox isolation. Both None →
            # legacy direct-network behavior. Both populated → per-trial
            # bridge + singleton attach.
            sandbox_allocator=(sandbox_allocator if sandbox_singleton is not None else None),
            sandbox_singleton=sandbox_singleton,
            # Phase D: step-JWT minting + rotation. Only wired when
            # singleton started (isolation on); the mint_callback closes
            # over `cp_client` so the rotator hits POST /admin/step-tokens
            # with a stable step_id (per-step attribution is a future
            # improvement — for now sandbox isolation prioritizes session
            # continuity over per-step cost attribution).
            sandbox_mint_token=(
                _build_mint_callback(cp_client, team_id) if sandbox_singleton is not None else None
            ),
            sandbox_secrets_root=(
                Path(settings.sandbox_singleton_secrets_dir) / "trials"
                if sandbox_singleton is not None
                else None
            ),
            sandbox_step_jwt_ttl_sec=settings.sandbox_step_jwt_ttl_sec,
            sandbox_extra_hosts=_sandbox_extra_hosts_for_url(subprocess_gateway_url_str),
            family_state_volumes=family_state_volumes,
            workspace_staging_policy=workspace_staging_policy,
            # Apply configured local resource limits to both the trial container
            # (via TrialContext → StartOptions) and the setup sidecars.
            container_cpus=settings.container_cpus,
            container_memory_mib=settings.container_memory_mib,
            container_pids=settings.container_pids,
            container_cgroup_parent=_worker_cgroup_parent(settings),
            runtime_identity_labels=_runtime_identity_labels(settings),
            sidecar_runtime_factory=_docker_sidecar_runtime,
            start_authorization=(
                trusted_execution.authorize if trusted_execution is not None else None
            ),
        )

        # #360 + #378: wrap the runner with the cancellation watchdog so
        # (a) operator-initiated CP cancellations propagate to the
        # in-container subprocess-agent within one poll interval and
        # (b) trials that hang past a generous multiple of the resolved
        # effective agent timeout get force-cancelled instead of running
        # indefinitely.
        try:
            await run_with_watchdog(
                runner.run(),
                trial_id=trial_id,
                cp_client=cp_client,
                owner_worker_id=worker_id,
                claimed_attempt_count=attempt_count,
                poll_interval_sec=settings.trial_cancel_poll_interval_sec,
                hard_deadline_sec=hard_deadline_sec,
                on_cancel=runner.interrupt_attempt,
            )
        except asyncio.CancelledError:
            # Watchdog fired (cancel / ownership revoke / hard_deadline).
            # Trial.run's own CancelledError handler already recorded what
            # it could; propagate so the outer task cleanup frees the seat.
            raise
        except WorkerUnhealthyError:
            logger.critical(
                "worker_unhealthy_after_agent_timeout trial_id=%s worker_id=%s",
                trial_id,
                worker_id,
            )
            raise
        except Exception as exc:
            logger.exception(
                "trial_runner_failed trial_id=%s worker_id=%s",
                trial_id,
                worker_id,
            )
            await _mark_setup_failed(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                detail=str(exc),
                diagnostic_detail=getattr(exc, "diagnostic_detail", None),
                setup_diagnostics_root=(settings.trajectory_cache_dir / "setup-diagnostics"),
                retry_policy=trial_config.retry,
                attempt_count=attempt_count,
            )
        finally:
            shutil.rmtree(task_dir, ignore_errors=True)
            # #672 PR-3: release the family-state staging dir. The
            # runner uses the bind-mount for the trial's lifetime;
            # after the sandbox stops, the tarball has been re-uploaded
            # by any subsequent adapter.evolve call, so the local copy
            # can go.
            if family_state_mount is not None:
                family_state_mount.cleanup()

    await pool.spawn(_setup_run_and_cleanup(), key=str(trial_id))


async def _run_pre_start_heartbeat(
    *,
    cp_client: HttpControlPlaneClient,
    trial_id: UUID,
    worker_id: UUID,
    interval_sec: float,
) -> None:
    interval = max(0.001, float(interval_sec))
    while True:
        try:
            accepted = await cp_client.pre_start_heartbeat(
                trial_id=trial_id,
                worker_id=worker_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "pre_start_heartbeat_error trial_id=%s worker_id=%s err=%s",
                trial_id,
                worker_id,
                exc,
            )
        else:
            if not accepted:
                logger.warning(
                    "pre_start_heartbeat_fenced trial_id=%s worker_id=%s",
                    trial_id,
                    worker_id,
                )
                return
        await asyncio.sleep(interval)


def _verifier_factory(task_config: TaskConfig) -> Callable[[], Verifier]:
    name = task_config.verifier.name
    ctor = _VERIFIER_CTORS.get(name)
    if ctor is None:
        raise ValueError(f"unknown verifier: {name!r}")
    args = dict(task_config.verifier.args)
    return lambda: ctor(**args)


def _build_mint_callback(
    cp_client: StepTokenClient,
    team_id: UUID,
) -> Callable[[UUID], Awaitable[str]]:
    """Closure the rotator calls on each tick. Stable step_id
    `"sandbox-rotated"` because Phase D's primary goal is sandbox
    isolation; per-step cost-attribution refinements are a follow-up."""

    async def _mint(trial_id: UUID) -> str:
        return await cp_client.mint_step_token(
            team_id=team_id,
            trial_id=trial_id,
            step_id="sandbox-rotated",
            ttl_sec=600,
        )

    return _mint


async def _mark_setup_failed(
    *,
    cp_client: HttpControlPlaneClient,
    trial_id: UUID,
    worker_id: UUID,
    detail: str,
    diagnostic_detail: str | None = None,
    setup_diagnostics_root: Path | None = None,
    retry_policy: RetryPolicy | None = None,
    attempt_count: int | None = None,
) -> None:
    safe_diagnostic_detail = redact_text(diagnostic_detail or detail).strip()
    diagnostic_path = _write_setup_failure_diagnostic(
        safe_diagnostic_detail=safe_diagnostic_detail,
        trial_id=trial_id,
        setup_diagnostics_root=setup_diagnostics_root,
    )
    safe_detail = _format_setup_failure_detail(
        detail,
        diagnostic_path=diagnostic_path,
    )
    classified_text = classify_failure_message(safe_detail)
    if classified_text is not None:
        failure_reason, classified_message = classified_text
        if classified_message:
            safe_detail = classified_message
    else:
        failure_reason = _classify_setup_failure(safe_detail)
    logger.warning(
        "trial_setup_failed trial_id=%s worker_id=%s detail=%s",
        trial_id,
        worker_id,
        safe_detail,
    )
    retry_after_sec = _setup_retry_after_seconds(
        failure_reason=failure_reason,
        retry_policy=retry_policy,
        attempt_count=attempt_count,
    )
    if retry_after_sec is not None:
        try:
            ok = await cp_client.requeue_trial_retry(
                trial_id=trial_id,
                worker_id=worker_id,
                failure_reason=failure_reason.value,
                failure_message=safe_detail,
                retry_after_sec=retry_after_sec,
            )
        except Exception:
            logger.exception(
                "trial_setup_retry_requeue_error trial_id=%s",
                trial_id,
            )
            ok = False
        if ok:
            logger.info(
                "trial_setup_retry_requeued trial_id=%s worker_id=%s "
                "failure_reason=%s attempt_count=%s retry_after_sec=%.3f",
                trial_id,
                worker_id,
                failure_reason.value,
                attempt_count,
                retry_after_sec,
            )
            return
        logger.warning(
            "trial_setup_retry_requeue_unaccepted trial_id=%s worker_id=%s",
            trial_id,
            worker_id,
        )
    try:
        ok = await cp_client.patch_state(
            trial_id=trial_id,
            worker_id=worker_id,
            state="failed",
            failure_reason=failure_reason.value,
            failure_message=safe_detail,
        )
    except Exception:
        logger.exception(
            "trial_setup_failed_state_patch_error trial_id=%s",
            trial_id,
        )
        return
    if not ok:
        logger.warning(
            "trial_setup_failed_state_patch_fenced trial_id=%s worker_id=%s",
            trial_id,
            worker_id,
        )


def _write_setup_failure_diagnostic(
    *,
    safe_diagnostic_detail: str,
    trial_id: UUID,
    setup_diagnostics_root: Path | None,
) -> Path | None:
    if setup_diagnostics_root is None:
        return None
    if len(safe_diagnostic_detail) <= _SETUP_FAILURE_MESSAGE_LIMIT:
        return None

    diagnostic_path = setup_diagnostics_root / f"{trial_id}.log"
    try:
        diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
        diagnostic_path.write_text(safe_diagnostic_detail + "\n", encoding="utf-8")
    except OSError:
        logger.warning(
            "trial_setup_failed_diagnostic_write_failed trial_id=%s path=%s",
            trial_id,
            diagnostic_path,
            exc_info=True,
        )
        return None
    return diagnostic_path


def _format_setup_failure_detail(
    detail: str,
    *,
    diagnostic_path: Path | None = None,
) -> str:
    safe_detail = redact_text(detail).strip()
    diagnostic_path_line = (
        f"\nfull_setup_diagnostic_path: {diagnostic_path}" if diagnostic_path is not None else ""
    )
    if len(safe_detail) + len(diagnostic_path_line) <= _SETUP_FAILURE_MESSAGE_LIMIT:
        if diagnostic_path_line:
            return f"{safe_detail}{diagnostic_path_line}"
        return safe_detail

    max_head_budget = min(
        _SETUP_FAILURE_HEAD_CHARS,
        _SETUP_FAILURE_MESSAGE_LIMIT - len(_SETUP_FAILURE_TRUNCATION_MARKER) - 1,
    )
    head = _setup_failure_diagnostic_head(safe_detail, max_head_budget)
    if diagnostic_path_line:
        head = f"{head}{diagnostic_path_line}"
        if len(head) + len(_SETUP_FAILURE_TRUNCATION_MARKER) >= (_SETUP_FAILURE_MESSAGE_LIMIT):
            head_budget = _SETUP_FAILURE_MESSAGE_LIMIT - len(_SETUP_FAILURE_TRUNCATION_MARKER) - 1
            head = head[:head_budget].rstrip()
    tail_budget = _SETUP_FAILURE_MESSAGE_LIMIT - len(head) - len(_SETUP_FAILURE_TRUNCATION_MARKER)
    tail = safe_detail[-tail_budget:].lstrip()
    first_newline = tail.find("\n")
    if 0 < first_newline < len(tail) - 1:
        tail = tail[first_newline + 1 :]
    compact = f"{head}{_SETUP_FAILURE_TRUNCATION_MARKER}{tail.lstrip()}"
    return compact[:_SETUP_FAILURE_MESSAGE_LIMIT].strip()


def _setup_failure_diagnostic_head(detail: str, max_chars: int) -> str:
    build_log_index = detail.lower().find("\nbuild log")
    if build_log_index >= 0:
        build_log_line_end = detail.find("\n", build_log_index + 1)
        if 0 < build_log_line_end <= max_chars:
            return detail[:build_log_line_end].rstrip()
    return detail[:max_chars].rstrip()


def _classify_setup_failure(detail: str) -> FailureReason:
    if "SETUP_ADMISSION_BLOCKED" in detail:
        return FailureReason.NODE_SETUP_HEALTH
    if "TASK_COMPAT_" in detail:
        return FailureReason.TASK_COMPATIBILITY
    if "building Docker image" in detail and " from " in detail and " exceeded " in detail:
        return FailureReason.TASK_IMAGE_BUILD_TIMEOUT
    if (
        "pulling materialized task image" in detail
        or "pulling materialized sidecar image" in detail
    ) and " exceeded " in detail:
        return FailureReason.TASK_IMAGE_BUILD_TIMEOUT
    lowered = detail.lower()
    if "failed to build layered image" in lowered and (
        "temporary failure resolving" in lowered
        or "could not resolve host" in lowered
        or "name or service not known" in lowered
    ):
        return FailureReason.TASK_COMPATIBILITY
    return FailureReason.INTERNAL_ERROR


def _setup_retry_reason_for_failure(reason: FailureReason) -> RetryReason | None:
    if reason == FailureReason.NODE_SETUP_HEALTH:
        return RetryReason.NODE_SETUP_HEALTH
    if reason == FailureReason.PROVIDER_TRANSPORT_DISCONNECT:
        return RetryReason.PROVIDER_TRANSPORT_DISCONNECT
    if reason == FailureReason.GATEWAY_ERROR:
        return RetryReason.GATEWAY_ERROR
    return None


def _setup_retry_after_seconds(
    *,
    failure_reason: FailureReason,
    retry_policy: RetryPolicy | None,
    attempt_count: int | None,
) -> float | None:
    if retry_policy is None or attempt_count is None:
        return None
    retry_reason = _setup_retry_reason_for_failure(failure_reason)
    if retry_reason is None:
        return None
    platform_owned_retry = retry_reason == RetryReason.NODE_SETUP_HEALTH
    if not platform_owned_retry and retry_reason not in retry_policy.retry_on:
        return None
    if not platform_owned_retry and attempt_count >= retry_policy.max_attempts:
        return None
    now = datetime.now(UTC)
    retry_at = next_attempt_at(
        attempt_count=attempt_count,
        backoff=retry_policy.backoff,
        now=now,
    )
    return max(0.0, (retry_at - now).total_seconds())


async def _materialize_task_dir(
    *,
    bundle: dict[str, Any],
    object_store: ObjectStore,
    trial_id: UUID,
    fixtures_root: Path | None = None,
    benchmark_cache: Path | None = None,
    timeout_sec: float | None = None,
) -> Path:
    """Create a fresh tempdir and populate it from `bundle["source"]`.

    Dispatches to the registered `Materializer` whose `matches(source)`
    returns True. Adding a new URL scheme is a new Materializer impl in
    `loom_worker.materializers`; the dispatcher stays as-is.
    """
    task_dir = Path(tempfile.mkdtemp(prefix=f"loom-trial-{trial_id}-"))
    materializers = build_default_materializers(
        object_store=object_store,
        fixtures_root=fixtures_root,
        benchmark_cache=benchmark_cache,
    )
    source = bundle.get("source")
    if timeout_sec is not None and timeout_sec <= 0:
        raise ValueError("task_materialize_timeout_sec must be > 0")
    try:
        if timeout_sec is None:
            return await dispatch_materialize(
                source=source,
                task_dir=task_dir,
                trial_id=trial_id,
                materializers=materializers,
            )
        async with asyncio.timeout(timeout_sec):
            return await dispatch_materialize(
                source=source,
                task_dir=task_dir,
                trial_id=trial_id,
                materializers=materializers,
            )
    except TimeoutError as exc:
        raise TimeoutError(
            f"task materialization timed out after {timeout_sec:g}s "
            f"(source_scheme={_source_scheme_for_diagnostic(source)})",
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            "task materialization failed "
            f"(source_scheme={_source_scheme_for_diagnostic(source)}): {exc}",
        ) from exc


def _source_scheme_for_diagnostic(source: object) -> str:
    if not isinstance(source, str) or not source:
        return "none"
    scheme, sep, _rest = source.partition(":")
    if not sep or not scheme:
        return "unknown"
    return scheme


def _verify_materialized_tb21_bundle_checksum(
    *,
    task_dir: Path,
    expected_checksum: str,
    source_provenance: dict[str, Any],
) -> None:
    """Fail closed when current object-store bytes differ from the audited row.

    The physical mirror prefix is content-addressed, but object storage cannot
    be treated as immutable merely because the catalog transaction was locked.
    Rehashing the worker's own materialized directory closes the audit-to-run
    time-of-check/time-of-use window before image build or driver startup.
    """
    actual_checksum = sha256_of_dir(task_dir)
    if expected_checksum.removeprefix("sha256:") != actual_checksum:
        raise ValueError(
            "materialized TB2.1 bundle checksum mismatch "
            f"expected={expected_checksum} actual=sha256:{actual_checksum}",
        )
    actual_metadata_digest = bundle_file_metadata_sha256(task_dir)
    if source_provenance.get("bundle_file_metadata_sha256") != actual_metadata_digest:
        raise ValueError(
            "materialized TB2.1 bundle file mode metadata mismatch "
            f"expected={source_provenance.get('bundle_file_metadata_sha256')} "
            f"actual={actual_metadata_digest}",
        )


def _default_agent_factory(
    team_id: UUID,
    trial_id: UUID,
    *,
    cp_client: StepTokenClient,
    worker_gateway_url: str,
    sandbox_gateway_url: str | None = None,
    provider_connection_id: str | None = None,
) -> AgentFactory:
    """Build the agent factory used by LocalTrialRunner. Routes by
    `agent_name` (read from `task_config.agent.name`):

    - "oracle"      → OracleAgent (solution/solve.sh baseline)
    - "direct-completion" (or legacy "litellm") → LiteLLMAgent
    - "terminus-2"  → LoomTerminus2Runtime (Harbor-embedded in-box runtime;
                      uses ``worker_gateway_url`` only)
    - anything else → SubprocessAgent wrapping the loom-launcher adapter
      of that name (uses ``sandbox_gateway_url`` when set). Raises
      AgentError if the name is unknown (i.e. no v0.7 runtime and no
      registered adapter).
    """

    def make(
        task_dir: Path,
        gateway: LLMGatewayClient,
        model: ModelSpec | None,
        agent_name: str,
    ) -> AgentRuntime:
        agent: AgentRuntime
        if agent_name == "oracle":
            agent = OracleAgent(task_dir=task_dir, trial_id=trial_id)
        elif agent_name in {"direct-completion", "litellm"}:
            if model is None:
                raise AgentError(
                    "direct-completion agent requires task.agent.model to be set",
                )
            # The worker token carried by the process-wide HTTP client is
            # intentionally not valid for model calls.  Bind a fresh
            # llm:call step JWT to every builtin request instead.  Direct
            # local-vLLM clients remain unchanged because they do not cross
            # the Loom Gateway auth boundary.
            if isinstance(gateway, HttpLLMGatewayClient):
                gateway = StepTokenGatewayClient(
                    gateway=gateway,
                    token_issuer=cp_client,
                    team_id=team_id,
                    trial_id=trial_id,
                )
            # mypy: LiteLLMAgent.model is ModelSpec while the AgentRuntime
            # protocol declares ModelSpec | None; covariant on a mutable
            # attribute trips invariance. Both are structurally compatible.
            agent = LiteLLMAgent(  # type: ignore[assignment]
                model=model,
                gateway=gateway,
                team_id=str(team_id),
                trial_id=trial_id,
                provider_connection_id=provider_connection_id,
            )
        elif agent_name == "terminus-2":
            if model is None:
                raise AgentError(
                    "terminus-2 agent requires task.agent.model to be set",
                )
            agent = LoomTerminus2Runtime(  # type: ignore[assignment]
                model=model,
                team_id=str(team_id),
                trial_id=trial_id,
                cp_client=cp_client,
                gateway_url=worker_gateway_url,
                provider_connection_id=provider_connection_id,
            )
        else:
            # Try the loom-launcher registry. Imports are lazy so the
            # launcher dep stays optional for sites that only run
            # built-in agents.
            from loom_launcher import get_adapter

            from loom.agent.subprocess import SubprocessAgent

            adapter = get_adapter(agent_name)
            if adapter is None:
                # Surface as AgentError so Trial.run() classifies it as
                # AGENT_ERROR and the trial fails cleanly instead of
                # crashing the worker.
                raise AgentError(
                    f"unknown agent.name {agent_name!r} — not a v0.7 "
                    f"runtime and not registered in loom-launcher",
                )
            if model is None:
                raise AgentError(
                    f"{agent_name} requires task.agent.model to be set",
                )
            # Same Protocol-variance situation as LiteLLMAgent above:
            # SubprocessAgent.model is ModelSpec while AgentRuntime.model
            # is ModelSpec | None. Structurally compatible.
            agent = SubprocessAgent(  # type: ignore[assignment]
                adapter=adapter,
                model=model,
                cp_client=cp_client,
                gateway_url=worker_gateway_url,
                agent_gateway_url=sandbox_gateway_url,
                team_id=team_id,
                trial_id=trial_id,
            )
        return agent

    return make


def _sandbox_extra_hosts_for_url(url: str | None) -> tuple[tuple[str, str], ...]:
    if not url:
        return ()
    if urlparse(url).hostname == "host.docker.internal":
        return _DOCKER_HOST_GATEWAY_EXTRA_HOSTS
    return ()
