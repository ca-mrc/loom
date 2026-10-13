"""Per-trial agent-install image cache (#317 Phase 1).

When a worker about to spawn a trial finds the task image doesn't have
the chosen agent's CLI, it builds a content-addressed layered image
that adds the agent install on top of the task image, then runs the
trial against the layered image. The build is shared:

- LOCAL CACHE: the worker's own Docker daemon caches the layered
  image; subsequent trials with the same (task_image, agent) on this
  worker hit instantly.

- REGISTRY CACHE (optional): if `trial_cache_registry_repo` is set,
  workers pull the cached image from a shared registry before
  building. After local build, they push for the next worker.

- BUILD COORDINATION: cluster-wide. Workers claim a builder slot via
  the `active_trial_cache_builds` table (4 CP HTTP routes); only the
  claimer builds, others wait. Crash-safe via TTL + heartbeat refresh.
  Builders also claim a daemon-wide synthetic slot before starting
  Docker build/push work so different cold cache keys cannot overload
  the same host Docker daemon concurrently.

See `.claude/plans/2026-06-22-issue-317-agent-runtime-install.md` for
the full design including the v3 self-review log.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import shutil
import textwrap
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

import docker
from docker.errors import APIError, BuildError, ImageNotFound, NotFound

if TYPE_CHECKING:
    from loom_launcher.adapter import AgentAdapter

from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import HttpControlPlaneClient

logger = logging.getLogger(__name__)

LOCAL_TAG_PREFIX = "loom-trial-cache"
DAEMON_BUILD_SLOT_PREFIX = "daemon-build"
HEARTBEAT_INTERVAL_SEC = 60.0


class TrialCacheError(RuntimeError):
    """Raised when the worker cannot resolve a layered trial image."""

    def __init__(
        self,
        *args: object,
        diagnostic_detail: str | None = None,
    ) -> None:
        super().__init__(*args)
        self.diagnostic_detail = diagnostic_detail


def _normalize_install_script(raw: str | None) -> str | None:
    """Empty / whitespace-only scripts are treated as None (no install
    needed — let the trial run on the bare task image)."""
    if raw is None:
        return None
    if not raw.strip():
        return None
    return raw


def _cache_key(*, task_image_digest: str, install_script: str) -> str:
    """Content-addressed cache key.

    Inputs that affect the resulting image bytes: the base image
    content (its digest) + the install script text. Output: 32 hex
    chars = 128 bits, comfortably collision-safe at any realistic
    Loom scale (Docker tags accept the full 64-hex; 32 chosen for
    readability)."""
    # NUL separator prevents (digest+script) collision between
    # (digestA, scriptB) and (digestAscriptB-prefix, suffix).
    material = (task_image_digest + "\x00" + install_script).encode("utf-8")
    return sha256(material).hexdigest()[:32]


async def _pull_or_get_digest(
    client: Any,
    image_ref: str,
    *,
    timeout_sec: float,
) -> str:
    """Return the local content digest for an image reference.

    - `sha256:...` → returned unchanged
    - tag already in local Docker daemon → inspect
    - tag not local → pull (with timeout) then inspect

    Timeout sized for the largest expected base image; SWE-Bench
    instance images are 1-2GB and on slow networks the pull can take
    10-20 min, hence the operator-configurable
    `trial_cache_base_image_pull_timeout_sec` (default 1800s).
    """
    if image_ref.startswith("sha256:"):
        return image_ref
    try:
        image = await asyncio.to_thread(client.images.get, image_ref)
        return str(image.id)
    except (ImageNotFound, NotFound):
        pass
    try:
        await asyncio.wait_for(
            asyncio.to_thread(client.images.pull, image_ref),
            timeout=timeout_sec,
        )
    except TimeoutError as exc:
        raise TrialCacheError(
            f"timed out pulling task image {image_ref!r} after {timeout_sec:g}s",
        ) from exc
    except APIError as exc:
        raise TrialCacheError(
            f"failed to pull task image {image_ref!r}: {exc}",
        ) from exc
    image = await asyncio.to_thread(client.images.get, image_ref)
    return str(image.id)


def _image_exists_locally(client: Any, tag: str) -> bool:
    try:
        client.images.get(tag)
        return True
    except (ImageNotFound, NotFound):
        return False


def _tag_alias(client: Any, source: str, target: str) -> None:
    """Tag an already-pulled image with a second name. Used after
    pulling `<registry>/<repo>:<key>` to also expose it as
    `loom-trial-cache:<key>` locally."""
    repo, _, tag = target.partition(":")
    client.images.get(source).tag(repository=repo, tag=tag or "latest")


def _build_layered_image_sync(
    *,
    client: Any,
    tag: str,
    base_digest: str,
    install_script: str,
) -> None:
    """Synthesize a Dockerfile that layers `install_script` on top of
    `base_digest`, then run `docker build`. Tempdir cleanup is automatic.

    Layer pattern: `COPY install.sh /tmp/install.sh; RUN bash /tmp/install.sh`
    — beats inline `RUN bash -c '<script>'` because shell quoting
    doesn't bite when scripts contain $, backticks, or single quotes."""
    with TemporaryDirectory(prefix="loom-trial-build-") as ctx:
        ctx_path = Path(ctx)
        (ctx_path / "install.sh").write_text(install_script)
        (ctx_path / "Dockerfile").write_text(
            textwrap.dedent(f"""\
            FROM {base_digest}
            COPY install.sh /tmp/install.sh
            RUN bash /tmp/install.sh && rm /tmp/install.sh
        """)
        )
        # Local development builds the image through the configured Docker daemon.
        try:
            client.images.build(
                path=str(ctx_path),
                tag=tag,
                rm=True,
                forcerm=True,
                pull=False,
                labels={
                    "loom.trial-cache": "true",
                    "loom.cache-key": tag.split(":", 1)[1],
                    "loom.created-at": datetime.now(UTC).isoformat(),
                    # No loom.last-used-at: Docker labels can't be
                    # updated post-build; eviction uses creation-age
                    # via `docker image prune --filter until=`.
                },
            )
        except BuildError as exc:
            # docker-py's BuildError stringifies to only the failing
            # RUN command — useless for diagnosing WHY the install
            # script failed. Walk build_log and surface the trailing
            # 40 lines so operators see pip's / apt's actual error.
            # Preserve the full log separately for diagnostic downloads.
            tail, full_log = _format_build_log_sections(exc.build_log)
            diagnostic_detail = f"failed to build layered image {tag!r}: {exc}" + (
                f"\nbuild log (full):\n{full_log}" if full_log else ""
            )
            raise TrialCacheError(
                f"failed to build layered image {tag!r}: {exc}"
                + (f"\nbuild log (last lines):\n{tail}" if tail else ""),
                diagnostic_detail=diagnostic_detail,
            ) from exc
        except APIError as exc:
            raise TrialCacheError(
                f"failed to build layered image {tag!r}: {exc}",
            ) from exc


_BUILD_LOG_TAIL_LINES = 40


def _format_build_log_sections(build_log: Any) -> tuple[str, str]:
    """Same shape as task_image._format_build_log_sections (#376)."""
    if build_log is None:
        return "", ""
    lines: list[str] = []
    try:
        for chunk in build_log:
            if isinstance(chunk, dict):
                text = chunk.get("stream") or chunk.get("error") or ""
            else:
                text = str(chunk)
            if not text:
                continue
            for line in text.splitlines():
                stripped = line.rstrip()
                if stripped:
                    lines.append(stripped)
    except Exception:
        pass
    if not lines:
        return "", ""
    return "\n".join(lines[-_BUILD_LOG_TAIL_LINES:]), "\n".join(lines)


@contextlib.asynccontextmanager
async def _builder_with_heartbeat(
    cp_client: HttpControlPlaneClient,
    cache_key: str,
    worker_id: UUID,
    *,
    ttl_sec: float,
    interval_sec: float = HEARTBEAT_INTERVAL_SEC,
) -> AsyncIterator[None]:
    """Spawn a background task that refreshes the slot TTL every
    `interval_sec` seconds until the context exits.

    On crash (cancellation, OOM, segfault), the heartbeat task dies →
    next refresh doesn't happen → slot expires within ttl_sec → another
    worker can steal it. This decouples maximum allowed build duration
    (TTL) from crash-recovery latency (~interval_sec)."""
    stop = asyncio.Event()

    async def _beat() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_sec)
            except TimeoutError:
                try:
                    refreshed = await cp_client.refresh_trial_cache_slot(
                        cache_key,
                        worker_id,
                        ttl_sec=ttl_sec,
                    )
                    if not refreshed:
                        logger.warning(
                            "trial_cache slot stolen mid-build "
                            "cache_key=%s; another worker is now the builder",
                            cache_key,
                        )
                except Exception:
                    logger.exception(
                        "trial_cache heartbeat failed cache_key=%s",
                        cache_key,
                    )

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _daemon_build_slot_keys(settings: WorkerSettings) -> list[str]:
    max_concurrent = getattr(settings, "trial_cache_build_max_concurrent", 1)
    if max_concurrent < 1:
        raise TrialCacheError("trial_cache_build_max_concurrent must be >= 1")
    daemon_id_material = "\x00".join(
        (
            str(getattr(settings, "pool_name", "") or "default"),
            str(getattr(settings, "hostname", "") or "unconfigured-host"),
            str(getattr(settings, "docker_socket", "") or "/var/run/docker.sock"),
        )
    )
    daemon_id = sha256(daemon_id_material.encode("utf-8")).hexdigest()[:24]
    return [f"{DAEMON_BUILD_SLOT_PREFIX}:{daemon_id}:{slot}" for slot in range(max_concurrent)]


async def _claim_any_trial_cache_slot(
    cp_client: HttpControlPlaneClient,
    slot_keys: list[str],
    worker_id: UUID,
    *,
    ttl_sec: float,
) -> str | None:
    for slot_key in slot_keys:
        if await cp_client.claim_trial_cache_slot(
            slot_key,
            worker_id,
            ttl_sec=ttl_sec,
        ):
            return slot_key
    return None


@contextlib.asynccontextmanager
async def _daemon_build_slot(
    cp_client: HttpControlPlaneClient,
    settings: WorkerSettings,
    worker_id: UUID,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    read_setup_health: Callable[[], Any] | None = None,
) -> AsyncIterator[str]:
    """Limit concurrent docker builds against a shared host daemon.

    The cache-specific slot prevents duplicate builds of the same
    `(task_image_digest, install_script)` image. This broader slot
    prevents different cold cache keys from hammering the same Docker
    daemon with concurrent apt/build setup containers.
    """
    from loom_worker.setup_admission import (
        policy_from_settings,
        read_node_health_snapshot,
        wait_for_setup_health,
    )

    policy = policy_from_settings(settings)
    read_snapshot = read_setup_health or read_node_health_snapshot
    await wait_for_setup_health(
        policy=policy,
        operation="setup-build",
        read_snapshot=read_snapshot,
        sleep=sleep,
    )
    slot_keys = _daemon_build_slot_keys(settings)
    ttl_sec = settings.trial_cache_build_lock_timeout_sec
    slot_key = await _claim_any_trial_cache_slot(
        cp_client,
        slot_keys,
        worker_id,
        ttl_sec=ttl_sec,
    )
    while slot_key is None:
        await sleep(random.uniform(2.0, 5.0))
        for candidate in slot_keys:
            if await cp_client.trial_cache_slot_exists(candidate):
                continue
            if await cp_client.claim_trial_cache_slot(
                candidate,
                worker_id,
                ttl_sec=ttl_sec,
            ):
                slot_key = candidate
                break

    try:
        await wait_for_setup_health(
            policy=policy,
            operation="setup-build",
            read_snapshot=read_snapshot,
            sleep=sleep,
        )
    except Exception:
        with contextlib.suppress(Exception):
            await cp_client.release_trial_cache_slot(slot_key, worker_id)
        raise
    logger.info(
        "trial_cache daemon build slot acquired slot=%s max_concurrent=%d",
        slot_key,
        len(slot_keys),
    )
    async with _builder_with_heartbeat(
        cp_client,
        slot_key,
        worker_id,
        ttl_sec=ttl_sec,
    ):
        try:
            yield slot_key
        finally:
            with contextlib.suppress(Exception):
                await cp_client.release_trial_cache_slot(slot_key, worker_id)


async def resolve_trial_image(
    *,
    task_image: str,
    adapter: AgentAdapter,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    worker_id: UUID,
    docker_client: Any | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> str:
    """Return the Docker image tag the worker should `docker run` for
    this trial.

    Flow (see plan §Architecture):

    1. If the adapter declares no install_script (oracle, in-box,
       legacy), just return the task image as-is.
    2. Resolve task image to a digest (pull if needed).
    3. Compute cache_key = sha256(digest + install_script)[:32].
    4. Local cache hit → return.
    5. Claim builder slot via CP HTTP route.
       - If not the builder: poll cheaply (SELECT) for the slot to
         disappear OR the image to appear locally/in the registry.
         Re-claim only when the slot row is gone.
       - If the builder: try registry pull first; on miss, build
         locally; push to registry (awaited under the slot to prevent
         the next worker from rebuilding).
    6. Return the layered image tag.
    """
    # Adapters without an install_script (oracle, in-box agents, the
    # `hello` test fixture) bypass the layered-image cache entirely
    # and run directly on the task's base image. `getattr` guards
    # against any future adapter whose Protocol conformance lags.
    install_script = _normalize_install_script(
        getattr(adapter, "install_script", None),
    )
    if install_script is None:
        return task_image

    client = (
        docker_client
        if docker_client is not None
        else docker.from_env(timeout=settings.docker_api_timeout_sec)
    )

    # Step 2: resolve task image to a stable digest.
    task_image_digest = await _pull_or_get_digest(
        client,
        task_image,
        timeout_sec=settings.trial_cache_base_image_pull_timeout_sec,
    )

    # Step 3: cache key + tag names.
    cache_key = _cache_key(
        task_image_digest=task_image_digest,
        install_script=install_script,
    )
    local_tag = f"{LOCAL_TAG_PREFIX}:{cache_key}"
    registry_repo = settings.trial_cache_registry_repo
    registry_tag: str | None = f"{registry_repo}:{cache_key}" if registry_repo else None

    # Step 4: local hit?
    if await asyncio.to_thread(_image_exists_locally, client, local_tag):
        return local_tag

    # Step 5: claim or wait for a builder slot.
    i_am_builder = await cp_client.claim_trial_cache_slot(
        cache_key,
        worker_id,
        ttl_sec=settings.trial_cache_build_lock_timeout_sec,
    )
    while not i_am_builder:
        await sleep(random.uniform(2.0, 5.0))

        # Most-common exit paths first.
        if await asyncio.to_thread(_image_exists_locally, client, local_tag):
            return local_tag
        if registry_tag and await _try_registry_pull(
            client,
            registry_tag,
            local_tag,
            settings,
        ):
            return local_tag

        # Cheap probe: is some other worker still holding the slot?
        if await cp_client.trial_cache_slot_exists(cache_key):
            continue
        # Slot is gone (builder finished or crashed). Race to claim.
        i_am_builder = await cp_client.claim_trial_cache_slot(
            cache_key,
            worker_id,
            ttl_sec=settings.trial_cache_build_lock_timeout_sec,
        )

    # We're the builder. Hold the slot through build + push via the
    # heartbeat-refreshed context manager. Long pulls/builds extend
    # the TTL every 60s; on crash, slot expires within ~60s.
    async with _builder_with_heartbeat(
        cp_client,
        cache_key,
        worker_id,
        ttl_sec=settings.trial_cache_build_lock_timeout_sec,
    ):
        try:
            # Even as the builder, try the registry first — another
            # worker may have pushed between our claim and now (race
            # window: slot disappeared, we got it, someone else's push
            # had completed by the time we acquired). Returning here
            # still runs the `finally` clause that releases the slot.
            if registry_tag and await _try_registry_pull(
                client,
                registry_tag,
                local_tag,
                settings,
            ):
                return local_tag

            async with _daemon_build_slot(
                cp_client,
                settings,
                worker_id,
                sleep=sleep,
            ):
                await asyncio.to_thread(
                    _build_layered_image_sync,
                    client=client,
                    tag=local_tag,
                    base_digest=task_image_digest,
                    install_script=install_script,
                )

                # AWAITED push under the slot — prevents the v3 race
                # where async push lets the next worker pull-miss and
                # build redundantly.
                if registry_tag:
                    try:
                        await asyncio.to_thread(
                            _push_image,
                            client,
                            local_tag,
                            registry_tag,
                        )
                    except Exception as exc:
                        logger.warning(
                            "trial_cache push failed cache_key=%s (next worker will rebuild): %s",
                            cache_key,
                            exc,
                        )
        finally:
            with contextlib.suppress(Exception):
                await cp_client.release_trial_cache_slot(cache_key, worker_id)

    return local_tag


async def _try_registry_pull(
    client: Any,
    registry_tag: str,
    local_tag: str,
    settings: WorkerSettings,
) -> bool:
    """Pull `registry_tag` with a bounded timeout. On success, tag it
    locally as `local_tag` and return True. On any error (missing tag,
    timeout, auth failure), return False."""
    try:
        await asyncio.wait_for(
            asyncio.to_thread(client.images.pull, registry_tag),
            timeout=settings.trial_cache_registry_pull_timeout_sec,
        )
    except (TimeoutError, ImageNotFound, NotFound, APIError) as exc:
        logger.debug(
            "trial_cache registry pull miss %s: %s",
            registry_tag,
            exc,
        )
        return False
    try:
        await asyncio.to_thread(_tag_alias, client, registry_tag, local_tag)
    except APIError as exc:
        logger.warning(
            "trial_cache pulled %s but failed to alias as %s: %s",
            registry_tag,
            local_tag,
            exc,
        )
        return False
    return True


def _push_image(client: Any, source_tag: str, registry_tag: str) -> None:
    """Tag `source_tag` as `registry_tag` then push. `push` raises on
    auth / network failure; caller logs + continues."""
    repo, _, tag = registry_tag.partition(":")
    client.images.get(source_tag).tag(repository=repo, tag=tag or "latest")
    # Docker SDK's push is generator-based; consume it so we wait for
    # completion and surface errors.
    for line in client.images.push(repository=repo, tag=tag, stream=True, decode=True):
        if isinstance(line, dict) and line.get("errorDetail"):
            raise APIError(line["errorDetail"].get("message", "push failed"))


# ─── Eviction ──────────────────────────────────────────────────────


_MANAGED_IMAGE_LABELS = (
    "loom.task-image=true",
    "loom.task-sidecar=true",
    "loom.trial-cache=true",
)
_MANAGED_IMAGE_LABEL_KEYS = tuple(label.partition("=")[0] for label in _MANAGED_IMAGE_LABELS)
_STOPPED_CONTAINER_STATES = frozenset({"created", "dead", "exited"})


@dataclass(frozen=True, slots=True)
class ManagedImageCleanupResult:
    """Final local-storage evidence after bounded Loom-owned cleanup."""

    docker_root: str
    storage_probe_path: str
    free_bytes: int
    required_free_bytes: int
    probe_available: bool
    error_count: int


def _required_managed_image_free_bytes(settings: WorkerSettings) -> int:
    min_free_gb = int(
        getattr(
            settings,
            "task_image_min_free_gb",
            getattr(settings, "trial_cache_min_free_gb", 20),
        )
    )
    return min_free_gb * 1024**3


def _docker_labels(resource: Any) -> dict[str, str]:
    labels = getattr(resource, "labels", None)
    if isinstance(labels, dict):
        return {str(key): str(value) for key, value in labels.items()}
    attrs = getattr(resource, "attrs", None)
    if not isinstance(attrs, dict):
        return {}
    config = attrs.get("Config")
    if not isinstance(config, dict) or not isinstance(config.get("Labels"), dict):
        return {}
    return {str(key): str(value) for key, value in config["Labels"].items()}


def _is_loom_managed(resource: Any) -> bool:
    labels = _docker_labels(resource)
    return any(labels.get(key) == "true" for key in _MANAGED_IMAGE_LABEL_KEYS)


def _container_status(container: Any) -> str:
    status = getattr(container, "status", None)
    if isinstance(status, str):
        return status.lower()
    attrs = getattr(container, "attrs", None)
    if isinstance(attrs, dict):
        state = attrs.get("State")
        if isinstance(state, dict) and isinstance(state.get("Status"), str):
            return str(state["Status"]).lower()
    return ""


def _remove_stopped_managed_containers(client: Any) -> int:
    errors = 0
    try:
        containers = client.containers.list(all=True)
    except Exception as exc:
        logger.warning("managed stopped-container list failed: %s", exc)
        return 1
    for container in containers:
        try:
            if _container_status(container) not in _STOPPED_CONTAINER_STATES:
                continue
            if not _is_loom_managed(container) and not _is_loom_managed(container.image):
                continue
            container.remove(v=False, force=False)
        except Exception as exc:
            errors += 1
            logger.warning(
                "managed stopped-container remove failed for %s: %s",
                getattr(container, "id", "unknown"),
                exc,
            )
    return errors


def _unavailable_cleanup_result(
    *,
    docker_root: str,
    storage_probe_path: str,
    required_free_bytes: int,
    error_count: int,
) -> ManagedImageCleanupResult:
    return ManagedImageCleanupResult(
        docker_root=docker_root,
        storage_probe_path=storage_probe_path,
        free_bytes=0,
        required_free_bytes=required_free_bytes,
        probe_available=False,
        error_count=error_count,
    )


def _managed_image_storage_probe_path(
    settings: WorkerSettings,
    *,
    docker_root: str,
) -> tuple[Path | None, str]:
    configured = getattr(settings, "task_image_storage_probe_path", None)
    path_text = str(configured) if configured is not None else docker_root
    if not path_text:
        return None, ""
    path = Path(path_text)
    return (path if path.is_absolute() else None), path_text


def evict_stale_managed_images(
    client: Any,
    settings: WorkerSettings,
) -> ManagedImageCleanupResult:
    """Clean only Loom-owned Docker resources and return final storage evidence."""
    error_count = _remove_stopped_managed_containers(client)
    ttl_hours = int(
        getattr(
            settings,
            "task_image_local_ttl_hours",
            getattr(settings, "trial_cache_ttl_hours", 168),
        )
    )
    required_free_bytes = _required_managed_image_free_bytes(settings)
    for label in _MANAGED_IMAGE_LABELS:
        try:
            client.images.prune(
                filters={
                    "label": label,
                    "until": f"{ttl_hours}h",
                    "dangling": False,
                }
            )
        except APIError as exc:
            error_count += 1
            logger.warning("managed image TTL prune failed label=%s: %s", label, exc)

    docker_root = ""
    docker_root_probe_failed = False
    try:
        info = client.info()
        if isinstance(info, dict) and isinstance(info.get("DockerRootDir"), str):
            docker_root = info["DockerRootDir"]
    except Exception as exc:
        docker_root_probe_failed = True
        error_count += 1
        logger.warning("managed image Docker-root probe failed: %s", exc)
    storage_probe_path, storage_probe_path_text = _managed_image_storage_probe_path(
        settings,
        docker_root=docker_root,
    )
    if storage_probe_path is None:
        if not docker_root_probe_failed:
            error_count += 1
        return _unavailable_cleanup_result(
            docker_root=docker_root,
            storage_probe_path=storage_probe_path_text,
            required_free_bytes=required_free_bytes,
            error_count=error_count,
        )
    try:
        free_bytes = shutil.disk_usage(storage_probe_path).free
    except OSError as exc:
        error_count += 1
        logger.warning("managed image disk-usage probe failed: %s", exc)
        return _unavailable_cleanup_result(
            docker_root=docker_root,
            storage_probe_path=storage_probe_path_text,
            required_free_bytes=required_free_bytes,
            error_count=error_count,
        )
    if free_bytes >= required_free_bytes:
        return ManagedImageCleanupResult(
            docker_root=docker_root,
            storage_probe_path=storage_probe_path_text,
            free_bytes=free_bytes,
            required_free_bytes=required_free_bytes,
            probe_available=True,
            error_count=error_count,
        )

    by_id: dict[str, Any] = {}
    try:
        for label in _MANAGED_IMAGE_LABELS:
            for image in client.images.list(filters={"label": label}):
                by_id[str(image.id)] = image
    except APIError as exc:
        error_count += 1
        logger.warning("managed image list failed: %s", exc)
        return ManagedImageCleanupResult(
            docker_root=docker_root,
            storage_probe_path=storage_probe_path_text,
            free_bytes=free_bytes,
            required_free_bytes=required_free_bytes,
            probe_available=True,
            error_count=error_count,
        )
    cached = sorted(by_id.values(), key=lambda image: image.attrs.get("Created", ""))

    for img in cached:
        if free_bytes >= required_free_bytes:
            break
        try:
            client.images.remove(img.id, force=True)
        except APIError as exc:
            error_count += 1
            logger.debug("managed image remove failed for %s: %s", img.id, exc)
            continue
        try:
            free_bytes = shutil.disk_usage(storage_probe_path).free
        except OSError as exc:
            error_count += 1
            logger.warning("managed image final disk-usage probe failed: %s", exc)
            return _unavailable_cleanup_result(
                docker_root=docker_root,
                storage_probe_path=storage_probe_path_text,
                required_free_bytes=required_free_bytes,
                error_count=error_count,
            )
    return ManagedImageCleanupResult(
        docker_root=docker_root,
        storage_probe_path=storage_probe_path_text,
        free_bytes=free_bytes,
        required_free_bytes=required_free_bytes,
        probe_available=True,
        error_count=error_count,
    )


def evict_stale_cache(client: Any, settings: WorkerSettings) -> ManagedImageCleanupResult:
    """Compatibility alias for the generalized managed-image eviction."""
    return evict_stale_managed_images(client, settings)


def evict_stale_managed_images_from_env(
    settings: WorkerSettings,
) -> ManagedImageCleanupResult:
    """Run managed cleanup and return fail-closed local-storage evidence."""
    required_free_bytes = _required_managed_image_free_bytes(settings)
    _, storage_probe_path = _managed_image_storage_probe_path(
        settings,
        docker_root="",
    )
    try:
        client = docker.from_env()
    except Exception:
        logger.exception("managed image eviction Docker client failed")
        return _unavailable_cleanup_result(
            docker_root="",
            storage_probe_path=storage_probe_path,
            required_free_bytes=required_free_bytes,
            error_count=1,
        )
    try:
        return evict_stale_managed_images(client, settings)
    except Exception:
        logger.exception("managed image eviction failed")
        return _unavailable_cleanup_result(
            docker_root="",
            storage_probe_path=storage_probe_path,
            required_free_bytes=required_free_bytes,
            error_count=1,
        )
    finally:
        with contextlib.suppress(Exception):
            client.close()


class _SupportsCacheSlots(Protocol):
    """Subset of HttpControlPlaneClient the cache layer uses. Kept
    here so unit tests can supply a fake without instantiating the
    full HTTP client."""

    async def claim_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool: ...

    async def trial_cache_slot_exists(self, cache_key: str) -> bool: ...

    async def release_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
    ) -> None: ...

    async def refresh_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool: ...
