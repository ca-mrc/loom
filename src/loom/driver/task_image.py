"""Resolve service-mode task images.

Published benchmarks can declare either a ready-to-run Docker image or a
Dockerfile inside the task bundle. The worker materializes the bundle before a
trial starts, so Dockerfile tasks can be built from that local directory and
cached under a deterministic tag.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import docker
from docker.errors import APIError, BuildError, ImageNotFound, NotFound

from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig

logger = logging.getLogger(__name__)

# Type alias for the optional build-slot provider a caller can pass to
# `resolve_task_image` so a task-image build (apt-get / dpkg / etc.)
# only proceeds while a daemon-wide semaphore slot is held. Callers
# that don't care about node overload can pass None. #275.
BuildSlotProvider = Callable[[], contextlib.AbstractAsyncContextManager[Any]]

DEFAULT_TASK_IMAGE = "alpine"
# Maximum trailing build-log lines (stdout+stderr from inside the
# Docker build) included in TaskImageBuildError messages when a
# Dockerfile build fails. Enough to show pip's actual error output
# while staying well under typical DB/JSON field limits.
_BUILD_LOG_TAIL_LINES = 40
DEFAULT_BUILD_CONTEXT_MAX_FILES = 2_000
DEFAULT_BUILD_CONTEXT_MAX_BYTES = 512 * 1024 * 1024
ENV_BUILD_CONTEXT_MAX_FILES = "LOOM_TASK_IMAGE_BUILD_MAX_FILES"
ENV_BUILD_CONTEXT_MAX_BYTES = "LOOM_TASK_IMAGE_BUILD_MAX_BYTES"


class TaskImageBuildError(RuntimeError):
    """Raised when the worker cannot resolve a task sandbox image."""

    def __init__(
        self,
        *args: object,
        diagnostic_detail: str | None = None,
    ) -> None:
        super().__init__(*args)
        self.diagnostic_detail = diagnostic_detail


class TaskImageBuildTimeoutError(TaskImageBuildError):
    """A daemon build outlived its awaiter and must keep the allocation exclusive."""


def _native_cpu_arch(task_config: TaskConfig, cpu_arch: str | None) -> str:
    # A selected worker architecture cannot reinterpret an ARM-only task.
    execution_cpu_arch(task_config.environment.cpu_arch)
    # A Mac/ARM CLI host does not change the remote workload architecture.
    return execution_cpu_arch(cpu_arch or task_config.environment.cpu_arch)


def _docker_platform(cpu_arch: str) -> str:
    execution_cpu_arch(cpu_arch)
    return "linux/amd64"


def task_image_tag(
    task_config: TaskConfig,
    *,
    task_checksum: str,
    cpu_arch: str | None = None,
) -> str:
    """Stable local Docker tag for a task-bundle Dockerfile build."""

    dockerfile = task_config.environment.dockerfile
    dockerfile_text = dockerfile.as_posix() if dockerfile is not None else ""
    build_context = task_config.environment.docker_build_context
    build_context_text = build_context.as_posix() if build_context is not None else ""
    native_cpu_arch = _native_cpu_arch(task_config, cpu_arch)
    material = "\n".join(
        [
            task_config.task.id,
            task_checksum,
            native_cpu_arch,
            dockerfile_text,
            build_context_text,
        ]
    )
    if task_config.environment.docker_build_args or task_config.environment.docker_build_target:
        material += "\n" + json.dumps(
            {
                "build_args": task_config.environment.docker_build_args,
                "target": task_config.environment.docker_build_target,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
    return f"loom-task:{digest}"


async def resolve_task_image(
    *,
    task_config: TaskConfig,
    task_dir: Path,
    task_checksum: str,
    docker_api_timeout_sec: int | None = None,
    build_slot_provider: BuildSlotProvider | None = None,
    registry_repo: str | None = None,
    registry_image: str | None = None,
    registry_pull_timeout_sec: float = 15.0,
    cpu_arch: str | None = None,
    build_if_missing: bool = True,
) -> str:
    """Return the Docker image a service worker should use for this task.

    ``environment.docker_image`` remains the fastest path when no Dockerfile is
    declared. A declared ``environment.dockerfile`` is authoritative, including
    for legacy configs that contain both fields, and is built from the
    materialized task bundle if the deterministic cache tag is absent. Tasks
    that declare neither keep the historic ``alpine`` fallback.

    When ``build_slot_provider`` is supplied and a build is actually
    required (deterministic tag not present in the local daemon), the
    provider is entered as an async context manager so callers can
    serialize concurrent apt-get / dpkg / build storms against a shared
    host Docker daemon. Cache hits skip the slot entirely — the check
    is a cheap `client.images.get(tag)` on the local daemon. #275.
    """

    _native_cpu_arch(task_config, cpu_arch)
    if task_config.environment.dockerfile is None:
        return task_config.environment.docker_image or DEFAULT_TASK_IMAGE

    dockerfile = _resolve_dockerfile_path(
        task_dir=task_dir,
        dockerfile=task_config.environment.dockerfile,
    )
    build_context = _resolve_build_context_path(
        task_dir=task_dir,
        dockerfile=dockerfile,
        docker_build_context=task_config.environment.docker_build_context,
    )
    if registry_image is not None:
        if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", registry_image) is None:
            raise TaskImageBuildError(
                "materialized task image must be an immutable registry digest reference"
            )
        if await asyncio.to_thread(
            _task_image_locally_cached,
            tag=registry_image,
            docker_api_timeout_sec=docker_api_timeout_sec,
        ):
            return registry_image
        exact_pull_slot_ctx: contextlib.AbstractAsyncContextManager[Any] = (
            build_slot_provider() if build_slot_provider is not None else contextlib.nullcontext()
        )
        try:
            async with exact_pull_slot_ctx:
                await _pull_exact_registry_image(
                    registry_image=registry_image,
                    pull_timeout_sec=registry_pull_timeout_sec,
                    docker_api_timeout_sec=docker_api_timeout_sec,
                )
        except TimeoutError as exc:
            raise TaskImageBuildTimeoutError(
                f"pulling materialized task image {registry_image!r} exceeded "
                f"{registry_pull_timeout_sec:g}s; execution is fenced to the "
                "recorded registry digest"
            ) from exc
        except (ImageNotFound, NotFound, APIError) as exc:
            raise TaskImageBuildError(
                f"materialized task image {registry_image!r} is unavailable; "
                "execution is fenced to the recorded registry digest"
            ) from exc
        return registry_image
    tag = task_image_tag(
        task_config,
        task_checksum=task_checksum,
        cpu_arch=cpu_arch,
    )
    native_cpu_arch = _native_cpu_arch(task_config, cpu_arch)

    # Fast path: image already present locally, no build needed and no
    # slot claim required. Keeps steady-state trial dispatch free of the
    # slot-claim HTTP round-trip once a task's Dockerfile is cached.
    if await asyncio.to_thread(
        _task_image_locally_cached,
        tag=tag,
        docker_api_timeout_sec=docker_api_timeout_sec,
    ):
        return tag

    # Prefer a published image before a local Docker build, matching the
    # layered-image cache in trial_cache.py.
    if registry_repo and await _try_registry_pull_task_image(
        tag=tag,
        registry_repo=registry_repo,
        pull_timeout_sec=registry_pull_timeout_sec,
        docker_api_timeout_sec=docker_api_timeout_sec,
    ):
        return tag
    if not build_if_missing:
        raise TaskImageBuildError(
            f"task image {tag!r} is not available from the configured registry; "
            "execution workers are pull-only",
        )

    timeout = task_config.environment.build_timeout_sec
    slot_ctx: contextlib.AbstractAsyncContextManager[Any] = (
        build_slot_provider() if build_slot_provider is not None else contextlib.nullcontext()
    )
    try:
        async with slot_ctx:
            # `_ensure_dockerfile_image` itself does a `.get()` inside
            # the slot, so a concurrent worker that built + pushed the
            # tag while this worker waited still short-circuits without
            # rebuilding.
            await asyncio.wait_for(
                asyncio.to_thread(
                    _ensure_dockerfile_image,
                    tag=tag,
                    task_config=task_config,
                    task_checksum=task_checksum,
                    task_dir=task_dir,
                    dockerfile=dockerfile,
                    build_context=build_context,
                    docker_api_timeout_sec=docker_api_timeout_sec,
                    registry_repo=registry_repo,
                    cpu_arch=native_cpu_arch,
                ),
                timeout=timeout,
            )
    except TimeoutError as exc:
        raise TaskImageBuildTimeoutError(
            f"building Docker image {tag!r} from "
            f"{task_config.environment.dockerfile.as_posix()!r} exceeded "
            f"{timeout:g}s",
        ) from exc
    return tag


async def _pull_exact_registry_image(
    *,
    registry_image: str,
    pull_timeout_sec: float,
    docker_api_timeout_sec: int | None,
) -> None:
    client: Any = (
        docker.from_env()
        if docker_api_timeout_sec is None
        else docker.from_env(timeout=docker_api_timeout_sec)
    )
    try:
        await asyncio.wait_for(
            asyncio.to_thread(client.images.pull, registry_image),
            timeout=pull_timeout_sec,
        )
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _task_image_locally_cached(
    *,
    tag: str,
    docker_api_timeout_sec: int | None,
) -> bool:
    """True iff the deterministic tag is already resident in the local
    Docker daemon's image store. Kept as a sync helper so it can be
    awaited via `asyncio.to_thread` from the async resolve path."""
    client: Any = (
        docker.from_env()
        if docker_api_timeout_sec is None
        else docker.from_env(timeout=docker_api_timeout_sec)
    )
    try:
        client.images.get(tag)
    except ImageNotFound:
        return False
    else:
        return True
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _registry_tag_for(tag: str, registry_repo: str) -> str:
    """The shared-registry ref for a local ``loom-task:<digest>`` tag.

    Uses ``rpartition`` so the deterministic digest (which never contains a
    colon) is split off the LAST colon — leaving a ``host:port/path`` registry
    repo intact. Every worker derives the same ref for the same task image, so
    a push by one is pullable by all.
    """
    key = tag.rpartition(":")[2]
    return f"{registry_repo}:{key}"


async def _try_registry_pull_task_image(
    *,
    tag: str,
    registry_repo: str,
    pull_timeout_sec: float,
    docker_api_timeout_sec: int | None,
) -> bool:
    """Pull the pre-built base image from the shared registry, aliasing it to
    the local ``tag``. Returns True on success; False on any miss (absent tag,
    timeout, auth/network error) so the caller falls through to build/refuse.

    Mirrors ``trial_cache._try_registry_pull`` for the base-image path so a
    containment-required worker can PULL an image it may not build (#1169).
    """
    registry_tag = _registry_tag_for(tag, registry_repo)
    client: Any = (
        docker.from_env()
        if docker_api_timeout_sec is None
        else docker.from_env(timeout=docker_api_timeout_sec)
    )
    try:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(client.images.pull, registry_tag),
                timeout=pull_timeout_sec,
            )
        except (TimeoutError, ImageNotFound, NotFound, APIError) as exc:
            logger.debug("task image registry pull miss %s: %s", registry_tag, exc)
            return False
        try:
            repo, _, local_key = tag.rpartition(":")
            await asyncio.to_thread(
                lambda: client.images.get(registry_tag).tag(
                    repository=repo, tag=local_key or "latest"
                )
            )
        except APIError as exc:
            logger.warning("pulled %s but failed to alias as %s: %s", registry_tag, tag, exc)
            return False
        logger.info("task image %s satisfied from registry %s", tag, registry_tag)
        return True
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _push_task_image_to_registry(client: Any, tag: str, registry_repo: str) -> str:
    """Tag the freshly-built local ``tag`` as ``<registry_repo>:<digest>`` and
    push it so containment-required workers can pull it. Raises on push failure
    (the caller logs + continues — the local build already succeeded).
    """
    registry_tag = _registry_tag_for(tag, registry_repo)
    repo, _, key = registry_tag.rpartition(":")
    client.images.get(tag).tag(repository=repo, tag=key or "latest")
    digest: str | None = None
    for line in client.images.push(repository=repo, tag=key, stream=True, decode=True):
        if isinstance(line, dict) and line.get("errorDetail"):
            raise APIError(line["errorDetail"].get("message", "push failed"))
        if isinstance(line, dict):
            aux = line.get("aux")
            if isinstance(aux, dict) and isinstance(aux.get("Digest"), str):
                digest = aux["Digest"]
    if digest is not None and re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        return f"{repo}@{digest}"
    image = client.images.get(registry_tag)
    attrs = getattr(image, "attrs", {})
    repo_digests = attrs.get("RepoDigests", []) if isinstance(attrs, dict) else []
    for ref in repo_digests or []:
        if isinstance(ref, str) and ref.startswith(f"{repo}@sha256:"):
            return ref
    raise TaskImageBuildError(
        f"registry push for {registry_tag!r} did not return an immutable digest",
    )


async def publish_local_image_to_registry(
    *,
    tag: str,
    registry_repo: str,
    docker_api_timeout_sec: int | None = None,
) -> str:
    """Push one managed local image and return its immutable registry ref."""
    client: Any = (
        docker.from_env()
        if docker_api_timeout_sec is None
        else docker.from_env(timeout=docker_api_timeout_sec)
    )
    try:
        return await asyncio.to_thread(
            _push_task_image_to_registry,
            client,
            tag,
            registry_repo,
        )
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _resolve_dockerfile_path(*, task_dir: Path, dockerfile: PurePosixPath) -> Path:
    if dockerfile.is_absolute() or ".." in dockerfile.parts:
        raise TaskImageBuildError(
            "environment.dockerfile must stay inside the task bundle; "
            f"got {dockerfile.as_posix()!r}",
        )
    path = task_dir.joinpath(*dockerfile.parts)
    if not path.is_file():
        raise TaskImageBuildError(
            f"environment.dockerfile {dockerfile.as_posix()!r} was not found "
            f"under materialized task bundle {task_dir}",
        )
    return path


def _resolve_build_context_path(
    *,
    task_dir: Path,
    dockerfile: Path,
    docker_build_context: PurePosixPath | None,
) -> Path:
    if docker_build_context is None:
        return task_dir
    if docker_build_context.is_absolute() or ".." in docker_build_context.parts:
        raise TaskImageBuildError(
            "environment.docker_build_context must stay inside the task "
            f"bundle; got {docker_build_context.as_posix()!r}",
        )
    path = task_dir.joinpath(*docker_build_context.parts)
    if not path.is_dir():
        raise TaskImageBuildError(
            "environment.docker_build_context "
            f"{docker_build_context.as_posix()!r} was not found under "
            f"materialized task bundle {task_dir}",
        )
    try:
        dockerfile.relative_to(path)
    except ValueError as exc:
        raise TaskImageBuildError(
            "environment.dockerfile must be inside "
            "environment.docker_build_context; got "
            f"dockerfile={dockerfile.relative_to(task_dir).as_posix()!r} "
            f"context={docker_build_context.as_posix()!r}",
        ) from exc
    return path


def _ensure_dockerfile_image(
    *,
    tag: str,
    task_config: TaskConfig,
    task_checksum: str,
    task_dir: Path,
    dockerfile: Path,
    build_context: Path,
    docker_api_timeout_sec: int | None = None,
    registry_repo: str | None = None,
    cpu_arch: str,
) -> None:
    configured_dockerfile = task_config.environment.dockerfile
    assert configured_dockerfile is not None
    client: Any = (
        docker.from_env()
        if docker_api_timeout_sec is None
        else docker.from_env(timeout=docker_api_timeout_sec)
    )
    try:
        try:
            client.images.get(tag)
            return
        except ImageNotFound:
            pass

        rel_dockerfile = dockerfile.relative_to(build_context).as_posix()
        _enforce_build_context_limits(build_context)
        client.images.build(
            path=str(build_context),
            dockerfile=rel_dockerfile,
            tag=tag,
            rm=True,
            forcerm=True,
            pull=False,
            platform=_docker_platform(cpu_arch),
            buildargs=task_config.environment.docker_build_args,
            target=task_config.environment.docker_build_target,
            labels={
                "loom.task-image": "true",
                "loom.task_id": task_config.task.id,
                "loom.task_checksum": task_checksum,
                "loom.task_dockerfile": rel_dockerfile,
                "loom.created-at": datetime.now(UTC).isoformat(),
            },
        )
        # #1169: this (non-contained) builder populates the shared registry so
        # containment-required workers can PULL this base image instead of
        # building it. Best-effort — a push failure must not fail the trial,
        # since the image is already built locally for this worker's own use.
        if registry_repo:
            try:
                _push_task_image_to_registry(client, tag, registry_repo)
            except Exception as exc:
                logger.warning(
                    "task image %s built but registry push to %s failed: %s",
                    tag,
                    registry_repo,
                    exc,
                )
    except TaskImageBuildError:
        raise
    except BuildError as exc:
        # docker-py's BuildError stringifies to only the failing RUN
        # command (e.g. "The command '/bin/sh -c pip install foo'
        # returned a non-zero code: 1") — useless for diagnosing WHY
        # the command failed. Walk the build_log iterator and surface
        # the tail of the captured stdout/stderr so operators can see
        # pip's actual error output (e.g. "ERROR: No matching
        # distribution found for pytest-jsonreport"). #319.
        tail, full_log = _format_build_log_sections(exc.build_log)
        diagnostic_detail = (
            f"failed to build Docker image {tag!r} from "
            f"{configured_dockerfile.as_posix()!r}: {exc}"
            + (f"\nbuild log (full):\n{full_log}" if full_log else "")
        )
        raise TaskImageBuildError(
            f"failed to build Docker image {tag!r} from "
            f"{configured_dockerfile.as_posix()!r}: {exc}"
            + (f"\nbuild log (last {_BUILD_LOG_TAIL_LINES} lines):\n{tail}" if tail else ""),
            diagnostic_detail=diagnostic_detail,
        ) from exc
    except Exception as exc:
        raise TaskImageBuildError(
            f"failed to build Docker image {tag!r} from "
            f"{configured_dockerfile.as_posix()!r}: {exc}",
        ) from exc
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _format_build_log_sections(build_log: Any) -> tuple[str, str]:
    """Return ``(tail, full)`` build-log strings from docker-py's iterator."""
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


def _enforce_build_context_limits(task_dir: Path) -> None:
    max_files = _operator_limit(
        ENV_BUILD_CONTEXT_MAX_FILES,
        DEFAULT_BUILD_CONTEXT_MAX_FILES,
    )
    max_bytes = _operator_limit(
        ENV_BUILD_CONTEXT_MAX_BYTES,
        DEFAULT_BUILD_CONTEXT_MAX_BYTES,
    )
    file_count = 0
    byte_count = 0

    for path in task_dir.rglob("*"):
        try:
            stat = path.lstat()
        except OSError as exc:
            raise TaskImageBuildError(
                f"failed to inspect Docker build context file {path}: {exc}",
            ) from exc
        if path.is_dir() and not path.is_symlink():
            continue

        file_count += 1
        if file_count > max_files:
            raise TaskImageBuildError(
                "Docker build context exceeds operator file limit "
                f"({file_count}>{max_files}) under {task_dir}",
            )

        byte_count += stat.st_size
        if byte_count > max_bytes:
            raise TaskImageBuildError(
                "Docker build context exceeds operator byte limit "
                f"({byte_count}>{max_bytes}) under {task_dir}",
            )


def _operator_limit(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise TaskImageBuildError(
            f"{name} must be a positive integer; got {raw!r}",
        ) from exc
    if value <= 0:
        raise TaskImageBuildError(
            f"{name} must be a positive integer; got {raw!r}",
        )
    return value
