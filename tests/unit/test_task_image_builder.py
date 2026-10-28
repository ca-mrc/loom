from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from loom_worker import task_image_builder
from loom_worker.control_plane_client import TaskImageBuildClaim
from loom_worker.trial_cache import ManagedImageCleanupResult


def _claim(*, cpu_arch: str = "x86_64") -> TaskImageBuildClaim:
    task_id = "benchmark/task"
    return TaskImageBuildClaim(
        id=UUID("00000000-0000-0000-0000-000000000123"),
        materialization_key="a" * 64,
        task_id=task_id,
        task_checksum="b" * 64,
        cpu_arch=cpu_arch,  # type: ignore[arg-type]
        task_config={
            "schema_version": "1",
            "task": {"id": task_id, "name": task_id},
            "environment": {
                "os": "linux",
                "cpu_arch": cpu_arch,
                "dockerfile": "environment/Dockerfile",
                "sidecars": [
                    {
                        "name": "database",
                        "dockerfile": "environment/database.Dockerfile",
                    }
                ],
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        },
        task_source="s3://loom-tasks/task",
        task_source_provenance={},
        attempt_count=1,
        max_attempts=3,
        lease_epoch=2,
        lease_expires_at="2026-08-14T12:00:00+00:00",
    )


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        trial_cache_registry_repo="registry.example/loom-task",
        docker_api_timeout_sec=120,
        fixtures_root=None,
        benchmark_cache=None,
        task_materialize_timeout_sec=300.0,
        heartbeat_interval_sec=0.01,
    )


class _Secret:
    def get_secret_value(self) -> str:
        return "builder-token"


def _cleanup_result(
    *,
    free_bytes: int = 25 * 1024**3,
    required_free_bytes: int = 20 * 1024**3,
    probe_available: bool = True,
) -> ManagedImageCleanupResult:
    return ManagedImageCleanupResult(
        docker_root="/var/lib/docker",
        storage_probe_path="/var/lib/docker",
        free_bytes=free_bytes,
        required_free_bytes=required_free_bytes,
        probe_available=probe_available,
        error_count=0,
    )


async def test_builder_uses_dedicated_idle_exit_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = 0

    class _IdleControlPlane:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        async def claim_task_image_materialization(self, **_kwargs: Any) -> None:
            nonlocal claims
            claims += 1
            if claims > 2:
                raise AssertionError("builder did not exit after its dedicated idle limit")
            return None

    clock = iter((0.0, 0.0, 121.0))
    monkeypatch.setattr(task_image_builder, "HttpControlPlaneClient", _IdleControlPlane)
    monkeypatch.setattr(
        task_image_builder,
        "evict_stale_managed_images_from_env",
        lambda _settings: _cleanup_result(),
    )

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(task_image_builder.asyncio, "sleep", no_sleep)

    await task_image_builder.run_builder(  # type: ignore[arg-type]
        SimpleNamespace(
            control_plane_url="http://cp:8080",
            token=_Secret(),
            docker_api_timeout_sec=30,
            task_image_builder_idle_exit_seconds=120,
            idle_exit_after_seconds=None,
            claim_poll_interval_sec=1,
        ),
        now=lambda: next(clock),
    )

    assert claims == 2


async def test_builder_evicts_managed_images_at_startup_and_after_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = iter((_claim(), None))
    evictions: list[object] = []

    class _OneClaimControlPlane:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        async def claim_task_image_materialization(
            self, **_kwargs: Any
        ) -> TaskImageBuildClaim | None:
            return next(claims)

    async def process(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(task_image_builder, "HttpControlPlaneClient", _OneClaimControlPlane)
    monkeypatch.setattr(task_image_builder, "process_task_image_claim", process)
    monkeypatch.setattr(
        task_image_builder,
        "evict_stale_managed_images_from_env",
        lambda settings: (evictions.append(settings), _cleanup_result())[1],
        raising=False,
    )

    settings = SimpleNamespace(
        control_plane_url="http://cp:8080",
        token=_Secret(),
        docker_api_timeout_sec=30,
        task_image_builder_idle_exit_seconds=0,
        claim_poll_interval_sec=1,
    )
    await task_image_builder.run_builder(  # type: ignore[arg-type]
        settings,
        now=lambda: 0.0,
    )

    assert evictions == [settings, settings]


async def test_builder_refreshes_storage_admission_after_an_empty_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admissions = iter((_cleanup_result(), _cleanup_result(free_bytes=19 * 1024**3)))
    events: list[str] = []
    claim_requests = 0

    class _IdleThenForbiddenControlPlane:
        def __init__(self, **_kwargs: Any) -> None:
            events.append("client")

        async def claim_task_image_materialization(self, **_kwargs: Any) -> None:
            nonlocal claim_requests
            claim_requests += 1
            if claim_requests > 1:
                raise AssertionError("builder requested a second claim after idle storage changed")
            events.append("claim")
            return None

    def prepare_storage(_settings: object) -> ManagedImageCleanupResult:
        events.append("admission")
        return next(admissions)

    async def idle_sleep(_seconds: float) -> None:
        events.append("sleep")

    monkeypatch.setattr(
        task_image_builder,
        "HttpControlPlaneClient",
        _IdleThenForbiddenControlPlane,
    )
    monkeypatch.setattr(
        task_image_builder,
        "evict_stale_managed_images_from_env",
        prepare_storage,
    )
    monkeypatch.setattr(task_image_builder.asyncio, "sleep", idle_sleep)

    with pytest.raises(task_image_builder.TaskImageBuilderFatalError, match="storage admission"):
        await task_image_builder.run_builder(  # type: ignore[arg-type]
            SimpleNamespace(
                control_plane_url="http://cp:8080",
                token=_Secret(),
                docker_api_timeout_sec=30,
                task_image_builder_idle_exit_seconds=None,
                claim_poll_interval_sec=1,
            )
        )

    assert events[:2] == ["admission", "client"]
    assert claim_requests == 1


@pytest.mark.parametrize(
    "cleanup_result",
    [
        _cleanup_result(probe_available=False),
        _cleanup_result(free_bytes=19 * 1024**3),
    ],
)
async def test_builder_rejects_storage_before_client_construction_or_claim(
    monkeypatch: pytest.MonkeyPatch,
    cleanup_result: ManagedImageCleanupResult,
) -> None:
    client_constructed = False

    class _ForbiddenControlPlane:
        def __init__(self, **_kwargs: Any) -> None:
            nonlocal client_constructed
            client_constructed = True

    monkeypatch.setattr(task_image_builder, "HttpControlPlaneClient", _ForbiddenControlPlane)
    monkeypatch.setattr(
        task_image_builder,
        "evict_stale_managed_images_from_env",
        lambda _settings: cleanup_result,
    )

    with pytest.raises(
        task_image_builder.TaskImageBuilderFatalError,
        match="storage admission",
    ):
        await task_image_builder.run_builder(  # type: ignore[arg-type]
            SimpleNamespace(
                control_plane_url="http://cp:8080",
                token=_Secret(),
                docker_api_timeout_sec=30,
                task_image_builder_idle_exit_seconds=0,
                claim_poll_interval_sec=1,
            ),
        )

    assert client_constructed is False


async def test_materialization_rejects_non_execution_builder_architecture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(task_image_builder, "host_cpu_arch", lambda: "x86_64")

    with pytest.raises(RuntimeError, match="execution architecture"):
        await task_image_builder.materialize_and_publish_task_images(
            _claim(cpu_arch="arm64"),
            _settings(),  # type: ignore[arg-type]
        )


async def test_published_image_architecture_must_match_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = SimpleNamespace(attrs={"Architecture": "arm64"})
    client = SimpleNamespace(
        images=SimpleNamespace(get=lambda _tag: image),
        close=lambda: None,
    )
    monkeypatch.setattr(task_image_builder.docker, "from_env", lambda **_kwargs: client)

    with pytest.raises(task_image_builder.TaskImageBuildError, match="architecture mismatch"):
        await task_image_builder.verify_local_image_architecture(
            tag="loom-task:x86_64",
            expected_cpu_arch="x86_64",
            docker_api_timeout_sec=30,
        )


@pytest.mark.parametrize("native_recipe", [False, True])
async def test_materialization_builds_and_publishes_every_dockerfile_component(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    native_recipe: bool,
) -> None:
    claim = _claim()
    if native_recipe:
        from loom.task_image_recipe import NATIVE_HARBOR_PREPARATION, task_image_materialization_key
        claim.task_config["upstream_conversion"] = {"importer": "loom.harbor-native.v1", "original_config": "upstream-task.toml", "changes": []}
        claim.task_config["environment"]["docker_build_args"] = {"LOOM_PREPARATION_OWNER": "root"}
        claim = replace(claim, materialization_key=task_image_materialization_key(
            task_id=claim.task_id, task_checksum=claim.task_checksum, cpu_arch=claim.cpu_arch,
            preparation_revision=NATIVE_HARBOR_PREPARATION,
        ))
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text("FROM alpine:3.20\nARG TASK_USER=nobody\nUSER ${TASK_USER}\n")
    (task_dir / "environment" / "database.Dockerfile").write_text("FROM postgres\n")
    observed: dict[str, Any] = {}
    monkeypatch.setattr(task_image_builder, "host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(task_image_builder, "_build_worker_object_store", lambda _s: object())

    async def materialize(**kwargs: Any) -> Path:
        observed["materialize"] = kwargs
        return task_dir

    async def resolve(**kwargs: Any) -> str:
        observed["resolve"] = kwargs
        assert ("mkdir -p /solution" in (task_dir / "environment/Dockerfile").read_text()) is native_recipe
        if native_recipe:
            assert "ARG LOOM_PREPARATION_OWNER_1=${TASK_USER}" in (task_dir / "environment/Dockerfile").read_text()
        assert kwargs["task_checksum"] == (claim.materialization_key if native_recipe else claim.task_checksum)
        return "loom-task:base"

    async def sidecars(**kwargs: Any) -> dict[str, str]:
        observed["sidecars"] = kwargs
        return {"database": "loom-sidecar:database"}

    async def publish(*, tag: str, registry_repo: str, **_kwargs: Any) -> str:
        observed.setdefault("publish", []).append((tag, registry_repo))
        digest = "1" * 64 if tag.startswith("loom-task") else "2" * 64
        return f"{registry_repo}@sha256:{digest}"

    async def record_publication(component: str, registry_image: str) -> bool:
        observed.setdefault("recorded", []).append((component, registry_image))
        return True

    async def verify_architecture(**kwargs: Any) -> None:
        observed.setdefault("verified", []).append(kwargs)

    monkeypatch.setattr(task_image_builder, "_materialize_task_dir", materialize)
    monkeypatch.setattr(task_image_builder, "sha256_of_dir", lambda _path: claim.task_checksum)
    monkeypatch.setattr(task_image_builder, "resolve_task_image", resolve)
    monkeypatch.setattr(task_image_builder, "build_task_sidecar_images", sidecars)
    monkeypatch.setattr(task_image_builder, "publish_local_image_to_registry", publish)
    monkeypatch.setattr(task_image_builder, "verify_local_image_architecture", verify_architecture)
    monkeypatch.setattr(
        task_image_builder.shutil,
        "rmtree",
        lambda path, **_kwargs: observed.setdefault("removed", path),
    )

    registry_images = await task_image_builder.materialize_and_publish_task_images(
        claim,
        _settings(),  # type: ignore[arg-type]
        publication_recorder=record_publication,
    )

    assert registry_images == {
        "task": "registry.example/loom-task@sha256:" + "1" * 64,
        "sidecar:database": "registry.example/loom-task@sha256:" + "2" * 64,
    }
    assert observed["resolve"]["cpu_arch"] == "x86_64"
    assert observed["sidecars"]["cpu_arch"] == "x86_64"
    assert observed["publish"] == [
        ("loom-task:base", "registry.example/loom-task"),
        ("loom-sidecar:database", "registry.example/loom-task"),
    ]
    assert observed["recorded"] == [
        ("task", "registry.example/loom-task@sha256:" + "1" * 64),
        ("sidecar:database", "registry.example/loom-task@sha256:" + "2" * 64),
    ]
    assert observed["verified"] == [
        {"tag": "loom-task:base", "expected_cpu_arch": "x86_64", "docker_api_timeout_sec": 120},
        {
            "tag": "loom-sidecar:database",
            "expected_cpu_arch": "x86_64",
            "docker_api_timeout_sec": 120,
        },
    ]
    assert observed["removed"] == task_dir


async def test_publication_recorder_failure_retains_just_pushed_digest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    claim = _claim()
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text("FROM alpine\n")
    registry_image = "registry.example/loom-task@sha256:" + "4" * 64
    monkeypatch.setattr(task_image_builder, "host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(task_image_builder, "_build_worker_object_store", lambda _s: object())

    async def materialize(**_kwargs: Any) -> Path:
        return task_dir

    async def resolve(**_kwargs: Any) -> str:
        return "loom-task:base"

    async def sidecars(**_kwargs: Any) -> dict[str, str]:
        return {}

    async def publish(**_kwargs: Any) -> str:
        return registry_image

    async def record_publication(_component: str, _registry_image: str) -> bool:
        raise ConnectionError("publication endpoint unavailable")

    async def verify_architecture(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(task_image_builder, "_materialize_task_dir", materialize)
    monkeypatch.setattr(task_image_builder, "sha256_of_dir", lambda _path: claim.task_checksum)
    monkeypatch.setattr(task_image_builder, "resolve_task_image", resolve)
    monkeypatch.setattr(task_image_builder, "build_task_sidecar_images", sidecars)
    monkeypatch.setattr(task_image_builder, "publish_local_image_to_registry", publish)
    monkeypatch.setattr(task_image_builder, "verify_local_image_architecture", verify_architecture)

    with pytest.raises(task_image_builder.TaskImagePublicationError) as exc_info:
        await task_image_builder.materialize_and_publish_task_images(
            claim,
            _settings(),  # type: ignore[arg-type]
            publication_recorder=record_publication,
        )

    assert exc_info.value.registry_images == {"task": registry_image}


async def test_materialization_rejects_bundle_metadata_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    claim = replace(
        _claim(),
        task_source_provenance={
            "bundle_file_metadata_sha256": "sha256:" + "0" * 64,
        },
    )
    task_dir = tmp_path / "task"
    (task_dir / "environment").mkdir(parents=True)
    (task_dir / "environment" / "Dockerfile").write_text("FROM alpine\n")
    monkeypatch.setattr(task_image_builder, "host_cpu_arch", lambda: "x86_64")
    monkeypatch.setattr(task_image_builder, "_build_worker_object_store", lambda _s: object())

    async def materialize(**_kwargs: Any) -> Path:
        return task_dir

    async def resolve(**_kwargs: Any) -> str:
        return "loom-task:base"

    async def publish(**_kwargs: Any) -> str:
        return "registry.example/loom-task@sha256:" + "1" * 64

    async def sidecars(**_kwargs: Any) -> dict[str, str]:
        return {}

    monkeypatch.setattr(task_image_builder, "_materialize_task_dir", materialize)
    monkeypatch.setattr(task_image_builder, "sha256_of_dir", lambda _path: claim.task_checksum)
    monkeypatch.setattr(task_image_builder, "resolve_task_image", resolve)
    monkeypatch.setattr(task_image_builder, "build_task_sidecar_images", sidecars)
    monkeypatch.setattr(task_image_builder, "publish_local_image_to_registry", publish)

    with pytest.raises(task_image_builder.TaskImageBuildError, match="metadata"):
        await task_image_builder.materialize_and_publish_task_images(
            claim,
            _settings(),  # type: ignore[arg-type]
        )


class _FakeControlPlane:
    def __init__(self) -> None:
        self.heartbeats = 0
        self.completed: dict[str, str] | None = None
        self.failed = False
        self.failed_registry_images: dict[str, str] = {}
        self.recorded_publications: list[tuple[int, str, str]] = []

    async def start_task_image_materialization(self, **_kwargs: Any) -> bool:
        return True

    async def heartbeat_task_image_materialization(self, **_kwargs: Any) -> bool:
        self.heartbeats += 1
        return True

    async def complete_task_image_materialization(
        self,
        *,
        registry_images: dict[str, str],
        **_kwargs: Any,
    ) -> bool:
        self.completed = registry_images
        return True

    async def fail_task_image_materialization(self, **kwargs: Any) -> bool:
        self.failed = True
        self.failed_registry_images = dict(kwargs["registry_images"])
        return True

    async def record_task_image_publication(self, **kwargs: Any) -> bool:
        self.recorded_publications.append(
            (
                int(kwargs["attempt_count"]),
                str(kwargs["component"]),
                str(kwargs["registry_image"]),
            )
        )
        return True


async def test_process_claim_heartbeats_and_completes_fenced_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _FakeControlPlane()

    async def materialize(_claim: Any, _settings: Any, **kwargs: Any) -> dict[str, str]:
        assert await kwargs["publication_recorder"](
            "task",
            "registry.example/task@sha256:" + "d" * 64,
        )
        await asyncio.sleep(0.04)
        return {"task": "registry.example/task@sha256:" + "d" * 64}

    monkeypatch.setattr(
        task_image_builder,
        "materialize_and_publish_task_images",
        materialize,
    )

    await task_image_builder.process_task_image_claim(
        control_plane,  # type: ignore[arg-type]
        claim=_claim(),
        builder_id="builder-a",
        settings=_settings(),  # type: ignore[arg-type]
    )

    assert control_plane.heartbeats >= 1
    assert control_plane.completed == {
        "task": "registry.example/task@sha256:" + "d" * 64,
    }
    assert control_plane.failed is False
    assert control_plane.recorded_publications == [
        (1, "task", "registry.example/task@sha256:" + "d" * 64),
    ]


async def test_process_claim_reports_partial_registry_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _FakeControlPlane()
    published = {"task": "registry.example/task@sha256:" + "e" * 64}

    async def materialize(_claim: Any, _settings: Any, **_kwargs: Any) -> dict[str, str]:
        raise task_image_builder.TaskImagePublicationError(
            "sidecar publication failed",
            registry_images=published,
        )

    monkeypatch.setattr(
        task_image_builder,
        "materialize_and_publish_task_images",
        materialize,
    )

    await task_image_builder.process_task_image_claim(
        control_plane,  # type: ignore[arg-type]
        claim=_claim(),
        builder_id="builder-a",
        settings=_settings(),  # type: ignore[arg-type]
    )

    assert control_plane.failed is True
    assert control_plane.failed_registry_images == published


async def test_process_claim_preserves_publication_evidence_when_completion_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published = {"task": "registry.example/task@sha256:" + "f" * 64}

    class _CompletionFailsControlPlane(_FakeControlPlane):
        async def complete_task_image_materialization(self, **_kwargs: Any) -> bool:
            raise RuntimeError("control-plane connection unavailable")

    control_plane = _CompletionFailsControlPlane()

    async def materialize(_claim: Any, _settings: Any, **_kwargs: Any) -> dict[str, str]:
        return published

    monkeypatch.setattr(
        task_image_builder,
        "materialize_and_publish_task_images",
        materialize,
    )

    await task_image_builder.process_task_image_claim(
        control_plane,  # type: ignore[arg-type]
        claim=_claim(),
        builder_id="builder-a",
        settings=_settings(),  # type: ignore[arg-type]
    )

    assert control_plane.failed_registry_images == published


async def test_build_timeout_reports_failure_and_terminates_builder_allocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = _FakeControlPlane()

    async def materialize(_claim: Any, _settings: Any, **_kwargs: Any) -> dict[str, str]:
        raise task_image_builder.TaskImageBuildTimeoutError("build timed out")

    monkeypatch.setattr(
        task_image_builder,
        "materialize_and_publish_task_images",
        materialize,
    )

    with pytest.raises(task_image_builder.TaskImageBuilderFatalError, match="timed out"):
        await task_image_builder.process_task_image_claim(
            control_plane,  # type: ignore[arg-type]
            claim=_claim(),
            builder_id="builder-a",
            settings=_settings(),  # type: ignore[arg-type]
        )

    assert control_plane.failed is True
