from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from loom_worker import main_loop as ml
from loom_worker.runner_pool import RunnerPool
from loom_worker.setup_admission import NodeHealthSnapshot


class _Settings:
    def __init__(
        self,
        *,
        max_concurrent: int,
        blocking_io_max_workers: int | None = None,
    ) -> None:
        self.max_concurrent = max_concurrent
        self.blocking_io_max_workers = blocking_io_max_workers


class _RegistrationSettings:
    hostname = "worker-host"
    max_concurrent = 3
    pool_name = "worker-pool"
    sandbox_identity = "production"
    candidate_sha = "a" * 40
    compose_project = "loom-production-aaaaaaaaaaaa-40740"
    executor_worker_credential = None


class _LegacyRegistrationSettings:
    hostname = "worker-host"
    max_concurrent = 3
    pool_name = "worker-pool"
    executor_worker_credential = None


class _ProtectedRegistrationSettings(_LegacyRegistrationSettings):
    class _Secret:
        def get_secret_value(self) -> str:
            return "launcher-bound-credential"

    executor_worker_credential = _Secret()


class _ObjectStoreSettings:
    minio_endpoint = "http://minio:9000"
    minio_region = "us-east-1"
    minio_max_pool_connections = 512
    minio_connect_timeout_sec = 7.5
    minio_read_timeout_sec = 180.0
    minio_operation_timeout_sec = 600.0
    minio_operation_attempts = 4

    class _Secret:
        def __init__(self, value: str) -> None:
            self.value = value

        def get_secret_value(self) -> str:
            return self.value

    minio_access_key = _Secret("access")
    minio_secret_key = _Secret("secret")


class _OrphanCleanupSettings:
    control_plane_url = "http://loom-control-plane:8080"
    trajectory_cache_dir = "/tmp/not-used"

    class _Secret:
        def get_secret_value(self) -> str:
            return "worker-token"

    token = _Secret()


def _healthy_setup_node() -> NodeHealthSnapshot:
    return NodeHealthSnapshot(
        io_full_avg10=0.0,
        swap_total_mb=0,
        swap_free_mb=0,
        d_state_processes=0,
    )


def test_blocking_io_worker_count_defaults_from_trial_concurrency() -> None:
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=5),
        )
        == 32
    )
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=64),
        )
        == 256
    )
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=128),
        )
        == 256
    )


def test_blocking_io_worker_count_accepts_operator_override() -> None:
    assert (
        ml._resolve_blocking_io_max_workers(  # type: ignore[attr-defined]
            _Settings(max_concurrent=64, blocking_io_max_workers=96),
        )
        == 96
    )


def test_idle_exit_tracker_disabled_by_default() -> None:
    tracker = ml._IdleExitTracker(after_seconds=None, now=lambda: 100.0)  # type: ignore[attr-defined]

    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=0, in_flight=0) is False


def test_idle_exit_tracker_exits_after_no_work_window() -> None:
    now = iter([100.0, 104.0, 106.0])
    tracker = ml._IdleExitTracker(after_seconds=5.0, now=lambda: next(now))  # type: ignore[attr-defined]

    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=0, in_flight=0) is True
    assert tracker.idle_for_seconds == 6.0


def test_idle_exit_tracker_active_trial_prevents_exit() -> None:
    now = iter([100.0, 106.0, 112.0])
    tracker = ml._IdleExitTracker(after_seconds=5.0, now=lambda: next(now))  # type: ignore[attr-defined]

    assert tracker.observe(claimed=0, in_flight=1) is False
    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=0, in_flight=0) is True
    assert tracker.idle_for_seconds == 6.0


def test_idle_exit_tracker_claim_resets_idle_timer() -> None:
    now = iter([100.0, 104.0, 109.0, 112.0, 115.0])
    tracker = ml._IdleExitTracker(after_seconds=5.0, now=lambda: next(now))  # type: ignore[attr-defined]

    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=1, in_flight=1) is False
    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.observe(claimed=0, in_flight=0) is False
    assert tracker.idle_for_seconds == 3.0


def test_managed_image_eviction_tracker_runs_periodically() -> None:
    now = iter([100.0, 3_699.0, 3_700.0, 7_299.0, 7_300.0])
    tracker = ml._PeriodicMaintenanceTracker(  # type: ignore[attr-defined]
        interval_seconds=3_600,
        now=lambda: next(now),
    )

    assert tracker.due() is False
    assert tracker.due() is True
    assert tracker.due() is False
    assert tracker.due() is True


def test_configure_blocking_io_executor_sets_loop_default(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    created: list[tuple[int, str]] = []
    installed: list[object] = []

    class _FakeExecutor:
        def __init__(self, *, max_workers: int, thread_name_prefix: str) -> None:
            created.append((max_workers, thread_name_prefix))

    class _FakeLoop:
        def set_default_executor(self, executor: object) -> None:
            installed.append(executor)

    monkeypatch.setattr(ml, "ThreadPoolExecutor", _FakeExecutor)
    monkeypatch.setattr(ml.asyncio, "get_running_loop", lambda: _FakeLoop())

    ml._configure_blocking_io_executor(  # type: ignore[attr-defined]
        _Settings(max_concurrent=8),
    )

    assert created == [(32, "loom-worker-io")]
    assert len(installed) == 1


def test_worker_object_store_uses_worker_s3_timeout_and_pool_config(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    created: dict[str, object] = {}

    class _FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            created.update(kwargs)

    monkeypatch.setattr(ml, "MinioObjectStore", _FakeObjectStore)

    store = ml._build_worker_object_store(_ObjectStoreSettings())  # type: ignore[attr-defined]

    assert isinstance(store, _FakeObjectStore)
    assert created == {
        "endpoint_url": "http://minio:9000",
        "access_key": "access",
        "secret_key": "secret",
        "region": "us-east-1",
        "max_pool_connections": 512,
        "connect_timeout": 7.5,
        "read_timeout": 180.0,
        "operation_timeout": 600.0,
        "operation_attempts": 4,
    }


async def test_register_worker_with_retry_retries_control_plane_dns_failure() -> None:
    from loom.startup_retry import StartupRetryConfig

    worker_id = uuid4()
    attempts = 0
    sleeps: list[float] = []

    class _FakeCPClient:
        async def register(self, **kwargs: object) -> dict[str, object]:
            nonlocal attempts
            attempts += 1
            assert kwargs == {
                "hostname": "worker-host",
                "version": "0.0.1",
                "capabilities": ml._DEFAULT_CAPS,  # type: ignore[attr-defined]
                "max_concurrent": 3,
                "pool_name": "worker-pool",
            }
            if attempts < 3:
                request = httpx.Request("POST", "http://loom-control-plane/workers/register")
                raise httpx.ConnectError(
                    "[Errno -3] Temporary failure in name resolution",
                    request=request,
                )
            return {"worker_id": str(worker_id)}

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)

    info = await ml._register_worker_with_retry(  # type: ignore[attr-defined]
        cp_client=_FakeCPClient(),
        settings=_LegacyRegistrationSettings(),
        retry_config=StartupRetryConfig(
            max_attempts=4,
            base_backoff_sec=0.1,
            max_backoff_sec=1.0,
            budget_sec=30.0,
            jitter_sec=0.0,
        ),
        sleep=_sleep,
    )

    assert info == {"worker_id": str(worker_id)}
    assert attempts == 3
    assert sleeps == [0.1, 0.2]


async def test_register_worker_with_retry_does_not_retry_auth_failure() -> None:
    from loom.startup_retry import StartupRetryConfig

    attempts = 0

    class _FakeCPClient:
        async def register(self, **_kwargs: object) -> dict[str, object]:
            nonlocal attempts
            attempts += 1
            request = httpx.Request("POST", "http://loom-control-plane/workers/register")
            response = httpx.Response(401, request=request, json={"detail": "bad token"})
            raise httpx.HTTPStatusError(
                "401 Unauthorized",
                request=request,
                response=response,
            )

    async def _sleep(_seconds: float) -> None:
        raise AssertionError("auth failures must not sleep/retry")

    try:
        await ml._register_worker_with_retry(  # type: ignore[attr-defined]
            cp_client=_FakeCPClient(),
            settings=_LegacyRegistrationSettings(),
            retry_config=StartupRetryConfig(
                max_attempts=4,
                base_backoff_sec=0.1,
                max_backoff_sec=1.0,
                budget_sec=30.0,
                jitter_sec=0.0,
            ),
            sleep=_sleep,
        )
    except httpx.HTTPStatusError as exc:
        assert exc.response.status_code == 401
    else:  # pragma: no cover - defensive clarity
        raise AssertionError("expected HTTPStatusError")

    assert attempts == 1


@pytest.mark.legacy_pool
async def test_register_worker_with_retry_forwards_launcher_credential() -> None:
    recorded: dict[str, object] = {}

    class _FakeCPClient:
        async def register(self, **kwargs: object) -> dict[str, object]:
            recorded.update(kwargs)
            return {"worker_id": str(uuid4())}

    await ml._register_worker_with_retry(  # type: ignore[attr-defined]
        cp_client=_FakeCPClient(),
        settings=_ProtectedRegistrationSettings(),
    )

    assert recorded["executor_worker_credential"] == "launcher-bound-credential"


async def test_pipeline_registration_advertises_both_work_kinds(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    recorded: dict[str, object] = {}

    class _FakeCPClient:
        async def register(self, **kwargs: object) -> dict[str, object]:
            recorded.update(kwargs)
            return {
                "worker_id": str(uuid4()),
                "capability_snapshot_digest": "sha256:" + "a" * 64,
            }

    monkeypatch.setattr(
        ml,
        "_pipeline_registration_payload",
        lambda _settings: {
            "capability_snapshot": {
                "schema_version": "loom.worker-capabilities.v1",
            },
        },
    )
    await ml._register_worker_with_retry(  # type: ignore[attr-defined]
        cp_client=_FakeCPClient(),
        settings=_LegacyRegistrationSettings(),
        pipeline_enabled=True,
    )

    assert recorded["supported_work_kinds"] == ["trial", "execution_attempt"]


def test_worker_orphan_cleanup_retries_transient_control_plane_lookup(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    from loom.startup_retry import StartupRetryConfig

    worker_id = uuid4()
    attempts = 0
    sleeps: list[float] = []

    def _cleanup(**kwargs: object) -> list[object]:
        nonlocal attempts
        attempts += 1
        assert kwargs["owned_worker_id"] == worker_id
        if attempts < 3:
            request = httpx.Request("GET", "http://loom-control-plane:8080/trials/t")
            raise httpx.ConnectError("[Errno 111] Connection refused", request=request)
        return []

    monkeypatch.setattr(ml, "cleanup_orphan_trajectories", _cleanup)

    ml._run_orphan_cleanup(  # type: ignore[attr-defined]
        _OrphanCleanupSettings(),
        worker_id,
        retry_config=StartupRetryConfig(
            max_attempts=4,
            base_backoff_sec=0.1,
            max_backoff_sec=1.0,
            budget_sec=30.0,
            jitter_sec=0.0,
        ),
        sleep=sleeps.append,
    )

    assert attempts == 3
    assert sleeps == [0.1, 0.2]


def test_docker_registry_auth_summary_reports_only_secret_free_metadata(
    tmp_path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    docker_config = tmp_path / "docker"
    docker_config.mkdir()
    (docker_config / "config.json").write_text(
        json.dumps(
            {
                "auths": {
                    "https://index.docker.io/v1/": {"auth": "base64-secret"},
                    "ghcr.io": {"identitytoken": "secret-token"},
                },
                "credsStore": "desktop",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DOCKER_CONFIG", str(docker_config))

    summary = ml._docker_registry_auth_summary()  # type: ignore[attr-defined]

    assert summary == {
        "config_path": str(docker_config / "config.json"),
        "present": True,
        "auth_registries": ["ghcr.io", "https://index.docker.io/v1/"],
        "uses_credential_store": True,
    }


def test_docker_registry_auth_summary_handles_missing_config(
    tmp_path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    docker_config = tmp_path / "missing"
    monkeypatch.setenv("DOCKER_CONFIG", str(docker_config))

    summary = ml._docker_registry_auth_summary()  # type: ignore[attr-defined]

    assert summary == {
        "config_path": str(docker_config / "config.json"),
        "present": False,
        "auth_registries": [],
        "uses_credential_store": False,
    }


class _ClaimingCP:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self.payloads = list(payloads)
        self.claim_calls = 0

    async def claim(self, *, worker_id, caps):  # type: ignore[no-untyped-def]
        self.claim_calls += 1
        if not self.payloads:
            return None
        return self.payloads.pop(0)


class _FailingClaimCP:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.claim_calls = 0

    async def claim(self, *, worker_id, caps):  # type: ignore[no-untyped-def]
        self.claim_calls += 1
        raise self.exc


async def test_claim_cycle_fills_available_pool_capacity(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    pool = RunnerPool(max_concurrent=3)
    release = asyncio.Event()
    cp = _ClaimingCP(
        [
            {
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": f"task-{idx}",
                "config": {"agent_name": "oracle", "agent_model": None},
            }
            for idx in range(3)
        ]
    )

    async def _fake_spawn_trial(**kwargs) -> None:  # type: ignore[no-untyped-def]
        async def _held() -> None:
            await release.wait()

        await kwargs["pool"].spawn(_held())

    monkeypatch.setattr(ml, "_spawn_trial", _fake_spawn_trial)

    claimed = await ml._claim_available_trials(  # type: ignore[attr-defined]
        pool=pool,
        settings=_Settings(max_concurrent=3),
        cp_client=cp,
        gateway_client=None,
        object_store=None,
        worker_id=uuid4(),
        sandbox_allocator=None,
        sandbox_singleton=None,
        read_setup_health=_healthy_setup_node,
    )

    assert claimed == 3
    assert cp.claim_calls == 3
    assert pool.in_flight == 3
    release.set()
    await pool.wait_all(timeout=2.0)


async def test_claim_cycle_treats_control_plane_transport_error_as_no_claim() -> None:
    cp = _FailingClaimCP(httpx.ReadError("control plane disconnected"))

    claimed = await ml._claim_available_trials(  # type: ignore[attr-defined]
        pool=RunnerPool(max_concurrent=3),
        settings=_Settings(max_concurrent=3),
        cp_client=cp,
        gateway_client=None,
        object_store=None,
        worker_id=uuid4(),
        sandbox_allocator=None,
        sandbox_singleton=None,
        read_setup_health=_healthy_setup_node,
    )

    assert claimed == 0
    assert cp.claim_calls == 1


async def test_claim_cycle_does_not_claim_when_node_is_unhealthy() -> None:
    cp = _ClaimingCP([])

    claimed = await ml._claim_available_trials(  # type: ignore[attr-defined]
        pool=RunnerPool(max_concurrent=3),
        settings=_Settings(max_concurrent=3),
        cp_client=cp,
        gateway_client=None,
        object_store=None,
        worker_id=uuid4(),
        sandbox_allocator=None,
        sandbox_singleton=None,
        read_setup_health=lambda: NodeHealthSnapshot(
            io_full_avg10=70.0,
            swap_total_mb=0,
            swap_free_mb=0,
            d_state_processes=0,
        ),
    )

    assert claimed == 0
    assert cp.claim_calls == 0


async def test_claim_cycle_rechecks_node_health_before_each_claim(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    pool = RunnerPool(max_concurrent=3)
    release = asyncio.Event()
    cp = _ClaimingCP(
        [
            {
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": "task-0",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            {
                "trial_id": str(uuid4()),
                "team_id": str(uuid4()),
                "task_id": "task-1",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        ]
    )
    snapshots = iter(
        [
            NodeHealthSnapshot(
                io_full_avg10=1.0,
                swap_total_mb=0,
                swap_free_mb=0,
                d_state_processes=0,
            ),
            NodeHealthSnapshot(
                io_full_avg10=70.0,
                swap_total_mb=0,
                swap_free_mb=0,
                d_state_processes=0,
            ),
        ]
    )

    async def _fake_spawn_trial(**kwargs) -> None:  # type: ignore[no-untyped-def]
        async def _held() -> None:
            await release.wait()

        await kwargs["pool"].spawn(_held())

    monkeypatch.setattr(ml, "_spawn_trial", _fake_spawn_trial)

    claimed = await ml._claim_available_trials(  # type: ignore[attr-defined]
        pool=pool,
        settings=_Settings(max_concurrent=3),
        cp_client=cp,
        gateway_client=None,
        object_store=None,
        worker_id=uuid4(),
        sandbox_allocator=None,
        sandbox_singleton=None,
        read_setup_health=lambda: next(snapshots),
    )

    assert claimed == 1
    assert cp.claim_calls == 1
    release.set()
    await pool.wait_all(timeout=2.0)
