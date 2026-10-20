from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml

from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.system import docker_compose


def test_compose_failure_reports_bounded_redacted_output_without_replacing_error(
    monkeypatch: Any, capsys: Any,
) -> None:
    secret = "s" * 40_000
    failure = subprocess.CalledProcessError(
        1,
        ["docker", "compose", "up", "--build"],
        output=(
            "discarded build preamble\n" + "build progress\n" * 4_000
            + f"Bearer {secret}\npackage build failed: concrete root cause\n"
        ),
        stderr="Dockerfile.control-plane:20\nloom_w_worker-secret\n",
    )

    def fail(*args: Any, **kwargs: Any) -> None:
        raise failure

    monkeypatch.setattr(docker_compose.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError) as caught:
        docker_compose._compose("up", "--build")

    assert caught.value is failure
    output = capsys.readouterr().err
    assert "package build failed: concrete root cause" in output
    assert "Dockerfile.control-plane:20" in output
    assert "discarded build preamble" not in output
    assert secret[:64] not in output
    assert "loom_w_worker-secret" not in output
    assert "[REDACTED:bearer]" in output
    assert "[REDACTED:loom-token]" in output
    assert len(output) <= 33_000


@pytest.mark.parametrize("requested_port", [None, "61234"])
@pytest.mark.parametrize("service,container_port,setting", [
    ("postgres", "5432", "POSTGRES"), ("registry", "5000", "REGISTRY"),
    ("minio", "9000", "MINIO"), ("llm-gateway", "9100", "GATEWAY"),
    ("control-plane", "8080", "CONTROL_PLANE"),
])
def test_compose_services_use_docker_allocated_loopback_ports(
    monkeypatch: Any, requested_port: str | None, service: str, container_port: str, setting: str,
) -> None:
    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert command[-3:] == ["port", service, container_port]
        assert kwargs["env"][f"LOOM_SYSTEM_{setting}_PORT"] == (requested_port or "0")
        assert 0 < kwargs["timeout"] <= 5
        return subprocess.CompletedProcess(command, 0, stdout="127.0.0.1:61234\n", stderr="")

    monkeypatch.delenv(f"LOOM_SYSTEM_{setting}_PORT", raising=False)
    if requested_port is not None:
        monkeypatch.setenv(f"LOOM_SYSTEM_{setting}_PORT", requested_port)
    monkeypatch.setattr(docker_compose.subprocess, "run", run)
    if service == "postgres":
        assert docker_compose.database_url() == "postgresql+psycopg://loom:loom@127.0.0.1:61234/loom"
    else:
        assert docker_compose._published_port(service, container_port) == 61234


@pytest.mark.parametrize(
    "binding",
    ["", "0.0.0.0:61234", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:61234\n127.0.0.1:61235"],
)
@pytest.mark.parametrize("service,port", [("postgres", "5432"), ("registry", "5000")])
def test_compose_rejects_missing_or_ambiguous_binding(
    monkeypatch: Any, binding: str, service: str, port: str,
) -> None:
    monkeypatch.setattr(
        docker_compose,
        "_compose",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, stdout=binding, stderr=""),
    )
    with pytest.raises(RuntimeError, match="database port" if service == "postgres" else "registry port"):
        if service == "postgres":
            docker_compose.database_url()
        else:
            docker_compose._published_port(service, port)


def test_additional_fixture_seed_uses_running_stack_database(monkeypatch: Any) -> None:
    allocated_database = "postgresql+psycopg://loom:loom@127.0.0.1:61234/loom"

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert command[command.index("--db-url") + 1] == allocated_database
        assert command[command.index("--task-id") + 1] == "multi-step-3"
        assert command[command.index("--print") + 1] == "team"
        return subprocess.CompletedProcess(command, 0, stdout="team-token\n", stderr="")

    monkeypatch.setattr(docker_compose, "database_url", lambda: allocated_database)
    monkeypatch.setattr(docker_compose.subprocess, "run", run)
    assert docker_compose.run_seed(task_id="multi-step-3") == "team-token"


def test_compose_uses_available_pinned_minio_fixture() -> None:
    compose = yaml.safe_load(docker_compose.COMPOSE_FILE.read_text(encoding="utf-8"))
    assert (
        compose["services"]["minio"]["image"]
        == "${LOOM_SYSTEM_MINIO_IMAGE:-" + MINIO_TEST_IMAGE + "}"
    )


def test_compose_uses_one_test_only_step_jwt_signing_key() -> None:
    compose = yaml.safe_load(docker_compose.COMPOSE_FILE.read_text(encoding="utf-8"))
    services = compose["services"]

    gateway_key = services["llm-gateway"]["environment"]["LOOM_GW_STEP_JWT_SIGNING_KEY"]
    control_plane_key = services["control-plane"]["environment"]["LOOM_CP_STEP_JWT_SIGNING_KEY"]

    assert gateway_key == control_plane_key
    assert "do-not-use-in-prod" in gateway_key


def test_compose_supplies_gateway_object_store_configuration() -> None:
    compose = yaml.safe_load(docker_compose.COMPOSE_FILE.read_text(encoding="utf-8"))
    gateway = compose["services"]["llm-gateway"]

    assert gateway["environment"]["LOOM_GW_MINIO_ENDPOINT"] == "http://minio:9000"
    assert gateway["environment"]["LOOM_GW_MINIO_ACCESS_KEY"] == "loomtest"
    assert gateway["environment"]["LOOM_GW_MINIO_SECRET_KEY"] == "loomtest"
    assert gateway["depends_on"]["minio"] == {"condition": "service_healthy"}


def test_compose_mounts_runtime_fixtures_and_uses_fast_test_cancellation() -> None:
    compose = yaml.safe_load(docker_compose.COMPOSE_FILE.read_text(encoding="utf-8"))
    services = compose["services"]
    worker = compose["services"]["worker"]
    builder = services["task-image-builder"]

    assert worker["environment"]["LOOM_WORKER_FIXTURES_ROOT"] == ("/app/tests/fixtures/tasks")
    assert worker["environment"]["LOOM_WORKER_TRIAL_CANCEL_POLL_INTERVAL_SEC"] == "0.1"
    assert worker["environment"]["LOOM_WORKER_SETUP_HEALTH_GUARD_ENABLED"] == "false"
    assert "../tests/fixtures/tasks:/app/tests/fixtures/tasks:ro" in worker["volumes"]

    hello_dockerfile = (
        docker_compose.REPO_ROOT / "tests/fixtures/tasks/hello-world/environment/Dockerfile"
    ).read_text(encoding="utf-8")
    assert "pytest-jsonreport" not in hello_dockerfile
    assert "pytest-json-report" in hello_dockerfile

    hello_task = tomllib.loads(
        (docker_compose.REPO_ROOT / "tests/fixtures/tasks/hello-world/task.toml").read_text(
            encoding="utf-8"
        ),
    )
    assert hello_task["environment"]["cpu_arch"] == "any"
    assert services["registry"]["ports"] == ["127.0.0.1:${LOOM_SYSTEM_REGISTRY_PORT:-55000}:5000"]
    assert builder["command"] == ["python", "-m", "loom_worker.task_image_builder"]
    assert builder["environment"]["LOOM_WORKER_TOKEN"] == (
        "${LOOM_TASK_IMAGE_BUILDER_TOKEN:?LOOM_TASK_IMAGE_BUILDER_TOKEN "
        "must be set before builder compose-up}"
    )
    assert builder["environment"]["LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO"] == (
        "${LOOM_SYSTEM_REGISTRY_REPO:-localhost:55000/loom-task-images}"
    )
    assert builder["environment"]["LOOM_WORKER_TASK_IMAGE_BUILDER_IDLE_EXIT_SECONDS"] == "120"
    assert (
        builder["environment"]["LOOM_WORKER_TASK_IMAGE_STORAGE_PROBE_PATH"]
        == "/run/loom/docker-storage-probe"
    )
    assert builder["profiles"] == ["task-image-builder"]
    assert "/var/run/docker.sock:/var/run/docker.sock" in builder["volumes"]
    assert (
        "task_image_builder_storage_probe:/run/loom/docker-storage-probe:ro" in builder["volumes"]
    )
    assert "task_image_builder_storage_probe" in compose["volumes"]


def test_worker_image_installs_direct_benchmark_runtime_dependency() -> None:
    worker_dockerfile = (docker_compose.REPO_ROOT / "deploy/Dockerfile.worker").read_text(
        encoding="utf-8"
    )

    assert "pip install --no-cache-dir -e ./packages/loom-benchmarks" in (worker_dockerfile)


def test_stack_up_migrates_blank_database_before_starting_services(
    monkeypatch: Any,
) -> None:
    events: list[tuple[Any, ...]] = []
    compose_envs: list[tuple[tuple[str, ...], dict[str, str] | None]] = []
    resolved_ports: list[str] = []
    allocated_database = "postgresql+psycopg://loom:loom@127.0.0.1:61234/loom"

    def fake_compose(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[0] == "port":
            resolved_ports.append(args[1])
            port = {"registry": 61235, "control-plane": 61236,
                    "llm-gateway": 61237, "minio": 61238}[args[1]]
            return subprocess.CompletedProcess(args, 0, stdout=f"127.0.0.1:{port}\n", stderr="")
        events.append(("compose", *args))
        compose_envs.append((args, kwargs.get("env_extra")))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    def fake_wait(services: list[str], timeout_sec: float) -> None:
        events.append(("wait", tuple(services), timeout_sec))

    def fake_run(
        command: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        if command[1:3] == ["-m", "alembic"]:
            events.append(("migrate", tuple(command), kwargs["env"]["LOOM_DB_URL"]))
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        events.append(("seed", tuple(command)))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="team-token\nworker-token\nbuilder-token\n",
            stderr="",
        )

    def fake_wait_for_materialization(task_id: str, *, timeout_sec: float, db_url: str) -> None:
        assert db_url == allocated_database
        events.append(("materialization-ready", task_id, timeout_sec))

    def fake_canary(team_token: str, *, timeout_sec: float) -> None:
        assert docker_compose.CONTROL_PLANE_URL == "http://127.0.0.1:61236"
        assert resolved_ports == ["registry", "control-plane", "llm-gateway", "minio"]
        events.append(("claim-canary", team_token, timeout_sec))

    for setting in ("CONTROL_PLANE_URL", "GATEWAY_URL", "MINIO_URL"):
        monkeypatch.setattr(docker_compose, setting, getattr(docker_compose, setting))
    monkeypatch.setattr(docker_compose, "prepare_test_image", lambda image: "local-minio-source")
    monkeypatch.setattr(docker_compose, "database_url", lambda: allocated_database)
    monkeypatch.setattr(docker_compose, "_compose", fake_compose)
    monkeypatch.setattr(docker_compose, "_wait_services_healthy", fake_wait)
    monkeypatch.setattr(
        docker_compose,
        "_wait_task_image_materialization_ready",
        fake_wait_for_materialization,
        raising=False,
    )
    monkeypatch.setattr(docker_compose, "_verify_worker_claim_canary", fake_canary)
    monkeypatch.setattr(docker_compose.subprocess, "run", fake_run)

    assert docker_compose.stack_up(timeout_sec=17) == ("team-token", "worker-token")
    assert docker_compose.service_urls() == {
        "control_plane": "http://127.0.0.1:61236", "gateway": "http://127.0.0.1:61237",
        "minio": "http://127.0.0.1:61238",
    }

    assert events[0] == ("compose", "up", "-d", "postgres", "minio", "registry")
    assert events[1] == ("wait", ("postgres", "minio", "registry"), 17)
    assert events[2][0] == "migrate"
    assert events[2][1][0:3] == (sys.executable, "-m", "alembic")
    assert events[2][2] == allocated_database
    assert events[3] == ("compose", "up", "-d", "--build")
    assert events[4] == (
        "wait",
        ("postgres", "minio", "registry", "llm-gateway", "control-plane"),
        17,
    )
    assert events[5][0] == "seed"
    assert events[5][1][0] == sys.executable
    seed_command = events[5][1]
    assert seed_command[seed_command.index("--db-url") + 1] == allocated_database
    assert events[6] == (
        "compose",
        "--profile",
        "task-image-builder",
        "up",
        "-d",
        "--build",
        "--no-deps",
        "task-image-builder",
    )
    assert events[7] == ("materialization-ready", "hello-world", 17)
    assert events[8] == (
        "compose",
        "--profile",
        "worker",
        "up",
        "-d",
        "--build",
        "--no-deps",
        "worker",
    )
    assert events[9] == ("wait", ("worker",), 60.0)
    assert events[10] == (
        "claim-canary",
        "team-token",
        docker_compose._CANARY_MAX_TIMEOUT_SEC,
    )
    assert all(
        env and env["LOOM_SYSTEM_MINIO_IMAGE"] == "local-minio-source" for _, env in compose_envs
    )
    builder_env = next(
        env for args, env in compose_envs if args and args[-1] == "task-image-builder"
    )
    worker_env = next(env for args, env in compose_envs if args and args[-1] == "worker")
    assert builder_env == {
        "LOOM_TASK_IMAGE_BUILDER_TOKEN": "builder-token",
        "LOOM_SYSTEM_MINIO_IMAGE": "local-minio-source",
        "LOOM_SYSTEM_REGISTRY_REPO": "localhost:61235/loom-task-images",
    }
    assert worker_env == {
        "LOOM_WORKER_TOKEN": "worker-token",
        "LOOM_SYSTEM_MINIO_IMAGE": "local-minio-source",
    }


def test_stack_up_rejects_ambiguous_registry_before_seeding_or_starting_builder(monkeypatch: Any):
    events = []

    def compose(*args, **kwargs):
        events.append(args)
        return subprocess.CompletedProcess(args, 0,
            stdout="127.0.0.1:61234\n127.0.0.1:61235\n" if args[0] == "port" else "", stderr="")

    def migrate(command, **kwargs):
        assert command[1:3] == ["-m", "alembic"]
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    cleanup = []
    monkeypatch.setattr(docker_compose, "prepare_test_image", lambda _: "local-minio-source")
    monkeypatch.setattr(docker_compose, "database_url", lambda: "postgresql://fixture")
    monkeypatch.setattr(docker_compose, "_compose", compose)
    monkeypatch.setattr(docker_compose, "_wait_services_healthy", lambda *args, **kwargs: None)
    monkeypatch.setattr(docker_compose.subprocess, "run", migrate)
    monkeypatch.setattr(docker_compose, "stack_down_with_diagnostics", lambda *, failed: cleanup.append(failed))
    with pytest.raises(RuntimeError, match="registry port"):
        docker_compose.stack_up()
    assert cleanup == [True]
    assert all("task-image-builder" not in args for args in events)


def test_wait_services_healthy_bounds_compose_inspection(monkeypatch: Any) -> None:
    timeouts: list[float] = []

    def fake_compose(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        timeouts.append(kwargs["timeout_sec"])
        return subprocess.CompletedProcess(
            args,
            0,
            stdout='{"Service":"postgres","State":"running","Health":"healthy"}\n',
            stderr="",
        )

    monkeypatch.setattr(docker_compose, "prepare_test_image", lambda image: "local-minio-source")
    monkeypatch.setattr(docker_compose, "_compose", fake_compose)

    docker_compose._wait_services_healthy(["postgres"], timeout_sec=30.0)

    assert timeouts
    assert 0 < timeouts[0] <= docker_compose._COMPOSE_INSPECT_TIMEOUT_SEC


def test_wait_materialization_fails_fast_when_builder_container_exits(
    monkeypatch: Any,
) -> None:
    class _Connection:
        def __enter__(self) -> _Connection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, _statement: object) -> SimpleNamespace:
            return SimpleNamespace(
                one_or_none=lambda: SimpleNamespace(
                    state="queued",
                    failure_message=None,
                ),
            )

    class _Engine:
        def connect(self) -> _Connection:
            return _Connection()

        def dispose(self) -> None:
            return None

    clock = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(docker_compose, "create_engine", lambda _url: _Engine())
    monkeypatch.setattr(docker_compose.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(docker_compose.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        docker_compose,
        "_task_image_builder_container_running",
        lambda: False,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="builder container exited before claiming"):
        docker_compose._wait_task_image_materialization_ready(
            "hello-world",
            timeout_sec=1.0,
            db_url="postgresql+psycopg://loom:loom@127.0.0.1:61234/loom",
        )


def test_stack_up_cleans_partial_compose_state_on_setup_failure(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    compose_calls: list[tuple[str, ...]] = []
    diagnostics = tmp_path / "setup-failure.log"

    def fake_compose(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        compose_calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    def fail_wait(services: list[str], timeout_sec: float) -> None:
        raise RuntimeError(f"failed waiting for {services} after {timeout_sec}")

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert command[:3] == ["docker", "ps", "-a"]
        return subprocess.CompletedProcess(
            command, 0, stdout="fixture-postgres-1\tfixture-image\tUp 1 second\n", stderr="",
        )

    monkeypatch.setenv(docker_compose.DIAGNOSTICS_ENV, str(diagnostics))
    monkeypatch.setattr(docker_compose, "prepare_test_image", lambda image: "local-minio-source")
    monkeypatch.setattr(docker_compose, "_compose", fake_compose)
    monkeypatch.setattr(docker_compose, "_wait_services_healthy", fail_wait)
    monkeypatch.setattr(docker_compose.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="failed waiting"):
        docker_compose.stack_up(timeout_sec=17)

    assert compose_calls[-1] == (
        "--profile",
        "worker",
        "--profile",
        "task-image-builder",
        "down",
        "-v",
        "--remove-orphans",
    )
    assert compose_calls[-3:-1] == [
        (
            "--profile",
            "worker",
            "--profile",
            "task-image-builder",
            "ps",
            "-a",
        ),
        (
            "--profile",
            "worker",
            "--profile",
            "task-image-builder",
            "logs",
            "--no-color",
            "--tail=300",
        ),
    ]
    assert "fixture-postgres-1" in diagnostics.read_text(encoding="utf-8")


def _worker_compose_result(state: str = "running") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["docker", "compose"],
        0,
        stdout=f'{{"Service":"worker","State":"{state}"}}\n',
        stderr="",
    )


def test_worker_claim_canary_claims_cancels_and_waits_for_terminal(
    monkeypatch: Any,
) -> None:
    calls: list[tuple[str, str, dict[str, Any]]] = []
    responses = iter(
        [
            httpx.Response(201, json={"trial_id": "trial-canary"}),
            httpx.Response(
                200,
                json={"state": "queued", "attempt_count": 0, "claimed_at": None},
            ),
            httpx.Response(
                200,
                json={
                    "state": "claimed",
                    "attempt_count": 1,
                    "claimed_at": "2026-07-22T00:00:00Z",
                },
            ),
            httpx.Response(200, json={"state": "cancelled"}),
            httpx.Response(200, json={"state": "cancelled", "attempt_count": 1}),
        ],
    )

    def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        calls.append((method, url, kwargs))
        return next(responses)

    monkeypatch.setattr(docker_compose, "_compose", lambda *a, **k: _worker_compose_result())
    monkeypatch.setattr(docker_compose.httpx, "request", fake_request)
    monkeypatch.setattr(docker_compose.time, "sleep", lambda _: None)

    docker_compose._verify_worker_claim_canary(
        "loom_team_do-not-leak",
        timeout_sec=2.0,
    )

    assert [(method, url.rsplit("/", 1)[-1]) for method, url, _ in calls] == [
        ("POST", "trials"),
        ("GET", "trial-canary"),
        ("GET", "trial-canary"),
        ("POST", "cancel"),
        ("GET", "trial-canary"),
    ]
    assert calls[0][2]["json"]["task_id"] == "hello-world"
    assert all(
        call[2]["headers"]["Authorization"] == "Bearer loom_team_do-not-leak" for call in calls
    )


def test_worker_claim_canary_times_out_and_best_effort_cancels(
    monkeypatch: Any,
) -> None:
    now = 0.0
    calls: list[tuple[str, str]] = []

    def monotonic() -> float:
        return now

    def sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        calls.append((method, url))
        if method == "POST" and url.endswith("/trials"):
            return httpx.Response(201, json={"trial_id": "trial-timeout"})
        if method == "POST" and url.endswith("/cancel"):
            return httpx.Response(200, json={"state": "cancelled"})
        return httpx.Response(
            200,
            json={"state": "queued", "attempt_count": 0, "claimed_at": None},
        )

    monkeypatch.setattr(docker_compose, "_compose", lambda *a, **k: _worker_compose_result())
    monkeypatch.setattr(docker_compose.httpx, "request", fake_request)
    monkeypatch.setattr(docker_compose.time, "monotonic", monotonic)
    monkeypatch.setattr(docker_compose.time, "sleep", sleep)

    with pytest.raises(RuntimeError, match="did not claim"):
        docker_compose._verify_worker_claim_canary("loom_team_timeout", timeout_sec=1.0)

    assert calls[-1] == (
        "POST",
        f"{docker_compose.CONTROL_PLANE_URL}/trials/trial-timeout/cancel",
    )
    assert now <= 1.0


def test_worker_claim_canary_fails_fast_when_worker_exits(monkeypatch: Any) -> None:
    calls: list[tuple[str, str]] = []

    def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        calls.append((method, url))
        if url.endswith("/trials"):
            return httpx.Response(201, json={"trial_id": "trial-exit"})
        return httpx.Response(200, json={"state": "cancelled"})

    monkeypatch.setattr(
        docker_compose,
        "_compose",
        lambda *a, **k: _worker_compose_result("exited"),
    )
    monkeypatch.setattr(docker_compose.httpx, "request", fake_request)

    with pytest.raises(RuntimeError, match="worker container exited"):
        docker_compose._verify_worker_claim_canary("loom_team_exit", timeout_sec=1.0)

    assert calls[-1] == (
        "POST",
        f"{docker_compose.CONTROL_PLANE_URL}/trials/trial-exit/cancel",
    )


def test_worker_claim_canary_tolerates_bounded_inspection_timeout(
    monkeypatch: Any,
) -> None:
    timeout = subprocess.TimeoutExpired(["docker", "compose", "ps"], 0.1)
    monkeypatch.setattr(
        docker_compose,
        "_compose",
        lambda *args, **kwargs: (_ for _ in ()).throw(timeout),
    )

    assert (
        docker_compose._worker_container_running(
            deadline=docker_compose.time.monotonic() + 1.0,
        )
        is None
    )
    docker_compose._require_worker_running(
        deadline=docker_compose.time.monotonic() + 1.0,
    )


def test_worker_claim_canary_http_error_does_not_leak_team_token(
    monkeypatch: Any,
) -> None:
    secret = "loom_team_highly-sensitive"

    def fail_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        raise httpx.ReadError(f"Authorization: Bearer {secret}")

    monkeypatch.setattr(docker_compose.httpx, "request", fail_request)

    with pytest.raises(RuntimeError) as exc_info:
        docker_compose._verify_worker_claim_canary(secret, timeout_sec=1.0)

    assert secret not in str(exc_info.value)
    assert "Bearer" not in str(exc_info.value)


def test_worker_claim_canary_retries_transient_initial_connect_error(
    monkeypatch: Any,
) -> None:
    attempts = 0

    def request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("control plane port is still warming")
        return httpx.Response(201, json={"trial_id": "trial-canary"})

    monkeypatch.setattr(docker_compose.httpx, "request", request)
    monkeypatch.setattr(docker_compose.time, "sleep", lambda _: None)

    response = docker_compose._canary_request(
        "POST",
        f"{docker_compose.CONTROL_PLANE_URL}/trials",
        deadline=docker_compose.time.monotonic() + 1.0,
        headers={"Authorization": "Bearer loom_team_not-logged"},
        json_body={"task_id": "hello-world"},
        retry_connect_errors=True,
    )

    assert response.status_code == 201
    assert attempts == 2


def test_failed_teardown_redacts_diagnostics_before_down(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "deploy")
    calls: list[tuple[str, ...]] = []
    timeouts: list[float] = []
    diagnostics = tmp_path / "system-smoke.log"
    raw_secrets = (
        "loom_w_worker-secret",
        "loom_team_team-secret",
        "Bearer arbitrary-secret",
        "https://example.com/object?X-Amz-Signature=signed-secret",
    )

    def fake_compose(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        timeouts.append(kwargs["timeout_sec"])
        return subprocess.CompletedProcess(
            args,
            0,
            stdout="\n".join(raw_secrets) + "\n",
            stderr="",
        )

    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args[0],
            0,
            stdout="deploy-worker-1\tloom-worker:dev\tUp 10 seconds\n",
            stderr="",
        )

    monkeypatch.setattr(docker_compose, "prepare_test_image", lambda image: "local-minio-source")
    monkeypatch.setattr(docker_compose, "_compose", fake_compose)
    monkeypatch.setattr(docker_compose.subprocess, "run", fake_run)
    monkeypatch.setenv(docker_compose.DIAGNOSTICS_ENV, str(diagnostics))

    docker_compose.stack_down_with_diagnostics(failed=True)

    assert calls == [
        (
            "--profile",
            "worker",
            "--profile",
            "task-image-builder",
            "ps",
            "-a",
        ),
        (
            "--profile",
            "worker",
            "--profile",
            "task-image-builder",
            "logs",
            "--no-color",
            "--tail=300",
        ),
        (
            "--profile",
            "worker",
            "--profile",
            "task-image-builder",
            "down",
            "-v",
            "--remove-orphans",
        ),
    ]
    assert timeouts == [
        docker_compose._COMPOSE_DIAGNOSTICS_TIMEOUT_SEC,
        docker_compose._COMPOSE_DIAGNOSTICS_TIMEOUT_SEC,
        docker_compose._COMPOSE_TEARDOWN_TIMEOUT_SEC,
    ]
    payload = diagnostics.read_text(encoding="utf-8")
    assert "===== compose ps -a =====" in payload
    assert "===== compose logs --tail=300 =====" in payload
    assert "===== docker ps for project deploy =====" in payload
    assert "deploy-worker-1" in payload
    assert all(secret not in payload for secret in raw_secrets)
    assert "[REDACTED:loom-token]" in payload
    assert "[REDACTED:bearer]" in payload
    assert "[REDACTED:signed-url]" in payload


def test_preserve_diagnostics_keeps_first_failure_evidence(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    diagnostics = tmp_path / "system-smoke.log"
    diagnostics.write_text("first failure evidence\n", encoding="utf-8")
    monkeypatch.setenv(docker_compose.DIAGNOSTICS_ENV, str(diagnostics))
    monkeypatch.setattr(
        docker_compose,
        "_compose",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args,
            0,
            stdout="later mocked output\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(
        docker_compose.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0],
            0,
            stdout="later docker output\n",
            stderr="",
        ),
    )

    docker_compose.preserve_compose_diagnostics()

    assert diagnostics.read_text(encoding="utf-8") == "first failure evidence\n"


def test_diagnostics_failure_does_not_mask_teardown(monkeypatch: Any) -> None:
    calls: list[tuple[str, ...]] = []

    def fail_diagnostics() -> None:
        raise OSError("diagnostics destination unavailable")

    def fake_compose(*args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(docker_compose, "preserve_compose_diagnostics", fail_diagnostics)
    monkeypatch.setattr(docker_compose, "prepare_test_image", lambda image: "local-minio-source")
    monkeypatch.setattr(docker_compose, "_compose", fake_compose)

    docker_compose.stack_down_with_diagnostics(failed=True)

    assert calls == [
        (
            "--profile",
            "worker",
            "--profile",
            "task-image-builder",
            "down",
            "-v",
            "--remove-orphans",
        ),
    ]


def test_workflow_prints_runner_temp_diagnostics_before_cleanup() -> None:
    workflow = (docker_compose.REPO_ROOT / ".github/workflows/staging-smoke.yml").read_text(
        encoding="utf-8"
    )
    diagnostics = "${{ runner.temp }}/system-smoke-compose.log"

    assert workflow.count(f"LOOM_SYSTEM_SMOKE_DIAGNOSTICS: {diagnostics}") == 2
    capture_index = workflow.index("preserve_compose_diagnostics()")
    cat_index = workflow.index('cat "${LOOM_SYSTEM_SMOKE_DIAGNOSTICS}"')
    cleanup_index = workflow.index("- name: Cleanup system-smoke compose stack")
    assert capture_index < cat_index < cleanup_index
    assert "timeout 60s docker compose" in workflow


def test_system_stack_uses_shared_digest_pinned_minio() -> None:
    from tests.integration.minio_test_images import MINIO_TEST_IMAGE

    config = yaml.safe_load(docker_compose.COMPOSE_FILE.read_text())
    assert (
        config["services"]["minio"]["image"]
        == "${LOOM_SYSTEM_MINIO_IMAGE:-" + MINIO_TEST_IMAGE + "}"
    )
