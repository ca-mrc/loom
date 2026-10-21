"""Helpers for spinning the deploy/docker-compose.test.yml stack up + down
around a single pytest session.

System tests are explicitly OUT of the default `pytest tests/` collection
(see pyproject.toml: `addopts = "--ignore=tests/system"`). Run them with
`pytest tests/system -v` from a host with Docker + docker-compose v2.

Compose-up has FIVE ordered phases because services require an Alembic-head
database and the execution services need distinct scoped tokens:

  1. Bring up Postgres + MinIO + the local registry, then apply migrations.
  2. Bring up control-plane + gateway (worker excluded by its profile).
  3. Run seed_test_data.py against the migrated Postgres to mint a
     team token + worker token + task-image builder token and seed the fixture.
  4. Start the dedicated task-image builder and wait for the native fixture
     materialization to become ready.
  5. Start the trial worker with its ordinary worker token.

stack_up() returns both raw tokens so tests can use the team token for
HTTP calls; the worker token lives inside the worker container env.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import create_engine, select

from loom.db.schema import TaskImageMaterialization
from loom.security.redaction import redact_text
from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.support.minio_images import prepare_test_image

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "deploy" / "docker-compose.test.yml"
ALEMBIC_CONFIG = REPO_ROOT / "database" / "migrations" / "alembic.ini"
CONTROL_PLANE_URL = "http://localhost:58080"
GATEWAY_URL = "http://localhost:59100"
MINIO_URL = "http://localhost:59000"
DIAGNOSTICS_ENV = "LOOM_SYSTEM_SMOKE_DIAGNOSTICS"
_DEFAULT_PROJECT_NAME = f"loom-system-{uuid4().hex[:12]}"
_CANARY_MAX_TIMEOUT_SEC = 30.0
_CANARY_POLL_INTERVAL_SEC = 0.25
_COMPOSE_INSPECT_TIMEOUT_SEC = 5.0
_COMPOSE_DIAGNOSTICS_TIMEOUT_SEC = 15.0
_COMPOSE_TEARDOWN_TIMEOUT_SEC = 60.0
_TERMINAL_TRIAL_STATES = {"succeeded", "failed", "cancelled"}
_RUNTIME_PROFILE_ARGS = (
    "--profile",
    "worker",
    "--profile",
    "task-image-builder",
)
_CONTROL_PLANE_LIVENESS_PROBE = """\
import json, time, urllib.request
started = time.monotonic()
try:
    with urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2) as response:
        outcome = str(response.status)
except Exception as error:
    outcome = type(error).__name__
print(json.dumps({'outcome': outcome, 'elapsed_seconds': round(time.monotonic() - started, 3)}))
"""
_POSTGRES_ACTIVITY_PROBE = """\
SET default_transaction_read_only = on;
SET statement_timeout = '2s';
SELECT pid, state, wait_event_type, wait_event,
       round(extract(epoch FROM clock_timestamp() - query_start), 3) AS query_age_seconds,
       round(extract(epoch FROM clock_timestamp() - xact_start), 3) AS transaction_age_seconds,
       pg_blocking_pids(pid) AS blockers
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
ORDER BY pid LIMIT 64;
"""


def _compose_project_name() -> str:
    return os.environ.get("COMPOSE_PROJECT_NAME") or _DEFAULT_PROJECT_NAME


def _compose(
    *args: str,
    check: bool = True,
    env_extra: dict[str, str] | None = None,
    timeout_sec: float | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), *args]
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = _compose_project_name()
    # Stub LOOM_WORKER_TOKEN so compose can parse the file even when the
    # worker service is not in the active profile. The :? required-form
    # in compose only triggers when the service is selected.
    env.setdefault("LOOM_WORKER_TOKEN", "unused-no-profile")
    env.setdefault("LOOM_TASK_IMAGE_BUILDER_TOKEN", "unused-no-profile")
    # Docker reserves each loopback port atomically. Keep these mappings at
    # zero across up/build/restart; discovered endpoints have separate values.
    for service in ("POSTGRES", "MINIO", "REGISTRY", "GATEWAY", "CONTROL_PLANE"):
        env.setdefault(f"LOOM_SYSTEM_{service}_PORT", "0")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run(
            cmd,
            check=check,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_sec,
        )
    except subprocess.CalledProcessError as exc:
        # Pytest truncates exception-local stdout/stderr, hiding build failures
        # before services exist. Redact before truncating so a cut token prefix
        # cannot leave its secret suffix in the diagnostic tail.
        output = _redact_diagnostics(f"{exc.stdout or ''}\n{exc.stderr or ''}")
        try:
            print(
                f"Compose command failed (exit {exc.returncode}); output tail:\n"
                f"{output[-32_768:]}",
                file=sys.stderr,
            )
        except OSError:
            pass  # A failed diagnostic must not replace the original failure.
        raise


def _published_port(service: str, container_port: str) -> int:
    result = _compose("port", service, container_port, timeout_sec=_COMPOSE_INSPECT_TIMEOUT_SEC)
    binding = re.fullmatch(r"127\.0\.0\.1:([0-9]+)", result.stdout.strip())
    if binding is None or not 1 <= int(binding[1]) <= 65535:
        label = "database" if service == "postgres" else service
        raise RuntimeError(f"system-smoke {label} port must have one valid loopback binding")
    return int(binding[1])


def database_url() -> str:
    """Resolve this stack's loopback database binding for host-side clients."""
    return f"postgresql+psycopg://loom:loom@127.0.0.1:{_published_port('postgres', '5432')}/loom"


def service_urls() -> dict[str, str]:
    """Endpoints resolved once at startup; canary polling never shells out."""
    return {"control_plane": CONTROL_PLANE_URL, "gateway": GATEWAY_URL, "minio": MINIO_URL}


def _wait_services_healthy(
    services: list[str],
    timeout_sec: float,
) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            ps = _compose(
                "ps",
                "--format",
                "json",
                timeout_sec=max(
                    0.001,
                    min(_COMPOSE_INSPECT_TIMEOUT_SEC, remaining),
                ),
            )
        except subprocess.TimeoutExpired:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
            continue
        statuses = [json.loads(line) for line in ps.stdout.splitlines() if line.strip()]
        observed = {s.get("Service"): s for s in statuses}
        healthy = all(
            observed.get(name, {}).get("Health") in (None, "", "healthy")
            and observed.get(name, {}).get("State") == "running"
            for name in services
        )
        if healthy and all(name in observed for name in services):
            return
        time.sleep(1.0)
    raise RuntimeError(
        f"services {services} did not become healthy within {timeout_sec}s; "
        f"inspect with `docker compose -f {COMPOSE_FILE} ps`.",
    )


def stack_up(
    task_id: str = "hello-world",
    timeout_sec: float = 300.0,
) -> tuple[str, str]:
    """Bootstrap the system stack and clean partial state on any failure."""
    try:
        return _stack_up(task_id=task_id, timeout_sec=timeout_sec)
    except BaseException:
        stack_down_with_diagnostics(failed=True)
        raise


def _stack_up(
    task_id: str,
    timeout_sec: float,
) -> tuple[str, str]:
    """Ordered system-stack bootstrap. Returns (team_token, worker_token)."""
    global CONTROL_PLANE_URL, GATEWAY_URL, MINIO_URL
    minio_env = {"LOOM_SYSTEM_MINIO_IMAGE": prepare_test_image(MINIO_TEST_IMAGE)}
    # Stage 1: start stateful dependencies, then apply migrations explicitly.
    # Long-running services validate schema-at-head and intentionally never
    # auto-migrate, so they must not be started against a blank database.
    _compose("up", "-d", "postgres", "minio", "registry", env_extra=minio_env)
    _wait_services_healthy(["postgres", "minio", "registry"], timeout_sec=timeout_sec)
    db_url = database_url()
    migration_env = os.environ.copy()
    migration_env["LOOM_DB_URL"] = db_url
    subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(ALEMBIC_CONFIG),
            "upgrade",
            "head",
        ],
        check=True,
        cwd=REPO_ROOT,
        env=migration_env,
    )

    # Stage 2: start application services. Worker remains excluded by profile.
    _compose("up", "-d", "--build", env_extra=minio_env)
    _wait_services_healthy(
        ["postgres", "minio", "registry", "llm-gateway", "control-plane"],
        timeout_sec=timeout_sec,
    )
    # Resolve only after the full default stack is healthy. Keep the Docker
    # mappings unchanged; only the builder's repository uses the actual port.
    registry_repo = f"localhost:{_published_port('registry', '5000')}/loom-task-images"
    urls = {
        "control_plane": f"http://127.0.0.1:{_published_port('control-plane', '8080')}",
        "gateway": f"http://127.0.0.1:{_published_port('llm-gateway', '9100')}",
        "minio": f"http://127.0.0.1:{_published_port('minio', '9000')}",
    }
    CONTROL_PLANE_URL, GATEWAY_URL, MINIO_URL = (
        urls["control_plane"], urls["gateway"], urls["minio"],
    )

    # Stage 3: mint a team + worker token (and seed the fixture).
    seed = subprocess.run(
        [
            sys.executable,
            "scripts/seed_test_data.py",
            "--task-id",
            task_id,
            "--db-url",
            db_url,
            "--print",
            "system",
        ],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    team_token, worker_token, builder_token = seed.stdout.strip().splitlines()[:3]

    # Stage 4: build and publish the native task image before a trial worker is
    # allowed to claim the Dockerfile-backed canary.
    _compose(
        "--profile",
        "task-image-builder",
        "up",
        "-d",
        "--build",
        "--no-deps",
        "task-image-builder",
        env_extra={**minio_env, "LOOM_TASK_IMAGE_BUILDER_TOKEN": builder_token,
                   "LOOM_SYSTEM_REGISTRY_REPO": registry_repo},
    )
    _wait_task_image_materialization_ready(task_id, timeout_sec=timeout_sec, db_url=db_url)

    # Stage 5: start the worker with the ordinary trial token wired in.
    _compose(
        "--profile",
        "worker",
        "up",
        "-d",
        "--build",
        "--no-deps",
        "worker",
        env_extra={**minio_env, "LOOM_WORKER_TOKEN": worker_token},
    )
    _wait_services_healthy(["worker"], timeout_sec=60.0)
    _verify_worker_claim_canary(team_token, timeout_sec=_CANARY_MAX_TIMEOUT_SEC)

    return team_token, worker_token


def _wait_task_image_materialization_ready(
    task_id: str,
    *,
    timeout_sec: float,
    db_url: str,
) -> None:
    # Local compose publishes the supported execution architecture (x86_64),
    # including on Apple Silicon hosts that cross-build via Docker Desktop.
    cpu_arch = "x86_64"

    engine = create_engine(db_url)
    deadline = time.monotonic() + timeout_sec
    next_builder_probe = 0.0
    try:
        while (observed_at := time.monotonic()) < deadline:
            with engine.connect() as connection:
                row = connection.execute(
                    select(
                        TaskImageMaterialization.state,
                        TaskImageMaterialization.failure_message,
                    ).where(
                        TaskImageMaterialization.task_id == task_id,
                        TaskImageMaterialization.cpu_arch == cpu_arch,
                    )
                ).one_or_none()
            if row is not None and row.state == "ready":
                return
            if row is not None and row.state == "failed":
                raise RuntimeError(
                    "system-smoke task image materialization failed: "
                    f"{row.failure_message or 'no diagnostic'}"
                )
            if observed_at >= next_builder_probe:
                builder_running = _task_image_builder_container_running()
                if builder_running is False:
                    raise RuntimeError(
                        "system-smoke task image builder container exited before "
                        "claiming the materialization"
                    )
                next_builder_probe = observed_at + 1.0
            time.sleep(0.25)
    finally:
        engine.dispose()
    raise RuntimeError(
        f"system-smoke task image materialization did not become ready within {timeout_sec}s"
    )


def _task_image_builder_container_running() -> bool | None:
    try:
        result = _compose(
            "--profile",
            "task-image-builder",
            "ps",
            "-a",
            "--format",
            "json",
            "task-image-builder",
            check=False,
            timeout_sec=_COMPOSE_INSPECT_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    try:
        decoded = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if isinstance(decoded, dict):
        statuses = [decoded]
    elif isinstance(decoded, list):
        statuses = [row for row in decoded if isinstance(row, dict)]
    else:
        return None
    builder = next(
        (row for row in statuses if row.get("Service") == "task-image-builder"),
        None,
    )
    if builder is None:
        return None
    return str(builder.get("State", "")).lower() == "running"


def stack_down() -> None:
    try:
        _compose(
            *_RUNTIME_PROFILE_ARGS,
            "down",
            "-v",
            "--remove-orphans",
            "--rmi",
            "local",
            check=False,
            timeout_sec=_COMPOSE_TEARDOWN_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        print(
            "warning: Compose teardown exceeded its bounded timeout",
            file=sys.stderr,
        )


def _response_json(response: httpx.Response, *, operation: str) -> dict[str, object]:
    try:
        payload = response.json()
    except ValueError:
        message = f"worker claim canary {operation} returned invalid JSON"
        raise RuntimeError(message) from None
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"worker claim canary {operation} returned a non-object payload",
        )
    return payload


def _canary_request(
    method: str,
    url: str,
    *,
    deadline: float,
    headers: dict[str, str],
    json_body: dict[str, object] | None = None,
    retry_connect_errors: bool = False,
) -> httpx.Response:
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("worker claim canary exceeded its 30 second deadline")
        try:
            return httpx.request(
                method,
                url,
                headers=headers,
                json=json_body,
                timeout=max(0.001, min(5.0, remaining)),
            )
        except (httpx.ConnectError, httpx.ConnectTimeout):
            if not retry_connect_errors or time.monotonic() >= deadline:
                # Keep request headers (and therefore the team token) out of failures.
                raise RuntimeError(
                    "worker claim canary local API request failed",
                ) from None
            _sleep_for_canary_poll(deadline)
        except httpx.HTTPError:
            # Keep request headers (and therefore the team token) out of failures.
            raise RuntimeError("worker claim canary local API request failed") from None


def _worker_container_running(*, deadline: float) -> bool | None:
    """Return the worker's confirmed running state, or None if uninspectable."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    try:
        result = _compose(
            "--profile",
            "worker",
            "ps",
            "-a",
            "--format",
            "json",
            "worker",
            check=False,
            timeout_sec=max(0.001, min(5.0, remaining)),
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    statuses: list[dict[str, object]] = []
    try:
        decoded = json.loads(result.stdout)
        if isinstance(decoded, dict):
            statuses = [decoded]
        elif isinstance(decoded, list):
            statuses = [row for row in decoded if isinstance(row, dict)]
    except json.JSONDecodeError:
        try:
            statuses = [
                row
                for line in result.stdout.splitlines()
                if line.strip() and isinstance((row := json.loads(line)), dict)
            ]
        except json.JSONDecodeError:
            return None
    worker = next(
        (row for row in statuses if row.get("Service") == "worker"),
        None,
    )
    if worker is None:
        return None
    return str(worker.get("State", "")).lower() == "running"


def _require_worker_running(*, deadline: float) -> None:
    running = _worker_container_running(deadline=deadline)
    if running is False:
        raise RuntimeError(
            "worker container exited during the hello-world claim canary",
        )


def _sleep_for_canary_poll(deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(min(_CANARY_POLL_INTERVAL_SEC, remaining))


def _verify_worker_claim_canary(
    team_token: str,
    *,
    timeout_sec: float = _CANARY_MAX_TIMEOUT_SEC,
) -> None:
    """Prove the healthy worker can claim a local hello-world trial.

    The entire probe is capped at 30 seconds. It observes the worker container
    while polling, cancels the claimed trial, and waits for a terminal state so
    the session starts without a canary trial still consuming worker capacity.
    """

    total_timeout = min(max(timeout_sec, 0.1), _CANARY_MAX_TIMEOUT_SEC)
    started_at = time.monotonic()
    deadline = started_at + total_timeout
    # Reserve up to five seconds (or 20% for very small unit-test deadlines)
    # for cancellation and its terminal-state observation.
    cancel_budget = min(5.0, total_timeout * 0.2)
    claim_deadline = deadline - cancel_budget
    headers = {"Authorization": f"Bearer {team_token}"}
    trial_id: str | None = None
    cancel_attempted = False

    try:
        response = _canary_request(
            "POST",
            f"{CONTROL_PLANE_URL}/trials",
            deadline=claim_deadline,
            headers=headers,
            json_body={
                "task_id": "hello-world",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            retry_connect_errors=True,
        )
        if response.status_code != 201:
            raise RuntimeError(
                f"worker claim canary trial submission failed with HTTP {response.status_code}",
            )
        submitted = _response_json(response, operation="submission")
        raw_trial_id = submitted.get("trial_id")
        if not isinstance(raw_trial_id, str) or not raw_trial_id:
            raise RuntimeError("worker claim canary submission omitted trial_id")
        trial_id = raw_trial_id

        while time.monotonic() < claim_deadline:
            _require_worker_running(deadline=claim_deadline)
            response = _canary_request(
                "GET",
                f"{CONTROL_PLANE_URL}/trials/{trial_id}",
                deadline=claim_deadline,
                headers=headers,
            )
            if response.status_code != 200:
                raise RuntimeError(
                    f"worker claim canary status read failed with HTTP {response.status_code}",
                )
            status = _response_json(response, operation="status read")
            attempt_count = status.get("attempt_count")
            claimed = (
                isinstance(attempt_count, int)
                and not isinstance(attempt_count, bool)
                and attempt_count > 0
            ) or bool(status.get("claimed_at"))
            if claimed:
                break
            state = status.get("state")
            if state in _TERMINAL_TRIAL_STATES:
                raise RuntimeError(
                    "hello-world canary reached a terminal state without claim evidence",
                )
            _sleep_for_canary_poll(claim_deadline)
        else:
            raise RuntimeError(
                "worker did not claim the hello-world canary within the bounded deadline",
            )

        _require_worker_running(deadline=deadline)
        cancel_attempted = True
        response = _canary_request(
            "POST",
            f"{CONTROL_PLANE_URL}/trials/{trial_id}/cancel",
            deadline=deadline,
            headers=headers,
        )
        if response.status_code not in (200, 409):
            raise RuntimeError(
                f"worker claim canary cancellation failed with HTTP {response.status_code}",
            )

        while time.monotonic() < deadline:
            _require_worker_running(deadline=deadline)
            response = _canary_request(
                "GET",
                f"{CONTROL_PLANE_URL}/trials/{trial_id}",
                deadline=deadline,
                headers=headers,
            )
            if response.status_code != 200:
                raise RuntimeError(
                    "worker claim canary cancellation status read failed "
                    f"with HTTP {response.status_code}",
                )
            status = _response_json(response, operation="cancellation status read")
            if status.get("state") in _TERMINAL_TRIAL_STATES:
                return
            _sleep_for_canary_poll(deadline)
        raise RuntimeError("worker claim canary cancellation did not become terminal")
    except BaseException:
        # A submitted-but-unclaimed trial should not leak into the real suite.
        # Best effort only: preserve the primary diagnostic and the 30s cap.
        if trial_id is not None and not cancel_attempted and time.monotonic() < deadline:
            try:
                _canary_request(
                    "POST",
                    f"{CONTROL_PLANE_URL}/trials/{trial_id}/cancel",
                    deadline=deadline,
                    headers=headers,
                )
            except RuntimeError:
                pass
        raise


def _redact_diagnostics(text: str) -> str:
    return redact_text(text)


def preserve_compose_diagnostics() -> None:
    """Capture Compose state while containers still exist, with redaction."""

    destination = os.environ.get(DIAGNOSTICS_ENV)
    destination_path = Path(destination) if destination else None
    if destination_path is not None and destination_path.exists() and destination_path.stat().st_size > 0:
        return
    sections: list[str] = []
    for heading, args in (
        # These run only inside this disposable Compose project. Capture the
        # volatile waits before slower logs; never include query text or bodies.
        ("Control Plane liveness", (*_RUNTIME_PROFILE_ARGS, "exec", "-T", "control-plane",
            "python", "-c", _CONTROL_PLANE_LIVENESS_PROBE)),
        ("PostgreSQL waits (no query text)", (*_RUNTIME_PROFILE_ARGS, "exec", "-T", "postgres",
            "psql", "-X", "-U", "loom", "-d", "loom", "-v", "ON_ERROR_STOP=1", "-c", _POSTGRES_ACTIVITY_PROBE)),
        ("compose stats --no-stream", (*_RUNTIME_PROFILE_ARGS, "stats", "--no-stream", "--format", "json")),
        ("compose ps -a", (*_RUNTIME_PROFILE_ARGS, "ps", "-a")),
        (
            "compose logs --tail=300",
            (*_RUNTIME_PROFILE_ARGS, "logs", "--no-color", "--timestamps", "--tail=300"),
        ),
    ):
        try:
            result = _compose(
                *args,
                check=False,
                timeout_sec=_COMPOSE_DIAGNOSTICS_TIMEOUT_SEC,
            )
            content = f"{result.stdout}{result.stderr}".rstrip() or (
                f"<no output; exit code {result.returncode}>"
            )
        except subprocess.TimeoutExpired:
            content = "capture exceeded its bounded timeout"
        sections.append(f"===== {heading} =====\n{content}".rstrip())
    project_name = _compose_project_name()
    try:
        raw_ps = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={project_name}",
                "--format",
                "{{.Names}}\t{{.Image}}\t{{.Status}}",
            ],
            check=False,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=_COMPOSE_DIAGNOSTICS_TIMEOUT_SEC,
        )
        raw_content = f"{raw_ps.stdout}{raw_ps.stderr}".rstrip() or (
            f"<no output; exit code {raw_ps.returncode}>"
        )
    except subprocess.TimeoutExpired:
        raw_content = "capture exceeded its bounded timeout"
    sections.append(f"===== docker ps for project {project_name} =====\n{raw_content}")
    payload = _redact_diagnostics("\n\n".join(sections)) + "\n"
    if destination_path is not None:
        destination_path.write_text(payload, encoding="utf-8")
    else:
        print(payload, file=sys.stderr, end="")


def stack_down_with_diagnostics(*, failed: bool) -> None:
    """Preserve failure evidence before the destructive Compose teardown."""

    try:
        if failed:
            try:
                preserve_compose_diagnostics()
            except Exception as exc:  # diagnostics must not mask the test failure
                print(
                    f"warning: failed to preserve Compose diagnostics: {redact_text(str(exc))}",
                    file=sys.stderr,
                )
    finally:
        stack_down()


def kill_service(name: str) -> None:
    _compose("kill", name, check=False)


def start_service(name: str, worker_token: str | None = None) -> None:
    """Restart a service. For the worker, pass the original worker_token
    so the compose env-var substitution doesn't fail on restart."""
    env_extra = {"LOOM_WORKER_TOKEN": worker_token} if worker_token else None
    if name == "worker":
        _compose(
            "--profile",
            "worker",
            "start",
            name,
            check=False,
            env_extra=env_extra,
        )
    else:
        _compose("start", name, check=False, env_extra=env_extra)


def run_seed(task_id: str = "hello-world", which: str = "team") -> str:
    """Run scripts/seed_test_data.py against the running stack.

    Used by tests that need to seed an ADDITIONAL fixture after the
    session-scoped stack_up() (which seeds one). Returns the chosen
    token (stripped). `which` is forwarded to --print.
    """
    out = subprocess.run(
        [
            sys.executable,
            "scripts/seed_test_data.py",
            "--task-id",
            task_id,
            "--db-url",
            database_url(),
            "--print",
            which,
        ],
        check=True,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()
