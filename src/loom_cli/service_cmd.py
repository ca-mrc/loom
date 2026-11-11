"""`loom service` — one-liner wrapper around docker-compose + migrations
+ test-data seeding for the dev stack.

Replaces the manual sequence:

    docker compose --env-file .env -f deploy/docker-compose.dev.yml up -d
    alembic -c database/migrations/alembic.ini upgrade head
    python scripts/seed_test_data.py --db-url 'postgresql+psycopg://loom:loom@localhost:5432/loom'

with:

    loom service up  # all of the above + endpoint summary + token
    loom service down    # stop containers (preserves volumes)
    loom service status  # container state + endpoint URLs + Swagger UI link

Assumes invocation from the loom repo root (where `deploy/` lives).
Use `--compose-file PATH` to override.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import httpx
import tomli_w

from loom.admin_secret import AdminSecretConfigError, AdminSecretVerifier

# Dev-stack defaults — match `deploy/docker-compose.dev.yml`. Keep in sync.
_DEFAULT_COMPOSE_FILE = Path("deploy/docker-compose.dev.yml")
_DEFAULT_ENV_FILE = Path(".env")
_DEFAULT_DB_URL = "postgresql+psycopg://loom:loom@localhost:5432/loom"
_DEFAULT_DEV_ADMIN_SECRET_FILE = Path(".loom/admin/secrets.toml")
_DEFAULT_ADMIN_SECRET_FILE = Path.home() / ".config" / "loom" / "secrets.toml"
_HEALTHCHECK_RETRIES = 30
_HEALTHCHECK_INTERVAL_SEC = 2.0
_DEFAULT_CP_URL = "http://localhost:8080"
_MUTABLE_DEV_IMAGE_RE = re.compile(r"^loom-[a-z0-9-]+:dev$")

# Endpoint map (matches compose YAML port bindings + k8s ingress).
# Order = order printed by `loom service up`. User panel goes first
# because that's the URL most users actually want to open after the
# stack is up.
_ENDPOINTS = {
    "user_panel": ("http://localhost:5173", "React SPA (Vite HMR)"),
    "loom_service": ("http://localhost:8090", "user-facing REST + Swagger UI at /docs"),
    "control_plane": ("http://localhost:8080", "worker-facing; normally internal in k8s"),
    "llm_gateway": ("http://localhost:9100", "agent-facing"),
    "minio_console": ("http://localhost:9001", "browse trajectory / artifact buckets"),
    "minio_s3": ("http://localhost:9000", "S3 API"),
    "postgres": ("postgresql://loom:loom@localhost:5432/loom", "DB"),
    "task_registry": (
        "http://localhost:55000",
        "loopback task-image registry (laptop builder only)",
    ),
}


def _compose_args(compose_file: Path, env_file: Path | None) -> list[str]:
    """Common `docker compose` argv prefix."""
    args = ["docker", "compose"]
    if env_file is not None and env_file.is_file():
        args += ["--env-file", str(env_file)]
    args += ["-f", str(compose_file)]
    return args


def _mutable_dev_images(compose_file: Path) -> tuple[str, ...]:
    """Return repo-built mutable local images in a compose file.

    Local ``:dev`` tags are intentionally mutable.  They therefore cannot be
    trusted as source-fresh merely because Docker has an image with that tag.
    ``loom service up`` sends compose files containing these images through
    ``docker compose up --build`` before any container starts.  Compose/
    BuildKit then reuses fresh layers and rebuilds stale build inputs.

    In addition to literal ``:dev`` tags, the checked-in developer Compose file
    uses the exact ``${LOOM_DEV_IMAGE_TAG:-dev}`` suffix so independently named
    sandbox projects do not overwrite each other's image tags. Other
    parameterised references remain excluded rather than being guessed at.
    """
    images: set[str] = set()
    image_line = re.compile(r"^\s+image:\s*['\"]?([^'\"\s#]+)")
    for line in compose_file.read_text(encoding="utf-8").splitlines():
        match = image_line.match(line)
        if match is None:
            continue
        image = match.group(1)
        if _MUTABLE_DEV_IMAGE_RE.fullmatch(image) or (
            image.startswith("loom-") and image.endswith(":${LOOM_DEV_IMAGE_TAG:-dev}")
        ):
            images.add(image)
    return tuple(sorted(images))


def _run(argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Subprocess wrapper that streams output to our stdout/stderr."""
    return subprocess.run(argv, check=check, text=True)


def _docker_cli_missing_message() -> str:
    return (
        "error: Docker CLI was not found on PATH.\n"
        "Install and start Docker Desktop for Mac, or install Docker CLI "
        "with the Compose plugin.\n"
        "Verify the prerequisite with `docker compose version`, then re-run "
        "the `loom service` command.\n"
    )


def _docker_compose_unavailable_message(detail: str) -> str:
    detail = detail.strip()
    message = (
        "error: Docker Compose is not available through `docker compose`.\n"
        "Install and start Docker Desktop for Mac, or install Docker CLI "
        "with the Compose plugin.\n"
        "Verify the prerequisite with `docker compose version`, then re-run "
        "the `loom service` command.\n"
    )
    if detail:
        message += f"Docker reported: {detail}\n"
    return message


def _ensure_docker_compose_available() -> int:
    """Return 0 when `docker compose` is usable, otherwise print a setup hint."""
    if shutil.which("docker") is None:
        sys.stderr.write(_docker_cli_missing_message())
        return 2
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        sys.stderr.write(_docker_cli_missing_message())
        return 2
    if result.returncode != 0:
        detail = result.stderr or result.stdout
        sys.stderr.write(_docker_compose_unavailable_message(detail))
        return 2
    return 0


def _wait_for_postgres(compose_file: Path, env_file: Path | None) -> bool:
    """Poll `docker compose ps` until postgres is healthy. Returns True
    on success, False on timeout."""
    cmd = [*_compose_args(compose_file, env_file), "ps", "--format", "json", "postgres"]
    for _ in range(_HEALTHCHECK_RETRIES):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, check=True)
            # Compose ps --format json prints one JSON object per line;
            # we only care whether "healthy" appears in the status.
            if '"Health":"healthy"' in r.stdout or '"health":"healthy"' in r.stdout:
                return True
        except subprocess.CalledProcessError:
            pass
        time.sleep(_HEALTHCHECK_INTERVAL_SEC)
    return False


def _alembic_upgrade(db_url: str) -> int:
    """Run alembic migrations against the dev postgres. Alembic reads
    `LOOM_DB_URL` per `database/migrations/env.py` convention."""
    env = os.environ.copy()
    env["LOOM_DB_URL"] = db_url
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            "database/migrations/alembic.ini",
            "upgrade",
            "head",
        ],
        env=env,
        check=False,
    ).returncode


def _benchmarks_sync_config(db_url: str) -> None:
    """Run `loom datasets sync-config` after seed.

    No-op when there's no config/benchmarks.toml (legacy). Failures
    log a WARN but don't fail `service up` — the registry layer is
    not on the critical path.
    """
    from loom.config.benchmarks import resolve_config_path

    config_path = resolve_config_path()
    if config_path is None:
        return
    fixtures_root = os.environ.get("LOOM_WORKER_FIXTURES_ROOT")
    if not fixtures_root:
        sys.stderr.write(
            f"warning: {config_path} present but "
            "LOOM_WORKER_FIXTURES_ROOT not set — skipping benchmarks "
            "sync. Set the env var to the dir holding "
            "<benchmark-id>/<task>/ bundles.\n",
        )
        return
    print(f"→ syncing {config_path}")
    env = os.environ.copy()
    env["LOOM_DB_URL"] = db_url
    rc = subprocess.run(
        [
            sys.executable,
            "-m",
            "loom_cli",
            "datasets",
            "sync-config",
            "--config",
            str(config_path),
            "--fixtures-root",
            fixtures_root,
            "--db-url",
            db_url,
        ],
        env=env,
        check=False,
    ).returncode
    if rc != 0:
        sys.stderr.write(
            "warning: `loom datasets sync-config` exited non-zero; "
            "skipping benchmark registry sync (this does not block "
            "service start).\n",
        )


def _seed_test_data(db_url: str) -> tuple[int, dict[str, str]]:
    """Run seed_test_data.py with `--mode dev --print all`. Dev mode
    registers the benchmark-source slate from HF Hub so the SPA dropdown is
    populated + submittable out of the box. Skips the hello-world Task
    + card-e2e RateCard test fixtures.

    Default register path: reads benchmark bundle manifests from
    `{HF_ORG}/loom-benchmark-*` on HF Hub (a few seconds total). Works without
    HF_TOKEN for public datasets; pass through HF_TOKEN if set for gated ones.
    HF is used only for benchmark source manifests/bundles here, not trial
    artifacts, trajectories, logs, debug outputs, Docker images, or runtime
    caches.

    Air-gapped path: set `LOOM_LOCAL_IMPORT=1` and `loom service up`
    will pass `--local-import` to seed, falling back to the slow
    upstream-fetch + MinIO-upload flow. Requires `LOOM_MINIO_*`.

    Returns (exit_code, {label: token}); the script prints each token
    on its own line as `<label>: <token>`. Register progress is streamed
    to stderr so the user sees per-benchmark status."""
    env = os.environ.copy()
    # Local-only MinIO defaults for the --local-import opt-in path. Keep these
    # in sync with deploy/docker-compose.dev.yml; production uses secrets.
    env.setdefault("LOOM_MINIO_ENDPOINT", "http://localhost:9000")
    env.setdefault("LOOM_MINIO_ACCESS_KEY", "loomdev")
    env.setdefault("LOOM_MINIO_SECRET_KEY", "loomdev123")
    # HF defaults: org pre-set so register works without explicit flag.
    env.setdefault("LOOM_HF_ORG", "PRHW")
    extra_args = []
    if os.environ.get("LOOM_LOCAL_IMPORT") in ("1", "true", "True"):
        extra_args.append("--local-import")
    r = subprocess.run(
        [
            sys.executable,
            "scripts/seed_test_data.py",
            "--db-url",
            db_url,
            "--mode",
            "dev",
            "--print",
            "all",
            *extra_args,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    # Always echo seed stderr so auto-import status (per-benchmark
    # ok/error lines) is visible even on success.
    if r.stderr:
        sys.stderr.write(r.stderr)
    if r.returncode != 0:
        return r.returncode, {}
    tokens: dict[str, str] = {}
    for line in r.stdout.strip().splitlines():
        label, _, value = line.partition(":")
        if value:
            tokens[label.strip()] = value.strip()
    return 0, tokens


def _mint_batch_runner_cp_token(
    admin_token: str,
    cp_url: str = _DEFAULT_CP_URL,
) -> str | None:
    """Mint a batch-runner CP token via `POST /admin/batch-runner-tokens`.

    Returns the raw token string on success, or None on failure (with a
    warning already printed to stderr). The returned token is written to
    .env as LOOM_SVC_BATCH_RUNNER_CP_TOKEN so loom-service picks it up
    on the next force-recreate.

    This mirrors production runbook step 6. Dev-only: called from
    `loom service up`; production operators use `loom admin tokens`
    with a port-forward.
    """
    url = f"{cp_url.rstrip('/')}/admin/batch-runner-tokens"
    try:
        resp = httpx.post(
            url,
            json={},
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(
            f"warning: could not reach CP at {url} to mint batch-runner token: {exc}\n"
            "Batches will queue but not fan out. Re-run `loom service up` "
            "once the control-plane is healthy.\n",
        )
        return None
    if resp.status_code != 201:
        sys.stderr.write(
            f"warning: CP returned {resp.status_code} minting batch-runner token: "
            f"{resp.text}\n"
            "Batches will queue but not fan out.\n",
        )
        return None
    token: str = resp.json()["token"]
    return token


def _mint_secret_store_master_key() -> str:
    """Generate a fresh 32-byte random key, base64-encoded.

    Matches the generation command documented in the SecretStoreError message:
        python -c 'import os, base64; print(base64.b64encode(os.urandom(32)).decode())'
    """
    import base64

    return base64.b64encode(os.urandom(32)).decode()


def _ensure_secret_store_master_key(env_file: Path) -> str:
    """Read LOOM_SECRET_STORE_MASTER_KEY from env_file if present; generate
    and write it if absent.

    Returns the key value (existing or freshly-generated).

    IMPORTANT: Never regenerate on a second call — rotating the master key
    would make all previously-encrypted provider-connection secrets unreadable.
    """
    key_name = "LOOM_SECRET_STORE_MASTER_KEY"

    # Check if the key already exists in .env.
    if env_file.exists():
        for raw in env_file.read_text().splitlines():
            stripped = raw.lstrip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            k, _, v = stripped.partition("=")
            if k.strip() == key_name and v.strip():
                return v.strip()

    # Key absent — generate once and append to .env.
    key = _mint_secret_store_master_key()
    lines: list[str] = []
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    lines.append(f"{key_name}={key}")
    env_file.write_text("\n".join(lines) + "\n")
    return key


def _print_summary(tokens: dict[str, str]) -> None:
    print()
    print("=" * 60)
    print("Loom dev stack is up.")
    print("=" * 60)
    print()
    print("Endpoints:")
    for name, (url, desc) in _ENDPOINTS.items():
        print(f"  {name:18} {url:48} {desc}")
    print()
    if tokens.get("team"):
        team_token = tokens["team"]
        print("Legacy dev team token (compat only; not browser login):")
        print(f"  {team_token}")
        print()
    if tokens.get("admin"):
        print(
            "Admin token (DEV-ONLY; file-backed singleton; not browser login):",
        )
        print("  raw token not printed by `loom service up`")
        print(
            "  reveal explicitly: loom service reveal-admin "
            "--secret-file .loom/admin/secrets.toml --yes",
        )
        print()
    if tokens.get("team"):
        print("Try it:")
        print("  curl http://localhost:8090/api/v1/health")
        print(f"  curl -H 'Authorization: Bearer {tokens['team']}' \\")
        print("       http://localhost:8090/api/v1/trials")
        print()
    print(
        "Dockerfile-backed local batches stay queued until the laptop "
        "task-image-builder publishes a ready x86_64 digest (linux/amd64; "
        "Apple Silicon uses Docker Desktop qemu). Trial workers remain "
        "pull-only."
    )
    print()
    print("Shut down:")
    print("  loom service down")
    print()


def _up_local(args: argparse.Namespace) -> int:
    compose_file = args.compose_file
    env_file = args.env_file
    if not compose_file.is_file():
        sys.stderr.write(
            f"error: compose file not found at {compose_file}.\n"
            "Run `loom service up` from the loom repo root, or pass "
            "--compose-file PATH.\n",
        )
        return 1

    rc = _ensure_docker_compose_available()
    if rc != 0:
        return rc

    print(f"→ ensuring dev singleton admin secret ({args.admin_secret_file})")
    try:
        admin_token = _ensure_dev_admin_secret(args.admin_secret_file)
    except AdminSecretConfigError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1

    # Ensure LOOM_SECRET_STORE_MASTER_KEY is in .env before loom-service boots.
    # The key is needed at startup to initialise LocalEncryptedSecretStore; it
    # cannot be injected post-boot like the batch-runner CP token.
    # We generate once and never rotate automatically — rotating the key
    # would make all existing provider-connection secrets unreadable.
    if env_file is not None:
        _ensure_secret_store_master_key(env_file)
        print(
            "✓ secret-store master key written to .env "
            "(DO NOT lose this — rotating it loses all stored provider secrets)"
        )

    compose_base = _compose_args(compose_file, env_file)

    # Control Plane / Gateway refuse to boot until Alembic is at head.
    # Start storage first so auto-migration can run before schema-gated
    # services are asked to become healthy (#1462).
    print("→ docker compose up -d postgres minio")
    r = _run([*compose_base, "up", "-d", "postgres", "minio"], check=False)
    if r.returncode != 0:
        return r.returncode

    print("→ waiting for postgres to be healthy...")
    if not _wait_for_postgres(compose_file, env_file):
        sys.stderr.write(
            "error: postgres did not become healthy within "
            f"{_HEALTHCHECK_RETRIES * _HEALTHCHECK_INTERVAL_SEC:.0f}s.\n",
        )
        return 1

    print("→ alembic upgrade head")
    rc = _alembic_upgrade(args.db_url)
    if rc != 0:
        sys.stderr.write("error: alembic upgrade failed.\n")
        return rc

    mutable_images = _mutable_dev_images(compose_file)
    up_argv = [*compose_base, "up", "-d"]
    if mutable_images:
        # Fail closed for mutable local images: asking Compose to build before
        # start is what prevents a new host migration tree from being paired
        # with stale image code.  Fresh images remain cheap because BuildKit
        # reuses their cached layers.
        up_argv.append("--build")
        print("→ mutable local dev images detected; checking build cache before container start")
    print(f"→ docker compose up -d ({compose_file})")
    r = _run(up_argv, check=False)
    if r.returncode != 0:
        return r.returncode

    print("→ seeding team + worker tokens + benchmark fixtures")
    rc, tokens = _seed_test_data(args.db_url)
    if rc != 0:
        sys.stderr.write("error: seed_test_data.py failed.\n")
        return rc
    tokens["admin"] = admin_token

    # Mint the batch-runner CP token so the batch fan-out loop in
    # loom-service can submit trials to the Control Plane. Without this
    # token, batches accept (201) but sit at state=submitted forever.
    # We mint after seeding so the DB is fully migrated before the CP
    # receives any admin requests.
    # Sync benchmarks.toml AFTER seed so seed-time entry-point benchmarks
    # populate REGISTRY before the preflight collision check runs.
    _benchmarks_sync_config(args.db_url)

    print("→ minting batch-runner CP token")
    batch_runner_token = _mint_batch_runner_cp_token(admin_token, args.cp_url)
    if batch_runner_token:
        tokens["batch_runner_cp"] = batch_runner_token

    # Persist the just-seeded tokens to .env so `docker compose`,
    # curl/HTTPie examples, and the SPA's local-storage bootstrap all
    # keep working after `down -v` invalidates the previous set. Without
    # this, every fresh seed leaves .env pointing at a deleted token
    # hash and every API request 401s until the operator copy-pastes
    # the new tokens by hand.
    if env_file is not None:
        _write_env_tokens(env_file, tokens)
        # `loom` loads .env into its own environment at startup, and Compose
        # gives shell variables precedence over `--env-file`. Without this,
        # the containers recreated below would get the tokens from the old
        # .env and fail with 401 (worker register, batch fan-out).
        _export_env_tokens(tokens)
        print(f"→ updated {env_file} with fresh tokens")
        if batch_runner_token:
            print("✓ batch-runner CP token written to .env")

        # The worker container booted in the earlier `compose up` with
        # whatever LOOM_WORKER_TOKEN was in .env BEFORE the seed ran —
        # stale on a fresh `down -v` cycle. `docker restart` reuses
        # the old env; only `up --force-recreate` re-reads .env. Skip
        # this step when env_file is None (operator chose not to
        # persist; nothing to re-read).
        print("→ recreating worker so it picks up the fresh LOOM_WORKER_TOKEN")
        _run(
            [
                *_compose_args(compose_file, env_file),
                "up",
                "-d",
                "--force-recreate",
                "--no-deps",
                "worker",
            ],
            check=False,
        )
        if tokens.get("builder"):
            print("→ recreating task-image-builder so it picks up LOOM_TASK_IMAGE_BUILDER_TOKEN")
            _run(
                [
                    *_compose_args(compose_file, env_file),
                    "up",
                    "-d",
                    "--force-recreate",
                    "--no-deps",
                    "task-image-builder",
                ],
                check=False,
            )

        if batch_runner_token:
            # loom-service booted without LOOM_SVC_BATCH_RUNNER_CP_TOKEN
            # (it wasn't in .env yet when compose first started it).
            # Force-recreate so it picks up the freshly-written token.
            print("→ recreating loom-service so it picks up LOOM_SVC_BATCH_RUNNER_CP_TOKEN")
            _run(
                [
                    *_compose_args(compose_file, env_file),
                    "up",
                    "-d",
                    "--force-recreate",
                    "--no-deps",
                    "loom-service",
                ],
                check=False,
            )

    _print_summary(tokens)
    return 0


def _dev_host() -> str:
    bind = os.environ.get("LOOM_DEV_BIND_ADDR", "").strip()
    return bind if bind and bind not in {"0.0.0.0", "::"} else "localhost"


def _local_db_url() -> str:
    """Host-side Postgres URL matching the Compose port and credentials.

    Compose reads LOOM_DEV_POSTGRES_* from the shell or .env (which `loom`
    loads at startup), so the migration and seeding steps must use the same
    values instead of assuming port 5432.
    """
    env = os.environ
    if not any(env.get(f"LOOM_DEV_POSTGRES_{k}") for k in ("PORT", "USER", "PASSWORD", "DB")) and (
        not env.get("LOOM_DEV_BIND_ADDR")
    ):
        return _DEFAULT_DB_URL
    user = quote(env.get("LOOM_DEV_POSTGRES_USER") or "loom", safe="")
    password = quote(env.get("LOOM_DEV_POSTGRES_PASSWORD") or "loom", safe="")
    port = env.get("LOOM_DEV_POSTGRES_PORT") or "5432"
    database = env.get("LOOM_DEV_POSTGRES_DB") or "loom"
    return f"postgresql+psycopg://{user}:{password}@{_dev_host()}:{port}/{database}"


def _local_cp_url() -> str:
    port = os.environ.get("LOOM_DEV_CONTROL_PLANE_PORT")
    if not port and not os.environ.get("LOOM_DEV_BIND_ADDR"):
        return _DEFAULT_CP_URL
    return f"http://{_dev_host()}:{port or '8080'}"


def _export_docker_socket_for_compose() -> None:
    """Point the worker's socket mount at the daemon `docker` itself uses.

    Rootless Docker serves `unix://$XDG_RUNTIME_DIR/docker.sock`, not
    /var/run/docker.sock; the Compose file mounts LOOM_DEV_DOCKER_SOCKET.
    """
    if os.environ.get("LOOM_DEV_DOCKER_SOCKET"):
        return
    docker_host = os.environ.get("DOCKER_HOST", "")
    if docker_host.startswith("unix://"):
        os.environ["LOOM_DEV_DOCKER_SOCKET"] = docker_host.removeprefix("unix://")


def _populate_local_defaults(args: argparse.Namespace) -> None:
    if args.compose_file is None:
        args.compose_file = _DEFAULT_COMPOSE_FILE
    if args.env_file is None:
        args.env_file = _DEFAULT_ENV_FILE
    if args.db_url is None:
        args.db_url = _local_db_url()
    if args.admin_secret_file is None:
        args.admin_secret_file = _DEFAULT_DEV_ADMIN_SECRET_FILE
    if args.cp_url is None:
        args.cp_url = _local_cp_url()
    _export_docker_socket_for_compose()


def _up(args: argparse.Namespace) -> int:
    environment = getattr(args, "environment", None)
    candidate = getattr(args, "candidate", None)
    idempotency_key = getattr(args, "idempotency_key", None)
    if environment is not None:
        if (
            not environment.startswith("dev-")
            or not candidate
            or any(
                getattr(args, name, None) is not None
                for name in ("compose_file", "env_file", "db_url", "admin_secret_file", "cp_url")
            )
        ):
            print(
                "Managed up requires --environment dev-<name> --candidate <id> and no local Compose options.",
                file=sys.stderr,
            )
            return 1
        from loom_cli.dev_cmd import create_environment

        return create_environment(environment[4:], candidate, idempotency_key=idempotency_key)
    if candidate is not None or idempotency_key is not None:
        print(
            "--candidate and --idempotency-key require an explicit --environment dev-<name>.",
            file=sys.stderr,
        )
        return 1
    print("Target: disposable local Docker Compose.")
    _populate_local_defaults(args)
    return _up_local(args)


_ENV_TOKEN_KEYS: dict[str, str] = {
    "team": "LOOM_TEAM_TOKEN",
    "worker": "LOOM_WORKER_TOKEN",
    "builder": "LOOM_TASK_IMAGE_BUILDER_TOKEN",
    "admin": "LOOM_ADMIN_TOKEN",
    "batch_runner_cp": "LOOM_SVC_BATCH_RUNNER_CP_TOKEN",
    "secret_store_master_key": "LOOM_SECRET_STORE_MASTER_KEY",
}


def _export_env_tokens(tokens: dict[str, str]) -> None:
    for label, env_key in _ENV_TOKEN_KEYS.items():
        if tokens.get(label):
            os.environ[env_key] = tokens[label]


def _write_env_tokens(env_file: Path, tokens: dict[str, str]) -> None:
    """Replace LOOM_{TEAM,WORKER,ADMIN}_TOKEN= lines in `env_file` with
    the freshly-seeded values. Preserves any other lines in the file
    (comments, custom overrides), and appends a missing key rather
    than silently dropping it. Creates the file if absent.
    """
    lines: list[str] = []
    if env_file.exists():
        lines = env_file.read_text().splitlines()
    seen: set[str] = set()
    for i, raw in enumerate(lines):
        stripped = raw.lstrip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        for label, env_key in _ENV_TOKEN_KEYS.items():
            if key == env_key and tokens.get(label):
                lines[i] = f"{env_key}={tokens[label]}"
                seen.add(env_key)
                break
    for label, env_key in _ENV_TOKEN_KEYS.items():
        if env_key not in seen and tokens.get(label):
            lines.append(f"{env_key}={tokens[label]}")
    env_file.write_text("\n".join(lines) + ("\n" if lines else ""))


def _down(args: argparse.Namespace) -> int:
    compose_file = args.compose_file
    if not compose_file.is_file():
        sys.stderr.write(f"error: compose file not found at {compose_file}.\n")
        return 1
    rc = _ensure_docker_compose_available()
    if rc != 0:
        return rc
    cmd = [*_compose_args(compose_file, args.env_file), "down"]
    if args.volumes:
        cmd.append("-v")
    print(f"→ docker compose down ({compose_file}{' -v' if args.volumes else ''})")
    return _run(cmd, check=False).returncode


def _status(args: argparse.Namespace) -> int:
    compose_file = args.compose_file
    if not compose_file.is_file():
        sys.stderr.write(f"error: compose file not found at {compose_file}.\n")
        return 1
    rc = _ensure_docker_compose_available()
    if rc != 0:
        return rc
    print(f"→ docker compose ps ({compose_file})")
    rc = _run(
        [*_compose_args(compose_file, args.env_file), "ps"],
        check=False,
    ).returncode
    print()
    print("Endpoints:")
    for name, (url, desc) in _ENDPOINTS.items():
        print(f"  {name:18} {url:48} {desc}")
    print()
    return rc


def _generate_admin_token() -> str:
    return f"loom_admin_{secrets.token_urlsafe(32)}"


def _admin_secret_payload(token: str) -> dict[str, dict[str, object]]:
    return {
        "admin": {
            "token": token,
            "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "version": 1,
        },
    }


def _write_admin_secret_file(secret_file: Path, token: str) -> None:
    AdminSecretVerifier.from_token(token)
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = secret_file.with_name(f".{secret_file.name}.tmp-{os.getpid()}")
    try:
        tmp.write_text(
            tomli_w.dumps(_admin_secret_payload(token)),
            encoding="utf-8",
        )
        tmp.chmod(0o600)
        os.replace(tmp, secret_file)
        secret_file.chmod(0o600)
    finally:
        if tmp.exists():
            tmp.unlink()


def _read_admin_secret_file(secret_file: Path) -> str:
    if not secret_file.is_file():
        raise AdminSecretConfigError(f"admin secret file not found: {secret_file}")
    mode = secret_file.stat().st_mode & 0o777
    if mode & 0o077:
        raise AdminSecretConfigError(
            f"admin secret file permissions must be 0600; got {mode:o}",
        )
    try:
        data = tomllib.loads(secret_file.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise AdminSecretConfigError(
            f"admin secret file is not valid TOML: {secret_file}",
        ) from exc
    admin = data.get("admin")
    if not isinstance(admin, dict):
        raise AdminSecretConfigError("admin secret file missing [admin] section")
    token = admin.get("token")
    if not isinstance(token, str):
        raise AdminSecretConfigError("admin secret file missing admin.token")
    AdminSecretVerifier.from_token(token)
    return token


def _ensure_dev_admin_secret(secret_file: Path) -> str:
    if secret_file.exists():
        return _read_admin_secret_file(secret_file)
    token = _generate_admin_token()
    _write_admin_secret_file(secret_file, token)
    return token


def _init_admin(args: argparse.Namespace) -> int:
    secret_file = args.secret_file
    if secret_file.exists() and not args.force:
        sys.stderr.write(
            f"error: admin secret file already exists at {secret_file}. "
            "Use rotate-admin to replace it, or pass --force to overwrite.\n",
        )
        return 1
    token = _generate_admin_token()
    _write_admin_secret_file(secret_file, token)
    print(f"Admin secret file written: {secret_file}")
    print("Raw token not printed. Use `loom service reveal-admin --secret-file PATH` when needed.")
    print(
        "Mount this file as loom-admin-secret/secrets.toml and restart "
        "loom-service, loom-control-plane, and llm-gateway.",
    )
    return 0


def _reveal_admin(args: argparse.Namespace) -> int:
    try:
        token = _read_admin_secret_file(args.secret_file)
    except AdminSecretConfigError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    if not args.yes:
        sys.stderr.write(
            "This prints the raw singleton admin bearer token. Type REVEAL to continue: ",
        )
        answer = sys.stdin.readline().strip()
        if answer != "REVEAL":
            sys.stderr.write("aborted; token was not revealed.\n")
            return 2
    print(token)
    return 0


def _rotate_admin(args: argparse.Namespace) -> int:
    try:
        _read_admin_secret_file(args.secret_file)
    except AdminSecretConfigError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    token = _generate_admin_token()
    _write_admin_secret_file(args.secret_file, token)
    print(f"Admin secret file rotated: {args.secret_file}")
    print("Raw token not printed. Use reveal-admin only when an operator needs it.")
    print(
        "Restart loom-service, loom-control-plane, and llm-gateway so "
        "all processes load the new singleton admin secret.",
    )
    print("Old admin tokens become invalid after those processes restart.")
    return 0


def add_service_subparser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register `loom service {up,down,status}` on the top-level argparse."""
    p_service = sub.add_parser(
        "service",
        help="Manage local Compose or explicitly target personal Nebius development",
        description=(
            "Without an explicit target, service up manages disposable local Docker Compose."
        ),
    )
    service_sub = p_service.add_subparsers(dest="service_cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--compose-file",
        type=Path,
        default=_DEFAULT_COMPOSE_FILE,
        help=f"Local-only Docker Compose YAML (default: {_DEFAULT_COMPOSE_FILE})",
    )
    common.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Local-only .env file (default: ./.env; ignored if missing)",
    )

    admin_secret_common = argparse.ArgumentParser(add_help=False)
    admin_secret_common.add_argument(
        "--secret-file",
        type=Path,
        default=_DEFAULT_ADMIN_SECRET_FILE,
        help=(f"Path to singleton admin secrets.toml (default: {_DEFAULT_ADMIN_SECRET_FILE})"),
    )

    p_up = service_sub.add_parser(
        "up",
        help="Start the disposable local development stack",
    )
    p_up.add_argument(
        "--environment", help="Explicit personal target dev-<name>; never falls back to local"
    )
    p_up.add_argument("--candidate", help="Approved candidate UUID; required for a personal target")
    p_up.add_argument("--idempotency-key", help="Reuse the same key when retrying a managed create")
    local_options = p_up.add_argument_group("local Compose options")
    local_options.add_argument(
        "--compose-file",
        type=Path,
        default=None,
        help=f"Docker Compose YAML. Default: {_DEFAULT_COMPOSE_FILE}.",
    )
    local_options.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help=f"Environment file; ignored if missing. Default: {_DEFAULT_ENV_FILE}.",
    )
    local_options.add_argument(
        "--db-url",
        default=None,
        help=f"Postgres URL for migrations + seeding (default: {_DEFAULT_DB_URL})",
    )
    local_options.add_argument(
        "--admin-secret-file",
        type=Path,
        default=None,
        help=(
            "Dev singleton admin secrets.toml path mounted by compose "
            f"(default: {_DEFAULT_DEV_ADMIN_SECRET_FILE})"
        ),
    )
    local_options.add_argument(
        "--cp-url",
        default=None,
        help=(
            "Control Plane base URL for minting the batch-runner token "
            f"(default: {_DEFAULT_CP_URL})"
        ),
    )
    p_up.set_defaults(handler=_up)

    p_down = service_sub.add_parser(
        "down",
        parents=[common],
        help="Stop stack (preserves named volumes by default)",
    )
    p_down.add_argument(
        "-v",
        "--volumes",
        action="store_true",
        help="Also remove named volumes (wipes postgres + minio state)",
    )
    p_down.set_defaults(handler=_down)

    p_status = service_sub.add_parser(
        "status",
        parents=[common],
        help="Show container state + endpoint URLs",
    )
    p_status.set_defaults(handler=_status)

    p_init_admin = service_sub.add_parser(
        "init-admin",
        parents=[admin_secret_common],
        help="Create a singleton admin secret file without printing the token",
    )
    p_init_admin.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing secret file. Prefer rotate-admin for normal rotation.",
    )
    p_init_admin.set_defaults(handler=_init_admin)

    p_reveal_admin = service_sub.add_parser(
        "reveal-admin",
        parents=[admin_secret_common],
        help="Reveal the raw singleton admin token with confirmation",
    )
    p_reveal_admin.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation prompt.",
    )
    p_reveal_admin.set_defaults(handler=_reveal_admin)

    p_rotate_admin = service_sub.add_parser(
        "rotate-admin",
        parents=[admin_secret_common],
        help="Atomically replace the singleton admin token without printing it",
    )
    p_rotate_admin.set_defaults(handler=_rotate_admin)
