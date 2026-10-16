"""Session fixtures: bring the compose stack up once per session.

`compose_stack` returns dict with service URLs + the team token + the
worker token. Tests that submit trials use `compose_stack["team_token"]`;
the worker_crash test passes `compose_stack["worker_token"]` back to
`start_service("worker", ...)` after killing the worker container.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tests.system.docker_compose import database_url, stack_down_with_diagnostics, stack_up


@pytest.fixture(scope="session")
def compose_stack(request: pytest.FixtureRequest) -> Iterator[dict[str, str]]:
    if os.environ.get("LOOM_SKIP_SYSTEM_TESTS") == "1":
        pytest.skip("LOOM_SKIP_SYSTEM_TESTS=1 — skipping compose-based suite")
    team_token, worker_token = stack_up()
    try:
        yield {
            "control_plane": "http://localhost:58080",
            "gateway": "http://localhost:59100",
            "minio": "http://localhost:59000",
            "team_token": team_token,
            "worker_token": worker_token,
            # Plan 16 smoke tests drive `run_import` directly, which
            # needs DB + MinIO creds. The compose file fixes these to
            # static test values — see deploy/docker-compose.test.yml
            # for `POSTGRES_USER/PASSWORD/DB` and `MINIO_ROOT_USER/PASSWORD`.
            # Docker allocates the host DB port; use the actual binding.
            # Keep this block in sync if the test creds ever change.
            "db_url": database_url(),
            "minio_access_key": "loomtest",
            "minio_secret_key": "loomtest",
        }
    finally:
        stack_down_with_diagnostics(failed=request.session.testsfailed > 0)
