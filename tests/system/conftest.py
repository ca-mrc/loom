"""Session fixtures: bring the compose stack up once per session.

`compose_stack` returns dict with service URLs + the team token + the
worker token. Tests that submit trials use `compose_stack["team_token"]`;
the worker_crash test passes `compose_stack["worker_token"]` back to
`start_service("worker", ...)` after killing the worker container.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Generator, Iterator

import pytest

from loom.security.redaction import redact_text
from tests.system.docker_compose import (
    database_url,
    preserve_compose_diagnostics,
    service_urls,
    stack_down_with_diagnostics,
    stack_up,
)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(
    item: pytest.Item,
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if report.failed and report.when in {"setup", "call"} and "compose_stack" in getattr(item, "funcargs", {}):
        # Session teardown can be minutes later, after another test recovers the
        # stack and pushes the original failure out of the bounded log tail.
        # Harness tests without the actual fixture must never capture Docker.
        try:
            preserve_compose_diagnostics()
        except Exception as exc:
            print(
                f"warning: failed to preserve Compose diagnostics: {redact_text(str(exc))}",
                file=sys.stderr,
            )
    return report


@pytest.fixture(scope="session")
def compose_stack(request: pytest.FixtureRequest) -> Iterator[dict[str, str]]:
    if os.environ.get("LOOM_SKIP_SYSTEM_TESTS") == "1":
        pytest.skip("LOOM_SKIP_SYSTEM_TESTS=1 — skipping compose-based suite")
    team_token, worker_token = stack_up()
    try:
        yield {
            **service_urls(),
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
