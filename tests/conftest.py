"""Top-level test fixtures live here. Per-package fixtures live in
nested conftest.py files.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from tests.support.postgres_databases import isolated_migration_database

_TEST_STEP_JWT_SIGNING_KEY = "test-step-jwt-signing-key-do-not-use-in-prod"


@pytest.fixture(autouse=True)
def _local_http_test_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy ASGI fixtures use HTTP. Hosted-auth tests explicitly turn this off."""
    monkeypatch.setenv("LOOM_SVC_AUTH_LOCAL_HTTP", "true")


@pytest.fixture(autouse=True)
def _default_step_jwt_signing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """ControlPlaneSettings + GatewaySettings now require this env var.
    Existing tests don't know about it; this autouse fixture supplies a
    fixed-but-distinct key so they continue to construct settings without
    each fixture needing the explicit setenv call. Production deploys
    must override via loom-secrets/step-jwt-signing-key.
    """
    monkeypatch.setenv("LOOM_CP_STEP_JWT_SIGNING_KEY", _TEST_STEP_JWT_SIGNING_KEY)
    monkeypatch.setenv("LOOM_GW_STEP_JWT_SIGNING_KEY", _TEST_STEP_JWT_SIGNING_KEY)
    monkeypatch.setenv("LOOM_GW_MINIO_ACCESS_KEY", "test-minio-access-key")
    monkeypatch.setenv("LOOM_GW_MINIO_SECRET_KEY", "test-minio-secret-key")


@pytest.fixture(scope="session")
def postgres_cluster_url() -> Iterator[str]:
    """Own the disposable server without migrating an application database."""
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://"
        )


@pytest.fixture(scope="session")
def _migration_template_postgres_url(postgres_cluster_url: str) -> Iterator[str]:
    """Run the real migration chain once and seal the clean session template.

    Both application fixtures and historical migration tests clone this closed
    template. Initializing a second full chain at the first historical test
    needlessly consumed its existing per-test setup deadline.
    """
    yield from isolated_migration_database(
        postgres_cluster_url, template_name="template0", prepare_template=True
    )


@pytest.fixture(scope="session")
def migration_template_postgres_url(_migration_template_postgres_url: str) -> str:
    """Public fixture alias; module imports reuse the one session template."""
    return _migration_template_postgres_url


@pytest.fixture(scope="session")
def postgres_url(_migration_template_postgres_url: str) -> Iterator[str]:
    """Session application database, isolated from the clean migration template."""
    template_name = make_url(_migration_template_postgres_url).database
    assert template_name is not None
    previous = os.environ.get("LOOM_DB_URL")
    try:
        with contextmanager(isolated_migration_database)(
            _migration_template_postgres_url, template_name=template_name, prepare_template=False
        ) as url:
            os.environ["LOOM_DB_URL"] = url
            yield url
    finally:
        if previous is None:
            os.environ.pop("LOOM_DB_URL", None)
        else:
            os.environ["LOOM_DB_URL"] = previous
