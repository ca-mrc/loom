"""Real fresh development DB bootstrap without local task execution authority."""

from __future__ import annotations

import json
import sys

import psycopg
import pytest
from sqlalchemy.engine import make_url

from loom import nebius_platform_bootstrap as bootstrap
from tests.integration.test_nebius_platform_bootstrap import platform_database as platform_database


def test_development_bootstrap_has_service_roles_but_no_execution_authority(platform_database, monkeypatch, tmp_path):
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    for name in ("SERVICE", "CONTROL_PLANE", "GATEWAY"):
        monkeypatch.setenv("LOOM_DB_" + name + "_PASSWORD", "dev-test-" + name + "x" * 30)
    for name in ("LOOM_COLLECTOR_TOKEN", "LOOM_BATCH_RUNNER_TOKEN", "LOOM_DB_ACTUATOR_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    config = {"schema_version": "loom.nebius-development-bootstrap.v1",
              "namespace": "loom-dev", "environment": "development"}
    bootstrap.bootstrap_development_database(config)
    config_path = tmp_path / "environment.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setenv("LOOM_PLATFORM_CONFIG", str(config_path))
    monkeypatch.setattr(sys, "argv", ["nebius_platform_bootstrap", "development-database"])
    assert bootstrap.main() == 0
    with psycopg.connect(platform_database) as db:
        assert db.execute("SELECT count(*) FROM tokens WHERE type='worker'").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM execution_targets").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM execution_capacity_policies").fetchone() == (0,)
        assert db.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('loom_service', 'loom_control_plane', 'loom_gateway', 'loom_actuator') ORDER BY rolname").fetchall() == [
            ("loom_control_plane",), ("loom_gateway",), ("loom_service",),
        ]
        assert db.execute("SELECT version_num FROM alembic_version").fetchone() is not None
    gateway_url = make_url(platform_database).set(username="loom_gateway",
        password="dev-test-GATEWAY" + "x" * 30).render_as_string(hide_password=False)
    with psycopg.connect(gateway_url, autocommit=True) as db:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute("CREATE ROLE forbidden_admin SUPERUSER")
        assert db.execute("SELECT has_table_privilege(current_user, 'tokens', 'INSERT')").fetchone() == (False,)
