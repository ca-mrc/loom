"""The rendered one-shot command must fail closed without its bound inputs."""
from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


def test_module_command_never_reports_success_without_mounted_targets():
    result = subprocess.run([sys.executable, "-m", "loom_service.environment_management.retirement"],
        capture_output=True, text=True, timeout=15,
        env=os.environ | {"LOOM_RETIREMENT_DB_URL": "private-value-that-must-not-leak"})
    assert result.returncode == 1
    assert json.loads(result.stdout) == {"status": "retirement_blocked"}
    assert "private-value" not in result.stdout + result.stderr


@pytest.mark.parametrize("field,value", [
    ("host", "foreign.svc"), ("username", "postgres"), ("drivername", "postgresql+asyncpg"),
    ("query", {"sslmode": "disable"}), ("database", "foreign"),
])
def test_retirement_database_rejects_wrong_scope_before_connecting(field, value):
    from sqlalchemy.engine import URL

    from loom_service.environment_management.retirement import retirement_database_url

    url = URL.create("postgresql", username="loom_service", password="private", host="loom-postgres.loom-nebius-management.svc",
        port=5432, database="loom", query={"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"})
    with pytest.raises(ValueError, match="retirement_database_unqualified"):
        retirement_database_url(url.set(**{field: value}).render_as_string(hide_password=False), "loom-nebius-management")
    qualified = retirement_database_url(url.render_as_string(hide_password=False), "loom-nebius-management")
    assert qualified.drivername == "postgresql+psycopg"
    assert qualified.query == {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}
