"""Cleanup diagnostics must never log connection material or exception text."""

import importlib

import psycopg
import pytest


def _module():
    try:
        return importlib.import_module("tests.support.postgres_cleanup_diagnostics")
    except ModuleNotFoundError:
        pytest.fail("slow PostgreSQL cleanup has no safe server-wait diagnostics")


@pytest.mark.parametrize("failure", [psycopg.OperationalError, RuntimeError])
def test_observer_failure_returns_only_closed_category(monkeypatch, failure):
    module = _module()
    def fail(*args, **kwargs):
        raise failure("PRIVATE DATABASE PASSWORD AND QUERY TEXT")
    monkeypatch.setattr(psycopg, "connect", fail)
    result = module.snapshot_database_waits("postgresql://PRIVATE@localhost/database", 123)
    assert result == {"status": "unavailable"}
    assert "PRIVATE" not in str(result)
