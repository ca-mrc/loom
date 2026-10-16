"""Observe actual PostgreSQL waits without weakening the failing cleanup deadline."""

import importlib
import json
import threading

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

pytestmark = pytest.mark.timeout(30)


def _module():
    try:
        return importlib.import_module("tests.support.postgres_cleanup_diagnostics")
    except ModuleNotFoundError:
        pytest.fail("slow PostgreSQL cleanup has no safe server-wait diagnostics")


@pytest.mark.parametrize("fails", [False, True])
def test_slow_operation_reports_server_wait_and_preserves_outcome(
    postgres_url, monkeypatch, capsys, fails
):
    module = _module()
    monkeypatch.setattr(module, "INITIAL_DELAY_SECONDS", 0.05)
    monkeypatch.setattr(module, "SAMPLE_INTERVAL_SECONDS", 0.05)
    engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql("SET application_name='PRIVATE_APPLICATION_VALUE'")
            with module.observe_database_cleanup(connection) as phase:
                phase("drop_database")
                if fails:
                    connection.exec_driver_sql("SET statement_timeout='1000ms'")
                    with pytest.raises(OperationalError) as caught:
                        connection.exec_driver_sql("SELECT pg_sleep(10) /* PRIVATE_QUERY_VALUE */")
                    assert caught.value.orig.sqlstate == "57014"
                else:
                    connection.exec_driver_sql("SELECT pg_sleep(1) /* PRIVATE_QUERY_VALUE */")
    finally:
        engine.dispose()
    captured = capsys.readouterr().err
    reports = [json.loads(line) for line in captured.splitlines()]
    assert reports and len(reports) <= 6
    assert any(
        row["role"] == "cleanup"
        and row["wait_event_type"] == "Timeout"
        and row["wait_event"] == "PgSleep"
        for report in reports
        for row in report["database"]["waits"]
    )
    assert all(
        report["phase"] == "drop_database" and report["elapsed_seconds"] >= 0 for report in reports
    )
    assert all(
        set(row) == {"role", "backend_type", "state", "wait_event_type", "wait_event"}
        for report in reports
        for row in report["database"]["waits"]
    )
    assert "PRIVATE" not in captured and postgres_url not in captured
    assert not any(
        thread.name == "loom-test-database-cleanup-observer" for thread in threading.enumerate()
    )


def test_healthy_cleanup_and_early_failure_do_not_open_observer_connection(
    postgres_url, monkeypatch, capsys
):
    module = _module()
    samples = []

    def unexpected(*args, **kwargs):
        samples.append(True)
        return {"status": "unavailable"}

    monkeypatch.setattr(module, "_bounded_snapshot_database_waits", unexpected)
    engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")
    failure = RuntimeError("original failure")
    try:
        with engine.connect() as connection:
            with module.observe_database_cleanup(connection) as phase:
                phase("prevent_connections")
                assert connection.exec_driver_sql("SELECT 1").scalar_one() == 1
            with pytest.raises(RuntimeError) as caught, module.observe_database_cleanup(connection):
                raise failure
            assert caught.value is failure
    finally:
        engine.dispose()
    assert capsys.readouterr().err == ""
    assert samples == []
    assert not any(
        thread.name == "loom-test-database-cleanup-observer" for thread in threading.enumerate()
    )
