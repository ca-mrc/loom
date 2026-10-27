"""Observe real SQLAlchemy events without PostgreSQL, network or cleanup rewrites."""

import builtins
import json
import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, delete, event

from tests.support import postgres_cleanup_diagnostics as diagnostics


@pytest.fixture
def cleanup_engine():
    engine = create_engine("sqlite://")
    table = Table("cleanup_rows", MetaData(), Column("id", Integer, primary_key=True))
    table.metadata.create_all(engine)
    try:
        yield engine, table
    finally:
        engine.dispose()


def test_fast_cleanup_adds_no_sql_or_samples(cleanup_engine, monkeypatch, capsys):
    engine, table = cleanup_engine
    statements = []
    event.listen(engine, "before_cursor_execute", lambda *args: statements.append(args[2]))
    monkeypatch.setattr(diagnostics, "_bounded_snapshot_database_waits",
                        lambda *args: pytest.fail("fast cleanup sampled"))
    with engine.begin() as connection:
        with diagnostics.observe_row_cleanup(engine, "_cleanup_service_execution_test_rows"):
            connection.execute(delete(table).where(table.c.id == 987654321))
    assert statements == ["DELETE FROM cleanup_rows WHERE cleanup_rows.id = ?"]
    assert capsys.readouterr().err == ""
    assert not any(t.name == "loom-test-database-cleanup-observer" for t in threading.enumerate())


@pytest.mark.parametrize("fails", [False, True])
def test_slow_event_reports_only_safe_context_and_stops_listeners(
    cleanup_engine, monkeypatch, capsys, fails,
):
    engine, table = cleanup_engine
    sampled = threading.Event()
    registrations = []
    original_listen = event.listen
    snapshots = []

    def listen(target, name, callback):
        registrations.append((target, name, callback))
        original_listen(target, name, callback)

    def snapshot(url, pid):
        snapshots.append(pid)
        return {"status": "observed", "server_version_num": 160015, "waits": [{
            "role": "cleanup", "backend_type": "client backend", "state": "active",
            "wait_event_type": "Lock", "wait_event": "transactionid",
        }]}

    def emit(*args, **kwargs):
        builtins.print(*args, **kwargs)
        sampled.set()

    monkeypatch.setattr(diagnostics.event, "listen", listen)
    monkeypatch.setattr(diagnostics, "_connection_pid", lambda _: 123456789)
    monkeypatch.setattr(diagnostics, "print", emit, raising=False)
    monkeypatch.setattr(diagnostics, "INITIAL_DELAY_SECONDS", 0.01)
    monkeypatch.setattr(diagnostics, "SAMPLE_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(diagnostics, "_bounded_snapshot_database_waits", snapshot)
    original = ValueError("PRIVATE original cleanup failure")
    delayed = False

    def pause(*args):
        nonlocal delayed
        if not args[4].compiled.statement.is_delete:
            return
        delayed = True
        assert sampled.wait(2), "slow stage never sampled"
        if fails:
            raise original

    expected = pytest.raises(ValueError) if fails else nullcontext()
    try:
        with expected as caught:
            with engine.begin() as connection:
                with diagnostics.observe_row_cleanup(engine, "_cleanup_service_execution_test_rows"):
                    event.listen(engine, "before_cursor_execute", pause)
                    connection.execute(delete(table).where(table.c.id == 987654321))
        if fails:
            assert caught.value is original
    finally:
        for target, name, callback in registrations:
            if callback is pause and event.contains(target, name, callback):
                event.remove(target, name, callback)
    assert delayed and snapshots and set(snapshots) == {123456789}
    assert all(not event.contains(*entry) for entry in registrations)
    assert not any(t.name == "loom-test-database-cleanup-observer" for t in threading.enumerate())
    output = capsys.readouterr().err
    reports = [json.loads(line) for line in output.splitlines()]
    assert reports and all(report["event"] == "slow_test_row_cleanup" for report in reports)
    assert all(report["fixture"] == "_cleanup_service_execution_test_rows" for report in reports)
    assert any(report["phase"] == "execute" and report["operation"] == "delete"
               and report["table"] == "cleanup_rows" for report in reports)
    assert all(report["database"]["server_version_num"] == 160015 for report in reports)
    assert all(word not in output for word in ["PRIVATE", "987654321", "123456789", "sqlite", "DELETE"])


def test_partial_listener_setup_does_not_replace_original_failure(cleanup_engine, monkeypatch):
    engine, _ = cleanup_engine
    original_listen = event.listen
    registered = []

    def listen(target, name, callback):
        original_listen(target, name, callback)
        registered.append((target, name, callback))
        raise RuntimeError("PRIVATE setup failure after registration")

    monkeypatch.setattr(diagnostics.event, "listen", listen)
    original = ValueError("original cleanup failure")
    with pytest.raises(ValueError) as caught, diagnostics.observe_row_cleanup(engine, "claim_setup"):
        raise original
    assert caught.value is original
    assert registered and all(not event.contains(*entry) for entry in registered)


def test_sampler_reports_libpq_server_version_without_an_extra_query(monkeypatch):
    executed = []
    connection = SimpleNamespace(
        info=SimpleNamespace(server_version=160015),
        execute=lambda sql, parameters: executed.append((sql, parameters)) or SimpleNamespace(
            fetchall=lambda: [("cleanup", "client backend", "active", "Lock", "transactionid")],
        ),
    )
    monkeypatch.setattr(diagnostics.psycopg, "connect", lambda *args, **kwargs: nullcontext(connection))
    result = diagnostics.snapshot_database_waits("postgresql://PRIVATE", 123456789)
    assert result["server_version_num"] == 160015 and len(executed) == 1
    assert "PRIVATE" not in json.dumps(result) and "123456789" not in json.dumps(result)


def test_early_sample_budget_finishes_before_cleanup_can_near_its_deadline(monkeypatch):
    finished = threading.Event()
    calls = []

    def snapshot(*args):
        calls.append(True)
        if len(calls) == 2:
            finished.set()
        return {"status": "unavailable"}

    monkeypatch.setattr(diagnostics, "INITIAL_DELAY_SECONDS", 0)
    monkeypatch.setattr(diagnostics, "SAMPLE_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(diagnostics, "_bounded_snapshot_database_waits", snapshot)
    with diagnostics._observe_cleanup(
        "unused", lambda: (1, time.monotonic(), {}), "test", sample_limit=2,
    ):
        assert finished.wait(2)
    assert len(calls) == 2
    assert not any(t.name == "loom-test-database-cleanup-observer" for t in threading.enumerate())
