"""Cleanup diagnostics must never log connection material or exception text."""

import importlib
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

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


@pytest.mark.parametrize("failure_point", ["connection", "thread_construct", "thread_start"])
def test_diagnostic_setup_failure_does_not_replace_cleanup_outcome(monkeypatch, failure_point):
    module = _module()

    def fail(*args, **kwargs):
        raise RuntimeError("PRIVATE diagnostic setup error")

    connection = SimpleNamespace(
        connection=SimpleNamespace(
            driver_connection=SimpleNamespace(info=SimpleNamespace(backend_pid=123))
        ),
        engine=SimpleNamespace(
            url=SimpleNamespace(
                set=lambda **kwargs: SimpleNamespace(
                    render_as_string=lambda **kwargs: "postgresql://PRIVATE@localhost/database"
                )
            )
        ),
        exec_driver_sql=fail,
    )
    if failure_point == "connection":
        connection.connection = None
    elif failure_point == "thread_construct":
        monkeypatch.setattr(threading, "Thread", fail)
    else:
        monkeypatch.setattr(threading.Thread, "start", fail)
    original = ValueError("original cleanup failure")
    cleanup_ran = False
    with pytest.raises(ValueError) as caught:
        with module.observe_database_cleanup(connection) as phase:
            phase("drop_database")
            cleanup_ran = True
            raise original
    assert cleanup_ran
    assert caught.value is original


@pytest.mark.parametrize("fails", [False, True])
def test_inflight_snapshot_is_killed_and_reaped_before_exit(monkeypatch, capsys, fails):
    module = _module()
    started = threading.Event()
    children = []
    real_popen = subprocess.Popen

    def stuck_child(command, **kwargs):
        # Keep the real process boundary and timeout; replace only PostgreSQL
        # with an indefinitely stalled client to exercise kill/reap behavior.
        assert "PRIVATE" not in str(command)
        child = real_popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        started.set()
        return child

    monkeypatch.setattr(subprocess, "Popen", stuck_child)
    monkeypatch.setattr(module, "INITIAL_DELAY_SECONDS", 0)
    monkeypatch.setattr(module, "SNAPSHOT_TIMEOUT_SECONDS", 0.5, raising=False)
    connection = SimpleNamespace(
        connection=SimpleNamespace(
            driver_connection=SimpleNamespace(info=SimpleNamespace(backend_pid=123))
        ),
        engine=SimpleNamespace(
            url=SimpleNamespace(
                set=lambda **kwargs: SimpleNamespace(
                    render_as_string=lambda **kwargs: "postgresql://PRIVATE@localhost/database"
                )
            )
        ),
        exec_driver_sql=lambda _: SimpleNamespace(scalar_one=lambda: 123),
    )
    original = ValueError("original cleanup failure")
    before = time.monotonic()
    try:
        try:
            with module.observe_database_cleanup(connection):
                assert started.wait(2), "snapshot did not use a killable child"
                if fails:
                    raise original
        except ValueError as caught:
            assert fails and caught is original
        else:
            assert not fails
        assert time.monotonic() - before < 3
        assert children and all(child.poll() is not None for child in children)
        assert not any(
            thread.name == "loom-test-database-cleanup-observer" for thread in threading.enumerate()
        )
        assert "PRIVATE" not in capsys.readouterr().err
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
