"""Read-only, bounded evidence for slow disposable-database cleanup.

Never change the cleanup SQL, retry it, or extend its pytest deadline. Only
server-owned wait categories are emitted: no SQL, database names, PIDs, client
addresses, connection strings, application names or exception messages.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import psycopg
from sqlalchemy import event
from sqlalchemy.engine import Connection, Engine

INITIAL_DELAY_SECONDS = 5.0
SAMPLE_INTERVAL_SECONDS = 10.0
SNAPSHOT_TIMEOUT_SECONDS = 3.0
_MAX_SAMPLES = 6
Phase = Literal["prevent_connections", "terminate_backends", "drop_database"]


def _category(value: object) -> str | None:
    if value is None:
        return None
    return (
        value
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_ -]{1,64}", value)
        else "other"
    )


def snapshot_database_waits(url: str, cleanup_pid: int) -> dict[str, object]:
    """A separate read-only connection cannot wait on the blocked cleanup client."""
    try:
        with psycopg.connect(
            url,
            autocommit=True,
            connect_timeout=2,
            options="-c default_transaction_read_only=on -c statement_timeout=1000 -c lock_timeout=1000",
        ) as connection:
            version = connection.info.server_version
            server_version = version if type(version) is int and version > 0 else None
            rows = connection.execute(
                """
                SELECT CASE WHEN pid = %s THEN 'cleanup'
                            WHEN backend_type = 'checkpointer' THEN 'checkpointer'
                            ELSE 'blocker' END,
                       backend_type, state, wait_event_type, wait_event
                FROM pg_stat_activity
                WHERE pid = %s OR pid = ANY(pg_blocking_pids(%s)) OR backend_type = 'checkpointer'
                ORDER BY 1, 2, 3, 4, 5 LIMIT 8
                """,
                (cleanup_pid, cleanup_pid, cleanup_pid),
            ).fetchall()
        fields = ("role", "backend_type", "state", "wait_event_type", "wait_event")
        return {
            "status": "observed",
            "waits": [dict(zip(fields, map(_category, row), strict=True)) for row in rows],
            "server_version_num": server_version,
        }
    except Exception:
        # Diagnostics are best-effort; do not hide the original failure with a
        # second exception or leak driver connection/error text into CI logs.
        return {"status": "unavailable"}


def _bounded_snapshot_database_waits(url: str, cleanup_pid: int) -> dict[str, object]:
    try:
        result = subprocess.run(
            [sys.executable, "-I", str(Path(__file__).resolve())],
            input=json.dumps({"url": url, "cleanup_pid": cleanup_pid}),
            capture_output=True,
            text=True,
            check=True,
            timeout=SNAPSHOT_TIMEOUT_SECONDS,
        )
        return json.loads(result.stdout)
    except Exception:
        # A client read can hang despite PostgreSQL's statement_timeout. run()
        # kills AND reaps the child on this wall-clock deadline. Credentials go
        # only through stdin, never argv, and child stderr is never forwarded.
        return {"status": "unavailable"}


@contextmanager
def _observe_cleanup(
    url: str, read_state: Callable[[], tuple[int | None, float, dict[str, object]]],
    event_name: str, *, sample_limit: int | None = None,
) -> Iterator[None]:
    stopped = threading.Event()

    def watch() -> None:
        delay = INITIAL_DELAY_SECONDS
        for _ in range(_MAX_SAMPLES if sample_limit is None else sample_limit):
            if stopped.wait(delay):
                return
            cleanup_pid, started, fields = read_state()
            result = (_bounded_snapshot_database_waits(url, cleanup_pid)
                      if cleanup_pid is not None else {"status": "unavailable"})
            if stopped.is_set():
                return
            try:
                print(
                    json.dumps(
                        {
                            "event": event_name,
                            **fields,
                            "elapsed_seconds": round(time.monotonic() - started, 3),
                            "database": result,
                        }
                    ),
                    file=sys.stderr,
                    flush=True,
                )
            except Exception:
                return
            delay = SAMPLE_INTERVAL_SECONDS

    try:
        observer = threading.Thread(
            target=watch, name="loom-test-database-cleanup-observer", daemon=True
        )
        observer.start()
    except Exception:
        yield
        return
    try:
        yield
    finally:
        stopped.set()
        # Wake sleepers immediately; drain at most one client-bounded subprocess.
        # Never leave a live thread or database client after fixture teardown.
        observer.join()


@contextmanager
def observe_database_cleanup(connection: Connection) -> Iterator[Callable[[Phase], None]]:
    """Sample at most six times and stop the observer on every exit path."""
    try:
        cleanup_pid = connection.connection.driver_connection.info.backend_pid
        url = connection.engine.url.set(drivername="postgresql").render_as_string(
            hide_password=False
        )
    except Exception:
        yield lambda _: None
        return
    phase: tuple[Phase, float] = ("prevent_connections", time.monotonic())

    def set_phase(value: Phase) -> None:
        nonlocal phase
        if value not in {"prevent_connections", "terminate_backends", "drop_database"}:
            raise ValueError("invalid database cleanup phase")
        phase = (value, time.monotonic())

    with _observe_cleanup(
        url, lambda: (cleanup_pid, phase[1], {"phase": phase[0]}),
        "slow_test_database_cleanup",
    ):
        yield set_phase


def _connection_pid(connection: Connection) -> int | None:
    try:
        value = connection.connection.driver_connection.info.backend_pid
        return value if type(value) is int and value > 0 else None
    except Exception:
        return None


@contextmanager
def observe_row_cleanup(engine: Engine, fixture_name: str) -> Iterator[None]:
    """Observe one DELETE through AsyncEngine.sync_engine without extra SQL.

    Keep the context around only the target execute, not its transaction or
    disposal. Safe table labels come from compiled DML metadata, never SQL text.
    Two early samples finish well before a nearly exhausted 60-second test;
    their bounded child must not add a late three-second join to its cleanup.
    """
    label = (fixture_name if isinstance(fixture_name, str)
             and re.fullmatch(r"[a-z_][a-z0-9_]{0,63}", fixture_name) else "other")
    state: tuple[int | None, float, dict[str, object]] = (
        None, time.monotonic(), {"fixture": label, "phase": "awaiting_statement"},
    )

    def before(connection: Connection, cursor, statement, parameters, context, executemany) -> None:
        nonlocal state
        try:
            expression = getattr(getattr(context, "compiled", None), "statement", None)
            if not getattr(expression, "is_delete", False):
                return
            table = getattr(getattr(expression, "table", None), "name", None)
            safe_table = (table if isinstance(table, str)
                          and re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", table) else None)
            state = (_connection_pid(connection), time.monotonic(), {
                "fixture": label, "phase": "execute", "operation": "delete", "table": safe_table,
            })
        except Exception:
            pass  # Diagnostics cannot change the original statement outcome.

    def remove_listener() -> None:
        try:
            event.remove(engine, "before_cursor_execute", before)
        except Exception:
            pass  # Also handles partial or failed registration.

    try:
        url = engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
        event.listen(engine, "before_cursor_execute", before)
    except Exception:
        remove_listener()
        yield
        return
    try:
        with _observe_cleanup(url, lambda: state, "slow_test_row_cleanup", sample_limit=2):
            yield
    finally:
        remove_listener()


if __name__ == "__main__":
    try:
        request = json.load(sys.stdin)
        response = snapshot_database_waits(request["url"], request["cleanup_pid"])
    except Exception:
        response = {"status": "unavailable"}
    print(json.dumps(response))
