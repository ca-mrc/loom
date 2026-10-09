"""Unit tests for #81 slice B-4 — worker metrics registered + HTTP
server starts on the configured port at process entry."""

from __future__ import annotations

from functools import partial

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def _isolate_env_from_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in (
        "LOOM_WORKER_TOKEN", "LOOM_TEAM_TOKEN", "LOOM_ADMIN_TOKEN",
    ):
        monkeypatch.delenv(k, raising=False)


def test_worker_metric_objects_registered() -> None:
    from loom_worker import metrics  # noqa: F401

    names = {m.name for m in REGISTRY.collect()}
    expected = {
        "loom_worker_trials_inflight",
        "loom_worker_trials_started",
        "loom_worker_trials_completed",
        "loom_worker_trial_duration_sec",
        "loom_worker_claim_loop_iterations",
        "loom_worker_heartbeat_failures",
    }
    for stem in expected:
        assert stem in names or f"{stem}_total" in names or any(
            n.startswith(stem) for n in names
        ), f"metric stem {stem!r} missing"


def test_main_entry_starts_metrics_http_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`python -m loom_worker` MUST call start_http_server on the
    configured port before running the asyncio worker loop. We
    monkeypatch both start_http_server (record the call) and
    asyncio.run (short-circuit it) so the test doesn't need a real
    CP / DB to drive run_worker."""
    from loom_worker import __main__ as worker_main
    from loom_worker.config import WorkerSettings

    # The server is intercepted, so no socket or real worker connection opens.
    port = 19090
    monkeypatch.setenv("LOOM_WORKER_METRICS_PORT", str(port))
    monkeypatch.setenv("LOOM_WORKER_TOKEN", "tok")
    monkeypatch.setenv("LOOM_WORKER_MINIO_ACCESS_KEY", "x")
    monkeypatch.setenv("LOOM_WORKER_MINIO_SECRET_KEY", "x")
    monkeypatch.setattr(worker_main, "WorkerSettings", partial(WorkerSettings, _env_file=None))

    calls: list[tuple[str, int]] = []

    def _fake_start_http_server(p: int, *a, **kw) -> None:  # type: ignore[no-untyped-def]
        calls.append(("metrics", p))

    def _fake_asyncio_run(coro) -> None:  # type: ignore[no-untyped-def]
        # Close the coroutine to avoid the "coroutine was never
        # awaited" RuntimeWarning.
        coro.close()
        calls.append(("worker", port))

    monkeypatch.setattr(worker_main, "start_http_server", _fake_start_http_server)
    monkeypatch.setattr(worker_main.asyncio, "run", _fake_asyncio_run)

    worker_main.main()

    assert calls == [("metrics", port), ("worker", port)]
