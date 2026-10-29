"""Prometheus metrics for loom_service (#81 slice B-3).

Cardinality bounded:
- `route` is a fixed set (the FastAPI route templates, ~25 paths).
- `status_class` is 2xx / 4xx / 5xx (3 values) instead of raw status.
- `result` is small per metric.

Naming: `loom_svc_*` prefix.
"""

from __future__ import annotations

import time
from typing import Any

from prometheus_client import Counter, Histogram
from sqlalchemy import event
from sqlalchemy.engine import Engine

HTTP_REQUESTS_TOTAL = Counter(
    "loom_svc_http_requests_total",
    "HTTP requests handled by loom_service",
    # status_class folded down to 2xx/4xx/5xx; raw status would be
    # unbounded if /metrics scraped infrequently.
    labelnames=("route", "method", "status_class"),
)

HTTP_REQUEST_LATENCY_SEC = Histogram(
    "loom_svc_http_request_latency_sec",
    "HTTP request latency by route",
    labelnames=("route", "method"),
    # Service is mostly synchronous DB+forwarder operations; the long
    # tail comes from gateway forwards.
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

BATCH_RUNNER_TICKS_TOTAL = Counter(
    "loom_svc_batch_runner_ticks_total",
    "Batch runner background-task ticks",
    # result: ok / skipped_no_token / error
    labelnames=("result",),
)

BATCH_RUNNER_TRIALS_DISPATCHED = Counter(
    "loom_svc_batch_runner_trials_dispatched_total",
    "Trials the batch runner has fanned out from queued batches",
)

TOKENS_ISSUED_TOTAL = Counter(
    "loom_svc_tokens_issued_total",
    "Tokens minted via /api/v1/tokens",
    # token_type: team / admin
    labelnames=("token_type",),
)

TOKENS_REVOKED_TOTAL = Counter(
    "loom_svc_tokens_revoked_total",
    "Tokens revoked via DELETE /api/v1/tokens/{prefix}",
)

AUTH_FAILURES_TOTAL = Counter(
    "loom_svc_auth_failures_total",
    "Authentication and session guard failures in loom_service",
    # auth_kind: anonymous / bearer / session
    # reason: missing_or_invalid / unsupported_principal / csrf
    labelnames=("auth_kind", "reason"),
)

INVITES_TOTAL = Counter(
    "loom_svc_invites_total",
    "Invite lifecycle mutations in loom_service",
    # action: create / accept / revoke / resend
    # result: success / rejected
    labelnames=("action", "result"),
)

SUBMISSION_REJECTS_TOTAL = Counter(
    "loom_svc_submission_rejects_total",
    "Batch submission requests rejected before fan-out",
    # reason: permission / team_paused / no_workers / invalid_input /
    # empty_filter / invalid_task_config / provider_connection / ...
    labelnames=("reason",),
)

ARTIFACT_DOWNLOAD_BYTES = Counter(
    "loom_svc_artifact_download_bytes_total",
    "Bytes served through authenticated artifact download routes",
    # artifact_kind: trajectory / atif / artifact
    labelnames=("artifact_kind",),
)

PIPELINE_LIVE_PREVIEW_READS_TOTAL = Counter(
    "loom_svc_pipeline_live_preview_reads_total",
    "Bounded same-origin Pipeline live preview read outcomes",
    labelnames=("result", "reason"),
)

TEAM_EMERGENCY_ACTIONS_TOTAL = Counter(
    "loom_svc_team_emergency_actions_total",
    "Emergency team controls invoked by staging operators",
    # action: disable / enable / pause_submissions / resume_submissions
    labelnames=("action",),
)


# #2687: split request latency into its database phases, so a slow page can be
# attributed to session-row lock waits, pool exhaustion, or slow SQL.
_DB_PHASE_BUCKETS = (0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 180.0)

AUTH_DURATION_SEC = Histogram(
    "loom_svc_auth_duration_sec",
    "Request authentication, including the session touch and its commit",
    labelnames=("auth_kind",),
    buckets=_DB_PHASE_BUCKETS,
)

DB_CHECKOUT_WAIT_SEC = Histogram(
    "loom_svc_db_checkout_wait_sec",
    "Wait for a pooled database connection at request start",
    buckets=_DB_PHASE_BUCKETS,
)

DB_STATEMENT_SEC = Histogram(
    "loom_svc_db_statement_sec",
    "Database statement execution time, including lock waits",
    # verb: select / insert / update / delete / other
    labelnames=("verb",),
    buckets=_DB_PHASE_BUCKETS,
)

INTERACTIVE_READ_TIMEOUTS_TOTAL = Counter(
    "loom_svc_interactive_read_timeouts_total",
    "Browser reads cancelled by the interactive statement timeout",
)

_STATEMENT_VERBS = frozenset({"select", "insert", "update", "delete"})


def instrument_engine(engine: Any) -> None:
    """Record per-statement execution time on an (async) SQLAlchemy engine."""
    sync_engine = getattr(engine, "sync_engine", engine)
    if not isinstance(sync_engine, Engine):
        return

    @event.listens_for(sync_engine, "before_cursor_execute")
    def _start(conn: Any, _cursor: Any, _statement: str, *_args: Any) -> None:
        conn.info.setdefault("_loom_statement_start", []).append(time.perf_counter())

    @event.listens_for(sync_engine, "after_cursor_execute")
    def _finish(conn: Any, _cursor: Any, statement: str, *_args: Any) -> None:
        starts = conn.info.get("_loom_statement_start")
        if not starts:
            return
        verb = statement.lstrip().split(None, 1)[0].lower() if statement.strip() else ""
        DB_STATEMENT_SEC.labels(verb=verb if verb in _STATEMENT_VERBS else "other").observe(
            time.perf_counter() - starts.pop(),
        )
