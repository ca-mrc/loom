"""Prometheus metric definitions for Control Plane (spec §7.3).

Cardinality is bounded by design — `team_id` is the only high-cardinality
label and is only attached where strictly necessary.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

TRIALS_STATE_TOTAL = Counter(
    "loom_trials_state_total",
    "Trial state transitions",
    labelnames=("from_state", "to_state", "team_id"),
)

TRIALS_INFLIGHT = Gauge(
    "loom_trials_inflight",
    "Trials in claimed/running",
    labelnames=("team_id", "state"),
)

QUEUE_DEPTH = Gauge(
    "loom_queue_depth",
    "Queued trials per team",
    labelnames=("team_id",),
)

CLAIM_LATENCY_SEC = Histogram(
    "loom_claim_latency_sec",
    "Time for POST /trials/claim",
    labelnames=("result",),  # 'hit' | 'miss'
)

STATE_PATCH_TOTAL = Counter(
    "loom_state_patch_total",
    "Trial state PATCH outcomes",
    # endpoint: 'state' / 'trajectory' / ...; result: 'ok' / 'fenced' / 'timeout'
    labelnames=("endpoint", "result"),
)

WORKERS_ACTIVE = Gauge(
    "loom_workers_active",
    "Workers with fresh heartbeat",
)

WORKER_POOL_TOTAL_SLOTS = Gauge(
    "loom_worker_pool_total_slots",
    "Fresh active worker execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_OCCUPIED_SLOTS = Gauge(
    "loom_worker_pool_occupied_slots",
    "Claimed and running trials assigned to fresh workers by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_FREE_SLOTS = Gauge(
    "loom_worker_pool_free_slots",
    "Unoccupied fresh active worker execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_WORKERS = Gauge(
    "loom_worker_pool_workers",
    "Fresh active worker processes by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_DRAINING_SLOTS = Gauge(
    "loom_worker_pool_draining_slots",
    "Draining worker execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_DRAINING_WORKERS = Gauge(
    "loom_worker_pool_draining_workers",
    "Draining worker processes by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_RECLAIM_TOTAL = Counter(
    "loom_worker_reclaim_total",
    "Trials reclaimed by crash detector",
)

SERVICE_EXECUTION_COMMAND_BACKLOG = Gauge(
    "loom_service_execution_command_backlog",
    "Pending or leased durable service execution commands",
)
SERVICE_EXECUTION_RECONCILE_LAG_SECONDS = Gauge(
    "loom_service_execution_reconcile_lag_seconds",
    "Age of the oldest unacknowledged service execution command",
)
SERVICE_EXECUTION_CLEANUP_DEBT = Gauge(
    "loom_service_execution_cleanup_debt",
    "Execution leases with pending, in-progress, or blocked cleanup",
)
SERVICE_EXECUTION_STALE_GENERATIONS_TOTAL = Counter(
    "loom_service_execution_stale_generations_total",
    "Rejected writes from stale or revoked execution generations",
    labelnames=("surface",),
)
SERVICE_EXECUTION_DUPLICATE_DELIVERIES_TOTAL = Counter(
    "loom_service_execution_duplicate_deliveries_total",
    "Idempotently converged command or event redeliveries",
    labelnames=("command_type",),
)
SERVICE_EXECUTION_MATERIALIZATION_BACKLOG = Gauge(
    "loom_service_execution_materialization_backlog",
    "Committed service execution bundles awaiting canonical materialization",
)
EXECUTION_CAPACITY_LOCK_WAIT_SECONDS = Histogram(
    "loom_execution_capacity_lock_wait_seconds",
    "Wait for the shared provisioning lock, excluding admission work after acquisition.",
    labelnames=("operation",), buckets=(.01, .05, .1, .5, 1, 2, 5, 10, 20, 60),
)
SERVICE_EXECUTION_RESERVATION_SECONDS = Histogram(
    "loom_service_execution_reservation_seconds",
    "One local scheduler reservation transaction including compilation and commit.",
    labelnames=("role",), buckets=(.01, .05, .1, .5, 1, 2, 5, 10, 20, 60),
)
SERVICE_EXECUTION_MATERIALIZATION_PENDING_BYTES = Gauge(
    "loom_service_execution_materialization_pending_bytes",
    "Source bundle bytes awaiting canonical materialization",
)
SERVICE_EXECUTION_MATERIALIZATION_OLDEST_AGE_SECONDS = Gauge(
    "loom_service_execution_materialization_oldest_age_seconds",
    "Age of the oldest committed bundle awaiting canonical materialization",
)
SERVICE_EXECUTION_MATERIALIZATION_RETRIES_TOTAL = Counter(
    "loom_service_execution_materialization_retries_total",
    "Transient canonical materialization attempts returned to the durable queue",
)
SERVICE_EXECUTION_MATERIALIZATION_FAILURES_TOTAL = Counter(
    "loom_service_execution_materialization_failures_total",
    "Permanent canonical materialization failures",
    labelnames=("reason",),
)
SERVICE_EXECUTION_MATERIALIZATION_UNAVAILABLE = Gauge(
    "loom_service_execution_materialization_unavailable",
    "Persisted service execution bundles that cannot be materialized",
)
SERVICE_EXECUTION_MATERIALIZATION_UNAVAILABLE_BYTES = Gauge(
    "loom_service_execution_materialization_unavailable_bytes",
    "Source bytes retained for unavailable service execution bundles",
)
SERVICE_EXECUTION_MATERIALIZATION_COMPLETED_TOTAL = Counter(
    "loom_service_execution_materialization_completed_total",
    "Service execution bundles committed to canonical Loom storage",
)
SERVICE_EXECUTION_SOURCE_SPOOL_RETAINED = Gauge(
    "loom_service_execution_source_spool_retained",
    "Canonical-acknowledged source bundles retained for recovery or cleanup retry",
)
SERVICE_EXECUTION_SOURCE_SPOOL_BYTES = Gauge(
    "loom_service_execution_source_spool_bytes",
    "Bytes in canonical-acknowledged source bundles retained for recovery",
)
SERVICE_EXECUTION_SOURCE_CLEANUP_RETRIES_TOTAL = Counter(
    "loom_service_execution_source_cleanup_retries_total",
    "Source spool cleanup attempts returned to the durable retention queue",
)
SERVICE_EXECUTION_SOURCE_CLEANUP_COMPLETED_TOTAL = Counter(
    "loom_service_execution_source_cleanup_completed_total",
    "Source spool bundles removed after their recovery window",
)

RETRY_EXHAUSTED_TOTAL = Counter(
    "loom_retry_exhausted_total",
    "Trials transitioned to failed because attempt_count >= max_attempts",
)

PIPELINE_RUNS = Gauge(
    "loom_pipeline_runs",
    "Current PipelineRuns by closed lifecycle state/result",
    labelnames=("state", "result_status"),
)
PIPELINE_STAGE_RUNS = Gauge(
    "loom_pipeline_stage_runs", "Current Pipeline StageRuns", labelnames=("state", "resource_class")
)
PIPELINE_STAGE_DURATION_SECONDS = Histogram(
    "loom_pipeline_stage_duration_seconds",
    "Committed Pipeline StageRun duration",
    labelnames=("resource_class", "result"),
    buckets=(
        1,
        5,
        15,
        30,
        60,
        120,
        300,
        600,
        1800,
        3600,
        7200,
        14400,
        28800,
        86400,
        172800,
        345600,
        864000,
    ),
)
EXECUTION_ATTEMPTS = Gauge(
    "loom_execution_attempts",
    "Current Pipeline execution Attempts",
    labelnames=("state", "resource_class"),
)
PIPELINE_ARTIFACT_BYTES_TOTAL = Counter(
    "loom_pipeline_artifact_bytes_total",
    "Newly committed Pipeline Artifact bytes charged to the run budget",
    labelnames=("artifact_class",),
)
PIPELINE_CANCEL_LATENCY_SECONDS = Histogram(
    "loom_pipeline_cancel_latency_seconds",
    "Pipeline cancellation acknowledgement latency",
    labelnames=("outcome",),
    buckets=(1, 2, 5, 10, 30, 60, 120, 300, 600),
)
PIPELINE_CONTROLLER_RECONCILE_ERRORS_TOTAL = Counter(
    "loom_pipeline_controller_reconcile_errors_total",
    "Exhausted Pipeline reconcile failures",
    labelnames=("reason",),
)

PIPELINE_LIVE_PREVIEW_FRAMES_TOTAL = Counter(
    "loom_pipeline_live_preview_frames_total",
    "Bounded Stage 1 preview frame publish outcomes",
    labelnames=("result", "reason"),
)
PIPELINE_LIVE_PREVIEW_BYTES_TOTAL = Counter(
    "loom_pipeline_live_preview_bytes_total",
    "Accepted Stage 1 preview JPEG bytes",
)
PIPELINE_LIVE_PREVIEW_ACTIVE_GENERATIONS = Gauge(
    "loom_pipeline_live_preview_active_generations",
    "Active ephemeral Stage 1 preview generations",
)
PIPELINE_LIVE_PREVIEW_LAST_FRAME_AGE_SECONDS = Gauge(
    "loom_pipeline_live_preview_last_frame_age_seconds",
    "Age of the oldest latest frame among active Stage 1 previews",
)
PIPELINE_LIVE_PREVIEW_PURGES_TOTAL = Counter(
    "loom_pipeline_live_preview_purges_total",
    "Ephemeral Stage 1 preview purge outcomes",
    labelnames=("reason",),
)
PIPELINE_STAGE_QUEUE_AGE_SECONDS = Gauge(
    "loom_pipeline_stage_queue_age_seconds",
    "Maximum Pipeline StageRun queue age",
    labelnames=("state", "resource_class"),
)
PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS = Gauge(
    "loom_pipeline_stage_deadline_overrun_seconds",
    "Maximum Pipeline Attempt deadline overrun",
    labelnames=("resource_class",),
)
PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS = Gauge(
    "loom_pipeline_checkpoint_oldest_age_seconds",
    "Oldest active Pipeline checkpoint age",
    labelnames=("resource_class",),
)
PIPELINE_ARTIFACT_COMMIT_FAILURES_TOTAL = Counter(
    "loom_pipeline_artifact_commit_failures_total",
    "Returned Pipeline Artifact commit failures",
    labelnames=("commit_kind", "reason"),
)

# Live worker tokens flagged by the staleness audit. Labels:
#   reason="unused_30d"  — last_seen_at (or issued_at, if never used)
#                          is older than 30 days
#   reason="aged_90d"    — issued_at is older than 90 days (rotation
#                          overdue per SOC2-equivalent service-credential
#                          cadence)
# Counts only non-revoked, non-expired worker-type tokens. An operator-
# facing alert fires when either count is > 0 for 1 hour; the metric is
# a SOFT signal (no auto-revocation — revoking a live worker pool's
# token 401s in-flight claims).
WORKER_TOKENS_STALE_COUNT = Gauge(
    "loom_worker_tokens_stale_count",
    "Live worker tokens flagged by the staleness audit",
    labelnames=("reason",),
)
