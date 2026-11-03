"""Prometheus metrics for the LLM Gateway (#81 slice B-2).

Cardinality is bounded by design:
- `provider` is a fixed set (openai, anthropic, google, openai-compatible,
  custom — 5 values).
- `dialect` is a fixed set (openai, anthropic, google, responses,
  gemini — 5 values).
- `result` is a small enum per metric.
- `team_id` is high-cardinality — we attach it ONLY where the
  per-team breakdown is critical (`COST_TOTAL`), never on rates or
  latencies.

Naming: `loom_gateway_*` prefix keeps gateway series distinguishable
from CP / service / worker series in shared dashboards.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

LLM_CALLS_TOTAL = Counter(
    "loom_gateway_llm_calls_total",
    "LLM calls forwarded through the gateway",
    # provider: the connection's provider_type (openai, anthropic, ...)
    # dialect: which facade routed the call (openai, anthropic, ...,
    #          may differ from provider for openai-compatible)
    # result: ok / upstream_error / ssrf_blocked / auth_error /
    #         rate_limited / timeout
    labelnames=("provider", "dialect", "result"),
)

LLM_CALL_LATENCY_SEC = Histogram(
    "loom_gateway_llm_call_latency_sec",
    "Wall-clock latency of LLM calls forwarded through the gateway",
    labelnames=("provider", "dialect"),
    # Standard prometheus-client default buckets are tuned for sub-second
    # web requests; LLM calls span 100ms (cache hit) → 60s (long
    # completion). Explicit buckets surface the long tail.
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0),
)

COST_USD_TOTAL = Counter(
    "loom_gateway_cost_usd_total",
    "Cumulative cost of LLM calls in USD, by team + provider",
    # team_id is high-cardinality but bounded by team count
    # (typically dozens, not thousands). Acceptable for cost
    # attribution dashboards.
    labelnames=("team_id", "provider"),
)

PROVIDER_VALIDATION_TOTAL = Counter(
    "loom_gateway_provider_validation_total",
    "Provider connection test results (POST /provider-connections/{id}/test)",
    # result: valid / invalid / unreachable
    labelnames=("provider", "result"),
)

# ─── #298 Slice A: transient-failure retry ──────────────────────────

RETRY_ATTEMPTS = Histogram(
    "loom_gateway_llm_retry_attempts",
    "Number of attempts taken to satisfy one upstream LLM request "
    "(1 = first attempt succeeded, no retries).",
    labelnames=("dialect",),
    buckets=(1, 2, 3, 4, 5, 10),
)

RETRY_TOTAL = Counter(
    "loom_gateway_llm_retry_total",
    "Outcomes of the gateway retry loop.",
    # outcome: success | exhausted | budget_exceeded
    # status:  the HTTP status of the LAST attempt (e.g. 503 for
    #          exhausted; 200 for success when at least one earlier
    #          attempt 5xx'd; 0 for transport-level errors).
    labelnames=("dialect", "outcome", "status"),
)

RETRY_AMBIGUOUS_504_TOTAL = Counter(
    "loom_gateway_llm_retry_ambiguous_504_total",
    "Times we retried a 504 from upstream. Counts the potentially-"
    "duplicate-completion case from #298 §D-idempotency so operators "
    "can spot if the risk is materializing.",
    labelnames=("dialect",),
)

# Authentication and signed attempt lifecycle.  Both series are deliberately
# free of credential, subject, and request identifiers.
AUTH_REJECTIONS_TOTAL = Counter(
    "loom_gateway_auth_rejections_total",
    "Gateway bearer requests rejected before provider dispatch.",
    # missing | malformed | invalid_signature | expired | missing_scope
    labelnames=("reason",),
)

ATTEMPT_DEADLINE_REACHED_TOTAL = Counter(
    "loom_gateway_attempt_deadline_reached_total",
    "Gateway requests stopped by their signed attempt deadline.",
)

LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL = Counter(
    "loom_gateway_legacy_attempt_deadline_token_total",
    "Gateway tokens without a signed attempt deadline by compatibility outcome.",
    # accepted | rejected
    labelnames=("outcome",),
)

CONNECTION_QUEUE_WAIT_SECONDS = Histogram(
    "loom_gateway_connection_queue_wait_seconds",
    "Time a gateway request waited for a free in-flight slot on a provider connection.",
    buckets=(0.25, 1.0, 2.0, 5.0, 15.0, 30.0, 60.0, 120.0),
)

CONNECTION_QUEUE_LIMITED_TOTAL = Counter(
    "loom_gateway_connection_queue_limited_total",
    "Provider-connection in-flight queue outcomes.",
    # admitted_after_wait | queue_timeout | deadline
    labelnames=("outcome",),
)

DISPATCH_DATABASE_SECONDS = Histogram(
    "loom_gateway_dispatch_database_seconds",
    "Dispatch audit database awaits, including connection acquisition.",
    # Fixed operation/phase/outcome enums; no request, connection or Trial IDs.
    labelnames=("operation", "phase", "outcome"),
    buckets=(0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0),
)
