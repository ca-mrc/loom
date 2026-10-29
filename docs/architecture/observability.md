# Loom Observability

Loom exposes Prometheus metrics from all four in-cluster services. This document
covers the metric naming convention, the five Grafana dashboards, the Prometheus
alert rules, and the on-call triage path for each alert.

## Metric naming convention

All metrics use the `loom_*` prefix and follow these cardinality rules:

- **`team_id` label** only where critical for per-team cost attribution or
  per-team queue depth. Avoid adding it to high-cardinality histograms.
- **Service prefix**: `loom_gateway_*` (LLM Gateway), `loom_svc_*` (Loom Service),
  `loom_worker_*` (Worker), `loom_*` without sub-prefix (Control Plane).
- **Counters** use the `_total` suffix; **histograms** use `_sec` (seconds) with
  `_bucket` / `_count` / `_sum` Prometheus extensions.

### Metric inventory

| Service | Metric | Type | Labels |
|---|---|---|---|
| Control Plane | `loom_trials_state_total` | Counter | `from_state`, `to_state`, `team_id` |
| Control Plane | `loom_trials_inflight` | Gauge | `team_id`, `state` |
| Control Plane | `loom_queue_depth` | Gauge | `team_id` |
| Control Plane | `loom_claim_latency_sec` | Histogram | `result` (hit\|miss) |
| Control Plane | `loom_state_patch_total` | Counter | `endpoint`, `result` (ok\|fenced\|timeout) |
| Control Plane | `loom_workers_active` | Gauge | — |
| Control Plane | `loom_worker_pool_total_slots` | Gauge | `pool_name`, `backend`, `cpu_arch` |
| Control Plane | `loom_worker_pool_occupied_slots` | Gauge | `pool_name`, `backend`, `cpu_arch` |
| Control Plane | `loom_worker_pool_free_slots` | Gauge | `pool_name`, `backend`, `cpu_arch` |
| Control Plane | `loom_worker_pool_workers` | Gauge | `pool_name`, `backend`, `cpu_arch` |
| Control Plane | `loom_worker_pool_draining_slots` | Gauge | `pool_name`, `backend`, `cpu_arch` |
| Control Plane | `loom_worker_pool_draining_workers` | Gauge | `pool_name`, `backend`, `cpu_arch` |
| Control Plane | `loom_worker_reclaim_total` | Counter | — |
| LLM Gateway | `loom_gateway_llm_calls_total` | Counter | `provider`, `dialect`, `result` |
| LLM Gateway | `loom_gateway_llm_call_latency_sec` | Histogram | `provider`, `dialect` |
| LLM Gateway | `loom_gateway_cost_usd_total` | Counter | `team_id`, `provider` |
| Loom Service | `loom_svc_http_requests_total` | Counter | `route`, `method`, `status_class` |
| Loom Service | `loom_svc_http_request_latency_sec` | Histogram | `route`, `method` |
| Loom Service | `loom_svc_batch_runner_ticks_total` | Counter | `result` (ok\|skipped_no_token\|error) |
| Loom Service | `loom_svc_batch_runner_trials_dispatched_total` | Counter | — |
| Loom Service | `loom_svc_tokens_issued_total` | Counter | `token_type` (team\|admin) |
| Loom Service | `loom_svc_tokens_revoked_total` | Counter | — |
| Loom Service | `loom_svc_auth_duration_sec` | Histogram | `auth_kind` (session\|bearer\|readonly_probe) |
| Loom Service | `loom_svc_db_checkout_wait_sec` | Histogram | — |
| Loom Service | `loom_svc_db_statement_sec` | Histogram | `verb` (select\|insert\|update\|delete\|other) |
| Loom Service | `loom_svc_interactive_read_timeouts_total` | Counter | — |
| Worker | `loom_worker_trials_inflight` | Gauge | — |
| Worker | `loom_worker_trials_started_total` | Counter | `backend` |
| Worker | `loom_worker_trials_completed_total` | Counter | `backend`, `result` |
| Worker | `loom_worker_trial_duration_sec` | Histogram | `backend`, `result` |
| Worker | `loom_worker_claim_loop_iterations_total` | Counter | `result` (hit\|miss\|error) |
| Worker | `loom_worker_heartbeat_failures_total` | Counter | — |

## Grafana dashboards

Five dashboards live in `deploy/grafana/dashboards/` and are auto-deployed as
a ConfigMap via `grafana-dashboards.yaml.j2` (rendered by `loom cluster render`).
The ConfigMap carries the `grafana_dashboard: "1"` label so the kube-prometheus-stack
Grafana sidecar auto-discovers and imports them.

| Dashboard | File | UID | Purpose |
|---|---|---|---|
| Operator Overview | `operator-overview.json` | `loom-operator-overview` | Single pane of glass — is anything broken? |
| Control Plane | `control-plane.json` | `loom-control-plane` | CP scheduling deep dive |
| LLM Gateway | `llm-gateway.json` | `loom-llm-gateway` | Provider calls, latency, cost |
| Loom Service | `loom-service.json` | `loom-service` | HTTP, batch runner, tokens |
| Worker Fleet | `worker.json` | `loom-worker` | Trial throughput, duration, failures |

### What each dashboard answers

**Operator Overview** — the first dashboard to open on any alert. Answers:
- Are all four services up? (stat panels per `up{job=~"loom-.+"}`)
- How deep is the queue? (time series, total across teams)
- How many trials are in-flight by state?
- How many workers are active?
- Is the claim latency P95 healthy?
- Is the gateway returning errors?
- Is the service returning 5xx?
- Is trial completion healthy (succeeded vs failed/crashed)?
- What are the top-5 teams by cumulative LLM cost?

**Control Plane** — drill in when `LoomQueueBacklog`, `LoomClaimLatencyP95High`,
`LoomTrialsStuckClaimed`, `LoomWorkerReclaimsSpiking`, or `LoomStatePatchTimeouts` fires:
- State-transition rates (all `to_state` values)
- Queue depth per team (top 10)
- In-flight trials per team × state
- Claim latency P50/P95/P99
- Claim hit/miss ratio
- State PATCH outcomes (ok / fenced / timeout)
- Worker reclaim rate
- Active workers gauge
- Local worker pool total, occupied, free and draining slots by
  pool/backend/CPU architecture
- Nebius execution targets, fresh capacity observations and native Job lifecycle

**LLM Gateway** — drill in when `LoomGatewayProviderErrorRate` fires or cost anomalies appear:
- Call rate by provider (stacked)
- Call rate by result (stacked — non-ok highlighted)
- Latency P50/P95/P99 by provider
- Latency P95 by dialect
- Provider error rate (% non-ok) per provider
- Cost rate (USD/min) by provider
- Cumulative cost by team (top 10)

**Loom Service** — drill in when `LoomServiceHighErrorRate` fires:
- HTTP request rate by status class (2xx/4xx/5xx stacked)
- Request latency P50/P95/P99
- Top 10 routes by request rate
- Top 10 routes by P95 latency
- Batch runner tick rate by result
- Trials dispatched rate
- Tokens issued rate by type

**Worker Fleet** — drill in when `LoomWorkerTrialFailureRateHigh`,
`LoomWorkerHeartbeatFailing`, or `LoomNoWorkersActive` fires:
- In-flight trials per worker instance (stacked)
- Trial throughput by backend (started rate)
- Trial completion by backend + result
- Trial duration P50/P95/P99 by backend
- Failure rate by backend (% failed + crashed)
- Claim loop iterations by result
- Heartbeat failure rate

## Alert rules

Alert rules live in `deploy/k8s/prometheus-rules.yaml` as a `PrometheusRule` CRD.
They require prometheus-operator (or kube-prometheus-stack). Each alert maps to
at least one panel in the dashboards above.

| Alert | Trigger | Severity | Dashboard |
|---|---|---|---|
| `LoomNoWorkersActive` | `loom_workers_active == 0` for 2m | critical | Operator Overview, Worker Fleet |
| `LoomQueueBacklog` | `sum(loom_queue_depth) > 100` for 10m | warning | Operator Overview, Control Plane |
| `LoomTrialsStuckClaimed` | `loom_trials_inflight{state="claimed"} > 5` per team for 15m | warning | Control Plane |
| `LoomWorkerReclaimsSpiking` | `rate(loom_worker_reclaim_total[5m]) > 0.5` for 10m | warning | Control Plane |
| `LoomStatePatchTimeouts` | `rate(loom_state_patch_total{result="timeout"}[5m]) > 0` for 5m | warning | Control Plane |
| `LoomClaimLatencyP95High` | claim P95 > 1s for 15m | warning | Operator Overview, Control Plane |
| `LoomControlPlaneDown` | `up{job=~".*loom-control-plane.*"} == 0` for 5m | critical | Operator Overview |
| `LoomLLMGatewayDown` | `up{job=~".*loom-llm-gateway.*"} == 0` for 5m | critical | Operator Overview |
| `LoomServiceDown` | `up{job=~".*loom-service.*"} == 0` for 5m | critical | Operator Overview |
| `LoomWorkerProcessDown` | `up{job=~".*loom-worker.*"} == 0` for 5m | warning | Operator Overview |
| `LoomGatewayProviderErrorRate` | provider error rate > 5% for 10m | warning | Operator Overview, LLM Gateway |
| `LoomServiceHighErrorRate` | 5xx rate > 2% for 10m | warning | Operator Overview, Loom Service |
| `LoomWorkerHeartbeatFailing` | `rate(loom_worker_heartbeat_failures_total[5m]) > 0` for 10m | warning | Worker Fleet |
| `LoomWorkerTrialFailureRateHigh` | non-succeeded trial rate > 20% for 15m | warning | Operator Overview, Worker Fleet |

## On-call triage path

### Service down (`LoomControlPlaneDown`, `LoomLLMGatewayDown`, `LoomServiceDown`, `LoomWorkerProcessDown`)

1. Open the **Operator Overview** dashboard — check the service-up stat panels.
2. `kubectl get pods -n loom -l app=<component>` — look for CrashLoopBackOff / OOMKilled.
3. `kubectl logs -n loom -l app=<component> --tail=200 --previous`.
4. If the pod is running but the scrape target is down, check the ServiceMonitor / PodMonitor label selectors in `prometheus-rules.yaml`.

### Queue backlog (`LoomQueueBacklog`)

1. Open **Control Plane** dashboard → Queue Depth per Team panel.
2. For hosted execution, inspect the Nebius target's admission decisions,
   capacity observation freshness and Kubernetes Job/Pod state in Monitor.
   See [Nebius service execution](nebius-service-execution.md).
3. For explicit local development, check `loom_workers_active`, worker logs,
   free slots and claim latency. Local worker capacity does not represent
   Nebius hosted capacity.

### Claim latency (`LoomClaimLatencyP95High`)

1. Open **Control Plane** dashboard → Claim Latency P50/P95/P99.
2. High across all quantiles → postgres bottleneck. Verify `trials(state, submitted_at)` index, check `pg_stat_statements` on the claim query.
3. High only at P99 → tail-latency from lock contention; look for lock-wait rows via `pg_locks`.

### State PATCH timeouts (`LoomStatePatchTimeouts`)

1. **Control Plane** dashboard → State PATCH Outcomes.
2. `kubectl exec deploy/loom-control-plane -- pg_isready -h loom-postgres` — check DB connectivity.
3. If DB is responding, check CP pod CPU/memory; consider rolling restart.

### Worker reclaims spiking (`LoomWorkerReclaimsSpiking`)

1. **Worker Fleet** dashboard → Claim Loop Iterations and Heartbeat Failure Rate.
2. `kubectl describe pod -n loom -l app=loom-worker | grep -A2 'OOMKilled\|Last State'`.
3. If OOMKilled, increase worker memory limits in `deploy/k8s/worker.yaml`.

### Gateway provider errors (`LoomGatewayProviderErrorRate`)

1. **LLM Gateway** dashboard → Provider Error Rate, Call Rate by Result.
2. `kubectl logs -n loom -l app=loom-llm-gateway --since=15m | grep -i "upstream\|auth_error"`.
3. If `auth_error`: rotate the provider API key via `loom providers rotate-key <name>`.
4. If `upstream_error`: check the provider's status page; consider disabling the provider connection.

### High trial failure rate (`LoomWorkerTrialFailureRateHigh`)

1. **Worker Fleet** dashboard → Trial Completion Rate by Backend + Result, Failure Rate by Backend.
2. Check if failures correlate with a specific backend (agent runtime).
3. If `crashed`: likely an OOM or agent bug — check trial trajectories in MinIO.
4. If `failed`: verifier is returning fail — may be expected if the benchmark is hard; check if rate spike is correlated with a new agent deploy.

### Service 5xx rate (`LoomServiceHighErrorRate`)

1. **Loom Service** dashboard → HTTP Request Rate by Status Class, Top Routes by P95 Latency.
2. `kubectl logs -n loom -l app=loom-service --since=15m | grep -i "500\|traceback"`.
3. Check if 5xx is on a specific route — if `/api/v1/trials` or `/api/v1/batches`, check CP reachability.
