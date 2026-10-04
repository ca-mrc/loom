# Service mode

Service mode provides a team evaluation platform: a FastAPI Control Plane
owns trial state, native Nebius controllers schedule and reconcile hosted
execution, an LLM Gateway centralizes provider calls and cost attribution, and
REST `loom_service` plus the React SPA provide user access. The worker polling
flow described below applies to explicit local/disposable service execution.
Team and worker auth still use database-backed bearer tokens, while admin auth
uses the file-backed singleton secret described in
[auth-and-teams.md](auth-and-teams.md).

Postgres and object storage hold durable state; the local Compose stack uses
MinIO for object storage.

Hosted service execution uses only the Nebius Kubernetes contract in
[Nebius service execution](nebius-service-execution.md). Worker polling and
Docker execution are local-development facilities. Compose explicitly enables
`LOOM_LOCAL_EXECUTION=1` in the `development` environment; hosted services do
not expose worker registration or claim routes.

Batch creation, failed-case reruns and Run Library clone/reuse share execution
admission and task-resource freezing in `loom_service.execution_admission`.
Routes retain authorization, attribution and transaction ownership, including
the atomic batch-plus-audit write for admin submissions. New submissions and
clone/reuse resolve current deployment policy; ordinary reruns retain their
stored runtime snapshot, with current-runtime selection available only through
the explicit rerun option. The admission module owns shared rejection metrics
and errors and does not depend on route modules.

Local service-mode development uses `docker compose`, so operators need Docker
CLI with the Compose plugin before running
`loom service up`, `down`, or `status`. On macOS, install
and start Docker Desktop first and confirm
`docker compose version` succeeds.

`loom service up`, `down`, and `status` manage local Compose only. Personal
shared-fleet environments and their deployment API have been retired.
Hosted platform deployments use the Nebius deployment configuration and
existing release controls.

## Service-prefix convention

| Service | Routes mounted at | Audience |
|---|---|---|
| `loom_service` | `/api/v1/*` | External (SPA, `curl`, customer scripts) |
| Control Plane | root (`/trials`, `/workers/...`, `/admin/worker-tokens`, etc.) | Workers + `loom_service` (cluster-internal only) |
| LLM Gateway | `/v1/*` (Loom-attributed dialects), `/openai/v1/*` (provider facade for stock SDKs), `/v1beta/*` (Gemini dialect), `/admin/*` | Agents from inside sandboxes |

Layering: SPA / external curl → `loom_service` (`/api/v1/*`) → CP
over cluster DNS. `deploy/k8s/ingress.yaml` exposes only
`loom_service` under `/api/v1` and `loom-web` at `/` on the configured
public host. The Control Plane and LLM Gateway are reachable only inside the
cluster; sandbox traffic reaches the Gateway through the worker's
subprocess gateway URL, singleton/gateway-router path, or another
sandbox-facing internal route, not public Ingress. For operator-side admin curls,
port-forward CP:
`kubectl port-forward deploy/loom-control-plane 8080:8080`.

The SPA's launch model picker is BYO-provider aware. It reads
`GET /api/v1/provider-connections` for the connection list and
`GET /api/v1/models` for the default, classifier-filtered model catalog.
`GET /api/v1/models?view=raw` preserves noisy provider entries with
`hidden_reason` metadata for debugging. Manual model ids are stored with
`POST /api/v1/provider-connections/{id}/models` and are submitted on
Trial/Batch payloads as `provider_connection_id` + `provider_model_id`.
`GET /api/v1/agents` also carries service-mode readiness metadata:
`service_mode_ready`, `readiness_message`, and a `runtime_contract`
covering sandbox executables/modules, provider dialect, env vars, and
capture mode. The SPA disables unavailable agents with a setup-needed
message, and service submit routes enforce the same readiness check so
clients cannot create doomed batches by bypassing the browser.
Staging and release restore drills materialize this same service catalog
into the `agents` table through `loom datasets provision-catalog`.
Those rows are an auditable restore snapshot with runtime contracts,
compatibility metadata, and provisioner provenance; they are not maintained by
manual SQL.

`loom qa matrix --compatibility-plan` turns the same service-mode agent
metadata into a login-free provider compatibility matrix. It emits one cell per
repo-known displayed `service_mode_ready=true` agent and provider endpoint type,
including generic `supported_providers=["*"]` agents as per-agent rows. Each
cell records whether the combination is repo-supported, skipped
because the harness takes no model, or blocked before submit because the
agent's `supported_providers` do not include the endpoint provider family. Live
provider-smoke evidence is optional and merged from sanitized JSON; absent live
evidence leaves usage, diagnostics, and other runtime dimensions as
`pending_live_smoke` rather than treating the cell as validated.

Agent/task compatibility is bidirectional. Catalog entries expose
`requires_capabilities` for task features the runtime depends on and
`provides_capabilities` for execution surfaces it supplies. Tasks declare
`required_agent_capabilities` in their version-1 config. Batch and single-trial
submission reject any pair with a missing capability before persistence or
fan-out. `workspace_exec` is the first task execution requirement:
workspace-oriented agents provide it, while `direct-completion` does not.
The API exposes `aliases=["litellm"]` on the canonical completion entry for
backward-compatible clients without displaying a duplicate agent.

Model-backed `direct-completion` submissions (including the deprecated
`litellm` alias) may include
`trial_config.request_params` for safe request controls such as
`temperature`, `top_p`, `seed`, max output limits, reasoning effort,
tool-choice mode, and provider decoding extras. The Control Plane
validates the trial config, the worker strips payload/secret-bearing
keys before the call, and the LLM Gateway persists the effective
non-sensitive controls in `llm_calls.request_params` for trial/batch
debug evidence. This field is not a generic subprocess-agent setting
channel; external CLI adapters must expose their own runtime-compatible
settings path.

Use `loom agents audit-runtime --image <trial-sandbox-image>` to verify
that the resolved trial image contains the executables and Python modules
declared by the agent catalog. For tasks with `environment.docker_image`,
that is the configured image. For tasks with `environment.dockerfile`, the
worker builds a deterministic `loom-task:<hash>` image from the materialized
task bundle before the first trial and reuses that local image on later
trials with the same task checksum, Dockerfile path, and optional
`environment.docker_build_context`. Build-only contexts should live under
`.loom-build/`; workspace materialization skips that directory so hidden build
assets are not copied into the agent-visible workdir. Before a cache-miss
build, the worker rejects contexts above `LOOM_TASK_IMAGE_BUILD_MAX_FILES`
(default 2000) or `LOOM_TASK_IMAGE_BUILD_MAX_BYTES` (default 536870912), and
`build_timeout_sec` bounds the Docker build call. Worker-created docker-py
clients also use `LOOM_WORKER_DOCKER_API_TIMEOUT_SEC`, which must be at least
as large as the largest expected pull/build/sidecar budget so the SDK does not
time out before the task's own timeout policy. Tasks may also declare
`environment.sidecars`; Docker-backed workers start those auxiliary containers
on the same per-trial network, wait for declared healthchecks through the final
Docker probe's timeout window, pass `environment.environment` into the primary
sandbox, and clean the sidecars up after the trial. This checks the trial
sandbox boundary, not the `loom-worker` container. A clean runtime audit is
necessary before flipping an external adapter to
`service_mode_ready=true`; it still must be followed by a live end-to-end
smoke that proves the adapter can finish a platform trial. Use
`loom agents smoke-runtime --image <trial-sandbox-image>` for that second
gate. It runs each displayed agent through `LocalTrialRunner`, a real
Docker sandbox, and a deterministic provider stub, then reports whether the
platform trial reached `succeeded`.

The repo ships a candidate agent-capable sandbox image for operator smoke
work:

```bash
docker build -f deploy/Dockerfile.agent-sandbox -t loom-agent-sandbox:dev .
loom agents audit-runtime --image loom-agent-sandbox:dev --json
loom agents smoke-runtime --image loom-agent-sandbox:dev --json
```

`deploy/Dockerfile.agent-sandbox` uses Python 3.12 plus Node 22 because
the displayed catalog spans both Python-module agents and modern Node
CLIs. Python CLI-only agents such as `aider` and `mini-swe-agent` are
installed in isolated virtual environments and linked onto `PATH` so
their pinned dependencies do not conflict with OpenHands. `openhands`
and `openhands-sdk` both use Loom's
`loom_launcher.openhands_sdk_runner` module because upstream OpenHands SDK
exposes a Python library rather than a stable one-shot CLI;
`openhands.server` is not a usable non-interactive runner. A
dynamic OpenHands install on top of an arbitrary benchmark task image uses a
pinned `uv` installer to create `/opt/loom-agents/openhands-sdk` with Python
3.12, installs `loom-launcher` from a pinned repository subdirectory ref, then
invokes that venv's interpreter. This fixes Python-version drift in task images
without baking the selected provider or model into the image; model choice is
still passed at trial runtime through the adapter invocation and gateway
environment. A successful image build or dependency audit does not by itself
make an agent ready. The displayed catalog is ready only when the selected
trial sandbox image satisfies the declared runtime dependencies and the
platform trial smoke has passed; an audit of a thinner image can still report
`blocked`.

## Process model

```
                        ┌──────────────────────────────────────┐
                        │     loom_service (REST + SPA)        │
                        │  /api/v1/{trials, batches,         │
                        │     benchmarks, tasks, tokens,       │
                        │     usage, rate-cards, teams,        │
                        │     team registrations,              │
                        │     admin audit events,              │
                        │     trajectory, atif, health}        │
                        └────┬──────────────────┬──────────────┘
                             │                  │
                             ▼                  ▼
        ┌────────────────────────┐    ┌─────────────────────┐
        │   Control Plane        │    │   LLM Gateway       │
        │   POST /trials         │    │   /v1/chat/         │
        │   POST /trials/claim   │    │       completions   │
        │   PATCH /trials/{id}/  │◄───┤   /v1/messages      │
        │         state          │    │   /v1/responses     │
        │   POST /trials/{id}/   │    │   /v1beta/models/   │
        │        cancel          │    │       {model_path}  │
        │   POST /workers/...    │    │   /admin/rate-cards │
        │   ...                  │    │   (LiteLLM-backed)  │
        └────┬───────────────────┘    └─────────┬───────────┘
             │                                  │
             │       ┌──────────────────────────┘
             │       │
             ▼       ▼
        ┌────────────────────┐    ┌──────────────┐
        │   Worker (×N)      │    │   MinIO      │
        │   claim → run →    │───►│  (S3-compat) │
        │   PATCH state →    │    │              │
        │   finalize         │    │  trajectories│
        │                    │    │  + atif      │
        └────┬───────────────┘    └──────────────┘
             │
             ▼
        ┌──────────────┐
        │   Postgres   │  ← all state lives here
        │              │     (trials, llm_calls,
        │              │      cloud_compute_records,
        │              │      teams, tokens, ...)
        └──────────────┘
```

Stateless services (Control Plane, Gateway, Worker, loom_service) can
all scale horizontally. Postgres is the durability boundary.

## Trial lifecycle (one happy path)

```
   Researcher          Service              Control Plane         Worker                    Sandbox
   ----------          -------              -------------         ------                    -------
        |                  |                      |                  |                          |
   POST trials -----------> POST /trials --------> INSERT trial      |                          |
        | (via SPA            (forwarder)          (state=queued)    |                          |
        |  or curl)            |                      |              |                          |
        |                      |                     [trial visible  |                          |
        |                      |                      in DRF queue]  |                          |
        |                      |                      |              |                          |
        |                      |                      | <-- POST /trials/claim --|              |
        |                      |                      | DRF SQL (CTE + UPDATE):                 |
        |                      |                      | SELECT ... FOR UPDATE OF t SKIP LOCKED  |
        |                      |                      | UPDATE trials SET state='claimed',      |
        |                      |                      |   worker_id=:wid, claimed_at=NOW(),     |
        |                      |                      |   attempt_count=attempt_count+1         |
        |                      |                      | ----------- claim row ----> spawn Trial.run()
        |                      |                      |                                         |
        |                      |                      |              | --- start --> [container/cloud-sandbox]
        |                      |                      |              | upload bundle           |
        |                      |                      |              | set baseline policy     |
        |                      |                      |              | (still state=claimed)   |
        |                      |                      |              |                          |
        |                      |                      | <-- PATCH -- | state=running           |
        |                      |                      |   (fenced)   |                          |
        |                      |                      |              | exec_streaming(agent argv)
        |                      |                      |              | -----------------------> |
        |                      |                      |              | <<-- events stream --    |
        |                      |                      |              |     append to local      |
        |                      |                      |              |     trajectory.jsonl     |
        |                      |                      |              |     (1 MB / 100 ev /     |
        |                      |                      |              |      10 s flush to MinIO |
        |                      |                      |              |      multipart, ≥ 5 MiB  |
        |                      |                      |              |      mid-trial parts)    |
        |                      |                      |              |                          |
        |                      |                      |              | LLM calls --HTTP-->   ┌─────────┐
        |                      |                      |              |                       │ Gateway │
        |                      |                      |              | <-- resp + cost ----- └─────────┘
        |                      |                      |              |   (writes llm_calls    (also writes
        |                      |                      |              |    row before resp)    to Postgres)
        |                      |                      |              |                          |
        |                      |                      |              | verifier.run()           |
        |                      |                      |              | finalize (always runs):  |
        |                      |                      |              |  1. last trajectory part |
        |                      |                      |              |  2. maybe fetch llm_calls|
        |                      |                      |              |     (CP HTTP; skipped if |
        |                      |                      |              |      agent already wrote |
        |                      |                      |              |      gateway llm events) |
        |                      |                      |              |  3. project_to_atif      |
        |                      |                      |              |  4. upload atif.json     |
        |                      |                      |              | PATCH /trajectory_index  |
        |                      |                      |              |   (result + output index)|
        |                      |                      | <-- PATCH -- | state=succeeded|failed   |
        |                      |                      |   (fenced)   |                          |
        |                      |                      |              | --- stop --> [delete sandbox]
        |                      |                      |              |                          |
   GET /trials/{id} ----------> GET /api/v1/         | <-- SELECT --|                          |
                                  trials/{id}                                                   |
        | <-- result + atif URL ----- (forwarder) -- |              |                          |
```

The shared `TrialState` contract in `src/loom/models/result.py` has six
states (`queued → claimed → running → succeeded | failed |
cancelled`). Finalize is a side-effect of reaching a terminal state,
not a state of its own.

## Fencing

Every Worker → CP state PATCH carries `(state, worker_id,
failure_reason?)`, and a `state=succeeded` PATCH must either include
`result` or target a trial whose result was already persisted by the
fenced trajectory projection endpoint. If a Worker reports
`state=succeeded` while the row still has `result IS NULL`, the CP
returns 400 before the database invariant can surface as an internal
error. The CP's `PATCH /trials/{id}/state` UPDATE matches on both
`id = :trial_id` AND `worker_id = :worker_id`; if a different Worker
has taken the trial over (heartbeat timeout → crash detector reassigned
ownership), the UPDATE matches zero rows and the CP returns 409. Two
Workers can never both think they own a trial.

`loom_worker.HttpControlPlaneClient` translates `409 Conflict` →
`False` return (the Worker logs + abandons the trial). The trial
stays in whatever state the new owner has put it in.

For local workers whose heartbeat expires, the crash detector clears the dead
owner and applies the existing 30-second retry backoff. It preserves an
actionable `worker_lost_claim` diagnostic for claimed/running trials and
historical rows that are queued despite having `started_at`. The diagnostic
identifies the expired worker, previous state and start time; the existing
pre-start and capacity-pressure messages retain their meanings. If the team
attempt ceiling is exhausted, the normal sweeper fails the trial while keeping
that diagnostic visible in user detail/list responses. A current native
execution-attempt lease excludes the trial from local-worker recovery, even
if a stale worker pointer remains.

Local startup orphan cleanup removes terminal/unknown trial containers only
within the worker's sandbox identity. Fresh nonterminal containers and other
sandbox identities are preserved; the existing age fallback bounds abandoned
nonterminal resources. These are local-worker contracts, separate from native
execution-controller cleanup. An injected SIGKILL verifies recovery but does
not establish an OOM diagnosis.

Worker-side cancellation preserves its source in the trial lifecycle. A
Control Plane or operator cancellation remains a `cancelled` trial. A worker
watchdog hard-deadline cancellation, or the Control Plane stale-running reclaim
for a still-heartbeating but silent worker, writes a terminal `failed` trial
with `failure_reason=agent_timeout` and a diagnostic message containing the
runtime, configured timeout, last event/LLM activity, and worker heartbeat
freshness. This keeps local runtime hangs distinct from user cancellation.

After a successful trial finalizes, the Worker sends a fenced
`PATCH /trials/{id}/trajectory_index` before reporting terminal
`state=succeeded`. This ordering keeps Control Plane state atomic from
the user's perspective: a trial should not persist as `succeeded`
without the result projection needed for reward gates and detail APIs.
If that projection is rejected or fails, the Worker reports
`trajectory_flush_failed` instead of success so the run remains
diagnosable and rerunnable.

The projection carries two durable outputs:

- `trials.result`: the serialized `TrialResult` plus an
  `aggregate_reward` scalar for list/batch rollups. Trial and batch
  read APIs aggregate `llm_calls` separately for
  `total_prompt_tokens`, `total_completion_tokens`, and
  `llm_calls_count`. Model-backed terminal trials also project
  `llm_evidence_status`; `no_calls_invalid` means the run cannot be used as
  benchmark evidence because the selected model path did not persist any
  gateway calls. Trial detail includes `no_call_reason`,
  `no_call_message`, and `no_call_retryable`; batch detail includes
  `no_call_trial_count`, `no_call_reason_counts`, and promotes a zero-call
  model-backed batch to diagnosis reason `batch.no_llm_calls`.
- `trials.trajectory_index`: trajectory URI, ATIF URI/schema version,
  and the actual uploaded artifact object keys and sizes.

The service detail API uses that projection to set `trajectory_ready`,
`atif_ready`, and artifact download links. Clients should not infer
output availability by guessing MinIO keys. An explicit ATIF URI marks it
ready; the legacy timestamp fallback requires a terminal trial that both
started and finished. A build failure or cancellation before execution does
not advertise an ATIF download merely because it has a completion timestamp.
These flags are projections, not live object-store availability checks.

The same detail API also exposes user-facing diagnosis and debug evidence:

- `GET /api/v1/trials/{trial_id}` includes `debug_evidence`;
  `GET /api/v1/trials/{trial_id}/debug` returns that object directly.
  Trial debug evidence includes per-call `request_params` summaries
  and `request_params_status_counts` so score-alignment audits can
  distinguish matched generation settings from legacy/unavailable
  request-parameter evidence. Provider summaries also include
  `call_status_counts`, `failed_llm_calls_count`, and
  `failure_category_counts`, so a terminal model-backed trial with an
  upstream provider failure is distinguishable from a trial where no
  provider request was attempted. For Codex, `codex_high_demand_no_call`
  marks a subprocess high-demand/runtime exit before the Gateway saw a
  request; request-parameter audits must exclude that trial unless a retry
  records `calls_observed`.
- `GET /api/v1/batches/{batch_id}` is lightweight by default for large
  batches and omits `debug_evidence`; pass `include_debug=true` or call
  `GET /api/v1/batches/{batch_id}/debug` to load that object. Batch debug
  evidence carries the same provider summary fields across its child calls,
  normalized failure classification summaries, and a failure ledger built from
  bounded trial projections instead of full `trajectory_index` rows.
- `GET /api/v1/batches/{batch_id}/rerun-plan` returns the deterministic
  supplemental rerun plan for the batch family. It accepts repeated `task_id`
  query parameters and separates coordinates into `auto_safe`,
  `operator_approval`, `not_rerunnable`, and `already_covered` buckets. Batch
  detail also includes `rerun_plan` and `final_trial_selection` so API clients
  can preserve main/supplemental lineage without loading debug evidence.
- Run Library batch detail stays lightweight on the default path: it uses
  bounded trial projections plus a capped typed-artifact preview and does not
  select full trial `trajectory_index` payloads or enumerate the complete typed
  artifact inventory. It loads the same batch `debug_evidence` on demand after
  the Run Library read policy allows access.
- `GET /api/v1/trials/{trial_id}` includes `diagnosis`;
  `GET /api/v1/batches/{batch_id}` includes batch `diagnosis` only when
  `include_debug=true`.
  `GET /api/v1/trials/{trial_id}/diagnosis` and
  `GET /api/v1/batches/{batch_id}/diagnosis` return that deterministic
  `DiagnosisReport` directly.
- Run Library batch detail includes the same batch `diagnosis` after the Run
  Library read policy allows access.

Diagnosis reports are schema-versioned and human-readable. They are derived
from redacted debug evidence and persisted run state, not from LLM
summarization. The stable fields are `entity`, `summary`, `primary_cause`,
`impact`, `evidence`, `next_actions`, and `reason_clusters`. Batch diagnosis
clusters failed child trials by stable reason code, reports affected counts
and ratios, and states whether the aggregate score is reliable for
model-quality comparison.

Debug evidence is schema-versioned and machine-readable. The stable fields are
`entity`, `lifecycle`, `worker`, `agent`, `provider`, `activity`,
`stale_running`, `failure`, `task` or `task_selection`, `reward`,
`evidence_refs`, and `next_actions`. The
`failure.reason_code` is stable for API/CLI agents and uses prefixes such as
`trial.verifier_error` or `batch.fanout_submit_failed`. Trial failure objects
also include normalized `failure_class`, `root_cause`, `platform_outcome`,
`score_outcome`, `rerun_recommendation`, and booleans describing whether the
coordinate is rerunnable, needs operator approval, or requires task changes.
A successful trial with numeric reward `0` is classified as platform success
and score failure, not platform failure. Trial debug evidence includes
`activity.last_trial_event`, `activity.last_llm_call_at`,
`worker.heartbeat_age_sec`, `agent.timeout.agent_timeout_sec`, and the
stale-running keep/reclaim decision. Batch debug evidence lists up to 50
stale-running candidates. The service redacts bearer tokens, provider keys,
secret refs, internal service URLs, and signed object-store URLs before
returning either the detail response or the direct debug endpoint.

## DRF scheduling

`team_quotas` table carries `fair_share_weight` and an `in_flight_count`
counter per team. The claim SQL (canonical in
`src/loom_control_plane/scheduler/claim.py`) is a single CTE + UPDATE
that atomically transitions one trial from `queued` to `claimed`:

```sql
WITH next AS (
  SELECT t.id
    FROM trials t
    JOIN team_quotas q ON q.team_id = t.team_id
   WHERE t.state = 'queued'
     AND t.attempt_count < q.max_attempts
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND (
       COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
       OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches)
     )
     AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
     AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
   ORDER BY (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,
            t.submit_priority DESC,
            t.submitted_at ASC
   LIMIT 1
   FOR UPDATE OF t SKIP LOCKED
)
UPDATE trials t
   SET state = 'claimed',
       worker_id = :worker_id,
       claimed_at = NOW(),
       failure_reason = NULL,
       failure_message = NULL,
       attempt_count = attempt_count + 1
  FROM next
 WHERE t.id = next.id
 RETURNING t.id, t.team_id, t.task_id, t.config, t.requires_caps,
           t.attempt_count;
```

New task submissions and worker registrations require x86_64. Task declarations
of `any` resolve to x86_64; historical ARM records remain readable but are not
eligible for new execution. Legacy trial rows without `requires_caps.cpu_arch`
retain the x86_64 default. ARM clients may submit to remote x86 workers.

Importers no longer promote base-image matches to `any`, and workers no longer
build ARM substitute base images. Task images are built for `linux/amd64`.

Ordering, most-important to least:
1. Lowest `in_flight_count / fair_share_weight` (Dominant Resource
   Fairness — the team with the smallest current share of the fleet
   wins)
2. Highest `submit_priority`
3. Oldest `submitted_at`

The eligibility predicate also filters by worker capabilities (OS,
GPU vendor, supported network policies) and retry windows
(`attempt_count`, `next_attempt_at`).

`FOR UPDATE OF t SKIP LOCKED` lets multiple Workers claim
concurrently without double-claiming.

Hypothesis property tests in `tests/property/test_drf_fairness_property.py`
verify fairness invariants across many simulated worker/team
configurations.

## LLM Gateway

`src/loom_llm_gateway/`:

- LiteLLM-backed proxy for OpenAI/Anthropic/Google + native httpx
  passthrough for dialect-specific routes
- Bearer auth (team-scoped or step-scoped JWT)
- Rate-card lookup (`rate_cards` table) per (provider, model)
- Provider-connection facade cost lookup via
  `provider_connections.rate_card_provider` for BYO endpoints
- Cost compute at request time for successful calls; failed upstream
  attempts insert zero-token `llm_calls` rows with
  `provider_extras._loom_call_status=failed` before surfacing the
  error, so finalize/debug paths can distinguish failed provider
  attempts from missing Gateway evidence.
- Per-call attribution via `(team_id, trial_id, step_id)` fields on
  the `llm_calls` row
- Per-call redacted request-parameter audit via `request_params`;
  legacy rows where this column is NULL surface as
  `status=unavailable_legacy` rather than silently implying matched
  inference settings.

Routes (mounted at the Gateway service root — agents inside sandboxes
hit them through a sandbox-facing Gateway URL):

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/messages` | Anthropic dialect |
| POST | `/v1/chat/completions` | OpenAI dialect |
| POST | `/openai/v1/chat/completions` | OpenAI-compatible provider facade for stock SDKs |
| POST | `/v1/responses` | OpenAI Responses dialect; also routes provider-connection calls when the step JWT carries `provider_connection_id`; OpenAI-compatible provider connections with chat-only upstreams can fall back to `/chat/completions` after the upstream missing-`messages` `/responses` error |
| POST | `/openai/v1/responses` | OpenAI-compatible Responses provider facade for stock SDKs and Codex CLI, including the same chat-only upstream compatibility fallback |
| POST | `/v1beta/models/{model_path}` | Gemini dialect (Google's `v1beta` namespace) |
| POST | `/admin/rate-cards` | Upsert rate card (gated on `admin:rate_cards` scope) |
| GET | `/admin/rate-cards` | List rate cards for an authenticated reader |
| GET | `/admin/rate-cards/{rate_card_id}` | Read one rate card for an authenticated reader |
| GET | `/healthz` | Liveness |

## Auth tokens

Team, worker, and user-owned API credentials remain database-backed. Browser
users sign in with username/password sessions. Admin authority comes from a
file-backed singleton secret or a platform-admin browser user; DB-backed admin
rows are ignored and revoked by migration.

Four token kinds, all bearer-format:

| Prefix | Issued by | Scope |
|---|---|---|
| `loom_api_...` | Team owner or platform user (`POST /api/v1/tokens`) | Named, scoped API token carrying team and creating-user identity; submit trials, view results, optionally manage providers/tokens |
| `worker:*` | Auto-issued at worker `POST /workers/register` | Long-lived; claim trials, PATCH state |
| `step:*` | Worker mints per-step JWT (`mint_step_token`) | Short-lived (per-step); CLI agent calls Gateway with bounded scope |
| `admin:*` | Operator secret file (`loom service init-admin`) | Manage tokens, rate-cards, teams |

Admin callers to `POST /api/v1/tokens`, `POST /api/v1/tokens/{prefix}/rotate`,
and `DELETE /api/v1/tokens/{prefix}` must send `X-Loom-Admin-Actor`; those
token mutations are recorded in `admin_audit_events` with safe metadata
such as names and token hash prefixes. Team callers minting, rotating, or
revoking their own user-owned API tokens do not need an admin actor, but they must
hold `tokens:manage`.

`tokens` table tracks `last_seen_at` and `last_used_at` per token for rotation
hygiene. DB-backed bearer verification debounces those timestamp writes to at
most once per token per 60 seconds so worker heartbeats, claims, and writebacks
do not serialize on one shared token row under high concurrency.

## Auto-cancellation: trial source-state awareness

Cancel requests (`POST /trials/{id}/cancel`) check the trial's current
state before propagating:
- `queued` → DB UPDATE state=cancelled only (Worker never sees it)
- `claimed` → DB UPDATE + Worker's per-trial cancel channel fires on
  next heartbeat / when Trial.run() reaches a checkpoint
- `running` → DB UPDATE + cancel channel fires; Driver tears down
  sandbox; finalize still runs (cancelled trials still get an ATIF
  document for audit)
- `succeeded | failed | cancelled` → no-op

## Persistence schema

| Table | Owner | Purpose |
|---|---|---|
| `trials` | CP | State machine + DRF claim |
| `llm_calls` | Gateway | Per-call cost attribution; FK to trial |
| `tasks` + `benchmarks` | Operator CLI | Cluster task catalog |
| `team_quotas` | CP | DRF weights + legacy license metadata |
| `pending_team_registrations` | Service | Default-closed team onboarding queue and admin review state |
| `team_invites` | Service | Hashed invite links for user membership onboarding |
| `tokens` | Service / CP | Bearer tokens (all 4 kinds) |
| `batches` | Service | Batch grouping + idempotency keys |
| `cloud_compute_records` | Cloud drivers | Per-sandbox lifetime + cost; `cloud_provider` column |

Migrations live in `database/migrations/versions/` and are applied with
`alembic -c database/migrations/alembic.ini upgrade head`.
`loom service up` runs this automatically after Postgres is
healthy. Direct service startup does
not auto-migrate: `loom-service`, the Control Plane, and the LLM Gateway refuse
to start when the database Alembic revision is behind repository code, so
production deploys must run migrations before rolling DB-facing services. Their
production images carry `database/migrations/alembic.ini` and `database/migrations/versions/` so
the startup gate compares the live DB against the same migration tree as the
running image. Startup DB probes use bounded retry for transient DNS,
connection, or Postgres-starting failures; worker startup retries initial
Control Plane registration and orphan-trajectory cleanup lookups. Schema
mismatch, missing migrations, bad credentials, and SecretStore decrypt failures
still fail startup without retry.

## SPA

`web/` — React 18 + Vite + TypeScript + TanStack Query + React Router.
Core pages include:

- **Home** — authenticated `/` overview backed by
  `GET /api/v1/overview`. The service aggregates current-team readiness,
  provider health, benchmark readiness, active workers, recent batch/trial
  activity, and next actions so the SPA does not fan out across multiple
  resources on first load.
- **TrialsList** — keyset cursor pagination, state filter
- **TrialDetail** — header + artifact download links + Debug evidence card +
  paginated trajectory viewer + EventTimeline (one row per event,
  click-to-expand JSON) + ATIF download button
- **BatchesList** + **BatchDetail** — `refetchInterval: 5000`
  while state ∈ {submitted, running}; stops on terminal.
  Multi-benchmark batch detail responses include `benchmark_summary`,
  grouped server-side from trial task ids to task benchmark ids and
  benchmark display names, so the SPA can show per-benchmark score,
  expected/completed trial counts, and platform failures without
  parsing task ids. Multi-agent/model batch detail responses include
  `combination_summary`, grouped server-side from `Trial.combination_idx`,
  preserving requested combinations that have no materialized trials. The
  Run Library batch detail renders this as a comparison table with reward,
  actual/expected trial count, scored-trial count, success/failure counts, and
  LLM usage for each combination. `GET /api/v1/batches/{id}` also exposes
  `effective_combination_summary` after successful supplemental rerun
  replacement; Run Library detail uses the effective version when shared
  supplemental reruns are available. Batch detail also renders the Debug
  evidence card from the same `debug_evidence` object exposed to API and CLI
  callers.
- **Run Library** — complete timestamp/id keyset traversal with URL-backed
  filters and session-local Previous/Next cursor history. Scope or filter
  changes synchronously reset to page one; guarded `aria-disabled` controls
  block concurrent page changes without dropping keyboard focus, and later-page
  errors retain Previous and Retry actions.
- **NewBatch** — textarea JSON parse for `task_filter` +
  `trial_config` with local validation
- **Tasks** — cluster task catalog
- **Benchmarks** — registered adapters
- **RateCardsAdmin** — list + create form (gated on
  `admin:rate_cards`)
- **UsageDashboard** — date-range picker + inline SVG bar chart +
  breakdown table; provider-neutral compute totals and Modal-specific totals
  surfaced via a CTE join against `cloud_compute_records` (see
  `src/loom_service/routes/usage.py`)
- **Settings** — authenticated Team/session settings. Signed-out onboarding
  (username/password login, account request, password-reset request, CLI
  guidance) lives at `/auth/login`; legacy signed-out `/settings` redirects
  there. Authenticated Settings keeps current team, role, team switcher, joined
  browser members, role-aware setup links, and API-token summaries
- **Admin access** — account setup/reset approvals, fixed-team maintenance,
  owner/team-admin legacy invite create/list/revoke/resend, and one-time
  API-token reveal with CLI setup commands. The page is split into role-aware
  sections, and platform-admin invite creation uses a team selector rather than
  requiring raw team ids. Its Audit section mounts lazily and uses the same
  session-local Previous/Next, loading, error/retry, and terminal-page contract.
  Audit keeps the event UUID wire cursor for rolling compatibility while the
  server resolves its timestamp and applies the stable timestamp/id predicate.
- **NotFound**

Auth model: browser users sign in through `/api/v1/auth/login` with username
and password. The service sets an HttpOnly session cookie; auth responses return
a CSRF token that the API client keeps in memory, sends with
`credentials: "include"`, and attaches to unsafe methods through the configured
CSRF header. A 401 from `/auth/me` means signed out, and later 401s clear
session state/query cache before returning the user to Settings. User-owned
bearer tokens remain supported for CLI/API automation, but the production SPA
does not store normal bearer-token login state in `localStorage`.

**Deployment:** `deploy/Dockerfile.web` is a multi-stage build —
node-slim builds the Vite bundle, nginx-alpine serves it, and a startup
script writes public `loom-frontend-config.json` route/API metadata from pod
environment variables. Nginx serves the bundle with a self-only Content
Security Policy, `nosniff`, `no-referrer`, and disabled camera/microphone/
geolocation permissions on success, redirect, and error responses. The bundle
uses system sans/monospace stacks and has no runtime Google Fonts dependency;
ingress-nginx continues to own HSTS at the TLS boundary.
`deploy/k8s/web.yaml` is a 2-replica Deployment + Service on port
80. The TLS `loom-ingress` routes `<ingress_host>/api/v1/*` to
`loom-service:8090` and everything else to `loom-web:80`; nginx inside
`loom-web` falls back to `index.html` for client-side React Router paths. The
canonical hosted profiles use `https://yylx.world/dev`,
`https://yylx.world/staging`, and `https://yylx.world/prod`, with API calls
routed under the matching `/dev/api/v1`, `/staging/api/v1`, or `/prod/api/v1`
prefix and rewritten to the service's `/api/v1` surface.

**Deployed version display (#2009):** the sidebar's persistent version entry
("Nebius · &lt;env&gt; · &lt;12-char commit&gt;") and the backend's
`GET /api/v1/version` both read locally available, immutable build
metadata — never GitHub, Kubernetes, or the database. The release publisher
(`scripts/ops/nebius_candidate.py`) already builds every component image,
web and service included, with `--build-arg LOOM_BUILD_SHA=<candidate>`; it
additionally passes `--build-arg LOOM_SOURCE_REF=<source_ref>` for this
feature. `deploy/Dockerfile.service` writes the candidate SHA and an image
build timestamp to `/opt/loom/build-sha` / `/opt/loom/build-time`, which
`loom_service.build_info` reads leniently (a missing/malformed file yields
`null`, never a failed request) for the version endpoint.
`deploy/Dockerfile.web` uses the same build args two ways: it sets them as
`VITE_BUILD_REVISION`/`VITE_BUILD_SOURCE_REF`/`VITE_BUILD_TIME` before
`npm run build` so Vite `define` (see `web/vite.config.ts` and
`web/src/lib/buildInfo.ts`) freezes them into the JS bundle itself — the
*loaded* identity, fixed for the lifetime of an already-open tab, never
changed by a later fetch — and it also stamps them into the nginx image's
environment/`/etc/loom-frontend-build-time` so
`deploy/web-runtime-config.sh` includes them in `loom-frontend-config.json`
as the *served* identity. `web/src/lib/buildVersion.ts` rechecks that served
identity (#2183) every 10 minutes while the page is visible (paused while
hidden, with no catch-up on return), on window focus or the page becoming
visible (at most once per 60 seconds), and immediately when version details
open; all triggers share one in-flight request, and a failed lookup keeps the
last valid comparison. The backend endpoint is refetched on focus, throttled
via react-query's `staleTime`. Drift from the loaded identity shows as the
sidebar dot and a non-disruptive notice in the version details with an
explicit refresh action — it never reloads or discards input on its own. A plain local build
with no `--build-arg` bakes the Dockerfiles' literal `unknown` default for
revision/source ref (build time is always a real timestamp — it's computed
at build time either way, not sourced from a build-arg); the frontend and
backend both treat that placeholder the same as absent, so the UI shows an
honest "local"/"unknown" identity rather than a misleading commit link.

For local dev: `cd web && npm run dev` runs Vite's dev server on
:5173 with HMR. Vite's `server.proxy` config sends `/api/*` to
`localhost:8090`, which is what `docker-compose.dev.yml` exposes
`loom-service` at — no separate proxy config needed. The compose web
service pins `node:20.19.5-slim` and uses `npm ci` because
`web/package-lock.json` is bind-mounted; avoid switching back to
`npm install` or a floating Node tag unless lockfile stability has
been re-verified. The web package pins the Linux x64 and arm64
Lightning CSS native optional packages at the root, and
`deploy/Dockerfile.web` validates the target-architecture bindings installed
by `npm ci` so Vite has the native modules needed for both image platforms.
It does not run a second unlocked install, which would change the dependency
graph after the lockfile-controlled installation. Dev compose
host ports bind to
`${LOOM_DEV_BIND_ADDR:-127.0.0.1}` by default; set
`LOOM_DEV_BIND_ADDR=0.0.0.0` only for deliberate shared-dev exposure.

## Cross-cutting concerns

- **Logging** — structlog JSON to stdout. Correlation IDs via
  `bind_trial_context`. Does **not** nest — bind at outermost scope.
- **Metrics** — Prometheus on `/metrics` (each service binds its own
  port, default 9090). Bounded cardinality — no per-trial labels.
- **License metadata** — `Task.license`, benchmark license fields, and legacy
  `team_quotas.license_allowlist` values are informational. `POST /trials`,
  benchmark readiness, `POST /api/v1/tasks/count`, and batch creation do not
  reject or hide tasks based on SPDX value. `license_execution_policy` tags are
  retained for catalog provenance but are not execution policy inputs.
- **Idempotency** — batches + trials accept an `idempotency_key`;
  partial unique index `WHERE NOT NULL` on `trials.idempotency_key`.
  Cross-team collisions return 409.
- **Task config validation** — `POST /trials` and
  `POST /api/v1/batches` validate the stored `TaskConfig` before
  queueing work. The batch runner filters legacy invalid task rows out
  of existing batches, lowers `expected_trial_count` to the valid slate,
  marks mixed slates `partial_failed`, and finishes all-invalid slates as
  `all_failed` instead of retrying forever.
- **Batch fan-out failures** — child `POST /trials` failures that are
  deterministic policy/config rejections (`400`, `403`, `404`, `409`,
  `422`) are recorded on `batches.fanout_errors`. The runner lowers
  `expected_trial_count`, marks the batch `partial_failed` or
  `all_failed`, and skips the same idempotency key on later ticks. Network
  errors, `429`, and `5xx` remain retryable.
- **Transient gateway retry + failed-case reruns** — trial config defaults
  to retrying `gateway_error` and `provider_transport_disconnect` up to 3
  attempts with bounded backoff. The worker classifies gateway `5xx`,
  timeouts, connection resets, remote protocol drops, and subprocess-agent
  transport text such as "server disconnected without sending a response" as
  retryable transport failures, while provider `4xx` remains a non-retryable
  provider/config error. Batch Detail can create a linked rerun batch through
  `POST /api/v1/batches/{id}/rerun-failed`; the route first builds the same
  supplemental rerun plan exposed by `GET /api/v1/batches/{id}/rerun-plan`.
  By default it selects only auto-safe platform/transient failures. A caller can
  include operator-approved rows explicitly, but task compatibility failures
  and reward `0` score failures are not auto-rerun. The child batch stores
  `rerun_of_batch_id` and exact `rerun_targets` so the runner re-submits only
  those task/sample/combination coordinates. Detail views expose both original
  rollups and effective rollups where successful supplemental trials replace
  the original replaceable failures.
  Runtime replay remains frozen by default. Explicit `use_current_runtime: true`
  on this route selects the deployed profile through the normal new-submission
  resolver and clears only the child's explicit agent versions. The current
  controller image is frozen with that profile; no catalog "latest" lookup or
  parent mutation occurs. Automatic Nebius Terminus execution is required for
  every selected task, and unsupported or unavailable runtimes fail before
  creating a child. Source provenance records the opt-in.
- **Runnable task counts** — benchmark `task_count` and
  `POST /api/v1/tasks/count` are user-facing runnable counts, not raw
  task-table row counts. Placeholder rows with empty or incomplete
  `TaskConfig` data do not make a benchmark look launchable in New Batch.
  `GET /api/v1/benchmarks` also returns raw/valid/invalid counts, compatibility
  license count fields (`license_allowed_task_count` equals valid tasks,
  `license_blocked_task_count` is 0, `blocked_licenses` is empty), readiness
  state, blocker reason, and a user-facing readiness message so the SPA can
  disable blocked benchmarks without hard-coded benchmark names. The batch
  creation path still performs the final strict validation and rejects invalid
  explicit selections with HTTP 400.
- **Task bundle lookup** — workers fetch the full task body from
  `GET /tasks/{task_id}/bundle`; task ids may include slashes such as
  `humaneval/HumanEval/26`.
- **Worker shutdown ordering** — stop heartbeat thread first
  (prevents fence-bump races); then drain in-flight trials with a
  configurable budget (default 5 min); then exit.

## Provider API key retirement

Rotating a provider key or soft-deleting its connection records
`secrets.provider_retired_at` in the same transaction as the connection change.
The old value remains decryptable for at least 24 hours. Each service worker
runs a cancellable collector every minute: one transaction discovers at most
100 legacy soft-deleted keys and examines at most 100 expired retirements.
Discovery starts a fresh grace period, independent of key age or deletion age.
Historical rotation orphans without a retained owner are deliberately retained;
a `team:` namespace alone does not establish provider ownership.

Collection checks all active provider references, including shared keys, and
all retained `DevInstance.secret_ref`, task-image build bootstrap/session refs,
and `PipelineStageRun.secret_refs`, regardless of consumer state. A referenced
candidate gets a fresh grace clock. Soft-deleted provider rows retain attribution
without retaining credentials forever. The gateway looks up and decrypts the
current key per request; already-started requests get the retirement grace.

Migration 0156 guards newly attached canonical `loom://team:<uuid>/<uuid>` refs
with a secret-row key-share lock and rejects missing refs. Other namespaces and
unchanged historical refs keep their existing contract. Reactivating a deleted
provider also requires its secret to exist. The guard uses a fixed-search-path
security-definer function with public execution revoked, so reference writers
do not gain ciphertext access. Completed application-upgrade validation recognizes
only this trigger's reviewed source hash, trigger signature and fixed search path;
changes to its body or execution properties remain rejected.
Collection locks only candidate secret rows,
skips locked rows, and rechecks references after claiming rows. This makes concurrent attachment and deletion safe without locking entire
consumer tables. Rewrap locks the same row before decrypting; concurrent walkers
and startup validation tolerate refs reclaimed after their initial listing.
The consumer guard and collector reference inventory must grow together if a new
secret-reference column is introduced.

A pass has a 15-second deadline, a 1-second lock timeout, and a 10-second statement
timeout. Failure or cancellation rolls back the entire pass; a later pass retries.
Shutdown cancels and awaits the loop. Reverting migration 0156 removes collection
metadata and attachment guards, but cannot recover already reclaimed ciphertext;
key recovery requires an authorized backup restore or supplying a new key.

## See also

- [overview.md](overview.md)
- [cli-mode.md](cli-mode.md) — same `Trial.run()`, no server stack
- [trajectory-and-atif.md](trajectory-and-atif.md) — what the
  trajectory writer + ATIF projector produce
- [driver-protocol.md](driver-protocol.md) — sandbox contract the
  Worker calls
- [Operator runbook](../runbooks/operator-runbook.md) — production deployment
  and operations
- `src/loom_control_plane/` — CP source
- `src/loom_llm_gateway/` — Gateway source
- `src/loom_worker/` — Worker source
- `src/loom_service/` — REST surface
- `web/` — SPA
