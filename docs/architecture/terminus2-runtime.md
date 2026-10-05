# Harbor-embedded Terminus-2 runtime

Loom runs pinned Harbor `Terminus2` in-process through two orchestration paths:

- Local and worker-owned trials route `agent.name == "terminus-2"` to
  `LoomTerminus2Runtime` in the worker image. They do not use a
  `loom-launcher` subprocess adapter.
- Automatic Nebius trials freeze a Terminus controller image and phase command
  into the execution plan. The per-trial `execution` container invokes
  `service_execution_sandbox_task terminus-2`; that hosted bridge reuses
  `LoomTerminus2Runtime` with a lease-scoped Gateway client and a
  `ServiceSandboxDriver` connected to the task-image sidecar over a private
  Unix socket. It skips the worker `setup()` method and never installs packages
  during the trial.

The common sandbox and verifier lifecycle belongs to hosted execution, not to
the Terminus harness. See
[`hosted-agent-harness.md`](hosted-agent-harness.md) for the extension boundary.

## Pins and runtime images

| Constant | Value |
|---|---|
| Harbor compat SHA | `527d50deb63a5d279e8c20593c18a2cbc7f61f9e` |
| Harbor runtime version | `0.18.0` |
| Loom bridge revision | `1.0` |

Harbor is installed in `deploy/Dockerfile.worker` for the local/worker path and
in `deploy/Dockerfile.harbor-runtime` for the native controller path. It is not
installed by a task-authored script. Worker Gate 1 evidence includes:

- `deploy/worker-image.lock` — frozen worker transitive deps after `pip check`
- `deploy/worker-image.wheels.json` — SHA256 for critical wheels
- `scripts/ops/update_worker_image_lock.sh` — regenerate lock/hashes after
  worker dependency changes

Worker provenance is emitted at trial start via `terminus2_runtime_provenance`
events and `loom.agent.terminus2.worker_provenance`.

This runtime identity is independent of benchmark identity. A TB2.1 trial must
record both the Harbor Hub rev-6 package/profile provenance and the Terminus-2
worker/bridge provenance; matching one does not attest the other. The benchmark
worker also verifies the materialized task checksum before agent startup, keeps
private tests/solutions/verifier files out of this agent runtime, and creates a
fresh verifier-only driver after Terminus-2 exits. The public agent workspace
is handed to that driver as a validated archive snapshot so directory layout,
regular-file modes, safe relative and absolute symlinks, and hardlinks remain
score-equivalent. Absolute symlink targets must stay inside the declared
snapshot root, which is the same path in both sandboxes; their original strings
are preserved. Link chains resolve in filesystem component order, before parent
components (`..`), and every intermediate target must remain inside that root
and outside private paths. Cross-root links, cycles, chains exceeding Linux's
40-link limit, traversal, devices, FIFOs, and sockets fail closed. These checks
also apply independently to each declared mutable directory snapshot.
Native Terminus tasks can additionally declare exact image executable leaves in
`environment.workspace_reference_files`. A workspace link may terminate at one
of those leaves; no parent-directory authority is granted. An image alias needs
an explicit literal target in `environment.reference_file_symlinks`, resolving
in one hop to a regular executable declared in the same reference group.
The trusted native RPC reads alias text without following it and fingerprints
the executable's bytes, mode and ownership. A private manifest binds those
records to the workspace archive digest. The fresh verifier must match before
replacement and after import; missing inspection support or drift fails closed.
Restoration replaces public workdir contents, including
deletions, while preserving freshly staged private verifier inputs and their
parent directories. It rejects symlink destinations, incomplete inventories,
and archive entries that would replace a parent of a preserved private input
before deleting public state. Cancellation waits for verifier-driver teardown before it
propagates, preventing sandbox cleanup from racing network/sidecar teardown.

Tasks that depend on POSIX access or default ACLs declare
`environment.preserve_acls = true`. That declaration applies to the workdir and
every mutable root. Preparation proves a numeric ACL roundtrip with `tar --acls`,
`getfacl` and `setfacl` on the task filesystem before model execution; missing
tools, unsupported filesystems or insufficient permissions fail explicitly.
Capture uses PAX ACL headers and numeric ownership, private-file filtering
retains public metadata, and independent extraction restores both access and
default ACLs. Archive validation rejects malformed, named-identity and non-POSIX
ACL metadata before replacing verifier contents. Restoration runs under the
normal sandbox identity and does not transfer general xattrs, file capabilities
or SELinux labels. Undeclared tasks retain the ordinary mode/link transfer
contract, including BusyBox tar compatibility; ACL-bearing archives cannot be
silently restored through that ordinary path.

The Nebius profile preserves a declared safe absolute task working directory
through image preparation, staging and isolated verification. Protected runtime
and verifier paths cannot be workspace roots or descendants. The trusted
controller retains its fixed `/app` working directory and isolated Python
imports; selecting a task directory never changes controller process discovery.
Authored Dockerfile `WORKDIR` mismatches remain intake diagnostics until the task
declaration preserves the original location, including separately declared
mutable state outside that workspace.

## Runtime shapes

Local and worker-owned execution:

```
Trial.run()
  └─ LoomTerminus2Runtime
       ├─ LoomHarborEnvironment  (Harbor BaseEnvironment → Loom Driver exec/tmux)
       ├─ harbor.agents.terminus_2.Terminus2  (pinned in-process)
       ├─ LLM via Gateway step JWT (openai/{model_id})
       └─ HarborCheckpointBridge
            ├─ reads Harbor trajectory.json + recording.cast under .loom/agent/
            ├─ GatewayCallLedger joins episodes to CP llm_calls
            │    (correlated client_call_id / episode+ordinal; legacy token join)
            └─ appends typed terminus2_* events to Loom trajectory JSONL
```

Automatic Nebius execution:

```text
execution lease with frozen runtime plan
  -> Kubernetes Job
     -> execution container (digest-pinned Harbor controller image)
        -> loom-execution-runtime
           -> service_execution_sandbox_task terminus-2
              -> service_execution_terminus2.run_terminus2
                 -> LoomTerminus2Runtime
                 -> ServiceSandboxDriver
                    | private Unix-socket RPC
                    v
     -> task-sandbox sidecar (digest-pinned task image)
        -> loom-sandbox-runtime
```

The hosted controller owns Harbor, model calls, typed events and native Harbor
artifacts. Terminal commands and task file operations execute in
`task-sandbox`. Shared verification later reuses that sandbox. Separate mode
commits the public workspace, releases the agent pod, and grades in a verifier
lease the control plane reserves on demand
([#2212](https://github.com/qianyi-sun/loom/issues/2212)). That lease restores
the committed workspace from the handoff input before the fixed verifier runs.

Harbor artifacts copied into the trial sandbox:

- `.loom/agent/trajectory.json`
- `.loom/agent/recording.cast`

The local step runner always includes `.loom/agent/**` in artifact patterns for
`terminus-2` trials. The hosted compiler instead declares each canonical and
native output path in the frozen runtime plan.

## Exception identity

Loom embeds Harbor's agent rather than Harbor's Trial orchestrator. When the
embedded agent raises, Loom preserves the original exception class even if its
message is empty (for example, `ContextLengthExceededError`). Agent wrappers are
unwrapped without replacing an SDK exception with its lower-level transport
cause. Messages use the standard secret redactor; tracebacks and local variables
are not added to the public failure record.

The worker path retains `steps[].error.exception_type`, its sanitized `message`
and timestamp in the Trial result. Native Nebius execution publishes optional
`diagnostics/agent-exception.json` and `diagnostics/verifier-exception.json` files
from the trusted controller workspace. Each contains `exception_type`,
`exception_message` and `occurred_at`. Materialization projects the failing
phase's identity into the canonical `trial_error` event and
`trial.result.exception_info`; accounting reconciliation preserves it. Canonical
bundle downloads retain the diagnostic files, and raw delivery
`execution_result.json` retains the enriched Trial result.

The existing `failure_reason`, terminal state and retry policy remain unchanged:
exception identity supplies detail, not a second retry taxonomy or a claim that
Harbor's verifier/orchestrator ran. Older results without these optional fields
remain readable. Previously lost exception types cannot be reconstructed from
an empty historical message.

## Attempt deadline and step-credential lifecycle (#1748)

Tasks may declare `agent.continue_until_timeout = true` to keep the selected
Terminus-2 session working until its existing absolute attempt deadline. The
default is `false`; default values are omitted from serialized task configs so
existing task checksums remain stable. The frozen task retains a true declaration.
Admission checks the resolved trial agent, which may differ from the source
task's default agent. Other runtimes reject the policy as incompatible.

For this policy, Loom disables its ordinary 50-turn limit and adapts the pinned
Harbor instance's completion confirmation. Completion requests remain recorded
as real `mark_task_complete` actions, but do not return from the session. The
adaptation is local to that instance; it does not restart Harbor or create a
second attempt. Deadline expiry remains a timeout with partial evidence, while
cancellation, runtime errors and existing resource limits remain terminal.
The shared worker/local trial pipeline and native Nebius adapter both carry
the task declaration into this runtime. An absolute attempt deadline is required.

> **Evidence boundary:** this section defines the repository contract. It does
> not prove that a particular environment has deployed the change, that its
> worker/Gateway/Control Plane images have the same candidate, or that the
> production-equivalent canary below has run.

Each agent attempt receives one immutable `AttemptDeadline` before Loom mints
a credential or starts agent code. The worker creates its executable boundary
from the running event loop's monotonic clock. That process-local monotonic
value is the authority for cancellation and must never cross an HTTP or JWT
boundary.

The same deadline object is passed to the runtime with
`begin_attempt(deadline)`. Direct-completion, subprocess, and Terminus-2
runtimes then bind their attempt-owned I/O to it. The attempt supervisor passes
the agent a guarded trajectory writer; platform code retains the underlying
writer for terminal diagnostics.

The cross-process bridge is a signed UTC wall-clock claim:

| Layer | Deadline representation | Enforcement |
|---|---|---|
| Worker supervisor | `AttemptDeadline.monotonic_deadline` | First terminal cause; cancel at zero remaining time |
| Step-token request/response | RFC 3339 `attempt_deadline_wall_clock` | Control Plane rejects elapsed, changed, or under-covered grants |
| Step JWT | signed `attempt_deadline_wall_clock` claim | Gateway converts it once into a local monotonic cutoff |
| Worker/Gateway HTTP | local `AttemptDeadline.remaining()` | No dispatch at zero; total, connect, read, write, and pool budgets are each capped by remaining time |

Wall-clock conversion never adds executable time. The extra 300 seconds covers
bounded cleanup and clock skew only. It is part of credential validity, not the
agent's execution budget. Worker TTL calculation adds one more encoding second
because JWT `iat`/`exp` NumericDate values have whole-second precision while the
attempt deadline retains microseconds; that second is not executable budget.

Terminus-2 uses one step JWT for one attempt and does not rotate it mid-attempt.
The token is minted only after the deadline exists, and the worker validates
the server-returned `expires_at` and `attempt_deadline_wall_clock` before agent
startup:

```text
expires_at >= attempt_deadline_wall_clock + 300 seconds
```

The effective timeout includes task defaults, per-step overrides, the trial
`override_agent_timeout_sec`, and `agent_timeout_multiplier`. The Control
Plane accepts step-token TTLs up to 30,000 seconds. An elapsed deadline, a
deadline plus reserve that exceeds the ceiling, or a grant that does not cover
the requested deadline fails closed before agent startup. A retry is a new
attempt with a new `AttemptDeadline` and a newly minted token; it never reuses
the prior attempt's credential or transport.

### Deadline supervision and write fence

`asyncio.wait_for(agent.run(), timeout=T)` is not the lifecycle authority. The
supervisor creates an agent task and waits against the absolute deadline. When
the deadline wins, the order is fixed:

1. Latch `agent_timeout` as the first terminal cause.
2. Fence agent-owned `append` and `write_raw_dict` calls. A late callback cannot
   add a model turn, checkpoint, retry decision, or other agent event.
3. Cancel the agent task and immediately call its attempt-scoped
   `aclose_attempt()` hook. This closes or interrupts only that attempt's HTTP
   transport, subprocess, or Harbor tmux session.
4. Drain cancellation for at most 30 seconds. A late credential, provider, or
   cancellation exception cannot replace the latched timeout.
5. Emit exactly one typed `agent_timeout` diagnostic for that timed-out
   attempt, then emit `agent_retry` only when `agent_timeout` is explicitly in
   `retry_on` and the old task actually stopped.

The diagnostic fields are:

| Field | Meaning |
|---|---|
| `configured_timeout_sec` | Effective attempt budget |
| `elapsed_monotonic_sec` | Monotonic elapsed time when supervision completed |
| `cancellation_drain_sec` | Time spent in bounded cancellation/transport drain |
| `transport_close_required` | Whether the runtime exposed an attempt close hook |
| `task_stopped` | Whether agent code stopped before the drain bound |

If `task_stopped=false`, Loom sets the fatal worker-health signal, does not
start a retry, verifier, or artifact read against the still-changing
workspace, and still lets platform-owned code write `step_end` and terminal
trial evidence. After output projection and terminal-state reporting, that
signal propagates through the runner pool, stops new claims, and requires the
worker process to restart. Production wiring exits with code 70 without asking
`asyncio.run()` to await the cancellation-resistant task; the container or job
supervisor then starts a fresh process. Claim loops check health before and
after each claim request, so a claim racing the fatal signal is not started and
returns through the normal lease-reclaim path. The old task remains fenced
throughout.

### Gateway authentication and failure taxonomy

Every user-facing LLM dialect uses the same bearer helper. Public responses do
not disclose whether a bearer had a bad signature or merely expired, while
internal logs retain only the low-cardinality reason and never the token.

| Condition | HTTP contract | Loom failure reason | Default retry |
|---|---|---|---|
| Missing, malformed, invalid-signature, revoked, or expired bearer | `401`; `WWW-Authenticate: Bearer error="invalid_token"`; `detail.code=invalid_or_expired_bearer` | `step_credential_invalid_or_expired` when received before the deadline | No |
| Valid bearer without `llm:call` | `403`; `detail.code=missing_scope`; `detail.required_scope=llm:call` | `step_credential_scope_invalid` | No |
| Provider-originated `401` or `403` after Loom authentication | Provider response | `provider_error` | No |
| Worker-to-Gateway timeout or disconnect before the deadline | Transport exception | Existing transport taxonomy; see #1749 | Policy-dependent |
| Any response or exception after the deadline latch | Not allowed to replace the terminal cause | `agent_timeout` | No, unless explicitly configured |

Internal bearer reasons are restricted to `missing`, `malformed`,
`invalid_signature`, `expired`, and `valid`. Classifiers consume the structured
HTTP codes above rather than matching human-readable response strings.

During rolling upgrade, pre-deadline tokens are accepted only inside the
Gateway process compatibility window configured by
`LOOM_GW_LEGACY_ATTEMPT_DEADLINE_COMPAT_SEC` (default 86,400 seconds), with
separate `accepted` and `rejected` counters. Set this value to `0` after all
Control Plane and worker images have rolled to make the deadline claim
mandatory. The default window restarts with the Gateway process, so a completed
rollout must pin `0`; repeated restarts are not a migration-completion signal.

Terminus-2 finalization uses the same attempt mutation gate before trajectory
sync, Control Plane episode checkpoint, sandbox artifact publication, and
artifact-ref emission. Checkpoint requests also carry the signed wall deadline;
the Control Plane locks the trial row, rejects terminal trials, and rolls back
if the deadline is reached before commit.

### Terminal persistence order

For a normal timeout, the durable surfaces must agree on
`failure_reason=agent_timeout`, with no credential failure:

1. The supervisor latches timeout and the platform writer records the unique
   `agent_timeout` diagnostic.
2. The step records an optional explicit `agent_retry`; otherwise it records
   `step_end` with agent timeout.
3. Trial finalization appends the unique `trial_end` after all other trajectory
   events and closes the canonical JSONL writer.
4. Loom computes the trajectory digest/version and ATIF projection, and builds
   the terminal result/output projection.
5. The worker persists that output projection before issuing the terminal
   Trial state PATCH.
6. Once terminal state commits, Control Plane trajectory/result writes are
   fenced. A cancellation-resistant worker becomes unhealthy only after the
   terminal projection attempt, so the restart cannot erase the terminal
   evidence.

The outer Trial row, result payload, unique `trial_end`, and ATIF metadata must
all report the same final state and failure reason.

## Typed trajectory events

The Harbor-embedded runtime emits these events for operators and debuggers:

| Event kind | Purpose |
|---|---|
| `terminus2_runtime_provenance` | Harbor pin, bridge revision, template hashes |
| `terminus2_turn` | Model turn boundary |
| `terminus2_command` | Command issued to the terminal |
| `terminus2_terminal_observation` | Bounded terminal output observation |
| `terminus2_parse_retry` | Parser retry after malformed model output |
| `terminus2_context_boundary` | Context/window boundary marker |
| `terminus2_artifact_ref` | Pointer to Harbor artifact (e.g. `recording.cast`) |
| `terminus2_model_switch_planned` | Durable K1/K2 cuts from the plan (before Harbor runs) |
| `terminus2_model_mix_planned` | Durable `beta_mixture` plan (`beta`, seed fingerprint) |
| `terminus2_model_switch` | Applied student↔teacher cut |
| `terminus2_llm_call_started` / `_completed` / `_failed` | Per-call correlation around the router |
| `terminus2_episode_checkpoint` | Multi-model **progress marker** for reclaim (not a Harbor resume save; distinct from `HarborCheckpointBridge`) |
| `terminus2_recovery_failed` | Retry refused rather than merging two Harbor runs |

Gateway join failures (missing `cp_client`, ambiguous token match, command
without observation on a full episode) fail closed via `CheckpointBridgeError`
→ trial `agent` phase error.

## Service catalog contract

`GET /api/v1/agents` lists `terminus-2` under builtins (`kind="builtin"`):

- `execution`: `builtin-terminus2-harbor`
- `capture`: `typed_events+harbor_artifacts`
- `needs_model`: true
- Model template: `openai/{model_id}` through the Gateway Chat facade

`terminus-2` does **not** use per-trial `AgentAdapter.install_script` layering.
It requires the worker image built from the current `Dockerfile.worker`.

## Execution architecture

New workloads and workers use x86_64 (`linux/amd64`). `cpu_arch = "any"`
resolves to x86_64. The former GB10 ARM substitute-image build and importer
promotion have been removed; ARM task declarations must be migrated explicitly.
Historical ARM records remain readable, and ARM clients may submit remotely.

## Hosted acceptance

Use the [Nebius Terminus 2 runbook](../runbooks/nebius-terminus2.md) for
native execution acceptance and retained reward, artifact and provenance
checks. The retired worker-pool smoke is not hosted release evidence.

## Ten-second production-equivalent deadline canary (#1748)

For current hosted Nebius acceptance, use the [native lease-based qualification](../runbooks/isolated-deadline-canary.md#current-nebius-qualification). The worker/pool and within-worker Case A/B details below describe the legacy implementation. They do not override native credential rotation, phase supervision, result vocabulary or scoped Pod isolation.

This canary is a post-deployment acceptance action, not a local test. Run it
only in an explicitly authorized production-equivalent environment with a
dedicated canary task, team/provider connection, worker pool, and fault
endpoint. Do not block shared provider traffic. Until all evidence below is
captured, describe #1748 as repository-implemented or deployed-but-unverified,
not production-accepted.

### Preconditions

1. Record the intended candidate commit, worker/Gateway/Control Plane image
   digests, environment and route identity, deployment request/evidence ID,
   and post-deployment read-back time. All components must resolve to the same
   accepted candidate.
2. Configure a dedicated provider fault endpoint that accepts exactly one
   canary request, records sanitized request start/finish timestamps plus the
   Loom `trial_id`/`step_id` correlation, and holds the response beyond the
   10-second attempt deadline. It must never record Authorization headers,
   bearer values, provider secrets, or request content.
3. Use a one-step task that deterministically makes a model call immediately.
   Pin the Terminus-2/Harbor provenance and the task checksum in the evidence.
4. Confirm the selected worker is otherwise idle and the canary cannot affect
   production batches. Capture the worker ID and pool before submission.

### Case A: default no-retry timeout

Submit one trial through `POST /api/v1/trials` with this decision-complete
payload shape (replace angle-bracket placeholders; keep the timeout and retry
values exact):

```json
{
  "task_id": "<dedicated-canary-task-id>",
  "idempotency_key": "<unique-case-A-idempotency-key>",
  "required_worker_pool": "<authorized-exclusive-canary-pool>",
  "config": {
    "agent_name": "terminus-2",
    "agent_model": {"provider": "<provider-type>", "name": "<model-id>"},
    "override_agent_timeout_sec": 10,
    "agent_timeout_multiplier": 1,
    "retry": {"max_attempts": 1, "retry_on": []}
  },
  "provider_connection_id": "<dedicated-provider-connection-id>",
  "provider_model_id": "<model-id>"
}
```

Prepare the fault endpoint before submission; arm the returned trial identity
and approve the exact durable dispatch receipt before allowing its hold.
Release it only after Loom
has reported the terminal trial or 40 seconds have elapsed. Then collect the
single-trial API response, canonical trajectory JSONL, ATIF artifact, output
projection response, Gateway logs/metrics, fault-endpoint log, worker log, and
post-run worker/pool read-back.

Case A passes only if all of these are true:

- The trajectory contains exactly one `agent_timeout`, zero `agent_retry`, and
  one terminal `trial_end`.
- `configured_timeout_sec == 10`, `elapsed_monotonic_sec >= 10`,
  `0 <= cancellation_drain_sec <= 30`, and
  `transport_close_required == true`.
- `elapsed_monotonic_sec <= 40`: the ten-second execution budget plus no more
  than the 30-second cancellation drain. Record the separate terminal
  persistence latency from supervision completion to the terminal Trial
  read-back; do not substitute batch duration for attempt duration or hide
  persistence latency inside the drain measurement.
- Trial row, result payload, `trial_end`, and ATIF all say `failed` with
  `failure_reason=agent_timeout`. None reports either step-credential failure.
- Gateway and provider evidence contains no new request for this trial after
  the deadline/terminal boundary, and contains no `missing_scope`,
  `invalid_or_expired_bearer`, provider `401`, or provider `403` outcome.
- If `task_stopped=true`, the worker remains healthy and claimable. If
  `task_stopped=false`, no verifier or artifact read ran, the worker accepted
  no later claim, and the supervisor restart/readiness cycle completed before
  that worker identity returned to service.

### Case B: explicit timeout retry

Repeat with a new single-trial idempotency key, `retry.max_attempts=2`, and
`retry.retry_on=["agent_timeout"]`. Hold only attempt 1 across its 10-second
deadline; allow attempt 2 to complete normally.

Case B passes only if attempt 1 stopped within the drain bound, the trajectory
contains one timeout diagnostic followed by one `agent_retry`, attempt 2 has a
new `attempt_deadline_wall_clock` and a separately minted step-token grant,
and no request from attempt 1 appears after its fence. Record the two sanitized
mint times, deadline claims, and returned expiry times; do not record or decode
the bearer values. If attempt 1 has `task_stopped=false`, retry must not start
and the worker-unhealthy/restart path from Case A is required instead.

### Evidence manifest

Store one secret-free manifest with at least these fields:

```yaml
issue: 1748
candidate_sha: <commit>
environment: <environment-id>
route_identity: <route-and-runtime-config-readback>
deployment_evidence_id: <authorized-deployment-record>
image_digests:
  worker: <digest>
  gateway: <digest>
  control_plane: <digest>
batch_id: null # single-trial canary, not a batch coverage run
trial_id: <uuid>
step_id: <id>
worker_id: <uuid>
worker_pool: <pool>
task_id: <id>
task_checksum: <sha256>
terminus2_runtime_provenance: <event-reference>
trial_submitted_at: <rfc3339>
attempts:
  - attempt: 1
    agent_attempt_id: <supervisor-uuid>
    step_jwt_ids: [<cp-issued-grant-uuid>]
    gateway_dispatch_receipt_ids: [<internal-receipt-uuid>]
    attempt_started_at: <rfc3339>
    attempt_deadline_wall_clock: <rfc3339>
    deadline_latched_at: <rfc3339>
    step_token_expires_at: <rfc3339>
    gateway_request_ids: [<sanitized-id>]
    fault_endpoint_request_id: <sanitized-id>
    provider_request_started_at: <rfc3339>
    provider_request_finished_at: <rfc3339-or-null>
    configured_timeout_sec: 10
    elapsed_monotonic_sec: <seconds>
    cancellation_drain_sec: <seconds>
    transport_close_required: true
    task_stopped: <true-or-false>
terminal:
  trial_row_state: failed
  result_state: failed
  trial_end_final_state: failed
  atif_metadata_final_state: failed
  atif_metadata_failure_reason: agent_timeout
  failure_reason: agent_timeout
  agent_timeout_event_count: 1
  agent_retry_event_count: 0
  supervision_completed_at: <rfc3339>
  terminal_trial_read_back_at: <rfc3339>
  terminal_persistence_latency_sec: <seconds>
post_deadline_provider_request_count: 0
auth_outcomes:
  invalid_or_expired_bearer: 0
  missing_scope: 0
  provider_401_403: 0
worker_unhealthy: <true-or-false>
worker_restart_evidence: <required-when-unhealthy>
```

Attach timestamps or immutable object/log references for every assertion. A
healthy route, green CI, or matching image tag alone is not canary evidence.

### Checked-in local transport harness

The separate [isolated fixture implementation](../runbooks/isolated-deadline-canary.md)
adds OpenAI model discovery, receipt-authorized hold/reply and a bounded Job
resource renderer. Its protected launcher and complete evidence collector are
still pending under #1857; it is not a completed live acceptance path.

`scripts/ops/issue_1748_deadline_canary.py` is a deliberately partial,
loopback-only fault provider and evidence validator. It exercises real HTTP at
the Gateway-to-provider boundary without a live provider, worker pool, or Loom
batch. Case A accepts and holds one request. Case B holds request 1, permits one
separately signed deadline grant to complete, and rejects any later request.
Both cases count wrong-capability and over-limit requests so an unexpected
dispatch cannot be hidden by the one-shot boundary. Request bodies and graceful
shutdown are independently wall-time bounded.

Run the real-HTTP regression with:

```bash
uv run --extra dev pytest -q \
  tests/ops/test_issue_1748_deadline_canary.py \
  tests/integration/test_issue_1748_deadline_canary.py
```

The integration test uses two loopback Uvicorn servers and a disposable
PostgreSQL schema. It proves the Gateway returns the stable `504` /
`agent_timeout` / `attempt_deadline_reached` result for the held request, sends
no extra request when the same expired grant is explicitly replayed, and lets a
separately minted Case B deadline-bearing grant reach the provider and complete.
That local observation does not prove that Control Plane retry authority created
a new execution attempt. It does not exercise Control Plane
terminal persistence, worker supervision/retry, canonical trajectory or ATIF
publication, deployed image/route read-back, or post-run pool health.
Its candidate strings are test fixtures; candidate provenance is enforced only
by the separate manual CLI checkout binding described below.

The provider can also be started manually for loopback development. The
capability is accepted only through an environment variable and is never
written to its evidence file:

```bash
export LOOM_1748_CANARY_NONCE="$(${PYTHON:-python3} -c \
  'import secrets; print(secrets.token_urlsafe(32))')"
candidate_sha="$(git rev-parse HEAD)"
candidate_tree="$(git rev-parse 'HEAD^{tree}')"

uv run python scripts/ops/issue_1748_deadline_canary.py serve \
  --case A \
  --candidate-sha "$candidate_sha" \
  --candidate-tree "$candidate_tree" \
  --trial-id '<local-trial-uuid>' \
  --step-id main \
  --deadline-budget-sec 10 \
  --hold-sec 15 \
  --output /tmp/issue-1748-fault-provider.json
```

The manual command refuses a dirty or candidate-mismatched checkout and any
non-loopback bind. Its output is only a provider observation; a combined local
transport document must additionally contain the Gateway outcomes and can be
checked with `... issue_1748_deadline_canary.py validate --input <path>`.
Every local document is forced to `full_canary_passed: false` and enumerates
the missing acceptance layers. Do not substitute it for the authorized
post-install Case A/B manifest above, and do not use this local command to
configure a staging provider connection, worker pool, or deployment.

## Export

| Mode | Status | Source |
|---|---|---|
| `raw-harbor-tb2-v1` | Shipped | Provider logs + reward joins |
| `raw-harbor-tb2-v2` | Shipped; requires typed source events | Checkpoint bridge / typed `terminus2_*` events via `Terminus2TrajectoryMapper` |

`raw-harbor-tb2-v2` is wired in the delivery API and CLI. Use it when the
trial trajectory contains typed `terminus2_*` events and hash-verified native
Harbor artifacts. Its exporter fails closed when that contract is incomplete;
use `raw-harbor-tb2-v1` for provider-log-based source trials.

## Mid-trajectory student/teacher/student switch (#1380)

When `trial_config.multi_model.enabled` is true on a **terminus-2** trial,
Loom constructs a second Harbor `LiteLLM` (teacher) with the same gateway JWT
and replaces `agent._llm` with a Loom `BaseLLM` router **after**
`Terminus2(...)` and **before** `agent.run(...)`. Harbor is not forked and
`_model_name` is not mutated.

v1 schedule (episode-based; later an off-track detector can set K1/K2):

```
episodes 1 .. K1-1  → student  (trial_config.agent_model)
episodes K1 .. K2-1 → teacher  (multi_model.secondary_model)
episodes K2 .. N    → student
```

`K2 = K1 + teacher_episodes` (default teacher_episodes = 2). At least two
role cuts if the run reaches K2.

Second policy `beta_mixture` (`--multi-model-beta`): each Harbor episode
hashes `seed:trial_id:episode` into `[0, 1)` and runs the **teacher if
`draw < beta`**, else student. Episode grain matches Path A (parse retries
stay on the same actor). This is **not** DAgger: the teacher is not queried
for a label on student-driven episodes. `beta = 1` is all teacher, including
episode 1. Emits `terminus2_model_mix_planned` instead of the two K1/K2
planned cuts. Applied `terminus2_model_switch` still fires when consecutive
episodes change role.

Third policy `student_to_teacher_turns` (`--multi-model-step-start` /
`--multi-model-step-end`): each `LoomRoleRouter.call()` / `call_ordinal` is a
schedule turn. Student before step start, rising β until step end, then force
teacher with one-way latch. Plan stores the window as `k1`/`k2`. Also emits
`terminus2_model_mix_planned` with `grain=turn`.

Constraints:

- Same BYO `provider_connection_id` for both models.
- Silent switch (no handoff prompt). On each role change the router drops
  `previous_response_id` and sends normalized chat history to the incoming model.
- `switch_episode` (K1) is sampled at batch/trial accept when omitted and
  stored on an immutable `model_switch_plans` row (not only on `trial_config`).
  `return_switch_episode` (K2) is always `K1 + teacher_episodes`.
- Router keys off Harbor `_n_episodes` so parse retries stay on the same role.
- Emits a `terminus2_model_switch` event per applied cut.
- Workers that do not advertise `terminus2_model_switch` cannot claim these trials.
- The worker claim payload must include `model_switch_plan` (seed, `mix_mode`,
  K1/K2 or `beta`). Both `POST /work/claim` and the local-compose legacy
  `POST /trials/claim` attach the same snapshot. `beta_mixture` cannot
  recover the mix seed from `trial_config.mix_seed` at runtime.

### Worker retry: fail closed (no merged Harbor rerun)

Loom's scheduler already retries a trial by letting a new worker claim it and
run the agent step again. For a **single-model** terminus-2 trial that is a
full restart: Harbor always boots from episode 1, and the new attempt is a
new run of the task.

For **multi-model** terminus-2 the durable object is one Terminus session
(`terminus_agent_executions`) plus the immutable K1/K2 plan. After each
completed episode the worker writes `episode_checkpoints` (episode, active
role, last call ordinal, seq, checksum).

On the next claim the worker **reclaims** that execution (new
`terminus_agent_run_attempt`, same execution and plan):

| Checkpoint | Harbor can resume mid-session? | What we do |
|---|---|---|
| None (never finished episode 1) | N/A | Start Harbor from episode 1 (`fresh`). Same as a normal first run. |
| Present and checksum fails | No | `recovery_failed`. Do not run. |
| Present and checksum verifies | No (pinned Harbor has no Loom-owned resume) | **Also `recovery_failed`.** Do not start Harbor from episode 1. |

Pinned Harbor `Terminus2.run()` always starts at episode 1. Restarting it
while the first attempt's events still sit on the trial would **append a
second student beginning onto a trajectory that already entered teacher**.
That merge is forbidden.

v1 therefore **fails the trial attempt** (`terminus2_recovery_failed`,
`AgentError` whose message starts with `terminus-2 recovery_failed`) instead
of stitching. A later exact replay is a **new trial** that **inherits** the
plan (same K1/K2, or the same `beta_mixture` seed), not a continuation of the crashed session.

This is intentional, not a missing Harbor hook. True mid-session resume
(same tmux, same episode, same role) would need a Harbor resume path,
which #1380 left out of scope.

Related: `src/loom_control_plane/terminus_recovery.py`,
`src/loom/agent/terminus2/runtime.py`.

## Related code

| Path | Role |
|---|---|
| `src/loom/agent/terminus2/runtime.py` | `LoomTerminus2Runtime` |
| `src/loom/agent/terminus2/model_switch.py` | Two LiteLLM + `BaseLLM` role router (#1380) |
| `src/loom/agent/terminus2/harbor_environment.py` | Driver bridge |
| `src/loom/agent/terminus2/checkpoint_bridge.py` | Harbor JSON → typed events |
| `src/loom/agent/terminus2/gateway_ledger.py` | CP `llm_calls` join |
| `src/loom/agent/terminus2/mapper.py` | TB2 v2 projection |
| `src/loom_service/delivery_export_tb2_v2.py` | TB2 v2 export framework |
| `src/loom_service/multi_model.py` | Batch validation / materialization |
| `src/loom_control_plane/routes/workers.py` | Claim payload `model_switch_plan` (`/work/claim` and `/trials/claim`) |
| `src/loom_control_plane/terminus_recovery.py` | Execution reclaim; fail closed if a checkpoint exists |
| `packages/loom-launcher/loom_launcher/terminus_2_runner.py` | Import-stability stub; exits with current runtime guidance |
| `tests/conformance/terminus2/README.md` | Local conformance notes |
| `tests/unit/test_multi_model_switch.py` | Role router + config unit tests |

## Execution architecture across platforms

Nebius native execution requires Linux x86_64. ARM clients may submit work to
Nebius remotely. This restriction belongs to Nebius admission and image-builder
claims; shared task validation, catalog publication and worker registration
continue to support local ARM execution. Tasks declaring `any`
retain both AMD64 and ARM64 image prerequisites outside Nebius, including the
local Terminus runtime fallback builds.

The [Harbor90 migration runbook](../runbooks/nebius-harbor90-migration.md)
prepares an x86 revision of that specific Nebius catalog. It does not retire ARM
execution globally or authorize publication or live maintenance by itself.
