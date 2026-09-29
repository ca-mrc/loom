# Trial resource accounting

Loom persists resource usage per trial attempt and execution container in
`trial_resource_usage`. This evidence is separate from configured limits,
Prometheus process health, provider billing, and requested execution resources.

## Semantics

- Identity is `(trial_id, attempt_count, execution_key)`. Worker replay is an
  idempotent upsert; cumulative counters and sampled peaks never decrease.
- `complete` means a final observation was collected before container removal.
  `partial` means some observations exist but final collection or delivery was
  interrupted. `unavailable` means the backend exposed no supported resource
  telemetry. Missing or unsupported fields are `null`, never fabricated zero.
- CPU usage/throttling and block-I/O counters are cumulative per container.
  `memory_peak_bytes` and `pids_peak` are maxima observed for that container.
- The multi-container projection exposes
  `memory_peak_upper_bound_bytes`/`pids_peak_upper_bound`. These sum individual
  peaks and are conservative upper bounds, not synchronized whole-trial peaks.
- The worker stores only a SHA-256 hash of the runtime/container identity. It
  never persists environment variables, commands, host paths, credentials, or
  unbounded backend payloads.

Docker workers sample Docker's cgroup-backed stats. Modal currently persists
typed `unavailable` records because its Loom adapter does not expose a
stable CPU/RSS/PID/I/O contract. Adding provider telemetry requires a new typed
adapter implementation and contract tests; it must not map billing duration to
resource use.

Native Kubernetes executions use the actuator's
[kubelet sampling adapter](nebius-platform.md#native-trial-resource-observations).
Their sampled maxima are not kernel peaks, and finalized rows remain `partial`
when the backend cannot supply the full counter contract. Native sampling writes
directly to the durable ledger; the worker outbox described below applies to
worker-backed executions.

## Durable node attribution

Native execution leases retain their observed `node_name` when later Kubernetes
observations omit it or report `null`, including after Pod deletion. A different
non-null node for the same lease is rejected, as are conflicting Pod identities.
Older event ordinals remain historical evidence and cannot change the projection.

This preserves the node match needed to allocate a provider bill after cleanup.
Settlement still requires a persisted execution interval and imported provider
billing coverage; resource reservations are not actual costs.

### Recover historical missing nodes

After migration `0169`, administrators with `admin:worker_pools` can use
`POST /admin/execution-leases/{lease_id}/recover-node-attribution` for an explicitly
selected lease. Send `{}` to preview; the response reports `recoverable`, the
candidate `node_name`, persisted Pod identity, and supporting `evidence_event_ids`.
Preview does not write the lease or an audit event. Send `{"apply": true}` to
recompute and apply the recovery under a row lock. Successful recovery returns
`recovered` and writes an `execution.node_attribution.recovered` admin audit event
in the same transaction. Repeated or concurrent applications return `unchanged`
after the first write; an existing node is never replaced.

Recovery requires a persisted Pod UID and exactly one node supported by retained
Kubernetes events between the lease's immutable resource generation and current
command generation. Named observations with conflicting Pod/Job identities or
different nodes cause HTTP 409. Missing evidence also causes HTTP 409; observations
without a Pod UID cannot establish attribution. The database independently checks
the evidence before allowing a deleted lease's null node to be filled. It still
rejects clearing or changing an existing node and unrelated deleted-record edits.

This is an explicit operator repair, not an automatic migration backfill. It
preserves lifecycle states, ordinals, and timestamps. It neither settles costs nor
reallocates previously imported bills. Preview and repair the affected leases
before importing their provider bills. Missing termination evidence and existing
misallocated bills require separate reconciliation. Downgrading `0169` restores
the prior mutation guard while retaining recovered node values and audit records.

## Durability and recovery

Each active execution checkpoints its latest report under the worker's private
`trajectory_cache_dir/resource-usage-outbox`. Files are mode `0600` in a mode
`0700` directory, atomically replaced, capped at 64 KiB each and 10,000 entries.
Normal teardown stages the final report before removing the container, sends an
idempotent Control Plane `PUT`, and deletes the file only after acknowledgement.
At worker startup, unfinished reports become final `partial` records with
`worker_restart_before_finalize`; acknowledged reports are then removed.

Collection and delivery failures do not change trial correctness. Operators can
alert on:

- `loom_worker_resource_accounting_events_total{result,reason}`
- `loom_worker_resource_accounting_outbox_backlog`

## Read and export surfaces

- `GET /api/v1/trials/{trial_id}/resource-usage`
- `GET /api/v1/batches/{batch_id}/resource-usage`
- delivery archives: `ledger/resource_usage.jsonl`
- raw Harbor delivery archives:
  `agent_runs/<task>/<trial>/resource_usage.json`

These surfaces use the parent trial/batch team authorization. A legacy trial
returns `items: []` plus aggregate `telemetry_status: unavailable`.

## Confirmed native OOM failures

The existing trial debug/diagnosis endpoints and `loom eval diagnose trial <trial-id>` report
confirmed Kubernetes `OOMKilled` terminations independently of periodic usage
samples. A sampled peak below the limit does not disprove a brief OOM spike;
missing measurements remain unknown, never zero.

The durable execution event history binds evidence to the lease, Job, Pod,
container role and restart incarnation. Later authoritative termination evidence
can enrich the original failed incarnation, including after runtime finalization.
Exit 137 alone is insufficient. A replacement container's OOM cannot reclassify
an earlier failure, and subsequent cleanup errors cannot overwrite a confirmed
cause. Terminal outcomes and already-produced artifacts are preserved.

The execution-actuator image includes the diagnosis store and its classifier.
These modules are loaded when execution observations are recorded, so importing
the service entrypoint alone does not verify their availability. The image build
also imports the diagnosis store and its dependencies without opening a database
connection. A missing module fails the build before publication.

The image's standalone requirements pin SQLAlchemy to the version in `uv.lock`
and request the `asyncio` extra explicitly. SQLAlchemy 2.1 no longer installs
`greenlet` by default, although the actuator's asynchronous database access
requires it. Keep the pin aligned when upgrading the locked version, and retain
the clean-image import check for the greenlet-backed asynchronous bridge.

Observation delivery order is not container start order. When snapshots disagree
on the start time for the same role and restart incarnation, diagnosis resolves
the earliest known start across the retained observations before selecting OOM
evidence. A replacement timestamp in an initially stale `lastState` must not
exclude the original container's later-arriving termination record. Job, Pod,
role and restart identity still bound the evidence; exit 137 alone remains
insufficient.

`GET /api/v1/trials/{id}/debug` includes `execution_failure` with stage, role,
incarnation, termination/start times, exit code, effective limits and supporting
event ordinal. The diagnosis report used by the CLI and Web includes the same
explanation and evidence. Facts remain readable after Pod cleanup from persisted
events and the immutable execution plan. Unknown timestamps/limits are explicit.

Both resource-usage endpoints additionally expose `termination_failures`, grouped
by task, container role and effective CPU/memory/storage limits, with a
`confirmed_oom_attempts` count. These are confirmed termination counts, separate
from sampled peaks and telemetry completeness. They do not trigger automatic
resource escalation or retries.

## Capacity calibration

A bounded, explicit per-Batch request comparison may use a sampled cohort via
[the native Terminus request override](nebius-terminus2.md#per-task-measured-overrides).
Use the frozen Trial allocation when comparing limits and reservations, and report incomplete telemetry.
This comparison does not establish generally calibrated slot limits.

For fleet-wide defaults, do not change slot limits from a small smoke. Collect at least 1,000
representative trials over at least two weeks and include one 100-200 concurrent
batch. Exclude or separately report trials whose telemetry is not complete.
Group by workload class, architecture, backend, image/candidate and resource
profile. Report P50/P95/P99/P99.5 for container memory peak, conservative trial
memory upper bound, CPU usage per wall second, CPU throttled ratio, PID peak and
I/O. Also report OOM/OOM-kill counts and telemetry completeness.

For memory, a starting sizing rule is `ceil(P99.5 * 1.25)` within each workload
class, followed by a fixed-candidate concurrency validation. CPU changes must
also show that throttling and end-to-end latency remain within the accepted
service objective. Live deployment and calibration runs require separate
authorization; a merged migration does not prove collection is active.

## Rollback

Disable writers/read projections first and drain or explicitly preserve every
outbox file. Migration 0107 refuses downgrade while accounting rows exist so
evidence cannot be silently dropped. Repository rollback is not authority to
delete live telemetry or modify a deployed database.
