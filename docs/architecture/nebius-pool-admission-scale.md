# Global-pool admission under large build backlogs

The management request journal remains the resource ledger. A local queued
materialization is not a management waiter: participants prepare their retained
outbox requests incrementally. The management `waiting` phase means that the
request was accepted durably, has not acquired capacity, and can be retried with
the exact same body. A waiting response neither claims a build attempt nor gives
permission to create a workload.

## Admission work

Preparation retains the global mutation lock and all current authority,
registration, profile, observation, resource-placement and provider-quota
qualification. Already charged requests stay charged through cancellation,
expiry and cleanup until release is confirmed. Eligible fitting work retains
production, staging, shared development, personal development, then age ordering.

Large backlogs no longer make a fitting production request fail merely because
more than 10,000 renewed development requests exist. Rather than loading every
waiting workload JSON and rejecting that count, preparation streams only the
priority/age prefix that can protect capacity from its caller. Envelopes are
buffered in groups of 100; payloads are loaded and rendered only for physically
fitting candidates. A stale, invalid, or impossible waiter never becomes a
protected promise. There is no total-page cutoff: an impossible prefix cannot
permanently hide a fitting tail.

Each transaction reuses profile resolution for a pool, participant and retained
revision (at most 128 memo entries), and fit results for equal resource shapes
under the same pool, workload kind, charged set, fixed observation and protection
generation (at most 512 entries). An observed candidate retains its exact demand
identity in the fit key because placement can discount already observed Pods.
Adding a protected waiter changes the fit key. Memos never survive the transaction,
so later node, profile, origin, admission or capacity changes are requalified.

Build counts, creation rate, pending counts and unhealthy-job limits are necessary
bounds: adding protected waiters cannot improve them. An exhausted bound returns
a normal `PoolWaitingV1` reason immediately, after qualifying the caller, preserving
its original age and exact retry identity. The same check ends a prefix scan once
qualified earlier promises exhaust such a budget. Physical best-fit placement is
still evaluated in full; no general monotonicity assumption is made about packing.

The charged-request safety bound remains 200,000. Exceeding it returns
`pool_admission_charged_backpressure` without making a grant, dropping a charge,
or deleting the accepted waiting request. Participants retain and retry the same
outbox entry. Creation and pending policies continue to bound capacity promotion;
backlog size itself is not permission to create more Jobs or reserve warm nodes.

This bounds buffering, memo state and redundant work, not worst-case lock time
for every possible malformed backlog. A prefix containing many distinct invalid
or impossible shapes may still require linear envelope/qualification work. Such
work is observable rather than silently truncated. A persistent qualification
cache or replacement scheduler is not introduced.

## Gateway progress

Each gateway pass selects at most 100 cleanup effects and 100 create effects,
with at most eight reconciliations in flight by default. Cleanup has an independent
batch and is selected first. The existing gateway journal still authorizes every
exact effect and recovers ambiguous writes; concurrent workers do not acquire a
second write authority.

Separate phase cursors rotate through a fixed high-water key. New arrivals are
left for the next rotation. Reaching the endpoint resets the cursor even if new
arrivals continuously exceed processing throughput, so old failed effects are
retried. Cursors are scheduling hints: losing them on restart safely replays
retained work through the journal. A phase transition behind a cursor becomes
visible in its next rotation. Only cleanup evidence releases a reservation.

This follows familiar controller practices: finite worker counts, bounded work
per pass, retryable items and durable state independent of worker lifetime.
[Kubernetes client-go workqueues](https://pkg.go.dev/k8s.io/client-go/util/workqueue)
and [Python's bounded asyncio queues](https://docs.python.org/3.11/library/asyncio-queue.html)
are references for those practices, not added runtime components. Existing pool
priority and age rules remain the admission policy.

## Measurements and local verification

`loom_pool_admission_seconds{phase="lock_wait"}` measures mutation-lock acquisition.
`phase="locked_prepare"` measures preparation after acquisition, excluding the
caller's later commit. `loom_pool_admission_work_total{operation=...}` counts
waiter envelopes, payload reads, registry profile resolutions, renders, fit
evaluations and policy checks. These metrics have no per-request identity labels.
Together with HTTP latency and database metrics they distinguish lock contention,
backlog traversal and expensive qualification. They do not measure participant
renewal latency or imply a deployed throughput result.

The deterministic PostgreSQL integration tests seed 1,000 and 10,000 renewed
waiters while preserving the real schema and admission transactions. On the
original implementation, 10,000 development waiters caused a fitting production
prepare to raise `PoolAdmissionError` when its own row crossed the old count
boundary. The new path grants it without reading younger-class payloads. A
same-class impossible prefix still reaches the fitting tail, with two fit
evaluations, one render and one profile resolution at both tested sizes. One local
sample took 0.0293 seconds for 1,000 rows and 0.1344 seconds for 10,000 rows;
these are single-machine samples, not percentile estimates or production claims.
The thin prefix scan remains linear.

Coverage also includes protection-generation invalidation, observation rejection
of ungranted reservation identities, invalid retained envelopes, retry with changed
capacity and unchanged age, gateway bounded concurrency, independent cleanup
progress, cursor rotation under continuous arrivals, restart replay, and existing
concurrent-grant/ambiguous-create/cleanup-before-release regressions. No live or
paid stress workload is launched by these tests.
