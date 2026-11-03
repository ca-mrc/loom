# Batch execution failure remediation (#2768)

This repair follows the November 3, 2026 investigation of batch
`5ec04ae6-989e-4d39-8d56-8bb3b16a977d`. The incident ran candidate
`a2f1ad7278dcb432f2a19d17ae2e4ffc22c94583`; the measurements below are dated
evidence, not current installed acceptance. Counts overlap across causes.

## Platform changes and remaining attribution

| Boundary | Repair and acceptance | Remaining evidence |
| --- | --- | --- |
| Internal ledger | Select accounting columns without raw provider logs; skip unchanged/already joined trajectory reads; retry a transient call-ledger read on the background observer, before a command is recorded, and at final reconciliation. | Real PostgreSQL/HTTP scope, generation and revocation tests; deployed latency and CPU still need measurement. |
| Trial/lease locks | Acquire Trial before lease across observations, cancellation, output, dispatch and materialization. | A deterministic concurrent cancellation/observation regression fails before the change and passes after it. It proves a concrete lock cycle, not the identity of all 28 historical deadlocks. |
| Dispatch audit | Keep durable admission before transport and the existing one-second audit deadline. Measure checkout, policy, capacity, execute and commit awaits separately; expose fixed SQLSTATE categories without driver text. | Correlate any remaining admission/observation failures with database wait and CPU measurements. Audit failure never authorizes provider transport. |
| Retry budget | Bound subsequent sends by the remaining retry budget; retain the configured initial-send timeout and signed attempt deadline. Budget exhaustion has its own cause. | Provider request IDs still require upstream correlation to distinguish supplier, proxy and network delays. |
| Separate verification | Grade a safely committed main-phase timeout exactly once; preserve execution timeout independently of numeric reward, including zero. Retain capacity-blocked work instead of failing it solely because it waited 30 minutes. | No historical task is automatically retried or regraded. Task-authored scoring failures remain separate. |
| Startup/verifier diagnosis | Name the internal startup operation and safe transport phase; retain numeric verifier exit code before secondary collection/cleanup failures. | OOM workload allocation and tmux process disappearance need their original task/process evidence; existing runtime diagnostics remain authoritative. |
| Usage/cost | Missing usage is unknown, partial usage reduces confidence, and incomplete totals are labelled known subtotals. | Provider invoices and missing vendor usage cannot be reconstructed as a confirmed bill by a code change. |

A running sandbox keeps its model token and its call-ledger read when the only target fault is a stale health timestamp. Dispatch and final reconciliation retry that transport failure. New sandbox admission, including a separate verifier, still requires a fresh health report. See [in-flight sandbox authority](../architecture/inflight-sandbox-authority.md).

The stored pool `attempt_timeout_sec` is intentionally not the initial send
cutoff following #2758. Restoring a blanket 20-second cutoff would reject valid
slow generations. The actual initial transport policy and signed deadline are
described in [provider onboarding](../integrations/provider-onboarding.md).

### Admission contention and uncertain commits

The authorized full475 rerun exposed additional internal audit timeouts after
deployment. Distinguish `gateway_dispatch_admission_failed` with the fixed
`error_category=timeout` from caller cancellation, upstream transport504 and the
signed agent deadline. A cancelled phase histogram alone does not establish
which one occurred or prove CPU/lock contention as the unique physical cause.

Stable trial bindings no longer take an exclusive lock for the entire pool.
They retain a shared pool lock and an exclusive binding lock until receipt
commit; first assignment and pool writers remain exclusive. Concurrent real
PostgreSQL regressions exercise unrelated sticky trials, first-assignment
round-robin, configuration writers and a stale binding hint. This removes a
reproducible serialization defect while preserving live admission checks and
the one-second audit budget.

An audit commit acknowledgement can time out after PostgreSQL has committed
the receipt. Provider I/O has not started in that path. The known receipt ID is
retained for bounded `not_dispatched` cleanup and independent HTTP status
observation. This prevents a confirmed cleanup from leaving a false open
generation in the connection cap. If cleanup also fails, preserve the unknown
outcome and sanitized log; do not edit historical rows or blindly resubmit.

Local concurrency and lost-acknowledgement regressions do not establish that
every historical timeout had the same cause. Track each published candidate
and its own installed acceptance separately from an already running frozen
batch; never replace that batch's runtime profile during execution.

## Optimize before expanding resources

Keep the existing CPU/memory requests and limits, database connection cap,
`work_mem`, node limits and provider quotas while qualifying this change.

The original approximately 12-second sample showed PostgreSQL using 2.946 of
its three CPU cores, with throttling in 81.6% of scheduling periods. Two short
database snapshots had 46/47 connections below the 100-connection cap and no
blocked backend. Historical deadlocks are a separate concurrency defect:
additional CPU does not correct lock order.

The first reduction is work removal, not a longer timeout: no full accounting
refresh on unchanged polls and no transferring/deserializing raw request logs.
The local 40-call equivalent-read comparison transferred 664,640 versus 920
bytes of extras JSON; ten alternating measured reads after warmup had median
full-ORM/serialization time 2.076 ms versus 1.166 ms for the projection. This
synthetic local comparison is reproducible in
`tests/integration/test_service_execution_call_ledger.py`; it is not a hosted
three-core capacity result. See [runtime architecture](../architecture/terminus2-runtime.md)
for the unchanged-poll and failure-handling regressions.

After a separately authorized deployment, use an ordinary comparable workload
and record concurrency, completed throughput, CPU usage/throttling, ledger
latency, audit failure counts, database await histograms, lock waits and
verifier queue age. Use the existing one-second audit and 15-second ledger
deadlines as correctness limits; a higher timeout is not acceptance of overload.
Do not submit paid model calls solely to create a measurement count.

### Conditional sizing advice

| Resource | When expansion is justified after optimization | Suggested sizing |
| --- | --- | --- |
| PostgreSQL CPU | Sustained comparable load still saturates three cores and causes CPU throttling plus audit/ledger latency, after hot-query and lock fixes. A short incident sample alone is insufficient. | Estimate cores as `ceil(measured sustained optimized CPU / 0.70)`. Four cores are a reasonable first candidate **if** demand remains near the observed 2.946 cores (about 74% utilization at four). Confirm node headroom, throughput and latency before committing a larger allocation. |
| PostgreSQL memory/connections | Expand only if optimized-load evidence shows memory pressure, spills or connection queuing. | No present evidence supports raising the 2 GiB limit, 100 connections or 4 MiB `work_mem`. Raising connections or per-query memory can worsen CPU/memory contention. |
| Gateway CPU | Per-replica saturation and latency persist after ledger/query work removal; distinguish database waiting from Gateway compute. | If the sampled near-one-core demand persists, evaluate two cores per affected replica or an extra replica using actual node headroom and database load; do not scale every component together. |
| Verifier capacity | Durable queueing works, but arrival rate exceeds sustainable grading throughput or queue latency is unacceptable after existing pool capacity is reused. | Measure arrival rate, verifier duration and per-verifier CPU/memory. Required concurrency is approximately arrival rate × mean duration, plus measured burst headroom. Compare its resource envelope to the pool/node/quota limits before requesting more nodes or vCPU quota. |

Expansion is a recommendation, not an applied configuration. Preserve existing
workspaces, output commits, cancellation fences and cleanup ownership. Local
tests, protected CI and merge do not prove deployed repair or supplier recovery.


### Missing native verifier reward

A missing numeric reward is a failed verification boundary, not a zero score.
The native private-input bridge records the original `test.sh` exit code beside
`harbor_reward_error`; later report-download or cleanup errors remain secondary.
On an existing verifier exec/report failure, the controller first quiesces the
sandbox, then attempts to retain `/logs/verifier` before teardown. Successful
verification, including reward zero, does not perform this capture.

The optional private outputs `diagnostics/verifier-files.tar` and
`diagnostics/verifier-files.json` preserve regular reports with an 8 MiB archive
budget, 1 MiB per file, 128-entry limit and three-second capture timeout inside
the existing cleanup grace. The existing native archive/file RPC owns transport;
no unbounded legacy fallback is used. Parent symlinks, archive traversal, links
and special files cannot become retained files. Oversized, missing or unsafe
evidence is explicitly partial/unavailable. Capture failures never turn a
failed verifier into success or override its primary error. These are optional
artifacts under ordinary Trial authority, not another gate or scoring input.

The two original 475-task rerun samples `24f97b1e-6375-4a78-a932-36b3ed65bf74` and
`6baed40c-8f82-4239-9de9-6e2c87a5c3c0` retain a missing reward and generic
exit-one wrapper failure. Their task wrappers deliberately remove rewards on
internal error, but the underlying reports were not retained; their deeper
initiating causes cannot be recovered from those exported bytes. This source
fix does not reconstruct historical reports, change task scripts, regrade or
rerun tasks. It is not deployed or accepted by the 475-task cohort frozen on `7badcb61b784`.
