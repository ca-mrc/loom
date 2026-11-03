# Batch execution failure remediation (#2768)

This repair follows the November 3, 2026 investigation of batch
`5ec04ae6-989e-4d39-8d56-8bb3b16a977d`. The incident ran candidate
`a2f1ad7278dcb432f2a19d17ae2e4ffc22c94583`; the measurements below are dated
evidence, not current installed acceptance. Counts overlap across causes.

## Platform changes and remaining attribution

| Boundary | Repair and acceptance | Remaining evidence |
| --- | --- | --- |
| Internal ledger | Select accounting columns without raw provider logs; skip unchanged/already joined trajectory reads; retry transient background reads without cancelling the healthy session. Dispatch and final synchronization remain strict. | Real PostgreSQL/HTTP scope, generation and revocation tests; deployed latency and CPU still need measurement. |
| Trial/lease locks | Acquire Trial before lease across observations, cancellation, output, dispatch and materialization. | A deterministic concurrent cancellation/observation regression fails before the change and passes after it. It proves a concrete lock cycle, not the identity of all 28 historical deadlocks. |
| Dispatch audit | Keep durable admission before transport and the existing one-second audit deadline. Measure checkout, policy, capacity, execute and commit awaits separately; expose fixed SQLSTATE categories without driver text. | Correlate any remaining admission/observation failures with database wait and CPU measurements. Audit failure never authorizes provider transport. |
| Retry budget | Bound subsequent sends by the remaining retry budget; retain the configured initial-send timeout and signed attempt deadline. Budget exhaustion has its own cause. | Provider request IDs still require upstream correlation to distinguish supplier, proxy and network delays. |
| Separate verification | Grade a safely committed main-phase timeout exactly once; preserve execution timeout independently of numeric reward, including zero. Retain capacity-blocked work instead of failing it solely because it waited 30 minutes. | No historical task is automatically retried or regraded. Task-authored scoring failures remain separate. |
| Startup/verifier diagnosis | Name the internal startup operation and safe transport phase; retain numeric verifier exit code before secondary collection/cleanup failures. | OOM workload allocation and tmux process disappearance need their original task/process evidence; existing runtime diagnostics remain authoritative. |
| Usage/cost | Missing usage is unknown, partial usage reduces confidence, and incomplete totals are labelled known subtotals. | Provider invoices and missing vendor usage cannot be reconstructed as a confirmed bill by a code change. |

The stored pool `attempt_timeout_sec` is intentionally not the initial send
cutoff following #2758. Restoring a blanket 20-second cutoff would reject valid
slow generations. The actual initial transport policy and signed deadline are
described in [provider onboarding](../integrations/provider-onboarding.md).

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
