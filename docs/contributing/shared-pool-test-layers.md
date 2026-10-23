# Shared-pool validation layers

Shared handoff remains reachable through `nebius_shared_pool_entry.py` and the
supported shared-pool installation entrypoint. An idle independent Nebius
environment does not establish that this feature, its terminal recovery or its
historical records can be retired. This pilot removes no product capability.

## Rules and connections

The ordinary and shared parents already use the same production
`advance_activation_record` engine. Test it directly instead of constructing a
complete installed platform for every transaction-result combination.

| Layer | Coverage | Input boundary |
| --- | --- | --- |
| Fast-root activation record | Opening, guard release, pool/guard fencing; response loss before and after commit; ordered one-dispatch behavior; intent-save failure; runtime refusal; terminal cancellation | Fresh in-memory storage and remote-transaction ports per case. The production engine and choice functions are unchanged. These rules run before the heavy pool lane. |
| Ordinary activation stage | Both transaction types in each direction connected to real private journals; all existing retained-authority, startup, malformed-record and cancellation-entry cases | Real journal readers/writers and the original remote API double. Four representative transaction cases replace the repeated eight-case matrix; all eight combinations remain at the rule layer. |
| Forward/recovery process contracts | The existing process, SQL, readiness and late-drift fault matrices, with every selector and assertion retained | Completed predecessor files captured separately for each case. Current child journals and all remote-observation behavior remain live within the test. |
| Connected process/activation | Initial binding and the concrete activation transport, including unknown opening/release replies and refusal after completion | Real predecessor reads and real current journals, with the existing Kubernetes/SQL transport doubles. Observation-only and recovery-process connections retain this boundary too. |

The contract input double is limited to paths already pinned by the genuine
predecessor history. It validates the captured bytes using those existing pins,
refuses any current handoff child path, preserves read bounds and rechecks the
physical predecessor history after restoring the real reader. It is neither a
production cache nor a substitute for live qualification. No mutable state is
shared between test cases.

Physical private-file protections remain covered by the certificate/storage
tests and the connected history/entrypoint suites. New process or local-receipt
faults belong in the contract layer; faults involving predecessor replacement,
permissions or history identity must use the physical connection layer.

The process suites use the existing `system_kubectl` fixture. It mounts trusted
search directories inside the test sandbox and retains real executable lookup;
the separate untrusted/missing-tool tests still exercise refusal. Tests no longer
depend on the workstation installing kubectl in a privileged directory.

## CI selection

`ci_runner_supply.py` is an explicitly owned live controller, not a read-only
reporting exception. Its changes retain root supply/cancellation/graph contracts
in the dedicated `ci-runner-controller` suite and native image/cluster/staging
qualification. Pure controller changes do not select application integration or
Docker integration. They no longer force unrelated platform
pool lifecycle modules merely because the script was previously unclassified.
Mixed changes retain the actual pool operator's consumers; shared inputs and
unknown tools keep their conservative fallback.

The existing four native merge checks, fail-fast behavior, 20 pool shards and
pre-deployment validation positions are unchanged. This pilot adds no gate,
runner capacity or nightly-only replacement coverage.

## Qualification

Compare the same forward case, assertions and fixed code in two sequential
processes: the physical predecessor reader and the contract reader. Include
fixture cost, use the same locked environment and report both elapsed time and
actual file-open calls. Full-suite, cloud and PR-to-deployment improvements need
their own measurements; a local reader comparison does not establish them.

Run the four process cases, activation record/stage suites, trusted-tool refusal
cases and CI selection regressions before publication. Temporary mutations which
omit opening-intent persistence or retry an unknown opening must be detected by
the fast rule tests; they are verification experiments, not additional CI gates.

Local macOS arm64 / locked Python 3.11.15 qualification on 2026-10-23:

| Measurement | Physical input reader | Contract input reader |
| --- | ---: | ---: |
| Same forward case, cumulative case time including fixture | 271.05s | 180.90s |
| Same forward case, real `os.open` calls | 6,748,580 | 631,675 |

These were sequential runs of the same assertions and code under test, not a
modeled shard rebalance. The case-time reduction is 33.3%; actual file opens fall
90.6%. Both include real mutable child journals and all original faults. The
connected activation path retains physical input reads.

The original four-case batch passed in 793.08s, and the layered batch in 634.76s.
The initial batch briefly overlapped selector checks, so the focused forward
comparison above owns the performance claim. Neither result is hosted CI or
PR-to-deployment acceptance. The original batch delegated all 9,465 `fsync`
calls to the OS: their combined measured cost was only 0.19s on this host, so no
fsync bypass was added.
