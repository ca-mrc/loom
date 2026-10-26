# Shared-pool validation layers

The combined development/staging/production shared pool is unlaunched. Its
handoff, migration, recovery and census simulations remain available as explicit
manual qualification. Ordinary operator and independent-environment coverage
continues in the existing CI lanes; fast activation rules run in tests-root.
No production behavior, admission gate or scenario timeout changes here.

## Manual qualification

Dispatch the CI workflow on the reviewed source with `shared_pool_simulations`
set to true. The flag is rejected for nonmanual events. Planner, shard selection
and execution use the same flag, so omitted modules never count as executed or
as reusable candidate coverage. Candidate supplementary execution excludes these
simulations. All 20 existing pool shards and their stable module assignments
remain available for manual qualification.

For a local complete manifest:

```bash
python scripts/component_ownership.py test-paths --lane nebius-pool \
  --include-shared-pool true --suite nebius-shared-pool-ops > /tmp/shared-tests.txt
mapfile -t tests < /tmp/shared-tests.txt
uv run --no-sync pytest -m "not legacy_pool" -p no:cov "${tests[@]}" -x
```

Before feature launch, finish installed multi-owner acceptance under #1915,
review actual manual-scenario cost and remove `manual_only` through a reviewed
change restoring automatic coverage. Manual tests do not authorize activation.

## Rules and connection boundaries

| Layer | Coverage | Inputs |
| --- | --- | --- |
| Fast activation record | Both result-loss boundaries for opening, release and fencing; write-ahead intent; ordered one-dispatch behavior; failed persistence; runtime refusal | Fresh in-memory storage/transaction ports exercising the unchanged production engine |
| Connected ordinary activation stage | Both transaction types in each direction, including response loss before and after commit; retained authority, malformed records and cancellation | Four representative real-journal transaction cases; the complete eight-combination rule matrix remains in fast tests |
| Connected shared activation and process observation | Installed binding, activation transport and live observer qualification | Real predecessor reads, current journals and the existing remote transport doubles |
| Forward/recovery process contracts | The complete original SQL, process and fault matrices | A case-local snapshot of fixed predecessor inputs, with current child journals and remote observations exercised normally |

`captured_predecessor_history` reads and verifies the existing predecessor pins
once for a contract case. It rejects current handoff child paths, retains read
bounds, restores the physical reader and verifies physical predecessor history
again on exit. The optional semantic projection calls the real handoff
constructor once at the boundary, stores independent snapshots of its arguments
and result, and returns independent copies for identical fixed arguments.
Changed arguments, including an in-place mutation, call the real constructor.
The production `_qualify_bindings` comparison, controller/database checks and
all current child journal/receipt checks remain active. No cross-case or
production cache is introduced.

Initial, activation and observation-only process cases keep real predecessor
construction. Census scenarios keep their original physical predecessor
construction and all fault assertions. Tests that exercise physical
predecessor replacement or permissions belong in the connected layer. The
separate helper boundary tests verify input invalidation, copy isolation, live
child changes and final physical history drift rejection.

## Long scenarios and measurement

The recovery census and preactivation recovery census retain their 1500-second
whole-test bounds and #2635's separate shard placements (20 and 4). They cannot
consume one 50-minute pool job together. The long stop-recovery fault matrix
remains complete in its existing module; opt-in does not shorten it or claim
that manual shard balance is fully solved.

Compare equivalent case/inputs in sequential isolated processes under the same
locked environment. Report elapsed and CPU time, including fixtures, alongside
constructor counts; file-open counts alone do not establish a speedup. Check
mutable journal and drift assertions on both paths. Local observations do not
establish hosted runner performance or installed acceptance.

A sequential local comparison on 2026-10-26 used the same complete forward
scenario, assertions, fault selectors, locked environment and captured file
transport; only semantic projection reuse changed. Both runs passed:

| Measurement, including fixtures | Reconstruct fixed plan | Case-local projection |
| --- | ---: | ---: |
| Elapsed seconds | 231.29 | 217.61 |
| Process CPU seconds | 230.08 | 216.64 |
| Operation fixed-plan constructor calls | 12 | 1 |
| Operation fixed-plan constructor CPU seconds | 9.65 | 0.74 |
| Binding qualification calls | 12 | 12 |

Constructor counts cover `nebius_shared_pool_operation` fixed-plan qualification,
not every fixture or observer handoff preparation.

This single local pair observed 5.9% less elapsed time and 5.8% less process CPU.
It demonstrates fewer repeated fixed-input constructions while retaining every
binding qualification. It does not establish repeatability, hosted speedup or an
improvement to the full census. Census recursive-construction cost remains a
separate prelaunch qualification task; its implementation is unchanged here.
