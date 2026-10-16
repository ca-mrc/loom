# Local pytest efficiency verification — 2026-10-16

Original verification base: `3cb457b280e57b6a6f6df85cadc4c57f59162d53`.
Local integration uses dev `4fcb12d00` plus PR #2540. The measured tree
started from `9f5a1a970` and is now aligned with its amended `c1ca4b926` head.
That alignment changed no integration-tree content: its only extra fix was the
same setup Action pin already present locally. No remote branch was changed.
The objective is submission-to-deployment latency and total validation work.

## Measured reduction

Same-machine cumulative JUnit case time, including function fixtures:

| Complete module | Cases before → after | Seconds before → after |
| --- | ---: | ---: |
| Participant delivery | 135 → 55 | 559.64 → 240.33 |
| Host / dispatch / protected runner transport | 24 → 8 | 403.18 → 148.06 |
| Participant release | 261 → 89 | 32.18 → 11.56 |
| Total | **420 → 152** | **995.00 → 399.95 (−59.8%)** |

The baseline completed all 420 cases in 996.14 seconds. Post-change module times
come from broader regression batches, rather than a synthetic sleep or fewer
assertions inside the surviving cases. Focused checks ran concurrently on the
same Mac; these are cumulative local test durations, not isolated hosted wall
clock, CPU time, a p50/p90, or a measured deployment speedup.

Across the complete participant suite, collection drops from 1,445 to 567 cases.
The 81 topology-owning attachment cases remain. Every functional test function
and every fault selector remains; repeated topology/fault combinations are reduced.

## Coverage and implementation

| Boundary | Retained verification |
| --- | --- |
| Ordinary / guest / auth target topology | All three rosters in attachment tests; connected successful release on all three. |
| Recovery, corruption and transport | Fresh complete roster by default; all original fault values and transport entry points. |
| Backup, migration and configuration jobs | All three job selectors, terminal process evidence, retry lineage and exact deletion preconditions. |
| Completion and uncertain writes | Prepare failure, lost acknowledgement, release before/after commit, immutable history and no duplicate effects. |
| Authority and isolation | Current private material, backend/UID/version drift, foreign references and forbidden write scope. |
| Retained development runtime | All 63 original tests and 182 parameter cases across two files; 79 parent/publication/input cases plus 103 runtime-stage cases. |

Shared platform/region fixtures and the external phase API double now live in
`tests/support/`. Their bodies are unchanged. Callers explicitly re-export pytest
fixtures, including names obtained through `request.getfixturevalue`. Executable
AST comparison confirmed 180 changed caller files differ only in imports. This avoids
importing unrelated renderer/stage test bodies merely to obtain fixture data.
The original component-local completion probe selected 108/112 pool modules
before and 91/113 after the initial local split. The integration keeps #2540's
additional scenario modules; the current participant-completion probe selects 93/115 modules. Shared
platform fixture edits retain all 115; the retired inventory selects zero pool
modules while its dedicated integration ownership remains. The read-only legacy inventory
selects its dedicated tests and no unrelated pool wave. Shared fixture changes
still retain the complete pool lane; unknown/full-selection safeguards are retained.

Complete modules are redistributed within the existing 20 pool shards. The long
retained-runtime module keeps #2540's split between parent/publication/input
checks and runtime stages. Two tests that enforced historical placement of skipped shared-pool
simulations were removed; disjointness, completeness and workflow failure
propagation remain executable checks. One duplicate 20-shard workflow execution
was removed after adding a fast assertion that its two pool scopes are identical.
Both distinct root scopes still execute.

The connected runtime tests now reuse a scoped, real-executable-lookup fixture.
This removes an existing host kubectl installation assumption: the unmodified
baseline failed on the same Mac. Missing trusted tools and untrusted ambient PATH
still have their original refusal checks. Subprocess transport was already doubled
by these tests; production tool lookup is unchanged.

## Verification result

**1,539 unique post-change cases have a recorded PASS**, with no uncovered target
case. This includes 648 participant/attachment cases, 182 retained runtime cases,
444 selection/manifest/candidate-reuse cases, 149 renderer/stage cases and 116
deployer cases. Real wheel construction and isolated bundle imports passed.

Darwin used locked Python 3.11.15. Canonical `TMPDIR=/private/tmp` preserved path
identity checks. A disposable Linux aarch64 Python 3.11.16 container independently
passed all 25 preparation cases, including the actual `waitid` watchdog and
selected-source rendering. Its source mount was read-only; the container was
removed and absence read back. Seven existing local HTTP regressions passed with
temporary loopback access. Dependencies retained the lock and wheel hashes.

An in-process mutation that removed recovery validation made the unchanged
negative assertion fail, as expected. Ruff, diff whitespace checks and component
ownership validation passed. Collection across 1,150 unit/integration/cluster
modules (15,145 cases) succeeded; collection is not execution of those cases.

Ignored local evidence is in `.loom/verification/pytest-efficiency/`.
`pass-origins.json` maps each passing node to its raw JUnit batch. Raw batches
that stopped at subsequently resolved host boundaries remain identifiable;
dedicated successful reruns cover those nodes. This is local verification, not
a manufactured green CI run.

## End-to-end interpretation

Completed [source CI 38921887321](https://github.com/ca-mrc/loom/actions/runs/38921887321)
and [merge-group CI 38922178538](https://github.com/ca-mrc/loom/actions/runs/38922178538)
supplied the full module-timing inventory used for placement. A replay model,
using measured local ratios for the three modules above and case-count ratios for
other reduced modules, estimates longest pool-test shards of 24.5 → 10.8 minutes
and 27.0 → 15.4 minutes respectively. Modeled cumulative pool-test work falls
176 → 131 and 173 → 134 runner-minutes. These exclude setup, allocation and other
gates; the other module ratios assume approximately linear cost and the runtime
split assumes the measured local cost proportions. They are estimates, not new
hosted measurements or PR-to-deployment forecasts.

The reduction applies before merge on both source and native-group waves. No test
is moved to candidate publication, nightly validation or deployment. Candidate
publication retains existing exact-commit native coverage reuse; image build/scan,
idle protection, backup, rollout and HTTPS/version readback retain their work.
The four native app-15368 gates and production source/dependency locks are
unchanged. The integration retains #2540's cache workflow, aligns its setup
Action with the current dev lock and triggers the writer on its own introduction. Already-skipped shared-pool simulations are not counted as saved
execution time. Additional parallel capacity is not part of the change.

Actual hosted PR-to-deployment improvement is **not yet measured**: this task did
not push, dispatch CI, publish a PR, merge or deploy. Allocation, queue rebuilding,
image publication and live guard wait can limit the wall-clock gain.

## Local integration and split comparison

The split boundary and shard placement were evaluated separately. A fair layout
comparison uses the same 182 retained cases and 55 deduplicated delivery cases
for all candidates, rather than comparing different coverage populations.
Function-scoped fixtures remain independent; no shared mutable state is introduced.

| Same-content candidate | Retained partition, measured case seconds | Modeled source / group longest pool shard |
| --- | --- | --- |
| Original local split and pins | 145 / 451 | 10.8 / 15.8 min |
| #2540 split and original pins | 293 / 303 | 17.4 / 20.7 min |
| #2540 split with recomputed pins | 293 / 303 | 8.7 / 14.3 min |

The combined tree ran all **237 cases successfully in 809.59 seconds** under
Darwin arm64, locked Python 3.11.15 and `TMPDIR=/private/tmp`, with no other
pytest batch running. Five complete modules passed: retained input/parent 79
cases (293.113 cumulative case-seconds), runtime stages 103 (302.944), delivery
48 (94.738), job failures 3 (51.336) and completion faults 4 (65.898).
Cumulative case times include function fixtures. Collection/import and reporting
account for approximately 1.6 seconds outside those case times in this batch.

Each candidate is replayed using those exact same per-case measurements; the
original local partition would place 145.430 / 450.627 seconds on its two
retained modules. This is not a claim that three separately scheduled layouts
were benchmarked on GitHub. The source/group model scales the measured fractions
by the complete historical hosted module inventory. The original measured
whole-module deduplication ratios are held fixed across layouts; other reduced
modules use case-count ratios. Modeled total pool work is identical across the
three layouts (131.3 source / 134.1 group runner-minutes), because only placement
changes. Setup, allocation, other gates, publication and deployment are excluded.
The remaining modeled group bottleneck is the unchanged image-history module.

The combined tree preserves all 63 original retained test functions exactly once.
After ignoring import extraction, 59 executable bodies are identical; the four
other functions use the already verified hermetic trusted-tool fixture. The
original local implementation and its verification evidence remain in a separate
local branch. The integration introduces no competing runtime test file.

Current dev and #2540 conflict only at cache inputs in three workflows: the
resolution keeps dev's checksum-pinned setup-uv v10 and #2540's read-only,
unpruned wheel cache. The new writer's old v8 pin failed the existing Action-pin
check; aligning that single step with the current lock passed it (13 workflows,
125 references, seven Actions). The #2540 owner independently published the same pin fix in `c1ca4b926`; the
final additive patch excludes that already delivered change. Adding the writer workflow itself to its dev
push filter lets the first merge populate the cache without waiting for a lock
change or a daily run. No workflow has been dispatched by this task.

The initial disposable merge with #2541 `7a2f28997` succeeds without conflicts.
Its later `6afbb7036` amendment touches a relocated private-reader fixture: the
local resolution retains the owner's constructor initialization in the one
existing fixture, with no duplicate definition. Its AST matches the owner head
exactly; the amended application-worker test differs only in fixture imports.
The new preactivation success path needs an explicit ordinary/guest/auth topology
parameter after the shared fixture stops multiplying every downstream test. That
small compatibility adjustment and the new PhaseAPI consumer import are kept as
a separate local handoff patch; #2541 production changes are not copied into this
optimization.

Policy and integration regressions passed **474 cases in 213.09 seconds**. They
cover complete/disjoint/fail-fast shard execution, selector fallbacks and labels,
exact-commit candidate reuse, untrusted cache-write refusal, the sole wheel-cache
writer, locked dependencies and throughput workflows. All tests passed without
changing a required gate. All-tests Ruff, Action-pin validation, upgrade policy,
four changed-workflow actionlint and whitespace checks passed. Ownership validation
covers 34 components and 1,887 tracked test files.

The two main local commands used the locked `.venv` through `uv run --no-sync`,
with the fallback Git directory first on `PATH`, `TMPDIR=/private/tmp` and
`UV_CACHE_DIR=/private/tmp/loom2518-uv-cache`:

```sh
uv run --no-sync pytest \
  tests/ops/test_nebius_development_pool_retained.py \
  tests/ops/test_nebius_development_pool_retained_runtime_stages.py \
  tests/ops/test_nebius_pool_participant_delivery.py \
  tests/ops/test_nebius_pool_participant_delivery_job_failures.py \
  tests/ops/test_nebius_pool_participant_delivery_completion.py \
  -q --tb=short --durations=20 --junitxml=integrated-layout-b.xml

uv run --no-sync pytest \
  tests/ops/test_component_ownership_manifest.py \
  tests/ops/test_ci_lane_selection.py tests/ops/test_ci_test_selection.py \
  tests/ops/test_nebius_candidate_test_plan.py \
  tests/ops/test_ci_secret_isolation.py tests/ops/test_uv_lock_contract.py \
  tests/ops/test_ci_throughput_workflows.py \
  -q --tb=short --junitxml=integrated-policy.xml
```

Their raw JUnit files are retained under the ignored evidence directory; the
filenames above shorten only the output location. The full 237-case timing run
completed before the other pytest batches started. The layout replay script and
historical hosted inputs are retained there as well.


The #2541 compatibility run used its own combined source tree on `PYTHONPATH`,
confirmed from imported module paths, the same locked interpreter and canonical
temporary root, and explicit `LOOM_RUN_SHARED_POOL_TESTS=1`:

```sh
python -m pytest tests/ops/test_nebius_pool_participant_release.py \
  tests/ops/test_nebius_shared_pool_handoff.py \
  -k 'participant_release or preactivation or completed_catalog_runtime_inventory or shared_v2_uses_completed_catalog' \
  -q --tb=short --junitxml=pr2541-compatibility.xml
```

This produced **104 PASS and two host-tool failures in 288.28 seconds**. Both
failed in the existing trusted-system `kubectl` lookup before their doubled
remote exec boundary. They now explicitly consume the same function-scoped,
real-executable-lookup fixture as the retained probe tests; production lookup,
all original business assertions, and timeouts are unchanged. Their exact rerun
passed **2 cases in 173.32 seconds**:

```sh
python -m pytest \
  tests/ops/test_nebius_shared_pool_handoff.py::test_completed_catalog_runtime_inventory_keeps_original_roots_and_exact_successors \
  tests/ops/test_nebius_shared_pool_handoff.py::test_connected_preactivation_preserves_staging_and_qualifies_actual_closed_successor \
  -q --tb=short --junitxml=pr2541-tool-recovery.xml
```

Thus all 106 selected compatibility identities have actual PASS evidence across
those runs; this is not described as one wholly green 106-case batch. The three
topology-success cases are explicitly present. The latest owner private-reader
fixture also passed **24 cases in 1.11 seconds** with `pytest
 tests/ops/test_nebius_development_pool_retained.py -k private_input -q`.
The unrelated application-worker test body was preserved exactly rather than
re-testing or claiming its owner's database-timing change as this optimization.
No installed shared-pool operation, provider, deployment or protected resource
was exercised. The temporary compatibility checkout is removed after saving its
local proof commit and handoff patches.
