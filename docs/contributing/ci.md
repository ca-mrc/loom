# Nebius CI on dev

`dev` uses Nebius as its only hosted platform and remains the pre-main branch.
The earlier `codex/nebius-main` CI supplied path routing, optional Python coverage
and a smaller runtime scope. Daily dev CI now follows the same Nebius runtime
boundary while retaining the four protected admission contexts.

## Workflow inventory

The repository retains the workflows below. Low run frequency or no recorded runs
does not make an operator entry point obsolete: promotion, retry, and catalog
publication run when needed. In particular, `main-promotion-gate` remains the
required check protecting promotion into `main`. The GitHub-managed Dependency
Graph entry has no workflow YAML in this repository and is maintained separately.

| Category | Workflow file | Purpose and triggers |
| --- | --- | --- |
| Required CI | `ci.yml` | Repository checks on PRs, merge groups, and `main` pushes; full daily regression at 08:23 UTC; manual validation and compatibility checks. |
| Required CI | `images.yml` | Read-only Nebius AMD64 image builds and scans on PRs, merge groups, and manual dispatch. |
| Required CI | `cluster-smoke.yml` | Credential-free Kubernetes and pinned-Skopeo disposable TLS-registry contracts on PRs, merge groups and manual dispatch. The merged SHA does not repeat on a dev push. |
| Required CI | `staging-smoke.yml` | Credential-free Compose system checks on PRs, merge groups, and manual dispatch. |
| Contributor compatibility | `macos-locked-environment.yml` | macOS ARM64 locked workspace installation, daily at 09:17 UTC or manually. |
| CI operations | `ci-retry.yml` | Manual bounded, classified retry by default; an explicit read-only capacity branch supplies disposable single-job hosted workers. |
| CI operations | `ci-dependency-cache.yml` | Writes the full uv wheel cache and the source-built MinIO fixture archives on relevant `dev` pushes, daily at 07:41 UTC, or manually; PR and merge-group jobs only read them. |
| Dependency maintenance | `harbor-upstream.yml` | Discovers Harbor drift daily at 09:41 UTC or manually. Draft candidate creation requires repository activation and is skipped by manual `check_only=true`; see [Harbor upgrades](terminus2-upgrades.md). |
| Production promotion | `release-promotion-gate.yml` | Manual validation of release evidence for an exact candidate SHA and image. |
| Production promotion | `main-promotion-gate.yml` | Manual verification that the open `dev` to `main` PR, current candidate SHA, and successful release evidence agree. |
| Nebius publication | `nebius-candidate.yml` | Automatic `dev` push publication or manual platform/Harbor runtime publication, controlled by `NEBIUS_RELEASE_ENABLED`. |
| Nebius deployment | `nebius-rollout.yml` | Application rollout after successful candidate publication or manual dispatch, controlled by `NEBIUS_AUTO_ROLLOUT_ENABLED` and the active-task guard. Separate protected manual operations provide read-only inventory, private certificate qualification, owned ingress installation/paused recovery, fixed wildcard/management DNS publication, and private dev foundation preflight/installation under the same concurrency. Development uses its own operation and digest-bound SSH key; it cannot invoke staging/management commands. Certificate and ingress operations each use dedicated forced-command SSH identities bound to exact tooling, not the Kubernetes-only deployment key. Only ingress installation scans/publishes the pinned image and receives registry credentials; recovery and DNS skip publication. DNS credentials remain on the gateway. |
| Nebius maintenance | `nebius-image-retention.yml` | Daily retention at 07:23 UTC or manual preview/apply; enabled by `NEBIUS_RELEASE_ENABLED`, with scheduled deletion separately controlled by `NEBIUS_IMAGE_RETENTION_APPLY`. |

The four CI workflows report protected gate contexts for eligible PR and merge
group runs; manual validation reports separate `*-manual` contexts. Their
changed-path planner controls which expensive jobs run. Candidate publication,
live rollout, and release promotion each retain their own authorization and
evidence checks.

The four development validation workflows can share a native execution queue
across isolated OLDLAB guests and disposable GitHub VMs. See
[runner supply and shared-host limits](ci-unified-runners.md); this execution
placement is independent of affected-test selection and protected admission.

The same protected rollout concurrency also serializes manager installation,
one-time upgrade, retained-operation recovery and
[repeatable software refresh](../runbooks/nebius-deployment.md#refresh-the-retained-application-manager).
Refresh uses dedicated protected metadata and a separate exact-bundle SSH key;
`management-refresh-preflight` and `management-refresh-install` cannot fall back
to another management authority. These are installed-operation entry points, not
PR validation or evidence that the development environment is already accepted.

The independent dev manager uses a separate manual-only job in this same queue:
`development-management-preflight` and `development-management-install`, with
`NEBIUS_DEVELOPMENT_MANAGEMENT_OPERATION_JSON` and dedicated
`NEBIUS_DEVELOPMENT_MANAGEMENT_SSH_KEY`. Its exact integrated-source checkout and
digest-bound bundle target only `loom-nebius-management-dev`; legacy manager and
foundation authorities are unchanged. The job has contents-read permissions,
uses the protected `nebius-integration` environment without recording a staging
deployment, removes its ephemeral key, and uploads only bounded outcome evidence.

Its certificate successor uses the separate `development-management-renewal` job,
with `development-management-renewal-preflight` and
`development-management-renewal-renew`. Dedicated
`NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON` and
`NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_SSH_KEY` pin the renewal bundle and operation;
there is no initial-manager-key fallback. It shares the same protected queue and
contents-read, dev-only, manual-dispatch restrictions. A successful renewal report
is certificate-delivery evidence, not multi-owner execution acceptance.

The same job and concurrency carry `management-pool-preflight`,
`management-pool-install` and `management-pool-rollback`. They require dedicated
`NEBIUS_MANAGEMENT_POOL_OPERATION_JSON` metadata and
`NEBIUS_MANAGEMENT_POOL_SSH_KEY`, with no other-key fallback. The metadata pins
the exact integrated tooling/candidate and private cutover operation; rollback
uses a separate fixed SSH command allowed only for that pool grant. A terminal
cutover receipt is migration evidence, not multi-owner acceptance.

## Retained validation

All four protected contexts remain required: `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate`. The shared planner selects their
work; selected failures, cancellations and unexpected skips still block merging.
Draft filtering, current-head/base requirements, and native squash auto-merge
retain the [repository contract](../../CONTRIBUTING.md).

The manifest retains ownership of both historical and Nebius tests. Daily dev CI
selects Nebius and common contracts; historical compatibility is manual. PR image CI uses the same seven-image Nebius set as candidate publication,
builds only affected AMD64 images and scans the resulting artifacts. Disposable
Kubernetes checks include the Nebius platform and execution contracts. Candidate
publication uses the protected `nebius-integration` Environment from `dev`;
PR checks do not receive its credentials. CI does not deploy the live platform.

## Public image pulls in validation

The protected Docker consumers (root tests, runtime payloads, both integration
tiers, cluster and staging smoke, and ordinary/Harbor image builds) configure
`https://mirror.gcr.io` on their disposable Linux Docker daemons
before pulling or building. `scripts/configure_ci_registry_mirror.py` preserves
existing daemon settings and mirror priority, validates the merged configuration
before installation, restarts the runner daemon, and checks the active mirrors.
It accepts GitHub-hosted Linux runners or disposable self-hosted Linux guests
with the root-owned ephemeral-runner marker. It refuses unmarked hosts,
non-Linux runners, and remote-Docker execution.

The separate image BuildKit containers use `config/ci-buildkit.toml` by default;
an existing readable `/etc/buildkit/loom-ci.toml` retains precedence and is not
modified. Both pull paths retain every original image reference and digest.

The pinned `setup-uv` action uses v10.0.1, which handles transient manifest
transport failures with at most three attempts and stops reading the version
list after the selected version. This addresses the observed single five-second
manifest lookup failure before Python or pytest starts. uv remains 0.11.26 with
the existing platform checksums; locked installs and PR/merge-group cache-write
restrictions are unchanged. Test jobs use the full, unpruned wheel cache
described below. A persistent manifest error still fails setup; this is
not a workflow or failed-test rerun policy.

The public Google mirror serves cached Docker Hub images over TLS, without
credentials, publication authority, or shared cache writes. Digest-pinned pulls
still verify the requested content; existing tag-only fixtures remain tag-only.
Mirror coverage is not guaranteed: Docker and BuildKit may fall back to Docker
Hub, and a mirror miss followed by an upstream quota failure still fails the
original build or test. This reduces anonymous Hub traffic; it does not bypass
validation gates or guarantee upstream availability.

## Nebius path routing

Static Python checks run for Python changes and ignored retired inputs. Frontend-only changes do not start
Python lint or test, Go, runtime-payload, dependency-lock, Terraform, Compose or Kubernetes
runners. The Compose system fixture has no Web service; browser/auth changes are
covered by the frontend job. Backend/frontend mixed changes and explicit smoke
labels still select the corresponding checks. Independent
test edits start their owning test jobs. Shared and unknown inputs retain the full
baseline; Terraform runs for its own inputs and full-regression requests. Terraform-only
changes use the IaC checks without Python lint, root/package tests, or integration.
Manifest-ignored retired inputs do not allocate backend test runners; static
validation still runs. Additional
heavy validation follows the changed files:

| Changed file | Additional validation |
| --- | --- |
| Frontend source, Web Dockerfile, Nginx SPA config or browser runtime config | Web type/lint/unit/coverage/browser checks and affected images |
| Platform renderer source | Integration, affected images, Kubernetes |
| Independent platform renderer unit tests | Kubernetes |
| Deployment/render operator scripts and `deploy/nebius/` configuration | Integration, Kubernetes |
| Restore verifier operator script | Integration |
| Nebius Terraform | IaC checks |
| Nebius IaC checker or its tests | Integration, the IaC checks |
| Action-pin, upgrade-policy and CI selection tooling | Python root contracts and static checks |
| Retained Go runtime modules | Go checks and their affected Docker/image contracts |
| Candidate workflow/publisher, registry authentication, shared scan validator | Full validation |
| Unknown runtime or operator files | Full validation |

Mixed changes take the union of their requirements. CI labels can add checks and
cannot subtract path-selected checks. This avoids treating known deployment
files as unknown code while retaining full validation for publication authority.
The actual tests are selected from `config/component-ownership.toml`.

## Coverage collection

Python root/package/integration tests normally run without coverage instrumentation.
Request `ci:coverage-summary` or dispatch CI with `coverage_summary=true` for
full selection within the active scope and combined accounting. Nebius coverage
is reported without applying the historical all-platform **70%** floor to a
different test population. The floor still applies in compatibility/main scope.
Coverage collection/reporting errors still block the repository gate. Frontend
coverage thresholds remain separate. No p50/p90 speedup is claimed before live
measurement; the removed instrumentation and artifact work is deterministic.

## Test-only changes

An edit consisting only of Python test modules runs those files and their known
transitive test-module consumers in their owning lanes. Imports and string
references retain consumer coverage; runtime, shared-helper and unknown consumers
retain the complete lane. Exact path literals in the
audited CI metadata tests do not make a test a shared fixture. Shared or deleted
test inputs select their possible consumer jobs as well as retaining all files
inside those jobs, including when mixed with source changes. Runtime,
migration, configuration, fixture, deleted-file and unknown changes stay full
unless a suite explicitly declares an audited unaffected component.
CI selector labels and manual runs request full regression within the active scope; ordinary labels such
as `bug` do not change test selection. Sharding happens before filtering,
so ownership and paired-fixture ordering remain stable. The planner emits only
nonempty root/pool/integration/Docker/cluster shards before runners are allocated. Each
runner writes its selected-path manifest before dependency or fixture setup;
selector errors fail the job. Empty selections do not invoke pytest.

## Component selection across CI

The October 15 audit covered all 12 workflows and 49 job definitions, including
the seven scheduled/manual release, maintenance and retry entry points outside
the four source workflows and candidate publication. The GitHub-managed
Dependency Graph has no repository YAML.

| Changed inputs | Selected work |
| --- | --- |
| Documentation | Planning and the four stable gate reports; no image build for README-only metadata. |
| Python unit/integration/browser test modules | Owning validation and transitive test consumers; no product-image rebuild. Declared runtime payloads still own their conformance images. |
| Archive/lease/Trial recovery CLI | Root and integration lifecycle/archive/idle regressions; no unrelated pool transitions, Go, Terraform or image wave. |
| Component-local pool operator | Owning pool consumers; shared helpers, generators, dependencies and selection changes retain full regression. |
| API-only change | Backend regressions, actual Docker COPY consumers and generated API verification. Unchanged frontend inputs do not repeat Vitest/build/browser work after the schema check passes. |
| Frontend/runtime dependency | Existing TypeScript, global coverage, production build, browser and relevant image checks. |
| Go command | Its package with vet/race tests; guest-channel changes include both importing commands. Shared module or cross-language contracts retain all Go packages. |
| Terraform module | Its mock tests and consuming stack/platform validation. Shared/provider/unknown inputs retain every target. |
| Unknown or shared authority | Complete applicable regression, with a full file-selection hint. |

All selected source results still feed the same four app-bound required gates.
Nightly/manual and explicit full requests preserve stronger coverage. Global
Python type checks, frontend coverage populations and broad Docker COPY inputs
are real dependencies; ownership globs cannot make those inputs disappear.
Selection failures propagate before pytest or toolchain execution. The audit's
native CI sample took 41m06s (109.47 runner-minutes, longest job 12m20s), with a
30m08s ready-to-start delay for one root shard. These are baseline measurements,
not a claimed post-change hosted speedup.

## Pool operator tests run before deployment

The `nebius-pool-ops` suite (`tests/ops/test_nebius*pool*.py`) took about 450 of
the ~500 tests-root runner-minutes. It runs in its own `nebius-pool` lane with 20
shards sized for its 15-30 minute lifecycle modules, so `tests-root` needs only 4
shards (~7 minutes of pytest each). A PR or merge group runs it only when a change
touches its `affected_paths` (operator scripts, Kubernetes and Nebius deploy
inputs, CI authority files, dependencies), its own tests, or a test helper they
import. Selection is made once for the whole lane and then sharded, so planned
shards and job manifests always run the same set. Other changes skip it before
merge. Component-local pool scripts use `component_sources` to follow their
operator and test-helper consumers. Common operator mechanisms, generators,
dependencies and selection authority retain the full suite. The standalone
archive/terminal-recovery CLIs and read-only retired-structure inventory use their dedicated lifecycle/archive/idle or database-inventory
regressions without the unrelated pool transition suite. Shared platform fixtures
and the external phase API double live in `tests/support/`, so importing a
fixture no longer imports unrelated renderer or management-stage test cases.
Every platform candidate selects affected modules against its immutable first
parent; an unavailable boundary retains the whole suite. Shared-pool simulations
stay skipped, below.
`nebius-candidate.yml` reuses successful native `CI` pool jobs on that exact
commit, with GitHub Actions app 15368 and the same manifest, scope and marker
selection. The planner reconstructs the native group's changed-path selection;
a passing narrowed shard covers only its complete selected test modules.
Only missing modules run before publication, grouped by the existing shard
policy; covered siblings are omitted and empty jobs are not allocated. Wrong
heads/workflows and failed or missing jobs cannot supply coverage. A partial job
listing retains proven successful modules and schedules the unseen modules;
without usable evidence every required affected module runs.
There is no cross-commit reuse or per-test-case cache. This is test scheduling,
not an additional branch-admission gate. Image building, scanning and artifact
qualification remain required, as do rollout idle protection, backup and rollback.
Scheduled and manual full runs still include the pool suite. Selector labels
retain full file selection in the validation lanes they request.

Participant attachment tests own the three supported target rosters: ordinary,
ordinary + guest, and ordinary + guest + auth. Downstream recovery, corruption,
and transport matrices use a fresh complete roster by default instead of
multiplying every fault by all three subsets. Connected release success also
runs with the two smaller rosters. Every original recovery fault, both candidate
write implementations, current/private-material refusal, immutable history and
exact-absence assertions remain before merge. No test is moved to candidate
publication, nightly validation or deployment, and no mutable fixture is shared
between cases.

The October 16 rebalance keeps the measured scenario boundaries: deploy-job
failures and completion faults run the same delivery lifecycle from
`test_nebius_pool_participant_delivery_{job_failures,completion}.py`.
The retained module's runtime probe, HTTPS, database/catalog stage and network
checks live in `test_nebius_development_pool_retained_runtime_stages.py`.
All 63 retained test functions and 182 parameter cases remain across the two
files (79 and 103). After removing repeated topology products, module placements
are recomputed across all 20 shards; placements chosen for the old counts are
not a performance contract. The local comparison and its hosted-model limits
are recorded in [pytest efficiency verification](pytest-efficiency-verification.md).

The dev-only candidate test job saves uv caches under the same checksum-pinned,
locked-workspace policy as candidate publication. PR and merge-group jobs retain
their existing cache-write restrictions.

CI test jobs restore a full, unpruned uv wheel cache (`cache-suffix:
ci-wheels-v2`, ~200 MB compressed) and never save it. Every writer and reader sets
`cache-local-path: /home/runner/.cache/uv`: GitHub's cache version hashes the
cached path, and setup-uv defaults to `RUNNER_TEMP` on GitHub-hosted runners but
`~/.cache/uv` on self-hosted ones, so `ci-wheels-v1` was written but never hit. setup-uv's default pruning
kept only locally built packages (~180 KB), so every job downloaded about 150
wheels from PyPI again: ~5 seconds on GitHub VMs, but 4-5 minutes with timeouts
on OLDLAB guests, whose GitHub cache downloads are fast. The only writer is
`ci-dependency-cache.yml`, which runs on protected dev when `uv.lock` or the
writer workflow changes (including its first introduction), and daily. It syncs the superset of every reader's extras and groups, and saves once
per lock. `test_ci_wheel_cache_has_one_trusted_writer_and_complete_contents`
keeps readers read-only and covered by that superset.

The shared worker pool is not in use (dev deploys with `NEBIUS_ROLLOUT_MODE`
unset, i.e. standalone), so `tests/ops/test_nebius_shared_pool_*.py` are skipped
by `tests/ops/conftest.py` unless `LOOM_RUN_SHARED_POOL_TESTS=1`. Participant,
legacy-completion and standalone rollout tests still run: normal rollout uses that
code. Remove the skip before enabling `NEBIUS_ROLLOUT_MODE=shared`.

Go checks validate the retained execution runtime, gateway sandbox, and sandbox
runtime. JWT and execution-materialization producer contracts also retain Go
coverage; unrelated Python changes do not allocate a Go runner. Removed host
controllers and builder supervisors are not restored by
manual compatibility selection.

## Test shard balance

The root Python lane has four complete, non-overlapping file shards; pool
operators have twenty. Both use manifest-owned stable hashing. Explicit pool
pins distribute complete modules according to completed source run 38921887321
and merge-group run 38922178538 (October 16): retained image history, shutdown,
origin history, recovery qualification, development installation and role
restoration no longer crowd the same long shards. The retained development
runtime module separates runtime stages from parent/publication/input checks
into two independently scheduled files, preserving all 63 sets of assertions
and all 182 parameter cases. Probe tests reuse a scoped trusted-tool
fixture instead of requiring a host kubectl installation; real executable lookup
and missing/untrusted-tool refusal remain covered. The shard count and runner
capacity are unchanged. The selector/workflow tests enforce disjointness, full
selected-file coverage, fail-fast failures and empty-selection refusal. Old tests
that asserted exact historical placement of skipped handoff matrices were removed;
a previous placement is not itself an operator behavior contract. The workflow
execution test also proves that the pool lane has identical `all` and `nebius`
module sets before exercising its 20 shards once, instead of repeating identical
selector/subprocess coverage. Root scopes still execute independently.

The pool job limit remains 50 minutes; individual lifecycle and transport limits
remain unchanged. Every selected successful shard runs all its tests, other
shards are not cancelled, and `repository-checks` still requires their results.
Measure execution, allocation wait and serialized merge-group time separately;
local reductions are not a measured PR-to-deployment hosted speedup. Candidate
publication continues reusing only successful native coverage on its exact commit,
so a shorter PR wave does not create a delayed duplicate pool wave after merge.

The fast integration lane uses six complete, non-overlapping file shards.
`config/component-ownership.toml` owns the stable hash salt and the paired
username/password fixture ordering. New files do not reshuffle existing files.
Docker integration uses two non-overlapping file shards, with duration-based
pins separating the guest runtime and build-cache tests. Guest payloads are built
only when the selected files consume them. Every required gate still waits for
all selected shards. Required platform tests are retained.

MinIO preparation follows selected fixture imports and parent conftests. Fast
integration normally needs only the testcontainers release; TLS storage tests
also need the newer release. Source-built fallback images retain their pinned
commit, compiler and recipe and are cached as Docker archives. `quay.io/minio/minio`
no longer serves anonymous pulls, so without the cache every job builds from
source (about 2 minutes, failing when the Go module proxy resets). CI jobs only
restore the archives, from `/home/runner/.cache/loom-minio-fixtures`: the cache
version hashes the path, and `RUNNER_TEMP` differs per hosted worker. The only
writer is `ci-dependency-cache.yml`, which builds every fixture under one key per
recipe set when `tests/support/minio_images.py` changes on dev, and daily. Each job
still prepares only the images it selected. Recipe or release changes get a new
key, and invalid archives fall back to normal pinned-image preparation. The lazy
per-fixture resolver remains available for local tests.

System smoke uses a Compose project per run/attempt and Docker-assigned loopback
ports; it discovers actual service endpoints once and passes the registry endpoint
to the image builder without changing the original port mappings.

Both integration lanes also prepare selected `postgres:16`, `postgres:17` and
`python:3.11-slim` fixtures before pytest, following literal references through
fixture imports and parent conftests. Cached images are reused. Cold pulls use
the configured Docker Hub mirror, have a 180-second preparation deadline and
fail the job visibly without a retry loop or alternate image. This keeps network
downloads outside the existing 60-second test setup budget; individual test
deadlines, fixture references and coverage remain unchanged. Parameterized image
matrices declare literal references so every selected version is prepared.

The final repository gate checks individual lane results directly. Coverage
aggregation runners are allocated only when optional Python coverage is requested.
Frontend protocol-only browser cases run once on Desktop Chrome; responsive,
layout and accessibility coverage still runs on all four projects. Production
and browser-test builds both remain because they compile different branches.

The cluster-contract lane also uses four complete, non-overlapping file shards,
with its stable hash policy in the same manifest. Each shard owns an independent
disposable Kubernetes runtime, preserves within-file fixture order, and retains
the 35-minute job limit and existing per-test deadlines. Independent cluster-test
edits allocate only their nonempty shards, and select the manifest before
dependency and Skopeo preparation. Selector failure stops
the shard even if it emitted partial paths; test failure remains a failed job.
`cluster-smoke-gate` waits for the aggregate result of all selected shards, with
fail-fast cancellation disabled. This replaces a serial run that exhausted its
35-minute limit after completing only 61% of 77 tests; it removes no test paths
or protected checks. Per-shard duration reporting supports later balance review.

The shared disposable K3s fixture requests normal Docker stop before removing
the stopped container and its volumes. Its 30-second shutdown allowance does not
change test deadlines. Stop and removal errors still fail the job. K3s remains
PID 1; adding an init wrapper would interfere with its cgroup startup sequence.

Disposable K3s API discovery does not imply that native DNS or dynamic storage
is ready. Tests that require these addons opt into `_load_client` readiness:
`required_addons=('dns',)` checks the current CoreDNS Deployment and a ready
`kube-dns` endpoint; `('dns', 'storage')` also checks local-path provisioner
readiness. These reads share the original 90-second startup budget. API-only
callers keep their existing boundary, and traffic/PVC assertions remain required.
Addon failures include bounded Pod waiting reasons and warning events; the
retirement fixture also records PVC status without logging Secrets or Pod specs.
The repeat-refresh test also requires this native DNS readiness as an existing
Deployment/ReplicaSet/Pod reconciliation startup witness before its functional
30-second window; it adds no startup budget or storage prerequisite.

The native repeat-refresh test retains its iteration and bounded Deployment,
ReplicaSet, Pod and warning-event status before deleting its disposable node on
failure. Diagnostic reads have individual request bounds and cannot replace the
original error. They include no resource specs, Secret data or arbitrary error
messages; the existing Pod-creation and overall test deadlines remain unchanged.

The 75-minute integration budget retains room for optional coverage and runner
variation. File counts alone do not measure shard balance; use the per-shard
duration reports from the current head.

All four development validation workflows support the opt-in unified native
runner queue, with GitHub-hosted fallback and exclusions as described in
[runner supply](ci-unified-runners.md). GitHub owns job assignment and CheckRuns;
the retired route action, lease broker and custom CheckRun publisher are not
part of this interface. See [runner placement](../architecture/ci-runner-acceleration.md).

Harbor runtime builds follow manifest-owned inputs rather than every image change.

## Publication ownership

`nebius-candidate.yml` is the only automatic **dev** image publisher. `images.yml`
uses the Nebius manifest set for dev PRs and untrusted manual validation. Harbor
uses its own selection, so a Web-only PR builds one image and a complete platform
validation builds seven, without a second Harbor build. PR scanner preparation
fetches only AMD64.

All hosted image publication, including production candidates, uses Nebius.
Production still promotes an exact `dev` candidate into `main` through
`release-promotion-gate` and `main-promotion-gate`, reusing the seven immutable
Nebius image digests. A `main` push does not rebuild or publish GHCR images.
The personal-dev controller, GHCR publisher, ARM64 manifest joiner and their
exclusive evidence/receipt helpers have been removed. `images.yml` is read-only
for every event; scanning and selected-build failure checks remain required.
See [production deployment](../runbooks/nebius-deployment.md) for the retained
release and environment checks. Source retirement does not stop live host services
or cancel existing jobs.

## Daily regression and expensive components

CI runs once daily at 08:23 UTC on the default branch (`dev`). It uses the same
jobs with full Nebius/common root/package/Go/Web/integration/Docker selection and coverage;
it does not publish or deploy and adds no PR admission context. The scheduled
result is named `repository-checks-scheduled`.

The historical schema-reference matrix is manual compatibility only. Nebius
bootstrap, migration lineage, Alembic upgrades, application migration authority,
runtime grants and schema inventory remain daily. Within manual compatibility,
the existing component selector can omit the historical matrix for audited
unrelated changes; full compatibility and coverage requests still run it.
Docker test-only changes use the same selection and failure propagation as
ordinary integration.

The October 2–4, 2026 sample of twelve successful full CI runs had a median
repository-gate wait of 25m20s. Docker job execution was 24m26s and the slowest
root shard was 21m02s at the median. Eleven matched four-workflow runs all finished
CI last. These are the optimization baseline, not a measured speedup; compare
current full runs separately from narrow PR selection and exclude cancelled runs.


## Daily tests and manual compatibility

The existing CI workflow accepts `legacy_compatibility=true`; no new workflow or
required check is introduced. Daily PRs, normal dispatch and scheduled runs use
`CI_TEST_SCOPE=nebius` plus the pytest expression `not legacy_pool`. Main production
validation and an explicit compatibility dispatch use `all`. Scope filtering is
after stable sharding, so shard pins and fixture ordering do not move. Unknown
and mixed/common test modules stay in daily validation.

`config/component-ownership.toml` owns the explicit `compatibility_test_paths`:

| Manual compatibility only | Reason | Daily coverage retained |
| --- | --- | --- |
| Protected database ownership/login handoff and the schema-reference matrix | Reconstructs published schema and privilege history in disposable databases | Nebius bootstrap, migration authority, runtime grants and schema inventory |
| Retained staging renderer and attachment fixtures | Exercises historical configuration compatibility without an operational attachment path | Independent Nebius deployment, restore and execution contracts |
| Retained historical authority/credential and migration tests | Durable records and published migrations still need compatibility validation | Native task-image plans, materialization, execution capacity and lease fencing |

Compatibility patterns apply only to files that still exist. Removed OLDLAB,
GB10, Slurm, global-capacity, personal-dev and standalone builder/controller
implementations and their retired tests cannot be recovered by choosing `all`.
Local Docker and disposable fixtures remain supported. Desktop/GUI, Behavior
GPU and unconverted pipeline workloads have no hosted legacy fallback.
Scope selection does not alter source lint, release authority or branch protection.

### Interpreting historical CI measurements

The pre-retirement #2007 and #2011 measurements describe earlier file populations,
not current runtime support or current-head validation. The ownership manifest
is the authority for retained paths. Current native source/materialization/lease
boundaries, API/auth, SQL, gateway, frontend, packaging and disposable backend
checks remain in their owning lanes. Shared fixture imports alone do not justify
running a removed service's former suite.

Run the full compatibility tests, including both integration tiers:

```sh
gh workflow run ci.yml --ref dev -f legacy_compatibility=true
# Historical Kubernetes/system contracts use the existing smoke workflows:
gh workflow run cluster-smoke.yml --ref dev -f legacy_compatibility=true
gh workflow run staging-smoke.yml --ref dev -f legacy_compatibility=true
# Also request the historical all-platform coverage floor:
gh workflow run ci.yml --ref dev -f legacy_compatibility=true -f coverage_summary=true
```

This validates retained disposable fixtures and does not publish or deploy. Known
PostgreSQL/k3s fixture failures recorded in #2008 remain unresolved compatibility
issues. The owner closed that investigation as not planned; changing daily CI
scope does not claim a fix or add retries for those defects.
