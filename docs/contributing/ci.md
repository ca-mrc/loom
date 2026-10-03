# Nebius CI on dev

`dev` uses Nebius as its only hosted platform and remains the pre-main branch.
The earlier `codex/nebius-main` CI supplied path routing, optional Python coverage
and a smaller runtime scope. Daily dev CI now follows the same Nebius runtime
boundary while retaining the four protected admission contexts.

## Workflow inventory

The repository retains twelve workflows. Low run frequency or no recorded runs
does not make an operator entry point obsolete: promotion, retry, and catalog
publication run when needed. In particular, `main-promotion-gate` remains the
required check protecting promotion into `main`. The GitHub-managed Dependency
Graph entry has no workflow YAML in this repository and is maintained separately.

| Category | Workflow file | Purpose and triggers |
| --- | --- | --- |
| Required CI | `ci.yml` | Repository checks on PRs, merge groups, and `main` pushes; full daily regression at 08:23 UTC; manual validation and compatibility checks. |
| Required CI | `images.yml` | Read-only Nebius AMD64 image builds and scans on PRs, merge groups, and manual dispatch. |
| Required CI | `cluster-smoke.yml` | Credential-free Kubernetes and pinned-Skopeo disposable TLS-registry contract checks on PRs, merge groups, manual dispatch, and selected cluster-path pushes to `dev`. |
| Required CI | `staging-smoke.yml` | Credential-free Compose system checks on PRs, merge groups, and manual dispatch. |
| Contributor compatibility | `macos-locked-environment.yml` | macOS ARM64 locked workspace installation, daily at 09:17 UTC or manually. |
| CI operations | `ci-retry.yml` | Manual bounded retry of a failed/cancelled required-source run, with root-cause classification and evidence. |
| Production promotion | `release-promotion-gate.yml` | Manual validation of release evidence for an exact candidate SHA and image. |
| Production promotion | `main-promotion-gate.yml` | Manual verification that the open `dev` to `main` PR, current candidate SHA, and successful release evidence agree. |
| Nebius publication | `nebius-candidate.yml` | Automatic `dev` push publication or manual platform/Harbor runtime publication, controlled by `NEBIUS_RELEASE_ENABLED`. |
| Nebius deployment | `nebius-rollout.yml` | Application rollout after successful candidate publication or manual dispatch, controlled by `NEBIUS_AUTO_ROLLOUT_ENABLED` and the active-task guard. Separate protected manual operations provide read-only inventory, private certificate qualification, owned ingress installation/paused recovery, and fixed wildcard/management DNS publication under the same concurrency. Certificate and ingress operations each use dedicated forced-command SSH identities bound to exact tooling, not the Kubernetes-only deployment key. Only ingress installation scans/publishes the pinned image and receives registry credentials; recovery and DNS skip publication. DNS credentials remain on the gateway. |
| Nebius maintenance | `nebius-image-retention.yml` | Daily retention at 07:23 UTC or manual preview/apply; enabled by `NEBIUS_RELEASE_ENABLED`, with scheduled deletion separately controlled by `NEBIUS_IMAGE_RETENTION_APPLY`. |

The four CI workflows report protected gate contexts for eligible PR and merge
group runs; manual validation reports separate `*-manual` contexts. Their
changed-path planner controls which expensive jobs run. Candidate publication,
live rollout, and release promotion each retain their own authorization and
evidence checks.

The same protected rollout concurrency also serializes manager installation,
one-time upgrade, retained-operation recovery and
[repeatable software refresh](../runbooks/nebius-deployment.md#refresh-the-retained-application-manager).
Refresh uses dedicated protected metadata and a separate exact-bundle SSH key;
`management-refresh-preflight` and `management-refresh-install` cannot fall back
to another management authority. These are installed-operation entry points, not
PR validation or evidence that the development environment is already accepted.

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

## Nebius path routing

Static checks remain the ordinary baseline. Frontend-only changes do not start
Python test, Go, runtime-payload, dependency-lock, Terraform, Compose or Kubernetes
runners. The Compose system fixture has no Web service; browser/auth changes are
covered by the frontend job. Backend/frontend mixed changes and explicit smoke
labels still select the corresponding checks. Independent
test edits start their owning test jobs. Shared and unknown inputs retain the full
baseline; Terraform runs for its own inputs and full-regression requests.
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
| Nebius Terraform and its checker | Integration, the IaC checks |
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

An edit consisting only of independent Python test modules runs those files in
their owning lanes. Any external reference to an edited test module name keeps
full validation because test modules can provide shared fixtures. Shared or deleted
test inputs select their possible consumer jobs as well as retaining all files
inside those jobs, including when mixed with source changes. Runtime,
migration, configuration, fixture, deleted-file and unknown changes stay full
unless a suite explicitly declares an audited unaffected component.
CI selector labels and manual runs request full regression within the active scope; ordinary labels such
as `bug` do not change test selection. Sharding happens before filtering,
so ownership and paired-fixture ordering remain stable. Empty selections do not
invoke pytest, and selector errors fail the job.

Go checks validate the retained execution runtime, gateway sandbox, and sandbox
runtime. Removed host controllers and builder supervisors are not restored by
manual compatibility selection.

## Test shard balance

The root Python lane uses eight complete, non-overlapping file shards with the
existing manifest-owned stable hash salt. The long gateway retirement and two
restoration files are assigned to separate shards, retaining the previous pins.
Both previous two-shard jobs
exhausted their 40-minute limits at approximately 35–36% in PR #2305. The job and
per-test limits remain unchanged. Each shard stops on its first failing test to
retain the underlying failure report before its deadline; successful shards run
every selected test. Other shards are not cancelled, and `repository-checks`
still requires the aggregate result. This repartitions the expanded lane without
excluding tests; its new duration balance requires measurement on CI runners.

The fast integration lane uses four complete, non-overlapping file shards.
`config/component-ownership.toml` owns the stable hash salt and the paired
username/password fixture ordering. New files do not reshuffle existing files.
Docker integration remains a separate lane, and every required gate still waits
for all selected shards. Required platform tests are retained.

The cluster-contract lane also uses four complete, non-overlapping file shards,
with its stable hash policy in the same manifest. Each shard owns an independent
disposable Kubernetes runtime, preserves within-file fixture order, and retains
the 35-minute job limit and existing per-test deadlines. Selector failure stops
the shard even if it emitted partial paths; test failure remains a failed job.
`cluster-smoke-gate` waits for the aggregate result of all selected shards, with
fail-fast cancellation disabled. This replaces a serial run that exhausted its
35-minute limit after completing only 61% of 77 tests; it removes no test paths
or protected checks. Per-shard duration reporting supports later balance review.

The successful PR #1973 head `69359261` ran its two integration shards in
66 and 90 minutes. Timestamped progress for 518 modules accounts for about
155 minutes of test work. Repartitioning that same work projects approximately
39.0, 38.2, 38.8 and 39.1 minutes across four runners, before setup, queueing and
new tests. This is a planning estimate, not a measured speedup. The 75-minute
job budget retains room for optional coverage and runner variation.

All four source workflows use GitHub-hosted runners directly. The OLDLAB route
action, lease broker, controller, custom CheckRun publisher and dedicated KVM
runner assets have been removed. This does not stop existing host services or
cancel in-flight jobs. See [runner placement](../architecture/ci-runner-acceleration.md).

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

The pre-change integration sample (run `35485701767`, shard 3) spent 41m55s in
pytest and about 22s preparing the runner and dependencies. This identifies test
work as the dominant cost; it is not a measurement of this change's speedup.


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
