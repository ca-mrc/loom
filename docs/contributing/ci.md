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
| Required CI | `images.yml` | Read-only Nebius AMD64 image builds and scans, called by CI after fast feedback on PRs/merge groups, or dispatched manually. |
| Required CI | `cluster-smoke.yml` | Credential-free Kubernetes and pinned-Skopeo disposable TLS-registry contracts, called by CI after fast feedback or dispatched manually. No repeat on a dev push. |
| Required CI | `staging-smoke.yml` | Credential-free Compose system checks, called by CI after fast feedback or dispatched manually. |
| Contributor compatibility | `macos-locked-environment.yml` | macOS ARM64 locked workspace installation, daily at 09:17 UTC or manually. |
| CI operations | `ci-retry.yml` | Manual bounded, classified retry by default; an explicit read-only capacity branch supplies disposable single-job hosted workers. |
| CI operations | `ci-dependency-cache.yml` | Writes the full uv wheel cache and the source-built MinIO fixture archives on relevant `dev` pushes, daily at 07:41 UTC, or manually; PR and merge-group jobs only read them. |
| Dependency maintenance | `harbor-upstream.yml` | Discovers Harbor drift daily at 09:41 UTC or manually. Draft candidate creation requires repository activation and is skipped by manual `check_only=true`; see [Harbor upgrades](terminus2-upgrades.md). |
| Production promotion | `release-promotion-gate.yml` | Manual validation of release evidence for an exact candidate SHA and image. |
| Production promotion | `main-promotion-gate.yml` | Manual verification that the open `dev` to `main` PR, current candidate SHA, and successful release evidence agree. |
| Independent environments | `nebius-environment-lifecycle.yml` | Manual identity-only dispatch of installed provisioning, adoption, enrollment, activation, suspension, resume, retirement and image release/rollback operations. Exact target allowlists, pinned tooling and participant concurrency select separate development, staging or production authority; production also requires authenticated promotion evidence. See the [lifecycle runbook](../runbooks/nebius-environment-lifecycle.md). |
| Nebius publication | `nebius-candidate.yml` | Automatic `dev` push publication or manual platform/Harbor runtime publication, controlled by `NEBIUS_RELEASE_ENABLED`. |
| Nebius deployment | `nebius-rollout.yml` | Application rollout after successful candidate publication or manual dispatch, controlled by `NEBIUS_AUTO_ROLLOUT_ENABLED` and the active-task guard. Separate protected manual operations provide read-only inventory, private certificate qualification, owned ingress installation/paused recovery, fixed wildcard/management DNS publication, and private dev foundation preflight/installation under the same concurrency. Development uses its own operation and digest-bound SSH key; it cannot invoke staging/management commands. Certificate and ingress operations each use dedicated forced-command SSH identities bound to exact tooling, not the Kubernetes-only deployment key. Only ingress installation scans/publishes the pinned image and receives registry credentials; recovery and DNS skip publication. DNS credentials remain on the gateway. |
| Nebius maintenance | `nebius-image-retention.yml` | Daily retention at 07:23 UTC or manual preview/apply; enabled by `NEBIUS_RELEASE_ENABLED`, with scheduled deletion separately controlled by `NEBIUS_IMAGE_RETENTION_APPLY`. |

`CI` reports all four protected gate contexts for eligible PR and merge-group
runs. It calls the image, cluster and staging workflows through native job
dependencies; those workflows also retain independent manual diagnostics, whose
internal result names cannot substitute for the four required checks. The
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

GitHub evaluates required checks from a workflow's newest run on a head, and
does not order runs by event time. A Draft `synchronize` delivered in the same
second as `ready_for_review` can get the newer run (#2635). Filtered, it hides
the Ready run's four contexts, and re-running that older run does not restore
them. `workflow-plan` therefore checks out every PR event, and
`scripts/ci_stale_draft_event.py` reads the live PR and this head's CI runs
with read-only `pull-requests`/`actions` tokens. A Draft payload runs the full
gate only when the live PR is open, Ready, on the same head, and either its
newest `gate=full` run is older or no `gate=full` run appears within about 30
seconds. GitHub can drop the `ready_for_review` run entirely (#2677), leaving
the Draft run as the head's only suite. It passes `stale_draft` to the image, cluster and
staging workflows so they plan instead of filtering. A genuine Draft, a newer
head, a newer full run, or an unreadable API keeps the event filtered. The upgraded run keeps its `gate=filtered` run
name and Draft concurrency slot. Candidate reuse therefore ignores it, and a
later push does not cancel it. The older Ready run also completes, so each
occurrence costs one extra full run (seven heads Oct 15–26); so does a
dropped Ready run that GitHub delivers after the wait.

The manifest retains ownership of both historical and Nebius tests. Daily dev CI
selects Nebius and common contracts; historical compatibility is manual. PR image CI uses the same seven-image Nebius set as candidate publication,
builds only affected AMD64 images and scans the resulting artifacts. Disposable
Kubernetes checks include the Nebius platform and execution contracts. Candidate
publication uses the protected `nebius-integration` Environment from `dev`;
PR checks do not receive its credentials. CI does not deploy the live platform.

## Early feedback and heavy-job ordering

Inside `CI`, selected lint/type/codegen, locked-environment, package, Go and
Terraform checks run first. Selected root tests and the non-Docker integration
tier follow those checks. Frontend API/type/lint/unit checks also contribute to
`fast-feedback`, an internal aggregation job, not a required merge context.
It rejects every selected failure, cancellation or unexpected skip. Unselected
lanes may skip. Existing changed-path selection, test manifests and coverage
requests remain in force; full-regression requests still use full manifests.
Ordinary documentation-only PRs skip feedback when no dependent work is selected.
Additive image/cluster/staging labels and manual baseline requests still run
feedback before their selected dependents, even when only documents changed.

For PRs selecting heavy work, successful quick checks are followed by a bounded
base-freshness preflight inside `fast-feedback`. It reads the active branch rules
and, only under strict status checks without a native merge queue, compares the
event's base SHA with the current base ref. An already-advanced base fails before
heavy fan-out. Native queue admission skips this comparison because GitHub
regenerates merge groups. This uses at most two read-only metadata requests with
20-second timeouts and adds no gate or permission. It does not lock the base or
cancel work if the base advances after the preflight.

Pool, Docker integration, runtime payload and frontend production/browser work
start only after `fast-feedback` succeeds. Frontend runtime work has its own
job and is still enforced by `repository-checks`; no test is omitted or repeated.
Heavy-job guards explicitly require successful planning and feedback and reject
cancellation. They do not inherit GitHub's implicit `success()` across skipped
ancestors: an unselected quick lane may skip without suppressing selected heavy
work after feedback accepts it. Selected heavy jobs that skip still fail the gate.
Images, cluster and staging are reusable workflows called by CI only when their
lane is selected and feedback succeeds. GitHub waits on `needs` before any of
those workflows' runners are allocated. There is no cross-workflow polling,
Actions read permission, title matching or separate PR/merge-group run. A single
CI concurrency group cancels obsolete PR attempts, including their called jobs;
manual specialist diagnostics remain independent. New nested jobs and their three
caller gates use GitHub-hosted fallback until the installed supply controller
passes reusable-graph readback and `LOOM_CI_REUSABLE_SUPPLY_READY=true`; see
[runner supply](ci-unified-runners.md#reusable-workflow-compatibility).

The four bare gate names are native jobs in the caller CI run. The specialist
gates reject selected work that failed, was cancelled or unexpectedly skipped,
and require successful feedback. An unselected lane must be skipped. Each called
workflow still validates its actual image/shard/smoke results before its caller
can succeed. The same dependency graph applies to PR heads and merge groups;
manual/scheduled/push validation cannot emit these three bare specialist names.
There is no custom check publisher, fifth required context or ruleset change.

PR and merge-group root, integration, pool, Docker, image and cluster matrices
stop sibling shards on failure. Candidate supplementary pool tests also stop
sibling shards; a failed or cancelled shard prevents publication. Docker,
cluster and staging pytest stop at the first failure and retain diagnostics and
fixture cleanup. Manual/scheduled source diagnostics retain all selected shards.

`fast-feedback` uses the same `ubuntu-24.04` hosted image as candidate planning
and executes its installed `gh` against a loopback HTTP response containing
colored checkout logs. The stdlib-only probe calls the real candidate reader,
keeps its flags and old-client fallback, isolates authentication/configuration,
and never calls GitHub. Missing or incompatible `gh` fails feedback before heavy
work. This requires no tool download or workspace installation. Root regressions
also exercise the probe where `gh` is installed; OLDLAB guests without that tool
skip only the duplicate CLI smoke, whose mandatory execution belongs to feedback.

Read-only candidate log readers and CI metrics tools run their owning root
regressions and static checks without selecting pool lifecycle tests, images,
Docker or cluster/staging work. A reader filename beginning `nebius_pool_` does
not make it a runtime operator. Shared selection/workflow authority, lifecycle
sources, dependencies and unknown paths retain their owning full coverage.

Before publishing a CI repair, reproduce the reported failure through the real
changed interface using the failing runner's tool version and representative
input, then verify the repair through the same path. Consolidate diagnostics
and the fix locally before one Ready publication; a large passing unit suite
alone does not demonstrate CLI/API compatibility. Preserve strict admission,
coordinate integration order when the native queue is disabled, and retry only
the affected failed jobs when evidence supports a transient failure. Adding
logging alone is not a reason to repeat an entire native test/publication round.

The one-time audit measured 149,605 runner-seconds across five superseded or
diagnostic iterations. Of that, 15,598 seconds were occupied feedback waiting
and 15,487 were duplicate candidate pool tests. Those are historical costs,
not a saving claimed for every future PR. Compare actual execution, retries,
queue and wall time separately; the final qualification and publication still
need their normal checks.

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

After the local disposable-runner check, cluster-contract jobs load
`br_netfilter` and require its bridge sysctl before creating the K3s container.
This lets kube-router initialize on a fresh guest; missing modules fail the
preparation step rather than waiting for policy enforcement that cannot start.
K3s still owns its namespace sysctl setup, and policy assertions and deadlines
remain unchanged.

The shared disposable K3s fixture preloads its pinned system images through
the host Docker daemon: pause, CoreDNS, local-path provisioner and its busybox
volume helper, and metrics-server. It saves a reusable archive and mounts that
file read-only in K3s's startup import directory. Volume setup and teardown use
the same preloaded helper image; provisioner readiness alone does not exercise
that dependency. Test-specific images retain their existing import/pull paths.

The separate image BuildKit containers use `config/ci-buildkit.toml` by default;
an existing readable `/etc/buildkit/loom-ci.toml` retains precedence and is not
modified. Both pull paths retain every original image reference and digest.

The connected ingress fixture prepares its pinned application images on the host
with Skopeo before starting disposable K3s, then imports OCI archives and verifies
the original references and digests. Public fixture sources use explicit anonymous
authentication (`--src-no-creds`) without discovering runner credentials; TLS
verification remains enabled. Multi-architecture pins retain the original
index plus the AMD64 payload, not a replacement child-manifest identity. Only a
mirror's `toomanyrequests` response permits the same digest from its upstream;
other acquisition errors fail setup. Deterministic helper coverage exercises that
fallback; the connected test does not deliberately force a live upstream pull.
This removes its nested application-image registry dependency, not native K3s
addon pulls or other test dependencies. Archives are removed after use or failure.

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
| Action-pin and CI selection tooling | Python root contracts and static checks |
| Retained Go runtime modules | Go checks and their affected Docker/image contracts |
| Candidate workflow/publisher, registry authentication, shared scan validator | Full validation |
| Unknown runtime or operator files | Full validation |

Mixed changes take the union of their requirements. CI labels can add checks and
cannot subtract path-selected checks. This avoids treating known deployment
files as unknown code while retaining full validation for publication authority.
The actual tests are selected from `config/component-ownership.toml`.

## Coverage collection

Python root/package/integration tests normally run without coverage instrumentation.
Coverage excludes only archived `loom-candidate-manifests-*/src/loom` source
roots. Historical renderers still execute in their owning tests, and their
coverage never counts as the current checkout's source. Other missing source
errors remain fatal during combine, XML and text reporting.
Instrumented root, package and integration jobs explicitly select the current
`pyproject.toml`; pytest-cov propagates its resolved path to Python children,
including historical renderers with a different working directory and config.
The slim-image copied-source import probe removes pytest-cov child-startup
variables from its isolated interpreter. That probe exercises the Dockerfile's
packaged imports without the checkout's test plugins; its disposable copy is
not current-checkout coverage. Current source measurement and unrelated
missing-source failures remain active.
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
Nightly/manual and explicit full requests preserve stronger coverage within
the default suite scope; the unlaunched shared simulations require their own
explicit manual input below. Global
Python type checks, frontend coverage populations and broad Docker COPY inputs
are real dependencies; ownership globs cannot make those inputs disappear.
Selection failures propagate before pytest or toolchain execution. The audit's
native CI sample took 41m06s (109.47 runner-minutes, longest job 12m20s), with a
30m08s ready-to-start delay for one root shard. These are baseline measurements,
not a claimed post-change hosted speedup.

`scripts/ops/ci_runner_supply.py` has a dedicated `ci-runner-controller` test
owner. A change confined to that controller selects its root contracts and
native image, cluster and staging validation. Mixed application, shared-fixture,
dependency and unknown inputs retain their applicable regression coverage;
shared simulations still require the explicit manual input.

## Pool operator simulations require manual qualification

Production/development pool lifecycle and shared-pool handoff simulations are
not launched. The existing `nebius-pool-ops` and `nebius-shared-pool-ops` suites
are both `manual_only`; ordinary PRs, merge groups, pushes, schedules and
candidate supplementary validation exclude them. This includes edits to their
operators, fixtures, dependencies and selection metadata. The existing manual
input below enables their original 20-shard layout before feature launch.

Current standalone staging coverage remains automatic. In particular,
`test_nebius_legacy_pool_completion.py` protects the deployed preflight reader,
and `test_nebius_staging_pool_attachment.py` retains current staging rendering,
profile and UID contracts in `tests-root`. Ordinary deployment, idle/recovery,
unit/integration tests and all four required source gates remain in place.
Pool fixture modules remain importable by other tests; manual ownership only
controls which test files are selected.

`nebius-candidate.yml` reuses successful native `CI` pool jobs on that exact
merge-group commit, or the native squash PR whose tested merge has the same Git
tree as the published candidate. For squash reuse, the planner verifies the
candidate's merged PR and parent, GitHub Actions app 15368, and each successful
job's actual merge checkout from the pinned checkout action's initial log. The
tested merge must have exactly the candidate parent and PR head as its parents.
The same tree preserves the manifest and selection code; the initial job scope,
markers and changed paths reconstruct only that job's complete selected modules.
Only missing modules run before publication, grouped by the existing shard
policy; covered siblings are omitted and empty jobs are not allocated. Wrong
heads/workflows, different trees, unavailable checkout logs and failed or missing
jobs cannot supply coverage. A partial job
listing retains proven successful modules and schedules the unseen modules;
without usable evidence every required affected module runs.
When a read or binding check fails, the plan also reports up to 25
`evidence_errors`: the owning stage, a fixed error category, the HTTP status when
available, and applicable numeric run/job IDs. Known rate-limit and permission
errors, PR association counts and recognized binding reasons are distinguished;
raw CLI stderr, response bodies, exception text and credentials are
never emitted. Use the native candidate plan's diagnostics to distinguish API
access from unavailable PR associations, checkout logs and checkout bindings;
a later successful local read does not establish why the native read failed.
The machine-only job-log reader explicitly allows terminal escape sequences:
gh 2.102's raw-response guard otherwise rejects normal colored Actions logs.
The captured log is never printed or stripped; the existing 4 MiB limit and
exact pinned-checkout proof still apply. Older clients that reject the flag
locally use their original raw reader; permission, HTTP and transport failures
do not trigger that compatibility fallback. Remove the old-client fallback
when the supported developer CLI minimum includes the flag (tracked in #2416).
Candidate plans print the CLI version alongside their bounded diagnostics.
There is no reuse across different Git trees or per-test-case cache. This is test scheduling,
not an additional branch-admission gate. Image building, scanning and artifact
qualification remain required, as do rollout idle protection, backup and rollback.
Schedules and ordinary full requests do not enable unlaunched pool simulations.
Selector labels retain full selection within the default automatic scope;
explicit manual qualification is required for the pool lifecycle suites.

Participant attachment tests own the three supported target rosters: ordinary,
ordinary + guest, and ordinary + guest + auth. Downstream recovery, corruption,
and transport matrices use a fresh complete roster by default instead of
multiplying every fault by all three subsets. Connected release success also
runs with the two smaller rosters. Every original recovery fault, both candidate
write implementations, current/private-material refusal, immutable history and
exact-absence assertions remain in manual qualification. No scenario is deleted,
and no mutable fixture is shared between cases.

The recovery and preactivation-recovery census scenarios run as separate whole
modules on two existing pool shards. Each keeps its 1500-second case limit,
fixtures and fault assertions. Their combined allowed budgets plus the other
modules exceeded one job's 50-minute budget; the separation retains all 20
shards and the existing job deadline without dropping either scenario.

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

The `nebius-pool-ops` suite owns production/development lifecycle simulations;
`nebius-shared-pool-ops` owns shared handoff, migration, recovery and census.
Both retain their complete source and parameter matrices for manual use.

For explicit qualification, dispatch `CI` with `shared_pool_simulations=true`.
The planner accepts the flag only for `workflow_dispatch`, emits it to the pool
matrix and each executing job, and preserves the full manual scenario set.
Local selection uses `scripts/component_ownership.py test-paths --lane nebius-pool
--include-shared-pool true`. Add `--suite nebius-pool-ops` or
`--suite nebius-shared-pool-ops` to select one ownership group. Pass the printed
paths to pytest to run it. Empty-diff manual qualification selects both suites;
the existing affected-source rules can narrow explicit selections by diff.

Candidate supplementary execution uses the default selection. Squash coverage
reuse reads `SHARED_POOL_SIMULATIONS` alongside the job's actual changed-path
and diff hints; a skipped simulation is never counted as covered. Logs without
that switch cannot prove optional simulation coverage. Excluded files cannot
contribute to the candidate's required/reused counts. Merge-group reuse covers
only default modules. Before shared feature launch, complete installed qualification under
#1915, review the measured long-scenario budgets and remove `manual_only` in a
reviewed change that restores automatic coverage. A successful manual simulation
alone neither establishes installed readiness nor authorizes shared activation.
See [shared-pool validation layers](shared-pool-test-layers.md).

Go checks validate the retained execution runtime, gateway sandbox, and sandbox
runtime. JWT and execution-materialization producer contracts also retain Go
coverage; unrelated Python changes do not allocate a Go runner. Removed host
controllers and builder supervisors are not restored by
manual compatibility selection.

## Test shard balance

The root Python lane has four complete, non-overlapping automatic file shards;
explicit pool qualification has twenty. Both use manifest-owned stable hashing. Explicit pool
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

Pool qualification retains its original `pins`, stable hash salt and 20-shard
count. The obsolete automatic-only relocation of the retained development
module is removed: both pool suites now require the same explicit manual flag.
The manual lane has 123 modules; the two deployed-staging boundary modules run
in the ordinary root lane instead. Selection and execution still partition each
selected file exactly once. The following measurements describe the historical
manual layout and do not claim a new hosted speedup.

Run 39471175183 (October 20) exhausted shard 8's budget after its handoff module
alone took 2445 seconds. Activation, cutover, gateway retirement and recovery
release now use two existing lightly loaded shards (288 and 280 seconds in that
run). The development installation module also moves off the 2706-second census
shard to an existing eight-minute shard. These five whole-file pin changes
preserve test selection and all scenarios; they add no jobs or runner capacity.
Their hosted timing must be verified on the new head, not inferred from a local
pass or a censored timeout.

Run 39677158201 exceeded the general census's 1200-second test limit in its
preactivation-recovery variant (27.68 seconds setup and 1172.32 seconds call).
That existing scenario now lives with ordinary recovery in the recovery census
module on shard 20, using its existing 1500-second whole-test budget. All eight
census scenarios and their assertions remain. Shard 20 took 552.05 seconds in
that run; placing the recovery variant there avoids concentrating both the
participant and recovery matrices on shard 19. Hosted completion and balance
were verified in follow-up run 39689189236: shard 19 completed 30 tests in
1728.44 seconds and shard 20 completed 31 in 2484.07 seconds, including the
1238.96-second preactivation-recovery call. These measurements describe that
cycle; the cause of the run-to-run timing variation is not established.

Run 39694426363 completed the schema-only successor scenario in 221.36 seconds
including setup, but the combined storage/schema variant reached its
300-second whole-test limit during retained private-input loading. Only that
combined variant now uses the connected-storage suite's 600-second budget;
schema-only retains 300 seconds. Both scenarios and all assertions are unchanged.
This is a scoped test-budget adjustment, not a repair of the unexplained hosted
timing variation. Completion on the updated head still needs hosted verification.

The pool job limit remains 50 minutes; other lifecycle and transport limits
remain unchanged. Every selected successful shard runs all its tests. PR and
merge-group matrices cancel remaining siblings after a failure; manual,
scheduled and push diagnostics retain all selected shards. `repository-checks`
still rejects selected failed, cancelled or skipped work.
Measure execution, allocation wait and serialized merge-group time separately;
local reductions are not a measured PR-to-deployment hosted speedup. Candidate
publication continues reusing only successful native coverage on its exact candidate tree,
so a shorter PR wave does not create a delayed duplicate pool wave after merge.

The fast integration lane uses six complete, non-overlapping file shards.
`config/component-ownership.toml` owns the stable hash salt and the paired
username/password fixture ordering. New files do not reshuffle existing files.
Docker integration uses two non-overlapping file shards, with duration-based
pins separating the guest runtime and build-cache tests. Guest payloads are built
only when the selected files consume them. Every required gate still waits for
all selected shards. Required platform tests are retained.

Guest Compose retains its two-phase handshake: a bounded read-only probe waits
for the supervised process identity, then the controller validates the returned
PID before a separate request releases the command gate. A cancelled Docker CLI
does not retire its container's inner exec, so a late probe must never release
that gate. Each probe performs at most one read. An observed absent marker is
the only not-ready status (75) that permits another probe within the existing
30-second activation budget; the dedicated inner status distinguishes absence
from a Docker CLI failure (1). An existing unreadable marker (2), execution
deadline (124), or any other failure is reported immediately with its exit code,
without command, environment or stderr contents. The 5-second request limit and
separate gate request remain. Cleanup
reads the marker once so a supervisor's normal EXIT removal cannot race a
separate existence check; an existing invalid marker still fails. If activation
or execution fails and cleanup also fails, the diagnostic names both phases and
preserves the first exception as its cause, without including authored commands
or environment values. Inspect that first failure before attributing an
activation expiry to cancellation cleanup.

A local ARM64 host-Docker witness held two accepted read probes with controlled
SIGSTOP: each client expired at the original 5 seconds while the inner probes
remained alive, then both retired after resume. This demonstrates ownership and
overlap under injected delay, not the physical cause of a native CI expiry. A
single missing-marker observation measured the former 19 external sleep calls
at 1.137 seconds and a single read at 0.061 seconds on that host. The inner
polling loop is removed; these measurements establish smaller per-probe work,
not an end-to-end CI speedup or a qualified cure for the original TCG timeout.
Native qualification on the final source must still exercise the original
240/120/30/5-second bounds and cancellation, collection and teardown assertions.

The Compose HTTP fixture keeps its listening socket open while individual
response workers run. Responses include their actual Content-Length; a health
request must not retire the only listener before the next client connects.
HTTP failures retain the original assertion and only project safe state fields
from the fixture's own containers, without repeating the request.

The TLS rotation test prepares only its pinned Traefik and Python images on the
host before starting K3s, using the existing Skopeo OCI fixture transport. It
imports the original references and verifies their manifest digests in the
disposable containerd. This avoids a nested Docker Hub token/DNS dependency;
classic Docker archives can reconstruct manifests and change those identities.
The existing acquisition policy and the full 240-second TLS/rotation test remain.

For a disposable K3s sandbox stall, early Flannel bootstrap warnings are not
enough to explain a later Pending Pod. The existing five-second process snapshot
slot retains owned netfilter/CNI kernel waits through a bounded host reader;
container cgroups and process identities limit which stacks can be read. It
does not change network policy, kernel modules or the networking backend. The
physical wait-chain cause tracked in #2611 remains separate from this diagnostic.

MinIO preparation follows selected fixture imports and parent conftests. Fast
integration normally needs only the testcontainers release; TLS storage tests
also need the newer release. Source-built fallback images retain their pinned
commit, compiler and recipe and are cached as Docker archives. `quay.io/minio/minio`
no longer serves anonymous pulls, so without the cache every job builds from
source (about 2 minutes, failing when the Go module proxy resets). CI jobs only
restore the archives, from `.ci-cache/loom-minio-fixtures` inside the checkout: the
cache version hashes the path, `RUNNER_TEMP` differs per hosted worker, and the
archive stores the path relative to the workspace, which differs between the
hosted writer and self-hosted jobs. The only
writer is `ci-dependency-cache.yml`, which builds every fixture under one key per
recipe set when `tests/support/minio_images.py` changes on dev, and daily. Each job
still prepares only the images it selected. Recipe or release changes get a new
key, and invalid archives fall back to normal pinned-image preparation. The lazy
per-fixture resolver remains available for local tests.

The disposable MinIO runtime keeps a minimum curl version instead of requiring
an old APK revision that Alpine has replaced. Its curl and CA security floors
accept newer published revisions; the MinIO commit, compiler and base-image
digest stay fixed.

System smoke uses a Compose project per run/attempt and Docker-assigned loopback
ports; it discovers actual service endpoints once and passes the registry endpoint
to the image builder without changing the original port mappings.
PostgreSQL health probes use TCP on `127.0.0.1` and the test database. The
official image's temporary initialization server listens only on a Unix socket;
accepting that server as healthy can release host-side migrations before the
final TCP server starts. The cold-init barrier regression runs in the existing
Docker integration tier. Health intervals, retry counts and migration behavior
remain unchanged.
Its FIFO release uses the same `postgres` OS user that creates the init barrier.
Failed Compose commands print a bounded, redacted tail of their captured output,
so image-build errors remain visible even before service containers exist.

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
`cluster-smoke-gate` waits for the aggregate result of all selected shards. PR
and merge-group matrices cancel unfinished siblings after a failure; manual
diagnostic runs retain independent outcomes. This replaces a serial run that exhausted its
35-minute limit after completing only 61% of 77 tests; it removes no test paths
or protected checks. Per-shard duration reporting supports later balance review.

The shared disposable K3s fixture requests normal Docker stop before removing
the stopped container and its volumes. Its 30-second shutdown allowance does not
change test deadlines. Stop and removal errors still fail the job. K3s remains
PID 1; adding an init wrapper would interfere with its cgroup startup sequence.

The actuator module prepares each image when its first consumer needs it, once
per role per module. The network-policy case builds only its small fixture
image within its existing 180-second whole-test limit. The production runtime
is built by its first actual runtime case, within that case's existing
360-second limit; later cases reuse it. A failed build is retained as a failure,
and every attempted image tag is cleaned up. Full-module coverage still builds
and exercises both roles. This corrects an eager runtime build charged to a
network-only case; it does not claim that the two builds' total work disappeared.
The cluster job reports stage/cache and wall-time records within a 64 KiB read
limit even on failure. It reads past earlier shared helper builds so the target
module's later records are retained. Python and reaped-child CPU counters
describe the test driver/client, not the build daemon; parallel BuildKit stage
durations are not additive.

Pod readiness retains its 60-second budget and Running/Ready predicate. Its
read-only transport copies the original configuration, scoped headers and
cookie, disables SDK retries on that private client, and bounds each request by
the remaining budget. Fractional timeout tuples are required by the pinned
SDK, which ignores scalar floats. Only observed startup TLS EOF and read
timeouts are polled within the same deadline; authorization, certificate and
other failures still propagate. The caller's shared/write client stays intact,
private connections are closed, and late responses cannot establish readiness.

Disposable K3s API discovery does not imply that native DNS or dynamic storage
is ready. Tests that require these addons opt into `_load_client` readiness:
`required_addons=('dns',)` checks the current CoreDNS Deployment and a ready
`kube-dns` endpoint; `('dns', 'storage')` also checks local-path provisioner
readiness. These reads share the original 90-second startup budget. API-only
callers keep their existing boundary, and traffic/PVC assertions remain required.
Addon failures include bounded Pod IPs, readiness conditions, container IDs,
waiting reasons, and normal/warning events with timestamps. Normal image-pull
and container-start events help distinguish startup stages after a sandbox
network error. The shared network failure helper also retains a bounded
containerd log tail and CRI sandbox/container listings from its disposable node;
each command has a five-second deadline and a 16-KiB output tail. Diagnostic
failures preserve the original error. The retirement fixture also records PVC
status without logging Secrets or Pod specs.
The database guard fixture collects the same node/runtime/event evidence before
teardown when PostgreSQL startup or its subsequent assertions fail. This is
diagnostic coverage, not a timeout extension or a claimed startup-stall repair.
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

The three complete independent-environment installation cases retain a real
administration status CLI invocation, followed by connected observations through
the complete `run_administration` entry in their worker thread. Each observation
reads the current files, verifies authorization/hash/schema, opens a fresh real
read-only PostgreSQL transaction, commits and disposes its engine. No receipt is
cached. The reusable enrollment fixture defaults to the CLI, and its dedicated
CLI apply/status/invalid-hash checks remain. The real CLI keeps its 30-second
subprocess bound; connected observations use a 30-second cooperative async
deadline, within the unchanged 180-second complete-case cap. The platform grant
fixture samples the PostgreSQL clock before opening its mutation transaction,
matching the audit clock while retaining the original five-minute validity.
Immediate readiness inspection grants use the same database timebase.

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
