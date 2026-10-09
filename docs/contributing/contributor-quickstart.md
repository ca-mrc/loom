# Contributor quickstart

For people working on Loom itself, not just running it. End-user
docs live in [`user-guide.md`](../user-guide.md) +
[`operator-runbook.md`](../runbooks/operator-runbook.md).

The canonical public development repository is
[`qianyi-sun/loom`](https://github.com/qianyi-sun/loom):

```bash
git clone https://github.com/qianyi-sun/loom.git
cd loom
```

## Repo layout

See the [repository map](repository-layout.md) for source, package, deployment,
migration and documentation ownership. The [architecture overview](../architecture/overview.md)
connects those directories to hosted and local execution.

## Components

| Component | Lives in | Talks to |
|---|---|---|
| Foundation library | `src/loom/` | (used by all) |
| `loom` CLI | `src/loom_cli/` | adapters, local disk, provider SDKs |
| Local CLI drivers | `src/loom_drivers/` | Modal |
| Control Plane | `src/loom_control_plane/` | Postgres, MinIO |
| LLM Gateway | `src/loom_llm_gateway/` | Anthropic / OpenAI / Google, Postgres |
| Local/disposable worker | `src/loom_worker/` | Control Plane, Gateway, MinIO, Docker |
| Service (REST) | `src/loom_service/` | CP, Gateway, Postgres |
| Web SPA | `web/` (served by `loom-web` k8s pod via nginx) | `loom_service` `/api/v1/*` |
| Benchmark adapters | `packages/loom-benchmarks/` + `packages/loom-benchmark-terminal-bench-2/` | (discovered via entry-points) |
| Agent adapters | `packages/loom-launcher/` | (discovered via `loom_launcher.get_adapter`) |
| Operator CLI | `src/loom_benchmark_tool/` | Postgres, MinIO |

## Dev setup

Service mode requires Docker CLI with the Compose plugin. On macOS, install and
start Docker Desktop first; `docker compose version` should succeed before
running `loom service up`.

The tracked workspace lock supports macOS arm64, Linux x86_64, and Linux arm64.
Intel macOS is not currently represented by that lock; use a supported Linux development environment or coordinate a platform-support change before updating the lock.

```bash
# One-time — uv 0.11.26 creates .venv/ from the tracked universal lock.
# The workspace lock covers macOS arm64, Linux x86_64, and legacy Linux arm64.
# Daily server CI validates the Nebius Linux x86_64 target.
uv python install 3.11
uv sync --locked --all-packages --extra dev --python 3.11
source .venv/bin/activate

# Provider keys + stack bootstrap
cp .env.example .env       # then edit
loom service up  # docker compose + migrations + token

# Front-end iteration (Vite HMR on :5173, proxies /api → :8090)
cd web && npm install && npm run dev
```

See the [local development workflow](../runbooks/local-dev-workflow.md) for
service lifecycle commands, non-default port mappings, fresh-database migration
recovery, debugging, and focused local checks.

## Frontend recovery contract

The SPA must remain visible and recoverable when runtime config, browser
session loading, the root render tree, or one routed page fails. Read
[`frontend-error-recovery.md`](../architecture/frontend-error-recovery.md)
before changing `web/src/main.tsx`, `web/src/bootstrap/FrontendBootstrap.tsx`,
`web/src/auth/AuthContext.tsx`, the root/route boundaries, or their reporter.

Keep these invariants when adding a new failure path:

- Render fixed, kind-specific safe copy plus a generated `WEB-*` reference;
  never render an API/browser error message or response body.
- Keep raw URLs, query strings, tokens, cookies, CSRF values, stacks, component
  stacks, and raw throwables out of both UI state and reporter payloads.
- Treat only the exact `/api/v1/auth/me` `401` as `signed-out`. Network,
  non-401 HTTP, `204`, malformed, and invalid-schema responses are
  `unavailable`, not a reason to show a login form. A successful response must
  include the declared identity, authorization, membership, and non-empty CSRF
  fields; do not silently default a malformed session into authenticated state.
- Treat an ambiguous response from login completion, password login, invite
  acceptance, or team switching as `unavailable`: clear old authorization,
  CSRF, and query trust, then reconcile through `/api/v1/auth/me`. The server
  may have committed the session mutation before the response was lost.
- Keep auth reads, session-producing mutations, and logout on the shared query
  client's session-operation queue. Root recovery remounts must wait for an old
  operation to settle and then read authoritative `/auth/me`; an older response
  must never reinstall CSRF after an exact unauthorized event.
- Offer Retry only when a fresh in-document attempt can work. A cached
  `React.lazy` rejection must use `retryPolicy="reload-required"` and omit the
  misleading Retry action.
- Preserve route-aware Home (`/dev/`, `/prod/`, or `/`), keyboard focus, a
  single `main#main-content`, and distinct reference/report correlation.
- Keep `BrowserFailureReport` bounded to reference, allowlisted kind, redacted
  pathname, and optional safe same-origin source position. The current reporter
  is an in-memory hook, not a telemetry transport, and does not retain the
  original throwable.

Run the focused recovery tests before the full web suite:

```bash
cd web
npm test -- \
  src/__tests__/AuthContext.test.tsx \
  src/__tests__/bootstrap/FrontendBootstrap.test.tsx \
  src/__tests__/components/BrowserErrorBoundary.test.tsx \
  src/__tests__/components/Layout.test.tsx \
  src/__tests__/components/RecoveryPanel.test.tsx \
  src/__tests__/components/RootErrorBoundary.test.tsx \
  src/__tests__/components/RouteRecoveryBoundary.test.tsx
npm test
npm run typecheck
npm run lint
npm run test:coverage
npm run build
npm run test:e2e -- e2e/recovery.test.ts
LOOM_E2E_ROUTE_PREFIX=/prod npm run test:e2e -- e2e/recovery.test.ts
```

The render-fault scenarios must stay inside the compile-time browser-test
build. A normal `npm run build` verifies that the emitted `dist/` assets do not
contain the recovery fault key or the fixed test-only fault strings.

## Tests

Every PR and merge-group candidate reports four stable validation contexts:
`repository-checks`, `images-gate`, `cluster-smoke-gate`, and
`staging-smoke-gate`. The shared validation planner selects the applicable
work automatically from changed paths. Labels may request additional work, but
they cannot turn off validation inferred from paths. Docs-only PRs take a
bounded location-and-format fast path while the stable gate contexts still
report. Runtime Markdown outside that boundary, executable files in `docs/`,
and unknown non-document paths do not take the fast path; unknown runtime paths
select all heavy lanes until they gain an explicit owner.

Rollout and production-release authority paths are fail-closed owners: changes
under `src/loom_cli/rollout/` and their tests, installed staging-rollout assets,
deployment/release workflows, or release evidence verification select every
heavy lane. The selected `cluster-smoke-gate` validates Nebius/common Kubernetes
contracts. Historical staging/production profile rendering and environment
isolation commands run only in main or manual compatibility scope. These checks
are credential-free candidate evidence; real model-backed tasks and live
environment readiness require deployment acceptance rather than PR jobs.

Changed paths select static checks, up to eight nonempty root-test shards and
package tests as needed, in parallel on GitHub-hosted runners. Web-only changes
skip backend baseline jobs; dependency, shared and unknown changes retain them.
`repository-checks` verifies every selected result. Coverage instrumentation and
aggregation run when requested with `ci:coverage-summary` or
`coverage_summary=true`, and in the daily full Nebius regression. Nebius coverage
is reported without the historical all-platform 70% floor; main and manual
compatibility coverage retain that floor. Report errors still fail. Independent
test-only edits select their owning files; shared fixtures and runtime changes
retain full lanes. Root shards have a 40-minute budget and integration shards
have a 75-minute budget; selection does not relax per-test timeouts or failures.
`repository-checks` enforces every selected result after the independent lanes
finish. Docs-only PRs preserve the stable required contexts while unselected
functional lanes remain skipped. Python jobs restore uv's package/download cache
and selected pinned MinIO fixture archives, never `.venv` or `.mypy_cache`;
PR and merge-group runs cannot save cache entries. Every job creates a clean
environment with `uv sync --locked`,
then uses `uv run --no-sync` so a test command cannot silently resolve a new
environment. The uv executable version and official per-platform archive
SHA256 values are reviewed in `config/uv-toolchain.toml`; every setup step must
verify the matching checksum. Go's setup cache is disabled rather than allowing
PR code to save module or build-cache state. `dev` pushes skip the Python gate
because the squash-merged PR already produced the required context.

Frontend, SPA image/runtime-config, and frontend gate changes additionally
select the required `web-checks` job. `repository-checks` fails when that
selected job fails or is missing. Reproduce the complete frontend gate with a
frozen install:

```bash
cd web
npm ci
npm run typecheck
npm run lint
npm run test:coverage
npm run build
npx --no-install playwright install chromium
npm run test:e2e
```

The Playwright command defaults to the local `/dev` prefix. To exercise the
same generic production-build harness at the production basename without
contacting a live environment, use:

```bash
LOOM_E2E_ROUTE_PREFIX=/prod npm run test:e2e
```

`LOOM_E2E_ORIGIN` may select a different localhost port. Both inputs are
validated and never authorize staging or production access. The harness never
reuses an existing server: if the selected local origin is occupied, stop that
process or select another localhost port so Playwright can build and serve its
own browser-test bundle.

Vitest enforces statements, lines, and functions at 80% and branches at 75%;
only the generated `web/src/api/schema.d.ts` production source is excluded. The
Playwright gate serves the production build at `/dev` by default and supports
the same local contract at `/prod`. It exercises logged-out, user, and admin
routes at 1440x900 and 390x844, reloads deep links, and rejects empty roots,
page errors, unexpected console output, same-origin request failures, failed
browser assets, and script/style MIME mismatches. Axe must report zero serious
or critical violations. Its exact request/response fixtures are local-only and
contain no deployment credentials.

```bash
uv run --no-sync ruff check src tests packages database/migrations database/capacity_guard_migrations database/capacity_migrations
uv run --no-sync mypy
mapfile -t root_tests < <(uv run --no-sync python scripts/component_ownership.py test-paths --lane tests-root --test-scope nebius)
uv run --no-sync pytest "${root_tests[@]}" -m "not legacy_pool" -p no:cov
mapfile -t package_tests < <(uv run --no-sync python scripts/component_ownership.py test-paths --lane tests-packages --test-scope nebius)
uv run --no-sync pytest "${package_tests[@]}" -m "not legacy_pool" -p no:cov
```

For the historical all-platform fast-tier coverage check, select all modules
and include legacy cases explicitly. Both commands contribute to the report:

```bash
mapfile -t root_tests < <(uv run --no-sync python scripts/component_ownership.py test-paths --lane tests-root --test-scope all)
uv run --no-sync pytest "${root_tests[@]}" -m "legacy_pool or not legacy_pool" --cov=src --cov=packages --cov-report=term
mapfile -t package_tests < <(uv run --no-sync python scripts/component_ownership.py test-paths --lane tests-packages --test-scope all)
uv run --no-sync pytest "${package_tests[@]}" -m "legacy_pool or not legacy_pool" --cov=src --cov=packages --cov-append --cov-report=term
uv run --no-sync coverage report --fail-under=70
```

The existing CI and smoke workflow dispatches accept `legacy_compatibility=true`
to restore historical platform tests. On CI, also set `coverage_summary=true`
when the all-platform coverage report and floor are needed.

Local verification should use Python 3.11, matching the `repository-checks`
job. The repository root `.python-version` pins uv-managed virtualenv creation
to 3.11; if a local `.venv` was created with another interpreter, remove it and
rerun `uv sync --locked --all-packages --extra dev --python 3.11` before
running mypy.

Heavier suites are opt-in:

```bash
# Integration tier — Docker + Postgres + MinIO via testcontainers
uv run --no-sync pytest tests/integration                # full directory
uv run --no-sync pytest tests/integration -m "not docker" # exclude docker-marked tests
uv run --no-sync pytest tests/integration -m docker       # Docker driver and runtime tests

# System tier — full docker-compose stack
uv run --no-sync pytest tests/system -v

# Live Modal — requires provider credentials
LOOM_RUN_MODAL_INTEGRATION=1 \
  uv run --no-sync pytest tests/integration/test_modal_driver_live.py -v
```

The `not docker` subset can still require Docker for PostgreSQL and MinIO
fixtures. These directory/marker commands include retained compatibility tests;
for CI's current Nebius scope, use `scripts/component_ownership.py test-paths`
with `--lane integration` or `--lane integration-docker` and `--test-scope nebius`,
following the manifest-based fast-tier commands above.

MinIO fixtures preserve two pinned releases: the Testcontainers 2022 release
and the 2025 TLS fixture. Run `uv run --no-sync python -m tests.support.minio_images`
to prepare them before test deadlines. A cached original image is reused. If
Quay rejects a cold pull, preparation verifies the exact upstream source archive
checksum and builds the same release commit with its pinned original Go compiler.
The rebuilt image has an explicit `loom-minio-test-source` local tag and source /
recipe labels; it does not claim the original image digest. Invalid downloads,
failed builds, and version mismatches fail setup. Cold builds can take several
minutes and require public GitHub, Docker Hub, Go modules, and Alpine package
access. Registry timeouts still fail without an unbounded retry.

Fixture resolution is lazy. System smoke prepares the image before Compose and
passes `LOOM_SYSTEM_MINIO_IMAGE` to every startup phase. This override belongs to
`docker-compose.test.yml`; ordinary developer Compose and its existing storage
volumes keep their current image and are not modified by fixture preparation.

On GitHub, selected non-Docker integration tests are split into four disjoint,
contiguous ranges of the manifest-owned filename order. Contiguous ordering
preserves the suite's session-scoped Postgres setup/cleanup contract while the
four shards start directly after the planner, in parallel with the fast tier.
The ownership manifest also pins measured slow modules to the shorter shard;
these whole-file moves preserve filename order within each shard and keep every
test assigned exactly once. Adjust pins from CI timing evidence, not by omitting
tests or extending the job budget whenever the distribution becomes uneven.
The local commands remain serial equivalents so they are easy to reproduce.

Tests using `isolated_migration_postgres_url` receive separate disposable
databases cloned from an independently migrated, connection-disabled template.
The template runs the full application migration history once per test session;
each test keeps its own data and runs any requested upgrade/downgrade operations
in its clone. Neither the shared suite database nor another test's data is used
as the template. Clones and the template are removed during fixture teardown.

Every relevant non-draft PR runs its path-selected validation plan and emits
the four protected contexts. Drafts and unrelated metadata events use only a
`*-filtered` context. No label, author, reviewer, or merge coordinator grants
gate authority; validation labels only add work to the path-inferred plan.

Each protected name is the final aggregate job emitted directly by its source
GitHub Actions workflow. The aggregate runs with `if: always()` and fails when
the planner fails, a selected lane fails, times out, is cancelled, or is
unexpectedly skipped. Unselected lanes may be skipped only when the planner's
explicit boolean says they are not required.

There is no cross-workflow publisher, custom CheckRun, same-name commit status,
or retired failure. Inspect the exact head's four CheckRuns when validating CI;
all must come from the GitHub Actions app and complete successfully before
native squash auto-merge can merge the PR. Branch push aggregates use `*-push`,
so they never duplicate a protected name on a later `dev`-to-`main` promotion
that points at the same commit.

The `slow` marker is applied at module level on the heaviest 9 test
files (Docker driver lifecycle / exec / io / healthcheck /
network-policy + full trial e2e + Modal live). CI selects integration for
non-documentation changes. The ownership authority at
`config/component-ownership.toml` inventories every tracked Dockerfile and
Python, Go, or web test path. Schema v2 also declares the allowed CI lanes,
versioned runtime-payload execution policies, immutable container digests,
per-payload minimal fixture cases, and component smoke, scan, and attestation
owners. Every ownership CLI invocation validates the current manifest, tracked
inventory, and Python test syntax. Docker-marker detection skips AST traversal
only after parsing a marker-free ASCII source; Unicode and possible-marker
sources retain exact AST inspection. No prior validation verdict is cached.
Validate the whole inventory, inspect the exact isolated
payload plan, or query one path with:

```bash
python3 scripts/component_ownership.py validate
python3 scripts/component_ownership.py execution-plan --lane runtime-payload
python3 scripts/runtime_payload_conformance.py
python3 scripts/component_ownership.py query tests/integration/test_trial_e2e_docker.py
python3 scripts/component_ownership.py test-paths --lane frontend
```

When the authority exposes its frontend-lane query, `web-checks` consumes that
output instead of copying the owned web test patterns into the workflow. Until
that command is available, the job runs the complete Vitest suite. The quality
gate still owns thresholds, build/browser behavior, and the two specialized
route-smoke unit harnesses; component and test ownership remains the manifest's
responsibility.

The validator fails for missing or ambiguous ownership, stale patterns,
undeclared owner names, and a `pytest.mark.docker` module outside the Docker
tier. Runtime payloads are not executed as host pytest: each manifest-owned
file runs in its own read-only, networkless, resource-limited container against
the payload case's minimal synthetic passing-workspace fixtures. The immutable
image is pulled once, then every case runs with further pulls disabled. This
proves verifier conformance only; it is not proof that a real task or trial
succeeded. Planner, staging-start, and release consumers use manifest-derived
inputs. Rollout build, registry publication, and expected-image evidence use one ten-image
matrix generated from the fixed candidate worktree's eight
`rollout_role = "primary"` and two `rollout_role = "auxiliary"` components, then
persisted by the build step. The two sandbox conformance images are intentionally
excluded. Every change under `tests/integration/`
continues to select the Docker tier;
relevant runtime paths do too. `ci:integration` and `ci:integration-docker`
add those tiers when paths do not already require them. The selected smoke
gates cancel superseded PR runs, so a new push to the same PR stops the older
`cluster-smoke` or `staging-smoke` run instead of
building a queue of stale checks.

`images-gate` is separate from the fast tier. Relevant image paths select its
PR validation automatically; `ci:images` adds validation when paths do not
already require it, and `.github/workflows/images.yml` remains manually
dispatchable. Manual runs report `images-gate-manual`, not the protected
`images-gate` context; the same `*-manual` rule applies to all four protected
workflows. The image workflow plans a path-aware matrix so web-only changes
build only the web image, Dockerfile-only changes build the matching component,
and shared Python/runtime changes rebuild the affected Python images. Relevant
pull requests, merge groups, and manual dispatches use the checked-in read-only
build path, do not log in to GHCR, and do not use a publication cache. Manual
dispatch is build-only. Image validation builds the seven-image Nebius AMD64 set
on GitHub-hosted native CPUs. `nebius-candidate` publishes immutable images from
`dev`; the existing production gates promote that same candidate into `main`.
There is no GHCR publisher, ARM64 manifest joiner, or personal-dev publication
controller. `images` requests no publication credentials. See the
[workflow inventory](ci.md#workflow-inventory) for the remaining entry points.
Same-repository branch workflow code still runs on the read-only PR
path; autonomous-agent hard isolation requires
fork-only execution or an external trusted workflow/App.

`staging-smoke-gate` proves the credential-free Compose system smoke only. It
never enters `ci-aws`, and a missing or skipped real-AWS run is not represented
as cloud validation. Real AWS evidence belongs to a separately protected,
trusted post-merge/release workflow rather than the required PR context.

## Coverage gates

- **Ordinary PRs:** affected functional Python tests run without coverage instrumentation.
- **Daily regression:** CI runs full Python/Go/Web and both integration tiers at
  08:23 UTC on dev in Nebius/common scope, with coverage, without publishing or deploying.
- **Explicit coverage runs:** add `ci:coverage-summary` or dispatch CI with
  `coverage_summary=true`. This requests full root/package/integration tests,
  within the selected scope and produces the combined report. Nebius coverage
  is reported without reusing the historical all-platform floor. Main and
  `legacy_compatibility=true` coverage keep the **70%** fast-tier floor.
  Collection/report errors still fail the selected check in either scope.
- `ci:integration` requests the full functional integration lane without coverage.
- `coverage.xml` ships as a workflow artifact for external tools.

Use the manifest-selected commands in [Tests](#tests) for local reproduction.
For Nebius coverage, select `--test-scope nebius`, keep `-m "not legacy_pool"`,
replace `-p no:cov` with the coverage arguments shown there, and report without
`--fail-under=70`. The second command needs `--cov-append`; reporting before both
root and package tests finish gives an incomplete total. CI combines these
parallel results in the optional `fast-coverage` job; `coverage-summary` adds
integration coverage. `repository-checks` validates the selected results without
recomputing coverage.

## Workflow

Use issue-scoped PRs into `dev` for normal development; `main` is
release-only and receives promotion PRs from `dev`. See
[`../CONTRIBUTING.md`](../../CONTRIBUTING.md) for issue ownership, commit
style, PR requirements, and the definition of done.

New contributors should start from an open issue or discuss scope in a
new issue before implementing. PRs use
[`.github/PULL_REQUEST_TEMPLATE.md`](../../.github/PULL_REQUEST_TEMPLATE.md)
and must link the issue they advance. Maintainers mark actively owned
issues with a `[WIP] ` title prefix, keep the project status current,
and follow the normal `dev` auto-merge policy.

Every non-draft `dev` PR uses GitHub's native squash auto-merge. A developer or
maintainer enables it without considering author or reviewer identity. GitHub
keeps each candidate queued until `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate` are visible and successful on
the current head SHA. Those four strict, GitHub-Actions-app-bound checks are
the only merge authority: `dev` requires no human approval, no CODEOWNER
approval, and no conversation resolution. Maintainers should not manually
merge an eligible `dev` PR just because CI is green.

For `main`, enable native squash auto-merge only for a same-repository `dev`
production candidate. The active `main protected promotion` ruleset requires
only `main-promotion-gate`, which binds the open PR, current `dev` head,
successful release gate, and evidence artifact to one SHA. Direct pushes,
deletion, force-pushes, and bypasses remain prohibited.

Current `dev protected admission` ruleset:

- Squash-only (no rebase merge, no merge commits)
- `required_linear_history: true`
- `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
  `staging-smoke-gate` are the required stable status checks
- `allow_auto_merge: true`; a developer or maintainer enables it for each
  non-draft PR, and GitHub holds the candidate until the policy above passes
- no bypass actors; repository admins go through the gate too
- no human approval, no CODEOWNER approval, and no conversation resolution

Current `main` promotion boundary:

- only a same-repository `dev` -> `main` promotion with
  `main-promotion-gate` is eligible for native squash auto-merge;
- the composite gate requires the PR head and current `dev` head to equal the
  exact SHA that passed `release-promotion-gate` with an unexpired evidence
  artifact;
- production deployment still re-verifies candidate tree and artifact identity
  before releasing production credentials.

Secrets and side-effect workflows:
- Pull request workflows use read-only `GITHUB_TOKEN` permissions and
  must not receive publish or deployment secrets.
- PRs from forks or external contributor branches must not depend on
  protected secrets; maintainers can rerun protected workflows from a
  trusted branch when needed.
- The former Hugging Face benchmark publishing workflow is retired.
  Upstream benchmark fetching/adaptation and prepared-local publication remain
  separate supported paths; they do not restore that workflow or its environment.
- Deployment or publish workflow changes are public-repository security-boundary
  changes, so the fail-closed planner must select the full CI validation set. A
  platform-admin review may be requested for context, but it is not a `dev`
  merge gate.
