# Contributing

> **Canonical repository:** use
> [`ca-mrc/loom`](https://github.com/ca-mrc/loom) for new branches,
> pull requests, and issues.

> Loom is public-readiness hardened and is operated as an issue-scoped
> GitHub-flow project. Normal
> changes land through PRs into `dev`; `main` remains reserved for
> release promotion from `dev`. Every relevant non-draft PR reports the four
> stable validation contexts: `repository-checks`,
> `images-gate`, `cluster-smoke-gate`, and
> `staging-smoke-gate`. The shared planner selects the applicable validation
> work from changed paths, while labels can request additional validation.
> Labels may add validation but cannot remove path-inferred validation. The
> developer or maintainer enables GitHub's native squash auto-merge for every
> non-draft PR, independent of author or reviewer identity. GitHub waits for
> every required gate on that current head SHA before adding it to the `dev`
> merge queue. The queue validates a merge group against the latest `dev` and
> earlier queued changes before performing the squash merge.
> Static documentation is a location-and-format allowlist; unknown runtime
> paths fail safe to the full validation set. Manual runs report `*-manual`
> contexts and cannot satisfy protected PR contexts. These four strict,
> GitHub-Actions-app-bound checks on PR heads and queued merge groups are the
> only merge authority for
> `dev`: it requires no human approval, no CODEOWNER approval, and no
> conversation resolution. CI or release-authority changes select full CI but
> add no human merge gate.
> External pull requests are accepted for issue-scoped work that follows
> the templates below. Workflows that need publish or deployment secrets
> must use protected GitHub Environments and must not expose secrets to
> pull request code.

## Active Workflow

### 1. Per-task TDD commit granularity

Each TDD cycle gets its own commit:

```
test(loom.driver): NetworkPolicy enforces allowlist
feat(loom.driver): add NetworkPolicy enforcement
fix(loom.driver): iptables guard for vanilla images
```

The trail documents the build-test-fix progression for later audit.
PR-mode merges squash the per-task commits. The repo forbids
merge commits and rebase merges, so the per-task history stays on the
pre-merge branch and remains recoverable via the PR's "commits" view.

### 2. Commit Style

Concise imperative messages. Prefix with `<scope>:` matching the changed
package or surface:

- `feat(loom.driver): add NetworkPolicy enforcement`
- `feat(loom_service): GET /api/v1/usage rollup`
- `feat(web): SPA scaffold + read pages`
- `fix(loom_drivers.modal): teardown ordering`
- `docs: update --task id format + --backend fake caveat`
- `chore(ci): bump actions/checkout v4→v5`

Multi-paragraph bodies are encouraged for non-trivial commits — explain
the *why* (not the *what*; diffs show the what). Include a
Co-Authored-By trailer if pair-programming with Claude:

```
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

## Definition Of Done

Before declaring work shipped:

- `uv run --no-sync ruff check src tests packages database/migrations database/capacity_guard_migrations database/capacity_migrations` passes
- `mypy --strict` passes
- `pytest tests/unit tests/contract tests/property tests/loom_cli`
  passes (the CI gate runs the same set + sibling packages)
- `pytest tests/integration` passes for Docker-touching changes
  (use `sg docker -c "pytest tests/integration"` if your shell lacks
  the docker group)
- `README.md` / relevant `docs/architecture/*.md` updated if the
  architectural surface changed
- All affected Markdown docs were scanned and updated for code,
  workflow, deployment, runtime, dependency, or contract changes
- **Architecture changes that depend on Docker / k8s / CNI / kernel
  primitive behavior**: add or update focused automated coverage under the
  owning test lane so CI empirically verifies the mechanism
- Post-ship self-audit pass (re-read each touched file once more for
  regressions, edge cases, type drift) - separate commit if anything
  surfaces

## Owner-Local Files

`AGENTS.md`, `MEMORY.md`, and `NEW_SESSION_BRIEFING.md` at the repo
root are owner-local context files, gitignored. They are not part of
the project artifact and must never be committed.

## Documentation and repository layout

Use the [repository map](docs/contributing/repository-layout.md) and
[documentation index](docs/index.md) to place durable project material.
Architecture pages describe implemented contracts; runbooks describe repeatable
procedures; contributor pages describe development and validation. Update the
owning index and all inbound references when moving or deleting a page.

Implementation plans, execution checklists for individual changes, session
notes and agent scratch work belong **outside the repository**, including its
ignored directories. Do not commit them under `docs/`, package directories or
an archive. `scripts/check_repository_paths.py` rejects known planning paths and
implementation-plan filenames; reviewers also check content, since a filename
cannot distinguish every plan from a durable contract.

Use Git history for superseded source and docs. Keep only concise, clearly
marked decision or data-lineage records under `docs/historical/`. Generated
run reports belong in external evidence artifacts. Published migration chains,
package layouts and vendored dependency boundaries are not cosmetic clutter.

## Branching

- `feature/<short-name>` for product or platform features
- `infra/<short-name>` for deployment and infrastructure work
- `docs/<short-name>` for documentation-only changes
- `fix/<short-name>` for defects
- `research/<short-name>` for exploratory prototypes

Feature branches, including Nebius work, cut from `dev`. PRs target `dev`.
`codex/nebius-main` records the earlier isolated integration series. `main` is reserved
for release promotion PRs from `dev`.

The [Nebius CI integration](docs/contributing/ci.md) retains `dev`'s four protected
checks and uses GitHub-hosted runners with amd64 PR image builds. Python tests
normally run without coverage instrumentation. Use `ci:coverage-summary` or the
CI dispatch input `coverage_summary` for coverage accounting within the selected
scope. The historical 70% floor applies only to all-platform compatibility runs. `ci:integration` requests full functional integration tests
without coverage. Independent test-only edits select their owning test files;
shared-fixture, deleted-test and cross-module fixture changes select all Python
test consumers and retain complete lanes, including in mixed source/test diffs. Audited component
selection avoids unrelated backend jobs and schema-reference provisioning. A daily
Nebius/common regression at 08:23 UTC retains complete selection within that scope
and coverage reporting. Use CI dispatch `legacy_compatibility=true` to include
historical platform tests and both integration tiers. Dev PRs
use the seven-image Nebius set. Retired personal-development fleet images are
not built or published.

Deployment environments are separated from branch workflow. The retained
`https://yylx.world/dev`, `https://yylx.world/staging`, and
`https://yylx.world/prod` routes describe the earlier installation,
not a three-environment ceiling or a mandate for newly provisioned Nebius hosts.
The [managed environment contract](docs/architecture/nebius-primary-platform.md#managed-environment-identity-and-rendering)
uses distinct hostnames behind shared HTTPS ingress; its renderer is currently
execution-disabled and does not itself establish provisioned entrypoints.
Use only the hostname verified for the selected deployment; do not invent
environment-specific yylx subdomains. Production follows `main` or
immutable `vX.Y.Z` production release tags and uses the protected GitHub
Environment named `production`; normal development jobs must not use production
kubeconfig, database, object-store, provider, SecretStore, or worker-token
secrets.

## Issue Ownership

- Work from an issue with acceptance criteria before opening a PR.
- Comment on the issue before starting substantial work. Maintainers
  mark actively owned issues with a `[WIP] ` title prefix and keep
  assignee + project status current so contributors can avoid duplicate
  work.
- Use `Refs #N` when a PR advances an issue but does not complete the
  whole acceptance scope. Use GitHub auto-close keywords only when the
  PR fully satisfies the issue.

## Pull Requests

- Link to a GitHub issue with acceptance criteria
- Keep one concern per PR
- Use the PR template
- The four stable gate contexts are `repository-checks`, `images-gate`,
  `cluster-smoke-gate`, and `staging-smoke-gate`. The shared planner selects
  their validation work from changed paths; labels request additional work but
  cannot turn off path-inferred work. A new non-document path without a known
  owner selects every heavy lane until its ownership is declared.
- For a completed non-draft `dev` PR, a trusted collaborator enables GitHub's
  native squash auto-merge. No workflow or custom controller enables or performs
  the merge. All four required gates must be visible and successful on the
  current PR head before queue admission. GitHub's native merge queue then
  requires those same four app-bound gates on its generated merge-group SHA,
  which includes the latest `dev` and any earlier queued changes. If another PR
  advances `dev`, GitHub rebuilds and validates the affected merge group;
  authors do not need to rebase solely because the base advanced. Update the
  branch when its code conflicts or needs an amendment, then validate its new
  head. Keep the four strict, app-bound checks as the
  only merge authority: `dev` requires no human approval, no CODEOWNER
  approval, and no conversation resolution. Do not hand-merge an eligible
  `dev` PR just because CI is green.
- Every non-draft PR event, including description edits and unrelated label
  changes, starts a complete required-check cycle. A newer filtered metadata
  workflow can leave native auto-merge blocked even when older checks on the
  same head are green. The changed-path planner still selects the applicable
  work; reporting all four gates does not force every heavy lane. Finish PR
  description and label updates before the final CI cycle to avoid restarting
  validation. Draft events remain filtered and cannot authorize a merge.
- Do not assume labels are the only way to select validation. For example,
  relevant image paths select `images-gate` automatically; `ci:images` adds
  Nebius AMD64 image validation when the changed paths do not already require it.
  In the checked-in workflow, pull requests, merge groups, and manual
  dispatches build with a read-only token, do not log in to GHCR, and do not
  use a shared publication cache. The `images` workflow has no publication
  permissions or push trigger. `nebius-candidate` publishes the immutable Nebius
  images from `dev`; production promotes those same digests through the existing
  `dev` to `main` gates without rebuilding or republishing them to GHCR.
  The old personal-dev controller and GHCR publisher have been removed.
  See the [workflow inventory](docs/contributing/ci.md#workflow-inventory) for
  the retained validation, publishing, and promotion entry points.
  This is not a repository-wide sandbox for
  same-repository writers: branch workflow code still runs from the PR branch.
  Autonomous agents need a fork-only
  execution boundary or an external trusted workflow/App before this can be
  treated as a hard token ceiling.
- `staging-smoke-gate` is a credential-free cluster validation lane. It does not
  request the `ci-aws` environment and does not treat a skipped real-AWS check
  as successful cloud evidence. Real AWS validation is a separate trusted,
  post-merge/release activity and cannot satisfy this protected PR context.
- For `main`, the active `main protected promotion` ruleset accepts only a
  same-repository, current-head `dev` production candidate. Its sole required
  context is the direct `main-promotion-gate` job, which verifies the open PR,
  current `dev` head, successful `release-promotion-gate` run, and unexpired
  evidence artifact all identify the same SHA. The four heavy gates remain the
  protected admission authority for changes entering `dev`.
- Changes to `.github/**`, CI validation-selection policy, and production release
  verification select full CI. Rollout authority, deployment, and release-gate
  changes also select every heavy lane; `cluster-smoke-gate` renders and audits
  both staging and production without credentials. Manual dispatches use
  distinct `*-manual` check names and cannot satisfy branch protection.
- Squash merge is the only allowed merge method, keeping `dev` linear
- Do not add credentials, private endpoints, local environment files, or
  generated run artifacts
- Do not expect publish/deploy secrets to be available in PR workflows
- External pull requests are accepted for issue-scoped work. If an issue
  is ambiguous, discuss the scope in the issue before implementing.

Before publishing a completed change, fetch the latest `dev`, update your own
branch, and exercise the changed behavior locally. Reproduce a bug before its
fix and run the same regression afterward. Run focused tests and static checks
on the final diff, then record the commands, environment, results, and any
unexercised acceptance boundary in the PR's `Verification` section. Complete
known amendments before pushing that head. After a push, confirm GitHub reports
the intended PR head SHA before marking the change Ready and enabling
auto-merge; push completion can precede the PR head readback. Keep Draft for
genuinely incomplete work; it is not a waiting-room state for finished PRs.

Include the consumers of a changed contract in this verification. A runner
interface change also needs compatible test doubles and cancellation/cleanup
tests. A migration-head advance needs the corresponding deployment policy and
positive schema fixtures updated together, while retaining deliberate old-head
upgrade tests and drift-rejection cases. Do not replace a pinned compatibility
assertion with an unconditional "whatever the current head is" check.

When CI fails, inspect the current head's failing source job and fix a
deterministic error before requesting another attempt. Distinguish an actual
failure from a cancelled superseded run or a filtered draft event. Repeating
the same failing head uses runner capacity without resolving the defect. These
are developer verification practices, not a fifth required check or an
additional approval gate.

## Release Flow

1. Let changes enter `dev` through its four protected heavy gates, then select
   the exact stable current `dev` SHA.
2. Open a release issue and bump
   `pyproject.toml [project] version` (root + any published
   `packages/<name>/pyproject.toml`); the GitHub release notes are
   the user-facing changelog (auto-generated from squash-merge PR
   titles between tags)
3. Choose the new immutable SemVer prod
   tag (`vX.Y.Z`), deploy that image tag to staging, and
   run `.github/workflows/release-promotion-gate.yml` with the structured
   release evidence manifest. Its `release_owner_approval` and Production
   Environment approval are distinct controls and are not interchangeable with
   CI merge authority.
4. While `dev` still points to that exact candidate, open the same-repository
   `dev` to `main` PR. Dispatch `.github/workflows/main-promotion-gate.yml`
   from `dev` with the candidate SHA, PR number, and successful release-gate
   run ID. If `dev` advances, select and validate the new head instead of
   reusing stale evidence.
5. Enable GitHub's native squash auto-merge after `main-promotion-gate` passes.
   The `main` ruleset requires only that composite gate; direct pushes,
   deletion, force-pushes, and bypasses remain prohibited.
6. Deploy production from `main` with `candidate_sha`, `image_tag`, and
   `release_gate_run_id`. The production deployment workflow rejects missing
   or mismatched gate evidence before it can use production environment
   secrets.
7. Tag the promoted `main` commit with the recorded `vX.Y.Z` tag to create
   the GitHub release. Never force-move or reuse a published prod tag; rollback
   deploys the previous recorded tag or image digest instead.

## Repository Hardening (maintainers)

These settings are part of Loom's public-readiness posture and must stay
on. They are listed here, not in the user-facing README, so maintainers
have one place to audit them.

- **Default `GITHUB_TOKEN` permissions are read-only.** The required
  `repository-checks` workflow inherits read-only token scope; jobs that
  need to write must opt in per-job and per-permission.
- **Publish and deploy workflows use protected GitHub Environments.**
  Secrets for benchmark-bundle publish or infrastructure deploy live in
  protected Environments so they are not available to pull request code.
- **The active `dev protected admission` ruleset** requires the native merge
  queue and the strict, GitHub-Actions-app-bound contexts `repository-checks`,
  `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`, blocks direct
  pushes, deletion, and force-pushes, and enforces squash-only linear-history
  merges. It has no bypass actors and requires zero human approvals, zero
  CODEOWNER approvals, and zero conversation resolution; these four CI checks
  are required on both PR heads and the queue's merge groups. The initial queue
  builds one merge group at a time and merges one PR per group.
- **The active `main protected promotion` ruleset** requires a pull request,
  and only the GitHub-Actions-app-bound `main-promotion-gate` context. That
  direct job binds the current `dev` head, same-repository PR, successful
  release gate, and evidence artifact to one candidate SHA. The ruleset is
  squash-only and linear, has no bypass actors, and blocks direct pushes,
  deletion, and force-pushes. Its status check is intentionally non-strict;
  candidate freshness is enforced by the composite gate itself.
- **Auto-merge is native and author-neutral.** `allow_auto_merge=true` lets a
  developer or maintainer enable GitHub's squash auto-merge on every eligible
  non-draft PR. For `dev`, this requests queue admission once its PR checks pass;
  the queue performs the squash merge after its merge-group checks pass.
  Enabling auto-merge grants no bypass. `main` uses the separate protected
  promotion ruleset described above.
  Verify remote rulesets through the GitHub API or repository settings rather
  than treating this checked-in description as live evidence.
- **Selected Actions sources** restricts third-party actions to the repository
  allowlist. Every remote `uses:` reference must also match the full commit SHA
  and upstream version recorded in `config/ci-actions-lock.json`; run
  `uv run --no-sync python scripts/check_ci_action_pins.py` when adding or updating an
  Action.
- **The uv toolchain is content-verified.** Keep the exact uv version and the
  official macOS arm64/Linux x86_64/Linux arm64 archive SHA256 values in
  `config/uv-toolchain.toml`; repository contracts require every setup step to
  use the matching checksum.
- **Secret scanning** is enabled at the repo level.

External pull requests for issue-scoped work go through the same CI
gate; they cannot reach publish/deploy secrets because those live in
protected Environments rather than as repository secrets.
