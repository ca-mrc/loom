# CI runner placement

Loom development validation and normal candidate publication use GitHub-hosted
runners by default. With `LOOM_CI_UNIFIED_RUNNERS=true`, eligible jobs share
native Actions labels across disposable OLDLAB KVM guests and disposable
GitHub-hosted donor VMs. GitHub assigns the jobs and owns their tokens, logs,
artifacts and CheckRuns. See [native runner supply](../contributing/ci-unified-runners.md)
for exclusions, capacity, activation and rollback. CI does not need Nebius
worker nodes.

The four protected source checks remain `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate`. GitHub's native queue validates
the merge-group SHA against the current base before squash integration; PR-head
checks establish queue eligibility. There is no bypass or replacement gate
publisher. See [merge authority](../contributing/required-checks.md).

Python root, package and integration tests run without coverage instrumentation
by default. Use the existing `ci:coverage-summary` selector or dispatch CI with
`coverage_summary=true` for the selected-scope coverage report. The historical
70% floor applies only to all-platform compatibility validation.
The selector adds work and never suppresses relevant tests. Daily CI also runs
Nebius/common regression and coverage at 08:23 UTC.
Historical tests are available through CI dispatch `legacy_compatibility=true`.

Dev PR image builds select from the seven AMD64 Nebius platform images.
Retired personal-development fleet images have no publication lane. Main
production promotion retains its protected contract. macOS CLI
compatibility retains its separate hosted locked-environment workflow.

Changing the routing flag affects future jobs; already queued common-label
jobs must drain before their supplies are retired. The runner-supply runbook
owns that procedure.

For a PR that edits only unreferenced Python test modules, each test lane runs
only the edited modules assigned to it. A test module referenced elsewhere is
potentially a shared fixture and retains full validation. Runtime, migration,
configuration, fixture, deleted-file and unknown changes also retain full lanes,
except explicitly audited unrelated components in the ownership manifest.
A CI selector label or manual CI dispatch requests full regression within the active test scope. Ordinary
labels do not affect selection. Empty selected
shards finish without invoking pytest; failures while selecting files still fail
the job. Tests are assigned to shards before filtering so ownership stays stable.

Current Go execution and sandbox runtimes are validated in `go-checks`. The
retired builder supervisor/publication handoff tests are deleted. Coverage collection is
optional, but test failures and all four protected source checks remain required.
