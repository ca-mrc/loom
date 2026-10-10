# Required checks and merge authority

## Decision

Loom uses the four final GitHub Actions jobs already emitted by its validation
workflows as pull-request merge authority:

- `repository-checks`
- `images-gate`
- `cluster-smoke-gate`
- `staging-smoke-gate`

The canonical repository is [`ca-mrc/loom`](https://github.com/ca-mrc/loom).
Collaborators with write access are trusted to change code and CI; no separate
GitHub App or CODEOWNER approval is required for `dev` integration.

The `dev protected admission` ruleset requires these four checks from the
GitHub Actions app and sets strict required-status-check evaluation to `true`.
GitHub's native merge queue uses squash integration. PR-head checks establish
queue eligibility; the same four workflows validate the generated merge-group
SHA against the current base before integration. Advancing `dev` alone is handled
by the queue. Conflicts or amendments require a branch update and new checks.

## Check ownership

Each source workflow plans its selected validation lanes and exposes one final,
fail-closed aggregate job under its stable required name. GitHub Actions owns
the CheckRun directly. There is no cross-workflow publisher, custom CheckRun,
same-name commit status, retired failure, or custom merge controller.

Push-triggered image publication and deployment remain separate from
pull-request admission. A publication failure makes the merged commit
unreleasable until repaired; it does not rewrite the pull request's admission
result.

Eligible pull requests use GitHub-native auto-merge/queue admission. A developer
or maintainer enables it after the current head is ready; GitHub performs squash
integration after the required merge-group checks. No workflow has
`contents: write` merely to enable auto-merge, and manual dispatch checks do not
substitute for either PR or merge-group validation.

The checked-in contract is not a readback of live GitHub settings. Verify the
rulesets and current-head check ownership before integration; preserve all
required checks and the empty bypass list. See [contribution policy](../../CONTRIBUTING.md).
