# Required checks and merge authority

## Decision

Loom uses four final native GitHub Actions jobs in `CI` as pull-request merge
authority:

- `repository-checks`
- `images-gate`
- `cluster-smoke-gate`
- `staging-smoke-gate`

The canonical repository is [`ca-mrc/loom`](https://github.com/ca-mrc/loom).
Collaborators with write access are trusted to change code and CI; no separate
GitHub App or CODEOWNER approval is required for `dev` integration.

The `dev protected admission` ruleset requires these four checks from the
GitHub Actions app and sets strict required-status-check evaluation to `true`.
When GitHub's native merge queue is enabled, PR-head checks establish queue
eligibility; the same CI dependency graph validates the generated merge-group SHA
against the current base before squash integration. In that mode, advancing
`dev` alone is handled by the queue. Conflicts or amendments require a branch
update and new checks.

While the queue is disabled, native squash auto-merge retains the same four
checks and strict latest-base protection. The branch writer updates an outdated
head and earns checks again. Follow the integration order and live queue
coordination in [#2416](https://github.com/ca-mrc/loom/issues/2416) to avoid
unnecessary competing base updates and validation runs.

## Check ownership

CI runs selected quick checks and affected tests first, then calls the image,
cluster and staging workflows with native `needs` dependencies. They retain
their existing build/test assertions and manual diagnostics. No runner waits
for another workflow over the API.

All four stable required names are direct jobs in the caller CI run, bound to
the original PR or merge-group event. Each specialist gate requires successful
feedback and its selected reusable workflow result; unexpected skips fail
closed. Unselected specialist lanes must be skipped. Nested result jobs and
manual diagnostics cannot replace the four bare names. GitHub Actions owns
each CheckRun directly (app id `15368`). There is no cross-workflow publisher, custom CheckRun,
same-name commit status, retired failure, or custom merge controller.

Push-triggered image publication and deployment remain separate from
pull-request admission. A publication failure makes the merged commit
unreleasable until repaired; it does not rewrite the pull request's admission
result.

Eligible pull requests use GitHub-native auto-merge. A developer or maintainer
enables it after the current head is ready; GitHub performs squash integration
after the required checks, including merge-group checks when the queue is enabled. No workflow has
`contents: write` merely to enable auto-merge, and manual dispatch checks do not
substitute for either PR or merge-group validation.

The checked-in contract is not a readback of live GitHub settings. Verify the
rulesets and current-head check ownership before integration; preserve all
required checks and the empty bypass list. See [contribution policy](../../CONTRIBUTING.md).
