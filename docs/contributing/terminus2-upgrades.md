# Keep Terminus-2 aligned with Harbor

Loom directly imports Harbor's `Terminus2`. Upstream owns prompts, parsing,
Chat and the agent loop; Loom connects the environment, Gateway, deadlines,
durable events and artifacts. Track remaining alignment in [#2390](https://github.com/qianyi-sun/loom/issues/2390).

## Discover and prepare an upgrade

[`config/harbor-runtime.json`](../../config/harbor-runtime.json) is the canonical
source revision and package version. Active Dockerfile pins and provenance
fallbacks are synchronized consumers. Validate them without network access:

```bash
python3 scripts/ops/harbor_upstream.py pins
```

Discover the latest `main` commit and read its version at that exact SHA:

```bash
python3 scripts/ops/harbor_upstream.py check --check-pins
```

The report distinguishes current pin, latest upstream, comparison link and
whether frozen worker dependency evidence needs regeneration. Add
`--require-aligned` to return exit 1 on upstream drift; API, metadata and local
pin failures return exit 2. No model or cloud resources are used.

Prepare an explicit candidate after reviewing the reported SHA and version:

```bash
python3 scripts/ops/harbor_upstream.py update \
  --revision <full-upstream-sha> --version <package-version> --dry-run
python3 scripts/ops/harbor_upstream.py update \
  --revision <full-upstream-sha> --version <package-version>
```

This updates only the manifest, both runtime Dockerfile pins and runtime
provenance fallbacks. It preflights every known consumer before writing. It does
not rewrite patches, loosen dependencies or fabricate frozen build evidence.
Rebuild and regenerate the existing worker dependency evidence with
`scripts/ops/update_worker_image_lock.sh` before considering the candidate Ready.

## Automatic candidates

The `Harbor upstream candidate` workflow discovers changes daily, or through
manual dispatch. GitHub requires the workflow on the default branch; this
repository currently uses `dev`. Manual `check_only=true` is read-only.

Candidate creation requires GitHub Actions permission to create pull requests
and the repository variable `LOOM_HARBOR_UPSTREAM_PR_ENABLED=true`. Discovery
still runs while publication is disabled, then reports an actionable failure
before any branch push. The workflow maintains one Draft at
`bot/harbor-upstream`, preserves human amendments, and stops updating a Ready
PR. After creating/updating the Draft, it builds the candidate production image
and runs both real-Harbor probes offline. A failed patch/build/probe leaves the
Draft and an actionable run summary. Success still requires real worker
dependency regeneration and runtime acceptance before Ready. Conflicts fail
visibly. It never force-pushes, enables auto-merge or changes a deployed runtime.

The repository currently disables Actions-created PRs. GitHub's setting combines
creating and approving PRs, so changing it is an operator decision; this workflow
only creates/updates candidates. No alternative token is copied into CI.

`GITHUB_TOKEN` writes do not trigger ordinary PR checks. After developer
verification, a collaborator marks the candidate Ready, starting the existing
four source-workflow gates, and may enable GitHub-native squash auto-merge.
This adds no branch-required context.

## Behavior and existing differences

Run [the real Harbor probes](../../tests/conformance/terminus2/README.md) before
Ready. The existing Harbor image job executes them in the built production
image with networking disabled. This catches upstream API/behavior changes and
trajectory projection loss that fake-agent tests or successful imports cannot
detect. It reuses the image build and adds only short offline probes.

New runtime provenance events record the effective `max_turns`,
`enable_summarize`, recording, continuation and multi-model options, excluding
credentials. Version equality and effective-configuration equality are separate.

These differences remain under #2390; passing conformance does not erase them:

| Difference | Reason and removal condition |
|---|---|
| `max_turns=50` | Existing Loom limit; upstream has no practical default limit. Removing it requires nullable limits through hosted callers while retaining absolute deadlines. Explicit limits must remain reproducible. |
| `enable_summarize=False` | Enable the upstream default only after forced-summary tests prove prompt/history behavior, unambiguous main/summary Gateway-call joins, auxiliary artifact delivery and export. |
| Recording enabled | Required native artifact capture; follows the current upstream recording default. |
| Current-user probes, UTF-8 locale fallback, tmux 1.x paste locking | Local patch supports existing task images. Remove each part when selected upstream covers it and the corresponding compatibility tests pass; never silently drop a failing patch. |
| Instance-local tmux recovery | Recreates a session once without replaying dispatched keys and reports changed shell state. Remove when upstream provides equivalent loss handling and no-replay guarantees. This fault path is outside ordinary-path parity. |
| `continue_until_timeout` | Explicit Loom task extension with separate real-loop tests; not upstream-default behavior. |
| Multi-model routing | Explicit extension coupled to private Harbor methods. Existing routing tests must pass on upgrades; outside the single-model differential contract. |
| Credential omission | Native constructor metadata is scrubbed before publication. Never compare secrets or bypass redaction for parity. |

## Publish, qualify and select

Use the existing `nebius-candidate` workflow's `harness-only` mode, then register
the publisher's original runtime release JSON. See
[runtime versions](../runbooks/harbor-runtime-versions.md). Qualification requires
ordinary-member exact-version execution, real model calls, independent numeric
verifier output, canonical trajectory download and resource cleanup.

Only after acceptance should a candidate become the default for new Batches.
Existing Batches and failed-case reruns keep their frozen runtime binding. A
detected commit, green offline probe, published image or registered version alone
does not establish acceptance or justify changing the default.
