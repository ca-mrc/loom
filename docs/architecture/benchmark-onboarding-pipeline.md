# Benchmark Catalog and Onboarding

Loom's native benchmark catalog is operator-managed. Catalog entries identify
an adapter, immutable source revision, task-manifest location, compatibility
metadata, readiness state, and activation profile. Runnable task rows and
their bundles are persisted in Postgres and object storage.

User-owned collections use [TaskSets](user-brought-tasksets.md) instead of
creating native benchmark rows.

## Sources of truth

- `config/benchmarks.toml` declares repository-known benchmark adapters and
  local fixtures.
- Adapter code under
  `packages/loom-benchmarks/loom_benchmarks/adapters/` converts upstream
  examples into Loom `TaskConfig` bundles. The Terminal-Bench adapter is the
  separate `packages/loom-benchmark-terminal-bench-2/` workspace package.
- Registered `benchmarks` and `tasks` rows are the service catalog used by the
  SPA and evaluation APIs.
- Catalog rows and immutable object-store bundles retain task IDs, checksums,
  bundle locations, source revision, and profile identity. Historical
  publications also have object-store or Hugging Face manifests.

Registration never treats an unversioned mutable upstream dataset as runnable
authority. Task IDs are normalized and unique within a benchmark, bundle
hashes are verified, and profile activation is atomic.

## Operator commands

The `loom datasets` group provides the current lifecycle:

```text
loom datasets sync-config
loom datasets import
loom datasets publish --benchmark SLUG
loom datasets publish PATH
loom datasets register
loom datasets audit
loom datasets activate --profile PROFILE --descriptor PROFILE.json
loom datasets verify
loom datasets provision-catalog
```

- `sync-config` reconciles `config/benchmarks.toml` with catalog rows.
- `import` converts tasks, uploads bundles, and inserts rows directly.
- `publish` accepts an upstream adapter or a prepared local folder. Both use
  the same object-store publisher and catalog registration backend.
- `register` verifies a historical published manifest, upserts benchmark/task
  rows, and can mirror bundles into internal object storage. New publications
  already register their rows and do not need this second command.
- `audit` reports catalog, manifest, task-config, verifier, adapter,
  architecture, and readiness blockers. `audit --verify-bundles` downloads the
  complete internal bundle for every benchmark and TaskSet row, recomputes the
  canonical checksum across all files, and fails closed on drift or unreadable
  content.
- `activate --profile PROFILE --descriptor PROFILE.json` admits the exact
  physical catalog through one format-neutral operation. `--harbor-source`
  normalizes the native Harbor descriptor into the same contract. Admission
  alone never changes a public alias. Add `--alias SELECTOR` and either
  `--expected-current PROFILE` or `--expect-absent` to compose admission and
  compare-and-swap alias promotion atomically.
- `verify` samples registered tasks and runs the oracle path end to end.
- `provision-catalog` copies selected benchmark, task, agent and bundle facts
  between controlled environments. New destination profiles remain pending. An identical repeat preserves an
  already admitted destination only when its own retained journal authority
  remains available. For
  journaled sources, the existing publisher transfers the immutable inventory
  and registers actual destination versions, including transport objects outside
  task prefixes. It never copies source-store VersionIds or qualification flags.
  Legacy sources retain their existing authoritative audit boundary. Run the
  canonical activation command against the destination after restoring.

Credentials are read from environment or credential-source files. Do not put
database, object-store, provider, or Hugging Face tokens in command arguments
or catalog files.

## Readiness

Catalog admission, selected-target compatibility, explicit product support
policy and runtime qualification are separate axes in the API, CLI and SPA.
New publish/register/import/config-sync profiles start `pending`. Shared
admission validates the pinned origin, full/subset census, exact selected task
identities, canonical configurations, import blockers, supported execution
contracts and available retained source authority. Only that owner can make a
profile `runnable`. License metadata remains descriptive.

A valid profile can be admitted before Oracle/model qualification. An admitted
profile may still be incompatible with the selected target; exact accelerator
and architecture requirements reject count, dry-run and submission before
Batch/Trial/model creation. Quota/capacity pressure remains a queue/preflight
condition. Product support/deferred records live in reviewed platform catalog
policy; an uploaded descriptor cannot enable an excluded workload.

Runtime qualification remains `unknown` in a catalog projection without exact
task/source/config/runtime/harness-bound Trial/result/artifact/export/cleanup
evidence. Existing results retain their own scope. A copied `runtime_verified`
flag, admission or green CI cannot establish qualification; no new aggregate
or blanket verified flag is manufactured.

Identical execution facts retain admission and observations across republishing;
display/license metadata changes do not demote them. A different pinned
revision/release/subset needs a new physical profile. Legacy in-place content or
conversion changes fail closed to pending during migration. Historical profiles
cannot be republished or reactivated. Historical Batch/Trial snapshots, original
requested selectors and frozen source references are never rewritten.

### Neutral descriptor

For a reviewed JSON adapter, the descriptor has no Harbor fields or optional
dataset SDK dependency:

```json
{
  "id": "example-json@7",
  "origin": {
    "kind": "json",
    "locator": "https://example.test/tasks.json",
    "revision": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "release": "7",
    "importer": "loom.json.v1"
  },
  "expected_task_count": 2,
  "selected_task_ids": ["one", "two"],
  "selection": "full",
  "validation_contract": "canonical.v1"
}
```

`expected_task_count` is the complete upstream census, even for a subset. A
subset declares `selection: "subset"`, lists exactly the selected identities,
and repeats those identities in `origin.subset`. The publisher retains the
selection facts; activation cannot replace them with an uploaded count. Task
rows are `PROFILE/IDENTITY` and bind to canonical config and existing source
registration. Format-specific package/layout/verifier checks are ordinary
reviewed validators in `benchmark_admission_contracts`; the descriptor cannot
register code, disable isolation or choose a weaker validation contract.

For an existing local folder (`s3-folder` or `local-folder`), an empty origin
revision means there is no separate upstream revision; canonical task checksums
and verified source facts bind the content. Remote Git/HF origins always require
a resolved 40-character commit. For an authored root task whose catalog ID is
exactly the profile ID, supply `selected_task_ids: ["root"]` and explicit
`catalog_task_ids: ["PROFILE"]`. Ordinary tasks omit that positional mapping and
use `PROFILE/UPSTREAM_ID`.

Pre-journal S3/HF registration uses a fresh reviewed materialization through the
existing bundle registration boundary: it compares registered checksum, authored
canonical config, file modes and compatibility. HF bundle sources require an
immutable commit; S3 access uses the operator's ordinary credential references.
Local fixture sources require the explicit trusted `--fixtures-root` (or
`LOOM_WORKER_FIXTURES_ROOT`). Uploaded audit/count JSON is never authority. A
source with journal identity/markers always uses that journal; retired or
unavailable sources cannot fall back to a legacy byte audit. Remote profiles
whose original upstream revision was never recorded need a new pinned import;
an operator cannot invent that provenance by uploading a descriptor.

### Compatibility and migration

`activate-native --harbor-source` and the old TB2.1 positional
`activate terminal-bench-2 --profile terminal-bench-2@tb2.1-r6 --audit-json ...`
are deprecated normalization wrappers around shared admission. The TB2.1
validator still performs fresh package-lock, exact-byte, verifier and private
workspace checks. Uploaded audit JSON is an expected snapshot, never validation
authority. The default public alias retains that reviewed profile/audit policy;
an arbitrary admitted profile cannot claim it.

Remove the wrappers after maintained operator/runbook callers use the canonical
command, both compatibility paths retain passing positive/negative parity tests,
and one normal catalog maintenance cycle confirms no wrapper users remain.
There is no alternate lifecycle or policy feature flag.

Metadata bootstrap uses the adapter's physical profile ID, not its entry-point
alias, and leaves missing source revisions unpinned. First real registration can
pin an empty, unbound metadata placeholder; it remains pending. Existing
alias-named seed placeholders are not automatically deleted or renamed. If
readback finds a selector collision, inspect its tasks/source references and
explicitly resolve the conflicting metadata before promotion; existing aliases
and frozen execution reads are retained.

Migration `0180` changes only the insert default from runnable to pending. It
does not mass-demote accepted catalogs, move aliases, rewrite in-flight work or
invent historical execution evidence. Downgrade restores only the old default;
it cannot reactivate profiles. Before an authorized rollout, record destination
profile/alias readback, apply the additive default migration, then read back the
same accepted rows and frozen executions. Source adoption uses the existing
journal or existing legacy audit; unavailable sources remain blocked. Rollout,
paid execution, hardware authorization and TB4 runtime acceptance remain
independent work.

### Owning implementation

| Surface | Lifecycle behavior |
| --- | --- |
| Local/native publish, direct adapter import, register, config sync | Common publication comparison under profile/task locks; new pending, identical preserved, changed legacy content invalidated |
| Canonical/legacy/native activation | `loom.benchmark_admission.admit_benchmark_profile` |
| Alias promotion | Same owner's expected-current operation and reviewed default-alias policy |
| Destination provisioning | Pending plus destination-local journal publication; explicit shared admission follows |
| API count/dry-run/submit and explicit task/rerun selectors | Shared profile/policy and selected-task requirement checks; frozen recovery keeps its prior contract |
| API/CLI/SPA readiness | Shared reason codes and distinct admission/target/support/qualification projections |

Catalog locks cover only the final comparison/write transaction, after uploads
or expensive adapter audits. Profile locks precede sorted task/source locks;
alias operations acquire sorted selector-name locks first. Failed admission or
alias CAS rolls back the composed operation, including when an operator catches
the validation exception inside a surrounding transaction.

Terminal-Bench-shaped `task.toml` bundles and `environment/` subtrees are
normalized by the current adapter/materializer path. Tests under
`packages/loom-benchmarks/tests/`, `tests/unit/`, and `tests/system/` exercise
manifest validation, conversion, registration, audit, activation, and sample
execution. Task-bundle fixtures live under `tests/fixtures/tasks/`.

For third-party adapter authoring, see
[Benchmark adapter](benchmark-adapter.md). For operator command examples, see
the [operator runbook](../runbooks/operator-runbook.md).

## Native Harbor packages

Pinned native packages use `loom datasets prepare-harbor SPEC.json --output DIR`
for local preservation/diagnostics and `loom datasets publish --harbor-source
SPEC.json` for journaled, versioned catalog publication. Source descriptors,
typed requirements, immutable origin binding and current runtime blockers are
specified in [Shared Harbor-native intake](harbor-native-intake.md).
