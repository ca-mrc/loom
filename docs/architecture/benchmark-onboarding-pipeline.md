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
loom datasets activate
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
- `activate` selects a fully audited immutable profile in one transaction.
- `verify` samples registered tasks and runs the oracle path end to end.
- `provision-catalog` copies selected runnable benchmark, task, agent, and
  bundle state between controlled environments.

Credentials are read from environment or credential-source files. Do not put
database, object-store, provider, or Hugging Face tokens in command arguments
or catalog files.

## Readiness

Catalog presence does not imply runnable support. A profile becomes active
only when its manifest and source are immutable, every selected task has a
valid canonical `TaskConfig`, referenced bundles match their checksums, the
adapter and verifier are available, and the target architecture is supported.

The SPA and service expose these readiness states. Submission routes enforce
the same checks, so a client cannot bypass a blocked catalog entry by calling
the API directly.

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
