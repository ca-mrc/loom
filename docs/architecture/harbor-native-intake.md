# Shared Harbor-native intake

Harbor-native packages use one importer, independent of benchmark identity.
Git/Hugging Face row adapters still construct tasks for datasets that are not
already executable Harbor packages. The existing Terminal-Bench normalizer
retains its legacy entry point and delegates native field mapping to
`loom.harbor_task_import`; additional native releases do not fork that logic.

## Source descriptor and operator surface

A JSON descriptor binds profile ID, display/license metadata, expected complete
source count, source directory and an immutable `origin`. Git and HF provenance
require resolved 40-character commits. Native intake fetches Git or pinned
Harbor Hub packages; HF row datasets continue using their existing adapters.
Hub intake also records the fetched dataset metadata version and per-task
package digest supplied by the existing package client.

The pinned Hub client exports task directories directly under its output root.
Loom materialization moves those unchanged directories under `tasks/`, matching
the adapter source layout. Dataset-level files remain at the root beside the
resolved package metadata; they are not treated as executable task bundles.
Refresh any cache created with the older flat layout before preparing a benchmark.

The official TB4 descriptor is
[`config/harbor-sources/terminal-bench-4.0.0.json`](../../config/harbor-sources/terminal-bench-4.0.0.json).
It identifies commit `452bf305c6daa62fc59061d22133a7cbc7c1572e`, release
`4.0.0`, Apache-2.0 license and all 66 source tasks. It is an import descriptor;
it does not activate an executable benchmark.

```bash
uv sync --locked --extra rollout
uv run loom datasets prepare-harbor \
  config/harbor-sources/terminal-bench-4.0.0.json \
  --output /writable/new-version-directory \
  --cache-dir /writable/benchmark-cache

# An authorized catalog operator supplies the ordinary DB/storage secret
# references and a versioning-enabled destination bucket.
uv run loom datasets publish \
  --harbor-source config/harbor-sources/terminal-bench-4.0.0.json \
  --bucket VERSIONED_CATALOG_BUCKET
```

`prepare-harbor` downloads and converts into a new directory atomically. It
uses no database, image build, model or cloud execution resources. Its manifest
retains per-task provenance, blockers and profile counts. Existing output
folders are rejected. Unknown execution fields fail with source field paths,
without publishing a partial directory.

This native format requires `[task].name` and original `instruction.md` /
`tests/test.sh`. Known upstream format stamps are `1`, `1.0`, `1.1`, `1.2`, `1.3` and `2.0`; unknown stamps
fail explicitly. Legacy metadata-only and YAML TB2 formats retain their
existing normalizer/adapter paths.

The descriptor owns selection. An `origin.subset` must contain explicit unique
source task IDs and use a profile ID containing `subset`. The expected source
count remains the complete upstream count. Optional `--instance-id` selections
must match that persisted subset; `--limit` is rejected for native intake.
A subset never changes the identity or claimed completeness of the official
profile. Native intake rejects legacy flattening and execution-profile repair
flags; reviewed repairs need a distinct derived producer/profile.

## Persistent contract

Each bundle keeps all original files and a byte-identical `upstream-task.toml`.
The canonical `task.toml` carries typed source origin, source task ID, importer
conversion description and resolved package digest when applicable. These
fields travel with the existing canonical config, immutable source inventory,
catalog provenance and frozen execution snapshots. No new hash or signature
chain is introduced.

Native publication uses the existing journaled task-bundle source publisher.
Storage writes are issued durably before PUT; the final transaction publishes
benchmark, task, source references and image requirements together. Repeating
an identical publication reuses the registered source. A destination without
Enabled object-store versioning fails before catalog publication. Existing
local-folder producers retain their existing opt-in behavior.

The profile origin is bound by PostgreSQL's `ON CONFLICT ... WHERE` predicate,
including simultaneous first imports. Another revision, release or subset
cannot replace that identity. A local-folder publication cannot erase an
existing native origin either. Source-upload failures/conflicts retain the
existing journal's recovery/retirement records; they do not activate a partial
catalog.

## Field mapping and execution admission

| Harbor declaration | Persistent Loom representation |
| --- | --- |
| Environment image/build/resources | `environment` |
| `architecture`, `memory`, `storage`, `env` | `cpu_arch`, MiB resource fields, `environment.environment` |
| `gpus`, exact `gpu_types` | GPU count and model declarations in each environment |
| Native Compose files | Original files plus `environment.compose_files` |
| `verifier.environment` and `tests/Dockerfile` | Independent typed verifier environment/build context |
| `verifier.env`, `collect`, execution mode | `environment_vars`, typed collection hooks, `env_mode` |
| Relative artifact strings | Existing step artifact globs |
| Absolute/structured artifacts | Typed `artifact_sources`, including service/destination/exclusions |
| `solution.env` | `solution_environment` |
| Existing package compatibility errors | Typed `import_blockers` with source path, line and diagnosis |

Absolute artifact paths identify paths inside the sandbox, never host paths.
Managed destinations are relative and reject traversal and reserved manifest
names. Package compatibility errors may be retained for official source
imports, but remain persistent execution blockers. Ordinary adapter/folder
imports continue rejecting those errors. Native publication rechecks that all
such errors were recorded; it does not repair the Dockerfile or erase the error.

A shared requirement checker drives catalog/API/CLI diagnostics, automatic
service materialization, worker admission before factories/model launch, and
the Trial's driver-start boundary. Schema validity and preserved raw files do
not grant runtime support. Current blockers include independent verifier
images/resources, native Compose, exact GPU-model placement, verifier env and
collection hooks, structured/absolute artifacts, oracle-specific environment
variables, and declared healthcheck startup intervals.

The native reward bridge runs the original `tests/test.sh`, removes stale
reward/output files, and accepts finite numeric scalar or JSON rewards,
including zero and fractional scores. Missing, malformed, boolean and
non-finite rewards fail explicitly. A shell timeout cannot reuse a prior
success. This bridge does not qualify the separate verifier's execution backend.

## Development evidence and remaining acceptance

[`terminal-bench-4.0.0.json`](../evidence/2282/terminal-bench-4.0.0.json)
records all 66 official tasks imported through the common preparation pipeline
from the existing commit-addressed archive. Original package errors and native
requirements remain visible. All 66 are blocked for execution by current
runtime capability requirements, including independent verifier environments.
This evidence used no catalog mutation, image build, model or cloud allocation.

Tests exercise another benchmark identity through the same importer, legacy
TB2 normalization, installed CLI pinned-Git preparation and atomic failure,
configuration/source round trips, numeric reward failures, real PostgreSQL and
versioned TLS object-store publication/reimport, concurrent origin binding, and
admission before runtime factories. Existing run/delivery snapshots retain
source linkage; no live run or delivery export has been qualified for TB4.

Issue #2282 remains open for reusable runtime materialization/collection,
H100 admission/placement and actual capacity observation, representative native
oracle equivalence, run/export readback and separately authorized bounded live
acceptance. The import census is not deployed or executable benchmark acceptance.
