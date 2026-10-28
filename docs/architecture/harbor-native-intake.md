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

After publication, an operator can admit the exact native profile with
`loom datasets activate-native --harbor-source DESCRIPTOR.json --db-url DATABASE_URL`
(or the existing `LOOM_DB_URL` / `LOOM_SVC_DB_URL` environment setting). This checks
the pinned origin, complete source count, exact subset identities when present,
all canonical task configurations and shared hosted-runtime compatibility, and
the existing journal's available immutable sources in one transaction. A failed
check leaves the profile pending. The operation reuses registered source versions;
it does not reupload bundles or add a second object integrity chain.

Admission changes only `execution_state` to `runnable`, allowing ordinary member
selectors and execution preflight. It preserves unqualified labels and runtime
verification metadata: successful Oracle/model runs and result/export readback
remain separate acceptance. Identical native reimports retain prior admission;
changed task content returns to pending. Historical native profiles cannot be
reactivated or republished. TB2.1 continues to require its existing audited
`datasets activate` flow.

Native preparation derives the per-task `oracle_eligible` marker from a regular,
nonempty `solution/solve.sh`. Missing, empty, directory and symlink entries stay
ineligible. Oracle submission checks this existing capability marker; runtime
qualification continues to require completed execution evidence.

## Field mapping and execution admission

| Harbor declaration | Persistent Loom representation |
| --- | --- |
| Environment image/build/resources | `environment` |
| `architecture`, `memory`, `storage`, `env` | `cpu_arch`, MiB resource fields, `environment.environment` |
| `gpus`, exact `gpu_types` | GPU count and model declarations in each environment |
| Native Compose files | Original files plus `environment.compose_files`; single workbenches carry `single_service_compose`, supported service topologies carry `native_compose` |
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
not grant runtime support. Hosted execution prepares independent verifier images,
keeps their resource and environment declarations distinct, and restores declared
artifacts before grading. Independent verifiers use their own image USER,
WORKDIR and ENTRYPOINT defaults unless explicitly overridden, including when
the agent uses Compose. An empty OCI working directory resolves to `/`.
An isolated verifier may also use
private paths such as `/tests`; the agent's writable-workspace restrictions do
not apply to the grader. Both roles still require a bounded canonical absolute
image working directory without NUL bytes or ambiguous path components, and
agent images remain unable to select private verifier/runtime roots.
When `[verifier.environment]` is declared, an omitted internet setting maps to
hosted `public-web` independently of the agent; explicit
offline and allowlist policies remain unchanged. Submission validates the
independent verifier against its selected execution class before execution.
An independent tests image does not inherit the agent workbench's readiness
probe: that image does not start the agent's services. An explicitly declared
verifier healthcheck remains authoritative. Original upstream configuration is
retained in the bundle alongside this executable projection.
The legacy worker still rejects these hosted-only contracts. Unsupported Compose fields and exact GPU-model placement remain
explicit blockers.

Supported native Compose topologies run inside a private software guest. The
agent can operate only the main service; the trusted controller manages service
health/dependency order and collection from named services. Host mounts, Docker
sockets, host networking and ambient environment interpolation are rejected.
The `setup-native-compose` phase imports prepared images and starts the service
topology before the agent/model clock begins. Harness setup and agent execution
reconnect through a controller-private receipt. The outer guest's root Docker
control channel is never exposed to the agent; the inner main container retains
its authored identity. Main collection runs before main shutdown, followed by
sidecar hooks and collection, then retirement of the owned containers/network.
The guest allocation preserves the main task requirements and adds each
service's declared CPU/memory. Undeclared sidecars receive 1 CPU, 1 GiB memory
and 2 GiB storage each; the guest Docker daemon adds 1 CPU, 1 GiB memory and
4 GiB storage. These are platform allocations, not rewritten upstream fields.
Image archives have a separate bounded input storage allocation plus 3 GiB for
one OCI-to-Docker transport conversion. Placement accounts for all resident
allocations and rejects a node that cannot fit them.

The trusted native image publisher stores Compose OCI archives as exact versions
in the canonical artifacts bucket. It journals component keys and write IDs in
the native attempt before upload, then records each returned version and native
storage checksum in the prepared image grant. The authenticated input broker
streams only that execution's admitted service images. Registry and storage
credentials never enter the guest or its services. `task_image_builder.archive_secret_name`
selects the trusted publisher's existing canonical storage credential reference.
The standalone platform renderer defaults it to `loom-task-build-source`, whose
source binding already uses the canonical artifacts bucket; explicit alternate
references remain supported. Deployment preflight verifies the required keys.

Archive publication and interrupted-write recovery treat S3 user-metadata
header names case-insensitively, including Nebius Title-Case HEAD responses.
Ownership values remain exact: conflicting aliases are rejected, as are changed
write IDs, image references, versions, checksum shape or invalid object sizes.
Equivalent key casing does not change the retained object or its ownership.

The existing control-plane materializer loop reconciles interrupted archive
writes after the actuator observes build Pod removal. It retains admitted
archives until materialization retirement, persists an exact deletion inventory
before deleting, verifies absence, and periodically revisits retained write
intents for late completions. Foreign versions under an issued key are not
owned by that attempt. The actuator can append lifecycle registrations; only the
control plane owns archive retirement.

Native Compose requires the explicitly qualified `supports_native_compose`
runtime profile, guest support and task identity isolation. Publication defaults
the capability off. A Compose agent uses an independent verifier after service
collection; shared grading and verifier-side Compose are not qualified.

Collection hooks run against their named services and preserve authored timeouts.
The model deadline remains unchanged; a bounded finalization interval permits
collection after that deadline.
The admitted agent phase's `finalization_timeout_seconds` is the sum of authored
collection-hook timeouts plus 120 seconds for capture, bounded to one hour. The
scheduler includes it in the overall lease. The broker retains the original
agent cutoff; external cancellation receives only ordinary termination grace.
Compose command cancellation retires the inner process group before collection
hooks run. A single read-only BusyBox helper provides session control without
exposing the guest Docker socket; the authored service user and umask are preserved.
The inner helper writes its PID privately and atomically renames the completed
file before the controller validates it and separately releases the execution
gate. Invalid PIDs remain fatal; cancellation and normal teardown remove any
unpublished PID file as well as the marker and gate.
Control failures retain the operation, return code and execution duration; if
cleanup also fails, both results remain visible without command, environment or
output contents. A return code of 124 alone does not distinguish an execution
deadline from the runtime's bounded output-pipe wait.
For 124 results, the runtime console records only the fixed reason (deadline,
cancellation, output-pipe wait or process exit), effective timeout and duration;
the execution response fields and timeout behavior remain unchanged.
Native independent-verifier artifact transfer preserves source
paths and exclusions, replaces each declared directory with its collected tree,
and rejects unsafe links and private paths. Excluded or agent-deleted files do
not survive from the verifier image; file artifacts retain unrelated siblings.
Generic separate verification keeps
its full-workspace handoff and overlays structured outputs to preserve staged
private verifier inputs; a native independent verifier consumes its authored
artifact declarations. Oracle solution variables apply only to the solution
process. Health probes honor the declared startup interval.

Recognized native Harbor tasks keep their image's authored working directory,
user, and business filesystem permissions. The controller does not copy the
public source bundle into that working directory. Oracle alone receives the
complete authored `solution/` tree at `/solution`, including helper modules,
hidden files, and binary assets; model agents receive neither that tree nor
private tests. Oracle cleanup empties the prepared directory without unlinking
its root, so a nonroot image user can clean it while `/` remains root-owned.
Custom and offline workspace staging retain their existing behavior.

Native Harbor preparation revision `harbor-reference-directory-v1` creates and
assigns only the fixed harness directories (`/solution`, `/tests`,
`/logs/verifier`, `/loom/verifier`) to the authored image user. It does
not recursively change business files or change `USER` or `WORKDIR`. Variable
`USER` arguments use Docker's own ARG/ENV expansion via an unused build-only
argument name, avoiding authored names and frozen build arguments. The shell
reads its value as one quoted operand without `eval`; no runtime environment
variable is added. This recipe
participates in the existing materialization cache identity: new native requests
build new rows instead of reusing legacy ready images without these directories.
Existing rows, grants, and frozen build claims retain their old recipe and exact
source binding. Ordinary custom and offline requests retain the empty revision.

Schema migration 0181 adds that revision to the existing unique identity and
key check, and freezes it with the source/config identity. Existing rows default
to the empty revision without relabeling. Deployment uses the normal schema
backup and guard workflow. Downgrade refuses while recipe-qualified rows exist;
it never deletes their results to permit rollback. Older binaries are not
qualified to consume new-recipe rows. Active frozen trials are not rebound or
restarted by this change.

The preparation field is loaded explicitly only by current native producers.
Legacy pool readers and writers continue to operate on the audited retained
schemas without selecting, inserting or implicitly returning that newer column.
Catalog retention compares its optional value through the whole row, including
aliased rows; missing historical fields mean the empty legacy revision. New
native builds still require migration0181 and never fall back to an old recipe.

The native reward bridge runs the original `tests/test.sh`, removes stale
reward/output files, and accepts finite numeric scalar or JSON rewards,
including zero and fractional scores. Like Harbor, `reward.json` takes precedence
over `reward.txt`; text-only rewards use the `reward` key. An invalid existing
JSON reward fails instead of falling back to text. Missing, malformed, boolean and
non-finite rewards fail explicitly. A shell timeout cannot reuse a prior
success. Previously generated native wrappers retain exact version recognition
and the private-input layout; the runtime-owned bridge uses the current reward
reader without rewriting task assets. This bridge does not qualify the separate
verifier's execution backend.

Native image build observations retain the actual failed `prepare`, `build`,
or `publish` phase's log in the existing `builder_log`, after Pod ownership and
pool identity qualification, with the existing 100-line/16-KiB read and redaction
limits; successful and in-progress builds retain their ordinary build-log view.

## Development evidence and remaining acceptance

[`terminal-bench-4.0.0.json`](../evidence/2282/terminal-bench-4.0.0.json)
records all 66 official tasks imported through the common preparation pipeline
from the existing commit-addressed archive. Original package errors and native
requirements remain visible. At that intake milestone all 66 were blocked by
runtime capability requirements, including independent verifier environments.
That historical evidence used no catalog mutation, image build, model or cloud allocation.

Tests exercise another benchmark identity through the same importer, legacy
TB2 normalization, installed CLI pinned-Git preparation and atomic failure,
configuration/source round trips, numeric reward failures, real PostgreSQL and
versioned TLS object-store publication/reimport, concurrent origin binding, and
admission before runtime factories. Existing run/delivery snapshots retain
source linkage; no live run or delivery export has been qualified for TB4.

Runtime qualification is tracked in [#2292](https://github.com/ca-mrc/loom/issues/2292).
The official 66-task catalog has passed publication and exact-version storage
readback; that does not establish execution acceptance. Per the owner's
2026-10-27 scope decision, the current runtime acceptance covers 63 tasks.
`fp8-rmsnorm-gemm`, `jax-speedrun-gpu` and `math-eval-grader` are excluded because
they require H100 hardware. Support is deferred in
[#2649](https://github.com/ca-mrc/loom/issues/2649); no GPU capacity is activated.

The explicit
[non-H100 subset descriptor](../../config/harbor-sources/terminal-bench-4.0.0-non-h100-subset.json)
retains the same upstream revision and complete source count of 66, with exactly
63 persisted source task IDs. The official descriptor remains unchanged.
This subset stays marked unqualified until image preparation, actual Oracle
runs, bounded Terminus-2 model runs and durable result/export readback pass.
Local tests and import census alone are not deployed benchmark acceptance.

### Native Terminus task skills and MCP metadata

Hosted Terminus forwards `environment.skills_dir` and `environment.mcp_servers`
to the pinned Harbor agent constructor. Harbor 0.24's
[Terminus2.run](https://github.com/harbor-framework/harbor/blob/2de4605b9404eada7d341388291e6e469c90330b/src/harbor/agents/terminus_2/terminus_2.py#L1774)
adds declared MCP endpoints/commands and an `available_skills` XML section to
the initial instruction. It does not create a host MCP client or proxy. The
agent accesses declared endpoints from its existing task sandbox/network.

Native skill discovery reads `skills_dir/*/SKILL.md` through the sandbox Driver,
using Harbor's inherited `BaseEnvironment.is_dir` and `exec` methods. The pinned
agent omits a missing directory and skips unreadable or malformed skill entries;
it includes valid YAML-frontmatter names, descriptions and sandbox paths. Loom
does not read these paths from the controller filesystem or copy task skills.

The supported MCP declaration transports are `stdio`, `sse`, and `http` (mapped
to Harbor's `streamable-http`). Stdio command vectors become the native command
and arguments. WebSocket, nonempty MCP environment/header maps and credentials
embedded in endpoint userinfo are rejected explicitly: the pinned Terminus
prompt cannot convey those fields. Older installed Harbor agents lacking the
declared metadata capabilities also reject the request instead of dropping it.
Harbor's Oracle/nop utility-agent semantics ignore task-provided skills/MCP;
explicit unsupported agent-level configuration remains independently rejected.

The offline native-runtime regression checks actual Harbor construction and its
initial model request with a mocked HTTP model transport, including absent and
unreadable skills. This verifies forwarding, not endpoint availability or
successful completion of the benchmark tasks.

When a task selects a prebuilt image with no explicit build or Compose
definition, findings in its unused default Dockerfile remain source warnings.
They cannot establish a failure of an image that Loom does not build. Explicit
build recipes and verifier Dockerfiles retain their blocking checks; immutable
image and architecture admission still qualify the actual execution image.
