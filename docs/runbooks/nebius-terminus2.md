# Terminus-2 on Nebius: first Terminal-Bench task

The initial task is `file-archive-manifest` from the old staging catalog
`terminal-bench-2-harbor-90`, using `terminus-2` and Gateway model `glm-5.2`.
This prepares one task, not a claim that all 90 tasks support Nebius.

## Prepare Dockerfile upload files

The existing GitHub-hosted `nebius-candidate` publication builds the trusted
`images.harbor_runtime` controller with the platform components. It does not
build or admit this task image. The ordinary task-image queue prepares the
submitted Dockerfile and binds the ready image to the Trial's frozen task
revision. The compiler uses that image for task and isolated verifier, and the
published Harbor runtime for the controller. Direct-completion continues using
the existing service image. No separate Nebius runner or CI gate is required.

For a local environment check, build from the repository root:

```sh
docker build --platform linux/amd64 \
  -f deploy/catalog/nebius-terminal-bench/file-archive-manifest/Dockerfile \
  -t <registry>/loom-terminal-bench-file-archive-manifest:<version> \
  deploy/catalog/nebius-terminal-bench/file-archive-manifest
```

Prepare the ordinary upload directory without a prebuilt image:

```sh
PYTHONPATH=src:. python scripts/ops/prepare_nebius_terminal_bench.py \
  --output /tmp/nebius-terminal-bench-taskset
loom tasksets submit /tmp/nebius-terminal-bench-taskset --format json
loom tasksets status '<returned TaskSet ID>' --format json
```

The helper writes `manifest.yaml`, `bundle.tar.gz` and `taskset-build.json`.
It does not build, publish, submit, create resources or call a model. Loom's
existing TaskSet upload contract uses TAR archives; ZIP is not accepted.
The output directory must be absent or empty to preserve existing operator work.
The default manifest uses a distinct `-dockerfile` name. Its task environment
declares `dockerfile="environment/Dockerfile"`,
`docker_build_context="environment"` and no `docker_image`. The context contains
only the reviewed prepared Dockerfile. Unchanged `instruction.md` and
`tests/test_outputs.py`, the offline `verifier/run.sh`, and source provenance
remain outside that context in the uploaded task directory. No oracle is uploaded.

The optional `--image '<registry>/image@sha256:<digest>'` retains preparation
for a prebuilt image already admitted by the target runtime profile. It does
not grant admission to arbitrary images. Historical prebuilt TaskSets remain
historical inputs; submitting them against a new profile without their image
admission is unsupported. Preserve old candidates and image references needed
by existing frozen Batches.

Import the directory through the ordinary TaskSet path after the target runtime
supports Terminus-2. In **Task Sets → Submit Task Set**, select `manifest.yaml`
and `bundle.tar.gz`; the archive already includes its verifier, so leave the
separate verifier and transform fields empty. Select `terminus-2` + `glm-5.2` when submitting a Trial;
model/provider configuration and credentials stay in the Gateway. Catalog/model
availability must be verified on the target environment. The example source task
keeps its 2-vCPU/4-GiB/8-GiB task and verifier hard limits and original phase
timeouts. Published Nebius profiles keep separate controller limits through
`controller_resources` (CPU and memory; storage remains task-derived). These are
execution limits, not the ordinary scheduling reservation described below.
Workspace, runtime and output emptyDir bounds remain unchanged. Existing frozen
Batches, reservations and historical cost records are not rewritten.

The runtime image supplies the Go plan reader; the selected Harbor image receives
task/trial inputs and phase arguments. Explicit harness versions retain their
selected controller image. Deploy matching Python services, actuator and Go
runtime before submitting plans with resource requests. Generic profiles without
a request template and existing frozen plans retain their previous behavior.

## Persistent Nebius scheduling baseline

Ordinary automatic native Terminus-2 submissions use a persistent baseline of
a **target node-capacity share**, frozen on the execution lease before admission.
Task-package CPU, memory and storage declarations are minimum requirements.
Existing leases keep their original allocation; changing a pool does not resize
running containers. New publisher profiles advertise `resource_allocation_policy:
node-share-v1` only with the matching execution runtime. Historical profiles and
explicit precompiled plans keep their prior contract; rerun against the current
runtime to adopt this policy. The normal candidate build publishes this capability.
Manual profile creation requires `--node-share-resources` and a compatible runtime.
Do not add the capability to an old runtime image. Rollback must retain a compatible
runtime for existing allocated leases until they finish.

The allocator uses a fresh placement observation for the target pool (or its
existing compatible allocatable sample when scaled to zero). Node allocatable
already excludes kube/system reservations. Subtract resident DaemonSet requests
once, and respect the configured per-node policy ceilings. It does not use
transiently free memory or subtract system reservations a second time.

Baseline slots are `max(1, floor(usable CPU millicores / 1000))`. Divide usable
memory and ephemeral storage by that slot count. Within each complete Pod share,
reserve the controller and ordinary sidecars first, then divide the remainder
between the concurrently resident task and verifier sandboxes. For each sandbox,
take the component-wise maximum of that share, its task declaration, and its
explicit request. Both sandboxes remain resident even while one is idle.

For example, 16 usable CPUs and 240 GiB usable memory give a 15 GiB Pod share.
After a 1 GiB controller, each of two sandboxes receives a 7 GiB default. A 4 GiB
task therefore receives 7 GiB; a 12 GiB task receives 12 GiB per sandbox and
reserves 25 GiB for the complete Pod. Every execution container's memory request
equals its limit. Controller CPU requests remain distinct from CPU limits and
throttling; storage remains subject to Kubernetes ephemeral-storage semantics.

Admission, cost reservations, placement and rendered container resources consume
this same frozen plan. Larger task requirements reduce concurrency; insufficient
capacity queues/scales through the existing capacity controller, while an
allocation larger than a supported node returns
`execution_capacity_workload_exceeds_node_allocatable`. No reservation is shrunk
to force placement. CPU-derived slots are a baseline, not guaranteed concurrency.

New node-share profiles omit the retired `default_task_resource_requests`
template. Submission also ignores a legacy default template if one was retained
in such a profile, so it cannot reject a small task declaration before target
allocation. Historical profiles retain that template for compatibility.

Revision-bound per-task request overrides remain optional compatibility inputs:
explicit submission overrides environment revision entries, and source checksum
and limit validation remain strict. The node allocator then raises effective
memory reservations and limits together. Original task checksums, source
declarations, output bounds and build resources are unchanged.

The Trial detail API exposes `materialization.resource_allocation`, including
source minima, effective requests and limits for every role, and complete Pod
requests. The same role allocations are shown on the Trial detail page. The
Batch's submitted request map is input policy; consult the Trial's frozen
allocation for actual scheduling and execution values.

### Per-task measured overrides

After collecting representative usage, use an override to compare scheduling
inputs for selected tasks without rebuilding images. The node-share allocator
then resolves effective requests and limits before admission.
Pass `--task-resource-requests @requests.json` to `loom eval batch create`, or the
same `task_resource_requests` object to `POST /api/v1/batches`:

```json
{
  "local/example-task": {
    "task_revision_sha256": "sha256:<existing task checksum>",
    "requests": {
      "controller": {
        "cpu_millis": 200,
        "memory_mib": 512,
        "ephemeral_storage_mib": 512
      },
      "task_sandbox": {
        "cpu_millis": 600,
        "memory_mib": 1024,
        "ephemeral_storage_mib": 1024
      },
      "verifier_sandbox": {
        "cpu_millis": 200,
        "memory_mib": 512,
        "ephemeral_storage_mib": 512
      }
    }
  }
}
```

To persist a deliberate task-specific override for ordinary users, place this map
under `task_resource_requests` in the environment configuration. Remove obsolete
cohort overrides when adopting the node-share policy; higher overrides remain
minimum inputs to the final allocation. Overrides for unselected
tasks, other backends/agents or explicit precompiled bindings are rejected.

The Batch detail API exposes its resolved `task_resource_requests` map; the page
shows submitted per-task totals. Trial details show the final allocation. Kubernetes placement, admission reservations and
request-based cost allocation use the same effective Pod requests. Reservations
are not maximum consumption, and request-based costs are not the provider bill.
Compare packing and node-hours while retaining OOM, eviction, latency and
resource-completeness evidence. Sampled task storage excludes read-only image
layers; retain node filesystem/image-cache headroom. See the
[resource-accounting runbook](trial-resource-accounting.md#capacity-calibration).

## Dockerfile task prerequisites

Ordinary Nebius Batches can also queue a compatible Terminus-2 TaskSet whose
primary environment specifies a Dockerfile. Import readiness means the task
files and input manifest have been published; it does not mean the image has
been built. The Trial waits for its linked x86_64 task-image materialization
before reserving execution, consuming an attempt, or calling a model. An
architecture-independent task may also have an arm64 prerequisite; that row
does not block the x86_64 Nebius execution path. A failed x86_64 prerequisite
finishes the waiting Trial with `task_image_build_failed` and no consumed attempt.

`GET /api/v1/trials/{id}` exposes `task_environment_preparation` to the Trial's
authorized readers, including before execution starts. Each entry reports the
linked architecture's current shared preparation state, build-attempt count,
known failure reason, safe explanation, prepare/build/publish phase timestamps
and exit code, and whether native build resources have been released. It uses
the current materialization epoch; this is not historical evidence of which
build a completed Trial used. A later cache rebuild can change this view.
Arbitrary Dockerfile output, source locations, registry references and frozen
build configuration are not exposed. Operators retain the bounded build log
for deeper diagnosis. Cancelling the last waiting Trial can leave preparation
queued with `build_cancelled` and no active demand; it does not mean another
build is running. A no-demand cancellation refunds that build's retry charge;
real build failures, deadlines and expired leases retain their bounded budget.
Attempt history and lease epochs remain unchanged, and resources remain reserved
until the old Job's deletion is acknowledged. New demand resumes the queued
image using a new epoch. When new demand references a legacy cancelled image,
the platform refunds recorded, previously charged cancellations from its current
retry budget; it preserves real failures and does not reset other failed images.
Already failed Trials retain their results and need a new submission after repair.
A pre-execution failure or cancellation has no execution
bundle, so `/bundle/download` continues returning HTTP 409 rather than creating
a synthetic successful trajectory.

When the platform's native task-image builder is enabled, its existing actuator
prepares and publishes the queued Dockerfile image. Import alone does not
activate a builder or permit arbitrary prebuilt images. The trusted controller and
runtime still come from the published platform profile; only the task and its
private verifier sandbox use the image associated with the frozen task revision.
Direct-completion cannot use this Dockerfile path. The other existing CPU,
workspace, networking, verifier and single-step restrictions still apply.

An original `agent.continue_until_timeout = true` declaration is preserved by
task conversion and supported when the resolved trial agent is `terminus-2`.
It keeps the same agent session active after completion requests until the
configured attempt deadline; reaching that deadline is reported as a timeout,
with the existing partial trajectory and verifier handling. Errors and
cancellation still stop the attempt. Do not drop the declaration to make an
unsupported agent pass admission, or submit another model attempt to replace
an existing original result. Qualify the deployed runtime before acceptance;
the static compatibility report does not prove installation.

Resubmitting an existing Trial preserves its original task-image links even
when the TaskSet has since been rebuilt. Submit a new Trial to use the new
revision. Cancelling a waiting Trial does not cancel a shared image build needed
by other Trials. Generated inputs remain retained while a current task revision,
a live Trial, an unexpired build lease, or a ready image cache references them.
A retired cache with no remaining consumer does not retain historical inputs.
Preparing the same content again after cache retirement uses the current upload
location; ready cache reuse preserves its existing frozen source.

Native preparation reads ordinary TaskSet file modes from the frozen
`service_execution_input` manifest already published by TaskSet materialization
(and, as of #1978, by benchmark `publish-local` using the same binding shape).
It verifies that binding and the transferred bundle revision; ordinary uploads
do not need the benchmark publisher's `.loom-bundle-files.v1.json` sidecar when
`source_provenance.service_execution_input` is present. Benchmark sources
without an input-manifest binding retain the sidecar path.
A task-root `.loom-bundle-files.v1.json` is reserved transport metadata, so
benchmark publication and TaskSet bundle upload remove it from their private
staging trees before computing the task checksum and service input inventory.
The source directory or uploaded archive is preserved. Authored dotfiles and
nested files with the same name remain inputs.
A missing or corrupt bound manifest fails preparation; it does not fall back to
unbound modes or a different source revision.

Benchmark and TaskSet tasks use the same immutable input binding for Nebius
Terminus admission. For a catalog suite, use `loom datasets publish-local` on
the adapted benchmark folder; converting it into a TaskSet is unnecessary.
Publication stores task files under
`.loom-revisions-v2/<checksum>/<mode-digest>/bundle/` and the input manifest
beside `bundle/`. The manifest is publication metadata, not another task file.
This separation lets the ordinary bundle audit and native image builder consume
the exact same revision.

If a benchmark was published by the earlier implementation that stored
`service-execution-input.json` inside the task prefix, republish the same local
folder with the fixed CLI. It updates the catalog source and binding without
changing task content or deleting old objects. Existing frozen Trials retain
their original inputs; submit a new Trial after republishing. Verify the current
catalog source with `loom datasets audit <benchmark-id> --verify-bundles` using
the target database and object-store configuration.

`publish-local` validates the general bundle schema and normalizes Harbor TOML.
By default it does **not** rewrite a task's architecture, network access,
verifier or resource contract for Nebius. Unadapted Harbor rows still fail
admission (`nebius_task_incompatible` / the specific contract reasons in
[#1996](https://github.com/qianyi-sun/loom/issues/1996)). Service-execution
input (#1978) is necessary but not sufficient.

For Harbor/TB packs that should become Nebius Terminus-admissible, pass the
opt-in ingest profile (keeps admission contracts; adapts the staged publish
tree + DB config only):

```sh
loom datasets validate-local /path/to/benchmark \
  --execution-profile nebius-terminus
loom datasets publish-local /path/to/benchmark \
  --execution-profile nebius-terminus \
  --minio-region eu-north1
```

Before applying the profile to unfamiliar tasks, inspect all inputs through
the ordinary validation command:

```sh
loom datasets validate-local /path/to/benchmark \
  --execution-profile nebius-terminus --compatibility-report --json \
  > compatibility-report.json
```

The complete report is emitted even when a task is blocked (exit code 1).
Each task retains its source location and declared requirements. Package
defects such as missing `COPY` sources, unsupported bootstrap conversions,
and missing runtime capabilities have separate dispositions and suggested
actions. Explicit users, network policies, workdirs, services and verifier
entrypoints must not disappear behind successful profile-admission counters.
The report lists generated image inputs and defaulted values separately from
declared requirement changes. It performs no builds or model calls, does not
establish registry availability, and makes no changes to the source tree.

For custom trajectory-generation archives, publication belongs in a team-owned
TaskSet with `intents=["trajectory_generation"]`; `publish-local` is the catalog
benchmark path. Validation itself publishes nothing. Full acceptance of the
60 inputs in [#2046](https://github.com/qianyi-sun/loom/issues/2046) requires a
report against the original archives and real trajectory-generation delivery;
fixture checks alone do not establish that evidence.

That profile selects `cpu_arch=x86_64`, preserves an explicit `web-allowlist`
policy, and otherwise selects `gateway-only` networking. It fills missing
`cpus`/`memory_mb`/`storage_mb` (defaults 1 / 2048 / 4096), defaults missing
`user` to `agent`, preserves explicit task and verifier identities, and preserves
supported declared working directories. Only a missing workdir defaults to `/app`.
Task working directories must be absolute canonical paths containing ASCII
letters, digits, `-`, `_`, and `.` within components. Dot components, `/`, `/tmp`,
and paths overlapping runtime or private verifier roots are rejected before
preparation writes generated files. For example, `/media/project` and
`/opt/project` are supported; `/loom`, `/tests`, `/opt/verifier`, and their
ancestors or descendants are not.
The compatibility report flags declared network changes and authored Dockerfile
`WORKDIR` mismatches for review. It follows known local stage inheritance and
relative directories after a known absolute path; unresolved variables and
relative paths inherited from registry images require image inspection. The
trusted controller keeps its fixed `/app` cwd and isolated Python imports;
the declared task cwd belongs only to the private task and verifier sandboxes.
The profile points the verifier at relative
`verifier/run.sh`, drops Harbor TB2.1 artifact globs that admission rejects,
and prepares a derived Dockerfile for the selected numeric identity.
Explicit identity and web egress require qualified deployment opt-ins;
the default runtime remains non-root with gateway-only networking.
For Harbor-shaped input, a bare JSON `CMD` containing only `sh`, `bash`, or
`zsh` (including `/bin/` and `/usr/bin/` paths) is recorded as an equivalent
conversion. The pinned Harbor Docker runner overrides image `CMD`, and its
Terminus session starts Bash independently, so these defaults do not require
a service initializer or a different terminal shell. Shell arguments, scripts,
shell-form commands, authored `ENTRYPOINT`, and native Loom input retain startup
review diagnostics. Registry-image entrypoints are not inspected by this static
check and still require source/runtime review before original-task acceptance.
Packaged `environment/docker-compose.yaml` and the standard `.yml` or
`compose.yaml` / `compose.yml` variants block profile conversion before derived
files are written. The compatibility report identifies each source as
`compose_environment_unsupported`. This includes main-container overrides:
their environment, mounts and network settings must not disappear. Preserve
supplied service fixtures and qualify their isolated images, network aliases,
health checks, dependencies and verifier lifecycle; a packaged local service
does not establish a missing external endpoint.
After explicit package review, one self-contained service can use the
[prepared fixture declaration](../architecture/nebius-service-execution.md):
separate disjoint build contexts, a bounded non-root read-only service with no
mounts, private task/verifier hostname resolution and an exact frozen build grant.
This declaration does not translate arbitrary Compose overrides or dependencies.
The report also identifies literal `echo`/`printf`-generated workspace shell
scripts that invoke `pytest`, `python -m pytest`, or `uvx ... pytest` against a
file in the bundled private `/tests` tree as `agent_private_verifier_dependency`.
For example, a public `validate.sh` that invokes `/tests/test_state.py` requires
an explicit task-package review and repair; copying withheld verifier inputs
into the agent workspace is not a repair. The diagnostic follows local image
stage inheritance and reports the original Dockerfile line without changing
the package. It is a source check, not a shell interpreter: dynamically generated
scripts, heredoc bodies, copied scripts and registry-image contents still need
task-author review. Later overwrites or deletion are not evaluated, so a finding
does not prove that the final image still contains the dependency. Independent
public checks and private scoring must preserve
the original task contract; see [#1263](https://github.com/qianyi-sun/loom/issues/1263).
The original Dockerfile and `tests/test.sh` remain
unchanged in the source bundle. The derived image prepares writable workspace,
home and verifier directories, installs Terminus tools, and preinstalls the
Python version and verifier dependencies declared by the supported Harbor
bootstrap. Build-time dependency installation may run as root independently
of the selected task execution identity.

For the complete Terminal World OpenHands packaging convention, the Terminus
profile omits the foreign agent cache from the derived Dockerfile and records
that adaptation in a comment at the omitted stage, preserving parser directives.
Recognition requires the exact
`terminalworld-openhands-sdk-cache:1.34.0-py312-musl-v3` image with alias
`terminalworld_openhands_runtime_cache`, an otherwise empty cache stage, one
task stage, and a final instruction block of exactly the three copies of `/opt/openhands-python`,
`/opt/openhands-sdk-venv` and `/opt/openhands-musl-loader` to the same paths.
The task stage's other instructions and original source file remain intact.
Partial patterns, extra copies, changed destinations, numeric stage references,
other cache tags and Dockerfile commands depending on the removed runtime
require explicit adaptation. This rule does not supply task dependencies or
download the task's requested artifacts.

The generated `verifier/harbor-offline.sh` removes only recognized online
bootstrap and runs pytest from the preinstalled verifier environment. Plain pip
bootstraps retain the base Python interpreter and its task dependencies through
a verifier venv with system-site-packages; uv bootstraps retain their declared
Python version, exact dependency pins and package-index selection. Pytest must
remain exactly pinned; auxiliary dependencies may also use ordinary unversioned
package names such as `pandas`, `uproot` or `GitPython`. These names resolve only
during image preparation. The image records installed versions in
`/opt/verifier/resolved-requirements.txt`, and verification uses that prepared
environment offline. A future rebuild may resolve different auxiliary versions.
Version ranges, arbitrary URLs, local paths and shell expressions are rejected.
Recognized
installer forms include apt's `-qq` and pip's `--no-cache-dir` flags, recognized uv
installation followed by `source "$HOME/.local/bin/env"` or
`export PATH="$HOME/.local/bin:$PATH"`, and simple missing-command guards for
curl or uv. A curl guard may install only curl; a uv guard may install curl and
must contain the complete recognized uv installation and activation. Task commands,
nested branches, or other conditional packages inside that guard require
explicit adaptation. A removed guard leaves a shell no-op to preserve any
enclosing branch or function. The converter does not evaluate arbitrary shell
control flow. Verifier scripts containing `<<` outside comment lines require
explicit adaptation, so heredoc payloads cannot be mistaken for installers.
Preparation does not install `python-is-python3` implicitly or replace an
authored `python` alias. When a Dockerfile declares `SHELL`, only appended
preparation commands use explicit JSON-form `/bin/sh -c`; the authored shell
and its task commands remain unchanged. Recognized fixed-commit Git dependencies
and explicit verifier asset downloads are prepared
at image build time. Assets are copied into the verifier workspace at the
original script location; test assertions and reward branches are unchanged.
If the original script emits a valid reward and then exits nonzero, the wrapper
records that reward and original exit code before returning the same failure.
This preserves outcome metadata without turning setup or verifier failures into
successful execution. Missing or invalid rewards remain errors.

For an exact generated Harbor wrapper, the isolated verifier stages its tests
and scripts at the reserved private input root `/loom/verifier/task`,
exports that location as `LOOM_TASK_DIR`, and runs the script with the task's
original working directory. Its result is written outside that working directory
as well. This keeps private grading files out of task file inventories while
preserving the original `/tests` paths, assertions and reward logic. Recognition
requires the generated script path and complete wrapper bytes. Custom or modified
scripts retain their relative input layout; verifier arguments are unchanged.

Task-specific setup, pytest arguments, reward logic and the complete private
`tests/` tree are preserved. Unsupported bootstrap forms, custom verifier
adapters or image shapes fail with an adaptation error; configuration admission
alone is not proof that an arbitrary task image can execute. Validate a newly
adapted image through sandbox upload, agent setup and offline verification
before a model batch. This adapter supports the final image families below and the
Harbor version-pinned `curl -LsSf https://astral.sh/uv/X.Y.Z/install.sh | sh`
or exact canonical `curl -LsSf https://astral.sh/uv/install.sh | sh` installer,
and preinstalled `uvx -p ... -w package==version ... pytest` (including the
equivalent `--python` and `--with` options),
pip with exactly pinned pytest plus pytest/python-module invocations, and explicit uv
venv/activation/pip/run forms. Both recognized installer forms relocate into
image preparation using Loom's pinned uv provisioner; the canonical URL is not
fetched during verification. Original scripts remain available, Python and
dependency declarations are retained, and the derived Dockerfile records the
provisioner version. This is an explicit bootstrap conversion. Altered hosts,
query strings, fragments and appended installer commands remain unsupported.
Combined apt update/install commands are handled
only when their package list is explicit. Supported derived bases include
official Debian-based Python and numeric Node tags (full or slim, including
Bullseye, Bookworm and Trixie variants), and numeric `rootproject/root` versions
with an explicit Ubuntu 20.04, 22.04 or 24.04 suffix. Versioned official PHP CLI
tags are also accepted, with optional Stretch, Buster, Bullseye, Bookworm or
Trixie suffixes. Preparation retains the
original application interpreter and PATH; plain pip verifiers inherit system
packages so compiled PyROOT dependencies remain available. Validate the actual
image and original verifier before execution; accepting a tag does not qualify
its upstream contents. Alpine variants of Python, Node and PHP images, and Fedora bases, remain unsupported.

Official `archlinux:latest`, `archlinux:base` and `archlinux:base-devel` final
images are also supported, including local stage inheritance. Preparation adds
missing harness tools with pacman and builds the separate verifier environment
with the same supported uv/pip bootstrap conversion. It leaves already installed
packages at their authored versions and rejects a dependency transaction that
would upgrade or downgrade one. A rolling repository can therefore require an
explicitly reviewed compatible image or repository snapshot. Preparation uses a
temporary package cache and preserves the image's existing package cache. Debian
`apt` bootstrap dependencies are rejected on Arch instead of translating package
names. This image support does not grant mounts, devices or kernel privileges;
declare supported directory state separately and qualify its private-verifier
handoff before running an original task.

Official versioned `alpine:3.x` and `alpine:3.x.y` final images support the
same separate verifier environment, including explicitly requested musl Python
versions available through uv. Only the explicit `apk add --no-cache package ...`
bootstrap in an unconditional script preamble is relocated (comments and common
`set` options may precede it). APK inside task setup, branches or functions,
arbitrary APK commands, local APK files and repository overrides require review. APK and Debian bootstrap dependencies are
not translated between distributions. Preparation simulates the harness package
transaction and rejects upgrades, downgrades, removals or reinstalls of authored
packages; a post-install inventory also rejects repository drift. Existing APK
caches remain intact. This prepares harness/verifier dependencies only: it does
not install task-required tools such as wget unless the supplied verifier itself
explicitly requests them. Such overlap must be resolved before original-task
acceptance. Validate actual package state transfer and runtime egress separately.

Prebuilt images, malformed
Dockerfile `SHELL`, selected build targets, floating Git dependencies and
unrecognized shell/installer forms need explicit adaptation. An upstream asset
URL can still change on a future rebuild; a prepared image fixes the bytes used
for its own executions, without making an upstream reproducibility claim. The publisher then dry-runs
`automatic_service_execution_rejections` before upsert. Bucket creation stays
opt-in via `--create-bucket` ([#1993](https://github.com/qianyi-sun/loom/issues/1993) /
[#1994](https://github.com/qianyi-sun/loom/pull/1994)); prefer an infra-managed
bucket and pass `--minio-region` for signing.

Dockerfile compatibility and image preparation share instruction-boundary
parsing. Quoted, tab-stripped and multiple heredoc bodies remain opaque: a
Python `from datetime` or literal `SHELL` inside a body cannot change the
detected build stage. Continuations and an earlier stage alias are supported;
unterminated heredocs and unresolved final base images fail before preparation
writes derived outputs. Original stages and build-context files are retained.
This does not repair unavailable base images, missing `COPY` sources, or task
mocks, and it does not validate every Dockerfile instruction in place of
BuildKit. Such package defects require an explicit, reviewable source repair.
The build uses Loom's pinned uv provisioner and installs an explicitly declared
verifier Python under `/opt/verifier-python`, preserving the task interpreter
and PATH even when the task image uses an older Python. An unpinned `uvx pytest`
bootstrap uses uv's tool resolver, which can select a compatible managed Python
from the verifier dependencies. Its isolated tool environment lives under
`/opt/verifier-tools`, with `/opt/verifier` pointing to it. Incompatible dependency
pins still fail the image build; they are never silently omitted or relaxed.
Authored HOME caches are retained because they can contain required offline
dependencies. Only the verifier preparation commands disable uv caching; that
setting does not change the task's runtime environment. Declare any cache or
virtualenv state the agent changes outside its workdir as a mutable path when
the private verifier needs the resulting state.

Task users are container-local declarations. With a deployment profile that
explicitly enables `supports_task_identity`, Terminus accepts `environment.user`
as `root`, `0`, or a numeric `UID:GID`. Numeric nonroot identities also require
`environment.environment.HOME`; HOME must be an absolute canonical directory
outside the private runtime and verifier paths. The existing `agent` default
retains the profile's nonroot UID/GID. Arbitrary usernames and a nonroot UID
without its GID remain unsupported because admission cannot resolve the image's
passwd/group metadata. A declared `verifier.user` is preserved and applied to
the independent verifier; it must be able to restore the task's recorded file
ownership. Per-step and agent-process identity overrides remain unsupported.

The trusted controller keeps its nonroot identity. Explicitly root private task
and verifier containers retain no-new-privileges and drop all capabilities,
then receive only `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `SETUID`, `SETGID`, and `KILL`
for package installation and cleanup of descendants that drop UID. This grants
no host mounts, devices, kernel administration, nested-container service, or
privileged mode. The active runtime binary and namespace security policy must
support this configuration before the deployment-owned opt-in is enabled.

Declare every relevant installed directory in `environment.mutable_paths`,
including package-manager state when required; root access alone does not copy
changes into a fresh verifier. For example, an installation into
`/usr/local/share/my-tool` tracked by dpkg needs that directory and
`/var/lib/dpkg` declared. Directory transfer retains its size, protected-path
and filesystem-entry restrictions; it is not a whole-rootfs snapshot. The local
Docker regression `tests/integration/test_task_identity_installation_docker.py`
installs an initially absent `.deb`, executes its ownership/UID-changing
maintainer script, cleans up its child process and verifies package files and
dpkg state in a fresh private sandbox without model calls or network access.
Real tasks requiring additional package paths, sockets, services, devices or
external inputs still need their corresponding support and acceptance evidence.

This is distinct from `scripts/ops/prepare_nebius_terminal_bench.py`, which
builds a one-task TaskSet upload. Use the TaskSet helper for a single adapted
upload; use `--execution-profile nebius-terminus` when republishing a catalog
benchmark folder through `publish-local`.

Review these fields when publishing without the profile:

| Contract | Nebius Terminus requirement |
| --- | --- |
| Architecture | Linux `x86_64` (or architecture-neutral `any`), no GPU; Dockerfile inputs must actually build for AMD64. |
| Task environment | A supported Dockerfile when native preparation is enabled, or an admitted immutable image. |
| Resource limits | Explicit `cpus`, `memory_mb`, and `storage_mb`; preserve requirements of the task. Scheduling requests are a separate policy. |
| Network | `gateway-only` baseline policy; model access goes through the gateway. |
| Workspace identity | A supported declared task directory, default `agent` user, or an explicitly qualified task/verifier identity. Custom agent identity overrides remain unsupported. |
| Verifier | Shared script verifier with an exact relative `verifier/...` path; no absolute path, glob or traversal. |
| Execution shape | One step with an exact instruction path and supported environment features; private verifier isolation remains enabled. |

Admission continues to reject incompatible fields with their specific reasons.
Successful publication proves the catalog input is registered, not that every
Harbor/GB10 task is Nebius-compatible or that its image has built successfully.

Native Terminus execution keeps a declared dedicated Docker build-context
directory and its Dockerfile in the controller's frozen inputs. They are used
for image construction and are not uploaded again into the agent workspace.
This prevents setup scripts removed by the Dockerfile, and duplicate private
tests under the build context, from reappearing during evaluation. Files baked
into the image, including task data and Git history, are preserved; the private
verifier still receives its original `tests/` and `verifier/` inputs.

For a root (`.`) or unspecified build context, only the declared Dockerfile is
excluded in addition to the existing private paths. Other root runtime assets
remain available because their purpose cannot be inferred from context alone.
This does not guarantee isolation of arbitrary setup sources or nested tests in
an ambiguous root context; use a dedicated context directory for build-only
inputs. Image-only tasks retain their normal runtime inputs, without guessing
that a directory named `environment/` must be private. The native entrypoint
reads the original frozen `task.toml`, even though admission resolves the built
image separately. No shared legacy workspace policy is changed.

A task-image build failure or timeout happens before model execution. Public
diagnosis keeps that preparation failure and treats zero model calls as expected;
it does not recommend retrying the model to fix a Dockerfile. Cancellation before
execution has the same expected zero-call treatment. A genuine agent failure
without calls still retains the missing-usage diagnostic.

Builder scratch and build output use separate volumes, each able to consume the
existing total ephemeral-storage budget (16 GiB in the integration profile).
They no longer divide that budget into fixed 7 GiB partitions. The aggregate Pod
limit and reservation remain unchanged; volume maxima are not extra capacity.
Private prepare/publish temporary storage stays separate. Before deleting a
failed Job, the actuator retains bounded, sanitized Pod and Job diagnostics on
the build attempt, distinguishing storage eviction, OOM, deadline and command
failure. Exit 137 alone is not classified as OOM.

The native Terminus controller shares the execution runtime's absolute agent
deadline, including initialization. At expiry it stops agent work and uses only
the existing termination grace for local usage/native artifacts, process
quiescence and a validated workspace snapshot. If all handoff and cleanup steps
finish, controller exit 124 permits the private verifier to evaluate the partial
workspace with its own original deadline. Cancellation, forced kill, setup
timeout or failed cleanup never permits this continuation. The Trial remains
failed with `timed_out`, even when the verifier returns a numeric reward; that
reward and available artifacts remain available for diagnosis.

Harbor startup uses packaged LiteLLM metadata with build-time fallback, described
in [the controller image guide](../../deploy/harbor-runtime.md). It does not
download a price table during the agent's execution budget.

## What changes from old staging

The original instruction and `tests/test_outputs.py` are copied byte for byte.
The original seven source files, registered checksum and object location remain
under `deploy/catalog/nebius-terminal-bench/file-archive-manifest/` for provenance.
The TaskSet uploads the assertion file only as a private verifier input; the
oracle solution and original package installer are not uploaded. Source identity
also travels in the TaskSet's `source-provenance.json`; the materializer remains
responsible for its existing database input-manifest provenance.

This is an explicitly adapted AMD64/non-root/offline execution profile. The
old staging `task.toml` explicitly declares ARM64 and a root verifier; that
original execution profile remains unsupported on this Nebius path. The saved
objects do not establish whether those declarations originated upstream or
in the old importer. The instruction, fixture commands and assertions contain
no architecture or UID dependency. The derived image preinstalls Python 3.13,
pytest 8.4.1, pytest-json-ctrf 0.3.5, bash, tmux and asciinema. Trials use non-root
UID/GID 65532, `/app`, and gateway-only networking. The original runtime apt/curl/
uv bootstrap is retained as source history and is not executed. The offline
wrapper replaces `tests/test.sh` execution with the same pytest invocation and
unchanged test assertions; this is a verifier bootstrap adaptation, not a claim
that the original verifier script runs unchanged.
`archive_manifest.json` and `build_manifest.py` are collected when present; the
original task declares no required artifact. Missing or incorrect answers must
reach the original verifier and yield numeric reward zero, not turn into an
artifact-collection platform failure. Successful acceptance requires reward one
and its corresponding manifest output.

Use the existing pinned Harbor Terminus-2 runtime and trajectory capture. The
controller talks to task and verifier over two different Unix sockets on
separate private volumes; the task cannot connect to the verifier socket. These
containers do not share a PID namespace. Keep
the controller in its image-owned `/app` directory, with Python isolated mode
and an explicit `--workspace /workspace` input path. This prevents task modules,
startup hooks and `.env` files from affecting trusted imports or configuration.
Keep
tests and verifier files out of the task image and agent workspace; the trusted
executor transfers them only to a distinct verifier container after the agent
finishes. That container uses the same image and receives the completed public
workspace. Preserve the baked `/app/archive_src` files when initializing it.
Both typed Terminus events and original Harbor trajectory/recording artifacts
are retained. The Control Plane uses the existing Terminus mapper for complete
ATIF export. Model calls join the execution ledger by the exact lease and
generation so another attempt's usage cannot be attributed to this Trial.

Terminus uses the Gateway OpenAI facade with a team-owned provider connection.
The Trial must bind `provider_connection_id` for that team and select its model;
selecting a model alone is insufficient. Configure the connection through the
normal provider API using a secret reference. A platform provider entry used by
direct-completion is not automatically a Terminus BYO connection.

The Harbor runtime applies the [Harbor current-user probe patch](../../deploy/patches/README.md)
at image build time. Read-only tool checks use the sandbox default user; only
actual installation requests root. Nebius images must have tools preinstalled,
and unsupported root execution still fails normally. Remove the #1550 patch
when the Harbor pin includes the corresponding upstream fix.

## Declared mutable state and services

Tasks that produce state outside the workdir declare exact directory roots:

```toml
[environment]
mutable_paths = ["/data", "/home/agent/.local/share/jupyter"]
```

These roots must exist at handoff and cannot overlap the workdir, another root,
or runtime/private verifier paths (including `/opt/verifier`,
`/opt/verifier-python`, `/opt/verifier-assets` and `/opt/verifier-tools`). The bundle retains per-root
archives and `artifacts/mutable-paths/manifest.json`. Restore replaces directory
contents to preserve deletions. Files, modes, ownership and internal links are
preserved; cross-root hardlinks and special files fail explicitly. The aggregate
limit is 256 MiB and 100,000 entries, across at most 16 roots. Declare all state
needed by private verification, including installation metadata when relevant;
undeclared system mutations do not appear in the fresh verifier.

For an ordinary Python virtual environment with an image-owned interpreter,
declare the actual executable and any single-hop image alias it references:

```toml
[environment]
workspace_reference_files = ["/usr/local/bin/python3", "/usr/local/bin/python3.11"]

[environment.reference_file_symlinks]
"/usr/local/bin/python3" = "python3.11"
```

Inspect the original image first; these are examples, not portable Python paths.
Keep the literal link targets created by the task. Each reference group admits
at most 16 exact external leaves, outside all transferred and protected paths.
Alias targets are either canonical absolute paths or a single relative basename;
chains and cycles are rejected. Each group containing an alias must also list
its regular target. The same alias declaration can accompany
`mutable_path_reference_files` for external interpreters in mutable roots.
Reference inspection shares a 256 MiB executable-byte budget per group.
Workspaces with references also have a 256 MiB/100,000-entry archive budget.
The required `artifacts/workspace-references.json` binds the archive to the
observed references. A changed alias, executable, mode or owner fails handoff
before workspace replacement; inspection is repeated after extraction. This
does not transfer system executables or preinstall task-requested packages.

For a task whose agent must start an HTTP service:

```toml
[environment.service_lifecycle]
readiness_timeout_sec = 30

[environment.service_lifecycle.readiness]
command = "curl --fail --silent http://127.0.0.1:8000/health"
interval_sec = 1
timeout_sec = 2
retries = 10
```

If the original environment initializes a prerequisite service, also declare
`startup_command = ["/entrypoint.sh", "/bin/true"]` and
`startup_timeout_sec = 60` in `[environment.service_lifecycle]`, after reviewing
the initializer. The command must return after leaving its background service
running; do not insert an agent solution or pre-complete required task work.
The native PID 1 remains in control, so image ENTRYPOINT/CMD is not automatically
executed. This explicit declaration records the reviewed initialization.

Readiness defaults to 30 seconds and may explicitly allow up to 1800 seconds
for slow prerequisites. The initializer still defaults to 60 seconds and is
capped at 300 seconds. A readiness allowance does not extend the agent phase:
startup readiness ends at the existing phase deadline or cancellation, and
handoff readiness remains within the existing finalization deadline and grace.
Size `readiness.retries` and `interval_sec` for the intended observation window;
exhausting those retries can end readiness before its timeout.
Deploy compatible task-config consumers before uploading declarations above
the previous 300-second readiness maximum.

The controller captures bounded initializer stdout/stderr and exit status in
`diagnostics/service-startup.json`. Initialization failure occurs before model
calls. Readiness is checked before the agent for initialized services and again
at handoff. Services pause during state capture, resume for private verification
over Pod loopback, and stop after verification or failed/cancelled handoff.
Background log files need ordinary artifact declarations. Deployment admission
requires `service_lifecycle_ready=true` in the runtime profile, qualified with
the matching sandbox pause/resume implementation. A declaration alone is not
evidence of live support or original-task acceptance.

## Verification and acceptance

A local image check should run with `--network none`, the image's non-root user,
and the private assertion/bridge files available only in the verifier container.
An empty answer must yield reward zero; running the original oracle in a separate
local task container and copying its resulting workspace to a fresh verifier
must yield reward one. This is a no-model environment/verifier check, not live
Terminus acceptance. A Mac AMD64 container uses emulation and does not establish
native Nebius execution.

Live acceptance additionally requires the ordinary member submit/monitor/download
flow, actual Terminus turns through `glm-5.2`, attributed Gateway usage, original
verifier reward, successful `archive_manifest.json` output, full Trial artifacts, and
resource release/scale-zero. Do not infer any of these from a successful image
build or local oracle. This preparation does not submit a paid Trial or activate
cross-region capacity.

To establish the generic path, inspect the Trial's task-image materialization
link and native build attempt, its ready image, and the runtime plan's non-empty
`task_image_materialization_id`. The task/verifier image must match that ready
record, independently of the controller image; a different image digest alone
is insufficient. Image preparation must not consume a Trial attempt or call a
model. Then verify the real Trial's usage, cost, trajectory and original reward
assertions. A single-attempt acceptance request should retain
`retry={"max_attempts":1,"retry_on":[]}` through the ordinary Batch API; the
current `loom eval batch create` command does not expose retry configuration.

## Sandbox completion diagnostics

The trusted agent writes trajectory-derived usage before asking its task sandbox
to stop descendants. A cleanup failure keeps this accounting and fails the
phase; it does not export an unstable workspace or start the verifier. Cleanup
still runs if local usage parsing fails.

Native phase stderr reports a fixed sandbox operation, HTTP status and one of
`pid_namespace_invalid`, `process_owner_mismatch`, `process_inspection_failed`,
`cleanup_timeout`, `cleanup_signal_denied`, `cleanup_signal_failed`,
`cleanup_cancelled` or `cleanup_failed` when that server boundary fails. Unknown
response reasons become `http_error`; connection and timeout failures use fixed
transport categories. Request bodies, commands,
environments, response bodies and endpoint URLs are excluded. A cleanup timeout
or signal failure may also include a bounded snapshot of the remaining
descendant: pid, ppid, state, uid, kernel wait channel, the last SIGKILL errno,
and whether SIGKILL is pending. An already-exited process (`ESRCH`) is not a
failure. A refused signal (`EPERM`) is `cleanup_signal_denied`, not a timeout.
These codes identify the failed boundary; a completed model conversation alone
does not prove workspace handoff or verifier success. The snapshot does not
choose a longer cleanup deadline.

Process cleanup checks the effective UID and state from the same `/proc/<pid>/status`
read. It tolerates process disappearance (`ENOENT`/`ESRCH`), while foreign effective
UIDs, malformed status and other read failures stop cleanup. Proc directory
ownership alone is not authoritative: Linux can return a successful root-owned
stat after a process has been reaped. The runtime must still be PID 1 of its own
sandbox, and workspace export still requires completed cleanup.


The programmatic versioned producer (`source_registration_mode="versioned-v1"`)
also supports the opt-in execution profile; the CLI retains its existing default
registration mode.
The adapter changes a temporary staged copy before immutable source registration,
so the registered configuration and uploaded `task.toml` agree; the original
operator directory remains unchanged. Versioned publication still requires an
existing version-enabled bucket and retains journal recovery and atomic catalog
registration. Neither publishing mode requires bucket administration by default;
`--create-bucket` remains an explicit bootstrap option and does not enable versioning.
