# Native CI queue across GitHub and OLDLAB

Set repository variable `LOOM_CI_UNIFIED_RUNNERS=true` to opt all four development
CI workflows and normal candidate publication into the common native Actions labels
`self-hosted,linux,x64,loom-ci-exec,loom-ci-isolated-v1`. This selects an execution
interface, not a physical pool: both disposable OLDLAB KVM guests and disposable
GitHub-hosted VMs serving one nested native job can accept work. GitHub retains
assignment, job tokens, logs, artifacts and CheckRuns. Required source and
merge-group gates, job dependencies, coverage, assertions and timeouts stay intact.

Coverage aggregation maps both providers’ `*/_work/*/*` roots into the local
checkout, retaining the existing hosted paths and coverage checks.

The flag defaults off, retaining original hosted placements. Main promotion,
fork PRs, macOS, ARM, rollout/deployment jobs and optional six-hour TB2.1 image
qualification retain existing routing. Missing pool tests and normal
platform/harness publication share the common queue; candidate planning and
the small `fast-feedback` CLI/base preflight keep the same `ubuntu-24.04`
hosted image with its GitHub CLI; the protected
publication environment and credential scope are unchanged.
Existing queued jobs do not migrate or restart when the flag changes. Plan and
final gate jobs also use the common queue, avoiding a deadlock when hosted donor
VMs fill the account's hosted quota.

## Supply and availability

Qualified OLDLAB guests always keep the common label. A cloud VM that is merely
queued or booting never suppresses ready OLDLAB capacity. A controller/API error
cannot hide existing OLDLAB labels. GitHub donor VMs are supplied on demand up
to the currently confirmed Free-organization ceiling of 20; other hosted
workflows share that limit. Only runnable queued jobs whose native `needs`
dependencies have completed contribute to demand, including candidate missing-test
matrix rows and publication after its prerequisite jobs finish.

Demand inventory uses GitHub's official paginated
`/actions/runs/{run_id}/attempts/{run_attempt}/jobs` endpoint, bound to the
positive integer `run_attempt` returned with each source run. This keeps rerun
demand separate from earlier attempts. On 2026-10-19, the ordinary latest-jobs
endpoint repeatedly returned HTTP 502 for a run whose exact-attempt inventory
succeeded. Missing or invalid attempts and API errors still stop reconciliation;
they never become zero demand or permission to retire an unused worker.

GitHub has no atomic provider preference between matching runners. Hosted
capacity is supplied while OLDLAB contributes overflow, but simultaneous ready
workers can win either way. Availability takes precedence over strict GitHub
priority: no intentional wait is introduced when another matching worker is
idle. If both pools are busy, booting or unavailable, normal queue delay remains.

The five authorized shared OLDLAB hosts retain dynamic borrowable-headroom
policies, system/volatility reserves, pressure-based reclaim, scale-down to zero,
and idle-only retirement. Hardware is a ceiling, not a reservation. The sixth
host is excluded until a working management login exists. Disk-full hosts stay
standby. This mechanism does not manage GPUs, Slurm, user jobs or app deployment.

## Root controller and single-job workers

On the existing management host install distro `python3-yaml` and `python3-nacl`,
`scripts/ops/ci_runner_supply.py`, and the config/service/timer in
`deploy/ci-runners/`. Create `/var/lib/loom-ci-runner-supply` mode 0700. The existing
management token stays root-only at `/etc/loom-ci-runner-pool/github-token`; it
never enters Actions secrets or job environments.

### Reusable-workflow compatibility

CI calls image, cluster and staging validation after fast feedback. GitHub names
those jobs with the caller prefix, for example `images-validation / image plan`.
The hosted-supply controller must understand both nested dependencies and the
runner-free call nodes. The version-3 graph reader resolves only local workflow
files at the same source-run SHA and invalidates old graph/count caches. It does
not execute workflow contents or read a moving branch. It retains matrix metadata
so static job names with GitHub's appended or truncated matrix values still
contribute to demand and unblock their dependent result jobs.

A source PR does not update the installed controller. Until the reviewed
`ci_runner_supply.py` has been installed through the existing management-host
procedure and its read-only demand preview has been checked against actual
queued nested jobs, leave `LOOM_CI_REUSABLE_SUPPLY_READY` unset/false. New called
jobs and their three caller gates then use their existing GitHub-hosted fallback;
other CI jobs and independent manual diagnostics keep their previous routing.
After that deployment/readback, setting this capability variable to `true`
allows the new jobs to use the opt-in common queue as well. Keep
`LOOM_CI_UNIFIED_RUNNERS=true` for the common queue to apply. Clearing the new
capability variable restores hosted fallback for future nested jobs; it does not
cancel or migrate jobs already queued/running.

No controller install, timer restart, token/state migration or capability-variable
activation is performed by a validation workflow or source merge. Preserve the
installed script for rollback, keep the token and slot state in place, and use a
separate temporary state root for the non-`--execute` demand preview. Preview
reads GitHub metadata and writes only its local graph cache; it does not register,
dispatch, deregister or cancel runners. Activation remains an operator action.

Verify activation with a fresh current-base PR that selects image, cluster and
staging validation together. A manual CI dispatch does not call these reusable
workflows, and rerunning an old PR event can correctly fail the strict-base
preflight. Record the native nested jobs' requested labels, runner IDs and runner
names, including image builds, cluster tests and the system smoke; an online
runner inventory alone does not prove pickup. Confirm their start times follow
successful fast feedback and that the same head has all four successful bare
gate names from Actions app `15368`.

Before enabling the capability variable, nested jobs still request hosted
labels, so zero nested demand in a read-only preview is expected. A replay of
actual native job names with projected common labels can check graph matching,
but record it as a simulation and follow it with real pickup after activation.
To roll back only reusable routing, clear `LOOM_CI_REUSABLE_SUPPLY_READY`, leave
the existing common queue enabled, and let already queued/running nested jobs
drain before retiring any idle capacity.

For each slot the controller registers a fresh one-job JIT runner, encrypts the
JIT config with the repository public key, sets `LOOM_CI_HOSTED_JIT_00` through
`LOOM_CI_HOSTED_JIT_19`, and dispatches the already registered `ci-retry.yml` capacity branch with a unique
request id. Dispatch uncertainty is persisted before POST and never blindly
retried. A slot is not reused until its previous worker safely exits/retires.

The dispatcher retains its original default retry behavior, classification,
required inputs, per-source serialization and retry-job permissions. Only
`purpose=runner_capacity` skips retry governance and selects a separate
read-only capacity job with per-request concurrency. Controller-supplied zero
retry ids cannot identify a source run. Reusing the existing registered
workflow allows activation on dev without changing or promoting main.

The provider runs on native `ubuntu-24.04`. `ci_hosted_worker.py` verifies its
pinned runner archive at download, removes the outer Actions/token/runtime
state, marks the disposable VM and serves exactly one native job. The nested
job receives its own native token/workspace/artifact ownership, then exits; the
outer VM is discarded normally. The capacity job allows 195 minutes so a normal
180-minute candidate publication retains its budget plus bootstrap/teardown time.
Optional 360-minute TB2.1 image qualification runs directly on hosted Ubuntu
24.04, preserving its full budget without nesting under a donor timeout. No custom status publisher or merge controller
is involved. Only this controller's own unused donor runs may be cancelled,
after fresh busy-state readback and successful deregistration. API/deletion
failure preserves execution. Source CI is never cancelled or retried here.
Inspect root-owned `status.json` and per-slot `worker.json`; never print JIT files.

An HTTP 409 conflict when cancelling an exactly owned, already deregistered
donor defers that slot's retirement while other supply work continues. GitHub
run status can lag a terminal worker job. The controller keeps the slot record
and any remaining JIT file, writes no retirement receipt, and waits for terminal
run readback before ordinary finalization. Busy-runner and exact-title checks
still precede cancellation; other API or transport errors still stop reconciliation.

On each OLDLAB host, install `scripts/ops/ci_runner_unified_host.py` and run it
with `--install-runtime /usr/local/lib/loom-ci-runner-expansion
--retire-legacy-idle`. It adds the common/capability labels and root-owned marker
to the existing isolated guest bootstrap. Before its listener starts, the guest
installs the existing service image's pinned kubectl CLI if absent. Common-queue
guests require at least 12 GiB RAM: the unchanged Docker qualification includes
an 8 GiB nested sandbox. The five host profiles use a six-slot ceiling with
12.5 GiB budget per slot including QEMU overhead, within the existing 80 GiB
aggregate limit and shared-host reserves. The adaptive policy takes its footprint
and ceiling from the actual profile; logical runner count is not usable capacity.
Idle legacy 6 GiB guests are deregistered
before removal; busy guests stay intact. One-job shutdown and JIT shredding stay
unchanged. The registry helper permits marked disposable guests, rejects shared
unmarked hosts/writable markers, and still refuses remote Docker contexts.

## Activation and rollback

Prepare both providers and prove actual native pickup, Docker and artifacts
before enabling the flag for all eligible development jobs together. Validate
actual runner identities and source/group jobs, not just online counts.

For rollback, set the flag false for future jobs. Keep supplies online until
already queued common-label jobs drain; then stop the hosted supply timer and
retire only idle owned workers through the normal deregister-before-cancel
path. Never remove busy workers, cancel healthy CI, mass rerun/rebase PRs, or
relax protected admission rules.
