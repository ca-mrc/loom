# Trial-owned guest kernels

The optional `qemu-tcg-v1` execution mode runs each task sandbox and verifier
sandbox in a separate software virtual machine. It supplies a guest kernel for
`nested_docker`, `singularity_mounts`, and `isolated_kernel_settings`. Declaring
these capabilities is not proof that a deployment has a qualified target.
Hosted readiness additionally requires registered class/target bindings, fresh
health, capacity admission, the matching namespace policy, and installed
capability qualification.

The separately enabled `emulated_pkcs11_authentication` capability supports
trial-owned software tokens, local SSH Unix-socket forwarding and actual PAM/sudo
authentication inside the guest. It grants no physical-card access or host-device
authority. The broader `pkcs11_authentication` declaration remains unsupported.

## Admission and immutable plans

`GuestExecutionClassV1` extends the execution class with a versioned,
guest-local capability set. New class identities are
`linux-amd64-cpu-guest-v1` and `linux-amd64-cpu-guest-web-v1`. Existing ordinary
class serialization is unchanged. Trusted-host privilege, Docker sockets,
mounts, devices, host networking and shared kernel changes remain forbidden.
The emulated-auth classes are `linux-amd64-cpu-guest-auth-v1` and
`linux-amd64-cpu-guest-auth-web-v1`; they add only emulated authentication to the
historical guest capabilities. Historical class definitions remain unchanged.

A deployment profile opts in with `guest_runtime = "qemu-tcg-v1"`, task identity
support and at least 1024 MiB of runtime volume. `guest_runtime_volume_mib` and
`guest_max_artifact_bytes` override these budgets for guest tasks while leaving
ordinary plan budgets unchanged. By default only tasks declaring a guest
capability select a guest class; ordinary tasks retain their original class.
`TrialConfig.isolation` (#2314) can change that per batch: `guest` forces a
plain guest (no guest capabilities) on an ordinary task, and `container`
keeps an ordinary task in a pod but is rejected as
`isolation_container_unsatisfiable` when the task declares guest capabilities.
`auto` is the default and is normalized away, so existing plans are unchanged.
`effective_guest_capabilities` is the single rule the planner, workload
requirements and admission share. A forced guest also requires the Terminus
controller (`isolation_guest_response_only`) and, until shared grading inside a
forced guest is qualified, rejects an explicitly requested shared verifier
(`isolation_guest_shared_unsupported`).
The compiler requires the Terminus controller with two private sandboxes and
explicit root task/verifier identities. Unresolved prerequisites and external
cluster, general PKCS#11 or DPDK requirements remain admission rejections.
Emulated authentication additionally requires the deployment profile's explicit
`supports_emulated_pkcs11 = true`. Only a task declaring that capability selects
the new class; enabling the profile does not rebind existing guest tasks.
Its distinct target can be staged while this readiness flag is false or absent.
Bootstrap registers that target disabled, and compilation continues to reject
emulated-authentication tasks until the profile explicitly opts in. This permits
installed qualification before enabling admission.

Each sidecar carries `guest_execution` with schema
`loom.guest-execution.v1`, runtime `qemu-tcg-v1`, and the sorted, unique declared
capabilities. A plain guest carries an empty list; Go rejects a missing
(`null`) list, and an empty one adds no `--nested-docker` or other capability
setup. Python and Go both validate the exact class, two private roles,
matching task images/resources, root identities, private socket/probe paths,
timeouts and payload reservation. Historical requirements cannot silently
upgrade into a guest. Nonroot guest identities are currently rejected.

## Isolation and lifetime

The existing Kubernetes Job, execution lease and attempt generation own the
outer sidecars. QEMU uses TCG without KVM, host devices or privileged container
mode. The outer root process drops all capabilities except `DAC_OVERRIDE`,
needed to read the immutable task image. Its root filesystem and runtime payload
are read-only. Only its own state and RPC socket volumes are mounted writable;
controller credentials and private verifier inputs are not mounted there.

A read-only 9p view of the outer sandbox filesystem forms an overlay lower layer. A private,
bounded ext4 disk holds the writable overlay and Docker data root. The payload
is another read-only export. The task image and payload stay immutable throughout
the guest lifetime. The outer sandbox's own changing launcher/socket mounts are
also visible read-only beneath reserved runtime paths; reading these paths is
unsupported and may return cached data. They contain no controller credentials,
foreign sandbox state or private verifier inputs. QID remapping keeps inodes on
different outer mounts distinct. The sandbox server runs as PID 1 of an
inner guest PID namespace, so process cleanup excludes guest kernel threads.
The guest mounts its own `/dev/shm` tmpfs for POSIX shared memory and named
semaphores, including Python multiprocessing. Its limit is half of guest RAM,
within the existing memory envelope; it disappears with the guest. No host or
other trial's shared-memory mount is exposed.

RPC streams use yamux over the named `loom.rpc` virtio-serial device. The outer
Unix socket preserves the existing sandbox API. There is no host TCP RPC
listener. A lost channel or guest reboot ends the incarnation; no reconnect can
replace the filesystem behind an existing sandbox identity.

The launcher exclusively creates its state directory, kills and waits for QEMU
on cancellation, and deletes its owned disk/initramfs/channel files. It retains
a tombstone until Pod deletion. A sidecar restart fails against that tombstone,
allowing existing execution reconciliation to report failure rather than reuse
an incomplete environment. Kubernetes ownership and cleanup reconciliation
remain authoritative; local process tests do not establish controller-restart
or hosted cleanup qualification.

Startup is bounded separately from task commands. Disk preparation has 30 seconds;
the subsequent guest boot has 90 seconds, including up to 60 seconds for Docker
to expose its private readiness API. The Kubernetes startup probe allows 150
seconds so it does not interrupt these stages. A healthy Docker daemon on a
one-vCPU TCG guest can take longer than 30 seconds to initialize. Daemon exit,
deadline expiry and cancellation still retire the incarnation without reuse.

## Resources, networking and artifacts

Minimum declared resources are 1000 CPU millicores, 512 MiB RAM and 160 MiB
storage. These are boot minima, not recommended budgets for Docker, a debugger,
or large image builds. The launcher reserves one eighth of the memory limit,
with a 256 MiB minimum, for QEMU, host page tables and disk writeback. Guest page
cache occupies QEMU anonymous memory and cannot be reclaimed by the outer host;
the reserve must remain available even when every guest RAM page is touched.
Thus an 8 GiB sandbox exposes 7 GiB of guest RAM without enlarging its outer limit.
32 MiB of each sandbox storage allocation covers launcher metadata/initramfs. The TCG translation
cache is explicitly bounded to 64 MiB within the emulator reservation. The remaining
storage bounds the guest disk, including Docker layers, containers and cache.
The controller allocation and request also reserve the shared runtime volume,
so placement cannot count its kernel/tool payload as free storage.

QEMU user networking inherits the outer Pod network policy. Guest web commands
translate the controller's loopback proxy to `10.0.2.2`. The guest Docker daemon
uses `http://10.0.2.2:18791`, mapped to the guest-mode controller's loopback
listener. That listener retains the same destination allowlist, phase deadline,
Gateway authorization and concurrency bounds. No active authorized listener
means no daemon registry egress. Package tools inside build steps must use the
authorized proxy; root in the guest does not authorize additional destinations.

The immutable plan's `max_artifact_bytes` bounds guest file transfers and output
capture. Uploads and downloads stream bounded chunks. The configured artifact
budget must cover complete image/core outputs; increasing it does not enlarge
task disk space or controller workspace automatically. A 4000 MiB image needs
space for bootstrap contents, image/export copies and verifier handoff, plus an
artifact allowance above its complete byte size.

Docker cache ownership is one sandbox incarnation within one attempt/generation.
The task can reuse and invalidate layers during that attempt. The verifier and
other attempts have separate daemons and disks. There is no retained cache
across attempts, and cancellation or Pod deletion removes the cache with its
owned state.

## Packaging and qualification

An independent, single-primary Nebius platform declares one guest sibling with
`guest_execution_target: {"target_id": "<distinct guest target>"}` alongside its
existing `private-root-v1` task identity policy. The guest catalog binds the
immutable `capacity_owner_target_id` to the ordinary target. Both classes share
one collector, namespace quota, node inventory and native builder; registration
invalidates old capacity observations until the collector captures the current
target membership. See [capacity ownership](nebius-service-execution.md).

The renderer creates a separate guest actuator and health identity. The extended
namespace policy permits only its exact guest launcher, probes, bounded state
volumes, read-only runtime/root and `DAC_OVERRIDE` capability. Native private
sidecars retain their existing shape. Bootstrap registers and prices the guest
through the control-plane API, using the owner's immutable price snapshot;
it leaves a new guest disabled and preserves existing guest intent on repeat.
Each actuator reconciles only its own leases, ignores declared sibling inventory
in the shared namespace, and reports unknown target annotations as drift.

Guest readiness in the published profile requires explicit protected workflow
configuration and storage/artifact budgets. This does not activate the target.
Hosted qualification and a fresh capacity observation precede explicit operator
activation through the existing target-health API. A platform rollout cannot
remove or rename an installed guest, or replace its ordinary owner: those
operations require a separately designed retirement protocol. Disabling/draining
the guest retains its capacity accounting and cleanup authority.

Emulated authentication adds `emulated_auth_execution_target: {"target_id":
"<distinct auth guest target>"}` beside the retained historical guest target.
Its catalog (`emulated-auth-catalog.json`), actuator, health and operator intent
are independent; its collector, quota, namespace and capacity owner remain
shared. Readiness and this configuration must agree. The namespace policy applies
the same exact guest restrictions to both declared target IDs. Neither guest
may be removed or renamed through a normal rollout. This standalone extension
does not extend the separate shared-pool cutover's fixed participant roster.

[The guest payload build](../../deploy/guest-runtime/README.md) locks kernel,
QEMU, Docker, Buildx and dependency versions/checksums. The execution-runtime
image adds static Go launcher and sandbox binaries. Ordinary plans do not copy
the large payload into their runtime volume.

`tests/integration/test_guest_sandbox_runtime.py` belongs to the Docker CI lane.
CI builds/extracts the payload and must not skip the fixture for a missing
payload. It exercises genuine core generation, independent kernels/filesystems,
process control, cancellation/reboot, Docker builds/cache invalidation,
container execution/artifact retrieval and registry proxy denial. Capability
acceptance still requires the real Singularity and pinned Node/LLDB/llnode
workflows, large-artifact round trips, deployed lifecycle reconciliation and
ordinary-entrypoint qualification. Incomplete original task packages retain
separate input blockers.

`tests/integration/test_guest_emulated_auth.py` adds a generic SoftHSM/PAM fixture
to the Docker lane. It verifies real signatures, forwarding-dependent sudo,
invalid PIN/certificate rejection, simultaneous guest isolation and state-disk
retirement with the same outer confinement. Benchmark instructions, private
grading and solutions are not part of this fixture. Local evidence does not
replace installed qualification or the original task's recorded outcome.
