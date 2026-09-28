# Trial-owned guest kernels

The optional `qemu-tcg-v1` execution mode runs each task sandbox and verifier
sandbox in a separate software virtual machine. It supplies a guest kernel for
`nested_docker`, `singularity_mounts`, and `isolated_kernel_settings`. Declaring
these capabilities is not proof that a deployment has a qualified target.
Hosted readiness additionally requires registered class/target bindings, fresh
health, capacity admission, the matching namespace policy, and installed
capability qualification.

## Admission and immutable plans

`GuestExecutionClassV1` extends the execution class with a versioned,
guest-local capability set. New class identities are
`linux-amd64-cpu-guest-v1` and `linux-amd64-cpu-guest-web-v1`. Existing ordinary
class serialization is unchanged. Trusted-host privilege, Docker sockets,
mounts, devices, host networking and shared kernel changes remain forbidden.

A deployment profile opts in with `guest_runtime = "qemu-tcg-v1"`, task identity
support and at least 1024 MiB of runtime volume. `guest_runtime_volume_mib` and
`guest_max_artifact_bytes` override these budgets for guest tasks while leaving
ordinary plan budgets unchanged. Only tasks declaring a guest
capability select a guest class; ordinary tasks retain their original class.
The compiler requires the Terminus controller with two private sandboxes and
explicit root task/verifier identities. Unresolved prerequisites and external
cluster, PKCS#11 or DPDK requirements remain admission rejections.

Each sidecar carries `guest_execution` with schema
`loom.guest-execution.v1`, runtime `qemu-tcg-v1`, and the sorted, unique declared
capabilities. Python and Go both validate the exact class, two private roles,
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
