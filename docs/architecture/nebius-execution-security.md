# Nebius execution security baseline

Status: accepted baseline for issue #1551. The project does not treat benchmark
tasks as a high-assurance hostile multi-tenant sandbox and does not require a
custom kernel-isolation runtime, escape corpus, or adversarial packet matrix
before ordinary development execution.

Hosted execution is Nebius-only. Explicit local development and disposable
fixtures remain separate; source retirement preserves historical records and
migrations and does not certify live infrastructure shutdown.

## Decision

Nebius uses the managed Kubernetes default container runtime. A custom gVisor,
Kata, or dedicated-node runtime may be added later as optional defense in depth
for a workload that has a demonstrated need, but it is not part of the baseline
execution class and must not block normal service validation.

The baseline keeps inexpensive controls that are already normal Kubernetes
practice:

- execution nodes have no public IP and use a dedicated tainted node group;
- workload images are immutable digest references;
- attempt controllers and default task containers run non-root with
  `RuntimeDefault` seccomp, dropped capabilities, no privilege escalation,
  no host namespaces, no hostPath, and explicit resource/deadline limits;
- service-account token automount is disabled for attempt Pods;
- the actuator has namespace-scoped Job/Pod permissions and receives no Nebius
  credential;
- model-provider credentials remain behind Loom Gateway;
- cleanup uses durable lease state and Kubernetes UID preconditions;
- disabling a target stops its hosted admission without enabling another provider
  or retired worker pool.

The deployment-owned `supports_task_identity` opt-in permits a task to declare
container-local root or explicit numeric UID/GID/HOME for its private Terminus
task and verifier containers. Image-default USER is resolved from the final OCI
image before compilation; admission always receives concrete numeric identity.
The controller remains non-root. Only explicitly root private containers add
`CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `SETUID`, `SETGID`, `KILL`,
and `SETPCAP` after dropping all capabilities; they retain no-new-privileges and
private PID namespaces. This supports package installation and cleanup of
task descendants that drop UID. `SETPCAP` lets native graders clear the capability
bounding set before dropping UID and running untrusted task code; the child
retains no capabilities. Host/device/kernel administration remains forbidden.
The final image ENTRYPOINT may prefix the canonical bounded sandbox command;
both are frozen into the image-bound execution plan. Trusted containers retain
their own fixed command and identity; the Pod's nonroot defaults remain intact.
Ordinary Loom tasks keep their existing nonroot defaults. Guest launchers require
the existing explicit root identity and reject inherited or explicit ENTRYPOINT
startup at admission.
A prepared service fixture is a separate untrusted native sidecar with no volume
mounts. Its reserved `fixture-` role runs as UID/GID 65532, drops all capabilities,
has a read-only root filesystem and receives no environment-based identity or
service-account token. Namespace admission rejects alternate identities, mounts,
capabilities, lifecycle hooks and ordinary-container substitution. It requires
both private sandboxes and forbids Pod-wide host aliases. The exact image and
runtime declaration are bound to the Trial's frozen build grant before admission;
a fixture cannot acquire a trusted platform sidecar role by supplying a digest.

Each private sandbox receives its own writable `/etc/hosts` and
`/etc/resolv.conf` files. The trusted materializer copies the Pod's initial files
into that sandbox's socket `emptyDir` before task processes start; exact file
mounts replace the container runtime's potentially shared network files.
Task changes cannot alter the controller's or independent verifier's copies.
Admission cannot infer arbitrary named users from image passwd metadata.
An older runtime profile defaults to rejecting this extension; enabling it
requires a compatible runtime and the explicitly target-bound
[`private-root-v1` admission policy](../runbooks/nebius-platform.md#private-task-identity-policy).
The default namespace remains restricted. Policy installation proves enforcement
before any namespace exception, and readiness remains a separate qualification.
The
[Terminus runbook](../runbooks/nebius-terminus2.md) documents declaration and
state-transfer limits. Local installation evidence does not establish live
activation or original-task acceptance.

These controls protect against common configuration mistakes and accidental
cross-workload access. They are not a claim that Loom contains intentionally
malicious kernel-escape code.

## Acceptance

The security slice is accepted when repository tests cover the restricted Pod
shape and scoped actuator permissions, and one real digest-pinned non-root Pod
runs on a Nebius execution node and is deleted afterward. The execution group
must then return to zero nodes. Network, Gateway, model, verifier, artifact, and
scheduler behavior are validated by their owning end-to-end canary rather than
duplicated as a separate adversarial security program.

The 2026-08-28 live smoke met the runtime portion: a digest-pinned non-root Pod
completed on the real Nebius execution group, and the temporary Pod, access
resources, and execution node were removed. The later gVisor work demonstrated
provider feasibility but is not required by this decision.

## Operations

Keep the actuator disabled until the normal deployment inputs exist: target
configuration, namespace, database secret, immutable actuator image, current
capacity observation, and a successful bounded end-to-end canary. This is an
operational readiness boundary, not a hostile-code gate.

For a suspected credential leak or cross-tenant data exposure, disable the
target, cancel affected leases, preserve relevant logs and object identities,
delete UID-matched Jobs, and rotate the credential that was actually exposed.
Do not require a full cluster rebuild unless evidence indicates node-level
compromise.
