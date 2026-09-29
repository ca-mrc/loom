# Independent integration platform state

The integration execution node group's `template.metadata.labels` explicitly
declares `loom.nebius/node-os=linux` and `loom.nebius/node-arch=amd64`, in addition to
the existing Loom isolation labels. These describe the configured Linux/x86_64
`cpu-e2` nodes. Nebius ignores custom template labels containing `kubernetes.io`
or `k8s.io`, so native builds use these provider-supported custom keys. They
must be available to the autoscaler's zero-node template before kubelet starts,
otherwise the pending build can report `NotTriggerScaleUp` with a node-selector
mismatch. This does not change the node group's CPU/disk shape or autoscaling
minimum/maximum, and does not label or change the system node group. Keep the
build Pod's OS/architecture selectors intact. Apply this change to the existing
`module.platform.nebius_mk8s_v1_node_group.integration["execution"]` resource;
do not create another node group or raise its minimum to work around scale-up.

For an operator-applied recovery, merge the two labels into the current
NodeGroup **template** labels using the native API, preserving all existing
labels and matching this Terraform change. A `kubectl label node` update does
not repair a group scaled to zero. Verify ordinary build demand subsequently
triggers scale-up; a successful metadata update alone does not prove recovery.

This root adds dedicated system and execution node groups, four
native storage buckets with three scoped identities, and one fixed public IPv4
allocation to the existing Nebius cluster. Existing cluster/network/registry IDs
are inputs. This root does not import, mutate or own the previous execution
stack or its deployment gateway.

Copy `../integration-platform.tfvars.json.example` outside the checkout and fill
IDs from the existing foundation's reviewed outputs. Configure the existing S3
backend with a **different state key**, for example
`nebius/development/eu-north1/integration-platform.tfstate`. The backend key must
remain within the operator's permitted prefix. Never use the execution stack's
`terraform.tfstate` key. Keep credentials in the existing operator profile and
protected environment, not in tfvars or command arguments.

On the configured macOS operator host, run from the repository root. The existing
wrapper reads the state identity from Keychain into the Terraform child process;
it rejects ambient AWS credentials and never places them in the backend file.

```sh
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform init -backend-config=/secure/platform.s3.tfbackend
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform plan -var-file=/secure/platform.tfvars.json -out=/secure/platform.tfplan
terraform -chdir=deploy/terraform/nebius/platform show /secure/platform.tfplan
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform apply /secure/platform.tfplan
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform plan -var-file=/secure/platform.tfvars.json -detailed-exitcode
```

The final readback should report no changes. Save the plan digest, non-secret
output IDs and readback beside deployment evidence. Reinitialize from the same
versioned backend to recover local state; do not import resources from the
execution state or force-unlock an active operation.

One 4-vCPU/16-GiB system node remains running. The execution group starts at zero
with the native API technical maximum of 100 nodes configured in Terraform.
Explicit lower `integration_platform.execution_max_nodes` values remain valid;
review existing tfvars before removing an intentional limit. Keep that value
aligned with `capacity_policy.max_nodes` in the platform environment when
choosing a lower operator ceiling. CP admission follows fresh provider quota and
actual Kubernetes fit; this technical maximum does not reserve or promise
100 nodes. Terraform remains the sole cloud-limit owner: no runtime writer or
`ignore_changes` is introduced. CI and image publication stay on GitHub-hosted
machines; no cloud runner node groups are provisioned. Platform services use the dedicated system node selector and
tolerations; only queued workloads scale execution nodes.
All nodes are private. Registry pull identity has no publication credential.
The existing shared cluster's costs are unchanged; review current compute,
disk, public IP and storage prices before applying the saved plan.

Bucket access-key resources use explicit secret delivery and output only resource
IDs. Retrieve each credential through its authorized operator path into the
namespace Secret, never Terraform output or rendered manifests. Canonical and
backup buckets are versioned; source objects use the existing acknowledged
retention/GC contract. There is no calendar deletion of completed user output.

After apply, follow `docs/runbooks/nebius-platform.md` and
`docs/runbooks/nebius-candidate.md` from the repository root. DNS, certificates, scoped
Secrets, signed publication, restore exercises and real
workload acceptance are separate operations; successful Terraform alone does
not establish their completion. Do not destroy versioned buckets or retire the
previous platform until migration/retirement acceptance authorizes it.


Optional `regional_execution_targets` entries add execution-only clusters through
`../modules/regional-execution`. See `../regional-execution.tfvars.json.example`.
The default empty map creates nothing. Each configured target adds a control
plane and system/execution node groups, using an existing regional project and
subnet. Existing-ID mode supplies a regional read-only registry identity and
creates exactly these three resources. Optional `managed_identities` instead
requires three distinct SPKI PEM public keys and two existing viewer group IDs;
omit the existing node-pull ID in that mode. See
`../regional-execution-managed-identities.tfvars.json.example`. Managed mode adds
four regional service accounts, three runtime authorized-public-key registrations
and two memberships (collector observer and node registry-pull), for exactly
12 creates on a new target. Node groups depend on those memberships and use the
new regional pull account. No key is registered for node-pull; actuator and
Gateway receive no cloud group membership. Existing groups must independently
be verified as viewer-only at the intended tenant/registry scopes. No group or
access permit is created, and no editor authority is added. Only public key text
enters Terraform; private keys remain in protected operator files. Output maps
provide runtime account IDs and authorized-public-key IDs, never credentials. The managed public API requires
TLS, native IAM authentication and the separate least-privilege Kubernetes RBAC.
Optional `public_control_plane_cidrs` defaults to an empty source-IP restriction;
explicit operator CIDRs are preserved. This avoids requiring separate NAT or
fixed egress infrastructure. Public API activation remains explicit in the
reviewed cloud plan. No workload public interface, registry, state service or
VPN is added. The source input remains in this platform state; a cloud plan and
its prerequisite regional permissions/cost require separate owner review.
The complete separate-cluster render/operator path is in
`docs/runbooks/nebius-platform.md` from the repository root.

Regional targets require an explicit `node_platform` from that region's native
catalog. The EU-west example uses `cpu-d3` with4vcpu-16gb system and16vcpu-64gb
execution presets; the original EU-north integration remains `cpu-e2`. Terraform
validates CPU-only intent but cannot prove native inventory or Kubernetes
version compatibility. Confirm those through the read-only native catalog
before reviewing an activation plan, and use the matching regional price SKU.

Set `suspended = true` in a regional target to keep both node groups at fixed
zero nodes with autoscaling disabled. The cluster, node-group identities, IAM
and network configuration remain available for recovery. The default is `false`
for compatibility; the EU-west examples explicitly start suspended. Disabling
Loom execution routing alone does not suspend cloud nodes.

Before suspension, disable the target in Loom, finish or cancel its work, and
check for retained workloads and storage. Change the **owning state inputs**,
review the plan, and apply through the existing operator path. Do not replace a
whole `regional_execution_targets` map with a partial override: Terraform does
not merge maps across variable files. Verify native node counts, autoscaling,
VMs and attached disks after drain, then check subsequent provider usage. Zero
nodes does not prove that separately retained volumes, IPs or services are free.
To resume, set `suspended = false`, review the system-node cost and execution
ceiling, apply, and verify system readiness before enabling Loom routing.
