# Nebius execution-target infrastructure

This directory owns one Terraform state for the shared Loom Nebius execution
cluster. It creates networking, registry, evidence storage, IAM identities, one
Managed Kubernetes control plane, a fixed system node group, and an autoscaled
execution node group. Development, staging, and production remain distinct
logical target bindings inside that cluster.

It does **not** authorize cloud creation, quota changes, billing, credentials,
production deployment, Kubernetes workload bootstrap, or Loom traffic. A web
console login is not a Nebius CLI profile. Before every plan or apply, follow
the authorization and cost gate in
[`docs/runbooks/nebius-infrastructure.md`](../../../docs/runbooks/nebius-infrastructure.md).

The capacity observer service account, tenant read-only permit, and authorized
public key are part of this state. The public key omits `expires_at`; its private
credential is supplied only to the Kubernetes runtime Secret and is never
committed or printed. Nebius SDK access tokens remain short-lived and refresh
automatically from that stable key, so a human CLI/browser login is not a
runtime dependency. Infrastructure convergence likewise uses a dedicated
non-expiring service-account profile for the Nebius provider and a separate
non-expiring Object Storage key held in macOS Keychain for the S3 backend.

## Layout

- `modules/execution-target`: reusable, version-pinned target resources.
- `stack`: the only root module operators plan and apply.
- `targets`: the sole shared-cluster example. Its development-named anchor is
  retained deliberately so the existing live state converges instead of
  creating replacement infrastructure.
- `backends`: the existing development remote-state anchor with a native lock
  file. The state bucket is an independently authorized prerequisite.

The cluster example is checked against the three environment bindings in
`config/service-execution-topology.json`. Each binding has a distinct namespace,
target ID, health identity, and evidence prefix, while all three bind the same
physical cluster scope, region, and failure domain. A second cluster, state, or
region requires a separately accepted requirement and owner decision.

## Version and offline validation

The contract pins Terraform `1.16.0` and `nebius/nebius` `0.6.46`. Provider
locks contain both `darwin_arm64` and `linux_amd64` checksums.

```bash
python3 scripts/check_nebius_iac.py
terraform fmt -check -recursive deploy/terraform/nebius
terraform -chdir=deploy/terraform/nebius/modules/execution-target init -backend=false
terraform -chdir=deploy/terraform/nebius/modules/execution-target test
terraform -chdir=deploy/terraform/nebius/stack init -backend=false
terraform -chdir=deploy/terraform/nebius/stack validate
```

These checks exercise repository structure and mocked Terraform plans only.
Validate the root `stack`, not the reusable child module in isolation: the
root supplies the child module's `nebius.no_default_labels` provider alias.
They do not prove Nebius credentials, quota, capacity, price, creation,
convergence, Kubernetes access, pod execution, autoscaling, isolation, disaster
recovery, or destruction.

Provider `0.6.46` exposes Managed Kubernetes audit logging but no native
monitoring, alert, dashboard, or budget resources. This stack enables audit
logs and exports stable target/cluster/node-group identities for #1552's
collector and accounting work. Account-level budget enforcement is separately
configurable; cluster health must never be treated as cost or alert evidence.

## Optional staging source spool

The existing shared state can additionally own one dedicated staging source
bucket, without another cluster, state, or VM. It is disabled by default:
omitting `staging_spool` leaves all five spool resources absent and preserves
the original resources and credentials. To opt in, add this non-secret input to
the protected variable file after selecting a globally unique unused name:

```json
"staging_spool": { "bucket_name": "loom-eu-north1-example-staging-spool" }
```

An opt-in plan adds exactly a bucket, service account, group, membership, and
access key. Never import/reuse an existing state or evidence bucket/key as this
spool. The variable rejects evidence-bucket equality and non-spool names; the
operator must also compare the selected name with the actual backend bucket
because Terraform does not expose backend configuration to module variables.

The dedicated group receives only `storage.object-editor` through that bucket's
policy: object read/write/list, multipart create/complete/abort, and deletes for
acknowledged materialization GC. It receives no project/tenant permit, anonymous
access, bucket administration, or policy-edit permission. Completed objects have
no expiry; only unfinished multipart uploads are aborted after seven days.
Versioning is disabled so application GC removes bytes rather than accumulating
old versions. No new size/budget limit is imposed; account quota still applies.
The canonical staging PostgreSQL and MinIO remain authoritative destinations.

The key omits `expires_at` and explicitly uses `EXPLICIT` delivery. The existing
automation service-account profile obtains refreshed API tokens; no personal
browser session or runtime STS-refresh support is required. The sensitive root
output `staging_spool` contains endpoint, region, bucket and service-account IDs,
`access_key_resource_id`, `aws_access_key_id`, and delivery mode, **not secret
bytes**. A protected bootstrap consumer captures the result of
`nebius iam v2 access-key get-secret --id <access_key_resource_id>` using the
automation profile, checks the returned top-level `aws_access_key_id` against
the protected output, and installs the top-level `secret` into the canonical
staging source Secret. Neither response nor raw Terraform output belongs in
terminal logs or review evidence. The persisted key can be fetched again after
provider refresh/import; creation does not depend on a one-time INLINE result.
Use the supported Secret reconciliation/rollout path when deliberately rotating
credentials; non-expiring does not mean irrevocable.

The native endpoint is `https://storage.<region>.nebius.cloud`; ordinary system
CA verification remains enabled. Tests prove plan shape, policy boundaries,
omitted expiration, and output references, not live S3 authorization. Activation
still requires the reviewed exact plan and a real small-object, multipart,
readback, canonical materialization, and post-acknowledgment GC smoke.

Provider/API contracts: [access-key resource at the pinned provider version](https://github.com/nebius/terraform-provider-nebius/blob/v0.6.46/docs/resources/iam_v2_access_key.md),
[non-expiring service-account access keys and native endpoints](https://docs.nebius.com/iam/service-accounts/access-keys),
[Object Storage action/role matrix](https://docs.nebius.com/object-storage/supported-actions),
and [bucket-scoped policies](https://docs.nebius.com/object-storage/buckets/bucket-policy).

## Integration catalog storage permissions

The integration canonical storage group has a `storage.viewer` IAM access permit
scoped to the artifacts bucket. Native catalog publication checks
`GetBucketVersioning` before uploading each task and requires `Enabled` plus a
non-null object version ID. Nebius's
[Object Storage action/role matrix](https://docs.nebius.com/object-storage/supported-actions)
excludes this bucket-settings read from `storage.object-editor`. The additional
read permission must be an IAM permit on that bucket; adding `storage.viewer`
to its object-access policy alone does not authorize the S3 versioning call.

The permit does not grant bucket configuration or policy mutation privileges.
Source, backups and trajectories keep their existing access, and all bucket
object policies, including native-builder read/cache rules, remain unchanged.
After reconciliation, verify `GetBucketVersioning` with the deployed canonical
credential; a successful operator IAM read does not prove that the publisher can
use the S3 API. Preserve versioning and require a real version ID on a bounded
publication-storage probe.

When adopting an existing operator-created permit, import its verified ID into
the platform root before applying, using the existing protected backend and
variable file:

```sh
terraform -chdir=deploy/terraform/nebius/platform import \
  -var-file=/protected/platform.tfvars.json \
  'module.platform.nebius_iam_v1_access_permit.integration_catalog_metadata["integration"]' \
  '<verified-permit-id>'
```

Read back its parent group, artifacts bucket resource ID and `storage.viewer`
role before import. Review the subsequent plan; importing this permit must not
recreate it or change other cloud resources.

## Plan shape

The deployment gateway now has a separately reserved private `/32` allocation
attached through the mutable `network_interfaces[].aliases` field. The
replacement-only primary `ip_address` and fixed public allocation are unchanged.
The output retains `deployment_access.private_address` for its original DHCP
meaning; private service consumers must instead use
`private_service_allocation` and `private_service_cidr`. The latter survives
a VM replacement while the allocation is retained.

Fresh stacks wait for Kubernetes to reserve its Service CIDR first. A
postcondition requires the resulting service alias to be a `/32` in the node
network; if this check fails, the allocation may already exist, but attachment
is blocked. Investigate and clean up only that exact new allocation.

An existing-stack plan should add this one allocation and update only the
gateway in place, with no VM/disk replacement. Cloud assignment does not prove
the guest owns the address: the scoped gateway proxy installer in the
[runbook](../../../docs/runbooks/nebius-infrastructure.md) persistently configures
the approved alias without changing DHCP/default routes. Neither planning nor
this output activates the staging link or provisions application credentials.

After the runbook gates are satisfied, prepare the one shared state anchor:

```bash
cp deploy/terraform/nebius/targets/development-eu-north1.tfvars.json.example /secure/path/development-eu-north1.tfvars.json
cp deploy/terraform/nebius/backends/development-eu-north1.s3.tfbackend.example /secure/path/development-eu-north1.s3.tfbackend
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack init -reconfigure -backend-config=/secure/path/development-eu-north1.s3.tfbackend
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack plan -var-file=/secure/path/development-eu-north1.tfvars.json -out=/secure/path/development-eu-north1.tfplan
terraform -chdir=deploy/terraform/nebius/stack show -json /secure/path/development-eu-north1.tfplan > /secure/path/development-eu-north1.plan.json
```

Never commit populated variables, backend credentials, kubeconfigs, plan
files, state, tokens, or private keys. The populated variable file includes the
non-secret public half of the capacity-observer key; keep it alongside the
other protected exact-target inputs. Apply only the reviewed saved plan and
only after the owner has approved its exact target, resources, maximum hourly
and monthly cost, cleanup deadline, and residual-cost list.

## Optional public Service allocations

The cluster subnet denies public allocations by default. A public Kubernetes
Service LoadBalancer needs its fixed allocation's pool to be available in that
subnet. Once the existing network has the selected public pool attached, set
`enable_public_service_allocations = true` in the protected foundation stack
variables to let the subnet inherit those pools. This changes allocation
eligibility for resources in the shared subnet; it does not automatically
assign public addresses. Both system and execution node interfaces continue to
omit `public_ip_address`.

For the existing integration platform, inspect the foundation plan with this
single opt-in: the expected infrastructure change is the existing target subnet
`ipv4_public_pools.use_network_pools` from false to true, with no creates,
deletes, allocation/IP replacement, network change or node-group update.
Use the original foundation backend and variables, not the independent platform
state. Review all other drift separately and obtain approval for this shared
subnet update before applying; do not hide it behind an allocation replacement
or a guessed LoadBalancer annotation.

See [Nebius LoadBalancer configuration](https://docs.nebius.com/kubernetes/clusters/load-balancer)
and the [subnet resource schema](https://docs.nebius.com/terraform-provider/reference/resources/vpc_v1_subnet).
