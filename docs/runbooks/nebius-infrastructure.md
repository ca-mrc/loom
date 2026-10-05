# Nebius infrastructure operations

This runbook provisions and accepts one shared Kubernetes execution cluster
using the fixed installation bindings in `config/service-execution-topology.json`.
That file is not a developer roster or a maximum environment count. Dynamic
child identities and their currently execution-disabled renderer are described
in the [managed environment contract](../architecture/nebius-primary-platform.md#managed-environment-identity-and-rendering);
this runbook does not yet enroll those children. It is intentionally separate from
Kubernetes workload bootstrap: Terraform establishes the cloud foundation; a
later, independently reviewed step installs environment-local Loom resources
and exercises them. For the optional execution-only regional extension of the
pure-Nebius integration environment, use [the platform runbook](nebius-platform.md);
its primary platform remains centralized.

## Authority boundary

Repository validation, a successful Terraform plan, an existing web-console
session, available quota, and an account balance are read-only evidence. None
authorizes resource creation or spend.

Before each mutable operation, record all of the following in the owning issue:

1. exact cluster scope, environment bindings, tenant, project, region, profile,
   and reviewed saved plan;
2. resources created or changed and the maximum node count;
3. current Nebius hourly and 730-hour monthly estimate, including disks,
   public IPs, object storage, egress, taxes, and any items the calculator omits;
4. budget/alert status, cleanup deadline, destroy owner, and residual-cost list;
5. explicit owner approval for that exact operation and cost envelope.

The 2026-09-02 live development readback found a 200 non-GPU-vCPU quota;
refresh it rather than treating that historical value as current. Per the
2026-09-05 owner revision of #1538, acceptance ends at the maximum batch
concurrency supported by the current account quota and accepted task resource
profile. Neither 200 concurrent tasks nor the pending 512-vCPU / 16-VM request
is a closure prerequisite. Quota is an admission ceiling, not a reservation or
proof of regional stock.
Refresh price and quota immediately before apply and retain the provider request
IDs with the owning issue.

## Authentication and state prerequisites

Use one Nebius CLI profile and one existing project for the shared cluster. Browser login
alone does not configure the CLI. For a human-driven, auditable development
apply:

```bash
export LOOM_NB_PROFILE=loom-development-eu-north1
export LOOM_NB_PROJECT=project-REPLACE-DEVELOPMENT-EU-NORTH1
nebius profile create \
  --profile "$LOOM_NB_PROFILE" \
  --endpoint api.nebius.cloud \
  --federation-endpoint auth.nebius.com \
  --parent-id "$LOOM_NB_PROJECT"
nebius profile list
```

Do not print or commit the profile configuration. Automation uses the dedicated
`loom-development-eu-north1-terraform-automation` service account and an
authorized-key profile without `expires_at`, never a copied human token. This
identity is not attached to a VM. The current monolithic state manages tenant
IAM groups, so its reviewed bootstrap membership must be able to read and
update those groups; splitting IAM into a separate state is required before
that tenant-level permission can be narrowed.

The recurring capacity observer uses a Terraform-managed authorized public key
with no `expires_at`. Generate the key pair once in a protected directory, put
only the public PEM in `capacity_observer_public_key_pem`, and install the full
Nebius credential JSON through the runtime bootstrap below. The SDK exchanges
that key for short-lived access tokens and refreshes them automatically. Do not
substitute a copied human access token or a static registry token.

The cluster uses a pre-existing versioned state bucket and the separate
`loom-development-eu-north1-terraform-state` service account. Its active S3
access key has no `expires_at`; the AWS-like key ID and secret live only in the
`loom-nebius-terraform-state` macOS Keychain item. The checked-in wrapper reads
that item into the Terraform process environment and rejects ambient AWS
credentials. Never put access keys in `.tfbackend` files. Reuse the existing
development remote-state key so convergence cannot fork the live cluster into
a new state, and confirm `use_lockfile = true`.

## Read-only preflight and saved plan

From the repository root, use exact pinned Terraform `1.16.0`:

```bash
python3 scripts/check_nebius_iac.py
terraform fmt -check -recursive deploy/terraform/nebius
terraform -chdir=deploy/terraform/nebius/modules/execution-target init -backend=false
terraform -chdir=deploy/terraform/nebius/stack init -backend=false
terraform -chdir=deploy/terraform/nebius/stack validate
terraform -chdir=deploy/terraform/nebius/modules/execution-target test
```

Copy the sole shared-cluster input and its existing backend anchor to a protected
directory. Replace the placeholder tenant, project, globally unique bucket
name, and state bucket; do not change topology fields without changing and
reviewing the canonical topology contract. The committed active envelope is
`0..8` regular `16vcpu-64gb` execution nodes with 64 Pods per node. Its 128-vCPU
execution envelope supports 56
concurrent 2-vCPU tasks while preserving 2 vCPUs and 8 GiB per node for
platform overhead. This smaller node shape avoids depending on currently
unavailable 32- and 48-vCPU regional inventory and consumes at most eight of
the twelve historically available VM slots. The capacity policy also retains
the historical expansion proposal (`0..10` at `48vcpu-192gb`, 200 tasks,
512 vCPUs / 16 VMs). Those target fields are not an acceptance requirement and
must not trigger expansion automatically.

Before the final #1538 batch, retain a fresh quota snapshot and the derivation
of its maximum: subtract existing shared-account usage and required system
capacity, then account for VM, CPU, memory and disk limits and per-node
allocatable overhead for the unchanged task profile. Change only Loom's exact
execution envelope through reviewed configuration if the existing 56-task
ceiling is below that maximum. Do not stop or resize other users' resources to
recover headroom. Record physical stock separately; a smaller stock-limited
wave is useful evidence but cannot silently replace the quota-derived target.

```bash
export LOOM_NB_TARGET=development-eu-north1
export LOOM_NB_INPUT=/secure/path/$LOOM_NB_TARGET.tfvars.json
export LOOM_NB_BACKEND=/secure/path/$LOOM_NB_TARGET.s3.tfbackend
export LOOM_NB_PLAN=/secure/path/$LOOM_NB_TARGET.tfplan
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack init -reconfigure -backend-config="$LOOM_NB_BACKEND"
terraform -chdir=deploy/terraform/nebius/stack validate
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack plan -var-file="$LOOM_NB_INPUT" -out="$LOOM_NB_PLAN"
terraform -chdir=deploy/terraform/nebius/stack show -json "$LOOM_NB_PLAN" > "$LOOM_NB_PLAN.json"
shasum -a 256 "$LOOM_NB_PLAN" "$LOOM_NB_PLAN.json"
```

Review that the plan touches only the shared cluster state, nodes have no public
IP fields, the API is private unless exact non-world operator CIDRs were
approved, both node groups forbid reservations, execution minimum remains zero,
all three environment bindings retain distinct namespaces/evidence prefixes,
and no unrelated project resource changes. Scan the JSON for secrets before
retaining it as evidence.

## Authorized apply and convergence

Apply only the reviewed plan hash:

```bash
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack apply "$LOOM_NB_PLAN"
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack output -json > /secure/path/$LOOM_NB_TARGET.outputs.json
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack plan -detailed-exitcode -var-file="$LOOM_NB_INPUT"
```

The second plan must exit `0`; exit `2` is drift and blocks acceptance. Redact
operator addresses and identifiers before publishing evidence. Confirm in the
Nebius API or console that the cluster and both node groups are Ready, the
execution group has the configured autoscaling bounds, node interfaces have no
assigned public IPs, the registry exists, the evidence bucket is versioned and
audited, and only the intended groups have access.

Nebius provider `0.6.46` has no Terraform resource for provider monitoring,
alerts, dashboards, or billing budgets. The target outputs are the durable
identity input to #1552's collector, alert, and accounting layer. Account-side
budget enforcement is separately configurable, while telemetry and cost
readback remain acceptance evidence; audit logs alone are not a substitute.

For a private control plane, use the Terraform-managed `deployment_access`
gateway. Its fixed public allocation, subnet, boot disk, SSH operator key, and
resource-scoped instance service account are part of the same remote state as
the cluster. The VM identity is obtained from instance metadata, so normal
deployment does not depend on a human browser session, copied access token, or
expiring authorized key. Nebius does not support an access permit scoped to a
Managed Kubernetes cluster, so the identity uses the minimum supported project
`editor` permit plus an explicit registry permit; it is not a task worker and
cannot receive Loom trials.

The gateway stays running so the private control plane remains recoverably
reachable. User execution still scales independently from zero to four and back
to zero. Do not substitute a date-named VM, temporary public endpoint, unrelated
user VM, or ad hoc tunnel. For an approved restricted public endpoint, use
`--external`. Always write a private mode-0600 kubeconfig outside the repository:

```bash
export LOOM_NB_CLUSTER=mk8scluster-REPLACE
export LOOM_NB_KUBECONFIG=/secure/path/$LOOM_NB_TARGET.kubeconfig
nebius mk8s cluster get-credentials --profile "$LOOM_NB_PROFILE" --id "$LOOM_NB_CLUSTER" --internal --kubeconfig "$LOOM_NB_KUBECONFIG"
chmod 600 "$LOOM_NB_KUBECONFIG"
kubectl --kubeconfig "$LOOM_NB_KUBECONFIG" cluster-info
kubectl --kubeconfig "$LOOM_NB_KUBECONFIG" get nodes -o wide
kubectl --kubeconfig "$LOOM_NB_KUBECONFIG" get pods -A
```

### Authorized gateway bootstrap repair

The deployment gateway's cloud-init grants the configured SSH-key operator
`codex` passwordless sudo and installs `sudo`/`wireguard-tools`. Password SSH
and direct root SSH remain disabled. This is the dedicated infrastructure
gateway, not an execution Pod: the operator needs a legitimate guest-admin path
to install and recover its authorized private connection. Package installation
does not configure a peer, start WireGuard, alter routes or expose database ports.

Older gateways created with `sudo: false` cannot acquire guest administration
merely by changing a Terraform file. Replacing one requires explicit owner
authorization for that exact gateway and a deployment-access interruption:

1. Save the remote state, exact instance/boot-disk identity, dedicated known-hosts
   entry, tool versions/hashes and necessary operator-owned configuration in a
   mode-0700 evidence directory. Recovery archives can contain credentials;
   keep them mode 0600, outside Git, and never print their contents.
2. Run a **full** saved plan with
   `-replace=module.execution_target.nebius_compute_v1_instance.deployment_access`.
   Require this to be the only non-no-op resource change. Do not use `-target`
   to hide unrelated changes. The nested managed boot disk belongs to this VM;
   compare its provider ID separately. Cluster, node groups, data disks,
   storage, registry, IAM and the fixed public allocation must not change.
3. Confirm the before/after allocation ID, instance service account, shape,
   source image, boot-disk specification and absence of secondary disks agree.
   Apply only that reviewed saved plan. Do not enable create-before-destroy
   against an allocation already held by the old gateway.
4. Read the new instance identity and fixed public allocation from the
   authenticated provider API. Verify the new SSH host-key fingerprint against
   that exact instance's authenticated provider serial logs before replacing
   only the dedicated known-hosts entry. A keyscan alone is not this verification.
5. Verify cloud-init, `sudo -n true`, `wg --version`, required CLI tools and
   instance-identity access to the private Kubernetes API. Restore only required
   operator configuration, never a stale kubeconfig as a substitute for renewed
   service-identity credentials. Private IPs may change; re-read them before
   preparing the private-link binding. Finish with a full no-change plan.

This repair does not deploy the staging attachment, migrate historical data,
change execution capacity or bypass the protected staging backup/rollout lane.
The former gateway and its managed boot disk are replaced, so retain the scoped
recovery archive until post-rebuild acceptance is complete. Private-link
activation and its service-level checks remain the next step.

## Live smoke order

Run live acceptance in this order; stop and clean up at the first failed gate.

1. Apply only `development-eu-north1` with the committed execution bounds, the
   pinned `16vcpu-64gb` shape, and 64 Pods per node. The current-inventory
   profile is `0..8` and requires readback of at least 200 non-GPU vCPUs and 12
   VM slots, with sufficient remaining headroom after shared-account usage.
   Reconcile any later envelope to the fresh quota-derived target through its
   reviewed plan; do not wait for or activate the historical 200-task proposal.
2. Prove Terraform convergence and cloud-side Ready/readback.
3. Create only the development binding namespace with its canonical topology
   labels and install only its environment-local identities/policies.
4. Run a system-node connectivity pod and record node identity, DNS, HTTPS
   egress, logs, exit code, and deletion.
5. Through the normal user API, upload an ordinary supported TaskSet that has
   no hand-written `service_execution` field, create a `backend=nebius` Batch,
   and record its automatically frozen input manifest, runtime plan, target,
   images, Trial and Job identity.
6. From execution minimum zero, observe that Batch's Job move Pending to
   Running to Succeeded; verify model/verifier output and immutable artifact
   readback; then verify Job cleanup and execution-node scale-down to zero.
   A manually created Pod or manually patched Task binding is not evidence for
   this gate.
7. Establish the current-quota maximum as described above, then run bounded
   increasing true-overlap stages ending at that target. The checked
   `accepted_concurrency=56` policy currently produces stages 1, 20, 40, 56;
   passing these proves that envelope, not that 56 is the account maximum.
   At every stage prove the persisted concurrency seats, simultaneous running
   Jobs/Pods, node-backed capacity, successful
   results, artifact digests, released seats, no orphan Jobs, and return to zero
   execution nodes. Merely submitting a large queued batch does not pass. Stop,
   diagnose, and clean up before advancing after any failed stage.
8. Create the separately reviewed staging/production bindings without traffic,
   then attempt cross-environment namespace, bucket-prefix, and credential
   access; acceptance requires denial.
9. Only after development passes, repeat the same gates in staging. Production
   work and traffic require separate approval. A second cluster or region is
   outside baseline scope.

A generic Linux Pod proves Kubernetes scheduling and network plumbing only. It
does not prove the Loom scheduler, worker, model, verifier, or artifact path.
Use the managed cluster's default container runtime with the repository's
restricted, non-root Pod settings. A custom sandbox RuntimeClass is optional
defense in depth, not an admission requirement for this project.

## Persistent development runtime

Runtime YAML is generated from the checked execution topology and physical
binding rather than copied and edited per environment. Render into a new or
empty directory and retain `render-manifest.json` with release evidence:

```bash
uv run --no-sync python scripts/ops/render_nebius_runtime.py \
  --environment development \
  --image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST \
  --capacity-policy deploy/k8s/nebius-development-capacity-policy.json \
  --output /secure/path/nebius-development-render
```

The renderer resolves the target and namespace from
`config/service-execution-topology.json`, resolves the shared provider IDs from
`config/nebius-runtime-physical-binding.json`, injects only a digest-pinned
image, rejects a cross-environment capacity policy, and writes a checksum
ledger. Staging and production use the same command with their environment and
their separately reviewed target-bound capacity policy. Rendering does not
deploy or enable traffic.

After the platform Deployment and its database/admin Secrets exist, attach the
development execution runtime once:

```bash
scripts/ops/apply_nebius_development_runtime.sh \
  --kubeconfig /secure/path/development-eu-north1.kubeconfig \
  --nebius-credentials /secure/path/capacity-observer-credentials.json \
  --service-execution-runtime-profile /secure/path/service-execution-runtime-profile.json \
  --execution-actuator-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST
```

From outside the Nebius VPC, run the same convergence through the managed
gateway. Pin the gateway host key once after Terraform creates or replaces the
VM, verify the fingerprint against the apply evidence, and retain that
`known_hosts` file with the operator SSH key:

```bash
scripts/ops/mirror_nebius_release_via_gateway.py \
  --gateway GATEWAY_IP \
  --ssh-key /secure/path/deployment-access-ed25519 \
  --known-hosts /secure/path/deployment-access-known-hosts \
  --candidate-sha MERGED_DEV_SHA \
  --target-registry cr.eu-north1.nebius.cloud/REGISTRY \
  --gateway-image ghcr.io/qianyi-sun/loom-llm-gateway@sha256:DIGEST \
  --control-plane-image ghcr.io/qianyi-sun/loom-control-plane@sha256:DIGEST \
  --service-image ghcr.io/qianyi-sun/loom-service@sha256:DIGEST \
  --execution-actuator-image ghcr.io/qianyi-sun/loom-execution-actuator@sha256:DIGEST \
  --execution-runtime-image ghcr.io/qianyi-sun/loom-execution-runtime@sha256:DIGEST \
  --output /secure/path/nebius-release-mirror.json

scripts/ops/collect_nebius_runtime_evidence_via_gateway.py \
  --gateway GATEWAY_IP \
  --ssh-key /secure/path/deployment-access-ed25519 \
  --known-hosts /secure/path/deployment-access-known-hosts \
  --service-image cr.eu-north1.nebius.cloud/REGISTRY/loom-service@sha256:DIGEST \
  --execution-runtime-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-runtime@sha256:DIGEST \
  --output-dir /secure/path/runtime-admission-evidence

uv run --no-sync python scripts/ops/prepare_nebius_runtime_profile.py \
  --candidate-sha MERGED_DEV_SHA \
  --mirror-record /secure/path/nebius-release-mirror.json \
  --evidence-summary /secure/path/runtime-admission-evidence/summary.json \
  --service-release-record /secure/path/service-amd64.json \
  --execution-runtime-release-record /secure/path/execution-runtime-amd64.json \
  --signing-key /secure/path/image-admission-signing-key.pem \
  --signing-key-id nebius-development-YYYY-MM \
  --output-profile /secure/path/service-execution-runtime-profile.json \
  --output-keyring /secure/path/image-admission-keyring.json \
  --output-policy /secure/path/image-admission-policy.json

scripts/ops/apply_nebius_development_runtime_via_gateway.sh \
  --gateway "$(terraform -chdir=deploy/terraform/nebius/stack output -json deployment_access | jq -r .public_address | cut -d/ -f1)" \
  --ssh-key /secure/path/deployment-access-ed25519 \
  --known-hosts /secure/path/deployment-access-known-hosts \
  --cluster-id mk8scluster-REPLACE \
  --nebius-credentials /secure/path/capacity-observer-credentials.json \
  --image-admission-keyring /secure/path/image-admission-keyring.json \
  --gateway-image cr.eu-north1.nebius.cloud/REGISTRY/loom-llm-gateway@sha256:DIGEST \
  --control-plane-image cr.eu-north1.nebius.cloud/REGISTRY/loom-control-plane@sha256:DIGEST \
  --service-image cr.eu-north1.nebius.cloud/REGISTRY/loom-service@sha256:DIGEST \
  --execution-actuator-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST \
  --execution-runtime-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-runtime@sha256:DIGEST \
  --service-execution-runtime-profile /secure/path/service-execution-runtime-profile.json
```

The mirror helper accepts only the five expected digest-pinned amd64 release
images. It downloads a checksum-pinned `crane` binary into a mode-0700 remote
temporary directory, obtains destination credentials from the gateway VM's
attached Nebius service account, copies each image under a release-specific
tag, verifies that the destination digest is unchanged, and writes an
owner-only local result. It needs no human Nebius session, registry password,
sudo access, or persistent credential-helper installation. Repeating it for
the same merged candidate is idempotent.

Every runtime apply also converges the repository-owned execution-capacity and
execution-admission policies. For the current provider quota, both the global
and `nebius-cpu` admission ceilings are 56 leases; this must match the 56-job
pending/create limits and the eight-node execution envelope rather than retain a
bootstrap single-trial ceiling.
It also explicitly enables the control-plane canonical materializer with its
checked polling, claim TTL, concurrency, and source-retention settings. A
restart reclaims expired materialization leases from PostgreSQL; no operator
must reconnect a completed Batch or copy artifacts manually.

The canonical materializer also needs the namespace-local control-plane to
MinIO TCP 9000 path: both control-plane egress and MinIO ingress must admit it.
The repository NetworkPolicy and CLI render include this connection. If compute
and output commit succeed but materialization retries with an S3 connection
timeout, compare a control-plane MinIO health probe with the service's probe and
check both policy directions. Reconcile the corrected repository policy, then
let the existing materialization outbox retry; do not rerun the model, rewrite
Trial state, or remove the retained source bundle to recover the transfer.

Uploaded TaskSets retain the source `task.toml` identity while registering the
Task under a team/TaskSet-qualified catalog ID. Canonical trajectory events and
ATIF use the Trial's referenced catalog Task ID; comparing it directly with the
embedded source ID is invalid. A historical `task_identity_drift` failure must
not be repaired by rewriting Task or Trial rows. Deploy the corrected
materializer and verify a fresh ordinary-user submission; keep the original
failed Trial and committed source evidence for diagnosis. This identity mapping
does not relax output manifest, producer, runtime-result, or digest validation.

The evidence collector uses checksum-pinned Trivy and `crane` binaries on the
gateway, authenticates to Nebius Registry with the VM service account, rejects
CRITICAL findings, and atomically retains owner-only CycloneDX SBOMs, complete
vulnerability reports, and the exact execution-runtime binary digest. The
profile helper binds those results to the mirrored images and protected amd64
release records, then signs the immutable profile with a persistent Ed25519
key. Add `--create-signing-key` only for the first bootstrap; later releases
reuse the same owner-only key. Admission metadata does not expire at runtime;
removing or replacing the public key is the explicit revocation mechanism. The
execution-runtime release must exactly match the profile candidate. An
unchanged service image may retain a protected `dev` release record only when
that release is a Git ancestor of the profile candidate, so path-scoped trusted
publication remains deployable without fabricating a new service release.

The helper accepts only digest-pinned platform images, transfers only the
reviewed runtime manifests plus the capacity observer credential into a
mode-0700 temporary directory, obtains an internal kubeconfig with the VM's
attached identity, rolls out Control Plane then Service, applies the idempotent
runtime, and deletes the remote and local staging directories. No human Nebius
token is copied to the gateway.

## Canonical destination and staging attachment

The owner-selected final canonical destination for #1765 is the existing live
staging Loom database and object store. The development helper above is not a
staging attachment or a data migration tool: its Control Plane, Service,
Gateway and database assumptions remain Nebius-local. Keep development
history intact and distinguish new staging work from any separately approved
historical-data migration.

Independent source-store wiring and both sides' offline rendering are now
implemented; they are not evidence of a live staging deployment. The staging
Control Plane reads the Nebius spool and writes the existing canonical MinIO;
the Nebius-local Gateway reads TaskSet inputs from canonical storage and commits
execution outputs to the spool. Only staging PostgreSQL owns these new Trials,
outbox claims and Artifact metadata. Keeping all source settings unset preserves
the existing same-store development behavior; partial source configuration fails
startup instead of silently using canonical credentials.

Keep the Pod-facing Gateway inside Nebius. Its peer-IP/lease check requires the
direct observed Pod address, not the NAT address of the public staging ingress.
The Gateway and actuator use separately provisioned staging database role
references. Gateway provider connections remain in staging's existing encrypted
secret store; this attachment does not introduce a copied user/provider login.

### Canonical staging side: opt-in protected render

#### Persistent host transport prerequisite

`scripts/ops/nebius_private_link.py` owns only `wg-loom-nb` on the existing
deployment-access gateway and the existing staging host. It does not deploy the
attachment, expose PostgreSQL, route cluster CIDRs, or change default routes.
Install the reviewed script through the authorized root installation path;
never use a developer-owned startup unit to bypass protected staging rollout.

On each host, run `sudo python3 nebius_private_link.py prepare`. This creates
or reuses a root-only local WireGuard identity and emits **only its public key**.
Exchange these public keys; never copy the private keys between hosts. Select
two non-overlapping RFC1918 `/32` transport addresses after checking both hosts'
routes, both clusters' address pools, and the dedicated UDP listener port.

On the public Nebius gateway, configure the peer without an endpoint:

```bash
sudo python3 nebius_private_link.py configure \
  --address "$GATEWAY_TRANSPORT_ADDRESS" \
  --peer-address "$STAGING_TRANSPORT_ADDRESS" \
  --peer-public-key "$STAGING_WIREGUARD_PUBLIC_KEY"
```

On staging, use the gateway's Terraform-owned fixed public allocation:

```bash
sudo python3 nebius_private_link.py configure \
  --address "$STAGING_TRANSPORT_ADDRESS" \
  --peer-address "$GATEWAY_TRANSPORT_ADDRESS" \
  --peer-public-key "$GATEWAY_WIREGUARD_PUBLIC_KEY" \
  --endpoint "$GATEWAY_FIXED_PUBLIC_IPV4:51871"
```

The standard `wg-quick@wg-loom-nb` system service is enabled at boot. Only the
peer `/32` is allowed; staging sends a 25-second keepalive to preserve its NAT
mapping. No short-lived browser session or copied user kubeconfig is involved.
These are WireGuard's standard [keepalive](https://www.wireguard.com/quickstart/)
and [systemd service](https://git.zx2c4.com/wireguard-tools/tree/src/systemd/wg-quick%40.service)
mechanisms, not a new tunnel supervisor. Reapplying identical configuration
does not restart the interface or rotate its identity. Failed changed-config
activation restores the prior managed configuration and unit enable/active state.
An unmanaged configuration or active interface is never adopted. A per-interface
lock serializes key preparation and reconciliation.

Before acceptance, verify a fresh handshake, a peer-only route, bidirectional
reachability, and recovery after restarting **only this dedicated unit** on each
host. Do not reboot a shared staging host for this test. Keep private keys and
configuration in root-only recovery storage; a gateway replacement requires
restoring its identity or reconciling its new public key on the peer. To disable
only this transport, use `sudo systemctl disable --now wg-quick@wg-loom-nb`;
retain the configuration/key for recovery and do not flush firewall tables.

Host transport is not application connectivity. Before attachment activation,
separately install narrowly bound service proxies/routes and validate the actual
DB, canonical store, Control Plane and model endpoints from the Nebius Pod
network. Their TLS identity, renewal and network-policy source addresses must
match the real path. Do not expose a service on a public wildcard listener,
disable certificate verification, assume PgBouncer speaks TLS, or claim that
a WireGuard handshake proves canonical persistence. The Pod-facing Loom Gateway
must remain in Nebius to preserve direct peer-IP/lease verification.

#### Private service entry (opt-in, separate from execution activation)

The canonical cluster renderer also supports a single private entry Pod on the
existing staging WireGuard peer node. It uses the same hostPort mechanism as the
existing worker/MinIO routers, but **every hostPort has an explicit WireGuard
hostIP**. It creates no Ingress, NodePort, LoadBalancer or new database. The
checked-in profile remains disabled; the table below is an operator example,
not an approved live binding:

```toml
[nebius_private_entry]
enabled = true
node_name = "staging-control-1"
wireguard_address = "10.253.71.2"
peer_address = "10.253.71.1"
proxy_image = "nginx:1.28-alpine@sha256:a8b39bd9cf0f83869a2162827a0caf6137ddf759d50a171451b335cecc87d236"
```

Select the real existing peer node and non-conflicting transport addresses in
the authorized candidate configuration. Both addresses must be distinct
RFC1918 IPv4 hosts; public, wildcard and reserved documentation ranges fail
render. This table is restricted to multi-node `loom-staging`, and may be
enabled **before** `nebius_execution` so connectivity can be validated before
provisioning execution credentials or enabling scheduling.

| WireGuard-bound TCP port | Destination | Transport |
| --- | --- | --- |
| 15432 | `loom-postgres-rw:5432` | Native PostgreSQL SSLRequest/TLS passthrough |
| 18443 | `loom-control-plane:8080` | TLS termination, unchanged HTTP bytes |
| 19443 | `loom-minio:9000` | TLS termination, unchanged S3 Host/URI/body/signature |

HTTPS clients must use a name covered by the existing
`ingress_tls_secret_name` certificate (the canonical profile's
`ingress_host`), with separately persisted private resolution to the Nebius
gateway service entry. Do not use an IP URL with a DNS certificate, publish a
private-service public DNS route, rewrite MinIO under a URL prefix, or add
these backends to the existing public ingress.

The Pod mounts the **existing cert-manager Secret directory**, without
`subPath` or a one-time host copy. Every ten seconds the main process checks
the projected certificate/key pair, validates nginx configuration, then reloads
nginx on changes. Invalid pairs leave the previous active certificate serving
and are retried; a failed reload validation emits a secret-free warning.
Kubelet projection adds its own delay before this interval starts. Certificate
renewal still belongs to the existing cert-manager Certificate/issuer, not to
this proxy. nginx upstream DNS refresh uses its
[dynamic upstream resolution](https://nginx.org/en/docs/stream/ngx_stream_upstream_module.html#server);
the image must provide nginx >=1.27.3 with stream and stream SSL support.

The entry follows the `loom-postgres-rw` Service, not a primary Pod IP or
PgBouncer's non-TLS listener. Native TLS passthrough does not replace database
certificate trust: approve the application's SSL mode and provide a persistent
CA/trust path when certificate verification requires one. Verify the endpoint,
role and `SELECT current_database()` before binding credentials; the accepted
live staging database is `loom`, not a newly provisioned database.

Configuration/script changes stamp a Pod-template revision and recreate this
one replica (brief planned entry interruption, no surge hostPort collision).
Kubernetes supervises the Pod; nginx exit ends its main process. New policies
allow only the WireGuard peer /32 to these ports and the proxy to canonical CP,
MinIO, CNPG and cluster DNS, with matching destination ingress allowances.
Verify actual CNI source handling after deployment; do not broaden the policy
because a host-only probe fails.

Pre-merge reproducible transport verification:

Requires Docker and locally cached copies of the pinned nginx image above and
`python:3.12-slim`. The script never pulls images or publishes host ports. Its
temporary Linux volume reproduces kubelet's atomic Secret-directory projection;
Docker Desktop host-bind symlink behavior is not used as a renewal test.

```bash
PYTHONPATH=src:.:packages/loom-bundle-checksum \
  python scripts/ops/smoke_nebius_private_entry.py
```

This uses disposable local Docker fixtures, not staging data or credentials.
It is not Nebius Pod/CNI, database role, S3 authorization or full task acceptance.
Protected activation must separately establish the host tunnel, stable
Nebius-side private entry address/forwarding, private DNS and client trust,
then test the three endpoints from the real Nebius Pod network. It must also
verify recovery after restarting only the dedicated proxy/WireGuard services,
and confirm that the public ingress and unrelated workloads are unchanged.
Do not resume another initiator's staging rollout to install this table.

Rollback through the supported candidate rollout by disabling this table and
removing **only** `Deployment/loom-nebius-private-entry`, its same-name
ConfigMap/NetworkPolicy and the three `loom-private-entry-to-*` policies.
Preserve the shared TLS Secret, canonical databases/buckets, other routers and
WireGuard identity. Verify resource removal rather than assuming an omitted
rendered document is automatically pruned.

#### Nebius gateway: reserved service address and persistent TCP forwarding

The Terraform gateway output now separately exposes
`private_service_allocation` and `private_service_cidr`. Use that reserved
`/32`, **not** the existing dynamic `private_address`, for Pod-facing service
routes and policy allowlists. The new allocation attaches using the
[mutable secondary alias](https://docs.nebius.com/compute/virtual-machines/network#how-to-assign-a-secondary-private-ip-address-to-a-vm);
it does not change the replacement-only primary IP or the fixed public
WireGuard/SSH allocation.

Before applying an authorized saved plan, require exactly one new private
allocation plus an in-place gateway update; reject unexpected replacements,
disk changes or unrelated resources. Fresh-stack ordering lets Kubernetes
reserve its Service CIDR first, and an allocation postcondition rejects addresses
outside the node network. A failed postcondition can leave a newly reserved
allocation in state; investigate and recover only that resource. After apply,
read back allocation/instance/subnet identity and the actual `/32` before
configuring a guest listener. A validated input IP is not ownership proof.

`scripts/ops/nebius_gateway_proxy.py` installs seven dedicated units through
the authorized root path: one address unit and three socket/service pairs.
It configures the reserved address on loopback with the `lo:loom-nb` label,
without editing DHCP, netplan or default routes. Native
[systemd-socket-proxyd](https://www.freedesktop.org/software/systemd/man/latest/systemd-socket-proxyd.html)
forwards TCP bytes from only that private IP's `15432/18443/19443` to the
same ports on the sole staging WireGuard peer. No extra proxy daemon dependency,
public listener, NAT/firewall rule or full-cluster route is introduced.

```bash
sudo python3 nebius_gateway_proxy.py install \
  --listen-address "$GATEWAY_RESERVED_PRIVATE_IPV4" \
  --peer-address "$STAGING_WIREGUARD_IPV4"
```

The existing `wg-loom-nb` peer and peer-only route must already be configured.
Installation rejects addresses assigned outside its managed loopback label,
unmanaged units/drop-ins and unsafe files. A local lock covers preflight and
reconciliation. The same configuration does not restart listeners; failed
updates restore the prior managed files and active/enabled state. Installer
success reports ports/status, not addresses or credentials. The offline
`render` action emits unit contents for private review; do not publish its
actual address-bearing output in logs or issue comments.

Sockets are pulled at boot and by the WireGuard service, with restart/stop
propagation. Explicit socket dependency ordering avoids a cycle with the
default early `sockets.target`. Validate actual PID 1 behavior after protected
installation: restart only `wg-quick@wg-loom-nb.service`, then confirm the
address unit, three sockets, new connections and canonical endpoints recover.
Unit parsing and a local socket-activation fixture are not that live proof.
Reinstall from the approved persisted binding when replacing the gateway;
retain the allocation and restore/reconcile the WireGuard identity separately.

To disable only these forwards, disable/stop
`loom-nebius-forward-15432.socket`, `loom-nebius-forward-18443.socket` and
`loom-nebius-forward-19443.socket`, then stop
`loom-nebius-private-address.service`. Paired forwarder services stop with their
sockets. Keep unit files and the reserved allocation for recovery. Do not remove
the primary address, stop the gateway VM, flush firewall rules or affect other
resources as part of this rollback.

#### Canonical render configuration

The former shared staging profiles have been retired. The generic renderer
retains this disabled-by-default attachment contract for disposable compatibility
validation; synthetic inputs live under `tests/fixtures/cluster-render/`.
Hosted deployments use the [Nebius deployment procedure](nebius-deployment.md).
The following table documents the compatibility contract, not a hosted rollout
input. Its revision and documentation-only addresses are examples:

```toml
[nebius_execution]
enabled = true
source_secret_name = "loom-nebius-staging-spool"
runtime_profile_secret_name = "loom-nebius-staging-runtime"
image_admission_secret_name = "loom-nebius-staging-admission"
configuration_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
source_egress_allowlist = ["192.0.2.3:9443"]
execution_ingress_cidrs = ["192.0.2.16/32"]
```

All fields are required when enabled. Existing namespace-local Secrets must
contain these exact keys; rendering neither reads their values nor creates them:

| Profile reference | Required keys | Consumer |
| --- | --- | --- |
| `source_secret_name` | `endpoint`, `region`, `bucket`, `access-key`, `secret-key` | Control Plane `LOOM_CP_SERVICE_EXECUTION_SOURCE_*` |
| `runtime_profile_secret_name` | `runtime-profile-json` | Service's signed automatic Nebius task profile |
| `image_admission_secret_name` | `public-keys-json` | Control Plane's image-admission keyring |

`configuration_revision` is a 64-character lowercase SHA-256 deployment revision
stamped on both Control Plane and Service Pod templates. Advance it through the
same supported deployment path whenever the referenced configuration or
Secret-backed values change, so restarted processes consume the new values.
Mounted source credential files are also supported by application settings, but
are read at startup, not a promise of hot credential reload.

The opt-in enables the `staging`/`nebius-cpu` scheduler and materializer without
changing canonical DB/storage settings or Docker backend selection. The renderer
adds Control Plane egress only to `source_egress_allowlist` IP/CIDR-and-port
destinations, and ingress from `execution_ingress_cidrs` only to the selected
Control Plane, MinIO, PostgreSQL/CNPG and PgBouncer workload ports. These policies
do not create a WAN route, TLS endpoint or database role. Actuator/runtime images
remain in the release image plan, but Nebius workloads are not deployed as
standing workloads inside the staging cluster.

### Nebius side: explicit staging attachment

The following is a minimal complete `staging-attachment.json` render example.
Every domain/IP/image digest is illustrative. Replace them with approved
durable service routes and released image digests; Secret values must never be
put in this file. Secret names and key names here are configurable references
in `loom-nebius-staging`, not claims that those objects already exist.

```json
{
  "schema_version": "loom.nebius-staging-attachment.v1",
  "environment": "staging",
  "target_id": "nebius-eu-north1-staging",
  "namespace": "loom-nebius-staging",
  "canonical_database": "loom",
  "configuration_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "local_providers_secret_name": "staging-local-providers",
  "gateway_image": "registry.example/gateway@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "canonical": {
    "endpoint": "https://canonical.example:9443",
    "region": "us-east-1",
    "artifacts_bucket": "loom-staging-artifacts",
    "trajectories_bucket": "loom-staging-trajectories",
    "db_secret": {"name": "staging-db", "gateway_key": "gw-url", "actuator_key": "actuator-url"},
    "storage_secret": {"name": "canonical-storage", "access_key": "access", "secret_key": "secret"}
  },
  "source": {
    "endpoint": "https://spool.example:9443",
    "region": "eu-north1",
    "bucket": "loom-staging-spool",
    "credentials_secret": {"name": "spool-storage", "access_key": "access", "secret_key": "secret"}
  },
  "gateway_secret": {"name": "staging-gateway", "step_jwt_key": "signing", "master_key": "master"},
  "collector": {
    "control_plane_url": "https://cp.example:8443",
    "token_secret": {"name": "staging-collector", "key": "token"},
    "nebius_secret": {"name": "nebius-observer", "key": "credentials.json"}
  },
  "network": {
    "database": [{"cidr": "192.0.2.1/32", "port": 5432}],
    "canonical_store": [{"cidr": "192.0.2.2/32", "port": 9443}],
    "source_store": [{"cidr": "192.0.2.3/32", "port": 9443}],
    "control_plane": [{"cidr": "192.0.2.4/32", "port": 8443}],
    "kubernetes_api": [{"cidr": "192.0.2.5/32", "port": 443}],
    "kubelet": [{"cidr": "10.40.0.0/24", "port": 10250}],
    "provider_api": [{"cidr": "192.0.2.6/32", "port": 443}],
    "model_api": [{"cidr": "192.0.2.7/32", "port": 443}]
  }
}
```

The required `network.kubelet` destinations must cover the approved private
worker-node subnet and the actuator's hosting Node, including when workers are at
zero. Use the actual cluster's RFC1918 IPv4 or ULA IPv6 CIDRs, not the illustrative
subnet above. Only TCP 10250 is accepted, and only the actuator receives this
egress. Existing attachment inputs need this field before upgrading to the direct
telemetry reader; there is no broad default route or node-proxy fallback. Rendering
validates the route; protected installed preflight separately probes CNI
reachability, kubelet TLS and ServiceAccount authorization. Known direct-kubelet
sampling failures produce unavailable-telemetry warnings. API access, Node/runtime
identity and capacity checks remain mandatory; missing samples never imply zero usage.

Render into a new directory with a separately reviewed staging capacity policy:

```bash
uv run --no-sync python scripts/ops/render_nebius_runtime.py \
  --environment staging \
  --image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST \
  --capacity-policy /secure/path/nebius-staging-capacity-policy.json \
  --staging-attachment /secure/path/staging-attachment.json \
  --output /secure/path/nebius-staging-render
```

Attachment mode renders only the Nebius-local Gateway, target-bound actuator and
collector, scoped network policies and capacity/evidence files. It emits no
Control Plane/Service patches, database, new canonical object store or Secret
payloads. Collector cluster-role names are staging-specific to avoid replacing
the existing development collector binding. Credential-bearing URLs, cross-env
bindings, unpinned Gateway images, incomplete Secret references and unscoped routes fail
before output is written. JSON Secret references are shape-checked only; actual
Secret existence/content and network DNS resolution remain deployment checks.

For the reserved gateway entry above, the v1 attachment accepts an optional
non-secret declaration:

```json
"private_entry": {
  "hostname": "staging.example",
  "address": "10.20.30.40"
}
```

Replace both example values with the approved TLS hostname and Terraform
`private_service_cidr` address (without `/32`). Set the canonical store origin
to `https://staging.example:19443` and collector origin to
`https://staging.example:18443`. The hostname must match both origins and the
actual existing certificate. The independent spool must use a different
hostname: a Pod hosts entry applies to all ports of a name.

The renderer persists Kubernetes-native `hostAliases` on all three consumers:
Gateway, actuator and collector. No hand-edited Pod hosts file, global cluster
DNS override or changed TLS URL is required; recreated Pods inherit the mapping.
It requires address-and-port policy permits for database `15432`, canonical
store `19443` and Control Plane `18443`. Keep both DB URLs in Secret references,
and verify separately that their DSNs use this same hostname on `15432` with
the approved database/TLS settings; the renderer never reads or rewrites them.
Omitting `private_entry` preserves the previous rendered files byte-for-byte.
This mapping is desired configuration, not proof that the reserved alias,
forwarders, certificate trust or credentials have been installed.

Use the same `configuration_revision` on both sides of the attachment. The
Nebius renderer stamps it on Gateway/actuator Pod templates and collector Job
and Pod templates. Rotating values under an unchanged Secret name requires a
new revision and supported reconciliation: this rolls the long-lived consumers
and makes subsequent collector Jobs consume the replacement values. Hashing
only the attachment JSON cannot detect a same-name Secret value change, and
updating a CronJob template does not restart a Job already in progress.

`local_providers_secret_name` is a required, dedicated Nebius-namespace Secret
containing only the existing `LOOM_GW_LOCAL_<NAME>_BASE_URL` and
`LOOM_GW_LOCAL_<NAME>_API_KEY` entries needed for staging's operator-configured
models such as `local/yibu/...`. Use an empty dedicated Secret when no local
models are configured. Verify this key allowlist before deployment: the
renderer reads no Secret payload. The reference is loaded through `envFrom`;
DB, canonical/source storage and identity bindings stay explicit `env` entries,
which take precedence over `envFrom`. No new JSON provider parser or raw API key
in the attachment file is required. Provider configuration/value changes also
advance the shared deployment revision. Gateway rollout preserves the existing
300-second termination grace and local `/drain` pre-stop hook for long LLM calls.

### Activation and acceptance boundary

The staging-specific [persistent credential workflow](nebius-staging-credentials.md)
implements seed-before-registration, CNPG-managed service roles, dedicated
source/input storage identities, renewable Kubernetes authentication and
recurring PostgreSQL CA/credential synchronization. Use that workflow instead
of the development apply helper. Its native `database_tls` binding is distinct
from the HTTPS private-entry certificate; activation remains separately gated.

Before any activation, read the current protected broker status and coordinate
its request owner. Resolve the current reported failure through its owning lane
(including backup or final-attestation failures); do not assume the historical
#1807 backup failure is still the current blocker. Do not skip backup, resume another initiator's request, or
substitute the development gateway helper for protected staging deployment.
This render work does not transfer rollout ownership or authorize production.

The existing shared staging database is named `loom`; `loom_staging` remains an
accepted declaration for installations already using that name. Set
`canonical_database` to the actual approved database name, not an environment
label. The renderer records this declaration in its manifest without reading
Secrets, rewriting a DSN, provisioning a database or migrating data. A database
named `loom` also exists in development, so the name alone is not authority proof.

Deployment preflight still must verify:

- Actual referenced actuator/Gateway DSNs reach the approved existing live
  staging PostgreSQL endpoint with the intended roles, TLS and schema/candidate.
  A read-only `SELECT current_database()` must match `canonical_database`
  (`loom` for the existing shared staging), but also verify the endpoint's
  environment identity: a matching name alone cannot distinguish development.
  Never create another database or substitute a label to pass the render check.
  Keep secrets out of evidence.
- Persistent, scoped routes for Nebius actuator/Gateway to staging DB, Gateway
  to canonical input storage and model providers, staging Control Plane to the
  source spool, and collector to its authenticated Control Plane endpoint.
  HTTPS origins, DNS addresses, NAT source CIDRs and policy ports must agree;
  temporary SSH tunnels do not satisfy this prerequisite. Collector HTTPS
  currently requires a system-trusted certificate; no private-CA mount is
  supplied by this attachment.
- Source credentials and bucket agree between staging Control Plane and the
  Nebius Gateway. Keep source and canonical credentials independently scoped;
  collector uses its durable service token and Nebius observer identity, not
  an expiring personal CLI login. Verify authorized service-secret replacement
  and restart/reconciliation procedures before calling access persistent.
- Gateway master/step-signing keys match the staging identities, local model
  routes are configured, and the collector token is issued by staging with
  worker-only `execution:capacity:observe` scope. The current token is single-scope,
  not token-level target-bound; the collector configuration selects the target.
  Development credentials are
  not substitutes; compare identities without exposing secret values.
- Runtime profile/admission keys and both clusters' image digests match the
  reviewed release, the staging target has a quota-backed capacity policy, and
  ordinary-user `backend=nebius` upload, full-bundle transfer/download and
  acknowledgement-gated retention GC pass after execution returns to zero.

Existing development rendering and historical results remain unchanged. This
attachment starts new staging work; it neither copies old databases nor imports
historical development Trials. Such a migration, if requested, needs its own
explicit scope and reconciliation evidence.

## Automated staged acceptance

After the exact protected candidate is deployed and the execution pool has
returned to zero nodes, build its one-task acceptance TaskSet directly from the
deployed runtime profile. This keeps task image and candidate identity coupled
and declares two required task artifacts plus trajectory, usage, and verifier
output:

```bash
uv run --no-sync python scripts/ops/build_nebius_acceptance_taskset.py \
  --runtime-profile /secure/path/service-execution-runtime-profile.json \
  --output /secure/path/nebius-acceptance-taskset

loom eval nebius-acceptance \
  --taskset-dir /secure/path/nebius-acceptance-taskset \
  --provider az-gateway-loom-testing \
  --model MODEL_ID \
  --candidate-sha MERGED_DEV_SHA \
  --capacity-policy deploy/k8s/nebius-development-capacity-policy.json \
  --output /secure/path/nebius-acceptance-MERGED_DEV_SHA
```

The command uses the persisted `loom auth login` session and only normal user
APIs. Model-backed submissions require an explicit Provider Connection
(#2054), so the run resolves `--provider` (default `az-gateway-loom-testing`)
to this environment's connection id; `--model` must be a model that connection
serves. It needs only the user's `read:own,submit` scopes and an existing
connection owned by or shared with the team; it does not create a Provider
Connection or require `providers:manage`, and it does not copy provider
credentials into the acceptance bundle.

It uploads and waits for the ordinary TaskSet, then runs bounded stages ending
at `accepted_concurrency` (currently `1,20,40,56`, deduplicated). There is no
fixed 200-task endpoint. Explicit repeated `--stage` values permit a smaller
canary, but cannot prove the policy ceiling unless that ceiling actually runs.
Stages above 100 use multiple legal sampling combinations. Monitor evidence
comes from `service_execution.targets` and batch-scoped observed lease states,
not `resources.pools` legacy workers or the top-level Trial `claimed` count.
Each stage must observe the requested number of simultaneously running
execution units backed by Ready nodes, all canonical successes, complete
bundle downloads, and a return to zero execution nodes and drained compute
demand before the next stage. The acceptance verifier intentionally holds each Pod for
three minutes so scale-from-zero timing cannot make a fast model response hide
the real-overlap measurement.

Every Trial's complete authenticated archive is retained under the evidence
directory. The runner verifies the response digest, safe archive inventory,
`bundle.json`, `checksums/SHA256SUMS`, every declared file size and SHA-256,
and the candidate frozen on the Batch. A timeout cancels the acceptance Batch
best-effort so failed validation does not intentionally leave paid work
running. The final `acceptance.json` and checksum sidecar contain sanitized
Batch, lifecycle, node, overlap, and bundle evidence. The canonical archive
places execution payloads below `files/`, including
`files/artifacts/answer.txt` and `files/trajectory/events.jsonl`; required-output
validation uses those archive paths without dropping the full inventory or
digest checks. `progress.json` retains completed stage and current Batch
evidence if later validation fails. No database, MinIO, kubectl, Nebius CLI,
or hand-connected transfer is part of the test path.

`accepted=true` in `acceptance.json` means the requested batch/download/compute
cleanup stages passed. `maximum_proven_concurrency` and
`policy_ceiling_reached` describe only those observed stages;
`quota_maximum_acceptance=not_evaluated` deliberately leaves the fresh
account-quota derivation to the owning #1538 preflight. A configured 56-task
ceiling is not independently a proof of the current account maximum.

Source retention is a separate phase. A canonical-ready Trial can retain its
source for the configured 86400-second recovery window while compute is zero
and canonical downloads work. Immediate acceptance records each source cleanup
state and retain-until timestamp, and reports `source_gc_complete=false` until
GC is actually observed. Do not shorten retention, reset retries or rerun the
model/verifier to make an immediate smoke appear fully garbage-collected.
After the recorded recovery window, perform a read-only continuation into a
new evidence directory:

```bash
loom eval nebius-acceptance \
  --resume-cleanup /secure/path/nebius-acceptance-MERGED_DEV_SHA \
  --output /secure/path/nebius-cleanup-MERGED_DEV_SHA
```

This continuation reads the saved acceptance identities and fresh ordinary
user API state; it never submits or cancels a Batch, changes retention or
deletes source objects. Exit status 2 means cleanup is still pending, not that
GC passed; status 0 requires observed completion and a healthy zero-compute
readback. Preserve the original bundle/evidence and each subsequent report.

Normal convergence preserves the existing `loom-nebius-model-provider`
Secret and hashes its current value into the Gateway rollout annotation
without exposing it. Pass `--model-provider-api-key-file` only for an explicit
provider-key bootstrap or rotation; that file must be owner-only and is removed
from the gateway staging directory after apply.

Likewise, omit `--image-admission-keyring` during ordinary convergence to reuse
the existing public key Secret. Pass it only for the first bootstrap or an
explicit signing-key rotation. Its content digest is stamped onto the Control
Plane pod template so an authorized rotation always rolls the verifier.

The runtime profile is a protected non-secret deployment input whose images
must exactly match the images being rolled out. The helper validates its schema,
pool/class identity and image-admission coverage, persists it as a Kubernetes
Secret before the first Deployment references it, and stamps its content
digest on the Service pod template. Changing the profile therefore causes an
ordinary rolling restart of the accepting API instead of leaving it on stale
configuration. The Control Plane reads the immutable profile snapshot from the
accepted Batch rather than from mutable deployment configuration.

The operation is idempotent. It applies the development-only Control Plane
patch that enables the `nebius-cpu` scheduler and loads its image-admission
keyring from the existing Secret, converges the repository-owned current-56
capacity policy through the authenticated admin API, reuses the existing
single-scope Loom collector token, applies the active actuator and one-minute
collector, and waits for both rollouts plus a scheduled collector Job. The
admin token never leaves the Control Plane Pod. The provider-neutral base
manifest stays disabled. The operation does not create a user Job or change the
node-group target count. Subsequent users only upload/submit through Loom; the
persisted scheduler, lease/outbox, actuator, and managed autoscaler complete the
connection automatically and return execution nodes to zero after demand
drains. The applied bound is currently `0..8`; the historical expansion target
is inactive and is not required for current-quota acceptance. A higher accepted
envelope requires a fresh quota/headroom readback and reviewed plan, not an
edit to an acceptance report.

The recurring capacity observation publishes desired, creating, ready, failed,
and deleting execution-node counts together with its source timestamp. The Loom
Monitor shows those values, current executable slots separately from configured
headroom, canonical transfer backlog/age/retries, last acknowledgement, and
source-cleanup debt. A stale observation blocks new admission rather than
reusing the last displayed node count as executable capacity.

Kubernetes rotates the actuator's projected ServiceAccount token. Nebius
refreshes access tokens from the non-expiring authorized key. Node-group image
pulls use the attached node service account instead of a registry login token.
The Loom batch-runner and capacity-collector identities are non-expiring and
single-scope. Human browser sessions and user API tokens may expire without
interrupting an already persisted Batch. A missing/revoked key or permission
makes the minute collector fail and capacity evidence become stale, which
fails closed before new reservations; it is an incident, not a periodic renewal
procedure.

## Workload placement contract

System components select `loom.nebius/node-role=system`. Attempt pods select
`loom.nebius/node-role=execution` and tolerate exactly:

```yaml
tolerations:
  - key: loom.nebius/execution
    operator: Equal
    value: "true"
    effect: NoSchedule
nodeSelector:
  loom.nebius/node-role: execution
```

Use digest-pinned images from the target registry. Do not grant attempt pods the
node registry service account or Terraform credentials. Add an identity to the
evidence-writers group only through a reviewed target-local bootstrap change.

## Upgrade, scaling, and incidents

- A cluster/node upgrade affects all three bindings. Disable new placement.
  The execution group deliberately uses `max_surge=0` and
  `max_unavailable=1` so execution-node replacement adds no surge demand above
  the accepted quota envelope; record active-workload retry/drain evidence
  before restoring each environment. The fixed system group retains its
  independent surge policy.
- Change only the shared cluster variable file per plan. Quota exhaustion must leave
  work queued and observable; never increase limits or swap to reserved/GPU
  capacity silently.
- On a target incident, set routing health false, stop new placement, preserve
  existing state/evidence, and diagnose before scaling or rebuilding. A healthy
  cluster alone does not restore Loom traffic authority.
- Rotate human and service-account credentials independently. After rotation,
  prove the old credential fails, the new identity has only its expected target
  access, and no kubeconfig or key entered source control or logs.

## Recovery and destruction rehearsal

Remote-state/configuration recovery of the shared cluster remains required. A
second cluster, eu-west1 target, or regional failover is not a baseline recovery
mechanism and must not be created without a separately accepted RTO/RPO,
isolation, compliance, or capacity requirement.

Destruction is a distinct owner-approved operation. First export redacted
outputs, provider-side resource lists, evidence-object hashes, state version,
and the final zero-drift plan. Confirm artifact retention and that no live Loom
route references the target. Review `terraform plan -destroy`, disclose the
residual state bucket, evidence objects, logs, snapshots, public IPs, and egress
charges, then obtain explicit approval for the saved destroy plan.

After destroy, read back the Nebius project: cluster, node groups, shared VPC,
registry, IAM resources, and evidence bucket must match the approved retention
decision. A restore rehearsal starts from the versioned remote state and
reviewed target inputs, applies to the same isolated target, repeats the entire
live-smoke order, and ends with a zero-drift plan. Never delete the state bucket
until its retention and recovery window have expired under separate approval.

## Optional H100 pool configuration

The shared execution target accepts an optional `h100_pool` object inside its
existing `target` input. Omission (`null`) creates no GPU node group. An initial
qualification configuration is:

```json
"h100_pool": {
  "max_nodes": 1,
  "disk_gib": 1100,
  "drivers_preset": "cuda12.8"
}
```

This adds an independent `eu-north1` node group on `gpu-h100-sxm` /
`1gpu-16vcpu-200gb`, with minimum zero, explicit maximum, no replacement surge,
on-demand capacity, the existing subnet/registry-pull identity and model-specific
labels/taints. CPU settings remain independent. Terraform validates the region,
integer 1–32 ceiling, at least 1100 GiB boot disk and supported driver preset.
The ceiling bounds configuration; it is not a claim about current shared quota.
The output explicitly reports `runtime_qualified = false`.

Validate with the existing mocked-provider Terraform tests; validate the stack
wrapper to supply the module's aliased provider configuration:

```bash
terraform -chdir=deploy/terraform/nebius/stack init -backend=false
terraform -chdir=deploy/terraform/nebius/stack validate
terraform -chdir=deploy/terraform/nebius/modules/execution-target test
```

The provider driver preset alone does not qualify the Kubernetes GPU device
plugin, allocatable resources, task-visible CUDA, GPU quota accounting or exact
model scheduling. Those capabilities remain blocked under #2282. In particular,
TB4 `jax-speedrun-gpu` needs 16 allocatable verifier CPUs, 32 GiB RAM, one H100
and 1000 GiB usable disk. A nominal 16-vCPU node loses CPU capacity to system
reservations, and a 1100-GiB boot disk does not prove 1000 GiB allocatable task
storage. Keep this original task blocked until those constraints are satisfied;
do not lower its requirement or substitute H200/CPU.

Do not apply this configuration or scale a pool based on local validation.
Use the authority boundary above for a separately approved one-node no-model
qualification and idle scale-down/provider readback.
