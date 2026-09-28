# Independent Nebius deployment

Nebius is the image publication path for both development and production.
`nebius-candidate` publishes the seven manifest-owned AMD64 images from an exact
`dev` commit. Production promotes those same immutable digest references through
`release-promotion-gate`, then the protected `dev` to `main` pull request and
`main-promotion-gate`. A `main` push does not rebuild or publish images to GHCR.
Use the candidate publication output as the source of image references; never
substitute a mutable branch tag during promotion.

Production still requires separately reviewed environment inputs, release-owner
and Production Environment approval. Run
`scripts/ops/verify_production_release_gate.sh` from the promoted `main` checkout
with the approved candidate, image selector and release-gate run before applying
manifests. Checked-in examples are not production deployment authorization.
`nebius-rollout` automates the independent integration environment only; it does
not turn a `main` merge into an automatic production rollout.

Before applying production, protect each of the seven approved image digests
with the release manifest's unused SemVer `prod_tag` in the same Nebius image
repository. Keep these tags for running production and retained rollback versions;
never move an existing release tag to another digest. Integration retention reads
integration workloads, not production workloads, so a candidate-only tag can
otherwise expire after production stops sharing integration's current version.
Its existing policy preserves images with non-candidate tags, including SemVer
release tags.

After the production evidence verifier succeeds, use the operator's configured
registry credentials for each approved `IMAGE_REF` (`repository@sha256:...`) and
`PROD_TAG` from the release evidence:

```sh
skopeo copy --preserve-digests \
  "docker://${IMAGE_REF}" "docker://${IMAGE_REF%@*}:${PROD_TAG}"
test "$(skopeo inspect --format '{{.Digest}}' \
  "docker://${IMAGE_REF%@*}:${PROD_TAG}")" = "${IMAGE_REF##*@}"
```

Do not apply production until all seven release tags read back the approved
digests. Render and deploy the original digest references, not the tags. Adding
these retention tags does not rebuild the images or create a second publisher.

`scripts/ops/deploy_nebius_platform.py` plans or applies the output of
`render_nebius_platform.py`. Its default is a read-only cluster preflight; cloud
mutation requires explicit `--apply`. This is the supported hosted deployment entrypoint. `loom cluster up` is
limited to disposable development targets; the shared-cluster rollout broker
and its CLI command are retired.

### Protected read-only installation inventory

Before qualifying a managed multi-person installation, dispatch the existing
protected Nebius workflow from `dev` with the explicit inspection operation:

```bash
gh workflow run nebius-rollout.yml --repo qianyi-sun/loom --ref dev -f operation=inspect
```

This uses the same protected `nebius-integration` environment, pinned SSH host and
cluster identity checks as rollout, but cannot select the rollout job. It does
not require automatic rollout to be enabled and performs no Kubernetes, database,
DNS or cloud mutation. It shares rollout concurrency so the two workflow modes
do not race each other.

Download `nebius-inspect-RUN_ID-ATTEMPT` for the sanitized
`management-preflight.json` artifact. It contains the configured candidate,
namespace identities, node allocatable resources, declared Pod requests including
init containers and overhead, services/ingress, PVC sizes and storage classes.
It excludes Secret values, Pod environment/commands, annotations, kubeconfig and
configuration payloads. Failed or incomplete inventory fails the command rather
than being treated as an empty cluster.

For up to three newest failed bootstrap Pods in the selected platform namespace,
inspection checks the exact Job owner UID, terminal failure and expected bootstrap
command before reading at most 50 log lines / 16 KiB per Pod. The artifact adds
`failed_bootstrap_jobs`: Job/Pod identities plus allowlisted phase/error type and,
for configuration HTTP errors, method, status and a fixed operation category.
No raw logs, arbitrary reason strings, route identifiers, SQL or credential values
are exported. Missing/unsupported logs are explicitly `unavailable`; they do not
turn a failed Job into success. This diagnosis grants no Job retry, dispatch-unpause
or rollback authority.

When `NEBIUS_INGRESS_INSTALLATION_JSON` is configured, `ingress_preflight`
reuses the installer's foundation and full Node/Pod capacity checks with a fixed
read-only transport. It reports the bound source/candidate, passed checks, blocked
phase, allowlisted reason codes and response byte counts against the gateway's
response-size limit. It never reads Secrets, issues writes, retries installation,
or exports raw API/configuration/error payloads. Missing authority is explicitly
`not_configured`; a blocked diagnostic does not discard the general inventory.
Foundation and capacity checks report independently, so candidate drift does not
hide current capacity failures. Each revalidates the exact namespace UIDs.
These checks run through protected inspection, not the installed gateway process:
the reported `bound_source_sha` identifies gateway authority, not the diagnostic
code. Gateway-source correspondence, gateway-local execution, certificate
files/delivery, staging and cutover remain
unverified even when these checks pass. Reconcile retained journals before any
mutating retry; this diagnostic does not authorize one or establish historical
failure-time state.

The capacity read excludes only `Succeeded` and `Failed` Pods at the API, matching
the accounting rule that already ignores terminal Pods. All nonterminal Pods
across all namespaces remain in scope, including pending and terminating foreign
workloads. This keeps retained completed Job history out of the response budget
without deleting it. The 4 MiB response limit, complete-list checks and capacity
envelope remain enforced; an oversized live inventory still blocks installation.

`observed` means inventory succeeded, not that personal environments are ready.
The configured candidate is read from the platform ConfigMap, which rollout can
update before migrations and workload replacement complete. It is not proof of
the running workload versions; use successful candidate-bound rollout evidence
and workload readback to qualify that separately.
No child capacity allowance is inferred from a naive request sum. Wildcard
DNS/TLS, provisioning IAM, management installation, live Nebius quota and pool
limits, and installed concurrent-owner acceptance still require qualification.
Keep this evidence outside the repository.

### Shared HTTPS installation boundary

The shared-controller renderer is `loom.nebius_shared_ingress.render_shared_ingress`.
Its `SharedIngressInstallation` input uses schema
`loom.nebius-shared-ingress.v1`, a non-nil `installation_id`, `foundation`,
digest-pinned native-registry `image`, and a separate `tls_secret_name`. Foundation
ingress namespace must equal the existing standalone namespace and its controller
label must be `loom-shared-ingress`. Use a certificate covering both the configured
child wildcard and the separate management hostname. Private keys stay in the
platform namespace; do not reuse the standalone Caddy key or publish Secret data.

Rendering does not install or switch traffic. Before enabling the standalone
`shared_ingress_enabled` flag through the protected Nebius workflow, qualify the
image and certificate, existing Service UID/allocation/ports, exact resource
ownership, controller readiness, capacity and legacy-host HTTPS/TLS-ALPN routing.
Preserve the original selector and protected configuration as rollback evidence.
The selector change and persisted flag must share the rollout concurrency boundary;
ordinary rollout rechecks the selected mode after acquiring its guard, before any
backup or resource mutation. Never remove `loom-web-tls`
or replace the LoadBalancer to perform this migration. Use the protected
`operation=ingress` installation described below, never an ad-hoc `kubectl`
cutover.

The renderer uses Traefik 3.7.13 features and receives trusted read-only Secret
discovery across the cluster. Budget 200m CPU, 256 MiB memory and 128 MiB ephemeral
storage including its rolling surge. Uploads and responses stream directly;
there is no uniform request-byte or global request-concurrency limit. Existing
application validation is not a pre-auth request-exhaustion defense. Qualify
bounded application request reception before public management activation; do
not treat the removed nginx body-size annotation as enforced. The controller has no certificate
issuance credentials; renewal and the corresponding safe reload still require
the protected installation workflow. Disposable routing evidence is not live
DNS/TLS or installed multi-owner acceptance.

### Certificate DNS-01 hooks

`scripts/ops/nebius_dns_challenge.py` implements the narrow GoDaddy **v3 bearer
PAT** provider boundary for Certbot manual authentication/cleanup hooks. Install
the locked `cluster` extra for `httpx` and `dnspython`. The hook does not implement
ACME, install certificates, change Kubernetes, or expose a public management
endpoint. Its presence is not permission to perform an ad-hoc ingress cutover.
The protected certificate operation below supplies the pinned ACME client,
private account state and SAN/expiry qualification. Safe Kubernetes Secret
delivery, controller reload and scheduled renewal remain installation boundaries.

The caller supplies `auth` or `cleanup`, `--zone`, `--certificate-domain`,
`--credential-file` and `--state-dir`; Certbot supplies `CERTBOT_DOMAIN` and
`CERTBOT_VALIDATION`. The certificate domain must be an exact child of the
selected zone, and the hook accepts only that domain (or its wildcard). It
derives the one `_acme-challenge` TXT name; it never replaces a recordset or
modifies A, CNAME, NS or other records. Existing standalone Caddy TLS remains
independent.

The owner-only regular credential file contains `token` and ISO-date
`expires_on`; mode 0600 and a nonexpired token are required. Provision and renew
the PAT through the existing protected operator route, never in ingress or a
personal namespace. File paths and secret values are not emitted in hook output.
Keep the journal directory private (0700); its 0600 files must persist across
renewals and interrupted operations. Use one shared protected journal and the
installation workflow's exclusion boundary, not copies on parallel hosts.

Authentication durably records intent and the bounded scoped record-ID inventory
before POST (including the parent entry on first journal creation), and persists
the returned record ID before reporting success. It checks the exact TXT value directly against
every discovered authoritative IPv4 DNS address; recursive resolver or provider
API success alone is insufficient. Aliased/delegated challenge routes are
rejected. Propagation waits at most ten minutes. Failure preserves the journal
and created TXT for retry or exact cleanup. No uncertain POST is retried.

A `pending` journal means the provider write outcome is uncertain. Stop and
reconcile its private intent with the provider's exact record inventory; do not
erase the journal, guess an ID or blindly repeat authentication. Cleanup reads
the recorded ID back and requires matching name/type/value/TTL before deleting
only that record. Other TXT values are preserved. An absent recorded ID completes
cleanup idempotently. GoDaddy supplies no conditional delete in this API: the
protected caller must exclude competing management of its recorded challenge
IDs between readback and DELETE. Arbitrary external DNS administration is not
made transactional by these hooks.

### Protected certificate qualification

`nebius-rollout` supports the manual `certificate` operation on protected `dev`:

```bash
gh workflow run nebius-rollout.yml --repo qianyi-sun/loom --ref dev -f operation=certificate
```

The certificate operation requires its own protected `NEBIUS_CERTIFICATE_SSH_KEY`.
Do not reuse `NEBIUS_DEPLOY_SSH_KEY`: that key may be forced to the Kubernetes-only
gateway and correctly rejects certificate commands. Keep its restriction intact.
The new key is restricted to the literal `loom-nebius-certificate-v1` command and
one reviewed bundle digest; it cannot upload arbitrary replacement tooling.
Use a new Ed25519 key, never an operator's ordinary private login key.

From the exact merged source and pinned `uv`, prepare the non-secret bundle:

```bash
uv export --locked --only-group nebius-certificates --no-emit-project --no-emit-workspace \
  --format requirements-txt --no-header --quiet --output-file /secure/requirements.txt
uv run --no-sync python scripts/ops/nebius_certificate_rollout.py \
  --requirements /secure/requirements.txt --evidence-dir /secure/preparation \
  --prepare-bundle /secure/certificate-bundle.zip
```

Supply the same `NEBIUS_CERTIFICATE_INSTALLATION_JSON` used by the protected
workflow. Preparation makes no SSH/DNS call and refuses to overwrite the bundle.
Preserve its reported SHA-256. Through the existing approved operator route,
transfer only that bundle, the reviewed installer and the new public key into a
private gateway directory. Keep the private key out of this transfer and logs.
Run the installer first without `--apply`, inspect the receipt, then apply:

```bash
python3 /secure/install_nebius_certificate_entrypoint.py \
  --bundle /secure/certificate-bundle.zip --bundle-sha256 REVIEWED_SHA256 \
  --public-key /secure/certificate.pub --apply
```

Inputs must be private owned regular files. Installation appends one restricted
key while preserving existing entries, under an operator-local lock with atomic
replacement/readback; coordinate other SSH key edits through the same operator.
It installs immutable hash-bound gateway source beneath the dedicated certificate
root, but does not issue a certificate or alter Kubernetes. Same-key conflicting
authority and changed installed files block. A changed bundle/configuration needs
another explicit authority installation; a self-reported bundle hash is not trust.
Set the matching private key only in the protected Environment's certificate
secret, verify the entrypoint/key binding, then dispatch the protected workflow.
`certificate_transport_authority_rejected` means command/bundle authorization
failed, not an ACME failure. Never remove restrictions to clear that diagnostic.

It does not require enabling automatic application rollout and cannot select
application deployment. The same workflow concurrency group serializes it with
rollout and inspection. There is **no renewal schedule yet**: recurring issuance
without corresponding Secret delivery/reload would give false confidence.

Configure `NEBIUS_CERTIFICATE_INSTALLATION_JSON` in the protected
`nebius-integration` GitHub Environment. This is non-secret installation metadata,
not the DNS token or private key. Its exact fields are:

```json
{
  "schema": "loom.nebius-certificate-installation.v1",
  "installation_id": "024cfbfb-a7e8-4d85-9c60-c1d838730f9a",
  "zone": "example.test",
  "child_domain": "dev.example.test",
  "management_host": "management.example.test",
  "credential_file": "/home/operator/.loom/private/godaddy.json",
  "state_dir": "/home/operator/.loom/nebius-certificates/state",
  "email": "operator@example.test"
}
```

These are examples, not provisioned names or an installation identity to reuse.
Choose a non-nil installation UUID; the state root must be a private owned
`nebius-certificates/state` below an existing trusted parent. Both subjects must
be below the selected DNS zone, and management must be outside the personal
child zone. The only requested SANs are the child wildcard and exact management
host. Use an operator contact email, or explicitly set `email` to `null` for an
ACME account without email. Arrange PAT rotation before its recorded expiry;
certificate lifetime does not extend credential lifetime.

The protected runner sends only the reviewed issuer, DNS hook and gateway watchdog, deterministic configuration,
the pinned uv executable and hash-locked wheel requirements. Certbot 5.8.0 is
installed in an isolated gateway virtual environment. DNS credentials, ACME
account keys, certificate keys, journals and private logs stay on the gateway;
they are not workflow artifacts or ingress mounts. It neither changes the old
operator environment nor runs hooks from personal source. At most eight
content-addressed tooling releases are retained; further new versions stop until
an operator retires exact obsolete tooling. State/accounts are never deleted as
part of tooling installation.

Issuance holds an owner-local lock and records durable intent. Existing pending,
created or unknown DNS journals block a new issuance, even if Certbot would use
a different challenge value. Failure retains its intent and previous selected
certificate. Do not delete `issuance.json` or retry until private process state,
ACME outcome and exact DNS record ownership have been reconciled. Automated
recovery of ambiguous issuance is not provided by this operation.
An independent Linux watchdog follows controller liveness through a private
pipe and terminates the command group on timeout, owner death or cancellation.
The persisted intent still fences recovery while descendant cleanup completes.

Successful issuance validates the leaf/key pair, exact two SANs, public trust
chain, server authentication, non-CA leaf and at least seven days of remaining
validity measured after the client finishes. Persisted ACME, work and log trees
are bounded and checked for private ownership before client writes; only exact
Certbot lineage links are allowed. Account keys/registration, renewal and lineage
files/directories are fsynced before qualification. A private generation is
fsynced before atomic `selected.json` publication;
the prior generation remains available. Certbot's original account/lineage and
the challenge journal also remain private for recovery. Public evidence contains
only installation ID, generation/fingerprint, SANs, expiry and a fixed status.

`qualified` means a recoverable certificate exists on the gateway. It does **not**
mean public DNS, Kubernetes TLS Secret delivery, safe reload, shared ingress or
management is installed. Preserve standalone Caddy, its certificate/PVC and the
existing public LoadBalancer. The protected ingress installer must verify exact
Secret ownership/UID and delivered fingerprint before activation, and connect
renewal to verified reload before enabling a schedule.

### Ingress TLS delivery and rotation status

`scripts/ops/nebius_ingress_gateway.py` supplies private delivery, journaled
controller switching and per-Pod TLS qualification primitives. The protected
`nebius-rollout` workflow connects initial installation as `operation=ingress`
and owned, paused recovery as `operation=ingress-rollback`. Do not invoke these
private modules directly against a shared cluster to bypass that authority.

Their receipts distinguish `tls_delivered`, `controller_switch_observed` and
`controller_qualified`. These mean, respectively, exact immutable Secret readback,
an observed controller specification change, and current-Pod certificate proof.
None establishes public DNS, selector cutover or management readiness. On an
unknown create/switch outcome, preserve the private delivery/switch journals and
old Secrets. Never erase the intent or repeat a write to make it disappear; only
exact readback can reconcile it. Renewal stays unscheduled until the protected
issuance, delivery, reload and public-route qualification are connected.

Initial resource staging and region-image publication have separate private
primitives in `nebius_ingress_stage.py` and `nebius_ingress_image.py`; neither is
a shared-cluster operator entrypoint. Staging freezes all eight renderer objects,
their full defaulted configurations and returned UIDs. It preserves the existing
public Service and refuses adoption or missing-resource recreation. Retain its
initial journal unchanged when later using the TLS rotation operation.

Image publication requires private, freshly minted auth scoped to the exact
region registry. It copies only the pinned Traefik manifest, using a digest-only
destination, and verifies raw manifest/configuration hashes plus architecture and
version. Preserve `image-mirror.json` on any failure; an unresolved recorded copy
is read back, not automatically repeated. `mirrored` is image-identity evidence,
not a vulnerability scan, controller readiness or public cutover. The protected
installation scans the exact upstream digest first, using the pinned Trivy
release policy with no exceptions, before registry publication or cluster writes.

### Install the restricted ingress authority

This is an operator bootstrap, not evidence that shared ingress is installed.
Use a clean checkout of the exact integrated `dev` commit and the existing
approved gateway operator route. Preserve the certificate entrypoint and all
other SSH grants. Never give Actions the operator key.

Prepare `NEBIUS_INGRESS_INSTALLATION_JSON` as non-secret protected configuration:

```json
{
  "schema": "loom.nebius-ingress-installation.v1",
  "source_sha": "<exact integrated tooling commit>",
  "candidate": "<exact installed application candidate>",
  "state_dir": "/home/operator/.loom/nebius-ingress/state",
  "certificate_config": "/home/operator/.loom/nebius-certificates/state/installation.json",
  "kubeconfig": "/home/operator/.kube/approved-nebius-config",
  "kubectl": "/usr/local/bin/kubectl",
  "cluster_id": "mk8scluster-<approved identifier suffix>",
  "api_server": "https://<approved API endpoint>",
  "ingress_class": "loom-shared",
  "image": "cr.<region>.nebius.cloud/<registry>/loom-shared-ingress@sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18",
  "binding": {
    "installation_id": "<new ingress UUID>",
    "certificate_installation_id": "<existing certificate UUID>",
    "namespace": "<existing application namespace>",
    "namespace_uid": "<freshly observed namespace UUID>",
    "kube_system_uid": "<freshly observed kube-system UUID>",
    "child_domain": "<approved personal environment zone>",
    "management_host": "<approved management hostname>"
  }
}
```

Resolve placeholders from protected readback, not historical examples. Nebius
managed Kubernetes cluster IDs use `mk8scluster-`, not `mk8s-`; malformed IDs
are rejected before private tooling is installed. The
application candidate must already contain `loom.nebius_rollout_guard observe`;
an older candidate fails before acquiring a pause. The live ConfigMap remains
the configuration authority: the installer reads it freshly and checks candidate,
cluster/API origin and namespace identity rather than rendering from this metadata.

With pinned uv 0.11.26, export dependencies outside the checkout and prepare an
exact tooling bundle. `--prepare-bundle` does not scan, publish, use SSH or mutate
Kubernetes; the destination must not exist:

```bash
uv export --locked --no-default-groups --extra cluster --group nebius-certificates \
  --no-emit-workspace --format requirements-txt --no-header --quiet \
  --output-file /private/ingress-requirements.txt
uv run --no-sync python -m scripts.ops.nebius_ingress_rollout \
  --operation install --requirements /private/ingress-requirements.txt \
  --prepare-bundle /private/ingress-approved.zip --evidence-dir /private/ingress-evidence
```

The bundle contains the exact scripts, uv, hash-locked dependencies, both Loom
and `loom-bundle-checksum` wheels, and installation metadata—no credentials.
Wheel construction uses a clean committed-source archive, the locked,
hash-verified setuptools backend, stable timestamps and canonical ZIP metadata.
Checkout modes, umask and ignored build artifacts cannot change the wheels.
Record the reported bundle SHA256. Through the operator route, place
that bundle, the reviewed standalone installer and a new plain Ed25519 public
key in private files; preview, then apply the same reviewed inputs:

```bash
python3 -I /private/install_nebius_ingress_entrypoint.py \
  --bundle /private/ingress-approved.zip --bundle-sha256 <approved-sha256> \
  --public-key /private/ingress.pub
# Repeat the exact command with --apply after inspecting the prepared receipt.
```

The installer appends one `restrict` forced-command grant, bound to the bundle
and bootstrap/supervisor source hashes. It accepts only
`loom-nebius-ingress-v1`, `loom-nebius-ingress-rollback-v1`,
`loom-nebius-ingress-image-intent-v1` and `loom-nebius-ingress-dns-v1`. Existing conflicting
authority for that key is rejected. Keep the private key only in protected
Environment secret `NEBIUS_INGRESS_SSH_KEY`; set the matching metadata variable
`NEBIUS_INGRESS_INSTALLATION_JSON`. Use the existing verified deployment target,
known-hosts setting and registry-only publication identity. No broad gateway
shell or operator credentials belong in Actions.

Dispatch `nebius-rollout` from `dev` with `operation=ingress`. It uses the same
workflow concurrency as application rollout and certificate operations. The
gateway installs a separate private, content-addressed tool environment, verifies
isolated imports, and supervises operation children for timeout and parent death.
Partial tooling releases are retained for reconciliation, not overwritten.
Before any registry copy, the fixed image-intent command durably reserves the
exact destination on the gateway. Only its first successful reply permits one
copy. Later workflow invocations verify the destination by readback only, even
if the previous runner disappeared or its evidence artifact expired. A lost
intent reply also consumes that permission. Missing or corrupt destination data
then requires explicit operator reconciliation; do not erase the gateway's
`state/image-publication.json` to trigger another copy. Registry credentials and
publication remain on Actions, not the gateway.

### Public cutover and paused recovery

Before writes, fresh full Node/Pod inventory must fit two additional ingress
Pods on the eligible system node. Foreign, pending, init/sidecar and resizing
workloads count; legacy-node capacity is not borrowed. No node or storage growth
is performed. Delivery and initial staging precede exact-Pod TLS and legacy-route
proof. Only then does cutover acquire this candidate's idle rollout guard.
Legacy HTTPS proof requires the health response, frontend environment and
responding API's `/api/v1/version` build revision to match the protected candidate.
This does not claim that every API replica has been inspected.

The Service selector and configuration flag are separate UID/resourceVersion-
conditioned writes. Durable intent and an operation UUID precede each write;
exact readback resolves a lost reply without repeating it. Public allocation,
ports, unrelated configuration and legacy TLS remain intact. `complete` means
the public legacy HTTPS route and new certificate qualified and the owned pause
was released. It does not mean DNS, renewal or management is ready.

On failure, preserve the private `state/stage`, `state/cutover` and certificate
journals, plus the workflow's bounded image/operation evidence. A paused,
incomplete cutover can use `operation=ingress-rollback`: it proves the retained
original backend, restores only still-owned journaled values, verifies the
original public route and releases only its own pause. It does not require image
publication, a new certificate or healthy new ingress. Foreign drift, missing
ownership or an unresolved guard-release intent blocks recovery; do not clear
the guard, delete a journal, or blindly dispatch again. Completed cutovers cannot
be reversed through this paused-recovery operation. A new attempt after completed
rollback requires operator reconciliation preserving the old journal.

### Publish the personal and management DNS routes

After a completed, freshly qualified ingress cutover, dispatch `nebius-rollout`
from `dev` with `operation=ingress-dns`. The exact installed tooling must include
this action; changing a bundle requires a new dedicated key grant, not an
overwrite of existing authority. DNS uses the same protected Environment and
workflow concurrency but receives no registry credential and performs no image
scan/copy, certificate issuance, Kubernetes write or rollout-guard mutation.

The fixed action loads the expiry-checked DNS credential privately on the gateway
from the bound certificate installation. It can create only the wildcard A record
`*.<child_domain>` and the exact `<management_host>` A record, with TTL 600. Their
public IPv4 address is freshly read from the retained, UID-bound public Service;
neither names nor address are dispatch inputs. Completed staging/cutover evidence,
current candidate, certificate delivery, controller Pods and legacy/public HTTPS
must still qualify. Both names' provider inventory and authoritative ownership
are checked before publication; target/credential drift blocks further writes.

Existing identical single A records are retained as `external`, not adopted.
Conflicting/duplicate A, AAAA, aliases or delegation block publication. Same-name
TXT and unrelated records remain untouched. The private
`state/dns/dns-publication.json` journal records intent before each POST and allows
at most one POST per name across replacement invocations. A lost reply followed by
matching readback is recorded as `uncertain`, not proof of exclusive ownership.
An absent uncertain record, changed recorded identity or changed target requires
operator reconciliation; never erase the journal to retry. There is no automatic
rollback or delete operation for a partially published pair.

`dns_published` means every discovered authority returned the exact A address and
no AAAA/alias for a fresh wildcard child and the management host, ordinary recursive
resolution agreed, and trusted exact-IP TLS matched the delivered certificate for
both hosts. The sanitized evidence contains record IDs and origins, not credentials.
This proves routing, not management API availability or multi-owner acceptance.
Scheduled renewal/delivery and DNS-token renewal remain separate operational work
before accepting unattended management or personal environments.

Ordinary application rollout also applies the existing `loom-web-origin` Service.
Its `kubectl.kubernetes.io/last-applied-configuration` annotation is bookkeeping,
not routing state: origin qualification ignores only that annotation when comparing
the retained staging snapshot. The original journal remains unchanged. Service UID,
ownership markers, other metadata, allocation, selector and ports still must match;
this exception does not authorize adopting or modifying a different backend.

### Render management manifests

Management HTTP requests default to a 1 MiB body limit, eight in-flight requests
per process and a 30-second total body-reception deadline, enforced before
parsing/authentication. Overload returns 503 with Retry-After rather than queuing;
oversize requests return 413. Response streaming is not buffered. Configure via
`LOOM_SVC_MANAGEMENT_HTTP_MAX_BODY_BYTES`,
`LOOM_SVC_MANAGEMENT_HTTP_MAX_INFLIGHT` and
`LOOM_SVC_MANAGEMENT_HTTP_BODY_TIMEOUT_SEC` only after accounting for raw bodies,
copies and parsing/application memory in the Pod envelope. These controls are
management-only and do not establish personal-upload or installed readiness.

Prepare the protected `loom.nebius-management-deployment.v1` JSON described in
[the management architecture](../architecture/nebius-primary-platform.md#management-deployment-manifests)
outside the repository. Its nested `installation.provider_runtime` is mandatory
for this deployment. Set its Kubernetes endpoint to the foundation's exact API
origin and `ca_file` to `/var/run/loom-management-kubernetes/ca.crt`.
For native projected identity, set `kubernetes.kind` to
`projected_service_account` and `token_file` to
`/var/run/loom-management-kubernetes/token`; omit `credentials_file`.
Kubernetes supplies and renews that token only for the management API's separate
`loom-management-provisioner` ServiceAccount. The protected installer must qualify
its namespace permissions before activation; rendering installs no RBAC grants.
For the retained explicit Nebius SDK mode, omit `kind`/`token_file` and set
`credentials_file` to `/var/run/loom-management-kubernetes/credentials.json`.
Both modes require `cloud_credentials_file` at
`/var/run/loom-management-cloud/credentials.json`. None uses an ambient operator login.

Set `installation.foundation.provisioning_project_id` to the dedicated project
qualified for management-owned IAM and object storage, separate from the cluster
project and tenant/quota parent. Provider activation rejects an omitted/null scope.
Qualify its region against the configured storage endpoints and its effective
permissions before supplying the dedicated cloud credential; the identifier alone
is not an IAM grant. Nebius project-scoped groups allow management provisioning
without tenant-wide IAM administration. Do not grant cluster-project administration
as a substitute or change platform `project_id`/`quota_parent_id` to redirect IAM.
Existing operations retain their frozen scope for retries and credential cleanup;
changing this field neither migrates resources nor repairs historical permissions.

For dynamic native-ServiceAccount namespace provisioning, the protected
`installation.foundation.namespace_authority` contains `installation_id` and
`namespace`, bound to the management installation. Its pure
`render_namespace_authority` helper emits installer-owned fail-closed admission
and bounded RBAC. It requires Kubernetes v1 ValidatingAdmissionPolicy support
(Kubernetes 1.30 or newer). Do not install its bootstrap grant on its own or treat
generated policy text as proof of enforcement. The protected installer must
qualify policy readiness, denial probes, the exact manager identity and ownership
before management can receive/use the credentials. No shared-cluster installation
command is provided by this helper.

New child plans carry three installation-labeled namespaces followed by their
local provisioner RoleBindings. The manager cannot retag/adopt foreign namespaces
or read their Secrets; imported namespaces require a different, explicitly
qualified enrollment operation. Missing or mismatched policy/grant state is an
activation blocker, never a reason to supply cluster-admin credentials.
Qualify both RoleBinding restrictions and the observer Role's exact read-only
rules; a fixed Role name alone does not constrain delegated permissions.

For new managed databases, set
`installation.foundation.generated_postgres_storage_gi` explicitly when the
standalone database's size is inappropriate. The value is an integer from 10 to
1024 GiB; omitted/`null` keeps the inherited size. For example, `10` selects a
10 GiB PVC and corresponding backup scratch for each newly generated child,
without shrinking imported or previously created databases. Account for these
requests in the separate `platform_budget`, including concurrent backups. This
is a creation default, not authorization to buy storage or a PVC resize command.

```bash
uv run --no-sync python scripts/ops/render_nebius_management.py \
  --deployment /secure/management-deployment.json \
  --candidate /secure/publication/candidate.json \
  --runtime-profile /secure/publication/runtime-profile.json \
  --output /secure/management-render
```

The output directory must not already exist. It and its files are private; errors
do not echo input values. `rendered-not-installed` reports the manifest list,
candidate, revision and fixed platform-resource envelope. Rendering validates
source identity, registry/digest binding and configuration, not successful remote
CI/publication or installed readiness. Select a protected `dev` publication, never
a personal snapshot or the retired integration branch.

All referenced Secrets belong only to the management namespace:
`loom-platform-db` (admin/service credentials and database CA),
`loom-management-db-tls`, `loom-platform-auth` (secret-store master key),
`loom-admin-secret`, `loom-management-publications` (`token`, read-only GitHub),
`loom-management-kubernetes` (`ca.crt`, `credentials.json`, SDK mode only),
`loom-management-cloud` (`credentials.json`), and `loom-platform-storage`
(`backup-access-key`, `backup-secret-key`). Identical names in another namespace
do not authorize copying that namespace's values. Preserve generated keys and
their recovery material; Kubernetes/cloud identities must be separately scoped.

Do not pass this render to the standalone platform deployer or apply it manually.
Management ownership/readback, initial Secret delivery, HTTPS ingress/TLS,
off-node backup and the protected management rollout must be qualified before
activation. No `nebius-rollout` management-install operation is introduced by the
render-only command, and it purchases no capacity or storage.

The private `nebius_management_material` helper implements initial delivery of
fresh management database/TLS/master-key/admin Secrets. It requires an already
qualified installation-owned namespace and records its UID plus the cluster UID.
Generated material is persisted once in a private recovery journal before any
immutable Secret create; retries read back exact data/ownership/UIDs instead of
regenerating credentials or repeating an ambiguous create. An independent private
initialization record detects missing or mismatched journal material. The fixed
HTTPS adapter requires explicit trusted TLS/authentication and disables redirects
and request retries; it does not load ambient kubeconfig or execute plugins.
Preserve the entire private directory, including both records, and never include
it in workflow artifacts. The caller must also retain independent installation
evidence so loss of the entire directory cannot be treated as a new installation.
Lost state or conflicting live
Secrets require explicit recovery; this helper is not a credential rotation or
namespace-adoption procedure. Cloud/Kubernetes/publication/backup credentials
remain separate inputs. This internal primitive has no shared-cluster CLI and
does not yet connect a protected management installation or prove readiness.

The `nebius_management_bootstrap` primitive composes create-only namespace setup
with that generated-material delivery. It journals the namespace create intent,
freezes the observed namespace UID, and checks installation ownership and restricted
Pod policy before each Secret write. Lost create replies permit readback only;
untracked namespaces, conflicting recovery state, and missing material after
delivery intent block rather than authorize adoption or regeneration. Retain its
outer journal as well as the nested material directory. Independent installation
evidence must still detect loss of the entire state tree. Its receipt means only
`management_bootstrapped`, not an installed API: scoped external credentials,
runtime authority, database/migration, backup, HTTPS readiness and protected
workflow integration remain installer responsibilities.

The internal `nebius_management_stage` primitive stages one fixed management render
phase with create-only, UID-bound recovery. Preserve each phase's private journal;
do not rerun with an empty state directory to recover a missing or failed workload.
Its read-only readiness observation checks the database, migration or service
against the original recorded identity and current controller state. A staged
CronJob or Ingress is not evidence of an uploaded backup, restore or working public
management API. This primitive also has no direct shared-cluster CLI; it does not
add a management-install operation to the protected workflow on its own.

### Protected initial management installation

Qualify platform capacity before preparing management's child allowance. The
primary Terraform platform input supports `integration_platform.system_preset`
(`4vcpu-16gb` or `8vcpu-32gb`) and `system_disk_gib` (integer 80–1024 GiB,
default 80). The disk is node-local OS/image/backup scratch, not PostgreSQL PVC
capacity. Include existing maintenance scratch as well as management, concurrent
children, rollout surge, system daemons and images; undeclared Pod requests do
not mean the workload uses no disk.

For a planned system-node replacement, `system_create_before_drain: true` selects
one temporary surge node and zero unavailable nodes; the steady count remains
one. The default remains the existing drain-first strategy. These inputs do not
alter execution groups or secondary-region capacity. A temporary node also needs
provider quota and incurs node/disk charges. This is not a zero-downtime guarantee:
single-replica databases and ingress can pause while retained volumes reattach.

Before applying, retain a current off-node backup/restore proof, PVC/PV identities,
ingress allocation, state/backend identity and exact saved Terraform plan. Inspect
the plan for only the intended primary system-group update; do not apply unrelated
changes or deletes. Use protected rollout for Kubernetes observations/recovery,
not an ad-hoc drain or workload deletion. Reconcile an uncertain provider outcome
before retrying. Rollback is a separately reviewed forward node-group update;
retain the enlarged disk and all data volumes rather than shrinking or deleting
them. Capacity configuration alone is not installed acceptance.

`nebius-rollout` provides two manual actions, `management-preflight` and
`management-install`, on `dev` in the protected `nebius-integration` environment.
They share the existing rollout serialization. Neither accepts shell commands,
manifests, credential values, a new capacity allocation or arbitrary code.

Prepare the following outside the repository, on the operator-owned gateway:

- A private `inputs.json` under `.loom/nebius-management`, using
  `loom.nebius-management-private-inputs.v1`. It contains the management
  `deployment`, exact published `candidate` and `profile`, bootstrap `binding`,
  typed `prerequisites`, explicit `operator_connection`,
  `operator_cloud_credentials` path, existing `ingress_config` path, current
  standalone `foundation_candidate`, and `material_files` paths for the three
  supplied runtime Secrets. Each supplied field comes from a distinct private
  regular file, never an alias of an operator credential. Select the current
  foundation without editing the historical ingress installation or journals.
- Prerequisites pin the published candidate ID, scoped Nebius project/account/
  group/key/bucket identities, StorageClass UID and parameters, and the actual
  regional compute-disk and object-storage quota names/units. Provisioning and
  backup identities are separate. Qualify the publication reader's read-only
  authority, expiry and renewal separately: successful artifact retrieval proves
  approved bytes, not the token's complete permission scope or renewal.
- Public `loom.nebius-management-operation.v1` metadata with `source_sha`,
  `candidate`, `installation_id`, `namespace`, `state_dir`, `anchor_dir`,
  `inputs_path` and `inputs_sha256`. The three paths end in
  `nebius-management/state`, `nebius-management/anchor` and
  `nebius-management/inputs.json` respectively. The anchor is independent of
  replaceable phase state. Pin the SHA256 of the private input file; do not put
  its contents or the files it references into Actions variables/artifacts.

The exact clean, integrated `source_sha` builds a deterministic tooling bundle
with hashed dependencies and two first-party wheels. Prepare it with
`python -m scripts.ops.nebius_management_rollout --operation preflight
--requirements <locked-export> --evidence-dir <private-evidence>
--prepare-bundle <new-bundle-path>` and the public metadata in
`NEBIUS_MANAGEMENT_OPERATION_JSON`. The locked export uses the same `cluster`
extra and `nebius-certificates` group as the workflow. The bundle contains no
runtime/installation credentials or private input file.

Using the existing approved operator route, preview
`scripts/ops/install_nebius_management_entrypoint.py --bundle <bundle>
--bundle-sha256 <exact-digest> --public-key <dedicated-key.pub>`; `--apply` installs
the reviewed grant. It preserves other SSH grants and authorizes only the exact
bundle with `loom-nebius-management-preflight-v1` or
`loom-nebius-management-install-v1`. A different source, input digest or key
authority requires a separately reviewed grant, not an unrestricted gateway key.
Configure the public metadata as protected `NEBIUS_MANAGEMENT_OPERATION_JSON` and
the dedicated transport key as `NEBIUS_MANAGEMENT_SSH_KEY`.

Run preflight before install. `preflight_qualified` is a read-only observation,
not a reservation or installed result. Pin the storage-class UID and parameters
from a fresh protected `inspect` result, not from documentation's default
semantics. Its `storage_classes` projection exposes only the Nebius driver options
`type` (`NETWORK_SSD` or `NETWORK_SSD_IO_M3`) and `csi.storage.k8s.io/fstype`
(`ext4` or `xfs`). Use `parameters` only when `parameters_complete` is true:
an empty complete map means the class omits explicit parameters, whereas false
means an unknown driver, key, value or malformed parameter map was redacted.
Incomplete observations cannot qualify installation inputs. This observation does
not modify the class or weaken the installer's exact comparison.
Provider storage and backup quota checks use two read-only `GetByName` requests,
each bound to the configured tenant, quota name and region, with no RPC retry or
list fallback. Returned identity, active/usage state, service, byte unit and
remaining headroom must match. Nebius's quota list can include unrelated-region
default placeholders and can exceed its requested page size; it is not the
management installer's qualification source. This does not change provider limits
or the separate IAM inventory checks.
Backup credential qualification uses a
bounded `ListObjectsV2` request (`MaxKeys=1`), after verifying the exact private,
versioned bucket and object-only policy through IAM. Nebius can deny `HeadBucket`
for that policy even when object access works; do not broaden the backup identity
to work around it. A successful list is not backup write or restore evidence.
When a bound operation fails, `blocked` may include an allowlisted `stage` such
as `cluster_identity`, `foundation`, `platform_capacity`, `storage_class`,
`publication`, `cloud_identity`, `provider_quota`, `backup_access` or
`public_route`. This identifies the failed prerequisite without exporting raw
exceptions, credentials or cluster payloads. The protected rollout still exits
nonzero; successful delivery of a diagnostic report is not successful installation.
An unqualified input/transport failure remains generic. Diagnose the reported
stage and preserve recovery evidence before retrying; a diagnostic is not a grant
to bypass the check or broaden permissions.
Installed-phase failures also identify `recovery`, `install_<phase>`,
`runtime_authority`, `ready_<phase>` or `public_authentication`. Backup diagnostics
distinguish the recorded Job, Pod list/identity/template/status, bounded uploader
log, unchanged readback and off-node object proof (`backup_object`). These are
fixed allowlisted stage names, not raw exception text, object payloads or logs.
They do not add requests, change writes, or make a blocked phase retryable.
`pending` records a database, migration,
backup or service readiness barrier; a later invocation with identical inputs
reconciles existing identities before advancing. A failed/unknown outcome is not
permission to delete state or blindly repeat a write. Preserve the entire state,
anchor and generated recovery material. No automatic rollback crosses migration.

`management_installed` requires runtime-authority probes, retained storage
identity, a completed backup with off-node object readback, healthy management
and authenticated public HTTPS. Backup execution evidence accepts omitted Pod
type fields only from an exact `v1/PodList`; explicit conflicting types fail.
The recorded Job/Pod ownership, executable template, zero restarts and unchanged
readback remain required before accepting the uploader's checksum report.
The bounded uploader log must contain exactly one JSON report, optionally followed
by the bootstrap CLI's exact `Nebius platform backup complete` line. Arbitrary
text, additional reports or other trailers are rejected; logs are never exported.
It does **not** prove restoring that backup,
credential renewal, two-owner lifecycle, or shared task/build execution. Those
remain separate installed acceptance steps; a green workflow alone does not
establish the fully operational multi-person environment.

### Retained management upgrade for shared-data applications

The same protected `management-preflight` and `management-install` actions can
select `loom.nebius-management-upgrade-operation.v1`. This is a fixed upgrade of
a completed management installation, not a retry of an incomplete bootstrap.
Keep the original inputs, state, anchor, database, PVC and credentials unchanged.
The upgrade uses separate `nebius-management/upgrade/inputs.json`, `state` and
`anchor` paths and the same exact-bundle SSH authorization described above.

The new private input schema is
`loom.nebius-management-upgrade-private-inputs.v1`. It contains the original
v1 `original_operation` (including its input digest), the new `deployment`,
published `candidate` and `profile`, the retained management `binding`,
`shared_namespace_uid`, typed `prerequisites`, and five distinct private
`material_files`: `manager_password`, `database_name`, `ca_pem`,
`secret_store_master_keys`, and `cloud_credentials_json`. Operator access and
ingress configuration come from the original private inputs, not new workload
credentials. Do not place these private values in the public operation metadata.

Prerequisites bind the management candidate ID, shared ConfigMap/service/database
Secret/auth Secret UIDs, existing business bucket IDs and application IAM scope.
Each application release ID selects a protected publication with matching source
archive digest and service/web image digests. The shared development profile,
database name, CA and encryption keys must match their actual shared consumers.
The fixed SQL setup Job verifies the exact shared schema before granting the
retained manager role; it does not migrate business data. Physical sizing reserves
personal frontend/API workloads, with no per-person database, PVC or backup.

Nebius provisioning-project admin alone cannot manage shared groups in another
project. The application provisioner must have exactly its dedicated
provisioning-project admin group and a tenant-owned membership-controller group
with admin permits on the two selected shared groups only. The shared data/source
groups have object-policy access only to the bound development buckets. Qualify
these existing grants read-only; the upgrade does not create cloud grants or
request more bucket policies. It must not receive shared-project or tenant admin.

The upgrade stages fixed application configuration, admission, network and
credentials, then proves shared SQL setup. It fences legacy Pod creation and
observes the old process fully retired before running management migrations and
switching the retained Deployment. Unknown writes require readback, not a blind
retry. There is no automatic restart of the legacy provisioner on failure.
Preserve the stopped and original templates and all private journals for recovery.

`pending` identifies admission, authority, database, retirement/retire, migration,
activation or service readiness. Reinvoke only the same qualified operation to
advance it. `management_upgraded` requires the exact new Deployment to be ready,
the application provisioner healthy, and authenticated public HTTPS to pass.
It does not establish a working personal deployment or multi-owner acceptance;
create/status/login and safe suspend/resume remain required installed checks.

## Before the first application

Use the independently configured Terraform platform state and its cluster ID/API
endpoint to fill the environment configuration. Create the dedicated integration
nodes and the two namespaces, then provision referenced secrets into their owning
namespace. This secret bootstrap is separate: the renderer never writes secret
values. The deployer checks names of nonempty keys through a Kubernetes template;
it never requests or records the values. Provision public DNS and TLS certificates
for the configured hostname and a private PostgreSQL certificate/CA for the exact
service hostname. Configure access to Nebius Registry and the independent backup
bucket. A renderer success does not prove these dependencies exist.

Render the published image references, review the Kubernetes output, then deploy
from an operator checkout. First prepare the
[locked operator environment](operator-runbook.md#locked-operator-environment).
Use `--no-sync` for the deployment command so it cannot implicitly modify that
environment. The deployment machine does not need the image source commit
checked out:

```sh
uv run --no-sync python scripts/ops/deploy_nebius_platform.py \
  --render-dir /secure/nebius-render \
  --kubeconfig /secure/nebius-kubeconfig \
  --expected-cluster-id mk8scluster-EXACT_ID \
  --evidence-dir /secure/nebius-deployment-evidence
```

Repeat the same command with `--apply` after reviewing the target and preflight.
Serialize deployments of this environment through the caller's workflow concurrency
group; do not run independent operator deployments simultaneously. This script
does not introduce another lock broker or silently steal an active deployment.

## Validation and phases

Historical Nebius databases at ambiguous revisions `0133`–`0136` require the
[qualified lineage conversion](nebius-lineage-conversion.md) before their first
dev migration. Preserve the backup and writer-quiescence requirements in that
runbook; ordinary deployment does not infer or stamp the historical lineage.

The deployer reads the known phase YAML once, taking environment settings from
the application ConfigMap. It checks
that resources belong to the two integration namespaces (plus their dedicated
collector RBAC). Reviewed replica/resource tuning and operator notes beside the
rendered files do not require re-signing, rehashing or a fresh Git checkout.
There is no hash inventory, candidate signature wrapper or deterministic rerender
gate. The existing control plane verifies runtime image admission at its own
boundary; the deployment scripts only transport that configuration.
The kubeconfig server must equal the separately configured API endpoint, its
cluster name must identify the expected Nebius cluster, TLS verification must use
the cluster CA, and provisioned integration nodes must have Nebius provider IDs.
The current observed Nebius kubeconfig convention is a name ending in
`cluster-<id suffix>` and Node provider IDs use `nebius://computeinstance-...`.

For an existing database that needs a new migration or candidate/configuration,
the first mutation is a new Job from the existing backup CronJob. That Job must
finish successfully before configuration, PostgreSQL, migrations or services are
changed. The backup uploader verifies the stored object's byte count and checksum
metadata before reporting success. A missing backup CronJob blocks the upgrade.

For a fresh database, the order is namespaces/configuration, PostgreSQL and
readiness, backup CronJob installation, the candidate migration Job, services and
readiness, catalog/capacity configuration, execution actuator, public entry, then
HTTPS API/frontend smoke. Installing backup immediately after database readiness
allows a partially completed first installation to resume with backup protection.

Completed candidate Jobs are reused. Pending Jobs are waited for. Failed Jobs
require a diagnosed fix followed by explicit `--retry-failed-jobs`; only the failed
Jobs belonging to this rendered candidate can then be replaced. The deployer never
deletes namespaces, PVCs, healthy Jobs, or unrelated resources. A retained database
PVC without its StatefulSet blocks application and requires an explicit restore.

Every attempt leaves a separate sanitized JSON phase record, including the candidate version, target
and failed phase. Evidence excludes kubeconfig material,
API endpoint details and secret values. A failed attempt is not automatically
rolled back: restore and database-version compatibility must be reviewed against
the retained backup before a downgrade. Reapplying a known candidate is not proof
that a database restore or schema downgrade is safe.

HTTPS health and frontend routing smoke are deployment checks only. User login,
real workload execution, result integrity and fault-recovery acceptance remain the
separate pure-Nebius E2E milestone.

**Verifying the deployed version (#2009):** confirm the rendered candidate SHA
actually reached the cluster by comparing it against what the running app
reports, not just this deployment's own logs. Open the target URL, click the
version entry at the bottom of the sidebar, and check the frontend commit
matches the candidate; separately curl `<target>/api/v1/version` (or open it
in the details) for the backend's own commit, keeping in mind one response is
evidence for the responding instance only — for a multi-replica rollout, check
more than one before declaring the rollout complete. Record this comparison
(URL, expected candidate, observed frontend/backend commits) as deployment
evidence; a green deploy script run or merged PR is not by itself live
version acceptance.
