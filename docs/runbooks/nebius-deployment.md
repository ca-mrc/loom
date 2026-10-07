# Independent Nebius deployment

Nebius is the image publication path for both development and production.
`nebius-candidate` publishes the seven manifest-owned AMD64 images from an exact
`dev` commit. Production promotes those same immutable digest references through
`release-promotion-gate`, then the protected `dev` to `main` pull request and
`main-promotion-gate`. A `main` push does not rebuild or publish images to GHCR.
Use the candidate publication output as the source of image references; never
substitute a mutable branch tag during promotion.

The service image includes the `cluster` runtime dependencies as well as the
pinned `nebius-gateway` SDK: the management application builder uses the Python
Kubernetes client with its projected ServiceAccount identity. The image build
checks both SDK imports; the disposable Kubernetes service-image test also
constructs the builder transport and checks token refresh as the runtime user.
Tests in a developer environment alone do not qualify these image dependencies.

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

### Personal application owner workflow

Use the verified management HTTPS origin and an ordinary owner **user session**
in a named CLI context. Application login requires a user session, not a delegable
API token. The management and application logins stay separate:

```bash
loom --context management-alice auth login --server https://MANAGEMENT_HOST \
  --username ALICE --password env:LOOM_LOGIN_PASSWORD
loom --context management-alice dev app create alice --release RELEASE_UUID \
  --idempotency-key alice-create-1
loom --context management-alice dev app wait OPERATION_UUID --timeout 300
loom --context management-alice dev app status APPLICATION_UUID
loom --context management-alice dev app login APPLICATION_UUID --browser
```

Replace uppercase placeholders with verified installation values. `--release`
selects a protected pinned release or your own completed application build;
it is not a branch, candidate ID or local directory. `create` itself does not build
or publish source. Use the exact `loom --context app-...` command
printed after login to talk to that personal API. The default and management
contexts, model-provider settings and credentials are not copied or replaced.
Omit `--browser` on a headless machine.

After the protected source/build runtime is installed, build local feature code
through that same management context:

```bash
loom --context management-alice dev app build --source /PATH/TO/CHECKOUT \
  --idempotency-key alice-feature-1
loom --context management-alice dev app build-status BUILD_UUID
loom --context management-alice dev app build-wait BUILD_UUID --timeout 900
loom --context management-alice dev app create alice --release BUILD_UUID \
  --idempotency-key alice-create-1
```

The source capture includes committed, modified and non-ignored untracked files,
with credential/owner-context exclusions. It is **not CI-approved source**. A build
uses shared capacity at personal-development priority; it does not deploy anything.
Only `ready` status, after publication and pool cleanup, includes the qualified
release. That release ID is the build ID and can also be used with `app update`.
An unconfigured management installation returns 503; these commands do not bypass
protected installation or prove that an installation is ready.

After an uncertain response, use the latest printed retry command with its original
key and management context. Before upload completes the command binds the captured
source digest; changed source is refused. After verification it uses `--upload-id`
and no longer reads the checkout. `build-wait` exits 0 only for a ready build,
1 for failure/cancellation/request errors, and 2 for a local timeout without cancelling
the remote build. Explicit controls retain the expected attempt from build status:

```bash
loom --context management-alice dev app build-cancel BUILD_UUID --attempt ATTEMPT
loom --context management-alice dev app build-retry BUILD_UUID --attempt ATTEMPT
```

Retry is available only after a failed/cancelled attempt has completed cleanup.
It keeps the original source and recipe; use a new build key for different source.
Ready builds cannot be retried or retargeted to different images.

The version sidebar identifies personal source by digest. Its details distinguish
the JavaScript this page loaded from the backend instance that answered, and show
the base commit only as informational—not CI approval. A newer served build shows
an update notice without relabelling or automatically refreshing the current page.
Compare these reports with the ready build's source digest; they are not a substitute
for the management operation's deployment/readiness evidence.

Operator installation must deliver the configured source-only Secret, private
builder token and shared-build read permissions through the protected operation.
The management renderer reserves 2 GiB of temporary disk per concurrent source
upload (4 GiB by default), plus its ordinary ephemeral overhead. This is a
Pod-lifetime upload spool, not an extra database/PVC or execution-pool allocation.
The private spool is mounted at `/run/loom-application-source/spool`; startup
clears inherited setgid only on an otherwise owner-only directory. It continues
to reject symlinks, foreign ownership and broader permissions. Apply renderer
changes through a qualified protected transition, never by editing a retained
cutover's inputs or manually patching its live workload.
Do not hand-mount credentials or treat the renderer as installation authority.

Subsequent lifecycle changes use the same management context:

```bash
loom --context management-alice dev app list
loom --context management-alice dev app update APPLICATION_UUID --release NEXT_RELEASE_UUID
loom --context management-alice dev app suspend APPLICATION_UUID
loom --context management-alice dev app resume APPLICATION_UUID
loom --context management-alice dev app destroy APPLICATION_UUID
loom --context management-alice dev app retry BLOCKED_OPERATION_UUID
loom --context management-alice dev app evidence OPERATION_UUID
```

Each mutation prints a replay key and exact retry command before its POST. After a
lost response, reuse that printed command, including its expected generation and
management context; do not submit the request again with a new key. Without an
explicit `--expected-generation`, lifecycle changes first read current status and
fence the request to that observed generation. `wait` exits 0 only for completed,
1 for blocked/superseded or request errors, and 2 for a local timeout. A timeout
does not cancel remote work. Retry of a blocked operation is an explicit action,
not a substitute for reconciling uncertain writes.

`evidence` is read-only and owner-scoped. It shows saved Kubernetes/cloud effect
counts by resource kind, action and journal phase, alongside lease activity and
whether completion was recorded. It exposes no credentials or resource contents.
The snapshot can distinguish unconfirmed mutations from observed journal entries,
but is not live provider or credential-revocation proof. Do not resume, release
capacity or retry a blocked operation merely because all displayed effects are
observed; the lifecycle's normal completion barriers still apply.

Destroy stops only the owned application and retains shared development data and
its identity claims; it is not a shared database, bucket or namespace purge.
Update/resume rotate access; run application login again after the operation
completes. Legacy `loom dev create --candidate` and `loom dev destroy` act on full
environment identities and remain available for retained-environment recovery.
They are not aliases for these application commands.

This workflow requires an installed, ready application manager and qualified
releases. Source tests and green CI do not establish installed HTTPS/login,
concurrent-owner acceptance or arbitrary-source publication.

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
It excludes Secret values, arbitrary Pod environment/commands, annotations, kubeconfig and
configuration payloads. Failed or incomplete inventory fails the command rather
than being treated as an empty cluster.

Pod `container_statuses` and `init_container_statuses` report readiness, restart
counts, and current/previous container states from that same inventory. Waiting
and termination reasons are allowlisted; messages, image/container IDs and unknown
reason strings are not exported. Missing status is unknown, not healthy or zero
restarts. These diagnostics do not qualify runtime readiness or permit a retry.

`controller_inventory` adds Deployment/CronJob identities, declared ServiceAccounts,
selected execution target/pool/group identifiers and database Secret references.
Referenced `envFrom` ConfigMaps are projected through the same field allowlist;
Only the explicit control-plane/actuator database Secret references are fetched,
once per Secret within `controller_inventory`; no Secret list or generic `envFrom` Secret read is
performed. `declared_database_endpoints` reports each controller/container/setting,
the referenced Secret UID/version/key, and allowlisted host/port/database fields.
It never exports the URL, username, password, connection options or other Secret
values. Inline/config-map database URLs and unsupported, unreadable or ambiguous
references are explicitly `unavailable`; routing overrides in a URL query do not
produce a misleading endpoint. The control plane's direct `LOOM_CP_DB_URL` and
optional `LOOM_CP_DB_URL_POOL` are reported separately: a configured pooled URL
takes precedence for engine connections. No claim that the two routes reach the
same backend is made. The report
also lists RoleBinding/ClusterRoleBinding grants for Job-write verbs, including
wildcards and group subjects, and explicitly identifies unresolved role references.
This discovers guest controllers and target aliases without assuming one controller
per environment. These are declared endpoints, not verified DNS/backend identities
or proof that two databases are the same. It does not prove running Pods match
templates, cover every possible workload writer, or establish effective fencing.
Use it to prepare exact migration inputs, not as permission to stop foreign work.
An unreadable or partially paginated resource list fails inspection.

`pool_startup_diagnostics` observes at most one failed manager, pool gateway and
pooled collector. Selection is bound to the configured management installation,
the selected platform execution namespace, current Deployment/ReplicaSet or
CronJob/Job ancestry, and matching container configuration. At most three
candidates per role are checked. For each selected Pod, inspection reads only
the failed current or previous container's last 100 lines / 32 KiB, then rechecks
the Pod and controller identity; replacement or restart drift makes the result
`unavailable`. Output contains only fixed exception/stage/component enums and
numeric source line locations, never messages, raw logs, source lines, SQL,
credential values or arbitrary paths. Missing metadata is `not_configured`;
missing or unsupported diagnostics are `unavailable`. These are startup symptoms,
not readiness, root-cause proof, or permission to retry a write. No Secret reads,
Pod exec, workload changes or new authority are added by this diagnostic.

The `kube-system/coredns` Service entry also includes fixed `dns_checks`
booleans for deletion/ownership, native or legacy selector matching, a usable
cluster IP, and TCP/UDP port 53. These reuse the existing Service inventory read;
they export no selector values, owner details or additional configuration.
They explain current DNS qualification, not successful recovery or readiness.

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

`integration_platform.system_max_pods` separately configures the primary system
node's Pod limit (integer 16–110, default 64); execution and regional groups are
unchanged. A platform can have free CPU, memory and disk but insufficient Pod
slots. Compare the protected preflight's numeric `required.pods` with
`allocatable.pods`, including maintenance, rollout surge and the full child
allowance. Do not lower the reservation or bypass admission just to pass the check.
The [pinned Nebius provider's node-group contract](https://github.com/nebius/terraform-provider-nebius/blob/v0.6.46/docs/resources/mk8s_v1_node_group.md#nestedatt--template)
documents 110 as the native default and derives the per-node Pod CIDR as
`32 - ceil(log2(2 * max_pods))`. Increasing 64 to 110 therefore changes /25 to
/24. Check cluster Pod-address availability, including temporary surge nodes;
do not assume VM subnet free addresses alone prove Pod-address availability.
The larger Pod limit adds no compute or persistent storage but must be treated
as a node-replacement operation with the same retained-data protections below.

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

`nebius-rollout` provides installation actions `management-preflight` and
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

Gateway preparation failures return a bound `blocked` report with a `tooling_*`
stage, including `tooling_dependency_sync` for locked dependency installation and
`tooling_retained_incomplete` when an earlier preparation lacks its completion
marker. These stages identify the failed boundary; they do not expose child
output or diagnose the underlying provider, network or filesystem error. A
blocked report still fails the rollout and never dispatches the requested action.
For dependency preparation failures, inspect gateway disk/inode availability and
diagnose the dependency installation through the approved operator route before
retrying. Preserve the failed release, bundle and operation records. The gateway
refuses an identical incomplete release. After correcting the cause, an anchored
image repair can use a new tooling continuation as described below.
The fixed Python entry uses `-I -B` for qualification and operation execution, so
retained tooling releases do not accumulate duplicate import bytecode caches.

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
The standalone authority installer accepts this exact upgrade layout while keeping
SSH grants under the original `nebius-management/authority/<bundle-digest>` root.
Use a dedicated key; bootstrap and upgrade inputs, journals and grants remain
separate. Installing a grant alone does not stage inputs or run the upgrade.

The new private input schema is
`loom.nebius-management-upgrade-private-inputs.v1`. It contains the original
v1 `original_operation` (including its input digest), the new `deployment`,
published `candidate` and `profile`, the retained management `binding`,
`shared_namespace_uid`, typed `prerequisites`, the current shared platform's
40-character `foundation_candidate` commit, and five distinct private
`material_files`: `manager_password`, `database_name`, `ca_pem`,
`secret_store_master_keys`, and `cloud_credentials_json`. Operator access and
ingress configuration come from the original private inputs, not new workload
credentials. Do not place these private values in the public operation metadata.
The new foundation pin qualifies the current shared deployment; do not rewrite
the original input or historical ingress candidate pins after a publication.

To prepare those shared inputs without an unprotected Kubernetes operation, run
protected `nebius-rollout` with `operation=inspect` and
`prepare_shared_inputs=true`. Its fixed gateway collector verifies the shared
namespace and cluster identities against both inspection and the retained original
management configuration. It reads the shared ConfigMap, service Deployment,
database/auth Secrets and namespace identities; it makes no Kubernetes writes.
The gateway retains configuration/profile/public keyring, resource UIDs, database
name, database CA and secret-store keys under the private
`.loom/nebius-management/shared-input-observations/<observation_id>/` directory.
It never copies the database administrator password, JWT keys or whole Secrets.
The same snapshot additionally retains `pool-resources.json` for cutover input
preparation: actual Deployments, CronJobs, StatefulSets, Services, ConfigMaps,
Roles and RoleBindings in the configured shared/execution/build namespaces,
their namespace identities and complete ClusterRole/ClusterRoleBinding lists.
Only identity/version pins for fixed database Secrets and an identity/version/
SHA256 pin for the fixed collector credential are recorded, not their contents.
The fixed shared `loom-platform-storage` Secret also supplies an
`application_source_credential` UID/version pin and SHA256 of canonical JSON
containing only its source access/secret keys. No source credential bytes are
exported. The capture rejects malformed material or a changed Secret generation;
the protected builder cutover independently rereads and qualifies the same pin
against the actual shared control-plane source consumer before delivery.
Collection count/size limits and missing pages fail closed. Typed list entries
inherit omitted Kubernetes kind/version fields from their collection; conflicting
types are rejected. The private resource snapshot is not atomic and grants no
authority: the protected cutover must requalify the selected live resources and
permissions. Do not publish its raw configuration, workload or RBAC documents.
Only the observation UUID and candidate commit return to Actions. Ordinary
inspection does not collect credentials. Select that private snapshot when
preparing upgrade inputs; the upgrade still checks it against live consumers.
The deployed SSH wrapper must authorize the exact reviewed collector bytes and
fixed cluster/namespace identities. A kubectl-only forced command cannot execute
this collector. Install a hash-bound, read-only collector grant through the
approved operator route, preserving the existing kubectl restrictions and a copy
of the previous wrapper. Never replace the protected key with an operator shell
key; a collector source change requires reviewing and updating its exact grant.

Use the captured shared configuration for the new application binding. Its guest
execution target reference may have advanced since management bootstrap; the
upgrade qualifies that reference against the current shared ConfigMap without
changing the target. Other retained foundation fields, management identity,
database/storage, routes, credentials and original input bytes remain fixed.

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
groups may belong to that same tenant or the shared project; their exact IDs must
have no IAM permits and object-policy access only to the bound development buckets. Qualify
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

### Retire superseded pre-execution allocations

After the shared-data upgrade, blocked legacy full-stack creates may still hold
compute reservations. Do not restart the legacy provisioner, increase the platform
allowance to hide those reservations, or edit the database directly. The protected
`management-preflight` and `management-install` actions also accept the fixed
`loom.nebius-management-retirement-operation.v1` operation. It runs only explicitly
bound retained-destroy operations in a one-shot Job, never the create queue.

First refresh the ordinary owner's environment status and protected namespace
inventory. This path supports generated personal environments whose original create
never delivered credential material. Prepare the executor and recovery evidence
before requesting owner `destroy_retained` with the observed generation and a stable
idempotency key. Retain the returned operation ID; a request alone does not release
capacity. Delivered-material environments are rejected and need a separately scoped
retirement route, not additional credentials mounted into this Job.

Keep separate `nebius-management/retirement/{inputs.json,state,anchor}` paths and
an exact-bundle SSH grant. Never overwrite the bootstrap or upgrade inputs, journals,
anchors or grants. Private inputs use
`loom.nebius-management-retirement-private-inputs.v1` and contain:

- The original `upgrade_operation`, SHA-256 of its completed `upgrade.json`, and
  SHA-256 of its active `switch/switch.json`. All recorded phase hashes must match.
- The unchanged upgraded `deployment` (only additive publication references are
  allowed), and authenticated `candidate`, `profile` and `candidate_id`. The
  operation's source SHA must equal this published candidate: its service image
  must contain the retirement runtime.
- Exact `targets`: the new `operation_id`, original `source_operation_id`, complete
  destroyed-generation `registration`, and `namespace_uids` for all three retained
  namespaces. Owner, incarnation, generation and resource identities are rechecked
  against the registry before any operation is claimed.

Preflight requires the exact upgraded management Deployment, no legacy-provisioner
Pods, and unchanged policy/binding UIDs and full configuration from the retained
upgrade fence journal. The Job uses a dedicated service account with target-namespace
controller permissions, exact Namespace reads, projected Kubernetes identity and
the management service database credential. It receives no cloud, administrator or
secret-store master credentials and no PVC, Secret or Namespace deletion authority.

Cleanup closes Pod admission, retains stopped/suspended controllers to fence delayed
creates, and verifies owned workloads are idle. Existing retained-destroy completion
releases CPU, memory and ephemeral-storage reservations while retaining storage
charges, names, namespaces, PVCs and business data. No retained data is deleted.

`pending` is not success. Reinvoke the same qualified operation and preserve its
state; `management_retired` requires the exact recorded Job to complete. A failed
Job, replaced resource, lost journal or uncertain write blocks automatic recovery:
preserve the Job and both journals, inspect the exact owner operation and protected
inventory, and repair the demonstrated cause before authorizing a replacement.
There are no automatic Job retries or replacement creates. Finally read back each
owner operation as completed, verify the released reservations, and retry the
original personal-application intent. HTTPS/login and suspend/resume still require
their own acceptance evidence.

Protected `inspect` includes `failed_retirement_jobs`: exact Job/Pod identities,
container exit code/signal and allowlisted termination reason, plus a sanitized
retirement result or import-error type when available. It reads at most three
failed retirement Pods and bounded log tails; raw logs, exception messages and
credential values are never exported. `retirement_blocked` alone does not identify
the cause, and inspection never retries or replaces the failed Job.

For `retirement_blocked`, inspection also attempts a fixed registry probe through
the matching, ready management service Pod. It qualifies the failed Job's immutable
target configuration and the manager's installation, service account and Pod UID.
With protected retirement-operation metadata configured, inspection compares both
live UIDs and normalized configurations with the original private `retirement-job`
create receipt. The fixed gateway probe reads that receipt in place and emits only
a match result; it does not edit or create journals. Installation, management
namespace and cluster namespace UIDs must match. A replacement, incomplete receipt,
or mismatched configuration remains unavailable, without a timestamp fallback.
The inspection SSH wrapper needs a separate exact-source, read-only grant for
`nebius_retirement_journal_probe.py`, bound to the retained state path and original
receipt identities. Install it through the same approved operator route as the
shared-input collector, preserving existing grants and the previous wrapper.
A collector-only or kubectl-only grant does not admit this probe; never replace
the protected inspection key with a general operator key.
Without protected metadata, the immutable ConfigMap must strictly predate the Job;
missing or equal-second timestamps remain unavailable rather than ambiguous.
PostgreSQL enforces a bounded read-only, repeatable-read transaction; the probe
performs only fixed SELECTs, never claims an operation or changes reservations.
`registry_probe` exports named boolean checks against the retirement qualification
contract, not database rows or credential material. An unavailable probe is not an
empty or successful registry. Fixed `stage` and `error_type` values distinguish
configuration/creation-lineage, manager selection, exec and output failures;
`api_reason`, when present, is an allowlisted Kubernetes failure code, not its
message. These labels do not relax the qualification checks. Even all-true checks
describe a current snapshot, not the failed Pod's network/credential behavior or
permission to retry cleanup.

### Diagnose startup in the original retirement runtime

When operator/manager reads cannot establish the failed Pod's mounted credentials,
database TLS or network access, use `management-diagnostic-preflight` and
`management-diagnostic-install` through protected `nebius-rollout`. These actions
select separate `NEBIUS_MANAGEMENT_DIAGNOSTIC_OPERATION_JSON` and
`NEBIUS_MANAGEMENT_DIAGNOSTIC_SSH_KEY` configuration. A missing diagnostic key
cannot fall back to retirement authority. Preserve the original management
metadata, SSH grant, inputs, failed Job and every journal.

The operation schema is
`loom.nebius-management-retirement-diagnostic-operation.v1`; its exact integrated
`source_sha` supplies diagnostic tooling while `candidate` stays on the original
retirement candidate. Use separate
`nebius-management/retirement-diagnostic/{inputs.json,state,anchor}` paths and an
exact-bundle SSH grant. Private inputs use
`loom.nebius-management-retirement-diagnostic-private-inputs.v1` and bind the
original `retirement_operation`, `retirement_state_sha256`, and
`retirement_journal_sha256` hashes for `permissions`, `network` and `job`.
Original private receipts and current resource UIDs/configurations must match;
the original Job must remain failed and inactive.

The fixed diagnostic creates one separate Job, retaining the original pinned
image, settings/credential mounts, projected identity, security/scheduling and
network-policy labels. Only its name, checked-in read-only command and bounded
deadline differ. It reads exact target operations in a PostgreSQL-enforced
read-only transaction and performs exact Namespace GETs. It cannot claim or
reconcile operations. Repeating the qualified diagnostic reads the same Job;
lost state, replacement resources, failed Jobs or ambiguous Pods never trigger
a retry or replacement create.

`retirement_diagnostic_observed` means a bounded, verified report was obtained,
not that retirement succeeded. Check `probe.status` and `probe.stage`: settings,
database binding, Kubernetes CA/token, database, Namespace GET/identity, or
complete. Failures export only allowlisted error types/statuses, never raw logs
or credential values. Even a complete probe proves only current startup access.
Choose recovery from the exact operation/resource evidence, retain the failed
Job and journals, and qualify the recovery separately. Reservation release and
personal HTTPS/login/suspend-resume still require their own evidence.

If readback rejects the completed diagnostic Pod, fixed `diagnostic_pod_*` and
`diagnostic_container_*` stages identify the failed check. After checking the
current Job receipt and unique Pod identity/owner, the reader retains the first
Job/Pod pair privately in `state/pod-observation.json` (owner-only, at most 2 MiB).
This file can contain sensitive configuration/status: keep it on the protected
gateway or in private operator evidence, never in CI artifacts or public logs.
It is diagnostic history, not acceptance or recovery authority. Existing evidence
is not overwritten, and every current label, template, security, termination and
final readback check still applies. This capture adds no Kubernetes requests and
does not rerun or replace either Job.
The reader qualifies one observed Nebius runtime addition:
`topology.kubernetes.io/region`, only when absent from the recorded Job labels
and exactly equal to the frozen foundation's configured region. Recorded labels
cannot be replaced; missing, changed or other additional policy labels still fail.

### Recover a qualified native-DNS retirement failure

When the retained diagnostic reports `database` / `OperationalError` and the
current `kube-system/coredns` Service uses `k8s-app=coredns`, use the separate
`management-recovery-preflight` and `management-recovery-install` actions of
protected `nebius-rollout`. They select only
`NEBIUS_MANAGEMENT_RECOVERY_OPERATION_JSON` and
`NEBIUS_MANAGEMENT_RECOVERY_SSH_KEY`; neither the original installation nor the
diagnostic key is a fallback. Preserve both old Jobs and all original grants,
inputs, receipts and anchors. Do not resubmit owners' destroy requests.

The operation schema is
`loom.nebius-management-retirement-recovery-operation.v1`, with exact integrated
`source_sha`, the unchanged original `candidate` and separate
`nebius-management/retirement-recovery/{inputs.json,state,anchor}` paths. Private
inputs use `loom.nebius-management-retirement-recovery-private-inputs.v1` and
contain only the existing `diagnostic_operation`, its
`diagnostic_journal_sha256`, and the current `dns_service_uid`. Qualification
reuses the original private-input chain, failed/inactive Job, manager/fence and
namespace identities, and the completed diagnostic's strict Pod/report readback.
It freshly checks the DNS Service UID and requires `k8s-app=coredns` in its
selector before writes. Additional selector entries are permitted: Kubernetes
combines them with AND, narrowing the same native DNS Pod set. A missing or
changed native selector still blocks; this does not change DNS policy or grants.

The exact-bundle recovery adds one DNS-only NetworkPolicy and one deterministic
Job. Only the recovery Pod receives the added TCP/UDP 53 permission to DNS Pods
in `kube-system`; the original rendering, image, identity, settings, mounts,
security and scheduling remain unchanged. Before calling the existing retirement
runtime, it reads every target and requires pending state, no previous claim,
lease, error or effects. It never writes reservations directly or runs the create
queue. Both objects share one intent-before-create journal; replay reads their
recorded identities, never replaces a failed, missing or foreign Job.

`pending` means to read the same operation again. `retirement_recovered` requires
a verified completion report showing the exact original operations completed,
leases/errors cleared, CPU/memory/ephemeral reservations released and storage
unchanged. Job exit zero alone is not success. A `blocked` result at
`recovery_runtime` retains the closed `recovery` report, including whether
retirement may have started, but never raw exception text. Preserve uncertain
effects and investigate; do not reset state or automatically create another Job.
Only proven release permits retrying the recorded first-application intent;
personal HTTPS/login/suspend-resume remains a separate acceptance gate.

### Qualify personal object-access retirement

Before accepting a provider's retirement protocol, use an ordinary application
create to establish successful signed read-only probes for all protected data and
source buckets, then suspend that generation and verify its original-key denials
after exact IAM absence. Check that a sibling application and shared data remain
available. The active-start qualification runs those positive probes without
additional object permissions, new buckets, policy changes or secret export.

Nebius can return structured `AccessDenied` for a nonexistent key. That response
alone is not retirement proof: completion requires the existing exact account,
key and membership deletion readbacks plus signed denials in every original scope,
SQL retirement and stopped-workload evidence. Do not substitute HTML/generic403,
transport failure, a synthetic missing-key experiment or a successful Job.

For an already-deleted historical key, retain its encrypted material and immutable
operation history. The installation's original-upgrade-rooted, unchanged storage
scope supplies legacy source-bucket binding; never invent a new frozen plan or
recreate a deleted key. Complete a separate ordinary application's positive-create,
suspend and original-key denial cycle before explicitly retrying the historical
blocked retirement. Missing
material, incompatible scope or unresolved provider effects remain blocked.

## Protected shared-pool cutover

Use the protected `nebius-rollout` actions `management-pool-preflight`,
`management-pool-install` and, when recovery is explicitly selected,
`management-pool-rollback`. These operate on a complete fixed migration, not
individual stages or caller-supplied Kubernetes commands. Source support does
not establish that the pool has been installed or accepted on a particular cluster.

Before preparing authority, obtain fresh installed inventory and complete the
ordinary protected platform/management schema upgrades and their backup proofs.
Pin all production, staging and shared-development participants, dormant consumers,
actual namespace/workload/credential identities, provider pool and quota scope,
effective writer permissions, and the protected candidate/runtime publication.
Preserve the original management upgrade and immediate completed predecessor.
When the standalone foundation's web, gateway and backup share the control-plane
ServiceAccount, retain those exact Deployment/CronJob observations in the private
`platform_consumers` roster. They must match the predecessor's foundation namespace
and the selected candidate's rendered templates. Their UIDs/templates are checked
without stopping or changing them; do not add unknown schedulers or execution
writers to this roster. A candidate mismatch requires a normal protected platform
rollout and fresh observations, not rewriting live image or revision fields.
Participant PostgreSQL StatefulSets need no extra roster entry: preflight uses
their existing migration database bindings as read-only roots, checking exact
UIDs/templates and same-namespace/account Pod ancestry without stopping the database.
Completed standalone migration/configuration/predeploy Jobs need no roster entry
or cleanup. Preflight qualifies only native singleton terminal platform Jobs with
automatic UID selectors and complete terminated-Pod evidence from the same API
snapshot. Active Jobs, incomplete container status, ambiguous ancestry, custom
controllers and execution/build identities still block; do not bypass this by
dropping terminal Pods from the inventory or adopting unknown roots.
Include both configured execution-only guest targets, including the emulated-auth
target, in the same participant's retained actuator roster and execution profiles.
They share the ordinary collector and capacity authority; do not omit a running
sibling or classify it as dormant merely because its originating issue is closed.

Use a new nonzero UUID and private
`nebius-management/pool-cutover/<uuid>/{inputs.json,state,anchor}` paths. The
`loom.nebius-pool-cutover-private-inputs.v1` contract contains that complete
retained scope, installation/catalog, dedicated machine-token file references
and original/predecessor selectors. New builder-enabled preparations must set
`source_delivery_version` to `v2`, selecting the canonical private source spool.
An omitted version retains the historical `v1` contract for exact evidence
reconstruction; ordinary forward installation refuses it. Do not edit an existing
operation's version, candidate or journal to substitute newer rendering.
Keep those inputs and credentials on the
operator host. The public `loom.nebius-pool-cutover-operation.v1` metadata contains
only `operation_id`, identical integrated `source_sha` and `candidate`,
`installation_id`, `namespace`, the three private paths and `inputs_sha256`.

Prepare the exact integrated tooling bundle with the existing management rollout
builder, then preview/apply its dedicated grant through
`install_nebius_management_entrypoint.py` and the existing operator route.
Configure protected `NEBIUS_MANAGEMENT_POOL_OPERATION_JSON` and
`NEBIUS_MANAGEMENT_POOL_SSH_KEY`; no bootstrap, recovery or refresh-key fallback
is permitted. The installer preserves other SSH grants and private state, pins
the bundle digest, and allows rollback's fixed `loom-nebius-pool-rollback-v1`
command only for the pool grant. It does not install cluster resources itself.

Preflight reloads the private inputs and qualifies current retained scope without
mutations; it is not a runtime-readiness certificate. Installation closes intake,
retires and fences old writers, stages the fixed successor, starts it closed,
qualifies runtime/capacity and permissions, then opens global admission and
releases local guards. Each mutating pass shares one operation lock, while child
journals retain their original locks and uncertain-write observation rules.

Connection failures retain fixed diagnostic stages for publication, operator
readers, runtime databases, runtime telemetry, management database, provider,
connected scope and changed private inputs (each prefixed `pool_`). Unknown
failures remain `pool_connection`. These codes contain no exception text,
credential, resource payload or retry authority; investigate the identified
prerequisite before another operation. They do not change installation ordering
or authorize retries of uncertain writes.

After connection, known preflight failures retain `pool_preflight_` categories:
`writer_bindings`, `writer_workloads`, `connected_prerequisites`, `capacity`,
`scope`, `database_report`, `pending_source`, `pending_page`, `origin_history`,
or `database_readiness`. These classify existing checks, not additional authority
or successful qualification of earlier checks. Unknown failures remain
`pool_preflight`; install and rollback failures keep their journaled phase.

Telemetry failures further identify fixed `pool_runtime_telemetry_...` categories:
binding, Pod, node inventory, probe delivery, identity recheck, settings, client
construction, TLS, authorization, network, HTTP, reader, counters, cleanup,
node identity/address, bearer or local trust authority, or response payload. These
are bounded diagnostics, not raw exceptions or statistics. TLS diagnostics identify
the Kubernetes API (`tls_api`) or direct kubelet (`tls_kubelet`) transport when the
bounded exception chain establishes it. Certificate-verification failures retain
only an integer OpenSSL verification code in 0–255, for example
`pool_runtime_telemetry_tls_kubelet_verify_20`; `tls_unknown_verify_<code>` means
the transport was not established. Without qualified details, the diagnostic stays
at the transport category or legacy `tls`. No exception text, URLs or certificate
contents are returned. The fixed in-Pod probe can exit zero to deliver a `blocked`
diagnostic; that exit code alone never qualifies telemetry.

Only positively identified direct-kubelet sampling failures (`tls_kubelet`,
`tls_kubelet_verify_0` through `_255`, `kubelet_authorization`, `kubelet_network`,
`kubelet_http`) and missing/invalid counters (`counters`) become optional warnings.
The protected pool result includes `telemetry`, for example:

```json
{"status": "unavailable", "checks": 2, "unavailable": 1, "reasons": ["tls_kubelet_verify_19"]}
```

Counts represent the latest checks per actuator against the current pool Nodes and
its host, not unique machines. `available` requires successful probes; zero checks
is `not_observed`. Historical results without this field provide no availability
evidence. Both protected report filters preserve it through startup, activation
and legacy recovery. A sampling warning still requires post-probe runtime, Node
and contract rechecks. API failures, unknown/ambiguous errors, malformed probe
output, wrong-node summaries, unqualified addresses or TLS/bearer configuration,
and client-close failures remain blocking. No failed TLS response is used as data.

Missing detailed samples do not block execution or cleanup and must not be reported
as zero usage. Scheduling/capacity readiness uses authenticated inventory, requested
resources, provider quota and reservation accounting. Canonical inference usage and
complete resource-calibration evidence retain their existing requirements. This
separation does not claim a durable kubelet certificate-refresh mechanism is installed.
Keep the same TLS, node-identity and statistics checks when investigating; no
node-proxy fallback, additional permissions or automatic retries are introduced.

`pending` means reconcile the same operation and named phase; it does not permit
recreating an uncertain resource or resetting evidence. Replays select the newest
recorded phase. Shutdown receipts optionally include a fixed `pending_reason`:
`pending_pool_cleanup` waits for journal drain, `pending_shutdown_update` records
a rejected stop preview/update, `pending_shutdown_outcome` requires readback of
the original uncertain stop, and `pending_successor_drain` waits for stopped
successor processes to disappear. Older receipts without a reason do not identify
which barrier is pending. These diagnostics do not authorize a new write or retry.
Explicit rollback requires the completed closed cutover, fences
global admission first, settles startup writes and drains effects, stops successor
processes, revokes machine authority, restricts gateway permissions, restores the
original templates/roles, then proves legacy runtime readiness before reopening
owners. Recovery does not rerun closed-mode drain checks against already reopened
owners. A pre-closure failure retains its original recovery evidence and cannot
use later rollback stages to bypass that boundary.

`pool_cutover_completed` binds the UUID, `global` or `legacy` outcome and
`completion_sha256`, with `acceptance_verified: false`. Preserve this immutable
receipt and all predecessor/phase evidence for subsequent management refreshes.
A completed global outcome cannot be rolled back by rewriting that same history;
a new transition needs new protected authority. Actual concurrent-owner builds,
tasks/results, isolation, teardown/redeploy and scale-to-zero are separate live
acceptance requirements.

### Repair an original source-spool initializer before opening

For a historical `v1` application delivery stopped at runtime qualification, use
the separate protected `management-pool-repair-preflight`,
`management-pool-repair-install`, and `management-pool-repair-rollback` actions.
This is a fixed source-spool correction, not an arbitrary manifest patch or a
replacement pool registration. All startup writes must be settled, admission
opening and guard release must still be prepared, and there must be no recovery
or completion descendant when repair is first anchored.

Prepare a new `loom.nebius-pool-startup-repair-operation.v1` envelope with a
distinct nonzero `operation_id`, the retained `original_operation_id`, and a new
integrated `source_sha` equal to its tooling `candidate`. Its private paths are
`nebius-management/pool-repair/<uuid>/{inputs.json,state,anchor}`. The private
`loom.nebius-pool-startup-repair-private-inputs.v1` document contains the exact
original operation metadata and a `binding` of the new operation/source to the
original metadata's canonical JSON SHA-256, original input SHA-256, and retained
`cutover.json`, `startup.json`, and prepared `activation.json` SHA-256 values.
Original inputs, image digests, pool identity, credentials and journals are not
replaced. Preserve the original authority bundle as evidence.

Build and byte-qualify the new tooling bundle, then install its grant through the
reviewed operator-only installer. Bind its metadata to
`NEBIUS_MANAGEMENT_POOL_REPAIR_OPERATION_JSON` and its dedicated key to
`NEBIUS_MANAGEMENT_POOL_REPAIR_SSH_KEY` in the protected environment. Neither
value falls back to the original pool or management authority. Preflight performs
read-only qualification; install takes the original operation's dispatch lock.

Repair stages one immutable application ConfigMap, stops and proves actual Pod
drain for only the manager, changes the fixed source initializer and spool path
to `/run/loom-application-source/spool`, then restarts it. Each write has durable
intent and exact object identity/version checks; an uncertain response permits
readback, not a new write. The original prepared activation bytes are retained in
the repair anchor. Normal runtime, settings, capacity and gateway checks must
then pass before the original activation can advance and open admission.

A `pending` repair result or `pool_startup_repair` failure is not readiness.
Retain all evidence and inspect the named phase. Repair rollback fences uncertain
manager writes before the existing stop/restore/reopen sequence. Completion
records the repair ancestry and both operation identities in the protected
report, but still reports `acceptance_verified: false`. Collector failures and
concurrent-owner acceptance require their own evidence; a repaired manager is
not proof that either has passed.

If retained original tooling already wrote rollback evidence after repair entry,
keep those bytes. Updated recovery recognizes its original empty startup fence
only while repair stop is prepared or unresolved and template/start have not
begun. Shutdown must prove the exact manager stopped at a changed object version
before restoration; a late stop is observed, not dispatched twice. An original
legacy completion receipt stays unchanged, with the separately qualified repair
ancestry included in historical loading. Later repair phases or altered old
evidence reject this compatibility path; do not reset either journal.

### Correct the manager image before first opening

If source delivery is qualified but the manager image cannot start, use the same
three protected pool-repair actions with a new
`loom.nebius-pool-startup-repair-operation.v2` envelope and dedicated grant. Do not
change the original cutover or source-repair inputs. This transition changes only
the manager's main and initializer image references, not configuration, Secrets,
execution images, pool registration, permissions, application releases or schema.

The private `loom.nebius-pool-manager-image-private-inputs.v1` document contains
`original_operation` and `binding`. The binding carries the original operation,
input, closure, settled-startup and prepared-activation hashes used by source
repair, plus `ordinal`, `source_repair_sha256` (null if none),
`predecessor_sha256` (null for the first image correction), and the exact
`publication`, `candidate` and `profile`. Source repair, if present, must be
complete. A new correction requires closed admission, held guards and no
recovery or completion descendant.

The tooling source and protected publication source must be the same integrated
commit. The bundle builder derives its single Alembic head from that source and
binds `manager-schema.json` in the immutable bundle. Entry requires the existing
pool's manager revision `0174`; a different head requires a separate migration,
which this action cannot perform. The retained publication reader and keyring
must verify the new image before operator connections are opened.

Install uses the original dispatch lock, first adds a Deployment-only metadata
isolation marker, stops the manager, proves actual Pod
drain, replaces only image references using exact identity/version/template
checks, and restarts it while removing the marker. The marker changes no Pod
template, but makes retained older tooling reject activation before a delayed
stop can exist. Unknown writes are observed, never resent. An interrupted
local enrollment with only its valid anchor may finish recording the same
all-prepared state; an existing write intent is never reset. Up to eight completed
corrections can form an append-only chain before first opening. A pending tail
cannot be replaced by a new operation.

Only the bound image continuation may advance installation afterward. Normal
runtime qualification and activation remain mandatory. The existing rollback
fences any outstanding image write before shutdown and restoration. Completion
preserves all correction records/anchors and supplies the final manager image to
later refreshes, while keeping the original execution profile. A successful image
correction alone is not installed multi-owner acceptance.

Older rollback tooling may save its fence before the first isolation request
commits. Updated recovery accepts those unchanged bytes only before any image
stop/template/start intent. Shutdown removes a late isolation marker and proves
a changed object version, invalidating any still-delayed isolation request. If
the isolate invalidated an older pending shutdown CAS, its original version is
retained and a separate nested stop intent is recorded; an uncertain nested stop
is readback-only. Existing legacy completion receipts remain unchanged while
historical readers include the separately qualified image ancestry. This narrow
compatibility path cannot cover a later correction or destructive image phase.

### Correct selected pool runtime images before first opening

After a completed manager image correction, the same protected repair actions
accept a new `loom.nebius-pool-startup-repair-operation.v3` envelope. Its private
`loom.nebius-pool-runtime-image-private-inputs.v2` document retains
`original_operation` and the previous image-binding fields, adding binding
`schema_version: loom.nebius-pool-runtime-image-binding.v2` and exactly one
`target`: `gateway` or `collector`. The target's name and UID cannot be supplied
by the caller. The gateway comes from its retained creation receipt; the collector
is the registered development participant's pooled collector. Gateway images use
the publication's `service` component; collector main and initializer images use
`execution_actuator`, not `control_plane`.

This appends to the same eight-entry `manager-image-NN` ancestry. The first entry
must remain a completed legacy manager correction; old binding bytes are unchanged.
Each entry preserves the latest image for every other corrected workload. Old
image-aware tooling rejects the additional target/version fields before writing;
an already-frozen cancellation or recovery prevents new enrollment. The narrow
first-manager legacy rollback exception above does not apply to targeted entries.

Gateway repair retains the existing Deployment stop/drain/template/start sequence.
Collector repair changes only CronJob suspension and Pod-template images, using
exact UID/resourceVersion/metadata/spec checks. Suspension stops future scheduling
but is not an atomic acknowledgement from the CronJob controller: an already
dispatched old Job may arrive late. Each drain check reads complete Job and Pod
collections, requires terminal owned Jobs and terminated containers, and refuses
to advance when an active child is observed. It does not delete Jobs, claim atomic
quiescence, change collector credentials, or give the collector execution authority.

Normal activation still requires fresh capacity evidence under the existing pool
transaction. For installed verification, observe a newly scheduled, successful
collector Job and its Pods using the corrected image; a patched CronJob alone is
not proof of execution. Preserve all original execution profiles and configuration.
Protected `inspect` exposes `pool_startup_diagnostics.collector_completion` when
it observes a successful Pod and Job bound to the current pooled CronJob template.
Compare its controller UID with the retained collector and its `image_sha256`
with the selected publication's execution-actuator digest. The observation includes
Job/Pod identities, checks successful main and initializer containers, and rejects
readback drift; `null` is not success. Only the digest is exported, not image URLs,
configuration or logs. This readout is evidence to inspect, not activation authority.
Cancellation fences the outstanding target's image write; shutdown and completion
retain the full mixed-target image history. This remains image repair, not
permission to open admission without qualification or a multi-owner acceptance claim.

### Continue an anchored image repair with corrected tooling

If an enrolled image repair needs a tooling fix, do not replace its private inputs,
image binding, phase record or anchor. Use the same protected repair actions with a
new `loom.nebius-pool-startup-repair-operation.v4` envelope, operation UUID and
dedicated grant. Its source and candidate select the exact new integrated tooling
commit. The new private `loom.nebius-pool-image-tooling-private-inputs.v1` document
contains only `schema_version` and `repair_operation`: the retained v2 or v3 public
repair metadata, including its original private-input path and SHA-256.

The loader rereads and verifies that original private file, requires the existing
latest image-repair anchor, and retains its exact binding, image publication,
execution profile and journal. A tooling continuation cannot enroll a new image
repair, reference another tooling continuation, or change the installation,
namespace or original pool operation. The new bundle must independently prove
schema revision `0174`; the original image publication must still qualify. It
uses the original dispatch lock and the same fixed image-update/activation paths.
Unknown writes remain readback-only. Current identity, metadata, spec and version
checks remain mandatory, including when a fresh observation replaces a stale
pre-preview snapshot. Subsequent image corrections still require a completed tail.

For an operation already cancelled or carrying recovery journals, preflight
qualifies the retained recovery chain and current workload options. It does not
attempt forward-only closed-image qualification on an incomplete image tail.
Forward repair still requires that qualification; recovery preflight neither
completes an image correction nor authorizes reopening intake.

Shutdown finishes its current recovery/drain checks before taking the final
workload snapshot. The fixed adapter verifies unchanged UID and stable metadata
and spec, records that snapshot's resourceVersion through the stage's write-ahead
callback, then sends the full UID/version/metadata/spec CAS. Controller status
updates during the earlier checks therefore do not force reuse of a stale version.
A definite rejection may return the new attempt to prepared. An existing unknown
intent retains its original version and is only observed, never refreshed or resent.

## Refresh the retained application manager

After the one-time application-runtime upgrade has completed, use protected
`nebius-rollout` actions `management-refresh-preflight` and
`management-refresh-install` to change the manager's software and compatible
release catalog. Do not replay bootstrap or overwrite the upgrade's inputs or
receipts. Finish any recovery pinned to the old manager before refreshing it.

Each refresh uses a new canonical, nonzero operation UUID and private
`nebius-management/refresh/<uuid>/{inputs.json,state,anchor}` paths. Preserve the
original installation and all prior operation directories. The private input
schema is `loom.nebius-management-refresh-private-inputs.v1`, with:

- `original_upgrade`: the completed original upgrade selector and receipt hashes.
- `predecessor`: that same upgrade selector, the completed shared-pool cutover,
  or the immediately preceding completed refresh selector. A refresh selector binds its operation UUID, private-input
  digest and completion receipt digest; it does not accumulate an unbounded chain.
  Receipt qualification compares Kubernetes resource quantities numerically
  (for example, `100m` and `0.1`) without rewriting frozen receipt bytes or
  accepting changed resource amounts or other runtime configuration.
- `pool_baseline`, when inherited from a completed cutover: the exact qualified
  pool selector. Keep it through subsequent refreshes; omitting it cannot restore
  legacy writer authority. The separately scoped pool reader requalifies workloads,
  credentials, mode and effective permissions around refresh writes and completion.
- `deployment`, `candidate` and `profile`: the target manager configuration and
  protected publication. The tooling source and candidate SHA must be identical.
- `manager_revision` and `target_manager_revision`: the expected management DB
  revisions before and after its migration. These are separate from the exact
  shared-development schema in the selected application configuration.
- `prerequisites` and `foundation_candidate`: the existing current-shared-runtime
  prerequisite contract and its protected publication source.
- Optional `supersedes`: the failed pre-migration refresh's complete `operation`
  metadata plus `refresh_sha256` and `switch_sha256`, binding its parent and
  cutover journals. Omit this for an ordinary refresh.

Prepare the exact integrated source bundle and install its dedicated forced-SSH
grant with `install_nebius_management_entrypoint.py`, as for the original manager.
Use a separate key and set protected `NEBIUS_MANAGEMENT_REFRESH_SSH_KEY` and
`NEBIUS_MANAGEMENT_REFRESH_OPERATION_JSON`. The latter uses schema
`loom.nebius-management-refresh-operation.v1`: the ordinary source, candidate,
installation, namespace, private-input digest and path fields, plus `operation_id`.
The workflow has no fallback to bootstrap, diagnostic or recovery credentials.
Authority installation preserves other grants and does not create private inputs
or reset operation state. Private credentials never pass through Actions.

Preflight qualifies the completed predecessor, retained resource identities,
publication and live shared prerequisites. Installation serializes on the original
installation lock, stages only fixed operation resources, and proceeds through:

1. Stage the immutable target configuration and scale the retained manager to zero.
2. Observe current Deployment **and** owned ReplicaSet generations, zero replicas,
   and complete manager Pod absence, including terminating Pods.
3. Run bounded, read-only manager/shared DB compatibility probes using ordinary
   namespace-local service credentials. Preserve pending plans, leases and effects.
4. Execute and verify a manager backup, including object byte/checksum readback,
   then run management-only migrations and the post-migration compatibility probe.
5. Requalify the saved evidence, activate the exact new image/configuration, and
   verify the actual current Pod/controller plus authenticated public HTTPS and
   application-provisioner readiness.

When retained preflight fails, the closed `stage` field preserves the failed check,
such as `refresh_resource_inventory`, `refresh_persistent_storage`,
`refresh_shared_material`, `refresh_publication` or `refresh_cloud_identity`.
Unknown details retain a coarse stage. These codes expose no resource contents or
provider messages and do not authorize retrying a blocked installation.
For `refresh_platform_capacity`, an optional closed `capacity` diagnostic
distinguishes rendering, inventory, controller decoding/counting, placement and
resource fit. For eligible nodes it reports only node UUIDs and numeric CPU,
memory, ephemeral-storage and Pod-slot totals. No Pod configuration, credential,
provider response or exception message is exported. A capacity-stage failure is
not by itself proof that larger machines are needed: inspect this report before
changing resources. Missing or invalid details stay coarse, and no qualification,
write or retry behavior is relaxed.

The operation retains Deployment and credential identities, storage, routes,
permissions and budgets. It does not migrate the shared business database, create
IAM/RBAC/network grants, or admit arbitrary manifests or commands. A candidate
requiring an incompatible runtime setting needs a separately designed operation.

`pending` means reconcile the **same** operation. `management_refreshed` is bound to
its UUID and requires all runtime/public proofs; Job success alone is insufficient.
Completed replay is read-only qualification. Preserve ambiguous writes and missing
or changed evidence; never reset a journal or blindly retry a write. Only a definite
API rejection permits its uncommitted intent to be retried. A failed migration
leaves the manager stopped: there is no automatic rollback onto a changed schema.
Any rollback after migration needs a qualified compatible candidate and a new
operation. Personal HTTPS/login/lifecycle and concurrent-owner acceptance remain
separate checks after a successful refresh.

A terminal failed **manager or shared compatibility probe**, before any backup or
migration intent, can be replaced by an explicit successor refresh. Use a new UUID,
private input path, current integrated source/candidate publication and dedicated
grant; retain the same last successful `predecessor` and original upgrade. Set
`supersedes` to the exact failed operation and journal hashes. Do not edit its
inputs, credentials or receipts, delete its Jobs, or retry its frozen candidate.
The new target must render a different immutable configuration name from every
failed ancestor: the create-only installer cannot reuse their ConfigMaps. An
unchanged target is rejected during input loading, before creating new state.
The successor requalifies the full failed prefix, immutable resource identities,
terminal failed Job and exact stopped manager. Missing journals, uncertain creates,
activation intent or any backup/migration/post-probe evidence make it ineligible.
`refresh_supersession` identifies failure of this qualification, not retry authority.

Under the original installation lock, the successor changes only the stopped
Deployment's operation marker: its UID, image, configuration and zero replicas
remain unchanged. This fences replay of the old operation. It then observes native
drain and repeats **all** ordinary compatibility, backup, migration, activation
and authenticated readiness barriers. A lost write response never permits a blind
second PATCH. Repeated eligible failures require explicit successors, with at most
eight failed ancestors and 128 retained history files; no journal is reset. This
path does not recover a failed migration or restore a manager onto an older schema.

For a failed compatibility probe, protected `inspect` adds
`failed_refresh_probes` for at most three recent failed probe Pods. It binds the
Job owner UID, operation marker, installation, namespace, image and command before
projecting exit status and allowlisted log diagnostics. After validating the
immutable probe settings and fixed service-credential reference, it reads only
the namespace-local `loom-platform-db` Secret to report URL-shape booleans. No
credential, URL, raw log, exception message or configuration payload is exported.
`current_url.status: observed_current` describes the credential currently stored,
not necessarily the value used by the failed Pod. Missing or unqualified evidence
is `unavailable`, not success. This refresh-probe inspection runs no SQL or Pod exec, creates no
resources, and grants no retry, journal reset or manager restart authority.
New probe failures also report only a closed `stage` and `error_type`, separating
settings, database URL, connection, read-only, schema and retained-operation checks.
Both `postgresql://` and `postgresql+psycopg://` service URLs are accepted; exact
role, host, port, database and TLS restrictions remain unchanged.

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

For an existing platform, the deployer checks shared-schema migration readiness
after acquiring or observing the exact rollout guard and before backup or manifest
application. Valid personal application credentials can block a schema change even
with no current database sessions. Resolve `application_database_access_active`
through the authorized application suspend lifecycle, including access revocation
and session drain. A missing application schema guard fails closed as
`application_database_schema_guard_not_installed`; unavailable readiness cannot be
treated as permission to migrate. Bootstrap diagnostics retain only fixed
application-schema `reason_code` values, never raw traceback messages.

To resume a terminal failed ordinary integration rollout with a retained pause,
use protected `nebius-rollout` from `dev` with `operation=recover` and
`recovery_run_id=FAILED_ROLLOUT_RUN_ID`. Follow the
[candidate-bound recovery procedure](nebius-platform.md#automatic-rollout-when-idle).
It retains the failed candidate's published image digests, observes its exact
owner/candidate guard, and retries only its failed Jobs. Failure during recovery
keeps the pause; successful HTTPS and workload readback permits release. Do not
delete the guard, manually edit credential state, disable the schema fence, or
stamp Alembic to make the retry pass. Primary-target replacement and database
restore require their separate procedures.

Every attempt leaves a separate sanitized JSON phase record, including the candidate version, target
and failed phase. Evidence excludes kubeconfig material,
API endpoint details and secret values. A failed attempt is not automatically
rolled back: restore and database-version compatibility must be reviewed against
the retained backup before a downgrade. Reapplying a known candidate is not proof
that a database restore or schema downgrade is safe.

HTTPS health and frontend routing smoke are deployment checks only. User login,
real workload execution, result integrity and fault-recovery acceptance remain the
separate pure-Nebius E2E milestone.

Shared-pool registration is an internal stage of the protected writer migration,
not a manual installation command. Its fixed Job consumes hash-only registration
configuration and leaves admission closed. Preserve its stage journal, ConfigMap,
Job and Pod evidence on failure or a lost response; do not delete/recreate them or
open the pool manually. Successful resource staging is not a successful database
registration or permission to switch controllers. The ordinary management image
refresh does not authorize this migration's credential, configuration or RBAC
changes. The connected protected switchover remains required before deployment.
Its initial closure stage keeps the existing rollout guard in each idle data
environment while waiting for the others. An interrupted attempt must resume with
the same operation, candidate, parent journal and independent anchor. Do not clear
those guards to make the operation appear idle, and do not treat
`pool_registered_closed` as completed writer migration or a usable global pool.

The internal connected cutover parent can additionally freeze the retained
manager/shared APIs, qualify producer and database readiness, retire/fence old
writers and stage dedicated material plus the disabled global runtimes. Its
`pool_runtime_staged_closed` result is not activation or a completed migration.
Preserve the cutover anchor, parent journal, writer child journals and each
material/configuration/authority/workload stage. Recovery after runtime template
replacement must use that parent: replaying the original retirement phase
against changed templates is invalid. A restarted producer, effective extra
writer grant, changed UID or missing evidence stops further mutation. Do not
interpret an idle activity count as an empty future queue. The fixed database
readiness pages also inspect delayed native work and batches awaiting fan-out,
require schema `0173` and reject live application access even with no connected
session. Preserve unknown queued origins: drain through the existing execution
path before cutover instead of rewriting provenance or cancelling unrelated work.
The protected parent still must qualify retained personal origin history in the
management database; the database page alone cannot authorize it. Do not
restore replicas, resume the collector, release guards or change pool mode
manually. Use the complete protected pool operation for activation or rollback;
subsequent manager refresh qualifies its completed pool baseline. These source
connections do not replace successful protected installation and live acceptance.

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
