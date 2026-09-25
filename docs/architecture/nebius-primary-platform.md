# Nebius platform contract

Hosted Loom runs only on Nebius. Platform services, supported Kubernetes
execution, database, object storage, registry, backups and monitoring belong to
that platform. GitHub-hosted CI, image builds and publication remain supported,
as do local development and external user-selected inference APIs.

## Platform boundaries

System services and elastic execution pools have separate capacity policies.
Production, staging and development data boundaries stay separate. Public web/API
access uses authenticated HTTPS; database and execution
management stay private. The [native execution contract](nebius-service-execution.md)
owns target placement, durable attempts, cancellation and fenced publication.

### Personal application and shared development data boundary

The [owner clarification in #1915](https://github.com/qianyi-sun/loom/issues/1915#issuecomment-5835681150)
selects independently versioned personal frontend/API instances connected to one
shared development database, object stores and worker pool. Personal lifecycle
must not own or remove shared data or background services. Production and staging
retain their data and credential boundaries. The v1 managed renderer described
below still provisions isolated child stacks; it has **not** been converted to
this shared-data model. Existing frozen v1 bindings retain their old meaning.

Loom Service supports `LOOM_SVC_SERVICE_MODE=api_only` as a process-level building
block for this model. It serves the same authenticated workload routes as the
default `application` mode and retains schema, secret-store and execution-profile
validation, but starts none of the batch runner, taskset materializer, taskset GC,
provider-secret GC or price-catalog synchronization loops. The shared application
service remains responsible for those workers using their existing claims. An
API-only process closes only its own database engine and HTTP/storage clients;
stopping it does not cancel work owned by a different process.

This setting is not a distributed singleton lock, an authorization boundary, or
a read-only API: authorized requests can still mutate shared state. It does not
bind sessions to a personal origin, select a per-task runtime, provision shared
credentials, or make arbitrary application schemas compatible. Exact schema-head
validation remains required; personal APIs must not independently run migrations.
Legacy isolated-child configuration and management provisioning configuration are
rejected in `api_only` mode. No existing deployment selects this mode implicitly,
and this process capability alone is not installed shared-development acceptance.

The primary region is `eu-north1`. Secondary-region routing remains disabled
pending separate qualification; checked-in regional support is not evidence
that a region is operationally accepted. Current deployment and recovery
procedures are indexed in [runbooks](../runbooks/README.md).

### Application-scoped browser authentication

`LOOM_SVC_AUTH_SESSION_AUDIENCE_JSON` optionally supplies a protected
`loom.application-session-audience.v1` binding: non-nil `application_id`, HTTPS
`origin`, and positive integer `access_generation`. The origin must match the
explicit `LOOM_SVC_PUBLIC_BASE_URL`; local HTTP, management mode and the legacy
managed-child configuration cannot use this binding.

Both one-use login challenges and browser sessions use audience- and
purpose-separated hashes in the existing database columns. A proof issued by
Alice's application cannot be redeemed or used through Bob's application, an
updated access generation, a changed origin, or an unconfigured legacy service.
Wrong-audience redemption does not consume the legitimate challenge. Password
login, invite acceptance, refresh and team switching preserve this boundary.
Host-only cookies and browser-origin/CSRF enforcement remain in place; a request's
Host, forwarded headers or audience metadata cannot select the Service's audience.

Cancellation retains independent Service and Control Plane authorization. The
Service forwards its **configured** audience with the existing cookie/CSRF proofs
on the internal hop; it never forwards a client's claimed audience. The shared
Control Plane checks the corresponding audience-bound session row and normal
expiry, revocation, user, team and CSRF authority. Audience metadata is not a
credential and confers no access on its own. This internal cancellation protocol
does not make the Control Plane a public personal-application endpoint.

Unconfigured installations keep their existing authentication contract. This
opt-in changes neither bearer tokens nor shared user/team/invitation/recovery
permissions and needs no database migration. Account recovery and invitations
remain data-environment operations; only their resulting sessions are app-local.
The hash binding is **not** process revocation: protected lifecycle must stop old
processes and revoke their access when retiring a generation. A cached process
configuration is not a live registration check. No current provisioner supplies
this new binding, and it does not establish installed shared-development readiness.

### Application-only manifest contract

`ApplicationRegistrationV1`, `ApplicationReleaseV1` and
`SharedDevelopmentBindingV1` describe personal applications independently of the
legacy full-environment registration. `render_application` produces exactly a
frontend and API Deployment in `loom-dev-<slug>`, their internal Services and the
personal HTTPS Ingress. It emits no database, PVC, migration, backup, Control Plane,
Gateway, worker or build workload. Adding another application emits no bucket or
policy creation request. Resource accounting includes both rolling-update surge
Pods and zero persistent storage.

The protected shared binding retains the existing development foundation's
namespace, object-store names and runtime profile. Application source and image
digests are independent of that executor profile; rendering does not rewrite its
candidate SHA or task/runtime images to match the personal API. Application and
shared schema revisions must match exactly. These input checks are not evidence
of live schema, publication authenticity, running-process fencing or execution
readiness: their caller must qualify the release and shared binding first.

The API selects `api_only`, the application session audience and shared CP/Gateway
endpoints. Namespace-local Secrets supply its individually revocable database
login and CA (`loom-application-db-…`), object access (`loom-application-storage-…`),
and the **shared** encryption keyring (`loom-application-auth-…`); the renderer does
not create those Secrets. It supplies no admin/worker/batch-runner/JWT-signing or
backup credentials and no workload Kubernetes permissions. The static frontend
receives no secrets. Developer-controlled backend code still has trusted
development-data access, not malicious-code isolation.

Personal NetworkPolicies deny ingress/egress by default, admit API/web ingress
only from the configured ingress controller, and permit API egress to the shared
database/CP/Gateway, cluster DNS and public IPv4 HTTPS (excluding private,
loopback and link-local destinations). This is not a hostname-level HTTPS
allowlist. The renderer never changes the shared namespace's policies: a protected
shared-side admission update is required for these connections to work. It also
does not rename/recreate the existing shared foundation.

No management route or provisioner invokes this renderer yet. Legacy frozen
full-stack operations retain their original meaning. Credential provisioning,
active registration/lifecycle fencing, schema coordination, shared network
admission and source qualification remain prerequisites for activating the new
personal applications; rendered resources alone do not satisfy installed
four-plus-one acceptance.

Application rendering rejects a hostname already used by the shared foundation.
It also rejects use of the legacy environment `namespace_authority` on its own:
that admission contract requires a full-environment identity, not an application
identity. Application IDs must not be relabelled as environment IDs to bypass
that boundary.

Personal DB, storage and authentication Secret references include the application
incarnation and access generation. All API environment references and the DB CA
volume move together. An old Deployment template therefore cannot implicitly
consume newer credentials through a reused Secret name. The protected provider
must still deliver immutable material for that generation, revoke retired DB/object
access and terminate old connections/processes. Naming alone does not revoke a
credential already held by a process. Existing frozen fixed-name plans retain their
historical meaning for cleanup, but must not be activated as proof of generation-
isolated material by the new lifecycle provider.

### Application namespace authority

`ApplicationNamespaceAuthorityV1` is a separate, opt-in protected binding for one
installation, management namespace, cluster and shared development data ID/namespace.
The application renderer requires those shared identifiers to match and emits the
application installation label plus one fixed management RoleBinding. A foundation
may retain legacy authority, but only the explicit application binding grants
personal-application management access; it never copies legacy authority labels.

`render_application_authority` emits fail-closed admission policies before RBAC.
The distinct `loom-application-provisioner` ServiceAccount can create only
restricted, application-labelled personal namespaces for its installation and data
binding. It cannot update/delete namespace identities or adopt legacy/foreign
namespaces. RoleBinding admission permits only its exact resources ClusterRole and
its management ServiceAccount; the frontend/API ServiceAccount receives no grant.

Within owned application namespaces, that role can create/read/patch/delete
Deployments, Services, Secrets, ServiceAccounts, Ingresses and NetworkPolicies,
observe ReplicaSets, and observe/delete Pods. It cannot create Pods directly,
call the ServiceAccount token API, exec into Pods, read global or foreign Secrets, create
roles, or provision PVCs, StatefulSets, Jobs or worker infrastructure. Normal
Kubernetes restricted Pod Security remains the workload admission boundary.
Secret admission permits only ordinary `Opaque` application credential bundles;
denying the token API alone would not prevent the legacy ServiceAccount-token
Secret controller from issuing tokens.
These restrictions do not forbid a custom Deployment template from requesting a
projected workload token. The personal renderer disables token automount and emits
no token projection or workload RBAC, and management must use qualified rendered
inputs. This authority is not a sandbox for arbitrary manager-supplied templates.

For application shutdown, `application_pod_fence` renders the fixed
`loom-application-retired` ResourceQuota with `hard.pods: 0`. Its admission rule
requires namespace-matching application/incarnation/data/install identities, no
quota scopes or scope selector, and a positive deployment generation plus operation
UUID. Updates cannot lower the generation or change the operation at the same
generation. The manager may create only that form and may get/patch/delete only
that quota name; it gains no general quota, shared-namespace or worker authority.

The quota blocks new Pods, including delayed controller creations; it does not
stop existing processes. A provider must observe enforcement, stop routing and
controllers, verify Pod shutdown, and revoke credentials/connections before it
claims retirement or releases resources. Reopening admission requires its recorded
UID/resourceVersion delete preconditions after prior-generation retirement. No
installer or lifecycle worker activates this primitive yet.

This is not an installed management upgrade: the protected installer must create
the distinct management ServiceAccount, verify the policies and their enforcement,
and only then grant bootstrap authority. The manager must still authenticate owners,
qualify immutable application inputs and enforce lifecycle generations. Rendering
RBAC alone supplies none of those controls and does not claim malicious-manager
isolation or authorize any live permission change.

### Shared-side application network admission

`render_application_shared_access` supplies three protected-installer policies in
the shared development namespace. They admit personal API Pods to PostgreSQL
(TCP5432), Control Plane (TCP8080) and Gateway (TCP9100), not to the shared frontend
or API. Each peer requires the installation and data-environment namespace labels,
restricted Pod Security and application/incarnation identity labels **together
with** the `app=loom-service` Pod label. Personal web Pods, other installations,
other data bindings and unlabelled namespaces receive no additional access.
The same policy set admits newly registered developers without per-owner edits.

The renderer validates its authority, shared-data and development-only foundation
bindings. It does not change existing shared service policies, grant shared writes
to the personal manager, or install anything. Activation belongs to the protected
shared installation once credentials and lifecycle admission are ready. Namespace
labels must remain under protected management; these rules are network admission,
not per-user authorization or isolation from hostile development backend code.
Disposable Kubernetes tests exercise actual CNI allow/deny behavior against live
fixture servers; they do not establish installed application readiness.

### Application registration and shared name claims

Migration `0160` adds `nebius_applications`, a distinct management registration
with application/incarnation, owner and shared-data identities, release ID,
deployment/access generations, desired state and retained/purged metadata. It has
no database, execution namespace, PVC or bucket ownership fields. The legacy
environment registration and its operation journals retain their existing meaning.

`nebius_deployment_name_claims` is the common transactional name index: slugs and
public hosts are global; namespace names are unique within a cluster. Migration-
owned invoker-permission triggers project both legacy registrations/reservations
and new applications into that index. A concurrent application and legacy create
cannot each commit the same physical name. Existing namespace reservations are
backfilled as recorded, never inferred from an environment's naming convention.

Retained destruction keeps the claims. Legacy verified purge releases host/slug
but keeps a namespace claim until its original namespace reservation is removed;
application verified purge releases its application claims. No new purge endpoint
or permission is provided. Downgrade refuses to erase application history and
otherwise removes only the new projection/schema, preserving legacy records.

The schema enforces record shape and atomic name exclusion. Authenticated
lifecycle code must enforce immutable owner, incarnation and shared-data bindings;
arbitrary direct SQL writers are not an isolation boundary. The management role
needs DML privileges on the claim table for invoker triggers, which the protected
installer must verify before activating registration.

These tables do not themselves expose application management routes or run provisioning.
Credentials, qualified publication, shared access and late-effect fencing remain
required before activation. This is a real application
schema-head advance: shared deployment must coordinate migrations and compatible
API versions through its protected workflow; personal APIs never run migrations.

### Application intent and lease journal

Migration `0161` adds separate application operation and platform reservation
tables. The internal `ApplicationRegistry` authenticates owner/team and scope for
create, update, suspend, resume, retained destroy, replay and progress reads. Its
prepared-plan interface is for trusted management code, not HTTP owner payloads:
publication and protected installation inputs must be qualified before calling it.
No application management route or provider worker is activated by this layer.

Idempotency fingerprints represent caller intent, not generated IDs or installation
defaults. Replay returns the original operation without requiring a new publication
lookup. Plans freeze application manifests, release metadata, shared execution/data
bindings and measured platform costs; credentials do not belong in these plans.
Update and resume need a completed predecessor and an exact next-generation plan.
Suspend and destroy may supersede incomplete work. Owner, incarnation, names,
cluster and data identity cannot change through these operations.

Application and legacy reservations count against the same cluster platform
allowance under its row lock. Applications reserve no persistent storage. Update
retains the componentwise larger old/new hold; its future worker must retire the
old process before starting the new one. Budget changes lock budget, application,
then operation; lease-only operations lock application before operation. No external
request occurs within these transactions. Downgrade refuses to erase operation or
reservation history.

Each transition advances deployment and access generations, preserves the original
plan and records its predecessor, and invalidates the older lease. Lease checks
bind application/incarnation, both generations, epoch, token and database-clock
expiry. They are database progress fencing, **not** proof that a running API has
stopped or an in-flight provider write cannot finish. This layer deliberately has
no completion or reservation-release operation. Requested suspend/destroy retains
capacity, names and shared data until future provider integration proves routing,
Pod admission and process shutdown plus credential/connection retirement. Accepted
shared tasks and shared users are never cancelled or revoked by these transactions.

### Application external-effect evidence

Migration `0162` adds a separate write-ahead journal for application Kubernetes
mutations. The trusted lifecycle provider records a resource locator, action,
request digest and exact UID/resourceVersion preconditions before dispatch. It
never stores Secret bodies in this table. Secret request digests are appropriate
only for high-entropy managed material, not guessable credentials. Targets are
limited to frozen application manifest names, Secret names referenced by those
Deployments' environment/CA-volume bindings, the fixed retirement quota, and
exact-identity Pod deletion in the application
namespace. Namespace and RoleBinding identities remain create-only.
The current lease may also delete exact Secret names referenced by earlier frozen
plans of the same application, with mandatory UID/resourceVersion preconditions.
This covers updates, resumes and stops that interrupt an update before old material
is retired. Historical references never authorize creation or patching, and neither
another application's history nor a foreign namespace expands the target set.

Effect keys have immutable replay semantics. Within each operation, only one
unresolved effect can be prepared at a time. Dispatch is an atomic, one-winner
transition from `prepared` to `dispatched`; only that caller receives permission
to attempt the write. Another caller, even with the same valid lease, must not
resend it. Expiry, retry, and supersession never erase uncertain dispatches.
The current application lease can read its predecessor history, not another
application's history. A provider can record an immutable observed UID/version
after validating the real response or reconciliation readback; PATCH/DELETE
observations must match the intended UID. Downgrade refuses any effect history.
An authoritative Kubernetes HTTP409/422 rejection is a separate terminal
`rejected` record with its status code, not an observed mutation or a reset.
The old key never dispatches again; a new key may freeze corrected preconditions.
Timeouts, throttling and server errors cannot supply this rejection proof.

This is journal authority, not Kubernetes authorization or a completed lifecycle
worker. The provider must still validate the exact request digest, ownership,
response and patch/delete preconditions. A crash between dispatch commit and the
request is deliberately ambiguous. A missing object on readback does not prove
that a late request cannot create it; safe recovery requires provider-side
fencing before a new attempt. `observed` means that specific effect was verified,
not that an application is healthy, credentials are retired, or capacity can be
released. No live installer consumes this capability yet.

`ApplicationKubernetesProvider` connects that journal to one-attempt HTTPS writes.
It stamps application/incarnation and operation/effect identity, derives the request
digest from the actual body, sends UID/resourceVersion tests in JSON PATCH and
DeleteOptions, and disables redirects. Namespaced mutations require the namespace
UID recorded by an observed same-application bootstrap, with live ownership readback.
CREATE/PATCH readback verifies the expected document; DELETE202 is not retirement
proof, and malformed readback is rejected. An uncertain dispatch only reads on
subsequent calls, including after lease takeover. A confirmed409/422 is retained
as rejection so trusted orchestration can use a new key after fresh observation.

This internal adapter receives qualified manifests/material from trusted lifecycle
code, not from an owner raw-manifest endpoint. That caller must qualify PATCH/DELETE
target ownership and history before supplying UID/resourceVersion, including Pod
owner chains; the adapter does not establish that lineage from a supplied UID.
It neither creates credentials nor
coordinates process shutdown, schema compatibility or capacity release. Returned
observed effects are historical evidence, not a new health check. Kubernetes child
CREATE has no namespace-UID precondition: readback detects namespace replacement,
but does not claim to fence a privileged external administrator replacing it.
The application manager itself has no namespace replacement/delete authority.

### Shared application database access

`loom.nebius_application_database` supplies a protected shared-side credential
interface, not an installed lifecycle worker. Its administrator-installed private
SQL schema binds one development data UUID, database identity and dedicated manager
login. That ordinary manager can invoke the credential routines but cannot perform
general role/schema administration or write the private records directly.

Each application incarnation/access generation gets a separate ordinary login.
PostgreSQL16 membership options grant inherited shared-data DML with `SET FALSE`
and `ADMIN FALSE`; the login cannot assume the common runtime role. The common
role has no schema ownership/DDL or migration-head writes. It is separate from
the historical service role. Protected shared migrations must reapply its grants
for new tables. Developer-controlled APIs remain trusted development code with
shared-data DML, not mutually adversarial database tenants.

Grant/revoke serialize on a private application row. A committed monotonic
revocation record prevents an earlier delayed grant from reopening retired access,
including a generation that had never finished provisioning. Revocation removes
LOGIN, password and membership without deleting users, tasks or data. A separate
committed call terminates existing connections and checks their absence; successor
access waits for predecessor connections to disappear. The SQL routine itself
rejects a retirement made in its current transaction before terminating anything;
rolling back a later drain cannot undo the prior revocation. `NOLOGIN` alone is never
retirement evidence. An authentication already in flight may outlive a backend
snapshot, but after revocation it has no shared runtime membership or data grants.

The interface preserves exact role OIDs, rejects role replacement/privilege drift,
and never rotates an unknown credential on replay. PUBLIC data privileges that
would defeat revocation are rejected. Private records retain only credential
fingerprints, not raw passwords. The protected caller must generate high-entropy
credentials and retain them in protected material for retry. This code has no
live installation/dispatch entry point and does not retire object-store keys,
close Pods, qualify application schema compatibility, or release capacity.

### Recoverable application credential material

The internal application registry persists a generation's credential bundles in
the existing management `LocalEncryptedSecretStore` before a trusted lifecycle
caller prepares or dispatches external grants or Kubernetes Secret delivery.
Migration0163 atomically links each operation to its unique encrypted record;
foreign keys retain both the operation and ciphertext, and downgrade refuses to
erase material history. Provider-secret collection recognizes these references,
preserving any referenced retired key without aborting unrelated collection.
Plans and public operation progress contain neither raw
material nor secret references. Management's encryption key remains separate from
the shared-development keyring delivered to APIs.

`ensure_material` validates the current operation lease and serializes competing
callers. Its synchronous, side-effect-free factory runs only when no committed
material exists; retries and lease takeover decrypt the original material without
rotating credentials. Factory/transaction failure leaves no partial reference.
New credentials require an active create/update/resume with exactly its frozen
generation-specific DB, storage and auth Secret targets. Historical fixed-name
plans and stop operations cannot generate new material. `load_material` requires
a current lease even to read earlier operations of the same application, allowing
retirement without granting sibling/future access. Missing or corrupted material
fails closed rather than generating a replacement.

This is encrypted persistence, not semantic validation of a password, CA, cloud
key or shared keyring. The trusted lifecycle provider still must qualify those
values, protect delivery and coordinate revocation; no credential provisioning,
runtime activation, readiness or capacity release is enabled by this journal.

## Managed environment identity and rendering

This section describes the retained full-environment v1 format. New personal
applications use the shared-data, application-only contracts above and do not own
execution/build namespaces, databases or buckets.

`loom.nebius_environment_contract` separates an environment's UUID/incarnation
from its class (`development`, `staging`, `production`), owner and mutable
deployment generation. Multiple developers can have development environments;
the class is not an instance identifier or an execution-capacity allocation.
Fresh shared dev is `loom-dev`; a personal application's namespace is
`loom-dev-<slug>`. Its execution namespace is `loom-run-<incarnation-hex>` and its
build namespace adds `-build`. Slugs such as `alice-exec` cannot collide with
Alice's auxiliary namespaces. A deployment update preserves these identities.

The separate, protected `FoundationBinding` contains operator-owned installation
settings, a public DNS zone, shared ingress identity and a configurable pool-wide
warm floor (default zero). It cannot be supplied by feature source. Constructing
this object validates inputs; it does not provision infrastructure or change the
native autoscaler's settings.

The optional `generated_postgres_storage_gi` setting selects the database size
for newly generated environments (a strict integer from 10 to 1024 GiB). Omitted
or `null` preserves inheritance from `platform_config_json.postgres_storage_gi`.
The selected size controls the PVC, backup dump/scratch and platform storage
reservation together. Imported bindings retain their existing configured size.
Creation records freeze the chosen size and manifests, so a later default change
does not resize an existing database or alter an idempotent creation replay.
This setting does not increase the protected platform/storage allowance.

`loom.nebius_environment_render.render_environment` reuses the standalone stack
templates for a registered child. Each render contains its own PostgreSQL
StatefulSet/PVC, namespaced credential references, incarnation-derived bucket
names and one environment-local V1 execution topology. Importing an existing
binding requires exact namespace, target and hostname matches to its protected
installation input and preserves existing bucket names. Import is an operator
operation, not a user-selected namespace override.

Each child has one HTTPS hostname, not a different public port: the shared
ingress routes `/api` to that child's service and `/` to its static web server.
The ingress controller owns the default wildcard certificate; no TLS private key
or per-child public LoadBalancer/Caddy volume is rendered. Imported hosts must
also fit that certificate's configured DNS zone. NetworkPolicy restricts this
ingress to the configured controller namespace and Pod identity; database access
remains namespace/role scoped. Actual DNS, certificate validity, credentials and
object-store permissions require provisioning and installed verification.

The renderer reports a conservative platform **request** envelope: all steady
Pods, simultaneous Deployment surge, migration/configuration/backup Jobs,
init-container peaks, backup scratch space and retained database storage. This
is input for platform admission, not measured usage or a physical reservation.
Execution tasks/builds are excluded from that envelope and belong on the shared
execution pool.

Migration `0154` adds `nebius_environments` and
`nebius_environment_namespaces` without changing retained `dev_instances`.
The management database owns these rows. A single physical-name index covers
all three namespace roles, preventing cross-role collisions even under concurrent
transactions. Registration and the complete namespace set must be reserved in
one transaction before resource creation. Active/suspended/destroyed rows retain
slug and hostname claims until verified purge; incarnation and target IDs are
never reused. Purge verification and the transaction coordinator are lifecycle
responsibilities, not capabilities conferred by inserting a row. A downgrade
refuses to discard nonempty registration history.

This is an **offline provisioning contract, not operational multi-person
acceptance**. The managed format keeps the scheduler and capacity policy disabled,
renders no actuator/collector Pods, grants only observer RBAC, and sets zero-Pod
quotas in execution/build namespaces. The standalone deploy helper rejects this
format. The management request layer below verifies candidates and reserves
identities, but rendering does not provision resources or establish shared
admission/write enforcement or installed concurrent-owner execution.

### Shared HTTPS foundation

`loom.nebius_shared_ingress` renders a Traefik shared controller for the existing
standalone platform namespace. `SharedIngressInstallation` binds an installation
UUID, the protected foundation, a region-local mirrored digest-pinned controller
image, and a separate TLS Secret. The controller label must be
`loom-shared-ingress`; the foundation's ingress namespace must match the current
public Service namespace. The qualified controller is Traefik 3.7.13. These are
rendering and disposable-cluster contracts, **not a live installation receipt**.

The public allocation is reused. A standalone configuration may explicitly set
`shared_ingress_enabled: true`; its existing `loom-web` LoadBalancer keeps its
allocation and port but selects the shared controller. `loom-web-origin` is an
internal Service pointing to the unchanged web/Caddy Pods. An exact-host TCP SNI
route passes the original hostname through to Caddy, including TLS-ALPN certificate
renewal. Caddy's TLS PVC is not copied, deleted, or replaced. Later standalone
rollouts retain that protected flag. Managed child configuration strips it and
cannot render a public LoadBalancer or the origin Service.

Personal/management routes terminate HTTPS at the shared controller using the
platform-owned wildcard/SAN certificate. The ingress class selects only the
configured routes; unknown routes have no application fallback. Uploads and
responses stream without shared-controller body buffering. WebSocket upgrades
and strict path-prefix matching are covered by the disposable Kubernetes test.
Request reads and upstream response headers have a
one-hour timeout; response streaming has no total write timeout. These Traefik
settings enforce the policy; obsolete nginx annotations have been removed. There is no HTTP
listener, public dashboard, access-request log or cloud credential in the controller.

This is trusted infrastructure: the standard Kubernetes Ingress provider receives
cluster-wide **read-only** access to Services, Secrets, Nodes, EndpointSlices,
Ingresses and IngressClasses. An ingress-class selector does not narrow Secret
discovery. It receives no Kubernetes mutation authority; ExternalName services
and cross-provider references from child routes are disabled. The shared TLS key
is mounted only into the controller, never into personal namespaces. Only trusted
provisioners write Services and Ingresses; controller defaults are not a general
annotation-admission policy. Each Pod requests 100m CPU, 128 MiB memory and 64 MiB
scratch; reserve twice that for rollout surge.

There is no uniform request-byte or global request-concurrency limit in this
renderer. Traefik's in-flight middleware is per route, not a shared budget; its
file-configured buffering middleware also buffers responses. Neither supplies the
required streaming/global-buffer contract. Existing endpoint validators are not
a universal pre-auth request-size defense: JSON/multipart parsing can happen
before authorization. Public management activation must first qualify a bounded
request-receive strategy for these application endpoints. Pod resource limits are
not a substitute for that remaining request-exhaustion work.

The renderer does not authorize resource adoption, mirror/scan the image, issue
or renew the certificate, change DNS, or switch the live Service selector. A
protected installer must first qualify those inputs, ownership, before-state,
readiness, legacy-host probes and rollback; certificate renewal must also reload
the controller. Management/personal activation remains closed until that installed
route is verified. See the [deployment runbook](../runbooks/nebius-deployment.md).

The private `scripts/ops/nebius_ingress_gateway.py` primitives deliver qualified
certificate generations as immutable, separately named TLS Secrets. The protected
binding includes exact cluster and destination-namespace UIDs. A private durable
intent precedes creation; matching UID, ownership and material readback resolves
an unknown reply. An untracked Secret is not adopted and a missing recorded Secret
is not recreated. Previous generations remain available for recovery.

For an already owned controller, certificate switching freshly validates the
selected certificate and delivery receipt, journals intent, and submits one
UID/resourceVersion-conditioned patch to only the mounted Secret reference.
Unknown outcomes require exact spec/generation readback, never a repeated write.
`controller_switch_observed` is not readiness. Separate qualification requires
current owned Pods, stable membership and an authenticated TLS fingerprint from
each exact Pod through a bounded loopback-only port-forward. Disposable Kubernetes
coverage proves fresh-Pod rotation and retained legacy HTTPS/TLS-ALPN passthrough.

Initial staging in `scripts/ops/nebius_ingress_stage.py` journals the eight fixed
renderer resources before any create. It rejects name collisions, freezes full
server-defaulted configuration, and reconciles unknown outcomes by exact UID and
configuration readback. Replay cannot adopt, recreate or update recorded resources.
The initial staging journal is immutable; certificate rotation uses the separate
controller-switch journal. `controller_staged` does not establish TLS readiness.

`scripts/ops/nebius_ingress_image.py` copies the qualified single-platform Traefik
manifest into the protected region registry by digest, without changing a mutable
tag. Both manifest and raw configuration bytes must match their digests, platform
and version. A private journal precedes the single copy; replay only rechecks the
destination. The protected caller supplies short-lived registry-only auth. This
publication primitive does not perform vulnerability scanning or install ingress.

The protected caller reserves copy intent durably on the gateway before the
registry mutation. Subsequent workflow runs can only inspect the pinned
destination, even when their local journals or Actions artifacts are gone.
This reservation grants no registry credentials or Kubernetes operation to the
gateway's fixed image-intent command.

The protected `nebius-rollout` ingress operation connects these primitives through
an exact-source, hash-bound forced SSH command separate from certificate and
Kubernetes-only credentials. It uses isolated hash-locked tooling, including both
first-party wheels, and the unchanged private parent-death supervisor. The
protected runner scans the fixed image under the no-exceptions release policy
before digest-only publication. Fresh live configuration and full Node/Pod
accounting authorize staging; they never imply paid expansion or spare capacity
on a foreign legacy node.

Cutover proves current-Pod TLS and the retained legacy route before acquiring the
candidate's database idle guard. Read-only guard observation identifies the exact
owner/candidate without stealing or releasing it. The public Service selector
and persisted shared-mode flag use separate UID/resourceVersion-conditioned
writes, each journaled and annotated with operation ownership. Unknown outcomes
are reconciled by exact readback, not retry. Allocation/ports and unrelated
configuration remain unchanged. Public HTTPS proof precedes pause release.

Explicit paused recovery can restore only still-owned incomplete transitions
using the journaled original backend. It does not depend on healthy new ingress
or certificate issuance. Drift and unresolved release intents fail closed;
completed deployments are not reversed by this operation.

The separate protected `operation=ingress-dns` publishes only the bound personal
wildcard and management A records at the freshly qualified public Service IPv4
address. Its preflight observes completed staging/cutover, retained resource UIDs,
current candidate, delivered TLS and working legacy/public routes without invoking
installation or mutating Kubernetes/the guard. DNS credentials stay on the gateway;
this operation receives no registry credential. Durable intent limits each name
to one POST across process replacement. Matching preexisting records remain external;
lost-reply readback is explicitly uncertain ownership. No record is overwritten or
deleted, and partial publication remains journaled for reconciliation. Authoritative
wildcard/management proof, recursive resolution and trusted TLS must agree before
success. This is routing qualification, not management application readiness.

Protected live installation, DNS publication and renewal scheduling still require
operational qualification; these source contracts do not establish installed readiness.

## Independent management service runtime

Management mode bounds request reception before routing, JSON parsing or
authentication. The pure-ASGI guard defaults to 1 MiB per request, eight in-flight
HTTP requests per process and a 30-second total body-read deadline. It counts
actual bytes even without Content-Length, rejects invalid/contradictory framing
and encoded bodies, and returns 413 (size), 400 (framing), 415 (encoding), 408
(body timeout) or 503 (full admission) without an internal queue or disk spool.
Admission is released on handler completion, error, cancellation or a disconnect
during body reception; responses stream normally. Once a complete request reaches
the application, a client disconnect does not cancel its mutation or free its
slot before the work finishes. Invalid HTTP/1 requests close their connection.

The positive, finite `LOOM_SVC_MANAGEMENT_HTTP_MAX_BODY_BYTES`,
`LOOM_SVC_MANAGEMENT_HTTP_MAX_INFLIGHT` and
`LOOM_SVC_MANAGEMENT_HTTP_BODY_TIMEOUT_SEC` settings configure this boundary.
Budget the raw body size times concurrency **per process**, plus body copies,
JSON parsing and application memory. This is not a cluster-wide rate limit or
general denial-of-service defense. Application-mode task/bundle uploads are
unchanged; their receive/temporary-storage qualification remains a separate
prerequisite for publicly activating personal environments.

`LOOM_SVC_SERVICE_MODE=management` selects the identity and environment-management runtime in
the existing Service image. It must use a **separate management database** via
`LOOM_SVC_DB_URL` (and its optional pool URL), not a child application's database.
It checks the current schema and existing encrypted secrets before serving.
Its database/admin credentials are installation-owned; do not put them into
personal deployments.

Management retains account/session, invitation, token, team and administrative
identity/audit APIs. It does not expose workload, pipeline, provider or storage
routes, initialize child Control Plane/Gateway/object-store clients, validate a
child execution profile, or start batch/materialization/GC loops. Storage keys
are unnecessary in this mode; they remain required by default application mode.
The local-execution flag cannot turn management into a workload service.

`/api/v1/health` is process liveness. In management mode `/api/v1/health/ready`
is an unauthenticated, bounded, read-only database probe returning only component
status, with HTTP 503 on failure. When the optional provisioner is configured,
its supervised-loop health is included; a dead or recovering worker is not ready.
It reports no identities, credentials or database errors and has no dependency on
child availability. Application-mode readiness
and its authentication contract are unchanged. Hosted sessions retain secure
host-only cookies and sibling-origin rejection in either mode.

The optional provider worker can provision an execution-disabled child and perform
retained teardown. These are implementation capabilities, **not installed Nebius
acceptance**. Public DNS/TLS, IAM isolation, protected installation, multi-owner
execution and lifecycle acceptance must still be qualified before enabling owner
creation. A healthy management process is not evidence that personal environments
or shared execution are operational.

An approved runtime profile may describe private-root task support even while
management and its initial children do not execute tasks. Their renderers preserve
that profile but do not inherit the standalone target's task-identity admission
policy or execution writers. Children remain restricted-PSS namespaces with
zero-Pod execution/build quotas and disabled scheduling; management has no execution
stack. Standalone rendering still requires its exact target-scoped policy before
claiming that task support is installed.

### Management deployment manifests

`loom_service.environment_management.deployment.render_management` renders the
always-on management stack using the existing platform database, migration and
backup templates. Its protected input schema is
`loom.nebius-management-deployment.v1`: a non-nil `installation_id`, independent
`loom-nebius-management[-suffix]` namespace, `public_host`, explicit
`postgres_storage_gi`, separate `backup_bucket`, and the full `installation`
described below. The management host is outside the child DNS zone and cannot
replace the existing standalone host. The installation UUID labels its objects;
labels alone are not permission to adopt existing objects.

Only one management Service Deployment, PostgreSQL StatefulSet/PVC, migration Job,
backup CronJob and shared Ingress are emitted. No Control Plane, Gateway, actuator,
execution namespace, cloud resource, public LoadBalancer or Secret is emitted.
`management-database` reuses the existing migration chain but creates only the
ordinary `loom_service` database role, without collector/batch-runner tokens.
The Service mounts its own database CA/admin/master-key material, a read-only
publication credential, and separate Kubernetes/cloud provider credentials.
The Kubernetes credential can be an explicit projected service-account token,
mounted only into this Deployment under `loom-management-provisioner`; database,
migration and backup retain the separate unprivileged `loom-platform` account.
Migration and backup Pods receive none of the provisioning/publication credentials;
backup dump and upload containers retain their separate database/storage access.
Management database keys are newly generated once, persisted privately and reused
on retry; regenerating them is not a supported update/recovery operation.

The protected installer's `nebius_management_bootstrap` stage creates only its
fixed management Namespace and the four generated management Secrets. It records
create intent before the Namespace POST, reconciles lost replies by readback
without retry, and freezes the Namespace UID before credential delivery. Existing
untracked namespaces are not adopted. Restricted Pod security and installation
ownership are checked at credential-write boundaries, not merely at entry.
The HTTPS transport requires explicit trusted TLS/authentication, disables HTTP
retries/redirects and does not load ambient kubeconfig or credential plugins.

An outer private bootstrap journal records the material-stage intent outside the
credential directory. Once that stage starts, missing material journals or loss
of the entire credential directory blocks recovery instead of regenerating keys.
An interruption between recording intent and creating the material journal also
requires explicit recovery. The caller must independently detect loss of the
entire installation state tree; it must not reinterpret that loss as a new install.
The receipt contains namespace/Secret UIDs, not credential material. This stage
does not install runtime authority, supplied cloud/publication/backup credentials,
database workloads or public routes, and does not establish management readiness.

The installer's fixed manifest staging primitive consumes only named management
renderer phases. It records server-defaulted intent before a single create per
resource, freezes returned UIDs and Service allocations, and rejects ambiguous
absence, replacement or configuration drift. Resource quantities are compared
exactly across equivalent Kubernetes spellings; generated Job labels must bind to
that Job's own UID. Namespace and policy checks continue at write boundaries.
Read-only workload readiness additionally requires the recorded workload identity,
current controller generation and complete rollout or migration status. It never
recreates a missing workload or retries a failed migration. Backup schedule and
Ingress creation are not backup/restore or public authentication proof. Independent
installer-start evidence, authority qualification, prerequisite delivery and phase
ordering remain responsibilities of the protected installer, not this primitive.

The connected installer composes these phases with independent start evidence,
actual service-account authority probes, retained PVC/PV/CSI identity, completed
backup Job/object verification and authenticated public management readiness.
Its read-only prerequisite adapter resolves exact candidate/profile bytes through
the protected publication catalog, verifies the dedicated provisioning key and
project-only grants, and checks a separate object-only backup identity. It rejects
inherited or broader IAM permissions and backup-group grants on another bucket.
Live platform sizing counts controller rollout/HPA maxima (including per-node
DaemonSet surge), scheduled maintenance,
pending/terminating Pods and the child allowance; UID-linked controller Pods are
not counted twice. Storage-class identity, pending/expanding PVC demand, missing
StatefulSet claims (including HPA maxima), and remaining provider disk quota are
separate checks. The backup bucket and regional object-storage quota must have
headroom for one full database-sized dump, including current objects, noncurrent
versions and inflight multipart parts. Zero bucket maximum retains the provider's
unlimited meaning, not a bypass of provider quota. Initial recovery evidence uses
a versioned bucket without enabled lifecycle deletion/transition rules; retention
automation is not established by the installer. Neither these observations nor an object
readback prove restoration or authorize infrastructure expansion. The protected
workflow exposes fixed preflight/install operations bound to exact integrated
tooling and a separately stored, digest-pinned private input file. It transfers no
operator or runtime credentials through Actions. Installed restoration, credential
renewal and multi-owner acceptance must still be completed before this source-level
installer can be described as an operational environment.

The returned `platform_envelope` includes database PVC, rollout/migration overhead
and backup scratch equal to the management database size. It is fixed overhead,
not part of the installation's child allowance or permission to resize a node.
All management Pods remain on the dedicated platform node. NetworkPolicy admits
public API traffic only from the configured shared ingress controller, and database
traffic only from the management namespace. Shared ingress must enforce HTTPS;
its certificate private key is never copied into a child namespace.

The [render-only operator command](../runbooks/nebius-deployment.md#render-management-manifests)
does not install the shared ingress, provision IAM/DNS/Secrets, verify a GitHub
publication, or perform a live rollout. Those activation prerequisites and
installed multi-owner acceptance remain separate from manifest generation.

### Management namespace authority

The optional protected `foundation.namespace_authority` binds an installation
UUID and `loom-nebius-management[-suffix]` namespace to the fixed
`loom-management-provisioner` ServiceAccount. Child rendering freezes the
installation marker on each new namespace and a provisioner RoleBinding after
the namespace creates, before cloud resources or Secrets. Imported namespaces
cannot use this bootstrap path; existing operation plans retain their frozen
documents. Omitting the field preserves the prior explicit-authority contract.

`loom.nebius_management_authority.render_namespace_authority` produces separate
installer-owned RBAC and Kubernetes v1 ValidatingAdmissionPolicy documents.
The bootstrap grant allows namespace creation/metadata reads, RoleBinding
creation/reads and binding one installation-specific namespaced resource role.
It grants no cluster-wide Secret read, namespace update/delete, Node access,
token issuance, exec or role escalation. Namespaced grants cover the existing
provisioner and retained cleanup; they do not allow PVC deletion.

Fail-closed admission limits the exact manager subject to generated namespace
names, installation/environment/incarnation markers and restricted Pod Security.
RoleBindings require that namespace ownership and either the exact provisioner
subject/role or the existing local execution-observer binding. Role admission also
limits that observer Role to get/list/watch on Jobs and Pods; fixing only its name
would let provisioning permissions be transferred to child code. A namespace prefix
alone never grants access. Cluster administrators remain trusted; runtime and
child Pods must never receive their credentials.

This pure renderer does not install authority. The protected installer must
verify API support, policy type checking and actual denial probes before activating
management. Applying RBAC without effective admission is not safe. Live grant
installation and connected management acceptance remain separate work.

### Managed provisioning requests

`LOOM_SVC_ENVIRONMENT_MANAGEMENT_CONFIG_FILE` optionally enables the request
layer. This protected installation JSON uses schema
`loom.nebius-management-installation.v1` and contains `foundation`, `registry_prefix`,
`keyring`, `publications` and `platform_budget`. `foundation` is the validated
shared infrastructure binding; its `platform_config_json` holds the standalone
configuration as JSON text. Neither the foundation nor resource requests come
from developer input. The budget supplies nonnegative `cpu_millis`, `memory_mib`,
`storage_mib` and `ephemeral_storage_mib` available **after** fixed platform and
management headroom. Startup inserts an absent budget or verifies an exact match;
a changed allowance is rejected, not silently resized.

Optional `provider_runtime` starts the worker. Its `kubernetes` object requires
an explicit HTTPS `endpoint` and CA `ca_file`, plus one closed authentication mode:
private Nebius `credentials_file`, or `kind: projected_service_account` with an
explicit `token_file`. The latter reopens the bounded private token file on every
request, following Kubernetes projected-volume rotation without caching old bytes.
The renderer projects a one-hour renewable token and namespace root CA only into
the management API. This establishes authentication, not namespace permissions;
the protected installer must separately qualify and install scoped RBAC.
A separate private `cloud_credentials_file` is required in both modes.
Credential files are bounded regular files with no world access or group write;
projected read-only files are supported. No ambient kubeconfig, login, proxy or
insecure transport is used. Both modes stay pinned to the configured origin.
`concurrency`
defaults to four provisioning operations (range 1–16), and `poll_seconds` defaults
to five (range 1–60); these are not task-capacity shares. Shutdown cancels operations
and lease heartbeats before closing HTTP/SDK/database clients. Database outages
leave uncertain intents charged and restart polling, without exposing exception
contents. Without this option, accepted requests remain pending.

Enabling that runtime also requires an explicit
`foundation.provisioning_project_id`, distinct from the cluster's project and
tenant/quota parent. Newly planned service accounts, IAM groups, access keys and
buckets use this dedicated project; memberships use their recorded group IDs.
This scope is a protected operation-plan field, not part of child platform
configuration. It does not change cluster/pool identity or regional storage
endpoints. The installer must qualify the actual project's region and effective
permissions; choosing an ID does not grant authority. Runtime credentials must
not receive tenant-wide or cluster-project administration merely to create IAM.

Creation freezes this project in PostgreSQL before provisioning. Replays and
retained credential revocation use the original operation's scope even if current
installation settings change. Historical plans without this field retain their
original cluster-project resources and tenant-scoped groups; they are not silently
moved or re-created. Insufficient credentials for a historical scope require
explicit recovery, not broader automatic grants. Registry-only/offline settings
remain compatible without an active provider or provisioning-project field.

`LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN` is a read-only credential for
publication metadata, PR checks and artifacts. Configuration is rejected outside
management mode or without that credential. It must not enter child manifests,
operation records or redirected artifact-download requests. Invalid configuration
fails startup without echoing its contents. Without the installation file,
management still serves identity/readiness; environment routes return 503.

Each protected publication binds a `candidate_id` UUID to `source_sha`, `run_id`,
`run_attempt`, `artifact_id`, `artifact_sha256` and `pull_request`. New requests
verify the successful same-repository `dev` publication attempt, the exact merged
PR's squash SHA, and all four required GitHub-Actions-app checks on that PR head.
There is no assumption that CI ran on the subsequent `dev` push. The exact named
artifact must be unexpired, its downloaded bytes must match both pinned and GitHub
digests, and its seven image identities must match the configured registry.
Runtime image signatures, platform and vulnerability policy are checked with the
installation keyring. Evidence timestamps remain metadata, not a new image TTL.
Unknown candidates return 404; unavailable or invalid publication authority fails
closed with a sanitized 503.

The authenticated API supports:

- `POST /api/v1/environments`: `{slug, candidate_id}` and an `Idempotency-Key`;
  returns 202 with the durable operation UUID, not a readiness assertion.
- `GET /api/v1/environments`: this user's current team's retained registrations.
- `GET /api/v1/environments/{environment_id}`: desired registration and operation.
- `GET /api/v1/environment-operations/{operation_id}`: current operation state.
- `POST /api/v1/environment-operations/{operation_id}/retry`: explicitly retry the
  owner's current blocked operation without changing its plan or identities.
- `POST /api/v1/environments/{environment_id}/operations`: an `Idempotency-Key`
  and `{action: "destroy_retained", expected_generation}` request retained cleanup.
- `POST /api/v1/environments/{environment_id}/login`: return an environment-bound,
  90-second one-use child login proof, never the management or child admin token.

The actual user/team and scopes come from existing authentication; owner fields
in the body are rejected, generic credentials without a user are insufficient,
and cookie mutations retain CSRF enforcement. Another owner's lookup is forbidden.
Migration `0155` adds a cluster-locked platform allowance/reservation and ordered
operation/resource journal. Creation atomically reserves all namespaces, resource
costs, registration and immutable resource intents before any provider action.
Over-capacity requests return 409 `platform_capacity_exhausted` with needed and
available values. This is application/PVC accounting, not execution-pool admission.

Same-user, same-key, same-request retries recover the existing operation even if
publication is now unavailable; changed requests conflict. The journal uses
database-time expiring leases and increasing runner epochs to reject stale or
out-of-order confirmations. Provider identities cannot change on replay, and
completion requires all steps through application readiness. Kubernetes creation
is create-only with exact frozen-field/ownership and UID readback. Native IAM
effects have individual intents and deterministic idempotency keys. Credentials
are encrypted atomically with their journal confirmation before immutable child
Secrets are published. Each child gets distinct DB roles, TLS, session/secret-store
keys, admin/collector/batch credentials and canonical/source/backup object identities;
the installation's model-provider credentials are not copied. Child object IAM
permissions are bucket-scoped, not project-wide data grants. Database TLS leaf certificates last
365 days; rotation remains a lifecycle obligation, not an automatic immutable-Secret
feature.

Readiness waits for database/migration, all four application Deployments, configure
completion and an authenticated exact-owner readback over the child's public HTTPS
host. The child loads its protected identity from
`LOOM_SVC_MANAGED_ENVIRONMENT_CONFIG_FILE`. Owner enrollment creates a non-platform-
admin identity without copying a password. Management-issued proof is consumed by
the child's existing `/api/v1/auth/login/complete` route, creating a new child
session. Proof expiry is checked after database locks; concurrent replay cannot
create two sessions. Login requires a mutation-capable management user session;
an attributed bearer must also carry every child-owner scope (`read:own`, `submit`,
`tokens:manage`, `providers:manage`, `team:manage`). Read-only or attenuated bearer
credentials cannot be exchanged for owner authority. `loom dev login ENVIRONMENT_ID`
exchanges the proof using a fresh, redirect-disabled client and saves a separate,
identity-bound child context without replacing the management identity. Select it
with `loom --context NAME <command>`. Optional `--browser` opens a second one-use
proof in a URL fragment, scrubbed before app startup; the page requires an explicit
sign-in click and refuses redirects before sending credentials to another origin.
See [explicit server contexts](cli-mode.md#explicit-server-contexts). These source
interfaces do not establish installed DNS/TLS or multi-owner execution readiness.

Retained destroy advances the desired generation immediately, fencing earlier
workers and management login issuance. It revokes the ready child's owner/team
identity and delivered object access keys, closes Pod admission with zero-Pod
quotas, suspends Jobs/CronJobs and scales application/database controllers to zero.
Controller names stay occupied by stopped objects so delayed create requests cannot
restart them. Changes test both UID and resource version; foreign/replaced/drifted
objects block cleanup. Discovered backup Jobs and completed Pods are journaled by
UID before their cleanup. Completion requires stopped-controller readback, complete
Pod inventory and enforced zero-Pod quota usage. Only then are CPU/RAM/ephemeral
reservations released. Namespaces, PVCs, buckets, storage reservations and name claims
remain; there is no data purge, slug reuse or automatic result-expiry policy.
Quota scopes and selectors must match the frozen intent, not merely contain its
fields: a scoped zero-Pod quota is not evidence that all Pod admission is closed.

After logging in to the selected management origin, the request/status commands are:

```sh
loom dev create alice --candidate <approved-candidate-uuid> --idempotency-key create-alice-1
loom dev list
loom dev status <environment-uuid>
loom dev wait <operation-uuid> --timeout 60
loom dev destroy <environment-uuid>
loom dev retry <blocked-operation-uuid>
```

`loom service up --environment dev-alice --candidate <approved-candidate-uuid>`
dispatches the same create request. It is not yet an update command or arbitrary
source deployment. Reuse the printed idempotency key after a lost response; `wait`
timeout exits 2 without cancelling the operation. Hosted-target errors never fall
back to local Compose. No target (or explicit `--environment local`) retains local
Compose and prints that target. Destroy reads the current generation unless
`--expected-generation` is supplied and prints the exact generation/idempotency-key
retry command before mutation. Retained destroy is not suspend/resume or data purge.
Explicit retry retains the runner epoch and all confirmed resource identities;
pending/running/completed calls are no-ops. After the bounded automatic retry budget
is exhausted, each explicit retry permits one additional reconciliation. It cannot
revive a generation superseded by destroy or authorize adoption of a replaced object.
Suspend/resume/update, arbitrary-source publication, shared execution and
shared-target management remain separate delivery work.

## Supported workload boundary

Native Kubernetes execution is the hosted path. OLDLAB, GB10, Slurm and remote
shared-cluster workers are retired, with no fallback route. Desktop/GUI and
Behavior GPU hosted workloads are unsupported. Other task and pipeline classes
require conversion according to the
[compatibility inventory](../evidence/service-workload-compatibility-v2.json).
Local execution and retained result access remain supported. Repository
retirement does not claim workload parity or implement those replacements.

Durable Trial/attempt identity, generation fencing, verifier rewards,
trajectories, artifacts, usage and provenance survive compute cleanup.
Kubernetes completion alone does not establish successful Loom finalization.
The [retirement record](../historical/shared-cluster-retirement-2026-09.md)
records the removed architecture and remaining compatibility obligations.

## Delivery and acceptance

Feature branches start from `dev`; `main` is reserved for release promotion.
[Contribution policy](../../CONTRIBUTING.md) owns required checks and merge
rules. [CI](../contributing/ci.md) describes validation selection;
[candidate publication](../runbooks/nebius-candidate.md) records commit identity
and immutable image references for deployment and rollback.

Source merge and credential-free CI do not establish live workload, recovery,
capacity or migration acceptance. Such evidence belongs to an exact candidate,
environment and explicitly authorized operation. No documentation cleanup
authorizes infrastructure shutdown, credential revocation or live data changes.
Workload qualification and operational acceptance remain tracked by #1550 and
#1538. Staging/production rollout automation still requires environment inputs
and approval wiring; see the retirement record for that boundary.

## Database lineage when moving from the isolated branch

The branches independently used revisions `0133`–`0135` for different changes.
`dev` keeps its published history through `0143`; the Nebius reward projection,
zero-quota observation and native-build observation migrations are appended as
`0144`, `0145` and `0146`. Fresh databases and existing `dev` databases upgrade
through that single chain. Nebius subsequently added native resource usage at `0136`;
`dev` preserves its published migrations through `0149` and appends native usage as `0150`.
The deployed Nebius series through `d07718e2` is retained by the conversion candidate.

An existing isolated-branch Nebius database at `0133`, `0134`, `0135` or `0136` is **not**
a database at the corresponding `dev` revision. Do not run this checkout's
normal upgrade against it or stamp it to a `dev` revision: that could skip
required schema changes. Moving an existing Nebius deployment requires
the [qualified lineage conversion](../runbooks/nebius-lineage-conversion.md), with
backup/restore evidence,
before selecting this `dev` candidate for deployment. Keep the previous
branch-bound candidate for that deployment until the conversion is qualified.

## Physical pool observation for managed environments

`InClusterKubernetesCapacityReader.capture_pool` reads one Node/Pod/DaemonSet
inventory for a protected physical-pool selector. It does not add per-environment
node totals. Its `PoolObservationScope` contains registry-bound environment
incarnations, execution/build namespaces and targets, plus protected gateway Job
receipts. A Pod discounts a global reservation only when its namespace,
controller Job UID/name, target, workload kind and local claim generation match
that receipt. Labels alone, a lost create receipt, or a same-named replacement Job
never establish ownership. Reservation UUIDs form placement-only keys, so the
same local claim ID in different environment databases cannot alias.

Foreign resident Pods and terminating nonterminal Pods remain charged. Pending
Pods are also charged unless an unregistered Pod has a hard node selector that
contradicts the selected pool. Unknown affinity or tolerations do not establish
exclusion; this can conservatively delay admission. Duplicate native node IDs,
Pod UIDs or live Pods for one reservation fail closed, as does registered work
scheduled outside the pool. The existing resource arithmetic retains Pod slots,
restartable init sidecars and init peaks. The pool reader preserves Pod-level
requests from raw API pages because older Kubernetes SDK models omit that field.
In-place resize is not qualified by this entrypoint: active resize conditions or
status allocations exceeding requested resources block the affected pool inventory
until convergence. Equal settled allocations and workloads already assigned to a
different pool do not cause that block. It never treats a requested downsize as
proof that kubelet has released the old resources.
Only the sanitized resource/identity projection leaves the reader; Pod payloads,
environment values and commands are not evidence. A scope fingerprint binds the
observation to the registry and gateway receipts used for that capture.

This is a read-only library entrypoint, not live global admission. The caller must
bind the selector to the actual native node group, combine fresh provider quota
evidence, and use the management ledger to retain unobserved grants. Existing
single-target collection remains unchanged. Managed hosted execution stays
disabled until the shared ledger, local claim protocol and protected Job-write
gateway are integrated and qualified together.

## Native task-image capacity fairness

Native build and trial admission share the existing capacity transaction lock,
placement model and provider quota identities. A lock alone does not prevent a
new trial from overtaking a builder whose capacity reservation was rejected.
The controller therefore retains one renewable waiting head per target in
`task_image_capacity_waits`, without consuming an attempt, retry budget, create
slot or cost reservation. Claim/render/admission run in a savepoint; a rejected
claim rolls back before the waiting record commits under the same outer lock.

A waiting record expires after 120 seconds unless a controller renews it after
validating actual demand and a realizable native shape. Cancellation, changed
materialization epoch, disabled target/policy and incompatible resource evidence
invalidate it. Compatible historical allocatable samples remain usable after
scale-to-zero. An impossible node/allowance combination cannot block other work.
An observed Ready node can also establish fit after its managed Pods drain,
without a compatible cold-node sample. Unknown foreign/DaemonSet resource and
slot occupancy remains charged; this does not establish cold-node capacity.
Changing a claim's epoch, target/pool or resource envelope loses its old waiting
priority and rejoins at the tail.

New admissions preserve waiting headroom in bin-packing, pending/create limits
and shared native quota accounting, including independent CPU pools sharing SSD
quota. Waiting itself is not an actual create or node-cost event. A builder
excludes its own head and respects older heads; already committed reservations
retain their precedence. Real reservations remain charged until UID-fenced
cleanup, even after cancellation or lease expiry. No running trial is preempted
and no machine is permanently reserved.

Fairness acceptance requires the matching controller and all capacity-admission
writers to be deployed. Mixed-version rollout and fixture tests alone do not
prove live no-overtaking behavior. This admission mechanism does not certify
Phase 2 rootless containment, signed publication or ARM support; those retain
their separate activation requirements.
