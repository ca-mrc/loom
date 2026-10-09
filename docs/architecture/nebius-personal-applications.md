# Nebius personal applications and shared development foundation

Personal applications own their frontend/API lifecycle and use a separately
managed development foundation for data and execution. This page owns their
identity, provisioning, credentials, source builds and completion contracts.
The [platform contract](nebius-primary-platform.md) owns environment boundaries
and the management service; [shared pools](nebius-shared-pools.md) own global
execution and build capacity. For commands, use the
[personal application runbook](../runbooks/personal-development.md).
Implemented mechanisms still require protected installation and live acceptance
on the selected target.

## Personal application and shared development data boundary

The [owner clarification in #1915](https://github.com/qianyi-sun/loom/issues/1915#issuecomment-5835681150)
selects independently versioned personal frontend/API instances connected to one
shared development database, object stores and worker pool. Personal lifecycle
must not own or remove shared data or background services. Production and staging
retain their data and credential boundaries. The [v1 managed renderer](nebius-primary-platform.md#managed-environment-identity-and-rendering)
still provisions isolated child stacks; it has **not** been converted to
this shared-data model. Existing frozen v1 bindings retain their old meaning.

The standalone foundation renderer also accepts the canonical `loom-dev` system
namespace, only with `environment=development`. Its execution namespace remains
in the separate `loom-nebius-` naming space (for example,
`loom-nebius-dev-execution`), so it cannot reserve a personal `loom-dev-<slug>`
name such as `loom-dev-execution`. Personal applications reference this foundation
through `SharedDevelopmentBindingV1` and continue to own only their web/API.
This namespace support neither creates a foundation nor qualifies shared-pool
admission; standalone execution writers must not independently admit against a
pool already owned by the shared manager.

`render_development_foundation` separately prepares a **private bootstrap** for
fresh `loom-dev` data and internal services. It reuses the platform templates but
emits no execution/build namespaces, actuator, collector, public route, backup
CronJob or execution configuration job. The API runs in `api_only` mode without
a task runtime profile; control-plane scheduling and materialization are off.
The database bootstrap creates only service, control-plane and gateway roles,
without an actuator role or worker tokens. The namespace allows internal ingress
only. This is not an execution-close operation for an existing installation.

The result includes a resource envelope for system-node headroom review and is
deliberately not an ordinary standalone rollout bundle. It grants no permission
to reuse staging credentials, adopt an existing database, or modify ingress or
physical capacity. A protected dev-only installer must qualify those boundaries
before applying it. Public/management access and connection to the single shared
pool remain separate activation requirements; a private bootstrap is not a
working multi-person development environment.

The fresh-only development preflight now binds the protected publication/source,
cluster and StorageClass identities, refuses pre-existing dev resources or
reusable old data volumes, and measures system capacity alongside existing
workloads. It includes HPA maxima, rollout/maintenance demand and terminating
Pods without changing other environments. Source preparation happens in the
publisher checkout; live inspection needs no Git checkout on the gateway. The
separate protected dev gateway authenticates the source-bound installer bundle.
This read-only report is not installation authority, a capacity reservation or
an interrupted-install recovery path. See the
[foundation runbook](../runbooks/nebius-deployment.md#independent-shared-development-foundation)
for the remaining write, credential and activation boundaries.

The fixed local-material bootstrap can create only `loom-dev` and its four
immutable database/TLS/authentication/admin Secrets. It generates independent
material once and retains no actuator, collector, batch-runner or cloud keys.
A private recovery journal and a separately retained start marker bind the
installation, cluster, operation, material digest and resource UIDs. Ambiguous
creates are resolved by readback, never automatically repeated; existing or
changed resources and missing recovery evidence stop the operation. Fresh state
is claimed exclusively, so callers with different marker directories cannot
overwrite one another's journal. This primitive has no CLI or installed write
authority and creates no workload, database volume or public route. Storage
identity delivery and the source-bound protected installer remain prerequisites.

Private installation sequencing now composes that bootstrap with fixed
renderer-derived configuration/network, supplied-storage, database, migration and
internal-service phases. An independently anchored installation journal freezes
inputs and completed phase journals. Recovery rechecks the existing installation;
it does not reuse the fresh namespace-absence preflight. Each phase previews server
defaults, records create intent before writing, resolves ambiguous outcomes only
by readback and retains exact resource UIDs/configuration. The installer pins the
dev PVC, dynamically provisioned PV and CSI disk before migrations, requires a
separate authenticated provider-disk qualification, and checks current workload
generations and migration completion. A final readback covers earlier phases too.

The connected development adapter now supplies live publication/source,
cloud/credential/quota/headroom checks, provider-disk ownership and authenticated
dependency probes. Resumed qualification counts installed dev resources once;
only the existing anchored installer grants write/recovery authority. The disk
check follows the recorded database controller through its Pod, node and dynamic
PVC/PV to the actual provider disk. Dependency proof runs a fixed read-only HTTP
probe inside the existing API Pod, checks its own database/object credentials and
exact build revision, and rechecks controller/Pod/container identity afterward.

The private entry module freezes the packaged source record, installation inputs
and credential files; it accepts no caller readiness flags or arbitrary manifests.
The dev-only gateway authenticates the complete bundle against an operator-pinned
digest before executing packaged code. It accepts only development preflight and
private installation, with a separate key and retained dev state; existing
management/staging commands are unchanged. Protected publication binds the clean,
integrated source, source archive record, hash-locked dependencies and first-party
wheels. Prepared releases are private and replay-checked; incomplete preparation
is retained for reconciliation. The protected `nebius-rollout` development actions
use only that dedicated authority. These source capabilities still require an
installed operator grant and live qualification, including exclusion of competing
privileged namespace or storage replacement. A private completion
receipt does not establish public access, shared execution/build admission or
personal-owner acceptance. Existing management/staging installer scope and runtime
behavior are unchanged.

Loom Service supports `LOOM_SVC_SERVICE_MODE=api_only` as a process-level building
block for this model. It serves the same authenticated workload routes as the
default `application` mode and retains schema, secret-store and execution-profile
validation, but starts none of the batch runner, taskset materializer, taskset GC,
provider-secret GC or price-catalog synchronization loops. The shared application
service remains responsible for those workers using their existing claims. An
API-only process closes only its own database engine and HTTP/storage clients;
stopping it does not cancel work owned by a different process.

In `api_only` mode, authenticated `/api/v1/health/ready` checks PostgreSQL with
`SELECT 1` and lists at most one object in each distinct configured
artifact/trajectory bucket using that API's own credentials. The
`ListObjectsV2(MaxKeys=1)` response must report HTTP 200; listing contents are
discarded, and bucket metadata permissions are not required. It returns 200 for healthy dependencies and 503 for
unavailable dependencies or invalid bucket configuration, without provider error
details. It does not query the legacy staging mutation/capacity tables or claim
execution capacity, task readiness, or lifecycle cleanup. Application mode reports
its configured environment and namespace with dependency status; lifecycle
capacity admission remains separate. Database errors
while authenticating a readiness caller return a secret-free 503; they never
authorize the caller or bypass ordinary authentication/authorization failures.

This setting is not a distributed singleton lock, an authorization boundary, or
a read-only API: authorized requests can still mutate shared state. It does not
bind sessions to a personal origin, select a per-task runtime, provision shared
credentials, or make arbitrary application schemas compatible. Exact schema-head
validation remains required; personal APIs must not independently run migrations.
Legacy isolated-child configuration and management provisioning configuration are
rejected in `api_only` mode. No existing deployment selects this mode implicitly,
and this process capability alone is not installed shared-development acceptance.

## Application-scoped browser authentication

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

## Application-only manifest contract

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
loopback and link-local destinations). DNS is limited to TCP/UDP port 53 on
`kube-system` Pods labelled `k8s-app=kube-dns` or `k8s-app=coredns`, covering
disposable Kubernetes and native Nebius resolvers. This is not a hostname-level HTTPS
allowlist. The renderer never changes the shared namespace's policies: a protected
shared-side admission update is required for these connections to work. It also
does not rename/recreate the existing shared foundation.

Operations keep their frozen rendered policies; upgrading the management source
does not rewrite old plans. New create/update/resume plans use the current
renderer. Disposable CNI coverage installs both the personal egress policies and
shared ingress policies, verifies service-name access with both DNS labels, and
retains rejection checks for foreign namespaces, frontend Pods and unrelated
shared services. This does not establish installed personal-application readiness.

Rendering alone does not admit or provision an application. Legacy frozen
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

## Application namespace authority

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

`ApplicationRuntimeProvider.close_admission` composes this quota with the
application effect journal and protected installation identity. It reconciles
outstanding quota dispatches before changing anything, never recreates a missing
recorded quota, and advances an older gate only with its observed UID and exact
resourceVersion. Live generation/operation/identity and unscoped zero-Pod spec
must match, and the quota controller's status must acknowledge `pods: 0` before
the call returns. Definitive patch conflicts wait for new preconditions; uncertain
writes are never resent. Closing admission alone does not prove process shutdown.

`ensure_namespace` reconciles lost bootstrap replies and resumes only current
prepared creation intent. An early stop with no dispatched Namespace request can
create the empty retained personal namespace under its current operation, so
retirement uses the same quota path. An existing unrecorded namespace is never
adopted, and a disappeared observed namespace is never recreated.

`ensure_resource_authority` then creates the exact frozen bootstrap RoleBinding
to the protected application resource role. It reconciles lost replies, never
dispatches an unsent predecessor create, and refuses foreign, missing-recorded or
changed bindings. It cannot patch/delete a RoleBinding or broaden its role/subject.
A read-only self-access review confirms quota-create authorization has propagated
before resource operations begin; denial or evaluation uncertainty remains pending.
This uses the installed application provisioner token, not an administrator token.

`stop_workloads` then removes exact journal-owned personal Ingress/Service objects
and scales retained Deployment names to zero. Requests use original frozen
templates and UID/resourceVersion preconditions; a prepared request resumes its
original preconditions before a new intent can be derived. Lost earlier replies
are reconciled, never resent. Completion requires current Deployment controller
observations and a live, unfiltered empty Pod list; terminating or foreign Pods
keep retirement pending without being manually deleted. Replaced or drifted
workload identities block cleanup. Shared resources are never cleanup targets.
These methods do not remove the admission gate, retire object/SQL access, release
capacity or constitute a completed lifecycle worker.

This is not an installed management upgrade: the protected installer must create
the distinct management ServiceAccount, verify the policies and their enforcement,
and only then grant bootstrap authority. The manager must still authenticate owners,
qualify immutable application inputs and enforce lifecycle generations. Rendering
RBAC alone supplies none of those controls and does not claim malicious-manager
isolation or authorize any live permission change.

## Shared-side application network admission

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

## Application registration and shared name claims

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

## Application intent and lease journal

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
retains the componentwise larger old/new hold; the lifecycle worker must retire the
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
capacity, names and shared data until the lifecycle provider proves routing,
Pod admission and process shutdown plus credential/connection retirement. Accepted
shared tasks and shared users are never cancelled or revoked by these transactions.

## Application external-effect evidence

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

After supersession, the current lease can also reconcile an old same-application
dispatch using its original operation, generation, effect key and request digest.
CREATE/PATCH reconciliation requires the exact original document; a different body
cannot satisfy the recorded request. This path makes no Kubernetes writes and
cannot dispatch a predecessor's prepared request. DELETE reconciliation confirms
absence of the original UID, without deleting any replacement. Terminal effects
remain immutable history, not a fresh readiness or retirement check. Stale leases
and sibling applications cannot inspect frozen predecessor plans or record their
effects. The disposable Kubernetes lane exercises a real successful CREATE whose
reply is lost, followed by suspension and reconciliation without another POST.

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

## Shared application database access

`loom.nebius_application_database` supplies a protected shared-side credential
interface, not an installed lifecycle worker. Its administrator-installed private
SQL schema binds one development data UUID, database identity and dedicated manager
login. That ordinary manager can invoke the credential routines but cannot perform
general role/schema administration or write the private records directly.

Grants require an explicit release schema revision. The schema-qualified routine
takes a shared transaction advisory lock and accepts only one matching live
`public.alembic_version` row; the manager cannot invoke the internal unqualified
grant. This uses read-committed isolation to avoid stale snapshots after waiting.
The protected installer takes the matching exclusive lock, including first
installation. Upgrading a legacy unqualified installation requires the dedicated
manager to be `NOLOGIN` with no sessions; the installer neither kills sessions nor
re-enables the manager. Existing binding and routine drift is never overwritten.

Online Alembic runs hold the matching exclusive **session** lock on their own
direct PostgreSQL connection, including across migration commits, and physically
close that connection on exit. A changed revision or purge is refused while any
personal generation remains unretired or a tracked login has a backend. Exact-head
no-op commands and read-only diagnostics remain possible. The guard checks the
complete Alembic migration plan, not only the first requested target; version-table
purge or recreation is checked before Alembic's plan callback. The database owner uses
a bounded, read-only `migration_ready()` routine, not access to private credential
records. A legacy installation without that routine must be upgraded before a
schema change. Lock contention and stale transaction isolation fail closed.

This admission boundary is not a process-retirement proof: the protected rollout
still coordinates the full personal stop lifecycle, shared/background services,
compatible candidates and reapplication of runtime grants before reopening access.
Personal deployment and rollback never migrate the shared database themselves.

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
close Pods or release capacity. Schema qualification establishes exact database
revision equality, not the correctness of arbitrary developer application code.

## Recoverable application credential material

The internal application registry persists a generation's credential bundles in
the existing management `LocalEncryptedSecretStore` before a trusted lifecycle
caller prepares or dispatches external grants or Kubernetes Secret delivery.
Migration0164 atomically links each operation to its unique encrypted record;
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

## Application object-store access

`application_management.cloud_effects` journals fixed-purpose IAM effects separately
from retained full-environment provisioning. A protected storage binding identifies
the shared development data UUID, dedicated provisioning project and existing
data/source access groups. Each application incarnation/access generation creates
one service account and one EXPLICIT access key; it creates no buckets, policies,
groups or backup credentials. Membership requests require the recorded key and
successfully decrypted, committed application material. The trusted lifecycle
caller qualifies the material and shared groups before using this interface.

Migration0165 retains cloud request identities, dispatch epochs and observed IDs.
One current-lease caller wins dispatch. Lost responses remain uncertain: absence
does not authorize another CREATE or DELETE. Current authority can reconcile earlier
same-application dispatches and delete exact observed predecessor identities, but
cannot dispatch a superseded CREATE or adopt unrelated/sibling resources. A
matching resource without recorded dispatch is not silently adopted. Downgrade
refuses to erase cloud history.
Retirement retains one deletion intent across superseding operations (for example,
suspend followed by destroy). A successor may dispatch a still-prepared retirement
once under its current lease; a previously dispatched retirement only reconciles.

`ApplicationCloudProvider` uses the existing native Nebius SDK with transport and
native renewable-credential authentication retries disabled, 30-second request and
authentication deadlines, and deterministic idempotency keys. Reads validate frozen names, project/group,
labels, specification and recorded resource ID. Observed resources that disappear
or change identity fail closed. Delete addresses only an exact recorded ID, after
ownership readback, and confirms absence without automatically resending uncertain
requests. EXPLICIT access-key material is retrieved separately for encrypted
persistence; no key values enter the cloud journal or public progress.

This adapter supplies bounded IAM I/O, not an activated lifecycle worker. Protected
installation still must qualify shared group/prefix grants and credential authority.
Lifecycle orchestration must reconcile all outstanding grants, retire credentials
and processes, verify object-store revocation propagation, and coordinate schema
and readiness before releasing reservations. The API does not claim that a cloud
resource snapshot fences a privileged external administrator, proves S3 access
denial, or completes personal-environment acceptance.

## Application credential integration

`application_management.credentials.ApplicationCredentialProvider` composes the
shared SQL/IAM adapters with the encrypted material journal. Protected installation
supplies the shared development CA, SecretStore keyring, database identity and
existing IAM groups; it does not generate a new CA/keyring for a personal API.
The delivered DB URL names the frozen shared PostgreSQL service and uses
`verify-full`, an ordinary generation login and the shared CA mount. Only DB,
storage and auth generation bundles are constructed; manager, backup and cloud
provisioning credentials never enter them.

Permissionless account/key creation precedes material persistence. Exact DB login
and group membership grants follow successful encrypted commit and semantic bundle
validation and equality with the frozen release's schema revision. Retry and lease
takeover reuse the same password/key. Changed shared
material or malformed persisted credentials fail closed rather than replacing a
generation's material. `AsyncApplicationDatabaseAccess` keeps synchronous SQL off
the heartbeat loop using private, bounded autocommit connections. The protected
caller must qualify the manager's database/TLS route; construction grants nothing.

After the durable SQL grant, `ApplicationOwnerProjection` reads the current
management User, Team and real membership under the operation lease. Owning a
deployment or having submit scope does not confer team-owner authority. The
bounded shared-side `enroll_principal` routine initializes missing identities with
the source UUIDs and actual owner/member/viewer role; it never copies passwords,
email or platform-admin authority. Existing enabled ordinary shared identities
retain their profiles, credentials and actual shared membership role. Disabled or
privileged shared identities are refused, never re-enabled or downgraded.

Retained private identity and application-enrollment records prevent replay from
recreating deleted users/teams, including through another application/team.
Missing membership between existing identities fails closed; it may represent an
administrator's removal. A new membership can be initialized only while creating
a genuinely new user or team. These short SQL transactions share the schema fence
and require the exact live application/access generation; they grant no direct
manager table access. Personal stop never deletes shared identities or provenance.
The installer validates retained table/routine structure rather than overwriting
partial or drifted installations. Source eligibility and the lease are rechecked
after external enrollment and group grants before returning deliverable material.
Failure retains generation material/reservation and does not open Pod admission.
Enrollment is not a login token: login and active readiness must still recheck
current authorization and origin/generation-bound session requirements.

Credential delivery uses three immutable, generation-named Kubernetes Secrets in
the already-observed personal namespace. The existing effect journal stores only
request hashes and object identities, not Secret values; a lost response reconciles
the same Secret instead of reposting it. These observations are historical write
evidence, not a substitute for live resource/readiness checks before API startup.

Database retirement commits revocation before draining existing sessions. It needs
the same data/application identity but not a still-deliverable CA/keyring, so
expired delivery material cannot itself prevent revocation. SQL cancellation may
leave an in-flight request; monotonic shared-side tombstones fence late grants.
This SQL step does not retire cloud access or Pods, prove S3 denial, coordinate
migrations, mark readiness or release platform reservations.

Cloud retirement separately reconciles prior-generation grants and deletes exact
owned memberships, keys and accounts in dependency order. Prepared predecessor
creates are never sent; uncertain deletion intents survive suspend-to-destroy
without another request. Shared groups, buckets and policies remain untouched.
Retained encrypted material supplies signed, read-only `ListObjectsV2` probes
after fresh provider readback confirms exact IAM retirement, including on replay.
Every distinct artifacts, trajectories and source bucket must return a bounded,
well-formed HTTP403 `InvalidAccessKeyId` or `AccessDenied` response. This is a
**composite access-retirement attestation**, not a claim that `AccessDenied` alone
proves universal key invalidity or that list denial tests every read/write action.
Successful reads, other errors, redirects, encoded or ambiguous XML responses and
transport failures keep retirement pending; no probe independently releases capacity.

The verifier's scope comes from the protected installation. Supported refreshes
preserve its endpoint, region, buckets, groups/project and shared data identity,
rooted to the original completed upgrade. Historical plans must match its
registration/shared data and cluster, shared namespace, endpoint/region and data
buckets; historical IAM parents must match its recorded groups/project. This
immutable installation boundary supplies legacy source-bucket scope without
rewriting a frozen plan. A data UUID alone cannot establish that boundary.
The HTTP origin must match the original frozen endpoint, and the signer uses the
original encrypted key with redirects and ambient client authentication disabled.
An interrupted permissionless key
with no committed material or membership intent is deleted without fabricating
probe credentials. This composes access retirement, not installed readiness or
permission to release capacity.

Before startup, credential `qualify` requires retained material and the four
already-observed current IAM grants. It reconciles their live identities without
new cloud mutations, validates the protected CA/keyring and original object key,
and requires positive responses to the identical read-only probe in every scoped
bucket. Only the expected bucket/prefix-bounded list response qualifies; ordinary
denial is not readiness. It then requalifies schema, SQL login and actual shared membership under the current
source/lease checks. Positive catalog checks require every individual runtime
table/sequence privilege; missing grants are never repaired here. Qualification
reuses bounded grant/enrollment replay, not a new administrative SQL interface.
Its returned evidence contains identities and an access-key hash, not credentials.

## Closed-admission application preparation

The runtime recovers the current operation's interrupted ServiceAccount,
NetworkPolicy and immutable Secret requests from frozen plans/encrypted material.
Prepared requests retain their original key and UID/resourceVersion; dispatched
requests only reconcile. Historical prepared requests never dispatch. This path
does not create workloads or routes, and stopped operations cannot invoke it.

`prepare_static` establishes protected namespace/resource authority and observes
the zero-Pod quota before installing the frozen ServiceAccount/network policies.
It retains the account's original observed identity and updates a prior owned
NetworkPolicy only through an exact preconditioned patch. Live disappearance,
replacement, spec drift or termination blocks preparation rather than recreating
or repairing unqualified resources. Historical write success is not readiness.

`read_prepared` separately observes current network resources and the three exact
immutable generation Secrets, including complete data equality, without writes
or material generation. The retained ServiceAccount keeps its original UID;
network/Secret observations must belong to the current operation. It works before
and after unfencing and returns only lease-bound resource references, so readiness
can refresh evidence without closing admission or exposing credential contents.

An exact current-operation quota DELETE intent is the durable activation boundary,
including when that request is rejected or awaiting observation. Under the journal
lock, this phase prevents new retirement, quota-closing and static/Secret writes
in the same operation. The single unresolved-effect slot prevents an earlier
prepared request crossing that boundary. A stopped successor can still close its
own generation's admission. New `start:` workload intents require an observed
opening under the same journal lock; preparation success alone cannot admit them.

The protected shared installer may separately grant the provisioner a namespaced
Role allowing GET of only the three installation-specific PostgreSQL, Control
Plane and Gateway ingress policies. It grants no policy list/write or shared
Secret access; the provisioner cannot install its own shared Role/RoleBinding.
`read_shared_network` verifies those exact live policy specifications against the
frozen development binding, rejects absence/drift/termination, and returns only
name/UID/resourceVersion observations under the current lease. This read authority
is not part of bootstrap and is not yet activated by a live installer.

`ApplicationLifecycleCoordinator.prepare` composes these concrete adapters: resume
current preparation, stop prior personal processes, retire prior SQL/object access,
prepare static resources, verify shared ingress, enroll/deliver and qualify current
credentials, then refresh live resource/network/process evidence. Missing
prerequisites keep admission closed. It returns internal typed preparation evidence
without starting Deployments/routes, completing the operation, or releasing its
reservation. The active coordinator and owner-facing deployment flow described
below consume these prerequisites. Protected installation and live acceptance
must still qualify the configured worker and its authority.

## Active application startup and completion

`ApplicationLifecycleCoordinator.activate` invokes preparation only before the
first activation intent. Recovery refreshes shared access, immutable resources and
shared ingress rules without re-entering retirement/static writes. The runtime's
`read_retired` is observation-only: current zero-Pod admission, observed zero-replica
controllers, absent routes and a complete empty PodList must still hold before an
unsent quota deletion. Prepared deletion retains its original UID/resourceVersion;
only definitive409/422 rejection permits a fresh precondition key. Dispatched
deletion only reconciles, and observed deletion requires actual quota absence. A
replacement quota is a conflict, never permission for another deletion.

`start_workloads` installs the frozen current Deployments and Services, preserving
retained Deployment UIDs through exact preconditioned patches from zero replicas.
Routes may be recreated only after their recorded prior identities retired.
Interrupted current requests recover the original frozen document and preconditions;
uncertain requests never resend. A current template/image match and controller
observed generation are required. Execution fields must match after normalizing
known Kubernetes API defaults; unplanned commands, lifecycle hooks, init containers
or scheduling changes cannot qualify as the frozen version. Updated/ready/available/total replicas all
equal to desired and no unavailable or terminating replicas. Only then is the
Ingress created. `read_ready` repeats live checks without resource writes.

`ApplicationLifecycleCoordinator.start` composes activation, workloads, refreshed
access/static/network/retirement observations, and durable ready completion.
`complete_ready` takes budget, application and operation locks in that order,
requires a current unexpired lease, settled journals, current activation and exact
workload/static references, schema/owner/access-key evidence, and prior access
retirement. Its secret-free immutable receipt completes the operation and clears
the lease. The current frozen CPU/memory/ephemeral reservation stays charged;
only excess held for an old/new transition is released. Shared storage stays0.
Exact receipt replay changes neither timestamp nor reservation.

This is internal lifecycle implementation, not installed acceptance. It does not
by itself enable a polling worker or live installer. Protected installation,
four distinct versions/fifth-owner onboarding, real execution provenance,
authorization, recovery/teardown isolation and scale-to-zero still need live proof.

## Personal application control

The management-only `/api/v1/applications` API accepts a personal slug and release
ID for creation. Owner-scoped detail/list and operation status expose registration
and progress, never frozen plans, credentials or completion evidence. The
`/applications/{id}/operations` endpoint accepts `update`, `suspend`, `resume` or
`destroy_retained` with an expected generation; only update supplies a release ID.
Mutation requests use idempotency keys. Exact replay returns the same operation's
current status, including when a peer commits it during request planning.
`/application-operations/{id}/retry` retries a blocked current operation.

The additive owner-scoped `GET /application-operations/{id}/evidence` returns a
bounded journal projection: operation state, runner epoch, boolean lease activity
and completion-record presence, and grouped Kubernetes/cloud effect counts by
fixed kind, action and journal phase. A read-only repeatable-read transaction
keeps the projection in one snapshot. It returns no frozen plan, resource identity,
intent, credential, lease token or raw provider error, and uses `Cache-Control:
no-store`. Existing operation/status response schemas are unchanged. These counts
do not prove current provider state, process absence, access revocation or readiness;
even every effect being observed does not authorize completion or a retry.

`ApplicationManager` uses protected installation-pinned release records, foundation,
shared-development binding and namespace authority. Owners cannot supply images,
provider authority, storage ownership or readiness assertions through these APIs.
Update/resume preserve identity and derive the next deployment/access generation;
the transactional registry rechecks generation, ownership, state and capacity.
Stop and replay do not require the release to remain in the catalog. These routes
reuse existing authentication/CSRF checks and close the auth transaction before
independent registry work. Unconfigured application management returns 503.

`ApplicationWorker` polls only current nonpurged operations with absent/expired
DB-time leases. One successful claim drives one concrete start/stop coroutine;
different owners have independent bounded concurrency. Lease heartbeats, readiness
deadlines and retry limits prevent unbounded execution. Lease loss, shutdown and
database outages cancel/drain work while preserving durable effects and charges.
Worker database calls, including polling, renewal and failure reporting, have a
five-second maximum further limited to one-sixth of the lease duration. Together
with the one-third-lease heartbeat interval this reserves time for response latency
and cancellation before expiry. A database deadline is an infrastructure failure,
not a provider retry: uncertain operation state remains retained for reconciliation.
Failure reporting happens after cancellation; only concrete coordinator evidence
can complete an operation. Poll health becomes false on DB failure or shutdown.

Protected management configuration may include `applications`, binding the shared
development environment, namespace authority, immutable release catalog and storage
access groups to explicit runtime inputs. It cannot activate the legacy environment
provisioner at the same time. `ApplicationServiceRuntime` composes the existing
cloud, SQL, Kubernetes and object-access adapters with one supervised worker.
Kubernetes authentication uses a projected ServiceAccount; cloud authentication
uses an explicit protected credentials file, with no ambient fallback. Shared
verify-full SQL credentials and CA/keyring material come from bounded private
files, not owner requests. Invalid startup material exposes no application manager.
Shutdown removes owner admission and drains the worker before closing its clients.
Management readiness includes actual application-worker poll health when configured.

The management renderer accepts this application runtime instead of the legacy
environment provisioner. It uses `loom-application-provisioner`, projected cluster
credentials, and separate application cloud/shared-material Secret references.
Application installation configuration is an immutable revision-named ConfigMap;
the management database and admin/master-key references remain unchanged. This
renderer creates no credentials and does not upgrade an existing installation:
the initial installer remains create-only and legacy-runtime-only. Protected
upgrade, old-process retirement and first-owner installed qualification remain
required before enabling this configuration live.

The fixed shared SQL setup command, `loom.nebius_application_database_install`,
accepts only protected namespace/data/schema configuration and Secret-provided
credentials. It qualifies the namespace-local TLS database route, coordinates
with the existing schema lock, creates a missing ordinary manager login and
installs the existing access routines. Retries authenticate the retained password
and preserve the role identity; mismatches or a lost bound role fail rather than
rotating or adopting credentials. It preserves business records and runs no
business-schema migration. Personal APIs must never invoke this administrator
command.

The fixed protected setup adapter uses the existing create-only recovery journal
for application admission, shared observer permissions, shared network access and
the SQL setup Job. Management-to-shared PostgreSQL ingress is separate from the
personal application policies. It binds both namespace identities, exposes the
admission type-checking barrier that must precede bootstrap permissions, and never
retries an uncertain Job creation. A failed Job requires explicit recovery.
The material phase delivers only three immutable revision-named Secrets: the
retained SQL manager password for the setup Job, and the application's management
cloud and shared SQL/CA/keyring bundles. It never generates new shared master keys,
rotates existing credentials or overwrites bootstrap Secrets. Protected caller
qualification of material provenance and actual cloud permissions remains required.
The management cutover is a fixed UID/resource-version-conditioned update of the
existing `loom-service` Deployment. It retains the original template, records each
update intent before sending it, and reconciles uncertain replies without repeating
the write. A narrowly scoped CREATE admission policy prevents delayed controller
requests from starting `loom-management-provisioner` Pods in the management
namespace. Retirement requires that actual denial, current zero controller
replicas, and no remaining Pods, including terminating ones. The database and new
application manager are not fenced. The new template is previewed and checked
before activation; no automatic recovery restarts the legacy provisioner.
The protected entry connects these adapters, but source/test coverage is not an
installed upgrade: the first personal HTTPS login still needs installed proof.

`POST /applications/{id}/login` exchanges the owning management **user session**
for a 90-second one-use proof, never a shared password or database credential.
Delegable bearer tokens cannot request a full browser session, which could otherwise
widen their team-limited authority. The internal bridge requires the current active,
completed generation and uses its existing SQL role, with the management-mounted
shared CA. It rechecks the generation before returning the proof and uses bounded
connections without retaining a pool. Retiring old processes/SQL access remains
the lifecycle fence; this exchange is not a cross-database atomic operation.

The proof is hashed with application ID, origin and access generation. Its starting
team is included in the hashed token, so `/auth/login/complete` selects that exact
current shared membership rather than the first alphabetical team. Disabled or
missing identities/memberships and platform-admin promotion fail closed; ordinary
shared role changes are honored. No email, password copy or new login table is
required. The existing `/auth/managed` browser route accepts the proof through its
scrubbed fragment and requires an explicit sign-in click. Management login responses
use `Cache-Control: no-store`; raw proofs must never be logged or placed in a query.

`loom dev app` invokes the application API for create, list, status, update,
suspend, resume, retained destroy, operation retry/wait/evidence and login. Create/update
require a qualified application `--release` UUID, not a legacy candidate ID or a
local source path. Generation-fenced mutations print their exact retry command,
including the selected management context, before submission. A timeout never
cancels or automatically retries an operation; blocked and superseded operations
are not reported as completed.

Application login validates identity, origin and both deployment/access generations
against completed active status. It consumes the one-use proof with a fresh HTTPS
client, without management credentials, redirects or proxy-environment inheritance.
The returned session must identify the expected ordinary user and current shared
team; owner, member and viewer roles remain valid. The CLI saves child credentials
in an `app-SLUG-APPLICATION_ID_HEX` context with an immutable application/incarnation
and management/child-origin binding. Login refresh preserves explicitly configured
child provider settings without copying management settings. Browser login obtains
a separate proof in the URL fragment, never a query parameter or CLI output.

The legacy `loom dev create --candidate` and environment lifecycle commands remain
distinct for retained full-environment management/recovery; they do not silently
switch to application APIs. See the [owner workflow](../runbooks/nebius-deployment.md#personal-application-owner-workflow).
Arbitrary-source publication and installed multi-owner acceptance remain separate
requirements. CLI/source coverage does not prove deployed personal readiness.

## Local application source capture

`loom_cli.application_source.capture_application_source` is the internal capture
boundary used by [`loom dev app build --source`](../runbooks/personal-development.md).
The build command packages, uploads and submits the captured source; capture alone
does not deploy it. The capture function
snapshots current tracked and nonignored untracked Git worktree bytes, including
dirty edits and local deletions, into a private temporary directory outside the
checkout. Normal Git exclusions are retained; ambient `GIT_*` tree/index/config
overrides (including alternate global-config files) are ignored. Fsmonitor/hooks,
lazy fetch, remote protocols and optional index
writes are disabled. Sparse checkouts/indices are rejected rather than expanded
through remote helpers or silently treated as complete source.
Mandatory exclusions also remove VCS/runtime/owner state, environment files
(except `.env.example`) and known private-key names even when tracked.
This is not a detector for secrets authored under arbitrary other names.

`loom.application_source.ApplicationSourceManifestV1` binds the sorted paths,
bytes, normalized executable modes and relative link targets with a canonical
SHA256 digest. It is separate from task-source and qualified-release authority;
the optional base commit is informational, never CI approval. Capture rejects
escaping/dangling/cyclic links, special/nonowned/hardlinked files, submodules,
unmerged indices and included-file/parent/inventory changes. Transfer bounds are
25,000 files, 512 MiB aggregate and 8 MiB canonical manifest, not execution quotas.
Consumers must use the verified manifest reader rather than reopen unchecked
paths. The owned temporary snapshot is removed on exit, including errors.

`package_application_source` composes that capture with the shared verified
archive encoder, yielding an anonymous owner-only temporary upload stream, its
byte length and archive SHA256. The archive checksum is distinct from the source
manifest digest; neither turns the informational base commit into CI approval.
The stream is rewound for upload and closed on caller exit or failure.
`loom.application_source_archive` encodes deterministic uncompressed USTAR:
one canonical manifest followed by fixed numbered regular byte records. Archive
names never become extraction paths. The trusted reader bounds and validates
headers, lengths, content hashes and final padding; it creates source files
exclusively under a pinned empty private directory and installs safe links last.
Failed extraction is not accepted context; the caller owns partial-file cleanup.

The management `ApplicationSourceRegistry` retains owner/team-qualified upload
intents in `nebius_application_source_uploads` (migration `0173`). Each intent
freezes its installation/data binding, source digest, archive checksum/length,
informational Git commit and one-hour-or-shorter database-clock expiry. Concurrent
same-key retries return one identity; changed intent or team conflicts. Only the
internal verifier can record `source_verified`, and verified receipts remain
readable after the upload deadline. SQL retains their immutable identity/history.
Identical archives select one content-addressed key in the shared source bucket,
while owners retain separate access records. A source receipt is not a build,
release, CI approval or capacity reservation.

`ApplicationSourceUploader` authenticates this intent before consuming its stream,
spools bounded bytes privately, verifies both the transport hash and full source
archive, then writes only verified content to the server-derived shared key. It
reads back and hashes stored content before recording acceptance; an ETag or a
successful PUT reply is insufficient, and an uncertain PUT is observed without
another explicit upload call. The shared S3 adapter can retry identical verified
bytes under its existing SDK retry policy; this does not grant another source
identity or retry a build/deployment mutation. Database-clock expiry is rechecked after reception and before
completion. Per-process in-flight limits and reception/storage deadlines bound the
work. At most two archive verifiers run concurrently, independently of the
configured reception/storage limit, to bound parsed-manifest memory in the
management Pod. Other admitted uploads wait with their private disk spool;
cancellation before verification releases that spool without starting a parser.
Cancellation during verification retains the spool and admission until off-loop
verification finishes, then cleans private temporary state. Manifest validation
uses sorted-path prefix lookup rather than expanding every directory ancestor.

Management exposes `POST /api/v1/application-sources` (idempotent intent),
`GET /api/v1/application-sources/{upload_id}` (owner status) and
`PUT /api/v1/application-sources/{upload_id}/content` (raw
`application/octet-stream`). Session/membership/CSRF checks and upload ownership
precede body consumption. Only that exact configured PUT route bypasses ordinary
JSON buffering; it retains strict framing, encoding and incremental byte limits.
Responses are non-cacheable; HTTP/1 rejection with an unread body closes the
connection. Personal application APIs do not expose these routes, and there is
no caller-controlled completion endpoint.

Optional protected `applications.runtime.source_upload` configuration enables the
uploader. It names absolute `credentials_file` and private `spool_directory` paths,
`max_inflight` (default 2, range 1–16), reception/storage deadlines (default 300
seconds each, range 1–3600) and `upload_ttl_seconds` (default 3600, range 60–3600).
The bounded private credential file contains only `access-key` and `secret-key`;
the installer must supply a source-scoped identity and bounded spool volume.
Bucket, endpoint, region and installation/data/cluster identities come from the
protected shared configuration, never owner input or ambient credentials. The
runtime owns the storage client and closes it on shutdown or failed startup;
omitting these settings preserves old installation fingerprints and disables
upload. Configuration support is not evidence that the capability is installed.

`ApplicationClient` connects packaged source to these authenticated intent,
status and streaming-upload routes. It checks every returned source identity and
upload ID, uses the same frozen archive on an explicit CSRF rejection, and never
automatically retries an uncertain network write. This is the transport used by
the build command, not a standalone deployment command or CI approval.
The native application Job adapter shares the existing task-image
prepare/rootless-build/publish rendering mechanism. Its protected claim binds
owner, source upload, build attempt, installation/data/cluster and an immutable
recipe; fixed service/web components use `deploy/Dockerfile.service` and
`deploy/Dockerfile.web`. Recipe identity includes platform, tool images, component
paths and output format. Application Jobs carry their own build identity, not
synthetic Task or materialization fields. Credentials remain outside the
untrusted build phase and publication sees the build volume read-only. Personal
build arguments distinguish the source digest from its informational base commit
and never label that commit as a CI-approved build.

The trusted application runtime verifies the exact uploaded archive size, SHA256
and source manifest before extraction. It parses migration revision literals and
their complete acyclic ancestry, including historical merges, without importing
developer Python or running migrations. The single declared head must match the
protected shared schema. This is a metadata compatibility check, not a claim that
arbitrary application code is semantically safe; personal code receives no DDL
authority. Only the rootless build phase executes developer build instructions.

Publication validates exactly two bounded local OCI outputs against the recipe's
architecture, preserves digests through Skopeo, and reads back each immutable
registry manifest. The final receipt binds both images to owner, source, recipe,
installation/data/cluster and build attempt. Interrupted progress has a distinct
schema and cannot qualify a release; unknown publication is not retried. Native
cache keys include a separate application domain, source digest and recipe
(including platform); the existing bounded blob store and GC are shared without
fabricating Tasks. Source/registry credentials never enter the build context.
These runtime helpers do not themselves admit or launch application builds.

`ApplicationBuildRegistry` records owner/team/install/data/cluster-bound build
intent from a verified source upload. Concurrent requests with the same replay
key retain one build and one queued attempt. Each attempt freezes its source and
protected recipe/storage/pool binding; replay after a management restart uses
those retained inputs even if the active recipe catalog changes. SQL prevents
deleting build history or rewriting source, owner or attempt inputs. Status
checks the retained claim against the original source and binding before returning
it. Creating this intent performs no network operation, resource admission or
release qualification; its queued state does not mean a builder has been launched.

`ApplicationBuildDispatch` commits the exact pool request and absolute deadline
on that same retained attempt before a caller can perform network I/O. Concurrent
selection and restart return the same request, including its original recipe and
deadline. SQL prevents rewriting or erasing it. A cancelled build cannot create
new demand, but its existing request remains readable for cancellation and
uncertain-reply reconciliation. Reading that record is not activation consent;
the automatic worker and its activation/completion evidence are separate consumers.

`ApplicationBuildWorker` drives that request through the common pool's existing
prepare/status/activate and stop/drain APIs. The attempt journal uses database-time
leases and runner epochs; stale workers cannot commit results. Each activation
consent and each cleanup message is retained once before HTTP, so uncertain replies
reconcile the same build attempt without extending consent or creating another
Job. Its Kubernetes interface only reads the exact gateway-bound Job and Pods.
Successful publication requires completed prepare/build/publish containers without
restarts and the complete trusted publisher receipt for this owner/source/recipe.
The attempt remains `settling` and charged until the common pool returns its
authenticated cleanup receipt; only then can it become `ready`. Cancellation wins
over a result observed afterward. Failed/cancelled attempts also retain their
charge until cleanup; an unsubmitted cancellation needs no pool call, while any
frozen request requires a pool cancellation tombstone or cleanup receipt. SQL
retains evidence and forbids a successor attempt before prior cleanup. Heartbeats
run independently of external reads, cancellation drains in-flight work before
releasing its lease, and bounded keyset polling avoids first-page starvation.
Management-only `/api/v1/application-builds` accepts a verified `upload_id` and
an idempotency header, not caller-selected images, target, recipe or priority.
Owner/team-scoped status and generation-checked cancel/retry controls use the same
retained build. Cancel records intent, not capacity release. Retry requires the
previous attempt to be failed/cancelled with cleanup completed; its expected
attempt number is the replay key, so a repeated retry cannot create two successors.
The successor keeps the original source and recipe. These endpoints return 503
without the explicitly configured build registry and are absent from personal
application services. This implementation does not itself configure management
credentials or install the read-only role.

Ready build status includes a deployable `ApplicationReleaseV1`: its release ID
is the build ID, and its source/schema and immutable service/web image digests
come from the retained qualified publisher receipt. Resolution is scoped to the
authenticated owner/team and installed management/data/cluster binding. A published
but still-settling build is not a release; the retained released pool receipt must
match the original request, reservation, plan and Job. SQL makes ready attempts
immutable and prohibits retrying them, so the same release ID cannot later target
different images. Changing the current recipe catalog does not change old releases.

`ApplicationManager` accepts these completed owner builds alongside the protected
pinned release catalog. Create/update/resume use the same build registry as the
automatic worker and owner status routes. They still enforce exact shared-schema
compatibility and freeze the selected release and rendered images in the operation.
Already-frozen operation replay does not need the current catalog or a running
builder. Resume resolves the application's recorded release, never a newer build.

The application builder supplies personal kind, manifest digest and informational
base commit as Docker build arguments, while leaving the actual Git build revision
unknown. The service image records these beside its existing build metadata;
`/api/v1/version` reports `buildKind`, `sourceDigest` and `sourceBaseCommit` for
personal code and never presents that code as the base Git revision. The web image
bakes the same fields into the loaded JavaScript and separately publishes served
metadata for update checks. A later fetch cannot relabel an already-open page.
Version details label personal code as not CI-approved, with separate frontend and
responding-backend source identities. These are informational reports from authored
code, not authorization or proof that every replica has rolled out; retained
publication and frozen deployment evidence remain authoritative.

Optional protected `applications.runtime.build` settings connect these controls
and the automatic worker to the management service lifecycle. They bind the
source/recipe/pool profile, management HTTPS origin, dedicated private bearer-token
file, concurrency (default 4, range 1–16), polling interval (default 5 seconds,
range 1–60), and pool HTTP timeout (default 30 seconds, range 1–60). The source
uploader must also be configured. Startup checks shared installation/data/cluster,
schema and source storage, matches the loaded profile catalog's digest against
protected pool registration, and authenticates the dedicated builder machine in
the real management database. The origin must match the management service's
public origin. A registered closed pool permits startup, not resource admission;
every common-pool operation still reauthorizes the machine and admission mode.

The runtime owns both deployment and build workers. Readiness requires both to
be healthy; shutdown drains both before closing their HTTP, Kubernetes and database
dependencies. Build observation reuses the native Kubernetes reader with an
explicit endpoint, CA and per-request projected-token refresh, never ambient
kubeconfig or the cloud provisioning identity. Routes receive the build registry
only after successful runtime creation. Omitted settings preserve historical
installation fingerprints.

The management renderer binds configured source uploads to a revision-named,
source-only credential Secret and a private disk-backed `emptyDir`. Its non-root
initializer verifies the spool directory's ownership and mode on every Pod start.
The spool mount uses canonical `/run/loom-application-source`, not Alpine's
symlinked `/var/run`, preserving the uploader's rejection of symlinked paths.
It clears an inherited setgid bit from an otherwise owner-only directory using
a no-follow directory descriptor, leaving exactly `0700`; symlinks, foreign
ownership and broader permissions remain errors rather than being repaired.
The spool is capped at 2 GiB per admitted concurrent upload (4 GiB at the default
concurrency of two), included in the manager's ephemeral-storage request and limit,
and disappears with the Pod; it creates no PVC or backup requirement. Archive
verification streams regular files in 1 MiB chunks, retaining content hashes,
strict headers/padding and link-last validation without allocating a whole source
file in management memory. Filesystem-metadata-heavy trees remain subject to the
spool limit. Source-enabled management uses `Recreate`, preventing a rolling surge
from multiplying this local upload allowance.

Configured builds additionally require a protected dedicated machine ID and pool
catalog operation. The renderer reuses the private process-owned token mount and
provides only Job reads and Pod reads/list/logs in the shared build namespace.
It grants no Job writes, Secret reads or access to other build namespaces.
The protected first cutover derives these settings from its completed predecessor
and exact registered machine/profile. It qualifies the retained shared control
plane's source endpoint/bucket/key references and freshly reads the fixed
`loom-platform-storage` Secret against its protected UID/version/source-only hash.
Only `source-access-key` and `source-secret-key` are copied into the manager's
immutable source Secret; data, backup and operator credentials are not copied.
It stages material, configuration and reader roles before replacing the stopped
manager, and checks platform fit including the upload spool before downtime.
Completed global ancestry retains the full builder-enabled configuration; recovery
to the legacy outcome retains the original manager. Historical
image-only refresh remains narrow: it preserves the existing source Secret even
when its revision differs from the older cloud/shared bundles, and cannot enable,
remove or change source/build runtime settings. Rendering these prerequisites
does not install them or establish multi-owner acceptance.

Before first pool opening, protected image correction can continue a completed
manager correction for the retained gateway or pooled development collector. It
preserves the physical pool, credentials, configuration and execution profiles;
only the selected workload's image changes. One bounded ancestry folds the latest
image per workload through activation, cancellation and completion. The collector
uses the execution-actuator publication component and CronJob suspension with
observed child drain. Suspension cannot atomically prevent an already-dispatched
Job, so installed proof includes a new successful corrected-image Job as well as
the existing fresh-capacity activation barrier. See the
[pre-opening runtime image correction procedure](../runbooks/nebius-deployment.md#correct-selected-pool-runtime-images-before-first-opening).

The common pool registry has an application-build adapter. New admission and
activation check the retained current build attempt, verified source, protected
participant/profile binding and cancellation state under the pool transaction.
The owner cannot replace the frozen claim. Personal builds receive priority 3
and share the existing build-concurrency/resource accounting with task builds and
execution. Cancelled or obsolete waiting builds no longer protect capacity from
other work. The native Job wrapper retains the same reservation-specific name,
absolute phase deadlines, credential isolation and rendered-Pod resource charge
for both build kinds. The existing machine-only pool API transports this typed
request. Gateway dispatch, retained native-runtime readback and the physical
collector bind application Jobs to build ID/attempt and observed Job UID. Stop
and output-drain use that same attempt; neither releases capacity without the
existing gateway absence check. Pool adapter support alone does not install the
management worker or enable owner-facing build endpoints.

Participant machine identity also retains an immutable workload scope. Ordinary
environment credentials can submit/control trials, verifiers and task builds;
the separate management builder credential can submit/control only application
builds in the same data participant. Prepare/replay, allocation and every retained
request operation recheck that scope under the existing authority locks. Credential
rotation cannot change it. Existing machines retain environment scope on upgrade;
this does not expand an already registered pool catalog. Initial registration
requires exactly one environment machine for each participant and exactly one
builder machine for each development participant with application-build targets.
Application targets are distinct from execution/task-build targets, use the same
participant build namespace and physical node group, and require bound profiles.
Default environment scope is omitted from serialized installations so historical
installation hashes do not change. Registration remains closed and exact-replay-only.

The protected material stage delivers the builder token only to the management
namespace, never to a shared controller, execution worker or personal namespace.
Ordinary actuator wiring selects the environment credential explicitly and does
not require an actuator for the dedicated application-build target.

Protected manager mounting and worker configuration, the user-facing build
command, durable management worker, image building and release qualification
are implemented as described above. Protected installation and live multi-owner
source-to-deploy acceptance remain separate, unproven delivery steps.

## Stopped application completion

`ApplicationLifecycleCoordinator.stop` composes the concrete retirement adapters:
close Pod admission and stop personal workloads, revoke/drain SQL access, retire
exact IAM identities and probe retained keys, then refresh the live workload and
admission observations. Only this internal path supplies completion attestations;
owner requests cannot submit evidence or a success flag. No polling worker or
owner-facing deployment flow is enabled by the coordinator itself.

The adapters return immutable, secret-free evidence bound to the application,
incarnation, shared data environment, operation/generations and lease epoch/token
digest. It identifies the observed namespace/quota and Deployment UIDs, live
resource versions, controller generations, empty unfiltered PodList version,
revoked SQL generation and original probed access-key hashes. These are trusted
adapter attestations, not cryptographic provider receipts or installed acceptance.

`ApplicationRegistry.complete_stopped` acquires budget, application and operation
locks in that order. It requires the current unexpired lease, matching attestations,
the exact recorded resource/deletion identities and encrypted original key material.
Current prepared effects and any dispatched effects prevent completion. Historical
unsent creates grant no external authority. Any error keeps the reservation charged.
One transaction records `completion_json`/`completed_at`, completes the operation,
clears its lease and zeros only its personal CPU/memory/ephemeral reservation.
Registration, namespace/name claims, encrypted history and the zero-storage reservation
row remain. Shared users, accepted work, data, services and sibling reservations
are not removed. Exact receipt replay is read-only; a different proof/token or a
later transition cannot replay the old completion. Supersession retains the receipt,
and schema downgrade refuses to discard retained completion evidence.
