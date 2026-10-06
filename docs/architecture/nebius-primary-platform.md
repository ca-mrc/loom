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

### Shared application database access

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

### Recoverable application credential material

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

### Application object-store access

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

### Application credential integration

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

### Closed-admission application preparation

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
reservation. The active coordinator below consumes these prerequisites; an installed
worker and owner-facing deployment flow remain separate unfinished steps.

### Active application startup and completion

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

### Personal application control

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

### Local application source capture

`loom_cli.application_source.capture_application_source` is an internal capture
boundary for the future owner upload/build flow, not a deployment command. It
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

### Stopped application completion

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
child availability. In application mode, the authenticated `/api/v1/health/ready`
checks PostgreSQL with `SELECT 1` and each configured artifacts/trajectories bucket
with `HEAD`, returning HTTP 503 if either dependency is unavailable. It works in
all application environments; environment and namespace are descriptive metadata.
It does not query staging mutation epochs or staging capacity evidence. The JSON
response contains `status`, `postgres`, `object_store`, `environment`, `namespace`
and `blockers`; the former staging-only `mutation_epoch`, `capacity`,
`capacity_ready` and `resource_digest` fields have been removed. This probe does
not certify storage capacity or admit destructive lifecycle operations; those
retain their own policy checks. Hosted sessions retain secure host-only cookies
and sibling-origin rejection in either mode.

The Control Plane starts legacy Worker heartbeat recovery only when
`LOOM_ENV=development` and `LOOM_LOCAL_EXECUTION=1`, matching its local Worker
routes. Native execution reconciliation, retry-exhaustion handling, metrics and
expired live-preview cleanup remain independent of that opt-in.

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

The fixed application-runtime upgrade composes the existing setup stages and
management Deployment switch without replaying bootstrap. It validates the
original input digest, completed phase journals, namespace UID and retained
Deployment snapshot. Separate upgrade state preserves the original configuration,
database, credentials and journals. The publication catalog may add a qualified
candidate but cannot remove or rewrite retained entries. New application
configuration/account and shared-access setup precede retirement; the fixed
management migration waits for the old process to stop. Only then may the new
template activate. A resumed operation never retires the new process or
automatically restarts the legacy worker. Public verification explicitly requires
`application_provisioner` readiness and authenticated management routes; legacy or
absent worker health is insufficient. The protected entry connects the upgrade to
live shared-material/IAM and actual-subject qualification. Running that protected
upgrade and proving personal-application readiness remain installed acceptance,
not results inferred from the connection's tests.

Subsequent manager software changes use a
[protected retained-manager refresh](../runbooks/nebius-deployment.md#refresh-the-retained-application-manager),
not a replay of that one-time upgrade. An immutable operation UUID binds the
original installation and the immediate completed predecessor; the original
installation lock serializes cutover. Only the qualified image, immutable
configuration and compatible release/schema selection may change. Credential,
storage, authority and route identities remain unchanged. Native Deployment and
ReplicaSet generation observations plus complete Pod absence fence the old writer.
Read-only compatibility Jobs and verified backup-object evidence precede the
management-only migration; activation rechecks those barriers. Completion requires
the actual current manager Pod/controller and authenticated public application
runtime health. Uncertain writes remain readback-only, failed migration does not
restart the old manager, and completed receipts retain bounded predecessor evidence.
An explicitly selected pre-migration probe failure may be superseded by a new
protected refresh. Its frozen history and terminal Job must qualify, and the new
operation adopts only the old stopped Deployment marker under the installation
lock. It preserves the retained runtime at zero replicas, fences old replay, and
repeats every normal probe, backup, migration and activation barrier. It cannot
reset uncertain history or recover a failed migration. Ordinary refresh histories
remain compatible; explicit supersession ancestry is bounded.
This source contract does not itself prove an installed refresh or owner acceptance.

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

Each environment binding lists its local target aliases; different environments
may use the same alias. A gateway receipt binds one exact alias, so a Pod with a
different alias cannot discount that reservation even when both aliases belong
to the same environment. Alias order does not change the capture fingerprint.
All aliases share the same one-time physical inventory rather than contributing
separate copies of node capacity.

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

The production collector now has an explicit `collection_mode=pool` path. It
requires a global pool UUID and separate management URL/observer credential,
fetches one frozen scope, binds its selector to the configured native node group,
and combines the existing native reader with one actual `capture_pool` inventory.
Ready provider/cluster counts must agree; cold samples must match the native
template. Existing single-target collection remains the default and is never an
error fallback. A cancelled pool read retains its clients until the synchronous
Kubernetes reads finish. This source integration is not installed global admission.
The management ledger still retains unobserved grants. Managed hosted execution stays
disabled until the shared ledger, local claim protocol and protected Job-write
gateway are integrated and qualified together.

## Global pool reservation journal

Submission provenance is stamped from protected `LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON`
configuration, not a public priority/origin parameter. Personal application rendering
binds the exact application, incarnation, deployment generation, release and source
digest. New batches, failed-case reruns, shared-run clones and artifact reuse record
the current submitting service's origin with their new submission ID; reusing shared
results does not give personal work shared-dev priority. The internal `submit:batch`
producer carries that retained origin into child trials. Ordinary callers referencing
a batch cannot inherit its origin. PostgreSQL prevents origin changes, including
silently classifying an old NULL, and refuses a downgrade that would erase origins.
Missing configuration/history remains unknown, not environment-priority work.
For direct single-trial submission, the authenticated service commits a small
immutable `nebius_pool_submissions` record before forwarding: server-issued UUID,
exact forwarded-payload digest, submitting user/team and protected origin. The
control plane independently authenticates the bearer or bound browser session/CSRF
and checks this record against the actual payload/caller before its idempotency
lookup. An HTTP origin or submission-ID assertion alone gives no priority. Public
input cannot replace the service-generated header. Personal backends retain the
existing trusted shared-development-DB access; this is not hostile-backend isolation.
The service releases SQL/session-auth locks before HTTP. A retained handoff replay
keeps its idempotency key; replay across application updates never overwrites the
original Trial origin. As with other submissions, clients should supply an
idempotency key to recover their own retry after a lost response. Missing
configuration remains compatible/unknown; this producer coverage does not establish
an installed global-admission or priority guarantee.

The native build handoff derives its origin from the highest-class live eligible
Trial consumer of that exact materialization, then oldest consumer within a class.
It reuses the native demand filter, including explicitly bound direct Nebius trials;
cancelled, terminal, wrong-pool, family/legacy-route and unknown-origin work cannot
promote a shared image. Selection and grant attachment both recheck this origin.
If eligible consumers change it before attachment, the unstarted grant is retained
for cancellation without consuming an attempt; a new immutable selection generation
is allowed only after that cancellation is confirmed. This local check does not
replace management registration qualification or establish an installed controller.

An authenticated unstarted cancellation may precede management prepare. In that
case the manager retains an immutable `nebius_pool_cancellations` identity/digest
record with a terminal receipt, but no resource envelope, namespace or render plan.
Delayed same-body prepare returns that cancellation, even after intake closes or
the deadline expires; changed-body replay conflicts. Existing started requests
still require the stop/drain/cleanup protocol and cannot use this shortcut. Both
SQL insertion paths share the global mutation lock and exclude a request and an
early-cancellation record for the same participant/work key. Cancellation records
cannot be changed, deleted or lost by downgrade. The local outbox can consequently
finish a pre-prepare cancellation after a restart without consuming a build attempt.

The management-side observation registry issues a persisted capture scope from
registered participants and committed gateway Job receipts. A dedicated observer
can publish exactly one provider/cluster snapshot for that scope; an identical
replay returns its retained identity, not a fresh observation. Changed bodies,
credentials, registration or physical node-group identity fail closed. New
reservations do not invalidate an in-flight capture and cannot be inserted into
its represented-Job list. Capture/observation rows are immutable and do not alter
request phases or release capacity. Collection runs outside SQL transactions.
The management-only `/internal/pools/v1/{pool_id}/captures` and `/observations`
routes connect this registry to the production collector. Only a current dedicated
pool-observer credential is accepted; ordinary users, admin/worker tokens and other
machine roles cannot publish. HTTPS transport uses no redirects or automatic write
retries, checks exact receipt identity/digest, and bounds streamed bodies to one
MiB with a total request timeout. Validation errors do not echo inputs. The routes
are absent from application APIs and confer no dispatch or cleanup authority.
The same observation registry feeds transactional execution/build prepare; live
installation still requires the protected collector/writer migration.

The internal execution-prepare adapter accepts a typed runtime/requirements
snapshot, not arbitrary Kubernetes documents or a caller's resource total. It
qualifies the registered namespace/profile, execution-class compatibility and
image signatures, then reuses the execution renderer and collector's scheduler
arithmetic to measure the complete single-Pod envelope. Named RuntimeClasses need
explicit protected overhead; absent requests cannot silently rely on API-server
defaulting. The request digest retains origin and absolute deadline while the
rendered remaining runtime changes with the time of activation. This adapter
also passes the original absolute deadline to the trusted execution runtime.
Delayed container startup cannot renew input, proxy or phase execution time;
expired startup is rejected and whole-lease expiry prevents verifier handoff.
The separate bounded output-commit context remains available for partial evidence.
This adapter does not expose an endpoint, grant capacity, write a Job or establish installed
global admission. The registry/gateway still must qualify current authority and
freeze the first activation document before a Kubernetes write.

The internal `pool_management.registry.prepare_execution` and `prepare_task_image`
entrypoints combine their respective adapters
with current machine/origin qualification and retained physical observations. One
management-wide advisory transaction lock serializes grants across physical pools
and shared provider quotas; callers must use READ COMMITTED and own the commit.
No network call or Job creation occurs in this transaction. The rendered selector
must match the protected physical pool, including its Nebius node-group ID.
Provider account usage and distinct native groups remain quota floors. Ordinary
CPU groups need no invented memory-quota identity when Nebius supplies none;
physical per-node memory fit is still enforced. Observed
Jobs discount only their exact captured reservation; later grants, foreign Pods,
expired callers and cleanup-pending work remain charged.

Fitting renewed waiting demand is ordered by trusted production, staging, shared
development and personal-development class, then age. Waiters protect headroom
without acquiring a reservation or consuming an attempt. Their original creation
time stays fixed; renewal is fresh for 120 seconds, while the first actual grant
has a separate timestamp for create-rate accounting. Impossible/stale waits do not
block fitting work. Exact admitted replay returns its retained receipt without
rerendering, refreshing a deadline or depending on currently free capacity.
Both kinds share the same transaction, waiting queue and class/age order. Charged
builds and earlier fitting build waiters also enforce pool-wide build concurrency;
that limit does not reserve an idle execution share or consume a local build attempt.
This is internal admission, not an activated global execution/build service.
The task-image adapter accepts typed materialization
selection and legacy or registered source identity, reuses the actual native
prepare/build/publish renderer, and measures its sequential-init peak. Storage,
registry, Secret, resource and runtime settings come only from its protected
profile. Selection generation and prospective native lease epoch are distinct;
Job/ConfigMap names use the global reservation identity. The adapter acquires no
attempt or grant itself. Capture binds the frozen native Job's actual attempt epoch,
not its separate selection generation. Admission HTTP and the native outbox are
connected in source, including the execution outbox. Installed gateway startup
and protected writer migration still require live qualification. Application-image
builds remain a later consumer of the same ledger.

`loom.nebius_pool_contract` binds request identity to a participant, workload kind,
local work ID and generation. Equal local IDs in independent environment databases
do not identify the same request. A reservation receipt retains its request digest,
admission epoch, frozen plan and any observed Job UID. Waiting is not a reservation
receipt; cancellation and uncertain external writes do not imply free capacity.

The internal activation control locks the exact request, requalifies its current
origin/profile and charged envelope, then freezes the actual execution Job or
native-build Job/ConfigMap once. The absolute deadline is retained and first
activation uses only the remaining runtime; replay returns the stored receipt,
never a rerender or renewed allowance. Status does not renew waiting freshness.
First activation also requires a distinct, immutable consent with an aware
`not_after` deadline. Management checks it under the request lock and again at
the intent write, bounds it by the workload deadline, and retains it in the frozen
plan. Exact replay can recover an expired consent's already-committed intent;
changed consent cannot renew it. Consent expiry does not release capacity or
shorten an already-accepted workload's runtime deadline.
Unstarted cancellation can atomically cancel waiting or reserved work, including
while intake is closed; it cannot cancel an already activated intent into free
capacity. Concurrent activation/cancellation has one winner. These functions own
no commit and perform no external write. The management-only participant API
exposes these operations under `/internal/pools/v1/{pool_id}` as `prepare`,
`status`, `activate`, `cancel-unstarted`, `stop`, `drain`, and `native-runtime`. It requires a dedicated credential
bound to that pool and participant; ordinary users, administrators, generic
workers and observer/gateway credentials cannot substitute. Prepare and first
activation require the protected runtime-profile catalog. The participant client
uses bounded HTTPS requests, checks exact response identity, and never redirects
or automatically retries uncertain writes. Execution-controller/output-drain
integration and protected controller/gateway installation remain required before
global admission can be enabled in the installation.

Native runtime readback derives a bounded description from the retained activated
plan, even when current profiles are unavailable. It returns the receipt, target,
namespace name/UID, deterministic Job name, actual native lease epoch, original
deadline, expected image repository and, when observed, the Job create-effect ID.
It returns no manifests, source payload or credentials and performs no rerender or
external write. The native epoch is distinct from the selection generation. A Job
UID requires its matching observed gateway effect. The runtime controller must
qualify read-only Kubernetes observations against these retained identities before
using publication or failure evidence; readback alone does not implement that
controller or prove a build completed.

The native runtime consumer composes the durable handoff with read-only Kubernetes
observation and the existing native publication/failure recorder. It checks the
namespace UID before and after reads, the exact Job UID and reservation/plan/effect
markers, and one controller-owned Pod. Logs are bounded/redacted and bracketed by
Pod-UID readback. Partial Pod lists fail closed. No observed global Job UID means
no speculative Kubernetes read or local create. The local claim, source and live
demand are rechecked around external reads; current attempts are heartbeated while
waiting, and stale/superseded attempts cannot publish results. Terminal or withdrawn
work enters local `stop_pending`, retaining its capacity charge. The same local
transaction freezes bounded output evidence and the exact stop/drain messages
before management HTTP. Published images come from that attempt's append-only
publication rows, not the materialization's combined history. Valid successful
publication is committed; failed, cancelled or lost-lease output is unavailable.
Restart replays the saved evidence, grace and cause after a lost stop/drain reply.
This consumer is not yet installed: protected startup and writer
migration remain required.

Migration `0172` adds protected pool/participant registrations, immutable request
journals and retained cleanup observations. PostgreSQL enforces unique request
keys and participant-to-pool binding. Registration identities cannot be reassigned;
binding changes require a newer revision, and epochs cannot move backwards.
Requests cannot change their workload, envelope, deadline, namespace or frozen plan.
Only never-started requests can become `cancelled_unstarted` without cleanup.
Started requests remain charged through `cleanup_intent`; a late-discovered Job UID
can be appended there but cannot then be replaced or removed. Release requires a
cleanup observation recorded after cleanup intent and tied to the exact request,
plan and namespace. Request and
cleanup history cannot be deleted, and downgrade refuses retained pool history.

The gateway effect table separately retains fixed write intent, an append-once
dispatch identity/machine epoch, and immutable observed UID/resource version or
definite rejection. Its composite foreign key binds the request, plan and namespace.
A dispatched write cannot reset to prepared, move to another target or disappear.
Database guards reject new create dispatch after intake closure, participant fencing,
epoch change, cleanup intent or deadline expiry. Bound deletion can still dispatch
with intake closed, but its observed UID must match the original deletion intent.
Neither observed nor rejected effects release the capacity request.

The internal fixed gateway journal owns its transactions and commits each one-use
dispatch permit before returning permission for Kubernetes I/O. Dedicated gateway
credentials authorize only their registered pool; callers supply request identities,
not manifests. The HTTP adapter uses the frozen Job/ConfigMap, verifies namespace
UIDs before and after readback, and compares exact workload fields while accepting
qualified API defaults. Native Job creation also rechecks the live, observed
ConfigMap UID and contents. A lost response followed by 404 never authorizes another
create. UID-bound deletion derives its target from the retained create observation;
neither a successful deletion nor an absent Job frees capacity.
Absence and definite rejection require a bounded Kubernetes `Status` response
matching the requested resource name, group, kind, status code and failure reason.
Bare proxy errors or mismatched responses preserve uncertain effects and charged
capacity; they cannot settle a dispatched create or authorize another write.

Residual-Pod inventory scans the complete bound namespace with bounded pagination
and one consistent list resource version. It qualifies original Job UID, name,
reservation/plan/effect markers and local claim generation, including terminal and
terminating Pods. Contradictory identities, partial lists and namespace replacement
fail closed. This inventory is read-only, not deletion or release authority. The
adapter's disposable-cluster tests qualify real API defaulting, restricted writes,
Job-created Pods and object retirement; they do not qualify the protected installer
or a running global worker.

Residual Pod retirement is a separate fixed write in the same effect journal,
keyed by Pod UID and bound to the retained Job create effect, plan and namespace.
It requires output drain and observed Job deletion before dispatch, so deleting a
Pod cannot ask an active Job to replace it. The gateway rechecks live ownership
and markers before one UID-preconditioned, zero-grace DELETE; response loss never
resets permission or retries the mutation. Replaced identities block, and existing
finalizers remain intact. An absent Pod completes only its deletion record, not
the reservation. Complete namespace/object absence and write-fencing qualification
still precede capacity release.

The fixed gateway's cleanup verifier now performs that release qualification.
It snapshots the request, retained stop/drain and all effect identities/phases in
one transaction. A dispatched but unobserved CREATE blocks release even after
404. Prepared creates cannot acquire dispatch permission after cleanup intent;
observed/rejected creates cannot acquire another permit. Outside SQL, the verifier
checks the original namespace UID, exact Job and build ConfigMap absence, and a
complete consistent namespace Pod inventory. Without an observed Job UID, any
candidate by Job name, owner name, reservation or Job-name label blocks absence;
the verifier does not invent a UID or delete unrelated Pods.

Finalization rechecks current gateway credentials, exact snapshot identity and
effect fingerprint, and a maximum 60-second observation window. It atomically
retains the cleanup observation and releases only that reservation. A raced effect,
stale snapshot, changed namespace, incomplete read or transaction failure leaves
capacity charged. Concurrent/lost-reply recovery returns the same retained release
receipt. These internal facts have no public submission endpoint. Unresolved
CREATE uncertainty remains charged until qualified observation or protected
writer-fencing recovery; the latter and installed gateway orchestration remain
separate boundaries.

The local gateway worker now orchestrates these same fixed operations. Its
pool-scoped keyset scan rechecks current gateway authority and closes SQL before
Kubernetes I/O. Creation orders the optional ConfigMap before the Job. Cleanup
recovers dispatched creates by observation only, signals an observed Job before
output drain, then retires the observed auxiliary and owned residual Pods and
runs the qualified absence verifier. It never dispatches a prepared create during
cleanup or resets an uncertain effect. A waiting or failed request cannot prevent
later retained requests from being reconciled. Completed requests leave the scan.
This worker is not yet connected to protected installed startup.

The machine lifecycle API retains separate immutable stop and drain attestations
bound to the exact request, reservation, frozen plan and native/execution lease
generation. Stop fences new creates immediately and freezes termination grace,
capped by the rendered Pod's grace and 300 seconds. The gateway can then send a
UID-bound foreground Job deletion without waiting for output drain. Auxiliary
deletion additionally requires the matching drain attestation: committed/unavailable
output state, output generation, evidence digest and the exact stop digest. Native
output generation must equal its build lease epoch. Neither acknowledgment nor a
successful deletion frees capacity. Lost replies replay the same retained evidence.

The execution cancellation adapter must derive these acknowledgments from the
existing runtime's actual state: signal cancellation while partial-output uploads
remain authorized, then confirm durable committed/unavailable output before final
cleanup and release. The API is not itself proof of that local output state.
The native runtime consumer supplies its own saved publication/failure evidence;
it does not qualify execution output. The execution runtime adapter,
and installed gateway startup remain unimplemented boundaries.
The execution PID1 runtime enforces its original
absolute deadline, but does not itself attest output drain or release capacity.

Global native builds also enforce that same original absolute deadline across
prepare, rootless build and publish. Each container runs the static
`loom-build-deadline` supervisor as PID1; expired startup cannot launch its phase.
The service image includes the supervisor, and prepare copies it into a separate
8-MiB volume mounted read-only by the untrusted builder, without claim or Secret
mounts. The guard disables same-UID process inspection, signals the phase process
group at timeout/cancellation and allows at most ten seconds before exiting PID1;
container teardown also retires descendants that changed process groups.
Existing per-component and Kubernetes Job timeouts remain additional bounds.
The small volume remains inside the existing aggregate Pod storage limit. Timeout
is a result/stop condition, never proof that global capacity has been released.

The native-build local outbox commits an immutable typed selection before contacting
management. It keeps selection generation separate from build lease epoch and permits
only one live selection per materialization, including across participant changes.
The local native controller exposes a separate database-only heartbeat pass for
attached, activation-pending and active attempts. It uses claimed-only keyset pages
and the same current-owner/source/demand checks as runtime reconciliation, with no
management or Kubernetes I/O. Slow admission cannot block this maintenance when
scheduled independently by protected startup. Renewal cannot revive an expired
claim, change saved activation consent or extend the original workload deadline;
stopped, cancelled and superseded work is excluded. Installed scheduling remains
part of the pending protected startup integration.
Receipt acceptance rechecks the exact source snapshot, current demand, deadline,
rollout guard and lease epoch, then atomically commits the existing build attempt
and global reservation link. Waiting and stale selections consume no attempt;
obsolete grants remain recorded for cancellation. Concurrent workers and restart
replay the same selection and grant. A newer admission epoch can recover old records
for cancellation, but cannot use them for a new claim. SQL guards retain request,
receipt and claim identity; local history has no foreign keys to the management
database. Attached claims can record cancellation intent without releasing capacity
or refunding attempts. Only the exact manager `cancelled_unstarted` receipt permits
the same still-current claim to return to queued and refund its attempt budget once.
This retains its immutable attempt and lease epoch, records a truthful no-Job result,
and commits the refund and terminal outbox evidence together. It cannot change a
superseding claim or hide an existing native-build effect. If activation won the race,
the grant remains charged and requires stop/drain reconciliation instead.
Before requesting activation the outbox rechecks the exact current claim, source,
live lease/demand, originating class and rollout intake, then saves consent bounded
by that lease and the original runtime deadline. A heartbeat cannot extend saved
consent. Local cancellation or stale ownership after manager activation retains
the receipt as `stop_pending`, without a refund. The outbox preserves first
activation evidence independently from the original reservation receipt.
After manager-confirmed cleanup, the native controller retains the exact released
receipt and marks only the original attempt and outbox terminal in one local
transaction. It checks the retained activation, native identity and saved
stop/drain/output evidence; neither a success result nor stop/drain acknowledgment
alone can close the handoff. Release recovery never changes the materialization's
result, retry budget or a newer claim. Exact replay preserves the receipt and first
release timestamp. Released selections no longer occupy the local live-selection
key or pending scan, so a later eligible retry can proceed with a new generation.
SQL retains both the original activation and terminal release receipt.
Reconciliation uses bounded keyset pages through the pass's initial high-water
key, closing each database session before external work. Terminal entries cannot
shift offsets and skip later builds, and a full first page of failing requests
does not prevent later entries from being reconciled. Newer selections wait for
the next pass.
This primitive does not yet connect the installed controller and does not
independently authorize an originating application.
Database-backed HTTP tests connect this journal to real management prepare and
activation; this is not evidence that installed controllers use it.
The native handoff driver composes those committed steps with the participant
client. It withdraws stale waiting demand before renewal, checks management status
before activation, and recovers lost replies with the same selection/grant/attempt.
It returns active or stop-pending work to the runtime reconciler; it never falls
back to local capacity admission or direct Kubernetes writes.
The queue selector derives requests from real native-consumer demand, the frozen
materialization and its persisted originating submission. It prefers shared
environment demand over personal-only demand, retaining age within each class;
management still independently qualifies origin and makes the global admission
decision. It excludes live selections, unreleased native effects, completed work,
backoff and exhausted attempts. Unsupported source snapshots or lost selection
races do not consume attempts or hide later candidates. Registered sources use
the retained canonical specification and must pass source admission/pinning in
the final outbox transaction. Generations advance from retained outbox history,
independently of build lease epochs. A configured native controller selects one
new candidate after reconciliation, then uses the same driver/runtime path.
The protected pool operation connects startup and the no-dual-writer transition;
its successful source tests do not establish installation on a live cluster.

The service scheduler separates workload compilation from reservation. Compilation
retains the existing image-readiness and configuration handling, but does not claim
the Trial, consume an attempt, reserve admission/cost/capacity, or append a command.
The legacy scheduler immediately reserves the compiled candidate. A global
consumer must durably freeze its selected target and runtime before prepare, then
recheck local authority when attaching the grant; compilation alone is not a lease.
Both schedulers classify runtime-contract validation and unsupported deadlines as
`service_execution_configuration_invalid`, without retaining private validation
input. Shared-pool proposal compilation uses a savepoint: partial compiler writes
roll back, and the terminal update clears stale scheduling diagnostics while the
same transaction still locks the Trial, task and batch profile. No proposal,
attempt or reservation is created for that failure, and later eligible work can
proceed. Stale selections and temporary provisioning failures remain deferred.
The execution outbox now retains that pre-claim proposal, including its prospective
lease UUID, immutable request and Trial/source/target snapshot. Exact grant
attachment rechecks eligibility and preserves existing local admission, image and
cost checks; only physical provisioning uses the global reservation. The lease
starts with its final reservation-qualified Job name. Claim and outbox attachment
commit together, including a deferred database check. Stale input or local denial
leaves an unclaimed cancellation intent; it cannot activate different work or
silently acquire local capacity. The execution driver persists a non-renewable
activation consent, capped at 30 seconds and the original execution deadline,
before HTTP. Status recovers an accepted activation before any retry; local
cancellation that loses to activation becomes stop-pending, not a release.
Only the exact never-started cancellation receipt closes an attached unstarted
lease and releases its local cost/admission. It retains that lease and attempt
number; execution attempts are immutable identities, unlike native build retry
budget counters. Participant-only execution runtime readback now returns the
retained plan's namespace, Job, unit, generations, deadline and observed gateway
effect identity, without manifests or credentials and without requiring current
profiles. Its read-only Kubernetes adapter qualifies namespace UID before/after,
the exact observed Job and sole controller-owned Pod before reusing existing
execution normalization. A missing Job is only absence, never deletion or release
authority. The local execution outbox separately commits immutable stop and drain
messages before HTTP. Stop derives from existing lease revocation and does not
wait for output; drain requires that lease's committed/unavailable output at its
original resource generation, retaining the exact manifest/marker or unavailable
evidence. Lost replies replay the same records and grace deadline. These messages
do not close the output window, project deletion, or release capacity. Only the
manager's exact released receipt, including its cleanup-observation reference,
permits the local outbox to project deletion through the existing execution event
handler. The receipt, local deletion and outbox completion commit atomically;
a deferred database guard rejects global-lease deletion without retained release
or never-started cancellation evidence. Replays preserve the old lease/attempt,
and release permits the next queued attempt's distinct selection. The existing
execution actuator now accepts this global resource adapter: bounded pending
scans advance durable handoffs, qualify namespace/Job/Pod observations, and reuse
the existing result finalization and usage recording. It sends stop before output
drain, preserves the output deadline even without a locally observed Pod, and
projects deletion only from the retained manager release. A missing Job never
authorizes a new create. Global mode rejects legacy namespace watches and has no
local provisioning or Kubernetes write fallback. The existing scheduler loop can
now select global proposals from its normal queued-Trial eligibility contract;
it preserves team fairness within shared/personal priority, skips live proposals
and incompatible candidates, and never falls back to local reservation on an
empty or failed global selection. Image-preparation and configuration failures
retain their existing no-attempt terminal semantics. Node-share compilation now
fetches participant/target/epoch-qualified sizing evidence from the management
observation before opening local SQL. It uses measured allocatable minus resident
DaemonSets, including compatible retained samples after scale-zero, never free
resources or environment-local totals. The local proposal rechecks freshness and
freezes the evidence and allocated runtime together. Missing evidence cannot fall
back to local allocation. This sizing read grants nothing; the registry still
checks current physical fit and provider quota for the rendered workload.

Protected process configuration now selects these adapters at startup. The control
plane's `service_execution_global_pool_json` and actuator's `global_pool` bind the
same registered participant, data environment, logical pool, HTTPS management
origin and private machine-token file. Configuration rejects mismatched scheduler,
target or namespace identities. Global mode fences direct admin reservations,
omits the legacy namespace watch and uses the native build outbox controller.
Build lease maintenance runs independently of admission HTTP and participates in
readiness. Shutdown cancels and awaits every controller loop before closing the
management, Kubernetes or database clients, including when another loop fails.
Global namespace/workload/log reads and usage sampling retain ownership through
cancellation until their bounded SDK calls finish, before client closure.
Cancellation still propagates if an SDK call fails during that drain.
Failed execution observations retain the ordinary scrubbed container-log excerpts,
with bounded reads and exact Pod UID/ownership checks before and after fetching
logs. An unavailable log does not erase the failure observation. Participant reader
roles permit log GETs only in their execution/build namespaces, never Pod exec.
These settings
do not register participants, install profiles, grant Kubernetes authority or
perform the protected writer migration.

Management startup loads `pool_profiles_file`, a bounded installer-owned
`loom.pool-profiles.v1` JSON catalog. It contains separate execution and native
build entries keyed by the registered profile UUID, plus public image-admission
keys. The loader rejects duplicate identities/JSON fields, unknown fields,
unqualified node groups or runtime overhead, and mutable trusted images without
echoing configuration in diagnostics. Both entries reuse the existing renderers;
no owner API can replace the catalog. The file grants neither registration nor
Kubernetes authority. Without it, prepare and activation remain unavailable;
retained status and cleanup do not depend on current rendering profiles.

Protected management deployment inputs retain `pool_catalog_operation_id` after
pool migration. The renderer mounts that exact operation's immutable catalog and
preserves the pool manager's `Recreate` strategy. Ordinary image/config refreshes
must retain the reference and its read-only mount; they cannot introduce, remove
or rebind a pool catalog. An absent reference leaves historical serialization and
rendering unchanged. Pool wiring advances each retained workload's same-image
initialization containers with its main image and rejects foreign initializer images. These
rendering checks do not establish a completed migration predecessor, catalog
ownership or live writer fencing; the connected protected operation must prove
those before applying the configuration.

The fixed gateway has a separate `python -m loom_service.pool_management` process.
`LOOM_POOL_GATEWAY_` settings bind its management database, pool/installation/
machine UUIDs, admission epoch, private machine-token file and explicit projected
Kubernetes connection. Startup checks the schema and dedicated gateway identity
before opening Kubernetes credentials; only closed/global pool modes qualify.
Every pass reopens the machine token and resolves current authority; each journal
mutation rechecks it under the existing locks. Kubernetes requests renew the
projected service-account token, verify the explicit CA/origin, and neither follow
redirects nor use ambient proxy credentials. The process runs the existing fixed
gateway worker, not caller manifests. `/readyz` requires a recent successful pass;
failed authorization or reconciliation clears readiness. Server termination stops
and drains background work before its HTTP clients and database engine close.
The entrypoint does not provision its own RBAC or bypass the protected migration.

Its fixed installation renderer separates configuration, gateway authority and
workload phases. It renders the gateway Deployment with zero replicas and a
dedicated ServiceAccount, projected rotating Kubernetes token/CA, and a distinct
owner-only machine token copied by a non-root initializer. The machine token has
no Kubernetes authority; the projected token is not a management API credential.
Only the registered execution/build namespaces receive Job/ConfigMap create and
delete plus Pod observation/cleanup rights. Cluster scope permits exact namespace
identity reads, not namespace listing, Secrets or RBAC access. The protected
migration still owns Secret delivery, old-writer fencing, manager catalog wiring,
startup and admission activation; rendering these documents performs none of them.

The protected runtime target builders preserve retained Deployment UIDs and
database/storage Secret references while pinning the integrated candidate images.
The manager mounts the immutable renderer catalog. Each shared control plane and
actuator receives the same participant binding and a dedicated owner-only machine
token; the shared API receives only its environment submission identity, never a
machine token. Hash-qualified credential delivery is immutable and create-only,
with exact namespace UID checks and no automatic credential rotation. All target
Deployments have zero replicas: these builders do not perform the protected
cutover, install RBAC or activate admission.

Runtime wiring also requires the published, signed execution profile, not just
new application images. Its candidate/image/binary identities must match the
global renderer catalog while retaining the environment's existing resource and
capability policy. The shared API receives that profile, and control-plane/
actuator image-admission keyrings are bound to the catalog together. This prevents
a newly deployed API from continuing to compile tasks against a rejected old
runtime. The protected caller still qualifies the publication and supplies the
environment-qualified profile; the wiring does not invent admission signatures.

The shared-development capacity CronJob is retained as the one pool observer,
using its existing read-only Nebius credential and a dedicated observer token in
that execution namespace. Its immutable configuration selects pool collection,
binds the exact node group and provider quota identities, and removes the legacy
control-plane publication URL/token. The rendered CronJob remains suspended until
the migration retires the other collectors and qualifies the global runtime.
Participant reader-role targets replace the existing actuator/builder roles,
allowing only scoped Job/Pod observation and native build logs, plus exact
namespace identity reads. They grant no Job creation/deletion. Applying these
roles alone is not proof that every old writer has been fenced; the protected
migration must verify all effective bindings and old-process retirement.

Initial registration uses the fixed `loom_service.pool_management.installation`
Job and a versioned `loom.pool-installation.v1` configuration. It binds physical
pool identity, exact participant namespaces/targets, the renderer catalog and
dedicated machine credential hashes. The database transaction serializes with
admission, rejects drift or revoked/expired authority, and registers the pool
**closed**. Exact replay never rotates credentials, resets state or opens intake.
Historical configurations remain readable after credential expiry; new writes
check validity against the database clock. The Job reports only a bounded receipt
after commit and receives no Kubernetes token or write role.

The protected registration stage retains exact ConfigMap/Job identities and
uncertain-create evidence. A missing response followed by absence does not permit
another CREATE. Its read-only execution verifier requires the recorded Job and
unique, unrestarted successful Pod, exact runtime/configuration, and matching
closed-registration commit receipt. Changed identities, configuration, logs or
final readback cannot qualify. Staging alone proves neither database registration nor writer retirement;
the parent migration must qualify candidate publication, namespace ownership,
successful runtime execution and the no-dual-writer barrier before opening intake.

The migration's initial closure stage binds every qualified data participant and
its retained control-plane Deployment/namespace identity. Environment classes do
not imply a fixed number of installed databases. The protected preflight must
qualify the complete installed participant/writer inventory; closure requires
exactly one original controller guard per participant and rejects missing,
duplicate or foreign guards. It uses the existing
`nebius_rollout_guard`, retaining earlier idle guards while another environment is
busy. A lost acquisition response requires exact owner/candidate observation; an
open database after an uncertain acquisition does not authorize another command.
The fixed command adapter qualifies the running Pod and ReplicaSet lineage and
unchanged template before and after invoking the guard, with no release command.
For recovery after controller retirement, a retained database binding selects the
same namespace-local PostgreSQL StatefulSet and Service. The adapter verifies
their identities and templates, the original controller's exact DB Secret
UID/version and connection destination, and the ready StatefulSet Pod before and
after the fixed read-only ownership query. Changed credentials, alternate database
destinations or unqualified Pods fail closed. Acquisition still uses the original
qualified controller; the database path never acquires or releases a guard. The
protected parent must supply the binding before retiring that controller.
This direct-database binding rejects an unresolved pooled-engine override; the
parent must qualify pooled-to-backend correspondence before using that topology.
The same retained database binding supports a separate fixed runtime-role stage.
It requires the exact idle rollout guard and schema, rejects privileged or
role-member actuator identities, and grants only local outbox reads/inserts/updates
and source-row lock columns. Task content, batch identity/provenance, management
capacity/credentials and journal deletion remain denied; qualification includes
column-specific grants. Registered-source builds may read published source and
incarnation records and insert materialization pins; they cannot publish, mutate,
retire or unpin sources. The source-row lock column is immutable, and qualification
requires complete pin-insert authority, not merely one insertable column.
An uncertain stage response is recovered by a read-only
qualification, not a repeated write. Neither action releases a guard or opens
admission. Normal bootstrap retains pre-cutover permissions; a connected protected
installer or refresh that replays bootstrap must restage and qualify these runtime
permissions before reopening intake. This stage is not an installed cutover.
An independent anchor and parent journal bind closure to registration; missing or
changed recovery evidence cannot start another registration. Successful closure
and registration explicitly leave writer migration incomplete. Controller
retirement, RBAC changes, global runtime installation and activation remain later
barriers; none is implied by a closed-registration receipt.

The separate controller-retirement stage retains that closure evidence without
replaying commands in control-plane Pods after stopping them. It suspends the
recorded collectors, then scales the recorded actuators and control
planes to zero, preserving their UIDs and Pod templates. Exact UID, resource
version and spec preconditions bound each PATCH. Lost or unqualified responses
retain write intent and permit readback only; only a complete Kubernetes conflict
or invalid-request rejection permits another attempt. Complete namespace
ReplicaSet/Job/Pod observations must prove drain, including terminating Pods and
all collector container states. Replay rechecks earlier stopped workloads without
writing. This stage neither changes RBAC nor activates a replacement writer;
the subsequent authority-fencing and runtime stages belong to the complete
protected pool operation, not to retirement alone.

An installed execution-only guest actuator belongs to its ordinary data
participant, not another database, collector or builder. Retirement and runtime
wiring require the complete registered target set, including both the ordinary
guest and the separate emulated-authentication guest when installed. Each sibling
must retain its renderer-defined name, mutually distinct UID, shared database references,
ServiceAccount and ordinary Pod configuration, differing only in target identity,
labels/affinity and absence of the native builder. Missing, duplicate or changed
siblings reject the migration inputs. Each replacement remains stopped, receives
the same participant credential and global binding, and does not acquire a build
loop. Guest Pods must drain before retirement qualifies; replay checks them again.
These checks do not replace installed database or effective writer qualification.

The retirement contract separately accepts an explicit dormant remote-consumer
roster through the optional `dormant_consumers` private cutover input; an omitted
roster does not authorize adopting undeclared consumers. Each entry binds an
already-zero actuator and suspended collector in an existing participant's
execution namespace. The actuator must retain that
participant's database Secret reference, use a distinct fixed ServiceAccount and
target a namespace outside this operation. A registered target cannot be
reclassified as dormant. Both retained UIDs and templates join the ordinary
retirement journal, complete process-drain checks and effective permission review.
Replay rejects a restarted or changed consumer. Their remote namespaces and pools
are not added to operation scope, and they are not wired or started as successor
participants. Unexpected grants remain rejected; dormancy is not credential
revocation. This is retirement-only support, not an installed cutover claim.

The participant-role phase composes that retained retirement barrier with two
fixed Role replacements per participant: the existing execution-actuator and
task-image-builder roles in its namespaces. It preserves Role UIDs, bindings and
unrelated metadata, changing only the recorded rules to the fixed reader rules
and adding its operation marker. Exact preconditions, retained update intent and
bounded readback handle lost replies without uncertain retries. It rechecks the
stopped workloads and restricted roles on replay. It also obtains complete
effective rules for each retained controller/collector ServiceAccount across all
qualified management, data, execution and build namespaces. Fixed per-request
impersonation includes the actual ServiceAccount groups; no runtime token or
persisted probe is created. Named grants are included, so an extra binding cannot
hide behind an unnamed access probe. Only explicit reader resources, discovery
and standard self-inspection qualify; credential access, indirect writes,
wildcards, incomplete rule resolution and evaluation errors reject the phase.
Recovery repeats these nonpersisted authorization reviews without repeating
confirmed Role writes. The operator must already have the required impersonation
authority; unsupported resolution has no permissive fallback. The phase does not
create bindings or gateway permissions, and its receipt still marks writer
migration incomplete: the protected parent owns complete external-writer
inventory and fresh qualification before activation. It never removes an
unexpected grant automatically.

Connected cutover preflight now reads complete, stable-paginated Role, ClusterRole,
RoleBinding and ClusterRoleBinding collections before producer downtime. Each
collection is pinned to the same API-server resource version, then requalified
after the other preflight reads. Every retained Role must have its exact original
or intended reader shape and UID, with the expected actuator subject. Foreign
subjects sharing a reduced Role, unresolved
references, duplicate identities and extra named/group/cross-namespace grants to
retired identities reject preflight. Separate Job writers in participant execution
or build namespaces and unregistered cluster-wide Job writers also reject,
including User and Group subjects. Discovery/self-inspection readers and unrelated
namespace-local grants outside the pool are preserved.
CronJob mutation grants count as indirect Job authority: the native CronJob
controller can create Jobs without a Job grant to the schedule's creator. Every
existing CronJob in a participant execution/build namespace must also be an exact
retained original, even with a different ServiceAccount or `suspend: true`.
Revoking the creator's credential does not retire an existing schedule. Unknown
schedules reject preflight without being adopted, suspended or deleted; schedules
in unrelated namespaces and foreign Job/Pod occupancy remain untouched.

Kubernetes controllers and provider administrators are an explicit platform trust
boundary, not writers the application migration can fence. Protected private inputs
must retain their complete ClusterRoles and ClusterRoleBindings with original UIDs,
bound to the kube-system namespace UID and immutable cutover journal. Live objects
must match these snapshots. Supported administrative bindings are `cluster-admin`
for `system:masters`, `kubeadm:cluster-admins`, and the `nebius:admin`/`nebius:editor`
groups, with their corresponding fixed role references. Native CronJob, Job, garbage
collection, namespace and finished-Job TTL controllers require exact bootstrap
bindings and bounded native rules; aggregation, widened capabilities, extra subjects
and alias bindings do not qualify. Application workloads cannot borrow these native
ServiceAccounts, even at zero replicas. The installer grants or removes none of
these platform permissions and does not claim to revoke external administrator
credentials. Effective reviews after participant-role reduction and the parent's
installed qualification remain required; this is not full runtime acceptance.

The same snapshot also inventories Deployments, ReplicaSets, StatefulSets,
DaemonSets, ReplicationControllers, CronJobs, Jobs and Pods. Terminal Pods are
included; zero replicas, suspension and Pod phase alone do not exempt an unregistered
consumer of a retiring ServiceAccount. Every retained root must match its original
UID and exact original or journal-qualified recovery template. A descendant must
resolve through an exact same-namespace, same-ServiceAccount controller chain:
Deployment → ReplicaSet → Pod or CronJob → Job → Pod. Dangling, replaced, cyclic
or contradictory ownership rejects preflight. Historical descendants can remain
without deletion; this check establishes identity consumers, not execution health
or shutdown. The existing drain and effective-permission barriers still apply.
The standalone foundation's web, LLM gateway and platform-backup roots may be
retained separately as `platform_consumers`: they share the control plane's
tokenless account but are not retiring writers. The protected entry qualifies
their fixed names, namespace and templates against the completed predecessor's
foundation configuration and the selected protected candidate/profile/keyring.
The cutover contract then pins their UIDs and complete stable observed snapshots;
the same live inventory and typed ancestry checks cover them and their descendants.
They are never mutation or drain targets. Unknown control-plane copies, changed
consumer templates, execution/build identity reuse and additional Kubernetes
grants still fail qualification. No blanket account or tokenless-Pod exemption
is introduced. An empty roster retains the previous journal contract.
Completed standalone platform migration/configuration/backup Jobs are census-only
history when the native non-indexed singleton Job has exactly one true `Complete`
or `Failed` condition, no active/terminating count, no owner, an automatic UID
selector and `restartPolicy: Never`. Only retained CP identities outside execution/
build namespaces qualify; any identity also used by an actuator, collector or
dormant writer is excluded. The same snapshot must prove every Pod owning that
Job UID or matching its selector has exact same-namespace/account Job ancestry,
a terminal phase and complete, uniquely matched terminated regular/init/ephemeral
container statuses. Native-controller non-restart behavior is covered by disposable
Kubernetes tests. This adds no retained input, mutation, deletion or drain target;
ambiguous, active, custom-managed and indexed Job history remains rejected.
Unrelated identities remain untouched. External credentials and custom-controller
authority still require the parent's separate installed qualification.

On recovery, successor gateway grants qualify only through the bound parent
anchor, retained closed/fenced receipts and fixed authority-stage journal. Exact
recorded UIDs/snapshots or unresolved CREATE intents are observed without writes;
matching names/labels do not authorize adoption, and aggregation cannot widen a
partial stage's authority.

An exact workload preview that receives a complete definite Kubernetes rejection
leaves that workload prepared and returns a pending update. It records no mutation
intent and sends no persistent update. The next invocation reads the current object
again; malformed responses and uncertain outcomes remain failures, not retry
permission. This covers controller-status resource-version races without weakening
the actual write's UID, version and template preconditions.

The connected cutover parent now sequences producer shutdown, closed
registration, workload retirement, effective-role fencing, dedicated material,
runtime ACL qualification, gateway configuration/authority and disabled runtime
replacement. Targets are generated from the retained manager, complete shared
API/actuator roster and one development collector; arbitrary manifests are not
inputs. Producer Pod drain is followed by independent application-access,
schema-readiness and queued-origin qualification. It cannot manufacture provenance
for a legacy queue. The fixed live adapter now reads schema `0173`, idle guard/
work state and personal-access quiescence through each retained database Pod,
qualifying its StatefulSet, Service, Secret version and Pod identity before and
after every read-only page. Complete EndpointSlice readback also binds the Service
UID, port, address family and ready/nonterminating backend to that exact PostgreSQL
Pod UID/IP. Missing, additional, foreign or changed backends reject even read-only
SQL. A direct database binding rejects an unresolved effective pooled URL for
either the control plane or management service. Bound guard acquisition also
checks the old control-plane container's loaded effective database URL against
that pinned credential before opening SQL. A fixed in-container command loads
the real typed settings once, checks a fresh HMAC challenge, then passes the same
settings instance to the existing guard functions. Pooled or image-local `.env`
overrides cannot silently close a different database. Only the challenge and
response appear in arguments; no URL, password or configuration is emitted.
Retained images need no new CLI option. Recovery still observes ownership through
the qualified PostgreSQL Pod and never retries an uncertain acquisition. This
check qualifies the acquisition backend. The private reader entry additionally
checks every retained participant control plane, shared API and ordinary/guest
actuator before yielding operator access. The actuator's namespace-local copied
database Secret has its own pinned UID/resource version in the migration contract;
it is not assumed identical to the platform Secret. Each original running consumer
must have the retained Deployment/ReplicaSet/Pod lineage and load the exact
effective URL resolved from its pinned Secret. The fixed read-only probe uses that
image's real typed settings and a fresh HMAC; it opens no SQL connection and emits
no credential. The resolved destination must be the already-qualified participant
PostgreSQL backend, not another database or an unresolved pool/proxy.
Recovery derives workload state from the same anchored parent validation used by
the cutover, plus the retained closed-registration and retirement journals. It
accepts only the original or precisely recorded stopped/rewired template. A state
file or zero replicas alone never bypasses the running check. Recorded stopped
consumers retain backend and credential-reference checks without trying to execute
in retired Pods; drain and successor startup remain separate mandatory barriers.
The predecessor manager is independently bound to its management database by the
same real-settings probe and phase-aware recovery checks, never by a participant
credential. Before returning operator access, the entry also rereads the retained
collector ConfigMap and uses the production Nebius reader with the **collector's
existing cloud credential**, not the operator's credential, to qualify its actual
node group, parent cluster and native quota identities against the protected
foundation. The private input and immutable cutover journal bind the source Secret's
UID, resource version and content digest. The read requires that exact, undeleted
Secret and fixed key before and after the provider request. An owner-only temporary
file carries the credential to the SDK and is removed on success or failure; neither
raw credentials nor SDK errors enter public reports. Ambient collector settings
cannot supply or override these inputs. Changed configuration, credential, provider
scope or quota identity rejects qualification before producer downtime.
The collector renderer also binds the fixed projection, initializer and read-only
consumer mounts; alternate volumes, command arguments, initialization environment
or lifecycle hooks cannot substitute another credential. This qualifies the retained
credential route and its cloud access, not a successful successor collector process.
The read accepts a scale-zero pool and does not reserve headroom, request nodes or
introduce a per-environment budget. Workload fit and actual collector startup still
require qualification; detailed kubelet sample availability is reported separately.
These checks do not prove the complete
external-writer inventory or an installed global activation.
The personal-access readiness routine must retain
the installed body, language, owner and security/search-path attributes; a
same-named replacement is not evidence of retired access. The queue includes
delayed trials/build consumers and unfanned native batches; quota, retry and target backoff do not hide future
work. Unknown origins, inconsistent inherited batch origins and ambiguous legacy
batches cannot be backfilled or silently assigned shared-development priority.
Every observed origin still requires independent management-registration/history
qualification before the parent advances; JSON parsing is not that authority.
The HTTPS adapter requires a separate history reader for every pending page.
Its fixed `REPEATABLE READ READ ONLY` query projects original application identity,
owner, deployment generation, registration and source release from the retained
management database, not the participant database. The manager's original database
Secret UID/version, Service, StatefulSet and current Pod are checked before and
after the read. The same history validator serves locked ordinary admission and
protected readback: suspended or destroyed applications may retain legitimate
older queued work, but missing history, changed source, wrong incarnation,
environment or cluster fail closed. No probe Job, new grant, source rewrite or
admission opening is performed. The fixed scope derivation reloads completed
upgrade/refresh evidence and the hash-bound original database/material journals,
then checks the live database credential against its original immutable material.
It preserves the latest completed manager template and never replays the original
installation. The history reader must match the cutover's exact manager and
migration inputs before transport creation. The private cutover input loader now
derives the manager from reloaded completed receipts and validates operation/path,
physical pool, participant database and machine-material bindings before operator
connection. Its reader context connects the history derivation and both fixed SQL
readers to the same explicit native API endpoint, CA and short-lived bearer. A
fresh private kubeconfig embeds only that authority, never ingress credentials,
ambient contexts, exec plugins or client certificates; it is removed on exit,
including parent failures. The gateway runtime separately uses its projected
service-account authority at the fixed in-cluster endpoint. The reader context also
resolves the exact protected GitHub publication before obtaining operator authority:
successful publication attempt, merged squash identity, current-head Actions-app
gates, artifact digest and signed runtime bytes must agree. Trust comes from the
completed manager predecessor, not the supplied pool catalog. Every participant's
replacement image/signature fields must match those publication bytes while its
retained environment policy stays unchanged. Private inputs are reloaded after
the remote read before obtaining operator credentials. Complete installed writer/
inventory qualification is still required; parsing publication labels is never
approval. No operational cutover command or admission activation is exposed by
this reader context.

The fixed connected cutover API now composes these readers with concrete
private/runtime/provider checks and the existing guard and registration adapters.
Its child migration preflight invokes the same parent inventory and readiness
checks. Schema, application-access and original queued-origin qualification run
before producer downtime as well as after producer drain, including management
history qualification for empty participant queues. A schema mismatch requires
the ordinary protected upgrade and its backup contract, followed by refreshed
cutover inputs; this path does not introduce another DDL mechanism.
Registration uses only the operation's fixed `writers/registration` journal,
requires every participant guard to remain held, and preserves the existing
uncertain-create readback rules. A created Job remains pending until its actual
successful Pod and committed registration receipt qualify. Private inputs and
reader bindings are rechecked at the concrete barriers; all child HTTPS clients
and temporary reader credentials close with the context. Creating this context
does not stage resources or open intake. A protected workflow operation,
installed participant completeness, collector startup, activation, rollback and
durable successor refresh remain required for an operational global cutover.
Runtime ACL intent is retained separately per participant;
unknown SQL outcomes permit qualification only, not repeated grant commands.

The internal successor-startup stage binds a separate journal to the exact bytes
of the completed closed parent and its retained workload identities. It starts
only the manager, fixed gateway, participant adapters and selected pool collector;
other collectors and explicitly dormant foreign-target roots remain stopped.
Startup changes only replicas or suspension, preserving the retained Pod templates.
Every write records its original resourceVersion before one compare-and-swap
request. A lost reply permits exact readback only, never a retry; seeing the old
replica count does not prove that a delayed request cannot still commit.
The connected startup adapter reuses the parent's HTTPS and fixed SQL authority.
It rechecks the closed registration, held local guards, restricted effective
writer permissions and exact staged material/configuration/authority identities.
Every persistent PATCH tests UID, resourceVersion and the complete current spec
before changing the one replica/suspend field. Definite API rejection permits a
fresh prepared attempt; ambiguous responses never do. Disposable Kubernetes tests
cover server defaults, retained identities and a lost committed PATCH response.

After startup intent exists, the old closed-stage mutation path refuses replay.
Read-only writer inventory and database readers accept only the recorded before
or after template for an uncertain start. They retain namespace, workload UID,
database and credential checks without depending on an unready successor Pod.
The retained management-database reader also exposes a fixed READ ONLY startup
check for the exact closed pool epoch, participant and machine roster, binding
digests and current dedicated credentials. It rejects changed/revoked/expired
authority rather than replaying registration to reset it. Backend and operator
identity are rechecked around that read; no raw bearer appears in its report.
This keeps recovery available; it is not runtime acceptance. A separate read-only
database-runtime barrier requires every startup write to have a settled `started`
journal entry, then selects the exact recorded successor templates itself. It
probes the manager's own backend and each participant controller, service and
ordinary/guest actuator through their original database credential identities.
The fixed probes allow only the expected replacement while retaining Deployment
UID, namespace/name, selector, container and ServiceAccount identity. They check
current ReplicaSet/Pod lineage, readiness, loaded effective database settings and
the unchanged pinned Secret reference; equivalent Kubernetes resource-quantity
spellings do not cause false drift. Actuator probes additionally check direct
telemetry against the current pool-node roster, reporting known sampling failures
as unavailable without relaxing runtime identity checks. A second fixed challenge loads
that image's real settings classes inside the qualified Pod and verifies the
effective global participant binding, enabled controller scheduler/materializer,
image-admission keyring, actuator target/builder configuration and shared API
submission identity/runtime profile. It reads machine tokens through the normal
current-UID-owned `0600` file reader and compares their hashes with the exact
registered participant credential; no operator credential is substituted.
The manager's settings must select the retained catalog path, and its normal
profile loader must accept bytes matching the immutable installed catalog before
and after loading. The gateway has no running predecessor: its closed child UID
comes from the completed startup journal, and only a replica-count change may
separate it from the running template. Its real settings must select the registered
pool, installation, machine and admission epoch, the exact owner-only machine
token and the projected Kubernetes connection. Its effective database URL must
resolve through the same retained management `service-url` Secret reference and
qualified backend; participant credentials cannot substitute for that binding.
Only the role and fresh challenge enter exec arguments, and
only a bounded qualification result leaves the Pod. Import, configuration and
file errors emit no configuration or token values. These settings checks make no SQL or
network request, issue no credential and open no admission.
Closure and all workload roots
are rechecked afterward. An unhealthy successor does not prevent constructing
the independent recovery connection. This barrier is not a saved health receipt,
complete gateway/collector acceptance or permission to activate admission.
The separate gateway-authority barrier resolves effective permissions through
nonpersisted SelfSubjectRulesReview requests with request-local gateway
impersonation. It checks every operation namespace and any foreign RoleBinding
namespace naming the account, equivalent user or its groups. Required permissions
must match the fixed renderer; extra named-resource grants, wildcards, Secret
access and unresolved rules are rejected. Ordinary self-inspection/discovery is
allowed. No bearer is minted, no grant is changed, and no impersonation persists
on the parent client. This proves effective authority, not runtime connectivity.
The bound gateway runtime barrier separately uses its actual projected token and
CA with the normal credential reader and origin-restricted HTTP authentication.
It reads only the registered execution/build namespace names and requires their
exact UIDs. Its challenge binds the loaded pool/installation/machine/epoch and
Kubernetes connection as well as every returned namespace identity. Each request
reopens the projected token for rotation; neither ambient kubeconfig nor proxy
credentials are used. Reads have per-request and total deadlines, bounded
uncompressed responses, verified TLS and no redirects or retries. This probe
issues no credential, changes no resource and returns no token or API error body.
Pod, database, credential and parent closure checks still surround the proof.
Disposable Kubernetes coverage exercises the real service-account permissions,
TLS/credential files and namespace UID matching, but not an installed gateway Pod
or concurrent-owner task execution.
The same bound gateway also authenticates its current dedicated machine credential
to management and runs the existing connected-capacity admission reader. That
reader requires a fresh accepted observation, exact current registration/capture
digest, physical node-group and provider-quota identities. The challenge separately
binds those current registration values to the protected installation, active
participant roster and gateway credential. It neither publishes a new observation
nor substitutes operator-supplied capacity. These fixed SQL reads use the existing
management mutation lock and row locks in a bounded `READ COMMITTED` transaction,
which is always rolled back; this is non-mutating qualification, not a SQL
`READ ONLY` transaction. Pod/backend/credential identity and closed authority are
rechecked afterward. The result is not a saved capacity grant: admission opening
must requalify current evidence under its own locked transition.
The internal fixed opening primitive repeats that qualification and changes only
the exact `closed/R` registration to `global/R` in the same transaction. Token
expiry and every connected observation's freshness are checked again at the SQL
write, and a success report follows commit. The separate protected recovery SQL
can observe the original, opened or fenced binding without a running gateway or
runtime token. Its cancellation fence takes the same mutation lock and changes
either `closed/R` or `global/R` to `closed/R+1`. Repeated fencing leaves that
revision unchanged. The old opening challenge can never authorize admission after
this fence, including when it was already waiting on the database lock. Opening
requires room for this successor within the canonical-JSON integer range.
Readback and fencing bind the exact installation, physical pool and immutable
configuration; an unrelated revision or configuration is rejected. Neither
cancellation nor readback releases charged requests, clears effects, releases
local guards or restores legacy writers. A failed transport response is ambiguous
and requires readback, not a repeated opening or fencing dispatch.
The internal activation stage anchors a child journal to the closed-stage and
startup-journal bytes. Opening requires all startup writes settled and fresh
runtime proof. Each opening, local-guard release and cancellation write has a
persisted intent before its one dispatch. Recovery accepts only the before/after
states permitted by that intent; an unchanged state or lost response never
authorizes a retry. Guard dispatch order follows the protected participant roster,
including after sorted JSON journals are reloaded. Once activation evidence
exists, the startup mutation entry refuses replay.
Cancellation also works before startup completes and does not require healthy
successor processes. It first confirms the global revision fence, then fences
each local intake guard under its existing admission lock. The same guard row is
transferred to `pool-recovery:<operation UUID>`, or inserted with that owner if
the original release already committed. Foreign ownership is never adopted.
An original-owner release already waiting on the row cannot delete the recovery
owner. Active work is retained; fencing does not assert idle state or completed
cleanup. Every stage result explicitly withholds legacy-restoration authority.
The fixed activation transports use the retained management or participant
PostgreSQL Pod and qualified Service backend, with private operator scope and
identity-bound reports checked around each dispatch. Recovery needs neither a
running application/gateway nor its runtime token. A separate read-only runtime
ACL inspection accepts active work and absent/recovery guards while preserving
the same schema and least-privilege checks; the initial role stage/observation
still requires the original idle guard. Inspection never repairs grants or work.
Opening uses the exact gateway Pod from fresh settings, database, projected-API
and capacity qualification. A separate fresh challenge invokes only the fixed
opening command, once. Its result and the retained Pod/backend/operator scope are
checked afterward; a lost reply or late drift is unconfirmed, never retried.
The connected activation adapter shares the parent's temporary operator authority
and validates anchored intent before each write. It retains private-input,
provider/backend, complete writer-inventory, restricted-role and staged-resource
checks during recovery, without rerunning initial idle/backlog or runtime-health
checks. Opening separately requires the full fresh startup runtime and gateway
authority barriers. A guard release requires the same global pool; recovery guard
fencing requires the confirmed terminal pool fence. Neither connection construction
nor recovery restores or restarts workloads.
After global and local cancellation are complete, a separate anchored startup
fence resolves any still-uncertain original startup PATCH. A different current
resourceVersion on the same qualified workload UID already invalidates that
original compare-and-swap. If the version is unchanged, the operation persists
intent and adds only the fixed top-level `loom.nebius/pool-startup-fence`
annotation, containing its operation UUID. UID, version, complete metadata and
spec tests protect that write. The real metadata change advances the version;
an unchanged replica count or a no-op PATCH is not cancellation evidence.
If the original startup wins first, the fence loses its version test; if the
fence wins, the original startup loses. Unknown replies are observed without
redispatch. Only an explicit API rejection permits another prepared attempt.
The child journal freezes the activation/startup hashes and each settled exact
template; entry, writer inventory and activation recovery share that projection.
Foreign markers and unanchored changes are rejected. Settled or never-dispatched
startup rows require no metadata write. This barrier leaves Pod templates and
running cleanup processes unchanged, does not prove process drain, and still
withholds permission to restore legacy writers.

Recovery also reads both sides of the existing handoff before successor shutdown.
The retained management database must still have this operation's terminal
revision fence and no waiting/reserved requests, active requests, uncertain
CREATEs, or releases lacking their matching cleanup/output evidence. Each
participant database must retain the exact recovery owner and candidate and have
no active claims, execution/output cleanup, native build cleanup, or unfinished
execution/build outbox. A selected outbox blocks even before any attempt or Pod
exists; queued work without a handoff remains available for later admission.
These fixed queries run in read-only snapshots through the retained PostgreSQL
Pods. The connected recovery adapter requires the settled startup fence and
rechecks retained authority and all local/global fences around the observations.
It never persists a zero-count result as a reusable permission. Existing
participant controllers cancel unstarted handoffs or finish result/output drain;
the existing gateway verifies complete cleanup before releasing capacity. A lost
CREATE followed by absence remains charged and pending, without redispatch or a
fabricated rejection. This read-only barrier itself neither stops processes nor
restores legacy writers.

Once both journals drain, an anchored shutdown stage stops only the successor
startup targets in reverse order, leaving the gateway and manager until last.
Its fixed updates change only Deployment replicas to zero or suspend the
collector CronJob; templates, UIDs, startup-fence annotations and dormant siblings
are preserved. Each one-time update retains its original resourceVersion before
dispatch and tests the UID, version, complete metadata and spec. Unknown replies
are observed, never resent; definite API rejection alone permits another prepared
attempt. Already-stopped roots need no update. The shared recovery projection
accepts only the anchored before/after templates, including partially started
operations. The connected transport rechecks fresh two-sided drain before each
PATCH and accepts no caller-supplied replacement manifest.
Completion requires complete ReplicaSet/Job and Pod inventories for every
successor, acknowledged Deployment generations with zero replica counts, and
terminal collector history. Terminating Deployment Pods still block; paginated
or incomplete lists cannot prove drain. Global/local fences and both journals
are rechecked afterward.
This phase neither deletes resources nor revokes cleanup credentials, and still
withholds legacy-restoration authority pending successor credential retirement.

An anchored machine-retirement stage subsequently rechecks both journal drains
and every successor's process inventory before one credential transaction. The
transaction takes the existing global mutation lock and locks the exact retained
binding, participants, machines, credential bindings and tokens in authentication
order. It requires the terminal pool revision, exact installation roster and
fresh global drain, then marks only those machines revoked and timestamps only
their dedicated tokens. Expired originals can be retired without renewal. No
history, credential binding, epoch or unrelated token is changed or deleted.
Readback accepts only the exact all-active or all-revoked authority; partial,
extra or foreign registrations are rejected. Revocation is atomic, emits only
one identity-bound report, and invalidates both new authentication and retained
principals at their next locked authorization boundary. A lost reply leaves the
anchored intent observation-only. The protected adapter continues to use the
retained management PostgreSQL backend and operator scope after revocation;
it does not require a running gateway or its token. This stage still grants no
legacy-restoration authority: gateway Kubernetes-role retirement and safe
workload/role restoration remain separate prerequisites.

Gateway-role retirement then removes only `create` and `delete` from the exact
installed gateway namespace Roles. Their UIDs, read rules, bindings and the
namespace-reader ClusterRole are retained. Its child journal is anchored to the
completed machine revocation and original authority receipt; each update tests
the UID, resourceVersion, complete metadata and original rules. Intent is durable
before dispatch. Unknown replies are observation-only, and a definite rejection
alone permits another prepared attempt. Both retained-resource and writer-inventory
readers accept the same anchored original/reduced projections during partial
retirement, without accepting arbitrary drift or unanchored permission changes.
The connected transport requires fresh process/journal drain, closed admission
and revoked machine authority before every update. Completion also requires
effective gateway permission reviews across registered namespaces and all foreign
RoleBinding namespaces naming its identity or groups; an extra named grant or
incomplete review blocks completion even if every fixed Role is read-only.
No binding is deleted, no unrelated resource is changed, and legacy-restoration
permission remains withheld until the separate workload/role restoration path.

The next anchored recovery phase restores only retained pre-migration workload
specs, with every Deployment kept at zero replicas and every CronJob suspended.
It preserves current top-level metadata and all retirement/startup fences; the
gateway, material, configuration and permission resources remain untouched.
Unchanged dormant and retired templates need no write. The journal binds the
completed gateway retirement, shutdown and exact before/after workload catalog.
Every spec update requires fresh gateway effective-readonly, revoked-machine and
process/journal-drain checks, then an intent-bound UID/version/metadata/spec CAS.
Unknown outcomes only observe the same retained object; definite rejection alone
permits another prepared attempt. Recovery entrypoints and writer inventories
consume the same anchored partial-restoration projection. Process drain still
requires complete child/Pod inventories and acknowledgment of the current
Deployment generation after a spec update, not merely zero requested replicas.
This stage grants no write role, starts no process and releases no intake guard;
legacy permission restoration, restart and reopening remain separate prerequisites.

Closed Role restoration follows completed stopped-template restoration. It
restores only the exact retained participant Role rules and original annotation
shape, removing the operation's role-fencing marker. UIDs, other metadata,
bindings, gateway authority and all workload specs remain unchanged. Each update
requires fresh revoked-machine, gateway effective-readonly and process/journal
drain checks, with durable intent and UID/version/full-metadata/rules CAS. An
unknown reply permits observation only; a definite rejection may prepare again.
The child journal binds completed template recovery and exact before/after Role
snapshots. Without that evidence, retained readers still require restricted Roles
and their original read-only qualification. With it, readers accept only the
recorded per-Role recovery projections and check effective permissions separately
for every retained account and destination, including foreign namespaces with
bindings to the account or its actual groups. The fixed participant binding map,
not a union of all restored rights, defines each account's required grants.
Harmless reader/discovery/self-inspection extras remain allowed; missing required
grants, extra writes, credential/exec access and incomplete reviews fail closed.
Exact Role and journal readbacks bracket these reviews. No workload starts and
no admission guard is released: `pool_legacy_roles_restored_closed` still sets
`legacy_restore_allowed` to false. Restart and reopening are separate phases.

Closed legacy restart restores only each retained predecessor's original replica
or suspend scalar. Its journal binds completed Role/template restoration, records
intent before UID/version/full-metadata/spec CAS, and admits only its exact
prepared/intent/started projections. Unknown replies only observe; a definite
rejection may prepare again. The first restart requires a fresh complete stopped
recovery barrier. Subsequent writes still require globally fenced mode, every
local recovery guard, revoked machine credentials, exact effective legacy rights,
fresh two-sided journal drain, and the stopped gateway's read-only authority and
process inventory. Running the exact old workload is no longer interpreted as a
surviving successor, without relaxing the original all-stopped retirement check.
Originally dormant workloads and the gateway remain unchanged. The
`pool_legacy_restart_staged_closed` result asserts neither runtime health nor
permission to reopen; restored runtime readiness and guard reopening are separate.

The fixed legacy settings challenge is separate from successor qualification. It
requires the original non-global controller/actuator settings, service runtime
profile and management mode, with no successor pool, submission source or profile
catalog. Actual typed runtime loaders answer a fresh challenge without printing
settings or credentials. Retained Pod identity/readiness is checked before and
after each probe, and the original spec cannot be substituted. These read-only
primitives neither release an admission guard nor authorize a rollback by themselves.
The closed restart runtime barrier consumes only a completed, anchored restart,
then probes the actual retained manager and every participant's controller,
service and active actuator. It checks their backend and legacy settings, plus
actuator telemetry availability, with recovery closure and exact workload/journal readbacks
before and after. Dormant roots and the stopped gateway are not started or probed
as active legacy consumers. Guard reopening uses a separate parent phase.

The internal legacy-reopening child anchors the completed restart and records an
ordered `prepared`/`intent`/`released` phase for every participant. Only a saved
intent can account for an observed open guard; unrelated open or foreign guards
fail qualification. Each release has durable intent before one fixed dispatch,
and unknown replies only observe. Reopening the first participant does not require
stopping its newly admitted legacy work to reopen the others: only still-fenced
local journals must be idle. The global pool remains terminally fenced, machine
credentials revoked and the successor gateway stopped with read-only authority.
The same actual legacy runtime probes run under this journal-derived partial-open
barrier; the original all-closed readiness check remains strict. The final result
reports legacy intake open only after every release settles and fresh runtime and
authority checks pass. It never opens global admission or rewrites the original
activation/restart receipts. This remains an internal protected-operation phase,
not a standalone rollback or release command.

The internal terminal handoff derives its outcome from these anchored journals,
never from a caller-selected mode. Global completion requires completed startup,
pool opening and every local release, with no recovery evidence. Legacy completion
requires the complete restoration/reopening chain. The receipt freezes the exact
request digest, phase bytes and UID-bearing stable workload snapshots; it adds no
live mutation. Repeated completion validates rather than rewrites the receipt or
its ancestors. An interrupted local receipt write can finish only the identical
anchor-bound bytes after fresh qualification. Current retained authority, workload
identities, pool mode and open guards are rechecked; legacy completion additionally
requires revoked machine credentials, a stopped read-only gateway and drained
global effects, without draining legitimate reopened legacy work. Historical
loading does not contact the cluster or replay any operation. This is terminal
phase/identity evidence, not fresh runtime or installed multi-owner acceptance;
the result explicitly withholds that claim. Protected entry and refresh consumers
must still qualify the baseline before using it for live operations.
The private predecessor reader binds that receipt to its original completed
management upgrade and exact cutover input hash. It derives the manager's pool
catalog operation from global completion (or retains the non-pool configuration
after legacy restoration), preserving the original workload UID and credentials.
The existing strict refresh renderer validates the derived configuration against
the recorded manager. No caller-supplied post-cutover manager is accepted, and
reading this baseline neither replays installation nor authorizes a live refresh.
Ordinary refresh completion contracts may retain this qualified pool baseline.
Their cumulative configuration and runtime checks are rooted in that baseline,
not a caller-provided `before` snapshot, and still preserve the original manager
UID. Repeated ordinary refreshes retain bounded root/pool/immediate evidence;
cross-kind pool/refresh loading rejects cycles and excessive ancestry. Historical
non-pool contracts remain byte-compatible. A pool-backed refresh requires the
exact bound pool authority verifier, even if a request omits the inherited
baseline. The protected refresh entry acquires that separately scoped reader for
the operation and closes its credentials and transports on success or failure;
the retained manager-only resource reader does not gain pool-namespace scope.
The separate read-only active-pool database proof requires exact global mode,
installation, epoch, participant and machine registrations, and current unrevoked
credentials. It permits waiting, reserved and active requests without changing
them. Its bound transport rechecks the management backend, credential identity
and operator authority around the read. This does not relax the closed-mode
startup proof or by itself qualify live refresh workloads and permissions.
The manager-only refresh projection reuses the anchored parent and switch
readers. It admits both sides only while a retirement or activation write is
uncertain, and requires the recorded activation prerequisites before accepting
the new manager. The original installation, immediate completed predecessor,
pool baseline and any superseded failed refresh are requalified; every other
pool workload retains its completed-cutover identity. This projection alone
does not supply the still-required live authority verification.
The connected read-only manager-backend and writer-inventory checks can consume
that bound projection. They match the actual upgraded manager, not a substituted
old observation, while preserving the common API-server revision for workload
and permission inventory and the original database credential identities.
The dedicated read-only pool refresh verifier composes those checks with exact
retained material, participant database roles, physical provider scope and
effective gateway permissions. An open global pool retains current active
authority and open participant guards without requiring idle work. A completed
rollback instead retains the fenced and drained global ledger, revoked machine
credentials and a stopped, process-drained read-only gateway; reopened legacy
owners may keep working. Fresh full workload and authority readbacks bracket
each qualification. The result is not a persisted or reusable write permit.
The refresh installer requalifies it in preflight, immediately before actual
resource creation and manager patches, around activation qualification and
before public completion. It does not require child journals during earlier
identity or dry-run calls. Unknown writes retain their existing observation-only
recovery contract. Bootstrap evidence stays bound to the original installation,
while current candidate/provider prerequisites are bound to the qualified refresh;
a later pool catalog does not rewrite or relax the bootstrap contract. This is
source-level upgrade support, not installed pool or multi-owner acceptance.
The fixed protected tooling bundle includes the pool readers and their recovery
dependencies. Refresh tooling qualification imports that dependency chain before
declaring the bundle usable, without reading private installation inputs or
opening cluster/provider connections; checkout imports cannot satisfy that proof.

The internal complete-operation adapter composes closure, startup, opening and
the terminal receipt under one per-operation dispatch lock. Replays select the
newest recorded phase (including an anchor without its state file) and let that
child validate its full predecessor chain; they never restart closure after
startup or closed recovery after legacy owners reopen. Rollback is an explicit
direction, not a response to an uncertain network result. Completed global
ancestry cannot be cancelled in place, and an install replay cannot reverse a
rollback already in progress. Preflight reads retained scope without dispatching
or claiming readiness. The private entry reloads its exact inputs before opening
transports. Protected `nebius-rollout` exposes only whole-operation preflight,
installation and explicit rollback, using a dedicated exact-bundle key and
operation metadata. It exposes no individual phase command. A terminal report
binds the operation UUID, outcome and completion digest, and explicitly reports
`acceptance_verified: false`; installed multi-owner acceptance remains separate.

The internal recovery-release database primitive is distinct from the original
activation release: it can remove only `pool-recovery:<operation>` for the exact
candidate. It takes the admission lock and the guard row lock, then freshly
checks the same six local activity/outbox counters as recovery-drain observation
before deleting the guard. Queued work without a handoff is retained. A missing
or foreign guard, active work, unresolved handoff or schema mismatch rolls back
the transaction. It emits one identity-bound report; its bound transport rechecks
the retained database and operator authority and never retries an unknown reply.
This primitive does not establish restored runtime health or authorize reopening
by itself. The anchored rollback parent must supply those barriers and durable
intent before using it; no standalone deployment command exposes it.

The startup stage does not open admission or claim a working execution pool,
and is not exposed as an independent deployment command. The complete protected
operation connects activation, uncertain-start rollback and charged-effect
cleanup; subsequent manager refresh qualifies the completed pool baseline.
Installed runtime/collector acceptance still requires live verification.

Once runtime replacement starts, recovery must not replay the original retirement
or fencing installer against the changed templates. The parent instead qualifies
the anchored child hashes, held guards, restricted roles, current effective rules
and each exact stopped old or journaled new workload. It rechecks every frozen
producer before further mutations, including partial replacement recovery.
Pure input-derived migration contracts and manifests use bounded, single-entry
reuse keyed by complete type-sensitive request snapshots. Nested input changes
are requalified and returned documents are detached. Each call captures a fresh
flat graph with local references, so shared subobjects are inspected once without
expanding or comparing repeated subtrees; alias changes also invalidate reuse.
This does not cache journal
reads, cluster identities, effective permissions, database observations or write
outcomes. Per-request transport scope checks compare the retained inputs and
still read the live cluster and namespace identities on every check. Image
admission continues to qualify against the current clock; collector settings
come only from retained inputs and explicit defaults, never operator environment
variables or local secret files.
Kubernetes previews qualify defaults before UID/resource-version/spec-fenced
updates; uncertain updates are observed, never retried. Immutable material and
gateway resource creation retain the existing single-create journals. The
final closed-stage readback freshly qualifies every recorded material,
configuration, authority and gateway identity after runtime replacement; drift
is rejected without repair or recreation. The
`pool_runtime_staged_closed` result still marks writer migration incomplete: all
replacement Deployments remain at zero and the single collector remains suspended.
Protected entry qualification, runtime activation, rollback and completed
successor-refresh authority remain required; this internal parent is not a
standalone operational command or evidence of an installed usable pool.

Actuator telemetry uses the qualified Node's private `InternalIP` and fixed
kubelet HTTPS `/stats/summary` endpoint. It requires the serving certificate to
validate against the configured cluster CA; clusters using a separate kubelet CA
may therefore have unavailable samples. The renewable runtime bearer authorizes GET `nodes/stats`;
GET `nodes` supplies endpoint identity. The reader preserves cumulative CPU,
sampled memory and filesystem counters without retaining foreign Pod data.
It rejects redirects, unqualified addresses/TLS/credentials and wrong-node
summaries, with no broad `nodes/proxy` fallback. The effective reader review
accepts only GET on `nodes/stats`, not node proxy or execution authority.
The staging attachment requires explicit `network.kubelet` private-node CIDRs
on TCP 10250 and grants that egress only to the actuator, not the gateway or
collector. Missing routes fail offline rendering; they are not replaced with
unrestricted egress. Existing attachment inputs must add the approved worker and
hosting-node routes before upgrading the reader.
The connected protected preflight checks kubelet reachability, certificate trust
and summary authorization **inside each retained ordinary and guest actuator Pod**
before controller retirement. It qualifies the Deployment/ReplicaSet/Pod lineage,
fixed ServiceAccount, loaded namespace/target settings and complete Node inventory,
then calls the production direct reader with each selected Node's exact UID.
Operator credentials never enter the Pod. Nonnegative CPU, memory and filesystem
counters establish sample availability; foreign Pod statistics and credentials
are not returned. A positively identified direct-kubelet TLS, HTTP, network or
authorization failure, or missing/invalid counters after summary identity is
verified, records unavailable telemetry rather than blocking installation.
API failures, ambiguous exception provenance, malformed probe output, wrong-node
summaries, unqualified addresses/credentials, incompatible readers and client-close
failures still block. Changed Pod or Node identities and incomplete inventories
reject the check even after a sampling warning. Existing images without the UID-aware reader require an ordinary
protected rollout followed by refreshed inputs; there is no node-proxy fallback.

The check covers all current physical-pool Nodes plus the actuator's hosting Node.
At scale zero, the hosting Node still tests runtime authority/TLS/network without
requesting a worker or creating an idle reservation. This does not prove reachability
of future workers. Collector/startup and real-task acceptance still qualify capacity
and execution when they appear; sampling failures retain the same optional semantics.
Recovery skips execution in a stopped Pod only when the same
anchored parent already qualifies its exact stopped or rewired template; it never
restarts a retired controller just to run a probe. Successor startup remains a
separate barrier before admission can reopen.

Protected pool results carry `telemetry` with `status` (`available`, `unavailable`
or `not_observed`), bounded `checks` and `unavailable` counts, and finite sanitized
`reasons`. Counts are runtime-node checks, not unique machines; re-probing an actuator
replaces its earlier observation only after Pod/Node/contract rechecks. No probes
means `not_observed`, never healthy or zero usage. This field survives both protected
gateway filters, including startup, activation and legacy recovery. Historical
reports may omit it; omission is not proof of availability.
Scheduling authority remains authenticated Node/Pod inventory, resource requests,
provider quota and reservation accounting, not sampled usage. Inference-usage
acceptance and complete-measurement requirements for resource calibration are
unchanged. Unavailable samples do not establish durable kubelet trust integration.

The standalone platform rollout checks for existing global participant settings
and retained pool-retirement markers before any mutation and again under its idle
guard. It refuses to overwrite these with legacy controller configuration or
direct-writer roles, including a cutover that completes between those checks.
This fail-closed boundary does not authorize a participant software refresh or
reset its pool bindings. Ordinary manager refresh uses the separate pool-aware
reader described above; participant changes need their own protected lifecycle.

Receipt storage and transition constraints alone are not Kubernetes cleanup proof
or installed global admission. The fixed gateway verifier supplies the qualified
absence/output-drain and settled-create evidence before recording cleanup.
The registry authenticates dedicated machine identities and must
validate all workload kinds and serialize physical-pool admission. The current
single-environment controllers do not switch writers merely because these tables
exist. The protected operation installs and qualifies the production pool
collector, connected admission and durable local handoff before opening intake;
an installed acceptance claim still requires live evidence.

## Native task-image capacity fairness

An explicitly registered guest capacity alias shares its ordinary target's
physical admission family. The ordinary collector captures both target IDs from
the control plane's catalog scope; it also retains the owner's build namespace.
This same-environment family is separate from the protected managed-environment
`capture_pool` registry above. See the
[target binding contract](nebius-service-execution.md#provider-neutral-contracts)
for independent health/intent and observation membership fencing.

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
