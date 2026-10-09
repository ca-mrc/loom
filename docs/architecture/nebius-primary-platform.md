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

Standalone platform inputs distinguish `development` and `staging`; they do not
infer deployment class from the publication branch (`dev`), namespace name, or
historical "integration" terminology. Environment is part of the immutable
execution-target identity, not just a frontend label. Ordinary rollout cannot
reclassify an installed platform. The existing Nebius staging installation's
historical development label requires a separate controlled correction; see
[platform operations](../runbooks/nebius-platform.md#correcting-a-historical-environment-classification).
It is not a shared-development foundation for personal applications.
Staging also selects seven-day ephemeral lifecycle authority and mandatory fresh
storage-capacity admission. The standalone Nebius renderer does not qualify that
evidence or retention/cleanup path; rendering and deployment smoke are not proof
of staging task readiness.

The primary region is `eu-north1`. Secondary-region routing remains disabled
pending separate qualification; checked-in regional support is not evidence
that a region is operationally accepted. Current deployment and recovery
procedures are indexed in [runbooks](../runbooks/README.md).

Personal frontend/API instances and their separately managed shared development
foundation follow the [personal application contract](nebius-personal-applications.md).
Shared execution/build capacity, its global reservation journal and native build
fairness follow the [pool contract](nebius-shared-pools.md).

## Detailed contract navigation

Application and pool details have dedicated pages. These links preserve the
section locations used by earlier references to this platform contract.

| Contract section | Owner |
| --- | --- |
| <a id="personal-application-and-shared-development-data-boundary"></a>[Personal application and shared development data boundary](nebius-personal-applications.md#personal-application-and-shared-development-data-boundary) | Applications |
| <a id="application-scoped-browser-authentication"></a>[Application-scoped browser authentication](nebius-personal-applications.md#application-scoped-browser-authentication) | Applications |
| <a id="application-only-manifest-contract"></a>[Application-only manifest contract](nebius-personal-applications.md#application-only-manifest-contract) | Applications |
| <a id="application-namespace-authority"></a>[Application namespace authority](nebius-personal-applications.md#application-namespace-authority) | Applications |
| <a id="shared-side-application-network-admission"></a>[Shared-side application network admission](nebius-personal-applications.md#shared-side-application-network-admission) | Applications |
| <a id="application-registration-and-shared-name-claims"></a>[Application registration and shared name claims](nebius-personal-applications.md#application-registration-and-shared-name-claims) | Applications |
| <a id="application-intent-and-lease-journal"></a>[Application intent and lease journal](nebius-personal-applications.md#application-intent-and-lease-journal) | Applications |
| <a id="application-external-effect-evidence"></a>[Application external-effect evidence](nebius-personal-applications.md#application-external-effect-evidence) | Applications |
| <a id="shared-application-database-access"></a>[Shared application database access](nebius-personal-applications.md#shared-application-database-access) | Applications |
| <a id="recoverable-application-credential-material"></a>[Recoverable application credential material](nebius-personal-applications.md#recoverable-application-credential-material) | Applications |
| <a id="application-object-store-access"></a>[Application object-store access](nebius-personal-applications.md#application-object-store-access) | Applications |
| <a id="application-credential-integration"></a>[Application credential integration](nebius-personal-applications.md#application-credential-integration) | Applications |
| <a id="closed-admission-application-preparation"></a>[Closed-admission application preparation](nebius-personal-applications.md#closed-admission-application-preparation) | Applications |
| <a id="active-application-startup-and-completion"></a>[Active application startup and completion](nebius-personal-applications.md#active-application-startup-and-completion) | Applications |
| <a id="personal-application-control"></a>[Personal application control](nebius-personal-applications.md#personal-application-control) | Applications |
| <a id="local-application-source-capture"></a>[Local application source capture](nebius-personal-applications.md#local-application-source-capture) | Applications |
| <a id="stopped-application-completion"></a>[Stopped application completion](nebius-personal-applications.md#stopped-application-completion) | Applications |
| <a id="physical-pool-observation-for-managed-environments"></a>[Physical pool observation for managed environments](nebius-shared-pools.md#physical-pool-observation-for-managed-environments) | Pools |
| <a id="global-pool-reservation-journal"></a>[Global pool reservation journal](nebius-shared-pools.md#global-pool-reservation-journal) | Pools |
| <a id="native-task-image-capacity-fairness"></a>[Native task-image capacity fairness](nebius-shared-pools.md#native-task-image-capacity-fairness) | Pools |

## Managed environment identity and rendering

This section describes the retained full-environment v1 format. New personal
applications use the [shared-data, application-only contract](nebius-personal-applications.md) and do not own
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

An independent manager may set `public_tls_secret_name` to an explicitly delivered
namespace-local TLS Secret. Omission preserves the existing default-certificate
route and serialized input identity. The renderer neither creates the Secret nor
changes the shared ingress controller. The private certificate issuer has a
separate exact-host management schema and entry point: it cannot request the
shared wildcard, and the legacy two-subject entry point rejects that schema.
Certificate issuance alone does not establish delivery, renewal activation or
public readiness; those require the protected installer and live HTTPS proof.
The fresh independent dev-manager composer requires a matching exact-host
certificate and stages its immutable generation Secret before the public Ingress.
The certificate, key and generation-derived Secret reference join the retained
installation input identity. Lost create replies use exact readback, not another
write; missing/replaced Secrets cannot be adopted on replay. This initial-delivery
stage does not rotate certificates or patch the shared controller. The connected
protected entry pins the issuer installation and immutable generation, not a
floating certificate selection. A separate protected manager-only renewal entry
qualifies the original retained installation, stages a new immutable generation,
then uses a journalled UID/resourceVersion JSON Patch to change only the retained
Ingress's certificate reference. It does not replay the initial renderer, require
an unexpired predecessor, or mutate the shared controller/default certificate.
One original-install lock serializes successors. Uncertain writes are resolved by
exact readback, never repeated; newer generations cannot overtake unfinished work.
Final route/Secret reads and normal-trust HTTPS must identify the new leaf. This
source-level renewal path still requires installed proof before operational
readiness; scheduled issuance/delivery is not provided.

Fresh development management has a separate fixed operation and gateway, bound
to `loom-nebius-management-dev`, its own private installation/anchor paths and a
dedicated exact-bundle SSH grant. Protected manual preflight/installation actions
in `nebius-rollout` select only this authority; neither can invoke the legacy
manager or foundation commands. Source publication requires the exact clean
integrated revision, hash-locked dependencies and deterministic first-party
wheels. The remote release qualifies actual imports and verifies retained bundle
bytes on replay. Private configuration, source and material files join the
existing installer identity; they are not copied into publication artifacts.

This application-only installer qualifies the completed private dev foundation
from retained source/input evidence and current resource identities rather than
rerendering its old source. It composes the existing database, application access,
shared SQL setup, backup and public authentication stages without starting a
legacy full-stack provisioner. Cloud identity, storage and system capacity checks
precede writes; actual manager PVC/PV/controller/node/provider disk identity is
verified before migrations. Personal capacity reserves only API/web resources,
not per-owner data volumes. A manager completion receipt still does not establish
source-build/task admission or multi-owner acceptance. See the
[delivery runbook](../runbooks/nebius-deployment.md#independent-development-manager-delivery).

The fresh installer can opt into the existing owner-scoped source-upload runtime
without enabling builds or tasks. Its `source_files` pair must match the
live-qualified retained dev foundation's source-storage identity. Only those two
keys are delivered in the revision-named immutable source Secret, before manager
startup. The configured concurrency's bounded ephemeral spool is included in
platform capacity accounting. No per-owner bucket, policy or PVC is introduced.
The option and credential bytes are frozen in installation identity; omitted
inputs retain historical hashes, and TLS renewal reconstructs source-enabled
history from retained evidence rather than reopening old credential files.

For a fresh independent manager, protected `shared_public_route: true` adds a final
shared-dev public phase. It creates only a fixed NetworkPolicy and Ingress in
`loom-dev`, using the existing shared ingress default wildcard certificate. The
foundation's configured hostname must be one label under the child zone and is
reserved against personal applications. Both namespace and controller Pod labels
select the allowed ingress peer; only the shared web/API ports are exposed.
The foundation's retained workload snapshots, database and disabled schedulers
remain unchanged. DNS/TLS, route conflicts, browser HTML shell, actual foundation
source and public authentication rejection are checked; ordinary-user login and execution are
separate acceptance gates. The default private-only path and its input identity
remain unchanged. The option is frozen before installation, not a replay-time
upgrade switch; manager-only TLS renewal accepts either original phase history.

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
