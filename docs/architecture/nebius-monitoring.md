# Nebius progress and placement

The Monitor, Batch detail and Trial detail share a read-only progress projection.
Persisted Trial, image materialization and execution lease states remain the
scheduling authorities. The state=stage:image_preparation filter (and the other
public stages) uses the same SQL expression as summary counts.
Raw Trial state filters remain supported.

Public stages are waiting for image, execution wait, environment startup,
execution, independent verification, output archival, terminal outcome and
unknown. The current attempt is bound to `Trial.attempt_count`; an independent
verifier is bound to that parent and the recorded verifier lease/retry identity.
Verifier activity is never inferred from output upload or archival. Shared image
counts are distinct from affected Trial counts. Terminal Trial outcomes take
precedence over later shared cache changes.

The build container includes scratch cleanup after BuildKit exits. Its running
state is displayed as **Build container active**, with the internal substep
explicitly unobserved, rather than a measured BuildKit-only duration. The separate
image cleanup stage means release of owned build resources. Only existing
structured observations determine these stages; no log polling is added.
The per-command build budget and whole-Job lifecycle deadline introduced by
PR #2020 remain distinct: timeout wording uses structured exit/Job conditions,
falls back to a generic deadline if those are absent, and never parses logs to
invent live phases. Bounded actuator logs remain outside the public projection.

Migration 0153 adds a nullable Trial scheduling observation and a nullable
capacity-wait reason. A scheduler rejection records a bounded reason and time;
successful reservation retains the image-readiness boundary and lease identity
for the timeline. Historical records without that boundary show combined
preparation/admission time instead of inventing a build duration. The new
observation is diagnostic only and never grants capacity or execution authority.
Retry preparation/admission timing is unknown when its queue-entry boundary was
not retained; original submission time must not count earlier runtime as wait.

GET /api/v1/monitor/placement?target_id=... reads existing capacity observations
on demand. Resource and Pod totals cover the shared target; workload links are
restricted to the reader's authorized Trial scope and Monitor filters. Ordinary
members see logical node labels, not physical node/provider identifiers or
other teams' workloads. Both execution and build Pods consume shared resources.
Requests are reservations, not measured resource utilization. Latest-attempt
Trial counts do not remove older attempts from cleanup/capacity accounting.

The UI retains adaptive polling and fetches node detail only while expanded.
Observation freshness is explicit. A build concurrency limit observed from an
active native build is shown only while fresh and unambiguous; missing evidence
is unavailable, never a hardcoded node count or inferred zero capacity.

## Trial and image pipeline drilldown

Monitor and Batch detail display stage cards by default. Trial lists show one
main stage, safe waiting reason, region and elapsed stage time. An absent stage
entry timestamp remains unknown. In particular, the mutable phase age of a
shared image is never used as an individual Trial's waiting time, and submission
age is not a phase duration. The image list labels its own phase duration at the
last refresh. Missing and stale observations are explicit.

Output submission and execution resource cleanup are independent of the main
stage. A succeeded Trial may still show output submitted and resources being
released. Cleanup includes unreleased older attempts; its freshness is evaluated
separately from the durable result. `state=output:uploading`,
`state=output:unavailable` and the `state=cleanup:pending`,
`state=cleanup:in_progress` and `state=cleanup:blocked` filters drill
into the same projection used for the corresponding summary counts. These
read-only filters do not change persistent business state or delay a result
until cleanup completes.

Image cards count unique materializations and distinguish queued, waiting to
retry, capacity wait, preparing context, build container active, publishing,
resource cleanup, ready, failed and unknown. Unreleased older build attempts stay
visible as cleanup even if an administrative retry advances the current image
epoch; each materialization is still counted once. Retry waits retain a safe failure
explanation. Selecting a card loads `GET /api/v1/monitor/images` on demand,
with a maximum of 100 entries per page (default 50). Each row reports affected
and image-waiting Trials within the authorized filter scope. `image_id` drills
into those Trials, and `batch_q` preserves batch-name scope when moving from a
batch search to Trial details. Neither the list nor the aggregate reveals other
teams' Trial counts or private native manifests.

The existing Monitor 4-second and Batch 5-second adaptive queries carry the
small summary. There is no added full summary poll. Image details use a shared
query key, explicit refresh and cursor pagination; obsolete scope/stage requests
are aborted. Routine refresh keeps the current selection, row identities and
scroll position. Lease reads project lifecycle columns and image reads project
only phase/status fields; heavyweight diagnosis and build contracts stay out of
the regular response. No new telemetry collector or history store is involved.

Focused verification uses API/database fixtures, a build/failure/cache-reuse
path and browser interactions. These checks do not replace the ordinary-member
deployed browser journey in issue #1981, nor require another paid model batch.

## Capacity and delivery presentation

Admission's conservative zero is not a capacity measurement. The public
projection returns unknown slot estimates when calibration/binding or fresh
observations are unavailable, while retaining confirmed zero forecasts and
unchanged admission decisions. The monitor API retains `execution_class_id` and
explicit `capacity_owner_target_id` for each target. Capacity cards group only by
that owner identity, never by pool name or region: independent owners in one
region remain separate, and older responses without ownership retain separate
cards. Node inventory, quota, policy and on-demand placement appear once per
physical capacity owner. Missing owners and non-active/unhealthy owners remain
explicit rather than borrowing a sibling's healthy state.

Runtime entries use Container sandbox, VM sandbox, and VM sandbox · Emulated
authentication. Each entry retains its own health, lifecycle, command backlog,
blockers and calibrated slot estimates. These estimates are alternative uses of
the same resources and must not be summed. Backend execution-class IDs and the
`container` / `guest` API values remain unchanged; the batch isolation control
and execution readback use the same user-facing sandbox names.

Disabled and retired targets appear under Inactive execution environments,
not as active service faults; their historical observations remain in the API.
Draining targets remain visible with node occupancy until their resources are released.

Trial artifact listings merge trajectory-index references and canonical artifact
records by the download API's file key. Canonical size and sharing metadata take
precedence. Unknown size stays null; zero means an actual empty file.

Run Library and Batch detail use the same LLM-call accounting projection for
monetary estimates and cost status. Legacy result cost defaults do not establish
that a tokens-only or unpriced run was free.

Owner-team members and platform administrators can access the existing batch
family delivery flow from Run Library as well as Batch detail. The server selects
final results across the batch and linked reruns; this is not arbitrary checkbox
selection. Active runs retain individual completed-Trial downloads and explain
why batch export is deferred. Cross-team shared artifact browsing does not grant
access to private full-batch delivery. Metadata export remains a separate action.


### Home readiness

Home uses the existing Nebius execution-capacity projection. An active target
with an enabled capacity policy satisfies execution setup, even when autoscaling
has reduced its node count to zero. Home readiness means ready to submit; it does
not promise immediate per-task admission. Missing or stale observations remain
unknown, and current capacity blockers link to Monitor without inventing a
worker-start prerequisite. Provider, catalog and team checks still apply. The
legacy `worker_health` response remains for API compatibility but no longer
controls Home readiness or its execution card.
