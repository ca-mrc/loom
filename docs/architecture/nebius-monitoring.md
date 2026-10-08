# Nebius progress and placement

The Monitor, Batch detail and Trial detail share a read-only progress projection.
Persisted Trial, image materialization and execution lease states remain the
scheduling authorities. The state=stage:image_preparation filter (and the other
public stages) uses the same SQL expression as summary counts.
Raw Trial state filters remain supported.

Public stages are image preparation, execution wait, environment startup,
execution, output archival and terminal outcome. Task verifier activity must not
be inferred from output upload or archival. Shared image counts are distinct
from affected Trial counts. Terminal Trial outcomes take precedence over later
shared cache changes.

The build container includes scratch cleanup after BuildKit exits. Its running
state is displayed as building/cleanup, not a measured BuildKit-only duration.
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
