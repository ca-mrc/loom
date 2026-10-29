# Task-image reuse decision and comparison plan

Date: 2026-10-29. Scope: #2704 and #2705 under #2625. Inspected source baseline:
`b3945dcdc78c20f66d37863adb6926e016afb69d`. #2701 is independent work owned by
Hrishi; this document does not change its controller dispatch or admission contract.

## Decision

Retain exact ready-image reuse and the existing remote BuildKit layer/blob cache.
Defer cross-task complete-recipe coalescing and cross-Job persistent BuildKit
workers. Complete the admission and cache-GC improvements first, then compare
additional reuse against that baseline. No warm-capacity, snapshotter, cache-mode,
CPU, image identity or isolation default changes follow from this decision.

The observed input cohorts contain **zero byte-identical complete static input
candidates among 120 tasks**, although shared first-base references are common.
This supports spending the immediate implementation effort on admission and cache
metadata work. It does not prove that all possible semantic reuse is absent, that
layer-cache hits are absent, or that repeated Trials require new builds.

For worker reuse, the existing #2056 measurements identify a possible warm-hit
unpack saving but do not justify a new persistent-worker lifecycle. A bounded
Job-local component-cache comparison is the first next experiment if residual
materialization cost remains significant. No worker or cross-task reuse
implementation is selected by this research result.

## Reproducible local census

Run the read-only tool against an authorized local task collection:

```sh
python scripts/task_image_reuse_census.py --tasks-root "$RST50_TASKS_ROOT"
python scripts/task_image_reuse_census.py --tasks-root "$HARBOR70_TASKS_ROOT"
# Multiple roots give a deduplicated-directory combined census:
python scripts/task_image_reuse_census.py \
  --tasks-root "$RST50_TASKS_ROOT" --tasks-root "$HARBOR70_TASKS_ROOT"
```

`RST50_TASKS_ROOT` is the existing local 50-task source snapshot used by the
October 22 RST50 investigation and #2642. `HARBOR70_TASKS_ROOT` is the existing
September 22 70-task Nebius-adapted Harbor snapshot. The latter is a distinct
adapted cohort, not a claim of canonical TB2.1 admission or current live runtime
qualification. Neither input snapshot was modified, republished or executed.
Source packages and private local locations are intentionally not copied here.

The [aggregate result](task-image-reuse-census-20261029.json) records this run.
Another authorized reader can rerun the tool on the same snapshots; differing
input snapshots or file modes can change the counts. This is a local input
census, not a frozen production database/cache inventory.

| Local cohort | Tasks/components scanned | Unique Dockerfile bytes | Unique context trees | Unique static input candidates | Unique first-base references | Largest first-base group |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing RST50 local source | 50 | 50 | 50 | 50 | 8 | 37 |
| Existing Nebius-adapted Harbor70 | 70 | 70 | 70 | 70 | 7 | 33 |
| Combined | 120 | 120 | 120 | 120 | 13 | 37 |

All 120 were single-primary-component inputs supported by this census; none were
skipped. The contexts contained 437 regular files and 25,200,211 bytes in total.
No cohort's external base references were all digest-pinned. All Dockerfiles
contained a remote-input command marker; that marker is a conservative text
indicator, not execution tracing. No static secret-mount marker was found;
absence of a marker does not prove a build is independent of secrets.

The tool compares full context trees, file names/modes/content, selected
Dockerfile bytes/path, context path and all environment configuration. It ignores
no `.dockerignore` entries, so unused files/settings can split candidates. It
bounds per-task reads, skips unsupported multi-component configurations and
symlinks, and never follows paths outside a task. It emits aggregate counts only:
no task names, paths, Dockerfile text, image names, build-argument values or content
fingerprints. These fingerprints support grouping only; they are not another
integrity attestation or permission to reuse an output.

| Question | Evidence and limit |
| --- | --- |
| Exact ready-image reuse | Already implemented by materialization identity and unique-key ensure. This local census does not observe actual ready hits or Trials. |
| Complete static-input duplicates | Zero in these cohorts under the conservative comparison above. A strict byte-equivalence policy would avoid zero additional builds within these one-pass cohorts. |
| Equivalent in-flight requests | Unknown: no runtime queue/attempt snapshot was read. |
| Partial layer sharing | Shared base references are candidates; actual BuildKit hit count and saved transfer/solve time remain unknown. |
| Truly unique build work | Unknown: different bytes can still share layers or equivalent semantics; arbitrary RUN/network behavior prevents a semantic proof from file bytes. |
| Additional time/bytes avoided | Unknown. Context bytes are local source size, not cache-transfer or image bytes. No speed/cost saving is inferred from them. |

This census does not cover all tasksets or recurring submissions. Repeated runs
can have much larger exact-ready reuse than this one-pass sample, using the
existing identity mechanism without any new coalescing feature.

## Authorized reuse proposal, if later justified

Keep every Task, materialization identity, frozen snapshot and Trial/result
history. A reuse entry would identify one physical build/output and attach
separate authorized consumers; it must not replace `task_id` in persisted
materialization keys or become a second resource-grant authority.

The internal candidate identity needs the following supported effective inputs:

- The authorized trust domain and environment/publication policy. Matching bytes
  never grant another team access to private context, cache or output.
- Complete selected context and Dockerfile inputs, `.dockerignore` semantics,
  target, architecture/platform, effective build arguments, frontend/builder
  semantics and all platform-injected preparation inputs/revision.
- Resolved base-image digests, remote-context revisions and any other supported
  immutable dependency inputs. Reuse existing input manifests and native OCI
  digests; do not add a second signature/hash inventory around the same boundary.
- An explicit freshness/refresh generation. A mutable tag or `apt`, `git`, `pip`
  or arbitrary network RUN does not become immutable because package bytes match.
  Reuse is valid only within the accepted freshness policy; an explicit refresh
  creates a separate generation. Disable complete-output reuse for unsupported
  dynamic inputs rather than silently promise fresh remote results.
- Secret/SSH-dependent builds opt out by default. Any later opt-in needs a
  non-secret authorization/version scope and a demonstrated output contract;
  secret values must never enter keys, logs or exported reports.

Existing exact-ready references remain the fastest path. Recheck authorization,
freshness and required runtime metadata at the reuse boundary, then retain the
published digest and provenance rather than repeatedly rehashing it downstream.

### Ownership, state and cancellation

A future design should reuse the current transactional operation/outbox and
lease fencing, not add another scheduler or capacity ledger. For each qualified
trust-domain/input/refresh-generation tuple, one physical operation progresses
through queued → granted → running → publishing → ready, or failed/cancelling →
confirmed cleanup. Separate waiter records retain each consumer's source,
identity, requested generation and status.

| Event | Required behavior |
| --- | --- |
| Authorized ready output | Link the existing immutable digest and metadata to the requesting materialization; record reuse provenance without fabricating a new solve. |
| Equivalent build in flight | Atomically attach a waiter to the one owner operation. Only that operation can reserve/create the workload. |
| One waiter cancelled | Cancel only its wait and preserve other consumers and durable task/catalog references. |
| Last demand cancelled | Request physical cancellation if no retained reference still needs the build. Keep the grant charged until authenticated cleanup acknowledgement. |
| Retry or worker restart | Preserve original consumer age and ownership. A failed attempt may be retried only under the current generation/lease; retry budget is not refunded by attaching another waiter. |
| Failed build | Retain diagnosable per-consumer failure and shared attempt provenance. Do not cache a failure as a ready image or silently substitute another recipe. |
| Late/stale publication | It may add cleanup debt; it cannot make a newer or cancelled request ready. Current-generation authority is required to attach output. |
| Ready entry GC | Retain all task/catalog/Trial/output references and retention obligations. Last waiter cancellation alone is not permission to delete a referenced image. |

Before implementation, exercise races between two first waiters, attach versus
cancellation, ready publication versus refresh, stale completion, last-reference
GC and worker loss. Preserve #1915's trusted priority/age for eligible requests;
adding a waiter cannot elevate its origin or preempt already granted work.

## Worker reuse comparison

The current renderer starts `buildctl-daemonless.sh` for each component, exports
independent output/cache, then removes scratch state. Job storage is `emptyDir`.
Job-local verified-blob download reuse already exists and is not a new proposal.
Remote cache hits may still require transfer, unpack and OCI output generation.

[The October 13 #2056 baseline](https://github.com/ca-mrc/loom/issues/2056#issuecomment-6153689051)
reported 20-task preparation medians of 567s from zero and 126s on already Ready
nodes. Warm exact-hit downloads took 0.9–17s and hit solve median was about 69s,
with instructions cached while layers were extracted onto empty disk. Solve
already included OCI writing. These are dated workload-specific observations,
not measurements of the present 120-task input census or the new GC code.

[The associated decision](https://github.com/ca-mrc/loom/issues/2056#issuecomment-6154760286)
rejected paid idle-node retention and cross-Job persistent daemons for that
residual saving. It retained archive export, 1 CPU and blob transfer. The new
large-burst proposal does not itself reverse that cost decision. Reconsider only
if legitimate sustained demand keeps a node busy long enough for useful reuse.

| Candidate | Mechanism that could save work | Cost/isolation/scale-to-zero boundary | Disposition |
| --- | --- | --- | --- |
| Current disposable per-component build | Remote layer hits avoid executing reusable steps; ready images avoid the whole rebuild | Empty local disk still pays transfer/unpack/export. Node and Job state vanish normally. | Retain as comparison baseline. |
| Earlier component's exported cache supplied to later components in one Job | BuildKit may reuse validated common dependency layers and avoid executing steps | Daemonless startup/state cleanup still means unpack can remain. Retained exports increase peak disk; each output remains independent. | First bounded experiment if residual layer work warrants it. |
| One daemon within a Job | The same local snapshots can avoid repeated expansion while that Job lives | Must prove process/workspace cleanup, timeout/cancellation and task/verifier component privacy. No-process-sandbox cannot be treated as a hostile-job boundary. | Defer until component-cache comparison identifies a residual mechanism. |
| Cross-Job local workers during active demand | Route compatible builds to retained local snapshots, avoiding repeated downloads/unpack on the same node | Requires trust partitions, per-build process/workspace cleanup, credential separation, disk GC/routing and failure recovery. Normal scale-down discards state. | Defer; no measured benefit versus the post-P1 baseline yet. |
| Worker state on persistent storage | Preserve cache across Pod/node replacement without retaining idle compute | Storage/I/O/attach cost remains; a volume is not an isolation mechanism and does not remove output export/publish. | Conditional alternative for the same later matched comparison. |

Builder traffic stays inside #1915's existing aggregate accounting and physical
scaler. Never place trusted registry/object-store credentials into an untrusted
build daemon. A persistent process must not inherit another build's secret files,
workspace, child processes or mutable private cache mounts. Cancellation must
clean exact owned work without erasing other live consumers; ambiguous cleanup
keeps its resource reservation. Default idle capacity remains zero.

## Mature mechanisms and applicability

Use existing mechanisms at their owning boundary rather than assemble a new
stack. [Buildx's Kubernetes driver](https://docs.docker.com/build/builders/drivers/kubernetes/)
already supports replicas, resource limits and per-replica PVC-backed cache.
Sticky routing selects by context-path hash; it is not complete-recipe identity,
in-flight coalescing or a tenant-isolation guarantee. These options can inform a
later worker experiment without authorizing a separate physical scaler.

[BuildKit GC](https://docs.docker.com/build/cache/garbage-collection/) runs
periodically with ordered policies for age and storage/free-space pressure.
Retained workers should use that native local-cache GC rather than a new local
content store. It does not collect Loom's remote blob manifests, so #2702 must
still preserve the remote publication/reference contract separately.

[Distribution GC](https://distribution.github.io/distribution/about/garbage-collection/)
uses mark/sweep over manifest references and warns that concurrent writes can
corrupt data without exclusion. Apply the shared-reference and safe-publication
principles to remote-cache GC; do not copy an offline algorithm into an online
publisher or remove synchronization because blobs are content-addressed. This
reference does not select a second registry/CAS service.

## Matched experiment and acceptance boundary

Post-#2701 admission throughput and the installed post-#2702 cache-GC baseline have
**not been measured here**. The census and existing #2056 evidence justify
retaining/defer decisions now; they do not constitute a performance acceptance
of either P1 implementation or a reusable worker.

When the improved baseline exists, compare each candidate individually before
combinations. Use the same frozen tasks, builder/frontend/preparation, 1-CPU
baseline or explicitly matched resource envelope, storage/snapshotter, admission
and concurrency policy. Include a shared-base multi-component fixture,
source-only change, dependency change, no-sharing fixture and a representative
unchanged real cohort. Separate from-zero/empty disk, Ready node/cold local cache,
remote hit and warm local state. Include mixed build/Trial demand where authorized.

Record one causal timeline: submission/eligible/reservation; scheduling/node
provisioning; prepare source/cache transfer and local materialization; daemon
startup; solve with OCI export; publish; cleanup. Report exclusive stages and
explicit overlaps rather than adding OCI export again to solve. Measure useful
image throughput, batch makespan, actual solve concurrency, executed/reused
steps, object requests/bytes, exact-ready/coalesced counts, peak disk/pressure,
node-hours and storage cost including any idle period. No unsupported speedup,
peak disk or cost figure is filled from the static census.

Qualify digest pull/use by a Trial, cancellation/cleanup, stale generation,
worker loss/restart, partial cache import, concurrent publication/GC and pressure.
Test source/secret isolation before any cross-Job experiment. Keep unknown or
stale pressure observations unknown and preserve cleanup-before-release.
Disposable no-model evidence comes first; shared-runtime or paid work needs its
actual authorized scope and must not disturb running evaluations.

Reopen an implementation decision only when this comparison shows material net
benefit, a supported isolation/cleanup model and acceptable total cost. Otherwise
retain the simpler path. Any later implementation or live qualification remains
an explicit follow-up; a source-only decision must not close outstanding hosted
performance acceptance.
