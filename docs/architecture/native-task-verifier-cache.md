# Same-Job task cache for verifier builds

`reuse_task_cache_for_verifier` is a strict boolean, default `false`. When enabled
for a native Task-image Job with caching, the `verifier` component imports its
own prepared `cache-in/<index>` and the successfully built `task` component's
`cache-out/<task-index>` as two separate local BuildKit sources. Role names select
the components; indices need not be0/1. A verifier occurring before the task,
sidecars, and application-image Jobs do not gain this import. With caching
disabled there is no cache export/import. Authored `--no-cache` remains intact.

The verifier's existing records are not overwritten or merged with the task
index. Each component still exports into its own output directory, renders its
own OCI artifact, and cleans its private native snapshots/daemon scratch after
the solve. Shared cache/OCI output stays outside that cleanup. The public task
prefix can be reused when its BuildKit cache key matches; the authored private
verifier layers remain separate and rebuild when their inputs differ. This is
neither an image inheritance rule nor a grant for another Task's source/cache.
Prepare/publish credentials, mounts, frozen claims, exports and publication
grants keep their existing boundaries.

`tests/integration/test_native_task_verifier_cache_docker.py` exercises the exact
generated shell with pinned BuildKit v0.33.0 rootless and the native snapshotter
in local disposable Docker volumes, without registry/network access. A small
public BusyBox/loader fixture from that same pinned image supplies `FROM scratch`
RUN commands. Separate independent cache branches prove that both verifier-own
and task-sibling records remain usable, while a changed private marker rebuilds.
A hardlink to an earlier layer is checked inside the solve; no overlay fallback
is added. The test also verifies independent cache indexes and unchanged own
input, and that a real `RUN exit42` executes once and fails the Job.

Pinned upstream behavior for corrupted local caches is deliberately explicit:

| Damage | Observed behavior |
|---|---|
| Missing index | Renderer omits the source; one cold solve succeeds. |
| Malformed index | BuildKit warns and skips the source; one cold solve succeeds. |
| Missing manifest or cache configuration blob | BuildKit cannot import the cache; one cold solve succeeds. |
| Missing/corrupt required rootfs COPY layer | The solve fails; it is not replayed cold. |
| Authored RUN exits42 | The solve fails once; cache error handling cannot retry it. |

The layer-damage fixture identifies its required rootfs COPY content, not the
last descriptor in an unordered cache manifest. A tiny/unused or recomputable
cache layer can instead be missed and recomputed inside the same upstream solve;
missing arbitrary layers are not universally fatal.

The runtime's normal prepared-cache validation remains the earlier boundary.
These tests pin what happens if damaged local files reach BuildKit, not a claim
that every future upstream error is a safe cache miss. There is no generic
failure-to-cold-build fallback or a second authored RUN execution.

An imported artifact or a `CACHED` LLB operation does not eliminate native
snapshot extraction/OCI materialization. Performance needs phase-specific
measurement; overlapping DAG/extraction/solve timing must not be added together.
The mechanism changes no CPU/memory/scratch request, node count or cloud quota.
