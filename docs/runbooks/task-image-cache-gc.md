# Task-image cache publication and collection

Remote BuildKit cache uses the existing manifest mark-and-sweep collector.
Publishers and the collector share `task-build-cache/.mutation-lock`: local
bounded output hashing happens before that lock; remote mutation, missing-blob
checks/uploads and manifest-last publication stay under it. A contender defers
its optional cache export. Required image, archive, runtime metadata and receipt
publication remain independent. No new daemon, storage plane or credentials are
introduced.

This follows [Distribution's manifest mark / blob sweep and exclusion of
concurrent uploads](https://distribution.github.io/distribution/about/garbage-collection/).
The batching uses age and space policies similar to
[BuildKit GC](https://docs.docker.com/build/cache/garbage-collection/), but local
BuildKit GC cannot collect this remote cache bucket.

## Batches and bounded debt

`task-build-cache/.gc-checkpoint.json` is small collector bookkeeping. Under the
same lock, every new publisher HEADs its unique blob keys, reads the checkpoint,
and **pre-accounts** its component's manifest plus the missing blob bytes before
any cache upload. If collection runs, the publisher repeats its HEADs under that
same lock: a previously present old orphan may have been swept and must become
accounted upload debt. The resulting missing set drives the uploads without
another HEAD pass. Shared warm blobs add no byte debt. Manifest replacements,
crashes or ambiguous writes can overcount; only a complete inventory resets debt. The object
is not an integrity/provenance envelope.

The first publication bootstraps a complete inventory. Later publications trigger
collection after at most 64 accounted publications, 256 MiB of additional debt,
or five minutes since the previous scan, whichever is reached first. A single
component may exceed the byte batch up to its existing bounded component size;
the 4 GiB total budget still applies. A successful sweep records retained bytes
and resets the counters. A publication is refused if retained bytes plus debt
and incoming bytes exceed 4 GiB. The existing seven-day grace protects recent
roots/blobs even when the pre-existing store is already above budget; publishers
then defer until collection can reclaim space. Refusals after a complete sweep
wait for its next age tick instead of repeatedly rescanning an unchanged store.

Collection runs lazily on the next publication; an idle bucket adds no debt.
The existing `trim_cache(client, bucket, 0)` maintenance entry point also performs
a complete locked sweep and refreshes the checkpoint, including repair of a
malformed checkpoint. No recurring job or shared-store action is installed by
this change. Missing checkpoint means bootstrap inventory; malformed/unreadable
checkpoint means defer, never zero debt. Unreadable manifests prevent deletion
and debt reset. Byte/count debt remains bounded; a failed required collector
cannot silently allow append-only growth.

All publishers and collectors must use the same checkpoint protocol for the
budget bound. A single mutex does **not** make old publishers' unrecorded writes
visible in the checkpoint. For rollout, stop/drain publishers running the older
runtime before allowing this runtime to publish to that bucket, and remove a
stale checkpoint only while all publishers are stopped so the first new writer
rebuilds it. Mixed old/new writers retain lock-based reference safety but cannot
claim the checkpoint's budget bound. During rollback, stop/drain new writers,
leave all cache data and manifests intact, and remove only the GC checkpoint
while publishers are stopped; the older collector then resumes its prior
per-publication behavior. A subsequent upgrade bootstraps again. This is a
runtime cutover requirement, not a claim of completed live qualification.

Servers that ignore conditional PUT or cannot establish the lock now **defer
optional cache retention** rather than append indefinitely without a collector.
Existing cache imports remain available. A crashed holder is never timed out or
force-unlocked. Operators must establish that old publishers are stopped before
removing its orphan lock, using the existing recovery boundary.

## Observability and local evidence

Existing `cache_export` and `cache_gc` stage/request markers remain. Added `debt`
markers report counted publications, conservative bytes and configured batch
limits; `lock_released` reports lock-held duration. `cache_checkpoint_unavailable`,
`cache_gc_incomplete`, `cache_budget_protected`, `cache_mutation_busy` and
`conditional_write_unsupported` identify distinct optional-cache deferrals.
Monitor export success/deferred rates alongside actual cache hits and successful
image throughput; a lower scan count alone does not prove production acceptance.

A deterministic fake-store fixture published eight components sharing one
12-byte blob against 1,000/10,000 retained manifests. The baseline was commit
`b3945dcd`; output contents, per-component limits and sequential demand matched.

| Retained manifests | Version | LIST | Manifest GET | All GET | HEAD | PUT | Exports / deferred | Blob uploads |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 1,000 | Before | 8 | 8,028 | 8,028 | 16 | 32 | 8 / 0 | 1 |
| 1,000 | Batched | 1 | 1,000 | 1,008 | 9 | 41 | 8 / 0 | 1 |
| 10,000 | Before | 8 | 80,028 | 80,028 | 16 | 32 | 8 / 0 | 1 |
| 10,000 | Batched | 1 | 10,000 | 10,008 | 9 | 41 | 8 / 0 | 1 |

The extra eight GETs are checkpoint reads; PUTs include conditional probes,
lock acquisition, manifests and checkpoints. Both versions uploaded 12 bytes.
On one local run, total fake-store publication time was 13.0 → 2.1 ms at 1k and
139.7 → 37.9 ms at 10k. These are in-process measurements, not network latency or
production throughput. Bootstrap and periodic sweeps still read the inventory;
batching amortizes that work and does not eliminate its inventory-dependent
cost or guarantee asymptotically linear work for an ever-growing workload.
A separate sparse-file/object-metadata fixture models a shared 512 MiB blob:
eight warm publications perform one bootstrap LIST, nine HEADs and no blob
upload; only their small manifests accrue debt. Newly uploaded large components
can still trigger space-driven sweeps; those do not disappear through caching.

`test_task_image_cache_storage.py` also uses two existing pinned real MinIO
fixtures: the 2025 release validates native conditional writes, eight successful
exports with one initial LIST, shared-blob reuse, exact import and reference
retention after a sweep; the 2022 release demonstrably ignores conditional writes
and validates safe deferral with legacy cache preserved. Registry subprocesses
remain mocked in these storage tests: this is not deployed image acceptance.

Outstanding live comparisons include concurrent burst export/deferred rates,
image throughput, peak disk, transferred bytes and later cache-hit performance
under a matched build/resource envelope. The local regression matrix covers
bounded count/bytes/age triggers, failed/ambiguous checkpoint writes, cancellation
before/after manifest publication, unreadable manifests, budget pressure, lock
contention and unsupported conditional writes.
