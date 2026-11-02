# Incoming execution warm fit

`ControlPlaneSettings.prefer_incoming_warm_fit` defaults to `False`
(`LOOM_CP_PREFER_INCOMING_WARM_FIT`). It prefers a Ready node for the **actual
incoming Pod**, after selecting work by the existing origin/team/priority/age
order. It does not reduce resource limits, provider quotas, create rates or I/O
requirements, and does not make execution warm-only. Unknown evidence retains
the ordinary cold-capable path. Already selected outboxes, grants and leases
are never moved. A separate verifier remains on its parent's target/region.

## Local path

The actuator's existing successful namespace reconciliation records only its
namespace, RuntimeClass and node selector alongside that same health timestamp.
The scheduler renders the target-specific allocated runtime using that report,
then measures the real Pod including init containers and sidecars. A historical
report, changed health timestamp, foreign namespace, named RuntimeClass with
unknown overhead or a selector not proven by the exact Node labels supplies no warm hint.
The collector projects a small scheduling-label whitelist from the same Node read; omitted historical labels preserve old observation bytes and give no selector proof. No new Kubernetes permission or observer is needed.

`admit_capacity_resources(preview=True)` reads the same qualified observations,
exact-UID release reconciliation, foreign occupancy, charged/unobserved
execution/native reservations, renewable older waits, create/pending/health
budgets and shared provider quotas as real admission, without its policy row
lock or any reservation. `incoming_warm_nodes` proves the incoming envelope on
a Ready, non-draining exact UID without displacing protected capacity. Full
fresh I/O and causal native-build feedback retain their existing identity and
binding requirements. It does not replace the final capacity transaction lock.

## Management-owned global path

`POST /internal/pools/v1/{pool_id}/execution-preview` accepts
`PoolExecutionPreviewRequestV1`. It uses the existing participant machine token
and only that token's pool/participant scope. The manager opens **REPEATABLE READ
READ ONLY before authentication**, qualifies current intake/origin/profile/
namespace/epoch, computes the actual target's node-share allocation where
requested, renders its real Pod including registered RuntimeClass overhead, and
uses the same charged/protected-wait qualification as normal preparation.
It inserts or renews no request, queue age, reservation, cost or outbox, and takes
no mutation/row lock. Expiry includes observation and applicable I/O freshness.
An active native build without owner-retained causal phase proof yields unknown
warm preference; its resources remain charged. Ordinary execution acquires no
new native-only admission gate.

The response is advisory. The selected owner recompiles under the existing
per-Trial lock, checks the existing immutable request identity and route
generation, and writes the one normal live outbox. The original manager
`prepare`/`activate` reauthorize and re-render under the existing mutation lock.
New reservations, cancellation, failed images, epoch/profile/namespace changes
cannot be bypassed by a prior preview. Preparing multiple alternatives and
cancelling losers is not used.

### Actual owner settings

Global preference requires **both** the actual CP flag and each compared pool's
actual management DB preference. Actor settings do not establish manager policy.
The protected normal administration request is:

```json
{
  "schema_version": "loom.pool-routing-preference.v1",
  "operation_id": "<fresh operation UUID>",
  "installation_id": "<existing installation UUID>",
  "pool_id": "<existing pool UUID>",
  "prefer_incoming_warm_fit": true
}
```

Use the existing platform-owner authorization and fixed administration Job
(`nebius_pool_administration_job.py`), with `apply` and fresh `status` readback.
`null`/omitted preference is inspect-only. The existing operation journal retains
intent/replay; the actual scalar value survives process/profile releases without
changing the physical binding fingerprint, epoch or participant revision. Partial,
unsupported or mismatching readback is not a successful enable. Disable through
the same owner interface; committed work is unchanged. This uses no new token
scope, signature or authorization channel.

### Multiple existing physical owners

One `PoolParticipantV1.targets` list is **one physical pool**, not two regions.
An empty `PoolRuntimeSettings.placement_peers` is the historical wire format and
has zero alternatives. Optional peers are already registered, non-secret
participant metadata in the existing persisted
`LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON`. They must share the correct source
data environment/catalog class, have distinct pool/participant identities, and
retain each namespace UID, epoch and revision. They contain no credentials.
Normal participant candidate release preserves this independently configured
metadata. Editing legal peer metadata needs normal saved configuration/CP rollout,
not a new image; it never enrolls or authorizes a participant.

Each owner process calls **its own manager with its own existing token**, closes
SQL transactions before HTTP, and publishes a thin expiring row in
`nebius_pool_placement_advice` in the existing shared CP DB. Advice contains only
the scope, existing immutable request identity/allocation, requests, source/route
fence and warm result; no full Task/Job payload or credentials. It is absent from
all resource ledgers. Original fair scanning chooses work; fresh complete owner
advice chooses the target before the one live outbox. A bounded gather interval
uses the existing dispatcher poll/transport budget, never resets its first time,
holds no SQL transaction, and falls back to ordinary selection on expiry. Old
managers, mixed owner flags, incomplete or stale evidence retain the original
order rather than treating missing owners as pressure zero. Advice also expires within two existing poll budgets (capped by observation/I/O expiry), and an owner that loses qualification removes its old warm hint. Prefetching other fair heads is bounded by that same poll budget; an absent peer cannot hold a dispatcher in an unbounded HTTP scan.

`configured_alternative_owner_count` counts metadata; `alternative_owner_count`
counts other owners with currently qualified, feasible advice. Neither a peer
string nor one participant's multiple profiles proves cross-region acceptance.
Without a second legitimate production binding, global multi-owner production
performance remains untested. Disposable integration tests use two actual
registered machine scopes, real management HTTP/Postgres, concurrent unique
outbox selection and only the winner's manager prepare/activation.

## Verification and performance boundary

Tests contrast the real local scheduler's old zero-pressure cold choice with
actual warm fit; cover explicit and target-specific node-share envelopes, init/
RuntimeClass overhead, foreign/stale/unknown evidence, cleanup/quota races,
old/mixed managers, cancellation and normal owner Job replay. Migration 0187
preserves physical identity protection when changing only the preference.

These prove functionality, not a throughput gain. The bounded positive comparison
keeps the same original 40 Task revisions ×3, Oracle/no-model and explicit shared
verifier mode, reporting fixed 300/600-second positive canonical and fully cleaned
outputs plus full makespan. Existing shared400 has one attempt lease per Trial;
it must not be explained using a separate-verifier two-lease create-rate ceiling.
Ready-image inputs may never exercise GC or sibling reuse, and absent warm/cold
alternatives do not measure routing benefit. Record those coverage limits rather
than deleting cache, changing Tasks or registering production owners for a test.
