# Correct a previously committed Terminus accounting export

Trajectory display uses each call's frozen pricing marker, not its numeric cost
snapshot alone. Token-only calls display `n/a`, missing/invalid pricing displays
`unknown/unpriced`, and failed upstream calls display `unavailable`; a priced
call can legitimately display `$0.0000`. This presentation also applies to
historical events and does not rewrite their snapshots or require an export repair.

The existing Control Plane materializer reconciles terminal, current-attempt Terminus-2 exports when new calls appear in their tenant, lease, and output-generation-bound Gateway ledger (#1962). It compares the published call count with the bound ledger and uses the same fenced helper described below. It retries transient or concurrent changes without resetting the committed materialization or changing the Trial outcome. The CLI remains available for an explicit inspection or correction of one lease. This operation performs no model calls, creates no Trial or worker, and changes no result, reward, attempt, source commit, or retention deadline.

Run the deployed module inside the Control Plane environment, using its existing database and object-storage configuration. Supply the exact lease UUID and owning team UUID. Start with a read-only projection:

```sh
python -m loom_control_plane.service_execution_accounting_repair \
  --lease-id LEASE_UUID --team-id TEAM_UUID
```

Check the reported request count and token totals against the selected lease's Gateway ledger. Publish that correction with:

```sh
python -m loom_control_plane.service_execution_accounting_repair \
  --lease-id LEASE_UUID --team-id TEAM_UUID --apply
```

The helper selects the source output generation, writes new objects under a unique `accounting-v2/<UUID>` revision, and atomically changes the canonical bundle and trajectory pointers with the materializer-owned event rows. Original objects and the lease’s original materialization ACK digests remain unchanged; corrected digests belong to the new canonical pointers. Objects remain registered under their existing lifecycle authority. Original usage and native trace are included as `source/accounting/usage.json` and `source/trajectory/events.jsonl`; corrected totals and the safe Gateway ledger are under `files/accounting/`. Native Harbor turns and commands retain their meaning. Each later revision reads the immutable `source/trajectory/events.jsonl`, never the already canonicalized event stream. Original source entries appear only once. A failed or cancelled runtime may lack verifier output or a native trace; reconciliation preserves that absence and the failed outcome rather than inventing a verifier result. Successful runtimes still require their original trace and verifier output.

The helper refuses another team's lease, a nonterminal or superseded attempt, foreign event producers, corrupt input, or changed projection inputs. It removes its unpublished objects on a known precommit failure. A lost database connection during commit leaves the uniquely named revision intact because commit may have succeeded; inspect the canonical pointer before cleanup. The accounting source marker alone never proves convergence: the helper compares the current ledger, usage, canonical events, persisted event rows, and trajectory/ATIF identities. If they match, it returns `already_corrected` without writing new objects; applying an old revision that lacks the published call count only backfills that metadata under the same transaction fence. A later inserted call produces a new derived revision. The selector count assumes Gateway accounting rows are immutable; it is not a detector for manual in-place ledger edits.

After `corrected`, download the ordinary Trial bundle and ATIF through the public API. Confirm all bound request/token totals (including failed and late calls), unchanged outcome and any verifier reward, unchanged native turn count, and that the original source files are still available. Deployment alone does not prove an older export was corrected.

## Failed calls and execution deadlines

Gateway failure audit rows retain the authenticated native lease and generation before projecting into the canonical ledger. A failed upstream call with unavailable usage carries `missing`; its numeric zero placeholders do not prove zero provider consumption. Canonical events retain typed call/usage status and bounded failure categories. Usage and ATIF report `failed_call_count`, `missing_usage_call_count` and `partial_usage_call_count` when any is nonzero, separately from the reported numeric totals. Four missing-usage failures plus one known 9,191-token response therefore remain five calls, four unknown usages and 9,191 known tokens; they do not become five fully measured calls. Healthy legacy usage documents retain their existing shape. No lease association is inferred from a Trial ID, capture time, request body or header. Historical rows lacking a trustworthy binding remain excluded from the lease export and may still appear in the Trial-wide API; do not manually invent that association to make totals agree.

The native Go supervisor supplies the actual phase deadline to its workload broker, which forwards it to the native token issuer. The issuer clamps it to the admitted lease deadline and signs it. The Gateway cancels outstanding upstream work at that deadline; a rotating 480-second native token can represent a longer phase while retaining the existing 600-second token lifetime ceiling. The broker invalidates its cached token at every phase boundary and rejects model requests outside an active phase. During a rolling update, older runtimes that omit the phase cutoff receive the signed lease cutoff. Generic worker token lifetime requirements are unchanged.

For the OpenAI facade used by Terminus, expiration before dispatch produces no call row; expiration after dispatch preserves a bound failed call with unknown usage. This stops requests continuing beyond their execution deadline. It does not establish why a provider failed to respond within its read timeout, recover usage that the provider never returned, or introduce genuine upstream streaming. Diagnose repeated provider timeouts separately from archive convergence; increasing the upstream or agent timeout is not part of this repair.

## Project older usage rows

Batches whose calls predate migration 0182 read cost and usage status from the raw-log JSON, which makes their detail and list reads slow. To project those rows, run the module in the Service environment, using its existing database configuration. Start with a read-only count:

```sh
python -m loom_service.llm_call_projection_backfill
```

Then update them, after confirming that no maintenance is in progress:

```sh
python -m loom_service.llm_call_projection_backfill --apply
```

Each chunk of 500 rows commits separately and is followed by a pause at least as long as the chunk took, so this can run during batches. It changes no cost, token, result or raw-log value, and an unprojected row already reads as the same value. Stopping is safe: a lock or statement timeout ends the command with an error, and rerunning skips rows already projected. Finish when it prints `{"remaining": 0}`. Autovacuum reclaims the old row versions. Compare Batch detail timings for an old Batch before and after. Keep the reader fallback in `usage_accounting.py` until every environment reports 0.
