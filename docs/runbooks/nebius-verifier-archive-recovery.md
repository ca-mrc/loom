# Recover a retained failed archive

## Native usage roundoff (#2199)

Deploy the compatible Control Plane with migration `0163` before using
`--reason usage-roundoff`. This is a storage-only recovery for a current-attempt,
deleted, finalized Terminus execution whose immutable runtime outcome succeeded
but whose canonical publication failed with `usage_output_identity_drift`.

```sh
python -m loom_control_plane.service_execution_archival_recovery \
  --lease-id LEASE_UUID --team-id TEAM_UUID --reason usage-roundoff --apply
```

Inspect the retained source first. Cost and duration are now summed with `fsum`;
historical totals allow only floating-point roundoff, bounded by one ULP per
model call. Call identity, model/provider, token counters, and all other fields
remain exact. A genuine accounting difference still fails recovery. Source
bytes and the original execution result are never rewritten.

The existing one-use archival queue, claim fencing, integrity verification and
canonical acknowledgement apply. Once the archive commits, its erroneous
`output_unavailable` Trial state becomes `succeeded`, matching the retained
successful runtime outcome. Artifact metadata records the original Trial state,
failure, finish time and archive failure under `usage_roundoff_archival_recovery`.
No new attempt or model call is created. Timeouts, model errors and verifier
errors are ineligible; a failed recovery retains the original Trial failure.

Download the recovered bundle and check the original runtime result, reward,
trace, call IDs, token accounting and unchanged attempt number before accepting
the repair. A queued retry alone is not acceptance. Migration `0163` downgrade
removes admission for usage recovery while preserving existing recovery history.

## Legacy verifier projection

Use this operation only for a deleted, finalized current-attempt execution whose
committed source failed canonical publication with `verifier_reward_drift` because
the old runtime read `diagnostics/verifier-exception.json` before
`verifier/output.json`. Deploy the compatible Control Plane and apply migration
`0158` (including `0157`) through the normal guarded rollout first. Inspect the
original source, lease/team identity, runtime failure and retention state before
requesting recovery.

Run the deployed module inside the Control Plane environment, using its existing
database and source-spool configuration:

```sh
python -m loom_control_plane.service_execution_archival_recovery \
  --lease-id LEASE_UUID --team-id TEAM_UUID --apply
```

`requeued` means the single archival retry was durably queued. It does not mean the
bundle is ready. `not_eligible` makes no change; this includes another team,
an advanced attempt, another integrity error, an active execution, a previously
requested recovery or an already-published archive. The command makes no model
calls and does not run or modify the deleted execution.

The ordinary materializer authenticates every source object and publishes through
its persisted claim. Transient storage failures retain the queue across restart;
another integrity failure returns the archive to `unavailable` and consumes the
one-use recovery. The new archive error is retained on the lease, while the Trial's
original state, result, reward, attempt, failure and finish time remain unchanged.
Do not clear the audit timestamp or patch a terminal state to repeat the operation.

Migration `0158` also appends a current history snapshot for an already-requested
recovery whose original timestamp was not recorded by the `0157` history trigger.
It preserves all existing lease values and history rows and does not repeat the
recovery. A downgrade/re-upgrade retains the snapshot without duplicating it.

After the archive becomes `committed`, download its ordinary Trial bundle and ATIF.
Verify the unchanged original runtime result, the independent verifier score,
source manifests, native trace, accounting and Trial outcome. A separate verifier
score does not change the original null Trial reward or turn the failed attempt
into a success. Source cleanup remains forbidden until canonical acknowledgement;
the normal configured retention window starts at that acknowledgement.

Migration downgrade restores the original guards only while no recovery timestamp
has been used. Once history exists it refuses to discard the audit; roll application
images back only with the repository's forward-schema compatibility checks.

## Separate-verifier settlement (#2474)

Before #2474, settling a separate verifier child violated
`execution_leases_materialization_group_check`, so its parent's archive rolled
back on every retry. The parent stays `pending` with
`transient_materialization_error`, and the child stays `pending` with zero
attempts. The idle rollout guard counts both as active, so the fix cannot be
installed until they leave `pending`.

`scripts/ops/nebius_verifier_archive_park.py` handles this in two steps. Each
step is a read-only preview unless `--apply` is given. With `--apply`, it changes
every named pair in one transaction, or changes nothing. Name up to eight parent
leases; each child is found through its Trial's `verifier_execution` handoff.

1. **Before rollout, on the installed schema:** park the pairs. This moves the
   parent and child from `pending` to `unavailable`, with code
   `verifier_settlement_constraint`. The parent's artifact records the previous
   attempt counts, error and next attempt times under `verifier_settlement_park`.

   ```sh
   python scripts/ops/nebius_verifier_archive_park.py park \
     --kubeconfig KUBECONFIG --namespace NAMESPACE --expected-cluster-id CLUSTER_ID \
     --team-id TEAM_UUID --parent-lease-id PARENT_UUID [--parent-lease-id ...] [--apply]
   ```

   Re-run the idle rollout check. These leases no longer count as active work.
2. **Roll out** the fix and migration `0177` through the normal idle rollout.
3. **Requeue** the same pairs with the same arguments, using `requeue` instead
   of `park`. Migration `0177` admits exactly this change: `unavailable` back to
   `pending`, with the recovery timestamp equal to the
   `verifier_settlement_requeue` audit. It only succeeds while the attempt
   counts still match the park audit. The ordinary materializer then archives
   the parent and settles the child in the same transaction.

Neither step rewrites a Trial, its result, its execution attempt, or the
retained source spool. Source cleanup only starts once the archive commits, as
usual. A requeue is single-use. A failure afterwards keeps the original Trial
outcome and records the new archive error on the lease. Download both Trial
bundles and check the runtime result, verifier output and accounting before
accepting the repair.
