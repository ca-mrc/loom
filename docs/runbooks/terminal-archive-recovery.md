# Terminal attempt and verifier archival recovery

A deleted execution can still legitimately block idle rollout until its durable
source has been archived. Revoke and Kubernetes cleanup do not replace the
canonical archive acknowledgement. Keep `ACTIVITY_SQL` and the rollout guard.

For the deferred-verifier closeout fault in #2507, use
`scripts/ops/nebius_terminal_archive_recovery.py` only with a successfully published
immutable `dev` Control Plane image containing the constraint fix. This separate
storage-only worker does not start the application, scheduler, task execution,
source cleanup, or migration loops. It processes one explicitly named parent and
the exact committed verifier already named by that Trial. It runs on an existing
Control Plane node; it does not request extra nodes or authorize paid Trials.

Only current-attempt, finalized, revoked, deleted, cleanup-complete, committed
parent and child sources qualify. Both must have pending archives and no active
claims; expired running claims are also refused. Failed/cancelled outcomes and
runtime result/reward/attempt are preserved. A successful `materializing` Trial
becomes succeeded only after ordinary validated canonical commit. Each source
is bounded to 1,024 files and 512 MiB. The independent source store is retained.

1. Inspect each exact team and parent lease using the kubeconfig already configured
   for ordinary protected rollout. Its current context is preserved; the cluster
   identity is checked again before submitting a Job. Inspection exports bindings and safe
   digests, not source contents, provider credentials or raw failure messages:

   ```bash
   uv run --no-sync python scripts/ops/nebius_terminal_archive_recovery.py \
     --kubeconfig "$RECOVERY_KUBECONFIG" --namespace "$RECOVERY_NAMESPACE" \
     --inspect-team-id "$RECOVERY_TEAM" --inspect-lease-id "$RECOVERY_LEASE" \
     --evidence-dir "$RECOVERY_EVIDENCE/inspection"
   ```

2. Build a private request from the returned `binding`, plus a fresh
   `operation_id`, installed `schema_head`, namespace/cluster, installed
   candidate/image and recovery candidate/image. Confirm the installed identity
   against the platform ConfigMap and live Deployment/Pod digest; bind the new
   image to the successful GitHub candidate publication. This lane supports
   installed schemas `0175` and `0176`; on `0175`, only the unrelated new baseline
   linkage is excluded from ledger reads. No schema update is performed.
3. Supply `--request`, `--publication-run-id`, `--kubeconfig`, and
   `--evidence-dir`. The default writes a reviewable Job without submitting it.
   Use the same arguments plus `--apply` once. The existing submission journal
   persists intent before creation. An ambiguous response permits only exact
   Job readback, never a new operation or duplicate Job.
4. Preserve the Job UID and private logs. The worker waits at most six minutes
   for normal backoff/ownership; it never changes retry times or steals a claim.
   The ordinary claim increment and one-use Artifact audit commit atomically,
   with the shared rollout admission lock held. Source, current Trial projection,
   and parent/child identity are rechecked before copying and canonical commit.
5. A `committed` report must be independently confirmed: parent and verifier
   committed, both canonical digests consistent, versioned canonical receipts
   and downloads available, original failed outcome/result/reward/attempt
   unchanged, and source retention deadlines intact. Do not run source GC or
   shorten retention as part of this repair. `deferred`/`blocked` is not success;
   retain its exact safe reason and private diagnostic state before further work.

The Job has no Kubernetes service-account token, no runtime signing authority,
no automatic retry, and an 1,800-second deadline. Its worker stops after one
owned archival pass, with a 1,500-second soft timeout and 1,700-second process
watchdog. Failures use normal pending backoff and retain private diagnosis
without rewriting the original Trial outcome. Do not launch a duplicate recovery
to erase a failed record.

Only after the six leases naturally reach canonical terminal state should the
normal broker rollout acquire idle protection and deploy the published latest
`dev`. Then read back workload digests, platform candidate, service health, and
the original Trials' downloadable artifacts. Green candidate/Job checks alone
are not deployment or acceptance evidence.
