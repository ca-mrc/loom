# Native build storage baseline (#2750)

Procedures for measuring native task-image build storage before any change to
the per-build `ephemeral_storage_mib` reservation, and for the remaining
two-node batching acceptance. Every live step is run by the runtime owner under
an explicit, scoped authorization. Nothing here lowers a limit, changes routing
or enables a collector by itself; both measurement sources ship disabled.

## What the measurements are

There are two independent sources. Neither is a guaranteed peak, and they are
reported side by side, never merged.

- **Trusted observer samples** (`resource_observer_builds`, off by default):
  - **What they are:** the existing node observer (the trusted helper over CRI and the Pod's disk-backed volumes and logs) also inventories builder Pods in `<execution-ns>-build`.
  - **Independence from Trial telemetry:** builder collection runs on its own thread, API client, 30 s schedule, 20 s deadline and ConfigMap (`builds-node-<node-uid>`), so a slow or failing build walk can't delay or replace Trial snapshots. It shares the observer container's CPU limit.
  - **Counters independent of disk walks:** each cycle first reads CPU and memory for every builder Pod in one call that walks no directories (neither volumes nor logs). Each Pod's storage walk, including its container log and writable-layer pair, is then a separate helper call with its own bound (at most 10 s, within the cycle). A walk that reaches the helper's deadline leaves only that Pod's storage unknown (`observation_deadline`, or `helper_timeout` if the call is killed); Pods the cycle can't reach are `storage_not_reached`.
  - **Fairness:** Pods are rotated each cycle, so a slow tree goes first at most once per rotation and can't keep later Pods unsampled.
  - **What the build controller stores:** per attempt and Pod, in `task_image_build_resource_usage`:
    - the validated Node UID. A snapshot from a different Node incarnation under the same name is never merged (`node_identity_changed`), and an attempt with a qualified Node UID only reads that Node;
    - the Pod's ephemeral-storage total, only when the helper produced a complete total, with the storage field's own measurement time;
    - otherwise the snapshot counts as **storage unknown**, with the helper's reason codes (for example `filesystem_limit` for trees over its entry cap, or `storage_stale`);
    - CPU and memory maxima per container;
    - each counted snapshot's own measurement times (up to 240, with any drop counted): the storage walk's start and end, and each container's CPU and memory times, so per-phase coverage and gaps can be stated per signal;
    - read failures as **unavailable**.
  - **How sampling runs:** a background round that a reconcile pass never waits on. Each round takes the 64 least recently sampled outstanding attempts, samples up to 8 at once (each bounded to 5 s), and reads each node's snapshot at most once at a time. A shared read is cut off after 12 s (`snapshot_read_timeout`) so it can't hold a reader slot, and in-flight reads are cancelled when the controller stops. A cached snapshot is checked again against its own freshness clocks when used. Samples are de-duplicated by producer snapshot time.
  - **What it still shares:** the round doesn't block lifecycle progress, but it uses the same database, API client and nodes as the controller. The first observer window measures that cost (section 3).
  - **Limits:** these are sampled maxima, and sampling is best effort. Short phases, peaks between snapshots, the state just before a build ends, and file-heavy builds the helper can't total can be missed. A build that finishes between rounds may have no row at all. The report states all of these as unobserved, never as small.
- **Boundary readings** (`record_boundary_usage`, off by default):
  - **What they are:** self-reported by the untrusted build phase, just before each component's BuildKit cleanup and on each solve failure.
  - **Volumes:** `builder-tmp` (`/scratch`) and `build` (`/loom/build`), each read separately. They don't cover the writable layer, logs, `prepare-tmp` or `publish-tmp`.
  - **Storage:** kept on the attempt's `native_build.boundary_usage`, with coverage status `complete`, `incomplete` (with the missing readings), `missing` or `rejected`, plus a status per record (`ok`, `partial`, `failed`, `timeout`, `skipped`). `complete` means every expected record arrived, not that each one is usable.

**Why not the kubelet Summary:** default MK8s kubelet serving certificates are self-signed, with no supported serving-CA, SAN or rotation contract (#2595). Verified direct `/stats/summary` access is not a supported Nebius path, so it is not used here.

## 1. Enabling and restoring the sources

**Observer samples** are an environment setting (`"resource_observer_builds": true`; it requires a configured `task_image_builder`). They take effect through the normal protected deployment:
- **What it renders:** a Role and RoleBinding granting the observer `get`/`list` on Pods in the build namespace only, the observer's `LOOM_RESOURCE_OBSERVER_BUILD_NAMESPACE`, and the actuator's `LOOM_EXECUTION_ACTUATOR_BUILD_RESOURCE_OBSERVATIONS`.
- **Without the flag:** none of these is rendered.
- **Kill switch:** removing the flag and redeploying.

**Boundary readings** are a builder setting applied live by the execution-tuning tool. Before changing anything:
1. **Capture the prior state.** In the output:
   - note `candidate_sha`;
   - confirm `controllers[].builder_boundary_usage_support` is `true` for the target's actuator;
   - record `builders.<target>.record_boundary_usage_saved`. It is exactly one of `absent`, `false` or `true`, the saved representation.

   ```bash
   python scripts/ops/nebius_execution_tuning.py inspect \
     --kubeconfig "$KUBECONFIG_PATH" --namespace "$PLATFORM_NAMESPACE" > before.json
   ```

2. **Change it:**

   ```bash
   python scripts/ops/nebius_execution_tuning.py apply-builders \
     --kubeconfig "$KUBECONFIG_PATH" --namespace "$PLATFORM_NAMESPACE" \
     --expected-candidate "$INSTALLED_CANDIDATE_SHA" \
     --build-record-boundary-usage "$TARGET_ID=true"
   ```

   The command:
   - holds the rollout mutex;
   - saves the intent with a ConfigMap resource-version check;
   - restarts only that target's actuator and reads the value back;
   - refuses before any write on an image without support;
   - never replaces Jobs. Existing Jobs keep their frozen command, and only later Jobs are instrumented.

3. **Restore the captured representation,** not just "off": `absent` → `=unset`, `false` → `=false`, `true` → `=true`.
   - **Exactness:** the saved config and the actuator's settings return byte for byte, and a change between `absent` and `false` still updates the actuator.
   - **Concurrent changes:** other builder settings changed during the window are left as they are. Compare `inspect` with `before.json` and resolve any difference, rather than overwriting it.

## 2. Exporting and reading the results

The report never connects to a database.
- **The export query** projects only:
  - identifiers;
  - a task digest;
  - the build exit code;
  - whether the build was instrumented;
  - the frozen Job's emptyDir limits and storage requests;
  - each phase's start and finish times;
  - the bounded `boundary_usage`;
  - the observer rows, with each counted snapshot's times.

  No ConfigMap, claim, command text or other Job field leaves the database.
- **Shape check:** the report rejects any input line with additional fields.

Create the export and the report privately:

```bash
python scripts/task_image_boundary_usage_report.py --print-query > export.sql
# Run export.sql read-only with :since bound, one JSON object per line, into a private file:
( umask 077; "$READ_ONLY_SQL_TOOL" ... < export.sql > /protected/attempts.jsonl )
python scripts/task_image_boundary_usage_report.py /protected/attempts.jsonl --output /protected/report.json
```

`--output` creates a new mode-0600 file and refuses to overwrite.

**Per attempt, the report shows:**
- identity;
- task revision and digest;
- cache configuration (cache hits aren't recorded, so they show as `unknown`);
- the exit code;
- the effective Pod storage request and all four volume limits from the frozen Job;
- every boundary record, against its own volume limit;
- every observer row, with the storage maximum against the effective Pod request;
- `observer_status`: `sampled`, `storage_unknown_only`, `no_usable_snapshot` or `no_observer_row`. Every exported attempt is listed, so unsampled attempts (failed builds included) are counted rather than left out;
- `phase_coverage` for `prepare`, `build` and `publish`, separately for storage, CPU and memory, each by its own measurement time (never the snapshot's start): measurements inside the phase's window and the start, end and largest gaps.
  - **Storage** is a walk over an interval. It counts for a phase only if the whole walk falls inside it; a walk that crosses a phase boundary is `ambiguous` and counts for neither.
  - **CPU and memory** count only for that phase's own container.
  - A signal with no measurement is `unobserved` (or `ambiguous_only`). A complete storage sample for an attempt does not mean each phase was observed.

**Unusable or unknown data stays explicit:** `not_instrumented`, `not_observed`, record-level `partial`, `failed`, `timeout` and `skipped`, and observer `storage_unknown`, `unknown_codes` and `unavailable`. None of these contributes to a distribution. Keep exports and reports private.

## 3. Observer feasibility (first enablement)

The first observer window establishes:
1. **Coverage:**
   - the share of build attempts by `observer_status`, including `no_observer_row` (short builds) and failed builds;
   - per phase and signal, the share `observed`, the `ambiguous` storage walks, and the start, end and largest gaps. `prepare` and `publish` are short and are the expected blind spots;
   - the unknown reasons for the rest, especially `filesystem_limit`, `observation_deadline` and `storage_not_reached` on file-heavy builds.
2. **Cost:** observer CPU against its limit, any change in node I/O pressure while builder collection is on, and controller database and API load from sampling rounds.
3. **Trial telemetry health:** Trial snapshot freshness on the same nodes must not degrade.
4. **Agreement:** whether observer maxima and boundary readings agree where both exist (the boundary reading covers two volumes; the observer covers the Pod).

**Decision:**
- **Adequate coverage at acceptable cost:** continue the baseline campaign.
- **Unknowns dominate:** report the gap and defer any reduction. Unknown is never treated as small.
- **Cost is unacceptable:** remove the flag; the Trial observer is unaffected.

## 4. Baseline trial (draft for owner review)

- **Candidate:** a `dev` commit containing #2810 and this change. Before starting, verify it with `inspect` (`candidate_sha`, `builder_boundary_usage_support: true`), and confirm the observer DaemonSet and actuator carry the builder settings from section 1.
- **Target:** the north standalone builder only (`nebius-web-2048-20260924`, namespace `<execution-ns>-build`).
- **Tasks:** a small representative set that **needs new materializations**. The earlier ten-task cohort now has ready current-recipe images and would not build again. Choose light, medium and heavy builds (by earlier build time and image size), and record for each:
  - task revision;
  - verifier mode;
  - materialization state;
  - whether cache import is configured.
- **Window and envelope:** at most 60 minutes. State the expected node count and cost before starting; report actual node-hours and cost afterwards (provider records where available, separate from observation-based estimates).
- **Who runs what:**
  - **The runtime owner:** deploys the observer flag, applies and restores the boundary flag, and holds the protected kubeconfig.
  - **The requester:** submits the tasks and runs the read-only export and report.
- **Stop conditions:**
  - an unexpected build failure or deadline;
  - node I/O pressure above the admission ceiling for a sustained period;
  - Trial snapshot freshness degrading;
  - readings mostly unknown, `timeout`, `failed` or `not_observed`.

  On any of these, restore the boundary flag, stop submitting, and remove the observer flag at the next deployment if the observer is implicated.
- **Afterwards:** confirm that:
  - every attempt is `ready` or failed with its reason;
  - reservations are released with receipts;
  - `inspect` matches `before.json` (or the difference is explained);
  - nodes have been reclaimed.
- **Results:** the section 2 report.
- **Inconclusive run:** if too few attempts have complete observer storage or usable boundary readings, say so. Don't extrapolate from the four local prototype workloads.

The earlier ten-task cohort is context, not a controlled comparison: routing,
cache state, task set and resource envelope all differ.

## 5. Scaling investigation and two-node acceptance

**Scaling capture:** run `scripts/ops/capture_nebius_cold_start.py` (see [cold-start diagnosis](nebius-cold-start.md)) from zero nodes to final reclamation. It takes one `--namespace`, so run it once each for the execution and build namespaces on each target in scope.
- **Lifetimes:** keep observed node lifetimes separate from provider VM lifetimes and billed cost.
- **The earlier extra west node:** its events have likely expired, so a later run can explain a reproduced extra node but can't prove what caused the earlier one.

**Two-node batching proof, as a procedure for the owner:**
1. **Snapshot the live configuration:** capacity policies (with versions, `max_nodes` and enabled state) and node-group autoscaling settings.
2. **Physical cap:** `execution_max_nodes = 2` on the north node group (the Terraform autoscaling maximum) through `nebius-environment-lifecycle`, plus the CP capacity policy `max_nodes = 2` so admission matches. A CP cap alone doesn't limit the physical node count.
3. **North-only routing:** use a supported, test-scoped mechanism if one exists. Otherwise the owner decides on a time-boxed alternative, with its impact on other users, its preconditions and its rollback written down first.
4. **Two qualified nodes:**
   - **Demand-driven scale-up:** storage requests limit each node to four concurrent builds, so a fifth admitted build can trigger node 2. This is possible but not guaranteed, because short builds may finish before a node fills.
   - **Fallback:** a supported pre-provisioning method, if the owner has one.
   - **Precondition to record:** both nodes qualified with free capacity in the same admission pass, with work still queued.
5. **Pass:** two claims in one admission pass bound to different qualified north nodes, followed by confirmed cleanup and released reservations, within the 60-minute window and with actual cost reported.
6. **Restore:** use the supported versioned compare-and-set path, resolving any concurrent changes rather than overwriting them. Then verify the cap, routing and eventual node reclamation.
7. **Inconclusive:** if step 4's precondition is never reached, the run is inconclusive, not failed.

## 6. Sizing decision gate

No lower `ephemeral_storage_mib` is proposed until a review covers all of the
following:

- **Representative builds:** light, medium and heavy, with task revision and cache conditions recorded.
- **Coverage:** complete observer Pod totals and usable boundary readings across the relevant attempts and phases, with every gap stated: unobserved attempts and phases, and the start, end and inter-snapshot gaps from `phase_coverage`.
- **Failed and evicted attempts:** including those with no reading or sample.
- **Every constraint the setting controls:**
  - the Pod's effective request (against observer Pod maxima);
  - the full-size `build` and `builder-tmp` caps;
  - the quarter-size `prepare-tmp` and `publish-tmp` caps, so the setting must be at least four times their headroom-adjusted maximum.
- **The effect on effective requests and packing.**
- **A justified margin** for sampling gaps and workload variation.

**Unobserved periods:** if short phases, transient peaks or file-heavy builds remain unknown, state what is unknown and whether it prevents a defensible reduction.

**Testing a change:** any change gets its own measured comparison before the final combined validation.
