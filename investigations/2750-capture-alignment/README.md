# #2750 collector capture alignment: evidence (not for merge)

This branch holds the evidence behind the decision on #2750 **not** to ship capture alignment or a 10s collector interval. It is not intended for merge. Nothing here changes deployed behaviour: the prototype is off unless `LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_ALIGNMENT_MARGIN_SECONDS` is set. No database exports are included; every number below can be regenerated with the SQL and commands given.

## Contents

| Path | What it is |
|---|---|
| `src/loom_execution_capacity_collector/capture_schedule.py` | Prototype scheduler. The schedule rules are in the #2750 design comment. |
| `src/loom_execution_capacity_collector/{__main__,collector,config}.py` | Wiring: `_with_node_io` records the PromQL request time and each window's newest point and step into an in-process `ContextVar`. Receipts are unchanged. |
| `tests/unit/test_capture_schedule.py` | Deterministic cases: phases, fallbacks, slow API calls, late CronJob starts, unchanged points, deadline, stepped clock, exact call count. |
| `tests/unit/test_capture_schedule_replay.py` | Replay with a fixed ingestion delay, calibrated against the 2026-11-02 probe. |
| `tests/unit/test_capture_schedule_measured_replay.py` | Replay with a per-point delay drawn from the measured availability curve. |
| `sql/probe-feedback-timing.sql` | Qianyi's per-build stage query for probe batch `f565d2e0`, with a 60s statement timeout. |
| `sql/probe-capture-phase.sql` | Provider point and capture phases (mod 15s) during the probe. |
| `sql/availability-by-age.sql` | Share of points returned by age, 24h, the input to the measured replay. |
| `sql/unseen-after-due.sql` | How often the next point was still missing after it was due. |
| `sql/nodes-in-window.sql` | Nodes and reads behind the 24h curve. |

## Reproduce

```bash
# Replays and tests (no database or cluster access needed)
uv run --no-sync pytest -q -p no:cov -s tests/unit/test_capture_schedule.py \
  tests/unit/test_capture_schedule_replay.py tests/unit/test_capture_schedule_measured_replay.py

# Queries: read-only diagnostic views, run through the private debug-access client
cd ~/.loom/nebius-debug-access
python3 database.py --file <checkout>/investigations/2750-capture-alignment/sql/availability-by-age.sql
```

The queries run against the `hrishi_diagnostics` read-only views (`trials`, `trial_task_image_materializations`, `task_image_materialization_attempts`, `execution_capacity_observations`). Each opens `BEGIN READ ONLY`, sets a 60s statement timeout and ends with `ROLLBACK`.

## Results

**Probe split (batch `f565d2e0`, medians after each node's first start):** eu-north1 had a 41.4s claim gap, made up of 2.5s to the build start, 11.3s to the first provider point after it, a 15.0s window and 14.1s until capture. eu-west1 had 29.4s: 3.6s, 3.2s, 15.0s and 7.0s.

**Fixed-delay replay** (ingestion 1–4s, 30 phases × pre-request times 2/5/8s): aligned was never slower than fixed at any phase and kept 4 captures a minute. The mean gap dropped from 35.8s to 33.5s, and eu-north1's probe phase dropped from 45s to 30s. With 6–8s ingestion and a 4s margin, aligned was slower at some phases.

**Availability curve**, 2026-11-01 19:00 to 2026-11-02 19:00 UTC, 15s steps only, 19 nodes and about 6,100–6,200 reads per region. The share of points returned by age:

| Age (s) | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 10 | 13 | 16 | 18 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| eu-north1 | 0.17 | 0.28 | 0.42 | 0.43 | 0.62 | 0.58 | 0.80 | 0.84 | 0.85 | 0.97 | 1.00 |
| eu-west1 | 0.03 | 0.32 | 0.28 | 0.34 | 0.48 | 0.34 | 0.50 | 0.77 | 0.96 | 0.99 | 1.00 |

**Measured-delay replay**, mean / worst seconds per start (30 phases × pre-request times 2/5/8s × 2 seeds):

| Schedule | eu-north1 | eu-west1 | Calls per minute |
|---|---|---|---|
| Fixed 15s (current) | 40.3 / 48.9 | 41.6 / 50.2 | 4 |
| Aligned, 4s margin | 40.0 / 48.9 | 40.9 / 50.2 | 4 |
| Aligned, 8s margin | 38.8 / 48.9 | 41.1 / 45.4 | 4 |
| Fixed 10s | 38.5 / 43.4 | 41.0 / 46.8 | 5 |

## What the availability curve does and does not show

**It bounds availability; it does not measure it.** The collector stores, for each node, only the newest two points that the provider returned at request time. We never see when a point first became readable at the provider. Each pair of an observation and a point is a single status check ("returned by age *a*" or "not yet returned at age *a*"), so the curve estimates the probability that a point is available by a given age. It is not a direct measurement of ingestion delay. The first capture that saw a point is an upper bound on that point's availability, not its availability time.

How it was extracted: for each observation, the newest returned point *p* counts as "returned at age `observed_at − p`", and the following point *p + 15s*, when due, counts as "not returned at age `observed_at − p − 15s`". Ages are binned to whole seconds. Only windows with a 15s step are used.

Known biases:
- **Age is measured from `observed_at`, not the request.** `observed_at` is set after the I/O read completes. Every age is therefore overstated by the read's duration, up to the 10s I/O budget and typically under a second. For a fixed true delay, this moves both returned and not-returned checks to larger ages. The curve therefore looks slower than the provider is, by roughly the read duration.
- **Phase locking produces uneven sampling.** Captures run at fixed offsets within the minute, and each node's points arrive at a fixed phase. So a given node is checked at only a few distinct ages, and each age bin mixes different nodes and times. That is why the curve is not monotone (for example, eu-west1 0.48 at 6s but 0.34 at 7s). The replay takes a running maximum, which makes it slightly *more* favourable to alignment than the raw bins.
- **Missing points look like slow points.** If the provider skips a sample, the following point counts as not returned until it is overdue by a whole step. Windows with a 30s step are excluded, but a skip that falls outside the stored window is not detectable here. This inflates the tail.
- **Only the next point is checked.** Points two or more steps ahead are not counted, and points are assumed to become readable in timestamp order.
- **Delays are drawn independently.** The measured replay draws each point's delay independently. Real delays are probably correlated over time and across nodes. Correlation would let the in-run margin widening adapt better, but it cannot remove the 15s step or the typical multi-second delay.
- **The probe window was a fast period.** The probe's 2.4–3.1s captures on eu-north1 sit at the fast end of this curve.

**Why the decision stands despite these biases.** Even the most favourable model, a fixed delay inside the margin, improves the mean only from 35.8s to 33.5s, and a quarter of phases cannot improve within the call budget. The measured-delay replay, which the biases above make slightly optimistic for alignment, shows at most about 1.5s.

**A direct measurement, if wanted.** Query the provider for one node every 1s for about 10 minutes with the collector's credentials, recording the first time each point is returned. That gives availability to 1s resolution, with no phase locking and no `observed_at` offset. It needs the collector's provider credentials, which the debug access does not have, so it would have to be run or approved by an operator.
