"""Replay with the measured provider ingestion delay (#2750).

Share of points returned by age (seconds after the point's timestamp) from
execution_capacity_observations, 2026-11-01 19:00 to 2026-11-02 19:00 UTC, 15s
steps only (investigations/2750-capture-alignment/sql/availability-by-age.sql).
This bounds availability from periodic reads; see that directory's README.
Each point's delay is drawn once from this distribution with a fixed seed.
"""

from __future__ import annotations

import math
import random
import statistics

from loom_execution_capacity_collector.capture_schedule import (
    CaptureSchedule,
    CaptureTiming,
    NodeIORead,
)

LEAD_JITTER = (0.0, 0.7, -0.4, 1.2)
READABLE_BY_AGE = {
    "eu-west1": [0, 0, .03, .32, .28, .34, .48, .34, .50, .59, .77, .68, .71, .96, 1, .98, .99, 1, 1, 1, 1],
    "eu-north1": [0, 0, .17, .28, .42, .43, .62, .58, .80, .74, .84, .70, .73, .85, .89, .90, .97, .99, 1, 1, 1],
}


def _delays(region, seed):
    cdf, peak = [], 0.0
    for share in READABLE_BY_AGE[region]:
        peak = max(peak, share)
        cdf.append(peak)
    rng, cache = random.Random(seed), {}

    def delay(point):
        key = round(point, 3)
        if key not in cache:
            draw = rng.random()
            cache[key] = next((age for age, share in enumerate(cdf) if share >= draw), 20) - 1 + rng.random()
        return cache[key]
    return delay


def mean_gap(region, *, phase, lead, margin, interval=15.0, seed=1, minutes=60, starts=40):
    delay = _delays(region, seed)

    def newest(at):
        point = phase + 15 * math.floor((at - phase) / 15)
        while point + delay(point) > at:
            point -= 15
        return point

    requests, index = [], 0
    for minute in range(minutes):
        base = 60.0 * minute
        start = base + 2.0
        deadline = min(start + 50.0, base + 55.0)
        schedule = CaptureSchedule(interval=interval, margin=margin or 0.0)
        while True:
            request = start + max(0.1, lead + LEAD_JITTER[index % 4])
            index += 1
            timing = CaptureTiming(started_monotonic=start, started_wall=start)
            if margin is not None:
                timing.read = NodeIORead(requested_wall=request, series=(("n1", newest(request), 15.0),))
            requests.append(request)
            nxt = schedule.next_start(previous=timing, now=request + 0.5, deadline=deadline)
            if nxt is None:
                break
            start = nxt
    claim, gaps = 1.0, []
    for _ in range(starts):
        build = claim + 3.0
        second = phase + 15 * math.ceil((build - phase) / 15) + 15
        read = next(request for request in requests if newest(request) >= second)
        gaps.append(read + 1.5 - claim)
        claim = read + 1.5
    return statistics.mean(gaps[2:])


def test_print_measured_replay():
    print("\nregion     schedule      mean   worst  (30 phases x leads 2/5/8s x 2 seeds)")
    for region in READABLE_BY_AGE:
        for label, interval, margin in (("fixed 15s", 15.0, None), ("aligned 4s", 15.0, 4.0),
                                        ("aligned 8s", 15.0, 8.0), ("fixed 10s", 10.0, None)):
            results = [mean_gap(region, phase=phase / 2, lead=lead, margin=margin, interval=interval, seed=seed)
                       for phase in range(30) for lead in (2.0, 5.0, 8.0) for seed in (1, 2)]
            print(f"{region:10s} {label:12s} {statistics.mean(results):5.1f} {max(results):7.1f}")
