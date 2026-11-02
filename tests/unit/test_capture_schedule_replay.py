"""Deterministic replay of single-node feedback pacing under both capture schedules (#2750).

Model, in seconds of wall time (monotonic equals wall here):
- provider points every `step` at `phase`, each readable `ingest` after its timestamp;
- a minutely job starts `job_start` into the minute and stops at min(start + 50, :55);
- a capture issues its PromQL request `lead` after it starts and publishes 0.5s later;
- the actuator claims 1.0s after publication and the build phase starts 3.0s later;
- a start is observed by the first capture whose request reads a window that begins
  at or after the build start, i.e. sees the second point after it.

The 2026-11-02 probe calibrates it: points at 6 mod 15 (eu-north1) and 13 mod 15
(eu-west1), captures reading at about 7 mod 15, gave median gaps of 41.4s and 29.4s.
"""

from __future__ import annotations

import math
import statistics

import pytest

from loom_execution_capacity_collector.capture_schedule import (
    CaptureSchedule,
    CaptureTiming,
    NodeIORead,
)

STEP = 15.0
PUBLISH = 0.5
CLAIM = 1.0
BUILD_START = 3.0
LEAD_JITTER = (0.0, 0.7, -0.4, 1.2)


def captures(*, phase, ingest, lead, margin, job_start=2.0, minutes=60):
    """Return (request, publish) times for every capture over `minutes` minutes."""
    result = []
    index = 0
    for minute in range(minutes):
        base = 60.0 * minute
        start = base + job_start
        deadline = min(start + 50.0, base + 55.0)
        schedule = CaptureSchedule(interval=15.0, margin=margin or 0.0)
        while True:
            request = start + max(0.1, lead + LEAD_JITTER[index % len(LEAD_JITTER)])
            index += 1
            newest = phase + STEP * math.floor((request - ingest - phase) / STEP)
            timing = CaptureTiming(started_monotonic=start, started_wall=start)
            if margin is not None:
                timing.read = NodeIORead(requested_wall=request, series=(("n1", newest, STEP),))
            result.append((request, request + PUBLISH))
            nxt = schedule.next_start(previous=timing, now=request + PUBLISH, deadline=deadline)
            if nxt is None:
                break
            start = nxt
    return result


def gaps(*, phase, ingest, lead, margin, job_start=2.0, starts=40):
    capture_list = captures(phase=phase, ingest=ingest, lead=lead, margin=margin, job_start=job_start)
    claim, result = 1.0, []
    for _ in range(starts):
        build = claim + BUILD_START
        first = phase + STEP * math.ceil((build - phase) / STEP)
        readable = first + STEP + ingest
        publish = next(published for request, published in capture_list if request >= readable)
        nxt = publish + CLAIM
        result.append(nxt - claim)
        claim = nxt
    return result


def per_minute(*, phase, ingest, lead, margin):
    return len(captures(phase=phase, ingest=ingest, lead=lead, margin=margin, minutes=10)) / 10


def mean_gap(**kwargs):
    # The first two starts include the run's first unaligned capture.
    return statistics.mean(gaps(**kwargs)[2:])


PHASES = [x / 2 for x in range(30)]


def test_replay_reproduces_the_probe_with_the_fixed_schedule():
    # Probe medians: 41.4s on eu-north1 (points at 6 mod 15), 29.4s on eu-west1 (13 mod 15).
    assert 40 <= statistics.median(gaps(phase=6, ingest=3, lead=5, margin=None)) <= 46
    assert 28 <= statistics.median(gaps(phase=13, ingest=3, lead=5, margin=None)) <= 32


def test_aligned_schedule_brings_the_probe_north_phase_to_two_steps():
    assert mean_gap(phase=6, ingest=3, lead=5, margin=None) == pytest.approx(45, abs=1)
    assert mean_gap(phase=6, ingest=3, lead=5, margin=4) == pytest.approx(30, abs=1)
    assert mean_gap(phase=13, ingest=3, lead=5, margin=4) == pytest.approx(30, abs=1)


@pytest.mark.parametrize("ingest", [1.0, 2.0, 3.0, 4.0])
@pytest.mark.parametrize("lead", [2.0, 5.0, 8.0])
def test_aligned_schedule_is_never_slower_and_keeps_the_capture_count(ingest, lead):
    for phase in PHASES:
        fixed = mean_gap(phase=phase, ingest=ingest, lead=lead, margin=None)
        aligned = mean_gap(phase=phase, ingest=ingest, lead=lead, margin=4)
        assert aligned <= fixed + 0.5, (phase, fixed, aligned)
        assert per_minute(phase=phase, ingest=ingest, lead=lead, margin=4) == per_minute(
            phase=phase, ingest=ingest, lead=lead, margin=None)


def test_ingestion_slower_than_the_margin_loses_at_most_one_step():
    for phase in PHASES:
        fixed = mean_gap(phase=phase, ingest=6, lead=5, margin=None)
        aligned = mean_gap(phase=phase, ingest=6, lead=5, margin=4)
        assert aligned <= max(fixed, 3 * STEP) + 0.5, (phase, fixed, aligned)
        assert per_minute(phase=phase, ingest=6, lead=5, margin=4) == 4


def test_late_cronjob_start_keeps_the_capture_count():
    for phase in PHASES:
        fixed = mean_gap(phase=phase, ingest=3, lead=5, margin=None, job_start=9)
        aligned = mean_gap(phase=phase, ingest=3, lead=5, margin=4, job_start=9)
        assert aligned <= fixed + 0.5, (phase, fixed, aligned)
        assert per_minute(phase=phase, ingest=3, lead=5, margin=4) == per_minute(
            phase=phase, ingest=3, lead=5, margin=None)


def test_print_replay_table():
    print("\nphase  fixed_mean  aligned_mean  (ingest 3s, lead 5s, 4 captures/min both)")
    for phase in range(15):
        fixed = mean_gap(phase=phase, ingest=3, lead=5, margin=None)
        print(f"{phase:5d} {fixed:11.1f} {mean_gap(phase=phase, ingest=3, lead=5, margin=4):13.1f}")
