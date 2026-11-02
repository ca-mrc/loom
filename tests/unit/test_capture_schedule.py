"""Deterministic cases for provider-aligned capture scheduling (#2750)."""

from __future__ import annotations

import itertools
import json
import math
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from loom_execution_capacity_collector import __main__ as entry
from loom_execution_capacity_collector.capture_schedule import (
    CaptureSchedule,
    CaptureTiming,
    NodeIORead,
    record_node_io_read,
)
from loom_execution_capacity_collector.collector import _with_node_io
from loom_execution_capacity_collector.contracts import NodeIOWindowPressure

# Wall clock = monotonic + 1000 unless a test moves it.
OFFSET = 1000.0


def visible(phase: float, request_wall: float, ingest: float = 3.0) -> float:
    """Newest point readable at `request_wall` for points at `phase` mod 15 (wall seconds)."""
    return phase + 15 * math.floor((request_wall - ingest - phase) / 15)


def timing(start: float, *, lead: float = 5.0, series=(("n1", 990.0, 15.0),), offset: float = OFFSET,
           read: bool = True) -> CaptureTiming:
    """A capture that started at monotonic `start` and requested I/O `lead` seconds later."""
    result = CaptureTiming(started_monotonic=start, started_wall=start + offset)
    if read:
        result.read = NodeIORead(requested_wall=start + offset + lead, series=tuple(series))
    return result


def test_without_a_read_the_fixed_schedule_is_unchanged():
    schedule = CaptureSchedule(interval=15, margin=4)
    assert schedule.next_start(previous=timing(0, read=False), now=6, deadline=50) == 15
    # Elapsed slots are skipped, never caught up in a burst.
    assert schedule.next_start(previous=timing(0, read=False), now=31, deadline=50) == 45
    assert schedule.next_start(previous=timing(45, read=False), now=51, deadline=50) is None


def test_single_node_reads_its_next_point_margin_seconds_after_the_timestamp():
    schedule = CaptureSchedule(interval=15, margin=4)
    # Newest point at wall 1006 (= monotonic 6), step 15: point 21 is read at 25,
    # so the capture starts at 25 - 5s lead.
    start = schedule.next_start(previous=timing(0, series=[("n1", 1006.0, 15.0)]), now=6, deadline=60)
    assert start == pytest.approx(20.0)
    # An aligned start may come up to 5s before the fixed 15 (point 13 read at 17)
    # when that leaves the same number of captures: 12, 27, 42 against 15, 30, 45.
    start = CaptureSchedule(interval=15, margin=4).next_start(
        previous=timing(0, series=[("n1", 998.0, 15.0)]), now=6, deadline=52)
    assert start == pytest.approx(12.0)
    # Where it would add a capture before the deadline, the next point is used.
    start = CaptureSchedule(interval=15, margin=4).next_start(
        previous=timing(0, series=[("n1", 998.0, 15.0)]), now=6, deadline=60)
    assert start == pytest.approx(27.0)


def test_alignment_never_starts_before_the_minimum_spacing_or_after_one_interval():
    for phase in [x / 4 for x in range(60)]:
        schedule = CaptureSchedule(interval=15, margin=4)
        start = schedule.next_start(previous=timing(0, series=[("n1", 990.0 + phase, 15.0)]), now=6, deadline=60)
        assert 10 <= start < 30


@pytest.mark.parametrize("other,phases", [(997.0, [1, 4]), (1003.0, [4, 7])])
def test_differing_node_phases_take_turns_when_both_keep_the_budget(other, phases):
    schedule = CaptureSchedule(interval=15, margin=4)
    starts, start = [], 0.0
    while True:
        request = start + OFFSET + 5
        series = [("a", visible(1000.0, request), 15.0), ("b", visible(other, request), 15.0)]
        if not (start := schedule.next_start(previous=timing(start, series=series), now=start + 6, deadline=200)):
            break
        starts.append(start)
    # With a 5s lead, the reads alternate between the two nodes' next points.
    assert [round((value + 5) % 15) for value in starts[:4]] == phases * 2
    assert len(starts) + 1 == 14  # The fixed schedule's 0, 15, ..., 195.


def test_a_node_whose_phase_would_cost_a_capture_is_read_on_the_fixed_slots():
    schedule = CaptureSchedule(interval=15, margin=4)
    starts, start = [], 0.0
    while True:
        request = start + OFFSET + 5
        series = [("a", visible(1000.0, request), 15.0), ("b", visible(992.0, request), 15.0)]
        if not (start := schedule.next_start(previous=timing(start, series=series), now=start + 6, deadline=200)):
            break
        starts.append(start)
    assert {round((value + 5) % 15) for value in starts} == {4}
    assert len(starts) + 1 == 14


@pytest.mark.parametrize("series", [
    [],
    [("n1", 1006.0, 5.0)],          # Step below the decodable window.
    [("n1", 1006.0, 90.0)],         # Step above it.
    [("n1", math.nan, 15.0)],
    [("n1", 1006.0, math.inf)],
    [("n1", 1020.0, 15.0)],         # A point after the request: clock skew.
])
def test_no_usable_phase_falls_back_to_the_fixed_schedule(series):
    schedule = CaptureSchedule(interval=15, margin=4)
    assert schedule.next_start(previous=timing(0, series=series), now=6, deadline=50) == 15


def test_pre_request_time_uses_the_fastest_lead_seen_in_the_run():
    schedule = CaptureSchedule(interval=15, margin=4)
    # Point 21 is read at 25: an 8s lead starts the capture at 17.
    slow = schedule.next_start(previous=timing(0, lead=8, series=[("n1", 1006.0, 15.0)]), now=9, deadline=90)
    assert slow == pytest.approx(17)
    # The read at 19 still saw point 6. A 2s lead lowers the estimate: point 36
    # is read at 40 by starting at 38.
    fast = schedule.next_start(previous=timing(slow, lead=2, series=[("n1", 1006.0, 15.0)]), now=slow + 3,
                               deadline=90)
    assert fast == pytest.approx(38)
    # A slower capture later keeps the 2s estimate: it reads late, never early.
    later = schedule.next_start(previous=timing(fast, lead=8, series=[("n1", 1036.0, 15.0)]), now=fast + 9,
                                deadline=90)
    assert later == pytest.approx(53)


def test_lead_longer_than_the_interval_is_not_used():
    schedule = CaptureSchedule(interval=15, margin=4)
    assert schedule.next_start(previous=timing(0, lead=16, series=[("n1", 1006.0, 15.0)]), now=17,
                               deadline=50) == 30


def test_late_start_and_elapsed_slots_still_align_after_the_floor():
    schedule = CaptureSchedule(interval=15, margin=4)
    # The capture ran until 33: slots 15 and 30 have elapsed, 45 is the floor.
    start = schedule.next_start(previous=timing(0, series=[("n1", 1006.0, 15.0)]), now=33, deadline=70)
    # Point 36 read at 40 would allow 35, 50, 65: one more than the fixed 45, 60.
    # Point 51 read at 55 keeps two captures.
    assert start == pytest.approx(50.0)


def test_an_on_time_read_that_missed_its_point_waits_a_step_and_widens_the_margin():
    schedule = CaptureSchedule(interval=15, margin=4)
    first = schedule.next_start(previous=timing(0, series=[("n1", 1006.0, 15.0)]), now=6, deadline=60)
    assert first == pytest.approx(20)  # Point 21 planned to be read at 25.
    # Ingestion was later than 4s: the on-time read at 25 still saw point 6. There is
    # no retry; point 36 is read with a 6s margin at 42.
    second = schedule.next_start(previous=timing(first, series=[("n1", 1006.0, 15.0)]), now=first + 6,
                                 deadline=60)
    assert second == pytest.approx(37)
    assert schedule.margin == 6


def test_deadline_prefers_the_fixed_start_and_never_extends_the_run():
    schedule = CaptureSchedule(interval=15, margin=4)
    # Point 49 would be read at 53 by starting at 48, past a 47s deadline.
    assert schedule.next_start(previous=timing(30, series=[("n1", 1034.0, 15.0)]), now=36,
                               deadline=47) == 45
    # An earlier aligned start inside the deadline is kept: point 44 at 47.
    assert CaptureSchedule(interval=15, margin=4).next_start(
        previous=timing(30, series=[("n1", 1029.0, 15.0)]), now=36, deadline=47) == pytest.approx(43)
    assert schedule.next_start(previous=timing(40, series=[("n1", 1029.0, 15.0)]), now=46, deadline=54) is None


@pytest.mark.parametrize("phase", [x / 2 for x in range(30)])
@pytest.mark.parametrize("lead", [0.5, 5.0, 9.0])
def test_a_run_makes_exactly_the_fixed_schedules_captures(phase, lead):
    schedule = CaptureSchedule(interval=15, margin=4)
    starts, start = [0.0], 0.0
    while True:
        # The newest point visible at the request: points every 15s at `phase`, ingested in 3s.
        point = phase + 15 * math.floor((start + lead - 3 - phase) / 15)
        previous = timing(start, lead=lead, series=[("n1", OFFSET + point, 15.0)])
        nxt = schedule.next_start(previous=previous, now=start + lead + 0.5, deadline=50)
        if nxt is None:
            break
        starts.append(start := nxt)
    assert len(starts) == len([0.0, 15.0, 30.0, 45.0])
    assert all(b - a >= 10 for a, b in itertools.pairwise(starts))


def test_a_stepped_wall_clock_stops_alignment_for_the_rest_of_the_run():
    schedule = CaptureSchedule(interval=15, margin=4)
    series = [("n1", 1006.0, 15.0)]
    assert schedule.next_start(previous=timing(0, series=series), now=6, deadline=60) != 15
    stepped = timing(20, series=[("n1", 1021.0 + 30, 15.0)], offset=OFFSET + 30)
    assert schedule.next_start(previous=stepped, now=26, deadline=60) == 35
    back = timing(35, series=[("n1", 1036.0, 15.0)])
    assert schedule.next_start(previous=back, now=41, deadline=60) == 50


@pytest.mark.asyncio
async def test_node_io_read_records_request_time_and_series_for_the_current_capture(monkeypatch):
    from loom_execution_capacity_collector import capture_schedule

    end = datetime(2026, 11, 2, 15, 0, 21, tzinfo=UTC)
    window = NodeIOWindowPressure(window_started_at=end - timedelta(seconds=15), sampled_at=end,
                                  full_window_percent=3.0)
    provider = SimpleNamespace(capture_node_io=lambda nodes: _windows({"uid-1": window, "uid-2": object()}))
    snapshot = SimpleNamespace(nodes=[SimpleNamespace(uid="uid-1"), SimpleNamespace(uid="uid-2")],
                               model_copy=lambda update: SimpleNamespace(**update))
    for node in snapshot.nodes:
        node.model_copy = lambda update, node=node: SimpleNamespace(uid=node.uid, **update)
    monkeypatch.setattr("loom_execution_capacity_collector.collector.time.time", lambda: 1_762_095_625.0)
    current = CaptureTiming(started_monotonic=0, started_wall=1_762_095_620.0)
    token = capture_schedule.CURRENT_CAPTURE.set(current)
    try:
        await _with_node_io(provider, snapshot)
    finally:
        capture_schedule.CURRENT_CAPTURE.reset(token)
    assert current.read == NodeIORead(requested_wall=1_762_095_625.0, series=(("uid-1", end.timestamp(), 15.0),))
    # Outside a capture the read is not retained anywhere.
    record_node_io_read(NodeIORead(requested_wall=0, series=()))


async def _windows(value):
    return value


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def now(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.value += seconds


@pytest.mark.asyncio
@pytest.mark.parametrize("margin,expected", [(None, [0, 15, 30, 45]), ("4", [0, 14, 29, 44])])
async def test_collector_run_aligns_only_when_a_margin_is_configured(monkeypatch, capsys, margin, expected):
    clock = Clock()
    calls: list[float] = []

    async def capture(_settings: object) -> SimpleNamespace:
        calls.append(clock.now())
        clock.value += 5
        # Points every 15s at monotonic 0 mod 15, ingested 3s later. Each read
        # comes 5s after its capture starts, so reads land at 19, 34 and 49.
        point = 15 * math.floor((clock.now() - 3) / 15)
        record_node_io_read(NodeIORead(requested_wall=clock.now() + OFFSET, series=(("n1", point + OFFSET, 15.0),)))
        clock.value += 1
        return SimpleNamespace(model_dump=lambda **_kw: {"id": str(len(calls))})

    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_COLLECTION_MODE", "target")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_WINDOW_SECONDS", "50")
    monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_INTERVAL_SECONDS", "15")
    if margin is None:
        monkeypatch.delenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_ALIGNMENT_MARGIN_SECONDS", raising=False)
    else:
        monkeypatch.setenv("LOOM_EXECUTION_CAPACITY_COLLECTOR_SAMPLE_ALIGNMENT_MARGIN_SECONDS", margin)
    monkeypatch.setattr(entry, "monotonic", clock.now, raising=False)
    monkeypatch.setattr(entry, "time", lambda: clock.now() + OFFSET)
    monkeypatch.setattr(entry.asyncio, "sleep", clock.sleep)
    monkeypatch.setattr(entry, "ExecutionCapacityCollectorSettings", object)
    monkeypatch.setattr(entry, "collect_capacity_observation", capture)
    await entry._run()
    assert calls == pytest.approx(expected)
    assert json.loads(capsys.readouterr().out) == {"id": str(len(calls))}
