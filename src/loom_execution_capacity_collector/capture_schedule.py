"""Align collector captures with provider I/O points inside the minutely window (#2750).

A start counts as observed only once a provider window begins at or after its
build phase starts, so feedback waits for the next point plus one provider step.
A fixed 15s clock can sit just before each point's ingestion and read it one step
late. This schedule only moves capture starts inside the existing window: the
evidence rules, the minimum spacing and the run deadline are unchanged.
"""

from __future__ import annotations

import math
from contextvars import ContextVar
from dataclasses import dataclass, field

# Provider steps outside this range are not used for alignment; window decoding
# accepts 10-60s and nothing here relaxes it.
_MIN_STEP_SECONDS = 10.0
_MAX_STEP_SECONDS = 60.0
# Larger wall-clock movement relative to the monotonic clock stops alignment for
# the rest of the run rather than chasing a stepped clock.
_CLOCK_TOLERANCE_SECONDS = 1.0
# An aligned start may come up to this much before the interval, never closer
# than the configured interval minimum to the previous start.
_MAX_ADVANCE_SECONDS = 5.0
_MIN_SPACING_SECONDS = 10.0
# A read that missed the point it was timed for widens the margin for the rest
# of the run, so slow ingestion does not keep reading just before each point.
_LATE_MARGIN_STEP_SECONDS = 2.0
_MAX_MARGIN_SECONDS = 10.0


@dataclass(frozen=True)
class NodeIORead:
    """Timing of one PromQL read. Kept in-process; never published."""

    requested_wall: float  # Epoch seconds immediately before the provider request.
    # (node uid, newest point epoch seconds, step seconds) for each decoded window.
    series: tuple[tuple[str, float, float], ...]


@dataclass
class CaptureTiming:
    """Per-capture sink; the collector's I/O read fills `read` when it runs."""

    started_monotonic: float
    started_wall: float
    read: NodeIORead | None = None


CURRENT_CAPTURE: ContextVar[CaptureTiming | None] = ContextVar("capacity_capture_timing", default=None)


def record_node_io_read(read: NodeIORead) -> None:
    timing = CURRENT_CAPTURE.get()
    if timing is not None:
        timing.read = read


@dataclass
class CaptureSchedule:
    """Choose the next capture start (monotonic seconds) or stop the run.

    `interval` sets the run's call budget, as today: from any capture, an aligned
    run makes exactly as many further captures as the fixed schedule would,
    including after skipped elapsed slots. An aligned start is never before now
    or 10s after the previous start, nor more than 5s before the fixed one
    unless slots elapsed, and always before the fixed start plus one interval.
    """

    interval: float
    margin: float = 4.0
    _offset: float | None = None
    _lead: float | None = None
    _disabled: bool = False
    _captures: int = 0
    _served: dict[str, int] = field(default_factory=dict)
    _expected: tuple[str, float, float] | None = None  # (node, point, planned read time)

    def next_start(self, *, previous: CaptureTiming, now: float, deadline: float) -> float | None:
        floor = previous.started_monotonic + self.interval
        while floor < now:
            floor += self.interval
        if floor >= deadline:
            return None
        # The fixed schedule's remaining captures are the run's call budget.
        budget = math.ceil((deadline - floor) / self.interval)
        earliest = max(now, previous.started_monotonic + max(_MIN_SPACING_SECONDS, self.interval - _MAX_ADVANCE_SECONDS))
        aligned = self._aligned(previous, earliest, floor, deadline, budget)
        return floor if aligned is None else aligned

    def _aligned(self, previous: CaptureTiming, earliest: float, floor: float, deadline: float,
                 budget: int) -> float | None:
        offset = previous.started_wall - previous.started_monotonic
        if self._offset is None:
            self._offset = offset
        elif abs(offset - self._offset) > _CLOCK_TOLERANCE_SECONDS:
            self._disabled = True
        read = previous.read
        if self._disabled or read is None:
            return None
        lead = read.requested_wall - previous.started_wall
        if not 0 <= lead < self.interval:
            return None
        # The fastest pre-request phase seen in this run, so a fast capture does
        # not request before ingestion: arriving late costs seconds, early a step.
        self._lead = lead if self._lead is None else min(self._lead, lead)
        newest = {uid: point for uid, point, _ in read.series}
        if self._expected is not None:
            uid, point, planned = self._expected
            # Only an on-time read that still missed its point is slow ingestion; a
            # capture that ran early because its own setup was faster is not.
            if (uid in newest and newest[uid] < point - _CLOCK_TOLERANCE_SECONDS
                    and read.requested_wall >= planned - _CLOCK_TOLERANCE_SECONDS):
                self.margin = min(_MAX_MARGIN_SECONDS, self.margin + _LATE_MARGIN_STEP_SECONDS)
        self._expected = None
        candidates = []
        for uid, point, step in read.series:
            if (not math.isfinite(point) or not math.isfinite(step)
                    or not _MIN_STEP_SECONDS <= step <= _MAX_STEP_SECONDS
                    or point > read.requested_wall + _CLOCK_TOLERANCE_SECONDS):
                continue
            first = point + step + self.margin - self._offset - self._lead
            start = first + max(0, math.ceil((earliest - first) / step)) * step
            # Each start must leave exactly the fixed schedule's remaining
            # captures, so by induction a run makes exactly as many calls.
            while start < floor + self.interval:
                if math.ceil((deadline - start) / self.interval) == budget:
                    # The point this start is timed to read.
                    planned = start + self._lead + self._offset
                    candidates.append((self._served.get(uid, -1), start, uid, planned))
                    break
                start += step
        if not candidates:
            return None
        # Least recently served node first, then the earliest start, so differing
        # node phases take turns instead of one phase taking every capture.
        _, start, uid, planned = min(candidates)
        self._expected = (uid, planned - self.margin, planned)
        self._served[uid] = self._captures
        self._captures += 1
        return start


__all__ = ["CURRENT_CAPTURE", "CaptureSchedule", "CaptureTiming", "NodeIORead", "record_node_io_read"]
