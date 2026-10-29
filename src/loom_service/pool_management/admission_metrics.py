"""Low-cardinality admission work measurements; no identities or workload data."""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field

from prometheus_client import Counter as MetricCounter
from prometheus_client import Histogram

_ADMISSION_SECONDS = Histogram("loom_pool_admission_seconds",
    "Management admission time at the mutation boundary (commit excluded).", ("phase",),
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 120))
_ADMISSION_WORK = MetricCounter("loom_pool_admission_work_total",
    "Registry operations performed during management preparation.", ("operation",))


@dataclass
class AdmissionWork:
    operations: Counter[str] = field(default_factory=Counter)
    lock_wait_seconds: float | None = None
    locked_at: float | None = None

    def observe(self) -> None:
        if self.lock_wait_seconds is not None:
            _ADMISSION_SECONDS.labels("lock_wait").observe(self.lock_wait_seconds)
        if self.locked_at is not None:
            _ADMISSION_SECONDS.labels("locked_prepare").observe(time.monotonic() - self.locked_at)
        for operation, count in self.operations.items():
            _ADMISSION_WORK.labels(operation).inc(count)
