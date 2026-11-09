"""Per-request time split, returned to the caller as a `Server-Timing` header.

The Prometheus histograms in `metrics.py` aggregate across requests, and the
service's metrics aren't reachable without cluster access. This header gives
the split for one request to anyone who can make that request, e.g. a slow
Batch detail read on a deployment (#2771):

    Server-Timing: total;dur=912.4, db;dur=703.1;desc="18 statements",
                   auth;dur=21.0, checkout;dur=1.2

- `db`: every SQL statement this request ran, from send until the driver has
  received all result rows, so it includes moving rows to the pod. Python
  work is roughly `total - db`.
- `auth`: authentication, including the connection checkout and the session
  touch commit (its statements are in `db` too).
- `checkout`: waiting for a pooled connection.

Only counts and durations are reported, never SQL or parameters. Statements
run outside a request (background loops) are not attributed to any request.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass

SERVER_TIMING_HEADER = "Server-Timing"


@dataclass
class RequestTiming:
    db_sec: float = 0.0
    statements: int = 0
    auth_sec: float | None = None
    checkout_sec: float | None = None

    def header_value(self, total_sec: float) -> str:
        parts = [
            f"total;dur={_ms(total_sec)}",
            f'db;dur={_ms(self.db_sec)};desc="{self.statements} statements"',
        ]
        if self.auth_sec is not None:
            parts.append(f"auth;dur={_ms(self.auth_sec)}")
        if self.checkout_sec is not None:
            parts.append(f"checkout;dur={_ms(self.checkout_sec)}")
        return ", ".join(parts)


_current: ContextVar[RequestTiming | None] = ContextVar("loom_request_timing", default=None)


def start_request_timing() -> RequestTiming:
    """Begin collecting for the current request.

    The object is mutated in place, so child tasks and SQLAlchemy's greenlets,
    which copy the context, add to the same totals.
    """
    timing = RequestTiming()
    _current.set(timing)
    return timing


def record_statement(seconds: float) -> None:
    timing = _current.get()
    if timing is not None:
        timing.db_sec += seconds
        timing.statements += 1


def record_auth(seconds: float) -> None:
    timing = _current.get()
    if timing is not None:
        timing.auth_sec = seconds


def record_checkout(seconds: float) -> None:
    timing = _current.get()
    if timing is not None:
        timing.checkout_sec = seconds


def _ms(seconds: float) -> str:
    return f"{seconds * 1000:.1f}"


__all__ = [
    "SERVER_TIMING_HEADER",
    "RequestTiming",
    "record_auth",
    "record_checkout",
    "record_statement",
    "start_request_timing",
]
