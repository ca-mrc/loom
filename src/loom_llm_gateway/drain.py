"""Graceful drain support for rolling upgrades + HPA scale-down (#547).

Kubernetes gives a pod `terminationGracePeriodSeconds` (bumped to 300s
via the llm-gateway.yaml.j2 template) between SIGTERM and SIGKILL. The
`preStop` hook calls `POST /drain`, which:

1. Flips `app.state.draining = True` so the `/healthz` readinessProbe
   starts returning 503 → the k8s Service load balancer removes this
   pod from the routing pool, so no NEW requests arrive
2. Polls the in-flight request counter until it hits zero
3. Returns 200 so the pod's `preStop` hook completes cleanly and the
   normal FastAPI lifespan teardown can run

The middleware tracks in-flight via a plain `asyncio.Lock`-guarded
integer. LLM calls can run for tens of seconds (reasoning models
minutes); accurate counting matters more than nanosecond overhead.

A drain that runs longer than `LOOM_GW_DRAIN_TIMEOUT_SEC` (default
270s, ~30s under the 300s grace period) returns 503 with a diagnostic
so operators can see when the grace period was insufficient. In that
case kubelet's SIGKILL still fires normally; the drain gave in-flight
requests every chance to finish.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send


@dataclass
class DrainState:
    """Per-app drain and in-flight tracking state.

    Attached to `app.state.drain` during lifespan startup so both the
    middleware, the `/healthz` probe, and the `/drain` endpoint share
    one instance. Never instantiated more than once per app.
    """

    in_flight: int = 0
    draining: bool = False
    _lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def enter(self) -> None:
        async with self._get_lock():
            self.in_flight += 1

    async def leave(self) -> None:
        async with self._get_lock():
            self.in_flight -= 1

    async def snapshot(self) -> tuple[int, bool]:
        async with self._get_lock():
            return self.in_flight, self.draining

    async def begin_drain(self) -> None:
        async with self._get_lock():
            self.draining = True

    async def wait_for_zero_in_flight(
        self,
        *,
        timeout_sec: float,
        poll_interval_sec: float = 0.5,
    ) -> tuple[bool, int, float]:
        """Poll until `in_flight` reaches zero or `timeout_sec` expires.

        Returns `(ok, remaining_in_flight, elapsed_sec)` — `ok` is True
        iff the counter reached zero within the timeout.
        """
        started = time.monotonic()
        deadline = started + timeout_sec
        while True:
            in_flight, _ = await self.snapshot()
            if in_flight == 0:
                return True, 0, time.monotonic() - started
            now = time.monotonic()
            if now >= deadline:
                return False, in_flight, now - started
            await asyncio.sleep(
                min(poll_interval_sec, max(0.0, deadline - now)),
            )


def ensure_drain_state(app: FastAPI) -> DrainState:
    """Return the app-wide drain state, creating it for no-lifespan tests.

    Runtime startup installs this during lifespan before serving traffic. Some
    ASGI test harnesses bypass lifespan and seed only the dependencies a route
    needs, so the request path must still fail closed instead of crashing with a
    missing ``app.state.drain``.
    """
    drain_state = getattr(app.state, "drain", None)
    if isinstance(drain_state, DrainState):
        return drain_state
    drain_state = DrainState()
    app.state.drain = drain_state
    return drain_state


class DrainMiddleware:
    """Count the entire ASGI response and cleanup, including streaming bodies."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") in {
            "/healthz", "/drain", "/metrics",
        }:
            await self.app(scope, receive, send)
            return
        drain_state = ensure_drain_state(scope["app"])
        await drain_state.enter()
        try:
            await self.app(scope, receive, send)
        finally:
            await drain_state.leave()


def install_drain_middleware(app: FastAPI) -> None:
    # BaseHTTPMiddleware returns after response headers and also introduces a
    # cancellation scope around the route's durable cleanup. Pure ASGI keeps
    # both accounting and the full response lifetime inside the drain count.
    app.add_middleware(DrainMiddleware)


async def drain_and_report(
    drain_state: DrainState,
    *,
    timeout_sec: float,
) -> dict[str, Any]:
    """Begin drain, wait for in-flight to reach zero, return report dict."""
    await drain_state.begin_drain()
    ok, remaining, elapsed = await drain_state.wait_for_zero_in_flight(
        timeout_sec=timeout_sec,
    )
    return {
        "status": "drained" if ok else "timeout",
        "remaining_in_flight": remaining,
        "elapsed_sec": round(elapsed, 3),
        "timeout_sec": timeout_sec,
    }
