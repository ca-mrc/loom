"""loom_worker.sandbox_network — bridge create/connect/teardown +
allocator (#78 Phase A)."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import pytest

from loom_worker.sandbox_network import (
    SandboxBridge,
    SandboxNetworkAllocator,
    SandboxNetworkError,
    connect_sandbox_to_singleton,
    create_sandbox_bridge,
    teardown_sandbox_bridge,
)

_TRIAL = UUID("00000000-0000-0000-0000-000000000001")


class FakeDockerRunner:
    """Records argv calls and serves pre-recorded responses. Tuple
    `responses` is consumed FIFO; each entry is either a stdout str or
    an Exception to raise. Unmatched (out-of-responses) calls raise."""

    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[list[str]] = []

    async def __call__(self, argv: Sequence[str]) -> str:
        self.calls.append(list(argv))
        if not self._responses:
            raise AssertionError(
                f"unexpected docker call: {argv}",
            )
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


# ─── allocator ────────────────────────────────────────────────────────


def test_allocator_acquires_lowest_free_index() -> None:
    a = SandboxNetworkAllocator()
    i1, s1 = a.acquire()
    i2, s2 = a.acquire()
    assert (i1, s1) == (1, "10.42.1.0/24")
    assert (i2, s2) == (2, "10.42.2.0/24")
    assert a.in_use_count == 2


def test_allocator_release_returns_to_pool() -> None:
    a = SandboxNetworkAllocator()
    i, _ = a.acquire()
    a.release(i)
    # Re-acquired the same index.
    again_i, _ = a.acquire()
    assert again_i == i


def test_allocator_release_is_idempotent() -> None:
    a = SandboxNetworkAllocator()
    i, _ = a.acquire()
    a.release(i)
    a.release(i)  # no error
    a.release(999)  # never acquired
    assert a.in_use_count == 0


def test_allocator_exhaustion_raises() -> None:
    a = SandboxNetworkAllocator()
    for _ in range(254):
        a.acquire()
    with pytest.raises(SandboxNetworkError, match="exhausted"):
        a.acquire()


def test_worker_index_partitions_second_octet() -> None:
    # Two allocators with distinct worker_index must produce CIDRs
    # in disjoint /16s so multi-worker hosts can coexist.
    a0 = SandboxNetworkAllocator(worker_index=0)
    a3 = SandboxNetworkAllocator(worker_index=3)
    _, s0 = a0.acquire()
    _, s3 = a3.acquire()
    assert s0 == "10.42.1.0/24"
    assert s3 == "10.45.1.0/24"


def test_worker_index_out_of_range_rejects() -> None:
    with pytest.raises(SandboxNetworkError, match="worker_index must be"):
        SandboxNetworkAllocator(worker_index=-1)
    with pytest.raises(SandboxNetworkError, match="worker_index must be"):
        SandboxNetworkAllocator(worker_index=16)


def test_worker_index_15_at_upper_bound_ok() -> None:
    a = SandboxNetworkAllocator(worker_index=15)
    _, s = a.acquire()
    assert s == "10.57.1.0/24"  # 42 + 15


# ─── create_sandbox_bridge ────────────────────────────────────────────


async def test_create_invokes_correct_docker_argv() -> None:
    runner = FakeDockerRunner(["net-abc123\n"])
    a = SandboxNetworkAllocator()
    b = await create_sandbox_bridge(
        trial_id=_TRIAL, allocator=a, docker_runner=runner,
    )
    assert runner.calls == [[
        "network", "create",
        "--internal",
        "--subnet", "10.42.1.0/24",
        "loom-sandbox-00000000-0000-0000-0000-000000000001",
    ]]
    assert b == SandboxBridge(
        name="loom-sandbox-00000000-0000-0000-0000-000000000001",
        subnet="10.42.1.0/24",
        network_id="net-abc123",
        subnet_index=1,
    )


async def test_create_retries_on_cidr_overlap() -> None:
    # First attempt collides; allocator releases the index, but since
    # the released index is the smallest free, the retry re-acquires
    # it. To force a fresh subnet on retry, pre-reserve .1 so .2 wins.
    runner = FakeDockerRunner([
        SandboxNetworkError(
            "docker network create exited rc=1: "
            "Pool overlaps with other one on this address space",
        ),
        "net-second\n",
    ])
    a = SandboxNetworkAllocator()
    a.acquire()  # holds index 1 — retry must pick 2.
    b = await create_sandbox_bridge(
        trial_id=_TRIAL, allocator=a, docker_runner=runner,
    )
    assert len(runner.calls) == 2
    # First call tried index 2; retry tried index 3 (collision freed 2).
    assert runner.calls[0][4] == "10.42.2.0/24"
    assert runner.calls[1][4] == "10.42.2.0/24"
    assert b.subnet == "10.42.2.0/24"
    assert b.subnet_index == 2
    # Pre-reserved + succeeded index both held.
    assert a.in_use_count == 2


async def test_create_non_recoverable_error_propagates() -> None:
    runner = FakeDockerRunner([
        SandboxNetworkError(
            "docker network create exited rc=125: "
            "Cannot connect to the Docker daemon",
        ),
    ])
    a = SandboxNetworkAllocator()
    with pytest.raises(SandboxNetworkError, match="Cannot connect"):
        await create_sandbox_bridge(
            trial_id=_TRIAL, allocator=a, docker_runner=runner,
        )
    # Allocator released the index even though create failed.
    assert a.in_use_count == 0


@pytest.mark.parametrize("created", [True, False])
async def test_cancelled_create_drains_before_releasing_its_subnet(created: bool) -> None:
    allocator = SandboxNetworkAllocator()
    entered, finish = asyncio.Event(), asyncio.Event()
    removing, removed = asyncio.Event(), asyncio.Event()
    calls: list[list[str]] = []

    async def docker(argv: Sequence[str]) -> str:
        calls.append(list(argv))
        if argv[:2] == ["network", "create"]:
            entered.set()
            await finish.wait()
            if not created:
                raise SandboxNetworkError("network name already exists")
            return "exact-created-id\n"
        assert argv == ["network", "rm", "exact-created-id"]
        assert allocator.in_use_count == 1
        removing.set()
        await removed.wait()
        return "exact-created-id"

    task = asyncio.create_task(create_sandbox_bridge(
        trial_id=_TRIAL, allocator=allocator, docker_runner=docker,
    ))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert allocator.in_use_count == 1
        finish.set()
        if created:
            await asyncio.wait_for(removing.wait(), 1)
            task.cancel()  # A repeated cancellation must not abandon removal.
            await asyncio.sleep(0)
            assert not task.done()
            removed.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert allocator.in_use_count == 0
        assert len(calls) == (2 if created else 1)
    finally:
        finish.set()
        removed.set()
        await asyncio.gather(task, return_exceptions=True)
        allocator.release(1)  # Only this test's fake slot can remain on RED.


async def test_create_gives_up_after_max_retries() -> None:
    # 5 CIDR-overlap errors → SandboxNetworkError after _MAX_CREATE_RETRIES.
    overlap = SandboxNetworkError(
        "docker network create exited rc=1: "
        "Pool overlaps with other one on this address space",
    )
    runner = FakeDockerRunner([overlap] * 5)
    a = SandboxNetworkAllocator()
    with pytest.raises(SandboxNetworkError, match="after 5"):
        await create_sandbox_bridge(
            trial_id=_TRIAL, allocator=a, docker_runner=runner,
        )
    # Every acquire was released.
    assert a.in_use_count == 0


# ─── connect_sandbox_to_singleton ─────────────────────────────────────


async def test_connect_invokes_correct_argv() -> None:
    runner = FakeDockerRunner([""])
    bridge = SandboxBridge(
        name="loom-sandbox-xyz", subnet="10.42.5.0/24",
        network_id="net", subnet_index=5,
    )
    await connect_sandbox_to_singleton(
        bridge=bridge, singleton_container="loom-llm-gateway-sandbox",
        docker_runner=runner,
    )
    assert runner.calls == [[
        "network", "connect",
        "loom-sandbox-xyz", "loom-llm-gateway-sandbox",
    ]]


async def test_connect_propagates_docker_error() -> None:
    runner = FakeDockerRunner([
        SandboxNetworkError("endpoint with name ... already exists"),
    ])
    bridge = SandboxBridge(
        name="loom-sandbox-xyz", subnet="10.42.5.0/24",
        network_id="net", subnet_index=5,
    )
    with pytest.raises(SandboxNetworkError, match="already exists"):
        await connect_sandbox_to_singleton(
            bridge=bridge, singleton_container="singleton",
            docker_runner=runner,
        )


# ─── teardown_sandbox_bridge ──────────────────────────────────────────


async def test_teardown_invokes_rm_and_releases_subnet() -> None:
    runner = FakeDockerRunner([""])
    a = SandboxNetworkAllocator()
    i, _ = a.acquire()  # simulate prior create()
    bridge = SandboxBridge(
        name="loom-sandbox-xyz", subnet="10.42.1.0/24",
        network_id="net", subnet_index=i,
    )
    await teardown_sandbox_bridge(
        bridge=bridge, allocator=a, docker_runner=runner,
    )
    assert runner.calls == [["network", "rm", "loom-sandbox-xyz"]]
    assert a.in_use_count == 0


async def test_teardown_releases_subnet_even_if_rm_fails() -> None:
    runner = FakeDockerRunner([
        SandboxNetworkError("network has active endpoints"),
    ])
    a = SandboxNetworkAllocator()
    i, _ = a.acquire()
    bridge = SandboxBridge(
        name="loom-sandbox-xyz", subnet="10.42.1.0/24",
        network_id="net", subnet_index=i,
    )
    with pytest.raises(SandboxNetworkError, match="active endpoints"):
        await teardown_sandbox_bridge(
            bridge=bridge, allocator=a, docker_runner=runner,
        )
    # Subnet released even though docker rm failed — the index would
    # otherwise leak across worker uptime.
    assert a.in_use_count == 0
