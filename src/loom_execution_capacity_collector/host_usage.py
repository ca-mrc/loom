"""Attach optional host telemetry to an existing capacity inventory."""

import asyncio
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Literal

from loom.nebius_kubernetes import read_sdk_before_close
from loom_execution_actuator.resource_observations import read_resource_observation
from loom_execution_capacity_collector.contracts import (
    KubernetesCapacitySnapshot,
    NodeHostUsage,
    NodePlacement,
    PoolResourceMeasurement,
    PoolResourceUsage,
)

_MAX_SAMPLE_SECONDS = 5
_MAX_PARALLEL_READS = 8
_Resource = Literal["cpu", "memory", "disk"]


def aggregate_host_usage(samples: list[NodeHostUsage], ready_nodes: int) -> PoolResourceUsage:
    def metric(kind: _Resource) -> PoolResourceMeasurement | None:
        amounts = [(sample.sampled_at, amount) for sample in samples
                   if (amount := getattr(sample, kind)) is not None]
        if not amounts:
            return None
        if kind == "disk":
            # A pool average can hide the node closest to running out of space.
            # Return that node's actual measured capacity without exposing its identity.
            _, highest = max(amounts, key=lambda item: item[1].used / item[1].total)
            used, total = highest.used, highest.total
        else:
            used = sum(amount.used for _, amount in amounts)
            total = sum(amount.total for _, amount in amounts)
        return PoolResourceMeasurement(
            used=used, total=total, sampled_at=min(at for at, _ in amounts),
            covered_nodes=len(amounts), ready_nodes=ready_nodes,
        )

    return PoolResourceUsage(
        ready_nodes=ready_nodes, cpu=metric("cpu"), memory=metric("memory"), disk=metric("disk")
    )


async def capture_host_usage(
    core: Any, snapshot: KubernetesCapacitySnapshot, *, namespace: str
) -> PoolResourceUsage:
    deadline = monotonic() + _MAX_SAMPLE_SECONDS
    now = datetime.now(UTC)
    semaphore = asyncio.Semaphore(_MAX_PARALLEL_READS)
    samples: list[NodeHostUsage] = []

    async def capture(node: NodePlacement, name: str) -> None:
        async with semaphore:
            remaining = deadline - monotonic()
            if remaining <= 0:
                return
            try:
                raw = await read_sdk_before_close(lambda: read_resource_observation(
                    core, node_name=name, namespace=namespace,
                    execution_namespace=namespace.removesuffix("-observations"),
                    expected_node_uid=node.uid, timeout_seconds=min(2.0, remaining),
                    host_usage_only=True, now=now,
                ))
                sample = NodeHostUsage.model_validate(raw)
                if sample.sampled_at <= now:
                    samples.append(sample)
            except Exception:
                # Source errors affect only this node's telemetry, never capacity.
                return

    async with asyncio.TaskGroup() as tasks:
        for node in snapshot.nodes:
            if node.name and node.ready and not node.deleting:
                tasks.create_task(capture(node, node.name))
    return aggregate_host_usage(samples, snapshot.ready_nodes)
