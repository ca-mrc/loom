"""One native physical snapshot under a server-issued scope, outside SQL."""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from typing import Any

from loom_execution_capacity_collector.collector import CapacityCollectionError, _matching_samples
from loom_execution_capacity_collector.config import PoolCapacityCollectorSettings
from loom_execution_capacity_collector.kubernetes import InClusterKubernetesCapacityReader
from loom_execution_capacity_collector.nebius import NebiusCapacityReader
from loom_execution_capacity_collector.pool import PoolPodClassifier
from loom_execution_capacity_collector.pool_client import PoolObservationClient
from loom_execution_capacity_collector.pool_contracts import (
    PoolObservationReceiptV1,
    PoolObservationV1,
)


async def collect_pool_observation(settings: PoolCapacityCollectorSettings, *, management: Any | None = None,
                                   provider: Any | None = None, kubernetes: Any | None = None) -> PoolObservationReceiptV1:
    """No environment policies, inventory summation or legacy fallback."""
    async with AsyncExitStack() as resources:
        if management is None:
            management = PoolObservationClient(origin=settings.management_url,
                bearer_token_file=settings.management_bearer_token_file, timeout_seconds=settings.request_timeout_seconds)
            resources.push_async_callback(management.close)
        capture = await management.issue_capture(settings.pool_id)
        now = datetime.now(UTC)
        if (capture.pool_id != settings.pool_id
                or capture.scope.node_selector.get("nebius.com/node-group-id") != settings.nebius_node_group_id
                or capture.created_at > now + timedelta(seconds=60)
                or capture.created_at < now - timedelta(minutes=5)):
            raise CapacityCollectionError("pool capture binding is unavailable")
        if provider is None:
            provider = NebiusCapacityReader(settings)
            resources.push_async_callback(provider.close)
        if kubernetes is None:
            kubernetes = InClusterKubernetesCapacityReader(request_timeout_seconds=settings.request_timeout_seconds,
                connection=settings.kubernetes_connection)
            resources.push_async_callback(kubernetes.close)
        async with asyncio.TaskGroup() as tasks:
            native_task = tasks.create_task(provider.capture_pool())
            cluster_task = tasks.create_task(kubernetes.capture_pool(scope=capture.scope))
        native, cluster = native_task.result(), cluster_task.result()
        if (native.node_group is None or native.node_group.id != settings.nebius_node_group_id
                or native.node_count != native.node_group.node_count
                or cluster.source_versions.get("pool_scope") != PoolPodClassifier(capture.scope).fingerprint
                or (native.autoscaler_state == "ready" and (
                    cluster.active_nodes != native.node_count or cluster.ready_nodes != native.ready_node_count))):
            raise CapacityCollectionError("pool physical inventories disagree")
        cluster = cluster.model_copy(update={"template_samples": _matching_samples(native, cluster)})
        observation = PoolObservationV1(capture_id=capture.capture_id, observed_at=datetime.now(UTC),
                                        provider=native, kubernetes=cluster)
        return await management.publish(settings.pool_id, observation)
