"""Single-source, read-only dependency readiness for service and rollout gates."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_capacity import StagingCapacityEvidence


@dataclass(frozen=True, slots=True)
class ApiDependencyReadiness:
    """API-only dependency health, not legacy staging capacity authority."""

    postgres_ready: bool
    object_store_ready: bool
    blockers: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.postgres_ready and self.object_store_ready and not self.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "ready" if self.ready else "not-ready",
            "mode": "api_only",
            "postgres": "ready" if self.postgres_ready else "not-ready",
            "object_store": "ready" if self.object_store_ready else "not-ready",
            "blockers": list(self.blockers),
        }


async def probe_api_dependencies(
    session: AsyncSession, *, minio_client: Any, buckets: tuple[str, ...],
) -> ApiDependencyReadiness:
    """Read this API's configured DB and buckets without staging lifecycle SQL.

    Uses the running API's own credentials, never management or worker authority.
    This is neither a capacity grant nor proof that a task can execute.
    """
    normalized_buckets = tuple(sorted(set(buckets)))
    if not normalized_buckets or any(not bucket or len(bucket) > 63 for bucket in normalized_buckets):
        return ApiDependencyReadiness(False, False, ("object-store-configuration-invalid",))
    blockers: list[str] = []
    postgres_ready = False
    try:
        postgres_ready = (await session.execute(text("SELECT 1"))).scalar_one() == 1
    except Exception:  # driver exceptions may contain credentials
        blockers.append("postgres-unavailable")
    if not postgres_ready and not blockers:
        blockers.append("postgres-unexpected-result")
    object_store_ready = True
    for bucket in normalized_buckets:
        try:
            await asyncio.to_thread(minio_client.head_bucket, Bucket=bucket)
        except Exception:  # provider exceptions may contain credentials
            object_store_ready = False
    if not object_store_ready:
        blockers.append("object-store-unavailable")
    return ApiDependencyReadiness(postgres_ready, object_store_ready, tuple(sorted(blockers)))


@dataclass(frozen=True, slots=True)
class DependencyReadiness:
    """Secret-free result shared by the HTTP route and rollout preflight."""

    postgres_ready: bool
    object_store_ready: bool
    environment: str
    namespace: str
    mutation_epoch: int
    capacity: StagingCapacity | None
    capacity_ready: bool
    resource_digest: str
    blockers: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.postgres_ready and self.object_store_ready and not self.blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "ready" if self.ready else "not-ready",
            "postgres": "ready" if self.postgres_ready else "not-ready",
            "object_store": "ready" if self.object_store_ready else "not-ready",
            "environment": self.environment,
            "namespace": self.namespace,
            "mutation_epoch": self.mutation_epoch,
            "capacity": (
                None
                if self.capacity is None
                else {
                    "object_count": self.capacity.object_count,
                    "bytes_used": self.capacity.bytes_used,
                    "disk_free_percent": self.capacity.disk_free_percent,
                    "inode_free_percent": self.capacity.inode_free_percent,
                    "policy_sha256": staging_capacity_policy_digest(),
                    "evidence_sha256": self.capacity.evidence_digest,
                }
            ),
            "capacity_ready": self.capacity_ready,
            "resource_digest": self.resource_digest,
            "blockers": list(self.blockers),
        }


async def probe_dependencies(
    session: AsyncSession,
    *,
    minio_client: Any,
    buckets: tuple[str, ...],
    environment: str,
    namespace: str,
) -> DependencyReadiness:
    """Probe PostgreSQL and exact configured buckets without writing state.

    The object-store call is HEAD-only and executes off the event loop.  Results
    deliberately expose stable component codes, never connection strings,
    credentials, provider error text, or object names.
    """
    normalized_buckets = tuple(sorted(set(buckets)))
    if (
        environment != "staging"
        or namespace != "loom-staging"
        or not normalized_buckets
        or any(
        not bucket or len(bucket) > 63 for bucket in normalized_buckets
        )
    ):
        raise ValueError("readiness bucket authority is invalid")

    blockers: list[str] = []
    postgres_ready = False
    try:
        value = (await session.execute(text("SELECT 1"))).scalar_one()
        postgres_ready = value == 1
    except Exception:  # pragma: no cover - driver/provider classes vary
        blockers.append("postgres-unavailable")
    if not postgres_ready and "postgres-unavailable" not in blockers:
        blockers.append("postgres-unexpected-result")

    mutation_epoch = -1
    if postgres_ready:
        try:
            mutation_epoch = int(
                (
                    await session.execute(
                        text(
                            "SELECT epoch FROM staging_mutation_epochs "
                            "WHERE environment = 'staging' "
                            "AND namespace = 'loom-staging'"
                        )
                    )
                ).scalar_one()
            )
            if mutation_epoch < 0:
                raise ValueError("negative epoch")
        except Exception:
            blockers.append("mutation-epoch-unavailable")
            mutation_epoch = -1

    capacity: StagingCapacity | None = None
    capacity_ready = False
    if postgres_ready:
        try:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT object_count, bytes_used, disk_free_percent, "
                            "inode_free_percent, policy_sha256, evidence_sha256, "
                            "source, observed_at FROM staging_lifecycle_capacity "
                            "WHERE environment = 'staging' "
                            "AND namespace = 'loom-staging'"
                        )
                    )
                )
                .mappings()
                .one()
            )
            capacity = StagingCapacity(
                object_count=int(row["object_count"]),
                bytes_used=int(row["bytes_used"]),
                disk_free_percent=int(row["disk_free_percent"]),
                inode_free_percent=int(row["inode_free_percent"]),
            )
            evidence = StagingCapacityEvidence(
                namespace=namespace,
                capacity=capacity,
                policy_sha256=str(row["policy_sha256"]),
                evidence_sha256=str(row["evidence_sha256"]),
                observed_at=row["observed_at"],
                source=str(row["source"]),
            )
            evidence.require_fresh_admission(now=datetime.now(UTC))
            capacity_ready = True
        except Exception:  # pragma: no cover - DB/provider classes vary
            blockers.append("capacity-evidence-unavailable")

    object_store_ready = True
    for bucket in normalized_buckets:
        try:
            await asyncio.to_thread(minio_client.head_bucket, Bucket=bucket)
        except Exception:  # pragma: no cover - botocore/provider classes vary
            object_store_ready = False
            blockers.append(f"object-store-bucket-unavailable:{bucket}")

    digest = hashlib.sha256(
        json.dumps(
            {
                "buckets": normalized_buckets,
                "object_store_ready": object_store_ready,
                "postgres_ready": postgres_ready,
                "environment": environment,
                "namespace": namespace,
                "mutation_epoch": mutation_epoch,
                "capacity_digest": (
                    "unavailable" if capacity is None else capacity.evidence_digest
                ),
                "capacity_ready": capacity_ready,
                "version": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return DependencyReadiness(
        postgres_ready=postgres_ready,
        object_store_ready=object_store_ready,
        environment=environment,
        namespace=namespace,
        mutation_epoch=mutation_epoch,
        capacity=capacity,
        capacity_ready=capacity_ready,
        resource_digest=digest,
        blockers=tuple(sorted(blockers)),
    )


__all__ = ["ApiDependencyReadiness", "DependencyReadiness", "probe_api_dependencies", "probe_dependencies"]
