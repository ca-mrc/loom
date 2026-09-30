"""Read-only application readiness for PostgreSQL and configured object storage."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


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
    """Secret-free component status returned by the application health route."""

    postgres_ready: bool
    object_store_ready: bool
    environment: str
    namespace: str
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
    if not normalized_buckets or any(
        not bucket or len(bucket) > 63 for bucket in normalized_buckets
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

    object_store_ready = True
    for bucket in normalized_buckets:
        try:
            await asyncio.to_thread(minio_client.head_bucket, Bucket=bucket)
        except Exception:  # pragma: no cover - botocore/provider classes vary
            object_store_ready = False
            blockers.append(f"object-store-bucket-unavailable:{bucket}")

    return DependencyReadiness(
        postgres_ready=postgres_ready,
        object_store_ready=object_store_ready,
        environment=environment,
        namespace=namespace,
        blockers=tuple(sorted(blockers)),
    )


__all__ = ["ApiDependencyReadiness", "DependencyReadiness", "probe_api_dependencies", "probe_dependencies"]
