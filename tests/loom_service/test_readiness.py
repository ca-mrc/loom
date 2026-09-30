from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_capacity import CAPACITY_SOURCE
from loom_service.readiness import probe_dependencies


class _Scalar:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class _Mappings:
    def __init__(self, value: dict[str, Any]) -> None:
        self._value = value

    def mappings(self) -> _Mappings:
        return self

    def one(self) -> dict[str, Any]:
        return self._value


class _Session:
    def __init__(
        self,
        *,
        values: tuple[int, ...] = (1, 8),
        capacity: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.values = list(values)
        exact_capacity = StagingCapacity(1, 2, 80, 90)
        self.capacity = capacity or {
            "object_count": exact_capacity.object_count,
            "bytes_used": exact_capacity.bytes_used,
            "disk_free_percent": exact_capacity.disk_free_percent,
            "inode_free_percent": exact_capacity.inode_free_percent,
            "policy_sha256": staging_capacity_policy_digest(),
            "evidence_sha256": exact_capacity.evidence_digest,
            "source": CAPACITY_SOURCE,
            "observed_at": datetime.now(UTC),
        }
        self.error = error
        self.statements: list[str] = []

    async def execute(self, statement: Any) -> _Scalar | _Mappings:
        self.statements.append(str(statement))
        if self.error is not None:
            raise self.error
        if "staging_lifecycle_capacity" in str(statement):
            return _Mappings(self.capacity)
        return _Scalar(self.values.pop(0))


class _Minio:
    def __init__(self, *, failing: set[str] | None = None) -> None:
        self.failing = failing or set()
        self.calls: list[tuple[str, str]] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803 - boto3 API
        self.calls.append(("HEAD", Bucket))
        if Bucket in self.failing:
            raise RuntimeError("provider detail must be redacted")


def test_dependency_readiness_is_read_only_and_secret_free() -> None:
    session = _Session()
    minio = _Minio()

    result = asyncio.run(
        probe_dependencies(
            session,  # type: ignore[arg-type]
            minio_client=minio,
            buckets=("trajectories", "artifacts", "artifacts"),
            environment="staging",
            namespace="loom-staging",
        )
    )

    assert result.ready
    assert session.statements == [
        "SELECT 1",
        "SELECT epoch FROM staging_mutation_epochs WHERE environment = "
        "'staging' AND namespace = 'loom-staging'",
        "SELECT object_count, bytes_used, disk_free_percent, "
        "inode_free_percent, policy_sha256, evidence_sha256, source, observed_at "
        "FROM staging_lifecycle_capacity WHERE environment = 'staging' "
        "AND namespace = 'loom-staging'",
    ]
    assert minio.calls == [("HEAD", "artifacts"), ("HEAD", "trajectories")]
    assert result.to_dict()["blockers"] == []
    assert result.mutation_epoch == 8
    assert result.capacity_ready
    assert len(result.resource_digest) == 64


def test_dependency_readiness_reports_all_components_without_provider_details() -> None:
    session = _Session(error=RuntimeError("postgresql://secret"))
    minio = _Minio(failing={"artifacts", "trajectories"})

    result = asyncio.run(
        probe_dependencies(
            session,  # type: ignore[arg-type]
            minio_client=minio,
            buckets=("artifacts", "trajectories"),
            environment="staging",
            namespace="loom-staging",
        )
    )

    assert not result.ready
    assert result.blockers == (
        "object-store-bucket-unavailable:artifacts",
        "object-store-bucket-unavailable:trajectories",
        "postgres-unavailable",
    )
    assert "secret" not in str(result.to_dict())


def test_dependency_readiness_rejects_empty_bucket_authority() -> None:
    try:
        asyncio.run(
            probe_dependencies(
                _Session(),  # type: ignore[arg-type]
                minio_client=_Minio(),
                buckets=(),
                environment="staging",
                namespace="loom-staging",
            )
        )
    except ValueError as exc:
        assert str(exc) == "readiness bucket authority is invalid"
    else:  # pragma: no cover - defensive
        raise AssertionError("empty bucket authority was accepted")


@pytest.mark.parametrize("failure", [None, "postgres", "object-store", "unexpected-postgres"])
def test_api_only_probe_checks_dependencies_without_staging_capacity(failure: str | None) -> None:
    from loom_service.readiness import probe_api_dependencies

    session = _Session(values=(0 if failure == "unexpected-postgres" else 1,),
                       error=RuntimeError("private-db-url") if failure == "postgres" else None)
    storage = _Minio(failing={"shared-data"} if failure == "object-store" else None)
    result = asyncio.run(probe_api_dependencies(
        session, minio_client=storage, buckets=("shared-data", "shared-data"),  # type: ignore[arg-type]
    ))
    assert result.ready is (failure is None)
    assert session.statements == ["SELECT 1"]
    assert storage.calls == [("HEAD", "shared-data")]
    body = result.to_dict()
    assert body["mode"] == "api_only"
    assert "capacity_ready" not in body and "mutation_epoch" not in body
    assert "private-db-url" not in str(body) and "provider detail" not in str(body)
    if failure == "object-store":
        assert body["postgres"] == "ready" and body["object_store"] == "not-ready"
    elif failure in {"postgres", "unexpected-postgres"}:
        assert body["postgres"] == "not-ready" and body["object_store"] == "ready"


@pytest.mark.parametrize("buckets", [(), ("",), ("x" * 64,)])
def test_api_only_probe_rejects_invalid_configuration_without_external_calls(buckets: tuple[str, ...]) -> None:
    from loom_service.readiness import probe_api_dependencies

    session, storage = _Session(), _Minio()
    result = asyncio.run(probe_api_dependencies(
        session, minio_client=storage, buckets=buckets,  # type: ignore[arg-type]
    ))
    assert not result.ready
    assert result.to_dict()["blockers"] == ["object-store-configuration-invalid"]
    assert session.statements == [] and storage.calls == []
