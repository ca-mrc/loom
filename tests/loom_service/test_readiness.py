from __future__ import annotations

import asyncio
from typing import Any

import pytest

from loom_service.readiness import probe_dependencies


class _Scalar:
    def __init__(self, value: int) -> None:
        self._value = value

    def scalar_one(self) -> int:
        return self._value


class _Session:
    def __init__(self, *, value: int = 1, error: Exception | None = None) -> None:
        self.value = value
        self.error = error
        self.statements: list[str] = []

    async def execute(self, statement: Any) -> _Scalar:
        self.statements.append(str(statement))
        if self.error is not None:
            raise self.error
        if str(statement) != "SELECT 1":
            raise AssertionError("dependency readiness must not query staging bookkeeping")
        return _Scalar(self.value)


class _Minio:
    def __init__(self, *, failing: set[str] | None = None) -> None:
        self.failing = failing or set()
        self.calls: list[tuple[str, str]] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803 - boto3 API
        self.calls.append(("HEAD", Bucket))
        if Bucket in self.failing:
            raise RuntimeError("provider detail must be redacted")


@pytest.mark.parametrize(
    ("environment", "namespace"),
    [("staging", "loom-staging"), ("production", "loom"), ("development", "personal"), ("", "")],
)
def test_dependency_readiness_is_read_only_and_secret_free(
    environment: str, namespace: str,
) -> None:
    session = _Session()
    minio = _Minio()

    result = asyncio.run(
        probe_dependencies(
            session,  # type: ignore[arg-type]
            minio_client=minio,
            buckets=("trajectories", "artifacts", "artifacts"),
            environment=environment,
            namespace=namespace,
        )
    )

    assert result.ready
    assert session.statements == ["SELECT 1"]
    assert minio.calls == [("HEAD", "artifacts"), ("HEAD", "trajectories")]
    assert result.to_dict() == {
        "status": "ready", "postgres": "ready", "object_store": "ready",
        "environment": environment, "namespace": namespace, "blockers": [],
    }


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

    session = _Session(value=0 if failure == "unexpected-postgres" else 1,
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
