from __future__ import annotations

import asyncio
from typing import Any

import pytest
from botocore.exceptions import ClientError

from loom_service.readiness import probe_api_dependencies, probe_dependencies


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
        self.calls: list[tuple[str, int]] = []

    def list_objects_v2(self, *, Bucket: str, MaxKeys: int) -> dict[str, Any]:  # noqa: N803 - boto3 API
        self.calls.append((Bucket, MaxKeys))
        if Bucket in self.failing:
            raise RuntimeError("provider detail must be redacted")
        return {"ResponseMetadata": {"HTTPStatusCode": 200}, "KeyCount": 0}


class _ObjectEditor:
    """Object access is permitted; bucket administration remains forbidden."""

    def __init__(self, response: Any, error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.calls: list[tuple[str, int]] = []

    def head_bucket(self, *, Bucket: str) -> None:  # noqa: N803 - boto3 API
        raise ClientError({"Error": {"Code": "AccessDenied"},
                           "ResponseMetadata": {"HTTPStatusCode": 403}}, "HeadBucket")

    def list_objects_v2(self, *, Bucket: str, MaxKeys: int) -> Any:  # noqa: N803 - boto3 API
        self.calls.append((Bucket, MaxKeys))
        if self.error is not None:
            raise self.error
        return self.response


def _object_editor_readiness(mode: str, storage: _ObjectEditor):
    session = _Session()
    kwargs: dict[str, Any] = {"minio_client": storage, "buckets": ("trajectories", "artifacts", "artifacts")}
    if mode == "api_only":
        result = asyncio.run(probe_api_dependencies(session, **kwargs))  # type: ignore[arg-type]
    else:
        result = asyncio.run(probe_dependencies(
            session, environment="production", namespace="application", **kwargs,  # type: ignore[arg-type]
        ))
    assert session.statements == ["SELECT 1"]
    return result


@pytest.mark.parametrize("mode", ["standalone", "api_only"])
@pytest.mark.parametrize("empty", [True, False])
def test_readiness_accepts_object_editor_access_without_bucket_metadata_permissions(mode: str, empty: bool) -> None:
    storage = _ObjectEditor({
        "ResponseMetadata": {"HTTPStatusCode": 200}, "Name": "private-bucket-name",
        "KeyCount": 0 if empty else 1,
        "Contents": [] if empty else [{"Key": "private-object-name"}],
    })
    result = _object_editor_readiness(mode, storage)
    assert result.ready
    assert storage.calls == [("artifacts", 1), ("trajectories", 1)]
    assert "private" not in str(result.to_dict())


@pytest.mark.parametrize("mode", ["standalone", "api_only"])
@pytest.mark.parametrize("response", [
    None, "private-provider-body", {}, {"ResponseMetadata": None},
    {"ResponseMetadata": {}}, {"ResponseMetadata": {"HTTPStatusCode": "200"}},
    {"ResponseMetadata": {"HTTPStatusCode": 403}},
    {"ResponseMetadata": {"HTTPStatusCode": 503}},
])
def test_readiness_rejects_missing_malformed_or_unsuccessful_object_response(mode: str, response: Any) -> None:
    storage = _ObjectEditor(response)
    result = _object_editor_readiness(mode, storage)
    assert not result.ready
    assert result.postgres_ready and not result.object_store_ready
    assert storage.calls == [("artifacts", 1), ("trajectories", 1)]
    assert "private" not in str(result.to_dict())
    if mode == "api_only":
        assert result.blockers == ("object-store-unavailable",)
        assert "artifacts" not in str(result.to_dict()) and "trajectories" not in str(result.to_dict())


@pytest.mark.parametrize("mode", ["standalone", "api_only"])
def test_readiness_rejects_object_access_denied_and_redacts_provider_error(mode: str) -> None:
    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "private-provider-secret"},
                          "ResponseMetadata": {"HTTPStatusCode": 403}}, "ListObjectsV2")
    storage = _ObjectEditor(None, error=denied)
    result = _object_editor_readiness(mode, storage)
    assert not result.ready
    assert result.postgres_ready and not result.object_store_ready
    assert storage.calls == [("artifacts", 1), ("trajectories", 1)]
    assert "private-provider-secret" not in str(result.to_dict())


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
    assert minio.calls == [("artifacts", 1), ("trajectories", 1)]
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
    assert storage.calls == [("shared-data", 1)]
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
