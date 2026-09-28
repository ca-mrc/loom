"""Application health route against disposable PostgreSQL and MinIO."""

from __future__ import annotations

from types import SimpleNamespace

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from loom.admin_secret import AdminSecretVerifier
from loom_service.routes.health import router


async def test_readiness_without_staging_tables_and_missing_bucket(
    shared_minio: MinioContainer, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = shared_minio.get_config()
    s3 = boto3.client(
        "s3", endpoint_url=f"http://{cfg['endpoint']}",
        aws_access_key_id=cfg["access_key"], aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1", config=Config(signature_version="s3v4"),
    )
    buckets = ("readiness-artifacts", "readiness-trajectories")
    for bucket in buckets:
        s3.create_bucket(Bucket=bucket)
    # A database with no Loom tables makes accidental staging SQL observable.
    with PostgresContainer("postgres:16") as pg:
        engine = create_async_engine(pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        ))
        app = FastAPI()
        app.include_router(router, prefix="/api/v1")
        app.state.session_factory = async_sessionmaker(engine)
        app.state.settings = SimpleNamespace(
            artifacts_bucket=buckets[0], trajectories_bucket=buckets[1],
            session_cookie_name="loom_session", session_audience="application",
        )
        token = "loom_admin_" + "disposable-readiness-fixture-" * 2
        app.state.admin_secret_verifier = AdminSecretVerifier.from_token(token)
        app.state.minio_client = s3
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test",
            ) as client:
                assert (await client.get("/api/v1/health/ready")).status_code == 401
                for environment in ("development", "staging", "production"):
                    monkeypatch.setenv("LOOM_ENV", environment)
                    monkeypatch.setenv("LOOM_NAMESPACE", "application")
                    response = await client.get(
                        "/api/v1/health/ready", headers={"Authorization": f"Bearer {token}"},
                    )
                    assert response.status_code == 200, response.text
                    assert response.json()["status"] == "ready"
                    assert response.json()["environment"] == environment
                s3.delete_bucket(Bucket=buckets[1])
                response = await client.get(
                    "/api/v1/health/ready", headers={"Authorization": f"Bearer {token}"},
                )
                assert response.status_code == 503
                assert response.json()["postgres"] == "ready"
                assert response.json()["blockers"] == [
                    "object-store-bucket-unavailable:readiness-trajectories",
                ]
                assert token not in response.text
                assert cfg["secret_key"] not in response.text
                assert (await client.get("/api/v1/health")).status_code == 200
        finally:
            await engine.dispose()
    s3.delete_bucket(Bucket=buckets[0])
    s3.close()
