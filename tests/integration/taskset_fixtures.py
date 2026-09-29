"""Shared TaskSet integration fixtures."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    Task,
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
    Team,
    TeamQuota,
    Token,
    User,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.minio_test_images import MINIO_TEST_IMAGE
from tests.support.minio_images import prepare_test_image

_MANIFEST_YAML = """
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: sample-tasks
  display_name: Sample Tasks
source:
  type: https
  locator: https://example.com/data.jsonl
instance_mapping:
  prompt: row.question
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }}"
  environment:
    os: linux
  agent:
    name: default
  steps:
    - artifacts: [out.txt]
"""


def _manifest_bytes(*, intents: str = "", verifier: str = "", display_name: str = "Sample Tasks") -> bytes:
    body = _MANIFEST_YAML.replace("display_name: Sample Tasks", f"display_name: {display_name}")
    if intents:
        body = body.replace(
            "metadata:",
            f"intents:\n{intents}\nmetadata:",
            1,
        )
    if verifier:
        body = body.replace(
            "task_template:",
            f"{verifier}\ntask_template:",
            1,
        )
    return body.encode()


@pytest.fixture(scope="module")
def tasksets_minio() -> MinioContainer:
    # The pinned MinIO image reports liveness before its object layer is writable.
    with MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).waiting_for(HttpWaitStrategy(9000, "/minio/health/cluster")) as m:
        yield m


@pytest.fixture
async def tasksets_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    tasksets_minio: MinioContainer,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    cfg = tasksets_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    if not tasksets_minio.get_client().bucket_exists(settings.artifacts_bucket):
        tasksets_minio.get_client().make_bucket(settings.artifacts_bucket)

    team_a = uuid4()
    team_b = uuid4()
    user_a = uuid4()
    user_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    legacy_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for team_id, raw, user_id in (
            (team_a, raw_a, user_a),
            (team_b, raw_b, user_b),
        ):
            s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            s.execute(insert(TeamQuota).values(team_id=team_id))
            username = f"TaskSetOwner-{team_id.hex[:8]}"
            s.execute(
                insert(User).values(
                    id=user_id,
                    username=username,
                    username_normalized=username.casefold(),
                    status="active",
                    is_platform_admin=False,
                ),
            )
            s.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode()).digest(),
                    type="team",
                    scopes=["read:own", "submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                ),
            )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_a,
                issued_at=datetime.now(UTC),
            ),
        )
        s.commit()

    tokens = {"team_a": raw_a, "team_b": raw_b, "legacy_a": legacy_raw}
    teams = {"team_a": team_a, "team_b": team_b}
    try:
        yield app, tokens, teams
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(TaskSetMaterializationJob))
            s.execute(delete(TaskSetManifest))
            # TaskSet's SET NULL FK would leak private task rows into the
            # global catalog of subsequent tests sharing this database.
            s.execute(delete(Task).where(Task.task_set_id.is_not(None)))
            s.execute(delete(TaskSet))
            s.execute(delete(Token))
            s.execute(delete(User))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()
