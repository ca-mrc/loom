"""Plan 28 PR-3: multi-(agent, model) combinations + backend +
subset semantics on POST /api/v1/batches.
"""

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
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    ProviderConnection,
    ProviderModelCache,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    User,
    Worker,
)
from loom_service import agent_catalog
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.gateway_db import delete_lifecycle_authorities


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _script_verifier_task_config(task_id: str) -> dict[str, object]:
    """aime-shape task (verifier=script, no solve.sh). See
    test_service_batches_crud._script_verifier_task_config for the
    parallel single-agent test."""
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {
            "name": "script",
            "args": {"script_path": "/workspace/verifier/run.sh"},
        },
        "steps": [{"name": "main"}],
    }


@pytest.fixture
async def setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str]]:
    for k, v in {
        "LOOM_ENV": "development",
        "LOOM_LOCAL_EXECUTION": "1",
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    app.state.gateway_client = httpx.AsyncClient(
        base_url=str(settings.gateway_url),
    )

    team_id = uuid4()
    user_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    worker_id = uuid4()
    owned_task_ids = tuple(
        [f"humaneval/HumanEval/{index}" for index in range(10)] + ["local/script-only-combo"]
    )
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(User).values(
                id=user_id,
                username=f"BatchComboUser-{user_id.hex[:8]}",
                username_normalized=f"batch-combo-user-{user_id.hex[:8]}",
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
            )
        )
        # Seed 10 deterministic tasks under benchmark "humaneval".
        for task_id in owned_task_ids[:10]:
            s.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="x" * 64,
                    config=_valid_task_config(task_id),
                    source="local",
                    license="MIT",
                    benchmark_id=None,
                )
            )
        # Live worker advertising every backend Loom ships drivers for —
        # required by the POST /batches reject-when-no-worker check.
        s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fixture-worker",
                version="test",
                capabilities=[
                    {"backend": "docker"},
                    {"backend": "fake"},
                    {"backend": "modal"},
                ],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        s.commit()

    try:
        yield app, raw
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            trial_ids = tuple(s.scalars(select(Trial.id).where(Trial.team_id == team_id)).all())
            batch_ids = tuple(s.scalars(select(Batch.id).where(Batch.team_id == team_id)).all())
            s.execute(delete(Trial).where(Trial.team_id == team_id))
            s.execute(delete(Batch).where(Batch.team_id == team_id))
            s.execute(delete(ProviderConnection).where(ProviderConnection.team_id == team_id))
            s.execute(delete(Task).where(Task.id.in_(owned_task_ids)))
            s.execute(delete(Token).where(Token.team_id == team_id))
            s.execute(delete(Worker).where(Worker.id == worker_id))
            delete_lifecycle_authorities(
                s,
                bindings=(
                    *(("trial", "trial", str(trial_id), team_id) for trial_id in trial_ids),
                    *(("event", "trial", str(trial_id), team_id) for trial_id in trial_ids),
                    *(("run", "batch", str(batch_id), team_id) for batch_id in batch_ids),
                ),
            )
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
            s.execute(delete(User).where(User.id == user_id))
            s.execute(delete(Team).where(Team.id == team_id))
            s.commit()
        sync_engine.dispose()


async def _post(app: FastAPI, raw: str, body: dict) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        return await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json=body,
        )


# ----------------------------------------------------------------
# Subset semantics
# ----------------------------------------------------------------


async def test_subset_first_n(setup: tuple[FastAPI, str]) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "first-3",
            "backend": "docker",
            "purpose": "evaluation",
            "task_filter": {
                "license": "MIT",
                "subset_kind": "first_n",
                "n": 3,
            },
            "trial_config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
        },
    )
    assert r.status_code == 201, r.text
    # First 3 by id-asc are HumanEval/0, /1, /2 (3 trials × 1 sample).
    assert r.json()["expected_trial_count"] == 3


async def test_subset_last_n(setup: tuple[FastAPI, str]) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "last-2",
            "purpose": "evaluation",
            "task_filter": {
                "license": "MIT",
                "subset_kind": "last_n",
                "n": 2,
            },
            "trial_config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
        },
    )
    assert r.status_code == 201
    assert r.json()["expected_trial_count"] == 2


async def test_subset_random_n_reproducible(
    setup: tuple[FastAPI, str],
) -> None:
    """Same seed → same task selection → same trial count.
    Different seeds with the same n still produce the same COUNT
    (sampling without replacement)."""
    app, raw = setup
    body = {
        "name": "rand-5-seed-42",
        "purpose": "evaluation",
        "task_filter": {
            "license": "MIT",
            "subset_kind": "random_n",
            "n": 5,
            "seed": 42,
        },
        "trial_config": {
            "agent_name": "oracle",
            "agent_model": None,
        },
    }
    r1 = await _post(app, raw, body)
    assert r1.status_code == 201
    assert r1.json()["expected_trial_count"] == 5


async def test_subset_random_n_requires_seed(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "no-seed",
            "purpose": "evaluation",
            "task_filter": {
                "license": "MIT",
                "subset_kind": "random_n",
                "n": 3,
            },
            "trial_config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
        },
    )
    assert r.status_code == 400
    assert "seed" in r.json()["detail"].lower()


async def test_subset_unknown_kind_rejected(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "phantom",
            "purpose": "evaluation",
            "task_filter": {
                "license": "MIT",
                "subset_kind": "fancy_n",
                "n": 3,
            },
            "trial_config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
        },
    )
    assert r.status_code == 400
    assert "unknown subset_kind" in r.json()["detail"]


# ----------------------------------------------------------------
# Combinations
# ----------------------------------------------------------------


async def test_combinations_compute_expected_count(
    setup: tuple[FastAPI, str],
) -> None:
    """Multi-combination batch: 10 tasks × 2 combinations (n=3 each)
    = 60 trials expected."""
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "multi-combo",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT"},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "n_per_task": 3,
                    "label": "oracle-3",
                },
                {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "n_per_task": 3,
                    "label": "oracle-3-bis",
                },
            ],
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 60
    assert len(body["combinations"]) == 2


async def test_combinations_preserve_per_combo_provider_routing(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    conn_a = uuid4()
    conn_b = uuid4()
    with sl() as s:
        team_id = s.execute(
            select(Token.team_id).where(
                Token.token_hash == hashlib.sha256(raw.encode()).digest(),
                Token.type == "team",
            )
        ).scalar_one()
        for conn_id, display_name, model_id in (
            (conn_a, "Combo provider A", "glm-5.1-thinking"),
            (conn_b, "Combo provider B", "qwen3.6-35b-a3b"),
        ):
            s.execute(
                insert(ProviderConnection).values(
                    id=conn_id,
                    team_id=team_id,
                    provider_type="openai-compatible",
                    display_name=display_name,
                    base_url="https://api.example.test/v1",
                    upstream_host="api.example.test",
                    resolved_egress_ips=["203.0.113.10"],
                    encrypted_api_key_ref=f"test://{conn_id}",
                    status="valid",
                    pricing_source="tokens-only",
                    created_by="test:combo",
                )
            )
            s.execute(
                insert(ProviderModelCache).values(
                    provider_connection_id=conn_id,
                    model_id=model_id,
                    last_preflight_status="valid",
                )
            )
        s.commit()
    sync_engine.dispose()

    r = await _post(
        app,
        raw,
        {
            "name": "combo-provider-routing",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 2},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "glm-5.1-thinking"},
                    "provider_connection_id": str(conn_a),
                    "provider_model_id": "glm-5.1-thinking",
                    "n_per_task": 1,
                    "label": "litellm-glm",
                },
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen3.6-35b-a3b"},
                    "provider_connection_id": str(conn_b),
                    "provider_model_id": "qwen3.6-35b-a3b",
                    "n_per_task": 1,
                    "label": "litellm-qwen",
                },
            ],
        },
    )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 4
    assert body["combinations"][0]["provider_connection_id"] == str(conn_a)
    assert body["combinations"][0]["provider_model_id"] == "glm-5.1-thinking"
    assert body["combinations"][1]["provider_connection_id"] == str(conn_b)
    assert body["combinations"][1]["provider_model_id"] == "qwen3.6-35b-a3b"

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch = s.execute(select(Batch).where(Batch.id == body["batch_id"])).scalar_one()
        assert batch.provider_connection_id is None
        assert batch.provider_model_id is None
        assert batch.combinations[0]["provider_connection_id"] == str(conn_a)
        assert batch.combinations[1]["provider_model_id"] == "qwen3.6-35b-a3b"
    sync_engine.dispose()


@pytest.mark.parametrize("provider_type", ["anthropic", "google"])
async def test_combinations_reject_native_provider_connection(
    setup: tuple[FastAPI, str],
    postgres_url: str,
    provider_type: str,
) -> None:
    """#2054: hosted agent submissions accept only OpenAI-compatible
    Provider Connections; native Anthropic/Google are rejected up front."""
    app, raw = setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    conn_id = uuid4()
    with sl() as s:
        team_id = s.execute(
            select(Token.team_id).where(
                Token.token_hash == hashlib.sha256(raw.encode()).digest(),
                Token.type == "team",
            )
        ).scalar_one()
        s.execute(
            insert(ProviderConnection).values(
                id=conn_id,
                team_id=team_id,
                provider_type=provider_type,
                display_name=f"Native {provider_type}",
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                resolved_egress_ips=["203.0.113.10"],
                encrypted_api_key_ref=f"test://{conn_id}",
                status="valid",
                pricing_source="tokens-only",
                created_by="test:combo",
            )
        )
        s.commit()
    sync_engine.dispose()

    r = await _post(
        app,
        raw,
        {
            "name": f"combo-native-{provider_type}",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": provider_type, "name": "native-model"},
                    "provider_connection_id": str(conn_id),
                    "provider_model_id": "native-model",
                    "n_per_task": 1,
                    "label": "native",
                },
            ],
        },
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert provider_type in detail
    assert "OpenAI-compatible" in detail


async def test_combinations_reject_provider_model_cache_per_combo(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    conn_id = uuid4()
    with sl() as s:
        team_id = s.execute(
            select(Token.team_id).where(
                Token.token_hash == hashlib.sha256(raw.encode()).digest(),
                Token.type == "team",
            )
        ).scalar_one()
        s.execute(
            insert(ProviderConnection).values(
                id=conn_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="Combo provider",
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                resolved_egress_ips=["203.0.113.10"],
                encrypted_api_key_ref=f"test://{conn_id}",
                status="valid",
                pricing_source="tokens-only",
                created_by="test:combo",
            )
        )
        s.execute(
            insert(ProviderModelCache).values(
                provider_connection_id=conn_id,
                model_id="bad-model",
                last_preflight_status="failed",
                last_preflight_error_code="upstream_404",
            )
        )
        s.commit()
    sync_engine.dispose()

    r = await _post(
        app,
        raw,
        {
            "name": "combo-provider-cache-fail",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "bad-model"},
                    "provider_connection_id": str(conn_id),
                    "provider_model_id": "bad-model",
                    "n_per_task": 1,
                    "label": "bad-combo",
                },
            ],
        },
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "combinations[0]" in detail
    assert "bad-combo" in detail
    assert "last preflight failed" in detail


async def test_combinations_reject_agent_in_trial_config(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "ambiguous",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT"},
            "trial_config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
            "combinations": [
                {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "n_per_task": 1,
                    "label": "x",
                }
            ],
        },
    )
    assert r.status_code == 400
    assert "trial_config.agent_name must be absent" in r.json()["detail"]


async def test_combinations_unique_labels(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "dupes",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT"},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "n_per_task": 1,
                    "label": "twin",
                },
                {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "n_per_task": 1,
                    "label": "twin",
                },
            ],
        },
    )
    assert r.status_code == 400
    assert "duplicated" in r.json()["detail"]


async def test_combinations_reject_unknown_agent(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "ghost",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT"},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "not-a-real-agent",
                    "agent_model": None,
                    "n_per_task": 1,
                    "label": "x",
                }
            ],
        },
    )
    assert r.status_code == 400
    assert "agent catalog" in r.json()["detail"].lower()


async def test_combinations_reject_agent_without_service_runtime(
    setup: tuple[FastAPI, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(agent_catalog._ADAPTER_RUNTIME_READY, "codex", False)
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "codex-combo",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT"},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "codex",
                    "agent_model": {"provider": "openai", "name": "gpt-4o"},
                    "n_per_task": 1,
                    "label": "codex",
                }
            ],
        },
    )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "combinations[0]" in detail
    assert "codex" in detail
    assert "runtime" in detail.lower()
    assert "GET /api/v1/agents" in detail


# ----------------------------------------------------------------
# Backend
# ----------------------------------------------------------------


async def test_combinations_reject_oracle_when_any_task_is_incompat(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """#320: a multi-agent combinations batch where oracle is one of
    the agents must be rejected if any task is non-pytest. The
    structured 400 lets the matrix runner split per-agent."""
    app, raw = setup
    # Inject an aime-shape task into the fixture's task table.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Task).values(
                id="local/script-only-combo",
                checksum="x" * 64,
                config=_script_verifier_task_config("local/script-only-combo"),
                source="local",
                license="MIT",
                benchmark_id=None,
            ),
        )
        s.commit()
    sync_engine.dispose()

    r = await _post(
        app,
        raw,
        {
            "name": "oracle+litellm-on-script-task",
            "purpose": "evaluation",
            "provider_connection_id": str(
                _insert_connection(postgres_url, raw, "Incompat provider", ("gpt-4o-mini",)),
            ),
            "task_filter": {
                "task_ids": ["local/script-only-combo"],
                "subset_kind": "explicit",
            },
            "trial_config": {},
            "combinations": [
                {"agent_name": "oracle", "agent_model": None, "n_per_task": 1, "label": "oracle"},
                {
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                    "n_per_task": 1,
                    "label": "litellm",
                },
            ],
        },
    )
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "oracle" in detail
    # litellm has no requirements — it must not appear in the offender list.
    assert "litellm:" not in detail


async def test_backend_stored_on_batch(setup: tuple[FastAPI, str]) -> None:
    app, raw = setup
    r = await _post(
        app,
        raw,
        {
            "name": "docker-batch",
            "backend": "docker",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {
                "agent_name": "oracle",
                "agent_model": None,
            },
        },
    )
    assert r.status_code == 201
    assert r.json()["backend"] == "docker"


# ----------------------------------------------------------------
# #2054: one effective connection/model per combination
# ----------------------------------------------------------------


def _insert_connection(
    postgres_url: str,
    raw: str,
    display_name: str,
    model_ids: tuple[str, ...],
) -> UUID:
    conn_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        team_id = s.execute(
            select(Token.team_id).where(
                Token.token_hash == hashlib.sha256(raw.encode()).digest(),
                Token.type == "team",
            )
        ).scalar_one()
        s.execute(
            insert(ProviderConnection).values(
                id=conn_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=display_name,
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                resolved_egress_ips=["203.0.113.10"],
                encrypted_api_key_ref=f"test://{conn_id}",
                status="valid",
                pricing_source="tokens-only",
                created_by="test:combo",
            )
        )
        for model_id in model_ids:
            s.execute(
                insert(ProviderModelCache).values(
                    provider_connection_id=conn_id,
                    model_id=model_id,
                    last_preflight_status="valid",
                )
            )
        s.commit()
    sync_engine.dispose()
    return conn_id


def _stored_combinations(postgres_url: str, batch_id: str) -> list[dict]:
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        batch = s.execute(select(Batch).where(Batch.id == batch_id)).scalar_one()
        combinations = list(batch.combinations)
    sync_engine.dispose()
    return combinations


def _litellm(model: str, **extra: object) -> dict:
    return {
        "agent_name": "litellm",
        "agent_model": {"provider": "openai", "name": model},
        "n_per_task": 1,
        **extra,
    }


async def test_combinations_reject_conflicting_model_identity(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    conn_id = _insert_connection(postgres_url, raw, "Conflict provider", ("gpt-4o", "gpt-4o-mini"))

    r = await _post(
        app,
        raw,
        {
            "name": "combo-model-conflict",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "combinations": [
                _litellm(
                    "gpt-4o",
                    provider_connection_id=str(conn_id),
                    provider_model_id="gpt-4o-mini",
                ),
            ],
        },
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "combinations[0]" in detail
    assert "'gpt-4o'" in detail and "'gpt-4o-mini'" in detail


async def test_combinations_reject_model_that_conflicts_with_inherited_batch_model(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    conn_id = _insert_connection(postgres_url, raw, "Inherit provider", ("gpt-4o", "gpt-4o-mini"))

    r = await _post(
        app,
        raw,
        {
            "name": "combo-inherited-conflict",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "provider_connection_id": str(conn_id),
            "provider_model_id": "gpt-4o",
            "combinations": [_litellm("gpt-4o"), _litellm("gpt-4o-mini")],
        },
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "combinations[1]" in detail
    assert "batch-level provider_model_id" in detail


async def test_combinations_store_resolved_route_inherited_from_batch(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    conn_id = _insert_connection(postgres_url, raw, "Default provider", ("gpt-4o",))

    r = await _post(
        app,
        raw,
        {
            "name": "combo-inherits-route",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "provider_connection_id": str(conn_id),
            "combinations": [_litellm("gpt-4o")],
        },
    )

    assert r.status_code == 201, r.text
    stored = _stored_combinations(postgres_url, r.json()["batch_id"])
    assert stored[0]["provider_connection_id"] == str(conn_id)
    assert stored[0]["provider_model_id"] == "gpt-4o"


async def test_combinations_compare_same_model_across_connections(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    conn_a = _insert_connection(postgres_url, raw, "Relay A", ("gpt-4o",))
    conn_b = _insert_connection(postgres_url, raw, "Relay B", ("gpt-4o",))

    r = await _post(
        app,
        raw,
        {
            "name": "combo-same-model",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 2},
            "trial_config": {},
            "combinations": [
                _litellm("gpt-4o", provider_connection_id=str(conn_a)),
                _litellm("gpt-4o", provider_connection_id=str(conn_b)),
            ],
        },
    )

    assert r.status_code == 201, r.text
    assert r.json()["expected_trial_count"] == 4
    stored = _stored_combinations(postgres_url, r.json()["batch_id"])
    assert [c["label"] for c in stored] == [
        "litellm/openai/gpt-4o@Relay A",
        "litellm/openai/gpt-4o@Relay B",
    ]
    assert [c["provider_connection_id"] for c in stored] == [str(conn_a), str(conn_b)]


async def test_combinations_reject_oracle_with_provider_fields(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    app, raw = setup
    conn_id = _insert_connection(postgres_url, raw, "Stale provider", ("gpt-4o",))

    r = await _post(
        app,
        raw,
        {
            "name": "combo-oracle-stale",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "combinations": [
                {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "provider_connection_id": str(conn_id),
                    "n_per_task": 1,
                },
            ],
        },
    )

    assert r.status_code == 400, r.text
    assert "does not take a model" in r.json()["detail"]


async def test_combinations_mixed_oracle_ignores_batch_level_route(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """The batch-level connection is a default for model-backed combinations
    only; the oracle combination is stored without a provider route."""
    app, raw = setup
    conn_id = _insert_connection(postgres_url, raw, "Mixed provider", ("gpt-4o",))

    r = await _post(
        app,
        raw,
        {
            "name": "combo-mixed-oracle",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "provider_connection_id": str(conn_id),
            "combinations": [
                {"agent_name": "oracle", "agent_model": None, "n_per_task": 1},
                _litellm("gpt-4o", n_per_task=2),
            ],
        },
    )

    assert r.status_code == 201, r.text
    assert r.json()["expected_trial_count"] == 3
    stored = _stored_combinations(postgres_url, r.json()["batch_id"])
    assert stored[0]["provider_connection_id"] is None
    assert stored[0]["provider_model_id"] is None
    assert stored[1]["provider_connection_id"] == str(conn_id)
    assert stored[1]["provider_model_id"] == "gpt-4o"


async def test_model_backed_submission_without_connection_is_rejected(
    setup: tuple[FastAPI, str],
) -> None:
    """#2054: no fallback to platform credentials for model-backed work."""
    app, raw = setup

    r = await _post(
        app,
        raw,
        {
            "name": "combo-no-connection",
            "purpose": "evaluation",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "combinations": [
                {"agent_name": "oracle", "agent_model": None, "n_per_task": 1},
                _litellm("gpt-4o"),
            ],
        },
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "combinations[1]" in detail
    assert "requires a Provider Connection" in detail


async def test_nebius_batch_rejects_agents_without_native_execution(
    setup: tuple[FastAPI, str],
    postgres_url: str,
) -> None:
    """#2054: a Nebius batch naming an agent native execution cannot run yet
    gets one clear reason before admission, not raw per-task codes."""
    app, raw = setup
    conn_id = _insert_connection(postgres_url, raw, "Native check", ("gpt-4o",))

    r = await _post(
        app,
        raw,
        {
            "name": "native-check",
            "purpose": "evaluation",
            "backend": "nebius",
            "task_filter": {"license": "MIT", "subset_kind": "first_n", "n": 1},
            "trial_config": {},
            "provider_connection_id": str(conn_id),
            "combinations": [
                {
                    "agent_name": "terminus-2",
                    "agent_model": {"provider": "openai", "name": "gpt-4o"},
                    "n_per_task": 1,
                },
                {
                    "agent_name": "openhands-sdk",
                    "agent_model": {"provider": "openai", "name": "gpt-4o"},
                    "n_per_task": 1,
                },
            ],
        },
    )

    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert detail.startswith("combinations[1]: agent 'openhands' is not yet runnable")
