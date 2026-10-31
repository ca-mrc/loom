"""Request pools reject incompatible work before forwarding or batch creation."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from loom.db.schema import (
    Batch,
    ProviderConnection,
    ProviderConnectionShare,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolMember,
)
from loom_service.provider_pool_validation import provider_submission_configs
from tests.integration.test_trial_batch_provider_connection_id import app_setup as app_setup

_PRIMARY = "gpt-4o"
_TEACHER = "gpt-4o-mini"


@pytest.fixture
def pool_setup(app_setup: tuple) -> Iterator[tuple[Any, dict, dict, list[httpx.Request]]]:
    app, tokens, ids = app_setup
    engine = create_engine(str(app.state.settings.db_url))
    with Session(engine) as session:
        session.execute(
            update(ProviderConnection)
            .where(
                ProviderConnection.id.in_([ids["conn_a"], ids["conn_b"]]),
            )
            .values(status="valid")
        )
        for cid in [ids["conn_a"], ids["conn_b"]]:
            for model in [_PRIMARY, _TEACHER]:
                session.execute(
                    pg_insert(ProviderModelCache)
                    .values(
                        provider_connection_id=cid,
                        model_id=model,
                        upstream_present=True,
                        visible=True,
                    )
                    .on_conflict_do_nothing()
                )
        session.execute(
            insert(ProviderConnectionShare).values(
                provider_connection_id=ids["conn_b"],
                target_team_id=ids["team_a"],
                created_by_actor="test:pool",
            )
        )
        session.execute(
            insert(ProviderRequestPool).values(
                connection_id=ids["conn_a"],
                required_models=[_PRIMARY, _TEACHER],
            )
        )
        for ordinal, cid in enumerate([ids["conn_a"], ids["conn_b"]]):
            session.execute(
                insert(ProviderRequestPoolMember).values(
                    pool_connection_id=ids["conn_a"],
                    connection_id=cid,
                    ordinal=ordinal,
                    quota_group=f"quota-{ordinal}",
                )
            )
        session.commit()
    captures: list[httpx.Request] = []

    def upstream(request: httpx.Request) -> httpx.Response:
        captures.append(request)
        return httpx.Response(
            201,
            json={
                "trial_id": "00000000-0000-0000-0000-000000000099",
                "state": "queued",
            },
        )

    # Replacing only the disposable CP mock lets rejection tests prove no
    # Trial submission was forwarded; no real model or hosted work is sent.
    old_client = app.state.http_client
    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream), base_url="http://cp"
    )
    try:
        yield app, tokens, ids, captures
    finally:
        # The imported owning fixture closes the replacement client.
        import asyncio

        asyncio.run(old_client.aclose())
        engine.dispose()


def _config(agent: str = "terminus-2", *, teacher: bool = False) -> dict[str, Any]:
    config: dict[str, Any] = {
        "agent_name": agent,
        "agent_model": {"provider": "openai", "name": _PRIMARY},
    }
    if teacher:
        config["multi_model"] = {
            "enabled": True,
            "policy": "student_teacher_student",
            "secondary_model": {"provider": "openai", "name": _TEACHER},
            "switch_episode": 2,
            "return_switch_episode": 4,
        }
    return config


def _submit(setup: tuple, surface: str, config: dict[str, Any]) -> httpx.Response:
    app, tokens, ids, _captures = setup
    body = {
        "provider_connection_id": str(ids["conn_a"]),
        "provider_model_id": _PRIMARY,
    }
    if surface == "trials":
        body.update(task_id=ids["task_id"], config=config)
    else:
        body.update(
            name="pool-admission",
            purpose="evaluation",
            task_filter={"task_ids": [ids["task_id"]]},
            trial_config=config,
        )
    return TestClient(app).post(
        f"/api/v1/{surface}",
        headers={"Authorization": f"Bearer {tokens['a']}"},
        json=body,
    )


def _mutate(setup: tuple, statement: Any) -> None:
    engine = create_engine(str(setup[0].state.settings.db_url))
    with engine.begin() as connection:
        connection.execute(statement)
    engine.dispose()


@pytest.mark.parametrize("surface", ["trials", "batches"])
def test_pool_submits_with_complete_student_teacher_set(pool_setup: tuple, surface: str) -> None:
    response = _submit(pool_setup, surface, _config(teacher=True))
    assert response.status_code == 201, response.text
    if surface == "trials":
        assert len(pool_setup[3]) == 1
    else:
        engine = create_engine(str(pool_setup[0].state.settings.db_url))
        with Session(engine) as session:
            batch = session.get(Batch, UUID(response.json()["batch_id"]))
            assert batch.provider_connection_id == pool_setup[2]["conn_a"]
        engine.dispose()


@pytest.mark.parametrize("surface", ["trials", "batches"])
def test_pool_rejects_other_agents_before_creating_work(pool_setup: tuple, surface: str) -> None:
    response = _submit(pool_setup, surface, _config("direct-completion"))
    assert response.status_code == 400, response.text
    assert "terminus-2" in response.text
    assert not pool_setup[3]
    engine = create_engine(str(pool_setup[0].state.settings.db_url))
    with Session(engine) as session:
        assert not session.scalars(
            select(Batch.id).where(Batch.team_id == pool_setup[2]["team_a"])
        ).all()
    engine.dispose()


def test_pool_requires_teacher_in_declared_set(pool_setup: tuple) -> None:
    _mutate(
        pool_setup,
        update(ProviderRequestPool)
        .where(
            ProviderRequestPool.connection_id == pool_setup[2]["conn_a"],
        )
        .values(required_models=[_PRIMARY]),
    )
    response = _submit(pool_setup, "trials", _config(teacher=True))
    assert response.status_code == 400
    assert _TEACHER in response.text
    assert not pool_setup[3]


@pytest.mark.parametrize("change", ["hidden", "absent", "allowed", "invalid", "unshared"])
def test_pool_revalidates_every_member_complete_model_set(pool_setup: tuple, change: str) -> None:
    ids = pool_setup[2]
    if change in {"hidden", "absent"}:
        statement = (
            update(ProviderModelCache)
            .where(
                ProviderModelCache.provider_connection_id == ids["conn_b"],
                ProviderModelCache.model_id == _TEACHER,
            )
            .values(**({"visible": False} if change == "hidden" else {"upstream_present": False}))
        )
    elif change == "allowed":
        statement = (
            update(ProviderConnection)
            .where(ProviderConnection.id == ids["conn_b"])
            .values(allowed_models=[_PRIMARY])
        )
    elif change == "invalid":
        statement = (
            update(ProviderConnection)
            .where(ProviderConnection.id == ids["conn_b"])
            .values(status="invalid")
        )
    else:
        statement = delete(ProviderConnectionShare).where(
            ProviderConnectionShare.provider_connection_id == ids["conn_b"]
        )
    _mutate(pool_setup, statement)
    response = _submit(pool_setup, "trials", _config())
    assert response.status_code == (404 if change == "unshared" else 400), response.text
    assert not pool_setup[3]


@pytest.mark.parametrize(
    "control",
    [{"stream": True}, {"use_responses_api": True}, {"previous_response_id": "response-1"}],
)
def test_pool_rejects_legacy_stateful_config(pool_setup: tuple, control: dict) -> None:
    response = _submit(pool_setup, "trials", {**_config(), **control})
    assert response.status_code == 400, response.text
    assert "provider request pools" in response.text
    assert not pool_setup[3]


def test_disabled_pool_preserves_ordinary_connection_submission(pool_setup: tuple) -> None:
    _mutate(
        pool_setup,
        update(ProviderRequestPool)
        .where(
            ProviderRequestPool.connection_id == pool_setup[2]["conn_a"],
        )
        .values(enabled=False),
    )
    response = _submit(pool_setup, "trials", _config("direct-completion"))
    assert response.status_code == 201, response.text


def test_rerun_selection_configs_keep_teacher_and_only_selected_combinations() -> None:
    from uuid import uuid4

    connection_id, other = uuid4(), uuid4()
    trial_config = _config(teacher=True)
    combinations = [
        {
            "agent_name": "direct-completion",
            "agent_model": {"name": "ignored"},
            "provider_connection_id": str(connection_id),
        },
        {
            "agent_name": "terminus-2",
            "agent_model": {"name": _PRIMARY},
            "provider_connection_id": str(connection_id),
        },
        {
            "agent_name": "terminus-2",
            "agent_model": {"name": "other"},
            "provider_connection_id": str(other),
        },
    ]
    configs = provider_submission_configs(
        trial_config=trial_config,
        combinations=combinations,
        connection_id=connection_id,
        indices={1},
    )
    assert len(configs) == 1
    assert configs[0]["agent_name"] == "terminus-2"
    assert configs[0]["agent_model"]["name"] == _PRIMARY
    assert configs[0]["multi_model"] == trial_config["multi_model"]
