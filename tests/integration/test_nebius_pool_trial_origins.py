"""Only the internal batch producer inherits retained asynchronous provenance."""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select

from loom.db.schema import Batch, Token, Trial
from tests.integration.test_cp_trials_idempotency import app as app
from tests.integration.test_cp_trials_idempotency import seed_team as seed_team
from tests.unit.test_nebius_pool_submission_source import source


@pytest.mark.parametrize("internal,known_origin", [(True, True), (False, True), (True, False)])
def test_trial_fanout_keeps_server_origin_but_ordinary_batch_reference_cannot_promote(app, seed_team, postgres_url,
                                                                                  internal, known_origin):
    team_id, raw = seed_team
    body = source()
    batch_id = uuid4()
    parent_origin = {"schema_version": "loom.pool-work-origin.v1", "data_environment_id": body["data_environment_id"],
        "kind": body["kind"], "application": body["application"], "submission_id": str(batch_id)} if known_origin else None
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(insert(Batch).values(id=batch_id, team_id=team_id, name="origin-fanout",
                task_filter={}, trial_config={}, created_by_token_prefix="origin", pool_origin=parent_origin))
            if internal:
                raw = "loom_worker_" + uuid4().hex
                connection.execute(insert(Token).values(token_hash=hashlib.sha256(raw.encode()).digest(),
                    type="worker", scopes=["submit:batch"], team_id=None, issued_at=datetime.now(UTC)))
        spoofed = {**(parent_origin or {}), "kind": "environment", "application": None}
        with TestClient(app) as client:
            payload = {"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                       "batch_id": str(batch_id), "idempotency_key": "origin-" + uuid4().hex, "pool_origin": spoofed}
            response = client.post("/trials", json=payload, headers={"Authorization": "Bearer " + raw})
            assert response.status_code == 201, response.text
            trial_id = UUID(response.json()["trial_id"])
            replay = client.post("/trials", json=payload | {"pool_origin": source()}, headers={"Authorization": "Bearer " + raw})
            assert replay.json()["trial_id"] == str(trial_id)
        with engine.connect() as connection:
            actual = connection.execute(select(Trial.pool_origin).where(Trial.id == trial_id)).scalar_one()
        assert actual == (parent_origin if internal else None)
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Trial).where(Trial.batch_id == batch_id))
            connection.execute(delete(Batch).where(Batch.id == batch_id))
        engine.dispose()
