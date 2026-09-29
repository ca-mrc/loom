"""Repaired legacy defaults are persisted at the real Trial submission boundary."""
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from loom.db.schema import Task, Trial
from tests.integration.test_submit_trial import app, seed_team  # noqa: F401


@pytest.mark.parametrize("override,marker,expected", [
    (None, "0" * 64, "separate"),
    ("shared", "0" * 64, "shared"),
    ("separate", "0" * 64, "separate"),
    (None, "1" * 64, None),
    (None, None, None),
])
def test_submit_persists_revision_bound_legacy_default(
    app, seed_team, postgres_url, override, marker, expected,  # noqa: F811
):
    engine = create_engine(postgres_url)
    try:
        with Session(engine) as session:
            task = session.get(Task, "hello")
            task.config = {**task.config, "verifier": {"name": "pytest", "env_mode": "shared"}}
            task.legacy_separate_verifier_checksum = marker
            snapshot = task.config
            session.commit()
        config = {"agent_name": "oracle", "agent_model": None}
        if override is not None:
            config["verifier_env_mode"] = override
        with TestClient(app) as client:
            response = client.post(
                "/trials", headers={"Authorization": f"Bearer {seed_team[1]}"},
                json={"task_id": "hello", "config": config},
            )
        assert response.status_code == 201, response.text
        with Session(engine) as session:
            trial = session.get(Trial, UUID(response.json()["trial_id"]))
            assert trial.config.get("verifier_env_mode") == expected
            assert session.get(Task, "hello").config == snapshot
    finally:
        engine.dispose()
