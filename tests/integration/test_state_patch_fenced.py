import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    EpisodeCheckpoint,
    Task,
    Team,
    TeamQuota,
    TerminusAgentExecution,
    TerminusAgentRunAttempt,
    Token,
    Trial,
    Worker,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def state_seed(postgres_url: str) -> Iterator[tuple[UUID, UUID, UUID, str, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_a = uuid4()
    worker_b = uuid4()
    trial_id = uuid4()
    raw_a = f"wa_{uuid4().hex}"
    raw_b = f"wb_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        for raw in (raw_a, raw_b):
            s.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw.encode()).digest(),
                    type="worker",
                    scopes=["worker:report"],
                    team_id=None,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
        for wid in (worker_a, worker_b):
            s.execute(
                insert(Worker).values(
                    id=wid,
                    hostname="h",
                    version="v",
                    capabilities=[],
                    registered_at=datetime.now(UTC),
                    last_seen_at=datetime.now(UTC),
                    status="active",
                )
            )
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(
            insert(Trial).values(
                id=trial_id,
                team_id=team_id,
                task_id="t",
                config={},
                requires_caps={},
                state="claimed",
                worker_id=worker_a,
            )
        )
        s.commit()
    try:
        yield trial_id, worker_a, worker_b, raw_a, raw_b
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    state_seed: tuple[UUID, UUID, UUID, str, str],
):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_state_patch_with_matching_worker(app, state_seed):  # type: ignore[no-untyped-def]
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "running"},
        )
        assert r.status_code == 200
        assert r.json()["state"] == "running"


def test_state_patch_with_wrong_worker_fenced(app, state_seed):  # type: ignore[no-untyped-def]
    trial_id, _, worker_b, _, raw_b = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_b}"},
            json={"worker_id": str(worker_b), "state": "running"},
        )
        assert r.status_code == 409
        assert "claim" in r.json()["detail"].lower()


@pytest.mark.parametrize(
    "identity", ["matching", "other_trial", "other_execution", "finished_attempt"]
)
def test_episode_checkpoint_requires_the_locked_trials_running_execution(
    app,
    state_seed,
    postgres_url,
    identity,
):  # type: ignore[no-untyped-def]
    trial_id, _, _, raw, _ = state_seed
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    other_trial_id, execution_id, other_execution_id = uuid4(), uuid4(), uuid4()
    attempt_id, other_attempt_id = uuid4(), uuid4()
    try:
        with sessions.begin() as session:
            trial = session.get(Trial, trial_id)
            session.add(
                Trial(
                    id=other_trial_id,
                    team_id=trial.team_id,
                    task_id=trial.task_id,
                    config={},
                    requires_caps={},
                    state="queued",
                )
            )
            session.flush()
            session.add_all(
                [
                    TerminusAgentExecution(id=execution_id, trial_id=trial_id, step_id="agent"),
                    TerminusAgentExecution(
                        id=other_execution_id, trial_id=other_trial_id, step_id="agent"
                    ),
                ]
            )
            session.flush()
            session.add_all(
                [
                    TerminusAgentRunAttempt(
                        id=attempt_id,
                        execution_id=execution_id,
                        attempt_number=1,
                        state="failed" if identity == "finished_attempt" else "running",
                    ),
                    TerminusAgentRunAttempt(
                        id=other_attempt_id,
                        execution_id=other_execution_id,
                        attempt_number=1,
                        state="running",
                    ),
                ]
            )
        # A different active Trial URL must not bypass the queued Trial's fence.
        with TestClient(app) as client:
            response = client.post(
                f"/trials/{trial_id}/terminus/episode-checkpoints",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "execution_id": str(
                        other_execution_id if identity == "other_trial" else execution_id
                    ),
                    "run_attempt_id": str(
                        other_attempt_id
                        if identity in {"other_trial", "other_execution"}
                        else attempt_id
                    ),
                    "episode": 1,
                    "active_role": "student",
                    "last_call_ordinal": 1,
                    "last_seq": 1,
                },
            )
        assert response.status_code == (200 if identity == "matching" else 409), response.text
        with sessions() as session:
            checkpoints = list(
                session.scalars(
                    select(EpisodeCheckpoint).where(
                        EpisodeCheckpoint.execution_id.in_((execution_id, other_execution_id)),
                    )
                )
            )
            assert len(checkpoints) == (1 if identity == "matching" else 0)
            if checkpoints:
                assert checkpoints[0].execution_id == execution_id
                assert checkpoints[0].run_attempt_id == attempt_id
    finally:
        engine.dispose()


def test_state_patch_terminal_state(
    app,
    state_seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    """Transition to a terminal state should set finished_at."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    result_payload = {"aggregate_reward": 1.0}
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "state": "succeeded",
                "result": result_payload,
            },
        )
        assert r.status_code == 200
        assert r.json()["state"] == "succeeded"
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            row = s.get(Trial, trial_id)
            assert row is not None
            assert row.result == result_payload
    finally:
        engine.dispose()


def test_family_skip_cancels_sibling_through_cancellation_authority(
    app,
    state_seed,
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
):  # type: ignore[no-untyped-def]
    trial_id, worker_a, _, raw_a, _ = state_seed
    batch_id = uuid4()
    sibling_id = uuid4()
    family_spec = {
        "enabled": True,
        "family_key_extractor": {"name": "instance_id_prefix", "params": {}},
        "sequencer": {"name": "alphabetical", "params": {}},
        "advance_predicate": {"name": "always_on_terminal", "params": {}},
        "adapter": {"name": "noop", "params": {}},
        "failure_policy": {"name": "stall_family", "params": {}},
        "state_backend": {"name": "s3_artifacts", "params": {}},
        "mount_path": "/root/.skills",
    }

    class _SkipPredicate:
        def decide(self, **_kwargs):  # type: ignore[no-untyped-def]
            from loom.family_run.spec import AdvanceDecision

            return AdvanceDecision.SKIP

    monkeypatch.setattr(
        "loom_control_plane.routes.state.resolve_plugin",
        lambda _group, _ref: _SkipPredicate(),
    )
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            team_id = connection.execute(
                text("SELECT team_id FROM trials WHERE id = :trial_id"),
                {"trial_id": trial_id},
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO batches "
                    "(id, team_id, name, purpose, task_filter, trial_config, state, "
                    "created_by_token_prefix, family_run_spec) VALUES "
                    "(:batch_id, :team_id, :name, 'evaluation', '{}'::jsonb, '{}'::jsonb, "
                    "'running', 'family', CAST(:spec AS jsonb))"
                ),
                {
                    "batch_id": batch_id,
                    "team_id": team_id,
                    "name": f"state-family-{batch_id}",
                    "spec": json.dumps(family_spec),
                },
            )
            connection.execute(
                text(
                    "UPDATE trials SET batch_id = :batch_id, family_key = 'fam' "
                    "WHERE id = :trial_id"
                ),
                {"batch_id": batch_id, "trial_id": trial_id},
            )
            connection.execute(
                insert(Trial).values(
                    id=sibling_id,
                    team_id=team_id,
                    task_id="t",
                    config={},
                    requires_caps={},
                    state="queued",
                    batch_id=batch_id,
                    family_key="fam",
                )
            )
            connection.execute(
                text(
                    "INSERT INTO batch_family_state "
                    "(batch_id, family_key, task_sequence, current_index, state, "
                    "attempt_count) VALUES "
                    "(:batch_id, 'fam', ARRAY['t', 't'], 0, 'running', 0)"
                ),
                {"batch_id": batch_id},
            )

        from loom_control_plane.routes import state as state_routes

        authoritative_cancel = state_routes.cancel_trial_under_authority

        async def cancel_after_family_is_non_runnable(**kwargs):  # type: ignore[no-untyped-def]
            with engine.connect() as connection:
                family_state = connection.execute(
                    text(
                        "SELECT state FROM batch_family_state "
                        "WHERE batch_id = :batch_id AND family_key = 'fam'"
                    ),
                    {"batch_id": batch_id},
                ).scalar_one()
            assert family_state == "cancelling"
            return await authoritative_cancel(**kwargs)

        monkeypatch.setattr(
            state_routes,
            "cancel_trial_under_authority",
            cancel_after_family_is_non_runnable,
        )

        with TestClient(app) as client:
            response = client.patch(
                f"/trials/{trial_id}/state",
                headers={"Authorization": f"Bearer {raw_a}"},
                json={"worker_id": str(worker_a), "state": "cancelled"},
            )
        assert response.status_code == 200, response.text
        with engine.connect() as connection:
            sibling = (
                connection.execute(
                    text(
                        "SELECT state, cancellation_requested_at IS NOT NULL AS requested, "
                        "cancellation_observed_at IS NOT NULL AS observed, "
                        "finished_at IS NOT NULL AS finished FROM trials "
                        "WHERE id = :trial_id"
                    ),
                    {"trial_id": sibling_id},
                )
                .mappings()
                .one()
            )
        assert dict(sibling) == {
            "state": "cancelled",
            "requested": True,
            "observed": True,
            "finished": True,
        }
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT state FROM batch_family_state "
                        "WHERE batch_id = :batch_id AND family_key = 'fam'"
                    ),
                    {"batch_id": batch_id},
                ).scalar_one()
                == "pending"
            )
    finally:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE trials SET batch_id = NULL, family_key = NULL "
                    "WHERE batch_id = :batch_id"
                ),
                {"batch_id": batch_id},
            )
            connection.execute(
                text("DELETE FROM batch_family_state WHERE batch_id = :batch_id"),
                {"batch_id": batch_id},
            )
            connection.execute(
                text("DELETE FROM batches WHERE id = :batch_id"),
                {"batch_id": batch_id},
            )
        engine.dispose()


def test_state_patch_rejects_terminal_result_conflict(
    app,
    state_seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        response = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "state": "succeeded",
                "result": {
                    "state": "failed",
                    "failure_reason": "verifier_error",
                    "aggregate_reward": 1.0,
                },
            },
        )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "trial_terminal_result_inconsistent"
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            trial = session.get(Trial, trial_id)
            assert trial is not None
            assert trial.state == "claimed"
            assert trial.result is None
    finally:
        engine.dispose()


def test_state_patch_rejects_persisted_success_failure_reason(
    app,
    state_seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    trial_id, worker_a, _, raw_a, _ = state_seed
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            trial = session.get(Trial, trial_id)
            assert trial is not None
            trial.result = {
                "state": "succeeded",
                "failure_reason": "verifier_error",
                "aggregate_reward": 1.0,
            }
            session.commit()
        with TestClient(app) as client:
            response = client.patch(
                f"/trials/{trial_id}/state",
                headers={"Authorization": f"Bearer {raw_a}"},
                json={"worker_id": str(worker_a), "state": "succeeded"},
            )
        assert response.status_code == 422
        assert response.json()["detail"]["code"] == ("trial_terminal_result_inconsistent")
    finally:
        engine.dispose()


def test_state_patch_accepts_explicit_unscored_collection(
    app,
    state_seed,
    postgres_url,
):  # type: ignore[no-untyped-def]
    trial_id, worker_a, _, raw_a, _ = state_seed
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            trial = session.get(Trial, trial_id)
            assert trial is not None
            trial.config = {"skip_verifier": True}
            session.commit()
    finally:
        engine.dispose()
    with TestClient(app) as client:
        response = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "state": "succeeded",
                "result": {"state": "succeeded", "reward": None},
            },
        )
    assert response.status_code == 200


def test_state_patch_rejects_succeeded_without_result(
    app,
    state_seed,
):  # type: ignore[no-untyped-def]
    """A bare succeeded PATCH must fail before the DB CHECK constraint.

    Migration 0039 makes `state='succeeded'` require `result IS NOT NULL`.
    The API contract should return a clear 4xx for workers that try to
    report success before writing/providing result.
    """
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app, raise_server_exceptions=False) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "succeeded"},
        )
        assert 400 <= r.status_code < 500
        assert "result" in r.json()["detail"].lower()


def test_state_patch_rejects_invalid_state(app, state_seed):  # type: ignore[no-untyped-def]
    """Bug 1 regression: garbage state strings rejected with 400."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "bogus"},
        )
        assert r.status_code == 400
        assert "invalid state" in r.json()["detail"]


def test_state_patch_rejects_invalid_failure_reason(app, state_seed):  # type: ignore[no-untyped-def]
    """Bug 1 regression: failure_reason validated against enum."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "state": "failed",
                "failure_reason": "made_up",
            },
        )
        assert r.status_code == 400
        assert "failure_reason" in r.json()["detail"]


def test_state_patch_rejects_succeeded_when_queued(  # type: ignore[no-untyped-def]
    app,
    state_seed,
    postgres_url,
):
    """Bug 4 regression: succeeded only from claimed/running, not queued.

    We mutate the seeded trial's state back to 'queued' and confirm the PATCH
    to 'succeeded' is refused with 409 even though the fencing predicate
    matches the worker.
    """
    from sqlalchemy import create_engine, text

    trial_id, worker_a, _, raw_a, _ = state_seed
    eng = create_engine(postgres_url)
    with eng.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state = 'queued' WHERE id = :id"),
            {"id": trial_id},
        )
    eng.dispose()
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "succeeded"},
        )
        assert r.status_code == 409


def test_state_patch_rejects_unreachable_target(app, state_seed):  # type: ignore[no-untyped-def]
    """Bug 4 regression: targets not in _ALLOWED_FROM (e.g. 'queued',
    'claimed') cannot be reached via PATCH and must return 400."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "queued"},
        )
        assert r.status_code == 400
        assert "cannot be reached" in r.json()["detail"]
