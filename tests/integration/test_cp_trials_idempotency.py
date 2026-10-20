"""POST /trials honors `idempotency_key` (Plan 19 Task 2).

Same patterns as test_submit_trial.py (TestClient + postgres_url +
synchronous seed)."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, text, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    DataLifecycleAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    ExecutionAdmissionReservation,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    User,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed_team(postgres_url: str) -> Iterator[tuple[UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"sub-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(User).values(
            id=user_id,
            username=f"CpSubmitUser-{team_id.hex[:8]}",
            username_normalized=f"cp-submit-user-{team_id.hex[:8]}",
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            created_by_user_id=user_id,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(
            id="hello", checksum="0" * 64,
            config={
                "schema_version": "1",
                "task": {"id": "hello", "name": "hello"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
        ))
        s.commit()
    try:
        yield team_id, raw
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.username_normalized.like("cp-submit-user-%")))
            s.execute(delete(DataLifecycleGcItem))
            s.execute(delete(DataLifecycleGcRun))
            s.execute(delete(DataLifecycleObject))
            s.execute(delete(DataLifecycleAuthority))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    seed_team: tuple[UUID, str],
):  # type: ignore[no-untyped-def]
    for k, v in {
        "LOOM_ENV": "development",
        "LOOM_LOCAL_EXECUTION": "1",
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_same_idempotency_key_returns_same_trial(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    team_id, raw = seed_team
    with TestClient(app) as client:
        r1 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                "idempotency_key": "abc-123",
            },
        )
        r2 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                "idempotency_key": "abc-123",
            },
        )
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text
    assert r1.json()["trial_id"] == r2.json()["trial_id"]
    trial_id = UUID(r1.json()["trial_id"])
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            trial, authority = session.execute(
                select(Trial, DataLifecycleAuthority)
                .join(
                    DataLifecycleAuthority,
                    DataLifecycleAuthority.id == Trial.lifecycle_authority_id,
                )
                .where(Trial.id == trial_id)
            ).one()
            team_authorities = session.scalars(
                select(DataLifecycleAuthority).where(
                    DataLifecycleAuthority.team_id == team_id
                )
            ).all()
    finally:
        engine.dispose()
    assert authority.environment == "development"
    assert authority.namespace == "loom"
    assert authority.data_class == "trial"
    assert authority.owner_kind == "trial"
    assert authority.owner_id == str(trial.id)
    assert authority.created_at == trial.submitted_at
    assert authority.pinned is True
    assert authority.expires_at is None
    assert [row.id for row in team_authorities] == [authority.id]


def test_different_idempotency_keys_different_trials(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
) -> None:
    _, raw = seed_team
    with TestClient(app) as client:
        r1 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                "idempotency_key": "k1",
            },
        )
        r2 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                "idempotency_key": "k2",
            },
        )
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["trial_id"] != r2.json()["trial_id"]


def test_submit_trial_persists_required_worker_pool_capability(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    _, raw = seed_team
    with TestClient(app) as client:
        response = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {"agent_name": "oracle", "agent_model": None},
                "idempotency_key": "pool-coverage-local-worker",
                "required_worker_pool": " local-worker ",
            },
        )

    assert response.status_code == 201, response.text
    trial_id = UUID(response.json()["trial_id"])
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as session:
        trial = session.execute(
            select(Trial).where(Trial.id == trial_id),
        ).scalar_one()
    engine.dispose()

    assert trial.requires_caps["worker_pool"] == "local-worker"


def test_no_idempotency_key_creates_distinct_trials(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
) -> None:
    """Hand-submitted trials (no idempotency_key) still get distinct ids."""
    _, raw = seed_team
    with TestClient(app) as client:
        r1 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        r2 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
    assert r1.json()["trial_id"] != r2.json()["trial_id"]


def test_team_token_records_submitter_user(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    team_id, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
    assert r.status_code == 201, r.text
    trial_id = UUID(r.json()["trial_id"])

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        token_user_id = s.execute(
            select(Token.created_by_user_id).where(Token.team_id == team_id),
        ).scalar_one()
        trial_user_id = s.execute(
            select(Trial.submitted_by_user_id).where(Trial.id == trial_id),
        ).scalar_one()
    engine.dispose()

    assert trial_user_id == token_user_id


def test_unknown_batch_id_returns_400(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
) -> None:
    """Audit C2: payload batch_id that doesn't exist returns 400,
    not 500 from a downstream FK IntegrityError."""
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {"agent_name": "oracle", "agent_model": None},
                "batch_id": str(uuid4()),
            },
        )
    assert r.status_code == 400
    assert "unknown batch" in r.json()["detail"]


@pytest.mark.parametrize("terminal_state", ["cancelled", "finished"])
def test_batch_terminal_transition_fences_captured_submission(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    terminal_state: str,
) -> None:
    from loom_control_plane.routes import trials as trials_route

    team_id, raw = seed_team
    batch_id = uuid4()
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    with sessions() as session:
        session.add(Batch(
            id=batch_id, team_id=team_id, name="cancel fanout race",
            task_filter={}, trial_config={}, state="submitted", backend="docker",
            created_by_token_prefix="abcdef12", expected_trial_count=2,
        ))
        session.commit()
    payload = {
        "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
        "batch_id": str(batch_id), "idempotency_key": f"{batch_id}:existing",
    }
    headers = {"Authorization": f"Bearer {raw}"}
    validated = Event()
    original_validate = trials_route.validate_submission_agent_task_compatibility

    async def capture_submission(*args, **kwargs):  # type: ignore[no-untyped-def]
        await original_validate(*args, **kwargs)
        validated.set()

    try:
        with TestClient(app) as client:
            existing = client.post("/trials", headers=headers, json=payload)
            assert existing.status_code == 201, existing.text
            monkeypatch.setattr(
                trials_route, "validate_submission_agent_task_compatibility", capture_submission,
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                with sessions() as cancelling:
                    cancelling.execute(select(Batch).where(Batch.id == batch_id).with_for_update())
                    submission = executor.submit(
                        client.post, "/trials", headers=headers,
                        json={**payload, "idempotency_key": f"{batch_id}:late"},
                    )
                    # The request has read the active parent and passed preflight;
                    # cancellation now commits before authoritative child admission.
                    assert validated.wait(timeout=5)
                    cancelling.execute(update(Batch).where(Batch.id == batch_id).values(
                        state=terminal_state, finished_at=datetime.now(UTC),
                    ))
                    cancelling.commit()
                late = submission.result(timeout=5)
            assert late.status_code == 409, late.text
            replay = client.post("/trials", headers=headers, json=payload)
            assert replay.status_code == 201, replay.text
            assert replay.json() == existing.json()
        with sessions() as session:
            assert session.scalars(select(Trial.id).where(Trial.batch_id == batch_id)).all() == [
                UUID(existing.json()["trial_id"]),
            ]
    finally:
        engine.dispose()


@pytest.mark.parametrize("admission_kind", ["reservation", "global_selection"])
def test_concurrent_batch_replay_does_not_deadlock_scheduler_admission(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    admission_kind: str,
) -> None:
    from loom_control_plane.routes import trials as trials_route

    team_id, raw = seed_team
    batch_id = uuid4()
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    with sessions() as session:
        session.add(Batch(
            id=batch_id, team_id=team_id, name="concurrent replay admission",
            task_filter={}, trial_config={}, state="submitted", backend="docker",
            created_by_token_prefix="abcdef12", expected_trial_count=1,
        ))
        session.commit()
    payload = {
        "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
        "batch_id": str(batch_id), "idempotency_key": f"{batch_id}:concurrent",
    }
    headers = {"Authorization": f"Bearer {raw}"}
    missed_replay = Event()
    canonical_committed = Event()
    replay_waiting = Event()
    captured_request = None
    original_validate = trials_route.validate_submission_agent_task_compatibility
    original_ensure = trials_route._ensure_trial_task_image_links

    async def capture_fastpath_miss(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal captured_request
        await original_validate(*args, **kwargs)
        if captured_request is None:
            captured_request = asyncio.current_task()
            missed_replay.set()
            assert await asyncio.to_thread(canonical_committed.wait, 5)

    async def capture_canonical_replay(*args, **kwargs):  # type: ignore[no-untyped-def]
        if captured_request is asyncio.current_task():
            # The canonical replay locks its existing Trial to preserve image
            # links; the scheduler below already owns that row.
            replay_waiting.set()
        await original_ensure(*args, **kwargs)

    monkeypatch.setattr(trials_route, "validate_submission_agent_task_compatibility", capture_fastpath_miss)
    monkeypatch.setattr(trials_route, "_ensure_trial_task_image_links", capture_canonical_replay)
    reservation_error = None
    try:
        with TestClient(app) as client:
            with ThreadPoolExecutor(max_workers=1) as executor:
                replay_future = executor.submit(client.post, "/trials", headers=headers, json=payload)
                try:
                    assert missed_replay.wait(timeout=5)
                    canonical = client.post("/trials", headers=headers, json=payload)
                    assert canonical.status_code == 201, canonical.text
                    trial_id = UUID(canonical.json()["trial_id"])
                    with sessions() as scheduler:
                        # Native admission takes Batch KEY SHARE via its FK;
                        # global selection explicitly locks the parent after
                        # its existing Trial. Neither may deadlock a replay.
                        scheduler.execute(select(Trial).where(Trial.id == trial_id).with_for_update())
                        scheduler.execute(text("SET LOCAL lock_timeout = '5s'"))
                        canonical_committed.set()
                        assert replay_waiting.wait(timeout=5)
                        try:
                            if admission_kind == "reservation":
                                scheduler.execute(insert(ExecutionAdmissionReservation).values(
                                    trial_id=trial_id, attempt=1, execution_role="attempt",
                                    team_id=team_id, batch_id=batch_id, pool_id="test-replay",
                                    owner_kind="service_execution_lease", owner_id=uuid4(),
                                    acquired_at=datetime.now(UTC),
                                ))
                            else:
                                scheduler.execute(select(Batch).where(Batch.id == batch_id).with_for_update())
                            # This test owns no actual execution lease. Roll back
                            # its reservation after proving FK admission proceeds.
                            scheduler.rollback()
                        except Exception as exc:
                            reservation_error = exc
                            scheduler.rollback()
                finally:
                    canonical_committed.set()
                replay = replay_future.result(timeout=10)
            assert reservation_error is None, repr(reservation_error)
            assert replay.status_code == 201, replay.text
            assert replay.json() == canonical.json()
        with sessions() as session:
            assert session.scalars(select(Trial.id).where(Trial.batch_id == batch_id)).all() == [trial_id]
    finally:
        engine.dispose()


def test_team_token_cannot_submit_trial_for_other_team_batch(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    """A normal submit token cannot attach a trial to another team's batch.

    This is the tenant-boundary failure from issue #142: the old path only
    checked that batch_id existed, then wrote Trial.team_id from the caller's
    token.
    """
    owner_team, _owner_raw = seed_team
    runner_team = uuid4()
    runner_raw = f"loom_team_{uuid4().hex}"
    batch_id = uuid4()

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(Team).values(
            id=runner_team, name=f"runner-{runner_team}",
        ))
        s.execute(insert(TeamQuota).values(team_id=runner_team))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(runner_raw.encode()).digest(),
            type="team",
            scopes=["submit"],
            team_id=runner_team,
            issued_at=datetime.now(UTC),
        ))
        s.add(Batch(
            id=batch_id,
            team_id=owner_team,
            name="owner-batch",
            task_filter={},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        ))
        s.commit()
    engine.dispose()

    try:
        with TestClient(app) as client:
            r = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {runner_raw}"},
                json={
                    "task_id": "hello",
                    "config": {
                        "agent_name": "oracle",
                        "agent_model": None,
                    },
                    "batch_id": str(batch_id),
                    "idempotency_key": f"{batch_id}::hello",
                },
            )
        assert r.status_code == 403
        assert "batch belongs to another team" in r.json()["detail"]

        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            trial = s.execute(
                select(Trial).where(Trial.batch_id == batch_id),
            ).scalar_one_or_none()
        engine.dispose()
        assert trial is None
    finally:
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch).where(Batch.id == batch_id))
            s.execute(delete(Token).where(Token.team_id == runner_team))
            s.execute(delete(TeamQuota).where(
                TeamQuota.team_id == runner_team,
            ))
            s.execute(delete(Team).where(Team.id == runner_team))
            s.commit()
        engine.dispose()


def test_batch_submit_token_creates_trial_for_batch_team(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    """A team-less internal batch-submit token derives ownership from Batch.

    This is the intended service-runner path for multi-team deployments:
    the token authorizes fan-out, but the parent batch row decides the
    trial's tenant.
    """
    owner_team, _owner_raw = seed_team
    owner_user_id = uuid4()
    runner_raw = f"loom_w_{uuid4().hex}"
    batch_id = uuid4()

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(runner_raw.encode()).digest(),
            type="worker",
            scopes=["submit:batch"],
            team_id=None,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(User).values(
            id=owner_user_id,
            username=f"BatchRunnerOwner-{owner_team.hex[:8]}",
            username_normalized=f"batch-runner-owner-{owner_team.hex[:8]}",
            status="active",
            is_platform_admin=False,
        ))
        s.add(Batch(
            id=batch_id,
            team_id=owner_team,
            submitted_by_user_id=owner_user_id,
            name="owner-batch",
            task_filter={},
            trial_config={},
            state="submitted",
            created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        ))
        s.commit()
    engine.dispose()

    try:
        with TestClient(app) as client:
            r = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {runner_raw}"},
                json={
                    "task_id": "hello",
                    "config": {
                        "agent_name": "oracle",
                        "agent_model": None,
                    },
                    "batch_id": str(batch_id),
                    "idempotency_key": f"{batch_id}::hello",
                },
            )
        assert r.status_code == 201, r.text
        trial_id = UUID(r.json()["trial_id"])

        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            trial = s.execute(
                select(Trial).where(Trial.id == trial_id),
            ).scalar_one()
        engine.dispose()
        assert trial.team_id == owner_team
        assert trial.batch_id == batch_id
        assert trial.submitted_by_user_id == owner_user_id
    finally:
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch).where(Batch.id == batch_id))
            s.execute(delete(Token).where(Token.token_hash == (
                hashlib.sha256(runner_raw.encode()).digest()
            )))
            s.execute(delete(User).where(User.id == owner_user_id))
            s.commit()
        engine.dispose()


def test_cross_team_idempotency_key_does_not_leak(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    """Audit H1: team A submits with idempotency_key X. Team B then
    submits the same key — must NOT receive team A's trial_id."""
    _, raw_a = seed_team

    team_b = uuid4()
    user_b = uuid4()
    raw_b = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_b, name=f"b-{team_b}"))
        s.execute(insert(User).values(
            id=user_b,
            username=f"CpSubmitUserB-{team_b.hex[:8]}",
            username_normalized=f"cp-submit-user-b-{team_b.hex[:8]}",
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(TeamQuota).values(team_id=team_b))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_b.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_b,
            created_by_user_id=user_b,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()

    try:
        with TestClient(app) as client:
            r_a = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_a}"},
                json={
                    "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                    "idempotency_key": "shared-key",
                },
            )
            r_b = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_b}"},
                json={
                    "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                    "idempotency_key": "shared-key",
                },
            )
        assert r_a.status_code == 201, r_a.text
        # Team B's ON CONFLICT fires; the recovery path sees the
        # canonical row belongs to team A and 409s.
        assert r_b.status_code == 409
        assert "collision" in r_b.json()["detail"]
    finally:
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token).where(Token.team_id == team_b))
            s.execute(delete(TeamQuota).where(TeamQuota.team_id == team_b))
            s.execute(delete(User).where(User.id == user_b))
            s.execute(delete(Team).where(Team.id == team_b))
            s.commit()
        sync_engine.dispose()


def test_batch_id_stored_on_trial(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    """When `batch_id` is present in the payload, it lands on the trial row."""
    _, raw = seed_team
    # Seed a batch so the FK is satisfied.
    from loom.db.schema import Batch
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    team_id, _ = seed_team
    batch_id = uuid4()
    with sl() as s:
        s.add(Batch(
            id=batch_id, team_id=team_id, name="c",
            task_filter={}, trial_config={},
            state="submitted", created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        ))
        s.commit()
    engine.dispose()
    try:
        with TestClient(app) as client:
            r = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None},
                    "batch_id": str(batch_id),
                    "idempotency_key": f"{batch_id}::hello",
                },
            )
        assert r.status_code == 201, r.text
        trial_id = UUID(r.json()["trial_id"])
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            trial = s.execute(
                select(Trial).where(Trial.id == trial_id),
            ).scalar_one()
        engine.dispose()
        assert trial.batch_id == batch_id
        assert trial.idempotency_key == f"{batch_id}::hello"
    finally:
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.commit()
        engine.dispose()
