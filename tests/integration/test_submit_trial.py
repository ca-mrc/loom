import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, func, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    Benchmark,
    DataLifecycleAuthority,
    Task,
    TaskImageMaterialization,
    TaskSet,
    Team,
    TeamQuota,
    Token,
    Trial,
    TrialTaskImageMaterialization,
    User,
)
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from tests.support.execution_image_admission import signed_image_admission_bundle


@pytest.fixture
def seed_team(postgres_url: str) -> Iterator[tuple[UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4()
    username = f"TrialSubmitter-{user_id.hex[:8]}"
    raw = f"loom_team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"sub-{team_id}"))
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username.casefold(),
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.execute(
            insert(Task).values(
                id="hello",
                checksum="0" * 64,
                config={
                    "schema_version": "1",
                    "task": {"id": "hello", "name": "hello"},
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
            )
        )
        s.execute(
            insert(Task).values(
                id="broken-config",
                checksum="1" * 64,
                config={},
            )
        )
        s.execute(
            insert(Task).values(
                id="dockerfile-any",
                checksum="sha256:" + "2" * 64,
                source="s3://loom-tasks/dockerfile-any",
                config={
                    "schema_version": "1",
                    "task": {"id": "dockerfile-any", "name": "dockerfile-any"},
                    "environment": {
                        "os": "linux",
                        "cpu_arch": "any",
                        "dockerfile": "environment/Dockerfile",
                    },
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
            )
        )
        s.commit()
    try:
        yield team_id, raw
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id == "dockerfile-any"
                )
            )
            s.execute(delete(Token))
            s.execute(
                delete(DataLifecycleAuthority).where(
                    DataLifecycleAuthority.team_id == team_id,
                ),
            )
            s.execute(delete(Task))
            s.execute(delete(TaskSet))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.id == user_id))
            s.execute(delete(Team))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    seed_team: tuple[UUID, str],
):
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


def test_submit_creates_trial(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text
        body = r.json()
        assert "trial_id" in body
        assert body["state"] == "queued"
        assert "submitted_at" in body


@pytest.mark.parametrize(
    ("agent_name", "separate_task_image", "admit_task_image", "has_controller", "expected_status", "web_egress", "web_ready", "guest_requested", "guest_ready"),
    [
        ("direct-completion", False, True, False, 201, False, False, False, False),
        ("terminus-2", True, True, True, 201, False, False, False, False),
        ("terminus-2", True, False, True, 400, False, False, False, False),
        ("terminus-2", True, True, False, 400, False, False, False, False),
        ("direct-completion", True, True, True, 400, False, False, False, False),
        ("terminus-2", True, True, True, 201, True, True, False, False),
        ("terminus-2", True, True, True, 400, True, False, False, False),
        ("terminus-2", False, True, True, 201, False, False, True, True),
        ("terminus-2", False, True, True, 400, False, False, True, False),
    ],
)
@pytest.mark.parametrize("dockerfile", [False, True], ids=["prebuilt", "dockerfile"])
def test_submit_ordinary_task_into_nebius_batch_uses_automatic_pool_binding(
    app,
    seed_team: tuple[UUID, str],
    postgres_url: str,
    agent_name: str,
    separate_task_image: bool,
    admit_task_image: bool,
    has_controller: bool,
    expected_status: int,
    dockerfile: bool,
    web_egress: bool,
    web_ready: bool,
    guest_requested: bool,
    guest_ready: bool,
) -> None:
    team_id, raw = seed_team
    if dockerfile:
        expected_status = 201 if agent_name == "terminus-2" and has_controller else 400
    if (web_egress and not web_ready) or (guest_requested and not guest_ready):
        expected_status = 400
    task_id = "automatic-nebius-submit"
    batch_id = uuid4()
    task_image = "registry.example/task@sha256:" + "a" * 64
    runtime_image = "registry.example/runtime@sha256:" + "b" * 64
    default_task_image = task_image
    controller_image = "registry.example/worker@sha256:" + "9" * 64
    if separate_task_image:
        task_image = "registry.example/terminal-bench@sha256:" + "f" * 64
    admitted_images = [default_task_image, runtime_image]
    if admit_task_image and separate_task_image:
        admitted_images.append(task_image)
    if has_controller:
        admitted_images.append(controller_image)
    profile = ServiceExecutionRuntimeProfileV1(
        candidate_sha="1" * 40,
        execution_class_id="linux-amd64-cpu-web-pod-v1" if web_ready else "linux-amd64-cpu-pod-v1",
        supports_task_web_egress=web_ready,
        guest_runtime="qemu-tcg-v1" if guest_ready else None,
        runtime_volume_mib=1024 if guest_ready else 32,
        supports_task_identity=guest_ready,
        task_image_ref=default_task_image,
        agent_image_ref=controller_image if has_controller else None,
        runtime_image_ref=runtime_image,
        runtime_binary_sha256="sha256:" + "e" * 64,
        image_admission=signed_image_admission_bundle(tuple(admitted_images)),
    )
    profile_json = json.dumps(
        profile.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    engine = create_engine(postgres_url)
    sessions = sessionmaker(engine)
    try:
        with sessions() as session:
            session.add(
                Task(
                    id=task_id,
                    checksum="c" * 64,
                    source="s3://artifacts/task-inputs/task/",
                    source_provenance={
                        "service_execution_input": {
                            "schema_version": "loom.service-execution-input.v1",
                            "manifest_uri": "s3://artifacts/task-inputs/task.json",
                            "manifest_sha256": "sha256:" + "d" * 64,
                            "file_count": 3,
                            "total_bytes": 4096,
                        }
                    },
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "cpu_arch": "any" if dockerfile else "x86_64",
                            "gpu_vendor": "none",
                            **({"dockerfile": "environment/Dockerfile"} if dockerfile else {
                                "docker_image": task_image,
                            }),
                            **({"user": "root", "execution_requirements": {
                                "capabilities": ["nested_docker"],
                            }} if guest_requested else {}),
                            "cpus": 1,
                            "memory_mb": 1024,
                            "storage_mb": 2048,
                            "baseline_network_policy": ({"kind": "web-allowlist", "destinations": [
                                {"host": "registry.npmjs.org", "protocol": "https"},
                            ]} if web_egress else {"kind": "gateway-only"}),
                            "network_policies_supported": ["web-allowlist" if web_egress else "gateway-only"],
                        },
                        "agent": {"name": agent_name},
                        "verifier": {
                            "name": "script",
                            "args": {"script_path": "verifier/check.sh"},
                            **({"user": "root"} if guest_requested else {}),
                        },
                        "steps": [
                            {
                                "name": "main",
                                "instruction_file": "instruction.md",
                                "artifacts": ["answer.txt"],
                            }
                        ],
                    },
                )
            )
            session.add(
                Batch(
                    id=batch_id,
                    team_id=team_id,
                    name="automatic Nebius submit",
                    task_filter={},
                    trial_config={},
                    backend="nebius",
                    service_execution_runtime_profile=profile.model_dump(mode="json"),
                    state="submitted",
                    created_by_token_prefix="test",
                    expected_trial_count=1,
                )
            )
            session.commit()

        with TestClient(app) as client:
            app.state.settings = app.state.settings.model_copy(
                update={"service_execution_runtime_profile_json": profile_json}
            )
            response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "task_id": task_id,
                    "batch_id": str(batch_id),
                    "config": {
                        "agent_name": agent_name,
                        "agent_model": {
                            "provider": "openai",
                            "name": "gpt-5",
                            "source": "api",
                        },
                    },
                },
            )

        assert response.status_code == expected_status, response.text
        if expected_status != 201:
            with sessions() as session:
                assert session.scalar(
                    select(func.count()).select_from(Trial).where(Trial.batch_id == batch_id)
                ) == 0
            return
        with sessions() as session:
            trial = session.get(Trial, UUID(response.json()["trial_id"]))
            assert trial is not None
            assert trial.requires_caps["backend"] == "nebius"
            assert trial.requires_caps["worker_pool"] == "nebius-cpu"
            assert trial.requires_caps["network_policies"] == ["web-allowlist" if web_egress else "gateway-only"]
            assert trial.attempt_count == 0
            assert trial.state == "queued"
            if dockerfile:
                prerequisites = session.scalars(
                    select(TaskImageMaterialization).join(TrialTaskImageMaterialization).where(
                        TrialTaskImageMaterialization.trial_id == trial.id,
                    )
                ).all()
                assert {row.cpu_arch for row in prerequisites} == {"x86_64"}
                assert all(row.state == "queued" for row in prerequisites)
    finally:
        with sessions() as session:
            session.execute(delete(Trial).where(Trial.batch_id == batch_id))
            session.execute(delete(Batch).where(Batch.id == batch_id))
            session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.task_id == task_id,
            ))
            session.execute(delete(Task).where(Task.id == task_id))
            session.commit()
        engine.dispose()


def test_submit_enqueues_and_links_each_required_task_image(
    app,
    seed_team,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        response = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "dockerfile-any",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )

    assert response.status_code == 201, response.text
    trial_id = UUID(response.json()["trial_id"])
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            materializations = session.scalars(
                select(TaskImageMaterialization)
                .join(
                    TrialTaskImageMaterialization,
                    TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
                )
                .where(TrialTaskImageMaterialization.trial_id == trial_id)
                .order_by(TaskImageMaterialization.cpu_arch.desc())
            ).all()
        assert [row.cpu_arch for row in materializations] == ["x86_64"]
        assert all(row.state == "queued" for row in materializations)
        assert all(row.task_checksum == "2" * 64 for row in materializations)
    finally:
        engine.dispose()


def test_idempotent_resubmission_repairs_missing_task_image_links(
    app,
    seed_team,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    idempotency_key = f"task-image-repair-{uuid4()}"
    request_json = {
        "task_id": "dockerfile-any",
        "idempotency_key": idempotency_key,
        "config": {"agent_name": "oracle", "agent_model": None},
    }
    with TestClient(app) as client:
        first = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json=request_json,
        )
    assert first.status_code == 201, first.text
    trial_id = UUID(first.json()["trial_id"])

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            session.execute(
                delete(TrialTaskImageMaterialization).where(
                    TrialTaskImageMaterialization.trial_id == trial_id
                )
            )
            session.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id == "dockerfile-any"
                )
            )
            session.commit()

        with TestClient(app) as client:
            second = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw}"},
                json=request_json,
            )
        assert second.status_code == 201, second.text
        assert UUID(second.json()["trial_id"]) == trial_id

        with sessionmaker(engine)() as session:
            repaired_architectures = session.scalars(
                select(TaskImageMaterialization.cpu_arch)
                .join(
                    TrialTaskImageMaterialization,
                    TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
                )
                .where(TrialTaskImageMaterialization.trial_id == trial_id)
                .order_by(TaskImageMaterialization.cpu_arch)
            ).all()
        assert repaired_architectures == ["x86_64"]
    finally:
        engine.dispose()


def test_submit_prebuilt_task_creates_no_task_image_links(
    app,
    seed_team,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        response = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )

    assert response.status_code == 201, response.text
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            link_count = len(
                session.scalars(
                    select(TrialTaskImageMaterialization).where(
                        TrialTaskImageMaterialization.trial_id == UUID(response.json()["trial_id"])
                    )
                ).all()
            )
            materialization_count = len(
                session.scalars(
                    select(TaskImageMaterialization).where(
                        TaskImageMaterialization.task_id == "hello"
                    )
                ).all()
            )
        assert link_count == 0
        assert materialization_count == 0
    finally:
        engine.dispose()


def test_submit_rejects_legacy_team_token_without_user_owner(
    app,
    seed_team,
    postgres_url: str,  # type: ignore[no-untyped-def]
) -> None:
    team_id, _raw = seed_team
    legacy_raw = f"loom_team_{uuid4().hex}"
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        s.commit()
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {legacy_raw}"},
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 403
        assert "legacy team token" in r.json()["detail"]


def test_submit_rejects_unknown_task(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "nope", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 404


def test_submit_hides_foreign_private_task_set_task(
    app,
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    _team_id, raw = seed_team
    foreign_team_id = uuid4()
    task_set_id = f"ts/{foreign_team_id}/private-source"
    task_id = f"{task_set_id}/tasks/row-1"
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        session.execute(
            insert(Team).values(
                id=foreign_team_id,
                name=f"foreign-{foreign_team_id}",
            ),
        )
        session.execute(
            insert(TaskSet).values(
                id=task_set_id,
                owning_team_id=foreign_team_id,
                slug="private-source",
                display_name="Private Source",
                status="ready",
                intents=["trajectory_generation"],
                evaluation_ready=False,
                task_count=1,
                manifest_blob_uri=(
                    f"s3://bucket/tasksets/user/{foreign_team_id}/private-source/manifest.yaml"
                ),
            ),
        )
        session.execute(
            insert(Task).values(
                id=task_id,
                checksum="f" * 64,
                config={
                    "schema_version": "1",
                    "required_agent_capabilities": ["workspace_exec"],
                    "task": {"id": task_id, "name": task_id},
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "script"},
                    "steps": [{"name": "main"}],
                },
                source="local",
                task_set_id=task_set_id,
            ),
        )
        session.commit()

    with TestClient(app) as client:
        response = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": task_id,
                "config": {
                    "agent_name": "direct-completion",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                },
            },
        )

    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "task not found"
    with session_factory() as session:
        trial_count = session.execute(select(func.count()).select_from(Trial)).scalar_one()
    engine.dispose()
    assert trial_count == 0


def test_submit_rejects_agent_that_cannot_satisfy_task_capabilities(
    app,
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    _team_id, raw = seed_team
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        task_config = session.execute(select(Task.config).where(Task.id == "hello")).scalar_one()
        session.execute(
            update(Task)
            .where(Task.id == "hello")
            .values(
                config={
                    **task_config,
                    "required_agent_capabilities": ["workspace_exec"],
                },
            ),
        )
        session.commit()

    with TestClient(app) as client:
        response = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {
                    "agent_name": "direct-completion",
                    "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                },
            },
        )

    assert response.status_code == 400, response.text
    assert "workspace_exec" in response.json()["detail"]
    with session_factory() as session:
        trial_count = session.execute(select(func.count()).select_from(Trial)).scalar_one()
    engine.dispose()
    assert trial_count == 0


def test_submit_applies_immutable_tb21_workspace_requirement(
    app,
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    _team_id, raw = seed_team
    profile_id = "terminal-bench-2@tb2.1-r6"
    task_id = f"{profile_id}/capability-overlay-test"
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    try:
        with session_factory() as session:
            session.execute(
                insert(Benchmark).values(
                    id=profile_id,
                    display_name="Terminal-Bench 2.1",
                    upstream_kind="harbor-package",
                    upstream_locator="terminal-bench/terminal-bench-2-1",
                    upstream_revision="6",
                    license_spdx="Apache-2.0",
                    license_url="https://example.test/license",
                    splits=["test"],
                    execution_state="runnable",
                ),
            )
            session.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="3" * 64,
                    benchmark_id=profile_id,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "script"},
                        "steps": [{"name": "main"}],
                    },
                ),
            )
            session.commit()

        with TestClient(app) as client:
            response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "task_id": task_id,
                    "config": {
                        "agent_name": "direct-completion",
                        "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
                    },
                },
            )

        assert response.status_code == 400, response.text
        assert "workspace_exec" in response.json()["detail"]
        with session_factory() as session:
            trial_count = session.execute(select(func.count()).select_from(Trial)).scalar_one()
        assert trial_count == 0
    finally:
        with session_factory() as session:
            session.execute(delete(Trial).where(Trial.task_id == task_id))
            session.execute(delete(Task).where(Task.id == task_id))
            session.execute(delete(Benchmark).where(Benchmark.id == profile_id))
            session.commit()
        engine.dispose()


def test_submit_rejects_invalid_task_config(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "broken-config",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
        assert r.status_code == 400
        assert "invalid task config" in r.json()["detail"]
        assert "broken-config" in r.json()["detail"]


@pytest.mark.parametrize(
    ("execution_state", "reason"),
    [("pending", "benchmark_not_runnable"), ("historical", "benchmark_retired")],
)
def test_submit_rejects_non_runnable_benchmark_task(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
    execution_state: str,
    reason: str,
) -> None:
    _, raw = seed_team
    profile_id = f"trial-submit-{execution_state}-{uuid4().hex}"
    task_id = f"{profile_id}/task"
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    try:
        with session_factory() as s:
            s.execute(
                insert(Benchmark).values(
                    id=profile_id,
                    display_name=profile_id,
                    upstream_kind="test",
                    upstream_locator="test",
                    upstream_revision="1",
                    license_spdx="MIT",
                    license_url="https://example.test/license",
                    splits=["test"],
                    execution_state=execution_state,
                )
            )
            s.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="2" * 64,
                    benchmark_id=profile_id,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )
            s.commit()

        with TestClient(app) as client:
            response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "task_id": task_id,
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )

        assert response.status_code == 409, response.text
        assert response.json()["detail"]["reason"] == reason
        assert response.json()["detail"]["benchmark_profile"] == profile_id
    finally:
        with session_factory() as s:
            s.execute(delete(Task).where(Task.id == task_id))
            s.execute(delete(Benchmark).where(Benchmark.id == profile_id))
            s.commit()
        engine.dispose()


def test_submit_rejects_unauth(app, seed_team):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 401


def test_submit_rejects_missing_task_id(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 400


def _fetch_trial_config(postgres_url: str, trial_id: str) -> dict:
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            row = (
                s.execute(
                    Trial.__table__.select().where(Trial.id == UUID(trial_id)),
                )
                .mappings()
                .one()
            )
            return row["config"]
    finally:
        engine.dispose()


def test_submit_snapshots_retry_defaults_when_absent(
    app,
    seed_team,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    """#401: submitter omits `retry` → deployment defaults snapshotted."""
    _, raw = seed_team
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
        cfg = _fetch_trial_config(postgres_url, r.json()["trial_id"])
    retry = cfg["retry"]
    assert retry["max_attempts"] == 3
    assert set(retry["retry_on"]) == {
        "gateway_error",
        "provider_transport_disconnect",
        "node_setup_health",
    }
    assert retry["backoff"] == {
        "base_sec": 30.0,
        "max_sec": 600.0,
        "multiplier": 2.0,
        "jitter": 0.2,
    }


def test_submit_clamps_max_attempts_to_team_ceiling(
    app,
    seed_team,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    """#401: submitter requests 10, team ceiling is 2 → snapshot stores 2."""
    team_id, raw = seed_team
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            s.execute(
                TeamQuota.__table__.update()
                .where(TeamQuota.team_id == team_id)
                .values(max_attempts_ceiling=2),
            )
            s.commit()
    finally:
        engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "retry": {"max_attempts": 10},
                },
            },
        )
        assert r.status_code == 201, r.text
        cfg = _fetch_trial_config(postgres_url, r.json()["trial_id"])
    assert cfg["retry"]["max_attempts"] == 2


def test_submit_preserves_explicit_retry_below_ceiling(
    app,
    seed_team,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    """#401: submitter's explicit retry passes through when under ceiling."""
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "retry": {
                        "max_attempts": 2,
                        "retry_on": ["agent_timeout"],
                    },
                },
            },
        )
        assert r.status_code == 201, r.text
        cfg = _fetch_trial_config(postgres_url, r.json()["trial_id"])
    assert cfg["retry"]["max_attempts"] == 2
    assert cfg["retry"]["retry_on"] == ["agent_timeout"]


def test_idempotent_resubmission_preserves_original_task_image_revision(
    app,
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    _, raw = seed_team
    payload = {
        "task_id": "dockerfile-any",
        "idempotency_key": f"frozen-task-image-{uuid4()}",
        "config": {"agent_name": "oracle", "agent_model": None},
    }
    headers = {"Authorization": f"Bearer {raw}"}
    engine = create_engine(postgres_url)
    try:
        with TestClient(app) as client:
            first = client.post("/trials", headers=headers, json=payload)
            assert first.status_code == 201, first.text
            trial_id = UUID(first.json()["trial_id"])
            with sessionmaker(engine)() as session:
                original_ids = set(session.scalars(
                    select(TrialTaskImageMaterialization.materialization_id).where(
                        TrialTaskImageMaterialization.trial_id == trial_id,
                    )
                ))
                assert len(original_ids) == 1
                session.execute(update(Task).where(Task.id == "dockerfile-any").values(
                    checksum="3" * 64, source="s3://loom-tasks/rebuilt-dockerfile-any",
                ))
                session.commit()
            second = client.post("/trials", headers=headers, json=payload)
            assert second.status_code == 201, second.text
            assert second.json()["trial_id"] == str(trial_id)
        with sessionmaker(engine)() as session:
            rows = session.scalars(
                select(TaskImageMaterialization).join(TrialTaskImageMaterialization).where(
                    TrialTaskImageMaterialization.trial_id == trial_id,
                )
            ).all()
            assert {row.id for row in rows} == original_ids
            assert {row.task_checksum for row in rows} == {"2" * 64}
            assert {row.task_source for row in rows} == {"s3://loom-tasks/dockerfile-any"}
            assert session.scalar(select(func.count()).select_from(TaskImageMaterialization).where(
                TaskImageMaterialization.task_id == "dockerfile-any",
            )) == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize("task_id", ["hello", "dockerfile-any"])
def test_arm_submission_returns_400_without_creating_work(
    app, seed_team, postgres_url: str, task_id: str,
):  # type: ignore[no-untyped-def]
    team_id, raw = seed_team
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            task = session.get(Task, task_id)
            config = dict(task.config)
            config["environment"] = {**config["environment"], "cpu_arch": "arm64"}
            task.config = config
            session.commit()
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/trials", headers={"Authorization": f"Bearer {raw}"},
                json={"task_id": task_id, "config": {"agent_name": "oracle", "agent_model": None}},
            )
        assert response.status_code == 400, response.text
        assert "x86_64 only" in response.json()["detail"]
        with sessionmaker(engine)() as session:
            assert session.scalar(select(func.count()).select_from(Trial)) == 0
            assert session.scalar(select(func.count()).select_from(TaskImageMaterialization)) == 0
            assert session.get(TeamQuota, team_id).in_flight_count == 0
    finally:
        engine.dispose()


def test_historical_arm_idempotent_submission_remains_readable(
    app, seed_team, postgres_url: str,
):  # type: ignore[no-untyped-def]
    team_id, raw = seed_team
    trial_id = uuid4()
    key = f"historical-arm-{trial_id}"
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as session:
            task = session.get(Task, "hello")
            config = dict(task.config)
            config["environment"] = {**config["environment"], "cpu_arch": "arm64"}
            task.config = config
            session.add(Trial(id=trial_id, team_id=team_id, task_id=task.id,
                              idempotency_key=key, state="succeeded", config={}, result={"status": "succeeded"},
                              requires_caps={"cpu_arch": "arm64"}))
            session.commit()
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/trials", headers={"Authorization": f"Bearer {raw}"},
                json={"task_id": "hello", "idempotency_key": key},
            )
        assert response.status_code == 201, response.text
        assert response.json()["trial_id"] == str(trial_id)
        assert response.json()["state"] == "succeeded"
        with sessionmaker(engine)() as session:
            assert session.scalar(select(func.count()).select_from(Trial)) == 1
            assert session.scalar(select(func.count()).select_from(TaskImageMaterialization)) == 0
    finally:
        engine.dispose()
