"""Old submitters cannot lose repaired verifier semantics during a rollout."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization, Trial
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.nebius_rollout_guard import acquire, release
from loom_control_plane.service_execution_scheduler import reserve_next_service_execution
from tests.integration.test_service_execution_image_readiness import (
    TASK_IMAGE,
    _clean_image_links,
    _seed_preparing_trial,
)
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)
from tests.support.execution_image_admission import IMAGE_ADMISSION_KEYRING


@pytest.mark.parametrize(("override", "catalog_checksum", "marker", "effective"), [
    (None, "2" * 64, "2" * 64, "separate"),
    ("shared", "2" * 64, "2" * 64, "shared"),
    ("separate", "2" * 64, "2" * 64, "separate"),
    (None, "8" * 64, "2" * 64, "separate"),
    (None, "8" * 64, "8" * 64, None),
], ids=["old-writer", "explicit-shared", "explicit-separate", "frozen-revision", "other-revision"])
async def test_scheduler_persists_revision_bound_default_before_admission(
    postgres_url, override, catalog_checksum, marker, effective,
):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    trial_id = None
    guard_owned = False
    try:
        async with sessions() as session:
            trial_id, ids = await _seed_preparing_trial(session, now=now, state="ready")
            trial = await session.get(Trial, trial_id)
            task = await session.get(Task, trial.task_id)
            shared = deepcopy(task.config)
            shared["verifier"]["env_mode"] = "shared"
            task.config = shared
            task.checksum = catalog_checksum
            task.legacy_separate_verifier_checksum = marker
            trial.config = {**trial.config, "verifier_env_mode": override}
            # Seed the historical frozen revision, including the unused ARM alternative.
            for identity in ids.values():
                image = await session.get(TaskImageMaterialization, identity)
                image.task_config = deepcopy(shared)
                if image.cpu_arch == "x86_64":
                    image.registry_images = {"task": TASK_IMAGE}
            await session.commit()
            image = await session.get(TaskImageMaterialization, ids["x86_64"])
            frozen = deepcopy(image.task_config)

            if override is None and catalog_checksum == marker == "2" * 64:
                guard = await acquire(session, owner="test-verifier-rollout", candidate="a" * 40)
                assert guard["status"] == "acquired"
                await session.commit()
                guard_owned = True
                assert await reserve_next_service_execution(
                    session, environment="staging", pool_id="nebius-cpu",
                    image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
                ) is None
                await session.commit()
                await session.refresh(trial)
                assert trial.state == "queued" and trial.attempt_count == 0
                assert trial.config["verifier_env_mode"] is None
                assert trial.scheduling_observation["reason"] == "platform_deploying"
                await release(session, owner="test-verifier-rollout", candidate="a" * 40)
                await session.commit()
                guard_owned = False
                now += timedelta(seconds=16)

            lease = await reserve_next_service_execution(
                session, environment="staging", pool_id="nebius-cpu",
                image_admission_keyring=IMAGE_ADMISSION_KEYRING, now=now,
            )
            await session.commit()
            assert lease is not None
            await session.refresh(trial)
            assert trial.config.get("verifier_env_mode") == effective
            plan = ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json)
            assert plan.verifier_execution == (
                "separate_execution" if effective == "separate" else "in_attempt"
            )
            assert plan.in_place_verifier is (effective != "separate")
            assert plan.task_revision_sha256 == "sha256:" + "2" * 64
            assert plan.task_image_materialization_id == ids["x86_64"]
            await session.refresh(image)
            assert image.task_config == frozen
    finally:
        async with sessions() as session:
            if guard_owned:
                await release(session, owner="test-verifier-rollout", candidate="a" * 40)
                await session.commit()
            await _clean_image_links(session, trial_id)
        await engine.dispose()
