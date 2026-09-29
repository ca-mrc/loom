"""TaskSet publication preserves compatibility only for the same legacy revision."""

import copy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskSet, TaskSetMaterializationJob, Team
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.taskset.materialize import MaterializeOutput, TaskRowDraft
from loom.verifier_runtime import apply_legacy_verifier_default
from loom_service.taskset_materializer import MaterializationLease, publish_if_current


@pytest.mark.parametrize("change,preserve", [
    ("none", True), ("checksum_prefix", True), ("generation_location", True),
    ("checksum", False), ("config", False), ("manifest", False),
    ("old_manifest", False), ("task_id", False), ("stale_marker", False),
])
async def test_rebuild_carries_legacy_mode_only_for_exact_revision(postgres_url, change, preserve):
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    team_id, job_id = uuid4(), uuid4()
    task_set_id = f"ts/{team_id}/legacy-rebuild"
    task_id = f"{task_set_id}/tasks/task"
    checksum = "a" * 64
    config = {
        "schema_version": "1", "task": {"id": task_id, "name": "task"},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest", "env_mode": "shared"},
        "steps": [{"name": "main"}],
    }
    source = "s3://tasks/generation-1/task/"
    provenance = {"service_execution_input_manifest_uri": "s3://tasks/generation-1/input.json"}
    old_provenance = ({"bundle_content_manifest_sha256": "b" * 64}
                      if change == "old_manifest" else provenance)
    new_config, new_source, new_provenance = copy.deepcopy(config), source, copy.deepcopy(provenance)
    new_checksum, new_id = checksum, task_id
    if change == "checksum_prefix":
        new_checksum = "sha256:" + checksum
    elif change == "generation_location":
        new_source = "s3://tasks/generation-2/task/"
        new_provenance["service_execution_input_manifest_uri"] = "s3://tasks/generation-2/input.json"
    elif change == "checksum":
        new_checksum = "b" * 64
    elif change == "config":
        new_config["task"]["name"] = "changed"
    elif change == "manifest":
        new_provenance["bundle_content_manifest_sha256"] = "b" * 64
    elif change == "task_id":
        new_id += "-new"
    try:
        async with factory() as session:
            session.add(Team(id=team_id, name=f"legacy-{team_id}"))
            await session.flush()
            session.add(TaskSet(
                id=task_set_id, owning_team_id=team_id, slug="legacy-rebuild",
                display_name="Legacy rebuild", status="materializing",
                intents=["evaluation"], manifest_blob_uri="s3://tasks/manifest.yaml",
            ))
            await session.flush()
            session.add(TaskSetMaterializationJob(
                id=job_id, task_set_id=task_set_id, owning_team_id=team_id,
                state="running", lease_epoch=1, claimed_by="rebuild-test",
                lease_heartbeat_at=datetime.now(UTC),
            ))
            session.add(Task(
                id=task_id, task_set_id=task_set_id, checksum=checksum, config=config,
                source=source, source_provenance=old_provenance,
                legacy_separate_verifier_checksum="c" * 64 if change == "stale_marker" else checksum,
            ))
            await session.commit()
        async with factory() as session:
            await publish_if_current(
                session,
                lease=MaterializationLease(job_id=job_id, lease_epoch=1, claimed_by="rebuild-test"),
                task_set_id=task_set_id,
                output=MaterializeOutput(
                    task_rows=[TaskRowDraft(
                        id=new_id, checksum=new_checksum, config=new_config,
                        source=new_source, source_provenance=new_provenance,
                    )],
                    task_count=1, status="ready", evaluation_ready=True,
                ),
                claim_ttl_sec=60,
            )
        async with factory() as session:
            task = await session.get(Task, new_id)
            assert task.legacy_separate_verifier_checksum == (checksum if preserve else None)
            assert task.config == new_config
            assert task.source == new_source
            assert task.source_provenance == new_provenance
            effective = apply_legacy_verifier_default(
                TaskConfig.model_validate(task.config),
                TrialConfig(agent_name="terminus-2", agent_model={"provider": "test", "name": "test"}),
                task_checksum=task.checksum,
                legacy_separate_verifier_checksum=task.legacy_separate_verifier_checksum,
                source_provenance=task.source_provenance,
            )
            assert effective.verifier_env_mode == ("separate" if preserve else None)
    finally:
        async with factory() as session:
            await session.execute(delete(Task).where(Task.task_set_id == task_set_id))
            await session.execute(delete(TaskSet).where(TaskSet.id == task_set_id))
            await session.execute(delete(Team).where(Team.id == team_id))
            await session.commit()
        await engine.dispose()
