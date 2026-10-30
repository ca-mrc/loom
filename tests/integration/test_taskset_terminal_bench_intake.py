"""The declared bundle dialect survives real API intake and worker publication."""

import pytest
import yaml
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from loom.db.schema import Task, TaskSetManifest
from loom.models.task import TaskConfig
from loom.verifier_runtime import native_independent_verifier_handoff
from tests.integration.test_taskset_materialization import (
    _run_materializer_once,
)
from tests.integration.test_taskset_materialization import (
    materialization_minio as materialization_minio,
)
from tests.integration.test_taskset_materialization import (
    materialization_setup as materialization_setup,
)
from tests.unit.test_taskset_terminal_bench_format import _CHILD_TOML, _archive, _base_files


@pytest.mark.parametrize("dialect", [None, "loom", "terminal-bench"])
async def test_child_bundle_api_to_materializer(materialization_setup, dialect):
    app, tokens, teams = materialization_setup
    source = {"type": "bundle-upload", "locator": "bundle.tar.gz"}
    if dialect is not None:
        source["task_toml_format"] = dialect
    manifest = {
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "slice", "display_name": "Slice"},
        "intents": ["trajectory_generation"], "source": source,
    }
    archive = _archive(_base_files(_CHILD_TOML, verifier_dockerfile=True))
    headers = {"Authorization": f"Bearer {tokens['team_a']}"}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        submitted = await client.post("/api/v1/tasksets", headers=headers, files={
            "manifest": ("manifest.yaml", yaml.safe_dump(manifest).encode(), "application/x-yaml"),
            "bundle": ("bundle.tar.gz", archive, "application/gzip"),
        })
        assert submitted.status_code == 202, submitted.text
        task_set_id = submitted.json()["task_set_id"]
        await _run_materializer_once(app)
        detail = await client.get(f"/api/v1/tasksets/{task_set_id}", headers=headers)
        assert detail.status_code == 200, detail.text
        body = detail.json()

    store = app.state.minio_client
    bucket = app.state.settings.artifacts_bucket
    uploaded_key = f"tasksets/user/{teams['team_a']}/slice/bundle.tar.gz"
    assert store.get_object(Bucket=bucket, Key=uploaded_key)["Body"].read() == archive
    async with app.state.session_factory() as session:
        stored = await session.scalar(select(TaskSetManifest).where(TaskSetManifest.task_set_id == task_set_id))
        assert stored.manifest["source"] == source
        tasks = (await session.scalars(select(Task).where(Task.task_set_id == task_set_id))).all()

    if dialect != "terminal-bench":
        assert body["status"] == "failed", body
        assert body["status_reason"] == "no_tasks_materialized"
        assert body["error_summary"][0]["code"] == "task_config_invalid"
        assert not tasks
        return

    assert body["status"] == "ready" and body["task_count"] == 1, body
    task = TaskConfig.model_validate(tasks[0].config)
    assert task.task.id == task.task.name == "family/child"
    assert task.environment.os == task.verifier.environment.os == "linux"
    assert task.verifier.environment.cpus == 2
    assert task.verifier.environment.memory_mb == 4096
    assert task.verifier.environment.storage_mb == 10240
    assert task.verifier.env_mode == "separate"
    assert task.steps[0].artifact_sources[0].source == "/app"
    assert native_independent_verifier_handoff(task)
    prefix = tasks[0].source.removeprefix(f"s3://{bucket}/")
    assert store.get_object(Bucket=bucket, Key=prefix + "upstream-task.toml")["Body"].read() == _CHILD_TOML
