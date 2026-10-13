"""Exercise custom Harbor upload through publication and native admission."""

import io
import json
import os
import subprocess
import tarfile
import tomllib
from uuid import uuid4

import pytest

from loom.models.task import TaskConfig
from loom.models.taskset import UserTaskSetManifest
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import automatic_service_execution_rejections
from loom.taskset.materialize import materialize_task_set
from tests.unit.test_harbor_taskset_materialize import ObjectStore


def materialize(tmp_path, *, environment="", verifier="", extra_files=None):
    files = {
        "task.toml": (
            'version = "1.0"\n[metadata]\nid = "sample"\n'
            '[environment]\ncpus = 1\nmemory_mb = 2048\nstorage_mb = 6144\n'
            + environment + '\n[verifier]\n' + verifier
        ).encode(),
        "instruction.md": b"Create the answer.\n",
        "environment/Dockerfile": b"FROM ubuntu:22.04\nWORKDIR /app\n",
        "solution/solve.sh": b"echo answer > answer.txt\n",
        "tests/test.sh": b'echo 0 > "$LOOM_HARBOR_LOG_DIR/reward.txt"\nexit 1\n',
        **(extra_files or {}),
    }
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as stream:
        for name, content in files.items():
            entry = tarfile.TarInfo("tasks/sample/" + name)
            entry.size = len(content)
            stream.addfile(entry, io.BytesIO(content))
    store = ObjectStore(archive.getvalue())
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "slice", "display_name": "Slice"},
        "intents": ["trajectory_generation"],
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz"},
    })
    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team/slice", owning_team_id="team",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None,
        minio_client=store, artifacts_bucket="artifacts", upstream_cache_root=tmp_path,
    )
    return result, store, files


def test_original_upload_is_admissible_and_bridge_preserves_zero_reward(tmp_path):
    result, store, files = materialize(tmp_path)
    assert result.status == "ready", result.error_summary
    row = result.task_rows[0]
    task = TaskConfig.model_validate(row.config)
    assert automatic_service_execution_rejections(
        task, TrialConfig(agent_name="oracle", agent_model=None),
        source_provenance=row.source_provenance, allow_task_image_preparation=True,
    ) == ()
    assert task.verifier.env_mode == "shared"
    prefix = row.source.removeprefix("s3://artifacts/")
    for name, content in files.items():
        target = "upstream-task.toml" if name == "task.toml" else name
        assert store.objects[prefix + target] == content
    assert TaskConfig.model_validate(tomllib.loads(store.objects[prefix + "task.toml"].decode())) == task
    binding = row.source_provenance["service_execution_input"]
    published = json.loads(store.objects[binding["manifest_uri"].removeprefix("s3://artifacts/")])
    assert "verifier/run.sh" in {item["relative_path"] for item in published["files"]}
    for key, body in store.objects.items():
        if key.startswith(prefix):
            path = tmp_path / "bundle" / key.removeprefix(prefix)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
    output = tmp_path / "output.json"
    completed = subprocess.run(
        ["sh", str(tmp_path / "bundle/verifier/run.sh")],
        env={**os.environ, "LOOM_TASK_DIR": str(tmp_path / "bundle"),
             "LOOM_VERIFIER_OUTPUT": str(output),
             "LOOM_HARBOR_TEST_DIR": str(tmp_path / "tests"),
             "LOOM_HARBOR_LOG_DIR": str(tmp_path / "logs")},
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(output.read_text())["rewards"] == {"resolved": 0.0}


@pytest.mark.parametrize("environment,expected", [
    ('allow_internet = false', "no-network"),
    ('allow_internet = true', "public-web"),
    ('baseline_network_policy = {kind = "web-allowlist", destinations = [{host = "example.com", protocol = "https"}]}\n'
     'network_policies_supported = ["web-allowlist"]', "web-allowlist"),
])
def test_explicit_network_and_separate_mode_survive_upload(tmp_path, environment, expected):
    result, _, _ = materialize(tmp_path, environment=environment, verifier='env_mode = "separate"')
    assert result.status == "ready", result.error_summary
    config = result.task_rows[0].config
    assert config["environment"]["baseline_network_policy"]["kind"] == expected
    assert config["verifier"]["env_mode"] == "separate"


def test_bridge_collision_fails_import_without_overwriting_source(tmp_path):
    result, _, _ = materialize(tmp_path, extra_files={"verifier/run.sh": b"custom verifier\n"})
    assert result.status == "failed"
    assert "reserved verifier/run.sh" in str(result.error_summary)


def test_source_config_collision_fails_import(tmp_path):
    result, _, _ = materialize(tmp_path, extra_files={"upstream-task.toml": b"authored data\n"})
    assert result.status == "failed"
    assert "reserved upstream-task.toml" in str(result.error_summary)


def test_independent_verifier_image_remains_an_explicit_requirement(tmp_path):
    result, _, _ = materialize(tmp_path, extra_files={"tests/Dockerfile": b"FROM python:3.11\n"})
    assert result.status == "ready", result.error_summary
    config = result.task_rows[0].config
    assert config["verifier"]["env_mode"] == "separate"
    assert config["verifier"]["environment"]["dockerfile"] == "tests/Dockerfile"


def test_custom_verifier_is_not_replaced(tmp_path):
    result, store, _ = materialize(
        tmp_path, verifier='[verifier.args]\nscript_path = "verifier/custom.sh"',
        extra_files={"verifier/custom.sh": b"echo custom\n"},
    )
    assert result.status == "ready", result.error_summary
    row = result.task_rows[0]
    assert row.config["verifier"]["args"]["script_path"] == "verifier/custom.sh"
    assert not any(key.endswith("verifier/run.sh") for key in store.objects)


def test_compose_requirement_is_not_silently_discarded(tmp_path):
    result, _, _ = materialize(
        tmp_path, extra_files={"environment/compose.yaml": b"services: {}\n"},
    )
    assert result.status == "ready", result.error_summary
    row = result.task_rows[0]
    assert "environment.compose_files: native_compose_runtime_unavailable" in (
        automatic_service_execution_rejections(
            TaskConfig.model_validate(row.config), TrialConfig(agent_name="oracle", agent_model=None),
            source_provenance=row.source_provenance, allow_task_image_preparation=True,
        )
    )


def test_rematerialization_from_original_is_repeatable(tmp_path):
    first, _, original = materialize(tmp_path)
    second, store, _ = materialize(tmp_path)
    assert first.status == second.status == "ready"
    assert first.task_rows[0].checksum == second.task_rows[0].checksum
    assert first.task_rows[0].config == second.task_rows[0].config
    prefix = second.task_rows[0].source.removeprefix("s3://artifacts/")
    for name, content in original.items():
        target = "upstream-task.toml" if name == "task.toml" else name
        assert store.objects[prefix + target] == content


def test_native_legacy_offline_task_reaches_admission_without_invented_artifacts(tmp_path, monkeypatch):
    from loom.models.networking import NoNetwork
    from loom.service_execution_sandbox_task import sandbox_driver

    original = b'''[task]
name = "enigma"
allow_internet = false
[environment]
cpus = 1
memory_mb = 2048
storage_mb = 6144
'''
    result, store, _ = materialize(tmp_path, extra_files={"task.toml": original})
    assert result.status == "ready", result.error_summary
    row = result.task_rows[0]
    task = TaskConfig.model_validate(row.config)
    assert task.environment.baseline_network_policy == NoNetwork()
    assert task.steps[0].artifacts == []
    trial = TrialConfig(agent_name="oracle", agent_model=None)
    assert automatic_service_execution_rejections(
        task, trial, source_provenance=row.source_provenance, allow_task_image_preparation=True,
    ) == ()
    monkeypatch.setenv("LOOM_EFFECTIVE_NETWORK_POLICY_JSON", '{"kind":"no-network"}')
    monkeypatch.delenv("LOOM_TASK_EGRESS_PROXY", raising=False)
    driver = sandbox_driver("task-sandbox", task, trial)
    assert "no-network" in driver.capabilities.network_policies
    assert driver._command_environment == {}
    prefix = row.source.removeprefix("s3://artifacts/")
    assert store.objects[prefix + "upstream-task.toml"] == original
    assert TaskConfig.model_validate(tomllib.loads(store.objects[prefix + "task.toml"].decode())) == task
