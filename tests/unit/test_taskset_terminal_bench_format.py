"""Explicit bundle dialects project Terminal-Bench tasks without [task].name."""

import io
import tarfile
from uuid import uuid4

from loom.models.task import TaskConfig
from loom.models.taskset import UserTaskSetManifest
from loom.models.trial import TrialConfig
from loom.service_execution_materialization import automatic_service_execution_rejections
from loom.taskset.materialize import materialize_task_set
from loom.verifier_runtime import (
    independent_verifier_rejections,
    native_independent_verifier_handoff,
    resolve_verifier_env_mode,
)
from loom_service.batch_purpose import validate_purpose_trial_config
from tests.unit.test_harbor_taskset_materialize import ObjectStore

_CHILD_TOML = b"""version = "1.0"
artifacts = [{ source = "/app" }]
[metadata]
author_name = "terminalgen-synthesis"
tags = ["operator:workflow_deepening"]
[verifier]
environment_mode = "separate"
timeout_sec = 900.0
[verifier.environment]
build_timeout_sec = 600.0
[agent]
timeout_sec = 3600.0
[environment]
build_timeout_sec = 1800.0
cpus = 2
memory_mb = 4096
storage_mb = 10240
"""

_TB2_TOML = b"""version = "1.0"
[metadata]
tags = ["shell"]
[agent]
timeout_sec = 1200.0
[environment]
cpus = 1
memory_mb = 2048
storage_mb = 4096
"""


def _archive(files: dict[str, bytes]) -> bytes:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as stream:
        for name, content in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            stream.addfile(entry, io.BytesIO(content))
    return archive.getvalue()


def _base_files(task_toml: bytes, *, verifier_dockerfile: bool) -> dict[str, bytes]:
    files = {
        "family/child/task.toml": task_toml,
        "family/child/instruction.md": b"Do the task.\n",
        "family/child/tests/test.sh": b"echo 1 > /logs/verifier/reward.txt\n",
        "family/child/environment/Dockerfile": b"FROM ubuntu:24.04\nWORKDIR /app\n",
    }
    if verifier_dockerfile:
        files["family/child/tests/Dockerfile"] = b"FROM python:3.11\n"
    return files


def _materialize(tmp_path, files: dict[str, bytes], *, task_toml_format: str | None):
    source = {"type": "bundle-upload", "locator": "bundle.tar.gz"}
    if task_toml_format is not None:
        source["task_toml_format"] = task_toml_format
    store = ObjectStore(_archive(files))
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "slice", "display_name": "Slice"},
        "intents": ["trajectory_generation"],
        "source": source,
    })
    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team/slice", owning_team_id="team",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None,
        minio_client=store, artifacts_bucket="artifacts", upstream_cache_root=tmp_path,
    )
    return result, store


def test_terminal_bench_child_shape_materializes_and_hands_off_artifacts(tmp_path):
    result, store = _materialize(
        tmp_path, _base_files(_CHILD_TOML, verifier_dockerfile=True),
        task_toml_format="terminal-bench",
    )
    assert result.status == "ready", result.error_summary
    row = result.task_rows[0]
    task = TaskConfig.model_validate(row.config)
    assert row.id == "ts/team/slice/tasks/family_child"
    assert task.task.id == task.task.name == "family/child"
    assert task.environment.os == "linux"
    assert task.environment.cpu_arch == "x86_64"
    assert task.agent.timeout_sec == 3600
    assert task.verifier.timeout_sec == 900
    assert task.verifier.env_mode == "separate"
    assert task.verifier.environment is not None
    assert str(task.verifier.environment.dockerfile) == "tests/Dockerfile"
    assert str(task.verifier.environment.docker_build_context) == "tests"
    assert task.verifier.environment.build_timeout_sec == 600
    assert task.verifier.environment.cpus == 2
    assert task.verifier.environment.memory_mb == 4096
    assert task.verifier.environment.storage_mb == 10240
    assert task.steps[0].artifact_sources[0].source == "/app"
    assert task.upstream_origin is None
    assert task.upstream_conversion is not None
    assert task.upstream_conversion.importer == "loom.harbor-native.v1"
    assert task.upstream_conversion.original_config == "upstream-task.toml"
    assert native_independent_verifier_handoff(task) is True
    prefix = row.source.removeprefix("s3://artifacts/")
    assert store.objects[prefix + "upstream-task.toml"] == _CHILD_TOML
    trial = TrialConfig(agent_name="oracle", agent_model=None)
    assert resolve_verifier_env_mode(task, trial) == "separate"
    assert "verifier.environment: explicit_resources_required" not in automatic_service_execution_rejections(
        task, trial, source_provenance=row.source_provenance, allow_task_image_preparation=True,
    )
    shared = trial.model_copy(update={"verifier_env_mode": "shared"})
    assert resolve_verifier_env_mode(task, shared) == "shared"
    assert "verifier.environment: separate_mode_required" in independent_verifier_rejections(
        task, shared, allow_image_preparation=True,
    )
    assert independent_verifier_rejections(
        task, trial.model_copy(update={"skip_verifier": True}),
    ) == ()
    assert validate_purpose_trial_config("evaluation", {"skip_verifier": True}) == (
        "evaluation_disallows_skip_verifier"
    )
    assert validate_purpose_trial_config("trajectory_generation", {"skip_verifier": True}) is None


def test_terminal_bench_preserves_an_authored_verifier_script(tmp_path):
    authored = _TB2_TOML + b"""
[verifier]
name = "script"
[verifier.args]
script_path = "verifier/custom.sh"
"""
    files = _base_files(authored, verifier_dockerfile=False)
    files["family/child/verifier/custom.sh"] = b"echo custom\n"
    for task_toml_format in ("auto", "terminal-bench"):
        result, store = _materialize(tmp_path, files, task_toml_format=task_toml_format)
        assert result.status == "ready", result.error_summary
        row = result.task_rows[0]
        assert row.config["verifier"]["args"]["script_path"] == "verifier/custom.sh", task_toml_format
        assert row.config.get("upstream_conversion") is None
        assert not any(key.endswith("verifier/run.sh") for key in store.objects)


def test_custom_separate_verifier_inherits_omitted_resources_without_a_bridge(tmp_path):
    authored = _CHILD_TOML.replace(
        b"[verifier.environment]",
        b'[verifier.args]\nscript_path = "verifier/custom.sh"\n[verifier.environment]',
    )
    files = _base_files(authored, verifier_dockerfile=True)
    files["family/child/verifier/custom.sh"] = b"echo custom\n"
    del files["family/child/tests/test.sh"]
    result, store = _materialize(tmp_path, files, task_toml_format="terminal-bench")
    assert result.status == "ready", result.error_summary
    task = TaskConfig.model_validate(result.task_rows[0].config)
    assert task.verifier.args["script_path"] == "verifier/custom.sh"
    assert str(task.verifier.environment.dockerfile) == "tests/Dockerfile"
    assert str(task.verifier.environment.docker_build_context) == "tests"
    assert task.verifier.environment.cpus == task.environment.cpus == 2
    assert task.verifier.environment.memory_mb == task.environment.memory_mb == 4096
    assert task.verifier.environment.storage_mb == task.environment.storage_mb == 10240
    assert "verifier.environment: explicit_resources_required" not in independent_verifier_rejections(
        task, TrialConfig(agent_name="oracle", agent_model=None), allow_image_preparation=True,
    )
    assert task.upstream_conversion is None
    assert not any(key.endswith("verifier/run.sh") for key in store.objects)
    prefix = result.task_rows[0].source.removeprefix("s3://artifacts/")
    assert store.objects[prefix + "verifier/custom.sh"] == b"echo custom\n"


def test_authored_task_identity_wins_over_the_directory(tmp_path):
    toml = _TB2_TOML.replace(
        b'[metadata]\n',
        b'[task]\nid = "kept-id"\nname = "Kept Name"\n[metadata]\n',
    )
    result, _ = _materialize(
        tmp_path, _base_files(toml, verifier_dockerfile=False),
        task_toml_format="terminal-bench",
    )
    assert result.status == "ready", result.error_summary
    task = TaskConfig.model_validate(result.task_rows[0].config)
    assert task.task.id == "kept-id"
    assert task.task.name == "Kept Name"


def test_auto_rejects_the_same_child_shape(tmp_path):
    result, _ = _materialize(
        tmp_path, _base_files(_CHILD_TOML, verifier_dockerfile=True),
        task_toml_format=None,
    )
    assert result.status == "failed"
    assert result.error_summary[0]["code"] == "task_config_invalid"


def test_terminal_bench_without_verifier_image_stays_shared(tmp_path):
    result, store = _materialize(
        tmp_path, _base_files(_TB2_TOML, verifier_dockerfile=False),
        task_toml_format="terminal-bench",
    )
    assert result.status == "ready", result.error_summary
    row = result.task_rows[0]
    task = TaskConfig.model_validate(row.config)
    assert task.verifier.env_mode == "shared"
    assert task.verifier.environment is None
    assert task.agent.timeout_sec == 1200
    assert task.upstream_conversion is not None
    assert native_independent_verifier_handoff(task) is False
    trial = TrialConfig(agent_name="oracle", agent_model=None)
    assert resolve_verifier_env_mode(task, trial) == "shared"
    separate = trial.model_copy(update={"verifier_env_mode": "separate"})
    assert resolve_verifier_env_mode(task, separate) == "separate"
    assert task.verifier.environment is None
    prefix = row.source.removeprefix("s3://artifacts/")
    assert prefix + "upstream-task.toml" in store.objects
    assert not any(key.endswith("solution/solve.sh") for key in store.objects)


def test_authored_network_timeout_and_mode_survive_terminal_bench_defaults(tmp_path):
    toml = b"""version = "1.0"
[verifier]
environment_mode = "separate"
timeout_sec = 42.0
[agent]
timeout_sec = 77.0
[environment]
allow_internet = false
cpus = 1
memory_mb = 512
storage_mb = 1024
"""
    result, _ = _materialize(
        tmp_path, _base_files(toml, verifier_dockerfile=True),
        task_toml_format="terminal-bench",
    )
    assert result.status == "ready", result.error_summary
    task = TaskConfig.model_validate(result.task_rows[0].config)
    assert task.agent.timeout_sec == 77
    assert task.verifier.timeout_sec == 42
    assert task.verifier.env_mode == "separate"
    assert task.environment.baseline_network_policy.kind == "no-network"
    assert task.verifier.environment is not None
    assert task.verifier.environment.baseline_network_policy.kind == "no-network"
    assert task.verifier.environment.cpus == 1


def test_shared_mode_with_verifier_image_still_fails(tmp_path):
    toml = b"""version = "1.0"
[verifier]
environment_mode = "shared"
[environment]
cpus = 1
memory_mb = 512
storage_mb = 1024
"""
    result, _ = _materialize(
        tmp_path, _base_files(toml, verifier_dockerfile=True),
        task_toml_format="terminal-bench",
    )
    assert result.status == "failed"
    assert result.error_summary[0]["code"] == "task_config_invalid"
    assert "shared verifier cannot declare a separate environment" in result.error_summary[0]["message"]


def test_loom_format_does_not_project_a_harbor_name(tmp_path):
    files = _base_files(
        b'[task]\nname = "enigma"\n[environment]\ncpus = 1\nmemory_mb = 2048\nstorage_mb = 6144\n',
        verifier_dockerfile=False,
    )
    result, store = _materialize(tmp_path, files, task_toml_format="loom")
    assert result.status == "failed"
    assert result.status_reason == "no_tasks_materialized"
    assert result.error_summary[0]["code"] == "task_config_invalid"
    assert not any(key.endswith("upstream-task.toml") for key in store.objects)
    assert not any(key.endswith("verifier/run.sh") for key in store.objects)


def test_loom_format_keeps_an_authored_taskconfig(tmp_path):
    files = _base_files(
        b"""schema_version = "1"
[task]
id = "authored"
name = "Authored"
[environment]
os = "linux"
cpus = 1
memory_mb = 1024
storage_mb = 2048
[agent]
name = "oracle"
[verifier]
name = "script"
timeout_sec = 30
env_mode = "separate"
""",
        verifier_dockerfile=False,
    )
    result, store = _materialize(tmp_path, files, task_toml_format="loom")
    assert result.status == "ready", result.error_summary
    task = TaskConfig.model_validate(result.task_rows[0].config)
    assert task.task.id == "authored"
    assert task.verifier.env_mode == "separate"
    assert task.upstream_conversion is None
    assert native_independent_verifier_handoff(task) is False
    assert not any(key.endswith("upstream-task.toml") for key in store.objects)


def test_explicit_auto_matches_omitted_format(tmp_path):
    files = _base_files(_TB2_TOML, verifier_dockerfile=False)
    omitted, _ = _materialize(tmp_path, files, task_toml_format=None)
    explicit, _ = _materialize(tmp_path, files, task_toml_format="auto")
    assert omitted.status == explicit.status == "ready"
    assert omitted.task_rows[0].config == explicit.task_rows[0].config
    assert omitted.task_rows[0].checksum == explicit.task_rows[0].checksum
    assert omitted.task_rows[0].config.get("upstream_conversion") is None
