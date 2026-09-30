"""Native sources retain semantics and identity independently of benchmark name."""

import json
import os
import subprocess
import tomllib
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import tomli_w
from pydantic import ValidationError

from loom.errors import DriverError
from loom.harbor_task_import import project_harbor_task
from loom.harbor_verifier_script import native_verifier_run_sh_bytes
from loom.models.harbor import ArtifactSource, UpstreamOrigin
from loom.models.task import TaskConfig
from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_runtime_compatibility import task_runtime_rejections
from loom_cli import benchmark_prepare, benchmark_publish, datasets_cmd
from loom_cli.harbor_benchmark_prepare import HarborBenchmarkSpec, HarborNativeAdapter
from tests.unit.test_trial_runner_start_authorization import runner

REVISION = "a" * 40


def source_task():
    return {
        "task": {"name": "upstream/sample"},
        "environment": {"cpus": 2, "memory": "4G", "storage": "10G"},
        "verifier": {
            "environment_mode": "separate",
            "environment": {
                "cpus": 16,
                "memory": "32G",
                "storage": "1000G",
                "gpus": 1,
                "gpu_types": ["H100"],
            },
            "env": {"MODE": "strict"},
            "collect": [{"service": "db", "command": "pg_dump app", "timeout_sec": 30}],
        },
        "artifacts": [
            "result.txt",
            "/app/output",
            {"source": "/data/dump", "service": "db", "destination": "db", "exclude": ["*.tmp"]},
        ],
    }


def spec(profile="native-fixture@1", subset=()):
    return HarborBenchmarkSpec(
        id=profile,
        display_name="Native fixture",
        series="fixtures",
        license_spdx="MIT",
        license_url="https://example.test/LICENSE",
        expected_task_count=1,
        origin=UpstreamOrigin(
            kind="git",
            locator="https://example.test/native.git",
            revision=REVISION,
            release="1",
            subset=subset,
        ),
    )


def native_tree(root, raw=None):
    task = root / "repo/tasks/sample"
    (task / "environment").mkdir(parents=True)
    (task / "tests").mkdir()
    (task / "environment/Dockerfile").write_text("FROM python:3.11-slim\n")
    (task / "tests/Dockerfile").write_text("FROM python:3.11-slim\n")
    (task / "environment/compose.yaml").write_text("services:\n  main:\n    build: .\n")
    (task / "instruction.md").write_text("Native fixture\n")
    (task / "tests/test.sh").write_text("echo 0 > /logs/verifier/reward.txt\n")
    (task / "task.toml").write_text(tomli_w.dumps(raw or source_task()))
    return task


def test_projection_is_pure_and_preserves_independent_requirements():
    raw = source_task()
    original = deepcopy(raw)
    task = TaskConfig.model_validate(project_harbor_task(raw))
    assert raw == original
    assert task.environment.memory_mb == 4096
    assert task.environment.gpus == 0
    verifier = task.verifier.environment
    assert (
        verifier.cpus,
        verifier.memory_mb,
        verifier.storage_mb,
        verifier.gpus,
        verifier.gpu_types,
    ) == (
        16,
        32768,
        1024000,
        1,
        ("H100",),
    )
    assert task.verifier.environment_vars == {"MODE": "strict"}
    assert task.verifier.collect[0].service == "db"
    assert task.steps[0].artifact_sources[0].source == "/app/output"
    assert task.steps[0].artifact_sources[1].destination == "db"
    assert task.steps[0].artifacts == ["result.txt", "logs/verifier/**"]
    restored = TaskConfig.model_validate_json(task.model_dump_json())
    assert restored == task
    assert (
        "verifier.environment: independent_verifier_environment_runtime_unavailable"
        in task_runtime_rejections(restored)
    )


@pytest.mark.parametrize(
    "section,field,value",
    [
        ("environment", "services", {"db": {}}),
        ("verifier", "unrecognized_hook", "foo"),
        ("agent", "privileged", True),
    ],
)
def test_unknown_execution_fields_fail_with_source_path(section, field, value):
    raw = source_task()
    raw.setdefault(section, {})[field] = value
    with pytest.raises(ValueError, match=f"{section}.{field}"):
        project_harbor_task(raw)


@pytest.mark.parametrize("revision", ["main", "v1", "abc", "sha256:"])
def test_origin_rejects_unresolved_revisions(revision):
    with pytest.raises(ValidationError):
        UpstreamOrigin(
            kind="git", locator="https://example.test/repo", revision=revision, release="1"
        )


@pytest.mark.parametrize("destination", ["../escape", "/host", "manifest.json/", ""])
def test_artifact_destination_stays_in_managed_collection(destination):
    with pytest.raises(ValidationError):
        ArtifactSource(source="/app/out", destination=destination)


@pytest.mark.parametrize("profile", ["terminal-bench-4@4.0.0", "another-benchmark@1"])
def test_shared_prepare_and_versioned_source_roundtrip(tmp_path, monkeypatch, profile):
    source = tmp_path / "source"
    task_dir = native_tree(source)
    original = (task_dir / "task.toml").read_bytes()
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    staging = tmp_path / "staging"
    staging.mkdir()
    prepared = benchmark_prepare.prepare_adapter_benchmark(
        profile,
        adapter_override=HarborNativeAdapter(spec(profile)),
        cache_dir=tmp_path / "cache",
        staging_dir=staging,
    )
    bundle = prepared.task_tomls[0].parent
    assert (bundle / "upstream-task.toml").read_bytes() == original
    assert (bundle / "tests/test.sh").read_bytes() == (task_dir / "tests/test.sh").read_bytes()
    task = TaskConfig.model_validate(tomllib.loads(prepared.task_tomls[0].read_text()))
    assert task.task.id == profile + "/upstream/sample"
    assert task.environment.compose_files == ("environment/compose.yaml",)
    assert task.verifier.environment.dockerfile.as_posix() == "tests/Dockerfile"
    assert prepared.manifest["benchmark_profile_provenance"]["compatibility"]["blocked_tasks"] == 1
    registered = prepare_task_bundle_registration(bundle, task_id=task.task.id)
    persisted = TaskBundleSourceSpecV1.from_registration(registered, bucket="task-sources")
    reread = TaskBundleSourceSpecV1.model_validate_json(persisted.model_dump_json())
    assert reread.task_config["verifier"]["environment"]["gpu_types"] == ["H100"]
    assert reread.provenance["upstream_origin"]["revision"] == REVISION
    assert reread.provenance["upstream_task_id"] == "upstream/sample"
    assert reread.provenance["conversion"]["original_config"] == "upstream-task.toml"
    assert registered.source_provenance["conversion"] == reread.provenance["conversion"]


def test_native_subsets_are_explicit_and_bound_to_identity(tmp_path, monkeypatch):
    source = tmp_path / "source"
    native_tree(source)
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    with pytest.raises(ValueError, match="subset profile"):
        HarborNativeAdapter(spec(subset=("upstream/sample",)))
    adapter = HarborNativeAdapter(spec("native-subset@1", subset=("upstream/sample",)))
    for options in ({"limit": 1}, {"instance_ids": {"other"}}):
        with pytest.raises(ValueError, match=r"subset|selected instance"):
            benchmark_prepare.prepare_adapter_benchmark(
                adapter.name,
                adapter_override=adapter,
                cache_dir=tmp_path / "cache",
                staging_dir=tmp_path,
                **options,
            )


async def test_native_publish_uses_common_publisher(tmp_path, monkeypatch):
    source = tmp_path / "source"
    native_tree(source)
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    descriptor = tmp_path / "source.json"
    descriptor.write_text(spec().model_dump_json())
    captured = []

    async def publish(root, **kwargs):
        prepared = kwargs["prepared_adapter"]
        captured.append(prepared.manifest)
        assert (prepared.task_tomls[0].parent / "upstream-task.toml").exists()
        return "published"

    monkeypatch.setattr(
        benchmark_publish.local_benchmark_publish, "publish_local_benchmark", publish
    )
    assert await benchmark_publish.publish_benchmark(harbor_source=descriptor) == "published"
    assert captured[0]["upstream_revision"] == REVISION
    with pytest.raises(ValueError, match="derived profile"):
        await benchmark_publish.publish_benchmark(
            harbor_source=descriptor, execution_profile="nebius-terminus"
        )


async def test_worker_rejects_before_factories_or_model_launch(tmp_path):
    subject = runner(tmp_path, AsyncMock(return_value=True))
    subject.task_config = TaskConfig.model_validate(project_harbor_task(source_task()))
    with pytest.raises(DriverError, match=r"verifier\.environment"):
        await subject.run()
    subject.driver_factory.assert_not_called()
    subject.agent_factory.assert_not_called()
    subject.verifier_factory.assert_not_called()


@pytest.mark.parametrize(
    "body,rewards",
    [
        ('echo 0 > "$LOOM_HARBOR_LOG_DIR/reward.txt"; exit 1', {"resolved": 0.0}),
        ('echo .25 > "$LOOM_HARBOR_LOG_DIR/reward.txt"', {"resolved": 0.25}),
        (
            'echo \'{"score": 0, "partial": 0.5}\' > "$LOOM_HARBOR_LOG_DIR/reward.json"',
            {"score": 0, "partial": 0.5},
        ),
    ],
)
def test_native_reward_bridge_preserves_numeric_rewards(tmp_path, body, rewards):
    result, output = run_bridge(tmp_path, body)
    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text())["rewards"] == rewards


@pytest.mark.parametrize(
    "body",
    [
        "true",
        'echo NaN > "$LOOM_HARBOR_LOG_DIR/reward.txt"',
        'echo garbage > "$LOOM_HARBOR_LOG_DIR/reward.txt"',
        'echo \'{"score": true}\' > "$LOOM_HARBOR_LOG_DIR/reward.json"',
        'echo 1 > "$LOOM_HARBOR_LOG_DIR/reward.txt"; exit 124',
    ],
)
def test_native_reward_failure_cannot_reuse_stale_success(tmp_path, body):
    result, output = run_bridge(tmp_path, body)
    assert result.returncode != 0
    assert not output.exists()


def run_bridge(root: Path, body: str):
    tests, logs = root / "tests", root / "logs"
    tests.mkdir()
    logs.mkdir()
    (tests / "test.sh").write_text(body)
    (logs / "reward.txt").write_text("1")
    output = root / "result.json"
    output.write_text('{"rewards":{"resolved":1}}')
    script = root / "run.sh"
    script.write_bytes(native_verifier_run_sh_bytes())
    result = subprocess.run(
        ["sh", str(script)],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "LOOM_TASK_DIR": str(root),
            "LOOM_HARBOR_LOG_DIR": str(logs),
            "LOOM_HARBOR_TEST_DIR": str(root / "mounted-tests"),
            "LOOM_VERIFIER_OUTPUT": str(output),
        },
        check=False,
    )
    return result, output


def test_cli_preparation_preserves_blocked_tasks_and_fails_atomically(
    tmp_path, monkeypatch, capsys
):
    from argparse import Namespace

    source = tmp_path / "source"
    task = native_tree(source)
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    descriptor = tmp_path / "source.json"
    descriptor.write_text(spec().model_dump_json())
    args = Namespace(
        spec=descriptor,
        output=tmp_path / "output",
        cache_dir=tmp_path / "cache",
        refresh=False,
        instance_ids=None,
    )
    assert datasets_cmd._cmd_prepare_harbor(args) == 0
    assert (
        json.loads((args.output / "manifest.json").read_text())["benchmark_profile_provenance"][
            "compatibility"
        ]["blocked_tasks"]
        == 1
    )
    raw = source_task()
    raw["environment"]["unsupported_mode"] = True
    (task / "task.toml").write_text(tomli_w.dumps(raw))
    args.output = tmp_path / "bad-output"
    assert datasets_cmd._cmd_prepare_harbor(args) == 1
    assert not args.output.exists()
    assert "environment.unsupported_mode" in capsys.readouterr().err


def test_catalog_api_and_cli_share_runtime_blockers():
    from loom.benchmark_readiness import (
        TaskAuditSource,
        build_readiness_item,
        readiness_display_fields,
        render_readiness_json,
    )
    from tests.unit.test_benchmark_readiness import _benchmark

    config = project_harbor_task(source_task())
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fixture/sample",
                config=config,
                source="s3://tasks/sample/",
            )
        ],
        registry_names=set(),
    )
    assert item.valid_task_config_count == 1
    assert item.license_allowed_task_count == 0
    assert item.blocker_reason == "task_requirements_unsupported"
    fields = readiness_display_fields(item)
    assert fields["selectable"] is False
    assert "verifier.environment" in fields["readiness_message"]
    assert json.loads(render_readiness_json([item]))["items"][0]["runtime_blockers"] == list(
        item.runtime_blockers
    )


def test_fragile_native_dockerfile_is_preserved_with_persisted_blocker(tmp_path, monkeypatch):
    from loom_cli.local_benchmark_source_publish import _validate_staged_package_compatibility
    from loom_cli.local_benchmark_validate import LocalBenchmarkValidationError

    source = tmp_path / "source"
    original = native_tree(source)
    dockerfile = original / "environment/Dockerfile"
    dockerfile.write_text("FROM python:3.11-slim\nRUN first && second || true\n")
    monkeypatch.setattr(benchmark_prepare, "_prepare_adapter_source", lambda **kwargs: source)
    staging = tmp_path / "staging"
    staging.mkdir()
    prepared = benchmark_prepare.prepare_adapter_benchmark(
        spec().id,
        adapter_override=HarborNativeAdapter(spec()),
        cache_dir=tmp_path / "cache",
        staging_dir=staging,
    )
    task = TaskConfig.model_validate(tomllib.loads(prepared.task_tomls[0].read_text()))
    assert task.import_blockers[0].code == "TASK_COMPAT_BROAD_TRAILING_TRUE"
    assert (
        prepared.task_tomls[0].parent / "environment/Dockerfile"
    ).read_bytes() == dockerfile.read_bytes()
    assert any(
        "TASK_COMPAT_BROAD_TRAILING_TRUE" in reason for reason in task_runtime_rejections(task)
    )
    bundle = prepared.task_tomls[0].parent
    _validate_staged_package_compatibility(bundle, task_id=task.task.id, native_source=True)
    with pytest.raises(LocalBenchmarkValidationError, match="TASK_COMPAT_BROAD_TRAILING_TRUE"):
        _validate_staged_package_compatibility(bundle, task_id=task.task.id, native_source=False)
    unrecorded = task.model_copy(update={"import_blockers": ()})
    prepared.task_tomls[0].write_text(
        tomli_w.dumps(unrecorded.model_dump(mode="json", exclude_none=True))
    )
    with pytest.raises(LocalBenchmarkValidationError, match="TASK_COMPAT_BROAD_TRAILING_TRUE"):
        _validate_staged_package_compatibility(bundle, task_id=task.task.id, native_source=True)


def test_unknown_format_and_invalid_args_are_not_silently_normalized():
    raw = source_task()
    raw["schema_version"] = "99"
    with pytest.raises(ValueError, match="schema_version="):
        project_harbor_task(raw)
    raw.pop("schema_version")
    raw["verifier"]["args"] = "invalid"
    with pytest.raises(ValueError, match=r"verifier\.args"):
        project_harbor_task(raw)


def test_healthcheck_start_interval_remains_a_durable_runtime_requirement():
    raw = source_task()
    raw["environment"]["healthcheck"] = {"command": "true", "start_interval_sec": 2}
    task = TaskConfig.model_validate(project_harbor_task(raw))
    task = TaskConfig.model_validate_json(task.model_dump_json())
    assert task.environment.healthcheck.start_interval_sec == 2
    assert (
        "environment.healthcheck.start_interval_sec: healthcheck_start_interval_runtime_unavailable"
        in task_runtime_rejections(task)
    )


def test_native_package_cannot_write_through_source_symlinks(tmp_path):
    source = tmp_path / "source"
    task = native_tree(source)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "run.sh"
    sentinel.write_text("user-owned source\n")
    (task / "verifier").symlink_to(outside, target_is_directory=True)
    adapter = HarborNativeAdapter(spec())
    instance = next(adapter.list_instances(source_dir=source, split="test"))
    with pytest.raises(ValueError, match="symlink"):
        adapter.convert_instance(instance, out_dir=tmp_path / "converted")
    assert sentinel.read_text() == "user-owned source\n"


@pytest.mark.parametrize("stamp", ["1", "1.0", "1.1", "1.2", "1.3", "2.0"])
def test_declared_formats_present_in_official_source_preserve_requirements(stamp):
    raw = source_task()
    raw["schema_version"] = stamp
    task = TaskConfig.model_validate(project_harbor_task(raw))
    assert task.schema_version == "1"
    assert task.verifier.environment.gpu_types == ("H100",)
