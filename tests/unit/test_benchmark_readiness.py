from __future__ import annotations

import json

from loom.benchmark_readiness import (
    NON_V1_SUPPORTED_BENCHMARK_IDS,
    V1_SUPPORTED_BENCHMARK_IDS,
)
from loom_cli.benchmark_readiness import (
    BenchmarkAuditSource,
    TaskAuditSource,
    build_readiness_item,
    render_readiness_json,
    render_readiness_table,
)


def _valid_task_config(task_id: str = "fake-bench/task-001") -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": "Fake task"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _benchmark(benchmark_id: str = "fake-bench") -> BenchmarkAuditSource:
    return BenchmarkAuditSource(
        id=benchmark_id,
        display_name="Fake Bench",
        series="fake",
        upstream_kind="huggingface",
        upstream_locator="fake/source",
        upstream_revision="main",
    )


def test_readiness_marks_legacy_manifest_rows_as_blocked() -> None:
    item = build_readiness_item(
        _benchmark("swe-bench-verified"),
        tasks=[
            TaskAuditSource(
                id="swe-bench-verified/task-001",
                config={},
                source="hf://PRHW/loom-benchmark-swe-bench-verified@main/task-001/",
            ),
        ],
        registry_names={"swe-bench-verified"},
    )

    assert item.readiness_state == "blocked"
    assert item.blocker_reason == "manifest_legacy_missing_task_config"
    assert item.raw_task_count == 1
    assert item.valid_task_config_count == 0
    assert item.source_schemes == ["hf"]
    assert item.materializer_status == "available"


def test_readiness_marks_valid_hf_tasks_as_runnable() -> None:
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fake-bench/task-001",
                config=_valid_task_config(),
                source="hf://PRHW/loom-benchmark-fake-bench@main/task-001/",
            ),
        ],
        registry_names={"fake-bench"},
    )

    assert item.readiness_state == "runnable"
    assert item.blocker_reason is None
    assert item.raw_task_count == 1
    assert item.valid_task_config_count == 1


def test_readiness_marks_known_unsupported_ui_benchmarks_as_blocked() -> None:
    for benchmark_id in ("osworld", "webarena"):
        item = build_readiness_item(
            _benchmark(benchmark_id),
            tasks=[
                TaskAuditSource(
                    id=f"{benchmark_id}/task-001",
                    config=_valid_task_config(f"{benchmark_id}/task-001"),
                    source=f"s3://benchmarks/{benchmark_id}/task-001/",
                ),
            ],
            registry_names={benchmark_id},
        )

        assert item.readiness_state == "blocked"
        assert item.blocker_reason == "unsupported_runtime"
        assert item.raw_task_count == 1
        assert item.valid_task_config_count == 1
        assert item.license_allowed_task_count == 0


def test_readiness_marks_known_non_v1_benchmarks_as_blocked() -> None:
    item = build_readiness_item(
        _benchmark("browsecomp"),
        tasks=[
            TaskAuditSource(
                id="browsecomp/task-001",
                config=_valid_task_config("browsecomp/task-001"),
                source="s3://benchmarks/browsecomp/task-001/",
            ),
        ],
        registry_names={"browsecomp"},
    )

    assert item.readiness_state == "blocked"
    assert item.blocker_reason == "not_v1_supported"
    assert item.raw_task_count == 1
    assert item.valid_task_config_count == 1
    assert item.license_allowed_task_count == 0


def test_v1_supported_math_scope_uses_math500_not_full_hendrycks_math() -> None:
    assert "math-500" in V1_SUPPORTED_BENCHMARK_IDS
    assert "hendrycks-math" not in V1_SUPPORTED_BENCHMARK_IDS
    assert "hendrycks-math" in NON_V1_SUPPORTED_BENCHMARK_IDS


def test_readiness_marks_deferred_gaia_as_blocked_without_manifest() -> None:
    item = build_readiness_item(
        _benchmark("gaia"),
        tasks=[],
        registry_names={"gaia"},
    )

    assert item.readiness_state == "blocked"
    assert item.blocker_reason == "deferred_support"
    assert item.raw_task_count == 0
    assert item.valid_task_config_count == 0
    assert item.license_allowed_task_count == 0


def test_readiness_blocks_unknown_source_scheme() -> None:
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fake-bench/task-001",
                config=_valid_task_config(),
                source="unknown://benchmark/task-001",
            ),
        ],
        registry_names={"fake-bench"},
    )

    assert item.readiness_state == "blocked"
    assert item.blocker_reason == "materializer_missing"
    assert item.materializer_status == "missing"


def test_readiness_renderers_are_stable() -> None:
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fake-bench/task-001",
                config=_valid_task_config(),
                source="hf://PRHW/loom-benchmark-fake-bench@main/task-001/",
            ),
        ],
        registry_names={"fake-bench"},
    )

    payload = json.loads(render_readiness_json([item]))
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "fake-bench"
    assert "READINESS" in render_readiness_table([item])
    assert "fake-bench" in render_readiness_table([item])


def test_arm_catalog_stays_visible_with_explicit_unsupported_diagnostic() -> None:
    from loom.benchmark_readiness import readiness_display_fields
    config = _valid_task_config()
    config["environment"]["cpu_arch"] = "arm64"
    item = build_readiness_item(_benchmark(), tasks=[TaskAuditSource(
        id="fake-bench/task-001", config=config, source="s3://tasks/task-001/",
    )], registry_names={"fake-bench"})
    assert item.raw_task_count == item.valid_task_config_count == 1
    assert item.invalid_task_config_count == 0
    assert item.license_allowed_task_count == 0
    assert item.blocker_reason == "unsupported_cpu_architecture"
    display = readiness_display_fields(item)
    assert display["selectable"] is False
    assert "x86_64 only" in display["readiness_message"]


def test_pending_and_historical_profiles_never_project_as_ready() -> None:
    from dataclasses import replace

    from loom.benchmark_readiness import readiness_display_fields

    for state, reason in (("pending", "benchmark_not_runnable"), ("historical", "benchmark_retired")):
        item = build_readiness_item(
            replace(_benchmark(), execution_state=state),
            tasks=[TaskAuditSource(
                id="fake-bench/task-001", config=_valid_task_config(), source=None,
            )],
            registry_names={"fake-bench"},
        )
        assert item.blocker_reason == reason
        assert item.catalog_admission_state == state
        assert item.license_allowed_task_count == 0
        assert readiness_display_fields(item)["selectable"] is False


def test_admission_does_not_claim_target_or_runtime_qualification() -> None:
    from dataclasses import replace

    from loom.benchmark_readiness import readiness_display_fields

    item = build_readiness_item(
        replace(_benchmark("arbitrary-new-adapter"), execution_state="runnable"),
        tasks=[TaskAuditSource(
            id="arbitrary-new-adapter/task-001",
            config=_valid_task_config("arbitrary-new-adapter/task-001"), source=None,
        )],
        registry_names={"arbitrary-new-adapter"},
    )
    fields = readiness_display_fields(item)
    assert fields["selectable"] is True
    assert fields["catalog_admission_state"] == "admitted"
    assert fields["target_compatibility_state"] == "unknown"
    assert fields["qualification_state"] == "unknown"
    assert fields["qualification_scope"] == []
    assert fields["support_policy_state"] == "supported"


def test_exact_accelerator_is_incompatible_without_changing_admission() -> None:
    from dataclasses import replace

    config = _valid_task_config()
    config["environment"]["gpu_types"] = ["H100"]
    config["environment"]["gpus"] = 1
    item = build_readiness_item(
        replace(_benchmark(), execution_state="runnable"),
        tasks=[TaskAuditSource(id="fake-bench/task-001", config=config, source=None)],
        registry_names={"fake-bench"},
    )
    assert item.catalog_admission_state == "admitted"
    assert item.target_compatibility_state == "incompatible"
    assert item.blocker_reason == "task_requirements_unsupported"
    assert item.qualification_state == "unknown"
    assert any("exact_gpu_model_runtime_unavailable" in reason for reason in item.runtime_blockers)


def test_reviewed_support_policy_cannot_be_overridden_by_task_metadata() -> None:
    from loom.catalog_policy import catalog_support_policy

    config = _valid_task_config("gaia/task-001")
    config["metadata"] = {"support_policy": "supported", "verified": True}
    item = build_readiness_item(
        _benchmark("gaia"),
        tasks=[TaskAuditSource(id="gaia/task-001", config=config, source=None)],
        registry_names={"gaia"},
    )
    assert catalog_support_policy("gaia").state == "deferred_support"
    assert item.blocker_reason == "deferred_support"
    assert item.qualification_state == "unknown"


def test_verifier_hardware_uses_same_target_rejection_as_agent_hardware() -> None:
    from dataclasses import replace

    config = _valid_task_config()
    config["verifier"].update({
        "env_mode": "separate",
        "environment": {"os": "linux", "docker_image": "python:3.12-slim", "cpu_arch": "arm64"},
    })
    item = build_readiness_item(
        replace(_benchmark(), execution_state="runnable"),
        tasks=[TaskAuditSource(id="fake-bench/task-001", config=config, source=None)],
        registry_names={"fake-bench"},
    )
    assert item.catalog_admission_state == "admitted"
    assert item.target_compatibility_state == "incompatible"
    assert item.blocker_reason == "unsupported_cpu_architecture"
    assert any("verifier.environment.cpu_arch" in reason for reason in item.runtime_blockers)
