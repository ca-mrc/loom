"""Unlaunched shared-pool simulations require explicit manual qualification."""
from pathlib import Path

import pytest
import yaml
from scripts import component_ownership as ownership
from scripts import plan_ci_validations as planner
from scripts.ops import nebius_pool_test_plan as candidate

ROOT = Path(__file__).resolve().parents[2]
FAST_SUCCESSOR = "tests/ops/test_nebius_pool_refresh_successor.py"
SIMULATED_SUCCESSOR = "tests/ops/test_nebius_shared_pool_refresh_successor.py"


@pytest.mark.parametrize("event", ["pull_request", "merge_group", "push", "schedule", "workflow_dispatch"])
@pytest.mark.parametrize("change", ["uv.lock", "scripts/ops/nebius_shared_pool_api.py",
                                   FAST_SUCCESSOR, SIMULATED_SUCCESSOR])
def test_default_events_exclude_simulations_but_keep_pool_operators(event, change):
    plan = planner.plan_validations(changed_paths=[change], labels=(), event_name=event)
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    paths = ownership.selected_test_paths(
        manifest, tracked_paths=ownership._tracked_paths(ROOT), lane="nebius-pool",
        repo_root=ROOT, changed_paths=plan.test_changes, diff_paths=plan.test_diff,
    )
    assert not any(manifest.test_owner_for_path(path).id == "nebius-shared-pool-ops" for path in paths)
    if change == "uv.lock":
        assert paths and all(manifest.test_owner_for_path(path).id == "nebius-pool-ops" for path in paths)
    if change in {"uv.lock", FAST_SUCCESSOR}:
        assert FAST_SUCCESSOR in paths
    assert plan.github_outputs()["shared_pool_simulations"] == "false"


def test_explicit_manual_selection_matches_every_shard_execution():
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    tracked = ownership._tracked_paths(ROOT)
    plan = planner.plan_validations(changed_paths=["uv.lock"], labels=(),
                                    event_name="workflow_dispatch", shared_pool_simulations=True)
    assert plan.github_outputs()["shared_pool_simulations"] == "true"
    args = dict(manifest=manifest, tracked_paths=tracked, lane="nebius-pool", repo_root=ROOT,
                changed_paths=plan.test_changes, diff_paths=plan.test_diff, include_shared_pool=True)
    paths = ownership.selected_test_paths(**args)
    shared = {path for path in paths if manifest.test_owner_for_path(path).id == "nebius-shared-pool-ops"}
    assert SIMULATED_SUCCESSOR in shared and FAST_SUCCESSOR in paths
    matrix = ownership.test_shard_matrix(**args)
    executed = [path for row in matrix for path in ownership.selected_test_paths(
        **args, shard_index=row["shard_index"], shard_count=row["shard_count"])]
    assert len(executed) == len(set(executed)) and set(executed) == set(paths)


@pytest.mark.parametrize("event", ["pull_request", "merge_group", "push", "schedule"])
def test_nonmanual_events_cannot_enable_simulations(event):
    with pytest.raises(ValueError, match="manual"):
        planner.plan_validations(changed_paths=["uv.lock"], labels=(), event_name=event,
                                 shared_pool_simulations=True)


@pytest.mark.parametrize("label", tuple(planner.LABEL_TO_CHECK))
def test_selector_labels_keep_full_ordinary_coverage_without_enabling_simulations(label):
    plan = planner.plan_validations(changed_paths=["uv.lock"], labels=(label,), event_name="pull_request")
    assert plan.test_changes == () and not plan.shared_pool_simulations
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    paths = ownership.selected_test_paths(manifest, tracked_paths=ownership._tracked_paths(ROOT),
                                          lane="nebius-pool", repo_root=ROOT)
    assert paths and not any(manifest.test_owner_for_path(path).manual_only for path in paths)


def test_candidate_default_does_not_execute_or_claim_simulations(monkeypatch):
    monkeypatch.setattr(candidate, "candidate_changes", lambda *_: ("uv.lock",))
    value = candidate.plan(ROOT, "a" * 40, read=lambda _: [])
    paths = [path for row in value["matrix"]["include"] for path in row["paths"]]
    assert paths and not any("/test_nebius_shared_pool_" in path for path in paths)
    assert FAST_SUCCESSOR in paths
    assert value["reused_files"] == 0


def test_workflow_manual_input_is_passed_to_planner_and_execution():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]["shared_pool_simulations"]["default"] is False
    jobs = workflow["jobs"]
    assert "shared_pool_simulations" in jobs["workflow-plan"]["outputs"]
    step = next(step for step in jobs["workflow-plan"]["steps"] if step.get("id") == "plan")
    assert "--shared-pool-simulations" in step["run"]
    assert jobs["nebius-pool"]["env"]["SHARED_POOL_SIMULATIONS"] == "${{ needs.workflow-plan.outputs.shared_pool_simulations }}"
    step = next(step for step in jobs["nebius-pool"]["steps"] if step.get("id") == "manifest")
    assert '--include-shared-pool "${SHARED_POOL_SIMULATIONS:-false}"' in step["run"]


@pytest.mark.parametrize("raw, error", [
    ({"manual_only": "true"}, "manual_only must be a boolean"),
    ({"manual_only": True, "lane": "tests-root"}, "manual_only requires the nebius-pool lane"),
])
def test_manual_ownership_requires_the_explicit_pool_boundary(raw, error):
    with pytest.raises(ownership.ManifestError, match=error):
        ownership._test_suite({"id": "manual", "language": "python", "lane": "nebius-pool",
                               "include_paths": ["tests/ops/test_manual.py"], **raw})


def test_long_recovery_cases_keep_distinct_manual_jobs():
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    policy = manifest.test_shard_policy("nebius-pool")
    pins = {pin.path: pin.shard_index for pin in policy.pins}
    assert pins["tests/ops/test_nebius_shared_pool_handoff_census_recovery.py"] == 19
    assert pins["tests/ops/test_nebius_shared_pool_handoff_census_preactivation_recovery.py"] == 3
    assert {suite.id for suite in manifest.test_suites if suite.manual_only} == {"nebius-shared-pool-ops"}


def test_fast_rules_stay_in_the_default_root_selection():
    manifest = ownership.load_manifest(ROOT / "config/component-ownership.toml")
    fast = {"tests/unit/test_nebius_pool_activation_record.py",
            "tests/unit/test_nebius_shared_pool_contract_inputs.py"}
    tracked = ownership._tracked_paths(ROOT)
    assert fast <= set(tracked)
    paths = ownership.selected_test_paths(manifest, tracked_paths=tracked, lane="tests-root",
                                          repo_root=ROOT, changed_paths=("uv.lock",))
    assert fast <= set(paths)
