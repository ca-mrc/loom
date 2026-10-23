"""The live CI runner controller has its own scope, not legacy pool semantics."""
from pathlib import Path

import pytest
from scripts.component_ownership import (
    _tracked_paths,
    load_manifest,
    selected_test_paths,
)
from scripts.component_ownership import (
    test_shard_matrix as shard_matrix,
)
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER = "scripts/ops/ci_runner_supply.py"


def pool_selection(changes):
    plan = plan_validations(changed_paths=changes, labels=set(), event_name="pull_request")
    selection = dict(
        manifest=load_manifest(ROOT / "config/component-ownership.toml"),
        tracked_paths=_tracked_paths(ROOT), repo_root=ROOT,
        changed_paths=plan.test_changes, test_scope="nebius",
    )
    return plan, selected_test_paths(lane="nebius-pool", **selection), selection


def test_runner_controller_keeps_native_specialists_without_legacy_pool_replay():
    plan, pool, selection = pool_selection([CONTROLLER])
    assert not plan.unowned_runtime
    assert plan.test_changes == (CONTROLLER,)
    assert plan.selected_heavy_checks() == {"images", "cluster_smoke", "staging_smoke"}
    assert pool == ()
    assert shard_matrix(lane="nebius-pool", **selection) == ()
    root = selected_test_paths(lane="tests-root", **selection)
    assert {"tests/ops/test_ci_runner_supply_static_matrix.py",
            "tests/ops/test_ci_unified_runner_supply.py",
            "tests/ops/test_ci_runner_supply_cancellation.py"} <= set(root)
    assert all(path.startswith("tests/ops/test_ci_runner_")
               or path == "tests/ops/test_ci_unified_runner_supply.py" for path in root)
    assert not any((plan.tests_packages, plan.go_checks, plan.nebius_iac,
                    plan.runtime_payload, plan.locked_environments))


@pytest.mark.parametrize("shared", ["tests/ops/conftest.py", "uv.lock",
                                    "scripts/ops/ci_runner_unclassified.py"])
def test_runner_scope_does_not_hide_shared_or_unknown_inputs(shared):
    plan, pool, selection = pool_selection([CONTROLLER, shared])
    full = selected_test_paths(
        manifest=selection["manifest"], tracked_paths=selection["tracked_paths"],
        repo_root=ROOT, lane="nebius-pool", test_scope="nebius",
    )
    assert pool == full and pool
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_runner_change_keeps_an_actual_pool_operators_selected_consumers():
    operator = "scripts/ops/nebius_shared_pool_activation.py"
    _, expected, _ = pool_selection([operator])
    plan, pool, _ = pool_selection([CONTROLLER, operator])
    assert expected and pool == expected
    assert not plan.unowned_runtime
