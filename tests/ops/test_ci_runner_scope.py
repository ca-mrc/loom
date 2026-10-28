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


def pool_selection(changes, *, manual=False):
    plan = plan_validations(changed_paths=changes, labels=set(),
                            event_name="workflow_dispatch" if manual else "pull_request",
                            shared_pool_simulations=manual)
    selection = dict(
        manifest=load_manifest(ROOT / "config/component-ownership.toml"),
        tracked_paths=_tracked_paths(ROOT), repo_root=ROOT,
        changed_paths=plan.test_changes, diff_paths=plan.test_diff, test_scope="nebius",
        include_shared_pool=plan.shared_pool_simulations,
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
@pytest.mark.parametrize("manual", [False, True])
def test_runner_scope_does_not_hide_shared_or_unknown_inputs(shared, manual):
    plan, pool, selection = pool_selection([CONTROLLER, shared], manual=manual)
    full = selected_test_paths(
        manifest=selection["manifest"], tracked_paths=selection["tracked_paths"],
        repo_root=ROOT, lane="nebius-pool", test_scope="nebius", include_shared_pool=manual,
    )
    assert pool == full and pool
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert sum("/test_nebius_shared_pool_" in path for path in pool) == (23 if manual else 0)


@pytest.mark.parametrize("operator", ["scripts/ops/nebius_pool_activation_stage.py",
                                       "scripts/ops/nebius_shared_pool_activation.py"])
def test_runner_change_keeps_an_actual_pool_operators_selected_consumers(operator):
    _, expected, _ = pool_selection([operator])
    plan, pool, _ = pool_selection([CONTROLLER, operator])
    assert expected and pool == expected
    assert not plan.unowned_runtime


@pytest.mark.parametrize("extra", ["src/loom_service/app.py", "tests/ops/conftest.py",
                                  "scripts/ops/ci_runner_unclassified.py"])
def test_mixed_application_fixture_and_unknown_changes_keep_root_consumers(extra):
    _, _, selection = pool_selection([extra])
    direct = set(selected_test_paths(lane="tests-root", **selection))
    _, _, mixed_selection = pool_selection([CONTROLLER, extra])
    mixed = set(selected_test_paths(lane="tests-root", **mixed_selection))
    assert direct <= mixed and direct
    if extra == "src/loom_service/app.py":
        assert any(not path.startswith("tests/ops/test_ci_runner_")
                   and path != "tests/ops/test_ci_unified_runner_supply.py" for path in direct)
