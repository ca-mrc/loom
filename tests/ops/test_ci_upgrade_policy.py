from __future__ import annotations

import json
from pathlib import Path

from scripts.check_ci_upgrade_policy import check_upgrade_policy

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_repository_upgrade_policy_is_complete() -> None:
    assert check_upgrade_policy(
        policy_file=REPO_ROOT / "config" / "ci-upgrade-policy.json",
        lock_file=REPO_ROOT / "config" / "ci-actions-lock.json",
    ) == ()


def test_batch_larger_than_two_fails_closed(tmp_path: Path) -> None:
    lock = {
        "actions": {
            f"owner/action-{index}": {"sha": str(index) * 40, "version": "v1"}
            for index in range(1, 4)
        },
    }
    policy = {
        "schema_version": 1,
        "max_actions_per_batch": 2,
        "node24_minimum_runner": "2.327.1",
        "required_canary_contexts": [
            "repository-checks",
            "images-gate",
            "cluster-smoke-gate",
            "staging-smoke-gate",
        ],
        "batches": [
            {
                "name": "too-wide",
                "actions": list(lock["actions"]),
                "compatibility_tests": ["test"],
                "rollback": lock["actions"],
            },
        ],
    }
    policy_file = tmp_path / "policy.json"
    lock_file = tmp_path / "lock.json"
    policy_file.write_text(json.dumps(policy), encoding="utf-8")
    lock_file.write_text(json.dumps(lock), encoding="utf-8")

    errors = check_upgrade_policy(policy_file=policy_file, lock_file=lock_file)

    assert any("unique action names" in error for error in errors)


def test_missing_action_and_rollback_fail_closed(tmp_path: Path) -> None:
    lock = {"actions": {"owner/action": {"sha": "a" * 40, "version": "v1"}}}
    policy = {
        "schema_version": 1,
        "max_actions_per_batch": 2,
        "node24_minimum_runner": "2.327.1",
        "required_canary_contexts": [
            "repository-checks",
            "images-gate",
            "cluster-smoke-gate",
            "staging-smoke-gate",
        ],
        "batches": [
            {
                "name": "bad",
                "actions": ["owner/other"],
                "compatibility_tests": ["test"],
                "rollback": {},
            },
        ],
    }
    policy_file = tmp_path / "policy.json"
    lock_file = tmp_path / "lock.json"
    policy_file.write_text(json.dumps(policy), encoding="utf-8")
    lock_file.write_text(json.dumps(lock), encoding="utf-8")

    errors = check_upgrade_policy(policy_file=policy_file, lock_file=lock_file)

    assert any("rollback must cover" in error for error in errors)
    assert any("cover exactly the action lock" in error for error in errors)
