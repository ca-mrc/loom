from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _normalize_command(text: str) -> str:
    return " ".join(text.split())


def test_repository_checks_ruff_scope_matches_repo_wide_local_lint() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["lint-and-static"]["steps"]
    ruff_step = next(step for step in steps if step.get("name") == "Ruff")

    assert ruff_step["run"] == (
        "uv run --no-sync ruff check src tests packages database/migrations "
        "database/capacity_guard_migrations database/capacity_migrations"
    )

    for doc in ("CONTRIBUTING.md", "docs/contributing/contributor-quickstart.md"):
        assert ruff_step["run"] in _normalize_command((REPO_ROOT / doc).read_text())


def test_local_python_version_is_pinned_to_ci_interpreter() -> None:
    assert (REPO_ROOT / ".python-version").read_text(encoding="utf-8").strip() == "3.11"


def test_workflow_plan_enforces_repository_paths_before_docs_only_fast_path() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["workflow-plan"]["steps"]
    step_names = [step.get("name") for step in steps]

    policy_index = step_names.index("Enforce repository path policy")
    planner_index = step_names.index("Plan required validations")
    docs_only_index = step_names.index("Docs-only fast path")

    assert policy_index < planner_index < docs_only_index
    assert steps[policy_index]["run"] == "python3 scripts/check_repository_paths.py"


def test_contributor_quickstart_uses_ci_python_for_local_verification() -> None:
    text = (REPO_ROOT / "docs/contributing/contributor-quickstart.md").read_text(encoding="utf-8")

    assert "uv python install 3.11" in text
    assert "uv sync --locked --all-packages --extra dev --python 3.11" in text


def test_contributor_quickstart_documents_nebius_and_compatibility_verification() -> None:
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/ci.yml").read_text())
    root_job = workflow["jobs"]["tests-root"]
    root_steps = root_job["steps"]
    package_steps = workflow["jobs"]["tests-packages"]["steps"]
    root_pytest_step = next(
        step for step in root_steps if step.get("name") == "Pytest — manifest-owned root shard"
    )
    sibling_pytest_step = next(
        step for step in package_steps if step.get("name") == "Pytest — manifest-owned package lane"
    )

    text = (REPO_ROOT / "docs/contributing/contributor-quickstart.md").read_text(encoding="utf-8")
    normalized_text = _normalize_command(text)
    root_shards = root_job["strategy"]["matrix"]["include"]
    assert {shard["shard_index"] for shard in root_shards} == set(range(8))
    assert "test-paths --lane tests-root" in root_pytest_step["run"]
    assert "--shard-index" in root_pytest_step["run"]
    assert "uv run --no-sync pytest" in root_pytest_step["run"]
    assert "test-paths --lane tests-packages" in sibling_pytest_step["run"]
    assert "uv run --no-sync pytest" in sibling_pytest_step["run"]
    assert "test-paths --lane tests-root" in normalized_text
    assert "test-paths --lane tests-packages" in normalized_text
    assert "--cov-append" in normalized_text
    assert "--test-scope nebius" in normalized_text
    assert '-m "not legacy_pool" -p no:cov' in normalized_text
    assert "--test-scope all" in normalized_text
    assert 'legacy_compatibility=true' in text
    assert "uv run --no-sync coverage report --fail-under=70" in text
