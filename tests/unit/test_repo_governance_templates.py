from pathlib import Path

from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_contributing_and_pr_template_accept_issue_scoped_external_prs() -> None:
    contributing = _read("CONTRIBUTING.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    assert "External pull requests are not accepted" not in contributing
    assert "External pull requests are accepted for issue-scoped work" in contributing
    assert "## Linked Issue" in pr_template
    assert "Refs #" in pr_template
    assert "pull request code must not rely on protected secrets" in pr_template


def test_all_non_draft_prs_use_author_neutral_ci_only_auto_merge() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    assert "For this normal `dev` PR" in pr_template

    for document in (contributing, quickstart, pr_template):
        normalized_document = " ".join(document.split()).lower()
        assert "ci:merge-ready" not in normalized_document
        assert (
            "every non-draft" in normalized_document
            or "every relevant non-draft" in normalized_document
        )
        assert "no human approval" in normalized_document
        assert "no codeowner approval" in normalized_document
        assert "no conversation resolution" in normalized_document
        for stale_review_gate in (
            "need platform-admin review",
            "require platform-admin review",
            "requires platform-admin review",
        ):
            assert stale_review_gate not in normalized_document


def test_governance_docs_define_path_inferred_validation_gates() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    for gate in (
        "repository-checks",
        "images-gate",
        "cluster-smoke-gate",
        "staging-smoke-gate",
    ):
        for document in (contributing, quickstart, pr_template):
            assert gate in document

    assert "Labels may add validation but cannot remove path-inferred validation" in contributing
    assert "squash auto-merge" in pr_template

    for document in (contributing, quickstart):
        normalized_document = " ".join(document.split())
        assert "visible and successful on the current head SHA" in normalized_document
        assert "main protected promotion" in normalized_document
        assert "main-promotion-gate" in normalized_document
        assert "same-repository `dev`" in normalized_document
        assert "manual squash merge" not in normalized_document.lower()
        assert "never enable auto-merge" not in normalized_document.lower()

    normalized_template = " ".join(pr_template.split())
    assert "succeed on the current head SHA" in normalized_template
    assert "author or reviewer" in normalized_template
    assert "only merge authority" in normalized_template


def test_main_promotion_is_ci_auto_merged_and_separates_release_approval() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")
    release_template = _read(".github/ISSUE_TEMPLATE/release.yml")

    for document in (
        contributing,
        quickstart,
        operator_runbook,
        pr_template,
        release_template,
    ):
        normalized_document = " ".join(document.split()).lower()
        assert "manual squash" not in normalized_document
        assert "never enable auto-merge" not in normalized_document
        assert "auto-merge" in normalized_document

    for document in (contributing, operator_runbook, release_template):
        normalized_document = " ".join(document.split()).lower()
        assert "release_owner_approval" in normalized_document
        assert "production environment approval" in normalized_document
        assert "not interchangeable" in normalized_document



def test_release_promotion_binds_main_merge_to_prod_evidence() -> None:
    contributing = _read("CONTRIBUTING.md")
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    staging_validation = _read("docs/runbooks/staging-launch.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    for required_text in (
        "Candidate SHA",
        "Staging URL",
        "Release gate workflow run and run ID",
        "Main promotion gate workflow run",
        "Production Environment approver",
        "main-promotion-gate",
    ):
        assert required_text in pr_template

    assert "immutable `vX.Y.Z` production release tags" in contributing
    assert "Never force-move or reuse a published prod tag" in contributing
    assert "`prod_tag`" in operator_runbook
    assert "`frontend_route_evidence`" in staging_validation


def test_issue_templates_use_current_loom_language() -> None:
    template_dir = ROOT / ".github" / "ISSUE_TEMPLATE"
    template_text = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(template_dir.glob("*.yml"))
    )

    assert "User evaluation flow" in template_text
    assert "Model provider / gateway" in template_text
    assert "Benchmark catalog / task data" in template_text
    assert "Agent adapter / sandbox runtime" in template_text
    assert "vX.Y.Z` tag exists on the merged `main` commit" in template_text


def test_codeowners_does_not_route_every_change_to_one_reviewer() -> None:
    assert not (ROOT / ".github/CODEOWNERS").exists()


def test_ci_docs_only_fast_path_includes_repo_metadata_not_workflows() -> None:
    for metadata_path in (
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/ISSUE_TEMPLATE/bug.yml",
    ):
        plan = plan_validations(
            changed_paths=[metadata_path], labels=set(), event_name="pull_request"
        )
        assert plan.docs_only is True

    codeowners_plan = plan_validations(
        changed_paths=[".github/CODEOWNERS"], labels=set(), event_name="pull_request"
    )
    assert codeowners_plan.docs_only is False
    assert codeowners_plan.selected_heavy_checks() == set(HEAVY_CHECKS)

    workflow_plan = plan_validations(
        changed_paths=[".github/workflows/ci.yml"],
        labels=set(),
        event_name="pull_request",
    )
    assert workflow_plan.docs_only is False
