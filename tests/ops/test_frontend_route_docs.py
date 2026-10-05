from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_current_environment_docs_use_only_path_prefix_routes() -> None:
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    naming = (ROOT / "docs/architecture/env-naming-convention.md").read_text(
        encoding="utf-8",
    )
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(
        encoding="utf-8",
    )

    assert "https://yylx.world/<short_name>" in naming
    for route in (
        "https://yylx.world/dev",
        "https://yylx.world/staging",
        "https://yylx.world/prod",
    ):
        assert route in contributing
        assert route in runbook
    for stale_route in ("dev.yylx.world", "staging.yylx.world", "prod.yylx.world"):
        for document in (contributing, naming, runbook):
            assert stale_route not in document
