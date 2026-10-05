from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_operator_runbook_uses_current_provider_model_syntax() -> None:
    runbook = _read("docs/runbooks/operator-runbook.md")

    assert "loom providers models refresh" not in runbook
    assert "loom providers models list" not in runbook
    assert "loom providers models CONNECTION --refresh" in runbook
    assert "loom providers models CONNECTION --preflight MODEL" in runbook


def test_staging_validation_smoke_matches_current_gate_contract() -> None:
    runbook = _read("docs/runbooks/staging-launch.md")

    assert "scripts/staging_smoke_gate.py" in runbook
    assert "--provider-connection-name CONNECTION" in runbook
    assert "--provider-model-provider PROVIDER" in runbook
    assert "--provider-model-name MODEL" in runbook
    assert "--required-worker-pool local" in runbook
    assert "local/disposable diagnostic" in runbook
    assert "--fail-on-skip" in runbook
    assert "--team-a-token file:/secure/path/team-a-token" in runbook
    assert "--team-b-token file:/secure/path/team-b-token" in runbook


def test_operator_runbook_staging_gate_matches_current_launch_scope() -> None:
    runbook = _read("docs/runbooks/staging-launch.md")
    gate_section = runbook.split("## Public route and authentication checks", maxsplit=1)[1].split(
        "## Promotion manifest",
        maxsplit=1,
    )[0]
    normalized_gate_section = " ".join(gate_section.split())

    assert "My team" in normalized_gate_section
    assert "All teams" in normalized_gate_section
    assert "owner-team labels" in normalized_gate_section
    assert "clone config" in normalized_gate_section
    assert "artifact reuse" in normalized_gate_section
    assert "provenance" in normalized_gate_section
    assert "cross-team denial" in normalized_gate_section


def test_cluster_deploy_lists_taskset_fence_canary_as_operator_maintenance() -> None:
    cluster_deploy = _read("docs/architecture/cluster-deploy.md")

    assert "loom cluster taskset-fence-canary" in cluster_deploy
    assert "Use each subcommand's `--help` for its exact required arguments" in cluster_deploy


def test_cluster_deploy_docs_do_not_advertise_missing_trial_download_commands() -> None:
    cluster_deploy = _read("docs/architecture/cluster-deploy.md")

    assert "loom eval run" not in cluster_deploy
    assert "loom eval trial" not in cluster_deploy


def test_user_guide_documents_current_batch_create_command() -> None:
    user_guide = _read("docs/user-guide.md")

    assert "loom eval batch create" in user_guide
    assert "--provider smoke-openai" in user_guide
    assert "--model gpt-4o-mini" in user_guide
    assert "--agent direct-completion" in user_guide
