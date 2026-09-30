import json
from types import SimpleNamespace

import pytest
from scripts.ops import nebius_idle_rollout as rollout


def publication_api(path, payload=None):
    if path.endswith("/artifacts?per_page=100"):
        return {"artifacts": [{"name": "nebius-candidate-" + "a" * 40 + "-42-2", "expired": False}]}
    return {"conclusion": "success", "head_branch": "dev", "head_repository": {"full_name": rollout.REPOSITORY},
            "path": ".github/workflows/nebius-candidate.yml", "event": "push", "head_sha": "a" * 40, "run_attempt": 2}


def test_selects_exact_successful_attempt_not_workflow_default_sha(monkeypatch):
    monkeypatch.setattr(rollout, "github", publication_api)
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0))
    result = rollout.select_publication("42")
    assert result == {"status": "ready", "sha": "a" * 40, "run_id": "42",
                      "artifact": "nebius-candidate-" + "a" * 40 + "-42-2"}


def test_harness_only_does_not_rollout(monkeypatch):
    def api(path, payload=None):
        if "/artifacts?" in path:
            return {"artifacts": [{"name": "nebius-agent-runtime-" + "a" * 40 + "-42-2", "expired": False}]}
        return publication_api(path)
    monkeypatch.setattr(rollout, "github", api)
    assert rollout.select_publication("42")["status"] == "skipped_no_platform_candidate"


@pytest.mark.parametrize("field,value", [("conclusion", "failure"), ("head_branch", "feature"),
                                          ("head_repository", {"full_name": "someone/fork"})])
def test_rejects_ineligible_publication(monkeypatch, field, value):
    monkeypatch.setattr(rollout, "github", lambda *a, **kw: {**publication_api("run"), field: value})
    with pytest.raises(rollout.DeploymentError, match="same-repository"):
        rollout.select_publication("42")


def test_older_candidate_is_not_deployed(monkeypatch):
    def command(argv, **kwargs):
        return SimpleNamespace(returncode=1 if "merge-base" in argv else 0)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    assert rollout.candidate_follows("b" * 40, "a" * 40) is False


def failed_rollout_record():
    return {"schema_version": "loom.nebius-deployment.v1", "status": "failed", "mode": "apply",
            "dispatch_paused": True, "candidate_sha": "a" * 40, "guard_owner": "rollout-" + "b" * 32,
            "cluster_id": "test-cluster", "namespace": "loom-nebius-platform",
            "execution_namespace": "loom-nebius-platform-execution"}


def recovery_api(path, payload=None):
    if path == "actions/runs/91":
        return {"conclusion": "failure", "status": "completed", "head_branch": "dev", "run_attempt": 3,
                "head_repository": {"full_name": rollout.REPOSITORY}, "path": ".github/workflows/nebius-rollout.yml"}
    if path.startswith("actions/workflows/nebius-candidate.yml/runs?"):
        assert "head_sha=" + "a" * 40 in path
        return {"workflow_runs": [{"id": 42, "head_sha": "a" * 40}]}
    return publication_api(path, payload)


def test_recovery_selects_failed_attempt_evidence_and_original_published_candidate(monkeypatch, tmp_path):
    commands = []

    def command(argv, **kwargs):
        commands.append(argv)
        if argv[:3] == ["gh", "run", "download"]:
            assert argv[3:8] == ["91", "--repo", rollout.REPOSITORY, "--name", "nebius-rollout-91-3"]
            directory = tmp_path / "recovery"
            (directory / "deployment-test.json").write_text(json.dumps(failed_rollout_record()))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(rollout, "github", recovery_api)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    result = rollout.select_recovery("91", tmp_path / "recovery")
    assert result == {"status": "ready", "sha": "a" * 40, "run_id": "42",
                      "artifact": "nebius-candidate-" + "a" * 40 + "-42-2",
                      "recovery_evidence": str(tmp_path / "recovery" / "deployment-test.json")}
    assert len(commands) == 2  # Evidence download and the original candidate source check.


@pytest.mark.parametrize("field,value", [
    ("status", "in_progress"), ("conclusion", "success"), ("conclusion", "cancelled"),
    ("head_branch", "feature"), ("head_repository", {"full_name": "someone/fork"}),
    ("path", ".github/workflows/nebius-candidate.yml"),
])
def test_recovery_rejects_nonterminal_or_foreign_rollout_before_download(monkeypatch, tmp_path, field, value):
    monkeypatch.setattr(rollout, "github", lambda path: {**recovery_api(path), field: value})
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: pytest.fail("must not download ineligible evidence"))
    with pytest.raises(rollout.DeploymentError, match="terminal failed same-repository"):
        rollout.select_recovery("91", tmp_path / "recovery")
    assert not (tmp_path / "recovery").exists()


def test_recovery_rejects_publication_that_does_not_match_evidence_candidate(monkeypatch, tmp_path):
    def command(argv, **kwargs):
        (tmp_path / "recovery" / "deployment-test.json").write_text(json.dumps(failed_rollout_record()))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(rollout, "github", recovery_api)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    monkeypatch.setattr(rollout, "select_publication", lambda run_id: {"status": "ready", "sha": "c" * 40})
    with pytest.raises(rollout.DeploymentError, match="not recoverable"):
        rollout.select_recovery("91", tmp_path / "recovery")


def test_recovery_finds_manual_platform_publication_after_newer_harness_only_run(monkeypatch, tmp_path):
    selected_runs = []

    def api(path, payload=None):
        if path.startswith("actions/workflows/nebius-candidate.yml/runs?"):
            # A manual full publication is eligible for the ordinary rollout.
            # Filtering to push publications loses that original candidate.
            if "event=push" in path:
                return {"workflow_runs": []}
            return {"workflow_runs": [{"id": 43, "head_sha": "a" * 40},
                                      {"id": 42, "head_sha": "a" * 40}]}
        return recovery_api(path, payload)

    def publication(run_id):
        selected_runs.append(run_id)
        return {"status": "skipped_no_platform_candidate" if run_id == "43" else "ready",
                "sha": "a" * 40, "run_id": run_id, "artifact": "original-platform-publication"}

    def command(argv, **kwargs):
        (tmp_path / "recovery" / "deployment-test.json").write_text(json.dumps(failed_rollout_record()))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout, "select_publication", publication)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    result = rollout.select_recovery("91", tmp_path / "recovery")
    assert result["sha"] == "a" * 40
    assert result["run_id"] == "42"
    assert result["artifact"] == "original-platform-publication"
    assert selected_runs == ["43", "42"]


def test_recovery_schema_head_comes_from_original_candidate_graph(monkeypatch):
    import io
    import tarfile

    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name, source in {
            "database/migrations/alembic.ini": "[alembic]\nscript_location = %(here)s\n",
            "database/migrations/versions/old_candidate.py": 'revision = "old_candidate_head"\ndown_revision = None\n',
        }.items():
            data = source.encode()
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))

    def command(argv, **kwargs):
        assert argv == ["git", "archive", "a" * 40, "database/migrations"]
        return SimpleNamespace(returncode=0, stdout=output.getvalue())

    monkeypatch.setattr(rollout.subprocess, "run", command)
    assert rollout.candidate_schema_head("a" * 40) == "old_candidate_head"


@pytest.mark.parametrize("field,value", [
    ("candidate_sha", "c" * 40), ("status", "complete"), ("dispatch_paused", False),
    ("cluster_id", "another-cluster"), ("namespace", "another-platform"),
    ("execution_namespace", "another-execution"), ("target_replacement", {"target_id": "replacement"}),
    ("schema_version", "other-schema"), ("mode", "plan"), ("guard_owner", "another-owner"),
])
def test_recovery_rejects_evidence_binding_mismatch_before_deploy(monkeypatch, tmp_path, field, value):
    record = {**failed_rollout_record(), field: value}
    evidence = tmp_path / "failed.json"
    evidence.write_text(json.dumps(record))
    (tmp_path / "candidate.json").write_text(json.dumps({"candidate_sha": "a" * 40}))
    (tmp_path / "runtime-profile.json").write_text("{}")
    config = {key: failed_rollout_record()[key] for key in ("cluster_id", "namespace", "execution_namespace")}
    data = {"environment.json": json.dumps(config), "profile.json": json.dumps({"candidate_sha": "a" * 40}),
            "keyring.json": "{}"}
    monkeypatch.setattr(rollout, "Kubectl", lambda path: SimpleNamespace(get=lambda *args: {"data": data}))
    monkeypatch.setattr(rollout, "build_platform", lambda *args, **kwargs: {})
    monkeypatch.setattr(rollout, "write_platform", lambda *args, **kwargs: None)
    monkeypatch.setattr(rollout, "deploy", lambda *args, **kwargs: pytest.fail("must not deploy mismatched recovery"))
    args = SimpleNamespace(kubeconfig=tmp_path / "unused", namespace=config["namespace"],
                           publication_dir=tmp_path, candidate="a" * 40, evidence_dir=tmp_path,
                           recovery_evidence=evidence, github=False)
    with pytest.raises(rollout.DeploymentError, match="recovery binding differs"):
        rollout.rollout(args)


def test_busy_cli_explains_activity_without_exposing_raw_evidence(monkeypatch, tmp_path, capsys):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr("sys.argv", [
        "rollout", "run", "--publication-dir", str(tmp_path), "--candidate", "a" * 40,
        "--kubeconfig", "unused", "--expected-cluster-id", "unused",
        "--evidence-dir", str(tmp_path),
    ])
    monkeypatch.setattr(rollout, "rollout", lambda args: {
        "status": "skipped_busy", "candidate_sha": "a" * 40,
        "guard": {"active": {"trials": 1, "executions": 1, "builds": 0, "build_cleanup": 0}},
        "private_diagnostic": "must-not-be-published",
    })
    assert rollout.main() == 0
    body = summary.read_text()
    log = capsys.readouterr().out
    assert "1 claimed/running trial(s)" in body
    assert "1 claimed/running trial(s)" in log
    assert "No deployment was applied" in body
    assert "| Image builds awaiting cleanup | 0 |" in body
    assert "must-not-be-published" not in body + log
