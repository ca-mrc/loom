import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import nebius_idle_rollout as rollout


def publication_api(path, payload=None):
    if path.endswith("/artifacts?per_page=100"):
        return {"artifacts": [{"name": "nebius-candidate-" + "a" * 40 + "-42-2", "expired": False}]}
    return {"conclusion": "success", "head_branch": "dev", "head_repository": {"full_name": rollout.REPOSITORY, "id": 1281629473},
            "path": ".github/workflows/nebius-candidate.yml", "event": "push", "head_sha": "a" * 40, "run_attempt": 2}


def test_selects_exact_successful_attempt_not_workflow_default_sha(monkeypatch):
    monkeypatch.setattr(rollout, "github", publication_api)
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0))
    result = rollout.select_publication("42")
    assert result == {"status": "ready", "sha": "a" * 40, "run_id": "42",
                      "artifact": "nebius-candidate-" + "a" * 40 + "-42-2"}


def test_transferred_publication_keeps_stable_repository_authority(monkeypatch):
    run = publication_api("run")
    run["head_repository"] = {"full_name": "ca-mrc/loom", "id": 1281629473}
    monkeypatch.setattr(rollout, "github", lambda path: publication_api(path) if "/artifacts?" in path else run)
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0))
    assert rollout.select_publication("42")["status"] == "ready"
    run["head_repository"]["id"] = 42
    with pytest.raises(rollout.DeploymentError, match="same-repository"):
        rollout.select_publication("42")


def test_github_api_uses_stable_id_without_slug_redirect(monkeypatch):
    def command(argv, **kwargs):
        assert argv == ["gh", "api", "repositories/1281629473/actions/runs/42"]
        return SimpleNamespace(returncode=0, stdout='{"id":42}')
    monkeypatch.setattr(rollout.subprocess, "run", command)
    assert rollout.github("actions/runs/42") == {"id": 42}


def test_canonical_download_name_is_resolved_from_stable_repository(monkeypatch):
    monkeypatch.setattr(rollout, "github", lambda path: {"full_name": "ca-mrc/loom", "id": 1281629473})
    assert rollout.canonical_repository_name() == "ca-mrc/loom"
    monkeypatch.setattr(rollout, "github", lambda path: {"full_name": "ca-mrc/loom", "id": 42})
    with pytest.raises(rollout.DeploymentError, match="repository identity"):
        rollout.canonical_repository_name()


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
    if path == "":
        return {"full_name": "ca-mrc/loom", "id": 1281629473}
    if path == "actions/runs/91":
        return {"conclusion": "failure", "status": "completed", "head_branch": "dev", "run_attempt": 3,
                "head_repository": {"full_name": rollout.REPOSITORY, "id": 1281629473}, "path": ".github/workflows/nebius-rollout.yml"}
    if path.startswith("actions/workflows/nebius-candidate.yml/runs?"):
        assert "head_sha=" + "a" * 40 in path
        return {"workflow_runs": [{"id": 42, "head_sha": "a" * 40}]}
    return publication_api(path, payload)


def test_recovery_selects_failed_attempt_evidence_and_original_published_candidate(monkeypatch, tmp_path):
    commands = []

    def command(argv, **kwargs):
        commands.append(argv)
        if argv[:3] == ["gh", "run", "download"]:
            assert argv[3:8] == ["91", "--repo", "ca-mrc/loom", "--name", "nebius-rollout-91-3"]
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
    ("candidate_sha", "c" * 40), ("status", "complete"),
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
    monkeypatch.setattr(rollout, "deploy", lambda *args, **kwargs: pytest.fail("must not deploy mismatched recovery"))
    args = SimpleNamespace(kubeconfig=tmp_path / "unused", namespace=config["namespace"],
                           publication_dir=tmp_path, candidate="a" * 40, evidence_dir=tmp_path,
                           recovery_evidence=evidence, github=False)
    with pytest.raises(rollout.DeploymentError, match="recovery"):
        rollout.rollout(args)


def running_rollout_intent():
    record = failed_rollout_record()
    record.pop("dispatch_paused")
    return {**record, "status": "running", "run_id": 91, "run_attempt": 3,
            "previous_candidate_sha": "c" * 40}


@pytest.mark.parametrize("conclusion", ["failure", "timed_out"])
def test_runner_loss_recovers_server_intent_when_artifact_is_missing(monkeypatch, tmp_path, conclusion):
    payload = {**running_rollout_intent(), "private_diagnostic": "must-not-be-copied"}

    def api(path, data=None):
        if path.startswith("deployments?"):
            return [{"environment": "nebius-integration", "sha": "a" * 40, "payload": payload}]
        response = recovery_api(path, data)
        return {**response, "conclusion": conclusion} if path == "actions/runs/91" else response

    def command(argv, **kwargs):
        return SimpleNamespace(returncode=1 if argv[:3] == ["gh", "run", "download"] else 0)

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    result = rollout.select_recovery("91", tmp_path / "recovery")
    record = json.loads(Path(result["recovery_evidence"]).read_text())
    assert record == running_rollout_intent()
    assert result["sha"] == "a" * 40


@pytest.mark.parametrize("field,value", [
    ("run_id", 92), ("run_attempt", 2), ("candidate_sha", "b" * 40),
    ("status", "complete"), ("mode", "plan"), ("schema_version", "another-schema"),
    ("previous_candidate_sha", "invalid"), ("guard_owner", "foreign-owner"),
    ("target_replacement", {"target_id": "replacement"}),
])
def test_runner_loss_rejects_foreign_or_invalid_server_intent(monkeypatch, tmp_path, field, value):
    payload = {**running_rollout_intent(), field: value}

    def api(path, data=None):
        if path.startswith("deployments?"):
            return [{"environment": "nebius-integration", "sha": "a" * 40, "payload": payload}]
        return recovery_api(path, data)

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=1))
    with pytest.raises(rollout.DeploymentError):
        rollout.select_recovery("91", tmp_path / "recovery")


@pytest.mark.parametrize("mutation", ["environment", "sha", "duplicate"])
def test_runner_loss_requires_unique_matching_deployment_binding(monkeypatch, tmp_path, mutation):
    row = {"environment": "nebius-integration", "sha": "a" * 40, "payload": running_rollout_intent()}
    rows = [row, row] if mutation == "duplicate" else [{**row, mutation: "foreign-binding"}]

    def api(path, data=None):
        return rows if path.startswith("deployments?") else recovery_api(path, data)

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=1))
    with pytest.raises(rollout.DeploymentError):
        rollout.select_recovery("91", tmp_path / "recovery")


def test_invalid_retained_artifact_cannot_be_hidden_by_server_intent_fallback(monkeypatch, tmp_path):
    def api(path, data=None):
        if path.startswith("deployments?"):
            pytest.fail("available invalid artifact must not be replaced by fallback")
        return recovery_api(path, data)

    def command(argv, **kwargs):
        (tmp_path / "recovery" / "deployment-test.json").write_text(json.dumps({
            **failed_rollout_record(), "candidate_sha": "not-a-commit",
        }))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout.subprocess, "run", command)
    with pytest.raises(rollout.DeploymentError):
        rollout.select_recovery("91", tmp_path / "recovery")


def wrapper_inputs(monkeypatch, tmp_path, *, current="a" * 40, record=None, github=False):
    (tmp_path / "candidate.json").write_text(json.dumps({"candidate_sha": "a" * 40}))
    (tmp_path / "runtime-profile.json").write_text("{}")
    config = {key: failed_rollout_record()[key] for key in ("cluster_id", "namespace", "execution_namespace")}
    config["public_host"] = "platform.example.test"
    data = {"environment.json": json.dumps(config), "profile.json": json.dumps({"candidate_sha": current}),
            "keyring.json": "{}"}
    monkeypatch.setattr(rollout, "Kubectl", lambda path: SimpleNamespace(get=lambda *args: {"data": data}))
    monkeypatch.setattr(rollout, "candidate_schema_head", lambda sha: "original_head")
    monkeypatch.setattr(rollout, "render_candidate", lambda *args: None)
    args = SimpleNamespace(kubeconfig=tmp_path / "unused", namespace=config["namespace"],
                           publication_dir=tmp_path, candidate="a" * 40, evidence_dir=tmp_path, github=github)
    if record is not None:
        args.recovery_evidence = tmp_path / "recovery.json"
        args.recovery_evidence.write_text(json.dumps(record))
    return args


def test_runner_loss_before_first_manifest_recovers_original_previous_candidate(monkeypatch, tmp_path):
    args = wrapper_inputs(monkeypatch, tmp_path, current="c" * 40, record=running_rollout_intent())
    monkeypatch.setattr(rollout, "candidate_follows", lambda *args: pytest.fail("recovery uses its persisted binding"))

    def deploy(args, **kwargs):
        assert args.resume_guard_owner == running_rollout_intent()["guard_owner"]
        assert args.expected_current_candidate == "c" * 40
        assert args.retry_failed_jobs is True
        assert args.migration_schema_head == "original_head"
        return {"status": "complete"}

    monkeypatch.setattr(rollout, "deploy", deploy)
    assert rollout.rollout(args) == {"status": "complete"}


@pytest.mark.parametrize("record,current", [
    (running_rollout_intent(), "d" * 40), (failed_rollout_record(), "c" * 40),
])
def test_recovery_never_accepts_unrecorded_or_legacy_previous_candidate(monkeypatch, tmp_path, record, current):
    args = wrapper_inputs(monkeypatch, tmp_path, current=current, record=record)
    monkeypatch.setattr(rollout, "candidate_follows", lambda *args: pytest.fail("must not infer recovery authority"))
    monkeypatch.setattr(rollout, "deploy", lambda *args, **kwargs: pytest.fail("must not deploy another candidate"))
    with pytest.raises(rollout.DeploymentError, match="recovery binding differs"):
        rollout.rollout(args)


def test_wrapper_persists_exact_guard_intent_before_deployment_acquisition(monkeypatch, tmp_path):
    args = wrapper_inputs(monkeypatch, tmp_path, current="c" * 40, github=True)
    monkeypatch.setattr(rollout, "candidate_follows", lambda *args: True)
    monkeypatch.setenv("GITHUB_RUN_ID", "91")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "3")
    persisted = {}

    def api(path, payload):
        if path == "deployments":
            persisted.update(payload["payload"])
            return {"id": 123}
        return {}

    def deploy(args, **kwargs):
        assert persisted["guard_owner"] == args.guard_owner
        assert persisted == {**running_rollout_intent(), "guard_owner": args.guard_owner}
        return {"status": "complete"}

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout, "deploy", deploy)
    assert rollout.rollout(args) == {"status": "complete"}


def test_local_github_deployment_without_workflow_identity_still_works(monkeypatch, tmp_path):
    args = wrapper_inputs(monkeypatch, tmp_path, github=True)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ATTEMPT", raising=False)

    def api(path, payload):
        if path == "deployments":
            assert "run_id" not in payload["payload"]
            assert "run_attempt" not in payload["payload"]
            return {"id": 123}
        assert "log_url" not in payload
        return {}

    monkeypatch.setattr(rollout, "github", api)
    monkeypatch.setattr(rollout, "deploy", lambda *args, **kwargs: {"status": "complete"})
    assert rollout.rollout(args) == {"status": "complete"}


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
