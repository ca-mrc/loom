from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from scripts.ops import nebius_pool_test_plan as planner

ROOT = Path(__file__).resolve().parents[2]
SHA, BASE = "a" * 40, "b" * 40
SOURCE, TREE, CHECKOUT = "c" * 40, "d" * 40, "e" * 40


class Actions:
    def __init__(self, sha=SHA, base=BASE):
        self.run = dict(
            id=10,
            run_attempt=1,
            check_suite_id=20,
            head_sha=sha,
            event="merge_group",
            status="completed",
            conclusion="success",
            path=".github/workflows/ci.yml",
            repository={"id": 1281629473},
            head_repository={"id": 1281629473},
            head_branch="gh-readonly-queue/dev/pr-1-base",
            display_title=f"gate=full / head={sha} / base={base} / updated=none",
        )
        self.suite = dict(head_sha=sha, app={"id": 15368})
        self.jobs = [
            dict(name=f"nebius-pool ({i + 1}-of-20)", status="completed", conclusion="success")
            for i in range(20)
        ]
        self.fail = False

    def read(self, endpoint):
        if self.fail:
            raise subprocess.CalledProcessError(1, ["gh", "api"])
        if endpoint.startswith("actions/workflows/"):
            return [dict(total_count=1, workflow_runs=[self.run])]
        if endpoint.startswith("check-suites/"):
            return [self.suite]
        if endpoint.startswith("git/commits/"):
            return []
        assert endpoint == "actions/runs/10/jobs?filter=latest&per_page=100"
        return [dict(total_count=len(self.jobs), jobs=self.jobs)]


class PullRequestActions(Actions):
    def __init__(self):
        super().__init__(SOURCE)
        self.run.update(
            event="pull_request",
            head_branch="fix/own-branch",
            display_title=f"gate=full / head={SOURCE} / base={BASE} / updated=none / pull=7",
        )
        self.pr = dict(
            number=7,
            state="closed",
            merged_at="2026-10-20T20:00:00Z",
            merge_commit_sha=SHA,
            base=dict(ref="dev", sha=BASE, repo={"id": 1281629473}),
            head=dict(sha=SOURCE, ref="fix/own-branch", repo={"id": 1281629473}),
        )
        self.commit = dict(sha=SHA, tree={"sha": TREE}, parents=[{"sha": BASE}])
        self.checked = dict(
            sha=CHECKOUT, tree={"sha": TREE}, parents=[{"sha": BASE}, {"sha": SOURCE}]
        )
        for index, job in enumerate(self.jobs):
            job.update(
                id=100 + index,
                steps=[
                    dict(
                        number=2,
                        name="Run actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd",
                        status="completed",
                        conclusion="success",
                        started_at="2026-10-20T19:00:00Z",
                        completed_at="2026-10-20T19:00:02Z",
                    ),
                    dict(
                        number=8,
                        name="Pytest — manifest-owned pool shard",
                        status="completed",
                        conclusion="success",
                    ),
                ],
            )

    def read(self, endpoint):
        if "event=merge_group" in endpoint:
            return [dict(workflow_runs=[])]
        if endpoint.startswith(f"git/commits/{SHA}"):
            return [self.commit]
        if endpoint.startswith(f"git/commits/{CHECKOUT}"):
            return [self.checked]
        if endpoint.startswith(f"commits/{SHA}/pulls"):
            return [self.pr]
        if endpoint.startswith("git/commits/"):
            return []
        return super().read(endpoint)

    def log(self, job_id):
        assert job_id in {job["id"] for job in self.jobs}
        return "\n".join(
            f"2026-10-20T19:00:01.1234567Z {line}"
            for line in [
                "##[group]Run actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd",
                "  CI_TEST_SCOPE: nebius",
                "  CI_PYTEST_MARKERS: not legacy_pool",
                "  TEST_CHANGED_PATHS: []",
                f"[command]/usr/bin/git fetch origin +{CHECKOUT}:refs/remotes/pull/7/merge",
                "[command]/usr/bin/git checkout --progress --force refs/remotes/pull/7/merge",
                "[command]/usr/bin/git log -1 --format=%H",
                CHECKOUT,
                "##[group]Run python3 scripts/configure_ci_registry_mirror.py",
            ]
        )


@pytest.fixture
def actions(monkeypatch):
    monkeypatch.setattr(
        planner, "candidate_changes", lambda *_: ("scripts/ops/deploy_nebius_platform.py",)
    )
    monkeypatch.setattr(
        planner, "source_changes", lambda *_: ("scripts/ops/deploy_nebius_platform.py",)
    )
    return Actions()


def pending(value):
    return {path for row in value["matrix"]["include"] for path in row["paths"]}


def test_complete_native_coverage_schedules_no_duplicate_tests(actions):
    value = planner.plan(ROOT, SHA, read=actions.read)
    assert value["required"] is False and value["matrix"] == {"include": []}
    assert value["required_files"] == value["reused_files"] > 0
    assert value["missing_files"] == 0 and value["reused_runs"] == [10]


def test_native_squash_reuses_the_actual_tested_merge_tree(actions, monkeypatch):
    pr = PullRequestActions()
    monkeypatch.setattr(planner, "github_job_log", pr.log, raising=False)
    value = planner.plan(ROOT, SHA, read=pr.read)
    assert value["required"] is False
    assert value["reused_files"] == value["required_files"] > 0
    assert value["reused_runs"] == [10]


@pytest.mark.parametrize(
    "damage",
    [
        "unmerged",
        "other_candidate",
        "other_target",
        "fork",
        "old_head",
        "old_base",
        "filtered",
        "other_workflow",
        "other_app",
        "other_branch",
        "other_pull",
        "tree",
        "parent",
        "source_parent",
        "extra_parent",
        "candidate_merge",
    ],
)
def test_squash_unqualified_identity_or_tested_content_retains_tests(actions, damage):
    pr = PullRequestActions()
    if damage == "unmerged":
        pr.pr["merged_at"] = None
    elif damage == "other_candidate":
        pr.pr["merge_commit_sha"] = BASE
    elif damage == "other_target":
        pr.pr["base"]["ref"] = "main"
    elif damage == "fork":
        pr.pr["head"]["repo"]["id"] = 1
    elif damage == "old_head":
        pr.pr["head"]["sha"] = "f" * 40
    elif damage == "old_base":
        pr.run["display_title"] = pr.run["display_title"].replace(BASE, "f" * 40)
    elif damage == "filtered":
        pr.run["display_title"] = pr.run["display_title"].replace("gate=full", "gate=filtered")
    elif damage == "other_workflow":
        pr.run["path"] = ".github/workflows/images.yml"
    elif damage == "other_app":
        pr.suite["app"]["id"] = 1
    elif damage == "other_branch":
        pr.run["head_branch"] = "foreign"
    elif damage == "other_pull":
        pr.run["display_title"] = pr.run["display_title"].replace("pull=7", "pull=8")
    elif damage == "tree":
        pr.checked["tree"]["sha"] = "f" * 40
    elif damage == "parent":
        pr.checked["parents"][0]["sha"] = "f" * 40
    elif damage == "source_parent":
        pr.checked["parents"][1]["sha"] = "f" * 40
    elif damage == "extra_parent":
        pr.checked["parents"].append({"sha": SHA})
    elif damage == "candidate_merge":
        pr.commit["parents"].append({"sha": SOURCE})
    result = planner.plan(ROOT, SHA, read=pr.read, read_log=pr.log)
    assert result["reused_files"] == 0
    assert len(pending(result)) == result["required_files"] > 0


@pytest.mark.parametrize(
    "damage",
    [
        "missing_log",
        "different_pin",
        "wrong_ref",
        "unbounded_time",
        "missing_sha",
        "late_spoof",
        "scope",
        "markers",
        "selection",
        "failed_pytest",
        "missing_checkout",
    ],
)
def test_squash_unproven_job_checkout_or_selection_retains_its_modules(actions, damage):
    pr = PullRequestActions()
    original = pr.log
    if damage == "failed_pytest":
        pr.jobs[0]["steps"][1]["conclusion"] = "skipped"
    elif damage == "missing_checkout":
        pr.jobs[0]["steps"].pop(0)

    def log(job_id):
        value = original(job_id)
        if job_id != 100:
            return value
        if damage == "missing_log":
            raise OSError("unavailable")
        if damage == "different_pin":
            return value.replace(planner.CHECKOUT_ACTION, "actions/checkout@other")
        if damage == "wrong_ref":
            return value.replace("pull/7/merge", "pull/8/merge")
        if damage == "unbounded_time":
            return value.replace("19:00:01.", "19:01:01.")
        if damage == "missing_sha":
            return value.replace(f"Z {CHECKOUT}\n", "Z short-sha\n")
        if damage == "late_spoof":
            # A later repository command cannot repair missing checkout output.
            return value.replace(f"Z {CHECKOUT}\n", "Z short-sha\n") + (
                f"\n2026-10-20T19:00:02Z [command]/usr/bin/git log -1 --format=%H"
                f"\n2026-10-20T19:00:02Z {CHECKOUT}"
            )
        if damage == "scope":
            return value.replace("CI_TEST_SCOPE: nebius", "CI_TEST_SCOPE: all")
        if damage == "markers":
            return value.replace("not legacy_pool", "legacy_pool")
        if damage == "selection":
            return value.replace("TEST_CHANGED_PATHS: []", 'TEST_CHANGED_PATHS: "unknown"')
        return value

    value = planner.plan(ROOT, SHA, read=pr.read, read_log=log)
    manifest = planner.load_manifest(ROOT / "config/component-ownership.toml")
    expected = set(
        planner.selected_test_paths(
            manifest,
            tracked_paths=planner._tracked_paths(ROOT),
            lane="nebius-pool",
            repo_root=ROOT,
            test_scope="nebius",
            suite="nebius-pool-ops",
            shard_index=0,
            shard_count=20,
        )
    )
    assert pending(value) == expected
    assert value["reused_files"] + len(expected) == value["required_files"]


def test_squash_reconstructs_the_jobs_actual_narrowed_selection(actions):
    pr = PullRequestActions()
    path = "tests/ops/" + "_".join(("test_nebius_pool", "activation_transport")) + ".py"
    value = planner.plan(
        ROOT,
        SHA,
        read=pr.read,
        read_log=lambda job_id: pr.log(job_id).replace(
            "TEST_CHANGED_PATHS: []", "TEST_CHANGED_PATHS: " + json.dumps([path])
        ),
    )
    full = planner.plan(ROOT, SHA, read=lambda _: [])
    assert value["reused_files"] == 1
    assert pending(value) == pending(full) - {path}


def test_squash_does_not_guess_between_duplicate_job_names(actions):
    pr = PullRequestActions()
    pr.jobs.append(copy.deepcopy(pr.jobs[0]))
    value = planner.plan(ROOT, SHA, read=pr.read, read_log=pr.log)
    assert 0 < value["missing_files"] < value["required_files"]
    assert [row["shard_index"] for row in value["matrix"]["include"]] == [0]


def test_job_only_retry_retains_other_successful_jobs_on_same_commit(actions):
    actions.run["run_attempt"] = 2
    assert planner.plan(ROOT, SHA, read=actions.read)["required"] is False


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "skipped", None])
def test_only_files_of_unproven_jobs_are_scheduled(actions, conclusion):
    actions.jobs[0]["conclusion"] = conclusion
    value = planner.plan(ROOT, SHA, read=actions.read)
    manifest = planner.load_manifest(ROOT / "config/component-ownership.toml")
    expected = set(
        planner.selected_test_paths(
            manifest,
            tracked_paths=planner._tracked_paths(ROOT),
            lane="nebius-pool",
            repo_root=ROOT,
            test_scope="nebius",
            suite="nebius-pool-ops",
            shard_index=0,
            shard_count=20,
        )
    )
    assert pending(value) == expected
    assert value["missing_files"] == len(expected)
    assert value["reused_files"] + len(expected) == value["required_files"]


def test_narrow_ci_coverage_does_not_rerun_covered_siblings(actions, monkeypatch):
    # This module has no executable test-helper consumers; construct its name
    # without adding a false fixture reference to the ownership selector.
    path = "tests/ops/" + "_".join(("test_nebius_pool", "activation_transport")) + ".py"
    monkeypatch.setattr(planner, "source_changes", lambda *_: (path,))
    full = planner.plan(ROOT, SHA, read=lambda _: [])
    value = planner.plan(ROOT, SHA, read=actions.read)
    assert value["reused_files"] == 1
    assert pending(value) == pending(full) - {path}
    assert all(path not in row["paths"] for row in value["matrix"]["include"])
    # The rest of its original shard still runs: reuse is by complete module,
    # not a broad passing shard name or a subset of individual test cases.
    assert any(row["shard_index"] == 1 for row in value["matrix"]["include"])


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("head_sha", "c" * 40),
        ("event", "pull_request"),
        ("conclusion", "failure"),
        ("status", "in_progress"),
        ("path", ".github/workflows/images.yml"),
        ("repository", {"id": 1}),
        ("head_repository", {"id": 1}),
        ("head_branch", "gh-readonly-queue/main/pr-1-base"),
        ("display_title", f"gate=filtered / head={SHA} / base={BASE} / "),
    ],
)
def test_unqualified_run_is_missing_coverage(actions, key, value):
    actions.run[key] = value
    result = planner.plan(ROOT, SHA, read=actions.read)
    assert result["reused_files"] == 0
    assert len(pending(result)) == result["required_files"]


@pytest.mark.parametrize(
    "suite", [dict(head_sha=SHA, app={"id": 1}), dict(head_sha=BASE, app={"id": 15368})]
)
def test_check_suite_must_be_native_actions_and_exact_head(actions, suite):
    actions.suite = suite
    assert planner.plan(ROOT, SHA, read=actions.read)["reused_files"] == 0


def test_unavailable_api_retains_tests(actions):
    actions.fail = True
    value = planner.plan(ROOT, SHA, read=actions.read)
    assert value["required"] and value["evidence_unavailable"]
    assert len(pending(value)) == value["required_files"]


def test_partial_job_listing_retains_proven_files_and_fills_only_missing(actions):
    good = actions.read

    def incomplete(endpoint):
        pages = copy.deepcopy(good(endpoint))
        if "/jobs?" in endpoint:
            pages[0]["jobs"] = pages[0]["jobs"][1:]
        return pages

    value = planner.plan(ROOT, SHA, read=incomplete)
    assert 0 < value["reused_files"] < value["required_files"]
    assert [row["shard_index"] for row in value["matrix"]["include"]] == [0]


def test_cli_uses_read_only_actions_and_emits_skip_for_real_fixed_checkout(tmp_path):
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    base = subprocess.check_output(["git", "rev-parse", "HEAD^"], cwd=ROOT, text=True).strip()
    actions = Actions(head, base)
    fixture = tmp_path / "actions.json"
    fixture.write_text(json.dumps(dict(run=actions.run, suite=actions.suite, jobs=actions.jobs)))
    shim = tmp_path / "gh"
    shim.write_text("""#!/usr/bin/env python3
import json,os,sys
d=json.load(open(os.environ['ACTIONS_FIXTURE']));p=sys.argv[-1]
assert sys.argv[1:5]==['api','--paginate','--slurp',p]
assert p.startswith('repos/ca-mrc/loom/')
if '/workflows/' in p: out=[dict(total_count=1,workflow_runs=[d['run']])]
elif '/check-suites/' in p: out=[d['suite']]
else: out=[dict(total_count=len(d['jobs']),jobs=d['jobs'])]
print(json.dumps(out))
""")
    shim.chmod(0o700)
    output = tmp_path / "github-output"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/ops/nebius_pool_test_plan.py",
            "--candidate",
            head,
            "--github-output",
            str(output),
        ],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
            "ACTIONS_FIXTURE": str(fixture),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["missing_files"] == 0
    assert "required=false\n" in output.read_text()


def test_metadata_reader_can_read_merged_prs_without_write_authority():
    workflow = yaml.safe_load((ROOT / ".github/workflows/nebius-candidate.yml").read_text())
    assert workflow["jobs"]["pool-ops-plan"]["permissions"] == {
        "contents": "read",
        "actions": "read",
        "checks": "read",
        "pull-requests": "read",
    }


def test_publication_shell_executes_only_missing_modules(actions, monkeypatch, tmp_path):
    path = "tests/ops/" + "_".join(("test_nebius_pool", "activation_transport")) + ".py"
    monkeypatch.setattr(planner, "source_changes", lambda *_: (path,))
    value = planner.plan(ROOT, SHA, read=actions.read)
    row = next(item for item in value["matrix"]["include"] if item["shard_index"] == 1)
    job = yaml.safe_load((ROOT / ".github/workflows/nebius-candidate.yml").read_text())["jobs"][
        "pool-ops-tests"
    ]
    select = next(step for step in job["steps"] if step.get("id") == "manifest")
    execute = next(
        step for step in job["steps"] if step.get("name") == "Pytest — pool operator shard"
    )
    output, recorded = tmp_path / "github-output", tmp_path / "pytest-argv.json"
    uv = tmp_path / "uv"
    uv.write_text("""#!/usr/bin/env python3
import json,os,sys
if sys.argv[1]=='run':
 json.dump(sys.argv[2:],open(os.environ['PYTEST_ARGV'],'w'))
""")
    uv.chmod(0o700)
    env = {
        **os.environ,
        "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
        "TEST_PATHS_JSON": json.dumps(row["paths"]),
        "RUNNER_TEMP": str(tmp_path),
        "GITHUB_OUTPUT": str(output),
        "PYTEST_ARGV": str(recorded),
    }
    subprocess.run(
        ["bash", "-c", select["run"]], cwd=ROOT, env=env, check=True, capture_output=True
    )
    selected = dict(line.split("=", 1) for line in output.read_text().splitlines())
    script = execute["run"].replace(
        "${{ steps.manifest.outputs.paths_file }}", selected["paths_file"]
    )
    subprocess.run(["bash", "-c", script], cwd=ROOT, env=env, check=True, capture_output=True)
    executed = {arg for arg in json.loads(recorded.read_text()) if arg.startswith("tests/")}
    assert executed == set(row["paths"]) and path not in executed
    assert "-x" in json.loads(recorded.read_text())
