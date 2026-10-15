"""Native runner bridge boundaries, without starting jobs or accessing secrets."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts/ops" / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def test_hosted_bridge_does_not_inherit_outer_runtime_tokens():
    bridge = module("ci_hosted_worker")
    env = {
        "PATH": "/bin",
        "HOME": "/home/runner",
        "USER": "runner",
        "GITHUB_TOKEN": "outer-token",
        "ACTIONS_RUNTIME_TOKEN": "outer-runtime",
        "RUNNER_TRACKING_ID": "outer-processes",
        "LOOM_CI_JIT_CONFIG": "one-job",
        "INPUT_TOKEN": "outer-input",
    }
    assert bridge.clean_environment(env) == {
        "PATH": "/bin",
        "HOME": "/home/runner",
        "USER": "runner",
    }


def test_guest_capability_preserves_one_job_shutdown_and_credential_disposal():
    plugin = module("ci_runner_unified_host")
    original = "#!/bin/bash\nsystemctl is-active --quiet docker\nrun-one-job\nshred -u jitconfig\npoweroff\n"
    updated = plugin.guest_script(original)
    assert updated.index(plugin.MARKER) < updated.index("run-one-job")
    assert updated.endswith("run-one-job\nshred -u jitconfig\npoweroff\n")
    assert '"provider":"oldlab"' in updated


def test_guest_plugin_refuses_unknown_lifecycle():
    with pytest.raises(ValueError, match="unsupported"):
        module("ci_runner_unified_host").guest_script("# different bootstrap\n")


def test_common_guest_refuses_insufficient_memory_before_creation():
    from types import SimpleNamespace

    created = []
    base = SimpleNamespace(
        _slot_run_script=lambda: "systemctl is-active --quiet docker\nrun-one-job\n",
        GitHubAPI=type("API", (), {"generate_jit_config": lambda *args, **kwargs: None}),
        _create_slot=lambda profile, **kwargs: created.append(profile.memory_mib_per_slot),
    )
    module("ci_runner_unified_host").install(base)
    with pytest.raises(ValueError, match="12 GiB"):
        base._create_slot(SimpleNamespace(memory_mib_per_slot=6144), slot=0)
    assert created == []
    base._create_slot(SimpleNamespace(memory_mib_per_slot=12288), slot=0)
    assert created == [12288]


def test_dependencies_do_not_allocate_workers_for_blocked_matrix_rows():
    supply = module("ci_runner_supply")
    graph = {
        "jobs": {
            "plan": {},
            "tests": {"name": "tests (${{ matrix.shard }})", "needs": "plan"},
            "gate": {"needs": ["plan", "tests"]},
        }
    }

    def row(name, status):
        return {"name": name, "status": status, "labels": ["loom-ci-exec"]}

    jobs = [
        row("plan", "in_progress"),
        row("tests (1)", "queued"),
        row("tests (2)", "queued"),
        row("gate", "queued"),
    ]
    assert supply.ready_jobs(jobs, graph) == []
    jobs[0]["status"] = "completed"
    assert supply.ready_jobs(jobs, graph) == jobs[1:3]
    jobs[1]["status"] = "completed"
    assert supply.ready_jobs(jobs, graph) == [jobs[2]]
    jobs[2]["status"] = "completed"
    assert supply.ready_jobs(jobs, graph) == [jobs[3]]


@pytest.mark.parametrize("github_status", ["offline", "online"])
def test_hosted_startup_never_hides_available_oldlab_capacity(github_status):
    supply = module("ci_runner_supply")
    runners = [
        {
            "id": 1,
            "name": "oldlab2-kvm-0",
            "status": "online",
            "busy": False,
            "labels": [{"name": "loom-ci-isolated-v1"}, {"name": "loom-ci-exec"}],
        },
        {
            "id": 2,
            "name": "ghvm-exec-00-test",
            "status": github_status,
            "busy": False,
            "labels": [{"name": "loom-ci-provider-github"}],
        },
    ]
    plan = supply.supply_plan(runners, 25, 1, 20)
    assert plan["desired_github"] == 20
    assert plan["oldlab_idle"] == 1
    assert "label_changes" not in plan


def test_busy_runner_is_not_removed_or_provider_cancelled(tmp_path):
    supply = module("ci_runner_supply")
    calls = []

    class API:
        def request(self, method, path, **kwargs):
            calls.append((method, path))
            return {"busy": True}

    assert not supply.retired_worker(API(), tmp_path, {"runner_id": 3}, {})
    assert calls == [("GET", "/actions/runners/3")]


def test_failed_deregistration_preserves_provider(tmp_path):
    supply = module("ci_runner_supply")
    calls = []

    class API:
        def request(self, method, path, **kwargs):
            calls.append((method, path))
            if method == "GET":
                return {"busy": False}
            raise supply.SupplyError("racing busy runner")

    assert not supply.retired_worker(API(), tmp_path, {"runner_id": 3, "run_id": 9}, {})
    assert calls == [("GET", "/actions/runners/3"), ("DELETE", "/actions/runners/3")]


def test_lost_dispatch_response_is_not_repeated(tmp_path):
    supply = module("ci_runner_supply")
    folder = tmp_path / "slot-00"
    folder.mkdir()
    record = {
        "slot": 0,
        "phase": "secret-ready",
        "workflow_path": "ci-hosted-capacity.yml",
        "ref": "dev",
        "nonce": "test",
    }
    calls = []

    class API:
        def request(self, *args, **kwargs):
            calls.append(args)
            raise supply.SupplyError("response lost")

    with pytest.raises(supply.SupplyError):
        supply.dispatch_worker(API(), tmp_path, record)
    assert record["phase"] == "dispatch-uncertain"
    supply.dispatch_worker(API(), tmp_path, record)
    assert len(calls) == 1


@pytest.mark.parametrize("name", ["ci", "images", "cluster-smoke", "staging-smoke"])
def test_real_workflow_graphs_are_supported(name):
    import yaml

    supply = module("ci_runner_supply")
    graph = yaml.safe_load((ROOT / f".github/workflows/{name}.yml").read_text())
    assert supply.ready_jobs([], graph) == []


def test_registered_capacity_dispatch_cannot_select_a_retry_target(tmp_path):
    supply = module("ci_runner_supply")
    (tmp_path / "slot-00").mkdir()
    record = {
        "slot": 0,
        "phase": "secret-ready",
        "workflow_path": "ci-retry.yml",
        "ref": "dev",
        "nonce": "test",
        "registered_capacity_dispatch": True,
    }
    calls = []

    class API:
        def request(self, method, path, body):
            calls.append((method, path, body))

    supply.dispatch_worker(API(), tmp_path, record)
    sent = calls[0][2]["inputs"]
    assert sent["purpose"] == "runner_capacity"
    assert sent["source_run_id"] == sent["failed_attempt"] == "0"
    assert sent["slot"] == "00" and sent["request_id"] == "test"


def test_registered_dispatcher_keeps_retry_policy_and_parallel_capacity_separate():
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/ci-retry.yml").read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["purpose"]["default"] == "retry"
    for name in ["source_run_id", "failed_attempt", "reason", "evidence_url", "mode"]:
        assert inputs[name]["required"] is True
    assert workflow["jobs"]["retry"]["if"] == "inputs.purpose != 'runner_capacity'"
    capacity = workflow["jobs"]["capacity"]
    assert capacity["if"] == "inputs.purpose == 'runner_capacity'"
    assert capacity["permissions"] == {"contents": "read"}
    assert capacity["runs-on"] == "ubuntu-24.04"
    assert "inputs.request_id" in workflow["concurrency"]["group"]
    assert "inputs.source_run_id" in workflow["concurrency"]["group"]
