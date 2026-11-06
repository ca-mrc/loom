"""Owner activation path for opt-in build-phase storage readings (#2750).

The standalone builder's ``record_boundary_usage`` is staged and applied through
the existing execution-tuning tool, read back from the running actuator, and
disabled by removing the key so the prior settings return byte for byte.
"""
from __future__ import annotations

import json
from copy import deepcopy

import pytest
import scripts.ops.nebius_execution_tuning as tuning
from scripts.ops.deploy_nebius_platform import DeploymentError

from loom.nebius_platform_render import build_platform
from loom_execution_actuator.task_image_boundary_usage import MARKER
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs
from tests.unit.test_nebius_execution_tuning import ROOT, FakeKube, _active_builder
from tests.unit.test_nebius_task_image_renderer import inputs as inputs

_ENV = "LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"


def _actuator_builder(kube, config):
    deployment = kube.objects["deployment", config["execution_namespace"], "loom-execution-actuator"]
    return tuning._env(deployment)[_ENV]


def _jobs(kube):
    return {key: deepcopy(value) for key, value in kube.objects.items() if key[0] == "job"}


def test_default_is_off_in_saved_config_rendering_and_inspection(regional_inputs) -> None:
    config, _, _, kube = _active_builder(regional_inputs)
    assert "record_boundary_usage" not in json.loads(_actuator_builder(kube, config))
    assert tuning._builders(config)[config["target_id"]]["record_boundary_usage"] is False
    job_config = NativeTaskImageSettings.model_validate_json(_actuator_builder(kube, config)).job_config()
    assert job_config.record_boundary_usage is False


def test_enable_reads_back_and_is_idempotent(regional_inputs) -> None:
    config, candidate, profile, kube = _active_builder(regional_inputs)
    target, sha, namespace = config["target_id"], candidate["candidate_sha"], config["namespace"]
    before_env, jobs = _actuator_builder(kube, config), _jobs(kube)
    enabled = tuning.apply_builders(kube, namespace, expected_candidate=sha,
                                    resources={target: {"record_boundary_usage": True}})
    assert enabled["builders"][target]["record_boundary_usage"] is True
    assert enabled["builders"][target]["record_boundary_usage_saved"] == "true"
    assert enabled["restarted_controllers"] == ["loom-execution-actuator"] and enabled["task_jobs_replaced"] is False
    live = NativeTaskImageSettings.model_validate_json(_actuator_builder(kube, config))
    assert live.record_boundary_usage is True and "record_boundary_usage" in live.model_fields_set
    # Every other builder setting is unchanged; only future Jobs render the reading.
    assert {k: v for k, v in json.loads(_actuator_builder(kube, config)).items() if k != "record_boundary_usage"} == \
        json.loads(before_env)
    assert _jobs(kube) == jobs
    saved = json.loads(kube.cm["data"]["environment.json"])
    rendered = build_platform(saved, candidate, profile, {}, repo_root=ROOT)
    actuator = next(doc for docs in rendered.values() for doc in docs
                    if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-execution-actuator")
    assert json.loads(tuning._env(actuator)[_ENV])["record_boundary_usage"] is True
    again = tuning.apply_builders(kube, namespace, expected_candidate=sha,
                                  resources={target: {"record_boundary_usage": True}})
    assert again["restarted_controllers"] == []


_RESTORE_VALUE = {"absent": None, "false": False, "true": True}


@pytest.mark.parametrize("prior", ["absent", "false", "true"])
@pytest.mark.parametrize("change", [True, False, None])
def test_rollback_restores_the_captured_representation_exactly(regional_inputs, prior, change) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    target, sha, namespace = config["target_id"], candidate["candidate_sha"], config["namespace"]
    if prior != "absent":
        tuning.apply_builders(kube, namespace, expected_candidate=sha,
                              resources={target: {"record_boundary_usage": _RESTORE_VALUE[prior]}})
    captured = tuning._builders(json.loads(kube.cm["data"]["environment.json"]))[target]
    assert captured["record_boundary_usage_saved"] == prior
    before_env, before_saved, jobs = _actuator_builder(kube, config), kube.cm["data"]["environment.json"], _jobs(kube)
    tuning.apply_builders(kube, namespace, expected_candidate=sha,
                          resources={target: {"record_boundary_usage": change}})
    restored = tuning.apply_builders(
        kube, namespace, expected_candidate=sha,
        resources={target: {"record_boundary_usage": _RESTORE_VALUE[captured["record_boundary_usage_saved"]]}})
    assert restored["builders"][target]["record_boundary_usage_saved"] == prior
    assert _actuator_builder(kube, config) == before_env  # byte for byte, including representation
    assert json.loads(kube.cm["data"]["environment.json"]) == json.loads(before_saved)
    assert _jobs(kube) == jobs


def test_restore_keeps_a_concurrent_change_to_another_setting(regional_inputs) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    target, sha, namespace = config["target_id"], candidate["candidate_sha"], config["namespace"]
    tuning.apply_builders(kube, namespace, expected_candidate=sha, resources={target: {"record_boundary_usage": True}})
    # Someone else tunes CPU requests during the window.
    tuning.apply_builders(kube, namespace, expected_candidate=sha, resources={target: {"cpu_request_millis": 2000}})
    tuning.apply_builders(kube, namespace, expected_candidate=sha, resources={target: {"record_boundary_usage": None}})
    builder = json.loads(kube.cm["data"]["environment.json"])["task_image_builder"]
    assert "record_boundary_usage" not in builder and builder["cpu_request_millis"] == 2000


def test_enabled_settings_render_instrumented_future_jobs_only(regional_inputs, inputs) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    tuning.apply_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                          resources={config["target_id"]: {"record_boundary_usage": True}})
    live = NativeTaskImageSettings.model_validate_json(_actuator_builder(kube, config))
    from loom_execution_actuator.task_image_renderer import render_task_image_job

    _, job = render_task_image_job(**{**inputs, "config": live.job_config()})
    assert MARKER in job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]


@pytest.mark.parametrize("value", ["true", 1, 0, "false", "unset"])
@pytest.mark.parametrize("operation", ["stage", "apply"])
def test_flag_requires_a_strict_boolean_and_writes_nothing(regional_inputs, value, operation) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    call = tuning.stage_builders if operation == "stage" else tuning.apply_builders
    with pytest.raises(DeploymentError):
        call(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
             resources={config["target_id"]: {"record_boundary_usage": value}})
    assert not any(args[0] in {"patch", "set", "rollout"} for args in kube.calls)


@pytest.mark.parametrize("target", ["unknown", "regional-without-builder"])
def test_target_must_have_a_configured_builder(regional_inputs, target) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    selected = "unknown-target" if target == "unknown" else config["regional_execution_targets"][0]["target_id"]
    with pytest.raises(DeploymentError):
        tuning.apply_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                              resources={selected: {"record_boundary_usage": True}})
    assert not any(args[0] in {"patch", "set", "rollout"} for args in kube.calls)


def test_installed_image_without_support_is_refused_before_any_write(regional_inputs) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    kube.builder_boundary_usage_support = False
    with pytest.raises(DeploymentError, match="boundary usage support"):
        tuning.apply_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                              resources={config["target_id"]: {"record_boundary_usage": True}})
    assert not any(args[0] in {"patch", "set", "rollout"} for args in kube.calls)


def test_staged_flag_blocks_unrelated_live_tuning_on_an_unsupporting_image(regional_inputs) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    tuning.stage_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                          resources={config["target_id"]: {"record_boundary_usage": True}})
    kube.builder_boundary_usage_support = False
    with pytest.raises(DeploymentError):
        tuning.apply_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                              resources={config["target_id"]: {"cpu_request_millis": 2000}})
    assert not any(args[0] in {"set", "rollout"} for args in kube.calls)


def test_staged_but_unapplied_flag_is_reported_as_drift_not_silently_applied(regional_inputs) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    tuning.stage_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                          resources={config["target_id"]: {"record_boundary_usage": True}})
    with pytest.raises(DeploymentError, match="unrequested builder settings differ"):
        tuning.apply_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                              resources={config["target_id"]: {"cpu_request_millis": 2000}})


def test_lost_support_after_restart_fails_readback(regional_inputs, monkeypatch) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    original_run = kube.run

    def run(*args, **kwargs):
        if args[:2] == ("rollout", "status"):
            kube.builder_boundary_usage_support = False  # the restarted Pod runs an image without the field
        return original_run(*args, **kwargs)

    monkeypatch.setattr(kube, "run", run)
    with pytest.raises(DeploymentError, match="lost boundary usage support"):
        tuning.apply_builders(kube, config["namespace"], expected_candidate=candidate["candidate_sha"],
                              resources={config["target_id"]: {"record_boundary_usage": True}})


@pytest.mark.parametrize("operation", ["stage-builders", "apply-builders"])
def test_operator_cli_sets_and_clears_the_flag(regional_inputs, monkeypatch, capsys, operation) -> None:
    config, candidate, _, kube = _active_builder(regional_inputs)
    monkeypatch.setattr(tuning, "Kubectl", lambda path: kube)
    target = config["target_id"]
    for value, expected in (("true", "true"), ("false", "false"), ("unset", "absent")):
        monkeypatch.setattr("sys.argv", ["execution-tuning", operation, "--kubeconfig", "/operator/config",
                                         "--namespace", config["namespace"],
                                         "--expected-candidate", candidate["candidate_sha"],
                                         "--build-record-boundary-usage", f"{target}={value}"])
        tuning.main()
        assert json.loads(capsys.readouterr().out)["builders"][target]["record_boundary_usage_saved"] == expected
    monkeypatch.setattr("sys.argv", ["execution-tuning", operation, "--kubeconfig", "/operator/config",
                                     "--namespace", config["namespace"], "--expected-candidate", candidate["candidate_sha"],
                                     "--build-record-boundary-usage", f"{target}=yes"])
    with pytest.raises(SystemExit):
        tuning.main()


def test_unconfigured_fake_cluster_default_remains_off(regional_inputs) -> None:
    kube = FakeKube(regional_inputs)
    config = regional_inputs[0]
    assert all(row.get("record_boundary_usage") in (None, False) for row in tuning._builders(config).values())
    assert kube.builder_boundary_usage_support is True
