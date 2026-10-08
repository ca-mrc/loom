from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.nebius_platform_bootstrap import MigrationError, database_url
from loom.nebius_platform_render import NebiusPlatformError, build_platform, write_platform

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("configured_default", [None, {
    "controller": {"cpu_millis": 100, "memory_mib": 256, "ephemeral_storage_mib": 256},
    "task_sandbox": {"cpu_millis": 400, "memory_mib": 512, "ephemeral_storage_mib": 512},
    "verifier_sandbox": {"cpu_millis": 100, "memory_mib": 256, "ephemeral_storage_mib": 256},
}])
def test_environment_resource_policy_survives_new_published_candidate(
    platform_inputs: tuple, configured_default: dict | None,
) -> None:
    config, candidate, profile = platform_inputs
    requests = {"local/measured-task": {
        "task_revision_sha256": "sha256:" + "d" * 64,
        "requests": {"controller": {
            "cpu_millis": 250, "memory_mib": 512, "ephemeral_storage_mib": 100,
        }},
    }}
    config["task_resource_requests"] = requests
    expected_default = configured_default or {
        "controller": {"cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 512},
        "task_sandbox": {"cpu_millis": 600, "memory_mib": 1024, "ephemeral_storage_mib": 1024},
        "verifier_sandbox": {"cpu_millis": 200, "memory_mib": 512, "ephemeral_storage_mib": 512},
    }
    if configured_default is not None:
        config["default_task_resource_requests"] = configured_default
    original_config = json.dumps(config, sort_keys=True)
    original_profile = json.dumps(profile, sort_keys=True)
    for commit in ("c" * 40, "e" * 40):
        files = build_platform(config, {**candidate, "candidate_sha": commit},
                               {**profile, "candidate_sha": commit,
                                "default_task_resource_requests": {"controller": {
                                    "cpu_millis": 1000, "memory_mib": 4096,
                                    "ephemeral_storage_mib": 10240,
                                }}}, {}, repo_root=ROOT)
        cm = next(doc for doc in files["10-config-network.yaml"]
                  if doc["kind"] == "ConfigMap" and doc["metadata"]["name"] == "loom-platform-config")
        rendered_profile = json.loads(cm["data"]["profile.json"])
        assert rendered_profile["task_resource_requests"] == requests
        assert rendered_profile["default_task_resource_requests"] == expected_default
        assert json.loads(cm["data"]["environment.json"])["default_task_resource_requests"] == expected_default
        service = next(doc for doc in files["40-services.yaml"]
                       if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")
        env = {item["name"]: item.get("value")
               for item in service["spec"]["template"]["spec"]["containers"][0]["env"]}
        service_profile = json.loads(env["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"])
        assert service_profile["task_resource_requests"] == requests
        assert service_profile["default_task_resource_requests"] == expected_default
    assert json.dumps(config, sort_keys=True) == original_config
    assert json.dumps(profile, sort_keys=True) == original_profile


@pytest.mark.parametrize("requests", [None, {}, [], {"unknown": {}}, {
    "controller": {"cpu_millis": True, "memory_mib": 512, "ephemeral_storage_mib": 512},
}, {"controller": {"cpu_millis": 0, "memory_mib": 512, "ephemeral_storage_mib": 512}}])
def test_environment_rejects_invalid_default_resource_template(
    platform_inputs: tuple, requests: object,
) -> None:
    config, candidate, profile = platform_inputs
    config["default_task_resource_requests"] = requests
    with pytest.raises(ValueError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


@pytest.mark.parametrize("requests", [[], {"": {}}, {"local/task": {}},
                                     {"local/task": {"task_revision_sha256": "unbound", "requests": {}}}])
def test_environment_rejects_unbound_resource_policy(platform_inputs: tuple, requests: object) -> None:
    config, candidate, profile = platform_inputs
    config["task_resource_requests"] = requests
    with pytest.raises(ValueError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


@pytest.mark.parametrize("maximum", [None, 14400])
def test_scheduler_deadline_can_cover_long_task_phase_timeouts(
    platform_inputs: tuple, maximum: int | None,
) -> None:
    config, candidate, profile = platform_inputs
    if maximum is not None:
        config["service_execution_scheduler_max_deadline_sec"] = maximum
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    control_plane = next(
        doc for doc in files["40-services.yaml"]
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-control-plane"
    )
    env = {
        item["name"]: item.get("value")
        for item in control_plane["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_MAX_DEADLINE_SEC"] == str(maximum or 7200)


@pytest.mark.parametrize("maximum", [0, -1, True, "14400", 14400.5])
def test_scheduler_deadline_requires_positive_integer(platform_inputs: tuple, maximum: object) -> None:
    config, candidate, profile = platform_inputs
    config["service_execution_scheduler_max_deadline_sec"] = maximum
    with pytest.raises(NebiusPlatformError, match="service_execution_scheduler_max_deadline_sec"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


@pytest.mark.parametrize("concurrency", [1, 2, 16])
def test_native_builds_share_the_actuator_but_have_an_isolated_namespace(
    platform_inputs: tuple, concurrency: int,
) -> None:
    config, candidate, profile = platform_inputs
    config["task_image_builder"] = {
        "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
        "max_concurrent": concurrency,
    }
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    namespaces = {doc["metadata"]["name"]: doc for doc in files["00-namespaces.yaml"]}
    build_ns = config["execution_namespace"] + "-build"
    assert (
        namespaces[config["execution_namespace"]]["metadata"]["labels"][
            "pod-security.kubernetes.io/enforce"
        ]
        == "restricted"
    )
    assert (
        namespaces[build_ns]["metadata"]["labels"]["pod-security.kubernetes.io/enforce"]
        == "privileged"
    )
    docs = files["60-execution.yaml"]
    actuator = next(doc for doc in docs if doc["kind"] == "Deployment")
    env = {
        row["name"]: row.get("value")
        for row in actuator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    settings = json.loads(env["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"])
    assert settings["namespace"] == build_ns
    assert settings["source_bucket"] == config["buckets"]["artifacts"]
    assert settings["service_image"] == candidate["images"]["service"]["image_ref"]
    assert settings["cache_secret_name"] is None
    assert len([doc for doc in docs if doc["kind"] == "Deployment"]) == 1
    build_docs = [doc for doc in docs if doc["metadata"].get("namespace") == build_ns]
    role = next(doc for doc in build_docs if doc["kind"] == "Role")
    assert all("secrets" not in rule["resources"] for rule in role["rules"])
    assert "delete" in next(
        rule["verbs"] for rule in role["rules"] if rule["resources"] == ["pods"]
    )
    quota = next(doc for doc in build_docs if doc["kind"] == "ResourceQuota")
    assert settings["max_concurrent"] == concurrency
    collector = next(doc for doc in docs if doc["kind"] == "ConfigMap"
                     and "LOOM_EXECUTION_CAPACITY_COLLECTOR_TARGET_ID" in doc["data"])
    assert collector["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_BUILD_CONCURRENCY_LIMIT"] == str(concurrency)
    assert quota["spec"]["hard"]["pods"] == str(concurrency)
    assert quota["spec"]["hard"]["count/jobs.batch"] == str(concurrency)
    assert quota["spec"]["hard"]["requests.cpu"] == f"{1000 * concurrency}m"
    assert quota["spec"]["hard"]["requests.memory"] == f"{2048 * concurrency}Mi"
    assert quota["spec"]["hard"]["requests.ephemeral-storage"] == f"{16384 * concurrency}Mi"
    for resource in ("cpu", "memory", "ephemeral-storage"):
        assert quota["spec"]["hard"][f"limits.{resource}"] == quota["spec"]["hard"][f"requests.{resource}"]
    assert quota["spec"]["hard"]["count/configmaps"] == str(concurrency + 1)
    network = next(doc for doc in build_docs if doc["kind"] == "NetworkPolicy")
    assert network["spec"]["ingress"] == []
    public = network["spec"]["egress"][1]
    assert "169.254.0.0/16" in public["to"][0]["ipBlock"]["except"]
    assert "10.0.0.0/8" in public["to"][0]["ipBlock"]["except"]


@pytest.mark.parametrize(
    "builder",
    [
        {"registry_repository": "docker.io/public/task-images"},
        {
            "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
            "max_concurrent": 17,
        },
        {
            "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
            "ephemeral_storage_mib": 2048,
        },
        {"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "privileged": True},
        {
            "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
            "cache_bucket": "loom-integration-backup",
        },
    ],
)
def test_native_build_configuration_rejects_wrong_boundary(
    platform_inputs: tuple, builder: dict
) -> None:
    config, candidate, profile = platform_inputs
    config["task_image_builder"] = builder
    with pytest.raises(NebiusPlatformError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_native_build_configuration_accepts_phase3_cache_policy(
    platform_inputs: tuple,
) -> None:
    config, candidate, profile = platform_inputs
    config["task_image_builder"] = {
        "registry_repository": "cr.eu-north1.nebius.cloud/test/task-images",
        "cache_bucket": "loom-integration-artifacts-cache",
        "snapshotter": "overlayfs",
        "compatible_revision_cache": "same_task",
        "export_cache_mode": "min",
        "cache_transfer": "tar",
        "oci_export_format": "directory",
    }
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    actuator = next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "Deployment")
    env = {
        row["name"]: row.get("value")
        for row in actuator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    settings = json.loads(env["LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER"])
    assert settings["compatible_revision_cache"] == "same_task"
    assert settings["export_cache_mode"] == "min"
    assert settings["snapshotter"] == "overlayfs"
    assert settings["cache_transfer"] == "tar"
    assert settings["oci_export_format"] == "directory"


@pytest.fixture
def platform_inputs() -> tuple[dict, dict, dict]:
    config = json.loads((ROOT / "deploy/nebius/integration.platform.json.example").read_text())
    for key in (
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
    ):
        config[key] = "test-" + key.replace("_", "-")
    config["project_id"] = "project-test"
    config["quota_parent_id"] = "tenant-test"
    config["kubernetes_api_server"] = "https://api.cluster.test"
    config["execution_price"]["vcpu_microusd_per_hour"] = 1000
    for key in ("postgres_image", "backup_image"):
        config[key] = "cr.eu-north1.nebius.cloud/test/postgres@sha256:" + "a" * 64
    config["buckets"] = {key: "loom-integration-" + key for key in config["buckets"]}
    images = {
        key: {"image_ref": "cr.eu-north1.nebius.cloud/test/" + key + "@sha256:" + "b" * 64}
        for key in (
            "service",
            "control_plane",
            "web",
            "gateway",
            "execution_runtime",
            "execution_actuator",
        )
    }
    profile = {
        "candidate_sha": "c" * 40,
        "task_image_ref": images["service"]["image_ref"],
        "runtime_image_ref": images["execution_runtime"]["image_ref"],
    }
    candidate = {
        "candidate_sha": "c" * 40,
        "source_ref": "refs/heads/codex/nebius-main",
        "repository": "qianyi-sun/loom",
        "images": images,
    }
    return config, candidate, profile


@pytest.mark.parametrize("component", ["worker", "harbor_runtime"])
def test_agent_profile_binding_matches_published_candidate(platform_inputs: tuple, component: str) -> None:
    config, candidate, profile = platform_inputs
    image = "cr.eu-north1.nebius.cloud/test/worker@sha256:" + "d" * 64
    candidate["images"][component] = {"image_ref": image}
    profile["agent_image_ref"] = image
    build_platform(config, candidate, profile, {}, repo_root=ROOT)
    profile["agent_image_ref"] = image.replace("d" * 64, "e" * 64)
    with pytest.raises(NebiusPlatformError, match="execution images"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_project_cannot_replace_tenant_quota_parent(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    config["quota_parent_id"] = config["project_id"]
    with pytest.raises(NebiusPlatformError, match=r"quota_parent_id.*tenant"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_independent_namespace_routing_storage_and_no_secret_material(
    platform_inputs: tuple, tmp_path: Path
) -> None:
    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    serialized = json.dumps(files)
    assert "oldlab" not in serialized and "gb10" not in serialized
    assert "hostPath" not in serialized and "loom.svc" not in serialized
    docs = [doc for batch in files.values() for doc in batch]
    assert not any(doc["kind"] == "Secret" for doc in docs)
    assert {doc["metadata"].get("namespace") for doc in docs if "namespace" in doc["metadata"]} == {
        config["namespace"],
        config["execution_namespace"],
    }
    postgres = next(doc for doc in docs if doc["kind"] == "StatefulSet")
    assert (
        postgres["spec"]["volumeClaimTemplates"][0]["spec"]["storageClassName"]
        == "compute-csi-default-sc"
    )
    services = [doc for doc in docs if doc["kind"] == "Service"]
    assert [
        doc["metadata"]["name"] for doc in services if doc["spec"].get("type") == "LoadBalancer"
    ] == ["loom-web"]
    for deployment in [doc for doc in docs if doc["kind"] == "Deployment"]:
        pod = deployment["spec"]["template"]["spec"]
        assert pod["nodeSelector"]["loom.nebius/platform"] == "integration"
        for volume in pod.get("volumes", []):
            if volume["name"] == "db-ca":
                assert volume["secret"]["items"] == [{"key": "ca.crt", "path": "ca.crt"}]
    output = tmp_path / "render"
    result = write_platform(files, config, candidate, output)
    assert set(result["files"]) == set(files)
    assert not (output / "manifest.json").exists()
    assert not (output / "candidate.json").exists()
    assert not (output / "environment.json").exists()
    (output / "README.md").write_text("Operator notes")
    write_platform(files, config, candidate, output)
    assert (output / "README.md").read_text() == "Operator notes"


@pytest.mark.parametrize("environment,label", [
    ("development", "Nebius integration"),
    ("staging", "Staging"),
])
def test_standalone_environment_reaches_catalog_processes_and_frontend(
    platform_inputs: tuple, environment: str, label: str,
) -> None:
    config, candidate, profile = platform_inputs
    config["environment"] = environment
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    cm = next(row for row in files["10-config-network.yaml"] if row["kind"] == "ConfigMap")
    assert json.loads(cm["data"]["environment.json"])["environment"] == environment
    targets = json.loads(cm["data"]["catalog.json"])["topology"]["targets"]
    assert targets and all(target["environment"] == environment for target in targets)
    processes = {
        row["metadata"]["name"]: {
            env["name"]: env.get("value")
            for env in row["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        for row in files["40-services.yaml"] if row["kind"] == "Deployment"
    }
    for component in ("loom-service", "loom-control-plane", "loom-llm-gateway"):
        assert processes[component]["LOOM_ENV"] == environment
    assert processes["loom-control-plane"]["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"] == environment
    assert processes["loom-web"]["LOOM_FRONTEND_ENVIRONMENT"] == environment
    assert processes["loom-web"]["LOOM_FRONTEND_ENVIRONMENT_LABEL"] == label


def test_config_only_change_creates_new_jobs_and_rollout(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    first = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    changed = dict(config, model_provider_base_url="https://provider.example/v1")
    second = build_platform(changed, candidate, profile, {}, repo_root=ROOT)
    assert (
        first["50-configure.yaml"][0]["metadata"]["name"]
        != second["50-configure.yaml"][0]["metadata"]["name"]
    )

    def templates(files):
        return [
            row["spec"]["template"]["metadata"]
            for row in files["40-services.yaml"]
            if row["kind"] == "Deployment"
        ]

    assert templates(first) != templates(second)


def test_batch_runner_token_is_required_by_bootstrap_and_service(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    secret = {"secretKeyRef": {"name": "loom-platform-batch-runner", "key": "token"}}
    migration_env = files["30-migrate.yaml"][0]["spec"]["template"]["spec"]["containers"][0]["env"]
    assert next(row for row in migration_env if row["name"] == "LOOM_BATCH_RUNNER_TOKEN") == {
        "name": "LOOM_BATCH_RUNNER_TOKEN",
        "valueFrom": secret,
    }
    for doc in files["40-services.yaml"]:
        if doc["kind"] != "Deployment":
            continue
        env = doc["spec"]["template"]["spec"]["containers"][0].get("env", [])
        matches = [row for row in env if row["name"] == "LOOM_SVC_BATCH_RUNNER_CP_TOKEN"]
        assert matches == (
            [{"name": "LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "valueFrom": secret}]
            if doc["metadata"]["name"] == "loom-service"
            else []
        )
    assert "loom-platform-batch-runner" not in json.dumps(files["50-configure.yaml"])


@pytest.mark.parametrize("changed_input", ["profile", "keyring"])
def test_runtime_configuration_changes_restart_consumers(
    platform_inputs: tuple, changed_input: str
) -> None:
    config, candidate, profile = platform_inputs
    first = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    updated_profile = (
        dict(profile, image_admission={"admissions": [{"signing_key_id": "rotated"}]})
        if changed_input == "profile"
        else profile
    )
    updated_keyring = (
        {"keys": [{"signing_key_id": "rotated"}]} if changed_input == "keyring" else {}
    )
    second = build_platform(config, candidate, updated_profile, updated_keyring, repo_root=ROOT)
    assert (
        first["50-configure.yaml"][0]["metadata"]["name"]
        != second["50-configure.yaml"][0]["metadata"]["name"]
    )
    before = [
        row["spec"]["template"]["metadata"]
        for row in first["40-services.yaml"]
        if row["kind"] == "Deployment"
    ]
    after = [
        row["spec"]["template"]["metadata"]
        for row in second["40-services.yaml"]
        if row["kind"] == "Deployment"
    ]
    assert before != after


@pytest.mark.parametrize(
    "field,value",
    [
        ("namespace", "loom-staging"),
        ("environment", "production"),
        ("environment", "dev"),
        ("environment", None),
        ("storage_endpoint", "https://storage.example.com"),
        ("postgres_image", "postgres:latest"),
        ("unknown_token", "secret"),
        ("max_concurrent", 0),
        ("model_provider_base_url", "https://user:secret@example.com"),
    ],
)
def test_rejects_implicit_or_unprotected_environment(
    platform_inputs: tuple, field: str, value: object
) -> None:
    config, candidate, profile = platform_inputs
    config[field] = value
    with pytest.raises(NebiusPlatformError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_profile_candidate_mismatch_rejected(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    profile["candidate_sha"] = "d" * 40
    with pytest.raises(NebiusPlatformError, match="profile"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_actual_execution_job_tolerates_both_dedicated_pool_taints(platform_inputs: tuple) -> None:
    from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
    from tests.unit.test_execution_actuator import _lease

    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    actuator = next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "Deployment")
    env = {
        row["name"]: row.get("value")
        for row in actuator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    lease = _lease()
    lease.target_id = config["target_id"]
    lease.namespace_name = config["execution_namespace"]
    target = ExecutionTargetRuntime(
        target_id=lease.target_id,
        namespace=lease.namespace_name,
        node_selector=json.loads(env["LOOM_EXECUTION_ACTUATOR_NODE_SELECTOR"]),
        tolerations=tuple(json.loads(env["LOOM_EXECUTION_ACTUATOR_TOLERATIONS"])),
        credential_broker_url=env["LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL"],
    )
    job = render_execution_job(lease, target=target)
    pod = job["spec"]["template"]["spec"]
    assert pod["nodeSelector"]["loom.nebius/platform"] == "integration"
    assert {(row["key"], row["value"]) for row in pod["tolerations"]} >= {
        ("loom.nebius/execution", "true"),
        ("loom.nebius/platform", "integration"),
    }
    import yaml

    historical = next(
        doc
        for doc in yaml.safe_load_all(
            (ROOT / "deploy/k8s/nebius-capacity-collector.yaml").read_text()
        )
        if doc and doc["kind"] == "ConfigMap"
    )
    selector_key = "LOOM_EXECUTION_CAPACITY_COLLECTOR_NODE_LABEL_SELECTOR"
    historical_labels = dict(
        item.split("=", 1) for item in historical["data"][selector_key].split(",")
    )
    new_collector = next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "ConfigMap")
    new_labels = dict(item.split("=", 1) for item in new_collector["data"][selector_key].split(","))
    assert not all(
        pod["nodeSelector"].get(key) == value for key, value in historical_labels.items()
    ), "integration nodes must not enter the historical collector's inventory"
    assert pod["nodeSelector"]["loom.nebius/node-role"] == "integration-execution"
    assert all(pod["nodeSelector"].get(key) == value for key, value in new_labels.items())


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://user:secret@localhost/loom",
        "postgresql://user:secret@loom-postgres.loom-nebius-platform.svc/loom?sslmode=disable",
        "postgresql://user:secret@loom-postgres.loom-staging.svc/loom?sslmode=verify-full&sslrootcert=/var/run/loom-db/ca.crt",
    ],
)
def test_database_cannot_attach_to_historical_or_unverified_authority(url: str) -> None:
    with pytest.raises(ValueError, match="namespace-local TLS"):
        database_url(url, "loom-nebius-platform")


def test_migration_diagnostic_preserves_revision_but_not_secret() -> None:
    import subprocess

    error = MigrationError(
        subprocess.CompletedProcess(
            [],
            1,
            "",
            "Running upgrade 0123 -> 0124\npsycopg.errors.UndefinedTable: postgres://user:secret@host/loom\n",
        )
    )
    assert error.details == {
        "exit_code": 1,
        "migration_revision": "0124",
        "database_error": "UndefinedTable",
    }
    assert "secret" not in json.dumps(error.details)


@pytest.mark.parametrize("reason_code", [
    "application_database_access_active",
    "application_database_schema_busy",
    "application_database_schema_guard_not_installed",
    "application_database_isolation",
])
def test_migration_diagnostic_preserves_terminal_application_schema_reason(reason_code: str) -> None:
    import subprocess

    # The application guard runs before Alembic emits a Running upgrade line.
    error = MigrationError(subprocess.CompletedProcess(
        [], 1, "", "Traceback (most recent call last):\n"
        '    raise RuntimeError("private-db-secret")\n'
        f"RuntimeError: {reason_code}\n",
    ))
    assert error.details == {
        "exit_code": 1,
        "migration_revision": "unknown",
        "database_error": "RuntimeError",
        "reason_code": reason_code,
    }
    assert "private-db-secret" not in json.dumps(error.details)


@pytest.mark.parametrize("stderr", [
    "RuntimeError: application_database_access_active private-db-secret\n",
    "RuntimeError: private-db-secret_application_database_access_active\n",
    "RuntimeError: application_database_access_active_private-db-secret\n",
    "RuntimeError: private-db-secret\n",
    '    raise RuntimeError("application_database_access_active")\n'
    "RuntimeError: private-db-secret\n",
    "RuntimeError: application_database_access_active\n"
    "RuntimeError: private-db-secret\n",
    "private-db-secret RuntimeError: application_database_access_active\n",
    "RuntimeError: application_database_access_active\nprivate-db-secret\n",
])
def test_migration_diagnostic_rejects_unrecognized_or_nonterminal_reason(stderr: str) -> None:
    import subprocess

    error = MigrationError(subprocess.CompletedProcess([], 1, "", stderr))
    assert "reason_code" not in error.details
    assert "private-db-secret" not in json.dumps(error.details)
    assert str(error) == "migration failed"


def test_migration_diagnostic_main_exports_only_safe_guard_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import subprocess

    from loom import nebius_platform_bootstrap as bootstrap

    config = tmp_path / "platform.json"
    config.write_text("{}")
    monkeypatch.setenv("LOOM_PLATFORM_CONFIG", str(config))
    monkeypatch.setattr(bootstrap.sys, "argv", ["bootstrap", "database"])

    def fail_migration(_config: dict) -> None:
        raise MigrationError(subprocess.CompletedProcess(
            [], 1, "private-stdout-secret", "private-stderr-secret\n"
            "RuntimeError: application_database_access_active\n",
        ))

    monkeypatch.setattr(bootstrap, "bootstrap_database", fail_migration)
    assert bootstrap.main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {
        "phase": "database",
        "error_type": "MigrationError",
        "exit_code": 1,
        "migration_revision": "unknown",
        "database_error": "RuntimeError",
        "reason_code": "application_database_access_active",
    }
    assert "private-" not in output.err


def test_rendered_application_settings_and_startup_factories_accept_signed_profile(
    platform_inputs: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts.ops.nebius_candidate import create_candidate

    from loom_control_plane.app import create_app as control_plane_app
    from loom_control_plane.config import ControlPlaneSettings
    from loom_llm_gateway.app import create_app as gateway_app
    from loom_llm_gateway.config import GatewaySettings
    from loom_service.app import create_app as service_app
    from loom_service.config import LoomServiceSettings
    from tests.ops.test_nebius_candidate import inputs

    config, _, _ = platform_inputs
    build_record, signer, trust = inputs(tmp_path)
    candidate, profile = create_candidate(
        build_record, signing_key=signer, signing_key_id="publisher", keyring_json=trust
    )
    files = build_platform(config, candidate, profile, json.loads(trust), repo_root=ROOT)
    for name, prefix, model, factory in (
        ("loom-service", "LOOM_SVC_", LoomServiceSettings, service_app),
        ("loom-control-plane", "LOOM_CP_", ControlPlaneSettings, control_plane_app),
        ("loom-llm-gateway", "LOOM_GW_", GatewaySettings, gateway_app),
    ):
        deployment = next(
            doc
            for doc in files["40-services.yaml"]
            if doc["kind"] == "Deployment" and doc["metadata"]["name"] == name
        )
        with monkeypatch.context() as scoped:
            for row in deployment["spec"]["template"]["spec"]["containers"][0]["env"]:
                if row["name"].startswith(prefix) and not row["name"].startswith("LOOM_GW_LOCAL_"):
                    assert row["name"][len(prefix) :].lower() in model.model_fields, row["name"]
                value = row.get("value", "synthetic-test-secret-" + "a" * 32)
                if row["name"].endswith("_DB_URL"):
                    value = (
                        "postgresql+psycopg://test:test@loom-postgres.loom-nebius-platform.svc/loom"
                    )
                scoped.setenv(row["name"], value)
            settings = model()
            app = factory(settings)
            assert app is not None
            assert settings.minio_endpoint == config["storage_endpoint"]
            if name == "loom-service":
                assert settings.gateway_url.host == f"loom-llm-gateway.{config['namespace']}.svc"
                assert (
                    settings.control_plane_url.host
                    == f"loom-control-plane.{config['namespace']}.svc"
                )
            elif name == "loom-control-plane":
                assert settings.service_execution_scheduler_enabled
                assert settings.service_execution_source_bucket == config["buckets"]["source"]
            else:
                assert (
                    settings.local_providers["yibu"].base_url == config["model_provider_base_url"]
                )


def test_public_tls_renewal_uses_same_candidate_and_persistent_state(
    platform_inputs: tuple,
) -> None:
    config, candidate, profile = platform_inputs
    config["public_tls_bootstrap"] = True
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    services = files["40-services.yaml"]
    web = next(doc for doc in services if doc["metadata"]["name"] == "loom-web")
    pod = web["spec"]["template"]["spec"]
    nginx, tls = pod["containers"]
    assert nginx["image"] == tls["image"] == candidate["images"]["web"]["image_ref"]
    assert "command" not in nginx
    assert tls["readinessProbe"]["httpGet"]["scheme"] == "HTTPS"
    assert tls["readinessProbe"]["httpGet"]["httpHeaders"] == [
        {"name": "Host", "value": config["public_host"]}
    ]
    claims = [
        doc for batch in files.values() for doc in batch if doc["kind"] == "PersistentVolumeClaim"
    ]
    assert len(claims) == 1 and claims[0] in services
    assert claims[0]["spec"] == {
        "accessModes": ["ReadWriteOnce"],
        "storageClassName": config["storage_class"],
        "resources": {"requests": {"storage": "4Gi"}},
    }
    cm = files["10-config-network.yaml"][0]
    caddy = json.loads(cm["data"]["public-tls.json"])
    server = caddy["apps"]["http"]["servers"]["public"]
    assert server["listen"] == [":8443"]
    assert server["automatic_https"]["ignore_loaded_certificates"] is True
    assert server["tls_connection_policies"] == [{"default_sni": config["public_host"]}]
    assert all("tags" not in item for item in caddy["apps"]["tls"]["certificates"]["load_files"])
    assert "certificate_selection" not in json.dumps(caddy)
    issuer = caddy["apps"]["tls"]["automation"]["policies"][0]["issuers"][0]
    assert issuer["challenges"]["http"]["disabled"] is True
    assert issuer["challenges"]["tls-alpn"]["alternate_port"] == 8443
    assert files["70-public.yaml"][0]["spec"]["ports"][0]["port"] == 443


@pytest.mark.parametrize("bootstrap", [False, None])
def test_steady_public_tls_does_not_load_bootstrap(platform_inputs: tuple, bootstrap) -> None:
    config, candidate, profile = platform_inputs
    config.pop("public_tls_bootstrap", None)
    if bootstrap is not None:
        config["public_tls_bootstrap"] = bootstrap
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    caddy = json.loads(files["10-config-network.yaml"][0]["data"]["public-tls.json"])
    assert "certificates" not in caddy["apps"]["tls"]
    web = next(doc for doc in files["40-services.yaml"] if doc["metadata"]["name"] == "loom-web")
    assert not any(v["name"] == "public-tls" for v in web["spec"]["template"]["spec"]["volumes"])


def test_public_tls_bootstrap_requires_boolean(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    config["public_tls_bootstrap"] = "false"
    with pytest.raises(NebiusPlatformError, match="public_tls_bootstrap must be a boolean"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_execution_quota_uses_native_envelope_and_namespace_control_requests(
    platform_inputs: tuple,
) -> None:
    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    docs = files["60-execution.yaml"]
    quota = next(doc for doc in docs if doc["kind"] == "ResourceQuota")
    assert config["max_concurrent"] is None
    assert quota["spec"]["hard"] == {
        "pods": "6403",
        "requests.cpu": "1600300m",
        "requests.memory": "6553984Mi",
        "requests.ephemeral-storage": "8192000Mi",
    }
    # Cold-template discovery adds no mutation or cloud privileges.
    role = next(doc for doc in docs if doc["kind"] == "ClusterRole" and doc["metadata"]["name"].endswith("-collector"))
    assert {"apiGroups": ["apps"], "resources": ["daemonsets"], "verbs": ["get", "list"]} in role[
        "rules"
    ]
    assert all(set(rule["verbs"]) <= {"get", "list"} for rule in role["rules"])
    usage = next(doc for doc in docs if doc["kind"] == "ClusterRole" and doc["metadata"]["name"].endswith("-actuator-usage"))
    assert usage["rules"] == [{"apiGroups": [""], "resources": ["nodes", "nodes/stats"], "verbs": ["get"]}]
    assert len({(doc["kind"], doc["metadata"]["name"]) for doc in docs}) == len(docs)


def test_execution_quota_preserves_explicit_lower_limits(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    config["capacity_policy"].update(
        max_nodes=3, max_vcpu_millis=48000, max_memory_mib=196608, max_storage_mib=245760
    )
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    quota = next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "ResourceQuota")
    assert quota["spec"]["hard"] == {
        "pods": "195",
        "requests.cpu": "48300m",
        "requests.memory": "196992Mi",
        "requests.ephemeral-storage": "245760Mi",
    }
    # An explicit namespace restriction, including a deliberate stop, wins.
    config["execution_resource_quota"] = {"pods": "0", "requests.cpu": "1"}
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    quota = next(doc for doc in files["60-execution.yaml"] if doc["kind"] == "ResourceQuota")
    assert quota["spec"]["hard"]["pods"] == "0"
    assert quota["spec"]["hard"]["requests.cpu"] == "1"
    assert quota["spec"]["hard"]["requests.memory"] == "196992Mi"


@pytest.mark.parametrize(
    "hard",
    [
        {"pods": "-1"},
        {"requests.cpu": "-1m"},
        {"requests.memory": "bad"},
        {"secrets": "1"},
        {"pods": 4},
    ],
)
def test_execution_quota_rejects_invalid_or_unowned_resource_types(
    platform_inputs: tuple, hard: dict
) -> None:
    config, candidate, profile = platform_inputs
    config["execution_resource_quota"] = hard
    with pytest.raises(NebiusPlatformError, match="execution_resource_quota"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_execution_policy_rejects_beyond_native_node_ceiling(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    config["capacity_policy"]["max_nodes"] = 101
    with pytest.raises(NebiusPlatformError, match="max_nodes"):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


@pytest.fixture
def regional_inputs(platform_inputs: tuple) -> tuple:
    from copy import deepcopy

    config, candidate, profile = platform_inputs
    regional_example = json.loads(
        (ROOT / "deploy/nebius/regional.platform.json.example").read_text()
    )
    regional = deepcopy(regional_example["regional_execution_targets"][0])
    for key in ("project_id", "quota_parent_id", "cluster_id", "execution_node_group_id"):
        regional[key] = config[key] + "-west"
    regional["kubernetes_api_server"] = "https://api.west.test"
    regional["execution_price"] = dict(
        config["execution_price"],
        region="eu-west1",
        sku=regional["execution_price"]["sku"],
        source_version="test-west",
    )
    config["public_gateway_ipv4"] = "8.8.4.4"
    config["regional_execution_targets"] = [regional]
    return config, candidate, profile


def test_regional_manifests_separate_native_roles_from_primary_processes(
    regional_inputs: tuple,
) -> None:
    from loom.nebius_platform_render import build_regional_execution

    config, candidate, profile = regional_inputs
    target = config["regional_execution_targets"][0]
    primary = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    remote = build_regional_execution(config, candidate, repo_root=ROOT)[target["target_id"]]
    assert not {
        "Deployment",
        "CronJob",
        "StatefulSet",
        "Service",
        "Secret",
        "PersistentVolumeClaim",
    } & {doc["kind"] for doc in remote}
    assert all(
        doc["metadata"].get("namespace") in (None, target["execution_namespace"]) for doc in remote
    )
    assert all(
        doc["metadata"].get("namespace") != target["execution_namespace"]
        for docs in primary.values()
        for doc in docs
    )
    bindings = [doc for doc in remote if doc["kind"] in {"RoleBinding", "ClusterRoleBinding"}]
    assert {doc["subjects"][0]["name"] for doc in bindings} == set(
        target["service_account_ids"].values()
    )
    usage_binding = next(doc for doc in bindings if doc["metadata"]["name"].endswith("-actuator-usage"))
    assert usage_binding["subjects"][0]["name"] == target["service_account_ids"]["actuator"]
    assert all(
        doc["subjects"]
        == [
            {
                "kind": "User",
                "apiGroup": "rbac.authorization.k8s.io",
                "name": doc["subjects"][0]["name"],
            }
        ]
        for doc in bindings
    )
    gateway_role = next(
        doc
        for doc in remote
        if doc["metadata"]["name"].endswith("gateway-tokenreview") and doc["kind"] == "ClusterRole"
    )
    assert gateway_role["rules"] == [
        {"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]}
    ]
    collector_role = next(
        doc for doc in remote if doc["kind"] == "ClusterRole" and doc != gateway_role
    )
    assert all(set(rule["verbs"]) <= {"get", "list"} for rule in collector_role["rules"])
    for role in (
        doc for doc in remote if doc["kind"] in {"Role", "ClusterRole"} and doc != gateway_role
    ):
        assert all(
            not {"secrets", "tokenreviews", "serviceaccounts/token"} & set(rule["resources"])
            for rule in role["rules"]
        )
    quota = next(doc for doc in remote if doc["kind"] == "ResourceQuota")
    assert quota["spec"]["hard"]["pods"] == "6400"
    egress = next(
        doc
        for doc in remote
        if doc["kind"] == "NetworkPolicy" and doc["metadata"]["name"].endswith("-egress")
    )
    assert egress["spec"]["egress"][-1] == {
        "to": [{"ipBlock": {"cidr": "8.8.4.4/32"}}],
        "ports": [{"protocol": "TCP", "port": 443}],
    }
    process_docs = primary["60-execution.yaml"]
    actuator = next(
        doc
        for doc in process_docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"] == target["target_id"] + "-actuator"
    )
    pod = actuator["spec"]["template"]["spec"]
    env = {row["name"]: row.get("value") for row in pod["containers"][0]["env"]}
    assert env["LOOM_EXECUTION_ACTUATOR_NAMESPACE"] == target["execution_namespace"]
    assert env["LOOM_EXECUTION_ACTUATOR_KUBERNETES_ENDPOINT"] == "https://api.west.test"
    assert (
        env["LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL"]
        == "https://" + config["public_host"] + "/internal/service-execution"
    )
    assert env["LOOM_EXECUTION_ACTUATOR_POD_IDENTITY_AUDIENCE"] == "loom-execution"
    assert pod["nodeSelector"]["loom.nebius/node-role"] == "system"
    assert any(
        volume.get("secret", {}).get("secretName") == "loom-execution-actuator-db"
        for volume in pod["volumes"]
    )
    collector = next(
        doc
        for doc in process_docs
        if doc["kind"] == "CronJob"
        and doc["metadata"]["name"] == target["target_id"] + "-collector"
    )
    collector_pod = collector["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    projected = next(
        volume for volume in collector_pod["volumes"] if volume["name"] == "projected-credentials"
    )
    assert (
        projected["projected"]["sources"][0]["secret"]["name"]
        == target["target_id"] + "-collector-kubernetes"
    )
    collector_env = {row["name"]: row.get("value") for row in collector_pod["containers"][0]["env"]}
    assert (
        collector_env["LOOM_EXECUTION_CAPACITY_COLLECTOR_KUBERNETES_ENDPOINT"]
        == "https://api.west.test"
    )
    quota = next(doc for doc in process_docs if doc["kind"] == "ResourceQuota")
    assert quota["spec"]["hard"]["pods"] == "6406"
    gateway = next(
        doc
        for doc in primary["40-services.yaml"]
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-llm-gateway"
    )
    volumes = gateway["spec"]["template"]["spec"]["volumes"]
    assert any(
        volume.get("secret", {}).get("secretName") == target["target_id"] + "-gateway-kubernetes"
        for volume in volumes
    )
    catalog = json.loads(primary["10-config-network.yaml"][0]["data"]["catalog.json"])
    targets = catalog["topology"]["targets"]
    assert [row["health_role"] for row in targets] == ["primary", "secondary"]
    assert targets[1]["pod_identity_audience"] == "loom-execution"
    assert "pod_identity_audience" not in targets[0]


@pytest.mark.parametrize(
    "target_id", ["nebius-eu-west1-integration", "nebius-loom-execution-actuator-west"]
)
def test_regional_object_renaming_preserves_published_images_and_settings(
    regional_inputs: tuple, target_id: str
) -> None:
    config, candidate, profile = regional_inputs
    target = config["regional_execution_targets"][0]
    target["target_id"] = target_id
    image = "cr.eu-north1.nebius.cloud/test/loom-execution-actuator@sha256:" + "d" * 64
    candidate["images"]["execution_actuator"]["image_ref"] = image
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    documents = files["60-execution.yaml"]
    for kind, role in (("Deployment", "actuator"), ("CronJob", "collector")):
        doc = next(
            row
            for row in documents
            if row["kind"] == kind and row["metadata"]["name"] == target_id + "-" + role
        )
        spec = doc["spec"] if kind == "Deployment" else doc["spec"]["jobTemplate"]["spec"]
        template = spec["template"]
        pod = template["spec"]
        assert pod["serviceAccountName"] == target_id + "-" + role
        assert template["metadata"]["labels"]["app.kubernetes.io/name"] == target_id + "-" + role
        if kind == "Deployment":
            affinity = pod["affinity"]["podAntiAffinity"][
                "preferredDuringSchedulingIgnoredDuringExecution"
            ]
            assert (
                affinity[0]["podAffinityTerm"]["labelSelector"]["matchLabels"][
                    "app.kubernetes.io/name"
                ]
                == target_id + "-actuator"
            )
            assert all(
                template["metadata"]["labels"][key] == value
                for key, value in doc["spec"]["selector"]["matchLabels"].items()
            )
        for container in pod.get("initContainers", []) + pod["containers"]:
            assert container["image"] == image
        if role == "actuator":
            env = {row["name"]: row.get("value") for row in pod["containers"][0]["env"]}
            assert env["LOOM_EXECUTION_ACTUATOR_TARGET_ID"] == target_id
        else:
            assert (
                pod["containers"][0]["envFrom"][0]["configMapRef"]["name"]
                == target_id + "-collector"
            )
            settings = next(
                row
                for row in documents
                if row["kind"] == "ConfigMap"
                and row["metadata"]["name"] == target_id + "-collector"
            )
            assert settings["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_TARGET_ID"] == target_id


def test_regional_public_routes_preserve_exact_model_and_broker_boundaries(
    regional_inputs: tuple,
) -> None:
    from loom.nebius_platform_render import public_tls_config

    config, _, _ = regional_inputs
    routes = public_tls_config(config)["apps"]["http"]["servers"]["public"]["routes"][0]["handle"][
        -1
    ]["routes"]
    assert routes[0]["match"] == [{"path": ["/internal/service-execution/*"]}]
    assert routes[1]["match"][0]["method"] == ["POST"]
    assert "/v1/chat/completions" in routes[1]["match"][0]["path"]
    assert "/admin/*" in routes[2]["match"][0]["path"]
    assert routes[2]["handle"] == [{"handler": "static_response", "status_code": 404}]
    assert routes[3]["match"] == [{"path": ["/api/v1/*"]}]
    assert routes[0]["handle"][0]["flush_interval"] == -1
    assert routes[0]["handle"][0]["upstreams"] == [
        {"dial": f"loom-llm-gateway.{config['namespace']}.svc:9100"}
    ]


@pytest.mark.parametrize(
    "change", ["missing_ip", "private_ip", "duplicate_identity", "non_eu", "duplicate_cluster"]
)
def test_regional_render_rejects_ambiguous_or_unreachable_bindings(
    regional_inputs: tuple, change: str
) -> None:
    config, candidate, profile = regional_inputs
    target = config["regional_execution_targets"][0]
    if change == "missing_ip":
        config.pop("public_gateway_ipv4")
    elif change == "private_ip":
        config["public_gateway_ipv4"] = "10.0.0.1"
    elif change == "duplicate_identity":
        target["service_account_ids"]["gateway"] = target["service_account_ids"]["actuator"]
    elif change == "non_eu":
        target["region"] = "us-central1"
        target["execution_price"]["region"] = "us-central1"
    else:
        target["cluster_scope_id"] = config["cluster_scope_id"]
    with pytest.raises(NebiusPlatformError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_regional_cli_requires_separate_cluster_output(
    regional_inputs: tuple,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from scripts.ops import render_nebius_platform as render_cli

    config, candidate, profile = regional_inputs
    paths = {}
    for name, value in (
        ("environment-config", config),
        ("candidate", candidate),
        ("runtime-profile", profile),
        ("trusted-keyring", {}),
    ):
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(value))
        paths[name] = path
    argv = ["render_nebius_platform.py"]
    for name, path in paths.items():
        argv.extend(["--" + name, str(path)])
    argv.extend(["--output", str(tmp_path / "primary")])
    monkeypatch.setattr("sys.argv", argv)
    assert render_cli.main() == 1
    assert "--regional-output" in capsys.readouterr().err
    assert not (tmp_path / "primary").exists()
    monkeypatch.setattr("sys.argv", [*argv, "--regional-output", str(tmp_path / "remote")])
    assert render_cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    target = config["regional_execution_targets"][0]
    assert result["regional_files"] == [target["target_id"] + ".yaml"]
    assert not (tmp_path / "primary" / result["regional_files"][0]).exists()
    assert (tmp_path / "remote" / result["regional_files"][0]).is_file()


def test_task_egress_defaults_remain_disabled(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    assert 'task-egress.json' not in json.dumps(files)
    assert 'LOOM_GW_TASK_EGRESS_CONFIG_FILE' not in json.dumps(files)
    published = json.loads(files['10-config-network.yaml'][0]['data']['profile.json'])
    assert 'supports_task_web_egress' not in published
    catalog = json.loads(files['10-config-network.yaml'][0]['data']['catalog.json'])
    assert 'supports_task_web_egress' not in catalog['execution_class']
    assert catalog['execution_class']['class_id'] == 'linux-amd64-cpu-pod-v1'


def test_task_egress_cannot_reuse_the_immutable_default_class(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    config['task_egress'] = {'protected_cidrs': ['198.51.100.0/24']}
    profile['supports_task_web_egress'] = True
    with pytest.raises(NebiusPlatformError, match='execution class'):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_task_egress_mounts_explicit_bounded_gateway_policy(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    config['task_egress'] = {
        'protected_cidrs': ['198.51.100.0/24', '2001:db8::/32'],
        'maximum_connections': 12,
        'maximum_connections_per_lease': 3,
    }
    profile['supports_task_web_egress'] = True
    profile['execution_class_id'] = 'linux-amd64-cpu-web-pod-v1'
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    cm = files['10-config-network.yaml'][0]
    assert json.loads(cm['data']['task-egress.json']) == config['task_egress']
    assert json.loads(cm['data']['environment.json'])['task_egress'] == config['task_egress']
    assert json.loads(cm['data']['profile.json'])['supports_task_web_egress'] is True
    catalog = json.loads(cm['data']['catalog.json'])
    assert catalog['execution_class']['class_id'] == profile['execution_class_id']
    assert catalog['execution_class']['supports_task_web_egress'] is True
    assert catalog['topology']['execution_class_id'] == profile['execution_class_id']
    assert all(t['execution_class_id'] == profile['execution_class_id']
               for t in catalog['topology']['targets'])
    gateway = next(row for row in files['40-services.yaml']
                   if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-llm-gateway')
    pod = gateway['spec']['template']['spec']
    container = pod['containers'][0]
    env = {row['name']: row.get('value') for row in container['env']}
    path = env['LOOM_GW_TASK_EGRESS_CONFIG_FILE']
    mount = next(row for row in container['volumeMounts'] if path.startswith(row['mountPath'] + '/'))
    assert mount['readOnly'] is True
    volume = next(row for row in pod['volumes'] if row['name'] == mount['name'])
    assert volume['configMap'] == {
        'name': 'loom-platform-config',
        'items': [{'key': 'task-egress.json', 'path': Path(path).name}],
    }
    service = next(row for row in files['40-services.yaml']
                   if row['kind'] == 'Deployment' and row['metadata']['name'] == 'loom-service')
    service_env = {row['name']: row.get('value')
                   for row in service['spec']['template']['spec']['containers'][0]['env']}
    assert json.loads(service_env['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON'])['supports_task_web_egress'] is True
    assert 'LOOM_GW_TASK_EGRESS_CONFIG_FILE' not in service_env
    for namespace in files['00-namespaces.yaml']:
        assert namespace['metadata']['labels']['pod-security.kubernetes.io/enforce'] == 'restricted'


@pytest.mark.parametrize('configured,ready', [(True, False), (False, True), (False, 'true')])
def test_task_egress_requires_matching_profile_readiness(
    platform_inputs: tuple, configured: bool, ready: object,
) -> None:
    config, candidate, profile = platform_inputs
    if configured:
        config['task_egress'] = {'protected_cidrs': ['198.51.100.0/24']}
    profile['supports_task_web_egress'] = ready
    with pytest.raises(NebiusPlatformError, match=r'task.*egress'):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


@pytest.mark.parametrize('policy', [None, {}, {'protected_cidrs': []},
    {'protected_cidrs': ['8.8.8.8/24']}, {'protected_cidrs': ['invalid']},
    {'protected_cidrs': ['198.51.100.0/24'], 'maximum_connections': 0},
    {'protected_cidrs': ['198.51.100.0/24'], 'maximum_connections_per_lease': 33},
    {'protected_cidrs': ['198.51.100.0/24'], 'allow_private': True}])
def test_task_egress_rejects_invalid_deployment_policy(platform_inputs: tuple, policy: object) -> None:
    config, candidate, profile = platform_inputs
    config['task_egress'] = policy
    profile['supports_task_web_egress'] = True
    with pytest.raises(ValueError):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_regional_task_egress_uses_existing_authenticated_broker_route(regional_inputs: tuple) -> None:
    config, candidate, profile = regional_inputs
    config['task_egress'] = {'protected_cidrs': [config['public_gateway_ipv4'] + '/32']}
    profile['supports_task_web_egress'] = True
    profile['execution_class_id'] = 'linux-amd64-cpu-web-pod-v1'
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    catalog = json.loads(files['10-config-network.yaml'][0]['data']['catalog.json'])
    assert all(t['execution_class_id'] == profile['execution_class_id']
               for t in catalog['topology']['targets'])
    caddy = json.loads(files['10-config-network.yaml'][0]['data']['public-tls.json'])
    routes = caddy['apps']['http']['servers']['public']['routes'][0]['handle'][-1]['routes']
    broker = routes[0]
    assert broker['match'] == [{'path': ['/internal/service-execution/*']}]
    assert broker['handle'][0]['upstreams'] == [{'dial': f"loom-llm-gateway.{config['namespace']}.svc:9100"}]
    assert broker['handle'][0]['flush_interval'] == -1


def test_task_egress_requires_known_public_platform_addresses_protected(regional_inputs: tuple) -> None:
    config, candidate, profile = regional_inputs
    config['task_egress'] = {'protected_cidrs': ['198.51.100.0/24']}
    profile['supports_task_web_egress'] = True
    with pytest.raises(NebiusPlatformError, match='protected_cidrs'):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_task_identity_readiness_cannot_relax_restricted_execution_policy(platform_inputs: tuple) -> None:
    config, candidate, profile = platform_inputs
    profile['supports_task_identity'] = True
    with pytest.raises(NebiusPlatformError, match=r'task identity.*restricted'):
        build_platform(config, candidate, profile, {}, repo_root=ROOT)


def test_closed_templates_refuse_inherited_task_identity_policy(platform_inputs: tuple) -> None:
    from loom.nebius_platform_render import _build_platform

    config, candidate, profile = platform_inputs
    config['task_identity_policy'] = {'mode': 'private-root-v1', 'target_id': config['target_id'],
                                      'execution_namespace': config['execution_namespace']}
    with pytest.raises(NebiusPlatformError, match='cannot inherit execution authority'):
        _build_platform(config, candidate, profile, {}, repo_root=ROOT, execution_enabled=False)


def test_node_share_profile_does_not_publish_retired_default_template(platform_inputs):
    config, candidate, profile = platform_inputs
    profile["resource_allocation_policy"] = "node-share-v1"
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    cm = next(doc for doc in files["10-config-network.yaml"]
              if doc["kind"] == "ConfigMap" and doc["metadata"]["name"] == "loom-platform-config")
    rendered = json.loads(cm["data"]["profile.json"])
    assert rendered["resource_allocation_policy"] == "node-share-v1"
    assert "default_task_resource_requests" not in rendered


def test_operator_renders_pinned_candidate_in_separate_process(platform_inputs, tmp_path, monkeypatch):
    """A later operator renderer cannot add flags unsupported by candidate images."""
    import subprocess
    from types import SimpleNamespace

    from scripts.ops import nebius_idle_rollout

    from loom import nebius_platform_render

    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    config, candidate, profile = platform_inputs
    candidate['candidate_sha'] = sha
    profile['candidate_sha'] = sha
    (tmp_path / 'candidate.json').write_text(json.dumps(candidate))
    (tmp_path / 'runtime-profile.json').write_text(json.dumps(profile))
    monkeypatch.setattr(nebius_platform_render, 'build_platform',
                        lambda *a, **kw: pytest.fail('must run the candidate renderer in its own process'))
    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(candidate=sha, publication_dir=Path('.'), render_dir=Path('rendered'))
    nebius_idle_rollout.render_candidate(args, config, '{}')
    import yaml
    docs = list(yaml.safe_load_all((args.render_dir / '10-config-network.yaml').read_text()))
    cm = next(doc for doc in docs if doc['kind'] == 'ConfigMap' and doc['metadata']['name'] == 'loom-platform-config')
    assert json.loads(cm['data']['profile.json'])['candidate_sha'] == sha
    assert (args.render_dir / '40-services.yaml').is_file()
