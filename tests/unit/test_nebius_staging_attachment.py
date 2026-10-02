from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from scripts.ops.render_nebius_runtime import main as render_main

from loom.nebius_runtime_render import NebiusRuntimeRenderError, render_nebius_runtime

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "registry.example/actuator@sha256:" + "a" * 64


def binding() -> dict:
    return {
        "schema_version": "loom.nebius-staging-attachment.v1",
        "environment": "staging",
        "target_id": "nebius-eu-north1-staging",
        "namespace": "loom-nebius-staging",
        "canonical_database": "loom_staging",
        "gateway_image": "registry.example/gateway@sha256:" + "b" * 64,
        "configuration_revision": "a" * 64,
        "local_providers_secret_name": "staging-local-providers",
        "canonical": {
            "endpoint": "https://canonical.example:9443",
            "region": "us-east-1",
            "artifacts_bucket": "loom-staging-artifacts",
            "trajectories_bucket": "loom-staging-trajectories",
            "db_secret": {
                "name": "staging-db",
                "gateway_key": "gw-url",
                "actuator_key": "actuator-url",
            },
            "storage_secret": {
                "name": "canonical-storage",
                "access_key": "access",
                "secret_key": "secret",
            },
        },
        "source": {
            "endpoint": "https://spool.example:9443",
            "region": "eu-north1",
            "bucket": "loom-staging-spool",
            "credentials_secret": {
                "name": "spool-storage",
                "access_key": "access",
                "secret_key": "secret",
            },
        },
        "gateway_secret": {
            "name": "staging-gateway",
            "step_jwt_key": "signing",
            "master_key": "master",
        },
        "collector": {
            "control_plane_url": "https://cp.example:8443",
            "token_secret": {"name": "staging-collector", "key": "token"},
            "nebius_secret": {"name": "nebius-observer", "key": "credentials.json"},
        },
        "network": {"kubelet": [{"cidr": "10.40.0.0/24", "port": 10250}]}
        | {
            name: [{"cidr": f"192.0.2.{index}/32", "port": port}]
            for index, (name, port) in enumerate(
                [
                    ("database", 5432),
                    ("canonical_store", 9443),
                    ("source_store", 9443),
                    ("control_plane", 8443),
                    ("kubernetes_api", 443),
                    ("provider_api", 443),
                    ("model_api", 443),
                ],
                1,
            )
        },
    }


def private_binding() -> dict:
    payload = binding()
    payload["private_entry"] = {"hostname": "staging.example", "address": "10.42.0.8"}
    payload["canonical"]["endpoint"] = "https://staging.example:19443"
    payload["collector"]["control_plane_url"] = "https://staging.example:18443"
    for name, port in (("database", 15432), ("canonical_store", 19443), ("control_plane", 18443)):
        payload["network"][name] = [{"cidr": "10.42.0.8/32", "port": port}]
    return payload


def render(tmp_path: Path, payload: dict, environment: str = "staging") -> tuple[dict, list[dict]]:
    attachment = tmp_path / "attachment.json"
    attachment.write_text(json.dumps(payload))
    capacity = json.loads((ROOT / "deploy/k8s/nebius-development-capacity-policy.json").read_text())
    capacity["schema_version"] = f"loom.nebius-{environment}-capacity.v1"
    capacity["target_id"] = f"nebius-eu-north1-{environment}"
    policy = tmp_path / "capacity.json"
    policy.write_text(json.dumps(capacity))
    manifest = render_nebius_runtime(
        repo_root=ROOT,
        environment=environment,
        image=IMAGE,
        topology_path=ROOT / "config/service-execution-topology.json",
        physical_binding_path=ROOT / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=policy,
        output_dir=tmp_path / "rendered",
        staging_attachment_path=attachment,
    )
    documents = []
    for item in manifest["files"]:
        if item["path"].endswith(".yaml"):
            documents.extend(yaml.safe_load_all((tmp_path / "rendered" / item["path"]).read_text()))
    return manifest, documents


def test_staging_attachment_separates_spool_without_second_control_plane(tmp_path: Path) -> None:
    manifest, docs = render(tmp_path, binding())
    assert not any("patch" in row["path"] for row in manifest["files"])
    assert not any(row["kind"] in {"Secret", "StatefulSet"} for row in docs)
    deployments = {row["metadata"]["name"]: row for row in docs if row["kind"] == "Deployment"}
    assert set(deployments) == {"loom-execution-actuator", "loom-llm-gateway"}
    assert all(
        row["metadata"]["namespace"] == "loom-nebius-staging" for row in deployments.values()
    )
    env = {
        row["name"]: row
        for row in deployments["loom-llm-gateway"]["spec"]["template"]["spec"]["containers"][0][
            "env"
        ]
    }
    assert env["LOOM_GW_MINIO_ENDPOINT"]["value"] == "https://canonical.example:9443"
    assert env["LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT"]["value"] == "https://spool.example:9443"
    assert env["LOOM_GW_DB_URL"]["valueFrom"]["secretKeyRef"] == {
        "name": "staging-db",
        "key": "gw-url",
    }
    assert (
        env["LOOM_GW_SERVICE_EXECUTION_SOURCE_ACCESS_KEY"]["valueFrom"]["secretKeyRef"]["name"]
        == "spool-storage"
    )
    actuator = deployments["loom-execution-actuator"]["spec"]["template"]["spec"]["containers"][0]
    actuator_env = {row["name"]: row for row in actuator["env"]}
    assert (
        actuator_env["LOOM_EXECUTION_ACTUATOR_CREDENTIAL_BROKER_URL"]["value"]
        == "http://loom-llm-gateway.loom-nebius-staging.svc.cluster.local:9100/internal/service-execution"
    )
    assert (
        actuator_env["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"]["secretKeyRef"]["name"]
        == "staging-db"
    )
    collector = next(row for row in docs if row["kind"] == "ConfigMap")
    assert (
        collector["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL"]
        == "https://cp.example:8443"
    )
    roles = [row for row in docs if row["kind"] in {"ClusterRole", "ClusterRoleBinding"}]
    assert {row["metadata"]["name"] for row in roles} == {
        "loom-execution-capacity-collector-staging",
        "loom-nebius-staging-actuator-usage",
    }
    usage_role = next(
        row
        for row in roles
        if row["kind"] == "ClusterRole"
        and row["metadata"]["name"] == "loom-nebius-staging-actuator-usage"
    )
    assert usage_role["rules"] == [
        {"apiGroups": [""], "resources": ["nodes", "nodes/stats"], "verbs": ["get"]}
    ]
    for role_binding in (row for row in roles if row["kind"] == "ClusterRoleBinding"):
        assert role_binding["roleRef"]["name"] == role_binding["metadata"]["name"]
        assert all(
            subject["namespace"] == "loom-nebius-staging" for subject in role_binding["subjects"]
        )


def test_staging_actuator_can_reach_only_configured_private_kubelet_destinations(
    tmp_path: Path,
) -> None:
    payload = binding()
    payload["network"]["kubelet"] = [
        {"cidr": "10.40.0.0/24", "port": 10250},
        {"cidr": "fd00:40::/64", "port": 10250},
    ]
    _, docs = render(tmp_path, payload)
    policies = {row["metadata"]["name"]: row for row in docs if row["kind"] == "NetworkPolicy"}
    routes = [
        row
        for row in policies["loom-attachment-actuator"]["spec"]["egress"]
        if row.get("ports") == [{"protocol": "TCP", "port": 10250}]
    ]
    assert routes == [
        {
            "to": [{"ipBlock": {"cidr": "10.40.0.0/24"}}],
            "ports": [{"protocol": "TCP", "port": 10250}],
        },
        {
            "to": [{"ipBlock": {"cidr": "fd00:40::/64"}}],
            "ports": [{"protocol": "TCP", "port": 10250}],
        },
    ]
    for name in ("loom-attachment-gateway", "loom-attachment-collector"):
        assert all(
            port["port"] != 10250
            for rule in policies[name]["spec"]["egress"]
            for port in rule["ports"]
        )


def test_staging_attachment_requires_explicit_kubelet_route_before_rendering(
    tmp_path: Path,
) -> None:
    payload = binding()
    payload["network"].pop("kubelet", None)
    with pytest.raises(NebiusRuntimeRenderError, match="kubelet"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "destinations",
    [
        [],
        [{"cidr": "0.0.0.0/0", "port": 10250}],
        [{"cidr": "8.8.8.8/32", "port": 10250}],
        [{"cidr": "127.0.0.1/32", "port": 10250}],
        [{"cidr": "169.254.0.0/16", "port": 10250}],
        [{"cidr": "::/0", "port": 10250}],
        [{"cidr": "fe80::/64", "port": 10250}],
        [{"cidr": "10.40.0.0/24", "port": 443}],
        [{"cidr": "10.40.0.0/24", "port": True}],
    ],
)
def test_staging_kubelet_route_cannot_widen_to_public_or_non_kubelet_endpoints(
    tmp_path: Path, destinations
) -> None:
    payload = binding()
    payload["network"]["kubelet"] = destinations
    with pytest.raises(NebiusRuntimeRenderError):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize("database", ["loom", "loom_staging"])
def test_attachment_preserves_existing_canonical_database_and_secret_refs(
    tmp_path: Path, database: str
) -> None:
    payload = binding()
    payload["canonical_database"] = database
    manifest, docs = render(tmp_path, payload)
    assert manifest["canonical_database"] == database
    assert manifest["source_sha256"]["staging_attachment"]
    assert not any(row["kind"] in {"Secret", "StatefulSet", "Cluster", "Job"} for row in docs)
    for row in docs:
        if row["kind"] != "Deployment":
            continue
        env = row["spec"]["template"]["spec"]["containers"][0]["env"]
        dsn_refs = [entry for entry in env if entry["name"].endswith("DB_URL")]
        assert len(dsn_refs) == 1
        assert "value" not in dsn_refs[0]
        assert dsn_refs[0]["valueFrom"]["secretKeyRef"]["name"] == "staging-db"


@pytest.mark.parametrize(
    "database", ["", "loom_development", "loom_production", "postgres", None, [], {}, "loom\n"]
)
def test_attachment_rejects_unknown_database_declarations(tmp_path: Path, database: object) -> None:
    payload = binding()
    payload["canonical_database"] = database
    with pytest.raises(NebiusRuntimeRenderError, match="staging target and database"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("environment", "development"),
        ("target_id", "nebius-eu-north1-development"),
        ("namespace", "loom-nebius-development"),
    ],
)
def test_shared_database_name_does_not_allow_cross_environment_attachment(
    tmp_path: Path, field: str, value: str
) -> None:
    payload = binding()
    payload["canonical_database"] = "loom"
    payload[field] = value
    with pytest.raises(NebiusRuntimeRenderError):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize("field", ["artifacts_bucket", "trajectories_bucket"])
def test_shared_database_name_keeps_canonical_bucket_fence(tmp_path: Path, field: str) -> None:
    payload = binding()
    payload["canonical_database"] = "loom"
    payload["canonical"][field] = "loom-development-artifacts"
    with pytest.raises(NebiusRuntimeRenderError, match="canonical buckets"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda x: x.update(environment="production"),
        lambda x: x.update(canonical_database="loom_development"),
        lambda x: x.update(namespace="loom-nebius-development"),
        lambda x: x.update(gateway_image="registry.example/gateway:latest"),
        lambda x: x.update(configuration_revision=""),
        lambda x: x.update(configuration_revision="A" * 64),
        lambda x: x.pop("configuration_revision"),
        lambda x: x.update(local_providers_secret_name=""),
        lambda x: x.pop("local_providers_secret_name"),
        lambda x: x["canonical"].update(endpoint="https://user:password@canonical.example"),
        lambda x: x["source"].update(endpoint="https://spool.example?token=secret"),
        lambda x: x["canonical"]["db_secret"].update(password="secret"),
        lambda x: x["network"].update(database=[{"cidr": "0.0.0.0/0", "port": 5432}]),
        lambda x: x["network"].update(database=[]),
        lambda x: x["canonical"].update(endpoint="https://192.0.2.99:9443"),
        lambda x: x["collector"].update(control_plane_url="https://cp.example:9999"),
    ],
)
def test_attachment_rejects_unsafe_or_cross_environment_binding(tmp_path: Path, mutation) -> None:
    payload = binding()
    mutation(payload)
    with pytest.raises(NebiusRuntimeRenderError):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


def test_attachment_cannot_change_development(tmp_path: Path) -> None:
    with pytest.raises(NebiusRuntimeRenderError, match="staging"):
        render(tmp_path, binding(), environment="development")


def test_configuration_revision_rolls_consumers_for_same_name_secret_rotation(
    tmp_path: Path,
) -> None:
    before = tmp_path / "before"
    after = tmp_path / "after"
    before.mkdir()
    after.mkdir()
    payload = binding()
    _, old_docs = render(before, payload)
    payload["configuration_revision"] = "c" * 64
    _, new_docs = render(after, payload)
    key = "loom.ca/nebius-configuration-revision"
    for name in (
        "loom-llm-gateway",
        "loom-execution-actuator",
        "loom-execution-capacity-collector",
    ):
        old = next(
            row
            for row in old_docs
            if row["kind"] in {"Deployment", "CronJob"} and row["metadata"]["name"] == name
        )
        new = next(
            row
            for row in new_docs
            if row["kind"] == old["kind"] and row["metadata"]["name"] == name
        )
        if old["kind"] == "CronJob":
            assert old["spec"]["jobTemplate"]["metadata"]["annotations"][key] == "a" * 64
            assert new["spec"]["jobTemplate"]["metadata"]["annotations"][key] == "c" * 64
            old, new = old["spec"]["jobTemplate"], new["spec"]["jobTemplate"]
        assert old["spec"]["template"]["metadata"]["annotations"][key] == "a" * 64
        assert new["spec"]["template"]["metadata"]["annotations"][key] == "c" * 64
        assert old["spec"]["template"] != new["spec"]["template"]
        assert old["spec"]["template"]["spec"] == new["spec"]["template"]["spec"]


def test_attachment_network_policies_scope_each_component(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    policies = {
        row["metadata"]["name"]: row["spec"] for row in docs if row["kind"] == "NetworkPolicy"
    }
    assert all(spec["podSelector"] for spec in policies.values())
    gateway = policies["loom-attachment-gateway"]
    assert gateway["ingress"] == [
        {
            "from": [
                {"podSelector": {"matchLabels": {"app.kubernetes.io/component": "execution-unit"}}}
            ],
            "ports": [{"protocol": "TCP", "port": 9100}],
        }
    ]
    assert policies["loom-execution-attempt-egress"]["egress"][-1]["to"] == [
        {"podSelector": {"matchLabels": {"app": "loom-llm-gateway"}}}
    ]
    for name, expected in {
        "loom-attachment-gateway": {"192.0.2.1/32", "192.0.2.2/32", "192.0.2.3/32", "192.0.2.7/32"},
        "loom-attachment-actuator": {"192.0.2.1/32", "192.0.2.5/32", "10.40.0.0/24"},
        "loom-attachment-collector": {"192.0.2.4/32", "192.0.2.5/32", "192.0.2.6/32"},
    }.items():
        actual = {
            peer["ipBlock"]["cidr"]
            for rule in policies[name]["egress"]
            for peer in rule["to"]
            if "ipBlock" in peer
        }
        assert actual == expected


def test_gateway_rollout_preserves_long_request_drain(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    gateway = next(
        row
        for row in docs
        if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-llm-gateway"
    )
    pod = gateway["spec"]["template"]["spec"]
    assert pod["terminationGracePeriodSeconds"] == 300
    command = pod["containers"][0]["lifecycle"]["preStop"]["exec"]["command"]
    assert command[:2] == ["python", "-c"]
    assert "http://127.0.0.1:9100/drain" in command[2]
    assert "method='POST'" in command[2]
    assert "timeout=280" in command[2]
    compile(command[2], "<rendered gateway preStop>", "exec")


def test_gateway_local_provider_secret_does_not_replace_explicit_identity(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    gateway = next(
        row
        for row in docs
        if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-llm-gateway"
    )
    container = gateway["spec"]["template"]["spec"]["containers"][0]
    assert container["envFrom"] == [{"secretRef": {"name": "staging-local-providers"}}]
    explicit = {entry["name"] for entry in container["env"]}
    assert {
        "LOOM_ENV",
        "LOOM_NAMESPACE",
        "LOOM_GW_DB_URL",
        "LOOM_GW_STEP_JWT_SIGNING_KEY",
        "LOOM_SECRET_STORE_MASTER_KEY",
        "LOOM_GW_MINIO_ENDPOINT",
        "LOOM_GW_MINIO_ACCESS_KEY",
        "LOOM_GW_MINIO_SECRET_KEY",
        "LOOM_GW_ARTIFACTS_BUCKET",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_BUCKET",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_ACCESS_KEY",
        "LOOM_GW_SERVICE_EXECUTION_SOURCE_SECRET_KEY",
    } <= explicit


def test_default_development_render_retains_template_bytes(tmp_path: Path) -> None:
    output = tmp_path / "default"
    manifest = render_nebius_runtime(
        repo_root=ROOT,
        environment="development",
        image=IMAGE,
        topology_path=ROOT / "config/service-execution-topology.json",
        physical_binding_path=ROOT / "config/nebius-runtime-physical-binding.json",
        capacity_policy_path=ROOT / "deploy/k8s/nebius-development-capacity-policy.json",
        output_dir=output,
    )
    assert "staging_attachment" not in manifest["source_sha256"]
    for entry in manifest["files"]:
        path = entry["path"]
        if "patch.yaml" in path:
            assert (output / path).read_bytes() == (ROOT / "deploy/k8s" / path).read_bytes()
    assert not (output / "nebius-staging-gateway.yaml").exists()


def test_cli_attachment_render_and_rejection_are_offline(tmp_path: Path, capsys) -> None:
    render(tmp_path, binding())
    argv = [
        "--environment",
        "staging",
        "--image",
        IMAGE,
        "--capacity-policy",
        str(tmp_path / "capacity.json"),
        "--staging-attachment",
        str(tmp_path / "attachment.json"),
        "--output",
        str(tmp_path / "cli"),
    ]
    assert render_main(argv) == 0
    assert "target: nebius-eu-north1-staging" in capsys.readouterr().out
    payload = binding()
    payload["source"]["endpoint"] = "https://user:must-not-leak@spool.example"
    (tmp_path / "attachment.json").write_text(json.dumps(payload))
    argv[-1] = str(tmp_path / "rejected")
    assert render_main(argv) == 1
    stderr = capsys.readouterr().err
    assert "must-not-leak" not in stderr
    assert not (tmp_path / "rejected").exists()


def test_private_entry_cli_persists_all_consumer_aliases_without_rewriting_identity(
    tmp_path: Path, capsys
) -> None:
    payload = private_binding()
    manifest, _ = render(tmp_path, payload)
    output = tmp_path / "cli"
    assert (
        render_main(
            [
                "--environment",
                "staging",
                "--image",
                IMAGE,
                "--capacity-policy",
                str(tmp_path / "capacity.json"),
                "--staging-attachment",
                str(tmp_path / "attachment.json"),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert "target: nebius-eu-north1-staging" in capsys.readouterr().out
    assert manifest["source_sha256"]["staging_attachment"]
    docs = [doc for path in output.glob("*.yaml") for doc in yaml.safe_load_all(path.read_text())]
    consumers = [doc for doc in docs if doc["kind"] in {"Deployment", "CronJob"}]
    assert len(consumers) == 3
    for doc in consumers:
        template = (
            doc["spec"] if doc["kind"] == "Deployment" else doc["spec"]["jobTemplate"]["spec"]
        )
        assert template["template"]["spec"]["hostAliases"] == [
            {"ip": "10.42.0.8", "hostnames": ["staging.example"]}
        ]
        if doc["kind"] == "Deployment":
            env = {
                item["name"]: item for item in template["template"]["spec"]["containers"][0]["env"]
            }
            db = next(item for name, item in env.items() if name.endswith("DB_URL"))
            assert "value" not in db
            assert db["valueFrom"]["secretKeyRef"]["name"] == "staging-db"
            if doc["metadata"]["name"] == "loom-llm-gateway":
                assert env["LOOM_GW_MINIO_ENDPOINT"]["value"] == payload["canonical"]["endpoint"]
                assert (
                    env["LOOM_GW_SERVICE_EXECUTION_SOURCE_ENDPOINT"]["value"]
                    == payload["source"]["endpoint"]
                )
    collector = next(doc for doc in docs if doc["kind"] == "ConfigMap")
    assert (
        collector["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_CONTROL_PLANE_URL"]
        == payload["collector"]["control_plane_url"]
    )


def test_private_entry_omission_keeps_legacy_render_without_aliases(tmp_path: Path) -> None:
    _, docs = render(tmp_path, binding())
    assert "hostAliases" not in yaml.safe_dump_all(docs)


def test_private_entry_does_not_redirect_independent_spool(tmp_path: Path) -> None:
    payload = private_binding()
    payload["source"]["endpoint"] = "https://staging.example:9443"
    payload["network"]["source_store"] = [{"cidr": "192.0.2.3/32", "port": 9443}]
    with pytest.raises(NebiusRuntimeRenderError, match="independent spool host"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "address",
    [
        "8.8.8.8",
        "192.0.2.8",
        "127.0.0.1",
        "169.254.1.1",
        "100.64.0.1",
        "::1",
        "fd00::1",
        "10.42.0.8/32",
        "010.42.0.8",
        "staging.example",
        "",
        None,
    ],
)
def test_private_entry_rejects_non_rfc1918_ipv4(tmp_path: Path, address: object) -> None:
    payload = private_binding()
    payload["private_entry"]["address"] = address
    with pytest.raises(NebiusRuntimeRenderError, match="private entry"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "hostname",
    [
        "10.42.0.8",
        "127.0.0.1",
        "::1",
        "localhost",
        "staging.example.",
        "bad..example",
        "-bad.example",
        "bad-.example",
        "bad_name.example",
        "https://staging.example",
        "staging.example:19443",
        "*.example",
        "x" * 64 + ".example",
        "staging.example\n",
        "",
        None,
    ],
)
def test_private_entry_rejects_invalid_dns_hostname(tmp_path: Path, hostname: object) -> None:
    payload = private_binding()
    payload["private_entry"]["hostname"] = hostname
    with pytest.raises(NebiusRuntimeRenderError, match="private entry"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "section,field,endpoint",
    [
        ("canonical", "endpoint", "https://other.example:19443"),
        ("canonical", "endpoint", "https://10.42.0.8:19443"),
        ("canonical", "endpoint", "https://staging.example:9443"),
        ("collector", "control_plane_url", "https://other.example:18443"),
        ("collector", "control_plane_url", "https://10.42.0.8:18443"),
        ("collector", "control_plane_url", "https://staging.example"),
    ],
)
def test_private_entry_requires_exact_tls_origins(
    tmp_path: Path, section: str, field: str, endpoint: str
) -> None:
    payload = private_binding()
    payload[section][field] = endpoint
    with pytest.raises(NebiusRuntimeRenderError):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize("name", ["database", "canonical_store", "control_plane"])
@pytest.mark.parametrize("change", ["address", "port", "uncoupled", "ipv6"])
def test_private_entry_requires_matching_address_and_port_rule(
    tmp_path: Path, name: str, change: str
) -> None:
    payload = private_binding()
    port = payload["network"][name][0]["port"]
    payload["network"][name] = {
        "address": [{"cidr": "10.42.0.9/32", "port": port}],
        "port": [{"cidr": "10.42.0.8/32", "port": port + 1}],
        "uncoupled": [
            {"cidr": "10.42.0.8/32", "port": port + 1},
            {"cidr": "10.42.0.9/32", "port": port},
        ],
        "ipv6": [{"cidr": "fd00::/64", "port": port}],
    }[change]
    with pytest.raises(NebiusRuntimeRenderError):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


@pytest.mark.parametrize(
    "cidr,address",
    [
        ("10.42.0.0/24", "10.42.0.8"),
        ("172.16.0.0/12", "172.31.255.254"),
        ("192.168.0.0/16", "192.168.42.8"),
    ],
)
def test_private_entry_accepts_rfc1918_routed_ranges(
    tmp_path: Path, cidr: str, address: str
) -> None:
    payload = private_binding()
    payload["private_entry"]["address"] = address
    for name in ("database", "canonical_store", "control_plane"):
        payload["network"][name][0]["cidr"] = cidr
    render(tmp_path, payload)


@pytest.mark.parametrize(
    "entry",
    [
        None,
        {},
        [],
        {"hostname": "staging.example"},
        {"hostname": "staging.example", "address": "10.42.0.8", "aliases": ["other.example"]},
    ],
)
def test_private_entry_rejects_incomplete_or_generic_dns_override(
    tmp_path: Path, entry: object
) -> None:
    payload = private_binding()
    payload["private_entry"] = entry
    with pytest.raises(NebiusRuntimeRenderError, match="private entry"):
        render(tmp_path, payload)
    assert not (tmp_path / "rendered").exists()


def database_tls_binding() -> dict:
    payload = private_binding()
    payload["database_tls"] = {
        "server_name": "loom-postgres-rw.loom-staging.svc.cluster.local",
        "ca_secret": {"name": "loom-staging-db-ca", "key": "server-ca.crt"},
    }
    return payload


def test_database_tls_mounts_renewable_ca_and_native_name_only_for_db_consumers(
    tmp_path: Path,
) -> None:
    payload = database_tls_binding()
    _, docs = render(tmp_path, payload)
    for doc in docs:
        if doc["kind"] not in {"Deployment", "CronJob"}:
            continue
        spec = doc["spec"] if doc["kind"] == "Deployment" else doc["spec"]["jobTemplate"]["spec"]
        pod = spec["template"]["spec"]
        container = pod["containers"][0]
        env = {item["name"]: item for item in container.get("env", [])}
        if doc["kind"] == "CronJob":
            assert pod["hostAliases"] == [{"ip": "10.42.0.8", "hostnames": ["staging.example"]}]
            assert not {"PGSSLMODE", "PGSSLROOTCERT"} & env.keys()
            assert "loom-postgres-ca" not in {item["name"] for item in pod.get("volumes", [])}
            continue
        assert pod["hostAliases"] == [
            {"ip": "10.42.0.8", "hostnames": ["staging.example"]},
            {"ip": "10.42.0.8", "hostnames": [payload["database_tls"]["server_name"]]},
        ]
        assert env["PGSSLMODE"]["value"] == "verify-full"
        assert env["PGSSLROOTCERT"]["value"] == "/var/run/loom/postgres-tls/ca.crt"
        ca = next(item for item in pod["volumes"] if item["name"] == "loom-postgres-ca")
        assert ca["secret"] == {
            "secretName": "loom-staging-db-ca",
            "defaultMode": 0o444,
            "items": [{"key": "server-ca.crt", "path": "ca.crt"}],
        }
        mount = next(item for item in container["volumeMounts"] if item["name"] == ca["name"])
        assert mount == {
            "name": "loom-postgres-ca",
            "mountPath": "/var/run/loom/postgres-tls",
            "readOnly": True,
        }
        # The public CA is world-readable, including the non-root runtime UID.
        assert ca["secret"]["defaultMode"] & 0o004
        assert "value" not in next(item for item in env.values() if item["name"].endswith("DB_URL"))
    assert not any(doc["kind"] == "Secret" for doc in docs)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda x: x.pop("private_entry"),
        lambda x: x.update(database_tls=None),
        lambda x: x["database_tls"].update(password="never-emit-this"),
        lambda x: x["database_tls"].pop("ca_secret"),
        lambda x: x["database_tls"].update(server_name="staging.example"),
        lambda x: x["database_tls"].update(server_name="spool.example"),
        lambda x: x["database_tls"].update(server_name="10.42.0.8"),
        lambda x: x["database_tls"].update(server_name="localhost"),
        lambda x: x["database_tls"].update(server_name="db.example."),
        lambda x: x["database_tls"].update(server_name="bad_name.example"),
        lambda x: x["database_tls"].update(server_name="db.example:15432"),
        lambda x: x["database_tls"].update(server_name="db.example\n"),
        lambda x: x["database_tls"].update(server_name=None),
        lambda x: x["database_tls"]["ca_secret"].update(key="../../ca.crt"),
        lambda x: x["database_tls"]["ca_secret"].update(name=""),
        lambda x: x["database_tls"]["ca_secret"].update(certificate="never-emit-this"),
    ],
)
def test_database_tls_rejects_incomplete_unsafe_or_colliding_identity(
    tmp_path: Path, mutation
) -> None:
    payload = database_tls_binding()
    mutation(payload)
    with pytest.raises(NebiusRuntimeRenderError) as error:
        render(tmp_path, payload)
    assert "never-emit-this" not in str(error.value)
    assert not (tmp_path / "rendered").exists()


def test_database_tls_omission_does_not_add_ca_mount_or_libpq_environment(tmp_path: Path) -> None:
    _, docs = render(tmp_path, private_binding())
    serialized = yaml.safe_dump_all(docs)
    assert "PGSSLMODE" not in serialized
    assert "PGSSLROOTCERT" not in serialized
    assert "loom-postgres-ca" not in serialized
