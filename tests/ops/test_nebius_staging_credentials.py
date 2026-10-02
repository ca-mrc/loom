"""Credential mapping, durable retries, and protected reconciliation boundaries."""

from __future__ import annotations

import base64
import copy
import datetime
import json
import os
import ssl
import stat
import subprocess
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID
from scripts.ops import nebius_staging_credentials as credentials
from tests.support.execution_image_admission import signed_image_admission_bundle

CONFIG_REVISION = "a" * 64
PASSWORD = "secret:/@?#%& + unicode-密码"


def encoded(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def attachment() -> dict:
    return {
        "schema_version": "loom.nebius-staging-attachment.v1",
        "environment": "staging",
        "target_id": "nebius-eu-north1-staging",
        "namespace": credentials.DESTINATION,
        "canonical_database": "loom",
        "gateway_image": "registry.example/gateway@sha256:" + "b" * 64,
        "configuration_revision": CONFIG_REVISION,
        "local_providers_secret_name": "staging-local-providers",
        "private_entry": {"hostname": "staging.example", "address": "10.42.0.8"},
        "database_tls": {
            "server_name": "loom-postgres-rw.loom-staging.svc.cluster.local",
            "ca_secret": {"name": "staging-db-ca", "key": "server-ca.crt"},
        },
        "canonical": {
            "endpoint": "https://staging.example:19443",
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
            "endpoint": "https://storage.eu-north1.nebius.cloud",
            "region": "eu-north1",
            "bucket": "loom-test-staging-spool",
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
            "control_plane_url": "https://staging.example:18443",
            "token_secret": {"name": "staging-collector", "key": "token"},
            "nebius_secret": {"name": "nebius-observer", "key": "credentials.json"},
        },
        "network": {
            "kubelet": [{"cidr": "10.40.0.0/24", "port": 10250}],
            **{
                name: [{"cidr": "10.42.0.8/32", "port": port}]
                for name, port in (
                    ("database", 15432),
                    ("canonical_store", 19443),
                    ("control_plane", 18443),
                )
            },
            **{
                name: [{"cidr": "192.0.2.1/32", "port": 443}]
                for name in (
                    "source_store",
                    "kubernetes_api",
                    "provider_api",
                    "model_api",
                )
            },
        },
    }


def ca_pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Disposable Loom test CA")])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM).decode()


def workload(name: str, namespace: str, *, cronjob: bool = False) -> dict:
    template = {
        "metadata": {"annotations": {"loom.ca/nebius-configuration-revision": CONFIG_REVISION}}
    }
    return {
        "metadata": {"name": name, "namespace": namespace, "generation": 1, "resourceVersion": "1"},
        "spec": {"jobTemplate": {"spec": {"template": template}}}
        if cronjob
        else {"replicas": 1, "template": template},
        "status": {
            "observedGeneration": 1,
            "replicas": 1,
            "updatedReplicas": 1,
            "readyReplicas": 1,
            "availableReplicas": 1,
        },
    }


class FakeKube(credentials.Kubernetes):
    """In-memory API transport; production Secret ownership/readback stays real."""

    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self.objects: dict[tuple[str, str], dict] = {}
        self.env = {
            "LOOM_GW_STEP_JWT_SIGNING_KEY": "running-signing-secret",
            "LOOM_SECRET_STORE_MASTER_KEY": "running-master-secret",
            "LOOM_GW_LOCAL_YIBU_BASE_URL": "https://provider.example/v1",
            "LOOM_GW_LOCAL_YIBU_API_KEY": "running-provider-secret",
        }
        self.puts: list[str] = []
        self.writes: list[tuple[str, str]] = []
        self.patches: list[tuple[str, str]] = []
        self.fail_once_name: str | None = None
        self.change_gateway_during_exec = False

    def add_secret(self, name: str, data: dict[str, str], *, managed: bool = True) -> None:
        self.objects["secret", name] = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "resourceVersion": "1",
                "labels": dict(credentials.OWNER) if managed else {},
            },
            "data": {k: encoded(v) for k, v in data.items()},
        }

    def get(self, kind: str, name: str, *, namespaced: bool = True) -> dict | None:
        return copy.deepcopy(self.objects.get((kind, name)))

    def put(self, name: str, values: dict[str, str]) -> bool:
        self.puts.append(name)
        if self.fail_once_name == name:
            self.fail_once_name = None
            raise credentials.ReconcileError("simulated destination conflict")
        return super().put(name, values)

    def command(self, args: list[str], payload: bytes | None = None) -> bytes:
        if args[0] == "exec":
            assert args[1] == "deployment/loom-llm-gateway"
            if self.change_gateway_during_exec:
                self.objects["deployment", "loom-llm-gateway"]["metadata"]["resourceVersion"] = "2"
            return json.dumps(self.env).encode()
        assert payload is not None
        document = json.loads(payload)
        if args[0] in {"create", "replace"}:
            name = document["metadata"]["name"]
            assert document["metadata"]["namespace"] == self.namespace
            self.objects["secret", name] = copy.deepcopy(document)
            self.writes.append((args[0], name))
            return b"{}"
        assert args[:1] == ["patch"]
        kind, name = args[1:3]
        obj = self.objects[kind, name]
        assert document[0] == {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": obj["metadata"]["resourceVersion"],
        }
        target = obj
        parts = document[1]["path"].strip("/").split("/")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = copy.deepcopy(document[1]["value"])
        obj["metadata"]["resourceVersion"] = str(int(obj["metadata"]["resourceVersion"]) + 1)
        self.patches.append((kind, name))
        return b"{}"


@pytest.fixture
def source() -> FakeKube:
    kube = FakeKube(credentials.SOURCE)
    for consumer in ("gateway", "actuator"):
        kube.add_secret(
            f"loom-nebius-staging-db-{consumer}",
            {
                "username": f"loom_nebius_staging_{consumer}",
                "password": PASSWORD,
            },
        )
    kube.add_secret("loom-postgres-ca", {"ca.crt": ca_pem()})
    binding = attachment()
    kube.add_secret(
        "loom-nebius-staging-spool",
        {
            **{key: binding["source"][key] for key in ("endpoint", "region", "bucket")},
            "access-key": "spool-only-access",
            "secret-key": "spool-only-secret",
        },
    )
    kube.add_secret(
        "loom-nebius-staging-canonical-inputs",
        {"access-key": "canonical-only-access", "secret-key": "canonical-only-secret"},
    )
    kube.add_secret("loom-nebius-staging-collector", {"token": "loom_ecc_" + "c" * 64})
    kube.add_secret(
        "loom-nebius-staging-observer",
        {"credentials.json": '{"subject-credentials":{"alg":"RS256"}}'},
    )
    kube.add_secret(
        "loom-secrets",
        {
            "step-jwt-signing-key": "unapplied-signing-secret",
            "secret-store-master-key": "unapplied-master-secret",
            "local-yibu-api-key": "unapplied-provider-secret",
        },
    )
    for name in ("loom-llm-gateway", "loom-control-plane"):
        kube.objects["deployment", name] = workload(name, credentials.SOURCE)
    return kube


@pytest.fixture
def destination() -> FakeKube:
    kube = FakeKube(credentials.DESTINATION)
    for kind, name in (
        ("deployment", "loom-llm-gateway"),
        ("deployment", "loom-execution-actuator"),
        ("cronjob", "loom-execution-capacity-collector"),
    ):
        kube.objects[kind, name] = workload(
            name, credentials.DESTINATION, cronjob=kind == "cronjob"
        )
    return kube


def test_source_mapping_uses_native_database_tls_and_running_gateway(
    source: FakeKube, tmp_path: Path
) -> None:
    payloads = credentials.source_payloads(source, attachment())
    for consumer, key in (("gateway", "gw-url"), ("actuator", "actuator-url")):
        url = urlsplit(payloads["staging-db"][key])
        assert url.scheme == "postgresql+psycopg"
        assert unquote(url.username or "") == f"loom_nebius_staging_{consumer}"
        assert unquote(url.password or "") == PASSWORD
        assert url.hostname == "loom-postgres-rw.loom-staging.svc.cluster.local"
        assert url.port == 15432 and url.path == "/loom"
        assert parse_qs(url.query) == {
            "sslmode": ["verify-full"],
            "sslrootcert": [credentials.CA_PATH],
        }
    ca_path = tmp_path / "server-ca.crt"
    ca_path.write_text(payloads["staging-db-ca"]["server-ca.crt"])
    context = ssl.create_default_context(cafile=str(ca_path))
    assert context.cert_store_stats()["x509_ca"] == 1
    assert payloads["spool-storage"] == {
        "access": "spool-only-access",
        "secret": "spool-only-secret",
    }
    assert payloads["canonical-storage"] == {
        "access": "canonical-only-access",
        "secret": "canonical-only-secret",
    }
    assert payloads["staging-gateway"] == {
        "signing": "running-signing-secret",
        "master": "running-master-secret",
    }
    assert payloads["staging-local-providers"] == {
        "LOOM_GW_LOCAL_YIBU_BASE_URL": "https://provider.example/v1",
        "LOOM_GW_LOCAL_YIBU_API_KEY": "running-provider-secret",
    }
    assert payloads["staging-collector"]["token"] == "loom_ecc_" + "c" * 64
    assert (
        json.loads(payloads["nebius-observer"]["credentials.json"])["subject-credentials"]["alg"]
        == "RS256"
    )
    assert "unapplied-" not in json.dumps(payloads)
    assert source.puts == []


def test_reconcile_idempotent_without_reminting_credentials(
    source: FakeKube, destination: FakeKube
) -> None:
    first = credentials.reconcile(source, destination, attachment())
    snapshot = copy.deepcopy(destination.objects)
    assert first == {
        "secrets_changed": 8,
        "workloads_reconciled": 3,
        "canonical_workloads_changed": False,
    }
    assert credentials.reconcile(source, destination, attachment()) == {
        "secrets_changed": 0,
        "workloads_reconciled": 0,
        "canonical_workloads_changed": False,
    }
    assert destination.objects == snapshot
    assert source.puts == [] and source.writes == [] and source.patches == []


def test_rotated_ca_updates_only_ca_secret_and_rolls_consumers(
    source: FakeKube, destination: FakeKube
) -> None:
    credentials.reconcile(source, destination, attachment())
    before = copy.deepcopy(destination.objects)
    source.add_secret("loom-postgres-ca", {"ca.crt": ca_pem()})
    result = credentials.reconcile(source, destination, attachment())
    assert result["secrets_changed"] == 1 and result["workloads_reconciled"] == 3
    assert destination.objects["secret", "staging-db"] == before["secret", "staging-db"]
    assert destination.objects["secret", "staging-db-ca"] != before["secret", "staging-db-ca"]
    assert credentials.reconcile(source, destination, attachment())["workloads_reconciled"] == 0


def test_partial_destination_failure_recovers_on_next_pass(
    source: FakeKube, destination: FakeKube
) -> None:
    destination.fail_once_name = "spool-storage"
    with pytest.raises(credentials.ReconcileError, match="simulated destination conflict"):
        credentials.reconcile(source, destination, attachment())
    assert destination.writes and destination.patches == []
    first_db = copy.deepcopy(destination.objects["secret", "staging-db"])
    result = credentials.reconcile(source, destination, attachment())
    assert result["secrets_changed"] == 6 and result["workloads_reconciled"] == 3
    assert destination.objects["secret", "staging-db"] == first_db
    assert credentials.reconcile(source, destination, attachment())["secrets_changed"] == 0


@pytest.mark.parametrize(
    "kind,name",
    [
        ("deployment", "loom-llm-gateway"),
        ("deployment", "loom-execution-actuator"),
        ("cronjob", "loom-execution-capacity-collector"),
    ],
)
def test_stale_workload_revision_refuses_before_any_secret_put(
    source: FakeKube, destination: FakeKube, kind: str, name: str
) -> None:
    obj = destination.objects[kind, name]
    template = (
        obj["spec"]["jobTemplate"]["spec"]["template"]
        if kind == "cronjob"
        else obj["spec"]["template"]
    )
    template["metadata"]["annotations"]["loom.ca/nebius-configuration-revision"] = "f" * 64
    with pytest.raises(credentials.ReconcileError, match="approved attachment revision"):
        credentials.reconcile(source, destination, attachment())
    assert destination.puts == [] and destination.writes == [] and destination.patches == []


def test_unmanaged_secret_is_never_overwritten(source: FakeKube, destination: FakeKube) -> None:
    destination.add_secret("staging-db", {"gw-url": "someone-elses-secret"}, managed=False)
    before = copy.deepcopy(destination.objects)
    with pytest.raises(credentials.ReconcileError, match="unmanaged Secret"):
        credentials.reconcile(source, destination, attachment())
    assert destination.objects == before and destination.writes == [] and destination.patches == []


@pytest.mark.parametrize("revision", [None, "f" * 64, "missing-deployment"])
def test_canonical_control_plane_requires_approved_revision_before_remote_writes(
    source: FakeKube, destination: FakeKube, revision: str | None
) -> None:
    cp = source.objects["deployment", "loom-control-plane"]
    annotations = cp["spec"]["template"]["metadata"]["annotations"]
    if revision == "missing-deployment":
        source.objects.pop(("deployment", "loom-control-plane"))
    elif revision is None:
        annotations.pop("loom.ca/nebius-configuration-revision")
    else:
        annotations["loom.ca/nebius-configuration-revision"] = revision
    with pytest.raises(credentials.ReconcileError, match=r"canonical Control Plane.*approved"):
        credentials.reconcile(source, destination, attachment())
    assert destination.puts == [] and destination.patches == []


@pytest.mark.parametrize(
    "mutation",
    [
        lambda kube: kube.add_secret(
            "loom-postgres-ca", {"ca.crt": "private-ca-invalid-never-print"}
        ),
        lambda kube: kube.add_secret(
            "loom-nebius-staging-db-gateway", {"username": "wrong-role", "password": PASSWORD}
        ),
        lambda kube: kube.add_secret(
            "loom-nebius-staging-collector", {"token": "private-token-invalid-never-print"}
        ),
        lambda kube: kube.add_secret(
            "loom-nebius-staging-spool",
            {
                "endpoint": "https://other.example",
                "region": "eu-north1",
                "bucket": "wrong",
                "access-key": "private-key-never-print",
                "secret-key": PASSWORD,
            },
        ),
        lambda kube: kube.env.update(LOOM_GW_LOCAL_UNSUPPORTED="private-provider-never-print"),
    ],
)
def test_invalid_canonical_sources_fail_without_destination_write(
    source: FakeKube, destination: FakeKube, mutation
) -> None:
    mutation(source)
    with pytest.raises(credentials.ReconcileError) as caught:
        credentials.reconcile(source, destination, attachment())
    assert PASSWORD not in str(caught.value) and "never-print" not in str(caught.value)
    assert destination.puts == [] and destination.patches == []


@pytest.mark.parametrize(
    "field,value", [("observedGeneration", 0), ("updatedReplicas", 0), ("availableReplicas", 0)]
)
def test_unconverged_running_gateway_not_read_as_authority(
    source: FakeKube, destination: FakeKube, field: str, value: int
) -> None:
    source.objects["deployment", "loom-llm-gateway"]["status"][field] = value
    with pytest.raises(credentials.ReconcileError):
        credentials.reconcile(source, destination, attachment())
    assert destination.puts == []


def test_gateway_changed_during_env_snapshot_refuses_write(
    source: FakeKube, destination: FakeKube
) -> None:
    source.change_gateway_during_exec = True
    with pytest.raises(credentials.ReconcileError, match="changed during snapshot"):
        credentials.reconcile(source, destination, attachment())
    assert destination.puts == []


def test_changed_source_between_snapshots_defers_all_rollouts(
    source: FakeKube, destination: FakeKube, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = source.command
    calls = 0

    def changing_environment(args: list[str], payload: bytes | None = None) -> bytes:
        nonlocal calls
        result = original(args, payload)
        calls += 1
        if calls == 1:
            source.env["LOOM_GW_LOCAL_YIBU_API_KEY"] = "new-running-provider-secret"
        return result

    monkeypatch.setattr(source, "command", changing_environment)
    with pytest.raises(credentials.ReconcileError, match="changed during reconciliation"):
        credentials.reconcile(source, destination, attachment())
    assert destination.writes and destination.patches == []
    result = credentials.reconcile(source, destination, attachment())
    assert result["secrets_changed"] == 1 and result["workloads_reconciled"] == 3
    assert credentials.reconcile(source, destination, attachment())["secrets_changed"] == 0


def test_initial_secret_preparation_does_not_create_workloads(source: FakeKube) -> None:
    destination = FakeKube(credentials.DESTINATION)
    result = credentials.reconcile(source, destination, attachment())
    assert result["secrets_changed"] == 8 and result["workloads_reconciled"] == 0
    assert all(kind == "secret" for kind, _ in destination.objects)


@pytest.mark.parametrize(
    "field",
    ["observedGeneration", "updatedReplicas", "readyReplicas", "availableReplicas", "replicas"],
)
def test_unconverged_canonical_control_plane_does_not_write_secrets(
    source: FakeKube, destination: FakeKube, field: str
) -> None:
    source.objects["deployment", "loom-control-plane"]["status"][field] = 0
    with pytest.raises(credentials.ReconcileError, match="Control Plane rollout"):
        credentials.reconcile(source, destination, attachment())
    assert destination.puts == []


def test_unexpected_parser_error_cannot_print_secret_input(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "sys.argv", ["credentials", "reconcile", "--config", "/protected/config.json"]
    )

    def invalid_yaml(path: Path) -> dict:
        raise yaml.YAMLError("secret-body-must-not-leak")

    monkeypatch.setattr(credentials, "private_json", invalid_yaml)
    assert credentials.main() == 1
    output = capsys.readouterr()
    assert not output.out and "secret-body-must-not-leak" not in output.err
    assert "reconciliation failed" in output.err


@pytest.mark.parametrize("uid", ["", "wrong-namespace-uid"])
def test_namespace_uid_is_checked_before_namespaced_access(
    monkeypatch: pytest.MonkeyPatch, uid: str
) -> None:
    commands: list[list[str]] = []

    def fake_run(argv: list[str], **kwargs) -> bytes:
        commands.append(argv)
        return json.dumps({"metadata": {"uid": "approved-namespace-uid"}}).encode()

    monkeypatch.setattr(credentials, "run", fake_run)
    with pytest.raises(credentials.ReconcileError, match="cluster/namespace identity"):
        credentials.Kubernetes("/protected/kubeconfig", credentials.SOURCE, uid)
    assert len(commands) == 1 and "namespace" in commands[0] and "-n" not in commands[0]


def test_private_config_requires_owner_regular_file_and_object(tmp_path: Path) -> None:
    path = tmp_path / "credentials.json"
    path.write_text('{"schema_version":"test"}')
    path.chmod(0o600)
    assert credentials.private_json(path) == {"schema_version": "test"}
    path.chmod(0o640)
    with pytest.raises(credentials.ReconcileError, match="owner-only regular file"):
        credentials.private_json(path)
    path.chmod(0o600)
    link = tmp_path / "symlink.json"
    link.symlink_to(path)
    with pytest.raises(credentials.ReconcileError, match="owner-only regular file"):
        credentials.private_json(link)
    path.write_text('["secret-never-print"]')
    with pytest.raises(credentials.ReconcileError, match="must be an object"):
        credentials.private_json(path)


def test_private_config_wrong_owner_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "config.json"
    path.write_text("{}")
    path.chmod(0o600)
    monkeypatch.setattr(os, "geteuid", lambda: path.stat().st_uid + 1)
    with pytest.raises(credentials.ReconcileError, match="owner-only regular file"):
        credentials.private_json(path)


@pytest.fixture
def auth_config(tmp_path: Path) -> dict:
    path = tmp_path / "nebius.yaml"
    path.write_text(
        json.dumps(
            {
                "profiles": {
                    "staging-automation": {
                        "auth-type": "service account",
                        "service-account-id": "serviceaccount-test",
                        "public-key-id": "publickey-test",
                        "private-key": str(tmp_path / "authorized-private.pem"),
                    }
                }
            }
        )
    )
    path.chmod(0o600)
    return {
        "canonical_kubeconfig": "/etc/rancher/k3s/k3s.yaml",
        "nebius_config": str(path),
        "nebius_profile": "staging-automation",
        "nebius_kubeconfig": str(tmp_path / "renewable-kubeconfig.yaml"),
    }


def test_auth_accepts_only_explicit_noninteractive_service_profile(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    kube = {
        "users": [
            {
                "user": {
                    "exec": {
                        "command": "/usr/local/bin/nebius",
                        "interactiveMode": "Never",
                        "args": [
                            "--config",
                            auth_config["nebius_config"],
                            "--profile",
                            "staging-automation",
                            "--no-browser",
                            "iam",
                            "get-access-token",
                        ],
                    }
                }
            }
        ]
    }
    monkeypatch.setattr(credentials, "run", lambda argv: json.dumps(kube).encode())
    credentials.validate_auth(auth_config, destination=True)
    kube["users"][0]["user"]["exec"]["interactiveMode"] = "IfAvailable"
    with pytest.raises(credentials.ReconcileError, match="exec authentication"):
        credentials.validate_auth(auth_config, destination=True)


@pytest.mark.parametrize(
    "user",
    [
        {"token": "private-static-token"},
        {"client-certificate-data": "private-cert", "client-key-data": "private-key"},
        {"exec": {}, "token": "private-static-token"},
    ],
)
def test_auth_rejects_static_destination_credentials(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch, user: dict
) -> None:
    monkeypatch.setattr(
        credentials, "run", lambda argv: json.dumps({"users": [{"user": user}]}).encode()
    )
    with pytest.raises(credentials.ReconcileError, match="renewable exec authentication"):
        credentials.validate_auth(auth_config, destination=True)


def test_auth_rejects_personal_profile_and_copied_canonical_kubeconfig(auth_config: dict) -> None:
    auth_config["canonical_kubeconfig"] = "/tmp/copied-kubeconfig"
    with pytest.raises(credentials.ReconcileError, match="live k3s kubeconfig"):
        credentials.validate_auth(auth_config, destination=False)
    auth_config["canonical_kubeconfig"] = "/etc/rancher/k3s/k3s.yaml"
    Path(auth_config["nebius_config"]).write_text(
        '{"profiles":{"staging-automation":{"token":"private-browser-token"}}}'
    )
    with pytest.raises(credentials.ReconcileError, match="service-account authorized-key"):
        credentials.validate_auth(auth_config, destination=False)


@pytest.mark.parametrize(
    "field", ["auth-type", "service-account-id", "public-key-id", "private-key"]
)
def test_auth_rejects_incomplete_service_profile(auth_config: dict, field: str) -> None:
    path = Path(auth_config["nebius_config"])
    document = json.loads(path.read_text())
    document["profiles"]["staging-automation"].pop(field)
    path.write_text(json.dumps(document))
    with pytest.raises(credentials.ReconcileError, match="service-account authorized-key"):
        credentials.validate_auth(auth_config, destination=False)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda plugin: plugin["args"].extend(["--config", "/other/config"]),
        lambda plugin: plugin["args"].extend(["--profile", "personal"]),
        lambda plugin: plugin["args"].extend(["--profile=personal"]),
        lambda plugin: plugin["args"].extend(["--config=/other/config"]),
        lambda plugin: plugin["args"].extend(["-p", "personal"]),
        lambda plugin: plugin["args"].extend(["-c", "/other/config"]),
        lambda plugin: plugin["args"].remove("--no-browser"),
        lambda plugin: plugin["args"].__setitem__(1, "/other/config"),
        lambda plugin: plugin["args"].__setitem__(3, "personal"),
        lambda plugin: plugin.update(env=[{"name": "NEBIUS_PROFILE", "value": "personal"}]),
        lambda plugin: plugin.update(command="other-auth-command"),
    ],
)
def test_auth_rejects_ambiguous_or_human_exec_plugin(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch, mutation
) -> None:
    plugin = {
        "command": "nebius",
        "interactiveMode": "Never",
        "args": [
            "--config",
            auth_config["nebius_config"],
            "--profile",
            "staging-automation",
            "--no-browser",
        ],
    }
    mutation(plugin)
    monkeypatch.setattr(
        credentials,
        "run",
        lambda argv: json.dumps({"users": [{"user": {"exec": plugin}}]}).encode(),
    )
    with pytest.raises(credentials.ReconcileError, match="exec authentication"):
        credentials.validate_auth(auth_config, destination=True)


@pytest.fixture
def seed_config(tmp_path: Path, auth_config: dict) -> dict:
    task_image = "registry.example/task@sha256:" + "1" * 64
    runtime_image = "registry.example/runtime@sha256:" + "2" * 64
    key = Ed25519PrivateKey.from_private_bytes(b"\x15" * 32).public_key()
    public_key = base64.b64encode(
        key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    ).decode()
    runtime = {
        "schema_version": "loom.service-execution-runtime-profile.v1",
        "candidate_sha": "b" * 40,
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "task_image_ref": task_image,
        "runtime_image_ref": runtime_image,
        "runtime_binary_sha256": "sha256:" + "3" * 64,
        "image_admission": signed_image_admission_bundle((task_image, runtime_image)).model_dump(
            mode="json"
        ),
    }
    binding = attachment()
    documents = {
        "runtime_profile_file": runtime,
        "admission_keyring_file": {
            "schema_version": 1,
            "keys": [{"signing_key_id": "test-builder", "public_key_base64": public_key}],
        },
        "observer_credentials_file": {
            "subject-credentials": {
                "alg": "RS256",
                "private-key": "private-observer",
                "kid": "publickey-observer",
                "iss": "serviceaccount-observer",
                "sub": "serviceaccount-observer",
            }
        },
        "terraform_spool_output": {
            "environment": "staging",
            "target_id": binding["target_id"],
            "secret_delivery_mode": "EXPLICIT",
            "endpoint": binding["source"]["endpoint"],
            "region": binding["source"]["region"],
            "bucket_name": binding["source"]["bucket"],
            "access_key_resource_id": "accesskey-spool",
            "aws_access_key_id": "spool-access-key",
        },
    }
    for name, value in documents.items():
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps(value))
        path.chmod(0o600)
        auth_config[name] = str(path)
    return auth_config


def test_seed_validates_release_and_getsecret_identity_before_persisting(
    source: FakeKube, seed_config: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands: list[list[str]] = []

    def fake_run(argv: list[str]) -> bytes:
        commands.append(argv)
        return json.dumps(
            {"aws_access_key_id": "spool-access-key", "secret": "protected-getsecret-value"}
        ).encode()

    monkeypatch.setattr(credentials, "run", fake_run)
    credentials.seed_inputs(source, seed_config, attachment())
    assert source.puts == [
        "loom-nebius-staging-spool",
        "loom-nebius-staging-observer",
        "loom-nebius-staging-runtime",
        "loom-nebius-staging-admission",
    ]
    assert len(commands) == 1 and commands[0][-3:] == ["get-secret", "--id", "accesskey-spool"]
    assert "--no-browser" in commands[0] and "staging-automation" in commands[0]
    assert "protected-getsecret-value" not in str(commands)
    first = copy.deepcopy(source.objects)
    credentials.seed_inputs(source, seed_config, attachment())
    assert source.objects == first


@pytest.mark.parametrize(
    "field,value",
    [
        ("runtime_profile_file", {"schema_version": "loom.service-execution-runtime-profile.v1"}),
        ("runtime_profile_file", {}),
        ("admission_keyring_file", {"schema_version": 1}),
        ("admission_keyring_file", {"schema_version": 1, "keys": []}),
        ("observer_credentials_file", {"subject-credentials": {"alg": "RS256"}}),
        (
            "terraform_spool_output",
            {"environment": "development", "secret_delivery_mode": "EXPLICIT"},
        ),
    ],
)
def test_seed_incomplete_or_wrong_inputs_fail_before_any_put_or_cloud_read(
    source: FakeKube, seed_config: dict, monkeypatch: pytest.MonkeyPatch, field: str, value: dict
) -> None:
    Path(seed_config[field]).write_text(json.dumps(value))

    def unexpected_run(argv: list[str]) -> bytes:
        pytest.fail("invalid seed must not contact cloud")

    monkeypatch.setattr(credentials, "run", unexpected_run)
    with pytest.raises((ValueError, credentials.ReconcileError)):
        credentials.seed_inputs(source, seed_config, attachment())
    assert source.puts == [] and source.writes == []


def test_seed_wrong_getsecret_identity_never_writes_source(
    source: FakeKube, seed_config: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        credentials,
        "run",
        lambda argv: json.dumps(
            {"aws_access_key_id": "wrong-key-id", "secret": "wrong-private-secret"}
        ).encode(),
    )
    with pytest.raises(credentials.ReconcileError, match="Terraform identity") as caught:
        credentials.seed_inputs(source, seed_config, attachment())
    assert "wrong-private-secret" not in str(caught.value) and source.puts == []


@pytest.mark.parametrize("missing_agent", [False, True])
def test_seed_accepts_signed_catalog_images_but_requires_agent_coverage(
    source: FakeKube, seed_config: dict, monkeypatch: pytest.MonkeyPatch, missing_agent: bool
) -> None:
    path = Path(seed_config["runtime_profile_file"])
    profile = json.loads(path.read_text())
    agent = "registry.example/worker@sha256:" + "4" * 64
    task = "registry.example/tb90@sha256:" + "5" * 64
    profile["agent_image_ref"] = agent
    images = [profile["task_image_ref"], profile["runtime_image_ref"], task]
    if not missing_agent:
        images.append(agent)
    profile["image_admission"] = signed_image_admission_bundle(tuple(images)).model_dump(
        mode="json"
    )
    path.write_text(json.dumps(profile))
    monkeypatch.setattr(
        credentials,
        "run",
        lambda argv: json.dumps(
            {"aws_access_key_id": "spool-access-key", "secret": "secret"}
        ).encode(),
    )
    if missing_agent:
        with pytest.raises(credentials.ReconcileError, match="admission is incomplete"):
            credentials.seed_inputs(source, seed_config, attachment())
        assert not source.puts
    else:
        credentials.seed_inputs(source, seed_config, attachment())
        assert "loom-nebius-staging-runtime" in source.puts


def test_configure_kubeconfig_normalizes_generated_auth_and_cleans_temporary_files(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_config["nebius_cluster_id"] = "mk8scluster-test"
    target = Path(auth_config["nebius_kubeconfig"])
    target.write_text("previous-private-kubeconfig")
    target.chmod(0o600)
    original = {
        "apiVersion": "v1",
        "kind": "Config",
        "current-context": "approved-cluster",
        "clusters": [
            {
                "name": "approved-cluster",
                "cluster": {
                    "server": "https://cluster.example",
                    "certificate-authority-data": "test-ca",
                },
            }
        ],
        "contexts": [
            {
                "name": "approved-cluster",
                "context": {"cluster": "approved-cluster", "user": "automation"},
            }
        ],
        "users": [
            {
                "name": "automation",
                "user": {
                    "exec": {
                        "command": "/usr/local/bin/nebius",
                        "apiVersion": "client.authentication.k8s.io/v1",
                        "interactiveMode": "IfAvailable",
                        "env": [{"name": "NEBIUS_PROFILE", "value": "personal"}],
                        "args": [
                            "--profile",
                            "personal",
                            "--config=/old/config",
                            "-p",
                            "old-profile",
                            "-c",
                            "/other/config",
                            "--profile=old",
                            "--config",
                            "/third/config",
                            "--no-browser",
                            "iam",
                            "get-access-token",
                        ],
                    }
                },
            }
        ],
    }
    directories: list[Path] = []

    def provider_run(argv: list[str]) -> bytes:
        assert "--internal" in argv and argv[argv.index("--id") + 1] == "mk8scluster-test"
        assert argv[argv.index("--profile") + 1] == "staging-automation"
        temporary = Path(argv[argv.index("--kubeconfig") + 1])
        directories.append(temporary.parent)
        assert stat.S_IMODE(temporary.parent.stat().st_mode) == 0o700
        assert target.read_text() == "previous-private-kubeconfig"
        temporary.write_text(yaml.safe_dump(original))
        return b""

    monkeypatch.setattr(credentials, "run", provider_run)
    assert credentials.configure_kubeconfig(auth_config) == {
        "renewable_kubeconfig_configured": True
    }
    actual = yaml.safe_load(target.read_text())
    assert actual["clusters"] == original["clusters"] and actual["contexts"] == original["contexts"]
    plugin = actual["users"][0]["user"]["exec"]
    assert plugin["args"] == [
        "--config",
        auth_config["nebius_config"],
        "--profile",
        "staging-automation",
        "--no-browser",
        "iam",
        "get-access-token",
    ]
    assert plugin["interactiveMode"] == "Never" and "env" not in plugin
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert directories and all(not path.exists() for path in directories)


@pytest.mark.parametrize(
    "user",
    [
        {"token": "private-static-token"},
        {"client-certificate-data": "private-cert", "client-key-data": "private-key"},
        {"exec": {"command": "unapproved-plugin"}},
    ],
)
def test_configure_rejects_unsupported_generated_auth_preserving_previous_file(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch, user: dict
) -> None:
    auth_config["nebius_cluster_id"] = "mk8scluster-test"
    target = Path(auth_config["nebius_kubeconfig"])
    target.write_text("previous-private-kubeconfig")
    target.chmod(0o600)

    def provider_run(argv: list[str]) -> bytes:
        temporary = Path(argv[argv.index("--kubeconfig") + 1])
        temporary.write_text(yaml.safe_dump({"users": [{"user": user}]}))
        return b""

    monkeypatch.setattr(credentials, "run", provider_run)
    with pytest.raises(credentials.ReconcileError) as caught:
        credentials.configure_kubeconfig(auth_config)
    assert "private-" not in str(caught.value)
    assert target.read_text() == "previous-private-kubeconfig"
    assert not list(target.parent.glob(".loom-nebius-auth-*"))


def test_configure_api_failure_preserves_existing_kubeconfig_and_cleans_temp(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_config["nebius_cluster_id"] = "mk8scluster-test"
    target = Path(auth_config["nebius_kubeconfig"])
    target.write_text("previous-private-kubeconfig")
    target.chmod(0o600)

    def provider_run(argv: list[str]) -> bytes:
        raise credentials.ReconcileError("provider unavailable")

    monkeypatch.setattr(credentials, "run", provider_run)
    with pytest.raises(credentials.ReconcileError, match="provider unavailable"):
        credentials.configure_kubeconfig(auth_config)
    assert target.read_text() == "previous-private-kubeconfig"
    assert not list(target.parent.glob(".loom-nebius-auth-*"))


def test_configure_refuses_symlink_target_before_cloud_read(
    auth_config: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_config["nebius_cluster_id"] = "mk8scluster-test"
    target = Path(auth_config["nebius_kubeconfig"])
    existing = target.parent / "unrelated-kubeconfig"
    existing.write_text("unrelated-private-kubeconfig")
    target.symlink_to(existing)

    def unexpected_run(argv: list[str]) -> bytes:
        pytest.fail("unsafe target must not contact cloud")

    monkeypatch.setattr(credentials, "run", unexpected_run)
    with pytest.raises(credentials.ReconcileError, match="must be protected"):
        credentials.configure_kubeconfig(auth_config)
    assert existing.read_text() == "unrelated-private-kubeconfig"


def test_remote_failure_discards_raw_secret_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 1, b"private-stdout", b"private-stderr"
        ),
    )
    with pytest.raises(credentials.ReconcileError) as caught:
        credentials.run(["kubectl", "get", "secret"])
    assert "private-stdout" not in str(caught.value) and "private-stderr" not in str(caught.value)


def test_main_reports_only_secret_safe_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        '{"schema_version":"loom.nebius-staging-credentials.v1","attachment_file":"/private/secret-token"}'
    )
    path.chmod(0o600)
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        credentials, "private_json", lambda _: (_ for _ in ()).throw(RuntimeError(PASSWORD))
    )
    monkeypatch.setattr(
        "sys.argv", ["nebius_staging_credentials.py", "reconcile", "--config", str(path)]
    )
    assert credentials.main() == 1
    output = capsys.readouterr()
    assert output.out == "" and PASSWORD not in output.err and "secret-token" not in output.err
    assert "reconciliation failed" in output.err
