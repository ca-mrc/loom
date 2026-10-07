"""Fresh development ownership and private credentials survive uncertain writes."""
from __future__ import annotations

import base64
import copy
import importlib
import json
import os
import shutil
import ssl
import stat
import subprocess
import sys
from dataclasses import asdict, replace
from uuid import uuid4

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from sqlalchemy.engine import make_url


def module():
    return importlib.import_module("scripts.ops.nebius_development_bootstrap")


class BootstrapAPI:
    """Only the external Kubernetes store is doubled; keys/journals are real."""

    def __init__(self, binding):
        self.binding = binding
        self.namespace = None
        self.secrets = {}
        self.creates = []
        self.failure = None

    def verify_cluster(self, binding):
        if binding != self.binding:
            raise RuntimeError("private-cluster-diagnostic")

    def get_namespace(self):
        return copy.deepcopy(self.namespace)

    def create_namespace(self, document):
        assert self.namespace is None
        assert document["metadata"]["name"] == "loom-dev"
        self.creates.append("namespace")
        if self.failure == ("namespace", "before"):
            raise OSError("private-request-diagnostic")
        self.namespace = copy.deepcopy(document)
        self.namespace["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        self.namespace["metadata"]["labels"]["kubernetes.io/metadata.name"] = "loom-dev"
        self.namespace["spec"] = {"finalizers": ["kubernetes"]}
        self.namespace["status"] = {"phase": "Active"}
        if self.failure == ("namespace", "after"):
            raise OSError("private-response-diagnostic")

    def get_secret(self, name):
        return copy.deepcopy(self.secrets.get(name))

    def create_secret(self, document, *, namespace_uid):
        assert self.namespace["metadata"]["uid"] == namespace_uid
        assert document["metadata"]["namespace"] == "loom-dev"
        name = document["metadata"]["name"]
        assert name not in self.secrets
        self.creates.append(name)
        if self.failure == (name, "before"):
            raise OSError("private-request-diagnostic")
        actual = copy.deepcopy(document)
        actual["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        self.secrets[name] = actual
        if self.failure == (name, "after"):
            raise OSError("private-response-diagnostic")


def setup():
    binding = module().DevelopmentBootstrapBinding(str(uuid4()), str(uuid4()), "loom-development-db-tls")
    return binding, BootstrapAPI(binding)


def bootstrap(binding, api, root):
    return module().bootstrap_development(binding=binding, api=api,
        state_dir=root / "state", anchor_dir=root / "anchor")


def test_create_only_namespace_and_local_material_replay_preserves_keys_and_uids(tmp_path):
    binding, api = setup()
    first = bootstrap(binding, api, tmp_path)
    before = copy.deepcopy((api.namespace, api.secrets))
    journal = (tmp_path / "state/bootstrap.json").read_bytes()
    assert bootstrap(binding, api, tmp_path) == first
    assert first["status"] == "development_local_bootstrap_complete"
    assert set(first) == {"status", "installation_id", "namespace", "namespace_uid", "secret_uids"}
    assert first["namespace"] == "loom-dev" and first["namespace_uid"] == api.namespace["metadata"]["uid"]
    assert first["installation_id"] == binding.installation_id
    assert api.creates == ["namespace", "loom-platform-db", "loom-development-db-tls", "loom-platform-auth", "loom-admin-secret"]
    assert (api.namespace, api.secrets) == before
    assert (tmp_path / "state/bootstrap.json").read_bytes() == journal
    assert stat.S_IMODE((tmp_path / "state/bootstrap.json").stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "state").stat().st_mode) == 0o700
    assert api.namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    for name, secret in api.secrets.items():
        assert secret["immutable"] is True
        assert first["secret_uids"][name] == secret["metadata"]["uid"]
        for value in secret["data"].values():
            assert value not in json.dumps(first)


def test_generated_keys_match_dev_database_only_and_contain_no_runtime_authority(tmp_path):
    binding, api = setup()
    bootstrap(binding, api, tmp_path)
    material = {name: {key: base64.b64decode(value).decode() for key, value in doc["data"].items()}
                for name, doc in api.secrets.items()}
    db = material["loom-platform-db"]
    assert set(db) == {"ca.crt", "postgres-password", "admin-url", "service-url", "service-password",
                       "control-plane-url", "control-plane-password", "gateway-url", "gateway-password"}
    for role in ("admin", "service", "control-plane", "gateway"):
        url = make_url(db[role + "-url"])
        assert url.host == "loom-postgres.loom-dev.svc" and url.database == "loom" and url.port == 5432
        assert url.username == ("postgres" if role == "admin" else "loom_" + role.replace("-", "_"))
        assert url.password == db["postgres-password" if role == "admin" else role + "-password"]
        assert url.query == {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}
    tls = material[binding.tls_secret_name]
    leaf = x509.load_pem_x509_certificate(tls["tls.crt"].encode())
    ca = x509.load_pem_x509_certificate(db["ca.crt"].encode())
    leaf.verify_directly_issued_by(ca)
    assert set(leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName)) == {
        "loom-postgres.loom-dev.svc", "loom-postgres.loom-dev.svc.cluster.local"}
    private = serialization.load_pem_private_key(tls["tls.key"].encode(), password=None)
    assert private.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo) == (
        leaf.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo))
    assert set(material["loom-platform-auth"]) == {"jwt-signing-key", "secret-store-master-key"}
    other_binding, other = setup()
    bootstrap(other_binding, other, tmp_path / "other")
    assert api.secrets["loom-platform-db"]["data"] != other.secrets["loom-platform-db"]["data"]


@pytest.mark.parametrize("target", ["namespace", "loom-platform-db", "loom-admin-secret"])
@pytest.mark.parametrize("point", ["before", "after"])
def test_uncertain_create_is_read_back_and_never_repeated(tmp_path, target, point):
    binding, api = setup()
    api.failure = target, point
    if point == "after":
        first = bootstrap(binding, api, tmp_path)
        assert bootstrap(binding, api, tmp_path) == first
    else:
        for _ in range(2):
            with pytest.raises(module().DevelopmentBootstrapError, match="unresolved"):
                bootstrap(binding, api, tmp_path)
            api.failure = None
    assert api.creates.count(target) == 1


@pytest.mark.parametrize("existing", ["namespace", "loom-platform-db", "loom-admin-secret"])
def test_foreign_or_untracked_namespace_and_credentials_are_never_adopted(tmp_path, existing):
    binding, api = setup()
    if existing == "namespace":
        api.namespace = {"metadata": {"name": "loom-dev", "uid": str(uuid4())}}
    else:
        api.secrets[existing] = {"foreign": "staging-key-material"}
    with pytest.raises(module().DevelopmentBootstrapError):
        bootstrap(binding, api, tmp_path)
    assert not api.creates


@pytest.mark.parametrize("drift", ["namespace_uid", "pss", "namespace_operation", "cluster", "secret_uid", "secret_data", "secret_missing"])
def test_identity_or_material_drift_stops_replay_without_replacement(tmp_path, drift):
    binding, api = setup()
    bootstrap(binding, api, tmp_path)
    journal = (tmp_path / "state/bootstrap.json").read_bytes()
    if drift == "namespace_uid":
        api.namespace["metadata"]["uid"] = str(uuid4())
    elif drift == "pss":
        api.namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"
    elif drift == "namespace_operation":
        api.namespace["metadata"]["annotations"]["loom.nebius/development-bootstrap-operation"] = str(uuid4())
    elif drift == "cluster":
        api.binding = replace(binding, kube_system_uid=str(uuid4()))
    elif drift == "secret_uid":
        api.secrets["loom-platform-auth"]["metadata"]["uid"] = str(uuid4())
    elif drift == "secret_data":
        api.secrets["loom-platform-auth"]["data"]["jwt-signing-key"] = "cmVwbGFjZWQ="
    else:
        del api.secrets["loom-platform-auth"]
    with pytest.raises(module().DevelopmentBootstrapError):
        bootstrap(binding, api, tmp_path)
    assert len(api.creates) == 5
    assert (tmp_path / "state/bootstrap.json").read_bytes() == journal


@pytest.mark.parametrize("loss", ["state_tree", "journal", "anchor", "bad_hash", "mode", "symlink"])
def test_lost_or_changed_private_evidence_never_regenerates(tmp_path, loss):
    binding, api = setup()
    bootstrap(binding, api, tmp_path)
    path = tmp_path / "state/bootstrap.json"
    if loss == "state_tree":
        shutil.rmtree(tmp_path / "state")  # This test's generated fixture only.
    elif loss == "journal":
        path.unlink()
    elif loss == "anchor":
        shutil.rmtree(tmp_path / "anchor")  # This test's generated fixture only.
    elif loss == "bad_hash":
        record = json.loads(path.read_bytes())
        record["material"]["loom-platform-auth"]["jwt-signing-key"] = "private-changed-key"
        path.write_text(json.dumps(record))
    elif loss == "mode":
        path.chmod(0o644)
    else:
        path.rename(path.with_suffix(".retained"))
        path.symlink_to(path.with_suffix(".retained"))
    api.namespace, api.secrets = None, {}
    with pytest.raises(module().DevelopmentBootstrapError) as error:
        bootstrap(binding, api, tmp_path)
    assert "private-changed-key" not in str(error.value)
    assert len(api.creates) == 5


def test_namespace_replacement_between_secrets_stops_later_delivery(tmp_path):
    binding, api = setup()
    create = api.create_secret
    def replace_after_first(document, *, namespace_uid):
        create(document, namespace_uid=namespace_uid)
        api.namespace["metadata"]["uid"] = str(uuid4())
    api.create_secret = replace_after_first
    with pytest.raises(module().DevelopmentBootstrapError):
        bootstrap(binding, api, tmp_path)
    assert api.creates == ["namespace", "loom-platform-db"]


def test_all_retained_secret_identities_are_checked_before_resuming_any_write(tmp_path):
    binding, api = setup()
    api.failure = "loom-admin-secret", "before"
    with pytest.raises(module().DevelopmentBootstrapError):
        bootstrap(binding, api, tmp_path)
    api.failure = None
    api.secrets["loom-platform-db"]["metadata"]["uid"] = str(uuid4())
    with pytest.raises(module().DevelopmentBootstrapError):
        bootstrap(binding, api, tmp_path)
    assert len(api.creates) == 5


def test_process_death_leaves_durable_intent_and_does_not_retry_namespace(tmp_path):
    binding, api = setup()
    program = r'''
import json, os, sys
from pathlib import Path
from scripts.ops.nebius_development_bootstrap import DevelopmentBootstrapBinding, bootstrap_development
from tests.ops.test_nebius_development_bootstrap import BootstrapAPI
binding = DevelopmentBootstrapBinding(**json.loads(sys.argv[1]))
api = BootstrapAPI(binding)
api.create_namespace = lambda document: os._exit(73)
bootstrap_development(binding=binding, api=api, state_dir=Path(sys.argv[2]), anchor_dir=Path(sys.argv[3]))
'''
    result = subprocess.run([sys.executable, "-c", program, json.dumps(asdict(binding)), str(tmp_path / "state"), str(tmp_path / "anchor")],
        env=dict(os.environ), capture_output=True, timeout=30)
    assert result.returncode == 73
    with pytest.raises(module().DevelopmentBootstrapError, match="unresolved"):
        bootstrap(binding, api, tmp_path)
    assert not api.creates


@pytest.mark.parametrize("change", [{"namespace": "loom-nebius-platform"}, {"namespace": "loom-dev-alice"},
    {"installation_id": "../escape"}, {"kube_system_uid": str(uuid4()).upper()}, {"tls_secret_name": "loom-platform-db"},
    {"tls_secret_name": "../foreign"}, {"tls_secret_name": "loom-admin-secret"}])
def test_bootstrap_binding_cannot_target_other_namespaces_or_shadow_local_secrets(change):
    binding, _ = setup()
    with pytest.raises(module().DevelopmentBootstrapError):
        replace(binding, **change)


def test_anchor_must_be_independent_of_working_state(tmp_path):
    binding, api = setup()
    for anchor in (tmp_path / "state", tmp_path / "state/anchor", tmp_path):
        with pytest.raises(module().DevelopmentBootstrapError):
            module().bootstrap_development(binding=binding, api=api, state_dir=tmp_path / "state", anchor_dir=anchor)
    assert not api.creates


def test_https_adapter_scope_and_nonretrying_create(tmp_path):
    binding, backend = setup()
    calls = []
    def respond(request):
        calls.append((request.method, request.url.path))
        assert request.headers["Authorization"] == "Bearer private-api-token"
        if request.method == "GET":
            if request.url.path == "/api/v1/namespaces/kube-system":
                return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                    "name": "kube-system", "uid": binding.kube_system_uid}})
            if request.url.path == "/api/v1/namespaces/loom-dev":
                return httpx.Response(404) if backend.namespace is None else httpx.Response(200, json=backend.namespace)
            assert request.url.path.startswith("/api/v1/namespaces/loom-dev/secrets/")
            row = backend.secrets.get(request.url.path.rsplit("/", 1)[-1])
            return httpx.Response(404) if row is None else httpx.Response(200, json=row)
        assert request.method == "POST"
        document = json.loads(request.content)
        if request.url.path == "/api/v1/namespaces":
            backend.create_namespace(document)
        else:
            assert request.url.path == "/api/v1/namespaces/loom-dev/secrets"
            backend.create_secret(document, namespace_uid=backend.namespace["metadata"]["uid"])
        # An ambiguous service-unavailable reply after actual persistence.
        return httpx.Response(503, headers={"Retry-After": "0", "Location": "/must-not-follow"})
    with module().HTTPSDevelopmentBootstrapAPI(binding=binding, api_server="https://cluster.invalid",
            ssl_context=ssl.create_default_context(), token="private-api-token") as client:
        client.client.close()
        client.client = httpx.Client(base_url=client.api_server, headers={"Authorization": "Bearer private-api-token"},
                                    transport=httpx.MockTransport(respond))
        first = bootstrap(binding, client, tmp_path)
        assert bootstrap(binding, client, tmp_path) == first
        assert sum(method == "POST" for method, _ in calls) == 5
        before = len(calls)
        foreign = copy.deepcopy(backend.secrets["loom-platform-db"])
        foreign["metadata"]["namespace"] = "loom-nebius-platform"
        with pytest.raises(module().DevelopmentBootstrapError):
            client.create_secret(foreign, namespace_uid=backend.namespace["metadata"]["uid"])
        with pytest.raises(module().DevelopmentBootstrapError):
            client.get_secret("loom-platform-storage")
        assert len(calls) == before
