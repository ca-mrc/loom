"""DNS may target only an already completed, freshly observed ingress cutover."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops import nebius_ingress_operation as operation
from scripts.ops.nebius_ingress_cutover import MARKER
from tests.ops.test_nebius_dns_publication import Provider
from tests.ops.test_nebius_dns_publication import target as target
from tests.ops.test_nebius_ingress_gateway import inputs as inputs
from tests.ops.test_nebius_ingress_install import installation as installation
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.support.nebius_platform import platform_inputs as platform_inputs


@pytest.fixture
def installed(installation, monkeypatch):
    arguments, tls, _ = installation
    api = arguments["api"]
    api.service["status"]["loadBalancer"]["ingress"] = [{"ip": "8.8.8.8"}]
    operation.install_ingress(**arguments)

    def denied(*args, **kwargs):
        pytest.fail("DNS qualification attempted a Kubernetes/guard write")

    for name in ("patch", "restore", "guard", "create_secret"):
        monkeypatch.setattr(api, name, denied)
    monkeypatch.setattr(api.stage, "create_resource", denied)
    return {key: arguments[key] for key in ("api", "certificate_config", "state_dir")}, tls


def test_read_only_dns_qualification_returns_exact_retained_target(installed):
    arguments, tls = installed
    api = arguments["api"]
    before = copy.deepcopy(api.read())
    target = operation.qualify_dns_target(**arguments)
    assert target == {
        "installation_id": api.binding.installation_id,
        "service_uid": before[0]["metadata"]["uid"], "candidate": api.candidate,
        "address": "8.8.8.8", "zone": arguments["certificate_config"]["zone"],
        "child_domain": api.binding.child_domain, "management_host": api.binding.management_host,
        "fingerprint_sha256": json.loads((Path(arguments["certificate_config"]["state_dir"]) / "selected.json").read_text())["fingerprint_sha256"],
    }
    assert api.read() == before and tls.creates == 1


def test_dns_qualification_accepts_new_protected_application_candidate(installed):
    arguments, _ = installed
    api = arguments["api"]
    api.candidate = "b" * 40
    profile = json.loads(api.config["data"]["profile.json"])
    api.config["data"]["profile.json"] = json.dumps({**profile, "candidate_sha": api.candidate})
    assert operation.qualify_dns_target(**arguments)["candidate"] == "b" * 40


def test_unrelated_service_annotation_does_not_require_dns_ownership(installed):
    arguments, _ = installed
    api = arguments["api"]
    api.service["metadata"]["annotations"]["foreign"] = "retained"
    operation.qualify_dns_target(**arguments)
    assert api.service["metadata"]["annotations"]["foreign"] == "retained"


@pytest.mark.parametrize("drift", ["service_uid", "ip", "selector", "port", "config_uid", "owner", "mode", "candidate", "host"])
def test_dns_qualification_refuses_live_routing_identity_drift(installed, drift):
    arguments, _ = installed
    api = arguments["api"]
    if drift == "service_uid":
        api.service["metadata"]["uid"] = str(uuid4())
    elif drift == "ip":
        api.service["status"]["loadBalancer"]["ingress"] = [{"ip": "1.1.1.1"}]
    elif drift == "selector":
        api.service["spec"]["selector"] = {"app": "foreign"}
    elif drift == "port":
        api.service["spec"]["ports"][0]["targetPort"] = 9443
    elif drift == "config_uid":
        api.config["metadata"]["uid"] = str(uuid4())
    elif drift == "owner":
        api.config["metadata"]["annotations"][MARKER] = str(uuid4())
    elif drift == "candidate":
        api.candidate = "b" * 40
    else:
        environment = json.loads(api.config["data"]["environment.json"])
        environment["shared_ingress_enabled" if drift == "mode" else "public_host"] = False if drift == "mode" else "other.example.test"
        api.config["data"]["environment.json"] = json.dumps(environment)
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)


@pytest.mark.parametrize("phase", ["release_intent", "configuration_switched", "rolled_back"])
def test_dns_qualification_requires_completed_cutover(installed, phase):
    arguments, _ = installed
    path = arguments["state_dir"] / "cutover/cutover.json"
    journal = json.loads(path.read_text())
    journal["phase"] = phase
    path.write_text(json.dumps(journal))
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)


def test_dns_qualification_does_not_create_missing_tls_delivery(installed):
    arguments, _ = installed
    root = arguments["certificate_config"]["state_dir"]
    receipt = next((Path(root) / "deliveries").glob("*.json"))
    receipt.unlink()
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)
    assert not receipt.exists()


@pytest.mark.parametrize("drift", ["cluster", "controller", "origin", "tls", "legacy", "public", "during_probe"])
def test_dns_qualification_requires_current_end_to_end_route(installed, monkeypatch, drift):
    arguments, tls = installed
    api = arguments["api"]
    if drift == "cluster":
        monkeypatch.setattr(api, "verify_identity", lambda binding: (_ for _ in ()).throw(RuntimeError("replaced namespace")))
    elif drift == "controller":
        deployment = next(row for row in api.stage.resources.values() if row["kind"] == "Deployment")
        deployment["metadata"]["uid"] = str(uuid4())
    elif drift == "origin":
        origin = next(row for row in api.stage.resources.values() if row["kind"] == "Service")
        origin["metadata"]["uid"] = str(uuid4())
    elif drift == "tls":
        monkeypatch.setattr(api, "probe_tls", lambda *args: "0" * 64)
    elif drift == "legacy":
        api.legacy_ok = False
    elif drift == "public":
        api.probe_ok = False
    else:
        def changed(receipt):
            api.service["metadata"]["uid"] = str(uuid4())
        monkeypatch.setattr(api, "probe_public", changed)
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)
    assert tls.creates == 1


@pytest.mark.parametrize("evidence", ["cutover", "stage", "selection"])
def test_dns_qualification_does_not_recreate_missing_evidence(installed, evidence):
    arguments, _ = installed
    paths = {"cutover": arguments["state_dir"] / "cutover/cutover.json",
             "stage": arguments["state_dir"] / "stage" / (arguments["api"].binding.installation_id + ".json"),
             "selection": Path(arguments["certificate_config"]["state_dir"]) / "selected.json"}
    paths[evidence].unlink()
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)
    assert not paths[evidence].exists()


@pytest.mark.parametrize("failure", [None, "expired", "changed", "expires_midway", "changed_after_authority",
                                    "expired_after_authority", "authority", "public"])
def test_fixed_dns_operation_checks_credentials_authority_and_public_route_before_success(target, tmp_path, monkeypatch, failure):
    from scripts.ops import nebius_dns_challenge as credentials
    from scripts.ops import nebius_dns_publication as publication

    events = []
    api = object()
    config = {"credential_file": str(tmp_path / "credential.json")}
    state = tmp_path / "ingress"
    state.mkdir(mode=0o700)

    def qualify(**arguments):
        assert arguments == {"api": api, "certificate_config": config, "state_dir": state}
        events.append("ingress")
        return dict(target)

    def token(path):
        assert path == tmp_path / "credential.json"
        events.append("credential")
        if (failure == "expired" or (failure == "expires_midway" and events.count("credential") > 1)
                or (failure == "expired_after_authority" and events.count("authority") >= 2)):
            raise credentials.DNSChallengeError("expired")
        if ((failure == "changed" and events.count("credential") > 1)
                or (failure == "changed_after_authority" and events.count("authority") >= 2)):
            return "replacement-private-token"
        return "private-fixture-token"

    class BoundProvider(Provider):
        def __init__(self, actual, token):
            super().__init__()
            assert actual == target and token == "private-fixture-token"
            events.append("provider")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def create(self, name, address):
            events.append("create:" + name)
            return super().create(name, address)

    def authority(actual):
        assert actual == target
        events.append("authority")
        if failure == "authority":
            raise publication.PublicationError("delegated")

    def public(actual):
        assert actual == target
        events.append("public")
        if failure == "public":
            raise publication.PublicationError("wrong TLS")

    monkeypatch.setattr(operation, "qualify_dns_target", qualify)
    monkeypatch.setattr(credentials, "load_token", token)
    monkeypatch.setattr(publication, "GoDaddyPublication", BoundProvider)
    monkeypatch.setattr(publication, "qualify_authority", authority)
    monkeypatch.setattr(publication, "wait_for_addresses", lambda actual: events.append("propagation"))
    monkeypatch.setattr(publication, "qualify_public_routes", public)
    if failure:
        with pytest.raises(operation.OperationError) as caught:
            operation.publish_ingress_dns(api=api, certificate_config=config, state_dir=state)
        assert "private-fixture-token" not in str(caught.value)
    else:
        result = operation.publish_ingress_dns(api=api, certificate_config=config, state_dir=state)
        assert result["status"] == "dns_published" and result["service_uid"] == target["service_uid"]
        assert events.index("authority") < events.index("create:*.dev.nebius")
        assert events.index("propagation") < events.index("public")
    if failure in {"expired", "changed", "expires_midway", "changed_after_authority", "expired_after_authority", "authority"}:
        assert not any(event.startswith("create:") for event in events)
    if failure == "expired":
        assert "provider" not in events
    if failure == "public":
        journal = json.loads((state / "dns/dns-publication.json").read_text())
        assert journal["status"] == "pending"
