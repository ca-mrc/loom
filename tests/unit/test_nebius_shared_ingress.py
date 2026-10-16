"""Shared HTTPS must not replace the standalone allocation or TLS state."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from loom.nebius_platform_render import build_platform, validate_environment
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_environment_render import rendered

ROOT = Path(__file__).resolve().parents[2]


def test_opt_in_routing_preserves_public_allocation_and_original_tls(platform_inputs):
    config, candidate, profile = platform_inputs
    original = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    enabled = build_platform({**config, "shared_ingress_enabled": True}, candidate, profile,
                             {}, repo_root=ROOT)
    public = enabled["70-public.yaml"][0]
    assert public["metadata"] == original["70-public.yaml"][0]["metadata"]
    assert public["spec"]["type"] == "LoadBalancer"
    assert public["spec"]["ports"] == original["70-public.yaml"][0]["spec"]["ports"]
    assert public["spec"]["selector"] == {"app": "loom-shared-ingress"}
    # Caddy and the RWO TLS PVC survive; only the config revision changes.
    old_web = next(d for d in original["40-services.yaml"] if d["kind"] == "Deployment"
                   and d["metadata"]["name"] == "loom-web")
    new_web = next(d for d in enabled["40-services.yaml"] if d["kind"] == "Deployment"
                   and d["metadata"]["name"] == "loom-web")
    assert new_web["spec"]["template"]["spec"] == old_web["spec"]["template"]["spec"]
    origin = next(d for d in enabled["40-services.yaml"]
                  if d["metadata"]["name"] == "loom-web-origin")
    assert origin["spec"]["selector"] == {"app": "loom-web"}
    assert origin["spec"].get("type", "ClusterIP") == "ClusterIP"
    assert origin["spec"]["ports"][0]["targetPort"] == 8443


@pytest.mark.parametrize("value", ["true", 1, None, {}, []])
def test_shared_routing_mode_rejects_non_booleans(platform_inputs, value):
    with pytest.raises(ValueError, match="shared_ingress_enabled"):
        validate_environment({**platform_inputs[0], "shared_ingress_enabled": value})


def test_children_do_not_inherit_shared_entrypoint_control(platform_inputs):
    config, candidate, profile = deepcopy(platform_inputs)
    config["shared_ingress_enabled"] = True
    child = rendered((config, candidate, profile))
    assert "shared_ingress_enabled" not in child.config
    assert not any(d["metadata"]["name"] == "loom-web-origin"
                   for docs in child.files.values() for d in docs)
    cm = next(d for docs in child.files.values() for d in docs if d["kind"] == "ConfigMap")
    assert "shared_ingress_enabled" not in json.loads(cm["data"]["environment.json"])


def test_managed_persisted_input_rejects_shared_routing_option(platform_inputs):
    child = rendered(platform_inputs)
    with pytest.raises(ValueError, match="shared_ingress_enabled"):
        validate_environment({**child.config, "shared_ingress_enabled": True})


@pytest.fixture
def ingress_input(platform_inputs):
    from tests.unit.test_nebius_environment_contract import foundation_from

    foundation = foundation_from(platform_inputs[0]).model_dump(mode="json")
    foundation.update(ingress_namespace=platform_inputs[0]["namespace"],
                      ingress_controller_label="loom-shared-ingress")
    return {"schema_version": "loom.nebius-shared-ingress.v1",
            "installation_id": "30000000-0000-4000-8000-000000000001",
            "foundation": foundation, "tls_secret_name": "loom-shared-public-tls",
            "image": "cr.eu-north1.nebius.cloud/test/traefik@sha256:" + "d" * 64}


def ingress_documents(value):
    from loom.nebius_shared_ingress import SharedIngressInstallation, render_shared_ingress

    return render_shared_ingress(SharedIngressInstallation.model_validate(value))


def test_shared_controller_preserves_legacy_sni_and_never_allocates_public_resources(ingress_input):
    docs = ingress_documents(ingress_input)
    assert not {"Secret", "Namespace", "PersistentVolumeClaim", "Ingress"} & {d["kind"] for d in docs}
    assert all(d["spec"].get("type", "ClusterIP") == "ClusterIP" for d in docs if d["kind"] == "Service")
    config = next(d["data"] for d in docs if d["kind"] == "ConfigMap")
    dynamic = json.loads(config["routes.yaml"])
    platform = json.loads(ingress_input["foundation"]["platform_config_json"])
    assert dynamic["tcp"]["routers"]["standalone"] == {
        "rule": "HostSNI(`" + platform["public_host"] + "`)",
        "entryPoints": ["websecure"], "service": "standalone", "tls": {"passthrough": True},
    }
    assert dynamic["tcp"]["services"]["standalone"]["loadBalancer"]["servers"] == [{
        "address": "loom-web-origin." + platform["namespace"] + ".svc.cluster.local:443",
    }]
    assert dynamic["tls"]["stores"]["default"]["defaultCertificate"]["keyFile"] == "/var/run/loom-ingress-tls/tls.key"
    # Strict SNI consults the selectable certificate map, not the fallback store.
    assert dynamic["tls"]["certificates"] == [{"certFile": "/var/run/loom-ingress-tls/tls.crt",
                                                "keyFile": "/var/run/loom-ingress-tls/tls.key"}]
    static = json.loads(config["traefik.json"])
    assert set(static["entryPoints"]) == {"websecure", "health"}
    assert static["entryPoints"]["websecure"]["http"]["tls"] == {}
    assert static["providers"]["kubernetesIngress"]["ingressClass"] == "loom-shared"
    assert static["providers"]["kubernetesIngress"]["strictPrefixMatching"] is True
    assert static["providers"]["kubernetesIngress"]["allowExternalNameServices"] is False
    assert static["providers"]["kubernetesIngress"]["crossProviderNamespaces"] == []
    assert not static.get("api")
    # Body buffering would hold streams and multiply disk use per child route.
    assert "middlewares" not in static["entryPoints"]["websecure"]["http"]
    assert "http" not in dynamic


def test_shared_controller_is_read_only_nonroot_and_has_surge_budget(ingress_input):
    docs = ingress_documents(ingress_input)
    for doc in docs:
        assert doc["metadata"]["labels"]["loom.nebius/ingress-installation-id"] == ingress_input["installation_id"]
        if doc["kind"] == "ClusterRole":
            assert all(set(rule["verbs"]) <= {"get", "list", "watch"} for rule in doc["rules"])
    pod = next(d["spec"]["template"]["spec"] for d in docs if d["kind"] == "Deployment")
    assert pod["serviceAccountName"] == "loom-shared-ingress"
    assert pod["securityContext"]["runAsNonRoot"] is True
    container = pod["containers"][0]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert container["image"] == ingress_input["image"]
    assert container["resources"]["requests"] == {"cpu": "100m", "memory": "128Mi", "ephemeral-storage": "64Mi"}
    assert {v["secret"]["secretName"] for v in pod["volumes"] if "secret" in v} == {"loom-shared-public-tls"}


@pytest.mark.parametrize("field,value", [
    ("image", "traefik:latest"), ("image", "docker.io/library/traefik@sha256:" + "a" * 64),
    ("image", None), ("tls_secret_name", "bad/name"),
    ("installation_id", "00000000-0000-0000-0000-000000000000"),
])
def test_invalid_shared_configuration_is_rejected_before_render(ingress_input, field, value):
    with pytest.raises(ValueError):
        ingress_documents({**ingress_input, field: value})


@pytest.mark.parametrize("field,value", [("ingress_namespace", "somewhere-else"),
                                         ("ingress_controller_label", "unselected")])
def test_shared_controller_must_match_public_service_and_network_policy(ingress_input, field, value):
    ingress_input["foundation"][field] = value
    with pytest.raises(ValueError):
        ingress_documents(ingress_input)
