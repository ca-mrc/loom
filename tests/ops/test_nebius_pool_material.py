"""Dedicated pool credentials reach only the registered shared processes."""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.integration.test_nebius_pool_installation import add_application_builder
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.ops.test_nebius_pool_migration import migration_request
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs


def material():
    from loom_service.pool_management.installation import PoolInstallation

    request = migration_request()
    config = request.registration.spec.model_dump(mode="json")
    tokens = {row.machine_id: "pool_material_" + row.machine_id.hex for row in request.registration.spec.machines}
    for row in config["machines"]:
        row["token_sha256"] = hashlib.sha256(("pool_material_" + row["machine_id"].replace("-", "")).encode()).hexdigest()
    return replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(config))), tokens


class MaterialAPI(PhaseAPI):
    key = staticmethod(_key)


def test_builder_credential_is_delivered_only_to_management_not_controllers_or_execution(build_inputs):
    from scripts.ops.nebius_pool_material import machine_documents

    from loom_service.pool_management.installation import PoolInstallation

    request, tokens = material()
    config, identity, secret = add_application_builder(request.registration.spec.model_dump(mode="json"), build_inputs[0].recipe)
    request = replace(request, registration=replace(request.registration, spec=PoolInstallation.model_validate(config)))
    documents = machine_documents(request, tokens | {identity: secret})
    selected = [row for row in documents.values() if row["metadata"]["name"] == "loom-pool-machine-" + identity.hex]
    assert len(documents) == 9 and len(selected) == 1
    assert selected[0]["metadata"]["namespace"] == request.registration.binding.namespace
    import base64

    assert base64.b64decode(selected[0]["data"]["token"]).decode() == secret


def test_deliver_exact_hash_qualified_machine_tokens_without_rotation(tmp_path):
    from scripts.ops.nebius_pool_material import deliver_pool_material, machine_documents

    request, tokens = material()
    api = MaterialAPI(request.registration.binding)
    documents = machine_documents(request, tokens)
    expected = {request.registration.binding.namespace, *(row.namespace for row in request.guards),
        *(row.execution_namespace.name for row in request.registration.spec.participants)}
    assert {doc["metadata"]["namespace"] for doc in documents.values()} == expected
    assert len(documents) == 8  # Observer + gateway, then CP/actuator for each shared participant.
    development, = [row for row in request.registration.spec.participants if row.environment_class == "development"]
    observer, = [row for row in request.registration.spec.machines if row.role == "observer"]
    observer_secret, = [doc for doc in documents.values() if doc["metadata"]["name"] == "loom-pool-machine-" + observer.machine_id.hex]
    assert observer_secret["metadata"]["namespace"] == development.execution_namespace.name
    assert all(doc["immutable"] is True and doc["type"] == "Opaque" and set(doc["data"]) == {"token"} for doc in documents.values())
    receipt = deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path)
    assert deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path) == receipt
    assert len(api.creates) == 8
    assert all(token not in json.dumps(receipt) for token in tokens.values())


@pytest.mark.parametrize("damage", ["missing", "extra", "wrong_hash", "newline"])
def test_incomplete_or_unqualified_tokens_cannot_write_any_secret(tmp_path, damage):
    from scripts.ops.nebius_pool_material import deliver_pool_material

    request, tokens = material()
    api = MaterialAPI(request.registration.binding)
    key = next(iter(tokens))
    if damage == "missing":
        tokens.pop(key)
    elif damage == "extra":
        tokens[uuid4()] = "private-marker"
    elif damage == "newline":
        tokens[key] += "\n"
    else:
        tokens[key] = "private-marker"
    with pytest.raises(ValueError) as error:
        deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path)
    assert "private-marker" not in str(error.value)
    assert not api.creates


@pytest.mark.parametrize("failure", ["before", "after"])
def test_lost_secret_create_does_not_generate_another_credential(tmp_path, failure):
    from scripts.ops.nebius_pool_material import deliver_pool_material

    request, tokens = material()
    api = MaterialAPI(request.registration.binding)
    api.failure = failure
    if failure == "before":
        for _ in range(2):
            with pytest.raises(ValueError):
                deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path)
        assert len(api.creates) == 1
    else:
        first = deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path)
        assert deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path) == first
        assert len(api.creates) == 8


def test_real_https_scope_checks_all_namespace_uids_before_secret_writes(tmp_path):
    from scripts.ops.nebius_pool_material import (
        HTTPSPoolMaterialAPI,
        deliver_pool_material,
        machine_documents,
    )

    request, tokens = material()
    binding = request.registration.binding
    identities = {"kube-system": binding.kube_system_uid, binding.namespace: binding.namespace_uid,
        **{row.namespace: str(row.namespace_uid) for row in request.guards},
        **{row.execution_namespace.name: str(row.execution_namespace.uid) for row in request.registration.spec.participants}}
    resources, calls = {}, []
    drift = False

    def respond(message):
        calls.append(message)
        path = message.url.path
        if path in {"/api/v1/namespaces/" + name for name in identities}:
            name = path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "uid": str(uuid4()) if drift and name == request.guards[1].namespace else identities[name],
                "labels": {"loom.nebius/management-installation": binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
        if message.method == "GET":
            return httpx.Response(200, json=resources[path]) if path in resources else httpx.Response(404)
        assert message.method == "POST" and path.endswith("/secrets")
        document = json.loads(message.content)
        actual = copy.deepcopy(document)
        actual["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        if "dryRun" not in message.url.params:
            resources[path + "/" + document["metadata"]["name"]] = actual
        return httpx.Response(201, json=actual)

    api = HTTPSPoolMaterialAPI(request=request, tokens=tokens, api_server="https://cluster.example", ssl_context=ssl.create_default_context())
    api.client.close()
    api.client = httpx.Client(base_url="https://cluster.example", transport=httpx.MockTransport(respond))
    with api:
        first = deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path)
        assert deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path) == first
        writes = [row for row in calls if row.method == "POST" and "dryRun" not in row.url.params]
        assert len(writes) == 8
        forbidden = copy.deepcopy(next(iter(machine_documents(request, tokens).values())))
        forbidden["metadata"]["namespace"] = "loom-dev-personal"
        with pytest.raises(RuntimeError):
            api.create_resource(forbidden)
        drift = True
        with pytest.raises(ValueError):
            deliver_pool_material(request=request, tokens=tokens, api=api, state_dir=tmp_path)
        assert len([row for row in calls if row.method == "POST" and "dryRun" not in row.url.params]) == 8
