"""The connected installer consumes live evidence, never readiness flags."""
from __future__ import annotations

import copy
import importlib
import io
import json
import ssl
import zipfile
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_candidate import source_checkout as source_checkout
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_development_preflight import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_preflight import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_preflight import (
    retained_namespace,
)
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.unit.test_nebius_candidate_catalog import github_transport
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_development_foundation import development_inputs as development_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_development_live")


@pytest.fixture
def live(preflight, cloud, tmp_path, monkeypatch):
    from nebius.api.nebius.iam import v1, v2
    from nebius.api.nebius.quotas import v1 as quotas
    from nebius.api.nebius.storage import v1 as storage
    from nebius.sdk import SDK
    from scripts.ops.nebius_development_bootstrap import DevelopmentBootstrapBinding
    from scripts.ops.nebius_development_cloud import DevelopmentCloudScope
    from scripts.ops.nebius_development_install import DevelopmentInstallRequest
    from scripts.ops.nebius_development_stage import DevelopmentStageInput

    mod = module()
    cloud.config["buckets"]["trajectories"] = "loom-dev-trajectories"
    cloud.scope["data_buckets"]["bucket-trajectories"] = "loom-dev-trajectories"
    bucket_type, data_bucket = cloud.rows["bucket-data"]
    trajectory_bucket = copy.deepcopy(data_bucket)
    trajectory_bucket["metadata"].update(id="bucket-trajectories", name="loom-dev-trajectories")
    cloud.rows["bucket-trajectories"] = bucket_type, trajectory_bucket
    preflight.config.update(cloud.config)
    preflight.config["postgres_storage_gi"] = 10
    with zipfile.ZipFile(io.BytesIO(preflight.publication.payload)) as bundle:
        profile = json.loads(bundle.read("runtime-profile.json"))
    selection = DevelopmentStageInput(preflight.config, preflight.publication.candidate,
        profile, preflight.publication.keyring, cloud.material)
    binding = DevelopmentBootstrapBinding(str(uuid4()), str(preflight.client.settings.kube_system_uid),
        selection.config["db_tls_secret_name"])
    request = DevelopmentInstallRequest(binding, selection)
    credentials = tmp_path / "operator.json"
    credentials.write_text('{"private":"operator-test"}')
    credentials.chmod(0o600)
    token_file = tmp_path / "publication-token"
    token_file.write_text("test-github-secret")
    token_file.chmod(0o600)
    settings = mod.DevelopmentLiveSettings(preflight=preflight.client.settings,
        cloud=DevelopmentCloudScope.model_validate(cloud.scope),
        operator_cloud_credentials=credentials, github_token_file=token_file)
    api = mod.HTTPSDevelopmentInstallationAPI(request=request, settings=settings,
        api_server=preflight.client.api_server, ssl_context=ssl.create_default_context(), token="operator-k8s-secret",
        state_dir=tmp_path / "state")

    @contextmanager
    def actual_preflight(**kwargs):
        assert kwargs["settings"] == preflight.client.settings
        yield preflight.client

    monkeypatch.setattr(mod, "HTTPSDevelopmentPreflight", actual_preflight)
    original_http = httpx.AsyncClient
    monkeypatch.setattr(mod.httpx, "AsyncClient", lambda **kwargs: original_http(
        **kwargs, transport=github_transport(preflight.publication.responses, preflight.publication.payload)))
    closes = []

    async def close(self):
        closes.append(True)

    monkeypatch.setattr(SDK, "__init__", lambda self, **kwargs: None)
    monkeypatch.setattr(SDK, "close", close)
    for owner, name, key in (
        (v1, "ProjectServiceClient", "projects"), (v1, "ServiceAccountServiceClient", "accounts"),
        (v1, "GroupServiceClient", "groups"), (v1, "GroupMembershipServiceClient", "memberships"),
        (v1, "AccessPermitServiceClient", "permits"), (v2, "AccessKeyServiceClient", "access_keys"),
        (storage, "BucketServiceClient", "buckets"), (quotas, "QuotaAllowanceServiceClient", "quotas"),
    ):
        monkeypatch.setattr(owner, name, lambda sdk, key=key: cloud.clients[key])
    lists = []
    state = SimpleNamespace(api=api, request=request, preflight=preflight, cloud=cloud,
        credentials=credentials, lists=lists, closes=closes, objects_fail=False, state_dir=tmp_path / "state")

    @contextmanager
    def s3(config, material, *, source):
        assert config == preflight.config and material == cloud.material

        def listing(**kwargs):
            lists.append((source, kwargs))
            return {"ResponseMetadata": {"HTTPStatusCode": 403 if state.objects_fail else 200}}

        yield SimpleNamespace(list_objects_v2=listing)

    monkeypatch.setattr(mod, "development_object_client", s3)
    return state


@pytest.fixture
def installed(live, monkeypatch):
    from nebius.api.nebius.compute import v1 as compute
    from scripts.ops.nebius_development_install import _storage_observation
    from scripts.ops.nebius_development_stage import DevelopmentResourceBinding, stage_development_resources
    from tests.ops.test_nebius_development_install import storage_ready
    from tests.ops.test_nebius_management_stage import PhaseAPI

    ns = retained_namespace(live.preflight, live.request.bootstrap.installation_id)
    binding = DevelopmentResourceBinding(live.request.bootstrap, ns["metadata"]["uid"],
        ns["metadata"]["annotations"]["loom.nebius/development-bootstrap-operation"])
    backend = PhaseAPI(binding)
    for phase in ("database", "services"):
        stage_development_resources(selection=live.request.selection, binding=binding, phase=phase,
            api=backend, state_dir=live.state_dir / phase)
    claim, volume = storage_ready(SimpleNamespace(stage=backend), live.request.selection)
    claim["metadata"]["creationTimestamp"] = "2026-10-07T12:00:01Z"
    volume["metadata"]["creationTimestamp"] = "2026-10-07T12:00:03Z"
    volume["spec"]["csi"]["volumeHandle"] = "computedisk-test"
    observation = _storage_observation(live.request, binding, backend)
    database = backend.resources["StatefulSet:loom-postgres"]
    spec = copy.deepcopy(database["spec"]["template"]["spec"])
    spec.setdefault("volumes", []).append({"name": "data", "persistentVolumeClaim": {"claimName": "data-loom-postgres-0"}})
    spec.update(nodeName="computeinstance-test", hostname="loom-postgres-0", subdomain="loom-postgres")
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"namespace": "loom-dev", "name": "loom-postgres-0",
        "uid": str(uuid4()), "ownerReferences": [{"apiVersion": "apps/v1", "kind": "StatefulSet", "controller": True,
            "uid": database["metadata"]["uid"], "name": "loom-postgres"}]}, "spec": spec}
    node = {"apiVersion": "v1", "kind": "Node", "metadata": {"name": "computeinstance-test", "uid": str(uuid4())},
        "spec": {"providerID": "nebius://computeinstance-test"}}
    docs = {"/api/v1/namespaces/kube-system": live.preflight.documents["/api/v1/namespaces/kube-system"],
        "/api/v1/namespaces/loom-dev": ns, "/api/v1/namespaces/loom-dev/pods/loom-postgres-0": pod,
        "/api/v1/nodes/computeinstance-test": node}
    resources = {"Service": ("/api/v1", "services"), "Deployment": ("/apis/apps/v1", "deployments"),
        "StatefulSet": ("/apis/apps/v1", "statefulsets"), "PersistentVolumeClaim": ("/api/v1", "persistentvolumeclaims")}
    for row in backend.resources.values():
        if row["kind"] == "PersistentVolume":
            path = "/api/v1/persistentvolumes/" + row["metadata"]["name"]
        else:
            prefix, plural = resources[row["kind"]]
            path = prefix + "/namespaces/loom-dev/" + plural + "/" + row["metadata"]["name"]
        docs[path] = row
    reads = []
    state = SimpleNamespace(live=live, binding=binding, backend=backend, observation=observation,
        docs=docs, pod=pod, node=node, volume=volume, reads=reads, on_read=None)

    def respond(request):
        assert request.method == "GET"
        assert request.headers["Authorization"] == "Bearer operator-k8s-secret"
        reads.append(request.url.path)
        if state.on_read:
            state.on_read(request.url.path)
        row = docs.get(request.url.path)
        return httpx.Response(404) if row is None else httpx.Response(200, json=row)

    monkeypatch.setattr(module().httpx, "HTTPTransport", lambda **kwargs: httpx.MockTransport(respond))
    monkeypatch.setattr(compute, "DiskServiceClient", lambda sdk: live.cloud.clients["disks"])
    return state


def test_connected_disk_qualification_reads_recorded_controller_pod_node_and_provider(installed):
    state = installed
    state.live.api.qualify_volume(state.live.request, state.binding, state.observation)
    assert ("get", "computedisk-test") in state.live.cloud.calls
    assert "/api/v1/nodes/computeinstance-test" in state.reads
    assert state.live.closes == [True]


@pytest.mark.parametrize("change", ["controller", "pod-owner", "pod-claim", "node", "disk", "late-replacement"])
def test_disk_proof_rejects_foreign_or_changing_live_ownership(installed, change):
    state = installed
    if change == "controller":
        state.backend.resources["StatefulSet:loom-postgres"]["metadata"]["uid"] = str(uuid4())
    elif change == "pod-owner":
        state.pod["metadata"]["ownerReferences"][0]["uid"] = str(uuid4())
    elif change == "pod-claim":
        state.pod["spec"]["volumes"][-1]["persistentVolumeClaim"]["claimName"] = "staging"
    elif change == "node":
        state.node["spec"]["providerID"] = "nebius://computeinstance-foreign"
    elif change == "disk":
        state.live.cloud.rows["computedisk-test"][1]["metadata"]["parent_id"] = "project-foreign"
    else:
        def replace_pod(path):
            if path.endswith("/pods/loom-postgres-0") and state.reads.count(path) == 2:
                state.pod["metadata"]["uid"] = str(uuid4())
        state.on_read = replace_pod
    with pytest.raises(module().DevelopmentInstallError):
        state.live.api.qualify_volume(state.live.request, state.binding, state.observation)


def test_connected_checks_verify_actual_publication_inventory_provider_and_both_keys(live):
    live.api.qualify(live.request, fresh=True)
    assert live.lists == [(False, {"Bucket": "loom-dev-data", "MaxKeys": 1}),
                          (False, {"Bucket": "loom-dev-trajectories", "MaxKeys": 1}),
                          (True, {"Bucket": "loom-dev-source", "MaxKeys": 1})]
    assert ("get", "compute-network-ssd") in live.cloud.calls
    assert live.closes == [True]
    retained_namespace(live.preflight, live.request.bootstrap.installation_id)
    live.api.qualify(live.request, fresh=False)
    with pytest.raises(module().DevelopmentInstallError):
        live.api.qualify(live.request, fresh=True)


@pytest.mark.parametrize("failure", ["publication", "quota", "s3", "namespace", "config", "credential-file"])
def test_failed_live_prerequisite_never_becomes_install_authority(live, failure):
    if failure == "publication":
        live.preflight.publication.responses["actions/runs/123/attempts/1"]["conclusion"] = "failure"
    elif failure == "quota":
        live.cloud.rows["compute-network-ssd"][1]["spec"]["limit"] = "0"
    elif failure == "s3":
        live.objects_fail = True
    elif failure == "namespace":
        retained_namespace(live.preflight, uuid4())
    elif failure == "config":
        live.request.selection.config["postgres_storage_gi"] += 1
    else:
        live.credentials.write_text("replaced operator credentials")
    with pytest.raises(module().DevelopmentInstallError) as error:
        live.api.qualify(live.request, fresh=False)
    assert "secret" not in str(error.value) and "operator-test" not in str(error.value)
    assert live.api.diagnostic_stage is not None
    assert all(not path.endswith("/secrets") for path in live.preflight.calls)


def test_connected_resources_cannot_substitute_another_selection_or_namespace(live):
    from scripts.ops.nebius_development_stage import (
        DevelopmentResourceBinding,
        HTTPSDevelopmentStageAPI,
    )

    binding = DevelopmentResourceBinding(live.request.bootstrap, str(uuid4()), str(uuid4()))
    with live.api.resources(binding, live.request.selection, "database") as api:
        assert isinstance(api, HTTPSDevelopmentStageAPI)
        assert set(api.documents) == {"StatefulSet:loom-postgres", "Service:loom-postgres"}
    altered = copy.deepcopy(live.request.selection)
    altered.storage["secret-key"] = "different"
    with pytest.raises(module().DevelopmentInstallError):
        live.api.resources(binding, altered, "database")
    other = replace(binding, bootstrap=replace(binding.bootstrap, installation_id=str(uuid4())))
    with pytest.raises(module().DevelopmentInstallError):
        live.api.resources(other, live.request.selection, "config")


def test_s3_client_uses_explicit_identity_and_rejects_origin_changes(live, monkeypatch):
    import boto3

    # The fixture replaces the I/O factory for composed checks, not this factory test.
    monkeypatch.undo()
    factory = module().development_object_client
    made, handlers, closed = [], [], []

    def client(service, **kwargs):
        made.append((service, kwargs))
        return SimpleNamespace(meta=SimpleNamespace(events=SimpleNamespace(
            register_first=lambda event, callback: handlers.append(callback))), close=lambda: closed.append(True))

    monkeypatch.setattr(boto3, "client", client)
    for source, key in ((False, "aws-data"), (True, "aws-source")):
        with factory(live.request.selection.config, live.cloud.material, source=source):
            kwargs = made[-1][1]
            assert kwargs["aws_access_key_id"] == key
            assert kwargs["config"].retries["total_max_attempts"] == 1
            assert kwargs["config"].proxies == {}
            handlers[-1](SimpleNamespace(url=live.request.selection.config["storage_endpoint"] + "/bucket"))
            with pytest.raises(module().DevelopmentInstallError):
                handlers[-1](SimpleNamespace(url="https://foreign.example/bucket"))
    assert len(closed) == 2
