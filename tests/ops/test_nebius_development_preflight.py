"""Fresh dev preflight authenticates source and reads, never adopts or writes."""
from __future__ import annotations

import copy
import hashlib
import importlib
import io
import json
import ssl
import subprocess
import zipfile
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_candidate import create_candidate
from tests.ops.test_nebius_candidate import inputs
from tests.ops.test_nebius_candidate import source_checkout as source_checkout
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_management_capacity import workload
from tests.unit.test_nebius_candidate_catalog import github_transport
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_development_foundation import development_inputs as development_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def module():
    return importlib.import_module("scripts.ops.nebius_development_preflight")


@pytest.fixture
def published_source(publication, source_checkout, tmp_path):
    reference, responses, _, _, _ = copy.deepcopy(publication)
    _, revision, archive = source_checkout
    record, private, keyring = inputs(tmp_path)
    record.update(candidate_sha=revision, source_archive_sha256="sha256:" + hashlib.sha256(archive).hexdigest())
    candidate, profile = create_candidate(record, signing_key=private, signing_key_id="publisher", keyring_json=keyring)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as bundle:
        bundle.writestr("candidate.json", json.dumps(candidate))
        bundle.writestr("runtime-profile.json", json.dumps(profile))
    payload = output.getvalue()
    reference.update(source_sha=revision, artifact_sha256="sha256:" + hashlib.sha256(payload).hexdigest())
    responses["actions/runs/123/attempts/1"]["head_sha"] = revision
    responses["pulls/40"]["merge_commit_sha"] = revision
    artifact = responses["actions/artifacts/123"]
    artifact.update(name=f"nebius-candidate-{revision}-123-1", digest=reference["artifact_sha256"], size_in_bytes=len(payload))
    artifact["workflow_run"]["head_sha"] = revision
    return SimpleNamespace(reference=reference, responses=responses, payload=payload,
                           keyring=json.loads(keyring), candidate=candidate)


@pytest.fixture
def preflight(development_inputs, published_source, inventory):
    mod = module()
    config, _, _ = development_inputs
    settings = mod.DevelopmentPreflightSettings(
        publication=published_source.reference, registry_prefix=published_source.candidate["registry_prefix"],
        source=mod.prepare_development_source(published_source.reference["source_sha"]),
        kube_system_uid=uuid4(), storage_class_uid=uuid4(), storage_parameters={})
    client = mod.HTTPSDevelopmentPreflight(settings=settings, api_server=config["kubernetes_api_server"],
        ssl_context=ssl.create_default_context(), token="test-kubernetes-secret")
    client.client.close()
    node = inventory["nodes"][0]
    node["status"]["allocatable"] = {"cpu": "16", "memory": "32Gi", "ephemeral-storage": "64Gi", "pods": "100"}
    rows = {"nodes": [node], "pods": inventory["pods"]}
    kinds = {"nodes": "Node", "pods": "Pod", "deployments": "Deployment", "statefulsets": "StatefulSet",
        "replicasets": "ReplicaSet", "daemonsets": "DaemonSet", "jobs": "Job", "cronjobs": "CronJob",
        "replicationcontrollers": "ReplicationController", "persistentvolumeclaims": "PersistentVolumeClaim",
        "persistentvolumes": "PersistentVolume", "horizontalpodautoscalers": "HorizontalPodAutoscaler"}
    documents = {
        "/api/v1/namespaces/kube-system": {"apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": "kube-system", "uid": str(settings.kube_system_uid)}},
        "/api/v1/namespaces/loom-dev": None,
        "/apis/storage.k8s.io/v1/storageclasses/" + config["storage_class"]: {
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass", "metadata": {
                "name": config["storage_class"], "uid": str(settings.storage_class_uid)},
            "provisioner": "compute.csi.nebius.com", "parameters": {}, "volumeBindingMode": "WaitForFirstConsumer"},
    }
    state = SimpleNamespace(client=client, config=config, publication=published_source, rows=rows,
        documents=documents, calls=[], pages={}, namespace_reads=0, on_read=None)

    def respond(request):
        # Any PATCH/POST/DELETE, including admission dry runs, fails the test.
        assert request.method == "GET"
        assert request.headers["Authorization"] == "Bearer test-kubernetes-secret"
        state.calls.append(request.url.path)
        if state.on_read:
            state.on_read(request)
        if request.url.path in documents:
            doc = documents[request.url.path]
            return httpx.Response(404) if doc is None else httpx.Response(200, json=doc)
        resource = request.url.path.rsplit("/", 1)[-1]
        api = "v1" if request.url.path.startswith("/api/v1/") else "/".join(request.url.path.split("/")[2:4])
        page = state.pages.get(resource, {"apiVersion": api, "kind": kinds[resource] + "List",
            "metadata": {"resourceVersion": "1"}, "items": rows.get(resource, [])})
        return httpx.Response(200, json=page)

    client.client = httpx.Client(base_url=client.api_server, headers={"Authorization": "Bearer test-kubernetes-secret"},
                                transport=httpx.MockTransport(respond))
    yield state
    client.client.close()


async def check(state):
    pub = state.publication
    async with httpx.AsyncClient(transport=github_transport(pub.responses, pub.payload), trust_env=False) as http:
        return await state.client.inspect(config=state.config, keyring=pub.keyring,
            github_token="test-github-secret", http=http)


async def test_verified_source_and_fresh_resources_produce_readonly_not_installed_evidence(preflight):
    before = copy.deepcopy(preflight.rows)
    result = await check(preflight)
    evidence = result.evidence
    assert evidence["status"] == "fresh_dev_preflight_only"
    assert evidence["namespace"] == "loom-dev"
    assert evidence["source_sha"] == preflight.publication.reference["source_sha"]
    assert evidence["revision"] == result.rendered.revision
    assert evidence["capacity"]["node_uid"] == preflight.rows["nodes"][0]["metadata"]["uid"]
    assert evidence["database_storage_mib"] == preflight.config["postgres_storage_gi"] * 1024
    assert set(evidence["unverified"]) == {"installer_bundle_authority", "provider_storage_quota", "credential_provenance", "installation",
        "runtime_isolation", "public_access", "shared_pool_activation", "owner_acceptance"}
    assert preflight.rows == before
    assert preflight.calls.count("/api/v1/namespaces/loom-dev") == 2
    assert "test-github-secret" not in json.dumps(evidence) and "test-kubernetes-secret" not in json.dumps(evidence)
    assert all(doc["metadata"].get("namespace", doc["metadata"]["name"]) == "loom-dev"
               for docs in result.rendered.files.values() for doc in docs)


@pytest.mark.parametrize("change", ["tracked", "untracked", "older-source"])
def test_unpublished_or_dirty_installer_cannot_prepare_source(preflight, source_checkout, change):
    root, _, _ = source_checkout
    if change == "older-source":
        # A valid approved publication cannot run an installer from a newer SHA.
        subprocess.run(["git", "commit", "--allow-empty", "-qm", "newer installer"], cwd=root, check=True)
    else:
        (root / ("source.txt" if change == "tracked" else "feature.txt")).write_text("unpublished\n")
    with pytest.raises(module().DevelopmentPreflightError):
        module().prepare_development_source(preflight.publication.reference["source_sha"])
    assert not preflight.calls


@pytest.mark.parametrize("change", ["failed-check", "expired-artifact", "wrong-artifact", "wrong-trust"])
async def test_rejected_publication_never_reaches_cluster(preflight, change):
    pub = preflight.publication
    if change == "failed-check":
        pub.responses["commits/" + "b" * 40 + "/check-runs"]["check_runs"][0]["conclusion"] = "failure"
    elif change == "expired-artifact":
        pub.responses["actions/artifacts/123"]["expired"] = True
    elif change == "wrong-artifact":
        pub.payload += b"changed"
    else:
        pub.keyring["keys"] = []
    with pytest.raises(module().DevelopmentPreflightError) as error:
        await check(preflight)
    assert "test-github-secret" not in str(error.value)
    assert not preflight.calls


@pytest.mark.parametrize("change", ["staging", "production", "personal", "api-server"])
async def test_cannot_retarget_to_staging_or_another_api(preflight, change):
    if change == "staging":
        preflight.config["namespace"] = "loom-nebius-platform"
    elif change == "production":
        preflight.config["environment"] = "production"
    elif change == "personal":
        preflight.config["namespace"] = "loom-dev-alice"
    else:
        preflight.config["kubernetes_api_server"] = "https://other.invalid"
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)
    assert not preflight.calls


@pytest.mark.parametrize("change", ["namespace", "terminating", "cluster-uid", "namespace-race", "cluster-race"])
async def test_existing_or_replaced_identity_is_not_a_fresh_installation(preflight, change):
    ns_path = "/api/v1/namespaces/loom-dev"
    if change in {"namespace", "terminating"}:
        preflight.documents[ns_path] = {"kind": "Namespace", "metadata": {"name": "loom-dev", "uid": str(uuid4())}}
        if change == "terminating":
            preflight.documents[ns_path]["metadata"]["deletionTimestamp"] = "2026-10-07T00:00:00Z"
    elif change == "cluster-uid":
        preflight.documents["/api/v1/namespaces/kube-system"]["metadata"]["uid"] = str(uuid4())
    else:
        def race(request):
            if request.url.path == ns_path:
                preflight.namespace_reads += 1
                if preflight.namespace_reads == 2:
                    if change == "namespace-race":
                        preflight.documents[ns_path] = {"kind": "Namespace", "metadata": {"uid": str(uuid4())}}
                    else:
                        preflight.documents["/api/v1/namespaces/kube-system"]["metadata"]["uid"] = str(uuid4())
        preflight.on_read = race
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


@pytest.mark.parametrize("resource", ["persistentvolumes", "persistentvolumeclaims", "deployments", "pods"])
async def test_retained_storage_and_orphan_objects_are_not_adopted(preflight, resource):
    if resource == "persistentvolumes":
        row = {"metadata": {"name": "retained", "uid": str(uuid4())}, "spec": {
            "claimRef": {"namespace": "loom-dev", "name": "data-loom-postgres-0", "uid": str(uuid4())}}}
    else:
        row = {"metadata": {"name": "orphan", "namespace": "loom-dev", "uid": str(uuid4())}}
    preflight.rows[resource] = [row]
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


@pytest.mark.parametrize("field,value", [("uid", "replacement"), ("provisioner", "foreign.csi.example"),
    ("parameters", {"disk-type": "unapproved"}), ("volumeBindingMode", "Unknown")])
async def test_unqualified_storage_class_is_rejected(preflight, field, value):
    row = preflight.documents["/apis/storage.k8s.io/v1/storageclasses/" + preflight.config["storage_class"]]
    (row["metadata"] if field == "uid" else row)[field] = value
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


@pytest.mark.parametrize("phase", ["Available", "Released", "Failed"])
async def test_reusable_unowned_volume_cannot_become_fresh_dev_data(preflight, phase):
    # Namespace absence is insufficient: a new PVC can bind a pre-existing PV.
    preflight.rows["persistentvolumes"] = [{"metadata": {"name": "old-disk", "uid": str(uuid4())},
        "spec": {"storageClassName": preflight.config["storage_class"],
                 "capacity": {"storage": "100Gi"}, "accessModes": ["ReadWriteOnce"]},
        "status": {"phase": phase}}]
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


async def test_staging_bound_volume_is_read_only_and_not_a_collision(preflight):
    preflight.rows["persistentvolumes"] = [{"metadata": {"name": "staging-disk", "uid": str(uuid4())},
        "spec": {"storageClassName": preflight.config["storage_class"], "claimRef": {
            "namespace": "loom-nebius-platform", "name": "data-loom-postgres-0", "uid": str(uuid4())}},
        "status": {"phase": "Bound"}}]
    before = copy.deepcopy(preflight.rows)
    result = await check(preflight)
    assert result.evidence["status"] == "fresh_dev_preflight_only"
    assert preflight.rows == before


async def test_gateway_inspection_does_not_require_the_publishers_git_checkout(preflight, source_checkout):
    root, _, _ = source_checkout
    # The fixed gateway validates the prepared bundle, not an ambient checkout.
    (root / ".git").rename(root / "git-unavailable")
    result = await check(preflight)
    assert result.evidence["source_sha"] == preflight.client.settings.source.source_sha


@pytest.mark.parametrize("field,value", [("source_sha", "0" * 40), ("source_archive_sha256", "sha256:" + "0" * 64)])
async def test_other_tooling_source_cannot_run_selected_publication(preflight, field, value):
    settings = preflight.client.settings
    preflight.client.settings = settings.model_copy(update={"source": settings.source.model_copy(update={field: value})})
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)
    assert not preflight.calls


@pytest.mark.parametrize("field,value", [("source_archive_sha256", None), ("source_archive_sha256", "sha256:" + "0" * 64)])
async def test_matching_commit_name_is_not_proof_of_published_source_bytes(preflight, field, value):
    pub = preflight.publication
    pub.candidate[field] = value
    with zipfile.ZipFile(io.BytesIO(pub.payload)) as old:
        profile = old.read("runtime-profile.json")
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as archive:
        archive.writestr("candidate.json", json.dumps(pub.candidate))
        archive.writestr("runtime-profile.json", profile)
    pub.payload = data.getvalue()
    digest = "sha256:" + hashlib.sha256(pub.payload).hexdigest()
    pub.responses["actions/artifacts/123"].update(digest=digest, size_in_bytes=len(pub.payload))
    preflight.client.settings = preflight.client.settings.model_copy(update={
        "publication": preflight.client.settings.publication.model_copy(update={"artifact_sha256": digest})})
    with pytest.raises(module().DevelopmentPreflightError, match="source"):
        await check(preflight)
    assert not preflight.calls


async def test_foreign_staging_headroom_and_hpa_maximum_are_counted_without_changing_them(preflight):
    dep = workload("Deployment", "staging-api", cpu="1000m")
    dep["metadata"]["namespace"] = "loom-nebius-platform"
    preflight.rows["deployments"] = [dep]
    low = await check(preflight)
    preflight.rows["horizontalpodautoscalers"] = [{"metadata": {"namespace": "loom-nebius-platform", "name": "scale", "uid": str(uuid4())},
        "spec": {"maxReplicas": 6, "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "staging-api"}}}]
    high = await check(preflight)
    assert high.evidence["capacity"]["required"]["cpu_millis"] - low.evidence["capacity"]["required"]["cpu_millis"] == 5000
    assert dep["spec"]["replicas"] == 1
    preflight.rows["nodes"][0]["status"]["allocatable"]["cpu"] = "7"
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


async def test_terminating_owned_pod_is_additional_to_future_staging_surge(preflight):
    dep = workload("Deployment", "staging-api", cpu="1000m")
    rs = workload("ReplicaSet", "staging-api-hash", cpu="1000m")
    rs["spec"]["replicas"] = 1
    rs["metadata"]["ownerReferences"] = [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "staging-api",
                                         "uid": dep["metadata"]["uid"], "controller": True}]
    preflight.rows["deployments"] = [dep]
    preflight.rows["replicasets"] = [rs]
    low = await check(preflight)
    pod = copy.deepcopy(dep["spec"]["template"])
    pod.update(apiVersion="v1", kind="Pod", status={"phase": "Running"})
    pod["metadata"].update(name="terminating", namespace=dep["metadata"]["namespace"], uid=str(uuid4()),
        deletionTimestamp="2026-10-07T00:00:00Z", ownerReferences=[{"apiVersion": "apps/v1", "kind": "ReplicaSet",
        "name": "staging-api-hash", "uid": rs["metadata"]["uid"], "controller": True}])
    pod["spec"]["nodeName"] = "computeinstance-test"
    preflight.rows["pods"].append(pod)
    before = copy.deepcopy(preflight.rows)
    high = await check(preflight)
    assert high.evidence["capacity"]["required"]["cpu_millis"] == low.evidence["capacity"]["required"]["cpu_millis"] + 1000
    assert high.evidence["capacity"]["required"]["pods"] == low.evidence["capacity"]["required"]["pods"] + 1
    assert preflight.rows == before
    preflight.rows["nodes"][0]["status"]["allocatable"]["cpu"] = str(low.evidence["capacity"]["required"]["cpu_millis"]) + "m"
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


@pytest.mark.parametrize("kind", ["ReplicaSet", "Pod"])
async def test_visible_unsupported_owner_cannot_hide_future_controller_demand(preflight, kind):
    if kind == "ReplicaSet":
        row = workload("ReplicaSet", "custom-managed", cpu="100m")
        row["spec"]["replicas"] = 1
        preflight.rows["replicasets"] = [row]
    else:
        row = preflight.rows["pods"][0]
    row["metadata"]["ownerReferences"] = [{"apiVersion": "argoproj.io/v1alpha1", "kind": "Rollout",
        "name": "unobserved-controller", "uid": str(uuid4()), "controller": True}]
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)


@pytest.mark.parametrize("change", ["missing-page", "pagination-loop", "unsupported-controller", "unknown-hpa"])
async def test_incomplete_or_unsupported_inventory_fails_closed(preflight, change):
    if change in {"missing-page", "pagination-loop"}:
        preflight.pages["pods"] = {"apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "7"}}
        if change == "pagination-loop":
            preflight.pages["pods"].update(items=[])
            preflight.pages["pods"]["metadata"]["continue"] = "same-token"
    elif change == "unsupported-controller":
        preflight.rows["replicationcontrollers"] = [{"metadata": {"namespace": "staging", "name": "old-controller", "uid": str(uuid4())}}]
    else:
        preflight.rows["horizontalpodautoscalers"] = [{"metadata": {"namespace": "staging", "name": "foreign", "uid": str(uuid4())},
            "spec": {"maxReplicas": 3, "scaleTargetRef": {"apiVersion": "other/v1", "kind": "Custom", "name": "foreign"}}}]
    with pytest.raises(module().DevelopmentPreflightError):
        await check(preflight)
