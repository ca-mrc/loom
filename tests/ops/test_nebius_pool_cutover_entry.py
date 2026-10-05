"""Protected cutover inputs derive history, never trust a supplied manager."""
from __future__ import annotations

import base64
import copy
import hashlib
import io
import json
import zipfile
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from tests.ops.test_nebius_management_refresh_predecessor import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    application_material as application_material,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    checks as checks,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    cloud as cloud,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    history_credential,
    load,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    installation as installation,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    material as material,
)
from tests.ops.test_nebius_management_refresh_predecessor import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_cutover import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_cutover import platform_writer_authority
from tests.ops.test_nebius_pool_cutover import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.execution_image_admission import (
    IMAGE_ADMISSION_KEYRING,
    signed_image_admission_bundle,
)
from tests.unit.test_nebius_management_render import management_inputs as base_management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def management_inputs(platform_inputs):
    """Install test trust before generating completed history, never rewrite it."""
    import base64

    deployment, candidate, profile = copy.deepcopy(base_management_inputs.__wrapped__(platform_inputs))
    key = IMAGE_ADMISSION_KEYRING._keys["test-builder"]
    deployment["installation"]["keyring"] = {"schema_version": 1, "keys": [{
        "signing_key_id": "test-builder", "public_key_base64": base64.b64encode(
            key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)).decode()}]}
    return deployment, candidate, profile


@pytest.fixture
def private_cutover(completed_upgrade, cutover_inputs, database_guard):
    root = load(completed_upgrade[0])
    request, tokens = copy.deepcopy(cutover_inputs)
    migration = request.fencing.retirement.migration
    spec = migration.registration.spec.model_dump(mode="json")
    foundation = root.deployment.installation.foundation.platform_config
    spec.update(installation_id=root.upgrade.setup.binding.installation_id,
        cluster_id=foundation["cluster_id"], node_group_id=foundation["execution_node_group_id"],
        node_selector={"nebius.com/node-group-id": foundation["execution_node_group_id"]})
    for row in spec["participants"]:
        row["installation_id"] = spec["installation_id"]
    for group, field in (("execution", "runtime"), ("task_images", "target")):
        for row in spec["profiles"][group]:
            row[field]["node_selector"] = spec["node_selector"]
    _, database = database_guard
    guards = []
    for target in migration.guards:
        bound = asdict(database.target.database)
        for field in ("statefulset", "service"):
            bound[field]["metadata"].update(namespace=target.namespace, uid=str(uuid4()))
        bound["credential_uid"] = str(uuid4())
        bound["actuator_credential_uid"] = str(uuid4())
        bound["actuator_credential_resource_version"] = '9'
        guards.append({**asdict(target), "database": bound})
    development, = (row for row in migration.registration.spec.participants if row.environment_class == "development")
    directory = Path(root.selector.operation["inputs_path"]).parent.parent / "pool-cutover" / str(spec["operation_id"])
    directory.mkdir(mode=0o700, parents=True)
    token_paths = {}
    for identity, token in tokens.items():
        path = directory / ("machine-" + identity.hex)
        path.write_text(token)
        path.chmod(0o600)
        token_paths[str(identity)] = str(path)
    collector_config = copy.deepcopy(request.collector_config)
    collector_config["data"]["LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_NODE_GROUP_ID"] = spec["node_group_id"]
    candidate = copy.deepcopy(migration.registration.candidate)
    candidate.update(schema_version="loom.nebius-candidate.v1", repository="qianyi-sun/loom",
        workflow_path=".github/workflows/nebius-candidate.yml", run_id=100,
        registry_prefix=root.deployment.installation.registry_prefix)
    candidate["images"] = {key: {"image_ref": candidate["registry_prefix"] + "/" + repository + "@sha256:" + "e" * 64}
        for key, repository in {"service": "loom-service", "control_plane": "loom-control-plane", "web": "loom-web",
            "gateway": "loom-llm-gateway", "execution_runtime": "loom-execution-runtime",
            "execution_actuator": "loom-execution-actuator", "harbor_runtime": "loom-harbor-runtime"}.items()}
    for row in spec["profiles"]["execution"]:
        row["runtime_image_ref"] = candidate["images"]["execution_runtime"]["image_ref"]
    for row in spec["profiles"]["task_images"]:
        row["settings"]["service_image"] = candidate["images"]["service"]["image_ref"]
    image_admission = signed_image_admission_bundle(tuple(candidate["images"][image]["image_ref"]
        for image in ("service", "execution_runtime", "harbor_runtime"))).model_dump(mode="json")
    profiles = {}
    for identity, row in request.profiles.items():
        desired = row.model_dump(mode="json")
        for field, image in (("task_image_ref", "service"), ("runtime_image_ref", "execution_runtime"), ("agent_image_ref", "harbor_runtime")):
            desired[field] = candidate["images"][image]["image_ref"]
        desired["image_admission"] = copy.deepcopy(image_admission)
        profiles[str(identity)] = desired
    profile = copy.deepcopy(profiles[str(development.participant_id)])
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("candidate.json", json.dumps(candidate))
        bundle.writestr("runtime-profile.json", json.dumps(profile))
    publication = {"candidate_id": str(uuid4()), "source_sha": candidate["candidate_sha"],
        "run_id": 100, "run_attempt": 1, "artifact_id": 200,
        "artifact_sha256": "sha256:" + hashlib.sha256(archive.getvalue()).hexdigest(),
        "pull_request": 2301}
    payload = {"schema_version": "loom.nebius-pool-cutover-private-inputs.v1",
        "original_upgrade": root.selector.model_dump(mode="json"),
        "predecessor": root.selector.model_dump(mode="json"),
        "installation": spec, "publication": publication, "candidate": candidate, "profile": profile,
        "guards": guards, "actuators": request.fencing.retirement.actuators,
        "collectors": request.fencing.retirement.collectors, "roles": request.fencing.originals,
        "services": request.services, "collector_config": collector_config,
        "collector_credential": request.collector_credential.model_dump(mode="json"),
        "platform_authority": platform_writer_authority(root.upgrade.setup.binding.kube_system_uid).model_dump(mode="json"),
        "profiles": profiles,
        "machine_token_files": token_paths, "foundation_candidate": "5" * 40}
    metadata = {"schema": "loom.nebius-pool-cutover-operation.v1", "operation_id": spec["operation_id"],
        "source_sha": migration.registration.candidate["candidate_sha"],
        "candidate": migration.registration.candidate["candidate_sha"],
        "installation_id": spec["installation_id"], "namespace": root.upgrade.setup.binding.namespace,
        "state_dir": str(directory / "state"), "anchor_dir": str(directory / "anchor"),
        "inputs_path": str(directory / "inputs.json"), "inputs_sha256": ""}
    save_private(metadata, payload)
    return metadata, payload, root


@pytest.fixture
def publication_http(private_cutover, monkeypatch):
    """Double GitHub/blob HTTPS only; the real catalog checks every proof."""
    _, inputs, root = private_cutover
    reference, candidate = inputs["publication"], inputs["candidate"]
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("candidate.json", json.dumps(candidate))
        bundle.writestr("runtime-profile.json", json.dumps(inputs["profile"]))
    # ZIP timestamps may change between fixture creation and transport setup.
    payload = archive.getvalue()
    reference["artifact_sha256"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    save_private(private_cutover[0], inputs)
    repo = {"full_name": "qianyi-sun/loom", "id": 1281629473}
    source, head = candidate["candidate_sha"], "b" * 40
    responses = {
        "actions/runs/100/attempts/1": {"id": 100, "run_attempt": 1, "head_sha": source,
            "head_branch": "dev", "repository": repo, "head_repository": repo,
            "status": "completed", "conclusion": "success", "event": "push",
            "path": ".github/workflows/nebius-candidate.yml"},
        "pulls/2301": {"number": 2301, "merged": True, "state": "closed", "merge_commit_sha": source,
            "base": {"ref": "dev", "repo": repo}, "head": {"sha": head, "repo": repo}},
        "commits/" + head + "/check-runs": {"total_count": 4, "check_runs": [
            {"id": index + 100, "name": name, "head_sha": head, "status": "completed", "conclusion": "success",
                "app": {"id": 15368, "slug": "github-actions"}} for index, name in enumerate((
                    "repository-checks", "images-gate", "cluster-smoke-gate", "staging-smoke-gate"))]},
        "actions/artifacts/200": {"id": 200, "name": "nebius-candidate-" + source + "-100-1",
            "expired": False, "size_in_bytes": len(payload), "digest": reference["artifact_sha256"],
            "workflow_run": {"id": 100, "head_sha": source, "head_branch": "dev",
                "repository_id": repo["id"], "head_repository_id": repo["id"]}}}
    state = {"payload": payload, "requests": []}
    def respond(request):
        state["requests"].append(request)
        assert request.method == "GET"
        if request.url.host == "api.github.com":
            assert request.headers["Authorization"] == "Bearer " + root.upgrade.original.material["loom-management-publications"]["token"]
            path = request.url.path.removeprefix("/repos/qianyi-sun/loom/")
            if path == "actions/artifacts/200/zip":
                return httpx.Response(302, headers={"Location": "https://loom.blob.core.windows.net/artifact?sig=private-marker"})
            return httpx.Response(200, json=responses[path])
        assert request.url.host == "loom.blob.core.windows.net" and "Authorization" not in request.headers
        if state.get("during_read"):
            state.pop("during_read")()
        return httpx.Response(200, content=state["payload"])
    client = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: client(transport=transport, **kwargs))
    return responses, state


def save_private(metadata, payload):
    path = Path(metadata["inputs_path"])
    path.write_text(json.dumps(payload, default=str))
    path.chmod(0o600)
    metadata["inputs_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("damage", [None, "extra_field", "running", "database"])
def test_private_cutover_loads_only_explicit_qualified_dormant_consumers(private_cutover, damage):
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_cutover import cutover_documents, retained_cutover_workloads
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs
    from tests.ops.test_nebius_pool_dormant import dormant_consumer

    metadata, payload, _ = private_cutover
    original = load_pool_cutover_inputs(metadata)
    consumer = dormant_consumer(original.request.fencing.retirement)
    value = asdict(consumer)
    if damage == "extra_field":
        value["allow_remote_write"] = True
    elif damage == "running":
        value["actuator"]["spec"]["replicas"] = 1
    elif damage == "database":
        settings = {row["name"]: row for row in value["actuator"]["spec"]["template"]["spec"]["containers"][0]["env"]}
        settings["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"]["secretKeyRef"]["name"] = "foreign-database"
    payload["dormant_consumers"] = [value]
    save_private(metadata, payload)
    if damage:
        with pytest.raises(EntryError):
            load_pool_cutover_inputs(metadata)
        return
    context = load_pool_cutover_inputs(metadata)
    assert context.request.fencing.retirement.dormant_consumers == (consumer,)
    runtime = cutover_documents(context.request)["runtime"]
    retained = retained_cutover_workloads(context.request,
        state_dir=Path(metadata["state_dir"]), anchor_dir=Path(metadata["anchor_dir"]))
    for kind, document in (("Deployment", consumer.actuator), ("CronJob", consumer.collector)):
        key = kind + ":" + document["metadata"]["namespace"] + ":" + document["metadata"]["name"]
        assert key in retained and key not in runtime
        assert retained[key]["metadata"]["uid"] == document["metadata"]["uid"]


@pytest.fixture
def connected_cutover_entry(private_cutover, monkeypatch):
    """The already-qualified reader transport is doubled, not the new assembly."""
    import ssl
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_cutover_entry as entry

    context = entry.load_pool_cutover_inputs(private_cutover[0])
    migration = context.request.fencing.retirement.migration
    observed = {"probes": [], "guard_calls": [], "guard_status": "held", "failure": None, "closed": False,
        "database_reads": [], "database_failure": None}
    def guard(target, action):
        assert target in migration.guards and action in {"acquire", "observe"}
        observed["guard_calls"].append((target.participant_id, action))
        return {"status": observed["guard_status"]}
    def history_binding(actual, manager):
        assert actual == migration and manager == context.request.manager
    def database_page(target, *, after):
        assert target in migration.guards and after is None
        observed["database_reads"].append("participant")
        return {"status": "observed", "schema_revision": "0171" if observed["database_failure"] == "participant" else "0174", "rows": []}
    def history_page(target, origins):
        assert target in migration.guards and origins == ()
        observed["database_reads"].append("manager")
        if observed["database_failure"] == "manager":
            raise ValueError("pool management history schema unqualified")
    readers = entry.ConnectedPoolReaders(
        base=SimpleNamespace(api_server=context.original.original_inputs.operator_connection.endpoint),
        ssl_context=ssl.create_default_context(), token="entry-test-operator-token",
        guards=SimpleNamespace(request=migration, guard=guard, cutover_readiness_page=database_page),
        history=SimpleNamespace(qualify_binding=history_binding, qualify_pending_origins=history_page))
    @contextmanager
    def connect(actual):
        assert actual == context
        try:
            yield readers
        finally:
            observed["closed"] = True
    monkeypatch.setattr(entry, "connected_pool_readers", connect)
    def probe(name, expected):
        def qualify(actual, reader):
            assert actual == context and reader is expected
            observed["probes"].append(name)
            if observed["failure"] == name:
                raise entry.EntryError("private-probe-marker")
            if observed["failure"] == "during_read" and name == "provider":
                path = Path(context.operation["inputs_path"])
                path.write_bytes(path.read_bytes() + b"\n")
        return qualify
    monkeypatch.setattr(entry, "qualify_pool_runtime_databases", probe("participants", readers.guards))
    monkeypatch.setattr(entry, "qualify_pool_manager_database", probe("manager", readers.history))
    monkeypatch.setattr(entry, "qualify_pool_provider", probe("provider", readers.base))
    return context, readers, observed


@pytest.mark.parametrize("boundary", [None, "participant", "manager"])
def test_concrete_migration_and_parent_qualify_databases_before_downtime(connected_cutover_entry, boundary):
    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from tests.ops.test_nebius_pool_cutover import (
        cutover_binding_inventory,
        writer_workload_inventory,
    )

    context, _, observed = connected_cutover_entry
    request = context.request
    migration = request.fencing.retirement.migration
    binding = migration.registration.binding
    observed["database_failure"] = boundary
    inventories = cutover_binding_inventory.__wrapped__((request, context.tokens)) | writer_workload_inventory(request)
    for documents in inventories.values():
        for document in documents:
            # Completed history omits server versions; a real API inventory
            # always supplies one. Do not mutate the retained input snapshot.
            document["metadata"].setdefault("resourceVersion", "7")
    namespaces = {binding.namespace: binding.namespace_uid, "kube-system": binding.kube_system_uid,
        **{row.namespace: str(row.namespace_uid) for row in migration.guards},
        **{ns.name: str(ns.uid) for row in migration.registration.spec.participants for ns in (row.execution_namespace, row.build_namespace)}}
    methods = []
    kinds = {"roles": "Role", "clusterroles": "ClusterRole", "rolebindings": "RoleBinding", "clusterrolebindings": "ClusterRoleBinding",
        "deployments": "Deployment", "replicasets": "ReplicaSet", "statefulsets": "StatefulSet", "daemonsets": "DaemonSet",
        "replicationcontrollers": "ReplicationController", "cronjobs": "CronJob", "jobs": "Job", "pods": "Pod"}
    def respond(message):
        methods.append(message.method)
        assert message.method == "GET"
        name = message.url.path.rsplit("/", 1)[-1]
        if message.url.path.startswith("/api/v1/namespaces/"):
            return httpx.Response(200, json={"apiVersion": "v1", "kind": "Namespace", "metadata": {
                "name": name, "uid": namespaces[name], "labels": {"loom.nebius/management-installation": binding.installation_id,
                    "pod-security.kubernetes.io/enforce": "restricted"}}})
        version = "rbac.authorization.k8s.io/v1" if "role" in name else "batch/v1" if name in {"cronjobs", "jobs"} else (
            "v1" if name in {"pods", "replicationcontrollers"} else "apps/v1")
        return httpx.Response(200, json={"apiVersion": version, "kind": kinds[name] + "List",
            "metadata": {"resourceVersion": "7"}, "items": inventories[name]})
    with entry.connected_pool_api(context) as api:
        api.client.close()
        api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(respond))
        if boundary:
            with pytest.raises(ValueError, match="preserve_evidence"):
                stage_pool_cutover(request=request, tokens=context.tokens, api=api,
                    state_dir=Path(context.operation["state_dir"]), anchor_dir=Path(context.operation["anchor_dir"]))
        else:
            api.migration.preflight(migration)
    assert observed["database_reads"] == (["participant"] if boundary == "participant" else
        ["participant", "manager"] if boundary == "manager" else ["participant", "manager"] * len(migration.guards))
    assert observed["guard_calls"] == [] and observed["closed"]
    assert methods and all(method == "GET" for method in methods)
    assert not Path(context.operation["state_dir"]).exists()


@pytest.mark.parametrize("damage", [None, "participants", "manager", "provider", "private_inputs", "during_read"])
def test_concrete_cutover_checks_requalify_the_bound_readers(connected_cutover_entry, damage):
    from scripts.ops import nebius_pool_cutover_entry as entry

    context, _, observed = connected_cutover_entry
    with entry.connected_pool_api(context) as api:
        assert api.state_dir == Path(context.operation["state_dir"])
        assert api.anchor_dir == Path(context.operation["anchor_dir"])
        observed["failure"] = damage
        if damage == "private_inputs":
            path = Path(context.operation["inputs_path"])
            path.write_bytes(path.read_bytes() + b"\n")
        if damage:
            with pytest.raises(entry.EntryError) as error:
                api.checks.preflight(context.request)
            assert "private-probe-marker" not in str(error.value)
        else:
            api.checks.preflight(context.request)
            assert observed["probes"] == ["participants", "manager", "provider"]
            api.checks.qualify_quiescence()
            assert observed["probes"] == ["participants", "manager", "provider"] * 2
        assert not Path(context.operation["state_dir"]).exists()
        assert observed["guard_calls"] == []
    assert observed["closed"]
    assert all(transport.client.is_closed for transport in (api, api.retirement, api.fencing, api.migration.registration))


@pytest.mark.parametrize('stage', ['startup', 'activation'])
def test_connected_startup_reuses_the_exact_parent_transport_without_mutations(connected_cutover_entry, stage):
    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from tests.ops.test_nebius_pool_cutover import CutoverAPI

    context, readers, observed = connected_cutover_entry
    closed = CutoverAPI(context.request)
    for document in closed.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    state, anchor = Path(context.operation['state_dir']), Path(context.operation['anchor_dir'])
    assert stage_pool_cutover(request=context.request, tokens=context.tokens, api=closed,
        state_dir=state, anchor_dir=anchor)['status'] == 'pool_runtime_staged_closed'
    before = {path: path.read_bytes() for path in state.rglob('*.json')}
    connect = entry.connected_pool_startup_api if stage == 'startup' else entry.connected_pool_activation_api
    with connect(context) as startup:
        parent = startup.parent
        assert startup.request == context.request and startup.state == state and startup.anchor == anchor
        assert parent.guards is readers.guards and parent.history is readers.history
        assert not observed['closed']
        assert not (state / 'startup.json').exists()
        assert not (state / 'activation.json').exists()
        assert {path: path.read_bytes() for path in state.rglob('*.json')} == before
    assert observed['closed'] and all(transport.client.is_closed for transport in (
        parent, parent.retirement, parent.fencing, parent.migration.registration))


@pytest.mark.parametrize("failure", [None, "before", "after"])
def test_connected_registration_stages_once_and_does_not_confuse_creation_with_success(connected_cutover_entry, failure):
    from scripts.ops import nebius_pool_cutover_entry as entry
    from tests.ops.test_nebius_management_stage import PhaseAPI

    context, _, observed = connected_cutover_entry
    state = Path(context.operation["state_dir"]) / "writers" / "registration"
    # The journaled parent creates these private phase directories before
    # invoking its registration child. This test isolates that child's effects.
    state.parent.parent.mkdir(mode=0o700)
    state.parent.mkdir(mode=0o700)
    fake = PhaseAPI(context.request.fencing.retirement.migration.registration.binding)
    fake.failure = failure
    with entry.connected_pool_api(context) as api:
        # Only external Kubernetes reads/writes are doubled. The fixed stage,
        # journal, uncertain-create recovery and registration proof are real.
        registration = api.migration.registration
        for name in ("verify_identity", "get_resource", "default_resource", "create_resource"):
            setattr(registration, name, getattr(fake, name))
        if failure == "before":
            for _ in range(2):
                with pytest.raises(ValueError):
                    api.migration.register(state)
            assert len(fake.creates) == 1
        else:
            assert api.migration.register(state) is None
            assert api.migration.register(state) is None
            assert len(fake.creates) == 2
            record = json.loads((state / "stage.json").read_text())
            assert record["phase"] == "pool-registration"
            assert {row["desired"]["kind"] for row in record["resources"].values()} == {"ConfigMap", "Job"}
            assert all(row["status"] == "created" for row in record["resources"].values())
        assert observed["guard_calls"] and all(action == "observe" for _, action in observed["guard_calls"])


@pytest.mark.parametrize("damage", ["path", "guard_open", "private_inputs"])
def test_connected_registration_refuses_unbound_or_open_state_without_writes(connected_cutover_entry, damage):
    from scripts.ops import nebius_pool_cutover_entry as entry

    context, _, observed = connected_cutover_entry
    state = Path(context.operation["state_dir"]) / "writers" / "registration"
    with entry.connected_pool_api(context) as api:
        if damage == "path":
            state = state.parent / "foreign-registration"
        elif damage == "guard_open":
            observed["guard_status"] = "open"
        else:
            path = Path(context.operation["inputs_path"])
            path.write_bytes(path.read_bytes() + b"\n")
        with pytest.raises(entry.EntryError):
            api.migration.register(state)
        assert not state.exists()


def test_private_cutover_derives_the_manager_and_keeps_history_read_only(private_cutover):
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

    metadata, payload, root = private_cutover
    before = {path: path.read_bytes() for path in root.history}
    context = load_pool_cutover_inputs(metadata)
    assert context.request.manager == root.active
    assert context.request.management_origin == "https://" + root.deployment.public_host
    assert context.request.kubernetes_endpoint == "https://kubernetes.default.svc"
    assert context.request.fencing.retirement.migration.registration.binding == root.upgrade.setup.binding
    assert context.request.platform_authority.model_dump(mode="json") == payload["platform_authority"]
    assert context.request.collector_credential.model_dump(mode="json") == payload["collector_credential"]
    assert len(context.tokens) == len(payload["machine_token_files"])
    assert {path: path.read_bytes() for path in root.history} == before
    assert not Path(metadata["state_dir"]).exists()


def test_private_pool_operation_reloads_inputs_before_transport_and_preflight_is_readonly(private_cutover, monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_management_entry import EntryError
    from tests.ops.test_nebius_pool_cutover import CutoverAPI

    metadata, _, _ = private_cutover
    selected = entry.load_pool_cutover_inputs(metadata)
    parent = CutoverAPI(selected.request)
    parent.state_dir, parent.anchor_dir, parent.refresh = Path(metadata['state_dir']), Path(metadata['anchor_dir']), None
    telemetry = {'status': 'unavailable', 'checks': 2, 'unavailable': 1, 'reasons': ['tls_kubelet_verify_19']}
    parent.guards = SimpleNamespace(telemetry_report=lambda: telemetry)
    opened = []

    @contextmanager
    def connect(context):
        assert context == selected
        opened.append('opened')
        try:
            yield parent
        finally:
            opened.append('closed')

    monkeypatch.setattr(entry, 'connected_pool_api', connect)
    assert entry.execute_pool_cutover(selected, 'preflight') == {
        'status': 'preflight_qualified', 'operation_id': metadata['operation_id'], 'telemetry': telemetry}
    assert not parent.state_dir.exists() and not parent.anchor_dir.exists()
    assert opened == ['opened', 'closed']
    with pytest.raises(EntryError):
        entry.execute_pool_cutover(selected, 'open')
    path = Path(metadata['inputs_path'])
    path.write_bytes(path.read_bytes() + b'\n')
    with pytest.raises(EntryError):
        entry.execute_pool_cutover(selected, 'install')
    assert opened == ['opened', 'closed'] and not parent.state_dir.exists()


def test_protected_main_routes_pool_preflight_through_bound_operation(private_cutover, monkeypatch, capsys):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_management_entry as management
    from scripts.ops import nebius_pool_cutover_entry as entry
    from tests.ops.test_nebius_pool_cutover import CutoverAPI

    metadata, _, _ = private_cutover
    selected = entry.load_pool_cutover_inputs(metadata)
    parent = CutoverAPI(selected.request)
    parent.state_dir, parent.anchor_dir, parent.refresh = Path(metadata['state_dir']), Path(metadata['anchor_dir']), None
    telemetry = {'status': 'unavailable', 'checks': 2, 'unavailable': 1, 'reasons': ['tls_kubelet_verify_19']}
    parent.guards = SimpleNamespace(telemetry_report=lambda: telemetry)
    path = Path(metadata['inputs_path']).with_name('operation.json')
    path.write_text(json.dumps(metadata))
    path.chmod(0o600)

    @contextmanager
    def connect(context):
        assert context == selected
        yield parent

    monkeypatch.setattr(entry, 'connected_pool_api', connect)
    assert management.main(str(path), 'preflight') == 0
    assert json.loads(capsys.readouterr().out) == {'status': 'preflight_qualified', 'telemetry': telemetry,
        **{key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}}
    assert not parent.state_dir.exists() and not parent.anchor_dir.exists()


def test_private_cutover_requires_a_bound_collector_credential(private_cutover):
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

    metadata, payload, _ = private_cutover
    payload.pop('collector_credential', None)
    save_private(metadata, payload)
    with pytest.raises(EntryError):
        load_pool_cutover_inputs(metadata)
    assert not Path(metadata['state_dir']).exists()


@pytest.mark.parametrize(('message', 'stage'), [
    ('pool cutover publication unqualified', 'pool_publication'),
    ('pool cutover operator readers unqualified', 'pool_operator_readers'),
    ('pool cutover runtime databases unqualified', 'pool_runtime_databases'),
    ('pool cutover runtime telemetry unqualified', 'pool_runtime_telemetry'),
    *(('pool cutover runtime telemetry ' + detail + ' unqualified', 'pool_runtime_telemetry_' + detail)
        for detail in ('binding', 'pod', 'nodes', 'probe', 'recheck', 'settings', 'client',
            'tls', 'authorization', 'network', 'http', 'reader', 'counters', 'close',
            'identity', 'address', 'authority', 'payload', 'tls_api', 'tls_kubelet',
            'tls_api_verify_20', 'tls_kubelet_verify_64', 'tls_unknown_verify_10',
            'tls_kubelet_verify_0', 'tls_kubelet_verify_255')),
    ('pool cutover runtime telemetry tls_kubelet_verify_256 unqualified', 'pool_connection'),
    ('pool cutover runtime telemetry tls_api_verify_-1 unqualified', 'pool_connection'),
    ('pool cutover runtime telemetry tls_private-marker unqualified', 'pool_connection'),
    ('pool cutover management database unqualified', 'pool_management_database'),
    ('pool cutover provider unqualified', 'pool_provider'),
    ('pool cutover connected scope unqualified', 'pool_connected_scope'),
    ('pool cutover context changed before connection', 'pool_private_inputs'),
    ('pool cutover context changed during publication', 'pool_private_inputs'),
    ('private-token: https://private.invalid/secret', 'pool_connection'),
    ('pool cutover publication unqualified private-token', 'pool_connection'),
])
def test_pool_prerequisite_failure_reports_only_closed_stage_without_installing(
    private_cutover, monkeypatch, capsys, message, stage,
):
    """Discarding a known prerequisite code must not turn it into an opaque connection failure."""
    from contextlib import contextmanager

    from scripts.ops import nebius_management_entry as management
    from scripts.ops import nebius_pool_cutover_entry as entry

    metadata, _, _ = private_cutover
    selected = entry.load_pool_cutover_inputs(metadata)
    path = Path(metadata['inputs_path']).with_name('operation.json')
    path.write_text(json.dumps(metadata))
    path.chmod(0o600)

    @contextmanager
    def connect(context):
        assert context == selected
        raise management.EntryError(message)
        yield  # pragma: no cover - context manager protocol, never entered

    monkeypatch.setattr(entry, 'connected_pool_api', connect)
    monkeypatch.setattr(entry, 'run_pool_operation', lambda **kwargs: pytest.fail('operation entered after failed prerequisites'))
    # The inner protocol acknowledges delivery; the existing outer CLI rejects blocked.
    assert management.main(str(path), 'preflight') == 0
    output = capsys.readouterr()
    assert json.loads(output.out) == {'status': 'blocked', 'stage': stage,
        **{key: metadata[key] for key in ('source_sha', 'candidate', 'installation_id', 'namespace', 'operation_id')}}
    assert not output.err and 'private-token' not in output.out
    assert not Path(metadata['state_dir']).exists() and not Path(metadata['anchor_dir']).exists()


@pytest.mark.parametrize("damage", ["hash", "extra_manager", "source", "installation", "cluster", "pool",
    "missing_database", "missing_actuator_credential", "partial_actuator_credential",
    "token_hash", "token_alias", "token_symlink", "token_public", "path", "publication",
    "missing_platform", "platform_cluster", "platform_native_grant"])
def test_private_cutover_rejects_unbound_inputs_before_transport_or_downtime(private_cutover, damage):
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

    metadata, payload, root = private_cutover
    if damage == "hash":
        metadata["inputs_sha256"] = "0" * 64
    elif damage == "extra_manager":
        payload["manager"] = {"private-marker": "foreign"}
    elif damage == "source":
        metadata["source_sha"] = "a" * 40
    elif damage == "installation":
        metadata["installation_id"] = str(uuid4())
    elif damage == "cluster":
        payload["installation"]["cluster_id"] = "foreign-cluster"
    elif damage == "pool":
        payload["installation"]["node_group_id"] = "foreign-group"
        payload["installation"]["node_selector"]["nebius.com/node-group-id"] = "foreign-group"
    elif damage == "missing_database":
        payload["guards"][0]["database"] = None
    elif damage == "missing_actuator_credential":
        payload["guards"][0]["database"].pop("actuator_credential_uid", None)
        payload["guards"][0]["database"].pop("actuator_credential_resource_version", None)
    elif damage == "partial_actuator_credential":
        payload["guards"][0]["database"].pop("actuator_credential_resource_version", None)
    elif damage == "token_hash":
        Path(next(iter(payload["machine_token_files"].values()))).write_text("private-marker")
    elif damage == "token_alias":
        payload["machine_token_files"][next(iter(payload["machine_token_files"]))] = str(root.original_inputs.operator_connection.credentials_file)
    elif damage == "token_symlink":
        identity, path = next(iter(payload["machine_token_files"].items()))
        link = Path(path).with_suffix(".link")
        link.symlink_to(path)
        payload["machine_token_files"][identity] = str(link)
    elif damage == "token_public":
        Path(next(iter(payload["machine_token_files"].values()))).chmod(0o644)
    elif damage == "path":
        metadata["state_dir"] = str(Path(metadata["state_dir"]).parent / "foreign-state")
    elif damage == "missing_platform":
        payload.pop("platform_authority")
    elif damage == "platform_cluster":
        payload["platform_authority"]["kube_system_uid"] = str(uuid4())
    elif damage == "platform_native_grant":
        payload["platform_authority"]["resources"][2]["rules"][0]["verbs"].append("create")
    else:
        payload["publication"]["source_sha"] = "a" * 40
    if damage != "hash":
        save_private(metadata, payload)
    with pytest.raises(EntryError) as error:
        load_pool_cutover_inputs(metadata)
    assert "private-marker" not in str(error.value)
    assert not Path(metadata["state_dir"]).exists()


def test_connected_readers_use_one_explicit_operator_authority_and_erase_temporary_token(private_cutover, publication_http, monkeypatch):
    import base64
    from contextlib import contextmanager

    from scripts.ops import nebius_pool_cutover_entry as entry

    metadata, _, root = private_cutover
    context = entry.load_pool_cutover_inputs(metadata)
    calls = []
    class Base:
        def _request(self, method, path):
            calls.append((method, path))
            assert method == "GET" and path == "/api/v1/namespaces/" + root.upgrade.setup.binding.namespace + "/secrets/loom-platform-db"
            return history_credential(root)
    @contextmanager
    def connect(inputs, ingress, *, foundation_candidate):
        assert inputs == root.original_inputs and ingress == root.ingress and foundation_candidate == "5" * 40
        yield Base(), object(), "bounded-private-operator-token"
    monkeypatch.setattr(entry, "connected_checks", connect)
    # This test owns transport lifetime; separate entry tests execute the runtime gate.
    monkeypatch.setattr(entry, "qualify_pool_runtime_databases", lambda *_args: None)
    monkeypatch.setattr(entry, "qualify_pool_manager_database", lambda *_args: None)
    monkeypatch.setattr(entry, "qualify_pool_provider", lambda *_args: None, raising=False)
    before = {path: path.read_bytes() for path in root.history}
    with entry.connected_pool_readers(context) as connected:
        path = connected.guards.kubeconfig
        config = json.loads(path.read_bytes())
        assert path.stat().st_mode & 0o777 == 0o600
        assert config["clusters"] == [{"name": "loom-pool", "cluster": {
            "server": root.original_inputs.operator_connection.endpoint,
            "certificate-authority-data": base64.b64encode(root.original_inputs.operator_connection.ca_file.read_bytes()).decode()}}]
        assert config["users"] == [{"name": "loom-pool-operator", "user": {"token": "bounded-private-operator-token"}}]
        assert config["current-context"] == "loom-pool"
        assert connected.history.kubeconfig == connected.guards.kubeconfig
        assert connected.history.target.controller == context.request.manager
        connected.history.qualify_binding(connected.guards.request, context.request.manager)
    assert not path.exists()
    assert len(calls) == 1
    assert {path: path.read_bytes() for path in root.history} == before
    assert not Path(metadata["state_dir"]).exists()


def test_reader_connection_rechecks_inputs_before_obtaining_operator_credentials(private_cutover, monkeypatch):
    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_management_entry import EntryError

    metadata, _, _ = private_cutover
    context = entry.load_pool_cutover_inputs(metadata)
    Path(metadata["inputs_path"]).write_bytes(Path(metadata["inputs_path"]).read_bytes() + b"\n")
    monkeypatch.setattr(entry, "connected_checks", lambda *args, **kwargs: pytest.fail("operator connection occurred"))
    with pytest.raises(EntryError):
        with entry.connected_pool_readers(context):
            pytest.fail("changed inputs were accepted")


def test_reader_context_preserves_parent_diagnostics_and_erases_credentials_on_failure(private_cutover, publication_http, monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    metadata, _, root = private_cutover
    context = entry.load_pool_cutover_inputs(metadata)
    @contextmanager
    def connect(*args, **kwargs):
        yield SimpleNamespace(_request=lambda *args: history_credential(root)), object(), "private-operator-token"
    monkeypatch.setattr(entry, "connected_checks", connect)
    monkeypatch.setattr(entry, "qualify_pool_runtime_databases", lambda *_args: None)
    monkeypatch.setattr(entry, "qualify_pool_manager_database", lambda *_args: None)
    monkeypatch.setattr(entry, "qualify_pool_provider", lambda *_args: None, raising=False)
    with pytest.raises(PoolMigrationError) as error:
        with entry.connected_pool_readers(context) as connected:
            path = connected.guards.kubeconfig
            raise PoolMigrationError("management_origin_history")
    assert error.value.stage == "management_origin_history"
    assert not path.exists()


@pytest.mark.parametrize("damage", [None, "controller", "service", "actuator", "unrecorded_stop", "manager", "provider", "telemetry",
    "telemetry_tls", "telemetry_tls_detail", "telemetry_unknown"])
def test_connected_entry_qualifies_all_runtime_consumers_before_returning_operator_access(private_cutover, publication_http, monkeypatch, damage):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_migration import PoolMigrationError
    from scripts.ops.nebius_pool_operation import PoolOperationError

    metadata, _, root = private_cutover
    context = entry.load_pool_cutover_inputs(metadata)
    migration = context.request.fencing.retirement.migration
    originals = [context.request.manager, *(row.controller for row in migration.guards), *context.request.services,
        *context.request.fencing.retirement.actuators]
    by_name = {(row['metadata']['namespace'], row['metadata']['name']): row for row in originals}
    checked, kubeconfigs, physical, telemetry = [], [], [], []
    @contextmanager
    def connect(*args, **kwargs):
        yield SimpleNamespace(_request=lambda *args: history_credential(root)), object(), 'operator-fixture-token'

    def get(self, kind, name, namespace=None):
        assert kind == 'deployment'
        document = copy.deepcopy(by_name[namespace, name])
        if damage == 'unrecorded_stop' and name == 'loom-service':
            document['spec']['replicas'] = 0
        return document

    def probe(self, target, *, original, credential_uid, credential_resource_version):
        # Remote runtime/SQL I/O is covered separately. This boundary proves
        # every original is selected with the correct independently pinned Secret.
        assert target in migration.guards and original in originals
        actuator = original['metadata']['namespace'] != target.namespace
        expected = (target.database.actuator_credential_uid, target.database.actuator_credential_resource_version) if actuator else (
            target.database.credential_uid, target.database.credential_resource_version)
        assert (credential_uid, credential_resource_version) == expected
        checked.append((original['metadata']['namespace'], original['metadata']['name']))
        kubeconfigs.append(self.kubeconfig)
        component = 'actuator' if actuator else 'controller' if original['metadata']['name'] == 'loom-control-plane' else 'service'
        if damage == component:
            raise PoolMigrationError('runtime_database')

    def manager_probe(self):
        assert self.target.controller == context.request.manager
        checked.append((self.target.namespace, 'loom-service'))
        kubeconfigs.append(self.kubeconfig)
        if damage == 'manager':
            raise PoolMigrationError('management_runtime_database')

    def provider_probe(selected, base):
        assert selected == context
        physical.append(True)
        if damage == 'provider':
            raise EntryError('pool cutover provider unqualified')

    def telemetry_probe(self, target, *, original):
        assert target in migration.guards and original in context.request.fencing.retirement.actuators
        telemetry.append(original)
        if damage == 'telemetry':
            raise PoolMigrationError('runtime_telemetry')
        if damage in {'telemetry_tls', 'telemetry_unknown'}:
            raise PoolMigrationError('runtime_telemetry_' + ('tls' if damage == 'telemetry_tls' else 'private-token'))
        if damage == 'telemetry_tls_detail':
            raise PoolMigrationError('runtime_telemetry_tls_kubelet_verify_20')

    monkeypatch.setattr(entry, 'connected_checks', connect)
    monkeypatch.setattr(entry.KubectlPoolGuardAPI, '_get', get)
    monkeypatch.setattr(entry.KubectlPoolGuardAPI, 'qualify_runtime_database', probe)
    monkeypatch.setattr(entry.KubectlPoolHistoryAPI, 'qualify_manager_database', manager_probe, raising=False)
    monkeypatch.setattr(entry, 'qualify_pool_provider', provider_probe, raising=False)
    monkeypatch.setattr(entry.KubectlPoolGuardAPI, 'qualify_runtime_telemetry', telemetry_probe, raising=False)
    if damage:
        with pytest.raises(PoolOperationError) as error:
            entry.execute_pool_cutover(context, 'preflight')
        expected_stage = {'manager': 'pool_management_database', 'provider': 'pool_provider',
            'telemetry': 'pool_runtime_telemetry', 'telemetry_tls': 'pool_runtime_telemetry_tls',
            'telemetry_tls_detail': 'pool_runtime_telemetry_tls_kubelet_verify_20'}.get(damage, 'pool_runtime_databases')
        assert error.value.stage == expected_stage
    else:
        with entry.connected_pool_readers(context):
            assert set(checked) == set(by_name)
            assert physical == [True]
            assert telemetry == list(context.request.fencing.retirement.actuators)
        assert len(checked) == len(by_name)
    assert all(not path.exists() for path in kubeconfigs)
    assert not Path(metadata['state_dir']).exists()


def _cancel_and_fence_recovery(context, metadata, closed, successor):
    from scripts.ops.nebius_pool_activation_stage import advance_pool_activation
    from scripts.ops.nebius_pool_startup_fence import fence_pool_startup
    from tests.ops.test_nebius_pool_startup_fence import FenceAPI

    state, anchor = Path(metadata['state_dir']), Path(metadata['anchor_dir'])
    api = FenceAPI((context.request, context.tokens, closed, successor, None, state.parent))
    api.state = state
    assert advance_pool_activation(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor, cancel=True)['status'] == 'pool_activation_cancelled'
    assert fence_pool_startup(request=context.request, api=api, state_dir=state,
        anchor_dir=anchor)['status'] == 'startup_writes_fenced'


@pytest.mark.parametrize('damage', [None, 'running_again', 'backend', 'copied_secret_drift'])
@pytest.mark.parametrize('startup', [None, 'started', 'uncertain', 'fenced'])
def test_entry_recovery_qualifies_stopped_rewired_references_without_exec_in_retired_pods(private_cutover, damage, startup):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from tests.ops.test_nebius_pool_cutover import CutoverAPI

    metadata, _, _ = private_cutover
    context = entry.load_pool_cutover_inputs(metadata)
    api = CutoverAPI(context.request)
    # Completed history retains identity/templates, not a current API revision.
    for document in api.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    stage_pool_cutover(request=context.request, tokens=context.tokens, api=api,
        state_dir=Path(metadata['state_dir']), anchor_dir=Path(metadata['anchor_dir']))
    migration = context.request.fencing.retirement.migration
    by_key = api.documents
    if startup:
        from scripts.ops.nebius_pool_startup import stage_pool_startup
        from tests.ops.test_nebius_pool_startup import StartupAPI

        successor = StartupAPI(context.request, api, Path(metadata['state_dir']))
        if startup in {'uncertain', 'fenced'}:
            successor.fail_key = _key(migration.guards[0].controller)
            successor.failure = 'before'
        result = stage_pool_startup(request=context.request, api=successor,
            state_dir=Path(metadata['state_dir']), anchor_dir=Path(metadata['anchor_dir']))
        assert result['status'] == ('pending_startup_outcome' if startup in {'uncertain', 'fenced'} else 'pool_startup_staged_closed')
        by_key = successor.documents
        if startup == 'uncertain':
            by_key[successor.fail_key]['spec']['replicas'] = 1  # The delayed CAS commits.
        elif startup == 'fenced':
            _cancel_and_fence_recovery(context, metadata, api, successor)
    reads = []
    def get(kind, name, namespace=None):
        assert kind == 'deployment'
        value = copy.deepcopy(by_key['Deployment:' + namespace + ':' + name])
        if damage == 'running_again' and name == 'loom-control-plane':
            value['spec']['replicas'] = 2 if startup else 1
        return value

    def credential(original, *, url_variable, credential_uid, credential_resource_version):
        reads.append(_key(original))
        namespace = original['metadata']['namespace']
        target, = (row for row in migration.guards if row.namespace == namespace or any(
            participant.participant_id == row.participant_id and participant.execution_namespace.name == namespace
            for participant in migration.registration.spec.participants))
        actuator = namespace != target.namespace
        binding = target.database
        assert (credential_uid, credential_resource_version) == ((binding.actuator_credential_uid, binding.actuator_credential_resource_version)
            if actuator else (binding.credential_uid, binding.credential_resource_version))
        host = 'loom-postgres.' + target.namespace + '.svc'
        if damage == 'backend' or (damage == 'copied_secret_drift' and actuator and reads.count(_key(original)) > 1):
            host = 'foreign.svc'
        return 'postgresql+psycopg://fixture:private-marker@' + host + ':5432/loom'

    guards = SimpleNamespace(request=migration, _get=get,
        _database=lambda target: {'metadata': {'uid': target.database.statefulset['metadata']['uid']}},
        _workload_database_url=credential,
        qualify_runtime_database=lambda *args, **kwargs: pytest.fail('attempted exec in a retired Pod'))
    if damage:
        with pytest.raises(EntryError):
            entry.qualify_pool_runtime_databases(context, guards)
    else:
        entry.qualify_pool_runtime_databases(context, guards)
        assert len(set(reads)) == 3 * len(migration.guards)


@pytest.mark.parametrize('damage', [None, 'running_again', 'backend', 'credential_drift'])
@pytest.mark.parametrize('startup', [None, 'started', 'uncertain', 'fenced'])
def test_stopped_manager_recovery_keeps_its_backend_binding_without_restarting(private_cutover, damage, startup):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_management_entry import EntryError
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_origin_history import derive_management_history_target
    from tests.ops.test_nebius_pool_cutover import CutoverAPI

    metadata, _, root = private_cutover
    context = entry.load_pool_cutover_inputs(metadata)
    api = CutoverAPI(context.request)
    for document in api.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    stage_pool_cutover(request=context.request, tokens=context.tokens, api=api,
        state_dir=Path(metadata['state_dir']), anchor_dir=Path(metadata['anchor_dir']))
    by_key = api.documents
    if startup:
        from scripts.ops.nebius_ingress_stage import _key
        from scripts.ops.nebius_pool_startup import stage_pool_startup
        from tests.ops.test_nebius_pool_startup import StartupAPI

        successor = StartupAPI(context.request, api, Path(metadata['state_dir']))
        if startup in {'uncertain', 'fenced'}:
            successor.fail_key, successor.failure = _key(context.request.manager), 'before'
        result = stage_pool_startup(request=context.request, api=successor,
            state_dir=Path(metadata['state_dir']), anchor_dir=Path(metadata['anchor_dir']))
        assert result['status'] == ('pending_startup_outcome' if startup in {'uncertain', 'fenced'} else 'pool_startup_staged_closed')
        by_key = successor.documents
        if startup == 'uncertain':
            by_key[successor.fail_key]['spec']['replicas'] = 1
        elif startup == 'fenced':
            _cancel_and_fence_recovery(context, metadata, api, successor)
    target = derive_management_history_target(original=context.original, predecessor=context.predecessor, credential=history_credential(root))
    reads = []

    def get(kind, name, namespace):
        assert kind == 'deployment' and (namespace, name) == (target.namespace, 'loom-service')
        value = copy.deepcopy(by_key['Deployment:' + namespace + ':' + name])
        if damage == 'running_again':
            value['spec']['replicas'] = 2 if startup else 1
        return value

    def credential(original, **binding):
        assert original == context.request.manager
        assert binding == {'url_variable': 'LOOM_SVC_DB_URL', 'credential_uid': target.database.credential_uid,
            'credential_resource_version': target.database.credential_resource_version}
        reads.append(True)
        if damage == 'credential_drift' and len(reads) > 1:
            raise ValueError('private-marker')
        return 'postgresql+psycopg://fixture:private-marker@loom-postgres.' + (
            'foreign' if damage == 'backend' else target.namespace) + '.svc:5432/loom'

    history = SimpleNamespace(request=context.request.fencing.retirement.migration, target=target, _get=get,
        qualify_binding=lambda request, manager: None,
        _database=lambda selected, **kwargs: {'metadata': {'uid': target.database.statefulset['metadata']['uid']}},
        _workload_database_url=credential,
        qualify_manager_database=lambda: pytest.fail('attempted exec in a retired manager'))
    if damage:
        with pytest.raises(EntryError):
            entry.qualify_pool_manager_database(context, history)
    else:
        entry.qualify_pool_manager_database(context, history)
        assert len(reads) == 2


@pytest.mark.parametrize('damage', [None, 'ambient', 'cluster', 'group', 'project', 'quota', 'quota_binding',
    'missing_setting', 'unavailable', 'config', 'late_config', 'credentials', 'secret_missing',
    'secret_uid', 'secret_version', 'secret_digest', 'secret_name', 'secret_namespace',
    'secret_deleted', 'secret_encoding', 'secret_empty', 'secret_extra_key', 'late_secret', 'sdk_denied'])
def test_pool_provider_preflight_binds_real_reader_and_live_configuration(collector_inputs, platform_inputs, tmp_path, monkeypatch, damage):
    """Only SDK and Kubernetes transports are doubled; native quota parsing runs."""
    from types import SimpleNamespace

    from nebius.api.nebius.compute.v1 import PlatformServiceClient
    from nebius.api.nebius.mk8s.v1 import NodeGroupServiceClient
    from nebius.api.nebius.quotas.v1 import QuotaAllowanceServiceClient
    from nebius.sdk import SDK
    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_management_entry import EntryError
    from tests.unit.test_execution_capacity_collector import (
        _enum,
        _node_group_spec,
        _platform_client,
        _quota,
    )

    migration, collector, configmap = copy.deepcopy(collector_inputs)
    spec = migration.registration.spec
    config = copy.deepcopy(platform_inputs[0])
    config.update(cluster_id=spec.cluster_id, execution_node_group_id=spec.node_group_id)
    credential = tmp_path / 'operator.json'
    credential.write_text('{"private":"marker"}')
    credential.chmod(0o600)
    collector_bytes = b'{"private":"collector-marker"}'
    secret = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
        'metadata': {'name': 'loom-execution-capacity-collector-nebius',
            'namespace': configmap['metadata']['namespace'], 'uid': str(uuid4()), 'resourceVersion': '27'},
        'data': {'credentials.json': base64.b64encode(collector_bytes).decode()}}
    binding = SimpleNamespace(uid=secret['metadata']['uid'], resource_version='27',
        sha256=hashlib.sha256(collector_bytes).hexdigest())
    context = SimpleNamespace(operation={'inputs_path': str(tmp_path / 'inputs.json')},
        request=SimpleNamespace(collector_config=configmap, collector_credential=binding,
            fencing=SimpleNamespace(retirement=SimpleNamespace(collectors=(collector,)))),
        inputs=SimpleNamespace(installation=spec),
        original=SimpleNamespace(deployment=SimpleNamespace(installation=SimpleNamespace(foundation=SimpleNamespace(platform_config=config))),
            original_inputs=SimpleNamespace(operator_cloud_credentials=credential)))
    group = SimpleNamespace(metadata=SimpleNamespace(id=spec.node_group_id, parent_id=spec.cluster_id, resource_version=9),
        spec=_node_group_spec(), status=SimpleNamespace(state=_enum('RUNNING'), node_count=0, target_node_count=0,
            ready_node_count=0, reconciling=False, events=[]))
    quotas = [_quota(parts[3], parts[4], 100 if name != 'storage' else 2000 * 1024**3, 0, index + 1)
        for index, (name, parts) in enumerate(spec.quota_identities.items())]
    if damage == 'cluster':
        group.metadata.parent_id = 'foreign-cluster'
    elif damage == 'group':
        group.metadata.id = 'foreign-group'
    elif damage == 'project':
        configmap['data']['LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_PROJECT_ID'] = 'foreign-project'
    elif damage == 'quota':
        quotas[0].status.unit = 'foreign-unit'
    elif damage == 'quota_binding':
        spec.quota_identities['nodes'] = (*spec.quota_identities['nodes'][:-1], 'foreign-unit')
    elif damage == 'missing_setting':
        del configmap['data']['LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_PROJECT_ID']
        monkeypatch.setenv('LOOM_EXECUTION_CAPACITY_COLLECTOR_NEBIUS_PROJECT_ID', config['project_id'])
    elif damage == 'unavailable':
        group.status.state = _enum('DELETING')
    elif damage == 'ambient':
        for key in ('NEBIUS_PROJECT_ID', 'NEBIUS_NODE_GROUP_ID', 'QUOTA_MEMORY_NAME', 'KUBERNETES_ENDPOINT'):
            monkeypatch.setenv('LOOM_EXECUTION_CAPACITY_COLLECTOR_' + key, 'foreign')
    calls, closed, files = [], [], []

    async def quota_read(_self, request, **kwargs):
        calls.append('quotas')
        assert request.parent_id == config['quota_parent_id']
        return SimpleNamespace(items=quotas, next_page_token='')

    async def group_read(_self, request, **kwargs):
        calls.append('group')
        assert request.id == spec.node_group_id
        if damage == 'credentials':
            files[-1].write_text('changed-private-marker')
        return group

    async def platform_read(_self, request, **kwargs):
        assert request.parent_id == config['project_id']
        return await _platform_client().get_by_name(request, **kwargs)

    async def close(_self):
        closed.append(True)

    def get(method, path):
        assert method == 'GET'
        if path == '/api/v1/namespaces/' + secret['metadata']['namespace'] + '/secrets/loom-execution-capacity-collector-nebius':
            result = copy.deepcopy(secret)
            if damage == 'secret_missing':
                return None
            if damage in {'secret_uid', 'late_secret'} and (damage != 'late_secret' or calls):
                result['metadata']['uid'] = str(uuid4())
            elif damage == 'secret_version':
                result['metadata']['resourceVersion'] = '28'
            elif damage == 'secret_digest':
                result['data']['credentials.json'] = base64.b64encode(b'foreign-private-marker').decode()
            elif damage == 'secret_name':
                result['metadata']['name'] = 'foreign'
            elif damage == 'secret_namespace':
                result['metadata']['namespace'] = 'foreign'
            elif damage == 'secret_deleted':
                result['metadata']['deletionTimestamp'] = '2026-10-01T00:00:00Z'
            elif damage == 'secret_encoding':
                result['data']['credentials.json'] = '@invalid'
            elif damage == 'secret_empty':
                result['data']['credentials.json'] = ''
            elif damage == 'secret_extra_key':
                result['data']['other'] = 'c2VjcmV0'
            return result
        assert path == '/api/v1/namespaces/' + configmap['metadata']['namespace'] + '/configmaps/' + configmap['metadata']['name']
        result = copy.deepcopy(configmap)
        if damage == 'config' or (damage == 'late_config' and calls):
            result['metadata']['uid'] = str(uuid4())
        return result

    def sdk_init(_self, *, credentials_file_name, user_agent_prefix):
        actual = Path(credentials_file_name)
        files.append(actual)
        assert actual != credential, 'preflight used operator authority instead of the mounted collector credential'
        assert actual.read_bytes() == collector_bytes
        assert actual.stat().st_mode & 0o777 == 0o600
        assert actual.parent.stat().st_mode & 0o777 == 0o700
        if damage == 'sdk_denied':
            raise ValueError('private-sdk-marker')

    # SDK methods themselves are the network boundary, not the capacity parser.
    monkeypatch.setattr(SDK, '__init__', sdk_init)
    monkeypatch.setattr(SDK, 'close', close)
    monkeypatch.setattr(QuotaAllowanceServiceClient, '__init__', lambda *args, **kwargs: None)
    monkeypatch.setattr(NodeGroupServiceClient, '__init__', lambda *args, **kwargs: None)
    monkeypatch.setattr(PlatformServiceClient, '__init__', lambda *args, **kwargs: None)
    monkeypatch.setattr(QuotaAllowanceServiceClient, 'list', quota_read)
    monkeypatch.setattr(NodeGroupServiceClient, 'get', group_read)
    monkeypatch.setattr(PlatformServiceClient, 'get_by_name', platform_read)
    if damage not in {None, 'ambient'}:
        with pytest.raises(EntryError) as error:
            entry.qualify_pool_provider(context, SimpleNamespace(_request=get))
        assert 'private' not in str(error.value)
    else:
        entry.qualify_pool_provider(context, SimpleNamespace(_request=get))
        assert calls == ['quotas', 'group'] and closed == [True]
    if calls:
        assert closed == [True]
    if damage is not None and damage.startswith('secret_'):
        assert files == [] and calls == []  # Reject before handing any bytes to the SDK.
    assert all(not path.exists() for path in files)
    assert credential.read_bytes() == b'{"private":"marker"}'


@pytest.mark.parametrize("damage", ["failed_run", "unmerged", "failed_gate", "forged_app", "expired",
    "tampered_artifact", "candidate_bytes", "profile_bytes", "registry", "keyring",
    "participant_signature", "catalog_trust", "private_drift"])
def test_cutover_requires_actual_publication_before_operator_credentials(private_cutover, publication_http, monkeypatch, damage):
    from scripts.ops import nebius_pool_cutover_entry as entry
    from scripts.ops.nebius_management_entry import EntryError

    metadata, payload, root = private_cutover
    responses, state = publication_http
    if damage == "failed_run":
        responses["actions/runs/100/attempts/1"]["conclusion"] = "failure"
    elif damage == "unmerged":
        responses["pulls/2301"]["merged"] = False
    elif damage in {"failed_gate", "forged_app"}:
        check = responses["commits/" + "b" * 40 + "/check-runs"]["check_runs"][0]
        if damage == "failed_gate":
            check["conclusion"] = "failure"
        else:
            check["app"]["id"] = 42
    elif damage == "expired":
        responses["actions/artifacts/200"]["expired"] = True
    elif damage == "tampered_artifact":
        state["payload"] += b"tampered-private-marker"
    elif damage == "candidate_bytes":
        payload["candidate"]["source_archive_sha256"] = "sha256:" + "e" * 64
        save_private(metadata, payload)
    elif damage == "profile_bytes":
        payload["profile"]["supports_task_web_egress"] = not payload["profile"].get("supports_task_web_egress", False)
        save_private(metadata, payload)
    elif damage == "registry":
        payload["candidate"]["registry_prefix"] = "cr.eu-north1.nebius.cloud/foreign"
        save_private(metadata, payload)
    elif damage == "participant_signature":
        profile = next(iter(payload["profiles"].values()))
        # Valid same-key evidence for the same images is still not the exact
        # protected publication being installed.
        profile["image_admission"] = signed_image_admission_bundle(tuple(profile[key] for key in (
            "task_image_ref", "runtime_image_ref", "agent_image_ref"))).model_dump(mode="json")
        save_private(metadata, payload)
    elif damage == "catalog_trust":
        import base64

        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        key = Ed25519PrivateKey.from_private_bytes(b"\x19" * 32).public_key()
        payload["installation"]["profiles"]["image_admission_keyring"]["keys"].append({
            "signing_key_id": "foreign-publisher", "public_key_base64": base64.b64encode(
                key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)).decode()})
        save_private(metadata, payload)
    elif damage == "private_drift":
        state["during_read"] = lambda: Path(metadata["inputs_path"]).write_bytes(
            Path(metadata["inputs_path"]).read_bytes() + b"\n")
    else:
        payload["profile"]["image_admission"]["admissions"][0]["signing_key_id"] = "foreign-publisher"
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr("candidate.json", json.dumps(payload["candidate"]))
            bundle.writestr("runtime-profile.json", json.dumps(payload["profile"]))
        state["payload"] = archive.getvalue()
        digest = "sha256:" + hashlib.sha256(state["payload"]).hexdigest()
        payload["publication"]["artifact_sha256"] = responses["actions/artifacts/200"]["digest"] = digest
        responses["actions/artifacts/200"]["size_in_bytes"] = len(state["payload"])
        save_private(metadata, payload)
    before = {path: path.read_bytes() for path in root.history}
    context = entry.load_pool_cutover_inputs(metadata)
    monkeypatch.setattr(entry, "connected_checks", lambda *args, **kwargs: pytest.fail("operator credential exchange occurred"))
    with pytest.raises(EntryError) as error:
        with entry.connected_pool_readers(context):
            pytest.fail("unqualified publication reached cutover")
    assert "private-marker" not in str(error.value)
    assert {path: path.read_bytes() for path in root.history} == before
    assert not Path(metadata["state_dir"]).exists()
