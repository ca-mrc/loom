"""Protected cutover inputs derive history, never trust a supplied manager."""
from __future__ import annotations

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


def test_private_cutover_derives_the_manager_and_keeps_history_read_only(private_cutover):
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

    metadata, payload, root = private_cutover
    before = {path: path.read_bytes() for path in root.history}
    context = load_pool_cutover_inputs(metadata)
    assert context.request.manager == root.active
    assert context.request.management_origin == "https://" + root.deployment.public_host
    assert context.request.kubernetes_endpoint == "https://kubernetes.default.svc"
    assert context.request.fencing.retirement.migration.registration.binding == root.upgrade.setup.binding
    assert len(context.tokens) == len(payload["machine_token_files"])
    assert {path: path.read_bytes() for path in root.history} == before
    assert not Path(metadata["state_dir"]).exists()


@pytest.mark.parametrize("damage", ["hash", "extra_manager", "source", "installation", "cluster", "pool",
    "missing_database", "token_hash", "token_alias", "token_symlink", "token_public", "path", "publication"])
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
    with pytest.raises(PoolMigrationError) as error:
        with entry.connected_pool_readers(context) as connected:
            path = connected.guards.kubeconfig
            raise PoolMigrationError("management_origin_history")
    assert error.value.stage == "management_origin_history"
    assert not path.exists()


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
