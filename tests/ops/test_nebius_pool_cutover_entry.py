"""Protected cutover inputs derive history, never trust a supplied manager."""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_cutover import cutover_inputs as cutover_inputs
from tests.ops.test_nebius_pool_cutover import (
    collector_inputs as collector_inputs,
    fencing_inputs as fencing_inputs,
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_management_refresh_predecessor import (
    application_management_inputs as application_management_inputs,
    application_material as application_material,
    checks as checks,
    cloud as cloud,
    completed_upgrade as completed_upgrade,
    entry_inputs as entry_inputs,
    history_credential,
    installation as installation,
    load,
    material as material,
    private_upgrade as private_upgrade,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


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
    publication = {"candidate_id": str(uuid4()), "source_sha": migration.registration.candidate["candidate_sha"],
        "run_id": 100, "run_attempt": 1, "artifact_id": 200, "artifact_sha256": "sha256:" + "a" * 64,
        "pull_request": 2301}
    payload = {"schema_version": "loom.nebius-pool-cutover-private-inputs.v1",
        "original_upgrade": root.selector.model_dump(mode="json"),
        "predecessor": root.selector.model_dump(mode="json"),
        "installation": spec, "publication": publication, "candidate": migration.registration.candidate,
        "profile": request.profiles[development.participant_id].model_dump(mode="json"),
        "guards": guards, "actuators": request.fencing.retirement.actuators,
        "collectors": request.fencing.retirement.collectors, "roles": request.fencing.originals,
        "services": request.services, "collector_config": collector_config,
        "profiles": {str(key): value.model_dump(mode="json") for key, value in request.profiles.items()},
        "machine_token_files": token_paths, "foundation_candidate": "5" * 40}
    metadata = {"schema": "loom.nebius-pool-cutover-operation.v1", "operation_id": spec["operation_id"],
        "source_sha": migration.registration.candidate["candidate_sha"],
        "candidate": migration.registration.candidate["candidate_sha"],
        "installation_id": spec["installation_id"], "namespace": root.upgrade.setup.binding.namespace,
        "state_dir": str(directory / "state"), "anchor_dir": str(directory / "anchor"),
        "inputs_path": str(directory / "inputs.json"), "inputs_sha256": ""}
    save_private(metadata, payload)
    return metadata, payload, root


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
    assert context.request.kubernetes_endpoint == root.original_inputs.operator_connection.endpoint
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


def test_connected_readers_use_one_explicit_operator_authority_and_erase_temporary_token(private_cutover, monkeypatch):
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
