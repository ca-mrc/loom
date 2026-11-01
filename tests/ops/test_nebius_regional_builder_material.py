"""Private real-file/CLI checks with a fixed fake operator Kubernetes surface."""
from __future__ import annotations

import base64
import copy
import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops import nebius_regional_builder_material as material
from scripts.ops.nebius_regional_rollout_gateway import SCHEMA
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.support.nebius_platform import regional_inputs as regional_inputs

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def case(regional_inputs, tmp_path):
    config, candidate, _ = regional_inputs
    config = copy.deepcopy(config)
    target = config["regional_execution_targets"][0]
    builder = {"registry_repository": "cr.eu-north1.nebius.cloud/test/task-images", "cache_bucket": "existing-build-cache"}
    config["task_image_builder"] = copy.deepcopy(builder)
    target["task_image_builder"] = copy.deepcopy(builder)
    source_uid, execution_uid, destination_uid = (str(uuid4()) for _ in range(3))
    namespace = target["execution_namespace"]
    scope = {"schema_version": SCHEMA, "kubeconfig": str(tmp_path / "retained-config"),
        "kubectl": str(tmp_path / "fixed-kubectl"), "regions": [{
            "context": "approved-west", "cluster_name": "cluster-" + target["cluster_id"].removeprefix("mk8s"),
            "api_server": target["kubernetes_api_server"], "namespace": namespace,
            "namespace_uids": {namespace: execution_uid, namespace + "-build": destination_uid},
            "resources": [{"kind": "Namespace", "name": name, "namespace": None}
                          for name in (namespace, namespace + "-build")], "secret_key_reads": []}]}
    roles = {"loom-task-build-source": (["archive", "source"], ["access-key", "secret-key"]),
        "loom-task-build-cache": (["cache"], ["access-key", "secret-key"]),
        "loom-task-build-registry": (["registry"], ["credentials.json"])}
    refs = {}
    for name, (names, keys) in roles.items():
        files = {}
        for key in keys:
            path = tmp_path / (name + "-" + key)
            path.write_bytes(("private-value-never-in-output:" + name + ":" + key).encode())
            path.chmod(0o600)
            files[key] = str(path)
        refs[name] = {"source_secret_uid": str(uuid4()), "roles": names, "files": files}
    return {"scope": scope, "config": config, "candidate": candidate,
            "source_sha": candidate["candidate_sha"], "target_id": target["target_id"],
            "source_namespace_uid": source_uid, "material_files": refs}


class FakeKube:
    def __init__(self, case):
        self.case = case
        self.calls = []
        self.created = {}
        self.namespace_changed = None
        self.source_changed = None
        self.missing_key = False
        self.conflict_on_create = False
        self.fail_create = False
        self.rotate_after_create = False
        self.fail_create_number = None
        self.lose_create_ack = False

    def __call__(self, scope, args, *, payload=None, timeout=30):
        assert scope == self.case["scope"] and timeout == 15
        args = list(args)
        self.calls.append((args, payload))
        destination = "--context" in args
        if destination:
            assert args[args.index("--context") + 1] == "approved-west"
        index = next(i for i, value in enumerate(args) if value in {"get", "create"})
        command = args[index:]
        output = b""
        if command[:2] == ["get", "namespace"]:
            name = command[2]
            expected = (self.case["source_namespace_uid"] if not destination else
                self.case["scope"]["regions"][0]["namespace_uids"][name])
            uid = str(uuid4()) if self.namespace_changed == ("destination" if destination else "source") else expected
            output = json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name, "uid": uid}}).encode()
        elif command[:2] == ["get", "secret"]:
            name = command[2]
            assert name in self.case["material_files"]
            ref = self.case["material_files"][name]
            keys = tuple(sorted(ref["files"]))
            if destination:
                assert command[-1] == material._destination_template(keys)
                document = self.created.get(name)
                if document is None:
                    return subprocess.CompletedProcess(args, 0, b"", b"")
                if not document.get('metadata'):
                    return subprocess.CompletedProcess(args, 0, b"unrelated-existing-secret\n", b"")
                uid, version = document["metadata"]["uid"], "2"
                annotations = document['metadata'].get('annotations', {})
                fields = (uid, version, 'true' if document.get('immutable') else 'false', document.get('type', ''),
                          *(annotations.get('loom.nebius/builder-' + key, '') for key in material._ANNOTATIONS))
            else:
                assert command[-1] == material._metadata_template(keys)
                uid = str(uuid4()) if self.source_changed == "uid" else ref["source_secret_uid"]
                version = "changed" if self.source_changed == "version" else "1"
                fields = (uid, version)
            visible = keys[:-1] if self.missing_key else keys
            output = ("\n".join((*fields, *visible)) + "\n").encode()
        else:
            assert command == ["create", "-f", "-", "-o", 'go-template={{.metadata.uid}}'] and destination
            document = json.loads(payload)
            name = document["metadata"]["name"]
            if self.conflict_on_create or self.fail_create or self.fail_create_number == len(self.created) + 1:
                return subprocess.CompletedProcess(args, 1, b"private-value-never-in-output", b"private file/error path")
            assert name not in self.created
            document["metadata"]["uid"] = str(uuid4())
            self.created[name] = document
            if self.lose_create_ack:
                self.lose_create_ack = False
                return subprocess.CompletedProcess(args, 1, b"", b"private uncertain response")
            output = document["metadata"]["uid"].encode()
            if self.rotate_after_create:
                self.source_changed = "version"
        return subprocess.CompletedProcess(args, 0, output, b"")


def invoke(case, **kwargs):
    if kwargs.get('apply') and 'journal_path' not in kwargs:
        handle = next(iter(next(iter(case['material_files'].values()))['files'].values()))
        kwargs['journal_path'] = Path(handle['path'] if isinstance(handle, dict) else handle).parent / 'install-state.json'
    return material.supply_builder_material(**case, **kwargs)


def test_plan_only_stats_owner_handles_and_returns_no_paths_or_values(case, monkeypatch, capsys):
    def forbidden(*_args, **_kwargs):
        pytest.fail("plan must not read material values or invoke Kubernetes")
    monkeypatch.setattr(material, "_private_bytes", forbidden)
    result = invoke(case, run=forbidden)
    assert result["status"] == "material_planned" and result["controllers_started"] is False
    assert len(result["required_secret_keys"]) == 3
    source = next(row for row in result["required_secret_keys"] if row["name"] == "loom-task-build-source")
    assert source["roles"] == ["archive", "source"]
    assert "private-value" not in json.dumps(result) + capsys.readouterr().out
    assert all(path not in json.dumps(result) for ref in case["material_files"].values() for path in ref["files"].values())


def test_apply_uses_only_owner_bytes_opaque_stdin_and_create_only_destinations(case, capsys):
    kube = FakeKube(case)
    result = invoke(case, apply=True, run=kube)
    assert result["status"] == "material_installed" and len(result["created_secret_uids"]) == 3
    for name, document in kube.created.items():
        assert document["immutable"] is True and document["type"] == "Opaque"
        assert document["metadata"]["namespace"] == result["destination_namespace"]
        for key, path in case["material_files"][name]["files"].items():
            assert base64.b64decode(document["data"][key]) == Path(path).read_bytes()
    assert all("apply" not in args and "patch" not in args and "delete" not in args for args, _ in kube.calls)
    assert all(not payload or "--context" in args for args, payload in kube.calls)
    output = json.dumps(result) + capsys.readouterr().out
    assert "private-value" not in output and all(path not in output and all(path not in arg for args, _ in kube.calls for arg in args)
        for ref in case["material_files"].values() for path in ref["files"].values())


@pytest.mark.parametrize("change", ["candidate", "target", "region", "source_uid", "destination_uid", "roles", "keys", "new_ref", "cache_scope"])
def test_plan_rejects_changes_outside_exact_primary_candidate_role_and_uid_binding(case, change):
    if change == "candidate":
        case["source_sha"] = "f" * 40
    elif change == "target":
        case["target_id"] = "foreign-target"
    elif change == "region":
        case["config"]["regional_execution_targets"][0]["region"] = "eu-north1"
    elif change == "source_uid":
        case["source_namespace_uid"] = "not-a-uid"
    elif change == "destination_uid":
        case["scope"]["regions"][0]["namespace_uids"].pop(case["config"]["regional_execution_targets"][0]["execution_namespace"] + "-build")
    elif change == "roles":
        case["material_files"]["loom-task-build-source"]["roles"] = ["registry"]
    elif change == "keys":
        case["material_files"]["loom-task-build-source"]["files"]["arbitrary-private-key"] = "private-path"
    elif change == "new_ref":
        case["config"]["regional_execution_targets"][0]["task_image_builder"]["archive_secret_name"] = "foreign-archive"
    else:
        case["config"]["regional_execution_targets"][0]["task_image_builder"]["cache_bucket"] = "new-storage-visibility"
    with pytest.raises(material.MaterialError):
        invoke(case)


@pytest.mark.parametrize("change", ["symlink", "parent_symlink", "hardlink", "permissions", "unreadable", "empty", "large", "directory", "owner"])
def test_plan_rejects_unsafe_private_file_handles(case, tmp_path, monkeypatch, change):
    ref = case["material_files"]["loom-task-build-source"]["files"]
    path = Path(ref["access-key"])
    if change == "symlink":
        link = tmp_path / "linked"
        link.symlink_to(path)
        ref["access-key"] = str(link)
    elif change == "parent_symlink":
        link = tmp_path / "parent"
        link.symlink_to(tmp_path, target_is_directory=True)
        ref["access-key"] = str(link / path.name)
    elif change == "hardlink":
        os.link(path, tmp_path / "another-name")
    elif change == "permissions":
        path.chmod(0o644)
    elif change == "unreadable":
        path.chmod(0)
    elif change == "empty":
        path.write_bytes(b"")
    elif change == "large":
        path.write_bytes(b"x" * (material._LIMIT + 1))
    elif change == "directory":
        ref["access-key"] = str(tmp_path)
    else:
        monkeypatch.setattr(material.os, "geteuid", lambda: os.getuid() + 1)
    with pytest.raises(material.MaterialError, match="private material"):
        invoke(case)


@pytest.mark.parametrize("change", ["source", "destination", "source_uid", "missing_key"])
def test_apply_checks_exact_namespaces_source_uids_and_populated_key_metadata_before_write(case, change):
    kube = FakeKube(case)
    if change in {"source", "destination"}:
        kube.namespace_changed = change
    elif change == "source_uid":
        kube.source_changed = "uid"
    else:
        kube.missing_key = True
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert not kube.created


def test_all_existing_destination_conflicts_are_found_before_any_material_read(case, monkeypatch):
    kube = FakeKube(case)
    kube.created["loom-task-build-source"] = {"existing": True}
    monkeypatch.setattr(material, "_private_bytes", lambda *_: pytest.fail("must not read a material value"))
    with pytest.raises(material.MaterialError, match="refusing adoption or rotation"):
        invoke(case, apply=True, run=kube)
    assert kube.created == {"loom-task-build-source": {"existing": True}}


@pytest.mark.parametrize("failure", ["create", "conflict", "rotate"])
def test_uncertain_create_or_source_rotation_never_retries_or_leaks_process_output(case, failure, capsys):
    kube = FakeKube(case)
    kube.fail_create = failure == "create"
    kube.conflict_on_create = failure == "conflict"
    kube.rotate_after_create = failure == "rotate"
    with pytest.raises(material.MaterialError) as error:
        invoke(case, apply=True, run=kube)
    assert len([args for args, _ in kube.calls if "create" in args]) == 1
    assert len(kube.created) == (1 if failure == "rotate" else 0)
    assert "private" not in str(error.value) + capsys.readouterr().out + capsys.readouterr().err


def test_file_replacement_after_plan_is_rejected_without_a_write(case):
    plan = material.plan_material(**case)
    path = Path(case["material_files"]["loom-task-build-source"]["files"]["access-key"])
    replacement = path.with_suffix(".new")
    replacement.write_bytes(path.read_bytes())
    replacement.chmod(0o600)
    replacement.replace(path)
    kube = FakeKube(case)
    with pytest.raises(material.MaterialError, match="handles changed"):
        material.apply_material(plan=plan, material_files=case["material_files"], journal_path=path.parent / 'install-state.json',
            run=lambda args, **kwargs: kube(case["scope"], args, **kwargs))
    assert not kube.calls


def test_symlink_swap_between_stat_and_open_never_reads_or_creates(case, monkeypatch, tmp_path):
    path = Path(case["material_files"]["loom-task-build-source"]["files"]["access-key"])
    other = tmp_path / "unapproved-material"
    other.write_bytes(b"unapproved-private-value")
    other.chmod(0o600)
    real_open = material.os.open

    def swap(value, flags, *args, **kwargs):
        if Path(value) == path:
            path.unlink()
            path.symlink_to(other)
        return real_open(value, flags, *args, **kwargs)

    monkeypatch.setattr(material.os, "open", swap)
    kube = FakeKube(case)
    with pytest.raises(material.MaterialError, match="handles changed"):
        invoke(case, apply=True, run=kube)
    assert not kube.created


def test_same_primary_refs_do_not_bind_unrelated_builder_performance_settings(case):
    case["config"]["regional_execution_targets"][0]["task_image_builder"]["cache_transfer"] = "tar"
    assert invoke(case)["status"] == "material_planned"


def test_partial_create_resumes_only_journal_owned_objects_and_is_idempotent(case, monkeypatch, tmp_path):
    kube = FakeKube(case)
    kube.fail_create_number = 3
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert len(kube.created) == 2
    before = {name: d['metadata']['uid'] for name, d in kube.created.items()}
    journal = json.loads((tmp_path / 'install-state.json').read_text())
    assert journal['operation'] and len(journal['secrets']) == 3
    assert 'private-value' not in json.dumps(journal) and str(tmp_path) not in json.dumps(journal)
    assert (tmp_path / 'install-state.json').stat().st_mode & 0o777 == 0o600
    private_bytes = material._private_bytes
    forbidden = {Path(path) for name in before for path in case['material_files'][name]['files'].values()}
    def read(path, identity):
        assert Path(path) not in forbidden, 'resume must not reread completed credential values'
        return private_bytes(path, identity)
    monkeypatch.setattr(material, '_private_bytes', read)
    kube.fail_create_number = None
    resumed = invoke(case, apply=True, run=kube)
    assert resumed['resumed_secret_uids'] == before
    assert all(kube.created[name]['metadata']['uid'] == uid for name, uid in before.items())
    count = sum('create' in args for args, _ in kube.calls)
    again = invoke(case, apply=True, run=kube)
    assert again['created_secret_uids'] == resumed['created_secret_uids']
    assert again['resumed_secret_uids'] == resumed['created_secret_uids']
    assert sum('create' in args for args, _ in kube.calls) == count


def test_uncertain_create_ack_recovers_by_exact_operation_metadata_without_second_create(case, tmp_path):
    kube = FakeKube(case)
    kube.lose_create_ack = True
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert len(kube.created) == 1
    name, document = next(iter(kube.created.items()))
    assert json.loads((tmp_path / 'install-state.json').read_text())['secrets'][name]['uid'] is None
    result = invoke(case, apply=True, run=kube)
    assert result['resumed_secret_uids'] == {name: document['metadata']['uid']}
    assert sum('create' in args for args, _ in kube.calls) == 3


@pytest.mark.parametrize('drift', ['operation', 'uid', 'immutable', 'type', 'source-version', 'journal-binding', 'deleted'])
def test_resume_never_adopts_replaced_rotated_or_mismatched_material(case, tmp_path, drift):
    kube = FakeKube(case)
    kube.fail_create_number = 2
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert len(kube.created) == 1
    name, document = next(iter(kube.created.items()))
    if drift == 'operation':
        document['metadata']['annotations']['loom.nebius/builder-material-operation'] = str(uuid4())
    elif drift == 'uid':
        document['metadata']['uid'] = str(uuid4())
    elif drift == 'immutable':
        document['immutable'] = False
    elif drift == 'type':
        document['type'] = 'other-authority'
    elif drift == 'source-version':
        kube.source_changed = 'version'
    elif drift == 'journal-binding':
        path = tmp_path / 'install-state.json'
        journal = json.loads(path.read_text())
        journal['binding']['candidate_sha'] = 'f' * 40
        path.write_text(json.dumps(journal))
    elif drift == 'deleted':
        del kube.created[name]
    calls = sum('create' in args for args, _ in kube.calls)
    kube.fail_create_number = None
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert sum('create' in args for args, _ in kube.calls) == calls


def test_material_journal_excludes_concurrent_apply_and_rejects_unsafe_parent(case, tmp_path):
    kube = FakeKube(case)
    journal = tmp_path / 'install-state.json'
    with material._journal_lock(journal), pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert not kube.created
    public = tmp_path / 'public'
    public.mkdir(mode=0o755)
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube, journal_path=public / 'journal.json')
    assert not kube.created


def test_existing_owner_s3_json_supplies_exact_keys_opaquely_without_split_value_files(case, tmp_path, monkeypatch):
    owner = tmp_path / 's3.json'
    owner.write_text(json.dumps({'access-key': 'owner-access-value', 'secret-key': 'owner-secret-value'}))
    owner.chmod(0o600)
    for name in ('loom-task-build-source', 'loom-task-build-cache'):
        case['material_files'][name]['files'] = {key: {'path': str(owner), 'json_key': key}
                                                 for key in ('access-key', 'secret-key')}
    read = material._private_bytes
    monkeypatch.setattr(material, '_private_bytes', lambda *_args: pytest.fail('plan only stats owner JSON handles'))
    assert invoke(case)['status'] == 'material_planned'
    monkeypatch.setattr(material, '_private_bytes', read)
    kube = FakeKube(case)
    result = invoke(case, apply=True, run=kube, journal_path=tmp_path / 'owner-install.json')
    for name in ('loom-task-build-source', 'loom-task-build-cache'):
        assert {key: base64.b64decode(body).decode() for key, body in kube.created[name]['data'].items()} == {
            'access-key': 'owner-access-value', 'secret-key': 'owner-secret-value'}
    safe = json.dumps(result) + (tmp_path / 'owner-install.json').read_text()
    assert str(owner) not in safe and 'owner-access-value' not in safe and 'owner-secret-value' not in safe


@pytest.mark.parametrize('drift', ['selector', 'extra-key', 'invalid-json'])
def test_owner_json_handles_cannot_select_other_fields_or_supply_invalid_json(case, tmp_path, drift):
    owner = tmp_path / 's3.json'
    document = {'access-key': 'opaque-access', 'secret-key': 'opaque-secret'}
    if drift == 'extra-key':
        document['arbitrary-key'] = 'unrelated-material'
    owner.write_text('invalid' if drift == 'invalid-json' else json.dumps(document))
    owner.chmod(0o600)
    case['material_files']['loom-task-build-source']['files']['access-key'] = {
        'path': str(owner), 'json_key': 'secret-key' if drift == 'selector' else 'access-key'}
    kube = FakeKube(case)
    with pytest.raises(material.MaterialError):
        invoke(case, apply=True, run=kube)
    assert not kube.created


def test_explicit_existing_primary_archive_is_supported_without_inventing_a_west_ref(case, tmp_path):
    for builder in (case["config"]["task_image_builder"], case["config"]["regional_execution_targets"][0]["task_image_builder"]):
        builder["archive_secret_name"] = "existing-primary-archive"
    case["material_files"]["loom-task-build-source"]["roles"] = ["source"]
    files = {}
    for key in ("access-key", "secret-key"):
        path = tmp_path / ("archive-" + key)
        path.write_bytes(b"owner-supplied-existing-archive-material")
        path.chmod(0o600)
        files[key] = str(path)
    case["material_files"]["existing-primary-archive"] = {"source_secret_uid": str(uuid4()), "roles": ["archive"], "files": files}
    assert len(invoke(case)["required_secret_keys"]) == 4
    assert len(invoke(case, apply=True, run=FakeKube(case))["created_secret_uids"]) == 4


def test_real_cli_plans_without_kubernetes_and_never_prints_private_paths_or_values(case, tmp_path):
    request = {key: value for key, value in case.items() if key != "material_files"}
    request_path, refs_path = tmp_path / "request.json", tmp_path / "private-refs.json"
    for path, value in ((request_path, request), (refs_path, case["material_files"])):
        path.write_text(json.dumps(value))
        path.chmod(0o600)
    command = [sys.executable, "-B", "-m", "scripts.ops.nebius_regional_builder_material", "plan",
        "--request-file", str(request_path), "--material-refs", str(refs_path)]
    result = subprocess.run(command, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True, text=True)
    assert result.returncode == 0 and json.loads(result.stdout)["status"] == "material_planned"
    assert "private-value" not in result.stdout + result.stderr and str(tmp_path) not in result.stdout + result.stderr
    command[4] = "apply"
    denied = subprocess.run(command, cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT / "src")}, capture_output=True, text=True)
    assert denied.returncode == 1 and str(tmp_path) not in denied.stdout + denied.stderr
