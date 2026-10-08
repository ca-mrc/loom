"""Dedicated renewal tooling/key must never inherit install/staging commands."""
from __future__ import annotations

import ast
import base64
import hashlib
import importlib
import io
import json
import os
import struct
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from tests.ops.test_nebius_development_management_gateway import archive


def module(suffix='gateway'):
    return importlib.import_module('scripts.ops.' + ('install_nebius_development_management_renewal_entrypoint'
        if suffix == 'grant' else 'nebius_development_management_renewal_' + suffix))


def operation(tmp_path):
    installation = '18718d96-d389-40b3-a79b-11489924d0d4'
    renewal = '780b7ee4-6384-4179-a13c-ea6701df3fbd'
    root = tmp_path / '.loom/nebius-development-management-renewal' / installation / renewal
    return {'schema': 'loom.nebius-development-management-renewal-operation.v1',
        'source_sha': 'a' * 40, 'installation_id': installation, 'operation_id': renewal,
        'namespace': 'loom-nebius-management-dev', 'inputs_path': str(root / 'inputs.json'), 'inputs_sha256': 'b' * 64}


def members(tmp_path):
    return {**dict.fromkeys(module().SOURCES, b'# reviewed source\n'), 'uv': b'test-uv',
        'requirements.txt': b'test==1 --hash=sha256:' + b'a' * 64,
        'wheels/loom-0.1.0-py3-none-any.whl': b'test-wheel',
        'wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl': b'test-wheel',
        'operation.json': json.dumps(operation(tmp_path), sort_keys=True).encode(),
        'development-management-renewal-source.json': json.dumps({'source_sha': 'a' * 40,
            'source_archive_sha256': 'sha256:' + 'c' * 64}, sort_keys=True).encode()}


def report(tmp_path, status='development_management_tls_renewed', **extra):
    return {key: value for key, value in operation(tmp_path).items() if key in {
        'source_sha', 'installation_id', 'operation_id', 'namespace'}} | {
            'status': status, 'fingerprint_sha256': 'd' * 64} | extra


def test_bundle_is_closed_over_all_transitive_runtime_imports(tmp_path):
    files, selected = module().unpack_bundle(archive(members(tmp_path)))
    assert selected == operation(tmp_path)
    root = Path(__file__).resolve().parents[2]
    pending, seen = ['scripts.ops.nebius_development_management_renewal_entry'], set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        path = name.replace('.', '/') + '.py'
        assert path in files, 'Missing runtime import: ' + path
        for node in ast.walk(ast.parse((root / path).read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith('scripts.ops.'):
                    pending.append(node.module)
                elif node.module == 'scripts.ops':
                    pending.extend('scripts.ops.' + alias.name for alias in node.names)


@pytest.mark.parametrize('damage', ['namespace', 'schema', 'source', 'traversal', 'duplicate', 'symlink', 'initial-input', 'nil-operation'])
def test_bundle_rejects_foreign_authority_and_unsafe_members(tmp_path, damage):
    files = members(tmp_path)
    op = operation(tmp_path)
    if damage == 'namespace':
        op['namespace'] = 'loom-nebius-platform'
    elif damage == 'schema':
        op['schema'] = 'loom.nebius-development-management-operation.v1'
    elif damage == 'source':
        op['source_sha'] = 'e' * 40
    elif damage == 'traversal':
        files['../foreign'] = b'no'
    elif damage == 'initial-input':
        op['inputs_path'] = str(Path(op['inputs_path']).parents[2] / op['installation_id'] / 'inputs.json')
    elif damage == 'nil-operation':
        op['operation_id'] = '00000000-0000-0000-0000-000000000000'
    files['operation.json'] = json.dumps(op).encode()
    with pytest.raises(module().GatewayError):
        module().unpack_bundle(archive(files, duplicate=damage == 'duplicate', symlink=damage == 'symlink'))


def test_unauthenticated_bundle_and_initial_install_command_never_prepare_release(tmp_path, monkeypatch):
    raw = archive(members(tmp_path))
    monkeypatch.setattr(sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(raw)))
    monkeypatch.setattr(module(), 'prepare_release', lambda *_: pytest.fail('unauthenticated execution'))
    monkeypatch.setenv('SSH_ORIGINAL_COMMAND', 'loom-nebius-development-management-install-v1')
    assert module().authorized_main(hashlib.sha256(raw).hexdigest()) == 126
    monkeypatch.setenv('SSH_ORIGINAL_COMMAND', 'loom-nebius-development-management-renewal-renew-v1')
    assert module().authorized_main('0' * 64) == 126


@pytest.mark.parametrize('action,status,extra', [
    ('preflight', 'development_management_tls_preflight_qualified', {}),
    ('renew', 'development_management_tls_renewed', {}),
    ('renew', 'pending', {'phase': 'public'}), ('renew', 'rejected', {}),
    ('renew', 'blocked', {'stage': 'renewal'}),
])
def test_protected_report_retains_only_closed_bound_fields(tmp_path, action, status, extra):
    value = report(tmp_path, status, **extra)
    visible = module().safe_report(json.dumps(value | {'private': 'not-exported'}).encode(), operation(tmp_path), action)
    expected = value if status != 'blocked' else {key: item for key, item in value.items() if key != 'fingerprint_sha256'}
    assert visible == expected
    for wrong in ({'namespace': 'loom-nebius-platform'}, {'operation_id': str('0' * 36)},
                  {'fingerprint_sha256': 'secret-value'}, {'status': 'development_management_installed'}):
        if status == 'blocked' and 'fingerprint_sha256' in wrong:
            continue
        with pytest.raises(module().GatewayError):
            module().safe_report(json.dumps(value | wrong).encode(), operation(tmp_path), action)


def test_release_preparation_is_private_replayable_and_separate_from_renewal_journal(tmp_path, monkeypatch):
    op = operation(tmp_path)
    root = Path(op['inputs_path']).parent
    root.mkdir(mode=0o700, parents=True)
    raw, calls = archive(members(tmp_path)), []
    def run(args, **kwargs):
        calls.append(args)
        return b'{"status":"tooling_qualified"}' if args[-1] == 'qualify' else b''
    monkeypatch.setattr(module(), 'run_private', run)
    release = module().prepare_release(raw)
    assert release.parent == root / 'releases'
    assert len(calls) == 4 and '--require-hashes' in calls[1] and '--offline' in calls[2]
    assert calls[2][1:3] == ['pip', 'install']
    assert module().prepare_release(raw) == release and len(calls) == 4
    assert not (tmp_path / '.loom/nebius-development-management').exists()
    (release / 'development-management-renewal-source.json').write_bytes(b'changed')
    with pytest.raises(module().GatewayError):
        module().prepare_release(raw)
    assert len(calls) == 4


def test_installed_dedicated_key_preserves_foreign_grants_and_rejects_other_commands(tmp_path):
    root = tmp_path / '.loom'
    root.mkdir(mode=0o700)
    ssh = tmp_path / '.ssh'
    ssh.mkdir(mode=0o700)
    keys = ssh / 'authorized_keys'
    previous = b'# retained staging authority\nrestrict,command="staging" ssh-ed25519 FOREIGN\n'
    keys.write_bytes(previous)
    keys.chmod(0o600)
    files = members(tmp_path)
    files['scripts/ops/nebius_development_management_renewal_gateway.py'] = b'def authorized_main(digest):\n    return 0\n'
    raw = archive(files)
    wire = struct.pack('>I', 11) + b'ssh-ed25519' + struct.pack('>I', 32) + b'i' * 32
    key = 'ssh-ed25519 ' + base64.b64encode(wire).decode()
    digest = hashlib.sha256(raw).hexdigest()
    assert module('grant').install(raw, expected_sha256=digest, public_key=key)['status'] == 'prepared'
    assert keys.read_bytes() == previous
    result = module('grant').install(raw, expected_sha256=digest, public_key=key, apply=True)
    after = keys.read_bytes()
    assert after.startswith(previous) and len(after.splitlines()) == 3
    assert module('grant').install(raw, expected_sha256=digest, public_key=key, apply=True) == result
    assert keys.read_bytes() == after
    entry = Path(operation(tmp_path)['inputs_path']).parent / 'authority' / digest / 'entrypoint.py'
    def invoke(command):
        return subprocess.run([sys.executable, '-I', str(entry)], capture_output=True, timeout=10,
            env={'PATH': os.defpath, 'SSH_ORIGINAL_COMMAND': command}).returncode
    for action in ('preflight', 'renew'):
        assert invoke('loom-nebius-development-management-renewal-' + action + '-v1') == 0
    for command in ('id', 'loom-nebius-development-management-install-v1', 'loom-nebius-management-install-v1'):
        assert invoke(command) == 126
    (entry.parent / 'scripts/ops/nebius_development_management_renewal_operation.py').write_bytes(b'changed')
    assert invoke('loom-nebius-development-management-renewal-renew-v1') == 126
    assert not (root / 'nebius-development-management').exists()


def test_reproducible_publisher_contains_no_private_inputs_and_uses_only_renewal_command(tmp_path, monkeypatch):
    uv, requirements, wheels = tmp_path / 'uv', tmp_path / 'requirements', tmp_path / 'wheels'
    uv.write_bytes(b'tool')
    requirements.write_bytes(b'locked dependencies')
    wheels.mkdir()
    for name in ('loom-0.1.0-py3-none-any.whl', 'loom_bundle_checksum-0.1.0-py3-none-any.whl'):
        (wheels / name).write_bytes(b'wheel')
    source = {'source_sha': 'a' * 40, 'source_archive_sha256': 'sha256:' + 'c' * 64}
    publisher = module('rollout')
    raw = publisher.build_bundle(operation(tmp_path), source=source, uv=uv, requirements=requirements, wheels=wheels)
    assert raw == publisher.build_bundle(operation(tmp_path), source=source, uv=uv, requirements=requirements, wheels=wheels)
    files, _ = module().unpack_bundle(raw)
    assert 'inputs.json' not in files and not any(name.endswith('.pem') for name in files)
    calls = []
    def ssh(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0, stdout=json.dumps(report(tmp_path)).encode())
    monkeypatch.setattr(publisher.subprocess, 'run', ssh)
    assert publisher.transfer(raw, action='renew', target='operator@host', key=tmp_path / 'key',
        known_hosts=tmp_path / 'hosts') == report(tmp_path)
    assert calls[0][0][-1] == 'loom-nebius-development-management-renewal-renew-v1'
    assert 'StrictHostKeyChecking=yes' in calls[0][0] and calls[0][1]['input'] == raw
    with pytest.raises(publisher.RolloutError):
        publisher.transfer(raw, action='install', target='operator@host', key=tmp_path / 'key', known_hosts=tmp_path / 'hosts')
    assert len(calls) == 1


def test_workflow_uses_dedicated_renewal_identity_and_serialized_protected_route():
    flow = yaml.safe_load((Path(__file__).resolve().parents[2] / '.github/workflows/nebius-rollout.yml').read_text())
    choices = flow.get('on', flow.get(True))['workflow_dispatch']['inputs']['operation']['options']
    assert {'development-management-renewal-preflight', 'development-management-renewal-renew'} <= set(choices)
    job = flow['jobs']['development-management-renewal']
    assert job['environment'] == {'name': 'nebius-integration', 'deployment': False}
    assert job['permissions'] == {'contents': 'read'}
    assert 'workflow_dispatch' in job['if'] and 'refs/heads/dev' in job['if']
    runner, = (step for step in job['steps'] if step.get('name') == 'Run fixed development management renewal operation')
    assert runner['env']['DEPLOY_SSH_KEY'] == '${{ secrets.NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_SSH_KEY }}'
    assert runner['env']['NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON'] == '${{ vars.NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON }}'
    assert flow['concurrency']['cancel-in-progress'] is False


@pytest.mark.parametrize("action", ["preflight", "renew"])
def test_workflow_executes_fixed_renewal_selector_and_cleans_ephemeral_key(tmp_path, action):
    flow = yaml.safe_load((Path(__file__).resolve().parents[2] / '.github/workflows/nebius-rollout.yml').read_text())
    choices = flow.get("on", flow.get(True))["workflow_dispatch"]["inputs"]["operation"]["options"]
    assert "development-management-renewal-" + action in choices
    job = flow["jobs"]["development-management-renewal"]
    assert job["permissions"] == {"contents": "read"}
    assert job["environment"] == {"name": "nebius-integration", "deployment": False}
    assert "workflow_dispatch" in job["if"] and "refs/heads/dev" in job["if"]
    selector = next(s for s in job["steps"] if s.get("id") == "tooling")
    runner = next(s for s in job["steps"] if s.get("name") == "Run fixed development management renewal operation")
    assert runner["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_SSH_KEY }}"
    assert runner["env"]["NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON"] == "${{ vars.NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON }}"
    bindir = tmp_path / "bin"
    bindir.mkdir()
    git = bindir / "git"
    git.write_text('#!/bin/sh\ntest "$1 $2 $3 $4" = "merge-base --is-ancestor $EXPECTED_SHA refs/remotes/origin/dev"\n')
    git.chmod(0o700)
    uv = bindir / "uv"
    uv.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
assert sys.argv[sys.argv.index('--operation') + 1] == os.environ['EXPECTED_ACTION']
assert 'scripts.ops.nebius_development_management_renewal_rollout' in sys.argv
assert pathlib.Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE']).read_text().strip() == 'private-dev-key'
assert json.loads(os.environ['NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON'])['namespace'] == 'loom-nebius-management-dev'
pathlib.Path(os.environ['RUNNER_TEMP'], 'transport-invoked').write_text('selected')
''')
    uv.chmod(0o700)
    env = os.environ | {"PATH": str(bindir) + ":" + os.environ["PATH"], "DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION": "development-management-renewal-" + action,
        "NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON": json.dumps(operation(tmp_path)), "DEPLOY_SSH_KEY": "private-dev-key",
        "DEPLOY_KNOWN_HOSTS": "test-host", "LOOM_DEPLOY_SSH_TARGET": "test-target", "RUNNER_TEMP": str(tmp_path),
        "GITHUB_OUTPUT": str(tmp_path / "outputs"), "EXPECTED_SHA": "a" * 40, "EXPECTED_ACTION": action}
    for step in (selector, runner):
        result = subprocess.run(["bash", "-e", "-c", step["run"]], env=env, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        assert "private-dev-key" not in result.stdout + result.stderr
    assert (tmp_path / "outputs").read_text() == "sha=" + "a" * 40 + "\n"
    assert (tmp_path / "transport-invoked").read_text() == "selected"
    assert not (tmp_path / "nebius-development-management-renewal-key").exists()
    (tmp_path / "transport-invoked").unlink()
    for damage in ({"DEPLOY_SSH_KEY": ""}, {"DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION": "management-install"}):
        result = subprocess.run(["bash", "-e", "-c", runner["run"]], env=env | damage, capture_output=True, timeout=20)
        assert result.returncode != 0 and not (tmp_path / "transport-invoked").exists()
    wrong = operation(tmp_path) | {"namespace": "loom-nebius-platform"}
    result = subprocess.run(["bash", "-e", "-c", selector["run"]],
        env=env | {"NEBIUS_DEVELOPMENT_MANAGEMENT_RENEWAL_OPERATION_JSON": json.dumps(wrong)}, capture_output=True, timeout=20)
    assert result.returncode != 0



@pytest.mark.parametrize("damage", [None, "dirty", "untracked", "wrong-head", "not-integrated"])
def test_renewal_publisher_requires_exact_clean_integrated_source(tmp_path, monkeypatch, damage):
    repository = tmp_path / "checkout"
    repository.mkdir()

    def git(*args):
        return subprocess.run(["git", *args], cwd=repository, check=True, capture_output=True, text=True).stdout.strip()

    git("init", "-q")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    (repository / "source").write_text("approved source\n")
    git("add", "source")
    git("commit", "-qm", "initial")
    sha = git("rev-parse", "HEAD")
    if damage != "not-integrated":
        git("update-ref", "refs/remotes/origin/dev", sha)
    metadata = operation(tmp_path) | {"source_sha": sha}
    if damage == "dirty":
        (repository / "source").write_text("changed")
    elif damage == "untracked":
        (repository / "untracked").write_text("unexpected")
    elif damage == "wrong-head":
        metadata.update(source_sha="b" * 40)
    monkeypatch.setattr(module('rollout'), "ROOT", repository)
    if damage:
        with pytest.raises(module('rollout').RolloutError):
            module('rollout').verify_source(metadata)
    else:
        module('rollout').verify_source(metadata)
