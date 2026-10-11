"""Fixed shared handoff delivery never widens the CLOSED runtime grant."""
from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import io
import json
import os
import shutil
import struct
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from tests.ops.test_nebius_development_management_gateway import archive

ROOT = Path(__file__).resolve().parents[2]


def module(suffix='delivery_gateway'):
    name = ('scripts.ops.install_nebius_shared_pool_entrypoint' if suffix == 'grant'
        else 'scripts.ops.nebius_shared_pool_' + suffix)
    assert importlib.util.find_spec(name) is not None, 'shared delivery component missing: ' + name
    return importlib.import_module(name)


def operation(tmp_path):
    installation = '18718d96-d389-40b3-a79b-11489924d0d4'
    pool_operation = '780b7ee4-6384-4179-a13c-ea6701df3fbd'
    root = tmp_path / '.loom/nebius-shared-pool' / installation / pool_operation
    return {'schema': 'loom.nebius-shared-pool-operation.v1', 'source_sha': 'a' * 40,
        'installation_id': installation, 'operation_id': pool_operation, 'namespace': 'loom-nebius-management-dev',
        'inputs_path': str(root / 'inputs.json'), 'inputs_sha256': 'b' * 64}


def members(tmp_path):
    return {**{name: (ROOT / name).read_bytes() for name in module().SOURCES}, 'uv': b'test-uv',
        'requirements.txt': b'test==1 --hash=sha256:' + b'a' * 64,
        'wheels/loom-0.1.0-py3-none-any.whl': b'test-wheel',
        'wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl': b'test-wheel',
        'operation.json': json.dumps(operation(tmp_path), sort_keys=True).encode(),
        'shared-pool-source.json': json.dumps({'source_sha': 'a' * 40,
            'source_archive_sha256': 'sha256:' + 'c' * 64}, sort_keys=True).encode()}


def report(tmp_path, status, *, phase=None, opened=False):
    value = {key: item for key, item in operation(tmp_path).items() if key in {
        'source_sha', 'installation_id', 'operation_id', 'namespace'}} | {
        'status': status, 'pool_id': 'a8c04f77-bdc6-4343-85e4-c6717bc6fed9',
        'admission_open': opened, 'writer_migration_complete': opened}
    if phase is not None:
        value['phase'] = phase
    return value


def test_shared_bundle_imports_bootstrap_without_site_packages_and_actual_entry_in_isolation(tmp_path):
    from scripts.ops.nebius_ingress_rollout import build_wheels

    gateway = module()
    build = tmp_path / 'wheel-build'
    build.mkdir()
    executable = shutil.which('uv')
    assert executable is not None
    wheels = build_wheels(build, uv=Path(executable))
    content = {name: raw for name, raw in members(tmp_path).items() if not name.startswith('wheels/')}
    content.update({'wheels/' + wheel.name: wheel.read_bytes() for wheel in wheels.iterdir()})
    bundle = archive(content)
    files, selected = gateway.unpack_bundle(bundle)
    assert selected == operation(tmp_path)
    release = tmp_path / 'isolated'
    site = tmp_path / 'wheel-site'
    for name, value in files.items():
        if name.startswith(('scripts/', 'deploy/')):
            path = release / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
        elif name.startswith('wheels/'):
            with zipfile.ZipFile(io.BytesIO(value)) as wheel:
                wheel.extractall(site)
    # The SSH bootstrap must work before the handoff venv/dependencies exist.
    bootstrap = subprocess.run([sys.executable, '-I', '-S', '-c',
        "import sys,json; sys.path.insert(0,sys.argv[1]); "
        "from scripts.ops.nebius_shared_pool_delivery_gateway import unpack_bundle; "
        "print(json.dumps(unpack_bundle(sys.stdin.buffer.read())[1],sort_keys=True))", str(release)],
        input=bundle, capture_output=True, cwd=tmp_path, timeout=20)
    assert bootstrap.returncode == 0, bootstrap.stderr.decode()
    assert json.loads(bootstrap.stdout) == selected
    # Third-party dependencies may be installed, but Loom and scripts.ops must
    # come from the actual bundle, not another checkout or an AST-only check.
    imported = subprocess.run([sys.executable, '-I', '-c',
        "import sys,pathlib; root=pathlib.Path(sys.argv[1]); site=pathlib.Path(sys.argv[2]); "
        "sys.path[:0]=[str(root),str(site)]; "
        "from scripts.ops.nebius_shared_pool_entry import main; "
        "assert callable(main); "
        "assert all(pathlib.Path(m.__file__).is_relative_to(root) for n,m in sys.modules.items() "
        "if n.startswith('scripts.ops.') and getattr(m,'__file__',None)); "
        "assert all(pathlib.Path(m.__file__).is_relative_to(site) for n,m in sys.modules.items() "
        "if n.split('.')[0] in ('loom','loom_service') and getattr(m,'__file__',None)); "
        "print('entry imports')", str(release), str(site)],
        capture_output=True, cwd=tmp_path, timeout=30)
    assert imported.returncode == 0, imported.stderr.decode()
    assert imported.stdout == b'entry imports\n'


@pytest.mark.parametrize('damage', ['namespace', 'schema', 'source', 'path', 'nil-operation', 'extra', 'missing', 'duplicate', 'symlink'])
def test_shared_bundle_rejects_wrong_authority_and_non_exact_members(tmp_path, damage):
    files = members(tmp_path)
    selected = operation(tmp_path)
    if damage == 'namespace':
        selected['namespace'] = 'loom-nebius-platform'
    elif damage == 'schema':
        selected['schema'] = 'loom.nebius-development-runtime-operation.v1'
    elif damage == 'source':
        selected['source_sha'] = 'e' * 40
    elif damage == 'path':
        selected['inputs_path'] = selected['inputs_path'].replace('/nebius-shared-pool/', '/nebius-development-runtime/')
    elif damage == 'nil-operation':
        selected['operation_id'] = '00000000-0000-0000-0000-000000000000'
    elif damage == 'extra':
        files['../private.json'] = b'not-shipped'
    elif damage == 'missing':
        del files['scripts/ops/nebius_shared_pool_api.py']
    files['operation.json'] = json.dumps(selected).encode()
    with pytest.raises(module().GatewayError):
        module().unpack_bundle(archive(files, duplicate=damage == 'duplicate', symlink=damage == 'symlink'))


def test_shared_unauthenticated_or_foreign_commands_never_prepare_release(tmp_path, monkeypatch):
    raw = archive(members(tmp_path))
    gateway = module()
    monkeypatch.setattr(gateway, 'prepare_release', lambda *_: pytest.fail('unauthorized release preparation'))
    for command in ('id', 'loom-nebius-development-runtime-install-v1', 'loom-nebius-shared-pool-forward-guards-v1'):
        monkeypatch.setattr(sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(raw)))
        monkeypatch.setenv('SSH_ORIGINAL_COMMAND', command)
        assert gateway.authorized_main(hashlib.sha256(raw).hexdigest()) == 126
    monkeypatch.setenv('SSH_ORIGINAL_COMMAND', 'loom-nebius-shared-pool-install-v1')
    monkeypatch.setattr(sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(raw)))
    assert gateway.authorized_main('0' * 64) == 126


def test_shared_report_checks_direction_and_truthful_admission_without_exporting_inputs(tmp_path):
    from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES, RECOVERY_PHASES

    gateway = module()
    cases = [('preflight', report(tmp_path, 'shared_pool_preflight_qualified', opened=opened)) for opened in (False, True)]
    cases += [('install', report(tmp_path, 'pending', phase=phase)) for phase in FORWARD_PHASES]
    cases += [('recover', report(tmp_path, 'pending', phase=phase)) for phase in RECOVERY_PHASES]
    cases += [('install', report(tmp_path, 'shared_pool_handoff_complete', opened=True)),
        ('recover', report(tmp_path, 'staging_restored_dev_closed'))]
    for action, value in cases:
        assert gateway.safe_report(json.dumps(value | {'private': 'not-exported'}).encode(), operation(tmp_path), action) == value
        for wrong in ({'namespace': 'loom-staging'}, {'operation_id': 'not-an-id'}, {'pool_id': 'private-data'},
                {'admission_open': 'true'}, {'writer_migration_complete': not value['writer_migration_complete']},
                {'phase': 'recovery-fence' if action == 'install' else 'forward-guards'}):
            with pytest.raises(gateway.GatewayError):
                gateway.safe_report(json.dumps(value | wrong).encode(), operation(tmp_path), action)
    for action in ('preflight', 'install', 'recover'):
        blocked = {'status': 'blocked', 'stage': 'handoff'}
        result = gateway.safe_report(json.dumps(blocked | {'private': 'not-exported'}).encode(), operation(tmp_path), action)
        assert result == blocked | {key: operation(tmp_path)[key] for key in ('source_sha', 'installation_id', 'namespace', 'operation_id')}
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(blocked | {'stage': 'private-secret'}).encode(), operation(tmp_path), action)
    for action, status, opened in (('recover', 'shared_pool_handoff_complete', True),
            ('install', 'staging_restored_dev_closed', False), ('install', 'pending', True)):
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(json.dumps(report(tmp_path, status, opened=opened)).encode(), operation(tmp_path), action)


def test_shared_release_preparation_and_acknowledgement_preserve_pending_and_blocked(tmp_path, monkeypatch, capsys):
    gateway = module()
    selected = operation(tmp_path)
    root = Path(selected['inputs_path']).parent
    root.mkdir(mode=0o700, parents=True)
    raw, calls = archive(members(tmp_path)), []
    output = {'status': 'tooling_qualified'}

    def run(argv, **kwargs):
        calls.append(argv)
        return json.dumps(output).encode() if argv[-1] in ('qualify', 'install', 'recover', 'preflight') else b''

    monkeypatch.setattr(gateway, 'run_private', run)
    release = gateway.prepare_release(raw)
    assert release.parent == root / 'releases' and (release / 'complete').read_bytes() == b'complete'
    assert len(calls) == 4 and '--require-hashes' in calls[1] and '--offline' in calls[2]
    assert gateway.prepare_release(raw) == release and len(calls) == 4
    assert not (root / 'inputs.json').exists()
    for action, value in (('preflight', report(tmp_path, 'shared_pool_preflight_qualified')),
            ('install', report(tmp_path, 'pending', phase='forward-guards')),
            ('recover', {'status': 'blocked', 'stage': 'handoff'})):
        output = copy.deepcopy(value)
        monkeypatch.setattr(sys, 'stdin', SimpleNamespace(buffer=io.BytesIO(raw)))
        monkeypatch.setenv('SSH_ORIGINAL_COMMAND', 'loom-nebius-shared-pool-' + action + '-v1')
        assert gateway.authorized_main(hashlib.sha256(raw).hexdigest()) == 0
        result = json.loads(capsys.readouterr().out)
        assert result['status'] == value['status'] and calls[-1][-1] == action
    before = len(calls)
    (release / 'shared-pool-source.json').write_bytes(b'changed')
    with pytest.raises(gateway.GatewayError):
        gateway.prepare_release(raw)
    assert len(calls) == before
    assert not (tmp_path / '.loom/nebius-development-runtime').exists()


def test_shared_installed_key_preserves_existing_authority_and_refuses_foreign_commands(tmp_path):
    (tmp_path / '.loom').mkdir(mode=0o700)
    ssh = tmp_path / '.ssh'
    ssh.mkdir(mode=0o700)
    keys = ssh / 'authorized_keys'
    previous = b'# retained runtime and staging grants\nrestrict,command="staging" ssh-ed25519 FOREIGN\n'
    keys.write_bytes(previous)
    keys.chmod(0o600)
    files = members(tmp_path)
    # The bootstrap's authority selection/hash checks are real. Bundle transport
    # and actual entry imports are independently exercised above.
    files['scripts/ops/nebius_shared_pool_delivery_gateway.py'] = b'def authorized_main(digest):\n    return 0\n'
    raw = archive(files)
    wire = struct.pack('>I', 11) + b'ssh-ed25519' + struct.pack('>I', 32) + b'i' * 32
    key = 'ssh-ed25519 ' + base64.b64encode(wire).decode()
    checksum = hashlib.sha256(raw).hexdigest()
    grant = module('grant')
    assert grant.install(raw, expected_sha256=checksum, public_key=key)['status'] == 'prepared'
    assert keys.read_bytes() == previous
    result = grant.install(raw, expected_sha256=checksum, public_key=key, apply=True)
    after = keys.read_bytes()
    assert after.startswith(previous) and len(after.splitlines()) == 3
    assert grant.install(raw, expected_sha256=checksum, public_key=key, apply=True) == result
    assert keys.read_bytes() == after
    entry = Path(operation(tmp_path)['inputs_path']).parent / 'authority' / checksum / 'entrypoint.py'

    def invoke(command):
        return subprocess.run([sys.executable, '-I', '-S', str(entry)], capture_output=True, timeout=10,
            env={'PATH': os.defpath, 'SSH_ORIGINAL_COMMAND': command}).returncode

    for action in ('preflight', 'install', 'recover'):
        assert invoke('loom-nebius-shared-pool-' + action + '-v1') == 0
    for command in ('id', 'loom-nebius-development-runtime-install-v1', 'loom-nebius-management-install-v1',
            'loom-nebius-shared-pool-forward-guards-v1'):
        assert invoke(command) == 126
    (entry.parent / 'scripts/ops/nebius_shared_pool_command.py').write_bytes(b'changed')
    assert invoke('loom-nebius-shared-pool-install-v1') == 126
    assert keys.read_bytes() == after and not (tmp_path / '.loom/nebius-development-runtime').exists()


def test_shared_publisher_is_reproducible_and_uses_only_fixed_authenticated_transport(tmp_path, monkeypatch):
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

    def ssh(argv, **options):
        calls.append((argv, options))
        status = 'staging_restored_dev_closed' if argv[-1].endswith('recover-v1') else 'shared_pool_preflight_qualified'
        return SimpleNamespace(returncode=0, stdout=json.dumps(report(tmp_path, status)).encode())

    monkeypatch.setattr(publisher.subprocess, 'run', ssh)
    for action in ('preflight', 'recover'):
        assert publisher.transfer(raw, action=action, target='operator@host', key=tmp_path / 'key',
            known_hosts=tmp_path / 'hosts')['source_sha'] == operation(tmp_path)['source_sha']
        assert calls[-1][0][-1] == 'loom-nebius-shared-pool-' + action + '-v1'
        assert 'StrictHostKeyChecking=yes' in calls[-1][0] and calls[-1][1]['input'] == raw
    for action in ('forward-guards', 'development-runtime-install'):
        with pytest.raises(publisher.RolloutError):
            publisher.transfer(raw, action=action, target='operator@host', key=tmp_path / 'key', known_hosts=tmp_path / 'hosts')
    assert len(calls) == 2
    monkeypatch.setattr(publisher.subprocess, 'run', lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError('lost reply')))
    with pytest.raises(publisher.RolloutError, match='reconcile before retry'):
        publisher.transfer(raw, action='install', target='operator@host', key=tmp_path / 'key', known_hosts=tmp_path / 'hosts')


@pytest.mark.parametrize('damage', [None, 'dirty', 'untracked', 'wrong-head', 'not-integrated'])
def test_shared_publisher_requires_exact_clean_integrated_source(tmp_path, monkeypatch, damage):
    publisher = module('rollout')
    checkout = tmp_path / 'checkout'
    checkout.mkdir()

    def git(*args):
        return subprocess.run(['git', *args], cwd=checkout, check=True, capture_output=True, text=True).stdout.strip()

    git('init', '-q')
    git('config', 'user.name', 'Test')
    git('config', 'user.email', 'test@example.invalid')
    (checkout / 'source').write_text('approved\n')
    git('add', 'source')
    git('commit', '-qm', 'initial')
    sha = git('rev-parse', 'HEAD')
    if damage != 'not-integrated':
        git('update-ref', 'refs/remotes/origin/dev', sha)
    selected = operation(tmp_path) | {'source_sha': sha}
    if damage == 'dirty':
        (checkout / 'source').write_text('changed')
    elif damage == 'untracked':
        (checkout / 'unexpected').write_text('untracked')
    elif damage == 'wrong-head':
        selected['source_sha'] = 'b' * 40
    monkeypatch.setattr(publisher, 'ROOT', checkout)
    if damage:
        with pytest.raises(publisher.RolloutError):
            publisher.verify_source(selected)
    else:
        publisher.verify_source(selected)


@pytest.mark.parametrize('action', ['preflight', 'install', 'recover'])
def test_shared_workflow_executes_protected_selector_and_cleans_its_key(tmp_path, action):
    flow = yaml.safe_load((ROOT / '.github/workflows/nebius-rollout.yml').read_text())
    choices = flow.get('on', flow.get(True))['workflow_dispatch']['inputs']['operation']['options']
    assert 'shared-pool-' + action in choices
    job = flow['jobs']['shared-pool']
    assert job['permissions'] == {'contents': 'read'}
    assert job['environment'] == {'name': 'nebius-integration', 'deployment': False}
    assert 'workflow_dispatch' in job['if'] and 'refs/heads/dev' in job['if']
    assert flow['concurrency']['cancel-in-progress'] is False
    selector = next(step for step in job['steps'] if step.get('id') == 'tooling')
    runner = next(step for step in job['steps'] if step.get('name') == 'Run fixed shared pool operation')
    assert runner['env']['DEPLOY_SSH_KEY'] == '${{ secrets.NEBIUS_SHARED_POOL_SSH_KEY }}'
    assert runner['env']['NEBIUS_SHARED_POOL_OPERATION_JSON'] == '${{ vars.NEBIUS_SHARED_POOL_OPERATION_JSON }}'
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    git = bindir / 'git'
    git.write_text('#!/bin/sh\ntest "$1 $2 $3 $4" = "merge-base --is-ancestor $EXPECTED_SHA refs/remotes/origin/dev"\n')
    git.chmod(0o700)
    uv = bindir / 'uv'
    uv.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
assert sys.argv[sys.argv.index('--operation') + 1] == os.environ['EXPECTED_ACTION']
assert 'scripts.ops.nebius_shared_pool_rollout' in sys.argv
assert pathlib.Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE']).read_text().strip() == 'private-shared-key'
assert json.loads(os.environ['NEBIUS_SHARED_POOL_OPERATION_JSON'])['namespace'] == 'loom-nebius-management-dev'
pathlib.Path(os.environ['RUNNER_TEMP'], 'transport-invoked').write_text('selected')
''')
    uv.chmod(0o700)
    env = os.environ | {'PATH': str(bindir) + ':' + os.environ['PATH'], 'SHARED_POOL_OPERATION': 'shared-pool-' + action,
        'NEBIUS_SHARED_POOL_OPERATION_JSON': json.dumps(operation(tmp_path)), 'DEPLOY_SSH_KEY': 'private-shared-key',
        'DEPLOY_KNOWN_HOSTS': 'test-host', 'LOOM_DEPLOY_SSH_TARGET': 'test-target', 'RUNNER_TEMP': str(tmp_path),
        'GITHUB_OUTPUT': str(tmp_path / 'outputs'), 'EXPECTED_SHA': 'a' * 40, 'EXPECTED_ACTION': action}
    for step in (selector, runner):
        result = subprocess.run(['bash', '-e', '-c', step['run']], env=env, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
        assert 'private-shared-key' not in result.stdout + result.stderr
    assert (tmp_path / 'outputs').read_text() == 'sha=' + 'a' * 40 + '\n'
    assert (tmp_path / 'transport-invoked').read_text() == 'selected'
    assert not (tmp_path / 'nebius-shared-pool-key').exists()
    (tmp_path / 'transport-invoked').unlink()
    for damage in ({'DEPLOY_SSH_KEY': ''}, {'SHARED_POOL_OPERATION': 'management-install'}):
        result = subprocess.run(['bash', '-e', '-c', runner['run']], env=env | damage, capture_output=True, timeout=20)
        assert result.returncode != 0 and not (tmp_path / 'transport-invoked').exists()
    selected = operation(tmp_path) | {'namespace': 'loom-nebius-platform'}
    result = subprocess.run(['bash', '-e', '-c', selector['run']],
        env=env | {'NEBIUS_SHARED_POOL_OPERATION_JSON': json.dumps(selected)}, capture_output=True, timeout=20)
    assert result.returncode != 0
