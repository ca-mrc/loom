"""Fixed shared handoff delivery never widens the CLOSED runtime grant."""
from __future__ import annotations

import copy
import hashlib
import importlib
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.ops.test_nebius_development_management_gateway import archive

ROOT = Path(__file__).resolve().parents[2]


def module(suffix='delivery_gateway'):
    name = 'scripts.ops.nebius_shared_pool_' + suffix
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
    gateway = module()
    files, selected = gateway.unpack_bundle(archive(members(tmp_path)))
    assert selected == operation(tmp_path)
    release = tmp_path / 'isolated'
    for name, value in files.items():
        if name.startswith(('scripts/', 'deploy/')):
            path = release / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
    # The SSH bootstrap must work before the handoff venv/dependencies exist.
    bootstrap = subprocess.run([sys.executable, '-I', '-S', '-c',
        "import sys,json; sys.path.insert(0,sys.argv[1]); "
        "from scripts.ops.nebius_shared_pool_delivery_gateway import unpack_bundle; "
        "print(json.dumps(unpack_bundle(sys.stdin.buffer.read())[1],sort_keys=True))", str(release)],
        input=archive(members(tmp_path)), capture_output=True, cwd=tmp_path, timeout=20)
    assert bootstrap.returncode == 0, bootstrap.stderr.decode()
    assert json.loads(bootstrap.stdout) == selected
    # Use installed Python dependencies, but every scripts.ops import must come
    # from the shipped release, not the developer checkout or an AST-only check.
    imported = subprocess.run([sys.executable, '-I', '-c',
        "import sys,pathlib; root=pathlib.Path(sys.argv[1]); sys.path.insert(0,str(root)); "
        "from scripts.ops.nebius_shared_pool_entry import main; "
        "assert callable(main); "
        "assert all(pathlib.Path(m.__file__).is_relative_to(root) for n,m in sys.modules.items() "
        "if n.startswith('scripts.ops.') and getattr(m,'__file__',None)); print('entry imports')", str(release)],
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
