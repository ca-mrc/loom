"""A new participant key cannot replace or inherit staging/handoff authority."""
from __future__ import annotations

import base64
import hashlib
import importlib
import json
import os
import struct
import subprocess
import sys
from pathlib import Path

import pytest
from tests.ops.test_nebius_development_management_gateway import archive
from tests.ops.test_nebius_pool_participant_gateway import members
from tests.ops.test_nebius_shared_pool_delivery import operation


def module():
    name = 'scripts.ops.install_nebius_pool_participant_entrypoint'
    assert importlib.util.find_spec(name) is not None, 'separate participant grant installer is missing'
    return importlib.import_module(name)


@pytest.fixture
def installation(tmp_path, monkeypatch):
    from scripts.ops import nebius_pool_participant_gateway as gateway

    for name in ('.loom', '.ssh'):
        (tmp_path / name).mkdir(mode=0o700)
    keys = tmp_path / '.ssh/authorized_keys'
    keys.write_bytes(b'# retained staging and handoff authority\nrestrict,command="staging" ssh-ed25519 FOREIGN\n')
    keys.chmod(0o600)
    original = operation(tmp_path)
    path = Path(original['inputs_path']).parent / 'operation.json'
    path.parent.mkdir(mode=0o700, parents=True)
    path.write_text(json.dumps(original))
    path.chmod(0o600)
    owner = tmp_path / '.loom'
    identity = original['installation_id']
    config = {'schema_version': 'loom.nebius-participant-host.v1',
        'shared_operation': str(path), 'shared_operation_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'state_root': str(owner / 'nebius-participant-releases' / identity),
        'anchor_dir': str(owner / 'nebius-participant-anchors' / identity),
        'operator_connection': {'endpoint': 'https://cluster.example.test',
            'ca_file': str(owner / 'current-ca.pem'), 'credentials_file': str(owner / 'current-operator.json')},
        'executable': '/usr/bin/kubectl'}
    for name in ('current-ca.pem', 'current-operator.json'):
        private = owner / name
        private.write_bytes(b'private-test-material')
        private.chmod(0o600)
    configuration = owner / 'participant-host.json'

    def save():
        raw = json.dumps(config).encode()
        configuration.write_bytes(raw)
        configuration.chmod(0o600)
        return hashlib.sha256(raw).hexdigest()

    save()
    wire = struct.pack('>I', 11) + b'ssh-ed25519' + struct.pack('>I', 32) + b'i' * 32
    key = 'ssh-ed25519 ' + base64.b64encode(wire).decode()
    calls = []

    def process(argv, *, timeout):
        calls.append(argv)
        if argv[1:3] == ['-I', '-c']:
            return json.dumps({'status': 'host_qualified' if 'load_participant_release_host' in argv[3]
                else 'tooling_qualified'}).encode()
        return b''

    monkeypatch.setattr(gateway, 'run_private', process)
    return keys, config, configuration, save, key, calls


def test_installer_preserves_other_keys_and_binds_exact_tooling_and_configuration(installation, tmp_path):
    keys, config, configuration, save, key, calls = installation
    files = members()
    # This tests the real forced-command bootstrap and key installation. The
    # actual gateway/watchdog and extracted-wheel imports have separate tests.
    files['scripts/ops/nebius_pool_participant_gateway.py'] = b'def authorized_main(*args):\n    return 0\n'
    raw = archive(files)
    checksum = hashlib.sha256(raw).hexdigest()
    options = dict(expected_sha256=checksum, public_key=key, configuration=configuration,
        configuration_sha256=save())
    previous = keys.read_bytes()
    installer = module()
    planned = installer.install(raw, **options)
    assert planned['status'] == 'prepared' and keys.read_bytes() == previous and not calls
    installed = installer.install(raw, **options, apply=True)
    assert installed['status'] == 'installed' and installed['configuration_sha256'] == options['configuration_sha256']
    for name in ('state_root', 'anchor_dir'):
        directory = Path(config[name])
        assert directory.is_dir(), 'host cannot create release history without its private parent directories'
        assert directory.stat().st_mode & 0o777 == 0o700 and list(directory.iterdir()) == []
    after = keys.read_bytes()
    assert after.startswith(previous) and len(after.splitlines()) == 3
    assert len(calls) == 5 and 'load_participant_release_host' in calls[-1][3]
    assert installer.install(raw, **options, apply=True) == installed and keys.read_bytes() == after
    root = tmp_path / '.loom/nebius-participant-delivery' / Path(config['state_root']).name
    entry, = root.glob('authority/*/entrypoint.py')

    def invoke(command):
        result = subprocess.run([sys.executable, '-I', '-S', str(entry)], capture_output=True, timeout=10,
            env={'PATH': os.defpath, 'SSH_ORIGINAL_COMMAND': command})
        assert result.stdout == result.stderr == b''
        return result.returncode

    for action in ('prepare', 'execute', 'recover'):
        assert invoke('loom-nebius-participant-' + action + '-v1') == 0
    for command in ('id', 'loom-nebius-shared-pool-install-v1', 'loom-nebius-development-runtime-install-v1',
            'loom-nebius-participant-execute-v1 --source /runner/source'):
        assert invoke(command) == 126
    bootstrap = entry.parent / 'scripts/ops/nebius_pool_participant_gateway.py'
    bootstrap.write_bytes(b'def authorized_main(*args):\n    return 0\n# changed\n')
    assert invoke('loom-nebius-participant-execute-v1') == 126
    assert keys.read_bytes() == after


@pytest.mark.parametrize('damage', ['bundle', 'configuration', 'history', 'state-scope', 'anchor-scope',
    'history-alias', 'credential-public', 'key-reuse'])
def test_installer_rejects_wrong_scope_before_tooling_or_key_mutation(installation, tmp_path, damage):
    keys, config, configuration, save, key, calls = installation
    raw = archive(members())
    checksum = hashlib.sha256(raw).hexdigest()
    if damage == 'history':
        config['shared_operation_sha256'] = 'f' * 64
    elif damage == 'state-scope':
        config['state_root'] = str(tmp_path / '.loom/nebius-participant-releases/foreign')
    elif damage == 'anchor-scope':
        config['anchor_dir'] = str(tmp_path / '.loom/nebius-participant-anchors/foreign')
    elif damage == 'history-alias':
        config['operator_connection']['credentials_file'] = config['shared_operation']
    elif damage == 'credential-public':
        Path(config['operator_connection']['credentials_file']).chmod(0o644)
    elif damage == 'key-reuse':
        keys.write_bytes(keys.read_bytes() + ('restrict,command="existing-handoff" ' + key + '\n').encode())
    configuration_sha256 = save()
    previous = keys.read_bytes()
    installer = module()
    with pytest.raises(installer.InstallError):
        installer.install(raw, expected_sha256='e' * 64 if damage == 'bundle' else checksum,
            public_key=key, configuration=configuration,
            configuration_sha256='d' * 64 if damage == 'configuration' else configuration_sha256, apply=True)
    assert keys.read_bytes() == previous and not calls
    assert not (tmp_path / '.loom/nebius-participant-delivery').exists()


@pytest.mark.parametrize('change', ['config', 'credential', 'qualification', 'tooling', 'concurrent-key'])
def test_installer_rechecks_private_selection_and_preserves_concurrent_keys(installation, monkeypatch, change):
    from scripts.ops import nebius_pool_participant_gateway as gateway

    keys, config, configuration, save, key, _ = installation
    raw = archive(members())
    options = dict(expected_sha256=hashlib.sha256(raw).hexdigest(), public_key=key,
        configuration=configuration, configuration_sha256=save(), apply=True)
    before = keys.read_bytes()
    concurrent = b'# concurrently installed unrelated grant\n'
    original = gateway.run_private

    def process(argv, **kwargs):
        result = original(argv, **kwargs)
        if argv[1:3] == ['-I', '-c'] and 'load_participant_release_host' in argv[3]:
            if change == 'config':
                configuration.write_bytes(configuration.read_bytes() + b'\n')
            elif change == 'credential':
                Path(config['operator_connection']['credentials_file']).write_bytes(b'rotated')
            elif change == 'qualification':
                return b'{"status":"unqualified"}'
            elif change == 'tooling':
                (Path(argv[4]) / 'scripts/ops/nebius_pool_participant_entry.py').write_bytes(b'changed')
            else:
                keys.write_bytes(before + concurrent)
        return result

    monkeypatch.setattr(gateway, 'run_private', process)
    installer = module()
    if change == 'concurrent-key':
        assert installer.install(raw, **options)['status'] == 'installed'
        assert keys.read_bytes().startswith(before + concurrent)
    else:
        with pytest.raises((installer.InstallError, gateway.GatewayError)):
            installer.install(raw, **options)
        assert keys.read_bytes() == before
