"""Install a separately scoped key without inheriting any legacy host history."""
from __future__ import annotations

import base64
import hashlib
import importlib
import json
import os
import struct
import subprocess
import sys
from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_management_gateway import archive
from tests.ops.test_nebius_pool_administration_gateway import members


def installer():
    name = 'scripts.ops.install_nebius_pool_administration_entrypoint'
    assert importlib.util.find_spec(name), 'independent scoped authority installer is missing'
    return importlib.import_module(name)


@pytest.fixture
def installation(tmp_path, monkeypatch):
    module = installer()
    from scripts.ops import nebius_pool_administration_gateway as gateway

    owner, ssh = tmp_path / '.loom', tmp_path / '.ssh'
    owner.mkdir(mode=0o700)
    ssh.mkdir(mode=0o700)
    keys = ssh / 'authorized_keys'
    keys.write_bytes(b'# retained staging key\nrestrict,command="staging" ssh-ed25519 FOREIGN\n')
    keys.chmod(0o600)
    identity = str(uuid4())
    scope = dict(installation_id=identity, pool_id=str(uuid4()), participant_id=str(uuid4()),
        environment_id=str(uuid4()), incarnation=str(uuid4()), authority='development')
    config = dict(schema_version='loom.pool-administration-host.v2', scope=scope,
        runtime=dict(binding=dict(schema_version='loom.platform-namespace.v2', installation_id=identity,
            namespace='platform-west', namespace_uid=str(uuid4()), kube_system_uid=str(uuid4())),
            service_image='registry.example/service@sha256:' + 'e' * 64,
            database_secret='ledger-db', database_url_key='admin-url', database_ca_key='ca.crt',
            node_selector={'example.org/role': 'platform'}, tolerations=[], region='eu-north1'),
        state_root=str(owner / 'operations'), anchor_root=str(owner / 'anchors'),
        connection=dict(endpoint='https://cluster.example', ca_file=str(owner / 'ca.pem'), credentials_file=str(owner / 'credential.json')))
    path = owner / 'host.json'
    raw = json.dumps(config).encode()
    path.write_bytes(raw)
    path.chmod(0o600)
    for name in ('ca.pem', 'credential.json'):
        item = owner / name
        item.write_bytes(b'private-material')
        item.chmod(0o600)
    key = 'ssh-ed25519 ' + base64.b64encode(struct.pack('>I', 11) + b'ssh-ed25519' + struct.pack('>I', 32) + b'g' * 32).decode()
    files = members()
    # Exercise the real isolated bootstrap; real gateway imports are covered by
    # the extracted-wheel test, without host/package installation in this fixture.
    files['scripts/ops/nebius_pool_administration_gateway.py'] = b'def authorized_main(*args): return 37\n'
    bundle = archive(files)
    calls = []

    def run(argv, *, timeout):
        calls.append(argv)
        return b'{"status":"tooling_qualified"}' if argv[1:3] == ['-I', '-c'] else b''

    monkeypatch.setattr(gateway, 'run_private', run)
    return module, keys, bundle, dict(home=tmp_path, public_key=key,
        expected_sha256=hashlib.sha256(bundle).hexdigest(), configuration=path,
        configuration_sha256=hashlib.sha256(raw).hexdigest()), calls


def test_scoped_install_is_idempotent_and_old_keys_stay_byte_identical(installation, tmp_path):
    module, keys, bundle, options, calls = installation
    original = keys.read_bytes()
    assert module.install(bundle, **options)['status'] == 'prepared'
    assert keys.read_bytes() == original and not calls
    result = module.install(bundle, **options, apply=True)
    assert result['status'] == 'installed'
    after = keys.read_bytes()
    assert after.startswith(original) and len(after.splitlines()) == 3
    assert module.install(bundle, **options, apply=True) == result and keys.read_bytes() == after
    assert (tmp_path / '.loom/operations').is_dir() and (tmp_path / '.loom/anchors').is_dir()
    entry, = (tmp_path / '.loom/pool-administration-delivery').glob('*/authority/*/entrypoint.py')
    for command, expected in [('loom-pool-administration-prepare-v2', 37), ('bash', 126), ('loom-nebius-participant-execute-v1', 126)]:
        process = subprocess.run([sys.executable, '-I', '-S', str(entry)], env={'PATH': os.defpath, 'SSH_ORIGINAL_COMMAND': command},
            capture_output=True, timeout=10)
        assert process.returncode == expected
    (entry.parent / 'scripts/ops/nebius_pool_administration_gateway.py').write_bytes(b'foreign')
    process = subprocess.run([sys.executable, '-I', '-S', str(entry)],
        env={'PATH': os.defpath, 'SSH_ORIGINAL_COMMAND': 'loom-pool-administration-prepare-v2'}, capture_output=True, timeout=10)
    assert process.returncode == 126


def test_existing_key_cannot_gain_different_target_authority(installation):
    module, keys, bundle, options, _ = installation
    keys.write_text('restrict,command="production" ' + options['public_key'] + '\n')
    before = keys.read_bytes()
    with pytest.raises(ValueError):
        module.install(bundle, **options, apply=True)
    assert keys.read_bytes() == before
