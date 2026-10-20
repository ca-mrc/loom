"""Installed lifecycle input authority, independent of runner-selected paths."""
from __future__ import annotations

import hashlib
import importlib
import json
from uuid import uuid4

import pytest


def private(path, body):
    raw = json.dumps(body, sort_keys=True).encode()
    path.write_bytes(raw)
    path.chmod(0o600)
    return hashlib.sha256(raw).hexdigest()


def configuration(tmp_path):
    scope = dict(installation_id=str(uuid4()), pool_id=str(uuid4()), participant_id=str(uuid4()),
        environment_id=str(uuid4()), incarnation=str(uuid4()), authority='development')
    operation = str(uuid4())
    inputs = tmp_path / 'inputs.json'
    checksum = private(inputs, {'schema_version': 'loom.environment-lifecycle-inputs.v2'})
    body = dict(schema_version='loom.environment-lifecycle-configuration.v2', scope=scope,
        operations={operation: dict(kind='activate', inputs_path=str(inputs), inputs_sha256=checksum)})
    path = tmp_path / 'configuration.json'
    sha = private(path, body)
    return path, sha, body, operation, inputs


def test_private_configuration_binds_exact_scope_operation_and_bytes(tmp_path):
    name = 'scripts.ops.nebius_environment_lifecycle_entry'
    assert importlib.util.find_spec(name), 'installed lifecycle entry missing'
    entry = importlib.import_module(name)
    path, sha, body, operation, inputs = configuration(tmp_path)
    config = entry.load_lifecycle_configuration(path, sha)
    request = dict(schema_version='loom.environment-lifecycle-command.v2', action='prepare',
        operation_id=operation, scope=body['scope'])
    selected = entry.select_operation(config, request)
    assert selected.kind == 'activate'
    assert selected.inputs_path == inputs
    for changed in ({**request, 'operation_id': str(uuid4())},
                    {**request, 'scope': {**request['scope'], 'participant_id': str(uuid4())}},
                    {**request, 'inputs_path': '/tmp/untrusted'}):
        with pytest.raises(ValueError):
            entry.select_operation(config, changed)
    inputs.write_text('{}')
    with pytest.raises(ValueError):
        entry.load_lifecycle_configuration(path, sha)


@pytest.mark.parametrize('change', ['symlink', 'permissions', 'duplicate', 'nil', 'hash'])
def test_lifecycle_configuration_rejects_ambiguous_or_mutable_authority(tmp_path, change):
    name = 'scripts.ops.nebius_environment_lifecycle_entry'
    assert importlib.util.find_spec(name), 'installed lifecycle entry missing'
    entry = importlib.import_module(name)
    path, sha, body, operation, inputs = configuration(tmp_path)
    if change == 'symlink':
        link = tmp_path / 'link.json'
        link.symlink_to(inputs)
        body['operations'][operation]['inputs_path'] = str(link)
        sha = private(path, body)
    elif change == 'permissions':
        path.chmod(0o644)
    elif change == 'duplicate':
        raw = path.read_text().replace('"scope":', '"scope": {}, "scope":')
        path.write_text(raw)
        sha = hashlib.sha256(raw.encode()).hexdigest()
    elif change == 'nil':
        body['operations']['00000000-0000-0000-0000-000000000000'] = body['operations'].pop(operation)
        sha = private(path, body)
    else:
        sha = '0' * 64
    with pytest.raises(ValueError):
        entry.load_lifecycle_configuration(path, sha)
