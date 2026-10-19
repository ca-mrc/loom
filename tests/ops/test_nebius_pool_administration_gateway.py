"""V2 tooling is pinned separately; old handoff grants do not gain commands."""
from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_management_gateway import archive

ROOT = Path(__file__).resolve().parents[2]


def module():
    name = 'scripts.ops.nebius_pool_administration_gateway'
    assert importlib.util.find_spec(name), 'independent administration tooling boundary is missing'
    return importlib.import_module(name)


def members():
    return {**{name: (ROOT / name).read_bytes() for name in module().SOURCES}, 'uv': b'test-uv',
        'requirements.txt': b'test==1 --hash=sha256:' + b'a' * 64,
        'wheels/loom-0.1.0-py3-none-any.whl': b'test-wheel',
        'wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl': b'test-wheel',
        'administration-tooling.json': json.dumps({'schema_version': 'loom.pool-administration-tooling.v2',
            'source_sha': 'a' * 40, 'source_archive_sha256': 'sha256:' + 'b' * 64}).encode()}


def reference():
    return dict(schema_version='loom.pool-administration-reference.v2', installation_id=str(uuid4()), pool_id=str(uuid4()),
        participant_id=str(uuid4()), environment_id=str(uuid4()), incarnation=str(uuid4()), authority='development',
        authorization_id=str(uuid4()), operation_id=str(uuid4()), request_sha256='d' * 64, document_sha256='e' * 64)


@pytest.mark.parametrize('damage', [None, 'private-input', 'missing', 'source', 'unknown-wheel'])
def test_only_exact_reviewed_tooling_is_installable(damage):
    gateway = module()
    files = members()
    if damage == 'private-input':
        files['host.json'] = b'private-marker'
    elif damage == 'missing':
        del files['scripts/ops/nebius_pool_administration_dispatch.py']
    elif damage == 'source':
        files['administration-tooling.json'] = b'{}'
    elif damage == 'unknown-wheel':
        files['wheels/unreviewed-0.1.0-py3-none-any.whl'] = b'foreign'
    if damage:
        with pytest.raises(ValueError):
            gateway.unpack_bundle(archive(files))
    else:
        _, source = gateway.unpack_bundle(archive(files))
        assert source['source_sha'] == 'a' * 40


def test_private_tooling_preparation_is_idempotent_but_tamper_never_reinstalls(tmp_path, monkeypatch):
    gateway = module()
    calls = []

    def run(argv, *, timeout):
        calls.append(argv)
        return b'{"status":"tooling_qualified"}' if argv[1:3] == ['-I', '-c'] else b''

    monkeypatch.setattr(gateway, 'run_private', run)
    root = tmp_path / 'tooling'
    root.mkdir(mode=0o700)
    wire = archive(members())
    release = gateway.prepare_release(wire, root=root)
    original = list(calls)
    assert gateway.prepare_release(wire, root=root) == release and calls == original
    assert any('--require-hashes' in argv for argv in calls)
    assert any('--offline' in argv and '--no-deps' in argv for argv in calls)
    (release / 'scripts/ops/nebius_pool_administration_dispatch.py').write_bytes(b'foreign')
    with pytest.raises(ValueError):
        gateway.prepare_release(wire, root=root)
    assert calls == original


@pytest.mark.parametrize('damage', [None, 'private-field', 'wrong-target', 'wrong-action', 'proof-extra'])
def test_public_report_only_projects_bound_receipt_identity(damage):
    gateway = module()
    ref = reference()
    incoming = dict(schema_version='loom.pool-administration-command.v2', action='apply', reference=ref)
    receipt = dict(schema_version='loom.pool-administration-result.v2', authorization_id=ref['authorization_id'],
        request_sha256=ref['request_sha256'], receipt={}, current=None)
    report = dict(schema_version='loom.pool-administration-dispatch.v2', action='apply', reference=dict(ref),
        observation_id=None, status='complete', proof=dict(job_uid=str(uuid4()), pod_uid=str(uuid4()), receipt=receipt))
    if damage == 'private-field':
        report['private'] = 'private-marker'
    elif damage == 'wrong-target':
        report['reference']['participant_id'] = str(uuid4())
    elif damage == 'wrong-action':
        report['action'] = 'recover'
    elif damage == 'proof-extra':
        report['proof']['private'] = 'private-marker'
    if damage:
        with pytest.raises(ValueError):
            gateway.safe_report(json.dumps(report).encode(), json.dumps(incoming).encode(), 'apply')
    else:
        output = gateway.safe_report(json.dumps(report).encode(), json.dumps(incoming).encode(), 'apply')
        assert output['status'] == 'complete' and output['reference'] == ref
        assert output['receipt_sha256'] == hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        assert 'proof' not in output, 'private receipt content is not public runner output'
