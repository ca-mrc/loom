"""A storage retry retains the failed recovery's exact source and projection."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.pipeline.keys import canonical_document, digest_bytes
from loom_control_plane import pending_archive_retry as target
from loom_control_plane.service_execution_materializer import MaterializationIntegrityError
from tests.unit.test_pending_archive_recovery import qualified


def requeued():
    original, lease, trial, artifact, now = qualified()
    request = target.ArchiveRetryRequest(operation_id=uuid4(), team_id=original.team_id,
        lease_id=original.lease_id, previous_request_sha256=original.digest,
        previous_claim_id=uuid4(), candidate_sha='c' * 40, schema_head='0175')
    old_audit = {'request': original.model_dump(mode='json'), 'request_sha256': original.digest,
        'claim_id': str(request.previous_claim_id), 'previous_materialization_attempts': 46,
        'failure': {'code': 'recovery_incomplete', 'claim_id': str(request.previous_claim_id)}}
    artifact.artifact_metadata = {target.AUDIT_KEY: old_audit, target.RETRY_AUDIT_KEY: {
        'request': request.model_dump(mode='json'), 'request_sha256': request.digest,
        'requested_at': now.isoformat(), 'original_audit_sha256': digest_bytes(canonical_document(old_audit))}}
    lease.materialization_recovery_requested_at = now
    lease.source_retain_until = None
    return original, request, lease, trial, artifact


@pytest.mark.parametrize('damage', ['missing-retry', 'both-audits-missing', 'claim', 'original-audit', 'time', 'request', 'projection',
                                  'events-version', 'atif-version', 'file-version', 'source-version', None])
async def test_ordinary_worker_preserves_original_recovery_qualification(monkeypatch, damage):
    original, _, lease, trial, artifact = requeued()
    audit = artifact.artifact_metadata[target.RETRY_AUDIT_KEY]
    if damage == 'missing-retry':
        del artifact.artifact_metadata[target.RETRY_AUDIT_KEY]
    elif damage == 'both-audits-missing':
        artifact.artifact_metadata = {}
    elif damage == 'claim':
        audit['request']['previous_claim_id'] = str(uuid4())
    elif damage == 'original-audit':
        artifact.artifact_metadata[target.AUDIT_KEY]['failure']['code'] = 'changed'
    elif damage == 'time':
        lease.materialization_recovery_requested_at = None
    elif damage == 'request':
        audit['request_sha256'] = 'sha256:' + '0' * 64
    qualify = AsyncMock(return_value=original)
    monkeypatch.setattr(target, '_qualify', qualify)
    result = SimpleNamespace(events_sha256=original.events_sha256, atif_sha256=original.atif_sha256,
        events_version_id='v1', atif_version_id='v2', files=[SimpleNamespace(version_id='v3')],
        source_evidence=[SimpleNamespace(version_id='v4')])
    if damage == 'projection':
        result.events_sha256 = 'sha256:' + '0' * 64
    if damage == 'events-version':
        result.events_version_id = 'null'
    if damage == 'atif-version':
        result.atif_version_id = None
    if damage == 'file-version':
        result.files[0].version_id = ''
    if damage == 'source-version':
        result.source_evidence[0].version_id = None
    if damage:
        with pytest.raises(MaterializationIntegrityError, match='archive_retry_qualification_failed'):
            await target.qualify_requeued_archive(None, None, lease, trial, artifact, result)
    else:
        await target.qualify_requeued_archive(None, None, lease, trial, artifact, result)
        qualify.assert_awaited_once()


@pytest.mark.parametrize('field,value', [('attempt_count', 2), ('state', 'failed'), ('team_id', uuid4())])
async def test_changed_trial_refuses_before_projection(monkeypatch, field, value):
    _, request, lease, trial, artifact = requeued()
    setattr(trial, field, value)
    project = AsyncMock()
    monkeypatch.setattr(target, 'project_oracle', project)
    with pytest.raises(target.RecoveryRefusedError):
        await target._qualify(None, None, request, lease, trial, artifact)
    project.assert_not_awaited()


@pytest.mark.parametrize('damage', ['candidate', 'namespace', 'schema', None])
def test_cli_requires_installed_candidate_and_lifecycle_scope(tmp_path, monkeypatch, damage):
    import json

    from loom.db.schema_startup import service_schema_head

    _, request, _, _, _ = requeued()
    request = request.model_copy(update={'schema_head': service_schema_head()})
    monkeypatch.setenv('LOOM_NAMESPACE', 'loom-test')
    (tmp_path / 'profile.json').write_text(json.dumps({'candidate_sha': request.candidate_sha}))
    (tmp_path / 'environment.json').write_text(json.dumps({'namespace': 'loom-test'}))
    if damage == 'candidate':
        request = request.model_copy(update={'candidate_sha': 'd' * 40})
    elif damage == 'namespace':
        monkeypatch.setenv('LOOM_NAMESPACE', 'foreign')
    elif damage == 'schema':
        request = request.model_copy(update={'schema_head': '0174'})
    if damage:
        with pytest.raises(ValueError):
            target.qualify_retry_platform(request, tmp_path)
    else:
        target.qualify_retry_platform(request, tmp_path)


async def test_isolated_worker_rechecks_scope_immediately_before_copy(monkeypatch):
    from loom_control_plane import pending_archive_recovery as recovery

    original, _, lease, trial, artifact = requeued()
    preflight = AsyncMock(side_effect=recovery.RecoveryRefusedError('lifecycle_namespace_changed'))
    monkeypatch.setattr(recovery, 'qualify_inputs', preflight)
    worker = object.__new__(recovery.OracleRecoveryMaterializer)
    worker.request = original
    with pytest.raises(recovery.RecoveryRefusedError, match='lifecycle_namespace_changed'):
        await worker._qualify_before_copy(None, lease, trial, artifact)
    preflight.assert_awaited_once()


def test_readback_keeps_historical_request_after_platform_advance(tmp_path, monkeypatch):
    import json

    _, request, _, _, _ = requeued()
    request = request.model_copy(update={'schema_head': '0174'})
    monkeypatch.setenv('LOOM_NAMESPACE', 'loom-test')
    (tmp_path / 'profile.json').write_text(json.dumps({'candidate_sha': 'd' * 40}))
    (tmp_path / 'environment.json').write_text(json.dumps({'namespace': 'loom-test'}))
    target.qualify_retry_platform(request, tmp_path, readback=True)
    monkeypatch.setenv('LOOM_NAMESPACE', 'foreign')
    with pytest.raises(ValueError):
        target.qualify_retry_platform(request, tmp_path, readback=True)
