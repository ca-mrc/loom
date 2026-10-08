"""A recovery must never compete with a worker whose claim was taken over."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom_control_plane.pending_archive_recovery import (
    ArchiveRecoveryRequest,
    RecoveryRefusedError,
    qualify_pending,
)


def qualified():
    now = datetime.now(UTC)
    request = ArchiveRecoveryRequest(
        team_id=uuid4(), trial_id=uuid4(), lease_id=uuid4(), artifact_id=uuid4(),
        upload_session_id=uuid4(), attempt=1, generation=1,
        output_manifest_sha256='sha256:' + '1' * 64,
        output_marker_sha256='sha256:' + '2' * 64,
        runtime_result_sha256='sha256:' + '3' * 64,
        cluster_id='mk8s-test', namespace='loom-test', installed_candidate='a' * 40,
        candidate_sha='b' * 40, image_ref='cr.eu-north1.nebius.cloud/test/loom-control-plane@sha256:' + '4' * 64,
        installed_image_ref='cr.eu-north1.nebius.cloud/test/loom-control-plane@sha256:'+'a'*64,
        schema_head='0173', trial_config_sha256='sha256:'+'5'*64,
        task_config_sha256='sha256:'+'6'*64, derivation_sha256='sha256:'+'7'*64,
        events_sha256='sha256:'+'8'*64, atif_sha256='sha256:'+'9'*64,
    )
    lease = SimpleNamespace(
        id=request.lease_id, team_id=request.team_id, trial_id=request.trial_id,
        attempt=1, resource_generation=1, output_generation=1,
        output_upload_session_id=request.upload_session_id,
        output_manifest_sha256=request.output_manifest_sha256, output_marker_sha256=request.output_marker_sha256,
        execution_role='attempt', parent_lease_id=None, finalized_at=now, runtime_contract_json={},
        revoked_at=now, desired_state='deleted', observed_state='deleted', deleted_at=now,
        cleanup_state='complete', output_commit_state='committed', materialization_state='pending',
        materialization_claim_id=None, materialization_claim_expires_at=None,
        materialization_next_attempt_at=now - timedelta(seconds=1), materialization_attempts=1,
        materialization_error_code='transient_materialization_error',
        materialization_error_message='multipart object readback identity mismatch',
        canonical_trajectory_sha256=None, canonical_atif_sha256=None,
        source_cleanup_state='not_ready', materialization_recovery_requested_at=None,
    )
    trial = SimpleNamespace(id=request.trial_id, team_id=request.team_id, attempt_count=1,
                            state='materializing', config={'agent_name': 'oracle'}, result={'runtime_result': {'status': 'succeeded'}})
    artifact = SimpleNamespace(id=request.artifact_id, team_id=request.team_id, trial_id=request.trial_id,
                               control_producer_kind='service_execution', control_producer_id=request.lease_id,
                               artifact_metadata={})
    from loom.pipeline.keys import canonical_document, digest_bytes
    request = request.model_copy(update={'runtime_result_sha256': digest_bytes(canonical_document(trial.result['runtime_result'])),
                                         'trial_config_sha256': digest_bytes(canonical_document(trial.config))})
    return request, lease, trial, artifact, now


def test_only_exact_pending_successful_cleaned_source_is_eligible():
    request, lease, trial, artifact, now = qualified()
    qualify_pending(request, lease, trial, artifact, now=now)


@pytest.mark.parametrize('subject,field,value', [
    ('lease','runtime_contract_json',{'execution_role':'attempt','verifier_execution':'separate_execution'}),
    ('lease','materialization_state','running'),
    ('lease','materialization_state','committed'),
    ('lease','materialization_state','unavailable'),
    ('lease','materialization_claim_id',uuid4()),
    ('lease','materialization_claim_expires_at',datetime.now(UTC) - timedelta(seconds=1)),
    ('lease','cleanup_state','pending'), ('lease','revoked_at',None),
    ('lease','source_cleanup_state','retained'), ('lease','output_generation',2),
    ('lease','output_manifest_sha256','sha256:'+'9'*64),
    ('lease','materialization_error_message','different transport failure'),
    ('trial','state','failed'), ('trial','attempt_count',2), ('trial','team_id',uuid4()),
    ('trial','result',{'runtime_result': {'status':'succeeded','changed':True}}),
    ('trial','result',{'runtime_result': {'status':'failed'}}),
    ('artifact','control_producer_id',uuid4()),
    ('artifact','artifact_metadata',{'pending_archive_recovery': {}}),
])
def test_stale_foreign_active_or_already_recovered_input_is_rejected(subject, field, value):
    request, lease, trial, artifact, now = qualified()
    setattr({'lease': lease, 'trial': trial, 'artifact': artifact}[subject], field, value)
    with pytest.raises(RecoveryRefusedError):
        qualify_pending(request, lease, trial, artifact, now=now)


@pytest.mark.parametrize('version', [None, '', 'null'])
async def test_recovery_rejects_unversioned_writes_before_canonical_ack(version):
    from loom_control_plane.service_execution_materializer import (
        MaterializationClaim,
        ServiceExecutionMaterializer,
    )

    class Probe(ServiceExecutionMaterializer):
        committed = False
        retried = False

        async def _load_and_materialize(self, claim):
            return SimpleNamespace(events_version_id='events-1', atif_version_id='atif-1',
                                   files=[SimpleNamespace(version_id=version)], source_evidence=[])

        async def _commit(self, claim, result):
            self.committed = True
            return True

        async def _retry(self, claim, error):
            assert str(error) == 'recovery_requires_immutable_versions'
            self.retried = True

    worker = Probe(session_factory=None, source_store=None, source_bucket='source',
                   canonical_store=None, artifacts_bucket='artifacts', trajectories_bucket='trajectories')
    await worker.materialize_claim(MaterializationClaim(lease_id=uuid4(), claim_id=uuid4()), require_versions=True)
    assert worker.retried and not worker.committed


@pytest.mark.parametrize('phase', ['projection-drift', 'timeout'])
async def test_entry_rejects_projection_before_claim_and_keeps_timeout_owned(monkeypatch, phase):
    import asyncio
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from loom_control_plane import pending_archive_recovery as module

    request, *_ = qualified()
    calls = []
    session = SimpleNamespace(execute=AsyncMock())
    @asynccontextmanager
    async def transaction():
        yield session
    sessions = SimpleNamespace(begin=transaction)
    engine = SimpleNamespace(dispose=AsyncMock())
    store = SimpleNamespace(close=lambda: calls.append('close'))
    monkeypatch.setattr(module, 'create_async_engine', lambda *a, **k: engine)
    monkeypatch.setattr(module, 'async_sessionmaker', lambda *a, **k: sessions)
    monkeypatch.setattr(module, 'MinioObjectStore', lambda **k: store)
    monkeypatch.setattr(module.ServiceExecutionSourceConfig, 'from_settings', lambda _: SimpleNamespace(
        bucket='source', build_store=lambda _: store))
    async def projection(*a, **k):
        values = {key: getattr(request, key) for key in module.PROJECTION_FIELDS}
        if phase == 'projection-drift':
            values['events_sha256'] = 'sha256:' + 'f' * 64
        return values
    async def claim(*a, **k):
        calls.append('claim')
        return SimpleNamespace(lease_id=request.lease_id, claim_id=uuid4())
    async def materialize(*a, **k):
        calls.append('materialize')
        assert k['require_versions']
        await asyncio.Future()
    monkeypatch.setattr(module, 'project_oracle', projection)
    monkeypatch.setattr(module, 'claim_pending_archive', claim)
    monkeypatch.setattr(module.OracleRecoveryMaterializer, 'materialize_claim', materialize)
    monkeypatch.setattr(module, 'SOFT_TIMEOUT', 0.05)
    secret = SimpleNamespace(get_secret_value=lambda: 'private')
    settings = SimpleNamespace(db_engine_url='test', db_engine_connect_args={}, minio_endpoint='test',
        minio_access_key=secret, minio_secret_key=secret, minio_region='test', artifacts_bucket='artifacts',
        trajectories_bucket='trajectories', service_execution_source_retention_sec=86400)
    with pytest.raises(RecoveryRefusedError if phase == 'projection-drift' else TimeoutError):
        await module.run_recovery(request, settings)
    assert calls == ([] if phase == 'projection-drift' else ['claim', 'materialize']) + ['close', 'close']
    engine.dispose.assert_awaited_once()


def test_process_watchdog_precedes_recovery_and_failure_output_is_redacted(monkeypatch, tmp_path, capsys):
    import json

    from loom_control_plane import pending_archive_recovery as module

    request, *_ = qualified()
    events = []
    monkeypatch.setattr('sys.argv', ['recovery', '--request-json', request.model_dump_json(), '--platform', str(tmp_path)])
    monkeypatch.setattr(module.signal, 'signal', lambda sig, handler: events.append('handler') if sig == module.signal.SIGALRM else None)
    monkeypatch.setattr(module.signal, 'alarm', lambda seconds: events.append(seconds))
    monkeypatch.setattr(module, 'qualify_platform', lambda *a: events.append('platform'))
    monkeypatch.setattr(module, 'ControlPlaneSettings', lambda: None)
    async def fail(*a):
        events.append('recovery')
        raise RuntimeError('private credential should not be emitted')
    monkeypatch.setattr(module, 'run_recovery', fail)
    assert module.main() == 1
    assert events == ['handler', module.HARD_TIMEOUT, 'platform', 'recovery', 0]
    assert json.loads(capsys.readouterr().out) == {'status':'blocked', 'reason':'recovery_incomplete'}


def test_request_accepts_nebius_provider_cluster_identity():
    request, *_ = qualified()
    values = request.model_dump(mode='json')
    values['cluster_id'] = 'mk8scluster-test123'
    assert ArchiveRecoveryRequest.model_validate(values).cluster_id == values['cluster_id']
