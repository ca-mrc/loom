"""Native previews stream before canonical publication, under real PostgreSQL fences."""
from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from botocore.exceptions import ClientError
from fastapi import FastAPI
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    ServiceExecutionLiveEvent,
    ServiceExecutionTarget,
    Token,
    Trial,
    TrialEvent,
)
from loom.service_execution_live import read_live_preview
from loom_control_plane.service_execution_output import VerifiedExecutionPod
from loom_llm_gateway.routes import service_execution
from loom_service.routes import trajectory
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


class _MissingObjectStore:
    def get_object(self, **kwargs):
        raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')


@pytest.fixture
async def live_setup(postgres_url: str):
    now = datetime.now(UTC)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = FastAPI()
    app.state.session_factory = sessions
    app.state.minio_client = _MissingObjectStore()
    app.state.settings = SimpleNamespace(db_url=postgres_url, trajectories_bucket='test-trajectories')
    app.include_router(service_execution.router)
    app.include_router(trajectory.router, prefix='/api/v1')
    raw = 'live-preview-test-' + uuid4().hex
    token_hash = hashlib.sha256(raw.encode()).digest()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            target_row = await session.get(ServiceExecutionTarget, target.target_id)
            target_row.spec_json = {**target_row.spec_json, 'cluster_scope_id': 'live-cluster',
                                   'pod_identity_audience': 'loom-execution'}
            lease.pod_uid, lease.pod_ip, lease.observed_state = 'live-pod', '10.42.0.4', 'running'
            trial = await session.get(Trial, trial_id)
            trial.state, trial.started_at = 'running', now
            session.add(Token(token_hash=token_hash, type='team', scopes=['read:own'],
                              team_id=trial.team_id, issued_at=now))
            await session.commit()
            lease_id = lease.id
        pod = {'uid': 'live-pod'}

        async def review(**kwargs):
            assert kwargs['token'] == 'bound-disposable-pod-token'
            return VerifiedExecutionPod(cluster_scope_id='live-cluster', namespace=target.namespace_name,
                service_account='loom-execution-attempt', audience='loom-execution', pod_uid=pod['uid'])
        app.state.execution_pod_reviewer = SimpleNamespace(review=review)
        payload = {'schema_version': 'loom.service-execution-trajectory-events.v1',
                   'lease_id': str(lease_id), 'generation': 1, 'execution_role': 'attempt',
                   'events': [{'kind': 'step_start', 'trial_id': str(trial_id), 'step_id': 'agent',
                               'seq': 0, 'emitted_at': now.isoformat(), 'instruction_excerpt': 'bounded live test'}]}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://loom.example') as client:
            yield SimpleNamespace(app=app, sessions=sessions, client=client, payload=payload,
                trial_id=trial_id, lease_id=lease_id, source=f'{lease_id}:1', pod=pod,
                write_headers={'Authorization': 'Bearer bound-disposable-pod-token'},
                read_headers={'Authorization': 'Bearer '+raw})
    finally:
        async with sessions() as session:
            row = await session.get(Token, token_hash)
            if row is not None:
                await session.delete(row)
            await session.commit()
        await engine.dispose()


async def _post(setup, payload=None, *, headers=None):
    return await setup.client.post('/internal/service-execution/trajectory-events',
        json=payload or setup.payload, headers=setup.write_headers if headers is None else headers)


async def test_native_preview_ingests_while_running_with_pod_identity(live_setup) -> None:
    s = live_setup
    response = await _post(s)
    assert response.status_code == 200, response.text
    assert response.json() == {'inserted': 1, 'next_after_seq': 0}
    duplicate = await _post(s)
    assert duplicate.json() == {'inserted': 0, 'next_after_seq': 0}
    second = deepcopy(s.payload)
    second['events'].append({**second['events'][0], 'seq': 1, 'instruction_excerpt': 'second event'})
    assert (await _post(s, second)).json() == {'inserted': 1, 'next_after_seq': 1}
    async with s.sessions() as session:
        trial = await session.get(Trial, s.trial_id)
        assert trial.state == 'running'
        assert await session.scalar(select(func.count()).select_from(TrialEvent).where(TrialEvent.trial_id == s.trial_id)) == 0
        rows = list((await session.scalars(select(ServiceExecutionLiveEvent).where(ServiceExecutionLiveEvent.trial_id == s.trial_id))).all())
        assert len(rows) == 2 and all(row.lifecycle_authority_id for row in rows)


@pytest.mark.parametrize('failure', ['unauthenticated', 'foreign_pod', 'stale_generation', 'verifier_role',
                                    'foreign_trial', 'sequence_gap', 'invalid_event', 'oversized', 'changed_duplicate'])
async def test_native_preview_rejects_identity_and_invalid_batches(live_setup, failure: str) -> None:
    s = live_setup
    assert (await _post(s)).status_code == 200
    payload = deepcopy(s.payload)
    headers = s.write_headers
    expected = 409
    if failure == 'unauthenticated':
        headers, expected = {}, 403
    elif failure == 'foreign_pod':
        s.pod['uid'], expected = 'foreign-pod', 403
    elif failure == 'stale_generation':
        payload['generation'] = 2
    elif failure == 'verifier_role':
        payload['execution_role'], expected = 'verifier', 403
    elif failure == 'foreign_trial':
        payload['events'][0]['trial_id'] = str(uuid4())
    elif failure == 'sequence_gap':
        payload['events'][0]['seq'] = 2
    elif failure == 'invalid_event':
        del payload['events'][0]['instruction_excerpt']
        expected = 422
    elif failure == 'oversized':
        payload['events'][0]['instruction_excerpt'] = 'x' * (256 * 1024)
        expected = 413
    else:
        payload['events'][0]['instruction_excerpt'] = 'conflicting retry'
    response = await _post(s, payload, headers=headers)
    assert response.status_code == expected, response.text
    async with s.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ServiceExecutionLiveEvent).where(ServiceExecutionLiveEvent.trial_id == s.trial_id)) == 1


async def test_native_preview_polling_has_independent_cursor_and_canonical_takeover(live_setup) -> None:
    s = live_setup
    assert (await _post(s)).status_code == 200
    route = f'/api/v1/trials/{s.trial_id}/events'
    original = await s.client.get(route, headers=s.read_headers)
    assert original.json() == {'events': [], 'next_after_seq': None}
    response = await s.client.get(route, params={'include_preview': True}, headers=s.read_headers)
    preview = response.json()['preview']
    assert preview['source'] == s.source and preview['next_after_seq'] == 0
    assert preview['events'][0]['seq'] == 0
    tail = await s.client.get(route, params={'include_preview': True, 'preview_source': s.source,
        'preview_after_seq': 0}, headers=s.read_headers)
    assert tail.json()['preview'] == {'source': s.source, 'events': [], 'next_after_seq': None}
    reset = await s.client.get(route, params={'include_preview': True, 'preview_source': 'old-attempt',
        'preview_after_seq': 999}, headers=s.read_headers)
    assert reset.json()['preview']['events'] == preview['events']
    async with s.sessions() as session:
        trial = await session.get(Trial, s.trial_id)
        trial.attempt_count += 1
        await session.commit()
        assert (await read_live_preview(session, trial=trial))['source'] is None
        trial.attempt_count -= 1
        session.add(TrialEvent(trial_id=trial.id, seq=0, kind='trial_start', source='service-execution-materializer',
                               payload={'kind': 'trial_start', 'seq': 0, 'marker': 'canonical'}))
        await session.commit()
    final = await s.client.get(route, params={'include_preview': True, 'after_seq': 0,
        'preview_source': s.source}, headers=s.read_headers)
    assert final.json() == {'events': [], 'next_after_seq': None,
                            'preview': None}


def _frames(content: str):
    result = []
    for frame in content.strip().split('\n\n'):
        fields = dict(line.split(': ', 1) for line in frame.splitlines() if ': ' in line and not line.startswith(':'))
        if 'data' in fields:
            result.append((fields.get('event', 'message'), json.loads(fields['data'])))
    return result


async def test_native_preview_sse_pushes_before_terminal_and_hands_off_canonical(live_setup, monkeypatch) -> None:
    s = live_setup
    ready, preview_drained = asyncio.Event(), asyncio.Event()
    read = trajectory.read_live_preview

    async def observe_preview(*args, **kwargs):
        result = await read(*args, **kwargs)
        if kwargs['trial'].id == s.trial_id:
            ready.set()
            if kwargs.get('after_seq') == 0 and not result['events']:
                preview_drained.set()
        return result
    monkeypatch.setattr(trajectory, 'read_live_preview', observe_preview)
    monkeypatch.setattr(trajectory, '_DEFAULT_SSE_POLL_INTERVAL_SEC', 30.0)

    async def produce():
        await ready.wait()
        response = await _post(s)
        assert response.status_code == 200, response.text
        await preview_drained.wait()
        async with s.sessions() as session:
            trial = await session.get(Trial, s.trial_id)
            assert trial.state == 'running'
            session.add(TrialEvent(trial_id=trial.id, seq=0, kind='trial_end', source='service-execution-materializer',
                payload={'seq': 0, 'kind': 'trial_end', 'trial_id': str(trial.id), 'final_state': 'succeeded'}))
            trial.state, trial.finished_at, trial.result = 'succeeded', datetime.now(UTC), {}
            await session.commit()
    producer = asyncio.create_task(produce())
    try:
        response = await asyncio.wait_for(s.client.get(f'/api/v1/trials/{s.trial_id}/stream', headers=s.read_headers), 8)
        await producer
    finally:
        producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
    assert response.status_code == 200
    frames = _frames(response.text)
    assert [kind for kind, _ in frames] == ['preview_reset', 'preview', 'preview_reset', 'message', 'complete']
    assert frames[0][1] == {'source': s.source}
    assert frames[1][1]['events'][0]['seq'] == 0
    assert frames[2][1] == {'source': None}
    assert frames[3][1]['seq'] == 0
    assert frames[-1][1]['last_seq'] == 0


async def test_terminal_native_failure_retains_partial_preview(live_setup) -> None:
    s = live_setup
    assert (await _post(s)).status_code == 200
    async with s.sessions() as session:
        trial = await session.get(Trial, s.trial_id)
        trial.state, trial.finished_at = 'failed', datetime.now(UTC)
        await session.commit()
    response = await s.client.get(f'/api/v1/trials/{s.trial_id}/stream', headers=s.read_headers)
    frames = _frames(response.text)
    assert [kind for kind, _ in frames] == ['preview_reset', 'preview', 'complete']
    assert frames[-1][1] == {'final_state': 'failed', 'last_seq': -1, 'canonical_available': False}
    resumed = await s.client.get(f'/api/v1/trials/{s.trial_id}/stream',
        params={'preview_source': s.source, 'preview_after_seq': 0}, headers=s.read_headers)
    assert [kind for kind, _ in _frames(resumed.text)] == ['complete']


@pytest.mark.parametrize('expire_on_commit', [False, True])
async def test_browser_session_stream_releases_auth_transaction_for_parallel_reads(
    live_setup, monkeypatch, expire_on_commit: bool,
) -> None:
    """An open SSE must not retain the browser-session last_seen UPDATE lock."""
    from sqlalchemy import delete

    from loom.db.schema import TeamMembership, User, UserSession
    from loom_service.session_auth import create_session_for_user

    s = live_setup
    user_id = uuid4()
    started = asyncio.Event()
    read_events = trajectory._read_events_with_minio_fallback
    stream_task = None
    original_sessions = s.app.state.session_factory
    s.app.state.session_factory = async_sessionmaker(
        s.sessions.kw['bind'], expire_on_commit=expire_on_commit,
    )
    s.app.state.settings.session_cookie_name = 'loom_session'
    s.app.state.settings.session_audience = None
    try:
        async with s.sessions() as session:
            trial = await session.get(Trial, s.trial_id)
            user = User(id=user_id, username=f'live-cookie-{user_id}',
                username_normalized=f'live-cookie-{user_id}', status='active')
            session.add(user)
            await session.flush()
            session.add(TeamMembership(user_id=user_id, team_id=trial.team_id, role='viewer'))
            await session.flush()
            created = await create_session_for_user(session, user=user, session_ttl_seconds=600,
                current_team_id=trial.team_id)
            await session.commit()
        s.client.cookies.set('loom_session', created.raw_session)

        async def observe_real_read(*args, **kwargs):
            events = await read_events(*args, **kwargs)
            if kwargs['trial'].id == s.trial_id:
                started.set()
            return events

        monkeypatch.setattr(trajectory, '_read_events_with_minio_fallback', observe_real_read)
        stream_task = asyncio.create_task(s.client.get(f'/api/v1/trials/{s.trial_id}/stream'))
        await asyncio.wait_for(started.wait(), 5)
        assert not stream_task.done()
        response = await asyncio.wait_for(
            s.client.get(f'/api/v1/trials/{s.trial_id}/events?include_preview=true'), 2,
        )
        assert response.status_code == 200, response.text
        assert not stream_task.done(), 'parallel request must finish while SSE remains open'
        async with s.sessions() as session:
            trial = await session.get(Trial, s.trial_id)
            assert trial.state == 'running'
    finally:
        if stream_task is not None:
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)
        s.client.cookies.clear()
        s.app.state.session_factory = original_sessions
        async with s.sessions() as session:
            await session.execute(delete(UserSession).where(UserSession.user_id == user_id))
            await session.execute(delete(TeamMembership).where(TeamMembership.user_id == user_id))
            await session.execute(delete(User).where(User.id == user_id))
            await session.commit()
