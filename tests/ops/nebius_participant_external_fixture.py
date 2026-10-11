"""HTTP/SDK boundary fixtures for the connected ordinary-release regression."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import io
import json
import zipfile
from pathlib import Path
from uuid import uuid4

import httpx
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_development_pool_retained import runtime_cloud_transport

from loom_service.environment_management.candidates import ProtectedPublication


def publication_fixture(candidate, profile):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, 'w') as target:
        target.writestr('candidate.json', json.dumps(candidate))
        target.writestr('runtime-profile.json', json.dumps(profile.model_dump(mode='json')))
    payload = archive.getvalue()
    reference = ProtectedPublication(candidate_id=uuid4(), source_sha=candidate['candidate_sha'],
        run_id=candidate['run_id'], run_attempt=1, artifact_id=923,
        artifact_sha256='sha256:' + hashlib.sha256(payload).hexdigest(), pull_request=2425)
    return reference, payload


def external_connection(*, observer, bindings, shared_state, resources, publisher_cloud, patch, payload):
    """Keep catalog approval, IAM inspection and provider reads real."""
    from httpx._client import AsyncClient

    retained = observer.retained
    completed = bindings.plan.development.runtime
    request = completed.request
    manager = request.database.manager.retained.request.retained
    for key in completed.plan.fixed['material']:
        resources.setdefault(key, copy.deepcopy(completed.resources[key]))
    record = json.loads(manager.files[Path(manager.operation['state_dir']) / 'supplied/stage.json'])
    key = 'Secret:' + manager.binding.namespace + ':loom-management-publications'
    item = record['resources'][key]
    credential = copy.deepcopy(item['observed'])
    credential['metadata']['uid'] = item['uid']
    resources[key] = credential
    operator, sessions, reads, closed, cloud_mode = runtime_cloud_transport(
        request, publisher_cloud, patch, shared_state.parent)
    mode, calls = {}, []
    reference = retained.inputs.publication
    repo = {'full_name': 'ca-mrc/loom', 'id': 1281629473}
    responses = {
        f'actions/runs/{reference.run_id}/attempts/1': {
            'id': reference.run_id, 'run_attempt': 1, 'head_sha': reference.source_sha, 'head_branch': 'dev',
            'repository': repo, 'head_repository': repo, 'status': 'completed', 'conclusion': 'success',
            'event': 'push', 'path': '.github/workflows/nebius-candidate.yml'},
        'pulls/2425': {'number': 2425, 'merged': True, 'state': 'closed', 'merge_commit_sha': reference.source_sha,
            'base': {'ref': 'dev', 'repo': repo}, 'head': {'sha': 'b' * 40, 'repo': repo}},
        'commits/' + 'b' * 40 + '/check-runs': {'total_count': 4, 'check_runs': [
            {'id': 100 + index, 'name': name, 'head_sha': 'b' * 40, 'status': 'completed', 'conclusion': 'success',
                'app': {'id': 15368, 'slug': 'github-actions'}} for index, name in enumerate(
                    ('repository-checks', 'images-gate', 'cluster-smoke-gate', 'staging-smoke-gate'))]},
        'actions/artifacts/923': {'id': 923,
            'name': f'nebius-candidate-{reference.source_sha}-{reference.run_id}-1', 'expired': False,
            'size_in_bytes': len(payload), 'digest': reference.artifact_sha256,
            'workflow_run': {'id': reference.run_id, 'head_sha': reference.source_sha, 'head_branch': 'dev',
                'repository_id': repo['id'], 'head_repository_id': repo['id']}},
    }

    def respond(message):
        assert message.method == 'GET'
        calls.append(message.url.path)
        if message.url.host == 'api.github.com':
            assert message.headers['Authorization'] == 'Bearer scoped-publication-test-token'
            path = message.url.path.removeprefix('/repositories/1281629473/')
            if path == 'actions/artifacts/923/zip':
                return httpx.Response(302, headers={'Location': 'https://fixture.blob.core.windows.net/artifact'})
            result = copy.deepcopy(responses[path])
            if mode.get('damage') == 'checks' and 'check_runs' in result:
                result['check_runs'][0]['conclusion'] = 'failure'
            return httpx.Response(200, json=result)
        assert message.url.host == 'fixture.blob.core.windows.net' and 'Authorization' not in message.headers
        if hook := mode.pop('hook', None):
            hook()
        return httpx.Response(200, content=payload + b'changed' if mode.get('damage') == 'artifact' else payload)

    async def observe():
        async with AsyncClient(transport=httpx.MockTransport(respond), trust_env=False) as http:
            return await observer.observe_external(bindings=bindings, shared_state=shared_state,
                operator_credentials=operator, http=http)

    def qualify():
        result = asyncio.run(observe())
        assert sessions == closed and not sessions[-1].path.exists()
        assert result.node_group.id == retained.release.refresh.before.node_group_id
        return result

    def fault(kind):
        mode['damage'] = kind
        cloud_mode['damage'] = kind

    collector_key, = (key for key, row in completed.resources.items() if row['kind'] == 'Secret'
        and row['metadata']['namespace'] == request.database.foundation.inputs.config['execution_namespace']
        and 'credentials.json' in row.get('data', {}))
    assert _key(resources[collector_key]) == collector_key
    return qualify, fault, mode, calls, reads, collector_key, key, operator
