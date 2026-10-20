"""Signed publication and remote-controller fixtures for connected v2 releases."""
from __future__ import annotations

import copy
import hashlib
import io
import json
import zipfile
from contextlib import contextmanager
from uuid import uuid4

import httpx
from scripts.ops.nebius_candidate import create_candidate
from scripts.ops.nebius_environment_release import release_environment_runtime
from scripts.ops.nebius_environment_release_contracts import EnvironmentReleaseV2
from scripts.ops.nebius_environment_runtime_start import runtime_resource_path
from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationReferenceV2
from tests.unit.test_nebius_candidate_catalog import GATES

from loom.execution_image_admission import ImageAdmissionKeyring
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
)


def signed_publication(original, private, *, following=False):
    reference, responses, payload, keyring, candidate = copy.deepcopy(original)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        old_profile = json.loads(archive.read('runtime-profile.json'))
    record = copy.deepcopy(candidate)
    record.update(service_schema_head='0178', runtime_binary_sha256=old_profile['runtime_binary_sha256'],
        policy_sha256=old_profile['image_admission']['admissions'][0]['statement']['policy_sha256'])
    for index, row in enumerate(record['images'].values()):
        row.update(sbom_sha256='sha256:' + 'e' * 64, vulnerability_report_sha256='sha256:' + 'f' * 64,
            highest_vulnerability_severity='high')
        if following:
            row['image_ref'] = row['image_ref'].split('@')[0] + '@sha256:' + hashlib.sha256(str(index).encode()).hexdigest()
    if following:
        record.update(candidate_sha='c' * 40, run_id=456)
        reference.update(candidate_id=str(uuid4()), source_sha=record['candidate_sha'], run_id=456, artifact_id=456, pull_request=41)
    candidate, profile = create_candidate(record, signing_key=private, signing_key_id='publisher', keyring_json=keyring)
    artifact = io.BytesIO()
    with zipfile.ZipFile(artifact, 'w') as archive:
        archive.writestr('candidate.json', json.dumps(candidate))
        archive.writestr('runtime-profile.json', json.dumps(profile))
    payload = artifact.getvalue()
    checksum = 'sha256:' + hashlib.sha256(payload).hexdigest()
    reference['artifact_sha256'] = checksum
    if following:
        repo = responses['pulls/40']['base']['repo']
        run_id, sha, head = record['run_id'], record['candidate_sha'], 'd' * 40
        responses = {f'actions/runs/{run_id}/attempts/1': {
            'id': run_id, 'run_attempt': 1, 'head_sha': sha, 'head_branch': 'dev', 'repository': repo,
            'head_repository': repo, 'status': 'completed', 'conclusion': 'success', 'event': 'push',
            'path': '.github/workflows/nebius-candidate.yml'},
            'pulls/41': {'number': 41, 'merged': True, 'state': 'closed', 'merge_commit_sha': sha,
                'base': {'ref': 'dev', 'repo': repo}, 'head': {'sha': head, 'repo': repo}},
            f'commits/{head}/check-runs': {'total_count': 4, 'check_runs': [
                {'id': n + 200, 'name': name, 'head_sha': head, 'status': 'completed', 'conclusion': 'success',
                    'app': {'id': 15368, 'slug': 'github-actions'}} for n, name in enumerate(GATES)]},
            'actions/artifacts/456': {'id': 456, 'name': f'nebius-candidate-{sha}-{run_id}-1', 'expired': False,
                'workflow_run': {'id': run_id, 'head_sha': sha, 'head_branch': 'dev', 'repository_id': repo['id'],
                    'head_repository_id': repo['id']}}}
    responses['actions/artifacts/' + str(reference['artifact_id'])].update(size_in_bytes=len(payload), digest=checksum)
    return reference, responses, payload, keyring, candidate


def controller_reconcile(remote, document):
    """Simulate the controller's new Pod/ReplicaSet after a Deployment CAS."""
    if document['kind'] != 'Deployment':
        return
    namespace, name = document['metadata']['namespace'], document['metadata']['name']
    document['metadata']['generation'] = document['metadata'].get('generation', 1) + 1
    replicas = document['spec']['replicas']
    document['status'] = {'observedGeneration': document['metadata']['generation'],
        'replicas': replicas, 'updatedReplicas': replicas, 'readyReplicas': replicas, 'availableReplicas': replicas}
    for kind, prefix, suffix in (('ReplicaSet', '/apis/apps/v1', 'replicasets'), ('Pod', '/api/v1', 'pods')):
        path = prefix + '/namespaces/' + namespace + '/' + suffix
        collection = remote.kubernetes.setdefault(path, {'apiVersion': 'apps/v1' if kind == 'ReplicaSet' else 'v1',
            'kind': kind + 'List', 'metadata': {'resourceVersion': '9'}, 'items': []})
        collection['items'] = [row for row in collection['items'] if not row['metadata']['name'].startswith(name + '-')]
    if not replicas:
        return
    labels = {**document['spec']['template']['metadata']['labels'], 'pod-template-hash': 'release'}
    replica = {'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'metadata': {'name': name + '-release',
        'namespace': namespace, 'uid': str(uuid4()), 'generation': 1, 'resourceVersion': '9', 'labels': labels,
        'ownerReferences': [{'apiVersion': 'apps/v1', 'kind': 'Deployment', 'name': name,
            'uid': document['metadata']['uid'], 'controller': True}]},
        'spec': {'replicas': 1, 'selector': {'matchLabels': {**document['spec']['selector']['matchLabels'],
            'pod-template-hash': 'release'}}, 'template': copy.deepcopy(document['spec']['template'])},
        'status': {'observedGeneration': 1, 'replicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
    replica['spec']['template']['metadata']['labels'] = labels
    pod = copy.deepcopy(replica['spec']['template'])
    pod.update(apiVersion='v1', kind='Pod')
    pod['metadata'].update(name=name + '-release-pod', namespace=namespace, uid=str(uuid4()),
        ownerReferences=[{'apiVersion': 'apps/v1', 'kind': 'ReplicaSet', 'name': replica['metadata']['name'],
            'uid': replica['metadata']['uid'], 'controller': True}])
    pod['status'] = {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}],
        'containerStatuses': [{'name': row['name'], 'ready': True, 'restartCount': 0, 'state': {'running': {}}}
            for row in pod['spec']['containers']],
        'initContainerStatuses': [{'name': row['name'], 'state': {'terminated': {'exitCode': 0}}}
            for row in pod['spec'].get('initContainers', [])]}
    remote.kubernetes['/apis/apps/v1/namespaces/' + namespace + '/replicasets']['items'].append(replica)
    remote.kubernetes['/api/v1/namespaces/' + namespace + '/pods']['items'].append(pod)


async def exercise_release(*, original_publication, private, final_inputs, runtime_id, predecessor,
        remote, workload_class, activation_connection, changes):
    following = signed_publication(original_publication, private, following=True)
    responses = original_publication[1] | following[1]
    before_sha, after_sha = original_publication[4]['candidate_sha'], following[4]['candidate_sha']
    responses[f'compare/{before_sha}...{after_sha}'] = {'status': 'ahead', 'base_commit': {'sha': before_sha},
        'merge_base_commit': {'sha': before_sha}, 'ahead_by': 1, 'behind_by': 0}
    payloads = {str(value[0]['artifact_id']): value[2] for value in (original_publication, following)}
    calls = []
    def github(request):
        calls.append(request.url.path)
        if request.url.host == 'api.github.com':
            assert request.headers['Authorization'] == 'Bearer test-github-secret'
            path = request.url.path.removeprefix('/repositories/1281629473/')
            if path.endswith('/zip'):
                return httpx.Response(302, headers={'Location': 'https://store.blob.core.windows.net/' + path.split('/')[-2]})
            return httpx.Response(200, json=responses[path])
        assert 'Authorization' not in request.headers
        return httpx.Response(200, content=payloads[request.url.path.strip('/')])
    class ReleaseWorkloads(workload_class):
        def patch_workload(self, key, before, desired):
            try:
                return super().patch_workload(key, before, desired)
            finally:
                controller_reconcile(remote, self.rows[key])
        def policy_ready(self):
            return True
        def check_inventory(self):
            pass
    @contextmanager
    def workloads(before, targets, namespaces, qualify):
        api = ReleaseWorkloads({key: remote.kubernetes[runtime_resource_path(doc) + '/' + doc['metadata']['name']]
            for key, doc in before.items()})
        api.ready, api.failure, api.qualify = True, 'after', qualify
        yield api
    policy = final_inputs['policy']
    originals = {path: copy.deepcopy(doc) for path, doc in remote.kubernetes.items()
        if doc.get('kind') in {'Secret', 'Namespace', 'ResourceQuota'}}
    selection = EnvironmentReleaseV2(runtime_operation_id=runtime_id,
        predecessor=PoolAdministrationReferenceV2.model_validate(predecessor), publication=ProtectedPublication.model_validate(following[0]))
    release_id, rollback_id = uuid4(), uuid4()
    async with httpx.AsyncClient(transport=httpx.MockTransport(github)) as github_http:
        catalog = GitHubCandidateCatalog(github_http, token='test-github-secret',
            publications=[policy.application_publication, selection.publication],
            keyring=ImageAdmissionKeyring.from_json(original_publication[3]), registry_prefix=original_publication[4]['registry_prefix'])
        common = {key: value for key, value in final_inputs.items() if key not in {'operation_id', 'catalog', 'connect'}}
        common.update(catalog=catalog, connect=activation_connection, workload_connection=workloads)
        before = len(changes)
        pending = await release_environment_runtime(operation_id=release_id, replacement=selection, **common)
        assert pending['status'] == 'pending' and pending['phase'] == 'begin_release'
        assert len(changes) == before, 'lost administration reply must defer runtime mutation'
        forward = await release_environment_runtime(operation_id=release_id, replacement=selection, **common)
        assert forward['status'] == 'complete' and forward['admission_open'] and forward['binding_revision'] == 2
        assert len(changes) == before + 17  # Five Deployment phases plus ConfigMap and policy.
        effects = len(changes)
        assert (await release_environment_runtime(operation_id=release_id, replacement=selection, **common))['status'] == 'complete'
        assert len(changes) == effects, 'recovery must not repeat any release PATCH'
        rollback = EnvironmentReleaseV2(runtime_operation_id=runtime_id, previous_release_id=release_id,
            rollback_to=runtime_id, predecessor=PoolAdministrationReferenceV2.model_validate(forward['reference']),
            publication=policy.application_publication)
        reverted = await release_environment_runtime(operation_id=rollback_id, replacement=rollback, **common)
        assert reverted['status'] == 'complete' and reverted['admission_open'] and reverted['binding_revision'] == 3
        assert len(changes) == effects + 16
        assert all(remote.kubernetes[path] == original for path, original in originals.items())
        assert '/repositories/1281629473/compare/' + before_sha + '...' + after_sha in calls
        from scripts.ops.nebius_environment_lifecycle import (
            resume_released_environment,
            transition_participant,
        )
        from scripts.ops.nebius_environment_release import resolve_completed_release_policy
        current_policy, _, _, _ = resolve_completed_release_policy(policy=policy, release_id=rollback_id)
        suspended = await transition_participant(policy=current_policy, operation_id=uuid4(), action='suspend',
            expected_head=PoolAdministrationReferenceV2.model_validate(reverted['reference']).operation_id,
            connect=activation_connection)
        resume_inputs = {key: value for key, value in common.items() if key not in {'workload_connection', 'policy'}}
        resumed = await resume_released_environment(policy=policy, release_id=rollback_id, transition_id=uuid4(),
            predecessor=PoolAdministrationReferenceV2.model_validate(suspended['reference']), boundary_connection=workloads, **resume_inputs)
        assert resumed['status'] == 'complete' and resumed['admission_open']
        assert len(changes) == effects + 16, 'resume after rollback must preserve every running workload'
