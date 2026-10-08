"""The isolated archival worker cannot become a replacement platform deployment."""
import copy
import json

import pytest
from scripts.ops.nebius_archive_recovery import recovery_job, submit_once
from tests.unit.test_pending_archive_recovery import qualified


def inputs():
    request, *_ = qualified()
    cp = {'spec': {'template': {'spec': {
        'containers': [{'name': 'loom-control-plane', 'image': request.installed_image_ref,
            'env': [
                {'name': name, 'valueFrom': {'secretKeyRef': {'name': 'loom-platform-db', 'key': 'test'}}}
                for name in ('LOOM_CP_DB_URL', 'LOOM_CP_MINIO_ACCESS_KEY', 'LOOM_CP_MINIO_SECRET_KEY',
                             'LOOM_CP_SERVICE_EXECUTION_SOURCE_ACCESS_KEY', 'LOOM_CP_SERVICE_EXECUTION_SOURCE_SECRET_KEY')
            ] + [{'name': name, 'value': value} for name, value in {
                'LOOM_ENV': 'development', 'LOOM_NAMESPACE': request.namespace,
                'LOOM_CP_MINIO_ENDPOINT':'https://storage.test', 'LOOM_CP_MINIO_REGION':'region',
                'LOOM_CP_ARTIFACTS_BUCKET':'artifacts', 'LOOM_CP_TRAJECTORIES_BUCKET':'trajectories',
                'LOOM_CP_SERVICE_EXECUTION_SOURCE_ENDPOINT':'https://storage.test',
                'LOOM_CP_SERVICE_EXECUTION_SOURCE_REGION':'region', 'LOOM_CP_SERVICE_EXECUTION_SOURCE_BUCKET':'source',
                'LOOM_CP_SERVICE_EXECUTION_SOURCE_RETENTION_SEC':'86400',
                'LOOM_CP_STEP_JWT_SIGNING_KEY':'MUST-NOT-COPY',
                'LOOM_SECRET_STORE_MASTER_KEY':'MUST-NOT-COPY',
            }.items()]}],
        'volumes': [{'name': 'db-ca', 'secret': {'secretName': 'loom-platform-db', 'defaultMode': 0o440, 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}}],
        'imagePullSecrets':[{'name':'loom-registry-pull'}],
        'nodeSelector': {'loom.nebius/node-role':'system'},
        'tolerations': [],
    }}}}
    cp['spec']['template']['spec']['containers'][0]['volumeMounts'] = [
        {'name': 'db-ca', 'mountPath': '/var/run/loom-db', 'readOnly': True}]
    return request, cp


def test_recovery_preserves_installed_lifecycle_scope(monkeypatch):
    from loom.data_lifecycle_registry import RuntimeLifecycleScope

    request, cp = inputs()
    installed = cp['spec']['template']['spec']['containers'][0]['env']
    job = recovery_job(request, cp)
    supplied = {item['name']: item for item in job['spec']['template']['spec']['containers'][0]['env']}
    for item in installed:
        if item['name'] in {'LOOM_ENV', 'LOOM_NAMESPACE'}:
            assert supplied.get(item['name']) == item
            monkeypatch.setenv(item['name'], supplied[item['name']]['value'])
    scope = RuntimeLifecycleScope.from_environ()
    assert scope.namespace == request.namespace
    assert scope.environment == 'development'


@pytest.mark.parametrize('change', ['missing-environment', 'missing-namespace', 'foreign-namespace', 'indirect-scope'])
def test_recovery_rejects_unqualified_lifecycle_scope_before_launch(change):
    request, cp = inputs()
    env = cp['spec']['template']['spec']['containers'][0]['env']
    if change.startswith('missing'):
        missing = 'LOOM_ENV' if change == 'missing-environment' else 'LOOM_NAMESPACE'
        env[:] = [item for item in env if item['name'] != missing]
    elif change == 'foreign-namespace':
        next(item for item in env if item['name'] == 'LOOM_NAMESPACE')['value'] = 'foreign'
    else:
        item = next(item for item in env if item['name'] == 'LOOM_NAMESPACE')
        item.pop('value')
        item['valueFrom'] = {'fieldRef': {'fieldPath': 'metadata.namespace'}}
    with pytest.raises(ValueError, match='lifecycle_scope'):
        recovery_job(request, cp)


def test_recovery_is_one_digest_pinned_nonservice_worker_with_no_authority_token():
    request, cp = inputs()
    job = recovery_job(request, cp)
    pod = job['spec']['template']
    spec = pod['spec']
    container, = spec['containers']
    assert container['image'] == request.image_ref
    assert container['command'][:3] == ['python','-m','loom_control_plane.pending_archive_recovery']
    assert 'loom-control-plane' not in pod['metadata']['labels'].values()
    assert spec['automountServiceAccountToken'] is False and spec['restartPolicy'] == 'Never'
    assert job['spec']['backoffLimit'] == 0 and job['spec']['parallelism'] == 1
    assert 0 < job['spec']['activeDeadlineSeconds'] < 3600
    assert container['securityContext']['readOnlyRootFilesystem'] is True
    assert container['securityContext']['capabilities']['drop'] == ['ALL']
    assert 'MUST-NOT-COPY' not in json.dumps(job)
    assert len(spec['volumes']) == 3  # installed platform binding + bounded scratch
    assert cp == inputs()[1]


class API:
    def __init__(self):
        self.job = {}
        self.created = []
        self.lose_response = False

    def get(self, kind, name, namespace):
        return copy.deepcopy(self.job)

    def run(self, *args, **kwargs):
        assert args[:2] == ('create', '-f')
        from pathlib import Path

        import yaml
        self.job = yaml.safe_load(Path(args[2]).read_text())
        self.job['metadata']['uid'] = 'test-job-uid'
        self.created.append(copy.deepcopy(self.job))
        if self.lose_response:
            raise RuntimeError('lost response')
        return ''


def test_uncertain_creation_is_read_back_without_second_launch(tmp_path):
    request, cp = inputs()
    job = recovery_job(request, cp)
    api = API()
    api.lose_response = True
    with pytest.raises(RuntimeError, match='lost response'):
        submit_once(api, job, tmp_path / 'evidence')
    api.lose_response = False
    result = submit_once(api, job, tmp_path / 'evidence')
    assert result['job_uid'] == 'test-job-uid' and len(api.created) == 1
    api.job = {}
    with pytest.raises(ValueError, match='missing'):
        submit_once(api, job, tmp_path / 'evidence')
    assert len(api.created) == 1


def test_foreign_existing_job_is_never_patched(tmp_path):
    request, cp = inputs()
    api = API()
    api.job = recovery_job(request, cp)
    with pytest.raises(ValueError, match='existing'):
        submit_once(api, api.job, tmp_path / 'evidence')
    assert not api.created


def test_remote_create_keeps_context_and_transmits_exact_manifest(monkeypatch, tmp_path):
    import shlex
    from types import SimpleNamespace

    from scripts.ops.deploy_nebius_platform import Kubectl

    monkeypatch.setenv('LOOM_DEPLOY_SSH_TARGET', 'deploy@gateway.example')
    monkeypatch.setenv('LOOM_DEPLOY_SSH_KEY_FILE', '/private/key')
    monkeypatch.setenv('LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE', '/private/known-hosts')
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout='', stderr='')
    monkeypatch.setattr('scripts.ops.deploy_nebius_platform.subprocess.run', run)
    manifest = tmp_path / 'job.json'
    manifest.write_text('{"kind":"Job"}\n')
    Kubectl(tmp_path / 'remote-config', context='loom-rollout').run('create','-f',str(manifest))
    command, options = calls[0]
    remote = shlex.split(command[-1])
    assert remote[remote.index('--context')+1] == 'loom-rollout'
    assert remote[-3:] == ['create','-f','-']
    assert options['input'] == manifest.read_text()


def test_recovery_uses_real_rendered_ca_projection():
    from pathlib import Path

    from tests.unit.test_nebius_platform_render import platform_inputs

    from loom.nebius_platform_render import build_platform

    config, candidate, profile = platform_inputs.__wrapped__()
    files = build_platform(config, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2])
    cp = next(doc for batch in files.values() for doc in batch
              if doc['kind'] == 'Deployment' and doc['metadata']['name'] == 'loom-control-plane')
    original = cp['spec']['template']['spec']
    request, *_ = qualified()
    request = request.model_copy(update={'installed_image_ref': original['containers'][0]['image'],
                                         'namespace': config['namespace']})
    job = recovery_job(request, cp)
    pod = job['spec']['template']['spec']
    ca = next(v for v in pod['volumes'] if v['name'] == 'db-ca')
    assert ca == next(v for v in original['volumes'] if v['name'] == 'db-ca')
    assert ca['secret']['defaultMode'] == 0o440
    assert ca['secret']['items'] == [{'key':'ca.crt', 'path':'ca.crt'}]
    assert pod['securityContext']['fsGroup'] == pod['securityContext']['runAsGroup']
    assert {'name':'db-ca', 'mountPath':'/var/run/loom-db', 'readOnly':True} in pod['containers'][0]['volumeMounts']
    assert {v['name'] for v in pod['volumes']} == {'platform','tmp','db-ca'}


@pytest.mark.parametrize('change', ['missing', 'extra-key', 'wrong-secret', 'writable'])
def test_recovery_rejects_unqualified_ca_mount(change):
    request, cp = inputs()
    pod = cp['spec']['template']['spec']
    if change == 'missing':
        pod['volumes'] = []
    elif change == 'extra-key':
        pod['volumes'][0]['secret']['items'].append({'key':'credentials', 'path':'credentials'})
    elif change == 'wrong-secret':
        pod['volumes'][0]['secret']['secretName'] = 'admin'
    else:
        pod['containers'][0]['volumeMounts'][0]['readOnly'] = False
    with pytest.raises(ValueError, match='database_ca'):
        recovery_job(request, cp)


@pytest.mark.parametrize('denial', ['publication', 'candidate', 'schema', 'image'])
def test_publication_denials_precede_any_launch(monkeypatch, denial):
    from types import SimpleNamespace

    from scripts.ops import nebius_archive_recovery as module
    request, _ = inputs()
    monkeypatch.setattr(module, 'select_publication', lambda _: {
        'status':'blocked' if denial == 'publication' else 'ready', 'sha':request.candidate_sha, 'artifact':'candidate'})
    monkeypatch.setattr(module, 'source_archive_digest', lambda _: 'sha256:'+'a'*64)
    monkeypatch.setattr(module, 'candidate_follows', lambda *a: denial != 'candidate')
    monkeypatch.setattr(module, 'candidate_schema_head', lambda _: '0000' if denial == 'schema' else request.schema_head)
    monkeypatch.setattr(module.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=0))
    monkeypatch.setattr(module, 'validate_identity', lambda *a, **k: None)
    monkeypatch.setattr(module, 'read_json', lambda _: {'candidate_sha':request.candidate_sha,
        'run_id':'123', 'images':{'control_plane':{'image_ref':'foreign'}}})
    with pytest.raises(ValueError, match=r'publication_binding|candidate_compatibility|published_image'):
        module.qualify_publication(request, '123')


@pytest.mark.parametrize('denial', ['config', 'not-ready', 'pod-image', 'actual-digest'])
def test_platform_denials_precede_any_launch(monkeypatch, denial):
    from scripts.ops import nebius_archive_recovery as module
    request, cp = inputs()
    cp.update(metadata={'generation':1}, status={'observedGeneration':1, 'readyReplicas':1})
    cp['spec']['replicas'] = 1
    config = {'cluster_id':request.cluster_id, 'namespace':request.namespace}
    pod = {'metadata':{}, 'spec':{'containers':[{'image':request.installed_image_ref}]},
           'status':{'containerStatuses':[{'ready':True,'imageID':request.installed_image_ref}]}}
    if denial == 'config':
        config['cluster_id'] = 'foreign'
    if denial == 'not-ready':
        cp['status']['readyReplicas'] = 0
    if denial == 'pod-image':
        pod['spec']['containers'][0]['image'] = 'foreign'
    if denial == 'actual-digest':
        pod['status']['containerStatuses'][0]['imageID'] = 'foreign'
    class PlatformAPI:
        def get(self, kind, *a):
            if kind == 'configmap':
                return {'data':{'environment.json':json.dumps(config), 'profile.json':json.dumps({'candidate_sha':request.installed_candidate})}}
            return cp
        def run(self, *a):
            assert a[0] == 'get'
            return json.dumps({'items':[pod]})
    monkeypatch.setattr(module, 'verify_cluster_identity', lambda *a: None)
    with pytest.raises(ValueError, match=r'platform_binding|control_plane_not_ready|installed_worker_image'):
        module.qualify_platform(PlatformAPI(), request)
