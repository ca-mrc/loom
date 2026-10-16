"""New preparation captures real objects; recovery never uses this capture path."""
from __future__ import annotations

import copy
import importlib
import ssl
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_operation import retain_participant_rollout
from scripts.ops.nebius_pool_participant_resource import _resource_path
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)

from loom.nebius_platform_render import build_platform
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1


def module():
    name = 'scripts.ops.nebius_pool_participant_prepare'
    assert importlib.util.find_spec(name) is not None, 'new release cannot capture a complete source-qualified baseline'
    return importlib.import_module(name)


@pytest.fixture
def preparation(operation_inputs, monkeypatch):
    from scripts.ops import nebius_shared_pool_forward_census as census

    inputs = operation_inputs
    predecessor = inputs['predecessor']
    current = {_key(row): copy.deepcopy(row) for row in inputs['current'] if row['kind'] != 'Job'}
    config = inputs['config']
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(
        current[f"ConfigMap:{config['namespace']}:loom-platform-config"]['data']['profile.json'])
    old_render = build_platform(config, predecessor.migration.registration.candidate,
        profile.model_dump(mode='json'), predecessor.migration.registration.spec.profiles.image_admission_keyring,
        repo_root=inputs['repo_root'])
    # This test begins at the completed semantic predecessor boundary. Initial
    # receipt selection is supplied explicitly, not manufactured by production.
    narrow = {'Namespace', 'Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
        'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}
    originals = tuple(copy.deepcopy(row) for rows in old_render.values() for row in rows if row['kind'] in narrow)
    for row in originals:
        row['metadata'].update(uid=current[_key(row)]['metadata']['uid'], resourceVersion='16')
    resources = {**current, _key(inputs['manager']): copy.deepcopy(inputs['manager']),
        _key(inputs['catalog']): copy.deepcopy(inputs['catalog'])}
    binding = predecessor.migration.registration.binding
    runtime = SimpleNamespace(resources={_key(inputs['manager']): copy.deepcopy(inputs['manager']),
        _key(inputs['catalog']): copy.deepcopy(inputs['catalog'])},
        request=SimpleNamespace(database=SimpleNamespace(manager=SimpleNamespace(deployment=SimpleNamespace(
            public_host='manage.example.test')))))
    bindings = SimpleNamespace(plan=SimpleNamespace(development=SimpleNamespace(runtime=runtime),
        effective_runtime_resources=copy.deepcopy(runtime.resources),
        staging=SimpleNamespace(source=None,
            attachment=SimpleNamespace(config=config, runtime_profile=profile, resources=originals))))
    selected = {key: (copy.deepcopy(row),) for key, row in current.items() if row['kind'] in {
        'Deployment', 'StatefulSet', 'CronJob', 'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}}
    choices = ({'test_boundary': 'completed handoff receipt choices'}, selected, {}, {})

    def forward(bound, state):
        assert bound is bindings and state == predecessor.state_dir
        return copy.deepcopy(choices)

    monkeypatch.setattr(census, '_forward_options', forward)
    reads = []
    paths = {_resource_path(row): key for key, row in resources.items()}
    control = SimpleNamespace(resources=resources, reads=reads, after_read=None, choices=choices)

    def respond(request):
        assert request.method == 'GET' and not request.url.query, 'preparation must have no effects'
        path = request.url.path
        reads.append(path)
        if path == '/api/v1/namespaces/kube-system':
            value = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'kube-system',
                'uid': binding.kube_system_uid, 'resourceVersion': '1'}}
        else:
            value = resources.get(paths[path])
        response = httpx.Response(404) if value is None else httpx.Response(200, json=copy.deepcopy(value))
        if control.after_read is not None:
            control.after_read(path)
        return response

    with ManagementKubernetesTransport(api_server=config['kubernetes_api_server'], ssl_context=ssl.create_default_context(),
            token='operator-test-token') as transport:
        transport.client.close()
        transport.client = httpx.Client(base_url=config['kubernetes_api_server'], transport=httpx.MockTransport(respond))
        yield dict(predecessor=predecessor, bindings=bindings, shared_state=predecessor.state_dir,
            repo_root=inputs['repo_root'], transport=transport), control


def independent_manager(operation_inputs, preparation):
    options, _ = preparation
    previous = options['predecessor']
    staging = copy.deepcopy(previous.migration.registration.candidate)
    registration = replace(previous.migration.registration, candidate={**staging, 'candidate_sha': 'f' * 40})
    options['predecessor'] = replace(previous, migration=replace(previous.migration, registration=registration))
    operation_inputs['predecessor'] = options['predecessor']
    options['bindings'].plan.staging.source = SimpleNamespace(candidate=staging)


def test_first_release_uses_effective_catalog_not_original_installation(operation_inputs, preparation):
    options, _ = preparation
    original = options['bindings'].plan.development.runtime.resources
    # The initial catalog remains in installation history, but the selected
    # completed catalog is the one that exists in the current release view.
    original.pop(_key(operation_inputs['catalog']))
    original[_key(operation_inputs['manager'])]['spec']['template']['spec']['containers'][0]['image'] = 'old-manager'
    captured = module().capture_participant_release(**options)
    assert captured.catalog == operation_inputs['catalog']
    assert captured.manager == operation_inputs['manager']


@pytest.mark.parametrize('independent', [False, True])
def test_new_release_captures_complete_live_baseline_then_retains_it(operation_inputs, preparation, independent):
    if independent:
        independent_manager(operation_inputs, preparation)
    options, live = preparation
    captured = module().capture_participant_release(**options)
    current = {_key(row): row for row in captured.current}
    expected = {_key(row): row for row in operation_inputs['current'] if row['kind'] != 'Job'}
    assert current == expected
    assert {'ServiceAccount', 'NetworkPolicy'} <= {row['kind'] for row in captured.current}
    policies = {key: row for key, row in expected.items() if row['kind'].startswith('ValidatingAdmissionPolicy')}
    assert all(current[key] == row for key, row in policies.items())
    assert captured.config == operation_inputs['config']
    assert captured.management_origin == 'https://manage.example.test'
    assert captured.manager == operation_inputs['manager'] and captured.catalog == operation_inputs['catalog']
    retained = retain_participant_rollout(**{**operation_inputs, 'current': captured.current,
        'config': captured.config, 'manager': captured.manager, 'catalog': captured.catalog,
        'management_origin': captured.management_origin})
    assert retained.inputs.current == captured.current
    assert retained.release.refresh.before == options['predecessor'].migration.registration.spec
    assert all(live.reads.count(_resource_path(row)) == 2 for row in captured.current)
    assert all(row['kind'] not in {'Secret', 'Job'} for row in captured.current)


@pytest.mark.parametrize('damage', ['missing-account', 'account-token', 'network-policy', 'policy', 'policy-condition', 'binding-selector',
    'controller-identity', 'controller-template', 'collector-open', 'manager-catalog',
    'resource-replaced-during-read', 'history-changed', 'receipt-changed', 'endpoint'])
def test_new_release_rejects_incomplete_changed_or_foreign_baseline(operation_inputs, preparation, damage):
    options, live = preparation
    namespace = operation_inputs['config']['namespace']
    execution = operation_inputs['config']['execution_namespace']
    if damage == 'missing-account':
        del live.resources[f'ServiceAccount:{namespace}:loom-platform']
    elif damage == 'account-token':
        live.resources[f'ServiceAccount:{namespace}:loom-platform']['automountServiceAccountToken'] = True
    elif damage == 'network-policy':
        row = next(row for row in live.resources.values() if row['kind'] == 'NetworkPolicy')
        row['spec']['podSelector'] = {'matchLabels': {'foreign': 'true'}}
    elif damage in {'policy', 'policy-condition', 'binding-selector'}:
        kind = 'ValidatingAdmissionPolicyBinding' if damage == 'binding-selector' else 'ValidatingAdmissionPolicy'
        row = next((row for row in live.resources.values() if row['kind'] == kind), None)
        if row is None:
            pytest.skip('the zero-guest baseline has no admission policy')
        if damage == 'policy':
            row['spec']['validations'] = [{'expression': 'true'}]
        elif damage == 'policy-condition':
            row['spec']['matchConditions'] = [{'name': 'skip-everything', 'expression': 'false'}]
        else:
            row['spec']['matchResources']['objectSelector'] = {'matchLabels': {'skip-all-tasks': 'true'}}
    elif damage.startswith('controller-'):
        row = live.resources[f'Deployment:{namespace}:loom-control-plane']
        if damage == 'controller-identity':
            row['metadata']['uid'] = str(uuid4())
        else:
            row['spec']['template']['spec']['containers'][0]['image'] = 'foreign@sha256:' + 'f' * 64
    elif damage == 'collector-open':
        live.resources[f'CronJob:{execution}:loom-execution-capacity-collector']['spec']['suspend'] = False
    elif damage == 'manager-catalog':
        live.resources[_key(operation_inputs['catalog'])]['data']['profiles.json'] = '{}'
    elif damage == 'endpoint':
        options['transport'].api_server = 'https://foreign.invalid'
    else:
        def changed(path):
            if damage == 'history-changed':
                history, = options['predecessor'].history
                history.write_text('changed retained history')
            elif damage == 'receipt-changed':
                live.choices[0]['changed'] = True
            elif path.endswith('/serviceaccounts/loom-platform'):
                live.resources[f'ServiceAccount:{namespace}:loom-platform']['metadata']['uid'] = str(uuid4())
        live.after_read = changed
    with pytest.raises(ValueError, match='participant preparation'):
        module().capture_participant_release(**options)
    assert not operation_inputs['state_root'].exists() and not operation_inputs['anchor_dir'].exists()


def test_preparation_accepts_only_inert_admission_defaults(preparation):
    options, live = preparation
    policies = [row for row in live.resources.values() if row['kind'].startswith('ValidatingAdmissionPolicy')]
    if not policies:
        pytest.skip('the zero-guest baseline has no admission policy')
    for row in policies:
        field = 'matchConstraints' if row['kind'] == 'ValidatingAdmissionPolicy' else 'matchResources'
        match = row['spec'][field]
        match.setdefault('namespaceSelector', {})
        match.setdefault('objectSelector', {})
        match.setdefault('matchPolicy', 'Equivalent')
    result = module().capture_participant_release(**options)
    captured = {_key(row): row for row in result.current}
    assert all(captured[_key(row)] == row for row in policies)


@pytest.mark.parametrize('failure', [None, 'independent', 'publication', 'ancestry', 'source', 'renderer', 'qualification', 'schema'])
def test_host_prepares_exact_publication_and_replays_only_retained_inputs(
        operation_inputs, preparation, tmp_path, monkeypatch, failure):
    from scripts.ops import nebius_development_entry as authentication
    from scripts.ops import nebius_pool_participant_entry as entry
    from scripts.ops import nebius_shared_pool_entry as shared_entry

    from loom.nebius_kubernetes import NebiusKubernetesConnection
    from loom_service.environment_management.manager import CandidateBundle

    assert callable(getattr(entry, 'prepare_retained_participant_release', None)), 'host publication preparation is not connected'
    options, live = preparation
    inputs = operation_inputs
    if failure == 'independent':
        independent_manager(inputs, preparation)
        failure = None
    ca, credential = (tmp_path / name for name in ('current-ca.pem', 'current-operator.json'))
    for path in (ca, credential):
        path.write_bytes(b'current-private-test-material')
        path.chmod(0o600)
    operator = NebiusKubernetesConnection(endpoint=inputs['config']['kubernetes_api_server'],
        ca_file=ca, credentials_file=credential)
    historical = SimpleNamespace(bindings=options['bindings'], files={path: path.read_bytes()
        for path in inputs['predecessor'].history})
    monkeypatch.setattr(shared_entry, 'load_completed_history', lambda **_: (historical, inputs['predecessor']))

    async def authenticate(selected):
        assert selected == operator
        return ssl.create_default_context(), 'current-operator-token'

    monkeypatch.setattr(authentication, '_transport', authenticate)
    @contextmanager
    def transport(**selected):
        assert selected['api_server'] == operator.endpoint and selected['token'] == 'current-operator-token'
        yield options['transport']

    monkeypatch.setattr(entry, 'ManagementKubernetesTransport', transport, raising=False)
    bundle = CandidateBundle(inputs['publication'].candidate_id, inputs['candidate'],
        inputs['runtime_profile'].model_dump(mode='json'))
    if failure == 'schema':
        bundle.candidate['service_schema_head'] = 'head'
    publications, qualified = [], []
    source_reads = []

    async def render_source(*, candidate_id):
        assert candidate_id == inputs['publication'].candidate_id
        source_reads.append(candidate_id)
        if failure == 'source':
            raise ValueError('private-source-error')
        root = inputs['repo_root']
        paths = [*(root / 'deploy/k8s').rglob('*'), *(root / 'src/loom').rglob('*.py'),
            root / 'scripts/ops/render_nebius_platform.py']
        files = {path.relative_to(root).as_posix(): path.read_bytes() for path in paths
            if path.is_file() and '__pycache__' not in path.parts}
        module_path = 'src/loom/nebius_platform_render.py'
        files[module_path] = files[module_path].replace(b'cpu="25m"', b'cpu="133m"')
        renderer = 'scripts/ops/render_nebius_platform.py'
        files[renderer] = files[renderer].replace(b'        remote = build_regional_execution',
            b'        for row in files["40-services.yaml"]:\n'
            b'            if row["kind"] == "Deployment" and row["metadata"]["name"] == "loom-web":\n'
            b'                row["metadata"].setdefault("annotations", {})["test-selected-renderer"] = "yes"\n'
            b'        remote = build_regional_execution')
        if failure == 'renderer':
            files[renderer] = b'raise RuntimeError("private-renderer-error")\n'
        return files

    async def follows(**selected):
        assert selected == {'candidate_id': inputs['publication'].candidate_id,
            'current_sha': options['bindings'].plan.staging.attachment.runtime_profile.candidate_sha}
        return failure != 'ancestry'

    @asynccontextmanager
    async def publication(**selected):
        assert selected['publication'] == inputs['publication']
        assert selected['transport'] is options['transport']
        publications.append(selected['publication'])
        if failure == 'publication':
            raise ValueError('private-publication-error')
        yield SimpleNamespace(candidate_follows=follows, render_source=render_source), copy.deepcopy(bundle)

    monkeypatch.setattr(module(), 'resolve_participant_publication', publication, raising=False)

    # A callable qualifier is an injected external runtime boundary. The actual
    # source/history, HTTPS capture, retention and original replay are exercised.
    @contextmanager
    def delivery(**selected):
        class Qualifier:
            kube = object()

            def __call__(self, kube, retained, phase):
                assert kube is self.kube and phase == 'preflight' and retained == selected['retained']
                assert selected['expected'] == inputs['predecessor']
                qualified.append(retained.reference)
                if failure == 'qualification':
                    raise ValueError('private-preflight-error')
        yield SimpleNamespace(qualifier=Qualifier())

    monkeypatch.setattr(entry, 'authenticated_participant_release', delivery)
    selected = dict(publication=inputs['publication'], operation_id=inputs['operation_id'],
        shared_operation=tmp_path / 'original-shared-operation.json', shared_operation_sha256='c' * 64,
        state_root=inputs['state_root'], anchor_dir=inputs['anchor_dir'],
        operator_connection=operator, executable=tmp_path / 'kubectl', repo_root=inputs['repo_root'])
    if failure is not None:
        with pytest.raises(ValueError, match='participant preparation') as error:
            entry.prepare_retained_participant_release(**selected)
        assert 'private-' not in str(error.value)
        if failure in {'publication', 'ancestry', 'source', 'renderer', 'schema'}:
            assert not inputs['state_root'].exists() and not inputs['anchor_dir'].exists()
        return
    retained = entry.prepare_retained_participant_release(**selected)
    assert retained.inputs.publication == inputs['publication']
    assert retained.inputs.candidate == bundle.candidate
    web, = (row for row in retained.release.files['40-services.yaml'] if row['kind'] == 'Deployment'
        and row['metadata']['name'] == 'loom-web')
    assert web['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] == '133m'
    assert web['metadata']['annotations']['test-selected-renderer'] == 'yes'
    assert source_reads == [inputs['publication'].candidate_id]
    assert {_key(row): row for row in retained.inputs.current} == {
        _key(row): row for row in inputs['current'] if row['kind'] != 'Job'}
    assert publications == [inputs['publication']] and qualified == [retained.reference]
    count = len(live.reads)
    monkeypatch.setattr(module(), 'capture_participant_release', lambda **_: pytest.fail('repeat preparation cannot recapture'))
    repeated = entry.prepare_retained_participant_release(**selected)
    assert repeated == retained and len(live.reads) == count
    assert source_reads == [inputs['publication'].candidate_id]
    assert publications == [inputs['publication']] and qualified == [retained.reference, retained.reference]
    wrong = inputs['publication'].model_copy(update={'run_attempt': 3})
    with pytest.raises(ValueError, match='participant preparation'):
        entry.prepare_retained_participant_release(**{**selected, 'publication': wrong})
