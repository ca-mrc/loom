"""Image-only release projection preserves participant and process authority."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json
from uuid import uuid4

import pytest
from scripts.ops.nebius_environment_runtime import render_environment_runtime
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.ops.test_nebius_production_promotion import promotion_fixture as promotion_fixture
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_management_render import management_inputs as management_inputs

from loom.nebius_environment_foundation import render_foundation
from loom_service.environment_management.manager import CandidateBundle


@pytest.fixture
def release_inputs(runtime_request):
    runtime = render_environment_runtime(**runtime_request)
    candidate = copy.deepcopy(runtime_request['candidate'])
    candidate['service_schema_head'] = '0178'
    profile = runtime_request['runtime_profile'].model_dump(mode='json')
    foundation = render_foundation(runtime_request['declaration'], candidate, profile=profile,
        keyring=runtime_request['profiles'].image_admission_keyring, repo_root=runtime_request['repo_root'])
    resources = {_key(row): copy.deepcopy(row) for docs in foundation.files.values() for row in docs
        if row['kind'] == 'Deployment' or (row['kind'] == 'ConfigMap' and row['metadata']['name'] == 'loom-platform-config')}
    resources.update({_key(row): copy.deepcopy(row) for row in (*runtime.workloads.values(), *runtime.isolation)})
    for row in resources.values():
        row['metadata']['uid'] = str(uuid4())
        if row['kind'] == 'Deployment':
            row['spec']['replicas'] = 1
    following, next_profile = copy.deepcopy(candidate), copy.deepcopy(profile)
    following['candidate_sha'] = next_profile['candidate_sha'] = '2' * 40
    for index, item in enumerate(following['images'].values()):
        item['image_ref'] = item['image_ref'].split('@')[0] + '@sha256:' + hashlib.sha256(str(index).encode()).hexdigest()
    for field, image in (('task_image_ref', 'service'), ('runtime_image_ref', 'execution_runtime'), ('agent_image_ref', 'harbor_runtime')):
        if image in following['images']:
            next_profile[field] = following['images'][image]['image_ref']
    from tests.support.execution_image_admission import signed_image_admission_bundle
    next_profile['image_admission'] = signed_image_admission_bundle(tuple(next_profile[field] for field in ('task_image_ref', 'runtime_image_ref'))).model_dump(mode='json')
    next_profile['runtime_binary_sha256'] = 'sha256:' + 'e' * 64
    return dict(operation_id=uuid4(), participant=runtime_request['participant'], profiles=runtime_request['profiles'],
        previous=CandidateBundle(uuid4(), candidate, profile), following=CandidateBundle(uuid4(), following, next_profile),
        resources=resources, application_namespace=runtime_request['declaration'].registration.application_namespace)


def project(**kwargs):
    name = 'scripts.ops.nebius_environment_release_contracts'
    assert importlib.util.find_spec(name), 'connected release projection is missing'
    return importlib.import_module(name).release_projection(**kwargs)


def test_release_renumbers_profiles_and_replaces_only_bound_images_and_runtime_settings(release_inputs):
    original = copy.deepcopy(release_inputs)
    release, targets = project(**release_inputs)
    before = release_inputs['participant']
    assert release.participant.binding_revision == before.binding_revision + 1
    assert release.participant.model_dump(exclude={'targets', 'binding_revision'}) == before.model_dump(exclude={'targets', 'binding_revision'})
    assert {row.profile_id for row in release.participant.targets}.isdisjoint(row.profile_id for row in before.targets)
    assert release_inputs == original
    assert all(row['metadata']['uid'] == release_inputs['resources'][key]['metadata']['uid'] for key, row in targets.items())
    assert {row['kind'] for row in targets.values()} == {'Deployment', 'ConfigMap', 'ValidatingAdmissionPolicy'}
    for row in targets.values():
        if row['kind'] != 'Deployment':
            continue
        container, = row['spec']['template']['spec']['containers']
        assert container['image'] in {item['image_ref'] for item in release_inputs['following'].candidate['images'].values()}
        for setting in container.get('env', []):
            if setting['name'] in {'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'}:
                assert json.loads(setting['value'])['participant'] == release.participant.model_dump(mode='json')
    assert project(**release_inputs) == (release, targets)


@pytest.mark.parametrize('damage', ['schema', 'capability', 'namespace', 'foreign-image', 'credential'])
def test_release_refuses_schema_protocol_or_runtime_authority_drift(release_inputs, damage):
    if damage == 'schema':
        release_inputs['following'].candidate['service_schema_head'] = '0179'
    elif damage == 'capability':
        release_inputs['following'].profile['supports_task_identity'] = not release_inputs['previous'].profile.get('supports_task_identity', False)
    else:
        key = next(key for key in release_inputs['resources'] if key.endswith(':loom-control-plane'))
        resource = release_inputs['resources'][key]
        if damage == 'namespace':
            resource['metadata']['namespace'] = 'foreign'
        else:
            container, = resource['spec']['template']['spec']['containers']
            if damage == 'foreign-image':
                container['image'] = 'foreign/image@sha256:' + 'c' * 64
            else:
                setting = next(row for row in container['env'] if row['name'] == 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON')
                value = json.loads(setting['value'])
                value['participant']['incarnation'] = str(uuid4())
                setting['value'] = json.dumps(value)
    with pytest.raises(ValueError):
        project(**release_inputs)


@pytest.mark.parametrize('failure', [None, 'before', 'after'])
def test_release_runtime_journal_covers_config_and_policy_with_no_unknown_repatch(release_inputs, tmp_path, failure):
    from scripts.ops.nebius_development_runtime_transition import _advance_fixed_resources
    from scripts.ops.nebius_environment_release_runtime import PHASES, release_options
    from scripts.ops.nebius_environment_release_runtime import release_inputs as inputs
    from scripts.ops.nebius_environment_runtime_install import qualify_runtime_defaulted
    from tests.ops.test_nebius_development_runtime_transition import RuntimeAPI

    from loom_service.pool_management.capacity import digest

    _, targets = project(**release_inputs)
    plan = {'originals': release_inputs['resources'], 'targets': targets}
    api = RuntimeAPI(plan['originals'])
    api.ready = True
    for row in api.rows.values():
        row['metadata']['resourceVersion'] = '1'
    cursor = dict.fromkeys(PHASES, 'prepared')
    for phase in PHASES:
        before, desired = inputs(plan, tmp_path, phase)
        cursor[phase] = 'intent'
        # Parent intent may be durable immediately before the child journal.
        assert release_options(plan, tmp_path, cursor)
        api.failure = failure if phase == 'replace' else None
        def advance(before=before, desired=desired, phase=phase):
            return _advance_fixed_resources(originals=before, targets=desired, input_digest='sha256:' + digest(plan),
                phase=phase, api=api, state_dir=tmp_path / phase, phases=frozenset(PHASES),
                kinds=frozenset({'Deployment', 'ConfigMap', 'ValidatingAdmissionPolicy'}),
                qualify_defaulted=qualify_runtime_defaulted)
        if phase == 'replace' and failure == 'before':
            count = len(api.patches)
            for _ in range(2):
                with pytest.raises(ValueError, match='preserve evidence'):
                    advance()
            assert len(api.patches) == count + 1
            assert any(len(value) == 2 for value in release_options(plan, tmp_path, cursor).values())
            return
        assert advance()
        cursor[phase] = 'complete'
    options = release_options(plan, tmp_path, cursor)
    assert all(len(choices) == 1 for choices in options.values())
    for key, target in targets.items():
        from scripts.ops.nebius_management_switch import _matches
        assert _matches(options[key][0], target, target['metadata']['uid'])
    # Every selected Deployment is stopped, replaced and started once. Config
    # and policy each receive one CAS even when the successful reply is lost.
    assert len(api.patches) == sum(3 if doc['kind'] == 'Deployment' else 1 for doc in targets.values())


def test_release_configmap_transport_compares_uid_version_and_data(release_inputs):
    import httpx
    from scripts.ops.nebius_environment_release_runtime import EnvironmentReleaseWorkloads
    from scripts.ops.nebius_ingress_stage import _snapshot

    _, targets = project(**release_inputs)
    key = next(key for key in targets if key.startswith('ConfigMap:'))
    before = copy.deepcopy(release_inputs['resources'][key])
    before['metadata']['resourceVersion'] = '31'
    desired = _snapshot(targets[key])
    calls = []
    class API:
        def __init__(self):
            self.client = httpx.Client(base_url='https://kubernetes.example.com', transport=httpx.MockTransport(self.handle))
        def handle(self, request):
            patches = json.loads(request.content)
            calls.append(patches)
            assert patches[:3] == [
                {'op': 'test', 'path': '/metadata/uid', 'value': before['metadata']['uid']},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '31'},
                {'op': 'test', 'path': '/data', 'value': before['data']}]
            assert request.url.path.endswith('/configmaps/loom-platform-config')
            value = copy.deepcopy(desired)
            value['metadata'].update(uid=before['metadata']['uid'], resourceVersion='32')
            return httpx.Response(200, json=value)
    class Workloads(EnvironmentReleaseWorkloads):
        def qualify(self):
            pass
    api = API()
    try:
        workloads = Workloads(api, {key: before}, {key: desired}, 'eu-north1', {}, build_policy={}, build_profile=None)
        assert workloads.preview_workload(key, before, desired)
        assert workloads.patch_workload(key, before, desired)
        assert len(calls) == 2
    finally:
        api.client.close()


def test_waiting_builds_keep_native_admission_through_forward_release_and_rollback(release_inputs):
    release, targets = project(**release_inputs)
    key = next(key for key in targets if key.startswith('ValidatingAdmissionPolicy:'))
    expression = targets[key]['spec']['validations'][-1]['expression']
    assert release_inputs['previous'].profile['task_image_ref'] in expression
    assert release_inputs['following'].profile['task_image_ref'] in expression
    from loom_service.pool_management.profiles import PoolProfileCatalog
    # Anchored JSON records sort mapping keys; recovery must regenerate exactly
    # the same policy and retain historical contracts after process restart.
    def persisted(value):
        return json.loads(json.dumps(value, sort_keys=True))
    rollback, reverted = project(operation_id=uuid4(), participant=release.participant,
        profiles=PoolProfileCatalog.model_validate(persisted(release.profiles.model_dump(mode='json'))),
        previous=release_inputs['following'], following=release_inputs['previous'],
        resources=persisted(release_inputs['resources'] | targets), application_namespace=release_inputs['application_namespace'],
        historical_profiles=(PoolProfileCatalog.model_validate(persisted(release_inputs['profiles'].model_dump(mode='json'))),))
    assert rollback.participant.binding_revision == 3
    assert key not in reverted, 'rollback must preserve still-valid historical native build contracts'
    assert {row.profile_id for row in rollback.participant.targets}.isdisjoint(
        row.profile_id for row in release_inputs['participant'].targets)


@pytest.mark.parametrize('damage', [None, 'selector', 'operator', 'duplicate-key', 'suffix'])
def test_historical_build_policy_accepts_only_inert_selector_map_order(release_inputs, damage):
    from scripts.ops.nebius_development_runtime_retained import qualify_build_policy_spec
    desired = next(doc for doc in release_inputs['resources'].values() if doc['kind'] == 'ValidatingAdmissionPolicy')
    observed = copy.deepcopy(desired)
    expression = observed['spec']['validations'][-1]['expression']
    start = expression.index('(variables.p.nodeSelector == ') + len('(variables.p.nodeSelector == ')
    selector, length = json.JSONDecoder().raw_decode(expression[start:])
    literal = json.dumps(dict(reversed(list(selector.items()))), separators=(',', ':'))
    if damage == 'selector':
        literal = literal.replace('amd64', 'arm64')
    elif damage == 'duplicate-key':
        literal = literal[:-1] + ',"loom.nebius/node-arch":"amd64"}'
    expression = expression[:start] + literal + expression[start + length:]
    if damage == 'operator':
        expression = expression.replace('nodeSelector ==', 'nodeSelector !=', 1)
    elif damage == 'suffix':
        expression += ' || true'
    observed['spec']['validations'][-1]['expression'] = expression
    if damage is None:
        qualify_build_policy_spec(desired, observed)
    else:
        with pytest.raises(ValueError):
            qualify_build_policy_spec(desired, observed)


def test_release_selection_requires_one_exact_v2_or_imported_predecessor(release_inputs):
    from scripts.ops.nebius_environment_release_contracts import EnvironmentReleaseV2

    from loom_service.environment_management.candidates import ProtectedPublication

    publication = ProtectedPublication(candidate_id=uuid4(), source_sha='a' * 40, run_id=1, run_attempt=1,
        artifact_id=1, artifact_sha256='sha256:' + 'a' * 64, pull_request=1)
    with pytest.raises(ValueError):
        EnvironmentReleaseV2(runtime_operation_id=uuid4(), publication=publication)
    selected = EnvironmentReleaseV2(runtime_operation_id=uuid4(), imported_head=uuid4(), publication=publication)
    assert selected.predecessor is None
    with pytest.raises(ValueError):
        EnvironmentReleaseV2.model_validate(selected.model_dump() | {'previous_release_id': uuid4()})


def test_imported_mixed_web_pin_uses_only_its_exact_snapshot_and_published_role(release_inputs):
    key = 'Deployment:' + release_inputs['application_namespace'] + ':loom-web'
    old = 'registry.example/old-web@sha256:' + '7' * 64
    release_inputs['resources'][key]['spec']['template']['spec']['containers'][0]['image'] = old
    with pytest.raises(ValueError):
        project(**release_inputs)
    _, projected = project(**release_inputs, imported_role_images={key: old})
    assert projected[key]['spec']['template']['spec']['containers'][0]['image'] == release_inputs['following'].candidate['images']['web']['image_ref']
    with pytest.raises(ValueError):
        project(**release_inputs, imported_role_images={key: old + 'changed'})
    with pytest.raises(ValueError):
        project(**release_inputs, imported_role_images={key.replace('loom-web', 'loom-service'): old})


async def test_production_release_requires_actual_source_and_rechecks_expiry(promotion_fixture, monkeypatch):
    from contextlib import AsyncExitStack
    from types import SimpleNamespace

    import httpx
    from scripts.ops import nebius_production_promotion as promotion_module
    from scripts.ops.nebius_environment_release import require_release_production_authority

    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import GitHubCandidateCatalog

    fixture = promotion_fixture
    original = fixture['freeze']()
    # Distinct protected publication identities must each carry their own
    # capability even when they reference the same signed candidate artifact.
    baseline, source, following = [original.model_copy(update={'publication':
        original.publication.model_copy(update={'candidate_id': uuid4()})}) for _ in range(3)]
    target = original.target
    cluster_id = 'cluster-production'
    actual = f'kubernetes:{cluster_id}/{target.namespace}/loom-postgres/loom'
    fixture['selected']['target']['database_identity'] = actual
    fixture['release_report']['targets']['production']['database_identity'] = actual
    fixture['release_report']['checks']['prod_staging_isolation']['state_profiles']['production']['database_identity'] = actual
    original = fixture['freeze']()
    baseline, source, following = [original.model_copy(update={'publication': item.publication})
        for item in (baseline, source, following)]
    policy = SimpleNamespace(participant=SimpleNamespace(environment_class='production'),
        application_publication=baseline.publication, host=SimpleNamespace(scope=target.scope),
        foundation=SimpleNamespace(identity=SimpleNamespace(application_namespace=target.namespace,
            execution_namespace=target.execution_namespace, public_host=target.public_url.removeprefix('https://'), cluster_id=cluster_id)))
    async with httpx.AsyncClient(transport=fixture['transport']) as http, AsyncExitStack() as authorities:
        catalog = GitHubCandidateCatalog(http, token='test-github-secret',
            publications=[item.publication for item in (baseline, source, following)],
            keyring=ImageAdmissionKeyring.from_json(fixture['keyring']), registry_prefix=fixture['candidate']['registry_prefix'])
        for item in (baseline, following):
            await authorities.enter_async_context(promotion_module.production_authority(promotion=item,
                scope=target.scope, publication=item.publication, catalog=catalog))
        with pytest.raises(ValueError, match='production_promotion_required'):
            require_release_production_authority(policy, source.publication, following.publication)
        await authorities.enter_async_context(promotion_module.production_authority(promotion=source,
            scope=target.scope, publication=source.publication, catalog=catalog))
        require_release_production_authority(policy, source.publication, following.publication)
        now = promotion_module.time.monotonic()
        with monkeypatch.context() as patch:
            patch.setattr(promotion_module.time, 'monotonic', lambda: now + 301)
            with pytest.raises(ValueError, match='production_promotion_required'):
                require_release_production_authority(policy, source.publication, following.publication)
