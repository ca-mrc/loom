"""Independent production binds retained release checks to one exact target."""
from __future__ import annotations

import copy
import hashlib
import importlib
import io
import json
import zipfile
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_release_promotion_gate import _passing_evidence
from tests.unit.test_nebius_candidate_catalog import publication as publication


def target(kind):
    return dict(scope=dict(installation_id=str(uuid4()), pool_id=str(uuid4()), participant_id=str(uuid4()),
        environment_id=str(uuid4()), incarnation=str(uuid4()), authority=kind), namespace='app-' + kind,
        execution_namespace='execute-' + kind, database_identity='database-' + str(uuid4()), database_name='loom',
        provider_connection_namespace=kind + '-red', public_url='https://' + kind + '.example.com',
        object_storage={key: kind + '-canonical' for key in ('task_bucket', 'trajectories_bucket', 'artifacts_bucket')},
        secret_ref_prefix='github-environment:' + kind + '/RED_')


def evidence():
    value = _passing_evidence()
    value['schema_version'] = 2
    value['targets'] = {kind: target(kind) for kind in ('production', 'staging')}
    value['staging_url'] = value['targets']['staging']['public_url']
    isolation = value['checks']['prod_staging_isolation']
    for kind, selected in value['targets'].items():
        profiles = isolation['state_profiles'][kind]
        profiles.update({key: selected[key] for key in ('namespace', 'database_name', 'database_identity',
            'object_storage', 'provider_connection_namespace')})
        profiles['scope'] = selected['scope']
        profiles['secret_refs'] = {name: selected['secret_ref_prefix'] + name.upper() for name in profiles['secret_refs']}
        isolation['frontend'][kind].update(route=selected['public_url'], api_base=selected['public_url'] + '/api')
        isolation['workers'][kind].update(api_url=selected['public_url'] + '/api', k8s_namespace=selected['execution_namespace'])
        isolation['workers'][kind].update(source_commit=value['candidate_sha'],
            image=value['image_digests']['loom-execution-runtime'], image_digest=value['image_digests']['loom-execution-runtime'])
        value['checks']['frontend_route_evidence'][kind + '_route'] = selected['public_url']
        value['checks']['frontend_route_evidence'][kind + '_api_base'] = selected['public_url'] + '/api'
    for row in value['checks']['execution_capacity_smoke']['execution_records']:
        row['namespace'] = value['targets']['staging']['execution_namespace']
    return value


@pytest.mark.parametrize('damage', [None, 'owner', 'missing-check', 'namespace', 'same-database', 'shared-storage', 'scope', 'route'])
def test_generic_release_gate_keeps_existing_evidence_requirements(damage):
    from scripts.ops import release_gate

    value = evidence()
    if damage == 'owner':
        value['checks']['release_owner_approval']['status'] = 'skip'
    elif damage == 'missing-check':
        del value['checks']['execution_capacity_smoke']
    elif damage == 'namespace':
        value['checks']['prod_staging_isolation']['state_profiles']['production']['namespace'] = 'foreign'
    elif damage == 'same-database':
        value['targets']['production']['database_identity'] = value['targets']['staging']['database_identity']
    elif damage == 'shared-storage':
        value['targets']['production']['object_storage'] = value['targets']['staging']['object_storage']
    elif damage == 'scope':
        value['checks']['prod_staging_isolation']['state_profiles']['production']['scope']['authority'] = 'development'
    elif damage == 'route':
        value['checks']['frontend_route_evidence']['production_route'] = 'https://foreign.example.com'
    errors = release_gate.validate_manifest(value)
    assert bool(errors) == bool(damage), errors


def test_generic_gate_output_retains_target_and_authoritative_run_attempt(monkeypatch):
    from scripts.ops import release_gate

    value = evidence()
    monkeypatch.setenv('GITHUB_RUN_ID', '101')
    monkeypatch.setenv('GITHUB_RUN_ATTEMPT', '2')
    report = release_gate._evidence_report(value)
    assert report['targets'] == value['targets']
    assert report['gate_run'] == {'run_id': 101, 'run_attempt': 2}
    assert not release_gate.validate_manifest(report)


@pytest.mark.parametrize('identity', [None, ('101', 'invalid'), ('101', '0')])
def test_generic_gate_output_fails_closed_without_protected_identity(tmp_path, monkeypatch, capsys, identity):
    from scripts.ops import release_gate

    value = evidence()
    source, output = tmp_path / 'input.json', tmp_path / 'output.json'
    source.write_text(json.dumps(value))
    monkeypatch.delenv('GITHUB_RUN_ID', raising=False)
    monkeypatch.delenv('GITHUB_RUN_ATTEMPT', raising=False)
    if identity is not None:
        monkeypatch.setenv('GITHUB_RUN_ID', identity[0])
        monkeypatch.setenv('GITHUB_RUN_ATTEMPT', identity[1])
    assert release_gate.main(['validate', '--manifest', str(source), '--candidate-sha', value['candidate_sha'],
        '--image-tag', value['image_tag'], '--output-json', str(output)]) == 1
    assert not output.exists()
    assert 'release gate run identity unavailable' in capsys.readouterr().err


def promotion_module():
    return importlib.import_module('scripts.ops.nebius_production_promotion')


@pytest.fixture
def promotion_fixture(publication):
    from loom.nebius_candidate_contract import NEBIUS_PLATFORM_IMAGES

    reference, responses, payload, keyring, candidate = copy.deepcopy(publication)
    source = reference['source_sha']
    repo = {'id': 1281629473, 'full_name': 'ca-mrc/loom'}
    released, head = 'c' * 40, 'd' * 40
    release_report = evidence()
    release_report.update(candidate_sha=source, image_tag='candidate-' + source,
        image_digests={image: candidate['images'][key]['image_ref'] for key, image in NEBIUS_PLATFORM_IMAGES.items()},
        status='pass', gate_run={'run_id': 101, 'run_attempt': 2})
    for worker in release_report['checks']['prod_staging_isolation']['workers'].values():
        worker.update(source_commit=source, image=release_report['image_digests']['loom-execution-runtime'],
            image_digest=release_report['image_digests']['loom-execution-runtime'])
    main_report = dict(schema_version='loom.main-promotion-evidence.v2', candidate_sha=source, dev_head=source,
        pr_number=88, release_gate_run_id=101, release_gate_artifact_id=201, repository='ca-mrc/loom',
        source='dev', target='main', gate_run={'run_id': 102, 'run_attempt': 3})
    for run_id, attempt, workflow, workflow_id in ((101, 2, 'release-promotion-gate.yml', 501), (102, 3, 'main-promotion-gate.yml', 502)):
        responses[f'actions/runs/{run_id}/attempts/{attempt}'] = dict(id=run_id, run_attempt=attempt, head_sha=source,
            head_branch='dev', repository=repo, head_repository=repo, status='completed', conclusion='success',
            event='workflow_dispatch', path='.github/workflows/' + workflow, workflow_id=workflow_id)
        responses['actions/workflows/' + workflow] = dict(id=workflow_id, path='.github/workflows/' + workflow)
    responses.update({'pulls/88': dict(number=88, merged=True, state='closed', merge_commit_sha=released,
        base=dict(ref='main', repo=repo), head=dict(ref='dev', sha=source, repo=repo)),
        f'git/commits/{source}': dict(sha=source, tree={'sha': 'f' * 40}),
        f'git/commits/{released}': dict(sha=released, tree={'sha': 'f' * 40}),
        'branches/main': {'commit': {'sha': head}},
        f'compare/{released}...{head}': dict(base_commit={'sha': released}, merge_base_commit={'sha': released},
            ahead_by=3, behind_by=0, status='ahead')})
    selected = dict(schema_version='loom.production-promotion.v2', publication=reference, release_sha=released,
        release_pull_request=88, target=release_report['targets']['production'], staging=release_report['targets']['staging'],
        image_tag=release_report['image_tag'], prod_tag=release_report['prod_tag'])
    downloads = {'123': payload}

    def freeze():
        for field, run_id, attempt, artifact_id, report, filename, name in (
            ('release_gate', 101, 2, 201, release_report, 'release-gate-evidence.json', 'release-gate-evidence'),
            ('main_gate', 102, 3, 202, main_report, 'main-promotion-evidence.json', 'main-promotion-evidence-102-3')):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w') as archive:
                archive.writestr(filename, json.dumps(report))
            wire = buffer.getvalue()
            checksum = 'sha256:' + hashlib.sha256(wire).hexdigest()
            selected[field] = dict(run_id=run_id, run_attempt=attempt, artifact_id=artifact_id, artifact_sha256=checksum)
            downloads[str(artifact_id)] = wire
            responses[f'actions/artifacts/{artifact_id}'] = dict(id=artifact_id, name=name, expired=False,
                digest=checksum, size_in_bytes=len(wire), workflow_run=dict(id=run_id, head_sha=source, head_branch='dev',
                    repository_id=1281629473, head_repository_id=1281629473))
        return promotion_module().ProductionPromotionV2.model_validate(selected)

    def respond(request):
        if request.url.host == 'api.github.com':
            assert request.headers['Authorization'] == 'Bearer test-github-secret'
            path = request.url.path.removeprefix('/repositories/1281629473/')
            if path.endswith('/zip'):
                return httpx.Response(302, headers={'Location': 'https://store.blob.core.windows.net/' + path.split('/')[2]})
            return httpx.Response(200, json=responses[path])
        assert request.url.host == 'store.blob.core.windows.net'
        assert 'Authorization' not in request.headers
        return httpx.Response(200, content=downloads[request.url.path.strip('/')])

    return dict(freeze=freeze, responses=responses, selected=selected, release_report=release_report,
        main_report=main_report, keyring=keyring, candidate=candidate, transport=httpx.MockTransport(respond))


@pytest.mark.parametrize('damage', [None, 'scope', 'images', 'owner', 'gate-run', 'gate-attempt', 'gate-repository',
    'main-link', 'main-pr', 'tree', 'lineage', 'artifact', 'publication'])
async def test_real_production_qualifier_requires_publication_target_release_and_main_authority(promotion_fixture, damage):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import GitHubCandidateCatalog

    fixture = promotion_fixture
    report, main, responses = fixture['release_report'], fixture['main_report'], fixture['responses']
    if damage == 'images':
        report['image_digests']['loom-service'] = report['image_digests']['loom-service'].replace('sha256:', 'sha256:f', 1)[:-1]
    elif damage == 'owner':
        report['checks']['release_owner_approval']['status'] = 'skip'
    elif damage == 'main-link':
        main['release_gate_run_id'] = 999
    elif damage == 'main-pr':
        responses['pulls/88']['head']['sha'] = 'e' * 40
    elif damage == 'tree':
        responses['git/commits/' + 'c' * 40]['tree']['sha'] = 'e' * 40
    elif damage == 'lineage':
        responses['compare/' + 'c' * 40 + '...' + 'd' * 40]['merge_base_commit']['sha'] = 'e' * 40
    promotion = fixture['freeze']()
    if damage == 'gate-run':
        responses['actions/runs/101/attempts/2']['conclusion'] = 'failure'
    elif damage == 'gate-attempt':
        responses['actions/runs/101/attempts/2']['run_attempt'] = 1
    elif damage == 'gate-repository':
        responses['actions/runs/101/attempts/2']['repository'] = {'id': 7, 'full_name': 'ca-mrc/loom'}
    elif damage == 'artifact':
        responses['actions/artifacts/201']['digest'] = 'sha256:' + 'f' * 64
    elif damage == 'publication':
        responses['actions/artifacts/123']['expired'] = True
    scope = promotion.target.scope
    if damage == 'scope':
        scope = scope.model_copy(update={'participant_id': uuid4()})
    async with httpx.AsyncClient(transport=fixture['transport']) as http:
        catalog = GitHubCandidateCatalog(http, token='test-github-secret', publications=[promotion.publication],
            keyring=ImageAdmissionKeyring.from_json(fixture['keyring']), registry_prefix=fixture['candidate']['registry_prefix'])
        call = promotion_module().qualify_production_promotion(promotion=promotion, scope=scope,
            publication=promotion.publication, catalog=catalog)
        if damage:
            with pytest.raises(ValueError, match='production_promotion_unqualified'):
                await call
        else:
            proof = await call
            assert proof.scope == scope and proof.publication == promotion.publication
            assert proof.release_sha == 'c' * 40, 'previous promoted ancestors support separately authorized rollback'
            assert proof.image_digests == report['image_digests']


async def test_production_capability_exists_only_inside_qualified_fresh_exact_scope(promotion_fixture, monkeypatch):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import GitHubCandidateCatalog

    module = promotion_module()
    fixture = promotion_fixture
    promotion = fixture['freeze']()
    arguments = dict(scope=promotion.target.scope, publication=promotion.publication)
    with pytest.raises(ValueError):
        module.require_production_authority(**arguments)
    async with httpx.AsyncClient(transport=fixture['transport']) as http:
        catalog = GitHubCandidateCatalog(http, token='test-github-secret', publications=[promotion.publication],
            keyring=ImageAdmissionKeyring.from_json(fixture['keyring']), registry_prefix=fixture['candidate']['registry_prefix'])
        async with module.production_authority(promotion=promotion, catalog=catalog, **arguments):
            assert module.require_production_authority(**arguments) == promotion.target
            with pytest.raises(ValueError):
                module.require_production_authority(**dict(arguments, scope=promotion.staging.scope))
            now = module.time.monotonic()
            with monkeypatch.context() as patch:
                patch.setattr(module.time, 'monotonic', lambda: now + 301)
                with pytest.raises(ValueError):
                    module.require_production_authority(**arguments)
    with pytest.raises(ValueError):
        module.require_production_authority(**arguments)
