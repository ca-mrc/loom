"""Handoff uses installed source-bound history, not a new rendering or staging."""
from __future__ import annotations

import hashlib
import json
import ssl
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_candidate import source_checkout as source_checkout
from tests.ops.test_nebius_development_cloud import cloud as cloud
from tests.ops.test_nebius_development_entry import entry as entry
from tests.ops.test_nebius_development_install import (
    InstallationAPI,
    storage_ready,
    workloads_ready,
)
from tests.ops.test_nebius_development_live import live as live
from tests.ops.test_nebius_development_management_install import installation as installation
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_development_preflight import preflight as preflight
from tests.ops.test_nebius_development_preflight import published_source as published_source
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_development_foundation import development_inputs as development_inputs
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def handoff(entry, installation):
    from scripts.ops.nebius_development_entry import load_inputs
    from scripts.ops.nebius_development_install import install_private_development

    operation, _, operation_path, _ = entry
    inputs, request, _ = load_inputs(operation)
    api = InstallationAPI(request)
    state, anchor = Path(operation['state_dir']), Path(operation['anchor_dir'])

    def install():
        return install_private_development(request=request, api=api, state_dir=state, anchor_dir=anchor)

    install()
    storage_ready(api, request.selection)
    workloads_ready(api, 'StatefulSet')
    install()
    workloads_ready(api, 'Job')
    install()
    workloads_ready(api, 'Deployment')
    result = install()
    original = json.loads((state / 'bootstrap/bootstrap.json').read_text())['material']
    manager = installation[0]
    raw = manager.deployment.model_dump(mode='json')
    raw['installation']['foundation']['platform_config_json'] = json.dumps(inputs.config)
    app = raw['installation']['applications']
    app['shared'].update(data_environment_id=inputs.binding.installation_id, cluster_id=inputs.config['cluster_id'])
    app['authority'].update(data_environment_id=inputs.binding.installation_id, cluster_id=inputs.config['cluster_id'])
    app['storage']['data_environment_id'] = inputs.binding.installation_id
    app['runtime']['kubernetes']['endpoint'] = inputs.config['kubernetes_api_server']
    manager = replace(manager, deployment=type(manager.deployment).model_validate(raw),
        binding=replace(manager.binding, kube_system_uid=inputs.binding.kube_system_uid),
        shared_namespace_uid=result['namespace_uid'], application_material=replace(manager.application_material,
            ca_pem=original['loom-platform-db']['ca.crt'],
            secret_store_master_keys=original['loom-platform-auth']['secret-store-master-key']))
    reference = {'operation_path': operation_path,
        'operation_sha256': hashlib.sha256(Path(operation_path).read_bytes()).hexdigest(),
        'installation_input_digest': json.loads((state / 'installation.json').read_text())['input_digest']}
    return reference, manager, api, state, anchor


def verify(handoff, monkeypatch, calls):
    from scripts.ops.nebius_development_management_foundation import (
        HTTPSRetainedDevelopmentFoundation,
        RetainedDevelopmentReference,
    )

    reference, request, store, _, _ = handoff
    api = HTTPSRetainedDevelopmentFoundation(api_server=request.deployment.installation.foundation.platform_config['kubernetes_api_server'],
        ssl_context=ssl.create_default_context(), token='manager-operator')
    resources = {'secrets': 'Secret', 'configmaps': 'ConfigMap', 'serviceaccounts': 'ServiceAccount',
        'services': 'Service', 'networkpolicies': 'NetworkPolicy', 'statefulsets': 'StatefulSet',
        'deployments': 'Deployment', 'jobs': 'Job', 'persistentvolumeclaims': 'PersistentVolumeClaim',
        'persistentvolumes': 'PersistentVolume'}

    def handle(req):
        calls.append(req)
        assert req.method == 'GET'
        collection, name = req.url.path.split('/')[-2:]
        if collection == 'namespaces':
            row = store.bootstrap.namespace if name == 'loom-dev' else {'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': 'kube-system', 'uid': request.binding.kube_system_uid}}
        elif collection == 'secrets' and name in store.bootstrap.secrets:
            row = store.bootstrap.secrets[name]
        else:
            row = store.stage.resources.get(resources[collection] + ':' + name)
        return httpx.Response(404) if row is None else httpx.Response(200, json=row)

    api.client.close()
    api.client = httpx.Client(base_url=api.api_server, transport=httpx.MockTransport(handle))
    with api:
        return api.verify(reference=RetainedDevelopmentReference.model_validate(reference), request=request)


def test_completed_foundation_handoff_never_rerenders_or_replays_installation(handoff, monkeypatch):
    from scripts.ops import nebius_development_stage

    before = {str(path): path.read_bytes() for root in handoff[3:] for path in root.rglob('*.json')}
    monkeypatch.setattr(nebius_development_stage, 'render_development_foundation',
        lambda *args, **kwargs: pytest.fail('new-source foundation render'))
    calls = []
    result = verify(handoff, monkeypatch, calls)
    assert result['namespace'] == 'loom-dev'
    assert result['namespace_uid'] == handoff[1].shared_namespace_uid
    assert result['data_environment_id'] == handoff[0]['operation_path'].split('/')[-2]
    assert calls and all(row.method == 'GET' for row in calls)
    assert all('loom-nebius-platform' not in str(row.url) for row in calls)
    assert {str(path): path.read_bytes() for root in handoff[3:] for path in root.rglob('*.json')} == before
    assert 'password' not in json.dumps(result) and 'PRIVATE KEY' not in json.dumps(result)


def test_manager_handoff_uses_only_durable_foundation_records_after_credentials_retire(handoff, monkeypatch):
    operation_path = Path(handoff[0]['operation_path'])
    operation_raw = operation_path.read_bytes()
    operation = json.loads(operation_raw)
    record = json.loads((Path(operation['state_dir']) / 'installation.json').read_text())
    reference = {'operation_path': operation_path, 'operation_sha256': hashlib.sha256(operation_raw).hexdigest(),
        'installation_input_digest': record['input_digest']}
    inputs = json.loads(Path(operation['inputs_path']).read_text())
    obsolete = {inputs['operator_connection']['credentials_file'], inputs['operator_connection']['ca_file'],
        *inputs['storage_files'].values()}
    for path in map(Path, obsolete):
        path.unlink()
    calls = []
    result = verify((reference, *handoff[1:]), monkeypatch, calls)
    assert result['namespace_uid'] == handoff[1].shared_namespace_uid
    assert calls and all(row.method == 'GET' for row in calls)


@pytest.mark.parametrize('damage', ['anchor', 'both', 'explicit'])
def test_retained_foundation_rejects_changed_qualification_preimage(handoff, monkeypatch, damage):
    from scripts.ops.nebius_management_install import ManagementInstallError

    reference, _, _, state, anchor = handoff
    paths = [next(anchor.glob('*.json'))]
    if damage == 'both':
        paths.append(state / 'installation.json')
    if damage == 'explicit':
        reference['qualification_digest'] = 'sha256:' + '0' * 64
    else:
        for path in paths:
            record = json.loads(path.read_text())
            record['qualification_digest'] = 'sha256:' + '0' * 64
            path.write_text(json.dumps(record))
    calls = []
    with pytest.raises(ManagementInstallError):
        verify(handoff, monkeypatch, calls)
    assert not calls


def test_old_foundation_history_requires_preserved_digest_without_rewriting_records(handoff, monkeypatch):
    from scripts.ops.nebius_management_install import ManagementInstallError

    reference, _, _, state, anchor = handoff
    preserved = None
    for path in (next(anchor.glob('*.json')), state / 'installation.json'):
        record = json.loads(path.read_text())
        preserved = record.pop('qualification_digest')
        path.write_text(json.dumps(record))
    before = {str(path): path.read_bytes() for root in handoff[3:] for path in root.rglob('*.json')}
    with pytest.raises(ManagementInstallError):
        verify(handoff, monkeypatch, [])
    reference['qualification_digest'] = preserved
    assert verify(handoff, monkeypatch, [])['namespace_uid'] == handoff[1].shared_namespace_uid
    assert {str(path): path.read_bytes() for root in handoff[3:] for path in root.rglob('*.json')} == before


@pytest.mark.parametrize('change', ['input-hash', 'anchor-digest', 'missing-anchor', 'phase-hash', 'pending-phase'])
def test_unqualified_retained_history_never_opens_live_reads(handoff, monkeypatch, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    reference, _, _, state, anchor = handoff
    if change == 'input-hash':
        reference['operation_sha256'] = '0' * 64
    elif change == 'anchor-digest':
        reference['installation_input_digest'] = 'sha256:' + '0' * 64
    elif change == 'missing-anchor':
        next(anchor.glob('*.json')).unlink()
    else:
        path = state / ('config/stage.json' if change == 'phase-hash' else 'installation.json')
        record = json.loads(path.read_text())
        if change == 'phase-hash':
            record['revision'] = 'sha256:' + '0' * 64
        else:
            record['phases']['services']['status'] = 'started'
        path.write_text(json.dumps(record))
    calls = []
    with pytest.raises(ManagementInstallError):
        verify(handoff, monkeypatch, calls)
    assert not calls


@pytest.mark.parametrize('change', ['namespace', 'secret', 'service', 'volume', 'not-ready', 'migration-failed'])
def test_handoff_rejects_changed_or_unready_live_foundation(handoff, monkeypatch, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    store = handoff[2]
    if change == 'namespace':
        store.bootstrap.namespace['metadata']['uid'] = str(uuid4())
    elif change == 'secret':
        store.bootstrap.secrets['loom-platform-auth']['data']['secret-store-master-key'] = 'Y2hhbmdlZA=='
    elif change in {'service', 'not-ready'}:
        row = store.stage.resources['Deployment:loom-service']
        if change == 'service':
            row['metadata']['uid'] = str(uuid4())
        else:
            row['status']['readyReplicas'] = 0
    elif change == 'volume':
        row = next(row for row in store.stage.resources.values() if row['kind'] == 'PersistentVolume')
        row['spec']['csi']['volumeHandle'] = 'staging-disk'
    else:
        row = next(row for row in store.stage.resources.values() if row['kind'] == 'Job')
        row['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}
    with pytest.raises(ManagementInstallError):
        verify(handoff, monkeypatch, [])


def test_newly_pinned_operation_cannot_rewrite_original_installation_inputs(handoff, monkeypatch):
    from scripts.ops.nebius_management_install import ManagementInstallError

    reference = handoff[0]
    path = Path(reference['operation_path'])
    operation = json.loads(path.read_text())
    inputs_path = Path(operation['inputs_path'])
    inputs = json.loads(inputs_path.read_text())
    inputs['candidate']['source_archive_sha256'] = 'sha256:' + 'a' * 64
    inputs['settings']['preflight']['source']['source_archive_sha256'] = 'sha256:' + 'a' * 64
    raw = json.dumps(inputs)
    inputs_path.write_text(raw)
    operation['inputs_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    path.write_text(json.dumps(operation))
    reference['operation_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    calls = []
    with pytest.raises(ManagementInstallError):
        verify(handoff, monkeypatch, calls)
    assert not calls
