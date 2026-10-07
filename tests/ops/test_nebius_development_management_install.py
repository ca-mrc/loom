"""Fresh dev manager uses real journals/stages, never the legacy provisioner."""
from __future__ import annotations

import copy
import json
import shutil
from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_application_setup import application_material as application_material
from tests.ops.test_nebius_development_management_tls import tls_material as tls_material
from tests.ops.test_nebius_management_install import InstallationAPI
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class DevelopmentAPI(InstallationAPI):
    @contextmanager
    def application_resources(self, request, phase):
        assert request.shared_namespace_uid == self.shared_uid
        assert request.deployment.installation.applications.shared.platform_namespace == 'loom-dev'
        with self.resources(request.binding, phase) as api:
            yield api

    def qualify_storage(self, binding, rendered, receipt):
        self.events.append('provider-storage')
        if self.block == 'provider-storage':
            raise RuntimeError('private-provider-detail')

    def qualify_application(self, request, state_dir):
        self.events.append('application')
        if self.block == 'application':
            raise RuntimeError('private-application-detail')

    def admit(self):
        for doc in self.store.resources.values():
            if doc['kind'] == 'ValidatingAdmissionPolicy':
                doc['status'] = {'observedGeneration': 1, 'typeChecking': {'expressionWarnings': []}}

    def verify_public(self, binding, rendered, material_dir):
        super().verify_public(binding, rendered, material_dir)
        if self.block == 'late-drift':
            self.store.resources['ConfigMap:loom-platform-config']['data']['environment.json'] = '{}'


@pytest.fixture
def installation(application_management_inputs, material, application_material, tls_material):
    from scripts.ops.nebius_development_management_install import DevelopmentManagementRequest
    from scripts.ops.nebius_development_management_tls import management_tls_secret_name
    from scripts.ops.nebius_management_bootstrap import BootstrapBinding

    from loom_service.environment_management.deployment import ManagementDeployment

    raw, candidate, profile = copy.deepcopy(application_management_inputs)
    raw['namespace'] = 'loom-nebius-management-dev'
    raw['public_tls_secret_name'] = management_tls_secret_name(raw['installation_id'], tls_material)
    foundation = raw['installation']['foundation']
    config = json.loads(foundation['platform_config_json'])
    config.update(namespace='loom-dev', environment='development', execution_namespace='loom-nebius-dev-execution')
    foundation['platform_config_json'] = json.dumps(config)
    application = raw['installation']['applications']
    application['shared']['platform_namespace'] = 'loom-dev'
    application['authority'].update(namespace=raw['namespace'], shared_namespace='loom-dev')
    del material['loom-management-cloud']
    binding = BootstrapBinding(raw['installation_id'], raw['namespace'], str(uuid4()))
    request = DevelopmentManagementRequest(binding=binding, deployment=ManagementDeployment.model_validate(raw),
        candidate=candidate, profile=profile, material=material, application_material=application_material,
        shared_namespace_uid=str(uuid4()), tls_material=tls_material)
    api = DevelopmentAPI(binding)
    api.shared_uid = request.shared_namespace_uid
    return request, api


def run(installation, tmp_path):
    from scripts.ops.nebius_development_management_install import install_development_management

    request, api = installation
    return install_development_management(request=request, api=api,
        state_dir=tmp_path / 'state', anchor_dir=tmp_path / 'anchor')


def to_admission(installation, tmp_path):
    assert run(installation, tmp_path)['phase'] == 'database'
    installation[1].complete('StatefulSet')
    assert run(installation, tmp_path)['phase'] == 'migration'
    installation[1].complete('Job')
    assert run(installation, tmp_path)['phase'] == 'application-admission'


def test_fresh_install_orders_real_database_admission_sql_backup_and_public_barriers(installation, tmp_path):
    request, api = installation
    to_admission(installation, tmp_path)
    assert 'provider-storage' in api.events
    assert not any(doc['kind'] in {'ClusterRoleBinding', 'RoleBinding', 'Deployment', 'Ingress'}
                   for doc in api.store.resources.values())
    before = len(api.store.creates)
    assert run(installation, tmp_path)['phase'] == 'application-admission'
    assert len(api.store.creates) == before
    api.admit()
    assert run(installation, tmp_path)['phase'] == 'application-database'
    assert 'application' in api.events
    assert not any(doc['kind'] == 'Deployment' for doc in api.store.resources.values())
    api.complete('Job')
    assert run(installation, tmp_path)['phase'] == 'backup'
    assert 'backup' not in api.events
    api.complete('Job')
    assert run(installation, tmp_path)['phase'] == 'service'
    assert not any(doc['kind'] == 'Ingress' for doc in api.store.resources.values())
    api.complete('Deployment')
    result = run(installation, tmp_path)
    assert result['status'] == 'development_management_installed'
    assert result['shared_namespace_uid'] == request.shared_namespace_uid
    assert api.events[-1] == 'public'
    before = len(api.store.creates)
    assert run(installation, tmp_path) == result
    assert len(api.store.creates) == before
    assert 'Secret:loom-management-cloud' not in api.store.resources
    assert 'ServiceAccount:loom-management-provisioner' not in api.store.resources
    assert not any('legacy-pods' in doc['metadata']['name'] for doc in api.store.resources.values())
    assert {doc['metadata'].get('namespace') for doc in api.store.resources.values()} <= {
        None, 'loom-dev', 'loom-nebius-management-dev'}
    assert len([doc for doc in api.store.resources.values() if doc['kind'] == 'StatefulSet']) == 1
    tls_name = request.deployment.public_tls_secret_name
    assert api.store.creates.index('Secret:' + tls_name) < api.store.creates.index('Ingress:loom-management')
    assert api.store.resources['Ingress:loom-management']['spec']['tls'] == [{
        'hosts': ['manage.example.com'], 'secretName': tls_name}]


@pytest.mark.parametrize('change', ['host', 'secret'])
def test_missing_or_mismatched_public_certificate_blocks_before_bootstrap(installation, tmp_path, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    if change == 'host':
        request = replace(request, tls_material=replace(request.tls_material, public_host='foreign.example.com'))
    else:
        request = replace(request, deployment=request.deployment.model_copy(update={'public_tls_secret_name': None}))
    with pytest.raises((ManagementInstallError, ValueError)):
        run((request, api), tmp_path)
    assert api.store is None


@pytest.mark.parametrize('phase', ['preflight', 'provider-storage', 'application', 'backup', 'public'])
def test_failed_live_qualification_never_reports_installed(installation, tmp_path, phase):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api = installation[1]
    if phase != 'preflight':
        run(installation, tmp_path)
        api.complete('StatefulSet')
    if phase in {'application', 'backup', 'public'}:
        run(installation, tmp_path)
        api.complete('Job')
        run(installation, tmp_path)
        api.admit()
    if phase in {'backup', 'public'}:
        run(installation, tmp_path)
        api.complete('Job')
        run(installation, tmp_path)
        api.complete('Job')
    if phase == 'public':
        run(installation, tmp_path)
        api.complete('Deployment')
    api.block = phase
    with pytest.raises(ManagementInstallError) as error:
        run(installation, tmp_path)
    assert 'private-' not in str(error.value)
    if phase in {'provider-storage', 'application', 'backup'}:
        assert not any(doc['kind'] in {'Deployment', 'Ingress'} for doc in api.store.resources.values())


@pytest.mark.parametrize('missing', ['state', 'state/bootstrap', 'state/config', 'anchor'])
def test_lost_recovery_evidence_does_not_reopen_writes(installation, tmp_path, missing):
    from scripts.ops.nebius_management_install import ManagementInstallError

    run(installation, tmp_path)
    api = installation[1]
    before = len(api.store.creates)
    shutil.rmtree(tmp_path / missing)
    with pytest.raises(ManagementInstallError, match='recovery'):
        run(installation, tmp_path)
    assert len(api.store.creates) == before


def test_recovery_cannot_rebind_shared_namespace(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    run(installation, tmp_path)
    request, api = installation
    before = len(api.store.creates)
    with pytest.raises(ManagementInstallError, match='recovery'):
        run((replace(request, shared_namespace_uid=str(uuid4())), api), tmp_path)
    assert len(api.store.creates) == before


def test_recovery_freezes_private_entry_configuration_and_files(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    request = replace(request, qualification_digest='sha256:' + 'a' * 64)
    run((request, api), tmp_path)
    before = len(api.store.creates)
    changed = replace(request, qualification_digest='sha256:' + 'b' * 64)
    with pytest.raises(ManagementInstallError, match='recovery'):
        run((changed, api), tmp_path)
    assert len(api.store.creates) == before


@pytest.mark.parametrize('value', ['', 'a' * 64, 'sha256:' + 'A' * 64, 12])
def test_private_entry_fingerprint_has_canonical_shape(installation, value):
    from scripts.ops.nebius_management_install import ManagementInstallError

    with pytest.raises(ManagementInstallError):
        replace(installation[0], qualification_digest=value)


def test_retained_backup_and_shared_setup_are_in_system_capacity_envelope(installation):
    from scripts.ops.nebius_development_management_install import render_installation

    from loom.nebius_environment_render import _envelope

    rendered = render_installation(installation[0])
    assert len(rendered.files['10-config-network.yaml']) == 5
    assert len(rendered.files['application-config.yaml']) == 2
    assert 'ttlSecondsAfterFinished' not in rendered.files['85-backup-verify.yaml'][0]['spec']
    without_setup = {name: docs for name, docs in rendered.files.items() if name != 'application-database.yaml'}
    assert rendered.platform_envelope.cpu_millis > _envelope(without_setup).cpu_millis


def test_staging_binding_is_rejected_before_bootstrap(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    raw = request.deployment.model_dump(mode='json')
    config = json.loads(raw['installation']['foundation']['platform_config_json'])
    config['namespace'] = 'loom-nebius-platform'
    raw['installation']['foundation']['platform_config_json'] = json.dumps(config)
    raw['installation']['applications']['shared']['platform_namespace'] = 'loom-nebius-platform'
    raw['installation']['applications']['authority']['shared_namespace'] = 'loom-nebius-platform'
    deployment = type(request.deployment).model_validate(raw)
    with pytest.raises(ManagementInstallError):
        run((replace(request, deployment=deployment), api), tmp_path)
    assert not api.bootstrap.creates
    assert api.store is None


def test_late_resource_drift_cannot_report_completed_installation(installation, tmp_path):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api = installation[1]
    to_admission(installation, tmp_path)
    api.admit()
    run(installation, tmp_path)
    api.complete('Job')
    run(installation, tmp_path)
    api.complete('Job')
    run(installation, tmp_path)
    api.complete('Deployment')
    api.block = 'late-drift'
    before = len(api.store.creates)
    with pytest.raises(ManagementInstallError):
        run(installation, tmp_path)
    assert api.store.creates[before:] == ['Secret:' + installation[0].deployment.public_tls_secret_name,
        'Ingress:loom-management']


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_application_create_does_not_repeat_request(installation, tmp_path, failure):
    from scripts.ops.nebius_management_install import ManagementInstallError

    api = installation[1]
    to_admission(installation, tmp_path)
    api.admit()
    api.store.failure = failure
    before = len(api.store.creates)
    for _ in range(2):
        if failure == 'before':
            with pytest.raises(ManagementInstallError):
                run(installation, tmp_path)
        else:
            assert run(installation, tmp_path)['phase'] == 'application-database'
    assert len(api.store.creates) == len(set(api.store.creates))
    if failure == 'before':
        assert len(api.store.creates) == before + 1


@pytest.mark.parametrize('change', ['shared-password', 'publication', 'legacy-secret'])
def test_invalid_material_is_rejected_before_bootstrap(installation, tmp_path, change):
    from scripts.ops.nebius_management_install import ManagementInstallError

    request, api = installation
    if change == 'shared-password':
        request = replace(request, application_material=replace(request.application_material, manager_password=''))
    elif change == 'publication':
        request.material['loom-management-publications']['token'] = ''
    else:
        request.material['loom-management-cloud'] = {'credentials.json': '{"unexpected":"key"}'}
    with pytest.raises(ManagementInstallError):
        run((request, api), tmp_path)
    assert not api.bootstrap.creates
    assert api.store is None
