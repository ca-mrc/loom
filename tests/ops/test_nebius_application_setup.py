"""Fixed shared setup stages reuse existing journals and bind both namespaces."""
from __future__ import annotations

import copy
import ssl
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from tests.ops.test_nebius_management_stage import PhaseAPI
from tests.unit.test_nebius_application_setup import ROOT, render_setup
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def setup_request(application_management_inputs):
    from scripts.ops.nebius_application_setup import ApplicationSetupRequest
    from scripts.ops.nebius_management_material import ManagementBinding

    from loom_service.environment_management.deployment import ManagementDeployment

    raw, candidate, profile = application_management_inputs
    binding = ManagementBinding(raw['installation_id'], raw['namespace'], str(uuid4()), str(uuid4()))
    request = ApplicationSetupRequest(ManagementDeployment.model_validate(raw), candidate, profile,
        binding, str(uuid4()), ROOT)
    return request, PhaseAPI(binding)


@pytest.mark.parametrize('phase', ['admission', 'permissions', 'network', 'database'])
def test_setup_phase_replays_exact_uids_without_recreating(setup_request, tmp_path, phase):
    from scripts.ops.nebius_application_setup import stage_application_setup

    request, api = setup_request
    args = dict(request=request, phase=phase, api=api, state_dir=tmp_path / 'state')
    first = stage_application_setup(**args)
    observed = copy.deepcopy(api.resources)
    assert stage_application_setup(**args) == first
    assert api.resources == observed and len(api.creates) == len(observed)


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_setup_create_never_resends(setup_request, tmp_path, failure):
    from scripts.ops.nebius_application_setup import stage_application_setup
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = setup_request
    api.failure = failure
    args = dict(request=request, phase='database', api=api, state_dir=tmp_path / 'state')
    if failure == 'before':
        for _ in range(2):
            with pytest.raises(ManagementStageError, match='unresolved'):
                stage_application_setup(**args)
        assert len(api.creates) == 1
    else:
        first = stage_application_setup(**args)
        assert stage_application_setup(**args) == first
        assert len(api.creates) == 2


def test_setup_recovery_cannot_rebind_a_recreated_shared_namespace(setup_request, tmp_path):
    from scripts.ops.nebius_application_setup import stage_application_setup
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = setup_request
    args = dict(request=request, phase='network', api=api, state_dir=tmp_path / 'state')
    stage_application_setup(**args)
    with pytest.raises(ManagementStageError):
        stage_application_setup(**(args | {'request': replace(request, shared_namespace_uid=str(uuid4()))}))
    assert len(api.creates) == 4


@pytest.mark.parametrize('api_group', ['', 'rbac.authorization.k8s.io'])
def test_shared_binding_accepts_only_service_account_api_group_default(setup_request, tmp_path, api_group):
    from scripts.ops.nebius_application_setup import stage_application_setup
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = setup_request

    def default_subject(doc):
        if doc['kind'] == 'RoleBinding':
            for subject in doc['subjects']:
                subject['apiGroup'] = api_group

    api.default_change = default_subject
    args = dict(request=request, phase='permissions', api=api, state_dir=tmp_path / 'state')
    if api_group:
        with pytest.raises(ManagementStageError, match='defaulting'):
            stage_application_setup(**args)
        assert not api.creates
    else:
        first = stage_application_setup(**args)
        assert stage_application_setup(**args) == first
        binding = next(doc for doc in api.resources.values() if doc['kind'] == 'RoleBinding')
        assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-application-provisioner',
            'namespace': request.binding.namespace, 'apiGroup': ''}]


def test_admission_readiness_requires_current_typechecking_and_keeps_bootstrap_ungranted(setup_request, tmp_path):
    from scripts.ops.nebius_application_setup import (
        application_setup_ready,
        stage_application_setup,
    )
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = setup_request
    args = dict(request=request, phase='admission', api=api, state_dir=tmp_path / 'state')
    stage_application_setup(**args)
    assert not any(doc['kind'] == 'ClusterRoleBinding' for doc in api.resources.values())
    assert application_setup_ready(**args) is False
    for doc in api.resources.values():
        if doc['kind'] == 'ValidatingAdmissionPolicy':
            doc['metadata']['generation'] = 2
            doc['status'] = {'observedGeneration': 2, 'typeChecking': {'expressionWarnings': []}}
    assert application_setup_ready(**args) is True
    policy = next(doc for doc in api.resources.values() if doc['kind'] == 'ValidatingAdmissionPolicy')
    policy['status']['observedGeneration'] = 1
    assert application_setup_ready(**args) is False
    policy['status']['typeChecking']['expressionWarnings'] = [{'warning': 'invalid'}]
    with pytest.raises(ManagementStageError):
        application_setup_ready(**args)


@pytest.mark.parametrize('damage', ['failed', 'uid', 'command'])
def test_setup_job_readiness_cannot_hide_failure_or_drift(setup_request, tmp_path, damage):
    from scripts.ops.nebius_application_setup import (
        application_setup_ready,
        stage_application_setup,
    )
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, api = setup_request
    args = dict(request=request, phase='database', api=api, state_dir=tmp_path / 'state')
    stage_application_setup(**args)
    assert application_setup_ready(**args) is False
    job = next(doc for doc in api.resources.values() if doc['kind'] == 'Job')
    job['status'] = {'conditions': [{'type': 'Complete', 'status': 'True'}], 'succeeded': 1}
    assert application_setup_ready(**args) is True
    if damage == 'failed':
        job['status'] = {'conditions': [{'type': 'Failed', 'status': 'True'}]}
    elif damage == 'uid':
        job['metadata']['uid'] = str(uuid4())
    else:
        job['spec']['template']['spec']['containers'][0]['command'] = ['arbitrary']
    count = len(api.creates)
    with pytest.raises(ManagementStageError):
        application_setup_ready(**args)
    assert len(api.creates) == count


def test_exact_setup_transport_checks_shared_uid_before_any_write(setup_request, application_management_inputs):
    from scripts.ops.nebius_application_setup import HTTPSApplicationSetupAPI
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, _ = setup_request
    attempts = []
    bindings = {request.binding.namespace: request.binding.namespace_uid, 'kube-system': request.binding.kube_system_uid,
        request.deployment.installation.applications.shared.platform_namespace: str(uuid4())}

    def respond(http):
        attempts.append(http.method)
        name = http.url.path.rsplit('/', 1)[1]
        return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {
            'name': name, 'uid': bindings[name], 'labels': {'loom.nebius/management-installation': request.binding.installation_id,
                'pod-security.kubernetes.io/enforce': 'restricted'}}})

    with HTTPSApplicationSetupAPI(request=request, phase='database',
            api_server=request.deployment.installation.applications.runtime.kubernetes.endpoint,
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(respond), base_url=api.api_server)
        doc = render_setup(application_management_inputs)['database'][0]
        doc['metadata']['annotations'] = {'loom.nebius/management-stage-operation': str(uuid4())}
        with pytest.raises(ManagementStageError, match='shared namespace'):
            api.create_resource(doc)
    assert attempts and set(attempts) == {'GET'}


def test_setup_transport_rejects_changed_sql_before_network(setup_request, application_management_inputs):
    from scripts.ops.nebius_application_setup import HTTPSApplicationSetupAPI
    from scripts.ops.nebius_management_stage import ManagementStageError

    request, _ = setup_request
    attempts = []
    with HTTPSApplicationSetupAPI(request=request, phase='database',
            api_server=request.deployment.installation.applications.runtime.kubernetes.endpoint,
            ssl_context=ssl.create_default_context()) as api:
        api.client.close()
        api.client = httpx.Client(transport=httpx.MockTransport(lambda request: attempts.append(request)))
        doc = render_setup(application_management_inputs)['database'][1]
        doc['metadata'].setdefault('annotations', {})['loom.nebius/management-stage-operation'] = str(uuid4())
        doc['spec']['template']['spec']['containers'][0]['command'] = ['arbitrary-sql']
        with pytest.raises(ManagementStageError, match='scope'):
            api.create_resource(doc)
    assert not attempts
