"""Fixed protected prerequisites; no credentials, database copies or live writes."""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from loom.nebius_application_authority import (
    APPLICATION_INSTALLATION_LABEL,
    render_application_authority,
    render_application_shared_observer,
)
from loom.nebius_application_network import render_application_shared_access
from loom.nebius_platform_render import _env, _obj, _secret_env, canonical
from loom_service.environment_management.deployment import ManagementDeployment, render_management


def render_application_setup(deployment: ManagementDeployment, *, candidate: dict[str, Any],
                             profile: dict[str, Any], repo_root: Path) -> dict[str, list[dict[str, Any]]]:
    deployment = ManagementDeployment.model_validate(deployment.model_dump())
    application = deployment.installation.applications
    if application is None:
        raise ValueError('application setup requires protected application configuration')
    rendered = render_management(deployment, candidate=candidate, profile=profile, repo_root=repo_root)
    authority, shared = application.authority, application.shared
    documents = render_application_authority(authority)
    # Grant no bootstrap access until the protected caller has observed current
    # admission policies. Actual-subject probes precede process activation too.
    admission = [doc for doc in documents if doc['kind'] != 'ClusterRoleBinding']
    permissions = [doc for doc in documents if doc['kind'] == 'ClusterRoleBinding']
    permissions.extend(render_application_shared_observer(authority))
    network = render_application_shared_access(authority, shared, deployment.installation.foundation)
    network.append(_obj('NetworkPolicy', authority.name + '-manager-postgres', shared.platform_namespace, {
        'podSelector': {'matchLabels': {'app': 'loom-postgres'}}, 'policyTypes': ['Ingress'],
        'ingress': [{'from': [{'namespaceSelector': {'matchLabels': {
            'kubernetes.io/metadata.name': deployment.namespace,
            'loom.nebius/management-installation': str(deployment.installation_id)}},
            'podSelector': {'matchLabels': {'app': 'loom-service'}}}],
            'ports': [{'protocol': 'TCP', 'port': 5432}]}],
    }, api='networking.k8s.io/v1'))
    suffix = rendered.revision[7:19]
    config = _obj('ConfigMap', 'loom-applications-setup-' + suffix, shared.platform_namespace)
    config.update(immutable=True, data={'setup.json': canonical({
        'namespace': shared.platform_namespace, 'data_environment_id': str(shared.data_environment_id),
        'schema_revision': shared.schema_revision}).decode()})
    # Reuse the qualified image, requests, restricted security and CA-only mount
    # of the management migration template. Its namespace now selects the shared
    # database Secret; this command does not execute management/business migrations.
    job = copy.deepcopy(rendered.files['30-migrate.yaml'][0])
    job['metadata'].update(name='loom-applications-setup-' + suffix, namespace=shared.platform_namespace)
    pod = job['spec']['template']['spec']
    container = pod['containers'][0]
    container['command'] = ['python', '-m', 'loom.nebius_application_database_install']
    container['env'] = [
        *_env({'LOOM_APPLICATION_SETUP_CONFIG': '/var/run/loom-application-setup/setup.json'}),
        _secret_env('LOOM_DB_URL', 'loom-platform-db', 'admin-url'),
        _secret_env('LOOM_APPLICATION_MANAGER_PASSWORD', 'loom-applications-manager-' + suffix, 'password'),
    ]
    pod['volumes'] = [volume for volume in pod['volumes'] if volume['name'] == 'db-ca'] + [{
        'name': 'application-setup', 'configMap': {'name': config['metadata']['name'],
            'items': [{'key': 'setup.json', 'path': 'setup.json'}]},
    }]
    container['volumeMounts'] = [mount for mount in container['volumeMounts'] if mount['name'] == 'db-ca'] + [{
        'name': 'application-setup', 'mountPath': '/var/run/loom-application-setup', 'readOnly': True,
    }]
    retirement_name = authority.name + '-legacy-pods'
    retirement = [
        {'apiVersion': 'admissionregistration.k8s.io/v1', 'kind': 'ValidatingAdmissionPolicy',
            'metadata': {'name': retirement_name}, 'spec': {'failurePolicy': 'Fail',
                'matchConstraints': {'resourceRules': [{'operations': ['CREATE'], 'apiGroups': [''],
                    'apiVersions': ['v1'], 'resources': ['pods']}]},
                'validations': [{'expression': f"request.namespace != '{deployment.namespace}' || "
                    "!has(object.spec.serviceAccountName) || object.spec.serviceAccountName != 'loom-management-provisioner'",
                    'message': 'legacy management process is retired', 'reason': 'Forbidden'}]}},
        {'apiVersion': 'admissionregistration.k8s.io/v1', 'kind': 'ValidatingAdmissionPolicyBinding',
            'metadata': {'name': retirement_name}, 'spec': {'policyName': retirement_name, 'validationActions': ['Deny']}},
    ]
    phases = {'admission': admission, 'permissions': permissions, 'network': network, 'database': [config, job],
        'retirement': retirement}
    for phase in phases.values():
        for doc in phase:
            doc['metadata'].setdefault('labels', {})[APPLICATION_INSTALLATION_LABEL] = str(deployment.installation_id)
    return phases
