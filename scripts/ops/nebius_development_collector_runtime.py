"""Fresh suspended dev pool observer; no legacy identity or cloud writer reuse.

The connected parent must deliver and qualify the fixed collector-only cloud
Secret before staging/start. A rendered Secret reference is not live authority.
"""
from __future__ import annotations

import base64
import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from scripts.ops.nebius_development_collector_cloud import (
    DevelopmentCollectorCloudScope,
    collector_public_key,
)
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime
from scripts.ops.nebius_development_shared_runtime import prepare_shared_runtime
from scripts.ops.nebius_pool_runtime import _RetainedPoolCollectorSettings

from loom.nebius_platform_render import _obj, _replace_tree

_ROOT = Path(__file__).resolve().parents[2]
_PREFIX = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_'


@dataclass(frozen=True, repr=False)
class DevelopmentCollectorRuntime:
    configuration: dict[str, Any]
    cronjob: dict[str, Any]
    authority: tuple[dict[str, Any], ...]
    credential_secret_name: str


def prepare_collector_material(request: DevelopmentDatabaseRuntime, *, scope: DevelopmentCollectorCloudScope,
                               credential: bytes) -> dict[str, Any]:
    """Project one private observer Secret, not an assertion of IAM readiness.

    The protected parent must qualify_collector_cloud before staging and probe
    the actual pool with this credential before starting the observer. No
    operator, storage, registry or database material is accepted here.
    """
    try:
        runtime = prepare_collector_runtime(request)
        collector_public_key(scope=scope, config=request.foundation.inputs.config, credential=credential)
        metadata = copy.deepcopy(runtime.configuration['metadata'])
        metadata['name'] = runtime.credential_secret_name
        return {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque', 'immutable': True,
            'metadata': metadata, 'data': {'credentials.json': base64.b64encode(credential).decode()}}
    except Exception:
        raise ValueError('development collector material unqualified') from None


def prepare_collector_runtime(request: DevelopmentDatabaseRuntime) -> DevelopmentCollectorRuntime:
    """Derive observer-only configuration from the immutable physical pool scope."""
    try:
        prepare_shared_runtime(request)
        spec = request.manager.retained.request.registration.spec
        config = request.foundation.inputs.config
        participant, = spec.participants
        observer, = (row for row in spec.machines if row.role == 'observer')
        scopes = {tuple(identity[:3]) for identity in spec.quota_identities.values()}
        if len(scopes) != 1:
            raise ValueError
        parent, region, service = scopes.pop()
        if (parent != config['quota_parent_id'] or region != config['region']
                or spec.node_group_id != config['execution_node_group_id']):
            raise ValueError
        cloud_file = '/var/run/loom-owned/credentials/nebius-credentials.json'
        observer_file = '/var/run/loom-owned/credentials/control-plane-token'
        values: dict[str, Any] = {'pool_id': spec.pool_id,
            'management_url': 'https://' + request.manager.deployment.public_host,
            'management_bearer_token_file': observer_file, 'nebius_project_id': config['project_id'],
            'nebius_quota_parent_id': parent, 'nebius_node_group_id': spec.node_group_id,
            'nebius_region': region, 'nebius_credentials_file': cloud_file, 'quota_service': service}
        for resource, identity in spec.quota_identities.items():
            values['quota_' + resource + '_name'] = identity[3]
            values['quota_' + resource + '_unit'] = identity[4]
        settings = _RetainedPoolCollectorSettings(_env_file=None, **values)
        namespace, name = participant.execution_namespace.name, 'loom-execution-capacity-collector'
        config_name = 'loom-pool-collector-' + spec.operation_id.hex
        secret_name = 'loom-dev-collector-' + request.operation_id.hex
        configuration = _obj('ConfigMap', config_name, namespace)
        configuration['immutable'] = True
        configuration['data'] = {_PREFIX + key.upper(): str(value)
            for key, value in settings.model_dump(mode='json').items()
            if value is not None and key not in {'nebius_credentials_file', 'management_bearer_token_file'}}
        configuration['data'][_PREFIX + 'COLLECTION_MODE'] = 'pool'

        cronjob, = (row for row in yaml.safe_load_all((_ROOT / 'deploy/k8s/nebius-capacity-collector.yaml').read_text())
            if row and row['kind'] == 'CronJob' and row['metadata']['name'] == name)
        cronjob = _replace_tree(cronjob, {'loom-nebius-development': namespace})
        cronjob['spec']['suspend'] = True
        cronjob['spec']['jobTemplate']['spec']['backoffLimit'] = 0
        pod = cronjob['spec']['jobTemplate']['spec']['template']['spec']
        pod['nodeSelector'] = {'loom.nebius/node-role': 'system', 'loom.nebius/platform': 'integration'}
        pod['tolerations'] = [{'key': 'loom.nebius/platform', 'operator': 'Equal', 'value': 'integration', 'effect': 'NoSchedule'}]
        container, = pod['containers']
        initializer, = pod['initContainers']
        image = request.manager.publication.bundle.candidate['images']['execution_actuator']['image_ref']
        container['image'] = initializer['image'] = image
        container['envFrom'] = [{'configMapRef': {'name': config_name}}]
        container['env'] = [{'name': _PREFIX + key, 'value': value} for key, value in (
            ('NEBIUS_CREDENTIALS_FILE', cloud_file), ('MANAGEMENT_BEARER_TOKEN_FILE', observer_file))]
        projected, = (row for row in pod['volumes'] if row['name'] == 'projected-credentials')
        projected['projected']['sources'] = [
            {'secret': {'name': secret_name, 'items': [{'key': 'credentials.json', 'path': 'nebius-credentials.json'}]}},
            {'secret': {'name': 'loom-pool-machine-' + observer.machine_id.hex,
                'items': [{'key': 'token', 'path': 'control-plane-token'}]}}]

        account = _obj('ServiceAccount', name, namespace)
        account['automountServiceAccountToken'] = True
        role_name = 'loom-pool-collector-' + spec.pool_id.hex
        role = _obj('ClusterRole', role_name, None, api='rbac.authorization.k8s.io/v1')
        role['rules'] = [{'apiGroups': [''], 'resources': ['nodes', 'pods'], 'verbs': ['get', 'list']},
            {'apiGroups': ['apps'], 'resources': ['daemonsets'], 'verbs': ['get', 'list']}]
        binding = _obj('ClusterRoleBinding', role_name, None, api='rbac.authorization.k8s.io/v1')
        binding['subjects'] = [{'kind': 'ServiceAccount', 'name': name, 'namespace': namespace}]
        binding['roleRef'] = {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': role_name}
        authority = (account, role, binding)
        for document in (configuration, cronjob, *authority):
            document['metadata'].setdefault('labels', {}).update({
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(request.operation_id)})
        return DevelopmentCollectorRuntime(configuration, cronjob, authority, secret_name)
    except Exception:
        raise ValueError('development collector runtime unqualified') from None
