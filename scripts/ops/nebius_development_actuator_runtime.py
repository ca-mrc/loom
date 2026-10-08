"""Fresh stopped actuator and fixed read-only authority for independent dev.

The connected runtime parent owns create journaling, live identity qualification,
build isolation, kubelet trust and activation. These documents grant no Job writes.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime
from scripts.ops.nebius_development_shared_runtime import prepare_shared_runtime
from scripts.ops.nebius_pool_runtime import _environment

from loom.nebius_platform_render import _mount_secret, _obj, _replace_tree, _secret_env
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom_service.pool_management.installation_render import mount_machine_token

_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, repr=False)
class DevelopmentActuatorRuntime:
    deployment: dict[str, Any]
    authority: tuple[dict[str, Any], ...]


def prepare_actuator_runtime(request: DevelopmentDatabaseRuntime) -> DevelopmentActuatorRuntime:
    """Render a new resource, not a successor with invented legacy provenance."""
    try:
        prepare_shared_runtime(request)
        manager = request.manager
        spec = manager.retained.request.registration.spec
        participant, = spec.participants
        machine, = (row for row in spec.machines
            if row.participant_id == participant.participant_id and row.workload_scope == 'environment')
        target_id = request.foundation.inputs.config['target_id']
        target = participant.target(target_id, 'trial')
        execution, = (row for row in spec.profiles.execution if row.profile_id == target.profile_id)
        build_target = participant.target(target_id, 'task_image_build')
        build, = (row for row in spec.profiles.task_images if row.profile_id == build_target.profile_id)
        # One ordinary actuator is the initial fixed roster. Guest/regional
        # controllers need their own explicit inventory before being supported.
        if any(row.target_id != target_id and set(row.workload_kinds) != {'application_image_build'}
                for row in participant.targets):
            raise ValueError
        namespace = participant.execution_namespace.name
        name = 'loom-execution-actuator'
        deployment, = (doc for doc in yaml.safe_load_all((_ROOT / 'deploy/k8s/nebius-execution-actuator.yaml').read_text())
            if doc and doc['kind'] == 'Deployment' and doc['metadata']['name'] == name)
        deployment = _replace_tree(deployment, {'loom-nebius-development': namespace})
        deployment['spec'].update(replicas=0, strategy={'type': 'Recreate'})
        pod = deployment['spec']['template']['spec']
        pod['securityContext'].update(runAsUser=65532, runAsGroup=65532, fsGroup=65532)
        pod['nodeSelector'] = {'loom.nebius/node-role': 'system', 'loom.nebius/platform': 'integration'}
        pod['tolerations'] = [{'key': 'loom.nebius/platform', 'operator': 'Equal', 'value': 'integration', 'effect': 'NoSchedule'}]
        container, = pod['containers']
        images = manager.publication.bundle.candidate['images']
        container['image'] = images['execution_actuator']['image_ref']
        settings = _environment(container)
        db_secret = request.material[1]['metadata']['name']
        settings['LOOM_EXECUTION_ACTUATOR_DB_URL'].clear()
        settings['LOOM_EXECUTION_ACTUATOR_DB_URL'].update(_secret_env('LOOM_EXECUTION_ACTUATOR_DB_URL', db_secret, 'actuator-url'))
        runtime = execution.runtime
        for key, value in {'TARGET_ID': target_id, 'NAMESPACE': namespace,
                'NODE_SELECTOR': json.dumps(runtime.node_selector or {}),
                'TOLERATIONS': json.dumps(runtime.tolerations),
                'CREDENTIAL_BROKER_URL': runtime.credential_broker_url}.items():
            settings['LOOM_EXECUTION_ACTUATOR_' + key]['value'] = value
        service_image = images['service']['image_ref']
        token = mount_machine_token(pod, machine_id=machine.machine_id, service_image=service_image, default_uid=65532)
        pool = PoolRuntimeSettings(participant=participant, environment='development',
            logical_pool_id=build.settings.pool_id, management_origin='https://' + manager.deployment.public_host,
            bearer_token_file=Path(token))
        extra = {'GLOBAL_POOL': pool.model_dump_json(), 'TASK_IMAGE_BUILDER': build.settings.model_dump_json(),
            'EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON': json.dumps(spec.profiles.image_admission_keyring),
            'SERVICE_ACCOUNT_NAME': runtime.service_account_name}
        if runtime.runtime_class_name is not None:
            extra['RUNTIME_CLASS_NAME'] = runtime.runtime_class_name
        if runtime.pod_identity_audience is not None:
            extra['POD_IDENTITY_AUDIENCE'] = runtime.pod_identity_audience
        container['env'].extend({'name': 'LOOM_EXECUTION_ACTUATOR_' + key, 'value': value} for key, value in extra.items())
        _mount_secret(pod, 'db-ca', db_secret, '/var/run/loom-db', ca_only=True)

        authority = [_obj('ServiceAccount', name, namespace),
            _obj('ServiceAccount', runtime.service_account_name, namespace)]
        authority[0]['automountServiceAccountToken'] = True
        authority[1]['automountServiceAccountToken'] = False
        if runtime.service_account_name != 'loom-execution-attempt':
            raise ValueError
        subject = {'kind': 'ServiceAccount', 'name': name, 'namespace': namespace}
        for scope, role_name, rules in (
            (namespace, name, [{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
                {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
                {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']}]),
            (participant.build_namespace.name, 'loom-task-image-builder', [
                {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
                {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
                {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']}]),
            (None, 'loom-pool-reader-' + participant.participant_id.hex, [
                {'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
                    'resourceNames': [namespace, participant.build_namespace.name]},
                {'apiGroups': [''], 'resources': ['nodes', 'nodes/stats'], 'verbs': ['get']}]),
        ):
            kind = 'ClusterRole' if scope is None else 'Role'
            role = _obj(kind, role_name, scope, api='rbac.authorization.k8s.io/v1')
            role['rules'] = rules
            binding = _obj(kind + 'Binding', role_name, scope, api='rbac.authorization.k8s.io/v1')
            binding.update(subjects=[subject.copy()], roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': kind, 'name': role_name})
            authority.extend((role, binding))
        for document in (deployment, *authority):
            document['metadata'].setdefault('labels', {}).update({
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(request.operation_id)})
        return DevelopmentActuatorRuntime(deployment, tuple(authority))
    except Exception:
        raise ValueError('development actuator runtime unqualified') from None
