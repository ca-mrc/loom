"""Pure independent participant runtime projection, closed until qualified.

The protected installer owns publication, live UID/ledger qualification, private
material, durable phase transitions and activation. This projection neither
relaxes namespace isolation/quotas nor starts processes or replaces a peer.
"""
from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_build_policy import build_policy_documents
from scripts.ops.nebius_pool_runtime import _environment, wire_scoped_participant

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2, render_foundation
from loom.nebius_platform_render import (
    _execution_documents,
    _network_policy,
    _obj,
    _task_image_builder_documents,
)
from loom.nebius_pool_contract import PoolParticipantV1
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation import PoolMachineInstallation
from loom_service.pool_management.profiles import PoolProfileCatalog, _ApplicationImage, _TaskImage


@dataclass(frozen=True)
class EnvironmentRuntimeV2:
    workloads: dict[str, dict[str, Any]]
    readers: tuple[dict[str, Any], ...]
    gateway_authority: tuple[dict[str, Any], ...]
    isolation: tuple[dict[str, Any], ...]


def _role_pair(*, namespace: str | None, name: str, rules: list[dict[str, Any]],
        subject: dict[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    kind = 'ClusterRole' if namespace is None else 'Role'
    role = _obj(kind, name, namespace, api='rbac.authorization.k8s.io/v1')
    role['rules'] = rules
    binding = _obj(kind + 'Binding', name, namespace, api=role['apiVersion'])
    binding.update(subjects=[subject.copy()], roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': kind, 'name': name})
    return role, binding


def render_environment_runtime(*, declaration: EnvironmentFoundationDeclarationV2,
        participant: PoolParticipantV1, profiles: PoolProfileCatalog, machine: PoolMachineInstallation,
        candidate: dict[str, Any], runtime_profile: ServiceExecutionRuntimeProfileV1,
        management_origin: str, management_namespace: str, repo_root: Path) -> EnvironmentRuntimeV2:
    """Use explicit data/participant identity, never a class-selected peer release.

    Resource manifests carry no invented installed UIDs. Native build policies
    must be qualified before any build namespace relaxation. The platform's
    gateway stays running; additive participant-specific grants do not rewrite
    its whole roster, credentials, workload or another participant's grants.
    """
    try:
        declaration = EnvironmentFoundationDeclarationV2.model_validate(declaration.model_dump())
        participant = PoolParticipantV1.model_validate(participant.model_dump())
        profiles = PoolProfileCatalog.model_validate(profiles.model_dump())
        row, config = declaration.registration, declaration.platform_config
        if (any(getattr(row, key) != getattr(participant, key) for key in (
                'installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation'))
                or row.kind != participant.environment_class or participant.legacy_catalog_environment is not None
                or participant.execution_namespace.name != row.execution_namespace
                or participant.build_namespace.name != row.build_namespace
                or re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', management_namespace) is None
                or management_namespace in {*row.namespaces, 'default', 'kube-system'}):
            raise ValueError
        resolved = profiles.profiles()
        for target in participant.targets:
            for kind in target.workload_kinds:
                if kind in {'trial', 'verifier'}:
                    runtime, namespace = resolved.execution[target.profile_id].runtime, row.execution_namespace
                elif kind == 'task_image_build':
                    runtime, namespace = resolved.task_images[target.profile_id].target, row.build_namespace
                else:
                    if row.kind != 'development':
                        raise ValueError
                    runtime, namespace = resolved.application_images[target.profile_id].target, row.build_namespace
                if (runtime.target_id != target.target_id or runtime.namespace != namespace
                        or (runtime.node_selector or {}).get('nebius.com/node-group-id') != row.physical_pool_id):
                    raise ValueError
        builds: list[tuple[_TaskImage | _ApplicationImage, str]] = [(profile, 'artifacts') for profile in profiles.task_images]
        builds += [(profile, 'source') for profile in profiles.application_images]
        for profile, bucket in builds:
            build_settings = profile.settings
            if (build_settings.namespace != row.build_namespace or build_settings.source_bucket != config['buckets'][bucket]
                    or build_settings.storage_endpoint != config['storage_endpoint'] or build_settings.storage_region != config['region']
                    or build_settings.service_image != candidate['images']['service']['image_ref']
                    or build_settings.pool_id != runtime_profile.logical_pool_id
                    or build_settings.registry_auth_kind != 'nebius'
                    or (build_settings.cache_bucket is not None and build_settings.cache_bucket in config['buckets'].values())):
                raise ValueError
        foundation = render_foundation(declaration, candidate, profile=runtime_profile.model_dump(mode='json'),
            keyring=profiles.image_admission_keyring, repo_root=repo_root)
        applications = {doc['metadata']['name']: doc for doc in foundation.files['40-services.yaml'] if doc['kind'] == 'Deployment'}
        images = {key: value['image_ref'] for key, value in candidate['images'].items()}
        execution = _execution_documents(config, images, repo_root)
        actuators = [doc for doc in execution if doc['kind'] == 'Deployment']
        primary, = [doc for doc in actuators if doc['metadata']['name'] == 'loom-execution-actuator']
        build_target = participant.target(row.target_id, 'task_image_build')
        build = resolved.task_images[build_target.profile_id]
        for actuator in actuators:
            pod = actuator['spec']['template']['spec']
            pod['nodeSelector'], pod['tolerations'] = copy.deepcopy(declaration.node_selector), copy.deepcopy(list(declaration.tolerations))
            container, = pod['containers']
            settings = _environment(container)
            target = participant.target(settings['LOOM_EXECUTION_ACTUATOR_TARGET_ID']['value'], 'trial')
            runtime = resolved.execution[target.profile_id].runtime
            if runtime.credential_broker_url != f'http://loom-llm-gateway.{row.application_namespace}.svc.cluster.local:9100/internal/service-execution':
                raise ValueError
            for key, value in {'NODE_SELECTOR': json.dumps(runtime.node_selector),
                    'TOLERATIONS': json.dumps(runtime.tolerations), 'CREDENTIAL_BROKER_URL': runtime.credential_broker_url}.items():
                settings['LOOM_EXECUTION_ACTUATOR_' + key]['value'] = value
            for key, optional_value in {'SERVICE_ACCOUNT_NAME': runtime.service_account_name,
                    'RUNTIME_CLASS_NAME': runtime.runtime_class_name, 'POD_IDENTITY_AUDIENCE': runtime.pod_identity_audience}.items():
                if optional_value is not None:
                    container['env'].append({'name': 'LOOM_EXECUTION_ACTUATOR_' + key, 'value': optional_value})
            if actuator is primary:
                container['env'].append({'name': 'LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER', 'value': build.settings.model_dump_json()})
        workloads = wire_scoped_participant(participant=participant, profiles=profiles, machine=machine,
            candidate=candidate, application_namespace=row.application_namespace, management_origin=management_origin,
            controller=applications['loom-control-plane'], service=applications['loom-service'], actuator=primary,
            guest_actuators=tuple(doc for doc in actuators if doc is not primary), runtime_profile=runtime_profile)
        _environment(workloads['control_plane']['spec']['template']['spec']['containers'][0])[
            'LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED']['value'] = 'true'
        # The full foundation's separate gateway owns these routes. The selected
        # API receives submission attribution, not a management service role.
        workloads['service']['spec']['template']['spec']['containers'][0]['env'].append(
            {'name': 'LOOM_SVC_SERVICE_MODE', 'value': 'application'})

        readers: list[dict[str, Any]] = []
        accounts = {(row.execution_namespace, 'loom-execution-actuator')}
        accounts.update((row.execution_namespace, profile.runtime.service_account_name) for profile in profiles.execution)
        accounts.update((row.build_namespace, profile.target.service_account_name) for profile, _ in builds)
        for namespace, name in sorted(accounts):
            if name == 'default':
                raise ValueError
            account = _obj('ServiceAccount', name, namespace)
            account['automountServiceAccountToken'] = namespace == row.execution_namespace and name == 'loom-execution-actuator'
            readers.append(account)
        subject = {'kind': 'ServiceAccount', 'name': 'loom-execution-actuator', 'namespace': row.execution_namespace}
        readers.extend(_role_pair(namespace=None, name='loom-pool-reader-' + participant.participant_id.hex, subject=subject,
            rules=[{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
                'resourceNames': [row.execution_namespace, row.build_namespace]},
                {'apiGroups': [''], 'resources': ['nodes', 'nodes/stats'], 'verbs': ['get']}]))
        for namespace, name in ((row.execution_namespace, 'loom-execution-actuator'), (row.build_namespace, 'loom-task-image-builder')):
            readers.extend(_role_pair(namespace=namespace, name=name, subject=subject,
                rules=[{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
                    {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
                    {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']}]))
        subject = {'kind': 'ServiceAccount', 'name': 'loom-pool-gateway', 'namespace': management_namespace}
        name = 'loom-pool-participant-' + participant.participant_id.hex
        gateway = list(_role_pair(namespace=None, name=name, subject=subject,
            rules=[{'apiGroups': [''], 'resources': ['namespaces'], 'verbs': ['get'],
                'resourceNames': [row.execution_namespace, row.build_namespace]}]))
        for namespace in (row.execution_namespace, row.build_namespace):
            gateway.extend(_role_pair(namespace=namespace, name=name, subject=subject,
                rules=[{'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get', 'create', 'delete']},
                    {'apiGroups': [''], 'resources': ['configmaps'], 'verbs': ['get', 'create', 'delete']},
                    {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list', 'delete']}]))
        isolation = [doc for doc in execution if doc['kind'] == 'NetworkPolicy']
        isolation.extend(doc for doc in _task_image_builder_documents(config, build.settings) if doc['kind'] == 'NetworkPolicy')
        isolation.extend(build_policy_documents(row.build_namespace, profiles))
        # The closed foundation denies all execution egress. Grant only reader
        # processes DNS, their own DB and authenticated HTTPS/Kubelet traffic;
        # task Pods retain the separate DNS/broker-only execution policy.
        for actuator in actuators:
            isolation.append(_network_policy('pool-reader-' + actuator['metadata']['name'], row.execution_namespace,
                {'matchLabels': {'app.kubernetes.io/name': actuator['metadata']['name']}}, [], [
                    {'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'kube-system'}},
                        'podSelector': {'matchExpressions': [{'key': 'k8s-app', 'operator': 'In', 'values': ['kube-dns', 'coredns']}]}}],
                        'ports': [{'protocol': protocol, 'port': 53} for protocol in ('UDP', 'TCP')]},
                    {'to': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': row.application_namespace}},
                        'podSelector': {'matchLabels': {'app': 'loom-postgres'}}}], 'ports': [{'protocol': 'TCP', 'port': 5432}]},
                    {'ports': [{'protocol': 'TCP', 'port': 443}]},
                    {'to': [{'ipBlock': {'cidr': network}} for network in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16', 'fc00::/7')],
                        'ports': [{'protocol': 'TCP', 'port': 10250}]},
                ]))
        for document in (*workloads.values(), *readers, *gateway, *isolation):
            document['metadata'].setdefault('labels', {}).update({'loom.nebius/environment-id': str(row.environment_id),
                'loom.nebius/incarnation': str(row.incarnation), 'loom.nebius/participant-id': str(row.participant_id)})
        return EnvironmentRuntimeV2(workloads, tuple(readers), tuple(gateway), tuple(isolation))
    except Exception:
        raise ValueError('environment_runtime_unqualified') from None
