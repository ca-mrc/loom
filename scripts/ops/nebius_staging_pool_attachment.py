"""Prepare current-source staging catalog rows; never acquire migration authority.

The protected caller must requalify this non-atomic observation, enumerate every
writer and bind forward/recovery history before using the returned predecessor.
No historical rollback template, caller-supplied Pod or republished profile is
accepted as the current staging source.
"""
from __future__ import annotations

import copy
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _retains_intent, _snapshot, _uid
from scripts.ops.nebius_management_stage import _canonical_quantities, _qualified_defaulted
from scripts.ops.nebius_pool_migration import PoolGuardDatabase, PoolGuardTarget

from loom.execution_contract import ExecutionClassV1, ExecutionTopologyV1
from loom.nebius_platform_render import build_platform
from loom.nebius_pool_contract import PoolParticipantV1
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_execution_actuator.config import ExecutionActuatorSettings
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings
from loom_service.pool_management.profiles import PoolProfileCatalog

_GROUP = 'nebius.com/node-group-id'
_KINDS = {'Namespace', 'Deployment', 'StatefulSet', 'CronJob', 'Service', 'ConfigMap',
    'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}


@dataclass(frozen=True, repr=False)
class StagingPoolAttachment:
    participant: PoolParticipantV1
    guard: PoolGuardTarget
    runtime_profile: ServiceExecutionRuntimeProfileV1
    profiles: PoolProfileCatalog
    actuators: tuple[dict[str, Any], ...]
    config: dict[str, Any]
    resources: tuple[dict[str, Any], ...]


def _pod_defaults(path: tuple[str, ...], wanted: dict[str, Any]) -> dict[str, Any]:
    if not path:
        return {'dnsPolicy': 'ClusterFirst', 'restartPolicy': 'Always', 'schedulerName': 'default-scheduler',
            'terminationGracePeriodSeconds': 30, 'enableServiceLinks': True,
            'serviceAccount': wanted.get('serviceAccountName', 'default')}
    if path[0] in {'containers', 'initContainers'}:
        if len(path) == 2:
            return {'imagePullPolicy': 'IfNotPresent', 'terminationMessagePath': '/dev/termination-log',
                'terminationMessagePolicy': 'File', 'resources': {}}
        if path[2:] == ('ports', '*'):
            return {'protocol': 'TCP'}
        if path[2:] == ('volumeMounts', '*'):
            return {'readOnly': False, 'mountPropagation': 'None'}
        if path[2:] == ('resources',):
            return {'limits': {}, 'requests': {}}
        if path[-1] in {'livenessProbe', 'readinessProbe', 'startupProbe'}:
            return {'failureThreshold': 3, 'successThreshold': 1, 'timeoutSeconds': 1,
                'periodSeconds': 10, 'initialDelaySeconds': 0}
        if len(path) == 4 and path[2] in {'livenessProbe', 'readinessProbe', 'startupProbe'} and path[3] == 'httpGet':
            return {'scheme': 'HTTP'}
        if path[-1] in {'secretKeyRef', 'configMapKeyRef'}:
            return {'optional': False}
        if path[-1] == 'fieldRef':
            return {'apiVersion': 'v1'}
    if len(path) == 3 and path[:2] == ('volumes', '*'):
        if path[2] in {'secret', 'configMap'}:
            return {'defaultMode': 420, 'optional': False}
        if path[2] == 'projected':
            return {'defaultMode': 420}
    return {}


def _exact_pod(actual: Any, wanted: Any, path: tuple[str, ...] = ()) -> bool:
    """Closed Pod trees, with narrowly enumerated API-default leaves only."""
    if isinstance(wanted, dict):
        if not isinstance(actual, dict):
            return False
        defaults = _pod_defaults(path, wanted)
        if any(key not in defaults or type(actual[key]) is not type(defaults[key]) or actual[key] != defaults[key]
                for key in actual.keys() - wanted.keys()):
            return False
        return all(_exact_pod(actual[key], value, (*path, key)) if key in actual else value == [] or (
            path[-2:] == ('env', '*') and key == 'value' and value == '' and 'valueFrom' not in actual)
            for key, value in wanted.items())
    if isinstance(wanted, list):
        return isinstance(actual, list) and len(actual) == len(wanted) and all(
            _exact_pod(left, right, (*path, '*')) for left, right in zip(actual, wanted, strict=True))
    return type(actual) is type(wanted) and actual == wanted


def _public_service(desired: dict[str, Any], observed: dict[str, Any]) -> None:
    _snapshot(observed)  # Preserve owner/lifecycle rejection on this branch too.
    if not _retains_intent(observed, desired):
        raise ValueError
    actual, wanted = copy.deepcopy(observed['spec']), desired['spec']
    defaults = {'sessionAffinity': 'None', 'externalTrafficPolicy': 'Cluster',
        'internalTrafficPolicy': 'Cluster', 'allocateLoadBalancerNodePorts': True, 'ipFamilyPolicy': 'SingleStack'}
    for key in actual.keys() - wanted.keys():
        value = actual[key]
        allocated = ((key == 'clusterIP' and isinstance(value, str) and bool(value) and value != 'None')
            or (key == 'clusterIPs' and value == [observed['spec'].get('clusterIP')])
            or (key == 'ipFamilies' and value in (['IPv4'], ['IPv6'])))
        if not allocated and (key not in defaults or type(value) is not type(defaults[key]) or value != defaults[key]):
            raise ValueError
        del actual[key]
    if len(actual['ports']) != len(wanted['ports']):
        raise ValueError
    for port, expected in zip(actual['ports'], wanted['ports'], strict=True):
        if 'nodePort' not in expected and 'nodePort' in port:
            value = port.pop('nodePort')
            if type(value) is not int or not 1 <= value <= 65535:
                raise ValueError
        if 'protocol' not in expected and port.get('protocol') == 'TCP':
            del port['protocol']
    if actual != wanted:
        raise ValueError


def _qualified_workload(desired: dict[str, Any], observed: dict[str, Any]) -> None:
    if desired['kind'] == 'ConfigMap' and observed.get('data') != desired.get('data'):
        raise ValueError
    if desired['kind'] == 'Service' and desired['spec'].get('type', 'ClusterIP') != 'ClusterIP':
        # The existing web LoadBalancer is retained, not a private management
        # Service. Qualify its original exposure instead of changing its type.
        _public_service(desired, observed)
    else:
        _qualified_defaulted(desired, observed)
    if desired['kind'] not in {'Deployment', 'StatefulSet', 'CronJob'}:
        return
    actual, wanted = _canonical_quantities(observed)['spec'], _canonical_quantities(desired)['spec']
    if actual.get('selector') != wanted.get('selector'):
        raise ValueError
    if desired['kind'] == 'CronJob':
        actual, wanted = actual['jobTemplate']['spec'], wanted['jobTemplate']['spec']
    actual, wanted = actual['template']['spec'], wanted['template']['spec']
    if not _exact_pod(actual, wanted):
        raise ValueError


def _runtime(actuator: dict[str, Any], *, node_group_id: str) -> tuple[ExecutionTargetRuntime, NativeTaskImageSettings | None]:
    pod = actuator['spec']['template']['spec']
    container, = pod['containers']
    if container.get('envFrom'):
        raise ValueError
    env = {row['name']: row['value'] for row in container['env'] if 'value' in row}
    prefix = 'LOOM_EXECUTION_ACTUATOR_'
    # Read class defaults, never BaseSettings' ambient process environment.
    defaults = ExecutionActuatorSettings.model_fields
    runtime = ExecutionTargetRuntime(
        target_id=env[prefix + 'TARGET_ID'], namespace=env[prefix + 'NAMESPACE'],
        runtime_class_name=env.get(prefix + 'RUNTIME_CLASS_NAME', defaults['runtime_class_name'].default),
        service_account_name=env.get(prefix + 'SERVICE_ACCOUNT_NAME', defaults['service_account_name'].default),
        pod_identity_audience=env.get(prefix + 'POD_IDENTITY_AUDIENCE', defaults['pod_identity_audience'].default),
        credential_broker_url=env[prefix + 'CREDENTIAL_BROKER_URL'],
        node_selector=json.loads(env[prefix + 'NODE_SELECTOR']),
        tolerations=tuple(json.loads(env[prefix + 'TOLERATIONS'])))
    selector = dict(runtime.node_selector or {})
    if selector.get(_GROUP, node_group_id) != node_group_id or runtime.runtime_class_name is not None:
        # RuntimeClass overhead would need authenticated qualification; do not
        # silently charge zero. The current standalone renderer uses none.
        raise ValueError
    # Preserve current label constraints while narrowing the shared gateway to
    # its one registered physical group. Live provider/label census is separate.
    runtime = replace(runtime, node_selector={**selector, _GROUP: node_group_id})
    raw = env.get(prefix + 'TASK_IMAGE_BUILDER')
    return runtime, NativeTaskImageSettings.model_validate_json(raw) if raw is not None else None


def _prepare(*, participant: PoolParticipantV1, observation: dict[str, Any], candidate: dict[str, Any],
        cluster_id: str, node_group_id: str, repo_root: Path) -> StagingPoolAttachment:
    if (participant.environment_class != 'staging'
            or observation['schema_version'] != 'loom.nebius-pool-resource-observation.v1'
            or observation['cluster_id'] != cluster_id or observation['candidate_sha'] != candidate['candidate_sha']
            or candidate.get('repository') != 'ca-mrc/loom' or candidate.get('source_ref') != 'refs/heads/dev'):
        raise ValueError
    resources = copy.deepcopy(observation['resources'])
    if not isinstance(resources, list) or not 0 < len(resources) <= 32768:
        raise ValueError
    indexed: dict[str, dict[str, Any]] = {}
    uids: set[str] = set()
    for row in resources:
        key, uid = _key(row), _uid(row)
        metadata = row['metadata']
        if (row['kind'] not in _KINDS or key in indexed or uid in uids
                or not isinstance(metadata.get('resourceVersion'), str) or not metadata['resourceVersion']
                or metadata.get('deletionTimestamp')):
            raise ValueError
        indexed[key] = row
        uids.add(uid)
    namespace = observation['namespace']

    def get(kind: str, name: str, ns: str | None = namespace) -> dict[str, Any]:
        return indexed[f'{kind}:{ns or "-"}:{name}']

    data = get('ConfigMap', 'loom-platform-config')['data']
    config, profile, keyring = (json.loads(data[key]) for key in ('environment.json', 'profile.json', 'keyring.json'))
    runtime_profile = ServiceExecutionRuntimeProfileV1.model_validate(profile)
    if (config['namespace'] != namespace or config['cluster_id'] != cluster_id
            or config['execution_node_group_id'] != node_group_id
            or config['execution_namespace'] != participant.execution_namespace.name
            or participant.build_namespace.name != config['execution_namespace'] + '-build'):
        raise ValueError
    for namespace_binding in (participant.execution_namespace, participant.build_namespace):
        if _uid(get('Namespace', namespace_binding.name, None)) != str(namespace_binding.uid):
            raise ValueError
    rendered = build_platform(config, candidate, profile, keyring, repo_root=repo_root)
    expected = {_key(row): row for group in rendered.values() for row in group if row['kind'] in _KINDS}
    for key, desired in expected.items():
        _qualified_workload(desired, indexed[key])
    # ConfigMap data is executable configuration. Superset matching is insufficient.
    if data != expected[f'ConfigMap:{namespace}:loom-platform-config']['data']:
        raise ValueError
    execution, task_images, actuators, target_ids = [], [], [], set()
    for field in ('catalog.json', 'guest-catalog.json', 'emulated-auth-catalog.json'):
        if field not in data:
            continue
        catalog = json.loads(data[field])
        execution_class = ExecutionClassV1.model_validate(catalog['execution_class'])
        target, = ExecutionTopologyV1.model_validate(catalog['topology']).targets
        if (target.environment != participant.catalog_environment
                or target.namespace_name != participant.execution_namespace.name
                or target.execution_class_id != execution_class.class_id):
            raise ValueError
        binding = participant.target(target.target_id, 'trial')
        ordinary = field == 'catalog.json'
        kinds = {'trial', 'verifier', 'task_image_build'} if ordinary else {'trial', 'verifier'}
        if set(binding.workload_kinds) != kinds:
            raise ValueError
        name = 'loom-execution-actuator' if ordinary else target.target_id + '-actuator'
        actuator = get('Deployment', name, participant.execution_namespace.name)
        runtime, builder = _runtime(actuator, node_group_id=node_group_id)
        if runtime.target_id != target.target_id or runtime.namespace != target.namespace_name:
            raise ValueError
        native_labels = {'loom.nebius/node-os': 'linux',
            'loom.nebius/node-arch': {'x86_64': 'amd64', 'arm64': 'arm64'}[execution_class.cpu_architecture]}
        selector = runtime.node_selector or {}
        if any(selector.get(key, value) != value for key, value in native_labels.items()):
            raise ValueError
        runtime = replace(runtime, node_selector={**selector, **native_labels})
        execution.append({'profile_id': str(binding.profile_id), 'runtime': asdict(runtime),
            'candidate_sha': runtime_profile.candidate_sha, 'execution_class_id': execution_class.class_id,
            'runtime_image_ref': runtime_profile.runtime_image_ref,
            'runtime_binary_sha256': runtime_profile.runtime_binary_sha256,
            'execution_class': execution_class.model_dump(mode='json')})
        if ordinary:
            if builder is None or builder.namespace != participant.build_namespace.name:
                raise ValueError
            # The native renderer supplies the same provider-owned architecture
            # labels retained above; global accounting freezes its final selector.
            task_images.append({'profile_id': str(binding.profile_id), 'cpu_arch': execution_class.cpu_architecture,
                'target': asdict(replace(runtime, namespace=builder.namespace)),
                'settings': builder.model_dump(mode='json')})
        elif builder is not None:
            raise ValueError
        actuators.append(actuator)
        target_ids.add(target.target_id)
    if target_ids != {row.target_id for row in participant.targets}:
        raise ValueError
    profiles = PoolProfileCatalog.model_validate({'schema_version': 'loom.pool-profiles.v1',
        'image_admission_keyring': keyring, 'execution': execution, 'task_images': task_images})
    profiles.profiles()

    def credential(key: str) -> tuple[UUID, str]:
        raw = observation[key]
        uid, version = UUID(raw['uid']), raw['resource_version']
        if not uid.int or not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        return uid, version

    database_uid, database_version = credential('database_credential')
    actuator_uid, actuator_version = credential('actuator_credential')
    guard = PoolGuardTarget(participant_id=participant.participant_id, namespace=namespace,
        namespace_uid=UUID(_uid(get('Namespace', namespace, None))), controller=get('Deployment', 'loom-control-plane'),
        database=PoolGuardDatabase(statefulset=get('StatefulSet', 'loom-postgres'), service=get('Service', 'loom-postgres'),
            credential_uid=database_uid, credential_resource_version=database_version,
            actuator_credential_uid=actuator_uid, actuator_credential_resource_version=actuator_version))
    return StagingPoolAttachment(participant.model_copy(deep=True), guard, runtime_profile, profiles,
        tuple(actuators), config, tuple(resources))


def prepare_staging_pool_attachment(*, participant: PoolParticipantV1, observation: dict[str, Any], candidate: dict[str, Any],
        cluster_id: str, node_group_id: str, repo_root: Path) -> StagingPoolAttachment:
    """Derive preparation only; this snapshot cannot establish sole-writer safety."""
    try:
        return _prepare(participant=participant, observation=observation, candidate=candidate,
            cluster_id=cluster_id, node_group_id=node_group_id, repo_root=repo_root)
    except Exception:
        raise ValueError('staging pool attachment unqualified') from None
