"""Completed current-source collector execution and its retained publication.

This read-only observation grants no restart/reopening authority. Its caller
must bracket it with recovery receipts, provider and writer qualification. An
enabled CronJob or an old successful Pod alone is not publication health.
"""
from __future__ import annotations

import base64
import copy
import json
from datetime import datetime
from typing import Any

from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _canonical_quantities
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI, _runtime_pod_spec
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_recovery_restart import _process_snapshot
from scripts.ops.nebius_shared_pool_recovery_workloads import MARKER
from scripts.ops.nebius_staging_pool_attachment import _exact_pod

from loom.execution_contract import ExecutionTopologyV1
from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings
from loom_execution_capacity_collector.contracts import CapacityObservationReceipt

Document = dict[str, Any]


def _owned(row: Document, parent: Document) -> bool:
    owners = row['metadata'].get('ownerReferences', [])
    if not any(owner.get('uid') == _uid(parent) for owner in owners):
        return False
    if len(owners) != 1:
        raise ValueError
    owner = dict(owners[0])
    blocking = owner.pop('blockOwnerDeletion', False)
    if (type(blocking) is not bool or owner != {'apiVersion': parent['apiVersion'], 'kind': parent['kind'],
            'name': parent['metadata']['name'], 'uid': _uid(parent), 'controller': True}):
        raise ValueError
    return True


def _pod_spec(actual: Document, wanted: Document, *, scheduled: bool) -> bool:
    actual = _runtime_pod_spec(actual, wanted) if scheduled else copy.deepcopy(actual)
    if scheduled:
        if 'nodeName' not in wanted:
            actual.pop('nodeName', None)
        for key in ('not-ready', 'unreachable'):
            default = {'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
                'effect': 'NoExecute', 'tolerationSeconds': 300}
            if default not in wanted.get('tolerations', []) and default in actual.get('tolerations', []):
                actual['tolerations'].remove(default)
        if actual.get('tolerations') == [] and 'tolerations' not in wanted:
            del actual['tolerations']

    def quantities(spec: Document) -> Document:
        result: Document = _canonical_quantities({'kind': 'Job', 'spec': {'template': {'spec': spec}}})['spec']['template']['spec']
        return result

    return _exact_pod(quantities(actual), quantities(wanted))


def _job_matches(job: Document, current: Document) -> bool:
    actual = copy.deepcopy(job['spec'])
    wanted = current['spec']['jobTemplate']['spec']
    uid, name = _uid(job), job['metadata']['name']
    if actual.pop('selector', None) != {'matchLabels': {'batch.kubernetes.io/controller-uid': uid}}:
        raise ValueError
    defaults = {'completions': 1, 'parallelism': 1, 'completionMode': 'NonIndexed',
        'manualSelector': False, 'suspend': False, 'podReplacementPolicy': 'TerminatingOrFailed'}
    for field in actual.keys() - wanted.keys():
        if field not in defaults or type(actual[field]) is not type(defaults[field]) or actual[field] != defaults[field]:
            raise ValueError
        del actual[field]
    metadata = actual['template']['metadata']
    labels = metadata['labels']
    for prefix in ('', 'batch.kubernetes.io/'):
        for field, expected in (('controller-uid', uid), ('job-name', name)):
            if labels.pop(prefix + field, None) != expected:
                raise ValueError
    if (metadata.get('labels', {}) != wanted['template']['metadata'].get('labels', {})
            or metadata.get('annotations', {}) != wanted['template']['metadata'].get('annotations', {})
            or any(actual.get(field) != value for field, value in wanted.items() if field != 'template')):
        return False
    return _pod_spec(actual['template']['spec'], wanted['template']['spec'], scheduled=False)


def _completed(current: Document, children: Document, pods: Document, *, region: str) -> tuple[Document, Document] | None:
    jobs, related_pods = _process_snapshot(current, children, pods)
    if any(not _owned(job, current) for job in jobs.values()):
        raise ValueError
    if any(not any(_owned(pod, job) for job in jobs.values()) for pod in related_pods.values()):
        raise ValueError
    candidates = []
    for job in jobs.values():
        if job['metadata'].get('deletionTimestamp') or not _job_matches(job, current):
            continue
        status = job.get('status', {})
        conditions = status.get('conditions', [])
        if len({row['type'] for row in conditions}) != len(conditions):
            raise ValueError
        counts = {field: status.get(field, 0) for field in ('active', 'terminating', 'succeeded')}
        if any(type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError
        if (counts != {'active': 0, 'terminating': 0, 'succeeded': 1}
                or not any(row.get('type') == 'Complete' and row.get('status') == 'True' for row in conditions)
                or any(row.get('type') == 'Failed' and row.get('status') == 'True' for row in conditions)):
            continue
        finished = datetime.fromisoformat(status['completionTime'].replace('Z', '+00:00'))
        if finished.tzinfo is None:
            raise ValueError
        candidates.append((finished, _uid(job), job))
    if not candidates:
        return None
    _, _, job = max(candidates, key=lambda item: (item[0], item[1]))
    owned = [pod for pod in related_pods.values() if _owned(pod, job)]
    if any(pod.get('status', {}).get('phase') not in {'Succeeded', 'Failed'} for pod in owned):
        return None
    successful = [pod for pod in owned if pod.get('status', {}).get('phase') == 'Succeeded']
    if len(successful) != 1:
        return None
    pod, = successful
    template = job['spec']['template']
    labels = dict(template['metadata']['labels'])
    region_key = 'topology.kubernetes.io/region'
    if region_key in pod['metadata'].get('labels', {}) and region_key not in labels:
        labels[region_key] = region
    if (pod['metadata'].get('deletionTimestamp')
            or pod['metadata'].get('labels') != labels
            or pod['metadata'].get('annotations', {}) != template['metadata'].get('annotations', {})
            or not _pod_spec(pod['spec'], template['spec'], scheduled=True)):
        raise ValueError
    for field, status_field in (('containers', 'containerStatuses'), ('initContainers', 'initContainerStatuses')):
        names = [row['name'] for row in template['spec'].get(field, [])]
        statuses = pod['status'].get(status_field, [])
        if (sorted(row['name'] for row in statuses) != sorted(names) or len(names) != len(set(names))
                or any(row.get('restartCount') != 0 or set(row.get('state', {})) != {'terminated'}
                    or type(row['state']['terminated'].get('exitCode')) is not int
                    or row['state']['terminated']['exitCode'] != 0 for row in statuses)):
            raise ValueError
    return job, pod


def observe_restored_staging_collector(*, bindings: SharedPoolGuardBindings, guards: KubectlPoolGuardAPI,
        expected: Document, controller: Document) -> bool:
    """Current completed lineage plus the exact independently checked SQL receipt.

    Missing completion/fresh publication is pending, not a retry or mutation.
    Whole recovery history, provider authority and reopening remain the parent's
    responsibility, including distinguishing permitted waits from failed replay.
    """
    try:
        retained, desired, expected_controller = copy.deepcopy(bindings), copy.deepcopy(expected), copy.deepcopy(controller)
        attachment = retained.plan.staging.attachment
        original, = (row for row in retained.plan.staging.originals.values() if row['kind'] == 'CronJob')
        if (guards.request != retained.migration or attachment.guard not in guards.request.guards
                or _stable(desired)['spec'] != _stable(original)['spec'] or desired['spec']['suspend'] is not False
                or _key(desired) != _key(original) or _uid(desired) != _uid(original)
                or _stable(expected_controller)['spec'] != _stable(attachment.guard.controller)['spec']):
            raise ValueError
        namespace = original['metadata']['namespace']
        participant = attachment.participant
        pod_spec = original['spec']['jobTemplate']['spec']['template']['spec']
        container, = pod_spec['containers']
        if container['name'] != 'collector' or container.get('command') != ['python', '-m', 'loom_execution_capacity_collector']:
            raise ValueError
        config_ref, = container['envFrom']
        config_name = config_ref['configMapRef']['name']
        config, catalog = (next(row for row in attachment.resources
            if row['kind'] == 'ConfigMap' and row['metadata']['namespace'] == ns and row['metadata']['name'] == name)
            for ns, name in ((namespace, config_name), (attachment.guard.namespace, 'loom-platform-config')))
        environment = dict(config['data'])
        for row in container.get('env', []):
            if set(row) != {'name', 'value'} or row['name'] in environment:
                raise ValueError
            environment[row['name']] = row['value']
        prefix = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_'
        source = environment.get(prefix + 'SOURCE', ExecutionCapacityCollectorSettings.model_fields['source'].default)
        if environment.get(prefix + 'COLLECTION_MODE', 'target') != 'target':
            raise ValueError
        targets = [target.model_dump(mode='json') for key, raw in catalog['data'].items()
            if key in {'catalog.json', 'guest-catalog.json', 'emulated-auth-catalog.json'}
            for target in ExecutionTopologyV1.model_validate(json.loads(raw)['topology']).targets]
        if ({row['target_id'] for row in targets} != {row.target_id for row in participant.targets}
                or len(targets) != len(participant.targets)):
            raise ValueError
        secrets: dict[str, set[str]] = {}
        for volume in pod_spec.get('volumes', []):
            references = [(item['secret']['name'], item['secret'])
                for item in volume.get('projected', {}).get('sources', []) if 'secret' in item]
            if 'secret' in volume:
                references.append((volume['secret']['secretName'], volume['secret']))
            for name, reference in references:
                secrets.setdefault(name, set()).update(item['key'] for item in reference.get('items', []))

        def inputs() -> dict[str, Document]:
            if bindings != retained or expected != desired or controller != expected_controller or guards.request != retained.migration:
                raise ValueError
            guards._namespaces(attachment.guard)
            execution_namespace = guards._get('namespace', namespace)
            if (execution_namespace['kind'] != 'Namespace' or _uid(execution_namespace) != str(participant.execution_namespace.uid)):
                raise ValueError
            _snapshot(execution_namespace)
            values = {}
            for resource in (config, catalog):
                current = guards._get('configmap', resource['metadata']['name'], resource['metadata']['namespace'])
                if not _matches(current, resource, _uid(resource)) or current.get('data') != resource.get('data'):
                    raise ValueError
                values[_key(resource)] = current
            for name, required in sorted(secrets.items()):
                current = guards._get('secret', name, namespace)
                _snapshot(current)
                _uid(current)
                version = current['metadata']['resourceVersion']
                data = current.get('data')
                if (current.get('apiVersion') != 'v1' or current.get('kind') != 'Secret'
                        or current['metadata'].get('namespace') != namespace or current['metadata'].get('name') != name
                        or not isinstance(version, str) or not 0 < len(version) <= 128
                        or not isinstance(data, dict) or not data or not required.issubset(data)
                        or any(not isinstance(data[key], str) or not base64.b64decode(data[key], validate=True) for key in required)):
                    raise ValueError
                values[_key(current)] = current
            return values

        def processes() -> tuple[Document, Document, Document]:
            current = guards._get('cronjob', original['metadata']['name'], namespace)
            if not _matches(current, desired, _uid(original)):
                raise ValueError
            children = guards._run(['get', '--raw', '/apis/batch/v1/namespaces/' + namespace + '/jobs?limit=1000'])
            pods = guards._run(['get', '--raw', '/api/v1/namespaces/' + namespace + '/pods?limit=1000'])
            return current, children, pods

        before_inputs = inputs()
        before = processes()
        selected = _completed(*before, region=attachment.config['region'])
        ready = False
        if selected is not None:
            _, pod = selected
            report = guards._run(['logs', '-n', namespace, 'pod/' + pod['metadata']['name'], '-c', 'collector',
                '--limit-bytes=16384', '--timestamps=false'])
            receipt = CapacityObservationReceipt.model_validate_json(json.dumps(report))
            if receipt.target_id != environment[prefix + 'TARGET_ID'] or receipt.source != source:
                raise ValueError
            ready = guards.observe_collector_publication(attachment.guard, expected=expected_controller,
                publication={'receipt': receipt.model_dump(mode='json'), 'source': source, 'targets': targets,
                    'node_group_id': retained.migration.registration.spec.node_group_id,
                    'quota_identities': {key: list(value) for key, value in retained.migration.registration.spec.quota_identities.items()}})
        after = processes()
        if (not _matches(after[0], before[0], _uid(original))
                or after[0]['metadata'].get('generation') != before[0]['metadata'].get('generation')
                or _process_snapshot(*after) != _process_snapshot(*before) or inputs() != before_inputs):
            raise ValueError
        return ready
    except Exception:
        raise ValueError('restored staging collector unqualified') from None


class SharedRecoveryRuntimeReadiness:
    """Concrete runtime callback for the restart child's receipt-bound reads.

    The child owns restart intents, readiness/drain and history validation. This
    callback adds real settings, backend, optional telemetry and collector
    publication challenges for only its fixed current-source staging roster.
    It is also read-only during terminal replay; it creates no active child.
    """

    def __init__(self, *, bindings: SharedPoolGuardBindings, guards: KubectlPoolGuardAPI):
        self.bindings, self.guards = bindings, guards
        self._captured = copy.deepcopy(bindings)
        if guards.request != self._captured.migration:
            raise ValueError('restored staging runtime binding differs')

    def _restored(self, captured: Document) -> Document:
        result = _snapshot(captured)
        result['metadata']['uid'] = _uid(captured)
        result['metadata'].setdefault('annotations', {})[MARKER] = str(self._captured.migration.registration.spec.operation_id)
        return result

    @staticmethod
    def _identity(pod: Document) -> tuple[Any, ...]:
        return (_uid(pod), copy.deepcopy(pod['status']['containerStatuses']),
            copy.deepcopy(pod['status'].get('initContainerStatuses', [])))

    def __call__(self, request: DevelopmentRuntimeInstallRequest, phase: str,
            original: Document, desired: Document, current: Document) -> bool:
        try:
            retained = copy.deepcopy(self._captured)
            before_documents = copy.deepcopy((original, desired, current))
            target = retained.plan.staging.attachment.guard
            captured = retained.plan.staging.originals[_key(current)]
            restored = self._restored(captured)
            stopped = copy.deepcopy(restored)
            deployment = captured['kind'] == 'Deployment'
            stopped['spec']['replicas' if deployment else 'suspend'] = 0 if deployment else True

            def scope() -> None:
                if (phase != 'recovery-restart' or request != retained.plan.development.runtime.request
                        or self.bindings != retained or self._captured != retained or self.guards.request != retained.migration
                        or _stable(original) != _stable(stopped) or _uid(original) != _uid(captured)
                        or _stable(desired) != _stable(restored) or not _matches(current, restored, _uid(captured))
                        or (original, desired, current) != before_documents):
                    raise ValueError

            scope()
            active = captured['spec']['replicas'] == 1 if deployment else captured['spec']['suspend'] is False
            if not active:
                return True  # The child separately proves current descendant drain.
            database_before = self._identity(self.guards._database(target))
            checked = captured if deployment else target.controller
            checked_current = current if deployment else self._restored(target.controller)
            process_before = self._identity(self.guards._runtime(target, original=checked, expected=checked_current))
            if deployment:
                database = target.database
                if database is None:
                    raise ValueError
                actuator = captured['metadata']['namespace'] == retained.plan.staging.attachment.participant.execution_namespace.name
                uid = database.actuator_credential_uid if actuator else database.credential_uid
                version = database.actuator_credential_resource_version if actuator else database.credential_resource_version
                if uid is None or version is None:
                    raise ValueError
                self.guards.qualify_runtime_database(target, original=captured, expected=current,
                    credential_uid=uid, credential_resource_version=version)
                self.guards.qualify_runtime_legacy_settings(target, original=captured, expected=current)
                if actuator:
                    self.guards.qualify_runtime_telemetry(target, original=captured, expected=current)
                ready = True
            else:
                ready = observe_restored_staging_collector(bindings=self.bindings, guards=self.guards,
                    expected=restored, controller=checked_current)
            # Finish all collector/configuration/process reads before this
            # closing backend barrier; even pending collection is bracketed.
            if (self._identity(self.guards._runtime(target, original=checked, expected=checked_current)) != process_before
                    or self._identity(self.guards._database(target)) != database_before):
                raise ValueError
            scope()
            return ready
        except Exception:
            raise ValueError('restored staging runtime unqualified') from None
