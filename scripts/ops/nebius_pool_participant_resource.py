"""Retained candidate file writes and CAS for the normal-release parent.

Resource and Job delivery are connected; this is not a whole rollout proof.
The parent still owns phase-aware inventory/guard qualification, policy-changing
bootstrap, application readiness, and completed-history promotion.
No caller supplies a replacement document or an unrestricted resource path.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import ssl
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_live import (
    HTTPSDevelopmentRuntimeWorkloads,
    runtime_resource_path,
)
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    _advance_fixed_resources,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import _qualified_defaulted
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_operation import (
    RetainedParticipantRollout,
    load_participant_rollout,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.kubernetes_provider import _contains

if TYPE_CHECKING:
    from scripts.ops.nebius_pool_participant_creation import ParticipantCreationAPI

_KINDS = frozenset({'Namespace', 'ConfigMap', 'ServiceAccount', 'Role', 'RoleBinding', 'NetworkPolicy',
    'StatefulSet', 'Service', 'Deployment', 'CronJob', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'})
_MARKER = 'loom.nebius/participant-candidate-operation'
_SERVICE_ALLOCATION = ('clusterIP', 'clusterIPs', 'ipFamilies', 'ipFamilyPolicy', 'healthCheckNodePort')
_PRESERVED_KINDS = frozenset({'PersistentVolumeClaim', 'ResourceQuota'})
_CANDIDATE_FILES = frozenset({'00-namespaces.yaml', '10-config-network.yaml', '20-database.yaml',
    '40-services.yaml', '60-execution.yaml', '70-public.yaml', '80-backup.yaml'})
_JOB_FILES = frozenset({'30-migrate.yaml', '50-configure.yaml'})


class RetainedCandidateFileWriter:
    """Connect fixed resource and Job engines to ordinary deployment phases.

    This includes pre-upgrade backup and explicit failed-Job recovery, but not
    policy-changing bootstrap or overall application readiness.
    The protected parent supplies source-bound API factories whose qualification
    callbacks own the current guard and full phase-aware inventory. An independent
    dispatch anchor prevents missing child evidence from reopening a write.
    """

    def __init__(self, *, retained: RetainedParticipantRollout,
            open_resource: Callable[[str], AbstractContextManager[DevelopmentRuntimeWorkloadAPI]],
            open_creation: Callable[[str], AbstractContextManager[ParticipantCreationAPI]]):
        self.retained = copy.deepcopy(retained)
        self.open_resource, self.open_creation = open_resource, open_creation

    def validate(self) -> None:
        if load_participant_rollout(reference=self.retained.reference,
                state_root=self.retained.state_dir.parent, anchor_dir=self.retained.anchor_dir) != self.retained:
            raise ValueError('candidate file source changed; preserve evidence')

    def qualify_identity_policy(self) -> None:
        """Prove the installed policy is unchanged, using read/dry-run only.

        A policy-changing candidate still requires a separately journalled
        bootstrap; this code-only route cannot silently adopt it. Namespace PSS
        stays at its installed value and no probe ServiceAccount is created.
        """
        try:
            self.validate()
            policies = self.retained.release.files['00-task-identity-policy.yaml']
            if {row['kind'] for row in policies} != {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'} or len(policies) != 2:
                raise ValueError
            for document in policies:
                key = _key(document)
                original, desired, _ = _inputs(self.retained, key)
                with self.open_resource(key) as api:
                    api.qualify()
                    observed = api.read_workload(key)
                    if not _matches(observed, original, _uid(original)):
                        raise ValueError
                    preview = api.preview_workload(key, observed, desired)
                    if preview is None:
                        raise ValueError
                    _qualified_candidate(desired, preview)
                    value = copy.deepcopy(preview)
                    annotations = value['metadata']['annotations']
                    if annotations[_MARKER] != str(self.retained.reference.operation_id):
                        raise ValueError
                    previous = original['metadata'].get('annotations', {})
                    if _MARKER in previous:
                        annotations[_MARKER] = previous[_MARKER]
                    else:
                        del annotations[_MARKER]
                    if not annotations and 'annotations' not in original['metadata']:
                        value['metadata'].pop('annotations')
                    if _stable(value) != _stable(original) or not _matches(api.read_workload(key), original, _uid(original)):
                        raise ValueError
                    api.qualify()
            self.validate()
        except Exception:
            raise ValueError('installed candidate identity policy differs; preserve evidence') from None

    def _children(self) -> dict[str, tuple[str, Path]]:
        originals = {_key(row) for row in self.retained.inputs.current}
        result = {}
        for filename in _CANDIDATE_FILES | _JOB_FILES:
            for document in self.retained.release.files[filename]:
                key = _key(document)
                if key in result:
                    raise ValueError
                update = key in originals
                root, journal = ('candidate-resources', 'transition.json') if update else ('candidate-creations', 'stage.json')
                action = 'preserve' if update and document['kind'] in _PRESERVED_KINDS else 'update' if update else 'create'
                result[key] = (action,
                    self.retained.state_dir / root / hashlib.sha256(key.encode()).hexdigest() / journal)
        backup = 'pre-upgrade-backup'
        result[backup] = ('create', self.retained.state_dir / 'candidate-creations'
            / hashlib.sha256(backup.encode()).hexdigest() / 'stage.json')
        return result

    def _job_key(self, selector: str) -> str:
        if selector == 'pre-upgrade-backup':
            return selector
        if selector not in _JOB_FILES:
            raise ValueError
        document, = self.retained.release.files[selector]
        if document['kind'] != 'Job':
            raise ValueError
        return _key(document)

    def job_name(self, selector: str) -> str:
        from scripts.ops.nebius_pool_participant_creation import _creation

        self.validate()
        document, _ = _creation(self.retained, self._job_key(selector))
        return str(document['metadata']['name'])

    def _record(self, path: Path, children: dict[str, tuple[str, Path]]) -> dict[str, Any]:
        identity = {'schema': 'loom.nebius-participant-file-writes.v1',
            'reference': self.retained.reference.model_dump(mode='json')}
        anchor = self.retained.anchor_dir / (str(self.retained.reference.operation_id) + '-candidate-file-writes.json')
        anchored = anchor.exists() or anchor.is_symlink()
        if path.exists() or path.is_symlink():
            record: dict[str, Any] = json.loads(private_state._private_read(path, limit=4 * 1024**2))
            if (set(record) != {*identity, 'started'} or any(record[key] != value for key, value in identity.items())
                    or not isinstance(record['started'], list)
                    or any(not isinstance(key, str) or key not in children or children[key][0] == 'preserve'
                        for key in record['started'])
                    or len(set(record['started'])) != len(record['started'])
                    or (record['started'] and not anchored)):
                raise ValueError
        else:
            if anchored or any(child.exists() or child.is_symlink() for _, child in children.values()):
                raise ValueError
            record = {**identity, 'started': []}
            private_state._atomic_json(path, record)
        if anchored:
            if json.loads(private_state._private_read(anchor)) != identity:
                raise ValueError
        else:
            # No effect precedes both records. An interrupted, empty local
            # preparation can finish; dispatched or missing evidence cannot.
            if any(child.exists() or child.is_symlink() for _, child in children.values()):
                raise ValueError
            private_state._write_private(anchor, json.dumps(identity, sort_keys=True).encode())
            private_state._sync_directory(anchor.parent)
        for key in record['started']:
            private_state._private_read(children[key][1], limit=4 * 1024**2)
        return record

    @staticmethod
    def _begin_child(*, key: str, record: dict[str, Any], path: Path, child: Path) -> None:
        if key not in record['started']:
            if child.exists() or child.is_symlink():
                raise ValueError
            record['started'].append(key)
            private_state._atomic_json(path, record)

    def run_job(self, selector: str, *, retry_failed_jobs: bool) -> dict[str, Any]:
        """Execute/recover one retained Job; at most one explicitly requested retry.

        A failed condition first reached during this invocation does not authorize
        retry. Cleanup polling only reads; all actual effects requalify the parent
        through the source-bound API. No result is accepted while owned Pods live.
        """
        from scripts.ops.nebius_pool_participant_creation import (
            _creation,
            stage_participant_creation,
        )

        try:
            self.validate()
            if type(retry_failed_jobs) is not bool:
                raise ValueError
            key = self._job_key(selector)
            document, _ = _creation(self.retained, key)
            children = self._children()
            root = self.retained.state_dir / 'candidate-file-writes'
            with private_state._locked_state(root):
                path = root / 'dispatch.json'
                record = self._record(path, children)
                deadline = time.monotonic() + (1860 if key == 'pre-upgrade-backup' else 660)
                eligible: bool | None = None
                retried = False
                while True:
                    self.validate()
                    with self.open_creation(key) as api:
                        if api.retained != self.retained or api.key != key:
                            raise ValueError
                        self._begin_child(key=key, record=record, path=path, child=children[key][1])
                        if api.retry_pending():
                            if retry_failed_jobs and not retried:
                                retried = True
                                ready = api.retry_failed_job()
                            else:
                                ready = api.reconcile_retry()
                            if ready:
                                continue  # The old handle cannot operate the newly selected generation.
                        else:
                            if eligible is None:
                                before = api.get_resource(document)
                                eligible = before is not None and any(row.get('type') == 'Failed' and row.get('status') == 'True'
                                    for row in before.get('status', {}).get('conditions', []))
                            stage_participant_creation(retained=self.retained, key=key, api=api)
                            outcome = api.job_outcome()
                            if outcome is not None:
                                if outcome['status'] == 'complete':
                                    self.validate()
                                    return outcome
                                if outcome['status'] != 'failed' or not eligible or not retry_failed_jobs or retried:
                                    raise ValueError
                                retried = True
                                if api.retry_failed_job():
                                    continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ValueError
                    time.sleep(min(5, remaining))
        except Exception:
            raise ValueError('participant Job execution unqualified; preserve evidence') from None

    def apply_file(self, filename: str) -> None:
        from scripts.ops.nebius_pool_participant_creation import (
            _creation,
            stage_participant_creation,
        )

        try:
            self.validate()
            if filename not in _CANDIDATE_FILES:
                raise ValueError
            children = self._children()
            keys = [_key(row) for row in self.retained.release.files[filename]]
            # Validate every selected resource before dispatching the first one.
            for key in keys:
                if children[key][0] in {'update', 'preserve'}:
                    _inputs(self.retained, key)
                else:
                    _creation(self.retained, key)
            root = self.retained.state_dir / 'candidate-file-writes'
            with private_state._locked_state(root):
                path = root / 'dispatch.json'
                record = self._record(path, children)
                # Read-only infrastructure must qualify before any application
                # write in this file, including when it occurs late in render order.
                for key in keys:
                    if children[key][0] == 'preserve':
                        original, _, _ = _inputs(self.retained, key)
                        with self.open_resource(key) as read_api:
                            read_api.qualify()
                            if not _matches(read_api.read_workload(key), original, _uid(original)):
                                raise ValueError
                            read_api.qualify()
                for key in keys:
                    self.validate()
                    if children[key][0] == 'preserve':
                        continue
                    self._begin_child(key=key, record=record, path=path, child=children[key][1])
                    if children[key][0] == 'update':
                        with self.open_resource(key) as update_api:
                            if not update_participant_resource(retained=self.retained, key=key, api=update_api):
                                raise ValueError
                    else:
                        with self.open_creation(key) as create_api:
                            stage_participant_creation(retained=self.retained, key=key, api=create_api)
                self.validate()
        except Exception:
            raise ValueError('candidate file write unqualified; preserve evidence') from None


def _resource_path(document: dict[str, Any]) -> str:
    metadata = document['metadata']
    if any(not isinstance(value, str) or re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]|[a-z0-9]', value) is None
            for value in (metadata['name'], *([metadata['namespace']] if 'namespace' in metadata else []))):
        raise ValueError
    extra = {'Namespace': ('v1', 'namespaces'), 'Service': ('v1', 'services'),
        'PersistentVolumeClaim': ('v1', 'persistentvolumeclaims'),
        'ResourceQuota': ('v1', 'resourcequotas'),
        'StatefulSet': ('apps/v1', 'statefulsets')}
    if document['kind'] not in extra:
        return runtime_resource_path(document) + '/' + str(metadata['name'])
    version, plural = extra[document['kind']]
    cluster = document['kind'] == 'Namespace'
    if document['apiVersion'] != version or cluster == ('namespace' in metadata):
        raise ValueError
    return ('/api/v1' if version == 'v1' else '/apis/' + version) + (
        '' if cluster else '/namespaces/' + str(metadata['namespace'])) + '/' + plural + '/' + str(metadata['name'])


def _preserved_quantities(document: dict[str, Any]) -> dict[str, Any]:
    """Compare storage/quota amounts exactly without changing retained spelling."""
    from kubernetes.utils.quantity import parse_quantity

    result = copy.deepcopy(document)
    if result['kind'] == 'ResourceQuota':
        maps = [result['spec']['hard']]
    elif result['kind'] == 'PersistentVolumeClaim':
        resources = result['spec']['resources']
        maps = [resources.get(field, {}) for field in ('requests', 'limits')]
    else:
        raise ValueError
    for quantities in maps:
        for key, value in quantities.items():
            amount = parse_quantity(value)
            if not amount.is_finite() or amount < 0:
                raise ValueError
            quantities[key] = str(amount.normalize())
    return result


def _inputs(retained: RetainedParticipantRollout, key: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    original, = (row for row in retained.inputs.current if _key(row) == key)
    candidate, = (row for rows in retained.release.files.values() for row in rows if _key(row) == key)
    if (candidate['kind'] not in _KINDS | _PRESERVED_KINDS or key in retained.release.preserved
            or original['metadata'].get('annotations', {}).get(_MARKER) == str(retained.reference.operation_id)):
        raise ValueError
    _uid(original)
    desired = _snapshot(candidate)
    if candidate['kind'] in _PRESERVED_KINDS:
        # Code rollout cannot resize/rebind storage or change infrastructure
        # quotas. Qualify the original request and preserve all allocations.
        if not _contains(_preserved_quantities(original), _preserved_quantities(desired)):
            raise ValueError
        return copy.deepcopy(original), _snapshot(original), digest({
            'reference': retained.reference.model_dump(mode='json'), 'key': key, 'preserved': original})
    for field in ('labels', 'annotations'):
        merged = {**original['metadata'].get(field, {}), **desired['metadata'].get(field, {})}
        if merged:
            desired['metadata'][field] = merged
    # Give a committed write a distinct observable intent even when the render
    # otherwise matches the original. An unknown PATCH cannot be inferred from
    # an unchanged old object or silently retried.
    desired['metadata'].setdefault('annotations', {})[_MARKER] = str(retained.reference.operation_id)
    if desired['kind'] == 'Namespace':
        # Namespace lifecycle/finalizers are not part of an application upgrade.
        if 'spec' in original:
            if 'spec' in desired and desired['spec'] != original['spec']:
                raise ValueError
            desired['spec'] = copy.deepcopy(original['spec'])
    if desired['kind'] == 'Service':
        for field in _SERVICE_ALLOCATION:
            if field in original['spec']:
                if field in desired['spec'] and desired['spec'][field] != original['spec'][field]:
                    raise ValueError
                desired['spec'][field] = copy.deepcopy(original['spec'][field])
        for port in desired['spec']['ports']:
            matches = [row for row in original['spec']['ports']
                if (row.get('name'), row['port'], row.get('protocol', 'TCP'))
                    == (port.get('name'), port['port'], port.get('protocol', 'TCP'))]
            if len(matches) == 1 and 'nodePort' in matches[0]:
                if 'nodePort' in port and port['nodePort'] != matches[0]['nodePort']:
                    raise ValueError
                port['nodePort'] = matches[0]['nodePort']
    identity = digest({'reference': retained.reference.model_dump(mode='json'), 'key': key,
        'original': original, 'desired': desired})
    return copy.deepcopy(original), desired, identity


def _qualified_candidate(desired: dict[str, Any], observed: dict[str, Any]) -> dict[str, Any]:
    if desired['kind'] != 'Service':
        _qualified_defaulted(desired, observed)
    else:
        # The management defaulting helper intentionally forbids public Services.
        # Here the exact candidate determines exposure, including its existing LB.
        if not _contains(observed, desired):
            raise ValueError
        wanted, actual = desired['spec'], observed['spec']
        for field, default in (('type', 'ClusterIP'), ('externalIPs', []), ('externalName', None),
                ('loadBalancerIP', None), ('loadBalancerClass', None), ('loadBalancerSourceRanges', []),
                ('externalTrafficPolicy', 'Cluster'), ('internalTrafficPolicy', 'Cluster'),
                ('publishNotReadyAddresses', False), ('allocateLoadBalancerNodePorts', True)):
            if actual.get(field, default) != wanted.get(field, default):
                raise ValueError
        for actual_port, wanted_port in zip(actual['ports'], wanted['ports'], strict=True):
            if actual_port.get('nodePort') != wanted_port.get('nodePort'):
                raise ValueError
    return _stable(observed)


def update_participant_resource(*, retained: RetainedParticipantRollout, key: str,
        api: DevelopmentRuntimeWorkloadAPI) -> bool:
    """Retain preview/UID/CAS and read uncertain outcomes without another write.

    True proves the resource update only. The full deployer and phase-aware pool
    parent must still qualify descendants, Jobs, application and capacity health.
    """
    try:
        retained = copy.deepcopy(retained)
        loaded = load_participant_rollout(reference=retained.reference, state_root=retained.state_dir.parent,
            anchor_dir=retained.anchor_dir)
        if loaded != retained:
            raise ValueError
        original, target, identity = _inputs(retained, key)
        root = retained.state_dir / 'candidate-resources'
        with private_state._locked_state(root):
            return _advance_fixed_resources(originals={key: original}, targets={key: target}, input_digest=identity,
                phase='candidate', api=api, state_dir=root / hashlib.sha256(key.encode()).hexdigest(),
                phases=frozenset({'candidate'}), kinds=_KINDS, qualify_defaulted=_qualified_candidate)
    except Exception:
        raise ValueError('participant resource update unqualified; preserve evidence') from None


class HTTPSParticipantResourceAPI(HTTPSDevelopmentRuntimeWorkloads):
    """One retained candidate object, patched with atomic UID/version tests."""

    def __init__(self, *, retained: RetainedParticipantRollout, key: str, api_server: str,
            ssl_context: ssl.SSLContext, qualify_resource: Callable[[RetainedParticipantRollout, str], None],
            token: str | None = None):
        try:
            self.retained, self.key = copy.deepcopy(retained), key
            original, target, self.input_digest = _inputs(self.retained, key)
            if (api_server.rstrip('/') != retained.inputs.config['kubernetes_api_server'].rstrip('/')
                    or not callable(qualify_resource)):
                raise ValueError
            self.originals, self.targets = {key: original}, {key: target}
            self.qualify_resource = qualify_resource
            self._path(key)
            self._private_inputs()
            ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)
        except Exception:
            raise ValueError('participant resource connection unqualified') from None

    def _private_inputs(self) -> None:
        current = load_participant_rollout(reference=self.retained.reference, state_root=self.retained.state_dir.parent,
            anchor_dir=self.retained.anchor_dir)
        original, target, identity = _inputs(current, self.key)
        if (current != self.retained or identity != self.input_digest
                or self.originals != {self.key: original} or self.targets != {self.key: target}):
            raise ValueError('participant resource inputs changed')

    def _qualify(self, *, writing: bool) -> None:
        self._private_inputs()
        self.qualify_resource(copy.deepcopy(self.retained), self.key)
        self._private_inputs()

    def _path(self, key: str) -> str:
        return _resource_path(self.originals[key])

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        try:
            original = self.originals[key]
            if original['kind'] in _PRESERVED_KINDS:
                raise ValueError
            if desired != self.targets[key] or not _matches(before, original, _uid(original)):
                raise ValueError
            version = before['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(original)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version}]
            for field in ('labels', 'annotations'):
                if field in desired['metadata']:
                    patches.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
            # Replace candidate-owned content, preserving API identity/status and
            # the explicitly retained Service allocations/Namespace finalizers.
            for field in sorted((before.keys() | desired.keys()) - {'apiVersion', 'kind', 'metadata', 'status'}):
                if field in desired:
                    patches.append({'op': 'add', 'path': '/' + field, 'value': desired[field]})
                else:
                    patches.append({'op': 'remove', 'path': '/' + field})
            self._qualify(writing=True)
            with self.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''), json=patches,
                    headers={'Content-Type': 'application/json-patch+json'}) as response:
                if (response.status_code not in {200, 409, 422}
                        or response.headers.get('content-encoding', 'identity').lower() != 'identity'):
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(content) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    content.extend(chunk)
                value = json.loads(content)
                if not isinstance(value, dict):
                    raise ValueError
                if response.status_code in {409, 422}:
                    if (value.get('apiVersion') != 'v1' or value.get('kind') != 'Status' or value.get('status') != 'Failure'
                            or value.get('code') != response.status_code
                            or value.get('reason') != {409: 'Conflict', 422: 'Invalid'}[response.status_code]):
                        raise ValueError
                    return None
                if _uid(value) != _uid(original):
                    raise ValueError
                _qualified_candidate(desired, value)
                return value
        except Exception:
            raise ValueError('participant resource update unconfirmed; preserve evidence') from None

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        """Resource readback only; the parent separately owns runtime readiness."""
        _qualified_candidate(self.targets[key], expected)
        return _matches(self.read_workload(key), expected, _uid(self.originals[key]))
