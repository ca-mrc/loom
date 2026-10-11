"""Retained candidate-resource CAS for the protected normal-release parent.

This is one update child, not a rollout or readiness proof. The parent owns
phase-aware inventory/guard qualification, independent child-start evidence,
ordering, creates/Job lineage, readiness, and completed-history promotion.
No caller supplies a replacement document or an unrestricted resource path.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import ssl
from collections.abc import Callable
from typing import Any

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

_KINDS = frozenset({'Namespace', 'ConfigMap', 'ServiceAccount', 'Role', 'RoleBinding', 'NetworkPolicy',
    'StatefulSet', 'Service', 'Deployment', 'CronJob', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'})
_MARKER = 'loom.nebius/participant-candidate-operation'
_SERVICE_ALLOCATION = ('clusterIP', 'clusterIPs', 'ipFamilies', 'ipFamilyPolicy', 'healthCheckNodePort')


def _resource_path(document: dict[str, Any]) -> str:
    metadata = document['metadata']
    if any(not isinstance(value, str) or re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]|[a-z0-9]', value) is None
            for value in (metadata['name'], *([metadata['namespace']] if 'namespace' in metadata else []))):
        raise ValueError
    extra = {'Namespace': ('v1', 'namespaces'), 'Service': ('v1', 'services'),
        'StatefulSet': ('apps/v1', 'statefulsets')}
    if document['kind'] not in extra:
        return runtime_resource_path(document) + '/' + str(metadata['name'])
    version, plural = extra[document['kind']]
    cluster = document['kind'] == 'Namespace'
    if document['apiVersion'] != version or cluster == ('namespace' in metadata):
        raise ValueError
    return ('/api/v1' if version == 'v1' else '/apis/' + version) + (
        '' if cluster else '/namespaces/' + str(metadata['namespace'])) + '/' + plural + '/' + str(metadata['name'])


def _inputs(retained: RetainedParticipantRollout, key: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    original, = (row for row in retained.inputs.current if _key(row) == key)
    candidate, = (row for rows in retained.release.files.values() for row in rows if _key(row) == key)
    if (candidate['kind'] not in _KINDS or key in retained.release.preserved
            or original['metadata'].get('annotations', {}).get(_MARKER) == str(retained.reference.operation_id)):
        raise ValueError
    _uid(original)
    desired = _snapshot(candidate)
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
