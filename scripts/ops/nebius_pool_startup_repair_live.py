"""One immutable ConfigMap and fixed manager CAS on the original operator scope.

No client lifetime, credentials, arbitrary manifest, permissions or retry policy
is introduced here. The protected continuation owns the parent connection.
"""
from __future__ import annotations

import copy
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import _MARKER
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI
from scripts.ops.nebius_pool_startup_repair import (
    _RECOVERY,
    _STEPS,
    PoolStartupRepairBinding,
    _configuration_record,
    _exists,
    _repair_record,
    source_repair_documents,
)


class _FixedRepairConfiguration:
    def __init__(self, owner: HTTPSPoolStartupRepairAPI):
        self.owner = owner

    def verify_identity(self, binding: ManagementBinding) -> None:
        if binding != self.owner.parent.binding:
            raise ValueError('pool_repair_configuration_scope_unqualified')
        self.owner.qualify_closed()

    def _path(self, document: dict[str, Any], *, writing: bool = False) -> str:
        self.owner._qualify_binding()
        value = copy.deepcopy(document)
        annotations = value['metadata'].get('annotations', {})
        operation = annotations.pop(_MARKER, None)
        if writing or operation is not None:
            if str(UUID(operation)) != operation or not UUID(operation).int:
                raise ValueError
        if not annotations and 'annotations' not in self.owner.config['metadata']:
            value['metadata'].pop('annotations', None)
        if value != self.owner.config:
            raise ValueError('pool_repair_configuration_scope_unqualified')
        return '/api/v1/namespaces/' + value['metadata']['namespace'] + '/configmaps'

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        return self.owner.parent._request('GET', self._path(document) + '/' + document['metadata']['name'])

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        path = self._path(document, writing=True)
        self.verify_identity(self.owner.parent.binding)
        actual = self.owner.parent._request('POST', path + '?dryRun=All', document=document)
        if actual is None:
            raise ValueError
        return actual

    def create_resource(self, document: dict[str, Any]) -> None:
        path = self._path(document, writing=True)
        self.verify_identity(self.owner.parent.binding)
        record = _configuration_record(self.owner.request, self.owner.config, self.owner.state)
        if record is None:
            raise ValueError
        item = record['resources'][_key(document)]
        if item['status'] != 'create_intent' or item['desired'] != document:
            raise ValueError('pool_repair_configuration_intent_required')
        self.owner.parent._request('POST', path, document=document)

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('pool_repair_database_outside_scope')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('pool_repair_database_outside_scope')


class HTTPSPoolStartupRepairAPI(HTTPSPoolStartupAPI):
    def __init__(self, *, parent: HTTPSPoolCutoverAPI, binding: PoolStartupRepairBinding):
        super().__init__(parent=parent)
        self.binding = PoolStartupRepairBinding.model_validate(binding.model_dump())
        self.documents, self.config, _, _ = _repair_record(self.request, state=self.state,
            anchor=self.anchor, binding=self.binding)
        self.resources = _FixedRepairConfiguration(self)

    def _qualify_binding(self) -> dict[str, Any] | None:
        self._scope()
        documents, config, identity, record = _repair_record(self.request, state=self.state,
            anchor=self.anchor, binding=self.binding)
        if (documents != self.documents or config != self.config
                or _hash(self.state / 'activation.json') != self.binding.activation_sha256
                or any(_exists(path) for phase in _RECOVERY for path in (
                    self.state / (phase + '.json'), self.anchor / (identity['operation_id'] + '-' + phase + '.json')))):
            raise ValueError('pool_repair_entry_changed')
        return record

    def qualify_closed(self) -> None:
        self._qualify_binding()
        super().qualify_closed()
        self._qualify_binding()

    def manager_drained(self, key: str, desired: dict[str, Any]) -> bool:
        self.qualify_closed()
        if (key != _key(self.request.manager)
                or not any(_stable(desired) == _stable(row) for row in self.documents[1:3])):
            raise ValueError('pool_repair_drain_scope_unqualified')
        current = self.read_workload(key)
        namespace = self.request.manager['metadata']['namespace']
        children = self.parent._request('GET', '/apis/apps/v1/namespaces/' + namespace + '/replicasets?limit=1000')
        pods = self.parent._request('GET', '/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        if children is None or pods is None or self.read_workload(key) != current:
            raise ValueError('pool_repair_drain_changed')
        drained = qualify_closed_workload_drain(original=self.closed[key], desired=desired,
            current=current, children=children, pods=pods)
        self.qualify_closed()
        return drained

    def _patch_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> bool:
        try:
            record = self._qualify_binding()
            index, version = _STEPS.index(phase), before['metadata']['resourceVersion']
            key = _key(self.request.manager)
            if (record is None or not _matches(before, self.documents[index], _uid(self.request.manager))
                    or _stable(desired) != _stable(self.documents[index + 1])
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or record['phases'][phase] != {'phase': 'prepared' if preview else 'intent',
                        'before_resource_version': None if preview else version}):
                raise ValueError
            self.qualify_closed()
            if phase in {'template', 'start'} and self.manager_drained(key, self.documents[index]) is not True:
                raise ValueError
            proposed = _snapshot(before)
            if phase == 'template':
                # Preserve server representation of every unrelated field.
                running = copy.deepcopy(before)
                running['spec']['replicas'] = 1
                fixed, _ = source_repair_documents(self.request, running)
                proposed['spec']['template'] = fixed['spec']['template']
                field, value = '/spec/template', proposed['spec']['template']
            else:
                proposed['spec']['replicas'] = desired['spec']['replicas']
                field, value = '/spec/replicas', desired['spec']['replicas']
            if _stable(proposed) != _stable(desired) or self._qualify_binding() != record:
                raise ValueError
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(self.request.manager)},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': field, 'value': value}]
            with self.parent.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=proposed, uid=_uid(self.request.manager))
        except Exception:
            raise ValueError('pool_repair_update_unconfirmed') from None

    def preview_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._patch_repair(phase, before, desired, preview=True) else None

    def patch_repair(self, phase: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch_repair(phase, before, desired, preview=False)
