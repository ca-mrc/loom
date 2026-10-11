"""Receipt-bound live resources for normal releases, not full runtime authority.

This read-only observer follows fixed create/CAS journals, including failed-Job
generations. The protected caller must ALSO qualify publication, predecessor
history, all pool writers/permissions, processes, databases, provider and guard.
No resource receipt or successful read substitutes for those independent checks.
"""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_transition import validate_runtime_transition
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_stage import (
    _qualified_defaulted,
    _validate_record,
)
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_adapter import RetainedParticipantPoolAdapter
from scripts.ops.nebius_pool_participant_catalog import ParticipantCatalogRequest, _projection
from scripts.ops.nebius_pool_participant_creation import _attempt, _creation
from scripts.ops.nebius_pool_participant_job import (
    PoolParticipantJobRequest,
    _parts,
    validate_participant_refresh_report,
)
from scripts.ops.nebius_pool_participant_operation import (
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import (
    _CANDIDATE_FILES,
    _JOB_FILES,
    _PRESERVED_KINDS,
    _inputs,
    _qualified_candidate,
    _resource_path,
)
from scripts.ops.nebius_shared_pool_operation import _files

from loom.nebius_platform_render import digest
from loom_service.pool_management.participant_refresh import RefreshAction

Document = dict[str, Any]


@dataclass(frozen=True)
class _Choice:
    document: Document
    uid: str | None
    requested: Document | None = None


class _ResourceView:
    """Derive only source-bound alternatives; never write or acknowledge intent."""

    def __init__(self, retained: RetainedParticipantRollout, preparing: str | None):
        self.retained = retained
        self.files: dict[Path, bytes | None] = {}
        self.documents: dict[str, Document] = {}
        self.options: dict[str, tuple[_Choice | None, ...]] = {}
        for row in (*retained.inputs.current, retained.inputs.manager, retained.inputs.catalog):
            key = _key(row)
            if key in self.documents:
                raise ValueError
            self.documents[key] = row
            choice = _Choice(row, _uid(row))
            self.options[key] = (None, choice) if row['kind'] == 'Job' else (choice,)
        self._candidate(preparing)
        self._pool()
        roots = ('candidate-file-writes', 'candidate-resources', 'candidate-creations', 'pool-refresh')
        actual = {retained.state_dir / root / name: checksum for root in roots
            if (retained.state_dir / root).exists() or (retained.state_dir / root).is_symlink()
            for name, checksum in _files(retained.state_dir / root, allow_empty=True).items()}
        if actual != {path: hashlib.sha256(raw).hexdigest() for path, raw in self.files.items()
                if raw is not None and path.is_relative_to(retained.state_dir)}:
            raise ValueError

    def _read(self, path: Path, *, required: bool = False) -> Document | None:
        if path != path.resolve():
            raise ValueError
        raw = private_state._private_read(path, limit=4 * 1024**2) if path.exists() or path.is_symlink() else None
        if (required and raw is None) or (path in self.files and self.files[path] != raw):
            raise ValueError
        self.files[path] = raw
        if raw is None:
            return None
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError
        return value

    def _parent(self, root: str, suffix: str, schema: str, fields: set[str]) -> Document | None:
        retained = self.retained
        identity = {'schema': schema, 'reference': retained.reference.model_dump(mode='json')}
        parent = self._read(retained.state_dir / root / 'dispatch.json')
        anchor = self._read(retained.anchor_dir / (str(retained.reference.operation_id) + suffix))
        if parent is None:
            if anchor is not None:
                raise ValueError
            return None
        if (set(parent) != {*identity, *fields} or any(parent[key] != value for key, value in identity.items())
                or (anchor is not None and anchor != identity) or (anchor is None and any(parent[key] for key in fields))):
            raise ValueError
        return parent

    def _transition(self, path: Path, *, original: Document, target: Document, identity: str,
            phase: str, candidate: bool, required: bool) -> Document | None:
        key = _key(original)
        record = self._read(path, required=required)
        if record is None:
            return None
        validate_runtime_transition(record, originals={key: original}, targets={key: target},
            input_digest=identity, phase=phase,
            qualify_defaulted=_qualified_candidate if candidate else _qualified_defaulted)
        item: Document = record['resources'][key]
        before = _Choice(original, _uid(original))
        after = None if item['expected'] is None else _Choice(item['expected'], _uid(original))
        self.documents[key] = original
        self.options[key] = ((before,) if item['status'] == 'prepared' else
            (before, after) if item['status'] == 'intent' else (after,))
        return item

    def _stage(self, path: Path, *, document: Document, revision: str, phase: str,
            dispatched: bool, required: bool = False) -> Document | None:
        key = _key(document)
        self.documents[key] = document
        self.options[key] = (None,)
        record = self._read(path, required=required)
        if record is None:
            return None
        binding = self.retained.release.migration.registration.binding
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(binding),
            'revision': revision, 'phase': phase}, {key: document})
        item: Document = record['resources'][key]
        if item['status'] == 'created':
            if not dispatched:
                raise ValueError
            observed = copy.deepcopy(item['observed'])
            observed['metadata']['uid'] = item['uid']
            _uid(observed)
            if _qualified_defaulted(item['desired'], observed) != item['expected']:
                raise ValueError
            self.options[key] = (_Choice(observed, item['uid']),)
        elif item['status'] == 'create_intent' and dispatched:
            self.options[key] = (None, _Choice(item['expected'], None, requested=item['desired']))
        return item

    def _candidate(self, preparing: str | None) -> None:
        retained = self.retained
        parent = self._parent('candidate-file-writes', '-candidate-file-writes.json',
            'loom.nebius-participant-file-writes.v1', {'started'})
        children = {_key(row): row for filename in _CANDIDATE_FILES | _JOB_FILES for row in retained.release.files[filename]}
        children['pre-upgrade-backup'] = _creation(retained, 'pre-upgrade-backup')[0]
        originals = {_key(row): row for row in retained.inputs.current}
        if preparing is not None and preparing not in children and preparing not in originals:
            raise ValueError
        started = [] if parent is None else parent['started']
        if (not isinstance(started, list) or any(not isinstance(key, str) or key not in children for key in started)
                or len(set(started)) != len(started)):
            raise ValueError
        binding = retained.release.migration.registration.binding
        for key, document in children.items():
            if key in originals and document['kind'] in _PRESERVED_KINDS:
                if key in started:
                    raise ValueError
                _inputs(retained, key)
                continue
            preparing_child = bool(started) and key == preparing == started[-1]
            required = key in started and not preparing_child
            folder = hashlib.sha256(key.encode()).hexdigest()
            if key in originals:
                original, target, identity = _inputs(retained, key)
                item = self._transition(retained.state_dir / 'candidate-resources' / folder / 'transition.json',
                    original=original, target=target, identity=identity, phase='candidate', candidate=True, required=required)
            else:
                document, identity, directory, retries = _attempt(retained, key)
                root = retained.state_dir / 'candidate-creations' / folder
                self._read(root / 'retries.json')
                if retries is not None:
                    for index in range(len(retries['attempts'])):
                        old = root if index == 0 else root / ('attempt-' + str(index))
                        self._read(old / 'stage.json', required=True)
                    last = retries['attempts'][-1]
                    required = (last['next_started'] and not preparing_child) if last['status'] == 'retired' else True
                item = self._stage(directory / 'stage.json', document=document,
                    revision=digest({'creation': identity, 'binding': asdict(binding)}), phase='participant-creation',
                    dispatched=key in started, required=required)
                if retries is not None and retries['attempts'][-1]['status'] == 'delete_intent':
                    self.options[_key(document)] = (None, *self.options[_key(document)])
            if item is not None and key not in started:
                raise ValueError

    def _pool(self) -> None:
        retained = self.retained
        root = retained.state_dir / 'pool-refresh'
        parent = self._parent('pool-refresh', '-pool-effects.json',
            'loom.nebius-participant-pool-effects.v1', {'steps', 'intents'})
        steps, intents = ({}, []) if parent is None else (parent['steps'], parent['intents'])
        if (not isinstance(steps, dict) or not isinstance(intents, list)
                or any(not isinstance(path, str) for path in intents) or len(set(intents)) != len(intents)):
            raise ValueError
        allowed: set[str] = set()
        for name, step in steps.items():
            if (set(step) != {'status', 'result'} or step['status'] not in {'started', 'complete'}
                    or (step['status'] == 'started' and step['result'] is not None)
                    or (step['status'] == 'complete' and not isinstance(step['result'], dict))):
                raise ValueError
            RetainedParticipantPoolAdapter._prerequisites({'steps': steps}, name)
        actions: list[tuple[str, RefreshAction, UUID | None]] = [
            (name, cast(RefreshAction, name), None) for name in ('fence', 'stage', 'open')]
        for name in steps:
            if name.startswith('capacity:'):
                observation_id = UUID(name.removeprefix('capacity:'))
                if not observation_id.int or name != 'capacity:' + str(observation_id):
                    raise ValueError
                actions.append((name, 'capacity', observation_id))
        binding = retained.release.migration.registration.binding
        for name, action, observation in actions:
            request = PoolParticipantJobRequest(retained.release, retained.inputs.manager, action, observation)
            if name in steps and steps[name]['status'] == 'complete':
                result = steps[name]['result']
                if set(result) != {'job_uid', 'pod_uid', 'refresh'}:
                    raise ValueError
                for field in ('job_uid', 'pod_uid'):
                    if str(UUID(result[field])) != result[field] or not UUID(result[field]).int:
                        raise ValueError
                validate_participant_refresh_report(request, result['refresh'])
            for document, phase, directory in _parts(request, root / 'commands'):
                path = directory / 'stage.json'
                relative = str(path.relative_to(root))
                if name in steps:
                    allowed.add(relative)
                item = self._stage(path, document=document,
                    revision=digest({'documents': {_key(document): document}, 'binding': asdict(binding)}),
                    phase=phase, dispatched=relative in intents, required=relative in intents)
                if item is not None and name not in steps and document['kind'] != 'ConfigMap':
                    raise ValueError
                if name in steps and steps[name]['status'] == 'complete':
                    if item is None or item['status'] != 'created':
                        raise ValueError
                    if document['kind'] == 'Job' and steps[name]['result'].get('job_uid') != item['uid']:
                        raise ValueError
        request_catalog = ParticipantCatalogRequest(retained.release, retained.inputs.manager, retained.inputs.catalog)
        catalog, manager, identity = _projection(request_catalog)
        path = root / 'catalog/catalog/stage.json'
        relative = str(path.relative_to(root))
        if 'catalog' in steps:
            allowed.add(relative)
        item = self._stage(path, document=catalog,
            revision=digest({'documents': {_key(catalog): catalog}, 'binding': asdict(binding)}),
            phase='participant-catalog', dispatched=relative in intents, required=relative in intents)
        if item is not None and 'catalog' not in steps:
            raise ValueError
        if steps.get('catalog', {}).get('status') == 'complete' and (item is None or item['status'] != 'created'):
            raise ValueError
        path = root / 'catalog/manager/transition.json'
        relative = str(path.relative_to(root))
        if 'manager' in steps:
            allowed.add(relative)
        item = self._transition(path, original=retained.inputs.manager, target=manager,
            identity=identity, phase='control', candidate=False, required=relative in intents)
        if item is not None and ('manager' not in steps or (item['status'] == 'applied' and relative not in intents)):
            raise ValueError
        if steps.get('manager', {}).get('status') == 'complete' and (
                item is None or item['status'] != 'applied' or steps['manager']['result'] != {'status': 'ready'}):
            raise ValueError
        if not set(intents) <= allowed:
            raise ValueError


class HTTPSParticipantResourceObserver(ManagementKubernetesTransport):
    """Exact source/receipt-selected resources over authenticated, read-only HTTPS."""

    def __init__(self, *, retained: RetainedParticipantRollout, api_server: str,
            ssl_context: ssl.SSLContext, token: str | None = None):
        self.retained = copy.deepcopy(retained)
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)
        self._source()

    def _source(self) -> None:
        retained = self.retained
        if (self.api_server.rstrip('/') != retained.inputs.config['kubernetes_api_server'].rstrip('/')
                or load_participant_rollout(reference=retained.reference, state_root=retained.state_dir.parent,
                    anchor_dir=retained.anchor_dir) != retained):
            raise ValueError('participant observation source differs; preserve evidence')

    def _cluster(self) -> None:
        row = self._request('GET', '/api/v1/namespaces/kube-system')
        if (row is None or row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace'
                or row['metadata'].get('name') != 'kube-system'
                or _uid(row) != self.retained.release.migration.registration.binding.kube_system_uid):
            raise ValueError

    def observe(self, *, preparing: str | None = None) -> dict[str, Document]:
        """No default candidate permission: all alternatives require child history.

        Only a fixed candidate API factory may name its currently preparing key.
        This permits that child's first pre-dispatch check before its journal
        exists, not recovery without recorded mutation evidence.
        """
        try:
            self._source()
            view = _ResourceView(self.retained, preparing)
            self._cluster()
            result = {}
            for key, document in view.documents.items():
                actual = self._request('GET', _resource_path(document))
                choices = view.options[key]
                if actual is None:
                    if None not in choices:
                        raise ValueError
                    continue
                uid = _uid(actual)
                if not any(choice is not None and (choice.uid is None or choice.uid == uid)
                        and (_qualified_defaulted(choice.requested, actual) == choice.document if choice.requested is not None else
                            _matches(actual, choice.document, uid)) for choice in choices):
                    raise ValueError
                result[key] = actual
            for original in result.values():
                current = self._request('GET', _resource_path(original))
                if current is None or _uid(current) != _uid(original) or _stable(current) != _stable(original):
                    raise ValueError
            for key in view.documents.keys() - result.keys():
                if self._request('GET', _resource_path(view.documents[key])) is not None:
                    raise ValueError
            self._cluster()
            self._source()
            final = _ResourceView(self.retained, preparing)
            if final.files != view.files or final.options != view.options:
                raise ValueError
            return result
        except Exception:
            raise ValueError('participant live resources unqualified; preserve evidence') from None
