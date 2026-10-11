"""Retained pool effects for the ordinary deployer, not an operator entry.

This composes the fixed HTTPS Job/catalog engines. The protected caller must
supply fresh source/history/publication, inventory, process and guard checks;
no default qualifier exists. This effect connection alone is not an installed
rollout route or completed release history.
"""
from __future__ import annotations

import copy
import json
import ssl
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import Kubectl
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_pool_participant_catalog import (
    HTTPSParticipantCatalogAPI,
    ParticipantCatalogRequest,
    refresh_participant_manager,
    stage_participant_catalog,
)
from scripts.ops.nebius_pool_participant_job import (
    HTTPSPoolParticipantJobAPI,
    PoolParticipantJobRequest,
    _parts,
    stage_participant_job,
)
from scripts.ops.nebius_pool_participant_operation import (
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_release import PreparedParticipantRelease

from loom_service.pool_management.participant_refresh import (
    ParticipantCapacityReceipt,
    RefreshAction,
)

QualificationPhase = Literal['preflight', 'guarded', 'ready', 'released']
_ORDER = ('fence', 'stage', 'catalog', 'manager', 'open')


class RetainedParticipantPoolAdapter:
    """Execute the selected participant's fixed effects under protected checks.

    Qualification is read-only and must understand the current retained phases.
    No receipt is treated as current live qualification. Child HTTP methods call
    the qualifier again; the existing deployer separately owns its idle guard,
    candidate application readiness and final capacity freshness check.
    """

    def __init__(self, *, retained: RetainedParticipantRollout, api_server: str,
            ssl_context: ssl.SSLContext,
            qualify: Callable[[Kubectl, RetainedParticipantRollout, QualificationPhase], None],
            token: str | None = None):
        self.retained = copy.deepcopy(retained)
        self.api_server, self.ssl_context, self.token = api_server, ssl_context, token
        self.qualifier = qualify
        self.root = self.retained.state_dir / 'pool-refresh'
        self._source()
        if not callable(qualify) or api_server.rstrip('/') != retained.inputs.config['kubernetes_api_server'].rstrip('/'):
            raise ValueError('participant pool adapter binding differs; preserve evidence')

    def _source(self) -> None:
        if (self.root != self.retained.state_dir / 'pool-refresh'
                or self.api_server.rstrip('/') != self.retained.inputs.config['kubernetes_api_server'].rstrip('/')
                or load_participant_rollout(reference=self.retained.reference,
                    state_root=self.retained.state_dir.parent, anchor_dir=self.retained.anchor_dir) != self.retained):
            raise ValueError('participant pool source changed; preserve evidence')

    def _qualify(self, kube: Kubectl, release: PreparedParticipantRelease, phase: QualificationPhase) -> None:
        self._source()
        if release != self.retained.release:
            raise ValueError('participant pool release differs; preserve evidence')
        self.qualifier(kube, copy.deepcopy(self.retained), phase)
        self._source()

    def _paths(self, name: str) -> tuple[Path, ...]:
        if name in {'catalog', 'manager'}:
            return (self.root / 'catalog' / ('catalog/stage.json' if name == 'catalog' else 'manager/transition.json'),)
        action: RefreshAction
        observation: UUID | None = None
        if name.startswith('capacity:'):
            observation = UUID(name.removeprefix('capacity:'))
            if not observation.int or name != 'capacity:' + str(observation):
                raise ValueError
            action = 'capacity'
        elif name in {'fence', 'stage', 'open'}:
            action = name  # type: ignore[assignment]
        else:
            raise ValueError
        request = PoolParticipantJobRequest(self.retained.release, self.retained.inputs.manager, action, observation)
        return tuple(directory / 'stage.json' for _, _, directory in _parts(request, self.root / 'commands'))

    def _record(self, *, create: bool) -> dict[str, Any] | None:
        identity = {'schema': 'loom.nebius-participant-pool-effects.v1',
            'reference': self.retained.reference.model_dump(mode='json')}
        path = self.root / 'dispatch.json'
        anchor = self.retained.anchor_dir / (str(self.retained.reference.operation_id) + '-pool-effects.json')
        present = path.exists() or path.is_symlink()
        anchored = anchor.exists() or anchor.is_symlink()
        if not anchored and (any(self.root.rglob('stage.json')) or any(self.root.rglob('transition.json'))):
            raise ValueError
        if not present:
            if anchored:
                raise ValueError
            if not create:
                return None
            record = {**identity, 'steps': {}, 'intents': []}
            private_state._atomic_json(path, record)
        else:
            record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
        if (set(record) != {*identity, 'steps', 'intents'}
                or any(record[key] != value for key, value in identity.items())
                or not isinstance(record['steps'], dict) or not isinstance(record['intents'], list)
                or ((record['steps'] or record['intents']) and not anchored)):
            raise ValueError
        if anchored:
            if json.loads(private_state._private_read(anchor)) != identity:
                raise ValueError
        elif create:
            private_state._write_private(anchor, json.dumps(identity, sort_keys=True).encode())
            private_state._sync_directory(anchor.parent)
        elif record['steps'] or record['intents']:
            raise ValueError
        allowed_paths: set[str] = set()
        for name, item in record['steps'].items():
            if (set(item) != {'status', 'result'} or item['status'] not in {'started', 'complete'}
                    or (item['status'] == 'started' and item['result'] is not None)
                    or (item['status'] == 'complete' and not isinstance(item['result'], dict))):
                raise ValueError
            self._prerequisites(record, name)
            allowed_paths.update(str(value.relative_to(self.root)) for value in self._paths(name))
        if (any(not isinstance(name, str) or name not in allowed_paths for name in record['intents'])
                or len(set(record['intents'])) != len(record['intents'])):
            raise ValueError
        for name in record['intents']:
            private_state._private_read(self.root / name, limit=4 * 1024**2)
        return record

    @staticmethod
    def _prerequisites(record: dict[str, Any], name: str) -> None:
        previous = _ORDER if name.startswith('capacity:') else _ORDER[:_ORDER.index(name)]
        if any(record['steps'].get(key, {}).get('status') != 'complete' for key in previous):
            raise ValueError
        if name != 'fence' and record['steps']['fence']['result']['refresh']['status'] != 'fenced':
            raise ValueError

    def _begin(self, record: dict[str, Any], name: str) -> None:
        self._prerequisites(record, name)
        if name not in record['steps']:
            record['steps'][name] = {'status': 'started', 'result': None}
            private_state._atomic_json(self.root / 'dispatch.json', record)

    def _intent(self, record: dict[str, Any], path: Path) -> None:
        # Called by the fixed child immediately before actual HTTP dispatch.
        # Its journal must already exist. A crash during earlier effect-free
        # child preparation need not masquerade as lost mutation evidence.
        private_state._private_read(path, limit=4 * 1024**2)
        name = str(path.relative_to(self.root))
        if name not in record['intents']:
            record['intents'].append(name)
            private_state._atomic_json(self.root / 'dispatch.json', record)

    def _complete(self, record: dict[str, Any], name: str, result: dict[str, Any]) -> None:
        item = record['steps'][name]
        if item['status'] == 'complete' and item['result'] != result:
            raise ValueError
        item.update(status='complete', result=copy.deepcopy(result))
        private_state._atomic_json(self.root / 'dispatch.json', record)

    @contextmanager
    def _operation(self, kube: Kubectl, release: PreparedParticipantRelease) -> Iterator[dict[str, Any]]:
        try:
            self._qualify(kube, release, 'guarded')
            with private_state._locked_state(self.root):
                record = self._record(create=True)
                assert record is not None
                yield record
                self._qualify(kube, release, 'guarded')
        except Exception:
            raise ValueError('participant pool operation unqualified; preserve evidence') from None

    def preflight(self, kube: Kubectl, release: PreparedParticipantRelease) -> None:
        try:
            self._qualify(kube, release, 'preflight')
            self._record(create=False)
        except Exception:
            raise ValueError('participant pool preflight unqualified; preserve evidence') from None

    def under_guard(self, kube: Kubectl, release: PreparedParticipantRelease) -> None:
        try:
            self._qualify(kube, release, 'guarded')
            self._record(create=False)
        except Exception:
            raise ValueError('participant pool guard unqualified; preserve evidence') from None

    def qualify_completion(self, kube: Kubectl, release: PreparedParticipantRelease, *, released: bool) -> None:
        self._qualify(kube, release, 'released' if released else 'ready')
        self._record(create=False)

    def _job(self, kube: Kubectl, record: dict[str, Any], action: RefreshAction,
            observation_id: UUID | None = None) -> dict[str, Any]:
        name = action if observation_id is None else action + ':' + str(observation_id)
        self._begin(record, name)
        owner = self
        request = PoolParticipantJobRequest(self.retained.release, self.retained.inputs.manager, action, observation_id)
        directory = self.root / 'commands'
        paths = {_key(doc): folder / 'stage.json' for doc, _, folder in _parts(request, directory)}

        class API(HTTPSPoolParticipantJobAPI):
            def verify_identity(self, binding: ManagementBinding) -> None:
                owner._qualify(kube, owner.retained.release, 'guarded')
                super().verify_identity(binding)

            def create_resource(self, document: dict[str, Any]) -> None:
                owner._intent(record, paths[_key(document)])
                super().create_resource(document)

        with API(request=request, api_server=self.api_server, ssl_context=self.ssl_context, token=self.token) as api:
            stage_participant_job(request=request, api=api, state_dir=directory)
            deadline = time.monotonic() + 660
            while True:
                report = api.refresh_report(directory)
                if report is not None:
                    self._complete(record, name, report)
                    return report
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError
                time.sleep(min(5, remaining))

    def _catalog(self, kube: Kubectl, record: dict[str, Any]) -> None:
        owner = self
        request = ParticipantCatalogRequest(self.retained.release, self.retained.inputs.manager, self.retained.inputs.catalog)
        directory = self.root / 'catalog'

        class API(HTTPSParticipantCatalogAPI):
            def verify_identity(self, binding: ManagementBinding) -> None:
                owner._qualify(kube, owner.retained.release, 'guarded')
                super().verify_identity(binding)

            def create_resource(self, document: dict[str, Any]) -> None:
                owner._intent(record, directory / 'catalog/stage.json')
                super().create_resource(document)

            def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
                owner._intent(record, directory / 'manager/transition.json')
                return super().patch_workload(key, before, desired)

        with API(request=request, state_dir=directory, api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token) as api:
            self._begin(record, 'catalog')
            self._complete(record, 'catalog', stage_participant_catalog(request=request, api=api, state_dir=directory))
            self._begin(record, 'manager')
            deadline = time.monotonic() + 300
            while not refresh_participant_manager(request=request, api=api, state_dir=directory):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError
                time.sleep(min(5, remaining))
            self._complete(record, 'manager', {'status': 'ready'})

    def activate(self, kube: Kubectl, release: PreparedParticipantRelease) -> Literal['ready', 'busy']:
        with self._operation(kube, release) as record:
            fence = self._job(kube, record, 'fence')['refresh']['status']
            if fence == 'pending_work':
                if set(record['steps']) != {'fence'}:
                    raise ValueError
                return 'busy'
            if fence != 'fenced' or self._job(kube, record, 'stage')['refresh']['status'] != 'staged':
                raise ValueError
            self._catalog(kube, record)
            return 'ready'

    def finish(self, kube: Kubectl, release: PreparedParticipantRelease) -> None:
        with self._operation(kube, release) as record:
            self._prerequisites(record, 'open')
            self._qualify(kube, release, 'ready')
            if self._job(kube, record, 'open')['refresh']['status'] != 'complete':
                raise ValueError

    def read_capacity(self, kube: Kubectl, release: PreparedParticipantRelease, *,
            observation_id: UUID) -> ParticipantCapacityReceipt:
        with self._operation(kube, release) as record:
            if not isinstance(observation_id, UUID) or not observation_id.int or 'capacity:' + str(observation_id) in record['steps']:
                raise ValueError
            self._qualify(kube, release, 'ready')
            report = self._job(kube, record, 'capacity', observation_id)['refresh']
            return ParticipantCapacityReceipt.model_validate_json(json.dumps(report['capacity']))
