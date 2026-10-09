"""Anchored fresh-dev installation through closed registration and stopped gateway.

No staging attachment, physical authority migration, admission opening or running
workload update. Original manager and foundation remain unchanged and usable.
"""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_retained import load_retained_management
from scripts.ops.nebius_development_pool_intent import (
    DevelopmentPoolIntent,
    bind_namespaces,
    namespace_documents,
    prepare_intent,
)
from scripts.ops.nebius_development_pool_registration import DevelopmentPoolRegistrationRequest

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.pool_management.installation import PoolMachineInstallation

_PHASES = ('namespaces', 'registration', 'material', 'configuration', 'workload')


class DevelopmentPoolInstallAPI(Protocol):
    def qualify(self, intent: DevelopmentPoolIntent) -> None: ...
    def inspect_namespaces(self, state: Path) -> None: ...
    def stage_namespaces(self, state: Path) -> dict[str, str]: ...
    def register(self, request: DevelopmentPoolRegistrationRequest) -> dict[str, Any]: ...
    def deliver(self, request: DevelopmentPoolRegistrationRequest, phase: str, state: Path) -> None: ...
    def inspect_delivery(self, request: DevelopmentPoolRegistrationRequest, state: Path) -> None: ...


def _hash(path: Path) -> str:
    return hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest()


def install_development_pool(*, intent: DevelopmentPoolIntent, api: DevelopmentPoolInstallAPI,
                             execute: bool) -> dict[str, Any]:
    try:
        if type(execute) is not bool:
            raise ValueError()
        intent = prepare_intent(reference=intent.reference, catalog=intent.catalog, tokens=intent.tokens)
        retained = load_retained_management(intent.reference)
        original = Path(retained.operation['state_dir']).parent
        state, anchor = original / 'pool-installation', Path(retained.operation['anchor_dir'])
        marker, journal = anchor / 'pool-installation.json', state / 'installation.json'
        identity = {'schema': 'loom.nebius-development-pool-install.v1',
            'operation_id': intent.catalog['operation_id'], 'state_dir': str(state),
            'intent_sha256': digest({'reference': intent.reference.model_dump(mode='json'), 'catalog': intent.catalog})}
        if state != state.resolve() or anchor != anchor.resolve() or not anchor.is_dir():
            raise ValueError()
        base = {'operation_id': identity['operation_id'], 'pool_id': intent.catalog['pool_id'],
            'installation_id': retained.binding.installation_id, 'namespace': retained.binding.namespace,
            'admission_open': False, 'writer_migration_complete': False}

        def path(phase: str) -> Path:
            return (original / 'pool-registration/registration.json' if phase == 'registration'
                else state / phase / 'stage.json')

        def read_record() -> dict[str, Any]:
            if marker.exists() or marker.is_symlink():
                if _json(private_state._private_read(marker)) != identity:
                    raise ValueError()
                record = _json(private_state._private_read(journal, limit=4 * 1024**2))
                if (set(record) != {*identity, 'phases'} or any(record[key] != value for key, value in identity.items())
                        or set(record['phases']) != set(_PHASES)):
                    raise ValueError()
                unfinished = False
                for phase in _PHASES:
                    item = record['phases'][phase]
                    if (set(item) != {'status', 'sha256'} or item['status'] not in {'prepared', 'started', 'complete'}
                            or (unfinished and item['status'] != 'prepared')
                            or (item['status'] == 'complete') != (item['sha256'] is not None)):
                        raise ValueError()
                    if item['status'] == 'prepared':
                        if path(phase).parent.exists() or path(phase).parent.is_symlink():
                            raise ValueError()
                    elif (not path(phase).is_file() or path(phase) != path(phase).resolve()
                            or (item['status'] == 'complete' and _hash(path(phase)) != item['sha256'])):
                        raise ValueError()
                    unfinished |= item['status'] != 'complete'
                return dict(record)
            if (state.exists() or state.is_symlink() or (original / 'pool-registration').exists()
                    or (anchor / 'pool-registration.json').exists()):
                raise ValueError()
            return {**identity, 'phases': {phase: {'status': 'prepared', 'sha256': None} for phase in _PHASES}}

        def qualify_initial_credentials(record: dict[str, Any]) -> None:
            # Eligibility for new writes, not historical evidence validity. A
            # started registration may already have committed before expiry;
            # its existing Job/receipt must remain readable on replay. The SQL
            # transaction independently checks credentials using database time.
            if record['phases']['registration']['status'] == 'prepared':
                now = datetime.now(UTC)
                machines = [PoolMachineInstallation.model_validate(row) for row in intent.catalog['machines']]
                if any(not row.issued_at <= now < row.expires_at for row in machines):
                    raise ValueError()

        api.qualify(intent)
        if not execute:
            record = read_record()
            api.inspect_namespaces(state / 'namespaces')
            api.qualify(intent)
            qualify_initial_credentials(record)
            return {**base, 'status': 'development_pool_preflight_qualified'}
        # Registration owns the original anchor's lock; use a distinct fixed lock
        # directory while retaining the parent marker in that original anchor.
        with private_state._locked_state(anchor / 'pool-installation-lock'):
            record = read_record()
            api.inspect_namespaces(state / 'namespaces')
            api.qualify(intent)
            qualify_initial_credentials(record)
            if not marker.exists():
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                private_state._atomic_json(journal, record)
            request = None
            for phase in _PHASES:
                api.qualify(intent)
                qualify_initial_credentials(record)
                item = record['phases'][phase]
                if item['status'] == 'prepared':
                    item['status'] = 'started'
                    private_state._atomic_json(journal, record)
                if phase == 'namespaces':
                    uids = api.stage_namespaces(state / phase)
                    if set(uids) != {row['metadata']['name'] for row in namespace_documents(intent).values()}:
                        raise ValueError()
                    request = bind_namespaces(intent, uids)
                elif phase == 'registration':
                    assert request is not None
                    result = api.register(request)
                    if result['status'] == 'pending_registration':
                        if item['status'] == 'complete':
                            raise ValueError()
                        return {**base, 'status': 'pending_registration'}
                    if result['status'] != 'development_pool_registered_closed':
                        raise ValueError()
                else:
                    assert request is not None
                    api.deliver(request, phase, state / phase)
                api.qualify(intent)
                checksum = _hash(path(phase))
                if item['status'] == 'complete' and item['sha256'] != checksum:
                    raise ValueError()
                item.update(status='complete', sha256=checksum)
                private_state._atomic_json(journal, record)
            api.inspect_namespaces(state / 'namespaces')
            assert request is not None
            for phase in _PHASES:
                if _hash(path(phase)) != record['phases'][phase]['sha256']:
                    raise ValueError()
            api.inspect_delivery(request, state)
            api.qualify(intent)
            return {**base, 'status': 'development_pool_installed_closed'}
    except Exception:
        raise ValueError('development pool installation unqualified; preserve retained evidence') from None
