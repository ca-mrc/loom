"""Serial dev-manager certificate successors; never replay the initial installer.

Only the existing manager Ingress TLS Secret reference may change. Actual cloud,
DNS, public TLS and retained-file checks belong to the connected fixed adapter.
"""
from __future__ import annotations

import hashlib
import re
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_retained import RetainedManagementState
from scripts.ops.nebius_development_management_tls import (
    ManagementTLSMaterial,
    deliver_management_tls,
)
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding, _uuid
from scripts.ops.nebius_management_stage import ManagementStageAPI

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json

_PHASES = {'prepared', 'tls_started', 'delivered', 'switch_intent', 'switched', 'complete', 'rejected'}


class RenewalError(RuntimeError):
    """Bounded failure; retain predecessor, Secret and unknown-write evidence."""


@dataclass(frozen=True, repr=False)
class RenewalRequest:
    retained: RetainedManagementState
    material: ManagementTLSMaterial
    operation_id: UUID
    qualification_digest: str


class RenewalAPI(Protocol):
    def verify(self) -> None:
        """Recheck cluster/namespace identity and captured private files."""
        ...

    def read_ingress(self) -> dict[str, Any]: ...
    def qualify_route(self, expected: dict[str, Any]) -> None:
        """Read-only DNS/shared ingress checks, without trusting the old leaf."""
        ...

    def tls(self, material: ManagementTLSMaterial, binding: ManagementBinding) -> AbstractContextManager[ManagementStageAPI]: ...
    def preview(self, before: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]: ...
    def patch(self, before: dict[str, Any], target: dict[str, Any]) -> bool:
        """False only for definite rejection; exceptions may have committed."""
        ...

    def public_ready(self, target: dict[str, Any], fingerprint: str) -> bool: ...


def _stable(value: dict[str, Any]) -> dict[str, Any]:
    result = _snapshot(value)
    result['metadata']['uid'] = _uid(value)
    return result


def _secret_name(installation_id: str, generation: str) -> str:
    return 'loom-management-tls-' + hashlib.sha256((installation_id + ':' + generation).encode()).hexdigest()[:40]


def _target(before: dict[str, Any], installation_id: str, generation: str) -> dict[str, Any]:
    result = _stable(before)
    result['spec']['tls'][0]['secretName'] = _secret_name(installation_id, generation)
    return result


def _read_current(api: RenewalAPI, expected: dict[str, Any]) -> dict[str, Any]:
    value = api.read_ingress()
    if _stable(value) != expected:
        raise RenewalError('development management renewal route differs')
    return value


def _history(path: Path, identity: dict[str, Any], initial: dict[str, Any]) -> dict[str, Any]:
    record = _json(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, 'operations'}
            or any(record[key] != value for key, value in identity.items())
            or not isinstance(record['operations'], list) or not record['operations']):
        raise ValueError()
    previous, seen = initial, set()
    for index, item in enumerate(record['operations']):
        _uuid(item['operation_id'])
        if (set(item) != {'operation_id', 'input_digest', 'generation', 'before', 'target', 'phase',
                         'tls_receipt', 'resource_version'}
                or item['operation_id'] in seen or not re.fullmatch(r'sha256:[0-9a-f]{64}', item['input_digest'])
                or not re.fullmatch(r'[0-9a-f]{64}', item['generation']) or item['before'] != previous
                or item['target'] != _target(previous, identity['binding']['installation_id'], item['generation'])
                or item['target'] == previous or item['phase'] not in _PHASES
                or (index != len(record['operations']) - 1 and item['phase'] not in {'complete', 'rejected'})
                or (item['phase'] in {'prepared', 'tls_started'}) != (item['tls_receipt'] is None)
                or (item['phase'] in {'prepared', 'tls_started', 'delivered'}) != (item['resource_version'] is None)):
            raise ValueError()
        if item['resource_version'] is not None and (not isinstance(item['resource_version'], str)
                or not 0 < len(item['resource_version']) <= 128):
            raise ValueError()
        seen.add(item['operation_id'])
        if item['phase'] == 'complete':
            previous = item['target']
    return record


def renew_management_tls(*, request: RenewalRequest, api: RenewalAPI, execute: bool) -> dict[str, Any]:
    """Preview or resume one exact successor; never repeat an uncertain patch."""
    try:
        retained, material = request.retained, request.material
        binding, operation = retained.binding, retained.operation
        if (not isinstance(request.operation_id, UUID) or not request.operation_id.int
                or not re.fullmatch(r'sha256:[0-9a-f]{64}', request.qualification_digest)
                or binding.namespace != 'loom-nebius-management-dev'
                or material.public_host != retained.inputs.deployment.public_host):
            raise ValueError()
        state = Path(operation['state_dir']).parent / 'tls-renewal'
        anchor = Path(operation['anchor_dir'])
        marker, journal = anchor / 'tls-renewal.json', state / 'renewal.json'
        if not anchor.is_dir() or anchor.is_symlink():
            raise ValueError()
        initial = _stable(retained.ingress)
        identity = {'schema': 'loom.nebius-development-management-renewal.v1', 'binding': asdict(binding),
            'state_dir': str(state), 'original_operation_digest': digest(operation), 'initial_ingress': initial}
        generation = hashlib.sha256(material.chain.encode()).hexdigest()
        input_digest = digest({'operation_id': str(request.operation_id), 'binding': asdict(binding),
            'material': asdict(material), 'qualification_digest': request.qualification_digest})

        def verify() -> None:
            if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in retained.files.items()):
                raise RenewalError('development management retained files changed')
            api.verify()

        with private_state._locked_state(anchor):
            verify()
            if marker.exists() or marker.is_symlink():
                if _json(private_state._private_read(marker)) != identity or not journal.is_file() or journal.is_symlink():
                    raise ValueError()
                record = _history(journal, identity, initial)
            else:
                if state.exists() or state.is_symlink():
                    raise ValueError()
                record = {**identity, 'operations': []}
            rows = record['operations']
            matches = [index for index, row in enumerate(rows) if row['operation_id'] == str(request.operation_id)]
            fresh = not matches
            if matches and matches != [len(rows) - 1]:
                raise RenewalError('development management renewal operation superseded')
            if fresh:
                if rows and rows[-1]['phase'] not in {'complete', 'rejected'}:
                    raise RenewalError('development management previous renewal unresolved')
                before = initial if not rows else rows[-1]['target' if rows[-1]['phase'] == 'complete' else 'before']
                target = _target(before, binding.installation_id, generation)
                if before == target:
                    raise RenewalError('development management certificate generation already active')
                item: dict[str, Any] = {'operation_id': str(request.operation_id), 'input_digest': input_digest,
                    'generation': generation, 'before': before, 'target': target, 'phase': 'prepared',
                    'tls_receipt': None, 'resource_version': None}
            else:
                item = rows[-1]
                if item['input_digest'] != input_digest or item['generation'] != generation:
                    raise RenewalError('development management renewal input differs')
            report = private_state.validate_management_certificate(material.chain.encode(), material.key.encode(),
                management_host=material.public_host)
            expected = item['target'] if item['phase'] in {'switch_intent', 'switched', 'complete'} else item['before']
            if item['phase'] == 'switch_intent':
                # Only exact committed readback resolves an uncertain patch.
                if _stable(api.read_ingress()) != expected:
                    raise RenewalError('development management certificate switch unresolved')
            _read_current(api, expected)
            api.qualify_route(expected)
            verify()
            _read_current(api, expected)
            base = {'installation_id': binding.installation_id, 'operation_id': str(request.operation_id),
                'namespace': binding.namespace, 'fingerprint_sha256': report['fingerprint_sha256']}
            if not execute:
                return {**base, 'status': 'development_management_tls_preflight_qualified'}
            if fresh:
                if not rows:
                    private_state._atomic_json(marker, identity)
                    private_state._private_directory(state)
                rows.append(item)
                private_state._atomic_json(journal, record)

            def save(phase: str) -> None:
                item['phase'] = phase
                private_state._atomic_json(journal, record)

            if item['phase'] == 'rejected':
                return {**base, 'status': 'rejected'}
            tls_state = state / 'generations' / generation
            started_here = item['phase'] == 'prepared'
            if started_here:
                # Retain generation journals across a definite-conflict retry.
                private_state._private_directory(tls_state.parent)
                save('tls_started')
            elif not (tls_state / 'stage.json').is_file() or tls_state.is_symlink():
                raise RenewalError('development management TLS delivery history missing')
            verify()
            with api.tls(material, binding) as tls_api:
                receipt = deliver_management_tls(material=material, binding=binding, api=tls_api, state_dir=tls_state)
            if item['tls_receipt'] is None:
                item['tls_receipt'] = receipt
                save('delivered')
            elif receipt != item['tls_receipt']:
                raise RenewalError('development management TLS delivery differs')
            if item['phase'] == 'delivered':
                verify()
                before = _read_current(api, item['before'])
                preview = api.preview(before, item['target'])
                if _stable(preview) != item['target']:
                    raise ValueError()
                api.qualify_route(item['before'])
                verify()
                # Read after slow preview/identity/DNS checks; this is the exact
                # resourceVersion persisted before the one actual CAS request.
                before = _read_current(api, item['before'])
                version = before['metadata']['resourceVersion']
                if not isinstance(version, str) or not 0 < len(version) <= 128:
                    raise ValueError()
                item['resource_version'] = version
                save('switch_intent')
                try:
                    accepted = api.patch(before, item['target'])
                except Exception:
                    accepted = None
                if accepted is False:
                    save('rejected')
                    return {**base, 'status': 'rejected'}
                if _stable(api.read_ingress()) != item['target']:
                    raise RenewalError('development management certificate switch unresolved')
                save('switched')
            elif item['phase'] == 'switch_intent':
                save('switched')
            api.qualify_route(item['target'])
            ready = api.public_ready(item['target'], report['fingerprint_sha256'])
            verify()
            with api.tls(material, binding) as tls_api:
                if deliver_management_tls(material=material, binding=binding, api=tls_api, state_dir=tls_state) != receipt:
                    raise ValueError()
            _read_current(api, item['target'])
            if not ready:
                return {**base, 'status': 'pending', 'phase': 'public'}
            if item['phase'] != 'complete':
                save('complete')
            return {**base, 'status': 'development_management_tls_renewed'}
    except RenewalError:
        raise
    except Exception:
        raise RenewalError('development management renewal unqualified; preserve private history') from None
