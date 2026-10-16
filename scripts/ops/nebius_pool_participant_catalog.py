"""Fixed catalog-only manager update for the protected participant-rollout parent.

The parent owns source/history/publication qualification, selected-participant
fencing and guard authority. This child preserves its original manager and uses
existing create/CAS journals and full descendant readiness, never blind retries.
"""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_live import HTTPSDevelopmentRuntimeWorkloads
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    advance_runtime_workloads,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    _qualified_defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_release import (
    PoolCatalogRefresh,
    PreparedParticipantRelease,
    catalog_binding,
    prepare_manager_catalog_refresh,
)

from loom.nebius_platform_render import digest


@dataclass(frozen=True, repr=False)
class ParticipantCatalogRequest:
    release: PreparedParticipantRelease | PoolCatalogRefresh
    manager: dict[str, Any]
    catalog: dict[str, Any]


def _projection(request: ParticipantCatalogRequest) -> tuple[dict[str, Any], dict[str, Any], str]:
    projected = prepare_manager_catalog_refresh(release=request.release, manager=request.manager, catalog=request.catalog)
    identity = digest({'refresh': request.release.refresh.model_dump(mode='json'),
        'binding': asdict(catalog_binding(request.release)),
        'manager': request.manager, 'catalog': request.catalog})
    return projected['catalog'], projected['manager'], identity


def stage_participant_catalog(*, request: ParticipantCatalogRequest, api: ManagementStageAPI,
        state_dir: Path) -> dict[str, Any]:
    try:
        request = copy.deepcopy(request)
        catalog, _, _ = _projection(request)
        binding = catalog_binding(request.release)
        documents = {_key(catalog): catalog}
        with private_state._locked_state(state_dir):
            return _stage_fixed_documents(documents=documents,
                revision=digest({'documents': documents, 'binding': asdict(binding)}),
                phase='participant-catalog', binding=binding, api=api, state_dir=state_dir / 'catalog')
    except Exception:
        raise ValueError('participant_catalog_stage_unavailable_preserve_evidence') from None


def refresh_participant_manager(*, request: ParticipantCatalogRequest, api: DevelopmentRuntimeWorkloadAPI,
        state_dir: Path) -> bool:
    """False is known pending readiness/conflict; uncertain effects raise."""
    request = copy.deepcopy(request)
    _, target, identity = _projection(request)
    return advance_runtime_workloads(originals={_key(request.manager): request.manager}, targets={_key(target): target},
        input_digest=identity, phase='control', api=api, state_dir=state_dir / 'manager')


class HTTPSParticipantCatalogAPI(HTTPSDevelopmentRuntimeWorkloads, HTTPSManagementStageAPI):
    """Only the projected ConfigMap and catalog-only manager CAS are writable."""

    def __init__(self, *, request: ParticipantCatalogRequest, state_dir: Path, api_server: str,
            ssl_context: ssl.SSLContext, token: str | None = None):
        self.catalog_request = copy.deepcopy(request)
        catalog, manager, self.input_digest = _projection(self.catalog_request)
        self.binding = catalog_binding(self.catalog_request.release)
        self.documents = {_key(catalog): catalog}
        self.originals = {_key(self.catalog_request.manager): self.catalog_request.manager}
        self.targets = {_key(manager): manager}
        self.state_dir = state_dir
        self.region = self.catalog_request.release.refresh.before.quota_identities['nodes'][1]
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        catalog, manager, identity = _projection(self.catalog_request)
        if (identity != self.input_digest or self.documents != {_key(catalog): catalog}
                or self.targets != {_key(manager): manager}):
            raise ValueError('participant catalog inputs changed')

    def _qualify(self, *, writing: bool) -> None:
        self._private_inputs()
        self.verify_identity(self.binding)
        record = json.loads(private_state._private_read(self.state_dir / 'catalog/stage.json', limit=4 * 1024**2))
        identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
            'revision': digest({'documents': self.documents, 'binding': asdict(self.binding)}), 'phase': 'participant-catalog'}
        _validate_record(record, identity, self.documents)
        for item in record['resources'].values():
            actual = self.get_resource(item['desired'])
            if (item['status'] != 'created' or actual is None or _uid(actual) != item['uid']
                    or _snapshot(actual) != item['observed']):
                raise ValueError('participant catalog differs from recorded immutable input')
        self.verify_identity(self.binding)

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        _qualified_defaulted(self.targets[key], expected)
        actual = self.read_workload(key)
        uid = _uid(self.originals[key])
        if not _matches(actual, expected, uid):
            raise ValueError('participant manager differs from recorded catalog target')
        namespace = self.binding.namespace
        children = self._request('GET', '/apis/apps/v1/namespaces/' + namespace + '/replicasets?limit=1000')
        pods = self._request('GET', '/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        if children is None or pods is None:
            raise ValueError('participant manager descendants unavailable')
        ready = qualify_started_deployment(current=actual, children=children, pods=pods, region=self.region)
        final = self.read_workload(key)
        if (final['metadata']['generation'] != actual['metadata']['generation']
                or _snapshot(final) != _snapshot(actual)):
            raise ValueError('participant manager changed during readiness observation')
        return ready and qualify_started_deployment(current=final, children=children, pods=pods, region=self.region)
