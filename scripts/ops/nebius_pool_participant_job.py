"""Fixed participant-refresh Jobs; the protected parent owns rollout authority.

One immutable request is shared by separately journaled actions. A recorded
mutation is never recreated after an unknown result. Observation receipts are
historical proof only; a parent needing current state must retain a new read.
"""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_release import (
    PreparedParticipantRelease,
    refresh_documents,
)
from scripts.ops.nebius_pool_registration import HTTPSPoolRegistrationAPI

from loom.nebius_platform_render import digest as render_digest
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.participant_refresh import RefreshAction


@dataclass(frozen=True, repr=False)
class PoolParticipantJobRequest:
    release: PreparedParticipantRelease
    manager: dict[str, Any]
    action: RefreshAction
    observation_id: UUID | None = None


def _parts(request: PoolParticipantJobRequest, state_dir: Path) -> tuple[tuple[dict[str, Any], str, Path], ...]:
    config, job = refresh_documents(request.release, manager=request.manager, action=request.action,
        observation_id=request.observation_id)
    return ((config, 'pool-participant-input', state_dir / 'inputs'),
        (job, 'pool-participant-' + request.action, state_dir / 'jobs' / job['metadata']['name']))


def stage_participant_job(*, request: PoolParticipantJobRequest, api: ManagementStageAPI,
        state_dir: Path) -> dict[str, Any]:
    """Retain one shared input and this exact Job, without implicit write retries."""
    try:
        request = copy.deepcopy(request)
        binding = request.release.migration.registration.binding
        parts = _parts(request, state_dir)
        resource_uids = {}
        with private_state._locked_state(state_dir):
            private_state._private_directory(state_dir / 'jobs')
            for document, phase, directory in parts:
                documents = {_key(document): document}
                revision = render_digest({'documents': documents, 'binding': asdict(binding)})
                result = _stage_fixed_documents(documents=documents, revision=revision, phase=phase,
                    binding=binding, api=api, state_dir=directory)
                resource_uids.update(result['resource_uids'])
        return {'status': 'participant_refresh_job_staged', 'job_name': parts[1][0]['metadata']['name'],
            'resource_uids': resource_uids}
    except Exception:
        raise ValueError('participant_refresh_job_unavailable_preserve_evidence') from None


class HTTPSPoolParticipantJobAPI(HTTPSPoolRegistrationAPI):
    """Fixed HTTPS writes and exact Job/Pod/commit readback, not a deployment CLI."""

    def __init__(self, *, request: PoolParticipantJobRequest, api_server: str,
            ssl_context: ssl.SSLContext, token: str | None = None):
        self.refresh_request = copy.deepcopy(request)
        self.documents = {_key(doc): doc for doc, _, _ in _parts(self.refresh_request, Path('.'))}
        self.binding = self.refresh_request.release.migration.registration.binding
        self.region = self.refresh_request.release.refresh.before.quota_identities['nodes'][1]
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _recorded(self, state_dir: Path) -> dict[str, dict[str, Any]]:
        result = {}
        for document, phase, directory in _parts(self.refresh_request, state_dir):
            documents = {_key(document): document}
            record = json.loads(private_state._private_read(directory / 'stage.json', limit=4 * 1024**2))
            identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
                'revision': render_digest({'documents': documents, 'binding': asdict(self.binding)}), 'phase': phase}
            _validate_record(record, identity, documents)
            for item in record['resources'].values():
                self.verify_identity(self.binding)
                actual = self.get_resource(item['desired'])
                if (item['status'] != 'created' or actual is None or _uid(actual) != item['uid']
                        or _snapshot(actual) != item['observed']):
                    raise ValueError
                result[actual['kind']] = actual
        return result

    def _validate_execution_report(self, report: Any) -> None:
        refresh, action = self.refresh_request.release.refresh, self.refresh_request.action
        allowed = {'observe': {'ready', 'fenced', 'staged', 'complete'}, 'fence': {'fenced', 'pending_work'},
            'stage': {'staged', 'pending_work'}, 'open': {'complete', 'pending_work'}}
        if not isinstance(report, dict) or report.get('status') not in allowed[action]:
            raise ValueError
        expected = {'schema_version': 'loom.pool-participant-refresh-receipt.v1',
            'operation_id': str(refresh.after.operation_id), 'participant_id': str(refresh.participant_id),
            'pool_id': str(refresh.after.pool_id), 'status': report['status'],
            'before_sha256': digest(refresh.before.model_dump(mode='json')),
            'after_sha256': digest(refresh.after.model_dump(mode='json'))}
        if report != expected:
            raise ValueError

    def refresh_report(self, state_dir: Path) -> dict[str, Any] | None:
        """A historical committed action; neither fresh capacity nor guard authority."""
        try:
            result = self._execution_report(state_dir)
            if result is None:
                return None
            return {'job_uid': result['job_uid'], 'pod_uid': result['pod_uid'], 'refresh': result['receipt']}
        except Exception:
            raise ManagementStageError('participant refresh execution unqualified; preserve evidence') from None
