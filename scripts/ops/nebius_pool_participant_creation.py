"""Create-only candidate and original-backup resources for the release parent.

The existing stage engine retains intent/defaults/UID and only reads an unknown
create outcome. This child does not prove execution, authorize failed-Job retry,
or replace the parent's independent dispatch history and phase/guard census.
"""
from __future__ import annotations

import copy
import hashlib
import ssl
from collections.abc import Callable
from dataclasses import asdict
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import _MARKER, ManagementStageAPI, _stage_fixed_documents
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_operation import (
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import _KINDS, _resource_path

from loom.nebius_platform_render import digest


def _creation(retained: RetainedParticipantRollout, key: str) -> tuple[dict[str, Any], str]:
    if key == 'pre-upgrade-backup':
        namespace = retained.inputs.config['namespace']
        original, = (row for row in retained.inputs.current
            if _key(row) == 'CronJob:' + namespace + ':loom-platform-backup')
        template = copy.deepcopy(original['spec']['jobTemplate'])
        metadata = template.get('metadata', {})
        if metadata.get('ownerReferences') or metadata.get('deletionTimestamp'):
            raise ValueError
        document = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {
            'name': 'loom-predeploy-' + retained.reference.operation_id.hex, 'namespace': namespace,
            **{field: metadata[field] for field in ('labels', 'annotations') if field in metadata}},
            'spec': template['spec']}
        document['metadata'].setdefault('annotations', {})['loom.nebius/participant-backup-source-uid'] = _uid(original)
    else:
        candidate, = (row for rows in retained.release.files.values() for row in rows if _key(row) == key)
        document = _snapshot(candidate)
    if (document['kind'] not in (_KINDS - {'Namespace'}) | {'Job'}
            or _MARKER in document['metadata'].get('annotations', {})
            or any(_key(row) == _key(document) for row in retained.inputs.current)):
        raise ValueError
    _resource_path(document)
    return document, digest({'reference': retained.reference.model_dump(mode='json'), 'key': key, 'document': document})


def stage_participant_creation(*, retained: RetainedParticipantRollout, key: str,
        api: ManagementStageAPI) -> dict[str, Any]:
    """Create one fixed resource or reconcile its original uncertain result.

    The returned UID receipt is not Job completion or application readiness.
    Existing names are never adopted, patched, deleted, or implicitly retried.
    """
    try:
        retained = copy.deepcopy(retained)
        if load_participant_rollout(reference=retained.reference, state_root=retained.state_dir.parent,
                anchor_dir=retained.anchor_dir) != retained:
            raise ValueError
        document, identity = _creation(retained, key)
        root = retained.state_dir / 'candidate-creations'
        binding = retained.release.migration.registration.binding
        with private_state._locked_state(root):
            return _stage_fixed_documents(documents={_key(document): document},
                revision=digest({'creation': identity, 'binding': asdict(binding)}), phase='participant-creation',
                binding=binding, api=api, state_dir=root / hashlib.sha256(key.encode()).hexdigest())
    except Exception:
        raise ValueError('participant creation unqualified; preserve evidence') from None


class HTTPSParticipantCreationAPI(ManagementKubernetesTransport):
    """POST only the retained candidate object or its original predeploy backup."""

    def __init__(self, *, retained: RetainedParticipantRollout, key: str, api_server: str,
            ssl_context: ssl.SSLContext, qualify_creation: Callable[[RetainedParticipantRollout, str], None],
            token: str | None = None):
        try:
            self.retained, self.key = copy.deepcopy(retained), key
            self.document, self.input_digest = _creation(self.retained, key)
            self.binding = self.retained.release.migration.registration.binding
            if (api_server.rstrip('/') != self.retained.inputs.config['kubernetes_api_server'].rstrip('/')
                    or not callable(qualify_creation)):
                raise ValueError
            self.qualify_creation = qualify_creation
            self._private_inputs()
            super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)
        except Exception:
            raise ValueError('participant creation connection unqualified') from None

    def _private_inputs(self) -> None:
        current = load_participant_rollout(reference=self.retained.reference, state_root=self.retained.state_dir.parent,
            anchor_dir=self.retained.anchor_dir)
        document, identity = _creation(current, self.key)
        if current != self.retained or document != self.document or identity != self.input_digest:
            raise ValueError('participant creation input changed')

    def verify_identity(self, binding: ManagementBinding) -> None:
        if binding != self.binding:
            raise ValueError('participant creation binding changed')
        self._private_inputs()
        self.qualify_creation(copy.deepcopy(self.retained), self.key)
        self._private_inputs()

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            value = copy.deepcopy(document)
            annotations = value['metadata'].get('annotations', {})
            marker = annotations.pop(_MARKER, None)
            if writing or marker is not None:
                if str(UUID(marker)) != marker or not UUID(marker).int:
                    raise ValueError
            if not annotations and 'annotations' not in self.document['metadata']:
                value['metadata'].pop('annotations', None)
            if value != self.document:
                raise ValueError
            return _resource_path(value)
        except Exception:
            raise ValueError('participant creation outside fixed resource') from None

    def get_resource(self, document: dict[str, Any]) -> dict[str, Any] | None:
        path = self._approved(document)
        self.verify_identity(self.binding)
        return self._request('GET', path)

    def default_resource(self, document: dict[str, Any]) -> dict[str, Any]:
        path = self._approved(document, writing=True).rsplit('/', 1)[0]
        self.verify_identity(self.binding)
        result = self._request('POST', path + '?dryRun=All', document=document)
        assert result is not None
        return result

    def create_resource(self, document: dict[str, Any]) -> None:
        path = self._approved(document, writing=True).rsplit('/', 1)[0]
        self.verify_identity(self.binding)
        self._request('POST', path, document=document)

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('participant creation does not inspect or allocate storage')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('participant creation does not inspect or allocate storage')
