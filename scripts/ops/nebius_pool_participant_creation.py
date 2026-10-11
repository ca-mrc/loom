"""Create-only candidate and original-backup resources for the release parent.

The existing stage engine retains intent/defaults/UID and only reads an unknown
create outcome. Creation receipts are not execution proof; the separate read-only
Job observer qualifies owned terminal processes. Neither authorizes failed-Job
retry or replaces the parent's independent dispatch history and phase/guard census.
"""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from collections.abc import Callable
from dataclasses import asdict
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    _MARKER,
    ManagementStageAPI,
    _qualified_defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_operation import (
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import _KINDS, _resource_path
from scripts.ops.nebius_pool_role_fencing import _terminal_platform_job
from scripts.ops.nebius_shared_pool_recovery_collector import _owned, _pod_spec

from loom.nebius_platform_render import digest


def _job_outcome(job: dict[str, Any], collection: dict[str, Any]) -> dict[str, Any] | None:
    """Complete inventory plus retained Job ancestry; no new execution authority."""
    metadata = collection['metadata']
    if (collection.get('apiVersion') != 'v1' or collection.get('kind') != 'PodList'
            or metadata.get('continue') or not isinstance(metadata.get('resourceVersion'), str)
            or not 0 < len(metadata['resourceVersion']) <= 128
            or not isinstance(collection.get('items'), list) or len(collection['items']) > 1000):
        raise ValueError
    uid, namespace = _uid(job), job['metadata']['namespace']
    template = job['spec']['template']
    pods = collection['items']
    identities = {uid: (namespace, template['spec'].get('serviceAccountName', 'default'))}
    owned = []
    selector = job['spec']['selector']['matchLabels']
    for pod in pods:
        pod_uid = _uid(pod)
        if (pod.get('apiVersion') != 'v1' or pod.get('kind') != 'Pod'
                or pod['metadata']['namespace'] != namespace or pod_uid in identities):
            raise ValueError
        identities[pod_uid] = (namespace, pod['spec'].get('serviceAccountName', 'default'))
        belongs = _owned(pod, job)
        selected = all(pod['metadata'].get('labels', {}).get(key) == value for key, value in selector.items())
        if selected != belongs:
            raise ValueError
        if belongs:
            if not _pod_spec(pod['spec'], template['spec'], scheduled=True):
                raise ValueError
            owned.append(pod)
    status = job.get('status', {})
    conditions = status.get('conditions', [])
    if (len({row['type'] for row in conditions}) != len(conditions)
            or any(type(status.get(field, 0)) is not int or status.get(field, 0) < 0
                for field in ('active', 'terminating', 'succeeded', 'failed'))):
        raise ValueError
    terminal = [row['type'] for row in conditions if row['type'] in {'Complete', 'Failed'} and row.get('status') == 'True']
    if len(terminal) > 1:
        raise ValueError
    if not terminal or not owned or not _terminal_platform_job(job, pods, identities):
        return None
    if terminal == ['Complete']:
        successes = [pod for pod in owned if pod['status']['phase'] == 'Succeeded']
        if len(successes) != 1 or status.get('succeeded') != 1:
            raise ValueError
        for field in ('containerStatuses', 'initContainerStatuses', 'ephemeralContainerStatuses'):
            if any(row['state']['terminated']['exitCode'] != 0 for row in successes[0]['status'].get(field, [])):
                raise ValueError
    elif status.get('succeeded', 0) != 0 or any(pod['status']['phase'] != 'Failed' for pod in owned):
        raise ValueError
    return {'status': 'complete' if terminal == ['Complete'] else 'failed', 'job_uid': uid,
        'job_name': job['metadata']['name'], 'pod_uids': sorted(_uid(pod) for pod in owned)}


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

    def job_outcome(self) -> dict[str, Any] | None:
        """Observe this recorded Job and its terminal processes, without writes.

        Failed is positive terminal proof, not permission to retry. None is an
        unfinished/unproven outcome; only the parent can decide its next phase.
        """
        try:
            self.verify_identity(self.binding)
            document, identity = _creation(self.retained, self.key)
            if document['kind'] != 'Job':
                raise ValueError
            path = self.retained.state_dir / 'candidate-creations' / hashlib.sha256(self.key.encode()).hexdigest() / 'stage.json'
            raw = private_state._private_read(path, limit=4 * 1024**2)
            record = json.loads(raw)
            _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
                'revision': digest({'creation': identity, 'binding': asdict(self.binding)}), 'phase': 'participant-creation'},
                {_key(document): document})
            item = record['resources'][_key(document)]
            if item['status'] != 'created':
                raise ValueError

            def observed_job() -> dict[str, Any]:
                job = self.get_resource(item['desired'])
                if (job is None or _uid(job) != item['uid'] or _snapshot(job) != item['observed']
                        or _qualified_defaulted(item['desired'], job) != item['expected']):
                    raise ValueError
                return job

            job = observed_job()
            collection = self._request('GET', '/api/v1/namespaces/' + document['metadata']['namespace'] + '/pods?limit=1000')
            if collection is None:
                raise ValueError
            outcome = _job_outcome(job, collection)
            final = observed_job()
            if final.get('status', {}) != job.get('status', {}):
                raise ValueError
            self.verify_identity(self.binding)
            if private_state._private_read(path, limit=4 * 1024**2) != raw:
                raise ValueError
            return outcome
        except Exception:
            raise ValueError('participant Job outcome unqualified; preserve evidence') from None

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('participant creation does not inspect or allocate storage')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('participant creation does not inspect or allocate storage')
