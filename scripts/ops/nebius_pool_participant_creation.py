"""Retained candidate/backup creation, terminal proof and explicit Job retry.

The existing stage engine retains intent/defaults/UID and only reads an unknown
create outcome. Creation receipts are not execution proof; the separate read-only
Job observer qualifies owned terminal processes. Explicit failed-Job recovery
retains each generation and conditionally retires only the proven failed UID and
version. The parent still owns independent dispatch history and phase/guard census.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import ssl
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol
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


class ParticipantCreationAPI(ManagementStageAPI, Protocol):
    @property
    def retained(self) -> RetainedParticipantRollout: ...
    @property
    def key(self) -> str: ...
    def job_outcome(self) -> dict[str, Any] | None: ...
    def retry_pending(self) -> bool: ...
    def retry_failed_job(self) -> bool: ...
    def reconcile_retry(self) -> bool: ...


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
    if not terminal or (terminal == ['Complete'] and not owned) or not _terminal_platform_job(job, pods, identities):
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


def _stage_record(*, document: dict[str, Any], identity: str, directory: Path,
        binding: ManagementBinding) -> tuple[bytes, dict[str, Any]]:
    raw = private_state._private_read(directory / 'stage.json', limit=4 * 1024**2)
    record = json.loads(raw)
    _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(binding),
        'revision': digest({'creation': identity, 'binding': asdict(binding)}), 'phase': 'participant-creation'},
        {_key(document): document})
    item = record['resources'][_key(document)]
    if item['status'] != 'created':
        raise ValueError
    observed = copy.deepcopy(item['observed'])
    observed['metadata']['uid'] = item['uid']
    if _qualified_defaulted(item['desired'], observed) != item['expected']:
        raise ValueError
    return raw, item


def _attempt(retained: RetainedParticipantRollout, key: str) -> tuple[dict[str, Any], str, Path, dict[str, Any] | None]:
    """Load the active generation without replacing any predecessor evidence."""
    document, base = _creation(retained, key)
    root = retained.state_dir / 'candidate-creations' / hashlib.sha256(key.encode()).hexdigest()
    path = root / 'retries.json'
    if not path.exists() and not path.is_symlink():
        if any(root.glob('attempt-*')):
            raise ValueError
        return document, base, root, None
    record: dict[str, Any] = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (document['kind'] != 'Job' or set(record) != {'schema', 'creation', 'attempts'}
            or record['schema'] != 'loom.nebius-participant-job-retries.v1' or record['creation'] != base
            or not isinstance(record['attempts'], list) or not record['attempts']):
        raise ValueError
    identity, directory = base, root
    for index, entry in enumerate(record['attempts']):
        if (set(entry) != {'status', 'stage_sha256', 'job_uid', 'resource_version', 'pod_uids', 'next_started'}
                or entry['status'] not in {'delete_intent', 'retired'} or type(entry['next_started']) is not bool
                or (entry['status'] == 'delete_intent' and entry['next_started'])
                or (index < len(record['attempts']) - 1 and (entry['status'] != 'retired' or not entry['next_started']))
                or not isinstance(entry['stage_sha256'], str) or re.fullmatch('[0-9a-f]{64}', entry['stage_sha256']) is None
                or not isinstance(entry['resource_version'], str) or not 0 < len(entry['resource_version']) <= 128
                or not isinstance(entry['pod_uids'], list) or len(entry['pod_uids']) > 1000
                or entry['pod_uids'] != sorted(set(entry['pod_uids']))):
            raise ValueError
        for uid in [entry['job_uid'], *entry['pod_uids']]:
            if str(UUID(uid)) != uid or not UUID(uid).int:
                raise ValueError
        raw, item = _stage_record(document=document, identity=identity, directory=directory,
            binding=retained.release.migration.registration.binding)
        if hashlib.sha256(raw).hexdigest() != entry['stage_sha256'] or item['uid'] != entry['job_uid']:
            raise ValueError
        if entry['status'] == 'retired':
            directory = root / ('attempt-' + str(index + 1))
            identity = digest({'creation': base, 'generation': index + 1, 'previous_stage_sha256': entry['stage_sha256']})
    return document, identity, directory, record


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
        root = retained.state_dir / 'candidate-creations'
        binding = retained.release.migration.registration.binding
        with private_state._locked_state(root):
            document, identity, directory, retries = _attempt(retained, key)
            if retries is not None:
                entry = retries['attempts'][-1]
                if entry['status'] != 'retired':
                    raise ValueError
                present = (directory / 'stage.json').exists() or (directory / 'stage.json').is_symlink()
                if entry['next_started'] != present:
                    raise ValueError
                if not entry['next_started']:
                    entry['next_started'] = True
                    private_state._atomic_json(directory.parent / 'retries.json', retries)
            return _stage_fixed_documents(documents={_key(document): document},
                revision=digest({'creation': identity, 'binding': asdict(binding)}), phase='participant-creation',
                binding=binding, api=api, state_dir=directory)
    except Exception:
        raise ValueError('participant creation unqualified; preserve evidence') from None


class HTTPSParticipantCreationAPI(ManagementKubernetesTransport):
    """Create the fixed resource; retry Jobs only with retained terminal proof."""

    def __init__(self, *, retained: RetainedParticipantRollout, key: str, api_server: str,
            ssl_context: ssl.SSLContext, qualify_creation: Callable[[RetainedParticipantRollout, str], None],
            token: str | None = None):
        try:
            self.retained, self.key = copy.deepcopy(retained), key
            self.document, self.input_digest, self.creation_dir, _ = _attempt(self.retained, key)
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
        document, identity, directory, _ = _attempt(current, self.key)
        if (current != self.retained or document != self.document or identity != self.input_digest
                or directory != self.creation_dir):
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
        return self._observe_job()[0]

    def _observe_job(self) -> tuple[dict[str, Any] | None, dict[str, Any], bytes]:
        try:
            self.verify_identity(self.binding)
            document, identity, directory, _ = _attempt(self.retained, self.key)
            if document['kind'] != 'Job':
                raise ValueError
            path = directory / 'stage.json'
            raw, item = _stage_record(document=document, identity=identity, directory=directory, binding=self.binding)

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
            return outcome, final, raw
        except Exception:
            raise ValueError('participant Job outcome unqualified; preserve evidence') from None

    def retry_pending(self) -> bool:
        self._private_inputs()
        retries = _attempt(self.retained, self.key)[3]
        return retries is not None and retries['attempts'][-1]['status'] == 'delete_intent'

    def _retry_path(self) -> Path:
        return self.retained.state_dir / 'candidate-creations' / hashlib.sha256(self.key.encode()).hexdigest() / 'retries.json'

    def _reconcile_retry(self, retries: dict[str, Any]) -> bool:
        """Observe foreground cleanup; never delete, create, or adopt an object."""
        self.verify_identity(self.binding)
        entry = retries['attempts'][-1]
        if entry['status'] != 'delete_intent':
            raise ValueError
        job = self.get_resource(self.document)
        if job is not None:
            if _uid(job) != entry['job_uid']:
                raise ValueError
            return False
        namespace = self.document['metadata']['namespace']
        collection = self._request('GET', '/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        if collection is None:
            raise ValueError
        metadata = collection['metadata']
        if (collection.get('apiVersion') != 'v1' or collection.get('kind') != 'PodList'
                or metadata.get('continue') or not isinstance(metadata.get('resourceVersion'), str)
                or not 0 < len(metadata['resourceVersion']) <= 128
                or not isinstance(collection.get('items'), list) or len(collection['items']) > 1000):
            raise ValueError
        _, item = _stage_record(document=self.document, identity=self.input_digest,
            directory=self.creation_dir, binding=self.binding)
        selector = item['observed']['spec']['selector']['matchLabels']
        seen = set()
        remaining = False
        for pod in collection['items']:
            uid = _uid(pod)
            if (pod.get('apiVersion') != 'v1' or pod.get('kind') != 'Pod'
                    or pod['metadata']['namespace'] != namespace or uid in seen):
                raise ValueError
            seen.add(uid)
            if (uid in entry['pod_uids'] or any(row.get('uid') == entry['job_uid']
                    for row in pod['metadata'].get('ownerReferences', []))
                    or all(pod['metadata'].get('labels', {}).get(key) == value for key, value in selector.items())):
                remaining = True
        if self.get_resource(self.document) is not None:
            raise ValueError
        self.verify_identity(self.binding)
        if _attempt(self.retained, self.key)[3] != retries:
            raise ValueError
        if remaining:
            return False
        entry['status'] = 'retired'
        private_state._atomic_json(self._retry_path(), retries)
        return True

    def reconcile_retry(self) -> bool:
        """Read-only cluster recovery of the recorded delete; True selects a new generation."""
        try:
            with private_state._locked_state(self.retained.state_dir / 'candidate-creations'):
                self.verify_identity(self.binding)
                retries = _attempt(self.retained, self.key)[3]
                if retries is None:
                    raise ValueError
                return self._reconcile_retry(retries)
        except Exception:
            raise ValueError('participant Job cleanup unqualified; preserve evidence') from None

    def retry_failed_job(self) -> bool:
        """One explicitly requested retry, guarded by terminal processes and UID/RV.

        An explicit recovery may reissue the identical conditional DELETE after
        an unknown response, only while the same failed UID/version is present.
        Transport and cleanup polling do not retry writes. A changed/replaced
        Job is never deleted; recreation waits for old Job and Pod disappearance.
        """
        try:
            with private_state._locked_state(self.retained.state_dir / 'candidate-creations'):
                self.verify_identity(self.binding)
                document, _, _, retries = _attempt(self.retained, self.key)
                if document['kind'] != 'Job':
                    raise ValueError
                pending = retries is not None and retries['attempts'][-1]['status'] == 'delete_intent'
                if pending:
                    job = self.get_resource(document)
                    if job is None or job['metadata'].get('deletionTimestamp'):
                        assert retries is not None
                        return self._reconcile_retry(retries)
                outcome, job, raw = self._observe_job()
                if outcome is None or outcome['status'] != 'failed':
                    raise ValueError
                version = job['metadata']['resourceVersion']
                if not isinstance(version, str) or not 0 < len(version) <= 128:
                    raise ValueError
                entry = {'status': 'delete_intent', 'stage_sha256': hashlib.sha256(raw).hexdigest(),
                    'job_uid': outcome['job_uid'], 'resource_version': version,
                    'pod_uids': outcome['pod_uids'], 'next_started': False}
                if pending:
                    assert retries is not None
                    if retries['attempts'][-1] != entry:
                        raise ValueError
                else:
                    if retries is None:
                        retries = {'schema': 'loom.nebius-participant-job-retries.v1',
                            'creation': _creation(self.retained, self.key)[1], 'attempts': []}
                    retries['attempts'].append(entry)
                    private_state._atomic_json(self._retry_path(), retries)
                self.verify_identity(self.binding)
                options = {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'propagationPolicy': 'Foreground',
                    'preconditions': {'uid': entry['job_uid'], 'resourceVersion': version}}
                try:
                    with self.client.stream('DELETE', self._approved(document), json=options) as response:
                        if response.status_code not in {200, 202}:
                            raise ValueError
                except Exception:
                    pass  # Reconcile the original intent, never infer success from a response.
                return self._reconcile_retry(retries)
        except Exception:
            raise ValueError('participant Job retry unqualified; preserve evidence') from None

    def get_database_claim(self) -> dict[str, Any] | None:
        raise ValueError('participant creation does not inspect or allocate storage')

    def get_database_volume(self) -> dict[str, Any] | None:
        raise ValueError('participant creation does not inspect or allocate storage')
