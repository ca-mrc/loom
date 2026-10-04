"""Fixed refresh resources; no bootstrap, shared migrations or new permissions.

This is the create/readiness boundary, not authority to run an installation.
The parent owns prerequisite ordering, journal existence and runtime evidence.
"""
from __future__ import annotations

import copy
import json
import re
import ssl
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import ApplicationSetupRequest, HTTPSApplicationSetupAPI
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding, _uuid
from scripts.ops.nebius_management_refresh import render_refresh
from scripts.ops.nebius_management_refresh_switch import MARKER, ManagementRefreshSwitchRequest
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
    _validate_record,
)

from loom.nebius_management_refresh_probe import RefreshProbeSettings
from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import render_management

_PHASES = {'config', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe'}


@dataclass(frozen=True, repr=False)
class ManagementRefreshResourcesRequest:
    switch: ManagementRefreshSwitchRequest
    binding: ManagementBinding
    shared_namespace_uid: str
    manager_revision: str
    target_manager_revision: str


def refresh_documents(request: ManagementRefreshResourcesRequest, phase: str) -> dict[str, dict[str, Any]]:
    """Derive the fixed phase; all credential references remain namespace-local."""
    try:
        operation = request.switch.operation_id
        render = request.switch.render
        _uuid(str(operation))
        _uuid(request.shared_namespace_uid)
        if (phase not in _PHASES or (request.binding.installation_id, request.binding.namespace)
                != (str(render.after.installation_id), render.after.namespace)
                or any(not re.fullmatch(r'[a-zA-Z0-9_]{1,64}', value)
                    for value in (request.manager_revision, request.target_manager_revision))):
            raise ValueError
        refreshed = render_refresh(render)
        if phase == 'config':
            documents = [refreshed.config]
        else:
            rendered = render_management(render.after, candidate=render.candidate, profile=render.profile,
                repo_root=render.repo_root)
            if phase == 'backup':
                cron = rendered.files['80-backup.yaml'][0]
                job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': copy.deepcopy(cron['metadata']),
                    'spec': copy.deepcopy(cron['spec']['jobTemplate']['spec'])}
            else:
                job = copy.deepcopy(rendered.files['30-migrate.yaml'][0])
            name = 'loom-refresh-' + {'post-migration-probe': 'post-probe'}.get(phase, phase) + '-' + operation.hex
            job['metadata']['name'] = name
            job['metadata'].setdefault('annotations', {})[MARKER] = str(operation)
            job['spec']['backoffLimit'] = 0
            job['spec'].pop('ttlSecondsAfterFinished', None)
            documents = [job]
            if phase.endswith('probe'):
                applications = render.after.installation.applications
                assert applications is not None
                mode = 'shared' if phase == 'shared-probe' else 'manager'
                namespace = applications.shared.platform_namespace if mode == 'shared' else request.binding.namespace
                revision = (applications.shared.schema_revision if mode == 'shared' else request.manager_revision
                    if phase == 'manager-probe' else request.target_manager_revision)
                settings = RefreshProbeSettings(mode=mode, namespace=request.binding.namespace,
                    expected_revision=revision, shared=applications.shared)
                job['metadata']['namespace'] = namespace
                job['spec']['activeDeadlineSeconds'] = 180
                template = job['spec']['template']
                template['metadata']['labels']['app'] = 'loom-management-refresh'
                template['metadata'].setdefault('annotations', {})[MARKER] = str(operation)
                pod = template['spec']
                pod['serviceAccountName'] = 'loom-platform'
                pod['automountServiceAccountToken'] = False
                pod.pop('initContainers', None)
                container, = pod['containers']
                container['command'] = ['python', '-m', 'loom.nebius_management_refresh_probe']
                container['env'] = [{'name': 'LOOM_REFRESH_DB_URL', 'valueFrom': {
                    'secretKeyRef': {'name': 'loom-platform-db', 'key': 'service-url'}}}]
                pod['volumes'] = [volume for volume in pod['volumes'] if volume['name'] == 'db-ca']
                container['volumeMounts'] = [mount for mount in container['volumeMounts'] if mount['name'] == 'db-ca']
                pod['volumes'].append({'name': 'refresh-probe', 'configMap': {
                    'name': name, 'items': [{'key': 'probe.json', 'path': 'probe.json'}]}})
                container['volumeMounts'].append({'name': 'refresh-probe',
                    'mountPath': '/var/run/loom-management-refresh', 'readOnly': True})
                config = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': copy.deepcopy(job['metadata']),
                    'immutable': True, 'data': {'probe.json': settings.model_dump_json()}}
                documents.insert(0, config)
        return {_key(document): document for document in documents}
    except Exception:
        raise ManagementStageError('management refresh resource binding differs') from None


def _revision(request: ManagementRefreshResourcesRequest, documents: dict[str, dict[str, Any]]) -> str:
    return digest({'documents': documents, 'shared_namespace_uid': request.shared_namespace_uid,
        'operation_id': str(request.switch.operation_id)})


class HTTPSManagementRefreshResourcesAPI(HTTPSApplicationSetupAPI):
    """Reuse exact-document HTTPS create and both namespace UID checks."""

    def __init__(self, *, request: ManagementRefreshResourcesRequest, phase: str, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None,
                 before_write: Callable[[], None] | None = None):
        documents = refresh_documents(request, phase)
        render = request.switch.render
        setup = ApplicationSetupRequest(render.after, render.candidate, render.profile, request.binding,
            request.shared_namespace_uid, render.repo_root)
        super().__init__(request=setup, phase='config', api_server=api_server, ssl_context=ssl_context, token=token)
        self.documents = documents
        self.before_write = before_write

    def create_resource(self, document: dict[str, Any]) -> None:
        # The stage has persisted its create intent by this point. Dry-run and
        # identity reads happen earlier and must not require a child journal.
        self._approved(document, writing=True)
        if self.before_write is not None:
            self.before_write()
        super().create_resource(document)


def stage_refresh_resources(*, request: ManagementRefreshResourcesRequest, phase: str,
                            api: ManagementStageAPI, state_dir: Path) -> dict[str, Any]:
    documents = refresh_documents(request, phase)
    return _stage_fixed_documents(documents=documents, revision=_revision(request, documents),
        phase='refresh-' + phase, binding=request.binding, api=api, state_dir=state_dir)


def refresh_resources_ready(*, request: ManagementRefreshResourcesRequest, phase: str,
                            api: ManagementStageAPI, state_dir: Path) -> bool:
    """Read recorded objects and Job status only, not probe/backup/public proof."""
    try:
        documents = refresh_documents(request, phase)
        identity = {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(request.binding),
            'revision': _revision(request, documents), 'phase': 'refresh-' + phase}
        with private_state._locked_state(state_dir):
            record = json.loads(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
            _validate_record(record, identity, documents)
            ready = True
            for item in record['resources'].values():
                api.verify_identity(request.binding)
                if item['status'] != 'created':
                    raise ValueError
                actual = api.get_resource(item['desired'])
                if actual is None or _uid(actual) != item['uid'] or _snapshot(actual) != item['observed']:
                    raise ValueError
                if actual['kind'] != 'Job':
                    continue
                status = actual.get('status', {})
                conditions = {row['type']: row['status'] for row in status.get('conditions', [])}
                if conditions.get('Failed') == 'True':
                    raise ManagementStageError('management refresh Job failed; preserve recovery evidence')
                ready &= conditions.get('Complete') == 'True' and status.get('succeeded') == 1
            api.verify_identity(request.binding)
            return ready
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError('management refresh resource evidence unavailable') from None
