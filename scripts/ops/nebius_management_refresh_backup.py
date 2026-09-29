"""Read-only bridge from this refresh's recorded backup Job to off-node bytes.

Reuse the installed backup's execution/log and object-readback contracts. This
adapter cannot create a workload or mint a runtime token; staging belongs to the
parent's earlier fixed-resource phase, under the original installation lock.
"""
from __future__ import annotations

import json
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import HTTPSManagementEvidenceAPI
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_proofs import verify_backup_object
from scripts.ops.nebius_management_refresh_resources import (
    ManagementRefreshResourcesRequest,
    _revision,
    refresh_documents,
)
from scripts.ops.nebius_management_stage import ManagementStageError, _validate_record
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport


class HTTPSManagementRefreshBackupAPI(HTTPSManagementEvidenceAPI):
    """Reuse exact backup evidence reads, with only the refresh's fixed Job scope."""

    def __init__(self, *, request: ManagementRefreshResourcesRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = refresh_documents(request, 'backup')
        self.backup, = self.documents.values()
        self.binding, self.request = request.binding, request
        application = request.switch.render.after.installation.applications
        assert application is not None
        if api_server.rstrip('/') != application.runtime.kubernetes.endpoint:
            raise ManagementStageError('refresh backup endpoint differs')
        self.shared_namespace = application.shared.platform_namespace
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        raise ManagementStageError('refresh backup evidence has no resource write route')

    def runtime_token(self, *, service_account_uid: str) -> str:
        raise ManagementStageError('refresh backup evidence has no token route')

    def verify_identity(self, binding: ManagementBinding) -> None:
        super().verify_identity(binding)
        shared = self._request('GET', '/api/v1/namespaces/' + self.shared_namespace)
        if (shared is None or shared.get('kind') != 'Namespace'
                or shared['metadata'].get('name') != self.shared_namespace
                or _uid(shared) != self.request.shared_namespace_uid
                or shared['metadata'].get('deletionTimestamp') or shared['metadata'].get('ownerReferences')):
            raise ManagementStageError('refresh backup shared namespace identity differs')

    def _recorded_job(self, state_dir: Path) -> dict[str, Any]:
        record = json.loads(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
            'revision': _revision(self.request, self.documents), 'phase': 'refresh-backup'}, self.documents)
        item, = record['resources'].values()
        if item['status'] != 'created':
            raise ValueError
        self.verify_identity(self.binding)
        job = self._request('GET', '/apis/batch/v1/namespaces/' + self.binding.namespace
            + '/jobs/' + self.backup['metadata']['name'])
        if job is None or _uid(job) != item['uid'] or _snapshot(job) != item['observed']:
            raise ValueError
        return job

    def backup_receipt(self, *, state_dir: Path, client: Any) -> dict[str, Any]:
        """Return closed proof only after Job/Pod and HEAD/GET/HEAD all qualify.

        The connected installer supplies the existing explicit-credential,
        endpoint-bound no-retry backup client, never ambient credentials.
        """
        try:
            before = self._recorded_job(state_dir)
            report = self.backup_report(job_uid=_uid(before))
            deployment = self.request.switch.render.after
            receipt = verify_backup_object(client=client, bucket=deployment.backup_bucket,
                namespace=self.binding.namespace, job_uid=_uid(before), report=report,
                max_bytes=deployment.postgres_storage_gi * 1024**3)
            after = self._recorded_job(state_dir)
            if _uid(after) != _uid(before) or _snapshot(after) != _snapshot(before):
                raise ValueError
            return receipt
        except Exception:
            raise ManagementStageError('refresh backup execution or object proof unavailable') from None
