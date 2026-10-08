"""Fixed fresh-runtime SQL delivery before any retained workload is changed.

The protected parent owns publication qualification and its phase-start anchor.
This stage deliberately requires the original completed foundation/manager and
closed pool. After successor patches, the parent must validate the completed
stage against its preserved receipt and phase-aware successor evidence, not
replay initial-installation qualification against changed workloads.
"""
from __future__ import annotations

import copy
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_management_foundation import HTTPSRetainedDevelopmentFoundation
from scripts.ops.nebius_development_pool_registration_live import (
    HTTPSRetainedDevelopmentManagementAPI,
)
from scripts.ops.nebius_development_runtime_job import read_runtime_job
from scripts.ops.nebius_development_runtime_setup import (
    DevelopmentDatabaseRuntime,
    database_runtime_documents,
    validate_database_runtime_proof,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import _MARKER, _validate_record

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


def _path(document: dict[str, Any]) -> str:
    kind = document['kind']
    if kind == 'Namespace' and document['apiVersion'] == 'v1':
        return '/api/v1/namespaces'
    version, plural = {'Secret': ('v1', 'secrets'), 'ConfigMap': ('v1', 'configmaps'),
        'ServiceAccount': ('v1', 'serviceaccounts'), 'Deployment': ('apps/v1', 'deployments'),
        'Job': ('batch/v1', 'jobs')}[kind]
    namespace = document['metadata']['namespace']
    if document['apiVersion'] != version or not isinstance(namespace, str):
        raise ValueError('development runtime database resource version differs')
    return ('/api/v1' if version == 'v1' else '/apis/' + version) + '/namespaces/' + namespace + '/' + plural


class HTTPSDevelopmentRuntimeDatabaseAPI(HTTPSRetainedDevelopmentManagementAPI):
    """Only two fixed immutable Secrets and the fixed SQL ConfigMap/Job may POST."""

    phase = 'development-runtime-database'

    def __init__(self, *, request: DevelopmentDatabaseRuntime, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None,
                 private_files: dict[Path, bytes] | None = None):
        documents = database_runtime_documents(request)
        self.runtime = copy.deepcopy(request)
        super().__init__(retained=request.manager.retained.request.retained, api_server=api_server,
            ssl_context=ssl_context, token=token, private_files=private_files)
        self.documents = documents
        self.foundation = HTTPSRetainedDevelopmentFoundation(api_server=api_server, ssl_context=ssl_context, token=token)

    def __exit__(self, *args: object) -> None:
        try:
            self.foundation.__exit__(*args)
        finally:
            super().__exit__(*args)

    def _private_inputs(self) -> None:
        # Retain every constraint: a current input path must never shadow a
        # different historical pin merely by appearing later in a merged map.
        files = (*self.runtime.manager.retained.files.items(), *self.runtime.foundation.files.items(),
            *self.private_files.items())
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files):
            raise ValueError('development runtime database inputs changed')

    def verify_identity(self, binding: ManagementBinding) -> None:
        try:
            super().verify_identity(binding)
            self.foundation.verify_retained(reference=self.retained.inputs.prerequisites.foundation)
            for expected in self.runtime.manager.retained.resources.values():
                actual = self._request('GET', _path(expected) + '/' + expected['metadata']['name'])
                if actual is None or _uid(actual) != _uid(expected) or _snapshot(actual) != _snapshot(expected):
                    raise ValueError
            self._private_inputs()
        except Exception:
            raise ValueError('development runtime database prerequisites unqualified') from None

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            desired = copy.deepcopy(document)
            expected = self.documents[_key(desired)]
            annotations = desired['metadata'].get('annotations', {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or not UUID(operation).int:
                    raise ValueError
            if not annotations and 'annotations' not in expected['metadata']:
                desired['metadata'].pop('annotations', None)
            if desired != expected:
                raise ValueError
            return _path(expected)
        except Exception:
            raise ValueError('resource outside fixed development runtime database stage') from None

    def _recorded(self, state_dir: Path) -> dict[str, dict[str, Any]]:
        self.verify_identity(self.binding)
        record = _json(private_state._private_read(state_dir / 'stage.json', limit=4 * 1024**2))
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(self.binding),
            'revision': digest(self.documents), 'phase': self.phase}, self.documents)
        result = {}
        for key, item in record['resources'].items():
            actual = self.get_resource(item['desired'])
            if (item['status'] != 'created' or actual is None or _uid(actual) != item['uid']
                    or _snapshot(actual) != item['observed']):
                raise ValueError('development runtime database staged resource differs')
            result[key] = actual
        self.verify_identity(self.binding)
        return result

    def database_report(self, state_dir: Path) -> dict[str, Any] | None:
        """GET-only proof of the fixed Job's sole unrestarted successful Pod."""
        try:
            return read_runtime_job(client=self.client, read=lambda path: self._request('GET', path),
                recorded=lambda: self._recorded(state_dir), private_inputs=self._private_inputs,
                region=self.runtime.foundation.inputs.config['region'], report_field='database',
                validate=lambda proof: validate_database_runtime_proof(self.runtime, state_dir, proof))
        except Exception:
            raise ValueError('development runtime database execution unqualified; preserve evidence') from None
