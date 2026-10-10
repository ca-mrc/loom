"""GET-only retained data and phase-aware workload inventory for the dev parent.

This is not cloud/process/SQL qualification or admission authority. It reads
completed and pending parent history without replaying the original installer.
"""
from __future__ import annotations

import copy
import ssl
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_bootstrap import _secret_documents
from scripts.ops.nebius_development_install import DevelopmentInstallRequest, _storage_observation
from scripts.ops.nebius_development_management_foundation import HTTPSRetainedDevelopmentFoundation
from scripts.ops.nebius_development_pool_registration_live import (
    HTTPSRetainedDevelopmentManagementAPI,
)
from scripts.ops.nebius_development_runtime_install import (
    DevelopmentRuntimeInstallRequest,
    _child_path,
    _read,
    _runtime_history_view,
    prepare_runtime_install,
)
from scripts.ops.nebius_development_runtime_live import _PATHS
from scripts.ops.nebius_development_stage import DevelopmentStageInput
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_material import _documents as management_material
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom_service.environment_management.candidates import _json

_READ_PATHS = {**_PATHS, 'Namespace': ('v1', 'namespaces', True), 'Service': ('v1', 'services', False),
    'StatefulSet': ('apps/v1', 'statefulsets', False), 'Ingress': ('networking.k8s.io/v1', 'ingresses', False),
    'ResourceQuota': ('v1', 'resourcequotas', False), 'LimitRange': ('v1', 'limitranges', False)}


class HTTPSDevelopmentRuntimeObserver(ManagementKubernetesTransport):
    def __init__(self, *, request: DevelopmentRuntimeInstallRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None,
                 private_files: dict[Path, bytes] | None = None):
        self.plan = prepare_runtime_install(request)
        self.request, self.private_files = copy.deepcopy(request), dict(private_files or {})
        retained = request.database.manager.retained.request.retained
        if api_server.rstrip('/') != request.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/'):
            raise ValueError('development runtime observation connection differs')
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)
        try:
            self.manager = HTTPSRetainedDevelopmentManagementAPI(retained=retained, api_server=api_server,
                ssl_context=ssl_context, token=token, private_files=self.private_files)
            try:
                self.foundation = HTTPSRetainedDevelopmentFoundation(api_server=api_server, ssl_context=ssl_context, token=token)
            except Exception:
                self.manager.__exit__()
                raise
        except Exception:
            super().__exit__()
            raise

    def __exit__(self, *args: object) -> None:
        try:
            self.foundation.__exit__(*args)
        finally:
            try:
                self.manager.__exit__(*args)
            finally:
                super().__exit__(*args)

    def _private_inputs(self) -> None:
        database = self.request.database
        files = (*database.manager.retained.files.items(), *database.foundation.files.items(), *self.private_files.items())
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in files):
            raise ValueError('development runtime observation inputs changed')

    def _get(self, document: dict[str, Any]) -> dict[str, Any]:
        version, plural, cluster = _READ_PATHS[document['kind']]
        namespace = document['metadata'].get('namespace')
        participant, = self.request.database.manager.retained.request.registration.spec.participants
        allowed = {'loom-dev', self.manager.binding.namespace, participant.execution_namespace.name, participant.build_namespace.name}
        if document['apiVersion'] != version or (namespace is not None if cluster else namespace not in allowed):
            raise ValueError
        path = ('/api/v1' if version == 'v1' else '/apis/' + version) + (
            '' if cluster else '/namespaces/' + namespace) + '/' + plural + '/' + document['metadata']['name']
        self._private_inputs()
        result = self._request('GET', path)
        if result is None:
            raise ValueError
        return result

    def inspect(self, *, state_dir: Path, _prechild_phase: str | None = None,
                _expected_record: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
        """Exact namespace/storage/material roots with recorded workload successors.

        Never require a replaced API/manager to equal its old template. All
        unchanged resources retain their old UID and configuration instead.
        """
        try:
            self._private_inputs()
            database, manager = self.request.database, self.manager.retained
            foundation = database.foundation
            view = _runtime_history_view(request=self.request, plan=self.plan, state_dir=state_dir,
                _prechild_phase=_prechild_phase)
            record, choices, _ = view
            if _expected_record is not None and record != _expected_record:
                raise ValueError
            # Reuse only namespace/storage predicates from the predecessor APIs,
            # never their original-only workload/readiness verification.
            HTTPSManagementStageAPI.verify_identity(self.manager, manager.binding)
            self.manager._verify_storage(manager.binding)
            self.foundation.verify_identity(foundation.binding)
            inputs = foundation.inputs
            selection = DevelopmentStageInput(inputs.config, inputs.candidate, inputs.profile, inputs.keyring, {})
            storage = _storage_observation(DevelopmentInstallRequest(inputs.binding, selection), foundation.binding, self.foundation)
            if storage is None or any(foundation.phases['storage'][key] != value for key, value in storage.items()):
                raise ValueError
            baseline: dict[str, dict[str, Any]] = {}

            def add(document: dict[str, Any], uid: str) -> None:
                value = copy.deepcopy(document)
                value['metadata']['uid'] = uid
                _uid(value)
                key = _key(value)
                if key in baseline and not _matches(baseline[key], value, uid):
                    raise ValueError
                baseline[key] = value

            for name, document in _secret_documents(foundation.bootstrap['material'], inputs.binding,
                    foundation.binding.operation_id).items():
                add(document, foundation.bootstrap['secrets'][name]['uid'])
            for phase, journal in foundation.phases.items():
                if phase != 'storage':
                    for item in journal['resources'].values():
                        add(item['observed'], item['uid'])
            state = Path(manager.operation['state_dir'])
            material = _json(manager.files[state / 'bootstrap/material/material.json'])
            for name, document in management_material(material['material'], manager.binding, material['operation_id']).items():
                add(document, material['resources'][name]['uid'])
            for path, raw in manager.files.items():
                if path.name == 'stage.json' and path.parent.parent == state:
                    for item in _json(raw).get('resources', {}).values():
                        add(item['observed'], item['uid'])
            for document in database.manager.retained.resources.values():
                add(document, _uid(document))
            for phase in self.plan.fixed:
                if record['phases'][phase]['status'] != 'prepared' and phase != _prechild_phase:
                    for item in _read(_child_path(state_dir, phase))['resources'].values():
                        if item['status'] == 'created':
                            add(item['observed'], item['uid'])
            for key, expected in baseline.items():
                if key not in choices and not _matches(self._get(expected), expected, _uid(expected)):
                    raise ValueError
            observed = {}
            for key, options in choices.items():
                actual = self._get(options[0])
                if not any(_matches(actual, expected, _uid(expected)) for expected in options):
                    raise ValueError
                observed[key] = actual
            if _runtime_history_view(request=self.request, plan=self.plan, state_dir=state_dir,
                    _prechild_phase=_prechild_phase) != view:
                raise ValueError
            self._private_inputs()
            return observed
        except Exception:
            raise ValueError('development runtime live inventory unqualified') from None
