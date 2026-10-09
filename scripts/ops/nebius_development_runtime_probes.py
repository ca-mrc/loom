"""Fixed read-only SQL/process observations for independent development startup.

The parent provides explicit authenticated TLS transport. A private temporary
kubectl configuration contains only that endpoint, trust and bearer; no ambient
kubeconfig, credential plugin or proxy environment is consulted. This adapter
cannot release a guard, register a pool or grant admission/writer authority.
"""
from __future__ import annotations

import base64
import copy
import json
import os
import shutil
import ssl
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_database_readiness import qualify_database_backend, qualify_database_pod
from scripts.ops.nebius_development_install import DevelopmentInstallRequest, _storage_observation
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
from scripts.ops.nebius_development_stage import DevelopmentStageInput
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI
from scripts.ops.nebius_pool_startup_database import (
    pool_startup_closed_sql,
    qualify_startup_closed_report,
)

from loom_service.environment_management.candidates import _json


class HTTPSDevelopmentRuntimeProbes(HTTPSDevelopmentRuntimeObserver):
    def __init__(self, *, request: DevelopmentRuntimeInstallRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str, private_files: dict[Path, bytes] | None = None):
        if not token:
            raise ValueError('development runtime probe credential missing')
        super().__init__(request=request, api_server=api_server, ssl_context=ssl_context,
            token=token, private_files=private_files)
        try:
            executable = shutil.which('kubectl')
            trust = ''.join(ssl.DER_cert_to_PEM_cert(row) for row in ssl_context.get_ca_certs(binary_form=True))
            if executable is None or not trust:
                raise ValueError
            self.scratch = tempfile.TemporaryDirectory(prefix='loom-dev-runtime-probe-')
            try:
                root = Path(self.scratch.name)
                path = root / 'connection.json'
                config = {'apiVersion': 'v1', 'kind': 'Config', 'current-context': 'dev-runtime',
                    'clusters': [{'name': 'dev-runtime', 'cluster': {'server': api_server,
                        'certificate-authority-data': base64.b64encode(trust.encode()).decode()}}],
                    'users': [{'name': 'dev-runtime', 'user': {'token': token}}],
                    'contexts': [{'name': 'dev-runtime', 'context': {'cluster': 'dev-runtime', 'user': 'dev-runtime'}}]}
                raw = json.dumps(config, sort_keys=True).encode()
                private_state._write_private(path, raw)
                self.private_files[path] = raw
                self.prefix = [str(Path(executable).resolve()), '--kubeconfig', str(path),
                    '--request-timeout=30s', '--cache-dir', str(root / 'cache')]
            except Exception:
                self.scratch.cleanup()
                raise
        except Exception:
            super().__exit__()
            raise ValueError('development runtime probe transport unavailable') from None

    def __exit__(self, *args: object) -> None:
        try:
            super().__exit__(*args)
        finally:
            self.scratch.cleanup()

    def _exec(self, pod: dict[str, Any], container: str, command: list[str]) -> dict[str, Any]:
        """Internal fixed-command transport; never accepts an operator command."""
        self._private_inputs()
        value = subprocess.run([*self.prefix, 'exec', '-n', pod['metadata']['namespace'],
            'pod/' + pod['metadata']['name'], '-c', container, '--', *command],
            capture_output=True, timeout=40, check=False, env={'PATH': os.defpath, 'LANG': 'C.UTF-8'})
        self._private_inputs()
        if value.returncode or value.stderr or len(value.stdout) > 65536:
            raise ValueError
        return _json(value.stdout)

    def _read(self, path: str) -> dict[str, Any]:
        self._private_inputs()
        value = self._request('GET', path)
        if value is None:
            raise ValueError
        return value

    def _database(self, namespace: str) -> dict[str, Any]:
        """Retained namespace/PVC/PV, original DB/Service and actual routed Pod."""
        self._private_inputs()
        retained = self.manager.retained
        if namespace == self.manager.binding.namespace:
            HTTPSManagementStageAPI.verify_identity(self.manager, self.manager.binding)
            self.manager._verify_storage(self.manager.binding)
            items = [item for path, raw in retained.files.items() if path.name == 'stage.json'
                for item in _json(raw).get('resources', {}).values()]
        elif namespace == 'loom-dev':
            foundation = self.request.database.foundation
            self.foundation.verify_identity(foundation.binding)
            inputs = foundation.inputs
            selection = DevelopmentStageInput(inputs.config, inputs.candidate, inputs.profile, inputs.keyring, {})
            storage = _storage_observation(DevelopmentInstallRequest(inputs.binding, selection), foundation.binding, self.foundation)
            if storage is None or any(foundation.phases['storage'][key] != value for key, value in storage.items()):
                raise ValueError
            items = list(foundation.phases['database']['resources'].values())
        else:
            raise ValueError
        expected = {}
        for kind in ('StatefulSet', 'Service'):
            item, = (row for row in items if row['observed']['kind'] == kind
                and row['observed']['metadata']['namespace'] == namespace
                and row['observed']['metadata']['name'] == 'loom-postgres')
            expected[kind] = copy.deepcopy(item['observed'])
            expected[kind]['metadata']['uid'] = item['uid']
        database, service = self._get(expected['StatefulSet']), self._get(expected['Service'])
        pod = qualify_database_pod(namespace=namespace, database=database, service=service,
            retained_database=expected['StatefulSet'], retained_service=expected['Service'],
            listing=self._read('/api/v1/namespaces/' + namespace + '/pods?labelSelector=app%3Dloom-postgres&limit=100'))
        qualify_database_backend(namespace=namespace, service=service, pod=pod,
            listing=self._read('/apis/discovery.k8s.io/v1/namespaces/' + namespace
                + '/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dloom-postgres&limit=100'))
        self._private_inputs()
        return pod

    def qualify_closed_pool(self) -> None:
        """One fixed READ ONLY query, bracketed by retained database qualification."""
        try:
            spec = self.request.database.manager.retained.request.registration.spec
            before = self._database(self.manager.binding.namespace)
            report = self._exec(before, 'loom-postgres', ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
                '-U', 'postgres', '-d', 'loom', '-c', pool_startup_closed_sql(spec)])
            qualify_startup_closed_report(spec, report)
            if _uid(self._database(self.manager.binding.namespace)) != _uid(before):
                raise ValueError
        except Exception:
            raise ValueError('development runtime closed database unqualified') from None
