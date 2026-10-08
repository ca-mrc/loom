"""Fixed HTTPS registration in the original dev manager's database only."""
from __future__ import annotations

import copy
import ssl
from pathlib import Path
from typing import Any

from kubernetes.utils.quantity import parse_quantity
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_pool_registration import DevelopmentPoolRegistrationRequest
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_material import _documents as material_documents
from scripts.ops.nebius_pool_registration import HTTPSPoolRegistrationAPI

from loom_service.environment_management.candidates import _json


class HTTPSDevelopmentPoolRegistrationAPI(HTTPSPoolRegistrationAPI):
    """All writes remain restricted to the existing registration ConfigMap/Job.

    Namespace-local Secret and database snapshots prevent redirecting the fixed
    registration command into staging or another metadata database. No arbitrary
    query, manifest, cloud identity, runtime role or execution token is accepted.
    """

    def __init__(self, *, request: DevelopmentPoolRegistrationRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.development_request = request
        retained = request.retained
        config = retained.inputs.deployment.installation.foundation.platform_config
        if (request.registration.binding.namespace != 'loom-nebius-management-dev'
                or api_server.rstrip('/') != config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('development pool registration connection differs')
        state = Path(retained.operation['state_dir'])
        material = _json(retained.files[state / 'bootstrap/material/material.json'])
        bootstrap = material_documents(material['material'], retained.binding, material['operation_id'])
        secret = bootstrap['loom-platform-db']
        secret['metadata']['uid'] = material['resources']['loom-platform-db']['uid']
        self.prerequisites = {'/api/v1/namespaces/' + retained.binding.namespace + '/secrets/loom-platform-db': secret}
        for phase, kind, name, prefix, plural in (
                ('database', 'Service', 'loom-postgres', '/api/v1', 'services'),
                ('database', 'StatefulSet', 'loom-postgres', '/apis/apps/v1', 'statefulsets'),
                ('service', 'Deployment', 'loom-service', '/apis/apps/v1', 'deployments')):
            record = _json(retained.files[state / phase / 'stage.json'])
            item = record['resources'][kind + ':' + retained.binding.namespace + ':' + name]
            document = copy.deepcopy(item['observed'])
            document['metadata']['uid'] = item['uid']
            if (item['status'] != 'created' or document['kind'] != kind
                    or document['metadata']['name'] != name
                    or document['metadata']['namespace'] != retained.binding.namespace):
                raise ValueError('development pool registration prerequisite differs')
            self.prerequisites[prefix + '/namespaces/' + retained.binding.namespace + '/' + plural + '/' + name] = document
        # A Deployment snapshot alone does not pin what its mounted names mean.
        # Use only original direct references, excluding the cluster-maintained
        # projected Kubernetes trust bundle and ingress-only TLS generations.
        references: set[tuple[str, str]] = set()
        for workload in tuple(self.prerequisites.values()):
            if workload['kind'] not in {'Deployment', 'StatefulSet'}:
                continue
            pod = workload['spec']['template']['spec']
            for volume in pod.get('volumes', []):
                for field, kind, key in (('secret', 'Secret', 'secretName'), ('configMap', 'ConfigMap', 'name')):
                    if field in volume:
                        references.add((kind, volume[field][key]))
            for container in [*pod.get('containers', []), *pod.get('initContainers', [])]:
                for variable in container.get('env', []):
                    for field, kind in (('secretKeyRef', 'Secret'), ('configMapKeyRef', 'ConfigMap')):
                        if field in variable.get('valueFrom', {}):
                            references.add((kind, variable['valueFrom'][field]['name']))
        originals = {}
        for name, document in bootstrap.items():
            document['metadata']['uid'] = material['resources'][name]['uid']
            originals[('Secret', name)] = document
        for phase in ('config', 'supplied', 'application-config', 'application-material'):
            record = _json(retained.files[state / phase / 'stage.json'])
            for item in record['resources'].values():
                document = copy.deepcopy(item['observed'])
                if (document['kind'] in {'ConfigMap', 'Secret'}
                        and document['metadata']['namespace'] == retained.binding.namespace):
                    if item['status'] != 'created':
                        raise ValueError('development pool registration material differs')
                    document['metadata']['uid'] = item['uid']
                    originals[(document['kind'], document['metadata']['name'])] = document
        for kind, name in references:
            plural = 'secrets' if kind == 'Secret' else 'configmaps'
            self.prerequisites['/api/v1/namespaces/' + retained.binding.namespace + '/' + plural + '/' + name] = originals[(kind, name)]
        self.storage = _json(retained.files[state / 'storage/stage.json'])
        super().__init__(request=request.registration, api_server=api_server, ssl_context=ssl_context, token=token)

    def _private_inputs(self) -> None:
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw
               for path, raw in self.development_request.retained.files.items()):
            raise ValueError('development pool registration retained files changed')

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        self._private_inputs()
        return super()._request(method, path, document=document)

    def qualify(self, request: DevelopmentPoolRegistrationRequest) -> None:
        if request != self.development_request:
            raise ValueError('development pool registration request differs')
        self.verify_identity(request.registration.binding)

    def _verify_storage(self, binding: ManagementBinding) -> None:
        retained = self.storage
        claim = self._request('GET', '/api/v1/namespaces/' + binding.namespace
            + '/persistentvolumeclaims/data-loom-postgres-0')
        volume = self._request('GET', '/api/v1/persistentvolumes/' + retained['pv_name'])
        for document, kind, uid in ((claim, 'PersistentVolumeClaim', retained['pvc_uid']),
                                   (volume, 'PersistentVolume', retained['pv_uid'])):
            if (document is None or document.get('apiVersion') != 'v1' or document.get('kind') != kind
                    or _uid(document) != uid or document.get('status', {}).get('phase') != 'Bound'):
                raise ValueError()
            _snapshot(document)
        assert claim is not None and volume is not None
        if (claim['metadata']['name'] != 'data-loom-postgres-0'
                or claim['metadata']['namespace'] != binding.namespace
                or claim['metadata'].get('labels', {}).get('loom.nebius/management-installation') != binding.installation_id
                or volume['metadata']['name'] != retained['pv_name']):
            raise ValueError()
        claim_spec, volume_spec = copy.deepcopy(claim['spec']), copy.deepcopy(volume['spec'])
        amount = parse_quantity(claim_spec['resources']['requests']['storage'])
        if not amount.is_finite() or amount <= 0:
            raise ValueError()
        claim_spec['resources']['requests']['storage'] = str(amount.normalize())
        volume_spec['claimRef'].pop('resourceVersion', None)
        if claim_spec != retained['pvc_spec'] or volume_spec != retained['pv_spec']:
            raise ValueError()

    def verify_identity(self, binding: ManagementBinding) -> None:
        try:
            self._private_inputs()
            super().verify_identity(binding)
            for path, expected in self.prerequisites.items():
                actual = self._request('GET', path)
                if actual is None or _uid(actual) != _uid(expected) or _snapshot(actual) != _snapshot(expected):
                    raise ValueError()
            self._verify_storage(binding)
            retained = self.development_request.retained
            identities = {'loom-dev': str(retained.inputs.shared_namespace_uid)}
            for row in self.request.spec.participants:
                for namespace in (row.execution_namespace, row.build_namespace):
                    identities[namespace.name] = str(namespace.uid)
            for name, uid in identities.items():
                actual = self._request('GET', '/api/v1/namespaces/' + name)
                if (actual is None or actual.get('kind') != 'Namespace' or _uid(actual) != uid
                        or actual['metadata']['name'] != name or actual['metadata'].get('deletionTimestamp')
                        or actual['metadata'].get('ownerReferences')):
                    raise ValueError()
        except Exception:
            raise ValueError('development pool registration database or namespace differs') from None
