"""Connected, read-only qualification for the independent application manager.

Consumes retained dev installation evidence, never staging configuration or the
legacy manager's runtime credentials. Private entry must freeze these settings
and operator files before invoking the create-only installation stages.
"""
from __future__ import annotations

import asyncio
import base64
import copy
import json
import re
import ssl
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid5

import httpx
from kubernetes.utils.quantity import parse_quantity
from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_cloud_scope import (
    ApplicationCloudScope,
    qualify_application_cloud,
)
from scripts.ops.nebius_development_cloud import qualify_development_cloud, qualify_development_disk
from scripts.ops.nebius_development_live import HTTPSDevelopmentInstallationAPI
from scripts.ops.nebius_development_management_foundation import (
    HTTPSRetainedDevelopmentFoundation,
    RetainedDevelopmentReference,
    RetainedDevelopmentState,
    load_retained_foundation,
)
from scripts.ops.nebius_development_management_install import (
    DevelopmentManagementRequest,
    render_installation,
)
from scripts.ops.nebius_development_management_route import (
    DevelopmentManagementRouteSettings,
    HTTPSDevelopmentManagementRoute,
)
from scripts.ops.nebius_development_preflight import (
    _CONTROLLERS,
    HTTPSDevelopmentPreflight,
    _pending_storage,
)
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_capacity import _count, qualify_platform_capacity
from scripts.ops.nebius_management_cloud_scope import (
    ManagementBackupScope,
    _read,
    qualify_backup_material,
)
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_install import ManagementInstallError, ManagementInstallRequest
from scripts.ops.nebius_management_live import backup_client
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_stage import HTTPSManagementStageAPI, _qualified_defaulted
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_application_contract import new_application_registration
from loom.nebius_application_render import render_application
from loom.nebius_environment_render import PlatformEnvelope
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.deployment import RenderedManagement


class DevelopmentManagementPrerequisiteSettings(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    foundation: RetainedDevelopmentReference
    candidate_id: UUID
    cloud: ApplicationCloudScope
    backup: ManagementBackupScope
    backup_quota_name: str = Field(min_length=1, max_length=255)
    backup_quota_unit: Literal['byte', 'bytes', 'B']
    route: DevelopmentManagementRouteSettings

    @field_validator('candidate_id')
    @classmethod
    def non_nil(cls, value: UUID) -> UUID:
        if not value.int:
            raise ValueError('candidate identity must be non-nil')
        return value


class HTTPSDevelopmentManagementPrerequisites(ManagementKubernetesTransport):
    error_type = ManagementInstallError

    def __init__(self, *, settings: DevelopmentManagementPrerequisiteSettings, operator_cloud_credentials: Path,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.settings, self.operator_cloud_credentials = settings, operator_cloud_credentials
        self.ssl_context, self.token = ssl_context, token
        self.diagnostic_stage: str | None = None
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def foundation(self, request: DevelopmentManagementRequest) -> RetainedDevelopmentState:
        retained = load_retained_foundation(self.settings.foundation)
        with HTTPSRetainedDevelopmentFoundation(api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token) as api:
            api.verify(reference=self.settings.foundation, request=request)
        # The retained history used below must be the history just qualified.
        if any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in retained.files.items()):
            raise ManagementInstallError('retained development history changed')
        with HTTPSDevelopmentPreflight(settings=retained.inputs.settings.preflight, api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token) as api:
            api._storage_class(retained.inputs.config)
        return retained

    def inventory(self, api: str, resource: str, kind: str) -> list[dict[str, Any]]:
        return inventory_resources(self._request, api, resource, kind, include_terminal_pods=True)

    def platform_capacity(self, request: DevelopmentManagementRequest, rendered: RenderedManagement) -> int:
        installation = request.deployment.installation
        application, budget = installation.applications, installation.platform_budget
        if application is None or not application.releases:
            raise ManagementInstallError('development application release unavailable')
        identity = uuid5(request.deployment.installation_id, 'render-only-application-capacity')
        registration = new_application_registration(installation.foundation, application.shared,
            application_id=identity, incarnation=identity, owner_user_id=identity, owner_team_id=identity,
            slug='preflight', release_id=application.releases[0].release_id)
        child = render_application(registration, application.releases[0], application.shared,
            installation.foundation, authority=application.authority)
        bounds = child.platform_envelope
        dimensions = ('cpu_millis', 'memory_mib', 'ephemeral_storage_mib')
        if bounds.storage_mib != 0 or any(getattr(bounds, key) <= 0 for key in dimensions):
            raise ManagementInstallError('development application footprint unqualified')
        children = min(getattr(budget, key) // getattr(bounds, key) for key in dimensions)
        if children < 1:
            raise ManagementInstallError('no application fits platform allowance')
        slots = sum(_count(row) for docs in child.files.values() for row in docs if row['kind'] == 'Deployment')
        nodes, pods = self.inventory('v1', 'nodes', 'Node'), self.inventory('v1', 'pods', 'Pod')
        controllers = [row for api, resource, kind in _CONTROLLERS for row in self.inventory(api, resource, kind)]
        if self.inventory('v1', 'replicationcontrollers', 'ReplicationController'):
            raise ManagementInstallError('unsupported platform controller')
        supported = {(api, kind) for api, _, kind in _CONTROLLERS} | {('v1', 'Node')}
        for row in [*pods, *controllers]:
            if any(owner.get('controller') is True and (owner['apiVersion'], owner['kind']) not in supported
                    for owner in row['metadata'].get('ownerReferences', [])):
                raise ManagementInstallError('unobserved platform controller')
        by_key = {(row['kind'], row['metadata']['namespace'], row['metadata']['name']): row for row in controllers}
        for hpa in self.inventory('autoscaling/v2', 'horizontalpodautoscalers', 'HorizontalPodAutoscaler'):
            target, maximum = hpa['spec']['scaleTargetRef'], hpa['spec']['maxReplicas']
            if (target['apiVersion'] != 'apps/v1' or target['kind'] not in {'Deployment', 'StatefulSet', 'ReplicaSet'}
                    or type(maximum) is not int or not 0 < maximum <= 10000):
                raise ManagementInstallError('unqualified platform autoscaling envelope')
            row = by_key[target['kind'], hpa['metadata']['namespace'], target['name']]
            replicas = row['spec'].get('replicas', 1)
            if type(replicas) is not int or not 0 <= replicas <= 10000:
                raise ManagementInstallError('unqualified platform replica envelope')
            row['spec']['replicas'] = max(replicas, maximum)
        planned = [row for docs in rendered.files.values() for row in docs
            if row['kind'] in {'Deployment', 'StatefulSet', 'Job', 'CronJob'}]
        accounting_pods = copy.deepcopy(pods)
        for pod in accounting_pods:
            if pod['metadata'].get('deletionTimestamp'):
                pod['metadata'].pop('ownerReferences', None)
        qualify_platform_capacity(nodes=nodes, pods=accounting_pods, controllers=controllers, planned=planned,
            reserve=PlatformEnvelope(budget.cpu_millis, budget.memory_mib, 0, budget.ephemeral_storage_mib),
            reserve_pods=children * slots)
        claims = self.inventory('v1', 'persistentvolumeclaims', 'PersistentVolumeClaim')
        # Available/retained PVs must not be silently adopted by a new claim.
        for volume in self.inventory('v1', 'persistentvolumes', 'PersistentVolume'):
            if volume['spec'].get('storageClassName') == installation.foundation.platform_config['storage_class']:
                reference = volume['spec'].get('claimRef', {})
                if (volume.get('status', {}).get('phase') != 'Bound'
                        or not all(reference.get(key) for key in ('namespace', 'name', 'uid'))):
                    raise ManagementInstallError('unqualified reusable platform volume')
        return _pending_storage(claims, controllers, planned)

    async def publications(self, request: DevelopmentManagementRequest, http: httpx.AsyncClient) -> None:
        installation = request.deployment.installation
        application = installation.applications
        if application is None or not application.releases:
            raise ManagementInstallError('development application release unavailable')
        catalog = GitHubCandidateCatalog(http, token=request.material['loom-management-publications']['token'],
            publications=list(installation.publications), registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        selected = await catalog.resolve(self.settings.candidate_id)
        if selected.candidate != request.candidate or selected.profile != request.profile:
            raise ManagementInstallError('development management publication differs')
        for release in application.releases:
            bundle = selected if release.release_id == self.settings.candidate_id else await catalog.resolve(release.release_id)
            if (release.source_digest != bundle.candidate['source_archive_sha256']
                    or release.service_image_ref != bundle.candidate['images']['service']['image_ref']
                    or release.web_image_ref != bundle.candidate['images']['web']['image_ref']
                    or release.schema_revision != application.shared.schema_revision):
                raise ManagementInstallError('development application publication differs')

    async def provider_and_publication(self, request: DevelopmentManagementRequest,
                                      retained: RetainedDevelopmentState, pending_storage_mib: int) -> None:
        from nebius.api.nebius.quotas import v1 as quotas
        from nebius.sdk import SDK

        try:
            async with asyncio.timeout(180):
                async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
                    await self.publications(request, http)
                self.diagnostic_stage = 'cloud_identity'
                app = request.deployment.installation.applications
                shared, scope, backup = retained.inputs.settings.cloud, self.settings.cloud, self.settings.backup
                if (app is None or scope.tenant_id != shared.tenant_id or scope.region != shared.region
                        or scope.shared_project_id != shared.object_project_id
                        or scope.provisioning_project_id != app.storage.project_id
                        or scope.data_group_id != app.storage.data_group_id or scope.data_group_id != shared.data_group_id
                        or scope.source_group_id != app.storage.source_group_id or scope.source_group_id != shared.source_group_id
                        or backup.tenant_id != shared.tenant_id or backup.region != shared.region
                        or backup.backup_project_id == scope.provisioning_project_id
                        or backup.backup_account_id in {scope.provisioning_account_id, shared.data_account_id, shared.source_account_id}
                        or backup.backup_group_id in {scope.provisioning_group_id, scope.membership_group_id,
                            shared.data_group_id, shared.source_group_id}
                        or backup.backup_bucket_id in {*shared.data_buckets, shared.source_bucket_id}
                        or request.deployment.backup_bucket in {*shared.data_buckets.values(), shared.source_bucket_name}):
                    raise ValueError()
                storage = retained.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
                material = {key: base64.b64decode(value, validate=True).decode() for key, value in storage.items()}
                before = private_state._private_read(self.operator_cloud_credentials, limit=1024**2)
                sdk = SDK(credentials_file_name=str(self.operator_cloud_credentials),
                    user_agent_prefix='loom-development-management-installer/1.0')
                try:
                    await qualify_development_cloud(sdk=sdk, scope=shared, config=retained.inputs.config,
                        material=material, pending_storage_mib=pending_storage_mib)
                    await qualify_application_cloud(sdk=sdk, scope=scope,
                        credentials_json=request.application_material.cloud_credentials_json,
                        data_buckets=shared.data_buckets, source_bucket=(shared.source_bucket_id, shared.source_bucket_name))
                    backup_bytes = request.deployment.postgres_storage_gi * 1024**3
                    await qualify_backup_material(sdk=sdk, scope=backup, material=request.material['loom-platform-storage'],
                        bucket_name=request.deployment.backup_bucket, backup_bytes=backup_bytes)
                    self.diagnostic_stage = 'backup_quota'
                    row = await _read(quotas.QuotaAllowanceServiceClient(sdk).get_by_name, quotas.GetByNameRequest(
                        parent_id=backup.tenant_id, name=self.settings.backup_quota_name, region=backup.region))
                    if (not row['metadata'].get('id') or row['metadata']['parent_id'] != backup.tenant_id
                            or row['metadata']['name'] != self.settings.backup_quota_name or row['spec']['region'] != backup.region
                            or row['status']['state'] != 'STATE_ACTIVE' or row['status']['service'] != 'storage'
                            or row['status']['usage_state'] not in {'USAGE_STATE_USED', 'USAGE_STATE_NOT_USED'}
                            or row['status']['unit'] != self.settings.backup_quota_unit):
                        raise ValueError()
                    limit, used = int(row['spec'].get('limit', 0)), int(row['status'].get('usage', 0))
                    if (min(limit, used) < 0 or limit - used < backup_bytes
                            or private_state._private_read(self.operator_cloud_credentials, limit=1024**2) != before):
                        raise ValueError()
                finally:
                    await sdk.close()
        except Exception:
            raise ManagementInstallError('development management publication or cloud qualification failed') from None

    def _route(self, request: DevelopmentManagementRequest, *, installed: bool) -> None:
        with HTTPSDevelopmentManagementRoute(settings=self.settings.route, api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token) as api:
            if installed:
                if request.shared_public_route:
                    # The foundation can precede the manager candidate. Prove
                    # its retained source, never the manager's newer image SHA.
                    retained = self.foundation(request)
                    api.verify_public(request, shared_candidate=retained.inputs.candidate['candidate_sha'])
                else:
                    api.verify_public(request)
            else:
                api.preflight(request)

    def _database(self, request: DevelopmentManagementRequest, binding: ManagementBinding,
                  rendered: RenderedManagement, receipt: dict[str, Any]) -> dict[str, Any]:
        object_row = HTTPSDevelopmentInstallationAPI._object
        with HTTPSManagementStageAPI(binding=binding, rendered=rendered, phase='20-database.yaml',
                api_server=self.api_server, ssl_context=self.ssl_context, token=self.token) as api:
            api.verify_identity(binding)
            claim = object_row(api.get_database_claim(), api='v1', kind='PersistentVolumeClaim',
                name='data-loom-postgres-0', namespace=binding.namespace)
            volume = object_row(api.get_database_volume(), api='v1', kind='PersistentVolume',
                name='pvc-' + _uid(claim), namespace=None)
            if (receipt != {'status': 'management_storage_verified', 'pvc_uid': _uid(claim), 'pv_uid': _uid(volume)}
                    or claim['metadata'].get('ownerReferences') or volume['metadata'].get('ownerReferences')
                    or claim['metadata'].get('labels', {}).get('loom.nebius/management-installation') != binding.installation_id
                    or claim.get('status', {}).get('phase') != 'Bound' or volume.get('status', {}).get('phase') != 'Bound'
                    or volume['metadata'].get('annotations', {}).get('pv.kubernetes.io/provisioned-by') != 'compute.csi.nebius.com'):
                raise ValueError()
            spec, physical = claim['spec'], volume['spec']
            requested, capacity = parse_quantity(spec['resources']['requests']['storage']), parse_quantity(physical['capacity']['storage'])
            if (not requested.is_finite() or requested != request.deployment.postgres_storage_gi * 1024**3
                    or not capacity.is_finite() or capacity < requested
                    or spec.get('volumeName') != volume['metadata']['name']
                    or any(spec.get(key) is not None for key in ('dataSource', 'dataSourceRef', 'selector', 'volumeAttributesClassName'))
                    or any(row.get('storageClassName') != request.deployment.installation.foundation.platform_config['storage_class']
                        or row.get('volumeMode', 'Filesystem') != 'Filesystem' or row.get('accessModes') != ['ReadWriteOnce']
                        for row in (spec, physical))
                    or any(physical['claimRef'].get(key) != value for key, value in {
                        'namespace': binding.namespace, 'name': 'data-loom-postgres-0', 'uid': _uid(claim)}.items())
                    or physical['csi'].get('driver') != 'compute.csi.nebius.com'):
                raise ValueError()
            desired = next(row for row in rendered.files['20-database.yaml'] if row['kind'] == 'StatefulSet')
            controller = object_row(api.get_resource(desired), api='apps/v1', kind='StatefulSet',
                name='loom-postgres', namespace=binding.namespace)
            _qualified_defaulted(desired, controller)
            if controller['metadata'].get('ownerReferences') or not HTTPSRetainedDevelopmentFoundation._ready(controller):
                raise ValueError()
            pod = object_row(api._request('GET', '/api/v1/namespaces/' + binding.namespace + '/pods/loom-postgres-0'),
                api='v1', kind='Pod', name='loom-postgres-0', namespace=binding.namespace)
            HTTPSDevelopmentInstallationAPI._owned(pod, controller)
            expected = copy.deepcopy(controller['spec']['template']['spec'])
            expected.setdefault('volumes', []).append({'name': 'data', 'persistentVolumeClaim': {'claimName': 'data-loom-postgres-0'}})
            actual = copy.deepcopy(pod['spec'])
            for row in (expected, actual):
                row['volumes'].sort(key=lambda item: item['name'])
            if not _matches_backup_template(actual, expected):
                raise ValueError()
            node_name = pod['spec']['nodeName']
            if not isinstance(node_name, str) or re.fullmatch(r'computeinstance-[a-z0-9]+', node_name) is None:
                raise ValueError()
            node = object_row(api._request('GET', '/api/v1/nodes/' + node_name), api='v1', kind='Node',
                name=node_name, namespace=None)
            if node['spec'].get('providerID') != 'nebius://' + node_name:
                raise ValueError()
            api.verify_identity(binding)
            # Generic staged snapshots forbid owner references. This Pod is a
            # qualified StatefulSet child, not an independently staged object.
            # Retain that verified owner separately for the second readback.
            pod_snapshot = copy.deepcopy(pod)
            pod_owners = pod_snapshot['metadata'].pop('ownerReferences')
            return {'disk_id': physical['csi']['volumeHandle'], 'capacity_bytes': int(capacity), 'instance_id': node_name,
                'claim_created_at': claim['metadata']['creationTimestamp'], 'volume_created_at': volume['metadata']['creationTimestamp'],
                'resources': {**{row['kind']: {'uid': _uid(row), 'snapshot': _snapshot(row)}
                    for row in (claim, volume, controller, node)},
                    'Pod': {'uid': _uid(pod), 'snapshot': _snapshot(pod_snapshot), 'ownerReferences': pod_owners}}}

    def qualify_storage(self, request: DevelopmentManagementRequest, binding: ManagementBinding,
                        rendered: RenderedManagement, receipt: dict[str, Any]) -> None:
        async def qualify(evidence: dict[str, Any], retained: RetainedDevelopmentState) -> None:
            from nebius.sdk import SDK

            sdk = SDK(credentials_file_name=str(self.operator_cloud_credentials),
                user_agent_prefix='loom-development-management-installer/1.0')
            try:
                await qualify_development_disk(sdk=sdk, scope=retained.inputs.settings.cloud,
                    **{key: value for key, value in evidence.items() if key != 'resources'})
            finally:
                await sdk.close()

        try:
            self.diagnostic_stage = 'database_storage'
            if ((binding.installation_id, binding.namespace, binding.kube_system_uid) != (
                    request.binding.installation_id, request.binding.namespace, request.binding.kube_system_uid)
                    or binding.namespace != 'loom-nebius-management-dev' or rendered != render_installation(request)):
                raise ValueError()
            retained = self.foundation(request)
            before = self._database(request, binding, rendered, receipt)
            identity = private_state._private_read(self.operator_cloud_credentials, limit=1024**2)
            self.diagnostic_stage = 'provider_disk'
            asyncio.run(qualify(before, retained))
            if (private_state._private_read(self.operator_cloud_credentials, limit=1024**2) != identity
                    or self._database(request, binding, rendered, receipt) != before):
                raise ValueError()
            self.diagnostic_stage = None
        except Exception:
            raise ManagementInstallError('development management physical database storage unqualified') from None

    def public_route(self, request: ManagementInstallRequest) -> None:
        if not isinstance(request, DevelopmentManagementRequest):
            raise ManagementInstallError('independent development request required')
        self.foundation(request)
        self._route(request, installed=True)
        self.foundation(request)

    def preflight(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> None:
        try:
            self.diagnostic_stage = 'render'
            if not isinstance(request, DevelopmentManagementRequest) or rendered != render_installation(request):
                raise ValueError()
            self.diagnostic_stage = 'foundation'
            retained = self.foundation(request)
            self.diagnostic_stage = 'platform_capacity'
            pending = self.platform_capacity(request, rendered)
            self.diagnostic_stage = 'publication'
            asyncio.run(self.provider_and_publication(request, retained, pending))
            self.diagnostic_stage = 'backup_access'
            with backup_client(request) as objects:
                response = objects.list_objects_v2(Bucket=request.deployment.backup_bucket, MaxKeys=1)
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 200:
                    raise ValueError()
            self.diagnostic_stage = 'public_route'
            self._route(request, installed=False)
            self.diagnostic_stage = 'foundation_readback'
            self.foundation(request)
            self.diagnostic_stage = None
        except Exception:
            raise ManagementInstallError('development management installation prerequisites unqualified') from None
