"""Read-only qualification for shared-data applications, not full-stack children."""
from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import replace
from typing import Any, Self
from uuid import UUID, uuid5

import httpx
from pydantic import BaseModel, ConfigDict, model_validator
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_cloud_scope import (
    ApplicationCloudScope,
    qualify_application_cloud,
)
from scripts.ops.nebius_management_capacity import _count, qualify_platform_capacity
from scripts.ops.nebius_management_prerequisites import (
    HTTPSManagementPrerequisites,
    ManagementPrerequisiteError,
)
from scripts.ops.nebius_management_upgrade import ManagementUpgradeRequest
from sqlalchemy.engine import make_url

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_application_contract import new_application_registration
from loom.nebius_application_render import render_application
from loom.nebius_environment_render import PlatformEnvelope
from loom_service.application_management.deployment import render_application_setup
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.deployment import render_management


class UpgradePrerequisiteSettings(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    candidate_id: UUID
    cloud: ApplicationCloudScope
    shared_config_uid: UUID
    shared_database_uid: UUID
    shared_auth_uid: UUID
    shared_service_uid: UUID
    bucket_ids: dict[str, str]

    @model_validator(mode='after')
    def bindings(self) -> Self:
        if (any(isinstance(value, UUID) and not value.int for value in self.__dict__.values())
                or set(self.bucket_ids) != {'artifacts', 'trajectories', 'source'}
                or any(not value or len(value) > 128 for value in self.bucket_ids.values())):
            raise ValueError('invalid shared upgrade prerequisite binding')
        return self


class ApplicationUpgradePrerequisites:
    def __init__(self, *, base: HTTPSManagementPrerequisites, settings: UpgradePrerequisiteSettings):
        self.base, self.settings = base, settings
        self.diagnostic_stage: str | None = None

    def public_route(self, request: ManagementUpgradeRequest) -> None:
        self.base.public_route(replace(request.original, deployment=request.setup.deployment,
            candidate=request.setup.candidate, profile=request.setup.profile))

    def preflight(self, request: ManagementUpgradeRequest) -> None:
        try:
            self.diagnostic_stage = 'foundation'
            self.base.foundation(replace(request.original, deployment=request.setup.deployment,
                candidate=request.setup.candidate, profile=request.setup.profile))
            self.diagnostic_stage = 'shared_material'
            self.shared_material(request)
            self.diagnostic_stage = 'platform_capacity'
            self.platform_capacity(request)
            self.diagnostic_stage = 'publication'
            asyncio.run(self.provider_and_publication(request))
            self.diagnostic_stage = 'public_route'
            self.public_route(request)
            self.diagnostic_stage = None
        except Exception:
            raise ManagementPrerequisiteError('application upgrade prerequisites unqualified') from None

    async def provider_and_publication(self, request: ManagementUpgradeRequest) -> None:
        from nebius.sdk import SDK

        async with asyncio.timeout(180):
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
                await self.publications(request, http)
            self.diagnostic_stage = 'cloud_identity'
            setup, scope = request.setup, self.settings.cloud
            application, material = setup.deployment.installation.applications, setup.material
            config = setup.deployment.installation.foundation.platform_config
            if (application is None or material is None or scope.tenant_id != config['quota_parent_id']
                    or scope.region != config['region'] or scope.shared_project_id != config['project_id']
                    or scope.provisioning_project_id != application.storage.project_id
                    or scope.data_group_id != application.storage.data_group_id
                    or scope.source_group_id != application.storage.source_group_id):
                raise ManagementPrerequisiteError('application cloud binding differs')
            ids = self.settings.bucket_ids
            data = {ids[key]: config['buckets'][key] for key in ('artifacts', 'trajectories')}
            if len(data) != len({config['buckets'][key] for key in ('artifacts', 'trajectories')}):
                raise ManagementPrerequisiteError('shared data bucket identities differ')
            before = private_state._private_read(self.base.operator_cloud_credentials, limit=1024**2)
            sdk = SDK(credentials_file_name=str(self.base.operator_cloud_credentials), user_agent_prefix='loom-application-installer/1.0')
            try:
                await qualify_application_cloud(sdk=sdk, scope=scope, credentials_json=material.cloud_credentials_json,
                    data_buckets=data, source_bucket=(ids['source'], config['buckets']['source']))
                if private_state._private_read(self.base.operator_cloud_credentials, limit=1024**2) != before:
                    raise ManagementPrerequisiteError('operator identity changed')
            finally:
                await sdk.close()

    def shared_material(self, request: ManagementUpgradeRequest) -> None:
        """Compare actual shared consumers and private material before copying it."""
        try:
            setup = request.setup
            application, material = setup.deployment.installation.applications, setup.material
            if application is None or material is None:
                raise ValueError
            shared = application.shared
            config = setup.deployment.installation.foundation.platform_config
            namespace = shared.platform_namespace
            def read(kind: str, name: str, uid: UUID) -> dict[str, Any]:
                prefix, resource = {'ConfigMap': ('/api/v1', 'configmaps'), 'Secret': ('/api/v1', 'secrets'),
                    'Deployment': ('/apis/apps/v1', 'deployments')}[kind]
                row = self.base._request('GET', prefix + '/namespaces/' + namespace + '/' + resource + '/' + name)
                if (row is None or row.get('kind') != kind or row['metadata'].get('name') != name
                        or row['metadata'].get('namespace') != namespace or row['metadata'].get('uid') != str(uid)
                        or row['metadata'].get('deletionTimestamp')):
                    raise ValueError
                return row
            cm = read('ConfigMap', 'loom-platform-config', self.settings.shared_config_uid)['data']
            current = json.loads(cm['environment.json'])
            # Ingress cutover owns this routing flag; it is not data identity.
            current['shared_ingress_enabled'] = config['shared_ingress_enabled'] = False
            profile = json.loads(shared.runtime_profile_json)
            if (current != config or config['environment'] != 'development'
                    or json.loads(cm['profile.json']) != profile
                    or json.loads(cm['keyring.json']) != setup.deployment.installation.keyring):
                raise ValueError
            database = read('Secret', 'loom-platform-db', self.settings.shared_database_uid)['data']
            auth = read('Secret', 'loom-platform-auth', self.settings.shared_auth_uid)['data']
            def decode(value: str) -> str:
                return base64.b64decode(value, validate=True).decode('utf-8')
            url = make_url(decode(database['admin-url']))
            if (decode(database['ca.crt']) != material.ca_pem
                    or decode(auth['secret-store-master-key']) != material.secret_store_master_keys
                    or url.host != 'loom-postgres.' + namespace + '.svc' or url.port != 5432
                    or url.database != material.database_name or url.username != 'postgres'
                    or url.query != {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}):
                raise ValueError
            workload = read('Deployment', 'loom-service', self.settings.shared_service_uid)
            containers = [row for row in workload['spec']['template']['spec']['containers'] if row['name'] == 'loom-service']
            if len(containers) != 1:
                raise ValueError
            profiles = [row.get('value') for row in containers[0]['env']
                if row['name'] == 'LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']
            if len(profiles) != 1 or json.loads(profiles[0]) != profile:
                raise ValueError
        except Exception:
            raise ManagementPrerequisiteError('shared development material unqualified') from None

    def platform_capacity(self, request: ManagementUpgradeRequest) -> None:
        """Use existing scheduler-fit proof, but reserve only application workloads."""
        setup = request.setup
        installation = setup.deployment.installation
        application = installation.applications
        if application is None or not application.releases:
            raise ManagementPrerequisiteError('application release unavailable')
        identity = uuid5(setup.deployment.installation_id, 'render-only-application-capacity')
        registration = new_application_registration(installation.foundation, application.shared,
            application_id=identity, incarnation=identity, owner_user_id=identity, owner_team_id=identity,
            slug='preflight', release_id=application.releases[0].release_id)
        child = render_application(registration, application.releases[0], application.shared,
            installation.foundation, authority=application.authority)
        bounds = child.platform_envelope
        dimensions = ('cpu_millis', 'memory_mib', 'ephemeral_storage_mib')
        if bounds.storage_mib != 0 or any(getattr(bounds, key) <= 0 for key in dimensions):
            raise ManagementPrerequisiteError('application footprint unqualified')
        budget = installation.platform_budget
        children = min(getattr(budget, key) // getattr(bounds, key) for key in dimensions)
        if children < 1:
            raise ManagementPrerequisiteError('no application fits platform allowance')
        slots = sum(_count(row) for docs in child.files.values() for row in docs if row['kind'] == 'Deployment')
        controllers = []
        for api, resource, kind in (('apps/v1', 'deployments', 'Deployment'), ('apps/v1', 'statefulsets', 'StatefulSet'),
                ('apps/v1', 'replicasets', 'ReplicaSet'), ('apps/v1', 'daemonsets', 'DaemonSet'),
                ('batch/v1', 'jobs', 'Job'), ('batch/v1', 'cronjobs', 'CronJob')):
            controllers.extend(self.base.inventory(api, resource, kind))
        by_key = {(row['kind'], row['metadata']['namespace'], row['metadata']['name']): row for row in controllers}
        for hpa in self.base.inventory('autoscaling/v2', 'horizontalpodautoscalers', 'HorizontalPodAutoscaler'):
            target = hpa['spec']['scaleTargetRef']
            maximum = hpa['spec']['maxReplicas']
            if target['kind'] not in {'Deployment', 'StatefulSet', 'ReplicaSet'} or type(maximum) is not int or maximum <= 0:
                raise ManagementPrerequisiteError('unqualified platform autoscaling inventory')
            row = by_key[target['kind'], hpa['metadata']['namespace'], target['name']]
            row['spec']['replicas'] = max(row['spec'].get('replicas', 1), maximum)
        rendered = render_management(setup.deployment, candidate=setup.candidate, profile=setup.profile, repo_root=setup.repo_root)
        phases = render_application_setup(setup.deployment, candidate=setup.candidate, profile=setup.profile, repo_root=setup.repo_root)
        planned = [row for row in rendered.files['40-services.yaml'] if row['kind'] == 'Deployment']
        planned += [row for phase in ('database', 'migration') for row in phases[phase] if row['kind'] == 'Job']
        qualify_platform_capacity(nodes=self.base.inventory('v1', 'nodes', 'Node'), pods=self.base.inventory('v1', 'pods', 'Pod'),
            controllers=controllers, planned=planned, reserve=PlatformEnvelope(budget.cpu_millis, budget.memory_mib, 0,
                budget.ephemeral_storage_mib), reserve_pods=children * slots)

    async def publications(self, request: ManagementUpgradeRequest, http: httpx.AsyncClient) -> None:
        """Use authenticated publication bytes, not a caller-supplied source label."""
        try:
            setup = request.setup
            installation = setup.deployment.installation
            application = installation.applications
            if application is None or not application.releases:
                raise ValueError
            catalog = GitHubCandidateCatalog(http,
                token=request.original.material['loom-management-publications']['token'],
                publications=list(installation.publications), registry_prefix=installation.registry_prefix,
                keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
            selected = await catalog.resolve(self.settings.candidate_id)
            if selected.candidate != setup.candidate or selected.profile != setup.profile:
                raise ValueError
            for release in application.releases:
                bundle = selected if release.release_id == self.settings.candidate_id else await catalog.resolve(release.release_id)
                if (release.source_digest != bundle.candidate['source_archive_sha256']
                        or release.service_image_ref != bundle.candidate['images']['service']['image_ref']
                        or release.web_image_ref != bundle.candidate['images']['web']['image_ref']
                        or release.schema_revision != application.shared.schema_revision):
                    raise ValueError
        except Exception:
            raise ManagementPrerequisiteError('application publication unqualified') from None
