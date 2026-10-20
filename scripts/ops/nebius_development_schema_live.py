"""Explicit HTTPS authority for the anchored closed-development schema upgrade.

All resource choices come from the parent's durable child history. Completed
installer objects remain ancestry; replaced workloads are never compared with
their original-only live predicates. Database Jobs run without competing probes.
"""
from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import ssl
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import uuid5

import httpx
from scripts.ops import nebius_development_schema_upgrade as schema
from scripts.ops.nebius_development_bootstrap import _secret_documents
from scripts.ops.nebius_development_build_policy import build_policy_documents
from scripts.ops.nebius_development_entry import _transport
from scripts.ops.nebius_development_preflight import _CONTROLLERS
from scripts.ops.nebius_development_runtime_external import (
    _publication_catalog,
    qualify_runtime_cloud,
    qualify_runtime_publication,
)
from scripts.ops.nebius_development_runtime_job import read_runtime_job
from scripts.ops.nebius_development_runtime_live import (
    HTTPSDevelopmentRuntimeResources,
    runtime_resource_path,
)
from scripts.ops.nebius_development_runtime_observation import _READ_PATHS
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_development_runtime_transition import validate_runtime_transition
from scripts.ops.nebius_development_schema_catalog import (
    closed_development_catalog_state_sql,
    qualify_closed_development_catalog_report,
    refresh_closed_development_catalog_sql,
)
from scripts.ops.nebius_development_schema_jobs import (
    schema_database_selection,
    schema_job_documents,
)
from scripts.ops.nebius_environment_boundary_live import EnvironmentBoundaryWorkloads
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_capacity import _count, qualify_platform_capacity
from scripts.ops.nebius_management_cloud_scope import _read as cloud_read
from scripts.ops.nebius_management_cloud_scope import qualify_backup_material
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_material import _documents as management_material
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_proofs import verify_backup_object
from scripts.ops.nebius_management_stage import (
    _comparison_snapshot,
    _qualified_defaulted,
    _validate_record,
)
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_migration_guard import _runtime_pod_spec
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_runtime_settings import PoolSettingsComponent
from scripts.ops.nebius_pool_startup_database import (
    pool_startup_closed_sql,
    qualify_startup_closed_report,
)

from loom.nebius_application_contract import new_application_registration
from loom.nebius_application_render import render_application
from loom.nebius_candidate_contract import candidate_service_schema_head
from loom.nebius_development_schema_database import (
    DevelopmentSchemaUpgrade,
    qualify_schema_successor_report,
    schema_idle_sql,
    schema_successor_readback_sql,
)
from loom.nebius_environment_render import PlatformEnvelope
from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.pool_management.profiles import _ApplicationImage, _TaskImage


def schema_inventory_options(retained: schema.RetainedDevelopmentSchema, record: dict[str, Any] | None, *,
        prechild: str | None = None) -> dict[str, tuple[dict[str, Any] | None, ...]]:
    """Only journaled predecessor/successor choices, including uncertain creates."""
    result: dict[str, tuple[dict[str, Any] | None, ...]] = {key: (copy.deepcopy(row),) for key, row in retained.plan.source.items()}
    database = retained.runtime.request.database
    foundation = database.foundation
    for name, doc in _secret_documents(foundation.bootstrap['material'], foundation.inputs.binding, foundation.binding.operation_id).items():
        doc['metadata']['uid'] = foundation.bootstrap['secrets'][name]['uid']
        result[_key(doc)] = (doc,)
    manager = database.manager.retained.request.retained
    state = Path(manager.operation['state_dir'])
    material = _json(manager.files[state / 'bootstrap/material/material.json'])
    for name, doc in management_material(material['material'], manager.binding, material['operation_id']).items():
        doc['metadata']['uid'] = material['resources'][name]['uid']
        result[_key(doc)] = (doc,)
    if record is None:
        return result
    for phase in schema.PHASES:
        status = record['phases'][phase]['status']
        path = schema._child(retained, phase)
        if path is None:
            continue
        if status == 'prepared':
            if path.exists() or path.is_symlink():
                raise ValueError('development_schema_untracked_child')
            continue
        if not path.exists():
            if status != 'started' or phase != prechild:
                raise ValueError('development_schema_child_history_missing')
            continue
        child = _json(schema._read(path))
        if phase == 'catalog':
            continue
        if phase in {'stop', 'replace', 'policy', 'start'}:
            originals, targets = schema.transition_documents(retained, phase)
            if not originals:
                if child != {'status': 'empty', 'phase': phase, 'input_digest': retained.plan.input_digest}:
                    raise ValueError('development_schema_empty_transition_changed')
                continue
            validate_runtime_transition(child, originals=originals, targets=targets, input_digest=retained.plan.input_digest,
                phase=phase, qualify_defaulted=schema._defaulted)
            for key, item in child['resources'].items():
                if status == 'complete' and item['status'] != 'applied':
                    raise ValueError('development_schema_transition_incomplete')
                if item['status'] == 'prepared':
                    result[key] = (originals[key],)
                else:
                    desired = copy.deepcopy(item['expected'])
                    desired['metadata']['uid'] = _uid(originals[key])
                    result[key] = (desired,) if item['status'] == 'applied' else (originals[key], desired)
        else:
            documents = schema.phase_documents(retained, record, phase)
            _validate_record(child, dict(schema='loom.nebius-management-stage.v1', binding=asdict(manager.binding),
                revision=digest(documents), phase='schema-' + phase), documents)
            for key, item in child['resources'].items():
                if key in result or (status == 'complete' and item['status'] != 'created'):
                    raise ValueError('development_schema_stage_collision_or_incomplete')
                if item['status'] == 'created':
                    desired = copy.deepcopy(item['observed'])
                    desired['metadata']['uid'] = item['uid']
                    result[key] = (desired,)
                elif item['status'] == 'create_intent':
                    result[key] = (None, copy.deepcopy(item['expected']))
                else:
                    result[key] = (None,)
    return result


class SchemaResources(HTTPSDevelopmentRuntimeResources):
    """Reuse fixed-document create validation with the schema parent's authority."""
    def __init__(self, parent: HTTPSDevelopmentSchemaAPI, phase: str, documents: dict[str, dict[str, Any]]):
        self.parent, self.phase = parent, phase
        self.binding = parent.manager.binding
        self.documents = copy.deepcopy(documents)
        ManagementKubernetesTransport.__init__(self, api_server=parent.api_server, ssl_context=parent.context, token=parent.token)

    def _private_inputs(self) -> None:
        self.parent._private_inputs()

    def _qualify(self, *, writing: bool) -> None:
        self.parent._child_qualify(self.phase, writing=writing)


class SchemaWorkloads:
    """Exact JSON Patch CAS for workloads, configuration and native policy."""
    def __init__(self, parent: HTTPSDevelopmentSchemaAPI, phase: str, originals: dict[str, dict[str, Any]],
            targets: dict[str, dict[str, Any]]):
        self.parent, self.phase = parent, phase
        self.originals, self.targets = copy.deepcopy(originals), copy.deepcopy(targets)

    def qualify(self) -> None:
        self.parent._child_qualify(self.phase, writing=False)

    def read_workload(self, key: str) -> dict[str, Any]:
        self.qualify()
        row = self.parent._get(self.originals[key])
        if _key(row) != key or _uid(row) != _uid(self.originals[key]):
            raise ValueError('development_schema_workload_identity_changed')
        return row

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        if desired != self.targets[key] or not _matches(before, self.originals[key], _uid(self.originals[key])):
            raise ValueError('development_schema_patch_outside_selection')
        version = before['metadata']['resourceVersion']
        if not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError('development_schema_patch_version_unqualified')
        patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(before)},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version}]
        for field in ('spec', 'data', 'binaryData', 'immutable'):
            if field in before:
                patches.append({'op': 'test', 'path': '/' + field, 'value': before[field]})
            if field in desired:
                patches.append({'op': 'replace' if field in before else 'add', 'path': '/' + field, 'value': desired[field]})
            elif field in before:
                patches.append({'op': 'remove', 'path': '/' + field})
        for field in ('labels', 'annotations'):
            if field in desired['metadata']:
                patches.append({'op': 'add', 'path': '/metadata/' + field, 'value': desired['metadata'][field]})
            elif field in before['metadata']:
                patches.append({'op': 'remove', 'path': '/metadata/' + field})
        self.parent._child_qualify(self.phase, writing=True)
        path = runtime_resource_path(desired) + '/' + desired['metadata']['name'] + ('?dryRun=All' if preview else '')
        with self.parent.client.stream('PATCH', path, json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
            if response.status_code not in {200, 409, 422} or response.headers.get('content-encoding', 'identity') != 'identity':
                raise ValueError('development_schema_patch_unconfirmed')
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                raw.extend(chunk)
                if len(raw) > 4 * 1024**2:
                    raise ValueError('development_schema_patch_unconfirmed')
            value = _json(bytes(raw))
            if response.status_code != 200:
                if (value.get('apiVersion') != 'v1' or value.get('kind') != 'Status' or value.get('status') != 'Failure'
                        or value.get('code') != response.status_code or value.get('reason') != {409: 'Conflict', 422: 'Invalid'}[response.status_code]):
                    raise ValueError('development_schema_patch_unconfirmed')
                return None
            if _uid(value) != _uid(before):
                raise ValueError('development_schema_patch_identity_changed')
            return schema._defaulted(desired, value)

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return self._patch(key, before, desired, preview=True)

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch(key, before, desired, preview=False) is not None

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        current = self.read_workload(key)
        if not _matches(current, expected, _uid(self.originals[key])):
            raise ValueError('development_schema_workload_readback_changed')
        return self.parent._workload_ready(current)


class _PolicyProof(EnvironmentBoundaryWorkloads):
    def __init__(self, parent: HTTPSDevelopmentSchemaAPI, resources: dict[str, dict[str, Any]]):
        self.api = parent
        profiles = parent.retained.projection.after_pool.profiles
        targets = {target.profile_id for target in parent.retained.projection.participant.targets}
        selected_profiles = profiles.model_copy(update={family: tuple(row for row in getattr(profiles, family)
            if row.profile_id in targets) for family in ('execution', 'task_images', 'application_images')})
        self.build_policy = {_key(row): resources[_key(row)] for row in build_policy_documents(
            parent.retained.projection.participant.build_namespace.name, selected_profiles)}
        selected: list[_TaskImage | _ApplicationImage] = [row for row in profiles.task_images if row.profile_id in targets]
        selected.extend(row for row in profiles.application_images if row.profile_id in targets)
        self.build_profile = selected[0]

    def qualify(self) -> None:
        self.api._observe(self.api._current_record())


class HTTPSDevelopmentSchemaAPI(HTTPSDevelopmentRuntimeProbes):
    def __init__(self, *, retained: schema.RetainedDevelopmentSchema, ssl_context: ssl.SSLContext,
            token: str, unchanged: Callable[[], None]):
        self.retained, self._retained_origin = copy.deepcopy(retained), copy.deepcopy(retained)
        self.unchanged = unchanged
        self.frame: dict[str, Any] | None = None
        self.external_frame: dict[str, Any] | None = None
        self.external_checked = False
        self.prechild: str | None = None
        self.backup_readbacks: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        connection = retained.inputs.connection
        if not callable(unchanged) or any(path not in retained.files for path in (connection.ca_file, connection.credentials_file)):
            raise ValueError('development_schema_operator_material_unqualified')
        unchanged()
        super().__init__(request=retained.runtime.request, api_server=connection.endpoint, ssl_context=ssl_context,
            token=token, private_files={**retained.runtime.files, **retained.files})
        self.context, self.token = ssl_context, token
        self.operator_credentials = connection.credentials_file
        self.children = ExitStack()

    def __exit__(self, *args: object) -> None:
        try:
            self.children.close()
        finally:
            super().__exit__(*args)

    def _private_inputs(self) -> None:
        self.unchanged()
        if self.retained != self._retained_origin:
            raise ValueError('development_schema_selection_changed')
        super()._private_inputs()

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        self._private_inputs()
        result = super()._request(method, path, document=document)
        self._private_inputs()
        return result

    def _current_record(self) -> dict[str, Any] | None:
        path = self.retained.state_dir / 'operation.json'
        if not path.exists():
            if self.frame is not None or schema._anchor(self.retained).exists():
                raise ValueError('development_schema_parent_history_missing')
            return None
        record = _json(schema._read(path))
        if _json(schema._read(schema._anchor(self.retained))) != schema._identity(self.retained):
            raise ValueError('development_schema_parent_anchor_changed')
        self.prechild = None
        if self.frame is not None:
            for phase in schema.PHASES:
                child = schema._child(self.retained, phase)
                if (record['phases'][phase]['status'] == 'started' and child is not None and not child.exists()
                        and self.frame['phases'][phase]['status'] == 'prepared'):
                    wanted = copy.deepcopy(self.frame)
                    wanted['phases'][phase]['status'] = 'started'
                    if wanted != record:
                        raise ValueError('development_schema_phase_start_changed')
                    self.prechild = phase
                    return schema._record(self.retained, _prechild_phase=phase)
        if schema._record(self.retained) != record:
            raise ValueError('development_schema_parent_frame_changed')
        return record

    def _backup_material(self) -> dict[str, str]:
        row = self.retained.plan.source['Secret:loom-nebius-management-dev:loom-platform-storage']
        return {key: base64.b64decode(row['data'][key], validate=True).decode()
            for key in ('backup-access-key', 'backup-secret-key')}

    def _remaining_backup_bytes(self, record: dict[str, Any] | None) -> int:
        sizes = {'shared-backup': self.request.database.foundation.inputs.config['postgres_storage_gi'],
            'manager-backup': self.retained.projection.before_manager.postgres_storage_gi}
        required = 0
        for phase, size in sizes.items():
            status = 'prepared' if record is None else record['phases'][phase]['status']
            if status == 'complete':
                continue
            if status == 'started':
                assert record is not None
                path = schema._child(self.retained, phase)
                assert path is not None
                staged = schema._staged_resources(self.retained, record, phase, allow_pending=True) if path.exists() else {}
                jobs = [row for row in staged.values() if row['kind'] == 'Job']
                if jobs:
                    job, = jobs
                    current = self._get(job)
                    if not _matches(current, job, _uid(job)):
                        raise ValueError('development_schema_checkpoint_job_changed')
                    if any(item.get('type') == 'Complete' and item.get('status') == 'True'
                            for item in current.get('status', {}).get('conditions', [])):
                        cached = getattr(self, 'backup_readbacks', {}).get(phase)
                        if cached is not None and cached[0] == record and cached[1]['job_uid'] == _uid(job):
                            continue
                        if self.report(phase, record) is None:
                            raise ValueError('development_schema_checkpoint_upload_unconfirmed')
                        continue
            required += size * 1024**3
        return max(1, required)

    async def _external(self, record: dict[str, Any] | None) -> None:
        from nebius.api.nebius.quotas import v1 as quotas
        from nebius.sdk import SDK

        self._private_inputs()
        projection = self.retained.projection
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_runtime_publication(request=self.request, http=http)
            for previous, selected in ((self.request.database.data_runtime, projection.data_publication),
                    (self.request.database.manager.publication, projection.manager_publication)):
                catalog = _publication_catalog(request=self.request, http=http, publications=[selected.publication])
                if (await catalog.resolve(selected.publication.candidate_id) != selected.bundle
                        or not await catalog.candidate_follows(candidate_id=selected.publication.candidate_id,
                            current_sha=previous.source.source_sha)):
                    raise ValueError('development_schema_publication_unqualified')
        await qualify_runtime_cloud(request=self.request, operator_credentials=self.operator_credentials)
        settings = self.manager.retained.inputs.prerequisites
        scope = settings.backup
        required = self._remaining_backup_bytes(record)
        async with asyncio.timeout(180):
            sdk = SDK(credentials_file_name=str(self.operator_credentials), user_agent_prefix='loom-dev-schema/1.0')
            try:
                await qualify_backup_material(sdk=sdk, scope=scope, material=self._backup_material(),
                    bucket_name=projection.before_manager.backup_bucket, backup_bytes=required)
                row = await cloud_read(quotas.QuotaAllowanceServiceClient(sdk).get_by_name,
                    quotas.GetByNameRequest(parent_id=scope.tenant_id, name=settings.backup_quota_name, region=scope.region))
                if (not row['metadata'].get('id') or row['metadata']['parent_id'] != scope.tenant_id
                        or row['metadata']['name'] != settings.backup_quota_name or row['spec']['region'] != scope.region
                        or row['status']['state'] != 'STATE_ACTIVE' or row['status']['service'] != 'storage'
                        or row['status']['usage_state'] not in {'USAGE_STATE_USED', 'USAGE_STATE_NOT_USED'}
                        or row['status']['unit'] != settings.backup_quota_unit):
                    raise ValueError('development_schema_backup_quota_unqualified')
                limit, used = int(row['spec'].get('limit', 0)), int(row['status'].get('usage', 0))
                if min(limit, used) < 0 or limit - used < required:
                    raise ValueError('development_schema_backup_capacity_unqualified')
            finally:
                await sdk.close()
        self._private_inputs()

    def _capacity(self) -> None:
        def inventory(api: str, resource: str, kind: str) -> list[dict[str, Any]]:
            return inventory_resources(self._request, api, resource, kind, include_terminal_pods=True)
        nodes, pods = inventory('v1', 'nodes', 'Node'), inventory('v1', 'pods', 'Pod')
        controllers = [row for api, resource, kind in _CONTROLLERS for row in inventory(api, resource, kind)]
        if inventory('v1', 'replicationcontrollers', 'ReplicationController'):
            raise ValueError('development_schema_capacity_controller_unqualified')
        supported = {(api, kind) for api, _, kind in _CONTROLLERS} | {('v1', 'Node')}
        if any(owner.get('controller') is True and (owner['apiVersion'], owner['kind']) not in supported
                for row in (*pods, *controllers) for owner in row['metadata'].get('ownerReferences', [])):
            raise ValueError('development_schema_capacity_controller_unqualified')
        by_key = {(row['kind'], row['metadata']['namespace'], row['metadata']['name']): row for row in controllers}
        for hpa in inventory('autoscaling/v2', 'horizontalpodautoscalers', 'HorizontalPodAutoscaler'):
            target, maximum = hpa['spec']['scaleTargetRef'], hpa['spec']['maxReplicas']
            if (target['apiVersion'] != 'apps/v1' or target['kind'] not in {'Deployment', 'StatefulSet', 'ReplicaSet'}
                    or type(maximum) is not int or not 0 < maximum <= 10000):
                raise ValueError('development_schema_capacity_autoscaler_unqualified')
            row = by_key[target['kind'], hpa['metadata']['namespace'], target['name']]
            replicas = row['spec'].get('replicas', 1)
            if type(replicas) is not int or not 0 <= replicas <= 10000:
                raise ValueError('development_schema_capacity_replicas_unqualified')
            row['spec']['replicas'] = max(replicas, maximum)
        installation = self.retained.projection.after_manager.installation
        application, budget = installation.applications, installation.platform_budget
        if application is None or not application.releases:
            raise ValueError('development_schema_application_capacity_unqualified')
        identity = uuid5(self.retained.projection.after_manager.installation_id, 'render-only-application-capacity')
        registration = new_application_registration(installation.foundation, application.shared,
            application_id=identity, incarnation=identity, owner_user_id=identity, owner_team_id=identity,
            slug='preflight', release_id=application.releases[0].release_id)
        child = render_application(registration, application.releases[0], application.shared,
            installation.foundation, authority=application.authority)
        dimensions = ('cpu_millis', 'memory_mib', 'ephemeral_storage_mib')
        bounds = child.platform_envelope
        if bounds.storage_mib != 0 or any(getattr(bounds, key) <= 0 for key in dimensions):
            raise ValueError('development_schema_application_capacity_unqualified')
        children = min(getattr(budget, key) // getattr(bounds, key) for key in dimensions)
        slots = sum(_count(row) for docs in child.files.values() for row in docs if row['kind'] == 'Deployment')
        if children < 1:
            raise ValueError('development_schema_application_capacity_unqualified')
        for pod in pods:
            if pod['metadata'].get('deletionTimestamp'):
                pod['metadata'].pop('ownerReferences', None)
        planned = list(self.retained.plan.targets.values())
        for phase in ('shared-probe', 'manager-probe', 'shared-backup', 'manager-backup'):
            job, = (row for row in schema_job_documents(self.retained.runtime, self.retained.projection, phase).values()
                if row['kind'] == 'Job')
            qualify_platform_capacity(nodes=nodes, pods=pods, controllers=controllers, planned=[*planned, job],
                reserve=PlatformEnvelope(budget.cpu_millis, budget.memory_mib, 0, budget.ephemeral_storage_mib),
                reserve_pods=children * slots)

    def qualify(self, retained: schema.RetainedDevelopmentSchema, record: dict[str, Any] | None) -> None:
        self._private_inputs()
        if retained != self.retained or self._current_record() != record:
            raise ValueError('development_schema_parent_selection_changed')
        if not self.external_checked or self.external_frame != record:
            asyncio.run(self._external(record))
            self._capacity()
            self.external_frame, self.external_checked = copy.deepcopy(record), True
        self._observe(record)
        self.frame = copy.deepcopy(record)
        if record is not None and all(row['status'] == 'complete' for row in record['phases'].values()) and not self.verify(record):
            raise ValueError('development_schema_completed_successor_not_ready')

    def _child_qualify(self, phase: str, *, writing: bool) -> None:
        record = self._current_record()
        if record is None or record['phases'][phase]['status'] != 'started':
            raise ValueError('development_schema_child_not_active')
        self._observe(record)
        if writing:
            self._private_inputs()

    def resources(self, phase: str, documents: dict[str, dict[str, Any]]) -> SchemaResources:
        record = self._current_record()
        if record is None or documents != schema.phase_documents(self.retained, record, phase):
            raise ValueError('development_schema_resource_selection_changed')
        return self.children.enter_context(SchemaResources(self, phase, documents))

    def workloads(self, phase: str, originals: dict[str, dict[str, Any]],
            targets: dict[str, dict[str, Any]]) -> SchemaWorkloads:
        if (originals, targets) != schema.transition_documents(self.retained, phase):
            raise ValueError('development_schema_workload_selection_changed')
        return SchemaWorkloads(self, phase, originals, targets)

    def _workload_ready(self, current: dict[str, Any]) -> bool:
        if current['kind'] not in {'Deployment', 'CronJob'}:
            return True
        namespace = current['metadata']['namespace']
        deployment = current['kind'] == 'Deployment'
        if not deployment and current['spec'].get('suspend') is not True:
            return True
        children = self._read(('/apis/apps/v1/namespaces/' if deployment else '/apis/batch/v1/namespaces/')
            + namespace + ('/replicasets?limit=1000' if deployment else '/jobs?limit=1000'))
        pods = self._read('/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        ready = (qualify_started_deployment(current=current, children=children, pods=pods,
            region=self.request.database.foundation.inputs.config['region']) if deployment and current['spec']['replicas'] == 1
            else qualify_closed_workload_drain(original=current, desired=_snapshot(current), current=current, children=children, pods=pods))
        final = self._get(current)
        if _snapshot(final) != _snapshot(current) or final['metadata'].get('generation') != current['metadata'].get('generation'):
            raise ValueError('development_schema_workload_changed_during_readiness')
        return ready

    def _sql(self, namespace: str, statement: str, *, max_output_bytes: int = 65536) -> dict[str, Any]:
        # The candidate runner rejects every other DB session during migration.
        # Suppress probes for all fixed Job phases, including their initial POST.
        frame = getattr(self, 'query_frame', self.frame)
        if frame is not None and any(item['status'] == 'started' and '-' in phase
                for phase, item in frame['phases'].items()):
            raise ValueError('development_schema_job_owns_database_observation')
        before = self._database(namespace)
        result = self._exec(before, 'loom-postgres', ['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
            '-U', 'postgres', '-d', 'loom', '-c', statement], max_output_bytes=max_output_bytes)
        if self._process_identity(self._database(namespace)) != self._process_identity(before):
            raise ValueError('development_schema_database_changed_during_observation')
        return result

    def _closed(self, record: dict[str, Any] | None) -> None:
        if record is not None and any(item['status'] == 'started' and '-' in phase for phase, item in record['phases'].items()):
            return
        projection = self.retained.projection
        if record is None or record['phases']['manager-upgrade']['status'] != 'complete':
            report = self._sql(self.manager.binding.namespace, pool_startup_closed_sql(projection.before_pool))
            qualify_startup_closed_report(projection.before_pool, report)
        else:
            report = self._sql(self.manager.binding.namespace, closed_development_catalog_state_sql(projection.catalog_request))
            state = qualify_closed_development_catalog_report(projection.catalog_request, report, read_only=True)
            permitted = {'ready', 'complete'} if record['phases']['catalog']['status'] == 'started' else {
                'complete' if record['phases']['catalog']['status'] == 'complete' else 'ready'}
            if state not in permitted:
                raise ValueError('development_schema_catalog_phase_changed')
        if record is None or record['phases']['stop']['status'] == 'prepared':
            for mode in ('shared', 'manager'):
                selection = schema_database_selection(self.retained.runtime, projection, mode)
                statement = 'BEGIN TRANSACTION READ ONLY; SET LOCAL statement_timeout=10000; SET LOCAL lock_timeout=2000;\n' + schema_idle_sql(selection) + ';\nROLLBACK;'
                observed = self._sql(selection.namespace, statement)
                expected = dict(schema_version='loom.development-schema-idle.v1', revision=selection.predecessor_revision,
                    selection_sha256=hashlib.sha256(json.dumps(selection.model_dump(mode='json'), sort_keys=True,
                        separators=(',', ':')).encode()).hexdigest(), status='qualified')
                if observed != expected:
                    raise ValueError('development_schema_predecessor_not_idle')

    def _collection(self, namespace: str, version: str, kind: str, resource: str) -> dict[str, Any]:
        prefix = '/api/v1' if version == 'v1' else '/apis/' + version
        result = self._read(prefix + '/namespaces/' + namespace + '/' + resource + '?limit=1000')
        revision = result.get('metadata', {}).get('resourceVersion')
        if (result.get('apiVersion') != version or result.get('kind') != kind + 'List'
                or not isinstance(revision, str) or not 0 < len(revision) <= 128
                or result['metadata'].get('continue') or not isinstance(result.get('items'), list)
                or len(result['items']) > 1000):
            raise ValueError('development_schema_inventory_incomplete')
        rows = [{'apiVersion': version, 'kind': kind, **row} for row in result['items']]
        if (any(row['apiVersion'] != version or row['kind'] != kind or row['metadata']['namespace'] != namespace for row in rows)
                or len({_uid(row) for row in rows}) != len(rows)
                or len({_key(row) for row in rows}) != len(rows)):
            raise ValueError('development_schema_inventory_identity_changed')
        return {**result, 'items': rows}

    @staticmethod
    def _owner(row: dict[str, Any], parents: dict[str, dict[str, Any]]) -> dict[str, Any]:
        owners = row['metadata'].get('ownerReferences', [])
        parent = parents.get(owners[0].get('uid')) if len(owners) == 1 else None
        if parent is None:
            raise ValueError('development_schema_unaccounted_workload')
        owner = dict(owners[0])
        blocking = owner.pop('blockOwnerDeletion', False)
        if (type(blocking) is not bool or owner != {'apiVersion': parent['apiVersion'], 'kind': parent['kind'],
                'name': parent['metadata']['name'], 'uid': _uid(parent), 'controller': True}
                or row['metadata']['namespace'] != parent['metadata']['namespace']):
            raise ValueError('development_schema_unaccounted_workload')
        return parent

    @staticmethod
    def _template(actual: dict[str, Any], wanted: dict[str, Any], *, pod: bool = False) -> bool:
        """Only ordinary Kubernetes defaults; ownership alone cannot bless code."""
        try:
            actual = _runtime_pod_spec(actual, wanted) if pod else copy.deepcopy(actual)
            if not _matches_backup_template(actual, wanted):
                return False
            if actual.get('serviceAccountName', 'default') != wanted.get('serviceAccountName', 'default'):
                return False
            if pod:
                if 'nodeName' not in wanted:
                    actual.pop('nodeName', None)
                tolerations = actual.get('tolerations', [])
                requested = wanted.get('tolerations', [])
                for key in ('not-ready', 'unreachable'):
                    default = {'key': 'node.kubernetes.io/' + key, 'operator': 'Exists',
                        'effect': 'NoExecute', 'tolerationSeconds': 300}
                    if default not in requested and default in tolerations:
                        tolerations.remove(default)
                if tolerations != requested:
                    return False
            _qualified_defaulted({'kind': 'Deployment', 'metadata': {}, 'spec': {'template': {'spec': wanted}}},
                {'kind': 'Deployment', 'metadata': {}, 'spec': {'template': {'spec': actual}}})
            return True
        except (ValueError, RuntimeError, KeyError, TypeError):
            return False

    def _census(self, observed: dict[str, dict[str, Any]], record: dict[str, Any] | None) -> None:
        """Account for every writer-capable controller and every descendant Pod."""
        participant = self.retained.projection.participant
        namespaces = {'loom-dev', self.manager.binding.namespace, participant.execution_namespace.name,
            participant.build_namespace.name}
        collections = (('apps/v1', 'Deployment', 'deployments'), ('apps/v1', 'StatefulSet', 'statefulsets'),
            ('apps/v1', 'DaemonSet', 'daemonsets'), ('apps/v1', 'ReplicaSet', 'replicasets'),
            ('batch/v1', 'CronJob', 'cronjobs'), ('batch/v1', 'Job', 'jobs'), ('v1', 'Pod', 'pods'),
            ('v1', 'ReplicationController', 'replicationcontrollers'),
            ('autoscaling/v2', 'HorizontalPodAutoscaler', 'horizontalpodautoscalers'))
        stopped = (record is not None and record['phases']['stop']['status'] == 'complete'
            and record['phases']['start']['status'] == 'prepared')
        before_stop = record is None or record['phases']['stop']['status'] != 'complete'
        def cron_allowed(cron: dict[str, Any] | None) -> bool:
            return cron is not None and not stopped and (before_stop or cron['spec'].get('suspend') is False)
        active_jobs: set[str] = set()
        if record is not None:
            for phase in schema.PHASES:
                if '-' in phase and record['phases'][phase]['status'] == 'started':
                    active_jobs.update(key for key, row in schema.phase_documents(self.retained, record, phase).items()
                        if row['kind'] == 'Job')
        for namespace in sorted(namespaces):
            lists = {kind: self._collection(namespace, version, kind, resource) for version, kind, resource in collections}
            if lists['ReplicationController']['items'] or lists['HorizontalPodAutoscaler']['items']:
                raise ValueError('development_schema_unaccounted_workload')
            direct: dict[str, dict[str, Any]] = {}
            for kind in ('Deployment', 'StatefulSet', 'DaemonSet', 'CronJob'):
                expected = {key: row for key, row in observed.items()
                    if row['kind'] == kind and row['metadata'].get('namespace') == namespace}
                actual = {_key(row): row for row in lists[kind]['items']}
                if actual.keys() != expected.keys() or any(not _matches(actual[key], row, _uid(row)) for key, row in expected.items()):
                    raise ValueError('development_schema_unaccounted_workload')
                direct.update({_uid(row): row for row in actual.values()})
            replicas = {_uid(row): row for row in lists['ReplicaSet']['items']}
            for row in replicas.values():
                parent = self._owner(row, direct)
                if parent['kind'] != 'Deployment':
                    raise ValueError('development_schema_unaccounted_workload')
                count = row['spec'].get('replicas', 1)
                if type(count) is not int or count < 0:
                    raise ValueError('development_schema_replicas_unqualified')
                live_pods = any(any(owner.get('uid') == _uid(row) for owner in pod['metadata'].get('ownerReferences', []))
                    and (pod.get('status', {}).get('phase') not in {'Succeeded', 'Failed'} or pod['metadata'].get('deletionTimestamp'))
                    for pod in lists['Pod']['items'])
                if count or live_pods:
                    choices = [parent, self.retained.plan.source[_key(parent)]]
                    if not any(self._template(row['spec']['template']['spec'], choice['spec']['template']['spec']) for choice in choices):
                        raise ValueError('development_schema_replica_template_changed')
            jobs = {_uid(row): row for row in lists['Job']['items']}
            expected_jobs = {_uid(row) for row in observed.values()
                if row['kind'] == 'Job' and row['metadata'].get('namespace') == namespace}
            if not expected_jobs <= jobs.keys():
                raise ValueError('development_schema_job_missing')
            for row in jobs.values():
                key = _key(row)
                retained = observed.get(key)
                cron = None
                terminal = any(item.get('type') in {'Complete', 'Failed'} and item.get('status') == 'True'
                    for item in row.get('status', {}).get('conditions', []))
                if retained is None:
                    cron = self._owner(row, direct)
                    options = [cron]
                    if terminal and _key(cron) in self.retained.plan.source:
                        options.append(self.retained.plan.source[_key(cron)])
                    if cron['kind'] != 'CronJob' or not any(self._template(
                            row['spec']['template']['spec'], option['spec']['jobTemplate']['spec']['template']['spec'])
                            for option in options):
                        raise ValueError('development_schema_unaccounted_workload')
                elif not _matches(row, retained, _uid(retained)):
                    raise ValueError('development_schema_job_changed')
                if not terminal and key not in active_jobs and not cron_allowed(cron):
                    raise ValueError('development_schema_unaccounted_active_job')
            parents = {**direct, **replicas, **jobs}
            for row in lists['Pod']['items']:
                parent = self._owner(row, parents)
                if parent['kind'] not in {'ReplicaSet', 'StatefulSet', 'DaemonSet', 'Job'}:
                    raise ValueError('development_schema_unaccounted_workload')
                live = row.get('status', {}).get('phase') not in {'Succeeded', 'Failed'} or row['metadata'].get('deletionTimestamp')
                if live and parent['kind'] in {'ReplicaSet', 'Job'} and not self._template(
                        row['spec'], parent['spec']['template']['spec'], pod=True):
                    raise ValueError('development_schema_pod_template_changed')
                if live and stopped and not (parent['kind'] == 'StatefulSet'
                        and parent['metadata']['name'] == 'loom-postgres') and _key(parent) not in active_jobs:
                    raise ValueError('development_schema_writers_not_drained')
                if live and parent['kind'] == 'Job' and _key(parent) not in active_jobs:
                    cron = None if _key(parent) in observed else self._owner(parent, direct)
                    if not cron_allowed(cron):
                        raise ValueError('development_schema_unaccounted_active_job')
            if stopped:
                for row in direct.values():
                    if row['kind'] in {'Deployment', 'CronJob'} and not self._workload_ready(row):
                        raise ValueError('development_schema_writers_not_drained')

    def _observe(self, record: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
        self._private_inputs()
        if self._current_record() != record:
            raise ValueError('development_schema_parent_frame_changed')
        # Keep the last qualified frame for prechild authorization, but select
        # this observation's phase before deciding whether SQL is permitted.
        self.query_frame = copy.deepcopy(record)
        options = schema_inventory_options(self.retained, record, prechild=self.prechild)
        observed = {}
        for key, choices in options.items():
            kind, namespace, name = key.split(':')
            version, resource, cluster = _READ_PATHS[kind]
            prefix = '/api/v1' if version == 'v1' else '/apis/' + version
            path = prefix + ('' if cluster else '/namespaces/' + namespace) + '/' + resource + '/' + name
            actual = self._request('GET', path)
            if actual is None:
                if None not in choices:
                    raise ValueError('development_schema_resource_missing')
                continue
            current: dict[str, Any] = actual
            def matches(expected: dict[str, Any] | None, actual: dict[str, Any] = current) -> bool:
                if expected is None:
                    return False
                if 'uid' in expected['metadata']:
                    return _matches(actual, expected, _uid(expected))
                return _comparison_snapshot(actual) == expected and schema._defaulted(expected, actual) == expected
            if _key(actual) != key or not any(matches(expected) for expected in choices):
                raise ValueError('development_schema_resource_changed')
            observed[key] = actual
        if len({_uid(row) for row in observed.values()}) != len(observed):
            raise ValueError('development_schema_resource_identity_collision')
        # These two methods reuse only the retained namespace/storage/database
        # roots. They deliberately never inspect original service templates.
        self._database('loom-dev')
        self._database(self.manager.binding.namespace)
        self._census(observed, record)
        self._closed(record)
        if (self._current_record() != record
                or schema_inventory_options(self.retained, record, prechild=self.prechild) != options):
            raise ValueError('development_schema_history_changed_during_observation')
        self._private_inputs()
        return observed

    def _recorded(self, phase: str, record: dict[str, Any]) -> dict[str, dict[str, Any]]:
        staged = schema._staged_resources(self.retained, record, phase)
        observed = self._observe(record)
        result = {key: observed[key] for key in staged}
        if any(not _matches(result[key], row, _uid(row)) for key, row in staged.items()):
            raise ValueError('development_schema_job_resources_changed')
        return result

    @contextmanager
    def _backup_client(self) -> Iterator[Any]:
        import boto3
        from botocore.config import Config

        self._private_inputs()
        config = self.retained.projection.before_manager.installation.foundation.platform_config
        endpoint = urlsplit(config['storage_endpoint'])
        if endpoint.scheme != 'https' or not endpoint.hostname or endpoint.username or endpoint.password:
            raise ValueError('development_schema_backup_endpoint_unqualified')
        credentials = self._backup_material()
        client = boto3.client('s3', endpoint_url=config['storage_endpoint'], region_name=config['region'],
            aws_access_key_id=credentials['backup-access-key'], aws_secret_access_key=credentials['backup-secret-key'],
            config=Config(retries={'total_max_attempts': 1, 'mode': 'standard'}, proxies={},
                connect_timeout=10, read_timeout=30, s3={'addressing_style': 'path'}))
        def exact_endpoint(request: Any, **_kwargs: Any) -> None:
            self._private_inputs()
            target = urlsplit(request.url.decode() if isinstance(request.url, bytes) else request.url)
            if (target.scheme, target.netloc) != (endpoint.scheme, endpoint.netloc):
                raise ValueError('development_schema_backup_endpoint_changed')
        try:
            client.meta.events.register_first('before-send.s3', exact_endpoint)
            yield client
            self._private_inputs()
        finally:
            client.close()

    def report(self, phase: str, record: dict[str, Any]) -> dict[str, Any] | None:
        if phase not in schema.PHASES or '-' not in phase or self._current_record() != record:
            raise ValueError('development_schema_report_phase_changed')
        mode, action = phase.split('-')
        modes: dict[str, Literal['shared', 'manager']] = {'shared': 'shared', 'manager': 'manager'}
        selection = schema_database_selection(self.retained.runtime, self.retained.projection, modes[mode])
        def validate(proof: dict[str, Any]) -> None:
            if action != 'backup':
                schema.validate_phase_proof(self.retained, record, phase, proof)
                if record['phases'][phase]['status'] == 'complete' and proof != record['phases'][phase]['proof']:
                    raise ValueError('development_schema_recorded_job_proof_changed')
        result = read_runtime_job(client=self.client, read=self._request_read,
            recorded=lambda: self._recorded(phase, record), private_inputs=self._private_inputs,
            region=self.request.database.foundation.inputs.config['region'], report_field='database',
            namespace=selection.namespace, validate=validate)
        if result is None or action != 'backup':
            return result
        allowance = (self.request.database.foundation.inputs.config['postgres_storage_gi'] if mode == 'shared'
            else self.retained.projection.before_manager.postgres_storage_gi) * 1024**3
        with self._backup_client() as client:
            proof = verify_backup_object(client=client, bucket=self.retained.projection.before_manager.backup_bucket,
                namespace=selection.namespace, job_uid=result['job_uid'], report=result['database'], max_bytes=allowance)
        schema.validate_phase_proof(self.retained, record, phase, proof)
        if record['phases'][phase]['status'] == 'complete' and proof != record['phases'][phase]['proof']:
            raise ValueError('development_schema_recorded_checkpoint_changed')
        self._recorded(phase, record)
        if not hasattr(self, 'backup_readbacks'):
            self.backup_readbacks = {}
        self.backup_readbacks[phase] = (copy.deepcopy(record), copy.deepcopy(proof))
        return proof

    def _request_read(self, path: str) -> dict[str, Any] | None:
        return self._request('GET', path)

    def catalog_observe(self) -> dict[str, Any]:
        record = self._current_record()
        if record is None or record['phases']['manager-upgrade']['status'] != 'complete':
            raise ValueError('development_schema_catalog_not_ready')
        self._observe(record)
        request = self.retained.projection.catalog_request
        report = self._sql(self.manager.binding.namespace, closed_development_catalog_state_sql(request))
        qualify_closed_development_catalog_report(request, report, read_only=True)
        self._private_inputs()
        return report

    def catalog_commit(self) -> None:
        record = self._current_record()
        if record is None or record['phases']['catalog']['status'] != 'started':
            raise ValueError('development_schema_catalog_not_active')
        path = schema._child(self.retained, 'catalog')
        assert path is not None
        request = self.retained.projection.catalog_request
        if _json(schema._read(path)) != dict(request_sha256=digest(request.model_dump(mode='json')), status='intent', proof=None):
            raise ValueError('development_schema_catalog_intent_missing')
        self._observe(record)
        report = self._sql(self.manager.binding.namespace, refresh_closed_development_catalog_sql(request))
        if qualify_closed_development_catalog_report(request, report, read_only=False) != 'complete':
            raise ValueError('development_schema_catalog_write_unconfirmed')
        self._private_inputs()

    @property
    def effective_registration(self) -> PoolRegistrationRequest:
        original = self.request.database.manager.retained.request.registration
        return replace(original, spec=self.retained.projection.after_pool,
            candidate=self.retained.projection.manager_publication.bundle.candidate)

    @property
    def manager_revision(self) -> str:
        return candidate_service_schema_head(self.retained.projection.manager_publication.bundle.candidate)

    def _running(self, key: str, state_dir: Path) -> tuple[PoolSettingsComponent, dict[str, Any], dict[str, Any]]:
        if state_dir != self.retained.runtime.state_dir:
            raise ValueError('development_schema_runtime_history_changed')
        desired = self.retained.plan.targets[key]
        if desired['kind'] != 'Deployment' or desired['spec']['replicas'] != 1:
            raise ValueError('development_schema_process_outside_selection')
        namespace, name = desired['metadata']['namespace'], desired['metadata']['name']
        components: dict[tuple[str, str], PoolSettingsComponent] = {
            (self.manager.binding.namespace, 'loom-service'): 'manager', ('loom-dev', 'loom-service'): 'service',
            ('loom-dev', 'loom-control-plane'): 'controller',
            (self.retained.projection.participant.execution_namespace.name, 'loom-execution-actuator'): 'actuator'}
        component = components[namespace, name]
        current = self._observe(self._current_record())[key]
        _qualified_defaulted(desired, current)
        if not self._workload_ready(current):
            raise ValueError('development_schema_process_not_ready')
        pods = self._collection(namespace, 'v1', 'Pod', 'pods')
        labels = current['spec']['selector']['matchLabels']
        pod, = (row for row in pods['items'] if all(row['metadata'].get('labels', {}).get(name) == value
            for name, value in labels.items()))
        return component, current, pod

    def _fresh_database(self, phase: str, record: dict[str, Any]) -> None:
        documents = schema.phase_documents(self.retained, record, phase)
        configuration, = (row for row in documents.values() if row['kind'] == 'ConfigMap')
        request = DevelopmentSchemaUpgrade.model_validate_json(configuration['data']['request.json'])
        report = self._sql(request.selection.namespace, schema_successor_readback_sql(request), max_output_bytes=4 * 1024**2)
        if qualify_schema_successor_report(request, report) != record['phases'][phase]['proof']['database']:
            raise ValueError('development_schema_current_database_changed')

    def verify(self, record: dict[str, Any]) -> bool:
        if (self._current_record() != record or record['phases']['verify']['status'] not in {'started', 'complete'}
                or any(record['phases'][phase]['status'] != 'complete' for phase in schema.PHASES if phase != 'verify')):
            raise ValueError('development_schema_verification_not_ready')
        observed = self._observe(record)
        if any(not self._workload_ready(observed[key]) for key in self.retained.plan.targets):
            return False
        for phase in ('shared-inspect', 'manager-inspect'):
            if self.report(phase, record) != record['phases'][phase]['proof']:
                raise ValueError('development_schema_final_database_proof_changed')
            self._fresh_database(phase, record)
        for key, row in self.retained.plan.targets.items():
            if row['kind'] == 'Deployment' and row['spec']['replicas'] == 1:
                self.qualify_runtime_settings(key=key, state_dir=self.retained.runtime.state_dir)
                self.qualify_runtime_database(key=key, state_dir=self.retained.runtime.state_dir)
        if not _PolicyProof(self, observed).policy_ready():
            return False
        if qualify_closed_development_catalog_report(self.retained.projection.catalog_request,
                self.catalog_observe(), read_only=True) != 'complete':
            raise ValueError('development_schema_final_catalog_changed')
        asyncio.run(self._external(record))
        self._observe(record)
        return True


@contextmanager
def connected_schema_api(retained: schema.RetainedDevelopmentSchema, *, unchanged: Callable[[], None]
        ) -> Iterator[HTTPSDevelopmentSchemaAPI]:
    """Resolve only captured explicit operator files; remove temporary transport."""
    connection = retained.inputs.connection
    if any(path not in retained.files for path in (connection.ca_file, connection.credentials_file)):
        raise ValueError('development_schema_operator_material_unqualified')
    unchanged()
    schema._unchanged(retained.files)
    context, token = asyncio.run(_transport(connection))
    unchanged()
    schema._unchanged(retained.files)
    with HTTPSDevelopmentSchemaAPI(retained=retained, ssl_context=context, token=token, unchanged=unchanged) as api:
        unchanged()
        yield api
        unchanged()
