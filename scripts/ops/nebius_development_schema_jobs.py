"""Fixed database Jobs for the connected, closed-development schema successor.

Jobs keep each database's retained admin URL and CA in its own namespace. Shared
backups use the existing same-namespace PostgreSQL route and a temporary copy of
only the manager's backup bucket keys; no policy, bucket or IAM change is needed.
The parent qualifies live credentials, backup scope/capacity, retained dispatch
intent and object readback. These projections themselves grant no write authority.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from typing import TYPE_CHECKING, Any, Literal

from scripts.ops.nebius_development_runtime_retained import CompletedDevelopmentRuntime
from scripts.ops.nebius_development_schema_projection import (
    DevelopmentSchemaProjection,
    project_development_schema_successor,
)
from scripts.ops.nebius_development_schema_resources import MARKER, schema_original_resources
from scripts.ops.nebius_ingress_stage import _key
from sqlalchemy.engine import make_url

from loom.nebius_candidate_contract import candidate_service_schema_head
from loom.nebius_development_schema_database import (
    DevelopmentSchemaSelection,
    DevelopmentSchemaUpgrade,
)
from loom.nebius_platform_bootstrap import database_url

if TYPE_CHECKING:
    from scripts.ops.nebius_pool_storage_operation import CompletedClosedStorage


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def schema_database_selection(runtime: CompletedDevelopmentRuntime, projection: DevelopmentSchemaProjection,
        mode: Literal['shared', 'manager'], *, storage: CompletedClosedStorage | None = None) -> DevelopmentSchemaSelection:
    """Bind each database's independent predecessor and protected publication."""
    try:
        expected = project_development_schema_successor(runtime=runtime, operation_id=projection.after_pool.operation_id,
            data_publication=projection.data_publication, manager_publication=projection.manager_publication, storage=storage)
        if mode not in {'shared', 'manager'} or expected != projection:
            raise ValueError
        shared = mode == 'shared'
        previous = runtime.request.database.data_runtime if shared else runtime.request.database.manager.publication
        publication = projection.data_publication if shared else projection.manager_publication
        return DevelopmentSchemaSelection(operation_id=projection.after_pool.operation_id, mode=mode,
            namespace='loom-dev' if shared else 'loom-nebius-management-dev',
            predecessor_revision=candidate_service_schema_head(previous.bundle.candidate),
            candidate_revision=candidate_service_schema_head(publication.bundle.candidate),
            candidate_sha=publication.source.source_sha,
            data_environment_id=projection.participant.environment_id if shared else None,
            pool_id=None if shared else projection.before_pool.pool_id)
    except Exception:
        raise ValueError('development_schema_job_selection_unqualified') from None


def _secret_env(name: str, secret: str, key: str) -> dict[str, Any]:
    return {'name': name, 'valueFrom': {'secretKeyRef': {'name': secret, 'key': key}}}


def schema_job_documents(runtime: CompletedDevelopmentRuntime, projection: DevelopmentSchemaProjection,
        phase: str, predecessor: dict[str, Any] | None = None,
        checkpoint: dict[str, Any] | None = None, *, storage: CompletedClosedStorage | None = None) -> dict[str, dict[str, Any]]:
    """Reproduce exact create-only probe, backup, upgrade or inspect documents.

    Upgrade and inspect bind the full independently qualified backup receipt;
    neither accepts just a caller-selected backup key or a migration revision.
    Completed Jobs remain available for recovery and have no automatic retry.
    """
    try:
        modes: dict[str, Literal['shared', 'manager']] = {'shared': 'shared', 'manager': 'manager'}
        mode_name, action = phase.split('-')
        mode = modes[mode_name]
        if action not in {'probe', 'backup', 'upgrade', 'inspect'}:
            raise ValueError
        selection = schema_database_selection(runtime, projection, mode, storage=storage)
        if action in {'probe', 'backup'} and (predecessor is not None or checkpoint is not None):
            raise ValueError
        source = schema_original_resources(runtime, storage=storage)
        publication = projection.data_publication if mode == 'shared' else projection.manager_publication
        name = 'loom-schema-' + phase + '-' + selection.operation_id.hex
        metadata = {'name': name, 'namespace': selection.namespace, 'labels': {MARKER: str(selection.operation_id)}}
        documents = []
        if action == 'backup':
            # Fresh dev never installed a backup CronJob or bucket credential.
            # Retain the installed manager dump/uploader contract for both DBs.
            cron = source['CronJob:loom-nebius-management-dev:loom-platform-backup']
            spec = copy.deepcopy(cron['spec']['jobTemplate']['spec'])
            pod = spec['template']['spec']
            container, = pod['containers']
            dump, = pod['initContainers']
            expected_image = runtime.request.database.foundation.inputs.config['backup_image']
            if (dump['name'] != 'pg-dump' or dump['image'] != expected_image
                    or dump['command'] != ['pg_dump', '--format=custom', '--no-owner', '--file=/backup/loom.dump']):
                raise ValueError
            backup_secret = 'loom-platform-storage'
            if mode == 'shared':
                foundation = runtime.request.database.foundation
                admin = make_url(database_url(foundation.bootstrap['material']['loom-platform-db']['admin-url'], 'loom-dev'))
                environment = {row['name']: row for row in dump['env']}
                if (admin.username != environment['PGUSER']['value'] or str(admin.port) != str(environment['PGPORT']['value'])
                        or admin.database != environment['PGDATABASE']['value']
                        or dict(admin.query) != {'sslmode': 'verify-full', 'sslrootcert': '/var/run/loom-db/ca.crt'}):
                    raise ValueError
                environment['PGHOST']['value'] = admin.host
                scratch, = (row for row in pod['volumes'] if row['name'] == 'dump')
                scratch['emptyDir'] = {'sizeLimit': str(foundation.inputs.config['postgres_storage_gi']) + 'Gi'}
                # The borrowed template reserved the manager database's size.
                # Scheduler accounting and eviction limits must cover this DB's
                # dump while both the init container and uploader use the volume.
                size = str(foundation.inputs.config['postgres_storage_gi'] * 1024) + 'Mi'
                for process in (dump, container):
                    for allocation in ('requests', 'limits'):
                        process['resources'][allocation]['ephemeral-storage'] = size
                retained = source['Secret:loom-nebius-management-dev:loom-platform-storage']
                keys = ('backup-access-key', 'backup-secret-key')
                material = {key: retained['data'][key] for key in keys}
                if any(not base64.b64decode(value, validate=True) for value in material.values()):
                    raise ValueError
                backup_secret = name
                documents.append({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': copy.deepcopy(metadata),
                    'type': 'Opaque', 'immutable': True, 'data': material})
            config = projection.before_manager.installation.foundation.platform_config
            payload = {'namespace': selection.namespace, 'region': config['region'],
                'storage_endpoint': config['storage_endpoint'], 'buckets': {'backup': projection.before_manager.backup_bucket}}
            container['command'] = ['python', '-m', 'loom.nebius_platform_bootstrap', 'backup']
            container['env'] = [{'name': 'LOOM_PLATFORM_CONFIG', 'value': '/var/run/loom-platform/environment.json'},
                _secret_env('LOOM_BACKUP_ACCESS_KEY', backup_secret, 'backup-access-key'),
                _secret_env('LOOM_BACKUP_SECRET_KEY', backup_secret, 'backup-secret-key')]
            container['volumeMounts'] = [
                {'name': 'platform-config', 'mountPath': '/var/run/loom-platform', 'readOnly': True},
                {'name': 'dump', 'mountPath': '/backup', 'readOnly': True}]
            config_volume, = (row for row in pod['volumes'] if row['name'] == 'platform-config')
            config_volume['configMap'] = {'name': name, 'items': [{'key': 'environment.json', 'path': 'environment.json'}]}
            filename = 'environment.json'
        else:
            original, = (row for row in runtime.request.database.database if row['kind'] == 'Job')
            spec = copy.deepcopy(original['spec'])
            pod = spec['template']['spec']
            pod.pop('initContainers', None)
            container, = pod['containers']
            container['command'] = ['python', '-m', 'loom.nebius_development_schema_database', action]
            container['env'] = [{'name': 'LOOM_DEVELOPMENT_SCHEMA_CONFIG', 'value': '/var/run/loom-schema/request.json'},
                _secret_env('LOOM_DB_URL', 'loom-platform-db', 'admin-url')]
            if mode == 'shared' and action == 'upgrade':
                password, = (row for row in source.values() if row['kind'] == 'Secret'
                    and row['metadata'].get('namespace') == 'loom-dev'
                    and row['metadata']['name'].startswith('loom-applications-manager-'))
                if password.get('immutable') is not True or set(password['data']) != {'password'}:
                    raise ValueError
                container['env'].append(_secret_env('LOOM_APPLICATION_MANAGER_PASSWORD', password['metadata']['name'], 'password'))
            ca, = (row for row in pod['volumes'] if row['name'] == 'db-ca')
            mount, = (row for row in container['volumeMounts'] if row['name'] == 'db-ca')
            if (ca['secret']['secretName'] != 'loom-platform-db'
                    or mount['mountPath'] != '/var/run/loom-db' or mount.get('readOnly') is not True):
                raise ValueError
            pod['volumes'] = [ca, {'name': 'schema-config', 'configMap': {
                'name': name, 'items': [{'key': 'request.json', 'path': 'request.json'}]}}]
            container['volumeMounts'] = [mount, {'name': 'schema-config', 'mountPath': '/var/run/loom-schema', 'readOnly': True}]
            payload = selection.model_dump(mode='json')
            if action in {'upgrade', 'inspect'}:
                if not isinstance(checkpoint, dict) or not checkpoint or predecessor is None:
                    raise ValueError
                payload = DevelopmentSchemaUpgrade.model_validate({'selection': payload, 'predecessor': predecessor,
                    'checkpoint_sha256': hashlib.sha256(_canonical(checkpoint).encode()).hexdigest()}).model_dump(mode='json')
            filename = 'request.json'
        container['image'] = publication.bundle.candidate['images']['service']['image_ref']
        container.pop('args', None)
        pod.update(serviceAccountName='loom-platform', automountServiceAccountToken=False, restartPolicy='Never')
        spec.update(backoffLimit=0, completions=1, parallelism=1)
        spec.pop('ttlSecondsAfterFinished', None)
        spec.pop('selector', None)
        spec.pop('manualSelector', None)
        spec['template']['metadata'] = {'labels': {'app': name, MARKER: str(selection.operation_id)}}
        documents.extend([
            {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': copy.deepcopy(metadata),
                'immutable': True, 'data': {filename: _canonical(payload)}},
            {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': metadata, 'spec': spec},
        ])
        return {_key(document): document for document in documents}
    except Exception:
        raise ValueError('development_schema_job_documents_unqualified') from None
