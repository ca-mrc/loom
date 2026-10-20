"""Schema Jobs retain DB identity and isolate existing backup credentials."""
from __future__ import annotations

import copy
import hashlib
import importlib
import json

import pytest
from tests.ops.test_nebius_development_schema_projection import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    application_material as application_material,
)
from tests.ops.test_nebius_development_schema_projection import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    capacity_checks as capacity_checks,
)
from tests.ops.test_nebius_development_schema_projection import (
    cloud as cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    collector_cloud as collector_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    completed_pool as completed_pool,
)
from tests.ops.test_nebius_development_schema_projection import (
    connected as connected,
)
from tests.ops.test_nebius_development_schema_projection import (
    development_inputs as development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    entry as entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    handoff as handoff,
)
from tests.ops.test_nebius_development_schema_projection import (
    installation as installation,
)
from tests.ops.test_nebius_development_schema_projection import (
    inventory as inventory,
)
from tests.ops.test_nebius_development_schema_projection import (
    live as live,
)
from tests.ops.test_nebius_development_schema_projection import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    manager_entry as manager_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    material as material,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_cloud as original_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    original_pool_inputs as original_pool_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    pool_entry as pool_entry,
)
from tests.ops.test_nebius_development_schema_projection import (
    pool_inputs as pool_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    preflight as preflight,
)
from tests.ops.test_nebius_development_schema_projection import (
    provider_checks as provider_checks,
)
from tests.ops.test_nebius_development_schema_projection import (
    publication as publication,
)
from tests.ops.test_nebius_development_schema_projection import (
    published_source as published_source,
)
from tests.ops.test_nebius_development_schema_projection import (
    publisher_cloud as publisher_cloud,
)
from tests.ops.test_nebius_development_schema_projection import (
    retained as retained,
)
from tests.ops.test_nebius_development_schema_projection import (
    route as route,
)
from tests.ops.test_nebius_development_schema_projection import (
    source_checkout as source_checkout,
)
from tests.ops.test_nebius_development_schema_projection import (
    successor_inputs as successor_inputs,
)
from tests.ops.test_nebius_development_schema_projection import (
    tls_material as tls_material,
)

pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True),
    pytest.mark.parametrize('published_source', ['0175'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


def jobs_module():
    name = 'scripts.ops.nebius_development_schema_jobs'
    assert importlib.util.find_spec(name), 'fixed schema database Jobs are missing'
    return importlib.import_module(name)


def selected(successor_inputs):
    from scripts.ops.nebius_development_schema_projection import (
        project_development_schema_successor,
    )
    return successor_inputs['runtime'], project_development_schema_successor(**successor_inputs)


def job(documents):
    row, = (row for row in documents.values() if row['kind'] == 'Job')
    assert row['spec']['backoffLimit'] == 0 and 'ttlSecondsAfterFinished' not in row['spec']
    pod = row['spec']['template']['spec']
    assert pod['restartPolicy'] == 'Never' and pod['automountServiceAccountToken'] is False
    return row, pod


def test_probes_bind_independent_publications_and_retained_admin_material(successor_inputs):
    module = jobs_module()
    runtime, projection = selected(successor_inputs)
    original = copy.deepcopy(runtime)
    for mode, namespace, previous in (('shared', 'loom-dev', '0175'), ('manager', 'loom-nebius-management-dev', '0176')):
        selection = module.schema_database_selection(runtime, projection, mode)
        assert (selection.namespace, selection.predecessor_revision, selection.candidate_revision) == (namespace, previous, '0178')
        publication = projection.data_publication if mode == 'shared' else projection.manager_publication
        assert selection.candidate_sha == publication.source.source_sha
        documents = module.schema_job_documents(runtime, projection, mode + '-probe')
        row, pod = job(documents)
        assert row['metadata']['namespace'] == namespace
        container, = pod['containers']
        assert container['image'] == publication.bundle.candidate['images']['service']['image_ref']
        assert container['command'] == ['python', '-m', 'loom.nebius_development_schema_database', 'probe']
        environment = {entry['name']: entry for entry in container['env']}
        assert environment['LOOM_DB_URL']['valueFrom']['secretKeyRef'] == {'name': 'loom-platform-db', 'key': 'admin-url'}
        assert 'LOOM_APPLICATION_MANAGER_PASSWORD' not in environment
        config, = (entry for entry in documents.values() if entry['kind'] == 'ConfigMap')
        assert json.loads(config['data']['request.json']) == selection.model_dump(mode='json')
    assert runtime == original


def test_backups_use_existing_manager_bucket_with_dump_and_uploader_separation(successor_inputs):
    from scripts.ops.nebius_development_schema_resources import schema_original_resources
    module = jobs_module()
    runtime, projection = selected(successor_inputs)
    # Independently sized databases must reserve their own complete dump space.
    runtime = copy.deepcopy(runtime)
    runtime.request.database.foundation.inputs.config['postgres_storage_gi'] = projection.before_manager.postgres_storage_gi + 1
    originals = schema_original_resources(runtime)
    for mode, namespace in (('shared', 'loom-dev'), ('manager', 'loom-nebius-management-dev')):
        documents = module.schema_job_documents(runtime, projection, mode + '-backup')
        row, pod = job(documents)
        assert row['metadata']['namespace'] == namespace
        old = originals['CronJob:loom-nebius-management-dev:loom-platform-backup']['spec']['jobTemplate']['spec']['template']['spec']
        dump, = pod['initContainers']
        expected_dump = copy.deepcopy(old['initContainers'][0])
        if mode == 'shared':
            host, = (entry for entry in expected_dump['env'] if entry['name'] == 'PGHOST')
            host['value'] = 'loom-postgres.loom-dev.svc'
            scratch, = (entry for entry in pod['volumes'] if entry['name'] == 'dump')
            assert scratch['emptyDir']['sizeLimit'] == str(runtime.request.database.foundation.inputs.config['postgres_storage_gi']) + 'Gi'
            for allocation in ('requests', 'limits'):
                expected_dump['resources'][allocation]['ephemeral-storage'] = str(
                    runtime.request.database.foundation.inputs.config['postgres_storage_gi'] * 1024) + 'Mi'
        assert dump == expected_dump
        upload, = pod['containers']
        expected_size = (runtime.request.database.foundation.inputs.config['postgres_storage_gi']
            if mode == 'shared' else projection.before_manager.postgres_storage_gi)
        for container in (dump, upload):
            for allocation in ('requests', 'limits'):
                assert container['resources'][allocation]['ephemeral-storage'] == str(expected_size * 1024) + 'Mi'
        assert upload['command'] == ['python', '-m', 'loom.nebius_platform_bootstrap', 'backup']
        publication = projection.data_publication if mode == 'shared' else projection.manager_publication
        assert upload['image'] == publication.bundle.candidate['images']['service']['image_ref']
        assert {entry['name'] for entry in upload['env']} == {'LOOM_PLATFORM_CONFIG', 'LOOM_BACKUP_ACCESS_KEY', 'LOOM_BACKUP_SECRET_KEY'}
        assert {entry['name'] for entry in upload['volumeMounts']} == {'platform-config', 'dump'}
        config, = (entry for entry in documents.values() if entry['kind'] == 'ConfigMap')
        assert json.loads(config['data']['environment.json']) == {
            'namespace': namespace, 'region': runtime.request.database.foundation.inputs.config['region'],
            'storage_endpoint': runtime.request.database.foundation.inputs.config['storage_endpoint'],
            'buckets': {'backup': projection.before_manager.backup_bucket}}
        secrets = [entry for entry in documents.values() if entry['kind'] == 'Secret']
        if mode == 'shared':
            secret, = secrets
            assert secret['immutable'] is True
            assert secret['metadata']['namespace'] == 'loom-dev'
            assert set(secret['data']) == {'backup-access-key', 'backup-secret-key'}
            original = originals['Secret:loom-nebius-management-dev:loom-platform-storage']['data']
            assert secret['data'] == {key: original[key] for key in secret['data']}
            assert all(entry['valueFrom']['secretKeyRef']['name'] == secret['metadata']['name']
                for entry in upload['env'] if 'valueFrom' in entry)
        else:
            assert secrets == []
            assert all(entry['valueFrom']['secretKeyRef']['name'] == 'loom-platform-storage'
                for entry in upload['env'] if 'valueFrom' in entry)


def test_upgrade_requires_matching_predecessor_and_checkpoint_and_keeps_password_reference(successor_inputs):
    from loom.nebius_development_schema_database import DevelopmentSchemaUpgrade
    module = jobs_module()
    runtime, projection = selected(successor_inputs)
    selection = module.schema_database_selection(runtime, projection, 'shared')
    predecessor = dict(schema_version='loom.development-schema-probe.v1',
        selection_sha256=hashlib.sha256(json.dumps(selection.model_dump(mode='json'), sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        revision='0175', database_oid=123, system_identifier='456', authority_sha256='a' * 64, grants_sha256='b' * 64)
    checkpoint = {'job_uid': '10000000-0000-4000-8000-000000000001',
        'key': 'loom-dev/2026/10/20/120000-aaaaaaaaaaaa.dump', 'sha256': 'a' * 64, 'bytes': 512}
    for phase in ('shared-upgrade', 'shared-inspect'):
        documents = module.schema_job_documents(runtime, projection, phase, predecessor=predecessor, checkpoint=checkpoint)
        _, pod = job(documents)
        config, = (entry for entry in documents.values() if entry['kind'] == 'ConfigMap')
        request = DevelopmentSchemaUpgrade.model_validate_json(config['data']['request.json'])
        assert request.selection == selection and request.predecessor.model_dump(mode='json') == predecessor
        assert request.checkpoint_sha256 == hashlib.sha256(json.dumps(checkpoint, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        environment = {entry['name']: entry for entry in pod['containers'][0]['env']}
        if phase.endswith('upgrade'):
            password = environment['LOOM_APPLICATION_MANAGER_PASSWORD']['valueFrom']['secretKeyRef']
            assert password['name'].startswith('loom-applications-manager-') and password['key'] == 'password'
        else:
            assert 'LOOM_APPLICATION_MANAGER_PASSWORD' not in environment
    manager = module.schema_database_selection(runtime, projection, 'manager')
    manager_predecessor = {**predecessor, 'revision': '0176',
        'selection_sha256': hashlib.sha256(json.dumps(manager.model_dump(mode='json'), sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    manager_checkpoint = {**checkpoint, 'key': 'loom-nebius-management-dev/2026/10/20/120000-aaaaaaaaaaaa.dump'}
    documents = module.schema_job_documents(runtime, projection, 'manager-upgrade',
        predecessor=manager_predecessor, checkpoint=manager_checkpoint)
    row, pod = job(documents)
    assert row['metadata']['namespace'] == 'loom-nebius-management-dev'
    assert 'LOOM_APPLICATION_MANAGER_PASSWORD' not in {entry['name'] for entry in pod['containers'][0]['env']}
    config, = (entry for entry in documents.values() if entry['kind'] == 'ConfigMap')
    assert DevelopmentSchemaUpgrade.model_validate_json(config['data']['request.json']).selection == manager
    for previous, backup in ((None, checkpoint), (predecessor, None), ({**predecessor, 'revision': '0176'}, checkpoint)):
        with pytest.raises(ValueError):
            module.schema_job_documents(runtime, projection, 'shared-upgrade', predecessor=previous, checkpoint=backup)
    with pytest.raises(ValueError):
        module.schema_job_documents(runtime, projection, 'production-upgrade', predecessor=predecessor, checkpoint=checkpoint)
