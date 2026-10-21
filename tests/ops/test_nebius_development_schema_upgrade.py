"""The connected schema parent preserves ancestry and ambiguous-write evidence."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops import nebius_certificates as state
from scripts.ops import nebius_development_schema_upgrade as schema
from scripts.ops.nebius_development_runtime_entry import DevelopmentDataRuntimeInputs
from scripts.ops.nebius_development_runtime_retained import RetainedRuntimeReference
from scripts.ops.nebius_development_schema_catalog import _identity as catalog_identity
from scripts.ops.nebius_development_schema_jobs import schema_database_selection
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_development_runtime_transition import RuntimeAPI
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
from tests.support.nebius_phase_api import PhaseAPI

from loom.nebius_development_schema_database import DevelopmentSchemaProbe, DevelopmentSchemaUpgrade

pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True),
    pytest.mark.parametrize('published_source', ['0175'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def publication_inputs(value):
    return DevelopmentDataRuntimeInputs(source=value.source, publication=value.publication,
        candidate=value.bundle.candidate, profile=value.bundle.profile)


def retain_runtime_inputs(runtime, connection):
    """Retain the same v2 input history produced by protected runtime entry."""
    request = runtime.request
    manager = request.database.manager.retained.request.retained
    owner = Path(manager.operation['inputs_path']).parents[3]
    root = owner / '.loom/nebius-development-runtime' / manager.binding.installation_id / str(request.database.operation_id)
    root.mkdir(mode=0o700, parents=True)
    material = {}
    for field, raw in (('collector_credential_file', request.collector_credential),
            ('registry_credential_file', request.registry_credential),
            ('actuator_password_file', b'runtime-actuator-' + b'p' * 40),
            ('batch_runner_token_file', b'loom_br_' + b'r' * 64)):
        path = root / (field + '.private')
        state._write_private(path, raw)
        material[field] = str(path)
    publication = request.database.manager.publication
    state._atomic_json(root / 'development-runtime-source.json', publication.source.model_dump(mode='json'))
    inputs = dict(schema_version='loom.nebius-development-runtime-inputs.v2',
        retained=request.database.reference.model_dump(mode='json'),
        publication=publication.publication.model_dump(mode='json'),
        publication_source=publication.source.model_dump(mode='json'),
        candidate=publication.bundle.candidate, profile=publication.bundle.profile,
        data_runtime=publication_inputs(request.database.data_runtime).model_dump(mode='json'),
        collector_scope=request.collector_scope.model_dump(mode='json'),
        registry_scope=request.registry_scope.model_dump(mode='json'),
        operator_connection=connection, **material)
    path = root / 'inputs.json'
    state._atomic_json(path, inputs)
    operation = dict(schema='loom.nebius-development-runtime-operation.v1',
        source_sha=publication.source.source_sha, installation_id=manager.binding.installation_id,
        namespace=manager.binding.namespace, operation_id=str(request.database.operation_id),
        inputs_path=str(path), inputs_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    operation_path = root / 'operation.json'
    state._atomic_json(operation_path, operation)
    return RetainedRuntimeReference(operation_path=operation_path,
        operation_sha256=hashlib.sha256(operation_path.read_bytes()).hexdigest())


def retain_schema_upgrade(successor_inputs, connection, tmp_path, *, runtime_reference=None):
    runtime_reference = runtime_reference or retain_runtime_inputs(successor_inputs['runtime'], connection)
    storage = successor_inputs.get('storage')
    inputs = schema.DevelopmentSchemaInputs(runtime=runtime_reference,
        data_runtime=publication_inputs(successor_inputs['data_publication']),
        manager_runtime=publication_inputs(successor_inputs['manager_publication']), connection=connection,
        storage=None if storage is None else storage.retained.reference)
    path = tmp_path / 'schema-inputs.json'
    state._atomic_json(path, inputs.model_dump(mode='json'))
    reference = schema.DevelopmentSchemaReference(operation_id=successor_inputs['operation_id'],
        inputs_path=path, inputs_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return schema.load_development_schema_upgrade(reference)


@pytest.fixture
def schema_retained(successor_inputs, completed_pool, tmp_path):
    return retain_schema_upgrade(successor_inputs, completed_pool[1]['operator_connection'], tmp_path)


class SchemaParentAPI:
    """Only external Kubernetes, DB and object-store effects are simulated."""

    def __init__(self, retained):
        self.retained = retained
        binding = retained.runtime.request.database.manager.retained.request.retained.binding
        self.store = PhaseAPI(binding)
        self.store.key = _key
        self.store.resources.update(copy.deepcopy(retained.plan.source))
        for row in self.store.resources.values():
            row['metadata']['resourceVersion'] = '1'
        self.transition = RuntimeAPI({})
        self.transition.rows = self.store.resources
        self.transition.ready = True
        self.pending = None
        self.catalog_state = 'ready'
        self.catalog_commits = 0
        self.catalog_failure = None
        self.visits = []
        self.reports = {}
        self.qualifications = 0
        self.verify_ready = True
        self.verify_calls = 0

    def qualify(self, retained, record):
        assert retained == self.retained
        if record is not None:
            assert json.loads((retained.state_dir / 'operation.json').read_bytes()) == record
        self.qualifications += 1

    def resources(self, phase, documents):
        self.visits.append(phase)
        return self.store

    def workloads(self, phase, originals, targets):
        self.visits.append(phase)
        return self.transition

    def report(self, phase, record):
        if phase == self.pending:
            return None
        if phase in self.reports:
            return copy.deepcopy(self.reports[phase])
        mode, action = phase.split('-')
        selection = schema_database_selection(self.retained.runtime, self.retained.projection, mode,
            storage=self.retained.storage)
        documents = schema.phase_documents(self.retained, record, phase)
        job, = (row for row in documents.values() if row['kind'] == 'Job')
        job_uid = self.store.resources[_key(job)]['metadata']['uid']
        if action == 'backup':
            return dict(job_uid=job_uid, key=selection.namespace + '/2026/10/20/120000-' + 'b' * 12 + '.dump',
                sha256='b' * 64, bytes=8192)
        probe = DevelopmentSchemaProbe(selection_sha256=checksum(selection.model_dump(mode='json')),
            revision=selection.predecessor_revision, database_oid=18000 if mode == 'shared' else 18001,
            system_identifier='123456789', authority_sha256='a' * 64, grants_sha256='b' * 64)
        if action == 'probe':
            payload = probe.model_dump(mode='json')
        else:
            config, = (row for row in documents.values() if row['kind'] == 'ConfigMap')
            request = DevelopmentSchemaUpgrade.model_validate_json(config['data']['request.json'])
            successor = probe.model_copy(update={'revision': selection.candidate_revision, 'grants_sha256': 'c' * 64})
            payload = dict(schema_version='loom.development-schema-upgrade.v1', status='complete',
                operation_id=str(selection.operation_id), mode=mode,
                request_sha256=checksum(request.model_dump(mode='json')), revision=selection.candidate_revision,
                checkpoint_sha256=request.checkpoint_sha256, successor=successor.model_dump(mode='json'))
        result = dict(job_uid=job_uid, pod_uid=str(uuid4()), database=payload)
        self.reports[phase] = result
        return copy.deepcopy(result)

    def catalog_observe(self):
        return dict(catalog_identity(self.retained.projection.catalog_request), read_only=True, state=self.catalog_state)

    def catalog_commit(self):
        self.catalog_commits += 1
        if self.catalog_failure == 'before':
            raise OSError('unknown dispatch')
        self.catalog_state = 'complete'
        if self.catalog_failure == 'after':
            raise OSError('unknown reply')

    def verify(self, record):
        self.verify_calls += 1
        return self.verify_ready

    def writes(self):
        return list(self.store.creates), list(self.transition.patches), self.catalog_commits


@pytest.fixture
def completed_schema(schema_retained):
    api = SchemaParentAPI(schema_retained)
    assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='apply')['status'] == 'complete'
    completed = schema.load_completed_development_schema_successor(runtime=schema_retained.runtime.request,
        reference=schema_retained.reference)
    return schema_retained, api, completed


def test_parent_completes_all_phases_without_rewriting_ancestry_and_replays(completed_schema):
    retained, api, completed = completed_schema
    assert completed.runtime == retained.runtime
    assert completed.reference == retained.reference
    assert completed.projection == retained.projection
    assert all(path.read_bytes() == raw for path, raw in retained.runtime.files.items())
    skipped = {'catalog', 'verify'} | (set() if retained.plan.updates else {'policy'})
    assert api.visits == [phase for phase in schema.PHASES if phase not in skipped]
    assert api.catalog_commits == 1
    assert all(row['status'] == 'complete' for row in schema._record(retained)['phases'].values())
    assert completed.resources['Deployment:loom-dev:loom-service']['spec']['replicas'] == 1
    assert completed.resources['Deployment:loom-nebius-management-dev:loom-pool-gateway']['spec']['replicas'] == 0
    writes = api.writes()
    raw = {path: path.read_bytes() for path in completed.files}
    for action in ('apply', 'recover', 'status'):
        assert schema.run_development_schema_upgrade(retained=retained, api=api, action=action)['status'] == 'complete'
    assert api.writes() == writes
    assert schema.load_completed_development_schema_successor(runtime=retained.runtime.request,
        reference=retained.reference) == completed
    assert all(path.read_bytes() == data for path, data in raw.items())


def test_prepare_has_no_remote_effect_and_recover_never_creates_new_operation(schema_retained):
    api = SchemaParentAPI(schema_retained)
    assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='prepare')['status'] == 'prepared'
    assert not schema_retained.state_dir.exists()
    assert not schema._anchor(schema_retained).exists()
    assert api.writes() == ([], [], 0)
    with pytest.raises(ValueError, match='recovery_missing'):
        schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='recover')


def test_same_invocation_prechild_observation_survives_real_stage_callbacks(schema_retained):
    class ObservingAPI(SchemaParentAPI):
        def resources(self, phase, documents):
            store = super().resources(phase, documents)
            def verify_identity(binding):
                assert binding == store.binding
                path = schema._child(schema_retained, phase)
                schema._record(schema_retained, _prechild_phase=phase if not path.exists() else None)
            store.verify_identity = verify_identity
            return store
    api = ObservingAPI(schema_retained)
    api.pending = 'shared-probe'
    assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='apply')['phase'] == 'shared-probe'
    with pytest.raises(ValueError, match='prechild'):
        schema._record(schema_retained, _prechild_phase='shared-probe')


def test_waiting_job_resumes_from_one_identity_and_refuses_lost_child(schema_retained):
    api = SchemaParentAPI(schema_retained)
    api.pending = 'shared-probe'
    assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='apply')['phase'] == 'shared-probe'
    writes = api.writes()
    assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='recover')['phase'] == 'shared-probe'
    assert api.writes() == writes
    child = schema._child(schema_retained, 'shared-probe')
    raw = child.read_bytes()
    child.unlink()
    # Even before the first write, a resumed started phase cannot distinguish a
    # lost journal from an interruption before journal creation.
    for action in ('status', 'apply', 'recover'):
        with pytest.raises(ValueError, match='child_missing'):
            schema.run_development_schema_upgrade(retained=schema_retained, api=api, action=action)
    assert api.writes() == writes
    state._write_private(child, raw)
    changed = json.loads(raw)
    changed['revision'] = 'sha256:' + 'f' * 64
    state._atomic_json(child, changed)
    with pytest.raises((ValueError, RuntimeError)):
        schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='status')
    child.write_bytes(raw)
    api.pending = None
    assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='recover')['status'] == 'complete'


@pytest.mark.parametrize('effect', ['patch', 'create', 'catalog'])
@pytest.mark.parametrize('failure', ['before', 'after'])
def test_unknown_effect_is_observed_without_dispatching_again(schema_retained, effect, failure):
    api = SchemaParentAPI(schema_retained)
    if effect == 'patch':
        api.transition.failure = failure
    elif effect == 'create':
        api.store.failure = failure
    else:
        api.catalog_failure = failure
    if failure == 'after':
        assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='apply')['status'] == 'complete'
        writes = api.writes()
        assert schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='recover')['status'] == 'complete'
        assert api.writes() == writes
    else:
        with pytest.raises((ValueError, RuntimeError)):
            schema.run_development_schema_upgrade(retained=schema_retained, api=api, action='apply')
        writes = api.writes()
        for action in ('apply', 'recover'):
            with pytest.raises((ValueError, RuntimeError)):
                schema.run_development_schema_upgrade(retained=schema_retained, api=api, action=action)
        assert api.writes() == writes


def test_completed_loader_rejects_rewritten_stage_even_if_parent_digest_is_updated(completed_schema):
    retained, api, _ = completed_schema
    phase = 'configuration'
    path = schema._child(retained, phase)
    journal = retained.state_dir / 'operation.json'
    original, parent_raw = path.read_bytes(), journal.read_bytes()
    child = json.loads(original)
    key = next(iter(child['resources']))
    child['resources'][key]['observed']['data']['unapproved'] = 'payload'
    state._atomic_json(path, child)
    parent = json.loads(parent_raw)
    parent['phases'][phase]['child_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    state._atomic_json(journal, parent)
    with pytest.raises(ValueError, match='stage_snapshot_changed'):
        schema.load_completed_development_schema_successor(runtime=retained.runtime.request, reference=retained.reference)
    assert api.catalog_commits == 1


def test_completed_loader_rejects_phase_jumps_anchor_and_ancestor_drift(completed_schema, monkeypatch):
    from scripts.ops import nebius_shared_pool_entry as runtime_entry

    retained, api, completed = completed_schema
    load_runtime = runtime_entry.load_completed_runtime
    runtime_loads = []

    def counted_runtime(request):
        runtime_loads.append(request)
        return load_runtime(request)

    monkeypatch.setattr(runtime_entry, 'load_completed_runtime', counted_runtime)
    monkeypatch.setattr(schema, 'load_completed_runtime', counted_runtime, raising=False)
    journal = retained.state_dir / 'operation.json'
    parent_raw = journal.read_bytes()
    changed = json.loads(parent_raw)
    changed['phases']['manager-upgrade'] = dict(status='prepared', proof=None, child_sha256=None)
    state._atomic_json(journal, changed)
    with pytest.raises(ValueError):
        schema.load_completed_development_schema_successor(runtime=retained.runtime.request, reference=retained.reference)
    journal.write_bytes(parent_raw)
    for path in (schema._anchor(retained), next(iter(retained.runtime.files))):
        raw = path.read_bytes()
        path.write_bytes(b'{}')
        with pytest.raises((ValueError, RuntimeError)):
            schema.load_completed_development_schema_successor(runtime=retained.runtime.request, reference=retained.reference)
        path.write_bytes(raw)
    runtime_loads.clear()
    assert schema.load_completed_development_schema_successor(runtime=retained.runtime.request,
        reference=retained.reference) == completed
    assert len(runtime_loads) == 1, 'one schema load reconstructed its completed runtime more than once'

    # Reusing the fresh projection must still reject changes during that call.
    project = schema.schema_runtime_resources
    ancestor, raw = next(iter(retained.runtime.files.items()))

    def changed_ancestor(**kwargs):
        result = project(**kwargs)
        ancestor.write_bytes(raw + b' ')
        return result

    monkeypatch.setattr(schema, 'schema_runtime_resources', changed_ancestor)
    try:
        with pytest.raises(ValueError, match='development_schema_history_changed'):
            schema.load_completed_development_schema_successor(runtime=retained.runtime.request,
                reference=retained.reference)
    finally:
        ancestor.write_bytes(raw)
    assert api.catalog_commits == 1


def test_phase_proofs_reject_cross_database_checkpoint_and_changed_poststart_grants(completed_schema):
    retained, _, _ = completed_schema
    record = schema._record(retained)
    wrong_probe = copy.deepcopy(record['phases']['shared-probe']['proof'])
    wrong_probe['database'] = record['phases']['manager-probe']['proof']['database']
    wrong_backup = copy.deepcopy(record['phases']['shared-backup']['proof'])
    wrong_backup['key'] = record['phases']['manager-backup']['proof']['key']
    wrong_upgrade = copy.deepcopy(record['phases']['shared-upgrade']['proof'])
    wrong_upgrade['database']['checkpoint_sha256'] = '0' * 64
    wrong_inspection = copy.deepcopy(record['phases']['shared-inspect']['proof'])
    wrong_inspection['database']['successor']['grants_sha256'] = 'd' * 64
    for phase, proof in (('shared-probe', wrong_probe), ('shared-backup', wrong_backup),
            ('shared-upgrade', wrong_upgrade), ('shared-inspect', wrong_inspection)):
        with pytest.raises(ValueError):
            schema.validate_phase_proof(retained, record, phase, proof)


def test_document_callers_cannot_modify_frozen_schema_plan(completed_schema):
    retained, _, _ = completed_schema
    original = copy.deepcopy(retained)
    config = schema.phase_documents(retained, schema._record(retained), 'configuration')
    next(iter(config.values()))['data']['foreign'] = 'value'
    before, after = schema.transition_documents(retained, 'stop')
    next(iter(before.values()))['metadata']['name'] = 'foreign'
    next(iter(after.values()))['metadata']['name'] = 'foreign'
    assert retained == original
