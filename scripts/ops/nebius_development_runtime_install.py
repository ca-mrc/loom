"""Fixed fresh-dev runtime installation intent and independent phase inventory.

The protected parent must consume this projection with anchored stage/transition
evidence. Preparing it performs no I/O beyond retained private-history reads and
is neither a live prerequisite proof nor authority to activate the physical pool.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
from scripts.ops.nebius_development_build_cloud import (
    DevelopmentRegistryCloudScope,
    registry_public_key,
)
from scripts.ops.nebius_development_build_policy import prepare_build_policy
from scripts.ops.nebius_development_build_runtime import prepare_build_runtime
from scripts.ops.nebius_development_catalog_runtime import (
    catalog_runtime_documents,
    validate_catalog_runtime_proof,
)
from scripts.ops.nebius_development_collector_cloud import (
    DevelopmentCollectorCloudScope,
    collector_public_key,
)
from scripts.ops.nebius_development_collector_runtime import (
    prepare_collector_material,
    prepare_collector_runtime,
)
from scripts.ops.nebius_development_network_runtime import prepare_network_runtime
from scripts.ops.nebius_development_runtime_setup import (
    DevelopmentDatabaseRuntime,
    database_runtime_documents,
    validate_database_runtime_proof,
)
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    advance_runtime_workloads,
    validate_runtime_transition,
)
from scripts.ops.nebius_development_shared_runtime import prepare_shared_runtime
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _defaulted,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_supplied import _defaulted as _secret_defaulted

from loom.nebius_platform_render import _obj, digest
from loom_service.environment_management.candidates import _json

_PHASES = ('database', 'material', 'authority', 'isolation', 'workloads',
    'stop', 'replace', 'control', 'catalog', 'start')


@dataclass(frozen=True, repr=False)
class DevelopmentRuntimeInstallRequest:
    database: DevelopmentDatabaseRuntime
    collector_scope: DevelopmentCollectorCloudScope
    collector_credential: bytes
    registry_scope: DevelopmentRegistryCloudScope
    registry_credential: bytes
    cache_material: dict[str, str] | None = None


@dataclass(frozen=True, repr=False)
class DevelopmentRuntimePlan:
    fixed: dict[str, dict[str, dict[str, Any]]]
    originals: dict[str, dict[str, Any]]
    stopped: dict[str, dict[str, Any]]
    targets: dict[str, dict[str, Any]]
    input_digest: str


def prepare_runtime_install(request: DevelopmentRuntimeInstallRequest) -> DevelopmentRuntimePlan:
    """Freeze every original/target and reject incomplete material before writes.

    Runtime phases must requalify this request, publication and live identities.
    A caller-held plan is a comparison value, never arbitrary manifest input.
    SQL and catalog Jobs remain separate ordered stages, not generic workloads.
    """
    try:
        request = copy.deepcopy(request)
        database = request.database
        config = database.foundation.inputs.config
        spec = database.manager.retained.request.registration.spec
        participant, = spec.participants
        namespaces = {'loom-dev', database.manager.retained.request.retained.binding.namespace,
            participant.execution_namespace.name, participant.build_namespace.name}
        if len(namespaces) != 4:
            raise ValueError
        collector_key = collector_public_key(scope=request.collector_scope, config=config,
            credential=request.collector_credential)
        repositories = {profile.settings.registry_repository for profile in spec.profiles.task_images}
        repositories.update(profile.settings.registry_repository for profile in spec.profiles.application_images)
        registry_key = registry_public_key(scope=request.registry_scope, config=config,
            credential=request.registry_credential, repositories=tuple(sorted(repositories)))
        if (collector_key == registry_key or any(getattr(request.collector_scope, field) == getattr(request.registry_scope, field)
                for field in ('account_id', 'key_id', 'group_id'))):
            raise ValueError
        fixed: dict[str, dict[str, dict[str, Any]]] = {
            phase: {} for phase in ('database', 'material', 'authority', 'isolation', 'workloads', 'catalog')}
        classification = {'Secret': 'material', 'ConfigMap': 'material',
            'ServiceAccount': 'authority', 'Role': 'authority', 'RoleBinding': 'authority',
            'ClusterRole': 'authority', 'ClusterRoleBinding': 'authority',
            'NetworkPolicy': 'isolation', 'ValidatingAdmissionPolicy': 'isolation',
            'ValidatingAdmissionPolicyBinding': 'isolation', 'Deployment': 'workloads', 'CronJob': 'workloads'}
        cluster_kinds = {'ClusterRole', 'ClusterRoleBinding', 'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}
        seen: set[str] = set()

        def add(document: dict[str, Any], phase: str | None = None) -> None:
            key, kind = _key(document), document['kind']
            namespace = document['metadata'].get('namespace')
            if (key in seen or (kind in cluster_kinds and namespace is not None)
                    or (kind not in cluster_kinds and namespace not in namespaces)
                    or any(not set(rule['verbs']) <= {'get', 'list', 'watch'} for rule in document.get('rules', []))):
                raise ValueError
            seen.add(key)
            fixed[phase or classification[kind]][key] = copy.deepcopy(document)

        for phase, documents in (('database', database_runtime_documents(database)),
                ('catalog', catalog_runtime_documents(database))):
            for document in documents.values():
                add(document, phase)
        actuator = prepare_actuator_runtime(database)
        collector = prepare_collector_runtime(database)
        shared = prepare_shared_runtime(database)
        for document in (*database.manager.delivery.configuration, actuator.deployment, *actuator.authority,
                collector.configuration, collector.cronjob, *collector.authority,
                prepare_collector_material(database, scope=request.collector_scope, credential=request.collector_credential),
                *prepare_build_runtime(database, registry_scope=request.registry_scope,
                    registry_credential=request.registry_credential, cache_material=request.cache_material),
                *prepare_network_runtime(database), *prepare_build_policy(database)):
            add(document)
        if database.manager.requires_source_material:
            stored = database.foundation.phases['supplied']['resources']['Secret:loom-platform-storage']['desired']['data']
            source = {key: base64.b64decode(stored['source-' + key], validate=True).decode('ascii')
                for key in ('access-key', 'secret-key')}
            secret = _obj('Secret', database.manager.delivery.source_secret_name,
                database.manager.retained.request.retained.binding.namespace)
            secret.update(type='Opaque', immutable=True, data={'credentials.json': base64.b64encode(
                json.dumps(source, sort_keys=True, separators=(',', ':')).encode()).decode()})
            secret['metadata']['labels'] = {'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(database.operation_id)}
            add(secret)
        originals = {_key(document): copy.deepcopy(document) for document in (
            database.manager.original, *shared.original.values())}
        targets = {_key(document): _snapshot(document) for document in (
            database.manager.delivery.deployment, *shared.targets.values())}
        if len(originals) != 3 or originals.keys() != targets.keys() or originals.keys() & seen:
            raise ValueError
        stopped = {}
        for key, original in originals.items():
            _uid(original)
            if targets[key]['spec']['replicas'] != 0:
                raise ValueError
            stopped[key] = _snapshot(original)
            stopped[key]['spec']['replicas'] = 0
        checksum = digest({'reference': database.reference.model_dump(mode='json'),
            'operation_id': str(database.operation_id), 'source': database.manager.publication.source.model_dump(mode='json'),
            'collector': request.collector_scope.model_dump(mode='json'), 'registry': request.registry_scope.model_dump(mode='json'),
            'fixed': fixed, 'originals': originals, 'stopped': stopped, 'targets': targets})
        return DevelopmentRuntimePlan(fixed, originals, stopped, targets, checksum)
    except Exception:
        raise ValueError('development runtime installation intent unqualified') from None


class DevelopmentRuntimeInstallAPI(Protocol):
    """Only the protected, phase-aware adapter may satisfy this live boundary.

Qualification preserves private pins, namespace/storage/publication identity,
closed admission and disabled writer grants; it checks cloud material before
startup. Every child transport independently repeats that qualification before
writes. Inspection is GET-only and validates current successors, fixed receipts,
live process/database bindings and exact SQL/catalog Job proofs.
"""
    def qualify(self, *, plan: DevelopmentRuntimePlan, state_dir: Path, record: dict[str, Any]) -> None: ...
    def resources(self, phase: str) -> ManagementStageAPI: ...
    def workloads(self, phase: str) -> DevelopmentRuntimeWorkloadAPI: ...
    def report(self, phase: str, state: Path) -> dict[str, Any] | None: ...
    def inspect_runtime(self, *, plan: DevelopmentRuntimePlan, state_dir: Path, record: dict[str, Any]) -> None: ...


def _read(path: Path) -> dict[str, Any]:
    if path != path.resolve():
        raise ValueError
    result = _json(private_state._private_read(path, limit=4 * 1024**2))
    if not isinstance(result, dict):
        raise ValueError
    return dict(result)


def _child_path(state: Path, phase: str) -> Path:
    return state / phase / ('transition.json' if phase in {'stop', 'replace', 'control', 'start'} else 'stage.json')


def _hash(path: Path) -> str:
    return hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest()


def runtime_transition_inputs(plan: DevelopmentRuntimePlan, state: Path,
                              phase: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Regenerate fixed transition pairs from prior validated parent receipts."""
    if phase == 'stop':
        return copy.deepcopy(plan.originals), copy.deepcopy(plan.stopped)

    def previous(previous_phase: str) -> dict[str, dict[str, Any]]:
        items = _read(_child_path(state, previous_phase))['resources']
        result = {}
        for key, item in items.items():
            row = copy.deepcopy(item['expected'])
            row['metadata']['uid'] = _uid(plan.originals[key])
            result[key] = row
        return result

    if phase == 'replace':
        return previous('stop'), copy.deepcopy(plan.targets)
    before = previous('replace')
    if phase == 'control':
        before.pop('Deployment:loom-dev:loom-service')
    elif phase == 'start':
        before = {'Deployment:loom-dev:loom-service': before['Deployment:loom-dev:loom-service']}
        for key, item in _read(_child_path(state, 'workloads'))['resources'].items():
            before[key] = copy.deepcopy(item['observed'])
            before[key]['metadata']['uid'] = item['uid']
    else:
        raise ValueError
    targets = {key: _snapshot(row) for key, row in before.items()}
    for row in targets.values():
        if row['kind'] == 'CronJob':
            row['spec']['suspend'] = False
        else:
            row['spec']['replicas'] = 1
    return before, targets


def _validate_runtime_child(request: DevelopmentRuntimeInstallRequest, plan: DevelopmentRuntimePlan,
                            state: Path, phase: str, item: dict[str, Any]) -> None:
    child = _read(_child_path(state, phase))
    if phase in plan.fixed:
        _validate_record(child, {'schema': 'loom.nebius-management-stage.v1',
            'binding': asdict(request.database.manager.retained.request.retained.binding), 'revision': digest(plan.fixed[phase]),
            'phase': 'development-runtime-' + phase}, plan.fixed[phase])
        complete = all(row['status'] == 'created' for row in child['resources'].values())
    else:
        before, targets = runtime_transition_inputs(plan, state, phase)
        validate_runtime_transition(child, originals=before, targets=targets,
            input_digest=plan.input_digest, phase=phase)
        complete = all(row['status'] == 'applied' for row in child['resources'].values())
    if item['status'] == 'complete':
        if not complete:
            raise ValueError
        if phase in {'database', 'catalog'}:
            validator = validate_database_runtime_proof if phase == 'database' else validate_catalog_runtime_proof
            validator(request.database, state / phase, item['proof'])
    elif item['proof'] is not None:
        raise ValueError


def _runtime_record(request: DevelopmentRuntimeInstallRequest, plan: DevelopmentRuntimePlan,
                    state: Path, anchor: Path) -> dict[str, Any]:
    manager = request.database.manager.retained.request.retained
    if (state != Path(manager.operation['state_dir']).parent / 'runtime-installation'
            or anchor != Path(manager.operation['anchor_dir'])
            or state != state.resolve() or anchor != anchor.resolve() or not anchor.is_dir()):
        raise ValueError
    identity = {'schema': 'loom.nebius-development-runtime-install.v1',
        'operation_id': str(request.database.operation_id), 'state_dir': str(state), 'input_digest': plan.input_digest}
    marker = anchor / 'runtime-installation.json'
    if marker.exists() or marker.is_symlink():
        if _read(marker) != identity:
            raise ValueError
        record = _read(state / 'installation.json')
        if (set(record) != {*identity, 'phases'} or any(record[key] != value for key, value in identity.items())
                or set(record['phases']) != set(_PHASES)):
            raise ValueError
        unfinished = False
        for phase in _PHASES:
            item = record['phases'][phase]
            if (set(item) != {'status', 'sha256', 'proof'} or item['status'] not in {'prepared', 'started', 'complete'}
                    or (unfinished and item['status'] != 'prepared')
                    or (item['status'] == 'complete') != (item['sha256'] is not None)
                    or (phase not in {'database', 'catalog'} and item['proof'] is not None)):
                raise ValueError
            path = _child_path(state, phase)
            if item['status'] == 'prepared':
                if path.parent.exists() or path.parent.is_symlink() or item['proof'] is not None:
                    raise ValueError
            else:
                if item['status'] == 'complete' and _hash(path) != item['sha256']:
                    raise ValueError
                _validate_runtime_child(request, plan, state, phase, item)
            unfinished |= item['status'] != 'complete'
        return record
    if state.exists() or state.is_symlink():
        raise ValueError
    return {**identity, 'phases': {phase: {'status': 'prepared', 'sha256': None, 'proof': None} for phase in _PHASES}}


def runtime_workload_options(*, request: DevelopmentRuntimeInstallRequest,
                             state_dir: Path) -> dict[str, tuple[dict[str, Any], ...]]:
    """Read anchored original-or-successor options without replaying any phase.

    Ambiguous writes retain both sides for observation only: this never grants a
    retry or asserts readiness. Missing child history is not an empty operation.
    The connected parent's pre-child callback uses its just-validated in-memory
    frame until the first child journal exists; this reader cannot resume it.
    """
    try:
        plan = prepare_runtime_install(request)
        anchor = Path(request.database.manager.retained.request.retained.operation['anchor_dir'])
        record = _runtime_record(request, plan, state_dir, anchor)
        paths = {_child_path(state_dir, phase) for phase, item in record['phases'].items() if item['status'] != 'prepared'}
        hashes = {path: _hash(path) for path in paths}
        result: dict[str, tuple[dict[str, Any], ...]] = {key: (copy.deepcopy(row),) for key, row in plan.originals.items()}
        for phase in _PHASES:
            if record['phases'][phase]['status'] == 'prepared':
                continue
            child = _read(_child_path(state_dir, phase))
            if phase == 'workloads':
                for key, item in child['resources'].items():
                    if item['status'] == 'created':
                        original = copy.deepcopy(item['observed'])
                        original['metadata']['uid'] = item['uid']
                        _uid(original)
                        result[key] = (original,)
            elif phase not in plan.fixed:
                before, _ = runtime_transition_inputs(plan, state_dir, phase)
                for key, item in child['resources'].items():
                    original = before[key]
                    if item['status'] == 'prepared':
                        result[key] = (original,)
                    else:
                        expected = copy.deepcopy(item['expected'])
                        expected['metadata']['uid'] = _uid(original)
                        result[key] = (expected,) if item['status'] == 'applied' else (original, expected)
        if (_runtime_record(request, plan, state_dir, anchor) != record
                or any(_hash(path) != value for path, value in hashes.items())):
            raise ValueError
        return result
    except Exception:
        raise ValueError('development runtime workload history unqualified') from None


def install_development_runtime(*, request: DevelopmentRuntimeInstallRequest,
                                api: DevelopmentRuntimeInstallAPI, execute: bool) -> dict[str, Any]:
    """Connected closed-runtime installation, not pool admission or migration.

The retained manager owns an independent phase-start anchor. Missing/changed
history is never adopted. Finished phases are inspected without replaying writes
or requiring superseded original workloads to remain installed forever.
"""
    try:
        if type(execute) is not bool:
            raise ValueError
        request = copy.deepcopy(request)
        plan = prepare_runtime_install(request)
        manager = request.database.manager.retained.request.retained
        state = Path(manager.operation['state_dir']).parent / 'runtime-installation'
        anchor = Path(manager.operation['anchor_dir'])
        marker, journal = anchor / 'runtime-installation.json', state / 'installation.json'
        if state != state.resolve() or anchor != anchor.resolve() or not anchor.is_dir():
            raise ValueError
        identity = {'schema': 'loom.nebius-development-runtime-install.v1',
            'operation_id': str(request.database.operation_id), 'state_dir': str(state), 'input_digest': plan.input_digest}
        base = {'operation_id': identity['operation_id'], 'installation_id': manager.binding.installation_id,
            'namespace': manager.binding.namespace, 'admission_open': False, 'writer_migration_complete': False}

        def validate_child(phase: str, item: dict[str, Any]) -> None:
            _validate_runtime_child(request, plan, state, phase, item)

        def read_record() -> dict[str, Any]:
            return _runtime_record(request, plan, state, anchor)

        def qualify(record: dict[str, Any]) -> None:
            api.qualify(plan=plan, state_dir=state, record=copy.deepcopy(record))

        record = read_record()
        qualify(record)
        if not execute:
            return {**base, 'status': 'development_runtime_preflight_qualified'}
        with private_state._locked_state(anchor / 'runtime-installation-lock'):
            record = read_record()
            qualify(record)
            if not marker.exists():
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                private_state._atomic_json(journal, record)
            for phase in _PHASES:
                item = record['phases'][phase]
                qualify(record)
                if item['status'] == 'complete':
                    continue
                if item['status'] == 'prepared':
                    item['status'] = 'started'
                    private_state._atomic_json(journal, record)
                proof = None
                if phase in plan.fixed:
                    def default(child_api: ManagementStageAPI, desired: dict[str, Any]) -> dict[str, Any]:
                        return (_secret_defaulted if desired['kind'] == 'Secret' else _defaulted)(child_api, desired)
                    _stage_fixed_documents(documents=plan.fixed[phase], revision=digest(plan.fixed[phase]),
                        phase='development-runtime-' + phase, binding=manager.binding, api=api.resources(phase),
                        state_dir=state / phase, default_document=default)
                    if phase in {'database', 'catalog'}:
                        proof = api.report(phase, state / phase)
                        if proof is None:
                            qualify(record)
                            return {**base, 'status': 'pending_' + phase}
                        validator = validate_database_runtime_proof if phase == 'database' else validate_catalog_runtime_proof
                        validator(request.database, state / phase, proof)
                else:
                    before, targets = runtime_transition_inputs(plan, state, phase)
                    if not advance_runtime_workloads(originals=before, targets=targets, input_digest=plan.input_digest,
                            phase=phase, api=api.workloads(phase), state_dir=state / phase):
                        qualify(record)
                        return {**base, 'status': 'pending_' + phase}
                qualify(record)
                item.update(status='complete', sha256=_hash(_child_path(state, phase)), proof=proof)
                private_state._atomic_json(journal, record)
                validate_child(phase, item)
            record = read_record()
            api.inspect_runtime(plan=plan, state_dir=state, record=copy.deepcopy(record))
            qualify(record)
            return {**base, 'status': 'development_runtime_installed_closed'}
    except Exception:
        raise ValueError('development runtime installation unqualified; preserve retained evidence') from None
