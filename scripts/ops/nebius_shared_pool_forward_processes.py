"""Fresh database and process observations through shared pool opening.

Only known in-flight stop/start readiness can be pending. A completed transition
is not permission to skip present drain, settings or backend correspondence.
This adapter is read-only; writers, provider capacity and queue drain remain
separate mandatory observations owned by the connected parent.
"""
from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_material import _documents as management_material
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_migration import PoolGuardDatabase
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import (
    KubectlPoolHistoryAPI,
    PoolManagementHistoryTarget,
    derive_management_history_target,
)
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_shared_pool_activation import shared_forward_activation_view
from scripts.ops.nebius_shared_pool_database import validate_shared_staging_database
from scripts.ops.nebius_shared_pool_gateway import shared_forward_gateway_view
from scripts.ops.nebius_shared_pool_guards import validate_shared_pool_guards
from scripts.ops.nebius_shared_pool_handoff import retired_manager_revision
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import _files
from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes
from scripts.ops.nebius_shared_pool_workloads import _frame, shared_forward_staging_options

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json


@dataclass(frozen=True, repr=False)
class SharedPoolForwardProcessObservation:
    bindings: SharedPoolGuardBindings
    pool_state: str
    guard_states: dict[str, str]
    retired_authority: str
    pending_workloads: tuple[str, ...]
    telemetry: dict[str, Any]
    input_digest: str


@dataclass(frozen=True, repr=False)
class _ProcessScope:
    frame: dict[str, Any]
    pool_states: tuple[str, ...]
    activation: bool
    workloads: dict[str, tuple[dict[str, Any], ...]]
    gateway: tuple[dict[str, Any], ...]
    guards: dict[str, tuple[str, ...]]
    staging_acl: bool
    receipts: dict[str, dict[str, str]]


def _scope(bindings: SharedPoolGuardBindings, state: Path) -> _ProcessScope:
    opening = shared_forward_activation_view(bindings=bindings, state_dir=state)
    frame = opening.frame
    guard_options: dict[str, tuple[str, ...]] = {target.namespace: ('open',) for target in bindings.migration.guards}
    status = frame['forward']['forward-guards']['status']
    path = state / 'forward-guards/guards.json'
    if status != 'prepared' or path.exists() or path.is_symlink():
        record = validate_shared_pool_guards(bindings=bindings, state_dir=path.parent, complete=status == 'complete')
        if status == 'prepared' and set(record['guards'].values()) != {'prepared'}:
            raise ValueError
        for target in bindings.migration.guards:
            item = record['guards'][str(target.participant_id)]
            guard_options[target.namespace] = {'prepared': ('open',), 'acquire_intent': ('open', 'held'), 'held': ('held',)}[item]
    if opening.guard_states is not None:
        guard_options = opening.guard_states
    staging_acl = False
    status = frame['forward']['forward-database']['status']
    path = state / 'forward-database/database.json'
    if status != 'prepared' or path.exists() or path.is_symlink():
        record = validate_shared_staging_database(bindings=bindings, state_dir=path.parent,
            complete=status == 'complete', prepared=status == 'prepared')
        staging_acl = record['status'] == 'staged'
    workloads = shared_forward_staging_options(bindings=bindings, state_dir=state)
    gateway = shared_forward_gateway_view(bindings=bindings, state_dir=state)
    gateway_options, = gateway[2].values()
    receipts = {phase: _files(state / phase, allow_empty=True)
        for phase in ('forward-guards', 'forward-stop', 'forward-database', 'forward-wire', 'forward-start', 'forward-build-policy')
        if (state / phase).exists() or (state / phase).is_symlink()}
    receipts['forward-gateway'] = gateway[1]
    receipts['forward-activate'] = opening.files
    if (gateway[0] != frame or _frame(bindings, state) != frame
            or shared_forward_gateway_view(bindings=bindings, state_dir=state) != gateway
            or shared_forward_activation_view(bindings=bindings, state_dir=state) != opening):
        raise ValueError
    return _ProcessScope(frame, opening.pool_states, opening.guard_states is not None,
        workloads, gateway_options, guard_options, staging_acl, receipts)


def _gateway_history_target(bindings: SharedPoolGuardBindings, credential: dict[str, Any]) -> PoolManagementHistoryTarget:
    """The new manager's completed database/runtime, never legacy upgrade history.

    The surrounding frame brackets every private input. The current Secret's
    version is observed, not invented from a historical snapshot that omitted it.
    """
    completed = bindings.plan.development.runtime
    manager = completed.request.database.manager.retained.request.retained
    registration = bindings.migration.registration.binding
    binding, state = manager.binding, Path(manager.operation['state_dir'])
    material = _json(manager.files[state / 'bootstrap/material/material.json'])
    if ((binding.namespace, binding.namespace_uid) != (registration.namespace, registration.namespace_uid)
            or material['binding'] != asdict(binding) or material['status'] != 'delivered'):
        raise ValueError
    expected = management_material(material['material'], binding, material['operation_id'])['loom-platform-db']
    uid = material['resources']['loom-platform-db']['uid']
    version = credential['metadata'].get('resourceVersion')
    if (not _matches(credential, expected, uid) or not isinstance(version, str) or not 0 < len(version) <= 128):
        raise ValueError
    journal = _json(manager.files[state / 'database/stage.json'])
    keys = {kind + ':' + binding.namespace + ':loom-postgres' for kind in ('StatefulSet', 'Service')}
    if (journal['schema'] != 'loom.nebius-management-stage.v1' or journal['binding'] != asdict(binding)
            or journal['phase'] != '20-database.yaml' or set(journal['resources']) != keys):
        raise ValueError
    resources = {}
    for key, row in journal['resources'].items():
        document = copy.deepcopy(row['observed'])
        if row['status'] != 'created' or key != document['kind'] + ':' + binding.namespace + ':loom-postgres':
            raise ValueError
        document['metadata']['uid'] = row['uid']
        _uid(document)
        resources[document['kind']] = document
    return PoolManagementHistoryTarget(namespace=binding.namespace, namespace_uid=UUID(binding.namespace_uid),
        manager_revision=bindings.plan.development.selection.manager_revision,
        controller=bindings.plan.effective_runtime_resources['Deployment:' + binding.namespace + ':loom-service'],
        database=PoolGuardDatabase(statefulset=resources['StatefulSet'], service=resources['Service'],
            credential_uid=UUID(uid), credential_resource_version=version))


def observe_shared_pool_forward_processes(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        development: HTTPSSharedPoolRuntimeProbes, kubeconfig: Path, executable: Path,
        recheck_application_scopes: Callable[[], None] | None = None) -> SharedPoolForwardProcessObservation:
    """Challenge current sources without replaying original open-guard preflight.

    The child readiness engine must finish every pending workload before the
    parent can record stop/start completion. Other phases require all observed
    staging resources ready or drained. No mutation or intake release is issued.
    """
    try:
        before_scope = _scope(bindings, state_dir)
        plan, migration = bindings.plan, bindings.migration
        completed = plan.development.runtime
        runtime = completed.request

        def dev_scope() -> None:
            if (development.shared_bindings != bindings or development.request != runtime or development.plan != completed.plan
                    or development.api_server.rstrip('/') != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
                raise ValueError

        dev_scope()
        guards = KubectlPoolGuardAPI(request=migration, kubeconfig=kubeconfig, executable=executable)

        def guard_states() -> dict[str, str]:
            result = {target.namespace: (guards.activation_guard(target, 'observe') if before_scope.activation
                else guards.guard(target, 'observe')['status']) for target in migration.guards}
            if any(value not in before_scope.guards[namespace] for namespace, value in result.items()):
                raise ValueError
            return result

        states = guard_states()
        databases = {target.namespace: development._process_identity(guards._database(target)) for target in migration.guards}
        legacy = plan.staging.legacy
        old_namespace = legacy.context.original.upgrade.setup.binding.namespace
        old_credential = guards._get('secret', 'loom-platform-db', old_namespace)
        history_target = derive_management_history_target(original=legacy.context.original,
            predecessor=legacy.context.predecessor, credential=old_credential)
        history = KubectlPoolHistoryAPI(request=legacy.context.request.fencing.retirement.migration,
            target=history_target, kubeconfig=kubeconfig, executable=executable)
        manager_namespace = migration.registration.binding.namespace
        manager_credential = guards._get('secret', 'loom-platform-db', manager_namespace)
        gateway_target = _gateway_history_target(bindings, manager_credential)
        gateway_history = KubectlPoolHistoryAPI(request=migration, target=gateway_target,
            kubeconfig=kubeconfig, executable=executable)
        manager_database = development._process_identity(gateway_history._database(gateway_target, url_variable='LOOM_SVC_DB_URL'))

        def ledgers() -> str:
            if history.machine_retirement('observe', manager_revision=retired_manager_revision(legacy)) != 'revoked':
                raise ValueError
            mode = gateway_history.activation_pool('observe')
            if mode not in before_scope.pool_states:
                raise ValueError
            if mode == 'global':
                gateway_history.qualify_active_pool()
            else:
                development.qualify_closed_pool()
            for target in migration.guards:
                if target.namespace == 'loom-dev' or before_scope.staging_acl:
                    if guards.runtime_role(target, 'inspect') != {'status': 'qualified'}:
                        raise ValueError
            return mode

        pool_state = ledgers()
        participant = plan.staging.attachment.participant
        dev_participant, = (row for row in migration.registration.spec.participants if row.participant_id != participant.participant_id)
        dev_processes = {}
        with development.process_observation(state_dir=completed.state_dir):
            for namespace, name in ((migration.registration.binding.namespace, 'loom-service'),
                    ('loom-dev', 'loom-service'), ('loom-dev', 'loom-control-plane'),
                    (dev_participant.execution_namespace.name, 'loom-execution-actuator')):
                key = 'Deployment:' + namespace + ':' + name
                dev_processes[key] = development._process_identity(development._running(key, completed.state_dir)[2])
                development.qualify_runtime_settings(key=key, state_dir=completed.state_dir)
                development.qualify_runtime_database(key=key, state_dir=completed.state_dir)
        target, = (row for row in migration.guards if row.participant_id == participant.participant_id)
        database = target.database
        if (database is None or database.actuator_credential_uid is None
                or database.actuator_credential_resource_version is None):
            raise ValueError
        credentials = {}
        for key, original in plan.staging.originals.items():
            if original['kind'] != 'Deployment':
                continue
            actuator = original['metadata']['namespace'] == participant.execution_namespace.name
            variable = 'LOOM_EXECUTION_ACTUATOR_DB_URL' if actuator else (
                'LOOM_CP_DB_URL' if original['metadata']['name'] == 'loom-control-plane' else 'LOOM_SVC_DB_URL')
            credentials[key] = (original, variable,
                database.actuator_credential_uid if actuator else database.credential_uid,
                database.actuator_credential_resource_version if actuator else database.credential_resource_version)

        def urls() -> dict[str, str]:
            return {key: guards._workload_database_url(original, url_variable=variable,
                credential_uid=uid, credential_resource_version=version)
                for key, (original, variable, uid, version) in credentials.items()}

        before_urls = urls()
        pending, observed = [], {}
        process_identities, readiness = {}, {}

        def read_staging(original: dict[str, Any]) -> dict[str, Any]:
            # Development's observer intentionally excludes staging. Use the
            # existing participant-bound transport for these fixed originals.
            return guards._get('deployment' if original['kind'] == 'Deployment' else 'cronjob',
                original['metadata']['name'], original['metadata']['namespace'])

        def workload_ready(original: dict[str, Any], current: dict[str, Any]) -> bool:
            deployment = current['kind'] == 'Deployment'
            if not deployment and current['spec'].get('suspend') is not True:
                return True  # The original collector has not been suspended yet.
            namespace = current['metadata']['namespace']
            children = guards._run(['get', '--raw', ('/apis/apps/v1/namespaces/' + namespace + '/replicasets' if deployment
                else '/apis/batch/v1/namespaces/' + namespace + '/jobs') + '?limit=1000'])
            pods = guards._run(['get', '--raw', '/api/v1/namespaces/' + namespace + '/pods?limit=1000'])
            if deployment and current['spec']['replicas'] == 1:
                return qualify_started_deployment(current=current, children=children, pods=pods,
                    region=runtime.database.foundation.inputs.config['region'])
            return qualify_closed_workload_drain(original=original, desired=current, current=current, children=children, pods=pods)

        for key, choices in before_scope.workloads.items():
            original = plan.staging.originals[key]
            current = read_staging(original)
            if not any(_matches(current, option, _uid(original)) for option in choices):
                raise ValueError
            _snapshot(current)
            deployment = current['kind'] == 'Deployment'
            namespace = current['metadata']['namespace']
            running = deployment and current['spec']['replicas'] == 1
            readiness[key] = ready = workload_ready(original, current)
            if not ready:
                stopping = before_scope.frame['forward']['forward-stop']['status'] == 'started'
                starting = running and before_scope.frame['forward']['forward-start']['status'] == 'started'
                if not (stopping or starting):
                    raise ValueError
                pending.append(key)
            elif running:
                _, _, uid, version = credentials[key]
                process_identities[key] = development._process_identity(guards._runtime(target, original=original, expected=current))
                guards.qualify_runtime_database(target, original=original, expected=current,
                    credential_uid=uid, credential_resource_version=version)
                if before_scope.frame['forward']['forward-wire']['status'] == 'complete':
                    guards.qualify_runtime_pool_settings(target, original=original, expected=current)
                else:
                    guards.qualify_runtime_legacy_settings(target, original=original, expected=current)
                if namespace == participant.execution_namespace.name:
                    guards.qualify_runtime_telemetry(target, original=original, expected=current)
            final = read_staging(original)
            if (not _matches(final, current, _uid(original))
                    or final['metadata'].get('generation') != current['metadata'].get('generation')):
                raise ValueError
            observed[key] = copy.deepcopy(current)
        gateway_original = plan.development.gateway_original
        gateway_key = 'Deployment:' + manager_namespace + ':loom-pool-gateway'

        def read_gateway() -> dict[str, Any]:
            value = guards._get('deployment', 'loom-pool-gateway', manager_namespace)
            if not any(_matches(value, expected, _uid(gateway_original)) for expected in before_scope.gateway):
                raise ValueError
            return value

        gateway = read_gateway()
        gateway_ready = workload_ready(gateway_original, gateway)
        gateway_process = None
        if not gateway_ready:
            if (gateway['spec']['replicas'] != 1 or before_scope.frame['forward']['forward-gateway']['status'] != 'started'):
                raise ValueError
            pending.append(gateway_key)
        elif gateway['spec']['replicas'] == 1:
            gateway_process = development._process_identity(gateway_history._runtime(
                gateway_target, original=gateway_original, expected=gateway))
            gateway_history.qualify_gateway_runtime(original=gateway_original, expected=gateway, active=pool_state == 'global')
        if (ledgers() != pool_state or guard_states() != states or urls() != before_urls
                or guards._get('secret', 'loom-platform-db', old_namespace) != old_credential):
            raise ValueError
        dev_scope()
        # Finish the full inventory barrier before the final staging, gateway,
        # guard and database identities. No scope-exit remote read may follow
        # those final checks and leave a newly changed backend unchallenged.
        with development.process_observation(state_dir=completed.state_dir):
            if recheck_application_scopes is not None:
                recheck_application_scopes()
            for key, identity in dev_processes.items():
                if development._process_identity(development._running(key, completed.state_dir)[2]) != identity:
                    raise ValueError
        for key, current in observed.items():
            original = plan.staging.originals[key]
            final = read_staging(original)
            if (not _matches(final, current, _uid(original))
                    or final['metadata'].get('generation') != current['metadata'].get('generation')
                    or (readiness[key] and not workload_ready(original, final))):
                raise ValueError
            if key in process_identities:
                if development._process_identity(guards._runtime(target, original=original, expected=final)) != process_identities[key]:
                    raise ValueError
        final_gateway = read_gateway()
        if (not _matches(final_gateway, gateway, _uid(gateway_original))
                or final_gateway['metadata'].get('generation') != gateway['metadata'].get('generation')
                or (gateway_ready and not workload_ready(gateway_original, final_gateway))
                or (gateway_process is not None and development._process_identity(gateway_history._runtime(
                    gateway_target, original=gateway_original, expected=final_gateway)) != gateway_process)):
            raise ValueError
        if gateway_history.activation_pool('observe') != pool_state or guard_states() != states:
            raise ValueError
        # Bracket final consumer, gateway and pool/guard reads, not just their
        # first challenges: even the last state read can overlap a DB restart.
        if any(development._process_identity(guards._database(target)) != databases[target.namespace] for target in migration.guards):
            raise ValueError
        if (guards._get('secret', 'loom-platform-db', manager_namespace) != manager_credential
                or development._process_identity(gateway_history._database(gateway_target, url_variable='LOOM_SVC_DB_URL')) != manager_database):
            raise ValueError
        if _scope(bindings, state_dir) != before_scope:
            raise ValueError
        telemetry = guards.telemetry_report()
        pending_keys = tuple(sorted(pending))
        checksum = digest({'handoff': bindings.input_digest, 'frame': before_scope.frame,
            'receipts': before_scope.receipts, 'pool_state': pool_state, 'guards': states, 'workloads': observed,
            'gateway': gateway, 'gateway_process': gateway_process,
            'retired_authority': 'revoked', 'pending_workloads': pending_keys, 'telemetry': telemetry})
        return SharedPoolForwardProcessObservation(bindings, pool_state, states, 'revoked', pending_keys, telemetry, checksum)
    except Exception:
        raise ValueError('shared pool forward process observation unqualified') from None
