"""Fresh database and process observations during the pre-gateway handoff.

Only known in-flight stop/start readiness can be pending. A completed transition
is not permission to skip present drain, settings or backend correspondence.
This adapter is read-only; writers, provider capacity and queue drain remain
separate mandatory observations owned by the connected parent.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import (
    KubectlPoolHistoryAPI,
    derive_management_history_target,
)
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_shared_pool_database import validate_shared_staging_database
from scripts.ops.nebius_shared_pool_guards import validate_shared_pool_guards
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import _files
from scripts.ops.nebius_shared_pool_workloads import _frame, shared_forward_staging_options

from loom.nebius_platform_render import digest


@dataclass(frozen=True, repr=False)
class SharedPoolForwardProcessObservation:
    bindings: SharedPoolGuardBindings
    guard_states: dict[str, str]
    retired_authority: str
    pending_workloads: tuple[str, ...]
    telemetry: dict[str, Any]
    input_digest: str


@dataclass(frozen=True, repr=False)
class _ProcessScope:
    frame: dict[str, Any]
    workloads: dict[str, tuple[dict[str, Any], ...]]
    guards: dict[str, tuple[str, ...]]
    staging_acl: bool
    receipts: dict[str, dict[str, str]]


def _scope(bindings: SharedPoolGuardBindings, state: Path) -> _ProcessScope:
    frame = _frame(bindings, state)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    if (frame['outcome'] != 'pending' or recovery.exists() or recovery.is_symlink()
            or any(frame['forward'][phase]['status'] != 'prepared'
                for phase in ('forward-gateway', 'forward-activate', 'forward-verify'))):
        raise ValueError
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
    staging_acl = False
    status = frame['forward']['forward-database']['status']
    path = state / 'forward-database/database.json'
    if status != 'prepared' or path.exists() or path.is_symlink():
        record = validate_shared_staging_database(bindings=bindings, state_dir=path.parent,
            complete=status == 'complete', prepared=status == 'prepared')
        staging_acl = record['status'] == 'staged'
    workloads = shared_forward_staging_options(bindings=bindings, state_dir=state)
    receipts = {phase: _files(state / phase, allow_empty=True)
        for phase in ('forward-guards', 'forward-stop', 'forward-database', 'forward-wire', 'forward-start', 'forward-build-policy')
        if (state / phase).exists() or (state / phase).is_symlink()}
    if _frame(bindings, state) != frame:
        raise ValueError
    return _ProcessScope(frame, workloads, guard_options, staging_acl, receipts)


def observe_shared_pool_forward_processes(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        development: HTTPSDevelopmentRuntimeProbes, kubeconfig: Path, executable: Path) -> SharedPoolForwardProcessObservation:
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
            if (development.request != runtime or development.plan != completed.plan
                    or development.api_server.rstrip('/') != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
                raise ValueError
            development.inspect(state_dir=completed.state_dir)

        dev_scope()
        guards = KubectlPoolGuardAPI(request=migration, kubeconfig=kubeconfig, executable=executable)

        def guard_states() -> dict[str, str]:
            result = {target.namespace: guards.guard(target, 'observe')['status'] for target in migration.guards}
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

        def ledgers() -> None:
            if history.machine_retirement('observe') != 'revoked':
                raise ValueError
            development.qualify_closed_pool()
            for target in migration.guards:
                if target.namespace == 'loom-dev' or before_scope.staging_acl:
                    if guards.runtime_role(target, 'inspect') != {'status': 'qualified'}:
                        raise ValueError

        ledgers()
        participant = plan.staging.attachment.participant
        dev_participant, = (row for row in migration.registration.spec.participants if row.participant_id != participant.participant_id)
        dev_processes = {}
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
        ledgers()
        if (guard_states() != states or urls() != before_urls
                or guards._get('secret', 'loom-platform-db', old_namespace) != old_credential
                or any(development._process_identity(guards._database(target)) != databases[target.namespace] for target in migration.guards)):
            raise ValueError
        dev_scope()
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
        if _scope(bindings, state_dir) != before_scope:
            raise ValueError
        telemetry = guards.telemetry_report()
        pending_keys = tuple(sorted(pending))
        checksum = digest({'handoff': bindings.input_digest, 'frame': before_scope.frame,
            'receipts': before_scope.receipts, 'guards': states, 'workloads': observed,
            'retired_authority': 'revoked', 'pending_workloads': pending_keys, 'telemetry': telemetry})
        return SharedPoolForwardProcessObservation(bindings, states, 'revoked', pending_keys, telemetry, checksum)
    except Exception:
        raise ValueError('shared pool forward process observation unqualified') from None
