"""Current SQL authority and drain through terminal shared-pool recovery.

Recovery must not require a healthy cancelled application or gateway. Only
recorded local intents permit fenced/revoked/reopened alternatives. This reader
does not claim process, writer, provider or restored-staging health, and cannot
authorize any mutation without the protected parent's other current barriers.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import (
    KubectlPoolHistoryAPI,
    derive_management_history_target,
)
from scripts.ops.nebius_shared_pool_database import validate_shared_staging_database
from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_recovery_census import _receipt_files, _recovery_options
from scripts.ops.nebius_shared_pool_recovery_fence import _entry_choices, _recovery_frame

from loom.nebius_platform_render import digest


@dataclass(frozen=True, repr=False)
class SharedPoolRecoveryDatabaseObservation:
    bindings: SharedPoolGuardBindings
    pool_state: str
    guard_states: dict[str, str]
    machine_authority: str
    retired_authority: str
    pool_drained: bool | None
    participant_drained: dict[str, bool | None]
    input_digest: str


@dataclass(frozen=True, repr=False)
class _DatabaseScope:
    frame: dict[str, Any]
    receipts: dict[str, dict[str, str]]
    pool_states: tuple[str, ...]
    guard_states: dict[str, tuple[str, ...]]
    machine_states: tuple[str, ...]
    drain_required: bool
    staging_acl: bool


def _scope(bindings: SharedPoolGuardBindings, state: Path) -> _DatabaseScope:
    files = _receipt_files(state)
    frame, _, _, receipts = _recovery_options(bindings, state)
    if receipts != files:
        raise ValueError
    pool, participants = _entry_choices(bindings, state)

    def child(phase: str, name: str) -> dict[str, Any] | None:
        path = state / phase / name
        return _read(path) if path.exists() or path.is_symlink() else None

    fenced = child('recovery-fence', 'fence.json')
    if fenced is not None:
        def options(status: str, original: tuple[str, ...]) -> tuple[str, ...]:
            return ('fenced',) if status == 'fenced' else (*original, 'fenced') if status == 'intent' else original
        pool = options(fenced['pool'], pool)
        participants = {key: options(fenced['guards'][key], choices) for key, choices in participants.items()}
    guards = {target.namespace: participants[str(target.participant_id)] for target in bindings.migration.guards}
    reopened = child('recovery-reopen', 'reopen.json')
    if reopened is not None:
        staging = bindings.plan.staging.attachment.guard
        if staging not in bindings.migration.guards:
            raise ValueError
        guards[staging.namespace] = {'prepared': ('fenced',), 'intent': ('fenced', 'open'), 'released': ('open',)}[reopened['status']]
    machines = child('recovery-revoke', 'machines.json')
    machine_states = {'prepared': ('active',), 'intent': ('active', 'revoked'), 'revoked': ('revoked',)}[
        machines['status'] if machines is not None else 'prepared']
    drained = child('recovery-drain', 'drain.json')
    staging_acl = False
    path = state / 'forward-database/database.json'
    status = frame['forward']['forward-database']['status']
    if status != 'prepared' or path.exists() or path.is_symlink():
        record = validate_shared_staging_database(bindings=bindings, state_dir=path.parent,
            complete=status == 'complete', prepared=status == 'prepared')
        staging_acl = record['status'] == 'staged'
    if _recovery_frame(bindings, state) != frame or _receipt_files(state) != files:
        raise ValueError
    return _DatabaseScope(frame, files, pool, guards, machine_states,
        drained is not None and drained['status'] == 'observed', staging_acl)


def observe_shared_pool_recovery_databases(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        kubeconfig: Path, executable: Path) -> SharedPoolRecoveryDatabaseObservation:
    """Read current retained backends even while successor applications are down.

    A not-yet-fenced ledger has no recovery-drain claim. Busy fenced ledgers may
    wait before drain completion; afterwards cancelled dev/global stay drained.
    Only staging actually opened under its reopening intent may admit new work.
    No SQL state is repaired and no unknown write is retried by this observer.
    """
    try:
        retained = copy.deepcopy(bindings)
        scope = _scope(retained, state_dir)
        migration, plan = retained.migration, retained.plan
        guards = KubectlPoolGuardAPI(request=migration, kubeconfig=kubeconfig, executable=executable)
        legacy = plan.staging.legacy
        old_namespace = legacy.context.original.upgrade.setup.binding.namespace
        manager_namespace = migration.registration.binding.namespace
        old_credential = guards._get('secret', 'loom-platform-db', old_namespace)
        new_credential = guards._get('secret', 'loom-platform-db', manager_namespace)
        old_target = derive_management_history_target(original=legacy.context.original,
            predecessor=legacy.context.predecessor, credential=old_credential)
        old = KubectlPoolHistoryAPI(request=legacy.context.request.fencing.retirement.migration,
            target=old_target, kubeconfig=kubeconfig, executable=executable)
        new_target = _gateway_history_target(retained, new_credential)
        new = KubectlPoolHistoryAPI(request=migration, target=new_target, kubeconfig=kubeconfig, executable=executable)

        def databases() -> dict[str, tuple[Any, ...]]:
            result = {target.namespace: HTTPSDevelopmentRuntimeProbes._process_identity(guards._database(target))
                for target in migration.guards}
            for api in (old, new):
                if api.target.namespace in result:
                    raise ValueError
                result[api.target.namespace] = HTTPSDevelopmentRuntimeProbes._process_identity(
                    api._database(api.target, url_variable='LOOM_SVC_DB_URL'))
            return result

        before_databases = databases()

        def states() -> tuple[str, dict[str, str], str]:
            if old.machine_retirement('observe') != 'revoked':
                raise ValueError
            pool = new.activation_pool('observe')
            machines = new.recovery_machine_state()
            local = {target.namespace: guards.activation_guard(target, 'observe') for target in migration.guards}
            if (pool not in scope.pool_states or machines not in scope.machine_states
                    or any(value not in scope.guard_states[namespace] for namespace, value in local.items())):
                raise ValueError
            return pool, local, machines

        pool, local, machines = states()
        for target in migration.guards:
            if target.namespace == 'loom-dev' or scope.staging_acl:
                if guards.runtime_role(target, 'inspect') != {'status': 'qualified'}:
                    raise ValueError
        pool_drained = new.recovery_pool_drained() if pool == 'fenced' else None
        participant_drained = {target.namespace: (guards.recovery_participant_drained(target)
            if local[target.namespace] == 'fenced' else None) for target in migration.guards}
        if scope.drain_required and (pool_drained is not True
                or any(value is not True for namespace, value in participant_drained.items() if local[namespace] == 'fenced')):
            raise ValueError
        # Finish ledger/guard reads before checking all backend process identities
        # again, so a restart during even the final SQL read is unconfirmed.
        if states() != (pool, local, machines):
            raise ValueError
        if (guards._get('secret', 'loom-platform-db', old_namespace) != old_credential
                or guards._get('secret', 'loom-platform-db', manager_namespace) != new_credential
                or databases() != before_databases):
            raise ValueError
        if (bindings != retained or _recovery_frame(retained, state_dir) != scope.frame
                or _receipt_files(state_dir) != scope.receipts):
            raise ValueError
        checksum = digest({'handoff': retained.input_digest, 'frame': scope.frame, 'receipts': scope.receipts,
            'pool_state': pool, 'guard_states': local, 'machine_authority': machines, 'retired_authority': 'revoked',
            'pool_drained': pool_drained, 'participant_drained': participant_drained, 'databases': before_databases,
            'credentials': {row['metadata']['namespace']: {'uid': _uid(row), 'version': row['metadata']['resourceVersion']}
                for row in (old_credential, new_credential)}})
        return SharedPoolRecoveryDatabaseObservation(retained, pool, local, machines, 'revoked',
            pool_drained, participant_drained, checksum)
    except Exception:
        raise ValueError('shared pool recovery database observation unqualified') from None
