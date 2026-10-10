"""Current identity bindings for the protected shared-pool forward handoff.

These GET-only observations do not acquire guards or attest sole-writer ownership.
The connected parent must separately qualify SQL/process/provider state, persist
its phase intent, and repeat current authority checks before any mutation.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_development_bootstrap import _secret_documents
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_migration import (
    PoolGuardDatabase,
    PoolMigrationRequest,
    migration_contract,
)
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover
from scripts.ops.nebius_shared_pool_handoff import (
    SharedPoolHandoffPlan,
    prepare_shared_pool_handoff,
)

from loom.nebius_platform_render import digest


@dataclass(frozen=True, repr=False)
class SharedPoolGuardBindings:
    plan: SharedPoolHandoffPlan
    migration: PoolMigrationRequest
    input_digest: str


def observe_shared_pool_guard_bindings(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path,
        development: HTTPSDevelopmentRuntimeObserver) -> SharedPoolGuardBindings:
    """Bind dev's actual database and credentials without an unbound fallback.

    Staging keeps the independently captured binding qualified by its attachment.
    Its live database must still be requalified by the protected parent. Returned
    resource versions are observation pins, never durable permission to operate.
    """
    try:
        plan = prepare_shared_pool_handoff(runtime=runtime, observation=observation,
            legacy=legacy, repo_root=repo_root)
        completed = plan.development.runtime
        foundation = runtime.database.foundation

        def qualify_observer() -> None:
            if (development.request != runtime or development.plan != completed.plan
                    or development.api_server.rstrip('/') != foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
                raise ValueError

        qualify_observer()
        expected: dict[str, dict[str, Any]] = {}
        for kind in ('StatefulSet', 'Service'):
            item, = (row for row in foundation.phases['database']['resources'].values()
                if row['observed']['kind'] == kind and row['observed']['metadata']['name'] == 'loom-postgres')
            document = copy.deepcopy(item['observed'])
            document['metadata']['uid'] = item['uid']
            expected[kind] = document
        credential = _secret_documents(foundation.bootstrap['material'], foundation.inputs.binding,
            foundation.binding.operation_id)['loom-platform-db']
        credential['metadata']['uid'] = foundation.bootstrap['secrets']['loom-platform-db']['uid']
        expected['credential'] = credential
        expected['actuator'] = completed.resources[_key(runtime.database.material[1])]

        def read_bindings() -> dict[str, dict[str, Any]]:
            result = {}
            for name, wanted in expected.items():
                actual = development._get(wanted)
                if not _matches(actual, wanted, _uid(wanted)):
                    raise ValueError
                if name in {'credential', 'actuator'}:
                    version = actual['metadata'].get('resourceVersion')
                    if not isinstance(version, str) or not 0 < len(version) <= 128:
                        raise ValueError
                result[name] = actual
            return result

        # Capture credentials before the full observation: unchanged values
        # with a changed resourceVersion during observation must also fail.
        before = read_bindings()
        workloads = development.inspect(state_dir=completed.state_dir)
        current = read_bindings()
        for name, original in before.items():
            if (_snapshot(current[name]) != _snapshot(original) or _uid(current[name]) != _uid(original)
                    or (name in {'credential', 'actuator'}
                        and current[name]['metadata']['resourceVersion'] != original['metadata']['resourceVersion'])):
                raise ValueError
        if prepare_shared_pool_handoff(runtime=runtime, observation=observation,
                legacy=legacy, repo_root=repo_root) != plan:
            raise ValueError
        qualify_observer()
        dev_guard, = (row for row in plan.migration.guards if row.namespace == 'loom-dev')
        controller = workloads[_key(dev_guard.controller)]
        if not _matches(controller, dev_guard.controller, _uid(dev_guard.controller)):
            raise ValueError
        binding = PoolGuardDatabase(statefulset=copy.deepcopy(before['StatefulSet']), service=copy.deepcopy(before['Service']),
            credential_uid=UUID(_uid(before['credential'])),
            credential_resource_version=before['credential']['metadata']['resourceVersion'],
            actuator_credential_uid=UUID(_uid(before['actuator'])),
            actuator_credential_resource_version=before['actuator']['metadata']['resourceVersion'])
        migration = replace(plan.migration, guards=tuple(
            replace(row, controller=copy.deepcopy(controller), database=binding) if row == dev_guard else row
            for row in plan.migration.guards))
        if any(row.database is None for row in migration.guards):
            raise ValueError
        checksum = digest({'handoff': plan.input_digest, 'migration': migration_contract(migration)})
        return SharedPoolGuardBindings(plan, migration, checksum)
    except Exception:
        raise ValueError('shared pool guard bindings unqualified') from None
