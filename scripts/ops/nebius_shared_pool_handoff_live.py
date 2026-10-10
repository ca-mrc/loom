"""Current identity bindings for the protected shared-pool forward handoff.

These GET-only observations do not acquire guards or attest sole-writer ownership.
The connected parent must separately qualify SQL/process/provider state, persist
its phase intent, and repeat current authority checks before any mutation.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from scripts.ops.nebius_development_bootstrap import _secret_documents
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_migration import (
    PoolGuardDatabase,
    PoolMigrationRequest,
    migration_contract,
)
from scripts.ops.nebius_pool_platform_authority import platform_controller_subjects
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover
from scripts.ops.nebius_pool_role_fencing import (
    POOL_WRITER_WORKLOAD_COLLECTIONS,
    qualify_pool_writer_workloads,
)
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


@dataclass(frozen=True, repr=False)
class SharedPoolWorkloadObservation:
    plan: SharedPoolHandoffPlan
    resource_version: str
    workloads: dict[str, dict[str, Any]]
    input_digest: str


def _workload_roots(plan: SharedPoolHandoffPlan) -> dict[str, dict[str, Any]]:
    """Current fixed consumers, including the retired old manager/gateway.

    Current staging replaces historical staging templates; completed dev runtime
    replaces its foundation templates. Neither source is caller-supplied authority.
    """
    result: dict[str, dict[str, Any]] = {}

    def add(row: dict[str, Any]) -> None:
        if row['kind'] in {'Deployment', 'StatefulSet', 'CronJob'}:
            _uid(row)
            result[_key(row)] = copy.deepcopy(row)

    for row in (*plan.staging.legacy.context.request.platform_consumers,
            *plan.staging.legacy.completion.workloads.values(), *plan.staging.attachment.resources):
        add(row)
    database = plan.development.runtime.request.database
    for phase, journal in database.foundation.phases.items():
        if phase != 'storage':
            for item in journal['resources'].values():
                row = copy.deepcopy(item['observed'])
                row['metadata']['uid'] = item['uid']
                add(row)
    manager = database.manager.retained.request.retained
    state = Path(manager.operation['state_dir'])
    for path, raw in manager.files.items():
        if path.name == 'stage.json' and path.parent.parent == state:
            for item in json.loads(raw).get('resources', {}).values():
                row = copy.deepcopy(item['observed'])
                row['metadata']['uid'] = item['uid']
                add(row)
    for row in plan.development.runtime.resources.values():
        add(row)
    if len({_uid(row) for row in result.values()}) != len(result):
        raise ValueError
    return result


def observe_shared_pool_workloads(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path,
        transport: ManagementKubernetesTransport) -> SharedPoolWorkloadObservation:
    """GET-only, exact-revision built-in workload census before the first guard.

    RBAC, external credentials, provider ownership and process drain are separate
    mandatory checks. This initial observation cannot authorize a later mutation
    or be replayed after the parent intentionally transitions workloads.
    """
    try:
        plan = prepare_shared_pool_handoff(runtime=runtime, observation=observation,
            legacy=legacy, repo_root=repo_root)
        if transport.api_server.rstrip('/') != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/'):
            raise ValueError
        roots = _workload_roots(plan)
        binding = plan.migration.registration.binding

        def cluster() -> None:
            row = transport._request('GET', '/api/v1/namespaces/kube-system')
            if (row is None or row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace'
                    or row['metadata'].get('name') != 'kube-system' or _uid(row) != binding.kube_system_uid):
                raise ValueError

        cluster()
        revision: str | None = None

        def read(method: str, path: str) -> dict[str, Any] | None:
            nonlocal revision
            if revision is not None and 'continue=' not in path:
                path += '&' + urlencode({'resourceVersion': revision, 'resourceVersionMatch': 'Exact'})
            page = transport._request(method, path)
            current = None if page is None else page.get('metadata', {}).get('resourceVersion')
            if not isinstance(current, str) or not 0 < len(current) <= 1024 or revision not in (None, current):
                raise ValueError
            revision = current
            return page

        inventory = {resource: inventory_resources(read, api, resource, kind, include_terminal_pods=True)
            for api, resource, kind in POOL_WRITER_WORKLOAD_COLLECTIONS}
        subjects = set()
        for row in roots.values():
            pod = (row['spec']['jobTemplate']['spec']['template']['spec'] if row['kind'] == 'CronJob'
                else row['spec']['template']['spec'])
            subjects.add((row['metadata']['namespace'], pod.get('serviceAccountName', 'default')))
        controllers = frozenset(_uid(guard.controller) for guard in plan.migration.guards)
        namespaces = frozenset(ns.name for participant in plan.migration.registration.spec.participants
            for ns in (participant.execution_namespace, participant.build_namespace))
        # Every qualified root in worker namespaces is a producer, including
        # guest actuators whose names differ from the ordinary actuator.
        producers = controllers | frozenset(_uid(row) for row in roots.values()
            if row['metadata']['namespace'] in namespaces)
        qualify_pool_writer_workloads(inventory, originals=roots, expected=roots,
            subjects=frozenset(subjects), writer_namespaces=namespaces,
            controller_uids=controllers, producer_uids=producers,
            platform_subjects=platform_controller_subjects(legacy.context.request.platform_authority))
        current = {_key(row): row for rows in inventory.values() for row in rows if _key(row) in roots}
        cluster()
        if (revision is None or prepare_shared_pool_handoff(runtime=runtime, observation=observation,
                legacy=legacy, repo_root=repo_root) != plan):
            raise ValueError
        checksum = digest({'handoff': plan.input_digest, 'resource_version': revision, 'workloads': current})
        return SharedPoolWorkloadObservation(plan, revision, copy.deepcopy(current), checksum)
    except Exception:
        raise ValueError('shared pool workload census unqualified') from None
