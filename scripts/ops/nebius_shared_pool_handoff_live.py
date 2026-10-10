"""Current identity bindings for the protected shared-pool forward handoff.

These read-only observations do not acquire guards or attest sole-writer ownership.
The connected parent must combine the identity, census, SQL/process and provider
evidence, persist its phase intent, and repeat authority checks before mutation.
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
from scripts.ops.nebius_development_runtime_external import qualify_runtime_cloud
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest
from scripts.ops.nebius_development_runtime_observation import HTTPSDevelopmentRuntimeObserver
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_gateway_retirement import _gateway_record
from scripts.ops.nebius_pool_migration import (
    PoolGuardDatabase,
    PoolMigrationRequest,
    migration_contract,
)
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import (
    KubectlPoolHistoryAPI,
    derive_management_history_target,
)
from scripts.ops.nebius_pool_platform_authority import platform_controller_subjects
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover
from scripts.ops.nebius_pool_role_fencing import (
    POOL_WRITER_WORKLOAD_COLLECTIONS,
    qualify_pool_writer_bindings,
    qualify_pool_writer_workloads,
)
from scripts.ops.nebius_shared_pool_handoff import (
    SharedPoolHandoffPlan,
    prepare_shared_pool_handoff,
)

from loom.nebius_platform_render import digest
from loom_control_plane.execution_placement import quota_identity
from loom_execution_capacity_collector.contracts import ProviderCapacitySnapshot


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
class SharedPoolWriterObservation:
    plan: SharedPoolHandoffPlan
    resource_version: str
    workloads: dict[str, dict[str, Any]]
    authority: dict[str, dict[str, Any]]
    input_digest: str


def _resource_roots(plan: SharedPoolHandoffPlan, kinds: frozenset[str]) -> dict[str, dict[str, Any]]:
    """Current fixed consumers, including the retired old manager/gateway.

    Current staging replaces historical staging templates; completed dev runtime
    replaces its foundation templates. Neither source is caller-supplied authority.
    """
    result: dict[str, dict[str, Any]] = {}

    def add(row: dict[str, Any]) -> None:
        if row['kind'] in kinds:
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


def _authority_roots(plan: SharedPoolHandoffPlan) -> dict[str, dict[str, Any]]:
    kinds = frozenset({'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'})
    legacy = plan.staging.legacy
    result = {_key(row): copy.deepcopy(row) for row in (*legacy.context.original.retained.values(),
        *legacy.context.predecessor.retained.values()) if row['kind'] in kinds}
    result.update(_resource_roots(plan, kinds))
    # Read the completed old gateway's reduced authority, never its pre-recovery
    # grants or a name-based exemption. The completed legacy chain was qualified
    # before this read and is checked again before returning the observation.
    originals, targets, _, record = _gateway_record(legacy.context.request,
        state=Path(legacy.context.operation['state_dir']), anchor=Path(legacy.context.operation['anchor_dir']))
    if record is None or any(item['phase'] != 'restricted' for item in record['roles'].values()):
        raise ValueError
    for key, original in originals.items():
        row = copy.deepcopy(targets.get(key, original))
        row['metadata']['uid'] = _uid(original)
        if key in result and result[key] != row:
            raise ValueError
        result[key] = row
    return result


def observe_shared_pool_writers(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path,
        transport: ManagementKubernetesTransport) -> SharedPoolWriterObservation:
    """GET-only RBAC and built-in workload census before the first guard.

    External credentials, provider ownership and process drain are separate
    mandatory checks. This initial observation cannot authorize a later mutation
    or be replayed after the parent intentionally transitions workloads.
    """
    try:
        plan = prepare_shared_pool_handoff(runtime=runtime, observation=observation,
            legacy=legacy, repo_root=repo_root)
        if transport.api_server.rstrip('/') != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/'):
            raise ValueError
        roots = _resource_roots(plan, frozenset({'Deployment', 'StatefulSet', 'CronJob'}))
        legacy_jobs = (*legacy.context.original.retained.values(), *legacy.context.predecessor.retained.values())
        retained_jobs = {_key(row): copy.deepcopy(row) for row in legacy_jobs if row['kind'] == 'Job'}
        retained_jobs.update(_resource_roots(plan, frozenset({'Job'})))
        authority = _authority_roots(plan)
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

        permissions = {resource: inventory_resources(read, 'rbac.authorization.k8s.io/v1', resource, kind)
            for resource, kind in (('roles', 'Role'), ('clusterroles', 'ClusterRole'),
                ('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))}
        inventory = {resource: inventory_resources(read, api, resource, kind, include_terminal_pods=True)
            for api, resource, kind in POOL_WRITER_WORKLOAD_COLLECTIONS}
        subjects = set()
        for row in (*roots.values(), *retained_jobs.values()):
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
        qualify_pool_writer_bindings(permissions, originals={}, targets={}, expected_subjects={},
            subjects=frozenset(subjects), writer_namespaces=namespaces, staged_authority=authority,
            kube_system_uid=binding.kube_system_uid, platform_authority=legacy.context.request.platform_authority)
        qualify_pool_writer_workloads(inventory, originals=roots, expected=roots,
            subjects=frozenset(subjects), writer_namespaces=namespaces,
            controller_uids=controllers, producer_uids=producers,
            platform_subjects=platform_controller_subjects(legacy.context.request.platform_authority),
            retained_jobs=retained_jobs)
        current = {_key(row): row for rows in inventory.values() for row in rows if _key(row) in roots}
        current_authority = {_key(row): row for rows in permissions.values() for row in rows if _key(row) in authority}
        cluster()
        if (revision is None or prepare_shared_pool_handoff(runtime=runtime, observation=observation,
                legacy=legacy, repo_root=repo_root) != plan):
            raise ValueError
        checksum = digest({'handoff': plan.input_digest, 'resource_version': revision,
            'workloads': inventory, 'permissions': permissions})
        return SharedPoolWriterObservation(plan, revision, copy.deepcopy(current), copy.deepcopy(current_authority), checksum)
    except Exception:
        raise ValueError('shared pool writer census unqualified') from None


@dataclass(frozen=True, repr=False)
class SharedPoolProviderObservation:
    plan: SharedPoolHandoffPlan
    provider: ProviderCapacitySnapshot
    input_digest: str


async def observe_shared_pool_provider(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path,
        development: HTTPSDevelopmentRuntimeObserver, operator_credentials: Path) -> SharedPoolProviderObservation:
    """Read the registered physical group with the installed observer's key.

    Available zero-size and currently scaling groups qualify, even with no spare
    quota. This does not reserve headroom, publish capacity, prove external writer
    exclusivity or authorize pool opening. The parent still needs fresh aggregate
    capacity and fenced-writer/process evidence after its guarded transition.
    """
    try:
        plan = prepare_shared_pool_handoff(runtime=runtime, observation=observation,
            legacy=legacy, repo_root=repo_root)
        completed = plan.development.runtime

        def inspect() -> dict[str, dict[str, Any]]:
            if (development.request != runtime or development.plan != completed.plan
                    or development.api_server.rstrip('/') != runtime.database.foundation.inputs.config[
                        'kubernetes_api_server'].rstrip('/')):
                raise ValueError
            return {_key(row): {'uid': _uid(row), 'document': _snapshot(row)}
                for row in development.inspect(state_dir=completed.state_dir).values()}

        before = inspect()  # Includes the installed collector Secret/configuration.
        snapshot = await qualify_runtime_cloud(request=runtime, operator_credentials=operator_credentials)
        spec = plan.migration.registration.spec
        if (snapshot.node_group is None or snapshot.node_group.id != spec.node_group_id
                or {key: quota_identity(value) for key, value in snapshot.quota_resources.items()} != spec.quota_identities
                or snapshot.provider_capacity_state != 'available'
                or snapshot.autoscaler_state not in {'ready', 'scaling'}):
            raise ValueError
        if (inspect() != before or prepare_shared_pool_handoff(runtime=runtime, observation=observation,
                legacy=legacy, repo_root=repo_root) != plan):
            raise ValueError
        checksum = digest({'handoff': plan.input_digest, 'provider': snapshot.model_dump(mode='json'), 'workloads': before})
        return SharedPoolProviderObservation(plan, snapshot, checksum)
    except Exception:
        raise ValueError('shared pool provider unqualified') from None


@dataclass(frozen=True, repr=False)
class SharedPoolProcessObservation:
    bindings: SharedPoolGuardBindings
    guard_states: dict[str, str]
    retired_authority: str
    telemetry: dict[str, Any]
    input_digest: str


def observe_shared_pool_processes(*, runtime: DevelopmentRuntimeInstallRequest,
        observation: dict[str, Any], legacy: CompletedPoolCutover, repo_root: Path,
        development: HTTPSDevelopmentRuntimeProbes, kubeconfig: Path,
        executable: Path) -> SharedPoolProcessObservation:
    """Qualify current DB/process state without changing intake or credentials.

    The new manager must be CLOSED with its exact active registration; the old
    one must instead retain its completed revocation and empty work journal.
    Current staging is challenged against its source-qualified running baseline,
    not the old recovery templates. This is pre-handoff evidence only: the parent
    still owns fresh guard acquisition, drain, fencing and anchored recovery.
    """
    try:
        def bind() -> SharedPoolGuardBindings:
            return observe_shared_pool_guard_bindings(runtime=runtime, observation=observation,
                legacy=legacy, repo_root=repo_root, development=development)

        bound = bind()
        plan, migration = bound.plan, bound.migration
        completed = plan.development.runtime
        guards = KubectlPoolGuardAPI(request=migration, kubeconfig=kubeconfig, executable=executable)

        def open_guards() -> dict[str, str]:
            states = {target.namespace: guards.guard(target, 'observe')['status'] for target in migration.guards}
            if any(state != 'open' for state in states.values()):
                raise ValueError
            return states

        states = open_guards()
        old_namespace = legacy.context.original.upgrade.setup.binding.namespace
        credential = guards._get('secret', 'loom-platform-db', old_namespace)
        history_target = derive_management_history_target(original=legacy.context.original,
            predecessor=legacy.context.predecessor, credential=credential)
        history = KubectlPoolHistoryAPI(request=legacy.context.request.fencing.retirement.migration,
            target=history_target, kubeconfig=kubeconfig, executable=executable)
        if history.machine_retirement('observe') != 'revoked':
            raise ValueError
        development.qualify_closed_pool()
        participant = plan.staging.attachment.participant
        dev_participant, = (row for row in migration.registration.spec.participants
            if row.participant_id != participant.participant_id)
        for namespace, name in ((migration.registration.binding.namespace, 'loom-service'),
                ('loom-dev', 'loom-service'), ('loom-dev', 'loom-control-plane'),
                (dev_participant.execution_namespace.name, 'loom-execution-actuator')):
            key = 'Deployment:' + namespace + ':' + name
            development.qualify_runtime_settings(key=key, state_dir=completed.state_dir)
            development.qualify_runtime_database(key=key, state_dir=completed.state_dir)

        target, = (row for row in migration.guards if row.participant_id == participant.participant_id)
        database = target.database
        if (database is None or database.actuator_credential_uid is None
                or database.actuator_credential_resource_version is None):
            raise ValueError
        credential_reads = {}
        # Keep each consumer's reference, including guest actuators. Guard reads
        # cover the controller credential, not the separately scoped actuators.
        for key, original in plan.staging.originals.items():
            if original['kind'] != 'Deployment':
                continue
            actuator = original['metadata']['namespace'] == participant.execution_namespace.name
            variable = 'LOOM_EXECUTION_ACTUATOR_DB_URL' if actuator else (
                'LOOM_CP_DB_URL' if original['metadata']['name'] == 'loom-control-plane' else 'LOOM_SVC_DB_URL')
            credential_reads[key] = (original, variable,
                database.actuator_credential_uid if actuator else database.credential_uid,
                database.actuator_credential_resource_version if actuator else database.credential_resource_version)

        def staging_urls() -> dict[str, str]:
            return {key: guards._workload_database_url(original, url_variable=variable,
                credential_uid=uid, credential_resource_version=version)
                for key, (original, variable, uid, version) in credential_reads.items()}

        before_urls = staging_urls()
        for original in plan.staging.originals.values():
            if original['kind'] != 'Deployment':
                continue  # The collector has no persistent process to challenge.
            actuator = original['metadata']['namespace'] == participant.execution_namespace.name
            guards.qualify_runtime_database(target, original=original,
                credential_uid=database.actuator_credential_uid if actuator else database.credential_uid,
                credential_resource_version=database.actuator_credential_resource_version if actuator else database.credential_resource_version)
            guards.qualify_runtime_legacy_settings(target, original=original, expected=original)
            if actuator:
                guards.qualify_runtime_telemetry(target, original=original)

        # A probe may take time: repeat ledger/guard and credential checks after
        # all remote reads, then requalify retained private history and dev binds.
        development.qualify_closed_pool()
        if (history.machine_retirement('observe') != 'revoked'
                or guards._get('secret', 'loom-platform-db', old_namespace) != credential
                or open_guards() != states):
            raise ValueError
        after = bind()
        if (after.plan != plan or migration_contract(after.migration) != migration_contract(migration)
                or staging_urls() != before_urls):
            raise ValueError
        telemetry = guards.telemetry_report()
        checksum = digest({'bindings': bound.input_digest, 'guards': states, 'retired_authority': 'revoked',
            'telemetry': telemetry, 'old_credential_uid': _uid(credential),
            'old_credential_resource_version': credential['metadata']['resourceVersion']})
        return SharedPoolProcessObservation(bound, states, 'revoked', telemetry, checksum)
    except Exception:
        raise ValueError('shared pool process preflight unqualified') from None
