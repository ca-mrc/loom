"""Selected worker/build admission boundaries, never platform ownership changes.

The protected activation parent supplies exact UID-bound originals from completed
startup/foundation history. Intent projection does not authorize a write: the
parent must prove closed participant admission, current platform/runtime readiness,
empty task inventory and functioning native-build policy before applying changes.
"""
from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_development_runtime_transition import (
    DevelopmentRuntimeWorkloadAPI,
    _advance_fixed_resources,
    validate_runtime_transition,
)
from scripts.ops.nebius_environment_boundary_live import (
    EnvironmentBoundaryWorkloads as EnvironmentBoundaryWorkloads,
)
from scripts.ops.nebius_environment_foundation import FoundationCloud, _read
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_environment_runtime_install import _resources, qualify_runtime_defaulted
from scripts.ops.nebius_environment_runtime_start import (
    StartedEnvironmentRuntimeV2,
    _frozen,
    load_started_environment_runtime,
    runtime_resource_path,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_platform_runtime_readiness import PlatformRuntimePolicyV2
from scripts.ops.nebius_pool_administration_dispatch import Connection
from scripts.ops.nebius_pool_enrollment_issuer import PoolEnrollmentPolicyV2, _policy

from loom.nebius_pool_contract import PoolParticipantV1
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.profiles import _ApplicationImage, _TaskImage

if TYPE_CHECKING:
    from scripts.ops.nebius_environment_runtime_readiness import ProbeConnection

_PHASES = ('build-psa', 'worker-quota', 'build-quota')


class BoundaryWorkloadAPI(DevelopmentRuntimeWorkloadAPI, Protocol):
    def policy_ready(self) -> bool: ...
    def check_inventory(self) -> None: ...


BoundaryConnection = Callable[[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
    dict[str, str], Callable[[], None]], AbstractContextManager[BoundaryWorkloadAPI]]


def boundary_targets(*, participant: PoolParticipantV1, reader_count: int,
        originals: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Remove closed-intake quotas without introducing per-environment budgets.

    Capacity remains the shared allocator's responsibility. Only the selected
    build namespace receives the existing narrow native-builder PSA exception;
    its already-installed admission policy must be observed before/after that CAS.
    """
    try:
        participant = PoolParticipantV1.model_validate(participant.model_dump())
        execution, build = participant.execution_namespace, participant.build_namespace
        namespace_key = 'Namespace:-:' + build.name
        quotas = {'ResourceQuota:' + execution.name + ':loom-execution-disabled': {
            'hard': {'pods': str(reader_count), 'count/jobs.batch': '0'}},
            'ResourceQuota:' + build.name + ':loom-execution-disabled': {'hard': {'pods': '0'}}}
        if (type(reader_count) is not int or reader_count < 1
                or set(originals) != {namespace_key, *quotas}):
            raise ValueError
        for key, document in originals.items():
            _uid(document)
            if key != _key(document) or document['apiVersion'] != 'v1' or document['metadata'].get('deletionTimestamp'):
                raise ValueError
            if key in quotas and document['spec'] != quotas[key]:
                raise ValueError
        namespace = originals[namespace_key]
        if (_uid(namespace) != str(build.uid) or namespace['metadata'].get('namespace') is not None
                or namespace['metadata']['labels']['pod-security.kubernetes.io/enforce'] != 'restricted'):
            raise ValueError
        targets = {key: _snapshot(doc) for key, doc in originals.items()}
        targets[namespace_key]['metadata']['labels']['pod-security.kubernetes.io/enforce'] = 'privileged'
        for key in quotas:
            targets[key]['spec'] = {}
        return targets
    except Exception:
        raise ValueError('environment_boundary_intent_unqualified') from None


def _readers(started: StartedEnvironmentRuntimeV2) -> dict[str, dict[str, Any]]:
    return {key: doc for key, doc in started.resources.items() if doc['kind'] == 'Deployment'
        and doc['metadata']['namespace'] == started.staged.plan['policy']['participant']['execution_namespace']['name']}


def _keys(participant: PoolParticipantV1) -> dict[str, str]:
    return dict(zip(_PHASES, ('Namespace:-:' + participant.build_namespace.name,
        'ResourceQuota:' + participant.execution_namespace.name + ':loom-execution-disabled',
        'ResourceQuota:' + participant.build_namespace.name + ':loom-execution-disabled'), strict=True))


@dataclass(frozen=True, repr=False)
class EnvironmentBoundaryProgressV2:
    options: dict[str, tuple[dict[str, Any], ...]]
    files: dict[Path, bytes]
    plan: dict[str, Any] | None
    cursor: dict[str, Any] | None

    @property
    def sha256(self) -> str:
        return digest({str(path): digest(raw.decode()) for path, raw in self.files.items()})


def load_environment_boundary_progress(*, policy: PoolEnrollmentPolicyV2,
        started: StartedEnvironmentRuntimeV2) -> EnvironmentBoundaryProgressV2:
    """Only anchored successors may relax original foundation/startup predicates."""
    try:
        directory = started.staged.state_dir / 'boundaries'
        operation_id = started.staged.plan['operation_id']
        paths = [directory / 'plan.json', directory / 'cursor.json',
            policy.host.anchor_root / ('runtime-boundaries-' + operation_id + '.json')]
        if not any(path.exists() or path.is_symlink() for path in paths):
            if any((directory / phase).exists() or (directory / phase).is_symlink() for phase in _PHASES):
                raise ValueError
            return EnvironmentBoundaryProgressV2({}, {}, None, None)
        files = {path: state._private_read(path, limit=8 * 1024**2) for path in paths}
        plan, cursor, anchor = (json.loads(files[path]) for path in paths)
        if (set(plan) != {'startup_sha256', 'originals'} or plan['startup_sha256'] != digest(started.resources)
                or anchor != {'operation_id': operation_id, 'plan_sha256': digest(plan)}
                or set(cursor) != {'phases'} or set(cursor['phases']) != set(_PHASES)):
            raise ValueError
        originals = plan['originals']
        targets = boundary_targets(participant=policy.participant, reader_count=len(_readers(started)), originals=originals)
        for key, original in originals.items():
            if _uid(original) != started.staged.plan['foundation_identities']['k8s:' + key]:
                raise ValueError
            if key in started.resources and not _matches(original, started.resources[key], _uid(started.resources[key])):
                raise ValueError
        options: dict[str, tuple[dict[str, Any], ...]] = {key: (doc,) for key, doc in originals.items()}
        incomplete = False
        for phase, key in _keys(policy.participant).items():
            status = cursor['phases'][phase]
            path = directory / phase / 'transition.json'
            if status not in {'prepared', 'intent', 'complete'} or (incomplete and status != 'prepared'):
                raise ValueError
            incomplete |= status != 'complete'
            if status == 'prepared':
                if path.exists() or path.is_symlink():
                    raise ValueError
                continue
            files[path] = state._private_read(path, limit=8 * 1024**2)
            record = json.loads(files[path])
            validate_runtime_transition(record, originals={key: originals[key]}, targets={key: targets[key]},
                input_digest='sha256:' + digest(plan), phase=phase, qualify_defaulted=qualify_runtime_defaulted)
            item = record['resources'][key]
            if status == 'complete' and item['status'] != 'applied':
                raise ValueError
            if item['status'] != 'prepared':
                desired = copy.deepcopy(item['expected'])
                desired['metadata']['uid'] = _uid(originals[key])
                options[key] = (desired,) if item['status'] == 'applied' else (originals[key], desired)
        if any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items()):
            raise ValueError
        return EnvironmentBoundaryProgressV2(options, files, plan, cursor)
    except Exception:
        raise ValueError('environment_boundary_history_unqualified') from None


@contextmanager
def _boundary_connection(before: dict[str, dict[str, Any]], desired: dict[str, dict[str, Any]],
        namespaces: dict[str, str], check: Callable[[], None], *, policy: PoolEnrollmentPolicyV2,
        started: StartedEnvironmentRuntimeV2) -> Iterator[BoundaryWorkloadAPI]:
    profiles: tuple[_TaskImage | _ApplicationImage, ...] = (*policy.profiles.task_images, *policy.profiles.application_images)
    if not profiles or any(row.target.namespace != policy.participant.build_namespace.name for row in profiles):
        raise ValueError('environment_boundary_build_profiles_unqualified')
    build_policy = {key: doc for key, doc in started.resources.items()
        if doc['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}}
    with _resources(policy, desired, namespaces, check) as api:
        yield EnvironmentBoundaryWorkloads(api, before, desired, policy.host.runtime.region,
            _readers(started), build_policy=build_policy, build_profile=profiles[0])


def verify_prepared_environment_boundaries(*, policy: PoolEnrollmentPolicyV2, started: StartedEnvironmentRuntimeV2,
        progress: EnvironmentBoundaryProgressV2, qualify: Callable[[], None],
        boundary_connection: BoundaryConnection | None = None) -> bool:
    """Fresh read-only admission/inventory proof from every completed successor."""
    if progress.plan is None or progress.cursor != {'phases': dict.fromkeys(_PHASES, 'complete')}:
        raise ValueError('environment_boundaries_not_prepared')
    qualify()
    factory: BoundaryConnection = boundary_connection or partial(_boundary_connection, policy=policy, started=started)
    targets = boundary_targets(participant=policy.participant, reader_count=len(_readers(started)), originals=progress.plan['originals'])
    with factory(progress.plan['originals'], targets, started.staged.plan['namespace_uids'], qualify) as api:
        api.check_inventory()
        if not api.policy_ready():
            return False
        for key, choices in progress.options.items():
            expected, = choices
            if not api.workload_ready(key, _snapshot(expected)):
                return False
        api.check_inventory()
        ready = api.policy_ready()
    qualify()
    return ready


async def prepare_environment_boundaries(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID,
        platform_policy: PlatformRuntimePolicyV2, catalog: GitHubCandidateCatalog, kubernetes: KubernetesEnvironmentProvider,
        cloud: FoundationCloud, public_http: httpx.AsyncClient, connect: Connection,
        probe_connection: ProbeConnection | None = None, platform_probe_connection: ProbeConnection | None = None,
        boundary_connection: BoundaryConnection | None = None) -> dict[str, Any] | None:
    """Open only selected Kubernetes boundaries; participant admission stays closed.

    Fresh combined application/platform evidence precedes each invocation. Native
    build policy is proven before/after PSA; only then are temporary quotas lifted.
    The same immutable startup, fixed intents and UID-CAS journals survive retries.
    """
    from scripts.ops.nebius_environment_runtime_readiness import observe_environment_pre_activation

    try:
        started = load_started_environment_runtime(policy=policy, operation_id=operation_id)
        directory = started.staged.state_dir / 'boundaries'
        with state._locked_state(directory):
            evidence = await observe_environment_pre_activation(policy=policy, operation_id=operation_id,
                platform_policy=platform_policy, catalog=catalog, kubernetes=kubernetes, cloud=cloud,
                public_http=public_http, connect=connect, probe_connection=probe_connection,
                platform_probe_connection=platform_probe_connection)
            if evidence is None:
                return None
            progress = load_environment_boundary_progress(policy=policy, started=started)
            path, cursor_path = directory / 'plan.json', directory / 'cursor.json'
            anchor = policy.host.anchor_root / ('runtime-boundaries-' + str(operation_id) + '.json')
            if progress.plan is None:
                foundation = load_foundation(policy.foundation_dir, policy.foundation)
                docs = {_key(doc): doc for group in foundation['files'].values() for doc in group}
                originals = {}
                for key in _keys(policy.participant).values():
                    doc = started.resources.get(key, docs[key])
                    actual = await kubernetes._request('GET', runtime_resource_path(doc) + '/' + doc['metadata']['name'])
                    if actual is None:
                        raise ValueError
                    uid = started.staged.plan['foundation_identities']['k8s:' + key]
                    if key in started.resources:
                        if not _matches(actual, doc, uid):
                            raise ValueError
                    else:
                        kubernetes._identity(actual, doc, uid)
                    originals[key] = _frozen(actual)
                boundary_targets(participant=policy.participant, reader_count=len(_readers(started)), originals=originals)
                initial_plan = {'startup_sha256': digest(started.resources), 'originals': originals}
                state._write_private(path, json.dumps(initial_plan, sort_keys=True).encode())
                state._atomic_json(cursor_path, {'phases': dict.fromkeys(_PHASES, 'prepared')})
                state._write_private(anchor, json.dumps({'operation_id': str(operation_id), 'plan_sha256': digest(initial_plan)}).encode())
                state._sync_directory(policy.host.anchor_root)
                progress = load_environment_boundary_progress(policy=policy, started=started)
            plan, cursor = progress.plan, progress.cursor
            assert plan is not None and cursor is not None
            targets = boundary_targets(participant=policy.participant, reader_count=len(_readers(started)), originals=plan['originals'])
            expires = datetime.fromisoformat(evidence['valid_until'])
            def qualify() -> None:
                if (datetime.now(UTC) >= expires or _policy(policy) != started.staged.plan['policy']
                        or _read(path) != plan or _read(cursor_path) != cursor
                        or _read(anchor) != {'operation_id': str(operation_id), 'plan_sha256': digest(plan)}
                        or any(state._private_read(file, limit=8 * 1024**2) != raw for file, raw in started.files.items())):
                    raise ValueError('environment_boundary_evidence_changed')
            factory: BoundaryConnection = boundary_connection or partial(_boundary_connection, policy=policy, started=started)
            for phase, key in _keys(policy.participant).items():
                before, desired = {key: plan['originals'][key]}, {key: targets[key]}
                child = directory / phase
                def advance(before: dict[str, dict[str, Any]], desired: dict[str, dict[str, Any]], phase: str, child: Path) -> bool:
                    with factory(before, desired, started.staged.plan['namespace_uids'], qualify) as api:
                        api.check_inventory()
                        if not api.policy_ready():
                            return False
                        if cursor['phases'][phase] == 'prepared':
                            if child.exists() or child.is_symlink():
                                raise ValueError('environment_boundary_untracked_child')
                            cursor['phases'][phase] = 'intent'
                            state._atomic_json(cursor_path, cursor)
                        if not _advance_fixed_resources(originals=before, targets=desired, input_digest='sha256:' + digest(plan),
                                phase=phase, api=api, state_dir=child, phases=frozenset(_PHASES),
                                kinds=frozenset({'Namespace', 'ResourceQuota'}), qualify_defaulted=qualify_runtime_defaulted):
                            return False
                        api.check_inventory()
                        return api.policy_ready()
                if not await asyncio.to_thread(advance, before, desired, phase, child):
                    return dict(status='runtime_boundaries_pending', phase=phase, operation_id=str(operation_id), admission_open=False)
                cursor['phases'][phase] = 'complete'
                state._atomic_json(cursor_path, cursor)
            completed = load_environment_boundary_progress(policy=policy, started=started)
            if completed.cursor != {'phases': dict.fromkeys(_PHASES, 'complete')}:
                raise ValueError
            qualify()
            return dict(status='runtime_boundaries_prepared_closed', operation_id=str(operation_id),
                participant_id=str(policy.participant.participant_id), plan_sha256=digest(plan), admission_open=False)
    except Exception:
        raise ValueError('environment_boundary_preparation_unqualified') from None
