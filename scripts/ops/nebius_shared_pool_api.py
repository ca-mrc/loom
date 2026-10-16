"""Concrete protected authority for shared-pool installation and terminal recovery.

The parent accepts retained inputs, not operator-supplied readiness callbacks.
Every phase uses its existing sealed child and durable intent. Observations are
current and read-only; completed replay never constructs an active mutator.
"""
from __future__ import annotations

import asyncio
import copy
import json
import ssl
from collections.abc import Callable
from contextlib import ExitStack
from functools import partial
from pathlib import Path
from typing import Any, TypedDict

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops import nebius_shared_pool_activation as activation
from scripts.ops import nebius_shared_pool_build_activation as build
from scripts.ops import nebius_shared_pool_database as database
from scripts.ops import nebius_shared_pool_drain as drain
from scripts.ops import nebius_shared_pool_gateway as gateway
from scripts.ops import nebius_shared_pool_guards as guard_phase
from scripts.ops import nebius_shared_pool_material as material
from scripts.ops import nebius_shared_pool_recovery_drain as recovery_drain
from scripts.ops import nebius_shared_pool_recovery_fence as fence
from scripts.ops import nebius_shared_pool_recovery_gateway as recovery_gateway
from scripts.ops import nebius_shared_pool_recovery_machines as machines
from scripts.ops import nebius_shared_pool_recovery_reopening as reopening
from scripts.ops import nebius_shared_pool_recovery_restart as restart
from scripts.ops import nebius_shared_pool_recovery_roles as recovery_roles
from scripts.ops import nebius_shared_pool_recovery_templates as templates
from scripts.ops import nebius_shared_pool_recovery_workloads as recovery_workloads
from scripts.ops import nebius_shared_pool_roles as roles
from scripts.ops import nebius_shared_pool_workloads as workloads
from scripts.ops.nebius_development_activation import review_development_gateway_permissions
from scripts.ops.nebius_development_runtime_external import (
    qualify_runtime_publication,
    qualify_staging_publication,
)
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest, _read
from scripts.ops.nebius_development_runtime_live import runtime_resource_path
from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_gateway_authority import subject_review_namespaces
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
from scripts.ops.nebius_shared_pool_activation_live import SharedPoolActivationTransport
from scripts.ops.nebius_shared_pool_forward_census import observe_shared_pool_forward_writers
from scripts.ops.nebius_shared_pool_forward_processes import (
    SharedPoolForwardProcessObservation,
    _gateway_history_target,
    observe_shared_pool_forward_processes,
)
from scripts.ops.nebius_shared_pool_handoff_live import (
    SharedPoolGuardBindings,
    SharedPoolWriterObservation,
    observe_shared_pool_processes,
    observe_shared_pool_provider,
    observe_shared_pool_writers,
)
from scripts.ops.nebius_shared_pool_operation import (
    FORWARD_PHASES,
    RECOVERY_PHASES,
    _files,
    _history,
    _qualify_bindings,
    _record,
)
from scripts.ops.nebius_shared_pool_recovery_census import observe_shared_pool_recovery_writers
from scripts.ops.nebius_shared_pool_recovery_collector import SharedRecoveryRuntimeReadiness
from scripts.ops.nebius_shared_pool_recovery_databases import observe_shared_pool_recovery_databases
from scripts.ops.nebius_shared_pool_recovery_fence_live import SharedPoolRecoveryFenceTransport
from scripts.ops.nebius_shared_pool_recovery_processes import (
    SharedPoolRecoveryProcessObservation,
    observe_shared_pool_recovery_processes,
)
from scripts.ops.nebius_shared_pool_runtime_observation import HTTPSSharedPoolRuntimeProbes
from scripts.ops.nebius_shared_pool_workloads import _frame

Document = dict[str, Any]
PhaseFunction = Callable[..., Any]


class _PhaseArguments(TypedDict):
    bindings: SharedPoolGuardBindings
    state_dir: Path


class _ObservationArguments(_PhaseArguments):
    kubeconfig: Path
    executable: Path


class _ConnectionArguments(TypedDict):
    bindings: SharedPoolGuardBindings
    api_server: str
    ssl_context: ssl.SSLContext
    token: str


class _RuntimeArguments(_ConnectionArguments):
    private_files: dict[Path, bytes]
    qualify_runtime: Callable[[DevelopmentRuntimeInstallRequest, str, bool], None]


class SharedPoolPhaseEvidence:
    """The real child validators without credentials, transport or mutators.

    Historical completion and the live parent use the same semantic validators.
    This reader never supplies preflight, qualification or dispatch authority.
    """

    supported_phases = frozenset((*FORWARD_PHASES, *RECOVERY_PHASES))

    def __init__(self, *, bindings: SharedPoolGuardBindings):
        self.bindings = copy.deepcopy(bindings)
        manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'

    def validators(self) -> dict[str, tuple[PhaseFunction, ...]]:
        result: dict[str, tuple[PhaseFunction, ...]] = {phase: (validate,) for phase, validate in {
            'forward-guards': guard_phase.validate_shared_pool_guards,
            'forward-drain': drain.validate_shared_pool_drain,
            'forward-roles': roles.validate_shared_staging_roles,
            'forward-database': database.validate_shared_staging_database,
            'forward-material': material.validate_shared_staging_material,
            'forward-build-policy': build.validate_shared_build_activation,
            'forward-gateway': gateway.validate_shared_gateway,
            'forward-activate': activation.validate_shared_pool_activation,
            'forward-verify': self.validate_forward_verification,
            'recovery-fence': fence.validate_shared_recovery_fence,
            'recovery-drain': recovery_drain.validate_shared_recovery_drain,
            'recovery-stop': recovery_workloads.validate_shared_recovery_workloads,
            'recovery-restore-workloads': templates.validate_shared_recovery_templates,
            'recovery-restore-roles': recovery_roles.validate_shared_recovery_roles,
            'recovery-restart': restart.validate_shared_recovery_restart,
            'recovery-reopen': reopening.validate_shared_recovery_reopening,
            'recovery-verify': reopening.validate_shared_recovery_verification,
        }.items()}
        for phase in ('forward-stop', 'forward-wire', 'forward-start'):
            result[phase] = (partial(workloads.validate_shared_staging_workloads, phase=phase),)
        result['recovery-revoke'] = (machines.validate_shared_recovery_machines,
            recovery_gateway.validate_shared_recovery_gateway)
        return result

    def validate_prepared_phase(self, phase: str, state: Path) -> None:
        self._validate(phase, state, complete=False, prepared=True)

    def validate_phase(self, phase: str, state: Path, *, complete: bool) -> None:
        self._validate(phase, state, complete=complete, prepared=False)

    def _validate(self, phase: str, state: Path, *, complete: bool, prepared: bool) -> None:
        if phase not in self.supported_phases or state != self.state / phase:
            raise ValueError('shared child validation scope differs')
        _history(self.bindings)
        _validate_child(phase, state, bindings=self.bindings, validators=self.validators()[phase],
            complete=complete, prepared=prepared)
        _history(self.bindings)

    def verification_identity(self, state: Path) -> Document:
        if state != self.state / 'forward-verify':
            raise ValueError('shared verification path differs')
        return {'schema': 'loom.nebius-shared-forward-verification.v1',
            'input_digest': self.bindings.input_digest, 'state_dir': str(state)}

    def validate_forward_verification(self, *, bindings: SharedPoolGuardBindings, state_dir: Path,
            complete: bool, prepared: bool = False) -> Document:
        if bindings != self.bindings:
            raise ValueError('shared verification binding differs')
        frame = _frame(self.bindings, self.state)
        identity = self.verification_identity(state_dir)
        value = _read(state_dir / 'verify.json')
        if (type(complete) is not bool or type(prepared) is not bool or (complete and prepared)
                or set(value) != {*identity, 'status'} or any(value[key] != item for key, item in identity.items())
                or value['status'] not in {'prepared', 'observed'} or (complete and value['status'] != 'observed')
                or (prepared and (value['status'] != 'prepared' or frame['forward']['forward-verify']['status'] != 'prepared'))
                or any(frame['forward'][phase]['status'] != 'complete' for phase in FORWARD_PHASES[:-1])):
            raise ValueError('shared forward verification differs')
        activation.validate_shared_pool_activation(bindings=self.bindings, state_dir=self.state / 'forward-activate', complete=True)
        if _frame(self.bindings, self.state) != frame:
            raise ValueError('shared forward verification inputs changed')
        return value


def _validate_child(phase: str, state: Path, *, bindings: SharedPoolGuardBindings,
        validators: tuple[PhaseFunction, ...], complete: bool, prepared: bool) -> None:
    for validate in validators:
        if phase == 'forward-guards':
            record = validate(bindings=bindings, state_dir=state, complete=complete)
            if prepared and set(record['guards'].values()) != {'prepared'}:
                raise ValueError('prepared guard already has intent')
        else:
            validate(bindings=bindings, state_dir=state, complete=complete, prepared=prepared)


class HTTPSSharedPoolOperationAPI(HTTPSSharedPoolRuntimeProbes):
    supported_phases = frozenset((*FORWARD_PHASES, *RECOVERY_PHASES))

    def __init__(self, *, bindings: SharedPoolGuardBindings, observation: Document, repo_root: Path,
            api_server: str, ssl_context: ssl.SSLContext, token: str, operator_credentials: Path,
            private_files: dict[Path, bytes]):
        if (operator_credentials not in private_files
                or private_state._private_read(operator_credentials, limit=1024**2) != private_files[operator_credentials]):
            raise ValueError('shared parent operator connection unqualified')
        super().__init__(bindings=bindings, api_server=api_server, ssl_context=ssl_context, token=token, private_files=private_files)
        self._captured = copy.deepcopy(bindings)
        self.observation, self.repo_root = copy.deepcopy(observation), repo_root
        self._origin = (copy.deepcopy(observation), repo_root)
        self.operator_credentials = operator_credentials
        self.context, self.token = ssl_context, token
        manager = self.request.database.manager.retained.request.retained
        self.shared_state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff'
        self._evidence = SharedPoolPhaseEvidence(bindings=self._captured)
        self.anchor = Path(manager.operation['anchor_dir'])
        self.guards = KubectlPoolGuardAPI(request=self.shared_bindings.migration,
            kubeconfig=Path(self.prefix[self.prefix.index('--kubeconfig') + 1]), executable=Path(self.prefix[0]))
        self._connection_prefix = tuple(self.prefix)
        self._guard_prefix = tuple(self.guards.prefix)

    def _scope(self, bindings: SharedPoolGuardBindings | None = None) -> None:
        if (self.shared_bindings != self._captured or (bindings is not None and bindings != self._captured)
                or self.request != self._captured.plan.development.runtime.request
                or self._evidence.bindings != self._captured or self._evidence.state != self.shared_state
                or (self.observation, self.repo_root) != self._origin
                or self.guards.request != self._captured.migration or tuple(self.prefix) != self._connection_prefix
                or tuple(self.guards.prefix) != self._guard_prefix or self.operator_credentials not in self.private_files):
            raise ValueError('shared parent binding changed')
        self._private_inputs()
        _history(self._captured)

    def _arguments(self) -> Document:
        return dict(runtime=self.request, observation=self.observation,
            legacy=self._captured.plan.staging.legacy, repo_root=self.repo_root)

    def _state(self) -> tuple[Document, bool]:
        self._scope()
        identity = {'schema': 'loom.nebius-shared-pool-handoff.v1',
            'operation_id': str(self._captured.migration.registration.spec.operation_id),
            'state_dir': str(self.shared_state), 'input_digest': self._captured.input_digest}
        return _record(state=self.shared_state, marker=self.anchor / 'shared-pool-handoff.json',
            recovery_marker=self.anchor / 'shared-pool-recovery.json', identity=identity, api=self)

    def _phase(self, phase: str, state: Path, status: str) -> Document:
        if phase not in self.supported_phases or state != self.shared_state / phase:
            raise ValueError('shared parent phase differs')
        frame, recovering = self._state()
        selected = RECOVERY_PHASES if recovering else FORWARD_PHASES
        direction = 'recovery' if recovering else 'forward'
        if (frame['outcome'] != 'pending' or phase not in selected
                or frame[direction][phase]['status'] != status
                or any(frame[direction][prior]['status'] != 'complete' for prior in selected[:selected.index(phase)])):
            raise ValueError('shared parent phase is not active')
        return frame

    async def _external(self, development: HTTPSDevelopmentRuntimeProbes) -> None:
        self._scope()
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_runtime_publication(request=self.request, http=http)
            await qualify_staging_publication(request=self.request, staging=self._captured.plan.staging, http=http)
        observed = await observe_shared_pool_provider(**self._arguments(), development=development,
            operator_credentials=self.operator_credentials)
        if observed.plan != self._captured.plan:
            raise ValueError('shared parent provider scope differs')
        self._scope()

    def _absent(self) -> None:
        plan = self._captured.plan
        for row in (*plan.staging_material.values(), *plan.staging_reader_authority.values(), *plan.development.authority.values()):
            if self._request('GET', runtime_resource_path(row) + '/' + row['metadata']['name']) is not None:
                raise ValueError('shared parent target already exists')

    def preflight(self, bindings: SharedPoolGuardBindings) -> None:
        self._scope(bindings)
        _qualify_bindings(**self._arguments(), bindings=self._captured)
        for path in (self.shared_state, self.anchor / 'shared-pool-handoff.json', self.anchor / 'shared-pool-recovery.json'):
            if path.exists() or path.is_symlink():
                raise ValueError('shared preflight cannot replay an anchored operation')
        with HTTPSDevelopmentRuntimeProbes(request=self.request, api_server=self.api_server,
                ssl_context=self.context, token=self.token, private_files=self.private_files) as initial:
            asyncio.run(self._external(initial))
            writers = observe_shared_pool_writers(**self._arguments(), transport=self)
            self._absent()
            processes = observe_shared_pool_processes(**self._arguments(), development=initial,
                kubeconfig=self.guards.kubeconfig, executable=Path(self.prefix[0]))
            if writers.plan != self._captured.plan or processes.bindings != self._captured:
                raise ValueError('shared preflight scope differs')
        self._absent()
        self._scope(bindings)

    def _permissions(self, frame: Document, *, recovering: bool, writers: SharedPoolWriterObservation) -> None:
        direction = frame['recovery' if recovering else 'forward']
        gateway_ready = direction['recovery-revoke' if recovering else 'forward-gateway']['status'] == 'complete'
        if gateway_ready:
            review_development_gateway_permissions(self.request, transport=self, granted=not recovering)
        roles_ready = direction['recovery-restore-roles' if recovering else 'forward-roles']['status'] == 'complete'
        if not roles_ready:
            return
        # Reuse the restored-account rules validator with the actual census's
        # receipt-qualified catalog. It also covers forward read-only Roles.
        def bindings() -> list[Document]:
            return sorted((row for resource, kind in (('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))
                for row in inventory_resources(self._request, 'rbac.authorization.k8s.io/v1', resource, kind)), key=_key)

        before = bindings()
        subjects = set()
        for row in self._captured.plan.staging.originals.values():
            pod = row['spec']['jobTemplate']['spec']['template']['spec'] if row['kind'] == 'CronJob' else row['spec']['template']['spec']
            subjects.add((row['metadata']['namespace'], pod.get('serviceAccountName', 'default')))
        for subject in sorted(subjects):
            namespace, account = subject
            headers = [('Impersonate-User', f'system:serviceaccount:{namespace}:{account}'),
                *(('Impersonate-Group', group) for group in ('system:serviceaccounts',
                    'system:serviceaccounts:' + namespace, 'system:authenticated'))]
            for selected in subject_review_namespaces(self._captured.migration, before, subject=subject):
                with self.client.stream('POST', '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews', headers=headers,
                        json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview', 'spec': {'namespace': selected}}) as response:
                    if response.status_code != 201 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                        raise ValueError('shared permissions unavailable')
                    raw = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        if len(raw) + len(chunk) > 4 * 1024**2:
                            raise ValueError('shared permissions exceed bound')
                        raw.extend(chunk)
                    recovery_roles._qualify_rules(json.loads(raw), catalog=writers.authority, subject=subject, namespace=selected)
        if bindings() != before:
            raise ValueError('shared permissions changed during review')

    def _observe(self, *, ready: bool = False) -> SharedPoolForwardProcessObservation | SharedPoolRecoveryProcessObservation:
        before, recovering = self._state()
        files = _files(self.shared_state)
        observer = observe_shared_pool_recovery_writers if recovering else observe_shared_pool_forward_writers
        writers = observer(bindings=self._captured, state_dir=self.shared_state, transport=self)
        if writers.plan != self._captured.plan:
            raise ValueError('shared writer scope differs')
        asyncio.run(self._external(self))
        self._permissions(before, recovering=recovering, writers=writers)
        # Finish provider/permission/writer inventory before final SQL/process
        # barriers; no trailing full inventory can hide a late backend restart.
        current = observer(bindings=self._captured, state_dir=self.shared_state, transport=self)
        if current.plan != self._captured.plan or current.authority.keys() != writers.authority.keys():
            raise ValueError('shared writer authority changed')
        for before_scope, after_scope in ((writers.authority, current.authority),
                (writers.application_scopes, current.application_scopes)):
            if (before_scope.keys() != after_scope.keys()
                    or any(not _matches(after_scope[key], row, _uid(row)) for key, row in before_scope.items())):
                raise ValueError('shared writer authority changed')
        def recheck_application_scopes() -> None:
            # Scope identity must survive the process challenges too. Use only
            # the captured namespace/admission objects, inside the process
            # barrier, so the final backend identities remain the last reads.
            for row in current.application_scopes.values():
                path = ('/api/v1/namespaces' if row['kind'] == 'Namespace' else runtime_resource_path(row))
                actual = self._request('GET', path + '/' + row['metadata']['name'])
                if actual is None or not _matches(actual, row, _uid(row)):
                    raise ValueError('shared application scope changed during process observation')

        arguments: _ObservationArguments = dict(bindings=self._captured, state_dir=self.shared_state,
            kubeconfig=self.guards.kubeconfig, executable=Path(self.prefix[0]))
        result: SharedPoolForwardProcessObservation | SharedPoolRecoveryProcessObservation
        if recovering:
            databases = observe_shared_pool_recovery_databases(**arguments)
            result = observe_shared_pool_recovery_processes(**arguments,
                recheck_application_scopes=recheck_application_scopes)
            # The process observer challenges restored staging, not the stopped
            # dev or manager backends. Pin every retained database across those
            # later reads, including on read-only terminal replay.
            if (databases.bindings != self._captured
                    or observe_shared_pool_recovery_databases(**arguments) != databases):
                raise ValueError('shared recovery database scope changed')
        else:
            result = observe_shared_pool_forward_processes(**arguments, development=self,
                recheck_application_scopes=recheck_application_scopes)
        if result.bindings != self._captured or (ready and result.pending_workloads):
            raise ValueError('shared process observation pending or mismatched')
        if self._state() != (before, recovering) or _files(self.shared_state) != files:
            raise ValueError('shared parent inputs changed during observation')
        self._scope()
        return result

    def qualify(self, bindings: SharedPoolGuardBindings, record: Document, *, recovering: bool) -> None:
        self._scope(bindings)
        if type(recovering) is not bool or self._state() != (record, recovering):
            raise ValueError('shared parent requested frame differs')
        self._observe(ready=record['outcome'] != 'pending')

    def _qualify_current(self) -> None:
        self._observe()

    def _child_qualify(self, request: DevelopmentRuntimeInstallRequest, phase: str, writing: bool) -> None:
        if request != self.request or type(writing) is not bool:
            raise ValueError('shared parent child binding differs')
        if writing:
            self._phase(phase, self.shared_state / phase, 'started')
        else:
            frame, recovering = self._state()
            if phase not in frame['recovery' if recovering else 'forward'] or frame['recovery' if recovering else 'forward'][phase]['status'] not in {'started', 'complete'}:
                raise ValueError('shared parent child read is unanchored')
        self._observe()

    def _bound_qualify(self, phase: str, bindings: SharedPoolGuardBindings, writing: bool) -> None:
        self._scope(bindings)
        self._child_qualify(self.request, phase, writing)

    def _ready(self, request: DevelopmentRuntimeInstallRequest, phase: str, original: Document,
            expected: Document, actual: Document) -> bool:
        self._child_qualify(request, phase, False)
        if not _matches(actual, expected, _uid(original)):
            return False
        return _key(actual) not in self._observe().pending_workloads

    def _history_api(self) -> KubectlPoolHistoryAPI:
        credential = self.guards._get('secret', 'loom-platform-db', self._captured.migration.registration.binding.namespace)
        return KubectlPoolHistoryAPI(request=self._captured.migration, target=_gateway_history_target(self._captured, credential),
            kubeconfig=self.guards.kubeconfig, executable=Path(self.prefix[0]))

    def _recovery_drained(self) -> bool:
        return recovery_drain.observe_shared_recovery_drain(bindings=self._captured, guards=self.guards,
            history=self._history_api(), state_dir=self.shared_state, qualify=self._qualify_current)

    def _registry(self) -> dict[str, tuple[tuple[PhaseFunction, PhaseFunction], ...]]:
        prepares: dict[str, tuple[PhaseFunction, ...]] = {phase: (prepare,) for phase, prepare in {
            'forward-guards': guard_phase.prepare_shared_pool_guards,
            'forward-drain': drain.prepare_shared_pool_drain,
            'forward-roles': roles.prepare_shared_staging_roles,
            'forward-database': database.prepare_shared_staging_database,
            'forward-material': material.prepare_shared_staging_material,
            'forward-build-policy': build.prepare_shared_build_activation,
            'forward-gateway': gateway.prepare_shared_gateway,
            'forward-activate': activation.prepare_shared_pool_activation,
            'forward-verify': self._prepare_forward_verification,
            'recovery-fence': fence.prepare_shared_recovery_fence,
            'recovery-drain': recovery_drain.prepare_shared_recovery_drain,
            'recovery-stop': recovery_workloads.prepare_shared_recovery_workloads,
            'recovery-restore-workloads': templates.prepare_shared_recovery_templates,
            'recovery-restore-roles': recovery_roles.prepare_shared_recovery_roles,
            'recovery-restart': restart.prepare_shared_recovery_restart,
            'recovery-reopen': reopening.prepare_shared_recovery_reopening,
            'recovery-verify': reopening.prepare_shared_recovery_verification,
        }.items()}
        for phase in ('forward-stop', 'forward-wire', 'forward-start'):
            prepares[phase] = (partial(workloads.prepare_shared_staging_workloads, phase=phase),)
        prepares['recovery-revoke'] = (machines.prepare_shared_recovery_machines,
            recovery_gateway.prepare_shared_recovery_gateway)
        validators = self._evidence.validators()
        validators['forward-verify'] = (self._validate_forward_verification,)
        return {phase: tuple(zip(prepare, validators[phase], strict=True)) for phase, prepare in prepares.items()}

    def prepare(self, phase: str, bindings: SharedPoolGuardBindings, state: Path) -> None:
        self._scope(bindings)
        frame = self._phase(phase, state, 'prepared')
        for prepare, _ in self._registry()[phase]:
            prepare(bindings=self._captured, state_dir=state)
        if self._phase(phase, state, 'prepared') != frame:
            raise ValueError('shared phase changed during preparation')

    def _validate(self, phase: str, state: Path, *, complete: bool, prepared: bool) -> None:
        self._scope()
        if phase not in self.supported_phases or state != self.shared_state / phase:
            raise ValueError('shared child validation scope differs')
        _validate_child(phase, state, bindings=self._captured,
            validators=tuple(validate for _, validate in self._registry()[phase]), complete=complete, prepared=prepared)

    def validate_prepared_phase(self, phase: str, state: Path) -> None:
        self._validate(phase, state, complete=False, prepared=True)

    def validate_phase(self, phase: str, state: Path, *, complete: bool) -> None:
        self._validate(phase, state, complete=complete, prepared=False)

    def _verification_identity(self, state: Path) -> Document:
        return self._evidence.verification_identity(state)

    def _validate_forward_verification(self, *, bindings: SharedPoolGuardBindings, state_dir: Path,
            complete: bool, prepared: bool = False) -> Document:
        self._scope(bindings)
        return self._evidence.validate_forward_verification(
            bindings=bindings, state_dir=state_dir, complete=complete, prepared=prepared)

    def _prepare_forward_verification(self, *, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
        self._scope(bindings)
        with private_state._locked_state(state_dir):
            path = state_dir / 'verify.json'
            if not path.exists() and not path.is_symlink():
                private_state._atomic_json(path, {**self._verification_identity(state_dir), 'status': 'prepared'})
            self._validate_forward_verification(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)

    def _advance_forward_verification(self, state: Path) -> bool:
        frame = self._phase('forward-verify', state, 'started')
        with private_state._locked_state(state):
            self._validate_forward_verification(bindings=self._captured, state_dir=state, complete=False)
            result = self._observe(ready=True)
            if (not isinstance(result, SharedPoolForwardProcessObservation) or result.pool_state != 'global'
                    or set(result.guard_states.values()) != {'open'} or self._phase('forward-verify', state, 'started') != frame):
                raise ValueError('shared forward completion is unconfirmed')
            private_state._atomic_json(state / 'verify.json', {**self._verification_identity(state), 'status': 'observed'})
            return True

    def advance(self, phase: str, bindings: SharedPoolGuardBindings, state: Path) -> bool:
        self._scope(bindings)
        self._phase(phase, state, 'started')
        self.validate_phase(phase, state, complete=False)
        bound = self._captured
        common: _PhaseArguments = dict(bindings=bound, state_dir=state)
        connection: _ConnectionArguments = dict(bindings=bound, api_server=self.api_server, ssl_context=self.context, token=self.token)
        runtime: _RuntimeArguments = dict(**connection, private_files=self.private_files, qualify_runtime=self._child_qualify)
        with ExitStack() as children:
            if phase == 'forward-guards':
                return guard_phase.advance_shared_pool_guards(**common, api=self.guards, qualify=self._qualify_current)
            if phase == 'forward-drain':
                return drain.advance_shared_pool_drain(**common, guards=self.guards, development=self, qualify=self._qualify_current)
            if phase in {'forward-stop', 'forward-wire', 'forward-start'}:
                api = children.enter_context(workloads.HTTPSSharedStagingWorkloads(**runtime, phase=phase, observe_ready=self._ready))
                return workloads.advance_shared_staging_workloads(**common, phase=phase, api=api)
            if phase == 'forward-roles':
                role_api = children.enter_context(roles.HTTPSSharedStagingRoles(**connection, qualify_runtime=partial(self._bound_qualify, phase)))
                return roles.advance_shared_staging_roles(**common, api=role_api)
            if phase == 'forward-database':
                return database.advance_shared_staging_database(**common, api=self.guards, qualify=self._qualify_current)
            if phase == 'forward-material':
                material_api = children.enter_context(material.HTTPSSharedStagingMaterial(**runtime))
                return material.advance_shared_staging_material(**common, api=material_api)
            if phase == 'forward-build-policy':
                build_api = children.enter_context(build.HTTPSSharedBuildActivation(**connection, qualify_runtime=partial(self._bound_qualify, phase)))
                return build.advance_shared_build_activation(**common, api=build_api)
            if phase == 'forward-gateway':
                gateway_api = children.enter_context(gateway.HTTPSSharedGateway(**runtime, observe_ready=self._ready))
                return gateway.advance_shared_gateway(**common, api=gateway_api)
            if phase == 'forward-activate':
                active = SharedPoolActivationTransport(bindings=bound, development=self, qualify=self._qualify_current)
                return activation.advance_shared_pool_activation(**common, api=active)
            if phase == 'forward-verify':
                return self._advance_forward_verification(state)
            if phase == 'recovery-fence':
                fenced = SharedPoolRecoveryFenceTransport(bindings=bound, kubeconfig=self.guards.kubeconfig,
                    executable=Path(self.prefix[0]), qualify=self._qualify_current)
                return fence.advance_shared_recovery_fence(**common, api=fenced)
            if phase == 'recovery-drain':
                return recovery_drain.advance_shared_recovery_drain(**common, guards=self.guards,
                    history=self._history_api(), qualify=self._qualify_current)
            if phase in {'recovery-stop', 'recovery-revoke'}:
                stopped = children.enter_context(recovery_workloads.HTTPSSharedRecoveryWorkloads(**runtime,
                    recovery_drained=self._recovery_drained, observe_ready=self._ready))
                if phase == 'recovery-stop':
                    return recovery_workloads.advance_shared_recovery_workloads(**common, api=stopped)
                machine_api = machines.SharedRecoveryMachineTransport(bindings=bound, workloads=stopped,
                    guards=self.guards, history=self._history_api())
                if not machines.advance_shared_recovery_machines(**common, api=machine_api):
                    return False
                retired = children.enter_context(recovery_gateway.HTTPSSharedRecoveryGateway(**connection,
                    private_files=self.private_files, machines=machine_api))
                return recovery_gateway.advance_shared_recovery_gateway(**common, api=retired)
            if phase == 'recovery-restore-workloads':
                restored = children.enter_context(templates.HTTPSSharedRecoveryTemplates(**runtime, recovery_drained=self._recovery_drained))
                return templates.advance_shared_recovery_templates(**common, api=restored)
            retired = children.enter_context(recovery_gateway.HTTPSSharedRecoveryGateway(**connection,
                private_files=self.private_files, machines=None))
            template_api = children.enter_context(templates.HTTPSSharedRecoveryTemplates(**runtime,
                recovery_drained=self._recovery_drained)) if phase == 'recovery-restore-roles' else None
            restored_roles = children.enter_context(recovery_roles.HTTPSSharedRecoveryRoles(**runtime, templates=template_api, gateway=retired))
            if phase == 'recovery-restore-roles':
                return recovery_roles.advance_shared_recovery_roles(**common, api=restored_roles)
            restarted = children.enter_context(restart.HTTPSSharedRecoveryRestart(**runtime, roles=restored_roles,
                gateway=retired, recovery_drained=self._recovery_drained,
                observe_ready=SharedRecoveryRuntimeReadiness(bindings=bound, guards=self.guards)))
            if phase == 'recovery-restart':
                return restart.advance_shared_recovery_restart(**common, api=restarted)
            release = reopening.SharedRecoveryReopeningTransport(bindings=bound, restart=restarted, guards=self.guards,
                history=self._history_api(), qualify=self._qualify_current)
            if phase == 'recovery-reopen':
                return reopening.advance_shared_recovery_reopening(**common, api=release)
            if phase == 'recovery-verify':
                return reopening.advance_shared_recovery_verification(**common, api=release)
        raise ValueError('unsupported shared parent phase')
