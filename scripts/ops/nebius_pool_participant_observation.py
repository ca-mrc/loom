"""Receipt-bound live resources for normal releases, not full runtime authority.

This read-only observer follows fixed create/CAS journals, including failed-Job
generations. The protected caller must ALSO qualify publication, predecessor
history, processes, databases, provider and guard.
No resource receipt or successful read substitutes for those independent checks.
"""
from __future__ import annotations

import copy
import hashlib
import json
import ssl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_runtime_transition import validate_runtime_transition
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_stage import (
    _qualified_defaulted,
    _validate_record,
)
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_adapter import RetainedParticipantPoolAdapter
from scripts.ops.nebius_pool_participant_catalog import ParticipantCatalogRequest, _projection
from scripts.ops.nebius_pool_participant_creation import _attempt, _creation
from scripts.ops.nebius_pool_participant_job import (
    PoolParticipantJobRequest,
    _parts,
    validate_participant_refresh_report,
)
from scripts.ops.nebius_pool_participant_operation import (
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import (
    _CANDIDATE_FILES,
    _JOB_FILES,
    _PRESERVED_KINDS,
    _inputs,
    _qualified_candidate,
    _resource_path,
)
from scripts.ops.nebius_pool_startup_database import (
    ParticipantRegistryState,
    pool_participant_authority_sql,
    qualify_participant_authority_report,
)
from scripts.ops.nebius_shared_pool_operation import _files

from loom.nebius_platform_render import digest
from loom_execution_capacity_collector.contracts import ProviderCapacitySnapshot
from loom_service.pool_management.participant_refresh import RefreshAction

if TYPE_CHECKING:
    import httpx
    from scripts.ops.nebius_development_runtime_retained import CompletedDevelopmentRuntime
    from scripts.ops.nebius_pool_migration import PoolMigrationRequest
    from scripts.ops.nebius_shared_pool_handoff_live import (
        SharedPoolGuardBindings,
        SharedPoolWriterObservation,
    )

    from loom_service.environment_management.candidates import GitHubCandidateCatalog

Document = dict[str, Any]


@dataclass(frozen=True)
class _Choice:
    document: Document
    uid: str | None
    requested: Document | None = None


class _ResourceView:
    """Derive only source-bound alternatives; never write or acknowledge intent."""

    def __init__(self, retained: RetainedParticipantRollout, preparing: str | None):
        self.retained = retained
        self.files: dict[Path, bytes | None] = {}
        self.documents: dict[str, Document] = {}
        self.options: dict[str, tuple[_Choice | None, ...]] = {}
        self.process_transitions: set[str] = set()
        for row in (*retained.inputs.current, retained.inputs.manager, retained.inputs.catalog):
            key = _key(row)
            if key in self.documents:
                raise ValueError
            self.documents[key] = row
            choice = _Choice(row, _uid(row))
            self.options[key] = (None, choice) if row['kind'] == 'Job' else (choice,)
        self._candidate(preparing)
        self._pool()
        roots = ('candidate-file-writes', 'candidate-resources', 'candidate-creations', 'pool-refresh')
        actual = {retained.state_dir / root / name: checksum for root in roots
            if (retained.state_dir / root).exists() or (retained.state_dir / root).is_symlink()
            for name, checksum in _files(retained.state_dir / root, allow_empty=True).items()}
        if actual != {path: hashlib.sha256(raw).hexdigest() for path, raw in self.files.items()
                if raw is not None and path.is_relative_to(retained.state_dir)}:
            raise ValueError

    def _read(self, path: Path, *, required: bool = False) -> Document | None:
        if path != path.resolve():
            raise ValueError
        raw = private_state._private_read(path, limit=4 * 1024**2) if path.exists() or path.is_symlink() else None
        if (required and raw is None) or (path in self.files and self.files[path] != raw):
            raise ValueError
        self.files[path] = raw
        if raw is None:
            return None
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError
        return value

    def _parent(self, root: str, suffix: str, schema: str, fields: set[str]) -> Document | None:
        retained = self.retained
        identity = {'schema': schema, 'reference': retained.reference.model_dump(mode='json')}
        parent = self._read(retained.state_dir / root / 'dispatch.json')
        anchor = self._read(retained.anchor_dir / (str(retained.reference.operation_id) + suffix))
        if parent is None:
            if anchor is not None:
                raise ValueError
            return None
        if (set(parent) != {*identity, *fields} or any(parent[key] != value for key, value in identity.items())
                or (anchor is not None and anchor != identity) or (anchor is None and any(parent[key] for key in fields))):
            raise ValueError
        return parent

    def _transition(self, path: Path, *, original: Document, target: Document, identity: str,
            phase: str, candidate: bool, required: bool) -> Document | None:
        key = _key(original)
        record = self._read(path, required=required)
        if record is None:
            return None
        validate_runtime_transition(record, originals={key: original}, targets={key: target},
            input_digest=identity, phase=phase,
            qualify_defaulted=_qualified_candidate if candidate else _qualified_defaulted)
        item: Document = record['resources'][key]
        before = _Choice(original, _uid(original))
        after = None if item['expected'] is None else _Choice(item['expected'], _uid(original))
        self.documents[key] = original
        self.options[key] = ((before,) if item['status'] == 'prepared' else
            (before, after) if item['status'] == 'intent' else (after,))
        if (original['kind'] == 'Deployment' and item['status'] in {'intent', 'applied'} and after is not None
                and original['spec']['template'] != after.document['spec']['template']):
            self.process_transitions.add(key)
        return item

    def _stage(self, path: Path, *, document: Document, revision: str, phase: str,
            dispatched: bool, required: bool = False) -> Document | None:
        key = _key(document)
        self.documents[key] = document
        self.options[key] = (None,)
        record = self._read(path, required=required)
        if record is None:
            return None
        binding = self.retained.release.migration.registration.binding
        _validate_record(record, {'schema': 'loom.nebius-management-stage.v1', 'binding': asdict(binding),
            'revision': revision, 'phase': phase}, {key: document})
        item: Document = record['resources'][key]
        if item['status'] == 'created':
            if not dispatched:
                raise ValueError
            observed = copy.deepcopy(item['observed'])
            observed['metadata']['uid'] = item['uid']
            _uid(observed)
            if _qualified_defaulted(item['desired'], observed) != item['expected']:
                raise ValueError
            self.options[key] = (_Choice(observed, item['uid']),)
        elif item['status'] == 'create_intent' and dispatched:
            self.options[key] = (None, _Choice(item['expected'], None, requested=item['desired']))
        return item

    def _candidate(self, preparing: str | None) -> None:
        retained = self.retained
        parent = self._parent('candidate-file-writes', '-candidate-file-writes.json',
            'loom.nebius-participant-file-writes.v1', {'started'})
        children = {_key(row): row for filename in _CANDIDATE_FILES | _JOB_FILES for row in retained.release.files[filename]}
        children['pre-upgrade-backup'] = _creation(retained, 'pre-upgrade-backup')[0]
        originals = {_key(row): row for row in retained.inputs.current}
        if preparing is not None and preparing not in children and preparing not in originals:
            raise ValueError
        started = [] if parent is None else parent['started']
        if (not isinstance(started, list) or any(not isinstance(key, str) or key not in children for key in started)
                or len(set(started)) != len(started)):
            raise ValueError
        binding = retained.release.migration.registration.binding
        for key, document in children.items():
            if key in originals and document['kind'] in _PRESERVED_KINDS:
                if key in started:
                    raise ValueError
                _inputs(retained, key)
                continue
            preparing_child = bool(started) and key == preparing == started[-1]
            required = key in started and not preparing_child
            folder = hashlib.sha256(key.encode()).hexdigest()
            if key in originals:
                original, target, identity = _inputs(retained, key)
                item = self._transition(retained.state_dir / 'candidate-resources' / folder / 'transition.json',
                    original=original, target=target, identity=identity, phase='candidate', candidate=True, required=required)
            else:
                document, identity, directory, retries = _attempt(retained, key)
                root = retained.state_dir / 'candidate-creations' / folder
                self._read(root / 'retries.json')
                if retries is not None:
                    for index in range(len(retries['attempts'])):
                        old = root if index == 0 else root / ('attempt-' + str(index))
                        self._read(old / 'stage.json', required=True)
                    last = retries['attempts'][-1]
                    required = (last['next_started'] and not preparing_child) if last['status'] == 'retired' else True
                item = self._stage(directory / 'stage.json', document=document,
                    revision=digest({'creation': identity, 'binding': asdict(binding)}), phase='participant-creation',
                    dispatched=key in started, required=required)
                if retries is not None and retries['attempts'][-1]['status'] == 'delete_intent':
                    self.options[_key(document)] = (None, *self.options[_key(document)])
            if item is not None and key not in started:
                raise ValueError

    def _pool(self) -> None:
        retained = self.retained
        root = retained.state_dir / 'pool-refresh'
        parent = self._parent('pool-refresh', '-pool-effects.json',
            'loom.nebius-participant-pool-effects.v1', {'steps', 'intents'})
        steps, intents = ({}, []) if parent is None else (parent['steps'], parent['intents'])
        if (not isinstance(steps, dict) or not isinstance(intents, list)
                or any(not isinstance(path, str) for path in intents) or len(set(intents)) != len(intents)):
            raise ValueError
        allowed: set[str] = set()
        dispatched: set[str] = set()
        for name, step in steps.items():
            if (set(step) != {'status', 'result'} or step['status'] not in {'started', 'complete'}
                    or (step['status'] == 'started' and step['result'] is not None)
                    or (step['status'] == 'complete' and not isinstance(step['result'], dict))):
                raise ValueError
            RetainedParticipantPoolAdapter._prerequisites({'steps': steps}, name)
        actions: list[tuple[str, RefreshAction, UUID | None]] = [
            (name, cast(RefreshAction, name), None) for name in ('fence', 'stage', 'open')]
        for name in steps:
            if name.startswith('capacity:'):
                observation_id = UUID(name.removeprefix('capacity:'))
                if not observation_id.int or name != 'capacity:' + str(observation_id):
                    raise ValueError
                actions.append((name, 'capacity', observation_id))
        binding = retained.release.migration.registration.binding
        for name, action, observation in actions:
            request = PoolParticipantJobRequest(retained.release, retained.inputs.manager, action, observation)
            if name in steps and steps[name]['status'] == 'complete':
                result = steps[name]['result']
                if set(result) != {'job_uid', 'pod_uid', 'refresh'}:
                    raise ValueError
                for field in ('job_uid', 'pod_uid'):
                    if str(UUID(result[field])) != result[field] or not UUID(result[field]).int:
                        raise ValueError
                validate_participant_refresh_report(request, result['refresh'])
            for document, phase, directory in _parts(request, root / 'commands'):
                path = directory / 'stage.json'
                relative = str(path.relative_to(root))
                if name in steps:
                    allowed.add(relative)
                item = self._stage(path, document=document,
                    revision=digest({'documents': {_key(document): document}, 'binding': asdict(binding)}),
                    phase=phase, dispatched=relative in intents, required=relative in intents)
                if document['kind'] == 'Job' and relative in intents:
                    if item is None or item['status'] not in {'create_intent', 'created'}:
                        raise ValueError
                    dispatched.add(name)
                if item is not None and name not in steps and document['kind'] != 'ConfigMap':
                    raise ValueError
                if name in steps and steps[name]['status'] == 'complete':
                    if item is None or item['status'] != 'created':
                        raise ValueError
                    if document['kind'] == 'Job' and steps[name]['result'].get('job_uid') != item['uid']:
                        raise ValueError
        self.registry_states: frozenset[ParticipantRegistryState] = frozenset({'ready'})
        transitions: tuple[tuple[str, ParticipantRegistryState, ParticipantRegistryState], ...] = (
            ('fence', 'ready', 'fenced'), ('stage', 'fenced', 'staged'), ('open', 'staged', 'complete'))
        for name, before, after in transitions:
            if name not in steps:
                continue
            if self.registry_states != frozenset({before}):
                raise ValueError
            step = steps[name]
            if step['status'] == 'complete':
                self.registry_states = frozenset({before if step['result']['refresh']['status'] == 'pending_work' else after})
            elif name in dispatched:
                # Only dispatch of this action's Job permits an uncertain SQL
                # transition; staging its shared input ConfigMap does not.
                self.registry_states = frozenset({before, after})
        for prerequisite, consumers, expected in (
                ('stage', {'catalog', 'manager', 'open'}, 'staged'),
                ('open', {name for name in steps if name.startswith('capacity:')}, 'complete')):
            if consumers & steps.keys() and steps[prerequisite]['result']['refresh']['status'] != expected:
                raise ValueError
        request_catalog = ParticipantCatalogRequest(retained.release, retained.inputs.manager, retained.inputs.catalog)
        catalog, manager, identity = _projection(request_catalog)
        path = root / 'catalog/catalog/stage.json'
        relative = str(path.relative_to(root))
        if 'catalog' in steps:
            allowed.add(relative)
        item = self._stage(path, document=catalog,
            revision=digest({'documents': {_key(catalog): catalog}, 'binding': asdict(binding)}),
            phase='participant-catalog', dispatched=relative in intents, required=relative in intents)
        if item is not None and 'catalog' not in steps:
            raise ValueError
        if steps.get('catalog', {}).get('status') == 'complete' and (item is None or item['status'] != 'created'):
            raise ValueError
        path = root / 'catalog/manager/transition.json'
        relative = str(path.relative_to(root))
        if 'manager' in steps:
            allowed.add(relative)
        item = self._transition(path, original=retained.inputs.manager, target=manager,
            identity=identity, phase='control', candidate=False, required=relative in intents)
        if item is not None and ('manager' not in steps or (item['status'] == 'applied' and relative not in intents)):
            raise ValueError
        if steps.get('manager', {}).get('status') == 'complete' and (
                item is None or item['status'] != 'applied' or steps['manager']['result'] != {'status': 'ready'}):
            raise ValueError
        if not set(intents) <= allowed:
            raise ValueError


class HTTPSParticipantResourceObserver(ManagementKubernetesTransport):
    """Exact source/receipt-selected resources over authenticated, read-only HTTPS."""

    def __init__(self, *, retained: RetainedParticipantRollout, api_server: str,
            ssl_context: ssl.SSLContext, token: str | None = None):
        self.retained = copy.deepcopy(retained)
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)
        self._source()

    def _source(self) -> None:
        retained = self.retained
        if (self.api_server.rstrip('/') != retained.inputs.config['kubernetes_api_server'].rstrip('/')
                or load_participant_rollout(reference=retained.reference, state_root=retained.state_dir.parent,
                    anchor_dir=retained.anchor_dir) != retained):
            raise ValueError('participant observation source differs; preserve evidence')

    def _cluster(self) -> None:
        row = self._request('GET', '/api/v1/namespaces/kube-system')
        if (row is None or row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace'
                or row['metadata'].get('name') != 'kube-system'
                or _uid(row) != self.retained.release.migration.registration.binding.kube_system_uid):
            raise ValueError

    def observe(self, *, preparing: str | None = None) -> dict[str, Document]:
        """No default candidate permission: all alternatives require child history.

        Only a fixed candidate API factory may name its currently preparing key.
        This permits that child's first pre-dispatch check before its journal
        exists, not recovery without recorded mutation evidence.
        """
        try:
            self._source()
            view = _ResourceView(self.retained, preparing)
            self._cluster()
            result = {}
            for key, document in view.documents.items():
                actual = self._request('GET', _resource_path(document))
                choices = view.options[key]
                if actual is None:
                    if None not in choices:
                        raise ValueError
                    continue
                uid = _uid(actual)
                if not any(choice is not None and (choice.uid is None or choice.uid == uid)
                        and (_qualified_defaulted(choice.requested, actual) == choice.document if choice.requested is not None else
                            _matches(actual, choice.document, uid)) for choice in choices):
                    raise ValueError
                result[key] = actual
            for original in result.values():
                current = self._request('GET', _resource_path(original))
                if current is None or _uid(current) != _uid(original) or _stable(current) != _stable(original):
                    raise ValueError
            for key in view.documents.keys() - result.keys():
                if self._request('GET', _resource_path(view.documents[key])) is not None:
                    raise ValueError
            self._cluster()
            self._source()
            final = _ResourceView(self.retained, preparing)
            if final.files != view.files or final.options != view.options:
                raise ValueError
            return result
        except Exception:
            raise ValueError('participant live resources unqualified; preserve evidence') from None

    @asynccontextmanager
    async def _publication_catalog(self, *, completed: CompletedDevelopmentRuntime,
            http: httpx.AsyncClient) -> AsyncIterator[GitHubCandidateCatalog]:
        """Bind every catalog read to the original installed reader and bundle."""
        from scripts.ops.nebius_pool_participant_prepare import resolve_participant_publication

        from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

        retained = self.retained
        async with resolve_participant_publication(completed=completed, publication=retained.inputs.publication,
                keyring=retained.release.refresh.before.profiles.image_admission_keyring,
                transport=self, http=http) as (catalog, bundle):
            if (bundle.candidate != retained.inputs.candidate
                    or ServiceExecutionRuntimeProfileV1.model_validate(bundle.profile) != retained.inputs.runtime_profile):
                raise ValueError
            yield catalog

    async def candidate_follows(self, *, bindings: SharedPoolGuardBindings,
            current_sha: str, http: httpx.AsyncClient) -> bool:
        """Read ancestry with retained publication authority, never local Git."""
        from scripts.ops.nebius_development_runtime_retained import load_completed_runtime

        try:
            bound, retained = copy.deepcopy(bindings), copy.deepcopy(self.retained)
            connection = (self.api_server, self.client, str(self.client.base_url), dict(self.client.headers))
            self._source()
            self._cluster()
            completed = bound.plan.development.runtime
            if load_completed_runtime(completed.request) != completed:
                raise ValueError
            async with self._publication_catalog(completed=completed, http=http) as catalog:
                result = await catalog.candidate_follows(
                    candidate_id=retained.inputs.publication.candidate_id, current_sha=current_sha)
            self._cluster()
            if (bindings != bound or self.retained != retained
                    or (self.api_server, self.client, str(self.client.base_url), dict(self.client.headers)) != connection
                    or any(private_state._private_read(path, limit=4 * 1024**2) != raw
                        for path, raw in completed.files.items())):
                raise ValueError
            self._source()
            return result
        except Exception:
            raise ValueError('participant source ancestry unqualified; preserve evidence') from None

    async def observe_external(self, *, bindings: SharedPoolGuardBindings, shared_state: Path,
            operator_credentials: Path, http: httpx.AsyncClient, preparing: str | None = None
            ) -> ProviderCapacitySnapshot:
        """Qualify this release's publication and the unchanged physical pool.

        Normal releases replace a participant and the manager catalog, not the
        collector/registry identity. Observe those immutable inputs directly;
        replaying the initial installation observer would reject valid releases.
        A busy, empty or scaling pool is valid; this does not publish capacity or
        authorize opening admission.
        """
        from scripts.ops.nebius_development_collector_runtime import prepare_collector_runtime
        from scripts.ops.nebius_development_pool_registration import development_profile_catalog
        from scripts.ops.nebius_development_runtime_external import qualify_runtime_cloud
        from scripts.ops.nebius_development_runtime_retained import load_completed_runtime

        from loom_control_plane.execution_placement import quota_identity

        try:
            bound, retained = copy.deepcopy(bindings), self.retained
            operator = private_state._private_read(operator_credentials, limit=1024**2)
            connection = (self.api_server, self.client, str(self.client.base_url), dict(self.client.headers))
            current = self.observe_writers(bindings=bound, shared_state=shared_state, preparing=preparing)
            view = _ResourceView(retained, preparing)
            completed = bound.plan.development.runtime
            request = completed.request
            if load_completed_runtime(request) != completed:
                raise ValueError
            collector = prepare_collector_runtime(request.database)
            config = collector.configuration
            keys = {_key(config), f"Secret:{config['metadata']['namespace']}:{collector.credential_secret_name}"}
            profiles = development_profile_catalog(request.database.manager.retained.request)
            keys.update(f'Secret:{row.settings.namespace}:{row.settings.registry_secret_name}'
                for row in profiles.task_images)
            keys.update(f'Secret:{row.settings.namespace}:{row.settings.registry_secret_name}'
                for row in profiles.application_images)
            if not keys <= completed.plan.fixed['material'].keys():
                raise ValueError
            expected = {key: completed.resources[key] for key in keys}

            def material() -> None:
                for row in expected.values():
                    actual = self._request('GET', _resource_path(row))
                    if actual is None or not _matches(actual, row, _uid(row)):
                        raise ValueError

            def unchanged() -> None:
                if (bindings != bound or self.retained != retained
                        or (self.api_server, self.client, str(self.client.base_url), dict(self.client.headers)) != connection
                        or private_state._private_read(operator_credentials, limit=1024**2) != operator
                        or _ResourceView(retained, preparing).files != view.files
                        or any(private_state._private_read(path, limit=4 * 1024**2) != raw
                            for path, raw in completed.files.items())):
                    raise ValueError

            unchanged()
            material()
            spec = retained.release.refresh.before
            async with self._publication_catalog(completed=completed, http=http):
                unchanged()
                snapshot = await qualify_runtime_cloud(request=request, operator_credentials=operator_credentials)
                if (snapshot.node_group is None or snapshot.node_group.id != spec.node_group_id
                        or {key: quota_identity(value) for key, value in snapshot.quota_resources.items()} != spec.quota_identities
                        or snapshot.provider_capacity_state != 'available'
                        or snapshot.autoscaler_state not in {'ready', 'scaling'}):
                    raise ValueError
                final = self.observe_writers(bindings=bound, shared_state=shared_state, preparing=preparing)
                for before, after in ((current.workloads, final.workloads), (current.authority, final.authority)):
                    if before.keys() != after.keys() or any(not _matches(after[key], row, _uid(row))
                            for key, row in before.items()):
                        raise ValueError
                material()
            unchanged()
            self._source()
            return snapshot
        except Exception:
            raise ValueError('participant external authority unqualified; preserve evidence') from None

    def observe_guard(self, *, kubeconfig: Path, executable: Path,
            phase: Literal['preflight', 'guarded', 'ready', 'released'], preparing: str | None = None) -> str:
        """Read this release's idle guard through its currently qualified database.

        Original credential/namespace/database identities remain pinned. Only
        source-and-journal-qualified database resources may replace their prior
        manifests. This does not acquire/release intake, attest application
        readiness or authorize treating an open guard as completed history.
        """
        from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
        from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
        from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
        from scripts.ops.nebius_pool_participant_operation import _paths

        try:
            allowed = {'preflight': {'open', 'held'}, 'guarded': {'held'}, 'ready': {'held'}, 'released': {'open'}}[phase]
            retained = self.retained
            current = self.observe(preparing=preparing)
            receipts = _ResourceView(retained, preparing).files
            _, marker = _paths(retained.reference, retained.state_dir.parent, retained.anchor_dir)
            anchor = private_state._private_read(marker)
            original, = (row for row in retained.inputs.previous.guards
                if row.participant_id == retained.release.refresh.participant_id)
            database = original.database
            if database is None:
                raise ValueError
            statefulset, service = (current[_key(row)] for row in (database.statefulset, database.service))
            if (_uid(statefulset) != _uid(database.statefulset) or _uid(service) != _uid(database.service)):
                raise ValueError
            target = replace(original, database=replace(database, statefulset=statefulset, service=service))
            request = replace(retained.inputs.previous,
                guards=tuple(target if row == original else row for row in retained.inputs.previous.guards))
            guards = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=executable)
            prefix = tuple(guards.prefix)
            query = rollout_guard_observation_sql(owner=retained.guard_owner, candidate=retained.inputs.candidate['candidate_sha'])

            def unchanged() -> None:
                if (self.retained != retained or guards.request != request or tuple(guards.prefix) != prefix
                        or hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest() != guards.kubeconfig_sha256
                        or hashlib.sha256(private_state._private_read(retained.state_dir / 'inputs.json', limit=16 * 1024**2)).hexdigest()
                            != retained.reference.inputs_sha256
                        or private_state._private_read(marker) != anchor
                        or _ResourceView(retained, preparing).files != receipts):
                    raise ValueError
                # Full semantic source loading brackets this read-only scope.
                # Nested SQL challenges recheck its exact bytes, not rebuild
                # the unchanged release and every ancestor for each read.

            def read() -> tuple[str, tuple[Any, ...]]:
                unchanged()
                before = HTTPSDevelopmentRuntimeProbes._process_identity(guards._database(target))
                report = guards._run(['exec', '-n', target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--',
                    'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c', query])
                if (set(report) != {'status'} or report['status'] not in allowed
                        or HTTPSDevelopmentRuntimeProbes._process_identity(guards._database(target)) != before):
                    raise ValueError
                unchanged()
                return cast(str, report['status']), before

            result = read()
            after = self.observe(preparing=preparing)
            if (after.keys() != current.keys()
                    or any(not _matches(after[key], row, _uid(row)) for key, row in current.items())
                    or read() != result):
                raise ValueError
            unchanged()
            self._source()
            return result[0]
        except Exception:
            raise ValueError('participant guard observation unqualified; preserve evidence') from None

    def observe_participant_processes(self, *, kubeconfig: Path, executable: Path,
            phase: Literal['preflight', 'guarded', 'ready', 'released'], preparing: str | None = None) -> tuple[str, ...]:
        """Challenge this participant's CP, API and every registered actuator.

        Known candidate transitions can return pending keys before final readiness;
        a pending result is NOT permission for more writes. The protected caller
        must wait/read again. Manager, gateway, other participants, guard and live
        registry qualification remain independent whole-pool requirements.
        """
        from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
        from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
        from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
        from scripts.ops.nebius_pool_participant_operation import _paths

        try:
            if phase not in {'preflight', 'guarded', 'ready', 'released'}:
                raise ValueError
            retained = self.retained
            current = self.observe(preparing=preparing)
            view = _ResourceView(retained, preparing)
            original, = (row for row in retained.inputs.previous.guards
                if row.participant_id == retained.release.refresh.participant_id)
            database = original.database
            if (database is None or database.actuator_credential_uid is None
                    or database.actuator_credential_resource_version is None):
                raise ValueError
            keys = {f'Deployment:{original.namespace}:{name}' for name in ('loom-control-plane', 'loom-service')}
            keys.update(_key(row) for row in retained.release.files['60-execution.yaml'] if row['kind'] == 'Deployment')
            originals = {_key(row): row for row in retained.inputs.current if _key(row) in keys}
            if originals.keys() != keys or not keys <= current.keys():
                raise ValueError
            controller = originals[_key(original.controller)]
            statefulset, service = (current[_key(row)] for row in (database.statefulset, database.service))
            if (_uid(controller) != _uid(original.controller) or _uid(statefulset) != _uid(database.statefulset)
                    or _uid(service) != _uid(database.service)):
                raise ValueError
            target = replace(original, controller=controller,
                database=replace(database, statefulset=statefulset, service=service))
            previous = replace(retained.inputs.previous,
                guards=tuple(target if row == original else row for row in retained.inputs.previous.guards))
            successor = replace(previous, registration=replace(previous.registration, spec=retained.release.refresh.after))
            before_api, after_api = (KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=executable)
                for request in (previous, successor))
            prefix = tuple(before_api.prefix)
            _, marker = _paths(retained.reference, retained.state_dir.parent, retained.anchor_dir)
            anchor = private_state._private_read(marker)
            region = retained.release.refresh.before.quota_identities['nodes'][1]

            def unchanged() -> None:
                if (self.retained != retained or tuple(before_api.prefix) != prefix or tuple(after_api.prefix) != prefix
                        or hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest() != before_api.kubeconfig_sha256
                        or hashlib.sha256(private_state._private_read(retained.state_dir / 'inputs.json', limit=16 * 1024**2)).hexdigest()
                            != retained.reference.inputs_sha256
                        or private_state._private_read(marker) != anchor
                        or _ResourceView(retained, preparing).files != view.files):
                    raise ValueError

            def ready(row: Document) -> bool:
                namespace = row['metadata']['namespace']
                actual = before_api._get('deployment', row['metadata']['name'], namespace)
                if not _matches(actual, row, _uid(row)):
                    raise ValueError
                children = before_api._run(['get', '--raw', f'/apis/apps/v1/namespaces/{namespace}/replicasets?limit=1000'])
                pods = before_api._run(['get', '--raw', f'/api/v1/namespaces/{namespace}/pods?limit=1000'])
                return qualify_started_deployment(current=actual, children=children, pods=pods, region=region)

            credentials: dict[str, tuple[str, UUID, str]] = {}
            for key, row in originals.items():
                actuator = row['metadata']['namespace'] != target.namespace
                variable = ('LOOM_EXECUTION_ACTUATOR_DB_URL' if actuator else 'LOOM_CP_DB_URL'
                    if row['metadata']['name'] == 'loom-control-plane' else 'LOOM_SVC_DB_URL')
                credentials[key] = (variable,
                    database.actuator_credential_uid if actuator else database.credential_uid,
                    database.actuator_credential_resource_version if actuator else database.credential_resource_version)

            def urls() -> dict[str, str]:
                return {key: before_api._workload_database_url(originals[key], url_variable=variable,
                    credential_uid=uid, credential_resource_version=version)
                    for key, (variable, uid, version) in credentials.items()}

            unchanged()
            before_urls = urls()
            identity = HTTPSDevelopmentRuntimeProbes._process_identity
            backend = identity(before_api._database(target))
            processes: dict[str, tuple[KubectlPoolGuardAPI, tuple[Any, ...]]] = {}
            pending = []
            for key in sorted(keys):
                row, prior = current[key], originals[key]
                updated = not _matches(row, prior, _uid(prior))
                if not ready(row):
                    if phase in {'ready', 'released'} or key not in view.process_transitions or not updated:
                        raise ValueError
                    pending.append(key)
                    continue
                api = after_api if updated else before_api
                process = identity(api._runtime(target, original=prior, expected=row))
                _, uid, version = credentials[key]
                api.qualify_runtime_database(target, original=prior, expected=row,
                    credential_uid=uid, credential_resource_version=version)
                api.qualify_runtime_pool_settings(target, original=prior, expected=row)
                if identity(api._runtime(target, original=prior, expected=row)) != process:
                    raise ValueError
                processes[key] = api, process
                unchanged()
            final = self.observe(preparing=preparing)
            if (final.keys() != current.keys()
                    or any(not _matches(final[key], row, _uid(row)) for key, row in current.items())):
                raise ValueError
            for key, (api, process) in processes.items():
                if not ready(final[key]) or identity(api._runtime(target, original=originals[key], expected=final[key])) != process:
                    raise ValueError
            if identity(before_api._database(target)) != backend or urls() != before_urls:
                raise ValueError
            unchanged()
            self._source()
            return tuple(pending)
        except Exception:
            raise ValueError('participant process observation unqualified; preserve evidence') from None

    def observe_peer_processes(self, *, bindings: SharedPoolGuardBindings, shared_state: Path,
            kubeconfig: Path, executable: Path, preparing: str | None = None) -> Document:
        """Keep peers running and sample every actuator without draining peers.

        The selected participant's separate process check must finish any known
        transition first. This check has no pending exemption: peers cannot be
        upgraded by this release, and telemetry must run in ready actuators.
        The returned availability report is diagnostic, never capacity authority.
        """
        from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
        from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
        from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
        from scripts.ops.nebius_pool_participant_operation import _paths

        try:
            bound, retained = copy.deepcopy(bindings), self.retained
            current = self.observe_writers(bindings=bound, shared_state=shared_state, preparing=preparing)
            view = _ResourceView(retained, preparing)
            original_selected = {_key(row): row for row in retained.inputs.current}
            targets, originals, owners, peers, actuators = [], {}, {}, set(), set()
            credentials: dict[str, tuple[str, UUID, str]] = {}
            for original in retained.inputs.previous.guards:
                participant, = (row for row in retained.release.refresh.before.participants
                    if row.participant_id == original.participant_id)
                peer = original.participant_id != retained.release.refresh.participant_id
                database = original.database
                if (database is None or database.actuator_credential_uid is None
                        or database.actuator_credential_resource_version is None):
                    raise ValueError
                execution = {key: row for key, row in current.workloads.items() if row['kind'] == 'Deployment'
                    and row['metadata']['namespace'] == participant.execution_namespace.name}
                target_ids = []
                for row in execution.values():
                    container, = row['spec']['template']['spec']['containers']
                    target_id, = (item['value'] for item in container['env']
                        if item['name'] == 'LOOM_EXECUTION_ACTUATOR_TARGET_ID')
                    participant.target(target_id, 'trial')
                    target_ids.append(target_id)
                if (len(set(target_ids)) != len(target_ids)
                        or set(target_ids) != {row.target_id for row in participant.targets if 'trial' in row.workload_kinds}):
                    raise ValueError
                keys = set(execution)
                actuators.update(keys)
                if peer:
                    keys.update(f'Deployment:{original.namespace}:{name}' for name in ('loom-control-plane', 'loom-service'))
                    peers.update(keys)
                selected = current.workloads if peer else original_selected
                controller = selected[_key(original.controller)]
                if _uid(controller) != _uid(original.controller):
                    raise ValueError
                statefulset = current.workloads[_key(database.statefulset)]
                service = self._request('GET', _resource_path(database.service))
                if (service is None or _uid(statefulset) != _uid(database.statefulset)
                        or (peer and not _matches(service, database.service, _uid(database.service)))
                        or _uid(service) != _uid(database.service)):
                    raise ValueError
                target = replace(original, controller=controller,
                    database=replace(database, statefulset=statefulset, service=service))
                targets.append(target)
                for key in keys:
                    originals[key], owners[key] = selected[key], target
                    actuator = key in execution
                    variable = ('LOOM_EXECUTION_ACTUATOR_DB_URL' if actuator else 'LOOM_CP_DB_URL'
                        if originals[key]['metadata']['name'] == 'loom-control-plane' else 'LOOM_SVC_DB_URL')
                    credentials[key] = (variable,
                        database.actuator_credential_uid if actuator else database.credential_uid,
                        database.actuator_credential_resource_version if actuator else database.credential_resource_version)
            request = replace(retained.inputs.previous, guards=tuple(targets))
            api = KubectlPoolGuardAPI(request=request, kubeconfig=kubeconfig, executable=executable)
            prefix = tuple(api.prefix)
            _, marker = _paths(retained.reference, retained.state_dir.parent, retained.anchor_dir)
            anchor = private_state._private_read(marker)

            def unchanged() -> None:
                if (bindings != bound or self.retained != retained or tuple(api.prefix) != prefix
                        or hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest() != api.kubeconfig_sha256
                        or hashlib.sha256(private_state._private_read(retained.state_dir / 'inputs.json', limit=16 * 1024**2)).hexdigest()
                            != retained.reference.inputs_sha256
                        or private_state._private_read(marker) != anchor
                        or _ResourceView(retained, preparing).files != view.files):
                    raise ValueError

            def urls() -> dict[str, str]:
                return {key: api._workload_database_url(originals[key], url_variable=variable,
                    credential_uid=uid, credential_resource_version=version)
                    for key, (variable, uid, version) in credentials.items()}

            def ready(row: Document) -> None:
                namespace = row['metadata']['namespace']
                actual = api._get('deployment', row['metadata']['name'], namespace)
                if not _matches(actual, row, _uid(row)):
                    raise ValueError
                children = api._run(['get', '--raw', f'/apis/apps/v1/namespaces/{namespace}/replicasets?limit=1000'])
                pods = api._run(['get', '--raw', f'/api/v1/namespaces/{namespace}/pods?limit=1000'])
                if not qualify_started_deployment(current=actual, children=children, pods=pods,
                        region=retained.release.refresh.before.quota_identities['nodes'][1]):
                    raise ValueError

            unchanged()
            before_urls = urls()
            identity = HTTPSDevelopmentRuntimeProbes._process_identity
            backends = {target.participant_id: identity(api._database(target)) for target in targets
                if target.participant_id != retained.release.refresh.participant_id}
            processes = {}
            for key, prior in sorted(originals.items()):
                row, target = current.workloads[key], owners[key]
                ready(row)
                processes[key] = identity(api._runtime(target, original=prior, expected=row))
                if key in peers:
                    _, uid, version = credentials[key]
                    api.qualify_runtime_database(target, original=prior, expected=row,
                        credential_uid=uid, credential_resource_version=version)
                    api.qualify_runtime_pool_settings(target, original=prior, expected=row)
                if key in actuators:
                    api.qualify_runtime_telemetry(target, original=prior, expected=row)
                unchanged()
            final = self.observe_writers(bindings=bound, shared_state=shared_state, preparing=preparing)
            if (final.workloads.keys() != current.workloads.keys()
                    or any(not _matches(final.workloads[key], row, _uid(row)) for key, row in current.workloads.items())):
                raise ValueError
            for key, process in processes.items():
                ready(final.workloads[key])
                if identity(api._runtime(owners[key], original=originals[key], expected=final.workloads[key])) != process:
                    raise ValueError
            for target in targets:
                if target.participant_id in backends and identity(api._database(target)) != backends[target.participant_id]:
                    raise ValueError
            if urls() != before_urls:
                raise ValueError
            unchanged()
            self._source()
            return api.telemetry_report()
        except Exception:
            raise ValueError('participant peer processes unqualified; preserve evidence') from None

    def observe_manager_process(self, *, bindings: SharedPoolGuardBindings, kubeconfig: Path,
            executable: Path, phase: Literal['preflight', 'guarded', 'ready', 'released'],
            preparing: str | None = None) -> tuple[str, ...]:
        """Bind the current manager process to its recorded catalog and backend.

        Pending means a known catalog restart is unfinished, never permission to
        open participant admission. Whole-pool writer/registry and other process
        observations remain separate requirements of the protected parent.
        """
        from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
        from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
        from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
        from scripts.ops.nebius_pool_participant_operation import _paths
        from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target

        try:
            if phase not in {'preflight', 'guarded', 'ready', 'released'}:
                raise ValueError
            bound, retained = copy.deepcopy(bindings), self.retained
            current = self.observe(preparing=preparing)
            view = _ResourceView(retained, preparing)
            manager = bound.plan.development.runtime.request.database.manager.retained.request.retained
            if (bound.migration.registration.binding != retained.inputs.previous.registration.binding
                    or not {path: hashlib.sha256(raw).hexdigest() for path, raw in manager.files.items()}.items()
                        <= retained.inputs.history.items()):
                raise ValueError
            secret_path = '/api/v1/namespaces/' + manager.binding.namespace + '/secrets/loom-platform-db'
            credential = self._request('GET', secret_path)
            if credential is None:
                raise ValueError
            original = _gateway_history_target(bound, credential)
            key = _key(retained.inputs.manager)
            if _key(original.controller) != key or _uid(original.controller) != _uid(retained.inputs.manager):
                raise ValueError
            target = replace(original, controller=retained.inputs.manager)
            workload = current[key]
            updated = not _matches(workload, target.controller, _uid(target.controller))
            request = retained.inputs.previous
            if updated:
                request = replace(request, registration=replace(request.registration, spec=retained.release.refresh.after))
            history = KubectlPoolHistoryAPI(request=request, target=target, kubeconfig=kubeconfig, executable=executable)
            prefix = tuple(history.prefix)
            _, marker = _paths(retained.reference, retained.state_dir.parent, retained.anchor_dir)
            anchor = private_state._private_read(marker)

            def unchanged() -> None:
                if (bindings != bound or self.retained != retained or tuple(history.prefix) != prefix
                        or hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest() != history.kubeconfig_sha256
                        or hashlib.sha256(private_state._private_read(retained.state_dir / 'inputs.json', limit=16 * 1024**2)).hexdigest()
                            != retained.reference.inputs_sha256
                        or private_state._private_read(marker) != anchor
                        or _ResourceView(retained, preparing).files != view.files
                        or any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in manager.files.items())):
                    raise ValueError

            def ready() -> bool:
                actual = history._get('deployment', target.controller['metadata']['name'], target.namespace)
                if not _matches(actual, workload, _uid(target.controller)):
                    raise ValueError
                children = history._run(['get', '--raw', f'/apis/apps/v1/namespaces/{target.namespace}/replicasets?limit=1000'])
                pods = history._run(['get', '--raw', f'/api/v1/namespaces/{target.namespace}/pods?limit=1000'])
                return qualify_started_deployment(current=actual, children=children, pods=pods,
                    region=request.registration.spec.quota_identities['nodes'][1])

            unchanged()
            identity = HTTPSDevelopmentRuntimeProbes._process_identity
            backend = identity(history._database(target, url_variable='LOOM_SVC_DB_URL'))
            pending = not ready()
            process = None
            if pending:
                if phase in {'ready', 'released'} or key not in view.process_transitions or not updated:
                    raise ValueError
            else:
                process = identity(history._runtime(target, expected=workload))
                history.qualify_manager_database(expected=workload)
                history.qualify_manager_pool_settings(expected=workload)
            final = self.observe(preparing=preparing)
            if (final.keys() != current.keys()
                    or any(not _matches(final[name], row, _uid(row)) for name, row in current.items())):
                raise ValueError
            if process is not None and (not ready() or identity(history._runtime(target, expected=workload)) != process):
                raise ValueError
            credential = self._request('GET', secret_path)
            if (credential is None or _gateway_history_target(bound, credential) != original
                    or identity(history._database(target, url_variable='LOOM_SVC_DB_URL')) != backend):
                raise ValueError
            unchanged()
            self._source()
            return (key,) if pending else ()
        except Exception:
            raise ValueError('participant manager process unqualified; preserve evidence') from None

    def observe_gateway_process(self, *, bindings: SharedPoolGuardBindings, shared_state: Path,
            kubeconfig: Path, executable: Path, preparing: str | None = None) -> None:
        """Keep the shared gateway healthy through a selected participant fence.

        Current whole-pool writer authority and completed gateway history select
        the running workload. This continuous process check neither requires all
        participants active nor substitutes for registry/final capacity checks.
        """
        from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
        from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
        from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
        from scripts.ops.nebius_pool_participant_operation import _paths
        from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target

        try:
            bound, retained = copy.deepcopy(bindings), self.retained
            current = self.observe_writers(bindings=bound, shared_state=shared_state, preparing=preparing)
            view = _ResourceView(retained, preparing)
            manager = bound.plan.development.runtime.request.database.manager.retained.request.retained
            if not {path: hashlib.sha256(raw).hexdigest() for path, raw in manager.files.items()}.items() <= retained.inputs.history.items():
                raise ValueError
            secret_path = '/api/v1/namespaces/' + manager.binding.namespace + '/secrets/loom-platform-db'
            credential = self._request('GET', secret_path)
            if credential is None:
                raise ValueError
            original_target = _gateway_history_target(bound, credential)
            if (_key(original_target.controller) != _key(retained.inputs.manager)
                    or _uid(original_target.controller) != _uid(retained.inputs.manager)):
                raise ValueError
            target = replace(original_target, controller=retained.inputs.manager)
            original = bound.plan.development.gateway_original
            key = _key(original)
            workload = current.workloads[key]
            history = KubectlPoolHistoryAPI(request=retained.inputs.previous, target=target,
                kubeconfig=kubeconfig, executable=executable)
            prefix = tuple(history.prefix)
            _, marker = _paths(retained.reference, retained.state_dir.parent, retained.anchor_dir)
            anchor = private_state._private_read(marker)

            def unchanged() -> None:
                if (bindings != bound or self.retained != retained or tuple(history.prefix) != prefix
                        or hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest() != history.kubeconfig_sha256
                        or hashlib.sha256(private_state._private_read(retained.state_dir / 'inputs.json', limit=16 * 1024**2)).hexdigest()
                            != retained.reference.inputs_sha256
                        or private_state._private_read(marker) != anchor
                        or _ResourceView(retained, preparing).files != view.files
                        or any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in manager.files.items())):
                    raise ValueError

            def ready() -> None:
                actual = history._get('deployment', original['metadata']['name'], target.namespace)
                if not _matches(actual, workload, _uid(original)):
                    raise ValueError
                children = history._run(['get', '--raw', f'/apis/apps/v1/namespaces/{target.namespace}/replicasets?limit=1000'])
                pods = history._run(['get', '--raw', f'/api/v1/namespaces/{target.namespace}/pods?limit=1000'])
                if not qualify_started_deployment(current=actual, children=children, pods=pods,
                        region=retained.release.refresh.before.quota_identities['nodes'][1]):
                    raise ValueError

            unchanged()
            ready()
            identity = HTTPSDevelopmentRuntimeProbes._process_identity
            backend = identity(history._database(target, url_variable='LOOM_SVC_DB_URL'))
            process = identity(history._runtime(target, original=original, expected=workload))
            history.qualify_gateway_process(original=original, expected=workload)
            final = self.observe_writers(bindings=bound, shared_state=shared_state, preparing=preparing)
            if (final.workloads.keys() != current.workloads.keys()
                    or any(not _matches(final.workloads[name], row, _uid(row)) for name, row in current.workloads.items())):
                raise ValueError
            ready()
            credential = self._request('GET', secret_path)
            if (credential is None or _gateway_history_target(bound, credential) != original_target
                    or identity(history._runtime(target, original=original, expected=workload)) != process
                    or identity(history._database(target, url_variable='LOOM_SVC_DB_URL')) != backend):
                raise ValueError
            unchanged()
            self._source()
        except Exception:
            raise ValueError('participant gateway process unqualified; preserve evidence') from None

    def observe_registry(self, *, bindings: SharedPoolGuardBindings, kubeconfig: Path,
            executable: Path, preparing: str | None = None) -> ParticipantRegistryState:
        """Read exact management authority allowed by this release's dispatch.

        This composes with resource/writer observation; it does not repeat that
        census or turn saved Job receipts into current database authority. Only
        fixed READ ONLY SQL is executed, never a refresh Job or locking action.
        """
        from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
        from scripts.ops.nebius_pool_origin_history import KubectlPoolHistoryAPI
        from scripts.ops.nebius_pool_participant_operation import _paths
        from scripts.ops.nebius_shared_pool_forward_processes import _gateway_history_target

        try:
            self._source()
            bound, retained = copy.deepcopy(bindings), self.retained
            view = _ResourceView(retained, preparing)
            manager = bound.plan.development.runtime.request.database.manager.retained.request.retained
            if (bound.migration.registration.binding != retained.inputs.previous.registration.binding
                    or not {path: hashlib.sha256(raw).hexdigest() for path, raw in manager.files.items()}.items()
                        <= retained.inputs.history.items()):
                raise ValueError
            secret_path = '/api/v1/namespaces/' + manager.binding.namespace + '/secrets/loom-platform-db'
            credential = self._request('GET', secret_path)
            if credential is None:
                raise ValueError
            original = _gateway_history_target(bound, credential)
            if (_key(original.controller) != _key(retained.inputs.manager)
                    or _uid(original.controller) != _uid(retained.inputs.manager)):
                raise ValueError
            target = replace(original, controller=retained.inputs.manager)
            history = KubectlPoolHistoryAPI(request=retained.inputs.previous, target=target,
                kubeconfig=kubeconfig, executable=executable)
            prefix = tuple(history.prefix)
            _, marker = _paths(retained.reference, retained.state_dir.parent, retained.anchor_dir)
            anchor = private_state._private_read(marker)
            query = pool_participant_authority_sql(retained.release.refresh)

            def unchanged() -> None:
                if (bindings != bound or self.retained != retained or tuple(history.prefix) != prefix
                        or hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest() != history.kubeconfig_sha256
                        or hashlib.sha256(private_state._private_read(retained.state_dir / 'inputs.json', limit=16 * 1024**2)).hexdigest()
                            != retained.reference.inputs_sha256
                        or private_state._private_read(marker) != anchor
                        or _ResourceView(retained, preparing).files != view.files
                        or any(private_state._private_read(path, limit=4 * 1024**2) != raw for path, raw in manager.files.items())):
                    raise ValueError

            def read() -> tuple[ParticipantRegistryState, tuple[Any, ...]]:
                unchanged()
                before = history._database(target, url_variable='LOOM_SVC_DB_URL')
                process = HTTPSDevelopmentRuntimeProbes._process_identity(before)
                report = history._run(['exec', '-n', target.namespace, 'pod/' + before['metadata']['name'], '-c', 'loom-postgres', '--',
                    'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'loom', '-c', query])
                result = qualify_participant_authority_report(retained.release.refresh, report, states=view.registry_states)
                if HTTPSDevelopmentRuntimeProbes._process_identity(history._database(target, url_variable='LOOM_SVC_DB_URL')) != process:
                    raise ValueError
                unchanged()
                return result, process

            result = read()
            credential = self._request('GET', secret_path)
            if credential is None or _gateway_history_target(bound, credential) != original or read() != result:
                raise ValueError
            unchanged()
            self._source()
            return result[0]
        except Exception:
            raise ValueError('participant registry observation unqualified; preserve evidence') from None

    def observe_writers(self, *, bindings: SharedPoolGuardBindings, shared_state: Path,
            preparing: str | None = None) -> SharedPoolWriterObservation:
        """Whole-pool census with only this release's source-bound successors.

        The protected parent supplies the original shared-root bindings and must
        separately load semantic root/latest completion history. This method
        checks the root's workload/Role receipts and their frozen membership in
        the release history, not publication, SQL readiness or guard ownership.
        No live snapshot becomes persisted authority or a mutation permit.
        """
        from scripts.ops.nebius_shared_pool_forward_census import _forward_options
        from scripts.ops.nebius_shared_pool_handoff_live import _observe_shared_writer_options

        try:
            bound, retained = copy.deepcopy(bindings), self.retained
            self._source()
            baseline = _forward_options(bound, shared_state)
            if (baseline[0]['outcome'] != 'shared_pool_handoff_complete'
                    or any(row['status'] != 'complete' for row in baseline[0]['forward'].values())):
                raise ValueError
            manager = bound.plan.development.runtime.request.database.manager.retained.request.retained
            anchor = Path(manager.operation['anchor_dir']) / 'shared-pool-handoff.json'
            files = {**{shared_state / name: checksum for name, checksum in _files(shared_state).items()},
                anchor: hashlib.sha256(private_state._private_read(anchor)).hexdigest()}
            if not files.items() <= retained.inputs.history.items():
                raise ValueError

            def scope(migration: PoolMigrationRequest) -> Document:
                spec = migration.registration.spec
                return {'binding': asdict(migration.registration.binding),
                    'pool': (spec.pool_id, spec.installation_id, spec.admission_epoch, spec.cluster_id, spec.node_group_id),
                    'participants': {row.participant_id: (row.environment_id, row.environment_class,
                        row.execution_namespace, row.build_namespace) for row in spec.participants},
                    'guards': {row.participant_id: (row.namespace, row.namespace_uid, _key(row.controller), _uid(row.controller))
                        for row in migration.guards}}

            if scope(retained.inputs.previous) != scope(bound.migration):
                raise ValueError
            participant, = (row for row in retained.release.refresh.before.participants
                if row.participant_id == retained.release.refresh.participant_id)
            guard, = (row for row in retained.inputs.previous.guards if row.participant_id == participant.participant_id)
            namespaces = {guard.namespace, participant.execution_namespace.name, participant.build_namespace.name}
            roots, authority = copy.deepcopy(baseline[1:3])
            manager_key = _key(retained.inputs.manager)
            original_keys = {_key(row) for row in retained.inputs.current}
            source_keys = {_key(row) for rows in retained.release.files.values() for row in rows}
            receipts = _ResourceView(retained, preparing)
            current = self.observe(preparing=preparing)
            for key, row in current.items():
                kind = row['kind']
                if kind in {'Role', 'RoleBinding', 'ClusterRole', 'ClusterRoleBinding'}:
                    choices = authority[key]
                    original, = choices
                    # Candidate receipts may add release annotations, never
                    # change grants, binding subjects or the installed UID.
                    if (original is None or _uid(row) != _uid(original)
                            or {k: v for k, v in row.items() if k not in {'metadata', 'status'}}
                                != {k: v for k, v in original.items() if k not in {'metadata', 'status'}}):
                        raise ValueError
                    authority[key] = (row,)
                elif kind in {'Deployment', 'StatefulSet', 'CronJob', 'Job'}:
                    if key in roots:
                        original, = roots[key]
                        if original is None or _uid(row) != _uid(original):
                            raise ValueError
                    if key == manager_key:
                        if key not in roots:
                            raise ValueError
                    elif kind == 'Job' and key not in original_keys:
                        # Only new Jobs in the fixed candidate/pool journals
                        # may run. Original captured Jobs remain terminal-only.
                        if row['metadata']['namespace'] not in namespaces | {retained.inputs.previous.registration.binding.namespace}:
                            raise ValueError
                    elif key not in source_keys or row['metadata']['namespace'] not in namespaces:
                        if key not in roots or not any(original is not None and _matches(row, original, _uid(original))
                                for original in roots[key]):
                            if kind != 'Job':
                                raise ValueError
                        continue
                    if kind != 'Job' or key not in original_keys:
                        roots[key] = (row,)
            result = _observe_shared_writer_options(plan=bound.plan, transport=self,
                workload_options=roots, authority_options=authority,
                historical_jobs={_key(row): row for row in retained.inputs.current if row['kind'] == 'Job'})
            self._review_writer_permissions(result)
            # An effective-rule review is not a snapshot lock. Re-run the full
            # census after it so a late writer/grant cannot hide in that gap.
            result = _observe_shared_writer_options(plan=bound.plan, transport=self,
                workload_options=roots, authority_options=authority,
                historical_jobs={_key(row): row for row in retained.inputs.current if row['kind'] == 'Job'})
            after = self.observe(preparing=preparing)
            if (bindings != bound or self.retained != retained or after.keys() != current.keys()
                    or any(not _matches(after[key], row, _uid(row)) for key, row in current.items())
                    or _forward_options(bound, shared_state) != baseline
                    or _ResourceView(retained, preparing).files != receipts.files):
                raise ValueError
            return replace(result, input_digest=digest({'reference': retained.reference.model_dump(mode='json'),
                'census': result.input_digest, 'resources': {key: {'uid': _uid(row), 'body': _stable(row)}
                    for key, row in current.items()}}))
        except Exception:
            raise ValueError('participant writer census unqualified; preserve evidence') from None

    def _review_writer_permissions(self, writers: SharedPoolWriterObservation) -> None:
        """Current per-account effective rules, including foreign/group bindings."""
        from scripts.ops.nebius_management_prerequisites import inventory_resources
        from scripts.ops.nebius_pool_gateway_authority import subject_review_namespaces
        from scripts.ops.nebius_shared_pool_handoff_live import _resource_roots
        from scripts.ops.nebius_shared_pool_recovery_roles import _qualify_rules

        def bindings() -> list[Document]:
            return sorted((row for plural, kind in (('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))
                for row in inventory_resources(self._request, 'rbac.authorization.k8s.io/v1', plural, kind)), key=_key)

        before = bindings()
        legacy = writers.plan.staging.legacy
        subjects = set()
        for row in (*writers.workloads.values(), *self.retained.inputs.current,
                *legacy.context.original.retained.values(), *legacy.context.predecessor.retained.values(),
                *_resource_roots(writers.plan, frozenset({'Job'})).values()):
            if row['kind'] not in {'Deployment', 'StatefulSet', 'CronJob', 'Job'}:
                continue
            pod = row['spec']['jobTemplate']['spec']['template']['spec'] if row['kind'] == 'CronJob' else row['spec']['template']['spec']
            subjects.add((row['metadata']['namespace'], pod.get('serviceAccountName', 'default')))
        for subject in sorted(subjects):
            namespace, account = subject
            headers = [('Impersonate-User', f'system:serviceaccount:{namespace}:{account}'),
                *(('Impersonate-Group', group) for group in ('system:serviceaccounts',
                    'system:serviceaccounts:' + namespace, 'system:authenticated'))]
            for selected in subject_review_namespaces(self.retained.inputs.previous, before, subject=subject):
                with self.client.stream('POST', '/apis/authorization.k8s.io/v1/selfsubjectrulesreviews', headers=headers,
                        json={'apiVersion': 'authorization.k8s.io/v1', 'kind': 'SelfSubjectRulesReview',
                            'spec': {'namespace': selected}}) as response:
                    if response.status_code != 201 or response.headers.get('content-encoding', 'identity').lower() != 'identity':
                        raise ValueError
                    raw = bytearray()
                    for chunk in response.iter_bytes(chunk_size=16384):
                        if len(raw) + len(chunk) > 4 * 1024**2:
                            raise ValueError
                        raw.extend(chunk)
                    _qualify_rules(json.loads(raw), catalog=writers.authority, subject=subject, namespace=selected)
        if bindings() != before:
            raise ValueError
