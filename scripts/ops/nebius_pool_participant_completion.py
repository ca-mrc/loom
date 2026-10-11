"""Retained final-release reconciliation; no deployment or admission replay.

The normal deployer creates this boundary only after application readiness and
fresh pool capacity. A private independent anchor binds the finished effects;
public deployment evidence cannot authorize completion recovery.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_pool_migration import PoolMigrationRequest, migration_contract
from scripts.ops.nebius_pool_participant_creation import _attempt, _stage_record
from scripts.ops.nebius_pool_participant_job import PoolParticipantJobRequest, _parts
from scripts.ops.nebius_pool_participant_operation import (
    ParticipantPredecessor,
    ParticipantRolloutReference,
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import candidate_children
from scripts.ops.nebius_shared_pool_operation import _directory

from loom.nebius_platform_render import digest
from loom_service.pool_management.participant_refresh import ParticipantCapacityReceipt

if TYPE_CHECKING:
    from scripts.ops.deploy_nebius_platform import Kubectl, ParticipantRolloutAPI
    from scripts.ops.nebius_pool_participant_resource import RetainedCandidateFileWriter

Document = dict[str, Any]


class _CompletionEvidence:
    """Pure shared validators; no transport or success-default qualifier exists."""

    def __init__(self, retained: RetainedParticipantRollout):
        self.retained = copy.deepcopy(retained)
        self.root = self.retained.state_dir / 'completion'
        self.path = self.root / 'release.json'
        self.anchor = self.retained.anchor_dir / (str(self.retained.reference.operation_id) + '-completion.json')

    def _source(self) -> None:
        retained = self.retained
        if load_participant_rollout(reference=retained.reference,
                state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir) != retained:
            raise ValueError

    def _effects(self) -> tuple[Document, Document]:
        """Validate all terminal effects; only new read-only capacity Jobs may follow."""
        from scripts.ops.deploy_nebius_platform import job_complete
        from scripts.ops.nebius_pool_participant_observation import _ResourceView

        self._source()
        retained = self.retained
        view = _ResourceView(retained, None)
        children = candidate_children(retained)
        dispatch = json.loads(private_state._private_read(retained.state_dir / 'candidate-file-writes/dispatch.json'))
        required = {key for key, (action, _) in children.items() if action != 'preserve'}
        original = {_key(row): row for row in retained.inputs.current}
        profile = original[f"ConfigMap:{retained.inputs.config['namespace']}:loom-platform-config"]['data']['profile.json']
        migration, = retained.release.files['30-migrate.yaml']
        if (json.loads(profile)['candidate_sha'] == retained.inputs.candidate['candidate_sha']
                and job_complete(original.get(_key(migration), {})) and 'pre-upgrade-backup' not in dispatch['started']):
            required.remove('pre-upgrade-backup')
        if set(dispatch['started']) != required:
            raise ValueError
        jobs: Document = {}
        for key in required:
            action, path = children[key]
            if action == 'update':
                item = json.loads(private_state._private_read(path, limit=4 * 1024**2))['resources'][key]
                if item['status'] != 'applied':
                    raise ValueError
            else:
                document, identity, directory, _ = _attempt(retained, key)
                _, item = _stage_record(document=document, identity=identity, directory=directory,
                    binding=retained.release.migration.registration.binding)
                if item['status'] != 'created':
                    raise ValueError
                if document['kind'] == 'Job':
                    jobs[key] = {'job_uid': item['uid'], 'job_name': document['metadata']['name']}
        if view.registry_states != frozenset({'complete'}):
            raise ValueError
        pool_path = retained.state_dir / 'pool-refresh/dispatch.json'
        pool = json.loads(private_state._private_read(pool_path, limit=4 * 1024**2))
        if any(pool['steps'].get(key, {}).get('status') != 'complete'
                for key in ('fence', 'stage', 'catalog', 'manager', 'open')):
            raise ValueError
        fixed = copy.deepcopy(pool)
        capacity_paths = set()
        for name in pool['steps']:
            if name.startswith('capacity:'):
                request = PoolParticipantJobRequest(retained.release, retained.inputs.manager, 'capacity',
                    UUID(name.removeprefix('capacity:')))
                _, _, directory = _parts(request, retained.state_dir / 'pool-refresh/commands')[1]
                capacity_paths.add(directory / 'stage.json')
                del fixed['steps'][name]
        fixed['intents'] = [name for name in fixed['intents']
            if retained.state_dir / 'pool-refresh' / name not in capacity_paths]
        files = {str(path): hashlib.sha256(raw).hexdigest() for path, raw in view.files.items()
            if raw is not None and path not in capacity_paths | {pool_path}}
        return {'files': files, 'pool': fixed, 'jobs': jobs}, pool

    def _read(self) -> Document | None:
        present = self.path.exists() or self.path.is_symlink()
        anchored = self.anchor.exists() or self.anchor.is_symlink()
        if not present:
            if anchored:
                raise ValueError
            return None
        record: Document = json.loads(private_state._private_read(self.path, limit=4 * 1024**2))
        effects, pool = self._effects()
        identity = {'schema': 'loom.nebius-participant-completion.v1',
            'reference': self.retained.reference.model_dump(mode='json'), 'effects_sha256': digest(effects),
            'jobs_sha256': digest(record['jobs'])}
        if ((anchored and json.loads(private_state._private_read(self.anchor)) != identity)
                or (not anchored and record['status'] != 'prepared')
                or set(record) != {*identity, 'effects', 'jobs', 'capacity_observation_id', 'capacity', 'checked_at', 'status'}
                or any(record[key] != value for key, value in identity.items())
                or record['effects'] != effects or record['status'] not in {'prepared', 'release_intent', 'released'}):
            raise ValueError
        self._jobs(record['jobs'], effects['jobs'])
        proof = ParticipantCapacityReceipt.model_validate_json(json.dumps(record['capacity']))
        observation = UUID(record['capacity_observation_id'])
        step = pool['steps']['capacity:' + str(observation)]
        checked = datetime.fromisoformat(record['checked_at'])
        if (not observation.int or str(observation) != record['capacity_observation_id']
                or step['status'] != 'complete'
                or ParticipantCapacityReceipt.model_validate_json(json.dumps(step['result']['refresh']['capacity'])) != proof
                or checked.tzinfo is None or not proof.checked_at - timedelta(seconds=60) <= checked < proof.valid_until):
            raise ValueError
        return record

    @staticmethod
    def _jobs(outcomes: Document, expected: Document) -> None:
        if outcomes.keys() != expected.keys():
            raise ValueError
        for key, result in outcomes.items():
            if (set(result) != {'status', 'job_uid', 'job_name', 'pod_uids'} or result['status'] != 'complete'
                    or any(result[name] != value for name, value in expected[key].items())
                    or not isinstance(result['pod_uids'], list) or not result['pod_uids']
                    or len(set(result['pod_uids'])) != len(result['pod_uids'])
                    or any(str(UUID(value)) != value or not UUID(value).int for value in result['pod_uids'])):
                raise ValueError


class RetainedParticipantCompletion(_CompletionEvidence):
    def __init__(self, writer: RetainedCandidateFileWriter, pool: ParticipantRolloutAPI):
        super().__init__(writer.retained)
        self.writer, self.pool = writer, pool

    def _source(self) -> None:
        self.writer.validate()
        if self.writer.retained != self.retained:
            raise ValueError

    def prepare(self, kube: Kubectl, *, observation_id: UUID, capacity: ParticipantCapacityReceipt) -> None:
        """Bind terminal children and actual Job outcomes before guard release."""
        try:
            self.pool.qualify_completion(kube, self.retained.release, released=False)
            with private_state._locked_state(self.root):
                existing = self._read()
                if existing is not None and existing['status'] == 'released':
                    raise ValueError
                effects, pool = self._effects()
                outcomes = {}
                for key in effects['jobs']:
                    with self.writer.open_creation(key) as api:
                        if api.retained != self.retained or api.key != key:
                            raise ValueError
                        outcomes[key] = api.job_outcome()
                self._jobs(outcomes, effects['jobs'])
                if (self._effects() != (effects, pool) or (existing is not None and existing['jobs'] != outcomes)
                        or ParticipantCapacityReceipt.model_validate_json(json.dumps(
                            pool['steps']['capacity:' + str(observation_id)]['result']['refresh']['capacity'])) != capacity):
                    raise ValueError
                self.pool.qualify_completion(kube, self.retained.release, released=False)
                checked = datetime.now(UTC)
                if not capacity.checked_at - timedelta(seconds=60) <= checked < capacity.valid_until:
                    raise ValueError
                identity = {'schema': 'loom.nebius-participant-completion.v1',
                    'reference': self.retained.reference.model_dump(mode='json'), 'effects_sha256': digest(effects),
                    'jobs_sha256': digest(outcomes)}
                record = {**identity, 'effects': effects, 'jobs': outcomes, 'capacity_observation_id': str(observation_id),
                    'capacity': capacity.model_dump(mode='json'), 'checked_at': checked.isoformat(), 'status': 'prepared'}
                private_state._atomic_json(self.path, record)
                if not self.anchor.exists():
                    private_state._write_private(self.anchor, json.dumps(identity, sort_keys=True).encode())
                    private_state._sync_directory(self.anchor.parent)
                record['status'] = 'release_intent'
                private_state._atomic_json(self.path, record)
                if self._read() != record:
                    raise ValueError
        except Exception:
            raise ValueError('participant release intent unqualified; preserve evidence') from None

    def reconcile(self, kube: Kubectl) -> Document | None:
        """Open guard plus complete pinned effects may finish, never replay them.

        Held intake returns to the ordinary guarded recovery, which reads back
        prior effects and obtains fresh capacity before another release attempt.
        """
        from scripts.ops import deploy_nebius_platform as deploy

        try:
            # No local completion state is created during a normal preflight.
            if not any(path.exists() or path.is_symlink() for path in (self.root, self.anchor)):
                return None
            with private_state._locked_state(self.root):
                record = self._read()
                if record is None:
                    return None
                retained = self.retained
                guard = deploy.observe_rollout_guard(kube, retained.inputs.config['namespace'],
                    retained.guard_owner, retained.inputs.candidate['candidate_sha'])['status']
                if guard == 'held' and record['status'] in {'prepared', 'release_intent'}:
                    return None
                if guard != 'open' or record['status'] == 'prepared':
                    raise ValueError
                self.pool.qualify_completion(kube, retained.release, released=True)
                deploy.verify_deployed_images(kube, retained.release.files)
                deploy.public_smoke('https://' + retained.inputs.config['public_host'], retained.inputs.config['environment'])
                self.pool.qualify_completion(kube, retained.release, released=True)
                if self._read() != record:
                    raise ValueError
                if record['status'] != 'released':
                    record['status'] = 'released'
                    private_state._atomic_json(self.path, record)
                return {'capacity_observation_id': record['capacity_observation_id'], 'capacity': record['capacity']}
        except Exception:
            raise ValueError('participant release completion unqualified; preserve evidence') from None


@dataclass(frozen=True, repr=False)
class CompletedParticipantRollout:
    retained: RetainedParticipantRollout
    migration: PoolMigrationRequest
    resources: dict[str, Document]
    manager: Document
    catalog: Document
    history: dict[Path, str]

    @property
    def state_dir(self) -> Path:
        return self.retained.state_dir


def load_completed_participant_rollout(*, reference: ParticipantRolloutReference,
        state_root: Path, anchor_dir: Path) -> CompletedParticipantRollout:
    """Load terminal semantic evidence, never infer current runtime readiness.

    Desired migration guards are only a projection. The successor must retain
    the actual receipt-selected wired/defaulted controller and database objects.
    Later release entry still qualifies these historical identities live.
    """
    from scripts.ops.nebius_pool_participant_observation import _ResourceView

    try:
        retained = load_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
        evidence = _CompletionEvidence(retained)
        record = evidence._read()
        if record is None or record['status'] != 'released':
            raise ValueError
        view = _ResourceView(retained, None)
        resources = {}
        for key, document in view.documents.items():
            if document['kind'] == 'Job':
                continue
            choice, = view.options[key]
            if choice is None or choice.uid is None or choice.requested is not None:
                raise ValueError
            value = copy.deepcopy(choice.document)
            value['metadata']['uid'] = choice.uid
            _uid(value)
            resources[key] = value
        release = retained.release
        selected = release.refresh.participant_id
        original, = (row for row in retained.inputs.previous.guards if row.participant_id == selected)
        if (original.database is None
                or _uid(resources['Namespace:-:' + original.namespace]) != str(original.namespace_uid)):
            raise ValueError
        database = replace(original.database, statefulset=resources[_key(original.database.statefulset)],
            service=resources[_key(original.database.service)])
        guard = replace(original, controller=resources[_key(original.controller)], database=database)
        if any(_uid(before) != _uid(after) for before, after in (
                (original.controller, guard.controller), (original.database.statefulset, database.statefulset),
                (original.database.service, database.service))):
            raise ValueError
        migration = replace(release.migration, guards=tuple(
            guard if row.participant_id == selected else row for row in retained.inputs.previous.guards))
        migration_contract(migration)
        manager = resources[_key(retained.inputs.manager)]
        catalog = resources['ConfigMap:' + migration.registration.binding.namespace
            + ':loom-pool-profiles-' + reference.operation_id.hex]
        history = dict(retained.inputs.history)
        files = dict(view.files)
        for path in (retained.state_dir / 'inputs.json',
                anchor_dir / (str(reference.operation_id) + '-participant-rollout.json'), evidence.path, evidence.anchor):
            files[path] = private_state._private_read(path, limit=16 * 1024**2)
        for path, raw in files.items():
            if raw is None:
                continue
            checksum = hashlib.sha256(raw).hexdigest()
            if path in history and history[path] != checksum:
                raise ValueError
            history[path] = checksum
        if (evidence._read() != record or _ResourceView(retained, None).files != view.files
                or load_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir) != retained):
            raise ValueError
        completed = CompletedParticipantRollout(retained, migration, resources, manager, catalog, history)
        _head_value(completed)  # Includes the final readback of every immutable ancestor/child byte.
        return completed
    except Exception:
        raise ValueError('completed participant history unavailable; preserve evidence') from None


def _head_value(predecessor: ParticipantPredecessor) -> Document:
    """An exact semantic token; mutable latest-pointer bytes never belong here."""
    _directory(predecessor.state_dir)
    if not predecessor.history or len(predecessor.history) > 32768:
        raise ValueError
    for path, checksum in predecessor.history.items():
        if (not path.is_absolute() or path != path.resolve()
                or hashlib.sha256(private_state._private_read(path, limit=16 * 1024**2)).hexdigest() != checksum):
            raise ValueError
    return {'operation_id': str(predecessor.migration.registration.spec.operation_id),
        'state_dir': str(predecessor.state_dir), 'migration_sha256': digest(migration_contract(predecessor.migration)),
        'history_sha256': digest({str(path): checksum for path, checksum in predecessor.history.items()})}


def _head_paths(state_root: Path, anchor_dir: Path) -> tuple[Path, Path]:
    if (state_root == anchor_dir or state_root in anchor_dir.parents or anchor_dir in state_root.parents
            or any(not root.is_absolute() or root != root.resolve() or root == Path('/')
                for root in (state_root, anchor_dir))):
        raise ValueError
    return state_root / 'latest.json', anchor_dir / 'participant-head.json'


def load_participant_head(*, initial: ParticipantPredecessor,
        state_root: Path, anchor_dir: Path) -> ParticipantPredecessor:
    """Read the initialized latest predecessor and semantically load its receipt."""
    try:
        path, anchor = _head_paths(state_root, anchor_dir)
        for root in (state_root, anchor_dir):
            _directory(root)
        identity = {'schema': 'loom.nebius-participant-head.v1', 'initial': _head_value(initial)}
        anchored = private_state._private_read(anchor)
        raw = private_state._private_read(path)
        record = json.loads(raw)
        if (json.loads(anchored) != identity or set(record) != {*identity, 'selected', 'reference'}
                or any(record[key] != value for key, value in identity.items())):
            raise ValueError
        selected: ParticipantPredecessor = initial
        if record['reference'] is not None:
            reference = ParticipantRolloutReference.model_validate_json(json.dumps(record['reference']))
            selected = load_completed_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
        if (record['selected'] != _head_value(selected)
                or selected.migration.registration.binding != initial.migration.registration.binding
                or any(selected.history.get(key) != value for key, value in initial.history.items())
                or path in selected.history or anchor in selected.history
                or private_state._private_read(path) != raw or private_state._private_read(anchor) != anchored
                or _head_value(initial) != identity['initial']):
            raise ValueError
        return selected
    except Exception:
        raise ValueError('participant head unavailable; preserve evidence') from None


def initialize_participant_head(*, initial: ParticipantPredecessor,
        state_root: Path, anchor_dir: Path) -> ParticipantPredecessor:
    """Initialize from a qualified handoff once; never reset lost/advanced state."""
    try:
        path, anchor = _head_paths(state_root, anchor_dir)
        identity = {'schema': 'loom.nebius-participant-head.v1', 'initial': _head_value(initial)}
        if path in initial.history or anchor in initial.history:
            raise ValueError
        with private_state._locked_state(state_root):
            private_state._private_directory(anchor_dir)
            if not anchor.exists() and not anchor.is_symlink():
                record = {**identity, 'selected': identity['initial'], 'reference': None}
                if path.exists() or path.is_symlink():
                    # Only an identical local initialization can precede its
                    # anchor. No promotion is authorized without that anchor.
                    if json.loads(private_state._private_read(path)) != record:
                        raise ValueError
                else:
                    private_state._atomic_json(path, record)
                private_state._atomic_json(anchor, identity)
            return load_participant_head(initial=initial, state_root=state_root, anchor_dir=anchor_dir)
    except Exception:
        raise ValueError('participant head initialization unqualified; preserve evidence') from None


def promote_participant_head(*, initial: ParticipantPredecessor, expected: ParticipantPredecessor,
        reference: ParticipantRolloutReference, state_root: Path, anchor_dir: Path) -> CompletedParticipantRollout:
    """CAS only from the retained exact predecessor, with lost-ack idempotence.

    This updates private selection, not admission or deployment. Guard release
    must already have been qualified by operational completion reconciliation.
    """
    try:
        path, _ = _head_paths(state_root, anchor_dir)
        with private_state._locked_state(state_root):
            current = load_participant_head(initial=initial, state_root=state_root, anchor_dir=anchor_dir)
            successor = load_completed_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
            inputs = successor.retained.inputs
            if (inputs.previous != expected.migration or inputs.predecessor_state_dir != expected.state_dir
                    or inputs.history != expected.history):
                raise ValueError
            expected_value = _head_value(expected)
            current_value, successor_value = _head_value(current), _head_value(successor)
            if current_value != successor_value:
                if current_value != expected_value:
                    raise ValueError
                private_state._atomic_json(path, {'schema': 'loom.nebius-participant-head.v1',
                    'initial': _head_value(initial), 'selected': successor_value,
                    'reference': reference.model_dump(mode='json')})
            if load_participant_head(initial=initial, state_root=state_root, anchor_dir=anchor_dir) != successor:
                raise ValueError
            return successor
    except Exception:
        raise ValueError('participant head promotion unqualified; preserve evidence') from None
