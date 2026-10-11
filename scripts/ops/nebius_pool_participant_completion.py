"""Retained final-release reconciliation; no deployment or admission replay.

The normal deployer creates this boundary only after application readiness and
fresh pool capacity. A private independent anchor binds the finished effects;
public deployment evidence cannot authorize completion recovery.
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_participant_creation import _attempt, _stage_record
from scripts.ops.nebius_pool_participant_job import PoolParticipantJobRequest, _parts

from loom.nebius_platform_render import digest
from loom_service.pool_management.participant_refresh import ParticipantCapacityReceipt

if TYPE_CHECKING:
    from scripts.ops.deploy_nebius_platform import Kubectl, ParticipantRolloutAPI
    from scripts.ops.nebius_pool_participant_resource import RetainedCandidateFileWriter

Document = dict[str, Any]


class RetainedParticipantCompletion:
    def __init__(self, writer: RetainedCandidateFileWriter, pool: ParticipantRolloutAPI):
        self.writer, self.pool = writer, pool
        self.retained = copy.deepcopy(writer.retained)
        self.root = self.retained.state_dir / 'completion'
        self.path = self.root / 'release.json'
        self.anchor = self.retained.anchor_dir / (str(self.retained.reference.operation_id) + '-completion.json')

    def _effects(self) -> tuple[Document, Document]:
        """Validate all terminal effects; only new read-only capacity Jobs may follow."""
        from scripts.ops.deploy_nebius_platform import job_complete
        from scripts.ops.nebius_pool_participant_observation import _ResourceView

        self.writer.validate()
        retained = self.retained
        if self.writer.retained != retained:
            raise ValueError
        view = _ResourceView(retained, None)
        children = self.writer._children()
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
