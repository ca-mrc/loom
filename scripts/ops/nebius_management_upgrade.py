"""Fixed application upgrade of a retained installation, never a second bootstrap.

The protected entry supplies live identity, material/IAM, actual-subject and public
checks. Resource creation and uncertain updates use the existing fixed journals.
This index only binds their order and prevents lost evidence reopening writes.
"""
from __future__ import annotations

import copy
import hashlib
import json
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import (
    ApplicationSetupRequest,
    application_setup_ready,
    stage_application_setup,
)
from scripts.ops.nebius_management_install import (
    _PHASES,
    ManagementInstallRequest,
    _validate_history,
)
from scripts.ops.nebius_management_stage import ManagementStageAPI
from scripts.ops.nebius_management_switch import (
    ManagementSwitchAPI,
    ManagementSwitchRequest,
    activate_management,
    retire_management,
)

from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import render_management

_STAGES = ('config', 'admission', 'permissions', 'network', 'material', 'database', 'retirement', 'migration')


class ManagementUpgradeError(RuntimeError):
    """Retain recovery evidence; no private inputs or provider errors in reports."""

    def __init__(self, *, stage: str):
        super().__init__('management upgrade incomplete; preserve recovery evidence')
        self.stage = stage


@dataclass(frozen=True, repr=False)
class ManagementUpgradeRequest:
    original: ManagementInstallRequest
    setup: ApplicationSetupRequest
    original_state: Path
    original_anchor: Path


class ManagementUpgradeAPI(Protocol):
    def preflight(self, request: ManagementUpgradeRequest) -> None:
        """Qualify retained resources, candidate, shared material/IAM and physical fit."""
        ...

    def resources(self, request: ApplicationSetupRequest, phase: str) -> AbstractContextManager[ManagementStageAPI]: ...
    def switch_api(self, request: ManagementSwitchRequest) -> AbstractContextManager[ManagementSwitchAPI]: ...
    def qualify_authority(self, request: ApplicationSetupRequest, state_dir: Path) -> bool:
        """Actual application subject; False means admission propagation pending."""
        ...

    def verify_public(self, request: ManagementUpgradeRequest, state_dir: Path) -> bool:
        """False while the exact workload starts; True only after public proof."""
        ...


def _hash(path: Path) -> str:
    return hashlib.sha256(private_state._private_read(path, limit=4 * 1024**2)).hexdigest()


def _original(request: ManagementUpgradeRequest) -> tuple[dict[str, Any], str]:
    """Qualify historical bytes without re-rendering or resuming initial install."""
    original, setup = request.original, request.setup
    before, after = (item.model_dump(mode='json') for item in (original.deployment, setup.deployment))
    for value in (before, after):
        value['installation'].pop('provider_runtime', None)
        value['installation'].pop('applications', None)
        value['installation'].pop('publications')
        foundation = value['installation']['foundation']
        config = json.loads(foundation['platform_config_json'])
        # The shared operator can add a guest execution target independently of
        # management. This is a live-qualified reference, not a resource owned by
        # this upgrade. All other retained data/infrastructure fields stay fixed;
        # prerequisite checks still require the exact current shared ConfigMap.
        config.pop('guest_execution_target', None)
        config.pop('emulated_auth_execution_target', None)
        foundation['platform_config_json'] = json.dumps(config, sort_keys=True)
    if (before != after or original.deployment.installation.provider_runtime is None
            or any(publication not in setup.deployment.installation.publications
                for publication in original.deployment.installation.publications)
            or original.deployment.installation.applications is not None
            or setup.deployment.installation.applications is None or setup.material is None
            or (original.binding.installation_id, original.binding.namespace, original.binding.kube_system_uid)
                != (setup.binding.installation_id, setup.binding.namespace, setup.binding.kube_system_uid)):
        raise ValueError
    state = request.original_state
    anchor = request.original_anchor / (original.binding.installation_id + '.json')
    started = json.loads(private_state._private_read(anchor))
    fingerprint = digest({'binding': asdict(original.binding), 'deployment': original.deployment.model_dump(mode='json'),
        'candidate': original.candidate, 'profile': original.profile, 'material': original.material})
    identity = {'schema': 'loom.nebius-management-install.v1', 'input_digest': fingerprint,
        'state_dir': str(state), 'binding': asdict(original.binding)}
    if (set(started) != {*identity, 'operation_id'} or any(started[key] != value for key, value in identity.items())
            or str(UUID(started['operation_id'])) != started['operation_id'] or not UUID(started['operation_id']).int):
        raise ValueError
    path = state / 'installation.json'
    record = json.loads(private_state._private_read(path, limit=1024**2))
    _validate_history(record, started, state)
    if (set(record['phases']) != set(_PHASES)
            or any(item['status'] != 'complete' for item in record['phases'].values())
            or record['phases']['bootstrap']['receipt']['namespace_uid'] != setup.binding.namespace_uid):
        raise ValueError
    # The old private stage is already hash-bound by the complete install history.
    # Its exact frozen Deployment is the only permitted cutover target.
    service = json.loads(private_state._private_read(state / 'service/stage.json', limit=4 * 1024**2))
    if (service['binding'] != asdict(setup.binding) or service['phase'] != '40-services.yaml'
            or service['schema'] != 'loom.nebius-management-stage.v1'):
        raise ValueError
    item = service['resources']['Deployment:' + setup.binding.namespace + ':loom-service']
    if item['status'] != 'created':
        raise ValueError
    deployment = copy.deepcopy(item['observed'])
    deployment['metadata']['uid'] = item['uid']
    return deployment, _hash(path)


def upgrade_management(*, request: ManagementUpgradeRequest, api: ManagementUpgradeAPI,
                       state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Advance fixed barriers; never reactivate legacy or replay an unknown write."""
    stage = 'recovery'
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        paths = (state, anchor, request.original_state, request.original_anchor)
        if (any(not path.is_absolute() or path != path.resolve() for path in paths)
                or any(a == b or a in b.parents or b in a.parents for index, a in enumerate(paths) for b in paths[index + 1:])
                or any(not path.is_dir() or path.is_symlink() for path in paths[2:])):
            raise ValueError
        # The initial install uses this same outer lock. Never modify its journals.
        with private_state._locked_state(request.original_anchor):
            original, original_digest = _original(request)
            setup = request.setup
            rendered = render_management(setup.deployment, candidate=setup.candidate, profile=setup.profile,
                repo_root=setup.repo_root)
            identity = {'schema': 'loom.nebius-management-upgrade.v1', 'state_dir': str(state),
                'binding': asdict(setup.binding), 'original_installation_sha256': original_digest,
                'input_digest': digest({'revision': rendered.revision, 'shared_namespace_uid': setup.shared_namespace_uid,
                    'material': asdict(setup.material) if setup.material is not None else None})}
            with private_state._locked_state(anchor):
                marker, journal = anchor / (setup.binding.installation_id + '.json'), state / 'upgrade.json'
                if marker.exists() or marker.is_symlink():
                    started = json.loads(private_state._private_read(marker))
                    if (set(started) != {*identity, 'operation_id'}
                            or any(started[key] != value for key, value in identity.items())
                            or str(UUID(started['operation_id'])) != started['operation_id'] or not UUID(started['operation_id']).int):
                        raise ValueError
                    record = json.loads(private_state._private_read(journal, limit=1024**2))
                    if (set(record) != {*started, 'phases', 'switch_started', 'activation_started'}
                            or any(record[key] != value for key, value in started.items())
                            or set(record['phases']) != set(_STAGES[:len(record['phases'])])
                            or type(record['switch_started']) is not bool or type(record['activation_started']) is not bool):
                        raise ValueError
                    for phase, item in record['phases'].items():
                        retained = state / phase / 'stage.json'
                        if (set(item) != {'sha256'} or not retained.is_file() or retained.is_symlink()
                                or (item['sha256'] is not None and item['sha256'] != _hash(retained))):
                            raise ValueError
                    if record['switch_started'] and not (state / 'switch/switch.json').is_file():
                        raise ValueError
                    if record['activation_started'] and (not record['switch_started']
                            or record['phases'].get('migration', {}).get('sha256') is None):
                        raise ValueError
                else:
                    if state.exists() or state.is_symlink():
                        raise ValueError
                    started = {**identity, 'operation_id': str(uuid4())}
                    record = {**started, 'phases': {}, 'switch_started': False, 'activation_started': False}
                stage = 'prerequisites'
                api.preflight(request)
                stage = 'recovery'
                if not marker.exists():
                    private_state._atomic_json(marker, started)
                with private_state._locked_state(state):
                    def save() -> None:
                        private_state._atomic_json(journal, record)

                    save()
                    result = {'installation_id': setup.binding.installation_id,
                        'namespace_uid': setup.binding.namespace_uid, 'revision': rendered.revision}

                    def pending(phase: str) -> dict[str, Any]:
                        return {**result, 'status': 'pending', 'phase': phase}

                    def resources(phase: str) -> bool:
                        nonlocal stage
                        stage = 'upgrade_' + phase
                        if phase not in record['phases']:
                            record['phases'][phase] = {'sha256': None}
                            save()
                        with api.resources(setup, phase) as connected:
                            stage_application_setup(request=setup, phase=phase, api=connected, state_dir=state / phase)
                            checksum = _hash(state / phase / 'stage.json')
                            if record['phases'][phase]['sha256'] not in (None, checksum):
                                raise ValueError
                            record['phases'][phase]['sha256'] = checksum
                            save()
                            return (application_setup_ready(request=setup, phase=phase, api=connected, state_dir=state / phase)
                                if phase in {'admission', 'database', 'retirement', 'migration'} else True)

                    for phase in _STAGES[:-1]:
                        if not resources(phase):
                            return pending(phase)
                        if phase == 'permissions':
                            stage = 'runtime_authority'
                            if not api.qualify_authority(setup, state):
                                return pending('authority')
                    switch = ManagementSwitchRequest(setup, original)
                    with api.switch_api(switch) as connected_switch:
                        if not record['activation_started']:
                            stage = 'upgrade_retire'
                            record['switch_started'] = True
                            save()
                            if not retire_management(request=switch, api=connected_switch, state_dir=state / 'switch'):
                                return pending('retire')
                        if not resources('migration'):
                            return pending('migration')
                        record['activation_started'] = True
                        save()
                        stage = 'upgrade_activate'
                        if not activate_management(request=switch, api=connected_switch, state_dir=state / 'switch'):
                            return pending('activate')
                    stage = 'public_authentication'
                    ready = api.verify_public(request, state)
                    if ready is False:
                        return pending('service')
                    if ready is not True:
                        raise ValueError
                    return {**result, 'status': 'management_upgraded'}
    except ManagementUpgradeError:
        raise
    except Exception:
        raise ManagementUpgradeError(stage=stage) from None
