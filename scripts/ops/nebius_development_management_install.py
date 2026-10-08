"""Fresh application-only dev management, composed from fixed installation stages.

The protected live caller qualifies the retained foundation and actual authorities.
This module installs no legacy provisioner and cannot adopt staging history. It is
not a CLI or a source of credentials, public ingress authority or pool admission.
"""
from __future__ import annotations

import copy
import json
import re
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import (
    ApplicationSetupMaterial,
    ApplicationSetupRequest,
    application_setup_ready,
    stage_application_setup,
)
from scripts.ops.nebius_application_setup import (
    _documents as application_documents,
)
from scripts.ops.nebius_development_management_tls import (
    ManagementTLSMaterial,
    deliver_management_tls,
    management_tls_secret_name,
)
from scripts.ops.nebius_development_management_tls import _documents as tls_documents
from scripts.ops.nebius_development_public import render_development_public
from scripts.ops.nebius_management_bootstrap import (
    BootstrapAPI,
    bootstrap_management,
)
from scripts.ops.nebius_management_install import (
    ManagementInstallError,
    ManagementInstallRequest,
    _hash_journals,
    _journal_names,
)
from scripts.ops.nebius_management_material import ManagementBinding, _uuid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    management_phase_ready,
    stage_management_resources,
)
from scripts.ops.nebius_management_storage import (
    prepare_management_storage,
    verify_management_storage,
)
from scripts.ops.nebius_management_supplied import _documents as supplied_documents
from scripts.ops.nebius_management_supplied import deliver_supplied_material

from loom.nebius_environment_render import _envelope
from loom.nebius_platform_render import digest
from loom_service.application_management.deployment import render_application_setup
from loom_service.environment_management.deployment import (
    ManagementDeployment,
    RenderedManagement,
    render_management,
)
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection

_ROOT = Path(__file__).resolve().parents[2]
_APPLICATION_PHASES = ('config', 'admission', 'permissions', 'network', 'material', 'database')
_PHASES = {
    'bootstrap': None, 'config': '10-config-network.yaml', 'supplied': None,
    'database': '20-database.yaml', 'storage': None, 'migration': '30-migrate.yaml',
    **{'application-' + phase: None for phase in _APPLICATION_PHASES},
    'backup': '85-backup-verify.yaml', 'schedule': '80-backup.yaml',
    'service': '40-services.yaml', 'tls': None, 'public': '70-public.yaml',
}


def installation_phases(shared_public_route: bool) -> dict[str, str | None]:
    """Old histories remain private-only; opt-in is pinned in the parent digest."""
    return {**_PHASES, **({'application-development-public': None} if shared_public_route else {})}


@dataclass(frozen=True, repr=False)
class DevelopmentManagementRequest(ManagementInstallRequest):
    application_material: ApplicationSetupMaterial
    shared_namespace_uid: str
    tls_material: ManagementTLSMaterial
    qualification_digest: str | None = None
    shared_public_route: bool = False

    def __post_init__(self) -> None:
        if type(self.shared_public_route) is not bool:
            raise ManagementInstallError('development public route selection invalid')
        # The private entry must bind settings/files that do not appear in the
        # renderer. None is reserved for isolated component use, not deployment.
        if self.qualification_digest is not None and (
                not isinstance(self.qualification_digest, str)
                or re.fullmatch(r'sha256:[0-9a-f]{64}', self.qualification_digest) is None):
            raise ManagementInstallError('development management qualification digest invalid')


class DevelopmentManagementAPI(Protocol):
    def preflight(self, request: DevelopmentManagementRequest, rendered: RenderedManagement) -> None:
        """Qualify source, retained foundation, private inputs, DNS/TLS and capacity."""
        ...

    def bootstrap_api(self) -> AbstractContextManager[BootstrapAPI]: ...
    def resources(self, binding: ManagementBinding, phase: str) -> AbstractContextManager[ManagementStageAPI]: ...
    def application_resources(self, request: ApplicationSetupRequest,
                              phase: str) -> AbstractContextManager[ManagementStageAPI]: ...
    def qualify_storage(self, binding: ManagementBinding, rendered: RenderedManagement,
                        receipt: dict[str, Any]) -> None:
        """Authenticate the recorded PVC/PV's actual provider disk before migration."""
        ...

    def qualify_application(self, request: ApplicationSetupRequest, state_dir: Path) -> None:
        """Check actual-subject Kubernetes/cloud enforcement before SQL setup."""
        ...

    def verify_backup(self, binding: ManagementBinding, rendered: RenderedManagement,
                      job_uid: str) -> dict[str, Any]: ...
    def verify_public(self, binding: ManagementBinding, rendered: RenderedManagement, material_dir: Path) -> None:
        """Require trusted HTTPS, application-worker readiness and public auth tests."""
        ...


def _setup(request: DevelopmentManagementRequest, binding: ManagementBinding) -> ApplicationSetupRequest:
    return ApplicationSetupRequest(request.deployment, request.candidate, request.profile, binding,
        request.shared_namespace_uid, _ROOT, request.application_material, request.shared_public_route)


def render_installation(request: DevelopmentManagementRequest) -> RenderedManagement:
    """Reject legacy/shared execution bindings before even creating a start marker."""
    deployment = ManagementDeployment.model_validate(request.deployment.model_dump())
    installation, binding = deployment.installation, request.binding
    app, foundation = installation.applications, installation.foundation
    config = foundation.platform_config
    _uuid(request.shared_namespace_uid)
    if ((binding.installation_id, binding.namespace) != (str(deployment.installation_id), 'loom-nebius-management-dev')
            or deployment.namespace != binding.namespace or installation.provider_runtime is not None
            or foundation.namespace_authority is not None or config['namespace'] != 'loom-dev'
            or config['environment'] != 'development' or app is None
            or app.shared.platform_namespace != 'loom-dev'
            or not isinstance(app.runtime.kubernetes, ProjectedKubernetesConnection)
            or app.runtime.build is not None or app.runtime.source_upload is not None
            or deployment.pool_catalog_operation_id is not None
            or request.tls_material.public_host != deployment.public_host
            or deployment.public_tls_secret_name != management_tls_secret_name(binding.installation_id, request.tls_material)):
        raise ManagementInstallError('development management requires independent application-only binding')
    # Validate both credential sets before any bootstrap write. The provisional
    # namespace UID is used only for pure rendering, never live qualification.
    provisional = ManagementBinding(binding.installation_id, binding.namespace,
                                    request.shared_namespace_uid, binding.kube_system_uid)
    supplied_documents(request.material, provisional, application_only=True)
    application_documents(_setup(request, provisional), 'material')
    tls_documents(request.tls_material, provisional)
    rendered = render_management(deployment, candidate=request.candidate, profile=request.profile, repo_root=_ROOT)
    setup = render_application_setup(deployment, candidate=request.candidate, profile=request.profile, repo_root=_ROOT)
    files = copy.deepcopy(rendered.files)
    application_keys = {(doc['kind'], doc['metadata']['name']) for doc in setup['config']}
    files['10-config-network.yaml'] = [doc for doc in files['10-config-network.yaml']
        if (doc['kind'], doc['metadata']['name']) not in application_keys]
    for phase in _APPLICATION_PHASES:
        if phase != 'material':
            files['application-' + phase + '.yaml'] = setup[phase]
    if request.shared_public_route:
        files['application-development-public.yaml'] = render_development_public(deployment)
    stateful = next(doc for doc in files['20-database.yaml'] if doc['kind'] == 'StatefulSet')
    for claim in stateful['spec']['volumeClaimTemplates']:
        claim['metadata'].setdefault('labels', {})['loom.nebius/management-installation'] = binding.installation_id
    cronjob = files['80-backup.yaml'][0]
    job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': copy.deepcopy(cronjob['metadata']),
           'spec': copy.deepcopy(cronjob['spec']['jobTemplate']['spec'])}
    job['metadata']['name'] = 'loom-management-backup-' + rendered.revision[7:19]
    job['spec'].pop('ttlSecondsAfterFinished', None)
    job['spec']['backoffLimit'] = 0
    files['85-backup-verify.yaml'] = [job]
    return replace(rendered, files=files, platform_envelope=_envelope(files))


def _history(record: dict[str, Any], identity: dict[str, Any], state: Path, *, shared_public_route: bool = False) -> None:
    if (not isinstance(record, dict) or set(record) != {*identity, 'phases'}
            or any(record[key] != value for key, value in identity.items()) or not isinstance(record['phases'], dict)):
        raise ManagementInstallError('development management recovery identity differs')
    phases = record['phases']
    expected = installation_phases(shared_public_route)
    if set(phases) != set(list(expected)[:len(phases)]):
        raise ManagementInstallError('development management recovery order differs')
    for index, phase in enumerate(list(expected)[:len(phases)]):
        item = phases[phase]
        if (set(item) != {'status', 'receipt', 'journals'} or item['status'] not in {'started', 'complete'}
                or (item['status'] == 'started' and
                    (index != len(phases) - 1 or item['receipt'] is not None or item['journals'] is not None))):
            raise ManagementInstallError('development management recovery phase differs')
        path = state / phase
        journal = path / _journal_names(phase)[0]
        if path.is_symlink() or not path.is_dir() or not journal.is_file() or journal.is_symlink():
            raise ManagementInstallError('development management recovery evidence missing')
        if item['status'] == 'complete' and item['journals'] != _hash_journals(state, phase):
            raise ManagementInstallError('development management recovery journals changed')


def _final_readback(request: DevelopmentManagementRequest, api: DevelopmentManagementAPI,
                    binding: ManagementBinding, rendered: RenderedManagement, state: Path,
                    record: dict[str, Any]) -> str | None:
    """All journals are complete: fixed replay can only verify, never create."""
    phases = installation_phases(request.shared_public_route)
    if (set(record['phases']) != set(phases)
            or any(item['status'] != 'complete' for item in record['phases'].values())):
        raise ManagementInstallError('development management final recovery evidence incomplete')
    _history(record, {key: value for key, value in record.items() if key != 'phases'}, state,
        shared_public_route=request.shared_public_route)
    for phase, filename in phases.items():
        ready, phase_state = True, state / phase
        if phase == 'bootstrap':
            with api.bootstrap_api() as bootstrap_api:
                receipt = bootstrap_management(binding=request.binding, api=bootstrap_api, state_dir=phase_state)
        elif phase.startswith('application-'):
            setup, selected = _setup(request, binding), phase.removeprefix('application-')
            with api.application_resources(setup, selected) as stage_api:
                receipt = stage_application_setup(request=setup, phase=selected, api=stage_api, state_dir=phase_state)
                if selected in {'admission', 'database'}:
                    ready = application_setup_ready(request=setup, phase=selected, api=stage_api, state_dir=phase_state)
        else:
            with api.resources(binding, 'database' if phase == 'storage' else phase) as stage_api:
                if phase == 'storage':
                    receipt = verify_management_storage(rendered=rendered, binding=binding, api=stage_api,
                        state_dir=state / 'database', evidence_dir=phase_state)
                elif phase == 'supplied':
                    receipt = deliver_supplied_material(material=request.material, binding=binding,
                        api=stage_api, state_dir=phase_state, application_only=True)
                elif phase == 'tls':
                    receipt = deliver_management_tls(material=request.tls_material, binding=binding,
                        api=stage_api, state_dir=phase_state)
                else:
                    assert filename is not None
                    receipt = stage_management_resources(rendered=rendered, phase=filename,
                        binding=binding, api=stage_api, state_dir=phase_state)
                    if phase in {'database', 'migration', 'backup', 'service'}:
                        ready = management_phase_ready(rendered=rendered, phase=filename, binding=binding,
                            api=stage_api, state_dir=phase_state)
        if receipt != record['phases'][phase]['receipt']:
            raise ManagementInstallError('development management final readback changed')
        if not ready:
            return phase
    return None


def install_development_management(*, request: DevelopmentManagementRequest, api: DevelopmentManagementAPI,
                                   state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Resume fixed phases; failed/unknown writes retain their original evidence."""
    stage: str | None = 'recovery'
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state.resolve() == anchor.resolve() or state.resolve() in anchor.resolve().parents
                or anchor.resolve() in state.resolve().parents):
            raise ManagementInstallError('development management recovery anchor must be independent')
        rendered = render_installation(request)
        fingerprint = digest({'binding': asdict(request.binding), 'deployment': request.deployment.model_dump(mode='json'),
            'candidate': request.candidate, 'profile': request.profile, 'material': request.material,
            'application_material': asdict(request.application_material), 'shared_namespace_uid': request.shared_namespace_uid,
            'tls_material': asdict(request.tls_material),
            'qualification_digest': request.qualification_digest,
            **({'shared_public_route': True} if request.shared_public_route else {})})
        identity: dict[str, Any] = {'schema': 'loom.nebius-development-management-install.v1', 'input_digest': fingerprint,
                    'state_dir': str(state), 'binding': asdict(request.binding)}
        with private_state._locked_state(anchor):
            marker, journal = anchor / (request.binding.installation_id + '.json'), state / 'installation.json'
            if marker.exists() or marker.is_symlink():
                started = json.loads(private_state._private_read(marker))
                # Preserve older input fingerprints without rewriting history.
                # Fresh installs retain the qualification preimage so renewal
                # does not need retired original operator credential files.
                if isinstance(started, dict) and 'qualification_digest' in started:
                    identity['qualification_digest'] = request.qualification_digest
                if (not isinstance(started, dict) or set(started) != {*identity, 'operation_id'}
                        or any(started[key] != value for key, value in identity.items())
                        or not journal.is_file() or journal.is_symlink() or state.is_symlink()):
                    raise ManagementInstallError('development management recovery evidence missing or changed')
                record = json.loads(private_state._private_read(journal, limit=1024 * 1024))
                _history(record, started, state, shared_public_route=request.shared_public_route)
            else:
                if state.exists() or state.is_symlink():
                    raise ManagementInstallError('untracked development management recovery state')
                stage = None
                api.preflight(request, rendered)
                stage = 'recovery'
                identity['qualification_digest'] = request.qualification_digest
                started = {**identity, 'operation_id': str(uuid4())}
                record = {**started, 'phases': {}}
                private_state._atomic_json(marker, started)
            with private_state._locked_state(state):
                if not journal.exists():
                    private_state._atomic_json(journal, record)
                stage = None
                api.preflight(request, rendered)
                binding: ManagementBinding | None = None
                backup: dict[str, Any] | None = None
                for phase, filename in installation_phases(request.shared_public_route).items():
                    stage = 'install_' + phase
                    if phase not in record['phases']:
                        record['phases'][phase] = {'status': 'started', 'receipt': None, 'journals': None}
                        private_state._atomic_json(journal, record)
                    item, phase_state = record['phases'][phase], state / phase
                    ready = True
                    if phase == 'bootstrap':
                        with api.bootstrap_api() as bootstrap_api:
                            receipt = bootstrap_management(binding=request.binding, api=bootstrap_api, state_dir=phase_state)
                        binding = ManagementBinding(request.binding.installation_id, request.binding.namespace,
                            receipt['namespace_uid'], request.binding.kube_system_uid)
                    elif phase.startswith('application-'):
                        assert binding is not None
                        setup_request, application_phase = _setup(request, binding), phase.removeprefix('application-')
                        with api.application_resources(setup_request, application_phase) as stage_api:
                            receipt = stage_application_setup(request=setup_request, phase=application_phase,
                                api=stage_api, state_dir=phase_state)
                            if application_phase in {'admission', 'database'}:
                                ready = application_setup_ready(request=setup_request, phase=application_phase,
                                    api=stage_api, state_dir=phase_state)
                    else:
                        assert binding is not None
                        with api.resources(binding, 'database' if phase == 'storage' else phase) as stage_api:
                            if phase == 'storage':
                                receipt = verify_management_storage(rendered=rendered, binding=binding, api=stage_api,
                                    state_dir=state / 'database', evidence_dir=phase_state)
                            elif phase == 'supplied':
                                receipt = deliver_supplied_material(material=request.material, binding=binding,
                                    api=stage_api, state_dir=phase_state, application_only=True)
                            elif phase == 'tls':
                                receipt = deliver_management_tls(material=request.tls_material, binding=binding,
                                    api=stage_api, state_dir=phase_state)
                            else:
                                assert filename is not None
                                if phase == 'database':
                                    prepare_management_storage(rendered=rendered, binding=binding, api=stage_api, state_dir=phase_state)
                                receipt = stage_management_resources(rendered=rendered, phase=filename,
                                    binding=binding, api=stage_api, state_dir=phase_state)
                                if phase in {'database', 'migration', 'backup', 'service'}:
                                    ready = management_phase_ready(rendered=rendered, phase=filename, binding=binding,
                                        api=stage_api, state_dir=phase_state)
                    if item['status'] == 'complete':
                        if receipt != item['receipt']:
                            raise ManagementInstallError('development management recovery receipt changed')
                    else:
                        item.update(status='complete', receipt=receipt, journals=_hash_journals(state, phase))
                        private_state._atomic_json(journal, record)
                    assert binding is not None
                    if not ready:
                        return {'status': 'pending', 'phase': phase, 'installation_id': binding.installation_id,
                            'namespace_uid': binding.namespace_uid, 'revision': rendered.revision}
                    if phase == 'storage':
                        stage = 'provider_storage'
                        api.qualify_storage(binding, rendered, receipt)
                    elif phase == 'application-material':
                        stage = 'application_authority'
                        api.qualify_application(_setup(request, binding), state)
                    elif phase == 'backup':
                        stage = 'backup_execution'
                        job_uid = next(iter(receipt['resource_uids'].values()))
                        backup = api.verify_backup(binding, rendered, job_uid)
                        if (set(backup) != {'job_uid', 'sha256', 'bytes', 'key'} or backup['job_uid'] != job_uid
                                or not isinstance(backup['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', backup['sha256'])
                                or type(backup['bytes']) is not int or backup['bytes'] <= 0
                                or not isinstance(backup['key'], str) or not 0 < len(backup['key']) <= 1024):
                            raise ManagementInstallError('development management backup readback differs')
                assert binding is not None and backup is not None
                stage = 'public_authentication'
                api.verify_public(binding, rendered, state / 'bootstrap' / 'material')
                stage = 'final_readback'
                pending = _final_readback(request, api, binding, rendered, state, record)
                if pending is not None:
                    return {'status': 'pending', 'phase': pending, 'installation_id': binding.installation_id,
                        'namespace_uid': binding.namespace_uid, 'revision': rendered.revision}
                return {'status': 'development_management_installed', 'installation_id': binding.installation_id,
                    'namespace_uid': binding.namespace_uid, 'shared_namespace_uid': request.shared_namespace_uid,
                    'revision': rendered.revision, 'backup': backup}
    except ManagementInstallError as error:
        if error.stage is None:
            error.stage = stage
        raise
    except Exception:
        raise ManagementInstallError('development management installation incomplete; preserve recovery evidence', stage=stage) from None
