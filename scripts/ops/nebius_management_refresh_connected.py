"""Connect refresh barriers without bootstrap replay or widened write authority."""
from __future__ import annotations

import json
import ssl
import tomllib
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import _PATHS, HTTPSApplicationSetupAPI
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_gateway import REFRESH_RETAINED_PREFLIGHT_STAGES
from scripts.ops.nebius_management_live import backup_client
from scripts.ops.nebius_management_proofs import ManagementPublicProbe
from scripts.ops.nebius_management_refresh import render_refresh
from scripts.ops.nebius_management_refresh_backup import HTTPSManagementRefreshBackupAPI
from scripts.ops.nebius_management_refresh_evidence import HTTPSManagementRefreshEvidenceAPI
from scripts.ops.nebius_management_refresh_install import (
    ManagementRefreshInstallError,
    ManagementRefreshInstallRequest,
    _history,
    qualify_refresh_activation,
)
from scripts.ops.nebius_management_refresh_live import HTTPSManagementRefreshSwitchAPI
from scripts.ops.nebius_management_refresh_predecessor import (
    CompletedRefresh,
    CompletedUpgrade,
    load_completed_refresh,
    load_completed_upgrade,
)
from scripts.ops.nebius_management_refresh_resources import (
    HTTPSManagementRefreshResourcesAPI,
    ManagementRefreshResourcesRequest,
    refresh_documents,
)
from scripts.ops.nebius_management_refresh_supersession import (
    FailedRefreshProof,
    load_failed_refresh,
)
from scripts.ops.nebius_management_refresh_switch import (
    ManagementRefreshSwitchRequest,
    refresh_initial,
    refresh_target,
)
from scripts.ops.nebius_management_stage import ManagementStageError
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_upgrade import ManagementUpgradeError, ManagementUpgradeRequest
from scripts.ops.nebius_management_upgrade_live import (
    HTTPSManagementUpgradeAPI,
    ManagementUpgradePrerequisites,
)
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover, load_completed_pool
from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
from scripts.ops.nebius_pool_refresh_live import HTTPSPoolRefreshAPI


class HTTPSManagementRefreshInstaller(HTTPSApplicationSetupAPI):
    """Fixed operations bound to one predecessor, target and private state path.

    The protected entry qualifies current publication/shared prerequisites and
    supplies explicit operator transport. This class composes the retained
    installation verifier, not its installer, with the new refresh adapters.
    """

    def __init__(self, *, request: ManagementRefreshInstallRequest, original: CompletedUpgrade,
                 predecessor: CompletedUpgrade | CompletedRefresh | CompletedPoolCutover, state_dir: Path, api_server: str,
                 ssl_context: ssl.SSLContext, runtime_ca_pem: str | None,
                 checks: ManagementUpgradePrerequisites, token: str | None = None,
                 superseded: FailedRefreshProof | None = None, pool: HTTPSPoolRefreshAPI | None = None):
        self.request, self.original, self.predecessor = request, original, predecessor
        self.superseded = superseded
        self.pool = pool
        if pool is not None and (type(pool) is not HTTPSPoolRefreshAPI or pool.parent.api_server != api_server):
            raise ManagementStageError('pool refresh transport binding differs')
        self.state_dir = state_dir
        self.ssl_context, self.runtime_ca_pem, self.token, self.checks = ssl_context, runtime_ca_pem, token, checks
        self.diagnostic_stage: str | None = None
        self._binding(request)
        render = request.resources.switch.render
        self.upgrade = replace(original.upgrade, setup=replace(original.upgrade.setup,
            deployment=render.after, candidate=render.candidate, profile=render.profile, repo_root=render.repo_root))
        super().__init__(request=self.upgrade.setup, phase='config', api_server=api_server,
            ssl_context=ssl_context, token=token)

    def _binding(self, request: ManagementRefreshInstallRequest) -> None:
        render, setup = request.resources.switch.render, self.original.upgrade.setup
        pooled = (request.pool_baseline is not None or isinstance(self.predecessor, CompletedPoolCutover)
                or render.before.pool_catalog_operation_id is not None
                or (isinstance(self.predecessor, CompletedRefresh) and self.predecessor.pool_baseline is not None))
        if pooled:
            if (self.pool is None or not isinstance(self.predecessor, (CompletedPoolCutover, CompletedRefresh))
                    or self.pool.refresh != PoolManagerRefresh(self.original, self.predecessor, request,
                        self.state_dir, self.superseded)):
                raise ManagementStageError('pool-aware refresh authority qualification is not connected')
            self.pool.refresh.qualify()
        elif self.pool is not None:
            raise ManagementStageError('unexpected pool refresh authority')
        root = Path(self.original.selector.operation['inputs_path']).parent.parent
        expected_state = root / 'refresh' / str(request.resources.switch.operation_id) / 'state'
        if (request != self.request or request.installation_anchor != self.original.upgrade.original_anchor
                or request.resources.binding != setup.binding or request.resources.shared_namespace_uid != setup.shared_namespace_uid
                or render.before != self.predecessor.deployment or render.active != self.predecessor.active
                or self.state_dir != expected_state or self.state_dir != self.state_dir.resolve()
                or any(request.history.get(path) != checksum for path, checksum in self.predecessor.history.items())):
            raise ManagementStageError('refresh request or completed predecessor binding differs')
        render_refresh(render)
        failed = self.superseded
        if failed is None:
            if request.resources.switch.initial_stopped is not None:
                raise ManagementStageError('refresh stopped source requires qualified failed history')
        elif (request.resources.switch.initial_stopped != failed.stopped
                or failed.request.resources.switch.operation_id == request.resources.switch.operation_id
                or failed.request.installation_anchor != request.installation_anchor
                or failed.request.resources.binding != request.resources.binding
                or failed.request.resources.shared_namespace_uid != request.resources.shared_namespace_uid
                or failed.request.resources.manager_revision != request.resources.manager_revision
                or failed.request.resources.switch.render.active != render.active
                or failed.request.resources.switch.render.before != render.before
                or any(request.history.get(path) != checksum for path, checksum in failed.history.items())):
            raise ManagementStageError('refresh failed history binding differs')
        refresh_initial(request.resources.switch)

    def _pool(self) -> None:
        self._binding(self.request)
        if self.pool is not None:
            self.pool.qualify()

    def _supersession(self) -> None:
        """Read only fixed old resources; preserve terminal Jobs and old journals."""
        proof = self.superseded
        if proof is None:
            return
        if load_failed_refresh(proof.request, proof.selector) != proof:
            raise ValueError
        for phase, recorded in proof.documents.items():
            with HTTPSManagementRefreshResourcesAPI(request=proof.request.resources, phase=phase,
                api_server=self.api_server, ssl_context=self.ssl_context, token=self.token) as api:
                desired = refresh_documents(proof.request.resources, phase)
                by_uid = {_uid(document): document for document in recorded}
                for document in desired.values():
                    api.verify_identity(self.binding)
                    actual = api.get_resource(document)
                    if actual is None or _uid(actual) not in by_uid or _snapshot(actual) != _snapshot(by_uid[_uid(actual)]):
                        raise ValueError
                    if document['kind'] == 'Job' and phase == proof.failed_phase:
                        status = actual.get('status', {})
                        conditions = status.get('conditions', [])
                        counters = [status.get(key, 0) for key in ('active', 'succeeded', 'terminating', 'failed')]
                        if (_uid(actual) != proof.failed_job_uid
                                or any(type(value) is not int or value < 0 for value in counters)
                                or any(counters[:3]) or not isinstance(conditions, list)
                                or sum(row.get('type') == 'Failed' and row.get('status') == 'True' for row in conditions) != 1
                                or any(row.get('type') in {'Complete', 'SuccessCriteriaMet'} and row.get('status') == 'True'
                                    for row in conditions)):
                            raise ValueError

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        raise ManagementStageError('connected refresh has no general resource route')

    def _retained(self) -> HTTPSManagementUpgradeAPI:
        installer = self

        class RefreshPrerequisites:
            def preflight(self, request: ManagementUpgradeRequest) -> None:
                if request != installer.original.upgrade:
                    raise ValueError
                installer._binding(installer.request)
                installer.checks.preflight(installer.upgrade)

            def public_route(self, request: ManagementUpgradeRequest) -> None:
                if request != installer.original.upgrade:
                    raise ValueError
                installer.checks.public_route(installer.upgrade)

        # Bootstrap history must retain its original contract: a later qualified
        # pool catalog is not part of that installation. Keep its read-only
        # resource/storage verifier rooted there, and separately bind current
        # candidate/provider prerequisites to this refresh's qualified contract.
        return HTTPSManagementUpgradeAPI(request=self.original.upgrade, api_server=self.api_server,
            ssl_context=self.ssl_context, runtime_ca_pem=self.runtime_ca_pem, checks=RefreshPrerequisites(), token=self.token)

    def _retained_setup(self) -> None:
        resources = dict(self.original.retained)
        if isinstance(self.predecessor, CompletedRefresh):
            resources.update(self.predecessor.retained)
        for document in resources.values():
            self.verify_identity(self.binding)
            version, plural = _PATHS[document['kind']]
            metadata = document['metadata']
            namespace = metadata.get('namespace')
            if document['apiVersion'] != version or namespace not in {None, self.binding.namespace, self.shared_namespace}:
                raise ValueError
            path = '/api/v1' if version == 'v1' else '/apis/' + version
            path += ('/namespaces/' + namespace if namespace else '') + '/' + plural + '/' + metadata['name']
            actual = self._request('GET', path)
            if actual is None or _uid(actual) != _uid(document) or _snapshot(actual) != _snapshot(document):
                raise ValueError
            if document['kind'] == 'ValidatingAdmissionPolicy':
                status = actual.get('status', {})
                generation, observed = actual['metadata'].get('generation'), status.get('observedGeneration')
                if (type(generation) is not int or generation < 1 or type(observed) is not int or observed < generation
                        or status.get('typeChecking', {}).get('expressionWarnings')):
                    raise ValueError

    def preflight(self, request: ManagementRefreshInstallRequest) -> None:
        try:
            self.diagnostic_stage = 'predecessor'
            self._binding(request)
            _history(request)
            original = load_completed_upgrade(self.original.selector)
            if original != self.original:
                raise ValueError
            predecessor = (load_completed_pool(self.predecessor.selector, original=original)
                if isinstance(self.predecessor, CompletedPoolCutover) else
                load_completed_refresh(self.predecessor.selector, original=original)
                if isinstance(self.predecessor, CompletedRefresh) else original)
            if predecessor != self.predecessor:
                raise ValueError
            self.diagnostic_stage = 'supersession'
            self._supersession()
            self.diagnostic_stage = 'pool_authority'
            self._pool()
            self.diagnostic_stage = 'retained_installation'
            with self._retained() as retained:
                try:
                    retained.preflight(self.original.upgrade)
                except ManagementUpgradeError as error:
                    # Preserve the existing check's closed stage, not its raw
                    # exception or provider payload. Unknown details stay coarse.
                    stage = error.stage
                    if stage == 'prerequisites':
                        detail = getattr(self.checks, 'diagnostic_stage', None)
                        if isinstance(detail, str) and detail in REFRESH_RETAINED_PREFLIGHT_STAGES:
                            stage = detail
                    if isinstance(stage, str) and stage in REFRESH_RETAINED_PREFLIGHT_STAGES:
                        self.diagnostic_stage = stage
                    raise
            self.diagnostic_stage = 'retained_application'
            self._retained_setup()
            self.diagnostic_stage = 'manager'
            with self.switch_api(request.resources.switch) as switch:
                actual = switch.read()
            allowed = [refresh_initial(request.resources.switch)]
            if (self.state_dir / 'switch/cutover.json').exists():
                allowed.extend((refresh_target(request.resources.switch, 'retire'), refresh_target(request.resources.switch, 'activate')))
            if not any(_matches(actual, target, _uid(self.predecessor.active)) for target in allowed):
                raise ValueError
            self.diagnostic_stage = None
        except Exception:
            raise ManagementRefreshInstallError(self.diagnostic_stage or 'prerequisites') from None

    def resources(self, request: ManagementRefreshResourcesRequest, phase: str) -> HTTPSManagementRefreshResourcesAPI:
        if request != self.request.resources:
            raise ManagementStageError('refresh resource request differs')
        return HTTPSManagementRefreshResourcesAPI(request=request, phase=phase, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token, before_write=self._pool)

    def switch_api(self, request: ManagementRefreshSwitchRequest) -> HTTPSManagementRefreshSwitchAPI:
        if request != self.request.resources.switch:
            raise ManagementStageError('refresh switch request differs')

        def activation(selected: ManagementRefreshSwitchRequest) -> bool:
            if selected != self.request.resources.switch:
                raise ValueError
            self._pool()
            ready = qualify_refresh_activation(request=self.request, api=self, state_dir=self.state_dir)
            self._pool()
            return ready

        return HTTPSManagementRefreshSwitchAPI(request=request, binding=self.binding,
            shared_namespace_uid=self.request.resources.shared_namespace_uid, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token, activation_check=activation, before_write=self._pool)

    def _phase(self, request: ManagementRefreshInstallRequest, state_dir: Path, phase: str) -> None:
        if request != self.request or state_dir != self.state_dir / phase or state_dir != state_dir.resolve():
            raise ManagementStageError('refresh evidence request differs')

    def verify_probe(self, request: ManagementRefreshInstallRequest, phase: str, state_dir: Path) -> dict[str, Any] | None:
        self._phase(request, state_dir, phase)
        with HTTPSManagementRefreshEvidenceAPI(request=request.resources, phase=phase, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token) as api:
            return api.probe_report(state_dir)

    def verify_backup(self, request: ManagementRefreshInstallRequest, state_dir: Path) -> dict[str, Any]:
        self._phase(request, state_dir, 'backup')
        with self._retained() as retained:
            retained._retained_phase('supplied')
            retained._retained_material()
        with backup_client(self.original.upgrade.original) as client, HTTPSManagementRefreshBackupAPI(
            request=request.resources, api_server=self.api_server, ssl_context=self.ssl_context, token=self.token) as api:
            return api.backup_receipt(state_dir=state_dir, client=client)

    def verify_public(self, request: ManagementRefreshInstallRequest, state_dir: Path) -> bool:
        try:
            self._binding(request)
            if state_dir != self.state_dir:
                raise ValueError
            self._pool()
            record = json.loads(private_state._private_read(state_dir / 'switch/cutover.json', limit=4 * 1024**2))
            if (record['phase'] != 'active' or record['operation_id'] != str(request.resources.switch.operation_id)
                    or record['original_uid'] != _uid(self.predecessor.active)):
                raise ValueError
            with self.switch_api(request.resources.switch) as switch:
                if not _matches(switch.read(), record['active'], _uid(self.predecessor.active)):
                    raise ValueError
                if not switch.workload_ready():
                    return False
                with self._retained() as retained:
                    retained._retained_phase('service')
                    retained._retained_phase('public')
                    material = retained._retained_material()
                self.checks.public_route(self.upgrade)
                token = tomllib.loads(material['loom-admin-secret']['secrets.toml'])['admin']['token']
                with ManagementPublicProbe(host=request.resources.switch.render.after.public_host, runtime='applications') as public:
                    public.verify(admin_token=token)
                ready = switch.workload_ready()
                self._pool()
                return ready
        except Exception:
            raise ManagementRefreshInstallError('public_authentication') from None
