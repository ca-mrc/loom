"""Live qualification for an authenticated protected operator-host release.

This composes real source/history, guard, writer, process, registry and external
authority readers. It is not a public document-input command or authentication
bootstrap. The protected entry must load the semantic initial handoff and supply
its separately authenticated current connection; no default-success probe exists.
"""
from __future__ import annotations

import asyncio
import copy
import os
import time
from pathlib import Path

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import Kubectl
from scripts.ops.nebius_pool_participant_adapter import QualificationPhase
from scripts.ops.nebius_pool_participant_completion import (
    CompletedParticipantRollout,
    load_participant_head,
)
from scripts.ops.nebius_pool_participant_observation import (
    HTTPSParticipantResourceObserver,
    _ResourceView,
)
from scripts.ops.nebius_pool_participant_operation import (
    ParticipantPredecessor,
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings


class ParticipantReleaseQualifier:
    """One exact release/connection; qualifications never survive a mutation."""

    def __init__(self, *, retained: RetainedParticipantRollout, initial: ParticipantPredecessor,
            bindings: SharedPoolGuardBindings, shared_state: Path, observer: HTTPSParticipantResourceObserver,
            kube: Kubectl, kubeconfig: Path, executable: Path, operator_credentials: Path,
            private_files: dict[Path, bytes]):
        self.retained, self.initial = copy.deepcopy(retained), copy.deepcopy(initial)
        self.bindings, self.shared_state = copy.deepcopy(bindings), shared_state
        self.observer, self.kube = observer, kube
        self.kubeconfig, self.executable, self.operator_credentials = kubeconfig, executable, operator_credentials
        self.private_files = dict(private_files)
        self._origin = (copy.deepcopy(bindings), shared_state, kubeconfig, executable, operator_credentials,
            dict(private_files), observer, kube)
        self._connection = (observer.api_server, observer.client, str(observer.client.base_url), dict(observer.client.headers))
        if not {kubeconfig, operator_credentials} <= private_files.keys() or not executable.is_absolute():
            raise ValueError('participant protected connection incomplete; preserve evidence')
        self._source('preflight')

    def _source(self, phase: QualificationPhase) -> None:
        retained = self.retained
        if (phase not in {'preflight', 'guarded', 'ready', 'released'}
                or (self.bindings, self.shared_state, self.kubeconfig, self.executable, self.operator_credentials,
                    self.private_files, self.observer, self.kube) != self._origin
                or self.kube.kubeconfig != self.kubeconfig or self.kube.context is not None
                or os.environ.get('LOOM_DEPLOY_SSH_TARGET')
                or self.observer.retained != retained
                or (self.observer.api_server, self.observer.client,
                    str(self.observer.client.base_url), dict(self.observer.client.headers)) != self._connection
                or load_participant_rollout(reference=retained.reference,
                    state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir) != retained):
            raise ValueError('participant protected source/connection changed; preserve evidence')
        for path, raw in self.private_files.items():
            if (not path.is_absolute() or path != path.resolve()
                    or private_state._private_read(path, limit=16 * 1024**2) != raw):
                raise ValueError('participant protected material changed; preserve evidence')
        head = load_participant_head(initial=self.initial,
            state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir)
        if isinstance(head, CompletedParticipantRollout) and head.retained == retained:
            # A lost final acknowledgement can requalify completion, never
            # reacquire an old release's mutation authority after promotion.
            if phase not in {'preflight', 'released'}:
                raise ValueError('participant release already promoted; preserve evidence')
        elif (head.state_dir != retained.inputs.predecessor_state_dir
                or head.migration != retained.inputs.previous or head.history != retained.inputs.history):
            raise ValueError('participant predecessor advanced; preserve evidence')

    async def _external(self, preparing: str | None) -> None:
        async with httpx.AsyncClient(timeout=30, follow_redirects=False, trust_env=False) as http:
            await self.observer.observe_external(bindings=self.bindings, shared_state=self.shared_state,
                operator_credentials=self.operator_credentials, http=http, preparing=preparing)

    def __call__(self, kube: Kubectl, retained: RetainedParticipantRollout,
            phase: QualificationPhase, *, preparing: str | None = None) -> None:
        """Fresh whole-pool checks; only a known process transition may wait.

        Foreign ownership, failed checks, missing evidence and transport errors
        are not retried. Optional telemetry unavailability remains diagnostic.
        """
        try:
            if kube is not self.kube or retained != self.retained or (preparing is not None and phase != 'guarded'):
                raise ValueError
            deadline = time.monotonic() + 300
            options = {'kubeconfig': self.kubeconfig, 'executable': self.executable}
            while True:
                self._source(phase)
                view = _ResourceView(self.retained, preparing)
                guard = self.observer.observe_guard(phase=phase, preparing=preparing, **options)
                asyncio.run(self._external(preparing))  # Includes bracketing writer and permission census.
                pending = self.observer.observe_participant_processes(phase=phase, preparing=preparing, **options)
                pending += self.observer.observe_manager_process(bindings=self.bindings,
                    phase=phase, preparing=preparing, **options)
                if not pending:
                    self.observer.observe_peer_processes(bindings=self.bindings, shared_state=self.shared_state,
                        preparing=preparing, **options)
                    self.observer.observe_gateway_process(bindings=self.bindings, shared_state=self.shared_state,
                        preparing=preparing, **options)
                    registry = self.observer.observe_registry(bindings=self.bindings, preparing=preparing, **options)
                    if ((phase == 'ready' and registry not in {'staged', 'complete'})
                            or (phase == 'released' and registry != 'complete')):
                        raise ValueError
                if (self.observer.observe_guard(phase=phase, preparing=preparing, **options) != guard
                        or _ResourceView(self.retained, preparing).files != view.files):
                    raise ValueError
                self._source(phase)
                if not pending:
                    return
                remaining = deadline - time.monotonic()
                if phase in {'ready', 'released'} or remaining <= 0:
                    raise ValueError
                time.sleep(min(5, remaining))
        except Exception:
            raise ValueError('participant protected qualification unavailable; preserve evidence') from None

    def candidate(self, retained: RetainedParticipantRollout, key: str) -> None:
        """Fixed candidate API callback; preparing never permits another child."""
        self(self.kube, retained, 'guarded', preparing=key)
