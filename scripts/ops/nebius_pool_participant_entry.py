"""Live qualification for an authenticated protected operator-host release.

This composes real source/history, guard, writer, process, registry and external
authority readers. It is not a public document-input command or authentication
bootstrap. The protected entry must load the semantic initial handoff and supply
its separately authenticated current connection; no default-success probe exists.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import copy
import json
import os
import ssl
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import Kubectl
from scripts.ops.nebius_pool_participant_adapter import (
    QualificationPhase,
    RetainedParticipantPoolAdapter,
)
from scripts.ops.nebius_pool_participant_completion import (
    CompletedParticipantRollout,
    load_participant_head,
    load_participant_predecessor,
    promote_participant_head,
)
from scripts.ops.nebius_pool_participant_creation import HTTPSParticipantCreationAPI
from scripts.ops.nebius_pool_participant_observation import (
    HTTPSParticipantResourceObserver,
    _ResourceView,
)
from scripts.ops.nebius_pool_participant_operation import (
    ParticipantPredecessor,
    RetainedParticipantRollout,
    load_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import (
    HTTPSParticipantResourceAPI,
    RetainedCandidateFileWriter,
)
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings

from loom.nebius_kubernetes import NebiusKubernetesConnection


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


def run_participant_release(args: argparse.Namespace, *, retained: RetainedParticipantRollout,
        initial: ParticipantPredecessor, expected: ParticipantPredecessor,
        pool: RetainedParticipantPoolAdapter, writer: RetainedCandidateFileWriter,
        kube: Kubectl) -> dict[str, Any]:
    """Run the real idle/deploy lifecycle and promote its exact predecessor.

    Inputs come from protected semantic history loading, not public documents.
    Recovery retains the original predecessor even after a committed promotion
    lost its acknowledgement. The deployer's completion hook owns failure
    evidence and never reports success before this promotion finishes.
    """
    from scripts.ops.nebius_idle_rollout import rollout

    initial, expected = copy.deepcopy(initial), copy.deepcopy(expected)
    original = load_participant_predecessor(initial=initial, retained=retained)
    if (original.state_dir, original.migration, original.history) != (expected.state_dir, expected.migration, expected.history):
        raise ValueError('participant delivery predecessor differs; preserve evidence')
    writer.validate()
    if (writer.retained != retained or pool.retained != retained
            or expected.state_dir != retained.inputs.predecessor_state_dir
            or expected.migration != retained.inputs.previous or expected.history != retained.inputs.history):
        raise ValueError('participant delivery predecessor differs; preserve evidence')
    head = load_participant_head(initial=initial, state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir)
    if not (isinstance(head, CompletedParticipantRollout) and head.retained == retained):
        if (head.state_dir, head.migration, head.history) != (expected.state_dir, expected.migration, expected.history):
            raise ValueError('participant delivery head advanced; preserve evidence')

    def finalize() -> None:
        promote_participant_head(initial=initial, state_root=retained.state_dir.parent,
            anchor_dir=retained.anchor_dir, expected=expected, reference=retained.reference)

    result: dict[str, Any] = rollout(args, participant_rollout=retained, pool=pool,
        candidate_writer=writer, participant_finalize=finalize, kube=kube)
    return result


@dataclass(frozen=True)
class ParticipantReleaseConnection:
    qualifier: ParticipantReleaseQualifier
    expected: ParticipantPredecessor
    pool: RetainedParticipantPoolAdapter
    writer: RetainedCandidateFileWriter

    def rollout(self, args: argparse.Namespace) -> dict[str, Any]:
        return run_participant_release(args, retained=self.qualifier.retained,
            initial=self.qualifier.initial, expected=self.expected, pool=self.pool,
            writer=self.writer, kube=self.qualifier.kube)


@contextmanager
def connected_participant_release(*, retained: RetainedParticipantRollout,
        initial: ParticipantPredecessor, expected: ParticipantPredecessor,
        bindings: SharedPoolGuardBindings, shared_state: Path, kube: Kubectl,
        kubeconfig: Path, executable: Path, operator_credentials: Path,
        private_files: dict[Path, bytes], ssl_context: ssl.SSLContext,
        token: str | None = None) -> Iterator[ParticipantReleaseConnection]:
    """Connect all fixed APIs to one explicitly authenticated operator route.

    Authentication, cluster-compatible private kubeconfig creation and semantic
    initial/predecessor loading belong to the protected host entry. This function
    does not obtain ambient credentials or expose an independent CLI route.
    """
    retained, expected = copy.deepcopy(retained), copy.deepcopy(expected)
    if (expected.state_dir != retained.inputs.predecessor_state_dir
            or expected.migration != retained.inputs.previous or expected.history != retained.inputs.history):
        raise ValueError('participant delivery predecessor differs; preserve evidence')
    connection: dict[str, Any] = dict(api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl_context, token=token)
    with HTTPSParticipantResourceObserver(retained=retained, **connection) as observer:
        qualifier = ParticipantReleaseQualifier(retained=retained, initial=initial, bindings=bindings,
            shared_state=shared_state, observer=observer, kube=kube, kubeconfig=kubeconfig,
            executable=executable, operator_credentials=operator_credentials, private_files=private_files)
        pool = RetainedParticipantPoolAdapter(retained=retained, qualify=qualifier, **connection)
        def resource(key: str) -> HTTPSParticipantResourceAPI:
            return HTTPSParticipantResourceAPI(retained=retained, key=key,
                qualify_resource=qualifier.candidate, **connection)

        def creation(key: str) -> HTTPSParticipantCreationAPI:
            return HTTPSParticipantCreationAPI(retained=retained, key=key,
                qualify_creation=qualifier.candidate, **connection)

        writer = RetainedCandidateFileWriter(retained=retained, open_resource=resource, open_creation=creation)
        yield ParticipantReleaseConnection(qualifier, expected, pool, writer)


@contextmanager
def authenticated_participant_release(*, retained: RetainedParticipantRollout,
        initial: ParticipantPredecessor, expected: ParticipantPredecessor,
        bindings: SharedPoolGuardBindings, shared_state: Path,
        operator_connection: NebiusKubernetesConnection, executable: Path,
        operator_credentials: Path, private_files: dict[Path, bytes]) -> Iterator[ParticipantReleaseConnection]:
    """Exchange the explicit operator credential and keep its config private.

    The kubeconfig uses the retained Nebius cluster ID, not the historical probe
    alias. It contains only this endpoint, CA and bearer, with no credential
    plugin. The temporary kubeconfig and discovery cache are removed on exit.
    The protected host bootstrap still owns source/publication and history intake.
    """
    from scripts.ops.nebius_development_entry import _transport

    files = dict(private_files)

    def unchanged() -> None:
        for path, raw in files.items():
            if (not path.is_absolute() or path != path.resolve()
                    or private_state._private_read(path, limit=16 * 1024**2) != raw):
                raise ValueError('participant operator material changed; preserve evidence')

    try:
        if (operator_connection.endpoint != retained.inputs.config['kubernetes_api_server'].rstrip('/')
                or not {operator_connection.ca_file, operator_connection.credentials_file, operator_credentials} <= files.keys()
                or os.environ.get('LOOM_DEPLOY_SSH_TARGET')):
            raise ValueError
        unchanged()
        context, token = asyncio.run(_transport(operator_connection))
        unchanged()
    except Exception:
        raise ValueError('participant operator authentication unavailable; preserve evidence') from None
    with tempfile.TemporaryDirectory(prefix='loom-participant-operator-') as temporary:
        path = Path(temporary) / 'connection.json'
        cluster = retained.inputs.config['cluster_id']
        config = {'apiVersion': 'v1', 'kind': 'Config', 'current-context': cluster,
            'clusters': [{'name': cluster, 'cluster': {'server': operator_connection.endpoint,
                'certificate-authority-data': base64.b64encode(files[operator_connection.ca_file]).decode()}}],
            'users': [{'name': 'participant-operator', 'user': {'token': token}}],
            'contexts': [{'name': cluster, 'context': {'cluster': cluster, 'user': 'participant-operator'}}]}
        raw = json.dumps(config, sort_keys=True).encode()
        private_state._write_private(path, raw)
        files[path] = raw
        kube = Kubectl(path, local_executable=executable)
        with connected_participant_release(retained=retained, initial=initial, expected=expected,
                bindings=bindings, shared_state=shared_state, kube=kube, kubeconfig=path,
                executable=executable, operator_credentials=operator_credentials,
                private_files=files, ssl_context=context, token=token) as delivery:
            unchanged()
            yield delivery
            unchanged()
