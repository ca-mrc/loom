"""Live qualification for an authenticated protected operator-host release.

This composes real source/history, guard, writer, process, registry and external
authority readers. Retained execution loads the semantic initial handoff before
authenticating an explicitly selected current operator identity. It is not a
public document-input command; paths belong to the protected installed caller,
and no default-success probe exists.
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
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import Kubectl
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_adapter import (
    QualificationPhase,
    RetainedParticipantPoolAdapter,
)
from scripts.ops.nebius_pool_participant_completion import (
    CompletedParticipantRollout,
    initialize_participant_head,
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
    ParticipantRolloutReference,
    RetainedParticipantRollout,
    load_participant_rollout,
    retain_participant_rollout,
)
from scripts.ops.nebius_pool_participant_resource import (
    HTTPSParticipantResourceAPI,
    RetainedCandidateFileWriter,
)
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings

from loom.nebius_candidate_contract import candidate_service_schema_head
from loom.nebius_kubernetes import NebiusKubernetesConnection
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.candidates import ProtectedPublication
from loom_service.environment_management.manager import CandidateBundle


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

    def candidate_follows(self, current: str, candidate: str) -> bool:
        """Use the exact retained publication and authenticated source reader."""
        async def compare() -> bool:
            async with httpx.AsyncClient(timeout=30, follow_redirects=False, trust_env=False) as http:
                return await self.observer.candidate_follows(bindings=self.bindings, current_sha=current, http=http)

        try:
            self._source('preflight')
            if candidate != self.retained.inputs.publication.source_sha or candidate != self.retained.inputs.candidate['candidate_sha']:
                raise ValueError
            result = asyncio.run(compare())
            self._source('preflight')
            return result
        except Exception:
            raise ValueError('participant protected ancestry unavailable; preserve evidence') from None

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
        kube: Kubectl, candidate_ancestry: Callable[[str, str], bool] | None = None) -> dict[str, Any]:
    """Run the real idle/deploy lifecycle and promote its exact predecessor.

    Inputs come from protected semantic history loading, not public documents.
    Recovery retains the original predecessor even after a committed promotion
    lost its acknowledgement. The deployer's completion hook owns failure
    evidence and never reports success before this promotion finishes.
    """
    from scripts.ops.nebius_idle_rollout import rollout

    if not callable(candidate_ancestry):
        raise ValueError('participant protected ancestry reader required; preserve evidence')
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
        candidate_writer=writer, participant_finalize=finalize, kube=kube, candidate_ancestry=candidate_ancestry)
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
            writer=self.writer, kube=self.qualifier.kube, candidate_ancestry=self.qualifier.candidate_follows)

    def execute(self, *, automatic: bool = False, recovery: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run on the operator host without runner paths or GitHub credentials.

        The protected caller already loaded this exact release and authenticated
        its connection. GitHub intent/reporting remain runner-owned; this returns
        private deployment evidence, not an unrestricted public report. Recovery
        supplies only a bounded selector and must still pass retained history,
        live guard and phase qualification in the ordinary lifecycle.
        """
        from scripts.ops.nebius_idle_rollout import recovery_record

        retained = copy.deepcopy(self.qualifier.retained)
        config = retained.inputs.config
        record = None
        try:
            if type(automatic) is not bool or (automatic and recovery is not None):
                raise ValueError
            if recovery is not None:
                if not isinstance(recovery, dict) or len(json.dumps(recovery, allow_nan=False).encode()) > 16384:
                    raise ValueError
                record = recovery_record(copy.deepcopy(recovery))
                previous, = (row for row in retained.inputs.current
                    if row['kind'] == 'ConfigMap' and row['metadata']['name'] == 'loom-platform-config'
                    and row['metadata'].get('namespace') == config['namespace'])
                if (record.get('shared_pool_release') != retained.reference.model_dump(mode='json')
                        or record['guard_owner'] != retained.guard_owner
                        or record['candidate_sha'] != retained.inputs.candidate['candidate_sha']
                        or record.get('previous_candidate_sha') != json.loads(previous['data']['profile.json'])['candidate_sha']
                        or any(record[key] != config[key] for key in ('cluster_id', 'namespace', 'execution_namespace'))):
                    raise ValueError
        except Exception:
            raise ValueError('participant host recovery selection unqualified; preserve evidence') from None
        self.writer.validate()
        if self.writer.retained != retained:
            raise ValueError('participant host release differs; preserve evidence')
        evidence = retained.state_dir / 'deployment-evidence'
        private_state._private_directory(evidence)
        with tempfile.TemporaryDirectory(prefix='inputs-', dir=evidence) as temporary:
            publication = Path(temporary)
            private_state._write_private(publication / 'candidate.json', json.dumps(retained.inputs.candidate, allow_nan=False).encode())
            private_state._write_private(publication / 'runtime-profile.json', retained.inputs.runtime_profile.model_dump_json().encode())
            args = argparse.Namespace(kubeconfig=self.qualifier.kubeconfig, namespace=config['namespace'],
                expected_cluster_id=config['cluster_id'], publication_dir=publication,
                candidate=retained.inputs.candidate['candidate_sha'], evidence_dir=evidence,
                github=False, automatic=automatic)
            if record is not None:
                args.recovery_evidence = publication / 'recovery.json'
                private_state._write_private(args.recovery_evidence, json.dumps(record, allow_nan=False).encode())
            return self.rollout(args)


def prepare_retained_participant_release(*, publication: ProtectedPublication, operation_id: UUID,
        shared_operation: Path, shared_operation_sha256: str, state_root: Path, anchor_dir: Path,
        operator_connection: NebiusKubernetesConnection, executable: Path, repo_root: Path) -> RetainedParticipantRollout:
    """Prepare a new exact publication on the host, without acquiring any guard.

    Repeated preparation loads the original independent anchor. It never renders
    or captures a replacement baseline for an operation whose reply was lost.
    History, paths, tooling and credentials are installed-caller selections.
    """
    from scripts.ops.nebius_development_entry import _transport
    from scripts.ops.nebius_pool_participant_prepare import (
        capture_participant_release,
        render_participant_candidate,
        resolve_participant_publication,
    )
    from scripts.ops.nebius_shared_pool_entry import load_completed_history

    try:
        if (not isinstance(operation_id, UUID) or not operation_id.int
                or any(not root.is_absolute() or root != root.resolve() for root in (state_root, anchor_dir, repo_root))):
            raise ValueError
        historical, initial = load_completed_history(operation_path=shared_operation,
            operation_sha256=shared_operation_sha256)
        publication = ProtectedPublication.model_validate(publication.model_dump())
        files = dict(historical.files)
        paths = (operator_connection.ca_file, operator_connection.credentials_file)
        if len(set(paths)) != len(paths) or set(paths) & files.keys():
            raise ValueError
        for path in paths:
            if not path.is_absolute() or path != path.resolve():
                raise ValueError
            files[path] = private_state._private_read(path, limit=16 * 1024**2)

        def unchanged() -> None:
            if any(path != path.resolve() or private_state._private_read(path, limit=16 * 1024**2) != raw
                    for path, raw in files.items()):
                raise ValueError

        marker = anchor_dir / (str(operation_id) + '-participant-rollout.json')
        if marker.exists() or marker.is_symlink():
            reference = ParticipantRolloutReference.model_validate(json.loads(private_state._private_read(marker))['reference'])
            if reference.operation_id != operation_id or reference.publication != publication:
                raise ValueError
            retained = load_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
            expected = load_participant_predecessor(initial=initial, retained=retained)
        else:
            # An unanchored interrupted preparation cannot be reinterpreted as
            # permission to capture newer state under the same operation ID.
            operation_state = state_root / str(operation_id)
            if operation_state.exists() or operation_state.is_symlink():
                raise ValueError
            has_head = any(path.exists() or path.is_symlink() for path in (
                state_root / 'latest.json', anchor_dir / 'participant-head.json'))
            expected = load_participant_head(initial=initial, state_root=state_root, anchor_dir=anchor_dir) if has_head else initial
            endpoint = historical.bindings.plan.staging.attachment.config['kubernetes_api_server'].rstrip('/')
            if operator_connection.endpoint != endpoint or os.environ.get('LOOM_DEPLOY_SSH_TARGET'):
                raise ValueError
            unchanged()
            context, token = asyncio.run(_transport(operator_connection))
            unchanged()
            with ManagementKubernetesTransport(api_server=endpoint, ssl_context=context, token=token) as transport:
                async def resolve() -> tuple[CandidateBundle, dict[str, bytes]]:
                    async with httpx.AsyncClient(timeout=30, follow_redirects=False, trust_env=False) as http:
                        async with resolve_participant_publication(completed=historical.bindings.plan.development.runtime,
                                publication=publication, keyring=expected.migration.registration.spec.profiles.image_admission_keyring,
                                transport=transport, http=http) as (catalog, bundle):
                            if not await catalog.candidate_follows(candidate_id=publication.candidate_id,
                                    current_sha=expected.migration.registration.candidate['candidate_sha']):
                                raise ValueError
                            candidate_service_schema_head(bundle.candidate)
                            source_files = await catalog.render_source(candidate_id=publication.candidate_id)
                            return bundle, source_files

                bundle, source_files = asyncio.run(resolve())
                unchanged()
                snapshot = capture_participant_release(predecessor=expected, bindings=historical.bindings,
                    shared_state=initial.state_dir, repo_root=repo_root, transport=transport)
            unchanged()
            profile = ServiceExecutionRuntimeProfileV1.model_validate(bundle.profile)
            source = PreparedDevelopmentSource(source_sha=bundle.candidate['candidate_sha'],
                source_archive_sha256=bundle.candidate['source_archive_sha256'])
            rendered = render_participant_candidate(bundle=bundle, source_files=source_files, config=snapshot.config,
                keyring=expected.migration.registration.spec.profiles.image_admission_keyring)
            if initialize_participant_head(initial=initial, state_root=state_root, anchor_dir=anchor_dir) != expected:
                raise ValueError
            retained = retain_participant_rollout(predecessor=expected, current=snapshot.current,
                config=snapshot.config, candidate=bundle.candidate, runtime_profile=profile,
                operation_id=operation_id, management_origin=snapshot.management_origin, repo_root=repo_root,
                rendered_files=rendered, source=source, publication=publication, manager=snapshot.manager,
                catalog=snapshot.catalog, state_root=state_root, anchor_dir=anchor_dir)
        unchanged()
        with authenticated_participant_release(retained=retained, initial=initial, expected=expected,
                bindings=historical.bindings, shared_state=initial.state_dir, operator_connection=operator_connection,
                executable=executable, operator_credentials=operator_connection.credentials_file,
                private_files=files) as delivery:
            delivery.qualifier(delivery.qualifier.kube, retained, 'preflight')
            unchanged()
        return retained
    except Exception:
        raise ValueError('participant preparation unqualified; preserve evidence') from None


def execute_retained_participant_release(*, reference: ParticipantRolloutReference,
        shared_operation: Path, shared_operation_sha256: str, state_root: Path, anchor_dir: Path,
        operator_connection: NebiusKubernetesConnection, executable: Path,
        automatic: bool = False, recovery: dict[str, Any] | None = None) -> dict[str, Any]:
    """Consume a reference using the protected host's original history and identity.

    The installed caller, not the runner, selects all paths and the current
    operator connection. A reference selects retained data only. This never
    prepares a new release, initializes a lost head, or recaptures cluster state.
    The returned evidence remains private until the gateway's bounded projection.
    """
    from scripts.ops.nebius_shared_pool_entry import load_completed_history

    try:
        historical, initial = load_completed_history(operation_path=shared_operation,
            operation_sha256=shared_operation_sha256)
        retained = load_participant_rollout(reference=reference, state_root=state_root, anchor_dir=anchor_dir)
        expected = load_participant_predecessor(initial=initial, retained=retained)
        files = dict(historical.files)
        paths = (operator_connection.ca_file, operator_connection.credentials_file)
        if len(set(paths)) != len(paths) or set(paths) & files.keys():
            raise ValueError
        for path in paths:
            if not path.is_absolute() or path != path.resolve():
                raise ValueError
            files[path] = private_state._private_read(path, limit=16 * 1024**2)
    except Exception:
        raise ValueError('participant host inputs unqualified; preserve evidence') from None

    def unchanged() -> None:
        try:
            if any(private_state._private_read(path, limit=16 * 1024**2) != raw for path, raw in files.items()):
                raise ValueError
        except Exception:
            raise ValueError('participant host material changed; preserve evidence') from None

    unchanged()
    with authenticated_participant_release(retained=retained, initial=initial, expected=expected,
            bindings=historical.bindings, shared_state=initial.state_dir, operator_connection=operator_connection,
            executable=executable, operator_credentials=operator_connection.credentials_file,
            private_files=files) as delivery:
        unchanged()
        result = delivery.execute(automatic=automatic, recovery=recovery)
        unchanged()
        return result


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
