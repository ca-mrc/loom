"""Read-only independent environment process and data qualification.

This closes the application-side readiness boundary. Platform writer, observer,
gateway and physical-capacity evidence are separate requirements for activation.
No worker quota, namespace security, participant state or workload is changed.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import ssl
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import httpx
from scripts.ops import nebius_certificates as state
from scripts.ops.nebius_environment_foundation import FoundationCloud
from scripts.ops.nebius_environment_foundation import _load as load_foundation
from scripts.ops.nebius_environment_runtime_install import _observe, _Resources
from scripts.ops.nebius_environment_runtime_probes import (
    EnvironmentComponent,
    EnvironmentRuntimeProbes,
)
from scripts.ops.nebius_environment_runtime_start import (
    load_started_environment_runtime,
    runtime_resource_path,
)
from scripts.ops.nebius_foundation_readiness import observe_foundation
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_administration_dispatch import (
    Connection,
    PoolAdministrationReferenceV2,
)
from scripts.ops.nebius_pool_enrollment_issuer import (
    PoolEnrollmentPolicyV2,
    _policy,
    _published_profiles,
)
from scripts.ops.nebius_pool_migration_guard import _BOUND_DATABASE_COMMAND
from scripts.ops.nebius_pool_runtime_settings import BOUND_POOL_SETTINGS_COMMAND

from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2
from loom.nebius_kubernetes import NebiusKubernetesCredentials
from loom_service.environment_management.candidates import GitHubCandidateCatalog, _json
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.inspection import PoolInspectionV2, qualify_inspection

ProbeConnection = Callable[[Callable[[], None], str], AbstractContextManager[EnvironmentRuntimeProbes]]


class FixedProcessTransport:
    """Ephemeral explicit-trust kubectl configuration, only fixed read probes."""
    def __init__(self, *, endpoint: str, trust: ssl.SSLContext, token: str, qualify: Callable[[], None]):
        self.qualify = qualify
        executable = shutil.which('kubectl', path='/usr/local/bin' + os.pathsep + os.defpath)
        if (executable is None or not token or trust.verify_mode != ssl.CERT_REQUIRED or not trust.check_hostname
                or not endpoint.startswith('https://')):
            raise ValueError('environment_runtime_probe_transport_unavailable')
        certificates = ''.join(ssl.DER_cert_to_PEM_cert(row) for row in trust.get_ca_certs(binary_form=True))
        if not certificates:
            raise ValueError('environment_runtime_probe_transport_unavailable')
        self.scratch = tempfile.TemporaryDirectory(prefix='loom-environment-probe-')
        try:
            root = Path(self.scratch.name)
            self.path = root / 'connection.json'
            self.raw = json.dumps({'apiVersion': 'v1', 'kind': 'Config', 'current-context': 'probe',
                'clusters': [{'name': 'probe', 'cluster': {'server': endpoint,
                    'certificate-authority-data': base64.b64encode(certificates.encode()).decode()}}],
                'users': [{'name': 'probe', 'user': {'token': token}}],
                'contexts': [{'name': 'probe', 'context': {'cluster': 'probe', 'user': 'probe'}}]}, sort_keys=True).encode()
            if len(self.raw) > 2 * 1024**2:
                raise ValueError
            state._write_private(self.path, self.raw)
            self.prefix = [str(Path(executable).resolve()), '--kubeconfig', str(self.path),
                '--request-timeout=30s', '--cache-dir', str(root / 'cache')]
        except Exception:
            self.scratch.cleanup()
            raise ValueError('environment_runtime_probe_transport_unavailable') from None

    def close(self) -> None:
        self.scratch.cleanup()

    def _check(self) -> None:
        self.qualify()
        if state._private_read(self.path, limit=2 * 1024**2) != self.raw:
            raise ValueError('environment_runtime_probe_connection_changed')

    def execute(self, pod: dict[str, Any], container: str, command: list[str]) -> dict[str, Any]:
        try:
            if (len(command) != 6 or command[:2] != ['python', '-c']
                    or command[2] not in {BOUND_POOL_SETTINGS_COMMAND, _BOUND_DATABASE_COMMAND}
                    or command[3] not in {'service', 'controller', 'actuator'}
                    or any(re.fullmatch('[0-9a-f]{64}', item) is None for item in command[4:])
                    or any(re.fullmatch('[a-z0-9][a-z0-9.-]{0,252}', item) is None for item in (
                        pod['metadata']['namespace'], pod['metadata']['name'], container))):
                raise ValueError
            _uid(pod)
            self._check()
            result = subprocess.run([*self.prefix, 'exec', '-n', pod['metadata']['namespace'],
                'pod/' + pod['metadata']['name'], '-c', container, '--', *command],
                capture_output=True, timeout=40, check=False, env={'PATH': os.defpath, 'LANG': 'C.UTF-8'})
            self._check()
            if result.returncode or result.stderr or len(result.stdout) > 65536:
                raise ValueError
            return _json(result.stdout)
        except Exception:
            raise ValueError('environment_runtime_probe_transport_unqualified') from None


async def observe_environment_runtime(*, policy: PoolEnrollmentPolicyV2, operation_id: UUID,
        catalog: GitHubCandidateCatalog, kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud,
        public_http: httpx.AsyncClient, connect: Connection, probe_connection: ProbeConnection | None = None) -> dict[str, Any] | None:
    """Fresh process/data readback from anchored completed startup, not activation."""
    try:
        retained = load_started_environment_runtime(policy=policy, operation_id=operation_id)
        staged = retained.staged
        observation = await asyncio.to_thread(_observe, policy, staged.state_dir / 'readiness-observations', connect)
        if observation is None:
            return None
        snapshot = qualify_inspection(observation['report']['receipt'], PoolInspectionV2.model_validate(observation['input']['request']),
            UUID(observation['input']['authorization']['authorization_id']))
        enrollment = PoolAdministrationReferenceV2.model_validate(staged.plan['enrollment'])
        selected = snapshot.participant
        if (selected is None or selected.binding != policy.participant or selected.profiles != policy.profiles
                or selected.head.state != 'enrolled' or selected.head.operation_id != enrollment.operation_id
                or str(kubernetes.http.base_url).rstrip('/') != policy.host.connection.endpoint.rstrip('/')):
            raise ValueError
        if any(catalog.publications.get(row.candidate_id) != row for row in (policy.application_publication, policy.manager_publication)):
            raise ValueError
        bundles = {row.candidate_id: await catalog.resolve(row.candidate_id)
            for row in (policy.application_publication, policy.manager_publication)}
        app = bundles[policy.application_publication.candidate_id]
        plan = load_foundation(policy.foundation_dir, policy.foundation)
        declaration = EnvironmentFoundationDeclarationV2.model_validate(plan['declaration'])
        if (digest(app.candidate) != plan['publication_sha256'] or digest(app.profile) != plan['profile_sha256']
                or policy.host.runtime.service_image != bundles[policy.manager_publication.candidate_id].candidate['images']['service']['image_ref']):
            raise ValueError
        _published_profiles(policy, app, catalog, plan)
        successors: dict[str, tuple[dict[str, Any], ...]] = {'k8s:' + key: (doc,) for key, doc in retained.resources.items()
            if 'k8s:' + key in staged.plan['foundation_identities']}
        foundation = await observe_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
            kubernetes=kubernetes, cloud=cloud, public_http=public_http, _runtime_successors=successors)
        if foundation.foundation.identities != staged.plan['foundation_identities']:
            raise ValueError
        files = {**retained.files, policy.foundation_dir / 'material.json': state._private_read(policy.foundation_dir / 'material.json', limit=8 * 1024**2)}
        material = _json(files[policy.foundation_dir / 'material.json'])
        if digest(material) != foundation.foundation.identities['credentials:material']:
            raise ValueError
        expires = min(snapshot.valid_until, foundation.foundation.observed_at + timedelta(minutes=5), *(row.expires_at for row in policy.machines))
        def qualify() -> None:
            if (not snapshot.observed_at <= datetime.now(UTC) < expires or _policy(policy) != staged.plan['policy']
                    or any(state._private_read(path, limit=8 * 1024**2) != raw for path, raw in files.items())):
                raise ValueError('environment_runtime_readiness_evidence_changed')
        async def resources() -> None:
            for doc in retained.resources.values():
                actual = await kubernetes._request('GET', runtime_resource_path(doc) + '/' + doc['metadata']['name'])
                if actual is None or not _matches(actual, doc, _uid(doc)):
                    raise ValueError('environment_runtime_resource_changed')
            qualify()
        await resources()
        machine, = (row for row in policy.machines if row.workload_scope == 'environment')
        @contextmanager
        def connection(check: Callable[[], None], region: str) -> Iterator[EnvironmentRuntimeProbes]:
            credentials = NebiusKubernetesCredentials(policy.host.connection)
            private = {path: state._private_read(path, limit=1024**2)
                for path in (policy.host.connection.ca_file, policy.host.connection.credentials_file)}
            def unchanged() -> None:
                check()
                if any(state._private_read(path, limit=1024**2) != raw for path, raw in private.items()):
                    raise ValueError('environment_runtime_probe_authority_changed')
            try:
                with _Resources(policy=policy, documents=retained.resources, namespaces=staged.plan['namespace_uids'],
                        qualify=unchanged, credentials=credentials) as api:
                    def read(path: str) -> dict[str, Any]:
                        api.verify_identity(policy.host.runtime.binding)
                        value = api._request('GET', path)
                        if value is None:
                            raise ValueError('environment_runtime_resource_missing')
                        return value
                    transport = FixedProcessTransport(endpoint=policy.host.connection.endpoint, trust=credentials.ssl_context,
                        token=credentials.get_token_sync(), qualify=unchanged)
                    try:
                        yield EnvironmentRuntimeProbes(read=read, execute=transport.execute, qualify=unchanged, region=region)
                    finally:
                        transport.close()
            finally:
                asyncio.run(credentials.close())
        factory = probe_connection or connection
        def inspect() -> list[dict[str, Any]]:
            result = []
            with factory(qualify, declaration.platform_config['region']) as probes:
                for name, desired in staged.plan['workloads'].items():
                    component = cast(EnvironmentComponent, {'service': 'service', 'control_plane': 'controller'}.get(name, 'actuator'))
                    workload = retained.resources[_key(desired)]
                    variable = {'service': 'LOOM_SVC_DB_URL', 'controller': 'LOOM_CP_DB_URL', 'actuator': 'LOOM_EXECUTION_ACTUATOR_DB_URL'}[component]
                    container, = workload['spec']['template']['spec']['containers']
                    row, = (row for row in container['env'] if row['name'] == variable)
                    reference = row['valueFrom']['secretKeyRef']
                    if (container.get('envFrom') or set(row) != {'name', 'valueFrom'} or set(row['valueFrom']) != {'secretKeyRef'}
                            or set(reference) != {'name', 'key'}):
                        raise ValueError
                    namespace = workload['metadata']['namespace']
                    if namespace == policy.foundation.identity.application_namespace:
                        url = material[reference['name']][reference['key']]
                    else:
                        secret = retained.resources['Secret:' + namespace + ':' + reference['name']]
                        url = base64.b64decode(secret['data'][reference['key']], validate=True).decode()
                    result.append(probes.qualify_process(workload=workload, component=component,
                        database_namespace=policy.foundation.identity.application_namespace, database_url=url,
                        token_sha256=machine.token_sha256 if component != 'service' else None))
            return result
        processes = await asyncio.to_thread(inspect)
        after = await observe_foundation(state_dir=policy.foundation_dir, reference=policy.foundation,
            kubernetes=kubernetes, cloud=cloud, public_http=public_http, _runtime_successors=successors)
        if (after.foundation.identities != foundation.foundation.identities or after.database_pod_uid != foundation.database_pod_uid
                or after.database_volume_name != foundation.database_volume_name):
            raise ValueError
        await resources()
        def recheck() -> None:
            with factory(qualify, declaration.platform_config['region']) as probes:
                for workload, proof in zip(staged.plan['workloads'].values(), processes, strict=True):
                    probes.recheck_process(workload=retained.resources[_key(workload)], proof=proof)
        await asyncio.to_thread(recheck)
        qualify()
        return {'status': 'runtime_processes_verified_closed', 'participant_id': str(policy.participant.participant_id),
            'operation_id': str(operation_id), 'admission_open': False, 'processes': processes,
            'observed_at': foundation.foundation.observed_at.isoformat(), 'valid_until': expires.isoformat(),
            'foundation': after.model_dump(mode='json'), 'inspection': observation, 'runtime_sha256': digest(staged.plan)}
    except Exception:
        raise ValueError('environment_runtime_readiness_unqualified') from None
