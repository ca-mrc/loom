"""Read-only new-release capture from source and a semantic predecessor.

Only the protected host calls this after loading completed history. A capture is
not publication approval, guard authority or a recovery baseline. Recovery loads
the already retained release instead. No supplied document is applied here.
"""
from __future__ import annotations

import base64
import copy
import json
import sys
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from scripts.ops.deploy_nebius_platform import load_render
from scripts.ops.nebius_certificate_gateway import run_private
from scripts.ops.nebius_development_runtime_retained import (
    CompletedDevelopmentRuntime,
    qualify_build_policy_spec,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_participant_completion import CompletedParticipantRollout, _head_value
from scripts.ops.nebius_pool_participant_operation import ParticipantPredecessor
from scripts.ops.nebius_pool_participant_resource import _resource_path
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_staging_pool_attachment import _qualified_workload

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_platform_render import build_platform
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
    _json,
)
from loom_service.environment_management.manager import CandidateBundle

Document = dict[str, Any]


@dataclass(frozen=True, repr=False)
class ParticipantReleaseSnapshot:
    current: tuple[Document, ...]
    config: Document
    manager: Document
    catalog: Document
    management_origin: str


def render_participant_candidate(*, bundle: CandidateBundle, source_files: dict[str, bytes],
        config: Document, keyring: Document) -> dict[str, list[Document]]:
    """Run only the approved candidate's renderer/imports in a private directory.

    Inputs came from the installed publication reader, not runner file uploads.
    Do not inherit Python paths or operator credentials into the renderer. Its
    output remains subject to the existing deployment and shared-pool validators.
    """
    with tempfile.TemporaryDirectory(prefix='loom-participant-render-') as directory:
        root = Path(directory)
        for name, raw in source_files.items():
            path = root / name
            if (not path.is_relative_to(root) or '..' in path.parts or path.is_symlink()
                    or not (name.startswith(('deploy/k8s/', 'src/loom/'))
                        or name == 'scripts/ops/render_nebius_platform.py')):
                raise ValueError('candidate rendering source outside scope')
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        for name, value in {'environment': config, 'candidate': bundle.candidate,
                'runtime-profile': bundle.profile, 'keyring': keyring}.items():
            (root / (name + '.json')).write_text(json.dumps(value, allow_nan=False))
        run_private([sys.executable, '-I', str(root / 'scripts/ops/render_nebius_platform.py'),
            '--environment-config', str(root / 'environment.json'), '--candidate', str(root / 'candidate.json'),
            '--runtime-profile', str(root / 'runtime-profile.json'), '--trusted-keyring', str(root / 'keyring.json'),
            '--output', str(root / 'render'), '--regional-output', str(root / 'regional')], timeout=60)
        return load_render(root / 'render')[2]


@asynccontextmanager
async def resolve_participant_publication(*, completed: CompletedDevelopmentRuntime,
        publication: ProtectedPublication, keyring: Document,
        transport: ManagementKubernetesTransport, http: httpx.AsyncClient
        ) -> AsyncIterator[tuple[GitHubCandidateCatalog, CandidateBundle]]:
    """Resolve with the installed reader, pinning its identity across every read."""
    manager = completed.request.database.manager.retained.request.retained
    key = 'Secret:' + manager.binding.namespace + ':loom-management-publications'
    record = _json(manager.files[Path(manager.operation['state_dir']) / 'supplied/stage.json'])
    item = record['resources'][key]
    reader = copy.deepcopy(item['observed'])
    reader['metadata']['uid'] = item['uid']

    def unchanged() -> None:
        actual = transport._request('GET', _resource_path(reader))
        if actual is None or not _matches(actual, reader, _uid(reader)):
            raise ValueError

    unchanged()
    catalog = GitHubCandidateCatalog(http,
        token=base64.b64decode(reader['data']['token'], validate=True).decode(), publications=[publication],
        registry_prefix=completed.request.database.manager.deployment.installation.registry_prefix,
        keyring=ImageAdmissionKeyring.from_json(json.dumps(keyring)))
    bundle = await catalog.resolve(publication.candidate_id)
    unchanged()
    yield catalog, bundle
    unchanged()


def capture_participant_release(*, predecessor: ParticipantPredecessor,
        bindings: SharedPoolGuardBindings, shared_state: Path, repo_root: Path,
        transport: ManagementKubernetesTransport) -> ParticipantReleaseSnapshot:
    """Read the full selected render and preserve actual API-allocated identities.

    Initial handoff receipts cover its intentional changes, not every platform
    resource. Unchanged objects missing from those receipts must still match the
    old source render. Subsequent releases use their actual completed resources.
    A second read brackets every resource and immutable history before return.
    """
    from scripts.ops.nebius_shared_pool_forward_census import _forward_options

    try:
        original = _head_value(predecessor)
        spec = predecessor.migration.registration.spec
        binding = predecessor.migration.registration.binding
        participant, = (row for row in spec.participants if row.environment_class == 'staging')
        guard, = (row for row in predecessor.migration.guards if row.participant_id == participant.participant_id)
        runtime = bindings.plan.development.runtime
        frame = None
        if isinstance(predecessor, CompletedParticipantRollout):
            config = copy.deepcopy(predecessor.retained.inputs.config)
            known = copy.deepcopy(predecessor.resources)
            profile = predecessor.retained.inputs.runtime_profile
            manager, catalog = copy.deepcopy(predecessor.manager), copy.deepcopy(predecessor.catalog)
        else:
            attachment = bindings.plan.staging.attachment
            config, profile = copy.deepcopy(attachment.config), attachment.runtime_profile
            known = {_key(row): copy.deepcopy(row) for row in attachment.resources}
            frame = _forward_options(bindings, shared_state)
            for key, choices in {**frame[1], **frame[2]}.items():
                row, = choices
                if row is None:
                    # A completed handoff cannot leave its selected resources
                    # absent or in an uncertain before/after transition.
                    if key in known:
                        raise ValueError
                    continue
                _uid(row)
                known[key] = copy.deepcopy(row)
            key = 'Deployment:' + binding.namespace + ':loom-service'
            manager = copy.deepcopy(known.get(key, runtime.resources[key]))
            key = 'ConfigMap:' + binding.namespace + ':loom-pool-profiles-' + spec.operation_id.hex
            catalog = copy.deepcopy(runtime.resources[key])
        if (config['namespace'] != guard.namespace
                or config['execution_namespace'] != participant.execution_namespace.name
                or config['cluster_id'] != spec.cluster_id or profile.candidate_sha != predecessor.migration.registration.candidate['candidate_sha']
                or transport.api_server.rstrip('/') != config['kubernetes_api_server'].rstrip('/')):
            raise ValueError
        connection = (transport.api_server, transport.client, str(transport.client.base_url), dict(transport.client.headers))
        rendered = (copy.deepcopy(predecessor.retained.inputs.rendered_files)
            if isinstance(predecessor, CompletedParticipantRollout) else
            build_platform(config, predecessor.migration.registration.candidate,
                profile.model_dump(mode='json'), spec.profiles.image_admission_keyring, repo_root=repo_root))
        documents = {_key(row): row for rows in rendered.values() for row in rows if row['kind'] != 'Job'}
        if not 0 < len(documents) <= 32768 or any(row['kind'] == 'Secret' for row in documents.values()):
            raise ValueError
        # Only source-selected resources are retained, not extras from a live
        # namespace. Whole-pool writer census remains the separate qualifier.
        pinned = {key: known[key] for key in documents.keys() & known.keys()}
        selected_keys = set(documents)
        for row in (manager, catalog):
            key = _key(row)
            if key in documents:
                raise ValueError
            documents[key] = row
            pinned[key] = row

        def unchanged() -> None:
            if (_head_value(predecessor) != original
                    or (transport.api_server, transport.client, str(transport.client.base_url), dict(transport.client.headers)) != connection
                    or (frame is not None and _forward_options(bindings, shared_state) != frame)):
                raise ValueError

        def cluster() -> None:
            row = transport._request('GET', '/api/v1/namespaces/kube-system')
            if (row is None or row.get('apiVersion') != 'v1' or row.get('kind') != 'Namespace'
                    or row['metadata'].get('name') != 'kube-system' or _uid(row) != binding.kube_system_uid):
                raise ValueError
            _snapshot(row)

        unchanged()
        cluster()
        captured = {}
        for key, desired in documents.items():
            actual = transport._request('GET', _resource_path(desired))
            if actual is None or _key(actual) != key:
                raise ValueError
            _uid(actual)
            _snapshot(actual)
            if key in pinned:
                if not _matches(actual, pinned[key], _uid(pinned[key])):
                    raise ValueError
            else:
                _qualified_workload(desired, actual)
                if desired['kind'] in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
                    qualify_build_policy_spec(desired, actual)
            captured[key] = actual
        if len({_uid(row) for row in captured.values()}) != len(captured):
            raise ValueError
        for row in captured.values():
            actual = transport._request('GET', _resource_path(row))
            if actual is None or not _matches(actual, row, _uid(row)):
                raise ValueError
        cluster()
        unchanged()
        return ParticipantReleaseSnapshot(tuple(copy.deepcopy(captured[key]) for key in sorted(selected_keys)), config,
            captured[_key(manager)], captured[_key(catalog)], 'https://' + runtime.request.database.manager.deployment.public_host)
    except Exception:
        raise ValueError('participant preparation baseline unqualified; preserve evidence') from None
