"""Read-only, exact-publication successor for a restored legacy participant.

A completed ordinary deployment is not a pool completion. Its protected run and
backup/readiness result select source for a renderer projection; retained pool
UIDs, credentials and live authority still require their existing qualifiers.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import re
import stat
import zipfile
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_ingress_stage import _key

from loom.repository_identity import GITHUB_API_ROOT, is_loom_repository
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
    _object,
    _require,
)

_LIMIT = 64 * 1024**2


class OrdinaryPlatformRollout(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    deployment_id: int = Field(gt=0, strict=True)
    publication: ProtectedPublication


@dataclass(frozen=True, repr=False)
class PlatformRolloutSuccessor:
    reference: OrdinaryPlatformRollout
    config: dict[str, Any]
    resources: dict[str, dict[str, Any]]


async def _read(catalog: GitHubCandidateCatalog, path: str) -> Any:
    raw, redirect = await catalog._read(GITHUB_API_ROOT + path, api=True, limit=2 * 1024**2)
    _require(redirect is None)
    return json.loads(raw, object_pairs_hook=_object)


async def resolve_platform_rollout(*, reference: OrdinaryPlatformRollout,
        catalog: GitHubCandidateCatalog, config: dict[str, Any], keyring: dict[str, Any],
        previous_sha: str) -> PlatformRolloutSuccessor:
    """Authenticate one complete ordinary run; render its exact published source."""
    from scripts.ops.nebius_candidate_render import render_candidate_source

    try:
        deployment = await _read(catalog, f'deployments/{reference.deployment_id}')
        payload = deployment['payload']
        source = reference.publication.source_sha
        scope = {key: config[key] for key in ('cluster_id', 'namespace', 'execution_namespace')}
        _require(deployment['id'] == reference.deployment_id
            and deployment['environment'] == 'nebius-integration' and deployment['sha'] == source
            and payload['schema_version'] == 'loom.nebius-deployment.v1' and payload['mode'] == 'apply'
            and payload['candidate_sha'] == source and payload.get('shared_pool_release') is None
            and all(payload[key] == value for key, value in scope.items()))
        run_id, attempt = payload['run_id'], payload['run_attempt']
        _require(type(run_id) is int and run_id > 0 and type(attempt) is int and attempt > 0)
        statuses = await _read(catalog, f'deployments/{reference.deployment_id}/statuses?per_page=1')
        _require(isinstance(statuses, list) and len(statuses) == 1 and statuses[0]['state'] == 'success')
        run = await _read(catalog, f'actions/runs/{run_id}/attempts/{attempt}')
        _require(run['id'] == run_id and run['run_attempt'] == attempt
            and run['status'] == 'completed' and run['conclusion'] == 'success'
            and run['head_branch'] == 'dev' and run['path'] == '.github/workflows/nebius-rollout.yml'
            and run['event'] in {'workflow_run', 'workflow_dispatch', 'schedule'}
            and is_loom_repository(run['repository']) and is_loom_repository(run['head_repository']))
        artifacts = await _read(catalog, f'actions/runs/{run_id}/artifacts?per_page=100')
        _require(type(artifacts['total_count']) is int and artifacts['total_count'] <= 100)
        artifact, = (row for row in artifacts['artifacts']
            if row['name'] == f'nebius-rollout-{run_id}-{attempt}')
        origin = artifact['workflow_run']
        _require(artifact['expired'] is False and 0 < artifact['size_in_bytes'] <= _LIMIT
            and origin['id'] == run_id and origin['head_sha'] == run['head_sha']
            and origin['repository_id'] == run['repository']['id']
            and origin['head_repository_id'] == run['repository']['id'])
        raw, redirect = await catalog._read(GITHUB_API_ROOT + f"actions/artifacts/{artifact['id']}/zip",
            api=True, limit=_LIMIT)
        if redirect is not None:
            url = httpx.URL(redirect)
            _require(url.scheme == 'https' and not url.userinfo and url.port in {None, 443}
                and url.host.endswith(('.blob.core.windows.net', '.actions.githubusercontent.com')))
            raw, second = await catalog._read(str(url), api=False, limit=_LIMIT)
            _require(second is None)
        _require('sha256:' + hashlib.sha256(raw).hexdigest() == artifact['digest'])
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            _require(len(archive.infolist()) <= 1000)
            record, = (row for row in archive.infolist()
                if re.fullmatch(r'deployment-[0-9a-f]{32}\.json', row.filename))
            _require(0 < record.file_size <= 1024**2 and not record.flag_bits & 1
                and not stat.S_ISLNK(record.external_attr >> 16))
            result = json.loads(archive.read(record), object_pairs_hook=_object)
        phases = {row['name'] for row in result['phases']}
        _require(result['schema_version'] == payload['schema_version'] and result['mode'] == 'apply'
            and result['status'] == 'complete' and result['candidate_sha'] == source
            and result.get('shared_pool_release') is None
            and result['previous_candidate_sha'] == payload['previous_candidate_sha']
            and all(result[key] == value for key, value in scope.items())
            and {'pre-upgrade-backup', 'public-https-smoke', 'candidate-readback'} <= phases
            and re.fullmatch('loom-predeploy-' + source[:12] + r'-[0-9a-f]{8}', result['backup_job']) is not None)
        bundle = await catalog.resolve(reference.publication.candidate_id)
        _require(bundle.candidate['candidate_sha'] == source
            and await catalog.candidate_follows(candidate_id=reference.publication.candidate_id,
                current_sha=previous_sha))
        files = await catalog.render_source(candidate_id=reference.publication.candidate_id)
        rendered = render_candidate_source(bundle=bundle, source_files=files, config=config, keyring=keyring)
        resources = {_key(row): copy.deepcopy(row) for rows in rendered.values() for row in rows}
        return PlatformRolloutSuccessor(reference, copy.deepcopy(config), resources)
    except Exception:
        raise ValueError('ordinary_platform_rollout_unqualified') from None
