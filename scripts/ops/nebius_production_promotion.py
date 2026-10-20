"""Read-only qualification of exact-target production promotion authority.

Publication authenticity, target release evidence and protected main admission
are separate checks. No runner claim, local checkout or image rebuild grants
production authority. Older promoted ancestors remain usable for exact rollback.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import re
import stat
import time
import zipfile
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from scripts.ops.nebius_pool_administration_dispatch import PoolAdministrationScopeV2
from scripts.ops.nebius_production_release_gate import validate_target
from scripts.ops.release_gate import validate_manifest

from loom.nebius_candidate_contract import NEBIUS_PLATFORM_IMAGES
from loom.repository_identity import (
    GITHUB_API_ROOT,
    REPOSITORY_ID,
    is_loom_repository,
    is_repository_name,
)
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
    _json,
)
from loom_service.pool_management.capacity import digest

_MAX_ARTIFACT = 4 * 1024**2
_AUTHORITY: ContextVar[tuple[tuple[QualifiedProductionPromotionV2, ProductionTargetV2, float], ...]] = ContextVar(
    'loom_qualified_production_authority', default=())


class _Strict(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)


class ProductionTargetV2(_Strict):
    scope: PoolAdministrationScopeV2
    namespace: str
    execution_namespace: str
    database_identity: str
    database_name: str
    provider_connection_namespace: str
    public_url: str
    object_storage: dict[str, str]
    secret_ref_prefix: str

    @model_validator(mode='after')
    def target(self) -> ProductionTargetV2:
        validate_target(self.model_dump(mode='json'), authority=self.scope.authority)
        return self


class PromotionArtifactV2(_Strict):
    run_id: int = Field(gt=0, strict=True)
    run_attempt: int = Field(gt=0, strict=True)
    artifact_id: int = Field(gt=0, strict=True)
    artifact_sha256: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')


class ProductionPromotionV2(_Strict):
    schema_version: Literal['loom.production-promotion.v2']
    target: ProductionTargetV2
    staging: ProductionTargetV2
    publication: ProtectedPublication
    release_sha: str = Field(pattern=r'^[0-9a-f]{40}$')
    release_pull_request: int = Field(gt=0, strict=True)
    release_gate: PromotionArtifactV2
    main_gate: PromotionArtifactV2
    image_tag: str = Field(min_length=1, max_length=128)
    prod_tag: str = Field(pattern=r'^v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$')

    @model_validator(mode='after')
    def authorities(self) -> ProductionPromotionV2:
        if (self.target.scope.authority != 'production' or self.staging.scope.authority != 'staging'
                or self.main_gate.run_id == self.release_gate.run_id
                or self.main_gate.artifact_id == self.release_gate.artifact_id):
            raise ValueError('production_promotion_authority_unqualified')
        return self


class QualifiedProductionPromotionV2(_Strict):
    schema_version: Literal['loom.qualified-production-promotion.v2'] = 'loom.qualified-production-promotion.v2'
    scope: PoolAdministrationScopeV2
    publication: ProtectedPublication
    release_sha: str
    promotion_sha256: str
    evidence_sha256: str
    image_digests: dict[str, str]


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError('production_promotion_unqualified')


async def _artifact(catalog: GitHubCandidateCatalog, reference: PromotionArtifactV2, *,
        candidate_sha: str, workflow: str, name: str, filename: str) -> dict[str, Any]:
    run, artifact, workflow_row = await asyncio.gather(
        catalog._api(f'actions/runs/{reference.run_id}/attempts/{reference.run_attempt}'),
        catalog._api(f'actions/artifacts/{reference.artifact_id}'),
        catalog._api('actions/workflows/' + workflow))
    _require(run['id'] == reference.run_id and run['run_attempt'] == reference.run_attempt
        and run['status'] == 'completed' and run['conclusion'] == 'success' and run['event'] == 'workflow_dispatch'
        and run['head_sha'] == candidate_sha and run['head_branch'] == 'dev'
        and is_loom_repository(run['repository']) and is_loom_repository(run['head_repository'])
        and run['path'] == workflow_row['path'] == '.github/workflows/' + workflow
        and type(workflow_row['id']) is int and workflow_row['id'] > 0 and run['workflow_id'] == workflow_row['id'])
    origin = artifact['workflow_run']
    _require(artifact['id'] == reference.artifact_id and artifact['name'] == name and artifact['expired'] is False
        and artifact['digest'] == reference.artifact_sha256 and type(artifact['size_in_bytes']) is int
        and 0 < artifact['size_in_bytes'] <= _MAX_ARTIFACT and origin['id'] == reference.run_id
        and origin['head_sha'] == candidate_sha and origin['head_branch'] == 'dev'
        and origin['repository_id'] == origin['head_repository_id'] == REPOSITORY_ID)
    payload, redirect = await catalog._read(GITHUB_API_ROOT + f'actions/artifacts/{reference.artifact_id}/zip',
        api=True, limit=_MAX_ARTIFACT)
    if redirect is not None:
        url = httpx.URL(redirect)
        _require(url.scheme == 'https' and not url.userinfo and url.port in {None, 443}
            and url.host.endswith(('.blob.core.windows.net', '.actions.githubusercontent.com')))
        payload, second = await catalog._read(str(url), api=False, limit=_MAX_ARTIFACT)
        _require(second is None)
    _require('sha256:' + hashlib.sha256(payload).hexdigest() == reference.artifact_sha256)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        entries = archive.infolist()
        allowed = ({'release-gate-evidence.json', 'release-gate-evidence.md', 'release-gate-input.json'}
            if filename == 'release-gate-evidence.json' else {'main-promotion-evidence.json'})
        names = [entry.filename for entry in entries]
        _require(len(names) == len(set(names)) and filename in names and set(names) <= allowed
            and all(0 < entry.file_size <= 1024**2 and not entry.flag_bits & 1
                and stat.S_IFMT(entry.external_attr >> 16) in {0, stat.S_IFREG} for entry in entries))
        report = _json(archive.read(filename))
    _require(report['gate_run'] == {'run_id': reference.run_id, 'run_attempt': reference.run_attempt})
    return report


async def qualify_production_promotion(*, promotion: ProductionPromotionV2, scope: PoolAdministrationScopeV2,
        publication: ProtectedPublication, catalog: GitHubCandidateCatalog) -> QualifiedProductionPromotionV2:
    """Re-observe independently pinned production authority before protected writes."""
    try:
        promotion = ProductionPromotionV2.model_validate(promotion.model_dump())
        _require(scope.authority == 'production' and promotion.target.scope == scope
            and promotion.publication == publication and catalog.publications.get(publication.candidate_id) == publication)
        bundle, reports = await asyncio.gather(
            catalog.resolve(publication.candidate_id),
            asyncio.gather(
                _artifact(catalog, promotion.release_gate, candidate_sha=publication.source_sha,
                    workflow='release-promotion-gate.yml', name='release-gate-evidence', filename='release-gate-evidence.json'),
                _artifact(catalog, promotion.main_gate, candidate_sha=publication.source_sha,
                    workflow='main-promotion-gate.yml',
                    name=f'main-promotion-evidence-{promotion.main_gate.run_id}-{promotion.main_gate.run_attempt}',
                    filename='main-promotion-evidence.json'),
                catalog._api(f'pulls/{promotion.release_pull_request}'),
                catalog._api(f'git/commits/{publication.source_sha}'),
                catalog._api(f'git/commits/{promotion.release_sha}'), catalog._api('branches/main')))
        release_report, main_report, pull, candidate_commit, release_commit, main = reports
        _require(pull['number'] == promotion.release_pull_request and pull['merged'] is True and pull['state'] == 'closed'
            and pull['base']['ref'] == 'main' and pull['head']['ref'] == 'dev'
            and is_loom_repository(pull['base']['repo']) and is_loom_repository(pull['head']['repo'])
            and pull['head']['sha'] == publication.source_sha and pull['merge_commit_sha'] == promotion.release_sha)
        tree = candidate_commit['tree']['sha']
        _require(candidate_commit['sha'] == publication.source_sha and release_commit['sha'] == promotion.release_sha
            and isinstance(tree, str) and re.fullmatch(r'[0-9a-f]{40}', tree) is not None and release_commit['tree']['sha'] == tree)
        head = main['commit']['sha']
        _require(isinstance(head, str) and re.fullmatch(r'[0-9a-f]{40}', head) is not None)
        ancestry = await catalog._api(f'compare/{promotion.release_sha}...{head}?per_page=1')
        _require(ancestry['base_commit']['sha'] == ancestry['merge_base_commit']['sha'] == promotion.release_sha
            and type(ancestry['ahead_by']) is int and type(ancestry['behind_by']) is int and ancestry['behind_by'] == 0
            and ((head == promotion.release_sha and ancestry['status'] == 'identical' and ancestry['ahead_by'] == 0)
                or (head != promotion.release_sha and ancestry['status'] == 'ahead' and ancestry['ahead_by'] > 0)))
        expected_main = dict(schema_version='loom.main-promotion-evidence.v2', candidate_sha=publication.source_sha,
            dev_head=publication.source_sha, pr_number=promotion.release_pull_request,
            release_gate_run_id=promotion.release_gate.run_id, release_gate_artifact_id=promotion.release_gate.artifact_id,
            source='dev', target='main', gate_run={'run_id': promotion.main_gate.run_id, 'run_attempt': promotion.main_gate.run_attempt})
        _require(set(main_report) == set(expected_main) | {'repository'} and is_repository_name(main_report['repository'])
            and all(main_report[key] == value for key, value in expected_main.items()))
        _require(release_report['schema_version'] == 2 and release_report['status'] == 'pass'
            and release_report['targets'] == {'production': promotion.target.model_dump(mode='json'), 'staging': promotion.staging.model_dump(mode='json')}
            and release_report['prod_tag'] == promotion.prod_tag
            and not validate_manifest(release_report, candidate_sha=publication.source_sha, image_tag=promotion.image_tag))
        images = {repository: bundle.candidate['images'][component]['image_ref'] for component, repository in NEBIUS_PLATFORM_IMAGES.items()}
        _require(bundle.candidate_id == publication.candidate_id and bundle.candidate['candidate_sha'] == publication.source_sha
            and release_report['image_digests'] == images)
        return QualifiedProductionPromotionV2(scope=scope, publication=publication, release_sha=promotion.release_sha,
            promotion_sha256=digest(promotion.model_dump(mode='json')), evidence_sha256=digest(release_report), image_digests=images)
    except Exception:
        raise ValueError('production_promotion_unqualified') from None


@asynccontextmanager
async def production_authority(*, promotion: ProductionPromotionV2, scope: PoolAdministrationScopeV2,
        publication: ProtectedPublication, catalog: GitHubCandidateCatalog) -> AsyncIterator[QualifiedProductionPromotionV2]:
    """Only fresh remote qualification installs this task-local capability."""
    proof = await qualify_production_promotion(promotion=promotion, scope=scope, publication=publication, catalog=catalog)
    token = _AUTHORITY.set((*_AUTHORITY.get(), (proof, promotion.target, time.monotonic() + 300)))
    try:
        yield proof
    finally:
        _AUTHORITY.reset(token)


def require_production_authority(*, scope: PoolAdministrationScopeV2, publication: ProtectedPublication) -> ProductionTargetV2:
    """No model, boolean or caller promise substitutes for the active qualifier."""
    for retained in reversed(_AUTHORITY.get()):
        if (time.monotonic() < retained[2] and retained[0].scope == scope and retained[0].publication == publication):
            return retained[1]
    raise ValueError('production_promotion_required')


def require_production_effect(scope: PoolAdministrationScopeV2) -> None:
    """Recheck every selected publication immediately before an installed write.

    The installed parent first qualifies the exact baseline/current/replacement
    set. Contexts propagate through asyncio.to_thread; an absent or expired
    production context cannot silently turn a worker into an unguarded writer.
    """
    if scope.authority != 'production':
        return
    selected = [row for row in _AUTHORITY.get() if row[0].scope == scope]
    if not selected or any(time.monotonic() >= row[2] for row in selected):
        raise ValueError('production_promotion_required')


def production_request_hook(scope: PoolAdministrationScopeV2) -> Callable[[httpx.Request], None]:
    """Also cover runtime patchers that stream directly through the HTTP client."""
    def check(request: httpx.Request) -> None:
        if request.method not in {'GET', 'HEAD', 'OPTIONS'}:
            require_production_effect(scope)
    return check


def require_production_foundation(*, scope: PoolAdministrationScopeV2, publication: ProtectedPublication,
        declaration: Any) -> None:
    """Bind the actual independent database, namespaces, storage and public route."""
    target = require_production_authority(scope=scope, publication=publication)
    row, config = declaration.registration, declaration.platform_config
    _require(target.namespace == row.application_namespace and target.execution_namespace == row.execution_namespace
        and target.public_url == 'https://' + row.public_host and target.database_name == 'loom'
        and target.database_identity == f'kubernetes:{row.cluster_id}/{row.application_namespace}/loom-postgres/loom'
        and target.object_storage == {'task_bucket': config['buckets']['artifacts'],
            'artifacts_bucket': config['buckets']['artifacts'], 'trajectories_bucket': config['buckets']['trajectories']})
