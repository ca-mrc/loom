"""Catalog-bound fresh-dev runtime targets; no mutation or activation authority."""
from __future__ import annotations

import copy
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_pool_retained import (
    RetainedDevelopmentPoolReference,
    RetainedDevelopmentPoolState,
    load_retained_pool,
)
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_pool_application_delivery import (
    RenderedApplicationBuildDelivery,
    derive_application_build_deployment,
    render_application_build_delivery,
)

from loom.repository_identity import is_repository_name
from loom_service.environment_management.candidates import ProtectedPublication, _json
from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.environment_management.manager import CandidateBundle

_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, repr=False)
class DevelopmentRuntimePublication:
    """Source-bound target; live callers must resolve it via GitHubCandidateCatalog.

    Neither constructing this object nor matching its fields proves publication
    approval. The protected parent rechecks the exact publication before writes.
    """

    source: PreparedDevelopmentSource
    publication: ProtectedPublication
    bundle: CandidateBundle

    def validate(self, registry_prefix: str) -> DevelopmentRuntimePublication:
        source = PreparedDevelopmentSource.model_validate(self.source.model_dump())
        selected = ProtectedPublication.model_validate(self.publication.model_dump())
        bundle = copy.deepcopy(self.bundle)
        candidate, profile = bundle.candidate, bundle.profile
        if (not selected.candidate_id.int or bundle.candidate_id != selected.candidate_id
                or source.source_sha != selected.source_sha
                or candidate.get('schema_version') != 'loom.nebius-candidate.v1'
                or not is_repository_name(candidate.get('repository'))
                or candidate.get('source_ref') != 'refs/heads/dev'
                or candidate.get('candidate_sha') != source.source_sha
                or candidate.get('source_archive_sha256') != source.source_archive_sha256
                or candidate.get('run_id') != selected.run_id
                or candidate.get('registry_prefix') != registry_prefix
                or profile.get('candidate_sha') != source.source_sha):
            raise ValueError('development runtime publication differs')
        return DevelopmentRuntimePublication(source, selected, bundle)


@dataclass(frozen=True, repr=False)
class DevelopmentManagerRuntime:
    retained: RetainedDevelopmentPoolState
    original: dict[str, Any]
    deployment: ManagementDeployment
    delivery: RenderedApplicationBuildDelivery
    requires_source_material: bool
    publication: DevelopmentRuntimePublication


def prepare_manager_runtime(reference: RetainedDevelopmentPoolReference, *,
        publication: DevelopmentRuntimePublication) -> DevelopmentManagerRuntime:
    """Derive one stopped successor from actual completed installation history.

    Existing source-only material keeps its original immutable Secret identity.
    Otherwise the protected delivery must qualify and deliver the derived source
    Secret before starting this target. The boolean is a derived prerequisite,
    never an operator assertion that credentials, grants or runtime are ready.
    """
    try:
        retained = load_retained_pool(reference)
        manager = retained.request.retained
        before = manager.inputs.deployment
        publication = publication.validate(before.installation.registry_prefix)
        spec = retained.request.registration.spec
        state = Path(manager.operation['state_dir'])
        record = _json(manager.files[state / 'service/stage.json'])
        item = record['resources']['Deployment:' + manager.binding.namespace + ':loom-service']
        original = copy.deepcopy(item['observed'])
        original['metadata']['uid'] = item['uid']
        after = derive_application_build_deployment(before, spec)
        delivery = render_application_build_delivery(before=before, pool=spec, active=original,
            candidate=publication.bundle.candidate, profile=publication.bundle.profile, repo_root=_ROOT)
        application = before.installation.applications
        assert application is not None
        requires_source = application.runtime.source_upload is None
        if not requires_source:
            old, = (row for row in original['spec']['template']['spec']['volumes']
                    if row['name'] == 'application-source-credentials')
            target = copy.deepcopy(delivery.deployment)
            source, = (row for row in target['spec']['template']['spec']['volumes']
                       if row['name'] == 'application-source-credentials')
            source['secret']['secretName'] = old['secret']['secretName']
            delivery = replace(delivery, deployment=target, source_secret_name=old['secret']['secretName'])
        return DevelopmentManagerRuntime(retained, original, after, delivery, requires_source, publication)
    except Exception:
        raise ValueError('development manager runtime unqualified') from None
