"""Read-only external authority for the protected closed development runtime."""
from __future__ import annotations

import base64
import json
from pathlib import Path

import httpx
from scripts.ops.nebius_development_runtime_install import DevelopmentRuntimeInstallRequest

from loom.execution_image_admission import ImageAdmissionKeyring
from loom_service.environment_management.candidates import GitHubCandidateCatalog, _json


async def qualify_runtime_publication(*, request: DevelopmentRuntimeInstallRequest,
                                      http: httpx.AsyncClient) -> None:
    """Resolve the exact protected artifact, using the retained reader identity."""
    try:
        manager = request.database.manager
        retained = manager.retained.request.retained
        installation = manager.deployment.installation
        selected = manager.publication.validate(installation.registry_prefix)
        history = _json(retained.files[Path(retained.operation['state_dir']) / 'supplied/stage.json'])
        key = 'Secret:' + retained.binding.namespace + ':loom-management-publications'
        token = base64.b64decode(history['resources'][key]['observed']['data']['token'], validate=True).decode()
        catalog = GitHubCandidateCatalog(http, token=token, publications=[selected.publication],
            registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        if await catalog.resolve(selected.publication.candidate_id) != selected.bundle:
            raise ValueError
    except Exception:
        raise ValueError('development runtime publication unqualified') from None
