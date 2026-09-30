"""Fixed registration stage for the protected pool migration, not an operator CLI.

The parent must qualify candidate publication, cluster and migration phase, and
retain an independent stage-start anchor. A create receipt proves neither SQL
registration success nor permission to open admission or replace old writers.
"""
from __future__ import annotations

import re
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import digest
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.installation_render import render_registration


@dataclass(frozen=True, repr=False)
class PoolRegistrationRequest:
    spec: PoolInstallation
    binding: ManagementBinding
    candidate: dict[str, Any]


def registration_documents(request: PoolRegistrationRequest) -> dict[str, dict[str, Any]]:
    try:
        if (str(request.spec.installation_id) != request.binding.installation_id
                or request.candidate["source_ref"] != "refs/heads/dev"
                or re.fullmatch(r"[0-9a-f]{40}", request.candidate["candidate_sha"]) is None):
            raise ValueError
        documents = render_registration(request.spec, namespace=request.binding.namespace,
            service_image=request.candidate["images"]["service"]["image_ref"])
        for doc in documents:
            doc["metadata"].setdefault("annotations", {})["loom.nebius/candidate-sha"] = request.candidate["candidate_sha"]
        return {_key(doc): doc for doc in documents}
    except (KeyError, TypeError, ValueError):
        raise ValueError("pool_registration_runtime_unqualified") from None


class HTTPSPoolRegistrationAPI(HTTPSManagementStageAPI):
    """Reuse fixed-document HTTPS, namespace UID checks and no-create-retry rules."""

    def __init__(self, *, request: PoolRegistrationRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = registration_documents(request)
        self.binding = request.binding
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)


def stage_pool_registration(*, request: PoolRegistrationRequest, api: ManagementStageAPI,
                            state_dir: Path) -> dict[str, Any]:
    try:
        documents = registration_documents(request)
        revision = digest({"documents": documents, "binding": asdict(request.binding)})
        return _stage_fixed_documents(documents=documents, revision=revision, phase="pool-registration",
            binding=request.binding, api=api, state_dir=state_dir)
    except Exception:
        raise ValueError("pool_registration_stage_unavailable_preserve_evidence") from None
