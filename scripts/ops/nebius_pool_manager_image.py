"""Image-only correction projections; no publication or installation authority."""
from __future__ import annotations

import re
from typing import Any

from scripts.ops.nebius_ingress_stage import _snapshot
from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh


def manager_image_target(request: ManagementRefreshRenderRequest) -> dict[str, Any]:
    """Preserve the qualified manager config and every field except its images.

    The caller separately binds the retained template and protected publication.
    Reuse ordinary refresh's strict runtime/defaulting and credential checks,
    but do not permit its configuration or application-release changes here.
    """
    try:
        if request.before != request.after:
            raise ValueError
        # Refresh validates the retained shape against the selected code. Its
        # candidate-bound config revision is deliberately not installed: this
        # operation retains the original configuration and changes images only.
        render_refresh(request)
        image = request.candidate['images']['service']['image_ref']
        registry = request.before.installation.registry_prefix
        if re.fullmatch(re.escape(registry) + r'/[a-z0-9._/-]+@sha256:[0-9a-f]{64}', image) is None:
            raise ValueError
        expected = _snapshot(request.active)
        pod = expected['spec']['template']['spec']
        if pod['containers'][0]['image'] == image:
            raise ValueError
        for container in (*pod['containers'], *pod.get('initContainers', [])):
            container['image'] = image
        return expected
    except Exception:
        raise ValueError('pool_manager_image_projection_unqualified') from None
