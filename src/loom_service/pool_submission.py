"""Stamp new work from protected service configuration, never HTTP metadata."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from loom_service.config import LoomServiceSettings


def submission_origin(settings: LoomServiceSettings, submission_id: UUID) -> dict[str, Any] | None:
    source = settings.pool_submission_source
    # Old work/configuration has unknown provenance. Global admission must not
    # interpret this NULL as an environment-class origin.
    return None if source is None else source.origin(submission_id).model_dump(mode="json")
