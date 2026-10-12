"""Shared publication component identities; build inputs remain manifest-owned."""

import re


def candidate_service_schema_head(candidate: dict[str, object]) -> str:
    """Require a concrete published revision, not a mutable Alembic alias."""
    value = candidate.get('service_schema_head')
    if (not isinstance(value, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value) is None
            or value in {'head', 'heads', 'base'}):
        raise ValueError('candidate service schema head unavailable or invalid')
    return value


NEBIUS_PLATFORM_IMAGES = {
    "service": "loom-service",
    "control_plane": "loom-control-plane",
    "web": "loom-web",
    "gateway": "loom-llm-gateway",
    "execution_runtime": "loom-execution-runtime",
    "execution_actuator": "loom-execution-actuator",
    "harbor_runtime": "loom-harbor-runtime",
}
