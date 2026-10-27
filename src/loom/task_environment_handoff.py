"""Bounded typed environment handoff from a frozen plan to its controller."""

from loom.models.task import EnvironmentConfig
from loom.runtime_environment import MAX_ENV_VALUE_BYTES, validate_task_environment

RESOLVED_TASK_ENVIRONMENT = "LOOM_TASK_ENVIRONMENT_JSON"
RESOLVED_VERIFIER_ENVIRONMENT = "LOOM_VERIFIER_ENVIRONMENT_JSON"


def decode_resolved_task_environment(raw: str) -> EnvironmentConfig:
    if len(raw.encode("utf-8")) > MAX_ENV_VALUE_BYTES:
        raise ValueError("resolved_task_environment_too_large")
    try:
        environment = EnvironmentConfig.model_validate_json(raw)
        if environment.user == "image" or environment.image_entrypoint or environment.image_workdir:
            raise ValueError("unresolved image defaults")
        validate_task_environment(environment.environment)
        return environment
    except (ValueError, TypeError):
        raise ValueError("resolved_task_environment_invalid") from None


def encode_resolved_task_environment(environment: EnvironmentConfig) -> str:
    # Keep discriminator defaults inside nested network/requirement models.
    raw = environment.model_dump_json(exclude_none=True)
    decode_resolved_task_environment(raw)
    return raw
