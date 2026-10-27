"""Frozen prepared image identities consumed through the authenticated source broker."""

import re
from pathlib import Path

from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.task_environment_handoff import (
    RESOLVED_TASK_ENVIRONMENT,
    decode_resolved_task_environment,
)

COMPOSE_IMAGE_ROOT = Path("/loom/compose-images")


def compose_image_inputs(plan: ExecutionRuntimePlanV1) -> dict[str, str]:
    raw = plan.main.environment.get(RESOLVED_TASK_ENVIRONMENT)
    if raw is None or plan.execution_role != "attempt":
        return {}
    environment = decode_resolved_task_environment(raw)
    spec = environment.native_compose
    if spec is None:
        return {}
    if plan.task_image_materialization_id is None:
        raise ValueError("Compose image inputs require the frozen trial image grant")
    result = {}
    for service in spec.services:
        if (
            service.build is not None
            or service.image is None
            or re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", service.image) is None
        ):
            raise ValueError("Compose image input is not prepared and immutable")
        result[service.name] = service.image
    return result
