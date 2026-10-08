"""Exact deployment-owned resolution of upstream task-image tags."""

from __future__ import annotations

import re
from collections.abc import Mapping

from loom.models.task import TaskConfig

MAX_PREBUILT_TASK_IMAGE_PINS = 128
_TAG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,2047}:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
_DIGEST = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")


def validate_prebuilt_image_pins(pins: Mapping[str, str]) -> dict[str, str]:
    if len(pins) > MAX_PREBUILT_TASK_IMAGE_PINS or any(
        _TAG.fullmatch(source) is None or _DIGEST.fullmatch(target) is None
        for source, target in pins.items()
    ):
        raise ValueError("prebuilt image pins require bounded explicit tags and immutable digests")
    return dict(pins)


def resolve_prebuilt_task_image(task: TaskConfig, pins: Mapping[str, str]) -> TaskConfig:
    """Resolve only an explicitly published tag, preserving the source snapshot.

    Unmapped tags remain unchanged and fail existing immutable-image admission.
    Dockerfile preparation and fixture component authority remain separate.
    """
    source = task.environment.docker_image
    if source is None or task.environment.dockerfile is not None or source not in pins:
        return task
    payload = task.model_dump(mode="json")
    payload["environment"]["docker_image"] = pins[source]
    return TaskConfig.model_validate(payload)
