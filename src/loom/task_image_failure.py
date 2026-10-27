"""Safe user-facing classification of image-preparation and native-build failures."""
from __future__ import annotations

from typing import Any

from loom.task_image_errors import PREPARATION_ERRORS

_DETAILS = {
    "image_preparation_package_change_required": (
        "platform", "Harness preparation would change an authored package; a compatible platform preparation is required."),
    "image_preparation_base_unsupported": (
        "platform", "The selected platform does not support preparing this task's base image."),
    "image_preparation_dockerfile_invalid": (
        "task_or_platform", "The platform could not interpret the task Dockerfile."),
    "image_preparation_input_invalid": (
        "unknown", "The registered image preparation input is invalid or does not match its frozen revision."),
    "image_preparation_source_missing": (
        "user_or_platform", "A registered image preparation source is missing."),
    "image_preparation_source_denied": (
        "user_or_platform", "The platform cannot read a registered image preparation source."),
    "image_preparation_source_unavailable": (
        "platform", "Task source storage is temporarily unavailable."),
    "image_preparation_internal_error": (
        "platform", "Image preparation failed internally; inspect operator diagnostics."),
}


def image_preparation_failure(code: str | None) -> dict[str, Any] | None:
    if not isinstance(code, str):
        return None
    # This trusted controller lifecycle reason is diagnostic only. Keep it
    # outside PREPARATION_ERRORS: untrusted prepare receipts cannot claim node
    # loss or obtain retry authority from this public classification.
    if code == "build_bound_node_unavailable":
        return {
            "reason": code,
            "message": "The node reserved for the image build became unavailable before startup. "
                       "The platform must clean up the old attempt before another attempt can be scheduled.",
            "attribution": "platform",
            "retryable": True,
        }
    if code not in _DETAILS or code not in PREPARATION_ERRORS:
        return None
    attribution, message = _DETAILS[code]
    return {"reason": code, "message": message, "attribution": attribution,
            "retryable": PREPARATION_ERRORS[code]}
