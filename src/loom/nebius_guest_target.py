"""Closed configuration for one guest sibling of an ordinary execution target."""

import re
from typing import Any


def guest_target_id(config: dict[str, Any]) -> str | None:
    declaration = config.get("guest_execution_target")
    if declaration is None:
        return None
    if not isinstance(declaration, dict) or set(declaration) != {"target_id"}:
        raise ValueError("guest execution target requires only its distinct target_id")
    target_id = declaration["target_id"]
    # Leave room for the -actuator suffix in Kubernetes label values.
    if not isinstance(target_id, str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,52}[a-z0-9])?", target_id):
        raise ValueError("guest target_id must be a DNS label of at most 54 characters")
    if target_id == config["target_id"]:
        raise ValueError("guest execution target must be distinct from its physical owner")
    if target_id == "loom-execution":
        raise ValueError("guest target_id collides with the ordinary actuator Deployment")
    if config.get("regional_execution_targets") or config.get("schema_version") != "loom.nebius-platform.v1":
        raise ValueError("guest execution is qualified only for one independent physical target")
    return target_id
