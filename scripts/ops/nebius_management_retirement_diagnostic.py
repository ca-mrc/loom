"""Separate fixed diagnostic Job; never replay or replace the retirement Job."""
from __future__ import annotations

import copy
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import _setup_defaulted
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_retirement import RetirementInstallRequest, retirement_documents
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
)

from loom.nebius_platform_render import digest


def diagnostic_documents(request: RetirementInstallRequest) -> dict[str, dict[str, Any]]:
    original, = [doc for doc in retirement_documents(request)["job"].values() if doc["kind"] == "Job"]
    source = Path(__file__).with_name("nebius_retirement_startup_probe.py").read_text()
    if not 0 < len(source.encode()) <= 131072:
        raise ManagementStageError("diagnostic source size differs")
    document = copy.deepcopy(original)
    document["metadata"]["name"] = "loom-retirement-probe-" + digest({"original": original, "source": source})[7:19]
    document["spec"]["activeDeadlineSeconds"] = 240
    document["spec"]["template"]["spec"]["containers"][0]["command"] = ["python", "-c", source]
    # Preserve original labels: the same network policies must select this Pod.
    # Original image, mounts, projected SA, security and scheduling remain exact.
    return {_key(document): document}


def stage_diagnostic(*, request: RetirementInstallRequest, api: ManagementStageAPI,
                     state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """Stage only after the caller has qualified original private/live receipts.

    The independent anchor prevents lost local state from reopening creates.
    A recorded missing/failed Job is never replaced. Reading its completed report
    is a separate step; a stage receipt is not successful runtime evidence.
    """
    documents = diagnostic_documents(request)
    revision = digest(documents)
    identity = {"schema": "loom.nebius-management-retirement-diagnostic.v1", "binding": asdict(request.binding),
        "revision": revision, "state_dir": str(state_dir)}
    try:
        if (any(not path.is_absolute() or path != path.resolve() for path in (state_dir, anchor_dir))
                or state_dir == anchor_dir or state_dir in anchor_dir.parents or anchor_dir in state_dir.parents):
            raise ValueError
        with private_state._locked_state(anchor_dir):
            marker = anchor_dir / (request.binding.installation_id + ".json")
            progress = state_dir / "diagnostic.json"
            if marker.exists() or marker.is_symlink():
                if (json.loads(private_state._private_read(marker)) != identity
                        or json.loads(private_state._private_read(progress)) != identity
                        or not (state_dir / "job/stage.json").is_file()):
                    raise ValueError
            else:
                if state_dir.exists() or state_dir.is_symlink():
                    raise ValueError
                private_state._atomic_json(marker, identity)
            with private_state._locked_state(state_dir):
                private_state._atomic_json(progress, identity)
                return _stage_fixed_documents(documents=documents, revision=revision, phase="retirement-diagnostic",
                    binding=request.binding, api=api, state_dir=state_dir / "job", default_document=_setup_defaulted)
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError("diagnostic recovery evidence unavailable; preserve state") from None
