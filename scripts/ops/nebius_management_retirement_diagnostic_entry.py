"""Diagnostic authority derived from exact original private retirement receipts."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_management_entry import EntryError, _private
from scripts.ops.nebius_management_gateway import validate_operation
from scripts.ops.nebius_management_retirement import retirement_documents
from scripts.ops.nebius_management_retirement_entry import RetirementContext, load_retirement_inputs
from scripts.ops.nebius_management_stage import _validate_record

from loom.nebius_platform_render import digest


class DiagnosticPrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.nebius-management-retirement-diagnostic-private-inputs.v1"]
    retirement_operation: dict[str, Any]
    retirement_state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    retirement_journal_sha256: dict[str, str]


@dataclass(frozen=True)
class DiagnosticContext:
    retirement: RetirementContext
    receipts: dict[str, dict[str, Any]]


def load_diagnostic_inputs(operation: dict[str, Any]) -> DiagnosticContext:
    """No new targets/configuration: qualify every original frozen phase first."""
    try:
        validate_operation(operation)
        if operation["schema"] != "loom.nebius-management-retirement-diagnostic-operation.v1":
            raise ValueError
        raw = _private(Path(operation["inputs_path"]), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError
        inputs = DiagnosticPrivateInputs.model_validate_json(raw)
        old = inputs.retirement_operation
        validate_operation(old)
        root = Path(operation["inputs_path"]).parent.parent
        if (old["schema"] != "loom.nebius-management-retirement-operation.v1"
                or Path(old["inputs_path"]) != root / "retirement/inputs.json"
                or any(old[key] != operation[key] for key in ("candidate", "installation_id", "namespace"))):
            raise ValueError
        context = load_retirement_inputs(old)
        phases = retirement_documents(context.request)
        if set(inputs.retirement_journal_sha256) != set(phases):
            raise ValueError
        state = Path(old["state_dir"])
        raw = _private(state / "retirement.json", 1024**2)
        if hashlib.sha256(raw).hexdigest() != inputs.retirement_state_sha256:
            raise ValueError
        identity = {"schema": "loom.nebius-management-retirement.v1", "binding": asdict(context.request.binding),
            "revision": digest(phases), "state_dir": str(state)}
        anchor = _private(Path(old["anchor_dir"]) / (old["installation_id"] + ".json"), 1024**2)
        if json.loads(anchor) != identity or json.loads(raw) != {**identity, "started": list(phases)}:
            raise ValueError
        receipts = {}
        for phase, documents in phases.items():
            raw = _private(state / phase / "stage.json", 4 * 1024**2)
            if hashlib.sha256(raw).hexdigest() != inputs.retirement_journal_sha256[phase]:
                raise ValueError
            record = json.loads(raw)
            _validate_record(record, {"schema": "loom.nebius-management-stage.v1", "binding": asdict(context.request.binding),
                "revision": digest(phases), "phase": "retirement-" + phase}, documents)
            if any(item["status"] != "created" for item in record["resources"].values()):
                raise ValueError
            receipts[phase] = record
        return DiagnosticContext(context, receipts)
    except Exception:
        raise EntryError("management private retirement diagnostic inputs unqualified") from None
