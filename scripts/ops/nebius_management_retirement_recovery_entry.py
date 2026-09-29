"""Independent recovery authority derives its targets from frozen old receipts."""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_management_entry import EntryError, _private
from scripts.ops.nebius_management_gateway import validate_operation
from scripts.ops.nebius_management_retirement_diagnostic import diagnostic_documents
from scripts.ops.nebius_management_retirement_diagnostic_entry import (
    DiagnosticContext,
    load_diagnostic_inputs,
)
from scripts.ops.nebius_management_stage import _validate_record

from loom.nebius_platform_render import digest


class RecoveryPrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.nebius-management-retirement-recovery-private-inputs.v1"]
    diagnostic_operation: dict[str, Any]
    diagnostic_journal_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    dns_service_uid: UUID


@dataclass(frozen=True)
class RecoveryContext:
    diagnostic: DiagnosticContext
    diagnostic_operation: dict[str, Any]
    original_job_uid: str
    dns_service_uid: str


def load_recovery_inputs(operation: dict[str, Any]) -> RecoveryContext:
    try:
        validate_operation(operation)
        if operation["schema"] != "loom.nebius-management-retirement-recovery-operation.v1":
            raise ValueError
        raw = _private(Path(operation["inputs_path"]), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError
        inputs = RecoveryPrivateInputs.model_validate_json(raw)
        old = inputs.diagnostic_operation
        validate_operation(old)
        root = Path(operation["inputs_path"]).parent.parent
        if (old["schema"] != "loom.nebius-management-retirement-diagnostic-operation.v1"
                or Path(old["inputs_path"]) != root / "retirement-diagnostic/inputs.json"
                or any(old[key] != operation[key] for key in ("candidate", "installation_id", "namespace"))
                or not inputs.dns_service_uid.int):
            raise ValueError
        diagnostic = load_diagnostic_inputs(old)
        documents = diagnostic_documents(diagnostic.retirement.request)
        state = Path(old["state_dir"])
        identity = {"schema": "loom.nebius-management-retirement-diagnostic.v1",
            "binding": asdict(diagnostic.retirement.request.binding), "revision": digest(documents), "state_dir": str(state)}
        if (json.loads(_private(state / "diagnostic.json", 1024**2)) != identity
                or json.loads(_private(Path(old["anchor_dir"]) / (old["installation_id"] + ".json"), 1024**2)) != identity):
            raise ValueError
        raw = _private(state / "job/stage.json", 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != inputs.diagnostic_journal_sha256:
            raise ValueError
        record = json.loads(raw)
        _validate_record(record, {"schema": "loom.nebius-management-stage.v1", "binding": identity["binding"],
            "revision": identity["revision"], "phase": "retirement-diagnostic"}, documents)
        if any(item["status"] != "created" for item in record["resources"].values()):
            raise ValueError
        job, = [item for item in diagnostic.receipts["job"]["resources"].values() if item["desired"]["kind"] == "Job"]
        return RecoveryContext(diagnostic, old, job["uid"], str(inputs.dns_service_uid))
    except Exception:
        raise EntryError("management private retirement recovery inputs unqualified") from None


def execute_recovery(context: RecoveryContext, operation: dict[str, Any], action: str) -> dict[str, Any]:
    from scripts.ops.nebius_management_entry import _operator_transport
    from scripts.ops.nebius_management_retirement_diagnostic_live import (
        HTTPSRetirementDiagnosticAPI,
    )
    from scripts.ops.nebius_management_retirement_recovery import stage_recovery
    from scripts.ops.nebius_management_retirement_recovery_live import (
        HTTPSRetirementRecoveryAPI,
        RecoveryError,
    )

    if action not in {"preflight", "install"}:
        raise EntryError("recovery action outside fixed authority")
    stage = "recovery_original"
    try:
        original = context.diagnostic.retirement
        trust, token = asyncio.run(_operator_transport(original.original_inputs.operator_connection))
        with HTTPSRetirementDiagnosticAPI(context=context.diagnostic, ssl_context=trust, token=token) as diagnostic:
            with HTTPSRetirementRecoveryAPI(context=context, diagnostic_api=diagnostic, ssl_context=trust, token=token) as api:
                api.verify_identity(original.request.binding)
                if action == "preflight":
                    return {"status": "preflight_qualified"}
                stage = "recovery_stage"
                state = Path(operation["state_dir"])
                stage_recovery(request=original.request, original_job_uid=context.original_job_uid,
                    api=api, state_dir=state, anchor_dir=Path(operation["anchor_dir"]))
                return api.result(state)
    except RecoveryError:
        raise
    except Exception:
        raise RecoveryError(stage) from None
