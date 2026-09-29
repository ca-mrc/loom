"""The fixed recovery Job alone receives native DNS, under independent authority."""
from __future__ import annotations

import json
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_entry import _private
from scripts.ops.nebius_management_gateway import validate_recovery_report
from scripts.ops.nebius_management_retirement_diagnostic_live import (
    DiagnosticError,
    HTTPSRetirementDiagnosticAPI,
)
from scripts.ops.nebius_management_retirement_recovery import recovery_documents
from scripts.ops.nebius_management_retirement_recovery_entry import RecoveryContext
from scripts.ops.nebius_management_stage import _validate_record

from loom.nebius_platform_render import digest


class RecoveryError(DiagnosticError):
    pass


class HTTPSRetirementRecoveryAPI(HTTPSRetirementDiagnosticAPI):
    """Reuse strict completed-Pod validation, never the old diagnostic's grants."""

    def __init__(self, *, context: RecoveryContext, diagnostic_api: HTTPSRetirementDiagnosticAPI,
                 ssl_context: ssl.SSLContext, token: str | None):
        super().__init__(context=context.diagnostic, ssl_context=ssl_context, token=token)
        self.recovery, self.original_diagnostic = context, diagnostic_api
        phases = recovery_documents(context.diagnostic.retirement.request, original_job_uid=context.original_job_uid)
        self.documents = {key: doc for phase in phases.values() for key, doc in phase.items()}

    def verify_identity(self, binding: Any) -> None:
        try:
            if binding != self.binding:
                raise ValueError
            report = self.original_diagnostic.result(Path(self.recovery.diagnostic_operation["state_dir"]))
            expected = {"schema": "loom.nebius-retirement-startup-probe.v1", "status": "unavailable",
                "stage": "database", "checks": ["database_binding", "kubernetes_ca", "kubernetes_token"],
                "operations": [], "error_type": "OperationalError", "http_status": None}
            if report.get("status") != "retirement_diagnostic_observed" or report.get("probe") != expected:
                raise ValueError
        except Exception:
            raise RecoveryError("recovery_original") from None
        try:
            dns = self._request("GET", "/api/v1/namespaces/kube-system/services/coredns")
            if (dns is None or dns.get("apiVersion") != "v1" or dns.get("kind") != "Service"
                    or _uid(dns) != self.recovery.dns_service_uid
                    or dns["metadata"].get("name") != "coredns" or dns["metadata"].get("namespace") != "kube-system"
                    or dns["metadata"].get("deletionTimestamp") or dns["metadata"].get("ownerReferences")
                    or dns.get("spec", {}).get("selector") != {"k8s-app": "coredns"}
                    or dns["spec"].get("type") != "ClusterIP"
                    or not dns["spec"].get("clusterIP") or dns["spec"]["clusterIP"] == "None"
                    or not {("TCP", 53, 53), ("UDP", 53, 53)} <= {
                        (row.get("protocol"), row.get("port"), row.get("targetPort")) for row in dns["spec"].get("ports", [])}):
                raise ValueError
        except Exception:
            raise RecoveryError("recovery_dns") from None

    def _job_receipt(self, state_dir: Path) -> dict[str, Any]:
        record = json.loads(_private(state_dir / "resources/stage.json", 4 * 1024**2))
        _validate_record(record, {"schema": "loom.nebius-management-stage.v1", "binding": asdict(self.binding),
            "revision": digest(self.documents), "phase": "retirement-recovery"}, self.documents)
        for item in record["resources"].values():
            actual = self.get_resource(item["desired"])
            if (item["status"] != "created" or actual is None or _uid(actual) != item["uid"]
                    or _snapshot(actual) != item["observed"]):
                raise ValueError
        job, = [item for item in record["resources"].values() if item["desired"]["kind"] == "Job"]
        return dict(job)

    def _report(self, value: dict[str, Any]) -> dict[str, Any]:
        report = validate_recovery_report(value)
        startup = report["startup"]
        if startup and startup["operations"] and {row["operation_id"] for row in startup["operations"]} != {
                str(target.operation_id) for target in self.context.request.targets}:
            raise ValueError
        return report

    def _pending(self, identity: dict[str, Any]) -> dict[str, Any]:
        return {"status": "pending", "phase": "retirement-recovery", **identity}

    def _result_report(self, report: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
        if report["status"] == "completed":
            return {"status": "retirement_recovered", **identity, "recovery": report}
        return {"status": "blocked", "stage": "recovery_runtime", "recovery": report}
