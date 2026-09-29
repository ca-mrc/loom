"""Exact original-resource checks and bounded diagnostic Pod evidence reads."""
from __future__ import annotations

import json
import re
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from scripts.ops.nebius_application_setup import _PATHS
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_entry import _private
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_gateway import validate_startup_report
from scripts.ops.nebius_management_retirement_diagnostic import diagnostic_documents
from scripts.ops.nebius_management_retirement_diagnostic_entry import DiagnosticContext
from scripts.ops.nebius_management_retirement_entry import HTTPSRetirementStageAPI
from scripts.ops.nebius_management_stage import ManagementStageError, _validate_record

from loom.nebius_platform_render import digest


class DiagnosticError(ManagementStageError):
    def __init__(self, stage: str):
        super().__init__("retirement diagnostic evidence unavailable; preserve state")
        self.stage = stage


class HTTPSRetirementDiagnosticAPI(HTTPSRetirementStageAPI):
    """Only the new fixed Job is writable; all original resources are read-only."""

    def __init__(self, *, context: DiagnosticContext, ssl_context: ssl.SSLContext, token: str | None):
        super().__init__(context=context.retirement, phase="job", ssl_context=ssl_context, token=token)
        self.diagnostic = context
        self.documents = diagnostic_documents(context.retirement.request)

    def verify_identity(self, binding: Any) -> None:
        try:
            super().verify_identity(binding)
            self.verify_namespaces()
            for record in self.diagnostic.receipts.values():
                for item in record["resources"].values():
                    doc = item["desired"]
                    version, plural = _PATHS[doc["kind"]]
                    prefix = "/api/v1" if version == "v1" else "/apis/" + version
                    namespace = doc["metadata"].get("namespace")
                    path = prefix + ("/namespaces/" + namespace if namespace else "") + "/" + plural + "/" + doc["metadata"]["name"]
                    actual = self._request("GET", path)
                    if actual is None or _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]:
                        raise ValueError
                    if doc["kind"] == "Job":
                        status = actual.get("status", {})
                        conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
                        if (conditions.get("Failed") != "True" or conditions.get("Complete") == "True"
                                or status.get("active", 0) != 0):
                            raise ValueError
        except Exception:
            raise DiagnosticError("diagnostic_original") from None

    def result(self, state_dir: Path) -> dict[str, Any]:
        stage = "diagnostic_job"
        try:
            self.verify_identity(self.binding)
            revision = digest(self.documents)
            record = json.loads(_private(state_dir / "job/stage.json", 4 * 1024**2))
            _validate_record(record, {"schema": "loom.nebius-management-stage.v1", "binding": asdict(self.binding),
                "revision": revision, "phase": "retirement-diagnostic"}, self.documents)
            item, = record["resources"].values()

            def job_readback() -> dict[str, Any]:
                job = self.get_resource(item["desired"])
                if (item["status"] != "created" or job is None or _uid(job) != item["uid"]
                        or _snapshot(job) != item["observed"]):
                    raise ValueError
                return job

            job = job_readback()
            status = job.get("status", {})
            conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
            if conditions.get("Failed") == "True":
                raise ValueError
            identity = {"namespace_uid": self.binding.namespace_uid, "revision": revision}
            if conditions.get("Complete") != "True":
                return {"status": "pending", "phase": "retirement-diagnostic", **identity}
            if status.get("succeeded") != 1 or status.get("active", 0) != 0:
                raise ValueError
            stage = "diagnostic_pod"
            namespace, name, uid = (job["metadata"][key] for key in ("namespace", "name", "uid"))
            base = "/api/v1/namespaces/" + namespace + "/pods"
            listing = self._request("GET", base + "?" + urlencode({
                "labelSelector": "batch.kubernetes.io/controller-uid=" + uid, "limit": 2}))
            if (listing is None or listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                    or listing.get("metadata", {}).get("continue") or len(listing.get("items", [])) != 1):
                raise ValueError
            pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
            _uid(pod)
            meta = pod["metadata"]
            labels = {**job["spec"]["template"]["metadata"].get("labels", {}),
                "batch.kubernetes.io/controller-uid": uid}
            if (pod.get("apiVersion") != "v1" or pod.get("kind") != "Pod" or meta.get("namespace") != namespace
                    or meta.get("deletionTimestamp") or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", meta["name"])
                    or meta.get("labels") != labels):
                raise ValueError
            owners = meta.get("ownerReferences", [])
            if (len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                    "apiVersion": "batch/v1", "kind": "Job", "name": name, "uid": uid, "controller": True}.items())):
                raise ValueError
            expected, actual = job["spec"]["template"]["spec"], pod["spec"]
            if (not _matches_backup_template(actual, expected) or pod.get("status", {}).get("phase") != "Succeeded"
                    or actual.get("securityContext", {}) != expected.get("securityContext", {})
                    or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
                    or any(actual.get(field, False) != expected.get(field, False)
                        for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))):
                raise ValueError
            for field, status_field in (("containers", "containerStatuses"), ("initContainers", "initContainerStatuses")):
                names = {row["name"] for row in expected.get(field, [])}
                states = pod["status"].get(status_field, [])
                if (len(states) != len(names) or {row["name"] for row in states} != names
                        or any(row.get("restartCount") != 0 or row.get("state", {}).get("terminated", {}).get("exitCode") != 0
                            for row in states)):
                    raise ValueError
                for container, wanted in zip(actual.get(field, []), expected.get(field, []), strict=True):
                    if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
                            or container.get("securityContext", {}) != wanted.get("securityContext", {})):
                        raise ValueError
            stage = "diagnostic_log"
            path = base + "/" + meta["name"]
            query = urlencode({"container": expected["containers"][0]["name"], "limitBytes": 16384, "timestamps": "false"})
            with self.client.stream("GET", path + "/log?" + query) as response:
                if response.status_code != 200 or response.headers.get("content-encoding", "identity") != "identity":
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=8192):
                    if len(content) + len(chunk) > 16384:
                        raise ValueError
                    content.extend(chunk)
            report = validate_startup_report(json.loads(content))
            operation_ids = {str(target.operation_id) for target in self.context.request.targets}
            if report["operations"] and {row["operation_id"] for row in report["operations"]} != operation_ids:
                raise ValueError
            stage = "diagnostic_readback"
            if self._request("GET", path) != pod or job_readback() != job:
                raise ValueError
            self.verify_identity(self.binding)
            return {"status": "retirement_diagnostic_observed", **identity, "probe": report}
        except DiagnosticError:
            raise
        except Exception:
            raise DiagnosticError(stage) from None
