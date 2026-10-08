"""Fixed registration stage for the protected pool migration, not an operator CLI.

The parent must qualify candidate publication, cluster and migration phase, and
retain an independent stage-start anchor. A create receipt proves neither SQL
registration success nor permission to open admission or replace old writers.
"""
from __future__ import annotations

import json
import re
import ssl
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
    _validate_record,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import digest
from loom_service.environment_management.candidates import _json
from loom_service.pool_management.capacity import digest as installation_digest
from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.installation_render import render_registration


@dataclass(frozen=True, repr=False)
class PoolRegistrationRequest:
    spec: PoolInstallation
    binding: ManagementBinding
    candidate: dict[str, Any]


def validate_registration_proof(request: PoolRegistrationRequest, state: Path, proof: Any) -> None:
    """Bind a parent's closed receipt to the recorded fixed Job's identity."""
    record = json.loads(private_state._private_read(state / 'stage.json', limit=4 * 1024**2))
    job, = (row for row in record['resources'].values() if row['desired']['kind'] == 'Job')
    if (not isinstance(proof, dict) or set(proof) != {'job_uid', 'pod_uid', 'registration'}
            or job['status'] != 'created' or job['uid'] != proof['job_uid']
            or any(str(UUID(proof[key])) != proof[key] or not UUID(proof[key]).int for key in ('job_uid', 'pod_uid'))):
        raise ValueError('pool registration proof differs')
    spec = request.spec
    expected = {'schema_version': 'loom.pool-installation-receipt.v1', 'operation_id': str(spec.operation_id),
        'pool_id': str(spec.pool_id), 'installation_sha256': installation_digest(spec.model_dump(mode='json')),
        'mode': 'closed', 'participants': len(spec.participants), 'machines': len(spec.machines)}
    report = proof['registration']
    if report != expected or type(report.get('participants')) is not int or type(report.get('machines')) is not int:
        raise ValueError('pool registration receipt differs')


def registration_documents(request: PoolRegistrationRequest) -> dict[str, dict[str, Any]]:
    try:
        if (str(request.spec.installation_id) != request.binding.installation_id
                or request.candidate["source_ref"] != "refs/heads/dev"
                or re.fullmatch(r"[0-9a-f]{40}", request.candidate["candidate_sha"]) is None):
            raise ValueError
        documents = render_registration(request.spec, namespace=request.binding.namespace,
            service_image=request.candidate["images"]["service"]["image_ref"])
        for doc in documents:
            doc["metadata"].setdefault("annotations", {})["loom.nebius/candidate-sha"] = request.candidate["candidate_sha"]
        return {_key(doc): doc for doc in documents}
    except (KeyError, TypeError, ValueError):
        raise ValueError("pool_registration_runtime_unqualified") from None


class HTTPSPoolRegistrationAPI(HTTPSManagementStageAPI):
    """Reuse fixed-document HTTPS, namespace UID checks and no-create-retry rules."""

    def __init__(self, *, request: PoolRegistrationRequest, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents = registration_documents(request)
        self.binding = request.binding
        self.request = request
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _recorded(self, state_dir: Path) -> dict[str, dict[str, Any]]:
        record = json.loads(private_state._private_read(state_dir / "stage.json", limit=4 * 1024**2))
        identity = {"schema": "loom.nebius-management-stage.v1", "binding": asdict(self.binding),
            "revision": digest({"documents": self.documents, "binding": asdict(self.binding)}), "phase": "pool-registration"}
        _validate_record(record, identity, self.documents)
        result = {}
        for item in record["resources"].values():
            self.verify_identity(self.binding)
            actual = self.get_resource(item["desired"])
            if (item["status"] != "created" or actual is None or _uid(actual) != item["uid"]
                    or _snapshot(actual) != item["observed"]):
                raise ValueError
            result[actual["kind"]] = actual
        return result

    def registration_report(self, state_dir: Path) -> dict[str, Any] | None:
        """Read-only proof of the fixed Job's commit receipt; never opens intake."""
        try:
            resources = self._recorded(state_dir)
            job = resources["Job"]
            status = job.get("status", {})
            conditions = {row["type"]: row["status"] for row in status.get("conditions", [])}
            if conditions.get("Failed") == "True":
                raise ValueError
            if conditions.get("Complete") != "True":
                return None
            if any(type(status.get(key, 0)) is not int or status.get(key, 0) != count
                    for key, count in (("succeeded", 1), ("active", 0), ("failed", 0))):
                raise ValueError
            namespace, name, uid = (job["metadata"][key] for key in ("namespace", "name", "uid"))
            base = "/api/v1/namespaces/" + namespace + "/pods"
            listing = self._request("GET", base + "?" + urlencode({
                "labelSelector": "batch.kubernetes.io/controller-uid=" + uid, "limit": 2}))
            if (listing is None or listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                    or listing.get("metadata", {}).get("continue") or len(listing.get("items", [])) != 1):
                raise ValueError
            pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
            pod_uid, meta = _uid(pod), pod["metadata"]
            if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod" or meta.get("namespace") != namespace
                    or meta.get("deletionTimestamp") or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", meta["name"])):
                raise ValueError
            owners = meta.get("ownerReferences", [])
            if (len(owners) != 1 or any(owners[0].get(key) != value for key, value in {
                    "apiVersion": "batch/v1", "kind": "Job", "name": name, "uid": uid, "controller": True}.items())):
                raise ValueError
            labels = {**job["spec"]["template"]["metadata"].get("labels", {}), "batch.kubernetes.io/controller-uid": uid}
            region_label = "topology.kubernetes.io/region"
            if isinstance(meta.get("labels"), dict) and region_label not in labels and region_label in meta["labels"]:
                labels[region_label] = self.request.spec.quota_identities["nodes"][1]
            expected, actual = job["spec"]["template"]["spec"], pod["spec"]
            if (meta.get("labels") != labels or not _matches_backup_template(actual, expected)
                    or actual.get("securityContext", {}) != expected.get("securityContext", {})
                    or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
                    or actual.get("serviceAccountName", "default") != expected.get("serviceAccountName", "default")
                    or any(actual.get(field, False) != expected.get(field, False)
                        for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
                    or pod.get("status", {}).get("phase") != "Succeeded"):
                raise ValueError
            for field, status_field in (("containers", "containerStatuses"), ("initContainers", "initContainerStatuses")):
                names = {row["name"] for row in expected.get(field, [])}
                states = pod["status"].get(status_field, [])
                if (len(states) != len(names) or {row["name"] for row in states} != names
                        or any(type(row.get("restartCount")) is not int or row["restartCount"] != 0
                            or type(row.get("state", {}).get("terminated", {}).get("exitCode")) is not int
                            or row["state"]["terminated"]["exitCode"] != 0 for row in states)):
                    raise ValueError
                for container, wanted in zip(actual.get(field, []), expected.get(field, []), strict=True):
                    if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
                            or container.get("securityContext", {}) != wanted.get("securityContext", {})):
                        raise ValueError
            path = base + "/" + meta["name"]
            query = urlencode({"container": "register", "limitBytes": 16384, "timestamps": "false"})
            with self.client.stream("GET", path + "/log?" + query) as response:
                if response.status_code != 200 or response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=8192):
                    if len(content) + len(chunk) > 16384:
                        raise ValueError
                    content.extend(chunk)
            report = _json(bytes(content))
            spec = self.request.spec
            expected_report = {"schema_version": "loom.pool-installation-receipt.v1", "operation_id": str(spec.operation_id),
                "pool_id": str(spec.pool_id), "installation_sha256": installation_digest(spec.model_dump(mode="json")),
                "mode": "closed", "participants": len(spec.participants), "machines": len(spec.machines)}
            if (report != expected_report or type(report.get("participants")) is not int or type(report.get("machines")) is not int
                    or self._request("GET", path) != pod or self._recorded(state_dir) != resources):
                raise ValueError
            self.verify_identity(self.binding)
            return {"job_uid": uid, "pod_uid": pod_uid, "registration": report}
        except Exception:
            raise ManagementStageError("pool registration execution unqualified; preserve evidence") from None


def stage_pool_registration(*, request: PoolRegistrationRequest, api: ManagementStageAPI,
                            state_dir: Path) -> dict[str, Any]:
    try:
        documents = registration_documents(request)
        revision = digest({"documents": documents, "binding": asdict(request.binding)})
        return _stage_fixed_documents(documents=documents, revision=revision, phase="pool-registration",
            binding=request.binding, api=api, state_dir=state_dir)
    except Exception:
        raise ValueError("pool_registration_stage_unavailable_preserve_evidence") from None
