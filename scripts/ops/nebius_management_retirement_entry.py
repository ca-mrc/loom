"""Private retirement input qualification and exact protected Kubernetes adapter."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_application_setup import HTTPSApplicationSetupAPI, _documents, _revision
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_gateway import validate_operation
from scripts.ops.nebius_management_prerequisites import qualify_management_publication
from scripts.ops.nebius_management_retirement import (
    RetirementInstallRequest,
    install_retirement,
    retirement_documents,
)
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    ManagementStageError,
    _qualified_defaulted,
    _validate_record,
)
from scripts.ops.nebius_management_switch import (
    ManagementSwitchRequest,
    _desired,
    _matches,
    _target,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_management_upgrade import ManagementUpgradeRequest, _original

from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.retirement import RetirementTarget

if TYPE_CHECKING:
    from scripts.ops.nebius_management_entry import PrivateInputs


class RetirementPrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.nebius-management-retirement-private-inputs.v1"]
    upgrade_operation: dict[str, Any]
    upgrade_state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    switch_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    candidate_id: UUID
    targets: tuple[RetirementTarget, ...] = Field(min_length=1, max_length=16)


@dataclass(frozen=True)
class RetirementContext:
    inputs: RetirementPrivateInputs
    request: RetirementInstallRequest
    upgrade: ManagementUpgradeRequest
    original_inputs: PrivateInputs
    active_management: dict[str, Any]
    legacy_fence: tuple[dict[str, Any], ...]


def load_retirement_inputs(operation: dict[str, Any]) -> RetirementContext:
    from scripts.ops.nebius_management_entry import EntryError, _private, load_upgrade_inputs

    try:
        validate_operation(operation)
        if operation["schema"] != "loom.nebius-management-retirement-operation.v1":
            raise ValueError
        raw = _private(Path(operation["inputs_path"]), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError
        inputs = RetirementPrivateInputs.model_validate_json(raw)
        old = inputs.upgrade_operation
        validate_operation(old)
        root = Path(operation["inputs_path"]).parent.parent
        if (old["schema"] != "loom.nebius-management-upgrade-operation.v1"
                or Path(old["inputs_path"]) != root / "upgrade/inputs.json"
                or any(old[key] != operation[key] for key in ("installation_id", "namespace"))
                or inputs.candidate.get("candidate_sha") != operation["candidate"]
                or inputs.profile.get("candidate_sha") != operation["candidate"]
                or operation["source_sha"] != operation["candidate"]):
            raise ValueError
        _, upgrade, original_inputs, _ = load_upgrade_inputs(old)
        before, after = (item.model_dump(mode="json") for item in (upgrade.setup.deployment, inputs.deployment))
        old_publications = before["installation"].pop("publications")
        publications = after["installation"].pop("publications")
        if before != after or any(item not in publications for item in old_publications):
            raise ValueError
        state = Path(old["state_dir"])
        upgrade_bytes = _private(state / "upgrade.json", 1024**2)
        switch_bytes = _private(state / "switch/switch.json", 4 * 1024**2)
        if (hashlib.sha256(upgrade_bytes).hexdigest() != inputs.upgrade_state_sha256
                or hashlib.sha256(switch_bytes).hexdigest() != inputs.switch_sha256):
            raise ValueError
        record, switch = json.loads(upgrade_bytes), json.loads(switch_bytes)
        binding = upgrade.setup.binding
        phases = ("config", "admission", "permissions", "network", "material", "database", "retirement", "migration")
        if (record["schema"] != "loom.nebius-management-upgrade.v1" or record["binding"] != asdict(binding)
                or record["state_dir"] != str(state) or record["activation_started"] is not True
                or record["switch_started"] is not True or set(record["phases"]) != set(phases)
                or switch["schema"] != "loom.nebius-management-switch.v1" or switch["binding"] != asdict(binding)
                or switch["phase"] != "active" or not isinstance(switch["active"], dict)):
            raise ValueError
        journals = {}
        for phase in phases:
            data = _private(state / phase / "stage.json", 4 * 1024**2)
            if hashlib.sha256(data).hexdigest() != record["phases"][phase]["sha256"]:
                raise ValueError
            journals[phase] = json.loads(data)
        fence_documents = _documents(upgrade.setup, "retirement")
        _validate_record(journals["retirement"], {
            "schema": "loom.nebius-management-stage.v1", "binding": asdict(binding),
            "revision": _revision(upgrade.setup, fence_documents), "phase": "application-retirement",
        }, fence_documents)
        fence = []
        for item in journals["retirement"]["resources"].values():
            if item["status"] != "created":
                raise ValueError
            document = copy.deepcopy(item["observed"])
            document["metadata"]["uid"] = item["uid"]
            _uid(document)
            fence.append(document)
        original, original_hash = _original(upgrade)
        target, revision = _target(ManagementSwitchRequest(upgrade.setup, original))
        if (record["original_installation_sha256"] != original_hash or switch["revision"] != revision
                or switch["original_uid"] != _uid(original) or not _matches(switch["original"], original, _uid(original))
                or _qualified_defaulted(_desired(original, target, "activate", switch["operation_id"]), switch["active"]) != switch["active"]):
            raise ValueError
        active = copy.deepcopy(switch["active"])
        active["metadata"]["uid"] = switch["original_uid"]
        request = RetirementInstallRequest(binding, inputs.deployment, inputs.candidate, inputs.profile,
            inputs.targets, Path(__file__).resolve().parents[2])
        retirement_documents(request)
        return RetirementContext(inputs, request, upgrade, original_inputs, active, tuple(fence))
    except Exception:
        raise EntryError("management private retirement inputs unqualified") from None


class HTTPSRetirementStageAPI(HTTPSApplicationSetupAPI):
    """Reuse fixed-document requests, never enable an arbitrary manifest route."""

    def __init__(self, *, context: RetirementContext, phase: str, ssl_context: ssl.SSLContext, token: str | None):
        self.context = context
        self.binding = context.request.binding
        self.documents = retirement_documents(context.request)[phase]
        self.namespaces = {name: (str(uid), target) for target in context.request.targets for name, uid in target.namespace_uids.items()}
        self.fence = context.legacy_fence
        ManagementKubernetesTransport.__init__(self,
            api_server=context.original_inputs.operator_connection.endpoint, ssl_context=ssl_context, token=token)

    def verify_identity(self, binding: Any) -> None:
        HTTPSManagementStageAPI.verify_identity(self, binding)
        namespace = self.binding.namespace
        current = self._request("GET", "/apis/apps/v1/namespaces/" + namespace + "/deployments/loom-service")
        expected = self.context.active_management
        if current is None or not _matches(current, expected, _uid(expected)):
            raise ManagementStageError("retirement requires the exact upgraded management process")
        pods = self._request("GET", "/api/v1/namespaces/" + namespace + "/pods?limit=500")
        if (pods is None or not isinstance(pods.get("items"), list) or len(pods["items"]) > 500
                or pods.get("metadata", {}).get("continue")
                or any(pod.get("spec", {}).get("serviceAccountName") == "loom-management-provisioner" for pod in pods["items"])):
            raise ManagementStageError("legacy management process is not proven absent")
        for doc in self.fence:
            plural = "validatingadmissionpolicies" if doc["kind"] == "ValidatingAdmissionPolicy" else "validatingadmissionpolicybindings"
            actual = self._request("GET", "/apis/admissionregistration.k8s.io/v1/" + plural + "/" + doc["metadata"]["name"])
            if actual is None or _uid(actual) != _uid(doc) or _snapshot(actual) != _snapshot(doc):
                raise ManagementStageError("legacy management admission fence differs")
            if doc["kind"] == "ValidatingAdmissionPolicy":
                status = actual.get("status", {})
                if (not isinstance(status.get("typeChecking"), dict) or status["typeChecking"].get("expressionWarnings")
                        or status.get("observedGeneration", 0) < actual["metadata"].get("generation", 1)):
                    raise ManagementStageError("legacy management admission fence is not ready")

    def verify_namespaces(self) -> None:
        for name in self.namespaces:
            self._namespace(name)

    def _namespace(self, name: str) -> None:
        uid, target = self.namespaces[name]
        row = self._request("GET", "/api/v1/namespaces/" + name)
        expected = {"metadata": {"name": name, "labels": {
            "loom.nebius/environment-id": str(target.registration.environment_id),
            "loom.nebius/incarnation": str(target.registration.incarnation),
        }}}
        if (row is None or _uid(row) != uid or not _contains(row, expected)
                or row["metadata"].get("deletionTimestamp") or row["metadata"].get("ownerReferences")):
            raise ManagementStageError("retirement namespace identity differs")

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        path = super()._approved(document, writing=writing)
        namespace = document["metadata"].get("namespace")
        if namespace in self.namespaces:
            self._namespace(namespace)
        return path


@contextmanager
def connected_retirement(context: RetirementContext) -> Iterator[Any]:
    from scripts.ops.nebius_management_entry import _operator_transport

    trust, token = asyncio.run(_operator_transport(context.original_inputs.operator_connection))

    def resources(phase: str) -> HTTPSRetirementStageAPI:
        return HTTPSRetirementStageAPI(context=context, phase=phase, ssl_context=trust, token=token)

    with resources("permissions") as api:
        api.verify_identity(context.request.binding)
        api.verify_namespaces()
    publication = replace(context.upgrade.original, deployment=context.inputs.deployment,
        candidate=context.inputs.candidate, profile=context.inputs.profile)

    async def qualify() -> None:
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_management_publication(request=publication, candidate_id=context.inputs.candidate_id, http=http)

    asyncio.run(qualify())
    yield resources


def execute_retirement(context: RetirementContext, operation: dict[str, Any], action: str) -> dict[str, Any]:
    with connected_retirement(context) as resources:
        if action == "preflight":
            return {"status": "preflight_qualified"}
        return install_retirement(request=context.request, resources=resources,
            state_dir=Path(operation["state_dir"]), anchor_dir=Path(operation["anchor_dir"]))
