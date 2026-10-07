"""Private source-bound entry; only the separately authenticated gateway calls it.

No installed authority or CLI is created by this module. Publication must bind
the installer code AND development-source.json before main may be invoked.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict
from scripts.ops.nebius_development_bootstrap import DevelopmentBootstrapBinding
from scripts.ops.nebius_development_install import (
    DevelopmentInstallRequest,
    install_private_development,
)
from scripts.ops.nebius_development_live import (
    DevelopmentLiveSettings,
    HTTPSDevelopmentInstallationAPI,
    _private,
)
from scripts.ops.nebius_development_operation import DIAGNOSTIC_STAGES, validate_operation
from scripts.ops.nebius_development_preflight import PreparedDevelopmentSource
from scripts.ops.nebius_development_stage import (
    _STORAGE_KEYS,
    DevelopmentResourceBinding,
    DevelopmentStageInput,
    development_documents,
)

from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom_service.environment_management.candidates import _json

SOURCE_RECORD = Path(__file__).resolve().parents[2] / "development-source.json"


class DevelopmentPrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.nebius-development-private-inputs.v1"]
    binding: DevelopmentBootstrapBinding
    config: dict[str, Any]
    candidate: dict[str, Any]
    profile: dict[str, Any]
    keyring: dict[str, Any]
    settings: DevelopmentLiveSettings
    operator_connection: NebiusKubernetesConnection
    storage_files: dict[str, Path]


def load_inputs(operation: dict[str, Any]) -> tuple[DevelopmentPrivateInputs, DevelopmentInstallRequest, dict[Path, bytes]]:
    try:
        validate_operation(operation)
        path = Path(operation["inputs_path"])
        raw = _private(path)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError()
        inputs = DevelopmentPrivateInputs.model_validate(_json(raw))
        source = PreparedDevelopmentSource.model_validate(_json(_private(SOURCE_RECORD)))
        connection, settings = inputs.operator_connection, inputs.settings
        if (inputs.binding.installation_id != operation["installation_id"]
                or inputs.binding.namespace != operation["namespace"]
                or source != settings.preflight.source or source.source_sha != operation["source_sha"]
                or inputs.candidate.get("candidate_sha") != operation["candidate"]
                or inputs.profile.get("candidate_sha") != operation["candidate"]
                or inputs.binding.kube_system_uid != str(settings.preflight.kube_system_uid)
                or inputs.config["kubernetes_api_server"].rstrip("/") != connection.endpoint
                or inputs.storage_files.keys() != _STORAGE_KEYS):
            raise ValueError()
        operators = {path, SOURCE_RECORD, connection.ca_file, connection.credentials_file,
            settings.operator_cloud_credentials, settings.github_token_file}
        runtime = set(inputs.storage_files.values())
        if (len(runtime) != len(_STORAGE_KEYS) or runtime & operators
                or settings.github_token_file in {path, SOURCE_RECORD, connection.ca_file,
                    connection.credentials_file, settings.operator_cloud_credentials}):
            raise ValueError()
        files = {item: _private(item) for item in operators | runtime}
        if files[path] != raw:
            raise ValueError()
        storage = {key: files[item].decode() for key, item in inputs.storage_files.items()}
        request = DevelopmentInstallRequest(inputs.binding,
            DevelopmentStageInput(inputs.config, inputs.candidate, inputs.profile, inputs.keyring, storage))
        # Only pure validation; the real namespace and operation UIDs come from
        # independently anchored bootstrap evidence, not these provisional IDs.
        provisional = DevelopmentResourceBinding(inputs.binding, inputs.binding.installation_id, inputs.binding.installation_id)
        development_documents(request.selection, provisional, "config")
        return inputs, request, files
    except Exception:
        raise ValueError("development private inputs unqualified") from None


async def _transport(connection: NebiusKubernetesConnection) -> tuple[ssl.SSLContext, str]:
    credentials = NebiusKubernetesCredentials(connection)
    try:
        async with asyncio.timeout(45):
            return credentials.ssl_context, await credentials.get_token()
    finally:
        await credentials.close()


def connected_api(inputs: DevelopmentPrivateInputs, request: DevelopmentInstallRequest,
                  files: dict[Path, bytes], operation: dict[str, Any]) -> HTTPSDevelopmentInstallationAPI:
    context, token = asyncio.run(_transport(inputs.operator_connection))
    if any(_private(path) != raw for path, raw in files.items()):
        raise ValueError("development connection inputs changed")
    return HTTPSDevelopmentInstallationAPI(request=request, settings=inputs.settings,
        api_server=inputs.operator_connection.endpoint, ssl_context=context, token=token,
        state_dir=Path(operation["state_dir"]), private_files=files)


def main(operation_path: str, action: str) -> int:
    stage, api = "operation", None
    try:
        if action not in {"qualify", "preflight", "install"}:
            raise ValueError()
        operation = _json(_private(Path(operation_path)))
        validate_operation(operation)
        if action == "qualify":
            print(json.dumps({"status": "tooling_qualified"}))
            return 0
        stage = "inputs"
        inputs, request, files = load_inputs(operation)
        files[Path(operation_path)] = _private(Path(operation_path))
        if _json(files[Path(operation_path)]) != operation:
            raise ValueError()
        stage = "connection"
        api = connected_api(inputs, request, files, operation)
        stage = "installation"
        if action == "preflight":
            marker = Path(operation["anchor_dir"]) / (operation["installation_id"] + ".json")
            api.qualify(request, fresh=not marker.exists())
            result = {"status": "development_preflight_qualified"}
        else:
            result = install_private_development(request=request, api=api,
                state_dir=Path(operation["state_dir"]), anchor_dir=Path(operation["anchor_dir"]))
        report = {"status": result["status"], "namespace": "loom-dev", "installation_id": operation["installation_id"],
            "candidate": operation["candidate"]}
        if result["status"] == "pending":
            report["phase"] = result["phase"]
        print(json.dumps(report))
        return 0
    except Exception:
        detail = getattr(api, "diagnostic_stage", None)
        print(json.dumps({"status": "blocked", "stage": detail if detail in DIAGNOSTIC_STAGES else stage}))
        return 1
