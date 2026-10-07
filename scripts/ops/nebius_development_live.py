"""Connect the private dev installer to authenticated reads and fixed phases.

No caller readiness flags, ambient credentials or staging adapters. The protected
entrypoint must authenticate the bundle and private inputs before constructing it.
"""
from __future__ import annotations

import asyncio
import copy
import json
import re
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit
from uuid import UUID

import httpx
from kubernetes.utils.quantity import parse_quantity
from pydantic import BaseModel, ConfigDict
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_bootstrap import HTTPSDevelopmentBootstrapAPI
from scripts.ops.nebius_development_cloud import (
    DevelopmentCloudScope,
    qualify_development_cloud,
    qualify_development_disk,
)
from scripts.ops.nebius_development_install import (
    DevelopmentInstallError,
    DevelopmentInstallRequest,
    _storage_observation,
)
from scripts.ops.nebius_development_preflight import (
    DevelopmentPreflightSettings,
    HTTPSDevelopmentPreflight,
)
from scripts.ops.nebius_development_probe import probe_private_service
from scripts.ops.nebius_development_stage import (
    DevelopmentResourceBinding,
    DevelopmentStageInput,
    HTTPSDevelopmentStageAPI,
    _identity,
    _observed,
    _validate,
    development_documents,
)
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template

from loom.nebius_development_foundation import render_development_foundation


class DevelopmentLiveSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    preflight: DevelopmentPreflightSettings
    cloud: DevelopmentCloudScope
    operator_cloud_credentials: Path
    github_token_file: Path
    kubectl: Path


def _private(path: Path) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise DevelopmentInstallError("development private input unavailable")
    return private_state._private_read(path, limit=1024**2)


@contextmanager
def development_object_client(config: dict[str, Any], material: dict[str, str], *, source: bool) -> Iterator[Any]:
    """Explicit dev-only credentials; bounded HTTPS requests without write retry."""
    import boto3
    from botocore.config import Config

    endpoint = urlsplit(config["storage_endpoint"])
    if (endpoint.scheme != "https" or endpoint.netloc != f'storage.{config["region"]}.nebius.cloud'
            or endpoint.path not in {"", "/"} or endpoint.query or endpoint.fragment):
        raise DevelopmentInstallError("development object endpoint unqualified")
    prefix = "source-" if source else ""
    client = boto3.client("s3", endpoint_url=config["storage_endpoint"], region_name=config["region"],
        aws_access_key_id=material[prefix + "access-key"], aws_secret_access_key=material[prefix + "secret-key"],
        config=Config(retries={"total_max_attempts": 1, "mode": "standard"}, proxies={},
            connect_timeout=10, read_timeout=30, s3={"addressing_style": "path"}))

    def exact_endpoint(request: Any, **_kwargs: Any) -> None:
        url = request.url.decode() if isinstance(request.url, bytes) else request.url
        target = urlsplit(url)
        if (target.scheme, target.netloc) != (endpoint.scheme, endpoint.netloc):
            raise DevelopmentInstallError("development object request left qualified origin")

    try:
        client.meta.events.register_first("before-send.s3", exact_endpoint)
        yield client
    finally:
        client.close()


class HTTPSDevelopmentInstallationAPI:
    def __init__(self, *, request: DevelopmentInstallRequest, settings: DevelopmentLiveSettings,
                 api_server: str, ssl_context: ssl.SSLContext, token: str, state_dir: Path):
        self.diagnostic_stage: str | None = "configuration"
        try:
            self.request = copy.deepcopy(request)
            self.settings = settings.model_copy(deep=True)
            self.api_server, self.ssl_context, self.token = api_server, ssl_context, token
            self.state_dir = state_dir
            if not state_dir.is_absolute() or state_dir != state_dir.resolve():
                raise ValueError()
            selected = self.request.selection
            self.rendered = render_development_foundation(selected.config, selected.candidate, selected.profile,
                selected.keyring, repo_root=Path(__file__).resolve().parents[2])
            if (request.bootstrap.kube_system_uid != str(settings.preflight.kube_system_uid)
                    or selected.candidate["candidate_sha"] != settings.preflight.source.source_sha
                    or selected.config["kubernetes_api_server"].rstrip("/") != api_server.rstrip("/")
                    or settings.operator_cloud_credentials == settings.github_token_file):
                raise ValueError()
            self.private_inputs = {path: _private(path) for path in (
                settings.operator_cloud_credentials, settings.github_token_file)}
            # Qualify TLS/endpoint/token shape now, before an installer can write.
            with self.bootstrap_api():
                pass
        except Exception:
            raise DevelopmentInstallError("development live configuration unqualified") from None

    def _request(self, request: DevelopmentInstallRequest) -> None:
        if request != self.request:
            raise DevelopmentInstallError("development frozen request differs")
        if any(_private(path) != value for path, value in self.private_inputs.items()):
            raise DevelopmentInstallError("development private input changed")

    def _binding(self, binding: DevelopmentResourceBinding) -> None:
        if binding.bootstrap != self.request.bootstrap:
            raise DevelopmentInstallError("development live binding differs")

    def bootstrap_api(self) -> HTTPSDevelopmentBootstrapAPI:
        return HTTPSDevelopmentBootstrapAPI(binding=self.request.bootstrap, api_server=self.api_server,
            ssl_context=self.ssl_context, token=self.token)

    def resources(self, binding: DevelopmentResourceBinding, selection: DevelopmentStageInput,
                  phase: str) -> HTTPSDevelopmentStageAPI:
        self._binding(binding)
        if selection != self.request.selection:
            raise DevelopmentInstallError("development frozen selection differs")
        return HTTPSDevelopmentStageAPI(binding=binding, selection=selection, phase=phase,
            api_server=self.api_server, ssl_context=self.ssl_context, token=self.token)

    async def _qualify(self, *, fresh: bool) -> None:
        from nebius.sdk import SDK

        selected = self.request.selection
        self.diagnostic_stage = "source_capacity"
        with HTTPSDevelopmentPreflight(settings=self.settings.preflight, api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token) as checks:
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
                kwargs: dict[str, Any] = dict(config=selected.config, keyring=selected.keyring,
                    github_token=self.private_inputs[self.settings.github_token_file].decode().strip(), http=http)
                if fresh:
                    result = await checks.inspect(**kwargs)
                else:
                    result = await checks.inspect_installation(**kwargs,
                        installation_id=UUID(self.request.bootstrap.installation_id))
        if result.rendered != self.rendered:
            raise DevelopmentInstallError("development live source selection differs")
        self.diagnostic_stage = "cloud_identity"
        sdk = SDK(credentials_file_name=str(self.settings.operator_cloud_credentials),
            user_agent_prefix="loom-development-installer/1.0")
        try:
            await qualify_development_cloud(sdk=sdk, scope=self.settings.cloud, config=selected.config,
                material=selected.storage, pending_storage_mib=result.evidence["pending_storage_mib"])
            self._request(self.request)
        finally:
            await sdk.close()

    def qualify(self, request: DevelopmentInstallRequest, *, fresh: bool) -> None:
        try:
            self.diagnostic_stage = "configuration"
            self._request(request)
            asyncio.run(self._qualify(fresh=fresh))
            self.diagnostic_stage = "object_access"
            config, material = self.request.selection.config, self.request.selection.storage
            for source, buckets in ((False, {config["buckets"][name] for name in ("artifacts", "trajectories")}),
                                    (True, {config["buckets"]["source"]})):
                with development_object_client(config, material, source=source) as client:
                    for bucket in sorted(buckets):
                        response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                            raise ValueError()
            self._request(request)
            self.diagnostic_stage = None
        except Exception:
            raise DevelopmentInstallError("development live prerequisites unavailable") from None

    def _recorded(self, binding: DevelopmentResourceBinding, phase: str, key: str,
                  api: HTTPSDevelopmentStageAPI) -> dict[str, Any]:
        """Read an existing phase, never repair or regenerate its journal."""
        selection = self.request.selection
        revision, docs = development_documents(selection, binding, phase)
        record = json.loads(private_state._private_read(self.state_dir / phase / "stage.json", limit=4 * 1024**2))
        _validate(record, _identity(binding, phase, revision), docs)
        item = record["resources"][key]
        if item["status"] != "created":
            raise DevelopmentInstallError("development recorded resource incomplete")
        api.verify_identity(binding)
        return _observed(api, item)[2]

    @staticmethod
    def _object(row: dict[str, Any] | None, *, api: str, kind: str, name: str,
                namespace: str | None = "loom-dev") -> dict[str, Any]:
        if (row is None or row.get("apiVersion") != api or row.get("kind") != kind
                or row["metadata"].get("name") != name or row["metadata"].get("namespace") != namespace
                or row["metadata"].get("deletionTimestamp")):
            raise DevelopmentInstallError("development evidence identity differs")
        _uid(row)
        return row

    @staticmethod
    def _owned(row: dict[str, Any], owner: dict[str, Any]) -> None:
        refs = row["metadata"].get("ownerReferences", [])
        if (len(refs) != 1 or any(refs[0].get(key) != value for key, value in {
                "apiVersion": owner["apiVersion"], "kind": owner["kind"], "uid": _uid(owner),
                "name": owner["metadata"]["name"], "controller": True}.items())):
            raise DevelopmentInstallError("development evidence owner differs")

    def _database(self, binding: DevelopmentResourceBinding, observation: dict[str, Any]) -> dict[str, Any]:
        with self.resources(binding, self.request.selection, "database") as api:
            current = _storage_observation(self.request, binding, api)
            if current is None or any(observation.get(key) != value for key, value in current.items()):
                raise DevelopmentInstallError("development observed storage changed")
            controller = self._recorded(binding, "database", "StatefulSet:loom-postgres", api)
            claim, volume = api.get_database_claim(), api.get_database_volume()
            assert claim is not None and volume is not None
            if _uid(claim) != current["pvc_uid"] or _uid(volume) != current["pv_uid"]:
                raise DevelopmentInstallError("development storage identity changed")
            pod = self._object(api._request("GET", "/api/v1/namespaces/loom-dev/pods/loom-postgres-0"),
                api="v1", kind="Pod", name="loom-postgres-0")
            self._owned(pod, controller)
            expected = copy.deepcopy(controller["spec"]["template"]["spec"])
            expected.setdefault("volumes", []).append({"name": "data", "persistentVolumeClaim": {
                "claimName": "data-loom-postgres-0"}})
            actual = copy.deepcopy(pod["spec"])
            for value in (expected, actual):
                value["volumes"].sort(key=lambda row: row["name"])
            if not _matches_backup_template(actual, expected):
                raise DevelopmentInstallError("development database pod differs")
            node_name = pod["spec"]["nodeName"]
            if not isinstance(node_name, str) or re.fullmatch(r"computeinstance-[a-z0-9]+", node_name) is None:
                raise ValueError()
            node = self._object(api._request("GET", "/api/v1/nodes/" + node_name),
                api="v1", kind="Node", name=node_name, namespace=None)
            if node["spec"].get("providerID") != "nebius://" + node_name:
                raise ValueError()
            api.verify_identity(binding)
            return {"observation": current, "controller_uid": _uid(controller), "pod_uid": _uid(pod),
                "node_uid": _uid(node), "instance_id": node_name,
                "claim_created_at": claim["metadata"]["creationTimestamp"],
                "volume_created_at": volume["metadata"]["creationTimestamp"]}

    async def _disk(self, evidence: dict[str, Any]) -> None:
        from nebius.sdk import SDK

        spec = evidence["observation"]["pv_spec"]
        sdk = SDK(credentials_file_name=str(self.settings.operator_cloud_credentials),
            user_agent_prefix="loom-development-installer/1.0")
        try:
            await qualify_development_disk(sdk=sdk, scope=self.settings.cloud, disk_id=spec["csi"]["volumeHandle"],
                capacity_bytes=int(parse_quantity(spec["capacity"]["storage"])), instance_id=evidence["instance_id"],
                claim_created_at=evidence["claim_created_at"], volume_created_at=evidence["volume_created_at"])
        finally:
            await sdk.close()

    def qualify_volume(self, request: DevelopmentInstallRequest, binding: DevelopmentResourceBinding,
                       observation: dict[str, Any]) -> None:
        try:
            self.diagnostic_stage = "database_storage"
            self._request(request)
            self._binding(binding)
            before = self._database(binding, observation)
            self.diagnostic_stage = "provider_disk"
            asyncio.run(self._disk(before))
            self._request(request)
            if self._database(binding, observation) != before:
                raise ValueError()
            self.diagnostic_stage = None
        except Exception:
            raise DevelopmentInstallError("development physical database storage unavailable") from None

    def _service(self, binding: DevelopmentResourceBinding) -> dict[str, Any]:
        with self.resources(binding, self.request.selection, "services") as api:
            deployment = self._recorded(binding, "services", "Deployment:loom-service", api)
            labels = deployment["spec"]["selector"]["matchLabels"]
            query = urlencode({"labelSelector": ",".join(key + "=" + value for key, value in sorted(labels.items())), "limit": 2})
            listing = api._request("GET", "/api/v1/namespaces/loom-dev/pods?" + query)
            if (listing is None or listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                    or listing.get("metadata", {}).get("continue") or len(listing.get("items", [])) != 1):
                raise ValueError()
            row = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
            name = row["metadata"]["name"]
            if not isinstance(name, str) or re.fullmatch(r"loom-service-[a-z0-9-]{1,200}", name) is None:
                raise ValueError()
            pod = self._object(row, api="v1", kind="Pod", name=name)
            owners = pod["metadata"].get("ownerReferences", [])
            if len(owners) != 1:
                raise ValueError()
            replica_name = owners[0]["name"]
            if not isinstance(replica_name, str) or re.fullmatch(r"loom-service-[a-z0-9-]{1,200}", replica_name) is None:
                raise ValueError()
            replicas = self._object(api._request("GET", "/apis/apps/v1/namespaces/loom-dev/replicasets/" + replica_name),
                api="apps/v1", kind="ReplicaSet", name=replica_name)
            self._owned(pod, replicas)
            self._owned(replicas, deployment)
            expected = deployment["spec"]["template"]["spec"]
            if (not _matches_backup_template(replicas["spec"]["template"]["spec"], expected)
                    or not _matches_backup_template(pod["spec"], expected)):
                raise ValueError()
            status = pod.get("status", {})
            containers = status.get("containerStatuses", [])
            if (status.get("phase") != "Running" or len(containers) != 1 or containers[0].get("name") != "loom-service"
                    or containers[0].get("ready") is not True or not containers[0].get("containerID")
                    or "running" not in containers[0].get("state", {})):
                raise ValueError()
            api.verify_identity(binding)
            return {"pod_name": name, "pod_uid": _uid(pod), "replica_uid": _uid(replicas),
                "deployment_uid": _uid(deployment), "container_id": containers[0]["containerID"],
                "restarts": containers[0]["restartCount"]}

    def verify_private_dependencies(self, request: DevelopmentInstallRequest, binding: DevelopmentResourceBinding,
                                    material_state: Path) -> None:
        try:
            self.diagnostic_stage = "private_service"
            self._request(request)
            self._binding(binding)
            if material_state != self.state_dir / "bootstrap":
                raise ValueError()
            before = self._service(binding)
            probe_private_service(kubectl=self.settings.kubectl, api_server=self.api_server,
                ssl_context=self.ssl_context, token=self.token, pod_name=before["pod_name"],
                candidate=self.request.selection.candidate["candidate_sha"])
            self._request(request)
            if self._service(binding) != before:
                raise ValueError()
            self.diagnostic_stage = None
        except Exception:
            raise DevelopmentInstallError("development running API dependencies unavailable") from None
