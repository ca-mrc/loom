"""Connect the private dev installer to authenticated reads and fixed phases.

No caller readiness flags, ambient credentials or staging adapters. The protected
entrypoint must authenticate the bundle and private inputs before constructing it.
"""
from __future__ import annotations

import asyncio
import copy
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_bootstrap import HTTPSDevelopmentBootstrapAPI
from scripts.ops.nebius_development_cloud import DevelopmentCloudScope, qualify_development_cloud
from scripts.ops.nebius_development_install import (
    DevelopmentInstallError,
    DevelopmentInstallRequest,
)
from scripts.ops.nebius_development_preflight import (
    DevelopmentPreflightSettings,
    HTTPSDevelopmentPreflight,
)
from scripts.ops.nebius_development_stage import (
    DevelopmentResourceBinding,
    DevelopmentStageInput,
    HTTPSDevelopmentStageAPI,
)

from loom.nebius_development_foundation import render_development_foundation


class DevelopmentLiveSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    preflight: DevelopmentPreflightSettings
    cloud: DevelopmentCloudScope
    operator_cloud_credentials: Path
    github_token_file: Path


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
                 api_server: str, ssl_context: ssl.SSLContext, token: str):
        self.diagnostic_stage: str | None = "configuration"
        try:
            self.request = copy.deepcopy(request)
            self.settings = settings.model_copy(deep=True)
            self.api_server, self.ssl_context, self.token = api_server, ssl_context, token
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
