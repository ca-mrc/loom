"""Scoped signed data-plane checks, never independent retirement authority.

Nebius AccessDenied does not prove universal key invalidity. Retirement combines
fresh exact IAM absence (the credential provider) with original-key denial in
every protected bucket. Successful ordinary-key probes qualify the same requests.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from loom.nebius_application_contract import ApplicationRegistrationV1, SharedDevelopmentBindingV1
from loom.nebius_environment_contract import FoundationBinding
from loom_service.application_management.cloud_effects import ApplicationStorageAccessV1
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError


def _scalar(root: ET.Element, name: str) -> str | None:
    rows = root.findall(name)
    return rows[0].text if len(rows) == 1 and not len(rows[0]) else None


class ApplicationObjectAccessVerifier:
    def __init__(self, http: httpx.AsyncClient, *, foundation: FoundationBinding,
                 shared: SharedDevelopmentBindingV1, storage: ApplicationStorageAccessV1):
        url = http.base_url
        if url.scheme != "https" or url.path != "/" or url.query or url.fragment or url.userinfo:
            raise ValueError("application object endpoint must be an HTTPS origin")
        # Protected installation and supported refresh preserve this scope from
        # the original upgrade. Never derive a missing source scope from a UUID
        # alone or amend historical frozen operation plans to invent one.
        foundation = FoundationBinding.model_validate(foundation.model_dump())
        self.shared = SharedDevelopmentBindingV1.model_validate(shared.model_dump())
        self.storage = ApplicationStorageAccessV1.model_validate(storage.model_dump())
        self.shared.validate_foundation(foundation)
        config = foundation.platform_config
        if self.storage.data_environment_id != self.shared.data_environment_id:
            raise ValueError("application object scope differs from shared data")
        self.endpoint, self.region = httpx.URL(config["storage_endpoint"]), config["region"]
        self.artifacts, self.trajectories, self.source = (
            config["buckets"][name] for name in ("artifacts", "trajectories", "source"))
        self.http = http

    def _binding(self, plan: dict[str, Any], storage: dict[str, str]) -> tuple[str, ...]:
        try:
            row = ApplicationRegistrationV1.model_validate(plan["registration"])
            shared = SharedDevelopmentBindingV1.model_validate(plan["shared"])
            if (row.data_environment_id != shared.data_environment_id
                    or row.data_environment_id != self.shared.data_environment_id
                    or row.cluster_id != shared.cluster_id or row.cluster_id != self.shared.cluster_id
                    or shared.platform_namespace != self.shared.platform_namespace):
                raise ValueError
            deployment = next(doc for docs in plan["files"].values() for doc in docs
                              if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")
            entries = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
            values = {entry["name"]: entry.get("value") for entry in entries}
            endpoint = httpx.URL(values["LOOM_SVC_MINIO_ENDPOINT"])
            if (len(values) != len(entries)
                    or endpoint.copy_with(path="/") != self.http.base_url.copy_with(path="/")
                    or endpoint.copy_with(path="/") != self.endpoint.copy_with(path="/")
                    or endpoint.path not in {"", "/"}
                    or values["LOOM_SVC_MINIO_REGION"] != self.region
                    or not re.fullmatch(r"[a-z0-9-]{1,63}", self.region)
                    or values["LOOM_SVC_ARTIFACTS_BUCKET"] != self.artifacts
                    or values["LOOM_SVC_TRAJECTORIES_BUCKET"] != self.trajectories
                    or any(not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", name)
                           for name in (self.artifacts, self.trajectories, self.source))
                    or set(storage) != {"access-key", "secret-key"}
                    or any(not isinstance(value, str) or not 1 <= len(value) <= 4096
                           or not value.isascii() or any(ord(char) < 33 for char in value)
                           for value in storage.values())):
                raise ValueError
        except (ValueError, TypeError, KeyError, IndexError, StopIteration):
            raise ProviderBlockedError("application_object_access_binding_conflict") from None
        return tuple(sorted({self.artifacts, self.trajectories, self.source}))

    async def _probe(self, bucket: str, storage: dict[str, str], *, active: bool) -> bool:
        url = self.http.base_url.join(bucket).copy_with(params={
            "list-type": "2", "max-keys": "1", "prefix": "loom-application-access-probe/"})
        request = AWSRequest(method="GET", url=str(url), headers={"Accept-Encoding": "identity"})
        S3SigV4Auth(Credentials(storage["access-key"], storage["secret-key"]), "s3", self.region).add_auth(request)
        try:
            async with self.http.stream("GET", str(url), headers=dict(request.headers), auth=None,
                                        follow_redirects=False, timeout=30) as response:
                if (response.status_code != (200 if active else 403)
                        or response.headers.get("content-encoding", "identity").lower() != "identity"):
                    return False
                content = bytearray()
                async for part in response.aiter_bytes(chunk_size=16384):
                    if len(content) + len(part) > 16384:
                        return False
                    content.extend(part)
            root = ET.fromstring(content)
            if not active:
                return root.tag == "Error" and _scalar(root, "Code") in {"InvalidAccessKeyId", "AccessDenied"}
            namespace = "{http://s3.amazonaws.com/doc/2006-03-01/}" if root.tag.startswith("{") else ""
            contents = root.findall(namespace + "Contents")
            count = _scalar(root, namespace + "KeyCount")
            keys = [_scalar(item, namespace + "Key") for item in contents]
            return (root.tag == namespace + "ListBucketResult"
                    and _scalar(root, namespace + "Name") == bucket
                    and _scalar(root, namespace + "Prefix") == "loom-application-access-probe/"
                    and _scalar(root, namespace + "MaxKeys") == "1"
                    and count in {"0", "1"} and len(contents) == int(count)
                    and _scalar(root, namespace + "IsTruncated") in {"true", "false"}
                    and not root.findall(namespace + "CommonPrefixes")
                    and all(key is not None and key.startswith("loom-application-access-probe/") for key in keys))
        except (httpx.RequestError, ET.ParseError, LookupError, ValueError):
            return False

    async def verify_retired(self, plan: dict[str, Any], storage: dict[str, str]) -> None:
        """Only call after fresh exact key/account/membership absence checks."""
        for bucket in self._binding(plan, storage):
            if not await self._probe(bucket, storage, active=False):
                raise ProviderWaitingError("application_object_access_retirement_pending")

    async def verify_active(self, plan: dict[str, Any], storage: dict[str, str]) -> None:
        """Qualify the identical safe ListObjectsV2 request with current access."""
        for bucket in self._binding(plan, storage):
            if not await self._probe(bucket, storage, active=True):
                raise ProviderWaitingError("application_object_access_not_ready")
