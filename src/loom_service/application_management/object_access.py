"""Read-only, signed object-service proof that an old key is rejected.

IAM deletion alone is not data-plane propagation evidence. Neither a generic
AccessDenied nor a transport failure proves the credential is no longer valid.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError


class ApplicationObjectAccessVerifier:
    def __init__(self, http: httpx.AsyncClient):
        url = http.base_url
        if url.scheme != "https" or url.path != "/" or url.query or url.fragment or url.userinfo:
            raise ValueError("application object endpoint must be an HTTPS origin")
        self.http = http

    async def verify_retired(self, plan: dict[str, Any], storage: dict[str, str]) -> None:
        """Probe the original frozen endpoint; no redirects or write requests."""
        try:
            deployment = next(doc for docs in plan["files"].values() for doc in docs
                              if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")
            entries = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
            values = {entry["name"]: entry.get("value") for entry in entries}
            endpoint = httpx.URL(values["LOOM_SVC_MINIO_ENDPOINT"])
            region, bucket = values["LOOM_SVC_MINIO_REGION"], values["LOOM_SVC_ARTIFACTS_BUCKET"]
            if (len(values) != len(entries)
                    or endpoint.copy_with(path="/") != self.http.base_url.copy_with(path="/")
                    or endpoint.path not in {"", "/"} or not re.fullmatch(r"[a-z0-9-]{1,63}", region)
                    or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket)
                    or set(storage) != {"access-key", "secret-key"}
                    or any(not isinstance(value, str) or not 1 <= len(value) <= 4096
                           or not value.isascii() or any(ord(char) < 33 for char in value)
                           for value in storage.values())):
                raise ValueError
        except (ValueError, TypeError, KeyError, IndexError, StopIteration):
            raise ProviderBlockedError("application_object_access_binding_conflict") from None
        url = self.http.base_url.join(bucket).copy_with(params={
            "list-type": "2", "max-keys": "1", "prefix": "loom-application-access-probe/"})
        request = AWSRequest(method="GET", url=str(url))
        S3SigV4Auth(Credentials(storage["access-key"], storage["secret-key"]), "s3", region).add_auth(request)
        try:
            response = await self.http.get(str(url), headers=dict(request.headers),
                                           follow_redirects=False, timeout=30)
            if response.status_code == 403 and len(response.content) <= 16384:
                root = ET.fromstring(response.content)
                if root.tag == "Error" and root.findtext("Code") == "InvalidAccessKeyId":
                    return
        except (httpx.TransportError, ET.ParseError):
            pass
        raise ProviderWaitingError("application_object_access_retirement_pending")
