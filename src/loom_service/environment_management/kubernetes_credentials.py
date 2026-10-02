"""Explicit projected identity; Kubernetes renews the token, never a cloud key."""
from __future__ import annotations

import os
import re
import ssl
import stat
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from loom.nebius_kubernetes import NebiusKubernetesConnection


class ProjectedKubernetesConnection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["projected_service_account"]
    endpoint: str
    ca_file: Path
    token_file: Path

    @field_validator("endpoint")
    @classmethod
    def _endpoint(cls, value: str) -> str:
        return NebiusKubernetesConnection._endpoint(value)


class ProjectedKubernetesCredentials:
    def __init__(self, connection: ProjectedKubernetesConnection):
        self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.ssl_context.load_verify_locations(cafile=str(connection.ca_file))
        self.token_file = connection.token_file

    async def get_token(self) -> str:
        return self.get_token_sync()

    def get_token_sync(self) -> str:
        try:
            # Reopen the projected symlink on every request so kubelet rotation
            # replaces credentials. Nonblocking open also rejects special files
            # without waiting for a writer; validate the actual opened inode.
            descriptor = os.open(self.token_file, os.O_RDONLY | os.O_NONBLOCK)
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o027:
                    raise ValueError()
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    raw = stream.read(16385)
            finally:
                os.close(descriptor)
            if not 0 < len(raw) <= 16384:
                raise ValueError()
            token = raw.decode("ascii").strip()
            if re.fullmatch(r"[A-Za-z0-9._~+/-]+={0,2}", token) is None:
                raise ValueError()
            return token
        except Exception:
            raise ValueError("projected Kubernetes credential unavailable") from None

    async def close(self) -> None:
        # All file descriptors are request-local. Match the owned credentials
        # lifecycle used by the explicit SDK mode without retaining token bytes.
        return None


def create_projected_api_client(connection: ProjectedKubernetesConnection) -> tuple[Any, ProjectedKubernetesCredentials]:
    """Explicit SDK transport; refresh the kubelet-projected token per request.

    The caller owns the returned client/credentials. This creates no API object
    and grants no Kubernetes permissions; RBAC remains separately installed.
    """
    from kubernetes import client

    credentials = ProjectedKubernetesCredentials(connection)
    configuration = client.Configuration()
    configuration.host = connection.endpoint
    configuration.ssl_ca_cert = str(connection.ca_file)
    configuration.verify_ssl = True
    configuration.api_key["authorization"] = ""
    configuration.api_key_prefix["authorization"] = "Bearer"

    def refresh(current: Any) -> None:
        current.api_key["authorization"] = credentials.get_token_sync()

    configuration.refresh_api_key_hook = refresh
    return client.ApiClient(configuration), credentials
