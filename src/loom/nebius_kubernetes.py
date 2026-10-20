"""Explicit Kubernetes transport using renewable native Nebius credentials."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import math
import re
import ssl
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any, TypeVar
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, field_validator

_ReadResult = TypeVar("_ReadResult")


async def read_sdk_before_close(read: Callable[[], _ReadResult]) -> _ReadResult:
    """Retain a bounded SDK read through cancellation before its owner closes.

    The caller supplies per-request SDK timeouts. This does not add retries or
    make mutations cancellation-safe; only read-only operations use it.
    """
    reading = asyncio.create_task(asyncio.to_thread(read))
    try:
        return await asyncio.shield(reading)
    except asyncio.CancelledError:
        while not reading.done():
            try:
                await asyncio.shield(reading)
            except (asyncio.CancelledError, Exception):
                pass
        if not reading.cancelled():
            reading.exception()
        raise


class NebiusKubernetesConnection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    endpoint: str
    ca_file: Path
    credentials_file: Path

    @field_validator("endpoint")
    @classmethod
    def _endpoint(cls, value: str) -> str:
        try:
            parsed = urlsplit(value)
            valid = (
                parsed.scheme == "https"
                and parsed.hostname
                and parsed.username is None
                and parsed.password is None
                and parsed.path in ("", "/")
                and not parsed.query
                and not parsed.fragment
                and not any(character.isspace() for character in value)
                and "\\" not in value
            )
            if parsed.port is not None and not 1 <= parsed.port <= 65535:
                valid = False
            host = parsed.hostname or ""
            try:
                ipaddress.ip_address(host)
            except ValueError:
                if len(host) > 253 or not all(
                    re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
                    for label in host.rstrip(".").split(".")
                ):
                    valid = False
        except ValueError:
            valid = False
        if not valid:
            raise ValueError("Kubernetes endpoint must be an explicit HTTPS origin")
        return value.rstrip("/")


def connection_from_fields(
    endpoint: str | None, ca_file: Path | None, credentials_file: Path | None
) -> NebiusKubernetesConnection | None:
    values = (endpoint, ca_file, credentials_file)
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError("remote Kubernetes endpoint, CA and credentials must be set together")
    assert endpoint is not None and ca_file is not None and credentials_file is not None
    return NebiusKubernetesConnection(
        endpoint=endpoint, ca_file=ca_file, credentials_file=credentials_file
    )


class NebiusKubernetesCredentials:
    """One SDK owns credential exchange, caching, renewal and shutdown."""

    def __init__(self, connection: NebiusKubernetesConnection, *, sdk_factory: Any = None) -> None:
        # Check the trust anchor before creating a client; never use ambient CAs
        # or an in-cluster fallback when explicit remote configuration is invalid.
        self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.ssl_context.load_verify_locations(cafile=str(connection.ca_file))
        metadata = connection.credentials_file.stat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_mode & 0o027
            or not 0 < metadata.st_size <= 1024 * 1024
        ):
            raise ValueError("Nebius credentials must be a private bounded regular file")
        if sdk_factory is None:
            from nebius.sdk import SDK

            sdk_factory = SDK
        self._sdk = sdk_factory(
            credentials_file_name=str(connection.credentials_file),
            user_agent_prefix="loom-nebius-kubernetes/1.0",
        )

    @staticmethod
    def _usable(token: Any) -> str:
        if not token.token or token.expiration is None or token.expiration <= datetime.now(UTC):
            raise RuntimeError("Nebius Kubernetes token is empty or expired")
        return str(token.token)

    async def get_token(self) -> str:
        return self._usable(await self._sdk.get_token(timeout=30))

    def get_token_sync(self) -> str:
        return self._usable(self._sdk.get_token_sync(timeout=30))

    async def close(self) -> None:
        await self._sdk.close()


def create_api_client(
    connection: NebiusKubernetesConnection,
) -> tuple[Any, NebiusKubernetesCredentials]:
    from kubernetes import client

    credentials = NebiusKubernetesCredentials(connection)
    configuration = client.Configuration()
    configuration.host = connection.endpoint
    configuration.ssl_ca_cert = str(connection.ca_file)
    configuration.verify_ssl = True
    # Kubernetes auth_settings only invokes the hook for registered API keys.
    # The empty placeholder is replaced before every authenticated request.
    configuration.api_key["authorization"] = ""
    configuration.api_key_prefix["authorization"] = "Bearer"

    def refresh(current: Any) -> None:
        current.api_key["authorization"] = credentials.get_token_sync()

    configuration.refresh_api_key_hook = refresh
    return client.ApiClient(configuration), credentials


def read_kubelet_summary(
    core_api: Any, *, node_name: str, expected_node_uid: str | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Read one identity-bound kubelet Summary using nodes/stats only.

    Reuse the client's current bearer and cluster CA. No administrator authority,
    redirects, ambient proxy or node-proxy fallback is accepted. The caller owns
    cancellation-safe lifetime and projects only the fields it needs. One
    monotonic budget is shared across stages; no request timeout exceeds ten
    seconds. Existing synchronous SDK token renewal retains its own 30-second
    boundary and cannot be preempted by this read's deadline. An in-progress
    socket phase can also finish after the cutoff; no later stage is started.
    """
    if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (float, int))
            or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 10):
        raise ValueError("kubelet summary timeout must be positive and at most ten seconds")
    deadline = monotonic() + timeout_seconds

    def remaining() -> float:
        value = deadline - monotonic()
        if value <= 0:
            raise TimeoutError("kubelet summary observation deadline exceeded")
        return value

    if (not isinstance(node_name, str) or len(node_name) > 253
            or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", node_name)):
        raise ValueError("unqualified kubelet node")
    if expected_node_uid is not None and (not isinstance(expected_node_uid, str)
            or str(UUID(expected_node_uid)) != expected_node_uid or not UUID(expected_node_uid).int):
        raise ValueError("unqualified expected kubelet node identity")
    node = core_api.read_node(name=node_name, _request_timeout=remaining())
    remaining()
    if (node.metadata.name != node_name or not node.metadata.uid
            or node.metadata.deletion_timestamp is not None
            or (expected_node_uid is not None and node.metadata.uid != expected_node_uid)):
        raise ValueError("unqualified kubelet node identity")
    address, = (row.address for row in node.status.addresses if row.type == "InternalIP")
    endpoint = ipaddress.ip_address(address)
    if (not endpoint.is_private or endpoint.is_loopback or endpoint.is_link_local
            or endpoint.is_unspecified or endpoint.is_multicast):
        raise ValueError("unqualified kubelet address")
    configuration = core_api.api_client.configuration
    authorization = configuration.auth_settings().get("BearerToken", {}).get("value")
    remaining()
    if (configuration.verify_ssl is not True or not configuration.ssl_ca_cert
            or not isinstance(authorization, str) or authorization[:7].lower() != "bearer "
            or not authorization[7:] or any(character.isspace() for character in authorization[7:])):
        raise ValueError("unqualified kubelet TLS or bearer authority")
    trust = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
    host = "[" + str(endpoint) + "]" if endpoint.version == 6 else str(endpoint)
    with httpx.Client(verify=trust, timeout=remaining(), trust_env=False, follow_redirects=False) as http:
        with http.stream("GET", "https://" + host + ":10250/stats/summary",
                         headers={"Authorization": authorization}, timeout=remaining()) as response:
            remaining()
            response.raise_for_status()
            content = bytearray()
            # Check each received chunk, not a large rebuffered output chunk:
            # a slow trickle must not renew the observation's total deadline.
            for chunk in response.iter_bytes():
                remaining()
                if len(content) + len(chunk) > 16 * 1024**2:
                    raise ValueError("kubelet summary exceeds its bound")
                content.extend(chunk)
            remaining()
            result = json.loads(content)
            remaining()
    if not isinstance(result, dict) or result.get("node", {}).get("nodeName") != node_name:
        raise ValueError("kubelet summary node identity differs")
    return result
