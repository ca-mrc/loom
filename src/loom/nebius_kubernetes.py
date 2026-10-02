"""Explicit Kubernetes transport using renewable native Nebius credentials."""

from __future__ import annotations

import asyncio
import ipaddress
import re
import ssl
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import urlsplit

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
