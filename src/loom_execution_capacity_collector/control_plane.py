"""Authenticated, bounded publication to the Loom capacity API."""

from __future__ import annotations

import asyncio
import os
import stat
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from loom_execution_capacity_collector.contracts import (
    CapacityObservationReceipt,
    CapacityObservationV1,
    CapacityPolicyBinding,
)

_MAX_TOKEN_BYTES = 16 * 1024
_MAX_RESPONSE_BYTES = 256 * 1024


class CapacityPublicationError(RuntimeError):
    """The control plane did not verifiably accept an observation."""


def read_owner_only_secret(path: Path, *, maximum_bytes: int = _MAX_TOKEN_BYTES) -> str:
    metadata = path.lstat()
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not 0 < metadata.st_size <= maximum_bytes
    ):
        raise ValueError("credential must be a bounded current-UID-owned 0600 regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
            or not stat.S_ISREG(opened.st_mode)
        ):
            raise ValueError("credential metadata changed while opening")
        chunks: list[bytes] = []
        total = 0
        while total <= maximum_bytes:
            chunk = os.read(descriptor, min(4096, maximum_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        raw = b"".join(chunks)
    finally:
        os.close(descriptor)
    if not raw or len(raw) > maximum_bytes:
        raise ValueError("credential is empty or exceeds its size bound")
    try:
        value = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("credential must be UTF-8") from exc
    if not value:
        raise ValueError("credential is empty")
    return value


def _origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("control-plane URL must be a credential-free HTTP(S) origin")
    return value.rstrip("/")


def _bounded_json(response: httpx.Response) -> Any:
    if len(response.content) > _MAX_RESPONSE_BYTES:
        raise CapacityPublicationError("control-plane response exceeded its size bound")
    try:
        return response.json()
    except ValueError as exc:
        raise CapacityPublicationError("control-plane response was not JSON") from exc


class CapacityControlPlaneClient:
    def __init__(
        self,
        *,
        origin: str,
        bearer_token_file: Path,
        timeout_seconds: float,
        attempts: int,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._origin = _origin(origin)
        self._token = read_owner_only_secret(bearer_token_file)
        self._attempts = attempts
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def fetch_policy(self, *, target_id: str, pool_id: str) -> CapacityPolicyBinding:
        response = await self._client.get(
            f"{self._origin}/admin/execution-capacity-collector-policy/{target_id}",
            params={"pool_id": pool_id},
            headers=self._headers,
        )
        if response.status_code != 200:
            raise CapacityPublicationError(
                f"capacity policy read failed with HTTP {response.status_code}"
            )
        payload = _bounded_json(response)
        if not isinstance(payload, dict):
            raise CapacityPublicationError("capacity policy response has an invalid shape")
        if payload.get("target_id") != target_id or payload.get("pool_id") != pool_id:
            raise CapacityPublicationError("capacity policy binding is unavailable")
        try:
            return CapacityPolicyBinding(
                target_id=target_id,
                target_scope=payload.get("target_scope"),
                pool_id=pool_id,
                enabled=payload["enabled"],
                max_nodes=payload["max_nodes"],
                node_cpu_millis=payload["node_cpu_millis"],
                node_memory_mib=payload["node_memory_mib"],
                node_storage_mib=payload["node_storage_mib"],
                version=payload["version"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CapacityPublicationError("capacity policy binding is invalid") from exc

    async def publish(self, observation: CapacityObservationV1) -> CapacityObservationReceipt:
        payload = observation.model_dump(mode="json")
        last_error: Exception | None = None
        for attempt in range(self._attempts):
            try:
                response = await self._client.post(
                    f"{self._origin}/admin/execution-capacity-observations",
                    headers=self._headers,
                    json=payload,
                )
                if response.status_code == 200:
                    _bounded_json(response)
                    receipt = CapacityObservationReceipt.model_validate_json(response.content)
                    if (
                        receipt.target_id != observation.target_id
                        or receipt.source != observation.source
                        or receipt.source_version != observation.source_version
                        or receipt.observed_at != observation.observed_at
                    ):
                        raise CapacityPublicationError(
                            "capacity observation receipt does not match the request"
                        )
                    return receipt
                if response.status_code < 500:
                    raise CapacityPublicationError(
                        f"capacity observation was rejected with HTTP {response.status_code}"
                    )
                last_error = CapacityPublicationError(
                    f"capacity observation failed with HTTP {response.status_code}"
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
            if attempt + 1 < self._attempts:
                await asyncio.sleep(min(2**attempt, 4))
        raise CapacityPublicationError(
            "capacity observation acceptance is unconfirmed"
        ) from last_error

    async def close(self) -> None:
        self._token = ""
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> CapacityControlPlaneClient:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()


__all__ = [
    "CapacityControlPlaneClient",
    "CapacityPublicationError",
    "read_owner_only_secret",
]
