"""Dedicated HTTPS observer transport: bounded streaming, no redirect or retry."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from loom_execution_capacity_collector.control_plane import read_owner_only_secret
from loom_execution_capacity_collector.pool_contracts import (
    MAX_POOL_OBSERVATION_BYTES,
    PoolCaptureV1,
    PoolObservationReceiptV1,
    PoolObservationV1,
)


class PoolPublicationError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("pool observation acceptance is unconfirmed")


class PoolObservationClient:
    def __init__(self, *, origin: str, bearer_token_file: Path, timeout_seconds: float,
                 client: httpx.AsyncClient | None = None) -> None:
        parsed = urlsplit(origin)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in {"", "/"}
                or parsed.query or parsed.fragment or not 0 < timeout_seconds <= 60):
            raise ValueError("pool management URL must be a credential-free HTTPS origin")
        self._origin = origin.rstrip("/")
        self._token = read_owner_only_secret(bearer_token_file)
        self._timeout = timeout_seconds
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=False, trust_env=False)

    async def _post(self, pool_id: UUID, operation: str, body: bytes) -> bytes:
        if not pool_id.int or len(body) > MAX_POOL_OBSERVATION_BYTES:
            raise PoolPublicationError
        try:
            async with asyncio.timeout(self._timeout):
                async with self._client.stream("POST", f"{self._origin}/internal/pools/v1/{pool_id}/{operation}",
                    headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json",
                             "Accept-Encoding": "identity"}, content=body, follow_redirects=False,
                    timeout=self._timeout) as response:
                    if response.status_code != 200 or response.headers.get("content-encoding", "identity") != "identity":
                        raise PoolPublicationError
                    declared = response.headers.get("content-length")
                    if declared is not None and (not declared.isdigit() or int(declared) > MAX_POOL_OBSERVATION_BYTES):
                        raise PoolPublicationError
                    result = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(result) + len(chunk) > MAX_POOL_OBSERVATION_BYTES:
                            raise PoolPublicationError
                        result.extend(chunk)
                    return bytes(result)
        except (httpx.HTTPError, TimeoutError, ValueError):
            # Never include upstream body, URL, credential or exception text.
            raise PoolPublicationError from None

    async def issue_capture(self, pool_id: UUID) -> PoolCaptureV1:
        body = await self._post(pool_id, "captures", b"{}")
        try:
            capture = PoolCaptureV1.model_validate_json(body)
            if capture.pool_id != pool_id:
                raise PoolPublicationError
            return capture
        except ValueError:
            raise PoolPublicationError from None

    async def publish(self, pool_id: UUID, observation: PoolObservationV1) -> PoolObservationReceiptV1:
        body = json.dumps(observation.payload(), sort_keys=True, separators=(",", ":")).encode()
        response = await self._post(pool_id, "observations", body)
        try:
            receipt = PoolObservationReceiptV1.model_validate_json(response)
            if (receipt.capture_id != observation.capture_id or receipt.observation_sha256 != observation.digest()):
                raise PoolPublicationError
            return receipt
        except ValueError:
            raise PoolPublicationError from None

    async def close(self) -> None:
        self._token = ""
        if self._owns_client:
            await self._client.aclose()
