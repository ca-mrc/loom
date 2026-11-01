"""Bounded task TCP tunnels; the only dialer lives outside the task Pod network."""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.websockets import WebSocket, WebSocketDisconnect

from loom.models.networking import WebDestination

logger = logging.getLogger(__name__)
_FRAME_BYTES = 64 * 1024
_MAX_BYTES = 1024**3
_IDLE_SECONDS = 60
_FENCE_SECONDS = 5
# Translation/tunnel ranges can encode otherwise forbidden IPv4 destinations.
_TRANSITION_RANGES = tuple(ipaddress.ip_network(value) for value in (
    "64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32", "192.88.99.0/24",
))


class TaskEgressConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    # Required even when the cluster has private-only API access; operators
    # explicitly inventory platform destinations before enabling this listener.
    protected_cidrs: tuple[str, ...] = Field(min_length=1, max_length=256)
    maximum_connections: int = Field(default=64, ge=1, le=256)
    # Parallel package downloads also retain metadata and redirect connections.
    maximum_connections_per_lease: int = Field(default=32, ge=1, le=32)

    @field_validator("protected_cidrs")
    @classmethod
    def valid_cidrs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for item in value:
            ipaddress.ip_network(item, strict=True)
        return value


class EgressDeniedError(Exception):
    """Fixed public diagnostic; never exposes credentials or upstream payloads."""


async def resolve_destination(
    destination: WebDestination,
    protected_cidrs: tuple[str, ...],
    *,
    resolver: Callable[..., Awaitable[Any]] | None = None,
) -> tuple[str, ...]:
    resolve = resolver or asyncio.get_running_loop().getaddrinfo
    try:
        async with asyncio.timeout(5):
            answers = await resolve(destination.host, destination.port, type=socket.SOCK_STREAM)
    except (OSError, TimeoutError) as exc:
        raise EgressDeniedError("destination_dns_failed") from exc
    addresses = set()
    protected = tuple(ipaddress.ip_network(item) for item in protected_cidrs)
    for answer in answers:
        try:
            address = ipaddress.ip_address(answer[4][0])
        except ValueError as exc:
            raise EgressDeniedError("destination_address_denied") from exc
        if (not address.is_global or address.is_multicast or address.is_reserved
            or getattr(address, "is_site_local", False) or "%" in str(address)
            or getattr(address, "ipv4_mapped", None) is not None
            or any(address.version == block.version and address in block
                   for block in (*_TRANSITION_RANGES, *protected))):
            raise EgressDeniedError("destination_address_denied")
        addresses.add(str(address))
    if not addresses or len(addresses) > 64:
        raise EgressDeniedError("destination_dns_failed")
    return tuple(sorted(addresses))


async def connect_destination(destination: WebDestination, protected_cidrs: tuple[str, ...]) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    addresses = await resolve_destination(destination, protected_cidrs)
    try:
        async with asyncio.timeout(10):
            for address in addresses:
                try:
                    # A numeric address is pinned; no second DNS resolution and
                    # no environment proxy or provider credentials participate.
                    return await asyncio.open_connection(address, destination.port, limit=_FRAME_BYTES)
                except OSError:
                    continue
    except TimeoutError as exc:
        raise EgressDeniedError("destination_connect_timeout") from exc
    raise EgressDeniedError("destination_connect_failed")


class TaskEgressRuntime:
    def __init__(self, config: TaskEgressConfig) -> None:
        self.config = config
        self.connections: dict[str, int] = {}

    def acquire(self, lease: str) -> None:
        if (sum(self.connections.values()) >= self.config.maximum_connections
            or self.connections.get(lease, 0) >= self.config.maximum_connections_per_lease):
            raise EgressDeniedError("task_egress_capacity_exceeded")
        self.connections[lease] = self.connections.get(lease, 0) + 1

    def release(self, lease: str) -> None:
        count = self.connections[lease] - 1
        if count:
            self.connections[lease] = count
        else:
            del self.connections[lease]


async def relay(
    websocket: WebSocket, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    *, deadline: datetime, reauthorize: Callable[[], Awaitable[None]],
) -> None:
    last_activity = asyncio.get_running_loop().time()

    async def upload() -> None:
        nonlocal last_activity
        total = 0
        while True:
            payload = await websocket.receive_bytes()
            last_activity = asyncio.get_running_loop().time()
            total += len(payload)
            if not payload or len(payload) > _FRAME_BYTES or total > _MAX_BYTES:
                raise EgressDeniedError("task_egress_transfer_limit")
            writer.write(payload)
            async with asyncio.timeout(_IDLE_SECONDS):
                await writer.drain()

    async def download() -> None:
        nonlocal last_activity
        total = 0
        while True:
            payload = await reader.read(_FRAME_BYTES)
            if not payload:
                return
            total += len(payload)
            if total > _MAX_BYTES:
                raise EgressDeniedError("task_egress_transfer_limit")
            async with asyncio.timeout(_IDLE_SECONDS):
                await websocket.send_bytes(payload)
            last_activity = asyncio.get_running_loop().time()

    async def fences() -> None:
        while True:
            await asyncio.sleep(_FENCE_SECONDS)
            if asyncio.get_running_loop().time() - last_activity >= _IDLE_SECONDS:
                raise EgressDeniedError("task_egress_idle_timeout")
            await reauthorize()

    tasks = [asyncio.create_task(function()) for function in (upload, download, fences)]
    try:
        seconds = min(900, (deadline - datetime.now(UTC)).total_seconds())
        if seconds <= 0:
            raise EgressDeniedError("task_egress_deadline")
        async with asyncio.timeout(seconds):
            completed, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in completed:
                task.result()
    except (WebSocketDisconnect, ConnectionError):
        return
    except TimeoutError as exc:
        raise EgressDeniedError("task_egress_timeout") from exc
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        writer.close()
        await writer.wait_closed()
