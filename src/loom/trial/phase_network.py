"""_phase_network — async context manager that temporarily switches the
driver's network policy for the duration of a phase and restores the
baseline on exit (spec §3.4).

asyncio.shield on the restore so cancellation during the phase still
restores the baseline rather than leaving the container in an unknown
network state.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

from loom.driver.base import Driver
from loom.models.networking import Allowlist, NetworkPolicy, NoNetwork, Public


@contextlib.asynccontextmanager
async def phase_network(
    driver: Driver, *, baseline: NetworkPolicy, phase: NetworkPolicy,
    set_policy: Callable[[Driver, NetworkPolicy], Awaitable[None]] | None = None,
) -> AsyncIterator[None]:
    if phase == baseline:
        yield
        return
    if set_policy is None:
        await driver.set_network_policy(phase)
    else:
        await set_policy(driver, phase)
    try:
        yield
    finally:
        restore = driver.set_network_policy(baseline) if set_policy is None else set_policy(driver, baseline)
        await asyncio.shield(restore)


def controlled_tool_policy_setter(
    set_gateway_policy: Callable[[NetworkPolicy], Awaitable[None]], proxy_url: str,
) -> Callable[[Driver, NetworkPolicy], Awaitable[None]]:
    """Preserve legacy namespace rules while the gateway checks actual targets.

    Allowlist needs a transport exception for the proxy IP; the gateway still
    limits the ultimate hosts/CIDRs. NoNetwork continues to block even direct
    internal connections. Public restoration clears only this attempt's rules.
    """
    from ipaddress import ip_address
    from urllib.parse import urlsplit

    from loom.errors import DriverError

    address = ip_address(urlsplit(proxy_url).hostname or "")
    proxy_cidr = str(address) + ("/32" if address.version == 4 else "/128")
    restricted: dict[int, Driver] = {}

    async def apply(driver: Driver, policy: NetworkPolicy) -> None:
        await set_gateway_policy(policy)
        namespace_policy = policy
        if isinstance(policy, Allowlist):
            namespace_policy = policy.model_copy(update={"cidrs": (*policy.cidrs, proxy_cidr)})
        if isinstance(namespace_policy, (Allowlist, NoNetwork)):
            await driver.set_network_policy(namespace_policy)
            restricted[id(driver)] = driver
        elif id(driver) in restricted:
            result = await driver.exec(
                "iptables -P OUTPUT ACCEPT && iptables -F OUTPUT", user="root", timeout_sec=30,
            )
            if result.return_code != 0:
                raise DriverError("mainland_proxy could not restore the task namespace network policy")
            await driver.set_network_policy(Public())
            restricted.pop(id(driver), None)
    return apply
