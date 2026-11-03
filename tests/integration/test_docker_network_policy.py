"""Network policy enforcement inside the container. Requires NET_ADMIN
(DockerDriver opts into it via cap_add) and iptables + curl in the image
(installed via apk in the fixture)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.models.networking import Allowlist, NoNetwork, Public

pytestmark = pytest.mark.docker


@pytest.fixture
async def docker_driver_with_iptables() -> AsyncGenerator[object, None]:
    pytest.importorskip("docker")
    import docker
    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not available")
    from loom.driver.docker import DockerDriver
    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    await d.start(options=StartOptions())
    # Install iptables + curl. apk add can be slow on a cold cache.
    r = await d.exec("apk add --no-cache iptables curl", user="root", timeout_sec=120)
    assert r.return_code == 0, f"apk add failed: {r.stderr!r}"
    try:
        yield d
    finally:
        await d.stop(delete=True)


async def test_no_network_blocks_external(docker_driver_with_iptables):  # type: ignore[no-untyped-def]
    d = docker_driver_with_iptables
    await d.set_network_policy(NoNetwork())
    r = await d.exec(
        "curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://example.com",
        timeout_sec=8,
    )
    # Either curl fails to resolve, gets blocked, or times out → non-success.
    assert r.return_code != 0 or r.stdout in (b"000", b"")


async def test_public_allows_external(docker_driver_with_iptables):  # type: ignore[no-untyped-def]
    d = docker_driver_with_iptables
    await d.set_network_policy(Public())
    r = await d.exec(
        "curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://example.com",
        timeout_sec=15,
    )
    assert r.stdout.startswith(b"2") or r.stdout.startswith(b"3"), (
        f"expected 2xx/3xx, got rc={r.return_code} stdout={r.stdout!r} stderr={r.stderr!r}"
    )


async def test_allowlist_permits_specific_domain(docker_driver_with_iptables):  # type: ignore[no-untyped-def]
    d = docker_driver_with_iptables
    await d.set_network_policy(Allowlist(domains=("example.com",)))
    r = await d.exec(
        "curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://example.com",
        timeout_sec=15,
    )
    assert r.stdout.startswith(b"2") or r.stdout.startswith(b"3"), (
        f"expected 2xx/3xx, got rc={r.return_code} stdout={r.stdout!r} stderr={r.stderr!r}"
    )


async def test_public_restores_output_after_restricted_phase(docker_driver_with_iptables):
    driver = docker_driver_with_iptables
    await driver.set_network_policy(NoNetwork())
    await driver.set_network_policy(Public())
    rules = (await driver.exec("iptables -S OUTPUT", user="root")).stdout.decode()
    assert "-P OUTPUT ACCEPT" in rules and "-j DROP" not in rules
