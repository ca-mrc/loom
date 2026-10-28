"""Gateway readiness and partial-start cleanup boundaries."""
from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml

from loom.driver.mainland_proxy import (
    MainlandProxyGateway,
    MainlandProxySettings,
    _task_policy_rules,
)
from loom.errors import DriverError
from loom.models.networking import (
    Allowlist,
    GatewayOnly,
    NoNetwork,
    Public,
    PublicWeb,
    WebAllowlist,
    WebDestination,
)


@pytest.fixture
def profile(tmp_path: Path) -> Path:
    directory = tmp_path / "rules"
    directory.mkdir()
    (directory / "gfw-rules.yaml").write_text("payload:\n  - '+.github.com'\n")
    (directory / "greatfire-rules.yaml").write_text("payload:\n  - DOMAIN,gitlab.com\n")
    (directory / "sources.json").write_text('{"gfw":{"provider_tag":"fixed-1"}}')
    (directory / "greatfire-decisions.json").write_text("[]")
    return directory


class FakeNetwork:
    def __init__(self, name: str) -> None:
        self.name = name
        self.removed = False
        self.connected: list[object] = []

    def connect(self, container: object) -> None:
        self.connected.append(container)
        container.attrs["NetworkSettings"]["Networks"][self.name] = {"IPAddress": "172.20.0.2"}  # type: ignore[attr-defined]

    def remove(self) -> None:
        self.removed = True


def controller_rules(tail: list[dict[str, object]]) -> list[dict[str, object]]:
    private = ["127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "::1/128", "fc00::/7", "fe80::/10"]
    return [*({"type":"IPCIDR", "payload":value, "proxy":"REJECT"} for value in private), {"type":"RuleSet", "payload":"greatfire", "proxy":"REJECT"}, {"type":"RuleSet", "payload":"gfw", "proxy":"REJECT"}, *tail]


class FakeContainer:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        self.attrs = {"NetworkSettings": {"Networks": {}}}
        self.status = "created"
        self.image = SimpleNamespace(id="sha256:gateway")
        self.removed = False
        self.state_dir: Path | None = None
        self.rules = controller_rules([{"type":"Match", "payload":"", "proxy":"DIRECT"}])
        self.signals: list[str] = []

    def start(self) -> None:
        self.status = "running"

    def reload(self) -> None:
        pass

    def exec_run(self, args: list[str]) -> SimpleNamespace:
        if args[0] == "cat":
            assert self.state_dir is not None
            return SimpleNamespace(exit_code=0, output=(self.state_dir / "config.yaml").read_bytes())
        if args[-1].endswith("/providers/rules"):
            return SimpleNamespace(exit_code=0, output=json.dumps({"providers": {key: {"ruleCount": value} for key, value in self.counts.items()}}).encode())
        assert args[-1].endswith("/rules")
        return SimpleNamespace(exit_code=0, output=json.dumps({"rules": self.rules}).encode())

    def kill(self, *, signal: str) -> None:
        self.signals.append(signal)

    def logs(self, **kwargs: object) -> bytes:
        return b"RuleSet(gfw) using REJECT\n"

    def remove(self, *, force: bool, v: bool) -> None:
        assert force and v
        self.removed = True


class FakeClient:
    def __init__(self, counts: dict[str, int] | None = None) -> None:
        self.network_calls: list[dict[str, object]] = []
        self.network_objects: list[FakeNetwork] = []
        self.container = FakeContainer(counts or {"gfw": 1, "greatfire": 1})
        self.container_kwargs: dict[str, object] = {}
        self.images = SimpleNamespace(get=lambda name: SimpleNamespace(id="sha256:gateway"))
        self.networks = SimpleNamespace(create=self.create_network)
        self.containers = SimpleNamespace(create=self.create_container)
        self.closed = False

    def create_network(self, name: str, **kwargs: object) -> FakeNetwork:
        self.network_calls.append({"name": name, **kwargs})
        result = FakeNetwork(name)
        self.network_objects.append(result)
        return result

    def create_container(self, image: str, **kwargs: object) -> FakeContainer:
        self.container_kwargs = kwargs
        self.container.state_dir = Path(next(iter(kwargs["volumes"])))  # type: ignore[arg-type]
        return self.container

    def close(self) -> None:
        self.closed = True


def test_settings_requires_both_operator_inputs(profile: Path) -> None:
    with pytest.raises(DriverError, match="no unrestricted fallback"):
        MainlandProxySettings.from_env({"LOOM_MAINLAND_PROXY_IMAGE": "gateway:test"})
    result = MainlandProxySettings.from_env({"LOOM_MAINLAND_PROXY_IMAGE": "gateway:test", "LOOM_MAINLAND_PROXY_PROFILE_DIR": str(profile)})
    assert result.profile_dir == profile
    assert result.binary == "/mihomo"


async def test_starts_only_after_fixed_providers_loaded_and_cleans(profile: Path, tmp_path: Path) -> None:
    client = FakeClient()
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile), tmp_path / "output", client_factory=lambda: client)
    assert await gateway.start() is gateway
    assert client.network_calls[0]["internal"] is True
    assert "internal" not in client.network_calls[1]
    assert gateway.network_name == client.network_objects[0].name
    assert all(value == "http://172.20.0.2:7890" for key, value in gateway.environment.items() if "proxy" in key.lower() and key.lower() != "no_proxy")
    assert gateway.metadata["loaded_rule_counts"] == {"gfw": 1, "greatfire": 1}
    kwargs = client.container_kwargs
    assert kwargs["cap_drop"] == ["ALL"]
    assert kwargs["read_only"] is True
    assert kwargs["sysctls"] == {"net.ipv4.ip_forward": "0", "net.ipv6.conf.all.forwarding": "0"}
    assert "ports" not in kwargs
    state_dir = Path(next(iter(kwargs["volumes"])))  # type: ignore[arg-type]
    config = yaml.safe_load((state_dir / "config.yaml").read_text())
    assert config["external-controller"] == "127.0.0.1:9090"
    assert "IP-CIDR,127.0.0.0/8,REJECT" in config["rules"]
    assert config["rules"][-1] == "MATCH,DIRECT"
    await gateway.stop()
    await gateway.stop()
    assert client.container.removed
    assert all(network.removed for network in client.network_objects)
    assert client.closed and not state_dir.exists()
    assert (tmp_path / "output/profile.json").is_file()
    assert (tmp_path / "output/mihomo.log").read_text().startswith("RuleSet")


async def test_provider_count_mismatch_fails_without_exposing_network(profile: Path) -> None:
    client = FakeClient({"gfw": 0, "greatfire": 0})
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile, startup_timeout_sec=0.001), client_factory=lambda: client)
    with pytest.raises(DriverError, match="expected rule snapshot"):
        await gateway.start()
    assert not gateway.network_name and not gateway.environment
    assert client.container.removed
    assert all(network.removed for network in client.network_objects)


async def test_bad_profile_fails_before_docker_factory(profile: Path) -> None:
    (profile / "gfw-rules.yaml").write_text("payload: []\n")
    def unexpected() -> None:
        raise AssertionError("Docker must not be touched")
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile), client_factory=unexpected)
    with pytest.raises(DriverError, match="GFW rule provider is empty"):
        await gateway.start()


async def test_partial_network_creation_failure_removes_created_network(profile: Path) -> None:
    client = FakeClient()
    original = client.create_network
    def create(name: str, **kwargs: object) -> FakeNetwork:
        if client.network_objects:
            raise RuntimeError("uplink failed")
        return original(name, **kwargs)
    client.networks.create = create
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile), client_factory=lambda: client)
    with pytest.raises(DriverError, match="uplink failed"):
        await gateway.start()
    assert len(client.network_objects) == 1
    assert client.network_objects[0].removed


async def test_cancelled_start_waits_for_thread_before_cleanup(profile: Path) -> None:
    client = FakeClient()
    entered, release = threading.Event(), threading.Event()
    original = client.create_network
    def create(name: str, **kwargs: object) -> FakeNetwork:
        if not client.network_objects:
            entered.set()
            assert release.wait(3)
        return original(name, **kwargs)
    client.networks.create = create
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile), client_factory=lambda: client)
    task = asyncio.create_task(gateway.start())
    assert await asyncio.to_thread(entered.wait, 3)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.container.removed
    assert all(network.removed for network in client.network_objects)


async def test_no_network_reload_must_match_live_rules(profile: Path) -> None:
    client = FakeClient()
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile), client_factory=lambda: client)
    await gateway.start()
    try:
        client.container.rules = controller_rules([{"type":"Match", "payload":"", "proxy":"REJECT"}])
        await gateway.set_network_policy(NoNetwork())
        assert client.container.signals == ["SIGHUP"]
        assert gateway.metadata["effective_task_policy"] == {"kind":"no-network"}
        assert yaml.safe_load((client.container.state_dir / "config.yaml").read_text())["rules"][-1] == "MATCH,REJECT"
    finally:
        await gateway.stop()


async def test_allowlist_reload_preserves_simulation_before_allow_rules(profile: Path) -> None:
    client = FakeClient()
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile), client_factory=lambda: client)
    await gateway.start()
    try:
        client.container.rules = controller_rules([{"type":"Domain", "payload":"pypi.org", "proxy":"DIRECT"}, {"type":"IPCIDR", "payload":"203.0.113.0/24", "proxy":"DIRECT"}, {"type":"Match", "payload":"", "proxy":"REJECT"}])
        await gateway.set_network_policy(Allowlist(domains=("pypi.org",), cidrs=("203.0.113.0/24",)))
        rules = yaml.safe_load((client.container.state_dir / "config.yaml").read_text())["rules"]
        assert rules.index("RULE-SET,gfw,REJECT") < rules.index("DOMAIN,pypi.org,DIRECT")
        assert rules[-1] == "MATCH,REJECT"
    finally:
        await gateway.stop()


async def test_unchanged_live_rules_do_not_claim_new_policy(profile: Path) -> None:
    client = FakeClient()
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("gateway:test", profile, startup_timeout_sec=0.02), client_factory=lambda: client)
    await gateway.start()
    try:
        with pytest.raises(DriverError, match="requested task network policy"):
            await gateway.set_network_policy(NoNetwork())
        assert gateway.metadata["effective_task_policy"] == {"kind":"public"}
    finally:
        await gateway.stop()


def test_policy_rule_semantics() -> None:
    assert _task_policy_rules(Public()) == _task_policy_rules(PublicWeb()) == ["MATCH,DIRECT"]
    assert _task_policy_rules(GatewayOnly()) == _task_policy_rules(NoNetwork()) == ["MATCH,REJECT"]
    assert _task_policy_rules(WebAllowlist(destinations=(WebDestination(host="pypi.org",protocol="https"),))) == ["AND,((DOMAIN,pypi.org),(DST-PORT,443)),DIRECT", "MATCH,REJECT"]
    with pytest.raises(DriverError, match="invalid domain"):
        _task_policy_rules(Allowlist(domains=("pypi.org,DIRECT",)))


def test_optional_profile_diagnostic_failure_does_not_fail_enforcement(tmp_path):
    from uuid import uuid4

    from loom.driver.mainland_proxy import MainlandProxyGateway, MainlandProxySettings
    unavailable = tmp_path / "not-a-directory"
    unavailable.write_text("occupied")
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings("unused", tmp_path), unavailable)
    gateway.metadata = {"enabled": True, "applied": True}
    gateway._save_metadata()
    assert gateway.metadata["applied"] is True
