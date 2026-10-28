"""Opt-in Docker tool-network simulation using a fixed mihomo rule snapshot.

This is a controlled approximation, not a mainland reachability certificate.
Model traffic stays outside this gateway; tool containers join only
``network_name``. No host routing or shared Docker networks are modified.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import docker
import yaml  # type: ignore[import-untyped]

from loom.errors import DriverError
from loom.models.networking import (
    Allowlist,
    GatewayOnly,
    NetworkPolicy,
    NoNetwork,
    Public,
    PublicWeb,
    WebAllowlist,
)

logger = logging.getLogger(__name__)

_PROFILE_FILES = ("gfw-rules.yaml", "greatfire-rules.yaml", "sources.json", "greatfire-decisions.json")
_PRIVATE_RULES = [
    "IP-CIDR,127.0.0.0/8,REJECT", "IP-CIDR,10.0.0.0/8,REJECT",
    "IP-CIDR,172.16.0.0/12,REJECT", "IP-CIDR,192.168.0.0/16,REJECT",
    "IP-CIDR,169.254.0.0/16,REJECT", "IP-CIDR6,::1/128,REJECT",
    "IP-CIDR6,fc00::/7,REJECT", "IP-CIDR6,fe80::/10,REJECT",
]


def _task_policy_rules(policy: NetworkPolicy) -> list[str]:
    if isinstance(policy, (Public, PublicWeb)):
        return ["MATCH,DIRECT"]
    if isinstance(policy, (NoNetwork, GatewayOnly)):
        return ["MATCH,REJECT"]
    rules = []
    if isinstance(policy, Allowlist):
        for value in policy.domains:
            host = value.lower().rstrip(".")
            try:
                address = ipaddress.ip_address(host)
            except ValueError:
                if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host):
                    raise DriverError("mainland_proxy allowlist has an invalid domain") from None
                rules.append(f"DOMAIN,{host},DIRECT")
            else:
                kind = "IP-CIDR6" if address.version == 6 else "IP-CIDR"
                rules.append(f"{kind},{address}/{address.max_prefixlen},DIRECT")
        for value in policy.cidrs:
            try:
                network = ipaddress.ip_network(value, strict=False)
            except ValueError:
                raise DriverError("mainland_proxy allowlist has an invalid CIDR") from None
            kind = "IP-CIDR6" if network.version == 6 else "IP-CIDR"
            rules.append(f"{kind},{network},DIRECT")
    elif isinstance(policy, WebAllowlist):
        rules.extend(f"AND,((DOMAIN,{item.host}),(DST-PORT,{item.port})),DIRECT" for item in policy.destinations)
    else:
        raise DriverError("mainland_proxy received an unsupported task network policy")
    return [*rules, "MATCH,REJECT"]


def _controller_rule_matches(actual: dict[str, Any], expected: str) -> bool:
    kind, _, remainder = expected.partition(",")
    payload, _, proxy = remainder.rpartition(",")
    if kind == "MATCH":
        payload, proxy = "", remainder
    # The API uses Go rule names (IPCIDR, DstPort, Domain), while YAML
    # accepts upper-case names with hyphens. Keep domain spelling otherwise.
    def normalize_kind(value: Any) -> str:
        return str(value).lower().replace("-", "")
    def normalize_payload(value: Any) -> str:
        return "".join(str(value).lower().split()).replace("(dst-port,", "(dstport,").replace("),(", ")&&(")
    return (normalize_kind(actual.get("type")) == normalize_kind("IPCIDR" if kind == "IP-CIDR6" else kind)
            and normalize_payload(actual.get("payload", "")) == normalize_payload(payload)
            and actual.get("proxy") == proxy)


@dataclass(frozen=True)
class MainlandProxySettings:
    image: str
    profile_dir: Path
    binary: str = "/mihomo"
    startup_timeout_sec: float = 20.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> MainlandProxySettings:
        values = os.environ if env is None else env
        image = values.get("LOOM_MAINLAND_PROXY_IMAGE", "").strip()
        directory = values.get("LOOM_MAINLAND_PROXY_PROFILE_DIR", "").strip()
        if not image or not directory:
            raise DriverError(
                "mainland_proxy requires LOOM_MAINLAND_PROXY_IMAGE and "
                "LOOM_MAINLAND_PROXY_PROFILE_DIR; no unrestricted fallback is allowed",
            )
        return cls(
            image=image, profile_dir=Path(directory).expanduser().resolve(),
            binary=values.get("LOOM_MAINLAND_PROXY_BINARY", "/mihomo"),
        )


class MainlandProxyGateway:
    """Attempt-owned internal network plus a dual-network mihomo container."""

    def __init__(
        self, trial_id: UUID, settings: MainlandProxySettings, output_dir: Path | None = None,
        *, client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.settings = settings
        self.trial_id = trial_id
        self.output_dir = output_dir
        self.network_name = ""
        self.environment: dict[str, str] = {}
        self.metadata: dict[str, Any] = {"enabled": True, "applied": False}
        self._prefix = f"loom-mainland-{trial_id.hex[:12]}-{uuid4().hex[:8]}"
        self._client_factory = client_factory or (lambda: docker.from_env(timeout=30))
        self._client: Any = None
        self._container: Any = None
        self._networks: list[Any] = []
        self._state_dir: Path | None = None
        self._started = False
        self._policy_lock = asyncio.Lock()
        self._config: dict[str, Any] = {}
        self._provider_counts: dict[str, int] = {}

    async def start(self) -> MainlandProxyGateway:
        if self._started:
            raise DriverError("mainland_proxy gateway has already started")
        self._started = True
        operation = asyncio.create_task(asyncio.to_thread(self._start_sync))
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            # A cancelled to_thread operation still runs. Wait for it before
            # cleanup, otherwise it could create a network after stop returns.
            try:
                await asyncio.shield(operation)
            except Exception:
                pass
            await self.stop()
            raise
        except Exception as exc:
            await self.stop()
            raise DriverError(f"mainland_proxy setup failed: {exc}") from exc
        return self

    def _prepare_profile(self) -> dict[str, int]:
        self._state_dir = Path(tempfile.mkdtemp(prefix="loom-mainland-profile-"))
        for name in _PROFILE_FILES:
            source = self.settings.profile_dir / name
            if not source.is_file() or source.stat().st_size > 10 * 1024 * 1024:
                raise DriverError(f"mainland_proxy missing or oversized profile file: {name}")
            shutil.copyfile(source, self._state_dir / name)
        counts: dict[str, int] = {}
        providers = {}
        for name, behavior in (("gfw", "domain"), ("greatfire", "classical")):
            filename = f"{name}-rules.yaml"
            payload = yaml.safe_load((self._state_dir / filename).read_text())
            rules = payload.get("payload") if isinstance(payload, dict) else None
            if not isinstance(rules, list) or not all(isinstance(row, str) for row in rules):
                raise DriverError(f"mainland_proxy invalid rule provider: {filename}")
            if name == "gfw" and not rules:
                raise DriverError("mainland_proxy GFW rule provider is empty")
            counts[name] = len(rules)
            providers[name] = {"type": "file", "behavior": behavior, "format": "yaml", "path": filename}
        sources = json.loads((self._state_dir / "sources.json").read_text())
        decisions = json.loads((self._state_dir / "greatfire-decisions.json").read_text())
        if not isinstance(sources, dict) or not isinstance(decisions, list):
            raise DriverError("mainland_proxy invalid source metadata")
        config = {
            "mixed-port": 7890, "allow-lan": True, "bind-address": "0.0.0.0",
            "mode": "rule", "log-level": "info", "ipv6": False,
            "external-controller": "127.0.0.1:9090",
            "profile": {"store-selected": False}, "rule-providers": providers,
            "rules": [*_PRIVATE_RULES, "RULE-SET,greatfire,REJECT", "RULE-SET,gfw,REJECT", "MATCH,DIRECT"],
        }
        self._config = config
        self._provider_counts = counts
        (self._state_dir / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
        self.metadata.update({
            "image": self.settings.image, "sources": sources, "greatfire_decisions": decisions,
            "expected_rule_counts": counts, "unknown_action": "DIRECT",
            "effective_task_policy": Public().model_dump(mode="json"),
            "scope": "HTTP/HTTPS simulation; fixed upstream snapshot, not mainland reachability proof",
        })
        return counts

    def _start_sync(self) -> None:
        counts = self._prepare_profile()
        self._client = self._client_factory()
        self._client.images.get(self.settings.image)  # preprovisioned only; never pull during a trial
        labels = {"loom.mainland-proxy": "true", "loom.mainland-proxy-owner": self._prefix, "loom.trial_id": str(self.trial_id)}
        internal = self._client.networks.create(self._prefix + "-internal", internal=True, enable_ipv6=False, labels=labels)
        self._networks.append(internal)
        uplink = self._client.networks.create(self._prefix + "-uplink", enable_ipv6=False, labels=labels)
        self._networks.append(uplink)
        self._container = self._client.containers.create(
            self.settings.image, entrypoint=[self.settings.binary],
            command=["-d", "/state", "-f", "/state/config.yaml"],
            name=self._prefix + "-gateway", network=uplink.name, labels=labels,
            detach=True, read_only=True, cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
            # Tool DockerDrivers can have NET_ADMIN for their own task policy.
            # Even if they add a route through this dual-homed gateway, only
            # mihomo may originate uplink connections: do not forward packets.
            sysctls={"net.ipv4.ip_forward": "0", "net.ipv6.conf.all.forwarding": "0"},
            mem_limit="256m", nano_cpus=1_000_000_000,
            pids_limit=128, tmpfs={"/tmp": "rw,nosuid,nodev,size=64m"},
            volumes={str(self._state_dir): {"bind": "/state", "mode": "rw"}},
        )
        internal.connect(self._container)
        self._container.start()
        deadline = time.monotonic() + self.settings.startup_timeout_sec
        while time.monotonic() < deadline:
            self._container.reload()
            if self._container.status not in {"created", "running"}:
                raise DriverError("mainland_proxy gateway exited during startup")
            probe = self._container.exec_run([
                "wget", "-qO-", "-T", "2", "http://127.0.0.1:9090/providers/rules",
            ])
            if probe.exit_code == 0:
                try:
                    providers = json.loads(probe.output)["providers"]
                    if all(providers.get(k, {}).get("ruleCount") == v for k, v in counts.items()):
                        self._container.reload()
                        address = self._container.attrs["NetworkSettings"]["Networks"][internal.name]["IPAddress"]
                        if not address:
                            raise DriverError("mainland_proxy gateway has no internal address")
                        proxy = f"http://{address}:7890"
                        self.network_name = internal.name
                        self.environment = {key: proxy for key in (
                            "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                            "npm_config_proxy", "npm_config_https_proxy",
                        )}
                        self.environment.update({"NO_PROXY": "localhost,127.0.0.1", "no_proxy": "localhost,127.0.0.1"})
                        self.metadata.update({"applied": True, "loaded_rule_counts": counts, "image_id": self._container.image.id})
                        self._save_metadata()
                        return
                except (ValueError, KeyError, TypeError):
                    pass
            time.sleep(0.2)
        raise DriverError("mainland_proxy gateway did not load the expected rule snapshot")

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        """Apply a phase policy on top of the immutable simulation rules."""
        async with self._policy_lock:
            if not self.metadata.get("applied") or self._container is None:
                raise DriverError("mainland_proxy gateway is not running")
            operation = asyncio.create_task(asyncio.to_thread(self._set_network_policy_sync, policy))
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                # Do not let a previous phase overwrite a later restore after
                # its thread continues running beyond cancellation.
                await asyncio.shield(operation)
                raise

    def _set_network_policy_sync(self, policy: NetworkPolicy) -> None:
        assert self._state_dir is not None
        rules = [*_PRIVATE_RULES, "RULE-SET,greatfire,REJECT", "RULE-SET,gfw,REJECT", *_task_policy_rules(policy)]
        updated = {**self._config, "rules": rules}
        pending = self._state_dir / "config.next.yaml"
        serialized = yaml.safe_dump(updated, sort_keys=False)
        pending.write_text(serialized)
        pending.replace(self._state_dir / "config.yaml")
        deadline = time.monotonic() + self.settings.startup_timeout_sec
        # Docker Desktop can briefly report ENOENT across an atomic host
        # rename. Confirm the mounted configuration is visible before HUP;
        # otherwise mihomo keeps the previous policy after a failed parse.
        while time.monotonic() < deadline:
            visible = self._container.exec_run(["cat", "/state/config.yaml"])
            if visible.exit_code == 0 and visible.output.decode() == serialized:
                break
            time.sleep(0.1)
        else:
            raise DriverError("mainland_proxy task policy file did not become visible")
        self._container.kill(signal="SIGHUP")
        while time.monotonic() < deadline:
            self._container.reload()
            if self._container.status != "running":
                raise DriverError("mainland_proxy gateway exited while applying task policy")
            probe = self._container.exec_run(["wget", "-qO-", "-T", "2", "http://127.0.0.1:9090/rules"])
            if probe.exit_code == 0:
                try:
                    actual = json.loads(probe.output)["rules"]
                    if len(actual) == len(rules) and all(_controller_rule_matches(row, rule) for row, rule in zip(actual, rules, strict=True)):
                        loaded_probe = self._container.exec_run(["wget", "-qO-", "-T", "2", "http://127.0.0.1:9090/providers/rules"])
                        loaded = json.loads(loaded_probe.output).get("providers", {}) if loaded_probe.exit_code == 0 else {}
                        if not all(loaded.get(name, {}).get("ruleCount") == count for name, count in self._provider_counts.items()):
                            time.sleep(0.2)
                            continue
                        self._config = updated
                        self.metadata["effective_task_policy"] = policy.model_dump(mode="json")
                        self._save_metadata()
                        return
                except (ValueError, KeyError, TypeError):
                    pass
            time.sleep(0.2)
        raise DriverError("mainland_proxy did not apply the requested task network policy")

    def _save_metadata(self) -> None:
        if self.output_dir is not None:
            try:
                self.output_dir.mkdir(parents=True, exist_ok=True)
                (self.output_dir / "profile.json").write_text(json.dumps(self.metadata, indent=2) + "\n")
            except OSError:
                # The authoritative profile also travels with the result/events.
                # Optional local diagnostics must not discard a valid reward.
                logger.warning("mainland_proxy_profile_diagnostic_write_failed", exc_info=True)

    async def stop(self) -> None:
        async with self._policy_lock:
            operation = asyncio.create_task(asyncio.to_thread(self._stop_sync))
            await asyncio.shield(operation)

    def _stop_sync(self) -> None:
        errors = []
        if self._container is not None:
            try:
                if self.output_dir is not None:
                    self.output_dir.mkdir(parents=True, exist_ok=True)
                    logs = self._container.logs(timestamps=True, tail=2000)
                    (self.output_dir / "mihomo.log").write_bytes(logs)
            except Exception:
                pass  # diagnostics must not prevent network cleanup
            try:
                self._container.remove(force=True, v=True)
                self._container = None
            except Exception as exc:
                errors.append(str(exc))
        for network in list(reversed(self._networks)):
            try:
                network.remove()
                self._networks.remove(network)
            except Exception as exc:
                errors.append(str(exc))
        self._save_metadata()
        if not errors:
            if self._state_dir is not None:
                shutil.rmtree(self._state_dir)
                self._state_dir = None
            if self._client is not None:
                self._client.close()
                self._client = None
        if errors:
            raise DriverError("mainland_proxy cleanup failed: " + "; ".join(errors))
