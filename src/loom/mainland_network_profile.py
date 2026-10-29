"""Frozen mainland-network simulation rules shared by hosted execution plans.

The operator supplies an immutable snapshot directory. Plans contain rule data
and public source versions, never the operator's local directory or a live URL
whose contents could change during an attempt.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator

_HOST = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$")
_MAX_FILE_BYTES = 2 * 1024 * 1024


class MainlandNetworkProfileV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["loom.mainland-network-profile.v1"] = "loom.mainland-network-profile.v1"
    provider_version: str = Field(min_length=1, max_length=128)
    gfw_suffixes: tuple[str, ...] = Field(max_length=200_000)
    gfw_hosts: tuple[str, ...] = Field(default=(), max_length=200_000)
    greatfire_hosts: tuple[str, ...] = Field(default=(), max_length=200_000)
    unknown_action: Literal["direct"] = "direct"
    intermittent_action: Literal["reject"] = "reject"
    sources: dict[str, Any] = Field(default_factory=dict)

    @field_validator("gfw_suffixes", "gfw_hosts", "greatfire_hosts")
    @classmethod
    def canonical_domains(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if list(value) != sorted(set(value)) or any(len(host) > 253 or not _HOST.fullmatch(host) for host in value):
            raise ValueError("mainland proxy rules must be canonical, unique and sorted")
        return value


def _read(path: Path) -> str:
    if not path.is_file() or path.stat().st_size > _MAX_FILE_BYTES:
        raise ValueError("mainland_proxy_profile_unavailable")
    return path.read_text(encoding="utf-8")


def _public_url(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def load_mainland_network_profile(directory: Path | None = None) -> MainlandNetworkProfileV1:
    configured = directory or os.environ.get("LOOM_MAINLAND_PROXY_PROFILE_DIR")
    if not configured:
        raise ValueError("mainland_proxy_profile_unavailable")
    root = Path(configured)
    try:
        gfw = yaml.safe_load(_read(root / "gfw-rules.yaml"))
        greatfire = yaml.safe_load(_read(root / "greatfire-rules.yaml"))
        source = json.loads(_read(root / "sources.json"))
        decisions = json.loads(_read(root / "greatfire-decisions.json"))
        if not isinstance(decisions, list) or any(not isinstance(row, dict) for row in decisions):
            raise ValueError("invalid GreatFire decisions")
        suffixes: set[str] = set()
        hosts: set[str] = set()
        measured: set[str] = set()
        if not isinstance(gfw, dict) or not isinstance(gfw.get("payload"), list) or not gfw["payload"]:
            raise ValueError("invalid GFW provider")
        for rule in gfw["payload"]:
            if not isinstance(rule, str):
                raise ValueError("invalid GFW domain")
            (suffixes if rule.startswith("+.") else hosts).add(rule.removeprefix("+."))
        if not isinstance(greatfire, dict) or not isinstance(greatfire.get("payload"), list):
            raise ValueError("invalid GreatFire provider")
        for rule in greatfire["payload"]:
            if not isinstance(rule, str) or not rule.startswith("DOMAIN,") or rule.count(",") != 1:
                raise ValueError("only exact GreatFire host rules are supported")
            measured.add(rule.split(",", 1)[1])
        version = source["gfw"]["provider_tag"]
        public_decisions = []
        for row in decisions:
            url = _public_url(row.get("url"))
            if url:
                public_decisions.append({"url": url, **{key: row.get(key) for key in ("headline", "last_tested", "stale", "action")}})
        return MainlandNetworkProfileV1(
            provider_version=version, gfw_suffixes=tuple(sorted(suffixes)),
            gfw_hosts=tuple(sorted(hosts)), greatfire_hosts=tuple(sorted(measured)),
            sources={"gfw_provider_release": _public_url(source["gfw"].get("provider_release")),
                     "fetched_at": source.get("fetched_at"),
                     "greatfire_attribution": "GreatFire, CC BY 4.0; https://en.greatfire.org/data/methodology",
                     "greatfire_decisions": public_decisions},
        )
    except (OSError, ValueError, KeyError, TypeError, yaml.YAMLError) as exc:
        raise ValueError("mainland_proxy_profile_unavailable") from exc
