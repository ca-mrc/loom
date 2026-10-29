#!/usr/bin/env python3
"""Prepare a new immutable GFWList/GreatFire snapshot; never run network probes.

Example (from an installed Loom checkout):
  python scripts/refresh_mainland_network_profile.py --output-dir profiles/20261028
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from loom.mainland_network_profile import MainlandNetworkProfileV1, load_mainland_network_profile
from loom.models.networking import WebDestination

LATEST_RELEASE_URL = "https://api.github.com/repos/Loyalsoldier/clash-rules/releases/latest"
RELEASE_ROOT = "https://github.com/Loyalsoldier/clash-rules/releases"
GREATFIRE_API_ROOT = "https://en.greatfire.org/api/url/"
DEFAULT_URLS = (
    "https://github.com",
    "https://pypi.org",
    "https://files.pythonhosted.org",
    "https://pypi.tuna.tsinghua.edu.cn",
    "https://registry.npmjs.org",
    "https://registry.npmmirror.com",
    "https://gitee.com",
    "https://gitlab.com",
)
MAX_FETCH_BYTES = 2 * 1024 * 1024
MAX_OBSERVATION_AGE_DAYS = 90
Fetch = Callable[[str, float], bytes]


def public_root_url(value: str) -> str:
    """Accept an explicit public root, never a trajectory/private URL or token."""
    try:
        parsed = urlsplit(value)
        if (
            any(character.isspace() for character in value)
            or parsed.scheme not in {"http", "https"}
            or parsed.username is not None
            or parsed.password is not None
            or "?" in value
            or "#" in value
            or parsed.path not in {"", "/"}
            or not parsed.hostname
        ):
            raise ValueError
        protocol: Literal["http", "https"] = "https" if parsed.scheme == "https" else "http"
        destination = WebDestination(host=parsed.hostname, protocol=protocol)
        if parsed.port not in {None, destination.port}:
            raise ValueError
        return f"{destination.protocol}://{destination.host}"
    except ValueError as exc:
        raise ValueError(
            "--url requires a public HTTP(S) root URL without credentials, path, query or fragment"
        ) from exc


def fetch_public_bytes(url: str, timeout: float) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "loom-mainland-network-profile/1.0",
            "Accept": "application/json, text/plain",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content = response.read(MAX_FETCH_BYTES + 1)
    if len(content) > MAX_FETCH_BYTES:
        raise ValueError("public source response exceeds the profile file size limit")
    return content


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _decision(url: str, response: dict[str, Any], fetched_at: datetime) -> dict[str, Any]:
    verdict = response.get("headline")
    last_tested = response.get("last_tested")
    age = None
    try:
        tested = datetime.fromisoformat(last_tested.replace("Z", "+00:00"))
        if tested.tzinfo is not None:
            age = (fetched_at - tested).total_seconds() / 86400
    except (AttributeError, TypeError, ValueError):
        pass
    try:
        same_url = public_root_url(response.get("url", "")) == url
    except (AttributeError, TypeError, ValueError):
        same_url = False
    fresh = (
        response.get("found") is True
        and same_url
        and response.get("stale") is False
        and age is not None
        and 0 <= age <= MAX_OBSERVATION_AGE_DAYS
    )
    reject = fresh and verdict in ("blocked", "redirected", "intermittent")
    return {
        "url": url,
        "host": urlsplit(url).hostname,
        "headline": verdict,
        "last_tested": last_tested,
        "stale": response.get("stale"),
        "age_days": round(age, 3) if age is not None else None,
        "action": "reject_exact_host" if reject else "no_override",
        "reason": (
            "fresh blocked/intermittent root observation; conservative exact-host approximation"
            if reject
            else "unknown/stale/error/not-blocked does not certify reachability"
        ),
    }


def refresh_profile(
    output_dir: Path,
    urls: Sequence[str] | None = None,
    *,
    timeout: float = 25.0,
    fetch: Fetch = fetch_public_bytes,
    now: datetime | None = None,
) -> MainlandNetworkProfileV1:
    """Read public sources, validate four files, then publish a new directory."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    roots = tuple(
        dict.fromkeys(public_root_url(url) for url in (DEFAULT_URLS if urls is None else urls))
    )
    if not roots:
        raise ValueError("at least one explicit public root URL is required")
    output_dir = Path(output_dir).expanduser().absolute()
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError("output directory already exists; choose a new profile version")
    fetched_at = now or datetime.now(UTC)
    if fetched_at.tzinfo is None:
        raise ValueError("snapshot timestamp must include a timezone")
    release = json.loads(fetch(LATEST_RELEASE_URL, timeout))
    if not isinstance(release, dict):
        raise ValueError("invalid GFW provider release response")
    tag = release.get("tag_name")
    if (
        not isinstance(tag, str)
        or not tag
        or len(tag) > 128
        or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for character in tag
        )
        or tag in {".", ".."}
    ):
        raise ValueError("invalid GFW provider release tag")
    asset_url = f"{RELEASE_ROOT}/download/{tag}/gfw.txt"
    if not any(
        asset.get("name") == "gfw.txt" and asset.get("browser_download_url") == asset_url
        for asset in release.get("assets", [])
        if isinstance(asset, dict)
    ):
        raise ValueError("fixed gfw.txt release asset is unavailable")
    gfw = fetch(asset_url, timeout)
    decisions = []
    for url in roots:
        api_url = GREATFIRE_API_ROOT + url.replace("://", "/", 1)
        row: dict[str, Any] = {"url": url, "api_url": api_url, "fetched_at": fetched_at.isoformat()}
        try:
            response = json.loads(fetch(api_url, timeout))
            if not isinstance(response, dict):
                raise ValueError("GreatFire response is not an object")
            row.update(_decision(url, response, fetched_at))
            row.update(http_status=200, observation=response)
        except (OSError, ValueError, TimeoutError) as exc:
            row.update(
                host=urlsplit(url).hostname,
                headline=None,
                last_tested=None,
                stale=None,
                action="no_override",
                reason="measurement unavailable; not certified accessible",
                http_status=getattr(exc, "code", None),
                error=type(exc).__name__,
            )
        decisions.append(row)
    blocked = sorted(
        {str(row["host"]) for row in decisions if row["action"] == "reject_exact_host"}
    )
    greatfire = (
        "payload:\n" + "".join(f"  - DOMAIN,{host}\n" for host in blocked)
        if blocked
        else "payload: []\n"
    )
    sources = {
        "fetched_at": fetched_at.isoformat(),
        "gfw": {
            "provider_tag": tag,
            "provider_release": f"{RELEASE_ROOT}/tag/{tag}",
            "provider_url": asset_url,
            "source": "Loyalsoldier/clash-rules GFWList-derived domain provider",
            "semantics": "community routing list used as a conservative simulation, not current nationwide proof",
        },
        "greatfire": {
            "attribution": "GreatFire, CC BY 4.0; https://en.greatfire.org/data/methodology",
            "api_documentation": "https://en.greatfire.org/data",
            "intermittent_action": "reject",
            "unknown_action": "direct",
            "max_observation_age_days": MAX_OBSERVATION_AGE_DAYS,
            "scope": "explicit public root URL observations, conservatively applied to exact hosts; no new tests triggered",
        },
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{output_dir.name}.tmp-", dir=output_dir.parent
    ) as temporary:
        stage = Path(temporary)
        (stage / "gfw-rules.yaml").write_bytes(gfw)
        (stage / "greatfire-rules.yaml").write_text(greatfire, encoding="utf-8")
        _write_json(stage / "sources.json", sources)
        _write_json(stage / "greatfire-decisions.json", decisions)
        profile = load_mainland_network_profile(stage)
        # Reserve a new name with mkdir(exist_ok=False). The following rename
        # replaces only our empty reservation, never an existing profile.
        output_dir.mkdir()
        try:
            stage.rename(output_dir)
        except BaseException:
            output_dir.rmdir()
            raise
    return profile


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="New version directory; existing directories are never overwritten",
    )
    parser.add_argument(
        "--url",
        action="append",
        default=None,
        help="Explicit public HTTP(S) root to query; repeat to replace the default roots",
    )
    parser.add_argument(
        "--timeout", type=float, default=25.0, help="Per-request timeout in seconds (default 25)"
    )
    args = parser.parse_args(argv)
    try:
        profile = refresh_profile(args.output_dir, args.url, timeout=args.timeout)
    except (OSError, ValueError) as exc:
        parser.exit(2, f"error: {exc}\n")
    print(
        f"Prepared {args.output_dir}: provider {profile.provider_version}, "
        f"{len(profile.gfw_suffixes) + len(profile.gfw_hosts)} GFW rules, "
        f"{len(profile.greatfire_hosts)} GreatFire host rules. "
        "Unknown observations are not certified accessible."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
