#!/usr/bin/env python3
"""Discover Harbor updates and synchronize explicit runtime pins, using stdlib only.

`check` is read-only; `update` requires an explicit source SHA and package version.
Frozen worker dependency evidence is reported, never rewritten by this updater.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import tomllib
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = "config/harbor-runtime.json"
SHA_RE = re.compile(r"[0-9a-f]{40}")
VERSION_RE = re.compile(r"[0-9][0-9A-Za-z.+!-]*")


class UpstreamError(RuntimeError):
    """An upstream response or a local pin cannot be verified safely."""


@dataclass(frozen=True)
class HarborPin:
    repository: str
    upstream_ref: str
    source_revision: str
    version: str

    def validate(self) -> None:
        if self.repository != "harbor-framework/harbor":
            raise UpstreamError("repository must be harbor-framework/harbor")
        if not self.upstream_ref or any(c.isspace() for c in self.upstream_ref):
            raise UpstreamError("upstream_ref must be a nonempty Git ref without whitespace")
        if not SHA_RE.fullmatch(self.source_revision):
            raise UpstreamError("source_revision must be a full lowercase 40-character Git SHA")
        if not VERSION_RE.fullmatch(self.version):
            raise UpstreamError("version must be a nonempty package version without whitespace")


def load_pin(root: Path) -> HarborPin:
    try:
        data = json.loads((root / MANIFEST).read_text(encoding="utf-8"))
        if not isinstance(data, dict) or set(data) != set(HarborPin.__dataclass_fields__):
            raise UpstreamError(f"{MANIFEST} must contain exactly the four HarborPin fields")
        if not all(isinstance(value, str) for value in data.values()):
            raise UpstreamError(f"{MANIFEST} fields must be strings")
        pin = HarborPin(**data)
        pin.validate()
        return pin
    except (OSError, json.JSONDecodeError) as exc:
        raise UpstreamError(f"cannot read valid {MANIFEST}: {exc}") from exc


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        raise UpstreamError("GitHub API redirect was rejected")


class GitHubClient:
    def __init__(self, token: str = "") -> None:
        self.token = token

    def get(self, path: str) -> Any:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "loom-harbor-upstream",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(f"https://api.github.com{path}", headers=headers)
        try:
            with build_opener(NoRedirects()).open(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            raise UpstreamError(f"GitHub API returned HTTP {exc.code} for {path}") from exc
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            raise UpstreamError(f"GitHub API request failed for {path}") from exc


def discover(pin: HarborPin, client: GitHubClient) -> HarborPin:
    pin.validate()
    path = f"/repos/{pin.repository}"
    commit = client.get(f"{path}/commits/{quote(pin.upstream_ref, safe='')}")
    revision = commit.get("sha") if isinstance(commit, dict) else None
    if not isinstance(revision, str) or not SHA_RE.fullmatch(revision):
        raise UpstreamError("upstream commit response did not contain a full source SHA")
    # Read metadata at the resolved SHA, so a moving ref cannot mix two revisions.
    metadata = client.get(f"{path}/contents/pyproject.toml?{urlencode({'ref': revision})}")
    try:
        if metadata["encoding"] != "base64":
            raise ValueError("expected base64 content")
        source = base64.b64decode(metadata["content"]).decode("utf-8")
        version = tomllib.loads(source)["project"]["version"]
        if not isinstance(version, str):
            raise ValueError("expected string version")
    except (KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise UpstreamError(
            "upstream pyproject.toml did not contain a valid project.version"
        ) from exc
    latest = replace(pin, source_revision=revision, version=version)
    latest.validate()
    return latest


# Exact known consumers only. Each capture must occur once before any file changes.
PIN_LOCATIONS: tuple[tuple[str, str, str], ...] = (
    (
        "deploy/Dockerfile.worker",
        r"(?m)^ARG HARBOR_COMPAT_SHA=(?P<value>[^\n]+)$",
        "source_revision",
    ),
    (
        "deploy/Dockerfile.harbor-runtime",
        r"(?m)^ARG HARBOR_COMPAT_SHA=(?P<value>[^\n]+)$",
        "source_revision",
    ),
    ("deploy/Dockerfile.harbor-runtime", r"(?m)^ARG HARBOR_VERSION=(?P<value>[^\n]+)$", "version"),
    (
        "src/loom/agent/terminus2/provenance.py",
        r'"LOOM_HARBOR_SOURCE_REVISION",\s*"(?P<value>[^"]+)"',
        "source_revision",
    ),
    (
        "src/loom/agent/terminus2/provenance.py",
        r'(?m)^    HARBOR_RUNTIME_VERSION = "(?P<value>[^"]+)"$',
        "version",
    ),
)


def pin_plan(root: Path, current: HarborPin, target: HarborPin) -> dict[str, str]:
    """Validate all current consumers before preparing any replacement."""
    contents: dict[str, str] = {}
    for relative, pattern, field in PIN_LOCATIONS:
        if relative not in contents:
            try:
                contents[relative] = (root / relative).read_text(encoding="utf-8")
            except OSError as exc:
                raise UpstreamError(f"cannot read runtime pin: {relative}") from exc
        text = contents[relative]
        matches = list(re.finditer(pattern, text))
        if len(matches) != 1:
            raise UpstreamError(f"expected exactly one {field} pin in {relative}")
        match = matches[0]
        if match.group("value") != getattr(current, field):
            raise UpstreamError(f"{relative} {field} differs from {MANIFEST}")
        contents[relative] = (
            text[: match.start("value")] + getattr(target, field) + text[match.end("value") :]
        )
    contents[MANIFEST] = json.dumps(asdict(target), indent=2) + "\n"
    return contents


def frozen_worker_status(root: Path, pin: HarborPin) -> dict[str, Any]:
    """Retain honest build evidence; new dependencies require a real image rebuild."""
    try:
        lock = (root / "deploy/worker-image.lock").read_text(encoding="utf-8")
        wheels = json.loads((root / "deploy/worker-image.wheels.json").read_text(encoding="utf-8"))
        matches = re.findall(
            r"(?m)^harbor @ git\+https://github.com/harbor-framework/harbor.git@([0-9a-f]{40})$",
            lock,
        )
        revision = matches[0] if len(matches) == 1 else None
        matches_pin = (
            revision == pin.source_revision
            and wheels.get("harbor_compat_sha") == pin.source_revision
            and wheels.get("harbor_runtime_version") == pin.version
        )
        return {
            "source_revision": revision,
            "matches_pin": matches_pin,
            "regeneration_required": not matches_pin,
        }
    except (OSError, ValueError, AttributeError):
        return {"source_revision": None, "matches_pin": False, "regeneration_required": True}


def update(root: Path, target: HarborPin, *, dry_run: bool) -> dict[str, Any]:
    current = load_pin(root)
    target.validate()
    plan = pin_plan(root, current, target)
    changes = {
        relative: text
        for relative, text in plan.items()
        if (root / relative).read_text(encoding="utf-8") != text
    }
    if not dry_run:
        for relative, text in changes.items():
            (root / relative).write_text(text, encoding="utf-8")
    return {
        "dry_run": dry_run,
        "changed_files": sorted(changes),
        "pinned": asdict(target),
        "frozen_worker": frozen_worker_status(root, target),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="read-only latest-vs-pinned report")
    check.add_argument("--check-pins", action="store_true", help="fail on active local pin drift")
    check.add_argument(
        "--require-aligned", action="store_true", help="exit 1 when upstream differs"
    )
    check.add_argument(
        "--github-output", type=Path, help="append resolved values for GitHub Actions"
    )
    commands.add_parser("pins", help="check local active pins without network access")
    change = commands.add_parser("update", help="synchronize active pins to an explicit candidate")
    change.add_argument("--revision", required=True)
    change.add_argument("--version", required=True)
    change.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        pin = load_pin(args.root)
        if args.command == "update":
            report = update(
                args.root,
                replace(pin, source_revision=args.revision, version=args.version),
                dry_run=args.dry_run,
            )
        elif args.command == "pins":
            pin_plan(args.root, pin, pin)
            report = {
                "active_pins_match": True,
                "frozen_worker": frozen_worker_status(args.root, pin),
            }
        else:
            if args.check_pins:
                pin_plan(args.root, pin, pin)
            latest = discover(
                pin, GitHubClient(os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN", ""))
            )
            aligned = (
                pin.source_revision == latest.source_revision and pin.version == latest.version
            )
            report = {
                "aligned": aligned,
                "pinned": asdict(pin),
                "latest": asdict(latest),
                "compare_url": (
                    f"https://github.com/{pin.repository}/compare/"
                    f"{pin.source_revision}...{latest.source_revision}"
                ),
                "frozen_worker": frozen_worker_status(args.root, pin),
            }
            if args.github_output:
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write(
                        f"aligned={str(aligned).lower()}\n"
                        f"revision={latest.source_revision}\nversion={latest.version}\n"
                        f"compare_url={report['compare_url']}\n"
                    )
            print(json.dumps(report, indent=2))
            return 1 if args.require_aligned and not aligned else 0
        print(json.dumps(report, indent=2))
        return 0
    except (UpstreamError, OSError) as exc:
        print(f"harbor-upstream: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
