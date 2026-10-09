"""Manual external baseline grants; never print a bearer or upstream key."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="loom harbor-baselines")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="Create a fixed provider/model grant")
    for flag in ("label", "provider-connection-id", "model", "token-file"):
        create.add_argument("--" + flag, required=True)
    for flag in ("ttl-seconds", "max-calls", "max-output-tokens", "max-total-tokens"):
        create.add_argument("--" + flag, type=int, required=True)
    create.add_argument("--team-id")
    create.add_argument(
        "--budget-usd", help="Optional configured-price quota, not a supplier invoice cap"
    )
    create.add_argument("--admin-actor")
    for command in ("show", "calls", "revoke"):
        sub = commands.add_parser(command)
        sub.add_argument("id")
        sub.add_argument("--admin-actor")
    args = parser.parse_args(argv)
    owned_file: Path | None = None
    try:
        cfg = require_logged_in()
        headers = {"X-Loom-Admin-Actor": args.admin_actor} if args.admin_actor else None
        with authed_client(cfg) as client:
            if args.command == "create":
                # Exclusively reserve an owner-only file before creating paid
                # authority; never overwrite an existing secret or follow links.
                path = Path(args.token_file)
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                owned_file = path
                with os.fdopen(descriptor, "w") as destination:
                    payload = {
                        key: getattr(args, key)
                        for key in (
                            "label",
                            "provider_connection_id",
                            "model",
                            "team_id",
                            "ttl_seconds",
                            "max_calls",
                            "max_output_tokens",
                            "max_total_tokens",
                            "budget_usd",
                        )
                        if getattr(args, key) is not None
                    }
                    result = assert_2xx(
                        client.post("/api/v1/harbor-baselines", json=payload, headers=headers),
                        action="create Harbor baseline",
                    )
                    try:
                        destination.write(result.pop("token") + "\n")
                        destination.flush()
                        os.fsync(destination.fileno())
                    except OSError:
                        # A file failure must not silently leave a usable grant.
                        revoked = client.delete(
                            "/api/v1/harbor-baselines/" + result["id"], headers=headers
                        )
                        assert_2xx(revoked, action="revoke unwritten Harbor baseline")
                        raise
                owned_file = None
                result["bearer_file"] = str(path)
                print(json.dumps(result, indent=2))
            elif args.command == "revoke":
                response = client.delete("/api/v1/harbor-baselines/" + args.id, headers=headers)
                assert_2xx(response, action="revoke Harbor baseline")
                print("Harbor baseline revoked")
            else:
                suffix = "/calls" if args.command == "calls" else ""
                result = assert_2xx(
                    client.get("/api/v1/harbor-baselines/" + args.id + suffix),
                    action="read Harbor baseline",
                )
                print(json.dumps(result, indent=2))
        return 0
    except httpx.RequestError:
        sys.stderr.write("error: Harbor baseline service transport failed\n")
        return 1
    except (HttpStatusError, NotLoggedInError, OSError, ValueError) as exc:
        # Existing server client redacts HTTP diagnostics. Do not dump result,
        # request headers, SDK exceptions or secret-bearing configuration.
        sys.stderr.write(f"error: {exc}\n")
        return 1
    finally:
        if owned_file is not None:
            owned_file.unlink(missing_ok=True)
