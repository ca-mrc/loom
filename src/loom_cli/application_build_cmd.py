"""Source-bound personal build requests; transport uncertainty never starts a retry."""
from __future__ import annotations

import argparse
import re
import shlex
import sys
from pathlib import Path
from uuid import UUID, uuid4

import httpx

from loom_cli.application_client import ApplicationClient
from loom_cli.application_source import package_application_source
from loom_cli.contexts import current_context
from loom_cli.server_client import HttpStatusError, NotLoggedInError


def _retry(arguments: list[str]) -> None:
    context = current_context()
    command = ["loom", *(["--context", context] if context is not None else []), "dev", "app", *arguments]
    print("Retry: " + shlex.join(command), file=sys.stderr)


def _run(args: argparse.Namespace) -> int:
    try:
        command = args.application_command
        if command == "build":
            key = args.idempotency_key or str(uuid4())
            if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", key) is None:
                raise ValueError("invalid idempotency key")
            key_args = [f"--idempotency-key={key}"] if key.startswith("-") else ["--idempotency-key", key]
            if args.source is not None:
                root = Path(args.source).resolve(strict=True)
                with package_application_source(root) as source:
                    if args.source_digest is not None and args.source_digest != source.manifest.digest:
                        print("Source changed since the original request; restore it or start a new build with a new key.", file=sys.stderr)
                        return 1
                    print(f"Personal source {source.manifest.digest}; local code is not CI-approved.", file=sys.stderr)
                    _retry(["build", "--source", str(root), "--source-digest", source.manifest.digest, *key_args])
                    with ApplicationClient() as client:
                        receipt = client.create_source_upload(source, idempotency_key=key)
                        if receipt.phase != "source_verified":
                            receipt = client.upload_source(receipt, source)
                        _retry(["build", "--upload-id", str(receipt.upload_id), *key_args])
                        result = client.create_build(receipt, idempotency_key=key)
            else:
                if args.source_digest is not None:
                    raise ValueError("source digest requires local source")
                identity = UUID(args.upload_id)
                _retry(["build", "--upload-id", str(identity), *key_args])
                with ApplicationClient() as client:
                    receipt = client.source_upload_status(identity)
                    result = client.create_build(receipt, idempotency_key=key)
            print(result.model_dump_json())
            return 0
        identity = UUID(args.build_id)
        with ApplicationClient() as client:
            if command == "build-status":
                print(client.build_status(identity).model_dump_json())
            elif command == "build-wait":
                result, terminal = client.wait_build(identity, timeout=args.timeout)
                print(result.model_dump_json())
                if not terminal:
                    print(f"Wait timed out; build {identity} continues. No cancellation was requested.", file=sys.stderr)
                    return 2
                if result.phase != "ready":
                    print(f"Build {identity} is {result.phase}.", file=sys.stderr)
                    return 1
            else:
                if not 0 < args.attempt < 2**63:
                    raise ValueError("invalid application build attempt")
                _retry([command, str(identity), "--attempt", str(args.attempt)])
                print(client.change_build(identity, action=command.removeprefix("build-"), attempt=args.attempt).model_dump_json())
        return 0
    except (NotLoggedInError, HttpStatusError) as exc:
        print(str(exc), file=sys.stderr)
    except httpx.RequestError as exc:
        print(f"Management request failed ({type(exc).__name__}); no automatic retry. "
            "For writes, reuse the printed retry command; status/wait can be read again.", file=sys.stderr)
    except OSError:
        print("Could not securely capture application source or load management credentials.", file=sys.stderr)
    except (ValueError, KeyError, TypeError):
        print("Invalid build arguments, source, or response; no automatic retry or deployment was attempted.", file=sys.stderr)
    return 1


def add_build_subparsers(commands: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    build = commands.add_parser("build", help="Build personal service/web images from local source (not CI-approved)")
    source = build.add_mutually_exclusive_group(required=True)
    source.add_argument("--source", help="Git checkout including committed, modified and non-ignored untracked files")
    source.add_argument("--upload-id", help="Resume from an already-verified source upload UUID")
    build.add_argument("--source-digest", help="Require this captured digest when replaying a local-source request")
    build.add_argument("--idempotency-key", help="Reuse the printed key after an uncertain reply")
    build.set_defaults(handler=_run)
    for name in ("build-status", "build-wait", "build-cancel", "build-retry"):
        child = commands.add_parser(name, help={
            "build-status": "Read owner build status and its qualified release, when ready",
            "build-wait": "Wait without cancelling; exits 2 on timeout, 1 for failure/cancellation",
            "build-cancel": "Request cancellation of this exact build attempt",
            "build-retry": "Explicitly retry a failed/cancelled attempt after cleanup",
        }[name])
        child.add_argument("build_id")
        if name == "build-wait":
            child.add_argument("--timeout", type=float, default=300)
        if name in {"build-cancel", "build-retry"}:
            child.add_argument("--attempt", type=int, required=True, help="Expected attempt; keep it unchanged when replaying")
        child.set_defaults(handler=_run)
