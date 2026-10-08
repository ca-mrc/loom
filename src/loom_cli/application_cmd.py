"""Owner controls for personal frontend/API deployments sharing development data."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import sys
from uuid import UUID, uuid4

import httpx

from loom.nebius_application_contract import (
    ApplicationCreateRequestV1,
    ApplicationOperationRequestV1,
)
from loom_cli.application_client import ApplicationClient
from loom_cli.application_versions import print_application_versions, print_release_compatibility
from loom_cli.contexts import current_context
from loom_cli.server_client import HttpStatusError, NotLoggedInError


def _retry_hint(arguments: list[str], key: str) -> None:
    context = current_context()
    # A leading hyphen is valid in API keys but argparse needs the attached form.
    key_args = [f"--idempotency-key={key}"] if key.startswith("-") else ["--idempotency-key", key]
    command = ["loom", *(["--context", context] if context is not None else []), "dev", "app", *arguments,
               *key_args]
    print(f"Idempotency-Key: {key}\nRetry: {shlex.join(command)}", file=sys.stderr)


def _run(args: argparse.Namespace) -> int:
    try:
        command = args.application_command
        request = None
        if command == "create":
            request = ApplicationCreateRequestV1(slug=args.slug, release_id=UUID(args.release))
        identity = UUID(args.application_id) if hasattr(args, "application_id") else None
        release = UUID(args.release) if command == "update" else None
        generation = getattr(args, "expected_generation", None)
        if generation is not None and generation < 1:
            raise ValueError("generation must be positive")
        key = ""
        if command in {"create", "update", "suspend", "resume", "destroy"}:
            key = args.idempotency_key or str(uuid4())
            if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", key) is None:
                raise ValueError("invalid idempotency key")
        with ApplicationClient() as client:
            if command == "capabilities":
                capabilities = client.capabilities()
                if args.json:
                    print(capabilities.model_dump_json())
                else:
                    labels = {
                        "not_configured": "not configured; ask the platform operator to enable it",
                        "configured": "configured",
                        "worker_unavailable": "configured; worker unavailable; ask the platform operator to inspect it",
                        "worker_unhealthy": "configured; worker unhealthy; ask the platform operator to inspect it",
                        "worker_healthy": "configured; worker healthy",
                    }
                    print(f"Application lifecycle: {labels[capabilities.application_lifecycle]}")
                    print(f"Source upload: {labels[capabilities.source_upload]}")
                    print(f"Image builds: {labels[capabilities.image_builds]}")
                    print("Task execution: not checked")
                    print("Reports management configuration and worker health only. "
                          "Storage access, pool admission, and deployed applications are not checked.")
            elif command == "versions":
                assert identity is not None
                versions = client.versions(identity)
                if args.json:
                    print(versions.model_dump_json())
                else:
                    print_application_versions(versions)
            elif command == "check-release":
                compatibility = client.check_release(UUID(args.release_id))
                if args.json:
                    print(compatibility.model_dump_json())
                else:
                    print_release_compatibility(compatibility)
                return 0 if compatibility.compatibility == "compatible" else 1
            elif request is not None:
                _retry_hint(["create", request.slug, "--release", str(request.release_id)], key)
                print(client.create(request, idempotency_key=key).model_dump_json())
            elif command in {"update", "suspend", "resume", "destroy"}:
                assert identity is not None
                if generation is None:
                    generation = client.status(identity).registration.deployment_generation
                transition = ApplicationOperationRequestV1.model_validate({
                    "action": "destroy_retained" if command == "destroy" else command,
                    "expected_generation": generation, "release_id": release,
                })
                flags = ["--release", str(release)] if release is not None else []
                _retry_hint([command, str(identity), "--expected-generation", str(generation), *flags], key)
                if command == "destroy":
                    print("Retaining shared data and application identity; no shared services are destroyed.", file=sys.stderr)
                print(client.transition(identity, transition, idempotency_key=key).model_dump_json())
            elif command == "list":
                print(json.dumps({"items": [row.model_dump(mode="json") for row in client.list()]}))
            elif command == "status":
                assert identity is not None
                print(client.status(identity).model_dump_json())
            elif command == "login":
                from loom_cli.application_login import login_application, open_application_browser

                assert identity is not None
                context = login_application(client, identity)
                print(f"Application login saved separately. Use: loom --context {context} auth whoami")
                if args.browser:
                    try:
                        opened = open_application_browser(client, identity)
                    except (ValueError, OSError, httpx.RequestError, HttpStatusError):
                        opened = False
                    if not opened:
                        print("CLI login is saved, but browser login could not open. Retry --browser on a desktop.", file=sys.stderr)
                        return 1
                    print("Browser login opened; confirm sign-in before the short-lived proof expires.")
            elif command == "retry":
                print(client.retry(UUID(args.operation_id)).model_dump_json())
            elif command == "evidence":
                print(client.evidence(UUID(args.operation_id)).model_dump_json())
            elif command == "wait":
                operation, terminal = client.wait(UUID(args.operation_id), timeout=args.timeout)
                print(operation.model_dump_json())
                if not terminal:
                    print(f"Wait timed out; operation {operation.operation_id} continues. No cancellation was requested.", file=sys.stderr)
                    return 2
                if operation.phase != "completed":
                    print(f"Operation {operation.operation_id} is {operation.phase}: {operation.error_code}", file=sys.stderr)
                    return 1
        return 0
    except (NotLoggedInError, HttpStatusError) as exc:
        print(str(exc), file=sys.stderr)
    except httpx.RequestError as exc:
        hint = ("Run this read-only command again." if args.application_command in {"evidence", "capabilities", "versions", "check-release"}
                else "Reuse the printed retry command.")
        print(f"Management request failed ({type(exc).__name__}); no automatic retry. {hint}", file=sys.stderr)
    except OSError:
        print("Could not securely save personal credentials; management login is unchanged.", file=sys.stderr)
    except (ValueError, KeyError, TypeError):
        print("Invalid application arguments or response; no local deployment was attempted.", file=sys.stderr)
    return 1


def add_application_subparser(commands: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    from loom_cli.application_build_cmd import add_build_subparsers

    parser = commands.add_parser("app", help="Manage personal frontend/API applications sharing development data")
    sub = parser.add_subparsers(dest="application_command", required=True)
    capabilities = sub.add_parser("capabilities", help="Read management configuration and worker health; not execution readiness")
    capabilities.add_argument("--json", action="store_true", help="Print the typed capability report as JSON")
    versions = sub.add_parser("versions", help="Read requested and last completed frontend/API versions")
    versions.add_argument("application_id")
    versions.add_argument("--json", action="store_true", help="Print the typed version report as JSON")
    compatibility = sub.add_parser("check-release", help="Check a release against the configured shared schema before deployment")
    compatibility.add_argument("release_id")
    compatibility.add_argument("--json", action="store_true", help="Print the typed compatibility report as JSON")
    create = sub.add_parser("create", help="Create a personal application from a qualified release")
    create.add_argument("slug")
    create.add_argument("--release", required=True, help="Qualified application release UUID")
    create.add_argument("--idempotency-key")
    sub.add_parser("list", help="List your personal applications")
    status = sub.add_parser("status", help="Read desired state and current application operation")
    status.add_argument("application_id")
    login = sub.add_parser("login", help="Sign into a ready application without replacing management credentials")
    login.add_argument("application_id")
    login.add_argument("--browser", action="store_true", help="Also open a separate short-lived browser sign-in")
    for action in ("update", "suspend", "resume", "destroy"):
        child = sub.add_parser(action, help=("Stop the application, retaining shared data and identity" if action == "destroy"
                                             else f"Request application {action}"))
        child.add_argument("application_id")
        child.add_argument("--expected-generation", type=int, help="Fence this generation; otherwise read current status")
        child.add_argument("--idempotency-key", help="Reuse with the printed generation after a lost response")
        if action == "update":
            child.add_argument("--release", required=True, help="Qualified application release UUID")
    retry = sub.add_parser("retry", help="Explicitly retry a blocked current application operation")
    retry.add_argument("operation_id")
    evidence = sub.add_parser("evidence", help="Read owner-scoped journal counts; not live readiness or retry authority")
    evidence.add_argument("operation_id")
    wait = sub.add_parser("wait", help="Wait without cancelling; exits 2 on timeout, 1 when blocked or superseded")
    wait.add_argument("operation_id")
    wait.add_argument("--timeout", type=float, default=300)
    parser.set_defaults(handler=_run)
    add_build_subparsers(sub)
