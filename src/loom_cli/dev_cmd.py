"""Personal applications and legacy isolated environments on the management server."""

from __future__ import annotations

import argparse
import json
import re
import sys
from uuid import UUID, uuid4

import httpx

from loom.nebius_environment_contract import EnvironmentCreateRequestV1
from loom_cli.environment_client import EnvironmentClient
from loom_cli.server_client import HttpStatusError, NotLoggedInError


def create_environment(slug: str, candidate: str, *, idempotency_key: str | None = None) -> int:
    return _run(argparse.Namespace(dev_command="create", slug=slug, candidate=candidate,
                                   idempotency_key=idempotency_key))


def _run(args: argparse.Namespace) -> int:
    try:
        create_request = None
        key = ""
        if args.dev_command == "create":
            create_request = EnvironmentCreateRequestV1(slug=args.slug, candidate_id=UUID(args.candidate))
        if args.dev_command in {"create", "destroy"}:
            key = args.idempotency_key or str(uuid4())
            if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", key) is None:
                raise ValueError("invalid idempotency key")
            # Print before any request so a lost response can be retried without
            # a duplicate create. A CLI timeout never cancels server-side work.
            print(f"Idempotency-Key: {key}", file=sys.stderr)
        with EnvironmentClient() as client:
            if create_request is not None:
                print(client.create(create_request, idempotency_key=key).model_dump_json())
            elif args.dev_command == "destroy":
                identity = UUID(args.environment_id)
                generation = args.expected_generation
                if generation is None:
                    generation = client.status(identity).registration.deployment_generation
                print(f"Retaining data and namespace claims. Retry: loom dev destroy {identity} "
                      f"--expected-generation {generation} --idempotency-key {key}", file=sys.stderr)
                print(client.destroy(identity, expected_generation=generation, idempotency_key=key).model_dump_json())
            elif args.dev_command == "login":
                from loom_cli.environment_login import login_environment, open_environment_browser

                context = login_environment(client, UUID(args.environment_id))
                print(f"Personal login saved separately. Use: loom --context {context} auth whoami")
                if args.browser:
                    try:
                        opened = open_environment_browser(client, UUID(args.environment_id))
                    except (ValueError, OSError, httpx.RequestError, HttpStatusError):
                        opened = False
                    if not opened:
                        print("CLI login is saved, but browser login could not open. Retry --browser on a desktop.", file=sys.stderr)
                        return 1
                    print("Browser login opened; confirm sign-in there before the short-lived proof expires.")
            elif args.dev_command == "list":
                print(json.dumps({"items": [row.model_dump(mode="json") for row in client.list()]}))
            elif args.dev_command == "status":
                print(client.status(UUID(args.environment_id)).model_dump_json())
            elif args.dev_command == "retry":
                print(client.retry(UUID(args.operation_id)).model_dump_json())
            elif args.dev_command == "wait":
                operation, terminal = client.wait(UUID(args.operation_id), timeout=args.timeout)
                print(operation.model_dump_json())
                if not terminal:
                    print(f"Wait timed out; operation {operation.operation_id} continues. Run loom dev wait again.", file=sys.stderr)
                    return 2
                if operation.phase == "blocked":
                    print(f"Operation {operation.operation_id} is blocked: {operation.error_code}", file=sys.stderr)
                    return 1
        return 0
    except (NotLoggedInError, HttpStatusError) as exc:
        print(str(exc), file=sys.stderr)
    except httpx.RequestError as exc:
        print(f"Management request failed ({type(exc).__name__}); retry with the same Idempotency-Key.", file=sys.stderr)
    except OSError:
        print("Could not securely save personal credentials; management login is unchanged.", file=sys.stderr)
    except (ValueError, KeyError):
        print("Invalid management arguments or response; no local deployment was attempted.", file=sys.stderr)
    return 1


def add_dev_subparser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    from loom_cli.application_cmd import add_application_subparser

    parser = sub.add_parser(
        "dev", help="Personal development: use loom dev app on your management server",
        description=("Recommended: loom dev app manages personal frontend/API applications sharing development data. "
                     "Direct create/list/status and other commands below are legacy isolated-environment controls; "
                     "they retain their original behavior and do not redirect to application commands."),
    )
    parser.set_defaults(handler=_run)
    commands = parser.add_subparsers(dest="dev_command", required=True)
    add_application_subparser(commands)
    create = commands.add_parser("create", help="Legacy: request an isolated environment from an approved candidate")
    create.add_argument("slug")
    create.add_argument("--candidate", required=True, help="Approved candidate UUID from the management installation")
    create.add_argument("--idempotency-key", help="Reuse this key when retrying the same create")
    commands.add_parser("list", help="Legacy: list your retained isolated-environment identities")
    destroy = commands.add_parser("destroy", help="Legacy: stop an isolated environment, retaining data and names")
    destroy.add_argument("environment_id")
    destroy.add_argument("--expected-generation", type=int, help="Fence this exact generation; otherwise read current status")
    destroy.add_argument("--idempotency-key", help="Reuse with the printed generation after a lost response")
    login = commands.add_parser("login", help="Legacy: sign into an isolated environment with separate credentials")
    login.add_argument("environment_id")
    login.add_argument("--browser", action="store_true", help="Also open a separate, short-lived browser sign-in")
    status = commands.add_parser("status", help="Legacy: read isolated-environment state and provisioning operation")
    status.add_argument("environment_id")
    retry = commands.add_parser("retry", help="Legacy: retry a blocked isolated-environment operation")
    retry.add_argument("operation_id")
    wait = commands.add_parser("wait", help="Legacy: wait for an isolated-environment operation without cancelling")
    wait.add_argument("operation_id")
    wait.add_argument("--timeout", type=float, default=300)
