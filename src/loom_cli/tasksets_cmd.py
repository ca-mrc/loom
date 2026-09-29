"""`loom tasksets {submit,status,lifecycle,rebuild,delete,list}` — manage team TaskSets.

Wraps the routes in `src/loom_service/routes/tasksets.py`.
Requires `loom auth login` to have been run first.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import httpx
import yaml  # type: ignore[import-untyped]

from loom.models.taskset import validate_bundle_archive_path
from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)
from loom_cli.time_format import format_local_datetime


class _IdNotFoundError(Exception):
    """Slug/id resolution failed — raise-not-exit for testability."""


def _run_with_error_handling(fn: Callable[[], int]) -> int:
    try:
        return fn()
    except (HttpStatusError, _IdNotFoundError) as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2


def _task_set_slug(task_set_id: str) -> str:
    return task_set_id.rsplit("/", 1)[-1]


def _resolve_task_set_id(client: httpx.Client, id_or_slug: str) -> str:
    if id_or_slug.startswith("ts/"):
        return id_or_slug
    list_resp = client.get("/api/v1/tasksets")
    items = assert_2xx(list_resp, action="list tasksets")["items"]
    matches = [
        it for it in items
        if _task_set_slug(it["task_set_id"]) == id_or_slug
    ]
    if not matches:
        raise _IdNotFoundError(
            f"no taskset matching {id_or_slug!r}. "
            "Run `loom tasksets list` to see what's available.",
        )
    if len(matches) > 1:
        raise _IdNotFoundError(
            f"multiple tasksets match slug {id_or_slug!r} "
            f"({len(matches)} rows). Use the full task_set_id.",
        )
    return cast(str, matches[0]["task_set_id"])


def _collect_submit_files(bundle_dir: Path) -> dict[str, tuple[str, bytes, str]]:
    manifest_path = bundle_dir / "manifest.yaml"
    if not manifest_path.is_file():
        raise _IdNotFoundError(
            f"manifest.yaml not found in {bundle_dir}. "
            "Expected a directory with manifest.yaml at the root.",
        )
    manifest_bytes = manifest_path.read_bytes()
    try:
        parsed = yaml.safe_load(manifest_bytes.decode("utf-8"))
    except yaml.YAMLError as exc:
        raise _IdNotFoundError(f"manifest.yaml is not valid YAML: {exc}") from exc
    if not isinstance(parsed, dict):
        raise _IdNotFoundError("manifest.yaml must contain a YAML mapping at the top level")

    files: dict[str, tuple[str, bytes, str]] = {
        "manifest": ("manifest.yaml", manifest_bytes, "application/x-yaml"),
    }

    source = parsed.get("source")
    if isinstance(source, dict) and source.get("type") == "bundle-upload":
        locator = source.get("locator")
        if not isinstance(locator, str):
            raise _IdNotFoundError("bundle-upload source must include locator")
        try:
            rel = validate_bundle_archive_path(locator).replace("\\", "/")
        except ValueError as exc:
            raise _IdNotFoundError(str(exc)) from exc
        bundle_path = bundle_dir / rel
        if not bundle_path.is_file():
            raise _IdNotFoundError(f"bundle upload archive not found: {bundle_path}")
        files["bundle"] = (rel, bundle_path.read_bytes(), "application/gzip")

    verifier = parsed.get("verifier")
    if verifier is not None:
        if not isinstance(verifier, dict) or not verifier.get("file"):
            raise _IdNotFoundError("manifest verifier block must include file")
        rel = str(verifier["file"]).replace("\\", "/")
        vpath = bundle_dir / rel
        if not vpath.is_file():
            raise _IdNotFoundError(f"verifier file not found: {vpath}")
        files["verifier"] = (rel, vpath.read_bytes(), "application/octet-stream")

    transform = parsed.get("transform")
    if transform is not None:
        if not isinstance(transform, dict) or not transform.get("file"):
            raise _IdNotFoundError("manifest transform block must include file")
        rel = str(transform["file"]).replace("\\", "/")
        tpath = bundle_dir / rel
        if not tpath.is_file():
            raise _IdNotFoundError(f"transform file not found: {tpath}")
        files["transform"] = (rel, tpath.read_bytes(), "text/x-python")

    return files


def _print_submit_summary(body: dict[str, Any]) -> None:
    print(f"task_set_id:   {body['task_set_id']}")
    print(f"status:        {body['status']}")
    print(f"capabilities:  {', '.join(body['capabilities'])}")
    print(f"job_id:        {body['materialization_job_id']}")
    for warning in body.get("warnings") or []:
        print(f"warning:       [{warning['code']}] {warning['message']}")


def _print_status_summary(body: dict[str, Any]) -> None:
    print(f"task_set_id:   {body['task_set_id']}")
    print(f"status:        {body['status']}")
    if body.get("status_reason"):
        print(f"status_reason: {body['status_reason']}")
    print(f"capabilities:  {', '.join(body['capabilities'])}")
    print(f"task_count:    {body['task_count']}")
    print(f"eval_ready:    {body['evaluation_ready']}")
    print(f"purpose:       {body.get('purpose') or '(unspecified)'}")
    print(f"expires_at:    {body.get('expires_at') or 'never'}")
    print(f"hold:          {body.get('hold', False)}")
    if body.get("materialization_job_state"):
        print(f"job_state:     {body['materialization_job_state']}")
    fence = body.get("materialization_fence")
    if isinstance(fence, dict):
        print(f"fence_lease_epoch:           {fence.get('lease_epoch')}")
        print(f"fence_lease_heartbeat_at:    {fence.get('lease_heartbeat_at')}")
        print(f"fence_lease_heartbeat_state: {fence.get('lease_heartbeat_state')}")
        print(f"fence_owner_fingerprint:     {fence.get('owner_fingerprint')}")
        print(f"fence_published_generation:  {fence.get('published_generation')}")
    errors = body.get("error_summary") or []
    if errors:
        print(f"errors:        {len(errors)} (first 50 retained on server)")
        for entry in errors[:5]:
            print(f"  - {entry}")


def _submit(args: argparse.Namespace) -> int:
    def _body() -> int:
        bundle_dir = Path(args.directory).resolve()
        if not bundle_dir.is_dir():
            sys.stderr.write(f"error: not a directory: {bundle_dir}\n")
            return 2
        files = _collect_submit_files(bundle_dir)
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.post("/api/v1/tasksets", files=files)
        body = assert_2xx(resp, action=f"submit taskset from {bundle_dir}")
        if args.format == "json":
            print(json.dumps(body, indent=2))
        else:
            _print_submit_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _status(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            task_set_id = _resolve_task_set_id(c, args.id)
            resp = c.get(f"/api/v1/tasksets/{task_set_id}")
        body = assert_2xx(resp, action=f"get taskset {args.id!r}")
        if args.format == "json":
            print(json.dumps(body, indent=2))
        else:
            _print_status_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _lifecycle(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            task_set_id = _resolve_task_set_id(c, args.id)
            path = f"/api/v1/tasksets/{task_set_id}"
            current = assert_2xx(c.get(path), action="read current lifecycle")
            payload = {key: current[key] for key in ("purpose", "expires_at", "hold")}
            if args.purpose is not None:
                payload["purpose"] = args.purpose
            if args.expires_at is not None or args.no_expiry:
                payload["expires_at"] = None if args.no_expiry else args.expires_at
            if args.hold is not None:
                payload["hold"] = args.hold
            payload["expected_updated_at"] = current["updated_at"]
            body = assert_2xx(c.patch(path + "/lifecycle", json=payload), action="set lifecycle")
        print(json.dumps(body, indent=2))
        return 0

    return _run_with_error_handling(_body)


def _rebuild(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            task_set_id = _resolve_task_set_id(c, args.id)
            resp = c.post(f"/api/v1/tasksets/{task_set_id}/rebuild")
        body = assert_2xx(resp, action=f"rebuild taskset {args.id!r}")
        if args.format == "json":
            print(json.dumps(body, indent=2))
        else:
            print(f"Re-enqueued materialization for {body['task_set_id']}")
            print(f"status: {body['status']}")
            print(f"job_id: {body['materialization_job_id']}")
        return 0

    return _run_with_error_handling(_body)


def _delete(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            task_set_id = _resolve_task_set_id(c, args.id)
            resp = c.delete(f"/api/v1/tasksets/{task_set_id}")
        assert_2xx(resp, action=f"delete taskset {args.id!r}")
        if args.format == "json":
            print(json.dumps({"deleted": task_set_id}, indent=2))
        else:
            print(f"Soft-deleted taskset {task_set_id!r}.")
        return 0

    return _run_with_error_handling(_body)


def _list(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/tasksets")
        body = assert_2xx(resp, action="list tasksets")
        items = body["items"]
        if args.format == "json":
            print(json.dumps(body, indent=2))
            return 0
        if not items:
            print("(no tasksets — run `loom tasksets submit ./my-taskset/`)")
            return 0
        for it in items:
            eval_flag = "yes" if it["evaluation_ready"] else "no"
            print(
                f"{it['task_set_id']:<48}  "
                f"{it['display_name']:<24}  "
                f"{it['status']:<14}  "
                f"tasks={it['task_count']:<4}  "
                f"eval={eval_flag}  "
                f"{format_local_datetime(it['created_at'])}",
            )
        return 0

    return _run_with_error_handling(_body)


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom tasksets",
        description=(
            "Manage team TaskSets on the deployed Loom server. "
            "Requires `loom auth login` first."
        ),
    )
    sub = parser.add_subparsers(dest="tasksets_cmd", required=True)

    p_submit = sub.add_parser(
        "submit",
        help=(
            "Submit a TaskSet directory "
            "(manifest.yaml + optional verifier/transform/bundle archive)."
        ),
    )
    p_submit.add_argument(
        "directory",
        help="Path to directory containing manifest.yaml",
    )
    p_submit.add_argument(
        "--format", choices=["text", "json"], default="text",
    )
    p_submit.set_defaults(handler=_submit)

    p_status = sub.add_parser(
        "status",
        help="Show materialization status for a TaskSet (full id or slug).",
    )
    p_status.add_argument("id", help="TaskSet id (ts/...) or slug")
    p_status.add_argument(
        "--format", choices=["text", "json"], default="text",
    )
    p_status.set_defaults(handler=_status)

    p_lifecycle = sub.add_parser("lifecycle", help="Update explicit expiry, purpose, or hold.")
    p_lifecycle.add_argument("id", help="TaskSet id (ts/...) or slug")
    p_lifecycle.add_argument("--purpose")
    expiry = p_lifecycle.add_mutually_exclusive_group()
    expiry.add_argument("--expires-at", help="ISO 8601 timestamp with time zone")
    expiry.add_argument("--no-expiry", action="store_true")
    p_lifecycle.add_argument("--hold", action=argparse.BooleanOptionalAction, default=None)
    p_lifecycle.set_defaults(handler=_lifecycle)

    p_rebuild = sub.add_parser(
        "rebuild",
        help="Re-enqueue materialization for a TaskSet.",
    )
    p_rebuild.add_argument("id", help="TaskSet id (ts/...) or slug")
    p_rebuild.add_argument(
        "--format", choices=["text", "json"], default="text",
    )
    p_rebuild.set_defaults(handler=_rebuild)

    p_delete = sub.add_parser(
        "delete",
        help="Soft-delete a TaskSet.",
    )
    p_delete.add_argument("id", help="TaskSet id (ts/...) or slug")
    p_delete.add_argument(
        "--format", choices=["text", "json"], default="text",
    )
    p_delete.set_defaults(handler=_delete)

    p_list = sub.add_parser(
        "list",
        help="List TaskSets visible to the logged-in team.",
    )
    p_list.add_argument(
        "--format", choices=["table", "json"], default="table",
    )
    p_list.set_defaults(handler=_list)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
