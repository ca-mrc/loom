"""`loom resources status` exposes Monitor resource-pool slots and native capacity."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, cast

import httpx

from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)


def _format_slots(pool: dict[str, Any]) -> str:
    active_slots = int(
        pool.get("current_active_slots", pool.get("total_slots", 0)),
    )
    return f"{int(pool.get('occupied_slots', 0))}/{active_slots}"



def _print_text(resources: dict[str, Any]) -> None:
    aggregate = cast(dict[str, Any], resources.get("aggregate") or {})
    pools = cast(list[dict[str, Any]], resources.get("pools") or [])

    occupied = int(aggregate.get("occupied_slots", 0))
    total = int(aggregate.get("current_active_slots", aggregate.get("total_slots", 0)))
    running = int(aggregate.get("running_tasks", 0))
    starting = int(aggregate.get("starting_tasks", 0))
    pre_start_fresh = int(aggregate.get("pre_start_heartbeat_fresh_tasks", 0))
    oldest_starting_age = aggregate.get("oldest_starting_task_age_sec")
    queued = int(aggregate.get("queued_tasks", 0))
    active_workers = int(aggregate.get("active_workers", 0))
    draining_workers = int(aggregate.get("draining_workers", 0))
    draining = int(aggregate.get("draining_slots", 0))
    free = int(aggregate.get("free_slots", 0))

    print(f"Concurrent tasks: {occupied} / {total}")
    print(f"Running: {running} · Starting: {starting} · Queued: {queued}")
    print(
        "Pre-start heartbeats: "
        f"{pre_start_fresh} fresh · oldest starting age "
        f"{oldest_starting_age if oldest_starting_age is not None else '-'}s",
    )
    print(f"Workers: {active_workers} active · Free slots: {free}")
    print(f"Draining: {draining} slots / {draining_workers} workers")
    print()
    print("Pools:")
    if not pools:
        print("  no active resource pools")
        return
    print(
        "  Pool                Backend  Arch    Used/Slots  Draining  "
        "Running  Starting  Queued  Workers  PreStart  OldestStart"
    )
    for pool in pools:
        print(
            "  "
            f"{pool.get('pool_name', 'default')!s:<19} "
            f"{pool.get('backend', 'docker')!s:<8} "
            f"{pool.get('cpu_arch', 'x86_64')!s:<7} "
            f"{_format_slots(pool):<11} "
            f"{int(pool.get('draining_slots', 0)):<9} "
            f"{int(pool.get('running_tasks', 0)):<8} "
            f"{int(pool.get('starting_tasks', 0)):<9} "
            f"{int(pool.get('queued_tasks', 0)):<7} "
            f"{int(pool.get('active_workers', 0)):<8} "
            f"{int(pool.get('pre_start_heartbeat_fresh_tasks', 0)):<9} "
            f"{pool.get('oldest_starting_task_age_sec') if pool.get('oldest_starting_task_age_sec') is not None else '-'}",
        )


def _is_inactive(target: dict[str, Any]) -> bool:
    return target.get("desired_state") in {"disabled", "retired"}


def _capacity_owners(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One target per shared compute pool, skipping fully inactive pools.

    Runtimes that name the same capacity owner share its nodes, so their
    observations must not be listed or added as separate capacity.
    """
    groups: dict[object, list[dict[str, Any]]] = {}
    for index, target in enumerate(targets):
        owner_id = target.get("capacity_owner_target_id") or target.get("target_id") or index
        groups.setdefault(owner_id, []).append(target)
    owners = []
    for owner_id, members in groups.items():
        if all(_is_inactive(member) for member in members):
            continue
        owners.append(next((m for m in members if m.get("target_id") == owner_id), members[0]))
    return owners


def _print_native_execution(
    service_execution: dict[str, Any],
    progress: dict[str, Any],
) -> None:
    stages = cast(dict[str, Any], progress.get("stages") or {})
    print("Native execution:")
    print(
        f"  Running: {int(stages.get('running', 0))} · "
        f"Starting: {int(stages.get('starting', 0))} · "
        f"Waiting: {int(stages.get('execution_wait', 0))} · "
        f"Preparing images: {int(stages.get('image_preparation', 0))} · "
        f"Archiving: {int(stages.get('archiving', 0))}",
    )
    owners = _capacity_owners(cast(list[dict[str, Any]], service_execution.get("targets") or []))
    if not owners:
        return
    print()
    print("  Region      Pool        Nodes  Occupied  Max  PendingJobs  Observation")
    for owner in owners:
        observation = cast(dict[str, Any], owner.get("observation") or {})
        policy = cast(dict[str, Any], owner.get("policy") or {})
        print(
            "  "
            f"{owner.get('region', '-')!s:<11} "
            f"{owner.get('pool_id', '-')!s:<11} "
            f"{observation.get('active_nodes', '?')!s:<6} "
            f"{observation.get('occupied_nodes', '?')!s:<9} "
            f"{policy.get('max_nodes', '?')!s:<4} "
            f"{observation.get('pending_jobs', '?')!s:<12} "
            f"{'fresh' if observation.get('is_fresh') else 'stale'}",
        )


def _status(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    try:
        with authed_client(cfg) as c:
            body = assert_2xx(
                c.get("/api/v1/monitor/summary", params={"view": "trials"}),
                action="fetch resource summary",
            )
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(f"error: could not reach {cfg.server_url}: {e}\n")
        return 2

    resources = cast(dict[str, Any], body.get("resources") or {})
    service_execution = cast(dict[str, Any], body.get("service_execution") or {})
    progress = cast(dict[str, Any], body.get("progress") or {})
    if args.format == "json":
        if service_execution:
            resources = {**resources, "service_execution": service_execution, "progress": progress}
        json.dump(resources, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    # Native Nebius trials hold leases, not worker slots, so the worker summary
    # below reads 0 for them. Like Monitor, omit it when only native capacity exists.
    native = bool(service_execution.get("targets"))
    if native:
        _print_native_execution(service_execution, progress)
    if not native or resources.get("pools"):
        if native:
            print()
        _print_text(resources)
    return 0


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom resources",
        description="Inspect concurrent task slots and resource-pool pressure.",
    )
    sub = parser.add_subparsers(dest="resources_cmd", required=True)

    p_status = sub.add_parser(
        "status",
        help="Show aggregate and per-pool execution-slot capacity.",
    )
    p_status.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format. Default: text.",
    )
    p_status.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Shortcut for --format json.",
    )
    p_status.set_defaults(handler=_status)

    args = parser.parse_args(argv)
    if getattr(args, "json_output", False):
        args.format = "json"
    return cast(int, args.handler(args))
