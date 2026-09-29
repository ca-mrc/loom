"""`loom eval {run, batch, trial}` — submit / inspect work on the deployed
Loom server.

Wraps the server-side routes:
- POST /api/v1/trials                  → `loom eval run`
- GET  /api/v1/trials[/{id}]           → `loom eval trial {list, show}`
- POST /api/v1/trials/{id}/cancel      → `loom eval trial cancel`
- POST /api/v1/batches                 → `loom eval batch create`
  (optional ``trial_config.workspace_staging_policy_name`` via
  ``--workspace-staging-policy``; #1263)
  (optional terminus-2 ``trial_config.multi_model`` via ``--multi-model``;
  #1380 — default remains single-model)
- GET  /api/v1/batches[/{id}]          → `loom eval batch {list, show}`
- POST /api/v1/batches/{id}/cancel     → `loom eval batch cancel`

`--provider NAME` is the CLI-facing handle; the route layer wants a
UUID. We resolve via `/provider-connections` (matches the lookup
`loom providers show/update/delete` already does).

`loom run` (existing, local-stateless) stays as-is. The distinction
(cluster-deploy.md §CLI surface): `loom run` runs one trial on your
machine with no server; `loom eval` submits to a deployed Loom
(batches, persistence, sharing across the team).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import sys
from pathlib import Path
from typing import Any, cast

import httpx

from loom.security.redaction import redact_mapping
from loom_cli.backend_flag import add_legacy_backend_flag, warn_legacy_backend_flag
from loom_cli.minio_storage_preflight import validate_minio_storage_preflight_artifact
from loom_cli.nebius_acceptance import configure_parser as configure_nebius_acceptance_parser
from loom_cli.providers_cmd import (
    _resolve_by_name,
    _run_with_error_handling,
)
from loom_cli.server_client import (
    assert_2xx,
    assert_2xx_response,
    authed_client,
    require_logged_in,
)
from loom_cli.time_format import format_local_datetime
from loom_service.agent_catalog import get_agent

# Map provider_connection.type → ModelSpec.provider. The agent uses
# the `provider/name` form for its LLM-call serialization (direct completion,
# provider SDK adapters, etc.); the gateway is what actually routes via the
# connection's credentials. For `custom`, default to openai-compatible
# wire shape — operators creating a `custom` connection are typically
# pointing at an OpenAI-compatible endpoint.
_TYPE_TO_AGENT_PROVIDER: dict[str, str] = {
    "openai-compatible": "openai",
    "anthropic": "anthropic",
    "google": "google",
    "custom": "openai",
}

_TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})


def _build_agent_model(
    connection_type: str,
    model_name: str,
    *,
    agent_provider_override: str | None = None,
) -> dict[str, Any]:
    """Construct the `agent_model` dict the worker hands to the agent.

    Provider resolution priority:
    1. `agent_provider_override` (the operator's explicit
       `--agent-provider` flag) — wins when set.
    2. `_TYPE_TO_AGENT_PROVIDER[connection_type]` — sensible default.
    3. Fallback `"openai"` for unknown types.

    Why the override matters (#69): connections of type
    `openai-compatible` or `custom` pointing at Together / Fireworks /
    Mistral need `agent_model.provider="together"` (etc.) for the
    legacy rate-card lookup to find pricing entries. Without the
    override every such connection became `provider="openai"` and
    silently lost cost attribution. The facade (PR #64) uses
    operator-supplied pricing so this matters mostly for the legacy
    `/v1/chat/completions` path — but operators upgrading partial
    deployments still hit it.
    """
    if agent_provider_override:
        provider = agent_provider_override
    else:
        provider = _TYPE_TO_AGENT_PROVIDER.get(connection_type, "openai")
    return {
        "provider": provider,
        "name": model_name,
        "source": "api",
    }


def _load_task_filter_json(raw: str, *, flag: str = "--task-filter") -> dict[str, Any]:
    """Parse an object argument from a JSON string or @path/to/file."""
    if raw.startswith("@"):
        path = raw[1:]
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except OSError as exc:
            raise argparse.ArgumentTypeError(
                f"{flag} @{path}: {exc}",
            ) from exc
        except json.JSONDecodeError as exc:
            raise argparse.ArgumentTypeError(
                f"{flag} @{path}: invalid JSON: {exc}",
            ) from exc
    else:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise argparse.ArgumentTypeError(
                f"{flag}: invalid JSON: {exc}",
            ) from exc
    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError(
            f"{flag} must be a JSON object",
        )
    return cast(dict[str, Any], data)


def _load_task_resource_requests(raw: str) -> dict[str, Any]:
    return _load_task_filter_json(raw, flag="--task-resource-requests")


def _load_combinations_json(raw: str) -> list[dict[str, Any]]:
    """Parse --combinations-json argument. JSON string or @path/to/file."""
    if raw.startswith("@"):
        path = raw[1:]
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except OSError as exc:
            raise argparse.ArgumentTypeError(
                f"--combinations-json @{path}: {exc}",
            ) from exc
        except json.JSONDecodeError as exc:
            raise argparse.ArgumentTypeError(
                f"--combinations-json @{path}: invalid JSON: {exc}",
            ) from exc
    else:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise argparse.ArgumentTypeError(
                f"--combinations-json: invalid JSON: {exc}",
            ) from exc
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise argparse.ArgumentTypeError(
            "--combinations-json must be a JSON array of objects",
        )
    if not data:
        raise argparse.ArgumentTypeError(
            "--combinations-json must contain at least one combination",
        )
    return cast(list[dict[str, Any]], data)


def _agent_items_from_response(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        items = data.get("items")
    else:
        items = data
    if not isinstance(items, list):
        return []
    return [cast(dict[str, Any], item) for item in items if isinstance(item, dict)]


def _agent_needs_model(
    client: httpx.Client,
    agent_name: str,
) -> tuple[bool | None, str | None]:
    agent = get_agent(agent_name)
    if agent is not None:
        return agent.needs_model, None

    data = assert_2xx(
        client.get("/api/v1/agents"),
        action=f"resolve service agent catalog for {agent_name!r}",
    )
    for item in _agent_items_from_response(data):
        if item.get("name") != agent_name:
            continue
        needs_model = item.get("needs_model")
        if not isinstance(needs_model, bool):
            return None, (
                f"error: service agent catalog entry {agent_name!r} did not "
                "include a boolean needs_model field.\n"
            )
        if item.get("service_mode_ready") is False:
            message = item.get("readiness_message")
            suffix = f": {message}" if isinstance(message, str) and message else ""
            return None, f"error: agent {agent_name!r} is not service-mode ready{suffix}.\n"
        return needs_model, None
    return None, (
        f"error: unknown agent {agent_name!r}. Local loom-launcher adapters "
        "were not available, and the server catalog did not list this agent. "
        "Run `loom agents list` or check GET /api/v1/agents.\n"
    )


# ──────────────────────────────────────────────────────────────────────
# eval run — single trial
# ──────────────────────────────────────────────────────────────────────


def _print_trial_summary(item: dict[str, Any]) -> None:
    print(f"id:               {item.get('id') or item.get('trial_id')}")
    print(f"task_id:          {item.get('task_id', '(unknown)')}")
    print(f"state:            {item.get('state', '(unknown)')}")
    if item.get("agent_name") is not None:
        print(f"agent:            {item['agent_name']}")
    if item.get("model") is not None:
        # `model` from `_trial_row` is the agent_model dict.
        m = item["model"]
        if isinstance(m, dict):
            print(f"model:            {m.get('provider')}/{m.get('name')}")
        else:
            print(f"model:            {m}")
    if item.get("aggregate_reward") is not None:
        print(f"reward:           {item['aggregate_reward']}")
    if item.get("llm_calls_count") is not None:
        print(f"llm_calls:        {item['llm_calls_count']}")
    if item.get("llm_evidence_status") is not None:
        print(f"llm_evidence:     {item['llm_evidence_status']}")
    if item.get("no_call") is True:
        print(
            "warning:          invalid benchmark evidence; trial did not reach the gateway/provider."
        )
    if (
        item.get("total_prompt_tokens") is not None
        or item.get("total_completion_tokens") is not None
    ):
        print(
            "tokens:           "
            f"prompt={item.get('total_prompt_tokens', 0)} "
            f"completion={item.get('total_completion_tokens', 0)}",
        )
    if item.get("submitted_at"):
        print(f"submitted_at:     {format_local_datetime(item['submitted_at'])}")
    if item.get("finished_at"):
        print(f"finished_at:     {format_local_datetime(item['finished_at'])}")
    if item.get("failure_reason"):
        print(f"failure_reason:   {item['failure_reason']}")


def _dump_json(data: Any) -> None:
    """Print JSON safely for public CLI output."""
    print(json.dumps(redact_mapping(data), indent=2))


def _print_debug_evidence_summary(evidence: dict[str, Any]) -> None:
    entity = evidence.get("entity") if isinstance(evidence, dict) else {}
    lifecycle = evidence.get("lifecycle") if isinstance(evidence, dict) else {}
    failure = evidence.get("failure") if isinstance(evidence, dict) else {}
    provider = evidence.get("provider") if isinstance(evidence, dict) else {}
    next_actions = evidence.get("next_actions")
    entity_type = entity.get("type") if isinstance(entity, dict) else "unknown"
    entity_id = entity.get("id") if isinstance(entity, dict) else "unknown"
    print(f"debug_evidence:        {entity_type}")
    print(f"id:                    {entity_id}")
    if isinstance(lifecycle, dict):
        if lifecycle.get("state") is not None:
            print(f"state:                 {lifecycle['state']}")
        terminal = lifecycle.get("terminal_status")
        if terminal is not None:
            print(f"terminal_status:       {terminal}")
        attempts = lifecycle.get("attempt_count")
        if attempts is not None:
            print(f"attempt_count:         {attempts}")
    if isinstance(failure, dict):
        print(f"reason_code:           {failure.get('reason_code', 'unknown')}")
        print(f"category:              {failure.get('category', 'unknown')}")
        print(f"attribution:           {failure.get('attribution', 'unknown')}")
        if failure.get("message"):
            print(f"message:               {failure['message']}")
    if isinstance(provider, dict):
        print(f"llm_calls:             {provider.get('llm_calls_count', 0)}")
        models = provider.get("models")
        if isinstance(models, list) and models:
            print("models:                " + ", ".join(str(m) for m in models))
    if isinstance(next_actions, list) and next_actions:
        print("next_actions:")
        for action in next_actions:
            print(f"  - {action}")


def _format_ratio(value: Any) -> str:
    if isinstance(value, int | float):
        return f"{value:.0%}"
    return "-"


def _print_diagnosis_report(report: dict[str, Any]) -> None:
    entity = report.get("entity") if isinstance(report, dict) else {}
    primary = report.get("primary_cause") if isinstance(report, dict) else {}
    evidence = report.get("evidence")
    actions = report.get("next_actions")
    clusters = report.get("reason_clusters")
    entity_type = entity.get("type") if isinstance(entity, dict) else "unknown"
    entity_id = entity.get("id") if isinstance(entity, dict) else "unknown"
    print(f"Diagnosis: {entity_type}")
    print(f"id:        {entity_id}")
    summary = report.get("summary")
    if isinstance(summary, str) and summary:
        print()
        print(summary)
    if isinstance(primary, dict):
        print()
        print("Primary cause:")
        print(f"  reason_code:     {primary.get('reason_code', 'unknown')}")
        print(f"  category:        {primary.get('category', 'unknown')}")
        print(f"  attribution:     {primary.get('attribution', 'unknown')}")
        print(f"  confidence:      {primary.get('confidence', 'unknown')}")
        affected = primary.get("affected_trials")
        if affected is not None:
            print(
                f"  affected:        {affected} ({_format_ratio(primary.get('affected_ratio'))})",
            )
    impact = report.get("impact")
    if isinstance(impact, str) and impact:
        print()
        print("Impact:")
        print(f"  {impact}")
    if isinstance(evidence, list) and evidence:
        print()
        print("Evidence:")
        for item in evidence:
            print(f"  - {item}")
    if isinstance(clusters, list) and clusters:
        print()
        print("Reason clusters:")
        for cluster in clusters:
            if not isinstance(cluster, dict):
                continue
            representative = cluster.get("representative_trial_id")
            suffix = f" representative={representative}" if representative else ""
            print(
                "  - "
                f"{cluster.get('reason_code', 'unknown')}: "
                f"{cluster.get('count', 0)} "
                f"({_format_ratio(cluster.get('affected_ratio'))})"
                f"{suffix}",
            )
    if isinstance(actions, list) and actions:
        print()
        print("Next actions:")
        for index, action in enumerate(actions, start=1):
            if isinstance(action, dict):
                label = action.get("label", "Action")
                command = action.get("command")
                action_name = action.get("action")
                if command:
                    print(f"  {index}. {label}: {command}")
                elif action_name:
                    print(f"  {index}. {label} ({action_name})")
                else:
                    print(f"  {index}. {label}")
            else:
                print(f"  {index}. {action}")


def _print_rerun_plan(plan: dict[str, Any]) -> None:
    summary = plan.get("summary") if isinstance(plan, dict) else {}
    print("Supplemental rerun plan")
    print(f"batch_id:            {plan.get('batch_id', '(unknown)')}")
    if isinstance(summary, dict):
        print(f"auto_safe:           {summary.get('auto_safe', 0)}")
        print(f"operator_approval:   {summary.get('operator_approval', 0)}")
        print(f"not_rerunnable:      {summary.get('not_rerunnable', 0)}")
        print(f"already_covered:     {summary.get('already_covered', 0)}")
    task_ids = plan.get("supplemental_task_ids")
    if isinstance(task_ids, list) and task_ids:
        print("supplemental_task_ids:")
        for task_id in task_ids:
            print(f"  - {task_id}")
    else:
        print("supplemental_task_ids: (none)")
    coordinates = plan.get("supplemental_coordinates")
    if isinstance(coordinates, list) and coordinates:
        print("supplemental_coordinates:")
        for row in coordinates:
            if not isinstance(row, dict):
                continue
            print(
                "  - "
                f"{row.get('task_id')} "
                f"sample={row.get('sample_idx', 0)} "
                f"combination={row.get('combination_idx', 0)}",
            )
    for key, title in (
        ("operator_approval", "Needs operator approval"),
        ("not_rerunnable", "Not rerunnable"),
    ):
        rows = plan.get(key)
        if not isinstance(rows, list) or not rows:
            continue
        print(title + ":")
        for row in rows[:10]:
            if not isinstance(row, dict):
                continue
            suffix = " / ".join(
                str(row.get(field))
                for field in ("failure_class", "root_cause")
                if row.get(field)
            )
            print(f"  - {row.get('task_id')} ({suffix})")


def _run(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            conn = _resolve_by_name(c, args.provider)
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": _build_agent_model(
                    conn["type"],
                    args.model,
                    agent_provider_override=args.agent_provider,
                ),
            }
            payload: dict[str, Any] = {
                "task_id": args.task,
                "config": trial_config,
                "provider_connection_id": conn["id"],
                "provider_model_id": args.model,
            }
            resp = c.post("/api/v1/trials", json=payload)
        body = assert_2xx(resp, action=f"submit trial for task {args.task!r}")
        print(
            f"Submitted trial for task {args.task!r} via provider {args.provider!r}:",
        )
        body.setdefault("task_id", args.task)
        _print_trial_summary(body)
        return 0

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# eval batch create / list / show / cancel
# ──────────────────────────────────────────────────────────────────────


def _print_batch_summary(item: dict[str, Any]) -> None:
    print(f"id:                    {item.get('id') or item.get('batch_id')}")
    print(f"name:                  {item.get('name', '(unset)')}")
    print(f"state:                 {item.get('state', '(unknown)')}")
    print(f"expected_trial_count:  {item.get('expected_trial_count', '?')}")
    print(f"n_per_task:            {item.get('n_per_task', 1)}")
    print(f"backend:               {item.get('backend') or '(unknown)'}")
    required_worker_pools = item.get("required_worker_pools") or []
    if required_worker_pools:
        print(f"required_worker_pools: {', '.join(required_worker_pools)}")
    if item.get("failure_reason"):
        print(f"failure_reason:        {item['failure_reason']}")
    if item.get("failure_message"):
        print(f"failure_message:       {item['failure_message']}")
    fanout_errors = item.get("fanout_errors") or []
    if fanout_errors:
        print(f"fanout_errors:         {len(fanout_errors)}")
    if item.get("trial_summary"):
        ts = item["trial_summary"]
        print(
            f"trials:                "
            f"q={ts.get('queued', 0)}  "
            f"c={ts.get('claimed', 0)}  "
            f"r={ts.get('running', 0)}  "
            f"s={ts.get('succeeded', 0)}  "
            f"f={ts.get('failed', 0)}  "
            f"x={ts.get('cancelled', 0)}",
        )
    if item.get("aggregate_reward") is not None:
        print(f"aggregate_reward:      {item['aggregate_reward']}")
    if item.get("llm_calls_count") is not None:
        print(f"llm_calls:             {item['llm_calls_count']}")
    if item.get("llm_evidence_status") is not None:
        print(f"llm_evidence_status: {item['llm_evidence_status']}")
    if item.get("no_call_trial_count") is not None:
        print(f"no_call_trials:      {item['no_call_trial_count']}")
    if item.get("llm_evidence_status") in {
        "no_calls_invalid",
        "partial_no_calls",
    }:
        print(
            "warning:               invalid benchmark evidence; terminal "
            "model-backed trials did not reach the gateway/provider.",
        )
    if (
        item.get("total_prompt_tokens") is not None
        or item.get("total_completion_tokens") is not None
    ):
        print(
            "tokens:                "
            f"prompt={item.get('total_prompt_tokens', 0)} "
            f"completion={item.get('total_completion_tokens', 0)}",
        )
    if item.get("created_at"):
        print(f"created_at:            {format_local_datetime(item['created_at'])}")
    if item.get("finished_at"):
        print(f"finished_at:           {format_local_datetime(item['finished_at'])}")


def _batch_create(args: argparse.Namespace) -> int:
    def _body() -> int:
        request_json = getattr(args, "request_json", None)
        if request_json is not None:
            conflicting = [
                "--" + dest.replace("_", "-")
                for dest, default in args.batch_create_defaults.items()
                if dest != "request_json" and getattr(args, dest, default) != default
            ]
            if conflicting:
                sys.stderr.write(
                    "error: --request-json is a complete batch request; omit "
                    + ", ".join(conflicting) + ".\n",
                )
                return 2
            cfg = require_logged_in()
            with authed_client(cfg) as c:
                response = c.post("/api/v1/batches", json=request_json)
            body = assert_2xx(response, action="create batch from request JSON")
            print(f"Created batch {body.get('name') or '(server-generated)'}:")
            _print_batch_summary(body)
            return 0
        warn_legacy_backend_flag(args.backend)
        if args.storage_preflight_evidence is not None:
            validation = validate_minio_storage_preflight_artifact(
                Path(args.storage_preflight_evidence).resolve(),
                allow_stop_override=args.override_storage_preflight_stop,
            )
            if not validation.ok:
                sys.stderr.write(f"error: {validation.message}\n")
                return 1
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            combinations = args.combinations_json
            conn: dict[str, Any] | None = None
            multi_model = bool(getattr(args, "multi_model", False))
            if multi_model and combinations is not None:
                sys.stderr.write(
                    "error: --multi-model is not supported with "
                    "--combinations-json; use the single-agent "
                    "`--agent terminus-2` path.\n",
                )
                return 2
            if multi_model:
                if args.agent != "terminus-2":
                    sys.stderr.write(
                        "error: --multi-model requires --agent terminus-2 "
                        f"(got {args.agent!r}). Default remains a single-model trial.\n",
                    )
                    return 2
                if not args.multi_model_secondary:
                    sys.stderr.write(
                        "error: --multi-model requires "
                        "--multi-model-secondary <model-id>.\n",
                    )
                    return 2
                if not args.provider or not args.model:
                    sys.stderr.write(
                        "error: --multi-model requires --provider and "
                        "--model (primary) plus --multi-model-secondary.\n",
                    )
                    return 2
            if combinations is not None:
                conflicting = [
                    flag
                    for flag, value in (
                        ("--agent", args.agent),
                        ("--agent-version", getattr(args, "agent_version", None)),
                        ("--provider", args.provider),
                        ("--model", args.model),
                        ("--agent-provider", args.agent_provider),
                        ("--n-per-task", args.n_per_task),
                    )
                    if value is not None
                ]
                if conflicting:
                    message = (
                        "error: --combinations-json carries agent/model/provider "
                        "routing; omit " + ", ".join(conflicting) + ".\n"
                    )
                    sys.stderr.write(message)
                    return 2
                trial_config: dict[str, Any] = {}
            else:
                if args.agent is None:
                    sys.stderr.write(
                        "error: --agent is required unless --combinations-json is supplied.\n",
                    )
                    return 2
                needs_model, agent_err = _agent_needs_model(c, args.agent)
                if agent_err is not None:
                    sys.stderr.write(agent_err)
                    return 2
                assert needs_model is not None

                if needs_model:
                    missing = [
                        flag
                        for flag, value in (
                            ("--provider", args.provider),
                            ("--model", args.model),
                        )
                        if not value
                    ]
                    if missing:
                        message = (
                            f"error: agent {args.agent!r} requires --provider and "
                            "--model for batch creation; missing " + ", ".join(missing) + ".\n"
                        )
                        sys.stderr.write(message)
                        return 2
                elif args.provider or args.model or args.agent_provider:
                    sys.stderr.write(
                        f"error: agent {args.agent!r} does not take a model; omit "
                        "--provider, --model, and --agent-provider.\n",
                    )
                    return 2

                trial_config = {
                    "agent_name": args.agent,
                    "agent_model": None,
                }
                if getattr(args, "agent_version", None) is not None:
                    trial_config["agent_version"] = args.agent_version
                if needs_model:
                    conn = _resolve_by_name(
                        c,
                        args.provider,
                        team_id=args.team_id,
                    )
                    trial_config["agent_model"] = _build_agent_model(
                        conn["type"],
                        args.model,
                        agent_provider_override=args.agent_provider,
                    )
            if args.workspace_staging_policy is not None:
                # #1263: shared batch knob (also applies with --combinations-json).
                trial_config["workspace_staging_policy_name"] = (
                    args.workspace_staging_policy
                )
            if multi_model:
                # #1380: K1/K2 blocks or beta_mixture (terminus-2 only).
                assert conn is not None
                assert args.multi_model_secondary is not None
                schedule_flags = (
                    args.multi_model_switch_episode is not None
                    or getattr(args, "multi_model_teacher_episodes", None) is not None
                    or args.multi_model_episode_ceiling is not None
                )
                beta = getattr(args, "multi_model_beta", None)
                mix_seed = getattr(args, "multi_model_seed", None)
                step_start = getattr(args, "multi_model_step_start", None)
                step_end = getattr(args, "multi_model_step_end", None)
                turn_flags = step_start is not None or step_end is not None
                policy_flag = getattr(args, "multi_model_policy", None)
                if beta is not None and schedule_flags:
                    sys.stderr.write(
                        "error: --multi-model-beta cannot be combined with "
                        "--multi-model-switch-episode, "
                        "--multi-model-teacher-episodes, or "
                        "--multi-model-episode-ceiling.\n",
                    )
                    return 2
                if beta is not None and turn_flags:
                    sys.stderr.write(
                        "error: --multi-model-beta cannot be combined with "
                        "--multi-model-step-start or --multi-model-step-end.\n",
                    )
                    return 2
                if turn_flags and schedule_flags:
                    sys.stderr.write(
                        "error: turn schedule flags cannot be combined with "
                        "K1/K2 schedule flags.\n",
                    )
                    return 2
                if mix_seed is not None and beta is None and not turn_flags:
                    sys.stderr.write(
                        "error: --multi-model-seed requires --multi-model-beta "
                        "or turn schedule (--multi-model-step-start/"
                        "--multi-model-step-end).\n",
                    )
                    return 2
                if beta is not None and not 0.0 <= float(beta) <= 1.0:
                    sys.stderr.write(
                        "error: --multi-model-beta must be in [0, 1].\n",
                    )
                    return 2
                if (step_start is None) ^ (step_end is None):
                    sys.stderr.write(
                        "error: --multi-model-step-start and "
                        "--multi-model-step-end must be set together.\n",
                    )
                    return 2
                if step_start is not None and step_end is not None:
                    if int(step_start) < 2 or int(step_end) <= int(step_start):
                        sys.stderr.write(
                            "error: require 2 <= step-start < step-end.\n",
                        )
                        return 2
                multi_model_block: dict[str, Any] = {
                    "enabled": True,
                    "secondary_model": _build_agent_model(
                        conn["type"],
                        args.multi_model_secondary,
                        agent_provider_override=args.agent_provider,
                    ),
                }
                if policy_flag == "student_to_teacher_turns" or turn_flags:
                    if step_start is None or step_end is None:
                        sys.stderr.write(
                            "error: student_to_teacher_turns requires "
                            "--multi-model-step-start and "
                            "--multi-model-step-end.\n",
                        )
                        return 2
                    multi_model_block["policy"] = "student_to_teacher_turns"
                    multi_model_block["step_start"] = int(step_start)
                    multi_model_block["step_end"] = int(step_end)
                    if mix_seed is not None:
                        multi_model_block["mix_seed"] = str(mix_seed)
                elif beta is not None:
                    multi_model_block["policy"] = "beta_mixture"
                    multi_model_block["beta"] = float(beta)
                    if mix_seed is not None:
                        multi_model_block["mix_seed"] = str(mix_seed)
                else:
                    multi_model_block["policy"] = "student_teacher_student"
                    if args.multi_model_switch_episode is not None:
                        multi_model_block["switch_episode"] = (
                            args.multi_model_switch_episode
                        )
                    if getattr(args, "multi_model_teacher_episodes", None) is not None:
                        multi_model_block["teacher_episodes"] = (
                            args.multi_model_teacher_episodes
                        )
                    if args.multi_model_episode_ceiling is not None:
                        multi_model_block["episode_ceiling"] = (
                            args.multi_model_episode_ceiling
                        )
                trial_config["multi_model"] = multi_model_block
            if getattr(args, "skip_verifier", False):
                if args.purpose != "trajectory_generation":
                    sys.stderr.write(
                        "error: --skip-verifier is only allowed with "
                        "--purpose trajectory_generation.\n",
                    )
                    return 2
                trial_config["skip_verifier"] = True
            if getattr(args, "verifier_env_mode", None):
                if trial_config.get("skip_verifier"):
                    sys.stderr.write(
                        "error: --verifier-env-mode cannot be combined with "
                        "--skip-verifier.\n",
                    )
                    return 2
                trial_config["verifier_env_mode"] = args.verifier_env_mode
            # --benchmark / --task-set are shortcuts for common task_filter
            # shapes. Operators wanting richer filters use --task-filter JSON
            # instead. Multiple selector forms are rejected so precedence stays
            # explicit.
            task_filter: dict[str, Any]
            selector_count = sum(
                1
                for selected in (
                    args.task_filter is not None,
                    args.benchmark is not None,
                    args.task_set is not None,
                )
                if selected
            )
            if selector_count > 1:
                sys.stderr.write(
                    "error: --benchmark, --task-set, and --task-filter are "
                    "mutually exclusive.\n",
                )
                return 2
            if args.task_filter is not None:
                task_filter = args.task_filter
            elif args.benchmark is not None:
                task_filter = {"benchmark_id": args.benchmark}
            elif args.task_set is not None:
                task_filter = {"task_set_id": args.task_set}
            else:
                sys.stderr.write(
                    "error: one of --benchmark, --task-set, or --task-filter "
                    "is required.\n",
                )
                return 2
            if args.purpose == "evaluation":
                if args.task_set is not None or (
                    isinstance(task_filter.get("task_set_id"), str)
                    and task_filter.get("task_set_id")
                ) or (
                    isinstance(task_filter.get("task_set_ids"), (list, tuple))
                    and any(
                        isinstance(item, str) and item
                        for item in task_filter.get("task_set_ids", [])
                    )
                ):
                    sys.stderr.write(
                        "error: --purpose evaluation only allows native "
                        "benchmarks (no TaskSet selectors).\n",
                    )
                    return 2
            payload: dict[str, Any] = {
                "purpose": args.purpose,
                "task_filter": task_filter,
                "trial_config": trial_config,
            }
            if args.name is not None:
                payload["name"] = args.name
            if args.name_suffix is not None:
                payload["name_suffix"] = args.name_suffix
            if args.team_id is not None:
                payload["team_id"] = args.team_id
            if conn is not None:
                payload["provider_connection_id"] = conn["id"]
                payload["provider_model_id"] = args.model
            if combinations is not None:
                payload["combinations"] = combinations
            if args.n_per_task is not None:
                payload["n_per_task"] = args.n_per_task
            if args.backend is not None:
                payload["backend"] = args.backend
            resource_requests = getattr(args, "task_resource_requests", None)
            if resource_requests is not None:
                payload["task_resource_requests"] = resource_requests
            if args.description is not None:
                payload["description"] = args.description
            resp = c.post("/api/v1/batches", json=payload)
        action_name = args.name if args.name is not None else "server-generated name"
        body = assert_2xx(resp, action=f"create batch {action_name!r}")
        if body.get("name") is None:
            body = {**body, "name": args.name or "(server-generated)"}
        print(f"Created batch {body['name']!r}:")
        _print_batch_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_list(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {}
        if args.state is not None:
            params["state"] = args.state
        if args.limit is not None:
            params["limit"] = args.limit
        for attr, param_name in (
            ("q", "q"),
            ("benchmark", "benchmark_id"),
            ("agent", "agent_name"),
            ("model_provider", "model_provider"),
            ("model", "model_name"),
            ("provider_connection_id", "provider_connection_id"),
            ("provider_model_id", "provider_model_id"),
        ):
            value = getattr(args, attr, None)
            if value is not None:
                params[param_name] = value
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/batches", params=params)
        body = assert_2xx(resp, action="list batches")
        items = body["items"]
        if args.format == "json":
            _dump_json(items)
            return 0
        if not items:
            print("(no batches — run `loom eval batch create`)")
            return 0
        for it in items:
            print(
                f"{it['id']:<38}  {it['state']:<10}  "
                f"n={it.get('expected_trial_count', '?'):<5}  "
                f"{it.get('name', '')}",
            )
        # Issue #67: surface the truncation. Server default is 50;
        # without this hint a team with >50 batches saw silent loss.
        # Stderr keeps the table on stdout pipeable while still
        # warning interactive users.
        if body.get("next_cursor"):
            sys.stderr.write(
                f"(more — {len(items)} shown; pass --limit N for more or filter with --state)\n",
            )
        return 0

    return _run_with_error_handling(_body)


def _batch_show(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/batches/{args.batch_id}")
        body = assert_2xx(resp, action=f"show batch {args.batch_id!r}")
        if args.format == "json":
            _dump_json(body)
        else:
            _print_batch_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_debug(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/batches/{args.batch_id}/debug")
        body = assert_2xx(
            resp,
            action=f"fetch batch debug evidence {args.batch_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_debug_evidence_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_rerun_plan(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: list[tuple[str, str | int | float | bool | None]] = []
        for task_id in args.task_id or []:
            params.append(("task_id", task_id))
        if args.include_operator_approval:
            params.append(("include_operator_approval", True))
        with authed_client(cfg) as c:
            resp = c.get(
                f"/api/v1/batches/{args.batch_id}/rerun-plan",
                params=params,
            )
        body = assert_2xx(
            resp,
            action=f"fetch batch rerun plan {args.batch_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        elif args.format == "task-ids":
            for task_id in body.get("supplemental_task_ids") or []:
                print(task_id)
        else:
            _print_rerun_plan(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_rerun_failed(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.post(
                f"/api/v1/batches/{args.batch_id}/rerun-failed",
                json={
                    "task_ids": args.task_id or [],
                    "include_operator_approval": args.include_operator_approval,
                    "use_current_runtime": args.use_current_runtime,
                },
            )
        body = assert_2xx(resp, action=f"rerun failed cases from batch {args.batch_id!r}")
        if args.format == "json":
            _dump_json(body)
        else:
            print(
                f"Created supplemental batch {body['batch_id']} "
                f"with {body['expected_trial_count']} trial(s)."
            )
        return 0

    return _run_with_error_handling(_body)


def _diagnose_batch(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/batches/{args.batch_id}/diagnosis")
        body = assert_2xx(
            resp,
            action=f"fetch batch diagnosis {args.batch_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_diagnosis_report(body)
        return 0

    return _run_with_error_handling(_body)


def _batch_cancel(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.post(f"/api/v1/batches/{args.batch_id}/cancel")
        body = assert_2xx(resp, action=f"cancel batch {args.batch_id!r}")
        print(
            f"Cancelled batch {body.get('batch_id', args.batch_id)} "
            f"(state={body.get('state', 'cancelled')}).",
        )
        print(
            "Note: this cancels the whole batch and still-active trials. "
            "To stop one hung trial, use `loom eval trial cancel <trial-id>`.",
        )
        return 0

    return _run_with_error_handling(_body)


def _batch_delivery_bundle(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        payload: dict[str, Any] = {}
        if args.mode != "lightweight":
            payload["mode"] = args.mode
        if args.supplemental_batch_id:
            payload["supplemental_batch_ids"] = list(args.supplemental_batch_id)
        if getattr(args, "trial_id", None):
            payload["selection"] = {"trial_ids": list(args.trial_id)}
        expected_sha = ""
        output: Path | None = None
        actual_hash = hashlib.sha256()
        with authed_client(cfg, timeout=args.timeout) as c:
            create_resp = c.post(
                f"/api/v1/batches/{args.batch_id}/delivery-export",
                json=payload,
            )
            body = assert_2xx(
                create_resp,
                action=f"create delivery bundle for batch {args.batch_id!r}",
            )
            download_url = body.get("download_url")
            if not isinstance(download_url, str) or not download_url:
                sys.stderr.write("error: server did not return a delivery download_url.\n")
                return 1
            expected_sha = str(body.get("sha256") or "")
            default_name = str(body.get("archive_filename") or f"{args.batch_id}-delivery.tar.gz")
            output = Path(args.output) if args.output else Path(default_name)
            output.parent.mkdir(parents=True, exist_ok=True)
            with c.stream("GET", download_url) as download_resp:
                assert_2xx_response(
                    download_resp,
                    action=f"download delivery bundle for batch {args.batch_id!r}",
                )
                with output.open("wb") as f:
                    for chunk in download_resp.iter_bytes():
                        if not chunk:
                            continue
                        actual_hash.update(chunk)
                        f.write(chunk)

        assert output is not None
        actual_sha = actual_hash.hexdigest()
        if expected_sha and actual_sha != expected_sha:
            sys.stderr.write(
                "error: downloaded delivery bundle checksum mismatch "
                f"(expected {expected_sha}, got {actual_sha}).\n"
            )
            return 1

        output.with_suffix(output.suffix + ".sha256").write_text(
            f"{actual_sha}  {output.name}\n",
            encoding="utf-8",
        )
        raw_manifest = body.get("manifest")
        manifest = raw_manifest if isinstance(raw_manifest, dict) else {}
        trial_count = manifest.get("trial_count") or manifest.get("task_count") or "unknown"
        object_counts = manifest.get("object_counts")
        object_text = ""
        if isinstance(object_counts, dict):
            object_text = (
                f", {object_counts.get('trajectory', 0)} trajectories, "
                f"{object_counts.get('atif', 0)} ATIF"
            )
        print(f"Delivery bundle ready: {trial_count} trials{object_text}")
        print(f"archive: {output}")
        print(f"sha256:  {actual_sha}")
        print(f"sidecar: {output.with_suffix(output.suffix + '.sha256')}")
        return 0

    return _run_with_error_handling(_body)


def _usage_cost_status(item: dict[str, Any]) -> str:
    status = item.get("cost_status")
    if isinstance(status, str) and status:
        return status
    if "estimated_cost_usd" in item or "total_cost_usd" in item:
        return "estimated"
    return "unknown"


def _format_usage_cost(item: dict[str, Any]) -> str:
    if "estimated_cost_usd" in item:
        value = item.get("estimated_cost_usd")
    else:
        value = item.get("total_cost_usd")
    if value is None:
        return "n/a"
    if isinstance(value, int | float):
        return f"{value:.6g}"
    return str(value)


def _usage_estimate_confidence(item: dict[str, Any]) -> str:
    confidence = item.get("usage_estimate_confidence")
    if isinstance(confidence, str) and confidence:
        return confidence
    status = item.get("usage_reporting_status")
    if isinstance(status, str) and status:
        return status
    return "-"


# ──────────────────────────────────────────────────────────────────────
# eval trial list / show
# ──────────────────────────────────────────────────────────────────────


def _trial_list(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {}
        if args.task_id is not None:
            params["task_id"] = args.task_id
        if args.state is not None:
            params["state"] = args.state
        if args.limit is not None:
            params["limit"] = args.limit
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/trials", params=params)
        body = assert_2xx(resp, action="list trials")
        items = body["items"]
        if args.format == "json":
            _dump_json(items)
            return 0
        if not items:
            print("(no trials)")
            return 0
        for it in items:
            reward = it.get("aggregate_reward")
            reward_s = f"{reward:.3f}" if isinstance(reward, (int, float)) else "-"
            print(
                f"{it['id']:<38}  {it['state']:<10}  reward={reward_s:<6}  {it.get('task_id', '')}",
            )
        # Issue #67: same truncation hint as batch list. Stderr keeps
        # stdout pipeable; interactive users still see the warning.
        if body.get("next_cursor"):
            sys.stderr.write(
                f"(more — {len(items)} shown; pass --limit N for more "
                "or filter with --state / --task-id)\n",
            )
        return 0

    return _run_with_error_handling(_body)


def _trial_show(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/trials/{args.trial_id}")
        body = assert_2xx(resp, action=f"show trial {args.trial_id!r}")
        if args.format == "json":
            _dump_json(body)
        else:
            _print_trial_summary(body)
            _print_trial_download_commands(body, args.trial_id)
        return 0

    return _run_with_error_handling(_body)


def _print_trial_cancel_next_steps(batch_id: str | None) -> None:
    print("Next steps:")
    print("  - Sibling trials in the batch are unaffected.")
    print(
        "  - The batch finishes when remaining trials are terminal "
        "(cancelled counts toward batch completion).",
    )
    print(
        "  - Delivery export needs an eligible completed attempt for this "
        "coordinate; cancelled does not select for export.",
    )
    print(
        "  - Native agent timeouts with completed verifier scores and complete "
        "evidence can also export, preserving their failed/timed_out metadata.",
    )
    if batch_id:
        print(
            f"  - Then: loom eval batch rerun-plan {shlex.quote(batch_id)}",
        )
    else:
        print("  - Then: loom eval batch rerun-plan <batch-id>")


def _trial_cancel(args: argparse.Namespace) -> int:
    """Cancel one trial without cascading to the parent batch (#1187)."""

    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            show_resp = c.get(f"/api/v1/trials/{args.trial_id}")
            trial = assert_2xx(
                show_resp,
                action=f"show trial {args.trial_id!r}",
            )
            previous = str(trial.get("state") or "(unknown)")
            batch_id = trial.get("batch_id")
            batch_id_s = batch_id if isinstance(batch_id, str) and batch_id else None
            if previous in _TERMINAL_TRIAL_STATES:
                print(
                    f"Trial {args.trial_id} already {previous}; "
                    "no cancel needed.",
                )
                return 0
            cancel_resp = c.post(f"/api/v1/trials/{args.trial_id}/cancel")
            body = assert_2xx(
                cancel_resp,
                action=f"cancel trial {args.trial_id!r}",
            )
        new_state = str(body.get("state") or "cancelled")
        print(
            f"Cancelled trial {body.get('trial_id', args.trial_id)} "
            f"({previous} → {new_state}).",
        )
        _print_trial_cancel_next_steps(batch_id_s)
        return 0

    return _run_with_error_handling(_body)


def _trial_debug(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/trials/{args.trial_id}/debug")
        body = assert_2xx(
            resp,
            action=f"fetch trial debug evidence {args.trial_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_debug_evidence_summary(body)
        return 0

    return _run_with_error_handling(_body)


def _diagnose_trial(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        with authed_client(cfg) as c:
            resp = c.get(f"/api/v1/trials/{args.trial_id}/diagnosis")
        body = assert_2xx(
            resp,
            action=f"fetch trial diagnosis {args.trial_id!r}",
        )
        if args.format == "json":
            _dump_json(body)
        else:
            _print_diagnosis_report(body)
        return 0

    return _run_with_error_handling(_body)


def _print_trial_download_commands(
    body: dict[str, Any],
    trial_id: str,
) -> None:
    commands: list[tuple[str, str]] = []
    materialization = body.get("materialization")
    if isinstance(materialization, dict) and materialization.get("canonical_ready") is True:
        commands.append(
            (
                "complete bundle",
                f"loom eval trial download {shlex.quote(trial_id)} --kind bundle",
            )
        )
    if body.get("atif_ready"):
        commands.append(
            (
                "atif",
                f"loom eval trial download {shlex.quote(trial_id)} --kind atif",
            )
        )
    if body.get("trajectory_ready"):
        commands.append(
            (
                "trajectory",
                f"loom eval trial download {shlex.quote(trial_id)} --kind trajectory",
            )
        )
    artifacts = body.get("artifacts")
    if isinstance(artifacts, list):
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            key = artifact.get("key")
            if not isinstance(key, str) or not key:
                continue
            label = str(artifact.get("path") or key)
            commands.append(
                (
                    f"artifact {label}",
                    f"loom eval trial download {shlex.quote(trial_id)} "
                    f"--kind artifact --artifact-key {shlex.quote(key)}",
                )
            )
    if not commands:
        return
    print("downloads:")
    for label, command in commands:
        print(f"  {label:<16} {command}")


def _trial_download(args: argparse.Namespace) -> int:
    def _body() -> int:
        if args.kind == "artifact" and not args.artifact_key:
            sys.stderr.write(
                "error: --kind artifact requires --artifact-key.\n",
            )
            return 2
        if args.kind != "artifact" and args.artifact_key:
            sys.stderr.write(
                "error: --artifact-key is only valid with --kind artifact.\n",
            )
            return 2

        cfg = require_logged_in()
        params: dict[str, Any] | None = None
        if args.kind == "atif":
            path = f"/api/v1/trials/{args.trial_id}/atif"
            default_name = f"{args.trial_id}-atif.json"
        elif args.kind == "trajectory":
            path = f"/api/v1/trials/{args.trial_id}/trajectory/download"
            default_name = f"{args.trial_id}-events.jsonl"
        elif args.kind == "bundle":
            path = f"/api/v1/trials/{args.trial_id}/bundle/download"
            default_name = f"{args.trial_id}-complete-trial-bundle.tar.gz"
        else:
            path = f"/api/v1/trials/{args.trial_id}/artifacts/download"
            params = {"key": args.artifact_key}
            default_name = Path(args.artifact_key).name or (f"{args.trial_id}-artifact")

        output = Path(args.output) if args.output else Path(default_name)
        with authed_client(cfg) as c:
            resp = c.get(path, params=params)
        assert_2xx_response(
            resp,
            action=f"download {args.kind} for trial {args.trial_id!r}",
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(resp.content)
        print(f"Downloaded {args.kind} to {output}")
        return 0

    return _run_with_error_handling(_body)


def _artifact_export(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {
            "scope": args.scope,
            "format": args.format,
        }
        for attr, param_name in (
            ("artifact_type", "artifact_type"),
            ("owner_team_id", "owner_team_id"),
            ("source_batch_id", "source_batch_id"),
            ("source_trial_id", "source_trial_id"),
            ("safety_state", "safety_state"),
            ("provenance_relation", "provenance_relation"),
            ("limit", "limit"),
        ):
            value = getattr(args, attr, None)
            if value is not None:
                params[param_name] = value
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/run-library/artifacts/export", params=params)
        assert_2xx_response(resp, action="export Run Library artifacts")

        default_name = (
            "run-library-artifacts.json" if args.format == "json" else "run-library-artifacts.jsonl"
        )
        output = Path(args.output) if args.output else Path(default_name)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(resp.content)
        print(f"Exported artifact metadata to {output}")
        return 0

    return _run_with_error_handling(_body)


def _usage(args: argparse.Namespace) -> int:
    def _body() -> int:
        cfg = require_logged_in()
        params: dict[str, Any] = {
            "start": args.start,
            "end": args.end,
            "group_by": args.group_by,
        }
        if args.team_id is not None:
            params["team_id"] = args.team_id
        if args.include_batches:
            params["include_batches"] = "true"
        if args.include_batch_family:
            params["include_batch_family"] = "true"
        for attr, param_name in (
            ("user_id", "user_id"),
            ("provider_connection_id", "provider_connection_id"),
            ("model", "model"),
            ("benchmark_id", "benchmark_id"),
            ("batch_id", "batch_id"),
            ("status", "status"),
            ("pricing_mode", "pricing_mode"),
            ("breakdown_by", "breakdown_by"),
        ):
            value = getattr(args, attr, None)
            if value is not None:
                params[param_name] = value
        with authed_client(cfg) as c:
            resp = c.get("/api/v1/usage", params=params)
        body = assert_2xx(resp, action="fetch usage")
        if args.format == "json":
            _dump_json(body)
            return 0
        buckets = body.get("buckets") or []
        if body.get("degraded"):
            print("(usage unavailable: server returned degraded=true)")
            return 0
        if not buckets:
            print("(no usage in range)")
            return 0
        print(
            "start_at                      trials  success  failed  "
            "breakdown                 cost_status      usage_conf  cost_usd  in_tok  out_tok",
        )
        for bucket in buckets:
            breakdown = ""
            if bucket.get("breakdown_by"):
                breakdown = (
                    f"{bucket.get('breakdown_by')}="
                    f"{bucket.get('breakdown_label') or bucket.get('breakdown_key')}"
                )
            print(
                f"{bucket.get('start_at', '')!s:<29} "
                f"{bucket.get('trial_count', 0):>6} "
                f"{bucket.get('succeeded_count', 0):>8} "
                f"{bucket.get('failed_count', 0):>7} "
                f"{breakdown:<25} "
                f"{_usage_cost_status(bucket):<15} "
                f"{_usage_estimate_confidence(bucket):<10} "
                f"{_format_usage_cost(bucket):>8} "
                f"{bucket.get('llm_input_tokens', 0):>7} "
                f"{bucket.get('llm_output_tokens', 0):>8}",
            )
            batches = bucket.get("batches")
            if args.include_batches and isinstance(batches, list) and batches:
                print(
                    "  batch_id                              trials  "
                    "cost_status      usage_conf  cost_usd  in_tok  out_tok  name",
                )
                for batch in batches:
                    if not isinstance(batch, dict):
                        continue
                    print(
                        f"  {batch.get('batch_id', '')!s:<36} "
                        f"{batch.get('trial_count', 0):>6} "
                        f"{_usage_cost_status(batch):<15} "
                        f"{_usage_estimate_confidence(batch):<10} "
                        f"{_format_usage_cost(batch):>8} "
                        f"{batch.get('llm_input_tokens', 0):>7} "
                        f"{batch.get('llm_output_tokens', 0):>8} "
                        f"{batch.get('batch_name', '')}",
                    )
        return 0

    return _run_with_error_handling(_body)


# ──────────────────────────────────────────────────────────────────────
# Dispatch
# ──────────────────────────────────────────────────────────────────────


def dispatch(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="loom eval",
        description=(
            "Submit + inspect evaluations on the deployed Loom server. "
            "Requires `loom auth login` first. See `loom run` for "
            "running a single trial locally without a server."
        ),
    )
    sub = parser.add_subparsers(dest="eval_cmd", required=True)

    configure_nebius_acceptance_parser(sub)

    # --- run (single trial) ---
    p_run = sub.add_parser(
        "run",
        help="Submit a single trial against a task on the deployed Loom.",
    )
    p_run.add_argument(
        "--provider",
        required=True,
        help="Provider connection name (`loom providers list`).",
    )
    p_run.add_argument(
        "--model",
        required=True,
        help="Upstream model id the gateway forwards to.",
    )
    p_run.add_argument(
        "--agent",
        required=True,
        help="Agent name from the catalog (`GET /api/v1/agents`).",
    )
    p_run.add_argument(
        "--task",
        required=True,
        help="Task id (e.g. `humaneval/HumanEval/0`).",
    )
    p_run.add_argument(
        "--agent-provider",
        dest="agent_provider",
        default=None,
        help=(
            "Override the agent_model.provider derived from the "
            "connection type. Use this for openai-compatible "
            "connections to Together / Fireworks / Mistral / etc. "
            "(e.g. --agent-provider together). When unset, the "
            "connection's type maps to a default (openai-compatible "
            "→ openai, anthropic → anthropic, google → google, "
            "custom → openai)."
        ),
    )
    p_run.set_defaults(handler=_run)

    # --- batch ---
    p_batch = sub.add_parser(
        "batch",
        help="Manage batches (create/list/show/cancel).",
    )
    batch_sub = p_batch.add_subparsers(dest="batch_cmd", required=True)

    p_bc = batch_sub.add_parser(
        "create",
        help="Create a batch + materialize trials.",
    )
    p_bc.add_argument(
        "--provider",
        default=None,
        help=(
            "Provider connection name. Required for agents that call a "
            "model; omit for no-model agents such as oracle."
        ),
    )
    p_bc.add_argument(
        "--model",
        default=None,
        help=(
            "Upstream model id. Required for agents that call a model; "
            "omit for no-model agents such as oracle."
        ),
    )
    p_bc.add_argument(
        "--agent",
        default=None,
        help=(
            "Agent name for single-combination creation; omit when using "
            "--combinations-json."
        ),
    )
    p_bc.add_argument("--agent-version", default=None, help="Published native Terminus runtime version; omit for deployment default.")
    p_bc.add_argument(
        "--combinations-json",
        dest="combinations_json",
        type=_load_combinations_json,
        default=None,
        help=(
            "Combination matrix as JSON array or @path. Each object uses the "
            "API Combination shape: agent_name, agent_model, n_per_task, "
            "optional label, provider_connection_id, and provider_model_id."
        ),
    )
    request_source = p_bc.add_mutually_exclusive_group(required=True)
    request_source.add_argument(
        "--request-json",
        type=lambda raw: _load_task_filter_json(raw, flag="--request-json"),
        default=None,
        help=(
            "Complete POST /api/v1/batches request as JSON or @file.json. "
            "Submitted unchanged with normal authentication and server validation; "
            "cannot be combined with other batch creation options."
        ),
    )
    request_source.add_argument(
        "--purpose",
        choices=("evaluation", "trajectory_generation"),
        help=(
            "Batch purpose. evaluation = native benchmarks with verification; "
            "trajectory_generation = TaskSets and/or benchmarks (transition), "
            "verifier optional. Same trial harness either way."
        ),
    )
    p_bc.add_argument(
        "--skip-verifier",
        dest="skip_verifier",
        action="store_true",
        help=(
            "Skip the verifier phase (trajectory_generation only). "
            "Rejected with --purpose evaluation."
        ),
    )
    p_bc.add_argument(
        "--verifier-env-mode",
        dest="verifier_env_mode",
        choices=("shared", "separate"),
        default=None,
        help=(
            "Grade in the agent sandbox (shared) or the second verifier "
            "sandbox (separate). Omit to use the task file, which defaults "
            "to separate. Cannot be combined with --skip-verifier."
        ),
    )
    p_bc.add_argument(
        "--benchmark",
        default=None,
        help=(
            "Native benchmark slug — shortcut for "
            '--task-filter \'{"benchmark_id":"..."}\'. Required selector form '
            "for --purpose evaluation."
        ),
    )
    p_bc.add_argument(
        "--task-set",
        dest="task_set",
        default=None,
        help=(
            "TaskSet id — shortcut for --task-filter "
            '\'{"task_set_id":"..."}\'. Use `loom tasksets list` to find ids. '
            "Allowed for --purpose trajectory_generation only."
        ),
    )
    p_bc.add_argument(
        "--task-filter",
        dest="task_filter",
        type=_load_task_filter_json,
        default=None,
        help=(
            "Task filter as JSON (object). Pass a literal JSON string "
            "or `@path/to/file.json` to read from disk."
        ),
    )
    p_bc.add_argument(
        "--name",
        default=None,
        help="Optional batch display name. When omitted, the server generates one.",
    )
    p_bc.add_argument(
        "--name-suffix",
        default=None,
        help="Optional suffix appended to the server-generated display name.",
    )
    p_bc.add_argument(
        "--team-id",
        default=None,
        help=(
            "Target team UUID for admin/platform-admin on-behalf-of "
            "batch creation. Also scopes provider-name lookup."
        ),
    )
    p_bc.add_argument("--description", default=None)
    p_bc.add_argument(
        "--n-per-task",
        dest="n_per_task",
        type=int,
        default=None,
        help="Number of trials per task (1–100).",
    )
    add_legacy_backend_flag(p_bc)
    p_bc.add_argument(
        "--task-resource-requests",
        type=_load_task_resource_requests,
        default=None,
        help=(
            "Native Terminus scheduling requests keyed by task ID, as JSON or @file.json. "
            "Each entry binds the existing task revision; task hard limits stay unchanged."
        ),
    )
    p_bc.add_argument(
        "--storage-preflight-evidence",
        default=None,
        help=(
            "MinIO storage preflight JSON from "
            "`loom cluster minio-storage-preflight --output`. If the artifact "
            "reports stop, batch creation is refused unless "
            "--override-storage-preflight-stop is also supplied."
        ),
    )
    p_bc.add_argument(
        "--override-storage-preflight-stop",
        action="store_true",
        help=(
            "Explicit operator override for a stopped MinIO storage preflight. "
            "Use only after accepting the object-store capacity risk."
        ),
    )
    p_bc.add_argument(
        "--agent-provider",
        dest="agent_provider",
        default=None,
        help="See `loom eval run --agent-provider`.",
    )
    p_bc.add_argument(
        "--workspace-staging-policy",
        dest="workspace_staging_policy",
        choices=["tb21", "none"],
        default=None,
        help=(
            "Named agent-workspace staging policy for every trial in the batch "
            "`tb21` excludes solution/, tests/, verifier/, and "
            "upstream-task.toml from the agent sandbox (Harbor-shaped packs). "
            "`none` uses full-bundle upload. Omit to keep current "
            "defaults (TB2.1-r6 tasks always enforce tb21). "
            "terminal-bench-2@tb2.1-r6/* cannot select `none`."
        ),
    )
    p_bc.add_argument(
        "--multi-model",
        dest="multi_model",
        action="store_true",
        default=False,
        help=(
            "Enable terminus-2 student/teacher model mix (#1380). "
            "Default is off (single-model trial). Requires "
            "--agent terminus-2, --provider/--model (student), and "
            "--multi-model-secondary (teacher). Same BYO provider connection. "
            "Default policy is student until K1, teacher for teacher_episodes, "
            "then student from K2. Pass --multi-model-beta for a per-episode "
            "teacher coin, or --multi-model-step-start/--multi-model-step-end "
            "for turn-grain rising beta with latch."
        ),
    )
    p_bc.add_argument(
        "--multi-model-secondary",
        dest="multi_model_secondary",
        default=None,
        help=(
            "Teacher (usually larger) upstream model id. Student is --model. "
            "Required with --multi-model. Same provider connection."
        ),
    )
    p_bc.add_argument(
        "--multi-model-policy",
        dest="multi_model_policy",
        choices=(
            "student_teacher_student",
            "beta_mixture",
            "student_to_teacher_turns",
        ),
        default=None,
        help=(
            "Optional explicit mix policy. Usually inferred from "
            "--multi-model-beta or --multi-model-step-start/--step-end."
        ),
    )
    p_bc.add_argument(
        "--multi-model-switch-episode",
        dest="multi_model_switch_episode",
        type=int,
        default=None,
        help=(
            "Harbor episode index (1-based, >= 2) where the teacher takes "
            "over (K1). Omit to let the server sample once into "
            "[2, --multi-model-episode-ceiling]."
        ),
    )
    p_bc.add_argument(
        "--multi-model-episode-ceiling",
        dest="multi_model_episode_ceiling",
        type=int,
        default=None,
        help=(
            "Upper bound for random K1 sampling when "
            "--multi-model-switch-episode is omitted (default on server: 50)."
        ),
    )
    p_bc.add_argument(
        "--multi-model-teacher-episodes",
        dest="multi_model_teacher_episodes",
        type=int,
        default=None,
        help=(
            "How many Harbor episodes the teacher keeps (default 2). "
            "K2 = K1 + this value; student then continues to the end. "
            "Incompatible with --multi-model-beta."
        ),
    )
    p_bc.add_argument(
        "--multi-model-beta",
        dest="multi_model_beta",
        type=float,
        default=None,
        help=(
            "Per-episode mix: P(teacher drives) = this value in [0, 1]. "
            "Teacher if hash(seed, trial, episode) < beta, else student. "
            "Incompatible with K1/K2 and turn-schedule flags. Not DAgger labels."
        ),
    )
    p_bc.add_argument(
        "--multi-model-step-start",
        dest="multi_model_step_start",
        type=int,
        default=None,
        help=(
            "Turn-grain schedule: first call_ordinal (>= 2) where rising beta "
            "may select teacher. Requires --multi-model-step-end."
        ),
    )
    p_bc.add_argument(
        "--multi-model-step-end",
        dest="multi_model_step_end",
        type=int,
        default=None,
        help=(
            "Turn-grain schedule: call_ordinal where teacher is forced "
            "(must be > step-start). One-way latch after first teacher."
        ),
    )
    p_bc.add_argument(
        "--multi-model-seed",
        dest="multi_model_seed",
        default=None,
        help=(
            "Optional mix seed stored on the plan (beta_mixture or "
            "student_to_teacher_turns). Omit to let the server generate one. "
            "Replays inherit this seed."
        ),
    )
    # Future creation options must also fail closed with a complete request.
    p_bc.set_defaults(
        handler=_batch_create,
        batch_create_defaults={
            action.dest: action.default
            for action in p_bc._actions
            if action.dest != "help"
        },
    )

    p_bl = batch_sub.add_parser("list", help="List batches.")
    p_bl.add_argument(
        "--state",
        default=None,
        help="Comma-separated state filter (e.g. submitted,running).",
    )
    p_bl.add_argument("--q", default=None, help="Search batch name, description, or ID.")
    p_bl.add_argument("--benchmark", default=None, help="Filter by benchmark id.")
    p_bl.add_argument("--agent", default=None, help="Filter by agent name.")
    p_bl.add_argument("--model-provider", dest="model_provider", default=None)
    p_bl.add_argument("--model", default=None, help="Filter by model name.")
    p_bl.add_argument("--provider-connection-id", default=None)
    p_bl.add_argument("--provider-model-id", default=None)
    p_bl.add_argument("--limit", type=int, default=None)
    p_bl.add_argument("--format", choices=["table", "json"], default="table")
    p_bl.set_defaults(handler=_batch_list)

    p_bs = batch_sub.add_parser("show", help="Show batch details + rollup.")
    p_bs.add_argument("batch_id", help="Batch UUID.")
    p_bs.add_argument("--format", choices=["text", "json"], default="text")
    p_bs.set_defaults(handler=_batch_show)

    p_bd = batch_sub.add_parser(
        "debug",
        help="Fetch structured debug evidence for a batch.",
    )
    p_bd.add_argument("batch_id", help="Batch UUID.")
    p_bd.add_argument("--format", choices=["text", "json"], default="text")
    p_bd.set_defaults(handler=_batch_debug)

    p_brp = batch_sub.add_parser(
        "rerun-plan",
        help="Generate a deterministic safe supplemental rerun plan.",
    )
    p_brp.add_argument("batch_id", help="Source batch UUID.")
    p_brp.add_argument(
        "--task-id",
        action="append",
        default=[],
        help="Limit the plan to this task id. Repeat for an explicit task list.",
    )
    p_brp.add_argument(
        "--include-operator-approval",
        action="store_true",
        help=(
            "Include operator-approval rows in the supplemental task-id list. "
            "Auto-safe rows are always included."
        ),
    )
    p_brp.add_argument(
        "--format",
        choices=["text", "json", "task-ids"],
        default="text",
    )
    p_brp.set_defaults(handler=_batch_rerun_plan)

    p_brf = batch_sub.add_parser(
        "rerun-failed", help="Submit a linked batch for the selected failed cases.",
    )
    p_brf.add_argument("batch_id", help="Source batch UUID.")
    p_brf.add_argument("--task-id", action="append", default=[],
                       help="Rerun only this task id. Repeat to select multiple tasks.")
    p_brf.add_argument("--include-operator-approval", action="store_true",
                       help="Include operator-approval rows from the existing rerun plan.")
    p_brf.add_argument(
        "--use-current-runtime", action="store_true",
        help=("Use the current deployed native Terminus runtime; "
              "default replays the frozen runtime."),
    )
    p_brf.add_argument("--format", choices=["text", "json"], default="text")
    p_brf.set_defaults(handler=_batch_rerun_failed)

    p_bx = batch_sub.add_parser(
        "cancel",
        help=(
            "Cancel a whole batch and cascade-cancel its still-active trials. "
            "For one hung trial, use `loom eval trial cancel` instead."
        ),
    )
    p_bx.add_argument("batch_id", help="Batch UUID.")
    p_bx.set_defaults(handler=_batch_cancel)

    p_bdel = batch_sub.add_parser(
        "delivery-bundle",
        help="Create and download a delivery bundle for a finished batch family.",
    )
    p_bdel.add_argument("batch_id", help="Main batch UUID.")
    p_bdel.add_argument(
        "--supplemental-batch-id",
        action="append",
        default=[],
        help=(
            "Supplemental rerun batch UUID in selection priority order. "
            "Repeat for primary failed-case and targeted reruns. When omitted, "
            "the service uses linked rerun descendants by created_at/id order."
        ),
    )
    p_bdel.add_argument(
        "--trial-id",
        action="append",
        default=[],
        help=(
            "Export only this trial UUID from the authorized batch family. "
            "Repeat to select multiple trials. When omitted, every main "
            "task/sample/combination coordinate must resolve (default)."
        ),
    )
    p_bdel.add_argument(
        "--output",
        default=None,
        help="Destination archive path. Defaults to the server archive filename.",
    )
    p_bdel.add_argument(
        "--mode",
        choices=[
            "lightweight",
            "raw-harbor",
            "raw-harbor-tb2-v1",
            "raw-harbor-tb2-v2",
            "openhands-export",
        ],
        default="lightweight",
        help=(
            "Export mode. lightweight emits the standard delivery bundle; "
            "raw-harbor adds raw provider logs, task bundles, agent-run "
            "artifacts, and derived SFT JSONL; raw-harbor-tb2-v1 adds the "
            "versioned TB2-facing delivery profile; raw-harbor-tb2-v2 "
            "projects execution from typed terminus2 events and native Harbor "
            "artifacts; openhands-export projects execution from native "
            "OpenHands SDK events and provider logs."
        ),
    )
    p_bdel.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout in seconds for create/download operations.",
    )
    p_bdel.set_defaults(handler=_batch_delivery_bundle)

    # --- trial ---
    p_trial = sub.add_parser(
        "trial",
        help="Inspect or cancel trials (list/show/debug/download/cancel).",
    )
    trial_sub = p_trial.add_subparsers(dest="trial_cmd", required=True)

    p_tl = trial_sub.add_parser("list", help="List trials.")
    p_tl.add_argument(
        "--task-id",
        dest="task_id",
        default=None,
        help="Filter to a specific task id.",
    )
    p_tl.add_argument(
        "--state",
        default=None,
        help="Comma-separated state filter.",
    )
    p_tl.add_argument("--limit", type=int, default=None)
    p_tl.add_argument("--format", choices=["table", "json"], default="table")
    p_tl.set_defaults(handler=_trial_list)

    p_ts = trial_sub.add_parser(
        "show",
        help="Show trial details + copyable download commands.",
    )
    p_ts.add_argument("trial_id", help="Trial UUID.")
    p_ts.add_argument("--format", choices=["text", "json"], default="text")
    p_ts.set_defaults(handler=_trial_show)

    p_tdebug = trial_sub.add_parser(
        "debug",
        help="Fetch structured debug evidence for a trial.",
    )
    p_tdebug.add_argument("trial_id", help="Trial UUID.")
    p_tdebug.add_argument("--format", choices=["text", "json"], default="text")
    p_tdebug.set_defaults(handler=_trial_debug)

    p_tc = trial_sub.add_parser(
        "cancel",
        help=(
            "Cancel one trial (queued/claimed/running). Does not cancel the "
            "parent batch or sibling trials. For whole-batch cancel, use "
            "`loom eval batch cancel`."
        ),
    )
    p_tc.add_argument("trial_id", help="Trial UUID.")
    p_tc.set_defaults(handler=_trial_cancel)

    p_td = trial_sub.add_parser(
        "download",
        help="Download a complete Trial bundle, ATIF, trajectory, or artifact through the public API.",
    )
    p_td.add_argument("trial_id", help="Trial UUID.")
    p_td.add_argument(
        "--kind",
        choices=["bundle", "atif", "trajectory", "artifact"],
        required=True,
        help="Object to download.",
    )
    p_td.add_argument(
        "--artifact-key",
        default=None,
        help="Artifact key from `loom eval trial show`; required for --kind artifact.",
    )
    p_td.add_argument(
        "--output",
        default=None,
        help="Destination path. Defaults to a filename derived from the trial/key.",
    )
    p_td.set_defaults(handler=_trial_download)

    # --- artifact ---
    p_artifact = sub.add_parser(
        "artifact",
        help="Browse and export typed Run Library artifacts.",
    )
    artifact_sub = p_artifact.add_subparsers(
        dest="artifact_cmd",
        required=True,
    )

    p_artifact_export = artifact_sub.add_parser(
        "export",
        help="Export safe typed Run Library artifact metadata.",
    )
    p_artifact_export.add_argument(
        "--scope",
        choices=["my", "all"],
        default="my",
        help="Run Library scope to export.",
    )
    p_artifact_export.add_argument("--artifact-type", default=None)
    p_artifact_export.add_argument("--owner-team-id", default=None)
    p_artifact_export.add_argument("--source-batch-id", default=None)
    p_artifact_export.add_argument("--source-trial-id", default=None)
    p_artifact_export.add_argument("--safety-state", default=None)
    p_artifact_export.add_argument("--provenance-relation", default=None)
    p_artifact_export.add_argument(
        "--format",
        choices=["jsonl", "json"],
        default="jsonl",
    )
    p_artifact_export.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum artifact records to export.",
    )
    p_artifact_export.add_argument(
        "--output",
        default=None,
        help="Destination path. Defaults to run-library-artifacts.jsonl.",
    )
    p_artifact_export.set_defaults(handler=_artifact_export)

    # --- diagnose ---
    p_diag = sub.add_parser(
        "diagnose",
        help="Fetch human-readable diagnosis reports for batches or trials.",
    )
    diag_sub = p_diag.add_subparsers(dest="diagnose_cmd", required=True)

    p_diag_batch = diag_sub.add_parser(
        "batch",
        help="Diagnose a batch failure summary and reason clusters.",
    )
    p_diag_batch.add_argument("batch_id", help="Batch UUID.")
    p_diag_batch.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
    )
    p_diag_batch.set_defaults(handler=_diagnose_batch)

    p_diag_trial = diag_sub.add_parser(
        "trial",
        help="Diagnose a trial failure or terminal outcome.",
    )
    p_diag_trial.add_argument("trial_id", help="Trial UUID.")
    p_diag_trial.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
    )
    p_diag_trial.set_defaults(handler=_diagnose_trial)

    p_usage = sub.add_parser(
        "usage",
        help="Show usage and cost rollups from the public API.",
    )
    p_usage.add_argument("--start", required=True, help="Start date YYYY-MM-DD.")
    p_usage.add_argument("--end", required=True, help="End date YYYY-MM-DD.")
    p_usage.add_argument(
        "--group-by",
        choices=["day", "week", "month"],
        default="day",
    )
    p_usage.add_argument("--team-id", default=None)
    p_usage.add_argument("--user-id", default=None)
    p_usage.add_argument("--provider-connection-id", default=None)
    p_usage.add_argument("--model", default=None)
    p_usage.add_argument("--benchmark-id", default=None)
    p_usage.add_argument("--batch-id", default=None)
    p_usage.add_argument("--status", default=None)
    p_usage.add_argument(
        "--pricing-mode",
        choices=["priced", "tokens-only", "price-unknown", "failed-upstream"],
        default=None,
    )
    p_usage.add_argument(
        "--breakdown-by",
        choices=[
            "team",
            "user",
            "provider_connection",
            "model",
            "benchmark",
            "batch",
            "status",
            "pricing_mode",
        ],
        default=None,
    )
    p_usage.add_argument(
        "--include-batches",
        action="store_true",
        help="Include per-batch usage/cost rows when the API permits it.",
    )
    p_usage.add_argument(
        "--include-batch-family",
        action="store_true",
        help=(
            "When --batch-id is set, include linked supplemental rerun batches "
            "in the usage/cost query."
        ),
    )
    p_usage.add_argument("--format", choices=["table", "json"], default="table")
    p_usage.set_defaults(handler=_usage)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
