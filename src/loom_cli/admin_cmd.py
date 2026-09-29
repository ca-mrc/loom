"""`loom admin` — operator-only admin operations.

Subcommands:

- ``loom admin tokens worker {mint,revoke,rotate}`` — worker-token
  rotation via the Control Plane's admin surface.
- ``loom admin tokens team {mint,revoke,rotate}`` — legacy team-token
  rotation via loom_service's /api/v1/tokens route.
- ``loom admin env-diagnostics`` — redacted runtime environment inspection
  for deploy/debug evidence without raw secret values.
- ``loom admin rate-cards sync-yibuapi`` — sync the official YibuAPI
  pricing catalog into the service rate-card store.
- ``loom admin secret-store rewrap`` — master-key rotation walker;
  re-encrypts all SecretStore rows with the primary key configured
  in ``LOOM_SECRET_STORE_MASTER_KEYS``.

The CP admin surface is NOT exposed via Ingress; reach it through a
port-forward (``kubectl port-forward deploy/loom-control-plane 8080:8080``).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, cast

import httpx

from loom.security.redaction import RedactedEnvironmentEntry, redact_environment_mapping
from loom_cli.backend_flag import add_legacy_backend_flag, warn_legacy_backend_flag
from loom_cli.secret_source import (
    SecretSourceError,
)
from loom_cli.server_client import (
    HttpStatusError,
    NotLoggedInError,
    assert_2xx,
    authed_client,
    require_logged_in,
)

# Same constraint as the CP route: prefix must be hex, 4-64 chars.
# Catching this client-side avoids a round-trip just to hit the 400.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")

_DEFAULT_CP_URL = "http://localhost:8080"
_DEFAULT_EXPIRES_DAYS = 365
_DEFAULT_ADMIN_TOKEN_SOURCE = "env:LOOM_ADMIN_TOKEN"
_DEFAULT_ENV_DIAGNOSTIC_PREFIX = "LOOM_"




def _env_diagnostic_value(entry: RedactedEnvironmentEntry) -> str:
    if not entry.sensitive:
        return entry.value
    if entry.fingerprint is None:
        return "[REDACTED]"
    length = entry.length if entry.length is not None else 0
    return f"[REDACTED {entry.fingerprint} len={length}]"


def _env_diagnostics(args: argparse.Namespace) -> int:
    prefixes = tuple(args.prefix or (_DEFAULT_ENV_DIAGNOSTIC_PREFIX,))
    entries = redact_environment_mapping(os.environ, prefixes=prefixes)
    if args.format == "json":
        json.dump(
            {
                "prefixes": list(prefixes),
                "entries": [entry.to_json() for entry in entries],
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return 0
    if args.format == "markdown":
        sys.stdout.write("| name | kind | value |\n")
        sys.stdout.write("| --- | --- | --- |\n")
        for entry in entries:
            kind = "sensitive" if entry.sensitive else "value"
            sys.stdout.write(f"| {entry.name} | {kind} | {_env_diagnostic_value(entry)} |\n")
        return 0
    for entry in entries:
        sys.stdout.write(f"{entry.name}={_env_diagnostic_value(entry)}\n")
    return 0


















def _resolve_admin_token(source: str) -> str:
    """Resolve an `env:VAR` / `file:PATH` / `-` source to a raw token.

    Mirrors `loom_cli.secret_source` but accepts only what makes sense
    for the admin-token use case (token cycles long-lived, not piped
    repeatedly), and reports errors with the `--admin-token` flag
    name.
    """
    if source == "-":
        return sys.stdin.read().strip()
    if source.startswith("env:"):
        var = source[len("env:") :]
        if not var:
            raise ValueError("--admin-token env:VAR — VAR cannot be empty")
        try:
            return os.environ[var]
        except KeyError:
            raise ValueError(
                f"--admin-token env:{var} — environment variable not set",
            ) from None
    if source.startswith("file:"):
        path = source[len("file:") :]
        if not path:
            raise ValueError("--admin-token file:PATH — PATH cannot be empty")
        try:
            with open(path, encoding="utf-8") as f:
                return f.read().strip()
        except OSError as e:
            raise ValueError(f"--admin-token file:{path} — {e}") from None
    raise ValueError(
        f"--admin-token must be one of: env:VAR, file:PATH, '-' (stdin). Got {source!r}",
    )






def _mint_worker_token(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    kind = getattr(args, "kind", "trial")
    endpoint = {
        "task-image-builder": "task-image-builder-tokens",
        "task-image-registry-gc": "task-image-registry-gc-tokens",
        "execution-capacity-collector": "execution-capacity-collector-tokens",
    }.get(kind, "worker-tokens")
    url = f"{args.cp_url.rstrip('/')}/admin/{endpoint}"
    body: dict[str, int] = {}
    if args.expires_in_days is not None and args.expires_in_days > 0:
        body["expires_in_days"] = args.expires_in_days

    try:
        resp = httpx.post(
            url,
            json=body,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach CP at {url}: {e}\n"
            f"hint: port-forward the Control Plane "
            f"(kubectl port-forward deploy/loom-control-plane 8080:8080)\n",
        )
        return 2

    if resp.status_code != 201:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1

    data = resp.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    elif args.show_secret:
        if kind == "task-image-builder":
            label = "task-image builder"
            next_step = (
                "write it as LOOM_WORKER_TOKEN in the dedicated builder env file; "
                "do not install it on trial workers"
            )
        elif kind == "task-image-registry-gc":
            label = "task-image registry GC"
            next_step = (
                "install it only in the registry-retention controller; "
                "do not install it on builders or trial workers"
            )
        elif kind == "execution-capacity-collector":
            label = "execution capacity collector"
            next_step = (
                "install it only as the collector control-plane token; "
                "do not install it on actuators or trial workers"
            )
        else:
            label = "worker"
            next_step = (
                "update the `worker-token` key in `loom-secrets` and restart `deploy/loom-worker`"
            )
        sys.stdout.write(
            f"New {label} token minted.\n"
            f"  prefix: {data['token_hash_prefix']}\n"
            f"  token:  {data['token']}\n"
            f"\nNext: {next_step}.\n",
        )
    else:
        if kind == "task-image-builder":
            install_hint = (
                "  loom admin tokens worker mint --kind task-image-builder "
                "--format json | jq -r '\"LOOM_WORKER_TOKEN=\" + .token' \\\n"
                "    > /secure/staging-task-image-builder.env\n"
            )
        elif kind == "task-image-registry-gc":
            install_hint = (
                "  loom admin tokens worker mint --kind task-image-registry-gc "
                "--format json | jq -r .token \\\n"
                "    > /secure/staging-task-image-registry-gc.token\n"
            )
        elif kind == "execution-capacity-collector":
            install_hint = (
                "  loom admin tokens worker mint --kind execution-capacity-collector "
                "--format json | jq -r .token \\\n"
                "    > /secure/staging-execution-capacity-collector.token\n"
            )
        else:
            install_hint = (
                "  loom admin tokens worker mint --format json | jq -r .token \\\n"
                "    | kubectl create secret generic loom-secrets \\\n"
                "        --from-file=worker-token=/dev/stdin \\\n"
                "        --dry-run=client -o yaml | kubectl apply -f -\n"
            )
        sys.stdout.write(
            f"New {('task-image builder' if kind == 'task-image-builder' else 'task-image registry GC' if kind == 'task-image-registry-gc' else 'execution capacity collector' if kind == 'execution-capacity-collector' else 'worker')} "
            f"token minted.\n"
            f"  prefix: {data['token_hash_prefix']}\n"
            f"\nThe raw token was NOT printed (terminal scrollback risk).\n"
            f"Pipe it straight into the secret store without exposing it\n"
            f"via shell history or `ps`:\n"
            f"\n"
            f"{install_hint}"
            f"\n"
            f"Or re-run with --show-secret to print the raw token to stdout.\n",
        )
    return 0


def _revoke_worker_token(args: argparse.Namespace) -> int:
    if not _HEX_PREFIX_RE.fullmatch(args.prefix):
        sys.stderr.write(
            f"error: prefix must be 4-64 hex characters; got {args.prefix!r}\n",
        )
        return 2

    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    url = f"{args.cp_url.rstrip('/')}/admin/worker-tokens/{args.prefix}"
    try:
        resp = httpx.delete(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach CP at {url}: {e}\n",
        )
        return 2

    if resp.status_code != 200:
        sys.stderr.write(
            f"error: CP returned {resp.status_code}: {resp.text}\n",
        )
        return 1
    sys.stdout.write(
        f"Worker token with prefix {args.prefix!r} revoked.\n",
    )
    return 0


def _rotate_worker_token(args: argparse.Namespace) -> int:
    """Mint a new worker token + print the rollout procedure. Does NOT
    revoke the old token automatically — that's an explicit
    `loom admin tokens worker revoke <prefix>` step, run AFTER the
    operator confirms the new token is live on every worker pod.
    A premature revoke would 401 in-flight worker claims.
    """
    rc = _mint_worker_token(args)
    if rc != 0:
        return rc
    if args.format != "json":
        if args.show_secret:
            install_step = (
                "  1. Install the new token into `loom-secrets` without\n"
                "     exposing it via shell history or `ps`:\n"
                "       kubectl create secret generic loom-secrets \\\n"
                '         --from-literal=worker-token="$NEW_TOKEN" \\\n'
                "         --dry-run=client -o yaml | kubectl apply -f -\n"
                "     (Or use the pipe-stdin form from `--format json`.)\n"
            )
        else:
            install_step = (
                "  1. The new token was NOT captured. To proceed without\n"
                "     orphaning it, re-run rotate piping straight into\n"
                "     the secret store, then revoke the prefix printed\n"
                "     above:\n"
                "       loom admin tokens worker rotate --format json \\\n"
                "         | jq -r .token \\\n"
                "         | kubectl create secret generic loom-secrets \\\n"
                "             --from-file=worker-token=/dev/stdin \\\n"
                "             --dry-run=client -o yaml \\\n"
                "         | kubectl apply -f -\n"
                "     Then revoke the prefix above so it doesn't linger.\n"
            )
        sys.stdout.write(
            "\nRotation checklist:\n"
            + install_step
            + "  2. Restart in-cluster workers:\n"
            + "       kubectl rollout restart deploy/loom-worker\n"
            + "  3. Verify workers re-register (no 401s) before revoking the old token.\n"
            + "  4. Revoke the OLD token by its hash prefix:\n"
            + "       loom admin tokens worker revoke <OLD_PREFIX>\n",
        )
    return 0


def _ensure_smoke_user(args: argparse.Namespace) -> int:
    """Provision the deployment-managed headless smoke-user credential.

    Idempotently ensures a dedicated non-human ``loom-smoke`` User + Team
    and mints a fresh user-owned ``submit`` token so the release-gate /
    operator trajectory smoke can submit ``oracle × a smoke task`` without a
    human login (see loom_cli.smoke_credential). Writes directly to the
    target service DB — run it after migrations during a deploy, then pipe
    the token straight into the secret store.
    """
    from loom_cli.smoke_credential import ensure_smoke_user_credential

    db_url = args.db_url
    if not db_url:
        sys.stderr.write(
            "error: no database URL. Pass --db-url or set LOOM_DB_URL "
            "(then LOOM_SVC_DB_URL) in the environment.\n",
        )
        return 2
    try:
        cred = ensure_smoke_user_credential(
            db_url,
            username=args.username,
            team_name=args.team,
            ttl_days=args.expires_in_days,
            revoke_prior=not args.keep_prior,
        )
    except Exception as e:  # surface any provisioning failure to the operator
        sys.stderr.write(f"error: could not provision smoke user: {e}\n")
        return 1

    if args.format == "json":
        json.dump(
            {
                "token": cred.raw_token,
                "token_hash_prefix": cred.token_hash_prefix,
                "username": cred.username,
                "team": cred.team_name,
                "user_id": str(cred.user_id),
                "team_id": str(cred.team_id),
                "expires_at": (cred.expires_at.isoformat() if cred.expires_at else None),
                "rotated_prior": cred.rotated_prior,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    elif args.show_secret:
        sys.stdout.write(
            f"Smoke-user credential provisioned.\n"
            f"  username: {cred.username}\n"
            f"  team:     {cred.team_name}\n"
            f"  prefix:   {cred.token_hash_prefix}\n"
            f"  token:    {cred.raw_token}\n"
            f"  revoked prior tokens: {cred.rotated_prior}\n",
        )
    else:
        sys.stdout.write(
            f"Smoke-user credential provisioned "
            f"(prefix {cred.token_hash_prefix}, "
            f"revoked {cred.rotated_prior} prior).\n"
            f"The raw token was NOT printed. Pipe it straight into the\n"
            f"secret store without exposing it via shell history or `ps`:\n"
            f"\n"
            f"  loom admin ensure-smoke-user --format json | jq -r .token \\\n"
            f"    | kubectl create secret generic loom-secrets \\\n"
            f"        --from-file=smoke-api-token=/dev/stdin \\\n"
            f"        --dry-run=client -o yaml | kubectl apply -f -\n"
            f"\n"
            f"Or re-run with --show-secret to print the raw token to stdout.\n",
        )
    return 0


def _ensure_batch_runner_token(args: argparse.Namespace) -> int:
    """Provision the deployment-managed batch-runner control-plane token.

    Mints the ``submit:batch`` token loom-service uses to fan batches out
    to the control-plane's ``POST /trials`` (see
    loom_cli.smoke_credential.ensure_batch_runner_token). A fresh/cutover
    DB has no valid one, so batches 401 and never dispatch. Writes directly
    to the target DB — run after migrations, then pipe the token into
    loom-secrets/batch-runner-cp-token and restart loom-service.
    """
    from loom_cli.smoke_credential import ensure_batch_runner_token

    db_url = args.db_url
    if not db_url:
        sys.stderr.write(
            "error: no database URL. Pass --db-url or set LOOM_DB_URL "
            "(then LOOM_SVC_DB_URL) in the environment.\n",
        )
        return 2
    try:
        tok = ensure_batch_runner_token(
            db_url,
            ttl_days=args.expires_in_days,
            revoke_prior=not args.keep_prior,
        )
    except Exception as e:  # surface any provisioning failure to the operator
        sys.stderr.write(f"error: could not provision batch-runner token: {e}\n")
        return 1

    if args.format == "json":
        json.dump(
            {
                "token": tok.raw_token,
                "token_hash_prefix": tok.token_hash_prefix,
                "expires_at": (tok.expires_at.isoformat() if tok.expires_at else None),
                "rotated_prior": tok.rotated_prior,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    elif args.show_secret:
        sys.stdout.write(
            f"Batch-runner token provisioned.\n"
            f"  prefix: {tok.token_hash_prefix}\n"
            f"  token:  {tok.raw_token}\n"
            f"  revoked prior tokens: {tok.rotated_prior}\n",
        )
    else:
        sys.stdout.write(
            f"Batch-runner token provisioned "
            f"(prefix {tok.token_hash_prefix}, revoked {tok.rotated_prior} prior).\n"
            f"The raw token was NOT printed. Pipe it into the secret store:\n"
            f"\n"
            f"  loom admin ensure-batch-runner-token --format json | jq -r .token \\\n"
            f"    | kubectl create secret generic loom-secrets \\\n"
            f"        --from-file=batch-runner-cp-token=/dev/stdin \\\n"
            f"        --dry-run=client -o yaml | kubectl apply -f -\n"
            f"  # then: kubectl rollout restart deploy/loom-service\n"
            f"\n"
            f"Or re-run with --show-secret to print the raw token to stdout.\n",
        )
    return 0


def _ensure_dev_worker_token(args: argparse.Namespace) -> int:
    """Seed the fixed dev/smoke worker token so the in-cluster loom-worker
    pods authenticate at `/workers/register` with no mint/patch/restart.

    LOCAL/DEV ONLY — installs a guessable, well-known worker credential
    (the same throwaway value `bootstrap-secrets --smoke-defaults` writes
    into `loom-secrets/worker-token`). Writes directly to the target DB;
    run after migrations. Idempotent (get-or-create by token hash).
    """
    from loom_cli.smoke_credential import ensure_dev_worker_token

    db_url = args.db_url
    if not db_url:
        sys.stderr.write(
            "error: no database URL. Pass --db-url or set LOOM_DB_URL "
            "(then LOOM_CP_DB_URL) in the environment.\n",
        )
        return 2
    try:
        res = ensure_dev_worker_token(db_url)
    except Exception as e:  # surface any provisioning failure to the operator
        sys.stderr.write(f"error: could not seed dev worker token: {e}\n")
        return 1

    if args.format == "json":
        json.dump(
            {"token_hash_prefix": res.token_hash_prefix, "created": res.created},
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    else:
        state = "seeded" if res.created else "already present"
        sys.stdout.write(
            f"Dev worker token {state} (prefix {res.token_hash_prefix}).\n"
            f"loom-worker pods carrying the --smoke-defaults worker-token now "
            f"register; crash-looping workers recover on their next retry.\n",
        )
    return 0








def _execution_admission_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    url = f"{args.cp_url.rstrip('/')}/admin/execution-admission/status"
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach CP at {url}: {exc}\n")
        return 2
    if response.status_code != 200:
        sys.stderr.write(f"error: CP returned {response.status_code}: {response.text}\n")
        return 1
    data = response.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    policies = data.get("policies", [])
    sys.stdout.write("Execution admission ceilings:\n")
    if not policies:
        sys.stdout.write("  no admission policies recorded\n")
        return 0
    for row in policies:
        available = row.get("available")
        sys.stdout.write(
            f"  {row['scope_kind']}/{row['scope_key']} "
            f"enabled={row['enabled']} active={row['active_count']} "
            f"ledger={row.get('ledger_active_count', row['active_count'])} "
            f"sync={row.get('counter_in_sync', False)} "
            f"max={row['max_concurrent']} "
            f"available={available if available is not None else '-'} "
            f"version={row['version']} reason={row.get('reason') or '-'}\n"
        )
    return 0


def _execution_finance_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    url = f"{args.cp_url.rstrip('/')}/admin/execution-finance/status"
    params = {"pool_id": args.pool_id} if args.pool_id else None
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            params=params,
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach CP at {url}: {exc}\n")
        return 2
    if response.status_code != 200:
        sys.stderr.write(f"error: CP returned {response.status_code}: {response.text}\n")
        return 1
    data = response.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    sys.stdout.write("Execution finance:\n")
    for row in data.get("budget_policies", []):
        daily_used = row["daily_reserved_microusd"] + row["daily_settled_microusd"]
        monthly_used = row["monthly_reserved_microusd"] + row["monthly_settled_microusd"]
        sys.stdout.write(
            f"  budget {row['scope_kind']}/{row['scope_key']} "
            f"enabled={row['enabled']} stop={row['emergency_stop']} "
            f"daily={daily_used}/{row['daily_limit_microusd']} "
            f"monthly={monthly_used}/{row['monthly_limit_microusd']} "
            f"sync={row.get('counter_in_sync', False)} "
            f"version={row['version']}\n"
        )
    for row in data.get("cost_reservations", []):
        sys.stdout.write(
            f"  reservation {row['id']} pool={row['pool_id']} state={row['state']} "
            f"estimate={row['estimated_cost_microusd']} "
            f"actual={row.get('actual_allocated_microusd')}\n"
        )
    for row in data.get("node_cost_records", []):
        sys.stdout.write(
            f"  node-cost {row['provider_record_id']} target={row['target_id']} "
            f"billed={row['provider_billed_microusd']} "
            f"allocated={row['allocated_microusd']} "
            f"overhead={row['idle_system_fragmentation_microusd']}\n"
        )
    if not any(
        data.get(key) for key in ("budget_policies", "cost_reservations", "node_cost_records")
    ):
        sys.stdout.write("  no execution finance records\n")
    return 0


def _execution_provisioning_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    url = f"{args.cp_url.rstrip('/')}/admin/execution-capacity/status"
    params = {"pool_id": args.pool_id} if args.pool_id else None
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            params=params,
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach CP at {url}: {exc}\n")
        return 2
    if response.status_code != 200:
        sys.stderr.write(f"error: CP returned {response.status_code}: {response.text}\n")
        return 1
    data = response.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    sys.stdout.write("Execution provisioning capacity:\n")
    targets = data.get("targets", [])
    if not targets:
        sys.stdout.write("  no Nebius execution targets\n")
        return 0
    for row in targets:
        observation = row.get("observation") or {}
        policy = row.get("policy") or {}
        counts = row.get("authorization_counts") or {}
        blockers = row.get("blockers") or []
        sys.stdout.write(
            f"  {row['target_id']} pool={row['pool_id']} "
            f"fresh={observation.get('is_fresh', False)} "
            f"provider={observation.get('provider_capacity_state', 'unknown')} "
            f"autoscaler={observation.get('autoscaler_state', 'unknown')} "
            f"nodes={observation.get('provider_used_nodes', '-')}/"
            f"{observation.get('provider_quota_nodes', '-')} "
            f"pending={observation.get('pending_jobs', '-')}/"
            f"{policy.get('max_pending_jobs', '-')} "
            f"commands={row.get('command_backlog', 0)} "
            f"authorized={counts.get('authorized', 0)} "
            f"running={counts.get('running', 0)} "
            f"blockers={','.join(str(item) for item in blockers) or '-'}\n"
        )
    return 0


def _execution_resource_profile_status(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    url = f"{args.cp_url.rstrip('/')}/admin/execution-resource-profile/status"
    params = {"pool_id": args.pool_id} if args.pool_id else None
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            params=params,
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach CP at {url}: {exc}\n")
        return 2
    if response.status_code != 200:
        sys.stderr.write(f"error: CP returned {response.status_code}: {response.text}\n")
        return 1
    data = response.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    sys.stdout.write("Execution resource calibration and forecast:\n")
    targets = data.get("targets", [])
    if not targets:
        sys.stdout.write("  no Nebius execution targets\n")
        return 0
    for row in targets:
        calibration = row.get("calibration") or {}
        blockers = row.get("blockers") or []
        sys.stdout.write(
            f"  {row['target_id']} pool={row['pool_id']} "
            f"fresh={row.get('forecast_is_fresh', False)} "
            f"profile={calibration.get('resource_profile', '-')} "
            f"attempts={calibration.get('trial_attempts', 0)} "
            f"peak={calibration.get('peak_batch_concurrency', 0)} "
            f"immediate={row.get('immediate_executable_slots', 0)} "
            f"scale_headroom={row.get('configured_scale_headroom_slots', 0)} "
            f"blockers={','.join(str(item) for item in blockers) or '-'}\n"
        )
    return 0


def _execution_resource_profile_calibrate(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    url = f"{args.cp_url.rstrip('/')}/admin/execution-resource-calibrations"
    payload = {
        "target_id": args.target_id,
        "source_pool_id": args.source_pool_id,
        "source_architecture": args.source_architecture,
        "resource_profile": args.resource_profile,
        "candidate_sha": args.candidate_sha,
        "source_version": args.source_version,
        "window_started_at": args.window_started_at,
        "window_stopped_at": args.window_stopped_at,
    }
    try:
        response = httpx.post(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            json=payload,
            timeout=30.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach CP at {url}: {exc}\n")
        return 2
    if response.status_code != 200:
        sys.stderr.write(f"error: CP returned {response.status_code}: {response.text}\n")
        return 1
    data = response.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    recommendation = data["recommendation"]
    blockers = data.get("blockers") or []
    sys.stdout.write(
        f"calibration={data['id']} created={data['created']} "
        f"eligible={data['eligible']} attempts={data['trial_attempts']} "
        f"tasks={data['distinct_tasks']} peak={data['peak_batch_concurrency']} "
        f"cpu={recommendation['cpu_millis']}m "
        f"memory={recommendation['memory_mib']}MiB "
        f"storage={recommendation['ephemeral_storage_mib']}MiB "
        f"pids={recommendation['pids']} "
        f"blockers={','.join(str(item) for item in blockers) or '-'}\n"
    )
    return 0


def _execution_resource_profile_bind(args: argparse.Namespace) -> int:
    try:
        admin_token = _resolve_admin_token(args.admin_token)
    except ValueError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    url = f"{args.cp_url.rstrip('/')}/admin/execution-resource-profile-bindings/{args.target_id}"
    try:
        response = httpx.put(
            url,
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "calibration_id": args.calibration_id,
                "enabled": args.enabled,
                "reason": args.reason,
            },
            timeout=10.0,
        )
    except httpx.RequestError as exc:
        sys.stderr.write(f"error: could not reach CP at {url}: {exc}\n")
        return 2
    if response.status_code != 200:
        sys.stderr.write(f"error: CP returned {response.status_code}: {response.text}\n")
        return 1
    data = response.json()
    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(
            f"target={data['target_id']} calibration={data['calibration_id']} "
            f"enabled={data['enabled']} version={data['version']}\n"
        )
    return 0






















_KNOWN_TEAM_SCOPES = (
    "read:own",
    "submit",
    "providers:manage",
    "tokens:manage",
)
_KNOWN_TOKEN_TYPES = ("team",)
_HEX_8_PREFIX_RE = re.compile(r"^[0-9a-f]{8}$")
_DEFAULT_TEAM_SCOPES = ("read:own", "submit")


def _mint_team_token(args: argparse.Namespace) -> int:
    """POST to `loom_service` /api/v1/tokens. Uses the bearer from
    `loom auth login` and adds X-Loom-Admin-Actor when supplied."""
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    body: dict[str, object] = {
        "name": args.name,
        "type": args.type,
        "scopes": list(args.scopes),
        "expires_in_days": args.expires_in_days,
    }
    if args.team_id is not None:
        body["team_id"] = args.team_id

    headers: dict[str, str] = {}
    if args.admin_actor is not None:
        headers["X-Loom-Admin-Actor"] = args.admin_actor

    try:
        with authed_client(cfg) as c:
            resp = c.post(
                "/api/v1/tokens",
                json=body,
                headers=headers or None,
            )
            data = assert_2xx(resp, action="mint legacy team token")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(
            f"New {args.type} token minted.\n"
            f"  name:       {args.name}\n"
            f"  prefix:     {data['token_hash_prefix']}\n"
            f"  token:      {data['token']}\n"
            f"  expires_at: {data['expires_at']}\n",
        )
    return 0


def _revoke_team_token(args: argparse.Namespace) -> int:
    if not _HEX_8_PREFIX_RE.fullmatch(args.prefix):
        sys.stderr.write(
            f"error: prefix must be exactly 8 lowercase hex chars; got {args.prefix!r}\n",
        )
        return 2

    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    headers: dict[str, str] = {}
    if args.admin_actor is not None:
        headers["X-Loom-Admin-Actor"] = args.admin_actor

    try:
        with authed_client(cfg) as c:
            resp = c.delete(
                f"/api/v1/tokens/{args.prefix}",
                headers=headers or None,
            )
            assert_2xx(resp, action="revoke legacy team token")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    sys.stdout.write(
        f"Token with prefix {args.prefix!r} revoked.\n",
    )
    return 0


def _rotate_team_token(args: argparse.Namespace) -> int:
    """Mint a new legacy team token + print the rollout checklist. Does NOT
    auto-revoke the old token — premature delete would break clients
    still using the old credential."""
    rc = _mint_team_token(args)
    if rc != 0:
        return rc
    if args.format != "json":
        sys.stdout.write(
            "\nRotation checklist:\n"
            "  1. Distribute the new token to its holders through a "
            "secure channel (1Password, signed email, etc).\n"
            "  2. Confirm clients are using the new token "
            "(check server logs for the new prefix).\n"
            "  3. Revoke the OLD token by its hash prefix:\n"
            "       loom admin tokens team revoke <OLD_PREFIX> "
            "[--admin-actor NAME]\n",
        )
    return 0


def _sync_yibuapi_rate_cards(args: argparse.Namespace) -> int:
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    body: dict[str, object] = {}
    if args.source_url is not None:
        body["source_url"] = args.source_url
    if args.group != "default":
        body["group"] = args.group

    try:
        with authed_client(cfg) as c:
            resp = c.post("/api/v1/rate-cards/sync/yibuapi", json=body)
            data = assert_2xx(resp, action="sync YibuAPI rate card")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    sys.stdout.write(
        "Synced YibuAPI rate card.\n"
        f"  id:              {data.get('id', '-')}\n"
        f"  source_url:      {data.get('source_url', '-')}\n"
        f"  pricing_version: {data.get('pricing_version', '-')}\n"
        f"  entries:         {data.get('entry_count', 0)}\n"
        f"  skipped:         {data.get('skipped_model_count', 0)}\n",
    )
    return 0


def _admin_submit_batch_on_behalf(args: argparse.Namespace) -> int:
    from loom_cli.eval_cmd import (
        _agent_needs_model,
        _build_agent_model,
        _print_batch_summary,
    )
    from loom_cli.providers_cmd import _resolve_by_name

    admin_actor = args.admin_actor.strip() if args.admin_actor else ""
    if not admin_actor:
        sys.stderr.write("error: --admin-actor is required for admin on-behalf submission\n")
        return 2
    warn_legacy_backend_flag(args.backend)
    if args.task_filter is not None and args.benchmark is not None:
        sys.stderr.write(
            "error: --benchmark and --task-filter are mutually exclusive "
            "(--benchmark B is sugar for --task-filter "
            '\'{"benchmark_id":"B"}\').\n',
        )
        return 2
    if args.task_filter is None and args.benchmark is None:
        sys.stderr.write("error: one of --benchmark or --task-filter is required.\n")
        return 2

    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    try:
        with authed_client(cfg) as c:
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
                    sys.stderr.write(
                        f"error: agent {args.agent!r} requires --provider "
                        "and --model for admin on-behalf batch submission; "
                        "missing " + ", ".join(missing) + ".\n",
                    )
                    return 2
            elif args.provider or args.model or args.agent_provider:
                sys.stderr.write(
                    f"error: agent {args.agent!r} does not take a model; "
                    "omit --provider, --model, and --agent-provider.\n",
                )
                return 2

            task_filter = (
                args.task_filter
                if args.task_filter is not None
                else {"benchmark_id": args.benchmark}
            )
            if args.purpose == "evaluation" and (
                (
                    isinstance(task_filter.get("task_set_id"), str)
                    and task_filter.get("task_set_id")
                )
                or (
                    isinstance(task_filter.get("task_set_ids"), (list, tuple))
                    and any(
                        isinstance(item, str) and item
                        for item in task_filter.get("task_set_ids", [])
                    )
                )
            ):
                sys.stderr.write(
                    "error: --purpose evaluation only allows native "
                    "benchmarks (no TaskSet selectors).\n",
                )
                return 2
            trial_config: dict[str, Any] = {
                "agent_name": args.agent,
                "agent_model": None,
            }
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
            payload: dict[str, Any] = {
                "represented_username": args.represented_username,
                "team_id": args.team_id,
                "purpose": args.purpose,
                "task_filter": task_filter,
                "trial_config": trial_config,
            }
            if args.name is not None:
                payload["name"] = args.name
            if args.name_suffix is not None:
                payload["name_suffix"] = args.name_suffix
            if args.description is not None:
                payload["description"] = args.description
            if args.n_per_task is not None:
                payload["n_per_task"] = args.n_per_task
            if args.backend is not None:
                payload["backend"] = args.backend
            if args.required_worker_pool:
                payload["required_worker_pools"] = args.required_worker_pool
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
                payload["provider_connection_id"] = conn["id"]
                payload["provider_model_id"] = args.model

            resp = c.post(
                "/api/v1/admin/batches/on-behalf",
                json=payload,
                headers={"X-Loom-Admin-Actor": admin_actor},
            )
            body = assert_2xx(resp, action="submit admin on-behalf batch")
    except HttpStatusError as e:
        sys.stderr.write(f"error: {e}\n")
        return 1
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    if body.get("name") is None:
        body = {**body, "name": args.name or "(server-generated)"}
    sys.stdout.write(
        f"Submitted on-behalf batch for {args.represented_username!r}:\n",
    )
    _print_batch_summary(body)
    return 0


def _load_task_filter_json(raw: str) -> dict[str, Any]:
    from loom_cli.eval_cmd import _load_task_filter_json as load

    return load(raw)


def _scopes_argparse_type(value: str) -> list[str]:
    """Accept a comma-separated list of scopes. Rejects unknown
    scopes client-side to surface typos before the round-trip."""
    items = [s.strip() for s in value.split(",") if s.strip()]
    if not items:
        raise argparse.ArgumentTypeError("--scopes cannot be empty")
    unknown = [s for s in items if s not in _KNOWN_TEAM_SCOPES]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown scope(s) {unknown}; known: {list(_KNOWN_TEAM_SCOPES)}",
        )
    return items


def _add_team_mint_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--name",
        required=True,
        help="Human-readable token name shown in token lists and audit logs.",
    )
    p.add_argument(
        "--type",
        choices=_KNOWN_TOKEN_TYPES,
        default="team",
        help=(
            "Token type. Admin credentials are file-backed singleton secrets; "
            "use `loom service init-admin` or `loom service rotate-admin` "
            "for that lifecycle."
        ),
    )
    p.add_argument(
        "--team-id",
        default=None,
        help=(
            "UUID of the team this token grants access to. Required "
            "for --type team when called as an admin; defaults to "
            "the caller's team for non-admin callers."
        ),
    )
    p.add_argument(
        "--scopes",
        type=_scopes_argparse_type,
        default=list(_DEFAULT_TEAM_SCOPES),
        help=(
            "Comma-separated scope list. Known: "
            f"{', '.join(_KNOWN_TEAM_SCOPES)}. "
            f"Default: {','.join(_DEFAULT_TEAM_SCOPES)}."
        ),
    )
    p.add_argument(
        "--expires-in-days",
        type=int,
        default=90,
        help="Token lifetime in days (default: 90).",
    )
    p.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets `X-Loom-Admin-Actor`. Required when the logged-in "
            "bearer is an admin token (audit trail)."
        ),
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )


def _add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--cp-url",
        default=_DEFAULT_CP_URL,
        help=(
            f"Control Plane base URL (default: {_DEFAULT_CP_URL}). The "
            f"CP admin surface is NOT public; port-forward in another "
            f"shell: kubectl port-forward "
            f"deploy/loom-control-plane 8080:8080"
        ),
    )
    p.add_argument(
        "--admin-token",
        default=_DEFAULT_ADMIN_TOKEN_SOURCE,
        help=(
            f"Admin token source. ONE of: env:VAR (read os.environ[VAR]), "
            f"file:PATH (read file content), or '-' (read stdin). "
            f"Default: {_DEFAULT_ADMIN_TOKEN_SOURCE!r}."
        ),
    )


def _generate_new_key() -> str:
    """Return a fresh base64-encoded 32-byte key."""
    import base64

    return base64.b64encode(os.urandom(32)).decode()


def _rewrap_secret_store(args: argparse.Namespace) -> int:
    """POST /api/v1/admin/secret-store/rewrap.

    2-stage online rotation protocol — see operator-runbook.md
    §Secret-store master-key rotation for the full procedure.
    """
    try:
        cfg = require_logged_in()
    except NotLoggedInError as e:
        sys.stderr.write(f"error: {e}\n")
        return 2

    # Handle --generate-new-key: mint a key, print it + the kubectl
    # commands, but do NOT call the endpoint (the key isn't deployed yet).
    if getattr(args, "generate_new_key", False):
        new_key = _generate_new_key()
        sys.stdout.write(
            f"Generated new master key (keep this secret!):\n"
            f"  {new_key}\n\n"
            f"Next steps:\n"
            f"  1. Deploy new key as FALLBACK (existing rows still readable):\n"
            f"       # Read the current key first:\n"
            f"       OLD_KEY=$(kubectl get secret loom-secrets "
            f"-o jsonpath='{{.data.secret-store-master-key}}' | base64 -d)\n"
            f"       kubectl patch secret loom-secrets \\\n"
            f'         -p \'{{"stringData":{{"secret-store-master-keys":'
            f'"{new_key},${{OLD_KEY}}"}},"data":{{"secret-store-master-key":null}}}}\'\n'
            f"       kubectl rollout restart deploy/loom-service\n\n"
            f"  2. Run the rewrap walk:\n"
            f"       loom admin secret-store rewrap --new-key {new_key!r} "
            f"--admin-actor <your-name>\n\n"
            f"  3. Drop the old key (new key only):\n"
            f"       kubectl patch secret loom-secrets \\\n"
            f'         -p \'{{"stringData":{{"secret-store-master-keys":null,'
            f'"secret-store-master-key":"{new_key}"}}}}\n'
            f"       kubectl rollout restart deploy/loom-service\n",
        )
        return 0

    if getattr(args, "dry_run", False):
        # Dry run: list refs without rewrapping.
        sys.stdout.write(
            "[dry-run] Would call POST /api/v1/admin/secret-store/rewrap\n"
            "[dry-run] Use without --dry-run to execute.\n",
        )
        return 0

    new_key_b64 = getattr(args, "new_key", None)
    admin_actor = getattr(args, "admin_actor", None)

    body: dict[str, object] = {}
    if new_key_b64:
        body["new_master_key"] = new_key_b64

    headers: dict[str, str] = {}
    if admin_actor is not None:
        headers["X-Loom-Admin-Actor"] = admin_actor

    try:
        with authed_client(cfg) as c:
            resp = c.post(
                "/api/v1/admin/secret-store/rewrap",
                json=body,
                headers=headers or None,
                timeout=300.0,  # may take a while for large secrets tables
            )
            # 200 = all rewrapped; 207 = partial (some failed)
            if resp.status_code not in (200, 207):
                try:
                    detail = resp.json().get("detail", resp.text)
                except Exception:
                    detail = resp.text
                sys.stderr.write(
                    f"error: server returned {resp.status_code}: {detail}\n",
                )
                return 1
            data = resp.json()
    except httpx.RequestError as e:
        sys.stderr.write(
            f"error: could not reach {cfg.server_url}: {e}\n",
        )
        return 2

    rewrapped = data.get("rewrapped", 0)
    failed = data.get("failed", [])

    if args.format == "json":
        json.dump(data, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(f"Rewrapped {rewrapped} secret(s).\n")
        if failed:
            sys.stdout.write(
                f"\nFailed ({len(failed)}):\n",
            )
            for ref, err in failed:
                sys.stdout.write(f"  {ref}: {err}\n")
            sys.stdout.write(
                "\nReview failures above. Refs that failed are still encrypted with the OLD key.\n",
            )
        else:
            sys.stdout.write(
                "\nAll secrets now use the primary key.\n"
                "Next step: drop the fallback key from loom-secrets "
                "and restart loom-service:\n"
                "  kubectl patch secret loom-secrets \\\n"
                '    -p \'{"stringData":{"secret-store-master-keys":null,'
                '"secret-store-master-key":"<NEW_KEY>"}}\n'
                "  kubectl rollout restart deploy/loom-service\n",
            )

    return 1 if failed else 0


def _register_agent_runtime(args: argparse.Namespace) -> int:
    from loom.agent_runtime import AgentRuntimeReleaseV1

    try:
        release = AgentRuntimeReleaseV1.model_validate_json(Path(args.release).read_text())
        token = _resolve_admin_token(args.admin_token)
        response = httpx.put(
            args.cp_url.rstrip("/") + f"/admin/agents/{release.agent_name}/versions/{release.agent_version}",
            json=release.model_dump(mode="json"),
            headers={"Authorization": f"Bearer {token}"}, timeout=30,
        )
        result = assert_2xx(response, action="register published agent runtime")
    except (OSError, ValueError, SecretSourceError, HttpStatusError):
        sys.stderr.write("error: invalid or conflicting published agent runtime release\n")
        return 2
    except httpx.RequestError:
        sys.stderr.write("error: control plane unavailable\n")
        return 2
    print(json.dumps(result, indent=2))
    return 0


def dispatch(argv: list[str]) -> int:
    """Entry point invoked from `loom_cli.__main__` when `argv[0]` is
    `admin`. Returns the process exit code."""
    parser = argparse.ArgumentParser(
        prog="loom admin",
        description=(
            "Operator-only admin operations. Subcommands: "
            "`tokens` (worker/team token rotation), "
            "`env-diagnostics` (redacted runtime environment inspection), "
            "`rate-cards` (pricing catalog sync), and "
            "`secret-store` (master-key rotation walker)."
        ),
    )
    sub = parser.add_subparsers(dest="admin_cmd", required=True)

    from loom_cli.pipeline_admin_cmd import add_pipeline_admin_subparser
    from loom_cli.platform_admin_cmd import add_platform_admin_subparser

    add_pipeline_admin_subparser(sub)
    add_platform_admin_subparser(sub)

    p_agents = sub.add_parser("agent-runtime", help="Register a trusted published native runtime.")
    agent_commands = p_agents.add_subparsers(dest="agent_runtime_command", required=True)
    p_register = agent_commands.add_parser("register")
    p_register.add_argument("--release", required=True, help="Published agent-runtime-release.json file.")
    _add_common_args(p_register)
    p_register.set_defaults(handler=_register_agent_runtime)

    p_env_diagnostics = sub.add_parser(
        "env-diagnostics",
        help="Print redacted runtime environment diagnostics.",
    )
    p_env_diagnostics.add_argument(
        "--prefix",
        action="append",
        default=None,
        help=(
            "Environment variable prefix to include. Repeat to inspect "
            "multiple scoped prefixes. Defaults to LOOM_."
        ),
    )
    p_env_diagnostics.add_argument(
        "--format",
        choices=["text", "json", "markdown"],
        default="text",
        help="Output format for terminal use or evidence artifacts.",
    )
    p_env_diagnostics.set_defaults(handler=_env_diagnostics)

    p_smoke = sub.add_parser(
        "ensure-smoke-user",
        help=(
            "Provision the deployment-managed headless smoke-user "
            "credential (user-owned submit token for the trajectory smoke)."
        ),
    )
    p_smoke.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL") or os.environ.get("LOOM_SVC_DB_URL"),
        help=(
            "Target service Postgres URL. Defaults to env LOOM_DB_URL, then "
            "LOOM_SVC_DB_URL, so it isn't exposed via argv."
        ),
    )
    p_smoke.add_argument(
        "--username",
        default="loom-smoke",
        help="Smoke-user username (default: loom-smoke).",
    )
    p_smoke.add_argument(
        "--team",
        default="loom-smoke",
        help="Team the smoke user owns (default: loom-smoke).",
    )
    p_smoke.add_argument(
        "--expires-in-days",
        type=int,
        default=90,
        help="Token lifetime in days (default: 90).",
    )
    p_smoke.add_argument(
        "--keep-prior",
        action="store_true",
        help=(
            "Do NOT revoke the smoke user's existing tokens. Default is to "
            "rotate so exactly one live credential remains."
        ),
    )
    p_smoke.add_argument(
        "--show-secret",
        action="store_true",
        help="Print the raw token to stdout (terminal scrollback risk).",
    )
    p_smoke.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_smoke.set_defaults(handler=_ensure_smoke_user)

    p_br = sub.add_parser(
        "ensure-batch-runner-token",
        help=(
            "Provision the deployment-managed batch-runner CP token "
            "(submit:batch token loom-service uses to dispatch batches)."
        ),
    )
    p_br.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL") or os.environ.get("LOOM_SVC_DB_URL"),
        help=(
            "Target service Postgres URL. Defaults to env LOOM_DB_URL, then "
            "LOOM_SVC_DB_URL, so it isn't exposed via argv."
        ),
    )
    p_br.add_argument(
        "--expires-in-days",
        type=int,
        default=90,
        help="Token lifetime in days (default: 90).",
    )
    p_br.add_argument(
        "--keep-prior",
        action="store_true",
        help=(
            "Do NOT revoke prior deploy-provisioned batch-runner tokens. "
            "Default rotates so exactly one is live."
        ),
    )
    p_br.add_argument(
        "--show-secret",
        action="store_true",
        help="Print the raw token to stdout (terminal scrollback risk).",
    )
    p_br.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_br.set_defaults(handler=_ensure_batch_runner_token)

    p_dwt = sub.add_parser(
        "ensure-dev-worker-token",
        help=(
            "LOCAL/DEV ONLY: seed the fixed --smoke-defaults worker token "
            "so in-cluster workers register with no mint/patch/restart."
        ),
    )
    p_dwt.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL") or os.environ.get("LOOM_CP_DB_URL"),
        help=(
            "Target Postgres URL. Defaults to env LOOM_DB_URL, then "
            "LOOM_CP_DB_URL, so it isn't exposed via argv."
        ),
    )
    p_dwt.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_dwt.set_defaults(handler=_ensure_dev_worker_token)

    p_tokens = sub.add_parser(
        "tokens",
        help="Token mint / revoke / rotate.",
    )
    tok_sub = p_tokens.add_subparsers(dest="tokens_target", required=True)

    p_worker = tok_sub.add_parser(
        "worker",
        help="Worker-token operations (Control Plane admin surface).",
    )
    worker_sub = p_worker.add_subparsers(
        dest="worker_op",
        required=True,
    )

    p_mint = worker_sub.add_parser(
        "mint",
        help="Issue a new worker token. Prints the raw token + prefix.",
    )
    _add_common_args(p_mint)
    p_mint.add_argument(
        "--expires-in-days",
        type=int,
        default=_DEFAULT_EXPIRES_DAYS,
        help=(
            f"Token lifetime in days "
            f"(default: {_DEFAULT_EXPIRES_DAYS}). Pass 0 to omit the "
            f"expires_in_days field; persistent identity kinds that support "
            f"it will then be non-expiring."
        ),
    )
    p_mint.add_argument(
        "--kind",
        choices=[
            "trial",
            "task-image-builder",
            "task-image-registry-gc",
            "execution-capacity-collector",
        ],
        default="trial",
        help=(
            "Mint an ordinary trial worker or a least-privilege task-image builder, "
            "registry-GC, or execution-capacity collector token."
        ),
    )
    p_mint.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format. JSON for scripting.",
    )
    p_mint.add_argument(
        "--show-secret",
        action="store_true",
        help=(
            "Print the raw token to stdout in text mode. Default is "
            "prefix-only to keep the raw value out of terminal "
            "scrollback. JSON mode always includes the token."
        ),
    )
    p_mint.set_defaults(handler=_mint_worker_token)

    p_revoke = worker_sub.add_parser(
        "revoke",
        help="Revoke a worker token by its hash prefix.",
    )
    p_revoke.add_argument(
        "prefix",
        help="4-64 hex chars from token_hash_prefix returned at mint.",
    )
    _add_common_args(p_revoke)
    p_revoke.set_defaults(handler=_revoke_worker_token)

    p_rotate = worker_sub.add_parser(
        "rotate",
        help=(
            "Mint a new worker token + print the rollout procedure. "
            "Does NOT revoke the old token automatically — run "
            "`revoke <OLD_PREFIX>` once new is live."
        ),
    )
    _add_common_args(p_rotate)
    p_rotate.add_argument(
        "--expires-in-days",
        type=int,
        default=_DEFAULT_EXPIRES_DAYS,
        help=f"Token lifetime in days (default: {_DEFAULT_EXPIRES_DAYS}).",
    )
    p_rotate.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_rotate.add_argument(
        "--show-secret",
        action="store_true",
        help=(
            "Print the raw token to stdout in text mode. Default is "
            "prefix-only; pipe `--format json` into the secret store "
            "to avoid putting the raw value in shell history."
        ),
    )
    p_rotate.set_defaults(handler=_rotate_worker_token)

    p_worker_pools = sub.add_parser("worker-pools", help="Inspect native execution admission, capacity, and resources.")
    worker_pools_sub = p_worker_pools.add_subparsers(dest="worker_pools_op", required=True)

    p_admission_status = worker_pools_sub.add_parser(
        "admission-status",
        help="Show native execution admission ceilings and active reservations.",
    )
    _add_common_args(p_admission_status)
    p_admission_status.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_admission_status.set_defaults(handler=_execution_admission_status)

    p_finance_status = worker_pools_sub.add_parser(
        "finance-status",
        help="Show paid-execution prices, budgets, reservations, and node-bill attribution.",
    )
    _add_common_args(p_finance_status)
    p_finance_status.add_argument(
        "--pool-id",
        default=None,
        help="Limit finance records to one logical pool.",
    )
    p_finance_status.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_finance_status.set_defaults(handler=_execution_finance_status)

    p_provisioning_status = worker_pools_sub.add_parser(
        "provisioning-status",
        help="Show Nebius quota, allocatable, Pending, autoscaler, and create authority.",
    )
    _add_common_args(p_provisioning_status)
    p_provisioning_status.add_argument(
        "--pool-id",
        default=None,
        help="Limit provisioning capacity to one logical pool.",
    )
    p_provisioning_status.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_provisioning_status.set_defaults(handler=_execution_provisioning_status)

    p_resource_profile = worker_pools_sub.add_parser(
        "resource-profile",
        help="Create evidence-gated resource calibrations, bind them, and inspect forecasts.",
    )
    resource_profile_sub = p_resource_profile.add_subparsers(
        dest="resource_profile_op",
        required=True,
    )
    p_resource_profile_status = resource_profile_sub.add_parser(
        "status",
        help="Show immutable calibration evidence and non-executable scale forecasts.",
    )
    _add_common_args(p_resource_profile_status)
    p_resource_profile_status.add_argument("--pool-id", default=None)
    p_resource_profile_status.add_argument("--format", choices=["text", "json"], default="text")
    p_resource_profile_status.set_defaults(handler=_execution_resource_profile_status)

    p_resource_profile_calibrate = resource_profile_sub.add_parser(
        "calibrate",
        help="Derive one immutable recommendation from persisted #1503 telemetry.",
    )
    _add_common_args(p_resource_profile_calibrate)
    p_resource_profile_calibrate.add_argument("--target-id", required=True)
    p_resource_profile_calibrate.add_argument("--source-pool-id", required=True)
    p_resource_profile_calibrate.add_argument(
        "--source-architecture", choices=["x86_64", "arm64"], required=True
    )
    p_resource_profile_calibrate.add_argument("--resource-profile", required=True)
    p_resource_profile_calibrate.add_argument("--candidate-sha", required=True)
    p_resource_profile_calibrate.add_argument("--source-version", required=True)
    p_resource_profile_calibrate.add_argument("--window-started-at", required=True)
    p_resource_profile_calibrate.add_argument("--window-stopped-at", required=True)
    p_resource_profile_calibrate.add_argument("--format", choices=["text", "json"], default="text")
    p_resource_profile_calibrate.set_defaults(handler=_execution_resource_profile_calibrate)

    p_resource_profile_bind = resource_profile_sub.add_parser(
        "bind",
        help="Bind an eligible immutable calibration to one Nebius target forecast.",
    )
    _add_common_args(p_resource_profile_bind)
    p_resource_profile_bind.add_argument("--target-id", required=True)
    p_resource_profile_bind.add_argument("--calibration-id", required=True)
    p_resource_profile_bind.add_argument(
        "--enabled", action=argparse.BooleanOptionalAction, default=False
    )
    p_resource_profile_bind.add_argument("--reason", default=None)
    p_resource_profile_bind.add_argument("--format", choices=["text", "json"], default="text")
    p_resource_profile_bind.set_defaults(handler=_execution_resource_profile_bind)

    p_team = tok_sub.add_parser(
        "team",
        help=(
            "Legacy team-token operations via `loom_service`'s "
            "`/api/v1/tokens` route. Uses the server + bearer from "
            "`loom auth login`."
        ),
    )
    team_sub = p_team.add_subparsers(dest="team_op", required=True)

    p_team_mint = team_sub.add_parser(
        "mint",
        help="Issue a legacy team token. Admin caller is recorded in audit.",
    )
    _add_team_mint_args(p_team_mint)
    p_team_mint.set_defaults(handler=_mint_team_token)

    p_team_revoke = team_sub.add_parser(
        "revoke",
        help="Revoke a legacy team token by its 8-hex-char prefix.",
    )
    p_team_revoke.add_argument(
        "prefix",
        help="Exactly 8 lowercase hex chars from token_hash_prefix.",
    )
    p_team_revoke.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets `X-Loom-Admin-Actor`. Required when the logged-in "
            "bearer is an admin token (audit trail). Ignored when "
            "the bearer is a user-owned API token."
        ),
    )
    p_team_revoke.set_defaults(handler=_revoke_team_token)

    p_team_rotate = team_sub.add_parser(
        "rotate",
        help=(
            "Mint a new legacy team token + print the rollout procedure. "
            "Does NOT revoke the old token automatically."
        ),
    )
    _add_team_mint_args(p_team_rotate)
    p_team_rotate.set_defaults(handler=_rotate_team_token)

    p_batches = sub.add_parser(
        "batches",
        help="Admin batch operations through the public service API.",
    )
    batches_sub = p_batches.add_subparsers(dest="batches_op", required=True)
    p_submit_on_behalf = batches_sub.add_parser(
        "submit-on-behalf",
        help="Submit an audited batch on behalf of an active user/team.",
    )
    p_submit_on_behalf.add_argument(
        "--represented-username",
        required=True,
        help="Active username to record as the represented submitter.",
    )
    p_submit_on_behalf.add_argument(
        "--team-id",
        required=True,
        help="Represented team UUID. The user must be a member of this team.",
    )
    p_submit_on_behalf.add_argument("--agent", required=True)
    p_submit_on_behalf.add_argument(
        "--provider",
        default=None,
        help=(
            "Provider connection name. Required for agents that call a "
            "model; omit for no-model agents such as oracle."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--model",
        default=None,
        help=(
            "Upstream model id. Required for agents that call a model; "
            "omit for no-model agents such as oracle."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--agent-provider",
        dest="agent_provider",
        default=None,
        help="Override the agent model provider field for pricing/adapter compatibility.",
    )
    p_submit_on_behalf.add_argument(
        "--purpose",
        choices=("evaluation", "trajectory_generation"),
        required=True,
        help=(
            "Batch purpose. evaluation = native benchmarks with verification; "
            "trajectory_generation = TaskSets and/or benchmarks (transition)."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--skip-verifier",
        dest="skip_verifier",
        action="store_true",
        help=(
            "Skip the verifier phase (trajectory_generation only). "
            "Rejected with --purpose evaluation."
        ),
    )
    p_submit_on_behalf.add_argument(
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
    p_submit_on_behalf.add_argument(
        "--benchmark",
        default=None,
        help=('Benchmark slug — shortcut for --task-filter \'{"benchmark_id":"..."}\'.'),
    )
    p_submit_on_behalf.add_argument(
        "--task-filter",
        dest="task_filter",
        type=_load_task_filter_json,
        default=None,
        help=(
            "Task filter as JSON (object). Pass a literal JSON string "
            "or `@path/to/file.json` to read from disk."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--name",
        default=None,
        help="Optional batch display name. When omitted, the server generates one.",
    )
    p_submit_on_behalf.add_argument(
        "--name-suffix",
        default=None,
        help="Optional suffix appended to the server-generated display name.",
    )
    p_submit_on_behalf.add_argument("--description", default=None)
    p_submit_on_behalf.add_argument(
        "--n-per-task",
        dest="n_per_task",
        type=int,
        default=None,
        help="Number of trials per task (1-100).",
    )
    add_legacy_backend_flag(p_submit_on_behalf)
    p_submit_on_behalf.add_argument(
        "--required-worker-pool",
        dest="required_worker_pool",
        action="append",
        default=[],
        help=(
            "Operator and release coverage only: add one extra "
            "pool-pinned coverage trial on this worker pool. Repeat for "
            "mixed-pool release canaries. Not for user eval batches — "
            "use a separate on-behalf canary batch instead."
        ),
    )
    p_submit_on_behalf.add_argument(
        "--admin-actor",
        default=None,
        help="Required audit actor for the real operator/admin submitting.",
    )
    p_submit_on_behalf.set_defaults(handler=_admin_submit_batch_on_behalf)

    # ── rate-cards subgroup ───────────────────────────────────────────
    p_rate_cards = sub.add_parser(
        "rate-cards",
        help="Rate-card catalog operations via the public service API.",
    )
    rate_cards_sub = p_rate_cards.add_subparsers(
        dest="rate_cards_op",
        required=True,
    )
    p_rc_sync_yibuapi = rate_cards_sub.add_parser(
        "sync-yibuapi",
        help="Sync YibuAPI official pricing into the service rate-card store.",
    )
    p_rc_sync_yibuapi.add_argument(
        "--source-url",
        default=None,
        help=(
            "Override the YibuAPI pricing endpoint. Defaults to the "
            "server's configured official pricing URL."
        ),
    )
    p_rc_sync_yibuapi.add_argument(
        "--group",
        default="default",
        help="YibuAPI group ratio to use when converting prices.",
    )
    p_rc_sync_yibuapi.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_rc_sync_yibuapi.set_defaults(handler=_sync_yibuapi_rate_cards)

    # ── secret-store subgroup ──────────────────────────────────────────
    p_ss = sub.add_parser(
        "secret-store",
        help=("SecretStore master-key rotation operations. See `loom admin secret-store --help`."),
    )
    ss_sub = p_ss.add_subparsers(dest="ss_op", required=True)

    p_ss_rewrap = ss_sub.add_parser(
        "rewrap",
        help=(
            "Walk all SecretStore refs and re-encrypt with the primary "
            "master key. Part of the online master-key rotation protocol. "
            "Run AFTER deploying the new key as a fallback in "
            "LOOM_SECRET_STORE_MASTER_KEYS."
        ),
    )
    p_ss_rewrap.add_argument(
        "--new-key",
        dest="new_key",
        default=None,
        help=(
            "Base64-encoded 32-byte key to rewrap to. Overrides the "
            "PRIMARY key in LOOM_SECRET_STORE_MASTER_KEYS. Normally "
            "omit this — the server uses the deployed primary."
        ),
    )
    p_ss_rewrap.add_argument(
        "--generate-new-key",
        dest="generate_new_key",
        action="store_true",
        default=False,
        help=(
            "Mint a fresh key, print it, and output the kubectl patch "
            "commands. Does NOT call the rewrap endpoint. Use this to "
            "start the rotation workflow."
        ),
    )
    p_ss_rewrap.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Print what would be done without calling the endpoint.",
    )
    p_ss_rewrap.add_argument(
        "--admin-actor",
        default=None,
        help=(
            "Sets `X-Loom-Admin-Actor`. Required when the logged-in "
            "bearer is an admin token (audit trail)."
        ),
    )
    p_ss_rewrap.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    p_ss_rewrap.set_defaults(handler=_rewrap_secret_store)

    args = parser.parse_args(argv)
    return cast(int, args.handler(args))
