"""`loom cluster bootstrap-secrets` — schema-driven Secret materialization.

Emits a single `kubectl create secret generic loom-secrets --from-literal=...`
line covering every secret-backed entry in the schema. Operator runs
the printed command; the CLI does NOT invoke kubectl directly so
secrets never live in the CLI's process memory longer than necessary.
"""
from __future__ import annotations

import shlex
import subprocess
from collections.abc import Mapping
from urllib.parse import urlsplit, urlunsplit

from loom_config.loader import Schema


def _rewrite_dsn_host_port(dsn: str, *, host: str, port: int) -> str:
    """Rewrite the netloc's host+port in a psycopg/SQLAlchemy DSN,
    preserving scheme, credentials, database name, and query parameters.

    Raises ValueError on malformed inputs (missing userinfo, missing
    host, non-URL input).
    """
    try:
        parsed = urlsplit(dsn)
    except ValueError as exc:
        raise ValueError(f"malformed DSN: {dsn!r}") from exc

    if not parsed.scheme or not parsed.scheme.startswith(("postgres", "postgresql")):
        raise ValueError(f"unsupported scheme in DSN: {dsn!r}")
    if parsed.username is None or parsed.password is None:
        raise ValueError(f"DSN missing userinfo (user:password@): {dsn!r}")
    if not parsed.hostname:
        raise ValueError(f"DSN missing host: {dsn!r}")

    netloc = f"{parsed.username}:{parsed.password}@{host}:{port}"
    return urlunsplit((
        parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment,
    ))


_PGBOUNCER_HOST = "loom-pgbouncer"
_PGBOUNCER_PORT = 6432

# Throwaway singleton admin bearer for `--smoke-defaults` (local/dev) only.
# `control-plane` and `loom-service` mount `loom-admin-secret`
# (key `secrets.toml`, TOML `[admin].token`) and `cluster up` preflight
# refuses to apply without it. The token satisfies AdminSecretVerifier's
# rule (prefix `loom_admin_` + >=32-char suffix). NEVER a real credential —
# real deploys provision `loom-admin-secret` out of band.
_SMOKE_ADMIN_TOKEN = "loom_admin_smoke-local-dev-not-for-production-use-0"


def _render_smoke_admin_secret_command(namespace: str) -> str:
    """Render the `kubectl create secret generic loom-admin-secret` command
    carrying a throwaway `secrets.toml` for smoke/local bring-up."""
    secrets_toml = f'[admin]\ntoken = "{_SMOKE_ADMIN_TOKEN}"\n'
    return " \\\n    ".join([
        "kubectl create secret generic loom-admin-secret",
        f"--namespace={namespace}",
        f"--from-literal=secrets.toml={shlex.quote(secrets_toml)}",
    ])


_SMOKE_DEFAULTS: Mapping[str, str] = {
    "step-jwt-signing-key":   "smoke-jwt-key-do-not-use-in-prod",
    "cp-db-url":              "postgresql+psycopg://loom:loom@loom-postgres:5432/loom",
    "gw-db-url":              "postgresql+psycopg://loom:loom@loom-postgres:5432/loom",
    "svc-db-url":             "postgresql+psycopg://loom:loom@loom-postgres:5432/loom",
    "minio-access-key":       "minioadmin",
    "minio-secret-key":       "minioadmin",
    "worker-token":           "smoke-worker-token",
    "anthropic-api-key":      "smoke-anthropic",
    "openai-api-key":         "smoke-openai",
    "google-api-key":         "smoke-google",
    "together-api-key":       "smoke-together",
    "batch-runner-cp-token":  "smoke-batch-cp-token",
    "secret-store-master-key": "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE",
    # infra_secrets: non-codegen secrets that read from loom-secrets
    "postgres-user":          "loom",
    "postgres-password":      "loom",
}


def _all_secret_keys(schema: Schema) -> set[str]:
    keys: set[str] = set()
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None:
            continue
        for svc in e.used_by:
            keys.add(e.secret_key_for(svc))
    keys.update(
        key
        for key, entry in schema.infra_secrets.items()
        if entry.bootstrap
    )
    return keys


def _value_for(key: str, *, smoke_defaults: bool, rotate: bool, schema: Schema) -> str:
    if smoke_defaults and key in _SMOKE_DEFAULTS:
        return _SMOKE_DEFAULTS[key]
    if rotate:
        for name in schema.service_config:
            e = schema.service_config[name]
            if e.secret is None or e.secret.generate is None:
                continue
            for svc in e.used_by:
                if e.secret_key_for(svc) == key:
                    return subprocess.check_output(
                        shlex.split(e.secret.generate), text=True,
                    ).strip()
        # infra_secrets entries
        if key in schema.infra_secrets:
            infra = schema.infra_secrets[key]
            if infra.generate:
                return subprocess.check_output(
                    shlex.split(infra.generate), text=True,
                ).strip()
    return "<EDIT_ME>"


def render_bootstrap_command(
    schema: Schema,
    namespace: str = "loom",
    smoke_defaults: bool = False,
    rotate: bool = False,
    pgbouncer_enabled: bool = False,
    admin_secret: bool = False,
) -> str:
    parts = ["kubectl create secret generic loom-secrets",
             f"--namespace={namespace}"]

    # Collect direct-URL values so we can derive pool URLs afterwards.
    resolved_direct_urls: dict[str, str] = {}

    for key in sorted(_all_secret_keys(schema)):
        # Pool keys are emitted in a separate pass below when pgbouncer is
        # enabled; skip them here regardless so they never get an <EDIT_ME>
        # placeholder or appear twice.
        if key.endswith("-db-url-pool"):
            continue
        val = _value_for(key, smoke_defaults=smoke_defaults, rotate=rotate, schema=schema)
        parts.append(f"--from-literal={key}={shlex.quote(val)}")
        if key.endswith("-db-url"):
            resolved_direct_urls[key] = val

    if pgbouncer_enabled:
        # Derive pool keys from their sibling direct-URL keys.  The pool
        # secret key is simply the direct key with "-pool" appended.
        for direct_key in sorted(resolved_direct_urls):
            pool_key = f"{direct_key}-pool"
            direct_url = resolved_direct_urls[direct_key]
            if smoke_defaults:
                try:
                    pool_value = _rewrite_dsn_host_port(
                        direct_url,
                        host=_PGBOUNCER_HOST,
                        port=_PGBOUNCER_PORT,
                    )
                except ValueError:
                    pool_value = "<EDIT_ME>"
            else:
                # Real-deploy: emit a shell substitution so the operator
                # only edits the direct URL; bootstrap derives the pool URL
                # via the loom CLI helper.
                pool_value = f"$(loom cluster derive-pool-dsn {shlex.quote(direct_url)})"
            parts.append(f"--from-literal={pool_key}={shlex.quote(pool_value)}")

    command = " \\\n    ".join(parts)

    # Opt-in (`--admin-secret`, smoke only): also emit the singleton admin
    # secret so a local/dev `eval "$(...)"` satisfies the `cluster up`
    # preflight in one shot. Off by default so callers that create
    # `loom-admin-secret` themselves (the cluster/staging smoke workflows,
    # real deploys) don't collide with a duplicate. Emitted as a second
    # command (newline-separated) so `eval` runs both.
    if smoke_defaults and admin_secret:
        command += "\n" + _render_smoke_admin_secret_command(namespace)

    return command
