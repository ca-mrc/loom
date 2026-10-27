"""`loom config set|show` handlers.

Supported keys for `set`:
  - `token.<provider>` — upstream LLM API key
    (provider in {anthropic, openai, google})
  - `server_url`       — service URL for authenticated hosted commands
  - `local.<name>.base_url` — register a locally-served
    OpenAI-compatible LLM server (vLLM, ollama, llama.cpp,
    lm-studio). `<name>` is your chosen identifier
    (`loom run --model local/<name>/<model_id>`).
  - `local.<name>.api_key`  — optional bearer for the local server
    (most local servers run without auth; vLLM `--api-key` does
    require one).
"""

from __future__ import annotations

import argparse
import re
import sys

from loom_cli.config import LocalProvider, load_config, save_config

_VALID_PROVIDERS = frozenset({"anthropic", "openai", "google"})
_LOCAL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def dispatch(args: argparse.Namespace) -> int:
    sub = args.config_cmd
    if sub == "set":
        return _set(args.key, args.value)
    if sub == "show":
        return _show()
    if sub == "codegen":
        from loom_config.codegen import main as codegen_main
        return codegen_main(["--check"] if args.check else [])
    print(f"unknown config subcommand: {sub}", file=sys.stderr)
    return 2


def _set(key: str, value: str) -> int:
    cfg = load_config()
    if key.startswith("token."):
        provider = key.split(".", 1)[1]
        if provider not in _VALID_PROVIDERS:
            print(
                f"unknown key {key!r}: provider must be one of "
                f"{sorted(_VALID_PROVIDERS)}",
                file=sys.stderr,
            )
            return 2
        cfg.tokens[provider] = value
    elif key == "server_url":
        if cfg.server_url != value:
            # Login credentials belong to one environment, not the new target.
            cfg.auth_token = None
            cfg.auth_session_cookie = None
            cfg.auth_csrf_token = None
            cfg.auth_session_cookie_name = "loom_session"
        cfg.server_url = value
    elif key.startswith("local."):
        parts = key.split(".")
        if len(parts) != 3 or parts[2] not in ("base_url", "api_key"):
            print(
                f"unknown key {key!r}; local provider keys are "
                "`local.<name>.base_url` or `local.<name>.api_key`",
                file=sys.stderr,
            )
            return 2
        _, name, field = parts
        if not _LOCAL_NAME_RE.match(name):
            print(
                f"invalid local provider name {name!r}; must match "
                "[a-z0-9][a-z0-9_-]*",
                file=sys.stderr,
            )
            return 2
        existing = cfg.local_providers.get(name)
        if field == "base_url":
            if not value.strip():
                print(
                    "empty base_url rejected; pass a URL like "
                    "http://localhost:8000/v1",
                    file=sys.stderr,
                )
                return 2
            # Soft warning: most OpenAI-compatible local servers serve
            # under a `/v1` path prefix (vLLM, ollama, lm-studio all
            # default to it). A url like `http://localhost:8000` (no
            # `/v1`) usually means `_call_local` requests will 404.
            # Warn but don't reject — llama.cpp's `--port 8080`
            # default has no prefix, and someone might be running a
            # reverse-proxy at /.
            if "/v1" not in value:
                print(
                    f"warning: base_url {value!r} doesn't contain '/v1' — "
                    "most local servers (vLLM, ollama, lm-studio) need "
                    "it. Run `loom models test local/" + name + "` to "
                    "verify before launching a trial.",
                    file=sys.stderr,
                )
            if existing is None:
                cfg.local_providers[name] = LocalProvider(base_url=value)
            else:
                cfg.local_providers[name] = LocalProvider(
                    base_url=value, api_key=existing.api_key,
                )
        else:  # api_key
            if existing is None:
                print(
                    f"local provider {name!r} not registered; set "
                    f"`local.{name}.base_url URL` first",
                    file=sys.stderr,
                )
                return 2
            cfg.local_providers[name] = LocalProvider(
                base_url=existing.base_url, api_key=value,
            )
    else:
        print(
            f"unknown key {key!r}; supported: token.<provider>, "
            "server_url, local.<name>.base_url, local.<name>.api_key",
            file=sys.stderr,
        )
        return 2
    save_config(cfg)
    print(f"set {key}")
    return 0


def _redact(value: str) -> str:
    if len(value) <= 4:
        return "***"
    return f"{value[:2]}***{value[-2:]}"


def _show() -> int:
    cfg = load_config()
    if (
        not cfg.tokens
        and cfg.server_url is None
        and not cfg.local_providers
    ):
        print("(empty — run `loom config set token.<provider> <key>`)")
        return 0
    if cfg.tokens:
        print("tokens:")
        for provider, val in sorted(cfg.tokens.items()):
            print(f"  {provider} = {_redact(val)}")
    if cfg.server_url is not None:
        print(f"server_url = {cfg.server_url}")
    if cfg.local_providers:
        print("local_providers:")
        for name, p in sorted(cfg.local_providers.items()):
            print(f"  {name}.base_url = {p.base_url}")
            if p.api_key is not None:
                print(f"  {name}.api_key = {_redact(p.api_key)}")
    return 0
