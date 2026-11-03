"""`loom models <subcommand>` — inspect + sanity-check configured models.

Subcommands:
- `list` — show registered local providers + remote provider tokens
- `test local/<name>` — ping the local server's `/v1/models` endpoint,
  report reachable + which models it advertises. Catches typos in
  base_url + "I forgot to start the server" before a trial fails 60s in.
"""

from __future__ import annotations

import argparse
import sys

import httpx

from loom_cli.config import LocalProvider, load_config


def add_models_subparser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register `loom models {list,test}` on the top-level argparse."""
    p_models = sub.add_parser(
        "models",
        help="Inspect + sanity-check configured LLM providers",
        description=(
            "List configured providers + test reachability of local "
            "LLM servers (vLLM, ollama, llama.cpp, lm-studio)."
        ),
    )
    models_sub = p_models.add_subparsers(dest="models_cmd", required=True)

    p_list = models_sub.add_parser(
        "list",
        help="Show registered providers (remote tokens + local servers)",
    )
    p_list.set_defaults(handler=_list)

    p_test = models_sub.add_parser(
        "test",
        help="Ping a configured local server (model_spec: local/<name>)",
    )
    p_test.add_argument(
        "model_spec",
        help=(
            "Local-provider name to test, e.g. `local/vllm`. Hits the "
            "server's /v1/models endpoint with a 5s timeout."
        ),
    )
    p_test.set_defaults(handler=_test)


def _list(_args: argparse.Namespace) -> int:
    cfg = load_config()
    print("Remote providers (API-key based):")
    for name in ("anthropic", "openai", "google"):
        present = name in cfg.tokens and bool(cfg.tokens[name])
        marker = "✓" if present else "✗"
        print(f"  {marker} {name:10}  "
              f"({'configured' if present else 'no token'})")
    print()
    print("Local providers (OpenAI-compatible servers):")
    if not cfg.local_providers:
        print(
            "  (none — register one with "
            "`loom config set local.<name>.base_url URL`)",
        )
    else:
        for name, p in sorted(cfg.local_providers.items()):
            auth = "with api_key" if p.api_key else "no api_key"
            print(f"  • {name:12}  {p.base_url}  ({auth})")
    return 0


def _test(args: argparse.Namespace) -> int:
    spec = args.model_spec
    # Accept both `local/<name>` and `<name>` for convenience.
    if spec.startswith("local/"):
        name = spec[len("local/"):]
    else:
        name = spec
    if "/" in name:
        name = name.split("/", 1)[0]
    cfg = load_config()
    provider = cfg.local_providers.get(name)
    if provider is None:
        registered = sorted(cfg.local_providers.keys()) or ["(none)"]
        print(
            f"local provider {name!r} not registered. "
            f"Run `loom config set local.{name}.base_url URL` to register "
            f"it. Currently registered: {registered}.",
            file=sys.stderr,
        )
        return 2

    rc, models = _probe(provider)
    if rc != 0:
        return rc
    print(f"✓ {name} reachable at {provider.base_url}")
    print(f"  models advertised by /v1/models: {len(models)}")
    for m in models[:20]:
        print(f"    • {m}")
    if len(models) > 20:
        print(f"    ...and {len(models) - 20} more")
    print()
    print("Run a trial against it with:")
    print(f"  loom run --model local/{name}/<model_id> ...")
    return 0


def _probe(provider: LocalProvider) -> tuple[int, list[str]]:
    """Return (exit_code, list_of_model_ids). Exit 0 on success."""
    url = provider.base_url.rstrip("/") + "/models"
    headers = {}
    if provider.api_dialect == "anthropic":
        from loom.anthropic_protocol import anthropic_api_base_url
        url = anthropic_api_base_url(provider.base_url) + "/models"
        headers["anthropic-version"] = "2023-06-01"
        if provider.api_key:
            headers["x-api-key"] = provider.api_key
    elif provider.api_key:
        headers["Authorization"] = f"Bearer {provider.api_key}"
    try:
        resp = httpx.get(url, headers=headers, timeout=5.0)
    except httpx.HTTPError as exc:
        print(
            f"could not reach {url}: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        print(
            "  is the server running? typical local-server start commands:",
            file=sys.stderr,
        )
        print("    vLLM:     vllm serve <model>", file=sys.stderr)
        print("    ollama:   ollama serve  (then `ollama pull <model>`)", file=sys.stderr)
        print(
            "    llama.cpp: ./server -m <model>.gguf --port 8080",
            file=sys.stderr,
        )
        return 1, []
    if resp.status_code != 200:
        print(
            f"GET {url} returned {resp.status_code}: {resp.text[:200]!r}",
            file=sys.stderr,
        )
        if resp.status_code == 401:
            print(
                "  auth failed — set the api_key with "
                "`loom config set local.<name>.api_key ...`",
                file=sys.stderr,
            )
        return 1, []
    try:
        body = resp.json()
        data = body.get("data", body if isinstance(body, list) else [])
        return 0, [str(m.get("id", m)) for m in data]
    except Exception as exc:
        print(
            f"{url} returned non-JSON or unexpected shape: {exc}",
            file=sys.stderr,
        )
        return 1, []
