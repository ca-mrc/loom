"""`python -m loom_cli` + `loom` console script entry point."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import cast

from dotenv import load_dotenv

from loom_cli import __version__

_LOG = logging.getLogger(__name__)


def _find_dotenv_from_cwd() -> Path | None:
    """Find .env from CWD upward without crossing a project boundary."""
    current = Path.cwd().resolve()
    home = Path.home().resolve()
    for directory in (current, *current.parents):
        dotenv_path = directory / ".env"
        if dotenv_path.is_file():
            return dotenv_path
        if (directory / ".git").exists() or directory == home:
            break
    return None


def _load_dotenv_from_cwd() -> None:
    dotenv_path = _find_dotenv_from_cwd()
    if dotenv_path is None:
        return
    load_dotenv(dotenv_path=dotenv_path, override=False)
    _LOG.debug("loaded .env from %s", dotenv_path)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="loom",
        description=(
            "Run local evaluations and manage Loom datasets, providers, "
            "service-mode evaluations, workers, and deployments."
        ),
        epilog="For isolated credentials: loom --context NAME COMMAND ... (put --context before the command).",
    )
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run a benchmark or single task")
    g = p_run.add_mutually_exclusive_group(required=True)
    g.add_argument("--dataset", help="Benchmark slug (e.g. humaneval)")
    g.add_argument(
        "--task",
        help=(
            "Single task id in `<dataset-slug>/<instance-id>` form. The "
            "instance-id portion may itself contain slashes — e.g. "
            "`humaneval/HumanEval/0`, `swe-bench-verified/django__django-12345`."
        ),
    )
    p_run.add_argument("--split", default="test")
    p_run.add_argument("--agent", required=True,
                       help="Agent name (oracle / direct-completion / claude-code / ...)")
    p_run.add_argument(
        "--model", action="append", default=None,
        help=(
            "Model spec. Repeatable — pass --model N times to compare N "
            "models on the same dataset.\n"
            "Shapes:\n"
            "  <provider>/<name>           cloud or registered local server "
            "(e.g. anthropic/claude-opus-4-7, local/vllm/Llama-3.1-8B)\n"
            "  hf:<org>/<name>             HuggingFace model id; Loom "
            "starts vLLM on it (e.g. hf:meta-llama/Llama-3.1-8B-Instruct)\n"
            "  /path/ or ~/path/ or ./path/  local weights dir; Loom starts "
            "vLLM on it\n"
            "  <model_id>                  any string, when --local-server "
            "is also set"
        ),
    )
    p_run.add_argument(
        "--parallel-models", dest="parallel_models", action="store_true",
        help=(
            "With multiple --model specs, launch all servers upfront and "
            "run trials in parallel. Default: sequential (load model → "
            "run all tasks → unload). --parallel-models requires GPU "
            "memory for all models simultaneously."
        ),
    )
    p_run.add_argument(
        "--local-server", dest="local_server", default=None,
        help=(
            "URL of an already-running OpenAI-compatible server (vLLM, "
            "ollama, llama.cpp, lm-studio). With this flag, --model is "
            "the upstream model id verbatim. Example: "
            "--local-server http://localhost:8000/v1 "
            "--model meta-llama/Llama-3.1-8B-Instruct"
        ),
    )
    p_run.add_argument(
        "--local-api-key", dest="local_api_key", default=None,
        help=(
            "Optional API key for --local-server (e.g. when vLLM was "
            "started with --api-key). Falls back to LOOM_LOCAL_API_KEY "
            "env var if unset."
        ),
    )
    p_run.add_argument(
        "--vllm-port", dest="vllm_port", type=int, default=0,
        help=(
            "vLLM port for hf:/path launches. 0 (default) auto-picks a "
            "free port starting at 8234."
        ),
    )
    p_run.add_argument(
        "--vllm-host", dest="vllm_host", default="127.0.0.1",
        help=(
            "vLLM bind host for hf:/path launches. Default 127.0.0.1 "
            "(loopback only); use 0.0.0.0 to expose on the LAN."
        ),
    )
    p_run.add_argument(
        "--gpu-memory-utilization", dest="gpu_memory_utilization",
        type=float, default=0.90,
        help="vLLM --gpu-memory-utilization (default 0.90).",
    )
    p_run.add_argument(
        "--tensor-parallel-size", dest="tensor_parallel_size",
        type=int, default=1,
        help="vLLM --tensor-parallel-size (default 1).",
    )
    p_run.add_argument(
        "--max-model-len", dest="max_model_len", type=int, default=None,
        help="vLLM --max-model-len. Default: model's max.",
    )
    p_run.add_argument(
        "--enforce-eager", dest="enforce_eager", action="store_true",
        help="vLLM --enforce-eager (disable CUDA graph; debug only).",
    )
    p_run.add_argument(
        "--keep-alive", dest="keep_alive", action="store_true",
        help=(
            "For hf:/path launches, leave vLLM running after the trial "
            "completes (useful when iterating on tasks against the "
            "same model). Only meaningful for single-`--model` runs; "
            "multi-model loops always tear down each iteration's server."
        ),
    )
    p_run.add_argument("--backend", default="docker",
                       choices=("docker", "fake", "modal"),
                       help="Driver backend")
    p_run.add_argument("--gpu", type=str, default=None,
                       help=(
                           "GPU spec (Modal only). Example: --gpu A10, "
                           "--gpu H100:2. Rejected for other backends."
                       ))
    p_run.add_argument("--concurrency", type=int, default=1)
    p_run.add_argument("--output-dir", type=Path, default=Path("./runs"))
    p_run.add_argument("--json", dest="json_output", action="store_true",
                       help="Emit JSON one-result-per-line instead of text")
    p_run.add_argument("--server-url", default=None,
                       help="If set, also POST results to this Control Plane")
    p_run.add_argument("--tb2-report", dest="tb2_report", type=Path, default=None,
                       help=(
                           "Path to write a Terminal-Bench-2.0 canonical "
                           "BenchmarkResults JSON file. Emitted alongside "
                           "Loom's native ATIF. Meaningful primarily for "
                           "--dataset terminal-bench-2."
                       ))
    p_run.set_defaults(handler=_run_handler)

    p_config = sub.add_parser("config", help="Manage CLI config")
    config_sub = p_config.add_subparsers(dest="config_cmd", required=True)
    p_config_set = config_sub.add_parser("set")
    p_config_set.add_argument("key")
    p_config_set.add_argument("value")
    config_sub.add_parser("show")
    p_config_codegen = config_sub.add_parser(
        "codegen",
        help="Regenerate _generated.py + cluster-config.example.toml from schema.",
    )
    p_config_codegen.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if any committed artifact drifts from schema.",
    )
    p_config.set_defaults(handler=_config_handler)

    # `datasets` is registered as a help-only stub here so it appears in
    # `loom --help`. The real subcommand surface (list/show/install/
    # refresh-catalog plus filters) is owned by loom_cli.datasets_cmd.dispatch
    # which receives raw argv from main() via a pre-route before argparse sees
    # the subcommand.
    sub.add_parser(
        "datasets",
        help="Discover, publish, register, audit, and activate datasets",
        add_help=False,
    )
    sub.add_parser(
        "agents",
        help="Inspect agent catalog readiness and sandbox runtime dependencies",
        add_help=False,
    )

    # `loom service {up,down,status}` — one-liner wrapper around
    # docker-compose + migrations + test-data seeding for the dev stack.
    from loom_cli.service_cmd import add_service_subparser
    add_service_subparser(sub)

    from loom_cli.dev_cmd import add_dev_subparser
    add_dev_subparser(sub)

    # `loom models {list,test}` — inspect provider config + sanity-check
    # local LLM servers (vLLM, ollama, llama.cpp, lm-studio).
    from loom_cli.models_cmd import add_models_subparser
    add_models_subparser(sub)

    # The commands below are dispatched before argparse in main() (check
    # `if raw and raw[0] == "<cmd>"` blocks). Stub parsers here are
    # help-only so they appear in `loom --help`; add_help=False prevents
    # double-handling when main() re-routes to the real dispatch function.
    sub.add_parser(
        "auth",
        help="Authenticate with a Loom server (login/logout/whoami)",
        add_help=False,
    )
    sub.add_parser(
        "providers",
        help="Manage service-mode provider connections and models",
        add_help=False,
    )
    sub.add_parser(
        "harbor-baselines",
        help="Manage restricted external Harbor baselines",
        add_help=False,
    )
    sub.add_parser(
        "tasksets",
        help="Manage team TaskSets (submit/status/rebuild/delete/list)",
        add_help=False,
    )
    sub.add_parser(
        "eval",
        help="Submit, inspect, diagnose, and export service-mode evaluations",
        add_help=False,
    )
    sub.add_parser(
        "pipeline",
        help="Submit and inspect fixed official-Recipe Pipelines",
        add_help=False,
    )
    sub.add_parser(
        "resources",
        help="Inspect concurrent task slots and resource-pool pressure",
        add_help=False,
    )
    sub.add_parser(
        "cluster",
        help="Manage Kubernetes deployments and protected rollouts",
        add_help=False,
    )
    sub.add_parser(
        "admin",
        help=(
            "Operate credentials, environment state, worker pools, batches, "
            "and rate cards"
        ),
        add_help=False,
    )
    sub.add_parser(
        "worker",
        help="Inspect worker-host trial-image cache and setup admission",
        add_help=False,
    )
    sub.add_parser(
        "qa",
        help="Run agent and benchmark compatibility matrices",
        add_help=False,
    )

    # `loom serve <spec> --name X` — foreground vLLM launcher that
    # auto-registers the server as local/<name> and removes the entry on
    # Ctrl-C / SIGTERM.
    p_serve = sub.add_parser(
        "serve",
        help=(
            "Launch a vLLM server in the foreground and register it "
            "as a local provider; Ctrl-C to stop."
        ),
    )
    p_serve.add_argument(
        "model_spec",
        help=(
            "Model spec: `hf:<org>/<name>` or an absolute/relative "
            "path to weights (e.g. /data/checkpoints/my-tune/)."
        ),
    )
    p_serve.add_argument(
        "--name", default=None,
        help=(
            "Registration name under `local.<NAME>` in config. "
            "Defaults to a sanitized slug of the model id."
        ),
    )
    p_serve.add_argument(
        "--vllm-port", dest="vllm_port", type=int, default=0,
        help="vLLM port (0 = autopick from 8234)",
    )
    p_serve.add_argument(
        "--vllm-host", dest="vllm_host", default="127.0.0.1",
        help=(
            "vLLM bind host (default 127.0.0.1; 0.0.0.0 for LAN)."
        ),
    )
    p_serve.add_argument(
        "--gpu-memory-utilization", dest="gpu_memory_utilization",
        type=float, default=0.90,
    )
    p_serve.add_argument(
        "--tensor-parallel-size", dest="tensor_parallel_size",
        type=int, default=1,
    )
    p_serve.add_argument(
        "--max-model-len", dest="max_model_len", type=int, default=None,
    )
    p_serve.add_argument(
        "--enforce-eager", dest="enforce_eager", action="store_true",
    )
    p_serve.set_defaults(handler=_serve_handler)

    return p


def _run_handler(args: argparse.Namespace) -> int:
    from loom_cli.run_cmd import run

    return run(args)


def _config_handler(args: argparse.Namespace) -> int:
    from loom_cli.config_cmd import dispatch

    return dispatch(args)


def _serve_handler(args: argparse.Namespace) -> int:
    from loom_cli.serve_cmd import serve as serve_handler

    return asyncio.run(serve_handler(args))


def main(argv: list[str] | None = None) -> int:
    _load_dotenv_from_cwd()
    raw = list(sys.argv[1:] if argv is None else argv)
    from loom_cli.contexts import selected_context

    context = None
    if raw and raw[0] == "--context":
        if len(raw) < 2:
            print("error: --context requires a name", file=sys.stderr)
            return 2
        context, raw = raw[1], raw[2:]
    elif raw and raw[0].startswith("--context="):
        context, raw = raw[0].partition("=")[2], raw[1:]
    try:
        with selected_context(context):
            return _dispatch(raw)
    except ValueError as exc:
        # Context validation errors are safe and contain no config values.
        if str(exc).startswith("context must"):
            print(f"error: {exc}", file=sys.stderr)
            return 2
        raise


def _dispatch(raw: list[str]) -> int:
    if raw and raw[0] == "datasets":
        from loom_cli.datasets_cmd import dispatch as datasets_dispatch
        return datasets_dispatch(raw[1:])
    if raw and raw[0] == "agents":
        from loom_cli.agents_cmd import dispatch as agents_dispatch
        return agents_dispatch(raw[1:])
    if raw and raw[0] == "auth":
        from loom_cli.auth_cmd import dispatch as auth_dispatch
        return auth_dispatch(raw[1:])
    if raw and raw[0] == "providers":
        from loom_cli.providers_cmd import dispatch as providers_dispatch
        return providers_dispatch(raw[1:])
    if raw and raw[0] == "harbor-baselines":
        from loom_cli.harbor_baselines_cmd import dispatch as baseline_dispatch
        return baseline_dispatch(raw[1:])
    if raw and raw[0] == "tasksets":
        from loom_cli.tasksets_cmd import dispatch as tasksets_dispatch
        return tasksets_dispatch(raw[1:])
    if raw and raw[0] == "eval":
        from loom_cli.eval_cmd import dispatch as eval_dispatch
        return eval_dispatch(raw[1:])
    if raw and raw[0] == "pipeline":
        from loom_cli.pipeline_cmd import dispatch as pipeline_dispatch
        return pipeline_dispatch(raw[1:])
    if raw and raw[0] == "resources":
        from loom_cli.resources_cmd import dispatch as resources_dispatch
        return resources_dispatch(raw[1:])
    if raw and raw[0] == "cluster":
        from loom_cli.cluster_cmd import dispatch as cluster_dispatch
        return cluster_dispatch(raw[1:])
    if raw and raw[0] == "admin":
        from loom_cli.admin_cmd import dispatch as admin_dispatch
        return admin_dispatch(raw[1:])
    if raw and raw[0] == "worker":
        from loom_cli.worker_cmd import dispatch as worker_dispatch
        return worker_dispatch(raw[1:])
    if raw and raw[0] == "qa":
        from loom_cli.qa_cmd import dispatch as qa_dispatch
        return qa_dispatch(raw[1:])
    parser = _build_parser()
    args = parser.parse_args(raw)
    return cast(int, args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
