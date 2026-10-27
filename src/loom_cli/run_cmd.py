"""`loom run` orchestrator — fans out trials over `loom_benchmarks` tasks.

Flow:
  1. Parse `--dataset` or `--task` -> list of LoadedTask
  2. Resolve `--agent` + `--model` -> patched TaskConfig per task
  3. Construct UpstreamDirectGatewayClient (or skip if agent is oracle)
  4. Bound by `--concurrency` asyncio.Semaphore, run each task through
     LocalRunner.run() in `asyncio.gather`
  5. Print result line per task (text or JSON)
  6. Retain results locally; warn about the deprecated upload option
  7. Exit 0 if every trial state == succeeded, else 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from uuid import uuid4

from loom.driver.base import Driver
from loom.driver.docker import DockerDriver
from loom.driver.fake import FakeDriver
from loom.models.result import TrialResult, TrialState
from loom.models.task import TaskConfig
from loom.models.types import ModelSpec
from loom_cli.config import LocalProvider, load_config
from loom_cli.local_object_store import LocalDiskObjectStore
from loom_cli.local_runner import LocalRunner
from loom_cli.model_spec import parse_model
from loom_cli.output import format_json_line, format_text_line
from loom_cli.task_loader import LoadedTask, load_tasks
from loom_cli.vllm_runner import (
    _LIVE_PROCESSES,
    MissingVLLMDependencyError,
    VLLMLaunchSpec,
    VLLMServerInfo,
    launch_vllm,
    model_slug,
    retain_one,
    stop_one,
)


def run(args: argparse.Namespace) -> int:
    return asyncio.run(_run_async(args))


async def _run_async(args: argparse.Namespace) -> int:
    """Top-level dispatcher. Single --model: today's behavior.
    Multiple --model: sequential or parallel based on the flag."""
    model_specs: list[str] = args.model or []

    if args.server_url or os.environ.get("LOOM_SERVER_URL"):
        sys.stderr.write(
            "warning: local result upload via --server-url or LOOM_SERVER_URL "
            "is deprecated and unsupported; results are not uploaded. "
            "Keep the files in --output-dir or use authenticated hosted commands "
            "to submit new trials.\n",
        )

    if args.parallel_models and len(model_specs) < 2:
        sys.stderr.write(
            "warning: --parallel-models has no effect with a single "
            "--model (it only applies when multiple --model specs "
            "are passed).\n",
        )

    completed: list[TrialResult] = []
    try:
        if len(model_specs) == 0:
            return await _run_one_model(args, model=None, output_dir=None, completed=completed)
        if len(model_specs) == 1:
            return await _run_one_model(
                args, model=_run_model_spec(args, model_specs[0]),
                output_dir=None, completed=completed,
            )
        if args.parallel_models:
            return await _run_parallel(args, model_specs, completed=completed)
        return await _run_sequential(args, model_specs, completed=completed)
    finally:
        _maybe_write_tb2_report(args, completed)


def _run_model_spec(args: argparse.Namespace, spec: str) -> ModelSpec:
    if not getattr(args, "local_server", None):
        return parse_model(spec)
    if not spec.strip():
        raise SystemExit("--local-server requires --model with a nonempty upstream model ID")
    if spec.startswith(("hf:", "/", "~", "./", "../")):
        raise SystemExit(
            "--local-server is mutually exclusive with --model hf:<id> or a weights path; "
            "it targets a server you already started.",
        )
    # This is an upstream model identifier, not Loom's provider/name syntax.
    return ModelSpec(provider="local", name=f"_inline/{spec}")


async def _run_one_model(
    args: argparse.Namespace,
    model: ModelSpec | None,
    output_dir: Path | None,
    *,
    completed: list[TrialResult] | None = None,
) -> int:
    """Run all selected tasks for one model.

    Accepts an explicit `output_dir` override so the multi-model
    sequential loop can bucket each model's outputs under
    `<base-output-dir>/<slug>/`. When None, falls back to
    `args.output_dir` (the original single-model behavior).
    """
    # Use the explicit override or fall back to args.output_dir.
    if output_dir is None:
        output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    workdir = output_dir / "_tasks"
    workdir.mkdir(parents=True, exist_ok=True)

    if args.dataset:
        dataset = args.dataset
        task_filter = None
    else:
        assert args.task is not None
        dataset = args.task.split("/", 1)[0]
        task_filter = args.task

    try:
        tasks = list(load_tasks(
            dataset=dataset, split=args.split,
            task_filter=task_filter, workdir=workdir,
        ))
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if not tasks:
        print(
            f"no tasks selected (dataset={dataset!r} task={task_filter!r})",
            file=sys.stderr,
        )
        return 2

    cfg = load_config()
    tokens = dict(cfg.tokens)
    for env_key, provider in (
        ("ANTHROPIC_API_KEY", "anthropic"),
        ("OPENAI_API_KEY", "openai"),
        ("GOOGLE_API_KEY", "google"),
    ):
        val = os.environ.get(env_key)
        if val:
            tokens[provider] = val

    # --local-server: ad-hoc dispatch against an already-running
    # OpenAI-compatible server. We register a transient `_inline`
    # local provider and rewrite the model spec to address it. No
    # vLLM subprocess; the server is the user's responsibility.
    inline_local_name: str | None = None
    if getattr(args, "local_server", None):
        if model is not None and model.provider in ("hf", "file"):
            raise SystemExit(
                "--local-server is mutually exclusive with "
                "--model hf:<id> or --model /path/ — those "
                "launch their own vLLM; --local-server targets a "
                "server you already started.",
            )
        if model is None:
            raise SystemExit(
                "--local-server requires --model <model_id>",
            )
        inline_local_name = "_inline"
        cfg.local_providers[inline_local_name] = LocalProvider(
            base_url=args.local_server,
            api_key=(
                getattr(args, "local_api_key", None)
                or os.environ.get("LOOM_LOCAL_API_KEY")
            ),
        )
        # _run_model_spec already preserved this iteration's complete upstream
        # identifier under the inline provider, including bare model names.

    # If the user passed `--model hf:<id>` or a path, we launch a
    # vLLM subprocess on those weights, register it as a transient
    # local provider, and rewrite `model` to point at it. The
    # trial(s) then dispatch through the existing local-provider
    # path. Cleanup owns the process until a single-model --keep-alive
    # run completes normally and explicitly transfers that ownership.
    auto_local_name: str | None = None
    _maybe_warn_unused_vllm_flags(args, model)
    info: VLLMServerInfo | None = None
    keep_alive = bool(getattr(args, "keep_alive", False)) and len(args.model or []) == 1
    completed_normally = False
    try:
        if model is not None and model.provider in ("hf", "file"):
            try:
                info = launch_vllm(VLLMLaunchSpec(
                    model=model.name,
                    port=getattr(args, "vllm_port", 0),
                    host=getattr(args, "vllm_host", "127.0.0.1"),
                    gpu_memory_utilization=getattr(args, "gpu_memory_utilization", 0.90),
                    tensor_parallel_size=getattr(args, "tensor_parallel_size", 1),
                    max_model_len=getattr(args, "max_model_len", None),
                    enforce_eager=getattr(args, "enforce_eager", False),
                    keep_alive=keep_alive,
                    log_path=output_dir / "vllm.log" if keep_alive else None,
                ))
            except MissingVLLMDependencyError as exc:
                print(str(exc), file=sys.stderr)
                return 2
            except RuntimeError as exc:
                # Launch failure: missing path, no free port, vLLM startup
                # crash, or 300s health-check timeout. Surface it as a clean
                # CLI error rather than a stacktrace.
                print(f"vLLM launch failed: {exc}", file=sys.stderr)
                return 2
            # Register under a fixed name so `loom config show` doesn't pick
            # this up. The dispatch contract reads `cfg.local_providers`;
            # we mutate cfg in-process only.
            auto_local_name = "_auto_vllm"
            cfg.local_providers[auto_local_name] = LocalProvider(
                base_url=info.base_url, api_key=None,
            )
            # Rewrite the spec so `_call_local` resolves the right server +
            # the right model id (vLLM may have shortened HF org/name into
            # something different in served_model_name).
            model = ModelSpec(
                provider="local",
                name=f"{auto_local_name}/{info.served_model_name}",
            )
        store = LocalDiskObjectStore(root=output_dir / "_store")

        sem = asyncio.Semaphore(max(1, int(args.concurrency)))
        completed_trials = completed if completed is not None else []

        incomplete_trial = False

        async def _one(loaded: LoadedTask) -> int:
            nonlocal incomplete_trial
            async with sem:
                patched = _patch_agent(loaded.task_config, args.agent, model)
                driver_factory = await _driver_factory(
                    args.backend,
                    patched,
                    task_dir=loaded.task_dir,
                    task_checksum=loaded.checksum,
                    gpu=getattr(args, "gpu", None),
                )
                runner = LocalRunner(
                    trial_id=uuid4(),
                    team_id=uuid4(),
                    task_config=patched,
                    task_checksum=loaded.checksum,
                    task_dir=loaded.task_dir,
                    driver_factory=driver_factory,
                    output_dir=output_dir,
                    object_store=store,
                    upstream_gateway_tokens=tokens,
                    anthropic_client=a_client,
                    openai_client=o_client,
                    google_client=g_client,
                    local_providers=cfg.local_providers,
                )
                try:
                    result = await runner.run()
                except Exception as exc:
                    incomplete_trial = True
                    print(
                        f"{loaded.task_config.task.id} [ERROR] "
                        f"{type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )
                    return 1
                line = (
                    format_json_line(result) if args.json_output
                    else format_text_line(result)
                )
                completed_trials.append(result)
                print(line, flush=True)
                return 0 if result.state == TrialState.SUCCEEDED else 1

        async with _build_sdk_clients(tokens) as (a_client, o_client, g_client):
            running = [asyncio.create_task(_one(t)) for t in tasks]
            try:
                exit_codes = await asyncio.gather(*running)
            finally:
                # gather propagates setup failures without joining siblings.
                # Their cleanup must finish before shared SDK clients close.
                for task in running:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*running, return_exceptions=True)
        completed_normally = not incomplete_trial
    finally:
        if info is not None:
            proc = next((p for p in _LIVE_PROCESSES if p.pid == info.pid), None)
            if proc is not None:
                if keep_alive and completed_normally and proc.poll() is None:
                    retain_one(proc)
                    print(
                        f"vLLM retained: PID {info.pid}; endpoint {info.base_url}; "
                        f"log {output_dir / 'vllm.log'}. Stop it with kill {info.pid}.",
                        file=sys.stderr,
                    )
                else:
                    stop_one(proc)

    return 0 if all(c == 0 for c in exit_codes) else 1


async def _run_sequential(
    args: argparse.Namespace, model_specs: list[str],
    *, completed: list[TrialResult] | None = None,
) -> int:
    """Run each model against the full dataset in turn. Outputs
    bucket under <output-dir>/<slug>/<trial-id>/. Exit code is
    max(all exit codes) so one failure surfaces."""
    base_output = Path(args.output_dir)
    base_output.mkdir(parents=True, exist_ok=True)

    exit_codes: list[int] = []
    for spec_str in model_specs:
        slug = model_slug(spec_str)
        sub_output = base_output / slug
        sys.stderr.write(
            f"\n→ [{slug}] starting trials for --model {spec_str}\n",
        )
        try:
            model = _run_model_spec(args, spec_str)
            code = await _run_one_model(args, model=model, output_dir=sub_output, completed=completed)
        except SystemExit:
            raise  # let argparse / explicit user-error exits propagate
        except Exception as exc:
            sys.stderr.write(
                f"→ [{slug}] failed: {exc}\n",
            )
            code = 2
        exit_codes.append(code)

    return max(exit_codes) if exit_codes else 0


async def _run_parallel(
    args: argparse.Namespace, model_specs: list[str],
    *, completed: list[TrialResult] | None = None,
) -> int:
    """Run all models concurrently; outputs still bucket by slug.

    Caller must ensure enough GPU memory for all models
    simultaneously (no auto partitioning at v1).
    """
    base_output = Path(args.output_dir)
    base_output.mkdir(parents=True, exist_ok=True)

    async def _one_model(spec_str: str, model: ModelSpec) -> int:
        slug = model_slug(spec_str)
        sub_output = base_output / slug
        sys.stderr.write(
            f"→ [{slug}] starting (parallel) for --model {spec_str}\n",
        )
        try:
            return await _run_one_model(
                args, model=model, output_dir=sub_output, completed=completed,
            )
        except SystemExit:
            raise
        except Exception as exc:
            sys.stderr.write(f"→ [{slug}] failed: {exc}\n")
            return 2

    # Validate before creating tasks: SystemExit inside an asyncio Task stops
    # the loop before sibling cleanup can finish.
    models = [(spec, _run_model_spec(args, spec)) for spec in model_specs]
    running = [asyncio.create_task(_one_model(spec, model)) for spec, model in models]
    try:
        codes = await asyncio.gather(*running)
    finally:
        # Report publication and process cleanup must wait for every model,
        # including siblings interrupted by a launch/configuration failure.
        for task in running:
            if not task.done():
                task.cancel()
        await asyncio.gather(*running, return_exceptions=True)
    return max(codes) if codes else 0


def _maybe_write_tb2_report(
    args: argparse.Namespace, trials: list[TrialResult],
) -> None:
    """Emit a TB-2 canonical BenchmarkResults JSON when --tb2-report is set.

    Imported lazily so the loom-benchmark-terminal-bench-2 package isn't
    a hard dependency unless the flag is actually used.
    """
    target: Path | None = getattr(args, "tb2_report", None)
    if target is None:
        return
    try:
        from loom_benchmark_terminal_bench_2.report import to_tb2_report
    except ImportError as exc:
        print(
            f"warning: --tb2-report set but loom-benchmark-terminal-bench-2 "
            f"is not installed: {exc}",
            file=sys.stderr,
        )
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(to_tb2_report(trials), indent=2, sort_keys=True))


def _patch_agent(
    cfg: TaskConfig, agent_name: str, model: ModelSpec | None,
) -> TaskConfig:
    """Return a TaskConfig with agent.name swapped to `agent_name` and
    agent.model swapped to `model` if provided."""
    update_kwargs: dict[str, object] = {"name": agent_name}
    if model is not None:
        update_kwargs["model"] = model
    new_agent = cfg.agent.model_copy(update=update_kwargs)
    return cfg.model_copy(update={"agent": new_agent})


class UnsupportedFlagError(ValueError):
    """Raised when a CLI flag is incompatible with the chosen backend."""


_VALID_BACKENDS = {"docker", "fake", "modal"}


def build_driver(*, backend: str, image: str, gpu: str | None = None) -> Driver:
    """Construct a Driver for ``backend``.

    Raises:
        UnsupportedFlagError: when ``--gpu`` is set with a backend that does
            not support GPU passthrough (docker / fake today).
        ValueError: for an unknown backend value.
        ModalConfigError: when ``--backend modal`` is chosen but Modal
            credentials env vars are not set.
    """
    if backend not in _VALID_BACKENDS:
        raise ValueError(
            f"Unknown --backend {backend!r}. "
            f"Valid: {sorted(_VALID_BACKENDS)}",
        )
    if backend == "fake":
        if gpu is not None:
            raise UnsupportedFlagError(
                "--gpu is not supported by --backend fake. "
                "Use --backend modal for GPU trials.",
            )
        return FakeDriver()
    if backend == "docker":
        if gpu is not None:
            raise UnsupportedFlagError(
                "--gpu is not supported by --backend docker. "
                "Use --backend modal for GPU trials.",
            )
        return DockerDriver(image=image)
    # backend == "modal"
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver
    return ModalDriver(image=image, gpu=gpu, config=ModalConfig.from_env())


async def _driver_factory(
    backend: str,
    cfg: TaskConfig,
    *,
    task_dir: Path,
    task_checksum: str,
    gpu: str | None = None,
) -> Callable[[], Driver]:
    """Return a zero-arg factory for the chosen backend.

    Each call to the returned factory constructs a fresh Driver instance.
    Heavy SDK config (for example, ``ModalConfig``) is captured
    in the closure so env vars are read once per ``loom run`` invocation.

    Resolves the task image up front via
    `loom.driver.task_image.resolve_task_image` — same path the worker
    uses (#232). When a task ships a Dockerfile (no `docker_image`), the
    image is built from the materialized bundle and cached under a
    deterministic tag; subsequent driver instantiations within the same
    `loom run` hit the docker daemon's cached tag.
    """
    if backend not in _VALID_BACKENDS:
        raise SystemExit(f"unknown backend: {backend!r}")
    if backend == "fake":
        if gpu is not None:
            raise UnsupportedFlagError(
                f"--gpu is not supported by --backend {backend}. "
                "Use --backend modal for GPU trials.",
            )
        return FakeDriver

    # Real backends need a concrete image. Build from the task's
    # Dockerfile when present; otherwise fall back to docker_image or
    # the alpine default. resolve_task_image performs the cached build
    # via docker-py, so we only pay the build cost once per task per
    # `loom run` invocation.
    from loom.driver.task_image import resolve_task_image

    image = await resolve_task_image(
        task_config=cfg,
        task_dir=task_dir,
        task_checksum=task_checksum,
    )

    if backend == "docker":
        if gpu is not None:
            raise UnsupportedFlagError(
                f"--gpu is not supported by --backend {backend}. "
                "Use --backend modal for GPU trials.",
            )
        return lambda: DockerDriver(image=image, workspace=cfg.environment.workdir)
    # backend == "modal"
    from loom_drivers.modal.config import ModalConfig
    from loom_drivers.modal.driver import ModalDriver
    modal_cfg = ModalConfig.from_env()
    return lambda: ModalDriver(image=image, gpu=gpu, config=modal_cfg)


@asynccontextmanager
async def _build_sdk_clients(
    tokens: dict[str, str],
) -> AsyncIterator[tuple[object, object, object]]:
    """Own configured SDK clients for one run, including partial startup failure."""
    async with AsyncExitStack() as stack:
        a_client: object = None
        o_client: object = None
        g_client: object = None
        if tokens.get("anthropic"):
            import anthropic

            a_client = await stack.enter_async_context(
                anthropic.AsyncAnthropic(api_key=tokens["anthropic"]),
            )
        if tokens.get("openai"):
            import openai

            o_client = await stack.enter_async_context(
                openai.AsyncOpenAI(api_key=tokens["openai"]),
            )
        if tokens.get("google"):
            from google import genai

            # Local token.google is a Gemini Developer API credential. Do not
            # let ambient Vertex/Enterprise settings change that contract.
            google_client = stack.enter_context(genai.Client(
                api_key=tokens["google"], vertexai=False, enterprise=False,
            ))
            await stack.enter_async_context(google_client.aio)
            g_client = google_client
        yield a_client, o_client, g_client


def _maybe_warn_unused_vllm_flags(
    args: argparse.Namespace, model: ModelSpec | None,
) -> None:
    """If the user set vLLM-launcher flags but the model spec doesn't
    trigger a launch (e.g. they passed `--model anthropic/...
    --tensor-parallel-size 2`), the flags are silently ignored. Warn
    once on stderr so users notice the typo."""
    if model is not None and model.provider in ("hf", "file"):
        return
    flagged: list[str] = []
    if getattr(args, "vllm_port", 0):
        flagged.append("--vllm-port")
    if getattr(args, "vllm_host", "127.0.0.1") != "127.0.0.1":
        flagged.append("--vllm-host")
    if getattr(args, "tensor_parallel_size", 1) != 1:
        flagged.append("--tensor-parallel-size")
    if getattr(args, "max_model_len", None) is not None:
        flagged.append("--max-model-len")
    if abs(getattr(args, "gpu_memory_utilization", 0.90) - 0.90) > 1e-9:
        flagged.append("--gpu-memory-utilization")
    if getattr(args, "enforce_eager", False):
        flagged.append("--enforce-eager")
    if getattr(args, "keep_alive", False):
        flagged.append("--keep-alive")
    if flagged:
        sys.stderr.write(
            "warning: ignoring vLLM-launcher flags "
            f"({', '.join(flagged)}) because --model is not "
            "`hf:<id>` or a local weights path. These flags only apply when "
            "Loom manages the vLLM lifecycle.\n",
        )
