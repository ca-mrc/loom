"""vLLM launcher — start an OpenAI-compatible vLLM server on a HuggingFace
model id or a local weights path, wait for it to be healthy, expose its
base URL via the existing `local_providers` dispatch, and tear it down at
trial end.

User-facing UX:

    loom run --task ... --agent ... --model hf:meta-llama/Llama-3.1-8B-Instruct
    loom run --task ... --agent ... --model /path/to/weights/

Loom handles vLLM subprocess lifecycle (start, health probe, port
auto-pick, atexit + SIGTERM cleanup) so the user doesn't have to manage
a separate server window.

Inspired by Harbor's `harbor.vllm.manager.VLLMServerManager`
(https://github.com/[REDACTED_CONTRIBUTOR]/harbor) but scoped to the local
subprocess backend — batch scheduler + multi-launcher (ollama, llama.cpp) are
follow-up work.

The vLLM dep is optional (`pip install loom[vllm]`). Without it,
`launch_vllm()` raises a `MissingVLLMDependencyError` with a copy-paste
fix command.
"""

from __future__ import annotations

import atexit
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


class MissingVLLMDependencyError(RuntimeError):
    """vLLM not installed in the active env."""


@dataclass(frozen=True)
class VLLMLaunchSpec:
    """User-supplied launch knobs. All defaults match vLLM's own
    defaults except `port` (we auto-pick to avoid collisions when
    several `loom run` invocations share a machine)."""

    # Model identifier — either a HuggingFace org/name (`hf:` prefix
    # stripped by the CLI) or a local filesystem path.
    model: str
    # 0 = auto-pick a free port starting at 8234 (Harbor's default).
    port: int = 0
    # Bind to loopback by default — Loom is the only client, and
    # exposing model weights / inference to the LAN by accident on an
    # untrusted network is the kind of surprise we'd rather avoid.
    # Override with `--vllm-host 0.0.0.0` for explicit LAN access.
    host: str = "127.0.0.1"
    gpu_memory_utilization: float = 0.90
    tensor_parallel_size: int = 1
    max_model_len: int | None = None
    enforce_eager: bool = False
    extra_args: tuple[str, ...] = ()
    # Don't stop vLLM after the trial finishes — useful when iterating
    # on tasks against the same model.
    keep_alive: bool = False
    # Persistent launches need file-backed output after the invoking CLI exits.
    log_path: Path | None = None


@dataclass
class VLLMServerInfo:
    """What the launcher returns to the caller."""

    base_url: str          # `http://localhost:<port>/v1`
    served_model_name: str # what /v1/models returns
    pid: int               # so callers can also kill it themselves


# CLI-owned processes. A successfully retained single-model server is removed
# before exit; all remaining processes receive exit/signal cleanup.
_LIVE_PROCESSES: list[subprocess.Popen[bytes]] = []
_SIGNAL_HANDLERS_INSTALLED = False


def launch_vllm(spec: VLLMLaunchSpec) -> VLLMServerInfo:
    """Start a vLLM server matching `spec`, wait for healthy, return
    its address + served-model name. Raises:

    - `MissingVLLMDependencyError` if vLLM not installed.
    - `RuntimeError` if the model path doesn't exist or no port in
      the search window accepted the bind (or the server didn't
      become healthy within the timeout).
    """
    _ensure_vllm_installed()
    if spec.keep_alive and spec.log_path is None:
        raise ValueError("a persistent vLLM launch requires log_path")
    _install_cleanup_handlers()

    model = _resolve_model_path(spec.model)
    start_port = spec.port or 8234
    max_port = start_port + 1000
    port = start_port

    while port < max_port:
        cmd = _build_cmd(model, port, spec)
        logger.info("starting vLLM: %s", " ".join(cmd))
        sys.stderr.write(
            f"→ starting vLLM (model={model}, port={port}) ...\n",
        )
        if spec.keep_alive:
            assert spec.log_path is not None
            with spec.log_path.open("ab") as log:
                proc = subprocess.Popen(
                    cmd, stdin=subprocess.DEVNULL, stdout=log,
                    stderr=subprocess.STDOUT, start_new_session=True,
                )
        else:
            proc = subprocess.Popen(cmd)
        _LIVE_PROCESSES.append(proc)

        try:
            # Brief settling window: if vLLM is going to die from a bind
            # failure, it does so within a few seconds. Sample its
            # state, then continue with the normal health-probe flow.
            time.sleep(2.0)
            if proc.poll() is not None and proc.returncode != 0:
                # Likely a bind failure on this port; try the next.
                if proc in _LIVE_PROCESSES:
                    _LIVE_PROCESSES.remove(proc)
                sys.stderr.write(
                    f"→ vLLM exited code {proc.returncode} on port "
                    f"{port}; retrying next port\n",
                )
                port += 1
                continue

            base_url = f"http://localhost:{port}/v1"
            _wait_for_ready(base_url, proc, timeout_sec=300.0)
            served = _query_served_model_name(base_url)
        except BaseException:
            stop_one(proc)
            raise

        info = VLLMServerInfo(
            base_url=base_url, served_model_name=served, pid=proc.pid,
        )
        sys.stderr.write(
            f"✓ vLLM ready (served_model_name={served}, base_url={base_url})\n",
        )
        return info

    raise RuntimeError(
        f"no port between {start_port} and {max_port} accepted vLLM; "
        "see stderr above for individual failure reasons.",
    )


def stop_all() -> None:
    """Terminate every vLLM subprocess started in this session. Safe to
    call multiple times. Honored by atexit + signal handlers."""
    while _LIVE_PROCESSES:
        _stop_process(_LIVE_PROCESSES.pop())


def model_slug(spec: str) -> str:
    """Derive a stable, filesystem-safe slug from any model spec.

    Used for output bucketing under `<output-dir>/<slug>/<trial-id>/`
    when N>1 models, and for `loom serve --name` defaults.
    """
    # Strip CLI prefixes / trailing separators we don't care about.
    body = spec.removeprefix("hf:").rstrip("/")
    # HF ids and paths share the same "take the last segment" rule.
    if "/" in body:
        body = body.rsplit("/", 1)[-1]
    return body.lower().replace(".", "-").replace(" ", "-")


def stop_one(proc: subprocess.Popen[bytes]) -> None:
    """Stop one specific vLLM subprocess and remove it from the live
    list. Safe if the process was never registered (no-op) or already
    exited. Used by the sequential-load loop when one model finishes
    and the next is about to launch.
    """
    if proc in _LIVE_PROCESSES:
        _LIVE_PROCESSES.remove(proc)
    _stop_process(proc)


def retain_one(proc: subprocess.Popen[bytes]) -> None:
    """Transfer a successfully used server out of CLI exit/signal cleanup."""
    _LIVE_PROCESSES.remove(proc)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _ensure_vllm_installed() -> None:
    if shutil.which("vllm") is None:
        raise MissingVLLMDependencyError(
            "vLLM is not installed in the active environment. Install "
            "with: `pip install loom[vllm]` or `pip install vllm`. "
            "Note that vLLM has GPU requirements — see "
            "https://docs.vllm.ai/en/latest/getting_started/installation.html.",
        )


def _resolve_model_path(spec_model: str) -> str:
    """For local-path specs, sanity-check the path exists. For HF ids,
    just return as-is — vLLM will resolve via the HuggingFace hub cache."""
    if spec_model.startswith(("/", "./", "../", "~")):
        path = Path(spec_model).expanduser().resolve()
        if not path.exists():
            raise RuntimeError(
                f"model path {path!s} does not exist. Pass an absolute "
                f"or relative path to a directory of weights, or use "
                f"`hf:<org>/<name>` for a HuggingFace model id.",
            )
        return str(path)
    return spec_model



def _build_cmd(model: str, port: int, spec: VLLMLaunchSpec) -> list[str]:
    """Construct the `vllm serve ...` argv."""
    args = [
        "vllm", "serve", model,
        "--host", spec.host,
        "--port", str(port),
        "--gpu-memory-utilization", str(spec.gpu_memory_utilization),
        "--tensor-parallel-size", str(spec.tensor_parallel_size),
    ]
    if spec.max_model_len is not None:
        args += ["--max-model-len", str(spec.max_model_len)]
    if spec.enforce_eager:
        args.append("--enforce-eager")
    args += list(spec.extra_args)
    return args


def _wait_for_ready(
    base_url: str, proc: subprocess.Popen[bytes], *, timeout_sec: float,
) -> None:
    """Poll `<base_url>/models` until 200 OK or timeout. Fails fast if
    the subprocess has died (vLLM startup failure → SIGKILL or normal
    exit — either way we surface a clear error rather than time out)."""
    url = f"{base_url}/models"
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"vLLM process exited prematurely with code "
                f"{proc.returncode} before /v1/models became reachable. "
                "Check stderr above for the vLLM startup error.",
            )
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(2.0)
    raise RuntimeError(
        f"vLLM did not become healthy within {timeout_sec:.0f}s at "
        f"{url}. The process is still running (PID {proc.pid}); shut "
        "it down manually with `kill ...` if needed.",
    )


def _query_served_model_name(base_url: str) -> str:
    """Read the first model id from `/v1/models`. vLLM defaults to the
    model path; users may also pass `--served-model-name X` via
    extra_args."""
    r = httpx.get(f"{base_url}/models", timeout=10.0)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        raise RuntimeError(
            f"vLLM /v1/models returned no entries at {base_url}. Server "
            "may be starting up — try waiting longer.",
        )
    name = data[0].get("id")
    if not isinstance(name, str):
        raise RuntimeError(
            f"vLLM /v1/models entry has no 'id' field: {data[0]!r}",
        )
    return name


def _stop_process(proc: subprocess.Popen[bytes]) -> None:
    """Graceful TERM → wait → KILL fallback. Synchronous so this works
    inside atexit + signal handlers (no asyncio loop available)."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=30)
            return
        except subprocess.TimeoutExpired:
            logger.warning("vLLM PID %d did not terminate in 30s; killing", proc.pid)
            proc.kill()
            proc.wait(timeout=10)
    except Exception as exc:  # pragma: no cover — best-effort cleanup
        logger.warning("failed to stop vLLM PID %d: %s", proc.pid, exc)


def _install_cleanup_handlers() -> None:
    """Register exit, termination and terminal-hangup cleanup once per process.

    Why no SIGINT handler: the default Python SIGINT raises
    `KeyboardInterrupt`, which propagates through `asyncio.run` and
    triggers the `try/finally` in `_run_async` (the right place to
    tear down vLLM and Docker together). Installing our own
    SIGINT handler here would short-circuit that path.

    SIGTERM and SIGHUP don't raise — atexit doesn't fire on them either.
    Stop CLI-owned servers before restoring their default signal behavior.
    This includes detached keep-alive launches whose ownership has not yet
    transferred after successful completion.
    """
    global _SIGNAL_HANDLERS_INSTALLED
    if _SIGNAL_HANDLERS_INSTALLED:
        return
    atexit.register(stop_all)

    def _termination_handler(signum: int, frame: Any) -> None:
        sys.stderr.write(
            f"\n→ caught {signal.Signals(signum).name}; tearing down "
            f"{len(_LIVE_PROCESSES)} vLLM server(s) ...\n",
        )
        stop_all()
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    try:
        signal.signal(signal.SIGTERM, _termination_handler)
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, _termination_handler)
    except ValueError:  # pragma: no cover — not the main thread
        pass
    _SIGNAL_HANDLERS_INSTALLED = True
