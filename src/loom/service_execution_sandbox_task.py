"""Trusted phase entry points for Harbor (Terminus-2) and Oracle against
private native Pod sandboxes.

Only the controller sees the immutable task bundle and durable outputs. The
agent task and fresh verifier receive their inputs over different Unix sockets.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import shlex
import shutil
import signal
import sys
import tempfile
import tomllib
from collections.abc import AsyncIterator, Callable
from glob import escape
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from uuid import UUID

import httpx
from pydantic import TypeAdapter, ValidationError

from loom.attempt_deadline import AttemptDeadline
from loom.driver.service_sandbox import SandboxRPCError, ServiceSandboxDriver
from loom.errors import AgentError, DriverError, exception_info
from loom.execution_requirements import ALL_GUEST_EXECUTION_CAPABILITIES
from loom.harbor_verifier_script import VERIFIER_SCRIPT_PATH, offline_verifier_run_sh_bytes
from loom.hosted_harness import (
    HARNESS_SETUP_PHASE,
    MAX_SETUP_CACHE_BYTES,
    HarnessSetup,
    HostedHarnessSpec,
    hosted_harness,
    workspace_controller_phases,
)
from loom.models.capabilities import Capabilities
from loom.models.networking import NetworkPolicy, hosted_http_egress
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.models.verifier import VerifierResult
from loom.service_execution_oracle import (
    ORACLE_SOLUTION_PATHS,
    oracle_usage,
    remove_oracle_solution,
    run_oracle,
)
from loom.service_execution_task import (
    ServiceExecutionTaskError,
    _safe_workspace_path,
    _write_json_atomic,
    task_artifact_paths,
)
from loom.service_execution_terminus2 import TASK_IMAGE_TOOLS_REQUIRED, run_terminus2
from loom.service_execution_terminus_trace import parse_terminus_events, terminus_usage
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths
from loom.trial.workspace import (
    WorkspaceStagingPolicy,
    materialize_workspace,
    refuse_planted_private_paths,
)
from loom.trial.workspace_references import (
    export_workspace_references,
    import_workspace_with_references,
)
from loom.trial.workspace_snapshot import (
    _export_workspace_archive,
    _import_workspace_archive,
    _strip_private_entries,
    _validate_workspace_archive,
)
from loom.verifier_runtime import resolve_verifier_env_mode

_PRIVATE_PATHS = ("tests/**", "verifier/**", "solution/**", "upstream-task.toml", ".loom/**")
_POLICY = WorkspaceStagingPolicy(_PRIVATE_PATHS, _PRIVATE_PATHS, ())
_PRIVATE_VERIFIER_INPUT_ROOT = PurePosixPath("/loom/verifier/task")


def _uses_harbor_private_inputs(workspace: Path, task: TaskConfig) -> bool:
    """Recognize only our complete immutable wrapper, never custom scripts."""
    if task.verifier.args.get("script_path") != VERIFIER_SCRIPT_PATH:
        return False
    wrapper = _safe_workspace_path(workspace, VERIFIER_SCRIPT_PATH)
    expected = offline_verifier_run_sh_bytes()
    return wrapper.is_file() and wrapper.stat().st_size == len(expected) and wrapper.read_bytes() == expected


def _agent_input_exclusions(task: TaskConfig) -> tuple[str, ...]:
    """Keep declared build-only inputs in the controller, not the agent upload.

    A dedicated context directory is supplied to the image builder. Uploading
    it again can restore setup scripts or duplicate verifier tests the image
    deliberately omitted. A root context is ambiguous: preserve runtime assets
    there and omit only its Dockerfile. Image-only tasks retain ordinary inputs.
    """
    excluded = [".loom/**"]
    env = task.environment
    if env.dockerfile is not None:
        excluded.append(escape(env.dockerfile.as_posix()))
        context = env.docker_build_context
        if context is not None and context != PurePosixPath("."):
            excluded.append(escape(context.as_posix()) + "/**")
    for sidecar in env.sidecars:
        if sidecar.dockerfile is not None:
            excluded.append(escape(sidecar.dockerfile.as_posix()))
            context = sidecar.docker_build_context
            if context is not None and context != PurePosixPath("."):
                excluded.append(escape(context.as_posix()) + "/**")
    return tuple(excluded)


_NETWORK_POLICY_ADAPTER: TypeAdapter[NetworkPolicy] = TypeAdapter(NetworkPolicy)


def _frozen_network_policy() -> NetworkPolicy:
    raw = os.environ.get("LOOM_EFFECTIVE_NETWORK_POLICY_JSON")
    if raw is None:
        raise ServiceExecutionTaskError("effective_network_policy_unavailable")
    try:
        return _NETWORK_POLICY_ADAPTER.validate_json(raw)
    except ValidationError:
        raise ServiceExecutionTaskError("effective_network_policy_invalid") from None


def sandbox_driver(role: str, task: TaskConfig) -> ServiceSandboxDriver:
    network_policy = _frozen_network_policy()
    command_environment = {}
    guest = (task.environment.execution_requirements is not None
             and bool(ALL_GUEST_EXECUTION_CAPABILITIES.intersection(task.environment.execution_requirements.capabilities)))
    max_transfer = 256 * 1024 * 1024
    if guest:
        try:
            max_transfer = int(os.environ.get("LOOM_SANDBOX_MAX_TRANSFER_BYTES", ""))
        except ValueError:
            raise ServiceExecutionTaskError("guest_transfer_limit_invalid") from None
        if not 0 < max_transfer <= 10 * 1024**3:
            raise ServiceExecutionTaskError("guest_transfer_limit_invalid")
    if hosted_http_egress(network_policy) is not None:
        from urllib.parse import urlsplit

        proxy = os.environ.get("LOOM_TASK_EGRESS_PROXY", "")
        url = urlsplit(proxy)
        if url.scheme != "http" or url.hostname != "127.0.0.1" or not url.port:
            raise ServiceExecutionTaskError("task_egress_runtime_unavailable")
        if guest:
            proxy = url._replace(netloc=f"10.0.2.2:{url.port}").geturl()
        command_environment = {name: proxy for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY")}
        command_environment.update(no_proxy="localhost,127.0.0.1,::1", NO_PROXY="localhost,127.0.0.1,::1")
    return ServiceSandboxDriver(
        Path(f"/loom/sandboxes/{role}/sandbox.sock"),
        capabilities=Capabilities(
            os="linux", cpu_arch="x86_64", gpu_vendor="none",
            network_policies=frozenset({"gateway-only", "web-allowlist", "public-web"}), dynamic_network_policy=False,
            mounted_fs=False, resource_modes=frozenset({"limit"}),
        ),
        network_policy=network_policy,
        command_environment=command_environment,
        max_transfer_bytes=max_transfer,
    )


async def _execution_identity(gateway: str) -> tuple[UUID, UUID]:
    # No caller-selected identity. Go authenticates this request as its own Pod.
    from urllib.parse import urlsplit

    url = urlsplit(gateway)
    if url.scheme != "http" or url.hostname not in {"127.0.0.1", "::1"}:
        raise ServiceExecutionTaskError("execution requires a loopback Gateway")
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        async with client.stream("GET", gateway.rstrip("/") + "/internal/loom/llm-calls") as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 16 * 1024 * 1024:
                    raise ServiceExecutionTaskError("execution ledger exceeds limit")
    envelope = json.loads(body)
    if envelope.get("step_id") != "agent":
        raise ServiceExecutionTaskError("execution ledger has an invalid step")
    return UUID(envelope["trial_id"]), UUID(envelope["team_id"])


# Workspace harness phases this controller implements (see `run_agent`).
CONTROLLER_PHASES = frozenset({"terminus-2", "oracle", "codex"})


def _workspace_spec(trial: TrialConfig) -> HostedHarnessSpec:
    spec = hosted_harness(trial.agent_name)
    if spec is None or not spec.workspace or spec.controller_phase not in CONTROLLER_PHASES:
        raise ServiceExecutionTaskError("selected agent has no workspace controller phase")
    return spec


# The runtime broker replaces any caller credential with the Pod's workload
# token, so installed agents only ever hold this fixed placeholder.
WORKLOAD_PROXY_API_KEY = "loom_workload_proxy"


def installed_agent_model_environment(
    spec: HostedHarnessSpec, *, base_url_env: str, api_key_env: str,
) -> dict[str, str]:
    """Model access for an installed agent inside the task sandbox (#2310).

    The agent reaches the Pod-local runtime broker over the shared loopback.
    The broker forwards only canonical model routes, only during the agent
    phase and before its deadline, with the Pod's own workload token. That
    token is bound to the Trial's Provider Connection and never leaves the
    trusted controller. Revocation is the end of the agent phase. Provider,
    control-plane and grading credentials are never passed to the sandbox.
    """
    from urllib.parse import urlsplit

    if spec.model != "required" or spec.gateway_protocol is None:
        raise ServiceExecutionTaskError("selected harness does not use a model")
    gateway = os.environ.get("LOOM_GATEWAY_URL", "")
    url = urlsplit(gateway)
    if url.scheme != "http" or url.hostname not in {"127.0.0.1", "::1"} or not url.port or url.path.strip("/"):
        raise ServiceExecutionTaskError("installed agent model access requires the loopback broker")
    # Both OpenAI protocols are served under /v1 (chat/completions, responses).
    return {base_url_env: gateway.rstrip("/") + "/v1", api_key_env: WORKLOAD_PROXY_API_KEY}


def _setup_proxy_environment() -> dict[str, str]:
    """The runtime's loopback egress proxy, which admits only install sources
    while the setup phase runs (#2310)."""
    from urllib.parse import urlsplit

    proxy = os.environ.get("LOOM_TASK_EGRESS_PROXY", "")
    url = urlsplit(proxy)
    if url.scheme != "http" or url.hostname != "127.0.0.1" or not url.port:
        raise ServiceExecutionTaskError("harness setup egress is unavailable")
    environment = {name: proxy for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY")}
    environment.update(no_proxy="localhost,127.0.0.1,::1", NO_PROXY="localhost,127.0.0.1,::1")
    return environment


# Controller-workspace files the runtime exchanges with the Gateway (#2310).
SETUP_CACHE_RESTORE = PurePosixPath(".loom/harness-cache/restore.tar.gz")
SETUP_CACHE_STORE = PurePosixPath(".loom/harness-cache/store.tar.gz")
SETUP_CACHE_OUTCOME = PurePosixPath(".loom/harness-cache/outcome.json")
_SANDBOX_CACHE_ARCHIVE = PurePosixPath("/tmp/loom-harness-cache.tar.gz")


def _remaining(deadline: AttemptDeadline | None, fallback: float) -> float:
    return deadline.remaining() if deadline else fallback


async def _check_install(driver: ServiceSandboxDriver, setup: HarnessSetup, timeout: float) -> bool:
    result = await driver.exec(shlex.join(setup.check), timeout_sec=timeout)
    return result.return_code == 0


async def _restore_cached_install(
    driver: ServiceSandboxDriver, workspace: Path, setup: HarnessSetup, deadline: AttemptDeadline | None,
) -> str:
    """Restore a fetched entry and prove it works; any problem means reinstall."""
    archive = workspace / SETUP_CACHE_RESTORE
    if not archive.is_file():
        return "miss"
    assert setup.install_root is not None
    root = shlex.quote(setup.install_root)
    remote = shlex.quote(str(_SANDBOX_CACHE_ARCHIVE))
    try:
        await driver.upload(archive, _SANDBOX_CACHE_ARCHIVE)
        extracted = await driver.exec(
            f"rm -rf -- {root} && mkdir -p -- {root} && tar -xzf {remote} -C {root}; status=$?; rm -f -- {remote}; exit $status",
            timeout_sec=_remaining(deadline, setup.timeout_seconds),
        )
        if extracted.return_code == 0 and await _check_install(driver, setup, _remaining(deadline, setup.timeout_seconds)):
            return "hit"
    except DriverError:
        pass
    await driver.exec(f"rm -rf -- {root}", timeout_sec=_remaining(deadline, setup.timeout_seconds))
    return "restore_rejected"


async def _archive_install(
    driver: ServiceSandboxDriver, workspace: Path, setup: HarnessSetup, deadline: AttemptDeadline | None,
) -> str:
    """Archive a fresh, checked install for the runtime to store."""
    assert setup.install_root is not None
    remote = shlex.quote(str(_SANDBOX_CACHE_ARCHIVE))
    target = workspace / SETUP_CACHE_STORE
    try:
        packed = await driver.exec(
            f"tar -czf {remote} -C {shlex.quote(setup.install_root)} .",
            timeout_sec=_remaining(deadline, setup.timeout_seconds),
        )
        if packed.return_code != 0:
            return "store_unavailable"
        target.parent.mkdir(parents=True, exist_ok=True)
        await driver.download(_SANDBOX_CACHE_ARCHIVE, target)
    except (DriverError, OSError):
        target.unlink(missing_ok=True)
        return "store_unavailable"
    finally:
        await driver.exec(f"rm -f -- {remote}", timeout_sec=_remaining(deadline, setup.timeout_seconds))
    if not 0 < target.stat().st_size <= MAX_SETUP_CACHE_BYTES:
        target.unlink(missing_ok=True)
        return "store_unavailable"
    return "stored"


_SANDBOX_RELEASE_ARCHIVE = PurePosixPath("/tmp/loom-harness-release.tar.gz")
_ARCHIVE_DOWNLOAD = PurePosixPath(".loom/harness-cache/release.download")


async def _run_install_command(
    driver: ServiceSandboxDriver, task: TaskConfig, setup: HarnessSetup,
    environment: dict[str, str], deadline: AttemptDeadline | None,
) -> None:
    handle = await driver.exec_streaming(
        list(setup.install), env_vars=environment, cwd=task.environment.workdir,
        timeout_sec=_remaining(deadline, setup.timeout_seconds),
    )
    try:
        async def forward(stream: AsyncIterator[bytes], sink: BinaryIO) -> None:
            async for chunk in stream:
                sink.write(chunk)
                sink.flush()

        await asyncio.gather(
            forward(handle.stdout, sys.stdout.buffer), forward(handle.stderr, sys.stderr.buffer),
        )
        code = await handle.wait()
    except BaseException:
        await handle.kill()
        raise
    if code != 0:
        raise ServiceExecutionTaskError(f"harness setup failed with exit status {code}")


async def _install_pinned_archive(
    driver: ServiceSandboxDriver, workspace: Path, setup: HarnessSetup,
    environment: dict[str, str], deadline: AttemptDeadline | None,
) -> None:
    """Fetch and verify the release in trusted code, then extract it in the sandbox."""
    import base64
    import hashlib

    archive = setup.archive
    assert archive is not None and setup.install_root is not None
    local = workspace / _ARCHIVE_DOWNLOAD
    local.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha512()
    received = 0
    try:
        async with httpx.AsyncClient(
            proxy=environment["https_proxy"], trust_env=False, follow_redirects=False,
            timeout=httpx.Timeout(_remaining(deadline, setup.timeout_seconds), connect=30),
        ) as client, client.stream("GET", archive.url) as response:
            if response.status_code != 200:
                raise ServiceExecutionTaskError(f"harness archive download failed with HTTP {response.status_code}")
            with local.open("wb") as sink:
                async for chunk in response.aiter_bytes(1024 * 1024):
                    received += len(chunk)
                    if received > MAX_SETUP_CACHE_BYTES:
                        raise ServiceExecutionTaskError("harness archive exceeds the size limit")
                    digest.update(chunk)
                    sink.write(chunk)
    except httpx.HTTPError as exc:
        local.unlink(missing_ok=True)
        raise ServiceExecutionTaskError("harness archive download failed") from exc
    except BaseException:
        local.unlink(missing_ok=True)
        raise
    if "sha512-" + base64.b64encode(digest.digest()).decode() != archive.integrity:
        local.unlink(missing_ok=True)
        raise ServiceExecutionTaskError("harness archive integrity mismatch")
    print(f"harness archive verified ({received} bytes)", flush=True)
    root = shlex.quote(setup.install_root)
    remote = shlex.quote(str(_SANDBOX_RELEASE_ARCHIVE))
    try:
        await driver.upload(local, _SANDBOX_RELEASE_ARCHIVE)
    finally:
        local.unlink(missing_ok=True)
    extracted = await driver.exec(
        f"command -v tar >/dev/null && command -v gzip >/dev/null || {{ echo 'task image lacks tar/gzip' >&2; exit 127; }}; "
        f"rm -rf -- {root} && mkdir -p -- {root} && "
        f"tar -xzf {remote} -C {root} --strip-components={archive.strip_components}; "
        f"status=$?; rm -f -- {remote}; exit $status",
        timeout_sec=_remaining(deadline, setup.timeout_seconds),
    )
    sys.stderr.buffer.write(extracted.stderr)
    if extracted.return_code != 0:
        raise ServiceExecutionTaskError(f"harness archive extraction failed with exit status {extracted.return_code}")


async def run_setup(workspace: Path, task: TaskConfig, trial: TrialConfig) -> None:
    """Run the selected harness's pinned install inside the task sandbox.

    Only the install runs here: no task inputs are staged, no task command
    runs and no model call is possible (the broker refuses non-agent phases).
    Its output becomes this phase's logs; a non-zero exit is a setup failure.
    A cacheable install is first restored from the runtime-fetched entry for
    this team and task image when one exists, and archived after a fresh
    install otherwise.
    """
    spec = hosted_harness(trial.agent_name)
    if spec is None or not spec.workspace or spec.setup is None:
        raise ServiceExecutionTaskError("selected harness declares no setup")
    setup = spec.setup
    raw_deadline = os.environ.get("LOOM_EXECUTION_PHASE_DEADLINE")
    deadline = AttemptDeadline.from_wall_deadline(float(raw_deadline)) if raw_deadline else None
    environment = _setup_proxy_environment()
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    assert current is not None
    loop.add_signal_handler(signal.SIGTERM, current.cancel)
    driver = sandbox_driver("task-sandbox", task)
    # {"restore": hit|miss|restore_rejected, "store": stored|store_unavailable}
    outcome: dict[str, str] = {}
    try:
        await driver.start()
        if setup.cacheable:
            outcome["restore"] = await _restore_cached_install(driver, workspace, setup, deadline)
            if outcome["restore"] == "hit":
                return
        if setup.archive is not None:
            await _install_pinned_archive(driver, workspace, setup, environment, deadline)
        else:
            await _run_install_command(driver, task, setup, environment, deadline)
        if setup.cacheable:
            if not await _check_install(driver, setup, _remaining(deadline, setup.timeout_seconds)):
                raise ServiceExecutionTaskError("harness install check failed")
            outcome["store"] = await _archive_install(driver, workspace, setup, deadline)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        await driver.stop()
        if outcome:
            _write_json_atomic(workspace / SETUP_CACHE_OUTCOME, {
                "schema_version": "loom.harness-cache-outcome.v1", **outcome,
            })


class AgentTimeoutFinalizedError(TimeoutError):
    """Agent deadline reached; quiescence and verifier handoff completed safely."""


async def run_agent(workspace: Path, task: TaskConfig, trial: TrialConfig) -> None:
    raw_deadline = os.environ.get("LOOM_EXECUTION_PHASE_DEADLINE")
    deadline = AttemptDeadline.from_wall_deadline(float(raw_deadline)) if raw_deadline else None
    grace = float(os.environ.get("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "30"))
    if not math.isfinite(grace) or grace <= 0:
        raise ServiceExecutionTaskError("termination grace must be finite and positive")
    gateway = os.environ["LOOM_GATEWAY_URL"]
    driver = sandbox_driver("task-sandbox", task)
    output = workspace / ".loom/agent"
    output.mkdir(parents=True, exist_ok=True)
    trial_id = None
    agent_entered = False
    handoff_allowed = False
    services_retained = False
    driver_started = False
    lifecycle = task.environment.service_lifecycle
    spec = _workspace_spec(trial)
    solution_staged = False
    timed_out = False
    finalizing = False
    termination_signals = 0
    loop = asyncio.get_running_loop()
    phase_task = asyncio.current_task()

    def terminate() -> None:
        nonlocal termination_signals
        termination_signals += 1
        # The local deadline may already have initiated cleanup just before Go
        # delivers SIGTERM. Do not interrupt that same cleanup a second time.
        if termination_signals == 1 and finalizing and deadline is not None and deadline.reached:
            return
        if phase_task is not None:
            phase_task.cancel()

    if deadline is not None:
        loop.add_signal_handler(signal.SIGTERM, terminate)
    try:
        try:
            async with asyncio.timeout(deadline.remaining() if deadline else None):
                trial_id, team_id = await _execution_identity(gateway)
                await driver.start()
                driver_started = True
                # Only Oracle's own sandbox receives the reference solution.
                solution_staged = spec.stages_solution
                await materialize_workspace(
                    driver=driver, task_dir=workspace, dst=task.environment.workdir, policy=_POLICY,
                    excluded_paths=_agent_input_exclusions(task),
                    trusted_private_paths=ORACLE_SOLUTION_PATHS if spec.stages_solution else (),
                )
                if task.environment.preserve_acls:
                    from loom.trial.workspace_acls import require_acl_support

                    for root in (task.environment.workdir, *task.environment.mutable_paths):
                        await require_acl_support(driver, root)
                if lifecycle is not None and lifecycle.startup_command:
                    result = await driver.exec(
                        shlex.join(lifecycle.startup_command), cwd=task.environment.workdir,
                        timeout_sec=lifecycle.startup_timeout_sec,
                    )
                    _write_json_atomic(workspace / ".loom/service-startup.json", {
                        "return_code": result.return_code,
                        "stdout": result.stdout.decode("utf-8", errors="replace"),
                        "stderr": result.stderr.decode("utf-8", errors="replace"),
                        "truncated": result.truncated,
                    })
                    if result.return_code:
                        raise ServiceExecutionTaskError("environment service startup failed")
                    async with asyncio.timeout(lifecycle.readiness_timeout_sec):
                        await driver.run_healthcheck(lifecycle.readiness)
                instruction = _safe_workspace_path(
                    workspace, str(task.steps[0].instruction_file),
                ).read_text()
                agent_entered = True
                # Explicit dispatch per harness phase; unknown phases fail closed.
                if spec.controller_phase == "oracle":
                    await run_oracle(
                        driver=driver, task_dir=workspace, workspace=output, task_config=task,
                        trial_config=trial, trial_id=trial_id, deadline=deadline,
                    )
                elif spec.controller_phase == "terminus-2":
                    await run_terminus2(
                        driver=driver, workspace=output, task_config=task, trial_config=trial,
                        trial_id=trial_id, team_id=team_id, instruction=instruction, gateway_url=gateway,
                        deadline=deadline,
                    )
                elif spec.controller_phase == "codex":
                    from loom.service_execution_codex import run_codex
                    from loom.service_execution_terminus2 import LeaseGatewayClient

                    ledger = LeaseGatewayClient(gateway_url=gateway, trial_id=trial_id, team_id=team_id)
                    await run_codex(
                        driver=driver, workspace=output, task_config=task, trial_config=trial,
                        trial_id=trial_id, instruction=instruction, deadline=deadline,
                        model_environment=installed_agent_model_environment(
                            spec, base_url_env="OPENAI_BASE_URL", api_key_env="OPENAI_API_KEY",
                        ),
                        ledger=ledger.get_trial_llm_calls,
                    )
                else:
                    raise ServiceExecutionTaskError("no controller implements this harness phase")
                handoff_allowed = True
        except (TimeoutError, asyncio.CancelledError):
            if deadline is None or not deadline.reached or not agent_entered:
                raise
            timed_out = True
            handoff_allowed = True
        except Exception as exc:
            _write_json_atomic(output / "exception.json", exception_info(exc).model_dump(mode="json"))
            raise
        finally:
            finalizing = True
            # This budget permits only local accounting, quiescence and a
            # validated workspace snapshot. It never extends the agent deadline.
            remaining = max(0, deadline.monotonic_deadline + grace - loop.time()) if deadline else None
            async with asyncio.timeout(remaining):
                try:
                    if agent_entered:
                        try:
                            trace = output / "trajectory.jsonl"
                            if spec.trace_format == "oracle":
                                _write_json_atomic(output / "usage.json", oracle_usage())
                            elif spec.trace_format == "terminus" and trace.exists() and trial_id is not None:
                                events = parse_terminus_events(
                                    trace.read_bytes(), trial=trial, trial_id=trial_id,
                                )
                                _write_json_atomic(output / "usage.json", terminus_usage(events, trial))
                        finally:
                            in_place = resolve_verifier_env_mode(task, trial) == "shared"
                            if in_place and driver_started:
                                # Harbor shared mode does not reap before grading.
                                if (
                                    lifecycle is not None
                                    and handoff_allowed
                                    and lifecycle.readiness_scope == "startup_and_handoff"
                                ):
                                    async with asyncio.timeout(lifecycle.readiness_timeout_sec):
                                        await driver.run_healthcheck(lifecycle.readiness)
                            elif lifecycle is not None and handoff_allowed:
                                if lifecycle.readiness_scope == "startup_and_handoff":
                                    async with asyncio.timeout(lifecycle.readiness_timeout_sec):
                                        await driver.run_healthcheck(lifecycle.readiness)
                                await driver.pause_processes()
                            else:
                                await driver.stop_processes()
                    if solution_staged and driver_started:
                        # Before the snapshot, and before an in-place verifier
                        # refuses planted private paths.
                        await remove_oracle_solution(driver, task.environment.workdir)
                    if agent_entered:
                        archive = workspace / ".loom/workspace.tar"
                        await _export_workspace_archive(
                            driver, task.environment.workdir, archive,
                            preserve_acls=task.environment.preserve_acls,
                        )
                        await asyncio.to_thread(_strip_private_entries, archive, _POLICY)
                        await asyncio.to_thread(
                            _validate_workspace_archive, archive, _POLICY,
                            root=task.environment.workdir,
                            external_reference_files=frozenset(task.environment.workspace_reference_files),
                        )
                        if task.environment.mutable_paths:
                            await export_mutable_paths(
                                driver, task.environment.mutable_paths, workspace / ".loom/mutable-paths",
                                workdir=task.environment.workdir,
                                preserve_acls=task.environment.preserve_acls,
                                reference_files=task.environment.mutable_path_reference_files,
                                reference_symlinks=task.environment.reference_file_symlinks,
                            )
                        if task.environment.workspace_reference_files:
                            await export_workspace_references(
                                driver, archive, root=task.environment.workdir, policy=_POLICY,
                                reference_files=task.environment.workspace_reference_files,
                                reference_symlinks=task.environment.reference_file_symlinks,
                            )
                        for path in task_artifact_paths(workspace, task):
                            destination = _safe_workspace_path(workspace / ".loom/collected", path)
                            try:
                                await driver.download(task.environment.workdir / path, destination)
                            except (DriverError, FileNotFoundError):
                                print(f"task artifact unavailable: {path}", file=sys.stderr)
                        if lifecycle is not None and handoff_allowed and not in_place:
                            await driver.resume_processes()
                            services_retained = True
                finally:
                    if lifecycle is None or services_retained:
                        await driver.stop()
        # A successful agent may use the grace period for its snapshot. Once
        # that handoff completes, acknowledge the expired Go phase with 124 too.
        if timed_out or (agent_entered and deadline is not None and deadline.reached):
            raise AgentTimeoutFinalizedError("agent deadline reached; verifier handoff completed")
    finally:
        try:
            if lifecycle is not None and not services_retained:
                try:
                    # Cleanup has its own bounded RPC and also runs if startup,
                    # snapshot, or the finalization budget fails or is cancelled.
                    if driver_started:
                        await driver.stop_processes()
                finally:
                    await driver.stop()
        finally:
            if deadline is not None:
                loop.remove_signal_handler(signal.SIGTERM)


async def run_verifier(workspace: Path, task: TaskConfig, trial: TrialConfig) -> None:
    raw_deadline = os.environ.get("LOOM_EXECUTION_PHASE_DEADLINE")
    deadline = AttemptDeadline.from_wall_deadline(float(raw_deadline)) if raw_deadline else None
    grace = float(os.environ.get("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "30"))
    if not math.isfinite(grace) or grace <= 0:
        raise ServiceExecutionTaskError("termination grace must be finite and positive")
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    assert current is not None
    finalizing = False

    def begin_cleanup() -> None:
        nonlocal finalizing
        finalizing = True

    def terminate() -> None:
        if not finalizing:
            current.cancel()

    loop.add_signal_handler(signal.SIGTERM, terminate)
    try:
        async with asyncio.timeout(deadline.remaining() if deadline else None):
            await _run_verifier(workspace, task, trial, deadline=deadline, grace=grace,
                                begin_cleanup=begin_cleanup)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def stage_committed_workspace_archive(workspace: Path) -> Path:
    """Place the durable agent archive where the verifier sandbox import reads it."""
    target = workspace / ".loom/workspace.tar"
    if target.is_file():
        return target
    committed = workspace / "artifacts" / "workspace.tar"
    if not committed.is_file():
        raise ServiceExecutionTaskError("verifier handoff archive is missing")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(committed, target)
    return target


async def _run_verifier(
    workspace: Path, task: TaskConfig, trial: TrialConfig, *, deadline: AttemptDeadline | None, grace: float,
    begin_cleanup: Callable[[], None],
) -> None:
    in_place = resolve_verifier_env_mode(task, trial) == "shared"
    separate_private_inputs = (not in_place) and _uses_harbor_private_inputs(workspace, task)
    input_root = _PRIVATE_VERIFIER_INPUT_ROOT if separate_private_inputs else task.environment.workdir
    driver = sandbox_driver("task-sandbox" if in_place else "verifier-sandbox", task)
    driver_started = False
    failure: BaseException | None = None

    def retain_failure(operation: str, exc: BaseException) -> None:
        nonlocal failure
        if failure is None:
            failure = exc
        else:
            # Preserve the original phase error. Only the RPC adapter's fixed
            # reason codes are safe to include; arbitrary exception text can
            # carry commands, paths or credentials.
            detail = f": {exc}" if isinstance(exc, SandboxRPCError) else ""
            print(
                f"secondary verifier {operation} failure ({type(exc).__name__}){detail}"[:256],
                file=sys.stderr,
            )

    try:
        await driver.start()
        driver_started = True
        if in_place:
            try:
                await refuse_planted_private_paths(driver, task.environment.workdir)
            except RuntimeError as exc:
                raise ServiceExecutionTaskError(str(exc)) from exc
        await materialize_workspace(
            driver=driver, task_dir=workspace, dst=input_root,
            policy=_POLICY, phase="verifier",
            excluded_paths=(".loom/**",),
        )
        if not in_place:
            archive = stage_committed_workspace_archive(workspace)
            # The archive was validated by the agent phase before durable capture;
            # it stays in the private controller workspace between phases.
            if task.environment.workspace_reference_files:
                await import_workspace_with_references(
                    driver, archive, task.environment.workdir, policy=_POLICY,
                    preserve_acls=task.environment.preserve_acls,
                    reference_files=task.environment.workspace_reference_files,
                    reference_symlinks=task.environment.reference_file_symlinks,
                )
            else:
                await _import_workspace_archive(
                    driver, archive, task.environment.workdir, policy=_POLICY,
                    preserve_acls=task.environment.preserve_acls,
                )
            if task.environment.mutable_paths:
                await import_mutable_paths(
                    driver, task.environment.mutable_paths, workspace / ".loom/mutable-paths",
                    workdir=task.environment.workdir,
                    preserve_acls=task.environment.preserve_acls,
                    reference_files=task.environment.mutable_path_reference_files,
                    reference_symlinks=task.environment.reference_file_symlinks,
                )
        remote_output = (
            _PRIVATE_VERIFIER_INPUT_ROOT.parent / "output.json" if separate_private_inputs
            else task.environment.workdir / ".loom/verifier/output.json"
        )
        # The trusted file RPC creates parents under the runtime identity and
        # refuses symlinks in every component. Clear stale scoring bytes too;
        # an empty report remains invalid unless the verifier writes its result.
        with tempfile.NamedTemporaryFile() as empty_report:
            await driver.upload(Path(empty_report.name), remote_output)
        script_path = str(task.verifier.args["script_path"])
        if separate_private_inputs:
            script_path = str(input_root / script_path)
        result = await driver.exec(
            "/bin/sh " + shlex.quote(script_path),
            cwd=task.environment.workdir,
            env={"LOOM_TASK_DIR": str(input_root),
                 "LOOM_VERIFIER_OUTPUT": str(remote_output)},
            timeout_sec=(trial.override_verifier_timeout_sec or task.verifier.timeout_sec)
            * trial.verifier_timeout_multiplier,
        )
        sys.stdout.buffer.write(result.stdout)
        sys.stderr.buffer.write(result.stderr)
        if result.return_code != 0:
            retain_failure("exec", ServiceExecutionTaskError("isolated verifier process failed"))
        # Capture already produced reports before cleanup can fail. These are
        # partial evidence until both validation and cleanup succeed; returning
        # a reward never changes a failed phase into successful execution.
        output = workspace / ".loom/verifier/output.json"
        try:
            await driver.download(remote_output, output)
            # Numeric zero is valid; missing/invalid feedback is not.
            VerifierResult.model_validate_json(output.read_bytes())
        except Exception as exc:
            retain_failure("report", exc)
        try:
            await driver.download(PurePosixPath("/logs/verifier/ctrf.json"), output.with_name("ctrf.json"))
        except (DriverError, FileNotFoundError):
            # CTRF is an optional native report, not another acceptance gate.
            print("optional verifier CTRF report unavailable", file=sys.stderr)
    except BaseException as exc:
        retain_failure("execution", exc)
    finally:
        begin_cleanup()

        async def cleanup_verifier() -> None:
            try:
                if driver_started:
                    await driver.stop_processes()
            except Exception as exc:
                retain_failure("stop_processes", exc)
            finally:
                try:
                    await driver.stop()
                except Exception as exc:
                    retain_failure("stop", exc)

        async def cleanup_service() -> None:
            # In-place grading already owns task-sandbox. Scraping it again
            # from a second driver races the verifier cleanup above.
            if in_place or task.environment.service_lifecycle is None:
                return
            service_driver = sandbox_driver("task-sandbox", task)
            try:
                await service_driver.start()
                await service_driver.stop_processes()
            except Exception as exc:
                retain_failure("service_cleanup", exc)
            finally:
                try:
                    await service_driver.stop()
                except Exception as exc:
                    retain_failure("service_disconnect", exc)

        remaining = min(grace, max(0, deadline.monotonic_deadline + grace - asyncio.get_running_loop().time())) if deadline else grace
        try:
            # A second Go SIGTERM at the same deadline cannot bypass cleanup.
            # The supervisor still has its independent hard kill/Pod teardown.
            async with asyncio.timeout(remaining):
                async with asyncio.TaskGroup() as cleanup_tasks:
                    cleanup_tasks.create_task(cleanup_verifier())
                    cleanup_tasks.create_task(cleanup_service())
        except BaseException as exc:
            retain_failure("cleanup_deadline", exc)
    if failure is not None:
        raise failure


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=(*workspace_controller_phases(), HARNESS_SETUP_PHASE, "verify-sandbox"))
    parser.add_argument("--workspace", required=True, type=Path)
    args = parser.parse_args()
    workspace = args.workspace
    if not workspace.is_absolute():
        raise ServiceExecutionTaskError("isolated execution requires an absolute workspace")
    # The interpreter runs from the trusted image directory. Some dependencies
    # add cwd to their import path or load .env; never chdir into task inputs.
    with (workspace / "task.toml").open("rb") as stream:
        task = normalize_steps(TaskConfig.model_validate(tomllib.load(stream)))
    trial = TrialConfig.model_validate_json(os.environ["LOOM_TASK_TRIAL_JSON"])
    setup_phase = args.phase == HARNESS_SETUP_PHASE
    agent_phase = args.phase not in {HARNESS_SETUP_PHASE, "verify-sandbox"}
    if agent_phase and args.phase != _workspace_spec(trial).controller_phase:
        raise ServiceExecutionTaskError("execution phase does not match the selected agent")
    phase = run_setup if setup_phase else run_agent if agent_phase else run_verifier
    try:
        asyncio.run(phase(workspace, task, trial))
    except AgentTimeoutFinalizedError:
        raise
    except Exception as exc:
        directory = "setup" if setup_phase else "agent" if agent_phase else "verifier"
        path = workspace / ".loom" / directory / "exception.json"
        # An agent failure is captured before cleanup, which can also fail.
        # Keep that original identity instead of replacing it during unwinding.
        if not path.exists():
            _write_json_atomic(path, exception_info(exc).model_dump(mode="json"))
        raise


if __name__ == "__main__":
    try:
        main()
    except AgentTimeoutFinalizedError:
        print("agent timed out; verified workspace handoff retained", file=sys.stderr)
        raise SystemExit(124) from None
    except Exception as exc:
        # HTTP exceptions can embed response/request details; never persist them.
        message = f"isolated execution failed ({type(exc).__name__})"
        if isinstance(exc, SandboxRPCError):
            message += f": {exc}"
        elif isinstance(exc, AgentError) and exc.args == (TASK_IMAGE_TOOLS_REQUIRED,):
            message += f": {TASK_IMAGE_TOOLS_REQUIRED}"
        print(message, file=sys.stderr)
        raise SystemExit(1) from None
