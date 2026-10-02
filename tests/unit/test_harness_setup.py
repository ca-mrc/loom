"""#2310: an installed harness's pinned install runs in a setup phase that may
reach only its declared sources, and is closed before the agent starts."""

from __future__ import annotations

import asyncio
from pathlib import Path, PurePosixPath

import pytest

import loom.hosted_harness as hosted
import loom.service_execution_sandbox_task as controller
from loom.driver.base import ExecHandle
from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT
from loom.hosted_harness import (
    HARNESS_SETUP_PHASE,
    SANDBOX_CONTROLLER_MODULE,
    HarnessSetup,
    HostedHarnessSpec,
    InstallSource,
)
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.service_execution_materialization import (
    automatic_service_execution_rejections,
    compile_service_execution_plan,
)
from loom.service_execution_task import ServiceExecutionTaskError
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs

_SETUP = HarnessSetup(
    install=("/bin/sh", "-c", "npm install -g @example/agent@1.2.3"),
    sources=(InstallSource("registry.npmjs.org"),),
    timeout_seconds=1200,
)
_INSTALLED = HostedHarnessSpec(
    name="installed-agent", execution_kind="workspace", controller_module=SANDBOX_CONTROLLER_MODULE,
    controller_phase="installed-agent", controller_image="harness-controller", model="required",
    gateway_protocol="openai-chat-completions", trace_format="terminus",
    required_driver_capabilities=frozenset({"exec", "exec_streaming", "upload", "download"}),
    setup=_SETUP,
)


@pytest.fixture
def _registered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hosted, "HOSTED_HARNESSES", hosted._index(
        (*{spec.name: spec for spec in hosted.HOSTED_HARNESSES.values()}.values(), _INSTALLED),
    ))


def _trial() -> TrialConfig:
    return TrialConfig(agent_name="installed-agent", agent_model=ModelSpec(provider="openai", name="gpt-5"))


# --- spec ---------------------------------------------------------------------


@pytest.mark.parametrize(("kwargs", "message"), [
    ({"install": ()}, "install command"),
    ({"install": ("",)}, "install command"),
    ({"sources": ()}, "install sources"),
    ({"timeout_seconds": 0}, "timeout"),
    ({"timeout_seconds": 3600}, "timeout"),
])
def test_setup_declaration_is_bounded(kwargs: dict, message: str) -> None:
    base = {"install": ("true",), "sources": (InstallSource("pypi.org"),), "timeout_seconds": 60}
    with pytest.raises(ValueError, match=message):
        HarnessSetup(**{**base, **kwargs})


def test_setup_requires_a_streaming_task_sandbox_harness() -> None:
    from dataclasses import replace

    with pytest.raises(ValueError, match="exec_streaming"):
        replace(_INSTALLED, required_driver_capabilities=frozenset({"exec", "upload", "download"}))


# --- plan ---------------------------------------------------------------------


@pytest.mark.usefixtures("_registered")
def test_plan_freezes_a_setup_phase_and_its_install_sources() -> None:
    task, _, profile = _inputs()

    plan = compile_service_execution_plan(
        task=task, trial=_trial(), profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )

    assert [p.role for p in plan.setup] == ["setup"]
    assert plan.setup[0].argv[3:5] == (SANDBOX_CONTROLLER_MODULE, HARNESS_SETUP_PHASE)
    assert plan.setup[0].timeout_seconds == 1200
    assert plan.setup_egress is not None
    assert [d.host for d in plan.setup_egress.destinations] == ["registry.npmjs.org"]
    # The task stays gateway-only: setup sources never become task egress.
    assert plan.task_egress is None
    assert plan.effective_network_policy is not None and plan.effective_network_policy.kind == "gateway-only"
    assert TASK_EGRESS_OUTPUT in plan.output_declarations
    assert any(o.relative_path == "diagnostics/setup-exception.json" for o in plan.output_declarations)
    # The sandbox's per-process ceiling covers the install deadline.
    task_sandbox = next(s for s in plan.sidecars if s.role_name == "task-sandbox")
    assert int(task_sandbox.argv[task_sandbox.argv.index("--exec-timeout-seconds") + 1]) >= 1200
    assert "setup_egress" in plan.canonical_payload()


@pytest.mark.usefixtures("_registered")
def test_installed_harness_is_not_admitted_on_guests_yet() -> None:
    from tests.unit.test_guest_execution_materialization import _guest_inputs

    task, _, profile = _guest_inputs("nested_docker")
    reasons = automatic_service_execution_rejections(
        task, _trial(), source_provenance=_provenance(),
        supported_capabilities=profile.supported_guest_capabilities,
    )
    assert "guest_driver_capabilities_unsupported" in reasons


# --- controller -------------------------------------------------------------------


class _FakeSandbox:
    def __init__(self, exit_code: int = 0, hang: bool = False) -> None:
        self.exit_code, self.hang = exit_code, hang
        self.calls: list[dict[str, object]] = []
        self.killed = False
        self.started = self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def exec_streaming(self, argv, *, env_vars, cwd, timeout_sec=None) -> ExecHandle:
        self.calls.append({"argv": argv, "env": env_vars, "cwd": cwd, "timeout": timeout_sec})

        async def output(data: bytes):
            if self.hang:
                await asyncio.sleep(60)
            yield data

        async def wait() -> int:
            return self.exit_code

        async def kill() -> None:
            self.killed = True

        return ExecHandle(pid=7, stdout=output(b"installed\n"), stderr=output(b""), _wait=wait, _kill=kill)


async def _run_setup(monkeypatch, sandbox: _FakeSandbox, tmp_path: Path) -> None:
    task, _, _ = _inputs()
    monkeypatch.setattr(controller, "sandbox_driver", lambda role, task: sandbox)
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:41234")
    await controller.run_setup(tmp_path, task, _trial())


@pytest.mark.usefixtures("_registered")
async def test_setup_runs_only_the_pinned_install_through_the_proxy(monkeypatch, tmp_path, capsysbinary) -> None:
    sandbox = _FakeSandbox()

    await _run_setup(monkeypatch, sandbox, tmp_path)

    task, _, _ = _inputs()
    assert sandbox.calls == [{
        "argv": list(_SETUP.install),
        "env": {**{name: "http://127.0.0.1:41234" for name in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY")},
                "no_proxy": "localhost,127.0.0.1,::1", "NO_PROXY": "localhost,127.0.0.1,::1"},
        "cwd": task.environment.workdir,
        "timeout": 1200,
    }]
    assert sandbox.started and sandbox.stopped
    assert capsysbinary.readouterr().out == b"installed\n"
    # Setup never stages task inputs into the sandbox.
    assert not any(isinstance(c["cwd"], PurePosixPath) and c["argv"][:1] == ["tar"] for c in sandbox.calls)


@pytest.mark.usefixtures("_registered")
async def test_failed_install_is_a_setup_failure(monkeypatch, tmp_path) -> None:
    sandbox = _FakeSandbox(exit_code=3)

    with pytest.raises(ServiceExecutionTaskError, match="harness setup failed with exit status 3"):
        await _run_setup(monkeypatch, sandbox, tmp_path)
    assert sandbox.stopped


@pytest.mark.usefixtures("_registered")
async def test_cancelled_setup_kills_the_install(monkeypatch, tmp_path) -> None:
    sandbox = _FakeSandbox(hang=True)
    task = asyncio.ensure_future(_run_setup(monkeypatch, sandbox, tmp_path))
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert sandbox.killed and sandbox.stopped


@pytest.mark.usefixtures("_registered")
async def test_setup_refuses_without_the_runtime_egress_proxy(monkeypatch, tmp_path) -> None:
    task, _, _ = _inputs()
    monkeypatch.setattr(controller, "sandbox_driver", lambda role, task: _FakeSandbox())
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://evil.example.org:8080")

    with pytest.raises(ServiceExecutionTaskError, match="setup egress is unavailable"):
        await controller.run_setup(tmp_path, task, _trial())


async def test_setup_refuses_a_harness_without_a_setup_declaration(monkeypatch, tmp_path) -> None:
    task, terminus, _ = _inputs()
    monkeypatch.setenv("LOOM_TASK_EGRESS_PROXY", "http://127.0.0.1:41234")

    with pytest.raises(ServiceExecutionTaskError, match="declares no setup"):
        await controller.run_setup(tmp_path, task, terminus)
