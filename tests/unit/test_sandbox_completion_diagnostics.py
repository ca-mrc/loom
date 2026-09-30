"""Cleanup failures retain trusted accounting and only publish fixed diagnostics."""
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from loom.errors import DriverError
from loom.service_execution_sandbox_task import run_agent
from tests.unit.test_service_execution_sandbox_task import Sandbox
from tests.unit.test_service_execution_terminus_plan import _inputs
from tests.unit.test_service_sandbox_driver import driver_for


@pytest.mark.asyncio
@pytest.mark.parametrize("usage_error", [False, True])
async def test_cleanup_failure_preserves_usage_but_never_exports_workspace(
    tmp_path, monkeypatch, usage_error,
):
    task, trial, _ = _inputs()
    (tmp_path / "instruction.md").write_text("Produce answer.txt")
    agent = Sandbox()

    async def identity(_gateway):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        (kwargs["workspace"] / "trajectory.jsonl").write_bytes(b"")

    async def fail_cleanup():
        agent.quiesced = True
        if not usage_error:
            raise DriverError("cleanup failed")

    def fail_usage(*_args, **_kwargs):
        raise ValueError("invalid local trajectory")

    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver", lambda *_: agent)
    monkeypatch.setattr("loom.service_execution_sandbox_task._execution_identity", identity)
    monkeypatch.setattr("loom.service_execution_sandbox_task.run_terminus2", terminus)
    monkeypatch.setattr(agent, "stop_processes", fail_cleanup)
    if usage_error:
        monkeypatch.setattr("loom.service_execution_sandbox_task.parse_terminus_events", fail_usage)
    with pytest.raises(ValueError if usage_error else DriverError):
        await run_agent(tmp_path, task, trial)
    assert (tmp_path / ".loom/agent/usage.json").is_file() is not usage_error
    assert agent.quiesced
    assert not (tmp_path / ".loom/workspace.tar").exists()
    assert agent.state == "stopped"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,expected", [
    ("process_owner_mismatch", "process_owner_mismatch"),
    ("cleanup_timeout", "cleanup_timeout"),
    ("private-header-fixture", "http_error"),
])
async def test_rpc_diagnostic_never_includes_response_body_or_unknown_header(reason, expected):
    def respond(request):
        return httpx.Response(409, headers={"X-Loom-Sandbox-Error": reason},
                              text="private-body-fixture", request=request)

    driver = driver_for(Path("/unused"))
    driver._client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                      base_url="http://private-endpoint-fixture")
    try:
        with pytest.raises(DriverError) as caught:
            await driver.stop_processes()
        assert str(caught.value) == f"sandbox stop_processes failed (HTTP 409; {expected})"
        assert "private" not in str(caught.value)
    finally:
        await driver.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("diagnostic,retained", [
    ("pid=31;ppid=1;state=S;uid=65533;expected_uid=65532", True),
    ("pid=31;ppid=1;state=S;uid=65533;expected_uid=65532;command=private", False),
    ("pid=31;ppid=1;state=S;uid=9999999999;expected_uid=65532", False),
    ("pid=31;ppid=1;state=private;uid=0;expected_uid=65532", False),
])
async def test_rpc_retains_only_bounded_kernel_process_identity(diagnostic, retained):
    from loom.driver.service_sandbox import SandboxRPCError

    response = httpx.Response(409, headers={
        "X-Loom-Sandbox-Error": "process_owner_mismatch",
        "X-Loom-Sandbox-Process": diagnostic,
    }, request=httpx.Request("POST", "http://private-endpoint/stop-processes"),
        text="private response")
    error = SandboxRPCError("/stop-processes", httpx.HTTPStatusError(
        "private exception", request=response.request, response=response,
    ))
    assert (diagnostic in str(error)) is retained
    assert "private" not in str(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,diagnostic,retained", [
    (
        "cleanup_timeout",
        "pid=31;ppid=1;state=D;uid=0;wchan=wait_on_bit;kill_errno=0;sigkill_pending=1",
        True,
    ),
    (
        "cleanup_signal_denied",
        "pid=18;ppid=1;state=S;uid=1000;wchan=do_wait;kill_errno=1;sigkill_pending=0",
        True,
    ),
    (
        "cleanup_timeout",
        "pid=31;ppid=1;state=D;uid=0;wchan=../secret;kill_errno=0;sigkill_pending=0",
        False,
    ),
    (
        "cleanup_timeout",
        "pid=31;ppid=1;state=D;uid=0;wchan=wait;kill_errno=0;sigkill_pending=0;command=private",
        False,
    ),
    (
        "cleanup_signal_failed",
        "pid=31;ppid=1;state=D;uid=0;wchan=wait;kill_errno=999;sigkill_pending=0",
        False,
    ),
])
async def test_rpc_retains_only_bounded_cleanup_snapshot(reason, diagnostic, retained):
    from loom.driver.service_sandbox import SandboxRPCError

    response = httpx.Response(409, headers={
        "X-Loom-Sandbox-Error": reason,
        "X-Loom-Sandbox-Process": diagnostic,
    }, request=httpx.Request("POST", "http://private-endpoint/stop-processes"),
        text="private response")
    error = SandboxRPCError("/stop-processes", httpx.HTTPStatusError(
        "private exception", request=response.request, response=response,
    ))
    assert (diagnostic in str(error)) is retained
    assert "private" not in str(error)
    assert "secret" not in str(error)
