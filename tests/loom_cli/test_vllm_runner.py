"""vllm_runner — subprocess lifecycle, dep-check, health-probe, cleanup.

vLLM itself is a heavy GPU dep; these tests use mocks throughout.
Coverage: the orchestration glue (find-free-port + cmd construction +
poll-until-healthy + graceful-stop), not the vLLM process itself.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import loom_cli.vllm_runner as vr


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner has module-level state (live-process list, signal-
    handler-installed flag). Reset between tests so order doesn't
    matter."""
    vr._LIVE_PROCESSES.clear()
    monkeypatch.setattr(vr, "_SIGNAL_HANDLERS_INSTALLED", False)


def test_missing_vllm_dep_raises_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(vr.shutil, "which", lambda _: None)
    with pytest.raises(vr.MissingVLLMDependencyError) as exc:
        vr.launch_vllm(vr.VLLMLaunchSpec(model="meta-llama/Llama-3.1-8B"))
    assert "pip install loom[vllm]" in str(exc.value)


def test_resolve_model_path_passes_through_hf_id() -> None:
    # HuggingFace ids look like `org/name` — no slash-prefix to
    # canonicalize, just pass through.
    assert vr._resolve_model_path("meta-llama/Llama-3.1-8B") == "meta-llama/Llama-3.1-8B"


def test_resolve_model_path_rejects_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    with pytest.raises(RuntimeError, match="does not exist"):
        vr._resolve_model_path(str(missing))


def test_resolve_model_path_accepts_existing_dir(tmp_path: Path) -> None:
    d = tmp_path / "weights"
    d.mkdir()
    resolved = vr._resolve_model_path(str(d))
    assert Path(resolved) == d.resolve()


def test_build_cmd_includes_required_flags() -> None:
    spec = vr.VLLMLaunchSpec(
        model="meta-llama/Llama-3.1-8B",
        gpu_memory_utilization=0.85,
        tensor_parallel_size=2,
        max_model_len=8192,
        enforce_eager=True,
        extra_args=("--served-model-name", "mymodel"),
    )
    cmd = vr._build_cmd("meta-llama/Llama-3.1-8B", port=8234, spec=spec)
    assert cmd[0:3] == ["vllm", "serve", "meta-llama/Llama-3.1-8B"]
    assert "--port" in cmd
    assert "8234" in cmd
    assert "--gpu-memory-utilization" in cmd
    assert "0.85" in cmd
    assert "--tensor-parallel-size" in cmd
    assert "2" in cmd
    assert "--max-model-len" in cmd
    assert "8192" in cmd
    assert "--enforce-eager" in cmd
    assert "--served-model-name" in cmd
    assert "mymodel" in cmd



def test_wait_for_ready_returns_on_200(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_resp = MagicMock(status_code=200)
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake_resp)

    proc = MagicMock()
    proc.poll.return_value = None  # still running
    # Should not raise
    vr._wait_for_ready("http://localhost:8000/v1", proc, timeout_sec=5.0)


def test_wait_for_ready_fails_fast_on_dead_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc = MagicMock()
    proc.poll.return_value = 1  # exited with code 1
    proc.returncode = 1
    with pytest.raises(RuntimeError, match="exited prematurely"):
        vr._wait_for_ready("http://localhost:8000/v1", proc, timeout_sec=5.0)


def test_wait_for_ready_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    import httpx
    proc = MagicMock()
    proc.poll.return_value = None  # never exits
    proc.pid = 12345

    def _refused(*_a: Any, **_kw: Any) -> Any:
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(vr.httpx, "get", _refused)
    # Tiny timeout so this test is fast
    monkeypatch.setattr(vr.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="did not become healthy"):
        vr._wait_for_ready("http://localhost:8000/v1", proc, timeout_sec=0.01)


def test_query_served_model_name_returns_first_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = MagicMock()
    fake.json.return_value = {"data": [{"id": "vllm-served-name"}]}
    fake.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake)
    assert vr._query_served_model_name("http://localhost:8000/v1") == "vllm-served-name"


def test_query_served_model_name_errors_on_empty_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = MagicMock()
    fake.json.return_value = {"data": []}
    fake.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake)
    with pytest.raises(RuntimeError, match="no entries"):
        vr._query_served_model_name("http://localhost:8000/v1")


def test_stop_process_graceful_then_kill(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = MagicMock()
    proc.poll.return_value = None  # alive
    # First wait() times out → escalate to kill
    proc.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="vllm", timeout=30),
        0,  # second wait after kill succeeds
    ]
    vr._stop_process(proc)
    proc.terminate.assert_called_once()
    proc.kill.assert_called_once()


def test_stop_process_noop_if_already_exited() -> None:
    proc = MagicMock()
    proc.poll.return_value = 0  # already done
    vr._stop_process(proc)
    proc.terminate.assert_not_called()
    proc.kill.assert_not_called()


def test_stop_all_drains_live_processes() -> None:
    p1 = MagicMock()
    p1.poll.return_value = None
    p1.wait.return_value = 0
    p2 = MagicMock()
    p2.poll.return_value = None
    p2.wait.return_value = 0
    vr._LIVE_PROCESSES.extend([p1, p2])

    vr.stop_all()

    assert vr._LIVE_PROCESSES == []
    p1.terminate.assert_called_once()
    p2.terminate.assert_called_once()


def test_launch_vllm_end_to_end_with_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The orchestration path: dep-check passes, subprocess starts,
    health check returns 200, served-model-name is queried, info is
    returned, atexit hook is installed."""
    monkeypatch.setattr(vr.shutil, "which", lambda _: "/usr/bin/vllm")
    monkeypatch.setattr(vr.time, "sleep", lambda _: None)

    fake_proc = MagicMock()
    fake_proc.poll.return_value = None
    fake_proc.pid = 99999
    monkeypatch.setattr(vr.subprocess, "Popen", lambda *a, **kw: fake_proc)

    fake_resp = MagicMock(status_code=200)
    fake_resp.json.return_value = {"data": [{"id": "meta-llama/Llama-3.1-8B"}]}
    fake_resp.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake_resp)

    info = vr.launch_vllm(vr.VLLMLaunchSpec(
        model="meta-llama/Llama-3.1-8B", port=18234,
    ))
    assert info.base_url == "http://localhost:18234/v1"
    assert info.served_model_name == "meta-llama/Llama-3.1-8B"
    assert info.pid == 99999
    assert fake_proc in vr._LIVE_PROCESSES


@pytest.mark.parametrize("outcome", ["complete", "setup_failed", "cancelled"])
def test_single_model_keep_alive_transfers_only_completed_process(
    tmp_xdg_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str,
) -> None:
    import asyncio

    from loom_benchmarks import registry

    from loom_cli import run_cmd
    from loom_cli.__main__ import main
    from tests.loom_cli.test_task_loader import _StubAdapter

    monkeypatch.setitem(registry.REGISTRY, "keepalive", _StubAdapter(name="keepalive"))
    monkeypatch.setattr(vr.shutil, "which", lambda _: "/usr/bin/vllm")
    monkeypatch.setattr(vr.time, "sleep", lambda _: None)
    monkeypatch.setattr(vr, "_wait_for_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr(vr, "_query_served_model_name", lambda _: "model")
    monkeypatch.setattr(vr.signal, "signal", lambda *args: None)
    exit_callbacks = []
    monkeypatch.setattr(vr.atexit, "register", exit_callbacks.append)
    proc = MagicMock()
    proc.pid = 91919
    proc.poll.return_value = None
    launch = MagicMock(return_value=proc)
    monkeypatch.setattr(vr.subprocess, "Popen", launch)
    if outcome != "complete":
        async def fail_setup(*args, **kwargs):
            raise (asyncio.CancelledError("cancelled") if outcome == "cancelled" else RuntimeError("setup failed"))

        monkeypatch.setattr(run_cmd, "_driver_factory", fail_setup)
    args = ["run", "--dataset", "keepalive", "--agent", "oracle", "--backend", "fake",
            "--model", "hf:org/model", "--keep-alive", "--output-dir", str(tmp_path / "out")]
    try:
        if outcome == "complete":
            assert main(args) in {0, 1}
        else:
            with pytest.raises(asyncio.CancelledError if outcome == "cancelled" else RuntimeError):
                main(args)
        assert proc not in vr._LIVE_PROCESSES
        for cleanup in exit_callbacks:
            cleanup()
        if outcome == "complete":
            proc.terminate.assert_not_called()
            assert launch.call_args.kwargs.get("start_new_session") is True
            assert launch.call_args.kwargs.get("stdin") == subprocess.DEVNULL
            assert (tmp_path / "out" / "vllm.log").exists()
        else:
            proc.terminate.assert_called_once()
    finally:
        if proc in vr._LIVE_PROCESSES:
            vr._LIVE_PROCESSES.remove(proc)


@pytest.mark.parametrize("failure_at", ["process_exit", "settle", "probe"])
def test_launch_vllm_cleans_up_on_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_at: str,
) -> None:
    """If wait_for_ready raises (server didn't come up), `_stop_process`
    is invoked on the subprocess rather than being orphaned. (When
    the subprocess died on its own, _stop_process detects this and
    no-ops — but the launch function still called it.)

    The proc passes the 2-second settle window (poll→None on the first
    call) but then dies during _wait_for_ready (poll→1 on all
    subsequent calls), so the retry path is NOT triggered.
    """
    monkeypatch.setattr(vr.shutil, "which", lambda _: "/usr/bin/vllm")
    monkeypatch.setattr(vr.time, "sleep", lambda _: None)

    fake_proc = MagicMock()
    # First poll() call is the settle-window check in launch_vllm → None
    # (process alive). All subsequent calls are inside _wait_for_ready →
    # 1 (process died), triggering the "exited prematurely" error.
    fake_proc.poll.side_effect = [None, 1]
    fake_proc.returncode = 1
    fake_proc.pid = 99998
    monkeypatch.setattr(vr.subprocess, "Popen", lambda *a, **kw: fake_proc)

    stop_calls: list[object] = []
    monkeypatch.setattr(vr, "_stop_process", lambda p: stop_calls.append(p))

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt("startup interrupted")

    if failure_at == "settle":
        monkeypatch.setattr(vr.time, "sleep", interrupted)
    elif failure_at == "probe":
        monkeypatch.setattr(vr, "_wait_for_ready", interrupted)
    with pytest.raises(RuntimeError if failure_at == "process_exit" else KeyboardInterrupt):
        vr.launch_vllm(vr.VLLMLaunchSpec(
            model="meta-llama/Llama-3.1-8B", port=18235,
        ))
    # _stop_process was called on the failed subprocess
    assert stop_calls == [fake_proc]
    assert fake_proc not in vr._LIVE_PROCESSES


def test_launch_vllm_retries_on_bind_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First Popen attempt 'binds' to a port that's actually busy:
    it exits with code 1. launch_vllm should detect that, retry the
    next port, and succeed on the second try."""
    monkeypatch.setattr(vr.shutil, "which", lambda _: "/usr/bin/vllm")
    monkeypatch.setattr(vr.time, "sleep", lambda _: None)

    proc1 = MagicMock()
    proc1.poll.return_value = 1  # exited (bind failure)
    proc1.returncode = 1
    proc1.pid = 11111

    proc2 = MagicMock()
    proc2.poll.return_value = None  # alive
    proc2.pid = 22222

    popens: list[MagicMock] = [proc1, proc2]
    ports_used: list[int] = []

    def _popen(cmd, *a, **kw):
        port_idx = cmd.index("--port") + 1
        ports_used.append(int(cmd[port_idx]))
        return popens.pop(0)

    monkeypatch.setattr(vr.subprocess, "Popen", _popen)

    fake_resp = MagicMock(status_code=200)
    fake_resp.json.return_value = {"data": [{"id": "model-x"}]}
    fake_resp.raise_for_status = MagicMock()
    monkeypatch.setattr(vr.httpx, "get", lambda *a, **kw: fake_resp)

    info = vr.launch_vllm(vr.VLLMLaunchSpec(
        model="model-x", port=18234,
    ))

    assert info.pid == 22222
    assert ports_used == [18234, 18235]  # retried next port
    assert proc1 not in vr._LIVE_PROCESSES   # bind-fail proc cleaned up
    assert proc2 in vr._LIVE_PROCESSES        # success proc registered


def test_model_slug_hf_id_uses_basename() -> None:
    # HuggingFace ids: drop the org prefix, lowercase, replace dots
    assert vr.model_slug("hf:meta-llama/Llama-3.1-8B-Instruct") == "llama-3-1-8b-instruct"


def test_model_slug_absolute_path_uses_dirname() -> None:
    assert vr.model_slug("/data/checkpoints/my-tune/") == "my-tune"


def test_model_slug_home_path_uses_dirname() -> None:
    assert vr.model_slug("~/weights/llama-3-1-8b") == "llama-3-1-8b"


def test_model_slug_strips_trailing_slash() -> None:
    assert vr.model_slug("./weights/v2/") == "v2"


def test_model_slug_bare_local_server_name_passthrough() -> None:
    # When called with a plain registered name (no prefix, no slash),
    # slug == name (used for `loom serve --name` defaults from a
    # pre-resolved id).
    assert vr.model_slug("llama8b") == "llama8b"


def test_model_slug_filesystem_safe() -> None:
    # No spaces, no uppercase, no dots — safe for output dirs
    slug = vr.model_slug("hf:Org/Name.With.Dots")
    assert " " not in slug
    assert slug == slug.lower()
    assert "." not in slug


def test_stop_one_terminates_and_removes_from_registry() -> None:
    proc = MagicMock()
    proc.poll.return_value = None
    proc.wait.return_value = 0
    vr._LIVE_PROCESSES.append(proc)

    vr.stop_one(proc)

    assert proc not in vr._LIVE_PROCESSES
    proc.terminate.assert_called_once()


def test_stop_one_is_noop_for_unknown_proc() -> None:
    proc = MagicMock()
    proc.poll.return_value = 0  # already exited
    # NOT in _LIVE_PROCESSES
    vr.stop_one(proc)

    # No exception, no terminate call (since already-exited)
    proc.terminate.assert_not_called()
    assert proc not in vr._LIVE_PROCESSES
