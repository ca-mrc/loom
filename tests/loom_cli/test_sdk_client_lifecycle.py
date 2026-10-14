"""Real provider clients belong to one run and close on every exit path."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from google import genai

from loom_cli.run_cmd import _build_sdk_clients
from tests.loom_cli.test_task_loader import _StubAdapter


class _ClosingTransport(httpx.MockTransport):
    def __init__(self) -> None:
        super().__init__(lambda _: httpx.Response(200))
        self.closed = False

    def close(self) -> None:
        self.closed = True

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize("fail_run", [False, True])
async def test_google_sdk_construction_and_cleanup(
    monkeypatch: pytest.MonkeyPatch, fail_run: bool,
) -> None:
    # A local API key must select the Gemini Developer API even when the shell
    # was previously configured for Vertex. Keep the actual SDK constructor.
    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
    real_client = genai.Client
    sync_transport = _ClosingTransport()
    async_transport = _ClosingTransport()

    def construct(**kwargs: Any) -> genai.Client:
        return real_client(
            **kwargs,
            http_options={
                "client_args": {"transport": sync_transport},
                "async_client_args": {"transport": async_transport},
            },
        )

    monkeypatch.setattr(genai, "Client", construct)
    try:
        try:
            async with _build_sdk_clients({"google": "synthetic-google-key"}) as clients:
                anthropic_client, openai_client, google_client = clients
                assert anthropic_client is None
                assert openai_client is None
                assert isinstance(google_client, real_client)
                assert google_client.vertexai is False
                assert not sync_transport.closed
                assert not async_transport.closed
                if fail_run:
                    raise RuntimeError("synthetic run failure")
        except RuntimeError as exc:
            assert fail_run and str(exc) == "synthetic run failure"
        assert sync_transport.closed
        assert async_transport.closed
    finally:
        await async_transport.aclose()
        sync_transport.close()


async def test_missing_credentials_do_not_construct_clients() -> None:
    async with _build_sdk_clients({}) as clients:
        assert clients == (None, None, None)


async def test_partial_construction_failure_closes_prior_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import anthropic

    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    real_anthropic = anthropic.AsyncAnthropic

    def construct_anthropic(**kwargs: Any) -> anthropic.AsyncAnthropic:
        return real_anthropic(**kwargs, http_client=http)

    def fail_google(**kwargs: Any) -> None:
        raise RuntimeError("synthetic construction failure")

    monkeypatch.setattr(anthropic, "AsyncAnthropic", construct_anthropic)
    monkeypatch.setattr(genai, "Client", fail_google)
    try:
        with pytest.raises(RuntimeError, match="construction failure"):
            async with _build_sdk_clients({"anthropic": "fake-ant", "google": "fake-g"}):
                pytest.fail("client construction should have failed")
        assert http.is_closed
    finally:
        await http.aclose()


async def test_setup_failure_joins_siblings_before_closing_clients(
    tmp_xdg_home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_benchmarks import registry

    from loom.driver.fake import FakeDriver
    from loom_cli import run_cmd
    from loom_cli.__main__ import _build_parser

    monkeypatch.setitem(registry.REGISTRY, "lifetime", _StubAdapter(name="lifetime"))
    started = asyncio.Event()
    cleanup_done = asyncio.Event()
    cleanup_saw_open_clients: list[bool] = []
    sibling_tasks: list[asyncio.Task[Any]] = []
    clients_open = False

    @asynccontextmanager
    async def clients(tokens: dict[str, str]) -> AsyncIterator[tuple[None, None, None]]:
        nonlocal clients_open
        clients_open = True
        try:
            yield None, None, None
        finally:
            clients_open = False

    async def driver_factory(backend: str, cfg: Any, **kwargs: Any) -> Any:
        if cfg.task.id.endswith("/t1"):
            await started.wait()
            raise RuntimeError("task image setup failed")
        return FakeDriver

    async def run_sibling(self: Any) -> None:
        task = asyncio.current_task()
        assert task is not None
        sibling_tasks.append(task)
        started.set()
        try:
            await asyncio.Future()
        finally:
            await asyncio.sleep(0)  # Cleanup itself needs to be awaited.
            cleanup_saw_open_clients.append(clients_open)
            cleanup_done.set()

    monkeypatch.setattr(run_cmd, "_build_sdk_clients", clients)
    monkeypatch.setattr(run_cmd, "_driver_factory", driver_factory)
    monkeypatch.setattr(run_cmd.LocalRunner, "run", run_sibling)
    args = _build_parser().parse_args([
        "run", "--dataset", "lifetime", "--agent", "oracle", "--backend", "fake",
        "--concurrency", "2", "--output-dir", str(tmp_path / "results"),
    ])
    try:
        with pytest.raises(RuntimeError, match="task image setup failed"):
            await run_cmd._run_one_model(args, model=None, output_dir=None)
        assert cleanup_done.is_set(), "run returned while a sibling still owned work"
        assert cleanup_saw_open_clients == [True]
        assert not clients_open
        assert all(task.done() for task in sibling_tasks)
    finally:
        for task in sibling_tasks:
            task.cancel()
        await asyncio.gather(*sibling_tasks, return_exceptions=True)
