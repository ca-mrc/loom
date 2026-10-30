"""Real SDK -> production LiteLLM forwarding -> isolated held upstreams.

This file can also run in the locked Harbor image with no pytest installation.
The default-client switches are negative controls, never production settings.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, cast

from openai import AsyncOpenAI

from loom.llm_http_client import create_model_http_client
from loom.llm_sdk_clients import ModelSDKClients
from loom_llm_gateway.litellm_wrapper import acompletion


async def _request(reader: asyncio.StreamReader) -> dict[str, Any]:
    headers = await reader.readuntil(b"\r\n\r\n")
    length = next(int(line.split(b":", 1)[1]) for line in headers.split(b"\r\n")
        if line.lower().startswith(b"content-length:"))
    return json.loads(await reader.readexactly(length))  # type: ignore[no-any-return]


async def _respond(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode()
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: "
        + str(len(body)).encode() + b"\r\n\r\n" + body)
    await writer.drain()


async def exercise_sdk_chain(
    *, count: int, origins: int = 1, provider: str = "openai", harbor: bool = False,
    default_sender: bool = False, default_forwarder: bool = False,
) -> dict[str, Any]:
    """Hold all replies until every concurrent SDK request reaches an upstream."""
    arrivals = 0
    forwarded = 0
    arrived = asyncio.Event()
    release = asyncio.Event()
    handlers: set[asyncio.Task[Any]] = set()
    errors: list[BaseException] = []
    model = "claude-3-5-sonnet-latest" if provider == "anthropic" else "gpt-4.1-mini"
    reply = ({"id": "offline", "type": "message", "role": "assistant", "model": model,
        "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
        "usage": {"input_tokens": 1, "output_tokens": 1}} if provider == "anthropic" else {
        "id": "offline", "object": "chat.completion", "created": 1, "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})

    async def upstream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal arrivals
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            await _request(reader)
            arrivals += 1
            if arrivals == count:
                arrived.set()
            await release.wait()
            await _respond(writer, reply)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            errors.append(error)
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    class DefaultForwardClients(ModelSDKClients):
        def client_for(self, **kwargs: Any) -> Any:
            return None

    clients = (DefaultForwardClients if default_forwarder else ModelSDKClients)(timeout_sec=30)
    upstreams = [await asyncio.start_server(upstream, "127.0.0.1", 0, backlog=2 * count) for _ in range(origins)]
    urls = [f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}" for server in upstreams]

    async def forward(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal forwarded
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            body = await _request(reader)
            index = forwarded % origins
            forwarded += 1
            response = await acompletion(model=f"{provider}/{model}", messages=body["messages"],
                api_key="synthetic", api_base=urls[index] + ("/v1" if provider == "openai" else ""),
                model_clients=clients, timeout=30, max_retries=0)
            await _respond(writer, response)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as error:
            errors.append(error)
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    gateway = await asyncio.start_server(forward, "127.0.0.1", 0, backlog=2 * count)
    gateway_url = f"http://127.0.0.1:{gateway.sockets[0].getsockname()[1]}/v1"
    sender_clients = ModelSDKClients(timeout_sec=30)
    sender = AsyncOpenAI(api_key="synthetic", base_url=gateway_url, max_retries=0,
        http_client=create_model_http_client(timeout_sec=30, trust_env=False))
    calls: list[asyncio.Task[Any]] = []
    try:
        if harbor:
            from harbor.llms.lite_llm import LiteLLM
            llm = LiteLLM(model_name="openai/gpt-4.1-mini", api_base=gateway_url, api_key="synthetic")
            kwargs = {} if default_sender else {"client": sender_clients.client_for(
                model="openai/gpt-4.1-mini", api_key="synthetic", api_base=gateway_url)}
            def call() -> Any:
                return llm.call(prompt="offline", **kwargs)
        else:
            def call() -> Any:
                return sender.chat.completions.create(model="gpt-4.1-mini",
                    messages=[{"role": "user", "content": "offline"}])
        for index in range(count):
            calls.append(asyncio.create_task(call()))
            # Ramp connection establishment so a host's listen backlog (128
            # on macOS) is not mistaken for a provider SDK admission limit.
            # Responses remain held: all requested calls must overlap.
            if index % 16 == 15:
                await asyncio.sleep(0.01)
        try:
            await asyncio.wait_for(arrived.wait(), timeout=15)
        except TimeoutError:
            raise AssertionError(f"upstream barrier {arrivals}/{count}, gateway arrivals {forwarded}, errors {[(type(error).__name__, str(error)[:180]) for error in errors[:2]]}") from None
        assert not any(call.done() for call in calls)
        release.set()
        async with asyncio.timeout(30):
            results = await asyncio.gather(*calls)
        assert len(results) == count and not errors
        return {"upstream_arrivals_before_reply": arrivals, "forwarded": forwarded,
            "responses": len(results), "origins": origins, "provider": provider, "harbor": harbor}
    finally:
        release.set()
        for server in [gateway, *upstreams]:
            server.close()
            await server.wait_closed()
        for task in [*calls, *handlers]:
            task.cancel()
        await asyncio.gather(*calls, *handlers, return_exceptions=True)
        await sender.close()
        await sender_clients.aclose()
        await clients.aclose()
        # Drain this SDK's best-effort callback worker before the test loop
        # exits; callbacks do not gate dispatch or the held-upstream barrier.
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
        await GLOBAL_LOGGING_WORKER.flush()
        await GLOBAL_LOGGING_WORKER.stop()


async def test_real_openai_sdk_crosses_aiohttp_per_host_threshold(monkeypatch: Any) -> None:
    monkeypatch.delenv("DISABLE_AIOHTTP_TRANSPORT", raising=False)
    await exercise_sdk_chain(count=501)


async def test_real_openai_sdk_crosses_disabled_aiohttp_httpx_threshold(monkeypatch: Any) -> None:
    monkeypatch.setenv("DISABLE_AIOHTTP_TRANSPORT", "True")
    await exercise_sdk_chain(count=101)


async def test_real_anthropic_sdk_crosses_total_aiohttp_threshold(monkeypatch: Any) -> None:
    monkeypatch.delenv("DISABLE_AIOHTTP_TRANSPORT", raising=False)
    await exercise_sdk_chain(count=1001, origins=3, provider="anthropic")


async def test_real_anthropic_sdk_crosses_disabled_aiohttp_httpx_threshold(monkeypatch: Any) -> None:
    monkeypatch.setenv("DISABLE_AIOHTTP_TRANSPORT", "True")
    await exercise_sdk_chain(count=101, provider="anthropic")


async def exercise_sdk_lifecycle(*, provider: str = "openai") -> dict[str, Any]:
    """Real peer EOF proves cancellation, stream close and owner close release I/O."""
    mode = "hold"
    arrived, eof = asyncio.Event(), asyncio.Event()
    seen = 0
    handlers: set[asyncio.Task[Any]] = set()
    model = "claude-3-5-sonnet-latest" if provider == "anthropic" else "gpt-4.1-mini"

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal seen
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            body = await _request(reader)
            seen += 1
            arrived.set()
            if body.get("stream"):
                chunk = {"id": "offline", "object": "chat.completion.chunk", "created": 1,
                    "model": model, "choices": [{"index": 0, "delta": {"content": "ok"}}]}
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: "
                    + json.dumps(chunk).encode() + b"\n\n")
                await writer.drain()
                await reader.read()
                eof.set()
            elif mode == "hold":
                await reader.read()
                eof.set()
            elif mode == "error":
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                await writer.drain()
            else:
                reply = ({"id": "offline", "type": "message", "role": "assistant", "model": model,
                    "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
                    "usage": {"input_tokens": 1, "output_tokens": 1}} if provider == "anthropic" else {
                    "id": "offline", "object": "chat.completion", "created": 1, "model": model,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
                await _respond(writer, reply)
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    base = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}" + ("/v1" if provider == "openai" else "")
    clients = ModelSDKClients(timeout_sec=10)
    task = None

    async def call(max_retries: int = 0) -> dict[str, Any]:
        return await acompletion(model=f"{provider}/{model}", messages=[{"role": "user", "content": "offline"}],
            api_key="synthetic", api_base=base, model_clients=clients, timeout=10, max_retries=max_retries)

    try:
        task = asyncio.create_task(call())
        await asyncio.wait_for(arrived.wait(), 5)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(eof.wait(), 5)
        mode = "ok"
        assert (await call())["choices"][0]["message"]["content"] == "ok"
        mode = "error"
        before = seen
        try:
            await call(max_retries=0)
        except Exception:
            pass
        assert seen == before + 1, "explicit retry budget zero must issue exactly one request"
        mode = "ok"
        assert (await call())["choices"][0]["message"]["content"] == "ok"
        if provider == "openai":
            eof.clear()
            sdk = clients.client_for(model=f"openai/{model}", api_key="synthetic", api_base=base)
            stream = await sdk.chat.completions.create(model=model, messages=[{"role": "user", "content": "offline"}], stream=True)
            await anext(stream)
            await stream.close()
            await asyncio.wait_for(eof.wait(), 5)
        mode = "hold"
        arrived.clear()
        eof.clear()
        task = asyncio.create_task(call())
        await asyncio.wait_for(arrived.wait(), 5)
        await clients.aclose()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        await asyncio.wait_for(eof.wait(), 5)
        mode = "ok"
        clients = ModelSDKClients(timeout_sec=10)
        assert (await call())["choices"][0]["message"]["content"] == "ok"
        return {"provider": provider, "cancel_peer_eof": True, "owner_close_peer_eof": True,
            "reuse_after_cancel_error_close": True, "explicit_retry_zero_sends": 1, "stream_closed": provider == "openai"}
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        server.close()
        await server.wait_closed()
        for handler in handlers:
            handler.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
        await clients.aclose()
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
        await GLOBAL_LOGGING_WORKER.flush()
        await GLOBAL_LOGGING_WORKER.stop()


async def test_injected_sdk_preserves_default_and_zero_retry_send_counts() -> None:
    counts = []
    for default_sdk in (True, False):
        for retry in (None, 0):
            seen = 0
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                nonlocal seen
                await _request(reader)
                seen += 1
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 0.001\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            clients = ModelSDKClients(timeout_sec=10)
            try:
                options = {} if retry is None else {"max_retries": retry}
                if default_sdk:
                    import litellm
                    try:
                        await litellm.acompletion(model="openai/gpt-4.1-mini", api_key="synthetic",
                            api_base=f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1",
                            messages=[{"role": "user", "content": "offline"}], timeout=10, **options)
                    except Exception:
                        pass
                else:
                    try:
                        await acompletion(model="openai/gpt-4.1-mini", api_key="synthetic",
                            api_base=f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1",
                            messages=[{"role": "user", "content": "offline"}], timeout=10,
                            model_clients=clients, **options)
                    except Exception:
                        pass
                counts.append(seen)
            finally:
                server.close()
                await server.wait_closed()
                await clients.aclose()
    assert counts == [3, 1, 3, 1]
    from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
    await GLOBAL_LOGGING_WORKER.flush()
    await GLOBAL_LOGGING_WORKER.stop()


async def test_real_sdk_cancel_error_close_and_retry_budget() -> None:
    await exercise_sdk_lifecycle(provider="openai")
    await exercise_sdk_lifecycle(provider="anthropic")


async def exercise_harbor_contract() -> dict[str, Any]:
    """Pinned Harbor's actual router, summary call and trajectory serializer."""
    import tempfile
    from pathlib import Path

    from harbor.agents.terminus_2.terminus_2 import Terminus2
    from harbor.llms.chat import Chat

    from loom.agent.terminus2.model_switch import install_role_router

    bodies = []
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        body = await _request(reader)
        bodies.append(body)
        await _respond(writer, {"id": "offline", "object": "chat.completion", "created": 1, "model": body["model"],
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
        writer.close()
        await writer.wait_closed()
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    base = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/v1"
    try:
        with tempfile.TemporaryDirectory() as directory:
            agent = Terminus2(logs_dir=Path(directory), model_name="openai/gpt-4.1-mini", api_base=base,
                llm_kwargs={"api_key": "synthetic", "max_retries": 0}, max_turns=4)
            async with ModelSDKClients(timeout_sec=10) as clients:
                sdk = clients.client_for(model="openai/gpt-4.1-mini", api_key="synthetic", api_base=base)
                agent._llm_call_kwargs["client"] = sdk
                router = install_role_router(agent, teacher_model_name="openai/gpt-4o-mini",
                    student_model_name="openai/gpt-4.1-mini", first_switch_episode=2, return_switch_episode=3)
                chat = Chat(router)
                for episode in (1, 2, 3):
                    agent._n_episodes = episode
                    await chat.chat("offline", **agent._llm_call_kwargs)
                await agent._run_subagent(prompt="offline summary", message_history=chat.messages,
                    steps=[], session_id="offline-summary", agent_name="summary", filename_suffix="summary", summary_text="offline")
                serialized = json.dumps(agent.options.model_dump(mode="json"))
                assert "client" not in serialized and "shared_session" not in serialized
                files = list(Path(directory).rglob("*.json"))
                assert files and all("AsyncOpenAI" not in f.read_text() for f in files)
                assert [len(body["messages"]) for body in bodies] == [1, 3, 5, 7]
                assert [body["model"] for body in bodies] == ["gpt-4.1-mini", "gpt-4o-mini", "gpt-4.1-mini", "gpt-4.1-mini"]
            assert sdk.is_closed
            return {"harbor_role_schedule": "student-teacher-student", "summary": True,
                "message_counts": [1, 3, 5, 7], "options_and_trajectory_serialized": True, "owned_client_closed": True}
    finally:
        server.close()
        await server.wait_closed()
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
        await GLOBAL_LOGGING_WORKER.flush()
        await GLOBAL_LOGGING_WORKER.stop()


async def test_owner_cleanup_attempts_all_clients_and_preserves_error() -> None:
    closed = []
    class Client:
        def __init__(self, name: str, fail: bool = False) -> None:
            self.name, self.fail = name, fail
        async def close(self) -> None:
            closed.append(self.name)
            if self.fail:
                raise RuntimeError("synthetic SDK close failure")
    clients = ModelSDKClients(timeout_sec=10)
    clients._clients = {("openai", "one", None): Client("one", True), ("openai", "two", None): Client("two")}
    try:
        await clients.aclose()
    except RuntimeError as error:
        assert str(error) == "synthetic SDK close failure"
    else:
        raise AssertionError("material close error was swallowed")
    assert set(closed) == {"one", "two"} and not clients._clients


async def test_native_tmux_cleanup_survives_sdk_close_error() -> None:
    from types import SimpleNamespace
    from uuid import uuid4

    from loom.agent.terminus2.runtime import LoomTerminus2Runtime
    from loom.models.types import ModelSpec

    closed = []
    async def fail() -> None:
        raise RuntimeError("synthetic SDK close failure")
    async def command(cmd: str) -> None:
        closed.append(cmd)
    runtime = LoomTerminus2Runtime(model=ModelSpec(provider="openai", name="test"), team_id=str(uuid4()),
        trial_id=uuid4(), cp_client=None, gateway_url="http://127.0.0.1")
    runtime._model_clients = cast(Any, SimpleNamespace(aclose=fail))
    runtime._active_env = cast(Any, SimpleNamespace(exec=command))
    try:
        await runtime.aclose_attempt()
    except RuntimeError as error:
        assert str(error) == "synthetic SDK close failure"
    else:
        raise AssertionError("material close error was swallowed")
    assert closed == ["tmux kill-session -t =terminus-2 2>/dev/null || true"]


async def test_gateway_cleanup_survives_sdk_close_error(monkeypatch: Any) -> None:
    from types import SimpleNamespace

    from loom_llm_gateway import app as gateway_app
    from loom_llm_gateway.config import GatewaySettings
    from loom_llm_gateway.egress_client_pool import EgressClientPool
    from loom_llm_gateway.pod_identity import ExecutionPodReviewer

    closed = []
    async def sdk_close() -> None:
        closed.append("sdk")
        raise RuntimeError("synthetic SDK close failure")
    async def upstream_close() -> None:
        closed.append("upstream")
    async def engine_close() -> None:
        closed.append("engine")
    async def reviewer_close(*args: Any) -> None:
        closed.append("reviewer")
    async def egress_close(*args: Any) -> None:
        closed.append("egress")
    async def noop(*args: Any) -> None:
        return None
    monkeypatch.setattr(gateway_app, "ModelSDKClients", lambda **kwargs: SimpleNamespace(aclose=sdk_close))
    monkeypatch.setattr(gateway_app, "create_upstream_client", lambda **kwargs: SimpleNamespace(aclose=upstream_close))
    monkeypatch.setattr(gateway_app, "create_async_engine", lambda *args, **kwargs: SimpleNamespace(dispose=engine_close))
    monkeypatch.setattr(gateway_app, "_assert_schema_startup", noop)
    monkeypatch.setattr(gateway_app, "_assert_secret_store_startup", noop)
    monkeypatch.setattr(ExecutionPodReviewer, "close", reviewer_close)
    monkeypatch.setattr(EgressClientPool, "aclose", egress_close)
    app = gateway_app.create_app(GatewaySettings(_env_file=None,
        db_url="postgresql+psycopg://offline:offline@127.0.0.1/offline", step_jwt_signing_key="offline"))
    try:
        async with app.router.lifespan_context(app):
            pass
    except RuntimeError as error:
        assert str(error) == "synthetic SDK close failure"
    else:
        raise AssertionError("material close error was swallowed")
    assert closed == ["sdk", "egress", "reviewer", "upstream", "engine"]


async def test_generic_protocol_recovery_keeps_primary_and_neighbor_alive() -> None:
    seen: dict[str, int] = {}
    neighbor_arrived, release = asyncio.Event(), asyncio.Event()
    handlers: set[asyncio.Task[Any]] = set()
    tasks = []
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            body = await _request(reader)
            content = body["messages"][0]["content"]
            if isinstance(content, list):
                content = content[0]["text"]
            seen[content] = seen.get(content, 0) + 1
            if content == "neighbor":
                neighbor_arrived.set()
                await release.wait()
            if content == "probe" and seen[content] == 1:
                return  # complete request accepted, no response: RemoteProtocolError
            await _respond(writer, {"id": "offline", "type": "message", "role": "assistant",
                "model": "claude-3-5-sonnet-latest", "content": [{"type": "text", "text": "ok"}],
                "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 1}})
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    base = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        async with ModelSDKClients(timeout_sec=5) as clients:
            async def call(text: str) -> dict[str, Any]:
                return await acompletion(model="anthropic/claude-3-5-sonnet-latest", api_key="synthetic", api_base=base,
                    messages=[{"role": "user", "content": text}], model_clients=clients, timeout=5, max_retries=0)
            neighbor = asyncio.create_task(call("neighbor"))
            tasks.append(neighbor)
            await asyncio.wait_for(neighbor_arrived.wait(), 5)
            assert (await call("probe"))["choices"][0]["message"]["content"] == "ok"
            assert not neighbor.done()
            release.set()
            assert (await neighbor)["choices"][0]["message"]["content"] == "ok"
            assert (await call("next"))["choices"][0]["message"]["content"] == "ok"
        assert seen == {"neighbor": 1, "probe": 2, "next": 1}
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        for task in [*tasks, *handlers]:
            task.cancel()
        await asyncio.gather(*tasks, *handlers, return_exceptions=True)
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
        await GLOBAL_LOGGING_WORKER.flush()
        await GLOBAL_LOGGING_WORKER.stop()


async def test_sdk_proxy_env_parser_preserves_true_and_one_semantics(monkeypatch: Any) -> None:
    import litellm
    monkeypatch.delenv("DISABLE_AIOHTTP_TRANSPORT", raising=False)
    monkeypatch.setattr(litellm, "aiohttp_trust_env", False)
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    monkeypatch.delenv("ALL_PROXY", raising=False)
    monkeypatch.delenv("all_proxy", raising=False)
    seen: list[str] = []
    def handler(label: str) -> Any:
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await _request(reader)
            seen.append(label)
            await _respond(writer, {"id": "offline", "object": "chat.completion", "created": 1, "model": "gpt-4.1-mini",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
            writer.close()
            await writer.wait_closed()
        return handle
    target = await asyncio.start_server(handler("direct"), "127.0.0.1", 0)
    proxy = await asyncio.start_server(handler("proxy"), "127.0.0.1", 0)
    base = f"http://127.0.0.1:{target.sockets[0].getsockname()[1]}/v1"
    proxy_url = f"http://127.0.0.1:{proxy.sockets[0].getsockname()[1]}"
    for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.setenv(key, proxy_url)
    try:
        for flag in ("1", " TRUE "):
            monkeypatch.setenv("AIOHTTP_TRUST_ENV", flag)
            async with ModelSDKClients(timeout_sec=5) as clients:
                await acompletion(model="openai/gpt-4.1-mini", api_key="synthetic", api_base=base,
                    messages=[{"role": "user", "content": "offline"}], model_clients=clients, timeout=5, max_retries=0)
        assert seen == ["direct", "proxy"]
    finally:
        for server in (target, proxy):
            server.close()
            await server.wait_closed()
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
        await GLOBAL_LOGGING_WORKER.flush()
        await GLOBAL_LOGGING_WORKER.stop()


async def test_owner_close_interrupts_protocol_fallback_and_forbids_recreation() -> None:
    seen = 0
    fallback_arrived, fallback_eof = asyncio.Event(), asyncio.Event()
    handlers: set[asyncio.Task[Any]] = set()
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal seen
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            await _request(reader)
            seen += 1
            if seen == 1:
                return
            if seen == 2:
                fallback_arrived.set()
                await reader.read()
                fallback_eof.set()
            else:
                await _respond(writer, {"id": "offline", "type": "message", "role": "assistant",
                    "model": "claude-3-5-sonnet-latest", "content": [{"type": "text", "text": "ok"}],
                    "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 1}})
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    base = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    clients = ModelSDKClients(timeout_sec=30)
    handler = clients.client_for(model="anthropic/claude-3-5-sonnet-latest", api_key="synthetic", api_base=base)
    async def call(owner: ModelSDKClients) -> dict[str, Any]:
        return await acompletion(model="anthropic/claude-3-5-sonnet-latest", api_key="synthetic", api_base=base,
            messages=[{"role": "user", "content": "offline"}], model_clients=owner, timeout=30, max_retries=0)
    task = asyncio.create_task(call(clients))
    try:
        await asyncio.wait_for(fallback_arrived.wait(), 5)
        await clients.aclose()
        try:
            await asyncio.wait_for(fallback_eof.wait(), 2)
        except TimeoutError:
            raise AssertionError("owner close did not interrupt held temporary SDK transport") from None
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        try:
            handler.create_client(timeout=30, event_hooks=None)
        except RuntimeError:
            pass
        else:
            raise AssertionError("closed SDK handler created a new transport")
        async with ModelSDKClients(timeout_sec=5) as successor:
            assert (await call(successor))["choices"][0]["message"]["content"] == "ok"
        assert seen == 3
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        server.close()
        await server.wait_closed()
        for current in handlers:
            current.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
        await clients.aclose()
        from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
        await GLOBAL_LOGGING_WORKER.flush()
        await GLOBAL_LOGGING_WORKER.stop()


if __name__ == "__main__":
    import argparse
    import importlib.metadata
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=501)
    parser.add_argument("--origins", type=int, default=1)
    parser.add_argument("--provider", choices=["openai", "anthropic"], default="openai")
    parser.add_argument("--harbor", action="store_true")
    parser.add_argument("--lifecycle", action="store_true")
    parser.add_argument("--harbor-contract", action="store_true")
    parser.add_argument("--default-sender", action="store_true")
    parser.add_argument("--default-forwarder", action="store_true")
    args = parser.parse_args()
    print({package: importlib.metadata.version(package) for package in ("litellm", "openai", "httpx")})
    if args.lifecycle:
        print(asyncio.run(exercise_sdk_lifecycle(provider=args.provider)))
    elif args.harbor_contract:
        print(asyncio.run(exercise_harbor_contract()))
    else:
        print(asyncio.run(exercise_sdk_chain(count=args.count, origins=args.origins, provider=args.provider,
        harbor=args.harbor, default_sender=args.default_sender, default_forwarder=args.default_forwarder)))
