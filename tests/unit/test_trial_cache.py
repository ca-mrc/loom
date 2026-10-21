"""Unit tests for `loom_worker.trial_cache` (#317 Phase 1).

Covers the pure helpers + the orchestration in `resolve_trial_image`
with mocked Docker client + mocked cp_client. Real Docker is
exercised in `tests/integration/test_trial_cache_e2e.py`.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from docker.errors import APIError, ImageNotFound

from loom_worker import trial_cache

# ─── Stubs ──────────────────────────────────────────────────────────


class _StubSettings:
    """Mirror of the codegen'd WorkerSettings.trial_cache_* fields."""

    def __init__(
        self,
        *,
        trial_cache_registry_repo: str = "",
        trial_cache_registry_pull_timeout_sec: float = 15.0,
        trial_cache_base_image_pull_timeout_sec: float = 1800.0,
        trial_cache_ttl_hours: int = 168,
        trial_cache_min_free_gb: int = 20,
        trial_cache_build_lock_timeout_sec: float = 1800.0,
        trial_cache_build_max_concurrent: int = 1,
        docker_api_timeout_sec: int = 900,
        setup_health_guard_enabled: bool = True,
        setup_health_io_full_avg10_max: float = 50.0,
        setup_health_min_swap_free_mb: int = 1024,
        setup_health_dstate_max: int = 32,
        setup_health_wait_timeout_sec: float = 0.0,
        setup_health_poll_interval_sec: float = 0.0,
    ) -> None:
        self.trial_cache_registry_repo = trial_cache_registry_repo
        self.trial_cache_registry_pull_timeout_sec = trial_cache_registry_pull_timeout_sec
        self.trial_cache_base_image_pull_timeout_sec = trial_cache_base_image_pull_timeout_sec
        self.trial_cache_ttl_hours = trial_cache_ttl_hours
        self.trial_cache_min_free_gb = trial_cache_min_free_gb
        self.trial_cache_build_lock_timeout_sec = trial_cache_build_lock_timeout_sec
        self.trial_cache_build_max_concurrent = trial_cache_build_max_concurrent
        self.docker_api_timeout_sec = docker_api_timeout_sec
        self.setup_health_guard_enabled = setup_health_guard_enabled
        self.setup_health_io_full_avg10_max = setup_health_io_full_avg10_max
        self.setup_health_min_swap_free_mb = setup_health_min_swap_free_mb
        self.setup_health_dstate_max = setup_health_dstate_max
        self.setup_health_wait_timeout_sec = setup_health_wait_timeout_sec
        self.setup_health_poll_interval_sec = setup_health_poll_interval_sec


class _StubAdapter:
    def __init__(self, install_script: str | None) -> None:
        self.install_script = install_script
        self.name = "stub"


class _StubCPClient:
    """In-memory cache-slot store mimicking the CP HTTP routes."""

    def __init__(self) -> None:
        self.slots: dict[str, UUID] = {}
        self.refresh_calls: list[tuple[str, UUID]] = []
        self.release_calls: list[tuple[str, UUID]] = []

    async def claim_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool:
        if cache_key in self.slots:
            return False
        self.slots[cache_key] = worker_id
        return True

    async def trial_cache_slot_exists(self, cache_key: str) -> bool:
        return cache_key in self.slots

    async def release_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
    ) -> None:
        self.release_calls.append((cache_key, worker_id))
        if self.slots.get(cache_key) == worker_id:
            del self.slots[cache_key]

    async def refresh_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool:
        self.refresh_calls.append((cache_key, worker_id))
        return self.slots.get(cache_key) == worker_id


def _stub_docker(*, locally: dict[str, str] | None = None) -> MagicMock:
    """Mocked docker client. `locally` maps tag → digest for images
    treated as "already pulled" locally."""
    client = MagicMock()
    locally = locally or {}

    def _get(ref: str) -> Any:
        if ref in locally:
            img = MagicMock()
            img.id = locally[ref]
            return img
        raise ImageNotFound(f"image {ref!r} not found locally")

    client.images.get = MagicMock(side_effect=_get)
    return client


# ─── _normalize_install_script ──────────────────────────────────────


@pytest.mark.parametrize("raw", [None, "", "   ", "\n\n", "\t  \n  "])
def test_normalize_install_script_blank_returns_none(raw: str | None) -> None:
    assert trial_cache._normalize_install_script(raw) is None


def test_normalize_install_script_real_passthrough() -> None:
    body = "set -euo pipefail\napt-get install -y curl"
    assert trial_cache._normalize_install_script(body) == body


# ─── _cache_key ─────────────────────────────────────────────────────


def test_cache_key_is_32_hex_chars() -> None:
    key = trial_cache._cache_key(
        task_image_digest="sha256:abc",
        install_script="pip install foo==1",
    )
    assert len(key) == 32
    assert all(c in "0123456789abcdef" for c in key)


def test_cache_key_changes_when_digest_changes() -> None:
    a = trial_cache._cache_key(
        task_image_digest="sha256:aaa",
        install_script="x",
    )
    b = trial_cache._cache_key(
        task_image_digest="sha256:bbb",
        install_script="x",
    )
    assert a != b


def test_cache_key_changes_when_script_changes() -> None:
    a = trial_cache._cache_key(
        task_image_digest="sha256:abc",
        install_script="x",
    )
    b = trial_cache._cache_key(
        task_image_digest="sha256:abc",
        install_script="y",
    )
    assert a != b


def test_cache_key_deterministic() -> None:
    a = trial_cache._cache_key(
        task_image_digest="sha256:abc",
        install_script="x",
    )
    b = trial_cache._cache_key(
        task_image_digest="sha256:abc",
        install_script="x",
    )
    assert a == b


# ─── _pull_or_get_digest ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pull_or_get_digest_passthrough_on_digest_input() -> None:
    client = _stub_docker()
    out = await trial_cache._pull_or_get_digest(
        client,
        "sha256:abc123",
        timeout_sec=10,
    )
    assert out == "sha256:abc123"
    # No pull call
    client.images.pull.assert_not_called()


@pytest.mark.asyncio
async def test_pull_or_get_digest_inspects_when_locally_present() -> None:
    client = _stub_docker(locally={"python:3.11-slim": "sha256:cached"})
    out = await trial_cache._pull_or_get_digest(
        client,
        "python:3.11-slim",
        timeout_sec=10,
    )
    assert out == "sha256:cached"
    client.images.pull.assert_not_called()


@pytest.mark.asyncio
async def test_pull_or_get_digest_pulls_when_missing() -> None:
    pulled = {"called": False, "digest": "sha256:newly-pulled"}

    def _pull(ref: str) -> None:
        pulled["called"] = True

    def _get(ref: str) -> Any:
        if not pulled["called"]:
            raise ImageNotFound(ref)
        img = MagicMock()
        img.id = pulled["digest"]
        return img

    client = MagicMock()
    client.images.pull = MagicMock(side_effect=_pull)
    client.images.get = MagicMock(side_effect=_get)

    out = await trial_cache._pull_or_get_digest(
        client,
        "python:3.11-slim",
        timeout_sec=10,
    )
    assert out == "sha256:newly-pulled"
    assert pulled["called"]


@pytest.mark.asyncio
async def test_pull_or_get_digest_timeout_raises_cache_error() -> None:
    def _slow_pull(ref: str) -> None:
        import time

        time.sleep(0.5)  # exceeds the 0.05s timeout below

    client = MagicMock()
    client.images.pull = MagicMock(side_effect=_slow_pull)
    client.images.get = MagicMock(side_effect=ImageNotFound("x"))

    with pytest.raises(trial_cache.TrialCacheError, match="timed out pulling"):
        await trial_cache._pull_or_get_digest(
            client,
            "python:3.11-slim",
            timeout_sec=0.05,
        )


# ─── resolve_trial_image ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_trial_image_no_install_returns_task_image() -> None:
    """Oracle / litellm / in-box adapters have no install_script."""
    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=None),
        settings=_StubSettings(),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
        docker_client=_stub_docker(),
    )
    assert out == "python:3.11-slim"


@pytest.mark.asyncio
async def test_resolve_trial_image_blank_install_returns_task_image() -> None:
    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script="   \n  "),
        settings=_StubSettings(),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
        docker_client=_stub_docker(),
    )
    assert out == "python:3.11-slim"


@pytest.mark.asyncio
async def test_resolve_trial_image_missing_attr_returns_task_image() -> None:
    """Phase 1 ships install_script on 3 adapters; the 9 legacy adapters
    (codex, gemini-cli, etc.) don't yet declare the field. getattr-default
    must keep them passthrough until Phase 2 adds the attribute."""

    class _LegacyAdapter:
        pass

    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_LegacyAdapter(),  # type: ignore[arg-type]
        settings=_StubSettings(),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
        docker_client=_stub_docker(),
    )
    assert out == "python:3.11-slim"


@pytest.mark.asyncio
async def test_resolve_trial_image_creates_docker_client_with_worker_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    digest = "sha256:cached"
    install = "echo install"
    key = trial_cache._cache_key(
        task_image_digest=digest,
        install_script=install,
    )
    client = _stub_docker(
        locally={
            "python:3.11-slim": digest,
            f"loom-trial-cache:{key}": "sha256:layered",
        }
    )
    from_env_timeouts: list[float | None] = []

    def _from_env(*, timeout: float | None = None) -> Any:
        from_env_timeouts.append(timeout)
        return client

    monkeypatch.setattr(trial_cache.docker, "from_env", _from_env)

    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=install),
        settings=_StubSettings(docker_api_timeout_sec=900.0),
        cp_client=_StubCPClient(),
        worker_id=uuid4(),
    )

    assert out == f"loom-trial-cache:{key}"
    assert from_env_timeouts == [900.0]


@pytest.mark.asyncio
async def test_resolve_trial_image_local_cache_hit() -> None:
    """When the layered tag already exists locally, no claim needed."""
    digest = "sha256:base-digest"
    install = "set -e\npip install --break-system-packages foo==1.0.0"
    key = trial_cache._cache_key(
        task_image_digest=digest,
        install_script=install,
    )
    layered_tag = f"loom-trial-cache:{key}"
    client = _stub_docker(
        locally={
            "python:3.11-slim": digest,
            layered_tag: "sha256:layered",
        }
    )
    cp = _StubCPClient()
    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=install),
        settings=_StubSettings(),
        cp_client=cp,
        worker_id=uuid4(),
        docker_client=client,
    )
    assert out == layered_tag
    # No slot claim needed
    assert cp.slots == {}


@pytest.mark.asyncio
async def test_resolve_trial_image_builder_path() -> None:
    """Cache miss → claim slot → build → release. Verifies the
    happy-path orchestration (without exercising real Docker build)."""
    digest = "sha256:base"
    install = "echo install\npip install --break-system-packages foo==1"
    client = _stub_docker(locally={"python:3.11-slim": digest})

    # Stub out the build so we don't actually invoke Docker.
    build_calls: list[dict[str, Any]] = []

    def _fake_build(**kwargs: Any) -> None:
        build_calls.append(kwargs)
        # After "building", the image exists locally too.
        client.images.get = MagicMock(
            side_effect=lambda ref: (
                MagicMock(id="sha256:layered")
                if ref.startswith("loom-trial-cache:") or ref == "python:3.11-slim"
                else (_ for _ in ()).throw(ImageNotFound(ref))
            )
        )

    import loom_worker.trial_cache as tc

    cp = _StubCPClient()
    worker_id = uuid4()

    # Replace the sync build helper with our stub
    import unittest.mock

    with unittest.mock.patch.object(
        tc,
        "_build_layered_image_sync",
        side_effect=_fake_build,
    ):
        out = await tc.resolve_trial_image(
            task_image="python:3.11-slim",
            adapter=_StubAdapter(install_script=install),
            settings=_StubSettings(setup_health_guard_enabled=False),
            cp_client=cp,
            worker_id=worker_id,
            docker_client=client,
        )

    expected_key = trial_cache._cache_key(
        task_image_digest=digest,
        install_script=install,
    )
    assert out == f"loom-trial-cache:{expected_key}"
    assert len(build_calls) == 1
    # Slot released after build
    assert (expected_key, worker_id) in cp.release_calls


@pytest.mark.asyncio
async def test_resolve_trial_image_serializes_builds_across_cache_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Different cache keys still hit the same host Docker daemon.

    The worker must take a daemon-wide build slot before calling
    docker-py build so concurrent setup/install containers cannot
    saturate a shared local Docker daemon.
    """
    cp = _StubCPClient()
    worker_id = uuid4()
    settings = _StubSettings(setup_health_guard_enabled=False)
    entered: list[str] = []
    first_entered = threading.Event()
    first_can_finish = threading.Event()

    clients = [
        _stub_docker(locally={"python:3.11-slim": "sha256:base-a"}),
        _stub_docker(locally={"python:3.12-slim": "sha256:base-b"}),
    ]

    def _fake_build(**kwargs: Any) -> None:
        tag = str(kwargs["tag"])
        entered.append(tag)
        if len(entered) == 1:
            first_entered.set()
            assert first_can_finish.wait(timeout=2), "first build did not release"

    monkeypatch.setattr(trial_cache, "_build_layered_image_sync", _fake_build)

    async def _fake_sleep(_seconds: float) -> None:
        await asyncio.sleep(0)

    first = asyncio.create_task(
        trial_cache.resolve_trial_image(
            task_image="python:3.11-slim",
            adapter=_StubAdapter(install_script="echo install a"),
            settings=settings,
            cp_client=cp,
            worker_id=worker_id,
            docker_client=clients[0],
            sleep=_fake_sleep,
        )
    )
    assert await asyncio.to_thread(first_entered.wait, 2)

    second = asyncio.create_task(
        trial_cache.resolve_trial_image(
            task_image="python:3.12-slim",
            adapter=_StubAdapter(install_script="echo install b"),
            settings=settings,
            cp_client=cp,
            worker_id=worker_id,
            docker_client=clients[1],
            sleep=_fake_sleep,
        )
    )
    await asyncio.sleep(0.05)
    assert len(entered) == 1, "second build entered before daemon slot released"

    first_can_finish.set()
    out1, out2 = await asyncio.gather(first, second)

    assert out1.startswith("loom-trial-cache:")
    assert out2.startswith("loom-trial-cache:")
    assert len(entered) == 2


@pytest.mark.asyncio
async def test_daemon_build_slot_checks_node_health_before_claiming_slot() -> None:
    """#275: the daemon-wide setup slot must not be claimed while the
    host is already under setup-blocking pressure. Otherwise a worker
    can keep admitting new Docker setup containers on an unhealthy
    OLDLAB node and make SSH/login symptoms worse.
    """
    cp = _StubCPClient()
    worker_id = uuid4()
    settings = _StubSettings(setup_health_wait_timeout_sec=0.0)

    from loom_worker.setup_admission import NodeHealthSnapshot, SetupAdmissionError

    with pytest.raises(SetupAdmissionError) as exc:
        async with trial_cache._daemon_build_slot(
            cp,
            settings,
            worker_id,
            read_setup_health=lambda: NodeHealthSnapshot(
                io_full_avg10=90.0,
                swap_total_mb=4096,
                swap_free_mb=4096,
                d_state_processes=1,
            ),
        ):
            raise AssertionError("slot should not be yielded")

    assert exc.value.reason == "node_io_pressure"
    assert cp.slots == {}


@pytest.mark.asyncio
async def test_daemon_build_slot_releases_slot_if_health_fails_after_claim() -> None:
    cp = _StubCPClient()
    worker_id = uuid4()
    settings = _StubSettings(setup_health_wait_timeout_sec=0.0)

    from loom_worker.setup_admission import NodeHealthSnapshot, SetupAdmissionError

    snapshots = [
        NodeHealthSnapshot(
            io_full_avg10=1.0,
            swap_total_mb=4096,
            swap_free_mb=4096,
            d_state_processes=1,
        ),
        NodeHealthSnapshot(
            io_full_avg10=90.0,
            swap_total_mb=4096,
            swap_free_mb=4096,
            d_state_processes=1,
        ),
    ]

    with pytest.raises(SetupAdmissionError):
        async with trial_cache._daemon_build_slot(
            cp,
            settings,
            worker_id,
            read_setup_health=lambda: snapshots.pop(0),
        ):
            raise AssertionError("slot should not be yielded")

    assert cp.slots == {}
    assert len(cp.release_calls) == 1
    assert cp.release_calls[0][1] == worker_id


@pytest.mark.asyncio
async def test_resolve_trial_image_waiter_path_polls_cheaply() -> None:
    """When another worker holds the slot, the waiter polls via
    `trial_cache_slot_exists` (cheap SELECT) rather than re-firing
    `claim_trial_cache_slot` every iteration."""
    digest = "sha256:base"
    install = "echo install"
    expected_key = trial_cache._cache_key(
        task_image_digest=digest,
        install_script=install,
    )
    layered_tag = f"loom-trial-cache:{expected_key}"

    other_worker = uuid4()
    cp = _StubCPClient()
    cp.slots[expected_key] = other_worker  # other worker holds the slot

    # After 2 polls, the slot disappears AND the image appears locally
    # (simulating other worker finishing + pushing).
    poll_count = {"n": 0}

    async def fake_sleep(_s: float) -> None:
        poll_count["n"] += 1
        if poll_count["n"] >= 2:
            # Other builder finished
            del cp.slots[expected_key]
            client._has_layered = True

    client = _stub_docker(locally={"python:3.11-slim": digest})
    client._has_layered = False

    original_get = client.images.get

    def _get_with_layered(ref: str) -> Any:
        if ref == layered_tag and client._has_layered:
            img = MagicMock()
            img.id = "sha256:layered"
            return img
        return original_get(ref)

    client.images.get = MagicMock(side_effect=_get_with_layered)

    out = await trial_cache.resolve_trial_image(
        task_image="python:3.11-slim",
        adapter=_StubAdapter(install_script=install),
        settings=_StubSettings(),
        cp_client=cp,
        worker_id=uuid4(),
        docker_client=client,
        sleep=fake_sleep,
    )
    assert out == layered_tag
    # We never became the builder
    assert cp.release_calls == []


# ─── B2: heartbeat refreshes slot TTL ───────────────────────────────


@pytest.mark.asyncio
async def test_builder_heartbeat_refreshes_slot() -> None:
    """The heartbeat task must call refresh_trial_cache_slot every
    `interval_sec` seconds while the context is open. This is the
    central crash-safety invariant — without it, a long build's slot
    expires mid-build and another worker steals it."""
    import asyncio

    cp = _StubCPClient()
    cache_key = "hb-key"
    worker_id = uuid4()
    cp.slots[cache_key] = worker_id  # we hold it

    async with trial_cache._builder_with_heartbeat(
        cp,
        cache_key,
        worker_id,
        ttl_sec=10.0,
        interval_sec=0.05,  # 50ms heartbeat
    ):
        await asyncio.sleep(0.28)  # expect ~5 refreshes

    # At least 3 refreshes, at most ~7 (loose bounds for scheduler jitter)
    assert 3 <= len(cp.refresh_calls) <= 7, cp.refresh_calls
    assert all(call == (cache_key, worker_id) for call in cp.refresh_calls)


@pytest.mark.asyncio
async def test_builder_heartbeat_stops_on_context_exit() -> None:
    """Exiting the context must drain the heartbeat task — no calls
    after the `async with` block returns."""
    import asyncio

    cp = _StubCPClient()
    cache_key, worker_id = "hb-exit", uuid4()
    cp.slots[cache_key] = worker_id

    async with trial_cache._builder_with_heartbeat(
        cp,
        cache_key,
        worker_id,
        ttl_sec=10.0,
        interval_sec=0.05,
    ):
        await asyncio.sleep(0.12)

    calls_at_exit = len(cp.refresh_calls)
    await asyncio.sleep(0.2)  # would be 4+ more refreshes if not stopped
    assert len(cp.refresh_calls) == calls_at_exit


# ─── B4: BuildError regression ──────────────────────────────────────


def test_build_layered_image_wraps_build_error() -> None:
    """docker.errors.BuildError (failed RUN in install.sh — the most
    common failure mode) must be caught and re-raised as
    TrialCacheError. Was escaping the narrow APIError filter before."""
    from docker.errors import BuildError

    client = MagicMock()
    client.images.build.side_effect = BuildError(
        reason="step 2 failed: exit code 1",
        build_log=[],
    )
    with pytest.raises(trial_cache.TrialCacheError) as exc:
        trial_cache._build_layered_image_sync(
            client=client,
            tag="loom-trial-cache:abc",
            base_digest="sha256:base",
            install_script="echo will fail && false",
        )
    assert "failed to build layered image" in str(exc.value)


def test_build_layered_image_preserves_full_diagnostic_detail() -> None:
    from docker.errors import BuildError

    client = MagicMock()
    noise = [{"stream": f"layer noise line {idx}\n"} for idx in range(120)]
    error = [{"stream": "LAYER_FINAL_ERROR: dpkg failed\n"}]
    client.images.build.side_effect = BuildError(
        reason="step 2 failed: exit code 1",
        build_log=iter(noise + error),
    )

    with pytest.raises(trial_cache.TrialCacheError) as exc:
        trial_cache._build_layered_image_sync(
            client=client,
            tag="loom-trial-cache:abc",
            base_digest="sha256:base",
            install_script="echo will fail && false",
        )

    msg = str(exc.value)
    assert "LAYER_FINAL_ERROR" in msg
    assert "layer noise line 0" not in msg
    assert exc.value.diagnostic_detail is not None
    assert "layer noise line 0" in exc.value.diagnostic_detail
    assert "LAYER_FINAL_ERROR" in exc.value.diagnostic_detail


def test_evict_stale_managed_images_covers_all_managed_kinds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    client.info.return_value = {"DockerRootDir": "/var/lib/docker"}
    settings = _StubSettings(trial_cache_ttl_hours=24)
    settings.task_image_local_ttl_hours = 48
    settings.task_image_min_free_gb = 20
    images = {
        "loom.task-image=true": [
            SimpleNamespace(id="base-old", attrs={"Created": "2026-01-01"}),
        ],
        "loom.task-sidecar=true": [
            SimpleNamespace(id="sidecar-middle", attrs={"Created": "2026-02-01"}),
        ],
        "loom.trial-cache=true": [
            SimpleNamespace(id="layer-new", attrs={"Created": "2026-03-01"}),
        ],
    }

    def list_images(*, filters):  # type: ignore[no-untyped-def]
        return images[filters["label"]]

    client.images.list.side_effect = list_images
    disk_samples = iter(
        [
            SimpleNamespace(free=1 * 1024**3),
            SimpleNamespace(free=5 * 1024**3),
            SimpleNamespace(free=25 * 1024**3),
        ]
    )
    monkeypatch.setattr(trial_cache.shutil, "disk_usage", lambda _path: next(disk_samples))

    result = trial_cache.evict_stale_managed_images(client, settings)

    assert [call.kwargs["filters"] for call in client.images.prune.call_args_list] == [
        {
            "label": "loom.task-image=true",
            "until": "48h",
            "dangling": False,
        },
        {
            "label": "loom.task-sidecar=true",
            "until": "48h",
            "dangling": False,
        },
        {
            "label": "loom.trial-cache=true",
            "until": "48h",
            "dangling": False,
        },
    ]
    assert [call.args[0] for call in client.images.remove.call_args_list] == [
        "base-old",
        "sidecar-middle",
    ]
    assert result == trial_cache.ManagedImageCleanupResult(
        docker_root="/var/lib/docker",
        storage_probe_path="/var/lib/docker",
        free_bytes=25 * 1024**3,
        required_free_bytes=20 * 1024**3,
        probe_available=True,
        error_count=0,
    )


@pytest.mark.parametrize("sandbox_identity", ["", "session-a"])
def test_cleanup_removes_only_stopped_labelled_containers_without_volumes(
    monkeypatch: pytest.MonkeyPatch, sandbox_identity: str,
) -> None:
    def image(labels: dict[str, str]) -> SimpleNamespace:
        return SimpleNamespace(labels=labels, attrs={"Config": {"Labels": labels}})

    def container(
        container_id: str,
        *,
        status: str,
        labels: dict[str, str],
        image_labels: dict[str, str],
    ) -> SimpleNamespace:
        return SimpleNamespace(
            id=container_id,
            status=status,
            labels=labels,
            image=image(image_labels),
            attrs={"State": {"Status": status}, "Config": {"Labels": labels}},
            remove=MagicMock(),
        )

    container_owned = container(
        "container-owned",
        status="exited",
        labels={"loom.task-image": "true", "loom.sandbox": sandbox_identity},
        image_labels={},
    )
    image_owned = container(
        "image-owned",
        status="dead",
        labels={"loom.sandbox": sandbox_identity},
        image_labels={"loom.task-sidecar": "true"},
    )
    unlabelled = container(
        "unlabelled",
        status="exited",
        labels={},
        image_labels={},
    )
    running = container(
        "running",
        status="running",
        labels={"loom.trial-cache": "true"},
        image_labels={"loom.task-image": "true"},
    )
    foreign = container(
        "foreign",
        status="exited",
        labels={"loom.sandbox": "session-b"},
        image_labels={"loom.task-image": "true"},
    )
    legacy = container(
        "legacy",
        status="exited",
        labels={},
        image_labels={"loom.task-image": "true"},
    )
    client = MagicMock()
    client.containers.list.return_value = [
        container_owned,
        image_owned,
        unlabelled,
        running,
        foreign,
        legacy,
    ]
    client.info.return_value = {"DockerRootDir": "/var/lib/docker"}
    monkeypatch.setattr(
        trial_cache.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=25 * 1024**3),
    )

    settings = _StubSettings(trial_cache_min_free_gb=20)
    settings.sandbox_identity = sandbox_identity
    result = trial_cache.evict_stale_managed_images(client, settings)  # type: ignore[arg-type]

    container_owned.remove.assert_called_once_with(v=False, force=False)
    image_owned.remove.assert_called_once_with(v=False, force=False)
    unlabelled.remove.assert_not_called()
    running.remove.assert_not_called()
    foreign.remove.assert_not_called()
    if sandbox_identity:
        legacy.remove.assert_not_called()
    else:
        legacy.remove.assert_called_once_with(v=False, force=False)
    assert not client.volumes.mock_calls
    assert result.probe_available is True
    assert result.error_count == 0


@pytest.mark.parametrize("free_gb", [1, 25])
def test_shared_daemon_cleanup_preserves_images_and_reports_actual_storage(
    monkeypatch: pytest.MonkeyPatch, free_gb: int,
) -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    client.info.return_value = {"DockerRootDir": "/var/lib/docker"}
    client.images.list.return_value = [SimpleNamespace(id="shared-image", attrs={})]
    monkeypatch.setattr(
        trial_cache.shutil, "disk_usage", lambda _path: SimpleNamespace(free=free_gb * 1024**3),
    )
    settings = _StubSettings(trial_cache_min_free_gb=20)
    settings.sandbox_identity = "session-a"

    result = trial_cache.evict_stale_managed_images(client, settings)  # type: ignore[arg-type]

    client.images.prune.assert_not_called()
    client.images.remove.assert_not_called()
    assert result.probe_available is True
    assert result.free_bytes == free_gb * 1024**3
    assert result.required_free_bytes == 20 * 1024**3
    assert result.error_count == 0


def test_cleanup_returns_failed_probe_and_counts_cleanup_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = SimpleNamespace(
        id="managed-stopped",
        status="exited",
        labels={"loom.task-image": "true"},
        image=SimpleNamespace(labels={}),
        attrs={},
        remove=MagicMock(side_effect=APIError("container is busy")),
    )
    client = MagicMock()
    client.containers.list.return_value = [failed]
    client.info.return_value = {"DockerRootDir": "/var/lib/docker"}
    monkeypatch.setattr(
        trial_cache.shutil,
        "disk_usage",
        MagicMock(side_effect=OSError("probe unavailable")),
    )

    result = trial_cache.evict_stale_managed_images(
        client,
        _StubSettings(trial_cache_min_free_gb=20),  # type: ignore[arg-type]
    )

    assert result == trial_cache.ManagedImageCleanupResult(
        docker_root="/var/lib/docker",
        storage_probe_path="/var/lib/docker",
        free_bytes=0,
        required_free_bytes=20 * 1024**3,
        probe_available=False,
        error_count=2,
    )


def test_cleanup_uses_explicit_storage_probe_path_when_docker_root_is_unmounted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    client.info.return_value = {"DockerRootDir": "/var/lib/docker"}
    settings = _StubSettings(trial_cache_min_free_gb=20)
    settings.task_image_storage_probe_path = Path(
        "/run/loom/docker-storage-probe",
    )
    observed_paths: list[str | Path] = []

    def disk_usage(path: str | Path) -> SimpleNamespace:
        observed_paths.append(path)
        return SimpleNamespace(free=25 * 1024**3)

    monkeypatch.setattr(trial_cache.shutil, "disk_usage", disk_usage)

    result = trial_cache.evict_stale_managed_images(client, settings)

    assert observed_paths == [Path("/run/loom/docker-storage-probe")]
    assert result.docker_root == "/var/lib/docker"
    assert result.storage_probe_path == "/run/loom/docker-storage-probe"
    assert result.probe_available is True
    assert result.free_bytes == 25 * 1024**3


def test_cleanup_counts_invalid_docker_root_separately_from_prune_errors() -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    client.images.prune.side_effect = [APIError("prune failed"), None, None]
    client.info.return_value = {"DockerRootDir": "relative/docker"}

    result = trial_cache.evict_stale_managed_images(
        client,
        _StubSettings(trial_cache_min_free_gb=20),  # type: ignore[arg-type]
    )

    assert result.probe_available is False
    assert result.error_count == 2


def test_managed_image_eviction_from_env_closes_docker_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.containers.list.return_value = []
    client.info.return_value = {"DockerRootDir": "/var/lib/docker"}
    settings = _StubSettings(trial_cache_ttl_hours=24)
    settings.task_image_local_ttl_hours = 48
    settings.task_image_min_free_gb = 20
    monkeypatch.setattr(trial_cache.docker, "from_env", lambda: client)
    monkeypatch.setattr(
        trial_cache.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=25 * 1024**3),
    )

    result = trial_cache.evict_stale_managed_images_from_env(settings)

    assert client.images.prune.call_count == 3
    client.close.assert_called_once_with()
    assert result.probe_available is True
