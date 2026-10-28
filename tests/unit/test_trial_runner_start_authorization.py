"""Online start must precede even factories, sidecars and local-model launch."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.trajectory.storage import FakeObjectStore
from loom_worker.sandbox_network import SandboxBridge, SandboxNetworkAllocator
from loom_worker.trial_runner import LocalTrialRunner
from tests._trial_config_defaults import stub_trial_config
from tests.integration.test_trial_runner import _task_config


def runner(tmp_path, authorize):
    return LocalTrialRunner(
        trial_id=uuid4(), team_id=uuid4(), task_config=_task_config(),
        task_checksum="1" * 64, task_dir=tmp_path,
        trial_config=stub_trial_config(), driver_factory=Mock(),
        agent_factory=Mock(), verifier_factory=Mock(),
        object_store=FakeObjectStore(), gateway_client=FakeLLMGatewayClient(scripted=[]),
        local_trajectory_root=tmp_path, state_patch_callback=AsyncMock(),
        start_authorization=authorize,
    )


@pytest.mark.parametrize("response", [False, None, 1, "true", {}])
async def test_nonpositive_start_never_constructs_runtime(tmp_path, response):
    authorize = AsyncMock(return_value=response)
    subject = runner(tmp_path, authorize)
    with pytest.raises(RuntimeError, match="start authorization"):
        await subject.run()
    authorize.assert_awaited_once_with()
    subject.driver_factory.assert_not_called()
    subject.agent_factory.assert_not_called()
    subject.verifier_factory.assert_not_called()
    subject.state_patch_callback.assert_not_called()


@pytest.mark.parametrize("failure", [TimeoutError, ConnectionError, asyncio.CancelledError])
async def test_uncertain_start_cannot_be_retried(tmp_path, failure):
    authorize = AsyncMock(side_effect=failure)
    subject = runner(tmp_path, authorize)
    with pytest.raises(failure):
        await subject.run()
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.run()
    authorize.assert_awaited_once_with()
    subject.driver_factory.assert_not_called()


async def test_concurrent_start_is_rejected_before_second_authority_call(tmp_path):
    entered, finish = asyncio.Event(), asyncio.Event()

    async def authorize():
        entered.set()
        await finish.wait()
        return False

    subject = runner(tmp_path, authorize)
    task = asyncio.create_task(subject.run())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with pytest.raises(RuntimeError, match="already attempted"):
            await subject.run()
    finally:
        finish.set()
    with pytest.raises(RuntimeError, match="start authorization"):
        await task
    subject.driver_factory.assert_not_called()


async def test_positive_start_precedes_first_factory_and_stays_one_use(tmp_path):
    events = []

    async def authorize():
        events.append("authorized")
        return True

    def driver():
        events.append("driver")
        raise LookupError("factory boundary")

    subject = runner(tmp_path, authorize)
    subject.driver_factory = driver
    with pytest.raises(LookupError, match="factory boundary"):
        await subject.run()
    assert events == ["authorized", "driver"]
    with pytest.raises(RuntimeError, match="already attempted"):
        await subject.run()
    assert events == ["authorized", "driver"]


@pytest.mark.parametrize("stage", ["agent", "verifier", "jwt"])
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_setup_failure_releases_acquired_bridge(tmp_path, monkeypatch, stage, failure):
    from loom_worker import trial_runner as module

    subject = runner(tmp_path, AsyncMock(return_value=True))
    allocator = SandboxNetworkAllocator()
    subject.sandbox_allocator = allocator
    subject.sandbox_singleton = Mock()
    subject.driver_factory = Mock(return_value=SimpleNamespace(
        capabilities=SimpleNamespace(supports_custom_network=True),
    ))
    acquired = []

    async def create(**kwargs):
        index, subnet = allocator.acquire()
        bridge = SandboxBridge("owned-test-bridge", subnet, "owned-test-id", index)
        acquired.append(bridge)
        return bridge

    async def teardown(*, bridge, allocator):
        assert bridge is acquired[0]
        allocator.release(bridge.subnet_index)

    cleanup = AsyncMock(side_effect=teardown)
    monkeypatch.setattr(module, "create_sandbox_bridge", create)
    monkeypatch.setattr(module, "teardown_sandbox_bridge", cleanup)
    if stage == "agent":
        subject.agent_factory.side_effect = failure("setup failed")
    elif stage == "verifier":
        subject.verifier_factory.side_effect = failure("setup failed")
    else:
        subject.sandbox_secrets_root = tmp_path / "secrets"
        subject.sandbox_mint_token = AsyncMock(side_effect=failure("setup failed"))
    try:
        with pytest.raises(failure, match="setup failed"):
            await subject.run()
        cleanup.assert_awaited_once()
        assert allocator.in_use_count == 0
        assert subject._active_agent is None
    finally:
        # A RED run uses fake networks; release only the fixture's reserved slot.
        if acquired and allocator.in_use_count:
            allocator.release(acquired[0].subnet_index)
