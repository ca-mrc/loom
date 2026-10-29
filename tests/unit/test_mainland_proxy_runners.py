"""The runtime switch controls real lifecycle wiring, not just request metadata."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from loom.driver.fake import FakeDriver
from loom.errors import DriverError
from loom.models.task import TaskSidecarConfig
from loom.models.trajectory import TrialStartEvent
from loom.trajectory.atif import project_to_atif
from loom.trial.step_runner import _isolated_verifier_start_options
from tests._trial_config_defaults import stub_trial_config
from tests.loom_cli.test_local_runner import _runner
from tests.unit.test_trial_runner_start_authorization import runner as worker_runner


def gateway():
    return SimpleNamespace(
        network_name="controlled-internal", environment={"HTTPS_PROXY":"http://172.20.0.2:7890","NO_PROXY":"localhost,127.0.0.1"},
        metadata={"enabled":True,"applied":True,"loaded_rule_counts":{"gfw":4443,"greatfire":2}},
        start=AsyncMock(), stop=AsyncMock(), set_network_policy=AsyncMock(),
    )


def capture_trial(monkeypatch, module, contexts):
    class Trial:
        def __init__(self, *, ctx, state_patch):
            contexts.append(ctx)
        async def run(self):
            return SimpleNamespace(config=contexts[-1].trial_config)
    monkeypatch.setattr(module,"Trial",Trial)


@pytest.mark.parametrize("enabled",[False,True])
async def test_cli_switch_wires_network_and_verifier(monkeypatch,tmp_path,enabled):
    import loom_cli.local_runner as module
    driver=FakeDriver()
    driver.capabilities=driver.capabilities.model_copy(update={"backend":"docker","supports_custom_network":True})
    subject=_runner(tmp_path,{"name":"pytest"},driver)
    subject.trial_config=stub_trial_config(mainland_proxy=enabled)
    subject.task_config=subject.task_config.model_copy(update={"environment":subject.task_config.environment.model_copy(update={"environment":{"HTTPS_PROXY":"http://uncontrolled.example:8080","NO_PROXY":"*"},"sidecars":[TaskSidecarConfig(name="db",docker_image="unused")]})})
    controlled=gateway()
    subject.mainland_proxy_factory=Mock(return_value=controlled)
    contexts=[]
    capture_trial(monkeypatch,module,contexts)
    await subject.run()
    ctx=contexts[0]
    if not enabled:
        subject.mainland_proxy_factory.assert_not_called()
        assert ctx.sandbox_network is None
        assert dict(ctx.sandbox_environment)["NO_PROXY"]=="*"
        return
    controlled.start.assert_awaited_once()
    controlled.stop.assert_awaited_once()
    assert ctx.sandbox_network=="controlled-internal"
    assert ctx.tool_network_profile["applied"] is True
    env=dict(ctx.sandbox_environment)
    assert env["HTTPS_PROXY"]==controlled.environment["HTTPS_PROXY"]
    assert "db" in env["NO_PROXY"] and "*" not in env["NO_PROXY"]
    verifier=_isolated_verifier_start_options(ctx)
    assert verifier.network==ctx.sandbox_network
    assert dict(verifier.environment)==env


@pytest.mark.parametrize("failure",[DriverError,asyncio.CancelledError])
async def test_cli_gateway_failure_never_starts_trial(monkeypatch,tmp_path,failure):
    import loom_cli.local_runner as module
    driver=FakeDriver()
    driver.capabilities=driver.capabilities.model_copy(update={"backend":"docker","supports_custom_network":True})
    subject=_runner(tmp_path,{"name":"pytest"},driver)
    subject.trial_config=stub_trial_config(mainland_proxy=True)
    controlled=gateway()
    controlled.start.side_effect=failure("network setup failed")
    subject.mainland_proxy_factory=lambda: controlled
    trial=Mock()
    monkeypatch.setattr(module,"Trial",trial)
    with pytest.raises(failure,match="network setup failed"):
        await subject.run()
    trial.assert_not_called()
    controlled.stop.assert_awaited_once()
    assert driver.state=="constructed"


@pytest.mark.parametrize("failure",[DriverError,asyncio.CancelledError])
async def test_worker_gateway_failure_never_starts_agent(tmp_path,failure):
    subject=worker_runner(tmp_path,None)
    subject.start_authorization=None
    subject.trial_config=stub_trial_config(mainland_proxy=True)
    driver=FakeDriver()
    driver.capabilities=driver.capabilities.model_copy(update={"backend":"docker","supports_custom_network":True})
    subject.driver_factory=Mock(return_value=driver)
    controlled=gateway()
    controlled.start.side_effect=failure("network setup failed")
    subject.mainland_proxy_factory=lambda: controlled
    with pytest.raises(failure,match="network setup failed"):
        await subject.run()
    subject.agent_factory.assert_not_called()
    controlled.stop.assert_awaited_once()
    assert driver.state=="constructed"


def test_effective_profile_survives_atif_projection():
    profile={"enabled":True,"applied":True,"loaded_rule_counts":{"gfw":4443,"greatfire":2}}
    event=TrialStartEvent(emitted_at=datetime.now(UTC),trial_id=uuid4(),step_id="__trial__",seq=0,task_id="sample",agent_name="oracle",agent_mode="out-of-box",tool_network_profile=profile)
    projected=project_to_atif([event],task_id="sample",agent_name="oracle",agent_version="1")
    assert projected.metadata.model_dump()["tool_network_profile"]==profile
    ordinary=event.model_copy(update={"tool_network_profile":None})
    assert "tool_network_profile" not in ordinary.model_dump()
    assert "tool_network_profile" not in project_to_atif([ordinary],task_id="sample",agent_name="oracle",agent_version="1").metadata.model_dump()


def test_native_readback_distinguishes_request_and_frozen_profile():
    from loom.execution_selection import execution_selection_readback
    from loom.mainland_network_profile import MainlandNetworkProfileV1
    profile=MainlandNetworkProfileV1(provider_version="snapshot",gfw_suffixes=("blocked.example",))
    plan=SimpleNamespace(verifier_execution="in_attempt",in_place_verifier=True,execution_class_id="sample",effective_network_policy=None,sidecars=[],mainland_proxy_profile=profile)
    readback=execution_selection_readback({"mainland_proxy":True},plan)
    assert readback["requested"]["mainland_proxy"] is True
    assert readback["effective"]["mainland_proxy"]["provider_version"]=="snapshot"
    assert "mainland_proxy" not in execution_selection_readback({},None)["requested"]


async def test_controlled_policy_preserves_legacy_restrictions():
    from loom.models.networking import Allowlist, NoNetwork, Public
    from loom.trial.phase_network import controlled_tool_policy_setter, phase_network
    driver=FakeDriver()
    await driver.start()
    gateway_policy=AsyncMock()
    apply=controlled_tool_policy_setter(gateway_policy,"http://172.20.0.2:7890")
    await apply(driver,NoNetwork())
    assert driver.network_policy.kind=="no-network"
    allowed=Allowlist(domains=("pypi.org",))
    await apply(driver,allowed)
    assert "172.20.0.2/32" in driver.network_policy.cidrs
    await apply(driver,Public())
    assert driver.network_policy.kind=="public"
    async with phase_network(driver,baseline=Public(),phase=NoNetwork(),set_policy=apply):
        assert driver.network_policy.kind=="no-network"
    assert driver.network_policy.kind=="public"
    assert gateway_policy.await_args.args[0].kind=="public"
