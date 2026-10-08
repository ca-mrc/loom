"""Complete repair rollback cases, independently scheduled from local phases."""
from __future__ import annotations

import pytest
from scripts.ops.nebius_ingress_stage import _key
from tests.ops.test_nebius_pool_startup_repair import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_startup_repair import (
    build_inputs as build_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    builder_cutover_inputs as builder_cutover_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    checks as checks,
)
from tests.ops.test_nebius_pool_startup_repair import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_startup_repair import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_startup_repair import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_startup_repair import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    historical_cutover as historical_cutover,
)
from tests.ops.test_nebius_pool_startup_repair import (
    installation as installation,
)
from tests.ops.test_nebius_pool_startup_repair import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    material as material,
)
from tests.ops.test_nebius_pool_startup_repair import (
    platform_inputs as platform_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    prepared_repair as prepared_repair,
)
from tests.ops.test_nebius_pool_startup_repair import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_startup_repair import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_startup_repair import (
    repair,
)
from tests.ops.test_nebius_pool_startup_repair import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_startup_repair import (
    runtime_inputs as runtime_inputs,
)


@pytest.mark.parametrize('phase', ['stop', 'template', 'start', 'complete'])
@pytest.mark.timeout(420)
def test_repair_rollback_restores_templates_roles_and_completes_legacy(prepared_repair, monkeypatch, phase):
    from types import SimpleNamespace

    from scripts.ops import nebius_pool_operation as target
    from scripts.ops.nebius_management_switch import _stable
    from tests.ops.test_nebius_pool_gateway_retirement import GatewayAPI
    from tests.ops.test_nebius_pool_legacy_reopening import ReopeningAPI
    from tests.ops.test_nebius_pool_legacy_restart import RestartAPI
    from tests.ops.test_nebius_pool_machine_retirement import MachineAPI
    from tests.ops.test_nebius_pool_role_restoration import RoleAPI
    from tests.ops.test_nebius_pool_template_restoration import TemplateAPI

    context, binding, remote, state, anchor = prepared_repair
    if phase != 'complete':
        remote.failure = (phase, 'before')
    repair(prepared_repair)
    fixture = context.request, context.tokens, remote.closed, remote.startup, None, state.parent
    machine = MachineAPI(fixture)
    gateway = GatewayAPI(fixture, machine)
    template = TemplateAPI(fixture, gateway)
    roles = RoleAPI(fixture, template)
    restart = RestartAPI(fixture, roles)

    class RecoveryAPI(ReopeningAPI):
        def successor_drained(self, key, desired):
            assert (desired['spec']['suspend'] is True if desired['kind'] == 'CronJob'
                else desired['spec']['replicas'] == 0)
            return self.processes_drained

    runtime = RecoveryAPI(fixture, restart)
    runtime.anchor = anchor
    parent = SimpleNamespace(request=context.request, state_dir=state, anchor_dir=anchor, refresh=None)
    monkeypatch.setattr(target, 'HTTPSPoolActivationAPI', lambda **kwargs: runtime)
    result = target.run_pool_operation(parent=parent, tokens=context.tokens, action='rollback', repair_binding=binding)
    assert result['status'] == 'pool_cutover_completed' and result['outcome'] == 'legacy'
    assert result['acceptance_verified'] is False
    assert runtime.mode == 'fenced' and runtime.machine_phase == 'revoked'
    assert set(runtime.guards.values()) == {'open'}
    assert _stable(runtime.read_workload(_key(context.request.manager)))['spec'] == _stable(context.request.manager)['spec']
    assert runtime.template_calls and runtime.legacy_calls and runtime.releases
    history = {path: path.read_bytes() for directory in (state, anchor) for path in directory.rglob('*.json')}
    assert target.run_pool_operation(parent=parent, tokens=context.tokens, action='rollback', repair_binding=binding) == result
    assert all(path.read_bytes() == raw for path, raw in history.items())
