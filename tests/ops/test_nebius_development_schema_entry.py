"""The installed runner selects schema history only within its pinned dev scope."""
from __future__ import annotations

import importlib
import json
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_environment_lifecycle_entry import configuration, private


@pytest.mark.parametrize('action', ['prepare', 'apply', 'recover', 'status'])
async def test_schema_entry_preserves_identity_only_protocol(tmp_path, monkeypatch, action):
    name = 'scripts.ops.nebius_development_schema_entry'
    assert importlib.util.find_spec(name), 'installed schema successor dispatch is missing'
    entry = importlib.import_module(name)
    from scripts.ops import nebius_environment_lifecycle as lifecycle
    from scripts.ops import nebius_environment_lifecycle_gateway as gateway
    from scripts.ops.nebius_environment_lifecycle_entry import (
        LifecycleCommandV2,
        load_lifecycle_configuration,
    )

    path, _, body, operation_id, _ = configuration(tmp_path)
    body['operations'][operation_id]['kind'] = 'schema-upgrade'
    checksum = private(path, body)
    config = load_lifecycle_configuration(path, checksum)
    request = LifecycleCommandV2.model_validate(dict(schema_version='loom.environment-lifecycle-command.v2',
        action=action, scope=body['scope'], operation_id=operation_id))
    scope = config.scope.model_dump(exclude={'authority'})
    retained = SimpleNamespace(projection=SimpleNamespace(participant=SimpleNamespace(
        **scope, environment_class='development')))
    calls = []

    def load(reference, *, operator):
        assert operator
        assert reference.operation_id == request.operation_id
        assert reference.inputs_path == config.operations[request.operation_id].inputs_path
        assert reference.inputs_sha256 == config.operations[request.operation_id].inputs_sha256
        calls.append('load')
        return retained

    @contextmanager
    def connect(selected, *, unchanged):
        assert selected is retained
        unchanged()
        calls.append('connect')
        yield 'authenticated-api'

    def run(**kwargs):
        assert kwargs == dict(retained=retained, api='authenticated-api', action=action)
        calls.append(action)
        return dict(status='prepared' if action == 'prepare' else 'complete',
            operation_id=operation_id, admission_open=False)

    monkeypatch.setattr(entry, 'load_development_schema_upgrade', load)
    monkeypatch.setattr(entry, 'connected_api', connect)
    monkeypatch.setattr(entry, 'run_development_schema_upgrade', run)
    result = await lifecycle.execute_lifecycle(configuration=config, request=request,
        operation=config.operations[request.operation_id], unchanged=lambda: load_lifecycle_configuration(path, checksum))
    assert calls == ['load', 'connect', action]
    assert result['admission_open'] is False
    assert result['status'] == ('prepared' if action == 'prepare' else 'observed' if action == 'status' else 'complete')
    assert gateway.safe_report(json.dumps(result).encode(), request.model_dump_json().encode(), action) == result

    retained.projection.participant.environment_id = uuid4()
    calls.clear()
    with pytest.raises(ValueError, match='scope'):
        await lifecycle.execute_lifecycle(configuration=config, request=request,
            operation=config.operations[request.operation_id], unchanged=lambda: config)
    assert calls == ['load']
