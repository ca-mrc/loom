"""Claude selection modes follow authorized connections through batch fan-out."""
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from loom.models.trial import TrialConfig
from loom_service.agent_catalog import get_agent, validate_agent_model_compat
from loom_service.batch_runner import _materialize_trial_config
from loom_service.effective_combination import freeze_connection_modes
from loom_service.execution_admission import submission_trials
from loom_service.provider_connection_lookup import connection_runtime_config, validate_provider_connection


def _claude(**extra):
    return {"agent_name": "claude-code", "agent_model": {"provider": "vendor", "name": "code"}, **extra}


def test_catalog_admits_claude_with_either_connection_protocol_and_any_namespace():
    agent = get_agent("claude-code")
    assert agent.product_support == "supported"
    assert set(agent.to_dict()["supported_connection_types"]) == {"anthropic", "openai-compatible", "custom"}
    assert validate_agent_model_compat("claude-code", TrialConfig(**_claude()).agent_model) is None
    assert "anthropic" not in get_agent("codex").supported_connection_types


@pytest.mark.parametrize(("kind", "mode"), [("anthropic", "native"), ("openai-compatible", "chat"), ("custom", "chat")])
def test_connection_mode_comes_from_type_not_model_namespace(kind, mode):
    assert connection_runtime_config(_claude(), kind)["claude_code_mode"] == mode
    with pytest.raises(HTTPException, match="conflicts"):
        connection_runtime_config(_claude(claude_code_mode="chat" if mode == "native" else "native"), kind)


def test_other_agents_and_protocols_do_not_silently_get_claude_routing():
    for config, kind in [(_claude(), "google"), ({"agent_name": "codex"}, "anthropic"),
                         ({"agent_name": "codex", "claude_code_mode": "chat"}, "custom")]:
        with pytest.raises(HTTPException):
            connection_runtime_config(config, kind)


@pytest.mark.parametrize("params", [{"temperature": .5}, {"unknown_option": True}])
def test_claude_explicit_generation_controls_fail_before_sanitizing(params):
    with pytest.raises(ValidationError, match="request_params"):
        TrialConfig(**_claude(request_params=params))
    with pytest.raises(HTTPException, match="request_params"):
        connection_runtime_config(_claude(request_params=params), "anthropic")


def test_mixed_batch_freezes_independent_modes_and_keeps_them_at_admission_and_fanout():
    a, b = uuid4(), uuid4()
    shared = {"skip_verifier": True}
    combos = [_claude(provider_connection_id=str(a)), _claude(provider_connection_id=str(b)),
              {"agent_name": "codex", "agent_model": {"provider": "vendor", "name": "code"}, "provider_connection_id": str(b)}]
    freeze_connection_modes(shared, combos, connection_id=a, provider_type="anthropic")
    freeze_connection_modes(shared, combos, connection_id=b, provider_type="custom")
    assert "claude_code_mode" not in shared and "claude_code_mode" not in combos[2]
    assert [trial.claude_code_mode for trial in submission_trials(shared, combos)] == ["native", "chat", None]
    assert [_materialize_trial_config(shared, c).get("claude_code_mode") for c in combos] == ["native", "chat", None]


def test_rerun_freezes_only_selected_combinations():
    connection = uuid4()
    combos = [_claude(provider_connection_id=str(connection)), _claude(provider_connection_id=str(connection))]
    freeze_connection_modes({}, combos, connection_id=connection, provider_type="anthropic", indices={1})
    assert "claude_code_mode" not in combos[0] and combos[1]["claude_code_mode"] == "native"


async def test_authorized_shared_native_connection_accepts_claude_but_hides_foreign():
    owner, team, connection = uuid4(), uuid4(), uuid4()
    class Session:
        shared = True
        async def execute(self, statement):
            class Result:
                def one_or_none(inner):
                    return (owner, None, "anthropic")
                def scalar_one_or_none(inner):
                    return connection if self.shared else None
            return Result()
        async def get(self, model, key):
            return None  # no request pool
    session = Session()
    assert await validate_provider_connection(session, connection, team_id=team, agent_submission=True,
                                              submission_configs=[_claude()]) == "anthropic"
    session.shared = False
    with pytest.raises(HTTPException) as error:
        await validate_provider_connection(session, connection, team_id=team, agent_submission=True,
                                           submission_configs=[_claude()])
    assert error.value.status_code == 404
