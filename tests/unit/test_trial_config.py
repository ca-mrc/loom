import pytest
from pydantic import ValidationError

from loom.models.trial import RetryPolicy, RetryReason, TrialConfig
from loom.models.types import ModelSpec

# Plan 23: every TrialConfig must specify agent_name + agent_model
# (no fallback to TaskConfig). These stubs keep tests focused on the
# fields they're actually exercising.
_AGENT = "oracle"
_MODEL = ModelSpec(provider="local", name="stub")


def test_trial_config_keeps_historical_workspace_staging_policy_name():
    c = TrialConfig(
        agent_name=_AGENT,
        agent_model=_MODEL,
        workspace_staging_policy_name="none",
    )
    assert c.workspace_staging_policy_name == "none"


def test_trial_config_defaults():
    c = TrialConfig(agent_name=_AGENT, agent_model=_MODEL)
    assert c.schema_version == "1"
    assert c.force_build is False
    assert c.delete_env is True
    assert c.skip_verifier is False
    assert c.verifier_env_mode is None
    assert c.agent_timeout_multiplier == 1.0
    assert c.submit_priority == 100
    assert c.request_params == {}
    assert c.retry == RetryPolicy()


def test_trial_config_override_agent_timeout():
    c = TrialConfig(
        agent_name=_AGENT, agent_model=_MODEL,
        override_agent_timeout_sec=900.0,
    )
    assert c.override_agent_timeout_sec == 900.0


def test_trial_config_priority_bounds():
    TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=0)
    TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=1000)
    with pytest.raises(ValidationError):
        TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=-1)
    with pytest.raises(ValidationError):
        TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=1001)


def test_trial_config_retry_on_set():
    c = TrialConfig(
        agent_name=_AGENT, agent_model=_MODEL,
        retry=RetryPolicy(
            max_attempts=3,
            retry_on=frozenset({RetryReason.WORKER_CRASH, RetryReason.AGENT_TIMEOUT}),
        ),
    )
    assert RetryReason.WORKER_CRASH in c.retry.retry_on


def test_trial_config_missing_agent_name_422s():
    with pytest.raises(ValidationError):
        TrialConfig(agent_model=_MODEL)  # type: ignore[call-arg]


def test_trial_config_missing_agent_model_422s():
    with pytest.raises(ValidationError):
        TrialConfig(agent_name=_AGENT)  # type: ignore[call-arg]


def test_trial_config_empty_agent_name_422s():
    with pytest.raises(ValidationError):
        TrialConfig(agent_name="", agent_model=_MODEL)


def test_trial_config_family_run_roundtrip():
    """#672 PR-3 hot-path: the optional family_run block round-trips
    through model_validate and dump so the batches route can persist
    it and the resolver merges it with catalog defaults."""
    payload = {
        "agent_name": _AGENT,
        "agent_model": _MODEL.model_dump(mode="json"),
        "family_run": {
            "enabled": True,
            "adapter": {"name": "skill_patcher_llm", "params": {}},
            "mount_path": "/root/.skills",
        },
    }
    c = TrialConfig.model_validate(payload)
    assert c.family_run is not None
    assert c.family_run.enabled is True
    assert c.family_run.adapter is not None
    assert c.family_run.adapter.name == "skill_patcher_llm"
    assert c.family_run.mount_path == "/root/.skills"
    dumped = c.model_dump(mode="json")
    assert dumped["family_run"]["enabled"] is True


def test_trial_config_family_run_absent_default():
    """Backward compatibility: omitting family_run keeps the field None
    and the batch runs in the classic mode."""
    c = TrialConfig(agent_name=_AGENT, agent_model=_MODEL)
    assert c.family_run is None
    dumped = c.model_dump(mode="json")
    assert dumped["family_run"] is None


def test_trial_config_family_run_unknown_key_forbidden():
    """Extra=forbid on FamilyRunSpec rejects typo'd role names so a
    misconfigured trial_config fails at submission, not at claim time."""
    with pytest.raises(ValidationError):
        TrialConfig.model_validate({
            "agent_name": _AGENT,
            "agent_model": _MODEL.model_dump(mode="json"),
            "family_run": {"enabled": True, "unknown_role": {"name": "x"}},
        })


def test_trial_config_sanitizes_request_params():
    c = TrialConfig(
        agent_name=_AGENT,
        agent_model=_MODEL,
        request_params={
            "temperature": 0,
            "top_p": 0.5,
            "seed": 1234,
            "messages": [{"role": "user", "content": "secret"}],
            "api_key": "sk-hidden",
            "extra_body": {
                "top_k": 40,
                "prompt": "secret",
            },
        },
    )

    assert c.request_params == {
        "temperature": 0,
        "top_p": 0.5,
        "seed": 1234,
        "extra_body": {"top_k": 40},
    }
