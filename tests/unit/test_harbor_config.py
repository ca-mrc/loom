"""Harbor config split between the Terminus constructor and LiteLLM kwargs (#2667)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.models.harbor_config import HarborConfig, apply_harbor_config_dict
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trial.stale_running import effective_agent_timeout_sec

_AGENT = "terminus-2"
_MODEL = ModelSpec(provider="openai", name="glm")


def _trial(**kwargs: object) -> TrialConfig:
    return TrialConfig(agent_name=_AGENT, agent_model=_MODEL, **kwargs)  # type: ignore[arg-type]


def test_harbor_config_merges_deadline_and_completion_params() -> None:
    trial = _trial(
        harbor_config={
            "max_turns": 128,
            "agent_timeout_sec": 7200,
            "max_input_tokens": 262144,
            "enable_summarize": False,
            "temperature": 1,
            "top_p": 0.9,
            "max_tokens": 16384,
        }
    )
    assert trial.override_agent_timeout_sec == 7200
    assert trial.request_params == {"temperature": 1, "top_p": 0.9, "max_tokens": 16384}
    assert trial.harbor_config is not None
    assert trial.harbor_config.max_turns == 128
    assert trial.harbor_config.max_input_tokens == 262144
    assert trial.harbor_config.enable_summarize is False
    assert "max_input_tokens" not in trial.request_params
    assert "max_turns" not in trial.request_params


def test_harbor_config_rejects_conflicting_timeout_and_sampling() -> None:
    with pytest.raises(ValidationError, match="agent_timeout_sec"):
        _trial(
            override_agent_timeout_sec=900,
            harbor_config={"agent_timeout_sec": 7200},
        )
    with pytest.raises(ValidationError, match="temperature"):
        _trial(
            request_params={"temperature": 0},
            harbor_config={"temperature": 1},
        )
    with pytest.raises(ValidationError, match="max_tokens or max_completion_tokens"):
        HarborConfig(max_tokens=10, max_completion_tokens=10)


def test_apply_harbor_config_dict_is_idempotent() -> None:
    once = apply_harbor_config_dict(
        {
            "harbor_config": {"agent_timeout_sec": 7200, "top_p": 0.9},
        }
    )
    twice = apply_harbor_config_dict(once)
    assert twice["override_agent_timeout_sec"] == 7200
    assert twice["request_params"] == {"top_p": 0.9}


def test_stale_running_reads_harbor_agent_timeout() -> None:
    assert (
        effective_agent_timeout_sec(
            task_config={"agent": {"timeout_sec": 900}},
            trial_config={"harbor_config": {"agent_timeout_sec": 7200}},
        )
        == 7200
    )
