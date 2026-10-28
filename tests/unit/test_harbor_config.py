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


@pytest.mark.parametrize(
    ("request_limit", "harbor_limit"),
    [
        ("max_tokens", "max_completion_tokens"),
        ("max_completion_tokens", "max_tokens"),
    ],
)
@pytest.mark.parametrize("trial_model", [False, True], ids=["api_normalizer", "trial_model"])
def test_rejects_output_limits_split_across_config_inputs(
    request_limit: str, harbor_limit: str, trial_model: bool,
) -> None:
    config = {
        "request_params": {request_limit: 16},
        "harbor_config": {harbor_limit: 32},
    }
    with pytest.raises(ValueError, match="max_tokens or max_completion_tokens, not both"):
        if trial_model:
            _trial(**config)
        else:
            apply_harbor_config_dict(config)


@pytest.mark.parametrize("limit", ["max_tokens", "max_completion_tokens"])
def test_output_limit_keeps_same_field_conflict(limit: str) -> None:
    with pytest.raises(ValueError, match=f"harbor_config.{limit} conflicts with request_params.{limit}"):
        apply_harbor_config_dict({
            "request_params": {limit: 16},
            "harbor_config": {limit: 32},
        })


@pytest.mark.parametrize("limit", ["max_tokens", "max_completion_tokens"])
def test_matching_output_limit_merges_idempotently(limit: str) -> None:
    config = {
        "request_params": {limit: 16, "temperature": 0},
        "harbor_config": {limit: 16, "top_p": 0.9},
    }
    once = apply_harbor_config_dict(config)
    assert once["request_params"] == {limit: 16, "temperature": 0, "top_p": 0.9}
    assert apply_harbor_config_dict(once) == once
    assert _trial(**once).request_params == once["request_params"]


def test_stale_running_reads_harbor_agent_timeout() -> None:
    assert (
        effective_agent_timeout_sec(
            task_config={"agent": {"timeout_sec": 900}},
            trial_config={"harbor_config": {"agent_timeout_sec": 7200}},
        )
        == 7200
    )
