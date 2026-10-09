"""Unit tests for student/teacher/student model switch (#1380)."""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.agent.terminus2.model_switch import (
    assert_terminus2_switch_contract,
    construct_teacher_llm,
    deterministic_episode_draw,
    gateway_llm_kwargs_for_teacher,
    install_role_router,
    redact_agent_llm_kwargs,
    role_for_beta_episode,
    role_for_episode,
)
from loom.errors import AgentError
from loom.models.trial import (
    MultiModelSwitchSpec,
    TrialConfig,
    materialize_multi_model_switch_episode,
)
from loom.models.types import ModelSpec
from loom_service.multi_model import (
    apply_multi_model_materialization,
    apply_plan_mode,
    parse_multi_model,
    validate_multi_model_for_batch,
)

_PRIMARY = ModelSpec(provider="openai", name="glm-5.1", source="api")
_SECONDARY = ModelSpec(provider="openai", name="qwen-test", source="api")


def test_multi_model_spec_requires_secondary_when_enabled() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(enabled=True)


def test_multi_model_spec_rejects_switch_above_ceiling() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            secondary_model=_SECONDARY,
            switch_episode=10,
            episode_ceiling=5,
        )


def test_trial_config_accepts_multi_model() -> None:
    c = TrialConfig(
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        multi_model=MultiModelSwitchSpec(
            enabled=True,
            secondary_model=_SECONDARY,
            switch_episode=3,
            teacher_episodes=2,
        ),
    )
    assert c.multi_model is not None
    assert c.multi_model.switch_episode == 3
    assert c.multi_model.policy == "student_teacher_student"


def test_materialize_persists_k1_and_k2() -> None:
    rng = random.Random(0)
    raw = {
        "enabled": True,
        "secondary_model": _SECONDARY.model_dump(mode="json"),
        "episode_ceiling": 4,
        "teacher_episodes": 2,
    }
    first = materialize_multi_model_switch_episode(raw, rng=rng)
    assert first is not None
    assert first["switch_episode"] in {2, 3, 4}
    assert first["return_switch_episode"] == first["switch_episode"] + 2
    second = materialize_multi_model_switch_episode(first, rng=rng)
    assert second is not None
    assert second["switch_episode"] == first["switch_episode"]
    assert second["return_switch_episode"] == first["return_switch_episode"]


def test_apply_multi_model_materialization_idempotent() -> None:
    cfg = {
        "agent_name": "terminus-2",
        "agent_model": _PRIMARY.model_dump(mode="json"),
        "multi_model": {
            "enabled": True,
            "secondary_model": _SECONDARY.model_dump(mode="json"),
            "switch_episode": 3,
            "episode_ceiling": 50,
        },
    }
    out = apply_multi_model_materialization(cfg)
    assert out["multi_model"]["switch_episode"] == 3
    assert out["multi_model"]["return_switch_episode"] == 5
    assert out["multi_model"]["teacher_episodes"] == 2


def test_role_for_episode_two_cuts() -> None:
    assert role_for_episode(1, first_switch_episode=3, return_switch_episode=5) == "student"
    assert role_for_episode(3, first_switch_episode=3, return_switch_episode=5) == "teacher"
    assert role_for_episode(4, first_switch_episode=3, return_switch_episode=5) == "teacher"
    assert role_for_episode(5, first_switch_episode=3, return_switch_episode=5) == "student"


def test_beta_spec_rejects_k1() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="beta_mixture",
            secondary_model=_SECONDARY,
            beta=0.6,
            switch_episode=3,
        )


def test_beta_spec_requires_beta() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="beta_mixture",
            secondary_model=_SECONDARY,
        )


def test_schedule_spec_rejects_beta() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            secondary_model=_SECONDARY,
            beta=0.6,
        )


def test_materialize_beta_does_not_sample_k1() -> None:
    raw = {
        "enabled": True,
        "policy": "beta_mixture",
        "secondary_model": _SECONDARY.model_dump(mode="json"),
        "beta": 0.6,
        "mix_seed": "42",
    }
    out = materialize_multi_model_switch_episode(raw)
    assert out is not None
    assert out["policy"] == "beta_mixture"
    assert out["beta"] == 0.6
    assert "switch_episode" not in out
    assert "return_switch_episode" not in out


def test_role_for_beta_teacher_if_draw_lt_beta() -> None:
    trial = "11111111-1111-1111-1111-111111111111"
    seed = "abc"
    draw = deterministic_episode_draw(seed, trial, 1)
    role = role_for_beta_episode(1, beta=draw + 1e-12, seed=seed, trial_id=trial)
    assert role == "teacher"
    role_s = role_for_beta_episode(1, beta=draw, seed=seed, trial_id=trial)
    assert role_s == "student"
    assert role_for_beta_episode(1, beta=1.0, seed=seed, trial_id=trial) == "teacher"
    assert role_for_beta_episode(1, beta=0.0, seed=seed, trial_id=trial) == "student"
    assert role_for_beta_episode(1, beta=0.6, seed=seed, trial_id=trial) == (
        role_for_beta_episode(1, beta=0.6, seed=seed, trial_id=trial)
    )


def test_apply_plan_mode_resample_keeps_beta() -> None:
    cfg = {
        "agent_name": "terminus-2",
        "agent_model": _PRIMARY.model_dump(mode="json"),
        "multi_model": {
            "enabled": True,
            "policy": "beta_mixture",
            "secondary_model": _SECONDARY.model_dump(mode="json"),
            "beta": 0.6,
            "mix_seed": "keep-me",
        },
    }
    resampled = apply_plan_mode(cfg, mode="resample")
    assert resampled["multi_model"]["beta"] == 0.6
    assert resampled["multi_model"]["policy"] == "beta_mixture"
    assert "switch_episode" not in resampled["multi_model"]


def test_turn_schedule_spec_requires_window() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="student_to_teacher_turns",
            secondary_model=_SECONDARY,
            step_start=2,
        )
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="student_to_teacher_turns",
            secondary_model=_SECONDARY,
            step_start=5,
            step_end=3,
        )


def test_turn_schedule_spec_rejects_beta_and_k1() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="student_to_teacher_turns",
            secondary_model=_SECONDARY,
            step_start=2,
            step_end=9,
            beta=0.5,
        )
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="student_to_teacher_turns",
            secondary_model=_SECONDARY,
            step_start=2,
            step_end=9,
            switch_episode=3,
        )


def test_turn_beta_edges_and_monotonic() -> None:
    from loom.agent.terminus2.model_switch import turn_beta

    assert turn_beta(1, step_start=2, step_end=9) == 0.0
    assert turn_beta(2, step_start=2, step_end=9) == 0.0
    assert turn_beta(9, step_start=2, step_end=9) == 1.0
    mid_a = turn_beta(4, step_start=2, step_end=9)
    mid_b = turn_beta(6, step_start=2, step_end=9)
    assert 0.0 < mid_a < mid_b < 1.0


def test_role_for_turn_schedule_forces_and_latches() -> None:
    from loom.agent.terminus2.model_switch import role_for_turn_schedule

    trial = "22222222-2222-2222-2222-222222222222"
    seed = "turn-seed"
    assert role_for_turn_schedule(
        1, step_start=3, step_end=6, seed=seed, trial_id=trial,
    ) == "student"
    # Force at end regardless of draw.
    assert role_for_turn_schedule(
        6, step_start=3, step_end=6, seed=seed, trial_id=trial,
    ) == "teacher"
    # After any teacher, later turns stay teacher (force at end also latches).
    assert role_for_turn_schedule(
        8, step_start=3, step_end=6, seed=seed, trial_id=trial,
    ) == "teacher"


@pytest.mark.asyncio
async def test_role_router_turn_schedule_student_then_force() -> None:
    student = _FakeLLM(name="openai/glm-5.1-student")
    teacher = _FakeLLM(name="openai/qwen-teacher")
    agent = _FakeAgent(_llm=student, _model_name="openai/glm-5.1-student")
    router = install_role_router(
        agent,
        teacher_model_name="openai/qwen-teacher",
        mix_mode="student_to_teacher_turns",
        step_start=3,
        step_end=4,
        seed="s",
        trial_id="trial-turn",
        teacher_llm=teacher,
    )
    agent._n_episodes = 1
    await router.call(prompt="t1")
    await router.call(prompt="t2")
    assert student.calls == [
        "openai/glm-5.1-student",
        "openai/glm-5.1-student",
    ]
    await router.call(prompt="t3")  # still before force at 4; beta at start=0
    assert len(student.calls) == 3
    await router.call(prompt="t4")  # force teacher
    assert teacher.calls == ["openai/qwen-teacher"]
    await router.call(prompt="t5")  # latch
    assert teacher.calls == ["openai/qwen-teacher", "openai/qwen-teacher"]
    assert router._call_ordinal == 5


@dataclass
class _FakeLLM:
    name: str
    calls: list[str] = field(default_factory=list)
    last_kwargs: dict[str, Any] = field(default_factory=dict)

    async def call(self, prompt: str = "", **kwargs: Any) -> str:
        del prompt
        self.last_kwargs = dict(kwargs)
        self.calls.append(self.name)
        return self.name

    def get_model_context_limit(self) -> int:
        return 100 if self.name.endswith("student") else 200

    def get_model_output_limit(self) -> int | None:
        return 10


@dataclass
class _FakeAgent:
    _llm: Any
    _model_name: str
    _n_episodes: int = 0
    _init_llm: Any = True
    _llm_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"api_key": "loom_step_secret", "timeout": 1},
    )


@pytest.mark.asyncio
async def test_role_router_student_teacher_student() -> None:
    student = _FakeLLM(name="openai/glm-5.1-student")
    teacher = _FakeLLM(name="openai/qwen-teacher")
    agent = _FakeAgent(_llm=student, _model_name="openai/glm-5.1-student")
    router = install_role_router(
        agent,
        teacher_model_name="openai/qwen-teacher",
        first_switch_episode=3,
        return_switch_episode=5,
        teacher_llm=teacher,
    )

    agent._n_episodes = 1
    await router.call(prompt="a", previous_response_id="resp-a")
    agent._n_episodes = 2
    await router.call(prompt="b", previous_response_id="resp-b")
    await router.call(prompt="b-retry", previous_response_id="resp-b2")
    assert student.calls == [
        "openai/glm-5.1-student",
        "openai/glm-5.1-student",
        "openai/glm-5.1-student",
    ]
    assert teacher.calls == []
    assert agent._model_name == "openai/glm-5.1-student"

    agent._n_episodes = 3
    await router.call(prompt="c", previous_response_id="resp-student")
    assert teacher.last_kwargs.get("previous_response_id") is None
    agent._n_episodes = 4
    await router.call(prompt="d", previous_response_id="resp-teacher")
    assert teacher.calls == ["openai/qwen-teacher", "openai/qwen-teacher"]

    agent._n_episodes = 5
    await router.call(prompt="e", previous_response_id="resp-teacher-end")
    assert student.last_kwargs.get("previous_response_id") is None
    assert student.calls[-1] == "openai/glm-5.1-student"
    assert [c["to_role"] for c in router.applied_switches] == ["teacher", "student"]
    assert agent._llm_kwargs == {"timeout": 1}
    assert router.get_model_context_limit() == 100


@pytest.mark.asyncio
async def test_role_router_beta_all_teacher() -> None:
    student = _FakeLLM(name="openai/glm-5.1-student")
    teacher = _FakeLLM(name="openai/qwen-teacher")
    agent = _FakeAgent(_llm=student, _model_name="openai/glm-5.1-student")
    router = install_role_router(
        agent,
        teacher_model_name="openai/qwen-teacher",
        mix_mode="beta_mixture",
        beta=1.0,
        seed="s",
        trial_id="trial-1",
        teacher_llm=teacher,
    )
    agent._n_episodes = 1
    await router.call(prompt="a")
    agent._n_episodes = 2
    await router.call(prompt="b")
    assert teacher.calls == ["openai/qwen-teacher", "openai/qwen-teacher"]
    assert student.calls == []
    assert [c["to_role"] for c in router.applied_switches] == ["teacher"]


def test_redact_agent_llm_kwargs() -> None:
    agent = _FakeAgent(_llm=_FakeLLM("x"), _model_name="x")
    redact_agent_llm_kwargs(agent)
    assert "api_key" not in agent._llm_kwargs
    assert agent._llm_kwargs["timeout"] == 1


@dataclass
class _StudentLiteLLM:
    """Mirrors Harbor LiteLLM: live constructor kwargs keep api_key."""

    name: str
    _llm_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"api_key": "loom_step_live", "timeout": 1},
    )
    _api_base: str | None = "http://gateway.test"
    _session_id: str | None = "sess"
    _max_thinking_tokens: int | None = None
    _use_responses_api: bool = False


@dataclass
class _ConstructAgent:
    _llm: Any
    _model_name: str = "openai/student"
    _n_episodes: int = 0
    _temperature: float | None = None
    _collect_rollout_details: bool = False
    _reasoning_effort: str | None = None
    _session_id: str | None = "agent-sess"
    _llm_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"api_key": "loom_step_dump", "timeout": 1},
    )
    captured_init: dict[str, Any] = field(default_factory=dict)

    def _resolve_model_info(self, model_name: str, model_info: Any) -> None:
        del model_name, model_info
        return None

    def _init_llm(self, **kwargs: Any) -> Any:
        self.captured_init = dict(kwargs)
        return _FakeLLM(name=str(kwargs.get("model_name") or "teacher"))


@dataclass
class _HarborOptions:
    """Harbor 0.24 stores constructor options separately from live LiteLLM."""

    llm_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"api_key": "loom_step_dump", "timeout": 9},
    )
    temperature: float | None = 0.25
    collect_rollout_details: bool = True
    reasoning_effort: str | None = "high"
    api_base: str | None = "http://gateway.test"
    max_thinking_tokens: int | None = 2048
    use_responses_api: bool = True


@dataclass
class _ConstructOptionsAgent:
    _llm: Any
    options: _HarborOptions = field(default_factory=_HarborOptions)
    _model_name: str = "openai/student"
    _n_episodes: int = 0
    _session_id: str = "agent-sess"
    captured_init: dict[str, Any] = field(default_factory=dict)

    def _resolve_model_info(self, model_name: str, model_info: Any) -> None:
        del model_name, model_info
        return None

    def _init_llm(self, **kwargs: Any) -> Any:
        self.captured_init = dict(kwargs)
        return _FakeLLM(name=str(kwargs.get("model_name") or "teacher"))


@pytest.fixture
def stub_harbor_llm_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit tests do not install Harbor; stub the backend enum import."""
    import sys
    import types

    harbor = types.ModuleType("harbor")
    harbor_llms = types.ModuleType("harbor.llms")
    harbor_base = types.ModuleType("harbor.llms.base")

    class LLMBackend:
        LITELLM = "litellm"

    harbor_base.LLMBackend = LLMBackend  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "harbor", harbor)
    monkeypatch.setitem(sys.modules, "harbor.llms", harbor_llms)
    monkeypatch.setitem(sys.modules, "harbor.llms.base", harbor_base)

def test_gateway_llm_kwargs_uses_agent_dump_when_present() -> None:
    agent = _ConstructAgent(_llm=_StudentLiteLLM("openai/student"))
    kwargs = gateway_llm_kwargs_for_teacher(agent)
    assert kwargs["api_key"] == "loom_step_dump"


def test_gateway_llm_kwargs_recovers_from_student_after_redact() -> None:
    """Regression: early redact of agent._llm_kwargs must not starve teacher."""
    student = _StudentLiteLLM(
        "openai/student",
        _llm_kwargs={"api_key": "loom_step_live", "timeout": 1},
    )
    agent = _ConstructAgent(
        _llm=student,
        _llm_kwargs={"api_key": "loom_step_dump", "timeout": 1},
    )
    redact_agent_llm_kwargs(agent)
    assert "api_key" not in agent._llm_kwargs
    kwargs = gateway_llm_kwargs_for_teacher(agent)
    assert kwargs["api_key"] == "loom_step_live"
    assert kwargs["timeout"] == 1


def test_gateway_llm_kwargs_fails_closed_without_any_api_key() -> None:
    student = _StudentLiteLLM("openai/student", _llm_kwargs={"timeout": 1})
    agent = _ConstructAgent(_llm=student, _llm_kwargs={"timeout": 1})
    with pytest.raises(AgentError, match="missing gateway api_key"):
        gateway_llm_kwargs_for_teacher(agent)


def test_construct_teacher_llm_survives_premature_redact(
    stub_harbor_llm_backend: None,
) -> None:
    student = _StudentLiteLLM(
        "openai/student",
        _llm_kwargs={"api_key": "loom_step_live", "timeout": 9},
    )
    agent = _ConstructAgent(
        _llm=student,
        _llm_kwargs={"api_key": "loom_step_dump", "timeout": 9},
    )
    redact_agent_llm_kwargs(agent)
    teacher = construct_teacher_llm(agent, teacher_model_name="openai/teacher")
    assert isinstance(teacher, _FakeLLM)
    assert agent.captured_init["llm_kwargs"]["api_key"] == "loom_step_live"
    assert agent.captured_init["api_base"] == "http://gateway.test"


@pytest.mark.parametrize("redact_first", [False, True])
def test_options_teacher_preserves_auth_and_constructor_parameters(
    redact_first: bool,
    stub_harbor_llm_backend: None,
) -> None:
    student = _StudentLiteLLM("openai/student")
    agent = _ConstructOptionsAgent(_llm=student)
    if redact_first:
        redact_agent_llm_kwargs(agent)
    construct_teacher_llm(agent, teacher_model_name="openai/teacher")
    assert agent.captured_init["llm_kwargs"] == {
        "api_key": "loom_step_live" if redact_first else "loom_step_dump",
        "timeout": 9,
    }
    assert agent.captured_init["temperature"] == 0.25
    assert agent.captured_init["collect_rollout_details"] is True
    assert agent.captured_init["reasoning_effort"] == "high"
    assert agent.captured_init["api_base"] == "http://gateway.test"
    assert agent.captured_init["session_id"] == "agent-sess"
    assert agent.captured_init["max_thinking_tokens"] == 2048
    assert agent.captured_init["use_responses_api"] is True


def test_options_redaction_keeps_live_student_credentials() -> None:
    # Even if the constructor shared the dictionary, replace the dump field;
    # mutating it in place would revoke the student's live gateway credential.
    kwargs = {"api_key": "loom_step_live", "timeout": 9}
    student = _StudentLiteLLM("openai/student", _llm_kwargs=kwargs)
    agent = _ConstructOptionsAgent(_llm=student, options=_HarborOptions(llm_kwargs=kwargs))
    redact_agent_llm_kwargs(agent)
    assert agent.options.llm_kwargs == {"timeout": 9}
    assert student._llm_kwargs == kwargs
    assert gateway_llm_kwargs_for_teacher(agent) == kwargs


@pytest.mark.parametrize(
    "mix_kwargs",
    [
        {
            "mix_mode": "student_teacher_student",
            "first_switch_episode": 3,
            "return_switch_episode": 5,
        },
        {
            "mix_mode": "beta_mixture",
            "beta": 1.0,
            "seed": "s",
            "trial_id": "t1",
        },
        {
            "mix_mode": "student_to_teacher_turns",
            "step_start": 2,
            "step_end": 5,
            "seed": "s",
            "trial_id": "t1",
        },
    ],
)
@pytest.mark.parametrize("options_api", [False, True])
def test_install_role_router_passes_jwt_then_redacts_dump(
    mix_kwargs: dict[str, Any],
    options_api: bool,
    stub_harbor_llm_backend: None,
) -> None:
    """Every mix policy constructs teacher with JWT, then scrubs dump kwargs."""
    student = _StudentLiteLLM(
        "openai/student",
        _llm_kwargs={"api_key": "loom_step_live", "timeout": 1},
    )
    kwargs = {"api_key": "loom_step_dump", "timeout": 1}
    agent = (
        _ConstructOptionsAgent(_llm=student, options=_HarborOptions(llm_kwargs=kwargs))
        if options_api
        else _ConstructAgent(_llm=student, _llm_kwargs=kwargs)
    )
    router = install_role_router(
        agent,
        teacher_model_name="openai/teacher",
        **mix_kwargs,
    )
    assert agent.captured_init["llm_kwargs"]["api_key"] == "loom_step_dump"
    dumped_kwargs = agent.options.llm_kwargs if options_api else agent._llm_kwargs
    assert "api_key" not in dumped_kwargs
    assert agent._llm is router
    # Live student LiteLLM still holds JWT (Harbor dump field is separate).
    assert student._llm_kwargs["api_key"] == "loom_step_live"


def test_scrub_harbor_trajectory_drops_step_api_key(tmp_path: Path) -> None:
    from loom.agent.terminus2.runtime import (
        _assert_harbor_artifacts_have_no_step_secrets,
        _scrub_harbor_trajectory_llm_kwargs,
    )

    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "agent": {
                    "name": "terminus-2",
                    "extra": {
                        "parser": "xml",
                        "llm_kwargs": {
                            "api_key": "loom_step_should_not_publish",
                            "timeout": 1,
                        },
                    },
                },
                "steps": [{"step_id": 1, "message": 'code mentions "api_key" ok'}],
            },
        ),
        encoding="utf-8",
    )
    _scrub_harbor_trajectory_llm_kwargs(traj)
    payload = json.loads(traj.read_text(encoding="utf-8"))
    assert payload["agent"]["extra"]["llm_kwargs"] == {"timeout": 1}
    # Bare "api_key" in step text must not fail publish after scrub.
    _assert_harbor_artifacts_have_no_step_secrets(tmp_path)


def test_assert_harbor_artifacts_still_blocks_raw_step_jwt(tmp_path: Path) -> None:
    from loom.agent.terminus2.runtime import (
        _assert_harbor_artifacts_have_no_step_secrets,
    )

    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "agent": {"name": "terminus-2", "extra": {}},
                "steps": [{"step_id": 1, "message": "leak loom_step_abcdef.payload"}],
            },
        ),
        encoding="utf-8",
    )
    with pytest.raises(AgentError, match="step credential material"):
        _assert_harbor_artifacts_have_no_step_secrets(tmp_path)


def test_assert_terminus2_switch_contract_missing_attrs() -> None:
    with pytest.raises(AgentError):
        assert_terminus2_switch_contract(object())


def test_validate_multi_model_rejects_non_terminus() -> None:
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": _SECONDARY.model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="litellm",
        agent_model=_PRIMARY,
        provider_connection_id=uuid4(),
        provider_connection=None,
    )
    assert err is not None
    assert "terminus-2" in err


def test_validate_multi_model_rejects_missing_connection() -> None:
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": _SECONDARY.model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        provider_connection_id=None,
        provider_connection=None,
    )
    assert err is not None
    assert "provider_connection_id" in err


def test_validate_multi_model_rejects_provider_mismatch() -> None:
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": ModelSpec(
                    provider="anthropic", name="claude", source="api",
                ).model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        provider_connection_id=uuid4(),
        provider_connection=None,
    )
    assert err is not None
    assert "provider" in err


@dataclass
class _FakeConnection:
    allowed_models: list[str] | None


def test_validate_multi_model_allowed_models() -> None:
    conn = _FakeConnection(allowed_models=["glm-5.1"])
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": _SECONDARY.model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        provider_connection_id=uuid4(),
        provider_connection=conn,  # type: ignore[arg-type]
    )
    assert err is not None
    assert "allowed_models" in err


def test_parse_multi_model_rejects_seed_internals() -> None:
    with pytest.raises(ValueError, match="seed internals"):
        parse_multi_model({"multi_model": {"enabled": False, "seed": "abc"}})


def test_apply_plan_mode_resample_redraws_k1() -> None:
    cfg = {
        "agent_name": "terminus-2",
        "agent_model": _PRIMARY.model_dump(mode="json"),
        "multi_model": {
            "enabled": True,
            "secondary_model": _SECONDARY.model_dump(mode="json"),
            "switch_episode": 3,
            "return_switch_episode": 5,
            "teacher_episodes": 2,
            "episode_ceiling": 4,
        },
    }
    inherited = apply_plan_mode(cfg, mode="inherit")
    assert inherited["multi_model"]["switch_episode"] == 3
    resampled = apply_plan_mode(cfg, mode="resample")
    assert resampled["multi_model"]["switch_episode"] in {2, 3, 4}
    assert resampled["multi_model"]["return_switch_episode"] == (
        resampled["multi_model"]["switch_episode"] + 2
    )


def test_required_capabilities_blocks_old_workers() -> None:
    from loom.models.capabilities import Capabilities, RequiredCapabilities

    req = RequiredCapabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public"]),
        terminus2_model_switch=True,
    )
    old = Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto"]),
        terminus2_model_switch=False,
    )
    new = old.model_copy(update={"terminus2_model_switch": True})
    assert not req.satisfied_by(old)
    assert req.satisfied_by(new)
