"""Gateway correlation helpers for #1380."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.agent.terminus2.gateway_ledger import GatewayCallLedger
from loom_llm_gateway.model_switch_correlation import (
    canonical_facade_model_id,
    extract_and_strip_loom_fields,
    persist_correlated_intent,
)


def test_extract_and_strip_loom_fields_from_extra_body() -> None:
    payload = {
        "model": "glm-5.1",
        "messages": [],
        "extra_body": {
            "loom_client_call_id": "abc",
            "temperature": 0.2,
        },
        "loom_role": "student",
    }
    stripped, extras = extract_and_strip_loom_fields(payload)
    assert "loom_role" not in stripped
    assert stripped["extra_body"] == {"temperature": 0.2}
    assert extras["loom_client_call_id"] == "abc"
    assert extras["loom_role"] == "student"
    assert payload["extra_body"]["loom_client_call_id"] == "abc"


@pytest.mark.asyncio
async def test_gateway_ledger_prefers_correlated_episode() -> None:
    trial_id = uuid4()
    ledger = GatewayCallLedger(trial_id=trial_id, step_id="agent")

    class _Cp:
        async def get_trial_llm_calls(self, tid):  # type: ignore[no-untyped-def]
            assert tid == trial_id
            return [
                {
                    "id": "legacy",
                    "step_id": "agent",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "correlation_status": "legacy_uncorrelated",
                },
                {
                    "id": "corr",
                    "step_id": "agent",
                    "episode": 3,
                    "call_ordinal": 1,
                    "correlation_status": "correlated",
                    "input_tokens": 99,
                    "output_tokens": 99,
                },
            ]

    await ledger.refresh(_Cp())
    row = ledger.resolve_for_metrics(
        {"prompt_tokens": 10, "completion_tokens": 5},
        episode=3,
    )
    assert row["id"] == "corr"


def test_extract_empty_is_legacy() -> None:
    stripped, extras = extract_and_strip_loom_fields({"model": "x"})
    assert extras == {}
    assert stripped["model"] == "x"


def test_canonical_facade_model_id_strips_openai_prefix() -> None:
    assert canonical_facade_model_id("openai/glm-5.2") == "glm-5.2"
    assert canonical_facade_model_id("glm-5.2") == "glm-5.2"
    assert canonical_facade_model_id("openai/openai/glm-5.2") == "openai/glm-5.2"


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar_one_or_none(self) -> object:
        return self._value


class _FakeSession:
    def __init__(self, plan: object) -> None:
        self.plan = plan
        self.committed = False
        self._calls = 0

    async def execute(self, _stmt: object) -> _ScalarResult:
        self._calls += 1
        if self._calls == 1:
            return _ScalarResult(self.plan)
        return _ScalarResult(None)

    async def commit(self) -> None:
        self.committed = True


def _switch_plan() -> SimpleNamespace:
    return SimpleNamespace(
        provider_connection_id=None,
        student_model_snapshot={"name": "glm-5.2"},
        teacher_model_snapshot={"name": "glm-5.2-urg"},
        k1=2,
        k2=4,
    )


def _beta_plan() -> SimpleNamespace:
    return SimpleNamespace(
        provider_connection_id=None,
        student_model_snapshot={"name": "glm-5.2"},
        teacher_model_snapshot={"name": "glm-5.2-urg"},
        mix_mode="beta_mixture",
        k1=None,
        k2=None,
        beta=1.0,
        seed="s",
    )


def _student_extras(*, requested_model: str) -> dict[str, object]:
    return {
        "loom_client_call_id": str(uuid4()),
        "loom_agent_execution_id": str(uuid4()),
        "loom_agent_run_attempt_id": str(uuid4()),
        "loom_episode": 1,
        "loom_call_ordinal": 1,
        "loom_requested_model": requested_model,
        "loom_role": "student",
    }


@pytest.mark.asyncio
async def test_persist_accepts_openai_prefixed_loom_requested_model() -> None:
    """LiteLLM posts model=glm-5.2; the router stamps openai/glm-5.2."""
    session = _FakeSession(_switch_plan())
    result = await persist_correlated_intent(
        session,  # type: ignore[arg-type]
        trial_id=uuid4(),
        step_id="agent",
        extras=_student_extras(requested_model="openai/glm-5.2"),
        jwt_connection_id=None,
        requested_model="glm-5.2",
    )
    assert result["correlation_status"] == "correlated"
    assert result["requested_model"] == "glm-5.2"
    assert session.committed is True


@pytest.mark.asyncio
async def test_persist_beta_plan_accepts_episode_1_teacher() -> None:
    extras = _student_extras(requested_model="openai/glm-5.2-urg")
    extras["loom_role"] = "teacher"
    extras["loom_episode"] = 1
    session = _FakeSession(_beta_plan())
    result = await persist_correlated_intent(
        session,  # type: ignore[arg-type]
        trial_id=uuid4(),
        step_id="agent",
        extras=extras,
        jwt_connection_id=None,
        requested_model="glm-5.2-urg",
    )
    assert result["correlation_status"] == "correlated"
    assert result["role"] == "teacher"


def _turn_plan() -> SimpleNamespace:
    return SimpleNamespace(
        provider_connection_id=None,
        student_model_snapshot={"name": "glm-5.2"},
        teacher_model_snapshot={"name": "glm-5.2-urg"},
        mix_mode="student_to_teacher_turns",
        k1=2,
        k2=3,
        beta=None,
        seed="turn-seed",
    )


@pytest.mark.asyncio
async def test_persist_turn_plan_forces_teacher_at_step_end() -> None:
    extras = _student_extras(requested_model="openai/glm-5.2-urg")
    extras["loom_role"] = "teacher"
    extras["loom_call_ordinal"] = 3
    session = _FakeSession(_turn_plan())
    result = await persist_correlated_intent(
        session,  # type: ignore[arg-type]
        trial_id=uuid4(),
        step_id="agent",
        extras=extras,
        jwt_connection_id=None,
        requested_model="glm-5.2-urg",
    )
    assert result["correlation_status"] == "correlated"
    assert result["role"] == "teacher"


@pytest.mark.asyncio
async def test_persist_turn_plan_rejects_wrong_role_before_window() -> None:
    extras = _student_extras(requested_model="openai/glm-5.2-urg")
    extras["loom_role"] = "teacher"
    extras["loom_call_ordinal"] = 1
    session = _FakeSession(_turn_plan())
    with pytest.raises(HTTPException) as exc:
        await persist_correlated_intent(
            session,  # type: ignore[arg-type]
            trial_id=uuid4(),
            step_id="agent",
            extras=extras,
            jwt_connection_id=None,
            requested_model="glm-5.2-urg",
        )
    assert exc.value.status_code == 400
    assert "does not match plan role" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_persist_sts_plan_accepts_teacher_inside_window() -> None:
    extras = _student_extras(requested_model="openai/glm-5.2-urg")
    extras["loom_role"] = "teacher"
    extras["loom_episode"] = 2
    session = _FakeSession(_switch_plan())
    result = await persist_correlated_intent(
        session,  # type: ignore[arg-type]
        trial_id=uuid4(),
        step_id="agent",
        extras=extras,
        jwt_connection_id=None,
        requested_model="glm-5.2-urg",
    )
    assert result["correlation_status"] == "correlated"
    assert result["role"] == "teacher"


@pytest.mark.asyncio
async def test_persist_sts_plan_rejects_teacher_before_k1() -> None:
    extras = _student_extras(requested_model="openai/glm-5.2-urg")
    extras["loom_role"] = "teacher"
    extras["loom_episode"] = 1
    session = _FakeSession(_switch_plan())
    with pytest.raises(HTTPException) as exc:
        await persist_correlated_intent(
            session,  # type: ignore[arg-type]
            trial_id=uuid4(),
            step_id="agent",
            extras=extras,
            jwt_connection_id=None,
            requested_model="glm-5.2-urg",
        )
    assert exc.value.status_code == 400
    assert "does not match plan role" in str(exc.value.detail)


@pytest.mark.asyncio
async def test_persist_still_rejects_different_models() -> None:
    session = _FakeSession(_switch_plan())
    with pytest.raises(HTTPException) as exc:
        await persist_correlated_intent(
            session,  # type: ignore[arg-type]
            trial_id=uuid4(),
            step_id="agent",
            extras=_student_extras(requested_model="openai/glm-5.2-urg"),
            jwt_connection_id=None,
            requested_model="glm-5.2",
        )
    assert exc.value.status_code == 400
    assert exc.value.detail == "requested model does not match loom_requested_model"
