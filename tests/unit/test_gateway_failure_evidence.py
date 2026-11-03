from __future__ import annotations

from uuid import uuid4

from loom.models.trajectory import LLMCallEvent
from loom.trajectory.llm_call_events import llm_call_row_to_event


def test_retry_budget_failure_category_survives_trajectory_serialization() -> None:
    call = llm_call_row_to_event(
        {
            "model": "model", "dialect": "openai_facade", "rate_card_hash": "failed-upstream",
            "provider_extras": {
                "_loom_call_status": "failed",
                "_loom_usage_status": "missing",
                "_loom_failure_category": "gateway_retry_budget_exhausted",
            },
        },
        trial_id=uuid4(), seq=1,
    )
    assert call.failure_category == "gateway_retry_budget_exhausted"
    exported = LLMCallEvent.model_validate_json(call.model_dump_json())
    assert exported.failure_category == "gateway_retry_budget_exhausted"
    assert exported.call_status == "failed"
    assert exported.usage_status == "missing"
