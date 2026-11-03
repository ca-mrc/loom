"""The execution ledger must not transfer provider request/response logs."""
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from loom.llm_call_ledger import read_service_execution_llm_calls


@pytest.mark.asyncio
async def test_execution_ledger_projects_accounting_without_raw_log_transfer():
    statements = []

    class Session:
        async def execute(self, stmt):
            statements.append(stmt)
            return SimpleNamespace(all=lambda: [])

        scalars = execute

    lease = SimpleNamespace(id=uuid4(), team_id=uuid4(), trial_id=uuid4(), generation=3)
    assert await read_service_execution_llm_calls(Session(), lease) == []
    sql = str(statements[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    selection, where = sql.split("\nFROM", 1)
    assert "llm_calls.provider_extras - '_loom_raw_provider_log'" in selection
    assert " AS finish_reason" in selection
    assert "service_execution" in where and "generation" in where
    assert str(lease.id) in where and str(lease.team_id) in where and str(lease.trial_id) in where


def test_gateway_retry_budget_cause_survives_safe_accounting_projection():
    from datetime import UTC, datetime
    from decimal import Decimal

    from loom.db.schema import LlmCall
    from loom.llm_call_ledger import serialize_execution_accounting_call

    row = LlmCall(
        id=uuid4(), trial_id=uuid4(), step_id="agent", model="glm-5.2",
        dialect="openai_facade", input_tokens=0, output_tokens=0,
        cost_usd=Decimal("0"), rate_card_hash="none", captured_at=datetime.now(UTC),
        attempt=2, provider_extras={
            "_loom_call_status": "failed", "_loom_usage_status": "missing",
            "_loom_failure_category": "gateway_retry_budget_exhausted",
            "private_detail": "must not export",
        },
    )
    safe = serialize_execution_accounting_call(row)
    assert safe["provider_extras"] == {
        "_loom_call_status": "failed", "_loom_usage_status": "missing",
        "_loom_failure_category": "gateway_retry_budget_exhausted",
    }
