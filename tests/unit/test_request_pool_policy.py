"""Boundary cases for upstream-provided retry delays and stateless pool input."""

from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from loom_llm_gateway.request_pool import validate_pool_payload
from loom_llm_gateway.retry import retry_after_seconds
from loom_service.routes.provider_request_pools import RequestPoolConfig


@pytest.mark.parametrize("value", ["inf", "-inf", "NaN", "1e1000", "not-a-date"])
def test_untrusted_retry_after_is_finite(value: str) -> None:
    assert retry_after_seconds(httpx.Response(429, headers={"Retry-After": value})) == 0


def test_plain_text_and_client_function_tools_supported() -> None:
    validate_pool_payload(
        {
            "messages": [{"role": "user", "content": "terminal output"}],
            "tools": [{"type": "function", "function": {"name": "run"}}],
        }
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"tools": [{"type": "web_search"}]},
        {"file_id": "x"},
        {"extra_body": {"previous_response_id": "x"}},
    ],
)
def test_extra_body_cannot_opt_into_hosted_resources(extra: dict) -> None:
    with pytest.raises(HTTPException):
        validate_pool_payload(
            {"messages": [{"role": "user", "content": "hi"}], "extra_body": extra}
        )


def _pool_config(**overrides: object) -> RequestPoolConfig:
    payload: dict[str, object] = {
        "required_models": ["glm-5.3"],
        "equivalent_models": True,
        "members": [
            {"connection_id": str(uuid4()), "quota_group": "a"},
            {"connection_id": str(uuid4()), "quota_group": "b"},
        ],
    }
    payload.update(overrides)
    return RequestPoolConfig.model_validate(payload)


def test_stall_sec_allows_a_day_long_bound() -> None:
    assert _pool_config(stall_sec=86400).stall_sec == 86400


def test_stall_sec_rejects_above_a_day() -> None:
    with pytest.raises(ValidationError):
        _pool_config(stall_sec=86401)
