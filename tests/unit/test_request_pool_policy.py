"""Boundary cases for upstream-provided retry delays and stateless pool input."""

import httpx
import pytest
from fastapi import HTTPException

from loom_llm_gateway.request_pool import validate_pool_payload
from loom_llm_gateway.retry import retry_after_seconds


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
