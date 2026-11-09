"""Closed Arrearage 400s hop; standing text or other 4xx do not."""

from __future__ import annotations

import httpx
import pytest

from loom_llm_gateway.account_standing import account_standing_400

_STANDING_MESSAGE = "Access denied, please make sure your account is in good standing."


def _response(
    status: int, json: object | None = None, *, text: str | None = None
) -> httpx.Response:
    if text is not None:
        return httpx.Response(status, text=text)
    return httpx.Response(status, json=json)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "error": {
                "message": _STANDING_MESSAGE,
                "type": "Arrearage",
                "param": "",
                "code": "Arrearage",
            }
        },
        {"error": {"type": "Arrearage"}},
        {"error": {"type": "arrearage"}},
        {"error": {"code": "Arrearage"}},
        {"error": {"code": "arrearage"}},
        {"error": {"type": "  Arrearage  ", "message": "other"}},
        {"error": {"code": "  arrearage  "}},
        {"error": {"type": "Arrearage", "code": "invalid_request"}},
        {"error": {"type": "invalid_request", "code": "Arrearage"}},
    ],
)
def test_arrearage_type_or_code_is_account_standing(payload: dict) -> None:
    assert account_standing_400(_response(400, payload)) is True


@pytest.mark.parametrize(
    "status,payload",
    [
        (400, {"error": "same-member"}),
        (
            400,
            {
                "error": {
                    "type": "invalid_request",
                    "code": "invalid_request",
                    "message": _STANDING_MESSAGE,
                }
            },
        ),
        (400, {"error": {"message": _STANDING_MESSAGE}}),
        (400, {"error": {"type": "access_denied", "message": "Access denied"}}),
        (400, {"message": _STANDING_MESSAGE, "type": "Arrearage"}),
        (400, {}),
        (400, {"error": None}),
        (400, {"error": {"type": 1, "code": 1}}),
        (401, {"error": {"type": "Arrearage", "code": "Arrearage"}}),
        (403, {"error": {"type": "Arrearage", "code": "Arrearage"}}),
        (429, {"error": {"type": "Arrearage", "code": "Arrearage"}}),
        (500, {"error": {"type": "Arrearage", "code": "Arrearage"}}),
        (400, ["Arrearage"]),
        (400, {"error": ["Arrearage"]}),
    ],
)
def test_other_errors_are_not_account_standing(status: int, payload: object) -> None:
    assert account_standing_400(_response(status, payload)) is False


def test_unparseable_body_is_not_account_standing() -> None:
    assert account_standing_400(_response(400, text="{not-json")) is False
