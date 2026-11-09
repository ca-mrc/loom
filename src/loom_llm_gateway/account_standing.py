"""Closed account-standing 400s; not a generic Access-denied matcher."""

from __future__ import annotations

import httpx

_ACCOUNT_STANDING_CODES = frozenset({"arrearage"})


def account_standing_400(response: httpx.Response) -> bool:
    """True only for HTTP 400 with error.type or error.code Arrearage."""
    if response.status_code != 400:
        return False
    try:
        payload = response.json()
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    if not isinstance(error, dict):
        return False
    for key in ("type", "code"):
        value = error.get(key)
        if isinstance(value, str) and value.strip().casefold() in _ACCOUNT_STANDING_CODES:
            return True
    return False
