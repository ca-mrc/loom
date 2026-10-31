"""Shared classification of cached upstream model preflight outcomes."""

from __future__ import annotations

# A failed preflight is either a *rejection* (the upstream refused this
# key/model: 401/403, unknown model, egress policy) or *inconclusive* (the
# probe never got a definitive answer: timeout, network error, 408/429/5xx).
# Only rejections may block batch submission (#948); an inconclusive probe
# is not evidence the model is unusable, and blocking on it indefinitely
# strands a valid connection until someone happens to re-run preflight.
_INCONCLUSIVE_PREFLIGHT_ERROR_CODES = frozenset(
    {"timeout", "request-error", "unexpected-error"},
)


def classify_preflight_failure(
    error_code: str | None,
    http_status: int | None,
) -> str:
    """Return ``'inconclusive'`` or ``'rejected'`` for a failed preflight.

    Unknown codes without a transient HTTP status stay ``'rejected'`` so
    genuine auth/model incompatibility remains fail-closed.
    """
    if error_code in _INCONCLUSIVE_PREFLIGHT_ERROR_CODES:
        return "inconclusive"
    if error_code == "model-not-found":
        # The upstream names the model as unroutable, whatever the status:
        # OpenAI-compatible relays answer 503 when no channel serves it.
        return "rejected"
    if http_status is not None and (http_status in (408, 429) or http_status >= 500):
        return "inconclusive"
    return "rejected"


def preflight_failure_kind(
    status: str | None,
    error_code: str | None,
    http_status: int | None,
) -> str | None:
    """Failure kind for a cached preflight row; ``None`` unless it failed."""
    if status != "failed":
        return None
    return classify_preflight_failure(error_code, http_status)
