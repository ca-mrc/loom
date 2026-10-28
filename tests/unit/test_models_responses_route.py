"""#2054: `/models` reports how Gateway would serve a Codex request."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from loom_service.routes.models import _responses_route

PROBED = datetime(2026, 10, 27, tzinfo=UTC)


@pytest.mark.parametrize(("provider_type", "supported", "probed_at", "route"), [
    ("openai-compatible", None, None, "unprobed"),
    # A probe that neither confirmed nor ruled out native Responses.
    ("openai-compatible", None, PROBED, "inconclusive"),
    ("openai-compatible", True, PROBED, "native"),
    ("custom", False, PROBED, "translated"),
    ("anthropic", None, None, "unsupported"),
])
def test_responses_route_separates_unprobed_from_inconclusive(
    provider_type: str, supported: bool | None, probed_at: datetime | None, route: str,
) -> None:
    row = SimpleNamespace(provider_type=provider_type, responses_api_supported=supported,
                          responses_api_probed_at=probed_at)

    assert _responses_route(row) == route  # type: ignore[arg-type]
