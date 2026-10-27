"""Local Harbor cost uses raw normalized counters without charging cache twice."""

from uuid import uuid4

import pytest

from loom.agent.direct_provider import DirectProviderConnection
from loom.models.types import ModelSpec
from loom_cli.rate_cards import RateCardEntry, RateCardTable
from loom_cli.terminus_ledger import LocalHarborLedger


@pytest.mark.parametrize("provider,name,rate_provider,output,written,expected", [
    ("anthropic", "model", "anthropic", 20, 5, 0.00013625),
    ("openai", "model", "openai", 20, 0, 0.000135),
    ("google", "model", "google", 27, 0, 0.000149),
    ("local", "dev/model", "local:dev", 20, 0, 0.000135),
])
def test_normalized_usage_prices_disjoint_token_buckets(
    provider: str, name: str, rate_provider: str, output: int, written: int, expected: float,
) -> None:
    model = ModelSpec(provider=provider, name=name)
    ledger = LocalHarborLedger(
        trial_id=uuid4(), model=model,
        connection=DirectProviderConnection(model=model, api_key="test-only"),
        rate_cards=RateCardTable(entries=(RateCardEntry(
            provider=rate_provider, model="model", input_per_mtok=1, output_per_mtok=2,
            cache_read_per_mtok=0.5, cache_write_per_mtok=1.25,
        ),)),
    )
    cost, fingerprint = ledger._price({
        "prompt_tokens": 100, "completion_tokens": output,
        "prompt_tokens_details": {"cached_tokens": 10, "cache_creation_tokens": written},
    })
    assert cost == pytest.approx(expected)
    assert len(fingerprint) == 64
