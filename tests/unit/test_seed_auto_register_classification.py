"""`_auto_register_benchmarks` classifies HF 401/404 as `not_published`.

The noise from this status was the user's actual complaint after the
series/tags rollout: every `loom service up` printed multi-line "error
RepositoryNotFoundError" tracebacks for every adapter that hadn't been
published to PRHW yet, drowning out the actual seed status. The fix is
classification, not behavior change — adapters that aren't published
still get a stub row, but the seed reports them as `not_published`
instead of `error <traceback>`."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError
from scripts.seed_test_data import _auto_register_benchmarks


class _DummyAdapter:
    series = None


_FAKE_REGISTRY = {"fake-published": _DummyAdapter(), "fake-missing": _DummyAdapter()}


@pytest.fixture
def fake_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loom_benchmarks.registry.REGISTRY", _FAKE_REGISTRY,
    )


def _async_dispatcher(by_slug: dict[str, Exception | dict[str, int]]) -> AsyncMock:
    """Build an AsyncMock that, when called with `benchmark=<slug>`,
    either raises or returns the mapped value."""
    async def _side_effect(*, benchmark: str, **_kw: object) -> dict[str, int]:
        outcome = by_slug[benchmark]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome
    return AsyncMock(side_effect=_side_effect)


def test_repository_not_found_classified_as_not_published(
    fake_registry: None,
) -> None:
    dispatcher = _async_dispatcher({
        "fake-missing": RepositoryNotFoundError("repo doesn't exist on HF"),
        "fake-published": {"registered": 0, "skipped": 0},
    })
    with patch(
        "loom_benchmark_tool.register_cmd.run_register", new=dispatcher,
    ):
        results = _auto_register_benchmarks(
            db_url="postgresql://noop", hf_org="PRHW", hf_token=None,
        )
    assert results["fake-missing"] == "not_published"
    assert results["fake-published"].startswith("ok ")


def test_entry_not_found_classified_as_not_published(
    fake_registry: None,
) -> None:
    """manifest.json missing → EntryNotFoundError. Same classification
    bucket as RepositoryNotFoundError — both mean "not yet published"."""
    dispatcher = _async_dispatcher({
        "fake-missing": EntryNotFoundError("manifest.json not found"),
        "fake-published": EntryNotFoundError("manifest.json not found"),
    })
    with patch(
        "loom_benchmark_tool.register_cmd.run_register", new=dispatcher,
    ):
        results = _auto_register_benchmarks(
            db_url="postgresql://noop", hf_org="PRHW", hf_token=None,
        )
    assert all(v == "not_published" for v in results.values())


def test_other_errors_still_surface_as_error(
    fake_registry: None,
) -> None:
    """Non-published-related failures keep the loud `error` classification
    so they're still visible — we're quieting only the steady-state
    noise, not all errors."""
    dispatcher = _async_dispatcher({
        "fake-missing": RuntimeError("kaboom"),
        "fake-published": RuntimeError("kaboom"),
    })
    with patch(
        "loom_benchmark_tool.register_cmd.run_register", new=dispatcher,
    ):
        results = _auto_register_benchmarks(
            db_url="postgresql://noop", hf_org="PRHW", hf_token=None,
        )
    assert all(v.startswith("error RuntimeError") for v in results.values())


def test_dev_seed_registers_without_prebuilding_task_images(fake_registry: None) -> None:
    """A laptop stack builds task images on first use; queueing every
    registered benchmark image filled the disk in about an hour (#2836)."""
    dispatcher = _async_dispatcher({
        "fake-missing": RepositoryNotFoundError("repo doesn't exist on HF"),
        "fake-published": {"registered": 1, "skipped": 0},
    })
    with patch("loom_benchmark_tool.register_cmd.run_register", new=dispatcher):
        _auto_register_benchmarks(db_url="postgresql://noop", hf_org="PRHW", hf_token=None)
    assert dispatcher.await_args_list
    assert all(call.kwargs["prebuild_task_images"] is False for call in dispatcher.await_args_list)
