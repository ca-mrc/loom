"""Shared fixtures for operator-script tests."""

import os

import pytest


@pytest.fixture(autouse=True)
def _skip_pool_journal_fsync(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool state-machine tests exercise journal content, not disk durability.

    Their journals live in tmp_path and are written thousands of times per case.
    Durability ordering keeps real coverage in the certificate and DNS writer
    tests, which record fsync calls themselves.
    """
    if request.module.__name__.rsplit(".", 1)[-1].startswith("test_nebius_pool_"):
        monkeypatch.setattr(os, "fsync", lambda descriptor: None)


# The shared worker pool is not in use: dev deploys in standalone mode
# (NEBIUS_ROLLOUT_MODE unset) and the one shared handoff was rolled back. Its
# handoff/recovery simulations cost ~240 CI runner-minutes per run, and no other
# test imports them. They stay collected and owned; set this to run them.
_SHARED_POOL_OPT_IN = "LOOM_RUN_SHARED_POOL_TESTS"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get(_SHARED_POOL_OPT_IN) == "1":
        return
    skip = pytest.mark.skip(reason=f"shared worker pool is not in use; set {_SHARED_POOL_OPT_IN}=1 to run")
    for item in items:
        if item.path.name.startswith("test_nebius_shared_pool_"):
            item.add_marker(skip)
