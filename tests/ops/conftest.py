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
    # The successor simulation was split unchanged from the ordinary module.
    module = request.module.__name__.rsplit(".", 1)[-1]
    if module.startswith("test_nebius_pool_") or module == "test_nebius_shared_pool_refresh_successor":
        monkeypatch.setattr(os, "fsync", lambda descriptor: None)
