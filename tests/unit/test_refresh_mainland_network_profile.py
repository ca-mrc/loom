"""Snapshot preparation is offline-testable and never overwrites a profile."""

from __future__ import annotations

import json
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from scripts import refresh_mainland_network_profile as updater

from loom.mainland_network_profile import load_mainland_network_profile

NOW = datetime(2026, 10, 28, 18, tzinfo=UTC)
TAG = "202610272235"
ASSET = f"{updater.RELEASE_ROOT}/download/{TAG}/gfw.txt"
RULES = b"payload:\n  - '+.github.com'\n"


def observation(url: str, **updates: object) -> dict[str, object]:
    return {
        "url": url,
        "found": True,
        "headline": "blocked",
        "stale": False,
        "last_tested": (NOW - timedelta(days=1)).isoformat(),
        "window_days": 90,
        **updates,
    }


class PublicSources:
    def __init__(self, data: dict[str, object] | None = None, rules: bytes = RULES):
        self.data = data or {}
        self.rules = rules
        self.calls: list[tuple[str, float]] = []

    def __call__(self, url: str, timeout: float) -> bytes:
        self.calls.append((url, timeout))
        if url == updater.LATEST_RELEASE_URL:
            return json.dumps(
                {
                    "tag_name": TAG,
                    "assets": [
                        {"name": "gfw.txt", "browser_download_url": ASSET},
                    ],
                }
            ).encode()
        if url == ASSET:
            return self.rules
        assert url.startswith(updater.GREATFIRE_API_ROOT)
        root = url.removeprefix(updater.GREATFIRE_API_ROOT).replace("/", "://", 1)
        value = self.data.get(root, observation(root))
        if isinstance(value, BaseException):
            raise value
        return json.dumps(value).encode()


def test_fetches_a_fixed_release_and_publishes_a_loadable_four_file_snapshot(tmp_path: Path):
    roots = ["https://GitHub.com/", "https://gitlab.com", "https://gitlab.com"]
    sources = PublicSources(
        {"https://gitlab.com": observation("https://gitlab.com", headline="intermittent")}
    )
    output = tmp_path / "version-1"
    result = updater.refresh_profile(output, roots, timeout=7, fetch=sources, now=NOW)
    assert result == load_mainland_network_profile(output)
    assert result.provider_version == TAG
    assert result.greatfire_hosts == ("github.com", "gitlab.com")
    assert {path.name for path in output.iterdir()} == {
        "gfw-rules.yaml",
        "greatfire-rules.yaml",
        "sources.json",
        "greatfire-decisions.json",
    }
    assert sources.calls == [
        (updater.LATEST_RELEASE_URL, 7),
        (ASSET, 7),
        (updater.GREATFIRE_API_ROOT + "https/github.com", 7),
        (updater.GREATFIRE_API_ROOT + "https/gitlab.com", 7),
    ]
    metadata = json.loads((output / "sources.json").read_text())
    assert metadata["gfw"]["provider_url"] == ASSET
    assert "CC BY 4.0" in metadata["greatfire"]["attribution"]
    assert not any(path.name.startswith(".") for path in tmp_path.iterdir())


@pytest.mark.parametrize(
    "url",
    [
        "https://user:secret@github.com",
        "https://@github.com",
        "https://github.com/repo",
        "https://github.com?token=secret",
        "https://github.com#secret",
        "https://github.com?",
        "https://github.com:8443",
        "https://127.0.0.1",
        "https://10.0.0.1",
        "https://localhost",
        "https://service.cluster.local",
        "https://service.internal",
        "https://service.test",
        "file:///tmp/private",
        "ftp://github.com",
        "https://github.com\n",
    ],
)
def test_non_public_or_non_root_urls_are_rejected_before_any_fetch(tmp_path: Path, url: str):
    sources = PublicSources()
    with pytest.raises(ValueError, match="public HTTP"):
        updater.refresh_profile(tmp_path / "profile", [url], fetch=sources, now=NOW)
    assert sources.calls == []
    assert list(tmp_path.iterdir()) == []


def test_default_observations_are_explicit_public_roots_only(tmp_path: Path):
    sources = PublicSources()
    updater.refresh_profile(tmp_path / "profile", fetch=sources, now=NOW)
    assert [url for url, _ in sources.calls[2:]] == [
        updater.GREATFIRE_API_ROOT + root.replace("://", "/", 1) for root in updater.DEFAULT_URLS
    ]
    assert all("/api/url/" in url for url, _ in sources.calls[2:])


def test_stale_unknown_not_blocked_and_errors_produce_an_explicit_empty_payload(tmp_path: Path):
    roots = [f"https://case{index}.example.com" for index in range(8)]
    data = {
        roots[0]: observation(roots[0], stale=True),
        roots[1]: observation(roots[1], last_tested=(NOW - timedelta(days=91)).isoformat()),
        roots[2]: observation(roots[2], headline="not blocked"),
        roots[3]: observation(roots[3], found=False),
        roots[4]: urllib.error.HTTPError("https://en.greatfire.org", 404, "Not Found", {}, None),
        roots[5]: TimeoutError("timed out"),
        roots[6]: observation(roots[6], last_tested="invalid"),
        roots[7]: observation(roots[7], last_tested=(NOW + timedelta(days=1)).isoformat()),
    }
    output = tmp_path / "profile"
    result = updater.refresh_profile(output, roots, fetch=PublicSources(data), now=NOW)
    assert result.greatfire_hosts == ()
    assert (output / "greatfire-rules.yaml").read_text() == "payload: []\n"
    assert yaml.safe_load((output / "greatfire-rules.yaml").read_text()) == {"payload": []}
    decisions = json.loads((output / "greatfire-decisions.json").read_text())
    assert all(row["action"] == "no_override" for row in decisions)
    assert decisions[4]["http_status"] == 404
    assert decisions[5]["error"] == "TimeoutError"
    assert all("accessible" not in row for row in decisions)


@pytest.mark.parametrize("content", [False, True])
def test_existing_output_directory_is_never_overwritten(tmp_path: Path, content: bool):
    output = tmp_path / "profile"
    output.mkdir()
    if content:
        (output / "sentinel").write_text("keep")
    sources = PublicSources()
    with pytest.raises(FileExistsError):
        updater.refresh_profile(output, fetch=sources, now=NOW)
    assert sources.calls == []
    assert sorted(path.name for path in output.iterdir()) == (["sentinel"] if content else [])


def test_invalid_provider_is_not_published_and_temporary_directory_is_removed(tmp_path: Path):
    with pytest.raises(ValueError, match="profile_unavailable"):
        updater.refresh_profile(
            tmp_path / "profile",
            ["https://github.com"],
            fetch=PublicSources(rules=b"payload: []\n"),
            now=NOW,
        )
    assert list(tmp_path.iterdir()) == []


def test_destination_created_during_fetch_is_not_overwritten(tmp_path: Path, monkeypatch):
    output = tmp_path / "profile"
    original = updater.load_mainland_network_profile

    def competing_writer(stage):
        profile = original(stage)
        output.mkdir()
        (output / "sentinel").write_text("keep")
        return profile

    monkeypatch.setattr(updater, "load_mainland_network_profile", competing_writer)
    with pytest.raises(FileExistsError):
        updater.refresh_profile(output, ["https://github.com"], fetch=PublicSources(), now=NOW)
    assert (output / "sentinel").read_text() == "keep"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["profile"]


def test_bad_release_asset_is_not_followed(tmp_path: Path):
    calls = []

    def fetch(url, timeout):
        calls.append(url)
        return json.dumps(
            {
                "tag_name": TAG,
                "assets": [
                    {
                        "name": "gfw.txt",
                        "browser_download_url": "https://example.com/not-the-release",
                    }
                ],
            }
        ).encode()

    with pytest.raises(ValueError, match=r"fixed gfw\.txt"):
        updater.refresh_profile(tmp_path / "profile", fetch=fetch, now=NOW)
    assert calls == [updater.LATEST_RELEASE_URL]
    assert list(tmp_path.iterdir()) == []


def test_explicit_http_root_retains_its_frozen_source_observation(tmp_path: Path):
    result = updater.refresh_profile(
        tmp_path / "profile", ["http://gitlab.com"], fetch=PublicSources(), now=NOW,
    )
    assert result.greatfire_hosts == ("gitlab.com",)
    assert result.sources["greatfire_decisions"][0]["url"] == "http://gitlab.com"
