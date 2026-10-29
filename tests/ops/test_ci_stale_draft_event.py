from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import scripts.ci_stale_draft_event as stale_draft

REPO = "ca-mrc/loom"
HEAD = "8731dd2c3da48740290f44687bf69f74b4fa9929"
NEWER_HEAD = "a" * 40
BASE = "afd93514bef9097864fbcc0eed81dc206a688f5f"
OWN_RUN = 40145353802
OWN_SUITE = 108812028929
WORKFLOW = 7


def _run(gate: str, suite: int, *, head: str = HEAD) -> dict[str, Any]:
    return {
        "check_suite_id": suite,
        "display_title": f"gate={gate} / head={head} / base={BASE} / updated=none / action=x",
    }


class FakeGitHub:
    def __init__(
        self,
        *,
        draft: bool = False,
        state: str = "open",
        head: str = HEAD,
        listings: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        self.pull = {"draft": draft, "state": state, "head": {"sha": head}}
        self.listings = listings if listings is not None else [[]]
        self.reads: list[str] = []
        self.sleeps: list[float] = []

    def read(self, endpoint: str) -> Any:
        self.reads.append(endpoint)
        if endpoint == f"repos/{REPO}/pulls/2635":
            return self.pull
        if endpoint == f"repos/{REPO}/actions/runs/{OWN_RUN}":
            return {"check_suite_id": OWN_SUITE, "workflow_id": WORKFLOW}
        assert endpoint == (
            f"repos/{REPO}/actions/workflows/{WORKFLOW}/runs"
            f"?event=pull_request&head_sha={HEAD}&per_page=100"
        )
        runs = self.listings[min(len(self.reads) - 3, len(self.listings) - 1)]
        return {"total_count": len(runs), "workflow_runs": runs}

    def classify(self) -> tuple[str, str]:
        return stale_draft.classify(
            repository=REPO, pull_number="2635", head_sha=HEAD, run_id=str(OWN_RUN),
            read=self.read, sleep=self.sleeps.append,
        )


def test_draft_event_newer_than_the_ready_full_suite_runs_the_full_gate() -> None:
    # PR #2635: synchronize (draft) and ready_for_review created in the same
    # second; the draft run got the newer suite and hid the green checks.
    github = FakeGitHub(listings=[[
        _run("filtered", OWN_SUITE), _run("full", 108812028823),
    ]])

    assert github.classify()[0] == "full"
    assert github.sleeps == []


def test_draft_event_older_than_the_ready_full_suite_stays_filtered() -> None:
    github = FakeGitHub(listings=[[_run("filtered", OWN_SUITE), _run("full", OWN_SUITE + 5)]])

    assert github.classify() == ("filtered", "a newer full suite exists on this head")


def test_a_ready_run_older_and_another_newer_than_the_draft_stays_filtered() -> None:
    github = FakeGitHub(listings=[[_run("full", OWN_SUITE - 9), _run("full", OWN_SUITE + 1)]])

    assert github.classify()[0] == "filtered"


def test_waits_for_the_ready_run_to_become_listable() -> None:
    github = FakeGitHub(listings=[[], [], [_run("full", OWN_SUITE + 1)]])

    assert github.classify()[0] == "filtered"
    assert github.sleeps == [stale_draft.FULL_SUITE_INTERVAL_SECONDS] * 2


def test_ready_head_without_any_full_suite_runs_the_full_gate() -> None:
    # PR #2677: push while Draft, Ready 2 s later; GitHub created only the
    # Draft synchronize run and never a ready_for_review run.
    github = FakeGitHub(listings=[[_run("filtered", OWN_SUITE)]])

    assert github.classify() == ("full", "live pull request is Ready on this head and has no full suite")
    assert len(github.sleeps) == stale_draft.FULL_SUITE_ATTEMPTS - 1


def test_genuine_draft_without_a_full_suite_stays_filtered() -> None:
    github = FakeGitHub(draft=True, listings=[[_run("filtered", OWN_SUITE)]])

    assert github.classify() == ("filtered", "live pull request is draft")
    assert github.sleeps == []


def test_full_suites_for_other_heads_and_manual_runs_do_not_count() -> None:
    github = FakeGitHub(listings=[[
        _run("full", OWN_SUITE - 1, head=NEWER_HEAD), _run("manual", OWN_SUITE - 2),
    ]])

    assert github.classify()[1] == "live pull request is Ready on this head and has no full suite"
    assert len(github.sleeps) == stale_draft.FULL_SUITE_ATTEMPTS - 1


@pytest.mark.parametrize(
    ("pull", "reason"),
    [
        ({"draft": True}, "live pull request is draft"),
        ({"state": "closed"}, "live pull request is closed"),
        ({"head": NEWER_HEAD}, "live pull request has a newer head"),
    ],
)
def test_genuine_draft_closed_or_superseded_heads_stay_filtered(pull: dict[str, Any], reason: str) -> None:
    github = FakeGitHub(**pull, listings=[[_run("full", OWN_SUITE - 1)]])

    assert github.classify() == ("filtered", reason)
    assert len(github.reads) == 1


@pytest.mark.parametrize(
    "listing",
    [
        {"total_count": 101, "workflow_runs": [_run("full", OWN_SUITE - 1)]},
        {"total_count": 1, "workflow_runs": [{"display_title": f"gate=full / head={HEAD} / "}]},
        {"workflow_runs": "not-a-list"},
    ],
)
def test_ambiguous_run_listings_are_errors(listing: dict[str, Any]) -> None:
    github = FakeGitHub()
    github.listings = []
    github.read = lambda endpoint, original=github.read: (  # type: ignore[method-assign]
        listing if "/workflows/" in endpoint else original(endpoint)
    )

    with pytest.raises(ValueError):
        github.classify()


@pytest.mark.parametrize(
    ("field", "value"),
    [("pull_number", "26 35"), ("head_sha", "HEAD"), ("run_id", "x"), ("repository", "../loom")],
)
def test_rejects_malformed_identity(field: str, value: str) -> None:
    arguments = {"repository": REPO, "pull_number": "2635", "head_sha": HEAD, "run_id": str(OWN_RUN)}
    arguments[field] = value

    with pytest.raises(ValueError):
        stale_draft.classify(**arguments, read=FakeGitHub().read)


def _main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, read: Any) -> str:
    output = tmp_path / "output"
    for name, value in {
        "GITHUB_REPOSITORY": REPO, "PR_NUMBER": "2635", "HEAD_SHA": HEAD,
        "GITHUB_RUN_ID": str(OWN_RUN), "GH_TOKEN": "token", "GITHUB_OUTPUT": str(output),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(stale_draft, "read_json", lambda _url, _token: read)
    assert stale_draft.main() == 0
    return output.read_text(encoding="utf-8")


def test_main_reports_a_stale_draft(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    github = FakeGitHub(listings=[[_run("full", OWN_SUITE - 1)]])

    assert _main(monkeypatch, tmp_path, github.read) == "stale_draft=true\n"


def test_main_keeps_the_event_filtered_when_github_is_unreadable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    def unreadable(endpoint: str) -> Any:
        raise ValueError(f"GitHub API read failed: {endpoint}")

    assert _main(monkeypatch, tmp_path, unreadable) == "stale_draft=false\n"
    assert "::warning::live state unavailable" in capsys.readouterr().out
