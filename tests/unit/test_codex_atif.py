"""Codex session log → ATIF: Harbor parity, clean-up and canonical publication."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.codex_atif import clean_trajectory, harbor_trajectory
from loom.hosted_harness import CODEX
from loom.models.exec import ExecResult
from loom.models.types import ModelSpec
from loom.service_execution_codex import CODEX_SESSION, codex_extra_config, collect_session
from loom_control_plane.service_execution_materializer import (
    build_canonical_atif,
    build_canonical_events,
)
from tests.unit.test_native_codex import _result, _trace, _trial
from tests.unit.test_service_execution_terminus_plan import _inputs

_FIXTURES = Path(__file__).parent / "fixtures" / "codex_atif"
_TB2 = "tb2-file-archive-manifest"


def _session(name: str) -> bytes:
    return (_FIXTURES / f"{name}.session.jsonl").read_bytes()


# --- Harbor parity -------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(p.name.removesuffix(".harbor.json") for p in _FIXTURES.glob("*.harbor.json")))
def test_conversion_matches_harbor(name: str) -> None:
    # Goldens are Harbor's own `Codex._convert_events_to_trajectory` output at the
    # harness controller's pinned commit (527d50de): Harbor's unit-test sessions
    # and a real Codex 0.146.0 run on terminal-bench-2-harbor-90/file-archive-manifest
    # (encrypted reasoning and base instructions removed). Harbor's LiteLLM
    # cost estimate is omitted; Loom's cost comes from the Gateway ledger.
    expected = json.loads((_FIXTURES / f"{name}.harbor.json").read_text())
    assert harbor_trajectory(_session(name), model_name=expected["model_name"]) == expected["trajectory"]


def test_empty_or_unparseable_logs_have_no_trajectory() -> None:
    assert harbor_trajectory(b"") is None
    assert harbor_trajectory(b"not json\n[1]\n") is None


# --- clean-up --------------------------------------------------------------------


def test_clean_trajectory_keeps_only_the_agents_work() -> None:
    converted = harbor_trajectory(_session(_TB2), model_name="gpt-5.4-mini")
    assert converted is not None
    cleaned = clean_trajectory(converted)

    steps = cleaned["steps"]
    # Codex's permissions and environment context are gone; the task stays.
    assert [s["source"] for s in steps] == ["user", "agent", "agent", "agent", "agent"]
    assert steps[0]["message"].startswith("Create a deterministic archive manifest")
    assert [s["step_id"] for s in steps] == [1, 2, 3, 4, 5]
    assert all("extra" not in s for s in steps) and "extra" not in cleaned["agent"]
    assert "final_metrics" not in cleaned

    agent = steps[1:]
    assert all(s["message"] and s["reasoning_content"] and s["metrics"] for s in agent)
    patch = next(c for s in agent for c in s.get("tool_calls", []) if c["function_name"] == "apply_patch")
    assert patch["arguments"]["input"].startswith("*** Begin Patch\n*** Add File: /app/build_manifest.py")
    results = [r for s in agent for r in s.get("observation", {}).get("results", [])]
    assert len(results) == 4 and all(r["exit_code"] == 0 for r in results)
    assert not any("Chunk ID" in r["content"] or "Wall time" in r["content"] for r in results)
    assert results[0]["content"].startswith("\n.private\n")


@pytest.mark.parametrize(("content", "cleaned"), [
    ("Chunk ID: ab\nWall time: 0.4 seconds\nProcess exited with code 2\nOriginal token count: 9\nOutput:\nboom\n",
     {"content": "boom\n", "exit_code": 2}),
    ("Exit code: 0\nWall time: 0 seconds\nOutput:\nSuccess.\n", {"content": "Success.\n", "exit_code": 0}),
    ("Wall time: 1 seconds\nOutput:\nstill running", {"content": "still running"}),
    ("plain output", {"content": "plain output"}),
])
def test_tool_output_headers_are_stripped_and_exit_codes_kept(content: str, cleaned: dict) -> None:
    document = {"agent": {}, "steps": [{"step_id": 1, "source": "agent", "message": "",
                                        "observation": {"results": [{"source_call_id": "c", "content": content}]}}]}
    assert clean_trajectory(document)["steps"][0]["observation"]["results"] == [{"source_call_id": "c", **cleaned}]


# --- controller --------------------------------------------------------------------


@pytest.mark.parametrize(("model", "summaries"), [
    ("gpt-5.4-mini", True), ("gpt-5", True), ("o3", True), ("o4-mini", True), ("openai/o3", True),
    ("codex-mini-latest", True), ("gpt-4.1-mini", False), ("gpt-4o", False), ("glm-5.2", False),
])
def test_reasoning_summaries_only_for_reasoning_models(model: str, summaries: bool) -> None:
    # A non-reasoning model rejects `reasoning.summary` and fails the call.
    config = codex_extra_config(ModelSpec(provider="openai", name=model))
    assert 'web_search="disabled"' in config
    assert ('model_reasoning_summary="detailed"' in config) is summaries


class _Sandbox:
    def __init__(self, listing: bytes, size: bytes = b"42\n", download_error: Exception | None = None) -> None:
        self.listing, self.size, self.download_error = listing, size, download_error
        self.downloads: list[PurePosixPath] = []

    async def exec(self, command: str, **_: object) -> ExecResult:
        stdout = self.listing if command.startswith("find ") else self.size
        return ExecResult(return_code=0, stdout=stdout, stderr=b"", duration_sec=0.0)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        if self.download_error is not None:
            raise self.download_error
        self.downloads.append(src)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(b"{}\n")


async def test_session_log_is_collected_when_unambiguous(tmp_path: Path) -> None:
    path = b"/tmp/loom-harness/codex-home/sessions/2026/10/08/rollout-1.jsonl"
    sandbox = _Sandbox(path + b"\n")
    assert await collect_session(sandbox, tmp_path)
    assert sandbox.downloads == [PurePosixPath(path.decode())] and (tmp_path / CODEX_SESSION).is_file()


@pytest.mark.parametrize("sandbox", [
    _Sandbox(b""),
    _Sandbox(b"/s/rollout-1.jsonl\n/s/rollout-2.jsonl\n"),
    _Sandbox(b"/s/rollout-1.jsonl\n", size=str(64 * 1024 * 1024 + 1).encode()),
    _Sandbox(b"/s/rollout-1.jsonl\n", download_error=OSError("gone")),
])
async def test_session_log_collection_is_best_effort(sandbox: _Sandbox, tmp_path: Path) -> None:
    assert not await collect_session(sandbox, tmp_path)
    assert not (tmp_path / CODEX_SESSION).exists()


def test_session_log_is_an_optional_native_output() -> None:
    session = [o for o in CODEX.native_outputs if o.source_path == "agent/codex/session.jsonl"]
    assert len(session) == 1 and session[0].relative_path == "artifacts/codex/session.jsonl"
    assert not session[0].required


# --- canonical ATIF ------------------------------------------------------------------


def _canonical(trial_id):
    _, body, rows = _trace(trial_id)
    task, _, _ = _inputs()
    return build_canonical_events(
        trial_id=trial_id, task_id="task-1", task_config=task, trial_config=_trial(), runtime_result=_result(),
        trace_body=body, verifier_body=b'{"rewards":{"passed":1}}', gateway_calls=rows,
    )


def test_canonical_atif_uses_codex_steps_with_loom_accounting_and_reward() -> None:
    trial_id = uuid4()
    events = _canonical(trial_id)

    document = json.loads(build_canonical_atif(
        events, task_id="task-1", agent_name="codex", agent_version="0.146.0", codex_session=_session(_TB2),
    ))

    assert document["schema_version"] == "ATIF-v1.7" and document["session_id"] == str(trial_id)
    assert [s["source"] for s in document["steps"]] == ["user", "agent", "agent", "agent", "agent"]
    assert document["metadata"]["final_state"] == "succeeded"
    assert document["metadata"]["verifier_rewards"] == {"passed": 1.0}
    # Authoritative usage is the Gateway ledger's, not Codex's own report.
    assert document["accounting"]["call_count"] == 2
    assert document["accounting"]["totals"]["input_tokens"] == 17


def test_canonical_atif_without_a_session_log_keeps_the_generic_projection() -> None:
    events = _canonical(uuid4())
    for session in (None, b""):
        document = json.loads(build_canonical_atif(
            events, task_id="task-1", agent_name="codex", agent_version="0.146.0", codex_session=session,
        ))
        assert document["schema_version"] == "1.7" and "accounting" not in document
