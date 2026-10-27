"""HumanEval end-to-end smoke (Plan 16 Task 3 / spec §14.4).

Brings up the full docker-compose stack, imports 2 HumanEval tasks via
`run_import` (against explicitly synthetic fixture provenance, no upstream
HF call), admits the actual source bytes through the shared owner, submits a
trial per task, waits for the worker to finish them, asserts the
trajectory state lands at `succeeded`.

The compose stack runs `--build` so this test exercises the same code
the production image would. Skipped automatically when
LOOM_SKIP_SYSTEM_TESTS=1.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.import_cmd import run_import
from tests.system.benchmark_catalog_admission import admit_fixture_catalog, pin_fixture_adapter

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures/humaneval/sample.json"
)


async def test_humaneval_end_to_end(
    compose_stack: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Import 2 HumanEval tasks → submit a trial per task → wait
    for succeeded."""
    from loom_benchmarks.adapters import humaneval as hv
    from loom_benchmarks.base import BenchmarkInstance

    fixture_records = json.loads(_FIXTURE.read_text())
    assert len(fixture_records) >= 2

    def _fake_list(
        self: hv.HumanEvalAdapter, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in fixture_records:
            yield BenchmarkInstance(
                instance_id=r["task_id"], split=split, raw=r,
            )

    monkeypatch.setattr(hv.HumanEvalAdapter, "list_instances", _fake_list)
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    profile_id = pin_fixture_adapter(monkeypatch, "humaneval")
    minio_store = MinioObjectStore(
        endpoint_url=compose_stack["minio"],
        access_key=compose_stack["minio_access_key"],
        secret_key=compose_stack["minio_secret_key"],
    )
    # Stack bootstrap doesn't create the bundle bucket by default.
    # Guard against BucketAlreadyOwnedByYou when other smokes in the
    # same session (e.g. all-benchmark smoke) created it first.
    try:
        minio_store._client.create_bucket(Bucket="loom-benchmarks")
    except minio_store._client.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {
            "BucketAlreadyOwnedByYou", "BucketAlreadyExists",
        }:
            raise

    stats = await run_import(
        benchmark="humaneval",
        db_url=compose_stack["db_url"],
        object_store=minio_store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        limit=None,
        imported_by="smoke",
    )
    assert stats["converted"] == len(fixture_records)
    await admit_fixture_catalog(db_url=compose_stack["db_url"], profile_id=profile_id,
                                object_store=minio_store)

    cp = compose_stack["control_plane"]
    token = compose_stack["team_token"]

    submitted: list[str] = []
    for i in range(len(fixture_records)):
        r = httpx.post(
            f"{cp}/trials",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "task_id": f"{profile_id}/HumanEval/{i}",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            timeout=10,
        )
        assert r.status_code == 201, r.text
        submitted.append(r.json()["trial_id"])

    deadline = time.time() + 600  # 10 min TOTAL across the batch
    pending = set(submitted)
    final: dict[str, str] = {}
    while pending and time.time() < deadline:
        for trial_id in list(pending):
            r = httpx.get(
                f"{cp}/trials/{trial_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            assert r.status_code == 200, r.text
            state = r.json()["state"]
            if state in ("succeeded", "failed", "cancelled"):
                final[trial_id] = state
                pending.discard(trial_id)
        if pending:
            time.sleep(2.0)

    assert not pending, f"timed out waiting for: {pending}; final={final}"
    assert all(s == "succeeded" for s in final.values()), final
