"""Unified all-benchmark opt-in end-to-end smoke (issue #806).

For supported adapters, import synthetic fixture profiles, validate their
actual source bytes through shared admission, then submit representative tasks
through the docker-compose rig. Excluded catalogs assert their reviewed policy
rejection. The incomplete TB2.1 sample asserts its locked-source rejection; it
cannot stand in for the full audited upstream profile or runtime qualification.

Opt-in only. Set LOOM_RUN_ALL_BENCHMARKS_SMOKE=1 to run. Wall-clock
budget on a warm cache is ~45-60 min; some benchmarks pull multi-GB
container images on first run.

Pattern mirrors ``tests/system/test_benchmark_humaneval_smoke.py``
and ``tests/system/test_benchmark_swe_bench_smoke.py``: the adapter's
``list_instances`` is monkey-patched to yield from the local fixture
(no upstream fetch), ``run_import`` drops the rows into the DB, then
trials are posted through the control plane and polled until they
land in a terminal state.
"""

from __future__ import annotations

import importlib
import json
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from loom.catalog_policy import catalog_support_policy
from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.import_cmd import run_import
from tests.system.benchmark_catalog_admission import admit_fixture_catalog, pin_fixture_adapter

_FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures"
)

_TB2_FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmark-terminal-bench-2/tests/fixtures/tb2-subset-10"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_ALL_BENCHMARKS_SMOKE") != "1",
    reason="set LOOM_RUN_ALL_BENCHMARKS_SMOKE=1 to run the "
    "all-benchmark opt-in smoke",
)


@dataclass(frozen=True)
class BenchmarkCase:
    """One benchmark's slot in the all-benchmark smoke.

    ``benchmark_id`` is the adapter slug passed to ``run_import``. Supported
    cases resolve to isolated synthetic profile IDs for task submission;
    excluded cases retain the reviewed policy identity and assert rejection.
    ``adapter_import_path`` + ``adapter_class_name`` locate the class
    whose ``list_instances`` gets monkey-patched.

    Four record-sourcing modes; set exactly one:

    * ``fixture_path`` (relative to ``_FIXTURE_ROOT``): load JSON rows,
      filter by ``str(row[id_field]) in instance_ids``. Used when the
      fixture row's raw id-field IS the adapter's ``instance_id``.
    * ``raw_records``: use these dicts directly, paired positionally
      with ``instance_ids``. Used when the adapter's ``list_instances``
      derives the instance_id from more than the raw id-field (e.g.
      AIME parses ``(year, exam, num)`` from a URL) — or when we
      simply want to synthesize test rows without a fixture file.
    * ``records_factory``: a callable ``(tmp_path) -> tuple[dict, ...]``
      that runs at test time and can populate on-disk state (dir tree,
      files) that the raw records reference. Used by adapters whose
      ``convert_instance`` copies files from a ``source_root`` path in
      the raw dict (e.g. tau2-bench).
    * ``override_source_dir``: do NOT patch ``list_instances`` at all;
      patch ``fetch_upstream`` to return this path so the adapter's
      real walk runs against local files. Used by adapters (e.g.
      terminal-bench-2) whose ``list_instances`` traverses a task-dir
      tree that already ships as an in-tree fixture.

    ``id_field`` is only consulted for ``fixture_path`` mode.
    """

    benchmark_id: str
    adapter_import_path: str
    adapter_class_name: str
    instance_ids: tuple[str, ...]
    fixture_path: str | None = None
    raw_records: tuple[dict[str, object], ...] | None = None
    records_factory: Callable[[Path], tuple[dict[str, object], ...]] | None = None
    override_source_dir: Path | None = None
    id_field: str = "task_id"
    # ``run_import`` calls list_instances once per split declared on the
    # adapter (see ``benchmarks.json`` -> ``splits``). The fake yields
    # the same records for every call, so multi-split benchmarks would
    # double-import. Restrict yielding to this one split.
    target_split: str = "test"
    timeout_sec: int = 600
    marks: tuple[pytest.MarkDecorator, ...] = field(default_factory=tuple)
    locked_materialization_rejection: str | None = None

    def __post_init__(self) -> None:
        sources = [
            self.fixture_path is not None,
            self.raw_records is not None,
            self.records_factory is not None,
            self.override_source_dir is not None,
        ]
        if sum(sources) != 1:
            raise ValueError(
                f"{self.benchmark_id}: set exactly one of fixture_path, "
                "raw_records, records_factory, override_source_dir",
            )
        if self.raw_records is not None and len(self.raw_records) != len(
            self.instance_ids,
        ):
            raise ValueError(
                f"{self.benchmark_id}: raw_records length "
                f"({len(self.raw_records)}) must match instance_ids length "
                f"({len(self.instance_ids)}) — pairs are positional",
            )


CASES: tuple[BenchmarkCase, ...] = (
    BenchmarkCase(
        benchmark_id="humaneval",
        adapter_import_path="loom_benchmarks.adapters.humaneval",
        adapter_class_name="HumanEvalAdapter",
        fixture_path="humaneval/sample.json",
        instance_ids=("HumanEval/0", "HumanEval/1"),
        id_field="task_id",
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="mbpp",
        adapter_import_path="loom_benchmarks.adapters.mbpp",
        adapter_class_name="MBPPAdapter",
        fixture_path="mbpp/sample.json",
        instance_ids=("11",),
        id_field="task_id",
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="bfcl",
        adapter_import_path="loom_benchmarks.adapters.bfcl",
        adapter_class_name="BFCLAdapter",
        fixture_path="bfcl/sample.json",
        instance_ids=("simple_0",),
        id_field="id",
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="livecodebench",
        adapter_import_path="loom_benchmarks.adapters.livecodebench",
        adapter_class_name="LiveCodeBenchAdapter",
        fixture_path="livecodebench/sample.json",
        instance_ids=("lcb-9001",),
        id_field="question_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="gaia",
        adapter_import_path="loom_benchmarks.adapters.gaia",
        adapter_class_name="GAIAAdapter",
        fixture_path="gaia/sample.json",
        instance_ids=("c61d22de-5f6c-4958-a7f6-5e9707bd3466",),
        id_field="task_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="osworld",
        adapter_import_path="loom_benchmarks.adapters.osworld",
        adapter_class_name="OSWorldAdapter",
        fixture_path="osworld/sample.json",
        instance_ids=("8849e9da-d935-46bb-9bef-d3204c1f1e3c",),
        id_field="id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="webarena",
        adapter_import_path="loom_benchmarks.adapters.webarena",
        adapter_class_name="WebArenaAdapter",
        fixture_path="webarena/sample.json",
        instance_ids=("42",),
        id_field="task_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="swe-bench",
        adapter_import_path="loom_benchmarks.adapters.swe_bench",
        adapter_class_name="SWEBenchAdapter",
        fixture_path="swe_bench/sample.json",
        instance_ids=("astropy__astropy-12907",),
        id_field="instance_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="swe-bench-verified",
        adapter_import_path="loom_benchmarks.adapters.swe_bench_verified",
        adapter_class_name="SWEBenchVerifiedAdapter",
        fixture_path="swe_bench_verified/sample.json",
        instance_ids=("django__django-12345",),
        id_field="instance_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="swe-bench-multimodal",
        adapter_import_path="loom_benchmarks.adapters.swe_bench_multimodal",
        adapter_class_name="SWEBenchMultimodalAdapter",
        fixture_path="swe_bench_multimodal/sample.json",
        instance_ids=("vega__vega-lite-9001",),
        id_field="instance_id",
        timeout_sec=1800,
    ),
    # AIME: the adapter derives instance_id from a URL parse (aime-22..24)
    # or a `(problem_idx, exam)` composite (aime-25), so the raw fixture
    # id-field doesn't map. Synthesize one row per year in
    # ``raw_records`` and let the fake list_instances yield with the
    # already-computed instance_id.
    BenchmarkCase(
        benchmark_id="aime-22",
        adapter_import_path="loom_benchmarks.adapters.aime",
        adapter_class_name="AIME22Adapter",
        instance_ids=("2022-I/1",),
        raw_records=(
            {
                "problem": (
                    "Find the number of ordered pairs of positive "
                    "integers (a, b) such that 1/a + 1/b = 1/2022."
                ),
                "answer": "45",
                "url": (
                    "https://artofproblemsolving.com/wiki/index.php/"
                    "2022_AIME_I_Problems/Problem_1"
                ),
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="aime-23",
        adapter_import_path="loom_benchmarks.adapters.aime",
        adapter_class_name="AIME23Adapter",
        instance_ids=("2023-I/1",),
        raw_records=(
            {
                "problem": (
                    "Five men and nine women stand equally spaced around "
                    "a circle in random order. Compute the probability "
                    "that every man stands diametrically opposite a "
                    "woman."
                ),
                "answer": "191",
                "url": (
                    "https://artofproblemsolving.com/wiki/index.php/"
                    "2023_AIME_I_Problems/Problem_1"
                ),
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="aime-24",
        adapter_import_path="loom_benchmarks.adapters.aime",
        adapter_class_name="AIME24Adapter",
        instance_ids=("2024-I/1",),
        raw_records=(
            {
                "problem": (
                    "Every morning Aya goes for a 9-kilometer walk. "
                    "Compute the total time she spends walking."
                ),
                "answer": "204",
                "url": (
                    "https://artofproblemsolving.com/wiki/index.php/"
                    "2024_AIME_I_Problems/Problem_1"
                ),
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="aime-25",
        adapter_import_path="loom_benchmarks.adapters.aime_2025",
        adapter_class_name="AIME25Adapter",
        instance_ids=("2025-I/1",),
        raw_records=(
            {
                "problem": (
                    "Find the sum of all integer bases b > 9 for which "
                    "17_b is a divisor of 97_b."
                ),
                "answer": "70",
                "exam": "I",
                "problem_idx": "1",
            },
        ),
        timeout_sec=600,
    ),
    # Bucket B: dynamic-loader benchmarks whose real list_instances
    # fetches from HF / git and derives instance_id from an enumeration
    # (``f"{split}/{idx:04d}"``) or a rekeying. Each case synthesizes
    # one row inline with the fields ``convert_instance`` reads.
    BenchmarkCase(
        benchmark_id="browsecomp",
        adapter_import_path="loom_benchmarks.adapters.browsecomp",
        adapter_class_name="BrowseCompAdapter",
        instance_ids=("test/0000",),
        raw_records=(
            {
                "problem": "What is the capital of France?",
                "answer": "Paris",
                "topic": "geography",
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="gpqa",
        adapter_import_path="loom_benchmarks.adapters.gpqa",
        adapter_class_name="GPQAAdapter",
        instance_ids=("smoke-record-1",),
        raw_records=(
            {
                "Record ID": "smoke-record-1",
                "Question": (
                    "What is the ground-state electron configuration of "
                    "atomic carbon?"
                ),
                "Correct Answer": "1s2 2s2 2p2",
                "Incorrect Answer 1": "1s2 2s2 2p3",
                "Incorrect Answer 2": "1s2 2s2 2p4",
                "Incorrect Answer 3": "1s2 2s1 2p3",
                "High-level domain": "Chemistry",
                "Subdomain": "Physical Chemistry",
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="gpqa-diamond",
        adapter_import_path="loom_benchmarks.adapters.gpqa",
        adapter_class_name="GPQADiamondAdapter",
        instance_ids=("smoke-record-diamond-1",),
        raw_records=(
            {
                "Record ID": "smoke-record-diamond-1",
                "Question": (
                    "Which particle mediates the strong nuclear force?"
                ),
                "Correct Answer": "gluon",
                "Incorrect Answer 1": "photon",
                "Incorrect Answer 2": "W boson",
                "Incorrect Answer 3": "graviton",
                "High-level domain": "Physics",
                "Subdomain": "Particle Physics",
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="hendrycks-math",
        adapter_import_path="loom_benchmarks.adapters.hendrycks_math",
        adapter_class_name="HendrycksMATHAdapter",
        instance_ids=("test/00000",),
        raw_records=(
            {
                "problem": "Compute $2 + 2$.",
                # HendrycksMATHAdapter has answer_field=None and pulls
                # the answer via _extract_boxed_answer(solution). Any
                # ``\boxed{...}`` in the solution string qualifies.
                "solution": "Adding gives $\\boxed{4}$.",
                "level": "Level 1",
                "type": "Prealgebra",
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="math-500",
        adapter_import_path="loom_benchmarks.adapters.hendrycks_math",
        adapter_class_name="MATH500Adapter",
        instance_ids=("test/00000",),
        raw_records=(
            {
                "problem": "What is the value of $3 \\times 5$?",
                "answer": "15",
                "level": "Level 1",
                "subject": "Prealgebra",
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="mmlu-pro",
        adapter_import_path="loom_benchmarks.adapters.mmlu_pro",
        adapter_class_name="MMLUProAdapter",
        instance_ids=("test/9001",),
        raw_records=(
            {
                "question_id": 9001,
                "question": (
                    "Which of the following is the SI unit of electric "
                    "current?"
                ),
                "options": ["ampere", "volt", "ohm", "coulomb"],
                "answer_index": 0,
                "answer": "A",
                "category": "physics",
            },
        ),
        timeout_sec=600,
    ),
    # Bucket C: skill family. skillflow / skillflow-iterative share the
    # SkillFlow manifest-based fixture (raw carries ``files`` dict);
    # SkillLearnBench variants inherit that path, and _inject_skills
    # gracefully skips when ``__upstream_root`` is missing, so the same
    # fixture flows through the model-specific variant adapters too.
    BenchmarkCase(
        benchmark_id="skillflow",
        adapter_import_path="loom_benchmarks.adapters.skillflow",
        adapter_class_name="SkillFlowAdapter",
        fixture_path="skillflow/sample.json",
        instance_ids=("skillflow/python-list-comprehension/01",),
        id_field="instance_id",
        timeout_sec=900,
    ),
    BenchmarkCase(
        benchmark_id="skillflow-iterative",
        adapter_import_path="loom_benchmarks.adapters.skillflow",
        adapter_class_name="SkillFlowIterativeAdapter",
        fixture_path="skillflow/sample.json",
        instance_ids=("skillflow/python-list-comprehension/01",),
        id_field="instance_id",
        timeout_sec=900,
    ),
    BenchmarkCase(
        benchmark_id="skilllearnbench",
        adapter_import_path="loom_benchmarks.adapters.skilllearnbench",
        adapter_class_name="SkillLearnBenchAdapter",
        fixture_path="skilllearnbench/sample.json",
        instance_ids=("skilllearnbench/python-iterators/01",),
        id_field="instance_id",
        timeout_sec=900,
    ),
    BenchmarkCase(
        benchmark_id="skilllearnbench-human-authored",
        adapter_import_path="loom_benchmarks.adapters.skilllearnbench",
        adapter_class_name="SkillLearnBenchHumanAuthoredAdapter",
        fixture_path="skilllearnbench/sample.json",
        instance_ids=("skilllearnbench/python-iterators/01",),
        id_field="instance_id",
        timeout_sec=900,
    ),
    BenchmarkCase(
        benchmark_id="skilllearnbench-b1-one-shot-claude-opus-4-6",
        adapter_import_path="loom_benchmarks.adapters.skilllearnbench",
        adapter_class_name="SkillLearnBenchB1OneShotClaudeOpus46Adapter",
        fixture_path="skilllearnbench/sample.json",
        instance_ids=("skilllearnbench/python-iterators/01",),
        id_field="instance_id",
        timeout_sec=900,
    ),
    # tau2-bench's convert_instance copies files from raw["source_root"]
    # into the task bundle. Use a records_factory so we can materialize
    # a fake source_root/domains/airline/ tree inside tmp_path before
    # yielding the record.
    BenchmarkCase(
        benchmark_id="tau2-bench",
        adapter_import_path="loom_benchmarks.adapters.tau2_bench",
        adapter_class_name="Tau2BenchAdapter",
        instance_ids=("airline/000",),
        records_factory=lambda tmp: _tau2_records_factory(tmp),
        timeout_sec=1200,
    ),
    # Bucket D: terminal-bench-2 lives in its own package. Real
    # list_instances walks a task-directory tree; the package already
    # ships ``tb2-subset-10`` fixture with 10 pinned upstream tasks, so
    # we point fetch_upstream at it and let the real walk run against
    # local files. 3 representative task shapes: single-service compose
    # (hello-world), test-heavy (chess-best-move), multi-step
    # interactive (blind-maze-explorer-5x5).
    BenchmarkCase(
        benchmark_id="terminal-bench-2",
        adapter_import_path="loom_benchmark_terminal_bench_2.adapter",
        adapter_class_name="TerminalBench2Adapter",
        override_source_dir=_TB2_FIXTURE_ROOT,
        instance_ids=(
            "blind-maze-explorer-5x5",
            "chess-best-move",
            "hello-world",
        ),
        locked_materialization_rejection="Harbor materialization metadata is unavailable",
        timeout_sec=3600,
    ),
)


def _tau2_records_factory(
    tmp_path: Path,
) -> tuple[dict[str, object], ...]:
    """Materialize a stub tau2-bench source tree and return one record
    that references it. ``_copy_domain_assets`` iterates the domain
    directory; an empty directory is a valid (no-op) tree."""
    source_root = tmp_path / "tau2-source"
    (source_root / "domains" / "airline").mkdir(parents=True, exist_ok=True)
    task = {
        "id": "smoke-airline-1",
        "description": {
            "purpose": (
                "Smoke: verify airline domain agent can lookup a "
                "reservation"
            ),
        },
        "user_scenario": {
            "persona": "Traveler who booked a one-way ticket.",
            "instructions": "Ask the agent to look up your reservation.",
        },
        "ticket": "Reservation lookup",
        "evaluation_criteria": {"expected_actions": []},
    }
    return (
        {
            "domain": "airline",
            "source_root": str(source_root),
            "upstream_task_id": "smoke-airline-1",
            "task": task,
        },
    )


def _load_adapter_class(case: BenchmarkCase) -> type:
    module = importlib.import_module(case.adapter_import_path)
    return getattr(module, case.adapter_class_name)


def _resolve_rows(
    case: BenchmarkCase, tmp_path: Path,
) -> list[tuple[str, dict[str, object]]]:
    """Return ``[(instance_id, raw), ...]`` in the order the smoke
    should submit trials."""
    if case.raw_records is not None:
        return list(zip(case.instance_ids, case.raw_records, strict=True))

    if case.records_factory is not None:
        records = case.records_factory(tmp_path)
        return list(zip(case.instance_ids, records, strict=True))

    assert case.fixture_path is not None
    path = _FIXTURE_ROOT / case.fixture_path
    rows = json.loads(path.read_text())
    wanted = set(case.instance_ids)
    # Some fixtures store the id as int (mbpp task_id, webarena task_id);
    # instance_ids in the case is str, so normalize both sides.
    by_id: dict[str, dict[str, object]] = {}
    for r in rows:
        key = str(r.get(case.id_field))
        if key in wanted:
            by_id[key] = r
    missing = wanted - set(by_id)
    assert not missing, (
        f"{case.benchmark_id}: fixture {path} missing instance ids: "
        f"{sorted(missing)}"
    )
    return [(iid, by_id[iid]) for iid in case.instance_ids]


@pytest.mark.parametrize(
    "case",
    [pytest.param(c, id=c.benchmark_id, marks=c.marks) for c in CASES],
)
async def test_benchmark_smoke(
    case: BenchmarkCase,
    compose_stack: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmarks.base import BenchmarkInstance

    adapter_cls = _load_adapter_class(case)
    if case.locked_materialization_rejection is not None:
        from loom_benchmark_terminal_bench_2.upstream import TB21LockError

        # This old sample tree lacks the independently locked package/audit
        # materialization. It must not stand in for full TB2.1 qualification.
        assert case.override_source_dir is not None
        with pytest.raises(TB21LockError, match=case.locked_materialization_rejection):
            list(adapter_cls().list_instances(source_dir=case.override_source_dir, split=case.target_split))
        return

    if case.override_source_dir is not None:
        # Real list_instances walks the pre-materialized fixture tree.
        # ``instance_ids`` is passed to run_import for explicit filtering.
        source_dir = case.override_source_dir
    else:
        rows = _resolve_rows(case, tmp_path)

        def _fake_list(
            self: object, *, source_dir: Path, split: str,
        ) -> Iterator[BenchmarkInstance]:
            # Adapter.splits may be multi-valued (e.g. gaia = validation
            # + test); yield only on target_split to avoid double-import.
            if split != case.target_split:
                return
            for instance_id, raw in rows:
                yield BenchmarkInstance(
                    instance_id=instance_id,
                    split=split,
                    raw=raw,
                )

        monkeypatch.setattr(adapter_cls, "list_instances", _fake_list)
        source_dir = tmp_path / "stub-source"

    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: source_dir,
    )

    policy = catalog_support_policy(case.benchmark_id)
    profile_id = pin_fixture_adapter(
        monkeypatch, case.benchmark_id, retain_policy_identity=policy.state != "supported",
    )
    minio_store = MinioObjectStore(
        endpoint_url=compose_stack["minio"],
        access_key=compose_stack["minio_access_key"],
        secret_key=compose_stack["minio_secret_key"],
    )
    # compose_stack is session-scoped, so the humaneval/swe-bench smokes
    # in the same test module create the same bucket first. Boto3 raises
    # BucketAlreadyOwnedByYou / BucketAlreadyExists on repeat; treat both
    # as no-op since ownership is uniform for a single-tenant test MinIO.
    try:
        minio_store._client.create_bucket(Bucket="loom-benchmarks")
    except minio_store._client.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {
            "BucketAlreadyOwnedByYou", "BucketAlreadyExists",
        }:
            raise

    stats = await run_import(
        benchmark=case.benchmark_id,
        db_url=compose_stack["db_url"],
        object_store=minio_store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        # Filter by instance_ids (not limit) so cases that use the
        # real list_instances (override_source_dir mode) don't pull
        # extraneous rows from the fixture tree.
        instance_ids=list(case.instance_ids),
        imported_by="all-benchmarks-smoke",
    )
    assert stats["converted"] == len(case.instance_ids), (
        f"{case.benchmark_id}: expected {len(case.instance_ids)} "
        f"converted, got {stats}"
    )

    await admit_fixture_catalog(db_url=compose_stack["db_url"], profile_id=profile_id,
                                object_store=minio_store)
    cp = compose_stack["control_plane"]
    token = compose_stack["team_token"]

    submitted: list[str] = []
    for instance_id in case.instance_ids:
        r = httpx.post(
            f"{cp}/trials",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "task_id": f"{profile_id}/{instance_id}",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
            timeout=10,
        )
        if policy.state != "supported":
            assert r.status_code == 409, r.text
            assert r.json()["detail"]["reason"] == policy.state
            continue
        assert r.status_code == 201, (
            f"{case.benchmark_id}/{instance_id}: {r.status_code} {r.text}"
        )
        submitted.append(r.json()["trial_id"])

    if policy.state != "supported":
        assert submitted == []
        return

    deadline = time.time() + case.timeout_sec
    pending = set(submitted)
    final: dict[str, str] = {}
    poll_interval = 5.0 if case.timeout_sec >= 900 else 2.0
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
            time.sleep(poll_interval)

    assert not pending, (
        f"{case.benchmark_id}: timed out waiting for "
        f"{pending}; final={final}"
    )
    assert all(s == "succeeded" for s in final.values()), (
        f"{case.benchmark_id}: non-success terminal states: {final}"
    )
