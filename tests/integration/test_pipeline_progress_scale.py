"""Disposable batch-scale query bounds; optional same-device baseline receipt.

Set LOOM_PIPELINE_BASELINE_SOURCE to a prior trial_progress.py and
LOOM_PIPELINE_PERF_REPORT to an ignored JSON path to record local before/after.
Latency values are observations, never flaky CI admission thresholds.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import statistics
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import httpx
from sqlalchemy import delete, event, insert, inspect, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    Batch,
    ServiceExecutionCommand,
    ServiceExecutionLease,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    Token,
    Trial,
    TrialTaskImageMaterialization,
    User,
)
from loom.pipeline.keys import canonical_digest
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.monitor_filters import apply_trial_monitor_filters
from loom_service.trial_progress import load_trial_progress, progress_summary
from tests.integration import test_service_execution_leases as execution
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)


async def test_batch_scale_queries_remain_bounded(postgres_url, monkeypatch):
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    batch_id, user_id = uuid4(), uuid4()
    token = "pipeline-scale-"+uuid4().hex
    trial_ids = [uuid4() for _ in range(1720)]
    image_ids = [uuid4() for _ in range(100)]
    async with sessions() as session:
        seed_id, target = await execution._seed_ready_trial(session, now=now)
        lease = await execution._reserve(session, trial_id=seed_id, target=target, now=now)
        seed = await session.get(Trial, seed_id)
        team_id = seed.team_id
        command = await session.scalar(select(ServiceExecutionCommand).where(
            ServiceExecutionCommand.lease_id == lease.id, ServiceExecutionCommand.command_type == "create"))
        command_template = {column.key: getattr(command, column.key) for column in inspect(command).mapper.column_attrs}
        template = {column.key: getattr(lease, column.key) for column in inspect(lease).mapper.column_attrs}
        session.add(Batch(id=batch_id, team_id=team_id, name="Pipeline scale 1720", backend="nebius",
            task_filter={}, trial_config={}, expected_trial_count=1720, created_by_token_prefix="test", state="running"))
        session.add(User(id=user_id, username="pipeline-scale-"+user_id.hex,
            username_normalized="pipeline-scale-"+user_id.hex, status="active", is_platform_admin=False))
        await session.flush()
        session.add(Token(token_hash=hashlib.sha256(token.encode()).digest(), type="team", scopes=["read:own"],
            team_id=team_id, created_by_user_id=user_id, issued_at=now))
        await session.execute(insert(Trial), [{"id": trial_id, "team_id": team_id, "task_id": seed.task_id,
            "batch_id": batch_id, "state": "running" if i<40 else "queued", "attempt_count": 1 if i<40 else 0,
            "config": seed.config, "requires_caps": {"backend": "nebius"}, "submitted_at": now}
            for i,trial_id in enumerate(trial_ids)])
        await session.execute(insert(TaskImageMaterialization), [{"id": image_id, "task_id": seed.task_id,
            "materialization_key": uuid4().hex*2, "task_checksum": f"{i+1:064x}", "cpu_arch": "x86_64",
            "task_config": {}, "state": "running" if i<40 else "ready", "lease_epoch": 1,
            "attempt_count": 1, "created_at": now, "ready_at": None if i<40 else now}
            for i,image_id in enumerate(image_ids)])
        await session.execute(insert(TrialTaskImageMaterialization), [{"trial_id": trial_id,
            "materialization_id": image_ids[i+40 if i<40 else i%100]} for i,trial_id in enumerate(trial_ids)])
        await session.execute(insert(TaskImageMaterializationAttempt), [{"id": uuid4(), "materialization_id": image_id,
            "attempt_number": 1, "lease_epoch": 1, "builder_id": "pipeline-scale", "claimed_at": now,
            "native_build": {"observed_at": now.isoformat(), "target_id": target.target_id,
                "phases": [{"name": "build", "state": {"running": {"startedAt": now.isoformat()}}}],
                "configmap": {"data": {"claim.json": "unreturned-private-context-"+"x"*65536}}}}
            for image_id in image_ids[:40]])
        for trial_id in trial_ids[:40]:
            values = {**template, "id": uuid4(), "request_id": uuid4(), "trial_id": trial_id,
                "provider_scope_key": "pipeline-scale-"+uuid4().hex, "job_name": "pipeline-scale-"+uuid4().hex,
                "execution_unit_key": uuid4(), "observed_state": "running", "pod_started_at": now,
                "last_reconciled_at": now}
            await session.execute(insert(ServiceExecutionLease).values(**values))
            payload = {**command.payload_json, "lease_id": str(values["id"]), "trial_id": str(trial_id)}
            await session.execute(insert(ServiceExecutionCommand).values(**{
                **command_template, "id": uuid4(), "lease_id": values["id"],
                "idempotency_key": "pipeline-scale-"+uuid4().hex, "payload_json": payload,
                "payload_sha256": canonical_digest(payload),
            }))
        await session.commit()
        # Fresh bulk fixtures have no planner statistics until autovacuum catches
        # up. Analyze the same unchanged data before both compared versions.
        for table in ("trials", "trial_task_image_materializations", "task_image_materializations",
                      "task_image_materialization_attempts", "execution_leases"):
            await session.execute(text("ANALYZE " + table))
        await session.commit()
    statements = []
    details = []
    query_started = []
    query_ms = []
    def record(_connection, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)
        details.append((statement, _parameters))
        query_started.append(perf_counter())
    def completed(_connection, _cursor, _statement, _parameters, _context, _executemany):
        query_ms.append((perf_counter()-query_started[-1])*1000)
    event.listen(engine.sync_engine, "after_cursor_execute", completed)
    event.listen(engine.sync_engine, "before_cursor_execute", record)
    scope = select(Trial.id).where(Trial.batch_id == batch_id)
    try:
        async with sessions() as session:
            statements.clear()
            result = await progress_summary(session, scope)
            assert len(statements) == 2
            explain_queries = list(details[-2:])
            plans = []
            connection = await session.connection()
            for statement, parameters in explain_queries:
                plans.append((await connection.exec_driver_sql("EXPLAIN (ANALYZE, FORMAT JSON) "+statement, parameters)).scalar_one()[0])
            assert result["trial_count"] == 1720
            assert result["images"]["image_count"] == 100
            assert result["images"]["stages"] == {"build": 40, "ready": 60}
            assert result["stages"]["running"] == 40
            trials = (await session.scalars(select(Trial).where(Trial.id.in_(trial_ids[:50])))).all()
            statements.clear()
            page = await load_trial_progress(session, trials)
            assert len(statements) <= 3 and len(page) == 50
            assert "unreturned-private-context" not in str(page)
            filtered = apply_trial_monitor_filters(select(Trial.id), target_team=team_id,
                                                    batch_id=batch_id, state="stage:image_preparation")
            compiled = filtered.compile(dialect=engine.sync_engine.dialect, compile_kwargs={"literal_binds": True})
            plan = (await session.execute(text("EXPLAIN (ANALYZE, FORMAT JSON) "+str(compiled)))).scalar_one()[0]
            subplan_loops = []
            def visit(node):
                if node.get("Parent Relationship") == "SubPlan":
                    subplan_loops.append(node["Actual Loops"])
                for child in node.get("Plans", []):
                    visit(child)
            visit(plan["Plan"])
            # Stage predicates are a relational semi-join, not 1720 subplans.
            assert max(subplan_loops, default=0) <= 1
            assert len((await session.scalars(filtered)).all()) == result["stages"]["image_preparation"]
        baseline_path = os.environ.get("LOOM_PIPELINE_BASELINE_SOURCE")
        report_path = os.environ.get("LOOM_PIPELINE_PERF_REPORT")
        if not baseline_path or not report_path:
            return
        spec = importlib.util.spec_from_file_location("pipeline_baseline", baseline_path)
        baseline = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baseline)
        settings = LoomServiceSettings(_env_file=None, db_url=postgres_url, minio_endpoint="http://minio:9000",
            minio_access_key="test", minio_secret_key="test", control_plane_url="http://cp/", gateway_url="http://gw/")
        app = create_app(settings)
        app.state.settings = settings
        app.state.session_factory = sessions
        from loom_service.routes import monitor
        samples = {label: [] for label in ("baseline", "after")}
        sizes = {label: [] for label in samples}
        counts = {label: [] for label in samples}
        sql_samples = {label: [] for label in samples}
        functions = {"baseline": baseline.progress_summary, "after": progress_summary}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local",
                headers={"Authorization": "Bearer "+token}) as client:
            # Alternate order within pairs to avoid attributing time drift or
            # another local process's CPU load to one version.
            for sample in range(10):
                order = ("baseline", "after") if sample % 2 == 0 else ("after", "baseline")
                for label in order:
                    monkeypatch.setattr(monitor, "progress_summary", functions[label])
                    statements.clear()
                    query_ms.clear()
                    start = perf_counter()
                    response = await client.get("/api/v1/monitor/summary", params={"view": "trials", "batch_id": str(batch_id)})
                    elapsed = (perf_counter()-start)*1000
                    assert response.status_code == 200, response.text
                    samples[label].append(elapsed)
                    sizes[label].append(len(response.content))
                    counts[label].append(len(statements))
                    sql_samples[label].append(list(query_ms))
        measurements = {label: {"first_api_ms": values[0], "warm_api_ms": values[1:],
            "warm_median_ms": statistics.median(values[1:]), "response_bytes": sizes[label],
            "sql_statements": counts[label], "sql_ms_by_request": sql_samples[label]}
            for label, values in samples.items()}
        report = {"environment": "same Mac, local Docker PostgreSQL16, in-process real ASGI API, no remote runtime",
            "batch_trials": 1720, "unique_images": 100, "active_builds": 40, "ready_images": 60,
            "current_execution_leases": 40, "private_native_context_bytes_each": 65536,
            "statistics_preparation": "ANALYZE five fixture tables before both versions",
            "comparison_order": "10 interleaved baseline/after pairs, alternating first version; warm excludes first pair",
            "pre_analyze_diagnosis": "Fresh bulk fixture estimates were ~1 row and produced 68,800/172,000-row nested-loop intermediates; un-analyzed new API median was ~186ms vs ~56ms baseline. Final analyzed-data SQL plans are retained below.",
            "measurements": measurements, "stage_filter_explain": plan, "summary_explain": plans,
            "stage_filter_subplan_loops": subplan_loops,
            "limitations": "Local disposable API evidence; not deployed/shared database latency or capacity evidence."}
        output = Path(report_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2)+"\n")
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", record)
        event.remove(engine.sync_engine, "after_cursor_execute", completed)
        async with sessions() as session, session.begin():
            await session.execute(delete(Token).where(Token.token_hash == hashlib.sha256(token.encode()).digest()))
            await session.execute(delete(User).where(User.id == user_id))
        await engine.dispose()
