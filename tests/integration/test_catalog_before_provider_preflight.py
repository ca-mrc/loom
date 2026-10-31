"""Reject inadmissible catalog/targets before a Codex generation probe."""

import json
from copy import deepcopy
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import create_engine, delete, select, update
from sqlalchemy.orm import Session

from loom.db.schema import Benchmark, BenchmarkAlias, ProviderModelCache, Task, Trial
from tests.integration.test_trial_batch_provider_connection_id import (
    _auth,
    _cache_model,
    _client,
    _stub_codex_preflight,
)
from tests.integration.test_trial_batch_provider_connection_id import (
    app_setup as app_setup,
)

MODEL = "catalog-order-model"


@pytest.fixture
async def catalog_probe(app_setup, monkeypatch):
    app, tokens, ids = app_setup
    profile, alias = "probe-profile-" + uuid4().hex, "probe-alias-" + uuid4().hex
    engine = create_engine(str(app.state.settings.db_url))
    with Session(engine) as session:
        session.add(Benchmark(
            id=profile, display_name="Probe order fixture", execution_state="runnable",
            upstream_kind="fixture", upstream_locator="fixture:probe-order", upstream_revision="one",
            license_spdx="MIT", license_url="", splits=["test"],
        ))
        session.flush()
        session.add(BenchmarkAlias(alias=alias, benchmark_id=profile))
        session.execute(update(Task).where(Task.id == ids["task_id"]).values(benchmark_id=profile))
        session.commit()
    _cache_model(app, ids["conn_a"], MODEL)
    probes = _stub_codex_preflight(monkeypatch, http_status=200, error_code=None)
    forwarded = []

    def cp(request):
        forwarded.append(json.loads(request.content))
        return httpx.Response(201, json={"trial_id": "00000000-0000-0000-0000-000000000099", "state": "queued"})

    await app.state.http_client.aclose()
    app.state.http_client = httpx.AsyncClient(transport=httpx.MockTransport(cp), base_url="http://cp")
    try:
        yield app, tokens, ids, profile, alias, probes, forwarded
    finally:
        with Session(engine) as session:
            session.execute(update(Task).where(Task.id == ids["task_id"]).values(benchmark_id=None))
            session.execute(delete(BenchmarkAlias).where(BenchmarkAlias.alias == alias))
            session.execute(delete(Benchmark).where(Benchmark.id == profile))
            session.commit()
        engine.dispose()


def _request(fixture, surface, selector="physical", **extra):
    app, tokens, ids, profile, alias, _probes, _forwarded = fixture
    config = {"agent_name": "codex", "agent_model": {"provider": "openai", "name": MODEL}}
    provider = {"provider_connection_id": str(ids["conn_a"]), "provider_model_id": MODEL}
    if surface == "trial":
        path, body = "/api/v1/trials", {"task_id": ids["task_id"], "config": config, **provider}
    else:
        selected = profile if selector == "physical" else alias
        path = "/api/v1/batches" + ("/dry-run" if surface == "dry-run" else "")
        body = {"name": "Catalog before probe", "purpose": "evaluation", "backend": "fake",
                "task_filter": {"benchmark_ids": [selected]}, "trial_config": config, **provider}
    return _client(app).post(path, headers=_auth(tokens["a"]), json={**body, **extra})


def _cache_timestamp(fixture):
    app, _tokens, ids, *_ = fixture
    engine = create_engine(str(app.state.settings.db_url))
    try:
        with Session(engine) as session:
            return session.scalar(select(ProviderModelCache.last_preflight_at).where(
                ProviderModelCache.provider_connection_id == ids["conn_a"],
                ProviderModelCache.model_id == MODEL,
            ))
    finally:
        engine.dispose()


def _change(fixture, *, state=None, hardware=False):
    app, _tokens, ids, profile, *_ = fixture
    engine = create_engine(str(app.state.settings.db_url))
    try:
        with Session(engine) as session:
            if state is not None:
                session.execute(update(Benchmark).where(Benchmark.id == profile).values(execution_state=state))
            if hardware:
                row = session.get(Task, ids["task_id"])
                config = deepcopy(row.config)
                config["environment"].update(gpus=1, gpu_types=["H100"])
                row.config = config
            session.commit()
    finally:
        engine.dispose()


@pytest.mark.parametrize("state,reason", [("pending", "benchmark_not_runnable"), ("historical", "benchmark_retired")])
@pytest.mark.parametrize("surface,selector", [("batch", "physical"), ("batch", "alias"),
    ("dry-run", "physical"), ("dry-run", "alias"), ("trial", "physical")])
def test_catalog_rejection_precedes_probe_cache_write_and_forward(catalog_probe, state, reason, surface, selector):
    _change(catalog_probe, state=state)
    response = _request(catalog_probe, surface, selector)
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["reason"] == reason
    assert catalog_probe[5] == []
    assert catalog_probe[6] == []
    assert _cache_timestamp(catalog_probe) is None


@pytest.mark.parametrize("surface", ["batch", "dry-run", "trial"])
def test_exact_h100_rejection_precedes_probe(catalog_probe, surface):
    _change(catalog_probe, hardware=True)
    response = _request(catalog_probe, surface)
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["reason"] == "task_requirements_unsupported"
    assert catalog_probe[5] == []
    assert catalog_probe[6] == []
    assert _cache_timestamp(catalog_probe) is None


@pytest.mark.parametrize("surface", ["batch", "dry-run", "trial"])
def test_missing_selected_runtime_rejected_before_probe(catalog_probe, monkeypatch, surface):
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "0")
    response = _request(catalog_probe, surface, **({"backend": "nebius"} if surface != "trial" else {}))
    assert response.status_code == (200 if surface == "dry-run" else 400), response.text
    body = response.json()
    if surface == "dry-run":
        assert body["accepted"] is False
        reasons = body["rejection_reasons"]
    else:
        assert body["detail"]["reason"] == "nebius_task_incompatible"
        reasons = body["detail"]["rejection_reasons"]
    assert "runtime_profile_unavailable" in reasons[catalog_probe[2]["task_id"]]
    assert catalog_probe[5] == []
    assert catalog_probe[6] == []
    assert _cache_timestamp(catalog_probe) is None


@pytest.mark.parametrize("surface", ["batch", "dry-run", "trial"])
def test_runnable_compatible_request_retains_one_probe(catalog_probe, surface):
    response = _request(catalog_probe, surface)
    assert response.status_code == (200 if surface == "dry-run" else 201), response.text
    assert catalog_probe[5] == [MODEL]
    assert _cache_timestamp(catalog_probe) is not None
    assert len(catalog_probe[6]) == (1 if surface == "trial" else 0)


def test_historical_idempotent_trial_replay_retains_existing_fast_path(catalog_probe):
    app, _tokens, ids, _profile, _alias, *_ = catalog_probe
    replay = "existing-" + uuid4().hex
    engine = create_engine(str(app.state.settings.db_url))
    try:
        with Session(engine) as session:
            session.add(Trial(id=uuid4(), task_id=ids["task_id"], team_id=ids["team_a"],
                state="succeeded", result={"aggregate_reward": 1}, config={}, requires_caps={},
                idempotency_key=replay))
            session.commit()
    finally:
        engine.dispose()
    _change(catalog_probe, state="historical")
    response = _request(catalog_probe, "trial", idempotency_key=replay)
    assert response.status_code == 201, response.text
    assert len(catalog_probe[6]) == 1


def test_foreign_team_idempotency_key_cannot_bypass_catalog_rejection(catalog_probe):
    app, _tokens, ids, _profile, _alias, *_ = catalog_probe
    replay = "foreign-" + uuid4().hex
    engine = create_engine(str(app.state.settings.db_url))
    try:
        with Session(engine) as session:
            session.add(Trial(
                id=uuid4(), task_id=ids["task_id"], team_id=ids["team_b"],
                state="succeeded", result={"aggregate_reward": 1}, config={},
                requires_caps={}, idempotency_key=replay,
            ))
            session.commit()
    finally:
        engine.dispose()
    _change(catalog_probe, state="historical")
    response = _request(catalog_probe, "trial", idempotency_key=replay)
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["reason"] == "benchmark_retired"
    assert catalog_probe[5] == []
    assert catalog_probe[6] == []
    assert _cache_timestamp(catalog_probe) is None


def test_hosted_trial_checks_shared_target_before_probe(catalog_probe, monkeypatch):
    _app, _tokens, ids, _profile, _alias, probes, forwarded = catalog_probe
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "0")
    admitted = []

    async def admit_target(_session, **kwargs):
        # The shared target admission owns compatibility checks. This test
        # exercises its successful route boundary and ordering without a model
        # or a live execution target.
        assert kwargs["backend"] == "nebius"
        assert kwargs["task_ids"] == [ids["task_id"]]
        assert probes == []
        assert _cache_timestamp(catalog_probe) is None
        admitted.append(kwargs)

    monkeypatch.setattr("loom_service.routes.trials.admit_execution_backend", admit_target)
    response = _request(catalog_probe, "trial")
    assert response.status_code == 201, response.text
    assert len(admitted) == 1
    assert probes == [MODEL]
    assert _cache_timestamp(catalog_probe) is not None
    assert len(forwarded) == 1
