"""Portable combination identity through the actual delivery tar writer."""

import csv
import io
import json
import tarfile
from typing import Any
from uuid import uuid4

from loom.db.schema import Batch, Trial
from loom_service.delivery_export import SelectedTrial, _build_archive, _ledger_rows, _summary
from loom_service.trial_bundles import ObjectRef


def _selected(batch: Batch, *, idx: int = 0, **overrides: Any) -> SelectedTrial:
    trial = Trial(
        id=uuid4(),
        task_id="task-1",
        sample_idx=0,
        combination_idx=idx,
        state="succeeded",
        config={},
        failure_reason=None,
        **overrides,
    )
    return SelectedTrial(
        trial=trial,
        batch=batch,
        priority=0,
        selection_source="main",
        trajectory=ObjectRef(kind="trajectory", trial_id=trial.id, bucket="data", key="events"),
        atif=ObjectRef(kind="atif", trial_id=trial.id, bucket="data", key="atif"),
        reward=0.0,
    )


def _archive(selected: list[SelectedTrial]) -> tuple[list[dict], list[dict], dict]:
    class Objects:
        def get_object(self, **kwargs: Any) -> dict:
            return {"Body": io.BytesIO(b"{}\n"), "ContentLength": 3}

    summary = _summary(
        main=selected[0].batch,
        supplements=[item.batch for item in selected[1:]],
        selected=selected,
        object_validation={"checked": 0, "missing": []},
    )
    archive = _build_archive(
        client=Objects(),
        manifest=summary,
        summary=summary,
        rows=_ledger_rows(selected),
        selected=selected,
        mode="lightweight",
    )
    try:
        with tarfile.open(fileobj=archive.body, mode="r:gz") as tar:
            ledger = [json.loads(line) for line in tar.extractfile("ledger/trials.jsonl")]
            csv_rows = list(
                csv.DictReader(
                    io.StringIO(
                        tar.extractfile("ledger/trials.csv").read().decode(),
                    )
                )
            )
            manifest = json.load(tar.extractfile("manifest.json"))
            assert manifest == json.load(tar.extractfile("summary.json"))
            return ledger, csv_rows, manifest
    finally:
        archive.body.close()


def test_downloaded_matrix_keeps_source_batch_labels_and_frozen_routes() -> None:
    first, second, frozen = uuid4(), uuid4(), uuid4()
    model = {"provider": "openai", "name": "same-model"}
    batches = [
        Batch(
            id=uuid4(),
            trial_config={},
            provider_model_id=None,
            combinations=[
                {
                    "label": label,
                    "agent_name": "terminus-2",
                    "agent_model": model,
                    "provider_connection_id": str(conn),
                    "provider_model_id": "same-model",
                    "api_key": "do-not-copy-config-secrets",
                    "base_url": "do-not-copy-endpoints",
                }
            ],
        )
        for label, conn in [("first-route", first), ("supplement-route", second)]
    ]
    selected = [
        _selected(batches[0], provider_connection_id=first, provider_model_id="same-model"),
        _selected(batches[1], provider_connection_id=frozen, provider_model_id="same-model"),
    ]
    ledger, csv_rows, manifest = _archive(selected)
    for rows in [ledger, csv_rows]:
        assert [row["combination_label"] for row in rows] == ["first-route", "supplement-route"]
        assert [row["provider_connection_id"] for row in rows] == [str(first), str(frozen)]
        assert [row["provider_model_id"] for row in rows] == ["same-model", "same-model"]
        assert [row["model_name"] for row in rows] == ["same-model", "same-model"]
    routes = manifest["combination_routes"]
    assert [row["selected_batch_id"] for row in routes] == [str(b.id) for b in batches]
    assert [row["provider_connection_id"] for row in routes] == [str(first), str(frozen)]
    assert "do-not-copy" not in json.dumps([ledger, csv_rows, manifest])


def test_legacy_batch_defaults_and_no_model_routes_remain_distinct() -> None:
    conn = uuid4()
    batch = Batch(
        id=uuid4(),
        combinations=[],
        provider_connection_id=conn,
        provider_model_id="legacy-model",
        trial_config={
            "agent_name": "terminus-2",
            "agent_model": {
                "provider": "openai",
                "name": "legacy-model",
            },
        },
    )
    legacy = _selected(batch)
    oracle = _selected(batch)
    oracle.trial.config = {"agent_name": "oracle", "agent_model": None}
    ledger, csv_rows, manifest = _archive([legacy, oracle])
    assert ledger[0]["provider_connection_id"] == str(conn)
    assert ledger[0]["provider_model_id"] == "legacy-model"
    assert ledger[0]["combination_label"] == "terminus-2 / legacy-model"
    assert ledger[1]["combination_label"] == "oracle / no model"
    for field in ["provider_connection_id", "provider_model_id", "model_provider", "model_name"]:
        assert ledger[1][field] is None
        assert csv_rows[1][field] == ""
    assert len(manifest["combination_routes"]) == 2
