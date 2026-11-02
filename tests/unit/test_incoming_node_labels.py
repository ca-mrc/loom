"""Actual operational labels preserve historical observation wire payloads."""
from types import SimpleNamespace

from loom_execution_capacity_collector.contracts import CapacityPlacement
from loom_execution_capacity_collector.kubernetes import scheduling_node_labels
from tests.execution_placement_fixtures import placement_fixture


def test_legacy_observation_labels_omission_roundtrips_without_digest_drift():
    raw = CapacityPlacement.model_validate(placement_fixture(target_id="owner")).model_dump(mode="json")
    assert "labels" not in raw["nodes"][0]
    assert CapacityPlacement.model_validate(raw).model_dump(mode="json") == raw
    raw["nodes"][0]["labels"] = {"loom.nebius/node-role": "integration-execution"}
    assert CapacityPlacement.model_validate(raw).model_dump(mode="json") == raw


def test_collector_projects_only_actual_scheduling_labels_without_private_metadata():
    labels = {"loom.nebius/node-role": "integration-execution", "loom.nebius/platform": "integration",
        "nebius.com/node-group-id": "group", "kubernetes.io/arch": "amd64",
        "private.example/marker": "do-not-project"}
    projected = scheduling_node_labels(SimpleNamespace(metadata=SimpleNamespace(labels=labels)))
    assert projected == {key: value for key, value in labels.items() if key != "private.example/marker"}
