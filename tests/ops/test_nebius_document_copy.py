"""Faster manifest copies must preserve deep-copy semantics and authority checks."""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest


def test_manifest_copy_detaches_mutable_values_and_preserves_aliases_cycles_and_types():
    from scripts.ops.nebius_ingress_stage import _copy_document

    shared = {"nested": [1, True, None, -0.0, "value"]}
    source = {"left": shared, "right": shared, "tuple": (shared,),
        "other": {UUID(int=1): Decimal("0.125"), "path": Path("/test"), "date": datetime(2026, 1, 1, tzinfo=UTC)}}
    source["cycle"] = source
    clone = _copy_document(source)
    assert clone is not source and clone["cycle"] is clone
    assert clone["left"] is clone["right"] is clone["tuple"][0]
    assert clone["left"] is not shared and type(clone["tuple"]) is tuple
    assert clone["other"] == source["other"] and type(clone["other"][UUID(int=1)]) is Decimal
    clone["left"]["nested"].append("changed")
    assert shared["nested"] == [1, True, None, -0.0, "value"]


def test_manifest_copy_retains_custom_deepcopy_protocol_for_non_json_objects():
    from scripts.ops.nebius_ingress_stage import _copy_document

    class Custom:
        def __deepcopy__(self, memo):
            return {"copied": True}

    item = Custom()
    clone = _copy_document({"one": item, "two": item})
    assert clone == {"one": {"copied": True}, "two": {"copied": True}}
    assert clone["one"] is clone["two"]


def test_stable_snapshot_normalizes_quantities_without_rewriting_input_or_ownership():
    from scripts.ops.nebius_ingress_stage import StageError
    from scripts.ops.nebius_management_switch import _stable

    source = {"kind": "Deployment", "metadata": {"name": "manager", "uid": str(UUID(int=1)),
        "resourceVersion": "7", "annotations": {"deployment.kubernetes.io/revision": "3", "retained": "yes"}},
        "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "manager",
            "resources": {"requests": {"cpu": "1000m", "memory": "1Gi"}, "limits": {"cpu": "1"}}}]}}},
        "status": {"readyReplicas": 1}}
    stable = _stable(source)
    assert "status" not in stable and "uid" not in stable["metadata"]
    assert stable["metadata"]["annotations"] == {"retained": "yes"}
    resources = stable["spec"]["template"]["spec"]["containers"][0]["resources"]
    assert resources["requests"] == {"cpu": "1", "memory": "1073741824"}
    assert source["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] == "1000m"
    assert source["metadata"]["uid"] == str(UUID(int=1)) and source["status"] == {"readyReplicas": 1}
    for field, value in (("deletionTimestamp", "2026-01-01T00:00:00Z"), ("ownerReferences", [{"uid": "foreign"}])):
        with pytest.raises(StageError):
            _stable(source | {"metadata": source["metadata"] | {field: value}})
