"""Pure reuse must neither alias inputs/outputs nor hide changed qualification."""
from __future__ import annotations

from dataclasses import dataclass

import pytest
from pydantic import BaseModel
from scripts.ops.nebius_pool_projection import pure_projection


@dataclass(frozen=True)
class Request:
    payload: object


def test_projection_observes_raw_pydantic_field_set_changes():
    class Model(BaseModel):
        count: int = 1

    @pure_projection
    def render(request):
        return request.payload.model_dump(exclude_unset=True)

    value = Model()
    assert render(Request(value)) == {}
    value.count = 1
    assert render(Request(value)) == {"count": 1}


@pytest.mark.parametrize("before,after", [(1, True), ([1], (1,)), ({1}, frozenset({1})), (1, 1.0)])
def test_projection_requalifies_equal_values_with_different_types(before, after):
    @pure_projection
    def render(request):
        if type(request.payload) is not type(before):
            raise ValueError("wrong input type")
        return {"value": request.payload}

    assert render(Request(before)) == {"value": before}
    with pytest.raises(ValueError, match="wrong input type"):
        render(Request(after))


def test_projection_detaches_nested_results_and_requalifies_nested_input_changes():
    @pure_projection
    def render(request):
        return request.payload

    source = {"rows": [{"replicas": 1}]}
    first = render(Request(source))
    first["rows"][0]["replicas"] = 7
    assert source == {"rows": [{"replicas": 1}]}
    assert render(Request(source)) == {"rows": [{"replicas": 1}]}
    source["rows"][0]["replicas"] = 2
    assert render(Request(source)) == {"rows": [{"replicas": 2}]}


def test_unknown_input_type_bypasses_reuse():
    class Mutable:
        replicas = 1

    @pure_projection
    def render(request):
        return {"replicas": request.payload.replicas}

    value = Mutable()
    assert render(Request(value)) == {"replicas": 1}
    value.replicas = 2
    assert render(Request(value)) == {"replicas": 2}


def test_projection_does_not_retain_failure_or_inputs_changed_during_derivation():
    calls = []

    @pure_projection
    def render(request):
        calls.append(None)
        count = request.payload["count"]
        request.payload["count"] += 1
        if count == 0:
            raise ValueError("not qualified")
        return {"count": count}

    request = Request({"count": 0})
    with pytest.raises(ValueError):
        render(request)
    assert render(request) == {"count": 1}
    assert render(request) == {"count": 2}
    assert len(calls) == 3


def test_projection_replaces_one_entry_instead_of_retaining_every_operation():
    calls = []

    @pure_projection
    def render(request):
        calls.append(request.payload)
        return {"operation": request.payload}

    assert render(Request("one")) == render(Request("one")) == {"operation": "one"}
    assert render(Request("two")) == {"operation": "two"}
    assert render(Request("one")) == {"operation": "one"}
    assert calls == ["one", "two", "one"]


def test_projection_requalifies_alias_changes_even_when_nested_values_are_equal():
    @pure_projection
    def render(request):
        return {"shared": request.payload[0] is request.payload[1]}

    child = {"replicas": 1}
    assert render(Request([child, child])) == {"shared": True}
    assert render(Request([child, dict(child)])) == {"shared": False}


def test_input_snapshot_visits_shared_graph_once_per_call_without_hiding_mutation():
    from scripts.ops.nebius_pool_projection import _snapshot

    reads = {}

    @dataclass
    class Node:
        edges: list

        def __getattribute__(self, name):
            if name == "edges":
                reads[id(self)] = reads.get(id(self), 0) + 1
            return object.__getattribute__(self, name)

    leaf = {"replicas": 1}
    root = Node([leaf])
    for _ in range(12):
        root = Node([root, root])
    before = _snapshot(root)
    assert len(reads) == 13 and set(reads.values()) == {1}
    reads.clear()
    assert _snapshot(root) == before
    assert len(reads) == 13 and set(reads.values()) == {1}
    leaf["replicas"] = 2
    assert _snapshot(root) != before
