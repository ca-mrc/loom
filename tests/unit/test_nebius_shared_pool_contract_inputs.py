"""Fixed predecessor projections never replace mutable child/drift checks."""
from pathlib import Path
from types import SimpleNamespace

import pytest


def _inputs(tmp_path, monkeypatch):
    from scripts.ops import nebius_certificates as private
    from scripts.ops import nebius_shared_pool_operation as operation

    state = tmp_path / "retained"
    state.mkdir()
    predecessor = state / "predecessor.json"
    predecessor.write_bytes(b"fixed predecessor")
    import hashlib

    plan = SimpleNamespace(history={predecessor: hashlib.sha256(predecessor.read_bytes()).hexdigest()},
                           preactivation=None, development=SimpleNamespace(schema_successor=None),
                           storage=None, nested={"value": "fixed"})
    retained = SimpleNamespace(operation={"state_dir": str(state / "operation")})
    runtime = SimpleNamespace(value="initial", database=SimpleNamespace(
        manager=SimpleNamespace(retained=SimpleNamespace(request=SimpleNamespace(retained=retained)))))
    plan.development.runtime = SimpleNamespace(request=runtime)
    calls = []

    def prepare(**arguments):
        calls.append(arguments["runtime"].value)
        return plan

    monkeypatch.setattr(operation, "prepare_shared_pool_handoff", prepare)
    monkeypatch.setattr(private, "_private_read", lambda path, *, limit: Path(path).read_bytes()[:limit])
    bindings = SimpleNamespace(plan=plan)
    return operation, private, bindings, runtime, calls, predecessor


def test_projection_is_case_local_and_mutation_falls_back_to_real_constructor(tmp_path, monkeypatch):
    from tests.support.nebius_shared_pool_history import captured_predecessor_history

    operation, _, bindings, runtime, calls, _ = _inputs(tmp_path, monkeypatch)
    arguments = dict(runtime=runtime, observation={}, legacy=None, repo_root=tmp_path)
    complete = dict(arguments, preactivation=None, schema_successor=None, storage=None)
    with captured_predecessor_history(bindings, monkeypatch, arguments=arguments):
        first = operation.prepare_shared_pool_handoff(**complete)
        first.nested["value"] = "caller mutation"
        assert operation.prepare_shared_pool_handoff(**complete).nested == {"value": "fixed"}
        assert calls == ["initial"]
        runtime.value = "changed in place"
        operation.prepare_shared_pool_handoff(**complete)
        assert calls == ["initial", "changed in place"]
    operation.prepare_shared_pool_handoff(**complete)
    assert calls == ["initial", "changed in place", "changed in place"]


def test_child_bytes_stay_current_and_physical_predecessor_drift_still_fails(tmp_path, monkeypatch):
    from tests.support.nebius_shared_pool_history import captured_predecessor_history

    _, private, bindings, _, _, predecessor = _inputs(tmp_path, monkeypatch)
    child = tmp_path / "retained/shared-pool-handoff/child.json"
    child.parent.mkdir()
    child.write_bytes(b"intent")
    with pytest.raises(ValueError):
        with captured_predecessor_history(bindings, monkeypatch):
            assert private._private_read(child, limit=100) == b"intent"
            child.write_bytes(b"complete")
            assert private._private_read(child, limit=100) == b"complete"
            predecessor.write_bytes(b"drift")


def test_captured_history_rejects_mutable_child_as_a_fixed_input(tmp_path, monkeypatch):
    from tests.support.nebius_shared_pool_history import captured_predecessor_history

    _, _, bindings, _, _, _ = _inputs(tmp_path, monkeypatch)
    child = tmp_path / "retained/shared-pool-handoff/child.json"
    bindings.plan.history = {child: "0" * 64}
    with pytest.raises(AssertionError):
        with captured_predecessor_history(bindings, monkeypatch):
            pass
