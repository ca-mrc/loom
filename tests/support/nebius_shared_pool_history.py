"""Read-only predecessor I/O for shared-pool process contract tests.

These tests vary remote process/SQL observations and current child journals.
Completed predecessor bytes are fixed inputs, not the behavior under test.
Connected cases keep real disk reads; production never uses this test double.
"""
from __future__ import annotations

import hashlib
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def captured_predecessor_history(bindings, monkeypatch):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops.nebius_shared_pool_operation import _history

    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    active = Path(manager.operation["state_dir"]).parent / "shared-pool-handoff"
    read = private_state._private_read
    captured = {}
    for path, checksum in bindings.plan.history.items():
        # Mutable child receipts must continue through the real file transport.
        assert not path.is_relative_to(active)
        raw = read(path, limit=4 * 1024**2)
        assert hashlib.sha256(raw).hexdigest() == checksum
        captured[path] = raw

    def read_predecessor(path, *, limit=65_536):
        raw = captured.get(path)
        if raw is not None and len(raw) <= limit:
            return raw
        return read(path, limit=limit)

    with monkeypatch.context() as patch:
        patch.setattr(private_state, "_private_read", read_predecessor)
        yield
    # The fake is per-case and does not permit tests to mutate predecessors.
    _history(bindings)
