"""Fast one-dispatch rules without reconstructing an installed platform.

The owning stages separately test private journals and retained authority. This
suite calls their shared production engine with storage/transaction ports.
"""
from __future__ import annotations

import copy

import pytest
from scripts.ops.nebius_pool_activation_stage import (
    _guard_choices,
    _pool_choices,
    advance_activation_record,
)


class Activation:
    participants = ("first-participant", "second-participant")

    def __init__(self):
        self.record = {
            "operation_id": "test-operation",
            "opening": "prepared",
            "cancellation": "prepared",
            # Persistence order must not determine dispatch order.
            "guards": {key: {"release": "prepared", "fence": "prepared"}
                       for key in reversed(self.participants)},
        }
        self.saved = copy.deepcopy(self.record)
        self.mode = "closed"
        self.guards = dict.fromkeys(self.participants, "held")
        self.effects = []
        self.failure = None
        self.save_failure = None
        self.ready = True
        self.runtime_checks = 0

    def save(self):
        if self.save_failure == "before":
            raise OSError("storage unavailable")
        self.saved = copy.deepcopy(self.record)
        if self.save_failure == "after":
            raise OSError("storage acknowledgement unavailable")

    def observe(self):
        assert self.record == self.saved
        assert self.mode in _pool_choices(self.record)
        assert all(self.guards[key] in _guard_choices(row)
                   for key, row in self.record["guards"].items())
        return self.mode, dict(self.guards)

    def qualify_runtime(self):
        self.runtime_checks += 1
        assert self.mode == "closed" and set(self.guards.values()) == {"held"}
        if not self.ready:
            raise ValueError("runtime unavailable")

    def _write(self, operation, participant=None):
        field = {"open": "opening", "fence": "cancellation",
                 "release": "release", "guard-fence": "fence"}[operation]
        persisted = self.saved if participant is None else self.saved["guards"][participant]
        assert persisted[field] == "intent"
        if operation == "release":
            assert self.saved["opening"] == "opened" and self.mode == "global"
        if operation == "guard-fence":
            assert self.saved["cancellation"] == "fenced" and self.mode == "fenced"
        self.effects.append((operation, participant))
        if self.failure == (operation, "before"):
            raise TimeoutError("transaction result unavailable")
        if operation in {"open", "fence"}:
            self.mode = "global" if operation == "open" else "fenced"
        else:
            self.guards[participant] = "open" if operation == "release" else "fenced"
        if self.failure == (operation, "after"):
            raise TimeoutError("committed transaction reply unavailable")

    def open_pool(self):
        self._write("open")

    def release_guard(self, participant):
        self._write("release", participant)

    def fence_pool(self):
        self._write("fence")

    def fence_guard(self, participant):
        self._write("guard-fence", participant)

    def advance(self, *, cancel=False):
        return advance_activation_record(
            api=self, record=self.record, participants=self.participants,
            observe=self.observe, save=self.save, cancel=cancel,
        )


@pytest.mark.parametrize("operation", ["open", "release", "fence", "guard-fence"])
@pytest.mark.parametrize("when", ["before", "after"])
def test_unknown_transactions_are_observed_once_in_both_directions(operation, when):
    api = Activation()
    cancel = operation in {"fence", "guard-fence"}
    if cancel:
        api.advance()
        api.effects.clear()
        api.ready = False  # Cancellation must not depend on runtime health.
    api.failure = operation, when
    result = api.advance(cancel=cancel)
    api.failure = None
    if when == "before":
        pending = {"open": "pending_pool_opening", "release": "pending_guard_release",
                   "fence": "pending_pool_fence", "guard-fence": "pending_guard_fence"}
        assert result["status"] == pending[operation]
        effects = list(api.effects)
        assert api.advance(cancel=cancel) == result and api.effects == effects
        if operation in {"open", "fence"}:
            api.mode = "global" if operation == "open" else "fenced"
        else:
            api.guards[api.participants[0]] = "fenced" if cancel else "open"
    result = api.advance(cancel=cancel)
    assert result["status"] == ("pool_activation_cancelled" if cancel else "pool_activation_complete")
    assert result["admission_open"] is not cancel
    assert result["legacy_restore_allowed"] is False
    expected = [("fence" if cancel else "open", None),
                *(("guard-fence" if cancel else "release", key) for key in api.participants)]
    assert api.effects == expected
    assert api.advance(cancel=cancel) == result and api.effects == expected


@pytest.mark.parametrize("when", ["before", "after"])
def test_unconfirmed_intent_storage_never_dispatches(when):
    api = Activation()
    api.save_failure = when
    with pytest.raises(OSError):
        api.advance()
    assert api.effects == []
    assert api.saved["opening"] == ("prepared" if when == "before" else "intent")


def test_unqualified_runtime_never_persists_opening_or_dispatches():
    api = Activation()
    api.ready = False
    with pytest.raises(ValueError, match="runtime unavailable"):
        api.advance()
    assert api.saved["opening"] == "prepared" and api.effects == []


def test_terminal_cancellation_never_reopens_or_requires_runtime_health():
    api = Activation()
    api.ready = False
    result = api.advance(cancel=True)
    assert result["status"] == "pool_activation_cancelled"
    assert api.runtime_checks == 0
    effects = list(api.effects)
    with pytest.raises(ValueError):
        api.advance()
    assert api.effects == effects
