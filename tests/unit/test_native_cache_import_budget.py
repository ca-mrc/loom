"""A whole prepare shares optional-cache limits without relaxing build errors."""

import signal
import time

import pytest
from botocore.exceptions import EndpointConnectionError

from loom_execution_actuator import task_image_runtime as runtime
from tests.unit.test_native_cache_pressure import claim
from tests.unit.test_nebius_task_image_runtime import FakeS3, _v2_cache_objects


def test_request_budget_is_shared_across_candidates_fallbacks_and_components(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_IMPORT_REQUESTS", 3)
    cache = FakeS3({})
    budget = runtime._CacheImportBudget()
    frozen = claim(cache_import_materialization_key="b" * 64)
    for index in range(2):
        runtime._try_import_cache(cache, frozen, index=index, work=tmp_path, budget=budget)
    assert cache.gets == [
        runtime._v2_manifest_key("a" * 64, 0),
        "task-build-cache/" + "a" * 64 + "/0.tar",
        runtime._v2_manifest_key("b" * 64, 0),
    ]
    assert budget.requests == 3
    assert not (tmp_path / "cache-in").exists()


@pytest.mark.parametrize("where", ["get", "body"])
def test_blocked_cache_network_becomes_one_bounded_miss(tmp_path, monkeypatch, where):
    monkeypatch.setattr(runtime, "_CACHE_IMPORT_SECONDS", 0.025)

    class Slow(FakeS3):
        def get_object(self, **kwargs):
            if where == "get":
                self.gets.append(kwargs["Key"])
                time.sleep(2)
            response = super().get_object(**kwargs)
            if where == "body":

                def read(_size):
                    time.sleep(2)

                response["Body"].read = read
            return response

    cache = Slow(_v2_cache_objects("a" * 64, 0, {"layer": b"bytes"}))
    started = time.monotonic()
    runtime._try_import_cache(cache, claim(), index=0, work=tmp_path)
    assert time.monotonic() - started < 0.7
    assert len(cache.gets) == 1
    assert all(body.closed for body in cache.bodies)
    assert not (tmp_path / "cache-in/0").exists()
    if (tmp_path / "cache-in").exists():
        assert list((tmp_path / "cache-in").iterdir()) == []


def test_deadline_remains_shared_after_successful_component(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "_CACHE_IMPORT_SECONDS", 0.1)
    cache = FakeS3(_v2_cache_objects("a" * 64, 0, {"layer": b"bytes"}))
    budget = runtime._CacheImportBudget()
    runtime._try_import_cache(cache, claim(), index=0, work=tmp_path, budget=budget)
    gets = list(cache.gets)
    budget.deadline = time.monotonic() - 1
    runtime._try_import_cache(cache, claim(), index=1, work=tmp_path, budget=budget)
    assert cache.gets == gets
    assert (tmp_path / "cache-in/0/layer").read_bytes() == b"bytes"
    assert not (tmp_path / "cache-in/1").exists()


def test_later_outer_alarm_is_restored_with_elapsed_time():
    original_handler = signal.getsignal(signal.SIGALRM)
    original_timer = signal.getitimer(signal.ITIMER_REAL)

    def outer(_signum, _frame):
        raise AssertionError("outer timeout")

    try:
        signal.signal(signal.SIGALRM, outer)
        signal.setitimer(signal.ITIMER_REAL, 2)
        budget = runtime._CacheImportBudget()
        budget.deadline = time.monotonic() + 0.3
        with budget.network():
            time.sleep(0.03)
        assert signal.getsignal(signal.SIGALRM) is outer
        left, interval = signal.getitimer(signal.ITIMER_REAL)
        assert 1.8 < left < 1.99 and interval == 0
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.setitimer(signal.ITIMER_REAL, *original_timer)


def test_earlier_outer_timeout_is_not_postponed_or_swallowed_as_cache_miss(tmp_path):
    original_handler = signal.getsignal(signal.SIGALRM)
    original_timer = signal.getitimer(signal.ITIMER_REAL)

    def outer(_signum, _frame):
        raise EndpointConnectionError(endpoint_url="https://private.invalid")

    class Slow(FakeS3):
        def get_object(self, **kwargs):
            time.sleep(2)

    try:
        signal.signal(signal.SIGALRM, outer)
        signal.setitimer(signal.ITIMER_REAL, 0.025)
        started = time.monotonic()
        with pytest.raises(EndpointConnectionError):
            runtime._try_import_cache(Slow({}), claim(), index=0, work=tmp_path)
        assert time.monotonic() - started < 0.7
        assert signal.getsignal(signal.SIGALRM) is outer
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.setitimer(signal.ITIMER_REAL, *original_timer)


def test_returning_earlier_alarm_retains_cooperative_sdk_boundary(monkeypatch):
    """Do not replace an existing earlier timer with a competing supervisor."""
    original_handler = signal.getsignal(signal.SIGALRM)
    original_timer = signal.getitimer(signal.ITIMER_REAL)
    fired = []

    def outer(_signum, _frame):
        fired.append(True)

    monkeypatch.setattr(runtime, "_CACHE_IMPORT_SECONDS", 0.03)
    try:
        signal.signal(signal.SIGALRM, outer)
        signal.setitimer(signal.ITIMER_REAL, 0.01)
        with pytest.raises(runtime._CacheBudgetExceededError):
            with runtime._CacheImportBudget().network():
                time.sleep(0.06)
        assert fired == [True]
        assert signal.getsignal(signal.SIGALRM) is outer
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.setitimer(signal.ITIMER_REAL, *original_timer)
