"""Retained settings must preserve omitted and explicit I/O policies."""
from __future__ import annotations

import copy
import json

import pytest
from pydantic import BaseModel, ValidationError

from loom_execution_actuator.task_image_settings import NativeTaskImageSettings


class StoredBuilder(BaseModel):
    settings: NativeTaskImageSettings


@pytest.mark.parametrize("policy", [None, 50.0, 17.5])
def test_retained_builder_preserves_io_policy_presence_through_roundtrips(policy):
    authored: dict[str, object] = {
        "namespace": "builds", "service_image": "registry.example/service",
        "storage_endpoint": "https://storage.example", "storage_region": "eu-north1",
        "source_bucket": "source", "registry_repository": "registry.example/tasks",
    }
    if policy is not None:
        authored["io_full_avg10_max"] = policy
    stored = StoredBuilder.model_validate({"settings": authored})
    for current in (stored, copy.deepcopy(stored),
            StoredBuilder.model_validate_json(stored.model_dump_json()),
            StoredBuilder(settings=stored.settings.model_copy(update={"service_image": "registry.example/new"}))):
        assert current.settings.io_full_avg10_max == (50.0 if policy is None else policy)
        for serialized in (current.model_dump(mode="json"), json.loads(current.model_dump_json())):
            settings = serialized["settings"]
            assert ("io_full_avg10_max" in settings) is (policy is not None)
            if policy is not None:
                assert settings["io_full_avg10_max"] == policy

    # An operator explicitly selecting the default must freeze that choice too.
    explicit = stored.settings.model_copy(update={"io_full_avg10_max": 50.0})
    assert json.loads(explicit.model_dump_json())["io_full_avg10_max"] == 50.0


@pytest.mark.parametrize("cpu_request", [None, 1000, 4000])
def test_cpu_request_is_optional_and_retained_without_changing_hard_limits(cpu_request):
    payload = {"namespace": "builds", "service_image": "registry.example/service@sha256:" + "a" * 64,
               "storage_endpoint": "https://storage.example", "storage_region": "eu-north1",
               "source_bucket": "source", "registry_repository": "registry.example/tasks"}
    if cpu_request is not None:
        payload["cpu_request_millis"] = cpu_request
    settings = NativeTaskImageSettings.model_validate(payload)
    restored = NativeTaskImageSettings.model_validate_json(settings.model_dump_json())
    assert restored.cpu_request_millis == cpu_request
    assert ("cpu_request_millis" in settings.model_dump(mode="json")) is (cpu_request is not None)
    assert restored.job_config().cpu_request_millis == cpu_request
    assert (restored.cpu_millis, restored.memory_mib, restored.ephemeral_storage_mib) == (4000, 4096, 32768)


@pytest.mark.parametrize("cpu_request", [True, 0, 99, 4001, 1000.5])
def test_cpu_request_rejects_unbounded_or_coerced_values(cpu_request):
    with pytest.raises(ValidationError):
        NativeTaskImageSettings(namespace="builds", service_image="registry.example/service",
            storage_endpoint="https://storage.example", storage_region="eu-north1",
            source_bucket="source", registry_repository="registry.example/tasks", cpu_request_millis=cpu_request)


@pytest.mark.parametrize("budget", [None, 1024, 4096, 65536, 1048576])
def test_cache_budget_preserves_legacy_omission_and_freezes_explicit_policy(budget):
    payload = {"namespace": "builds", "service_image": "registry.example/service",
               "storage_endpoint": "https://storage.example", "storage_region": "eu-north1",
               "source_bucket": "source", "registry_repository": "registry.example/tasks"}
    if budget is not None:
        payload["cache_total_mib"] = budget
    stored = StoredBuilder.model_validate({"settings": payload})
    for current in (stored, copy.deepcopy(stored),
            StoredBuilder.model_validate_json(stored.model_dump_json()),
            StoredBuilder(settings=stored.settings.model_copy(update={"service_image": "registry.example/new"}))):
        assert current.settings.cache_total_mib == (4096 if budget is None else budget)
        for settings in (current.model_dump(mode="json")["settings"],
                         json.loads(current.model_dump_json())["settings"],
                         current.settings.runtime_configuration()):
            assert ("cache_total_mib" in settings) is (budget is not None)
            if budget is not None:
                assert settings["cache_total_mib"] == budget

    explicit_default = stored.settings.model_copy(update={"cache_total_mib": 4096})
    assert explicit_default.model_dump(mode="json")["cache_total_mib"] == 4096
    assert explicit_default.runtime_configuration()["cache_total_mib"] == 4096


@pytest.mark.parametrize("budget", [None, True, False, 0, -1, 1023, 1048577, 2**63, "65536", 65536.0])
def test_cache_budget_rejects_coercion_and_out_of_range_values(budget):
    with pytest.raises(ValidationError):
        NativeTaskImageSettings(namespace="builds", service_image="registry.example/service",
            storage_endpoint="https://storage.example", storage_region="eu-north1",
            source_bucket="source", registry_repository="registry.example/tasks", cache_total_mib=budget)


@pytest.mark.parametrize("options", [
    {}, {"cache_gc_policy": "legacy-seven-day", "reuse_task_cache_for_verifier": False},
    {"cache_gc_policy": "budget-pressure", "reuse_task_cache_for_verifier": True},
])
def test_cache_optimizations_keep_legacy_shape_and_freeze_explicit_choices(options):
    payload = {"namespace": "builds", "service_image": "registry.example/service",
               "storage_endpoint": "https://storage.example", "storage_region": "eu-north1",
               "source_bucket": "source", "registry_repository": "registry.example/tasks", **options}
    original = NativeTaskImageSettings.model_validate(payload)
    restored = NativeTaskImageSettings.model_validate_json(original.model_dump_json())
    assert restored.cache_gc_policy == options.get("cache_gc_policy", "legacy-seven-day")
    assert restored.reuse_task_cache_for_verifier is options.get("reuse_task_cache_for_verifier", False)
    for value in (original.model_dump(mode="json"), restored.model_dump(mode="json"),
                  restored.runtime_configuration()):
        for key in ("cache_gc_policy", "reuse_task_cache_for_verifier"):
            assert (key in value) is (key in options)
            if key in options:
                assert value[key] == options[key]


@pytest.mark.parametrize("options", [
    {"cache_gc_policy": "off"}, {"cache_gc_policy": None},
    {"reuse_task_cache_for_verifier": 1}, {"reuse_task_cache_for_verifier": "true"},
])
def test_cache_optimization_settings_reject_invalid_or_coerced_choices(options):
    with pytest.raises(ValidationError):
        NativeTaskImageSettings(namespace="builds", service_image="registry.example/service",
            storage_endpoint="https://storage.example", storage_region="eu-north1",
            source_bucket="source", registry_repository="registry.example/tasks", **options)
