"""Retained settings must preserve omitted and explicit I/O policies."""
from __future__ import annotations

import copy
import json

import pytest
from pydantic import BaseModel

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
