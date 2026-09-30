"""Kubernetes defaulting must not erase required security or expand workload scope."""
import copy
from uuid import uuid4

import pytest

from loom_service.pool_management.kubernetes_identity import matches_frozen_workload
from tests.unit.test_nebius_pool_task_image_render import build_inputs, render


def documents():
    expected = render(*build_inputs()).job
    actual = copy.deepcopy(expected)
    actual["metadata"].update(uid=str(uuid4()), resourceVersion="11")
    return actual, expected


@pytest.mark.parametrize("field", ["hostNetwork", "hostPID", "hostIPC", "readOnly"])
def test_known_nonnullable_false_fields_may_be_omitted_by_real_api(field):
    actual, expected = documents()
    pod = actual["spec"]["template"]["spec"]
    if field == "readOnly":
        del pod["initContainers"][0]["volumeMounts"][1]["readOnly"]
    else:
        del pod[field]
    assert matches_frozen_workload(actual, expected)


@pytest.mark.parametrize("field", ["automountServiceAccountToken", "enableServiceLinks", "shareProcessNamespace"])
def test_pointer_security_booleans_cannot_disappear(field):
    actual, expected = documents()
    del actual["spec"]["template"]["spec"][field]
    assert not matches_frozen_workload(actual, expected)


def test_readonly_true_cannot_disappear():
    actual, expected = documents()
    del actual["spec"]["template"]["spec"]["initContainers"][0]["volumeMounts"][0]["readOnly"]
    assert not matches_frozen_workload(actual, expected)


def test_api_quantity_canonicalization_is_semantically_equal():
    actual, expected = documents()
    pod = actual["spec"]["template"]["spec"]
    for container in pod["containers"] + pod["initContainers"]:
        for quantities in container["resources"].values():
            quantities.update(cpu="1", memory="2Gi", **{"ephemeral-storage": "16Gi"})
    assert matches_frozen_workload(actual, expected)


@pytest.mark.parametrize("quantity", ["invalid", "sensitive-provider-message", "NaN", "Infinity", "-1", "10"])
def test_invalid_or_changed_resource_quantity_cannot_be_observed(quantity):
    actual, expected = documents()
    actual["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = quantity
    assert not matches_frozen_workload(actual, expected)
