"""Deployment qualification is separate from nonempty-node sampling acceptance."""
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest
from scripts.ops.deploy_nebius_platform import DeploymentError, verify_resource_observers

NAME = "loom-native-resource-observer"
NS = "loom-west-execution-observations"


def documents():
    return [{"kind": "DaemonSet", "metadata": {"name": NAME, "namespace": NS},
             "spec": {"template": {"spec": {"containers": [{"name": "observer", "image": "candidate@sha256:abc"}]}}}}]


def live(count):
    doc = deepcopy(documents()[0])
    doc["metadata"]["generation"] = 2
    doc["status"] = {"observedGeneration": 2, "desiredNumberScheduled": count,
                     "currentNumberScheduled": count, "updatedNumberScheduled": count,
                     "numberReady": count, "numberAvailable": count, "numberMisscheduled": 0}
    return doc


@pytest.mark.parametrize("count", [0, 2])
def test_fixed_candidate_observer_deployment_checks_current_inventory(count):
    kube = SimpleNamespace(get=lambda *args: live(count))
    verify_resource_observers(kube, documents())


@pytest.mark.parametrize("field", ["observedGeneration", "currentNumberScheduled", "updatedNumberScheduled", "numberReady", "numberAvailable"])
def test_unready_or_old_generation_observer_cannot_pass_deployment(field):
    doc = live(2)
    doc["status"][field] = 1
    with pytest.raises(DeploymentError, match="observer readback"):
        verify_resource_observers(SimpleNamespace(get=lambda *args: doc), documents())


def test_old_observer_image_cannot_pass_fixed_candidate_readback():
    doc = live(2)
    doc["spec"]["template"]["spec"]["containers"][0]["image"] = "old-image"
    with pytest.raises(DeploymentError, match="observer readback"):
        verify_resource_observers(SimpleNamespace(get=lambda *args: doc), documents())
