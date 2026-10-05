"""Retained remote consumers are retired, never registered as pool aliases."""
from __future__ import annotations

import copy
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from tests.ops.test_nebius_pool_retirement import initialize, retire
from tests.ops.test_nebius_pool_retirement import retirement_inputs as retirement_inputs
from tests.ops.test_nebius_pool_role_fencing import fencing_inputs as fencing_inputs
from tests.ops.test_nebius_pool_runtime import env
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def dormant_consumer(request, *, target="nebius-retained-remote"):
    from scripts.ops.nebius_pool_dormant import DormantPoolConsumer

    participant = request.migration.registration.spec.participants[0]
    actuator = copy.deepcopy(next(row for row in request.actuators
        if row["metadata"]["namespace"] == participant.execution_namespace.name))
    collector = copy.deepcopy(next(row for row in request.collectors
        if row["metadata"]["namespace"] == participant.execution_namespace.name))
    for document, suffix in ((actuator, "actuator"), (collector, "collector")):
        name = target + "-" + suffix
        document["metadata"].update(name=name, uid=str(uuid4()))
        template = document["spec"]["template"] if suffix == "actuator" else document["spec"]["jobTemplate"]["spec"]["template"]
        template["spec"]["serviceAccountName"] = name
        template["metadata"].setdefault("labels", {})["app.kubernetes.io/name"] = name
    actuator["spec"]["replicas"] = 0
    actuator["spec"]["selector"] = {"matchLabels": {"app.kubernetes.io/name": target + "-actuator"}}
    env(actuator)["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"] = target
    env(actuator)["LOOM_EXECUTION_ACTUATOR_NAMESPACE"]["value"] = "loom-retained-remote"
    collector["spec"]["suspend"] = True
    return DormantPoolConsumer(participant_id=participant.participant_id, actuator=actuator, collector=collector)


def test_dormant_pair_is_drained_without_becoming_an_active_participant(retirement_inputs, tmp_path):
    consumer = dormant_consumer(retirement_inputs)
    request = replace(retirement_inputs, dormant_consumers=(consumer,))
    api = initialize(request, tmp_path)
    key = "Deployment:loom-nebius-exec-0:nebius-retained-remote-actuator"
    cron_key = "CronJob:loom-nebius-exec-0:nebius-retained-remote-collector"
    api.busy.add(cron_key)
    assert retire(request, api, tmp_path)["status"] == "pending_drain"
    api.busy.clear()
    assert retire(request, api, tmp_path)["status"] == "old_pool_workloads_retired"
    assert api.documents[key]["spec"]["replicas"] == 0
    assert api.documents[cron_key]["spec"]["suspend"] is True
    assert api.documents[key]["spec"]["template"] == consumer.actuator["spec"]["template"]
    assert api.documents[cron_key]["spec"]["jobTemplate"] == consumer.collector["spec"]["jobTemplate"]
    assert len(api.patches) == 11
    assert len(request.migration.registration.spec.participants) == 3
    assert all(target.target_id != "nebius-retained-remote"
        for participant in request.migration.registration.spec.participants for target in participant.targets)
    assert retire(request, api, tmp_path)["status"] == "old_pool_workloads_retired"
    assert len(api.patches) == 11
    api.documents[key]["spec"]["replicas"] = 1
    with pytest.raises(ValueError):
        retire(request, api, tmp_path)
    assert len(api.patches) == 11


def test_dormant_identities_are_in_the_effective_permission_review(fencing_inputs):
    from scripts.ops.nebius_pool_role_fencing import role_fence_review_scope

    consumer = dormant_consumer(fencing_inputs.retirement)
    request = replace(fencing_inputs, retirement=replace(fencing_inputs.retirement, dormant_consumers=(consumer,)))
    subjects, namespaces = role_fence_review_scope(request)
    assert ("loom-nebius-exec-0", "nebius-retained-remote-actuator") in subjects
    assert ("loom-nebius-exec-0", "nebius-retained-remote-collector") in subjects
    assert "loom-retained-remote" not in namespaces


@pytest.mark.parametrize("damage", ["running", "boolean_replicas", "unsuspended", "db", "namespace", "account",
    "same_namespace", "participant", "uid", "terminating", "duplicate"])
def test_dormant_consumer_cannot_widen_retirement_authority(retirement_inputs, damage):
    from scripts.ops.nebius_pool_retirement import retirement_documents

    consumer = dormant_consumer(retirement_inputs)
    if damage == "running":
        consumer.actuator["spec"]["replicas"] = 1
    elif damage == "boolean_replicas":
        consumer.actuator["spec"]["replicas"] = False
    elif damage == "unsuspended":
        consumer.collector["spec"]["suspend"] = False
    elif damage == "db":
        env(consumer.actuator)["LOOM_EXECUTION_ACTUATOR_DB_URL"]["valueFrom"]["secretKeyRef"]["name"] = "other-db"
    elif damage == "namespace":
        consumer.collector["metadata"]["namespace"] = "foreign-namespace"
    elif damage == "account":
        consumer.actuator["spec"]["template"]["spec"]["serviceAccountName"] = "loom-execution-actuator"
    elif damage == "same_namespace":
        env(consumer.actuator)["LOOM_EXECUTION_ACTUATOR_NAMESPACE"]["value"] = "loom-nebius-exec-0"
    elif damage == "participant":
        consumer = replace(consumer, participant_id=UUID(int=0))
    elif damage == "uid":
        consumer.collector["metadata"]["uid"] = consumer.actuator["metadata"]["uid"]
    elif damage == "terminating":
        consumer.actuator["metadata"]["deletionTimestamp"] = "2026-10-02T00:00:00Z"
    consumers = (consumer, consumer) if damage == "duplicate" else (consumer,)
    with pytest.raises(ValueError):
        retirement_documents(replace(retirement_inputs, dormant_consumers=consumers))


def test_registered_target_cannot_be_reclassified_as_a_dormant_remote(retirement_inputs):
    from scripts.ops.nebius_pool_retirement import retirement_documents

    consumer = dormant_consumer(retirement_inputs,
        target=retirement_inputs.migration.registration.spec.participants[0].targets[0].target_id)
    with pytest.raises(ValueError):
        retirement_documents(replace(retirement_inputs, dormant_consumers=(consumer,)))
