"""A protected retirement may claim only its exact pre-execution legacy intent."""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from loom.db.nebius_environment_schema import (
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
    NebiusPlatformReservation,
)
from loom_service.environment_management.registry import ManagementError
from tests.integration.test_nebius_environment_management import (
    environment_registry as environment_registry,
)
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


async def prepare_retirement(environment_registry, kubernetes=None):
    registry, _, (alice, _), prepare = environment_registry
    created = await registry.create(principal=alice, idempotency_key="legacy", prepared=prepare())
    lease = await registry.claim(created.operation_id)
    namespaces = {}
    for _ in range(3):
        step = await registry.next_step(lease)
        assert step.payload["kind"] == "Namespace"
        uid = (str(uuid4()) if kubernetes is None else
            await kubernetes.apply(await registry.provisioning_context(lease), step))
        namespaces[step.payload["metadata"]["name"]] = uid
        await registry.confirm_step(lease, step.key, provider_identity=uid)
    await registry.finish_attempt(lease, error_code="nebius_operation_failed", retry=False)
    operation = await registry.destroy_retained(created.environment_id, principal=alice,
        expected_generation=1, idempotency_key="retire")
    status = await registry.status(created.environment_id, principal=alice)
    return {
        "operation_id": str(operation.operation_id), "source_operation_id": str(created.operation_id),
        "registration": status.registration.model_dump(mode="json"), "namespace_uids": namespaces,
    }


@pytest.mark.parametrize("change", ["action", "owner", "incarnation", "generation", "source", "namespace_uid", "material"])
async def test_wrong_retirement_target_never_claims_or_releases(environment_registry, change):
    from loom_service.environment_management import retirement

    registry, factory, (alice, _), _ = environment_registry
    raw = await prepare_retirement(environment_registry)
    selected_id = raw["operation_id"]
    if change == "action":
        raw["operation_id"] = raw["source_operation_id"]
    elif change == "owner":
        raw["registration"]["owner_user_id"] = str(uuid4())
    elif change == "incarnation":
        incarnation = uuid4()
        for key, name in (("execution_namespace", "loom-run-" + incarnation.hex),
                          ("build_namespace", "loom-run-" + incarnation.hex + "-build")):
            raw["namespace_uids"][name] = raw["namespace_uids"].pop(raw["registration"][key])
            raw["registration"][key] = name
        raw["registration"].update(incarnation=str(incarnation), target_id="env-" + incarnation.hex)
    elif change == "generation":
        raw["registration"]["deployment_generation"] += 1
    elif change == "source":
        raw["source_operation_id"] = str(uuid4())
    elif change == "namespace_uid":
        raw["namespace_uids"][next(iter(raw["namespace_uids"]))] = str(uuid4())
    else:
        async with factory.begin() as session:
            row = await session.get(NebiusEnvironmentResource, (raw["source_operation_id"], "credentials:material"))
            row.phase, row.provider_identity = "applied", "delivered-material"
    target = retirement.RetirementTarget.model_validate(raw)
    bounded = retirement.RetirementRegistry(factory, target)
    with pytest.raises(ManagementError, match="retirement_target_unqualified"):
        await bounded.claim(target.operation_id)
    async with factory() as session:
        row = await session.get(NebiusEnvironmentOperation, selected_id)
        assert row.runner_epoch == 0 and row.phase == "pending" and row.lease_token is None
    status = await registry.status(target.registration.environment_id, principal=alice)
    assert status.registration.desired_state == "destroyed"


async def test_bound_claim_cannot_consume_another_owners_runnable_create(environment_registry):
    from loom_service.environment_management import retirement

    registry, factory, (_, bob), prepare = environment_registry
    target = retirement.RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    foreign = await registry.create(principal=bob, idempotency_key="foreign", prepared=prepare("bob", bob))
    bounded = retirement.RetirementRegistry(factory, target)
    with pytest.raises(ManagementError, match="retirement_target_unqualified"):
        await bounded.claim(foreign.operation_id)
    lease = await bounded.claim(target.operation_id)
    assert lease is not None and lease.operation_id == target.operation_id
    assert await bounded.claim(target.operation_id) is None
    context = await bounded.provisioning_context(lease)
    assert context.action == "destroy_retained"
    assert context.source.lease.operation_id == target.source_operation_id
    assert (await registry.get_operation(foreign.operation_id, principal=bob)).phase == "pending"
    async with factory() as session:
        row = await session.get(NebiusEnvironmentOperation, foreign.operation_id)
        assert row.runner_epoch == 0


@pytest.mark.parametrize("foreign_namespace", [False, True])
async def test_real_retirement_preserves_storage_and_does_not_run_create_queue(environment_registry, foreign_namespace):
    import copy
    import json

    from loom_service.environment_management import retirement
    from loom_service.environment_management.kubernetes_provider import (
        KubernetesEnvironmentProvider,
    )
    from loom_service.environment_management.provider import ProvisioningContext
    from loom_service.environment_management.registry import OperationLease
    from loom_service.environment_management.steps import ProvisioningStep

    registry, factory, (_, bob), prepare = environment_registry
    target = retirement.RetirementTarget.model_validate(await prepare_retirement(environment_registry))
    foreign = await registry.create(principal=bob, idempotency_key="foreign", prepared=prepare("bob", bob))
    async with factory() as session:
        before = await session.get(NebiusPlatformReservation, target.registration.environment_id)
        storage = before.storage_mib
        source = await session.get(NebiusEnvironmentOperation, target.source_operation_id)
        source_rows = (await session.scalars(select(NebiusEnvironmentResource).where(
            NebiusEnvironmentResource.operation_id == target.source_operation_id,
        ))).all()
        context = ProvisioningContext(OperationLease(source.operation_id, source.environment_id,
            source.deployment_generation, source.runner_epoch, uuid4()), source.plan_json["registration"],
            source.plan_json["config"], {})
        objects = {}
        for row in source_rows:
            if row.kind == "kubernetes" and row.payload_json["kind"] == "Namespace":
                doc = KubernetesEnvironmentProvider._expected(context, ProvisioningStep(row.resource_key, row.kind, row.payload_json))
                doc["metadata"]["uid"] = str(target.namespace_uids[doc["metadata"]["name"]])
                objects["/api/v1/namespaces/" + doc["metadata"]["name"]] = doc
        if foreign_namespace:
            objects[next(iter(objects))]["metadata"]["uid"] = str(uuid4())
    mutations = []

    def api(request):
        path = request.url.path
        if request.method == "GET":
            if path in objects:
                return httpx.Response(200, json=objects[path])
            if path.endswith(("/pods", "/jobs", "/replicasets")):
                return httpx.Response(200, json={"items": [], "metadata": {}})
            return httpx.Response(404)
        assert request.method == "POST", "empty legacy namespace needs only stopped tombstones and zero quota"
        doc = json.loads(request.content)
        assert doc["metadata"]["namespace"] in target.namespace_uids
        assert doc["kind"] not in {"Namespace", "Pod", "Secret", "PersistentVolumeClaim"}
        if doc["kind"] == "ResourceQuota":
            assert doc["spec"] == {"hard": {"pods": "0"}}
            doc["status"] = {"hard": {"pods": "0"}, "used": {"pods": "0"}}
        else:
            quota_path = "/api/v1/namespaces/" + doc["metadata"]["namespace"] + "/resourcequotas/loom-environment-retained"
            assert quota_path in objects, "controller write preceded closed Pod admission"
            if doc["kind"] in {"Deployment", "StatefulSet"}:
                assert doc["spec"]["replicas"] == 0
                doc["status"] = {"observedGeneration": 1, "replicas": 0}
            elif doc["kind"] in {"Job", "CronJob"}:
                assert doc["spec"]["suspend"] is True
        doc["metadata"].update(uid=str(uuid4()), generation=1, resourceVersion="1")
        objects[path + "/" + doc["metadata"]["name"]] = copy.deepcopy(doc)
        mutations.append(doc)
        return httpx.Response(201, json=doc)

    async with httpx.AsyncClient(base_url="https://kubernetes.test", transport=httpx.MockTransport(api)) as http:
        phase = await retirement.reconcile_retirement(factory, target, KubernetesEnvironmentProvider(http))
        if not foreign_namespace:
            frozen = copy.deepcopy(objects)
            assert await retirement.reconcile_retirement(factory, target, KubernetesEnvironmentProvider(http)) == "completed"
            assert objects == frozen
    assert phase == ("blocked" if foreign_namespace else "completed")
    assert not foreign_namespace or not mutations
    async with factory() as session:
        reservation = await session.get(NebiusPlatformReservation, target.registration.environment_id)
        assert reservation.storage_mib == storage
        assert (reservation.cpu_millis == 0) is not foreign_namespace
        assert (reservation.memory_mib == 0) is not foreign_namespace
        assert (reservation.ephemeral_storage_mib == 0) is not foreign_namespace
        untouched = await session.get(NebiusEnvironmentOperation, foreign.operation_id)
        assert untouched.phase == "pending" and untouched.runner_epoch == 0
