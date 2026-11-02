"""New owner controls reuse the fixed launcher without changing legacy inputs."""
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from scripts.ops import nebius_pool_administration_job as jobs

from loom_service.pool_management.administration import PoolAdministrationAuthorizationV2
from loom_service.pool_management.cache_policy import PoolCachePolicyV1
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.routing_policy import PoolRoutingPreferenceV1


def operation(cache):
    installation, pool = uuid4(), uuid4()
    request = (PoolCachePolicyV1(operation_id=uuid4(), installation_id=installation, pool_id=pool,
        profile_id=uuid4(), cache_gc_policy="budget-pressure") if cache else
        PoolRoutingPreferenceV1(operation_id=uuid4(), installation_id=installation, pool_id=pool,
            prefer_incoming_warm_fit=True))
    now = datetime.now(UTC)
    grant = PoolAdministrationAuthorizationV2(schema_version="loom.pool-administration-authorization.v2",
        authorization_id=uuid4(), installation_id=installation, pool_id=pool, authority="platform",
        request_sha256=digest(request.model_dump(mode="json")), issued_at=now, expires_at=now + timedelta(minutes=5))
    runtime = jobs.PoolAdministrationRuntimeV2(binding=jobs.PlatformNamespaceBindingV2(
        installation_id=str(installation), namespace="platform-services", namespace_uid=str(uuid4()), kube_system_uid=str(uuid4())),
        service_image="registry.example/service@sha256:" + "e" * 64, database_secret="management-database",
        database_url_key="admin-url", database_ca_key="ca.crt", node_selector={"role": "control"},
        tolerations=(), region="eu-north1")
    return jobs.PoolAdministrationJobV2(runtime=runtime, request=request, authorization=grant, action="apply")


@pytest.mark.parametrize("cache", [False, True])
def test_only_cache_owner_job_supplies_its_bound_reader_image(cache):
    request = operation(cache)
    documents = jobs.administration_documents(request)
    job, = (value for value in documents.values() if value["kind"] == "Job")
    container, = job["spec"]["template"]["spec"]["containers"]
    environment = {item["name"]: item.get("value") for item in container["env"]}
    if cache:
        assert environment["LOOM_POOL_ADMINISTRATION_SERVICE_IMAGE"] == container["image"] == request.runtime.service_image
    else:
        assert "LOOM_POOL_ADMINISTRATION_SERVICE_IMAGE" not in environment
    config, = (value for value in documents.values() if value["kind"] == "ConfigMap")
    payload = json.loads(config["data"]["request.json"])
    assert "trusted_reader_image" not in payload
    if cache:
        with pytest.raises(ValueError):
            PoolCachePolicyV1.model_validate({**payload, "trusted_reader_image": request.runtime.service_image})
