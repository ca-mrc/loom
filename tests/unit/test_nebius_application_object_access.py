"""Only explicit key rejection at the frozen object endpoint proves revocation."""
from __future__ import annotations

import copy
import json

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def plan():
    return {"files": {"application": [{"kind": "Deployment", "metadata": {"name": "loom-service"},
        "spec": {"template": {"spec": {"containers": [{"env": [
            {"name": "LOOM_SVC_MINIO_ENDPOINT", "value": "https://storage.test"},
            {"name": "LOOM_SVC_MINIO_REGION", "value": "eu-north1"},
            {"name": "LOOM_SVC_ARTIFACTS_BUCKET", "value": "shared-data"},
        ]}]}}}}]}}


@pytest.mark.parametrize("code,status", [("InvalidAccessKeyId", 403), ("AccessDenied", 403),
    ("SignatureDoesNotMatch", 403), ("InvalidAccessKeyId", 500), ("NoSuchBucket", 404), ("", 200)])
async def test_only_explicit_invalid_key_denial_confirms_retirement(code, status):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    requests = []

    def response(request):
        requests.append(request)
        return httpx.Response(status, text=f"<Error><Code>{code}</Code></Error>")

    async with httpx.AsyncClient(base_url="https://storage.test", transport=httpx.MockTransport(response)) as http:
        verifier = ApplicationObjectAccessVerifier(http)
        if (code, status) == ("InvalidAccessKeyId", 403):
            await verifier.verify_retired(plan(), {"access-key": "retired-access", "secret-key": "private-key"})
        else:
            with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
                await verifier.verify_retired(plan(), {"access-key": "retired-access", "secret-key": "private-key"})
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET" and request.url.path == "/shared-data"
    assert request.url.params["list-type"] == "2" and request.url.params["max-keys"] == "1"
    assert "Credential=retired-access/" in request.headers["Authorization"]
    assert "/eu-north1/s3/aws4_request" in request.headers["Authorization"]
    assert "private-key" not in str(request.headers)


async def test_wrong_protected_origin_never_receives_retired_credentials():
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    requests = []
    async with httpx.AsyncClient(base_url="https://foreign.test",
                                transport=httpx.MockTransport(lambda request: requests.append(request))) as http:
        with pytest.raises(ProviderBlockedError, match="application_object_access_binding_conflict"):
            await ApplicationObjectAccessVerifier(http).verify_retired(
                plan(), {"access-key": "retired-access", "secret-key": "private-key"})
    assert requests == []


@pytest.mark.parametrize("failure", ["timeout", "redirect", "malformed", "encoding"])
async def test_unknown_response_never_proves_revocation_or_retries(failure):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    requests = []

    def response(request):
        requests.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("private-key must not escape")
        if failure == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.test/"})
        if failure == "encoding":
            return httpx.Response(403, text='<?xml version="1.0" encoding="invalid-encoding"?><Error/>')
        return httpx.Response(403, text="not XML")

    async with httpx.AsyncClient(base_url="https://storage.test", transport=httpx.MockTransport(response)) as http:
        with pytest.raises(ProviderWaitingError) as error:
            await ApplicationObjectAccessVerifier(http).verify_retired(
                plan(), {"access-key": "retired-access", "secret-key": "private-key"})
    assert len(requests) == 1 and "private-key" not in str(error.value)


async def test_client_auth_cannot_replace_the_original_key_probe():
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    class ForeignAuth(httpx.Auth):
        def auth_flow(self, request):
            request.headers["Authorization"] = "foreign missing key"
            yield request

    def response(request):
        if "Credential=retired-access/" in request.headers["Authorization"]:
            return httpx.Response(200, text="<ListBucketResult/>")  # Original key still works.
        return httpx.Response(403, text="<Error><Code>InvalidAccessKeyId</Code></Error>")

    async with httpx.AsyncClient(base_url="https://storage.test", auth=ForeignAuth(),
                                transport=httpx.MockTransport(response)) as http:
        with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
            await ApplicationObjectAccessVerifier(http).verify_retired(
                plan(), {"access-key": "retired-access", "secret-key": "private-key"})


@pytest.fixture
def protected_scope(platform_inputs):
    from loom.nebius_application_render import render_application
    from loom_service.application_management.cloud_effects import ApplicationStorageAccessV1
    from loom_service.application_management.plans import freeze_plan

    row, release, shared, foundation = inputs(platform_inputs)
    config = copy.deepcopy(foundation.platform_config)
    config["buckets"].update(artifacts="probe-artifacts", trajectories="probe-trajectories", source="probe-source")
    foundation = foundation_from(config)
    frozen = freeze_plan(render_application(row, release, shared, foundation), release, shared)
    storage = ApplicationStorageAccessV1(data_environment_id=shared.data_environment_id,
        project_id="application-project", data_group_id="data-group", source_group_id="source-group")
    return foundation, shared, storage, frozen


@pytest.mark.parametrize("denial", ["AccessDenied", "InvalidAccessKeyId"])
async def test_scoped_denial_probes_all_original_business_and_source_buckets(protected_scope, denial):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    foundation, shared, storage, frozen = protected_scope
    before = json.dumps(frozen, sort_keys=True)
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(403, text=f"<Error><Code>{denial}</Code></Error>")

    async with httpx.AsyncClient(base_url=foundation.platform_config["storage_endpoint"], transport=httpx.MockTransport(respond)) as http:
        verifier = ApplicationObjectAccessVerifier(http, foundation=foundation, shared=shared, storage=storage)
        await verifier.verify_retired(frozen, {"access-key": "original-key", "secret-key": "original-secret"})
    assert {request.url.path for request in requests} == {"/probe-artifacts", "/probe-trajectories", "/probe-source"}
    assert len(requests) == 3
    assert all(request.method == "GET" and request.url.params["max-keys"] == "1" for request in requests)
    assert all("Credential=original-key/" in request.headers["Authorization"] for request in requests)
    assert json.dumps(frozen, sort_keys=True) == before  # No invented legacy plan fields.


@pytest.mark.parametrize("bad_bucket", ["probe-artifacts", "probe-trajectories", "probe-source"])
@pytest.mark.parametrize("status,body", [(200, "<ListBucketResult/>"), (500, "<Error><Code>AccessDenied</Code></Error>"),
    (403, "<html>denied</html>"), (403, "<Error><Code>SignatureDoesNotMatch</Code></Error>")])
async def test_one_unqualified_scope_prevents_retirement(protected_scope, bad_bucket, status, body):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    foundation, shared, storage, frozen = protected_scope

    def respond(request):
        return (httpx.Response(status, text=body) if request.url.path == "/" + bad_bucket
                else httpx.Response(403, text="<Error><Code>AccessDenied</Code></Error>"))

    async with httpx.AsyncClient(base_url=foundation.platform_config["storage_endpoint"], transport=httpx.MockTransport(respond)) as http:
        verifier = ApplicationObjectAccessVerifier(http, foundation=foundation, shared=shared, storage=storage)
        with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
            await verifier.verify_retired(frozen, {"access-key": "original-key", "secret-key": "original-secret"})


async def test_active_key_qualifies_identical_read_only_probes_in_every_scope(protected_scope):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    foundation, shared, storage, frozen = protected_scope
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, text=("<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
            f"<Name>{request.url.path[1:]}</Name><Prefix>loom-application-access-probe/</Prefix>"
            "<KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>"))

    async with httpx.AsyncClient(base_url=foundation.platform_config["storage_endpoint"], transport=httpx.MockTransport(respond)) as http:
        verifier = ApplicationObjectAccessVerifier(http, foundation=foundation, shared=shared, storage=storage)
        await verifier.verify_active(frozen, {"access-key": "active-key", "secret-key": "active-secret"})
    assert {request.url.path for request in requests} == {"/probe-artifacts", "/probe-trajectories", "/probe-source"}
    assert len(requests) == 3 and all(request.method == "GET" for request in requests)
    assert all("Credential=active-key/" in request.headers["Authorization"] for request in requests)


@pytest.mark.parametrize("status,body", [(403, "<Error><Code>AccessDenied</Code></Error>"),
    (200, "<html>ok</html>"), (200, "<ListBucketResult><Name>foreign</Name></ListBucketResult>")])
async def test_invalid_positive_probe_never_qualifies_active_access(protected_scope, status, body):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    foundation, shared, storage, frozen = protected_scope
    async with httpx.AsyncClient(base_url=foundation.platform_config["storage_endpoint"],
            transport=httpx.MockTransport(lambda request: httpx.Response(status, text=body))) as http:
        verifier = ApplicationObjectAccessVerifier(http, foundation=foundation, shared=shared, storage=storage)
        with pytest.raises(ProviderWaitingError, match="application_object_access_not_ready"):
            await verifier.verify_active(frozen, {"access-key": "active-key", "secret-key": "active-secret"})
