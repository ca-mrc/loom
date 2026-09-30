"""Scoped object observations compose with mandatory exact IAM retirement."""
from __future__ import annotations

import copy
import gzip
import json

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from tests.unit.test_nebius_application_render import inputs
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def scoped_verifier(http, protected_scope):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    foundation, shared, storage, _ = protected_scope
    return ApplicationObjectAccessVerifier(http, foundation=foundation, shared=shared, storage=storage)


@pytest.mark.parametrize("code,status", [("InvalidAccessKeyId", 403), ("AccessDenied", 403),
    ("SignatureDoesNotMatch", 403), ("InvalidAccessKeyId", 500), ("NoSuchBucket", 404), ("", 200)])
async def test_only_structured_denial_qualifies_scoped_retirement(protected_scope, code, status):
    requests = []

    def response(request):
        requests.append(request)
        return httpx.Response(status, text=f"<Error><Code>{code}</Code></Error>")

    foundation, _, _, frozen = protected_scope
    accepted = status == 403 and code in {"AccessDenied", "InvalidAccessKeyId"}
    async with httpx.AsyncClient(base_url=foundation.platform_config["storage_endpoint"], transport=httpx.MockTransport(response)) as http:
        verifier = scoped_verifier(http, protected_scope)
        if accepted:
            await verifier.verify_retired(frozen, {"access-key": "retired-access", "secret-key": "private-key"})
        else:
            with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
                await verifier.verify_retired(frozen, {"access-key": "retired-access", "secret-key": "private-key"})
    assert len(requests) == (3 if accepted else 1)
    request = requests[0]
    assert request.method == "GET" and request.url.path == "/probe-artifacts"
    assert request.url.params["list-type"] == "2" and request.url.params["max-keys"] == "1"
    assert "Credential=retired-access/" in request.headers["Authorization"]
    assert "/eu-north1/s3/aws4_request" in request.headers["Authorization"]
    assert "private-key" not in str(request.headers)


async def test_wrong_protected_origin_never_receives_retired_credentials(protected_scope):
    requests = []
    async with httpx.AsyncClient(base_url="https://foreign.test",
                                transport=httpx.MockTransport(lambda request: requests.append(request))) as http:
        with pytest.raises(ProviderBlockedError, match="application_object_access_binding_conflict"):
            await scoped_verifier(http, protected_scope).verify_retired(
                protected_scope[3], {"access-key": "retired-access", "secret-key": "private-key"})
    assert requests == []


@pytest.mark.parametrize("failure", ["timeout", "redirect", "malformed", "encoding", "multibyte-encoding"])
async def test_unknown_response_never_proves_revocation_or_retries(protected_scope, failure):
    requests = []

    def response(request):
        requests.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("private-key must not escape")
        if failure == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.test/"})
        if failure == "encoding":
            return httpx.Response(403, text='<?xml version="1.0" encoding="invalid-encoding"?><Error/>')
        if failure == "multibyte-encoding":
            return httpx.Response(403, text='<?xml version="1.0" encoding="UTF-32"?><Error/>')
        return httpx.Response(403, text="not XML")

    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"], transport=httpx.MockTransport(response)) as http:
        with pytest.raises(ProviderWaitingError) as error:
            await scoped_verifier(http, protected_scope).verify_retired(
                protected_scope[3], {"access-key": "retired-access", "secret-key": "private-key"})
    assert len(requests) == 1 and "private-key" not in str(error.value)


async def test_client_auth_cannot_replace_the_original_key_probe(protected_scope):
    class ForeignAuth(httpx.Auth):
        def auth_flow(self, request):
            request.headers["Authorization"] = "foreign missing key"
            yield request

    def response(request):
        if "Credential=retired-access/" in request.headers["Authorization"]:
            return httpx.Response(200, text="<ListBucketResult/>")  # Original key still works.
        return httpx.Response(403, text="<Error><Code>InvalidAccessKeyId</Code></Error>")

    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"], auth=ForeignAuth(),
                                transport=httpx.MockTransport(response)) as http:
        with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
            await scoped_verifier(http, protected_scope).verify_retired(
                protected_scope[3], {"access-key": "retired-access", "secret-key": "private-key"})


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


@pytest.mark.parametrize("bad_bucket", ["probe-artifacts", "probe-trajectories", "probe-source"])
@pytest.mark.parametrize("status,body", [(403, "<Error><Code>AccessDenied</Code></Error>"),
    (200, "<html>ok</html>"), (200, "<ListBucketResult><Name>foreign</Name></ListBucketResult>")])
async def test_invalid_positive_probe_never_qualifies_active_access(protected_scope, bad_bucket, status, body):
    from loom_service.application_management.object_access import ApplicationObjectAccessVerifier

    foundation, shared, storage, frozen = protected_scope

    def respond(request):
        if request.url.path == "/" + bad_bucket:
            return httpx.Response(status, text=body)
        return httpx.Response(200, text=(f"<ListBucketResult><Name>{request.url.path[1:]}</Name>"
            "<Prefix>loom-application-access-probe/</Prefix><MaxKeys>1</MaxKeys>"
            "<KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"))

    async with httpx.AsyncClient(base_url=foundation.platform_config["storage_endpoint"],
            transport=httpx.MockTransport(respond)) as http:
        verifier = ApplicationObjectAccessVerifier(http, foundation=foundation, shared=shared, storage=storage)
        with pytest.raises(ProviderWaitingError, match="application_object_access_not_ready"):
            await verifier.verify_active(frozen, {"access-key": "active-key", "secret-key": "active-secret"})


@pytest.mark.parametrize("fields", [
    "", "<KeyCount>0</KeyCount><IsTruncated>invalid</IsTruncated>",
    "<KeyCount>2</KeyCount><IsTruncated>false</IsTruncated>",
    "<KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>",
    "<KeyCount>0</KeyCount><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>",
    "<KeyCount>1</KeyCount><IsTruncated>false</IsTruncated><Contents><Key>foreign/key</Key></Contents>",
    "<KeyCount>0</KeyCount><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>foreign/</Prefix></CommonPrefixes>",
])
async def test_contradictory_list_result_never_qualifies_positive_access(protected_scope, fields):
    def respond(request):
        return httpx.Response(200, text=(f"<ListBucketResult><Name>{request.url.path[1:]}</Name>"
            f"<Prefix>loom-application-access-probe/</Prefix><MaxKeys>1</MaxKeys>{fields}</ListBucketResult>"))

    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"],
            transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(ProviderWaitingError, match="application_object_access_not_ready"):
            await scoped_verifier(http, protected_scope).verify_active(protected_scope[3],
                {"access-key": "active-key", "secret-key": "active-secret"})


@pytest.mark.parametrize("truncated", ["true", "false"])
async def test_valid_nonempty_prefix_probe_qualifies_positive_access(protected_scope, truncated):
    def respond(request):
        return httpx.Response(200, text=(f"<ListBucketResult><Name>{request.url.path[1:]}</Name>"
            "<Prefix>loom-application-access-probe/</Prefix><MaxKeys>1</MaxKeys><KeyCount>1</KeyCount>"
            f"<IsTruncated>{truncated}</IsTruncated><Contents><Key>loom-application-access-probe/file</Key>"
            "<Size>0</Size></Contents></ListBucketResult>"))

    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"],
            transport=httpx.MockTransport(respond)) as http:
        await scoped_verifier(http, protected_scope).verify_active(protected_scope[3],
            {"access-key": "active-key", "secret-key": "active-secret"})


@pytest.mark.parametrize("response_kind", ["compressed", "duplicate-code", "nested-code"])
async def test_ambiguous_or_encoded_denial_is_not_retirement_evidence(protected_scope, response_kind):
    def respond(request):
        if response_kind == "compressed":
            return httpx.Response(403, content=gzip.compress(b"<Error><Code>AccessDenied</Code></Error>"),
                headers={"Content-Encoding": "gzip"})
        body = ("<Error><Code>AccessDenied</Code><Code>SignatureDoesNotMatch</Code></Error>"
                if response_kind == "duplicate-code" else "<Error><Code>AccessDenied<Unexpected/></Code></Error>")
        return httpx.Response(403, text=body)

    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"], transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(ProviderWaitingError, match="application_object_access_retirement_pending"):
            await scoped_verifier(http, protected_scope).verify_retired(protected_scope[3],
                {"access-key": "original-key", "secret-key": "original-secret"})


@pytest.mark.parametrize("damage", ["row-data", "shared-data", "row-cluster", "shared-namespace",
    "endpoint", "region", "artifacts", "trajectories", "duplicate-env"])
async def test_scope_mismatch_fails_before_sending_any_original_credential(protected_scope, damage):
    from uuid import uuid4

    frozen = copy.deepcopy(protected_scope[3])
    if damage in {"row-data", "shared-data"}:
        frozen["registration" if damage == "row-data" else "shared"]["data_environment_id"] = str(uuid4())
    elif damage == "row-cluster":
        frozen["registration"]["cluster_id"] = "foreign-cluster"
    elif damage == "shared-namespace":
        frozen["shared"]["platform_namespace"] = "foreign-namespace"
    else:
        deployment = next(doc for docs in frozen["files"].values() for doc in docs
            if doc["kind"] == "Deployment" and doc["metadata"]["name"] == "loom-service")
        entries = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        if damage == "duplicate-env":
            entries.append(copy.deepcopy(entries[0]))
        else:
            name, value = {"endpoint": ("LOOM_SVC_MINIO_ENDPOINT", "https://foreign.example.com"),
                "region": ("LOOM_SVC_MINIO_REGION", "other-region"), "artifacts": ("LOOM_SVC_ARTIFACTS_BUCKET", "foreign-bucket"),
                "trajectories": ("LOOM_SVC_TRAJECTORIES_BUCKET", "foreign-bucket")}[damage]
            next(item for item in entries if item["name"] == name)["value"] = value
    requests = []
    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"],
            transport=httpx.MockTransport(lambda request: requests.append(request))) as http:
        with pytest.raises(ProviderBlockedError, match="application_object_access_binding_conflict"):
            await scoped_verifier(http, protected_scope).verify_retired(frozen,
                {"access-key": "original-key", "secret-key": "original-secret"})
    assert requests == []


@pytest.mark.parametrize("active", [False, True])
async def test_oversized_response_stops_reading_and_never_qualifies_access(protected_scope, active):
    class OversizedStream(httpx.AsyncByteStream):
        def __init__(self):
            self.reads = 0
            self.closed = False

        async def __aiter__(self):
            for _ in range(4):
                self.reads += 1
                yield b" " * 16384

        async def aclose(self):
            self.closed = True

    stream = OversizedStream()
    async with httpx.AsyncClient(base_url=protected_scope[0].platform_config["storage_endpoint"],
            transport=httpx.MockTransport(lambda request: httpx.Response(200 if active else 403, stream=stream))) as http:
        verifier = scoped_verifier(http, protected_scope)
        verify = verifier.verify_active if active else verifier.verify_retired
        with pytest.raises(ProviderWaitingError):
            await verify(protected_scope[3], {"access-key": "original-key", "secret-key": "original-secret"})
    assert stream.reads == 2 and stream.closed
