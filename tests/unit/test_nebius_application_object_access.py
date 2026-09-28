"""Only explicit key rejection at the frozen object endpoint proves revocation."""
from __future__ import annotations

import httpx
import pytest

from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError


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
