"""Native generated SDK methods retain exact IDs and disabled write retries."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from nebius.api.nebius.iam import v1, v2

from loom_service.environment_management.nebius_api import NebiusSdkEnvironmentApi


@pytest.mark.parametrize("kind,resource", [
    ("service_account", v1.ServiceAccount), ("membership", v1.GroupMembership), ("access_key", v2.AccessKey),
])
async def test_exact_cloud_read_delete_use_native_requests_without_retries(kind, resource):
    calls = []

    class Operation:
        async def wait(self, **kwargs):
            assert kwargs == {"timeout": 30, "poll_retries": 0}

        def successful(self):
            return True

    class Client:
        async def get(self, request, **kwargs):
            calls.append(("get", json.loads(request.to_json(preserving_proto_field_name=True)), kwargs))
            return resource.from_json(json.dumps({"metadata": {"id": "recorded-id"}}))

        async def delete(self, request, **kwargs):
            calls.append(("delete", json.loads(request.to_json(preserving_proto_field_name=True)), kwargs))
            return Operation()

    api = NebiusSdkEnvironmentApi(SimpleNamespace(), clients={kind: Client()})
    assert (await api.get_resource(kind, "recorded-id"))["metadata"]["id"] == "recorded-id"
    await api.delete_resource(kind, "recorded-id", idempotency_key="frozen-delete-key")
    assert calls == [
        ("get", {"id": "recorded-id"}, {"timeout": 30, "auth_timeout": 30, "retries": 0,
                                       "auth_options": {"max_fetch_token_retries": "0"}}),
        ("delete", {"id": "recorded-id"}, {"timeout": 30, "auth_timeout": 30, "retries": 0,
                                           "auth_options": {"max_fetch_token_retries": "0"},
                                           "metadata": [("x-idempotency-key", "frozen-delete-key")]}),
    ]


@pytest.mark.parametrize("kind", ["bucket", "group", "foreign"])
async def test_application_exact_mutation_primitive_excludes_shared_resources(kind):
    api = NebiusSdkEnvironmentApi(SimpleNamespace(), clients={})
    with pytest.raises(ValueError, match="unsupported application IAM kind"):
        await api.get_resource(kind, "not-owned")
    with pytest.raises(ValueError, match="unsupported application IAM kind"):
        await api.delete_resource(kind, "not-owned", idempotency_key="not-authorized")


@pytest.mark.parametrize("action", ["create", "delete"])
async def test_native_sdk_authentication_rejection_does_not_repeat_mutation(action):
    import grpc
    from nebius.aio.token.renewable import Bearer as RenewableBearer
    from nebius.aio.token.static import Bearer as StaticBearer
    from nebius.base.options import INSECURE
    from nebius.base.resolver import Constant
    from nebius.sdk import SDK

    from loom_service.environment_management.provider import ProviderBlockedError

    attempts = []

    async def rejected(request, context):
        attempts.append(request)
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, "controlled token rejection")

    server = grpc.aio.server()
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        "nebius.iam.v1.ServiceAccountService",
        {action.title(): grpc.unary_unary_rpc_method_handler(rejected)},
    ),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    sdk = SDK(credentials=RenewableBearer(StaticBearer("test-only-token")),
              resolver=Constant(f"127.0.0.1:{port}"), options=[(INSECURE, True)],
              user_agent_prefix="loom-auth-retry-test/1.0")
    try:
        api = NebiusSdkEnvironmentApi(sdk)
        with pytest.raises(ProviderBlockedError, match="nebius_request_rejected"):
            if action == "create":
                await api.create("service_account", {"metadata": {
                    "parent_id": "test-project", "name": "test-account"}, "spec": {}},
                    idempotency_key="test-request")
            else:
                await api.delete_resource("service_account", "test-account", idempotency_key="test-request")
        assert len(attempts) == 1
    finally:
        await sdk.close()
        await server.stop(None)
