import json
import re
import ssl
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import httpx
import pytest
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import ReadTimeoutError, SSLError

from loom_execution_actuator.contracts import (
    KubernetesApiError,
    KubernetesJobObservation,
    NormalizedJobState,
)
from loom_execution_actuator.controller import ExecutionActuator
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.resource_diagnostics import resource_sample_diagnostic


async def test_collect_failure_preserves_safe_diagnostic_through_node_cache(caplog):
    reads = 0
    secret = "private-endpoint-and-bearer-must-not-appear"

    async def resource_summary(*, node_name):
        nonlocal reads
        reads += 1
        if reads > 1:
            return {
                "pods": [
                    {
                        "podRef": {"uid": "pod-1", "namespace": "execution"},
                        "containers": [
                            {"name": "execution", "cpu": {"usageCoreNanoSeconds": 10000}}
                        ],
                    }
                ]
            }
        try:
            raise httpx.ConnectTimeout(secret)
        except httpx.ConnectTimeout as error:
            raise KubernetesApiError("Kubernetes resource_summary failed") from error

    actuator = ExecutionActuator(
        sessions=Mock(),
        kubernetes=SimpleNamespace(resource_summary=resource_summary),
        target=ExecutionTargetRuntime(target_id="target", namespace="execution"),
        controller_id="test",
    )
    observation = KubernetesJobObservation(
        namespace="execution",
        job_name="job",
        lease_id=str(uuid4()),
        resource_generation=1,
        target_id="target",
        execution_unit_key=str(uuid4()),
        normalized_state=NormalizedJobState.RUNNING,
        pod_uid="pod-1",
        node_name="node-1",
    )
    now = datetime.now(UTC)
    assert await actuator._usage_samples(observation, now=now) == ({}, "kubelet_connect_timeout")
    assert await actuator._usage_samples(observation, now=now + timedelta(seconds=1)) == (
        {},
        "kubelet_connect_timeout",
    )
    assert reads == 1
    assert "kubelet_connect_timeout" in caplog.text
    assert secret not in caplog.text

    samples, diagnostic = await actuator._usage_samples(
        observation, now=now + timedelta(seconds=16)
    )
    assert reads == 2
    assert diagnostic is None
    assert samples["execution"].counters.cpu_usage_usec == 10
    assert await actuator._usage_samples(observation, now=now + timedelta(seconds=17)) == (
        samples,
        None,
    )
    assert reads == 2


def _wrapped(error):
    wrapped = KubernetesApiError("Kubernetes resource_summary failed")
    wrapped.__cause__ = error
    return wrapped


def _http_error(status):
    request = httpx.Request("GET", "https://private.example.test/stats/summary")
    return httpx.HTTPStatusError(
        "private endpoint and authorization",
        request=request,
        response=httpx.Response(status, request=request),
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ConnectTimeout("private details"), "kubelet_connect_timeout"),
        (httpx.ReadTimeout("private details"), "kubelet_read_timeout"),
        (httpx.ConnectError("private details"), "kubelet_network"),
        (SSLError("private details"), "tls_api"),
        (ReadTimeoutError(None, "private endpoint", "private details"), "api_network"),
        (_http_error(401), "kubelet_unauthenticated"),
        (_http_error(403), "kubelet_forbidden"),
        (_http_error(404), "kubelet_not_found"),
        (_http_error(500), "kubelet_http"),
        (ApiException(status=401, reason="private details"), "api_unauthenticated"),
        (ApiException(status=403, reason="private details"), "api_forbidden"),
        (ApiException(status=404, reason="private details"), "api_not_found"),
        (
            json.JSONDecodeError("private details", "private response", 0),
            "kubelet_sample_malformed",
        ),
        (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid"), "kubelet_sample_malformed"),
        (httpx.DecodingError("private payload"), "kubelet_sample_malformed"),
        (FileNotFoundError("private CA path"), "kubelet_authority"),
        (ValueError("kubelet summary node identity differs"), "kubelet_sample_malformed"),
        (ValueError("unqualified kubelet node identity"), "kubelet_node_identity"),
        (ValueError("unqualified kubelet address"), "kubelet_node_address"),
        (RuntimeError("private-endpoint-and-bearer"), "kubelet_sample_unavailable"),
    ],
)
def test_collect_diagnostic_distinguishes_known_failures_without_private_details(error, expected):
    result = resource_sample_diagnostic(_wrapped(error))
    assert result == expected
    assert re.fullmatch(r"[a-z0-9_.:-]{1,80}", result)
    assert "private" not in result


@pytest.mark.parametrize("transport", ["api", "kubelet"])
@pytest.mark.parametrize("verify_code", [0, 10, 19, 20, 64, 255])
def test_collect_diagnostic_retains_tls_transport_and_verification_code(transport, verify_code):
    certificate = ssl.SSLCertVerificationError(1, "private certificate details")
    certificate.verify_code = verify_code
    if transport == "api":
        network = SSLError("private API endpoint")
    else:
        network = httpx.ConnectError("private kubelet endpoint")
    network.__cause__ = certificate
    assert resource_sample_diagnostic(_wrapped(network)) == f"tls_{transport}_verify_{verify_code}"


@pytest.mark.parametrize("verify_code", [None, True, -1, 256, "20"])
def test_collect_diagnostic_does_not_emit_unbounded_verification_details(verify_code):
    certificate = ssl.SSLCertVerificationError(1, "private certificate details")
    certificate.verify_code = verify_code
    network = httpx.ConnectError("private kubelet endpoint")
    network.__cause__ = certificate
    assert resource_sample_diagnostic(_wrapped(network)) == "tls_kubelet"


@pytest.mark.parametrize("damage", ["cycle", "depth", "mixed_transport"])
def test_collect_diagnostic_unknown_exception_chains_remain_unavailable(damage):
    error = _wrapped(httpx.ConnectError("private details"))
    if damage == "cycle":
        error.__cause__.__cause__ = error
    elif damage == "depth":
        for _ in range(9):
            error = _wrapped(error)
    else:
        error.__cause__.__cause__ = ApiException(status=403, reason="private details")
    assert resource_sample_diagnostic(error) == "kubelet_sample_unavailable"
