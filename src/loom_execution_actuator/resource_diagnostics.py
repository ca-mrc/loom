"""Bounded, secret-safe reasons for optional native resource observations."""

from __future__ import annotations

import json
import ssl

import httpx

from loom_execution_actuator.contracts import KubernetesApiError

_UNAVAILABLE = "kubelet_sample_unavailable"
_READER_ERRORS = {
    "unqualified kubelet node": "kubelet_node_identity",
    "unqualified expected kubelet node identity": "kubelet_node_identity",
    "unqualified kubelet node identity": "kubelet_node_identity",
    "unqualified kubelet address": "kubelet_node_address",
    "unqualified kubelet TLS or bearer authority": "kubelet_authority",
    "kubelet summary exceeds its bound": "kubelet_sample_malformed",
    "kubelet summary node identity differs": "kubelet_sample_malformed",
}


def resource_sample_diagnostic(error: Exception) -> str:
    """Classify known failures without retaining messages, URLs or response bodies.

    The reader wraps both the Node API and direct-kubelet HTTP errors. Inspect
    their causes before classifying SSL errors as ordinary connection failures.
    Optional Kubernetes dependencies remain lazy: their exception modules mark
    the API transport without adding an import requirement to the controller.
    """
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = error
    for _ in range(8):
        if current is None:
            break
        if id(current) in seen:
            return _UNAVAILABLE
        chain.append(current)
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    if current is not None:
        return _UNAVAILABLE

    transports: set[str] = set()
    for item in chain:
        if isinstance(item, httpx.HTTPError):
            transports.add("kubelet")
        elif any(
            base.__module__.startswith(("kubernetes.", "urllib3.")) for base in type(item).__mro__
        ):
            transports.add("api")
    if len(transports) > 1:
        return _UNAVAILABLE
    transport = next(iter(transports), "unknown")

    for item in chain:
        if isinstance(item, ssl.SSLCertVerificationError):
            code = getattr(item, "verify_code", None)
            if type(code) is int and 0 <= code <= 255:
                return f"tls_{transport}_verify_{code}"
    if any(isinstance(item, ssl.SSLError) or _urllib3_type(item, "SSLError") for item in chain):
        return f"tls_{transport}"

    for item in chain:
        status = (
            item.response.status_code
            if isinstance(item, httpx.HTTPStatusError)
            else getattr(item, "status", None)
        )
        if isinstance(item, KubernetesApiError) and status is None:
            status = item.status_code
        if type(status) is int and status > 0:
            source = "api" if transport == "unknown" else transport
            suffix = {401: "unauthenticated", 403: "forbidden", 404: "not_found"}.get(
                status, "http"
            )
            return f"{source}_{suffix}"
        if isinstance(item, (json.JSONDecodeError, UnicodeError, httpx.DecodingError)):
            return "kubelet_sample_malformed"
        if isinstance(item, (FileNotFoundError, PermissionError)):
            return "kubelet_authority"
        if isinstance(item, ValueError):
            known = _READER_ERRORS.get(str(item))
            if known is not None:
                return known

    if any(isinstance(item, httpx.ConnectTimeout) for item in chain):
        return "kubelet_connect_timeout"
    if any(isinstance(item, httpx.ReadTimeout) for item in chain):
        return "kubelet_read_timeout"
    if any(isinstance(item, httpx.TimeoutException) for item in chain):
        return "kubelet_timeout"
    if any(
        isinstance(item, (httpx.NetworkError, TimeoutError, OSError))
        or _urllib3_type(item, "TimeoutError")
        for item in chain
    ):
        return f"{transport}_network" if transport != "unknown" else _UNAVAILABLE
    return _UNAVAILABLE


def _urllib3_type(error: BaseException, name: str) -> bool:
    return any(
        base.__module__ == "urllib3.exceptions" and base.__name__ == name
        for base in type(error).__mro__
    )
