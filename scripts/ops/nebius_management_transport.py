"""Explicit-trust, bounded, non-retrying HTTPS connection for management installers.

Scope is enforced by each fixed-operation adapter. This connection never loads
ambient credentials, executes kubeconfig plugins or provides an operator CLI.
"""
from __future__ import annotations

import json
import re
import ssl
from typing import Any, Self
from urllib.parse import urlsplit

import httpx


class ManagementNotDispatchedError(ValueError):
    """Trusted adapters may signal only a failure before invoking write transport."""


class ManagementKubernetesTransport:
    error_type: type[RuntimeError] = RuntimeError

    def __init__(self, *, api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        try:
            endpoint = urlsplit(api_server)
            if (endpoint.scheme != "https" or not endpoint.hostname or endpoint.username or endpoint.password
                    or endpoint.path not in {"", "/"} or endpoint.query or endpoint.fragment
                    or any(char.isspace() for char in api_server) or endpoint.port == 0
                    or not isinstance(ssl_context, ssl.SSLContext)
                    or ssl_context.verify_mode != ssl.CERT_REQUIRED or not ssl_context.check_hostname
                    or (token is not None and (not isinstance(token, str) or len(token) > 16384
                                               or re.fullmatch(r"[A-Za-z0-9._~+/-]+={0,2}", token) is None))):
                raise ValueError()
        except Exception:
            raise self.error_type("private management Kubernetes configuration unavailable") from None
        self.api_server = api_server
        self.client = httpx.Client(
            base_url=api_server, headers={"Accept-Encoding": "identity",
                                         **({"Authorization": "Bearer " + token} if token else {})},
            timeout=30, follow_redirects=False, trust_env=False,
            transport=httpx.HTTPTransport(verify=ssl_context, retries=0, trust_env=False),
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.client.close()

    def _request(self, method: str, path: str, *, document: dict[str, Any] | None = None) -> dict[str, Any] | None:
        try:
            with self.client.stream(method, path, json=document) as response:
                if method == "GET" and response.status_code == 404:
                    return None
                if (response.status_code != (200 if method == "GET" else 201)
                        or response.headers.get("content-encoding", "identity").lower() != "identity"):
                    raise ValueError()
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(content) + len(chunk) > 4 * 1024 * 1024:
                        raise ValueError()
                    content.extend(chunk)
                value = json.loads(content)
                if not isinstance(value, dict):
                    raise ValueError()
                return value
        except Exception:
            raise self.error_type("protected management Kubernetes outcome unavailable") from None
