"""Bounded exact-address TLS/HTTPS proof without DNS, redirects or key export."""
from __future__ import annotations

import hashlib
import http.client
import ipaddress
import json
import re
import socket
import ssl
from collections.abc import Iterator
from contextlib import contextmanager


class ProbeError(RuntimeError):
    """A public route could not be authenticated; no response body is exposed."""


@contextmanager
def _connect(address: str, port: int, hostname: str) -> Iterator[ssl.SSLSocket]:
    try:
        ipaddress.ip_address(address)
        if (type(port) is not int or not 1 <= port <= 65535 or len(hostname) > 253 or "." not in hostname
                or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in hostname.split("."))):
            raise ProbeError("invalid exact-address HTTPS endpoint")
        context = ssl.create_default_context()
        context.set_alpn_protocols(["http/1.1"])
        with socket.create_connection((address, port), timeout=5) as connection:
            with context.wrap_socket(connection, server_hostname=hostname) as secured:
                yield secured
    except ProbeError:
        raise
    except Exception:
        raise ProbeError("exact-address HTTPS verification failed") from None


def probe_management(*, address: str, port: int, hostname: str, fingerprint: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ProbeError("invalid expected ingress certificate")
    with _connect(address, port, hostname) as stream:
        certificate = stream.getpeercert(binary_form=True)
        if not certificate or hashlib.sha256(certificate).hexdigest() != fingerprint:
            raise ProbeError("public ingress serves a different certificate")


def probe_legacy(*, address: str, port: int, hostname: str, environment: str, candidate: str) -> None:
    if environment not in {"development", "staging", "production"} or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise ProbeError("invalid expected legacy identity")
    for path in ("/api/v1/health", "/api/v1/version", "/loom-frontend-config.json"):
        with _connect(address, port, hostname) as stream:
            stream.sendall(f"GET {path} HTTP/1.1\r\nHost: {hostname}\r\nConnection: close\r\n\r\n".encode("ascii"))
            with http.client.HTTPResponse(stream) as response:
                response.begin()
                payload = response.read(1024 * 1024 + 1)
                if response.status != 200 or len(payload) > 1024 * 1024:
                    raise ProbeError("legacy HTTPS response failed qualification")
                value = json.loads(payload)
                if (not isinstance(value, dict)
                        or (path.endswith("/health") and value.get("status") != "ok")
                        or (path.endswith("/version") and value.get("buildRevision") != candidate)
                        or (path.endswith("config.json") and (
                            value.get("environment") != environment or value.get("apiRouteBase") != "https://" + hostname + "/api"))):
                    raise ProbeError("legacy HTTPS health, candidate or environment differs")


def probe_shared_development(*, address: str, port: int, hostname: str, candidate: str) -> None:
    """Read-only routing/auth boundary proof, not login or execution acceptance."""
    if not re.fullmatch(r'[0-9a-f]{40}', candidate):
        raise ProbeError('invalid shared development candidate')
    probes: list[tuple[str, str | None]] = [
        (path, None) for path in ('/', '/api/v1/health', '/api/v1/version', '/loom-frontend-config.json')]
    probes += [('/api/v1/health/ready', token) for token in (None, 'loom-dev-invalid-probe-token')]
    try:
        for path, token in probes:
            with _connect(address, port, hostname) as stream:
                headers = f'Authorization: Bearer {token}\r\n' if token else ''
                stream.sendall((f'GET {path} HTTP/1.1\r\nHost: {hostname}\r\nAccept-Encoding: identity\r\n'
                    f'{headers}Connection: close\r\n\r\n').encode('ascii'))
                with http.client.HTTPResponse(stream) as response:
                    response.begin()
                    raw = response.read(65537)
                    if len(raw) > 65536 or response.getheader('Content-Encoding', 'identity').lower() != 'identity':
                        raise ValueError()
                    if path.endswith('/ready'):
                        if response.status not in {401, 403}:
                            raise ValueError()
                        continue
                    if response.status != 200:
                        raise ValueError()
                    if path == '/':
                        if (response.getheader('Content-Type', '').split(';')[0].strip().lower() != 'text/html'
                                or b'<!doctype html>' not in raw.lower() or b'id="root"' not in raw):
                            raise ValueError()
                        continue
                    value = json.loads(raw)
                    if (not isinstance(value, dict)
                            or (path.endswith('/health') and value.get('status') != 'ok')
                            or (path.endswith('/version') and value.get('buildRevision') != candidate)
                            or (path.endswith('config.json') and (value.get('environment') != 'development'
                                or value.get('apiRouteBase') != 'https://' + hostname + '/api'))):
                        raise ValueError()
    except Exception:
        raise ProbeError('shared development public route or authentication unqualified') from None
