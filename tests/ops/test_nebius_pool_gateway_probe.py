"""Real subprocess, TLS, projected-file reader and HTTP authentication; no cloud."""
from __future__ import annotations

import copy
import hmac
import json
import os
import ssl
import subprocess
import sys
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from tests.ops import test_nebius_certificates as certificates


@pytest.fixture
def projected_gateway(tmp_path, monkeypatch):
    monkeypatch.setattr(certificates, 'NOW', datetime.now(UTC))
    chain, key, roots = certificates.material(names=('localhost',))
    certificate, private, ca = (tmp_path / name for name in ('server.crt', 'server.key', 'ca.crt'))
    certificate.write_bytes(chain)
    private.write_bytes(key)
    private.chmod(0o600)
    ca.write_bytes(roots[0].public_bytes(serialization.Encoding.PEM))
    token, generation = tmp_path / 'token', tmp_path / 'token-1'
    generation.write_text('private-projected-one')
    generation.chmod(0o440)
    token.symlink_to(generation)
    namespaces = {'loom-build': str(uuid4()), 'loom-exec': str(uuid4())}
    state = {'requests': [], 'damage': None, 'namespaces': namespaces, 'bearer': 'private-projected-one'}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state['requests'].append((self.command, self.path, self.headers.get('Authorization')))
            name = self.path.removeprefix('/api/v1/namespaces/')
            if name not in namespaces or self.headers.get('Authorization') != 'Bearer ' + state['bearer']:
                status, body = 403, b'private-access-error'
            else:
                status = 200
                document = {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name, 'uid': namespaces[name]}}
                if state['damage'] == 'uid':
                    document['metadata']['uid'] = str(uuid4())
                elif state['damage'] == 'name':
                    document['metadata']['name'] = 'foreign'
                elif state['damage'] == 'deleting':
                    document['metadata']['deletionTimestamp'] = '2026-10-02T00:00:00Z'
                elif state['damage'] == 'kind':
                    document['kind'] = 'Secret'
                body = json.dumps(document).encode()
                if state['damage'] == 'oversized':
                    body = b' ' * (512 * 1024 + 1) + body
                elif state['damage'] == 'invalid_json':
                    body = b'private-invalid-json'
                elif state['damage'] in {'redirect', 'unauthorized', 'unavailable'}:
                    status = {'redirect': 307, 'unauthorized': 401, 'unavailable': 503}[state['damage']]
                    body = b'private-api-error'
            if state['damage'] == 'rotation' and len(state['requests']) == 1:
                successor, pending = tmp_path / 'token-2', tmp_path / 'token-next'
                successor.write_text('private-projected-two')
                successor.chmod(0o440)
                pending.symlink_to(successor)
                pending.replace(token)
                state['bearer'] = 'private-projected-two'
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Location', '/foreign-redirect')
            if state['damage'] == 'encoding':
                self.send_header('Content-Encoding', 'gzip')
            self.end_headers()
            try:
                self.wfile.write(body)
            except (ConnectionResetError, BrokenPipeError, ssl.SSLEOFError):
                pass
        def log_message(self, *args):
            pass

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, private)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    expected = {'pool_id': str(uuid4()), 'installation_id': str(uuid4()), 'machine_id': str(uuid4()),
        'admission_epoch': 2, 'kubernetes': {'kind': 'projected_service_account',
            'endpoint': f'https://localhost:{server.server_port}', 'ca_file': str(ca), 'token_file': str(token)},
        'namespaces': namespaces}
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update({f'LOOM_POOL_GATEWAY_{name.upper()}': str(expected[name])
        for name in ('pool_id', 'installation_id', 'machine_id', 'admission_epoch')})
    environment.update(LOOM_POOL_GATEWAY_DB_URL='postgresql+psycopg://fixture:private-db@localhost/loom',
        LOOM_POOL_GATEWAY_BEARER_TOKEN_FILE=str(tmp_path / 'unused-machine-token'),
        LOOM_POOL_GATEWAY_KUBERNETES=json.dumps(expected['kubernetes']),
        HTTPS_PROXY='http://127.0.0.1:1', ALL_PROXY='http://127.0.0.1:1', NO_PROXY='',
        KUBECONFIG=str(tmp_path / 'absent-operator-kubeconfig'))
    try:
        yield state, expected, environment, token, ca
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


def run_probe(projected_gateway, tmp_path, *, names=None, expected=None):
    from scripts.ops.nebius_pool_gateway_probe import BOUND_GATEWAY_KUBERNETES_COMMAND

    _, wanted, environment, _, _ = projected_gateway
    nonce = 'ab' * 32
    payload = json.dumps(wanted if expected is None else expected, sort_keys=True, separators=(',', ':')).encode()
    signature = hmac.new(bytes.fromhex(nonce), payload, 'sha256').hexdigest()
    return subprocess.run([sys.executable, '-c', BOUND_GATEWAY_KUBERNETES_COMMAND,
        json.dumps(sorted(wanted['namespaces']) if names is None else names), nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=35, check=False)


@pytest.mark.parametrize('rotation', [False, True])
def test_projected_gateway_probe_reads_only_registered_namespace_identities(projected_gateway, tmp_path, rotation):
    state, expected, _, _, _ = projected_gateway
    if rotation:
        state['damage'] = 'rotation'
    result = run_probe(projected_gateway, tmp_path)
    assert (result.returncode, result.stdout, result.stderr) == (0, b'{"status": "qualified"}\n', b'')
    assert state['requests'] == [('GET', '/api/v1/namespaces/' + name,
        'Bearer private-projected-' + ('two' if rotation and index else 'one'))
        for index, name in enumerate(sorted(expected['namespaces']))]


@pytest.mark.parametrize('damage', ['uid', 'name', 'deleting', 'kind', 'oversized', 'invalid_json',
    'redirect', 'unauthorized', 'unavailable', 'encoding', 'ca', 'untrusted_ca', 'hostname', 'token_missing', 'token_permissions',
    'token_invalid', 'connection', 'machine', 'challenge'])
def test_projected_gateway_probe_rejects_wrong_scope_tls_credentials_or_response(projected_gateway, tmp_path, damage):
    state, expected, environment, token, ca = projected_gateway
    wanted = copy.deepcopy(expected)
    state['damage'] = damage
    if damage == 'ca':
        ca.write_text('private-invalid-ca')
    elif damage == 'untrusted_ca':
        _, _, foreign_roots = certificates.material(names=('foreign.localhost',))
        ca.write_bytes(foreign_roots[0].public_bytes(serialization.Encoding.PEM))
    elif damage == 'hostname':
        wanted['kubernetes']['endpoint'] = wanted['kubernetes']['endpoint'].replace('localhost', '127.0.0.1')
        environment['LOOM_POOL_GATEWAY_KUBERNETES'] = json.dumps(wanted['kubernetes'])
    elif damage == 'token_missing':
        token.unlink()
    elif damage == 'token_permissions':
        token.chmod(0o644)
    elif damage == 'token_invalid':
        token.chmod(0o640)
        token.write_text('private invalid credential')
    elif damage == 'connection':
        wanted['kubernetes']['endpoint'] = 'https://foreign.example.com'
    elif damage == 'machine':
        environment['LOOM_POOL_GATEWAY_MACHINE_ID'] = str(uuid4())
    elif damage == 'challenge':
        wanted['namespaces']['foreign'] = str(uuid4())
    result = run_probe(projected_gateway, tmp_path, expected=wanted)
    assert result.returncode == 1 and result.stdout == b''
    assert result.stderr == b'Pool gateway Kubernetes access unqualified\n'
    assert all(method == 'GET' and path in {'/api/v1/namespaces/' + name for name in expected['namespaces']}
        for method, path, _ in state['requests'])
    assert len(state['requests']) <= len(expected['namespaces'])


@pytest.mark.parametrize('names', [[], ['../secrets'], ['loom-exec', 'loom-exec'], ['loom-exec'] * 257, {'loom-exec': 'ignored'}])
def test_projected_gateway_probe_rejects_invalid_destinations_before_network(projected_gateway, tmp_path, names):
    state, _, _, _, _ = projected_gateway
    result = run_probe(projected_gateway, tmp_path, names=names)
    assert result.returncode == 1 and result.stdout == b'' and b'private-' not in result.stderr
    assert state['requests'] == []
