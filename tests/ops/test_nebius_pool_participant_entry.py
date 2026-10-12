"""Explicit operator authentication/config lifetime; no real cluster access."""
from __future__ import annotations

import base64
import json
import os
import ssl
import stat
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import certifi
import pytest
from scripts.ops import nebius_development_entry as authentication
from scripts.ops import nebius_pool_participant_entry as entry

from loom.nebius_kubernetes import NebiusKubernetesConnection


@pytest.mark.parametrize('ancestry', [None, False, 'git'])
def test_protected_delivery_requires_authenticated_ancestry_before_opening_history(ancestry):
    # Missing protected composition must not fall back to the host's Git state.
    with pytest.raises(ValueError, match='ancestry'):
        entry.run_participant_release(SimpleNamespace(), retained=None, initial=None,
            expected=None, pool=None, writer=None, kube=None, candidate_ancestry=ancestry)


@pytest.mark.parametrize('failure', [None, 'operation', 'authentication', 'changed-material', 'endpoint', 'nested-ssh'])
def test_authenticated_operator_connection_pins_cluster_trust_and_cleans_up(tmp_path, monkeypatch, failure):
    assert hasattr(entry, 'authenticated_participant_release'), 'participant delivery has no explicit operator authentication'
    ca, credentials, executable = (tmp_path / name for name in ('ca.pem', 'operator.json', 'kubectl'))
    ca.write_bytes(Path(certifi.where()).read_bytes())
    credentials.write_bytes(b'private-test-credentials')
    executable.write_text('#!/bin/sh\nexit 0\n')
    for path in (ca, credentials):
        path.chmod(0o600)
    executable.chmod(0o700)
    connection = NebiusKubernetesConnection(endpoint='https://cluster.test', ca_file=ca, credentials_file=credentials)
    retained = SimpleNamespace(inputs=SimpleNamespace(config={
        'cluster_id': 'test-nebius-cluster', 'kubernetes_api_server': 'https://cluster.test'}))
    files = {ca: ca.read_bytes(), credentials: credentials.read_bytes()}
    if failure == 'endpoint':
        retained.inputs.config['kubernetes_api_server'] = 'https://foreign.test'
    monkeypatch.delenv('LOOM_DEPLOY_SSH_TARGET', raising=False)
    if failure == 'nested-ssh':
        monkeypatch.setenv('LOOM_DEPLOY_SSH_TARGET', 'foreign@host.invalid')
    monkeypatch.setenv('KUBECONFIG', '/foreign/kubeconfig')
    monkeypatch.setenv('HTTPS_PROXY', 'https://foreign-proxy.test')
    selected, authenticated, commands = [], [], []

    async def transport(value):
        authenticated.append(value)
        if failure == 'authentication':
            raise RuntimeError('private-error-token')
        if failure == 'changed-material':
            credentials.write_bytes(b'changed-credentials')
        return ssl.create_default_context(cafile=str(ca)), 'operator-test-token'

    # This test owns the authentication/config lifetime boundary. The actual
    # observer/writer/pool composition is exercised by the connected census test.
    @contextmanager
    def consume(**kwargs):
        selected.append(kwargs)
        yield kwargs

    def command(argv, **kwargs):
        commands.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout='{"kind":"Namespace"}', stderr='')

    monkeypatch.setattr(authentication, '_transport', transport)
    monkeypatch.setattr(entry, 'connected_participant_release', consume)
    monkeypatch.setattr(entry.Kubectl.run.__globals__['subprocess'], 'run', command)
    path = None
    try:
        with entry.authenticated_participant_release(retained=retained, initial=object(), expected=object(),
                bindings=object(), shared_state=tmp_path / 'history', operator_connection=connection,
                executable=executable, operator_credentials=credentials, private_files=files) as delivery:
            assert failure in {None, 'operation'}
            path = delivery['kubeconfig']
            config = json.loads(path.read_bytes())
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
            assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
            assert config['current-context'] == 'test-nebius-cluster'
            cluster, = config['clusters']
            assert cluster == {'name': 'test-nebius-cluster', 'cluster': {
                'server': 'https://cluster.test', 'certificate-authority-data': base64.b64encode(files[ca]).decode()}}
            assert config['contexts'] == [{'name': 'test-nebius-cluster',
                'context': {'cluster': 'test-nebius-cluster', 'user': 'participant-operator'}}]
            assert config['users'] == [{'name': 'participant-operator', 'user': {'token': 'operator-test-token'}}]
            assert delivery['private_files'] == {**files, path: path.read_bytes()}
            assert delivery['token'] == 'operator-test-token'
            assert delivery['ssl_context'].check_hostname
            assert delivery['ssl_context'].verify_mode == ssl.CERT_REQUIRED
            assert delivery['kube'].get('namespace', 'loom-dev', 'loom-dev') == {'kind': 'Namespace'}
            argv, kwargs = commands[0]
            assert argv[0] == str(executable) and argv[1:3] == ['--kubeconfig', str(path)]
            assert kwargs['env'] == {'PATH': os.defpath, 'LANG': 'C.UTF-8'}
            if failure == 'operation':
                raise RuntimeError('preserve original deployment error')
    except RuntimeError as error:
        assert failure == 'operation' and str(error) == 'preserve original deployment error'
    except ValueError as error:
        assert failure in {'authentication', 'changed-material', 'endpoint', 'nested-ssh'}
        assert 'private-error-token' not in str(error)
        assert not selected and not commands
    else:
        assert failure is None
    assert len(authenticated) == (0 if failure in {'endpoint', 'nested-ssh'} else 1)
    if path is not None:
        assert not path.exists() and not path.parent.exists()
