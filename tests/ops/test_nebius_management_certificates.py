"""Independent manager TLS never requests or inherits the shared wildcard."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from tests.ops.test_nebius_certificates import NOW, client_output, installation, material, module

HOST = 'development-management.example.test'


def config_path(tmp_path: Path) -> Path:
    path = installation(tmp_path)
    config = json.loads(path.read_text())
    config['schema'] = 'loom.nebius-management-certificate-installation.v1'
    del config['child_domain']
    config['management_host'] = HOST
    path.write_text(json.dumps(config))
    return path


def test_manager_certificate_validates_one_exact_host_without_shared_wildcard():
    chain, key, roots = material(names=(HOST,))
    report = module().validate_management_certificate(chain, key, management_host=HOST, now=NOW, roots=roots)
    assert report['sans'] == [HOST]
    assert report['expires_at'] == '2026-12-02T12:00:00+00:00'
    assert len(report['fingerprint_sha256']) == 64


@pytest.mark.parametrize('names', [('*.example.test',), ('foreign.example.test',),
    (HOST, '*.dev.example.test'), (HOST, HOST)])
def test_manager_certificate_rejects_wildcard_foreign_and_additional_subjects(names):
    chain, key, roots = material(names=names)
    with pytest.raises(module().CertificateError):
        module().validate_management_certificate(chain, key, management_host=HOST, now=NOW, roots=roots)


@pytest.mark.parametrize('change', [{'days': 6}, {'start_days': 1}, {'ca_leaf': True}, {'client_only': True}])
def test_manager_certificate_retains_delivery_window_and_server_checks(change):
    chain, key, roots = material(names=(HOST,), **change)
    with pytest.raises(module().CertificateError):
        module().validate_management_certificate(chain, key, management_host=HOST, now=NOW, roots=roots)


def test_manager_certificate_requires_matching_key_and_trusted_chain():
    chain, key, roots = material(names=(HOST,))
    _, other_key, other_roots = material(names=(HOST,))
    for selected_key, selected_roots in ((other_key, roots), (key, other_roots)):
        with pytest.raises(module().CertificateError):
            module().validate_management_certificate(chain, selected_key, management_host=HOST, now=NOW,
                                                      roots=selected_roots)


def test_manager_issuer_retains_scoped_history_and_renews_without_shared_names(tmp_path, monkeypatch):
    path = config_path(tmp_path)
    chain, key, roots = material(names=(HOST,))
    state = tmp_path / 'certificate-state'

    def client(args, **kwargs):
        assert [args[i + 1] for i, word in enumerate(args) if word == '--domain'] == [HOST]
        assert 'management-hook auth' in args[args.index('--manual-auth-hook') + 1]
        assert 'management-hook cleanup' in args[args.index('--manual-cleanup-hook') + 1]
        client_output(state, chain, key)
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(module(), '_certbot_version', lambda: '5.8.0')
    monkeypatch.setattr(module(), '_run_client', client)
    first = module().issue_management_certificate(path, now=NOW, roots=roots)
    assert first['status'] == 'qualified' and first['sans'] == [HOST]
    assert json.loads((state / 'installation.json').read_text())['schema'] == 'loom.nebius-management-certificate-installation.v1'
    chain, key, roots = material(names=(HOST,))
    # The external issuer reuses its lineage; remove only its test symlinks so
    # the existing external-boundary fixture can write the next returned pair.
    for name in ('fullchain.pem', 'privkey.pem'):
        (state / 'acme/live/loom-managed' / name).unlink()
    second = module().issue_management_certificate(path, now=NOW, roots=roots)
    assert second['previous_generation'] == first['generation']
    assert second['generation'] != first['generation']
    assert (state / 'generations' / first['generation'] / 'privkey.pem').is_file()


def test_legacy_entry_rejects_management_schema_before_issuance(tmp_path, monkeypatch):
    path = config_path(tmp_path)
    monkeypatch.setattr(module(), '_run_client', lambda *a, **kw: pytest.fail('legacy issuer accepted a new scope'))
    with pytest.raises(module().CertificateError):
        module().issue_certificate(path, now=NOW)
    assert not (tmp_path / 'certificate-state').exists()


def test_management_entry_rejects_legacy_schema_before_issuance(tmp_path):
    with pytest.raises(module().CertificateError):
        module().issue_management_certificate(installation(tmp_path), now=NOW)
    assert not (tmp_path / 'certificate-state').exists()


@pytest.mark.parametrize('domain', ['*.dev.example.test', 'dev.example.test', 'management.example.test', '*' + HOST])
def test_management_hook_cannot_change_other_dns_names(tmp_path, monkeypatch, domain):
    monkeypatch.setenv('CERTBOT_DOMAIN', domain)
    monkeypatch.setenv('CERTBOT_VALIDATION', 'v' * 43)
    with pytest.raises(module().CertificateError):
        module().management_certificate_hook(config_path(tmp_path), 'auth')
    assert not (tmp_path / 'certificate-state').exists()
