"""Declared DB endpoints distinguish participants without exporting credentials."""
from __future__ import annotations

import base64
import json
from uuid import uuid4

import pytest
from tests.ops.test_nebius_controller_inventory import Installed, metadata, observe


class Databases(Installed):
    def __init__(self):
        super().__init__()
        self.secret = {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {
            **metadata('database', 'execution'), 'uid': str(uuid4())}, 'data': {
                'actuator-url': base64.b64encode(b'postgresql+psycopg://private-user:private-password@loom-postgres.platform.svc:5432/loom?sslmode=verify-full').decode(),
                'private-unrelated-key': 'private-unrelated-data'}}
        self.secret_reads = []

    def get(self, kind, name, namespace):
        if kind == 'secret':
            self.secret_reads.append((kind, name, namespace))
            assert (name, namespace) == ('database', 'execution')
            return self.secret
        return super().get(kind, name, namespace)


def test_shared_credential_references_are_resolved_once_without_exposing_authentication():
    cluster = Databases()
    report = observe(cluster)
    entries = report['declared_database_endpoints']
    assert len(entries) == 3
    actuators = [row for row in entries if row['setting'] == 'LOOM_EXECUTION_ACTUATOR_DB_URL']
    assert {row['controller']['name'] for row in actuators} == {'standard-actuator', 'guest-actuator'}
    for row in actuators:
        assert row['status'] == 'observed'
        assert row['endpoint'] == {'host': 'loom-postgres.platform.svc', 'port': 5432, 'database': 'loom'}
        assert row['credential'] == {'name': 'database', 'namespace': 'execution', 'uid': cluster.secret['metadata']['uid'],
            'resource_version': '21', 'key': 'actuator-url'}
    assert cluster.secret_reads == [('secret', 'database', 'execution')]
    assert 'private-' not in json.dumps(report)
    assert 'resolved_database_identity' in report['unverified']
    inline, = [row for row in entries if row['setting'] == 'LOOM_CP_DB_URL']
    assert inline['status'] == 'unavailable'


@pytest.mark.parametrize('damage', ['host_query', 'database_query', 'service_query', 'port_query', 'fragment', 'invalid_port',
    'zero_port', 'duplicate_query', 'encoded_database', 'raw_password_separator', 'empty_port',
    'scheme', 'uppercase_scheme', 'bad_encoding', 'deleting', 'wrong_name', 'wrong_namespace', 'missing_uid', 'missing_version', 'missing_key', 'read_error'])
def test_ambiguous_or_unqualified_database_endpoint_is_not_reported(damage, monkeypatch):
    cluster = Databases()
    url = 'postgresql://private-user:private-password@loom-postgres.platform.svc:5432/loom'
    if damage.endswith('_query'):
        query = {'host_query': 'host=private-host', 'database_query': 'dbname=private-db',
            'service_query': 'service=private-service', 'port_query': 'port=5433',
            'duplicate_query': 'sslmode=verify-full&sslmode=disable'}[damage]
        url += '?' + query
    elif damage == 'fragment':
        url += '#private-fragment'
    elif damage == 'invalid_port':
        url = url.replace(':5432/', ':65536/')
    elif damage == 'zero_port':
        url = url.replace(':5432/', ':0/')
    elif damage == 'empty_port':
        url = url.replace(':5432/', ':/')
    elif damage == 'encoded_database':
        url = url.replace('/loom', '/%6coom')
    elif damage == 'raw_password_separator':
        url = url.replace('private-password@', 'private-password@extra@')
    elif damage == 'scheme':
        url = url.replace('postgresql:', 'https:')
    elif damage == 'uppercase_scheme':
        url = url.replace('postgresql:', 'POSTGRESQL:')
    cluster.secret['data']['actuator-url'] = base64.b64encode(url.encode()).decode()
    if damage == 'bad_encoding':
        cluster.secret['data']['actuator-url'] = 'invalid base64'
    elif damage == 'deleting':
        cluster.secret['metadata']['deletionTimestamp'] = '2026-09-30T00:00:00Z'
    elif damage in {'wrong_name', 'wrong_namespace'}:
        cluster.secret['metadata'][damage.removeprefix('wrong_')] = 'foreign'
    elif damage in {'missing_uid', 'missing_version'}:
        cluster.secret['metadata'].pop('uid' if damage == 'missing_uid' else 'resourceVersion')
    elif damage == 'missing_key':
        cluster.secret['data'].pop('actuator-url')
    elif damage == 'read_error':
        original = cluster.get
        def get(kind, name, namespace):
            if kind == 'secret':
                raise ValueError('private-server-response')
            return original(kind, name, namespace)
        monkeypatch.setattr(cluster, 'get', get)
    rows = observe(cluster)['declared_database_endpoints']
    assert len(rows) == 3 and all(row['status'] == 'unavailable' and 'endpoint' not in row for row in rows)
    assert 'private-' not in json.dumps(rows)


@pytest.mark.parametrize('url', [
    'postgresql+psycopg://user:private%40password@loom-postgres.platform.svc:5432/loom?sslmode=verify-full',
    'postgresql+psycopg://user:private-password@loom-postgres.platform.svc/loom',
])
def test_observed_endpoint_agrees_with_the_application_connection_parser(url):
    from sqlalchemy.engine import make_url

    cluster = Databases()
    cluster.secret['data']['actuator-url'] = base64.b64encode(url.encode()).decode()
    report = observe(cluster)['declared_database_endpoints'][0]
    parsed = make_url(url)
    _, connection = parsed.get_dialect()().create_connect_args(parsed)
    assert report['status'] == 'observed'
    assert report['endpoint'] == {'host': connection['host'], 'port': connection.get('port', 5432), 'database': connection['dbname']}
    assert 'private-' not in json.dumps(report)


def test_control_plane_pooled_database_override_is_not_hidden_by_direct_url():
    cluster = Databases()
    controller = next(row for row in cluster.lists['deployments'] if row['metadata']['name'] == 'control')
    controller['metadata']['namespace'] = 'execution'
    controller['spec']['template']['spec']['containers'][0]['env'] = [
        {'name': setting, 'valueFrom': {'secretKeyRef': {'name': 'database', 'key': key}}}
        for setting, key in [('LOOM_CP_DB_URL', 'actuator-url'), ('LOOM_CP_DB_URL_POOL', 'pooled-url')]]
    cluster.secret['data']['pooled-url'] = base64.b64encode(
        b'postgresql+psycopg://private-user:private-password@pool.platform.svc:6432/loom').decode()
    rows = {row['setting']: row for row in observe(cluster)['declared_database_endpoints'] if row['controller']['name'] == 'control'}
    assert rows['LOOM_CP_DB_URL']['endpoint']['host'] == 'loom-postgres.platform.svc'
    assert rows['LOOM_CP_DB_URL_POOL']['endpoint'] == {'host': 'pool.platform.svc', 'port': 6432, 'database': 'loom'}
    assert rows['LOOM_CP_DB_URL_POOL']['credential']['key'] == 'pooled-url'
    assert cluster.secret_reads == [('secret', 'database', 'execution')]
