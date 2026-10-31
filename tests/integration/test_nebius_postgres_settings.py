"""Exercise the rendered startup cache setting in a disposable PostgreSQL 16."""
from pathlib import Path

from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from loom.nebius_platform_render import build_platform
from tests.support.nebius_platform import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


def test_postgres_starts_with_selected_cache_and_resource_limits(platform_inputs):
    config, candidate, profile = platform_inputs
    config['postgres_settings'] = {
        'cpu_request_millis': 1000, 'cpu_limit_millis': 3000,
        'memory_request_mib': 1024, 'memory_limit_mib': 2048, 'shared_buffers_mib': 512,
    }
    files = build_platform(config, candidate, profile, {}, repo_root=ROOT)
    manifest = next(row for row in files['20-database.yaml'] if row['kind'] == 'StatefulSet')
    args = manifest['spec']['template']['spec']['containers'][0]['args']
    setting = next(value for value in args if value.startswith('shared_buffers='))
    with PostgresContainer('postgres:16').with_kwargs(
        mem_limit='2g', nano_cpus=3_000_000_000,
    ).with_command('postgres -c ' + setting) as pg:
        engine = create_engine(pg.get_connection_url().replace('postgresql+psycopg2://', 'postgresql+psycopg://'))
        try:
            for _ in range(2):
                with engine.connect() as connection:
                    assert connection.scalar(text('SELECT 1')) == 1
                    assert connection.scalar(text("SELECT pg_size_bytes(current_setting('shared_buffers'))")) == 512 * 1024**2
                    assert connection.scalar(text("SELECT current_setting('max_connections')")) == '100'
                    assert connection.scalar(text("SELECT current_setting('work_mem')")) == '4MB'
                    assert connection.scalar(text("SELECT pending_restart FROM pg_settings WHERE name='shared_buffers'")) is False
            container = pg.get_wrapped_container()
            container.reload()
            assert container.attrs['HostConfig']['Memory'] == 2 * 1024**3
            assert container.attrs['HostConfig']['NanoCpus'] == 3_000_000_000
            assert container.attrs['State']['OOMKilled'] is False
        finally:
            engine.dispose()
