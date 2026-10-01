"""Actual management startup loads admission profiles used by authenticated HTTP."""
import json

import httpx

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.test_nebius_pool_participant_http import client, setup
from tests.integration.test_nebius_pool_registry import sessions as sessions
from tests.unit.test_nebius_pool_profiles import catalog_document


async def test_management_lifespan_loads_profiles_for_real_prepare(sessions, tmp_path):
    seeded, _, token, _, executions, _ = await setup(sessions, tmp_path)
    file = tmp_path / "profiles.json"
    file.write_text(json.dumps(catalog_document(seeded.state.pool_profiles)))
    app = create_app(LoomServiceSettings(_env_file=None, service_mode="management", pool_profiles_file=file,
        db_url=sessions.kw["bind"].url.render_as_string(hide_password=False)))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
            receipt = await client(http, token).prepare(executions[0])
            assert receipt.phase == "reserved"
    assert not hasattr(app.state, "pool_profiles")
    assert not hasattr(app.state, "session_factory")
