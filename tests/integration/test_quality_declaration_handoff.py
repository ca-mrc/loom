"""Zero-model verification of the exact Poetry/Jupyter declaration boundaries."""
from pathlib import PurePosixPath

import pytest
from scripts.ops.repair_quality_task_declarations import (
    JUPYTER,
    JUPYTER_ROOTS,
    POETRY,
    corrected_config,
)

from loom.models.task import TaskConfig
from loom.service_execution_sandbox_task import _POLICY
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths
from loom.trial.workspace_references import (
    export_workspace_references,
    import_workspace_with_references,
)
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _export_workspace_archive,
    _validate_workspace_archive,
)
from tests.integration.test_task_identity_installation_docker import (  # noqa: F401
    native_binary,
    sandboxes,
)
from tests.unit.test_service_execution_terminus_plan import _inputs

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


def _environment(task_id):
    task, _, _ = _inputs()
    config = task.model_dump(mode="json")
    config["task"]["id"] = task_id
    config["environment"]["mutable_paths"] = ["/root/.cache/pypoetry"] if task_id == POETRY else [JUPYTER_ROOTS[0]]
    return TaskConfig.model_validate(corrected_config(config)).environment


@pytest.mark.parametrize("sandboxes", ["python:3.9-slim"], indirect=True)
async def test_poetry_python39_links_survive_workspace_and_cache_handoff(sandboxes, tmp_path):  # noqa: F811
    agent, verifier, other = sandboxes
    for driver in (agent, verifier):
        assert (await driver.exec("mkdir -p /app /root/.cache/pypoetry")).return_code == 0
    created = await agent.exec("python3.9 -m venv --without-pip /app/.venv && python3.9 -m venv --without-pip /root/.cache/pypoetry/example")
    assert created.return_code == 0, created.stderr
    root = PurePosixPath('/app')
    archive = tmp_path / 'workspace.tar'
    await _export_workspace_archive(agent, root, archive)
    with pytest.raises(WorkspaceSnapshotError, match="escapes workdir"):
        _validate_workspace_archive(archive, _POLICY, root=root)
    env = _environment(POETRY)
    await export_workspace_references(agent, archive, root=root, policy=_POLICY, reference_files=env.workspace_reference_files)
    await export_mutable_paths(agent, env.mutable_paths, tmp_path/'mutable', workdir=root, reference_files=env.mutable_path_reference_files)
    await import_workspace_with_references(verifier, archive, root, policy=_POLICY, reference_files=env.workspace_reference_files)
    await import_mutable_paths(verifier, env.mutable_paths, tmp_path/'mutable', workdir=root, reference_files=env.mutable_path_reference_files)
    check = await verifier.exec("/app/.venv/bin/python -c 'import sys; assert sys.version_info[:2] == (3,9)' && /root/.cache/pypoetry/example/bin/python -c 'print(39)'")
    assert check.return_code == 0 and check.stdout.strip() == b'39', check.stderr
    assert (await other.exec('test ! -e /app/.venv && test ! -e /root/.cache/pypoetry/example')).return_code == 0


async def test_jupyter_user_and_system_registration_roots_survive_fresh_verifier(sandboxes, tmp_path):  # noqa: F811
    agent, verifier, other = sandboxes
    env = _environment(JUPYTER)
    for driver in (agent, verifier):
        assert (await driver.exec("mkdir -p /app " + " ".join(JUPYTER_ROOTS))).return_code == 0
    for i, root in enumerate(JUPYTER_ROOTS):
        result = await agent.exec(f"mkdir -p {root}/kernels/example; printf 'registration-{i}' > {root}/kernels/example/kernel.json")
        assert result.return_code == 0, result.stderr
    await export_mutable_paths(agent, env.mutable_paths, tmp_path/'mutable', workdir=PurePosixPath('/app'))
    await import_mutable_paths(verifier, env.mutable_paths, tmp_path/'mutable', workdir=PurePosixPath('/app'))
    for i, root in enumerate(JUPYTER_ROOTS):
        assert (await verifier.exec(f'cat {root}/kernels/example/kernel.json')).stdout == f'registration-{i}'.encode()
        assert (await other.exec(f'test ! -e {root}/kernels/example/kernel.json')).return_code == 0
