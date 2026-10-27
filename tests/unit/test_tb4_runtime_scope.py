from loom.harbor_task_import import project_harbor_task
from loom.models.task import TaskConfig
from loom.task_runtime_compatibility import task_runtime_rejections
from tests.unit.test_harbor_native_import import source_task


def test_hosted_main_artifacts_and_independent_verifier_supported_but_legacy_blocked():
    raw = source_task()
    raw['artifacts'] = ['/app/output']
    raw['verifier'].pop('collect', None)
    task = TaskConfig.model_validate(project_harbor_task(raw))
    hosted = task_runtime_rejections(task, runtime='hosted', agent_name='terminus-2')
    legacy = task_runtime_rejections(task, runtime='legacy', agent_name='terminus-2')
    assert not any('independent_verifier' in r or 'structured_artifact' in r for r in hosted)
    assert any('independent_verifier' in r for r in legacy)
    assert any('structured_artifact' in r for r in legacy)


def test_unbound_service_still_blocks_before_execution():
    raw = source_task()
    raw['artifacts'] = [{'source': '/app/output', 'service': 'db'}]
    task = TaskConfig.model_validate(project_harbor_task(raw))
    assert any('service' in r for r in task_runtime_rejections(task, runtime='hosted'))
