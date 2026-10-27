"""Prepared Compose images cross the source broker without registry credentials."""
import pytest

from loom.compose_image_inputs import compose_image_inputs
from loom.models.compose import ComposeServiceSpec, NativeComposeSpec
from loom.task_environment_handoff import RESOLVED_TASK_ENVIRONMENT
from tests.unit.test_independent_verifier_environment import _independent_plan

PREFIX='cr.eu-north1.nebius.cloud/e00registry'


def prepared_plan(*, native=False):
    _, task, trial, plan = _independent_plan()
    image=PREFIX+'/tasks@sha256:'+'a'*64
    other=PREFIX+'/tasks@sha256:'+'b'*64
    env=task.environment.model_copy(update={'docker_image':image,'compose_files':('environment/compose.yaml',),
        'native_compose':NativeComposeSpec(source='environment/compose.yaml',services=(
            ComposeServiceSpec(name='main',image=image),ComposeServiceSpec(name='database',image=other),
        ))})
    from loom.hosted_harness import hosted_harness
    from loom.service_execution_materialization import service_execution_input_binding
    from loom.task_sandbox_planner import TaskSandboxPlanRequest, compile_task_sandbox_plan
    from tests.unit.test_guest_execution_materialization import _guest_inputs
    from tests.unit.test_service_execution_materialization import _REVISION, _provenance
    _, _, profile = _guest_inputs("nested_docker")
    task = task.model_copy(update={'environment':env})
    if native:
        from loom.harbor_verifier_script import VERIFIER_SCRIPT_PATH
        from loom.models.harbor import HarborConversion
        task = task.model_copy(update={'upstream_conversion': HarborConversion(changes=()),
            'verifier': task.verifier.model_copy(update={'args': {'script_path': VERIFIER_SCRIPT_PATH}})})
    plan = compile_task_sandbox_plan(TaskSandboxPlanRequest(
        spec=hosted_harness('terminus-2'), task=task, trial=trial, profile=profile,
        binding=service_execution_input_binding(_provenance()), task_revision_sha256=_REVISION,
        command_identity='sha256:'+'4'*64, output_paths=('answer.txt',),
        effective_network_policy=env.baseline_network_policy,
        controller_image=profile.agent_image_ref, task_image_materialization_id=plan.task_image_materialization_id,
        has_resolved_task_environment=True,
    ))
    return task,trial,plan


def test_compose_inputs_require_the_frozen_image_grant():
    _, _, plan = prepared_plan()
    assert set(compose_image_inputs(plan)) == {'main', 'database'}
    with pytest.raises(ValueError, match='image grant'):
        compose_image_inputs(plan.model_copy(update={'task_image_materialization_id': None}))


def _lease_for(plan,task,trial):
    from loom.execution_contract import workload_requirements_from_task
    from loom.pipeline.keys import canonical_digest
    from tests.unit.test_execution_actuator import _lease
    lease=_lease()
    lease.runtime_contract_json=plan.canonical_payload()
    lease.runtime_contract_sha256=canonical_digest(lease.runtime_contract_json)
    lease.execution_class_id=plan.execution_class_id
    lease.workload_requirements_json=workload_requirements_from_task(task,trial).model_dump(mode='json')
    return lease


def test_renderer_mounts_prepared_archives_only_in_trusted_controller():
    from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
    task,trial,plan=prepared_plan()
    lease=_lease_for(plan,task,trial)
    target=ExecutionTargetRuntime(target_id=lease.target_id,namespace=lease.namespace_name)
    pod=render_execution_job(lease,target=target)['spec']['template']['spec']
    assert plan.controller_resources.ephemeral_storage_mib == plan.task_resources.ephemeral_storage_mib + task.environment.storage_mb + plan.runtime_volume_mib + 3072
    assert not any(c['name']=='compose-image-materializer' for c in pod['initContainers'])
    assert not any('registry' in v['name'] for v in pod['volumes'])
    for container in [*pod['initContainers'],*pod['containers']]:
        mounts={m['name']:m for m in container['volumeMounts']}
        assert ('compose-images' in mounts) == (container['name']=='execution')


@pytest.mark.parametrize('native', [False, True])
def test_only_declared_native_independent_handoff_makes_workspace_diagnostic(native):
    _, _, plan = prepared_plan(native=native)
    declarations = {item.relative_path:item for item in plan.output_declarations}
    assert declarations['artifacts/workspace.tar'].required is not native
    assert ('artifacts/workspace-references.json' in declarations) is not native
    assert ('diagnostics/workspace-archive.json' in declarations) is native


def test_compose_outer_guest_never_executes_authored_main_entrypoint():
    from loom.hosted_harness import hosted_harness
    from loom.service_execution_materialization import service_execution_input_binding
    from loom.task_sandbox_planner import TaskSandboxPlanRequest, compile_task_sandbox_plan
    from tests.unit.test_guest_execution_materialization import _guest_inputs
    from tests.unit.test_service_execution_materialization import _REVISION, _provenance
    task, trial, previous = prepared_plan()
    task = task.model_copy(update={'environment': task.environment.model_copy(update={'entrypoint': ('/authored-init',)})})
    _, _, profile = _guest_inputs('nested_docker')
    plan = compile_task_sandbox_plan(TaskSandboxPlanRequest(
        spec=hosted_harness('terminus-2'), task=task, trial=trial, profile=profile,
        binding=service_execution_input_binding(_provenance()), task_revision_sha256=_REVISION,
        command_identity='sha256:'+'4'*64, output_paths=('answer.txt',),
        effective_network_policy=task.environment.baseline_network_policy,
        controller_image=profile.agent_image_ref, task_image_materialization_id=previous.task_image_materialization_id,
        has_resolved_task_environment=True,
    ))
    assert plan.sidecars[0].argv[0] == '/loom/bin/loom-sandbox-runtime'
    from loom.task_environment_handoff import decode_resolved_task_environment
    assert decode_resolved_task_environment(plan.main.environment[RESOLVED_TASK_ENVIRONMENT]).entrypoint == ('/authored-init',)
