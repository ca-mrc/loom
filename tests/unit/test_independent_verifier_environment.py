"""Independent grading images remain distinct throughout materialization."""
from pathlib import PurePosixPath
from uuid import uuid4

import pytest

from loom.models.networking import GatewayOnly, NoNetwork, Public, PublicWeb, WebAllowlist, WebDestination
from loom.models.task import EnvironmentConfig
from loom.task_image_build_plan import derive_task_image_build_components
from loom.task_image_materialization import (
    TaskImageExecutionGrantV1,
    required_task_image_components,
    resolve_prepared_task,
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def independent_inputs():
    task, trial, profile = _inputs()
    task = task.model_copy(update={
        'environment': task.environment.model_copy(update={
            'docker_image': None, 'dockerfile': PurePosixPath('environment/Dockerfile'),
            'docker_build_context': PurePosixPath('environment'),
        }),
        'verifier': task.verifier.model_copy(update={'environment': EnvironmentConfig(
            os='linux', dockerfile=PurePosixPath('tests/Dockerfile'), docker_build_context=PurePosixPath('tests'),
            cpus=3, memory_mb=6144, storage_mb=12288, user='image', image_workdir=True,
            image_entrypoint=True, environment={'ONLY_VERIFIER':'yes'},
        ), 'environment_vars': {'GRADE_MODE':'strict'}}),
    })
    return task, trial, profile


def verifier_grant(task, profile, workdir='/grading'):
    verifier_image = 'registry.example/verifier@sha256:' + 'a' * 64
    return TaskImageExecutionGrantV1(
        schema_version='loom.task-image-execution-grant.v1', materialization_id=uuid4(),
        materialization_key='1' * 64, cpu_arch='x86_64', task_checksum=_REVISION.removeprefix('sha256:'),
        task_config=task.model_dump(mode='json'), task_source=None, task_source_provenance=_provenance(),
        registry_images={'task':profile.task_image_ref,'verifier':verifier_image},
        image_runtime_metadata={'verifier':{
            'image_ref':verifier_image,'user':'grader','uid':1002,'gid':1003,'home':'/home/grader',
            'workdir':workdir,'entrypoint':['/init-grader'],'environment':{'FROM_IMAGE':'yes'},
        }},
    )


def test_independent_verifier_has_its_own_build_component():
    task, _, _ = independent_inputs()
    assert required_task_image_components(task) == {'task', 'verifier'}
    components = derive_task_image_build_components(task.model_dump(mode='json'))
    assert [(x.name,x.dockerfile_path,x.context_path) for x in components] == [
        ('task','environment/Dockerfile','environment'),('verifier','tests/Dockerfile','tests')]


def test_independent_verifier_resolves_its_own_image_defaults_without_rewriting_source():
    task, _, profile = independent_inputs()
    original = task.model_dump_json()
    grant = verifier_grant(task, profile)
    result = resolve_prepared_task(task, grant)
    env = result.verifier.environment
    assert env.docker_image == grant.registry_images['verifier']
    assert env.dockerfile is None
    assert env.user == '1002:1003'
    assert str(env.workdir) == '/grading'
    assert env.entrypoint == ('/init-grader',)
    assert env.environment == {'ONLY_VERIFIER':'yes','FROM_IMAGE':'yes','HOME':'/home/grader'}
    assert result.environment.docker_image == profile.task_image_ref
    assert task.model_dump_json() == original


def test_independent_verifier_rejects_missing_image_metadata():
    task, _, profile = independent_inputs()
    grant = verifier_grant(task, profile).model_copy(update={'image_runtime_metadata':{}})
    with pytest.raises(ValueError, match='verifier image OCI runtime metadata'):
        resolve_prepared_task(task, grant)


def _independent_plan(workdir='/grading', *, agent_policy=None, verifier_policy=None, harness='terminus-2'):
    from loom.hosted_harness import hosted_harness
    from loom.service_execution_materialization import service_execution_input_binding
    from loom.task_sandbox_planner import TaskSandboxPlanRequest, compile_task_sandbox_plan
    task, trial, profile = independent_inputs()
    if harness == 'oracle':
        from loom.models.trial import TrialConfig
        trial = TrialConfig(agent_name='oracle', agent_model=None)
    if agent_policy is not None:
        task = task.model_copy(update={'environment': task.environment.model_copy(update={
            'baseline_network_policy': agent_policy,
        })})
    if verifier_policy is not None:
        task = task.model_copy(update={'verifier': task.verifier.model_copy(update={
            'environment': task.verifier.environment.model_copy(update={
                'baseline_network_policy': verifier_policy,
            }),
        })})
    if agent_policy is not None or verifier_policy is not None:
        profile = profile.model_copy(update={'supports_task_web_egress': True})
    grant = verifier_grant(task, profile, workdir)
    prepared = resolve_prepared_task(task, grant)
    plan = compile_task_sandbox_plan(TaskSandboxPlanRequest(
        spec=hosted_harness(harness), task=prepared, trial=trial, profile=profile,
        binding=service_execution_input_binding(_provenance()), task_revision_sha256=_REVISION,
        command_identity='sha256:'+'4'*64, output_paths=('answer.txt',),
        effective_network_policy=prepared.environment.baseline_network_policy,
        controller_image=profile.agent_image_ref, task_image_materialization_id=grant.materialization_id,
        has_resolved_task_environment=True,
    ))
    return task, prepared, trial, plan


@pytest.mark.parametrize('harness', ['terminus-2', 'oracle'])
@pytest.mark.parametrize('agent_policy', [NoNetwork(), PublicWeb()], ids=lambda p: p.kind)
@pytest.mark.parametrize('verifier_policy', [
    NoNetwork(), GatewayOnly(), Public(), PublicWeb(),
    WebAllowlist(destinations=(WebDestination(host='example.com', protocol='https'),)),
], ids=lambda p: p.kind)
def test_independent_verifier_requires_egress_diagnostic_only_when_its_proxy_runs(
    agent_policy, verifier_policy, harness,
):
    from loom.execution_runtime_contract import TASK_EGRESS_OUTPUT, RuntimeHandoffInputV1
    from loom.task_sandbox_planner import compile_deferred_verifier_plan
    task, _, _, plan = _independent_plan(
        agent_policy=agent_policy, verifier_policy=verifier_policy, harness=harness,
    )
    original = plan.model_dump_json()
    deferred = compile_deferred_verifier_plan(
        plan, task, verifier_timeout_seconds=150,
        handoff_input=RuntimeHandoffInputV1(
            manifest_sha256='sha256:'+'a'*64, file_count=1, total_bytes=1,
        ),
    )
    assert deferred.effective_network_policy == verifier_policy
    proxy_runs = deferred.task_egress is not None or deferred.setup_egress is not None
    assert (TASK_EGRESS_OUTPUT in deferred.output_declarations) == proxy_runs
    assert plan.model_dump_json() == original


@pytest.mark.parametrize('workdir', ['/grading', '/', '/tests', '/opt/verifier'])
def test_deferred_verifier_keeps_own_image_identity_resources_and_environment(workdir):
    from loom.execution_contract import workload_requirements_from_task
    from loom.execution_runtime_contract import (
        RuntimeHandoffInputV1,
        validate_runtime_plan_requirements,
    )
    from loom.task_sandbox_planner import compile_deferred_verifier_plan
    from loom.verifier_runtime import validate_prepared_sandbox_images, verifier_execution_task
    task, prepared, trial, plan = _independent_plan(workdir)
    assert str(prepared.verifier.environment.workdir) == workdir
    deferred = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=150,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256='sha256:'+'a'*64,file_count=1,total_bytes=1))
    sandbox, = [s for s in deferred.sidecars if s.private_sandbox]
    assert sandbox.image_ref == prepared.verifier.environment.docker_image
    assert sandbox.image_ref != plan.task_image_ref
    assert sandbox.identity.run_as_user == 1002
    assert sandbox.environment['ONLY_VERIFIER'] == 'yes'
    assert sandbox.resources.cpu_millis == 3000
    assert sandbox.resources.memory_mib == 6144
    assert sandbox.resources.ephemeral_storage_mib == 12288
    assert deferred.task_image_ref == sandbox.image_ref
    assert sandbox.argv[0] == '/init-grader'
    assert deferred.node_resource_allocation is None
    validate_runtime_plan_requirements(deferred, workload_requirements_from_task(verifier_execution_task(prepared), trial))
    validate_prepared_sandbox_images(deferred, prepared)
    changed = sandbox.model_copy(update={'image_ref':plan.task_image_ref})
    with pytest.raises(ValueError, match='sandbox image does not match'):
        validate_prepared_sandbox_images(deferred.model_copy(update={'sidecars':(changed,)}),prepared)


def test_frozen_verifier_environment_reaches_real_controller_entrypoint(tmp_path, monkeypatch):
    import tomli_w

    from loom import service_execution_sandbox_task as module
    from loom.task_environment_handoff import (
        RESOLVED_TASK_ENVIRONMENT,
        RESOLVED_VERIFIER_ENVIRONMENT,
    )
    task, prepared, trial, plan = _independent_plan()
    source=tomli_w.dumps(task.model_dump(mode='json',exclude_none=True)).encode()
    (tmp_path/'task.toml').write_bytes(source)
    monkeypatch.setenv('LOOM_TASK_TRIAL_JSON',trial.model_dump_json())
    monkeypatch.setenv(RESOLVED_TASK_ENVIRONMENT,plan.main.environment[RESOLVED_TASK_ENVIRONMENT])
    monkeypatch.setenv(RESOLVED_VERIFIER_ENVIRONMENT,plan.main.environment[RESOLVED_VERIFIER_ENVIRONMENT])
    monkeypatch.setattr('sys.argv',['sandbox-task','verify-sandbox','--workspace',str(tmp_path)])
    observed=[]
    async def capture(workspace, received, received_trial):
        observed.append(received)
    monkeypatch.setattr(module,'run_verifier',capture)
    module.main()
    assert observed[0].verifier.environment == prepared.verifier.environment
    assert observed[0].environment == prepared.environment
    assert (tmp_path/'task.toml').read_bytes()==source


@pytest.mark.asyncio
async def test_native_verifier_uses_declared_cwd_env_and_preserves_numeric_zero(tmp_path, monkeypatch):
    import json

    from loom import service_execution_sandbox_task as module
    from loom.harbor_verifier_script import native_verifier_run_sh_bytes
    from loom.models.exec import ExecResult
    from tests.unit.test_service_execution_sandbox_task import Sandbox
    task, _, trial, _ = _independent_plan()
    from loom.models.harbor import HarborConversion
    task = task.model_copy(update={'upstream_conversion': HarborConversion(changes=()),
        'verifier':task.verifier.model_copy(update={
        'environment':task.verifier.environment.model_copy(update={'workdir':PurePosixPath('/grading')}),
        'args':{'script_path':'verifier/run.sh'},
    })})
    (tmp_path/'verifier').mkdir()
    (tmp_path/'verifier/run.sh').write_bytes(native_verifier_run_sh_bytes())
    (tmp_path/'tests').mkdir()
    (tmp_path/'tests/test.sh').write_text('echo 0 > /logs/verifier/reward.txt')
    driver=Sandbox()
    from loom.structured_artifacts import capture_structured_artifacts
    await driver.start()
    await capture_structured_artifacts(sources=(), drivers={'main': driver},
        workdir=task.environment.workdir, destination=tmp_path/'.loom/structured-artifacts.tar')
    await driver.stop()
    driver = Sandbox()
    monkeypatch.setattr(module,'sandbox_driver',lambda *_:driver)
    def check(cmd,user,cwd,env):
        assert cwd == PurePosixPath('/grading')
        assert env['GRADE_MODE']=='strict'
        assert env['LOOM_HARBOR_WORKDIR']=='/grading'
        assert driver.filesystem[PurePosixPath('/loom/verifier/task/tests/test.sh')]
        driver.filesystem[PurePosixPath(env['LOOM_VERIFIER_OUTPUT'])]=b'{"rewards":{"passed":0}}'
        return ExecResult(return_code=0,stdout=b'',stderr=b'',duration_sec=0)
    driver.exec_handler=check
    await module.run_verifier(tmp_path,task,trial)
    assert driver.quiesced and driver.state=='stopped'
    assert json.loads((tmp_path/'.loom/verifier/output.json').read_text())['rewards']=={'passed':0}


@pytest.mark.parametrize('damage', ['workdir', 'environment', 'missing'])
def test_frozen_verifier_environment_must_equal_prepared_grant(damage):
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1
    from loom.task_environment_handoff import (
        RESOLVED_VERIFIER_ENVIRONMENT,
        encode_resolved_task_environment,
    )
    from loom.verifier_runtime import validate_prepared_sandbox_images
    _, prepared, _, plan = _independent_plan()
    environment = dict(plan.main.environment)
    if damage == 'missing':
        del environment[RESOLVED_VERIFIER_ENVIRONMENT]
    else:
        update = {'workdir': PurePosixPath('/foreign')} if damage == 'workdir' else {'environment': {'OTHER': 'injected'}}
        environment[RESOLVED_VERIFIER_ENVIRONMENT] = encode_resolved_task_environment(
            prepared.verifier.environment.model_copy(update=update))
    altered = plan.model_copy(update={'main': plan.main.model_copy(update={'environment': environment})})
    altered = ExecutionRuntimePlanV1.model_validate(altered.canonical_payload())
    with pytest.raises(ValueError, match='runtime verifier environment'):
        validate_prepared_sandbox_images(altered, prepared)


def test_native_compose_guest_defers_independent_verifier_in_ordinary_class():
    from loom.execution_contract import workload_requirements_from_task
    from loom.execution_runtime_contract import (
        RuntimeHandoffInputV1,
        validate_runtime_plan_requirements,
    )
    from loom.task_sandbox_planner import compile_deferred_verifier_plan
    from loom.verifier_runtime import independent_verifier_rejections, verifier_execution_task
    from tests.unit.test_compose_image_inputs import prepared_plan
    task, trial, plan = prepared_plan()
    assert not independent_verifier_rejections(task, trial)
    assert plan.verifier_execution == 'separate_execution'
    sandbox, = plan.sidecars
    assert sandbox.role_name == 'task-sandbox'
    assert sandbox.guest_execution.capabilities == ('nested_docker',)
    validate_runtime_plan_requirements(plan, workload_requirements_from_task(task, trial))
    deferred = compile_deferred_verifier_plan(plan, task, verifier_timeout_seconds=150,
        handoff_input=RuntimeHandoffInputV1(manifest_sha256='sha256:'+'a'*64,file_count=1,total_bytes=1))
    verifier, = deferred.sidecars
    assert verifier.role_name == 'verifier-sandbox'
    assert verifier.guest_execution is None and verifier.guest_task_identity is None
    assert verifier.identity.run_as_user == 1002
    assert 'guest' not in deferred.execution_class_id
    validate_runtime_plan_requirements(deferred, workload_requirements_from_task(verifier_execution_task(task), trial))


def test_independent_verifier_uses_native_proxy_loopback_after_compose_agent(monkeypatch):
    from loom import service_execution_sandbox_task as module
    from loom.models.networking import PublicWeb
    from tests.unit.test_compose_image_inputs import prepared_plan
    task, trial, _ = prepared_plan()
    monkeypatch.setattr(module, '_frozen_network_policy', lambda: PublicWeb())
    monkeypatch.setenv('LOOM_TASK_EGRESS_PROXY', 'http://127.0.0.1:18080')
    monkeypatch.setenv('LOOM_SANDBOX_MAX_TRANSFER_BYTES', '1048576')
    verifier = module.sandbox_driver('verifier-sandbox', task, trial)
    agent = module.sandbox_driver('task-sandbox', task, trial)
    assert verifier._command_environment['HTTP_PROXY'] == 'http://127.0.0.1:18080'
    assert agent._command_environment['HTTP_PROXY'] == 'http://10.0.2.2:18080'


@pytest.mark.parametrize('name', ['POSTGRES_PASSWORD', 'CUSTOMER_ADMIN_TOKEN', 'SEED_HMAC_SECRET'])
def test_authored_fixture_credentials_stay_inside_private_verifier(name):
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1, ProcessPhaseV1
    from loom.task_environment_handoff import encode_resolved_task_environment
    from loom.verifier_runtime import independent_verifier_rejections
    _, prepared, trial, plan = _independent_plan()
    env = prepared.verifier.environment.model_copy(update={'environment': {name: 'public-test-fixture'}})
    assert encode_resolved_task_environment(env)
    configured = prepared.model_copy(update={'verifier': prepared.verifier.model_copy(update={
        'environment': env, 'environment_vars': {name:'public-test-fixture'}})})
    assert not independent_verifier_rejections(configured, trial)
    sidecar = plan.sidecars[0].model_copy(update={'environment': {name:'public-test-fixture'}})
    ExecutionRuntimePlanV1.model_validate(plan.model_copy(update={'sidecars':(sidecar,)}).canonical_payload())
    with pytest.raises(ValueError, match='forbidden name'):
        ProcessPhaseV1(role='agent', argv=('true',), timeout_seconds=1, environment={name:'public-test-fixture'})


@pytest.mark.parametrize('name', ['LOOM_GATEWAY_TOKEN', 'HTTP_PROXY', 'KUBECONFIG'])
def test_authored_verifier_cannot_override_platform_environment(name):
    from loom.task_environment_handoff import encode_resolved_task_environment
    _, prepared, _, _ = _independent_plan()
    with pytest.raises(ValueError, match='resolved_task_environment_invalid'):
        encode_resolved_task_environment(prepared.verifier.environment.model_copy(update={'environment': {name:'fixture'}}))
