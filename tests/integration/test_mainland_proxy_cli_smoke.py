"""Real local CLI -> LocalRunner -> DockerDriver smoke, with a disposable dataset."""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.docker


@pytest.mark.parametrize("enabled", [False, True])
def test_cli_mainland_proxy_real_task(monkeypatch, tmp_path, enabled):
    image=os.environ.get("LOOM_TEST_MAINLAND_PROXY_IMAGE")
    profile=os.environ.get("LOOM_TEST_MAINLAND_PROXY_PROFILE_DIR")
    endpoint=os.environ.get("LOOM_TEST_MAINLAND_PROXY_DOCKER_HOST")
    if not image or not profile or not endpoint:
        pytest.skip("requires explicit local Docker image/profile/endpoint")
    if not endpoint.startswith("unix://"):
        pytest.fail("smoke requires an explicit local Docker socket")
    import loom_cli.run_cmd as command
    from loom.models.task import TaskConfig
    from loom_cli.__main__ import main
    from loom_cli.task_loader import LoadedTask

    task=tmp_path/"task"
    (task/"solution").mkdir(parents=True)
    (task/"tests").mkdir()
    (task/"instruction.md").write_text("Run the public command probes and record their results.")
    (task/"solution/solve.sh").write_text("#!/bin/sh\nset -eu\npython /workspace/probe.py\n")
    probe=r"""
import json,os,subprocess,tempfile
from pathlib import Path
work=Path(tempfile.mkdtemp())
env=dict(os.environ,GIT_TERMINAL_PROMPT='0',PIP_CONFIG_FILE='/dev/null',npm_config_cache=str(work/'cache'),npm_config_fetch_retries='0',npm_config_fetch_timeout='15000')
commands={
'git':['git','clone','--depth','1','https://github.com/octocat/Hello-World.git',str(work/'repo')],
'pip':['python','-m','pip','install','--no-cache-dir','--no-deps','--no-compile','--retries','0','--timeout','15','--index-url','https://pypi.org/simple','--target',str(work/'python'),'six==1.17.0'],
'npm':['npm','install','--ignore-scripts','--no-audit','--no-fund','--registry','https://registry.npmjs.org','is-number@7.0.0']}
results={}
for name,args in commands.items():
 p=subprocess.run(args,cwd=work,env=env,capture_output=True,text=True,timeout=45)
 results[name]={'rc':p.returncode,'stderr':p.stderr[-1000:]}
assert (results['git']['rc'] != 0) == (env['EXPECT_MAINLAND']=='1'),results
assert results['pip']['rc']==results['npm']['rc']==0,results
assert (work/'python/six.py').is_file()
assert (work/'node_modules/is-number/package.json').is_file()
Path('/workspace/result.json').write_text(json.dumps(results))
"""
    (task/"probe.py").write_text(probe)
    verifier=r"""#!/bin/sh
set -eu
python - <<'CHECK'
import json,os,subprocess
from pathlib import Path
r=json.loads(Path('/workspace/result.json').read_text())
p=subprocess.run(['curl','-fsS','--connect-timeout','3','--max-time','8','https://github.com'],capture_output=True)
assert (p.returncode!=0)==(os.environ['EXPECT_MAINLAND']=='1')
assert r['pip']['rc']==r['npm']['rc']==0
Path(os.environ['LOOM_VERIFIER_OUTPUT']).write_text(json.dumps({'rewards':{'passed':1.0}}))
CHECK
"""
    (task/"tests/verify.sh").write_text(verifier)
    config=TaskConfig.model_validate({"schema_version":"1","task":{"id":"mainland-smoke/commands","name":"commands"},"environment":{"os":"linux","docker_image":image,"environment":{"EXPECT_MAINLAND":str(int(enabled))}},"agent":{"name":"oracle"},"verifier":{"name":"script","env_mode":"separate","args":{"script_path":"/workspace/tests/verify.sh"}},"steps":[{"name":"solve","artifacts":["result.json"]}]})
    monkeypatch.setattr(command,"load_tasks",lambda **kwargs:iter([LoadedTask(task,config,"1"*64,"MIT")]))
    monkeypatch.setattr(command,"load_config",lambda:SimpleNamespace(tokens={},local_providers={}))
    for key in ("ANTHROPIC_API_KEY","OPENAI_API_KEY","GOOGLE_API_KEY","LOOM_SERVER_URL"):
        monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv("DOCKER_HOST",endpoint)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_IMAGE",image)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_PROFILE_DIR",profile)
    monkeypatch.setenv("LOOM_MAINLAND_PROXY_BINARY","/usr/local/bin/mihomo")
    output=tmp_path/"output"
    argv=["run","--dataset","mainland-smoke","--agent","oracle","--backend","docker","--output-dir",str(output),"--json"]
    argv.append("--mainland-proxy" if enabled else "--no-mainland-proxy")
    assert main(argv)==0
    events=list(output.glob("*/events.jsonl"))
    assert len(events)==1
    start=next(json.loads(line) for line in events[0].read_text().splitlines() if json.loads(line).get("kind")=="trial_start")
    atif=json.loads((events[0].parent/"atif.json").read_text())
    if enabled:
        assert start["tool_network_profile"]["applied"] is True
        assert atif["metadata"]["tool_network_profile"]["loaded_rule_counts"]["gfw"]==4443
    else:
        assert "tool_network_profile" not in start
        assert "tool_network_profile" not in atif["metadata"]
