"""Explicit opt-in Docker smoke for the real mihomo gateway implementation."""
from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import pytest

from loom.driver.mainland_proxy import MainlandProxyGateway, MainlandProxySettings

pytestmark = pytest.mark.docker


async def test_real_gateway_blocks_git_and_keeps_package_installations(tmp_path: Path) -> None:
    image = os.environ.get("LOOM_TEST_MAINLAND_PROXY_IMAGE")
    profile = os.environ.get("LOOM_TEST_MAINLAND_PROXY_PROFILE_DIR")
    endpoint = os.environ.get("LOOM_TEST_MAINLAND_PROXY_DOCKER_HOST")
    if not image or not profile or not endpoint:
        pytest.skip("requires explicit local Docker image, profile and endpoint")
    if not endpoint.startswith("unix://"):
        pytest.fail("this disposable smoke only supports a local Docker socket")
    import docker
    client = docker.DockerClient(base_url=endpoint, timeout=30)
    settings = MainlandProxySettings(image, Path(profile), binary="/usr/local/bin/mihomo")
    gateway = MainlandProxyGateway(uuid4(), settings, tmp_path / "gateway", client_factory=lambda: docker.DockerClient(base_url=endpoint, timeout=30))
    tool = None
    script = '''
import json, os, subprocess, tempfile
from pathlib import Path
work=Path(tempfile.mkdtemp())
env=dict(os.environ, HOME=str(work), GIT_TERMINAL_PROMPT="0", PIP_CONFIG_FILE="/dev/null", npm_config_cache=str(work/"cache"), npm_config_fetch_retries="0", npm_config_fetch_timeout="15000")
commands={
 "git_github":["git","clone","--depth","1","https://github.com/octocat/Hello-World.git",str(work/"repo")],
 "pip":["python","-m","pip","install","--no-cache-dir","--no-deps","--no-compile","--retries","0","--timeout","15","--index-url","https://pypi.org/simple","--target",str(work/"python"),"six==1.17.0"],
 "npm":["npm","install","--ignore-scripts","--no-audit","--no-fund","--registry","https://registry.npmjs.org","is-number@7.0.0"],
 "controller_via_proxy":["curl","--proxy",env["HTTP_PROXY"],"--noproxy","","--connect-timeout","2","--max-time","4","-fsS","http://127.0.0.1:9090/configs"],
 "direct":["curl","--noproxy","*","--connect-timeout","2","--max-time","4","-fsS","https://pypi.org/simple/six/"],
}
results={}
for name,command in commands.items():
 p=subprocess.run(command,cwd=work,env=env,capture_output=True,text=True,timeout=50)
 results[name]={"returncode":p.returncode,"stdout":p.stdout[-2000:],"stderr":p.stderr[-2000:]}
assert (work/"python/six.py").is_file()
assert (work/"node_modules/is-number/package.json").is_file()
print(json.dumps(results))
'''
    try:
        await gateway.start()
        tool = client.containers.create(image, entrypoint=["python"], command=["-c", script], network=gateway.network_name, environment=gateway.environment, cap_drop=["ALL"], security_opt=["no-new-privileges"], read_only=True, tmpfs={"/tmp":"rw,nosuid,nodev,size=128m"}, mem_limit="512m", pids_limit=128)
        tool.start()
        status = tool.wait(timeout=150)
        raw = tool.logs().decode()
        assert status["StatusCode"] == 0, raw
        results = json.loads(raw)
        assert results["git_github"]["returncode"] != 0
        assert results["pip"]["returncode"] == results["npm"]["returncode"] == 0
        assert results["controller_via_proxy"]["returncode"] != 0
        assert results["direct"]["returncode"] != 0
        (tmp_path / "commands.json").write_text(json.dumps(results, indent=2))
    finally:
        if tool is not None:
            tool.remove(force=True, v=True)
        await gateway.stop()
        client.close()
    assert "RuleSet(greatfire) using REJECT" in (tmp_path / "gateway/mihomo.log").read_text()


async def test_net_admin_tool_cannot_route_around_proxy(tmp_path: Path) -> None:
    image = os.environ.get("LOOM_TEST_MAINLAND_PROXY_IMAGE")
    profile = os.environ.get("LOOM_TEST_MAINLAND_PROXY_PROFILE_DIR")
    endpoint = os.environ.get("LOOM_TEST_MAINLAND_PROXY_DOCKER_HOST")
    if not image or not profile or not endpoint:
        pytest.skip("requires explicit local Docker image, profile and endpoint")
    if not endpoint.startswith("unix://"):
        pytest.fail("this disposable smoke only supports a local Docker socket")
    import docker
    client = docker.DockerClient(base_url=endpoint, timeout=30)
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings(image, Path(profile), binary="/usr/local/bin/mihomo"), tmp_path / "gateway", client_factory=lambda: docker.DockerClient(base_url=endpoint, timeout=30))
    tool = None
    try:
        await gateway.start()
        # Match the real DockerDriver's task-owned namespace capability.
        tool = client.containers.run(image, entrypoint=["sleep"], command=["30"], network=gateway.network_name, detach=True, cap_add=["NET_ADMIN"])
        gateway_ip = urlsplit(gateway.environment["HTTP_PROXY"]).hostname
        route = tool.exec_run(["ip", "route", "add", "default", "via", gateway_ip])
        assert route.exit_code == 0, route.output.decode()
        address = socket.gethostbyname("pypi.org")
        direct = tool.exec_run(["curl", "--noproxy", "*", "--resolve", f"pypi.org:443:{address}", "--connect-timeout", "2", "--max-time", "4", "-fsS", "https://pypi.org/simple/six/"])
        assert direct.exit_code != 0, direct.output.decode()[:1000]
        (tmp_path / "route-boundary.json").write_text(json.dumps({"task_route_add_returncode":route.exit_code,"direct_returncode":direct.exit_code,"direct_output":direct.output.decode()},indent=2))
    finally:
        if tool is not None:
            tool.remove(force=True, v=True)
        await gateway.stop()
        client.close()


async def test_task_policy_reload_changes_real_commands_and_restores_public(tmp_path: Path) -> None:
    """One running gateway must enforce every phase, including a restore."""
    image = os.environ.get("LOOM_TEST_MAINLAND_PROXY_IMAGE")
    profile = os.environ.get("LOOM_TEST_MAINLAND_PROXY_PROFILE_DIR")
    endpoint = os.environ.get("LOOM_TEST_MAINLAND_PROXY_DOCKER_HOST")
    if not image or not profile or not endpoint:
        pytest.skip("requires explicit local Docker image, profile and endpoint")
    if not endpoint.startswith("unix://"):
        pytest.fail("this disposable smoke only supports a local Docker socket")
    import docker

    from loom.models.networking import Allowlist, NoNetwork, Public, WebAllowlist, WebDestination

    client = docker.DockerClient(base_url=endpoint, timeout=30)
    gateway = MainlandProxyGateway(uuid4(), MainlandProxySettings(image, Path(profile), binary="/usr/local/bin/mihomo"), tmp_path / "gateway", client_factory=lambda: docker.DockerClient(base_url=endpoint, timeout=30))
    tool = None
    results: dict[str, dict[str, object]] = {}
    try:
        await gateway.start()
        tool = client.containers.run(image, entrypoint=["sleep"], command=["180"], network=gateway.network_name, detach=True, environment=gateway.environment, cap_drop=["ALL"], read_only=True, tmpfs={"/tmp":"rw,nosuid,nodev,size=128m"})

        def execute(name: str, args: list[str]) -> int:
            assert tool is not None
            result = tool.exec_run(args)
            results[name] = {"returncode": result.exit_code, "output": result.output.decode()[-3000:]}
            return result.exit_code

        def pip_install(name: str) -> int:
            return execute(name, ["python", "-m", "pip", "install", "--disable-pip-version-check", "--no-cache-dir", "--no-deps", "--no-compile", "--retries", "0", "--timeout", "10", "--index-url", "https://pypi.org/simple", "--target", f"/tmp/{name}", "six==1.17.0"])

        await gateway.set_network_policy(Public())
        assert pip_install("public_before") == 0
        await gateway.set_network_policy(NoNetwork())
        assert pip_install("no_network") != 0
        await gateway.set_network_policy(Allowlist(domains=("pypi.org",)))
        assert execute("allow_pypi", ["curl", "-fsS", "--max-time", "10", "https://pypi.org/simple/six/"]) == 0
        assert execute("deny_npm", ["curl", "-fsS", "--max-time", "5", "https://registry.npmjs.org/is-number"]) != 0
        # Index access alone does not authorize the separate wheel host.
        assert pip_install("allow_index_not_wheel") != 0
        await gateway.set_network_policy(WebAllowlist(destinations=(WebDestination(host="files.pythonhosted.org",protocol="https"), WebDestination(host="pypi.org",protocol="https"))))
        assert pip_install("web_https_allowed") == 0
        assert execute("web_http_denied", ["curl", "-fsS", "--max-time", "5", "http://pypi.org/simple/six/"]) != 0
        await gateway.set_network_policy(Public())
        assert pip_install("public_restored") == 0
        assert execute("simulation_still_blocks_github", ["git", "clone", "--depth", "1", "https://github.com/octocat/Hello-World.git", "/tmp/github"]) != 0
        assert gateway.metadata["effective_task_policy"] == {"kind":"public"}
        (tmp_path / "phase-commands.json").write_text(json.dumps(results, indent=2))
    finally:
        if tool is not None:
            tool.remove(force=True, v=True)
        await gateway.stop()
        client.close()
