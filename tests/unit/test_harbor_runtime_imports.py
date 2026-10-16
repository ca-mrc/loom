"""The native controller does not import worker-only storage or DB clients."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_native_controller_import_has_no_storage_or_database_side_effects() -> None:
    subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import sys; import loom.service_execution_sandbox_task; "
            "assert not any(name in sys.modules for name in "
            "('loom.db.schema', 'loom.trajectory.writer', 'loom.agent.litellm', "
            "'loom.security.secret_store', 'boto3', 'sqlalchemy', "
            "'loom.execution_image_admission', 'rfc8785'))",
        ],
        check=True,
    )


def test_lazy_security_exports_preserve_public_objects() -> None:
    import loom.security as security
    from loom.security import secret_store

    for name in security.__all__:
        assert getattr(security, name) is getattr(secret_store, name)
    try:
        _ = security.not_a_security_export
    except AttributeError:
        pass
    else:
        raise AssertionError("unknown module attributes must still raise AttributeError")


def test_unknown_agent_error_does_not_expose_its_message() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import runpy; from unittest.mock import patch; from loom.errors import AgentError; "
            "patcher = patch('argparse.ArgumentParser.parse_args', "
            "side_effect=AgentError('private-token-fixture')); patcher.start(); "
            "runpy.run_module('loom.service_execution_sandbox_task', run_name='__main__')",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    assert result.stderr == "isolated execution failed (AgentError)\n"
    assert result.stdout == ""


def test_validation_error_stderr_is_location_and_type_only() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import sys\n"
            f"sys.path.insert(0, {str(Path(__file__).resolve().parents[2] / 'src')!r})\n"
            "import runpy\n"
            "from unittest.mock import patch\n"
            "from pydantic import BaseModel, ConfigDict, ValidationError\n"
            "class Probe(BaseModel):\n"
            "    model_config = ConfigDict(extra='forbid')\n"
            "    name: str\n"
            "try:\n"
            "    Probe.model_validate({\n"
            "        'name': 'ok',\n"
            "        'token': 'private-token-fixture',\n"
            "        'trial_json': '{\"secret\":\"private-token-fixture\"}',\n"
            "    })\n"
            "except ValidationError as exc:\n"
            "    error = exc\n"
            "patch('argparse.ArgumentParser.parse_args', side_effect=error).start()\n"
            "runpy.run_module('loom.service_execution_sandbox_task', run_name='__main__')\n",
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("isolated execution failed (ValidationError): ")
    assert "token:extra_forbidden" in result.stderr
    assert "trial_json:extra_forbidden" in result.stderr
    assert "private-token-fixture" not in result.stderr
    assert "Extra inputs" not in result.stderr
    assert result.stderr.count("\n") == 1


def test_safe_sandbox_rpc_error_survives_native_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-B", "-c",
         "import runpy,httpx; from unittest.mock import patch; "
         "from loom.driver.service_sandbox import SandboxRPCError; "
         "request=httpx.Request('POST','http://private-endpoint-fixture', "
         "content='private-command-fixture'); "
         "response=httpx.Response(409,request=request, "
         "headers={'X-Loom-Sandbox-Error':'process_owner_mismatch'}, "
         "text='private-response-fixture'); "
         "error=SandboxRPCError('/stop-processes',httpx.HTTPStatusError("
         "'private-exception-fixture',request=request,response=response)); "
         "patcher=patch('argparse.ArgumentParser.parse_args',side_effect=error); "
         "patcher.start(); "
         "runpy.run_module('loom.service_execution_sandbox_task',run_name='__main__')"],
        text=True, capture_output=True,
    )
    assert result.returncode == 1
    assert result.stderr == (
        "isolated execution failed (SandboxRPCError): "
        "sandbox stop_processes failed (HTTP 409; process_owner_mismatch)\n"
    )
    assert result.stdout == ""
