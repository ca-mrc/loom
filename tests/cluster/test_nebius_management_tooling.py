"""Install the real locked tooling bundle, outside checkout/ambient imports."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from scripts.ops.nebius_certificate_gateway import run_private
from scripts.ops.nebius_ingress_rollout import build_wheels
from scripts.ops.nebius_management_gateway import prepare_release
from scripts.ops.nebius_management_rollout import build_bundle

from tests.ops.test_nebius_application_setup import setup_request as setup_request
from tests.ops.test_nebius_management_cloud_scope import cloud as cloud
from tests.ops.test_nebius_management_entry import entry_inputs as entry_inputs
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_prerequisites import checks as checks
from tests.ops.test_nebius_management_retirement import retirement_request as retirement_request
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_management_render import (
    application_management_inputs as application_management_inputs,
)
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.skipif(os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
                                reason="explicit disposable cluster tooling qualification")


@pytest.mark.timeout(360)
def test_installed_management_bundle_imports_and_renders_without_checkout(entry_inputs, retirement_request, tmp_path):
    operation, _, _, _ = entry_inputs
    root = Path(__file__).resolve().parents[2]
    tooling = tmp_path / "tooling"
    tooling.mkdir(mode=0o700)
    binary = shutil.which("uv")
    assert binary is not None
    uv = Path(binary)
    wheels = build_wheels(tooling, uv=uv)
    requirements = tooling / "requirements.txt"
    subprocess.run([str(uv), "export", "--locked", "--no-default-groups", "--extra", "cluster", "--group", "nebius-certificates",
        "--no-emit-workspace", "--format", "requirements-txt", "--no-header", "--quiet", "--output-file", str(requirements)],
        cwd=root, check=True, timeout=60)
    content = build_bundle(operation, uv=uv, requirements=requirements, wheels=wheels)
    release = prepare_release(content)
    code = ("import json,sys; from pathlib import Path; sys.path.insert(0,sys.argv[1]); "
            "from scripts.ops.nebius_management_entry import load_inputs; "
            "from scripts.ops.nebius_management_install import render_installation; "
            "_,request,_=load_inputs(json.loads(Path(sys.argv[2]).read_bytes())); "
            "rendered=render_installation(request); print(json.dumps({'revision':rendered.revision,'phases':list(rendered.files)}))")
    result = json.loads(run_private([str(release / "venv/bin/python"), "-I", "-c", code,
                                    str(release), str(release / "operation.json")], timeout=30))
    assert result["revision"].startswith("sha256:")
    assert "85-backup-verify.yaml" in result["phases"] and "40-services.yaml" in result["phases"]
    # Qualify the new diagnostic from the installed locked bundle, not checkout
    # imports or the developer's editable packages. No cluster authority is used.
    request = retirement_request[0]
    from dataclasses import asdict

    fixture = tmp_path / "diagnostic-render.json"
    fixture.write_text(json.dumps({"binding": asdict(request.binding),
        "deployment": request.deployment.model_dump(mode="json"), "candidate": request.candidate,
        "profile": request.profile, "targets": [target.model_dump(mode="json") for target in request.targets]}))
    code = """
import json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_retirement import RetirementInstallRequest
from scripts.ops.nebius_management_retirement_diagnostic import diagnostic_documents
from scripts.ops.nebius_management_retirement_diagnostic_live import HTTPSRetirementDiagnosticAPI
from scripts.ops.nebius_management_retirement_recovery import recovery_documents
from scripts.ops.nebius_management_retirement_recovery_live import HTTPSRetirementRecoveryAPI
from loom_service.environment_management.deployment import ManagementDeployment
from loom_service.environment_management.retirement import RetirementTarget
raw = json.loads(Path(sys.argv[2]).read_bytes())
request = RetirementInstallRequest(ManagementBinding(**raw['binding']),
    ManagementDeployment.model_validate(raw['deployment']), raw['candidate'], raw['profile'],
    tuple(RetirementTarget.model_validate(target) for target in raw['targets']), Path(sys.argv[1]))
job, = diagnostic_documents(request).values()
command = job['spec']['template']['spec']['containers'][0]['command']
probe = {'__name__': 'qualified_probe_import'}
exec(command[2], probe)
recovery_job, = recovery_documents(request, original_job_uid='18718d96-d389-40b3-a79b-11489924d0d9')['job'].values()
recovery = {'__name__': 'qualified_recovery_import'}
exec(recovery_job['spec']['template']['spec']['containers'][0]['command'][2], recovery)
print(json.dumps({'command': command[:2], 'schema': probe['SCHEMA'],
    'settings_path': str(probe['SETTINGS_PATH']), 'adapter': HTTPSRetirementDiagnosticAPI.__name__,
    'recovery_schema': recovery['SCHEMA'], 'recovery_adapter': HTTPSRetirementRecoveryAPI.__name__,
    'embedded_startup_schema': recovery['_startup'].SCHEMA}))
"""
    result = json.loads(run_private([str(release / "venv/bin/python"), "-I", "-c", code,
                                    str(release), str(fixture)], timeout=30))
    assert result == {"command": ["python", "-c"], "schema": "loom.nebius-retirement-startup-probe.v1",
        "settings_path": "/var/run/loom-retirement/retirement.json", "adapter": "HTTPSRetirementDiagnosticAPI",
        "recovery_schema": "loom.nebius-retirement-recovery-report.v1", "recovery_adapter": "HTTPSRetirementRecoveryAPI",
        "embedded_startup_schema": "loom.nebius-retirement-startup-probe.v1"}
    assert prepare_release(content) == release
