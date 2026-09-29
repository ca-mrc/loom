"""Additive native-DNS recovery; original retirement rendering stays frozen."""
from __future__ import annotations

import copy
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import _setup_defaulted
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_retirement import RetirementInstallRequest, retirement_documents
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
)

from loom.nebius_platform_render import digest


def recovery_documents(request: RetirementInstallRequest, *, original_job_uid: str) -> dict[str, dict[str, dict[str, Any]]]:
    if (not isinstance(original_job_uid, str) or not UUID(original_job_uid).int
            or str(UUID(original_job_uid)) != original_job_uid):
        raise ValueError("recovery_original_job_unqualified")
    original, = [doc for doc in retirement_documents(request)["job"].values() if doc["kind"] == "Job"]
    root = Path(__file__).parent
    startup = (root / "nebius_retirement_startup_probe.py").read_text()
    runner = (root / "nebius_retirement_recovery_runner.py").read_text()
    # Execute both exact checked-in sources in the old image, without importing
    # a new operator wheel or accepting caller-supplied source/modules.
    source = ("import types\n_startup = types.ModuleType('loom_retirement_startup_probe')\n"
              + "exec(" + repr(startup) + ", _startup.__dict__)\n"
              + "exec(" + repr(runner) + ", globals())\n")
    if not 0 < len(source.encode()) <= 131072:
        raise ValueError("recovery_source_size")
    name = "loom-retirement-recovery-" + digest({"original": original, "job_uid": original_job_uid, "source": source})[7:19]
    job = copy.deepcopy(original)
    job["metadata"].update(name=name, annotations={"loom.nebius/recovery-of-job-uid": original_job_uid})
    recovery_label = {"loom.nebius/retirement-recovery": name}
    job["metadata"]["labels"].update(recovery_label)
    job["spec"]["template"]["metadata"]["labels"].update(recovery_label)
    job["spec"]["template"]["spec"]["containers"][0]["command"] = ["python", "-c", source]
    selector = {"loom.nebius/management-installation": request.binding.installation_id, **recovery_label}
    network = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {
        "name": name + "-dns", "namespace": request.binding.namespace, "labels": selector}, "spec": {
        "podSelector": {"matchLabels": selector}, "policyTypes": ["Egress"], "egress": [{
            "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                "podSelector": {"matchExpressions": [{"key": "k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]}]}}],
            "ports": [{"protocol": "TCP", "port": 53}, {"protocol": "UDP", "port": 53}],
        }]}}
    return {"network": {_key(network): network}, "job": {_key(job): job}}


def stage_recovery(*, request: RetirementInstallRequest, original_job_uid: str, api: ManagementStageAPI,
                   state_dir: Path, anchor_dir: Path) -> dict[str, Any]:
    """One journal qualifies both objects before writes; DNS precedes execution.

    The caller qualifies original and diagnostic receipts plus live DNS first.
    A retained independent anchor prevents any missing journal from reopening
    creation. A receipt is not runtime completion or reservation-release proof.
    """
    phases = recovery_documents(request, original_job_uid=original_job_uid)
    documents = {key: doc for phase in phases.values() for key, doc in phase.items()}
    revision = digest(documents)
    identity = {"schema": "loom.nebius-management-retirement-recovery.v1", "binding": asdict(request.binding),
        "revision": revision, "state_dir": str(state_dir)}
    try:
        if (any(not path.is_absolute() or path != path.resolve() for path in (state_dir, anchor_dir))
                or state_dir == anchor_dir or state_dir in anchor_dir.parents or anchor_dir in state_dir.parents):
            raise ValueError
        with private_state._locked_state(anchor_dir):
            marker = anchor_dir / (request.binding.installation_id + ".json")
            progress = state_dir / "recovery.json"
            if marker.exists() or marker.is_symlink():
                if (json.loads(private_state._private_read(marker)) != identity
                        or json.loads(private_state._private_read(progress)) != identity
                        or not (state_dir / "resources/stage.json").is_file()):
                    raise ValueError
            else:
                if state_dir.exists() or state_dir.is_symlink():
                    raise ValueError
                private_state._atomic_json(marker, identity)
            with private_state._locked_state(state_dir):
                private_state._atomic_json(progress, identity)
                return _stage_fixed_documents(documents=documents, revision=revision, phase="retirement-recovery",
                    binding=request.binding, api=api, state_dir=state_dir / "resources", default_document=_setup_defaulted)
    except ManagementStageError:
        raise
    except Exception:
        raise ManagementStageError("retirement recovery evidence unavailable; preserve state") from None
