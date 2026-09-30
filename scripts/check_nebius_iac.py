from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
NEBIUS_ROOT = REPO_ROOT / "deploy" / "terraform" / "nebius"
TOPOLOGY_PATH = REPO_ROOT / "config" / "service-execution-topology.json"
PHYSICAL_BINDING_PATH = REPO_ROOT / "config" / "nebius-runtime-physical-binding.json"
CAPACITY_POLICY_PATH = REPO_ROOT / "deploy" / "k8s" / "nebius-development-capacity-policy.json"


class ContractError(RuntimeError):
    pass


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContractError(f"{path}: expected a JSON object")
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _backend_value(text: str, name: str, path: Path) -> str:
    match = re.search(rf'^\s*{re.escape(name)}\s*=\s*"([^"]+)"\s*$', text, re.MULTILINE)
    if match is None:
        raise ContractError(f"{path}: missing quoted {name}")
    return match.group(1)


def check_nebius_iac(
    *,
    repo_root: Path = REPO_ROOT,
    nebius_root: Path | None = None,
) -> None:
    root = nebius_root or repo_root / "deploy" / "terraform" / "nebius"
    topology = _load_json(repo_root / "config" / "service-execution-topology.json")
    targets = topology.get("targets")
    if not isinstance(targets, list):
        raise ContractError("execution topology must contain a targets list")
    topology_targets = {
        str(target["target_id"]): target
        for target in targets
        if isinstance(target, dict) and target.get("provider") == "nebius"
    }

    target_paths = sorted((root / "targets").glob("*.tfvars.json.example"))
    _require(
        len(target_paths) == 1,
        f"expected exactly 1 shared-cluster example, found {len(target_paths)}",
    )
    _require(
        len(topology_targets) == 3,
        f"expected exactly 3 environment bindings, found {len(topology_targets)}",
    )
    path = target_paths[0]
    _require(
        path.name == "development-eu-north1.tfvars.json.example",
        f"{path}: the live development state anchor must be retained during convergence",
    )
    document = _load_json(path)
    target = document.get("target")
    if not isinstance(target, dict):
        raise ContractError(f"{path}: target must be an object")
    _require(
        target.get("target_id") == "nebius-eu-north1-development",
        f"{path}: target_id must retain the live state/resource anchor",
    )
    cluster_scope_ids = {
        str(item.get("cluster_scope_id", "")) for item in topology_targets.values()
    }
    _require(
        len(cluster_scope_ids) == 1 and "" not in cluster_scope_ids,
        "topology targets must share one cluster_scope_id",
    )
    cluster_scope_id = next(iter(cluster_scope_ids))
    _require(
        target.get("cluster_scope_id") == cluster_scope_id,
        f"{path}: cluster_scope_id must match topology",
    )
    physical_binding_path = repo_root / "config" / PHYSICAL_BINDING_PATH.name
    physical_binding = _load_json(physical_binding_path)
    _require(
        physical_binding.get("schema_version") == "loom.nebius-runtime-physical-binding.v1",
        f"{physical_binding_path}: unsupported physical binding schema",
    )
    _require(
        physical_binding.get("cluster_scope_id") == cluster_scope_id,
        f"{physical_binding_path}: cluster_scope_id must match topology",
    )
    _require(
        physical_binding.get("region") == target.get("region"),
        f"{physical_binding_path}: region must match Terraform",
    )
    for name, prefix in (
        ("project_id", "project-"),
        ("quota_parent_id", "tenant-"),
        ("execution_node_group_id", "mk8snodegroup-"),
    ):
        value = str(physical_binding.get(name, ""))
        _require(
            value.startswith(prefix) and re.fullmatch(r"[a-z][a-z0-9-]{2,159}", value) is not None,
            f"{physical_binding_path}: {name} must be a concrete Nebius resource id",
        )
    collector_path = repo_root / "deploy" / "k8s" / "nebius-capacity-collector.yaml"
    collector_text = collector_path.read_text(encoding="utf-8")
    for name in ("project_id", "quota_parent_id", "execution_node_group_id", "region"):
        _require(
            str(physical_binding[name]) in collector_text,
            f"{collector_path}: source runtime binding must contain {name}",
        )
    regions = {str(item.get("region", "")) for item in topology_targets.values()}
    failure_domains = {str(item.get("failure_domain", "")) for item in topology_targets.values()}
    _require(
        regions == {target.get("region")},
        f"{path}: all bindings must use the shared cluster region",
    )
    _require(
        failure_domains == {target.get("failure_domain")},
        f"{path}: all bindings must use one failure domain",
    )

    bindings = target.get("environment_bindings")
    if not isinstance(bindings, dict):
        raise ContractError(f"{path}: environment_bindings must be an object")
    _require(
        set(bindings) == {"development", "staging", "production"},
        f"{path}: all three environment bindings are required",
    )
    for environment, binding in bindings.items():
        if not isinstance(binding, dict):
            raise ContractError(f"{path}: {environment} binding must be an object")
        target_id = str(binding.get("target_id", ""))
        topology_target = topology_targets.get(target_id)
        if topology_target is None:
            raise ContractError(f"{path}: target_id {target_id!r} is absent from topology")
        _require(
            topology_target.get("environment") == environment,
            f"{path}: {environment} target environment must match topology",
        )
        _require(
            binding.get("namespace_name") == topology_target.get("namespace_name"),
            f"{path}: {environment} namespace_name must match topology",
        )
        _require(
            binding.get("evidence_prefix") == target_id,
            f"{path}: {environment} evidence prefix must equal target_id",
        )

    _require(
        document.get("tenant_id") == "tenant-REPLACE",
        f"{path}: example tenant must remain a placeholder",
    )
    _require(
        str(document.get("project_id", "")).startswith("project-REPLACE-"),
        f"{path}: example project must remain a placeholder",
    )
    for name in ("nebius_profile", "project_id", "evidence_bucket_name"):
        _require(bool(str(document.get(name, ""))), f"{path}: {name} is required")
    _require(
        document.get("nebius_profile") == "loom-development-eu-north1-terraform-automation",
        f"{path}: Terraform must use the non-expiring service-account profile",
    )
    for name in (
        "deployment_access_public_pool_id",
        "deployment_access_ssh_public_key",
        "deployment_access_image_id",
    ):
        _require(bool(str(document.get(name, ""))), f"{path}: {name} is required")
    for name in ("network_cidr", "service_cidr"):
        _require(bool(str(target.get(name, ""))), f"{path}: {name} is required")
    cidrs = target.get("public_control_plane_cidrs")
    if not isinstance(cidrs, list):
        raise ContractError(f"{path}: public_control_plane_cidrs must be a list")
    _require("0.0.0.0/0" not in cidrs, f"{path}: public control plane may never allow 0.0.0.0/0")
    _require(
        int(target.get("execution_min_nodes", -1)) == 0,
        f"{path}: shared execution baseline must scale to zero",
    )

    capacity_path = repo_root / "deploy" / "k8s" / CAPACITY_POLICY_PATH.name
    capacity = _load_json(capacity_path)
    _require(
        capacity.get("schema_version") == "loom.nebius-development-capacity.v1",
        f"{capacity_path}: unsupported capacity policy schema",
    )
    _require(
        capacity.get("target_id") == target.get("target_id"),
        f"{capacity_path}: target_id must match the Terraform target",
    )
    _require(
        int(capacity.get("provider_required_quota_vcpu_millis", 0)) == 512_000,
        f"{capacity_path}: provider vCPU quota target must be 512 vCPU",
    )
    _require(
        int(capacity.get("provider_required_quota_nodes", 0)) == 16,
        f"{capacity_path}: provider VM quota target must preserve replacement headroom",
    )
    _require(
        int(capacity.get("provider_current_quota_vcpu_millis", 0)) == 200_000
        and int(capacity.get("provider_current_quota_nodes", 0)) == 12,
        f"{capacity_path}: current provider quota snapshot must be explicit",
    )
    _require(
        int(capacity.get("target_concurrency", 0)) == 200
        and int(capacity.get("target_execution_max_nodes", 0)) == 10,
        f"{capacity_path}: requested target must retain 200-task capacity",
    )
    _require(
        capacity.get("target_execution_preset") == "48vcpu-192gb",
        f"{capacity_path}: requested target must retain the 48-vCPU node shape",
    )
    _require(
        int(capacity.get("accepted_concurrency", 0)) == 56
        and int(capacity.get("task_cpu_millis", 0)) == 2_000,
        f"{capacity_path}: current inventory must cover 56 concurrent 2-vCPU tasks",
    )
    admission_policies = capacity.get("admission_policies")
    if not isinstance(admission_policies, list):
        raise ContractError(f"{capacity_path}: admission_policies must be a list")
    _require(
        admission_policies
        == [
            {
                "scope_kind": "global",
                "scope_key": "*",
                "max_concurrent": 56,
                "enabled": True,
                "reason": (
                    "Current-inventory Nebius acceptance permits 56 concurrent "
                    "service-execution leases."
                ),
            },
            {
                "scope_kind": "pool",
                "scope_key": "nebius-cpu",
                "max_concurrent": 56,
                "enabled": True,
                "reason": ("Current-inventory Nebius CPU pool permits 56 concurrent 2-vCPU tasks."),
            },
        ],
        f"{capacity_path}: admission policies must permit exactly 56 Nebius leases",
    )
    policy = capacity.get("policy")
    if not isinstance(policy, dict):
        raise ContractError(f"{capacity_path}: policy must be an object")
    preset_match = re.fullmatch(
        r"(?P<cpu>[1-9][0-9]*)vcpu-(?P<memory>[1-9][0-9]*)gb",
        str(target.get("execution_preset", "")),
    )
    _require(preset_match is not None, f"{path}: execution_preset must be explicit")
    assert preset_match is not None
    node_cpu_millis = int(preset_match.group("cpu")) * 1_000
    node_memory_mib = int(preset_match.group("memory")) * 1_024
    max_nodes = int(target.get("execution_max_nodes", 0))
    _require(
        max_nodes == 8 and int(target.get("execution_max_pods", 0)) == 64,
        f"{path}: current inventory requires 8 nodes and 64 Pods per node",
    )
    _require(
        policy.get("enabled") is True
        and int(policy.get("max_nodes", 0)) == max_nodes
        and int(policy.get("node_cpu_millis", 0)) == node_cpu_millis
        and int(policy.get("node_memory_mib", 0)) == node_memory_mib,
        f"{capacity_path}: policy node shape must match Terraform",
    )
    _require(
        int(policy.get("max_vcpu_millis", 0)) == max_nodes * node_cpu_millis
        and int(policy.get("max_memory_mib", 0)) == max_nodes * node_memory_mib
        and int(policy.get("max_storage_mib", 0))
        == max_nodes * int(policy.get("node_storage_mib", 0)),
        f"{capacity_path}: policy maxima must equal the complete execution pool",
    )
    requested_cpu = int(capacity["accepted_concurrency"]) * int(capacity["task_cpu_millis"])
    _require(
        int(policy["max_vcpu_millis"]) - requested_cpu >= 16_000,
        f"{capacity_path}: execution pool must preserve at least 2 vCPUs per node for overhead",
    )
    _require(
        int(capacity["provider_current_quota_vcpu_millis"]) - int(policy["max_vcpu_millis"])
        >= 4_000,
        f"{capacity_path}: current provider quota must preserve fixed infrastructure headroom",
    )
    target_preset_match = re.fullmatch(
        r"(?P<cpu>[1-9][0-9]*)vcpu-(?P<memory>[1-9][0-9]*)gb",
        str(capacity.get("target_execution_preset", "")),
    )
    _require(target_preset_match is not None, f"{capacity_path}: target preset must be explicit")
    assert target_preset_match is not None
    target_node_cpu_millis = int(target_preset_match.group("cpu")) * 1_000
    target_pool_vcpu_millis = int(capacity["target_execution_max_nodes"]) * target_node_cpu_millis
    _require(
        int(capacity["provider_required_quota_vcpu_millis"]) - target_pool_vcpu_millis >= 32_000,
        f"{capacity_path}: requested provider quota must preserve target infrastructure headroom",
    )
    _require(
        target_pool_vcpu_millis
        - int(capacity["target_concurrency"]) * int(capacity["task_cpu_millis"])
        >= 32_000,
        f"{capacity_path}: requested target must preserve execution overhead",
    )
    _require(
        int(capacity["provider_current_quota_nodes"]) - max_nodes >= 4,
        f"{capacity_path}: current VM quota must preserve fixed infrastructure slots",
    )
    _require(
        int(policy.get("max_pending_jobs", 0)) >= int(capacity["accepted_concurrency"])
        and int(policy.get("max_unschedulable_jobs", 0)) >= int(capacity["accepted_concurrency"])
        and int(policy.get("max_create_per_minute", 0)) >= int(capacity["accepted_concurrency"]),
        f"{capacity_path}: admission limits must permit the acceptance batch",
    )

    backend_paths = sorted((root / "backends").glob("*.s3.tfbackend.example"))
    _require(
        len(backend_paths) == 1,
        f"expected exactly 1 shared-cluster backend, found {len(backend_paths)}",
    )
    _require(
        backend_paths[0].name == "development-eu-north1.s3.tfbackend.example",
        "the existing development backend anchor must be reused, not forked",
    )
    for path in backend_paths:
        text = path.read_text(encoding="utf-8")
        key = _backend_value(text, "key", path)
        _require(
            key == "nebius/development/eu-north1/terraform.tfstate",
            f"{path}: existing remote state key must be retained",
        )
        _require(
            re.search(r"^\s*use_lockfile\s*=\s*true\s*$", text, re.MULTILINE) is not None,
            f"{path}: use_lockfile must be true",
        )
        _require(
            re.search(r"(?i)(access_key|secret_key|token)\s*=", text) is None,
            f"{path}: credentials may not be stored in backend files",
        )

    state_wrapper = repo_root / "scripts" / "ops" / "with_nebius_terraform_state_credentials.sh"
    _require(
        state_wrapper.is_file(), f"{state_wrapper}: persistent state credential wrapper is required"
    )
    state_wrapper_text = state_wrapper.read_text(encoding="utf-8")
    _require(
        "loom-nebius-terraform-state" in state_wrapper_text
        and "security_bin find-generic-password" in state_wrapper_text,
        f"{state_wrapper}: state credentials must come from the protected Keychain item",
    )
    _require(
        "refusing ambient AWS credentials" in state_wrapper_text,
        f"{state_wrapper}: ambient state credentials must be rejected",
    )

    for directory in (
        "modules/execution-target",
        "stack",
        "modules/platform",
        "platform",
        "modules/regional-execution",
    ):
        path = root / directory / "versions.tf"
        text = path.read_text(encoding="utf-8")
        _require(
            'required_version = "= 1.16.0"' in text, f"{path}: Terraform must be pinned to 1.16.0"
        )
        _require(
            re.search(r'\bsource\s*=\s*"nebius/nebius"', text) is not None,
            f"{path}: provider source must be nebius/nebius",
        )
        _require(
            re.search(r'\bversion\s*=\s*"= 0.6.46"', text) is not None,
            f"{path}: Nebius provider must be pinned to 0.6.46",
        )
        _require(
            path.with_name(".terraform.lock.hcl").is_file(),
            f"{path}: provider lock file is required",
        )

    module = (root / "modules" / "execution-target" / "main.tf").read_text(encoding="utf-8")
    node_groups = re.findall(
        r'resource "nebius_mk8s_v1_node_group" "[^"]+"(.*?)(?=\nresource |\Z)',
        module, re.DOTALL,
    )
    compute_instances = re.findall(
        r'resource "nebius_compute_v1_instance" "[^"]+"(.*?)(?=\nresource |\Z)',
        module, re.DOTALL,
    )
    _require(
        bool(node_groups) and all("public_ip_address" not in group for group in node_groups),
        "system and execution nodes must not assign public IP addresses",
    )
    _require(
        bool(compute_instances) and all(re.search(r'policy\s*=\s*"FORBID"', body)
                                       for body in (*node_groups, *compute_instances)),
        "all node groups and compute instances must forbid capacity reservations",
    )
    _require('key    = "loom.nebius/execution"' in module, "execution node taint is required")
    _require("audit_logs        = {}" in module, "managed control-plane audit logging is required")
    _require(
        'role        = "viewer"' in module,
        "registry pull access must remain resource-scoped viewer",
    )
    _require(
        "for_each = var.environment_bindings" in module,
        "evidence writer groups must remain environment-local",
    )
    _require(
        "nebius_iam_v1_group.evidence_writers[environment].id" in module,
        "every evidence prefix must bind its environment-local writer group",
    )
    _require(
        'roles    = ["storage.object-editor"]' in module, "evidence writers need object-only access"
    )
    _require(
        'resource "nebius_compute_v1_instance" "deployment_access"' in module,
        "the private control plane requires a Terraform-managed deployment gateway",
    )
    _require(
        "service_account_id = nebius_iam_v1_service_account.deployment_access.id" in module,
        "the deployment gateway must use a non-human instance identity",
    )
    _require(
        "public_ip_address = {\n      allocation_id = nebius_vpc_v1_allocation.deployment_access.id"
        in module,
        "the deployment gateway must use its persistent allocation",
    )
    _require(
        'role        = "editor"' in module,
        "the deployment identity needs resource-scoped cluster and registry access",
    )
    _require(
        'resource "nebius_iam_v1_access_permit" "deployment_access_project"' in module
        and "resource_id = var.project_id" in module,
        "Nebius Managed Kubernetes deployment access requires the supported project scope",
    )
    _require(
        "provider  = nebius.no_default_labels" in module,
        "immutable imported IAM relationships need a provider without default labels",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the repository-owned Nebius IaC contract."
    )
    parser.parse_args()
    try:
        check_nebius_iac()
    except (ContractError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Nebius IaC contract failed: {exc}") from exc
    print(
        "Nebius IaC contract passed for the shared execution stack and independent platform roots."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
