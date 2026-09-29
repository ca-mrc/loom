mock_provider "nebius" {}
variables {
  target = {
    target_id                             = "nebius-eu-west1-integration"
    cluster_scope_id                      = "nebius-eu-west1-execution"
    project_id                            = "project-e00test"
    subnet_id                             = "vpcsubnet-e00test"
    region                                = "eu-west1"
    node_platform                         = "cpu-d3"
    kubernetes_version                    = "1.33"
    service_cidr                          = "172.27.0.0/16"
    node_registry_pull_service_account_id = "serviceaccount-e00test"
  }
}
run "execution_only" {
  command = plan
  assert {
    condition     = length(nebius_iam_v1_service_account.regional) == 0 && length(nebius_iam_v1_auth_public_key.regional) == 0 && length(nebius_iam_v1_group_membership.regional) == 0
    error_message = "Existing-ID mode must preserve its identity and create no IAM resources."
  }
  assert {
    condition     = alltrue([for node in nebius_mk8s_v1_node_group.regional : node.template.resources.platform == "cpu-d3"]) && nebius_mk8s_v1_node_group.regional["system"].template.resources.preset == "4vcpu-16gb" && nebius_mk8s_v1_node_group.regional["execution"].template.resources.preset == "16vcpu-64gb"
    error_message = "Use the explicit region-native CPU platform and its verified system/execution presets."
  }
  assert {
    condition     = length(nebius_mk8s_v1_cluster.regional.control_plane.endpoints.public_endpoint.allowed_cidrs) == 0
    error_message = "Default public API has native IAM/RBAC without a source-IP allowlist."
  }
  assert {
    condition     = length(nebius_mk8s_v1_node_group.regional) == 2 && nebius_mk8s_v1_node_group.regional["system"].fixed_node_count == 1 && nebius_mk8s_v1_node_group.regional["execution"].autoscaling.min_node_count == 0 && nebius_mk8s_v1_node_group.regional["execution"].autoscaling.max_node_count == 100
    error_message = "Only a fixed system node and native scale-to-zero execution pool."
  }
  assert {
    condition     = alltrue([for node in nebius_mk8s_v1_node_group.regional : node.template.network_interfaces[0].subnet_id == var.target.subnet_id && node.template.service_account_id == var.target.node_registry_pull_service_account_id])
    error_message = "Reuse private regional subnet and read-only pull identity."
  }
}
# Observed native Cilium Operator tolerations in eu-west1, 2026-09-10.
# It cannot register CRDs if every node has Loom's custom NoSchedule taint.
run "native_addon_scheduling" {
  command = plan
  assert {
    condition = alltrue([
      for taint in nebius_mk8s_v1_node_group.regional["system"].template.taints :
      contains(["CriticalAddonsOnly", "node.cilium.io/agent-not-ready", "node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"], taint.key)
    ])
    error_message = "The native Cilium Operator must tolerate every regional system node taint so it can register CRDs."
  }
  assert {
    condition = (
      length(nebius_mk8s_v1_node_group.regional["system"].template.taints) == 0 &&
      nebius_mk8s_v1_node_group.regional["system"].fixed_node_count == 1 &&
      nebius_mk8s_v1_node_group.regional["system"].template.resources.preset == "4vcpu-16gb" &&
      nebius_mk8s_v1_node_group.regional["system"].template.metadata.labels["loom.nebius/node-role"] == "system" &&
      nebius_mk8s_v1_node_group.regional["execution"].template.metadata.labels["loom.nebius/node-role"] == "integration-execution" &&
      toset([for taint in nebius_mk8s_v1_node_group.regional["execution"].template.taints : "${taint.key}=${taint.value}:${taint.effect}"]) == toset([
        "loom.nebius/platform=integration:NO_SCHEDULE",
        "loom.nebius/execution=true:NO_SCHEDULE"
      ])
    )
    error_message = "Unblock addons on the existing single system node without increasing its shape or weakening execution-node taints and selector labels."
  }
}
run "lower_limit" {
  command = plan
  variables { target = merge(var.target, { execution_max_nodes = 3 }) }
  assert {
    condition     = nebius_mk8s_v1_node_group.regional["execution"].autoscaling.max_node_count == 3
    error_message = "Do not widen an explicit operator ceiling."
  }
}
run "suspended_region_has_no_paid_nodes" {
  command = plan
  variables { target = merge(var.target, { suspended = true, execution_max_nodes = 3 }) }
  assert {
    condition     = alltrue([for node in nebius_mk8s_v1_node_group.regional : node.fixed_node_count == 0 && node.autoscaling == null])
    error_message = "Suspending a region must stop the system node and disable execution autoscaling."
  }
  assert {
    condition     = length(nebius_mk8s_v1_node_group.regional) == 2 && nebius_mk8s_v1_cluster.regional.name == "nebius-eu-west1-integration-cluster"
    error_message = "Suspension retains the cluster and node-group identities for later recovery."
  }
}
run "reject_invalid_api_cidr" {
  command = plan
  variables { target = merge(var.target, { public_control_plane_cidrs = ["not-a-cidr"] }) }
  expect_failures = [var.target]
}

run "preserve_optional_api_allowlist" {
  command = plan
  variables { target = merge(var.target, { public_control_plane_cidrs = ["192.0.2.1/32"] }) }
  assert {
    condition     = nebius_mk8s_v1_cluster.regional.control_plane.endpoints.public_endpoint.allowed_cidrs == tolist(["192.0.2.1/32"])
    error_message = "An explicit operator source-IP restriction must remain unchanged."
  }
}

run "reject_gpu_platform" {
  command = plan
  variables { target = merge(var.target, { node_platform = "gpu-h200-sxm" }) }
  expect_failures = [var.target]
}

run "managed_identities" {
  command = plan
  variables {
    target = merge(var.target, {
      node_registry_pull_service_account_id = null
      managed_identities = {
        actuator_public_key       = "-----BEGIN PUBLIC KEY-----\nYWN0dWF0b3I=\n-----END PUBLIC KEY-----"
        collector_public_key      = "-----BEGIN PUBLIC KEY-----\nY29sbGVjdG9y\n-----END PUBLIC KEY-----"
        gateway_public_key        = "-----BEGIN PUBLIC KEY-----\nZ2F0ZXdheQ==\n-----END PUBLIC KEY-----"
        collector_viewer_group_id = "group-existing-observer"
        registry_pull_group_id    = "group-existing-registry-pull"
      }
    })
  }
  override_resource {
    override_during = plan
    target          = nebius_iam_v1_service_account.regional["actuator"]
    values          = { id = "serviceaccount-test-actuator" }
  }
  override_resource {
    override_during = plan
    target          = nebius_iam_v1_service_account.regional["collector"]
    values          = { id = "serviceaccount-test-collector" }
  }
  override_resource {
    override_during = plan
    target          = nebius_iam_v1_service_account.regional["gateway"]
    values          = { id = "serviceaccount-test-gateway" }
  }
  override_resource {
    override_during = plan
    target          = nebius_iam_v1_service_account.regional["node-pull"]
    values          = { id = "serviceaccount-test-node-pull" }
  }
  assert {
    condition     = length(nebius_iam_v1_service_account.regional) == 4 && toset(keys(nebius_iam_v1_auth_public_key.regional)) == toset(["actuator", "collector", "gateway"]) && toset(keys(nebius_iam_v1_group_membership.regional)) == toset(["collector", "node-pull"])
    error_message = "Managed mode adds exactly four identities, three runtime public keys and only two viewer memberships; node-pull has no key."
  }
  assert {
    condition     = alltrue([for role, key in nebius_iam_v1_auth_public_key.regional : key.parent_id == var.target.project_id && key.account.service_account.id == "serviceaccount-test-${role}" && key.data == local.runtime_keys[role] && key.expires_at == null])
    error_message = "Public keys bind only to their corresponding regional runtime accounts without implicit expiry."
  }
  assert {
    condition     = nebius_iam_v1_group_membership.regional["collector"].parent_id == "group-existing-observer" && nebius_iam_v1_group_membership.regional["collector"].member_id == "serviceaccount-test-collector" && nebius_iam_v1_group_membership.regional["node-pull"].parent_id == "group-existing-registry-pull" && nebius_iam_v1_group_membership.regional["node-pull"].member_id == "serviceaccount-test-node-pull" && alltrue([for node in nebius_mk8s_v1_node_group.regional : node.template.service_account_id == "serviceaccount-test-node-pull"])
    error_message = "Observer and registry scopes must remain separate; both node groups attach only the regional pull account."
  }
  assert {
    condition     = output.execution.service_account_ids == { actuator = "serviceaccount-test-actuator", collector = "serviceaccount-test-collector", gateway = "serviceaccount-test-gateway" } && toset(keys(output.execution.authorized_public_key_ids)) == toset(["actuator", "collector", "gateway"]) && !can(output.execution.private_key) && !can(output.execution.credentials)
    error_message = "Operator output supplies IDs for the three runtime roles, never key data or credentials."
  }
}
run "reject_private_key" {
  command = plan
  variables {
    target = merge(var.target, {
      node_registry_pull_service_account_id = null
      managed_identities = {
        actuator_public_key       = "-----BEGIN PRIVATE KEY-----\ninvalid-test-fixture\n-----END PRIVATE KEY-----"
        collector_public_key      = "-----BEGIN PUBLIC KEY-----\nY29sbGVjdG9y\n-----END PUBLIC KEY-----"
        gateway_public_key        = "-----BEGIN PUBLIC KEY-----\nZ2F0ZXdheQ==\n-----END PUBLIC KEY-----"
        collector_viewer_group_id = "group-existing-observer"
        registry_pull_group_id    = "group-existing-registry-pull"
      }
    })
  }
  expect_failures = [var.target]
}
run "reject_mixed_identity_modes" {
  command = plan
  variables {
    target = merge(var.target, {
      managed_identities = {
        actuator_public_key       = "-----BEGIN PUBLIC KEY-----\nYWN0dWF0b3I=\n-----END PUBLIC KEY-----"
        collector_public_key      = "-----BEGIN PUBLIC KEY-----\nY29sbGVjdG9y\n-----END PUBLIC KEY-----"
        gateway_public_key        = "-----BEGIN PUBLIC KEY-----\nZ2F0ZXdheQ==\n-----END PUBLIC KEY-----"
        collector_viewer_group_id = "group-existing-observer"
        registry_pull_group_id    = "group-existing-registry-pull"
      }
    })
  }
  expect_failures = [var.target]
}
