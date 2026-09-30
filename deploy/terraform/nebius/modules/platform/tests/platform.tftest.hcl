mock_provider "nebius" {}
variables {
  tenant_id                             = "tenant-e00test"
  project_id                            = "project-e00test"
  cluster_id                            = "mk8scluster-e00test"
  cluster_scope_id                      = "nebius-eu-north1-shared"
  subnet_id                             = "vpcsubnet-e00test"
  public_pool_id                        = "vpcpool-e00test"
  node_registry_pull_service_account_id = "serviceaccount-e00test"
  registry_fqdn                         = "cr.eu-north1.nebius.cloud/test"
  region                                = "eu-north1"
  kubernetes_version                    = "1.35"
}
run "disabled_is_empty" {
  command = plan
  assert {
    condition     = output.integration_platform == null && length(nebius_mk8s_v1_node_group.integration) == 0 && length(nebius_storage_v1_bucket.integration) == 0
    error_message = "No platform resources without explicit configuration."
  }
}
run "independent_platform" {
  command = plan
  variables {
    integration_platform = { bucket_prefix = "loom-platform-test" }
  }
  assert {
    condition     = length(nebius_mk8s_v1_node_group.integration) == 2 && length(nebius_storage_v1_bucket.integration) == 4 && length(nebius_iam_v2_access_key.integration_store) == 3
    error_message = "Exactly system/execution node roles, four stores and three credential scopes."
  }
  assert {
    condition     = alltrue([for group in nebius_mk8s_v1_node_group.integration : group.parent_id == var.cluster_id && group.template.network_interfaces[0].subnet_id == var.subnet_id && group.template.service_account_id == var.node_registry_pull_service_account_id])
    error_message = "Reuse only bound private cluster and read-only node identity."
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.integration["system"].fixed_node_count == 1 && alltrue([for role in ["execution"] : nebius_mk8s_v1_node_group.integration[role].autoscaling.min_node_count == 0])
    error_message = "Only the system node is persistent."
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.integration["execution"].autoscaling.max_node_count == 100
    error_message = "The default is the native API technical maximum; CP admits only real quota-backed demand."
  }
  assert {
    condition     = nebius_storage_v1_bucket.integration["source"].versioning_policy == "DISABLED" && alltrue([for name in ["artifacts", "trajectories", "backups"] : nebius_storage_v1_bucket.integration[name].versioning_policy == "ENABLED"])
    error_message = "Source cleanup must release bytes; canonical and backups retain versions."
  }
  assert {
    condition     = toset(keys(nebius_mk8s_v1_node_group.integration)) == toset(["system", "execution"]) && alltrue([for group in nebius_mk8s_v1_node_group.integration : group.template.metadata.labels["loom.nebius/platform"] == "integration"])
    error_message = "CI and publication remain GitHub-hosted; no runner node groups may be provisioned."
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.integration["execution"].template.metadata.labels["loom.nebius/node-role"] == "integration-execution"
    error_message = "Integration nodes must not match the existing collector's node-role=execution selector."
  }
  assert {
    condition     = alltrue([for key in nebius_iam_v2_access_key.integration_store : key.secret_delivery_mode == "EXPLICIT" && key.expires_at == null])
    error_message = "No secret bytes in state and no calendar credential outage."
  }
}
run "reject_unbounded_capacity" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", execution_max_nodes = 999 } }
  expect_failures = [var.integration_platform]
}

run "native_builder_scales_from_zero" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", execution_max_nodes = 2 } }
  assert {
    condition = nebius_mk8s_v1_node_group.integration["execution"].template.metadata.labels == tomap({
      "loom.nebius/node-role"        = "integration-execution"
      "loom.nebius/platform"         = "integration"
      "loom.nebius/cluster-scope-id" = var.cluster_scope_id
      "loom.nebius/node-os"          = "linux"
      "loom.nebius/node-arch"        = "amd64"
    })
    error_message = "Zero-node execution templates must match native builds' complete node selector."
  }
  assert {
    condition = (
      nebius_mk8s_v1_node_group.integration["execution"].autoscaling.min_node_count == 0 &&
      nebius_mk8s_v1_node_group.integration["execution"].autoscaling.max_node_count == 2 &&
      nebius_mk8s_v1_node_group.integration["execution"].template.resources.platform == "cpu-e2" &&
      nebius_mk8s_v1_node_group.integration["execution"].template.resources.preset == "16vcpu-64gb" &&
      nebius_mk8s_v1_node_group.integration["execution"].template.boot_disk.size_gibibytes == 80 &&
      !contains(keys(nebius_mk8s_v1_node_group.integration["system"].template.metadata.labels), "loom.nebius/node-arch")
    )
    error_message = "Template label repair must preserve capacity and leave the system node group unchanged."
  }
}

run "preserve_explicit_operator_ceiling" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", execution_max_nodes = 3 } }
  assert {
    condition     = nebius_mk8s_v1_node_group.integration["execution"].autoscaling.max_node_count == 3
    error_message = "A deliberate lower Terraform ceiling must remain unchanged."
  }
}
run "reject_fractional_node_ceiling" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", execution_max_nodes = 2.5 } }
  expect_failures = [var.integration_platform]
}

run "system_capacity_and_safe_replacement_are_opt_in" {
  command = plan
  variables {
    integration_platform = {
      bucket_prefix              = "loom-platform-test"
      system_preset              = "8vcpu-32gb"
      system_disk_gib            = 200
      system_create_before_drain = true
    }
  }
  assert {
    condition = (
      nebius_mk8s_v1_node_group.integration["system"].template.resources.preset == "8vcpu-32gb" &&
      nebius_mk8s_v1_node_group.integration["system"].template.boot_disk.size_gibibytes == 200 &&
      nebius_mk8s_v1_node_group.integration["system"].fixed_node_count == 1 &&
      nebius_mk8s_v1_node_group.integration["system"].strategy.max_surge.count == 1 &&
      nebius_mk8s_v1_node_group.integration["system"].strategy.max_unavailable.count == 0
    )
    error_message = "Explicit system sizing must create the replacement before draining, with one steady node."
  }
  assert {
    condition = (
      nebius_mk8s_v1_node_group.integration["execution"].template.boot_disk.size_gibibytes == 80 &&
      nebius_mk8s_v1_node_group.integration["execution"].template.resources.preset == "16vcpu-64gb" &&
      nebius_mk8s_v1_node_group.integration["execution"].strategy.max_surge.count == 0 &&
      nebius_mk8s_v1_node_group.integration["execution"].strategy.max_unavailable.count == 1
    )
    error_message = "System replacement settings must never resize or replace execution nodes."
  }
}
run "existing_system_defaults_are_preserved" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test" } }
  assert {
    condition = (
      nebius_mk8s_v1_node_group.integration["system"].template.boot_disk.size_gibibytes == 80 &&
      nebius_mk8s_v1_node_group.integration["system"].template.max_pods == 64 &&
      nebius_mk8s_v1_node_group.integration["system"].strategy.max_surge.count == 0 &&
      nebius_mk8s_v1_node_group.integration["system"].strategy.max_unavailable.count == 1
    )
    error_message = "An unchanged configuration must not resize or replace the existing system node."
  }
}
run "system_pod_headroom_does_not_resize_execution_or_compute" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_max_pods = 110 } }
  assert {
    condition = (
      nebius_mk8s_v1_node_group.integration["system"].template.max_pods == 110 &&
      nebius_mk8s_v1_node_group.integration["system"].template.resources.preset == "4vcpu-16gb" &&
      nebius_mk8s_v1_node_group.integration["system"].template.boot_disk.size_gibibytes == 80 &&
      nebius_mk8s_v1_node_group.integration["system"].fixed_node_count == 1 &&
      nebius_mk8s_v1_node_group.integration["execution"].template.max_pods == 64 &&
      nebius_mk8s_v1_node_group.integration["execution"].autoscaling.min_node_count == 0 &&
      nebius_mk8s_v1_node_group.integration["execution"].autoscaling.max_node_count == 100
    )
    error_message = "Explicit Pod headroom must affect only the system group's Pod limit, not compute or execution capacity."
  }
}
run "minimum_system_pod_limit_is_supported" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_max_pods = 16 } }
  assert {
    condition     = nebius_mk8s_v1_node_group.integration["system"].template.max_pods == 16
    error_message = "The documented lower bound must be forwarded; live fit is qualified separately."
  }
}
run "reject_undersized_system_pod_limit" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_max_pods = 15 } }
  expect_failures = [var.integration_platform]
}
run "reject_oversized_system_pod_limit" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_max_pods = 111 } }
  expect_failures = [var.integration_platform]
}
run "reject_fractional_system_pod_limit" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_max_pods = 64.5 } }
  expect_failures = [var.integration_platform]
}
run "reject_undersized_system_disk" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_disk_gib = 79 } }
  expect_failures = [var.integration_platform]
}
run "reject_fractional_system_disk" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_disk_gib = 200.5 } }
  expect_failures = [var.integration_platform]
}
run "reject_oversized_system_disk" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-platform-test", system_disk_gib = 1025 } }
  expect_failures = [var.integration_platform]
}

run "disabled_preserves_existing_buckets" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-native-test-artifacts" } }
  assert {
    condition     = length(nebius_storage_v1_bucket.integration["artifacts"].bucket_policy.rules) == 1
    error_message = "Disabled builder must not change bucket access."
  }
}
run "enabled_scopes_access_and_retention" {
  command = plan
  variables { integration_platform = { bucket_prefix = "loom-native-test-artifacts", native_builder_group_id = "group-e00nativebuilder" } }
  assert {
    condition     = length(nebius_storage_v1_bucket.integration["artifacts"].bucket_policy.rules) == 3
    error_message = "Builder requires two scoped rules in addition to canonical access."
  }
  assert {
    condition     = alltrue([for key in ["source", "backups", "trajectories"] : length(nebius_storage_v1_bucket.integration[key].bucket_policy.rules) == 1 && length(nebius_storage_v1_bucket.integration[key].lifecycle_configuration.rules) == 1])
    error_message = "Builder must not alter unrelated buckets."
  }
  assert {
    condition     = nebius_storage_v1_bucket.integration["artifacts"].bucket_policy.rules[2].paths == tolist(["task-build-cache/*"])
    error_message = "Write permission must be restricted to cache."
  }
  assert {
    condition     = nebius_storage_v1_bucket.integration["artifacts"].lifecycle_configuration.rules[1].filter.prefix == "task-build-cache/" && nebius_storage_v1_bucket.integration["artifacts"].lifecycle_configuration.rules[1].noncurrent_version_expiration.noncurrent_days == 1
    error_message = "Version retention must be scoped to cache."
  }
}
