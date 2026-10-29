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
run "default_system_node_count" {
  command = plan
  assert {
    condition = (
      var.target.system_node_count == 1 &&
      nebius_mk8s_v1_node_group.regional["system"].fixed_node_count == 1 &&
      nebius_mk8s_v1_node_group.regional["system"].autoscaling == null
    )
    error_message = "Omitted system_node_count must preserve the existing fixed single system node."
  }
}
run "two_system_nodes_for_native_addons" {
  command = plan
  variables { target = merge(var.target, { system_node_count = 2 }) }
  assert {
    condition = (
      nebius_mk8s_v1_node_group.regional["system"].fixed_node_count == 2 &&
      nebius_mk8s_v1_node_group.regional["system"].autoscaling == null &&
      length(nebius_mk8s_v1_node_group.regional["system"].template.taints) == 0 &&
      nebius_mk8s_v1_node_group.regional["system"].template.resources.preset == "4vcpu-16gb" &&
      nebius_mk8s_v1_node_group.regional["system"].template.boot_disk.size_gibibytes == 80 &&
      nebius_mk8s_v1_node_group.regional["execution"].fixed_node_count == null &&
      nebius_mk8s_v1_node_group.regional["execution"].autoscaling.min_node_count == 0 &&
      length(nebius_mk8s_v1_node_group.regional["execution"].template.taints) == 2
    )
    error_message = "Two eligible system nodes must preserve their shape and the min-zero tainted execution pool."
  }
}
run "suspension_overrides_two_system_nodes" {
  command = plan
  variables { target = merge(var.target, { system_node_count = 2, suspended = true }) }
  assert {
    condition = alltrue([
      for node in nebius_mk8s_v1_node_group.regional :
      node.fixed_node_count == 0 && node.autoscaling == null
    ])
    error_message = "Suspension must override an explicit system count and stop both groups."
  }
}
run "reject_zero_system_node_count" {
  command = plan
  variables { target = merge(var.target, { system_node_count = 0 }) }
  expect_failures = [var.target]
}
run "reject_fractional_system_node_count" {
  command = plan
  variables { target = merge(var.target, { system_node_count = 1.5 }) }
  expect_failures = [var.target]
}
