mock_provider "nebius" {}
variables {
  nebius_profile                        = "test"
  integration_platform                  = null
  tenant_id                             = "tenant-e00test"
  project_id                            = "project-e00test"
  cluster_id                            = "mk8scluster-e00test"
  cluster_scope_id                      = "nebius-eu-north1-shared"
  subnet_id                             = "vpcsubnet-e00test"
  public_pool_id                        = "vpcpool-e00test"
  node_registry_pull_service_account_id = "serviceaccount-e00test"
  registry_fqdn                         = "cr.eu-north1.nebius.cloud/test"
  region                                = "eu-north1"
  kubernetes_version                    = "1.33"
  regional_execution_targets = { west = {
    target_id                             = "nebius-eu-west1-integration"
    cluster_scope_id                      = "nebius-eu-west1-execution"
    project_id                            = "project-e00test"
    subnet_id                             = "vpcsubnet-e00test"
    region                                = "eu-west1"
    node_platform                         = "cpu-d3"
    kubernetes_version                    = "1.33"
    service_cidr                          = "172.27.0.0/16"
    node_registry_pull_service_account_id = "serviceaccount-e00test"
  } }
}
run "root_defaults_to_one_system_node" {
  command = plan
  assert {
    condition = (
      var.regional_execution_targets["west"].system_node_count == 1 &&
      !output.regional_execution_targets["west"].suspended &&
      output.integration_platform == null
    )
    error_message = "The root schema must preserve the default single regional system node."
  }
}
run "root_forwards_two_system_nodes" {
  command = plan
  variables {
    regional_execution_targets = {
      west = merge(var.regional_execution_targets["west"], { system_node_count = 2 })
    }
  }
  assert {
    condition = (
      var.regional_execution_targets["west"].system_node_count == 2 &&
      !output.regional_execution_targets["west"].suspended &&
      output.integration_platform == null
    )
    error_message = "The root schema must forward an explicit system count without changing the primary platform."
  }
}
run "root_forwards_suspension_with_two_system_nodes" {
  command = plan
  variables {
    regional_execution_targets = {
      west = merge(var.regional_execution_targets["west"], { system_node_count = 2, suspended = true })
    }
  }
  assert {
    condition = (
      var.regional_execution_targets["west"].system_node_count == 2 &&
      output.regional_execution_targets["west"].suspended &&
      output.integration_platform == null
    )
    error_message = "Root forwarding must retain the count while suspension takes precedence in the module."
  }
}
run "root_rejects_zero_system_node_count" {
  command = plan
  variables {
    regional_execution_targets = {
      west = merge(var.regional_execution_targets["west"], { system_node_count = 0 })
    }
  }
  expect_failures = [var.regional_execution_targets]
}
run "root_rejects_fractional_system_node_count" {
  command = plan
  variables {
    regional_execution_targets = {
      west = merge(var.regional_execution_targets["west"], { system_node_count = 1.5 })
    }
  }
  expect_failures = [var.regional_execution_targets]
}
