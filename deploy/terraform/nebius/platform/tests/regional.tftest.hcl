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
}
run "no_regional_resources_by_default" {
  command = plan
  assert {
    condition     = length(module.regional_execution) == 0 && length(output.regional_execution_targets) == 0 && output.integration_platform == null
    error_message = "The optional regional extension must create nothing by default."
  }
}
run "root_preserves_explicit_system_capacity" {
  command = plan
  variables {
    integration_platform = {
      bucket_prefix              = "loom-platform-test"
      system_preset              = "8vcpu-32gb"
      system_disk_gib            = 200
      system_max_pods            = 110
      system_create_before_drain = true
    }
  }
  assert {
    condition = (
      var.integration_platform.system_disk_gib == 200 &&
      var.integration_platform.system_max_pods == 110 &&
      var.integration_platform.system_create_before_drain &&
      length(module.regional_execution) == 0
    )
    error_message = "The root input schema must preserve system settings forwarded to the primary module only."
  }
}
run "one_explicit_regional_cluster" {
  command = plan
  variables {
    regional_execution_targets = { west = {
      target_id                             = "nebius-eu-west1-integration"
      cluster_scope_id                      = "nebius-eu-west1-execution"
      project_id                            = "project-e00test"
      subnet_id                             = "vpcsubnet-e00test"
      region                                = "eu-west1"
      node_platform                         = "cpu-d3"
      kubernetes_version                    = "1.33"
      service_cidr                          = "172.27.0.0/16"
      public_control_plane_cidrs            = ["192.0.2.1/32"]
      node_registry_pull_service_account_id = "serviceaccount-e00test"
      execution_max_nodes                   = 3
    } }
  }
  assert {
    condition     = length(module.regional_execution) == 1 && output.regional_execution_targets["west"].target_id == "nebius-eu-west1-integration" && output.integration_platform == null
    error_message = "Only the explicitly selected regional module may be added."
  }
}

run "suspended_regional_target_is_forwarded" {
  command = plan
  variables {
    regional_execution_targets = { west = {
      target_id                             = "nebius-eu-west1-integration"
      cluster_scope_id                      = "nebius-eu-west1-execution"
      project_id                            = "project-e00test"
      subnet_id                             = "vpcsubnet-e00test"
      region                                = "eu-west1"
      node_platform                         = "cpu-d3"
      kubernetes_version                    = "1.33"
      service_cidr                          = "172.27.0.0/16"
      public_control_plane_cidrs            = ["192.0.2.1/32"]
      node_registry_pull_service_account_id = "serviceaccount-e00test"
      execution_max_nodes                   = 3
      suspended                             = true
    } }
  }
  assert {
    condition     = output.regional_execution_targets["west"].suspended && length(module.regional_execution) == 1 && output.regional_execution_targets["west"].target_id == "nebius-eu-west1-integration" && output.integration_platform == null
    error_message = "The root must preserve suspension when forwarding a regional target."
  }
}

run "managed_regional_identities" {
  command = plan
  variables {
    regional_execution_targets = { west = {
      target_id                  = "nebius-eu-west1-integration"
      cluster_scope_id           = "nebius-eu-west1-execution"
      project_id                 = "project-e00test"
      subnet_id                  = "vpcsubnet-e00test"
      region                     = "eu-west1"
      node_platform              = "cpu-d3"
      kubernetes_version         = "1.33"
      service_cidr               = "172.27.0.0/16"
      public_control_plane_cidrs = ["192.0.2.1/32"]
      managed_identities = {
        actuator_public_key       = "-----BEGIN PUBLIC KEY-----\nYWN0dWF0b3I=\n-----END PUBLIC KEY-----"
        collector_public_key      = "-----BEGIN PUBLIC KEY-----\nY29sbGVjdG9y\n-----END PUBLIC KEY-----"
        gateway_public_key        = "-----BEGIN PUBLIC KEY-----\nZ2F0ZXdheQ==\n-----END PUBLIC KEY-----"
        collector_viewer_group_id = "group-existing-observer"
        registry_pull_group_id    = "group-existing-registry-pull"
      }
      execution_max_nodes = 3
    } }
  }
  assert {
    condition     = length(module.regional_execution) == 1 && output.regional_execution_targets["west"].target_id == "nebius-eu-west1-integration" && output.integration_platform == null && toset(keys(output.regional_execution_targets["west"].service_account_ids)) == toset(["actuator", "collector", "gateway"])
    error_message = "Only the explicitly selected regional module may be added."
  }
}
