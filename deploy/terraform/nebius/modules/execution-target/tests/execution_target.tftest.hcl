mock_provider "nebius" {}
mock_provider "nebius" {
  alias = "no_default_labels"
}

variables {
  tenant_id        = "tenant-e00test"
  project_id       = "project-e00test"
  target_id        = "nebius-eu-north1-development"
  cluster_scope_id = "nebius-eu-north1-shared"
  region           = "eu-north1"
  failure_domain   = "nebius-eu-north1-shared"
  environment_bindings = {
    development = {
      target_id       = "nebius-eu-north1-development"
      namespace_name  = "loom-nebius-development"
      evidence_prefix = "nebius-eu-north1-development"
    }
    staging = {
      target_id       = "nebius-eu-north1-staging"
      namespace_name  = "loom-nebius-staging"
      evidence_prefix = "nebius-eu-north1-staging"
    }
    production = {
      target_id       = "nebius-eu-north1-production"
      namespace_name  = "loom-nebius-production"
      evidence_prefix = "nebius-eu-north1-production"
    }
  }
  network_cidr                     = "10.0.0.0/16"
  service_cidr                     = "172.20.0.0/16"
  control_plane_etcd_size          = 1
  public_control_plane_cidrs       = []
  system_node_count                = 1
  execution_min_nodes              = 0
  execution_max_nodes              = 1
  execution_max_pods               = 64
  evidence_bucket_name             = "loom-nebius-test-evidence"
  deployment_access_public_pool_id = "vpcpool-e00test"
  deployment_access_ssh_public_key = "ssh-ed25519 dGVzdA== terraform-test"
  deployment_access_image_id       = "computeimage-e00test"
  capacity_observer_public_key_pem = <<-EOT
    -----BEGIN PUBLIC KEY-----
    dGVzdA==
    -----END PUBLIC KEY-----
  EOT
}

override_resource {
  target          = nebius_vpc_v1_allocation.deployment_access_private
  override_during = plan
  values = {
    id = "vpcallocation-private-service-test"
    status = {
      details = {
        allocated_cidr = "10.0.23.1/32"
      }
    }
  }
}

run "development_private_payg_plan" {
  command = plan
  providers = {
    nebius                   = nebius
    nebius.no_default_labels = nebius.no_default_labels
  }

  assert {
    condition     = nebius_vpc_v1_subnet.target.ipv4_public_pools.use_network_pools == false
    error_message = "Public service allocations must remain disabled by default."
  }

  assert {
    condition     = yamldecode(nebius_compute_v1_instance.deployment_access.cloud_init_user_data).users[0].sudo == ["ALL=(ALL) NOPASSWD:ALL"]
    error_message = "The deployment gateway operator needs a reproducible guest administration path."
  }

  assert {
    condition     = nebius_vpc_v1_allocation.deployment_access_private.ipv4_private.cidr == "/32" && nebius_vpc_v1_allocation.deployment_access_private.ipv4_public == null
    error_message = "The durable service address must be a private single-host allocation."
  }

  assert {
    condition     = nebius_compute_v1_instance.deployment_access.network_interfaces[0].aliases[0].allocation_id == "vpcallocation-private-service-test" && nebius_compute_v1_instance.deployment_access.network_interfaces[0].ip_address.allocation_id == null
    error_message = "Attach the reserved alias without changing the replacement-only primary IP."
  }

  assert {
    condition     = output.deployment_access.private_service_cidr == "10.0.23.1/32"
    error_message = "Consumers must receive the reserved service endpoint, not a dynamic VM address."
  }

  assert {
    condition     = contains(yamldecode(nebius_compute_v1_instance.deployment_access.cloud_init_user_data).packages, "wireguard-tools") && yamldecode(nebius_compute_v1_instance.deployment_access.cloud_init_user_data).disable_root && !yamldecode(nebius_compute_v1_instance.deployment_access.cloud_init_user_data).ssh_pwauth
    error_message = "Prepare private-link tooling without enabling root or password SSH."
  }

  assert {
    condition = toset([
      for block in nebius_vpc_v1_pool.target_private.cidrs : block.cidr
    ]) == toset([var.network_cidr, var.service_cidr])
    error_message = "The inherited private pool must contain both node/pod and Kubernetes service CIDRs."
  }

  assert {
    condition = alltrue([
      for block in nebius_vpc_v1_pool.target_private.cidrs : block.max_mask_length == 32
    ])
    error_message = "The inherited private pool must permit the /32 allocations required by the control plane and nodes."
  }

  assert {
    condition     = nebius_mk8s_v1_cluster.target.control_plane.endpoints.public_endpoint == null
    error_message = "Development control plane must remain private unless CIDRs are explicit."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.autoscaling.min_node_count == 0 && nebius_mk8s_v1_node_group.execution.autoscaling.max_node_count == 1
    error_message = "Development execution autoscaling must remain bounded to 0-1."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.template.max_pods == 64
    error_message = "Execution Pod density must remain an explicit target input."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.strategy.max_surge.count == 0 && nebius_mk8s_v1_node_group.execution.strategy.max_unavailable.count == 1
    error_message = "Execution replacement must stay inside the provider vCPU envelope."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.template.reservation_policy.policy == "FORBID"
    error_message = "Execution nodes must use regular PAYG capacity only."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.template.taints[0].key == "loom.nebius/execution"
    error_message = "Execution nodes must be isolated by the accepted taint."
  }


  assert {
    condition     = nebius_iam_v1_auth_public_key.capacity_observer.expires_at == null
    error_message = "The recurring capacity observer key must not acquire a calendar expiry."
  }

  assert {
    condition     = output.staging_spool == null && length(nebius_storage_v1_bucket.staging_spool) == 0 && length(nebius_iam_v1_service_account.staging_spool) == 0 && length(nebius_iam_v1_group.staging_spool) == 0 && length(nebius_iam_v1_group_membership.staging_spool) == 0 && length(nebius_iam_v2_access_key.staging_spool) == 0
    error_message = "Omitted staging spool must create no bucket, identity, membership, or credential."
  }
}

run "dedicated_staging_spool" {
  command = plan
  variables {
    staging_spool = { bucket_name = "loom-eu-north1-test-staging-spool" }
  }
  override_resource {
    target          = nebius_iam_v1_service_account.staging_spool["staging"]
    override_during = plan
    values          = { id = "serviceaccount-staging-spool" }
  }
  override_resource {
    target          = nebius_iam_v1_group.staging_spool["staging"]
    override_during = plan
    values          = { id = "group-staging-spool" }
  }
  override_resource {
    target          = nebius_iam_v2_access_key.staging_spool["staging"]
    override_during = plan
    values = {
      id = "accesskey-staging-spool"
      status = {
        aws_access_key_id = "test-aws-id"
      }
    }
  }
  assert {
    condition     = length(nebius_storage_v1_bucket.staging_spool) == 1 && length(nebius_iam_v1_service_account.staging_spool) == 1 && length(nebius_iam_v1_group.staging_spool) == 1 && length(nebius_iam_v1_group_membership.staging_spool) == 1 && length(nebius_iam_v2_access_key.staging_spool) == 1
    error_message = "Opt-in must provision exactly one dedicated bucket and one identity/key chain."
  }
  assert {
    condition     = nebius_iam_v1_group_membership.staging_spool["staging"].member_id == "serviceaccount-staging-spool" && nebius_iam_v1_group_membership.staging_spool["staging"].parent_id == "group-staging-spool" && nebius_iam_v2_access_key.staging_spool["staging"].account.service_account.id == "serviceaccount-staging-spool"
    error_message = "The spool key and membership must use only the dedicated service account."
  }
  assert {
    condition     = nebius_iam_v2_access_key.staging_spool["staging"].expires_at == null && nebius_iam_v2_access_key.staging_spool["staging"].secret_delivery_mode == "EXPLICIT"
    error_message = "No calendar expiry or one-time INLINE secret dependency is permitted."
  }
  assert {
    condition     = length(nebius_storage_v1_bucket.staging_spool["staging"].bucket_policy.rules) == 1 && nebius_storage_v1_bucket.staging_spool["staging"].bucket_policy.rules[0].group_id == "group-staging-spool" && nebius_storage_v1_bucket.staging_spool["staging"].bucket_policy.rules[0].paths == tolist(["*"]) && nebius_storage_v1_bucket.staging_spool["staging"].bucket_policy.rules[0].roles == tolist(["storage.object-editor"]) && nebius_storage_v1_bucket.staging_spool["staging"].bucket_policy.rules[0].anonymous == null
    error_message = "The dedicated bucket alone must grant object operations, not bucket administration or anonymous access."
  }
  assert {
    condition     = nebius_storage_v1_bucket.staging_spool["staging"].versioning_policy == "DISABLED" && nebius_storage_v1_bucket.staging_spool["staging"].max_size_bytes == 0 && length(nebius_storage_v1_bucket.staging_spool["staging"].lifecycle_configuration.rules) == 1 && nebius_storage_v1_bucket.staging_spool["staging"].lifecycle_configuration.rules[0].expiration == null && nebius_storage_v1_bucket.staging_spool["staging"].lifecycle_configuration.rules[0].noncurrent_version_expiration == null && nebius_storage_v1_bucket.staging_spool["staging"].lifecycle_configuration.rules[0].abort_incomplete_multipart_upload.days_after_initiation == 7
    error_message = "Only unfinished multipart uploads expire; completed objects await acknowledged materialization GC with no added budget ceiling."
  }
  assert {
    condition     = output.staging_spool.endpoint == "https://storage.eu-north1.nebius.cloud" && output.staging_spool.target_id == "nebius-eu-north1-staging" && output.staging_spool.access_key_resource_id == "accesskey-staging-spool" && output.staging_spool.aws_access_key_id == "test-aws-id" && !contains(keys(output.staging_spool), "secret") && !contains(keys(output.staging_spool), "secret_access_key")
    error_message = "Output must identify the native HTTPS endpoint and protected credential reference, never secret bytes."
  }
}

run "staging_spool_rejects_evidence_reuse" {
  command = plan
  variables {
    evidence_bucket_name = "loom-eu-north1-test-staging-spool"
    staging_spool        = { bucket_name = "loom-eu-north1-test-staging-spool" }
  }
  expect_failures = [var.staging_spool]
}

run "staging_spool_rejects_state_bucket_name" {
  command = plan
  variables {
    staging_spool = { bucket_name = "loom-nebius-terraform-state" }
  }
  expect_failures = [var.staging_spool]
}

run "gateway_alias_rejects_service_cidr" {
  command = plan
  providers = {
    nebius                   = nebius
    nebius.no_default_labels = nebius.no_default_labels
  }
  override_resource {
    target          = nebius_vpc_v1_allocation.deployment_access_private
    override_during = plan
    values = {
      id = "vpcallocation-outside-node-network"
      status = {
        details = {
          allocated_cidr = "172.20.23.1/32"
        }
      }
    }
  }
  expect_failures = [nebius_vpc_v1_allocation.deployment_access_private]
}

run "public_control_plane_is_cidr_bounded" {
  command = plan
  providers = {
    nebius                   = nebius
    nebius.no_default_labels = nebius.no_default_labels
  }

  variables {
    public_control_plane_cidrs = ["203.0.113.9/32"]
  }

  assert {
    condition     = length(nebius_mk8s_v1_cluster.target.control_plane.endpoints.public_endpoint.allowed_cidrs) == 1 && one(nebius_mk8s_v1_cluster.target.control_plane.endpoints.public_endpoint.allowed_cidrs) == "203.0.113.9/32"
    error_message = "A public control plane must preserve the exact approved CIDR allowlist."
  }
}

run "shared_cluster_rejects_missing_environment_binding" {
  command = plan
  providers = {
    nebius                   = nebius
    nebius.no_default_labels = nebius.no_default_labels
  }

  variables {
    environment_bindings = {
      development = {
        target_id       = "nebius-eu-north1-development"
        namespace_name  = "loom-nebius-development"
        evidence_prefix = "nebius-eu-north1-development"
      }
      staging = {
        target_id       = "nebius-eu-north1-staging"
        namespace_name  = "loom-nebius-staging"
        evidence_prefix = "nebius-eu-north1-staging"
      }
    }
  }

  expect_failures = [var.environment_bindings]
}

run "public_service_allocations_opt_in" {
  command = plan
  providers = {
    nebius                   = nebius
    nebius.no_default_labels = nebius.no_default_labels
  }
  variables {
    enable_public_service_allocations = true
  }
  assert {
    condition     = nebius_vpc_v1_subnet.target.ipv4_public_pools.use_network_pools == true
    error_message = "Explicit service opt-in must permit allocations from the existing network public pools."
  }
}

run "h100_default_disabled" {
  command = plan
  assert {
    condition     = length(nebius_mk8s_v1_node_group.h100_execution) == 0 && output.h100_resource_pool == null
    error_message = "Omitted H100 pool must create no GPU node group."
  }
}

run "h100_opt_in_bounded_private_pool" {
  command = plan
  variables {
    h100_pool = { max_nodes = 1, disk_gib = 1100, drivers_preset = "cuda12.8" }
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.execution.template.resources.platform == "cpu-e2" && nebius_mk8s_v1_node_group.h100_execution[0].template.resources.platform == "gpu-h100-sxm" && nebius_mk8s_v1_node_group.h100_execution[0].template.resources.preset == "1gpu-16vcpu-200gb"
    error_message = "The independent H100 pool must preserve the CPU node group's platform."
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.h100_execution[0].autoscaling.min_node_count == 0 && nebius_mk8s_v1_node_group.h100_execution[0].autoscaling.max_node_count == 1 && nebius_mk8s_v1_node_group.h100_execution[0].strategy.max_surge.count == 0
    error_message = "H100 qualification configuration must start at zero and stay inside its one-node ceiling."
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.h100_execution[0].template.gpu_settings.drivers_preset == "cuda12.8" && nebius_mk8s_v1_node_group.h100_execution[0].template.boot_disk.size_gibibytes == 1100 && nebius_mk8s_v1_node_group.h100_execution[0].template.metadata.labels["loom.nebius/gpu-model"] == "H100"
    error_message = "Preserve the GPU driver, exact model selector and disk headroom."
  }
  assert {
    condition     = nebius_mk8s_v1_node_group.h100_execution[0].template.taints[1].key == "loom.nebius/gpu-model" && nebius_mk8s_v1_node_group.h100_execution[0].template.taints[1].value == "H100" && nebius_mk8s_v1_node_group.h100_execution[0].template.reservation_policy.policy == "FORBID"
    error_message = "The on-demand pool must be GPU-tainted and avoid reservations."
  }
  assert {
    condition     = output.h100_resource_pool.runtime_qualified == false
    error_message = "IaC configuration must never assert GPU runtime qualification."
  }
}

run "h100_rejects_other_region" {
  command = plan
  variables {
    region    = "eu-west1"
    h100_pool = { max_nodes = 1, disk_gib = 1100, drivers_preset = "cuda12.8" }
  }
  expect_failures = [terraform_data.contract]
}

run "h100_rejects_unbounded_or_empty_pool" {
  command = plan
  variables {
    h100_pool = { max_nodes = 0, disk_gib = 1100, drivers_preset = "cuda12.8" }
  }
  expect_failures = [var.h100_pool]
}

run "h100_rejects_insufficient_disk" {
  command = plan
  variables {
    h100_pool = { max_nodes = 1, disk_gib = 1000, drivers_preset = "cuda12.8" }
  }
  expect_failures = [var.h100_pool]
}
