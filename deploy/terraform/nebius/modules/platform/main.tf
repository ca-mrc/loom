# Opt-in full-platform integration resources. Keep the original execution
# target/state intact while the independent candidate is accepted.
variable "integration_platform" {
  description = "Independent pure Nebius integration footprint; null creates nothing."
  type = object({
    bucket_prefix              = string
    system_preset              = optional(string, "4vcpu-16gb")
    system_disk_gib            = optional(number, 80)
    system_max_pods            = optional(number, 64)
    system_create_before_drain = optional(bool, false)
    execution_max_nodes        = optional(number, 100)
    execution_disk_type        = optional(string, "NETWORK_SSD")
    execution_disk_gib         = optional(number, 80)
    native_builder_group_id    = optional(string)
  })
  default = null
  validation {
    condition = var.integration_platform == null ? true : (
      can(regex("^[a-z0-9][a-z0-9-]{5,45}$", var.integration_platform.bucket_prefix)) &&
      contains(["4vcpu-16gb", "8vcpu-32gb"], var.integration_platform.system_preset) &&
      var.integration_platform.system_disk_gib >= 80 && var.integration_platform.system_disk_gib <= 1024 &&
      floor(var.integration_platform.system_disk_gib) == var.integration_platform.system_disk_gib &&
      var.integration_platform.system_max_pods >= 16 && var.integration_platform.system_max_pods <= 110 &&
      floor(var.integration_platform.system_max_pods) == var.integration_platform.system_max_pods &&
      var.integration_platform.execution_max_nodes >= 1 && var.integration_platform.execution_max_nodes <= 100 &&
      floor(var.integration_platform.execution_max_nodes) == var.integration_platform.execution_max_nodes &&
      contains(["NETWORK_SSD", "NETWORK_SSD_NON_REPLICATED"], var.integration_platform.execution_disk_type) &&
      var.integration_platform.execution_disk_gib >= 80 && var.integration_platform.execution_disk_gib <= 8192 &&
      floor(var.integration_platform.execution_disk_gib) == var.integration_platform.execution_disk_gib &&
      (var.integration_platform.execution_disk_type != "NETWORK_SSD_NON_REPLICATED" || var.integration_platform.execution_disk_gib % 93 == 0) &&
      (var.integration_platform.native_builder_group_id == null ? true : can(regex("^group-[a-z0-9]+$", var.integration_platform.native_builder_group_id)))
    )
    error_message = "Integration needs a unique bucket prefix, a supported system preset, an integer system disk from 80 to 1024 GiB, an integer system Pod limit from 16 to 110, an integer execution ceiling from 1 to 100, and an execution disk from 80 to 8192 GiB of type NETWORK_SSD or NETWORK_SSD_NON_REPLICATED (multiples of 93 GiB for NRD)."
  }
}

locals {
  integration = var.integration_platform == null ? {} : { integration = var.integration_platform }
  integration_labels = merge(var.labels, {
    "loom-purpose" = "nebius-integration"
  })
  integration_stores = var.integration_platform == null ? {} : {
    artifacts    = { identity = "canonical", versioning = "ENABLED" }
    trajectories = { identity = "canonical", versioning = "ENABLED" }
    source       = { identity = "source", versioning = "DISABLED" }
    backups      = { identity = "backup", versioning = "ENABLED" }
  }
  integration_identities = var.integration_platform == null ? toset([]) : toset(["canonical", "source", "backup"])
  # The dedicated builder identity is provisioned by the operator; only its
  # non-secret group reference belongs in Terraform state. Keep cache retention
  # local to its prefix because canonical artifacts retain version history.
  native_builder_enabled = try(var.integration_platform.native_builder_group_id, null) != null
  native_builder_rules = local.native_builder_enabled ? [
    {
      group_id = var.integration_platform.native_builder_group_id
      paths    = ["*"]
      roles    = ["storage.object-viewer", "storage.object-lister"]
    },
    {
      group_id = var.integration_platform.native_builder_group_id
      paths    = ["task-build-cache/*"]
      roles    = ["storage.object-editor"]
    }
  ] : []
  native_builder_lifecycle = [
    {
      id                            = "task-build-cache-retention"
      status                        = "ENABLED"
      filter                        = { prefix = "task-build-cache/" }
      expiration                    = { days = 7, expired_object_delete_marker = null }
      noncurrent_version_expiration = { noncurrent_days = 1 }
    },
    {
      id                            = "task-build-cache-delete-markers"
      status                        = "ENABLED"
      filter                        = { prefix = "task-build-cache/" }
      expiration                    = { days = null, expired_object_delete_marker = true }
      noncurrent_version_expiration = null
    }
  ]
  integration_nodes = var.integration_platform == null ? {} : {
    system    = { preset = var.integration_platform.system_preset, minimum = 1, maximum = 1, disk = var.integration_platform.system_disk_gib, disk_type = "NETWORK_SSD" }
    execution = { preset = "16vcpu-64gb", minimum = 0, maximum = var.integration_platform.execution_max_nodes, disk = var.integration_platform.execution_disk_gib, disk_type = var.integration_platform.execution_disk_type }
  }
}

resource "nebius_vpc_v1_allocation" "integration_public" {
  for_each  = local.integration
  parent_id = var.project_id
  name      = "loom-nebius-integration-https"
  labels    = local.integration_labels
  ipv4_public = {
    pool_id = var.public_pool_id
    cidr    = "/32"
  }
}

resource "nebius_iam_v1_service_account" "integration_store" {
  for_each    = local.integration_identities
  parent_id   = var.project_id
  name        = "loom-nebius-integration-${each.key}"
  description = "Only integration ${each.key} objects, no project access"
  labels      = local.integration_labels
}

resource "nebius_iam_v1_group" "integration_store" {
  for_each  = local.integration_identities
  parent_id = var.tenant_id
  name      = "loom-nebius-integration-${each.key}"
  labels    = local.integration_labels
}

resource "nebius_iam_v1_group_membership" "integration_store" {
  for_each  = local.integration_identities
  parent_id = nebius_iam_v1_group.integration_store[each.key].id
  member_id = nebius_iam_v1_service_account.integration_store[each.key].id
  labels    = local.integration_labels
}

resource "nebius_storage_v1_bucket" "integration" {
  for_each              = local.integration_stores
  parent_id             = var.project_id
  name                  = "${var.integration_platform.bucket_prefix}-${each.key}"
  labels                = local.integration_labels
  default_storage_class = "STANDARD"
  force_storage_class   = true
  max_size_bytes        = 0
  object_audit_logging  = "ALL"
  versioning_policy     = each.value.versioning
  bucket_policy = {
    rules = concat([{
      group_id = nebius_iam_v1_group.integration_store[each.value.identity].id
      paths    = ["*"]
      roles    = ["storage.object-editor"]
    }], each.key == "artifacts" ? local.native_builder_rules : [])
  }
  lifecycle_configuration = {
    rules = concat([{
      id                                = "abort-incomplete-uploads"
      status                            = "ENABLED"
      abort_incomplete_multipart_upload = { days_after_initiation = 7 }
    }], [for rule in local.native_builder_lifecycle : rule if each.key == "artifacts" && local.native_builder_enabled])
  }
  lifecycle { prevent_destroy = true }
}

# Bucket policies authorize object operations. The native catalog publisher
# additionally reads versioning through S3, which needs a resource-scoped permit.
resource "nebius_iam_v1_access_permit" "integration_catalog_metadata" {
  for_each    = local.integration
  parent_id   = nebius_iam_v1_group.integration_store["canonical"].id
  resource_id = nebius_storage_v1_bucket.integration["artifacts"].id
  role        = "storage.viewer"
}

resource "nebius_iam_v2_access_key" "integration_store" {
  for_each    = local.integration_identities
  parent_id   = var.project_id
  name        = "loom-nebius-integration-${each.key}"
  description = "Integration ${each.key} object credential, retrieve outside Terraform state"
  labels      = local.integration_labels
  account = {
    service_account = { id = nebius_iam_v1_service_account.integration_store[each.key].id }
  }
  secret_delivery_mode = "EXPLICIT"
}

# All application and execution nodes retain registry-read-only cloud identity.
# CI and publication run on GitHub-hosted machines, with no cloud runner pool.
resource "nebius_mk8s_v1_node_group" "integration" {
  for_each         = local.integration_nodes
  parent_id        = var.cluster_id
  name             = "loom-nebius-integration-${each.key}"
  labels           = local.integration_labels
  version          = var.kubernetes_version
  auto_repair      = {}
  fixed_node_count = each.key == "system" ? 1 : null
  autoscaling = each.key == "system" ? null : {
    min_node_count = each.value.minimum
    max_node_count = each.value.maximum
  }
  strategy = {
    drain_timeout   = "20m"
    max_surge       = { count = each.key == "system" && var.integration_platform.system_create_before_drain ? 1 : 0 }
    max_unavailable = { count = each.key == "system" && var.integration_platform.system_create_before_drain ? 0 : 1 }
  }
  template = {
    service_account_id = var.node_registry_pull_service_account_id
    max_pods           = each.key == "system" ? var.integration_platform.system_max_pods : 64
    metadata = {
      # Zero-node autoscaler templates need the same OS/architecture labels
      # required by native builds; kubelet cannot add them before scale-up.
      labels = merge({
        "loom.nebius/node-role"        = each.key == "execution" ? "integration-execution" : each.key
        "loom.nebius/platform"         = "integration"
        "loom.nebius/cluster-scope-id" = var.cluster_scope_id
        }, each.key == "execution" ? {
        "loom.nebius/node-os"   = "linux"
        "loom.nebius/node-arch" = "amd64"
      } : {})
    }
    taints = concat([
      { key = "loom.nebius/platform", value = "integration", effect = "NO_SCHEDULE" }
      ], each.key == "execution" ? [
      { key = "loom.nebius/execution", value = "true", effect = "NO_SCHEDULE" }
    ] : [])
    boot_disk          = { type = each.value.disk_type, size_gibibytes = each.value.disk }
    network_interfaces = [{ subnet_id = var.subnet_id }]
    resources          = { platform = "cpu-e2", preset = each.value.preset }
    reservation_policy = { policy = "FORBID" }
  }
}

output "integration_platform" {
  description = "Non-secret independent integration identities and storage credential references."
  value = var.integration_platform == null ? null : {
    cluster_id           = var.cluster_id
    public_allocation_id = nebius_vpc_v1_allocation.integration_public["integration"].id
    public_cidr          = nebius_vpc_v1_allocation.integration_public["integration"].status.details.allocated_cidr
    registry             = var.registry_fqdn
    storage_endpoint     = "https://storage.${var.region}.nebius.cloud"
    buckets              = { for name, resource in nebius_storage_v1_bucket.integration : name => resource.name }
    storage_credentials = {
      for name, resource in nebius_iam_v2_access_key.integration_store : name => {
        access_key_resource_id = resource.id
        service_account_id     = nebius_iam_v1_service_account.integration_store[name].id
      }
    }
    node_groups = { for name, resource in nebius_mk8s_v1_node_group.integration : name => resource.id }
  }
}
