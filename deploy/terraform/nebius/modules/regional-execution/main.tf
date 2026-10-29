# Execution-only regional extension, with optional native runtime identities.
# Only caller-supplied PUBLIC keys enter Terraform; private credentials never do.
variable "target" {
  type = object({
    target_id                             = string
    cluster_scope_id                      = string
    project_id                            = string
    system_node_count                     = optional(number, 1)
    subnet_id                             = string
    region                                = string
    kubernetes_version                    = string
    service_cidr                          = string
    public_control_plane_cidrs            = optional(list(string), [])
    node_registry_pull_service_account_id = optional(string)
    managed_identities = optional(object({
      actuator_public_key       = string
      collector_public_key      = string
      gateway_public_key        = string
      collector_viewer_group_id = string
      registry_pull_group_id    = string
    }))
    suspended           = optional(bool, false)
    execution_max_nodes = optional(number, 100)
    node_platform       = string
    system_preset       = optional(string, "4vcpu-16gb")
    execution_preset    = optional(string, "16vcpu-64gb")
    execution_disk_gib  = optional(number, 80)
  })
  validation {
    condition     = var.target.managed_identities == null ? can(regex("^serviceaccount-[a-zA-Z0-9_-]+$", var.target.node_registry_pull_service_account_id)) : var.target.node_registry_pull_service_account_id == null
    error_message = "Choose existing node_registry_pull_service_account_id OR managed_identities, never both."
  }
  validation {
    condition     = var.target.system_node_count >= 1 && floor(var.target.system_node_count) == var.target.system_node_count
    error_message = "Regional system_node_count must be a positive integer."
  }
  validation {
    condition = var.target.managed_identities == null ? true : (
      alltrue([for value in [var.target.managed_identities.actuator_public_key, var.target.managed_identities.collector_public_key, var.target.managed_identities.gateway_public_key] : can(regex("(?s)^-----BEGIN PUBLIC KEY-----[\\r\\n]+[A-Za-z0-9+/=\\r\\n]+-----END PUBLIC KEY-----$", trimspace(value)))]) &&
      length(distinct([trimspace(var.target.managed_identities.actuator_public_key), trimspace(var.target.managed_identities.collector_public_key), trimspace(var.target.managed_identities.gateway_public_key)])) == 3 &&
      alltrue([for value in [var.target.managed_identities.collector_viewer_group_id, var.target.managed_identities.registry_pull_group_id] : can(regex("^group-[a-zA-Z0-9_-]+$", value))]) &&
      var.target.managed_identities.collector_viewer_group_id != var.target.managed_identities.registry_pull_group_id
    )
    error_message = "Managed identities require three distinct PEM PUBLIC keys and two distinct existing viewer group IDs; private keys are forbidden."
  }
  validation {
    condition = (
      can(regex("^nebius-[a-z0-9-]+$", var.target.target_id)) &&
      startswith(var.target.region, "eu-") &&
      can(regex("^cpu-[a-z0-9-]+$", var.target.node_platform)) &&
      can(cidrnetmask(var.target.service_cidr)) &&
      alltrue([for cidr in var.target.public_control_plane_cidrs : can(cidrnetmask(cidr))]) &&
      var.target.execution_max_nodes >= 1 && var.target.execution_max_nodes <= 100 &&
      floor(var.target.execution_max_nodes) == var.target.execution_max_nodes &&
      contains(["4vcpu-16gb", "8vcpu-32gb"], var.target.system_preset) &&
      can(regex("^[1-9][0-9]*vcpu-[1-9][0-9]*gb$", var.target.execution_preset)) &&
      var.target.execution_disk_gib >= 80 &&
      floor(var.target.execution_disk_gib) == var.target.execution_disk_gib
    )
    error_message = "Regional execution needs EU placement, an explicit CPU platform, valid optional API CIDRs, network, integer node ceiling 1..100, a CPU execution preset in <vCPU>vcpu-<GiB>gb format, and an integer execution disk of at least 80 GiB."
  }
}
locals {
  labels = { "loom-managed-by" = "terraform", "loom-target" = var.target.target_id }
  runtime_keys = var.target.managed_identities == null ? {} : {
    actuator  = var.target.managed_identities.actuator_public_key
    collector = var.target.managed_identities.collector_public_key
    gateway   = var.target.managed_identities.gateway_public_key
  }
  identities = var.target.managed_identities == null ? toset([]) : toset(["actuator", "collector", "gateway", "node-pull"])
  viewer_groups = var.target.managed_identities == null ? {} : {
    collector = var.target.managed_identities.collector_viewer_group_id
    node-pull = var.target.managed_identities.registry_pull_group_id
  }
  node_pull_identity = var.target.managed_identities == null ? var.target.node_registry_pull_service_account_id : nebius_iam_v1_service_account.regional["node-pull"].id
  nodes = {
    system    = { preset = var.target.system_preset, disk_gib = 80 }
    execution = { preset = var.target.execution_preset, disk_gib = var.target.execution_disk_gib }
  }
}
# Reuse the reviewed viewer-only groups. No new group, access permit, project
# editor or workload-secret grant is created. Kubernetes RBAC remains separate.
resource "nebius_iam_v1_service_account" "regional" {
  for_each    = local.identities
  parent_id   = var.target.project_id
  name        = "${var.target.target_id}-${each.key}"
  description = "Regional execution ${each.key}; authority supplied by explicit viewer membership or Kubernetes RBAC"
  labels      = local.labels
}
resource "nebius_iam_v1_auth_public_key" "regional" {
  for_each  = local.runtime_keys
  parent_id = var.target.project_id
  name      = "${var.target.target_id}-${each.key}"
  account   = { service_account = { id = nebius_iam_v1_service_account.regional[each.key].id } }
  data      = each.value
  # Omitted expiry permits native short-lived-token refresh without a calendar
  # outage. Registering/rotating a key remains an explicit Terraform operation.
}
resource "nebius_iam_v1_group_membership" "regional" {
  for_each  = local.viewer_groups
  parent_id = each.value
  member_id = nebius_iam_v1_service_account.regional[each.key].id
}
resource "nebius_mk8s_v1_cluster" "regional" {
  parent_id = var.target.project_id
  name      = "${var.target.target_id}-cluster"
  labels    = local.labels
  control_plane = {
    subnet_id         = var.target.subnet_id
    version           = var.target.kubernetes_version
    etcd_cluster_size = 1
    audit_logs        = {}
    endpoints         = { public_endpoint = { allowed_cidrs = var.target.public_control_plane_cidrs } }
  }
  kube_network = { service_cidrs = [var.target.service_cidr] }
}
resource "nebius_mk8s_v1_node_group" "regional" {
  depends_on       = [nebius_iam_v1_group_membership.regional]
  for_each         = local.nodes
  parent_id        = nebius_mk8s_v1_cluster.regional.id
  name             = "${var.target.target_id}-${each.key}"
  labels           = local.labels
  version          = var.target.kubernetes_version
  auto_repair      = {}
  fixed_node_count = var.target.suspended ? 0 : (each.key == "system" ? var.target.system_node_count : null)
  autoscaling      = var.target.suspended || each.key == "system" ? null : { min_node_count = 0, max_node_count = var.target.execution_max_nodes }
  strategy         = { drain_timeout = "20m", max_surge = { count = 0 }, max_unavailable = { count = 1 } }
  template = {
    service_account_id = local.node_pull_identity
    max_pods           = 64
    metadata = {
      # The min-zero template must match the native execution Pod selectors
      # before kubelet can add its own standard OS/architecture labels.
      labels = merge({
        "loom.nebius/node-role"        = each.key == "execution" ? "integration-execution" : "system"
        "loom.nebius/platform"         = "integration"
        "loom.nebius/cluster-scope-id" = var.target.cluster_scope_id
        }, each.key == "execution" ? {
        "loom.nebius/node-os"   = "linux"
        "loom.nebius/node-arch" = "amd64"
      } : {})
    }
    # Native addons must be schedulable before Cilium can initialize the cluster.
    # Keep workload isolation on execution nodes, not the dedicated system node.
    taints = each.key == "execution" ? [
      { key = "loom.nebius/platform", value = "integration", effect = "NO_SCHEDULE" },
      { key = "loom.nebius/execution", value = "true", effect = "NO_SCHEDULE" }
    ] : []
    boot_disk          = { type = "NETWORK_SSD", size_gibibytes = each.value.disk_gib }
    network_interfaces = [{ subnet_id = var.target.subnet_id }]
    resources          = { platform = var.target.node_platform, preset = each.value.preset }
    reservation_policy = { policy = "FORBID" }
  }
}
output "execution" {
  value = {
    suspended                             = var.target.suspended
    target_id                             = var.target.target_id
    region                                = var.target.region
    cluster_scope_id                      = var.target.cluster_scope_id
    cluster_id                            = nebius_mk8s_v1_cluster.regional.id
    execution_node_group_id               = nebius_mk8s_v1_node_group.regional["execution"].id
    kubernetes_api_server                 = nebius_mk8s_v1_cluster.regional.status.control_plane.endpoints.public_endpoint
    node_registry_pull_service_account_id = local.node_pull_identity
    service_account_ids                   = { for role, key in nebius_iam_v1_auth_public_key.regional : role => nebius_iam_v1_service_account.regional[role].id }
    authorized_public_key_ids             = { for role, key in nebius_iam_v1_auth_public_key.regional : role => key.id }
    group_membership_ids                  = { for role, membership in nebius_iam_v1_group_membership.regional : role => membership.id }
  }
}
