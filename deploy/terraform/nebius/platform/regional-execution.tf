variable "regional_execution_targets" {
  type = map(object({
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
    execution_disk_type = optional(string, "NETWORK_SSD")
    execution_disk_gib  = optional(number, 80)
  }))
  default = {}
  validation {
    condition     = alltrue([for target in values(var.regional_execution_targets) : target.system_node_count >= 1 && floor(target.system_node_count) == target.system_node_count])
    error_message = "Regional system_node_count must be a positive integer."
  }
  validation {
    condition = alltrue([for target in values(var.regional_execution_targets) :
      contains(["NETWORK_SSD", "NETWORK_SSD_NON_REPLICATED"], target.execution_disk_type) &&
      target.execution_disk_gib >= 80 && floor(target.execution_disk_gib) == target.execution_disk_gib &&
      (target.execution_disk_type != "NETWORK_SSD_NON_REPLICATED" || target.execution_disk_gib % 93 == 0)
    ])
    error_message = "Regional execution disk must be an integer of at least 80 GiB, using NETWORK_SSD or NETWORK_SSD_NON_REPLICATED in multiples of 93 GiB."
  }
}
module "regional_execution" {
  for_each = var.regional_execution_targets
  source   = "../modules/regional-execution"
  target   = each.value
}
output "regional_execution_targets" {
  value = { for key, target in module.regional_execution : key => target.execution }
}
