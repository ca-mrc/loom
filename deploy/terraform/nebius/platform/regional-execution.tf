variable "regional_execution_targets" {
  type = map(object({
    target_id                             = string
    cluster_scope_id                      = string
    project_id                            = string
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
  }))
  default = {}
}
module "regional_execution" {
  for_each = var.regional_execution_targets
  source   = "../modules/regional-execution"
  target   = each.value
}
output "regional_execution_targets" {
  value = { for key, target in module.regional_execution : key => target.execution }
}
