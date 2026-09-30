# Existing shared cluster identities are inputs, not managed/imported here.
variable "tenant_id" { type = string }
variable "project_id" { type = string }
variable "cluster_id" { type = string }
variable "cluster_scope_id" { type = string }
variable "subnet_id" { type = string }
variable "public_pool_id" { type = string }
variable "node_registry_pull_service_account_id" { type = string }
variable "registry_fqdn" { type = string }
variable "region" { type = string }
variable "kubernetes_version" { type = string }
variable "labels" {
  type = map(string)
  default = {
    "loom-managed-by" = "terraform"
    "loom-stack"      = "nebius-platform"
  }
}
variable "nebius_profile" { type = string }
variable "integration_platform" {
  type = object({
    bucket_prefix              = string
    system_preset              = optional(string, "4vcpu-16gb")
    system_disk_gib            = optional(number, 80)
    system_max_pods            = optional(number, 64)
    system_create_before_drain = optional(bool, false)
    execution_max_nodes        = optional(number, 100)
    native_builder_group_id    = optional(string)
  })
}
