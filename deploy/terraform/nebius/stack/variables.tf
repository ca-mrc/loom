variable "nebius_profile" {
  description = "Locally configured Nebius CLI profile. The profile file is never committed."
  type        = string
  default     = "default"
}

variable "tenant_id" {
  description = "Exact tenant for target-scoped IAM groups."
  type        = string
}

variable "project_id" {
  description = "Exact pre-existing, separately authorized project for this target."
  type        = string
}

variable "target" {
  description = "One shared physical cluster with three environment-local bindings."
  type = object({
    target_id        = string
    cluster_scope_id = string
    region           = string
    failure_domain   = string
    environment_bindings = map(object({
      target_id       = string
      namespace_name  = string
      evidence_prefix = string
    }))
    network_cidr               = string
    service_cidr               = string
    kubernetes_version         = string
    control_plane_etcd_size    = number
    public_control_plane_cidrs = list(string)
    system_node_count          = number
    execution_min_nodes        = number
    execution_max_nodes        = number
    execution_max_pods         = number
    system_preset              = string
    execution_preset           = string
    system_disk_gib            = number
    execution_disk_gib         = number
    h100_pool = optional(object({
      max_nodes      = number
      disk_gib       = number
      drivers_preset = string
    }))
  })
}

variable "evidence_bucket_name" {
  description = "Globally unique bucket name chosen at authorized plan time."
  type        = string
}

variable "evidence_bucket_max_bytes" {
  description = "Hard evidence storage ceiling."
  type        = number
  default     = 107374182400
}

variable "capacity_observer_public_key_pem" {
  description = "PEM public key for the non-expiring capacity-observer authorized key."
  type        = string
  sensitive   = true
}

variable "deployment_access_public_pool_id" {
  description = "Existing project public IPv4 pool for the persistent deployment gateway."
  type        = string
}

variable "deployment_access_ssh_public_key" {
  description = "OpenSSH public key for the non-root deployment gateway operator."
  type        = string
}

variable "deployment_access_image_id" {
  description = "Pinned Nebius image ID for the persistent deployment gateway."
  type        = string
}

variable "deployment_access_preset" {
  description = "Pinned regular CPU preset for the deployment gateway."
  type        = string
  default     = "2vcpu-8gb"
}

variable "deployment_access_disk_gib" {
  description = "Deployment gateway boot disk size."
  type        = number
  default     = 40
}

variable "staging_spool" {
  description = "Optional dedicated staging source spool. Omit to preserve the current stack."
  type = object({
    bucket_name = string
  })
  default = null
}

variable "enable_public_service_allocations" {
  description = "Allow public LoadBalancer allocations from the target network pools. Nodes still receive private addresses only."
  type        = bool
  default     = false
}
