variable "tenant_id" {
  description = "Nebius tenant that owns the target project and IAM groups."
  type        = string

  validation {
    condition     = startswith(var.tenant_id, "tenant-")
    error_message = "tenant_id must be an exact Nebius tenant ID."
  }
}

variable "project_id" {
  description = "Existing, separately authorized Nebius project for this one target."
  type        = string

  validation {
    condition     = startswith(var.project_id, "project-")
    error_message = "project_id must be an exact Nebius project ID."
  }
}

variable "target_id" {
  description = "Stable state/resource anchor retained from the development cluster during shared-cluster convergence."
  type        = string

  validation {
    condition     = can(regex("^nebius-[a-z0-9-]+-(development|staging|production)$", var.target_id))
    error_message = "target_id must be a canonical Nebius execution target ID."
  }
}

variable "cluster_scope_id" {
  description = "Physical shared-cluster identity bound by every logical environment target."
  type        = string

  validation {
    condition     = can(regex("^nebius-[a-z0-9-]+-shared$", var.cluster_scope_id))
    error_message = "cluster_scope_id must identify the shared Nebius cluster."
  }
}

variable "region" {
  description = "Nebius region encoded in the project and target identity."
  type        = string

  validation {
    condition     = contains(["eu-north1", "eu-west1"], var.region)
    error_message = "The accepted Loom target matrix currently permits eu-north1 and eu-west1 only."
  }
}

variable "failure_domain" {
  description = "Unique failure domain recorded by Loom routing evidence."
  type        = string

  validation {
    condition     = length(trimspace(var.failure_domain)) > 0
    error_message = "failure_domain must be explicit."
  }
}

variable "environment_bindings" {
  description = "Exactly one isolated namespace and evidence prefix for each Loom environment on the shared cluster."
  type = map(object({
    target_id       = string
    namespace_name  = string
    evidence_prefix = string
  }))

  validation {
    condition     = toset(keys(var.environment_bindings)) == toset(["development", "staging", "production"])
    error_message = "environment_bindings must contain exactly development, staging, and production."
  }

  validation {
    condition = alltrue([
      for environment, binding in var.environment_bindings :
      can(regex("^nebius-[a-z0-9-]+-${environment}$", binding.target_id)) &&
      binding.namespace_name == "loom-nebius-${environment}" &&
      binding.evidence_prefix == binding.target_id
    ])
    error_message = "every environment binding must use its canonical target, namespace, and evidence prefix."
  }
}

variable "network_cidr" {
  description = "Private CIDR reserved for the target network."
  type        = string

  validation {
    condition     = can(cidrhost(var.network_cidr, 1)) && startswith(var.network_cidr, "10.")
    error_message = "network_cidr must be a valid RFC1918 10/8 CIDR."
  }
}

variable "service_cidr" {
  description = "Non-overlapping Kubernetes service CIDR for the target."
  type        = string

  validation {
    condition     = can(cidrhost(var.service_cidr, 1)) && startswith(var.service_cidr, "172.")
    error_message = "service_cidr must be a valid non-overlapping 172/8 CIDR."
  }
}

variable "kubernetes_version" {
  description = "Pinned Managed Kubernetes minor version."
  type        = string
  default     = "1.35"

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version must be a pinned major.minor version."
  }
}

variable "control_plane_etcd_size" {
  description = "Pinned etcd member count for the shared cluster; HA expansion requires a separately accepted SLO."
  type        = number

  validation {
    condition     = contains([1, 3], var.control_plane_etcd_size)
    error_message = "control_plane_etcd_size must be 1 or 3."
  }
}

variable "public_control_plane_cidrs" {
  description = "Exact operator CIDRs allowed to the API. Empty means private control plane only."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr in var.public_control_plane_cidrs : can(cidrhost(cidr, 0)) && cidr != "0.0.0.0/0"
    ])
    error_message = "Control-plane CIDRs must be valid and may never allow 0.0.0.0/0."
  }
}

variable "system_node_count" {
  description = "Fixed regular on-demand nodes for cluster services and actuator components."
  type        = number

  validation {
    condition     = var.system_node_count >= 1 && var.system_node_count <= 5
    error_message = "system_node_count must be between 1 and 5."
  }
}

variable "execution_min_nodes" {
  description = "Minimum regular on-demand execution nodes; the baseline shared pool scales to zero."
  type        = number

  validation {
    condition     = var.execution_min_nodes >= 0
    error_message = "execution_min_nodes cannot be negative."
  }
}

variable "execution_max_nodes" {
  description = "Hard target-local autoscaling ceiling for execution nodes."
  type        = number

  validation {
    condition     = var.execution_max_nodes >= 1 && var.execution_max_nodes <= 100
    error_message = "execution_max_nodes must be between 1 and 100."
  }
}

variable "execution_max_pods" {
  description = "Maximum Pods per execution node, including platform DaemonSets."
  type        = number
  default     = 64

  validation {
    condition     = var.execution_max_pods >= 16 && var.execution_max_pods <= 110
    error_message = "execution_max_pods must be between 16 and 110."
  }
}

variable "system_preset" {
  description = "Pinned regular CPU preset for system nodes."
  type        = string
  default     = "2vcpu-8gb"
}

variable "execution_preset" {
  description = "Pinned regular CPU preset for attempt nodes."
  type        = string
  default     = "2vcpu-8gb"
}

variable "system_disk_gib" {
  description = "System-node NETWORK_SSD boot disk size."
  type        = number
  default     = 64

  validation {
    condition     = var.system_disk_gib >= 64
    error_message = "Nebius node boot disks must be at least 64 GiB."
  }
}

variable "execution_disk_gib" {
  description = "Execution-node NETWORK_SSD boot disk size."
  type        = number
  default     = 64

  validation {
    condition     = var.execution_disk_gib >= 64
    error_message = "Nebius node boot disks must be at least 64 GiB."
  }
}

variable "evidence_bucket_name" {
  description = "Globally unique target-local Object Storage bucket for immutable acceptance evidence."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{2,61}[a-z0-9]$", var.evidence_bucket_name))
    error_message = "evidence_bucket_name must be a valid globally unique bucket name."
  }
}

variable "evidence_bucket_max_bytes" {
  description = "Hard storage budget for target evidence."
  type        = number
  default     = 107374182400

  validation {
    condition     = var.evidence_bucket_max_bytes > 0
    error_message = "evidence_bucket_max_bytes must be positive."
  }
}

variable "capacity_observer_public_key_pem" {
  description = "PEM public key for the non-expiring capacity-observer authorized key. Keep the matching private credential only in the runtime secret store."
  type        = string
  sensitive   = true

  validation {
    condition     = startswith(trimspace(var.capacity_observer_public_key_pem), "-----BEGIN PUBLIC KEY-----")
    error_message = "capacity_observer_public_key_pem must be a PEM public key."
  }
}

variable "deployment_access_public_pool_id" {
  description = "Existing project public IPv4 pool used by the persistent deployment gateway allocation."
  type        = string

  validation {
    condition     = startswith(var.deployment_access_public_pool_id, "vpcpool-")
    error_message = "deployment_access_public_pool_id must be an exact Nebius VPC pool ID."
  }
}

variable "deployment_access_ssh_public_key" {
  description = "OpenSSH public key for the non-root deployment gateway operator."
  type        = string

  validation {
    condition     = can(regex("^ssh-ed25519 [A-Za-z0-9+/=]+(?: .*)?$", trimspace(var.deployment_access_ssh_public_key)))
    error_message = "deployment_access_ssh_public_key must be one OpenSSH Ed25519 public key."
  }
}

variable "deployment_access_image_id" {
  description = "Pinned Nebius Ubuntu image used by the persistent deployment gateway."
  type        = string

  validation {
    condition     = startswith(var.deployment_access_image_id, "computeimage-")
    error_message = "deployment_access_image_id must be an exact Nebius Compute image ID."
  }
}

variable "deployment_access_preset" {
  description = "Pinned regular CPU preset for the deployment gateway."
  type        = string
  default     = "2vcpu-8gb"
}

variable "deployment_access_disk_gib" {
  description = "Persistent deployment gateway NETWORK_SSD boot disk size."
  type        = number
  default     = 40

  validation {
    condition     = var.deployment_access_disk_gib >= 40
    error_message = "The pinned deployment gateway image requires at least 40 GiB."
  }
}

variable "labels" {
  description = "Additional non-secret labels applied to every supported Nebius resource."
  type        = map(string)
  default     = {}
}

variable "staging_spool" {
  description = "Opt-in dedicated staging runtime spool; null preserves existing resources and credentials."
  type = object({
    bucket_name = string
  })
  default = null

  validation {
    condition = var.staging_spool == null ? true : (
      can(regex("^loom-[a-z0-9-]+-staging-spool$", var.staging_spool.bucket_name)) &&
      length(var.staging_spool.bucket_name) <= 63 &&
      var.staging_spool.bucket_name != var.evidence_bucket_name
    )
    error_message = "staging_spool must name a new dedicated loom-*-staging-spool bucket (at most 63 characters), never the evidence or state bucket."
  }
}

variable "enable_public_service_allocations" {
  description = "Allow public LoadBalancer allocations from the target network pools. Nodes still receive private addresses only."
  type        = bool
  default     = false
}

variable "h100_pool" {
  description = "Opt-in dedicated H100 pool; null preserves the CPU-only topology. This config is not runtime qualification."
  type = object({
    max_nodes      = number
    disk_gib       = number
    drivers_preset = string
  })
  default = null

  validation {
    condition = var.h100_pool == null ? true : (
      var.h100_pool.max_nodes >= 1 && var.h100_pool.max_nodes <= 32 &&
      floor(var.h100_pool.max_nodes) == var.h100_pool.max_nodes &&
      var.h100_pool.disk_gib >= 1100 && floor(var.h100_pool.disk_gib) == var.h100_pool.disk_gib &&
      contains(["cuda12", "cuda12.4", "cuda12.8"], var.h100_pool.drivers_preset)
    )
    error_message = "H100 pool needs an explicit 1-32 node ceiling, at least 1100 GiB disk, and a supported driver preset."
  }
}
