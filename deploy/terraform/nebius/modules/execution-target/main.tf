locals {
  resource_prefix = replace(var.target_id, "nebius-", "loom-")
  # Preserve the labels already attached to the live development-origin
  # resources. Some Nebius IAM relationship resources replace on metadata-only
  # label changes. The physical shared scope is carried by the contract/output
  # and node template without churning those authorities.
  common_labels = merge(var.labels, {
    "loom-target"      = var.target_id
    "loom-environment" = "development"
    "loom-region"      = var.region
    "loom-managed-by"  = "terraform"
  })
}

resource "terraform_data" "contract" {
  input = {
    target_id      = var.target_id
    cluster_scope  = var.cluster_scope_id
    region         = var.region
    failure_domain = var.failure_domain
    bindings       = var.environment_bindings
  }

  lifecycle {
    precondition {
      condition     = strcontains(var.target_id, var.region) && contains([for binding in values(var.environment_bindings) : binding.target_id], var.target_id)
      error_message = "the stable target_id anchor must belong to this shared cluster's region and bindings."
    }

    precondition {
      condition     = var.h100_pool == null || var.region == "eu-north1"
      error_message = "gpu-h100-sxm is available only in eu-north1; do not substitute another GPU platform."
    }

    precondition {
      condition     = var.execution_min_nodes <= var.execution_max_nodes
      error_message = "execution_min_nodes cannot exceed execution_max_nodes."
    }

  }
}

resource "nebius_vpc_v1_pool" "target_private" {
  parent_id  = var.project_id
  name       = "${local.resource_prefix}-private-pool"
  version    = "IPV4"
  visibility = "PRIVATE"
  labels     = local.common_labels

  # Managed Kubernetes reserves service_cidr from the control-plane subnet.
  # The pool must also permit /32 allocations for control-plane and node IPs;
  # /28 would leave the cluster operation stuck before its first allocation.
  cidrs = [
    {
      cidr            = var.network_cidr
      max_mask_length = 32
      state           = "AVAILABLE"
    },
    {
      cidr            = var.service_cidr
      max_mask_length = 32
      state           = "AVAILABLE"
    },
  ]

  depends_on = [terraform_data.contract]
}

resource "nebius_vpc_v1_network" "target" {
  parent_id = var.project_id
  name      = "${local.resource_prefix}-network"
  labels    = local.common_labels

  ipv4_private_pools = {
    pools = [{
      id = nebius_vpc_v1_pool.target_private.id
    }]
  }

  depends_on = [terraform_data.contract]
}

resource "nebius_vpc_v1_subnet" "target" {
  parent_id  = var.project_id
  name       = "${local.resource_prefix}-private"
  network_id = nebius_vpc_v1_network.target.id
  labels     = local.common_labels

  ipv4_private_pools = {
    use_network_pools = true
  }

  # Public Services may opt into the network pools. This only permits
  # allocations; node interfaces still omit public_ip_address.
  ipv4_public_pools = {
    use_network_pools = var.enable_public_service_allocations
  }
}

resource "nebius_registry_v1_registry" "target" {
  parent_id   = var.project_id
  name        = "${local.resource_prefix}-images"
  description = "Digest-pinned Loom images for ${var.target_id}"
  labels      = local.common_labels
}

resource "nebius_iam_v1_service_account" "node_registry_pull" {
  parent_id   = var.project_id
  name        = "${local.resource_prefix}-node-registry-pull"
  description = "Node-only image pull identity for ${var.target_id}"
  labels      = local.common_labels
}

resource "nebius_iam_v1_group" "node_registry_pull" {
  parent_id = var.tenant_id
  name      = "${local.resource_prefix}-node-registry-pull"
  labels    = local.common_labels
}

resource "nebius_iam_v1_group_membership" "node_registry_pull" {
  parent_id = nebius_iam_v1_group.node_registry_pull.id
  member_id = nebius_iam_v1_service_account.node_registry_pull.id
  labels    = local.common_labels
}

resource "nebius_iam_v1_access_permit" "node_registry_pull" {
  parent_id   = nebius_iam_v1_group.node_registry_pull.id
  resource_id = nebius_registry_v1_registry.target.id
  role        = "viewer"
  labels      = local.common_labels
}

resource "nebius_iam_v1_service_account" "capacity_observer" {
  parent_id   = var.project_id
  name        = "${local.resource_prefix}-capacity-observer"
  description = "Read-only quota and node-group observer for ${var.target_id}"
  labels      = local.common_labels
}

resource "nebius_iam_v1_group" "capacity_observer" {
  parent_id = var.tenant_id
  name      = "${local.resource_prefix}-capacity-observer"
  labels    = local.common_labels
}

resource "nebius_iam_v1_group_membership" "capacity_observer" {
  provider  = nebius.no_default_labels
  parent_id = nebius_iam_v1_group.capacity_observer.id
  member_id = nebius_iam_v1_service_account.capacity_observer.id

  # Nebius does not implement Update for group memberships. Provider default
  # labels are state-only metadata for this imported, already-correct binding.
  lifecycle {
    ignore_changes = all
  }
}

resource "nebius_iam_v1_access_permit" "capacity_observer" {
  provider  = nebius.no_default_labels
  parent_id = nebius_iam_v1_group.capacity_observer.id
  # Nebius publishes the effective compute quota limits only on the tenant
  # allowance collection. The narrower tenant auditor role cannot list that
  # API (live permission probe returns PERMISSION_DENIED), so viewer is the
  # least non-mutating role that can combine quota and project node-group
  # readback in one identity.
  resource_id = var.tenant_id
  role        = "viewer"

  # Nebius does not implement Update for access permits.
  lifecycle {
    ignore_changes = all
  }
}

resource "nebius_iam_v1_auth_public_key" "capacity_observer" {
  parent_id = var.project_id
  account = {
    service_account = {
      id = nebius_iam_v1_service_account.capacity_observer.id
    }
  }
  data = var.capacity_observer_public_key_pem
  # expires_at is deliberately omitted. The SDK exchanges this stable key for
  # short-lived access tokens and refreshes those tokens without an operator.

  # Provider 0.6.46 does not return the key material after import, so comparing
  # data would replace a live non-expiring credential on every convergence.
  # Rotation is an explicit two-key operation with old-key failure proof.
  lifecycle {
    ignore_changes = [data]
  }
}

moved {
  from = nebius_iam_v1_group.evidence_writers
  to   = nebius_iam_v1_group.evidence_writers["development"]
}

resource "nebius_iam_v1_group" "evidence_writers" {
  for_each = var.environment_bindings

  parent_id = var.tenant_id
  name      = "${replace(each.value.target_id, "nebius-", "loom-")}-evidence-writers"
  labels = merge(local.common_labels, {
    "loom-target"      = each.value.target_id
    "loom-environment" = each.key
  })
}

resource "nebius_storage_v1_bucket" "evidence" {
  parent_id             = var.project_id
  name                  = var.evidence_bucket_name
  labels                = local.common_labels
  default_storage_class = "STANDARD"
  force_storage_class   = true
  max_size_bytes        = var.evidence_bucket_max_bytes
  object_audit_logging  = "ALL"
  versioning_policy     = "ENABLED"

  bucket_policy = {
    rules = [
      for environment, binding in var.environment_bindings : {
        group_id = nebius_iam_v1_group.evidence_writers[environment].id
        paths    = ["${binding.evidence_prefix}/*"]
        roles    = ["storage.object-editor"]
      }
    ]
  }

  lifecycle_configuration = {
    rules = [{
      id     = "abort-incomplete-uploads"
      status = "ENABLED"
      abort_incomplete_multipart_upload = {
        days_after_initiation = 7
      }
    }]
  }
}

resource "nebius_mk8s_v1_cluster" "target" {
  parent_id = var.project_id
  name      = "${local.resource_prefix}-cluster"
  labels    = local.common_labels

  control_plane = {
    subnet_id         = nebius_vpc_v1_subnet.target.id
    version           = var.kubernetes_version
    etcd_cluster_size = var.control_plane_etcd_size
    audit_logs        = {}
    endpoints = length(var.public_control_plane_cidrs) == 0 ? {} : {
      public_endpoint = {
        allowed_cidrs = var.public_control_plane_cidrs
      }
    }
  }

  kube_network = {
    service_cidrs = [var.service_cidr]
  }
}

# The cluster API remains private. This small, fixed-address gateway is the
# durable operator/automation ingress into the cluster network; it is not an
# execution worker and never participates in user task scheduling.
resource "nebius_vpc_v1_subnet" "deployment_access" {
  parent_id  = var.project_id
  name       = "${local.resource_prefix}-deployment-access"
  network_id = nebius_vpc_v1_network.target.id
  labels     = local.common_labels

  ipv4_private_pools = {
    use_network_pools = true
  }
  ipv4_public_pools = {
    use_network_pools = true
  }
}

resource "nebius_vpc_v1_allocation" "deployment_access" {
  parent_id = var.project_id
  name      = "${local.resource_prefix}-deployment-access"
  labels    = local.common_labels

  ipv4_public = {
    pool_id = var.deployment_access_public_pool_id
    cidr    = "/32"
  }
}

# Reserved independently of the VM so its private service endpoint survives a
# gateway replacement. Attach as a mutable alias, never replace the primary IP.
resource "nebius_vpc_v1_allocation" "deployment_access_private" {
  parent_id = var.project_id
  name      = "${local.resource_prefix}-deployment-access-private"
  labels    = local.common_labels

  ipv4_private = {
    subnet_id = nebius_vpc_v1_subnet.deployment_access.id
    cidr      = "/32"
  }

  # The shared pool also contains the Kubernetes Service CIDR. On a fresh
  # stack, let Kubernetes reserve that block before requesting another /32.
  depends_on = [nebius_mk8s_v1_cluster.target]

  lifecycle {
    postcondition {
      condition     = endswith(self.status.details.allocated_cidr, "/32") && cidrhost("${split("/", self.status.details.allocated_cidr)[0]}/${split("/", var.network_cidr)[1]}", 0) == cidrhost(var.network_cidr, 0)
      error_message = "The gateway service alias must be allocated from the routable node network."
    }
  }
}

resource "nebius_iam_v1_service_account" "deployment_access" {
  parent_id   = var.project_id
  name        = "${local.resource_prefix}-deployment-access"
  description = "Instance identity for persistent Loom deployment access to ${var.target_id}"
  labels      = local.common_labels
}

resource "nebius_iam_v1_group" "deployment_access" {
  parent_id = var.tenant_id
  name      = "${local.resource_prefix}-deployment-access"
  labels    = local.common_labels
}

resource "nebius_iam_v1_group_membership" "deployment_access" {
  parent_id = nebius_iam_v1_group.deployment_access.id
  member_id = nebius_iam_v1_service_account.deployment_access.id
  labels    = local.common_labels
}

resource "nebius_iam_v1_access_permit" "deployment_access_project" {
  parent_id = nebius_iam_v1_group.deployment_access.id
  # Managed Kubernetes does not accept cluster-scoped access permits. Project
  # editor is the narrowest supported scope that can obtain cluster credentials
  # and deploy workloads through the private API.
  resource_id = var.project_id
  role        = "editor"
  labels      = local.common_labels
}

resource "nebius_iam_v1_access_permit" "deployment_access_registry" {
  parent_id   = nebius_iam_v1_group.deployment_access.id
  resource_id = nebius_registry_v1_registry.target.id
  role        = "editor"
  labels      = local.common_labels
}

resource "nebius_compute_v1_instance" "deployment_access" {
  parent_id          = var.project_id
  name               = "${local.resource_prefix}-deployment-access"
  hostname           = "loom-nebius-deployment-access"
  labels             = local.common_labels
  stopped            = false
  recovery_policy    = "RECOVER"
  service_account_id = nebius_iam_v1_service_account.deployment_access.id

  resources = {
    platform = "cpu-e2"
    preset   = var.deployment_access_preset
  }
  reservation_policy = {
    policy = "FORBID"
  }
  boot_disk = {
    attach_mode = "READ_WRITE"
    managed_disk = {
      name   = "${local.resource_prefix}-deployment-access"
      labels = local.common_labels
      spec = {
        size_gibibytes  = var.deployment_access_disk_gib
        type            = "NETWORK_SSD"
        source_image_id = var.deployment_access_image_id
      }
    }
  }
  network_interfaces = [{
    name       = "eth0"
    subnet_id  = nebius_vpc_v1_subnet.deployment_access.id
    ip_address = {}
    aliases = [{
      allocation_id = nebius_vpc_v1_allocation.deployment_access_private.id
    }]
    public_ip_address = {
      allocation_id = nebius_vpc_v1_allocation.deployment_access.id
    }
  }]
  cloud_init_user_data = <<-CLOUD_INIT
    #cloud-config
    users:
      - name: codex
        sudo: ["ALL=(ALL) NOPASSWD:ALL"]
        shell: /bin/bash
        ssh_authorized_keys:
          - ${trimspace(var.deployment_access_ssh_public_key)}
    packages:
      - sudo
      - wireguard-tools
    ssh_pwauth: false
    disable_root: true
  CLOUD_INIT

  depends_on = [
    nebius_iam_v1_access_permit.deployment_access_project,
    nebius_iam_v1_access_permit.deployment_access_registry,
  ]
}

resource "nebius_mk8s_v1_node_group" "system" {
  parent_id        = nebius_mk8s_v1_cluster.target.id
  name             = "${local.resource_prefix}-system"
  labels           = local.common_labels
  version          = var.kubernetes_version
  fixed_node_count = var.system_node_count
  auto_repair      = {}

  strategy = {
    drain_timeout = "10m"
    max_surge = {
      count = 1
    }
    max_unavailable = {
      count = 0
    }
  }

  template = {
    service_account_id = nebius_iam_v1_service_account.node_registry_pull.id
    max_pods           = 32
    metadata = {
      labels = {
        "loom.nebius/node-role"        = "system"
        "loom.nebius/cluster-scope-id" = var.cluster_scope_id
      }
    }
    boot_disk = {
      type           = "NETWORK_SSD"
      size_gibibytes = var.system_disk_gib
    }
    network_interfaces = [{
      subnet_id = nebius_vpc_v1_subnet.target.id
    }]
    resources = {
      platform = "cpu-e2"
      preset   = var.system_preset
    }
    reservation_policy = {
      policy = "FORBID"
    }
  }

  depends_on = [nebius_iam_v1_access_permit.node_registry_pull]
}

resource "nebius_mk8s_v1_node_group" "execution" {
  parent_id   = nebius_mk8s_v1_cluster.target.id
  name        = "${local.resource_prefix}-execution"
  labels      = local.common_labels
  version     = var.kubernetes_version
  auto_repair = {}

  autoscaling = {
    min_node_count = var.execution_min_nodes
    max_node_count = var.execution_max_nodes
  }

  strategy = {
    drain_timeout = "20m"
    max_surge = {
      # The provider envelope is sized for the complete execution pool plus
      # persistent infrastructure, not an extra replacement node. Replace one
      # drained node at a time without exceeding provider quota.
      count = 0
    }
    max_unavailable = {
      count = 1
    }
  }

  template = {
    service_account_id = nebius_iam_v1_service_account.node_registry_pull.id
    max_pods           = var.execution_max_pods
    metadata = {
      labels = {
        "loom.nebius/node-role"        = "execution"
        "loom.nebius/cluster-scope-id" = var.cluster_scope_id
      }
    }
    taints = [{
      key    = "loom.nebius/execution"
      value  = "true"
      effect = "NO_SCHEDULE"
    }]
    boot_disk = {
      type           = "NETWORK_SSD"
      size_gibibytes = var.execution_disk_gib
    }
    network_interfaces = [{
      subnet_id = nebius_vpc_v1_subnet.target.id
    }]
    resources = {
      platform = "cpu-e2"
      preset   = var.execution_preset
    }
    reservation_policy = {
      policy = "FORBID"
    }
  }

  depends_on = [
    nebius_iam_v1_access_permit.node_registry_pull,
    nebius_mk8s_v1_node_group.system,
  ]
}

resource "nebius_mk8s_v1_node_group" "h100_execution" {
  count       = var.h100_pool == null ? 0 : 1
  parent_id   = nebius_mk8s_v1_cluster.target.id
  name        = "${local.resource_prefix}-execution-h100"
  labels      = local.common_labels
  version     = var.kubernetes_version
  auto_repair = {}

  autoscaling = {
    min_node_count = 0
    max_node_count = var.h100_pool.max_nodes
  }
  strategy = {
    drain_timeout   = "20m"
    max_surge       = { count = 0 }
    max_unavailable = { count = 1 }
  }
  template = {
    service_account_id = nebius_iam_v1_service_account.node_registry_pull.id
    max_pods           = var.execution_max_pods
    metadata = {
      labels = {
        "loom.nebius/node-role"        = "execution"
        "loom.nebius/cluster-scope-id" = var.cluster_scope_id
        "loom.nebius/resource-pool"    = "h100"
        "loom.nebius/gpu-model"        = "H100"
      }
    }
    taints = [
      { key = "loom.nebius/execution", value = "true", effect = "NO_SCHEDULE" },
      { key = "loom.nebius/gpu-model", value = "H100", effect = "NO_SCHEDULE" },
    ]
    boot_disk = {
      type           = "NETWORK_SSD"
      size_gibibytes = var.h100_pool.disk_gib
    }
    network_interfaces = [{ subnet_id = nebius_vpc_v1_subnet.target.id }]
    resources = {
      platform = "gpu-h100-sxm"
      preset   = "1gpu-16vcpu-200gb"
    }
    gpu_settings       = { drivers_preset = var.h100_pool.drivers_preset }
    reservation_policy = { policy = "FORBID" }
  }
  depends_on = [nebius_iam_v1_access_permit.node_registry_pull, terraform_data.contract]
}
