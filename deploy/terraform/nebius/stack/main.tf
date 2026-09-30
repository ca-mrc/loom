module "execution_target" {
  source = "../modules/execution-target"

  providers = {
    nebius                   = nebius
    nebius.no_default_labels = nebius.no_default_labels
  }

  tenant_id                         = var.tenant_id
  project_id                        = var.project_id
  target_id                         = var.target.target_id
  cluster_scope_id                  = var.target.cluster_scope_id
  region                            = var.target.region
  failure_domain                    = var.target.failure_domain
  environment_bindings              = var.target.environment_bindings
  network_cidr                      = var.target.network_cidr
  service_cidr                      = var.target.service_cidr
  kubernetes_version                = var.target.kubernetes_version
  control_plane_etcd_size           = var.target.control_plane_etcd_size
  public_control_plane_cidrs        = var.target.public_control_plane_cidrs
  system_node_count                 = var.target.system_node_count
  execution_min_nodes               = var.target.execution_min_nodes
  execution_max_nodes               = var.target.execution_max_nodes
  execution_max_pods                = var.target.execution_max_pods
  system_preset                     = var.target.system_preset
  execution_preset                  = var.target.execution_preset
  h100_pool                         = var.target.h100_pool
  system_disk_gib                   = var.target.system_disk_gib
  execution_disk_gib                = var.target.execution_disk_gib
  evidence_bucket_name              = var.evidence_bucket_name
  evidence_bucket_max_bytes         = var.evidence_bucket_max_bytes
  capacity_observer_public_key_pem  = var.capacity_observer_public_key_pem
  deployment_access_public_pool_id  = var.deployment_access_public_pool_id
  deployment_access_ssh_public_key  = var.deployment_access_ssh_public_key
  deployment_access_image_id        = var.deployment_access_image_id
  deployment_access_preset          = var.deployment_access_preset
  deployment_access_disk_gib        = var.deployment_access_disk_gib
  staging_spool                     = var.staging_spool
  enable_public_service_allocations = var.enable_public_service_allocations
}
