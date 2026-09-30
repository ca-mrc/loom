output "target" {
  value = module.execution_target.target
}

output "network" {
  value = module.execution_target.network
}

output "cluster" {
  value = module.execution_target.cluster
}

output "node_groups" {
  value = module.execution_target.node_groups
}

output "registry" {
  value = module.execution_target.registry
}

output "evidence" {
  value = module.execution_target.evidence
}

output "node_registry_pull_service_account_id" {
  value = module.execution_target.node_registry_pull_service_account_id
}

output "capacity_observer_service_account_id" {
  value = module.execution_target.capacity_observer_service_account_id
}

output "capacity_observer_public_key_id" {
  value = module.execution_target.capacity_observer_public_key_id
}

output "deployment_access" {
  value = module.execution_target.deployment_access
}

output "staging_spool" {
  description = "Protected staging source-spool configuration and GetSecret reference, not secret bytes."
  sensitive   = true
  value       = module.execution_target.staging_spool
}

output "h100_resource_pool" {
  value = module.execution_target.h100_resource_pool
}
