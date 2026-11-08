# Read with: terraform output            (non-sensitive)
#            terraform output -raw sa_kafka_basic_api_secret   (one sensitive value)
# Other modules can read these with a terraform_remote_state data source pointing at this
# folder's state, or simply copy the values into their own tfvars.

output "environment_id" {
  value = confluent_environment.crash_course.id
}

output "service_account_id" {
  value = confluent_service_account.sa_crash_admin.id
}

# --- cc-basic ---
output "basic_cluster_id" {
  value = confluent_kafka_cluster.basic.id
}

output "basic_bootstrap_endpoint" {
  value = confluent_kafka_cluster.basic.bootstrap_endpoint
}

output "basic_rest_endpoint" {
  value = confluent_kafka_cluster.basic.rest_endpoint
}

output "sa_kafka_basic_api_key" {
  value = confluent_api_key.sa_crash_admin_kafka_basic.id
}

output "sa_kafka_basic_api_secret" {
  value     = confluent_api_key.sa_crash_admin_kafka_basic.secret
  sensitive = true
}

# --- Schema Registry (Essentials) ---
output "schema_registry_id" {
  value = data.confluent_schema_registry_cluster.essentials.id
}

output "schema_registry_rest_endpoint" {
  value = data.confluent_schema_registry_cluster.essentials.rest_endpoint
}

output "sa_sr_api_key" {
  value = confluent_api_key.sa_crash_admin_sr.id
}

output "sa_sr_api_secret" {
  value     = confluent_api_key.sa_crash_admin_sr.secret
  sensitive = true
}

# --- paid-session (null when the toggle is off) ---
output "dedicated_cluster_id" {
  value = try(confluent_kafka_cluster.dedicated[0].id, null)
}

output "dedicated_bootstrap_endpoint" {
  value = try(confluent_kafka_cluster.dedicated[0].bootstrap_endpoint, null)
}

output "dedicated_rest_endpoint" {
  value = try(confluent_kafka_cluster.dedicated[0].rest_endpoint, null)
}

output "sa_kafka_dedicated_api_key" {
  value = try(confluent_api_key.sa_crash_admin_kafka_dedicated[0].id, null)
}

output "sa_kafka_dedicated_api_secret" {
  value     = try(confluent_api_key.sa_crash_admin_kafka_dedicated[0].secret, null)
  sensitive = true
}

output "enterprise_cluster_id" {
  value = try(confluent_kafka_cluster.enterprise[0].id, null)
}

output "private_link_attachment_id" {
  value = try(confluent_private_link_attachment.azure[0].id, null)
}

output "private_link_service_alias" {
  description = "Azure Private Link service alias for module 02's private endpoint."
  value       = try(confluent_private_link_attachment.azure[0].azure[0].private_link_service_alias, null)
}

output "private_link_dns_domain" {
  value = try(confluent_private_link_attachment.azure[0].dns_domain, null)
}
