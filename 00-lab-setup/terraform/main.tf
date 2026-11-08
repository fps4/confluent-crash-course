# cc-basic: environment `crash-course`, Basic cluster `cc-basic` on Azure westeurope,
# Schema Registry (Stream Governance Essentials), service account `sa-crash-admin` with
# a Kafka API key and a Schema Registry API key.
#
# Essentials is the free governance package. It does NOT run data-contract rules
# (CEL / JSONata / encryption rules need Advanced). An environment cannot be downgraded from
# Advanced to Essentials, so keep this one on Essentials and do rule labs locally (dc-a/sr-a)
# or in a throw-away Advanced environment.

resource "confluent_environment" "crash_course" {
  display_name = var.environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "basic" {
  display_name = var.basic_cluster_name
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region
  basic {}

  environment {
    id = confluent_environment.crash_course.id
  }
}

# Schema Registry is provisioned automatically per environment; read it, do not create it.
data "confluent_schema_registry_cluster" "essentials" {
  environment {
    id = confluent_environment.crash_course.id
  }

  depends_on = [
    confluent_kafka_cluster.basic
  ]
}

# ---------------------------------------------------------------------------
# Service account used by all labs. EnvironmentAdmin covers every cluster and the
# Schema Registry in `crash-course`. Lab convenience only: production uses one
# service account per application with DeveloperRead/DeveloperWrite on prefixes.
# ---------------------------------------------------------------------------

resource "confluent_service_account" "sa_crash_admin" {
  display_name = "sa-crash-admin"
  description  = "Crash course lab admin (EnvironmentAdmin on crash-course). Delete after the course."
}

resource "confluent_role_binding" "sa_crash_admin_env_admin" {
  principal   = "User:${confluent_service_account.sa_crash_admin.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.crash_course.resource_name
}

resource "confluent_api_key" "sa_crash_admin_kafka_basic" {
  display_name = "sa-crash-admin-kafka-cc-basic"
  description  = "Kafka API key for cc-basic, owned by sa-crash-admin"

  owner {
    id          = confluent_service_account.sa_crash_admin.id
    api_version = confluent_service_account.sa_crash_admin.api_version
    kind        = confluent_service_account.sa_crash_admin.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind

    environment {
      id = confluent_environment.crash_course.id
    }
  }

  depends_on = [
    confluent_role_binding.sa_crash_admin_env_admin
  ]
}

resource "confluent_api_key" "sa_crash_admin_sr" {
  display_name = "sa-crash-admin-schema-registry"
  description  = "Schema Registry API key for crash-course, owned by sa-crash-admin"

  owner {
    id          = confluent_service_account.sa_crash_admin.id
    api_version = confluent_service_account.sa_crash_admin.api_version
    kind        = confluent_service_account.sa_crash_admin.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.essentials.id
    api_version = data.confluent_schema_registry_cluster.essentials.api_version
    kind        = data.confluent_schema_registry_cluster.essentials.kind

    environment {
      id = confluent_environment.crash_course.id
    }
  }

  depends_on = [
    confluent_role_binding.sa_crash_admin_env_admin
  ]
}

# Shared example topics on cc-basic (PII-free). PII topic `customers.customer-journey.v1`
# is created by the CSFLE / governance modules, not here.
locals {
  basic_topics = {
    "orders.order-status.updated.v1"        = 3
    "tracking.item-scan.recorded.v1"        = 3
    "sites.queue-length.measured.v1"        = 1
  }
}

resource "confluent_kafka_topic" "basic" {
  for_each = local.basic_topics

  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  topic_name       = each.key
  partitions_count = each.value
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint

  config = {
    "retention.ms" = "604800000" # 7 days
  }

  credentials {
    key    = confluent_api_key.sa_crash_admin_kafka_basic.id
    secret = confluent_api_key.sa_crash_admin_kafka_basic.secret
  }
}
