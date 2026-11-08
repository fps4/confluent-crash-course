# paid-session: billable clusters, created and destroyed on the same day.
# Both are off by default (count = 0). See variables.tf and README.md.

# ---------------------------------------------------------------------------
# Dedicated, PUBLIC networking: `cc-paid`
# Why Dedicated and not Enterprise for the laptop labs:
#   - Enterprise clusters are always privately networked. A laptop cannot reach them
#     without an Azure private endpoint + VPN into that VNet.
#   - Broker-side schema ID validation in Confluent Cloud is Dedicated-only.
#   - The hybrid CP -> CC tutorial is written for a Dedicated cluster with public access.
# Dedicated provisioning can take a long time (tens of minutes, sometimes more). [V]
# ---------------------------------------------------------------------------

resource "confluent_kafka_cluster" "dedicated" {
  count = var.enable_paid_dedicated ? 1 : 0

  display_name = "cc-paid"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region

  dedicated {
    cku = var.dedicated_cku
  }

  environment {
    id = confluent_environment.crash_course.id
  }
}

resource "confluent_api_key" "sa_crash_admin_kafka_dedicated" {
  count = var.enable_paid_dedicated ? 1 : 0

  display_name = "sa-crash-admin-kafka-cc-paid"
  description  = "Kafka API key for cc-paid, owned by sa-crash-admin"

  owner {
    id          = confluent_service_account.sa_crash_admin.id
    api_version = confluent_service_account.sa_crash_admin.api_version
    kind        = confluent_service_account.sa_crash_admin.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.dedicated[0].id
    api_version = confluent_kafka_cluster.dedicated[0].api_version
    kind        = confluent_kafka_cluster.dedicated[0].kind

    environment {
      id = confluent_environment.crash_course.id
    }
  }

  depends_on = [
    confluent_role_binding.sa_crash_admin_env_admin
  ]
}

# ---------------------------------------------------------------------------
# Enterprise + Azure PrivateLink Attachment: `cc-ent`
# NOTE (review 2026-11-07): since 2026-05-04 Enterprise Private Link uses the ingress
# PrivateLink Gateway + Access Points (`confluent_gateway`, `confluent_access_point`); the
# PrivateLink Attachment below is the older model [V]. Module 02's lab builds its own gateway
# and Enterprise cluster, so keep `enable_paid_enterprise_private = false` and use lab 02.
# This creates only the Confluent side. The Azure private endpoint, the private DNS zone
# and the PrivateLink Attachment *connection* belong to module 02 (cloud networking),
# which needs the azurerm provider and an Azure subscription.
# Enterprise can be a Cluster Linking destination and source (private networking rules apply).
# ---------------------------------------------------------------------------

resource "confluent_private_link_attachment" "azure" {
  count = var.enable_paid_enterprise_private ? 1 : 0

  cloud        = var.cloud
  region       = var.region
  display_name = "crash-course-platt"

  environment {
    id = confluent_environment.crash_course.id
  }
}

resource "confluent_kafka_cluster" "enterprise" {
  count = var.enable_paid_enterprise_private ? 1 : 0

  display_name = "cc-ent"
  availability = "LOW" # provider accepts SINGLE_ZONE, MULTI_ZONE, LOW, HIGH; examples use LOW/HIGH for Enterprise. HIGH needs >= 2 eCKU. [V]
  cloud        = var.cloud
  region       = var.region
  enterprise {}

  environment {
    id = confluent_environment.crash_course.id
  }

  depends_on = [
    confluent_private_link_attachment.azure
  ]
}
