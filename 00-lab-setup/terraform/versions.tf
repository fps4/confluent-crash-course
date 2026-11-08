# Confluent crash course — Terraform base for the `cc-basic` and `paid-session` environments.
# Written 2026-11-07. UNTESTED: `terraform validate` / `plan` / `apply` have not been run.
#
# Provider pinned to the version that was current on 2026-11-07 (v2.94.0, released 2026-11-06).
# Pinned exactly so every lab sees the same schema. Bump on purpose, not by accident.

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.94.0"
    }
  }
}

# Credentials: a Confluent Cloud *Cloud resource management* API key (not a cluster key).
# Pass them as environment variables so they never land in a .tf file:
#   export CONFLUENT_CLOUD_API_KEY=...
#   export CONFLUENT_CLOUD_API_SECRET=...
provider "confluent" {}
