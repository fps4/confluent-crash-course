---
title: 07 — Lab — Production streaming systems
subtitle: A "no schema, no topic" golden Terraform module, a GitHub Actions CI sketch, quotas, lag monitoring, and the CFK/ArgoCD equivalent
---

> **Video:** episode 7, "From “it works” to “it runs a business”" (coming soon). The video gives the reasoning; this lab lets you see it happen.

**[V]** = check against current docs, your cluster type or your CLI version.

# Goal

| # | Deliverable | Environment |
|---|---|---|
| 1 | Terraform **golden module** `data-product`: topic + schema + compatibility + tags + owner + role bindings, refuses to create a topic without a schema, refuses PII in a C1/C2 product | `cc-std-lab07` |
| 2 | GitHub Actions **CI sketch**: fmt/validate, policy, SR compatibility check, plan on PR, apply on merge | written; `validate` and `schema-compat` run in a scratch GitHub repo, `plan`/`apply` need a remote state backend |
| 3 | A **quota** that visibly throttles a producer | `local` (CC Basic has no client quotas [V]) |
| 4 | **Consumer lag** monitoring: CLI, then the CC Metrics API | `local` + `cc-basic` |
| 5 | The same data product as **CFK CRs + ArgoCD Application** | written only (no OpenShift in the lab) |

Uses `../00-lab-setup/` (`local` dc-a, `cc-basic`; optional `cc-paid` Dedicated from the
paid session for a real CC quota) and a temporary Standard cluster:

`cc-std-lab07` = a **Standard** cluster on Azure `westeurope` in environment `crash-course`,
created at the start of the lab and deleted in teardown, the same pattern as module 06's
`cc-std-lab06`. Basic has no Kafka RBAC (module 06 notes §2.5), and steps 1 and 1b bind roles on
topics and consumer groups. About 1–2 USD for 2 hours [V]. Steps 3 and 4a run on `local`, step 4b
on `cc-basic` (the Metrics API needs no Kafka RBAC).

The golden module creates two **new** data products on `cc-std-lab07`:
`network.cell-config.changed.v1` (C1) and `network.cell-load.forecast.v1` (C2).
The PII schema for the negative test is `04-stream-governance/schemas/subscriber-profile.avsc`,
which is in the repo, so you do not need your files from module 04.

# Prerequisites

- Lab 04 Part B understood (catalog qualified names, tags). Its tag definitions must exist (checked below).
- `confluent` CLI v4.x **logged in as your own user (OrganizationAdmin)**, as in module 06: steps 1
  and 4b create service accounts and a MetricsViewer binding, and `sa-crash-admin` only has
  EnvironmentAdmin (`00-lab-setup`).
- Terraform ≥ 1.6, `jq`, `conftest` (optional; needed for the delete-guard test in step 2), Docker.
- For Terraform: the Cloud API key of `sa-terraform` (OrganizationAdmin, `00-lab-setup` step 3) in
  `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET`. The module creates service accounts,
  which `sa-crash-admin` cannot.
- For step 4b: a Cloud API key with **MetricsViewer** (a cluster-scoped key gives an auth error); step 4b creates it.
- Start clean, because modules 02 and 06 created the same topic on `dc-a`: `( cd "$LAB" && docker compose down -v && docker compose up -d )`

```bash
a() { docker exec -i dc-a "$@"; }                 # 00-lab-setup helper; internal bootstrap dc-a:29092
cd ../00-lab-setup/terraform
export ENV_ID=$(terraform output -raw environment_id)
export BASIC_ID=$(terraform output -raw basic_cluster_id)      # cc-basic: step 4b
export SR_ID=$(terraform output -raw schema_registry_id)
export SR_URL=$(terraform output -raw schema_registry_rest_endpoint)
export SR_AUTH="$(terraform output -raw sa_sr_api_key):$(terraform output -raw sa_sr_api_secret)"
cd -

# cc-std-lab07: Standard, because Basic has no Kafka RBAC. Deleted in teardown.
confluent environment use $ENV_ID
confluent kafka cluster create cc-std-lab07 --cloud azure --region westeurope --type standard -o json | tee /tmp/std07.json
export CLUSTER_ID=$(jq -r .id /tmp/std07.json)                 # steps 1, 1b, verification
export TF_VAR_cluster_id=$CLUSTER_ID                           # read by live/cc-std-lab07

# module 04's tag definitions: expect PII, Public, Confidential, DataProduct
curl -s -u $SR_AUTH $SR_URL/catalog/v1/types/tagdefs | jq -r '.[].name'
```

If you tore down module 04 (its teardown deletes the tags), recreate them with module 04 B2:

```bash
curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/json' \
  --data '[
    {"entityTypes":["cf_entity"],"name":"PII","description":"Personal data (GDPR)"},
    {"entityTypes":["cf_entity"],"name":"Public","description":"Classification C1: may leave the DC, may be shared"},
    {"entityTypes":["cf_entity"],"name":"Confidential","description":"Classification C3: minimised or encrypted before it leaves the DC"},
    {"entityTypes":["cf_entity"],"name":"DataProduct","description":"Published, owned, contract-backed stream"}
  ]' $SR_URL/catalog/v1/types/tagdefs | jq '.[].name'
```

# 1. Golden module — CLI first (what the module automates)

Doing it by hand shows how many steps a team would skip. On `cc-std-lab07`, logged in as your own
user (creating service accounts needs OrganizationAdmin):

```bash
confluent environment use $ENV_ID && confluent kafka cluster use $CLUSTER_ID
confluent iam service-account create sa-lab07-billing-producer   --description "lab07 producer (CLI)" -o json | tee /tmp/sa-p.json
confluent iam service-account create sa-lab07-analytics-consumer --description "lab07 consumer (CLI)" -o json | tee /tmp/sa-c.json
P=$(jq -r .id /tmp/sa-p.json); C=$(jq -r .id /tmp/sa-c.json)

# 1 schema first ("no schema, no topic"): the subject is environment-wide, from lab 04 B3. If you
#   tore lab 04 down, register ../04-stream-governance/schemas/call-record-created.avsc again as in B3.
curl -s -u $SR_AUTH $SR_URL/subjects/billing.call-record.created.v1-value/versions/latest | jq '{id, version}'
# 2 topic (cc-std-lab07 is new, so it is not there yet)
confluent kafka topic create billing.call-record.created.v1 --partitions 3
# 3 tags/owner (lab 04 B4 with this $CLUSTER_ID), 4 role bindings:
confluent iam rbac role-binding create --principal User:$P --role DeveloperWrite \
  --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID \
  --resource Topic:billing.call-record.created.v1
confluent iam rbac role-binding create --principal User:$C --role DeveloperRead \
  --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID \
  --resource Topic:billing.call-record.created.v1
confluent iam rbac role-binding create --principal User:$C --role DeveloperRead \
  --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID \
  --resource Group:analytics- --prefix
confluent iam rbac role-binding create --principal User:$C --role DeveloperRead \
  --environment $ENV_ID --schema-registry-cluster $SR_ID \
  --resource Subject:billing.call-record.created.v1-value
```

Expected: each command prints the binding. Count the steps: about 12 per data product, each one a
chance to forget the owner, the tag or the subject read grant. Step 1b uses its own service
accounts and topics, so nothing here collides with it; teardown deletes these two accounts.

## 1b. The module

Layout (create under `07-production-systems/terraform/`):

```
terraform/
  modules/data-product/   main.tf  variables.tf  outputs.tf
  live/cc-std-lab07/      main.tf  versions.tf
  policy/                 data_product.rego
```

`modules/data-product/variables.tf`:

```hcl
variable "name" {
  type        = string
  description = "<domain>.<dataproduct>[.<event>].v<major>"
  validation {
    condition     = can(regex("^[a-z0-9]+\\.[a-z0-9-]+(\\.[a-z0-9-]+)?\\.v[0-9]+$", var.name))
    error_message = "Topic name must be <domain>.<dataproduct>[.<event>].v<major>, lower case."
  }
}

variable "schema_file" {
  type        = string
  description = "Path to the .avsc. Required: no schema, no topic."
  validation {
    condition     = fileexists(var.schema_file)
    error_message = "No schema, no topic: schema_file must point to an existing .avsc."
  }
}

variable "classification" {
  type = string
  validation {
    condition     = contains(["C1", "C2", "C3", "C4"], var.classification)
    error_message = "classification must be C1, C2, C3 or C4."
  }
}

variable "owner"        { type = string }
variable "owner_email"  { type = string }
variable "description"  { type = string }

variable "partitions" {
  type    = number
  default = 3
  validation {
    condition     = var.partitions >= 1 && var.partitions <= 24
    error_message = "1..24 partitions. More needs a platform review."
  }
}

variable "retention_ms" {
  type    = number
  default = 604800000 # 7 days
  validation {
    condition     = var.retention_ms > 0 && var.retention_ms <= 2592000000
    error_message = "Retention up to 30 days. Longer history belongs in Tableflow/the lake."
  }
}

variable "producer_principals"   { type = list(string) } # service account ids, sa-...
variable "consumer_principals"   { type = list(string) }
variable "consumer_group_prefix" { type = string }

variable "broker_validation" {
  type        = bool
  default     = false
  description = "confluent.value.schema.validation. CC: Dedicated only [V]. CP: always true on published topics."
}

variable "platform" {
  description = "Shared ids and credentials from the platform state"
  type = object({
    environment_id    = string
    kafka_cluster_id  = string
    kafka_rest        = string
    kafka_rbac_crn    = string
    kafka_api_key     = string
    kafka_api_secret  = string
    sr_id             = string
    sr_rest           = string
    sr_resource_name  = string
    sr_api_key        = string
    sr_api_secret     = string
  })
  sensitive = true
}
```

`modules/data-product/main.tf`:

```hcl
terraform {
  required_providers {
    confluent = { source = "confluentinc/confluent" }
  }
}

locals {
  subject     = "${var.name}-value"
  schema_text = file(var.schema_file)
  has_pii     = strcontains(local.schema_text, "\"PII\"")
  class_tag   = { C1 = "Public", C2 = "Internal", C3 = "Confidential", C4 = "Restricted" }[var.classification]
  domain      = split(".", var.name)[0]
}

# 1. Schema first
resource "confluent_subject_config" "this" {
  subject_name        = local.subject
  compatibility_level = "BACKWARD_TRANSITIVE"
  schema_registry_cluster { id = var.platform.sr_id }
  rest_endpoint = var.platform.sr_rest
  credentials {
    key    = var.platform.sr_api_key
    secret = var.platform.sr_api_secret
  }
}

resource "confluent_schema" "this" {
  subject_name = local.subject
  format       = "AVRO"
  schema       = local.schema_text
  metadata {
    properties = {
      owner          = var.owner
      classification = var.classification
      major_version  = regex("v([0-9]+)$", var.name)[0]
    }
  }
  schema_registry_cluster { id = var.platform.sr_id }
  rest_endpoint = var.platform.sr_rest
  credentials {
    key    = var.platform.sr_api_key
    secret = var.platform.sr_api_secret
  }
  depends_on = [confluent_subject_config.this]

  lifecycle {
    precondition {
      condition     = !(local.has_pii && contains(["C1", "C2"], var.classification))
      error_message = "Schema has PII-tagged fields but the product is ${var.classification}. Use C3/C4 or remove the fields."
    }
  }
}

# 2. Topic only after the schema exists
resource "confluent_kafka_topic" "this" {
  topic_name       = var.name
  partitions_count = var.partitions
  config = merge(
    {
      "cleanup.policy" = "delete"
      "retention.ms"   = tostring(var.retention_ms)
    },
    var.broker_validation ? { "confluent.value.schema.validation" = "true" } : {}
  )
  kafka_cluster { id = var.platform.kafka_cluster_id }
  rest_endpoint = var.platform.kafka_rest
  credentials {
    key    = var.platform.kafka_api_key
    secret = var.platform.kafka_api_secret
  }
  depends_on = [confluent_schema.this]
}

# 3. Catalog: classification + DataProduct tags, owner, description
resource "confluent_tag_binding" "tags" {
  for_each    = toset([local.class_tag, "DataProduct"])
  tag_name    = each.value
  entity_name = "${var.platform.sr_id}:${var.platform.kafka_cluster_id}:${var.name}"
  entity_type = "kafka_topic"
  schema_registry_cluster { id = var.platform.sr_id }
  rest_endpoint = var.platform.sr_rest
  credentials {
    key    = var.platform.sr_api_key
    secret = var.platform.sr_api_secret
  }
  depends_on = [confluent_kafka_topic.this]
}

resource "confluent_catalog_entity_attributes" "topic" {
  entity_name = "${var.platform.kafka_cluster_id}:${var.name}" # format per provider docs [V]
  entity_type = "kafka_topic"
  attributes = {
    owner       = var.owner
    ownerEmail  = var.owner_email
    description = var.description
  }
  schema_registry_cluster { id = var.platform.sr_id }
  rest_endpoint = var.platform.sr_rest
  credentials {
    key    = var.platform.sr_api_key
    secret = var.platform.sr_api_secret
  }
  depends_on = [confluent_kafka_topic.this]
}

# 4. Access: producers write topic + read subject; consumers read topic, subject, group prefix
locals {
  topic_crn   = "${var.platform.kafka_rbac_crn}/kafka=${var.platform.kafka_cluster_id}/topic=${var.name}"
  subject_crn = "${var.platform.sr_resource_name}/subject=${local.subject}"
  group_crn   = "${var.platform.kafka_rbac_crn}/kafka=${var.platform.kafka_cluster_id}/group=${var.consumer_group_prefix}*"
  bindings = merge(
    { for p in var.producer_principals : "p-topic-${p}" => { principal = p, role = "DeveloperWrite", crn = local.topic_crn } },
    { for p in var.producer_principals : "p-subj-${p}" => { principal = p, role = "DeveloperRead", crn = local.subject_crn } },
    { for c in var.consumer_principals : "c-topic-${c}" => { principal = c, role = "DeveloperRead", crn = local.topic_crn } },
    { for c in var.consumer_principals : "c-subj-${c}" => { principal = c, role = "DeveloperRead", crn = local.subject_crn } },
    { for c in var.consumer_principals : "c-group-${c}" => { principal = c, role = "DeveloperRead", crn = local.group_crn } },
  )
}

resource "confluent_role_binding" "this" {
  for_each    = local.bindings
  principal   = "User:${each.value.principal}"
  role_name   = each.value.role
  crn_pattern = each.value.crn
}
```

Producers get `DeveloperRead` on the subject, not write: with `auto.register.schemas=false` they
only look up IDs. Registration happens in this pipeline.

`modules/data-product/outputs.tf`:

```hcl
output "topic"      { value = confluent_kafka_topic.this.topic_name }
output "subject"    { value = local.subject }
output "schema_id"  { value = confluent_schema.this.schema_identifier }
output "class_tag"  { value = local.class_tag }
```

`live/cc-std-lab07/versions.tf`:

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    confluent = { source = "confluentinc/confluent", version = "~> 2.94" } # 2.94.0 = 2026-11-06 [V]
  }
  # In real life: backend "azurerm" { ... } with state per domain per environment
}
provider "confluent" {}
```

`live/cc-std-lab07/main.tf`:

```hcl
data "terraform_remote_state" "base" {
  backend = "local"
  config  = { path = "${path.module}/../../../../00-lab-setup/terraform/terraform.tfstate" }
}
variable "cluster_id" {
  type        = string
  description = "cc-std-lab07 (Standard; Basic has no Kafka RBAC), created with the CLI at the start of the lab"
}

locals {
  base       = data.terraform_remote_state.base.outputs
  env_id     = local.base.environment_id
  cluster_id = var.cluster_id
}

# data sources for the REST endpoint, RBAC CRN and SR resource name (not in the base outputs)
data "confluent_kafka_cluster" "this" {
  id = local.cluster_id
  environment { id = local.env_id }
}
data "confluent_schema_registry_cluster" "this" {
  environment { id = local.env_id }
}

# The base has a Kafka key for cc-basic only: give sa-crash-admin (EnvironmentAdmin) one for this cluster
resource "confluent_api_key" "admin" {
  display_name = "sa-crash-admin-cc-std-lab07"
  owner {
    id          = local.base.service_account_id
    api_version = "iam/v2"
    kind        = "ServiceAccount"
  }
  managed_resource {
    id          = data.confluent_kafka_cluster.this.id
    api_version = data.confluent_kafka_cluster.this.api_version
    kind        = data.confluent_kafka_cluster.this.kind
    environment { id = local.env_id }
  }
}

resource "confluent_service_account" "producer" { display_name = "sa-lab07-network-producer-tf" }
resource "confluent_service_account" "consumer" { display_name = "sa-lab07-analytics-consumer-tf" }

# Tag definitions are platform-owned; lab 04 B2 creates PII/Public/Confidential/DataProduct
# (recreate them as in Prerequisites if you tore lab 04 down).
# Add Internal and Restricted here if they do not exist yet.
resource "confluent_tag" "extra" {
  for_each = toset(["Internal", "Restricted"])
  name     = each.value
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.this.id }
  rest_endpoint = data.confluent_schema_registry_cluster.this.rest_endpoint
  credentials {
    key    = local.base.sa_sr_api_key
    secret = local.base.sa_sr_api_secret
  }
}

locals {
  platform = {
    environment_id   = local.env_id
    kafka_cluster_id = local.cluster_id
    kafka_rest       = data.confluent_kafka_cluster.this.rest_endpoint
    kafka_rbac_crn   = data.confluent_kafka_cluster.this.rbac_crn
    kafka_api_key    = confluent_api_key.admin.id
    kafka_api_secret = confluent_api_key.admin.secret
    sr_id            = local.base.schema_registry_id
    sr_rest          = local.base.schema_registry_rest_endpoint
    sr_resource_name = data.confluent_schema_registry_cluster.this.resource_name
    sr_api_key       = local.base.sa_sr_api_key
    sr_api_secret    = local.base.sa_sr_api_secret
  }
}

module "cell_config" {
  source                = "../../modules/data-product"
  name                  = "network.cell-config.changed.v1"
  schema_file           = "${path.module}/schemas/cell-config-changed.avsc"
  classification        = "C1"
  owner                 = "Network team"
  owner_email           = "cells-platform@example.org"
  description           = "Configuration change per cell (frequency band). Source: network planning."
  partitions            = 6
  producer_principals   = [confluent_service_account.producer.id]
  consumer_principals   = [confluent_service_account.consumer.id]
  consumer_group_prefix = "analytics-"
  platform              = local.platform
  depends_on            = [confluent_tag.extra]
}

module "load_forecast" {
  source                = "../../modules/data-product"
  name                  = "network.cell-load.forecast.v1"
  schema_file           = "${path.module}/schemas/cell-load-forecast.avsc"
  classification        = "C2"
  owner                 = "Network team"
  owner_email           = "cells-platform@example.org"
  description           = "Forecast load per cell, next 2 hours in 15-min slots. No PII."
  producer_principals   = [confluent_service_account.producer.id]
  consumer_principals   = [confluent_service_account.consumer.id]
  consumer_group_prefix = "analytics-"
  platform              = local.platform
  depends_on            = [confluent_tag.extra]
}
```

`live/cc-std-lab07/schemas/cell-config-changed.avsc`:

```json
{
  "type": "record", "name": "CellConfigChanged", "namespace": "com.example.network",
  "doc": "A cell got a new configuration (frequency band).",
  "fields": [
    {"name": "cellId", "type": "string", "doc": "Cell id, e.g. CELL-0412"},
    {"name": "band", "type": ["null", "string"], "default": null, "doc": "New frequency band, e.g. n78"},
    {"name": "changeReason", "type": ["null", "string"], "default": null, "doc": "Why the configuration changed, e.g. CAPACITY"},
    {"name": "previousBand", "type": ["null", "string"], "default": null, "doc": "Band before the change"},
    {"name": "changedAt", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "When the change was decided"}
  ]
}
```

`live/cc-std-lab07/schemas/cell-load-forecast.avsc`:

```json
{
  "type": "record", "name": "CellLoadForecast", "namespace": "com.example.network",
  "doc": "Forecast load per cell. No personal data.",
  "fields": [
    {"name": "cellId", "type": "string", "doc": "e.g. CELL-0412"},
    {"name": "slotStart", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "Start of the 15-min slot"},
    {"name": "expectedLoadPercent", "type": "int", "doc": "Forecast load, 0-100"},
    {"name": "modelVersion", "type": "string", "doc": "Forecast model version"}
  ]
}
```

Run and try to break it:

```bash
cd 07-production-systems/terraform/live/cc-std-lab07
terraform init && terraform plan -out tf.plan && terraform apply tf.plan     # cluster_id from TF_VAR_cluster_id
```

Expected: 2 topics, 2 schemas, 2 subject configs, 4 tag bindings, 2 entity-attribute resources,
10 role bindings, 2 service accounts, 2 tags, 1 API key. Tag definitions `Public`, `Confidential`,
`DataProduct` must exist (lab 04 B2, recreated in Prerequisites, or add them to `confluent_tag.extra`).
If a topic fails with an authentication error, the new API key has not propagated yet: run
`apply` again [V].

Negative tests (each must fail at `plan`):

| Change | Expected error |
|---|---|
| `name = "CallRecord"` | naming validation |
| `schema_file = "missing.avsc"` | "No schema, no topic" |
| module with `schema_file = "${path.module}/../../../../04-stream-governance/schemas/subscriber-profile.avsc"` and `classification = "C1"` | PII precondition |
| `partitions = 64` | partition limit |

# 2. CI sketch — GitHub Actions

`.github/workflows/data-products.yml` (in the platform-config repo):

```yaml
name: data-products
on:
  pull_request:
    paths: ["terraform/**", "schemas/**", "products.yaml"]
  push:
    branches: [main]
    paths: ["terraform/**", "schemas/**", "products.yaml"]

permissions:
  contents: read
  pull-requests: write

env:
  TF_DIR: terraform/live/cc-std-lab07
  CONFLUENT_CLOUD_API_KEY: ${{ secrets.CC_API_KEY }}
  CONFLUENT_CLOUD_API_SECRET: ${{ secrets.CC_API_SECRET }}
  TF_VAR_cluster_id: ${{ vars.CLUSTER_ID }}

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with: { terraform_version: "1.9.8" }
      - run: terraform fmt -check -recursive terraform
      - run: terraform -chdir=$TF_DIR init -backend=false && terraform -chdir=$TF_DIR validate

  schema-compat:
    runs-on: ubuntu-latest
    needs: validate
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - name: Lint and check compatibility of changed schemas
        env:
          SR_URL: ${{ vars.SR_URL }}
          SR_AUTH: ${{ secrets.SR_API_KEY }}:${{ secrets.SR_API_SECRET }}
        run: |
          set -euo pipefail
          for f in $(git diff --name-only origin/${{ github.base_ref || 'main' }}...HEAD -- '*.avsc'); do
            # lint: every field has doc; new optional fields have a default
            jq -e '[.fields[] | select(.doc == null)] | length == 0' "$f" >/dev/null \
              || { echo "::error file=$f::every field needs a doc"; exit 1; }
            subject="$(yq -r ".products[] | select(.schema == \"$f\") | .name" products.yaml)-value"  # map file -> subject (products.yaml below)
            code=$(curl -s -o /tmp/r.json -w '%{http_code}' -u "$SR_AUTH" \
              -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
              --data "$(jq -n --rawfile s "$f" '{schema:$s}')" \
              "$SR_URL/compatibility/subjects/$subject/versions?verbose=true")   # all versions [V]
            if [ "$code" = "404" ]; then echo "$subject is new"; continue; fi
            jq -e '.is_compatible == true' /tmp/r.json >/dev/null \
              || { echo "::error file=$f::incompatible: $(jq -c .messages /tmp/r.json)"; exit 1; }
          done

  plan:
    runs-on: ubuntu-latest
    needs: [validate, schema-compat]
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with: { terraform_version: "1.9.8" }
      - run: terraform -chdir=$TF_DIR init
      - run: terraform -chdir=$TF_DIR plan -out tf.plan -no-color | tee plan.txt
      - run: terraform -chdir=$TF_DIR show -json tf.plan > plan.json
      - name: Policy (OPA/Conftest)
        run: |
          curl -sL https://github.com/open-policy-agent/conftest/releases/download/v0.56.0/conftest_0.56.0_Linux_x86_64.tar.gz | tar xz
          ./conftest test plan.json -p terraform/policy
      - uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const body = '```\n' + fs.readFileSync('plan.txt','utf8').slice(-60000) + '\n```';
            github.rest.issues.createComment({...context.repo, issue_number: context.issue.number, body});

  apply:
    runs-on: ubuntu-latest
    needs: [validate, schema-compat]
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    environment: prod          # required reviewers / protection rules
    concurrency: tf-cc-std-lab07   # one apply at a time per state
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with: { terraform_version: "1.9.8" }
      - run: terraform -chdir=$TF_DIR init
      - run: terraform -chdir=$TF_DIR apply -auto-approve
```

Action and tool versions are examples; pin them by SHA in a real repo [V].

`products.yaml` (repo root, next to `terraform/`): one entry per data product, with the same inputs
as the module calls in `live/cc-std-lab07/main.tf`. The `schema-compat` job uses it to map a
changed `.avsc` to its subject; in a real repo the module calls would be generated from it.

```yaml
products:
  - name: network.cell-config.changed.v1
    schema: terraform/live/cc-std-lab07/schemas/cell-config-changed.avsc
    classification: C1
    owner: Network team
    owner_email: cells-platform@example.org
    partitions: 6
    consumer_group_prefix: analytics-
  - name: network.cell-load.forecast.v1
    schema: terraform/live/cc-std-lab07/schemas/cell-load-forecast.avsc
    classification: C2
    owner: Network team
    owner_email: cells-platform@example.org
    consumer_group_prefix: analytics-
```

To see it run, push `terraform/`, `products.yaml` and `.github/` to a scratch GitHub repo with the
variable `SR_URL` and the secrets `SR_API_KEY` / `SR_API_SECRET` (the SR key from `00-lab-setup`).
`validate` and `schema-compat` then run as written. `plan` and `apply` also need `CC_API_KEY` /
`CC_API_SECRET`, the variable `CLUSTER_ID` and the base state in a remote backend: the local
`terraform_remote_state` path does not exist on a runner, so here they stay a sketch.

`terraform/policy/data_product.rego` (Rego v1):

```rego
package main

import rego.v1

topics := [r | some r in input.resource_changes; r.type == "confluent_kafka_topic"; "create" in r.change.actions]
tagged := {split(r.change.after.entity_name, ":")[2] |
  some r in input.resource_changes
  r.type == "confluent_tag_binding"
  r.change.after.tag_name in {"Public", "Internal", "Confidential", "Restricted"}
}

deny contains msg if {
  some t in topics
  not t.change.after.topic_name in tagged
  msg := sprintf("topic %s has no classification tag binding in this plan", [t.change.after.topic_name])
}

deny contains msg if {
  some r in input.resource_changes
  r.type == "confluent_kafka_topic"
  "delete" in r.change.actions
  msg := sprintf("topic %s would be deleted: needs a separate, approved decommission PR", [r.change.before.topic_name])
}
```

The second rule matters: a renamed resource in Terraform is a **delete + create** of the topic,
i.e. data loss. Block it in CI.

Negative test for the delete rule (needs `conftest`): rename a module, which Terraform plans as a
delete of the old topic and a create of the new one. Nothing is applied.

```bash
cd 07-production-systems/terraform/live/cc-std-lab07
sed -i.bak 's/module "load_forecast"/module "load_forecast_v2"/' main.tf
terraform plan -out rename.plan && terraform show -json rename.plan > rename.json
conftest test rename.json -p ../../policy
mv main.tf.bak main.tf && rm rename.plan rename.json
```

Expected: `FAIL - rename.json - main - topic network.cell-load.forecast.v1 would be deleted: needs a
separate, approved decommission PR`, and `conftest` exits non-zero.

# 3. Quota — local

CC Basic has no client quotas (Enterprise, Freight, Dedicated only [V]). Do it on `dc-a`. The
local cluster has no authentication, so use a **client-id** quota (in production: a `user`
quota on the authenticated principal).

```bash
a kafka-topics --bootstrap-server dc-a:29092 --create \
  --topic noc.cell-alarm.raised.v1 --partitions 3 --replication-factor 1

# baseline: no quota
a kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 \
  --num-records 200000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=replay-job acks=all

# 1 MB/s produce quota for client.id=replay-job (per broker!)
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --add-config 'producer_byte_rate=1048576,consumer_byte_rate=2097152' \
  --entity-type clients --entity-name replay-job
a kafka-configs --bootstrap-server dc-a:29092 --describe \
  --entity-type clients --entity-name replay-job

# again, with quota
a kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 \
  --num-records 20000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=replay-job acks=all \
  --print-metrics | grep -E 'records/sec|produce-throttle-time-avg'
```

Expected: baseline shows tens of MB/s; with the quota ≈ 1 MB/s (≈ 1,000 records/s) and
`produce-throttle-time-avg` > 0. No errors — throttling is latency. Consumer side: run
`kafka-consumer-perf-test --group analytics-replay --consumer.config` with `client.id=replay-job` and
see ≈ 2 MB/s.

Also try `request_percentage=10` (share of broker I/O and network thread time). Remove with
`--delete-config producer_byte_rate,consumer_byte_rate`, plus `request_percentage` if you set it;
teardown removes all three.

The same in Confluent Cloud needs an **Enterprise, Freight or Dedicated** cluster. On the paid
day, `cc-paid` (Dedicated, from `00-lab-setup`) works; otherwise read it only:

```bash
confluent kafka quota create --name analytics-replay --description "Analytics replay cap" \
  --ingress 1048576 --egress 10485760 --principals sa-xxxx --cluster lkc-xxxx   # flags [V]
```

```hcl
variable "cluster_supports_quotas" {
  type    = bool
  default = false # true only for Enterprise, Freight, Dedicated [V]
}

resource "confluent_kafka_client_quota" "analytics_replay" {
  count        = var.cluster_supports_quotas ? 1 : 0
  display_name = "analytics-replay"
  description  = "Cap replay egress for analytics consumers"
  throughput {
    ingress_byte_rate = "1048576"
    egress_byte_rate  = "10485760"
  }
  principals = [confluent_service_account.consumer.id]
  kafka_cluster { id = local.cluster_id }
  environment { id = local.env_id }
}
```

# 4. Lag monitoring

## 4a. Local, CLI

```bash
# produce 50k records, then consume only 5k with a group
a kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 \
  --num-records 50000 --record-size 200 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092
a kafka-console-consumer --bootstrap-server dc-a:29092 \
  --topic noc.cell-alarm.raised.v1 --group analytics-alarms --from-beginning \
  --max-messages 5000 > /dev/null

a kafka-consumer-groups --bootstrap-server dc-a:29092 \
  --describe --group analytics-alarms
```

Expected columns `TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG`; total lag ≈ 45k+ (plus
earlier records). Lag is offsets. To get **seconds**, a monitor compares the timestamp of the
record at the committed offset with "now" (what exporters such as `kafka-lag-exporter` or
Datadog do) [V].

## 4b. Confluent Cloud, Metrics API

Back on `cc-basic` (no Kafka RBAC needed here): `confluent kafka cluster use $BASIC_ID`, then
produce and consume a little as in lab 04 B6 (schema file
`../04-stream-governance/schemas/call-record-created.avsc`), with consumer group `analytics-lab07`.

A Cloud API key with MetricsViewer, as your own user (creating a service account needs
OrganizationAdmin):

```bash
confluent iam service-account create sa-lab07-metrics --description "lab07 Metrics API reader" -o json | tee /tmp/sa-m.json
M=$(jq -r .id /tmp/sa-m.json)
confluent iam rbac role-binding create --principal User:$M --role MetricsViewer     # organisation scope [V]
confluent api-key create --resource cloud --service-account $M -o json > /tmp/key-m.json
METRICS_KEY=$(jq -r .api_key /tmp/key-m.json); METRICS_SECRET=$(jq -r .api_secret /tmp/key-m.json)
sleep 60
```

Then:

```bash
curl -s -u "$METRICS_KEY:$METRICS_SECRET" -H 'Content-Type: application/json' \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query --data "{
    \"aggregations\": [{\"metric\": \"io.confluent.kafka.server/consumer_lag_offsets\"}],
    \"filter\": {\"field\": \"resource.kafka.id\", \"op\": \"EQ\", \"value\": \"$BASIC_ID\"},
    \"granularity\": \"PT1M\",
    \"group_by\": [\"metric.consumer_group_id\", \"metric.topic\"],
    \"intervals\": [\"now-30m/now\"],
    \"limit\": 100 }" | jq '.data[] | select(."metric.consumer_group_id" | startswith("analytics-"))'
```

Expected: one point per minute per group/topic with the lag value. Metrics arrive with a few
minutes delay. For Prometheus/Datadog scraping use
`https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=$BASIC_ID` [V].
Cluster-link lag, when you have a link (module on hybrid): metric
`io.confluent.kafka.server/cluster_link_mirror_topic_offset_lag` (preview) [V].

CLI shortcut: `confluent kafka consumer group lag list analytics-lab07 --cluster $BASIC_ID` [V].

# 5. Same data product on-prem — CFK CRs + ArgoCD (written only)

Rendered from the same spec. API group `platform.confluent.io/v1beta1`; field names [V] against
your CFK version (3.3 at time of writing).

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: billing-call-record-created-v1-avsc
  namespace: confluent
data:
  schema: |    # contents of 04-stream-governance/schemas/call-record-created.avsc
    { "type": "record", "name": "CallRecordCreated", "namespace": "com.example.billing", "fields": [ ... ] }
---
apiVersion: platform.confluent.io/v1beta1
kind: Schema
metadata:
  name: billing.call-record.created.v1-value    # subject name [V]
  namespace: confluent
spec:
  data:
    configRef: billing-call-record-created-v1-avsc
    format: avro
  compatibilityLevel: BACKWARD_TRANSITIVE           # [V]
  schemaRegistryClusterRef:
    name: schemaregistry
---
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: billing.call-record.created.v1
  namespace: confluent
  labels:
    domain: billing
    classification: c1
    link-to-cloud: "true"            # read by the renderer of the ClusterLink topic filter
spec:
  replicas: 3
  partitionCount: 6
  configs:
    min.insync.replicas: "2"
    retention.ms: "604800000"
    confluent.value.schema.validation: "true"
  kafkaRestClassRef:
    name: ops-mc-rest
---
apiVersion: platform.confluent.io/v1beta1
kind: ConfluentRolebinding
metadata:
  name: billing-producer-write
  namespace: confluent
spec:
  principal:
    type: user
    name: sa-billing-producer
  role: DeveloperWrite
  resourcePatterns:
    - name: billing.call-record.created.v1
      patternType: LITERAL
      resourceType: Topic
  kafkaRestClassRef:
    name: ops-mc-rest
```

ArgoCD Application per domain:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: kafka-dataproducts-billing-prod
  namespace: openshift-gitops
spec:
  project: kafka-platform
  source:
    repoURL: https://github.com/example/platform-config
    targetRevision: main
    path: onprem/prod/billing
  destination:
    server: https://kubernetes.default.svc
    namespace: confluent
  syncPolicy:
    automated:
      prune: false      # never let a Git deletion delete a topic automatically
      selfHeal: true    # revert manual changes
```

Policy at admission (Kyverno, sketch): reject a `KafkaTopic` whose name does not match the
pattern, whose `partitionCount` > 24, or that has no `classification` label. Same rules as the
Rego in CI.

Quotas on-prem: no CFK CR for client quotas was found [V]. Keep quota values in the same Git
folder and apply them with a small Job (`kafka-configs --alter --entity-type users ...`) as an
ArgoCD PostSync hook.

# Verification

| Check | How | Expected |
|---|---|---|
| Module refuses bad input | negative tests in step 1b | 4 plan-time errors |
| Delete guard | renamed module in step 2, `conftest test` | `deny`: topic would be deleted |
| Role bindings complete | `confluent iam rbac role-binding list --principal User:<id of sa-lab07-analytics-consumer-tf> --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID` | topic read + group prefix read (subject read listed under SR scope) |
| Catalog | Data Portal → `network` | owner, description, `Internal` + `DataProduct` tags |
| Quota | perf test with/without | ≈ 1 MB/s with quota, throttle time > 0 |
| Lag | 4a + 4b | lag visible for `analytics-*` groups |
| CI | in the scratch repo (step 2), before teardown: open a PR that adds a required field without default | `schema-compat` job fails with the SR message |

# Teardown

```bash
cd 07-production-systems/terraform/live/cc-std-lab07 && terraform destroy     # needs the cluster, so first
# CLI-created items from steps 1 and 4b, as your own user
confluent iam service-account delete $(jq -r .id /tmp/sa-p.json) --force
confluent iam service-account delete $(jq -r .id /tmp/sa-c.json) --force
confluent api-key delete $(jq -r .api_key /tmp/key-m.json) --force
confluent iam service-account delete $(jq -r .id /tmp/sa-m.json) --force
# the Standard cluster (with the step 1 topic)
confluent kafka cluster delete $CLUSTER_ID --force
# local
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --delete-config producer_byte_rate,consumer_byte_rate --entity-type clients --entity-name replay-job
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --delete-config request_percentage --entity-type clients --entity-name replay-job   # if you tried it in step 3
a kafka-topics --bootstrap-server dc-a:29092 --delete --topic noc.cell-alarm.raised.v1
```

`terraform destroy` deletes the topics: the Rego delete rule protects only the PR pipeline, not
your laptop.

# Cost note [V]

| Part | Cost |
|---|---|
| Standard cluster `cc-std-lab07`, ~2 h | 0.75 USD/eCKU-h list → about 1–2 USD |
| Golden module on `cc-std-lab07` | cluster usage; 3 schemas within Essentials' 100 |
| CI | GitHub Actions minutes (free tier usually enough) |
| Quota, lag (local) | none |
| Metrics API | free; rate-limited |
| CC client quota | only on `cc-paid` during the paid session (Dedicated hourly) |
