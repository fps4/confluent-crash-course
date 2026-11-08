---
title: 06 — Lab — Production streaming systems
subtitle: A "no schema, no topic" golden Terraform module, a GitHub Actions CI sketch, quotas, lag monitoring, and the CFK/ArgoCD equivalent
---

**[V]** = check against current docs, your cluster type or your CLI version.

# Goal

| # | Deliverable | Environment |
|---|---|---|
| 1 | Terraform **golden module** `data-product`: topic + schema + compatibility + tags + owner + role bindings, refuses to create a topic without a schema, refuses PII in a C1/C2 product | `cc-basic` |
| 2 | GitHub Actions **CI sketch**: fmt/validate, policy, SR compatibility check, plan on PR, apply on merge | written; runs against `cc-basic` |
| 3 | A **quota** that visibly throttles a producer | `local` (CC Basic has no client quotas [V]) |
| 4 | **Consumer lag** monitoring: CLI, then the CC Metrics API | `local` + `cc-basic` |
| 5 | The same data product as **CFK CRs + ArgoCD Application** | written only (no OpenShift in the lab) |

Uses `../00-lab-setup/` (`local` dc-a, `cc-basic`; optional `cc-paid` Dedicated from the
paid session for a real CC quota). The three PII-free example topics in `cc-basic` are owned by
`00-lab-setup`, so the golden module creates two **new** data products:
`orders.location-assignment.changed.v1` (C1) and `sites.queue-length.forecast.v1` (C2).
The PII schema for the negative test comes from `../03-stream-governance/schemas/`.

# Prerequisites

- Lab 03 Part B understood (catalog qualified names, tags).
- `confluent` CLI v4.x, Terraform ≥ 1.6, `jq`, `conftest` (optional), Docker.
- A Cloud API key for `sa-crash-admin` in `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET`.
- For step 4 (CC part): a Cloud API key with **MetricsViewer** (a cluster-scoped key gives an auth error).

```bash
a() { docker exec -i dc-a "$@"; }                 # 00-lab-setup helper; internal bootstrap dc-a:29092
cd ../00-lab-setup/terraform
export ENV_ID=$(terraform output -raw environment_id)
export CLUSTER_ID=$(terraform output -raw basic_cluster_id)
export SR_ID=$(terraform output -raw schema_registry_id)
cd -
```

# 1. Golden module — CLI first (what the module automates)

Doing it by hand shows how many steps a team would skip:

```bash
confluent environment use $ENV_ID && confluent kafka cluster use $CLUSTER_ID
confluent iam service-account create sa-orders-producer --description "lab06 producer" -o json | tee /tmp/sa-p.json
confluent iam service-account create sa-analytics-consumer       --description "lab06 consumer" -o json | tee /tmp/sa-c.json
P=$(jq -r .id /tmp/sa-p.json); C=$(jq -r .id /tmp/sa-c.json)

# 1 schema first ("no schema, no topic"), 2 topic, 3 tags/owner (lab 03 B4), 4 role bindings:
confluent iam rbac role-binding create --principal User:$P --role DeveloperWrite \
  --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID \
  --resource Topic:orders.order-status.updated.v1
confluent iam rbac role-binding create --principal User:$C --role DeveloperRead \
  --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID \
  --resource Topic:orders.order-status.updated.v1
confluent iam rbac role-binding create --principal User:$C --role DeveloperRead \
  --environment $ENV_ID --cloud-cluster $CLUSTER_ID --kafka-cluster $CLUSTER_ID \
  --resource Group:analytics- --prefix
confluent iam rbac role-binding create --principal User:$C --role DeveloperRead \
  --environment $ENV_ID --schema-registry-cluster $SR_ID \
  --resource Subject:orders.order-status.updated.v1-value
```

Expected: each command prints the binding. Count the steps: about 12 per data product, each one a
chance to forget the owner, the tag or the subject read grant. Delete these by hand before
step 1b, or let Terraform import them.

## 1b. The module

Layout (create under `06-production-systems/terraform/`):

```
terraform/
  modules/data-product/   main.tf  variables.tf  outputs.tf
  live/cc-basic/          main.tf  versions.tf
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

`live/cc-basic/versions.tf`:

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

`live/cc-basic/main.tf`:

```hcl
data "terraform_remote_state" "base" {
  backend = "local"
  config  = { path = "${path.module}/../../../../00-lab-setup/terraform/terraform.tfstate" }
}
locals {
  base       = data.terraform_remote_state.base.outputs
  env_id     = local.base.environment_id
  cluster_id = local.base.basic_cluster_id
}

# data sources only for the RBAC CRN / resource name (not in the base outputs)
data "confluent_kafka_cluster" "this" {
  id = local.cluster_id
  environment { id = local.env_id }
}
data "confluent_schema_registry_cluster" "this" {
  environment { id = local.env_id }
}

resource "confluent_service_account" "producer" { display_name = "sa-orders-producer" }
resource "confluent_service_account" "consumer" { display_name = "sa-analytics-consumer" }

# Tag definitions are platform-owned; lab 03 creates PII/Public/Confidential/DataProduct.
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
    kafka_rest       = local.base.basic_rest_endpoint
    kafka_rbac_crn   = data.confluent_kafka_cluster.this.rbac_crn
    kafka_api_key    = local.base.sa_kafka_basic_api_key
    kafka_api_secret = local.base.sa_kafka_basic_api_secret
    sr_id            = local.base.schema_registry_id
    sr_rest          = local.base.schema_registry_rest_endpoint
    sr_resource_name = data.confluent_schema_registry_cluster.this.resource_name
    sr_api_key       = local.base.sa_sr_api_key
    sr_api_secret    = local.base.sa_sr_api_secret
  }
}

module "location_assignment" {
  source                = "../../modules/data-product"
  name                  = "orders.location-assignment.changed.v1"
  schema_file           = "${path.module}/schemas/location-assignment-changed.avsc"
  classification        = "C1"
  owner                 = "Orders team"
  owner_email           = "orders-platform@example.org"
  description           = "Location/slot (re)assignment per order. Source: location planning."
  partitions            = 6
  producer_principals   = [confluent_service_account.producer.id]
  consumer_principals   = [confluent_service_account.consumer.id]
  consumer_group_prefix = "analytics-"
  platform              = local.platform
  depends_on            = [confluent_tag.extra]
}

module "queue_forecast" {
  source                = "../../modules/data-product"
  name                  = "sites.queue-length.forecast.v1"
  schema_file           = "${path.module}/schemas/queue-length-forecast.avsc"
  classification        = "C2"
  owner                 = "Sites team"
  owner_email           = "sites-platform@example.org"
  description           = "Forecast wait time per service queue, next 2 hours in 15-min slots. No PII."
  producer_principals   = [confluent_service_account.producer.id]
  consumer_principals   = [confluent_service_account.consumer.id]
  consumer_group_prefix = "analytics-"
  platform              = local.platform
  depends_on            = [confluent_tag.extra]
}
```

`live/cc-basic/schemas/location-assignment-changed.avsc`:

```json
{
  "type": "record", "name": "LocationAssignmentChanged", "namespace": "com.example.orders",
  "doc": "An order got a new location or slot.",
  "fields": [
    {"name": "orderId", "type": "string", "doc": "Order id, e.g. SO-1001234"},
    {"name": "location", "type": ["null", "string"], "default": null, "doc": "New location, e.g. ZONE-D7"},
    {"name": "slot", "type": ["null", "string"], "default": null, "doc": "New slot inside the location"},
    {"name": "previousLocation", "type": ["null", "string"], "default": null, "doc": "Location before the change"},
    {"name": "changedAt", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "When the change was decided"}
  ]
}
```

`live/cc-basic/schemas/queue-length-forecast.avsc`:

```json
{
  "type": "record", "name": "QueueLengthForecast", "namespace": "com.example.sites",
  "doc": "Forecast queue per service queue. No personal data.",
  "fields": [
    {"name": "queueId", "type": "string", "doc": "e.g. SITE-2-Q1"},
    {"name": "slotStart", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "Start of the 15-min slot"},
    {"name": "expectedWaitSeconds", "type": "int", "doc": "Forecast wait"},
    {"name": "modelVersion", "type": "string", "doc": "Forecast model version"}
  ]
}
```

Run and try to break it:

```bash
cd 06-production-systems/terraform/live/cc-basic
terraform init && terraform plan -out tf.plan && terraform apply tf.plan
```

Expected: 2 topics, 2 schemas, 2 subject configs, 4 tag bindings, 2 entity-attribute resources,
10 role bindings, 2 service accounts, 2 tags. Tag definitions `Public`, `Confidential`,
`DataProduct` must exist (lab 03, or add them to `confluent_tag.extra`).

Negative tests (each must fail at `plan`):

| Change | Expected error |
|---|---|
| `name = "OrderStatus"` | naming validation |
| `schema_file = "missing.avsc"` | "No schema, no topic" |
| module with `schema_file = "${path.module}/../../../../03-stream-governance/schemas/customer-journey.avsc"` and `classification = "C1"` | PII precondition |
| `partitions = 64` | partition limit |

# 2. CI sketch — GitHub Actions

`.github/workflows/data-products.yml` (in the platform-config repo):

```yaml
name: data-products
on:
  pull_request:
    paths: ["terraform/**", "schemas/**"]
  push:
    branches: [main]
    paths: ["terraform/**", "schemas/**"]

permissions:
  contents: read
  pull-requests: write

env:
  TF_DIR: terraform/live/cc-basic
  CONFLUENT_CLOUD_API_KEY: ${{ secrets.CC_API_KEY }}
  CONFLUENT_CLOUD_API_SECRET: ${{ secrets.CC_API_SECRET }}

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
            subject="$(yq -r ".products[] | select(.schema == \"$f\") | .name" products.yaml)-value"  # map file -> subject [your convention]
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
    concurrency: tf-cc-basic   # one apply at a time per state
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with: { terraform_version: "1.9.8" }
      - run: terraform -chdir=$TF_DIR init
      - run: terraform -chdir=$TF_DIR apply -auto-approve
```

Action and tool versions are examples; pin them by SHA in a real repo [V].

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

# 3. Quota — local

CC Basic has no client quotas (Enterprise, Freight, Dedicated only [V]). Do it on `dc-a`. The
local cluster has no authentication, so use a **client-id** quota (in production: a `user`
quota on the authenticated principal).

```bash
a kafka-topics --bootstrap-server dc-a:29092 --create \
  --topic tracking.item-scan.recorded.v1 --partitions 3 --replication-factor 1

# baseline: no quota
a kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 \
  --num-records 200000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=replay-job acks=all

# 1 MB/s produce quota for client.id=replay-job (per broker!)
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --add-config 'producer_byte_rate=1048576,consumer_byte_rate=2097152' \
  --entity-type clients --entity-name replay-job
a kafka-configs --bootstrap-server dc-a:29092 --describe \
  --entity-type clients --entity-name replay-job

# again, with quota
a kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 \
  --num-records 20000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=replay-job acks=all \
  --print-metrics | grep -E 'records/sec|produce-throttle-time-avg'
```

Expected: baseline shows tens of MB/s; with the quota ≈ 1 MB/s (≈ 1,000 records/s) and
`produce-throttle-time-avg` > 0. No errors — throttling is latency. Consumer side: run
`kafka-consumer-perf-test --group analytics-replay --consumer.config` with `client.id=replay-job` and
see ≈ 2 MB/s.

Also try `request_percentage=10` (share of broker I/O and network thread time). Remove with
`--delete-config producer_byte_rate,consumer_byte_rate`.

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
a kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 \
  --num-records 50000 --record-size 200 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092
a kafka-console-consumer --bootstrap-server dc-a:29092 \
  --topic tracking.item-scan.recorded.v1 --group analytics-itemscan --from-beginning \
  --max-messages 5000 > /dev/null

a kafka-consumer-groups --bootstrap-server dc-a:29092 \
  --describe --group analytics-itemscan
```

Expected columns `TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG`; total lag ≈ 45k+ (plus
earlier records). Lag is offsets. To get **seconds**, a monitor compares the timestamp of the
record at the committed offset with "now" (what exporters such as `kafka-lag-exporter` or
Datadog do) [V].

## 4b. Confluent Cloud, Metrics API

Produce and consume a little on `cc-basic` (lab 03 B6), with consumer group `analytics-lab06`, then:

```bash
curl -s -u "$METRICS_KEY:$METRICS_SECRET" -H 'Content-Type: application/json' \
  https://api.telemetry.confluent.cloud/v2/metrics/cloud/query --data "{
    \"aggregations\": [{\"metric\": \"io.confluent.kafka.server/consumer_lag_offsets\"}],
    \"filter\": {\"field\": \"resource.kafka.id\", \"op\": \"EQ\", \"value\": \"$CLUSTER_ID\"},
    \"granularity\": \"PT1M\",
    \"group_by\": [\"metric.consumer_group_id\", \"metric.topic\"],
    \"intervals\": [\"now-30m/now\"],
    \"limit\": 100 }" | jq '.data[] | select(."metric.consumer_group_id" | startswith("analytics-"))'
```

Expected: one point per minute per group/topic with the lag value. Metrics arrive with a few
minutes delay. For Prometheus/Datadog scraping use
`https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.kafka.id=$CLUSTER_ID` [V].
Cluster-link lag, when you have a link (module on hybrid): metric
`io.confluent.kafka.server/cluster_link_mirror_topic_offset_lag` (preview) [V].

CLI shortcut: `confluent kafka consumer group lag list analytics-lab06` [V].

# 5. Same data product on-prem — CFK CRs + ArgoCD (written only)

Rendered from the same spec. API group `platform.confluent.io/v1beta1`; field names [V] against
your CFK version (3.3 at time of writing).

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-order-status-updated-v1-avsc
  namespace: confluent
data:
  schema: |
    { "type": "record", "name": "OrderStatusUpdated", "namespace": "com.example.orders", "fields": [ ... ] }
---
apiVersion: platform.confluent.io/v1beta1
kind: Schema
metadata:
  name: orders.order-status.updated.v1-value    # subject name [V]
  namespace: confluent
spec:
  data:
    configRef: orders-order-status-updated-v1-avsc
    format: avro
  compatibilityLevel: BACKWARD_TRANSITIVE           # [V]
  schemaRegistryClusterRef:
    name: schemaregistry
---
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata:
  name: orders.order-status.updated.v1
  namespace: confluent
  labels:
    domain: orders
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
  name: orders-producer-write
  namespace: confluent
spec:
  principal:
    type: user
    name: sa-orders-producer
  role: DeveloperWrite
  resourcePatterns:
    - name: orders.order-status.updated.v1
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
  name: kafka-dataproducts-orders-prod
  namespace: openshift-gitops
spec:
  project: kafka-platform
  source:
    repoURL: https://github.com/example/platform-config
    targetRevision: main
    path: onprem/prod/orders
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
| Role bindings complete | `confluent iam rbac role-binding list --principal User:<sa-analytics-consumer> --environment $ENV_ID --cloud-cluster $CLUSTER_ID` | topic read + group prefix read (subject read listed under SR scope) |
| Catalog | Data Portal → `sites` | owner, description, `Internal` + `DataProduct` tags |
| Quota | perf test with/without | ≈ 1 MB/s with quota, throttle time > 0 |
| Lag | 4a + 4b | lag visible for `analytics-*` groups |
| CI | open a PR that adds a required field without default | `schema-compat` job fails with the SR message |

# Teardown

```bash
cd 06-production-systems/terraform/live/cc-basic && terraform destroy
# CLI-created items from step 1 (if any)
confluent iam service-account delete $(jq -r .id /tmp/sa-p.json) --force
confluent iam service-account delete $(jq -r .id /tmp/sa-c.json) --force
# local
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --delete-config producer_byte_rate,consumer_byte_rate --entity-type clients --entity-name replay-job
a kafka-topics --bootstrap-server dc-a:29092 --delete --topic tracking.item-scan.recorded.v1
```

`terraform destroy` deletes the topics: the Rego delete rule protects only the PR pipeline, not
your laptop.

# Cost note [V]

| Part | Cost |
|---|---|
| Golden module on `cc-basic` | Basic cluster usage; 3 schemas within Essentials' 100 |
| CI | GitHub Actions minutes (free tier usually enough) |
| Quota, lag (local) | none |
| Metrics API | free; rate-limited |
| CC client quota | only on `cc-paid` during the paid session (Dedicated hourly) |
