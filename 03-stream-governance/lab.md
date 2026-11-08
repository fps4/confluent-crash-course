---
title: 03 — Lab — Stream Governance
subtitle: Schemas with tags and metadata, compatibility gates, broker-side validation, catalog search, lineage, Data Portal — CLI and curl first, then Terraform
---

**[V]** = check against current docs or your package before you rely on it.

# Goal

End state, built twice (CLI/curl, then Terraform):

| Item | Value |
|---|---|
| Topics | `orders.order-status.updated.v1` (C1, no PII), `customers.customer-journey.v1` (C3, PII) |
| Subjects | `<topic>-value`, Avro, `BACKWARD_TRANSITIVE` |
| Schema metadata | `owner`, `classification`, `major_version` properties |
| Tags | `PII` on fields (inline), `Public` / `Confidential` on topics, `DataProduct` on both topics |
| Catalog | owner, ownerEmail, description on topics; search by tag via REST |
| Lineage | one CLI producer + one CLI consumer visible in the 10-minute window |
| Optional (paid) | business metadata `DataProduct{domain, sla, support}`, a CEL rule, Data Portal access request |

# Environments

| Part | Environment | Why |
|---|---|---|
| A — Schema Registry, compatibility, broker-side validation | `local` (`dc-a`) | Free; Confluent Server has broker-side validation |
| B — Catalog, tags, lineage, Data Portal browse | `cc-basic` | Stream Catalog exists only in Confluent Cloud |
| C — Business metadata, rules (optional) | throw-away CC env `crash-course-adv` with **Advanced** | Advanced cannot be downgraded; do not upgrade `crash-course` |
| D — Data Portal access request (optional) | needs a **Standard or higher** cluster | Access requests are not available on Basic [V] |

See `../00-lab-setup/README.md` for how `local` and `cc-basic` are built. This lab uses its
helper functions (`a`, `sra`), ports and Terraform outputs.

# Prerequisites

- `confluent` CLI v4.82+ (pinned in `00-lab-setup`), logged in (`confluent login --save`).
- `curl`, `jq`, Docker, Terraform ≥ 1.6, and the Cloud API key from `00-lab-setup` step 3 in
  `CONFLUENT_CLOUD_API_KEY` / `CONFLUENT_CLOUD_API_SECRET`.
- `00-lab-setup` applied: `local` running, `cc-basic` created, outputs available.

```bash
# --- local (from 00-lab-setup) ---
a()   { docker exec -i dc-a "$@"; }      # Kafka tools inside dc-a (internal bootstrap dc-a:29092)
sra() { docker exec -i sr-a "$@"; }      # kafka-avro-console-* inside sr-a
export A_SR=http://localhost:8081        # sr-a from the host

# --- cc-basic (00-lab-setup terraform outputs) ---
cd ../00-lab-setup/terraform
export ENV_ID=$(terraform output -raw environment_id)
export CLUSTER_ID=$(terraform output -raw basic_cluster_id)
export SR_ID=$(terraform output -raw schema_registry_id)
export SR_URL=$(terraform output -raw schema_registry_rest_endpoint)
export SR_AUTH="$(terraform output -raw sa_sr_api_key):$(terraform output -raw sa_sr_api_secret)"
cd -
```

The base already created `orders.order-status.updated.v1` in `cc-basic` (PII-free topics are
owned by `00-lab-setup`). This lab creates `customers.customer-journey.v1` and adds schemas,
tags and catalog attributes to both.

Create the two schema files once (used by every part).

`schemas/order-status-updated.avsc`:

```json
{
  "type": "record",
  "name": "OrderStatusUpdated",
  "namespace": "com.example.orders",
  "doc": "A change of the operational status of one order.",
  "fields": [
    {"name": "orderId", "type": "string", "doc": "Order id, e.g. SO-1001234"},
    {"name": "status", "type": {"type": "enum", "name": "OrderStatus",
      "symbols": ["CREATED", "CONFIRMED", "DELAYED", "LOCATION_CHANGE", "READY", "DISPATCHED", "CANCELLED", "UNKNOWN"],
      "default": "UNKNOWN"}, "doc": "Operational status"},
    {"name": "location", "type": ["null", "string"], "default": null, "doc": "Location, if assigned, e.g. ZONE-D7"},
    {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "When the status changed"}
  ]
}
```

`schemas/customer-journey.avsc` (synthetic PII, for the tag demo):

```json
{
  "type": "record",
  "name": "CustomerJourney",
  "namespace": "com.example.customers",
  "doc": "One customer's progress through a site. Contains personal data.",
  "fields": [
    {"name": "journeyId", "type": "string", "doc": "Pseudonymous journey id"},
    {"name": "email", "type": ["null", "string"], "default": null, "doc": "Contact email", "confluent:tags": ["PII"]},
    {"name": "customerCardHash", "type": "string", "doc": "Hash of the customer card number", "confluent:tags": ["PII"]},
    {"name": "location", "type": "string", "doc": "e.g. ZONE-D7, COUNTER-2"},
    {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-millis"}, "doc": "Scan time"}
  ]
}
```

# Part A — local: Schema Registry, compatibility, broker-side validation

## A1. Create topics and register schemas with metadata

```bash
for t in orders.order-status.updated.v1 customers.customer-journey.v1; do
  a kafka-topics --bootstrap-server dc-a:29092 --create --topic $t --partitions 3 --replication-factor 1
done

# set compatibility first, then register
for s in orders.order-status.updated.v1-value customers.customer-journey.v1-value; do
  curl -s -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data '{"compatibility":"BACKWARD_TRANSITIVE"}' $A_SR/config/$s | jq .
done

jq -n --rawfile s schemas/order-status-updated.avsc \
  '{schemaType:"AVRO", schema:$s,
    metadata:{properties:{owner:"orders-team", classification:"C1", major_version:"1"}}}' \
| curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data @- $A_SR/subjects/orders.order-status.updated.v1-value/versions | jq .

jq -n --rawfile s schemas/customer-journey.avsc \
  '{schemaType:"AVRO", schema:$s,
    metadata:{properties:{owner:"customers-team", classification:"C3", major_version:"1"}}}' \
| curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data @- $A_SR/subjects/customers.customer-journey.v1-value/versions | jq .
```

Expected: `{"id": 1}` and `{"id": 2}` (IDs may differ). Read it back and see the metadata:

```bash
curl -s $A_SR/subjects/customers.customer-journey.v1-value/versions/latest | jq '{id, version, metadata}'
```

Expected: `metadata.properties` shows owner/classification; `metadata.tags` may show
`{"CustomerJourney.email":["PII"], ...}` derived from the inline tags [V: tag extraction depends
on SR version].

## A2. Compatibility gate (what CI will call)

A compatible change — add an optional field with a default:

```bash
jq '.fields += [{"name":"locationChangeReason","type":["null","string"],"default":null,"doc":"Why the location changed"}]' \
  schemas/order-status-updated.avsc > /tmp/os-v2.avsc

jq -n --rawfile s /tmp/os-v2.avsc '{schema:$s}' \
| curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @- \
  "$A_SR/compatibility/subjects/orders.order-status.updated.v1-value/versions/latest?verbose=true" | jq .
```

Expected: `{"is_compatible": true, "messages": []}`.

An incompatible change — add a required field without a default:

```bash
jq '.fields += [{"name":"region","type":"string"}]' schemas/order-status-updated.avsc > /tmp/os-bad.avsc
jq -n --rawfile s /tmp/os-bad.avsc '{schema:$s}' \
| curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @- \
  "$A_SR/compatibility/subjects/orders.order-status.updated.v1-value/versions/latest?verbose=true" | jq .
```

Expected: `"is_compatible": false` and a message like `READER_FIELD_MISSING_DEFAULT_VALUE` for
`region`. Trying to register it returns HTTP **409**.

Note: `versions/latest` checks only against the latest version. For a `*_TRANSITIVE` subject the
registry checks all versions on **registration** anyway; in CI, call `.../versions` (no `latest`)
to test against all versions [V].

## A3. Broker-side schema ID validation (Confluent Server)

The broker needs to know the SR URL; `00-lab-setup` already sets
`KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL: http://sr-a:8081` on `dc-a`. Enable validation on the
published topic and test:

```bash
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --entity-type topics --entity-name orders.order-status.updated.v1 \
  --add-config confluent.value.schema.validation=true

# 1) plain text, no schema ID -> must be rejected
echo 'not-avro' | a kafka-console-producer \
  --bootstrap-server dc-a:29092 --topic orders.order-status.updated.v1

# 2) proper Avro with the registered schema -> accepted
echo '{"orderId":"SO-1001234","status":"READY","location":{"string":"ZONE-D7"},"eventTime":1762500000000}' \
| sra kafka-avro-console-producer \
  --bootstrap-server dc-a:29092 --topic orders.order-status.updated.v1 \
  --reader-property schema.registry.url=http://localhost:8081 \
  --reader-property value.schema.id=1 --reader-property auto.register.schemas=false
```

Expected: (1) the producer logs an error containing `InvalidRecordException` / "This record has
failed the validation on broker" [V wording]; (2) succeeds.

What this proves: the broker rejects a record **without** a valid ID. It does not prove the
payload matches the schema. Try (3): produce with `value.schema.id=2` (the customer schema) to
the order topic — with `TopicNameStrategy` the broker checks the ID is registered **under this
topic's subject**, so it should be rejected [V].

# Part B — cc-basic: catalog, tags, owner, lineage

## B1. Select context

```bash
confluent environment use $ENV_ID
confluent kafka cluster use $CLUSTER_ID
# Kafka key of sa-crash-admin, stored as in 00-lab-setup step 5
curl -s -u $SR_AUTH $SR_URL/subjects | jq .
```

Expected: `[]` or the subjects other modules created. The SR key from the base works for the
catalog API too (same endpoint).

## B2. Tag definitions first

```bash
curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/json' \
  --data '[
    {"entityTypes":["cf_entity"],"name":"PII","description":"Personal data (GDPR)"},
    {"entityTypes":["cf_entity"],"name":"Public","description":"Classification C1: may leave the DC, may be shared"},
    {"entityTypes":["cf_entity"],"name":"Confidential","description":"Classification C3: minimised or encrypted before it leaves the DC"},
    {"entityTypes":["cf_entity"],"name":"DataProduct","description":"Published, owned, contract-backed stream"}
  ]' $SR_URL/catalog/v1/types/tagdefs | jq '.[].name'
```

Expected: the four names. `cf_entity` = applicable to any entity type.

## B3. Topics and schemas

```bash
# orders.order-status.updated.v1 already exists (00-lab-setup)
confluent kafka topic create customers.customer-journey.v1 --partitions 3

for s in orders.order-status.updated.v1-value customers.customer-journey.v1-value; do
  curl -s -u $SR_AUTH -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data '{"compatibility":"BACKWARD_TRANSITIVE"}' $SR_URL/config/$s | jq -c .
done

jq -n --rawfile s schemas/order-status-updated.avsc \
  '{schemaType:"AVRO", schema:$s, metadata:{properties:{owner:"orders-team",classification:"C1",major_version:"1"}}}' \
| curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @- $SR_URL/subjects/orders.order-status.updated.v1-value/versions | jq .

jq -n --rawfile s schemas/customer-journey.avsc \
  '{schemaType:"AVRO", schema:$s, metadata:{properties:{owner:"customers-team",classification:"C3",major_version:"1"}}}' \
| curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @- $SR_URL/subjects/customers.customer-journey.v1-value/versions | jq .
```

Expected: IDs in the 100000+ range (CC IDs are not 1, 2, …). Note them as `OS_ID`, `CJ_ID`.

## B4. Tag topics, set owner and description

```bash
Q_OS="$SR_ID:$CLUSTER_ID:orders.order-status.updated.v1"
Q_CJ="$SR_ID:$CLUSTER_ID:customers.customer-journey.v1"

curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/json' --data "[
  {\"entityType\":\"kafka_topic\",\"entityName\":\"$Q_OS\",\"typeName\":\"Public\"},
  {\"entityType\":\"kafka_topic\",\"entityName\":\"$Q_OS\",\"typeName\":\"DataProduct\"},
  {\"entityType\":\"kafka_topic\",\"entityName\":\"$Q_CJ\",\"typeName\":\"Confidential\"},
  {\"entityType\":\"kafka_topic\",\"entityName\":\"$Q_CJ\",\"typeName\":\"DataProduct\"}
]" $SR_URL/catalog/v1/entity/tags | jq -c '.[] | {entityName, typeName}'

# owner / ownerEmail / description on a topic [V: request shape; Terraform below is the reliable path]
curl -s -u $SR_AUTH -X PUT -H 'Content-Type: application/json' --data "{
  \"entity\": {\"typeName\": \"kafka_topic\", \"attributes\": {
    \"qualifiedName\": \"$Q_OS\", \"owner\": \"Orders team\",
    \"ownerEmail\": \"orders-platform@example.org\",
    \"description\": \"Operational status changes per order. Source: order management system. SLA tier T1.\"}}}" \
  $SR_URL/catalog/v1/entity | jq -c .
```

If tagging returns "entity not found", wait a minute: new topics take time to appear in the
catalog. Retry.

## B5. Search the catalog

```bash
# all fields tagged PII (comes from inline confluent:tags)
curl -s -u $SR_AUTH "$SR_URL/catalog/v1/search/basic?type=sr_field&tag=PII" \
  | jq -r '.entities[].attributes.qualifiedName'

# all topics tagged DataProduct
curl -s -u $SR_AUTH "$SR_URL/catalog/v1/search/basic?type=kafka_topic&tag=DataProduct" \
  | jq -r '.entities[] | [.attributes.qualifiedName, (.classificationNames|join(","))] | @tsv'

# prefix search by name (REST is prefix-only)
curl -s -u $SR_AUTH "$SR_URL/catalog/v1/search/basic?types=kafka_topic&query=customers" \
  | jq -r '.entities[].attributes.qualifiedName'
```

Expected: two PII fields (`...CustomerJourney.email`, `...customerCardHash`), two DataProduct
topics with their classification tag, one `customers` topic.

Exercise: write the query a CI gate would run — "topics tagged `Confidential` whose value schema
has PII fields" — and decide which of them may go into a cluster-link filter.

## B6. Lineage

Run a producer and a consumer within the same 10 minutes:

```bash
confluent kafka topic produce orders.order-status.updated.v1 \
  --value-format avro --schema schemas/order-status-updated.avsc \
  --schema-registry-api-key ${SR_AUTH%%:*} --schema-registry-api-secret ${SR_AUTH#*:}
# paste a few JSON lines like the one in A3, then Ctrl-D

confluent kafka topic consume orders.order-status.updated.v1 --from-beginning \
  --group lab03-lineage --value-format avro \
  --schema-registry-api-key ${SR_AUTH%%:*} --schema-registry-api-secret ${SR_AUTH#*:}
```

Then: Console → environment `crash-course` → cluster `cc-basic` → **Stream Lineage**. Expected:
a producer node → topic → consumer group `lab03-lineage`. Wait 10+ minutes and refresh: the
nodes disappear (Essentials window). That is the lesson: lineage shows activity, not
ownership. [V: CLI flag names for SR credentials differ by CLI version; `--schema` may register
the schema if it is new — set `--schema-id` instead if your CLI supports it.]

## B7. Data Portal (browse only on Basic)

Console → **Data Portal**. Search `orders`, open the topic. Expected: description, owner,
tags, schema, lineage. The **Request access** button is missing or disabled on a Basic cluster
[V]. That is Part D.

# Part C — optional, paid: Advanced governance in a throw-away environment

Do **not** upgrade `crash-course`: Advanced cannot be downgraded.

```bash
confluent environment create crash-course-adv --governance-package advanced -o json | tee /tmp/adv.json   # flag [V]
ADV_ENV=$(jq -r .id /tmp/adv.json)
confluent environment use $ADV_ENV
confluent kafka cluster create cc-adv --cloud azure --region westeurope --type basic -o json | tee /tmp/adv-lkc.json
CLUSTER_ID=$(jq -r .id /tmp/adv-lkc.json); confluent kafka cluster use $CLUSTER_ID
SA_ID=$(confluent iam service-account list -o json | jq -r '.[] | select(.name=="sa-crash-admin") | .id')
confluent iam rbac role-binding create --principal User:$SA_ID --role EnvironmentAdmin --environment $ADV_ENV
SR_ID=$(confluent schema-registry cluster describe -o json | jq -r '.cluster_id // .id')        # field [V]
SR_URL=$(confluent schema-registry cluster describe -o json | jq -r '.endpoint_url // .url')    # field [V]
confluent api-key create --resource $SR_ID --service-account $SA_ID -o json > /tmp/adv-sr.json
export SR_AUTH="$(jq -r .api_key /tmp/adv-sr.json):$(jq -r .api_secret /tmp/adv-sr.json)"
confluent api-key create --resource $CLUSTER_ID --service-account $SA_ID -o json > /tmp/adv-kafka.json
confluent api-key use $(jq -r .api_key /tmp/adv-kafka.json) --resource $CLUSTER_ID
confluent kafka topic create orders.order-status.updated.v1 --partitions 3
# then repeat B2-B4 here (tags, schemas, Q_OS with the new SR_ID and CLUSTER_ID)
```

These variables now point at `crash-course-adv`. Re-run the export block from Prerequisites
before you go back to `cc-basic`.

Business metadata definition and binding:

```bash
curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/json' --data '[{
  "name": "DataProduct",
  "description": "Data product contract fields",
  "attributeDefs": [
    {"name":"domain","isOptional":"false","cardinality":"SINGLE","typeName":"string","options":{"maxStrLength":30,"applicableEntityTypes":"[\"kafka_topic\",\"sr_schema\"]"}},
    {"name":"sla","isOptional":"false","cardinality":"SINGLE","typeName":"string","options":{"maxStrLength":10,"applicableEntityTypes":"[\"kafka_topic\",\"sr_schema\"]"}},
    {"name":"support","isOptional":"true","cardinality":"SINGLE","typeName":"string","options":{"maxStrLength":80,"applicableEntityTypes":"[\"kafka_topic\",\"sr_schema\"]"}}
  ]}]' $SR_URL/catalog/v1/types/businessmetadatadefs | jq '.[].name'

curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/json' --data "[{
  \"entityType\":\"kafka_topic\",\"entityName\":\"$Q_OS\",\"typeName\":\"DataProduct\",
  \"attributes\":{\"domain\":\"orders\",\"sla\":\"T1\",\"support\":\"#orders-platform\"}}]" \
  $SR_URL/catalog/v1/entity/businessmetadata | jq -c .

curl -s -u $SR_AUTH "$SR_URL/catalog/v1/search/attribute?type=kafka_topic&attrName=DataProduct.sla&attrValuePrefix=T1" \
  | jq -r '.entities[].attributes.qualifiedName'
```

Note: the tag `DataProduct` and the business metadata `DataProduct` share a name here for
readability. In a real taxonomy pick distinct names (e.g. tag `DataProduct`, metadata
`ProductContract`) [V: name collision rules not checked].

A data-contract **CEL rule** (runs in the serializer, so test it with a client that has the rule
executor on its classpath, e.g. the Java Avro serializer ≥ 7.4) [V]:

```bash
jq -n --rawfile s schemas/order-status-updated.avsc '{
  schemaType:"AVRO", schema:$s,
  metadata:{properties:{owner:"orders-team",classification:"C1",major_version:"1"}},
  ruleSet:{domainRules:[{
    name:"orderIdFormat", kind:"CONDITION", type:"CEL", mode:"WRITE",
    expr:"message.orderId.matches(\"^SO-[0-9]{7}$\")",
    onFailure:"DLQ",
    params:{"dlq.topic":"orders.order-status.updated.v1.dlq"}}]}}' \
| curl -s -u $SR_AUTH -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @- $SR_URL/subjects/orders.order-status.updated.v1-value/versions | jq .
```

Expected: a new version with `ruleSet`. On Essentials the same call fails (rules need Advanced).

# Part D — optional, paid: Data Portal access request

Needs a Standard (or higher) cluster and a second user in the organisation with only
`DataDiscovery` on the environment [V].

1. As admin: create a Standard cluster `cc-std-tmp` in `crash-course-adv`, create the topic, set
   `ownerEmail` (B4).
2. As the second user: Data Portal → topic → **Request access** → Read only → reason.
3. As admin: email or Accounts & access → Access requests → Approve.
4. Check: `confluent iam rbac role-binding list --principal User:<id> --environment $ADV_ENV
   --cloud-cluster <lkc> -o json` shows `DeveloperRead` on the topic.
5. Note what is **not** granted: consumer-group read and SR subject read. Write down how the
   platform would add these (template in the request, or a follow-up PR).

# Same end state in Terraform

Save under `03-stream-governance/terraform/`. Pin the provider; `2.94.0` was released
2026-11-06 [V].

`versions.tf`:

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.94"
    }
  }
}

provider "confluent" {}   # CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET from env
```

`main.tf`:

```hcl
data "terraform_remote_state" "base" {
  backend = "local"
  config  = { path = "${path.module}/../../00-lab-setup/terraform/terraform.tfstate" }
}

locals {
  base       = data.terraform_remote_state.base.outputs
  env_id     = local.base.environment_id
  cluster_id = local.base.basic_cluster_id
  kafka_rest = local.base.basic_rest_endpoint
  kafka_key  = { id = local.base.sa_kafka_basic_api_key, secret = local.base.sa_kafka_basic_api_secret }
  sr_key     = { id = local.base.sa_sr_api_key, secret = local.base.sa_sr_api_secret }
}

locals {
  products = {
    "orders.order-status.updated.v1" = {
      schema_file    = "${path.module}/../schemas/order-status-updated.avsc"
      owner          = "Orders team"
      owner_email    = "orders-platform@example.org"
      description    = "Operational status changes per order. Source: order management system. SLA tier T1."
      classification = "C1"
      class_tag      = "Public"
      create_topic   = false   # owned by 00-lab-setup
    }
    "customers.customer-journey.v1" = {
      schema_file    = "${path.module}/../schemas/customer-journey.avsc"
      owner          = "Customers team"
      owner_email    = "customers-platform@example.org"
      description    = "Customer progress through sites. Contains PII. Never in the link filter unencrypted."
      classification = "C3"
      class_tag      = "Confidential"
      create_topic   = true
    }
  }
  sr = {
    id       = local.base.schema_registry_id
    endpoint = local.base.schema_registry_rest_endpoint
  }
}

resource "confluent_tag" "defs" {
  for_each = {
    PII          = "Personal data (GDPR)"
    Public       = "Classification C1"
    Confidential = "Classification C3"
    DataProduct  = "Published, owned, contract-backed stream"
  }
  name        = each.key
  description = each.value

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
}

resource "confluent_kafka_topic" "this" {
  for_each         = { for k, v in local.products : k => v if v.create_topic }
  topic_name       = each.key
  partitions_count = 3
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "604800000"
  }
  kafka_cluster { id = local.cluster_id }
  rest_endpoint = local.kafka_rest
  credentials {
    key    = local.kafka_key.id
    secret = local.kafka_key.secret
  }
}

resource "confluent_subject_config" "this" {
  for_each            = local.products
  subject_name        = "${each.key}-value"
  compatibility_level = "BACKWARD_TRANSITIVE"

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
}

resource "confluent_schema" "this" {
  for_each     = local.products
  subject_name = "${each.key}-value"
  format       = "AVRO"
  schema       = file(each.value.schema_file)   # inline confluent:tags carry the PII field tags

  metadata {
    properties = {
      owner          = each.value.owner
      classification = each.value.classification
      major_version  = "1"
    }
  }

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
  depends_on = [confluent_subject_config.this, confluent_tag.defs]
}

resource "confluent_tag_binding" "class" {
  for_each    = local.products
  tag_name    = each.value.class_tag
  entity_name = "${local.sr.id}:${local.cluster_id}:${each.key}"
  entity_type = "kafka_topic"

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
  depends_on = [confluent_kafka_topic.this, confluent_tag.defs]
}

resource "confluent_tag_binding" "product" {
  for_each    = local.products
  tag_name    = "DataProduct"
  entity_name = "${local.sr.id}:${local.cluster_id}:${each.key}"
  entity_type = "kafka_topic"

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
  depends_on = [confluent_kafka_topic.this, confluent_tag.defs]
}

resource "confluent_catalog_entity_attributes" "topic" {
  for_each    = local.products
  # provider docs show "<lkc>:<topic>" here, while tag bindings use "<lsrc>:<lkc>:<topic>" [V]
  entity_name = "${local.cluster_id}:${each.key}"
  entity_type = "kafka_topic"
  attributes = {
    owner       = each.value.owner
    ownerEmail  = each.value.owner_email
    description = each.value.description
  }

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
  depends_on = [confluent_kafka_topic.this]
}
```

`advanced.tf` (apply only against the Advanced environment; set `-var advanced=true`):

```hcl
variable "advanced" {
  type    = bool
  default = false
}

resource "confluent_business_metadata" "contract" {
  count       = var.advanced ? 1 : 0
  name        = "ProductContract"
  description = "Data product contract fields"
  attribute_definition { name = "domain" }
  attribute_definition { name = "sla" }
  attribute_definition {
    name        = "support"
    is_optional = true
  }

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
}

resource "confluent_business_metadata_binding" "contract" {
  for_each               = var.advanced ? local.products : {}
  business_metadata_name = confluent_business_metadata.contract[0].name
  entity_name            = "${local.sr.id}:${local.cluster_id}:${each.key}"
  entity_type            = "kafka_topic"
  attributes = {
    domain  = split(".", each.key)[0]
    sla     = "T1"
    support = "#${split(".", each.key)[0]}-platform"
  }

  schema_registry_cluster { id = local.sr.id }
  rest_endpoint = local.sr.endpoint
  credentials {
    key    = local.sr_key.id
    secret = local.sr_key.secret
  }
  depends_on = [confluent_kafka_topic.this]
}
```

Rules in Terraform: add a `ruleset { domain_rules { ... } }` block to `confluent_schema` (kind,
type, mode, expr, on_failure, params). Advanced only. Do not leave empty `metadata {}` or
`ruleset {}` blocks — the provider treats them as a diff.

Run:

```bash
cd 03-stream-governance/terraform
terraform init && terraform plan -out tf.plan && terraform apply tf.plan
```

Expected: 4 tags, 1 topic (`customers.customer-journey.v1`), 2 subject configs, 2 schemas, 4 tag
bindings, 2 entity attribute resources. If you did Part B first, delete what it created (Teardown,
CLI path) or Terraform will hit "already exists" on the tags and the topic. If tag bindings fail with "not found", re-run apply (catalog lag); the
`disable_wait_for_ready` default (`false`) should normally handle it.

# Verification

| Check | Command | Expected |
|---|---|---|
| Compatibility mode | `curl -s -u $SR_AUTH $SR_URL/config/customers.customer-journey.v1-value` | `BACKWARD_TRANSITIVE` |
| PII fields | B5 first query | 2 fields |
| Topic classification | B5 second query | `Public` / `Confidential` + `DataProduct` |
| Owner set | Data Portal → topic | owner, email, description shown |
| Drift | change the description in the console, run `terraform plan` | plan shows the change back |

The last check is the point of "Git is the source of truth": the console edit is overwritten.

# Teardown

```bash
# Terraform path
cd 03-stream-governance/terraform && terraform destroy

# CLI path (if you did not use Terraform). Keep orders.order-status.updated.v1: 00-lab-setup owns it.
confluent kafka topic delete customers.customer-journey.v1 --force
for s in orders.order-status.updated.v1-value customers.customer-journey.v1-value; do
  curl -s -u $SR_AUTH -X DELETE "$SR_URL/subjects/$s"                  # soft delete
  curl -s -u $SR_AUTH -X DELETE "$SR_URL/subjects/$s?permanent=true"   # hard delete (frees schema count)
done
for t in PII Public Confidential DataProduct; do
  curl -s -u $SR_AUTH -X DELETE $SR_URL/catalog/v1/types/tagdefs/$t
done

# Advanced and Standard extras: delete the whole environment (only way to drop Advanced)
confluent environment delete $ADV_ENV --force

# local
a kafka-topics --bootstrap-server dc-a:29092 --delete --topic orders.order-status.updated.v1
a kafka-topics --bootstrap-server dc-a:29092 --delete --topic customers.customer-journey.v1
```

Soft-deleted schema versions still count against the schema allowance until hard-deleted.

# Cost note [V]

| Part | Cost |
|---|---|
| A (local) | none |
| B (cc-basic, Essentials) | Basic cluster usage only; Essentials includes 100 schemas |
| C (Advanced env) | Advanced hourly charge (console "from $1/hour") + a Basic cluster; delete the env the same day |
| D (Standard cluster) | Standard hourly base + usage; delete within the session |
