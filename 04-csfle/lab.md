---
title: 04 — Lab — encrypt customer PII with a KEK in Azure Key Vault
subtitle: CSFLE on customers.customer-journey.v1. Consumers with and without key access, two checks (Confluent RBAC on the KEK, Azure RBAC on the key), rotation. Confluent Cloud path first, local CP path as a variant.
---

## Goal

1. Register a schema for `customers.customer-journey.v1` with two fields tagged `PII` and an ENCRYPT rule that uses a KEK in Azure Key Vault.
2. Produce records; see ciphertext in the Console and with a raw consumer.
3. Consume with: (A) full access → plaintext; (B) Key Vault access but no Confluent RBAC on the KEK → ciphertext; (C) RBAC on the KEK but no Key Vault access → ciphertext.
4. Look inside the DEK Registry; rotate the Key Vault key and see why versioned key ids matter.

## Environments used

| Path | Environment | Paid feature |
|---|---|---|
| Main | **Confluent Cloud**, a separate environment `crash-course-csfle` with **Stream Governance Advanced** + a Basic cluster on Azure `westeurope`, deleted at the end | **CSFLE needs Stream Governance Advanced [V]** (billed per environment-hour) |
| Variant | `local` (`dc-a` + its Schema Registry) with the DEK Registry extension enabled; KMS = Azure Key Vault or `local-kms` | CP docs: **Confluent Enterprise 8.0+ with CSFLE add-on licence [V]**. Whether the 30-day trial mode enables the add-on is not documented — test it. |

Why a separate environment and not `crash-course` itself: the package is set per environment. A separate environment keeps the `cc-basic` base on Essentials and makes teardown one command. Downgrading an environment from Advanced back to Essentials may not be possible **[V]** — another reason not to touch the base. The Basic cluster type is fine: encryption happens in the client.

Azure: one resource group with a Key Vault (Azure RBAC mode), one RSA key, and three Entra service principals.

## Prerequisites

- `confluent` CLI v4.x, logged in as OrganizationAdmin (to create the environment).
- `az` CLI logged in; rights to create Key Vault and role assignments (Owner or User Access Administrator on the resource group) and app registrations.
- Java 17+ and **Confluent Platform 8.x tools** with the Avro console producer/consumer. Easiest: run them inside the `dc-a` Schema Registry container from `00-lab-setup` (it ships `kafka-avro-console-producer`). Check the Azure encryption jar is present:
  ```bash
  docker compose -f ../00-lab-setup/docker-compose.yml exec <dc-a-schema-registry-service> \
    bash -c 'ls /usr/share/java/schema-registry /usr/share/java/kafka-serde-tools 2>/dev/null | grep -i encryption'
  ```
  Expected: `kafka-schema-registry-client-encryption-azure-*.jar` among others **[V]**. If missing, download the CP 8.x tarball and use its `bin/` instead.
- `jq`, `curl`.

---

## Part 1 — CLI path (Confluent Cloud)

### 1.1 Azure: Key Vault, KEK, identities

```bash
export AZ_REGION=westeurope
RG=rg-crash-csfle
SUFFIX=$RANDOM
KV=kv-crash-csfle-$SUFFIX
TENANT=$(az account show --query tenantId -o tsv)

az group create -n $RG -l $AZ_REGION
az keyvault create -n $KV -g $RG -l $AZ_REGION --enable-rbac-authorization true --retention-days 7
KV_ID=$(az keyvault show -n $KV --query id -o tsv)
az role assignment create --role "Key Vault Crypto Officer" \
  --assignee $(az ad signed-in-user show --query id -o tsv) --scope $KV_ID
sleep 30   # RBAC propagation

az keyvault key create --vault-name $KV -n kek-customers --kty RSA --size 3072 --ops encrypt decrypt wrapKey unwrapKey
KEK_URI=$(az keyvault key show --vault-name $KV -n kek-customers --query key.kid -o tsv)
echo $KEK_URI     # https://kv-crash-csfle-NNNN.vault.azure.net/keys/kek-customers/<32-hex version>
```

Expected: `KEK_URI` ends with a **version**. Keep it versioned (see Part 1.7).

Three service principals. The key-scoped role assignment gives only encrypt/decrypt on this one key:

```bash
KEY_SCOPE=$KV_ID/keys/kek-customers
az ad sp create-for-rbac -n sp-csfle-producer --role "Key Vault Crypto User" --scopes $KEY_SCOPE -o json > /tmp/sp-producer.json
az ad sp create-for-rbac -n sp-csfle-reader   --role "Key Vault Crypto User" --scopes $KEY_SCOPE -o json > /tmp/sp-reader.json
az ad sp create-for-rbac -n sp-csfle-nokv -o json > /tmp/sp-nokv.json       # no role at all
```

Expected: each file has `appId`, `password`, `tenant`. (Microsoft Learn: newer `az` versions create no role assignment unless `--role` is given.)

### 1.2 Confluent: environment, cluster, Schema Registry, PII tag, KEK

```bash
confluent environment create crash-course-csfle --governance-package advanced -o json | tee /tmp/env.json
export CSFLE_ENV=$(jq -r .id /tmp/env.json)
confluent environment use $CSFLE_ENV
confluent kafka cluster create cc-csfle --cloud azure --region $AZ_REGION --type basic -o json | tee /tmp/cl.json
export CSFLE_CL=$(jq -r .id /tmp/cl.json)
confluent kafka cluster use $CSFLE_CL
sleep 60
confluent schema-registry cluster describe -o json | tee /tmp/sr.json
export SR_URL=$(jq -r '.endpoint_url // .url' /tmp/sr.json)
export SR_ID=$(jq -r '.cluster // .id' /tmp/sr.json)
```

Expected: environment with package `ADVANCED`, Basic cluster `lkc-…`, Schema Registry `lsrc-…` with a `https://psrc-….westeurope.azure.confluent.cloud` endpoint (field names vary by CLI version **[V]**).

Service accounts — one per consumer pattern, so we can show the Confluent RBAC check:

```bash
for sa in sa-csfle-producer sa-csfle-reader sa-csfle-nokek; do
  confluent iam service-account create $sa --description "lab04" -o json > /tmp/$sa.json
done
P=$(jq -r .id /tmp/sa-csfle-producer.json); R=$(jq -r .id /tmp/sa-csfle-reader.json); N=$(jq -r .id /tmp/sa-csfle-nokek.json)
```

Kafka access: Basic clusters do **not** support RBAC on Kafka resources, so use ACLs for the topic (RBAC works on Schema Registry, which is environment-level):

```bash
confluent kafka topic create customers.customer-journey.v1 --partitions 3
confluent kafka acl create --allow --service-account $P --operations write,describe --topic customers. --prefix
for s in $R $N; do
  confluent kafka acl create --allow --service-account $s --operations read,describe --topic customers. --prefix
  confluent kafka acl create --allow --service-account $s --operations read --consumer-group lab04- --prefix
done
for s in $P $R $N; do
  confluent api-key create --resource $CSFLE_CL --service-account $s -o json > /tmp/kafka-$s.json
  confluent api-key create --resource $SR_ID    --service-account $s -o json > /tmp/sr-$s.json
done
```

Schema Registry RBAC — subjects for everyone, KEK only for producer and reader:

```bash
SUBJ=customers.customer-journey.v1-value
confluent iam rbac role-binding create --principal User:$P --role DeveloperWrite --environment $CSFLE_ENV --schema-registry-cluster $SR_ID --resource "Subject:$SUBJ"
confluent iam rbac role-binding create --principal User:$P --role DeveloperRead  --environment $CSFLE_ENV --schema-registry-cluster $SR_ID --resource "Subject:$SUBJ"
confluent iam rbac role-binding create --principal User:$R --role DeveloperRead  --environment $CSFLE_ENV --schema-registry-cluster $SR_ID --resource "Subject:$SUBJ"
confluent iam rbac role-binding create --principal User:$N --role DeveloperRead  --environment $CSFLE_ENV --schema-registry-cluster $SR_ID --resource "Subject:$SUBJ"
confluent iam rbac role-binding create --principal User:$P --role DeveloperWrite --environment $CSFLE_ENV --schema-registry-cluster $SR_ID --resource "Kek:kek-customers"
confluent iam rbac role-binding create --principal User:$R --role DeveloperRead  --environment $CSFLE_ENV --schema-registry-cluster $SR_ID --resource "Kek:kek-customers"
```

Expected: each command prints the new binding. (`Kek:` resource type in the CLI **[V]** — the Terraform form is `crn_pattern = "<sr resource_name>/kek=kek-customers"`.)

Admin SR key for setup (your user), then the PII tag definition (Stream Catalog API) and the KEK:

```bash
confluent api-key create --resource $SR_ID -o json > /tmp/sr-admin.json
SR_AUTH="$(jq -r .api_key /tmp/sr-admin.json):$(jq -r .api_secret /tmp/sr-admin.json)"

curl -s -u $SR_AUTH -X POST "$SR_URL/catalog/v1/types/tagdefs" -H 'Content-Type: application/json' \
  -d '[{"entityTypes":["sr_field"],"name":"PII","description":"Personal data, must be encrypted before leaving the DC"}]' | jq .

confluent schema-registry kek create kek-customers --kms-type azure-kms --kms-key "$KEK_URI" \
  --doc "customer PII, non-shared" --environment $CSFLE_ENV
confluent schema-registry kek describe kek-customers
```

Expected: tag `PII` created; KEK `kek-customers` with `kmsType: azure-kms`, `shared: false`.

### 1.3 Schema with tags and the ENCRYPT rule

```bash
cat > /tmp/customer-journey.avsc <<'EOF'
{
  "type": "record",
  "name": "CustomerJourney",
  "namespace": "com.example.customers",
  "fields": [
    {"name": "journey_id",    "type": "string"},
    {"name": "customer_key",  "type": "string", "doc": "pseudonymous id"},
    {"name": "customer_name", "type": "string", "confluent:tags": ["PII"]},
    {"name": "national_id",   "type": "string", "confluent:tags": ["PII"], "doc": "national ID-like reference"},
    {"name": "order_id",      "type": "string"},
    {"name": "location",      "type": "string"},
    {"name": "event_time",    "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
EOF

jq -n --arg schema "$(cat /tmp/customer-journey.avsc)" --arg kek "$KEK_URI" '{
  schemaType: "AVRO",
  schema: $schema,
  metadata: {properties: {owner: "customers-team", classification: "confidential"}},
  ruleSet: {domainRules: [{
    name: "encrypt-pii", kind: "TRANSFORM", type: "ENCRYPT", mode: "WRITEREAD",
    tags: ["PII"],
    params: {"encrypt.kek.name": "kek-customers", "encrypt.kms.type": "azure-kms", "encrypt.kms.key.id": $kek},
    onFailure: "ERROR,NONE"
  }]}
}' > /tmp/register.json

curl -s -u $SR_AUTH -X POST "$SR_URL/subjects/$SUBJ/versions" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d @/tmp/register.json | tee /tmp/schema-id.json
SCHEMA_ID=$(jq -r .id /tmp/schema-id.json)
curl -s -u $SR_AUTH "$SR_URL/subjects/$SUBJ/versions/latest" | jq '{id, ruleSet, metadata}'
```

Expected: an `id`; `latest` shows the rule set with `encrypt-pii`.

### 1.4 Produce

Client config files (one per service account):

```bash
BOOT=$(confluent kafka cluster describe $CSFLE_CL -o json | jq -r .endpoint | sed 's#SASL_SSL://##')
mk() { # $1=sa id  $2=file
cat > $2 <<EOF
bootstrap.servers=$BOOT
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='$(jq -r .api_key /tmp/kafka-$1.json)' password='$(jq -r .api_secret /tmp/kafka-$1.json)';
EOF
}
mk $P /tmp/producer.properties; mk $R /tmp/reader.properties; mk $N /tmp/nokek.properties
```

Producer (run where the CP 8.x tools are; copy the `/tmp` files into the container if you use it):

```bash
kafka-avro-console-producer --bootstrap-server $BOOT --topic customers.customer-journey.v1 \
  --producer.config /tmp/producer.properties \
  --reader-property schema.registry.url=$SR_URL \
  --reader-property basic.auth.credentials.source=USER_INFO \
  --reader-property basic.auth.user.info=$(jq -r .api_key /tmp/sr-$P.json):$(jq -r .api_secret /tmp/sr-$P.json) \
  --reader-property value.schema.id=$SCHEMA_ID \
  --reader-property auto.register.schemas=false \
  --reader-property use.latest.version=true \
  --reader-property rule.executors._default_.param.tenant.id=$TENANT \
  --reader-property rule.executors._default_.param.client.id=$(jq -r .appId /tmp/sp-producer.json) \
  --reader-property rule.executors._default_.param.client.secret=$(jq -r .password /tmp/sp-producer.json)
```

Paste, one per line:

```json
{"journey_id":"j-001","customer_key":"pk-7f3a","customer_name":"Anna de Vries","national_id":"ID1234567","order_id":"SO-1001234","location":"ZONE-A2","event_time":1762508400000}
{"journey_id":"j-002","customer_key":"pk-91bc","customer_name":"Ravi Kumar","national_id":"ID9876543","order_id":"SO-1000645","location":"ZONE-B4","event_time":1762508460000}
```

Expected log line, once: `Registered dek for kek kek-customers, subject customers.customer-journey.v1-value` (wording may differ). Ctrl-D to finish.

### 1.5 Consume — three cases plus raw

Common part:

```bash
consume() { # $1=props $2=sa $3=sp json $4=group
kafka-avro-console-consumer --bootstrap-server $BOOT --topic customers.customer-journey.v1 \
  --consumer.config $1 --group $4 --from-beginning --max-messages 2 \
  --formatter-property schema.registry.url=$SR_URL \
  --formatter-property basic.auth.credentials.source=USER_INFO \
  --formatter-property basic.auth.user.info=$(jq -r .api_key /tmp/sr-$2.json):$(jq -r .api_secret /tmp/sr-$2.json) \
  --formatter-property rule.executors._default_.param.tenant.id=$TENANT \
  --formatter-property rule.executors._default_.param.client.id=$(jq -r .appId $3) \
  --formatter-property rule.executors._default_.param.client.secret=$(jq -r .password $3)
}
```

| Case | Command | Expected |
|---|---|---|
| A — full access | `consume /tmp/reader.properties $R /tmp/sp-reader.json lab04-a` | `customer_name: "Anna de Vries"`, `national_id: "ID1234567"` in plaintext |
| B — Key Vault yes, KEK RBAC no | `consume /tmp/nokek.properties $N /tmp/sp-reader.json lab04-b` | record printed, PII fields are base64 ciphertext; log shows a 403 from the DEK Registry |
| C — KEK RBAC yes, Key Vault no | `consume /tmp/reader.properties $R /tmp/sp-nokv.json lab04-c` | PII fields ciphertext; log shows a Key Vault `Forbidden` on decrypt |
| D — no rule executor credentials at all | same as A without the three `rule.executors…` lines | default Azure credential chain fails → ciphertext (or uses your `az login` identity if the chain finds it — interesting to observe) |
| Raw | `kafka-console-consumer --bootstrap-server $BOOT --consumer.config /tmp/reader.properties --topic customers.customer-journey.v1 --from-beginning --max-messages 2` | Avro bytes; `SO-1001234`, `ZONE-A2` visible, names not visible |

Also open the topic in the Confluent Cloud Console → Messages: `customer_name` shows ciphertext. The Console is just another client without the key.

Optional: `confluent kafka topic consume customers.customer-journey.v1 -b --value-format avro --schema-registry-endpoint $SR_URL ...` — note whether the Go-based CLI applies the rule or shows ciphertext **[V]**.

Switch case B's read failure mode to `ERROR` to see the difference: register a new schema version with `"onFailure": "ERROR,ERROR"`, rerun case B, expect the consumer to stop with a serialization exception. Set it back to `ERROR,NONE` after.

### 1.6 Inside the DEK Registry

```bash
curl -s -u $SR_AUTH "$SR_URL/dek-registry/v1/keks/kek-customers" | jq .
curl -s -u $SR_AUTH "$SR_URL/dek-registry/v1/keks/kek-customers/deks" | jq .
curl -s -u $SR_AUTH "$SR_URL/dek-registry/v1/keks/kek-customers/deks/$SUBJ" | jq .
```

Expected: the KEK has `kmsKeyId` = your Key Vault URI and `shared: false`. The DEK for the subject has `algorithm: AES256_GCM` and `encryptedKeyMaterial` (base64), **no** `keyMaterial` — Confluent cannot unwrap it.

### 1.7 Rotation and revocation

**Revoke the reader in Azure:**

```bash
az role assignment delete --assignee $(jq -r .appId /tmp/sp-reader.json) --role "Key Vault Crypto User" --scope $KEY_SCOPE
sleep 60
consume /tmp/reader.properties $R /tmp/sp-reader.json lab04-a2
```

Expected: ciphertext now (new process, no cached DEK). A long-running consumer would keep plaintext until its DEK cache or token expires — this is the answer to "how fast is revocation?". Re-add the role after.

**Rotate the KEK in Key Vault:**

```bash
az keyvault key rotate --vault-name $KV -n kek-customers
az keyvault key list-versions --vault-name $KV -n kek-customers --query '[].kid' -o tsv
consume /tmp/reader.properties $R /tmp/sp-reader.json lab04-a3
```

Expected: still plaintext, because the KEK was registered with a **versioned** URI and the old version is still enabled. Now think about the other case: if the KEK had been registered **without** a version, the client would resolve to the latest version, which did not wrap the existing DEK → unwrap fails → ciphertext. The fix is `encrypt.azure.key.version.save=true` and rewrapping DEKs before rotation (CC manage-keys doc). Rewrapping uses the `rewrap-deks` tool or the DEK API with `rewrap=true` **[V]** — read the doc, do not run it blind.

**DEK rotation:** add `"encrypt.dek.expiry.days": "1"` to the rule params in a new schema version, produce one record, and look at `deks/$SUBJ/versions` the next day if the environment still exists (it should not — this one is "read only").

---

## Part 2 — Terraform path (same end state)

Providers: `confluentinc/confluent ~> 2.94`, `hashicorp/azurerm ~> 5.13`, `hashicorp/azuread ~> 3.10`. The block passes `terraform validate` with confluent 2.94.0, azurerm 5.13.0, azuread 3.10.0 (checked 2026-11-07); `apply` has not been run.

```hcl
terraform {
  required_version = ">= 1.9"
  required_providers {
    confluent = { source = "confluentinc/confluent", version = "~> 2.94" }
    azurerm   = { source = "hashicorp/azurerm",      version = "~> 5.13" }
    azuread   = { source = "hashicorp/azuread",      version = "~> 3.10" }
  }
}

provider "confluent" {}                 # CONFLUENT_CLOUD_API_KEY/SECRET (OrganizationAdmin)
provider "azurerm" {
  features {}
}
provider "azuread" {}

variable "region" { default = "westeurope" }

data "azurerm_client_config" "me" {}
resource "random_id" "s" { byte_length = 2 }

# ---------------- Azure ----------------
resource "azurerm_resource_group" "rg" {
  name     = "rg-crash-csfle"
  location = var.region
}

resource "azurerm_key_vault" "kv" {
  name                       = "kv-crash-csfle-${random_id.s.hex}"
  location                   = var.region
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.me.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = false     # lab only; true in production
}

resource "azurerm_role_assignment" "me_crypto_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Crypto Officer"
  principal_id         = data.azurerm_client_config.me.object_id
}

resource "azurerm_key_vault_key" "kek" {
  name         = "kek-customers"
  key_vault_id = azurerm_key_vault.kv.id
  key_type     = "RSA"
  key_size     = 3072
  key_opts     = ["encrypt", "decrypt", "wrapKey", "unwrapKey"]
  depends_on   = [azurerm_role_assignment.me_crypto_officer]
}

locals {
  apps = toset(["producer", "reader", "nokv"])
}

resource "azuread_application" "app" {
  for_each     = local.apps
  display_name = "sp-csfle-${each.key}"
}
resource "azuread_service_principal" "sp" {
  for_each  = local.apps
  client_id = azuread_application.app[each.key].client_id
}
resource "azuread_application_password" "pw" {
  for_each       = local.apps
  application_id = azuread_application.app[each.key].id
}

# Key-scoped: encrypt/decrypt on this key only. "nokv" gets nothing.
resource "azurerm_role_assignment" "crypto_user" {
  for_each             = toset(["producer", "reader"])
  scope                = azurerm_key_vault_key.kek.resource_versionless_id
  role_definition_name = "Key Vault Crypto User"
  principal_id         = azuread_service_principal.sp[each.key].object_id
}

# ---------------- Confluent ----------------
resource "confluent_environment" "csfle" {
  display_name = "crash-course-csfle"
  stream_governance { package = "ADVANCED" }
}

resource "confluent_kafka_cluster" "basic" {
  display_name = "cc-csfle"
  availability = "SINGLE_ZONE"
  cloud        = "AZURE"
  region       = var.region
  basic {}
  environment { id = confluent_environment.csfle.id }
}

data "confluent_schema_registry_cluster" "sr" {
  environment { id = confluent_environment.csfle.id }
  depends_on = [confluent_kafka_cluster.basic]
}

locals { sas = toset(["producer", "reader", "nokek"]) }

resource "confluent_service_account" "sa" {
  for_each     = local.sas
  display_name = "sa-csfle-${each.key}"
  description  = "lab04"
}

# Admin SA for Terraform data-plane calls (topic, schema, tag, KEK)
resource "confluent_service_account" "admin" {
  display_name = "sa-csfle-admin"
  description  = "lab04 admin"
}
resource "confluent_role_binding" "admin_cluster" {
  principal   = "User:${confluent_service_account.admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.basic.rbac_crn
}
resource "confluent_role_binding" "admin_env" {
  principal   = "User:${confluent_service_account.admin.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.csfle.resource_name
}

resource "confluent_api_key" "admin_kafka" {
  display_name = "csfle-admin-kafka"
  owner {
    id          = confluent_service_account.admin.id
    api_version = confluent_service_account.admin.api_version
    kind        = confluent_service_account.admin.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind
    environment { id = confluent_environment.csfle.id }
  }
  depends_on = [confluent_role_binding.admin_cluster]
}

resource "confluent_api_key" "admin_sr" {
  display_name = "csfle-admin-sr"
  owner {
    id          = confluent_service_account.admin.id
    api_version = confluent_service_account.admin.api_version
    kind        = confluent_service_account.admin.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment { id = confluent_environment.csfle.id }
  }
  depends_on = [confluent_role_binding.admin_env]
}

resource "confluent_kafka_topic" "pj" {
  kafka_cluster { id = confluent_kafka_cluster.basic.id }
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint
  topic_name       = "customers.customer-journey.v1"
  partitions_count = 3
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Basic cluster: Kafka access by ACL (no Kafka RBAC on Basic)
resource "confluent_kafka_acl" "write" {
  kafka_cluster { id = confluent_kafka_cluster.basic.id }
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  resource_type = "TOPIC"
  resource_name = "customers."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.sa["producer"].id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}
resource "confluent_kafka_acl" "read" {
  for_each      = toset(["reader", "nokek"])
  kafka_cluster { id = confluent_kafka_cluster.basic.id }
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  resource_type = "TOPIC"
  resource_name = "customers."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.sa[each.key].id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}
resource "confluent_kafka_acl" "group" {
  for_each      = toset(["reader", "nokek"])
  kafka_cluster { id = confluent_kafka_cluster.basic.id }
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  resource_type = "GROUP"
  resource_name = "lab04-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.sa[each.key].id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  credentials {
    key    = confluent_api_key.admin_kafka.id
    secret = confluent_api_key.admin_kafka.secret
  }
}

# Schema Registry RBAC: subject for all, KEK only for producer (write) and reader (read)
locals {
  sr_crn = data.confluent_schema_registry_cluster.sr.resource_name
  subj   = "${confluent_kafka_topic.pj.topic_name}-value"
}
resource "confluent_role_binding" "subj_read" {
  for_each    = local.sas
  principal   = "User:${confluent_service_account.sa[each.key].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.sr_crn}/subject=${local.subj}"
}
resource "confluent_role_binding" "kek_write_producer" {
  principal   = "User:${confluent_service_account.sa["producer"].id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${local.sr_crn}/kek=kek-customers"
}
resource "confluent_role_binding" "kek_read_reader" {
  principal   = "User:${confluent_service_account.sa["reader"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.sr_crn}/kek=kek-customers"
}

resource "confluent_tag" "pii" {
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.sr.id }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
  name        = "PII"
  description = "Personal data, must be encrypted before leaving the DC"
}

resource "confluent_schema_registry_kek" "kek" {
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.sr.id }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
  name        = "kek-customers"
  kms_type    = "azure-kms"
  kms_key_id  = azurerm_key_vault_key.kek.id     # versioned key identifier
  shared      = false
  hard_delete = true
}

resource "confluent_schema" "pj" {
  schema_registry_cluster { id = data.confluent_schema_registry_cluster.sr.id }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  credentials {
    key    = confluent_api_key.admin_sr.id
    secret = confluent_api_key.admin_sr.secret
  }
  subject_name = local.subj
  format       = "AVRO"
  schema       = file("${path.module}/customer-journey.avsc")   # same file as Part 1.3
  ruleset {
    domain_rules {
      name       = "encrypt-pii"
      kind       = "TRANSFORM"
      type       = "ENCRYPT"
      mode       = "WRITEREAD"
      tags       = [confluent_tag.pii.name]
      params     = { "encrypt.kek.name" = confluent_schema_registry_kek.kek.name }
      on_failure = "ERROR,NONE"
    }
  }
  hard_delete = true
}

# Per-SA client keys (Kafka + SR) for the console tools
resource "confluent_api_key" "kafka" {
  for_each     = local.sas
  display_name = "csfle-${each.key}-kafka"
  owner {
    id          = confluent_service_account.sa[each.key].id
    api_version = confluent_service_account.sa[each.key].api_version
    kind        = confluent_service_account.sa[each.key].kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind
    environment { id = confluent_environment.csfle.id }
  }
}
resource "confluent_api_key" "sr" {
  for_each     = local.sas
  display_name = "csfle-${each.key}-sr"
  owner {
    id          = confluent_service_account.sa[each.key].id
    api_version = confluent_service_account.sa[each.key].api_version
    kind        = confluent_service_account.sa[each.key].kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment { id = confluent_environment.csfle.id }
  }
}

output "tenant_id"   { value = data.azurerm_client_config.me.tenant_id }
output "kek_uri"     { value = azurerm_key_vault_key.kek.id }
output "bootstrap"   { value = confluent_kafka_cluster.basic.bootstrap_endpoint }
output "sr_url"      { value = data.confluent_schema_registry_cluster.sr.rest_endpoint }
output "schema_id"   { value = confluent_schema.pj.schema_identifier }
output "app_ids"     { value = { for k, a in azuread_application.app : k => a.client_id } }
output "app_secrets" {
  value     = { for k, p in azuread_application_password.pw : k => p.value }
  sensitive = true
}
```

Add `random` to `required_providers` (`hashicorp/random`) if your Terraform warns. Then run Parts 1.4–1.7 with the outputs (`terraform output -json app_secrets`).

Note: the rule here sets only `encrypt.kek.name`; the KMS type and key id come from the registered KEK. Part 1.3 sets them explicitly — both work.

---

## Part 3 — Variant on `local` (CP 8.x)

Add to the `dc-a` Schema Registry service, as a compose override file in your own scratch folder (do not edit `00-lab-setup`):

```yaml
services:
  <dc-a-schema-registry-service>:
    environment:
      SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS: "io.confluent.kafka.schemaregistry.rulehandler.RuleSetResourceExtension,io.confluent.dekregistry.DekRegistryResourceExtension"
      SCHEMA_REGISTRY_CONFLUENT_LICENSE: "${CP_LICENSE:-}"               # [V] empty = trial; add-on behaviour in trial is undocumented
      SCHEMA_REGISTRY_CONFLUENT_LICENSE_ADDON_CSFLE: "${CP_LICENSE:-}"
```

```bash
docker compose -f ../00-lab-setup/docker-compose.yml -f override.yml up -d <dc-a-schema-registry-service>
curl -s http://localhost:<dc-a SR port>/dek-registry/v1/keks
```

Expected: `[]` (the DEK Registry endpoint exists). A 404 means the extension did not load — check the SR logs for a licence message. If the add-on refuses to run without a licence, use the Confluent Cloud path and note it as a finding.

Then repeat 1.3–1.5 against the local SR (no auth), with either:
- **Azure Key Vault**: same rule and credentials as above — the client talks to Key Vault directly; the local DEK Registry never needs Azure access. This mirrors the on-prem producer in the target design.
- **Local KMS (offline)**: rule params `"encrypt.kms.type": "local-kms"`, `"encrypt.kms.key.id": "lab-key"` **[V]**, and client property `rule.executors._default_.param.secret=$(openssl rand -base64 16)` (or env `LOCAL_SECRET`). Testing only.

Optional, connects to module 03/Cluster Linking: if `dc-b` has a Schema Registry and you run a schema exporter from `dc-a` to `dc-b`, check whether the DEK appears in `dc-b`'s `/dek-registry/v1/keks/kek-customers/deks` — the CC docs say Schema Linking copies DEKs and KEK pointers.

---

## Verification checklist

- [ ] `latest` schema shows the `encrypt-pii` rule and PII tags.
- [ ] DEK exists with `encryptedKeyMaterial` only.
- [ ] Case A plaintext; cases B, C, D ciphertext; raw consumer shows no names.
- [ ] Console shows ciphertext for `customer_name`.
- [ ] Removing the reader's Key Vault role → new consumer sees ciphertext.
- [ ] Key rotation with a versioned key id → still plaintext.

## Teardown

```bash
confluent environment delete $CSFLE_ENV --force       # removes cluster, SR, keys, KEK metadata, role bindings
for s in $P $R $N; do confluent iam service-account delete $s --force; done
for n in sp-csfle-producer sp-csfle-reader sp-csfle-nokv; do az ad app delete --id $(az ad app list --display-name $n --query '[0].appId' -o tsv); done
az group delete -n $RG --yes --no-wait
az keyvault purge -n $KV --location $AZ_REGION        # after the group is gone; frees the name
rm -f /tmp/sp-*.json /tmp/kafka-*.json /tmp/sr-*.json /tmp/*.properties
```

Terraform path: `terraform destroy` (then `az keyvault purge` if you want the name back). The CC environment is deleted with it.

## Cost note

| Item | Rough cost **[V]** |
|---|---|
| Stream Governance Advanced on `crash-course-csfle` | billed per environment-hour; a 2-hour lab costs a few USD at most — check the current price before starting |
| Basic cluster | free (first eCKU) |
| Key Vault standard, RSA key operations | cents |
| Entra app registrations | free |

Delete the environment the same day; Advanced keeps billing while the environment exists.
