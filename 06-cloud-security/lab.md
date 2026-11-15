---
title: 06 — Lab — team service accounts, prefix RBAC, OAuth with Entra ID, audit log, quotas
subtitle: Part 1 on cc-basic (ACLs, because Basic has no Kafka RBAC), Part 2 on a temporary Standard cluster (RBAC, OAuth, audit log), Part 3 quota in the paid-session, Part 4 quotas on local CP
---

> **Video:** episode 6, "Who is allowed to read this" (coming soon). The video gives the reasoning; this lab lets you see it happen.

## Goal

1. Per-team service accounts with least privilege, scoped by topic prefix.
2. Same with RBAC role bindings on a cluster type that supports them.
3. Entra ID app registrations (client credentials) → Confluent identity provider + identity pool → RBAC. Produce/consume with OAuth. Reproduce the group-assignment pitfall.
4. Find your own authentication and denied-authorization events in the audit log.
5. Apply a client quota and see throttling — in Confluent Cloud (bytes only) and on-prem (bytes + request percentage).

## Environments used — and why not only `cc-basic`

| Feature | Basic | Standard | Enterprise | Where this lab does it |
|---|---|---|---|---|
| Service accounts, API keys, ACLs | yes | yes | yes | Part 1, `cc-basic` |
| RBAC on topics / groups | **no** | yes | yes | Part 2, `cc-std-lab06` |
| OAuth for Kafka | **no** | yes | yes | Part 2 |
| Audit logs | **no** | yes | yes | Part 2 |
| Client quotas | **no** | **no** | yes | Part 3, `paid-session` Enterprise cluster (module 03) |
| Quotas incl. `request_percentage` | — | — | — | Part 4, `local` `dc-a` |

`cc-std-lab06` = a **Standard** cluster on Azure `westeurope` in environment `crash-course`, created at the start of Part 2 and deleted at the end. List price is 0.75 USD per eCKU-hour and Standard scales to zero when idle **[V]**, so 2 hours cost about 1–2 USD. This is an addition to the agreed environments; it is the cheapest way to test RBAC, OAuth and audit logs. If you prefer, run Part 2 against the paid-session Enterprise cluster from the VM instead (Enterprise supports all of it) — then skip the Standard cluster.

## Prerequisites

- `confluent` CLI v4.x as OrganizationAdmin (identity providers and audit-log keys need it).
- `az` CLI with rights to create app registrations in your Entra tenant (Application Developer or higher) and to grant app role assignments (Cloud Application Administrator or the app owner).
- Kafka 4.x CLI tools (or CP 8.x) and Java 17+; `jq`, `curl`, `base64`.
- Topics used: `billing.call-record.created.v1`, `noc.cell-alarm.raised.v1`, `network.cell-load.measured.v1` (PII-free), `customers.subscriber-profile.v1` (PII).
- Part 4 (`local`): start clean, because module 02 created the same topic on `dc-a`: `( cd "$LAB" && docker compose down -v && docker compose up -d )`

```bash
cd ../00-lab-setup/terraform
export CC_ENV=$(terraform output -raw environment_id)
export CC_BASIC=$(terraform output -raw basic_cluster_id)
export SR_AUTH="$(terraform output -raw sa_sr_api_key):$(terraform output -raw sa_sr_api_secret)"   # Part 1 produces Avro
cd -
export TENANT=$(az account show --query tenantId -o tsv)
confluent environment use $CC_ENV
```

---

## Part 1 — Team service accounts and ACLs on `cc-basic` (CLI)

```bash
confluent kafka cluster use $CC_BASIC
for t in billing.call-record.created.v1 noc.cell-alarm.raised.v1 network.cell-load.measured.v1 customers.subscriber-profile.v1; do
  confluent kafka topic create $t --partitions 3 2>/dev/null || true
done

for sa in sa-billing-producer sa-analytics-consumer sa-cells-app; do
  confluent iam service-account create $sa --description "lab06 team account" -o json > /tmp/$sa.json
done
BIL=$(jq -r .id /tmp/sa-billing-producer.json)
ANA=$(jq -r .id /tmp/sa-analytics-consumer.json)
CEL=$(jq -r .id /tmp/sa-cells-app.json)
```

ACLs by prefix (Basic has no Kafka RBAC):

```bash
# billing team: write its own domain only
confluent kafka acl create --allow --service-account $BIL --operations write,describe --topic billing. --prefix
# analytics: read PII-free topics + own consumer groups; NOT the subscriber profile
confluent kafka acl create --allow --service-account $ANA --operations read,describe --topic billing. --prefix
confluent kafka acl create --allow --service-account $ANA --operations read,describe --topic network.cell-load. --prefix
confluent kafka acl create --allow --service-account $ANA --operations read --consumer-group analytics- --prefix
# network team: read/write its domain
confluent kafka acl create --allow --service-account $CEL --operations read,write,describe --topic network. --prefix
confluent kafka acl create --allow --service-account $CEL --operations read --consumer-group cells- --prefix
confluent kafka acl list
```

Keys and tests:

```bash
for s in $BIL $ANA $CEL; do confluent api-key create --resource $CC_BASIC --service-account $s -o json > /tmp/key-$s.json; done
sleep 60
k() { echo "--api-key $(jq -r .api_key /tmp/key-$1.json) --api-secret $(jq -r .api_secret /tmp/key-$1.json)"; }

# billing.call-record.created.v1 has a registered Avro schema since module 04 ("no schema, no topic"),
# so produce Avro with that schema, not a JSON string. The SR key is sa-crash-admin's; the Kafka key under test is the team's.
# (SR flag names as in module 04 B6 [V]; if you tore module 04 down, --schema registers the schema again.)
echo 'SUB-1001601:{"recordId":"CDR-1001601","recordType":"VOICE","cellId":"CELL-0412","durationSec":184,"startTime":1762499000000,"eventTime":1762499184000}' \
  | confluent kafka topic produce billing.call-record.created.v1 $(k $BIL) --parse-key --delimiter ":" \
    --value-format avro --schema ../04-stream-governance/schemas/call-record-created.avsc \
    --schema-registry-api-key ${SR_AUTH%%:*} --schema-registry-api-secret ${SR_AUTH#*:}
echo 'X:{"x":1}'  | confluent kafka topic produce noc.cell-alarm.raised.v1 $(k $BIL) --parse-key --delimiter ":"
confluent kafka topic consume billing.call-record.created.v1 $(k $ANA) --group analytics-lab06 -b
confluent kafka topic consume customers.subscriber-profile.v1 $(k $ANA) --group analytics-lab06 -b
confluent kafka topic consume billing.call-record.created.v1 $(k $ANA) --group other-group -b
```

| Command | Expected |
|---|---|
| billing produce to `billing.` | success |
| billing produce to `noc.` | `TOPIC_AUTHORIZATION_FAILED` |
| analytics consume `billing.` with group `analytics-lab06` | record printed (as Avro bytes: Kafka authorization does not need the schema) |
| analytics consume `customers.subscriber-profile.v1` | topic authorization failed |
| analytics consume with group `other-group` | **group** authorization failed — the classic onboarding error |

API key rotation drill (two-key overlap):

```bash
confluent api-key create --resource $CC_BASIC --service-account $ANA -o json > /tmp/key-$ANA-new.json
# roll the consumer to the new key, verify, then:
confluent api-key delete $(jq -r .api_key /tmp/key-$ANA.json) --force
```

Schema Registry is environment-level, so **subject RBAC works even with a Basic cluster**:

```bash
SR_ID=$(confluent schema-registry cluster describe -o json | jq -r '.cluster // .id')
confluent iam rbac role-binding create --principal User:$ANA --role DeveloperRead \
  --environment $CC_ENV --schema-registry-cluster $SR_ID --resource "Subject:billing." --prefix
```

---

## Part 2 — RBAC, OAuth with Entra ID and audit log on `cc-std-lab06`

### 2.1 Standard cluster and RBAC prefix bindings (CLI)

```bash
confluent kafka cluster create cc-std-lab06 --cloud azure --region westeurope --type standard -o json | tee /tmp/std.json
STD=$(jq -r .id /tmp/std.json)
confluent kafka cluster use $STD
for t in billing.call-record.created.v1 network.cell-load.measured.v1 customers.subscriber-profile.v1; do confluent kafka topic create $t --partitions 3; done

confluent iam rbac role-binding create --principal User:$BIL --role DeveloperWrite --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:billing. --prefix
confluent iam rbac role-binding create --principal User:$ANA --role DeveloperRead  --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:network.cell-load. --prefix
confluent iam rbac role-binding create --principal User:$ANA --role DeveloperRead  --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Group:analytics- --prefix
confluent iam rbac role-binding create --principal User:$CEL --role ResourceOwner  --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:network. --prefix
confluent iam rbac role-binding list --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD -o json | jq '.[] | {principal, role, resource_type, name, pattern_type}'
```

Keys for `$STD` and the tests that apply here:

```bash
for s in $BIL $ANA $CEL; do confluent api-key create --resource $STD --service-account $s -o json > /tmp/key-std-$s.json; done
sleep 60
ks() { echo "--api-key $(jq -r .api_key /tmp/key-std-$1.json) --api-secret $(jq -r .api_secret /tmp/key-std-$1.json)"; }

echo 'CELL-0412:{"loadPercent":7}' | confluent kafka topic produce network.cell-load.measured.v1 $(ks $CEL) --parse-key --delimiter ":"
confluent kafka topic consume network.cell-load.measured.v1 $(ks $ANA) --group analytics-lab06 -b
confluent kafka topic consume customers.subscriber-profile.v1 $(ks $ANA) --group analytics-lab06 -b
confluent kafka topic consume network.cell-load.measured.v1 $(ks $ANA) --group other-group -b
```

Expected: bindings listed with `pattern_type: PREFIXED`; the cells record is produced and the analytics consumer prints it; the PII topic gives topic authorization failed; `other-group` gives group authorization failed. Same pattern as Part 1, now from RBAC. On `$STD` analytics reads only `network.cell-load.` and there is no `noc.` topic, so Part 1's billing tests do not apply.

### 2.2 Entra ID: resource app, app role, client apps (CLI)

```bash
# Resource app = "Kafka" API, v2 tokens, one app role for applications
ROLE_ID=$(uuidgen | tr A-Z a-z)
cat > /tmp/roles.json <<EOF
[{"allowedMemberTypes":["Application"],"description":"Read cell-load topics (PII-free)","displayName":"Kafka.Cells.Read",
  "id":"$ROLE_ID","isEnabled":true,"value":"Kafka.Cells.Read"}]
EOF
API_APPID=$(az ad app create --display-name crash-kafka-api --identifier-uris api://crash-kafka-api --app-roles @/tmp/roles.json --query appId -o tsv)
# If your tenant's app management policy rejects a non-standard Application ID URI [V],
# create without --identifier-uris, then: az ad app update --id $API_APPID --identifier-uris api://$API_APPID
# and use scope api://$API_APPID/.default everywhere below.
API_OBJ=$(az ad app show --id $API_APPID --query id -o tsv)
az rest --method PATCH --uri https://graph.microsoft.com/v1.0/applications/$API_OBJ \
  --body '{"api":{"requestedAccessTokenVersion":2}}'
API_SP=$(az ad sp create --id $API_APPID --query id -o tsv)

# Client app = analytics Databricks workload
ANA_APPID=$(az ad app create --display-name crash-analytics-databricks --query appId -o tsv)
ANA_SP=$(az ad sp create --id $ANA_APPID --query id -o tsv)
ANA_SECRET=$(az ad app credential reset --id $ANA_APPID --display-name lab06 --years 1 --query password -o tsv)

# Assign the app role DIRECTLY to the workload's service principal
az rest --method POST --uri https://graph.microsoft.com/v1.0/servicePrincipals/$ANA_SP/appRoleAssignments \
  --body "{\"principalId\":\"$ANA_SP\",\"resourceId\":\"$API_SP\",\"appRoleId\":\"$ROLE_ID\"}"
```

Get a token and read it:

```bash
TOKEN=$(curl -s -X POST https://login.microsoftonline.com/$TENANT/oauth2/v2.0/token \
  -d client_id=$ANA_APPID -d client_secret=$ANA_SECRET \
  -d scope=api://crash-kafka-api/.default -d grant_type=client_credentials | jq -r .access_token)
jwt() { python3 -c 'import sys,json,base64;p=sys.argv[1].split(".")[1];p+="="*(-len(p)%4);print(json.dumps(json.loads(base64.urlsafe_b64decode(p))))' "$1"; }
jwt $TOKEN | jq '{iss, aud, sub, azp, roles, ver, exp}'
```

Expected: `iss` = `https://login.microsoftonline.com/<tenant>/v2.0`, `ver` = `2.0`, `aud` = `$API_APPID` (a GUID), `roles` = `["Kafka.Cells.Read"]`, `sub` = `$ANA_SP`. If `iss` starts with `https://sts.windows.net/`, the v2 setting did not apply — wait a minute and retry; this is the pitfall from notes §2.4.

### 2.3 Confluent: identity provider, pool, role bindings (CLI)

```bash
confluent iam provider create entra-crash --description "Entra ID tenant, v2 tokens" \
  --issuer-uri https://login.microsoftonline.com/$TENANT/v2.0 \
  --jwks-uri https://login.microsoftonline.com/$TENANT/discovery/v2.0/keys -o json | tee /tmp/idp.json
IDP=$(jq -r .id /tmp/idp.json)

confluent iam pool create pool-cells-read --provider $IDP --identity-claim claims.sub \
  --description "workloads with app role Kafka.Cells.Read" \
  --filter "claims.aud == \"$API_APPID\" && \"Kafka.Cells.Read\" in claims.roles" -o json | tee /tmp/pool.json
POOL=$(jq -r .id /tmp/pool.json)

confluent iam rbac role-binding create --principal User:$POOL --role DeveloperRead --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:network.cell-load. --prefix
confluent iam rbac role-binding create --principal User:$POOL --role DeveloperRead --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Group:analytics- --prefix
```

(Flag names `--issuer-uri` / `--jwks-uri` per CLI v4 **[V]**; `confluent iam provider create --help` to confirm.)

### 2.4 Produce and consume with OAuth (Kafka 4.x Java tools)

```bash
BOOT=$(confluent kafka cluster describe $STD -o json | jq -r .endpoint | sed 's#SASL_SSL://##')
cat > /tmp/oauth.properties <<EOF
bootstrap.servers=$BOOT
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
sasl.oauthbearer.token.endpoint.url=https://login.microsoftonline.com/$TENANT/oauth2/v2.0/token
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId='$ANA_APPID' clientSecret='$ANA_SECRET' scope='api://crash-kafka-api/.default' extension_logicalCluster='$STD' extension_identityPoolId='$POOL';
EOF
export KAFKA_OPTS="-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://login.microsoftonline.com/$TENANT/oauth2/v2.0/token"

# seed one record with the cells service account key
echo 'CELL-0412:{"loadPercent":7}' | confluent kafka topic produce network.cell-load.measured.v1 \
  $(ks $CEL) --parse-key --delimiter ":"

kafka-console-consumer.sh --bootstrap-server $BOOT --consumer.config /tmp/oauth.properties \
  --topic network.cell-load.measured.v1 --group analytics-oauth --from-beginning --max-messages 1
kafka-console-consumer.sh --bootstrap-server $BOOT --consumer.config /tmp/oauth.properties \
  --topic customers.subscriber-profile.v1 --group analytics-oauth --from-beginning --max-messages 1 --timeout-ms 15000
echo 'x:1' | kafka-console-producer.sh --bootstrap-server $BOOT --producer.config /tmp/oauth.properties \
  --topic network.cell-load.measured.v1 --reader-property parse.key=true --reader-property key.separator=:
```

| Test | Expected |
|---|---|
| consume `network.cell-load.measured.v1` | the record — authenticated as `pool-…`, authorised by prefix |
| consume `customers.subscriber-profile.v1` | `TopicAuthorizationException` |
| produce | authorization failed (read-only pool) |
| run without `KAFKA_OPTS` | client refuses the token endpoint URL — the Kafka 4.x allowed-URLs check |
| remove `extension_identityPoolId` from the JAAS line | still works — **auto pool mapping** |

### 2.5 Reproduce the client-credentials group pitfall

```bash
# Remove the direct assignment
ASSIGN_ID=$(az rest --method GET --uri https://graph.microsoft.com/v1.0/servicePrincipals/$ANA_SP/appRoleAssignments --query "value[0].id" -o tsv)
az rest --method DELETE --uri https://graph.microsoft.com/v1.0/servicePrincipals/$ANA_SP/appRoleAssignments/$ASSIGN_ID

# Put the SP in a security group and assign the app role to the group instead
GRP=$(az ad group create --display-name crash-kafka-cells-readers --mail-nickname crash-kafka-cells-readers --query id -o tsv)
az ad group member add --group $GRP --member-id $ANA_SP
az rest --method POST --uri https://graph.microsoft.com/v1.0/groups/$GRP/appRoleAssignments \
  --body "{\"principalId\":\"$GRP\",\"resourceId\":\"$API_SP\",\"appRoleId\":\"$ROLE_ID\"}"
sleep 60
# New token
TOKEN=$(curl -s -X POST https://login.microsoftonline.com/$TENANT/oauth2/v2.0/token \
  -d client_id=$ANA_APPID -d client_secret=$ANA_SECRET -d scope=api://crash-kafka-api/.default -d grant_type=client_credentials | jq -r .access_token)
jwt $TOKEN | jq '{roles}'
```

Expected: `roles` is **missing**. The consumer from 2.4 now fails authentication (no pool matches). Put the direct assignment back (2.2 last command) and it works again. You have now seen the pitfall, not just read it.

### 2.6 Audit log

```bash
confluent audit-log describe -o json | tee /tmp/audit.json
AUD_ENV=$(jq -r .environment_id /tmp/audit.json); AUD_CL=$(jq -r .cluster_id /tmp/audit.json); AUD_SA=$(jq -r .service_account_id /tmp/audit.json)
confluent environment use $AUD_ENV
confluent kafka cluster use $AUD_CL
confluent api-key list --resource $AUD_CL          # max 2 keys on this cluster
confluent api-key create --resource $AUD_CL --service-account $AUD_SA -o json > /tmp/audit-key.json
confluent api-key use $(jq -r .api_key /tmp/audit-key.json) --resource $AUD_CL
sleep 60
confluent kafka topic consume confluent-audit-log-events -b > /tmp/audit.jsonl &
sleep 90; kill %1
```

Look for your own events (field paths per CloudEvents format **[V]**):

```bash
# denied authorizations in the last run
jq -c 'select(.data.authorizationInfo.granted == false) | {time, method: .data.methodName, principal: .data.authenticationInfo.principal, resource: .data.resourceName}' /tmp/audit.jsonl | tail -20
# authentications via the identity pool
jq -c 'select(.data.authenticationInfo.principal | tostring | test("pool-")) | {time, principal: .data.authenticationInfo, method: .data.methodName}' /tmp/audit.jsonl | tail -5
# role binding / API key changes (organisation events)
jq -c 'select(.data.methodName | test("RoleBinding|ApiKey"; "i")) | {time, method: .data.methodName, principal: .data.authenticationInfo.principal}' /tmp/audit.jsonl | tail -10
```

Expected: denied `kafka.Fetch`/`kafka.Produce`-style entries for the analytics identity on `customers.subscriber-profile.v1`; authentication entries showing `pool-…` and the `sub`; role-binding creations from 2.1/2.3. Events can take a few minutes to appear. Switch back: `confluent environment use $CC_ENV`.

Note what is **not** there (for example every allowed fetch) and write it down — that is the answer to "is the audit log enough for NIS2?".

### 2.7 Terraform for Part 2 (same end state)

```hcl
terraform {
  required_version = ">= 1.9"
  required_providers {
    confluent = { source = "confluentinc/confluent", version = "~> 2.94" }
    azuread   = { source = "hashicorp/azuread",      version = "~> 3.10" }
    random    = { source = "hashicorp/random",       version = "~> 3.6" }
  }
}
provider "confluent" {}   # Cloud API key with OrganizationAdmin (identity provider is org-level)
provider "azuread" {}

variable "env_id" { type = string }
data "azuread_client_config" "me" {}
data "confluent_environment" "env" { id = var.env_id }

# ---------- Entra ----------
resource "random_uuid" "role_cells_read" {}

resource "azuread_application" "kafka_api" {
  display_name    = "crash-kafka-api"
  identifier_uris = ["api://crash-kafka-api"]
  owners          = [data.azuread_client_config.me.object_id]
  api { requested_access_token_version = 2 }          # v2 tokens, or issuer mismatch
  app_role {
    allowed_member_types = ["Application"]
    description          = "Read cell-load topics (PII-free)"
    display_name         = "Kafka.Cells.Read"
    id                   = random_uuid.role_cells_read.result
    enabled              = true
    value                = "Kafka.Cells.Read"
  }
}
resource "azuread_service_principal" "kafka_api" {
  client_id = azuread_application.kafka_api.client_id
}

resource "azuread_application" "analytics" {
  display_name = "crash-analytics-databricks"
  owners       = [data.azuread_client_config.me.object_id]
}
resource "azuread_service_principal" "analytics" {
  client_id = azuread_application.analytics.client_id
}
resource "azuread_application_password" "analytics" {
  application_id = azuread_application.analytics.id
}
# Direct assignment to the SP — group assignments are not evaluated for client credentials
resource "azuread_app_role_assignment" "analytics_cells_read" {
  app_role_id         = random_uuid.role_cells_read.result
  principal_object_id = azuread_service_principal.analytics.object_id
  resource_object_id  = azuread_service_principal.kafka_api.object_id
}

# ---------- Confluent ----------
resource "confluent_kafka_cluster" "std" {
  display_name = "cc-std-lab06"
  availability = "SINGLE_ZONE"
  cloud        = "AZURE"
  region       = "westeurope"
  standard {}
  environment { id = var.env_id }
}

resource "confluent_service_account" "team" {
  for_each     = toset(["billing-producer", "analytics-consumer", "cells-app"])
  display_name = "sa-${each.key}-tf"
  description  = "lab06 team account (terraform)"
}

locals {
  kafka_crn = "${confluent_kafka_cluster.std.rbac_crn}/kafka=${confluent_kafka_cluster.std.id}"
}

resource "confluent_role_binding" "billing_write" {
  principal   = "User:${confluent_service_account.team["billing-producer"].id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${local.kafka_crn}/topic=billing.*"
}
resource "confluent_role_binding" "analytics_read_topic" {
  principal   = "User:${confluent_service_account.team["analytics-consumer"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/topic=network.cell-load.*"
}
resource "confluent_role_binding" "analytics_read_group" {
  principal   = "User:${confluent_service_account.team["analytics-consumer"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/group=analytics-*"
}
resource "confluent_role_binding" "cells_owner" {
  principal   = "User:${confluent_service_account.team["cells-app"].id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${local.kafka_crn}/topic=network.*"
}

resource "confluent_identity_provider" "entra" {
  display_name = "entra-crash"
  description  = "Entra ID tenant, v2 tokens"
  issuer       = "https://login.microsoftonline.com/${data.azuread_client_config.me.tenant_id}/v2.0"
  jwks_uri     = "https://login.microsoftonline.com/${data.azuread_client_config.me.tenant_id}/discovery/v2.0/keys"
}

resource "confluent_identity_pool" "cells_read" {
  identity_provider { id = confluent_identity_provider.entra.id }
  display_name   = "pool-cells-read"
  description    = "workloads with app role Kafka.Cells.Read"
  identity_claim = "claims.sub"
  filter         = "claims.aud == \"${azuread_application.kafka_api.client_id}\" && \"Kafka.Cells.Read\" in claims.roles"
}

resource "confluent_role_binding" "pool_read_topic" {
  principal   = "User:${confluent_identity_pool.cells_read.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/topic=network.cell-load.*"
}
resource "confluent_role_binding" "pool_read_group" {
  principal   = "User:${confluent_identity_pool.cells_read.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/group=analytics-*"
}

output "cluster_id"   { value = confluent_kafka_cluster.std.id }
output "bootstrap"    { value = confluent_kafka_cluster.std.bootstrap_endpoint }
output "pool_id"      { value = confluent_identity_pool.cells_read.id }
output "analytics_client_id" { value = azuread_application.analytics.client_id }
output "analytics_secret" {
  value     = azuread_application_password.analytics.value
  sensitive = true
}
output "token_scope"  { value = "api://crash-kafka-api/.default" }
```

This block passes `terraform validate` with confluent 2.94.0 and azuread 3.10.0 (checked 2026-11-07); `apply` has not been run.

Topics: add `confluent_kafka_topic` resources with an admin API key (pattern as in module 05), or create them with the CLI. The audit-log key is not worth Terraform for a lab.

### Teardown Part 2

```bash
for s in $BIL $ANA $CEL; do confluent api-key delete $(jq -r .api_key /tmp/key-std-$s.json) --force; done
confluent kafka cluster delete $STD --force
confluent iam pool delete $POOL --provider $IDP --force
confluent iam provider delete $IDP --force
confluent api-key delete $(jq -r .api_key /tmp/audit-key.json) --force     # frees one of the two audit keys
az ad group delete --group $GRP
az ad app delete --id $ANA_APPID
az ad app delete --id $API_APPID
```

Terraform path: `terraform destroy`.

---

## Part 3 — Client quota in Confluent Cloud (`paid-session`)

Quotas need Enterprise, Freight or Dedicated. This part reuses module 03 Part C on the same day, so these must still exist:

- the Enterprise cluster `cc-ent-lab03` with its gateway, access point and private DNS zone;
- the VM `vm-lab03` with the Kafka 4.x tools, the Confluent CLI and `cc-ent.properties` (module 03 C1 step 8; the Terraform path ends with the same step).

Run everything below **on the VM** (`ssh azureuser@$VM_IP`), logged in with `confluent login` as your own user (OrganizationAdmin: it creates a service account).

```bash
export PATH=$PATH:$HOME/kafka_2.13-4.4.0/bin        # the Kafka version from module 03 step 8
CC_ENV=<env id of crash-course>                      # confluent environment list
confluent environment use $CC_ENV
ENT=<lkc of cc-ent-lab03>                            # confluent kafka cluster list
confluent kafka cluster describe $ENT -o json | jq '.endpoint, .endpoints'
BOOT_PL=<access-point bootstrap from above, without SASL_SSL:// and :9092>     # as in module 03 steps 6-7

# the noc. topic on cc-ent-lab03 (module 03 step 8 created it; this is a no-op then)
kafka-topics.sh --bootstrap-server $BOOT_PL:9092 --command-config cc-ent.properties \
  --create --if-not-exists --topic noc.cell-alarm.raised.v1 --partitions 3

confluent iam service-account create sa-analytics-replay --description "lab06 quota test" -o json > sa-replay.json
REP=$(jq -r .id sa-replay.json)
confluent iam rbac role-binding create --principal User:$REP --role DeveloperRead  --environment $CC_ENV --cloud-cluster $ENT --kafka-cluster $ENT --resource Topic:noc. --prefix
confluent iam rbac role-binding create --principal User:$REP --role DeveloperWrite --environment $CC_ENV --cloud-cluster $ENT --kafka-cluster $ENT --resource Topic:noc. --prefix
confluent iam rbac role-binding create --principal User:$REP --role DeveloperRead  --environment $CC_ENV --cloud-cluster $ENT --kafka-cluster $ENT --resource Group:analytics- --prefix
confluent api-key create --resource $ENT --service-account $REP -o json > key-replay.json
# client properties as cc-ent.properties in module 03 C1 step 8, with this key
cat > replay.properties <<EOF
bootstrap.servers=$BOOT_PL:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='$(jq -r .api_key key-replay.json)' password='$(jq -r .api_secret key-replay.json)';
EOF
sleep 60

# baseline
kafka-producer-perf-test.sh --topic noc.cell-alarm.raised.v1 --num-records 50000 --record-size 1000 \
  --throughput -1 --producer.config replay.properties

# 1 MB/s ingress and egress for this principal
confluent kafka quota create --name lab06-analytics-replay --ingress 1048576 --egress 1048576 --principals $REP --cluster $ENT   # flags [V]
confluent kafka quota list --cluster $ENT
sleep 60
kafka-producer-perf-test.sh --topic noc.cell-alarm.raised.v1 --num-records 50000 --record-size 1000 \
  --throughput -1 --producer.config replay.properties
kafka-consumer-perf-test.sh --bootstrap-server $BOOT_PL:9092 --consumer.config replay.properties \
  --topic noc.cell-alarm.raised.v1 --group analytics-replay --messages 50000
```

Expected: baseline several MB/s; after the quota about **1 MB/s** (≈1,000 records/s) for both tools, with latency going up — that is throttling, not errors. Add a default quota for everyone else:

```bash
confluent kafka quota create --name lab06-default --ingress 5242880 --egress 5242880 --principals "<default>" --cluster $ENT
```

Terraform equivalent:

```hcl
resource "confluent_kafka_client_quota" "analytics_replay" {
  display_name = "lab06-analytics-replay"
  description  = "cap analytics backfills"
  throughput {
    ingress_byte_rate = "1048576"
    egress_byte_rate  = "1048576"
  }
  principals    = [confluent_service_account.replay.id]      # or an identity pool id
  kafka_cluster { id = var.enterprise_cluster_id }
  environment   { id = var.env_id }
}
```

Teardown: `confluent kafka quota delete <id>` for both, `confluent iam service-account delete $REP --force` — then the module 03 teardown removes the cluster.

---

## Part 4 — Quotas on Confluent Platform (`local`, `dc-a`)

Shows what CC does not have: request-rate quotas, and quotas by client id.

```bash
source ~/labs/confluent-crash-course/00-lab-setup/lab-env.sh     # a, b, sra, srb, LAB
a kafka-topics --bootstrap-server dc-a:29092 --create --topic noc.cell-alarm.raised.v1 --partitions 3 --replication-factor 1
a kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 \
  --num-records 100000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=replay-job

a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --add-config 'producer_byte_rate=1048576,consumer_byte_rate=1048576,request_percentage=25' \
  --entity-type clients --entity-name replay-job
a kafka-configs --bootstrap-server dc-a:29092 --describe --entity-type clients --entity-name replay-job

a kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 \
  --num-records 20000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=replay-job
a kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 \
  --num-records 20000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092 client.id=alarm-processor
```

Expected: `replay-job` drops to about 1 MB/s; `alarm-processor` (no quota) keeps full speed — the isolation story in one picture.

In production you quota by **user** (authenticated principal), not by client id, because any client can choose its client id. On the real platform the most important on-prem quota is on the Cluster Link principal (`notes-cp-security.md` §2.6).

Remove the quota now. Module 07 runs the same experiment, and its "no quota" baseline must not be throttled:

```bash
a kafka-configs --bootstrap-server dc-a:29092 --alter \
  --delete-config 'producer_byte_rate,consumer_byte_rate,request_percentage' \
  --entity-type clients --entity-name replay-job
a kafka-configs --bootstrap-server dc-a:29092 --describe --entity-type clients --entity-name replay-job   # no quota configs left
```

**CP-side OAuth/mTLS/MDS** is not built in this lab (the `local` stack runs without auth). Read `notes-cp-security.md` §2.3–2.4 and be able to write the broker listener block from memory.

---

## Verification checklist

- [ ] Part 1: denied produce to another domain; denied consume without the group ACL.
- [ ] Part 2: prefix RBAC bindings listed; OAuth token has v2 issuer and `roles`; OAuth consume works, PII topic and produce denied; group-assignment pitfall reproduced; audit log shows denials and `pool-…` authentications.
- [ ] Part 3: throughput capped at ~1 MB/s for the quota principal.
- [ ] Part 4: `replay-job` throttled, `alarm-processor` not; quota removed afterwards.

## Teardown summary

| Part | Remove |
|---|---|
| 1 | `confluent api-key delete` for the three team keys; ACLs (`confluent kafka acl delete …`) or keep them on `cc-basic` for later modules; service accounts if not reused |
| 2 | team keys for `$STD`, Standard cluster, identity pool, identity provider, audit key, Entra apps and group (see 2.7 teardown) |
| 3 | quotas, `sa-analytics-replay` (cluster goes with module 03 teardown) |
| 4 | client quota config on `dc-a` (already removed at the end of Part 4) |

## Cost note

| Item | Rough cost **[V]** |
|---|---|
| Part 1 on Basic | free |
| Part 2 Standard cluster, ~2 h | 0.75 USD/eCKU-h list → about 1–2 USD |
| Part 3 | inside the module 03 paid-session budget |
| Entra app registrations, groups | free |
