---
title: 05 — Lab — team service accounts, prefix RBAC, OAuth with Entra ID, audit log, quotas
subtitle: Part 1 on cc-basic (ACLs, because Basic has no Kafka RBAC), Part 2 on a temporary Standard cluster (RBAC, OAuth, audit log), Part 3 quota in the paid-session, Part 4 quotas on local CP
---

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
| RBAC on topics / groups | **no** | yes | yes | Part 2, `cc-std-lab05` |
| OAuth for Kafka | **no** | yes | yes | Part 2 |
| Audit logs | **no** | yes | yes | Part 2 |
| Client quotas | **no** | **no** | yes | Part 3, `paid-session` Enterprise cluster (module 02) |
| Quotas incl. `request_percentage` | — | — | — | Part 4, `local` `dc-a` |

`cc-std-lab05` = a **Standard** cluster on Azure `westeurope` in environment `crash-course`, created at the start of Part 2 and deleted at the end. List price is 0.75 USD per eCKU-hour and Standard scales to zero when idle **[V]**, so 2 hours cost about 1–2 USD. This is an addition to the agreed environments; it is the cheapest way to test RBAC, OAuth and audit logs. If you prefer, run Part 2 against the paid-session Enterprise cluster from the VM instead (Enterprise supports all of it) — then skip the Standard cluster.

## Prerequisites

- `confluent` CLI v4.x as OrganizationAdmin (identity providers and audit-log keys need it).
- `az` CLI with rights to create app registrations in your Entra tenant (Application Developer or higher) and to grant app role assignments (Cloud Application Administrator or the app owner).
- Kafka 4.x CLI tools (or CP 8.x) and Java 17+; `jq`, `curl`, `base64`.
- Topics used: `orders.order-status.updated.v1`, `tracking.item-scan.recorded.v1`, `sites.queue-length.measured.v1` (PII-free), `customers.customer-journey.v1` (PII).

```bash
cd ../00-lab-setup/terraform
export CC_ENV=$(terraform output -raw environment_id)
export CC_BASIC=$(terraform output -raw basic_cluster_id)
cd -
export TENANT=$(az account show --query tenantId -o tsv)
confluent environment use $CC_ENV
```

---

## Part 1 — Team service accounts and ACLs on `cc-basic` (CLI)

```bash
confluent kafka cluster use $CC_BASIC
for t in orders.order-status.updated.v1 tracking.item-scan.recorded.v1 sites.queue-length.measured.v1 customers.customer-journey.v1; do
  confluent kafka topic create $t --partitions 3 2>/dev/null || true
done

for sa in sa-orders-producer sa-analytics-consumer sa-sites-app; do
  confluent iam service-account create $sa --description "lab05 team account" -o json > /tmp/$sa.json
done
ORD=$(jq -r .id /tmp/sa-orders-producer.json)
ANA=$(jq -r .id /tmp/sa-analytics-consumer.json)
SIT=$(jq -r .id /tmp/sa-sites-app.json)
```

ACLs by prefix (Basic has no Kafka RBAC):

```bash
# orders team: write its own domain only
confluent kafka acl create --allow --service-account $ORD --operations write,describe --topic orders. --prefix
# analytics: read PII-free topics + own consumer groups; NOT the customer journey
confluent kafka acl create --allow --service-account $ANA --operations read,describe --topic orders. --prefix
confluent kafka acl create --allow --service-account $ANA --operations read,describe --topic sites.queue-length. --prefix
confluent kafka acl create --allow --service-account $ANA --operations read --consumer-group analytics- --prefix
# sites team: read/write its domain
confluent kafka acl create --allow --service-account $SIT --operations read,write,describe --topic sites. --prefix
confluent kafka acl create --allow --service-account $SIT --operations read --consumer-group sites- --prefix
confluent kafka acl list
```

Keys and tests:

```bash
for s in $ORD $ANA $SIT; do confluent api-key create --resource $CC_BASIC --service-account $s -o json > /tmp/key-$s.json; done
sleep 60
k() { echo "--api-key $(jq -r .api_key /tmp/key-$1.json) --api-secret $(jq -r .api_secret /tmp/key-$1.json)"; }

echo 'SO-1001234:{"status":"READY"}' | confluent kafka topic produce orders.order-status.updated.v1 $(k $ORD) --parse-key --delimiter ":"
echo 'X:{"x":1}'  | confluent kafka topic produce tracking.item-scan.recorded.v1 $(k $ORD) --parse-key --delimiter ":"
confluent kafka topic consume orders.order-status.updated.v1 $(k $ANA) --group analytics-lab05 -b
confluent kafka topic consume customers.customer-journey.v1 $(k $ANA) --group analytics-lab05 -b
confluent kafka topic consume orders.order-status.updated.v1 $(k $ANA) --group other-group -b
```

| Command | Expected |
|---|---|
| orders produce to `orders.` | success |
| orders produce to `tracking.` | `TOPIC_AUTHORIZATION_FAILED` |
| analytics consume `orders.` with group `analytics-lab05` | record printed |
| analytics consume `customers.customer-journey.v1` | topic authorization failed |
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
  --environment $CC_ENV --schema-registry-cluster $SR_ID --resource "Subject:orders." --prefix
```

---

## Part 2 — RBAC, OAuth with Entra ID and audit log on `cc-std-lab05`

### 2.1 Standard cluster and RBAC prefix bindings (CLI)

```bash
confluent kafka cluster create cc-std-lab05 --cloud azure --region westeurope --type standard -o json | tee /tmp/std.json
STD=$(jq -r .id /tmp/std.json)
confluent kafka cluster use $STD
for t in orders.order-status.updated.v1 sites.queue-length.measured.v1 customers.customer-journey.v1; do confluent kafka topic create $t --partitions 3; done

confluent iam rbac role-binding create --principal User:$ORD --role DeveloperWrite --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:orders. --prefix
confluent iam rbac role-binding create --principal User:$ANA --role DeveloperRead  --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:sites.queue-length. --prefix
confluent iam rbac role-binding create --principal User:$ANA --role DeveloperRead  --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Group:analytics- --prefix
confluent iam rbac role-binding create --principal User:$SIT --role ResourceOwner  --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:sites. --prefix
confluent iam rbac role-binding list --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD -o json | jq '.[] | {principal, role, resource_type, name, pattern_type}'
```

Expected: bindings listed with `pattern_type: PREFIXED`. Repeat the tests from Part 1 with keys for `$STD`: same results, now with RBAC instead of ACLs.

### 2.2 Entra ID: resource app, app role, client apps (CLI)

```bash
# Resource app = "Kafka" API, v2 tokens, one app role for applications
ROLE_ID=$(uuidgen | tr A-Z a-z)
cat > /tmp/roles.json <<EOF
[{"allowedMemberTypes":["Application"],"description":"Read sites topics (PII-free)","displayName":"Kafka.Sites.Read",
  "id":"$ROLE_ID","isEnabled":true,"value":"Kafka.Sites.Read"}]
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
ANA_SECRET=$(az ad app credential reset --id $ANA_APPID --display-name lab05 --years 1 --query password -o tsv)

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

Expected: `iss` = `https://login.microsoftonline.com/<tenant>/v2.0`, `ver` = `2.0`, `aud` = `$API_APPID` (a GUID), `roles` = `["Kafka.Sites.Read"]`, `sub` = `$ANA_SP`. If `iss` starts with `https://sts.windows.net/`, the v2 setting did not apply — wait a minute and retry; this is the pitfall from notes §2.4.

### 2.3 Confluent: identity provider, pool, role bindings (CLI)

```bash
confluent iam provider create entra-crash --description "Entra ID tenant, v2 tokens" \
  --issuer-uri https://login.microsoftonline.com/$TENANT/v2.0 \
  --jwks-uri https://login.microsoftonline.com/$TENANT/discovery/v2.0/keys -o json | tee /tmp/idp.json
IDP=$(jq -r .id /tmp/idp.json)

confluent iam pool create pool-sites-read --provider $IDP --identity-claim claims.sub \
  --description "workloads with app role Kafka.Sites.Read" \
  --filter "claims.aud == \"$API_APPID\" && \"Kafka.Sites.Read\" in claims.roles" -o json | tee /tmp/pool.json
POOL=$(jq -r .id /tmp/pool.json)

confluent iam rbac role-binding create --principal User:$POOL --role DeveloperRead --environment $CC_ENV --cloud-cluster $STD --kafka-cluster $STD --resource Topic:sites.queue-length. --prefix
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

# seed one record with the sites service account key
echo 'ZONE-A2:{"queue_minutes":7}' | confluent kafka topic produce sites.queue-length.measured.v1 \
  --api-key <SIT key for STD> --api-secret <secret> --parse-key --delimiter ":"

kafka-console-consumer.sh --bootstrap-server $BOOT --consumer.config /tmp/oauth.properties \
  --topic sites.queue-length.measured.v1 --group analytics-oauth --from-beginning --max-messages 1
kafka-console-consumer.sh --bootstrap-server $BOOT --consumer.config /tmp/oauth.properties \
  --topic customers.customer-journey.v1 --group analytics-oauth --from-beginning --max-messages 1 --timeout-ms 15000
echo 'x:1' | kafka-console-producer.sh --bootstrap-server $BOOT --producer.config /tmp/oauth.properties \
  --topic sites.queue-length.measured.v1 --reader-property parse.key=true --reader-property key.separator=:
```

| Test | Expected |
|---|---|
| consume `sites.queue-length.measured.v1` | the record — authenticated as `pool-…`, authorised by prefix |
| consume `customers.customer-journey.v1` | `TopicAuthorizationException` |
| produce | authorization failed (read-only pool) |
| run without `KAFKA_OPTS` | client refuses the token endpoint URL — the Kafka 4.x allowed-URLs check |
| remove `extension_identityPoolId` from the JAAS line | still works — **auto pool mapping** |

### 2.5 Reproduce the client-credentials group pitfall

```bash
# Remove the direct assignment
ASSIGN_ID=$(az rest --method GET --uri https://graph.microsoft.com/v1.0/servicePrincipals/$ANA_SP/appRoleAssignments --query "value[0].id" -o tsv)
az rest --method DELETE --uri https://graph.microsoft.com/v1.0/servicePrincipals/$ANA_SP/appRoleAssignments/$ASSIGN_ID

# Put the SP in a security group and assign the app role to the group instead
GRP=$(az ad group create --display-name crash-kafka-sites-readers --mail-nickname crash-kafka-sites-readers --query id -o tsv)
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

Expected: denied `kafka.Fetch`/`kafka.Produce`-style entries for the analytics identity on `customers.customer-journey.v1`; authentication entries showing `pool-…` and the `sub`; role-binding creations from 2.1/2.3. Events can take a few minutes to appear. Switch back: `confluent environment use $CC_ENV`.

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
resource "random_uuid" "role_sites_read" {}

resource "azuread_application" "kafka_api" {
  display_name    = "crash-kafka-api"
  identifier_uris = ["api://crash-kafka-api"]
  owners          = [data.azuread_client_config.me.object_id]
  api { requested_access_token_version = 2 }          # v2 tokens, or issuer mismatch
  app_role {
    allowed_member_types = ["Application"]
    description          = "Read sites topics (PII-free)"
    display_name         = "Kafka.Sites.Read"
    id                   = random_uuid.role_sites_read.result
    enabled              = true
    value                = "Kafka.Sites.Read"
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
resource "azuread_app_role_assignment" "analytics_sites_read" {
  app_role_id         = random_uuid.role_sites_read.result
  principal_object_id = azuread_service_principal.analytics.object_id
  resource_object_id  = azuread_service_principal.kafka_api.object_id
}

# ---------- Confluent ----------
resource "confluent_kafka_cluster" "std" {
  display_name = "cc-std-lab05"
  availability = "SINGLE_ZONE"
  cloud        = "AZURE"
  region       = "westeurope"
  standard {}
  environment { id = var.env_id }
}

resource "confluent_service_account" "team" {
  for_each     = toset(["orders-producer", "analytics-consumer", "sites-app"])
  display_name = "sa-${each.key}-tf"
  description  = "lab05 team account (terraform)"
}

locals {
  kafka_crn = "${confluent_kafka_cluster.std.rbac_crn}/kafka=${confluent_kafka_cluster.std.id}"
}

resource "confluent_role_binding" "orders_write" {
  principal   = "User:${confluent_service_account.team["orders-producer"].id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${local.kafka_crn}/topic=orders.*"
}
resource "confluent_role_binding" "analytics_read_topic" {
  principal   = "User:${confluent_service_account.team["analytics-consumer"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/topic=sites.queue-length.*"
}
resource "confluent_role_binding" "analytics_read_group" {
  principal   = "User:${confluent_service_account.team["analytics-consumer"].id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/group=analytics-*"
}
resource "confluent_role_binding" "sites_owner" {
  principal   = "User:${confluent_service_account.team["sites-app"].id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${local.kafka_crn}/topic=sites.*"
}

resource "confluent_identity_provider" "entra" {
  display_name = "entra-crash"
  description  = "Entra ID tenant, v2 tokens"
  issuer       = "https://login.microsoftonline.com/${data.azuread_client_config.me.tenant_id}/v2.0"
  jwks_uri     = "https://login.microsoftonline.com/${data.azuread_client_config.me.tenant_id}/discovery/v2.0/keys"
}

resource "confluent_identity_pool" "sites_read" {
  identity_provider { id = confluent_identity_provider.entra.id }
  display_name   = "pool-sites-read"
  description    = "workloads with app role Kafka.Sites.Read"
  identity_claim = "claims.sub"
  filter         = "claims.aud == \"${azuread_application.kafka_api.client_id}\" && \"Kafka.Sites.Read\" in claims.roles"
}

resource "confluent_role_binding" "pool_read_topic" {
  principal   = "User:${confluent_identity_pool.sites_read.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/topic=sites.queue-length.*"
}
resource "confluent_role_binding" "pool_read_group" {
  principal   = "User:${confluent_identity_pool.sites_read.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.kafka_crn}/group=analytics-*"
}

output "cluster_id"   { value = confluent_kafka_cluster.std.id }
output "bootstrap"    { value = confluent_kafka_cluster.std.bootstrap_endpoint }
output "pool_id"      { value = confluent_identity_pool.sites_read.id }
output "analytics_client_id" { value = azuread_application.analytics.client_id }
output "analytics_secret" {
  value     = azuread_application_password.analytics.value
  sensitive = true
}
output "token_scope"  { value = "api://crash-kafka-api/.default" }
```

This block passes `terraform validate` with confluent 2.94.0 and azuread 3.10.0 (checked 2026-11-07); `apply` has not been run.

Topics: add `confluent_kafka_topic` resources with an admin API key (pattern as in module 04), or create them with the CLI. The audit-log key is not worth Terraform for a lab.

### Teardown Part 2

```bash
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

Run while the module 02 Enterprise cluster exists, from the VM in the VNet. Quotas need Enterprise, Freight or Dedicated.

```bash
ENT=<lkc of cc-ent-lab02>
confluent iam service-account create sa-analytics-replay --description "lab05 quota test" -o json > sa-replay.json
REP=$(jq -r .id sa-replay.json)
confluent iam rbac role-binding create --principal User:$REP --role DeveloperRead  --environment $CC_ENV --cloud-cluster $ENT --kafka-cluster $ENT --resource Topic:tracking. --prefix
confluent iam rbac role-binding create --principal User:$REP --role DeveloperWrite --environment $CC_ENV --cloud-cluster $ENT --kafka-cluster $ENT --resource Topic:tracking. --prefix
confluent iam rbac role-binding create --principal User:$REP --role DeveloperRead  --environment $CC_ENV --cloud-cluster $ENT --kafka-cluster $ENT --resource Group:analytics- --prefix
confluent api-key create --resource $ENT --service-account $REP -o json > key-replay.json
# client properties replay.properties as in module 02 C1 step 8, with this key

# baseline
kafka-producer-perf-test.sh --topic tracking.item-scan.recorded.v1 --num-records 50000 --record-size 1000 \
  --throughput -1 --producer.config replay.properties

# 1 MB/s ingress and egress for this principal
confluent kafka quota create lab05-analytics-replay --ingress 1048576 --egress 1048576 --principals $REP --cluster $ENT
confluent kafka quota list --cluster $ENT
sleep 60
kafka-producer-perf-test.sh --topic tracking.item-scan.recorded.v1 --num-records 50000 --record-size 1000 \
  --throughput -1 --producer.config replay.properties
kafka-consumer-perf-test.sh --bootstrap-server $BOOT_PL:9092 --consumer.config replay.properties \
  --topic tracking.item-scan.recorded.v1 --group analytics-replay --messages 50000
```

Expected: baseline several MB/s; after the quota about **1 MB/s** (≈1,000 records/s) for both tools, with latency going up — that is throttling, not errors. Add a default quota for everyone else:

```bash
confluent kafka quota create lab05-default --ingress 5242880 --egress 5242880 --principals "<default>" --cluster $ENT
```

Terraform equivalent:

```hcl
resource "confluent_kafka_client_quota" "analytics_replay" {
  display_name = "lab05-analytics-replay"
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

Teardown: `confluent kafka quota delete <id>` for both, `confluent iam service-account delete $REP --force` — then the module 02 teardown removes the cluster.

---

## Part 4 — Quotas on Confluent Platform (`local`, `dc-a`)

Shows what CC does not have: request-rate quotas, and quotas by client id.

```bash
DC=../00-lab-setup/docker-compose.yml
docker compose -f $DC exec dc-a kafka-topics --bootstrap-server localhost:9092 --create --topic tracking.item-scan.recorded.v1 --partitions 3 2>/dev/null
docker compose -f $DC exec dc-a kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 \
  --num-records 100000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 client.id=replay-job

docker compose -f $DC exec dc-a kafka-configs --bootstrap-server localhost:9092 --alter \
  --add-config 'producer_byte_rate=1048576,consumer_byte_rate=1048576,request_percentage=25' \
  --entity-type clients --entity-name replay-job
docker compose -f $DC exec dc-a kafka-configs --bootstrap-server localhost:9092 --describe --entity-type clients --entity-name replay-job

docker compose -f $DC exec dc-a kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 \
  --num-records 20000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 client.id=replay-job
docker compose -f $DC exec dc-a kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 \
  --num-records 20000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 client.id=scan-processor
```

Expected: `replay-job` drops to about 1 MB/s; `scan-processor` (no quota) keeps full speed — the isolation story in one picture. (Service names `dc-a` and port 9092 depend on `00-lab-setup`; adjust.)

In production you quota by **user** (authenticated principal), not by client id, because any client can choose its client id. On the real platform the most important on-prem quota is on the Cluster Link principal (`notes-cp-security.md` §2.6).

Remove: `kafka-configs … --alter --delete-config 'producer_byte_rate,consumer_byte_rate,request_percentage' --entity-type clients --entity-name replay-job`.

**CP-side OAuth/mTLS/MDS** is not built in this lab (the `local` stack runs without auth). Read `notes-cp-security.md` §2.3–2.4 and be able to write the broker listener block from memory.

---

## Verification checklist

- [ ] Part 1: denied produce to another domain; denied consume without the group ACL.
- [ ] Part 2: prefix RBAC bindings listed; OAuth token has v2 issuer and `roles`; OAuth consume works, PII topic and produce denied; group-assignment pitfall reproduced; audit log shows denials and `pool-…` authentications.
- [ ] Part 3: throughput capped at ~1 MB/s for the quota principal.
- [ ] Part 4: `replay-job` throttled, `scan-processor` not.

## Teardown summary

| Part | Remove |
|---|---|
| 1 | `confluent api-key delete` for the three team keys; ACLs (`confluent kafka acl delete …`) or keep them on `cc-basic` for later modules; service accounts if not reused |
| 2 | Standard cluster, identity pool, identity provider, audit key, Entra apps and group (see 2.7 teardown) |
| 3 | quotas, `sa-analytics-replay` (cluster goes with module 02 teardown) |
| 4 | client quota config on `dc-a` |

## Cost note

| Item | Rough cost **[V]** |
|---|---|
| Part 1 on Basic | free |
| Part 2 Standard cluster, ~2 h | 0.75 USD/eCKU-h list → about 1–2 USD |
| Part 3 | inside the module 02 paid-session budget |
| Entra app registrations, groups | free |
