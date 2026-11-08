---
title: "08 — Lab: data contract + DLQ, broker-side validation, Schema Linking with a context, source-initiated link dc-a → Confluent Cloud"
subtitle: Parts A–C local, D cc-basic, E paid-session, F as code. Written 2026-11-07, untested.
---

# Goal

| Part | You prove | Environment |
|---|---|---|
| A | A data contract (metadata + CEL rules + DLQ) is enforced in the producer; bad records go to a DLQ | `local` |
| B | Broker-side schema ID validation on `dc-a` rejects records without a schema ID — and nothing more | `local` |
| C | Schema Linking keeps IDs valid across registries; a **context** avoids ID collisions; clients choose the context by URL | `local` |
| D | Schema Linking from the on-prem registry (`sr-a`) to the Confluent Cloud registry | `cc-basic` |
| E | Source-initiated Cluster Linking from a laptop "DC" to Confluent Cloud, outbound only; cloud consumer decodes on-prem schema IDs; CC broker-side validation on Dedicated | `paid-session` |
| F | The same end state as code: Terraform (CC side) + CFK CRs (CP side) | `paid-session` |

Time: A–C ~60 min, D ~15 min, E ~60 min (plus Dedicated provisioning time), F ~20 min.

# Prerequisites

> Console consumers started with `--timeout-ms` end with a `TimeoutException` message when no more
> records arrive. That is normal and not an error in the lab.

- `00-lab-setup`: start clean so IDs match the expected output:
  `docker compose --profile mm2 down -v && docker compose up -d`.
- Helpers loaded: `source <repo>/00-lab-setup/lab-env.sh` (`a`, `b`, `sra`, `srb`, `LAB`, `DCA_ID`, `DCB_ID`); `lab_check` passes, `jq`, Confluent CLI ≥ v4.82.0.
- D/E/F: `00-lab-setup/terraform` applied; for E/F with `-var enable_paid_dedicated=true`.

```bash
T=orders.order-status.updated.v1
S=$T-value
```

---

# Part A — Data contract with CEL rules and a DLQ (local)

## A1. Topic, DLQ topic, contract files

```bash
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T --partitions 3 --replication-factor 1
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T.dlq --partitions 1 --replication-factor 1

cat > $LAB/work/order-status-v1.avsc <<'EOF'
{"type":"record","name":"OrderStatusUpdated","namespace":"com.example.orders",
 "doc":"Status change of one order. PII-free.",
 "fields":[
  {"name":"orderId","type":"string","doc":"Order id, e.g. SO-1001601"},
  {"name":"status","type":"string","doc":"CREATED|CONFIRMED|DELAYED|LOCATION_CHANGE|READY|DISPATCHED|CANCELLED|UNKNOWN"},
  {"name":"scheduledTime","type":{"type":"long","logicalType":"timestamp-millis"}},
  {"name":"eventTime","type":{"type":"long","logicalType":"timestamp-millis"}}
 ]}
EOF

cat > $LAB/work/os-metadata.json <<'EOF'
{"properties":{"owner":"orders-team","owner_email":"orders@example.org",
 "classification":"internal","major_version":"1","sla":"tier-1"}}
EOF

cat > $LAB/work/os-ruleset.json <<EOF
{"domainRules":[
 {"name":"orderIdFormat","kind":"CONDITION","type":"CEL","mode":"WRITE",
  "expr":"message.orderId.matches('^SO-[0-9]{7}\$')",
  "params":{"dlq.topic":"$T.dlq","dlq.auto.flush":"true"},
  "onFailure":"DLQ"},
 {"name":"statusKnown","kind":"CONDITION","type":"CEL","mode":"WRITE",
  "expr":"message.status in ['CREATED','CONFIRMED','DELAYED','LOCATION_CHANGE','READY','DISPATCHED','CANCELLED','UNKNOWN']",
  "onFailure":"ERROR"}
]}
EOF
jq . $LAB/work/os-ruleset.json    # check the regex ends with $' and the dlq.topic is filled in
```

`dlq.auto.flush` makes the DLQ write visible immediately in a short-lived console producer. [V]

## A2. Register the contract (schema + metadata + rules) as one request

```bash
jq -n --rawfile s $LAB/work/order-status-v1.avsc \
      --slurpfile md $LAB/work/os-metadata.json --slurpfile rs $LAB/work/os-ruleset.json \
  '{schemaType:"AVRO", schema:$s, metadata:$md[0], ruleSet:$rs[0]}' > $LAB/work/os-v1-register.json

curl -s -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  localhost:8081/config/$S -d '{"compatibility":"BACKWARD_TRANSITIVE","compatibilityGroup":"major_version"}' | jq
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @$LAB/work/os-v1-register.json localhost:8081/subjects/$S/versions | jq
ID=$(curl -s localhost:8081/subjects/$S/versions/latest | jq .id); echo "schema id: $ID"
curl -s localhost:8081/subjects/$S/versions/latest | jq '{id, version, metadata, ruleSet}'
```

Expected: `{"id": 1}` on a fresh registry; `latest` returns the metadata and both rules. If the
POST fails with an error about rules or unknown fields, the `RuleSetResourceExtension` is not
loaded — check `SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS` in the compose file.

The same with the Confluent CLI (on-prem mode, against `sr-a`):

```bash
confluent schema-registry schema create --subject $S --schema $LAB/work/order-status-v1.avsc --type avro \
  --metadata $LAB/work/os-metadata.json --ruleset $LAB/work/os-ruleset.json \
  --schema-registry-endpoint http://localhost:8081
```

## A3. Produce good, bad-format (→ DLQ), and bad-status (→ ERROR)

The producer uses the registered contract, does not register anything itself, and wraps the key
serializer so the DLQ record keeps its key.

```bash
produce() {  # reads key|json lines from stdin
  sra kafka-avro-console-producer --bootstrap-server dc-a:29092 --topic $T \
    --property schema.registry.url=http://sr-a:8081 \
    --property value.schema.id=$ID \
    --property auto.register.schemas=false \
    --property use.latest.version=true \
    --property parse.key=true --property 'key.separator=|' \
    --property key.serializer=io.confluent.kafka.serializers.WrapperKeySerializer \
    --property wrapped.key.serializer=org.apache.kafka.common.serialization.StringSerializer \
    --property bootstrap.servers=dc-a:29092
}

echo 'SO-1001601|{"orderId":"SO-1001601","status":"READY","scheduledTime":1762500000000,"eventTime":1762499000000}' | produce
echo 'BAD1|{"orderId":"not an order","status":"READY","scheduledTime":1762500000000,"eventTime":1762499000000}' | produce
echo 'SO-1001602|{"orderId":"SO-1001602","status":"READIED??","scheduledTime":1762500000000,"eventTime":1762499000000}' | produce
```

Expected:
1. First line: accepted.
2. Second: no exception; the record lands in `$T.dlq` (rule `orderIdFormat`, action DLQ).
3. Third: the producer throws a serialization exception naming rule `statusKnown` (action ERROR)
   and exits.

```bash
sra kafka-avro-console-consumer --bootstrap-server dc-a:29092 --topic $T --from-beginning \
  --property schema.registry.url=http://sr-a:8081 --timeout-ms 10000            # only SO-1001601
a kafka-console-consumer --bootstrap-server dc-a:29092 --topic $T.dlq --from-beginning \
  --property print.key=true --property print.headers=true --timeout-ms 10000   # BAD1 + headers
```

What the DLQ record looks like (headers with rule name/mode/error, value encoding) is not
documented in detail — note what you see. [V] If the key-wrapper properties are rejected by the
console producer, drop the two `key.serializer` lines and rerun; the DLQ record then has no key.

Fallback if `value.schema.id` + `use.latest.version` does not pick up the rules: the documented
console-producer pattern passes the schema and the rules inline (`--property value.schema=…
--property value.rule.set=…`, auto-registers). Use it only to see the rule fire; it bypasses the
"producers don't register" principle.

## A4. Optional — breaking change on the same topic (compatibility group + JSONata)

v2 renames `status` → `orderStatus`. Without the compatibility group this is rejected; with
`compatibilityGroup=major_version` and `major_version=2` it is checked only against other
major-2 schemas (none yet).

```bash
sed 's/"name":"status"/"name":"orderStatus"/' $LAB/work/order-status-v1.avsc > $LAB/work/order-status-v2.avsc
cat > $LAB/work/os-v2-rules.json <<'EOF'
{"migrationRules":[
 {"name":"statusToOrderStatus","kind":"TRANSFORM","type":"JSONATA","mode":"UPGRADE",
  "expr":"$merge([$sift($, function($v, $k) {$k != 'status'}), {'orderStatus': $.'status'}])"},
 {"name":"orderStatusToStatus","kind":"TRANSFORM","type":"JSONATA","mode":"DOWNGRADE",
  "expr":"$merge([$sift($, function($v, $k) {$k != 'orderStatus'}), {'status': $.'orderStatus'}])"}
]}
EOF
jq -n --rawfile s $LAB/work/order-status-v2.avsc --slurpfile rs $LAB/work/os-v2-rules.json \
  '{schemaType:"AVRO", schema:$s,
    metadata:{properties:{owner:"orders-team",classification:"internal",major_version:"2",sla:"tier-1"}},
    ruleSet:$rs[0]}' > $LAB/work/os-v2-register.json
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @$LAB/work/os-v2-register.json localhost:8081/subjects/$S/versions | jq    # {"id": 2}

# A consumer pinned to major version 1 reads everything as v1 (v2 records are DOWNGRADEd)
sra kafka-avro-console-consumer --bootstrap-server dc-a:29092 --topic $T --from-beginning \
  --property schema.registry.url=http://sr-a:8081 \
  --property use.latest.with.metadata=major_version=1 --timeout-ms 10000
```

To see the migration, produce one v2 record (`value.schema.id=2`, field `orderStatus`) and
consume pinned to 1 (shows `status`) and pinned to 2 (shows `orderStatus`). Note: v2 has no
domain rules; rule sets are per version — copy the CEL rules into v2 in real life, or put them in
`defaultRuleSet`. [V — exact behaviour of the console consumer with `use.latest.with.metadata`]

---

# Part B — Broker-side schema ID validation on dc-a (local)

`dc-a` already has `confluent.schema.registry.url=http://sr-a:8081` (compose file).

```bash
a kafka-configs --bootstrap-server dc-a:29092 --alter --entity-type topics --entity-name $T \
  --add-config confluent.value.schema.validation=true
a kafka-configs --bootstrap-server dc-a:29092 --describe --entity-type topics --entity-name $T --all \
  | grep -E 'schema.validation|subject.name.strategy'
```

Expected: `confluent.value.schema.validation=true`; value subject strategy shows the default
(`AssociatedNameStrategy` on CP 8.4, falling back to TopicNameStrategy). [V]

```bash
# 1. plain text, no schema id -> rejected
echo 'SO-1001603|{"orderId":"SO-1001603","status":"DELAYED"}' | a kafka-console-producer \
  --bootstrap-server dc-a:29092 --topic $T --property parse.key=true --property 'key.separator=|'

# 2. Avro with a registered schema id -> accepted
echo 'SO-1001604|{"orderId":"SO-1001604","status":"DELAYED","scheduledTime":1762500000000,"eventTime":1762499500000}' | produce

# 3. tombstone (null value) -> accepted, even without a schema id
echo 'SO-1001604|NULL' | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T \
  --property parse.key=true --property 'key.separator=|' --property null.marker=NULL
```

Expected: (1) error containing "This record has failed the validation on broker" — the batch is
discarded; (2) OK; (3) OK.

What this did **not** test, and what the docs say: the broker checks the wire-format prefix and
that the ID exists for the topic's subject; it does **not** parse the payload. A record with a
valid ID and a corrupt body passes. Content quality is Part A's job.

Leave validation **off** on `$T.dlq`: DLQ records may not carry a valid ID.

---

# Part C — Schema Linking dc-a → dc-b with a context, and why the context matters (local)

## C1. Make an ID collision possible on purpose

`sr-b` already has its own schema with ID 1 (a different team's subject):

```bash
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' localhost:8082/subjects/local-only-value/versions \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Unrelated\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}"}' | jq   # {"id": 1}
```

## C2. Mirror the topic to dc-b

```bash
echo 'bootstrap.servers=dc-a:29092' > $LAB/work/sl-link.properties
b kafka-cluster-links --bootstrap-server dc-b:29092 --create --link sl-link --config-file /work/sl-link.properties
b kafka-mirrors --create --mirror-topic $T --link sl-link --bootstrap-server dc-b:29092
```

## C3. Consumer on dc-b without Schema Linking

```bash
srb kafka-avro-console-consumer --bootstrap-server dc-b:29092 --topic $T --from-beginning \
  --property schema.registry.url=http://sr-b:8081 --timeout-ms 10000
```

Expected: failure or garbage. The bytes say "schema 1"; `sr-b`'s schema 1 is `Unrelated`. This is
the silent-corruption case that makes Schema Linking non-optional. (Records with schema ID 2 from
A4, if any: "schema not found".)

## C4. Exporter sr-a → sr-b into context `.dc-a`

```bash
curl -s -X POST -H 'Content-Type: application/json' localhost:8081/exporters -d '{
  "name": "to-dc-b",
  "subjects": ["orders.order-status.updated.v1-value"],
  "contextType": "CUSTOM",
  "context": "dc-a",
  "config": {"schema.registry.url": "http://sr-b:8081"}
}' | jq
sleep 5
curl -s localhost:8081/exporters/to-dc-b/status | jq          # state RUNNING, offset, ts
curl -s localhost:8082/contexts | jq                          # [".", ".dc-a"]
curl -s 'localhost:8082/subjects?subjectPrefix=:.dc-a:' | jq
curl -s "localhost:8082/subjects/:.dc-a:$S/versions/latest" | jq '{id, version, metadata, ruleSet}'
curl -s localhost:8082/contexts/.dc-a/mode | jq               # IMPORT
```

Expected: same ID(s) as in `sr-a` (1, and 2 if you did A4), metadata and rules copied, context in
`IMPORT` mode. CLI alternative inside the container:
`sra schema-exporter --create --name to-dc-b --subjects $S --context-type CUSTOM --context-name dc-a --config-file /work/exporter-sr-b.txt --schema.registry.url http://sr-a:8081`
(config file with `schema.registry.url=http://sr-b:8081`). [V — binary present in the image]

Try to write into the imported context:

```bash
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  "localhost:8082/subjects/:.dc-a:$S/versions" --data @$LAB/work/os-v1-register.json | jq
```

Expected: refused (subject/context in IMPORT mode). One write master per subject, enforced.

## C5. Consumer on dc-b, pointed at the context

```bash
srb kafka-avro-console-consumer --bootstrap-server dc-b:29092 --topic $T --from-beginning \
  --property schema.registry.url=http://sr-b:8081/contexts/.dc-a --timeout-ms 10000
```

Expected: the records decode correctly. The only change is the base path in the URL — this is
what a platform template hides from data teams.

Remember from the notes: CP broker-side validation on `dc-b` would **not** find schemas in
`.dc-a` (default context only). On a mirror topic that does not matter (read-only), but it matters
if you ever fail over and produce on `dc-b`.

---

# Part D — Schema Linking sr-a → Confluent Cloud (cc-basic)

```bash
cd $LAB/terraform
SR_URL=$(terraform output -raw schema_registry_rest_endpoint)
SR_KEY=$(terraform output -raw sa_sr_api_key); SR_SECRET=$(terraform output -raw sa_sr_api_secret)

jq -n --arg url "$SR_URL" --arg ui "$SR_KEY:$SR_SECRET" '{
  name: "to-cc", subjects: ["orders.order-status.updated.v1-value"],
  contextType: "CUSTOM", context: "onprem",
  config: {"schema.registry.url": $url, "basic.auth.credentials.source": "USER_INFO", "basic.auth.user.info": $ui}
}' > $LAB/work/exporter-to-cc.json

curl -s -X POST -H 'Content-Type: application/json' localhost:8081/exporters --data @$LAB/work/exporter-to-cc.json | jq
sleep 10
curl -s localhost:8081/exporters/to-cc/status | jq
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects?subjectPrefix=:.onprem:" | jq
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects/:.onprem:$S/versions/latest" | jq '{id, version, metadata}'
```

Expected: exporter `RUNNING`; subject `:.onprem:orders.order-status.updated.v1-value` in CC
with the **same ID** as on `sr-a`. The exporter connection is outbound HTTPS from the container —
same direction rule as the cluster link. Essentials allows 10 exporters per environment. Rules are
copied as data, but CC will only *execute* them in clients if the environment is on Advanced. [V]

---

# Part E — Source-initiated link dc-a → Confluent Cloud (paid-session)

## Why this works from a laptop

`dc-a` is a container behind Docker's NAT and your home router. Nothing on the internet can open a
connection to it. A **source-initiated** link needs exactly that: `dc-a`'s broker opens TLS
connections **out** to `pkc-….westeurope.azure.confluent.cloud:9092`; Confluent Cloud sends its
fetch requests back over those connections. A destination-initiated link would make CC try to
reach `dc-a:29092` — an unresolvable private name — and fail. Requirements on your side: outbound
TCP 9092 (and 443 for the CLI/REST) allowed; no proxy that breaks long-lived TCP. Corporate
networks often block 9092 — use a home network.

## E0. Variables

```bash
cd $LAB/terraform
terraform apply -var enable_paid_dedicated=true        # wait until cc-paid is provisioned
ENV_ID=$(terraform output -raw environment_id)
CC_ID=$(terraform output -raw dedicated_cluster_id)
CC_BOOT=$(terraform output -raw dedicated_bootstrap_endpoint | sed 's#^SASL_SSL://##')
CC_KEY=$(terraform output -raw sa_kafka_dedicated_api_key); CC_SECRET=$(terraform output -raw sa_kafka_dedicated_api_secret)
confluent environment use $ENV_ID
confluent api-key store $CC_KEY $CC_SECRET --resource $CC_ID && confluent api-key use $CC_KEY
```

## E1. Destination half on Confluent Cloud (first)

```bash
cat > $LAB/work/hybrid-dst.properties <<'EOF'
link.mode=DESTINATION
connection.mode=INBOUND
EOF
confluent kafka link create from-dc-a --cluster $CC_ID --source-cluster $DCA_ID \
  --config $LAB/work/hybrid-dst.properties
confluent kafka link list --cluster $CC_ID
```

Expected: `Created cluster link "from-dc-a".` Read-replica settings: no offset sync, no ACL sync.
Any link config (offset sync, auto-create filters) goes in **this** file, never on the CP half.

## E2. Source half on dc-a (second)

```bash
cat > $LAB/work/hybrid-src.properties <<EOF
link.mode=SOURCE
connection.mode=OUTBOUND
bootstrap.servers=$CC_BOOT
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$CC_KEY" password="$CC_SECRET";
ssl.endpoint.identification.algorithm=https
local.listener.name=PLAINTEXT
local.security.protocol=PLAINTEXT
EOF
a kafka-cluster-links --bootstrap-server dc-a:29092 --create --link from-dc-a \
  --config-file /work/hybrid-src.properties --cluster-id $CC_ID
a kafka-cluster-links --bootstrap-server dc-a:29092 --list
```

Expected: `Cluster link 'from-dc-a' creation successfully completed.`; `--list` shows remote
cluster `lkc-…`. The `local.*` lines tell the link how to talk to its own cluster — PLAINTEXT
because the lab broker has no auth. The docs' tutorial uses a SCRAM listener
(`local.sasl.mechanism=SCRAM-SHA-512`); production must use an authenticated listener and the
`ClusterLinkConnection` role or ACLs for the link principal. [untested with PLAINTEXT local]

## E3. Mirror topic in Confluent Cloud

```bash
confluent kafka mirror create $T --link from-dc-a --cluster $CC_ID
confluent kafka mirror describe $T --link from-dc-a --cluster $CC_ID
confluent kafka mirror list --cluster $CC_ID
```

Expected: mirror `ACTIVE`, per-partition lag 0, offsets equal to `dc-a` (`a kafka-get-offsets …`).
Produce one more record on `dc-a` (`produce` from A3) and watch it appear.

## E4. Cloud consumer decodes on-prem schema IDs (needs Part D)

```bash
cat > $LAB/work/cc-client.properties <<EOF
bootstrap.servers=$CC_BOOT
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$CC_KEY" password="$CC_SECRET";
EOF
sra kafka-avro-console-consumer --bootstrap-server $CC_BOOT --consumer.config /work/cc-client.properties \
  --topic $T --from-beginning --timeout-ms 20000 \
  --property schema.registry.url=$SR_URL/contexts/.onprem \
  --property basic.auth.credentials.source=USER_INFO --property basic.auth.user.info=$SR_KEY:$SR_SECRET
```

Expected: records decode. Then open the topic in the Cloud Console message viewer: per the docs,
the viewer looks in the **default** context, so the Avro values are not decoded there. That is the
landing-environment argument from the notes, seen on screen. [V]

## E5. Broker-side validation in Confluent Cloud (Dedicated only)

```bash
V=orders.order-status.validated.v1
confluent kafka topic create $V --cluster $CC_ID --partitions 1 --config confluent.value.schema.validation=true
echo '{"orderId":"SO-1001605"}' | confluent kafka topic produce $V --cluster $CC_ID          # rejected
confluent kafka topic produce $V --cluster $CC_ID --value-format avro \
  --schema $LAB/work/order-status-v1.avsc --schema-registry-endpoint $SR_URL \
  --schema-registry-api-key $SR_KEY --schema-registry-api-secret $SR_SECRET
# then type: {"orderId":"SO-1001605","status":"READY","scheduledTime":1762500000000,"eventTime":1762499000000}
```

Expected: plain JSON rejected; Avro accepted (CLI registers `orders.order-status.validated.v1-value`
in the default context). Try the same `topic create … --config confluent.value.schema.validation=true`
on `cc-basic`: refused / setting not available. [V — exact CLI flags for produce]

---

# Part F — Same end state as code

## F1. Terraform, Confluent Cloud side (save as `$LAB/work/tf-08/main.tf`)

The provider manages Confluent Cloud only. The CP half of the link and the CP exporter are not
Terraform resources (→ F2).

```hcl
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.94.0"
    }
  }
}

provider "confluent" {}

variable "dc_a_cluster_id" {
  type    = string
  default = "Q0PvpgzYSwirapvmRY0NFg" # CLUSTER_ID of dc-a in docker-compose.yml
}

data "terraform_remote_state" "base" {
  backend = "local"
  config  = { path = "../../terraform/terraform.tfstate" }
}

locals {
  b = data.terraform_remote_state.base.outputs
}

data "confluent_kafka_cluster" "paid" {
  id = local.b.dedicated_cluster_id
  environment { id = local.b.environment_id }
}

# Destination half of the source-initiated link. Create this BEFORE the CP half.
resource "confluent_cluster_link" "from_dc_a" {
  link_name       = "from-dc-a"
  link_mode       = "DESTINATION"
  connection_mode = "INBOUND"

  destination_kafka_cluster {
    id            = data.confluent_kafka_cluster.paid.id
    rest_endpoint = data.confluent_kafka_cluster.paid.rest_endpoint
    credentials {
      key    = local.b.sa_kafka_dedicated_api_key
      secret = local.b.sa_kafka_dedicated_api_secret
    }
  }

  source_kafka_cluster {
    id = var.dc_a_cluster_id
    # The provider wants exactly one endpoint. For an INBOUND link CC never dials it,
    # so this is a placeholder (same as the provider's source-initiated example). [V]
    bootstrap_endpoint = "dc-a:29092"
  }
}

resource "confluent_kafka_mirror_topic" "order_status" {
  source_kafka_topic {
    topic_name = "orders.order-status.updated.v1"
  }
  cluster_link {
    link_name = confluent_cluster_link.from_dc_a.link_name
  }
  kafka_cluster {
    id            = data.confluent_kafka_cluster.paid.id
    rest_endpoint = data.confluent_kafka_cluster.paid.rest_endpoint
    credentials {
      key    = local.b.sa_kafka_dedicated_api_key
      secret = local.b.sa_kafka_dedicated_api_secret
    }
  }
  # Apply only after the CP half exists, or the mirror cannot start.
}

# Cloud-native published topic with broker-side schema ID validation (Dedicated only).
resource "confluent_kafka_topic" "validated" {
  kafka_cluster {
    id = data.confluent_kafka_cluster.paid.id
  }
  topic_name       = "orders.order-status.validated.v1"
  partitions_count = 1
  rest_endpoint    = data.confluent_kafka_cluster.paid.rest_endpoint
  config = {
    "confluent.value.schema.validation" = "true"
  }
  credentials {
    key    = local.b.sa_kafka_dedicated_api_key
    secret = local.b.sa_kafka_dedicated_api_secret
  }
}

# Compatibility policy for the cloud-native subject.
resource "confluent_subject_config" "validated_value" {
  schema_registry_cluster {
    id = local.b.schema_registry_id
  }
  rest_endpoint       = local.b.schema_registry_rest_endpoint
  subject_name        = "orders.order-status.validated.v1-value"
  compatibility_level = "BACKWARD_TRANSITIVE"
  compatibility_group = "major_version" # [V] accepted on Essentials?
  credentials {
    key    = local.b.sa_sr_api_key
    secret = local.b.sa_sr_api_secret
  }
}

# Data contract with rules = Stream Governance ADVANCED. The base environment is Essentials and
# cannot be downgraded later, so rules belong in a separate environment, e.g.:
#
# resource "confluent_environment" "adv" {
#   display_name = "crash-course-adv"
#   stream_governance { package = "ADVANCED" }
# }
# resource "confluent_schema" "order_status" {
#   schema_registry_cluster { id = data.confluent_schema_registry_cluster.adv.id }
#   rest_endpoint = data.confluent_schema_registry_cluster.adv.rest_endpoint
#   subject_name  = "orders.order-status.validated.v1-value"
#   format        = "AVRO"
#   schema        = file("../order-status-v1.avsc")
#   metadata {
#     properties = { owner = "orders-team", classification = "internal", major_version = "1" }
#   }
#   ruleset {
#     domain_rules {
#       name       = "orderIdFormat"
#       kind       = "CONDITION"
#       type       = "CEL"
#       mode       = "WRITE"
#       expr       = "message.orderId.matches('^SO-[0-9]{7}$')"
#       on_failure = "DLQ"
#       params     = { "dlq.topic" = "orders.order-status.validated.v1.dlq" }
#     }
#   }
#   credentials { key = "<adv SR key>" secret = "<adv SR secret>" }
# }
```

```bash
cd $LAB/work/tf-08 && terraform init
terraform apply -target=confluent_cluster_link.from_dc_a     # CC half first
# create the CP half (E2), then:
terraform apply
```

If you already did E1–E3 by CLI, delete them first (`confluent kafka mirror failover …` or delete
the mirror topic, then `confluent kafka link delete from-dc-a`) or `terraform import` them.

## F2. CP side as CFK custom resources (on-prem / OpenShift, GitOps) — not runnable on Docker

```yaml
# Source half of the source-initiated link, on the CP cluster managed by CFK
apiVersion: platform.confluent.io/v1beta1
kind: ClusterLink
metadata:
  name: from-dc-a
  namespace: dc-a
spec:
  name: from-dc-a
  sourceInitiatedLink:
    linkMode: Source
  destinationKafkaCluster:
    bootstrapEndpoint: pkc-xxxxx.westeurope.azure.confluent.cloud:9092
    clusterID: lkc-xxxxx
    authentication:
      type: plain
      jaasConfig:
        secretRef: cc-paid-link-credentials    # CC API key/secret
    tls:
      enabled: true                            # [V] CA handling for CC's public certificate
  sourceKafkaCluster:
    kafkaRestClassRef:
      name: dc-a-rest
  configs:
    local.security.protocol: SASL_SSL          # authenticated local listener in production
---
# Schema exporter from the on-prem registry to the CC registry
apiVersion: platform.confluent.io/v1beta1
kind: SchemaExporter
metadata:
  name: to-cc
  namespace: dc-a
spec:
  sourceCluster:
    schemaRegistryClusterRef:
      name: schemaregistry
      namespace: dc-a
  destinationCluster:
    schemaRegistryRest:
      endpoint: https://psrc-xxxxx.westeurope.azure.confluent.cloud
      authentication:
        type: basic
        basic:
          secretRef: cc-sr-credentials
  subjects:
    - orders.order-status.updated.v1-value
  contextName: onprem        # or, for a landing environment: contextType DEFAULT [V: CFK accepts DEFAULT?]
```

Broker-side validation on CP is a topic config: in CFK, a `KafkaTopic` CR with
`configs: {confluent.value.schema.validation: "true"}`, plus `confluent.schema.registry.url` in the
Kafka CR's `configOverrides.server`.

---

# Verification checklist

- [ ] A2: contract registered with metadata + 2 rules; compatibility group set.
- [ ] A3: good → topic; bad format → DLQ (with key); bad status → producer error.
- [ ] B: plain text rejected by the broker; Avro accepted; tombstone accepted.
- [ ] C3: decoding with `sr-b` default context fails/garbles (collision on ID 1).
- [ ] C4/C5: `.dc-a` context has the same IDs, IMPORT mode, and decoding via `/contexts/.dc-a` works.
- [ ] D: `:.onprem:` subject in CC with the on-prem ID.
- [ ] E3: mirror `ACTIVE` in `cc-paid`, offsets equal to `dc-a`.
- [ ] E4: cloud consumer decodes with `/contexts/.onprem`; Console viewer does not.
- [ ] E5: CC validation rejects plain JSON on Dedicated.

# Teardown

```bash
# paid-session first (money)
confluent kafka mirror failover $T --link from-dc-a --cluster $CC_ID     # or delete the mirror topic
confluent kafka topic delete $T --cluster $CC_ID --force
confluent kafka topic delete orders.order-status.validated.v1 --cluster $CC_ID --force
confluent kafka link delete from-dc-a --cluster $CC_ID
a kafka-cluster-links --bootstrap-server dc-a:29092 --delete --link from-dc-a
cd $LAB/work/tf-08 && terraform destroy                                   # if you used F1
cd $LAB/terraform && terraform apply -var enable_paid_dedicated=false

# cc-basic exporter target (keep the environment for other modules)
curl -s -X PUT localhost:8081/exporters/to-cc/pause && curl -s -X DELETE localhost:8081/exporters/to-cc
curl -s -u "$SR_KEY:$SR_SECRET" -X DELETE "$SR_URL/subjects/:.onprem:$S"   # soft delete; add ?permanent=true after

# local
curl -s -X PUT localhost:8081/exporters/to-dc-b/pause && curl -s -X DELETE localhost:8081/exporters/to-dc-b
docker compose -f $LAB/docker-compose.yml down -v
```

`--force` flags and delete order: [V] on CLI v4.82.

# Cost

A–C free. D: a few schemas in Essentials (100 free). E/F: Dedicated cluster hours + one link +
bytes — destroy the same day; check the Billing page. [V]

# Untested / check on first run

- Rule execution in `kafka-avro-console-producer` with `value.schema.id` + `use.latest.version`;
  `WrapperKeySerializer` via `--property`; `dlq.auto.flush`; DLQ record format and headers.
- `use.latest.with.metadata` in the console consumer (A4).
- Default subject strategy shown on CP 8.4 topics (B).
- Behaviour of `sr-b` ID lookup in C3 (does it fall back to other contexts?).
- `schema-exporter` binary in `cp-schema-registry:8.4.0`.
- PLAINTEXT `local.*` settings for the source half (E2); Console viewer behaviour with contexts (E4).
- `confluent kafka topic produce` flags (E5); Terraform F1 placeholder `bootstrap_endpoint`;
  `compatibility_group` on Essentials; CFK TLS block for CC.
