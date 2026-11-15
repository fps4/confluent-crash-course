---
title: "01 — Lab: one outbound link, schemas that keep their IDs, and what validation checks"
subtitle: Parts A–C local, D cc-basic, E paid-session, F as code. Part A tested on CP 8.4.0 on 2026-11-14; the rest written 2026-11-07, untested.
---

> **Video:** episode 1, "Getting data out of the data centre". The video gives the reasoning; this
> lab lets you see it happen, in the same order.

# Goal

The running example is the mobile operator from [00-lab-setup](../00-lab-setup/README.md#the-running-example):
call detail records (`billing.call-record.created.v1`) leave the data centre for the data teams in
the cloud.

| Part | You see | Environment | Video take | Time |
|---|---|---|---|---|
| A | The data centre dials out; the cloud side holds the settings; a mirror skips history but keeps offsets; a quota slows a catch-up | `local` | 3, 4 | 25 min |
| B | Mirrored records carry only a schema ID, so the copy is unreadable until Schema Linking brings the schemas with the same IDs | `local` | 6, 7 | 20 min |
| C | Broker-side validation checks the ID, never the content; the data contract in the serializer checks the content | `local` | 8 | 30 min |
| D | Schema Linking to Confluent Cloud: into a context, and into the default context of a landing environment | `cc-basic` | 7 | 20 min |
| E | The real thing: `dc-a` dials out to Confluent Cloud; the Console can't decode a context; a rotated key stops the link | `paid-session` | 3, 5, 7 | 60 min + provisioning |
| F | The same end state as code: Terraform for the cloud half, CFK custom resources for the CP half | `paid-session` | — | 20 min |

# Before you start

```bash
source ~/labs/hybrid-kafka-on-azure/00-lab-setup/lab-env.sh     # a, b, sra, srb, LAB, DCA_ID, DCB_ID
( cd "$LAB" && docker compose --profile mm2 down -v && docker compose up -d )   # start clean: IDs below assume empty registries
lab_check                                                          # ends with "All good."
```

- D/E/F also need `00-lab-setup/terraform` applied; E/F with `-var enable_paid_dedicated=true`.
- Console consumers started with `--timeout-ms` end with a `TimeoutException` when no more records
  arrive. That is normal.

Two helpers for the whole lab: `produce` writes Avro call records to `dc-a` with the registered
schema (producers never register schemas themselves), and `cdr` makes one record.

```bash
T=billing.call-record.created.v1
S=$T-value

produce() {  # reads key|json lines from stdin
  sra kafka-avro-console-producer --bootstrap-server dc-a:29092 --topic $T \
    --reader-property schema.registry.url=http://sr-a:8081 \
    --reader-property value.schema.id=$ID \
    --reader-property auto.register.schemas=false \
    --reader-property use.latest.version=true \
    --reader-property parse.key=true --reader-property 'key.separator=|' \
    --reader-property bootstrap.servers=dc-a:29092
}
cdr() {      # cdr <n> <recordType> <durationSec>
  echo "SUB-$((1001600 + $1 % 7))|{\"recordId\":\"CDR-$((1001600 + $1))\",\"recordType\":\"$2\",\"cellId\":\"CELL-0412\",\"durationSec\":$3,\"startTime\":1762499000000,\"eventTime\":1762499184000}"
}
```

---

# Part A — One outbound link (local)

`dc-a` plays the on-prem cluster, `dc-b` plays Confluent Cloud. Part E does the same against the
real Confluent Cloud.

## A1. Topic, schema, ten call records on dc-a

```bash
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T --partitions 3 --replication-factor 1

cat > $LAB/work/call-record-v1.avsc <<'EOF'
{"type":"record","name":"CallRecordCreated","namespace":"com.example.billing",
 "doc":"One call detail record: a call, a text or a data session. No direct identifiers; the key is a pseudonymous subscriber reference.",
 "fields":[
  {"name":"recordId","type":"string","doc":"CDR id, e.g. CDR-1001601"},
  {"name":"recordType","type":"string","doc":"VOICE|SMS|DATA|ROAMING_VOICE|ROAMING_DATA|UNKNOWN"},
  {"name":"cellId","type":"string","doc":"Serving cell, e.g. CELL-0412"},
  {"name":"durationSec","type":"long","doc":"Seconds for a call or a session; 0 for a text"},
  {"name":"startTime","type":{"type":"long","logicalType":"timestamp-millis"}},
  {"name":"eventTime","type":{"type":"long","logicalType":"timestamp-millis"}}
 ]}
EOF

curl -s -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  localhost:8081/config/$S -d '{"compatibility":"BACKWARD_TRANSITIVE"}' | jq
jq -n --rawfile s $LAB/work/call-record-v1.avsc '{schemaType:"AVRO", schema:$s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @- \
      localhost:8081/subjects/$S/versions | jq
ID=$(curl -s localhost:8081/subjects/$S/versions/latest | jq .id); echo "schema id: $ID"

for i in $(seq 1 10); do cdr $i VOICE 184; done | produce
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T
```

Expected: `{"id": 1}`; end offsets that sum to 10.

## A2. The cloud half first: it holds the settings

```bash
cat > $LAB/work/from-dc-a-dst.properties <<'EOF'
link.mode=DESTINATION
connection.mode=INBOUND
mirror.start.offset.spec=latest
consumer.offset.sync.enable=false
acl.sync.enable=false
EOF
b kafka-cluster-links --bootstrap-server dc-b:29092 --create --link from-dc-a \
  --config-file /work/from-dc-a-dst.properties --cluster-id $DCA_ID
```

Expected: `Cluster link 'from-dc-a' creation successfully completed.` Look at the file: there is no
address of `dc-a` in it. The cloud half can't dial anyone; it waits.

The settings are the read-replica ones from the notes: start new mirrors at the latest offset (no
full-retention read on day one), and no offset or ACL sync (cloud consumers are other groups with
other identities).

## A3. The on-prem half second: it dials out

```bash
cat > $LAB/work/from-dc-a-src.properties <<'EOF'
link.mode=SOURCE
connection.mode=OUTBOUND
bootstrap.servers=dc-b:29092
local.listener.name=PLAINTEXT
local.security.protocol=PLAINTEXT
EOF
a kafka-cluster-links --bootstrap-server dc-a:29092 --create --link from-dc-a \
  --config-file /work/from-dc-a-src.properties --cluster-id $DCB_ID

b kafka-cluster-links --bootstrap-server dc-b:29092 --list
b kafka-configs --bootstrap-server dc-b:29092 --describe --cluster-link from-dc-a | grep -E 'connection.mode|start.offset'
```

Expected (tested 2026-11-14):

```
Link name: 'from-dc-a', link ID: '…', remote cluster ID: 'Q0PvpgzYSwirapvmRY0NFg', local cluster ID: 'UmwHWmvUR6KH9yIQ3xbmhg', remote cluster available: 'true', link state: 'ACTIVE'
  connection.mode=INBOUND sensitive=false synonyms={}
  mirror.start.offset.spec=latest sensitive=false synonyms={}
  remote.link.connection.mode=OUTBOUND sensitive=false synonyms={}
```

`remote.link.connection.mode=OUTBOUND` is the whole point: the other side opened the connection.
Both halves have the same name. The `local.*` lines tell the on-prem half how to read its own
cluster; production uses an authenticated listener there.

In this lab both clusters sit on one Docker network, so `dc-b` *could* reach `dc-a`. It doesn't
need to. Against Confluent Cloud (part E) this is the only way that works through a firewall that
allows outbound only.

## A4. The mirror is the allow-list, and it keeps offsets

```bash
b kafka-mirrors --create --mirror-topic $T --link from-dc-a --bootstrap-server dc-b:29092
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T                  # end offsets
b kafka-get-offsets --bootstrap-server dc-b:29092 --topic $T                  # the same end offsets
b kafka-get-offsets --bootstrap-server dc-b:29092 --topic $T --time earliest  # the same again: no history
```

One `kafka-mirrors --create` per topic is the allow-list: nothing crosses unless someone created its
mirror. In real life that command runs from a reviewed pull request, and automatic mirror creation
stays off.

Before you run the next block, predict: the next record lands on `dc-b` at offset 0, or at the
offset it has on `dc-a`?

```bash
for i in 11 12 13; do cdr $i SMS 0; done | produce
b kafka-console-consumer --bootstrap-server dc-b:29092 --topic $T --from-beginning --timeout-ms 8000 \
  --formatter-property print.partition=true --formatter-property print.offset=true \
  --formatter-property print.value=false
```

Expected: three lines like `Partition:0	Offset:4`, continuing dc-a's numbering. The mirror skipped the
first ten records and still kept every offset. (The values aren't printed yet: decoding them is
part B.)

> **In the video:** take 4 shows this output.

## A5. A quota stops a catch-up from starving the source

The NOC wants its alarm history in the cloud: a backfill.

```bash
H=noc.cell-alarm.history.v1
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $H --partitions 1 --replication-factor 1
a kafka-producer-perf-test --topic $H --num-records 4000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=dc-a:29092                               # 4 MB stand-in history

# the quota, on dc-a, on the principal the link reads as
a kafka-configs --bootstrap-server dc-a:29092 --alter --entity-type users --entity-name ANONYMOUS \
  --add-config consumer_byte_rate=20000

# this topic needs its history; the start offset is a link setting, read when a mirror is created
b kafka-configs --bootstrap-server dc-b:29092 --alter --cluster-link from-dc-a --add-config mirror.start.offset.spec=earliest
b kafka-mirrors --create --mirror-topic $H --link from-dc-a --bootstrap-server dc-b:29092
for n in 1 2 3; do sleep 10; b kafka-get-offsets --bootstrap-server dc-b:29092 --topic $H; done
```

Expected: a few hundred records after 30 seconds, out of 4000 (tested: 192, 384, 576). Without the
quota the same 4 MB arrives in under a second. Now lift it and put the start offset back:

```bash
a kafka-configs --bootstrap-server dc-a:29092 --alter --entity-type users --entity-name ANONYMOUS \
  --delete-config consumer_byte_rate
sleep 5; b kafka-get-offsets --bootstrap-server dc-b:29092 --topic $H         # 4000
b kafka-configs --bootstrap-server dc-b:29092 --alter --cluster-link from-dc-a --add-config mirror.start.offset.spec=latest
```

The lab's brokers have no authentication, so the link reads as `User:ANONYMOUS`, like every other
lab client. In production the link authenticates on the CP side with its own principal, and the
quota goes on that principal only.

> **In the video:** take 4 shows the slow catch-up.

---

# Part B — A record carries a number (local)

## B1. dc-b's registry has its own schema 1

```bash
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' localhost:8082/subjects/local-only-value/versions \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Unrelated\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}"}' | jq   # {"id": 1}
```

A team on the cloud side registered something of its own. IDs are per registry, so it got ID 1 too.

## B2. A cloud consumer reads the mirror

The mirror is `ACTIVE` and its lag is zero. Predict what this consumer prints:

```bash
srb kafka-avro-console-consumer --bootstrap-server dc-b:29092 --topic $T --from-beginning \
  --formatter-property schema.registry.url=http://sr-b:8081 --timeout-ms 10000
```

Expected: an error or garbage. Each record starts with schema ID 1; in `sr-b`, ID 1 is `Unrelated`.
This is the silent-corruption case that makes Schema Linking non-optional. [V: exact output]

> **In the video:** take 6 shows this output.

## B3. Schema Linking sr-a → sr-b, into the context `.dc-a`

```bash
curl -s -X POST -H 'Content-Type: application/json' localhost:8081/exporters -d '{
  "name": "to-dc-b",
  "subjects": ["billing.call-record.created.v1-value"],
  "contextType": "CUSTOM",
  "context": "dc-a",
  "config": {"schema.registry.url": "http://sr-b:8081"}
}' | jq
sleep 5
curl -s localhost:8081/exporters/to-dc-b/status | jq          # state RUNNING, offset, ts
curl -s localhost:8082/contexts | jq                          # [".", ".dc-a"]
curl -s "localhost:8082/subjects/:.dc-a:$S/versions/latest" | jq '{id, version}'
curl -s localhost:8082/contexts/.dc-a/mode | jq               # IMPORT
```

Expected: the same ID as in `sr-a` (1), in a context of its own, in `IMPORT` mode. CLI alternative
inside the container: `sra schema-exporter --create --name to-dc-b --subjects $S --context-type CUSTOM
--context-name dc-a --config-file /work/exporter-sr-b.txt --schema.registry.url http://sr-a:8081`
(config file with `schema.registry.url=http://sr-b:8081`). [V: binary present in the image]

Now try to write into the imported context, as a cloud team would:

```bash
jq -n --rawfile s $LAB/work/call-record-v1.avsc '{schemaType:"AVRO", schema:$s}' \
  | curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data @- \
      "localhost:8082/subjects/:.dc-a:$S/versions" | jq
```

Expected: refused (IMPORT mode). One write master per subject, enforced by the registry.

> **In the video:** take 6 shows the exporter status.

## B4. The same consumer, pointed at the context

```bash
srb kafka-avro-console-consumer --bootstrap-server dc-b:29092 --topic $T --from-beginning \
  --formatter-property schema.registry.url=http://sr-b:8081/contexts/.dc-a --timeout-ms 10000
```

Expected: the three call records from A4, decoded. The only change is the base path in the URL;
a platform template hides it from data teams. Tools that only read the default context don't get
that URL: part E shows the Console failing on it, and part D builds the landing environment that
avoids it.

In real life the exporter runs **before** the first mirrored record arrives. Here it ran after, so
you could see what happens without it.

---

# Part C — What validation checks, and what the contract adds (local)

## C1. Broker-side schema ID validation on dc-a

`dc-a` already has `confluent.schema.registry.url=http://sr-a:8081` (compose file).

```bash
a kafka-configs --bootstrap-server dc-a:29092 --alter --entity-type topics --entity-name $T \
  --add-config confluent.value.schema.validation=true
```

Four records. Predict which ones the broker accepts:

```bash
# 1. plain JSON, no schema ID
echo 'SUB-1001601|{"recordId":"CDR-1001614","recordType":"VOICE"}' | a kafka-console-producer \
  --bootstrap-server dc-a:29092 --topic $T --reader-property parse.key=true --reader-property 'key.separator=|'
# 2. a valid call record
cdr 15 VOICE 61 | produce
# 3. a well-formed call record with a negative duration
cdr 16 VOICE -40 | produce
# 4. a tombstone (null value)
echo 'SUB-1001601|NULL' | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T \
  --reader-property parse.key=true --reader-property 'key.separator=|' --reader-property null.marker=NULL
```

Expected: (1) rejected with "This record has failed the validation on broker", and the whole batch
with it; (2) accepted; (3) **accepted**; (4) accepted. The broker checks that the record starts with
the wire-format prefix and that the ID exists for the topic's subject. It never reads the payload,
so a call that lasted minus forty seconds goes straight to billing.

> **In the video:** take 8 shows records 1 and 3.

## C2. The data contract: rules that run in the serializer

```bash
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T.dlq --partitions 1 --replication-factor 1

cat > $LAB/work/cr-metadata.json <<'EOF'
{"properties":{"owner":"billing-team","owner_email":"billing@example.org",
 "classification":"C1","major_version":"1","sla":"tier-1"}}
EOF
cat > $LAB/work/cr-ruleset.json <<EOF
{"domainRules":[
 {"name":"recordIdFormat","kind":"CONDITION","type":"CEL","mode":"WRITE",
  "expr":"message.recordId.matches('^CDR-[0-9]{7}\$')",
  "params":{"dlq.topic":"$T.dlq","dlq.auto.flush":"true"},
  "onFailure":"DLQ"},
 {"name":"durationNotNegative","kind":"CONDITION","type":"CEL","mode":"WRITE",
  "expr":"message.durationSec >= 0",
  "onFailure":"ERROR"}
]}
EOF
jq . $LAB/work/cr-ruleset.json    # check the regex ends with $' and dlq.topic is filled in

jq -n --rawfile s $LAB/work/call-record-v1.avsc \
      --slurpfile md $LAB/work/cr-metadata.json --slurpfile rs $LAB/work/cr-ruleset.json \
  '{schemaType:"AVRO", schema:$s, metadata:$md[0], ruleSet:$rs[0]}' > $LAB/work/cr-v1-register.json
curl -s -X PUT -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  localhost:8081/config/$S -d '{"compatibility":"BACKWARD_TRANSITIVE","compatibilityGroup":"major_version"}' | jq
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @$LAB/work/cr-v1-register.json localhost:8081/subjects/$S/versions | jq
ID=$(curl -s localhost:8081/subjects/$S/versions/latest | jq .id); echo "schema id: $ID"
curl -s localhost:8081/subjects/$S/versions/latest | jq '{id, version, metadata, ruleSet}'
```

Expected: a new version with a new ID (2): same schema, now with metadata and two rules. If the
POST fails on rules or unknown fields, the `RuleSetResourceExtension` is not loaded; check
`SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS` in the compose file. [V: new ID vs same ID]

Same three kinds of record as before, now through the contract:

```bash
cdr 17 VOICE 95 | produce                                                  # good
echo 'SUB-1001601|{"recordId":"not a cdr","recordType":"VOICE","cellId":"CELL-0412","durationSec":30,"startTime":1762499000000,"eventTime":1762499030000}' | produce
cdr 18 VOICE -40 | produce                                                 # the negative duration again
```

Expected:
1. Accepted.
2. No exception; the record lands in `$T.dlq` (rule `recordIdFormat`, action DLQ).
3. The producer throws a serialization exception naming `durationNotNegative` (action ERROR). The
   record that the broker let through in C1 now never leaves the producer.

```bash
a kafka-console-consumer --bootstrap-server dc-a:29092 --topic $T.dlq --from-beginning \
  --formatter-property print.headers=true --timeout-ms 10000
```

Note what the DLQ record looks like: which headers carry the rule name and the error. [V] To keep
the key on DLQ records, add `--reader-property
key.serializer=io.confluent.kafka.serializers.WrapperKeySerializer --reader-property
wrapped.key.serializer=org.apache.kafka.common.serialization.StringSerializer` to `produce`. [V]

Rules run in the client, so they only protect you from clients that use the Confluent serializer
with rule execution. Broker validation guarantees *that* there is a schema ID; the contract checks
*what* is in the record. Leave validation off on `$T.dlq`: DLQ records may not carry a valid ID.

## C3. Optional — a breaking change on the same topic (compatibility group + migration rules)

v2 renames `recordType` → `usageType`. Without the compatibility group this is rejected; with
`compatibilityGroup=major_version` and `major_version=2` it is checked only against other major-2
schemas (none yet).

```bash
sed 's/"name":"recordType"/"name":"usageType"/' $LAB/work/call-record-v1.avsc > $LAB/work/call-record-v2.avsc
cat > $LAB/work/cr-v2-rules.json <<'EOF'
{"migrationRules":[
 {"name":"recordTypeToUsageType","kind":"TRANSFORM","type":"JSONATA","mode":"UPGRADE",
  "expr":"$merge([$sift($, function($v, $k) {$k != 'recordType'}), {'usageType': $.'recordType'}])"},
 {"name":"usageTypeToRecordType","kind":"TRANSFORM","type":"JSONATA","mode":"DOWNGRADE",
  "expr":"$merge([$sift($, function($v, $k) {$k != 'usageType'}), {'recordType': $.'usageType'}])"}
]}
EOF
jq -n --rawfile s $LAB/work/call-record-v2.avsc --slurpfile rs $LAB/work/cr-v2-rules.json \
  '{schemaType:"AVRO", schema:$s,
    metadata:{properties:{owner:"billing-team",classification:"C1",major_version:"2",sla:"tier-1"}},
    ruleSet:$rs[0]}' > $LAB/work/cr-v2-register.json
curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  --data @$LAB/work/cr-v2-register.json localhost:8081/subjects/$S/versions | jq    # {"id": 3}

# A consumer pinned to major version 1 reads everything as v1 (v2 records are downgraded)
sra kafka-avro-console-consumer --bootstrap-server dc-a:29092 --topic $T --from-beginning \
  --formatter-property schema.registry.url=http://sr-a:8081 \
  --formatter-property use.latest.with.metadata=major_version=1 --timeout-ms 10000
```

To see the migration, produce one v2 record (`value.schema.id=3`, field `usageType`) and consume
pinned to 1 (shows `recordType`) and pinned to 2 (shows `usageType`). v2 has no domain rules: rule
sets are per version, so copy them into v2 in real life, or use `defaultRuleSet`. [V: console
consumer with `use.latest.with.metadata`]

---

# Part D — Schema Linking to Confluent Cloud (cc-basic)

## D1. Into a context, `onprem`

```bash
cd $LAB/terraform
SR_URL=$(terraform output -raw schema_registry_rest_endpoint)
SR_KEY=$(terraform output -raw sa_sr_api_key); SR_SECRET=$(terraform output -raw sa_sr_api_secret)

jq -n --arg url "$SR_URL" --arg ui "$SR_KEY:$SR_SECRET" '{
  name: "to-cc", subjects: ["billing.call-record.created.v1-value"],
  contextType: "CUSTOM", context: "onprem",
  config: {"schema.registry.url": $url, "basic.auth.credentials.source": "USER_INFO", "basic.auth.user.info": $ui}
}' > $LAB/work/exporter-to-cc.json

curl -s -X POST -H 'Content-Type: application/json' localhost:8081/exporters --data @$LAB/work/exporter-to-cc.json | jq
sleep 10
curl -s localhost:8081/exporters/to-cc/status | jq
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects?subjectPrefix=:.onprem:" | jq
curl -s -u "$SR_KEY:$SR_SECRET" "$SR_URL/subjects/:.onprem:$S/versions/latest" | jq '{id, version, metadata}'
```

Expected: exporter `RUNNING`; subject `:.onprem:billing.call-record.created.v1-value` in Confluent
Cloud with the **same ID** as on `sr-a`. The exporter connection is outbound HTTPS from the
container: the same direction rule as the cluster link. Essentials allows 10 exporters per
environment. Rules are copied as data, but Confluent Cloud only *runs* them in clients if the
environment is on Advanced. [V]

## D2. Into the default context of a landing environment

The recommended shape from the notes: an environment of its own where only the on-prem registry
writes, so the schemas sit in the default context that every Confluent Cloud tool reads. An
environment with Stream Governance Essentials costs nothing.

```bash
confluent environment create crash-course-landing --governance-package essentials -o json | tee /tmp/land.json
LAND_ENV=$(jq -r .id /tmp/land.json)
confluent schema-registry cluster describe --environment $LAND_ENV -o json | tee /tmp/land-sr.json
LAND_SR_URL=$(jq -r .endpoint_url /tmp/land-sr.json); LAND_SR_ID=$(jq -r .cluster /tmp/land-sr.json)
confluent api-key create --resource $LAND_SR_ID --environment $LAND_ENV -o json > /tmp/land-sr-key.json
LK=$(jq -r .api_key /tmp/land-sr-key.json); LS=$(jq -r .api_secret /tmp/land-sr-key.json)

# nobody else writes here
curl -s -u "$LK:$LS" -X PUT -H 'Content-Type: application/json' "$LAND_SR_URL/mode?force=true" -d '{"mode":"IMPORT"}' | jq

jq -n --arg url "$LAND_SR_URL" --arg ui "$LK:$LS" '{
  name: "to-landing", subjects: ["billing.call-record.created.v1-value"],
  contextType: "DEFAULT",
  config: {"schema.registry.url": $url, "basic.auth.credentials.source": "USER_INFO", "basic.auth.user.info": $ui}
}' > $LAB/work/exporter-to-landing.json
curl -s -X POST -H 'Content-Type: application/json' localhost:8081/exporters --data @$LAB/work/exporter-to-landing.json | jq
sleep 10
curl -s localhost:8081/exporters/to-landing/status | jq
curl -s -u "$LK:$LS" "$LAND_SR_URL/subjects" | jq                           # no context prefix
curl -s -u "$LK:$LS" "$LAND_SR_URL/subjects/$S/versions/latest" | jq '{id, version}'
```

Expected: `billing.call-record.created.v1-value` in the landing registry's default context, with the
same ID as on `sr-a`, and the registry in `IMPORT` mode. [V: `--governance-package` flag, the
`describe` field names, `contextType: DEFAULT`, `mode?force=true` on Essentials]

In production the cluster with the mirror topics lives in this environment too, so the Console,
Flink and Tableflow find the schemas. The lab keeps its one paid cluster in `crash-course`, so
part E shows the trap rather than the fix.

---

# Part E — Source-initiated link dc-a → Confluent Cloud (paid-session)

## Why this works from a laptop

`dc-a` is a container behind Docker's NAT and your home router. Nothing on the internet can open a
connection to it. A source-initiated link needs exactly that: `dc-a`'s broker opens TLS connections
**out** to `pkc-….westeurope.azure.confluent.cloud:9092`, and Confluent Cloud sends its fetch
requests back over those connections. Requirements on your side: outbound TCP 9092 (and 443 for the
CLI and REST) allowed, and no proxy that breaks long-lived TCP. Corporate networks often block 9092;
use a home network.

The lab uses a **Dedicated** cluster with a public endpoint because your laptop can't reach a
private one. In production the destination is a privately networked Enterprise or Dedicated
cluster (module 03).

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

## E1. Cloud half first

```bash
cat > $LAB/work/hybrid-dst.properties <<'EOF'
link.mode=DESTINATION
connection.mode=INBOUND
mirror.start.offset.spec=latest
EOF
confluent kafka link create from-dc-a --cluster $CC_ID --source-cluster $DCA_ID \
  --config $LAB/work/hybrid-dst.properties
confluent kafka link list --cluster $CC_ID
```

Expected: `Created cluster link "from-dc-a".` Offset sync and ACL sync are off by default. Every
link setting goes in **this** file, never on the CP half.

## E2. On-prem half second, with a key of its own

The link gets its own API key, so rotating it later touches nothing else. [V: a user-owned key is
fine for a lab; production uses a service account with the `ClusterLinkConnection` role or ACLs]

```bash
confluent api-key create --resource $CC_ID -o json > /tmp/link-key.json
LINK_KEY=$(jq -r .api_key /tmp/link-key.json); LINK_SECRET=$(jq -r .api_secret /tmp/link-key.json)

cat > $LAB/work/hybrid-src.properties <<EOF
link.mode=SOURCE
connection.mode=OUTBOUND
bootstrap.servers=$CC_BOOT
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$LINK_KEY" password="$LINK_SECRET";
ssl.endpoint.identification.algorithm=https
local.listener.name=PLAINTEXT
local.security.protocol=PLAINTEXT
EOF
a kafka-cluster-links --bootstrap-server dc-a:29092 --create --link from-dc-a \
  --config-file /work/hybrid-src.properties --cluster-id $CC_ID
a kafka-cluster-links --bootstrap-server dc-a:29092 --list
```

Expected: `Cluster link 'from-dc-a' creation successfully completed.`; `--list` shows the remote
cluster `lkc-…`. A new API key can take a minute before it works; retry once if the link is not
available yet.

## E3. Mirror topic in Confluent Cloud

```bash
confluent kafka mirror create $T --link from-dc-a --cluster $CC_ID
for i in 21 22 23; do cdr $i DATA 312; done | produce
confluent kafka mirror describe $T --link from-dc-a --cluster $CC_ID
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T
```

Expected: mirror `ACTIVE`, lag 0, the three new records at the same offsets as on `dc-a`.

> **In the video:** take 3 shows the mirror topic in the Cloud Console, with its link and lag.
> Take a screenshot of the topic page.

## E4. Cloud consumer decodes on-prem schema IDs (needs D1)

```bash
cat > $LAB/work/cc-client.properties <<EOF
bootstrap.servers=$CC_BOOT
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$CC_KEY" password="$CC_SECRET";
EOF
sra kafka-avro-console-consumer --bootstrap-server $CC_BOOT --consumer.config /work/cc-client.properties \
  --topic $T --from-beginning --timeout-ms 20000 \
  --formatter-property schema.registry.url=$SR_URL/contexts/.onprem \
  --formatter-property basic.auth.credentials.source=USER_INFO --formatter-property basic.auth.user.info=$SR_KEY:$SR_SECRET
```

Expected: the records decode. Then open the topic in the Cloud Console message viewer. Per the docs
the viewer looks in the **default** context, so the values are not decoded there. That is the
landing-environment argument (D2), on screen. [V]

> **In the video:** take 7 shows the undecoded values in the Console. Take a screenshot.

## E5. Break the link with a rotated key, then fix it

Someone "rotates" the link's key the wrong way: they delete it first.

```bash
confluent api-key delete $LINK_KEY --force
sleep 60
for i in 31 32; do cdr $i VOICE 45; done | produce          # billing on dc-a doesn't notice
confluent kafka mirror describe $T --link from-dc-a --cluster $CC_ID
a kafka-cluster-links --bootstrap-server dc-a:29092 --list
```

Expected: producing on `dc-a` works, but records 31 and 32 never reach the cloud. The link reports
authentication errors and the mirror stops advancing. [V: exact state, `SOURCE_UNAVAILABLE` per the
notes] Fix it with a new key on the on-prem half:

```bash
confluent api-key create --resource $CC_ID -o json > /tmp/link-key2.json
cat > $LAB/work/hybrid-src-key2.properties <<EOF
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$(jq -r .api_key /tmp/link-key2.json)" password="$(jq -r .api_secret /tmp/link-key2.json)";
EOF
a kafka-configs --bootstrap-server dc-a:29092 --alter --cluster-link from-dc-a \
  --add-config-file /work/hybrid-src-key2.properties
sleep 60; confluent kafka mirror describe $T --link from-dc-a --cluster $CC_ID
```

Expected: records 31 and 32 arrive and the mirror is `ACTIVE` again. The right order is: create the
new key, update the link, check that it is `ACTIVE`, then delete the old key. And alert on link
authentication errors, because nothing on the on-prem side fails.

## E6. Broker-side validation in Confluent Cloud (Dedicated only)

```bash
V=billing.call-record.validated.v1
confluent kafka topic create $V --cluster $CC_ID --partitions 1 --config confluent.value.schema.validation=true
echo '{"recordId":"CDR-1001640"}' | confluent kafka topic produce $V --cluster $CC_ID          # rejected
confluent kafka topic produce $V --cluster $CC_ID --value-format avro \
  --schema $LAB/work/call-record-v1.avsc --schema-registry-endpoint $SR_URL \
  --schema-registry-api-key $SR_KEY --schema-registry-api-secret $SR_SECRET
# then type: {"recordId":"CDR-1001641","recordType":"VOICE","cellId":"CELL-0412","durationSec":61,"startTime":1762499000000,"eventTime":1762499061000}
```

Expected: plain JSON rejected; Avro accepted (the CLI registers `billing.call-record.validated.v1-value`
in the default context). The same `topic create … --config confluent.value.schema.validation=true`
on `cc-basic` is refused. [V: exact CLI flags for produce]

---

# Part F — Same end state as code

## F1. Terraform, Confluent Cloud side (save as `$LAB/work/tf-01/main.tf`)

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
  config = {
    "mirror.start.offset.spec" = "latest"
  }

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

# The allow-list: one resource per mirrored topic, changed by pull request.
resource "confluent_kafka_mirror_topic" "call_record" {
  source_kafka_topic {
    topic_name = "billing.call-record.created.v1"
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
  topic_name       = "billing.call-record.validated.v1"
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
  subject_name        = "billing.call-record.validated.v1-value"
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
# resource "confluent_schema" "call_record" {
#   schema_registry_cluster { id = data.confluent_schema_registry_cluster.adv.id }
#   rest_endpoint = data.confluent_schema_registry_cluster.adv.rest_endpoint
#   subject_name  = "billing.call-record.validated.v1-value"
#   format        = "AVRO"
#   schema        = file("../call-record-v1.avsc")
#   metadata {
#     properties = { owner = "billing-team", classification = "C1", major_version = "1" }
#   }
#   ruleset {
#     domain_rules {
#       name       = "recordIdFormat"
#       kind       = "CONDITION"
#       type       = "CEL"
#       mode       = "WRITE"
#       expr       = "message.recordId.matches('^CDR-[0-9]{7}$')"
#       on_failure = "DLQ"
#       params     = { "dlq.topic" = "billing.call-record.validated.v1.dlq" }
#     }
#   }
#   credentials { key = "<adv SR key>" secret = "<adv SR secret>" }
# }
```

```bash
cd $LAB/work/tf-01 && terraform init
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
        secretRef: cc-paid-link-credentials    # the link's own CC API key/secret
    tls:
      enabled: true                            # [V] CA handling for CC's public certificate
  sourceKafkaCluster:
    kafkaRestClassRef:
      name: dc-a-rest
  configs:
    local.security.protocol: SASL_SSL          # authenticated local listener in production
---
# Schema exporter from the on-prem registry to the CC landing environment's registry
apiVersion: platform.confluent.io/v1beta1
kind: SchemaExporter
metadata:
  name: to-landing
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
    - billing.call-record.created.v1-value
  contextName: onprem        # or, for a landing environment: contextType DEFAULT [V: CFK accepts DEFAULT?]
```

Broker-side validation on CP is a topic config: in CFK, a `KafkaTopic` CR with
`configs: {confluent.value.schema.validation: "true"}`, plus `confluent.schema.registry.url` in the
Kafka CR's `configOverrides.server`. The quota on the link principal is a `KafkaUser`-level client
quota or a `kafka-configs` call from the platform pipeline. [V: CFK quota resource]

---

# What you saw

- The data centre opened the connection; the cloud half held every setting and no address of the
  data centre (A2, A3, E1, E2).
- The mirror is the allow-list. It started at the latest offset and still kept every offset (A4).
- A quota on the link principal turned a 4 MB catch-up from one second into minutes (A5).
- Mirrored bytes carry a schema ID only. Without Schema Linking the cloud reads the wrong schema;
  with it, the IDs match and the cloud copy is read-only for schemas too (B).
- Broker validation let a negative call duration through; the contract stopped it in the producer
  (C).
- A named context works for your own clients and hides the data from the Console; a landing
  environment's default context doesn't (D, E4).
- Deleting the link's key stopped the copy while nothing failed on-prem (E5).

# Verification checklist

- [ ] A3: both halves `ACTIVE`; dc-b shows `remote.link.connection.mode=OUTBOUND`.
- [ ] A4: equal end offsets, no history on dc-b, new records at dc-a's offsets.
- [ ] A5: catch-up throttled with the quota, done within seconds without it.
- [ ] B2: decoding with `sr-b`'s default context fails or garbles (collision on ID 1).
- [ ] B3/B4: `.dc-a` context has the same ID, IMPORT mode, writes refused; decoding via
  `/contexts/.dc-a` works.
- [ ] C1: plain text rejected; valid record, negative duration and tombstone accepted.
- [ ] C2: contract registered; bad id → DLQ; negative duration → producer error.
- [ ] D1: `:.onprem:` subject in Confluent Cloud with the on-prem ID.
- [ ] D2: subject in the landing registry's default context, same ID, IMPORT mode.
- [ ] E3: mirror `ACTIVE` in `cc-paid`, offsets equal to `dc-a`.
- [ ] E4: cloud consumer decodes with `/contexts/.onprem`; the Console viewer does not.
- [ ] E5: link stops after the key is deleted and recovers with the new key.
- [ ] E6: Confluent Cloud validation rejects plain JSON on Dedicated.

# Teardown

```bash
# paid-session first (money)
confluent kafka mirror failover $T --link from-dc-a --cluster $CC_ID     # or delete the mirror topic
confluent kafka topic delete $T --cluster $CC_ID --force
confluent kafka topic delete billing.call-record.validated.v1 --cluster $CC_ID --force
confluent kafka link delete from-dc-a --cluster $CC_ID
a kafka-cluster-links --bootstrap-server dc-a:29092 --delete --link from-dc-a
confluent api-key delete $(jq -r .api_key /tmp/link-key2.json) --force
cd $LAB/work/tf-01 && terraform destroy                                   # if you used F1
cd $LAB/terraform && terraform apply -var enable_paid_dedicated=false

# cc-basic exporter target (keep the environment for other modules) and the landing environment
curl -s -X PUT localhost:8081/exporters/to-cc/pause && curl -s -X DELETE localhost:8081/exporters/to-cc
curl -s -u "$SR_KEY:$SR_SECRET" -X DELETE "$SR_URL/subjects/:.onprem:$S"   # soft delete; add ?permanent=true after
curl -s -X PUT localhost:8081/exporters/to-landing/pause && curl -s -X DELETE localhost:8081/exporters/to-landing
confluent environment delete $LAND_ENV --force

# local
( cd "$LAB" && docker compose down -v )
```

`--force` flags and delete order: [V] on CLI v4.82.

# Cost

A–C free. D: a few schemas in Essentials (100 free per environment), and the landing environment
is free. E/F: Dedicated cluster hours, one link and its bytes; destroy the same day and check the
Billing page. [V]

# Untested / check on first run

- Part A was run on CP 8.4.0 on 2026-11-14 (link halves, start offset, quota). Everything else
  is untested.
- `sr-b` decoding with a colliding ID (B2): error or silent garbage.
- `schema-exporter` binary in `cp-schema-registry:8.4.0`.
- Rule execution in `kafka-avro-console-producer` with `value.schema.id` + `use.latest.version`;
  `dlq.auto.flush`; DLQ headers; `WrapperKeySerializer` via `--reader-property`.
- Whether registering the same schema with metadata and rules gives a new ID (C2).
- `use.latest.with.metadata` in the console consumer (C3).
- D2: `confluent environment create --governance-package`, the `describe` fields, `contextType:
  DEFAULT`, IMPORT mode on a fresh Essentials registry.
- PLAINTEXT `local.*` settings against Confluent Cloud (E2); Console viewer with contexts (E4);
  link state after the key is deleted (E5); `confluent kafka topic produce` flags (E6).
- Terraform F1 placeholder `bootstrap_endpoint` and `config` block; `compatibility_group` on
  Essentials; CFK TLS block for Confluent Cloud.
