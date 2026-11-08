---
title: "01 — Lab: Cluster Linking DR between two DCs, failback, and MirrorMaker 2 for comparison"
subtitle: Environment local (Docker); Terraform part needs paid-session. Written 2026-11-07, untested.
---

# Goal

See with your own eyes what the notes claim:

1. A mirror topic has the **same offsets** as the source, and consumer offsets follow.
2. **Failover** makes the DR copy writable; the group resumes where it was.
3. **Failback** on a bidirectional link: `truncate-and-restore` drops divergent records,
   `reverse-and-start` restores the original direction.
4. **MirrorMaker 2** gives prefixed topics, **different offsets** and translated group offsets.
5. The same end state as code: CFK custom resources (on-prem/OpenShift) and Terraform (Confluent Cloud).

| | |
|---|---|
| Environment | `local` for parts A–C; `paid-session` (`cc-paid`) only for part D2 |
| Time | ~90 min for A–C, 30 min for D |
| Topics | `orders.order-status.updated.v1`, `tracking.item-scan.recorded.v1`, `sites.queue-length.measured.v1` |

# Prerequisites

> Console consumers started with `--timeout-ms` end with a `TimeoutException` message when no more
> records arrive. That is normal and not an error in the lab.

- `00-lab-setup` running: `docker compose --profile mm2 up -d`, smoke test passed.
- Helper functions from `00-lab-setup/README.md` loaded (`a`, `b`, `DCA_ID`, `DCB_ID`, `LAB`).
- `jq`.

All commands run on the laptop; Kafka tools run inside the containers via `a …` / `b …`. Files go
to `$LAB/work/` = `/work/` in the containers.

---

# Part A — One-way DR link dc-a → dc-b, failover

## A1. Source topic, data, one consumer group

```bash
T=orders.order-status.updated.v1
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T --partitions 3 --replication-factor 1

# 60 keyed records (key = order ID, so one order stays in one partition)
for i in $(seq 1 60); do o="SO-$((1001600 + i % 7))"; echo "$o|{\"orderId\":\"$o\",\"status\":\"READY\",\"seq\":$i}"; done \
 | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T \
     --property parse.key=true --property key.separator='|'

# group ops-dashboard reads everything and commits 60
a kafka-console-consumer --bootstrap-server dc-a:29092 --topic $T --group ops-dashboard \
  --from-beginning --timeout-ms 10000 > /dev/null

# 40 more records the group has NOT read yet
for i in $(seq 61 100); do o="SO-$((1001600 + i % 7))"; echo "$o|{\"orderId\":\"$o\",\"status\":\"DISPATCHED\",\"seq\":$i}"; done \
 | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T \
     --property parse.key=true --property key.separator='|'

a kafka-consumer-groups --bootstrap-server dc-a:29092 --describe --group ops-dashboard
```

Expected: three partitions; CURRENT-OFFSET sums to 60, LOG-END-OFFSET sums to 100, LAG sums to 40.

## A2. Create the link on dc-b (destination-initiated: dc-b pulls from dc-a)

```bash
cat > $LAB/work/dr-link.properties <<'EOF'
bootstrap.servers=dc-a:29092
consumer.offset.sync.enable=true
consumer.offset.sync.ms=5000
consumer.offset.group.filters={"groupFilters":[{"name":"ops-dashboard","patternType":"LITERAL","filterType":"INCLUDE"}]}
EOF

b kafka-cluster-links --bootstrap-server dc-b:29092 --create --link dr-link \
  --config-file /work/dr-link.properties
b kafka-cluster-links --bootstrap-server dc-b:29092 --list
b kafka-configs --bootstrap-server dc-b:29092 --describe --cluster-link dr-link
```

Expected: `Cluster link 'dr-link' creation successfully completed.`; `--list` shows link name, link
ID, remote cluster ID `Q0PvpgzYSwirapvmRY0NFg`; `--describe` shows the configs above
(`sensitive=false synonyms={...}`). The offset sync interval is 5 s here only to make the lab
fast; the default is 30 s.

## A3. Mirror topic, same offsets

```bash
b kafka-mirrors --create --mirror-topic $T --link dr-link --bootstrap-server dc-b:29092
b kafka-mirrors --describe --links dr-link --bootstrap-server dc-b:29092
```

Expected: `State: ACTIVE`; per partition `DestLogEndOffset` equal to `LastFetchSourceHighWatermark`,
`Lag: 0`.

```bash
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T
b kafka-get-offsets --bootstrap-server dc-b:29092 --topic $T
# the same record, by partition + offset, on both sides
for h in a b; do $h kafka-console-consumer --bootstrap-server dc-$h:29092 --topic $T \
   --partition 0 --offset 5 --max-messages 1 --property print.offset=true --property print.key=true; done
```

Expected: identical end offsets per partition; the record at partition 0 offset 5 is the same
key and value on both clusters. (With MM2 in part C this is *not* guaranteed.)

Wait ~10 s (2 × sync interval), then:

```bash
b kafka-consumer-groups --bootstrap-server dc-b:29092 --describe --group ops-dashboard
```

Expected: same CURRENT-OFFSET per partition as on dc-a (sum 60), no active members.

## A4. The mirror is read-only

```bash
echo 'SO-1001601|{"seq":999}' | b kafka-console-producer --bootstrap-server dc-b:29092 --topic $T \
  --property parse.key=true --property key.separator='|'
```

Expected: the produce fails with an error that the topic is a read-only mirror topic (exact
text differs by version). [V]

## A5. Disaster: lose dc-a, fail over

```bash
docker stop dc-a        # sr-a will log connection errors; ignore
b kafka-mirrors --describe --links dr-link --bootstrap-server dc-b:29092   # SOURCE_UNAVAILABLE (may take a while)

b kafka-mirrors --failover --topics $T --bootstrap-server dc-b:29092 --validate-only
b kafka-mirrors --failover --topics $T --bootstrap-server dc-b:29092
b kafka-mirrors --describe --links dr-link --bootstrap-server dc-b:29092   # State: STOPPED
```

Now dc-b is primary for this topic. The group continues:

```bash
b kafka-console-consumer --bootstrap-server dc-b:29092 --topic $T --group ops-dashboard \
  --timeout-ms 10000 --property print.offset=true | wc -l
```

Expected: **40** lines (seq 61–100). If the group had committed after the last sync, you would see
a few more (re-read). Promote would have refused here, because promote needs the source online.

Produce works now:

```bash
echo 'SO-1001601|{"orderId":"SO-1001601","status":"DELAYED","seq":101}' | b kafka-console-producer \
  --bootstrap-server dc-b:29092 --topic $T --property parse.key=true --property key.separator='|'
```

## A6. Why this link cannot fail back

`dr-link` was created with the default `link.mode=DESTINATION`. `truncate-and-restore` and
`reverse-and-start` need `link.mode=BIDIRECTIONAL`, and the mode cannot be changed. To go back you
would rebuild a link in the other direction and recreate topics. Clean up and do it properly in B:

```bash
docker start dc-a && sleep 20
b kafka-cluster-links --bootstrap-server dc-b:29092 --delete --link dr-link   # mirror is STOPPED, so allowed
b kafka-topics --bootstrap-server dc-b:29092 --delete --topic $T
a kafka-topics --bootstrap-server dc-a:29092 --delete --topic $T
```

---

# Part B — Bidirectional link: failover, truncate-and-restore, reverse-and-start

Requirements met by this lab: both clusters CP ≥ 7.7, KRaft, no `cluster.link.prefix`, unclean
leader election off (default). Both link halves use the **same name**.

## B1. Topic, link on both sides, mirror on dc-b

```bash
T=tracking.item-scan.recorded.v1
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T --partitions 3 --replication-factor 1

cat > $LAB/work/bidi-on-b.properties <<'EOF'
link.mode=BIDIRECTIONAL
bootstrap.servers=dc-a:29092
consumer.offset.sync.enable=true
consumer.offset.sync.ms=5000
consumer.offset.group.filters={"groupFilters":[{"name":"item-sorter","patternType":"LITERAL","filterType":"INCLUDE","topicTypes":["LOCAL_MIRROR"]}]}
EOF
cat > $LAB/work/bidi-on-a.properties <<'EOF'
link.mode=BIDIRECTIONAL
bootstrap.servers=dc-b:29092
EOF

b kafka-cluster-links --bootstrap-server dc-b:29092 --create --link bidi-link --config-file /work/bidi-on-b.properties
a kafka-cluster-links --bootstrap-server dc-a:29092 --create --link bidi-link --config-file /work/bidi-on-a.properties
b kafka-mirrors --create --mirror-topic $T --link bidi-link --bootstrap-server dc-b:29092
```

`LOCAL_MIRROR` only = active/passive best practice: dc-b copies offsets for topics that are
mirrored *on dc-b*, nothing flows back.

## B2. Normal operation

```bash
for i in $(seq 1 50); do item="IT-$((100000 + i))"; echo "$item|{\"itemTag\":\"$item\",\"scanner\":\"S12\",\"seq\":$i}"; done \
 | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T --property parse.key=true --property key.separator='|'
a kafka-console-consumer --bootstrap-server dc-a:29092 --topic $T --group item-sorter --from-beginning --timeout-ms 10000 > /dev/null
sleep 10
b kafka-consumer-groups --bootstrap-server dc-b:29092 --describe --group item-sorter   # sum 50
```

## B3. Create divergence, then lose dc-a

Pause the mirror so the next 5 records stay only on dc-a (simulates replication lag at the moment
of the outage):

```bash
b kafka-mirrors --pause --topics $T --bootstrap-server dc-b:29092
for i in $(seq 51 55); do item="IT-$((100000 + i))"; echo "$item|{\"itemTag\":\"$item\",\"note\":\"DIVERGENT\",\"seq\":$i}"; done \
 | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T --property parse.key=true --property key.separator='|'
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T     # sum 55
b kafka-get-offsets --bootstrap-server dc-b:29092 --topic $T     # sum 50

docker stop dc-a
b kafka-mirrors --failover --topics $T --bootstrap-server dc-b:29092
for i in $(seq 101 110); do item="IT-$((100000 + i))"; echo "$item|{\"itemTag\":\"$item\",\"note\":\"WRITTEN-IN-DR\",\"seq\":$i}"; done \
 | b kafka-console-producer --bootstrap-server dc-b:29092 --topic $T --property parse.key=true --property key.separator='|'
b kafka-console-consumer --bootstrap-server dc-b:29092 --topic $T --group item-sorter --timeout-ms 10000 | wc -l   # 10
```

State now: dc-b has 60 records (1–50 + 101–110) and is primary. dc-a (down) has 55 (1–55); 51–55
exist nowhere else.

## B4. dc-a is back: truncate-and-restore on dc-a

The CP REST API v3 is used here because it exposes every mirror action. (Check whether your
`kafka-mirrors` has the flags too: `a kafka-mirrors --help | grep -i -E 'reverse|truncate'`.)

```bash
docker start dc-a && sleep 20

# dry run, with per-partition truncation counts
curl -s -X POST -H 'Content-Type: application/json' \
  "localhost:8090/kafka/v3/clusters/$DCA_ID/links/bidi-link/mirrors:truncate-and-restore?validate_only=true&include_partition_level_truncation_data=true" \
  -d "{\"mirror_topic_names\":[\"$T\"]}" | jq

# for real
curl -s -X POST -H 'Content-Type: application/json' \
  "localhost:8090/kafka/v3/clusters/$DCA_ID/links/bidi-link/mirrors:truncate-and-restore?include_partition_level_truncation_data=true" \
  -d "{\"mirror_topic_names\":[\"$T\"]}" | jq

a kafka-mirrors --describe --links bidi-link --bootstrap-server dc-a:29092
```

Expected: truncation data shows **5 records** removed in total; the dc-a topic moves
`PENDING_SETUP_FOR_RESTORE` → `PENDING_RESTORE_MIRROR` → `ACTIVE` (it is now a mirror of dc-b). Do
not consume from it while it is in the pending states (offset clamping). Then:

```bash
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T     # sum 60, same as dc-b
a kafka-console-consumer --bootstrap-server dc-a:29092 --topic $T --from-beginning --timeout-ms 10000 \
  | grep -c DIVERGENT                                             # 0
```

**Records 51–55 are gone.** In production you would first read that range from dc-a by offset and
hand it to the owning team, or replay from the source system.

## B5. Fail back: reverse-and-start on dc-a

Give dc-a's link half an offset filter (LOCAL_MIRROR: dc-a is the mirror right now), move the
consumers off dc-b (ours have exited), then reverse:

```bash
cat > $LAB/work/bidi-on-a-offsets.properties <<'EOF'
consumer.offset.sync.enable=true
consumer.offset.sync.ms=5000
consumer.offset.group.filters={"groupFilters":[{"name":"item-sorter","patternType":"LITERAL","filterType":"INCLUDE","topicTypes":["LOCAL_MIRROR"]}]}
EOF
a kafka-configs --bootstrap-server dc-a:29092 --alter --cluster-link bidi-link --add-config-file /work/bidi-on-a-offsets.properties
sleep 10

curl -s -X POST -H 'Content-Type: application/json' \
  "localhost:8090/kafka/v3/clusters/$DCA_ID/links/bidi-link/mirrors:reverse-and-start-mirror?validate_only=true" \
  -d "{\"mirror_topic_names\":[\"$T\"]}" | jq
curl -s -X POST -H 'Content-Type: application/json' \
  "localhost:8090/kafka/v3/clusters/$DCA_ID/links/bidi-link/mirrors:reverse-and-start-mirror" \
  -d "{\"mirror_topic_names\":[\"$T\"]}" | jq

a kafka-mirrors --describe --links bidi-link --bootstrap-server dc-a:29092   # PENDING_SYNCHRONIZE -> STOPPED (writable source)
b kafka-mirrors --describe --links bidi-link --bootstrap-server dc-b:29092   # PENDING_MIRROR -> ACTIVE (mirror again)
a kafka-consumer-groups --bootstrap-server dc-a:29092 --describe --group item-sorter   # sum 60, lag 0
```

Expected: produce to dc-a works, produce to dc-b fails (read-only), `item-sorter` on dc-a resumes
at 60. If the topic sticks in `PENDING_STOPPED`, a consumer group was still active on the other
side; the documented escape is `failover` on the stuck topic (some records re-read).

```bash
curl -s "localhost:8090/kafka/v3/clusters/$DCA_ID/links/bidi-link/mirrors/$T?include_state_transition_errors=true" | jq
```

shows state-transition errors if a step hangs.

---

# Part C — MirrorMaker 2 for comparison

## C1. Source topic with a moved log start offset

```bash
T=sites.queue-length.measured.v1
a kafka-topics --bootstrap-server dc-a:29092 --create --topic $T --partitions 1 --replication-factor 1
for i in $(seq 1 30); do echo "{\"site\":\"SITE-$((i % 4))\",\"queueMinutes\":$((i % 17)),\"seq\":$i}"; done \
 | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T

# Simulate retention: delete the first 10 records (log start offset -> 10)
echo "{\"partitions\":[{\"topic\":\"$T\",\"partition\":0,\"offset\":10}],\"version\":1}" > $LAB/work/delete-10.json
a kafka-delete-records --bootstrap-server dc-a:29092 --offset-json-file /work/delete-10.json

a kafka-console-consumer --bootstrap-server dc-a:29092 --topic $T --group sites-ui --from-beginning --timeout-ms 10000 > /dev/null
for i in $(seq 31 50); do echo "{\"site\":\"SITE-$((i % 4))\",\"queueMinutes\":$((i % 17)),\"seq\":$i}"; done \
 | a kafka-console-producer --bootstrap-server dc-a:29092 --topic $T
a kafka-consumer-groups --bootstrap-server dc-a:29092 --describe --group sites-ui   # CURRENT-OFFSET 30, LOG-END 50
```

## C2. Deploy the three MM2 connectors on the Connect worker (runs on dc-b)

```bash
curl -s localhost:8083/connector-plugins | jq -r '.[].class' | grep -i mirror
# expect MirrorSourceConnector, MirrorCheckpointConnector, MirrorHeartbeatConnector [V]

common='"source.cluster.alias":"dc-a","target.cluster.alias":"dc-b",
 "source.cluster.bootstrap.servers":"dc-a:29092","target.cluster.bootstrap.servers":"dc-b:29092",
 "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter",
 "value.converter":"org.apache.kafka.connect.converters.ByteArrayConverter","tasks.max":"1"'

cat > $LAB/work/mm2-source.json <<EOF
{"name":"mm2-source","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector",$common,
 "topics":"sites\\\\..*","replication.factor":"1","offset-syncs.topic.replication.factor":"1",
 "sync.topic.acls.enabled":"false","refresh.topics.interval.seconds":"10"}}
EOF
cat > $LAB/work/mm2-checkpoint.json <<EOF
{"name":"mm2-checkpoint","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorCheckpointConnector",$common,
 "groups":"sites-.*","emit.checkpoints.interval.seconds":"5",
 "sync.group.offsets.enabled":"true","sync.group.offsets.interval.seconds":"5",
 "checkpoints.topic.replication.factor":"1"}}
EOF
cat > $LAB/work/mm2-heartbeat.json <<EOF
{"name":"mm2-heartbeat","config":{"connector.class":"org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",$common,
 "heartbeats.topic.replication.factor":"1","emit.heartbeats.interval.seconds":"5"}}
EOF

for c in source checkpoint heartbeat; do
  curl -s -X POST -H 'Content-Type: application/json' --data @$LAB/work/mm2-$c.json localhost:8083/connectors | jq -r .name
done
sleep 20
curl -s 'localhost:8083/connectors?expand=status' | jq -r '.[] | "\(.status.name) \(.status.connector.state)"'
```

Expected: three connectors `RUNNING`. Check the JSON escaping of `topics` with
`jq .config.topics $LAB/work/mm2-source.json` — it must read `sites\..*`.

## C3. Compare with Cluster Linking

```bash
b kafka-topics --bootstrap-server dc-b:29092 --list | grep -E 'sites|checkpoints|heartbeats'
a kafka-topics --bootstrap-server dc-a:29092 --list | grep offset-syncs
a kafka-get-offsets --bootstrap-server dc-a:29092 --topic $T
b kafka-get-offsets --bootstrap-server dc-b:29092 --topic dc-a.$T
b kafka-console-consumer --bootstrap-server dc-b:29092 --topic dc-a.$T --partition 0 --offset 0 \
  --max-messages 1 --property print.offset=true
b kafka-consumer-groups --bootstrap-server dc-b:29092 --describe --group sites-ui
```

Expected:

| Check | Cluster Linking (part A) | MM2 (here) |
|---|---|---|
| Target topic name | `orders.order-status.updated.v1` | **`dc-a.sites.queue-length.measured.v1`** |
| Internal topics | none | `mm2-offset-syncs.dc-b.internal` on dc-a; `dc-a.checkpoints.internal`, `heartbeats` on dc-b |
| End offset | same as source | **40** on dc-b vs **50** on dc-a |
| Record at offset 0 | n/a | `seq 11` (source offset 10) — offsets shifted |
| Group offset on target | identical (sum 60) | translated: **≤ 20** (source 30 − 10). Lower than 20 = conservative translation → re-reads [V] |

## C4. "Fail over" a consumer with MM2

```bash
b kafka-console-consumer --bootstrap-server dc-b:29092 --topic dc-a.$T --group sites-ui --timeout-ms 10000 | wc -l
```

Expected: 20 if translation was exact, more if not (duplicates). Note the client had to change
the **topic name** as well as the bootstrap. That is the operational cost the notes talk about.

Cleanup C:

```bash
for c in source checkpoint heartbeat; do curl -s -X DELETE localhost:8083/connectors/mm2-$c; done
```

---

# Part D — The same end state as code

The `confluentinc/confluent` Terraform provider manages **Confluent Cloud** only. On-prem links are
managed by **CFK** (`ClusterLink` CRs) on Kubernetes/OpenShift, or by the CLI/REST calls above
(Ansible). Two parts:

## D1. CFK custom resources for part B (on-prem, GitOps with ArgoCD) — not runnable on Docker

Assumes CFK manages `kafka-dc-a` (namespace `dc-a`) and `kafka-dc-b` (namespace `dc-b`), each
with a `KafkaRestClass` and a password encoder secret. Based on the CFK bidirectional example.

```yaml
# Lives in dc-b: receives the forward mirror (dc-a -> dc-b)
apiVersion: platform.confluent.io/v1beta1
kind: ClusterLink
metadata:
  name: bidi-link-on-dc-b
  namespace: dc-b
spec:
  name: bidi-link                    # same logical name on both CRs
  sourceInitiatedLink:
    linkMode: Bidirectional
  destinationKafkaCluster:
    name: kafka-dc-b
    namespace: dc-b
  sourceKafkaCluster:
    name: kafka-dc-a
    namespace: dc-a
  configs:
    consumer.offset.sync.enable: "true"
    consumer.offset.sync.ms: "30000"
    consumer.offset.group.filters: '{"groupFilters":[{"name":"item-sorter","patternType":"LITERAL","filterType":"INCLUDE","topicTypes":["LOCAL_MIRROR"]}]}'
  mirrorTopics:
    - name: tracking.item-scan.recorded.v1
      direction: toDestination
---
# Lives in dc-a: the other half; no mirror topics in normal operation
apiVersion: platform.confluent.io/v1beta1
kind: ClusterLink
metadata:
  name: bidi-link-on-dc-a
  namespace: dc-a
spec:
  name: bidi-link
  sourceInitiatedLink:
    linkMode: Bidirectional
  sourceKafkaCluster:
    name: kafka-dc-b
    namespace: dc-b
  destinationKafkaCluster:
    name: kafka-dc-a
    namespace: dc-a
```

Warning from the CFK docs: deleting a `ClusterLink` CR forcibly **fails over** every mirror topic
still attached to it. In GitOps, a careless delete in Git = an unplanned failover. Protect these
CRs (ArgoCD `Prune=false`, review rules).

## D2. Terraform: the same DR pattern inside Confluent Cloud (paid-session)

`cc-basic` → `cc-paid`, destination-initiated, offset sync for `ops-dashboard`, one mirror
topic. Basic can be the *source* of a destination-initiated link; Dedicated is a valid destination.
Note: Terraform does not support the failback APIs (reverse / truncate); failover and promote are
possible by changing the mirror topic `status`.

Save as `$LAB/work/tf-01/main.tf` (reads the base state from `00-lab-setup/terraform`):

```hcl
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.94.0"
    }
  }
}

provider "confluent" {} # CONFLUENT_CLOUD_API_KEY / _SECRET from the environment

data "terraform_remote_state" "base" {
  backend = "local"
  config  = { path = "../../terraform/terraform.tfstate" }
}

locals {
  b = data.terraform_remote_state.base.outputs
}

data "confluent_kafka_cluster" "basic" {
  id = local.b.basic_cluster_id
  environment { id = local.b.environment_id }
}

data "confluent_kafka_cluster" "paid" {
  id = local.b.dedicated_cluster_id # requires enable_paid_dedicated=true in the base
  environment { id = local.b.environment_id }
}

resource "confluent_cluster_link" "cc_dr" {
  link_name = "cc-dr-link"
  link_mode = "DESTINATION"

  source_kafka_cluster {
    id                 = data.confluent_kafka_cluster.basic.id
    bootstrap_endpoint = data.confluent_kafka_cluster.basic.bootstrap_endpoint
    credentials {
      key    = local.b.sa_kafka_basic_api_key
      secret = local.b.sa_kafka_basic_api_secret
    }
  }

  destination_kafka_cluster {
    id            = data.confluent_kafka_cluster.paid.id
    rest_endpoint = data.confluent_kafka_cluster.paid.rest_endpoint
    credentials {
      key    = local.b.sa_kafka_dedicated_api_key
      secret = local.b.sa_kafka_dedicated_api_secret
    }
  }

  config = {
    "consumer.offset.sync.enable"   = "true"
    "consumer.offset.sync.ms"       = "30000"
    "consumer.offset.group.filters" = jsonencode({
      groupFilters = [{ name = "ops-dashboard", patternType = "LITERAL", filterType = "INCLUDE" }]
    })
  }
}

resource "confluent_kafka_mirror_topic" "order_status" {
  source_kafka_topic {
    topic_name = "orders.order-status.updated.v1"
  }
  cluster_link {
    link_name = confluent_cluster_link.cc_dr.link_name
  }
  kafka_cluster {
    id            = data.confluent_kafka_cluster.paid.id
    rest_endpoint = data.confluent_kafka_cluster.paid.rest_endpoint
    credentials {
      key    = local.b.sa_kafka_dedicated_api_key
      secret = local.b.sa_kafka_dedicated_api_secret
    }
  }
  # status = "FAILED_OVER"   # uncomment + apply = failover; "PROMOTED" = promote
}
```

```bash
cd $LAB/work/tf-01 && terraform init && terraform apply
confluent kafka mirror list --cluster $(terraform -chdir=$LAB/terraform output -raw dedicated_cluster_id)
confluent kafka mirror describe orders.order-status.updated.v1 --link cc-dr-link \
  --cluster $(terraform -chdir=$LAB/terraform output -raw dedicated_cluster_id)
```

Same CLI end state, for reference:

```bash
confluent kafka link create cc-dr-link --cluster <cc-paid-id> \
  --source-cluster <cc-basic-id> --source-bootstrap-server <cc-basic-bootstrap> \
  --source-api-key <basic-key> --source-api-secret <basic-secret> \
  --config consumer.offset.sync.enable=true,consumer.offset.sync.ms=30000
confluent kafka mirror create orders.order-status.updated.v1 --link cc-dr-link --cluster <cc-paid-id>
confluent kafka mirror failover orders.order-status.updated.v1 --link cc-dr-link --cluster <cc-paid-id>
```

(Group filters contain commas, so put them in a config file and pass `--config <file>`.)

---

# Verification checklist

- [ ] A3: end offsets equal on dc-a and dc-b; record at p0/o5 identical.
- [ ] A3: `ops-dashboard` offsets appear on dc-b without the group ever running there.
- [ ] A5: after failover the group reads exactly the 40 unread records.
- [ ] B4: truncate-and-restore reports 5 truncated records; `DIVERGENT` count on dc-a = 0.
- [ ] B5: dc-a `STOPPED` (writable source), dc-b `ACTIVE` (mirror); group resumes at 60.
- [ ] C3: MM2 target topic prefixed, offsets shifted by 10, group offset translated ≤ 20.
- [ ] D2: mirror topic ACTIVE in `cc-paid`.

# Teardown

```bash
# local
b kafka-cluster-links --bootstrap-server dc-b:29092 --delete --link bidi-link   # mirror topics must be STOPPED/removed first
a kafka-cluster-links --bootstrap-server dc-a:29092 --delete --link bidi-link
docker compose -f $LAB/docker-compose.yml --profile mm2 down -v                 # or keep for module 08

# paid-session
cd $LAB/work/tf-01 && terraform destroy        # if a mirror was failed over: terraform state rm the mirror first
cd $LAB/terraform && terraform apply -var enable_paid_dedicated=false
```

If `kafka-cluster-links --delete` refuses because a mirror is still `ACTIVE`, fail it over or
delete the topic first.

# Cost

Parts A–C: free (local, developer licence). Part D2: Dedicated cluster hours + one link + a few
KB — minutes of trial credit if you destroy right after. [V]

# Untested / check on first run

- Read-only produce error text (A4); time until `SOURCE_UNAVAILABLE` shows (A5).
- REST action paths `mirrors:truncate-and-restore` / `mirrors:reverse-and-start-mirror` taken from
  the CP REST API reference; behaviour on two single-node clusters not tried.
- Whether `kafka-mirrors` 8.4 has CLI flags for reverse/truncate.
- MM2 plugin availability in `cp-kafka-connect:8.4.0`; translated offset value in C3.
- Terraform D2 not planned or applied.
