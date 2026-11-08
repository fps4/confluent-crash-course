---
title: 07 — Lab — Kafka internals
subtitle: ISR shrink and expand, acks=all vs acks=1 with min.insync.replicas, simulated cross-DC latency, compaction, a KIP-848 consumer group, a share group, a controller outage, tiered storage on MinIO
---

**[V]** = check against your CP/Kafka version, image tag or CLI.

# Goal

See the internals from `notes.md` happen, on a cluster you can break:

| # | Experiment | Internal | Consequence to say out loud |
|---|---|---|---|
| E1 | Stop brokers, watch ISR shrink/expand | ISR, `replica.lag.time.max.ms`, min ISR, ELR | Why RF 3 / minISR 2 / acks=all |
| E2 | acks=1 vs acks=all, then add 20 ms delay to one follower | Purgatory waits for all ISR members | Why stretched clusters need low RTT |
| E3 | Compacted topic with tombstones | Log cleaner, active segment, `delete.retention.ms` | Current-state topics; GDPR limits |
| E4 | KIP-848 consumer group | Server-side assignment, incremental rebalance | Rolling deploys on OpenShift |
| E5 | Share group (CP 8.2+ / Kafka 4.2+) | Per-record acquisition | Work queues vs Artemis |
| E6 | Stop the controller, then a broker | KRaft quorum, leader election | Why a 3rd site for controllers |
| E7 | Tiered storage to MinIO (optional) | Segment upload, hotset | Replays off broker disks |

# Environment

`local`, but **not** the single-node `dc-a`/`dc-b` clusters from `../00-lab-setup/`: one broker
cannot show replication (RF > 1 is impossible, ISR is always just the leader). Use the 3-broker
override below. It runs **next to** `00-lab-setup` (own project name and ports) or alone.

Design of the override: **one dedicated KRaft controller + three brokers**. A single controller
is a lab shortcut (production: 3 or 5 voters); it keeps the controller alive when we stop
brokers, so ISR changes can still be recorded. E6 then stops the controller on purpose.

# Prerequisites

- Docker with ~4 GB free memory.
- `00-lab-setup` read for image conventions; this file uses the same tag (`cp-server:8.4.0`,
  Kafka 4.4). If 8.4.0 misbehaves, use the latest 8.3.x as `00-lab-setup` suggests. Share groups
  (E5) need Kafka 4.2+ [V].
- Licence: the developer licence covers **one broker per cluster**. This is a separate,
  three-broker cluster, so it starts a **30-day trial** for this cluster only (it does not touch
  `dc-a`/`dc-b`). Fine for a lab; `down -v` and start again if it expires.

`07-kafka-internals/docker-compose.3broker.yml` (create it from this snippet):

```yaml
name: internals

x-broker-env: &broker-env
  CLUSTER_ID: "q1Sh-9_ISia_zwGINzRvyQ"          # any base64 UUID, same for all nodes
  KAFKA_PROCESS_ROLES: "broker"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "100@c1:29093"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
  KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_REPLICA_LAG_TIME_MAX_MS: 10000             # 10 s instead of 30 s, so E1 is quick
  KAFKA_LOG_CLEANER_BACKOFF_MS: 5000               # cleaner checks often (E3)
  KAFKA_HEAP_OPTS: "-Xms512m -Xmx512m"

services:
  c1:
    image: confluentinc/cp-server:8.4.0            # same tag as 00-lab-setup
    hostname: c1
    environment:
      CLUSTER_ID: "q1Sh-9_ISia_zwGINzRvyQ"
      KAFKA_NODE_ID: 100
      KAFKA_PROCESS_ROLES: "controller"
      KAFKA_LISTENERS: "CONTROLLER://c1:29093"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "100@c1:29093"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx256m"

  b1:
    image: confluentinc/cp-server:8.4.0
    hostname: b1
    depends_on: [c1]
    ports: ["39092:39092"]
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: "INTERNAL://b1:29092,EXTERNAL://0.0.0.0:39092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://b1:29092,EXTERNAL://localhost:39092"

  b2:
    image: confluentinc/cp-server:8.4.0
    hostname: b2
    depends_on: [c1]
    ports: ["39093:39093"]
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: "INTERNAL://b2:29092,EXTERNAL://0.0.0.0:39093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://b2:29092,EXTERNAL://localhost:39093"

  b3:
    image: confluentinc/cp-server:8.4.0
    hostname: b3
    depends_on: [c1]
    ports: ["39094:39094"]
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: "INTERNAL://b3:29092,EXTERNAL://0.0.0.0:39094"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://b3:29092,EXTERNAL://localhost:39094"
```

Start and set helpers:

```bash
cd 07-kafka-internals
docker compose -f docker-compose.3broker.yml up -d
export K="docker exec -i internals-b1-1"           # run tools inside b1
export BS="b1:29092,b2:29092,b3:29092"
$K kafka-metadata-quorum --bootstrap-server $BS describe --status
$K kafka-broker-api-versions --bootstrap-server $BS | grep -c 'b[123]:29092'
```

Expected: quorum status with `LeaderId: 100`; `3` brokers. Ports 39092–39094 do not clash with
`00-lab-setup` (9092/9093, 8081/8082, 8083, 8090/8091).

# E1 — ISR shrink and expand

```bash
$K kafka-topics --bootstrap-server $BS --create --topic orders.order-status.updated.v1 \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
$K kafka-topics --bootstrap-server $BS --describe --topic orders.order-status.updated.v1
```

Expected: 3 partitions, each `Replicas: x,y,z  Isr: x,y,z`. Kafka 4.x also prints `Elr:` and
`LastKnownElr:` (empty) [V].

Stop one broker:

```bash
docker stop internals-b3-1
sleep 15
$K kafka-topics --bootstrap-server $BS --describe --topic orders.order-status.updated.v1
$K kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions
```

Expected: broker 3 gone from every `Isr`; partitions it led now have a new leader; all 3
partitions are under-replicated. Writes with `acks=all` still work (ISR 2 ≥ minISR 2):

```bash
$K kafka-producer-perf-test --topic orders.order-status.updated.v1 --num-records 1000 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BS acks=all
```

Stop a second broker:

```bash
docker stop internals-b2-1
sleep 15
$K kafka-topics --bootstrap-server b1:29092 --describe --under-min-isr-partitions
echo 'SO-1001234:READY' | $K kafka-console-producer --bootstrap-server b1:29092 \
  --topic orders.order-status.updated.v1 --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=all --command-property delivery.timeout.ms=15000 \
  --command-property request.timeout.ms=10000
```

Expected: all partitions under min ISR. The `acks=all` produce fails after the timeout with
`NOT_ENOUGH_REPLICAS` (or `NotEnoughReplicasException`). Now `acks=1`:

```bash
echo 'SO-1005678:DISPATCHED' | $K kafka-console-producer --bootstrap-server b1:29092 \
  --topic orders.order-status.updated.v1 --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=1
$K kafka-console-consumer --bootstrap-server b1:29092 --topic orders.order-status.updated.v1 \
  --from-beginning --formatter-property print.key=true --timeout-ms 10000 | grep -c . ; \
$K kafka-get-offsets --bootstrap-server b1:29092 --topic orders.order-status.updated.v1
```

Expected: the `acks=1` write is **accepted**. With ELR active (default on new 4.1+ clusters), the
high watermark does not advance while ISR < minISR, so the consumer **does not see** `SO-1005678`
yet, and `kafka-get-offsets` (which reports the high watermark for "latest") does not move
either [V]. Without ELR the record is visible at once, but it lives on one disk only. Note what
you saw — it is a useful detail either way.

Bring brokers back:

```bash
docker start internals-b2-1 internals-b3-1
sleep 20
$K kafka-topics --bootstrap-server $BS --describe --topic orders.order-status.updated.v1
$K kafka-console-consumer --bootstrap-server $BS --topic orders.order-status.updated.v1 \
  --from-beginning --formatter-property print.key=true --timeout-ms 10000 | grep SO-1005678
```

Expected: ISR back to 3 for all partitions (expand), leadership may stay on b1 until preferred
leader election runs (`auto.leader.rebalance.enable`, default every 5 min) or you run
`kafka-leader-election --election-type preferred --all-topic-partitions`. `SO-1005678` is now visible.

# E2 — acks and cross-DC latency

Pin one partition's leader to b1 so we control who is "remote":

```bash
$K kafka-topics --bootstrap-server $BS --create --topic tracking.item-scan.recorded.v1 \
  --replica-assignment 1:2:3 --config min.insync.replicas=2

for a in 1 all; do
  echo "acks=$a"
  $K kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 --num-records 20000 \
    --record-size 500 --throughput 2000 \
    --producer-props bootstrap.servers=$BS acks=$a linger.ms=0 | tail -1
done
```

Expected: last line per run with `avg latency`, `99th`. On a laptop both are a few ms; `acks=all`
slightly higher.

Make broker 3 "the other DC": add 20 ms one-way delay to its network (needs a helper image that
shares b3's network namespace):

```bash
docker run --rm --net container:internals-b3-1 --cap-add NET_ADMIN nicolaka/netshoot \
  tc qdisc add dev eth0 root netem delay 20ms

for a in 1 all; do
  echo "acks=$a"
  $K kafka-producer-perf-test --topic tracking.item-scan.recorded.v1 --num-records 20000 \
    --record-size 500 --throughput 2000 \
    --producer-props bootstrap.servers=$BS acks=$a linger.ms=0 | tail -1
done
```

Expected: `acks=1` unchanged; **`acks=all` average and p99 rise by roughly the added round trip**
(≈ 20–40 ms), because the leader waits for b3's next fetch. That is the stretched-cluster cost on
every write. Now try `delay 200ms 100ms` (jitter) and watch for ISR churn:

```bash
docker run --rm --net container:internals-b3-1 --cap-add NET_ADMIN nicolaka/netshoot \
  tc qdisc change dev eth0 root netem delay 200ms 100ms
$K kafka-topics --bootstrap-server $BS --describe --topic tracking.item-scan.recorded.v1
```

With 200 ms the follower usually stays in the ISR (it still catches up within 10 s), but latency
follows it. To see it **drop out**, also throttle bandwidth (`netem rate 100kbit`) while producing
at full speed. Remove the delay:

```bash
docker run --rm --net container:internals-b3-1 --cap-add NET_ADMIN nicolaka/netshoot \
  tc qdisc del dev eth0 root
```

Talking point: an **observer** in DC-B (MRC) would not be in the ISR, so `acks=all` would not wait
for it — that is why observers exist.

# E3 — compaction

```bash
$K kafka-topics --bootstrap-server $BS --create --topic orders.order-status.latest.v1 \
  --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=5000 \
  --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=20000 \
  --config min.compaction.lag.ms=0

cat <<'EOF' | $K kafka-console-producer --bootstrap-server $BS --topic orders.order-status.latest.v1 \
  --reader-property parse.key=true --reader-property key.separator=: --reader-property null.marker=NULL
SO-1001001:CREATED
SO-1001002:CREATED
SO-1001001:READY
SO-1001003:CREATED
SO-1001001:DISPATCHED
SO-1001002:CANCELLED
SO-1001003:NULL
EOF
```

`null.marker` turns `NULL` into a real null value (a tombstone) [V: console producer option since
Kafka 3.2]. Read immediately:

```bash
$K kafka-console-consumer --bootstrap-server $BS --topic orders.order-status.latest.v1 \
  --from-beginning --formatter-property print.key=true --formatter-property print.offset=true --timeout-ms 5000
```

Expected: all 7 records — they are in the **active segment**, which is never compacted.
Wait > 10 s, produce one more record to roll the segment (`SO-1001004:CREATED`), wait 15 s, read
again:

Expected: `SO-1001001 DISPATCHED`, `SO-1001002 CANCELLED`, `SO-1001003 null` (tombstone still kept),
`SO-1001004 CREATED`; offsets have **gaps**. After `delete.retention.ms` (20 s) plus another
cleaning, the `SO-1001003` tombstone disappears as well. Exact timing depends on cleaner scheduling.

Talking points: duplicates per key are normal until cleaning; a consumer offline longer than
`delete.retention.ms` misses the delete; compaction is not a GDPR erase for copies elsewhere.

# E4 — a KIP-848 consumer group

```bash
$K kafka-topics --bootstrap-server $BS --create --topic sites.queue-length.measured.v1 \
  --partitions 6 --replication-factor 3

# producer in the background: one record per second
docker exec -d internals-b1-1 bash -c 'for i in $(seq 1 100000); do echo "Q-$((i%6)):q=$((RANDOM%80))"; sleep 1; done \
  | kafka-console-producer --bootstrap-server b1:29092 --topic sites.queue-length.measured.v1 \
    --reader-property parse.key=true --reader-property key.separator=:'

# two consumers with the new protocol
for n in 1 2; do
  docker exec -d internals-b1-1 kafka-console-consumer --bootstrap-server b1:29092 \
    --topic sites.queue-length.measured.v1 --group analytics-kip848 \
    --command-property group.protocol=consumer --command-property client.id=c$n
done
sleep 10
$K kafka-consumer-groups --bootstrap-server $BS --describe --group analytics-kip848 --members --verbose
$K kafka-consumer-groups --bootstrap-server $BS --describe --group analytics-kip848 --state
```

Expected: 2 members, 3 partitions each; `--state` shows the assignor (`uniform`) and the group
type/protocol (`consumer`) [V: column names vary by version]. Add a third consumer
(`client.id=c3`), describe again: 2 partitions each. The first two consumers never stop
consuming — only moved partitions pause.

Compare with a classic group: start two consumers in `analytics-classic` **without** the
`group.protocol` property and list group types:

```bash
$K kafka-groups --bootstrap-server $BS --list     # Kafka 4.0+ tool, shows TYPE/PROTOCOL [V]
```

Negative test: add `--command-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor`
to a `group.protocol=consumer` consumer. Expected: the client refuses the config (not supported
with the new protocol).

Stop the consumers: `docker exec internals-b1-1 pkill -f ConsoleConsumer`.

# E5 — a share group (CP 8.2+ / Kafka 4.2+)

```bash
$K kafka-features --bootstrap-server $BS describe | grep -i share
# if share.version is 0 / missing:
$K kafka-features --bootstrap-server $BS upgrade --feature share.version=1      # [V]

$K kafka-topics --bootstrap-server $BS --create --topic sites.queue-length.jobs.v1 \
  --partitions 1 --replication-factor 3

for n in 1 2; do
  docker exec -d internals-b1-1 bash -c "kafka-console-share-consumer --bootstrap-server b1:29092 \
    --topic sites.queue-length.jobs.v1 --group sg-workers > /tmp/w$n.txt 2>&1"
done
seq 1 50 | sed 's/^/job-/' | $K kafka-console-producer --bootstrap-server b1:29092 \
  --topic sites.queue-length.jobs.v1
sleep 5
docker exec internals-b1-1 bash -c 'wc -l /tmp/w1.txt /tmp/w2.txt'
$K kafka-share-groups --bootstrap-server $BS --describe --group sg-workers
```

Expected: **both** workers received jobs from a **single partition** (a classic group would give
the partition to one member). Their outputs interleave — no ordering. If the tools are missing,
your image is older than Kafka 4.2 / CP 8.2 — skip and read notes part 2 §2.9.

# E6 — control plane outage

```bash
docker stop internals-c1-1
# data plane still works for partitions whose leaders are alive
$K kafka-producer-perf-test --topic orders.order-status.updated.v1 --num-records 500 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BS acks=all
# metadata changes do not
timeout 40 $K kafka-topics --bootstrap-server $BS --create --topic should-fail \
  --partitions 1 --replication-factor 1 2>&1 | tail -2
# now lose a broker while the controller is down
docker stop internals-b2-1
sleep 15
$K kafka-producer-perf-test --topic orders.order-status.updated.v1 --num-records 500 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=b1:29092 acks=all \
  delivery.timeout.ms=20000 request.timeout.ms=10000 2>&1 | tail -3
docker start internals-c1-1 internals-b2-1
```

Expected: first produce OK; topic creation times out; after b2 stops, writes to partitions that
b2 led fail (no controller → no new leader, and the ISR cannot shrink, so `acks=all` on other
partitions may also stall). After restart: elections, writes resume. Talking point: this is what
"losing quorum majority" means — and why 2 DCs need a 3rd-site controller.

# E7 — tiered storage on MinIO (optional) [V]

Confluent Tiered Storage supports MinIO as a certified S3-compatible store. Config names below
follow the CP docs pattern; check them against your CP version before you spend time debugging.

Add to the compose file:

```yaml
  minio:
    image: minio/minio:RELEASE.2025-04-22T22-12-26Z   # [V] any recent release
    command: server /data --console-address ":9001"
    environment: { MINIO_ROOT_USER: minio, MINIO_ROOT_PASSWORD: minio12345 }
    ports: ["9000:9000", "9001:9001"]
```

and to `x-broker-env`:

```yaml
  KAFKA_CONFLUENT_TIER_FEATURE: "true"
  KAFKA_CONFLUENT_TIER_ENABLE: "false"                 # opt-in per topic
  KAFKA_CONFLUENT_TIER_BACKEND: "S3"
  KAFKA_CONFLUENT_TIER_S3_BUCKET: "kafka-tier"
  KAFKA_CONFLUENT_TIER_S3_REGION: "us-east-1"
  KAFKA_CONFLUENT_TIER_S3_AWS_ENDPOINT_OVERRIDE: "http://minio:9000"
  KAFKA_CONFLUENT_TIER_S3_FORCE_PATH_STYLE_ACCESS: "true"
  KAFKA_CONFLUENT_TIER_METADATA_REPLICATION_FACTOR: 3
  AWS_ACCESS_KEY_ID: "minio"
  AWS_SECRET_ACCESS_KEY: "minio12345"
```

Create the bucket (`docker run --rm --net internals_default minio/mc sh -c "mc alias set m
http://minio:9000 minio minio12345 && mc mb m/kafka-tier"`), recreate the brokers, then:

```bash
$K kafka-topics --bootstrap-server $BS --create --topic tracking.item-scan.history.v1 \
  --partitions 1 --replication-factor 3 \
  --config confluent.tier.enable=true --config confluent.tier.local.hotset.ms=60000 \
  --config segment.ms=20000 --config retention.ms=86400000
$K kafka-producer-perf-test --topic tracking.item-scan.history.v1 --num-records 200000 \
  --record-size 500 --throughput 2000 --producer-props bootstrap.servers=$BS
# after ~2 minutes
docker run --rm --net internals_default minio/mc sh -c \
  "mc alias set m http://minio:9000 minio minio12345 >/dev/null && mc ls --recursive m/kafka-tier | head"
docker exec internals-b1-1 bash -c 'ls /var/lib/kafka/data/tracking.item-scan.history.v1-0/ | head'
```

Expected: segment objects in the bucket; after the hotset (60 s) the local partition directory
keeps only recent segments. A consumer `--from-beginning` still reads everything — old data now
comes from MinIO. If it does not run, the concept is in notes part 2 §2.12; that is enough to
explain it.

Reminder: this works on a normal cluster. **Not with Multi-Region Clusters** (CP docs) [V].

# Same end state in Terraform

The internals are not configurable in Confluent Cloud (RF is fixed at 3, no broker access, ISR
hidden, no unclean election). What you **can** declare are the topic-level settings from E1 and
E3. On `cc-basic` — `orders.order-status.updated.v1` already exists there (owned by
`00-lab-setup`, where you would set `min.insync.replicas`), so this creates only the compacted
current-state topic. Save as `07-kafka-internals/terraform/main.tf`:

```hcl
terraform {
  required_providers {
    confluent = { source = "confluentinc/confluent", version = "~> 2.94" } # [V] 2.94.0 = 2026-11-06
  }
}
provider "confluent" {}

data "terraform_remote_state" "base" {
  backend = "local"
  config  = { path = "${path.module}/../../00-lab-setup/terraform/terraform.tfstate" }
}
locals {
  base = data.terraform_remote_state.base.outputs
}

# E1 + E3: compacted current-state topic with the durability floor (RF is always 3 in CC)
resource "confluent_kafka_topic" "order_status_latest" {
  topic_name       = "orders.order-status.latest.v1"
  partitions_count = 1
  config = {
    "cleanup.policy"        = "compact"
    "min.insync.replicas"   = "2"
    "delete.retention.ms"   = "86400000"
    "min.compaction.lag.ms" = "0"
    "max.compaction.lag.ms" = "3600000"
  }
  kafka_cluster { id = local.base.basic_cluster_id }
  rest_endpoint = local.base.basic_rest_endpoint
  credentials {
    key    = local.base.sa_kafka_basic_api_key
    secret = local.base.sa_kafka_basic_api_secret
  }
}
```

CC accepts only a subset of topic configs (e.g. `min.insync.replicas`, `cleanup.policy`,
`retention.*`, `segment.*`, `*.compaction.lag.ms`, `delete.retention.ms`); `segment.ms` has a
minimum, so E3's 5-second segments are not possible there [V]. Infinite Storage replaces tiered
storage — nothing to configure.

On-prem equivalent (CFK, for the same two topics; field names [V]):

```yaml
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata: { name: orders.order-status.latest.v1, namespace: confluent }
spec:
  replicas: 3
  partitionCount: 1
  configs:
    cleanup.policy: "compact"
    min.insync.replicas: "2"
    delete.retention.ms: "86400000"
  kafkaRestClassRef: { name: ops-mc-rest }
```

For a Multi-Region Cluster the placement goes on the topic as
`confluent.placement.constraints` (JSON, `"version": 2`, `replicas` + `observers` +
`observerPromotionPolicy`) [V]:

```json
{"version": 2,
 "replicas":  [{"count": 2, "constraints": {"rack": "dc-a"}}, {"count": 2, "constraints": {"rack": "dc-b"}}],
 "observers": [],
 "observerPromotionPolicy": "under-min-isr"}
```

(layout A from `notes.md` §5; for layout B put 3 replicas in `dc-a` and 2 observers in `dc-b`.)

# Verification

| Experiment | You should have seen |
|---|---|
| E1 | ISR 3 → 2 → 1 → 3; `NOT_ENOUGH_REPLICAS` for acks=all at ISR 1; acks=1 accepted; visibility gated by ELR (note what you saw) |
| E2 | acks=all latency rises by ~the injected RTT; acks=1 does not |
| E3 | 7 records before roll; latest-per-key + gaps after; tombstone gone after `delete.retention.ms` |
| E4 | incremental reassignment; group protocol `consumer`; classic assignor config rejected |
| E5 | two workers sharing one partition |
| E6 | produce OK, metadata ops fail, no failover without controller |
| E7 | segments in MinIO, local dir trimmed |

# Teardown

```bash
docker run --rm --net container:internals-b3-1 --cap-add NET_ADMIN nicolaka/netshoot tc qdisc del dev eth0 root 2>/dev/null
docker exec internals-b1-1 pkill -f 'ConsoleConsumer|ConsoleShareConsumer|ConsoleProducer' 2>/dev/null
docker compose -f docker-compose.3broker.yml down -v
# Terraform part
terraform destroy
```

# Cost note

Local: free (Confluent Server trial licence for the 3-broker cluster). Terraform part on
`cc-basic`: one small topic,
negligible Basic-cluster usage. E5 in Confluent Cloud would need an Enterprise/Dedicated cluster
[V] — do it locally.
