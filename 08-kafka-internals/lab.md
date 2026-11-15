---
title: 08 — Lab — Kafka internals
subtitle: ISR shrink and expand, acks=all vs acks=1 with min.insync.replicas, simulated cross-DC latency, compaction, a KIP-848 consumer group, a share group, a controller outage, tiered storage on MinIO
---

> **Video:** episode 8, "One layer down" (coming soon). The video gives the reasoning; this lab lets you see it happen.

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
| E6 | Stop 1, then 2 of 3 controllers, then a broker | KRaft quorum majority, leader election | Why a 3rd site for controllers |
| E7 | Tiered storage to MinIO (optional) | Segment upload, hotset | Replays off broker disks |

# Environment

`local`, but **not** the single-node `dc-a`/`dc-b` clusters from `../00-lab-setup/`: one broker
cannot show replication (RF > 1 is impossible, ISR is always just the leader). Use the 3-broker
override below. It runs **next to** `00-lab-setup` (own project name and ports) or alone.

Design of the override: **three dedicated KRaft controllers + three brokers**. Dedicated
controllers (not combined with the brokers) keep the quorum alive when we stop brokers, so ISR
changes can still be recorded. Three voters is the smallest quorum that survives losing one; E6
then stops one controller, and then a second, on purpose.

# Prerequisites

- Docker with ~5 GB free memory (three brokers + three small controllers) [V].
- `00-lab-setup` read for image conventions; this file uses the same tag (`cp-server:8.4.0`,
  Kafka 4.4). If 8.4.0 misbehaves, use the latest 8.3.x as `00-lab-setup` suggests. Share groups
  (E5) need Kafka 4.2+ [V].
- Licence: the developer licence covers **one broker per cluster**. This is a separate,
  three-broker cluster, so it starts a **30-day trial** for this cluster only (it does not touch
  `dc-a`/`dc-b`). Fine for a lab; `down -v` and start again if it expires.
- The lab helpers loaded in this terminal
  (`source ~/labs/hybrid-kafka-on-azure/00-lab-setup/lab-env.sh`): the commands below use
  `$LAB/..` (the course folder), so they work from any directory.
- Start clean if you ran this lab before, because the topics below would already exist:
  `( cd "$LAB/../08-kafka-internals" && docker compose -f docker-compose.3broker.yml down -v )`
  (the start step below brings it up again).

Save this snippet as `$LAB/../08-kafka-internals/docker-compose.3broker.yml`:

```yaml
name: internals

x-controller-env: &controller-env
  CLUSTER_ID: "q1Sh-9_ISia_zwGINzRvyQ"
  KAFKA_PROCESS_ROLES: "controller"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "100@c1:29093,101@c2:29093,102@c3:29093"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
  KAFKA_HEAP_OPTS: "-Xms256m -Xmx256m"

x-broker-env: &broker-env
  CLUSTER_ID: "q1Sh-9_ISia_zwGINzRvyQ"          # any base64 UUID, same for all nodes
  KAFKA_PROCESS_ROLES: "broker"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "100@c1:29093,101@c2:29093,102@c3:29093"
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
      <<: *controller-env
      KAFKA_NODE_ID: 100
      KAFKA_LISTENERS: "CONTROLLER://c1:29093"

  c2:
    image: confluentinc/cp-server:8.4.0
    hostname: c2
    environment:
      <<: *controller-env
      KAFKA_NODE_ID: 101
      KAFKA_LISTENERS: "CONTROLLER://c2:29093"

  c3:
    image: confluentinc/cp-server:8.4.0
    hostname: c3
    environment:
      <<: *controller-env
      KAFKA_NODE_ID: 102
      KAFKA_LISTENERS: "CONTROLLER://c3:29093"

  b1:
    image: confluentinc/cp-server:8.4.0
    hostname: b1
    depends_on: [c1, c2, c3]
    ports: ["39092:39092"]
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: "INTERNAL://b1:29092,EXTERNAL://0.0.0.0:39092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://b1:29092,EXTERNAL://localhost:39092"

  b2:
    image: confluentinc/cp-server:8.4.0
    hostname: b2
    depends_on: [c1, c2, c3]
    ports: ["39093:39093"]
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: "INTERNAL://b2:29092,EXTERNAL://0.0.0.0:39093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://b2:29092,EXTERNAL://localhost:39093"

  b3:
    image: confluentinc/cp-server:8.4.0
    hostname: b3
    depends_on: [c1, c2, c3]
    ports: ["39094:39094"]
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: "INTERNAL://b3:29092,EXTERNAL://0.0.0.0:39094"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://b3:29092,EXTERNAL://localhost:39094"
```

Start and set helpers:

```bash
cd "$LAB/../08-kafka-internals"
docker compose -f docker-compose.3broker.yml up -d
export K="docker exec -i internals-b1-1"           # run tools inside b1
export BS="b1:29092,b2:29092,b3:29092"
$K kafka-metadata-quorum --bootstrap-server $BS describe --status
$K kafka-broker-api-versions --bootstrap-server $BS | grep -c 'b[123]:29092'
```

Expected: quorum status with `LeaderId:` one of 100–102 and `CurrentVoters` listing 100, 101 and
102 [V: output format]; `3` brokers. Ports 39092–39094 do not clash with
`00-lab-setup` (9092/9093, 8081/8082, 8083, 8090/8091).

# E1 — ISR shrink and expand

```bash
$K kafka-topics --bootstrap-server $BS --create --topic billing.call-record.created.v1 \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
$K kafka-topics --bootstrap-server $BS --describe --topic billing.call-record.created.v1
```

Expected: 3 partitions, each `Replicas: x,y,z  Isr: x,y,z`. Kafka 4.x also prints `Elr:` and
`LastKnownElr:` (empty) [V].

Stop one broker:

```bash
docker stop internals-b3-1
sleep 15
$K kafka-topics --bootstrap-server $BS --describe --topic billing.call-record.created.v1
$K kafka-topics --bootstrap-server $BS --describe --under-replicated-partitions
```

Expected: broker 3 gone from every `Isr`; partitions it led now have a new leader; all 3
partitions are under-replicated. Writes with `acks=all` still work (ISR 2 ≥ minISR 2):

```bash
$K kafka-producer-perf-test --topic billing.call-record.created.v1 --num-records 1000 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BS acks=all
```

Stop a second broker:

```bash
docker stop internals-b2-1
sleep 15
$K kafka-topics --bootstrap-server b1:29092 --describe --under-min-isr-partitions
echo 'SUB-1001234:VOICE' | $K kafka-console-producer --bootstrap-server b1:29092 \
  --topic billing.call-record.created.v1 --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=all --command-property delivery.timeout.ms=15000 \
  --command-property request.timeout.ms=10000
```

Expected: all partitions under min ISR. The `acks=all` produce fails after the timeout with
`NOT_ENOUGH_REPLICAS` (or `NotEnoughReplicasException`). Now `acks=1`:

```bash
echo 'SUB-1005678:SMS' | $K kafka-console-producer --bootstrap-server b1:29092 \
  --topic billing.call-record.created.v1 --reader-property parse.key=true --reader-property key.separator=: \
  --command-property acks=1
$K kafka-console-consumer --bootstrap-server b1:29092 --topic billing.call-record.created.v1 \
  --from-beginning --formatter-property print.key=true --timeout-ms 10000 | grep -c . ; \
$K kafka-get-offsets --bootstrap-server b1:29092 --topic billing.call-record.created.v1
```

Expected: the `acks=1` write is **accepted**. With ELR active (default on new 4.1+ clusters), the
high watermark does not advance while ISR < minISR, so the consumer **does not see** `SUB-1005678`
yet, and `kafka-get-offsets` (which reports the high watermark for "latest") does not move
either [V]. Without ELR the record is visible at once, but it lives on one disk only. Note what
you saw — it is a useful detail either way.

Bring brokers back:

```bash
docker start internals-b2-1 internals-b3-1
sleep 20
$K kafka-topics --bootstrap-server $BS --describe --topic billing.call-record.created.v1
$K kafka-console-consumer --bootstrap-server $BS --topic billing.call-record.created.v1 \
  --from-beginning --formatter-property print.key=true --timeout-ms 10000 | grep SUB-1005678
```

Expected: ISR back to 3 for all partitions (expand), leadership may stay on b1 until preferred
leader election runs (`auto.leader.rebalance.enable`, default every 5 min) or you run
`kafka-leader-election --election-type preferred --all-topic-partitions` (E6 does). `SUB-1005678` is now visible.

# E2 — acks and cross-DC latency

Pin one partition's leader to b1 so we control who is "remote":

```bash
$K kafka-topics --bootstrap-server $BS --create --topic noc.cell-alarm.raised.v1 \
  --replica-assignment 1:2:3 --config min.insync.replicas=2

for a in 1 all; do
  echo "acks=$a"
  $K kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 --num-records 20000 \
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
  $K kafka-producer-perf-test --topic noc.cell-alarm.raised.v1 --num-records 20000 \
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
$K kafka-topics --bootstrap-server $BS --describe --topic noc.cell-alarm.raised.v1
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
$K kafka-topics --bootstrap-server $BS --create --topic billing.subscriber-balance.latest.v1 \
  --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=5000 \
  --config min.cleanable.dirty.ratio=0.01 --config delete.retention.ms=20000 \
  --config min.compaction.lag.ms=0

cat <<'EOF' | $K kafka-console-producer --bootstrap-server $BS --topic billing.subscriber-balance.latest.v1 \
  --reader-property parse.key=true --reader-property key.separator=: --reader-property null.marker=NULL
SUB-1001001:{"subscriberRef":"SUB-1001001","balanceCents":1250}
SUB-1001002:{"subscriberRef":"SUB-1001002","balanceCents":500}
SUB-1001001:{"subscriberRef":"SUB-1001001","balanceCents":1100}
SUB-1001003:{"subscriberRef":"SUB-1001003","balanceCents":2000}
SUB-1001001:{"subscriberRef":"SUB-1001001","balanceCents":950}
SUB-1001002:{"subscriberRef":"SUB-1001002","balanceCents":0}
SUB-1001003:NULL
EOF
```

`null.marker` turns `NULL` into a real null value (a tombstone: subscriber `SUB-1001003` left)
[V: console producer option since Kafka 3.2]. Read immediately:

```bash
$K kafka-console-consumer --bootstrap-server $BS --topic billing.subscriber-balance.latest.v1 \
  --from-beginning --formatter-property print.key=true --formatter-property print.offset=true --timeout-ms 5000
```

Expected: all 7 records — they are in the **active segment**, which is never compacted.
Wait > 10 s, produce one more record to roll the segment
(`SUB-1001004:{"subscriberRef":"SUB-1001004","balanceCents":1000}`), wait 15 s, read again:

Expected: `SUB-1001001` with `"balanceCents":950`, `SUB-1001002` with `"balanceCents":0`,
`SUB-1001003 null` (tombstone still kept), `SUB-1001004` with `"balanceCents":1000`; offsets have
**gaps**. After `delete.retention.ms` (20 s) plus another cleaning, the `SUB-1001003` tombstone
disappears as well. Exact timing depends on cleaner scheduling.

Talking points: duplicates per key are normal until cleaning; a consumer offline longer than
`delete.retention.ms` misses the delete; compaction is not a GDPR erase for copies elsewhere.

# E4 — a KIP-848 consumer group

```bash
$K kafka-topics --bootstrap-server $BS --create --topic network.cell-load.measured.v1 \
  --partitions 6 --replication-factor 3

# producer in the background: five records per second, 60 cell keys so all 6 partitions get data
docker exec -d internals-b1-1 bash -c 'for i in $(seq 1 100000); do echo "CELL-$((i%60)):load=$((RANDOM%80))"; sleep 0.2; done \
  | kafka-console-producer --bootstrap-server b1:29092 --topic network.cell-load.measured.v1 \
    --reader-property parse.key=true --reader-property key.separator=:'

# start consumer c<n> with the new protocol in the background; every record it reads goes to
# /tmp/c<n>.log with the time it was read, the partition and the offset
kip848() {
  docker exec -d internals-b1-1 bash -c "kafka-console-consumer --bootstrap-server b1:29092 \
    --topic network.cell-load.measured.v1 --group analytics-kip848 \
    --command-property group.protocol=consumer --command-property client.id=c$1 \
    --formatter-property print.partition=true --formatter-property print.offset=true 2>&1 \
    | while IFS= read -r l; do echo \"\$(date +%T) c$1 \$l\"; done >> /tmp/c$1.log"
}
kip848 1; kip848 2
sleep 10
$K kafka-consumer-groups --bootstrap-server $BS --describe --group analytics-kip848 --members --verbose
$K kafka-consumer-groups --bootstrap-server $BS --describe --group analytics-kip848 --state
```

Expected: 2 members, 3 partitions each; `--state` shows the assignor (`uniform`) and the group
type/protocol (`consumer`) [V: column names vary by version].

Now change the group while you watch it. In a **second terminal**, follow all members' logs
(lines look like `14:02:07 c1 Partition:4 Offset:211 load=37` [V]):

```bash
docker exec internals-b1-1 tail -F /tmp/c1.log /tmp/c2.log /tmp/c3.log
```

Back in the first terminal: scale out, then roll a member the way a rolling deploy does (stop it,
start it again):

```bash
kip848 3                                             # a third member joins
sleep 20
$K kafka-consumer-groups --bootstrap-server $BS --describe --group analytics-kip848 --members --verbose
docker exec internals-b1-1 pkill -f 'client.id=c1'   # c1 leaves (clean shutdown)
sleep 10
kip848 1                                             # c1 is back
sleep 20
$K kafka-consumer-groups --bootstrap-server $BS --describe --group analytics-kip848 --members --verbose
```

Expected: after c3 joins, 2 partitions each; with c1 gone, c2 and c3 hold 3 each; after c1
returns, 2 each again. In the `tail`, the members that keep partitions print several lines a
second through all three changes, with **no gap** in their timestamps: they never stop
consuming. Only a partition that moves goes quiet for a few seconds, then shows up under its new
owner, continuing from the committed offset.

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

$K kafka-topics --bootstrap-server $BS --create --topic network.cell-load.jobs.v1 \
  --partitions 1 --replication-factor 3

for n in 1 2; do
  docker exec -d internals-b1-1 bash -c "kafka-console-share-consumer --bootstrap-server b1:29092 \
    --topic network.cell-load.jobs.v1 --group sg-workers > /tmp/w$n.txt 2>&1"
done
sleep 10      # let both workers join first: a new share group starts at the latest offset
# 200 jobs, one every 50 ms: many small batches, not one batch that a single worker takes whole
for i in $(seq 1 200); do echo "job-$i"; sleep 0.05; done | $K kafka-console-producer \
  --bootstrap-server b1:29092 --topic network.cell-load.jobs.v1
sleep 5
docker exec internals-b1-1 bash -c 'grep -c "^job-" /tmp/w1.txt /tmp/w2.txt; head -5 /tmp/w1.txt /tmp/w2.txt'
$K kafka-share-groups --bootstrap-server $BS --describe --group sg-workers
```

Expected: **both** counts above 0, together about 200, from a **single partition** (a classic
group would give the partition to one member). The split need not be even. The `head` output
shows the job numbers interleave across the workers — no ordering. If one worker still got
everything, run the producer loop again with `sleep 0.2`. If the tools are missing, your image is
older than Kafka 4.2 / CP 8.2 — skip and read notes part 2 §2.9.

# E6 — control plane outage

E1 left most partition leaders on b1 (b2 and b3 came back as followers). Move leadership back to
the preferred replicas first, so that stopping b2 below takes away a leader:

```bash
$K kafka-leader-election --bootstrap-server $BS --election-type PREFERRED --all-topic-partitions
$K kafka-topics --bootstrap-server $BS --describe --topic billing.call-record.created.v1
$K kafka-metadata-quorum --bootstrap-server $BS describe --status | grep -E 'LeaderId|CurrentVoters'
```

Expected: the election reports the partitions it moved ("Valid replica already elected" for the
rest) [V]. In the describe, each partition's `Leader:` is now the first ID in its `Replicas:`, so
the three partitions usually have three different leaders. **Find the partition with
`Leader: 2`.** If none has it, stop the broker that leads one of the other partitions instead of
b2 below — but not b1: the tools run inside b1.

Lose one controller. Read c1, c2, c3 as DC-A, DC-B and a third site:

```bash
docker stop internals-c1-1
sleep 10
$K kafka-metadata-quorum --bootstrap-server $BS describe --status | grep -E 'LeaderId|CurrentVoters'
$K kafka-topics --bootstrap-server $BS --create --topic quorum-ok --partitions 1 --replication-factor 3
```

Expected: `LeaderId` is 101 or 102 (a new election if c1 was the leader); the topic is created.
Two of three voters are a majority, so metadata changes still work.

Lose a second controller — one of three is no majority:

```bash
docker stop internals-c2-1
sleep 10
$K timeout 20 kafka-metadata-quorum --bootstrap-server $BS describe --status 2>&1 | tail -2
docker logs --since 20s internals-c3-1 2>&1 | grep -E 'Candidate|Prospective' | tail -3
# data plane still works for partitions whose leaders are alive
$K kafka-producer-perf-test --topic billing.call-record.created.v1 --num-records 500 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BS acks=all
# metadata changes do not
$K timeout 40 kafka-topics --bootstrap-server $BS --create --topic should-fail \
  --partitions 1 --replication-factor 1 2>&1 | tail -2
# now lose a broker while the quorum has no majority
docker stop internals-b2-1
sleep 15
$K kafka-producer-perf-test --topic billing.call-record.created.v1 --num-records 500 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=b1:29092 acks=all \
  delivery.timeout.ms=20000 request.timeout.ms=10000 2>&1 | tail -3
# one controller back: 2 of 3 again
docker start internals-c2-1
sleep 20
$K kafka-topics --bootstrap-server b1:29092 --describe --topic billing.call-record.created.v1
$K kafka-producer-perf-test --topic billing.call-record.created.v1 --num-records 500 \
  --record-size 200 --throughput -1 --producer-props bootstrap.servers=b1:29092 acks=all 2>&1 | tail -1
docker start internals-c1-1 internals-b2-1
```

Expected: c3 is running, but there is no leader: the quorum describe gets no answer within 20 s,
and c3's log shows it starting election after election [V: exact log wording]. The first produce
works (all leaders alive); topic creation times out. After b2 stops, writes to the partition b2
led fail: no controller can fence b2 or elect a new leader, and the ISR cannot shrink, so
`acks=all` on the other partitions may stall too. With c2 back, the quorum has a leader again; b2
is fenced after `broker.session.timeout.ms` (9 s), its partition gets a new leader, the ISR
shrinks to 2 and the last produce succeeds. Talking point: with only two sites, one of them holds
2 of the 3 voters, and losing that site is the "1 of 3" state — the surviving controller is up and
still cannot act. A third site with one voter makes every single-site loss the "2 of 3" state.
That is why 2 DCs need a 3rd-site controller.

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
$K kafka-topics --bootstrap-server $BS --create --topic noc.cell-alarm.history.v1 \
  --partitions 1 --replication-factor 3 \
  --config confluent.tier.enable=true --config confluent.tier.local.hotset.ms=60000 \
  --config segment.ms=20000 --config retention.ms=86400000
$K kafka-producer-perf-test --topic noc.cell-alarm.history.v1 --num-records 200000 \
  --record-size 500 --throughput 2000 --producer-props bootstrap.servers=$BS
# after ~2 minutes
docker run --rm --net internals_default minio/mc sh -c \
  "mc alias set m http://minio:9000 minio minio12345 >/dev/null && mc ls --recursive m/kafka-tier | head"
docker exec internals-b1-1 bash -c 'ls /var/lib/kafka/data/noc.cell-alarm.history.v1-0/ | head'
```

Expected: segment objects in the bucket; after the hotset (60 s) the local partition directory
keeps only recent segments. A consumer `--from-beginning` still reads everything — old data now
comes from MinIO. If it does not run, the concept is in notes part 2 §2.12; that is enough to
explain it.

Reminder: this works on a normal cluster. **Not with Multi-Region Clusters** (CP docs) [V].

# Same end state in Terraform

The internals are not configurable in Confluent Cloud (RF is fixed at 3, no broker access, ISR
hidden, no unclean election). What you **can** declare are the topic-level settings from E1 and
E3. On `cc-basic` — `billing.call-record.created.v1` already exists there (owned by
`00-lab-setup`, where you would set `min.insync.replicas`), so this creates only the compacted
current-state topic. Save as `$LAB/../08-kafka-internals/terraform/main.tf`. It has no schema on
purpose, only to show the topic settings; a real data product goes through module 07's golden
module ("no schema, no topic").

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
resource "confluent_kafka_topic" "subscriber_balance_latest" {
  topic_name       = "billing.subscriber-balance.latest.v1"
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

```bash
cd "$LAB/../08-kafka-internals/terraform"
terraform init && terraform apply
```

CC accepts only a subset of topic configs (e.g. `min.insync.replicas`, `cleanup.policy`,
`retention.*`, `segment.*`, `*.compaction.lag.ms`, `delete.retention.ms`); `segment.ms` has a
minimum, so E3's 5-second segments are not possible there [V]. Infinite Storage replaces tiered
storage — nothing to configure.

On-prem equivalent (CFK, for the same two topics; field names [V]):

```yaml
apiVersion: platform.confluent.io/v1beta1
kind: KafkaTopic
metadata: { name: billing.subscriber-balance.latest.v1, namespace: confluent }
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
| E4 | incremental reassignment, no gap in the logs of members that keep partitions; group protocol `consumer`; classic assignor config rejected |
| E5 | two workers sharing one partition |
| E6 | 1 of 3 controllers down: metadata still works; 2 of 3 down: produce OK, metadata ops fail, no failover; one back: elections, writes resume |
| E7 | segments in MinIO, local dir trimmed |

# Teardown

```bash
docker run --rm --net container:internals-b3-1 --cap-add NET_ADMIN nicolaka/netshoot tc qdisc del dev eth0 root 2>/dev/null
docker exec internals-b1-1 pkill -f 'ConsoleConsumer|ConsoleShareConsumer|ConsoleProducer' 2>/dev/null
( cd "$LAB/../08-kafka-internals" && docker compose -f docker-compose.3broker.yml down -v )
# Terraform part (saved in 08-kafka-internals/terraform/)
( cd "$LAB/../08-kafka-internals/terraform" && terraform destroy )
```

# Cost note

Local: free (Confluent Server trial licence for the 3-broker cluster). Terraform part on
`cc-basic`: one small topic,
negligible Basic-cluster usage. E5 in Confluent Cloud would need an Enterprise/Dedicated cluster
[V] — do it locally.
