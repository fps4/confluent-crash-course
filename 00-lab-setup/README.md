---
title: 00 — Lab setup
subtitle: The three lab environments every module uses (local Docker, cc-basic, paid-session). Written 2026-11-07; dc-a and sr-a tested on 2026-11-08.
---

# What this is

One place for the lab plumbing, so the other modules only say "environment: `local`" and start.
Three environments, from free to billable:

| Environment | What it is | Used for | Cost |
|---|---|---|---|
| `local` | Docker Compose: two single-node KRaft clusters **`dc-a`** and **`dc-b`** (Confluent Server 8.4.0), one Schema Registry each (**`sr-a`**, **`sr-b`**), optional Kafka Connect worker for MirrorMaker 2 | Cluster Linking CP↔CP, failover/failback, MM2, Schema Linking CP→CP, data contracts with rules, broker-side schema ID validation | Free (developer licence, see below) |
| `cc-basic` | Confluent Cloud environment **`crash-course`**, Basic cluster **`cc-basic`** on Azure **`westeurope`**, Schema Registry (Stream Governance **Essentials**), service account **`sa-crash-admin`** with API keys | Cloud clients, Schema Linking CP→CC, governance basics, Terraform practice | Near zero: Basic's first eCKU is free; you pay data in/out and storage. Trial credits cover it |
| `paid-session` | One day: Dedicated cluster **`cc-paid`** (1 CKU, public) and/or Enterprise cluster **`cc-ent`** (always private, Azure PrivateLink Attachment) in the same environment. Created in the morning, destroyed the same day | Hybrid source-initiated link **dc-a → CC**, CC as Cluster Linking destination, CC broker-side validation, Private Link (module 02) | Billable per hour. See "Cost" |

Versions pinned on 2026-11-07:

| Tool | Version | Source |
|---|---|---|
| Confluent Platform images | **8.4.0** (Apache Kafka 4.4), tags pushed 2026-10-26 | Docker Hub `confluentinc/cp-server` tags; CP release notes |
| Confluent CLI | **v4.82.0** (2026-11-03) | CLI release notes |
| Terraform provider `confluentinc/confluent` | **2.94.0** (2026-11-06) | registry.terraform.io, GitHub releases |
| Terraform | ≥ 1.5 | — |

CP 8.4 is two weeks old today. If something in 8.4.0 breaks a lab, change the four image tags to
the latest 8.3.x and note it in the lab. [V]

# Licence terms (checked 2026-11-07)

- **Developer licence**: full use of commercial features, free, **no expiry**, **non-production**,
  limited to **one broker per cluster**. Several single-broker clusters can share it — this is
  exactly the `dc-a` + `dc-b` setup. **Adding a second broker to a cluster starts a 30-day trial,
  and you cannot go back** to the developer licence.
  https://docs.confluent.io/platform/current/installation/license.html
- The Docker configuration page says it differently: without `KAFKA_CONFLUENT_LICENSE`,
  "Confluent Server can be used for a 30-day trial period". [V] After first start, check the log:
  `docker logs dc-a 2>&1 | grep -i licen`.
- Features the labs need and their licence: Cluster Linking, Schema Validation, Multi-Region
  Clusters, Tiered Storage, RBAC → Confluent Server, Enterprise licence. Schema Linking exporters,
  broker-side schema validation and data-contract rules → Enterprise (commercial) Schema Registry
  features. The developer licence covers all of them on single-broker clusters.
- Production note: the cheaper **"Customer-managed Confluent Platform for Confluent
  Cloud"** subscription lists Connect, Replicator, CFK, Control Center and the Cloud Gateway — it
  does **not** list Confluent Server or Cluster Linking. A source-initiated link from the DC needs
  the full Enterprise (or Private Cloud) subscription on the brokers. Schema exporters *do* work
  under that subscription when the destination registry is in Confluent Cloud.
- CP 8.4 deprecates storing the licence in `_confluent-license`; 8.5 is expected to default to
  `_confluent-command`. Irrelevant for the labs. [V]

# Prerequisites

- Docker Desktop (or Colima/OrbStack) with **≥ 6 GB RAM** for the containers. Two brokers + two
  registries + Connect is about 4–5 GB.
- `jq`, `curl`.
- Confluent CLI (on macOS: `brew install confluentinc/tap/cli`; other platforms: see the CLI
  install docs), then `confluent version` should show v4.82.0 or later. (`confluent update`
  upgrades in place.)
- Terraform ≥ 1.5 (on macOS: `brew install hashicorp/tap/terraform`).
- A Confluent Cloud account for `cc-basic` and `paid-session`.

# `local` — start, use, stop

```bash
mkdir -p ~/labs && cd ~/labs
git clone https://github.com/fps4/confluent-crash-course.git
export LAB=~/labs/confluent-crash-course/00-lab-setup
cd "$LAB"

docker compose up -d                    # dc-a, sr-a, dc-b, sr-b
docker compose --profile mm2 up -d      # same, plus the Connect worker for MM2 (module 01 part C only)
docker compose ps                       # dc-a and dc-b should be "healthy"
```

> Docker host on another machine (`DOCKER_HOST=ssh://…` or `docker context use <remote>`)? Read
> [Remote Docker host](#remote-docker-host) first. The labs assume the containers run on the
> machine where you type the commands.

**In every new terminal**, load the lab helpers (bash or zsh):

```bash
source ~/labs/confluent-crash-course/00-lab-setup/lab-env.sh
lab_check                               # containers up, /work shared, REST ports reachable
```

[`lab-env.sh`](lab-env.sh) sets `LAB` (this folder), `DCA_ID` / `DCB_ID` (the fixed cluster IDs
from `docker-compose.yml`) and four functions that run a tool inside a container: `a` (dc-a), `b`
(dc-b), `sra` (sr-a), `srb` (sr-b). So `a kafka-topics --list …` = `docker exec -i dc-a kafka-topics
--list …`. Put the `source` line in `~/.zshrc` if you do not want to repeat it.

Files you write to `$LAB/work/` appear inside every container at `/work/` (git-ignored, because
link configs may hold API secrets).

Smoke test:

```bash
curl -s localhost:8090/kafka/v3/clusters | jq -r '.data[0].cluster_id'   # Q0PvpgzYSwirapvmRY0NFg
curl -s localhost:8091/kafka/v3/clusters | jq -r '.data[0].cluster_id'   # UmwHWmvUR6KH9yIQ3xbmhg
curl -s localhost:8081/subjects    # []
curl -s localhost:8082/subjects    # []
a kafka-topics --bootstrap-server dc-a:29092 --list
```

If the REST calls fail but Kafka works, the embedded REST API is off: add
`KAFKA_KAFKA_REST_ENABLE: "true"` to both brokers and restart. [V]

Stop and reset:

```bash
docker compose stop                       # keep data
docker compose --profile mm2 down -v      # delete everything
```

Ports: dc-a `9092` (Kafka), `8090` (REST v3), sr-a `8081`; dc-b `9093`, `8091`, sr-b `8082`;
Connect `8083`. Inside the Docker network: `dc-a:29092`, `dc-b:29092`, `sr-a:8081`, `sr-b:8081`.

Memory: dc-a and dc-b take about 1.2 GB each, the registries 0.3 GB each, Connect about 1.1 GB.
Other containers on the same Docker host count too. If a container shows `Exited (137)`, check
`docker inspect <name> --format '{{.State.OOMKilled}}'` and give Docker more memory or stop other
stacks.

## Remote Docker host

If your Docker daemon runs on another machine (a home server, a VM), **run the labs on that
machine**, not on the laptop:

```bash
ssh <docker-host>
mkdir -p ~/labs && cd ~/labs
git clone https://github.com/fps4/confluent-crash-course.git
cd confluent-crash-course/00-lab-setup
docker compose up -d
source ./lab-env.sh && lab_check
```

The host needs `docker compose`, `jq` and `curl`. The Terraform and Confluent CLI parts
(`cc-basic`, `paid-session`) only talk to Confluent Cloud, so they can stay on the laptop.

Why not drive a remote daemon from the laptop: two things break silently.

- The `./work:/work` bind mount resolves on the **Docker host**. Docker creates an empty
  `/Users/<you>/…/work` directory there, so the config files you write to `$LAB/work/` on the laptop
  never reach `/work/` in the containers. `lab_check` reports this.
- The labs call `localhost:8090`, `:8091`, `:8081`, `:8082`, `:8083`. Those ports are published on
  the Docker host, not on the laptop. (You could add `ssh -L` tunnels for all five and sync `work/`
  with rsync, but that is more moving parts than the lab is worth.)

Do not run the stack on two daemons at once. `lab_check` prints which Docker context it is
talking to; `docker context show` does the same.

## Next

The local stack is all module 01 needs for parts A–C, and all module 08 needs for its local part.
The suggested order in the [course README](../README.md#modules--suggested-order) starts with
[08](../08-linking-and-contracts/lab.md), then [01](../01-hybrid-multicloud/lab.md). Set up
`cc-basic` (below) when a lab first asks for it.

# `cc-basic` — create with Terraform

1. **Account and credits.** Sign up at confluent.io. New accounts get **$400 of credit, valid 30
   days**; a payment method is needed before you can create resources but is not charged until the
   trial ends. If the trial ends without a card or promo code, the organisation is suspended and
   resources can be deleted. Older course pages advertise promo codes **`CONFLUENTDEV1`** and
   **`HYBRCLOUD101`** ($25, skips the card screen) — not confirmed as valid in 2026. [V]
   Alternative: subscribe through the **Azure Marketplace** (pay-as-you-go via the Azure bill).
2. **CLI login.**
   ```bash
   confluent login --save                 # browser SSO or email/password; --save keeps the token
   confluent environment list
   ```
3. **Cloud API key for Terraform** (an *organisation-level* key, not a cluster key). Easiest:
   create a service account `sa-terraform` with `OrganizationAdmin` in the console (Administration →
   Accounts & access), then *Cloud resource management* API key for it. Or with the CLI:
   ```bash
   confluent iam service-account create sa-terraform --description "crash course terraform"
   confluent iam rbac role-binding create --principal User:<sa-id> --role OrganizationAdmin
   confluent api-key create --resource cloud --service-account <sa-id>
   export CONFLUENT_CLOUD_API_KEY=<key>
   export CONFLUENT_CLOUD_API_SECRET=<secret>
   ```
4. **Apply.**
   ```bash
   cd "$LAB/terraform"
   terraform init
   terraform plan          # expect: environment, basic cluster, SA, role binding, 2 API keys, 3 topics
   terraform apply
   terraform output
   ```
5. **Point the CLI at it.**
   ```bash
   confluent environment use $(terraform output -raw environment_id)
   confluent kafka cluster use $(terraform output -raw basic_cluster_id)
   confluent api-key store $(terraform output -raw sa_kafka_basic_api_key) \
     $(terraform output -raw sa_kafka_basic_api_secret) --resource $(terraform output -raw basic_cluster_id)
   confluent api-key use $(terraform output -raw sa_kafka_basic_api_key)
   confluent kafka topic list
   ```

What the base creates (and what it does not):

| Resource | Name | Notes |
|---|---|---|
| Environment | `crash-course` | Stream Governance **Essentials**. Essentials: no data-contract rules, 100 schemas included, 10 exporters. Cannot downgrade from Advanced |
| Kafka cluster | `cc-basic` | Basic, single zone, Azure westeurope. Basic **cannot** be a Cluster Linking destination, and cannot be the source of a source-initiated link |
| Schema Registry | data source | Auto-provisioned per environment |
| Service account | `sa-crash-admin` | `EnvironmentAdmin` on `crash-course` (lab shortcut) |
| API keys | Kafka key for `cc-basic`, SR key | Secrets only in state and `terraform output -raw` |
| Topics | `orders.order-status.updated.v1`, `tracking.item-scan.recorded.v1`, `sites.queue-length.measured.v1` | PII-free. `customers.customer-journey.v1` (PII) is created by the CSFLE/governance modules |

Other modules reference these values with `terraform output`, or with a `terraform_remote_state`
data source pointing at `00-lab-setup/terraform/terraform.tfstate`.

# `paid-session` — one day, then destroy

```bash
cd "$LAB/terraform"
terraform apply -var enable_paid_dedicated=true                 # cc-paid, Dedicated 1 CKU, public
terraform apply -var enable_paid_enterprise_private=true        # cc-ent + PrivateLink Attachment (module 02)
# ... labs ...
terraform apply -var enable_paid_dedicated=false -var enable_paid_enterprise_private=false
```

Which cluster for which lab:

| Need | Cluster | Why |
|---|---|---|
| Source-initiated link from the laptop (`dc-a` → CC) | `cc-paid` (Dedicated, public) | CP → CC needs a Dedicated or privately networked Enterprise destination. Enterprise is always private, so a laptop can only reach it through an Azure private endpoint + VPN. Dedicated with public endpoints works over the internet |
| CC as Cluster Linking destination | `cc-paid` or `cc-ent` | Only Dedicated, Enterprise (and Freight on AWS) can be destinations |
| Broker-side schema ID validation in CC | `cc-paid` | Dedicated only |
| Private Link | `cc-ent` | Cheaper and faster to create than a Dedicated private network; Azure side is in module 02 |

Rules for the day: create in the morning, check the Billing page before you stop, destroy before
you sleep. Dedicated provisioning can take tens of minutes or longer. [V]

# Cost (list prices 2026-11-07, USD, before replication) [V]

| Item | Price | Source |
|---|---|---|
| Basic | first eCKU free, then $0.14/eCKU-h; $0.05/GB in/out; $0.08/GB-month | confluent.io pricing page |
| Standard | $0.75/eCKU-h | same |
| Enterprise | $1.75–2.25/eCKU-h; $0.02–0.05/GB | same |
| Freight | $2.25/eCKU-h, min 2 eCKU | same |
| Dedicated | not on the pricing page ("contact us"); console shows the estimate when you create it | — |
| Cluster Linking | billed per link-hour and per GB mirrored; no public rate on the pricing page | Cluster Linking docs |
| Stream Governance | Essentials free (100 schemas); Advanced "starting at $1/hour" | governance packages doc |
| Private Link / PLATT | not on the pricing page; Azure private endpoint charges on the Azure bill | — |

Budget for one paid session (4–6 h, 1 CKU Dedicated + one link + a few MB): roughly the price of a
lunch, all covered by trial credit. Read the console's estimate before you press create.

# Teardown checklist

- [ ] `terraform apply -var enable_paid_dedicated=false -var enable_paid_enterprise_private=false` (end of paid day)
- [ ] Delete cluster links and mirror topics first if a lab created them outside Terraform (a link with mirror topics cannot be deleted)
- [ ] `terraform destroy` when the course is over (environment, cluster, SA, keys)
- [ ] `confluent api-key delete <terraform key>` and remove `sa-terraform`
- [ ] `docker compose --profile mm2 down -v`
- [ ] Billing page in the console: no running clusters

# Status and untested items in this folder

- `docker-compose.yml`: passes `docker compose config`. **All five containers started on
  2026-11-08** (Docker Desktop, 8 GB VM). Connect was OOM-killed with the image's default 2 GB heap;
  now capped at 768 MB, and `/connector-plugins` lists the three MM2 connectors.
- `lab-env.sh` + `lab_check` tested in bash and zsh, against the local stack and against a remote
  Docker context (where it correctly reports the missing `/work` share).
- Embedded REST on port 8090 enabled by default on cp-server 8.4.0. [V]
- `SCHEMA_REGISTRY_CONFLUENT_LICENSE_REPLICATION_FACTOR` needed for licensed SR features on a single broker. [V]
- Terraform: passes `terraform validate`; not planned or applied against a live account. Enterprise `availability = "LOW"`. [V]
