---
title: Hybrid Kafka on Azure
subtitle: Designing Confluent Platform and Confluent Cloud between your data centres and Azure. An advanced, hands-on Apache Kafka course — Cluster Linking, Azure private networking, governance, security. Built 2026-11-07.
---

# Hybrid Kafka on Azure

*Designing Confluent Platform and Confluent Cloud between your data centres and Azure.*

An advanced, hands-on **Apache Kafka** course for **experienced integration, platform and data
engineers and architects**: people who already run Kafka or similar messaging in production and now
have to design Kafka with Confluent across a **hybrid and multicloud** estate. Confluent Platform in your own data
centres, Confluent Cloud on Azure, and the links, networks, schemas and security between them.
Topics, partitions, consumer groups and offsets are assumed knowledge; the course starts where
they stop.

The running example is a **mobile operator**. Billing, the network operations centre and customer
care run on Confluent Platform in two data centres; the data and AI teams in Azure want call records
and network events for fraud and churn models, without putting load on the operational clusters.
Topics and schemas: [00-lab-setup](00-lab-setup/README.md#the-running-example).

Every module has three parts, under the same number:

- a **video** with the reasoning: the diagrams, the trade-offs, what goes wrong in real estates,
  and what to ask the platform team;
- **notes**: concepts, Mermaid diagrams, what changed since the official course, design advice,
  Q&A and sources;
- a **lab** you run yourself, in the same order as the video: CLI first, then the same end state
  in Terraform, then teardown. Where a lab step shows something on screen (a console page, a
  JSON response, a chart), the video shows it too, so you know what to look for.

Facts marked **[V]** depend on version, region or price. Check them before you state them as fact.

## Modules

Do them in order: each lab assumes the earlier ones, and each video builds on the one before.
Videos are published one at a time; the titles below link once they are out.

| # | Module | Video | Source | Lab environment |
|---|---|---|---|---|
| 01 | [Linking and data contracts](01-linking-and-contracts/notes.md) · [contracts](01-linking-and-contracts/notes-data-contracts.md) · [lab](01-linking-and-contracts/lab.md) | Getting data out of the data centre | Confluent docs (no course exists) | local → cc-basic → paid-session |
| 02 | [Hybrid and multicloud](02-hybrid-multicloud/notes.md) · [lab](02-hybrid-multicloud/lab.md) | Copies, failover and the way back | Hybrid and Multicloud Architecture course | local (+ paid link) |
| 03 | [Cloud networking on Azure](03-cloud-networking-azure/notes.md) · [lab](03-cloud-networking-azure/lab.md) | Kafka doesn't fit through a load balancer | Confluent Cloud Networking course, rewritten for Azure | cc-basic → paid-session |
| 04 | [Stream governance](04-stream-governance/notes.md) · [lab](04-stream-governance/lab.md) | Governance for teams that don't know each other | Streaming Data Governance course | local → cc-basic |
| 05 | [CSFLE](05-csfle/notes.md) · [lab](05-csfle/lab.md) | Encrypting a field nobody else should read | CSFLE course + docs | `crash-course-csfle` (Advanced) |
| 06 | [Security](06-cloud-security/notes.md) · [CP side](06-cloud-security/notes-cp-security.md) · [lab](06-cloud-security/lab.md) | Who is allowed to read this | Confluent Cloud Security course + docs | `cc-std-lab06` + paid-session |
| 07 | [Production systems](07-production-systems/notes.md) · [lab](07-production-systems/lab.md) | From "it works" to "it runs a business" | Mastering Production Data Streaming Systems | local → cc-basic + `cc-std-lab07` |
| 08 | [Kafka internals](08-kafka-internals/notes.md) · [clients and storage](08-kafka-internals/notes-clients-and-storage.md) · [lab](08-kafka-internals/lab.md) | One layer down | Apache Kafka Internal Architecture | local (3-broker override) |

## Lab environments and cost

Details, versions and licence terms: [00-lab-setup](00-lab-setup/README.md).

| Name | What | Cost |
|---|---|---|
| `local` | Docker: two single-node KRaft CP 8.4 clusters `dc-a`, `dc-b` (stand-ins for two on-prem data centres), one Schema Registry each, optional MirrorMaker 2 worker | free (developer licence [V]) |
| `cc-basic` | Confluent Cloud environment `crash-course` (Stream Governance Essentials), Basic cluster on Azure westeurope | near zero; covered by trial credits |
| `cc-std-lab06` | temporary Standard cluster — Basic has no RBAC, OAuth or audit logs | ~1–2 USD for 2 h [V] |
| `cc-std-lab07` | temporary Standard cluster for module 07's role bindings (Basic has no Kafka RBAC) | ~1–2 USD for 2 h [V] |
| `crash-course-csfle` | throwaway environment with Stream Governance Advanced (the upgrade cannot be undone) | small [V] |
| `paid-session` | same-day sessions: a **Dedicated** cluster with a public endpoint (the link from your laptop and broker-side validation, module 01) and an **Enterprise** cluster with a Private Link gateway + VNet/VM (module 03) | Enterprise < 10 USD for 3 h [V]; Dedicated: read the console's estimate; `terraform destroy` the same day |

New Confluent Cloud accounts get trial credit (USD 400 for 30 days at the time of writing [V]),
which covers all paid parts if you destroy resources the same day.

Pinned (2026-11-07): CP 8.4.0 images, Confluent CLI v4.82.0, Terraform `confluentinc/confluent`
2.94.0, `azurerm` 5.13.0, `azuread` 3.10.0.

## Prerequisites

- Docker (Docker Desktop, Colima or OrbStack) with **at least 6 GB RAM** for the containers.
- `jq`, `curl`, `dig`, `openssl`.
- Confluent CLI v4.82.0 or later, Terraform ≥ 1.9.
- A Confluent Cloud account (for `cc-basic` and the paid parts).
- For modules 03, 05 and 06: an Azure subscription where you can create resource groups, Key Vaults
  and Entra app registrations, and the `az` CLI.
- Working Kafka experience: you have run clusters, producers and consumers, and you know
  replication, consumer groups and offsets without looking them up.

Quick start:

```bash
mkdir -p ~/labs && cd ~/labs
git clone https://github.com/fps4/hybrid-kafka-on-azure.git
cd hybrid-kafka-on-azure/00-lab-setup
docker compose up -d        # dc-a, sr-a, dc-b, sr-b
```

## Status

Terraform validated, compose tested; labs not all run end to end — see the [V] items.

- Terraform blocks pass `terraform validate`; the compose files pass `docker compose config`.
- The local stack was started on 2026-11-08: `dc-a` and `sr-a` came up healthy.
- Most labs have not yet been run end to end against a live cluster. Expect small fixes on first
  run (CLI flags, REST bodies, licence behaviour). Labs mark [V] items where they occur.
- Module 01 part A was run end to end on CP 8.4.0 on 2026-11-14.
- In `00-lab-setup/terraform/paid-session.tf` the Enterprise toggle uses the older PrivateLink
  Attachment. Use module 03's lab instead, which builds the current Gateway + Access Point model.
- Content is written in our own words from the Confluent courses and current docs; course URLs are
  cited per section.

## Companion course

[Camel / Quarkus crash course](https://github.com/fps4/camel-quarkus-crash-course) — Apache Camel
routes on Quarkus with Apache Artemis for queues and Kafka for events: a small enterprise service
bus built step by step. Its lab runs against this course's `dc-a` + `sr-a`, so clone both repos
side by side under `~/labs/`.

## License

MIT — see [LICENSE](LICENSE).
