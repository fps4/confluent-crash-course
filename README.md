---
title: Confluent crash course
subtitle: Hands-on course on Confluent Platform and Confluent Cloud for hybrid and multicloud designs — Cluster Linking, Azure private networking, governance, security. Built 2026-11-07.
---

# Confluent crash course

A short, hands-on course for **integration and platform architects** who want to learn how
Confluent works in a **hybrid and multicloud** setup: Confluent Platform in your own data centres,
Confluent Cloud on Azure, and the links, networks, schemas and security between them. The scenario
is an organisation that runs mission-critical Kafka on-prem in two data centres and wants to serve
cloud data and AI consumers without touching the operational clusters. Every module has notes
(concepts, diagrams, what changed since the official course, design advice, Q&A, sources) and a lab
you run yourself: CLI first, then the same end state in Terraform, then teardown. You do not need
to be a Kafka developer; you should be comfortable with a shell, Docker and basic Kafka terms.

Facts marked **[V]** depend on version, region or price. Check them before you state them as fact.

## Modules — suggested order

Each module has `notes.md` (concepts, Mermaid diagrams, *Changed since the course*, *In practice*,
Q&A, sources) and `lab.md`.

| Order | Module | Source | Lab environment |
|---|---|---|---|
| 1 | [08 Linking and data contracts](08-linking-and-contracts/notes.md) · [contracts](08-linking-and-contracts/notes-data-contracts.md) · [lab](08-linking-and-contracts/lab.md) | Confluent docs (no course exists) | local → cc-basic → paid-session |
| 2 | [01 Hybrid and multicloud](01-hybrid-multicloud/notes.md) · [lab](01-hybrid-multicloud/lab.md) | Hybrid and Multicloud Architecture course | local (+ paid link) |
| 3 | [02 Cloud networking — Azure](02-cloud-networking-azure/notes.md) · [lab](02-cloud-networking-azure/lab.md) | Confluent Cloud Networking course, rewritten for Azure | cc-basic → paid-session |
| 4 | [03 Stream governance](03-stream-governance/notes.md) · [lab](03-stream-governance/lab.md) | Streaming Data Governance course | local → cc-basic |
| 5 | [04 CSFLE](04-csfle/notes.md) · [lab](04-csfle/lab.md) | CSFLE course + docs | `crash-course-csfle` (Advanced) |
| 6 | [05 Security](05-cloud-security/notes.md) · [CP side](05-cloud-security/notes-cp-security.md) · [lab](05-cloud-security/lab.md) | Confluent Cloud Security course + docs | `cc-std-lab05` + paid-session |
| 7 | [06 Production systems](06-production-systems/notes.md) · [lab](06-production-systems/lab.md) | Mastering Production Data Streaming Systems | local → cc-basic |
| 8 | [07 Kafka internals](07-kafka-internals/notes.md) · [clients and storage](07-kafka-internals/notes-clients-and-storage.md) · [lab](07-kafka-internals/lab.md) | Apache Kafka Internal Architecture | local (3-broker override) |

## Lab environments and cost

Details, versions and licence terms: [00-lab-setup](00-lab-setup/README.md).

| Name | What | Cost |
|---|---|---|
| `local` | Docker: two single-node KRaft CP 8.4 clusters `dc-a`, `dc-b` (stand-ins for two on-prem data centres), one Schema Registry each, optional MirrorMaker 2 worker | free (developer licence [V]) |
| `cc-basic` | Confluent Cloud environment `crash-course` (Stream Governance Essentials), Basic cluster on Azure westeurope | near zero; covered by trial credits |
| `cc-std-lab05` | temporary Standard cluster — Basic has no RBAC, OAuth or audit logs | ~1–2 USD for 2 h [V] |
| `crash-course-csfle` | throwaway environment with Stream Governance Advanced (the upgrade cannot be undone) | small [V] |
| `paid-session` | one same-day session: **Enterprise** cluster on Azure (Private Link Gateway, Cluster Linking destination, quotas) + VNet/VM; a Dedicated toggle exists for broker-side validation | < 10 USD for 3 h estimated [V]; `terraform destroy` the same day |

New Confluent Cloud accounts get trial credit (USD 400 for 30 days at the time of writing [V]),
which covers all paid parts if you destroy resources the same day.

Pinned (2026-11-07): CP 8.4.0 images, Confluent CLI v4.82.0, Terraform `confluentinc/confluent`
2.94.0, `azurerm` 5.13.0, `azuread` 3.10.0.

## Prerequisites

- Docker (Docker Desktop, Colima or OrbStack) with **at least 6 GB RAM** for the containers.
- `jq`, `curl`, `dig`, `openssl`.
- Confluent CLI v4.82.0 or later, Terraform ≥ 1.5.
- A Confluent Cloud account (for `cc-basic` and the paid parts).
- For module 02 and the paid session: an Azure subscription where you can create resource groups,
  and the `az` CLI.
- Basic Kafka knowledge: topics, partitions, consumer groups, producers and consumers.

Quick start:

```bash
mkdir -p ~/labs && cd ~/labs
git clone https://github.com/fps4/confluent-crash-course.git
cd confluent-crash-course/00-lab-setup
docker compose up -d        # dc-a, sr-a, dc-b, sr-b
```

## Status

Terraform validated, compose tested; labs not all run end to end — see the [V] items.

- Terraform blocks pass `terraform validate`; the compose files pass `docker compose config`.
- The local stack was started on 2026-11-08: `dc-a` and `sr-a` came up healthy.
- Most labs have not yet been run end to end against a live cluster. Expect small fixes on first
  run (CLI flags, REST bodies, licence behaviour). Each lab lists its [V] items at the end.
- In `00-lab-setup/terraform/paid-session.tf` the Enterprise toggle uses the older PrivateLink
  Attachment. Use module 02's lab instead, which builds the current Gateway + Access Point model.
- Content is written in our own words from the Confluent courses and current docs; course URLs are
  cited per section.

## Companion course

[Camel / Quarkus crash course](https://github.com/fps4/camel-quarkus-crash-course) — Apache Camel
routes on Quarkus with Apache Artemis for queues and Kafka for events: a small enterprise service
bus built step by step. Its lab runs against this course's `dc-a` + `sr-a`, so clone both repos
side by side under `~/labs/`.

## License

MIT — see [LICENSE](LICENSE).
