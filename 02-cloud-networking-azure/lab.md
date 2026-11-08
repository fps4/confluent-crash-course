---
title: 02 — Lab — public endpoint with IP filtering, then Azure Private Link from a VNet
subtitle: Part A on cc-basic (free), Part B on local, Part C in the paid-session (Enterprise + ingress PrivateLink Gateway), Part D written only (ExpressRoute + on-prem DNS)
---

## Goal

1. See how a Kafka client really connects (bootstrap → metadata → brokers) and why that matters for exposure.
2. Restrict a public cluster with IP filtering and see what it does and does not block.
3. Build the Azure Private Link path end to end: gateway → private endpoint → access point → private DNS → client on a VM. Prove the laptop cannot reach it.
4. Be able to draw and explain the ExpressRoute + on-prem DNS version without building it.

## Environments used

| Part | Environment | Cost |
|---|---|---|
| A | `cc-basic` (env `crash-course`, Basic cluster on Azure `westeurope`) | free (first Basic eCKU free) |
| B | `local` (`dc-a`) | free |
| C | `paid-session` — Enterprise cluster on Azure `westeurope` + Azure VNet + VM, same day, destroyed after | about 5–10 USD for 3 hours **[V]** |
| D | none (paper) | — |

See `../00-lab-setup/` for `local` and `cc-basic`. The Terraform base there exposes outputs for the environment id, cluster id and Schema Registry endpoint; this lab reads them with `terraform output`. Check the exact output names in `00-lab-setup/terraform/outputs.tf` and adjust the commands below if they differ.

**Why Enterprise for the paid session, not Dedicated:** Enterprise on Azure is private-only, elastic (bills per eCKU-hour, about 1.75–2.25 USD list **[V]**), and provisions in minutes. A Dedicated cluster needs a Confluent network first (often an hour or more to provision) and bills at least 1 CKU for every hour it exists. For a 2–3 hour lab, Enterprise is cheaper and leaves time for the actual tests.

## Prerequisites

- `confluent` CLI v4.x (`confluent version`; IP filter commands need 4.18.0+), logged in as an OrganizationAdmin or with an API key for `sa-crash-admin` that has EnvironmentAdmin on `crash-course`.
- `az` CLI logged in to a subscription where you can create resource groups (`az account show`).
- `terraform` 1.9+.
- `dig`, `openssl`, `jq`.
- For Part B: `local` stack running (`00-lab-setup`).

Shell variables used below:

```bash
cd ../00-lab-setup/terraform
export CC_ENV=$(terraform output -raw environment_id)
export CC_BASIC=$(terraform output -raw basic_cluster_id)
cd -
export AZ_REGION=westeurope
export MY_IP=$(curl -s https://ifconfig.me)
```

---

## Part A — Public endpoint and IP filtering on `cc-basic` (CLI)

### A1. Look at the endpoint

```bash
confluent environment use $CC_ENV
confluent kafka cluster use $CC_BASIC
confluent kafka cluster describe -o json | jq '{id, endpoint, rest_endpoint, provider, region, type}'
```

Expected: `endpoint` like `SASL_SSL://pkc-xxxxx.westeurope.azure.confluent.cloud:9092`, `type` `BASIC`.

```bash
BOOT=$(confluent kafka cluster describe -o json | jq -r .endpoint | sed 's#SASL_SSL://##; s#:9092##')
dig +short $BOOT
```

Expected: one or a few public IPs (or a CNAME chain then IPs). Note them. They are **not static**; never put them in a firewall rule. Allow by FQDN.

```bash
openssl s_client -connect $BOOT:9092 -servername $BOOT </dev/null 2>/dev/null | openssl x509 -noout -subject -ext subjectAltName
```

Expected: a wildcard SAN for the cluster domain. That wildcard is how all brokers share a few IPs behind the proxy, routed by SNI.

### A2. Produce and consume on the public endpoint

```bash
confluent api-key create --resource $CC_BASIC --description "lab02 public" -o json > /tmp/lab02-key.json
confluent api-key use $(jq -r .api_key /tmp/lab02-key.json) --resource $CC_BASIC

confluent kafka topic create orders.order-status.updated.v1 --partitions 3
printf 'SO-1001001:{"orderId":"SO-1001001","status":"READY"}\nSO-1001002:{"orderId":"SO-1001002","status":"DELAYED"}\n' | \
  confluent kafka topic produce orders.order-status.updated.v1 --parse-key --delimiter ":"
confluent kafka topic consume orders.order-status.updated.v1 -b --print-key
```

Expected: the two records printed with their keys. Stop the consumer with Ctrl-C. (Wait a minute after creating the API key; new keys take a short time to propagate.)

### A3. See the broker list the client gets

```bash
# Uses Kafka CLI tools from a CP 8.x or Apache Kafka 4.x install
cat > /tmp/cc-basic.properties <<EOF
bootstrap.servers=$BOOT:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='$(jq -r .api_key /tmp/lab02-key.json)' password='$(jq -r .api_secret /tmp/lab02-key.json)';
EOF
kafka-broker-api-versions --bootstrap-server $BOOT:9092 --command-config /tmp/cc-basic.properties | grep -E '^[a-z0-9-]+\.' | cut -d' ' -f1
```

Expected: several host names like `b0-pkc-xxxxx.westeurope.azure.confluent.cloud:9092`, `b1-…`. The bootstrap name is not a broker. Each broker is a separate name, all under one domain. Keep this picture for Part B.

### A4. IP filtering

Create an IP group for your own IP and a management filter (the first filter must include your IP; the CLI refuses a change that would lock you out):

```bash
confluent iam ip-group create lab02-my-ip --cidr-blocks ${MY_IP}/32 -o json | tee /tmp/ipg.json
IPG=$(jq -r .id /tmp/ipg.json)
confluent iam ip-filter create lab02-mgmt --resource-group multiple \
  --operations MANAGEMENT,SCHEMA --ip-groups $IPG --environment $CC_ENV
confluent iam ip-filter list
```

Expected: the filter is listed. `confluent kafka topic list` still works from your laptop.

Now block Kafka data from all public networks for this environment. The CLI help lists only `MANAGEMENT`, `SCHEMA`, `FLINK` for `--operations` **[V]**; the Terraform provider and the docs list `KAFKA_DATA`. Try the CLI first; if it rejects `KAFKA_DATA`, use the Terraform block at the end of this part.

```bash
confluent iam ip-filter create lab02-block-data --resource-group multiple \
  --operations KAFKA_DATA,KAFKA_DISCOVERY --no-public-networks --environment $CC_ENV
sleep 120
confluent kafka topic consume orders.order-status.updated.v1 -b
```

Expected if `KAFKA_DATA` filtering applies to Basic clusters: the consume fails with an authorization or connection error. If it still works, record that as a finding: `KAFKA_DATA` filtering does not apply to this cluster type or region **[V]**. Either way, management calls (`confluent kafka topic list`) keep working because the management filter allows your IP.

Remove the blocking filter:

```bash
confluent iam ip-filter list
confluent iam ip-filter delete <id-of-lab02-block-data>
```

Terraform version of the same filters (put in a scratch folder, `terraform apply`, then `terraform destroy`):

```hcl
terraform {
  required_providers {
    confluent = { source = "confluentinc/confluent", version = "~> 2.94" }
  }
}
provider "confluent" {}   # CONFLUENT_CLOUD_API_KEY / _SECRET from env (sa-crash-admin, OrganizationAdmin needed for org-level filters)

variable "my_ip"  { type = string }
variable "env_id" { type = string }

resource "confluent_ip_group" "my_ip" {
  group_name  = "lab02-my-ip"
  cidr_blocks = ["${var.my_ip}/32"]
}

resource "confluent_ip_filter" "mgmt" {
  filter_name      = "lab02-mgmt"
  resource_group   = "multiple"
  operation_groups = ["MANAGEMENT", "SCHEMA"]
  ip_groups        = [confluent_ip_group.my_ip.id]
  resource_scope   = data.confluent_environment.lab.resource_name   # environment scope; omit for the whole organisation
}

data "confluent_environment" "lab" {
  id = var.env_id
}

resource "confluent_ip_filter" "block_data" {
  filter_name      = "lab02-block-data"
  resource_group   = "multiple"
  operation_groups = ["KAFKA_DATA", "KAFKA_DISCOVERY"]
  ip_groups        = ["ipg-none"]   # predefined "No Public Networks" group
  resource_scope   = data.confluent_environment.lab.resource_name
}
```

Clean up Part A IP filters and group before you leave:

```bash
confluent iam ip-filter list    # delete lab02-* filters
confluent iam ip-group delete $IPG
```

---

## Part B — Advertised listeners on `local` (`dc-a`)

Purpose: see the on-prem problem with your own eyes.

```bash
docker compose -f ../00-lab-setup/docker-compose.yml exec dc-a \
  kafka-configs --bootstrap-server localhost:9092 --describe --entity-type brokers --entity-name 1 --all \
  | grep -E 'listeners|advertised'
```

Expected: `listeners` and `advertised.listeners` with one or more named listeners (for example an internal Docker name and a `localhost` one). Exact names depend on `00-lab-setup`.

```bash
kafka-broker-api-versions --bootstrap-server localhost:<dc-a host port> | grep -E '^[^ ]+:[0-9]+ ' | cut -d' ' -f1
```

Expected: the **advertised** address, not necessarily the address you typed. Now imagine an Azure client: it would need each broker's advertised name to resolve and route from Azure, a listener for that zone, and that name in the certificate. With 6 brokers on OpenShift that is 6 routes, 6 DNS names and 6 firewall entries per zone, changed on every broker move. That is the argument in `notes.md` §2.11.

Optional: change the `advertised.listeners` host for the external listener in a copy of the compose file to a name your laptop cannot resolve, restart, and watch the client bootstrap succeed and then fail with "could not be established. Node may not be available". That is the classic symptom.

---

## Part C — Private Link from an Azure VNet (`paid-session`)

Plan the session: about 20 min build, 60–90 min tests, 10 min teardown. Do the CLI path **or** the Terraform path, not both.

### C1. CLI path

**1. Azure network and VM**

```bash
RG=rg-crash-lab02
az group create -n $RG -l $AZ_REGION
az network vnet create -g $RG -n vnet-lab02 --address-prefixes 10.42.0.0/16 \
  --subnet-name snet-pe --subnet-prefixes 10.42.1.0/24
az network vnet subnet create -g $RG --vnet-name vnet-lab02 -n snet-vm --address-prefixes 10.42.2.0/24
az vm create -g $RG -n vm-lab02 --image Ubuntu2404 --size Standard_B2s \
  --vnet-name vnet-lab02 --subnet snet-vm --admin-username azureuser --generate-ssh-keys \
  --public-ip-sku Standard --nsg-rule NONE
az network nsg rule create -g $RG --nsg-name vm-lab02NSG -n ssh-from-me --priority 100 \
  --source-address-prefixes ${MY_IP}/32 --destination-port-ranges 22 --access Allow --protocol Tcp
```

Expected: VM with a public IP for SSH from your IP only. (Azure Bastion is the proper way; it costs more per hour.)

**2. Confluent gateway and Enterprise cluster**

```bash
confluent environment use $CC_ENV
confluent network gateway create lab02-ingress --cloud azure --region $AZ_REGION --type ingress-privatelink -o json | tee /tmp/gw.json
GW=$(jq -r .id /tmp/gw.json)
confluent network gateway describe $GW -o json | jq .
```

Expected: gateway id `gw-…`, state `CREATED`, and a Private Link Service **alias** and **resource id** (field names vary by CLI version **[V]**; look for `private_link_service_alias` / `..._resource_id`).

```bash
confluent kafka cluster create cc-ent-lab02 --cloud azure --region $AZ_REGION --type enterprise --availability high -o json | tee /tmp/ent.json
ENT=$(jq -r .id /tmp/ent.json)
```

Expected: an `lkc-…` id within a few minutes. Its endpoint is not reachable from your laptop.

**3. Private endpoint to the gateway's Private Link Service**

```bash
PLS_ID=$(confluent network gateway describe $GW -o json | jq -r '.. | .private_link_service_resource_id? // empty' | head -1)
az network private-endpoint create -g $RG -n pe-confluent --vnet-name vnet-lab02 --subnet snet-pe \
  --private-connection-resource-id "$PLS_ID" --connection-name cc-lab02 \
  --manual-request true --request-message "crash-course lab02"
PE_ID=$(az network private-endpoint show -g $RG -n pe-confluent --query id -o tsv)
PE_IP=$(az network private-endpoint show -g $RG -n pe-confluent --query 'customDnsConfigs[0].ipAddresses[0]' -o tsv)
# if PE_IP is empty, read it from the NIC:
[ -z "$PE_IP" ] && PE_IP=$(az network nic show --ids $(az network private-endpoint show -g $RG -n pe-confluent --query 'networkInterfaces[0].id' -o tsv) --query 'ipConfigurations[0].privateIPAddress' -o tsv)
echo $PE_IP
```

Expected: endpoint created with connection state **Pending** (manual request, cross-tenant). If Azure rejects the resource id, retry with the alias using `--private-connection-resource-id <alias>` **[V]** (the Terraform path uses `private_connection_resource_alias`, which is the documented way).

**4. Register the endpoint as an access point**

```bash
confluent network access-point private-link ingress-endpoint create lab02-ap \
  --cloud azure --gateway $GW --endpoint-resource-id "$PE_ID" -o json | tee /tmp/ap.json
AP=$(jq -r .id /tmp/ap.json)
sleep 60
confluent network access-point private-link ingress-endpoint describe $AP -o json | jq .
az network private-endpoint show -g $RG -n pe-confluent --query 'manualPrivateLinkServiceConnections[0].privateLinkServiceConnectionState.status' -o tsv
```

Expected: access point `READY` with a **DNS domain** like `ap-xxxxx.westeurope.azure.accesspoint.confluent.cloud`; Azure shows **Approved**.

**5. Private DNS**

```bash
ZONE=$(confluent network access-point private-link ingress-endpoint describe $AP -o json | jq -r '.. | .dns_domain? // empty' | head -1)
az network private-dns zone create -g $RG -n $ZONE
az network private-dns record-set a add-record -g $RG -z $ZONE -n '*' -a $PE_IP
az network private-dns record-set a update -g $RG -z $ZONE -n '*' --set ttl=60
az network private-dns link vnet create -g $RG -z $ZONE -n link-lab02 -v vnet-lab02 -e false
```

Expected: zone with one wildcard A record, linked to the VNet.

**6. Credentials and endpoint for the client**

```bash
confluent api-key create --resource $ENT --description "lab02 private" -o json > /tmp/lab02-ent-key.json
confluent kafka cluster describe $ENT -o json | jq '.endpoint, .endpoints'
```

Expected: a bootstrap like `lkc-xxxxx-ap-xxxxx.westeurope.azure.accesspoint.glb.confluent.cloud:9092` for the access point **[V]** (with gateways, a cluster can list one endpoint per access point; pick the one with your access point id).

**7. Test from the laptop (should fail)**

```bash
BOOT_PL=<bootstrap from step 6, without SASL_SSL:// and :9092>
dig +short $BOOT_PL           # resolves: public CNAME into the ap- domain, then nothing useful / no private IP
nc -vz -w 5 $BOOT_PL 9092     # fails: no route
```

**8. Test from the VM**

```bash
VM_IP=$(az vm show -d -g $RG -n vm-lab02 --query publicIps -o tsv)
scp /tmp/lab02-ent-key.json azureuser@$VM_IP:
ssh azureuser@$VM_IP
# on the VM:
sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-21-jre-headless dnsutils jq netcat-openbsd
curl -sL --http1.1 https://cnfl.io/cli | sudo sh -s -- -b /usr/local/bin
KV=4.1.1   # pick the current Kafka 4.x from https://downloads.apache.org/kafka/ [V]
curl -sO https://downloads.apache.org/kafka/$KV/kafka_2.13-$KV.tgz && tar xzf kafka_2.13-$KV.tgz
export PATH=$PATH:$HOME/kafka_2.13-$KV/bin

BOOT_PL=<same bootstrap>
dig +short $BOOT_PL           # CNAME, then 10.42.1.x (the private endpoint)
nc -vz $BOOT_PL 9092          # succeeded

cat > cc-ent.properties <<EOF
bootstrap.servers=$BOOT_PL:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='$(jq -r .api_key lab02-ent-key.json)' password='$(jq -r .api_secret lab02-ent-key.json)';
EOF
kafka-topics.sh --bootstrap-server $BOOT_PL:9092 --command-config cc-ent.properties \
  --create --topic tracking.item-scan.recorded.v1 --partitions 3
kafka-broker-api-versions.sh --bootstrap-server $BOOT_PL:9092 --command-config cc-ent.properties | grep -E '^[^ ]+:9092' | cut -d' ' -f1
for b in $(kafka-broker-api-versions.sh --bootstrap-server $BOOT_PL:9092 --command-config cc-ent.properties | grep -oE '^[^ ]+:9092' | cut -d: -f1 | head -3); do dig +short $b | tail -1; done
echo 'IT-123:{"itemTag":"IT-123","scan":"SORTER-3"}' | kafka-console-producer.sh --bootstrap-server $BOOT_PL:9092 \
  --producer.config cc-ent.properties --topic tracking.item-scan.recorded.v1 --property parse.key=true --property key.separator=:
kafka-console-consumer.sh --bootstrap-server $BOOT_PL:9092 --consumer.config cc-ent.properties \
  --topic tracking.item-scan.recorded.v1 --from-beginning --max-messages 1
```

Expected:
- Every broker name resolves to the **same private endpoint IP** (the wildcard). That is the point of the SNI proxy design.
- Topic created, one record produced and consumed.
- `--property` may print a deprecation warning on newer Kafka 4.x tools (`--reader-property` / `--formatter-property`) **[V]**.

Also try the Confluent CLI from the VM (`confluent login`, `confluent kafka topic list --cluster $ENT`) — the control plane is public, the data plane goes through the private endpoint.

**9. Negative tests worth doing**

| Test | Expected |
|---|---|
| Delete the VNet link of the private zone, flush, `dig` again | bootstrap name no longer resolves to 10.42.x → client fails. This is the most common real-world error. |
| Re-add the link | works again (TTL 60 s). |
| In the Console, open the topic browser from your laptop | messages cannot be loaded — the browser must reach the private endpoint. |

### C2. Terraform path (same end state)

Folder: a scratch folder, for example `~/crash-course/lab02-pl/`. Credentials: `CONFLUENT_CLOUD_API_KEY/SECRET` for `sa-crash-admin`, `ARM_SUBSCRIPTION_ID` + `az login`.

```hcl
terraform {
  required_version = ">= 1.9"
  required_providers {
    confluent = { source = "confluentinc/confluent", version = "~> 2.94" }  # 2.94.0 released 2026-11-06
    azurerm   = { source = "hashicorp/azurerm",      version = "~> 5.13" }  # 5.13.0 released 2026-11-05; main branch is preparing 6.0
  }
}

provider "confluent" {}
provider "azurerm" {
  features {}
}

variable "env_id"  { type = string }                 # from 00-lab-setup output
variable "region"  { default = "westeurope" }
variable "my_ip"   { type = string }
variable "ssh_pub" { default = "~/.ssh/id_rsa.pub" }

# ---------- Azure ----------
resource "azurerm_resource_group" "rg" {
  name     = "rg-crash-lab02"
  location = var.region
}

resource "azurerm_virtual_network" "vnet" {
  name                = "vnet-lab02"
  location            = var.region
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = ["10.42.0.0/16"]
}

resource "azurerm_subnet" "pe" {
  name                 = "snet-pe"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.42.1.0/24"]
}

resource "azurerm_subnet" "vm" {
  name                 = "snet-vm"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.42.2.0/24"]
}

resource "azurerm_network_security_group" "vm" {
  name                = "nsg-vm-lab02"
  location            = var.region
  resource_group_name = azurerm_resource_group.rg.name
  security_rule {
    name                       = "ssh-from-me"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "${var.my_ip}/32"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "vm" {
  subnet_id                 = azurerm_subnet.vm.id
  network_security_group_id = azurerm_network_security_group.vm.id
}

resource "azurerm_public_ip" "vm" {
  name                = "pip-vm-lab02"
  location            = var.region
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "vm" {
  name                = "nic-vm-lab02"
  location            = var.region
  resource_group_name = azurerm_resource_group.rg.name
  ip_configuration {
    name                          = "ipcfg"
    subnet_id                     = azurerm_subnet.vm.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.vm.id
  }
}

resource "azurerm_linux_virtual_machine" "vm" {
  name                  = "vm-lab02"
  location              = var.region
  resource_group_name   = azurerm_resource_group.rg.name
  size                  = "Standard_B2s"
  admin_username        = "azureuser"
  network_interface_ids = [azurerm_network_interface.vm.id]
  admin_ssh_key {
    username   = "azureuser"
    public_key = file(pathexpand(var.ssh_pub))
  }
  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }
  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }
  custom_data = base64encode(<<-EOT
    #cloud-config
    packages: [openjdk-21-jre-headless, dnsutils, jq, netcat-openbsd]
    runcmd:
      - curl -sL --http1.1 https://cnfl.io/cli | sh -s -- -b /usr/local/bin
  EOT
  )
}

# ---------- Confluent: gateway ----------
resource "confluent_gateway" "ingress" {
  display_name = "lab02-ingress"
  environment { id = var.env_id }
  azure_ingress_private_link_gateway { region = var.region }
}

# ---------- Azure: private endpoint to Confluent's Private Link Service ----------
resource "azurerm_private_endpoint" "cc" {
  name                = "pe-confluent"
  location            = var.region
  resource_group_name = azurerm_resource_group.rg.name
  subnet_id           = azurerm_subnet.pe.id
  private_service_connection {
    name                              = "cc-lab02"
    private_connection_resource_alias = confluent_gateway.ingress.azure_ingress_private_link_gateway[0].private_link_service_alias
    is_manual_connection              = true          # cross-tenant: Confluent approves when the access point is registered
    request_message                   = "crash-course lab02"
  }
}

# ---------- Confluent: access point = register that endpoint ----------
resource "confluent_access_point" "ap" {
  display_name = "lab02-ap"
  environment { id = var.env_id }
  gateway     { id = confluent_gateway.ingress.id }
  azure_ingress_private_link_endpoint {
    private_endpoint_resource_id = azurerm_private_endpoint.cc.id
  }
}

# ---------- Confluent: Enterprise cluster ----------
resource "confluent_kafka_cluster" "ent" {
  display_name = "cc-ent-lab02"
  availability = "HIGH"
  cloud        = "AZURE"
  region       = var.region
  enterprise {}
  environment { id = var.env_id }
}

# ---------- Azure: private DNS ----------
resource "azurerm_private_dns_zone" "cc" {
  name                = confluent_access_point.ap.azure_ingress_private_link_endpoint[0].dns_domain
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_private_dns_a_record" "wildcard" {
  name                = "*"
  private_dns_zone_id = azurerm_private_dns_zone.cc.id      # azurerm 5.x: zone id, not zone_name + resource_group_name
  ttl                 = 60
  records             = [azurerm_private_endpoint.cc.private_service_connection[0].private_ip_address]
}

resource "azurerm_private_dns_zone_virtual_network_link" "cc" {
  name                  = "link-lab02"
  private_dns_zone_id   = azurerm_private_dns_zone.cc.id    # azurerm 5.x
  virtual_network_id    = azurerm_virtual_network.vnet.id
  registration_enabled  = false
}

# ---------- Confluent: client identity ----------
resource "confluent_service_account" "lab02" {
  display_name = "sa-lab02-client"
  description  = "lab02 private link client"
}

resource "confluent_role_binding" "lab02_admin" {
  principal   = "User:${confluent_service_account.lab02.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.ent.rbac_crn
}

resource "confluent_api_key" "lab02" {
  display_name = "lab02-ent-key"
  owner {
    id          = confluent_service_account.lab02.id
    api_version = confluent_service_account.lab02.api_version
    kind        = confluent_service_account.lab02.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.ent.id
    api_version = confluent_kafka_cluster.ent.api_version
    kind        = confluent_kafka_cluster.ent.kind
    environment { id = var.env_id }
  }
  depends_on = [confluent_role_binding.lab02_admin, confluent_access_point.ap]
}

output "vm_public_ip"   { value = azurerm_public_ip.vm.ip_address }
output "pe_ip"          { value = azurerm_private_endpoint.cc.private_service_connection[0].private_ip_address }
output "dns_zone"       { value = azurerm_private_dns_zone.cc.name }
output "bootstrap" {
  # With gateways a cluster lists one endpoint per access point; pick ours.
  value = [for e in confluent_kafka_cluster.ent.endpoints : e.bootstrap_endpoint if e.access_point_id == confluent_access_point.ap.id]
}
output "api_key"        { value = confluent_api_key.lab02.id }
output "api_secret" {
  value     = confluent_api_key.lab02.secret
  sensitive = true
}
```

Notes:
- Topics are **not** created by Terraform here: `confluent_kafka_topic` calls the cluster REST endpoint, which is private. Either run Terraform from the VM (or a CI runner in the VNet), or create topics with the CLI from the VM as in C1 step 8.
- This block passes `terraform validate` with confluent 2.94.0 and azurerm 5.13.0 (checked 2026-11-07). Note azurerm 5.x changed the private DNS record and VNet link resources to take `private_dns_zone_id`; older examples with `zone_name` + `resource_group_name` fail.
- Then do C1 step 8 on the VM using the outputs.

### Verification checklist

- [ ] Access point `READY`, Azure endpoint connection `Approved`.
- [ ] `dig` on the VM: bootstrap and broker names → private endpoint IP.
- [ ] Laptop: name resolves publicly to a CNAME but TCP 9092 fails.
- [ ] Produce/consume works on the VM.
- [ ] Removing the private zone VNet link breaks it; re-adding fixes it.

### Teardown (same day)

CLI path:

```bash
confluent api-key delete $(jq -r .api_key /tmp/lab02-ent-key.json) --force
confluent kafka cluster delete $ENT --force
confluent network access-point private-link ingress-endpoint delete $AP --force
confluent network gateway delete $GW --force
az group delete -n rg-crash-lab02 --yes --no-wait
```

Terraform path: `terraform destroy`. Then check in the Console (Network management, Clusters) and in `az group list -o table` that nothing from lab02 is left. Billing: check the Confluent usage page the next day.

### Cost note

| Item | Rough cost for 3 h **[V]** |
|---|---|
| Enterprise cluster, ~1 eCKU idle | 1.75–2.25 USD/eCKU-h list → about 6–7 USD |
| Confluent throughput | cents (KB of test data) |
| Azure private endpoint | ~0.01 USD/h + ~0.01 USD/GB |
| B2s VM + Standard public IP + disk | < 0.20 USD |
| Total | under 10 USD |

Dedicated alternative for comparison: at least 1 CKU for every hour the cluster exists plus network setup time; usually several times the Enterprise cost for this lab **[V]**.

---

## Part D — Written walkthrough: ExpressRoute and on-prem DNS (not built)

Draw this on paper or a whiteboard, then explain it in 3 minutes.

**1. Routing**
- ExpressRoute circuits (two, in two peering locations of the same metro) with **private peering** to the ExpressRoute gateway in the **hub VNet**.
- Private endpoint for the Confluent ingress gateway in a `snet-privatelink` subnet of the hub. On-prem receives the hub address space by BGP from the gateway; the PE subnet is inside it, so no extra route is needed.
- Spokes (data & analytics on Databricks, AI on ARO) are peered to the hub with "use remote gateway". They reach the PE IP directly.
- If the hub has Azure Firewall, decide whether on-prem → PE traffic is inspected. If yes: UDR on the GatewaySubnet to the firewall, and enable private endpoint network policies on the PE subnet **[V]**. Check the firewall's TCP idle timeout against long-lived Kafka connections.

**2. Flows**
| From | To | Port | Direction |
|---|---|---|---|
| CP brokers (Cluster Link) | PE IP (Kafka) | 9092 | outbound from DC |
| CP Schema Registry exporter (Schema Linking) | PE IP (Schema Registry) | 443 | outbound from DC |
| Azure consumers | PE IP | 9092 / 443 | within Azure |
| Nothing | on-prem brokers | — | no inbound into the DC |

**3. DNS**
- Azure DNS Private Resolver in the hub: inbound endpoint `10.10.2.4`, outbound endpoint + ruleset for on-prem zones such as `corp.example.local`.
- Private DNS zone `<accesspoint dns_domain>` with `* A <PE IP>`, linked to the hub VNet.
- On-prem DNS (AD DNS or Infoblox): conditional forwarder for `<accesspoint dns_domain>` → `10.10.2.4`. Public names (`…accesspoint.glb.confluent.cloud`) resolve through normal internet recursion; if the DC has no internet recursion, forward `confluent.cloud` to the inbound endpoint instead.
- Prod and non-prod environments = two gateways = two zones = two forwarders.
- Test from a CP broker pod: `nslookup <bootstrap>` → PE IP; `openssl s_client -connect <bootstrap>:9092 -servername <broker name>`.

**4. Failure modes to name**
- Zone not linked to the resolver's VNet → names resolve publicly → timeouts.
- One ExpressRoute circuit down → link stalls, catches up after; alarm on link lag, not on single errors.
- Someone adds a per-broker A record → works until Confluent changes broker names.
- Firewall idle timeout shorter than the connection's idle time → periodic reconnect storms.

**5. Say it in one sentence**
"The DC opens one outbound, quota-capped flow over ExpressRoute private peering to a private endpoint in the hub; DNS for that one zone is forwarded to the Private Resolver; Azure teams use the same endpoint through the hub; nothing in Azure can address an on-prem broker."
