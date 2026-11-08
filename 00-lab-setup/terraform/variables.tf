variable "cloud" {
  description = "Cloud provider for all Confluent Cloud clusters in the course."
  type        = string
  default     = "AZURE"
}

variable "region" {
  description = "Azure region. West Europe (Netherlands). [V] check the region supports the cluster type and Private Link you need."
  type        = string
  default     = "westeurope"
}

variable "environment_name" {
  type    = string
  default = "crash-course"
}

variable "basic_cluster_name" {
  type    = string
  default = "cc-basic"
}

# ---------------------------------------------------------------------------
# paid-session toggles. Default false = nothing billable beyond Basic usage.
# Turn on in the morning, run the labs, turn off the same day:
#   terraform apply -var enable_paid_dedicated=true
#   ...labs...
#   terraform apply -var enable_paid_dedicated=false
# ---------------------------------------------------------------------------

variable "enable_paid_dedicated" {
  description = "Create Dedicated cluster `cc-paid` (1 CKU, single zone, PUBLIC networking). Needed for: hybrid source-initiated Cluster Linking from the laptop (dc-a -> CC), Cluster Linking destination, broker-side schema ID validation in CC."
  type        = bool
  default     = false
}

variable "enable_paid_enterprise_private" {
  description = "Create Enterprise cluster `cc-ent` + Azure PrivateLink Attachment. Enterprise is always privately networked: you need an Azure private endpoint and a client inside that VNet (module 02 owns the Azure side)."
  type        = bool
  default     = false
}

variable "dedicated_cku" {
  type    = number
  default = 1
}
