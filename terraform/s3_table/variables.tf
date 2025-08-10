variable "region" {
  type        = string
  description = "AWS region where resources will be deployed"
}

variable "profile" {
  type        = string
  description = "AWS configuration profile with all required permissions"
}

variable "terraform_remote_state_bucket" {
  type        = string
  description = "Name of the S3 bucket where the Terraform state files are stored"
}

variable "terraform_remote_state_github_actions_s3_key" {
  type        = string
  description = "S3 key for the Terraform remote state of the github actions role created"
}

variable "table_bucket_name" {
  description = "Name of the S3 Tables bucket"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.table_bucket_name))
    error_message = "Table bucket name must be 3-63 characters, contain only lowercase letters, numbers, and hyphens, and begin/end with a letter or number"
  }
}

variable "table_bucket_policy_actions" {
  description = "List of actions for the table bucket policy - grants all necessary permissions"
  type        = list(string)
}

variable "namespace" {
  description = "Name of the namespace to create"
  type        = string

  validation {
    condition = (
      length(var.namespace) >= 1 &&
      length(var.namespace) <= 255 &&
      can(regex("^[a-z0-9][a-z0-9_]*[a-z0-9]$", var.namespace)) &&
      !startswith(var.namespace, "aws")
    )
    error_message = "Namespace name must be 1-255 characters, contain only lowercase letters, numbers, and underscores, begin and end with a letter or number (no underscores at start/end), and not start with 'aws'"
  }
}

variable "table_name" {
  description = "Name of the table to create"
  type        = string

  validation {
    condition = (
      length(var.table_name) >= 1 &&
      length(var.table_name) <= 255 &&
      can(regex("^[a-z0-9][a-z0-9_]*[a-z0-9]$", var.table_name))
    )
    error_message = "Table name must be 1-255 characters, contain only lowercase letters, numbers, and underscores, and begin and end with a letter or number (no underscores at start/end)"
  }
}

# Maintenance configuration variables
variable "enable_compaction" {
  description = "Whether to enable automatic compaction"
  type        = bool
  default     = true
}

variable "target_file_size_mb" {
  description = "Target file size in MB for compaction"
  type        = number
  default     = 128

  validation {
    condition     = var.target_file_size_mb >= 64 && var.target_file_size_mb <= 512
    error_message = "Target file size must be between 64 and 512 MB"
  }
}

variable "enable_snapshot_management" {
  description = "Whether to enable automatic snapshot management"
  type        = bool
  default     = true
}

variable "min_snapshots_to_keep" {
  description = "Minimum number of snapshots to keep"
  type        = number
  default     = 3

  validation {
    condition     = var.min_snapshots_to_keep >= 1
    error_message = "Must keep at least 1 snapshot"
  }
}

variable "max_snapshot_age_hours" {
  description = "Maximum age of snapshots in hours before deletion"
  type        = number
  default     = 168 # 7 days

  validation {
    condition     = var.max_snapshot_age_hours > 0
    error_message = "Maximum snapshot age must be at least 1 hour"
  }
}

# Encryption variables
variable "enable_encryption" {
  description = "Whether to enable encryption for the table"
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for encryption. If null, AES256 will be used"
  type        = string
  default     = null
}
