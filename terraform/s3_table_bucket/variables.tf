variable "region" {
  type        = string
  description = "AWS region where resources will be deployed"
  default     = "us-east-1"
}

variable "profile" {
  type        = string
  description = "AWS configuration profile with all required permissions"
  default     = "admin"
}

variable "terraform_remote_state_bucket" {
  type        = string
  description = "Name of the S3 bucket where the Terraform state files are stored"
  default     = "tf-cf-templates"
}

variable "terraform_remote_state_github_actions_s3_key" {
  type        = string
  description = "S3 key for the Terraform remote state of the GitHub Actions role created"
  default     = "terraform-states/tbills-scraper/iam/terraform.tfstate"
}

variable "table_bucket_name" {
  description = "Name of the S3 Tables bucket"
  type        = string
  default     = "financial-data"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.table_bucket_name))
    error_message = "Table bucket name must be 3-63 characters, contain only lowercase letters, numbers, and hyphens, and begin/end with a letter or number"
  }
}

# See https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-setting-up.html#s3-tables-actions
variable "table_bucket_policy_actions" {
  description = "List of actions for the table bucket policy - grants all necessary permissions"
  type        = list(string)
  default = [
    # Table bucket management
    "s3tables:CreateTableBucket",
    "s3tables:GetTableBucket",
    "s3tables:DeleteTableBucket",
    "s3tables:ListTableBuckets",
    # Namespace management
    "s3tables:CreateNamespace",
    "s3tables:GetNamespace",
    "s3tables:ListNamespaces",
    "s3tables:DeleteNamespace",
    # Table management
    "s3tables:CreateTable",
    "s3tables:GetTable",
    "s3tables:ListTables",
    "s3tables:DeleteTable",
    "s3tables:RenameTable",
    # Data operations
    "s3tables:GetTableMetadataLocation",
    "s3tables:UpdateTableMetadataLocation",
    "s3tables:PutTableData",
    "s3tables:GetTableData",
    # Policy management
    "s3tables:PutTableBucketPolicy",
    "s3tables:GetTableBucketPolicy",
    "s3tables:DeleteTableBucketPolicy",
    "s3tables:PutTablePolicy",
    "s3tables:GetTablePolicy",
    "s3tables:DeleteTablePolicy",
    # Maintenance configuration
    "s3tables:GetTableBucketMaintenanceConfiguration",
    "s3tables:PutTableBucketMaintenanceConfiguration",
    "s3tables:GetTableMaintenanceConfiguration",
    "s3tables:PutTableMaintenanceConfiguration",
    "s3tables:GetTableMaintenanceJobStatus",
    # Encryption configuration
    "s3tables:GetTableBucketEncryption",
    "s3tables:PutTableBucketEncryption",
    "s3tables:DeleteTableBucketEncryption",
    "s3tables:GetTableEncryption",
    "s3tables:PutTableEncryption"
  ]
}

# Table configuration
variable "namespace" {
  description = "Name of the namespace to create"
  type        = string
  default     = "treasury_bills"

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
  default     = "daily_yields"

  validation {
    condition = (
      length(var.table_name) >= 1 &&
      length(var.table_name) <= 255 &&
      can(regex("^[a-z0-9][a-z0-9_]*[a-z0-9]$", var.table_name))
    )
    error_message = "Table name must be 1-255 characters, contain only lowercase letters, numbers, and underscores, and begin and end with a letter or number (no underscores at start/end)"
  }
}

# Table-level maintenance configuration variables
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
    error_message = "Minimum snapshots to keep must be at least 1"
  }
}

variable "max_snapshot_age_hours" {
  description = "Maximum age of snapshots in hours before deletion"
  type        = number
  default     = 168 # 7 days

  validation {
    condition     = var.max_snapshot_age_hours >= 1
    error_message = "Maximum snapshot age must be at least 1 hour"
  }
}

# Table-level encryption variables
variable "enable_encryption" {
  description = "Whether to enable encryption for the table"
  type        = bool
  default     = true
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for encryption. If null, AES256 will be used"
  type        = string
  default     = null
  sensitive   = true
}

# Bucket-level maintenance configuration variables
variable "enable_unreferenced_file_removal" {
  description = "Whether to enable unreferenced file removal at the bucket level"
  type        = bool
  default     = true
}

variable "unreferenced_days" {
  description = "Number of days after which unreferenced objects are marked for deletion"
  type        = number
  default     = 3

  validation {
    condition     = var.unreferenced_days >= 1
    error_message = "Unreferenced days must be at least 1"
  }
}

variable "non_current_days" {
  description = "Number of days after which non-current objects are permanently deleted"
  type        = number
  default     = 10

  validation {
    condition     = var.non_current_days >= 1
    error_message = "Non-current days must be at least 1"
  }
}
