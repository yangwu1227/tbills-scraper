# Local values for cleaner code
locals {
  # Default encryption configuration
  default_encryption_configuration = var.enable_encryption ? {
    sse_algorithm = var.kms_key_arn != null ? "aws:kms" : "AES256"
    kms_key_arn   = var.kms_key_arn
  } : null

  # Default maintenance configuration
  default_maintenance_configuration = {
    iceberg_compaction = {
      status = var.enable_compaction ? "enabled" : "disabled"
      settings = {
        target_file_size_mb = var.target_file_size_mb
      }
    }
    iceberg_snapshot_management = {
      status = var.enable_snapshot_management ? "enabled" : "disabled"
      settings = {
        min_snapshots_to_keep  = var.min_snapshots_to_keep
        max_snapshot_age_hours = var.max_snapshot_age_hours
      }
    }
  }
}

# Table bucket
resource "aws_s3tables_table_bucket" "main" {
  name = var.table_bucket_name
}

# Data source for table bucket policy
data "aws_iam_policy_document" "table_bucket_policy" {
  statement {
    sid    = "GitHubActionsFullAccess"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [data.terraform_remote_state.github_actions.outputs.github_actions_role_arn]
    }

    actions = var.table_bucket_policy_actions

    resources = [
      aws_s3tables_table_bucket.main.arn,
      "${aws_s3tables_table_bucket.main.arn}/namespace/${var.namespace}",
      "${aws_s3tables_table_bucket.main.arn}/namespace/${var.namespace}/table/${var.table_name}"
    ]

    condition {
      test     = "StringEquals"
      variable = "s3tables:namespace"
      values   = [var.namespace]
    }
  }
}

# Table bucket policy
resource "aws_s3tables_table_bucket_policy" "main" {
  table_bucket_arn = aws_s3tables_table_bucket.main.arn
  resource_policy  = data.aws_iam_policy_document.table_bucket_policy.json
}

# Namespace
resource "aws_s3tables_namespace" "main" {
  namespace        = var.namespace
  table_bucket_arn = aws_s3tables_table_bucket.main.arn
}

# Table
resource "aws_s3tables_table" "main" {
  name             = var.table_name
  namespace        = var.namespace
  table_bucket_arn = aws_s3tables_table_bucket.main.arn
  format           = "ICEBERG"

  encryption_configuration  = local.default_encryption_configuration
  maintenance_configuration = local.default_maintenance_configuration

  depends_on = [aws_s3tables_namespace.main]
}
