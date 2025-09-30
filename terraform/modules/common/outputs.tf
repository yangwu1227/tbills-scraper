output "aws_region" {
  value = data.aws_region.current.region
  description = "AWS region where resources will be deployed"
}

output "aws_account_id" {
  value = data.aws_caller_identity.current.account_id
  description = "AWS account ID where resources are deployed"
}

output "profile" {
    value = local.profile
    description = "AWS configuration profile with all required permissions"
}

output "project_prefix" {
  value = local.project_prefix
  description = "Prefix to use when naming all resources for the project"
}

output "terraform_remote_state_bucket" {
  value = local.terraform_remote_state_bucket
  description = "S3 bucket name for storing Terraform remote state"
}
