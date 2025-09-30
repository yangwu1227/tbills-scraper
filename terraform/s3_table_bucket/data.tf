# Data source to get github actions role arn from remote state
data "terraform_remote_state" "github_actions" {
  backend = "s3"
  config = {
    bucket  = local.terraform_remote_state_bucket
    key     = var.terraform_remote_state_github_actions_s3_key
    region  = local.aws_region
    profile = local.profile
  }
}
