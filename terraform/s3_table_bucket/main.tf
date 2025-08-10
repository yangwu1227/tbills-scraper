terraform {
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
}

# Data source to get github actions role arn from remote state
data "terraform_remote_state" "github_actions" {
  backend = "s3"
  config = {
    bucket  = var.terraform_remote_state_bucket
    key     = var.terraform_remote_state_github_actions_s3_key
    region  = var.region
    profile = var.profile
  }
}
