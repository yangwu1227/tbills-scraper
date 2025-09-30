module "common" {
  source = "../modules/common"
}


locals {
  project_prefix                = module.common.project_prefix
  terraform_remote_state_bucket = module.common.terraform_remote_state_bucket
  profile                       = module.common.profile
  aws_region                    = module.common.aws_region
  aws_account_id                = module.common.aws_account_id
}
