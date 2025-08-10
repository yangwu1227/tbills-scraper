# OIDC provider
resource "aws_iam_openid_connect_provider" "github_oidc_provider" {
  count = var.create_github_oidc_provider == true ? 1 : 0
  url   = "https://token.actions.githubusercontent.com"
  client_id_list = [
    "sts.amazonaws.com"
  ]
  # https://github.blog/changelog/2023-06-27-github-actions-update-on-oidc-integration-with-aws/
  thumbprint_list = [
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
    "6938fd4d98bab03faadb97b34396831e3780aea1"
  ]
  tags = {
    Name = "github_oidc_provider"
  }
}

locals {
  github_oidc_provider_arn = var.create_github_oidc_provider == true ? aws_iam_openid_connect_provider.github_oidc_provider[0].arn : var.existing_oidc_provider_arn
}

# IAM role for workflow
resource "aws_iam_role" "github_actions_role" {
  name = "${var.project_prefix}_github_actions_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Federated = local.github_oidc_provider_arn
        },
        Action = "sts:AssumeRoleWithWebIdentity",
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          },
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_username}/${var.github_repo_name}:*"
          }
        }
      }
    ]
  })
  tags = {
    Name = "${var.project_prefix}_iam_github_actions_role"
  }
  depends_on = [aws_iam_openid_connect_provider.github_oidc_provider]
}

resource "aws_iam_policy" "github_actions_policy" {
  name = "${var.project_prefix}_github_actions_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        # See required permissions: https://developer.hashicorp.com/terraform/language/backend/s3
        Action = [
          # For terraform remote state management
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
        ],
        Resource = [
          "arn:aws:s3:::${var.terraform_remote_state_bucket}",
          "arn:aws:s3:::${var.terraform_remote_state_bucket}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          # For global state locking
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:DeleteItem",
          "dynamodb:DescribeTable"
        ],
        Resource = "arn:aws:dynamodb:${var.region}:${var.account_id}:table/${var.terraform_state_dynamodb_table}"
      },
    ]
  })
  tags = {
    Name = "${var.project_prefix}_iam_github_actions_policy"
  }
}

resource "aws_iam_policy_attachment" "github_actions_policy_attachment" {
  name       = "${var.project_prefix}_github_actions_policy_attachment"
  roles      = [aws_iam_role.github_actions_role.name]
  policy_arn = aws_iam_policy.github_actions_policy.arn
}

resource "aws_iam_policy_attachment" "github_actions_managed_policy_attachment" {
  name  = "${var.project_prefix}_github_actions_managed_policy_attachment"
  roles = [aws_iam_role.github_actions_role.name]
  # We set table bucket policy allowed actions to limit this, but set this on the github actions side for simplicity
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3TablesFullAccess"
}
