#!/usr/bin/env sh
# Integrate Amazon S3 table buckets with AWS Glue data catalog and AWS lake formation
# Creates an IAM role for Lake Formation, registers S3 Tables as a data location (with federation)
# Creates the federated Glue data catalog: s3tablescatalog

set -eu
# Try to enable pipefail if the shell supports it (bash/zsh/ksh)
if (set -o pipefail) 2>/dev/null; then
  set -o pipefail
fi

usage() {
  cat <<'EOF'
        Usage:

        Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html

        integrate_s3tables_lake_formation.sh \
            --aws_account_id <aws_account_id> \
            --aws_cli_profile_name <aws_cli_profile_name> \
            --s3_table_bucket_name <s3_table_bucket_name> \
            [--aws_region <aws_region>]

        Required:

        --aws_account_id         AWS Account ID (e.g., 111122223333)
        --aws_cli_profile_name   AWS CLI profile to use (for --profile)
        --s3_table_bucket_name   Name of the already-created S3 table bucket (used for validation/logs)

        Optional:

        --aws_region             AWS Region (default: us-east-1)

        This script is idempotent where possible:

        - Creates IAM role 'S3TablesRoleForLakeFormation' if missing; just attach the 'AmazonS3TablesLakeFormationServiceRole' managed policy on every run.
        - Registers S3 Tables location with Lake Formation if not already registered.
        - Creates Glue data catalog 's3tablescatalog' if missing; otherwise leaves it as-is.

        Prereqs:
        - AWS CLI v2 updated; see https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
        - Caller has permissions for iam:create-role/attach-role-policy, lakeformation:RegisterResource, glue:CreateCatalog, glue:GetCatalog, sts:GetCallerIdentity, etc.

EOF
}

AWS_ACCOUNT_ID=""
AWS_PROFILE=""
TABLE_BUCKET_NAME=""
REGION="us-east-1"

while [ $# -gt 0 ]; do
  case "$1" in
    --aws_account_id)
      [ $# -ge 2 ] || { echo "Option --aws_account_id requires an argument" >&2; usage; exit 2; }
      AWS_ACCOUNT_ID="$2"; shift 2 ;;
    --aws_cli_profile_name)
      [ $# -ge 2 ] || { echo "Option --aws_cli_profile_name requires an argument" >&2; usage; exit 2; }
      AWS_PROFILE="$2"; shift 2 ;;
    --s3_table_bucket_name)
      [ $# -ge 2 ] || { echo "Option --s3_table_bucket_name requires an argument" >&2; usage; exit 2; }
      TABLE_BUCKET_NAME="$2"; shift 2 ;;
    --aws_region)
      [ $# -ge 2 ] || { echo "Option --aws_region requires an argument" >&2; usage; exit 2; }
      REGION="$2"; shift 2 ;;
    --help|-h)
      usage; exit 0 ;;
    --)
      shift; break ;;
    -*)
      echo "Invalid option: $1" >&2; usage; exit 2 ;;
    *)
      echo "Unexpected argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [ -z "${AWS_ACCOUNT_ID}" ] || [ -z "${AWS_PROFILE}" ] || [ -z "${TABLE_BUCKET_NAME}" ]; then
  echo "Missing required arguments" >&2
  usage
  exit 2
fi

# Disable AWS CLI pager to avoid any interactive paging in non-TTY contexts
# See: https://stackoverflow.com/questions/60122188/how-to-turn-off-the-pager-for-aws-cli-return-value
export AWS_PAGER=""

# Variables to simplify aws cli commands and reduce repetition
AWS="aws --profile ${AWS_PROFILE} --region ${REGION}"
ROLE_NAME="S3TablesRoleForLakeFormation"
ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}"
S3TABLES_RESOURCE_ARN="arn:aws:s3tables:${REGION}:${AWS_ACCOUNT_ID}:bucket/*"
CATALOG_NAME="s3tablescatalog"

echo "Validating AWS identity and region..."
${AWS} sts get-caller-identity >/dev/null

# ------------------------- Temporary files & cleanup ------------------------ #

TMPDIR="$(mktemp -d 2>/dev/null || mktemp -d -t s3tables)"
trap 'rm -rf "${TMPDIR}"' EXIT

# This trust policy allows Lake Formation to assume the role
TRUST_JSON="${TMPDIR}/role_trust_policy.json"
# Inputs for registering table buckets with Lake Formation
REGISTER_JSON="${TMPDIR}/register_input.json"
# Inputs for creating the data catalog
CATALOG_JSON="${TMPDIR}/catalog.json"

# Trust policy structure mirrors AWS docs example for this integration
cat > "${TRUST_JSON}" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LakeFormationDataAccessPolicy",
      "Effect": "Allow",
      "Principal": {
        "Service": "lakeformation.amazonaws.com"
      },
      "Action": [
        "sts:AssumeRole",
        "sts:SetContext",
        "sts:SetSourceIdentity"
      ],
      "Resource": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/lakeformation-service-role",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "${AWS_ACCOUNT_ID}"
        }
      }
    }
  ]
}
EOF

cat > "${REGISTER_JSON}" <<EOF
{
  "ResourceArn": "${S3TABLES_RESOURCE_ARN}",
  "WithFederation": true,
  "RoleArn": "${ROLE_ARN}"
}
EOF

cat > "${CATALOG_JSON}" <<EOF
{
  "Name": "${CATALOG_NAME}",
  "CatalogInput": {
    "FederatedCatalog": {
      "Identifier": "${S3TABLES_RESOURCE_ARN}",
      "ConnectionName": "aws:s3tables"
    },
    "CreateDatabaseDefaultPermissions": [],
    "CreateTableDefaultPermissions": [],
    "AllowFullTableExternalDataAccess": "True"
  }
}
EOF

# ---------------------- Service role for Lake Formation --------------------- #

echo "Ensuring IAM role '${ROLE_NAME}' exists..."
if ${AWS} iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "    ${ROLE_NAME} already exists"
else
  ${AWS} iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "file://${TRUST_JSON}" >/dev/null
  echo "    Role ${ROLE_NAME} created"
fi

echo "Attaching managed policy to role ${ROLE_NAME}..."
${AWS} iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonS3TablesLakeFormationServiceRole"
echo "    Managed policy attached"

# -------------- Register S3 Tables resource with Lake Formation ------------- #

echo "Registering S3 Tables resource with Lake Formation (privileged, federated)..."
REGISTERED="$(${AWS} lakeformation list-resources \
  --query "ResourceInfoList[?ResourceArn=='${S3TABLES_RESOURCE_ARN}'] | length(@)" \
  --output text || echo 0)"

if [ "${REGISTERED}" = "0" ]; then
  ${AWS} lakeformation register-resource \
    --with-privileged-access \
    --cli-input-json "file://${REGISTER_JSON}" >/dev/null
  echo "    Resource registered"
else
  echo "    Resource already registered"
fi

# ------------------------- Create glue data catalog ------------------------- #

echo "Creating Glue federated catalog '${CATALOG_NAME}' if missing..."
if ${AWS} glue get-catalog --catalog-id "${CATALOG_NAME}" >/dev/null 2>&1; then
  echo "    Catalog exists"
else
  ${AWS} glue create-catalog --cli-input-json "file://${CATALOG_JSON}" >/dev/null
  echo "    Catalog created"
fi

# ------------------------------- Verifications ------------------------------ #

echo "Verifying integration..."
${AWS} glue get-catalog --catalog-id "${CATALOG_NAME}" >/dev/null
${AWS} lakeformation list-resources \
  --query "ResourceInfoList[?ResourceArn=='${S3TABLES_RESOURCE_ARN}'].ResourceArn" \
  --output text >/dev/null

echo "Done:"
echo "    S3 Tables bucket '${TABLE_BUCKET_NAME}' (and all table buckets in ${REGION}) are now integrated via:"
echo "      - IAM role: ${ROLE_ARN}"
echo "      - Lake Formation registered location: ${S3TABLES_RESOURCE_ARN}"
echo "      - Glue (account-level) data catalog: ${CATALOG_NAME}"
