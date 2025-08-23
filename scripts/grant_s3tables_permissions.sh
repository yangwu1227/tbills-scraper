#!/usr/bin/env sh
# Grant access to a table within an S3 table bucket to a specified IAM principal (user or role)
# Use the federated Glue data catalog (e.g., s3tablescatalog)

set -eu
# Try to enable pipefail if supported (bash/zsh/ksh); harmless no-op in plain sh
if (set -o pipefail) 2>/dev/null; then
  set -o pipefail
fi

usage() {
  cat <<'EOF'
        Usage:

        Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/grant-permissions-tables.html

        grant_s3tables_permissions.sh \
            --aws_account_id <aws_account_id> \
            --aws_cli_profile_name <aws_cli_profile_name> \
            --s3_table_bucket_name <s3_table_bucket_name> \
            --namespace_name <namespace_name> \
            --table_name <table_name> \
            --principal_arn <principal_arn> \
            [--catalog_name <catalog_name>] \
            [--aws_region <aws_region>] \
            [--with-grant]

        Required:

        --aws_account_id         AWS Account ID (e.g., 111122223333)
        --aws_cli_profile_name   AWS CLI profile to use (for --profile)
        --s3_table_bucket_name   S3 table bucket name (e.g., amzn-s3-demo-bucket)
        --namespace_name         S3 Tables namespace (Glue database name)
        --table_name             S3 Tables table name
        --principal_arn          Principal ARN to grant to (user or role), e.g. arn:aws:iam::111122223333:role/example-role

        Optional:

        --catalog_name           Glue catalog name for S3 Tables federation (default: s3tablescatalog)
        --aws_region             AWS Region (default: us-east-1)
        --with-grant             Also grant the Grant option for the same permissions

        Behavior:

        - Grants Lake Formation permissions on a single table.
        - Default permission is ALL, consistent with the referenced example in the documentation.
        - If --with-grant is specified, also grants the Grant option, which allows the target
          principal to grant permissions to other principals.

        Prereqs:
        - AWS CLI v2; caller has lakeformation:ListPermissions and lakeformation:GrantPermissions
EOF
}

AWS_ACCOUNT_ID=""
AWS_PROFILE=""
TABLE_BUCKET_NAME=""
NAMESPACE_NAME=""
TABLE_NAME=""
PRINCIPAL_ARN=""
CATALOG_NAME="s3tablescatalog"
REGION="us-east-1"
WITH_GRANT="no"

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
    --namespace_name)
      [ $# -ge 2 ] || { echo "Option --namespace_name requires an argument" >&2; usage; exit 2; }
      NAMESPACE_NAME="$2"; shift 2 ;;
    --table_name)
      [ $# -ge 2 ] || { echo "Option --table_name requires an argument" >&2; usage; exit 2; }
      TABLE_NAME="$2"; shift 2 ;;
    --principal_arn)
      [ $# -ge 2 ] || { echo "Option --principal_arn requires an argument" >&2; usage; exit 2; }
      PRINCIPAL_ARN="$2"; shift 2 ;;
    --catalog_name)
      [ $# -ge 2 ] || { echo "Option --catalog_name requires an argument" >&2; usage; exit 2; }
      CATALOG_NAME="$2"; shift 2 ;;
    --aws_region)
      [ $# -ge 2 ] || { echo "Option --aws_region requires an argument" >&2; usage; exit 2; }
      REGION="$2"; shift 2 ;;
    --with-grant)
      WITH_GRANT="yes"; shift 1 ;;
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

if [ -z "${AWS_ACCOUNT_ID}" ] || [ -z "${AWS_PROFILE}" ] || [ -z "${TABLE_BUCKET_NAME}" ] || \
   [ -z "${NAMESPACE_NAME}" ] || [ -z "${TABLE_NAME}" ] || [ -z "${PRINCIPAL_ARN}" ]; then
  echo "Missing required arguments" >&2
  usage
  exit 2
fi

# Disable AWS CLI pager to avoid any interactive paging in non-TTY contexts
# See: https://stackoverflow.com/questions/60122188/how-to-turn-off-the-pager-for-aws-cli-return-value
export AWS_PAGER=""

AWS="aws --profile ${AWS_PROFILE} --region ${REGION}"
# Federated S3 Tables data catalog uses a composite catalog id form
CATALOG_COMPOSITE_ID="${AWS_ACCOUNT_ID}:${CATALOG_NAME}/${TABLE_BUCKET_NAME}"

echo "Validating AWS identity and region..."
${AWS} sts get-caller-identity >/dev/null

# ------------------------- Temporary files & cleanup ------------------------ #

TMPDIR="$(mktemp -d 2>/dev/null || mktemp -d -t s3tables-grant)"
trap 'rm -rf "${TMPDIR}"' EXIT

GRANT_JSON="${TMPDIR}/grant_permissions.json"

# Build grant payload (default Permissions = ALL); optionally include grant option, also defaults to ALL
# See: https://docs.aws.amazon.com/lake-formation/latest/APIReference/API_GrantPermissions.html
if [ "${WITH_GRANT}" = "yes" ]; then
  cat > "${GRANT_JSON}" <<EOF
{
  "Principal": { "DataLakePrincipalIdentifier": "${PRINCIPAL_ARN}" },
  "Resource": {
    "Table": {
      "CatalogId": "${CATALOG_COMPOSITE_ID}",
      "DatabaseName": "${NAMESPACE_NAME}",
      "Name": "${TABLE_NAME}"
    }
  },
  "Permissions": [ "ALL" ],
  "PermissionsWithGrantOption": [ "ALL" ]
}
EOF
else
  cat > "${GRANT_JSON}" <<EOF
{
  "Principal": { "DataLakePrincipalIdentifier": "${PRINCIPAL_ARN}" },
  "Resource": {
    "Table": {
      "CatalogId": "${CATALOG_COMPOSITE_ID}",
      "DatabaseName": "${NAMESPACE_NAME}",
      "Name": "${TABLE_NAME}"
    }
  },
  "Permissions": [ "ALL" ]
}
EOF
fi

# ----------------------------- Grant permissions ---------------------------- #

echo "Granting Lake Formation permissions (ALL) on table..."
${AWS} lakeformation grant-permissions \
  --cli-input-json "file://${GRANT_JSON}" >/dev/null
echo "    Grant applied"

echo "Done:"
echo "    Granted: ALL"
[ "${WITH_GRANT}" = "yes" ] && echo "    With grant option: YES" || echo "    With grant option: NO"
echo "    Principal: ${PRINCIPAL_ARN}"
echo "    Table: ${CATALOG_COMPOSITE_ID}:${NAMESPACE_NAME}.${TABLE_NAME}"
echo "    Region: ${REGION}"
