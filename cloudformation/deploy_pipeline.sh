#!/bin/bash

set -e
set -o pipefail

ENV=dev
PREFIX=gp
REGION=us-east-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Resource references
S3_BUCKET="${PREFIX}-elt-${ACCOUNT_ID}-${ENV}"
GLUE_JOB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PREFIX}-glue-job-role-${ENV}"
REDSHIFT_SECRET_ARN="arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:connection_parameters_redshift-${ENV}"
SQLSERVER_SECRET_ARN="arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:connection_parameters_sqlserver-${ENV}"

CLOUDFORMATION_DIR="cloudformation"
SCRIPT_DIR="scripts"
DRIVER_DIR="drivers"

# Get VPC ID for endpoint stack
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=network-stack-vpc" --query "Vpcs[0].VpcId" --output text)

declare -a STACKS=(
  "01_network.yml network-stack"
  "02_iam.yml iam-stack"
  "03_secrets.yml secrets-stack"
)

echo "Deploying foundational stacks..."

for entry in "${STACKS[@]}"; do
  FILE=$(echo $entry | awk '{print $1}')
  NAME=$(echo $entry | awk '{print $2}')
  echo "🔧 Deploying $NAME from $FILE"
  aws cloudformation deploy \
    --stack-name "$NAME" \
    --template-file "$CLOUDFORMATION_DIR/$FILE" \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides Environment=$ENV Prefix=$PREFIX \
    --region $REGION
done

echo " Foundational stacks deployed."

echo "Deploying RDS SQL Server stack..."
aws cloudformation deploy \
  --template-file "$CLOUDFORMATION_DIR/04_rds_sqlserver.yml" \
  --stack-name rds-sqlserver-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides DBSecretArn=$SQLSERVER_SECRET_ARN Environment=$ENV Prefix=$PREFIX \
  --region $REGION

echo "Uploading Glue scripts and JDBC driver to S3..."
aws s3 cp "$SCRIPT_DIR/" "s3://$S3_BUCKET/scripts/" --recursive
aws s3 cp "$DRIVER_DIR/mssql-jdbc-12.10.0.jre8.jar" "s3://$S3_BUCKET/drivers/sqljdbc42.jar"

echo "Deploying Glue Jobs stack..."
aws cloudformation deploy \
  --template-file "$CLOUDFORMATION_DIR/05_gluejobs.yml" \
  --stack-name gluejobs-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides GlueJobRoleArn=$GLUE_JOB_ROLE_ARN S3Bucket=$S3_BUCKET Environment=$ENV Prefix=$PREFIX \
  --region $REGION

echo " All stacks deployed successfully!"
