#!/usr/bin/env bash
# Runs automatically when LocalStack is ready (mounted into ready.d).
set -euo pipefail

echo "==> creating S3 bucket"
awslocal s3 mb s3://lazarus-local

echo "==> creating DynamoDB watermarks table"
awslocal dynamodb create-table \
  --table-name lazarus-watermarks \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

echo "==> creating Glue database"
awslocal glue create-database \
  --database-input '{"Name":"lazarus_local"}'

echo "==> localstack bootstrap complete"
