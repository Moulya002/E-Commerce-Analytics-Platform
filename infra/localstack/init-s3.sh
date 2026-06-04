#!/bin/bash
# Initialize S3 data lake bucket structure in LocalStack

awslocal s3 mb s3://ecommerce-data-lake 2>/dev/null || true

for layer in bronze silver gold; do
  awslocal s3api put-object \
    --bucket ecommerce-data-lake \
    --key "${layer}/.keep" \
    --body /dev/null 2>/dev/null || true
done

echo "S3 data lake initialized: s3://ecommerce-data-lake/{bronze,silver,gold}/"
