#!/bin/bash
set -euo pipefail

echo "[LocalStack init] Creating S3 buckets, SNS topics, and SQS queues..."

# Helper za LocalStack AWS CLI
AWS="awslocal"

# 1) S3 buckets za bronze/silver/gold zone
$AWS s3 mb s3://customer-360-bronze || true
$AWS s3 mb s3://customer-360-silver || true
$AWS s3 mb s3://customer-360-gold   || true

# 2) SNS topic za evente (aplikacijski eventovi, npr. login, order_created)
TOPIC_ARN=$($AWS sns create-topic --name app-events-topic --query 'TopicArn' --output text)
echo "[LocalStack init] Created SNS topic: $TOPIC_ARN"

# 3) SQS queue za evente
QUEUE_URL=$($AWS sqs create-queue --queue-name app-events-queue --query 'QueueUrl' --output text)
echo "[LocalStack init] Created SQS queue: $QUEUE_URL"

# 4) Subscribe SQS na SNS (da svi eventovi idu u queue)
QUEUE_ARN=$($AWS sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

$AWS sns subscribe \
  --topic-arn "$TOPIC_ARN" \
  --protocol sqs \
  --notification-endpoint "$QUEUE_ARN" >/dev/null

echo "[LocalStack init] Subscribed SQS queue to SNS topic."
echo "[LocalStack init] Done."

