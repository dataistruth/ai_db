#!/bin/bash
# scripts/bootstrap.sh
# Usage: ./scripts/bootstrap.sh dev
# Run once per environment — safe to run multiple times due to IF NOT EXISTS

ENV=${1:-dev}
PROFILE="${ENV}_w"

case $ENV in
  dev)     CATALOG="ai_dev_db" ;;
  qa)      CATALOG="ai_qa_db" ;;
  prod)    CATALOG="ai_db" ;;
  *)       echo "Unknown env: $ENV"; exit 1 ;;
esac

echo "Bootstrapping $ENV environment — catalog: $CATALOG"

databricks sql execute \
    --profile "$PROFILE" \
    --statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.bronze"

databricks sql execute \
    --profile "$PROFILE" \
    --statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.silver"

databricks sql execute \
    --profile "$PROFILE" \
    --statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.gold"

databricks sql execute \
    --profile "$PROFILE" \
    --statement "CREATE VOLUME IF NOT EXISTS ${CATALOG}.bronze.libs VOLUME_TYPE = MANAGED"

echo "Bootstrap complete for $ENV"