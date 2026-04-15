#!/bin/bash

set -e  # Exit on any error

echo "🚀 Databricks Deploy Script"

# ── Inputs ───────────────────────────────────────────────
read -p "Enter target (dev/qa/prod): " TARGET
read -p "Enter Databricks profile (e.g. dev_w): " PROFILE

echo "👉 Target: $TARGET"
echo "👉 Profile: $PROFILE"

# ── Step 0: Validate bundle (fail fast) ─────────────────
echo "🔍 Validating bundle..."
databricks bundle validate --target "$TARGET" --profile "$PROFILE" > /dev/null
echo "✅ Bundle valid"

# ── Step 1: Deploy bundle ───────────────────────────────
echo "📦 Deploying bundle..."
databricks bundle deploy --target "$TARGET" --profile "$PROFILE"
echo "✅ Bundle deployed"

# ── Step 2: Resolve schema from bundle (IMPORTANT) ──────
echo "🔍 Resolving schema from bundle..."

SCHEMA=$(databricks bundle validate \
  --target "$TARGET" \
  --profile "$PROFILE" \
  --output json \
  | jq -r '.resources.schemas.bronze_schema.name')

if [ -z "$SCHEMA" ] || [ "$SCHEMA" == "null" ]; then
  echo "❌ Failed to resolve schema name"
  exit 1
fi

echo "👉 Schema: $SCHEMA"

# ── Step 3: Resolve catalog (env-based) ─────────────────
if [ "$TARGET" == "dev" ]; then
  CATALOG="dev_db"
elif [ "$TARGET" == "qa" ]; then
  CATALOG="qa_db"
elif [ "$TARGET" == "prod" ]; then
  CATALOG="db"
else
  CATALOG="ai_db"
fi

echo "👉 Catalog: $CATALOG"

# ── Step 4: Find latest wheel ───────────────────────────
echo "🔍 Finding latest wheel..."

WHEEL_PATH=$(ls -t ../common_utils/dist/common_utils-*.whl 2>/dev/null | head -n 1)

if [ -z "$WHEEL_PATH" ]; then
  echo "❌ No wheel found in ../common_utils/dist"
  exit 1
fi

echo "👉 Wheel: $WHEEL_PATH"

# ── Step 5: Build volume path ───────────────────────────


# ── Step 5.5: Ensure libs directory exists ──────────────
# ── Paths ───────────────────────────────────────────────
DIR_PATH="dbfs:/Volumes/$CATALOG/$SCHEMA/libs/platform_libs"
WHEEL_PATH_DBFS="$DIR_PATH/common_utils-latest.whl"
WHEEL_VERSION_PATH_DBFS="$DIR_PATH/common_utils-0.1.0-py3-none-any.whl"

echo "📁 Ensuring directory exists: $DIR_PATH"

# Create full directory path (safe + idempotent)
databricks fs mkdirs "$DIR_PATH" --profile "$PROFILE"

echo "📤 Uploading to: $WHEEL_PATH_DBFS"

# ── Upload wheel as latest ───────────────────────────────────────
databricks fs cp "$WHEEL_PATH" "$WHEEL_PATH_DBFS" \
  --overwrite \
  --profile "$PROFILE"

  # ── Upload wheel as versioned  ───────────────────────────────────────
databricks fs cp "$WHEEL_PATH" "$WHEEL_VERSION_PATH_DBFS" \
  --overwrite \
  --profile "$PROFILE"

echo "✅ Upload complete"
echo " Deployement succeded"