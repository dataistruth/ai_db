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

# ── Step 2: Resolve schema from bundle ──────────────────

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

# ── Step 3: Resolve catalog ─────────────────────────────

if [ "$TARGET" == "dev" ]; then
CATALOG="ai_dev_db"
elif [ "$TARGET" == "qa" ]; then
CATALOG="ai_qa_db"
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

# Extract filename (e.g. common_utils-0.1.0-py3-none-any.whl)

WHEEL_FILE=$(basename "$WHEEL_PATH")

# ── Step 5: Prepare DBFS paths ──────────────────────────

DIR_PATH="dbfs:/Volumes/$CATALOG/$SCHEMA/libs/platform_libs"

LATEST_WHEEL_DBFS="$DIR_PATH/common_utils-latest.whl"
VERSIONED_WHEEL_DBFS="$DIR_PATH/$WHEEL_FILE"

echo "📁 Ensuring directory exists: $DIR_PATH"
databricks fs mkdirs "$DIR_PATH" --profile "$PROFILE"

# ── Step 6: Upload versioned wheel ──────────────────────

echo "📤 Uploading versioned wheel: $VERSIONED_WHEEL_DBFS"

databricks fs cp "$WHEEL_PATH" "$VERSIONED_WHEEL_DBFS" --overwrite --profile "$PROFILE" 

# ── Step 7: Upload latest alias ─────────────────────────

echo "📤 Uploading latest wheel: $LATEST_WHEEL_DBFS"

databricks fs cp "$WHEEL_PATH" "$LATEST_WHEEL_DBFS" --overwrite --profile "$PROFILE"

echo "✅ Upload complete"
echo "🚀 Deployment succeeded"

