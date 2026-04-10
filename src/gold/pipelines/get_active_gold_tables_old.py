# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ### Task 1 · Fetch active gold tables → pass to ForEach

# COMMAND ----------
import sys
import json
import os

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if IS_DATABRICKS:
    dbutils.widgets.text("catalog",      "")
    dbutils.widgets.text("gold_schema",  "")

    catalog     = dbutils.widgets.get("catalog")
    gold_schema = dbutils.widgets.get("gold_schema")
else:
    catalog="ai_dev_db"
    gold_schema="dev_msingh_gold"
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("local-run")
        .master("local[*]")
        .getOrCreate()
    )


# COMMAND ----------
from common_utils.gold.transform.data_registry import get_active_gold_tables_json,get_active_gold_tables

records = get_active_gold_tables(spark, catalog, gold_schema)
print(f"Active gold tables: {len(records)}")
for r in records:
    print(f"  → {catalog}.{r['schema']}.{r['table_nm']}")

# COMMAND ----------
# Serialize to JSON string — ForEach requires a JSON array
gold_tables_json = get_active_gold_tables_json(spark, catalog, gold_schema)
print(f"Task value JSON preview: {gold_tables_json[:200]}")

# COMMAND ----------
dbutils.jobs.taskValues.set(
    key   = "gold_tables",
    value = gold_tables_json        # ← must be JSON string, not list
)
print("✅ Task value set successfully")
