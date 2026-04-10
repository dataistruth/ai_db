# Databricks notebook source
# COMMAND ----------
from common_utils.gold.transform.data_registry import get_active_gold_tables
import json

# ── Widgets ──────────────────────────────────────────────────────────
dbutils.widgets.text("catalog",     "")
dbutils.widgets.text("gold_schema", "")

catalog     = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")

print(f"Catalog is : {catalog}")
print(f"Gold_schema is : {gold_schema}")

records = get_active_gold_tables(spark, catalog, gold_schema)

# COMMAND ----------

# ── Pass all three fields as JSON array of objects ────────────────────
payload = [
    {
        "table_nm":    r["table_nm"],
        "schema":      r["schema"],
        "resolved_sql": r["resolved_sql"]
    }
    for r in records
]

print(f"Found {len(payload)} active gold tables")
for i, item in enumerate(payload):
    print(f"\n[{i+1}] table : {item['table_nm']}")
    print(f"      schema: {item['schema']}")
    print(f"      sql   : {item['resolved_sql'][:80]}...")

dbutils.jobs.taskValues.set(
    key   = "gold_tables",
    value = json.dumps(payload)
)

print(f"✅ Task value set with {len(payload)} gold tables")