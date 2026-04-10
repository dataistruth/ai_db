# Databricks notebook source

# COMMAND ----------
import json
from databricks import sql

# ── Widgets ──────────────────────────────────────────────────────────
dbutils.widgets.text("gold_table", "")   # JSON object from ForEach {{input}}
dbutils.widgets.text("http_path",  "")

raw       = dbutils.widgets.get("gold_table")
http_path = dbutils.widgets.get("http_path")

# ── Parse JSON object ─────────────────────────────────────────────────
item         = json.loads(raw)
table_nm     = item["table_nm"]
schema       = item["schema"]
resolved_sql = item["resolved_sql"]

print(f"table    : {table_nm}")
print(f"schema   : {schema}")
print(f"sql      : {resolved_sql}")

# ── Get workspace context ─────────────────────────────────────────────
context       = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token         = context.apiToken().get()
workspace_url = context.apiUrl().get().replace("https://", "")

# ── Connect to SQL Warehouse and execute ─────────────────────────────
with sql.connect(
    server_hostname = workspace_url,
    http_path       = http_path,
    access_token    = token
) as conn:
    with conn.cursor() as cur:
        print(f"Executing: {resolved_sql}")
        cur.execute(resolved_sql)
        print(f"✅ Done: {schema}.{table_nm}")