# Databricks notebook source

# COMMAND ----------
import json
from databricks import sql
from common_utils.logging.pg_settings import get_pg_settings
from common_utils.logging.job_logger  import JobLogger

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


#COMMAND ------------
# ── Execute with logging ──────────────────────────────────────────────
# ── Get PG settings ───────────────────────────────────────────────────
pg_settings = get_pg_settings(dbutils=dbutils)

from datetime import datetime


try:
    job_id = context.jobId().get()
    run_id = str(context.currentRunId().get())
except Exception:
    job_id = None
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")

print(f"Job ID : {job_id}")
print(f"Run ID : {run_id}")

# COMMAND ----------
# ── Build job params for logging ─────────────────────────────────────
job_params = {
    "table_nm":    table_nm,
    "schema":      schema,
    "resolved_sql": resolved_sql[:200],
}
with JobLogger(
    pg_settings = pg_settings,
    job_name    = "gold_mv_orchestrator",
    task_key    = "execute_gold_mv",
    run_id      = run_id,
    job_params  = job_params,
) as logger:

    # ── These log() calls get written to log_msg column (400 chars) ───
    logger.log(f"Starting refresh: {schema}.{table_nm}")
    logger.log(f"SQL preview: {resolved_sql[:120]}")

catalog="db"
# ── Connect to SQL Warehouse and execute ─────────────────────────────
def mv_exists(cur, schema, table_nm):
    cur.execute(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table_nm}'")
    return cur.fetchone() is not None

# ── Connect to SQL Warehouse and execute ─────────────────────────────
with sql.connect(
    server_hostname = workspace_url,
    http_path       = http_path,
    access_token    = token
) as conn:
    with conn.cursor() as cur:

        if mv_exists(cur, schema, table_nm):
            exec_sql = f"REFRESH MATERIALIZED VIEW {catalog}.{schema}.{table_nm}"
            action   = "REFRESH"
        else:
            exec_sql = resolved_sql          # full CREATE OR REPLACE DDL
            action   = "CREATE"

        print(f"Action   : {action}")
        print(f"Executing: {exec_sql[:120]}")
        cur.execute(exec_sql)
        print(f"✅ Done [{action}]: {schema}.{table_nm}")

logger.log(f"Completed: {schema}.{table_nm}")