# Databricks notebook source
import os
import sys
from pathlib import Path

import os

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if IS_DATABRICKS:
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("gold_schema", "")
    dbutils.widgets.text("silver_schema", "")
    dbutils.widgets.text("bronze_schema", "")
    dbutils.widgets.text("env", "")
    dbutils.widgets.text("sql_dir", "")

    catalog = dbutils.widgets.get("catalog")
    gold_schema = dbutils.widgets.get("gold_schema")
    silver_schema = dbutils.widgets.get("silver_schema")
    bronze_schema = dbutils.widgets.get("bronze_schema")
    env = dbutils.widgets.get("env")  # ← was "bronze_schema"
    sql_dir = dbutils.widgets.get("sql_dir")

    gold_fqn = f"{catalog}.{gold_schema}"
    silver_fqn = f"{catalog}.{silver_schema}"
    bronze_fqn = f"{catalog}.{bronze_schema}"


    print(f"Gold:   {gold_fqn}")
    print(f"Silver: {silver_fqn}")
    print(f"Bronze: {bronze_fqn}")
else:
    catalog = "ai_dev_db"
    env = "dev"
    developer = "msingh"
    bronze_schema = "dev_msingh_bronze"
    silver_schema = "dev_msingh_silver"
    gold_schema = "dev_msingh_gold"
    gold_fqn = f"{catalog}.{gold_schema}"
    silver_fqn = f"{catalog}.{silver_schema}"
    bronze_fqn = f"{catalog}.{bronze_schema}"
    source_path = ""
    sql_dir = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "sql")
    )

# COMMAND ----------

S = f"{catalog}.{silver_schema}"
G = f"{catalog}.{gold_schema}"

print(f"IS_DATABRICKS : {IS_DATABRICKS}")
print(f"catalog       : {catalog}")
print(f"env           : {env}")
print(f"S             : {S}")
print(f"G             : {G}")
print(f"sql_dir       : {sql_dir}")
# ── List all .sql files in directory ──────────────────────────────────────
sql_files = sorted([
    f for f in os.listdir(sql_dir)
    if f.endswith(".sql")
])

print(f"All sql files in directory : {sql_files}")

# COMMAND ----------
from pathlib import Path

def resolve_sql(sql: str) -> str:
    return (
        sql
        .replace("{S}", silver_fqn)
        .replace("{G}", gold_fqn)
        .replace("{B}", bronze_fqn)
    )

# COMMAND ----------
sql_files = [f.name for f in Path(sql_dir).glob("*.sql")]
print(f"Found {len(sql_files)} SQL files: {sql_files}")

for s in sql_files:
    sql_path = Path(sql_dir) / s
    sql      = sql_path.read_text()
    resolved = resolve_sql(sql)
    print(f"\n--- {s} ---\n{resolved}")

# COMMAND ----------

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import Row
import pyspark.sql.functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ### Build registry rows from SQL files

# COMMAND ----------
rows = []

for sql_file in sorted(Path(sql_dir).glob("*.sql")):
    raw_sql     = sql_file.read_text()
    resolved    = resolve_sql(raw_sql)
    table_nm    = sql_file.stem                  # filename without .sql
    rows.append(
        Row(
            schema       = gold_schema,          # e.g. gold
            table_nm     = table_nm,             # e.g. claim_quality_summary
            is_active    = True,
            resolved_sql = resolved,
            create_ts    = datetime.utcnow()
        )
    )

df_new = spark.createDataFrame(rows)
df_new.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### Upsert into gold_dataset_properties (SCD1 / Delta MERGE)

# COMMAND ----------
target_fqn = f"{gold_fqn}.gold_dataset_properties"

# Create table if it doesn't exist yet
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_fqn} (
        schema        STRING        NOT NULL,
        table_nm      STRING        NOT NULL,
        is_active     BOOLEAN,
        resolved_sql  STRING,
        create_ts     TIMESTAMP
    )
    USING DELTA
    COMMENT 'Registry of gold layer table definitions and resolved SQL'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------
delta_target = DeltaTable.forName(spark, target_fqn)

(
    delta_target.alias("tgt")
    .merge(
        df_new.alias("src"),
        "tgt.schema = src.schema AND tgt.table_nm = src.table_nm"
    )
    .whenMatchedUpdate(set={
        "is_active"    : F.col("src.is_active"),
        "resolved_sql" : F.col("src.resolved_sql"),
        "create_ts"    : F.col("src.create_ts"),
    })
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✅ Upserted {len(rows)} rows into {target_fqn}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------
spark.sql(f"""
    SELECT schema, table_nm, is_active, create_ts,
           LEFT(resolved_sql, 80) AS sql_preview
    FROM {target_fqn}
    ORDER BY schema, table_nm
""").display()