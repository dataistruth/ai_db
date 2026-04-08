# Databricks notebook source
# ─────────────────────────────────────────────────────────────
# Bronze Ingestion Script (Auto Loader)
# ─────────────────────────────────────────────────────────────

import json
from pathlib import Path

from pyspark.sql.functions import col, current_timestamp, to_date
from common_utils.constants import BRONZE_AUDIT_COLS, SILVER_AUDIT_COLS


# ─────────────────────────────────────────────────────────────
# 🔹 Parameter Handling (Notebook + Job compatible)
# ─────────────────────────────────────────────────────────────

def get_param(name: str, default: str = None):
    try:
        return dbutils.widgets.get(name)
    except:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument(f"--{name}", default=default)
        args, _ = parser.parse_known_args()
        return getattr(args, name)


config_path = get_param("config")
catalog = get_param("catalog")
schema = get_param("schema")

print(f"📌 Config Path: {config_path}")
print(f"📌 Catalog: {catalog}")
print(f"📌 Schema: {schema}")

# ─────────────────────────────────────────────────────────────
# 🔹 Resolve Bundle Root Path
# ─────────────────────────────────────────────────────────────

def get_base_dir():
    try:
        # Notebook case — cwd is the notebook's directory,
        # go up 3 levels (pipelines → bronze → src → files)
        return Path.cwd().parents[2]
    except:
        # Python file case
        return Path(__file__).resolve().parents[3]


BASE_DIR = get_base_dir()
CONFIG_PATH = BASE_DIR / config_path

print(f"📂 Resolved Config Path: {CONFIG_PATH}")

# ─────────────────────────────────────────────────────────────
# 🔹 Load Config
# ─────────────────────────────────────────────────────────────

def load_config(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"❌ Config not found: {path}")
    with open(path, "r") as f:
        return json.load(f)


config = load_config(CONFIG_PATH)

# ─────────────────────────────────────────────────────────────
# 🔹 Auto Loader Function
# ─────────────────────────────────────────────────────────────

def run_autoloader(cfg: dict):
    table_name = cfg["table_nm"]
    source_path = f"/Volumes/{catalog}/{schema}/raw/{table_name}"

    print(f"🚀 Processing table: {table_name}")
    print(f"📥 Source: {source_path}")

    schema_location = f"/Volumes/{catalog}/{schema}/raw/schema/{table_name}"
    checkpoint_location = f"/Volumes/{catalog}/{schema}/raw/checkpoint/{table_name}"

    print(f"📁 Schema Location: {schema_location}")
    print(f"📁 Checkpoint Location: {checkpoint_location}")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # schema evolution
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(source_path)
    )

    # 🔥 Use _metadata instead of input_file_name()
    df = (
        df
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("file_name", col("_metadata.file_name"))
        .withColumn("file_size", col("_metadata.file_size"))
        .withColumn("file_modification_time", col("_metadata.file_modification_time"))
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("ingestion_date", to_date("ingestion_ts"))
    )
    (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.{table_name}")
    )

# ─────────────────────────────────────────────────────────────
# 🔹 Execute Pipeline
# ─────────────────────────────────────────────────────────────

active_tables = [t for t in config if t.get("is_active", False)]

print(f"✅ Active tables: {[t['table_nm'] for t in active_tables]}")

for table in active_tables:
    run_autoloader(table)

print("🎉 Bronze ingestion completed successfully!")