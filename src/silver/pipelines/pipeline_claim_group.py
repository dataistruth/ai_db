# Databricks notebook source
import dlt
import sys
from pyspark.sql.functions import col
sys.path.insert(0, spark.conf.get("bundle.sourcePath") + "/src")
from silver.transform.transform_dim import ( transform_dim_drug, transform_dim_member,
transform_dim_plan, transform_dim_prescriber )
from common_utils.constants import ALL_AUDIT_COLS
from pyspark.sql.functions import col, expr


# ── Read bundle-injected config ──────────────────────────────────────
catalog       = spark.conf.get("bundle.catalog")
env           = spark.conf.get("bundle.env")
developer     = spark.conf.get("bundle.developer")
schema_bronze = spark.conf.get("bundle.schema.bronze")
schema_silver = spark.conf.get("bundle.schema.silver")
schema_gold   = spark.conf.get("bundle.schema.gold")
sourcePath    = spark.conf.get("bundle.sourcePath")




# ── Step 1: Declare target table explicitly ────────────────────────────
dlt.create_streaming_table(
    name    = "d_drug",
    comment = "SCD Type 2 drug dimension",
    expect_all = {
        "drug_code_not_null": "drug_code IS NOT NULL"
        }
)

# ── Step 2: Streaming view as source ──────────────────────────────────
@dlt.view(name="d_drug_staged")
def d_drug_staged():
    bronze_table = f"{catalog}.{schema_bronze}.drug_raw"
    return (
        dlt.read_stream(bronze_table)
        .transform(transform_dim_drug)
    )

# ── Step 3: SCD Type 2 ────────────────────────────────────────────────
dlt.apply_changes(
    target             = "d_drug",
    source             = "d_drug_staged",
    keys               = ["drug_code", "drug_name"],
    sequence_by        = col("insert_ts"),
    stored_as_scd_type = 2,
    except_column_list = []
)

# ── Step 1: Declare target table explicitly ────────────────────────────
dlt.create_streaming_table(
    name    = "d_member",
    comment = "SCD Type 2 member dimension",
    expect_all = {
        "member_id_not_null": "member_id IS NOT NULL",
        "valid_phone" : "phone RLIKE '^[+]?[0-9][0-9-]{0,11}$'"
        }
)
@dlt.view(name="d_member_staged")
def d_member_staged():
    bronze_table = f"{catalog}.{schema_bronze}.member_raw"
    return (
        dlt.read_stream(bronze_table)
        .transform(transform_dim_member)
    )

dlt.apply_changes(
    target= "d_member",
    source= "d_member_staged",
    keys=["member_id","dob","full_name"],
    sequence_by = col("insert_ts"),
    stored_as_scd_type = 2,
    except_column_list = []
)

dlt.create_streaming_table(
    name="d_plan",
    comment ="SCD Type 2 plan ",
    expect_all={
        "plan_id_not_null": "plan_id IS NOT NULL" }
)
@dlt.view(name="d_plan_staged")
def d_plan_staged():
    bronze_table = f"{catalog}.{schema_bronze}.plan_raw"
    return(dlt.read_stream(bronze_table)
           .transform(transform_dim_plan)
           )

dlt.apply_changes(
    target="d_plan",
    source="d_plan_staged",
    keys=["plan_id","plan_type"],
    sequence_by =col("insert_ts"),
    stored_as_scd_type=2,
    except_column_list = []
)

dlt.create_streaming_table(
    name="d_prescriber",
    comment = "SCD type 2 prescriber ",
    expect_all={
        "prescriber_id_not_null": "prescriber_id IS NOT NULL",
        "valid_npi"              : "npi RLIKE '^[0-9]{10}$'",        # exactly 10 digits
        "valid_phone"            : "phone RLIKE '^[+]?[0-9][0-9-]{0,11}$'"
    }
)

@dlt.view(name="d_prescriber_staged")
def d_prescriber_staged():
    bronze_table = f"{catalog}.{schema_bronze}.prescriber_raw"
    return(
        dlt.read_stream(bronze_table)
        .transform(transform_dim_prescriber)
    )
dlt.apply_changes(
    target="d_prescriber",
    source="d_prescriber_staged",
    keys=["prescriber_id","npi"],
    sequence_by =col("insert_ts"),
    stored_as_scd_type=2,
    except_column_list = ["insert_ts"]
)