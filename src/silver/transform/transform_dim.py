from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from common_utils.common.scd_transform import scd2_merge_delta
from common_utils.common.utils import sk_hash_expr

def transform_dim_drug(df: DataFrame ) -> DataFrame:
    # ── column expression map ──────────────────────────────────────────────────────
    transform_dim_drug_expr: dict = {
        "drug_code": F.col("drug_code").cast("string"),
        "drug_name": F.col("drug_name"),
        "drug_class": F.col("drug_class"),
        "drug_sk": sk_hash_expr(["drug_code"]),
        "insert_ts": F.current_timestamp(),
    }

    # ── select derived from dict keys — single source of truth ────────────────────
    dim_drug_df = (
        df
        .withColumns(transform_dim_drug_expr)
        .select(*transform_dim_drug_expr.keys())  # ← only silver columns, no bronze noise
    )
    df_uniq=dim_drug_df.dropDuplicates()
    return df_uniq


def transform_dim_member(df: DataFrame) -> DataFrame:

    # ── pass 1: base columns + type casts ─────────────────────────────────────
    base_expr: dict = {
        "member_id"  : F.col("member_id"),
        "first_name" : F.col("first_name"),
        "last_name"  : F.col("last_name"),
        "full_name"  : F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
        "dob"        : F.to_date(F.col("dob"), "yyyy-MM-dd"),
        "state"      : F.upper(F.col("state")),
        "ssn"        : F.upper(F.col("ssn")),
        "phone"      : F.col("phone"),
        "insert_ts"  : F.to_timestamp(F.col("ingestion_ts")),
    }

    # ── pass 2: derived columns that depend on pass-1 results ─────────────────
    derived_expr: dict = {
        "age"       : F.floor(F.datediff(F.current_date(), F.col("dob")) / 365),
        "member_sk" : sk_hash_expr(["member_id"]),
    }

    # ── final select = all keys across both passes ─────────────────────────────
    final_cols = list(base_expr.keys()) + list(derived_expr.keys())

    return (
        df
        .withColumns(base_expr)
        .withColumns(derived_expr)
        .select(*final_cols)
    )

def transform_dim_plan(df:DataFrame)-> DataFrame:

    transform_dim_drug_expr: dict = {
                "plan_id" :F.col("plan_id") ,
                "plan_name" :F.lower(F.col("plan_name")) ,
                "plan_type" : F.lower(F.col("plan_type") ) ,
                "plan_sk" : sk_hash_expr(["plan_id"]) ,
                "insert_ts" :F.to_timestamp(F.col("ingestion_ts"))
    }
    plan_df=(df
             .withColumns(transform_dim_drug_expr)
             .select(*transform_dim_drug_expr.keys()))
    return plan_df


def transform_dim_prescriber(df: DataFrame) -> DataFrame:
    transform_dim_prescriber_base_expr: dict = {
        "prescriber_id": F.col("prescriber_id"),
        "first_name": F.col("first_name"),
        "last_name": F.col("last_name"),
        "full_name": F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
        "speciality": F.trim(F.col("specialty")),
        "state": F.upper(F.col("state")),
        "phone": F.col("phone"),
        "npi": F.col("npi").cast("int"),
        "insert_ts": F.to_timestamp(F.col("ingestion_ts")),
    }

    # ── pass 2: derived columns that depend on pass-1 results ─────────────────────
    transform_dim_prescriber_derived_expr: dict = {
        "prescriber_sk": sk_hash_expr(["prescriber_id"]),
    }

    # ── final select = all keys across both passes ─────────────────────────────────
    final_cols = list(transform_dim_prescriber_base_expr.keys()) + \
                 list(transform_dim_prescriber_derived_expr.keys())

    return (
        df
        .withColumns(transform_dim_prescriber_base_expr)
        .withColumns(transform_dim_prescriber_derived_expr)
        .select(*final_cols)
    )




