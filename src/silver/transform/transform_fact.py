from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from common_utils.common.scd_transform import scd2_merge_delta
from common_utils.common.utils import sk_hash_expr

def transform_fact_claim(input_df:DataFrame,
                     member_sk_df:DataFrame,
                     plan_sk_df:DataFrame,
                     prescriber_sk_df:DataFrame,
                     drug_sk_df:DataFrame,
                     date_df:DataFrame,
)->DataFrame:
    join_df = (
        input_df
        .join(
            F.broadcast(member_sk_df),
            on=input_df.member_id == member_sk_df.member_id,
            how="left"
        )
        .join(
            F.broadcast(plan_sk_df),
            on=input_df.plan_id == plan_sk_df.plan_id,
            how="left"
        )
        .join(
            F.broadcast(prescriber_sk_df),
            on=input_df.prescriber_id == prescriber_sk_df.prescriber_id,
            how="left"
        )
        .join(
            F.broadcast(drug_sk_df),
            on=input_df.drug_code == drug_sk_df.drug_sk,
            how="left"
        )
        .join(
            F.broadcast(date_df),
            on=input_df.fill_date == date_df.date,
            how="left"
        )
    ).select(
        input_df["*"],
        member_sk_df["member_sk"],
        plan_sk_df["plan_sk"],
        prescriber_sk_df["prescriber_sk"],
        drug_sk_df["drug_sk"],
        date_df["date"],
        date_df["year"],
        date_df["month"],
        date_df["day"],
        date_df["quarter"],
        date_df["week_of_year"],
        date_df["day_of_week"],
        date_df["is_weekend"],
        F.current_timestamp().alias("insert_ts"))

# Write the trasnform
    transform_expr = {
        "claim_id" : F.col("claim_id"),
        "member_sk": F.col("member_sk"),
        "plan_sk": F.col("plan_sk"),
        "prescriber_sk": F.col("prescriber_sk"),
        "drug_sk": F.col("drug_sk"),

        "cost": F.col("cost").cast("decimal(11,2)"),
        "days_supply": F.col("days_supply").cast("int"),
        "quantity": F.col("quantity").cast("int"),

        "fill_date": F.to_date(F.col("fill_date")),
        "insert_ts": F.to_timestamp(F.col("ingestion_ts")),

        "refill_flag": F.when(
            F.col("refill_flag").isNull() | (F.col("refill_flag") == ""),
            F.lit(None).cast("string")
        ).otherwise(F.col("refill_flag")),

        "pharmacy_id": F.col("pharmacy_id"),
        "status": F.col("status"),

        "year": F.col("year"),
        "month": F.col("month"),
        "day": F.col("day"),
        "is_weekend": F.col("is_weekend"),

        # ⚠️ FIX: quarter should NOT be quarter(quarter)
        "quarter": F.col("quarter"),

        "claim_sk": sk_hash_expr(
            ["member_sk", "plan_sk", "prescriber_sk", "drug_sk", "fill_date"]
        )
    }

    transform_df = (join_df
                .withColumns(transform_expr)
                .select(*transform_expr.keys())
                .dropDuplicates(["claim_sk"])
    )
    return transform_df

