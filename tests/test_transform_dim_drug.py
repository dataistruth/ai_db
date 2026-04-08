from pyspark.sql import Row
from src.silver.transform.transform_dim import transform_dim_drug


def test_transform_dim_drug_dedup(spark):
    # -----------------------
    # Input Data
    # -----------------------
    input_data = [
        Row(drug_code=1, drug_name="A", drug_class="X"),
        Row(drug_code=1, drug_name="A", drug_class="X"),  # duplicate
        Row(drug_code=2, drug_name="B", drug_class="Y"),
    ]

    df_input = spark.createDataFrame(input_data)

    # -----------------------
    # Apply Transformation
    # -----------------------
    result_df = transform_dim_drug(df_input)

    # -----------------------
    # Collect Results
    # -----------------------
    result = result_df.collect()

    # -----------------------
    # Assertions
    # -----------------------
    assert len(result) == 2

    result_set = {(r.drug_code, r.drug_name, r.drug_class) for r in result}

    expected = {
        ("1", "A", "X"),
        ("2", "B", "Y"),
    }

    assert result_set == expected