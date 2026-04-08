-- ============================================
-- Catalog & Schema Setup
-- ============================================
USE CATALOG ai_dev_db;

CREATE SCHEMA IF NOT EXISTS silver;

USE SCHEMA silver;

-- ============================================
-- Dimension: Member
-- ============================================
CREATE TABLE IF NOT EXISTS dim_member (
    member_id STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    dob DATE,
    age INT,
    state STRING,
    phone STRING,
    ingest_ts TIMESTAMP
)
USING DELTA;

-- ============================================
-- Dimension: Plan
-- ============================================
CREATE TABLE IF NOT EXISTS dim_plan (
    plan_id STRING,
    plan_name STRING,
    plan_type STRING,
    deductible DOUBLE,
    oop_max DOUBLE,
    ingest_ts TIMESTAMP
)
USING DELTA;

-- ============================================
-- Dimension: Drug
-- ============================================
CREATE TABLE IF NOT EXISTS dim_drug (
    drug_code STRING,
    drug_name STRING,
    drug_class STRING,
    formulary_flag STRING,
    ingest_ts TIMESTAMP
)
USING DELTA;

-- ============================================
-- Dimension: Prescriber
-- ============================================
CREATE TABLE IF NOT EXISTS dim_prescriber (
    prescriber_id STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    specialty STRING,
    npi STRING,
    state STRING,
    phone STRING,
    ingest_ts TIMESTAMP
)
USING DELTA;

-- ============================================
-- Dimension: Date (Optional but recommended)
-- ============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT,
    date DATE,
    year INT,
    month INT,
    day INT,
    day_of_week STRING
)
USING DELTA;

-- ============================================
-- Fact Table: Claim
-- ============================================
CREATE TABLE IF NOT EXISTS fact_claim (
    claim_id STRING,
    member_id STRING,
    prescriber_id STRING,
    drug_code STRING,
    plan_id STRING,

    fill_date DATE,
    quantity INT,
    days_supply INT,

    billed_amount DOUBLE,
    allowed_amount DOUBLE,
    paid_amount DOUBLE,

    claim_status STRING,
    reject_code STRING,

    ingest_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (fill_date);

-- ============================================
-- Constraints (Optional)
-- ============================================
ALTER TABLE fact_claim
ADD CONSTRAINT chk_billed_amount CHECK (billed_amount >= 0);

-- ============================================
-- Table Properties (Performance Optimization)
-- ============================================
ALTER TABLE fact_claim SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================
-- Z-Ordering (Run after data load)
-- ============================================
-- OPTIMIZE fact_claim
-- ZORDER BY (member_id, drug_code, prescriber_id);