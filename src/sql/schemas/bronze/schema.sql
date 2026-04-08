-- =========================================
-- CATALOG + SCHEMA (optional)
-- =========================================
-- CREATE CATALOG IF NOT EXISTS ai_db;
-- CREATE SCHEMA IF NOT EXISTS ai_db.bronze;

-- =========================================
-- CLAIM EVENT (RAW)
-- =========================================
CREATE TABLE IF NOT EXISTS ai_db.bronze.claim_event_raw (

    -- Event metadata (core for raw ingestion)
    event_id STRING,                    -- unique event id (UUID or hash)
    event_type STRING,                  -- INSERT / UPDATE / DELETE
    event_timestamp TIMESTAMP,          -- when event occurred at source
    ingestion_timestamp TIMESTAMP,      -- when we ingested the event

    -- Business keys (can be null / inconsistent in raw)
    member_id STRING,
    plan_id STRING,
    drug_code STRING,
    pharmacy_id STRING,

    -- Fact attributes
    fill_date DATE,
    quantity INT,
    cost DECIMAL(10,2),
    status STRING,

    -- Additional useful columns (real-world raw feeds)
    prescriber_id STRING,
    refill_flag BOOLEAN,
    days_supply INT,
    diagnosis_code STRING,

    -- Raw payload (very important for debugging / replay)
    raw_payload STRING,

    -- ingestion metadata
    _source_file STRING,
    _ingest_ts TIMESTAMP

)
USING DELTA;

-- =========================================
-- MEMBER (RAW)  [PII INCLUDED]
-- =========================================
CREATE TABLE IF NOT EXISTS ai_db.bronze.member_raw (
    member_id STRING,
    first_name STRING,
    last_name STRING,
    dob STRING,
    state STRING,
    phone STRING,
    ssn STRING,   -- Sensitive PII (raw)

    -- ingestion metadata
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA;

-- =========================================
-- PLAN (RAW)
-- =========================================
CREATE TABLE IF NOT EXISTS ai_db.bronze.plan_raw (
    plan_id STRING,
    plan_name STRING,
    plan_type STRING,

    -- ingestion metadata
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA;

-- =========================================
-- DRUG (RAW)
-- =========================================
CREATE TABLE IF NOT EXISTS ai_db.bronze.drug_raw (
    drug_code STRING,
    drug_name STRING,
    drug_class STRING,

    -- ingestion metadata
    _source_file STRING,
    _ingest_ts TIMESTAMP
)
USING DELTA;


CREATE TABLE ai_dev_db.dev_msingh_bronze.prescriber_raw (
  first_name STRING,
  last_name STRING,
  npi STRING,
  phone STRING,
  prescriber_id STRING,
  specialty STRING,
  state STRING,
  ingestion_date DATE)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')