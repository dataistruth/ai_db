-- 1. claim_quality_summary.sql
CREATE OR REPLACE MATERIALIZED VIEW {G}.claim_quality_summary
COMMENT 'Adjudication quality, ops SLA, status trending'
AS
SELECT
    DATE_FORMAT(f.fill_date, 'yyyy-MM')                             AS fill_month,
    f.plan_sk,
    f.drug_sk,
    COUNT(f.claim_id)                                               AS total_claims,
    COUNT(CASE WHEN f.status = 'PAID'     THEN 1 END)              AS paid_claims,
    COUNT(CASE WHEN f.status = 'REJECTED' THEN 1 END)              AS rejected_claims,
    COUNT(CASE WHEN f.status = 'REVERSED' THEN 1 END)              AS reversed_claims,
    ROUND(COUNT(CASE WHEN f.status = 'REJECTED' THEN 1 END)
        / NULLIF(COUNT(f.claim_id), 0), 4)                          AS reject_rate,
    ROUND(COUNT(CASE WHEN f.status = 'REVERSED' THEN 1 END)
        / NULLIF(COUNT(f.claim_id), 0), 4)                          AS reversal_rate,
    ROUND(COUNT(CASE WHEN f.status = 'PAID'     THEN 1 END)
        / NULLIF(COUNT(f.claim_id), 0), 4)                          AS pct_paid,
    CURRENT_TIMESTAMP()                                             AS batch_ts
FROM {S}.fact_claim f
GROUP BY
    DATE_FORMAT(f.fill_date, 'yyyy-MM'),
    f.plan_sk,
    f.drug_sk