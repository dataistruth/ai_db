-- 3. member_spend_summary.sql
CREATE OR REPLACE MATERIALIZED VIEW {G}.member_spend_summary
COMMENT 'Monthly member cost exposure'
AS
SELECT
    DATE_FORMAT(f.fill_date, 'yyyy-MM')                             AS fill_month,
    f.member_sk,
    f.plan_sk,
    COUNT(f.claim_id)                                               AS total_claims,
    COUNT(CASE WHEN f.status = 'REJECTED' THEN 1 END)              AS rejected_claims,
    SUM(f.cost)                                                     AS total_cost,
    ROUND(COUNT(CASE WHEN f.status = 'REJECTED' THEN 1 END)
        / NULLIF(COUNT(f.claim_id), 0), 4)                          AS rejection_rate,
    ROUND(SUM(f.cost)
        / NULLIF(COUNT(f.claim_id), 0), 2)                          AS avg_cost_per_claim,
    CURRENT_TIMESTAMP()                                             AS batch_ts
FROM {S}.fact_claim f
GROUP BY
    DATE_FORMAT(f.fill_date, 'yyyy-MM'),
    f.member_sk,
    f.plan_sk