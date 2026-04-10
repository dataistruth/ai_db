-- 4. plan_financial_summary.sql
CREATE OR REPLACE MATERIALIZED VIEW {G}.plan_financial_summary
COMMENT 'Plan cost benchmarking by type'
AS
SELECT
    DATE_FORMAT(f.fill_date, 'yyyy-MM')                             AS fill_month,
    f.plan_sk,
    p.plan_type,
    COUNT(f.claim_id)                                               AS total_claims,
    COUNT(CASE WHEN f.status = 'PAID'     THEN 1 END)              AS paid_claims,
    COUNT(CASE WHEN f.status = 'REJECTED' THEN 1 END)              AS rejected_claims,
    SUM(f.cost)                                                     AS total_cost,
    ROUND(SUM(f.cost)
        / NULLIF(COUNT(f.claim_id), 0), 2)                          AS avg_cost_per_claim,
    CURRENT_TIMESTAMP()                                             AS batch_ts
FROM {S}.fact_claim  f
JOIN {S}.d_plan      p ON f.plan_sk = p.plan_sk
                       AND p.__END_AT IS NULL
GROUP BY
    DATE_FORMAT(f.fill_date, 'yyyy-MM'),
    f.plan_sk,
    p.plan_type