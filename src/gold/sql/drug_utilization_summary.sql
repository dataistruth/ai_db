-- 2. drug_utilization_summary.sql
CREATE OR REPLACE MATERIALIZED VIEW {G}.drug_utilization_summary
COMMENT 'Monthly drug trend by class'
AS
SELECT
    DATE_FORMAT(f.fill_date, 'yyyy-MM')                             AS fill_month,
    f.drug_sk,
    f.plan_sk,
    d.drug_class,
    COUNT(f.claim_id)                                               AS total_scripts,
    SUM(f.quantity)                                                 AS total_quantity,
    SUM(f.days_supply)                                              AS total_days_supply,
    SUM(f.cost)                                                     AS total_cost,
    ROUND(SUM(f.cost)
        / NULLIF(COUNT(f.claim_id), 0), 2)                          AS avg_cost_per_script,
    CURRENT_TIMESTAMP()                                             AS batch_ts
FROM {S}.fact_claim  f
JOIN {S}.d_drug      d ON f.drug_sk = d.drug_sk
                       AND d.__END_AT IS NULL
GROUP BY
    DATE_FORMAT(f.fill_date, 'yyyy-MM'),
    f.drug_sk,
    f.plan_sk,
    d.drug_class