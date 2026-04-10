-- 5. prescriber_performance_summary.sql
CREATE OR REPLACE MATERIALIZED VIEW {G}.prescriber_performance_summary
COMMENT 'GDR reporting, prescriber outlier detection'
AS
SELECT
    DATE_FORMAT(f.fill_date, 'yyyy-MM')                             AS fill_month,
    f.prescriber_sk,
    pr.speciality,
    f.drug_sk,
    d.drug_class,
    COUNT(f.claim_id)                                               AS total_scripts,
    SUM(f.cost)                                                     AS total_cost,
    ROUND(AVG(f.days_supply), 1)                                    AS avg_days_supply,
    ROUND(COUNT(CASE WHEN d.drug_class = 'GENERIC' THEN 1 END)
        / NULLIF(COUNT(f.claim_id), 0), 4)                          AS generic_dispense_rate,
    ROUND(COUNT(CASE WHEN f.status = 'REJECTED'    THEN 1 END)
        / NULLIF(COUNT(f.claim_id), 0), 4)                          AS reject_rate,
    CURRENT_TIMESTAMP()                                             AS batch_ts
FROM {S}.fact_claim      f
JOIN {S}.d_prescriber    pr ON f.prescriber_sk = pr.prescriber_sk
                            AND pr.__END_AT IS NULL
JOIN {S}.d_drug          d  ON f.drug_sk       = d.drug_sk
                            AND d.__END_AT IS NULL
GROUP BY
    DATE_FORMAT(f.fill_date, 'yyyy-MM'),
    f.prescriber_sk,
    pr.speciality,
    f.drug_sk,
    d.drug_class