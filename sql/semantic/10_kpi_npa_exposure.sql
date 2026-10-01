-- Certified KPI: NPA exposure (ref.kpi_definition kpi_code = 'NPA').
-- Grain: loan x reporting date. RBI IRAC asset class and NPA flag come from the gold fact
-- (days past due against config/kpi.json); provision rates come from ref.kpi_parameter, so a
-- rate changes in one place. Consumers aggregate this view and never re-derive the formula:
--   gross NPA       = SUM(gross_npa)
--   gross advances  = SUM(gross_advance)
--   gross NPA ratio = SUM(gross_npa) / SUM(gross_advance)
--   net NPA         = SUM(gross_npa) - SUM(npa_provision)

DROP VIEW IF EXISTS rsingh_gdl_semantic.kpi_npa_exposure;

CREATE VIEW rsingh_gdl_semantic.kpi_npa_exposure
COMMENT 'Certified KPI NPA exposure v1.0: loan x reporting date, RBI IRAC classes, provision from ref.kpi_parameter'
AS
SELECT
    f.business_date                                            AS reporting_date,
    f.date_key,
    f.loan_key,
    l.loan_id,
    f.party_id,
    f.party_sk,
    f.product_code,
    pr.product_name,
    f.branch_code,
    b.ifsc                                                     AS branch_ifsc,
    b.branch_name,
    b.region,
    f.dpd,
    f.sma_status,
    f.asset_class,
    f.is_npa,
    f.npa_since,
    f.loan_status,
    CAST(f.principal_outstanding AS DECIMAL(18,2))             AS gross_advance,
    CAST(f.principal_overdue + f.interest_overdue AS DECIMAL(18,2)) AS overdue_amount,
    CAST(CASE WHEN f.is_npa THEN f.principal_outstanding ELSE 0 END AS DECIMAL(18,2)) AS gross_npa,
    COALESCE(r.value, 0)                                       AS provision_rate,
    CAST(f.principal_outstanding * COALESCE(r.value, 0) AS DECIMAL(18,2)) AS provision_amount,
    CAST(CASE WHEN f.is_npa THEN f.principal_outstanding * COALESCE(r.value, 0) ELSE 0 END
         AS DECIMAL(18,2))                                     AS npa_provision
FROM rsingh_gdl_gold.fact_loan_position_daily f
LEFT JOIN rsingh_gdl_gold.dim_loan l ON l.loan_sk = f.loan_sk
LEFT JOIN rsingh_gdl_gold.dim_product pr ON pr.product_code = f.product_code
LEFT JOIN rsingh_gdl_gold.dim_branch b ON b.branch_code = f.branch_code
LEFT JOIN rsingh_gdl_ref.kpi_parameter r
       ON r.kpi_code = 'NPA' AND r.parameter = concat('provision_rate.', f.asset_class);
