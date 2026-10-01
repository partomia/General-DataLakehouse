-- Certified KPI: CASA ratio (ref.kpi_definition kpi_code = 'CASA').
-- Grain: deposit account x reporting date, end-of-day ledger balance in INR. The customer
-- segment is the one valid on the reporting date (the dim_party SCD2 version the fact points to).
--   CASA ratio = SUM(casa_balance) / SUM(deposit_balance)

DROP VIEW IF EXISTS rsingh_gdl_semantic.kpi_casa_ratio;

CREATE VIEW rsingh_gdl_semantic.kpi_casa_ratio
COMMENT 'Certified KPI CASA ratio v1.0: account x reporting date, SUM(casa_balance) / SUM(deposit_balance)'
AS
SELECT
    f.business_date                                            AS reporting_date,
    f.date_key,
    f.account_key,
    f.party_id,
    f.party_sk,
    COALESCE(p.segment, 'UNRESOLVED')                          AS segment,
    f.product_code,
    pr.product_name,
    f.branch_code,
    b.ifsc                                                     AS branch_ifsc,
    b.branch_name,
    b.region,
    f.deposit_type,
    f.is_casa,
    f.currency,
    CAST(f.balance_inr AS DECIMAL(18,2))                       AS deposit_balance,
    CAST(CASE WHEN f.is_casa THEN f.balance_inr ELSE 0 END AS DECIMAL(18,2)) AS casa_balance,
    CAST(CASE WHEN f.deposit_type = 'TERM' THEN f.balance_inr ELSE 0 END AS DECIMAL(18,2)) AS term_balance
FROM rsingh_gdl_gold.fact_deposit_balance_daily f
LEFT JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = f.party_sk
LEFT JOIN rsingh_gdl_gold.dim_product pr ON pr.product_code = f.product_code
LEFT JOIN rsingh_gdl_gold.dim_branch b ON b.branch_code = f.branch_code;
