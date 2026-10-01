-- Certified KPI: customer relationship value (ref.kpi_definition kpi_code = 'CRV').
-- Two views, one definition:
--   kpi_crv_component                party x component x product x reporting date (finest grain)
--   kpi_customer_relationship_value  party x reporting date, the sum of its components
-- Annualised INR. Spreads, margins, the fee window and the annualisation factor come from
-- ref.kpi_parameter. Non-performing loans earn nothing. Fees count settled payments in the
-- window ending on the reporting date, by channel. Parties not resolved by MDM (UNKNOWN)
-- have no relationship value.
--   crv = casa_value + td_value + lending_value + fee_value

DROP VIEW IF EXISTS rsingh_gdl_semantic.kpi_customer_relationship_value;
DROP VIEW IF EXISTS rsingh_gdl_semantic.kpi_crv_component;

CREATE VIEW rsingh_gdl_semantic.kpi_crv_component
COMMENT 'Certified KPI customer relationship value v1.0, finest grain: party x component x product x reporting date'
AS
WITH prm AS (
    SELECT MAX(CASE WHEN parameter = 'casa_spread' THEN value END)          AS casa_spread,
           MAX(CASE WHEN parameter = 'td_spread' THEN value END)            AS td_spread,
           MAX(CASE WHEN parameter = 'fee_window_days' THEN value END)      AS fee_window_days,
           MAX(CASE WHEN parameter = 'annualisation_factor' THEN value END) AS annualisation_factor
    FROM rsingh_gdl_ref.kpi_parameter
    WHERE kpi_code = 'CRV'
),
reporting_dates AS (
    SELECT DISTINCT business_date AS reporting_date FROM rsingh_gdl_gold.fact_deposit_balance_daily
),
deposits AS (
    SELECT f.business_date AS reporting_date, f.party_id,
           CASE WHEN f.is_casa THEN 'CASA' ELSE 'TERM_DEPOSIT' END AS component,
           f.product_code, SUM(f.balance_inr) AS base_amount
    FROM rsingh_gdl_gold.fact_deposit_balance_daily f
    WHERE f.party_id <> 'UNKNOWN'
    GROUP BY f.business_date, f.party_id, CASE WHEN f.is_casa THEN 'CASA' ELSE 'TERM_DEPOSIT' END, f.product_code
),
loans AS (
    SELECT f.business_date AS reporting_date, f.party_id, f.product_code,
           SUM(CASE WHEN f.is_npa THEN 0 ELSE f.principal_outstanding END) AS base_amount
    FROM rsingh_gdl_gold.fact_loan_position_daily f
    WHERE f.party_id <> 'UNKNOWN'
    GROUP BY f.business_date, f.party_id, f.product_code
),
fees AS (
    SELECT d.reporting_date, p.party_id, p.channel, SUM(p.fee_amount) AS base_amount
    FROM reporting_dates d
    CROSS JOIN rsingh_gdl_gold.fact_payment p
    CROSS JOIN prm
    WHERE p.status = 'SETTLED' AND p.party_id <> 'UNKNOWN' AND p.fee_amount > 0
      AND p.business_date <= d.reporting_date
      AND datediff(d.reporting_date, p.business_date) < prm.fee_window_days
    GROUP BY d.reporting_date, p.party_id, p.channel
),
components AS (
    SELECT x.reporting_date, x.party_id, x.component, x.product_code, pr.product_name, x.base_amount,
           CASE WHEN x.component = 'CASA' THEN prm.casa_spread ELSE prm.td_spread END AS rate
    FROM deposits x
    CROSS JOIN prm
    LEFT JOIN rsingh_gdl_gold.dim_product pr ON pr.product_code = x.product_code
    UNION ALL
    SELECT x.reporting_date, x.party_id, 'LENDING', x.product_code, pr.product_name, x.base_amount,
           COALESCE(m.value, 0)
    FROM loans x
    LEFT JOIN rsingh_gdl_ref.kpi_parameter m
           ON m.kpi_code = 'CRV' AND m.parameter = concat('lending_margin.', x.product_code)
    LEFT JOIN rsingh_gdl_gold.dim_product pr ON pr.product_code = x.product_code
    UNION ALL
    SELECT x.reporting_date, x.party_id, 'FEES', x.channel, concat('Fees: ', x.channel), x.base_amount,
           prm.annualisation_factor
    FROM fees x
    CROSS JOIN prm
)
SELECT reporting_date,
       party_id,
       component,
       product_code,
       product_name,
       CAST(base_amount AS DECIMAL(18,2))        AS base_amount,
       rate,
       CAST(base_amount * rate AS DECIMAL(18,2)) AS annual_value
FROM components;

CREATE VIEW rsingh_gdl_semantic.kpi_customer_relationship_value
COMMENT 'Certified KPI customer relationship value v1.0: golden party x reporting date, sum of kpi_crv_component'
AS
SELECT
    c.reporting_date,
    CAST(regexp_replace(CAST(c.reporting_date AS STRING), '-', '') AS INT) AS date_key,
    c.party_id,
    p.party_sk,
    COALESCE(p.segment, 'UNRESOLVED') AS segment,
    p.home_branch,
    c.casa_balance,
    c.td_balance,
    c.performing_loans,
    c.fee_income_window,
    c.casa_value,
    c.td_value,
    c.lending_value,
    c.fee_value,
    c.crv
FROM (
    SELECT reporting_date, party_id,
           CAST(SUM(CASE WHEN component = 'CASA' THEN base_amount ELSE 0 END) AS DECIMAL(18,2))          AS casa_balance,
           CAST(SUM(CASE WHEN component = 'TERM_DEPOSIT' THEN base_amount ELSE 0 END) AS DECIMAL(18,2))  AS td_balance,
           CAST(SUM(CASE WHEN component = 'LENDING' THEN base_amount ELSE 0 END) AS DECIMAL(18,2))       AS performing_loans,
           CAST(SUM(CASE WHEN component = 'FEES' THEN base_amount ELSE 0 END) AS DECIMAL(18,2))          AS fee_income_window,
           CAST(SUM(CASE WHEN component = 'CASA' THEN annual_value ELSE 0 END) AS DECIMAL(18,2))         AS casa_value,
           CAST(SUM(CASE WHEN component = 'TERM_DEPOSIT' THEN annual_value ELSE 0 END) AS DECIMAL(18,2)) AS td_value,
           CAST(SUM(CASE WHEN component = 'LENDING' THEN annual_value ELSE 0 END) AS DECIMAL(18,2))      AS lending_value,
           CAST(SUM(CASE WHEN component = 'FEES' THEN annual_value ELSE 0 END) AS DECIMAL(18,2))         AS fee_value,
           CAST(SUM(annual_value) AS DECIMAL(18,2))                                                      AS crv
    FROM rsingh_gdl_semantic.kpi_crv_component
    GROUP BY reporting_date, party_id
) c
LEFT JOIN rsingh_gdl_gold.dim_party p
       ON p.party_id = c.party_id AND p.effective_from <= c.reporting_date AND p.effective_to >= c.reporting_date;
