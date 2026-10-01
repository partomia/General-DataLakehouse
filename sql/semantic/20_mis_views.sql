-- MIS consumers (the "Banking KPIs MIS" dashboard in Data Visualization). Every view here
-- aggregates a certified KPI view and nothing else, so MIS can only differ from the
-- certified figure by grouping, never by formula.

-- ---------------------------------------------------------------- NPA exposure

DROP VIEW IF EXISTS rsingh_gdl_semantic.mis_npa_trend;

CREATE VIEW rsingh_gdl_semantic.mis_npa_trend
COMMENT 'MIS: bank-level NPA by reporting date, from kpi_npa_exposure'
AS
SELECT reporting_date,
       COUNT(*)                                                    AS loans,
       SUM(CASE WHEN is_npa THEN 1 ELSE 0 END)                     AS npa_loans,
       SUM(gross_advance)                                          AS gross_advances,
       SUM(gross_npa)                                              AS gross_npa,
       CAST(SUM(gross_npa) AS DOUBLE) / CAST(SUM(gross_advance) AS DOUBLE) AS gross_npa_ratio,
       SUM(npa_provision)                                          AS npa_provision,
       SUM(gross_npa) - SUM(npa_provision)                         AS net_npa,
       SUM(provision_amount)                                       AS total_provision,
       CASE WHEN SUM(gross_npa) = 0 THEN NULL
            ELSE CAST(SUM(npa_provision) AS DOUBLE) / CAST(SUM(gross_npa) AS DOUBLE) END AS provision_coverage
FROM rsingh_gdl_semantic.kpi_npa_exposure
GROUP BY reporting_date;

DROP VIEW IF EXISTS rsingh_gdl_semantic.mis_npa_breakdown;

CREATE VIEW rsingh_gdl_semantic.mis_npa_breakdown
COMMENT 'MIS: NPA by reporting date, region, branch, product and asset class, from kpi_npa_exposure'
AS
SELECT reporting_date, region, branch_code, branch_name, product_code, product_name, asset_class,
       COUNT(*)              AS loans,
       SUM(gross_advance)    AS gross_advances,
       SUM(gross_npa)        AS gross_npa,
       SUM(provision_amount) AS provision_amount
FROM rsingh_gdl_semantic.kpi_npa_exposure
GROUP BY reporting_date, region, branch_code, branch_name, product_code, product_name, asset_class;

-- ---------------------------------------------------------------- CASA ratio

DROP VIEW IF EXISTS rsingh_gdl_semantic.mis_casa_trend;

CREATE VIEW rsingh_gdl_semantic.mis_casa_trend
COMMENT 'MIS: bank-level CASA ratio by reporting date, from kpi_casa_ratio'
AS
SELECT reporting_date,
       COUNT(*)             AS accounts,
       SUM(casa_balance)    AS casa_balance,
       SUM(term_balance)    AS term_balance,
       SUM(deposit_balance) AS total_deposits,
       CAST(SUM(casa_balance) AS DOUBLE) / CAST(SUM(deposit_balance) AS DOUBLE) AS casa_ratio
FROM rsingh_gdl_semantic.kpi_casa_ratio
GROUP BY reporting_date;

DROP VIEW IF EXISTS rsingh_gdl_semantic.mis_casa_breakdown;

CREATE VIEW rsingh_gdl_semantic.mis_casa_breakdown
COMMENT 'MIS: CASA ratio by reporting date, region, branch and customer segment, from kpi_casa_ratio'
AS
SELECT reporting_date, region, branch_code, branch_name, segment,
       COUNT(*)             AS accounts,
       SUM(casa_balance)    AS casa_balance,
       SUM(term_balance)    AS term_balance,
       SUM(deposit_balance) AS total_deposits,
       CASE WHEN SUM(deposit_balance) = 0 THEN NULL
            ELSE CAST(SUM(casa_balance) AS DOUBLE) / CAST(SUM(deposit_balance) AS DOUBLE) END AS casa_ratio
FROM rsingh_gdl_semantic.kpi_casa_ratio
GROUP BY reporting_date, region, branch_code, branch_name, segment;

-- ---------------------------------------------------------------- customer relationship value

DROP VIEW IF EXISTS rsingh_gdl_semantic.mis_crv_segment;

CREATE VIEW rsingh_gdl_semantic.mis_crv_segment
COMMENT 'MIS: relationship value by reporting date, segment, home branch and value band, from kpi_customer_relationship_value'
AS
SELECT reporting_date, segment, home_branch,
       CASE WHEN crv >= 100000 THEN 'A: 1 lakh and above'
            WHEN crv >= 25000 THEN 'B: 25k to 1 lakh'
            WHEN crv >= 5000 THEN 'C: 5k to 25k'
            ELSE 'D: below 5k' END AS value_band,
       COUNT(*)           AS parties,
       SUM(casa_value)    AS casa_value,
       SUM(td_value)      AS td_value,
       SUM(lending_value) AS lending_value,
       SUM(fee_value)     AS fee_value,
       SUM(crv)           AS crv_total,
       AVG(crv)           AS crv_average
FROM rsingh_gdl_semantic.kpi_customer_relationship_value
GROUP BY reporting_date, segment, home_branch,
         CASE WHEN crv >= 100000 THEN 'A: 1 lakh and above'
              WHEN crv >= 25000 THEN 'B: 25k to 1 lakh'
              WHEN crv >= 5000 THEN 'C: 5k to 25k'
              ELSE 'D: below 5k' END;

DROP VIEW IF EXISTS rsingh_gdl_semantic.mis_crv_top_relationships;

CREATE VIEW rsingh_gdl_semantic.mis_crv_top_relationships
COMMENT 'MIS: the 25 most valuable relationships per reporting date, from kpi_customer_relationship_value'
AS
SELECT * FROM (
    SELECT v.reporting_date,
           ROW_NUMBER() OVER (PARTITION BY v.reporting_date ORDER BY v.crv DESC, v.party_id) AS crv_rank,
           v.party_id, p.full_name, v.segment, v.home_branch,
           v.casa_value, v.td_value, v.lending_value, v.fee_value, v.crv
    FROM rsingh_gdl_semantic.kpi_customer_relationship_value v
    LEFT JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = v.party_sk
) ranked
WHERE crv_rank <= 25;
