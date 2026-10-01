-- Ad-hoc consumers of the certified KPIs, for Hue (Impala). Hue asks for the ${...}
-- variables when a query runs. Each query only reads the certified KPI views (plus dimensions
-- for descriptive attributes), and scripts/run_semantic.py runs the same queries in its
-- consistency check, so an ad-hoc answer is checked against the certified figure.

-- name: npa_borrowers
-- NPA borrowers on a reporting date, with their golden record (how many source records were
-- merged into it, from which systems) and the rest of their relationship with the bank.
SELECT k.reporting_date,
       k.party_id,
       p.full_name,
       p.pan,
       p.mobile,
       p.segment,
       p.source_systems,
       p.member_records,
       k.loan_id,
       k.product_name,
       k.branch_name,
       k.asset_class,
       k.dpd,
       k.npa_since,
       k.gross_npa                    AS npa_outstanding,
       k.provision_amount,
       COALESCE(dep.deposit_accounts, 0) AS deposit_accounts,
       COALESCE(dep.deposit_balance, 0)  AS deposit_balance,
       ln.loans - 1                   AS other_loans,
       ln.npa_loans - 1               AS other_npa_loans
FROM rsingh_gdl_semantic.kpi_npa_exposure k
LEFT JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = k.party_sk
LEFT JOIN (
    SELECT party_id, COUNT(*) AS deposit_accounts, SUM(deposit_balance) AS deposit_balance
    FROM rsingh_gdl_semantic.kpi_casa_ratio
    WHERE reporting_date = DATE '${reporting_date}'
    GROUP BY party_id
) dep ON dep.party_id = k.party_id AND k.party_id <> 'UNKNOWN'
LEFT JOIN (
    SELECT party_id, COUNT(*) AS loans, SUM(CASE WHEN is_npa THEN 1 ELSE 0 END) AS npa_loans
    FROM rsingh_gdl_semantic.kpi_npa_exposure
    WHERE reporting_date = DATE '${reporting_date}'
    GROUP BY party_id
) ln ON ln.party_id = k.party_id
WHERE k.reporting_date = DATE '${reporting_date}' AND k.is_npa
ORDER BY npa_outstanding DESC;

-- name: casa_branch_change
-- CASA ratio by branch on two reporting dates, branches where it fell first.
SELECT branch_code,
       branch_name,
       region,
       casa_balance,
       total_deposits,
       casa_ratio,
       previous_casa_ratio,
       casa_ratio - previous_casa_ratio AS ratio_change,
       CASE WHEN casa_ratio < previous_casa_ratio THEN 'FELL' ELSE 'HELD OR ROSE' END AS movement
FROM (
    SELECT branch_code, branch_name, region,
           SUM(CASE WHEN reporting_date = DATE '${reporting_date}' THEN casa_balance ELSE 0 END)    AS casa_balance,
           SUM(CASE WHEN reporting_date = DATE '${reporting_date}' THEN deposit_balance ELSE 0 END) AS total_deposits,
           CAST(SUM(CASE WHEN reporting_date = DATE '${reporting_date}' THEN casa_balance ELSE 0 END) AS DOUBLE)
             / CAST(SUM(CASE WHEN reporting_date = DATE '${reporting_date}' THEN deposit_balance ELSE 0 END) AS DOUBLE)
             AS casa_ratio,
           CAST(SUM(CASE WHEN reporting_date = DATE '${previous_date}' THEN casa_balance ELSE 0 END) AS DOUBLE)
             / CAST(SUM(CASE WHEN reporting_date = DATE '${previous_date}' THEN deposit_balance ELSE 0 END) AS DOUBLE)
             AS previous_casa_ratio
    FROM rsingh_gdl_semantic.kpi_casa_ratio
    WHERE reporting_date IN (DATE '${reporting_date}', DATE '${previous_date}')
    GROUP BY branch_code, branch_name, region
) b
ORDER BY ratio_change, branch_code;

-- name: party_value_by_product
-- One customer's relationship value on a reporting date, broken down by component and product.
SELECT c.reporting_date,
       c.party_id,
       p.full_name,
       p.segment,
       c.component,
       c.product_code,
       c.product_name,
       c.base_amount,
       c.rate,
       c.annual_value
FROM rsingh_gdl_semantic.kpi_crv_component c
LEFT JOIN rsingh_gdl_gold.dim_party p
       ON p.party_id = c.party_id AND p.effective_from <= c.reporting_date AND p.effective_to >= c.reporting_date
WHERE c.reporting_date = DATE '${reporting_date}' AND c.party_id = '${party_id}'
ORDER BY c.annual_value DESC;
