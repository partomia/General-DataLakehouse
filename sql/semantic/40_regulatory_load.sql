-- Load the regulatory / external datasets for one reporting date (${reporting_date}).
-- INSERT OVERWRITE replaces only that date's partition in one Iceberg commit, so a re-run
-- restates the date and leaves the earlier submissions alone. Each dataset reads its
-- certified KPI view; names and PAN come from the dim_party version valid on the date.

INSERT OVERWRITE TABLE rsingh_gdl_semantic.reg_asset_classification
SELECT k.reporting_date,
       '${bank_name}',
       k.branch_ifsc,
       k.party_id,
       p.full_name,
       p.pan,
       k.loan_id,
       k.product_name,
       l.sanction_amount,
       k.gross_advance,
       k.overdue_amount,
       k.dpd,
       k.asset_class,
       k.is_npa,
       k.npa_since,
       k.provision_rate,
       k.provision_amount,
       '${batch_id}'
FROM rsingh_gdl_semantic.kpi_npa_exposure k
LEFT JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = k.party_sk
LEFT JOIN rsingh_gdl_gold.dim_loan l
       ON l.loan_key = k.loan_key AND l.effective_from <= k.reporting_date AND l.effective_to >= k.reporting_date
WHERE k.reporting_date = DATE '${reporting_date}';

INSERT OVERWRITE TABLE rsingh_gdl_semantic.reg_deposit_composition
SELECT reporting_date, '${bank_name}', branch_ifsc, region, deposit_type, is_casa, size_band,
       COUNT(*), CAST(SUM(deposit_balance) AS DECIMAL(18,2)), '${batch_id}'
FROM (
    SELECT reporting_date, branch_ifsc, region, deposit_type, is_casa, deposit_balance,
           CASE WHEN deposit_balance < 100000 THEN 'A: below 1 lakh'
                WHEN deposit_balance < 1000000 THEN 'B: 1 lakh to 10 lakh'
                WHEN deposit_balance < 10000000 THEN 'C: 10 lakh to 1 crore'
                ELSE 'D: 1 crore and above' END AS size_band
    FROM rsingh_gdl_semantic.kpi_casa_ratio
    WHERE reporting_date = DATE '${reporting_date}'
) d
GROUP BY reporting_date, branch_ifsc, region, deposit_type, is_casa, size_band;

INSERT OVERWRITE TABLE rsingh_gdl_semantic.rpt_customer_profitability
SELECT v.reporting_date, v.party_id, p.full_name, v.segment, v.home_branch,
       v.casa_value, v.td_value, v.lending_value, v.fee_value, v.crv,
       CAST(ROW_NUMBER() OVER (ORDER BY v.crv DESC, v.party_id) AS INT),
       '${batch_id}'
FROM rsingh_gdl_semantic.kpi_customer_relationship_value v
LEFT JOIN rsingh_gdl_gold.dim_party p ON p.party_sk = v.party_sk
WHERE v.reporting_date = DATE '${reporting_date}';
